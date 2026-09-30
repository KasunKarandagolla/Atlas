"""Fail-closed resolution of decision-calendar entries into existing outcomes.

This module reads immutable indexed evidence and assembles ``MaturedOutcomeV2``.
Persistence remains in the atlas-ops controller through ``index_matured_outcome``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import decimal_value, json_value, sha256_json, timestamp
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import ActualClosedPositionSourceV2

from . import outcomes as outcome_contract
from .outcomes import (
    ActualActionPositionBindingV2,
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DiagnosticTargetEvidenceV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
)

ResolutionStatusV1 = Literal[
    "PENDING", "MATURABLE", "MATURED", "UNRESOLVED", "CENSORED", "UNSUPPORTED"
]

_EVIDENCE_LOOKUP_LIMIT = 8
_MAX_REFERENCED_EVIDENCE = 128
_EXECUTABLE_LABEL_DEFINITION = "net_action_value_v2"
_PAYOFF_UNIT = "USDT"
_QUANTITY_UNIT = "CONTRACTS"


@dataclass(frozen=True)
class OutcomeResolutionV1:
    """One deterministic read-only resolution attempt for a decision calendar entry."""

    decision_ref: str
    status: ResolutionStatusV1
    outcome: MaturedOutcomeV2 | None
    horizon_end_ns: int | None
    reason_code: str | None

    def __post_init__(self) -> None:
        allowed = {"PENDING", "MATURABLE", "MATURED", "UNRESOLVED", "CENSORED", "UNSUPPORTED"}
        if self.status not in allowed:
            raise ValueError("unsupported outcome resolution status")
        if self.horizon_end_ns is not None:
            timestamp(self.horizon_end_ns, field="horizon_end_ns")
        if self.status == "MATURED":
            if self.outcome is None or self.outcome.label_state != LabelStateV2.MATURED:
                raise ValueError("matured resolution requires one matured typed outcome")
        elif self.status == "CENSORED":
            if self.outcome is None or self.outcome.label_state != LabelStateV2.CENSORED:
                raise ValueError("censored resolution requires one censored typed outcome")
        elif self.outcome is not None:
            raise ValueError("intermediate resolution cannot carry a typed outcome")
        if self.status in {"PENDING", "MATURABLE", "UNRESOLVED", "UNSUPPORTED", "CENSORED"}:
            if not isinstance(self.reason_code, str) or not self.reason_code:
                raise ValueError("non-matured resolution requires a sanitized reason code")


@dataclass(frozen=True)
class _ActionIdentity:
    artifact_ref: str
    action_hash: str
    body: Mapping[str, object]
    identity: Mapping[str, object]
    quantity: Decimal


@dataclass(frozen=True)
class _Lookup:
    entries: tuple[ArtifactIndexEntryV2, ...]
    problem: str | None = None


def _wire_decimal(value: object, *, field: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"{field} wire value must be a canonical decimal string")
    return decimal_value(value, field=field, wire=True)


def _actual_close_source(
    repository: OpsRepository,
    source_ref: str,
    now_ns: int,
    *,
    candidate: CandidateActionV2,
    horizon_end_ns: int,
) -> tuple[ArtifactIndexEntryV2, ActualClosedPositionSourceV2]:
    entry = repository.get_artifact(source_ref)
    body = json_value(entry.metadata) if entry is not None else None
    if (entry is None or entry.artifact_type != "ActualClosedPositionSourceV2"
            or entry.artifact_ref != source_ref or entry.content_hash != source_ref
            or entry.available_at_ns > now_ns or not isinstance(body, Mapping)):
        raise ValueError("indexed actual close source unavailable")
    account_scope = body.get("account_scope")
    position_epoch_id = body.get("position_epoch_id")
    key_body = body.get("key")
    close_at_ns = body.get("close_at_ns")
    actual_available_at_ns = body.get("available_at_ns")
    realized_net_pnl = body.get("realized_net_pnl")
    actual_provenance = body.get("actual_system_provenance")
    execution_source_ref = body.get("execution_source_ref")
    economic_source_ref = body.get("economic_source_ref")
    if (body.get("version") != "ACTUAL_ATLAS_V2_CLOSED_POSITION_SOURCE_V1"
            or not isinstance(account_scope, str)
            or not isinstance(position_epoch_id, str)
            or not isinstance(key_body, Mapping)
            or type(close_at_ns) is not int
            or type(actual_available_at_ns) is not int
            or not isinstance(realized_net_pnl, str)
            or not isinstance(actual_provenance, str)
            or not isinstance(execution_source_ref, str)
            or not isinstance(economic_source_ref, str)):
        raise ValueError("indexed actual close source has malformed fields")
    actual = ActualClosedPositionSourceV2(
        account_scope,
        position_epoch_id,
        InstrumentKeyV2.from_dict(key_body),
        close_at_ns,
        _wire_decimal(realized_net_pnl, field="realized_net_pnl"),
        actual_available_at_ns,
        actual_provenance,
        execution_source_ref,
        economic_source_ref,
    )
    if (json_value(body) != actual.to_dict() or actual.content_hash != source_ref
            or actual.key != candidate.key or actual.close_at_ns > horizon_end_ns
            or entry.available_at_ns != actual.available_at_ns):
        raise ValueError("indexed actual close source identity mismatch")

    shared = {
        "account_scope": actual.account_scope,
        "position_epoch_id": actual.position_epoch_id,
        "key": actual.key.to_dict(),
        "close_at_ns": actual.close_at_ns,
    }
    execution = _reference_entry(
        repository, actual.execution_source_ref, "V2ActualExecutionCloseObservationV1", as_of_ns=now_ns
    )
    economics = _reference_entry(
        repository, actual.economic_source_ref, "V2ActualAccountPnlObservationV1", as_of_ns=now_ns
    )
    execution_body = json_value(execution.metadata)
    economics_body = json_value(economics.metadata)
    if (sha256_json(execution_body) != actual.execution_source_ref
            or sha256_json(economics_body) != actual.economic_source_ref
            or execution.available_at_ns > actual.available_at_ns
            or economics.available_at_ns > actual.available_at_ns
            or not isinstance(execution_body, Mapping) or not isinstance(economics_body, Mapping)
            or any(execution_body.get(name) != value for name, value in shared.items())
            or any(economics_body.get(name) != value for name, value in shared.items())
            or execution_body.get("source_system") != "VENUE_RECONCILED_EXECUTION"
            or economics_body.get("source_system") != "ACCOUNT_RECONCILED_CASH"
            or economics_body.get("realized_net_pnl") != canonical_decimal_str(actual.realized_net_pnl)):
        raise ValueError("actual close lacks exact reconciled execution/cash sources")
    return entry, actual


def _result(
    decision_ref: str,
    status: ResolutionStatusV1,
    *,
    horizon_end_ns: int | None = None,
    reason_code: str | None = None,
    outcome: MaturedOutcomeV2 | None = None,
) -> OutcomeResolutionV1:
    return OutcomeResolutionV1(decision_ref, status, outcome, horizon_end_ns, reason_code)


def _lookup(
    repository: OpsRepository,
    *,
    artifact_type: str,
    metadata_path: tuple[str, ...],
    identity_value: str,
    as_of_ns: int,
) -> _Lookup:
    """Use a bounded exact-identity index query; overflow is never sampled."""

    method = getattr(repository, "artifact_entries_by_metadata_identity", None)
    if not callable(method):
        return _Lookup((), "BOUNDED_EVIDENCE_LOOKUP_UNAVAILABLE")
    page = method(
        artifact_type,
        metadata_path,
        identity_value,
        as_of_ns=as_of_ns,
        limit=_EVIDENCE_LOOKUP_LIMIT,
    )
    entries = tuple(getattr(page, "entries", ()))
    if getattr(page, "has_more", False):
        return _Lookup(entries, "EVIDENCE_IDENTITY_OVERFLOW")
    if getattr(page, "invalid_entry_count", 0):
        return _Lookup(entries, "MALFORMED_INDEXED_EVIDENCE")
    for entry in entries:
        if (entry.artifact_type != artifact_type or entry.available_at_ns > as_of_ns
                or entry.content_hash != entry.artifact_ref):
            return _Lookup(entries, "MALFORMED_INDEXED_EVIDENCE")
        node: object = entry.metadata
        for part in metadata_path:
            if not isinstance(node, dict) and not hasattr(node, "get"):
                return _Lookup(entries, "MALFORMED_INDEXED_EVIDENCE")
            node = node.get(part)  # type: ignore[union-attr]
        if node != identity_value:
            return _Lookup(entries, "MALFORMED_INDEXED_EVIDENCE")
    return _Lookup(entries)


def _decision(repository: OpsRepository, indexed: ArtifactIndexEntryV2) -> DecisionCalendarEntryV2:
    if indexed.artifact_type != "DecisionCalendarEntryV2":
        raise ValueError("calendar index entry type mismatch")
    decision = outcome_contract._resolve_decision_calendar_entry(repository, indexed.artifact_ref)
    if decision.content_hash != indexed.content_hash or indexed.artifact_ref != indexed.content_hash:
        raise ValueError("calendar index identity mismatch")
    return decision


def _candidate(repository: OpsRepository, decision: DecisionCalendarEntryV2) -> CandidateActionV2:
    if decision.candidate_ref is None:
        raise ValueError("candidate identity absent")
    indexed = repository.get_artifact(decision.candidate_ref)
    body = indexed.metadata.get("candidate") if indexed is not None else None
    if (indexed is None or indexed.artifact_type != "CandidateActionV2"
            or indexed.content_hash != decision.candidate_ref or not isinstance(body, Mapping)
            or indexed.available_at_ns > decision.decision_at_ns):
        raise ValueError("exact candidate evidence unavailable")
    candidate = CandidateActionV2.from_dict(json_value(body))
    if (candidate.content_hash != decision.candidate_ref
            or candidate.decision_at_ns != decision.decision_at_ns
            or candidate.policy_hash != decision.policy_hash
            or candidate.horizon_end_ns <= decision.decision_at_ns):
        raise ValueError("candidate decision or horizon identity mismatch")
    return candidate


def _action(repository: OpsRepository, decision: DecisionCalendarEntryV2,
            candidate: CandidateActionV2) -> _ActionIdentity:
    ref = decision.action_artifact_ref
    if ref is None or decision.action_hash is None:
        raise ValueError("frozen action absent")
    indexed = repository.get_artifact(ref)
    body = indexed.metadata.get("action_artifact") if indexed is not None else None
    identity = indexed.metadata.get("action_identity") if indexed is not None else None
    if (indexed is None or indexed.artifact_type != "ActionArtifactV2"
            or indexed.content_hash != ref or indexed.available_at_ns > decision.available_at_ns
            or not isinstance(body, Mapping) or not isinstance(identity, Mapping)
            or sha256_json(body) != ref or sha256_json(identity) != decision.action_hash):
        raise ValueError("exact indexed frozen action unavailable")
    key = identity.get("key")
    if (body.get("action_hash") != decision.action_hash
            or body.get("candidate_ref") != decision.candidate_ref
            or body.get("candidate_set_ref") != decision.candidate_set_ref
            or body.get("policy_hash") != decision.policy_hash
            or identity.get("policy_id") != decision.policy_id
            or identity.get("policy_version") != decision.policy_version
            or identity.get("policy_hash") != decision.policy_hash
            or identity.get("horizon_end_ns") != candidate.horizon_end_ns
            or identity.get("product_ref") != body.get("product_ref")
            or json_value(key) != candidate.key.to_dict()):
        raise ValueError("frozen action does not match exact decision and candidate")
    quantity = _wire_decimal(identity.get("quantity"), field="action quantity")
    if quantity <= 0:
        raise ValueError("frozen action quantity is invalid")
    return _ActionIdentity(ref, decision.action_hash, body, identity, quantity)


def _outcome_base(
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    *,
    action: _ActionIdentity | None,
    horizon_end_ns: int,
    matured_at_ns: int,
    available_at_ns: int,
    execution_state: ExecutionOutcomeStateV2,
    label_state: LabelStateV2,
    provenance: OutcomeProvenanceV2,
    label_view: str,
    outcome_target: OutcomeTargetV2,
    evidence_refs: tuple[str, ...],
    evidence_resolution: str,
    evidence_quality: str,
    execution_evidence_ref: str | None = None,
    actual_closed_source_ref: str | None = None,
    actual_action_binding_ref: str | None = None,
    gross_payoff: Decimal | None = None,
    fees: Decimal | None = None,
    funding_cashflow: Decimal | None = None,
    net_payoff: Decimal | None = None,
    fill_quantity: Decimal | None = None,
    requested_quantity: Decimal | None = None,
    label_definition: str = _EXECUTABLE_LABEL_DEFINITION,
    diagnostic_value: Decimal | None = None,
    diagnostic_unit: str | None = None,
    diagnostic_evidence_ref: str | None = None,
    reason: str | None = None,
) -> MaturedOutcomeV2:
    return MaturedOutcomeV2(
        decision_ref=decision.content_hash,
        candidate_set_ref=decision.candidate_set_ref,
        candidate_ref=decision.candidate_ref,
        policy_id=decision.policy_id,
        policy_version=decision.policy_version,
        policy_hash=decision.policy_hash,
        action_hash=action.action_hash if action is not None else None,
        action_artifact_ref=action.artifact_ref if action is not None else None,
        action_absence_reason=None if action is not None else "NO_FROZEN_ACTION",
        instrument_revision=candidate.key.contract_revision,
        venue=candidate.key.venue.value,
        product=candidate.key.product.value,
        decision_at_ns=decision.decision_at_ns,
        horizon_end_ns=horizon_end_ns,
        matured_at_ns=matured_at_ns,
        available_at_ns=available_at_ns,
        label_definition=label_definition,
        label_view=label_view,
        selection_state=decision.selection_state,
        admission_state=decision.admission_state,
        execution_state=execution_state,
        label_state=label_state,
        provenance=provenance,
        payoff_unit=_PAYOFF_UNIT,
        quantity_unit=_QUANTITY_UNIT,
        gross_payoff=gross_payoff,
        fees=fees,
        funding_cashflow=funding_cashflow,
        net_payoff=net_payoff,
        fill_quantity=fill_quantity,
        requested_quantity=requested_quantity,
        mfe=None,
        mae=None,
        evidence_refs=tuple(sorted(set(evidence_refs))),
        execution_evidence_ref=execution_evidence_ref,
        extrema_evidence_ref=None,
        actual_closed_source_ref=actual_closed_source_ref,
        evidence_resolution=evidence_resolution,
        evidence_quality=evidence_quality,
        ambiguity=(),
        outcome_target=outcome_target,
        diagnostic_value=diagnostic_value,
        diagnostic_unit=diagnostic_unit,
        diagnostic_evidence_ref=diagnostic_evidence_ref,
        actual_action_binding_ref=actual_action_binding_ref,
        reason=reason,
    )


def _typed_payload(entry: ArtifactIndexEntryV2, *, key: str) -> dict[str, object]:
    payload = entry.metadata.get(key)
    if not isinstance(payload, Mapping) or sha256_json(payload) != entry.artifact_ref:
        raise ValueError("indexed evidence payload hash mismatch")
    result = json_value(payload)
    if not isinstance(result, dict):
        raise ValueError("indexed evidence payload is not an object")
    return result


def _reference_entry(
    repository: OpsRepository,
    ref: object,
    artifact_type: str | None,
    *,
    as_of_ns: int,
) -> ArtifactIndexEntryV2:
    if not isinstance(ref, str):
        raise ValueError("typed evidence reference missing")
    entry = repository.get_artifact(ref)
    if (entry is None or (artifact_type is not None and entry.artifact_type != artifact_type)
            or entry.artifact_ref != ref
            or entry.content_hash != ref or entry.available_at_ns > as_of_ns):
        raise ValueError("typed evidence reference unavailable")
    return entry


def _validate_replay_support(
    repository: OpsRepository,
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    action: _ActionIdentity,
    payoff: Mapping[str, object],
    payoff_entry: ArtifactIndexEntryV2,
    now_ns: int,
) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, object], tuple[str, ...]]:
    """Require explicit causal fee, funding and replay support before monetary maturity."""

    path_ref = payoff.get("path_ref")
    path_entry = _reference_entry(repository, path_ref, "ReplayPathV2", as_of_ns=now_ns)
    path = path_entry.metadata.get("path")
    if not isinstance(path, Mapping) or sha256_json(path) != path_ref:
        raise ValueError("replay path payload invalid")

    assumptions_ref = payoff.get("replay_assumptions_ref")
    assumptions_entry = _reference_entry(repository, assumptions_ref, "ReplayAssumptionsV2", as_of_ns=now_ns)
    assumptions = json_value(assumptions_entry.metadata)
    if not isinstance(assumptions, Mapping) or sha256_json(assumptions) != assumptions_ref:
        raise ValueError("typed replay assumptions invalid")

    fee_ref = payoff.get("fee_ref")
    fee_entry = _reference_entry(repository, fee_ref, "FeeScheduleV2", as_of_ns=now_ns)
    fee = json_value(fee_entry.metadata)
    if (not isinstance(fee, Mapping) or sha256_json(fee) != fee_ref
            or fee.get("version") != "V2_TAKER_FEES_V1"
            or fee.get("key") != candidate.key.to_dict()
            or fee.get("available_at_ns") != fee_entry.available_at_ns
            or fee_entry.available_at_ns > decision.decision_at_ns):
        raise ValueError("cutoff-known typed fee schedule invalid")
    entry_fee_rate = _wire_decimal(fee.get("entry_taker_rate"), field="entry_taker_rate")
    exit_fee_rate = _wire_decimal(fee.get("exit_taker_rate"), field="exit_taker_rate")
    if not Decimal(0) <= entry_fee_rate <= Decimal(1) or not Decimal(0) <= exit_fee_rate <= Decimal(1):
        raise ValueError("fee rate outside supported range")
    fee_source = _reference_entry(repository, fee.get("source_ref"), None, as_of_ns=now_ns)
    if fee_source.available_at_ns > fee_entry.available_at_ns:
        raise ValueError("fee source was unavailable when fee schedule was declared")

    funding_ref = payoff.get("funding_schedule_ref")
    funding_entry = _reference_entry(repository, funding_ref, "FundingScheduleV2", as_of_ns=now_ns)
    funding_schedule = json_value(funding_entry.metadata)
    expected_times = funding_schedule.get("expected_settlement_times_ns") if isinstance(funding_schedule, Mapping) else None
    explicit_zero = funding_schedule.get("explicit_zero_funding") if isinstance(funding_schedule, Mapping) else None
    if (not isinstance(funding_schedule, Mapping)
            or sha256_json(funding_schedule) != funding_ref
            or funding_schedule.get("version") != "V2_FUNDING_SCHEDULE_V1"
            or funding_schedule.get("available_at_ns") != funding_entry.available_at_ns
            or funding_entry.available_at_ns > decision.decision_at_ns
            or not isinstance(expected_times, list)
            or any(type(item) is not int for item in expected_times)
            or expected_times != sorted(set(expected_times))
            or type(explicit_zero) is not bool
            or (explicit_zero and expected_times)
            or (not explicit_zero and not expected_times)):
        raise ValueError("cutoff-known funding schedule is missing or contradictory")
    funding_source = _reference_entry(repository, funding_schedule.get("source_ref"), None,
                                      as_of_ns=now_ns)
    if funding_source.available_at_ns > funding_entry.available_at_ns:
        raise ValueError("funding schedule source was unavailable when declared")

    product_ref = action.identity.get("product_ref")
    product_entry = _reference_entry(repository, product_ref, "ProductContractV2", as_of_ns=now_ns)
    product = product_entry.metadata.get("product")
    product_contract = ProductContractV2.from_dict(json_value(product)) if isinstance(product, Mapping) else None
    if (product_contract is None or product_contract.content_hash != product_ref
            or product_contract.key != candidate.key):
        raise ValueError("exact indexed product contract invalid")

    input_refs_raw = payoff_entry.metadata.get("input_refs")
    if (not isinstance(input_refs_raw, (tuple, list))
            or len(input_refs_raw) > _MAX_REFERENCED_EVIDENCE
            or any(not isinstance(ref, str) for ref in input_refs_raw)):
        raise ValueError("replay input evidence exceeds its deterministic bound")
    input_refs = tuple(input_refs_raw)
    if input_refs != tuple(sorted(set(input_refs))):
        raise ValueError("replay input refs are not canonical")
    action_body_sizing_ref = action.body.get("sizing_ref")
    expected_types: dict[str, str] = {
        action.artifact_ref: "ActionArtifactV2",
        decision.candidate_set_ref: "CandidateSetV2",
    }
    typed_refs = (
        (path_ref, "ReplayPathV2"),
        (payoff.get("existing_portfolio_ref"), "ExistingPortfolioPathV2"),
        (assumptions_ref, "ReplayAssumptionsV2"),
        (fee_ref, "FeeScheduleV2"),
        (funding_ref, "FundingScheduleV2"),
        (decision.candidate_ref, "CandidateActionV2"),
        (action_body_sizing_ref, "SizingDecisionV2"),
        (product_ref, "ProductContractV2"),
    )
    if any(not isinstance(ref, str) for ref, _kind in typed_refs):
        raise ValueError("replay omitted an exact typed input reference")
    for ref, kind in typed_refs:
        assert isinstance(ref, str)
        expected_types[ref] = kind
    scenario_ref = payoff.get("scenario_manifest_ref")
    if not isinstance(scenario_ref, str):
        raise ValueError("replay scenario manifest reference missing")
    required_refs = set(expected_types) | {scenario_ref}
    if not required_refs.issubset(set(input_refs)):
        raise ValueError("replay omitted a required typed execution/cost input")
    for ref in input_refs:
        _reference_entry(repository, ref, expected_types.get(ref), as_of_ns=now_ns)

    return path, fee, {**funding_schedule, "source_ref": funding_schedule.get("source_ref")}, input_refs


def _replay_economics(
    repository: OpsRepository,
    payoff: Mapping[str, object],
    payoff_entry: ArtifactIndexEntryV2,
    path: Mapping[str, object],
    funding_schedule: Mapping[str, object],
    now_ns: int,
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    """Sum exact recorded replay cash rows and require declared funding coverage.

    The existing outcome validator remains authoritative for payoff interpretation.
    This routine adds no execution or cost simulator.
    """

    entry_fill = payoff.get("entry")
    exits = payoff.get("exits")
    funding_rows = payoff.get("funding_cashflows")
    if not isinstance(exits, (tuple, list)) or not isinstance(funding_rows, (tuple, list)):
        raise ValueError("replay exit/funding evidence malformed")
    if entry_fill is None:
        if exits or funding_rows:
            raise ValueError("no-fill replay cannot carry fee or funding rows")
        net = _wire_decimal(payoff.get("payoff"), field="payoff")
        fill = _wire_decimal(payoff.get("filled_quantity"), field="filled_quantity")
        if net != 0 or fill != 0:
            raise ValueError("exact no-fill replay must carry zero fill and payoff")
        return Decimal(0), Decimal(0), Decimal(0), net, fill
    if (not isinstance(entry_fill, Mapping) or len(funding_rows) > _MAX_REFERENCED_EVIDENCE
            or any(not isinstance(item, Mapping) for item in exits)):
        raise ValueError("replay fill/funding evidence exceeds its deterministic bound")

    fees = Decimal(0)
    for fill_row in (entry_fill, *exits):
        if "fee" not in fill_row:
            raise ValueError("replay fill fee evidence missing")
        fee = _wire_decimal(fill_row["fee"], field="fee")
        if fee < 0:
            raise ValueError("replay fee cannot be negative")
        fees += fee

    schedule_times = funding_schedule.get("expected_settlement_times_ns")
    path_funding_refs = path.get("funding_refs")
    if (not isinstance(schedule_times, (tuple, list))
            or not isinstance(path_funding_refs, (tuple, list))
            or len(path_funding_refs) > _MAX_REFERENCED_EVIDENCE):
        raise ValueError("funding schedule or path refs malformed or over bound")
    entry_at_ns = entry_fill.get("at_ns")
    exit_times = [item.get("at_ns") for item in exits]
    if (type(entry_at_ns) is not int or not exit_times
            or any(type(value) is not int for value in exit_times)):
        raise ValueError("closed replay requires exact entry and exit chronology")
    close_at_ns = max(exit_times)
    expected_times = tuple(
        at_ns for at_ns in schedule_times
        if type(at_ns) is int and entry_at_ns <= at_ns < close_at_ns
    )
    funding_by_time: dict[int, str] = {}
    for ref in path_funding_refs:
        if not isinstance(ref, str):
            raise ValueError("replay path funding ref malformed")
        funding = _reference_entry(repository, ref, "FundingCashflowV2", as_of_ns=now_ns)
        body = json_value(funding.metadata)
        if (not isinstance(body, Mapping) or sha256_json(body) != ref
                or body.get("version") != "V2_SETTLED_FUNDING_V1"
                or type(body.get("at_ns")) is not int
                or body.get("available_at_ns") != funding.available_at_ns
                or funding.available_at_ns > payoff_entry.available_at_ns):
            raise ValueError("typed funding settlement identity invalid")
        at_ns = body["at_ns"]
        if at_ns in funding_by_time:
            raise ValueError("conflicting funding evidence at one settlement time")
        funding_by_time[at_ns] = ref

    expected_refs = {funding_by_time[at_ns] for at_ns in expected_times if at_ns in funding_by_time}
    if len(expected_refs) != len(expected_times):
        raise ValueError("required funding settlement evidence is missing")
    rows_by_ref: dict[str, Decimal] = {}
    for row in funding_rows:
        if not isinstance(row, (tuple, list)) or len(row) != 2 or not isinstance(row[0], str):
            raise ValueError("replay funding row malformed")
        if row[0] in rows_by_ref:
            raise ValueError("replay double-counts a funding observation")
        rows_by_ref[row[0]] = _wire_decimal(row[1], field="funding cashflow")
    if set(rows_by_ref) != expected_refs:
        raise ValueError("replay funding rows do not match declared settlements")

    funding_total = sum(rows_by_ref.values(), Decimal(0))
    net = _wire_decimal(payoff.get("payoff"), field="payoff")
    filled = _wire_decimal(payoff.get("filled_quantity"), field="filled_quantity")
    # This algebra only fills the existing contract's gross component. The existing
    # validator independently recomputes all components from exact replay rows.
    gross = net + fees - funding_total
    return gross, fees, funding_total, net, filled


def _resolve_diagnostic(
    repository: OpsRepository,
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    now_ns: int,
    *,
    horizon_end_ns: int,
) -> OutcomeResolutionV1:
    lookup = _lookup(
        repository,
        artifact_type="DiagnosticTargetEvidenceV2",
        metadata_path=("diagnostic", "decision_ref"),
        identity_value=decision.content_hash,
        as_of_ns=now_ns,
    )
    if lookup.problem is not None:
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code=lookup.problem)
    if not lookup.entries:
        if (decision.source_stage.value == "EXPIRY"
                and decision.admission_state == AdmissionStateV2.EXPIRED):
            return _censored_expiry(repository, decision, candidate, now_ns,
                                    horizon_end_ns=horizon_end_ns)
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="DIAGNOSTIC_TARGET_EVIDENCE_MISSING")
    if len(lookup.entries) != 1:
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="CONFLICTING_DIAGNOSTIC_EVIDENCE")
    entry = lookup.entries[0]
    try:
        payload = _typed_payload(entry, key="diagnostic")
        diagnostic = DiagnosticTargetEvidenceV2.from_dict(json_value(payload))
        if (diagnostic.content_hash != entry.artifact_ref
                or diagnostic.decision_ref != decision.content_hash
                or diagnostic.candidate_set_ref != decision.candidate_set_ref
                or diagnostic.candidate_ref != decision.candidate_ref
                or diagnostic.decision_at_ns != decision.decision_at_ns
                or diagnostic.horizon_end_ns != horizon_end_ns
                or diagnostic.available_at_ns != entry.available_at_ns
                or diagnostic.available_at_ns > now_ns):
            return _result(decision.content_hash, "UNSUPPORTED", horizon_end_ns=horizon_end_ns,
                           reason_code="DIAGNOSTIC_TARGET_IDENTITY_UNSUPPORTED")
        declaration = repository.get_artifact(diagnostic.target_declaration_ref)
        if (declaration is None or declaration.artifact_type != "DiagnosticTargetDefinitionV2"
                or declaration.content_hash != diagnostic.target_declaration_ref
                or declaration.available_at_ns > decision.decision_at_ns
                or sha256_json(declaration.metadata) != diagnostic.target_declaration_ref
                or declaration.metadata.get("label_definition") != diagnostic.label_definition
                or declaration.metadata.get("unit") != diagnostic.unit):
            return _result(decision.content_hash, "UNSUPPORTED", horizon_end_ns=horizon_end_ns,
                           reason_code="DIAGNOSTIC_TARGET_NOT_PREDECLARED")
        source_entries = tuple(repository.get_artifact(ref) for ref in diagnostic.source_refs)
        if any(source is None or source.available_at_ns > diagnostic.completed_at_ns for source in source_entries):
            return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="DIAGNOSTIC_SOURCE_INCOMPLETE")
        matured_at_ns = max(
            horizon_end_ns,
            diagnostic.completed_at_ns,
            diagnostic.available_at_ns,
            declaration.available_at_ns,
            *(source.available_at_ns for source in source_entries if source is not None),
        )
        if matured_at_ns > now_ns:
            return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="DIAGNOSTIC_EVIDENCE_NOT_YET_AVAILABLE")
        refs = (*diagnostic.source_refs, diagnostic.target_declaration_ref, entry.artifact_ref)
        outcome = _outcome_base(
            decision,
            candidate,
            action=None,
            horizon_end_ns=horizon_end_ns,
            matured_at_ns=matured_at_ns,
            available_at_ns=max(now_ns, matured_at_ns),
            execution_state=ExecutionOutcomeStateV2.NOT_APPLICABLE,
            label_state=LabelStateV2.MATURED,
            provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
            label_view="RECONSTRUCTED_MARKET",
            outcome_target=OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
            evidence_refs=refs,
            evidence_resolution="DECLARED_DIAGNOSTIC_TARGET",
            evidence_quality="CAUSAL_DIAGNOSTIC_EVIDENCE",
            label_definition=diagnostic.label_definition,
            diagnostic_value=diagnostic.value,
            diagnostic_unit=diagnostic.unit,
            diagnostic_evidence_ref=entry.artifact_ref,
        )
        outcome_contract._validate_diagnostic_target(repository, outcome)
        return _result(decision.content_hash, "MATURED", horizon_end_ns=horizon_end_ns, outcome=outcome)
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="MALFORMED_DIAGNOSTIC_EVIDENCE")


def _censored_expiry(
    repository: OpsRepository,
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    now_ns: int,
    *,
    horizon_end_ns: int,
) -> OutcomeResolutionV1:
    """Retain exact terminal expiry as a censored calendar observation."""

    try:
        indexed = repository.get_artifact(decision.source_artifact_ref)
        body = indexed.metadata.get("expiry") if indexed is not None else None
        expiry = (outcome_contract.CandidateExpiryEvidenceV2.from_dict(json_value(body))
                  if isinstance(body, Mapping) else None)
        if (indexed is None or indexed.artifact_type != "CandidateExpiryEvidenceV2"
                or indexed.content_hash != decision.source_artifact_ref or expiry is None
                or expiry.content_hash != decision.source_artifact_ref
                or indexed.available_at_ns > now_ns
                or indexed.available_at_ns != decision.available_at_ns
                or expiry.candidate_set_ref != decision.candidate_set_ref
                or expiry.candidate_ref != decision.candidate_ref
                or expiry.policy_hash != decision.policy_hash
                or expiry.deadline_ns != candidate.deadline_ns
                or expiry.expired_at_ns > horizon_end_ns):
            raise ValueError("expiry evidence identity mismatch")
        matured_at_ns = max(horizon_end_ns, indexed.available_at_ns)
        outcome = _outcome_base(
            decision,
            candidate,
            action=None,
            horizon_end_ns=horizon_end_ns,
            matured_at_ns=matured_at_ns,
            available_at_ns=max(now_ns, matured_at_ns),
            execution_state=ExecutionOutcomeStateV2.NOT_APPLICABLE,
            label_state=LabelStateV2.CENSORED,
            provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
            label_view="RECONSTRUCTED_MARKET",
            outcome_target=OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
            evidence_refs=(decision.source_artifact_ref,),
            evidence_resolution="EXACT_CANDIDATE_EXPIRY",
            evidence_quality="EXPIRY_ONLY",
            label_definition="calendar",
            reason="EXPIRED_BEFORE_FROZEN_ACTION",
        )
        return _result(decision.content_hash, "CENSORED", horizon_end_ns=horizon_end_ns,
                       reason_code="EXPIRED_BEFORE_FROZEN_ACTION", outcome=outcome)
    except (KeyError, TypeError, ValueError):
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="MALFORMED_CANDIDATE_EXPIRY_EVIDENCE")


def _resolve_actual(
    repository: OpsRepository,
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    action: _ActionIdentity,
    now_ns: int,
    *,
    horizon_end_ns: int,
) -> tuple[OutcomeResolutionV1 | None, bool]:
    lookup = _lookup(
        repository,
        artifact_type="ActualActionPositionBindingV2",
        metadata_path=("binding", "action_hash"),
        identity_value=action.action_hash,
        as_of_ns=now_ns,
    )
    if lookup.problem is not None:
        return (_result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                        reason_code=lookup.problem), True)
    if not lookup.entries:
        return None, False
    if len(lookup.entries) != 1:
        return (_result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                        reason_code="CONFLICTING_ACTUAL_BINDINGS"), True)
    if decision.admission_state not in (AdmissionStateV2.RISK_SIZED, AdmissionStateV2.CANDIDATE):
        return (_result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                        reason_code="ACTUAL_EVIDENCE_CONTRADICTS_ADMISSION"), True)
    indexed_binding = lookup.entries[0]
    try:
        binding_payload = _typed_payload(indexed_binding, key="binding")
        binding = ActualActionPositionBindingV2.from_dict(json_value(binding_payload))
        if (binding.content_hash != indexed_binding.artifact_ref
                or binding.action_hash != action.action_hash
                or binding.action_artifact_ref != action.artifact_ref
                or binding.candidate_ref != decision.candidate_ref
                or binding.candidate_set_ref != decision.candidate_set_ref
                or binding.available_at_ns != indexed_binding.available_at_ns
                or binding.available_at_ns > now_ns):
            raise ValueError("actual binding identity mismatch")
        source, actual_source = _actual_close_source(
            repository, binding.actual_closed_source_ref, now_ns,
            candidate=candidate, horizon_end_ns=horizon_end_ns,
        )
        if (actual_source.account_scope != binding.account_scope
                or actual_source.position_epoch_id != binding.position_epoch_id):
            raise ValueError("actual close is not bound to the indexed account position")
        economics = repository.get_artifact(binding.economics_observation_ref)
        link = repository.get_artifact(binding.action_position_observation_ref)
        shared = {
            "action_hash": binding.action_hash,
            "action_artifact_ref": binding.action_artifact_ref,
            "candidate_ref": binding.candidate_ref,
            "candidate_set_ref": binding.candidate_set_ref,
            "actual_closed_source_ref": binding.actual_closed_source_ref,
            "account_scope": binding.account_scope,
            "position_epoch_id": binding.position_epoch_id,
        }
        if (economics is None or link is None or economics.artifact_type != "V2ActualActionEconomicsObservationV1"
                or link.artifact_type != "V2ActualActionPositionLinkObservationV1"
                or economics.artifact_ref != binding.economics_observation_ref
                or link.artifact_ref != binding.action_position_observation_ref
                or economics.content_hash != binding.economics_observation_ref
                or link.content_hash != binding.action_position_observation_ref
                or economics.available_at_ns > binding.available_at_ns
                or link.available_at_ns > binding.available_at_ns
                or economics.available_at_ns > now_ns or link.available_at_ns > now_ns):
            raise ValueError("actual execution/economic observation unavailable")
        economics_body = json_value(economics.metadata)
        link_body = json_value(link.metadata)
        if (not isinstance(economics_body, Mapping) or not isinstance(link_body, Mapping)
                or sha256_json(economics_body) != binding.economics_observation_ref
                or sha256_json(link_body) != binding.action_position_observation_ref
                or any(economics_body.get(name) != value for name, value in shared.items())
                or any(link_body.get(name) != value for name, value in shared.items())
                or economics_body.get("source_system") != "ACCOUNT_RECONCILED_ACTION_CASH"
                or economics_body.get("economic_source_ref") != actual_source.economic_source_ref
                or link_body.get("source_system") != "VENUE_RECONCILED_ACTION_POSITION"
                or link_body.get("execution_source_ref") != actual_source.execution_source_ref):
            raise ValueError("actual action binding lacks exact immutable evidence")
        required = (
            "gross_payoff", "fees", "funding_cashflow", "net_payoff",
            "requested_quantity", "fill_quantity", "fill_status",
        )
        if any(field not in economics_body for field in required):
            raise ValueError("actual economics are incomplete")
        gross, fees, funding, net, requested, filled = (
            _wire_decimal(economics_body[field], field=field)
            for field in required[:6]
        )
        fill_status = economics_body["fill_status"]
        if not isinstance(fill_status, str):
            raise ValueError("actual fill status is malformed")
        state = ExecutionOutcomeStateV2(fill_status)
        if (fees < 0 or net != gross - fees + funding or requested != action.quantity
                or canonical_decimal_str(actual_source.realized_net_pnl) != canonical_decimal_str(net)
                or (state == ExecutionOutcomeStateV2.NO_FILL and filled != 0)
                or (state == ExecutionOutcomeStateV2.PARTIAL_FILL and not 0 < filled < requested)
                or (state == ExecutionOutcomeStateV2.FULL_FILL and filled != requested)
                or state not in (ExecutionOutcomeStateV2.NO_FILL,
                                 ExecutionOutcomeStateV2.PARTIAL_FILL,
                                 ExecutionOutcomeStateV2.FULL_FILL)):
            raise ValueError("actual payoff, quantity or fill state is contradictory")
        matured_at_ns = max(
            horizon_end_ns,
            source.available_at_ns,
            indexed_binding.available_at_ns,
            economics.available_at_ns,
            link.available_at_ns,
        )
        if matured_at_ns > now_ns:
            return (_result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                            reason_code="ACTUAL_EVIDENCE_NOT_YET_AVAILABLE"), True)
        refs = (
            binding.actual_closed_source_ref,
            indexed_binding.artifact_ref,
            binding.economics_observation_ref,
            binding.action_position_observation_ref,
        )
        outcome = _outcome_base(
            decision,
            candidate,
            action=action,
            horizon_end_ns=horizon_end_ns,
            matured_at_ns=matured_at_ns,
            available_at_ns=max(now_ns, matured_at_ns),
            execution_state=state,
            label_state=LabelStateV2.MATURED,
            provenance=OutcomeProvenanceV2.ACTUAL,
            label_view="ACTUAL_SYSTEM",
            outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE,
            evidence_refs=refs,
            evidence_resolution="ACTUAL_CLOSED_POSITION",
            evidence_quality="RECONCILED_ACTION_POSITION_AND_CASH",
            execution_evidence_ref=binding.content_hash,
            actual_closed_source_ref=binding.actual_closed_source_ref,
            actual_action_binding_ref=binding.content_hash,
            gross_payoff=gross,
            fees=fees,
            funding_cashflow=funding,
            net_payoff=net,
            fill_quantity=filled,
            requested_quantity=requested,
        )
        outcome_contract._validate_actual_binding(repository, outcome)
        return _result(decision.content_hash, "MATURED", horizon_end_ns=horizon_end_ns,
                       outcome=outcome), True
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return (_result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                        reason_code="MALFORMED_ACTUAL_EVIDENCE"), True)


def _resolve_replay(
    repository: OpsRepository,
    decision: DecisionCalendarEntryV2,
    candidate: CandidateActionV2,
    action: _ActionIdentity,
    now_ns: int,
    *,
    horizon_end_ns: int,
) -> OutcomeResolutionV1:
    lookup = _lookup(
        repository,
        artifact_type="PolicyPayoffV2",
        metadata_path=("payoff", "action_hash"),
        identity_value=action.action_hash,
        as_of_ns=now_ns,
    )
    if lookup.problem is not None:
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code=lookup.problem)
    if not lookup.entries:
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="EXECUTION_EVIDENCE_MISSING")
    if len(lookup.entries) != 1:
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="CONFLICTING_REPLAY_EVIDENCE")
    entry = lookup.entries[0]
    try:
        payoff = _typed_payload(entry, key="payoff")
        if (payoff.get("action_hash") != action.action_hash
                or payoff.get("action_artifact_ref") != action.artifact_ref
                or payoff.get("available_at_ns") != entry.available_at_ns
                or entry.available_at_ns > now_ns):
            raise ValueError("replay identity mismatch")
        payoff_status = payoff.get("status")
        if not isinstance(payoff_status, str):
            raise ValueError("replay execution status is malformed")
        state = ExecutionOutcomeStateV2(payoff_status)
        if state not in (ExecutionOutcomeStateV2.NO_FILL,
                         ExecutionOutcomeStateV2.PARTIAL_FILL,
                         ExecutionOutcomeStateV2.FULL_FILL):
            return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="REPLAY_NOT_ESTIMABLE")
        fill = _wire_decimal(payoff.get("filled_quantity"), field="filled_quantity")
        remaining = _wire_decimal(payoff.get("remaining_quantity"), field="remaining_quantity")
        if remaining != 0:
            raise ValueError("replay leaves residual quantity")
        if (state == ExecutionOutcomeStateV2.NO_FILL and fill != 0
                or state == ExecutionOutcomeStateV2.PARTIAL_FILL and not 0 < fill < action.quantity
                or state == ExecutionOutcomeStateV2.FULL_FILL and fill != action.quantity):
            raise ValueError("replay fill quantity disagrees with terminal state")
        path, fee, funding_schedule, replay_refs = _validate_replay_support(
            repository, decision, candidate, action, payoff, entry, now_ns
        )
        gross, fees, funding, net, supported_fill = _replay_economics(
            repository, payoff, entry, path, funding_schedule, now_ns
        )
        if supported_fill != fill:
            raise ValueError("replay filled quantity changed during resolution")
        refs = tuple(sorted({entry.artifact_ref, *replay_refs}))
        reference_entries = tuple(
            _reference_entry(repository, ref, None, as_of_ns=now_ns) for ref in refs
        )
        matured_at_ns = max(horizon_end_ns, *(item.available_at_ns for item in reference_entries))
        if matured_at_ns > now_ns:
            return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="REPLAY_EVIDENCE_NOT_YET_AVAILABLE")
        admission_provenance = (
            OutcomeProvenanceV2.COUNTERFACTUAL
            if decision.admission_state in (AdmissionStateV2.NO_TRADE,
                                            AdmissionStateV2.NOT_ESTIMABLE)
            else OutcomeProvenanceV2.SIMULATED
        )
        outcome = _outcome_base(
            decision,
            candidate,
            action=action,
            horizon_end_ns=horizon_end_ns,
            matured_at_ns=matured_at_ns,
            available_at_ns=max(now_ns, matured_at_ns),
            execution_state=state,
            label_state=LabelStateV2.MATURED,
            provenance=admission_provenance,
            label_view="RECONSTRUCTED_MARKET",
            outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE,
            evidence_refs=refs,
            evidence_resolution="POLICY_PAYOFF_REPLAY",
            evidence_quality="REPLAY_BOUND",
            execution_evidence_ref=entry.artifact_ref,
            gross_payoff=gross,
            fees=fees,
            funding_cashflow=funding,
            net_payoff=net,
            fill_quantity=fill,
            requested_quantity=action.quantity,
        )
        outcome_contract._validate_policy_payoff(repository, outcome, action.identity)
        return _result(decision.content_hash, "MATURED", horizon_end_ns=horizon_end_ns,
                       outcome=outcome)
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return _result(decision.content_hash, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="MALFORMED_REPLAY_EVIDENCE")


def resolve_decision_outcome(
    repository: OpsRepository,
    calendar_entry: ArtifactIndexEntryV2,
    now_ns: int,
) -> OutcomeResolutionV1:
    """Resolve one immutable indexed calendar entry without persisting or guessing.

    A matured outcome is returned only after its exact horizon has elapsed and one
    complete, unambiguous existing evidence chain validates. Missing evidence never
    becomes a no-fill or zero-cost result. Intermediate states carry no label because
    the repository has no accepted outcome supersession semantics.
    """

    timestamp(now_ns, field="now_ns")
    decision_ref = calendar_entry.artifact_ref
    try:
        decision = _decision(repository, calendar_entry)
    except (KeyError, TypeError, ValueError):
        return _result(decision_ref, "UNSUPPORTED", reason_code="INVALID_DECISION_CALENDAR_EVIDENCE")
    if decision.candidate_ref is None:
        return _result(decision_ref, "UNSUPPORTED", reason_code="NO_DECLARED_CANDIDATE_HORIZON")
    try:
        candidate = _candidate(repository, decision)
    except (KeyError, TypeError, ValueError):
        return _result(decision_ref, "UNSUPPORTED", reason_code="INVALID_CANDIDATE_HORIZON_EVIDENCE")
    horizon_end_ns = candidate.horizon_end_ns
    if now_ns < horizon_end_ns:
        return _result(decision_ref, "PENDING", horizon_end_ns=horizon_end_ns,
                       reason_code="DECLARED_HORIZON_NOT_REACHED")

    action: _ActionIdentity | None = None
    if decision.action_hash is not None:
        try:
            action = _action(repository, decision, candidate)
        except (KeyError, TypeError, ValueError):
            return _result(decision_ref, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="FROZEN_ACTION_EVIDENCE_INVALID")
    elif decision.action_artifact_ref is not None:
        return _result(decision_ref, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                       reason_code="FROZEN_ACTION_IDENTITY_INCOMPLETE")

    if action is None:
        return _resolve_diagnostic(
            repository, decision, candidate, now_ns, horizon_end_ns=horizon_end_ns
        )

    actual, actual_evidence_exists = _resolve_actual(
        repository, decision, candidate, action, now_ns, horizon_end_ns=horizon_end_ns
    )
    if actual is not None and actual.status != "MATURED":
        return actual
    replay = _resolve_replay(
        repository, decision, candidate, action, now_ns, horizon_end_ns=horizon_end_ns
    )
    if actual_evidence_exists:
        if actual is None or actual.status != "MATURED":
            return actual or _result(decision_ref, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                                     reason_code="ACTUAL_EVIDENCE_UNRESOLVED")
        if replay.status == "MATURED":
            return _result(decision_ref, "UNRESOLVED", horizon_end_ns=horizon_end_ns,
                           reason_code="ACTUAL_AND_REPLAY_PROVENANCE_CONFLICT")
        return actual
    return replay
