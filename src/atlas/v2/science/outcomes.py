"""Immutable matured labels for chronological research; no capital authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import (
    canonical_json,
    decimal_value,
    json_value,
    nonblank,
    sha256_json,
    sha256_ref,
    strict_fields,
    timestamp,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import ActualClosedPositionSourceV2

VERSION = "MATURED_OUTCOME_V2_V2"


class OutcomeProvenanceV2(StrEnum):
    ACTUAL = "ACTUAL"
    SIMULATED = "SIMULATED"
    COUNTERFACTUAL = "COUNTERFACTUAL"


class CalendarStateV2(StrEnum):
    SELECTED = "SELECTED"
    UNSELECTED = "UNSELECTED"
    REJECTED = "REJECTED"
    NO_CANDIDATE = "NO_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    EXPIRED = "EXPIRED"
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"


class LabelStateV2(StrEnum):
    MATURED = "MATURED"
    UNRESOLVED = "UNRESOLVED"
    CENSORED = "CENSORED"


class OutcomeTargetV2(StrEnum):
    EXECUTABLE_ACTION_VALUE = "EXECUTABLE_ACTION_VALUE"
    NON_EXECUTABLE_DIAGNOSTIC = "NON_EXECUTABLE_DIAGNOSTIC"


@dataclass(frozen=True)
class ActualActionPositionBindingV2:
    """A reconciled action-to-position link; its observation must be independently indexed."""

    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    actual_closed_source_ref: str
    account_scope: str
    position_epoch_id: str
    action_position_observation_ref: str
    economics_observation_ref: str
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
                     "actual_closed_source_ref", "action_position_observation_ref",
                     "economics_observation_ref"):
            sha256_ref(getattr(self, name), field=name)
        nonblank(self.account_scope, field="account_scope")
        nonblank(self.position_epoch_id, field="position_epoch_id")
        timestamp(self.available_at_ns, field="available_at_ns")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ACTUAL_ACTION_POSITION_BINDING_V2_V1",
                **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActualActionPositionBindingV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="ActualActionPositionBindingV2")
        if d["version"] != "ACTUAL_ACTION_POSITION_BINDING_V2_V1":
            raise ValueError("unsupported actual binding version")
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class DiagnosticTargetEvidenceV2:
    decision_ref: str
    candidate_set_ref: str
    candidate_ref: str
    label_definition: str
    target_declaration_ref: str
    decision_at_ns: int
    horizon_end_ns: int
    value: Decimal
    unit: str
    source_refs: tuple[str, ...]
    completed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("decision_ref", "candidate_set_ref", "candidate_ref", "target_declaration_ref"):
            sha256_ref(getattr(self, name), field=name)
        nonblank(self.label_definition, field="label_definition")
        nonblank(self.unit, field="unit")
        object.__setattr__(self, "value", decimal_value(self.value, field="diagnostic value"))
        for name in ("decision_at_ns", "horizon_end_ns", "completed_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.decision_at_ns < self.horizon_end_ns <= self.completed_at_ns <= self.available_at_ns:
            raise ValueError("diagnostic target chronology invalid")
        if not self.source_refs or self.source_refs != tuple(sorted(set(self.source_refs))):
            raise ValueError("diagnostic source refs required, sorted and unique")
        for ref in self.source_refs:
            sha256_ref(ref, field="diagnostic source ref")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "DIAGNOSTIC_TARGET_EVIDENCE_V2_V1",
                           **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticTargetEvidenceV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="DiagnosticTargetEvidenceV2")
        if d["version"] != "DIAGNOSTIC_TARGET_EVIDENCE_V2_V1" or not isinstance(d["source_refs"], list):
            raise ValueError("unsupported diagnostic evidence wire")
        return cls(d["decision_ref"], d["candidate_set_ref"], d["candidate_ref"], d["label_definition"],
                   d["target_declaration_ref"], d["decision_at_ns"], d["horizon_end_ns"],
                   decimal_value(d["value"], field="diagnostic value", wire=True), d["unit"],
                   tuple(d["source_refs"]), d["completed_at_ns"], d["available_at_ns"])


@dataclass(frozen=True)
class MaturedOutcomeV2:
    decision_ref: str
    candidate_set_ref: str
    candidate_ref: str | None
    policy_id: str
    policy_version: str
    policy_hash: str
    action_hash: str | None
    action_artifact_ref: str | None
    action_absence_reason: str | None
    instrument_revision: str | None
    venue: str | None
    product: str | None
    decision_at_ns: int
    horizon_end_ns: int
    matured_at_ns: int
    available_at_ns: int
    label_definition: str
    label_view: str
    calendar_state: CalendarStateV2
    label_state: LabelStateV2
    provenance: OutcomeProvenanceV2
    payoff_unit: str
    quantity_unit: str
    gross_payoff: Decimal | None
    fees: Decimal | None
    funding_cashflow: Decimal | None
    net_payoff: Decimal | None
    fill_quantity: Decimal | None
    requested_quantity: Decimal | None
    mfe: Decimal | None
    mae: Decimal | None
    evidence_refs: tuple[str, ...]
    execution_evidence_ref: str | None
    extrema_evidence_ref: str | None
    actual_closed_source_ref: str | None
    evidence_resolution: str
    evidence_quality: str
    ambiguity: tuple[str, ...]
    outcome_target: OutcomeTargetV2
    diagnostic_value: Decimal | None
    diagnostic_unit: str | None
    diagnostic_evidence_ref: str | None
    actual_action_binding_ref: str | None
    reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("decision_ref", "candidate_set_ref", "policy_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("candidate_ref", "action_hash", "action_artifact_ref", "instrument_revision", "execution_evidence_ref",
                     "extrema_evidence_ref", "actual_closed_source_ref", "actual_action_binding_ref",
                     "diagnostic_evidence_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        for name in ("policy_id", "policy_version", "label_definition", "label_view", "evidence_resolution", "evidence_quality"):
            nonblank(getattr(self, name), field=name)
        if self.label_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unsupported label view")
        if self.payoff_unit != "USDT" or self.quantity_unit != "CONTRACTS":
            raise ValueError("unsupported payoff/quantity units for V2 linear perpetual label")
        for name in ("decision_at_ns", "horizon_end_ns", "matured_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.decision_at_ns < self.horizon_end_ns <= self.matured_at_ns <= self.available_at_ns:
            raise ValueError("outcome chronology invalid")
        for name, enum in (("calendar_state", CalendarStateV2), ("label_state", LabelStateV2), ("provenance", OutcomeProvenanceV2)):
            object.__setattr__(self, name, enum(getattr(self, name)))
        object.__setattr__(self, "outcome_target", OutcomeTargetV2(self.outcome_target))
        if self.candidate_ref is None:
            if self.calendar_state != CalendarStateV2.NO_CANDIDATE or self.action_hash is not None:
                raise ValueError("only NO_CANDIDATE may lack candidate identity")
        elif self.calendar_state == CalendarStateV2.NO_CANDIDATE:
            raise ValueError("NO_CANDIDATE cannot claim a candidate")
        if self.action_hash is None:
            if self.action_artifact_ref is not None:
                raise ValueError("absent action cannot claim action artifact")
            nonblank(self.action_absence_reason or "", field="action_absence_reason")
        elif self.action_absence_reason is not None or self.candidate_ref is None or self.action_artifact_ref is None:
            raise ValueError("frozen action identity and absence reason conflict")
        if self.calendar_state in (CalendarStateV2.UNSELECTED, CalendarStateV2.REJECTED, CalendarStateV2.NO_CANDIDATE) and self.action_hash is not None:
            raise ValueError("unselected/rejected/no-candidate cannot invent a frozen action")
        if self.instrument_revision is None:
            if self.candidate_ref is not None or self.venue is not None or self.product is not None:
                raise ValueError("candidate instrument identity incomplete")
        elif not self.venue or not self.product:
            raise ValueError("venue and product required with instrument revision")
        for name in ("gross_payoff", "fees", "funding_cashflow", "net_payoff", "fill_quantity", "requested_quantity", "mfe", "mae", "diagnostic_value"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        if self.outcome_target == OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC:
            if self.action_hash is not None or self.execution_evidence_ref is not None or self.actual_closed_source_ref is not None:
                raise ValueError("diagnostic target cannot claim executable action or close")
            if any(getattr(self, name) is not None for name in (
                    "gross_payoff", "fees", "funding_cashflow", "net_payoff", "fill_quantity",
                    "requested_quantity", "mfe", "mae", "extrema_evidence_ref", "actual_action_binding_ref")):
                raise ValueError("diagnostic target cannot claim action payoff, fill or extrema")
            if self.label_state == LabelStateV2.MATURED:
                if self.diagnostic_value is None or not self.diagnostic_unit or self.diagnostic_evidence_ref is None:
                    raise ValueError("matured diagnostic requires measured target, unit and evidence")
            elif self.diagnostic_value is not None or self.diagnostic_evidence_ref is not None:
                raise ValueError("unresolved diagnostic cannot claim measured value")
        elif self.diagnostic_value is not None or self.diagnostic_unit is not None or self.diagnostic_evidence_ref is not None:
            raise ValueError("executable target cannot carry diagnostic value")
        if self.label_state == LabelStateV2.MATURED and self.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE:
            if self.calendar_state not in (CalendarStateV2.NO_FILL, CalendarStateV2.PARTIAL_FILL,
                                           CalendarStateV2.FULL_FILL):
                raise ValueError("matured executable action value requires terminal fill classification")
            if None in (self.gross_payoff, self.fees, self.funding_cashflow, self.net_payoff):
                raise ValueError("matured monetary label requires complete components")
            assert self.gross_payoff is not None and self.fees is not None and self.funding_cashflow is not None and self.net_payoff is not None
            if self.fees < 0 or self.net_payoff != self.gross_payoff - self.fees + self.funding_cashflow:
                raise ValueError("net payoff must count fees and funding once")
            if self.action_hash is None:
                raise ValueError("executable monetary label requires exact frozen action")
        elif any(x is not None for x in (self.gross_payoff, self.fees, self.funding_cashflow, self.net_payoff, self.fill_quantity, self.mfe, self.mae)):
            raise ValueError("unresolved/censored label cannot assert measured payoff or fill")
        if self.requested_quantity is not None and self.requested_quantity <= 0:
            raise ValueError("requested quantity must be positive")
        if (self.mfe is None) != (self.mae is None):
            raise ValueError("MFE and MAE must be measured together")
        if self.mfe is not None:
            if self.extrema_evidence_ref is None or self.mfe < 0 or self.mae is None or self.mae > 0:
                raise ValueError("extrema require signed, referenced measured evidence")
        elif self.extrema_evidence_ref is not None:
            raise ValueError("extrema evidence cannot assert unmeasured extrema")
        if self.fill_quantity is not None:
            if self.fill_quantity < 0 or self.requested_quantity is None or self.fill_quantity > self.requested_quantity:
                raise ValueError("fill quantity invalid")
            if self.calendar_state == CalendarStateV2.NO_FILL and self.fill_quantity != 0:
                raise ValueError("no-fill quantity must be zero")
            if self.calendar_state == CalendarStateV2.PARTIAL_FILL and not 0 < self.fill_quantity < self.requested_quantity:
                raise ValueError("partial-fill quantity invalid")
            if self.fill_quantity > 0 and self.execution_evidence_ref is None:
                raise ValueError("positive fill requires execution evidence")
            if self.fill_quantity > 0 and self.action_hash is None:
                raise ValueError("candidate without frozen action cannot claim executable fill")
        if self.label_state == LabelStateV2.MATURED and self.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE and self.calendar_state in (
                CalendarStateV2.NO_FILL, CalendarStateV2.PARTIAL_FILL, CalendarStateV2.FULL_FILL):
            if self.execution_evidence_ref is None or self.fill_quantity is None or self.requested_quantity is None:
                raise ValueError("matured fill state requires measured execution evidence")
            if self.calendar_state == CalendarStateV2.FULL_FILL and self.fill_quantity != self.requested_quantity:
                raise ValueError("full-fill quantity must equal requested quantity")
        if self.provenance == OutcomeProvenanceV2.ACTUAL:
            if self.label_view != "ACTUAL_SYSTEM":
                raise ValueError("ACTUAL outcome requires ACTUAL_SYSTEM view")
            if self.label_state == LabelStateV2.MATURED and (
                    self.actual_closed_source_ref is None or self.actual_action_binding_ref is None
                    or self.action_hash is None or self.outcome_target != OutcomeTargetV2.EXECUTABLE_ACTION_VALUE):
                raise ValueError("matured ACTUAL requires exact action/position binding")
        elif self.actual_closed_source_ref is not None or self.actual_action_binding_ref is not None or self.label_view != "RECONSTRUCTED_MARKET":
            raise ValueError("research labels cannot claim actual-system view/source")
        if self.provenance == OutcomeProvenanceV2.COUNTERFACTUAL and self.fill_quantity is not None and self.fill_quantity > 0 and self.execution_evidence_ref is None:
            raise ValueError("counterfactual market path does not prove execution")
        refs = tuple(self.evidence_refs)
        if refs != tuple(sorted(set(refs))):
            raise ValueError("evidence refs must be sorted and unique")
        for ref in refs:
            sha256_ref(ref, field="evidence_ref")
        if self.execution_evidence_ref is not None and self.execution_evidence_ref not in refs:
            raise ValueError("execution evidence must be in evidence_refs")
        if self.extrema_evidence_ref is not None and self.extrema_evidence_ref not in refs:
            raise ValueError("extrema evidence must be in evidence_refs")
        if self.actual_closed_source_ref is not None and self.actual_closed_source_ref not in refs:
            raise ValueError("actual source must be in evidence_refs")
        if self.actual_action_binding_ref is not None and self.actual_action_binding_ref not in refs:
            raise ValueError("actual binding must be in evidence_refs")
        if self.diagnostic_evidence_ref is not None and self.diagnostic_evidence_ref not in refs:
            raise ValueError("diagnostic target must be in evidence_refs")
        if self.label_state != LabelStateV2.MATURED:
            nonblank(self.reason or "", field="reason")

    def _body(self) -> dict[str, Any]:
        from atlas.v2._serialization import json_value
        return json_value({"version": VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MaturedOutcomeV2:
        fields = set(cls.__dataclass_fields__) | {"version", "content_hash"}
        d = strict_fields(data, expected=fields, required=fields, name="MaturedOutcomeV2")
        if d["version"] != VERSION:
            raise ValueError("unsupported MaturedOutcomeV2 version")
        if not isinstance(d["evidence_refs"], list) or not isinstance(d["ambiguity"], list):
            raise ValueError("refs and ambiguity must be arrays")
        values = {name: d[name] for name in cls.__dataclass_fields__}
        for name in ("gross_payoff", "fees", "funding_cashflow", "net_payoff", "fill_quantity",
                     "requested_quantity", "mfe", "mae", "diagnostic_value"):
            if values[name] is not None:
                values[name] = decimal_value(values[name], field=name, wire=True)
        values["evidence_refs"] = tuple(values["evidence_refs"])
        values["ambiguity"] = tuple(values["ambiguity"])
        result = cls(**values)
        if result.content_hash != d["content_hash"]:
            raise ValueError("MaturedOutcomeV2 content_hash mismatch")
        return result


def _decimal_wire(value: Any, field: str) -> Decimal:
    return decimal_value(value, field=field, wire=True)


def _validate_policy_payoff(repo: OpsRepository, outcome: MaturedOutcomeV2, identity: Mapping[str, Any]) -> None:
    ref = outcome.execution_evidence_ref
    entry = repo.get_artifact(ref or "")
    body = entry.metadata.get("payoff") if entry is not None else None
    if entry is None or entry.artifact_type != "PolicyPayoffV2" or not isinstance(body, Mapping):
        raise ValueError("matured research action value requires typed PolicyPayoffV2")
    required = {"version", "action_hash", "action_artifact_ref", "path_ref", "common_path_id",
        "scenario_manifest_ref", "existing_portfolio_ref", "replay_assumptions_ref", "fee_ref",
        "funding_schedule_ref", "entry", "exits", "funding_cashflows", "filled_quantity",
        "remaining_quantity", "payoff", "status", "exit_reason", "portfolio_horizon_end_ns",
        "available_at_ns", "reasons"}
    strict_fields(body, expected=required, required=required, name="PolicyPayoffV2")
    if (sha256_json(body) != ref or body["version"] != "S1_S2_EXECUTION_REPLAY_V1"
            or body["action_hash"] != outcome.action_hash
            or body["action_artifact_ref"] != outcome.action_artifact_ref
            or body["status"] != outcome.calendar_state.value
            or body["status"] not in ("NO_FILL", "PARTIAL_FILL", "FULL_FILL")
            or body["reasons"] or body["available_at_ns"] != entry.available_at_ns
            or body["available_at_ns"] > outcome.matured_at_ns
            or body["portfolio_horizon_end_ns"] < outcome.horizon_end_ns):
        raise ValueError("payoff identity/status/chronology mismatch")
    path_ref = body["path_ref"]
    path = repo.get_artifact(path_ref)
    if (path is None or path.artifact_type != "ReplayPathV2"
            or path.available_at_ns > entry.available_at_ns
            or not isinstance(path.metadata.get("path"), Mapping)
            or sha256_json(path.metadata["path"]) != path_ref
            or path.metadata["path"].get("common_path_id") != body["common_path_id"]
            or path.metadata["path"].get("scenario_manifest_ref") != body["scenario_manifest_ref"]):
        raise ValueError("payoff retrospective replay path mismatch")
    refs = entry.metadata.get("input_refs")
    if not isinstance(refs, (list, tuple)) or not {outcome.action_artifact_ref, path_ref}.issubset(set(refs)):
        raise ValueError("payoff action/path input refs absent")
    for input_ref in refs:
        input_entry = repo.get_artifact(input_ref)
        if input_entry is None or input_entry.available_at_ns > entry.available_at_ns:
            raise ValueError("payoff input unavailable by replay")
    requested = _decimal_wire(identity.get("quantity"), "action quantity")
    filled = _decimal_wire(body["filled_quantity"], "filled_quantity")
    if (requested != outcome.requested_quantity or filled != outcome.fill_quantity
            or _decimal_wire(body["remaining_quantity"], "remaining_quantity") != 0
            or _decimal_wire(body["payoff"], "payoff") != outcome.net_payoff):
        raise ValueError("payoff quantity, residual or net mismatch")
    replay_entry = body["entry"]
    exits = body["exits"]
    funding_rows = body["funding_cashflows"]
    if not isinstance(exits, (list, tuple)) or not isinstance(funding_rows, (list, tuple)):
        raise ValueError("payoff fills/funding malformed")
    candidate = repo.get_artifact(outcome.candidate_ref or "")
    candidate_body = candidate.metadata.get("candidate") if candidate is not None else None
    if not isinstance(candidate_body, Mapping):
        raise ValueError("payoff candidate unavailable")
    fill_fields = {"at_ns", "quantity", "price", "fee", "reason"}
    for fill in ((replay_entry,) if replay_entry is not None else ()) + tuple(exits):
        strict_fields(fill, expected=fill_fields, required=fill_fields, name="ReplayFillV2")
        if (_decimal_wire(fill["quantity"], "fill quantity") <= 0
                or _decimal_wire(fill["price"], "fill price") <= 0
                or _decimal_wire(fill["fee"], "fill fee") < 0
                or not outcome.decision_at_ns <= fill["at_ns"] <= entry.available_at_ns):
            raise ValueError("replay fill quantity/price/fee/time invalid")
        nonblank(fill["reason"], field="replay fill reason")
    for funding_row in funding_rows:
        if (not isinstance(funding_row, (list, tuple)) or len(funding_row) != 2
                or funding_row[0] not in path.metadata["path"].get("funding_refs", ())):
            raise ValueError("replay funding source mismatch")
        _decimal_wire(funding_row[1], "funding cashflow")
    if replay_entry is None:
        if filled != 0 or exits or funding_rows or body["status"] != "NO_FILL":
            raise ValueError("no-fill replay disagrees with outcome")
        gross = fees = funding = Decimal(0)
    else:
        if (not isinstance(replay_entry, Mapping) or body["status"] == "NO_FILL"
                or replay_entry["at_ns"] > candidate_body.get("deadline_ns", -1)
                or any(x["at_ns"] < replay_entry["at_ns"] for x in exits)):
            raise ValueError("entry fill/status mismatch")
        entry_qty = _decimal_wire(replay_entry.get("quantity"), "entry quantity")
        if entry_qty != filled or sum((_decimal_wire(x.get("quantity"), "exit quantity") for x in exits), Decimal(0)) != filled:
            raise ValueError("closed replay fill quantity mismatch")
        product_ref = identity.get("product_ref")
        product = repo.get_artifact(product_ref) if isinstance(product_ref, str) else None
        product_body = product.metadata.get("product") if product is not None else None
        if not isinstance(product_body, Mapping):
            raise ValueError("indexed product multiplier required for exact payoff components")
        multiplier = _decimal_wire(product_body.get("base_units_per_contract"), "base_units_per_contract")
        direction = Decimal(1) if identity.get("side") == "LONG" else Decimal(-1) if identity.get("side") == "SHORT" else None
        if direction is None:
            raise ValueError("frozen action side invalid")
        gross = direction * multiplier * (
            sum((_decimal_wire(x.get("quantity"), "exit quantity") * _decimal_wire(x.get("price"), "exit price")
                 for x in exits), Decimal(0))
            - filled * _decimal_wire(replay_entry.get("price"), "entry price"))
        fees = _decimal_wire(replay_entry.get("fee"), "entry fee") + sum(
            (_decimal_wire(x.get("fee"), "exit fee") for x in exits), Decimal(0))
        funding = sum((_decimal_wire(row[1], "funding cashflow") for row in funding_rows), Decimal(0))
    if (outcome.gross_payoff != gross or outcome.fees != fees or outcome.funding_cashflow != funding
            or outcome.net_payoff != gross - fees + funding):
        raise ValueError("payoff components disagree with typed replay")


def index_actual_action_position_binding(repo: OpsRepository, binding: ActualActionPositionBindingV2) -> str:
    action = repo.get_artifact(binding.action_artifact_ref)
    action_body = action.metadata.get("action_artifact") if action is not None else None
    action_identity = action.metadata.get("action_identity") if action is not None else None
    source = repo.get_artifact(binding.actual_closed_source_ref)
    link = repo.get_artifact(binding.action_position_observation_ref)
    economics = repo.get_artifact(binding.economics_observation_ref)
    expected = {"action_hash": binding.action_hash, "action_artifact_ref": binding.action_artifact_ref,
        "candidate_ref": binding.candidate_ref, "candidate_set_ref": binding.candidate_set_ref,
        "actual_closed_source_ref": binding.actual_closed_source_ref, "account_scope": binding.account_scope,
        "position_epoch_id": binding.position_epoch_id}
    link_expected = {**expected, "source_system": "VENUE_RECONCILED_ACTION_POSITION",
                     "execution_source_ref": source.metadata.get("execution_source_ref") if source else None}
    economics_expected = {**expected, "source_system": "ACCOUNT_RECONCILED_ACTION_CASH",
                          "economic_source_ref": source.metadata.get("economic_source_ref") if source else None}
    if (action is None or action.artifact_type != "ActionArtifactV2" or not isinstance(action_body, Mapping)
            or not isinstance(action_identity, Mapping)
            or sha256_json(action_body) != binding.action_artifact_ref
            or sha256_json(action_identity) != binding.action_hash
            or any(action_body.get(k) != expected[k] for k in ("action_hash", "candidate_ref", "candidate_set_ref"))
            or source is None or source.artifact_type != "ActualClosedPositionSourceV2"
            or source.metadata.get("account_scope") != binding.account_scope
            or source.metadata.get("position_epoch_id") != binding.position_epoch_id
            or sha256_json(source.metadata) != binding.actual_closed_source_ref
            or link is None or link.artifact_type != "V2ActualActionPositionLinkObservationV1"
            or economics is None or economics.artifact_type != "V2ActualActionEconomicsObservationV1"
            or any(item.available_at_ns > binding.available_at_ns for item in (action, source, link, economics))
            or any(not isinstance(item.metadata, Mapping) or sha256_json(item.metadata) != item.content_hash
                   or any(item.metadata.get(k) != v for k, v in required.items())
                   for item, required in ((link, link_expected), (economics, economics_expected)))):
        raise ValueError("actual action/position binding lacks exact reconciled observations")
    repo.register_artifact(ArtifactIndexEntryV2(binding.content_hash, "ActualActionPositionBindingV2",
        binding.content_hash, binding.available_at_ns, binding.available_at_ns, {"binding": binding.to_dict()}))
    return binding.content_hash


def _validate_actual_binding(repo: OpsRepository, outcome: MaturedOutcomeV2) -> None:
    entry = repo.get_artifact(outcome.actual_action_binding_ref or "")
    body = entry.metadata.get("binding") if entry is not None else None
    if entry is None or entry.artifact_type != "ActualActionPositionBindingV2" or not isinstance(body, Mapping):
        raise ValueError("typed actual action/position binding required")
    binding = ActualActionPositionBindingV2.from_dict(body)
    if (binding.content_hash != entry.content_hash or entry.available_at_ns > outcome.matured_at_ns
            or binding.action_hash != outcome.action_hash or binding.action_artifact_ref != outcome.action_artifact_ref
            or binding.candidate_ref != outcome.candidate_ref or binding.candidate_set_ref != outcome.candidate_set_ref
            or binding.actual_closed_source_ref != outcome.actual_closed_source_ref
            or outcome.execution_evidence_ref != binding.content_hash):
        raise ValueError("actual binding does not match exact action")
    economics = repo.get_artifact(binding.economics_observation_ref)
    if economics is None or economics.available_at_ns > outcome.matured_at_ns:
        raise ValueError("actual action economics unavailable")
    for field, value in (("gross_payoff", outcome.gross_payoff), ("fees", outcome.fees),
                         ("funding_cashflow", outcome.funding_cashflow), ("net_payoff", outcome.net_payoff),
                         ("requested_quantity", outcome.requested_quantity), ("fill_quantity", outcome.fill_quantity)):
        if _decimal_wire(economics.metadata.get(field), field) != value:
            raise ValueError("actual economics components disagree with outcome")
    if economics.metadata.get("fill_status") != outcome.calendar_state.value:
        raise ValueError("actual fill status mismatch")


def index_diagnostic_target_evidence(repo: OpsRepository, item: DiagnosticTargetEvidenceV2) -> str:
    declaration = repo.get_artifact(item.target_declaration_ref)
    if (declaration is None or declaration.artifact_type != "DiagnosticTargetDefinitionV2"
            or declaration.available_at_ns > item.decision_at_ns
            or sha256_json(declaration.metadata) != item.target_declaration_ref
            or declaration.metadata.get("label_definition") != item.label_definition
            or declaration.metadata.get("unit") != item.unit):
        raise ValueError("diagnostic target was not predeclared by decision")
    for ref in item.source_refs:
        source = repo.get_artifact(ref)
        if source is None or source.available_at_ns > item.completed_at_ns:
            raise ValueError("diagnostic measured source unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, "DiagnosticTargetEvidenceV2",
        item.content_hash, item.completed_at_ns, item.available_at_ns, {"diagnostic": item.to_dict()}))
    return item.content_hash


def _validate_diagnostic_target(repo: OpsRepository, outcome: MaturedOutcomeV2) -> None:
    entry = repo.get_artifact(outcome.diagnostic_evidence_ref or "")
    body = entry.metadata.get("diagnostic") if entry is not None else None
    if entry is None or entry.artifact_type != "DiagnosticTargetEvidenceV2" or not isinstance(body, Mapping):
        raise ValueError("matured diagnostic requires typed measured evidence")
    item = DiagnosticTargetEvidenceV2.from_dict(json_value(body))
    if (item.content_hash != outcome.diagnostic_evidence_ref or item.decision_ref != outcome.decision_ref
            or item.candidate_set_ref != outcome.candidate_set_ref or item.candidate_ref != outcome.candidate_ref
            or item.label_definition != outcome.label_definition or item.decision_at_ns != outcome.decision_at_ns
            or item.horizon_end_ns != outcome.horizon_end_ns or item.value != outcome.diagnostic_value
            or item.unit != outcome.diagnostic_unit or item.completed_at_ns > outcome.matured_at_ns
            or item.available_at_ns > outcome.matured_at_ns or entry.available_at_ns != item.available_at_ns):
        raise ValueError("diagnostic target identity, value or chronology mismatch")
    declaration = repo.get_artifact(item.target_declaration_ref)
    if (declaration is None or declaration.available_at_ns > outcome.decision_at_ns
            or sha256_json(declaration.metadata) != item.target_declaration_ref
            or declaration.metadata.get("label_definition") != item.label_definition
            or declaration.metadata.get("unit") != item.unit):
        raise ValueError("diagnostic target not predeclared")
    for ref in item.source_refs:
        source = repo.get_artifact(ref)
        if source is None or source.available_at_ns > item.completed_at_ns:
            raise ValueError("diagnostic source unavailable")


def index_matured_outcome(repo: OpsRepository, outcome: MaturedOutcomeV2) -> str:
    """Index an available label; callers retain its immutable payload separately."""
    candidate_set = repo.get_artifact(outcome.candidate_set_ref)
    set_body = candidate_set.metadata.get("candidate_set") if candidate_set is not None else None
    set_identity = candidate_set.metadata.get("identity") if candidate_set is not None else None
    if (candidate_set is None or candidate_set.artifact_type != "CandidateSetV2"
            or not isinstance(set_body, Mapping) or not isinstance(set_identity, Mapping)
            or outcome.decision_at_ns != set_identity.get("cutoff_ns")
            or (outcome.candidate_ref is not None
                and outcome.candidate_ref not in set_identity.get("candidate_refs", ()))):
        raise ValueError("outcome decision/candidate must belong to exact CandidateSet")
    if outcome.candidate_ref is not None:
        candidate = repo.get_artifact(outcome.candidate_ref)
        candidate_body = candidate.metadata.get("candidate") if candidate is not None else None
        if (candidate is None or candidate.artifact_type != "CandidateActionV2"
                or not isinstance(candidate_body, Mapping)
                or candidate_body.get("policy_hash") != outcome.policy_hash
                or candidate_body.get("decision_at_ns") != outcome.decision_at_ns):
            raise ValueError("outcome candidate identity mismatch")
    if outcome.action_artifact_ref is not None:
        action = repo.get_artifact(outcome.action_artifact_ref)
        artifact = action.metadata.get("action_artifact") if action is not None else None
        identity = action.metadata.get("action_identity") if action is not None else None
        key = identity.get("key") if isinstance(identity, Mapping) else None
        if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > outcome.horizon_end_ns
                or not isinstance(artifact, Mapping) or not isinstance(identity, Mapping) or not isinstance(key, Mapping)
                or sha256_json(artifact) != outcome.action_artifact_ref
                or sha256_json(identity) != outcome.action_hash
                or artifact.get("action_hash") != outcome.action_hash
                or artifact.get("candidate_ref") != outcome.candidate_ref
                or artifact.get("candidate_set_ref") != outcome.candidate_set_ref
                or identity.get("policy_id") != outcome.policy_id
                or identity.get("policy_version") != outcome.policy_version
                or identity.get("policy_hash") != outcome.policy_hash
                or key.get("contract_revision") != outcome.instrument_revision
                or key.get("venue") != outcome.venue or key.get("product") != outcome.product):
            raise ValueError("matured outcome exact frozen action identity mismatch")
    for ref in outcome.evidence_refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.available_at_ns > outcome.matured_at_ns:
            raise ValueError("outcome maturity cannot precede required evidence availability")
    if outcome.label_state == LabelStateV2.MATURED and outcome.outcome_target == OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC:
        _validate_diagnostic_target(repo, outcome)
    if outcome.label_state == LabelStateV2.MATURED and outcome.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE:
        if outcome.provenance == OutcomeProvenanceV2.ACTUAL:
            _validate_actual_binding(repo, outcome)
        else:
            assert isinstance(identity, Mapping)
            _validate_policy_payoff(repo, outcome, identity)
    if outcome.provenance == OutcomeProvenanceV2.ACTUAL and outcome.label_state == LabelStateV2.MATURED:
        entry = repo.get_artifact(outcome.actual_closed_source_ref or "")
        if entry is None or entry.artifact_type != "ActualClosedPositionSourceV2":
            raise ValueError("qualified actual closed source required")
        key = entry.metadata.get("key")
        if (not isinstance(key, Mapping) or key.get("contract_revision") != outcome.instrument_revision
                or key.get("venue") != outcome.venue or key.get("product") != outcome.product
                or entry.metadata.get("close_at_ns", outcome.horizon_end_ns + 1) > outcome.horizon_end_ns
                or entry.available_at_ns > outcome.matured_at_ns
                or outcome.net_payoff is None
                or entry.metadata.get("realized_net_pnl") != canonical_decimal_str(outcome.net_payoff)
                or sha256_json(entry.metadata) != outcome.actual_closed_source_ref):
            raise ValueError("actual close reconciliation mismatch")
    repo.register_artifact(ArtifactIndexEntryV2(outcome.content_hash, "MaturedOutcomeV2", outcome.content_hash,
        outcome.matured_at_ns, outcome.available_at_ns, {"outcome": outcome.to_dict()}))
    return outcome.content_hash


def executable_action_value_training_eligible(outcome: MaturedOutcomeV2, cutoff_ns: int) -> bool:
    timestamp(cutoff_ns, field="training cutoff")
    return (outcome.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE
            and outcome.action_hash is not None and outcome.label_state == LabelStateV2.MATURED
            and outcome.net_payoff is not None and outcome.available_at_ns <= cutoff_ns)


def matured_diagnostic_eligible(outcome: MaturedOutcomeV2, cutoff_ns: int) -> bool:
    timestamp(cutoff_ns, field="training cutoff")
    return (outcome.outcome_target == OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC
            and outcome.label_state == LabelStateV2.MATURED and outcome.diagnostic_value is not None
            and outcome.available_at_ns <= cutoff_ns)


def training_eligible(outcome: MaturedOutcomeV2, cutoff_ns: int) -> bool:
    """Compatibility alias with deliberately narrow exact action-value meaning."""
    return executable_action_value_training_eligible(outcome, cutoff_ns)


def realized_risk_source(repo: OpsRepository, outcome: MaturedOutcomeV2, source: ActualClosedPositionSourceV2) -> str:
    """Return only the qualified source identity, never a research payoff proxy."""
    if (outcome.provenance != OutcomeProvenanceV2.ACTUAL or outcome.label_state != LabelStateV2.MATURED
            or outcome.outcome_target != OutcomeTargetV2.EXECUTABLE_ACTION_VALUE
            or outcome.actual_action_binding_ref is None
            or outcome.actual_closed_source_ref != source.content_hash or outcome.net_payoff != source.realized_net_pnl
            or outcome.available_at_ns < source.available_at_ns or outcome.instrument_revision != source.key.contract_revision):
        raise ValueError("matured outcome is not reconciled actual risk evidence")
    indexed_outcome = repo.get_artifact(outcome.content_hash)
    indexed_source = repo.get_artifact(source.content_hash)
    if (indexed_outcome is None or indexed_outcome.artifact_type != "MaturedOutcomeV2"
            or canonical_json(indexed_outcome.metadata.get("outcome")) != canonical_json(outcome.to_dict())
            or indexed_source is None or indexed_source.artifact_type != "ActualClosedPositionSourceV2"
            or sha256_json(indexed_source.metadata) != source.content_hash):
        raise ValueError("actual risk source requires indexed exact outcome and close")
    _validate_actual_binding(repo, outcome)
    return source.content_hash
