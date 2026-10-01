"""Scientific calendar closure for native S3 one-minute evidence.

This module persists the existing CandidateSetV2 and DecisionCalendarEntryV2
contracts for timely native M1 origins, and a separate non-outcome record for
late/missed origins. It does not create candidates, actions or payoff labels.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .._serialization import FrozenMap, json_value, sha256_json, sha256_ref, strict_fields, timestamp
from ..contracts import (
    ArtifactEnvelope,
    CandidateSelectionStatus,
    CandidateSetV2,
    EligibilityStatusV2,
)
from ..data.s3_forward_evidence import S3QuoteBridgeResultV1
from ..instruments import (
    InstrumentKeyV2,
    ProductContractV2,
    StrategyEligibilityV2,
    UniverseContractV2,
    UniverseEntryV2,
)
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..runtime.ops_supervisor import OpsDecisionEventV1
from ..runtime.s3_native_cadence import (
    S3_M1_DEFAULT_MAX_LATENESS_NS,
    S3_M1_EVENT_TYPE,
    find_s3_m1_origin_late_gate,
    s3_m1_event_id,
    s3_m1_origin_ref,
)
from ..science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
    _resolve_candidate_set,
    _resolve_decision_calendar_entry,
    index_decision_calendar_entry,
)
from ..science.research_selection import (
    MULTI_SLEEVE_SELECTION_HASH,
    MULTI_SLEEVE_SELECTION_ID,
    MULTI_SLEEVE_SELECTION_VERSION,
)
from ..strategies.s3_mean_reversion import S3_POLICY

S3_MISSINGNESS_ARTIFACT_TYPE = "S3DecisionCalendarMissingnessV1"
S3_MISSINGNESS_IDENTITY_TYPE = "S3DecisionCalendarMissingnessIdentityV1"
S3_NATIVE_UNIVERSE_IDENTITY_TYPE = "S3NativeM1ResearchUniverseIdentityV1"
S3_NATIVE_CANDIDATE_SET_PRODUCER = "S3_NATIVE_M1_NOT_ESTIMABLE_CANDIDATE_SET_V1"
S3_NATIVE_UNIVERSE_PRODUCER = "S3_NATIVE_M1_CUTOFF_RESEARCH_UNIVERSE_V1"
S3_BLOCKER_REASON = "BYBIT_TRADE_COMPLETENESS_UNPROVEN"
S3_NOT_ESTIMABLE_TIE_BREAK_RULE = "NO_SELECTION_WHILE_BYBIT_TRADE_COMPLETENESS_IS_UNPROVEN"
_S3_MISSINGNESS_REASONS = frozenset({
    "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE",
    "NATIVE_M1_ORIGIN_EVENT_PREREQUISITE_UNAVAILABLE",
})


def _native_event_source(repository: OpsRepository, event: OpsDecisionEventV1) -> tuple[
    ProductContractV2, str, tuple[str, ...]
]:
    if event.event_type != S3_M1_EVENT_TYPE:
        raise ValueError("native S3 calendar requires a CONFIRMED_1M_CLOSE event")
    trigger = repository.get_artifact(event.trigger_ref)
    trigger_body = trigger.metadata.get("trigger") if trigger is not None else None
    if (trigger is None or trigger.artifact_type != "OpsPublicFinalBarTriggerV1"
            or trigger.content_hash != event.trigger_ref or not isinstance(trigger_body, Mapping)
            or trigger_body.get("version") != "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1"
            or trigger_body.get("information_cutoff_ns") != event.information_cutoff_ns
            or trigger.available_at_ns > event.information_cutoff_ns):
        raise ValueError("native S3 event does not bind its exact cutoff-visible trigger")
    product_ref = trigger_body.get("product_ref")
    bar_ref = trigger_body.get("bar_ref")
    if not isinstance(product_ref, str) or not isinstance(bar_ref, str):
        raise ValueError("native S3 trigger is missing its exact product or M1 bar reference")
    product_entry = repository.get_artifact(product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2"
            or product_entry.content_hash != product_ref or not isinstance(product_body, Mapping)
            or product_entry.available_at_ns > event.information_cutoff_ns):
        raise ValueError("native S3 trigger product is missing or unavailable at the fixed cutoff")
    product = ProductContractV2.from_dict(product_body)
    bar_entry = repository.get_artifact(bar_ref)
    bar_body = bar_entry.metadata.get("bar") if bar_entry is not None else None
    if (bar_entry is None or bar_entry.artifact_type != "CausalBarV2"
            or bar_entry.content_hash != bar_ref or not isinstance(bar_body, Mapping)
            or bar_entry.available_at_ns > event.information_cutoff_ns
            or bar_body.get("interval") != "1M" or bar_body.get("final") is not True
            or bar_body.get("instrument_revision") != product.key.contract_revision):
        raise ValueError("native S3 event does not bind its exact cutoff-visible final M1 bar")
    close_at_ns = bar_body.get("close_at_ns")
    if (type(close_at_ns) is not int or event.event_id != s3_m1_event_id(product.key, close_at_ns)
            or event.deadline_ns != close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS
            or product.effective_at_ns > close_at_ns):
        raise ValueError("native S3 event origin, product revision or fixed deadline conflicts")
    source_refs = tuple(sorted(set(event.causal_input_refs)))
    if not {event.trigger_ref, product_ref, bar_ref}.issubset(source_refs):
        raise ValueError("native S3 event omits its exact trigger, product or bar lineage")
    for ref in source_refs:
        indexed = repository.get_artifact(ref)
        if indexed is None or indexed.available_at_ns > event.information_cutoff_ns:
            raise ValueError("native S3 event includes missing or future cutoff evidence")
    return product, bar_ref, source_refs


def ensure_native_s3_research_universe(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    diagnostic_refs: Sequence[str],
    *,
    created_at_ns: int,
    available_at_ns: int,
) -> UniverseContractV2:
    """Create/reuse one exact-key research universe with S3 explicitly NOT_ESTIMABLE."""
    timestamp(created_at_ns, field="native S3 universe created_at_ns")
    timestamp(available_at_ns, field="native S3 universe available_at_ns")
    product, _bar_ref, source_refs = _native_event_source(repository, event)
    diagnostics = tuple(sorted(set(diagnostic_refs)))
    if any(not isinstance(ref, str) for ref in diagnostics):
        raise ValueError("native S3 diagnostic references must be SHA-256 strings")
    for ref in diagnostics:
        sha256_ref(ref, field="native S3 diagnostic ref")
        indexed = repository.get_artifact(ref)
        if indexed is None or indexed.available_at_ns > event.deadline_ns:
            raise ValueError("native S3 diagnostic is unavailable by the fixed event deadline")
    diagnostic_entries = tuple(repository.get_artifact(ref) for ref in diagnostics)
    if any(entry is None for entry in diagnostic_entries):
        raise ValueError("native S3 universe lost a persisted diagnostic artifact")
    earliest = max(
        event.information_cutoff_ns,
        *(entry.available_at_ns for entry in diagnostic_entries if entry is not None),
    )

    identity_ref = sha256_json({
        "artifact_type": S3_NATIVE_UNIVERSE_IDENTITY_TYPE,
        "decision_event_id": event.event_id,
    })
    existing = repository.get_artifact(identity_ref)
    if existing is not None:
        identity = existing.metadata.get("identity")
        identity_wire = json_value(identity) if isinstance(identity, Mapping) else None
        universe_ref = identity_wire.get("universe_ref") if isinstance(identity_wire, Mapping) else None
        expected_static = {
            "version": "S3_NATIVE_M1_RESEARCH_UNIVERSE_IDENTITY_V1",
            "decision_event_id": event.event_id,
            "instrument_key": product.key.to_dict(),
            "product_ref": product.content_hash,
            "evidence_cutoff_ns": event.information_cutoff_ns,
            "decision_slot_ns": event.deadline_ns,
            "source_input_refs": list(source_refs),
            "diagnostic_refs": list(diagnostics),
            "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
            "strategy_policy_hash": S3_POLICY.policy_hash,
            "authority": "ZERO",
        }
        if (existing.artifact_type != S3_NATIVE_UNIVERSE_IDENTITY_TYPE
                or existing.content_hash != sha256_json(identity_wire)
                or not isinstance(identity_wire, Mapping)
                or any(identity_wire.get(name) != value for name, value in expected_static.items())
                or not isinstance(universe_ref, str)):
            raise ValueError("native S3 research-universe identity already has conflicting durable state")
        universe_entry = repository.get_artifact(universe_ref)
        body = universe_entry.metadata.get("universe") if universe_entry is not None else None
        if (universe_entry is None or universe_entry.artifact_type != "UniverseContractV2"
                or universe_entry.content_hash != universe_ref or not isinstance(body, Mapping)):
            raise ValueError("native S3 research-universe identity points to missing typed evidence")
        universe = UniverseContractV2.from_dict(json_value(body))
        _validate_native_s3_universe(repository, event, product, universe)
        if (universe.content_hash != universe_ref
                or existing.created_at_ns != universe.envelope.available_at_ns
                or existing.available_at_ns != universe.envelope.available_at_ns
                or identity_wire.get("created_at_ns") != universe.envelope.created_at_ns
                or identity_wire.get("available_at_ns") != universe.envelope.available_at_ns
                or identity_wire.get("universe_artifact_id") != universe.envelope.artifact_id):
            raise ValueError("native S3 research-universe identity timestamps or content conflict")
        return universe

    if not earliest <= created_at_ns <= available_at_ns <= event.deadline_ns:
        raise ValueError("native S3 universe production is outside its fixed cutoff/deadline")

    eligibility = StrategyEligibilityV2(EligibilityStatusV2.NOT_ESTIMABLE, S3_BLOCKER_REASON)
    entry = UniverseEntryV2(
        product.key,
        product.content_hash,
        observed=True,
        data_eligible=False,
        scanner_eligible=False,
        deep_analysis_eligible=False,
        capital_eligible=False,
        strategy_eligibility=FrozenMap({S3_POLICY.policy_id: eligibility}),
        reasons=(S3_BLOCKER_REASON,),
    )
    identity = {
        "version": "S3_NATIVE_M1_RESEARCH_UNIVERSE_IDENTITY_V1",
        "decision_event_id": event.event_id,
        "instrument_key": product.key.to_dict(),
        "product_ref": product.content_hash,
        "evidence_cutoff_ns": event.information_cutoff_ns,
        "decision_slot_ns": event.deadline_ns,
        "source_input_refs": list(source_refs),
        "diagnostic_refs": list(diagnostics),
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        "strategy_policy_hash": S3_POLICY.policy_hash,
        "created_at_ns": created_at_ns,
        "available_at_ns": available_at_ns,
        "authority": "ZERO",
    }
    universe = UniverseContractV2(
        ArtifactEnvelope(
            1,
            sha256_json(identity),
            created_at_ns,
            available_at_ns,
            S3_NATIVE_UNIVERSE_PRODUCER,
            source_refs,
        ),
        "S3_NATIVE_M1_CUTOFF_MINIMAL_V1",
        event.deadline_ns,
        MULTI_SLEEVE_SELECTION_HASH,
        (entry,),
    )
    _validate_native_s3_universe(repository, event, product, universe)
    identity_body = {**identity, "universe_ref": universe.content_hash,
                     "universe_artifact_id": universe.envelope.artifact_id}
    repository.register_artifacts((
        ArtifactIndexEntryV2(
            universe.content_hash, "UniverseContractV2", universe.content_hash,
            universe.envelope.created_at_ns, universe.envelope.available_at_ns,
            {"universe": universe.to_dict()},
        ),
        ArtifactIndexEntryV2(
            identity_ref, S3_NATIVE_UNIVERSE_IDENTITY_TYPE, sha256_json(identity_body),
            universe.envelope.available_at_ns, universe.envelope.available_at_ns,
            {"identity": identity_body},
        ),
    ))
    return universe


def _validate_native_s3_universe(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    product: ProductContractV2,
    universe: UniverseContractV2,
) -> None:
    if (universe.selection_policy_hash != MULTI_SLEEVE_SELECTION_HASH
            or universe.universe_version != "S3_NATIVE_M1_CUTOFF_MINIMAL_V1"
            or universe.decision_slot_ns != event.deadline_ns
            or universe.envelope.created_at_ns < event.information_cutoff_ns
            or universe.envelope.available_at_ns > event.deadline_ns
            or not set(universe.envelope.input_refs).issubset(set(event.causal_input_refs))):
        raise ValueError("native S3 universe does not preserve its exact causal origin")
    for ref in universe.envelope.input_refs:
        indexed = repository.get_artifact(ref)
        if indexed is None or indexed.available_at_ns > event.information_cutoff_ns:
            raise ValueError("native S3 universe source evidence exceeds the fixed cutoff")
    if len(universe.entries) != 1:
        raise ValueError("native S3 minimal universe must retain exactly the originating instrument")
    entry = universe.entries[0]
    eligibility = entry.strategy_eligibility.get(S3_POLICY.policy_id)
    if (entry.key != product.key or entry.product_ref != product.content_hash or not entry.observed
            or entry.data_eligible or entry.scanner_eligible or entry.deep_analysis_eligible
            or entry.capital_eligible or eligibility is None
            or eligibility.status != EligibilityStatusV2.NOT_ESTIMABLE
            or eligibility.reason != S3_BLOCKER_REASON
            or entry.reasons != (S3_BLOCKER_REASON,)):
        raise ValueError("native S3 universe fabricates eligibility or loses its evidence blocker")


def _validate_native_s3_diagnostics(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    diagnostic_refs: Sequence[str],
    *,
    computation_started_ns: int,
) -> tuple[str, ...]:
    allowed = {
        "S3ForwardTradeEvidenceV1": "evidence",
        "S3SequenceBookQuoteEvidenceV1": "bridge",
        "S3NativeWarmupReadinessV1": "readiness",
    }
    refs = tuple(sorted(set(diagnostic_refs)))
    if not refs:
        raise ValueError("native S3 candidate evidence requires persisted cutoff-bounded diagnostics")
    types: set[str] = set()
    trigger = repository.get_artifact(event.trigger_ref)
    trigger_body = trigger.metadata.get("trigger") if trigger is not None else None
    expected_bar_ref = trigger_body.get("bar_ref") if isinstance(trigger_body, Mapping) else None
    product_ref = trigger_body.get("product_ref") if isinstance(trigger_body, Mapping) else None
    product_entry = repository.get_artifact(product_ref) if isinstance(product_ref, str) else None
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    product = ProductContractV2.from_dict(json_value(product_body)) if isinstance(product_body, Mapping) else None
    if not isinstance(expected_bar_ref, str) or product is None:
        raise ValueError("native S3 diagnostic validation has no exact trigger instrument/bar identity")
    for ref in refs:
        sha256_ref(ref, field="native S3 diagnostic ref")
        indexed = repository.get_artifact(ref)
        if indexed is None or indexed.artifact_type not in allowed:
            raise ValueError("native S3 candidate evidence contains an unknown diagnostic artifact")
        types.add(indexed.artifact_type)
        if (indexed.artifact_ref != ref or indexed.content_hash != ref
                or indexed.available_at_ns > computation_started_ns
                or indexed.available_at_ns > event.deadline_ns
                or indexed.metadata.get("decision_event_id") != event.event_id
                or indexed.metadata.get("trigger_bar_ref") != expected_bar_ref):
            raise ValueError("native S3 diagnostic is late or bound to a different event")
        body = indexed.metadata.get(allowed[indexed.artifact_type])
        if not isinstance(body, Mapping):
            raise ValueError("native S3 diagnostic artifact is missing its typed body")
        body_wire = json_value(body)
        if indexed.artifact_type == "S3SequenceBookQuoteEvidenceV1":
            typed_bridge = S3QuoteBridgeResultV1.from_dict(body_wire)
            if (typed_bridge.evidence_ref != ref or typed_bridge.key != product.key
                    or typed_bridge.cutoff_ns != event.information_cutoff_ns):
                raise ValueError("native S3 quote diagnostic conflicts with its exact instrument/cutoff")
        elif (body_wire.get("key") != product.key.to_dict()
              or sha256_json({"artifact_type": indexed.artifact_type,
                              allowed[indexed.artifact_type]: body_wire}) != ref):
            raise ValueError("native S3 diagnostic body hash or instrument identity conflicts")
        context_body = body.get("computation_context")
        if not isinstance(context_body, Mapping):
            raise ValueError("native S3 diagnostic artifact is missing computation timing")
        if (context_body.get("evidence_cutoff_ns") != event.information_cutoff_ns
                or context_body.get("consumer_deadline_ns") != event.deadline_ns
                or type(context_body.get("computation_started_ns")) is not int
                or type(context_body.get("computation_finished_ns")) is not int
                or context_body["computation_started_ns"] < event.information_cutoff_ns
                or context_body["computation_finished_ns"] < context_body["computation_started_ns"]
                or context_body["computation_finished_ns"] > indexed.available_at_ns):
            raise ValueError("native S3 diagnostic violates the fixed causal timeline")
        if indexed.artifact_type in {"S3ForwardTradeEvidenceV1", "S3NativeWarmupReadinessV1"}:
            if (body.get("cutoff_ns") != event.information_cutoff_ns
                    or body.get("trade_completeness_proven") is not False):
                raise ValueError("native S3 trade-completeness blocker was changed or omitted")
    required = {"S3ForwardTradeEvidenceV1", "S3NativeWarmupReadinessV1"}
    if not required.issubset(types):
        raise ValueError("native S3 candidate set lacks forward-trade or readiness evidence")
    return refs


def _native_candidate_set_index_ref(event_id: str, universe_ref: str) -> str:
    return sha256_json({
        "artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": event_id,
        "universe_ref": universe_ref,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
    })


def _missingness_identity_ref(origin_ref: str) -> str:
    return sha256_json({
        "artifact_type": S3_MISSINGNESS_IDENTITY_TYPE,
        "origin_ref": origin_ref,
        "policy_hash": S3_POLICY.policy_hash,
    })


def persist_native_s3_not_estimable_candidate_set(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    universe: UniverseContractV2,
    diagnostic_refs: Sequence[str],
    *,
    computation_started_ns: int,
    computation_finished_ns: int,
    available_at_ns: int,
) -> CandidateSetV2:
    """Persist/reuse the one empty NOT_ESTIMABLE CandidateSetV2 for this event."""
    for name, value in (("computation_started_ns", computation_started_ns),
                        ("computation_finished_ns", computation_finished_ns),
                        ("candidate_set.available_at_ns", available_at_ns)):
        timestamp(value, field=name)
    product, _bar_ref, source_refs = _native_event_source(repository, event)
    _validate_native_s3_universe(repository, event, product, universe)
    diagnostics = _validate_native_s3_diagnostics(
        repository, event, diagnostic_refs, computation_started_ns=computation_started_ns,
    )
    diagnostic_entries = tuple(repository.get_artifact(ref) for ref in diagnostics)
    if any(entry is None for entry in diagnostic_entries):
        raise ValueError("native S3 CandidateSet lost a persisted diagnostic artifact")
    index_ref = _native_candidate_set_index_ref(event.event_id, universe.content_hash)
    for prior_index in repository.artifact_entries("CandidateSetDecisionIndexV1"):
        if (prior_index.metadata.get("decision_event_id") == event.event_id
                and prior_index.artifact_ref != index_ref):
            raise ValueError("native S3 decision already has a conflicting CandidateSet index identity")
    index_entry = repository.get_artifact(index_ref)
    if index_entry is not None:
        return _reuse_native_s3_candidate_set(
            repository, index_entry, index_ref, event, universe, source_refs, diagnostics,
        )

    stray = []
    for entry in repository.artifact_entries("CandidateSetV2"):
        body = entry.metadata.get("candidate_set")
        if isinstance(body, Mapping) and body.get("decision_event_id") == event.event_id:
            stray.append(entry)
    if stray:
        raise ValueError("native S3 decision already has an unindexed CandidateSet identity")

    if not (max(event.information_cutoff_ns, universe.envelope.available_at_ns,
                *(entry.available_at_ns for entry in diagnostic_entries if entry is not None))
            <= computation_started_ns <= computation_finished_ns <= available_at_ns
            <= event.deadline_ns):
        raise ValueError("native S3 CandidateSet timing is backdated or misses the fixed deadline")

    identity = {
        "decision_event_id": event.event_id,
        "universe_ref": universe.content_hash,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        "candidate_refs": [],
        "causal_input_refs": list(source_refs),
        "cutoff_ns": event.information_cutoff_ns,
        "deadline_ns": event.deadline_ns,
        "producer_version": S3_NATIVE_CANDIDATE_SET_PRODUCER,
        "authority": "ZERO",
        "diagnostic_refs": list(diagnostics),
        "universe_identity_ref": sha256_json({
            "artifact_type": S3_NATIVE_UNIVERSE_IDENTITY_TYPE,
            "decision_event_id": event.event_id,
        }),
        "computation_started_ns": computation_started_ns,
        "computation_finished_ns": computation_finished_ns,
        "candidate_set_available_at_ns": available_at_ns,
    }
    candidate_set = CandidateSetV2(
        ArtifactEnvelope(
            1,
            sha256_json(identity),
            computation_finished_ns,
            available_at_ns,
            S3_NATIVE_CANDIDATE_SET_PRODUCER,
            source_refs,
        ),
        event.event_id,
        universe.content_hash,
        MULTI_SLEEVE_SELECTION_HASH,
        (),
        None,
        S3_NOT_ESTIMABLE_TIE_BREAK_RULE,
        CandidateSelectionStatus.NOT_ESTIMABLE,
    )
    index_body = {
        "candidate_set_ref": candidate_set.content_hash,
        "decision_event_id": event.event_id,
        "universe_ref": universe.content_hash,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        "cutoff_ns": event.information_cutoff_ns,
        "deadline_ns": event.deadline_ns,
    }
    repository.register_artifacts((
        ArtifactIndexEntryV2(
            candidate_set.content_hash, "CandidateSetV2", candidate_set.content_hash,
            candidate_set.envelope.created_at_ns, candidate_set.envelope.available_at_ns,
            {"candidate_set": candidate_set.to_dict(), "identity": identity},
        ),
        ArtifactIndexEntryV2(
            index_ref, "CandidateSetDecisionIndexV1", sha256_json(index_body),
            available_at_ns, available_at_ns, index_body,
        ),
    ))
    return candidate_set


def _reuse_native_s3_candidate_set(
    repository: OpsRepository,
    index_entry: ArtifactIndexEntryV2,
    index_ref: str,
    event: OpsDecisionEventV1,
    universe: UniverseContractV2,
    source_refs: tuple[str, ...],
    diagnostic_refs: tuple[str, ...],
) -> CandidateSetV2:
    body = index_entry.metadata
    candidate_ref = body.get("candidate_set_ref")
    if (index_entry.artifact_ref != index_ref or index_entry.artifact_type != "CandidateSetDecisionIndexV1"
            or index_entry.content_hash != sha256_json(body)
            or body.get("decision_event_id") != event.event_id
            or body.get("universe_ref") != universe.content_hash
            or body.get("selection_policy_hash") != MULTI_SLEEVE_SELECTION_HASH
            or body.get("cutoff_ns") != event.information_cutoff_ns
            or body.get("deadline_ns") != event.deadline_ns
            or not isinstance(candidate_ref, str)):
        raise ValueError("native S3 CandidateSet decision index conflicts with the immutable event")
    candidate_entry = repository.get_artifact(candidate_ref)
    candidate_body = candidate_entry.metadata.get("candidate_set") if candidate_entry is not None else None
    identity = candidate_entry.metadata.get("identity") if candidate_entry is not None else None
    candidate_set = CandidateSetV2.from_dict(json_value(candidate_body)) if isinstance(candidate_body, Mapping) else None
    if (candidate_entry is None or candidate_entry.artifact_type != "CandidateSetV2"
            or candidate_entry.content_hash != candidate_ref or candidate_set is None
            or not isinstance(identity, Mapping) or candidate_set.content_hash != candidate_ref
            or candidate_set.decision_event_id != event.event_id
            or candidate_set.universe_ref != universe.content_hash
            or candidate_set.selection_policy_hash != MULTI_SLEEVE_SELECTION_HASH
            or candidate_set.candidates or candidate_set.selected_candidate_id is not None
            or candidate_set.selection_status != CandidateSelectionStatus.NOT_ESTIMABLE
            or candidate_set.tie_break_rule != S3_NOT_ESTIMABLE_TIE_BREAK_RULE
            or candidate_set.envelope.input_refs != source_refs
            or identity.get("producer_version") != S3_NATIVE_CANDIDATE_SET_PRODUCER
            or identity.get("authority") != "ZERO"
            or identity.get("cutoff_ns") != event.information_cutoff_ns
            or identity.get("deadline_ns") != event.deadline_ns
            or identity.get("universe_ref") != universe.content_hash
            or tuple(identity.get("diagnostic_refs", ())) != diagnostic_refs
            or tuple(identity.get("causal_input_refs", ())) != source_refs
            or sha256_json(identity) != candidate_set.envelope.artifact_id
            or candidate_entry.available_at_ns != candidate_set.envelope.available_at_ns
            or candidate_entry.created_at_ns != candidate_set.envelope.created_at_ns
            or candidate_set.envelope.available_at_ns > event.deadline_ns
            or index_entry.created_at_ns != candidate_set.envelope.available_at_ns
            or index_entry.available_at_ns != candidate_set.envelope.available_at_ns):
        raise ValueError("native S3 CandidateSet has conflicting durable identity or semantics")
    validated, _validated_identity = _resolve_candidate_set(repository, candidate_ref)
    if validated.content_hash != candidate_set.content_hash:
        raise ValueError("native S3 CandidateSet resolver rejected its durable production timing")
    return candidate_set


def persist_native_s3_not_estimable_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    event: OpsDecisionEventV1,
) -> str:
    if (candidate_set.decision_event_id != event.event_id
            or candidate_set.selection_status != CandidateSelectionStatus.NOT_ESTIMABLE
            or candidate_set.tie_break_rule != S3_NOT_ESTIMABLE_TIE_BREAK_RULE
            or candidate_set.candidates or candidate_set.selected_candidate_id is not None):
        raise ValueError("native S3 calendar requires its exact empty NOT_ESTIMABLE CandidateSet")
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash,
        None,
        MULTI_SLEEVE_SELECTION_ID,
        MULTI_SLEEVE_SELECTION_VERSION,
        MULTI_SLEEVE_SELECTION_HASH,
        event.information_cutoff_ns,
        SelectionStateV2.NOT_ESTIMABLE,
        AdmissionStateV2.NOT_APPLICABLE,
        None,
        None,
        DecisionSourceStageV2.CANDIDATE_SET,
        (S3_BLOCKER_REASON,),
        candidate_set.content_hash,
        candidate_set.envelope.available_at_ns,
        candidate_set.envelope.available_at_ns,
    )
    return index_decision_calendar_entry(repository, row)


@dataclass(frozen=True)
class S3DecisionCalendarMissingnessV1:
    """Scientific denominator record for a late/missed native M1 origin only."""

    instrument_key: InstrumentKeyV2
    policy_id: str
    policy_version: str
    policy_hash: str
    decision_slot_ns: int
    origin_ref: str
    late_gate_ref: str
    state: str
    reason_code: str
    created_at_ns: int
    available_at_ns: int
    authority: str = "ZERO"

    ARTIFACT_TYPE = S3_MISSINGNESS_ARTIFACT_TYPE
    VERSION = "S3_DECISION_CALENDAR_MISSINGNESS_V1"

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_key, InstrumentKeyV2):
            raise ValueError("S3 missingness requires exact InstrumentKeyV2")
        if (self.policy_id != S3_POLICY.policy_id or self.policy_version != S3_POLICY.version
                or self.policy_hash != S3_POLICY.policy_hash):
            raise ValueError("S3 missingness must bind the accepted S3 policy identity")
        timestamp(self.decision_slot_ns, field="decision_slot_ns")
        sha256_ref(self.origin_ref, field="origin_ref")
        sha256_ref(self.late_gate_ref, field="late_gate_ref")
        if self.origin_ref != s3_m1_origin_ref(self.instrument_key, self.decision_slot_ns):
            raise ValueError("S3 missingness origin identity conflicts with its key/slot")
        if self.state != "TEST GATE" or self.reason_code not in _S3_MISSINGNESS_REASONS:
            raise ValueError("S3 missingness must preserve the exact late TEST GATE state/reason")
        timestamp(self.created_at_ns, field="created_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("S3 missingness availability cannot precede creation")
        if self.authority != "ZERO":
            raise ValueError("S3 missingness carries no decision authority")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "instrument_key": self.instrument_key.to_dict(),
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "decision_slot_ns": self.decision_slot_ns,
            "origin_ref": self.origin_ref,
            "late_gate_ref": self.late_gate_ref,
            "state": self.state,
            "reason_code": self.reason_code,
            "created_at_ns": self.created_at_ns,
            "available_at_ns": self.available_at_ns,
            "authority": self.authority,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> S3DecisionCalendarMissingnessV1:
        fields = {
            "version", "instrument_key", "policy_id", "policy_version", "policy_hash",
            "decision_slot_ns", "origin_ref", "late_gate_ref", "state", "reason_code",
            "created_at_ns", "available_at_ns", "authority",
        }
        d = strict_fields(data, expected=fields, required=fields, name=cls.ARTIFACT_TYPE)
        if d["version"] != cls.VERSION or not isinstance(d["instrument_key"], Mapping):
            raise ValueError("unsupported S3DecisionCalendarMissingnessV1 wire version")
        return cls(
            InstrumentKeyV2.from_dict(d["instrument_key"]), d["policy_id"], d["policy_version"],
            d["policy_hash"], d["decision_slot_ns"], d["origin_ref"], d["late_gate_ref"],
            d["state"], d["reason_code"], d["created_at_ns"], d["available_at_ns"], d["authority"],
        )


def persist_s3_late_origin_missingness(
    repository: OpsRepository,
    *,
    instrument_key: InstrumentKeyV2,
    decision_slot_ns: int,
    origin_ref: str,
    late_gate_ref: str,
    created_at_ns: int,
    available_at_ns: int,
) -> str:
    gate_entry = repository.get_artifact(late_gate_ref)
    origin_gate = find_s3_m1_origin_late_gate(
        (gate_entry,) if gate_entry is not None else (), instrument_key, decision_slot_ns,
    )
    if origin_gate is None or origin_ref != s3_m1_origin_ref(instrument_key, decision_slot_ns):
        raise ValueError("S3 missingness must bind the exact persisted late-origin gate")
    gate_body = origin_gate.metadata.get("deadline_gate")
    reason = gate_body.get("reason_code") if isinstance(gate_body, Mapping) else None
    if not isinstance(reason, str):
        raise ValueError("S3 late-origin gate has no exact reason code")
    if min(created_at_ns, available_at_ns) < origin_gate.available_at_ns:
        raise ValueError("S3 missingness cannot predate its durable late-origin gate")
    identity_ref = _missingness_identity_ref(origin_ref)
    existing = repository.get_artifact(identity_ref)
    if existing is not None:
        identity = existing.metadata.get("identity")
        identity_wire = json_value(identity) if isinstance(identity, Mapping) else None
        missingness_ref = identity_wire.get("missingness_ref") if isinstance(identity_wire, Mapping) else None
        if (existing.artifact_type != S3_MISSINGNESS_IDENTITY_TYPE
                or existing.content_hash != sha256_json(identity_wire)
                or not isinstance(identity_wire, Mapping)
                or identity_wire.get("instrument_key") != instrument_key.to_dict()
                or identity_wire.get("decision_slot_ns") != decision_slot_ns
                or identity_wire.get("origin_ref") != origin_ref
                or identity_wire.get("late_gate_ref") != late_gate_ref
                or identity_wire.get("policy_hash") != S3_POLICY.policy_hash
                or not isinstance(missingness_ref, str)):
            raise ValueError("S3 missingness identity already has conflicting durable state")
        indexed = repository.get_artifact(missingness_ref)
        body = indexed.metadata.get("missingness") if indexed is not None else None
        record = S3DecisionCalendarMissingnessV1.from_dict(json_value(body)) if isinstance(body, Mapping) else None
        if (indexed is None or indexed.artifact_type != S3_MISSINGNESS_ARTIFACT_TYPE
                or indexed.content_hash != missingness_ref or record is None
                or record.content_hash != missingness_ref or record.instrument_key != instrument_key
                or record.decision_slot_ns != decision_slot_ns or record.origin_ref != origin_ref
                or record.late_gate_ref != late_gate_ref or record.reason_code != reason):
            raise ValueError("S3 missingness identity points to conflicting typed evidence")
        return missingness_ref

    missingness = S3DecisionCalendarMissingnessV1(
        instrument_key,
        S3_POLICY.policy_id,
        S3_POLICY.version,
        S3_POLICY.policy_hash,
        decision_slot_ns,
        origin_ref,
        late_gate_ref,
        "TEST GATE",
        reason,
        created_at_ns,
        available_at_ns,
        "ZERO",
    )
    identity = {
        "version": "S3_DECISION_CALENDAR_MISSINGNESS_IDENTITY_V1",
        "instrument_key": instrument_key.to_dict(),
        "policy_hash": S3_POLICY.policy_hash,
        "decision_slot_ns": decision_slot_ns,
        "origin_ref": origin_ref,
        "late_gate_ref": late_gate_ref,
        "missingness_ref": missingness.content_hash,
        "authority": "ZERO",
    }
    repository.register_artifacts((
        ArtifactIndexEntryV2(
            missingness.content_hash, S3_MISSINGNESS_ARTIFACT_TYPE, missingness.content_hash,
            created_at_ns, available_at_ns, {"missingness": missingness.to_dict()},
        ),
        ArtifactIndexEntryV2(
            identity_ref, S3_MISSINGNESS_IDENTITY_TYPE, sha256_json(identity),
            created_at_ns, available_at_ns, {"identity": identity},
        ),
    ))
    return missingness.content_hash


@dataclass(frozen=True)
class S3DecisionCalendarDenominatorRowV1:
    population_class: str
    artifact_ref: str
    artifact_type: str
    instrument_key: InstrumentKeyV2
    policy_id: str
    policy_version: str
    policy_hash: str
    decision_slot_ns: int
    state: str
    reason_code: str


def s3_decision_calendar_denominator(repository: OpsRepository) -> tuple[
    S3DecisionCalendarDenominatorRowV1, ...
]:
    """Read timely calendar rows plus explicit late-origin TEST GATE rows.

    The two population classes stay separate. Legacy deadline gates are exposed
    directly until their additive typed missingness record is present.
    """
    native_events: dict[str, tuple[InstrumentKeyV2, int, int]] = {}
    for source in repository.artifact_entries("OpsDecisionEventSourceV1"):
        body = source.metadata.get("event")
        origin = source.metadata.get("native_m1_origin")
        if not isinstance(body, Mapping) or body.get("event_type") != S3_M1_EVENT_TYPE:
            continue
        if not isinstance(origin, Mapping) or not isinstance(origin.get("instrument_key"), Mapping):
            raise ValueError("native S3 event source lost its exact origin metadata")
        key = InstrumentKeyV2.from_dict(origin["instrument_key"])
        slot = origin.get("close_at_ns")
        cutoff = body.get("information_cutoff_ns")
        deadline = body.get("deadline_ns")
        if (type(slot) is not int or origin.get("origin_ref") != s3_m1_origin_ref(key, slot)
                or body.get("event_id") != s3_m1_event_id(key, slot)
                or deadline != slot + S3_M1_DEFAULT_MAX_LATENESS_NS
                or type(cutoff) is not int or type(deadline) is not int
                or cutoff < slot or cutoff > deadline):
            raise ValueError("native S3 event source has conflicting origin/deadline identity")
        event_id = body.get("event_id")
        if not isinstance(event_id, str) or event_id in native_events:
            raise ValueError("native S3 event source identity is duplicate or malformed")
        native_events[event_id] = key, slot, cutoff

    rows: list[S3DecisionCalendarDenominatorRowV1] = []
    seen_timely: set[str] = set()
    seen_timely_events: set[str] = set()
    candidate_sets: dict[str, CandidateSetV2] = {}
    for candidate_entry in repository.artifact_entries("CandidateSetV2"):
        body = candidate_entry.metadata.get("candidate_set")
        if not isinstance(body, Mapping):
            continue
        candidate_set = CandidateSetV2.from_dict(json_value(body))
        if candidate_set.decision_event_id in native_events:
            parsed, identity = _resolve_candidate_set(repository, candidate_entry.artifact_ref)
            if (parsed.content_hash != candidate_set.content_hash
                    or identity.get("producer_version") != S3_NATIVE_CANDIDATE_SET_PRODUCER
                    or identity.get("authority") != "ZERO"):
                raise ValueError("native S3 CandidateSet is not the exact timed research artifact")
            if candidate_set.decision_event_id in candidate_sets:
                raise ValueError("native S3 event has multiple immutable CandidateSet artifacts")
            candidate_sets[candidate_set.decision_event_id] = candidate_set

    for calendar_index in repository.artifact_entries("DecisionCalendarEntryV2"):
        raw = calendar_index.metadata.get("decision_entry")
        if not isinstance(raw, Mapping):
            continue
        calendar = DecisionCalendarEntryV2.from_dict(json_value(raw))
        candidate_set, _identity = _resolve_candidate_set(repository, calendar.candidate_set_ref)
        if candidate_set.decision_event_id not in native_events:
            continue
        validated_calendar = _resolve_decision_calendar_entry(repository, calendar_index.artifact_ref)
        key, slot, cutoff = native_events[candidate_set.decision_event_id]
        if (calendar_index.artifact_ref in seen_timely or candidate_sets.get(candidate_set.decision_event_id) is None
                or candidate_set.selection_status != CandidateSelectionStatus.NOT_ESTIMABLE
                or candidate_set.tie_break_rule != S3_NOT_ESTIMABLE_TIE_BREAK_RULE
                or candidate_set.candidates or candidate_set.selected_candidate_id is not None
                or calendar.candidate_ref is not None or calendar.action_hash is not None
                or calendar.action_artifact_ref is not None
                or calendar.selection_state != SelectionStateV2.NOT_ESTIMABLE
                or calendar.admission_state != AdmissionStateV2.NOT_APPLICABLE
                or calendar.source_stage != DecisionSourceStageV2.CANDIDATE_SET
                or S3_BLOCKER_REASON not in calendar.reason_codes
                or validated_calendar.content_hash != calendar.content_hash
                or calendar.decision_at_ns != cutoff):
            raise ValueError("native S3 timely calendar row has conflicting NOT_ESTIMABLE semantics")
        seen_timely.add(calendar_index.artifact_ref)
        seen_timely_events.add(candidate_set.decision_event_id)
        rows.append(S3DecisionCalendarDenominatorRowV1(
            "TIMELY_NOT_ESTIMABLE",
            calendar_index.artifact_ref,
            "DecisionCalendarEntryV2",
            key,
            S3_POLICY.policy_id,
            S3_POLICY.version,
            S3_POLICY.policy_hash,
            slot,
            SelectionStateV2.NOT_ESTIMABLE.value,
            S3_BLOCKER_REASON,
        ))

    if set(candidate_sets) != set(native_events):
        raise ValueError("native S3 event denominator has an event without its immutable CandidateSetV2")
    if seen_timely_events != set(native_events):
        raise ValueError("native S3 event denominator has an event without its DecisionCalendarEntryV2")

    missingness_entries = repository.artifact_entries(S3_MISSINGNESS_ARTIFACT_TYPE)
    missing_by_origin: dict[str, tuple[ArtifactIndexEntryV2, S3DecisionCalendarMissingnessV1]] = {}
    for indexed in missingness_entries:
        raw = indexed.metadata.get("missingness")
        if not isinstance(raw, Mapping):
            raise ValueError("S3 missingness index has no typed body")
        missing = S3DecisionCalendarMissingnessV1.from_dict(json_value(raw))
        if (indexed.artifact_ref != missing.content_hash or indexed.content_hash != missing.content_hash
                or indexed.created_at_ns != missing.created_at_ns
                or indexed.available_at_ns != missing.available_at_ns
                or missing.origin_ref in missing_by_origin):
            raise ValueError("S3 missingness is malformed or duplicated by origin")
        gate_index = repository.get_artifact(missing.late_gate_ref)
        gate = find_s3_m1_origin_late_gate(
            (gate_index,) if gate_index is not None else (),
            missing.instrument_key,
            missing.decision_slot_ns,
        )
        if gate is None:
            raise ValueError("S3 missingness points to no exact late-origin gate")
        identity_entry = repository.get_artifact(_missingness_identity_ref(missing.origin_ref))
        missingness_identity = identity_entry.metadata.get("identity") if identity_entry is not None else None
        identity_wire = json_value(missingness_identity) if isinstance(missingness_identity, Mapping) else None
        if (identity_entry is None or identity_entry.artifact_type != S3_MISSINGNESS_IDENTITY_TYPE
                or not isinstance(identity_wire, Mapping)
                or identity_entry.content_hash != sha256_json(identity_wire)
                or identity_entry.created_at_ns != missing.created_at_ns
                or identity_entry.available_at_ns != missing.available_at_ns
                or identity_wire.get("origin_ref") != missing.origin_ref
                or identity_wire.get("instrument_key") != missing.instrument_key.to_dict()
                or identity_wire.get("decision_slot_ns") != missing.decision_slot_ns
                or identity_wire.get("late_gate_ref") != missing.late_gate_ref
                or identity_wire.get("missingness_ref") != indexed.artifact_ref
                or identity_wire.get("policy_hash") != S3_POLICY.policy_hash
                or identity_wire.get("authority") != "ZERO"):
            raise ValueError("S3 missingness has no exact immutable origin identity")
        missing_by_origin[missing.origin_ref] = indexed, missing

    seen_late: set[str] = set()
    timely_origin_slots = {(key, slot) for key, slot, _cutoff in native_events.values()}
    for gate_index in repository.artifact_entries("OpsPublicAcquisitionDeadlineGateV1"):
        origin = gate_index.metadata.get("native_m1_origin")
        if not isinstance(origin, Mapping) or not isinstance(origin.get("instrument_key"), Mapping):
            continue
        key = InstrumentKeyV2.from_dict(origin["instrument_key"])
        slot = origin.get("close_at_ns")
        if type(slot) is not int:
            raise ValueError("native S3 late gate has no exact close slot")
        gate = find_s3_m1_origin_late_gate((gate_index,), key, slot)
        if gate is None:
            continue
        if (key, slot) in timely_origin_slots:
            raise ValueError("native S3 origin has both a timely decision event and a late TEST GATE")
        origin_ref = s3_m1_origin_ref(key, slot)
        missing_pair = missing_by_origin.get(origin_ref)
        if missing_pair is not None:
            indexed, missing = missing_pair
            if missing.late_gate_ref != gate.artifact_ref:
                raise ValueError("S3 missingness and late gate identity disagree")
            record_ref, record_type, state, reason = (
                indexed.artifact_ref, S3_MISSINGNESS_ARTIFACT_TYPE, missing.state, missing.reason_code,
            )
        else:
            gate_body = gate.metadata.get("deadline_gate")
            if not isinstance(gate_body, Mapping):
                raise ValueError("legacy S3 late gate is missing its typed state")
            record_ref, record_type, state, reason = (
                gate.artifact_ref, gate.artifact_type, str(gate_body.get("status")),
                str(gate_body.get("reason_code")),
            )
        if origin_ref in seen_late:
            raise ValueError("native S3 origin appears more than once in the denominator")
        seen_late.add(origin_ref)
        rows.append(S3DecisionCalendarDenominatorRowV1(
            "LATE_OR_MISSED_TEST_GATE",
            record_ref,
            record_type,
            key,
            S3_POLICY.policy_id,
            S3_POLICY.version,
            S3_POLICY.policy_hash,
            slot,
            state,
            reason,
        ))

    if not set(missing_by_origin).issubset(seen_late):
        raise ValueError("S3 missingness identity has no governed late-origin denominator row")

    return tuple(sorted(rows, key=lambda row: (
        row.decision_slot_ns,
        row.instrument_key.to_canonical_json(),
        row.population_class,
        row.artifact_ref,
    )))
