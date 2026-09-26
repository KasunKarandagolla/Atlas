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
from atlas.v2.contracts import CandidateActionV2, CandidateSelectionStatus, CandidateSetV2, EligibilityStatusV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import (
    NOTIONAL_CONVENTION,
    SIZING_VERSION,
    ActualClosedPositionSourceV2,
)

VERSION = "MATURED_OUTCOME_V2_V3"
DECISION_ENTRY_VERSION = "DECISION_CALENDAR_ENTRY_V2_V1"
ECONOMIC_EVALUATION_VERSION = "EVALUATION_ARTIFACT_V2_AMENDED_V1"
ECONOMIC_EVALUATION_VERSIONS = frozenset({
    ECONOMIC_EVALUATION_VERSION,
    "EVALUATION_ARTIFACT_V2_AMENDED_V2",
})


class OutcomeProvenanceV2(StrEnum):
    ACTUAL = "ACTUAL"
    SIMULATED = "SIMULATED"
    COUNTERFACTUAL = "COUNTERFACTUAL"


class SelectionStateV2(StrEnum):
    SELECTED = "SELECTED"
    UNSELECTED = "UNSELECTED"
    REJECTED = "REJECTED"
    NO_CANDIDATE = "NO_CANDIDATE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


class AdmissionStateV2(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_EVALUATED = "NOT_EVALUATED"
    RISK_SIZED = "RISK_SIZED"
    CANDIDATE = "CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    EXPIRED = "EXPIRED"


class ExecutionOutcomeStateV2(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNRESOLVED = "UNRESOLVED"
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


class DecisionSourceStageV2(StrEnum):
    CANDIDATE_SET = "CANDIDATE_SET"
    HARD_RISK = "HARD_RISK"
    ECONOMIC_EVALUATION = "ECONOMIC_EVALUATION"
    EXPIRY = "EXPIRY"


@dataclass(frozen=True)
class DecisionCalendarEntryV2:
    """Immutable historical selection/admission state bound to its indexed producer evidence."""

    candidate_set_ref: str
    candidate_ref: str | None
    policy_id: str
    policy_version: str
    policy_hash: str
    decision_at_ns: int
    selection_state: SelectionStateV2
    admission_state: AdmissionStateV2
    action_hash: str | None
    action_artifact_ref: str | None
    source_stage: DecisionSourceStageV2
    reason_codes: tuple[str, ...]
    source_artifact_ref: str
    created_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("candidate_set_ref", "policy_hash", "source_artifact_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("candidate_ref", "action_hash", "action_artifact_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        for name in ("policy_id", "policy_version"):
            nonblank(getattr(self, name), field=name)
        for name in ("decision_at_ns", "created_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.decision_at_ns <= self.created_at_ns <= self.available_at_ns:
            raise ValueError("decision-calendar chronology invalid")
        object.__setattr__(self, "selection_state", SelectionStateV2(self.selection_state))
        object.__setattr__(self, "admission_state", AdmissionStateV2(self.admission_state))
        object.__setattr__(self, "source_stage", DecisionSourceStageV2(self.source_stage))
        reasons = tuple(self.reason_codes)
        if reasons != tuple(sorted(set(reasons))):
            raise ValueError("decision reason codes must be sorted and unique")
        for reason in reasons:
            nonblank(reason, field="decision reason")
        object.__setattr__(self, "reason_codes", reasons)
        if self.action_hash is None:
            if self.action_artifact_ref is not None:
                raise ValueError("absent decision action cannot carry artifact ref")
        elif self.action_artifact_ref is None:
            raise ValueError("frozen decision action requires artifact ref")
        if self.selection_state != SelectionStateV2.SELECTED and self.admission_state != AdmissionStateV2.NOT_APPLICABLE:
            raise ValueError("non-selected calendar entries cannot assert admission state")
        if self.admission_state == AdmissionStateV2.RISK_SIZED and self.action_hash is None:
            raise ValueError("risk-sized admission requires its frozen action")
        if self.action_hash is not None and self.selection_state != SelectionStateV2.SELECTED:
            raise ValueError("unselected/rejected/no-candidate entries cannot invent an action")

    @property
    def decision_identity_ref(self) -> str:
        return sha256_json({"version": "DECISION_CALENDAR_IDENTITY_V2_V1",
            "candidate_set_ref": self.candidate_set_ref, "candidate_ref": self.candidate_ref,
            "policy_hash": self.policy_hash})

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": DECISION_ENTRY_VERSION,
            **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DecisionCalendarEntryV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="DecisionCalendarEntryV2")
        if d["version"] != DECISION_ENTRY_VERSION or not isinstance(d["reason_codes"], list):
            raise ValueError("unsupported decision calendar wire version")
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class CandidateExpiryEvidenceV2:
    candidate_set_ref: str
    candidate_ref: str
    policy_hash: str
    deadline_ns: int
    expired_at_ns: int
    reason_code: str

    def __post_init__(self) -> None:
        for name in ("candidate_set_ref", "candidate_ref", "policy_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("deadline_ns", "expired_at_ns"):
            timestamp(getattr(self, name), field=name)
        nonblank(self.reason_code, field="reason_code")
        if self.expired_at_ns < self.deadline_ns:
            raise ValueError("candidate expiry cannot precede its declared deadline")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "CANDIDATE_EXPIRY_EVIDENCE_V2_V1",
            **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CandidateExpiryEvidenceV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="CandidateExpiryEvidenceV2")
        if d["version"] != "CANDIDATE_EXPIRY_EVIDENCE_V2_V1":
            raise ValueError("unsupported candidate-expiry evidence version")
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_candidate_expiry_evidence(repo: OpsRepository, item: CandidateExpiryEvidenceV2,
                                    *, available_at_ns: int) -> str:
    timestamp(available_at_ns, field="expiry evidence available_at_ns")
    if available_at_ns < item.expired_at_ns:
        raise ValueError("expiry evidence cannot be available before expiry")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, "CandidateExpiryEvidenceV2",
        item.content_hash, item.expired_at_ns, available_at_ns, {"expiry": item.to_dict()}))
    return item.content_hash


def _resolve_candidate_set(repo: OpsRepository, ref: str) -> tuple[CandidateSetV2, Mapping[str, Any]]:
    indexed = repo.get_artifact(ref)
    body = indexed.metadata.get("candidate_set") if indexed is not None else None
    identity = indexed.metadata.get("identity") if indexed is not None else None
    if (indexed is None or indexed.artifact_type != "CandidateSetV2" or indexed.content_hash != ref
            or not isinstance(body, Mapping) or not isinstance(identity, Mapping)):
        raise ValueError("typed indexed CandidateSet decision evidence required")
    candidate_set = CandidateSetV2.from_dict(json_value(body))
    cutoff_ns = identity.get("cutoff_ns")
    if type(cutoff_ns) is not int:
        raise ValueError("CandidateSet identity must declare an exact integer cutoff")
    candidate_refs = tuple(identity.get("candidate_refs", ()))
    if (candidate_set.content_hash != ref or cutoff_ns != candidate_set.envelope.available_at_ns
            or candidate_refs != tuple(sorted(set(candidate_refs)))
            or not set(candidate_refs).issubset(set(candidate_set.envelope.input_refs))
            or len(candidate_refs) != len(candidate_set.candidates)
            or tuple(identity.get("causal_input_refs", ())) != candidate_set.envelope.input_refs
            or tuple(identity.get("causal_input_refs", ())) != tuple(sorted(set(identity.get("causal_input_refs", ()))))
            or identity.get("selection_policy_hash") != candidate_set.selection_policy_hash
            or identity.get("decision_event_id") != candidate_set.decision_event_id
            or sha256_json(identity) != candidate_set.envelope.artifact_id):
        raise ValueError("CandidateSet content or selection identity mismatch")
    indexed_candidates: dict[str, CandidateActionV2] = {}
    for candidate_ref in candidate_refs:
        item = repo.get_artifact(candidate_ref)
        candidate_body = item.metadata.get("candidate") if item is not None else None
        if (item is None or item.artifact_type != "CandidateActionV2" or item.content_hash != candidate_ref
                or not isinstance(candidate_body, Mapping)):
            raise ValueError("CandidateSet references an unavailable typed candidate")
        candidate = CandidateActionV2.from_dict(json_value(candidate_body))
        if (candidate.content_hash != candidate_ref or candidate.candidate_id in indexed_candidates
                or candidate.envelope.available_at_ns > cutoff_ns):
            raise ValueError("CandidateSet candidate identity is duplicate or contradictory")
        indexed_candidates[candidate.candidate_id] = candidate
    if set(indexed_candidates) != {item.candidate_id for item in candidate_set.candidates}:
        raise ValueError("CandidateSet candidate membership is incomplete")
    for candidate_entry in candidate_set.candidates:
        candidate = indexed_candidates[candidate_entry.candidate_id]
        if candidate.key != candidate_entry.key or candidate.side != candidate_entry.side:
            raise ValueError("CandidateSet selection entry disagrees with exact candidate")
    decision_index_ref = sha256_json({"artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": candidate_set.decision_event_id, "universe_ref": candidate_set.universe_ref,
        "selection_policy_hash": candidate_set.selection_policy_hash})
    decision_index = repo.get_artifact(decision_index_ref)
    if (decision_index is None or decision_index.artifact_type != "CandidateSetDecisionIndexV1"
            or decision_index.metadata.get("candidate_set_ref") != ref
            or decision_index.metadata.get("cutoff_ns") != identity.get("cutoff_ns")
            or decision_index.content_hash != sha256_json(decision_index.metadata)
            or decision_index.created_at_ns != identity.get("cutoff_ns")
            or decision_index.available_at_ns != identity.get("cutoff_ns")):
        raise ValueError("CandidateSet lacks its exact immutable decision-event index")
    return candidate_set, identity


def _resolve_decision_calendar_entry(repo: OpsRepository, ref: str) -> DecisionCalendarEntryV2:
    indexed = repo.get_artifact(ref)
    body = indexed.metadata.get("decision_entry") if indexed is not None else None
    if indexed is None or indexed.artifact_type != "DecisionCalendarEntryV2" or not isinstance(body, Mapping):
        raise ValueError("indexed typed DecisionCalendarEntryV2 required")
    entry = DecisionCalendarEntryV2.from_dict(json_value(body))
    identity = repo.get_artifact(entry.decision_identity_ref)
    if (entry.content_hash != ref or indexed.content_hash != ref
            or indexed.created_at_ns != entry.created_at_ns or indexed.available_at_ns != entry.available_at_ns
            or identity is None or identity.artifact_type != "DecisionCalendarIdentityV2"
            or identity.content_hash != ref or identity.metadata.get("decision_ref") != ref):
        raise ValueError("decision calendar identity is missing or contradictory")
    return entry


def index_decision_calendar_entry(repo: OpsRepository, entry: DecisionCalendarEntryV2) -> str:
    candidate_set, identity = _resolve_candidate_set(repo, entry.candidate_set_ref)
    if entry.decision_at_ns != identity.get("cutoff_ns"):
        raise ValueError("decision timestamp must equal CandidateSet cutoff")
    candidate = None
    set_entry = None
    if entry.candidate_ref is not None:
        if entry.candidate_ref not in identity.get("candidate_refs", ()):
            raise ValueError("candidate is absent from exact CandidateSet")
        indexed_candidate = repo.get_artifact(entry.candidate_ref)
        candidate_body = indexed_candidate.metadata.get("candidate") if indexed_candidate is not None else None
        if (indexed_candidate is None or indexed_candidate.artifact_type != "CandidateActionV2"
                or indexed_candidate.content_hash != entry.candidate_ref or not isinstance(candidate_body, Mapping)):
            raise ValueError("typed indexed candidate evidence required")
        candidate = CandidateActionV2.from_dict(json_value(candidate_body))
        if candidate.content_hash != entry.candidate_ref:
            raise ValueError("candidate content hash mismatch")
        set_entry = next((x for x in candidate_set.candidates if x.candidate_id == candidate.candidate_id), None)
        if (set_entry is None or set_entry.key != candidate.key or set_entry.side != candidate.side
                or set_entry.policy_id != entry.policy_id or candidate.policy_hash != entry.policy_hash
                or candidate.decision_at_ns != entry.decision_at_ns):
            raise ValueError("CandidateSet membership or policy identity mismatch")
    elif entry.policy_hash != candidate_set.selection_policy_hash:
        raise ValueError("no-candidate calendar identity must use the exact selection policy")

    state, admission, stage = entry.selection_state, entry.admission_state, entry.source_stage
    if stage == DecisionSourceStageV2.CANDIDATE_SET:
        if (entry.source_artifact_ref != entry.candidate_set_ref or entry.action_hash is not None
                or entry.created_at_ns != candidate_set.envelope.available_at_ns
                or entry.available_at_ns != candidate_set.envelope.available_at_ns):
            raise ValueError("CandidateSet decision source/action mismatch")
        if state == SelectionStateV2.NO_CANDIDATE:
            if candidate is not None or candidate_set.candidates or candidate_set.selection_status != CandidateSelectionStatus.NO_CANDIDATE:
                raise ValueError("NO_CANDIDATE requires an empty exact CandidateSet")
            if admission != AdmissionStateV2.NOT_APPLICABLE:
                raise ValueError("NO_CANDIDATE has no admission state")
        elif state == SelectionStateV2.NOT_ESTIMABLE:
            if (candidate_set.selection_status != CandidateSelectionStatus.NOT_ESTIMABLE
                    or admission != AdmissionStateV2.NOT_APPLICABLE):
                raise ValueError("selection NOT_ESTIMABLE must be established by CandidateSet")
        elif state == SelectionStateV2.SELECTED:
            if (candidate is None or candidate_set.selection_status != CandidateSelectionStatus.SELECTED
                    or candidate_set.selected_candidate_id != candidate.candidate_id
                    or admission != AdmissionStateV2.NOT_EVALUATED):
                raise ValueError("selected state must match CandidateSet and remain unevaluated")
        elif state == SelectionStateV2.UNSELECTED:
            if (candidate is None or candidate_set.selection_status != CandidateSelectionStatus.SELECTED
                    or candidate_set.selected_candidate_id == candidate.candidate_id
                    or set_entry is None or set_entry.eligibility_status != EligibilityStatusV2.ELIGIBLE
                    or admission != AdmissionStateV2.NOT_APPLICABLE):
                raise ValueError("UNSELECTED requires a different selected candidate and eligible membership")
        elif state == SelectionStateV2.REJECTED:
            if (candidate is None or set_entry is None or set_entry.eligibility_status != EligibilityStatusV2.INELIGIBLE
                    or not set_entry.rejection_reason or admission != AdmissionStateV2.NOT_APPLICABLE
                    or set_entry.rejection_reason not in entry.reason_codes
                    or candidate_set.selected_candidate_id == candidate.candidate_id):
                raise ValueError("REJECTED requires typed CandidateSet rejection evidence")
    elif stage == DecisionSourceStageV2.HARD_RISK:
        source = repo.get_artifact(entry.source_artifact_ref)
        body = source.metadata.get("sizing") if source is not None else None
        if (state != SelectionStateV2.SELECTED or candidate is None or source is None
                or source.artifact_type != "SizingDecisionV2" or source.content_hash != entry.source_artifact_ref
                or not isinstance(body, Mapping) or sha256_json(body) != entry.source_artifact_ref
                or source.available_at_ns != entry.available_at_ns
                or body.get("candidate_ref") != entry.candidate_ref
                or body.get("candidate_set_ref") != entry.candidate_set_ref
                or body.get("selected_candidate_id") != candidate.candidate_id
                or body.get("available_at_ns") != source.available_at_ns
                or body.get("version") != SIZING_VERSION
                or body.get("notional_convention") != NOTIONAL_CONVENTION):
            raise ValueError("hard-risk decision is not the exact indexed SizingDecisionV2")
        required_sizing_fields = {"version", "notional_convention", "candidate_ref", "candidate_set_ref",
            "selected_candidate_id", "risk_policy_hash", "risk_policy_v2_hash", "account_snapshot_ref",
            "product_ref", "risk_input_refs", "quantity", "normal_risk", "stress_risk", "notional",
            "margin", "leverage", "notional_reference_price", "rolling_loss_consumed",
            "rolling_new_risk_consumed", "status", "reasons", "available_at_ns"}
        strict_fields(body, expected=required_sizing_fields, required=required_sizing_fields,
                      name="SizingDecisionV2")
        if (not isinstance(body.get("risk_input_refs"), (list, tuple))
                or tuple(body["risk_input_refs"]) != tuple(sorted(set(body["risk_input_refs"])))
                or not isinstance(body.get("reasons"), (list, tuple))):
            raise ValueError("SizingDecisionV2 input/reason lists must be canonical")
        required_risk_refs = {entry.candidate_ref, entry.candidate_set_ref, body.get("account_snapshot_ref"),
                              body.get("product_ref"), body.get("risk_policy_hash"),
                              body.get("risk_policy_v2_hash")}
        if (None in required_risk_refs or not required_risk_refs.issubset(set(body["risk_input_refs"]))):
            raise ValueError("SizingDecisionV2 does not bind its exact policy/candidate/risk inputs")
        for ref in body["risk_input_refs"]:
            input_artifact = repo.get_artifact(ref)
            if input_artifact is None or input_artifact.available_at_ns > source.available_at_ns:
                raise ValueError("SizingDecisionV2 input was unavailable")
        if tuple(body.get("reasons", ())) != entry.reason_codes:
            raise ValueError("decision reason codes disagree with SizingDecisionV2")
        result_fields = ("quantity", "normal_risk", "stress_risk", "notional", "margin", "leverage",
                         "notional_reference_price", "rolling_loss_consumed", "rolling_new_risk_consumed")
        if body.get("status") == "SIZED":
            if any(body.get(field) is None for field in result_fields):
                raise ValueError("SIZED decision must retain every hard-risk result component")
            if _decimal_wire(body["quantity"], "sizing quantity") <= 0:
                raise ValueError("SIZED quantity must be positive")
        elif any(body.get(field) is not None for field in result_fields):
            raise ValueError("non-SIZED hard-risk decision cannot retain a fabricated action quantity/result")
        expected = {"SIZED": AdmissionStateV2.RISK_SIZED.value,
                    "NO_TRADE": AdmissionStateV2.NO_TRADE.value,
                    "NOT_ESTIMABLE": AdmissionStateV2.NOT_ESTIMABLE.value}
        sizing_status = body.get("status")
        if not isinstance(sizing_status, str) or expected.get(sizing_status) != admission.value:
            raise ValueError("hard-risk state disagrees with SizingDecisionV2")
        if admission == AdmissionStateV2.RISK_SIZED:
            if entry.action_hash is None or entry.action_artifact_ref is None:
                raise ValueError("SIZED risk evidence must bind frozen action")
        elif entry.action_hash is not None:
            raise ValueError("hard-risk rejection cannot carry an action")
    elif stage == DecisionSourceStageV2.ECONOMIC_EVALUATION:
        source = repo.get_artifact(entry.source_artifact_ref)
        body = source.metadata.get("evaluation") if source is not None else None
        if (state != SelectionStateV2.SELECTED or candidate is None
                or admission not in (AdmissionStateV2.CANDIDATE, AdmissionStateV2.NO_TRADE,
                                     AdmissionStateV2.NOT_ESTIMABLE)
                or entry.action_hash is None or source is None or source.artifact_type != "EvaluationArtifactV2"
                or source.content_hash != entry.source_artifact_ref or not isinstance(body, Mapping)
                or sha256_json(body) != entry.source_artifact_ref or body.get("version") not in ECONOMIC_EVALUATION_VERSIONS
                or body.get("candidate_set_ref") != entry.candidate_set_ref
                or body.get("candidate_ref") != entry.candidate_ref
                or body.get("action_hash") != entry.action_hash
                or body.get("action_artifact_ref") != entry.action_artifact_ref
                or body.get("policy_hash") != entry.policy_hash
                or tuple(body.get("reason_codes", ())) != entry.reason_codes
                or body.get("decision_at_ns") != entry.decision_at_ns
                or body.get("decision") != admission.value
                or body.get("available_at_ns") != source.available_at_ns
                or source.available_at_ns != entry.available_at_ns):
            raise ValueError("future economic admission requires indexed exact amended EvaluationArtifact")
    elif stage == DecisionSourceStageV2.EXPIRY:
        source = repo.get_artifact(entry.source_artifact_ref)
        body = source.metadata.get("expiry") if source is not None else None
        expiry = CandidateExpiryEvidenceV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
        if (state != SelectionStateV2.SELECTED or candidate is None or admission != AdmissionStateV2.EXPIRED
                or source is None or source.artifact_type != "CandidateExpiryEvidenceV2"
                or source.content_hash != entry.source_artifact_ref or not isinstance(body, Mapping)
                or sha256_json(body) != entry.source_artifact_ref
                or expiry is None or expiry.candidate_set_ref != entry.candidate_set_ref
                or expiry.candidate_ref != entry.candidate_ref or expiry.policy_hash != entry.policy_hash
                or expiry.deadline_ns != candidate.deadline_ns or expiry.reason_code not in entry.reason_codes
                or source.available_at_ns != entry.available_at_ns):
            raise ValueError("expiry state requires exact typed candidate expiry evidence")
    else:
        raise ValueError("unsupported decision evidence stage")

    if entry.action_hash is not None:
        action = repo.get_artifact(entry.action_artifact_ref or "")
        action_body = action.metadata.get("action_artifact") if action is not None else None
        action_identity = action.metadata.get("action_identity") if action is not None else None
        if (action is None or action.artifact_type != "ActionArtifactV2" or not isinstance(action_body, Mapping)
                or not isinstance(action_identity, Mapping) or sha256_json(action_body) != entry.action_artifact_ref
                or sha256_json(action_identity) != entry.action_hash
                or action_body.get("candidate_ref") != entry.candidate_ref
                or action_body.get("candidate_set_ref") != entry.candidate_set_ref
                or action_identity.get("policy_id") != entry.policy_id
                or action_identity.get("policy_version") != entry.policy_version
                or action_identity.get("policy_hash") != entry.policy_hash
                or (entry.source_stage == DecisionSourceStageV2.HARD_RISK
                    and action_body.get("sizing_ref") != entry.source_artifact_ref)
                or action.available_at_ns > entry.available_at_ns):
            raise ValueError("decision entry frozen action identity mismatch")

    existing = repo.get_artifact(entry.decision_identity_ref)
    if existing is not None and (existing.artifact_type != "DecisionCalendarIdentityV2"
            or existing.content_hash != entry.content_hash
            or existing.metadata.get("decision_ref") != entry.content_hash):
        raise ValueError("decision identity already has a contradictory immutable state")
    repo.register_artifact(ArtifactIndexEntryV2(entry.content_hash, "DecisionCalendarEntryV2",
        entry.content_hash, entry.created_at_ns, entry.available_at_ns, {"decision_entry": entry.to_dict()}))
    repo.register_artifact(ArtifactIndexEntryV2(entry.decision_identity_ref, "DecisionCalendarIdentityV2",
        entry.content_hash, entry.created_at_ns, entry.available_at_ns,
        {"version": "DECISION_CALENDAR_IDENTITY_V2_V1", "decision_ref": entry.content_hash}))
    return entry.content_hash


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
    selection_state: SelectionStateV2
    admission_state: AdmissionStateV2
    execution_state: ExecutionOutcomeStateV2
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
        for name, enum in (("selection_state", SelectionStateV2), ("admission_state", AdmissionStateV2),
                           ("execution_state", ExecutionOutcomeStateV2), ("label_state", LabelStateV2),
                           ("provenance", OutcomeProvenanceV2)):
            object.__setattr__(self, name, enum(getattr(self, name)))
        object.__setattr__(self, "outcome_target", OutcomeTargetV2(self.outcome_target))
        if self.candidate_ref is None:
            if self.selection_state not in (SelectionStateV2.NO_CANDIDATE, SelectionStateV2.NOT_ESTIMABLE) or self.action_hash is not None:
                raise ValueError("only NO_CANDIDATE may lack candidate identity")
        elif self.selection_state == SelectionStateV2.NO_CANDIDATE:
            raise ValueError("NO_CANDIDATE cannot claim a candidate")
        if self.action_hash is None:
            if self.action_artifact_ref is not None:
                raise ValueError("absent action cannot claim action artifact")
            nonblank(self.action_absence_reason or "", field="action_absence_reason")
        elif self.action_absence_reason is not None or self.candidate_ref is None or self.action_artifact_ref is None:
            raise ValueError("frozen action identity and absence reason conflict")
        if self.selection_state in (SelectionStateV2.UNSELECTED, SelectionStateV2.REJECTED,
                                    SelectionStateV2.NO_CANDIDATE, SelectionStateV2.NOT_ESTIMABLE) and self.action_hash is not None:
            raise ValueError("unselected/rejected/no-candidate cannot invent a frozen action")
        if self.action_hash is None and self.execution_state != ExecutionOutcomeStateV2.NOT_APPLICABLE:
            raise ValueError("an absent frozen action cannot have an execution outcome")
        if self.action_hash is not None and self.execution_state == ExecutionOutcomeStateV2.NOT_APPLICABLE:
            raise ValueError("an existing frozen action requires a distinct execution state")
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
            if (self.action_hash is not None or self.execution_state != ExecutionOutcomeStateV2.NOT_APPLICABLE
                    or self.execution_evidence_ref is not None or self.actual_closed_source_ref is not None):
                raise ValueError("diagnostic target cannot claim executable outcome or close")
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
            if self.execution_state not in (ExecutionOutcomeStateV2.NO_FILL, ExecutionOutcomeStateV2.PARTIAL_FILL,
                                            ExecutionOutcomeStateV2.FULL_FILL):
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
            if self.execution_state == ExecutionOutcomeStateV2.NO_FILL and self.fill_quantity != 0:
                raise ValueError("no-fill quantity must be zero")
            if self.execution_state == ExecutionOutcomeStateV2.PARTIAL_FILL and not 0 < self.fill_quantity < self.requested_quantity:
                raise ValueError("partial-fill quantity invalid")
            if self.fill_quantity > 0 and self.execution_evidence_ref is None:
                raise ValueError("positive fill requires execution evidence")
            if self.fill_quantity > 0 and self.action_hash is None:
                raise ValueError("candidate without frozen action cannot claim executable fill")
        if self.label_state == LabelStateV2.MATURED and self.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE and self.execution_state in (
                ExecutionOutcomeStateV2.NO_FILL, ExecutionOutcomeStateV2.PARTIAL_FILL, ExecutionOutcomeStateV2.FULL_FILL):
            if self.execution_evidence_ref is None or self.fill_quantity is None or self.requested_quantity is None:
                raise ValueError("matured fill state requires measured execution evidence")
            if self.execution_state == ExecutionOutcomeStateV2.FULL_FILL and self.fill_quantity != self.requested_quantity:
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
            or body["status"] != outcome.execution_state.value
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
    if economics.metadata.get("fill_status") != outcome.execution_state.value:
        raise ValueError("actual fill status mismatch")


def index_diagnostic_target_evidence(repo: OpsRepository, item: DiagnosticTargetEvidenceV2) -> str:
    decision = _resolve_decision_calendar_entry(repo, item.decision_ref)
    if (decision.candidate_set_ref != item.candidate_set_ref or decision.candidate_ref != item.candidate_ref
            or decision.decision_at_ns != item.decision_at_ns):
        raise ValueError("diagnostic target must bind exact decision calendar identity")
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
    decision = _resolve_decision_calendar_entry(repo, outcome.decision_ref)
    if (outcome.candidate_set_ref != decision.candidate_set_ref
            or outcome.candidate_ref != decision.candidate_ref
            or outcome.policy_id != decision.policy_id or outcome.policy_version != decision.policy_version
            or outcome.policy_hash != decision.policy_hash or outcome.decision_at_ns != decision.decision_at_ns
            or outcome.selection_state != decision.selection_state or outcome.admission_state != decision.admission_state
            or outcome.action_hash != decision.action_hash or outcome.action_artifact_ref != decision.action_artifact_ref):
        raise ValueError("outcome decision state/identity must match indexed decision-calendar evidence")
    candidate_set, set_identity = _resolve_candidate_set(repo, outcome.candidate_set_ref)
    if (outcome.decision_at_ns != set_identity.get("cutoff_ns")
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
            and outcome.action_hash is not None and outcome.action_artifact_ref is not None
            and outcome.label_state == LabelStateV2.MATURED
            and outcome.execution_state in (ExecutionOutcomeStateV2.NO_FILL,
                ExecutionOutcomeStateV2.PARTIAL_FILL, ExecutionOutcomeStateV2.FULL_FILL)
            and outcome.execution_evidence_ref is not None and outcome.fill_quantity is not None
            and outcome.requested_quantity is not None and outcome.gross_payoff is not None
            and outcome.fees is not None and outcome.funding_cashflow is not None
            and outcome.net_payoff == outcome.gross_payoff - outcome.fees + outcome.funding_cashflow
            and outcome.available_at_ns <= cutoff_ns)


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
