"""Immutable observational records for later prospective hidden-shadow measurement."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import json_value, nonblank, sha256_json, sha256_ref, timestamp
from atlas.v2.agent_intelligence.contracts import (
    ACTION_ASSESSMENT_FINDING_TYPES,
    ActionAssessmentRequestV2,
    ActionAssessmentResultV2,
    SealedActionAssessmentPacketV1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1, _receipt_from_dict
from atlas.v2.science.outcomes import DecisionCalendarEntryV2

MAX_OBSERVATION_LATENCY_NS = 120_000_000_000
_TERMINAL_STATUSES = frozenset({"COMPLETE", "REFUSED", "UNAVAILABLE", "INVALID", "EXPIRED", "SKIPPED"})
_NO_PROVIDER_RESULT_TERMINALS = frozenset({
    "DISPATCH_HANDOFF_FAILED", "DISPATCH_OUTCOME_LOST_ON_RESTART",
})


@dataclass(frozen=True)
class ActionCriticShadowObservationV1:
    originating_receipt_ref: str
    decision_calendar_ref: str
    candidate_set_ref: str
    packet_ref: str
    packet_hash: str
    request_id: str
    request_ref: str
    request_hash: str
    action_artifact_ref: str
    action_hash: str
    critic_terminal_status: str
    terminal_reason_code: str | None
    accepted_shadow_evidence: bool
    finding_types: tuple[str, ...]
    dispatch_authorized_at_ns: int | None
    result_received_at_ns: int | None
    dispatch_to_result_latency_ns: int | None
    provider_profile_hash: str
    model_profile_hash: str
    decision_influence: bool
    admission_influence: bool
    deterministic_terminal_status: str
    deterministic_admission_status: str
    recorded_at_ns: int

    VERSION = "ActionCriticShadowObservationV1"

    def __post_init__(self) -> None:
        for name in ("originating_receipt_ref", "decision_calendar_ref", "candidate_set_ref", "packet_ref",
                     "packet_hash", "request_ref", "request_hash", "action_artifact_ref", "action_hash",
                     "provider_profile_hash", "model_profile_hash"):
            sha256_ref(getattr(self, name), field=name)
        nonblank(self.request_id, field="request_id")
        if self.critic_terminal_status not in _TERMINAL_STATUSES:
            raise ValueError("critic observation status is not terminal")
        if self.terminal_reason_code is not None:
            if (not self.terminal_reason_code or len(self.terminal_reason_code) > 96
                    or not self.terminal_reason_code.replace("_", "").isalnum()
                    or self.terminal_reason_code.upper() != self.terminal_reason_code):
                raise ValueError("critic observation reason code is not a bounded enum-like value")
        if type(self.accepted_shadow_evidence) is not bool:
            raise ValueError("accepted shadow evidence must be boolean")
        if self.accepted_shadow_evidence != (self.critic_terminal_status == "COMPLETE"):
            raise ValueError("only a validated COMPLETE critic result is accepted shadow evidence")
        findings = tuple(self.finding_types)
        if len(findings) > 8 or any(item not in ACTION_ASSESSMENT_FINDING_TYPES for item in findings):
            raise ValueError("critic observation findings are outside the closed finding taxonomy")
        if not self.accepted_shadow_evidence and findings:
            raise ValueError("non-accepted critic observation cannot claim finding output")
        object.__setattr__(self, "finding_types", findings)
        for name in ("dispatch_authorized_at_ns", "result_received_at_ns", "recorded_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if (self.dispatch_to_result_latency_ns is None) != (self.dispatch_authorized_at_ns is None
                or self.result_received_at_ns is None):
            raise ValueError("critic observation latency requires both dispatch and result timestamps")
        if self.dispatch_authorized_at_ns is not None:
            if self.dispatch_authorized_at_ns > self.recorded_at_ns:
                raise ValueError("critic dispatch authorization is later than observation recording")
        if self.result_received_at_ns is not None:
            if self.result_received_at_ns > self.recorded_at_ns:
                raise ValueError("critic result receipt is later than observation recording")
        if self.dispatch_to_result_latency_ns is not None:
            actual = self.result_received_at_ns - self.dispatch_authorized_at_ns  # type: ignore[operator]
            if (actual != self.dispatch_to_result_latency_ns or actual < 0
                    or actual > MAX_OBSERVATION_LATENCY_NS):
                raise ValueError("critic observation latency is invalid or exceeds its measurement bound")
        if self.decision_influence is not False or self.admission_influence is not False:
            raise ValueError("critic observations must explicitly have zero decision/admission influence")
        nonblank(self.deterministic_terminal_status, field="deterministic_terminal_status")
        nonblank(self.deterministic_admission_status, field="deterministic_admission_status")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionCriticShadowObservationV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        from atlas.v2._serialization import strict_fields

        row = dict(strict_fields(json_value(data), expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION or not isinstance(row["finding_types"], list):
            raise ValueError("unsupported action-critic shadow observation wire")
        row["finding_types"] = tuple(row["finding_types"])
        return cls(**row)


def request_artifact_ref(request: ActionAssessmentRequestV2) -> str:
    return request.content_hash


def index_packet_and_request(repository: OpsRepository, *, receipt: OpsSupervisorReceiptV1,
                             packet: SealedActionAssessmentPacketV1,
                             request: ActionAssessmentRequestV2) -> None:
    """Make the durable request identity available to later observation/link validation."""
    if (packet.originating_receipt_ref == "" or request.packet_ref != packet.packet_ref
            or request.packet_hash != packet.content_hash or request.action_hash != packet.action_hash
            or packet.original_deadline_d_ns != request.deadline_ns):
        raise ValueError("measurement packet/request binding is invalid")
    receipt_entry = repository.get_artifact(packet.originating_receipt_ref)
    receipt_body = receipt_entry.metadata.get("receipt") if receipt_entry is not None else None
    if (receipt_entry is None or receipt_entry.artifact_type != "OpsSupervisorReceiptV1"
            or receipt_entry.content_hash != receipt.content_hash or not isinstance(receipt_body, Mapping)
            or sha256_json(receipt_body) != receipt.content_hash):
        raise ValueError("measurement packet lacks its exact durable deterministic receipt")
    created = receipt.created_at_ns
    packet_body = packet.to_dict()
    request_body = request.to_dict()
    repository.register_artifacts((
        ArtifactIndexEntryV2(packet.packet_ref, packet.VERSION, packet.content_hash, created, created,
                             {"packet": packet_body}),
        ArtifactIndexEntryV2(request_artifact_ref(request), request.VERSION, request.content_hash, created, created,
                             {"request": request_body}),
    ))


def build_action_critic_shadow_observation(repository: OpsRepository, row: Mapping[str, Any], *,
                                            recorded_at_ns: int) -> ActionCriticShadowObservationV1:
    """Project one terminal critic ledger row onto its exact immutable baseline decision."""
    request = ActionAssessmentRequestV2.from_dict(json.loads(str(row["request_json"])))
    packet = SealedActionAssessmentPacketV1.from_dict(json.loads(str(row["packet_json"])))
    status = str(row["status"])
    if (request.request_id != row["request_id"] or request.content_hash != row["request_hash"]
            or packet.packet_ref != row["packet_ref"] or packet.content_hash != row["packet_hash"]
            or request.packet_ref != packet.packet_ref or request.action_hash != packet.action_hash
            or status not in _TERMINAL_STATUSES):
        raise ValueError("terminal critic row does not bind its immutable request and packet")
    receipt_entry = repository.get_artifact(packet.originating_receipt_ref)
    receipt_body = receipt_entry.metadata.get("receipt") if receipt_entry is not None else None
    if (receipt_entry is None or receipt_entry.artifact_type != "OpsSupervisorReceiptV1"
            or not isinstance(receipt_body, Mapping) or sha256_json(receipt_body) != receipt_entry.content_hash):
        raise ValueError("critic observation requires its exact durable deterministic receipt")
    receipt = _receipt_from_dict(receipt_body)
    if (receipt.content_hash != receipt_entry.content_hash
            or receipt.action_ref != packet.action_artifact_ref
            or receipt.result.terminal_status.value == "FAILED"):
        raise ValueError("critic observation receipt/action binding mismatch")
    candidates: list[tuple[str, DecisionCalendarEntryV2]] = []
    for ref in receipt.calendar_refs:
        indexed = repository.get_artifact(ref)
        body = indexed.metadata.get("decision_entry") if indexed is not None else None
        if indexed is None or indexed.artifact_type != "DecisionCalendarEntryV2" or not isinstance(body, Mapping):
            continue
        entry = DecisionCalendarEntryV2.from_dict(json_value(body))
        if (entry.content_hash == ref and indexed.content_hash == ref
                and entry.action_hash == packet.action_hash
                and entry.action_artifact_ref == packet.action_artifact_ref
                and entry.candidate_set_ref == packet.candidate_set_ref):
            candidates.append((ref, entry))
    if len(candidates) != 1:
        raise ValueError("critic observation requires one exact DecisionCalendar identity")
    decision_ref, decision = candidates[0]
    packet_entry = repository.get_artifact(packet.packet_ref)
    request_entry = repository.get_artifact(request_artifact_ref(request))
    if (packet_entry is None or packet_entry.artifact_type != packet.VERSION
            or packet_entry.content_hash != packet.content_hash
            or json_value(packet_entry.metadata.get("packet")) != packet.to_dict()
            or request_entry is None or request_entry.artifact_type != request.VERSION
            or request_entry.content_hash != request.content_hash
            or json_value(request_entry.metadata.get("request")) != request.to_dict()):
        raise ValueError("critic observation packet/request are not indexed as immutable evidence")
    authorization_body = json.loads(str(row["authorization_json"])) if row.get("authorization_json") else None
    authorized_at = None
    if authorization_body is not None:
        if (not isinstance(authorization_body, Mapping)
                or authorization_body.get("authorization_hash") != row.get("authorization_hash")
                or authorization_body.get("request_hash") != request.content_hash
                or authorization_body.get("packet_hash") != packet.content_hash
                or authorization_body.get("action_hash") != packet.action_hash):
            raise ValueError("critic observation dispatch identity mismatch")
        authorized_at = int(authorization_body["authorized_at_ns"])
    result_received_at = None
    finding_types: tuple[str, ...] = ()
    if row.get("received_at_ns") is not None and row.get("authorization_json") is not None:
        if row.get("failure_code") not in _NO_PROVIDER_RESULT_TERMINALS:
            result_received_at = int(row["received_at_ns"])
    accepted = bool(row.get("eligible"))
    if accepted:
        if status != "COMPLETE" or not isinstance(row.get("result_json"), str):
            raise ValueError("accepted critic evidence lacks its validated complete result")
        result = ActionAssessmentResultV2.from_dict(json.loads(str(row["result_json"])))
        if (result.request_id != request.request_id or result.packet_ref != packet.packet_ref
                or result.packet_hash != packet.content_hash or result.action_hash != packet.action_hash):
            raise ValueError("accepted critic findings do not bind the exact observation action")
        finding_types = tuple(item.finding_type for item in result.findings)
    latency = None
    if authorized_at is not None and result_received_at is not None:
        latency = result_received_at - authorized_at
    reason = row.get("failure_code")
    if reason is not None and (not isinstance(reason, str) or reason.upper() != reason
            or not reason.replace("_", "").isalnum() or len(reason) > 96):
        reason = "PROVIDER_ERROR"
    return ActionCriticShadowObservationV1(
        packet.originating_receipt_ref, decision_ref, decision.candidate_set_ref, packet.packet_ref,
        packet.content_hash, request.request_id, request_artifact_ref(request), request.content_hash,
        packet.action_artifact_ref, packet.action_hash, status, reason, accepted, finding_types,
        authorized_at, result_received_at, latency, request.profile_hash, request.provider_binding_hash,
        False, False, receipt.result.terminal_status.value, decision.admission_state.value, recorded_at_ns)


def index_action_critic_shadow_observation(repository: OpsRepository,
                                           observation: ActionCriticShadowObservationV1) -> str:
    receipt = repository.get_artifact(observation.originating_receipt_ref)
    calendar = repository.get_artifact(observation.decision_calendar_ref)
    packet = repository.get_artifact(observation.packet_ref)
    request = repository.get_artifact(observation.request_ref)
    calendar_body = calendar.metadata.get("decision_entry") if calendar is not None else None
    packet_body = packet.metadata.get("packet") if packet is not None else None
    request_body = request.metadata.get("request") if request is not None else None
    if (receipt is None or receipt.artifact_type != "OpsSupervisorReceiptV1"
            or not isinstance(receipt.metadata.get("receipt"), Mapping)
            or calendar is None or calendar.artifact_type != "DecisionCalendarEntryV2"
            or not isinstance(calendar_body, Mapping)
            or packet is None or packet.artifact_type != SealedActionAssessmentPacketV1.VERSION
            or not isinstance(packet_body, Mapping) or request is None
            or request.artifact_type != ActionAssessmentRequestV2.VERSION or not isinstance(request_body, Mapping)):
        raise ValueError("observation lacks its exact receipt/calendar/packet/request identities")
    receipt_body = receipt.metadata["receipt"]
    typed_receipt = _receipt_from_dict(json_value(receipt_body))
    decision = DecisionCalendarEntryV2.from_dict(json_value(calendar_body))
    typed_packet = SealedActionAssessmentPacketV1.from_dict(json_value(packet_body))
    typed_request = ActionAssessmentRequestV2.from_dict(json_value(request_body))
    if (typed_receipt.content_hash != receipt.content_hash
            or receipt.content_hash != sha256_json(receipt_body)
            or observation.decision_calendar_ref not in typed_receipt.calendar_refs
            or typed_receipt.action_ref != observation.action_artifact_ref
            or typed_receipt.to_dict().get("action_hash") != observation.action_hash
            or decision.content_hash != observation.decision_calendar_ref
            or decision.action_hash != observation.action_hash
            or decision.action_artifact_ref != observation.action_artifact_ref
            or decision.candidate_set_ref != observation.candidate_set_ref
            or typed_packet.packet_ref != observation.packet_ref
            or typed_packet.content_hash != observation.packet_hash
            or typed_packet.originating_receipt_ref != observation.originating_receipt_ref
            or typed_packet.action_hash != observation.action_hash
            or typed_packet.action_artifact_ref != observation.action_artifact_ref
            or typed_request.request_id != observation.request_id
            or typed_request.content_hash != observation.request_hash
            or typed_request.content_hash != observation.request_ref
            or typed_request.packet_ref != typed_packet.packet_ref):
        raise ValueError("observation exact receipt/calendar/action binding mismatch")
    repository.register_artifact(ArtifactIndexEntryV2(observation.content_hash, observation.VERSION,
        observation.content_hash, observation.recorded_at_ns, observation.recorded_at_ns,
        {"observation": observation.to_dict()}))
    return observation.content_hash
