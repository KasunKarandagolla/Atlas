"""Bounded, immutable public-shadow prerequisite publication; no capital authority.

An available artifact records supplied evidence, never qualification inferred from
public socket health. Missing private account/execution facts remain unestimable.
The source cutoff and later processing/publication clock are separate identities.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, cast

from atlas.domain.risk import RiskPolicy
from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json, timestamp
from atlas.v2.contracts import ArtifactEnvelope
from atlas.v2.instruments import ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    AbnormalityEvidenceV2,
    CalendarCoverageV2,
    EventSafetyGateBuilderV2,
    EventSafetyGateV2,
    OperationalIncidentV2,
    ScheduledEventV2,
)
from atlas.v2.risk import AccountRiskSnapshotV2, RiskPolicyV2, StressBoundV2, VenueSizingLimitsV2
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.science.admission import (
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
    validate_venue_capability_snapshot,
)
from atlas.v2.science.costs import FeeScheduleV2
from atlas.v2.science.pretrade import CausalInputV2

PREREQUISITE_VERSION = "PUBLIC_RESEARCH_PREREQUISITES_V1"
PREREQUISITE_TYPE = "ResearchPrerequisiteInventoryV1"
MAX_EVENT_INPUTS = 64
MAX_SOURCE_REFS = 128
_TYPES: dict[str, type[Any]] = {
    "POLICY_V1": RiskPolicy,
    "POLICY_V2": RiskPolicyV2,
    "ACCOUNT": AccountRiskSnapshotV2,
    "FEE": FeeScheduleV2,
    "STRESS": StressBoundV2,
    "VENUE_SIZING": VenueSizingLimitsV2,
    "VENUE_CAPABILITY": VenueCapabilitySnapshotV2,
    "EXECUTION_MODEL": CausalInputV2,
}
_MISSING = {
    "POLICY_V1": "EXACT_RISK_POLICY_V1_UNAVAILABLE",
    "POLICY_V2": "EXACT_RISK_POLICY_V2_UNAVAILABLE",
    "ACCOUNT": "AUTHENTICATED_ACCOUNT_EVIDENCE_UNAVAILABLE_IN_PUBLIC_SHADOW",
    "FEE": "EXACT_ACCOUNT_FEE_EVIDENCE_UNAVAILABLE",
    "STRESS": "EXECUTABLE_STRESS_BOUND_UNSUPPORTED",
    "VENUE_SIZING": "EXACT_ACCOUNT_VENUE_SIZING_LIMITS_UNAVAILABLE",
    "VENUE_CAPABILITY": "AUTHENTICATED_VENUE_EXECUTION_QUALIFICATION_UNAVAILABLE",
    "EXECUTION_MODEL": "CAUSAL_EXECUTION_MODEL_EVIDENCE_UNAVAILABLE",
}


@dataclass(frozen=True)
class ResearchPrerequisitePublicationV1:
    inventory_ref: str
    event_gate_ref: str
    available_at_ns: int
    status: str
    consumer_eligible: bool
    missing_reasons: tuple[str, ...]
    prerequisite_statuses: FrozenMap

    @property
    def refs(self) -> tuple[str, ...]:
        return tuple(sorted((self.inventory_ref, self.event_gate_ref)))


class _GateCapture:
    """Stage cutoff-visible typed inputs until the publication commits atomically."""

    def __init__(self, repository: OpsRepository) -> None:
        self.repository = repository
        self.entries: dict[str, ArtifactIndexEntryV2] = {}

    def get_artifact(self, ref: str) -> ArtifactIndexEntryV2 | None:
        return self.entries.get(ref) or self.repository.get_artifact(ref)

    def register_artifact(self, entry: ArtifactIndexEntryV2) -> None:
        if entry.artifact_type != EventSafetyGateV2.ARTIFACT_TYPE:
            prior = self.entries.get(entry.artifact_ref)
            if prior is not None and prior != entry:
                raise ValueError("event prerequisite repeats conflicting immutable evidence")
            self.entries[entry.artifact_ref] = entry


def prerequisite_identity_ref(event_id: str, product_ref: str) -> str:
    return sha256_json({"version": PREREQUISITE_VERSION, "event_id": event_id, "product_ref": product_ref})


def _source(repository: OpsRepository, ref: str, at_ns: int) -> ArtifactIndexEntryV2:
    entry = repository.get_artifact(ref)
    if entry is None or entry.content_hash != ref or entry.available_at_ns > at_ns:
        raise ValueError("prerequisite source is absent, invalid or unavailable")
    return entry


def _supplied(repository: OpsRepository, role: str, item: Any, product: ProductContractV2,
              cutoff_ns: int) -> tuple[dict[str, Any], tuple[str, ...]]:
    if type(item) is not _TYPES[role]:
        raise ValueError("prerequisite role requires its exact typed evidence")
    if isinstance(item, CausalInputV2):
        if item.kind != "ExecutionModelV1":
            raise ValueError("execution prerequisite requires the declared execution model type")
        ref, kind, at = item.ref, item.kind, item.available_at_ns
        entry = _source(repository, ref, at)
        if entry.artifact_type != kind or sha256_json(entry.metadata) != ref:
            raise ValueError("execution prerequisite source content/type mismatch")
        sources: tuple[str, ...] = ()
    else:
        ref = item.policy_hash() if isinstance(item, RiskPolicy) else (
            item.policy_hash if isinstance(item, RiskPolicyV2) else item.content_hash)
        kind = "RiskPolicyV1" if isinstance(item, RiskPolicy) else type(item).__name__
        at = item.policy_effective_at_ns if isinstance(item, RiskPolicy) else (
            item.effective_at_ns if isinstance(item, RiskPolicyV2) else item.available_at_ns)
        entry = _source(repository, ref, at)
        expected = {"policy": item.to_dict()} if isinstance(item, (RiskPolicy, RiskPolicyV2)) else (
            {"capability": item.to_dict()} if isinstance(item, VenueCapabilitySnapshotV2) else item.to_dict())
        if entry.artifact_type != kind or canonical_json(entry.metadata) != canonical_json(expected):
            raise ValueError("prerequisite indexed content/type mismatch")
        if isinstance(item, AccountRiskSnapshotV2):
            sources = (item.exposure_completeness_ref, *item.closed_outcome_refs,
                       *item.pending_risk_refs, *item.existing_exposure_refs)
        elif isinstance(item, VenueCapabilitySnapshotV2):
            sources = item.evidence_refs
        elif isinstance(item, (FeeScheduleV2, StressBoundV2, VenueSizingLimitsV2)):
            sources = (item.source_ref,)
        else:
            sources = ()
    if len(sources) > MAX_SOURCE_REFS:
        raise ValueError("prerequisite source references exceed the bounded publication budget")
    if entry.available_at_ns != at:
        raise ValueError("prerequisite indexed availability mismatch")
    for source_ref in sources:
        _source(repository, source_ref, at)
    if hasattr(item, "key") and item.key != product.key:
        raise ValueError("prerequisite instrument identity mismatch")
    if hasattr(item, "product_ref") and item.product_ref != product.content_hash:
        raise ValueError("prerequisite product identity mismatch")
    if isinstance(item, VenueCapabilitySnapshotV2) and (
            item.instrument_key_ref != product.key.content_hash or item.venue != product.key.venue
            or item.environment != product.key.environment):
        raise ValueError("prerequisite venue capability scope mismatch")
    if isinstance(item, VenueCapabilitySnapshotV2):
        validate_venue_capability_snapshot(repository, item, cutoff_ns=cutoff_ns)
    reasons: list[str] = []
    if at > cutoff_ns:
        raise ValueError("prerequisite raw evidence is unavailable at information cutoff")
    if isinstance(item, AccountRiskSnapshotV2) and item.operational_status != "CURRENT":
        reasons.append("ACCOUNT_EVIDENCE_NOT_CURRENT")
    if isinstance(item, VenueCapabilitySnapshotV2) and (
            item.observed_status != VenueCapabilityStatusV2.SUPPORTED or item.synthetic_fixture):
        reasons.append("VENUE_CAPABILITY_UNQUALIFIED_OR_SYNTHETIC")
    return ({"status": "NOT_ESTIMABLE" if reasons else "AVAILABLE", "evidence_ref": ref,
             "artifact_type": kind, "available_at_ns": at, "reasons": reasons,
             "source_refs": sorted(set(sources))}, tuple(sorted({ref, *sources})))


def _event_sources(repository: OpsRepository, *, cutoff_ns: int, coverage: CalendarCoverageV2 | None,
                   scheduled_events: Sequence[ScheduledEventV2], abnormality: AbnormalityEvidenceV2 | None,
                   incidents: Sequence[OperationalIncidentV2]) -> tuple[str, ...]:
    if len(scheduled_events) + len(incidents) > MAX_EVENT_INPUTS:
        raise ValueError("event prerequisites exceed the bounded publication budget")
    refs: set[str] = set()
    typed_inputs = ((CalendarCoverageV2, coverage), (AbnormalityEvidenceV2, abnormality),
        *((ScheduledEventV2, item) for item in scheduled_events),
        *((OperationalIncidentV2, item) for item in incidents))
    for kind, item in typed_inputs:
        if item is None:
            continue
        if type(item) is not kind:
            raise ValueError("event prerequisite requires its exact typed evidence")
        if item.available_at_ns > cutoff_ns:
            raise ValueError("event prerequisite source is unavailable at information cutoff")
        _source(repository, item.evidence_ref, item.available_at_ns)
        refs.add(item.evidence_ref)
        if isinstance(item, OperationalIncidentV2) and item.resolved_by_ref:
            _source(repository, item.resolved_by_ref, item.available_at_ns)
            refs.add(item.resolved_by_ref)
    return tuple(sorted(refs))


def _publication(entry: ArtifactIndexEntryV2) -> ResearchPrerequisitePublicationV1:
    body = entry.metadata.get("prerequisites")
    if (not isinstance(body, Mapping) or body.get("version") != PREREQUISITE_VERSION
            or sha256_json(body) != entry.content_hash
            or entry.artifact_type != PREREQUISITE_TYPE
            or body.get("computation_started_ns") != entry.created_at_ns
            or body.get("available_at_ns") != entry.available_at_ns
            or body.get("authority") != "ZERO"
            or body.get("capital_enabled") is not False
            or body.get("assisted_execution_enabled") is not False):
        raise ValueError("prerequisite inventory content or chronology is invalid")
    if not (body["information_cutoff_ns"] <= body["computation_started_ns"]
            <= body["computation_finished_ns"] <= body["available_at_ns"]):
        raise ValueError("prerequisite inventory chronology is invalid")
    if body["consumer_eligible"] != (body["available_at_ns"] < body["deadline_ns"]):
        raise ValueError("prerequisite inventory deadline eligibility is invalid")
    return ResearchPrerequisitePublicationV1(entry.artifact_ref, str(body["event_gate_ref"]),
        entry.available_at_ns, str(body["status"]), bool(body["consumer_eligible"]),
        tuple(body["missing_reasons"]), FrozenMap(body["prerequisites"]))


def publish_research_prerequisites(repository: OpsRepository, *, event: OpsDecisionEventV1,
        product: ProductContractV2, clock_ns: Callable[[], int] = time.time_ns,
        evidence: Mapping[str, Any] | None = None, coverage: CalendarCoverageV2 | None = None,
        scheduled_events: Sequence[ScheduledEventV2] = (), abnormality: AbnormalityEvidenceV2 | None = None,
        incidents: Sequence[OperationalIncidentV2] = ()) -> ResearchPrerequisitePublicationV1:
    """Publish once for an exact event/product; supplied facts must already exist.

    ``AVAILABLE`` means exact supplied evidence is present, not capital admission.
    No implicit policy, fee, balance, fill, calendar or leverage default exists.
    Late computation is retained and explicitly unavailable to a decision consumer.
    """
    supplied = dict(evidence or {})
    if set(supplied) - set(_TYPES):
        raise ValueError("unknown research prerequisite role")
    product_entry = _source(repository, product.content_hash, product.available_at_ns)
    if (product_entry.artifact_type != "ProductContractV2"
            or canonical_json(product_entry.metadata.get("product")) != canonical_json(product.to_dict())
            or product.available_at_ns > event.information_cutoff_ns
            or product.effective_at_ns > event.information_cutoff_ns):
        raise ValueError("research prerequisite exact product is invalid or future")
    event_refs = _event_sources(repository, cutoff_ns=event.information_cutoff_ns,
        coverage=coverage, scheduled_events=scheduled_events, abnormality=abnormality, incidents=incidents)
    metadata = repository.get_artifact(product.metadata_ref)
    metadata_valid = bool(metadata is not None and metadata.content_hash == product.metadata_ref
                          and metadata.available_at_ns <= product.available_at_ns)
    statuses: dict[str, Any] = {"PRODUCT": {"status": "AVAILABLE" if metadata_valid else "NOT_ESTIMABLE",
        "evidence_ref": product.content_hash,
        "artifact_type": "ProductContractV2", "available_at_ns": product.available_at_ns,
        "reasons": [] if metadata_valid else ["EXACT_PRODUCT_METADATA_SOURCE_UNAVAILABLE"],
        "source_refs": [product.metadata_ref] if metadata_valid else []}}
    input_refs = {product.content_hash}
    if metadata_valid:
        input_refs.add(product.metadata_ref)
    for role in _TYPES:
        if role in supplied:
            statuses[role], refs = _supplied(repository, role, supplied[role], product, event.information_cutoff_ns)
            input_refs.update(refs)
        else:
            statuses[role] = {"status": "NOT_ESTIMABLE", "evidence_ref": None,
                "artifact_type": "RiskPolicyV1" if role == "POLICY_V1" else (
                    "ExecutionModelV1" if role == "EXECUTION_MODEL" else _TYPES[role].__name__),
                "available_at_ns": None,
                "reasons": [_MISSING[role]], "source_refs": []}
    if "POLICY_V1" in supplied and "POLICY_V2" in supplied and (
            supplied["POLICY_V1"].policy_hash() != supplied["POLICY_V2"].base_v1_risk_policy_hash):
        raise ValueError("research prerequisite V1/V2 risk policy binding mismatch")
    if "ACCOUNT" in supplied and "VENUE_CAPABILITY" in supplied and (
            supplied["ACCOUNT"].account_scope != supplied["VENUE_CAPABILITY"].account_scope):
        raise ValueError("research prerequisite account/capability scope mismatch")
    manifest = {"event_id": event.event_id, "event_ref": event.content_hash,
        "product_ref": product.content_hash,
        "information_cutoff_ns": event.information_cutoff_ns, "deadline_ns": event.deadline_ns,
        "supplied_prerequisites": statuses, "coverage": coverage.to_dict() if coverage else None,
        "scheduled_events": [item.to_dict() for item in scheduled_events],
        "abnormality": abnormality.to_dict() if abnormality else None,
        "incidents": [item.to_dict() for item in incidents]}
    manifest_hash = sha256_json(manifest)
    identity_ref = prerequisite_identity_ref(event.event_id, product.content_hash)
    prior = repository.get_artifact(identity_ref)
    if prior is not None:
        publication = _publication(prior)
        if prior.metadata["prerequisites"].get("input_manifest_hash") != manifest_hash:
            raise ValueError("research prerequisites cannot revise an already observed opportunity")
        prior_gate = repository.get_artifact(publication.event_gate_ref)
        if (prior_gate is None or prior_gate.artifact_type != "EventSafetyGateV2"
                or prior_gate.artifact_ref != prior_gate.content_hash
                or prior_gate.available_at_ns != publication.available_at_ns):
            raise ValueError("sealed research prerequisite gate is unavailable")
        wire = prior_gate.metadata.get("gate")
        if not isinstance(wire, Mapping) or not isinstance(wire.get("envelope"), Mapping):
            raise ValueError("sealed research prerequisite gate is invalid")
        envelope = ArtifactEnvelope.from_dict(json_value(wire["envelope"]))
        preimage = {**wire, "envelope": envelope.to_dict(include_hash=False)}
        if (sha256_json({"artifact_type": "EventSafetyGateV2", "artifact": preimage}) != prior_gate.content_hash
                or envelope.content_hash != prior_gate.artifact_ref or envelope.created_at_ns != prior_gate.created_at_ns
                or envelope.available_at_ns != prior_gate.available_at_ns
                or wire.get("cutoff_ns") != event.information_cutoff_ns):
            raise ValueError("sealed research prerequisite gate content/chronology mismatch")
        for source_ref in prior.metadata["prerequisites"]["input_refs"]:
            _source(repository, source_ref, event.information_cutoff_ns)
        return publication
    if len(input_refs | set(event_refs)) > MAX_SOURCE_REFS:
        raise ValueError("prerequisite input references exceed the bounded publication budget")
    started_ns = timestamp(clock_ns(), field="prerequisite computation start")
    if started_ns < event.information_cutoff_ns:
        raise ValueError("research prerequisite computation precedes information cutoff")
    capture = _GateCapture(repository)
    draft = EventSafetyGateBuilderV2(cast(OpsRepository, capture)).evaluate(
        key=product.key, cutoff_ns=event.information_cutoff_ns, coverage=coverage,
        scheduled_events=scheduled_events, abnormality=abnormality, incidents=incidents)
    finished_ns = timestamp(clock_ns(), field="prerequisite computation finish")
    available_ns = timestamp(clock_ns(), field="prerequisite publication")
    if not started_ns <= finished_ns <= available_ns:
        raise ValueError("research prerequisite processing clock regressed")
    gate = replace(draft, envelope=ArtifactEnvelope(1,
        sha256_json({"event_id": event.event_id, "product_ref": product.content_hash, "role": "EVENT"}),
        started_ns, available_ns, PREREQUISITE_VERSION,
        tuple(sorted(set(draft.envelope.input_refs) | set(event_refs)))))
    gate_entry = ArtifactIndexEntryV2(gate.content_hash, gate.ARTIFACT_TYPE, gate.content_hash,
        started_ns, available_ns, {"gate": gate.to_dict(), "source_event_refs": list(gate.envelope.input_refs)})
    event_reasons = list(gate.reasons)
    statuses["EVENT"] = {"status": "AVAILABLE" if gate.state.value == "CLEAR" else "NOT_ESTIMABLE",
        "evidence_ref": gate.content_hash, "artifact_type": gate.ARTIFACT_TYPE,
        "available_at_ns": available_ns, "state": gate.state.value,
        "reasons": event_reasons or (["EVENT_SAFETY_NOT_CLEAR"] if gate.state.value != "CLEAR" else []),
        "source_refs": list(gate.envelope.input_refs)}
    input_refs.update((*event_refs, *gate.envelope.input_refs))
    if len(input_refs) > MAX_SOURCE_REFS:
        raise ValueError("prerequisite input references exceed the bounded publication budget")
    missing = sorted({reason for value in statuses.values() for reason in value["reasons"]})
    eligible = available_ns < event.deadline_ns
    if not eligible:
        missing.append("PREREQUISITE_PUBLICATION_AFTER_CONSUMER_DEADLINE")
    body = {"version": PREREQUISITE_VERSION, "event_id": event.event_id,
        "product_ref": product.content_hash, "information_cutoff_ns": event.information_cutoff_ns,
        "deadline_ns": event.deadline_ns, "input_manifest_hash": manifest_hash,
        "input_refs": sorted(input_refs), "event_gate_ref": gate.content_hash,
        "computation_started_ns": started_ns, "computation_finished_ns": finished_ns,
        "available_at_ns": available_ns, "consumer_eligible": eligible,
        "status": "NOT_ESTIMABLE" if missing else "AVAILABLE", "missing_reasons": sorted(missing),
        "prerequisites": statuses, "authority": "ZERO", "capital_enabled": False,
        "assisted_execution_enabled": False, "execution_evidence": "NOT_ESTIMABLE_PUBLIC_SHADOW"}
    entry = ArtifactIndexEntryV2(identity_ref, PREREQUISITE_TYPE, sha256_json(body),
        started_ns, available_ns, {"prerequisites": body})
    repository.register_artifacts((*capture.entries.values(), gate_entry, entry))
    return _publication(entry)
