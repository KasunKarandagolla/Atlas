"""Original population/selection evidence on the existing trusted Ops writer.

Generic artifact bytes cannot attest original observation time. These two
reserved record types are minted by the recorder using its own clock, before
any target is present. Import requires an explicitly trusted original Ops
repository, not a caller-supplied portable certificate. Source/SQL/clock
replacement is outside the existing trusted-writer boundary.

The currently supported evidence grade remains synthetic offline only. Test
clock simulation is not historical or prospective market qualification.
"""
from __future__ import annotations

import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import ClassVar

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.mean_calibration import (
    GRADE,
    MeanCalibrationProtocolV1,
    MeanOriginEvidenceV1,
    MeanOutcomeEvidenceV1,
    SelectedPolicyResidualV2,
)
from atlas.v2.science.mean_common import OfflineArtifact, unique
from atlas.v2.science.mean_models import MeanTrainingRowV1

ORIGINAL_TYPES = ("MeanPopulationRegistrationV1", "MeanPopulationSlotV1", "MeanPopulationOutcomeV1")


def _observed_now_ns():
    return time.time_ns()


def population_contract_ref():
    return sha256_json({"registration": MeanPopulationRegistrationV1.VERSION,
                        "slot": MeanPopulationSlotV1.VERSION,
                        "outcome": MeanPopulationOutcomeV1.VERSION, "grade": GRADE})


def registration_ref(protocol_ref):
    return sha256_json({"original_mean_population": protocol_ref})


def slot_ref(registration, slot_ns):
    return sha256_json({"original_mean_population_slot": registration, "slot_ns": slot_ns})


def outcome_ref(registration, origin_ref):
    return sha256_json({"original_mean_population_outcome": registration, "origin_ref": origin_ref})


@dataclass(frozen=True)
class MeanPopulationRegistrationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_ORIGINAL_MEAN_POPULATION_REGISTRATION_V1"
    protocol_ref: str
    scope_ref: str
    policy_ref: str
    producer_ref: str
    producer_id: str
    # Original due slot, source cutoff and unextendable decision deadline.
    slots: tuple[tuple[int, int, int], ...]
    recorded_at_ns: int
    evidence_grade: str = GRADE

    def __post_init__(self):
        super().__post_init__()
        if (not self.producer_id or not 1 <= len(self.slots) <= 512
                or self.evidence_grade != GRADE or self.slots != tuple(sorted(self.slots))
                or any(not self.recorded_at_ns < s <= c < d for s, c, d in self.slots)):
            raise ValueError("INVALID_ORIGINAL_POPULATION_REGISTRATION")
        unique(tuple(s for s, _, _ in self.slots))


@dataclass(frozen=True)
class MeanPopulationSlotV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_ORIGINAL_MEAN_POPULATION_SLOT_V1"
    registration_ref: str
    scope_ref: str
    policy_ref: str
    producer_ref: str
    slot_ns: int
    source_cutoff_ns: int
    deadline_ns: int
    recorded_at_ns: int
    origin_refs: tuple[str, ...]
    source_prefix_ref: str
    # One disposition for every original candidate, including missing/refused.
    decisions: tuple[tuple[str, str, str], ...]
    slot_status: str
    reason: str
    evidence_grade: str = GRADE

    def __post_init__(self):
        super().__post_init__()
        unique(self.origin_refs)
        missed = self.slot_status == "NOT_EVALUATED_DEADLINE"
        if (self.evidence_grade != GRADE or len(self.origin_refs) > 512 or not self.reason
                or self.slot_status not in ("RECORDED", "NO_CANDIDATE", "UNAVAILABLE",
                                           "NOT_EVALUATED_RESOURCE", "NOT_EVALUATED_DEADLINE", "CENSORED")
                or not self.slot_ns <= self.source_cutoff_ns <= self.recorded_at_ns
                or (missed and self.recorded_at_ns < self.deadline_ns)
                or (not missed and self.recorded_at_ns >= self.deadline_ns)
                or (self.slot_status == "NO_CANDIDATE" and bool(self.origin_refs))
                or (self.slot_status != "RECORDED" and any(s == "SELECTED" for _, s, _ in self.decisions))
                or self.origin_refs != tuple(sorted(self.origin_refs))
                or sum(status == "SELECTED" for _, status, _ in self.decisions) > 1
                or tuple(ref for ref, _, _ in self.decisions) != self.origin_refs
                or any(status not in ("SELECTED", "REJECTED", "UNAVAILABLE", "NOT_EVALUATED_RESOURCE",
                                      "NOT_EVALUATED_DEADLINE", "CENSORED") or not reason
                       for _, status, reason in self.decisions)):
            raise ValueError("INCOMPLETE_ORIGINAL_POPULATION_DECISIONS")


@dataclass(frozen=True)
class MeanPopulationOutcomeV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_ORIGINAL_MEAN_POPULATION_OUTCOME_V1"
    registration_ref: str
    slot_ref: str
    origin_ref: str
    outcome_ref: str
    producer_ref: str
    recorded_at_ns: int


def _entry(ref, obj):
    return ArtifactIndexEntryV2(ref, type(obj).__name__, obj.content_hash,
        obj.recorded_at_ns, obj.recorded_at_ns,
        {"mean_artifact": obj.to_dict(), "authority": "ZERO"})


def _read(repo, ref, cls):
    entry = repo.get_artifact(ref)
    if entry is None or entry.artifact_type != cls.__name__:
        raise ValueError("ORIGINAL_POPULATION_PROOF_UNAVAILABLE")
    obj = cls.from_dict(entry.metadata["mean_artifact"])
    if entry != _entry(ref, obj):
        raise ValueError("ORIGINAL_POPULATION_RECORD_CHANGED")
    return obj


@contextmanager
def _authorized_original_records(repo, entries):
    # Internal sole-writer capability, never serialized or accepted from an
    # artifact caller. The public recorder/importer independently constructs it.
    with repo._lock:
        prior = getattr(repo, "_mean_original_writes", {})
        repo._mean_original_writes = {**prior, **{e.artifact_ref: e for e in entries}}
        try:
            yield
        finally:
            repo._mean_original_writes = prior


def validate_original_registration(repo, entry):
    if getattr(repo, "_mean_original_writes", {}).get(entry.artifact_ref) != entry:
        raise ValueError("ORIGINAL_POPULATION_REQUIRES_TRUSTED_RECORDER_OR_IMPORT")


def _recording_transaction(function):
    @wraps(function)
    def record(repo, **kwargs):
        with repo.atomic_composition():
            return function(repo, **kwargs)
    return record


@_recording_transaction
def record_population_registration(repo, *, protocol, slots, producer_id):
    """Register the entire relevant original schedule before its first slot."""
    from atlas.v2.science.mean_calibration import MeanEvidenceScopeV1
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref
    from atlas.v2.science.mean_evaluation import read_mean_artifact

    now = _observed_now_ns()
    ref = registration_ref(protocol.content_hash)
    if repo.get_artifact(ref) is not None:
        original = _read(repo, ref, MeanPopulationRegistrationV1)
        if (original.slots != tuple(slots) or original.producer_id != producer_id
                or original.producer_ref != validation_implementation_ref()):
            raise ValueError("ORIGINAL_POPULATION_REGISTRATION_REWRITE")
        return original
    stored = read_mean_artifact(repo, protocol.content_hash, MeanCalibrationProtocolV1, consumer_ns=now)
    scope = read_mean_artifact(repo, stored.scope_ref, MeanEvidenceScopeV1, consumer_ns=now)
    if (now > protocol.registered_at_ns or scope.registered_at_ns > now or protocol.end_ns > scope.expires_at_ns
            or any(not protocol.start_ns <= s <= c < d <= min(protocol.end_ns, s + scope.horizon_ns)
                   for s, c, d in slots)):
        raise ValueError("POPULATION_REGISTRATION_NOT_ORIGINAL")
    for entry in repo.artifact_entries(MeanOutcomeEvidenceV1.__name__):
        body = entry.metadata.get("mean_artifact", {})
        if body.get("scope_ref") == scope.content_hash and protocol.start_ns <= body.get("origin_ns", -1) < protocol.end_ns:
            raise ValueError("POPULATION_REGISTRATION_AFTER_OUTCOME_DISCLOSURE")
    obj = MeanPopulationRegistrationV1(protocol.content_hash, scope.content_hash, scope.policy_ref,
        validation_implementation_ref(), producer_id, tuple(slots), now)
    entry = _entry(ref, obj)
    with _authorized_original_records(repo, (entry,)):
        repo.register_artifact(entry)
    return obj


@_recording_transaction
def record_population_slot(repo, *, protocol, slot_ns, decisions, slot_status=None, reason=None):
    """Record actual original dispositions, never infer them from later targets.

    The trusted upstream producer supplies its decision for EACH member; the
    writer independently snapshots all cutoff-known origins in this scope/slot.
    The supported producer contract is explicitly synthetic/offline. Real B
    production integration and real-market source qualification remain gates.
    """
    registration = _read(repo, registration_ref(protocol.content_hash), MeanPopulationRegistrationV1)
    expected = next((x for x in registration.slots if x[0] == slot_ns), None)
    if expected is None:
        raise ValueError("OUTSIDE_REGISTERED_POPULATION_SLOT")
    ref = slot_ref(registration_ref(protocol.content_hash), slot_ns)
    status = slot_status or ("RECORDED" if decisions else "NO_CANDIDATE")
    reason = reason or ("original producer dispositions" if decisions else "no cutoff-known candidate")
    previous = repo.get_artifact(ref)
    if previous is not None:
        obj = _read(repo, ref, MeanPopulationSlotV1)
        if obj.decisions != tuple(sorted(decisions)) or obj.slot_status != status or obj.reason != reason:
            raise ValueError("ORIGINAL_SELECTION_REWRITE")
        return obj  # Exact retry does not manufacture a new recording time.
    now = _observed_now_ns()
    _, cutoff, deadline = expected
    if not cutoff <= now or (now >= deadline) != (status == "NOT_EVALUATED_DEADLINE"):
        raise ValueError("SELECTION_NOT_RECORDED_BEFORE_ORIGINAL_DEADLINE")
    origins = []
    for entry in repo.artifact_entries(MeanOriginEvidenceV1.__name__):
        body = entry.metadata.get("mean_artifact", {})
        if body.get("scope_ref") != registration.scope_ref or body.get("origin_ns") != slot_ns:
            continue
        origin = MeanOriginEvidenceV1.from_dict(entry.metadata["mean_artifact"])
        if origin.scope_ref == registration.scope_ref and origin.origin_ns == slot_ns:
            if entry.content_hash != origin.content_hash or entry.artifact_ref != origin.content_hash:
                raise ValueError("ORIGINAL_SOURCE_IDENTITY_CHANGED")
            if entry.available_at_ns != origin.available_at_ns or origin.available_at_ns > cutoff:
                raise ValueError("ORIGINAL_SOURCE_UNAVAILABLE")
            origins.append(origin)
    refs = tuple(sorted(o.content_hash for o in origins))
    for entry in repo.artifact_entries(MeanOutcomeEvidenceV1.__name__):
        if (entry.metadata.get("mean_artifact", {}).get("action_ref") in refs
                and any(s == "SELECTED" for _, s, _ in decisions)):
            raise ValueError("SELECTION_RECORDED_AFTER_OUTCOME_DISCLOSURE")
    obj = MeanPopulationSlotV1(registration_ref(protocol.content_hash), registration.scope_ref,
        registration.policy_ref, registration.producer_ref, slot_ns, cutoff, deadline, now,
        refs, sha256_json([o.to_dict() for o in sorted(origins, key=lambda o: o.content_hash)]),
        tuple(sorted(decisions)), status, reason)
    with _authorized_original_records(repo, (_entry(ref, obj),)):
        repo.register_artifact(_entry(ref, obj))
    return obj


@_recording_transaction
def record_population_outcome(repo, *, protocol, outcome):
    """Bind the original mature observation once; targets cannot be replaced."""
    from atlas.v2.science.mean_evaluation import read_mean_artifact

    reg_ref = registration_ref(protocol.content_hash)
    reg = _read(repo, reg_ref, MeanPopulationRegistrationV1)
    original_slot_ref = slot_ref(reg_ref, outcome.origin_ns)
    slot = _read(repo, original_slot_ref, MeanPopulationSlotV1)
    if not any(r == outcome.action_ref and status == "SELECTED" for r, status, _ in slot.decisions):
        raise ValueError("OUTCOME_NOT_ORIGINALLY_SELECTED")
    now = _observed_now_ns()
    evidence = read_mean_artifact(repo, outcome.content_hash, MeanOutcomeEvidenceV1, consumer_ns=now)
    origin = read_mean_artifact(repo, outcome.action_ref, MeanOriginEvidenceV1, consumer_ns=slot.source_cutoff_ns)
    if (evidence != outcome or outcome.scope_ref != reg.scope_ref or origin.scope_ref != reg.scope_ref
            or outcome.features != origin.features or outcome.origin_ns != origin.origin_ns
            or now < outcome.available_at_ns or outcome.label_end_ns <= slot.recorded_at_ns):
        raise ValueError("ORIGINAL_OUTCOME_CHRONOLOGY_OR_SOURCE_CHANGED")
    ref = outcome_ref(reg_ref, outcome.action_ref)
    previous = repo.get_artifact(ref)
    if previous is not None:
        obj = _read(repo, ref, MeanPopulationOutcomeV1)
        if obj.outcome_ref != outcome.content_hash:
            raise ValueError("ORIGINAL_MATURE_OUTCOME_REWRITE")
        return obj
    obj = MeanPopulationOutcomeV1(reg_ref, original_slot_ref, origin.content_hash,
                                 outcome.content_hash, reg.producer_ref, now)
    with _authorized_original_records(repo, (_entry(ref, obj),)):
        repo.register_artifact(_entry(ref, obj))
    return obj


def verify_selected_population(repo, protocol, residual_refs, *, cutoff_ns):
    """OFFLINE: independently join the full anchored selection to mature OOF.

    Missing/censored/unavailable slots remain in the denominator. A selected
    action with a missing mature observation blocks support, rather than being
    silently omitted. The statistical algorithm still sees the exact selected
    residual population, not the entire candidate population.
    """
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref
    from atlas.v2.science.mean_evaluation import read_mean_artifact

    reg_ref = registration_ref(protocol.content_hash)
    reg = _read(repo, reg_ref, MeanPopulationRegistrationV1)
    if (reg.protocol_ref != protocol.content_hash or reg.scope_ref != protocol.scope_ref
            or reg.recorded_at_ns > protocol.registered_at_ns
            or reg.producer_ref != validation_implementation_ref()):
        raise ValueError("POPULATION_POLICY_OR_PRODUCER_CHANGED")
    selected = set()
    all_refs = set()
    for slot, cutoff, deadline in reg.slots:
        obj = _read(repo, slot_ref(reg_ref, slot), MeanPopulationSlotV1)
        if ((obj.scope_ref, obj.policy_ref, obj.producer_ref) != (reg.scope_ref, reg.policy_ref, reg.producer_ref)
                or obj.registration_ref != reg_ref
                or (obj.slot_ns, obj.source_cutoff_ns, obj.deadline_ns) != (slot, cutoff, deadline)
                or obj.recorded_at_ns > cutoff_ns):
            raise ValueError("ORIGINAL_POPULATION_BINDING_CHANGED")
        origins = tuple(read_mean_artifact(repo, r, MeanOriginEvidenceV1, consumer_ns=cutoff)
                        for r in obj.origin_refs)
        if (any(o.scope_ref != reg.scope_ref or o.origin_ns != slot for o in origins)
                or sha256_json([o.to_dict() for o in origins]) != obj.source_prefix_ref):
            raise ValueError("ORIGINAL_POPULATION_SOURCE_PREFIX_CHANGED")
        if all_refs.intersection(obj.origin_refs):
            raise ValueError("DUPLICATE_POPULATION_MEMBER")
        all_refs.update(obj.origin_refs)
        selected.update(r for r, status, _ in obj.decisions if status == "SELECTED")
    actual = []
    for ref in residual_refs:
        residual = read_mean_artifact(repo, ref, SelectedPolicyResidualV2, consumer_ns=cutoff_ns)
        row = read_mean_artifact(repo, residual.target_row_ref, MeanTrainingRowV1, consumer_ns=cutoff_ns)
        if row.action_ref not in selected or row.label_available_at_ns >= cutoff_ns:
            raise ValueError("RESIDUAL_NOT_ORIGINALLY_SELECTED_OR_IMMATURE")
        original = _read(repo, outcome_ref(reg_ref, row.action_ref), MeanPopulationOutcomeV1)
        if (original.registration_ref != reg_ref or original.producer_ref != reg.producer_ref
                or original.origin_ref != row.action_ref or original.outcome_ref != row.outcome_ref
                or original.slot_ref != slot_ref(reg_ref, row.origin_ns)
                or not row.label_available_at_ns <= original.recorded_at_ns <= residual.available_at_ns
                or original.recorded_at_ns >= cutoff_ns):
            raise ValueError("SELECTED_OUTCOME_MEMBERSHIP_OR_AVAILABILITY_CHANGED")
        actual.append(row.action_ref)
    unique(tuple(actual))
    if set(actual) != selected or not selected:
        raise ValueError("INCOMPLETE_OR_OUTCOME_CHOSEN_SELECTED_POPULATION")
    return reg


def import_population_evidence(repo, *, trusted_source, entries):
    """Explicit trust of an original Ops owner; never trust a serialized issuer.

    The application chooses/opens this source under the same local-writer trust
    boundary as Ops itself. Installing an arbitrary replacement database or
    mutating private recorder state is outside that boundary. An artifact-only
    import has no such capability and fails closed.
    """
    from atlas.v2.memory.repository import OpsRepository

    if not isinstance(trusted_source, OpsRepository) or trusted_source is repo:
        raise ValueError("EXPLICIT_TRUSTED_ORIGINAL_OPS_SOURCE_REQUIRED")
    batch = tuple(entries)
    originals = tuple(e for e in batch if e.artifact_type in ORIGINAL_TYPES)
    with ExitStack() as locks:
        for owner in sorted((trusted_source, repo), key=id):
            locks.enter_context(owner._lock)
        for entry in originals:
            if trusted_source.get_artifact(entry.artifact_ref) != entry:
                raise ValueError("IMPORTED_ORIGINAL_RECORD_NOT_IN_TRUSTED_SOURCE")
            cls = {"MeanPopulationRegistrationV1": MeanPopulationRegistrationV1,
                   "MeanPopulationSlotV1": MeanPopulationSlotV1,
                   "MeanPopulationOutcomeV1": MeanPopulationOutcomeV1}[entry.artifact_type]
            obj = _read(trusted_source, entry.artifact_ref, cls)
            expected_ref = (registration_ref(obj.protocol_ref) if cls is MeanPopulationRegistrationV1 else
                slot_ref(obj.registration_ref, obj.slot_ns) if cls is MeanPopulationSlotV1 else
                outcome_ref(obj.registration_ref, obj.origin_ref))
            if entry.artifact_ref != expected_ref:
                raise ValueError("IMPORTED_ORIGINAL_RECORD_IDENTITY_CHANGED")
        with _authorized_original_records(repo, originals):
            return repo.register_artifacts(batch)
