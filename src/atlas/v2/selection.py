"""Immutable research-only CandidateSet assembly and untuned scanner-rank selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from atlas.v2._serialization import canonical_json, sha256_json, sha256_ref
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    CandidateSelectionStatus,
    CandidateSetEntryV2,
    CandidateSetV2,
    EligibilityStatusV2,
    PolicySpecV2,
)
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

SELECTION_POLICY_ID = "S1_S2_SCANNER_RANK_V1"
PRODUCER_VERSION = "S1_S2_CANDIDATE_PIPELINE_V1"
RANK_MAX_AGE_NS = 5_000_000_000
ORDERING = ("scanner_rank_ascending", "policy_id_ascending",
            "canonical_InstrumentKeyV2_ascending", "candidate_id_ascending")
SELECTION_POLICY_BODY = {
    "selection_policy_id": SELECTION_POLICY_ID, "version": "1.0.0-shadow",
    "capital_status": "SHADOW_ONLY", "ordering": list(ORDERING),
    "scanner_evidence_max_age_ns": RANK_MAX_AGE_NS,
    "missing_or_contradictory_evidence": "NOT_ESTIMABLE",
    "all_generated_candidates_retained": True,
}
SELECTION_POLICY_HASH = sha256_json(SELECTION_POLICY_BODY)
TIE_BREAK_RULE = " > ".join(ORDERING)


@dataclass(frozen=True)
class ScannerRankEvidenceV1:
    candidate_id: str
    scanner_rank: int
    scanner_policy_id: str
    scanner_policy_version: str
    universe_ref: str
    decision_event_id: str
    available_at_ns: int
    source_artifact_ref: str

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.scanner_policy_id or not self.scanner_policy_version:
            raise ValueError("scanner evidence identity is required")
        if type(self.scanner_rank) is not int or self.scanner_rank < 1:
            raise ValueError("scanner rank must be a positive integer")
        if not self.decision_event_id:
            raise ValueError("scanner decision event is required")
        sha256_ref(self.universe_ref, field="universe_ref")
        sha256_ref(self.source_artifact_ref, field="source_artifact_ref")
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("scanner evidence availability must be UTC nanoseconds")

    def to_dict(self) -> dict[str, object]:
        return {
            "version": "SCANNER_RANK_EVIDENCE_V1", "candidate_id": self.candidate_id,
            "scanner_rank": self.scanner_rank, "scanner_policy_id": self.scanner_policy_id,
            "scanner_policy_version": self.scanner_policy_version, "universe_ref": self.universe_ref,
            "decision_event_id": self.decision_event_id, "available_at_ns": self.available_at_ns,
            "source_artifact_ref": self.source_artifact_ref,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def register_scanner_rank(repository: OpsRepository, evidence: ScannerRankEvidenceV1) -> str:
    """Index an immutable scanner-stage observation, never a naked rank."""
    ref = evidence.content_hash
    repository.register_artifact(ArtifactIndexEntryV2(ref, "ScannerRankEvidenceV1", ref,
        evidence.available_at_ns, evidence.available_at_ns, evidence.to_dict()))
    return ref


def _candidate_index(repository: OpsRepository, candidate: CandidateActionV2, policy: PolicySpecV2,
                     universe: UniverseContractV2, cutoff_ns: int) -> ArtifactIndexEntryV2:
    entry = repository.get_artifact(candidate.content_hash)
    if entry is None or entry.artifact_type != "CandidateActionV2" or entry.content_hash != candidate.content_hash:
        raise ValueError("candidate must already be durably indexed")
    if canonical_json(entry.metadata.get("candidate")) != candidate.to_canonical_json():
        raise ValueError("indexed candidate content mismatch")
    if candidate.policy_hash != policy.policy_hash or policy.policy_id not in (
        "S1_MTF_TREND_PULLBACK", "S2_COMPRESSION_BREAKOUT"):
        raise ValueError("candidate policy identity mismatch")
    if candidate.quantity is not None:
        raise ValueError("research selection requires unsized candidate")
    if candidate.deadline_ns < cutoff_ns:
        raise ValueError("expired candidate cannot enter CandidateSet")
    if candidate.decision_at_ns != cutoff_ns or candidate.envelope.available_at_ns > cutoff_ns:
        raise ValueError("candidate decision event or causal cutoff mismatch")
    if entry.available_at_ns > cutoff_ns:
        raise ValueError("candidate artifact unavailable at cutoff")
    if not any(item.key == candidate.key for item in universe.entries):
        raise ValueError("candidate full instrument key absent from universe")
    if "universe_ref" in entry.metadata and entry.metadata["universe_ref"] != universe.content_hash:
        raise ValueError("candidate universe reference mismatch")
    return entry


def assemble_candidate_set(repository: OpsRepository, *, universe: UniverseContractV2,
                           decision_event_id: str, cutoff_ns: int,
                           candidates: Sequence[CandidateActionV2],
                           policies: Mapping[str, PolicySpecV2],
                           scanner_evidence_refs: Mapping[str, Sequence[str]]) -> CandidateSetV2:
    """Persist every generated candidate, then select only on causal scanner evidence."""
    if not decision_event_id or type(cutoff_ns) is not int or cutoff_ns < 0:
        raise ValueError("decision event and cutoff required")
    if universe.selection_policy_hash != SELECTION_POLICY_HASH:
        raise ValueError("universe selection policy identity mismatch")
    if universe.envelope.available_at_ns > cutoff_ns or cutoff_ns > universe.decision_slot_ns:
        raise ValueError("universe unavailable at selection cutoff")
    ids = [candidate.candidate_id for candidate in candidates]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate generated candidate")
    if set(scanner_evidence_refs) - set(ids):
        raise ValueError("scanner evidence refers to absent candidate")
    ordered = sorted(candidates, key=lambda candidate: candidate.candidate_id)
    for candidate in ordered:
        policy = policies.get(candidate.policy_hash)
        if policy is None:
            raise ValueError("candidate policy spec missing")
        _candidate_index(repository, candidate, policy, universe, cutoff_ns)
    repository.register_artifact(ArtifactIndexEntryV2(SELECTION_POLICY_HASH, "SelectionPolicyV1",
        SELECTION_POLICY_HASH, 0, 0, SELECTION_POLICY_BODY))
    repository.register_artifact(ArtifactIndexEntryV2(universe.content_hash, "UniverseContractV2",
        universe.content_hash, universe.envelope.created_at_ns, universe.envelope.available_at_ns,
        {"universe": universe.to_dict()}))
    ranks: dict[str, int] = {}
    reasons: dict[str, str] = {}
    statuses: dict[str, EligibilityStatusV2] = {}
    all_refs: set[str] = {SELECTION_POLICY_HASH, universe.content_hash}
    uncertain = False
    for candidate in ordered:
        all_refs.add(candidate.content_hash)
        universe_entry = next(item for item in universe.entries if item.key == candidate.key)
        policy = policies[candidate.policy_hash]
        strategy_status = universe_entry.strategy_eligibility.get(policy.policy_id)
        if not universe_entry.data_eligible or not universe_entry.scanner_eligible or (
            strategy_status is not None and strategy_status.status == EligibilityStatusV2.INELIGIBLE
        ):
            statuses[candidate.candidate_id] = EligibilityStatusV2.INELIGIBLE
            reasons[candidate.candidate_id] = (strategy_status.reason if strategy_status is not None and
                strategy_status.status == EligibilityStatusV2.INELIGIBLE else "UNIVERSE_INELIGIBLE") or "UNIVERSE_INELIGIBLE"
            continue
        if strategy_status is None or strategy_status.status == EligibilityStatusV2.NOT_ESTIMABLE:
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "STRATEGY_ELIGIBILITY_NOT_ESTIMABLE"
            uncertain = True
            continue
        refs = tuple(scanner_evidence_refs.get(candidate.candidate_id, ()))
        all_refs.update(refs)
        if len(refs) != 1:
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "SCANNER_EVIDENCE_MISSING_OR_CONTRADICTORY"
            uncertain = True
            continue
        ref = refs[0]
        evidence = repository.get_artifact(ref)
        if (evidence is None or evidence.artifact_type != "ScannerRankEvidenceV1"
                or evidence.content_hash != ref or evidence.metadata.get("candidate_id") != candidate.candidate_id
                or evidence.metadata.get("universe_ref") != universe.content_hash
                or evidence.metadata.get("decision_event_id") != decision_event_id
                or not evidence.metadata.get("scanner_policy_id")
                or not evidence.metadata.get("scanner_policy_version")
                or not evidence.metadata.get("source_artifact_ref")
                or evidence.metadata.get("available_at_ns") != evidence.available_at_ns
                or evidence.available_at_ns > cutoff_ns
                or cutoff_ns - evidence.available_at_ns > RANK_MAX_AGE_NS
                or type(evidence.metadata.get("scanner_rank")) is not int
                or evidence.metadata["scanner_rank"] < 1
                or sha256_json(evidence.metadata) != ref):
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "SCANNER_EVIDENCE_UNAVAILABLE_STALE_OR_CONTRADICTORY"
            uncertain = True
            continue
        statuses[candidate.candidate_id] = EligibilityStatusV2.ELIGIBLE
        ranks[candidate.candidate_id] = int(evidence.metadata["scanner_rank"])
    ranking = sorted((candidate for candidate in ordered if candidate.candidate_id in ranks),
        key=lambda candidate: (ranks[candidate.candidate_id], policies[candidate.policy_hash].policy_id,
            candidate.key.to_canonical_json(), candidate.candidate_id))
    rank_positions = {candidate.candidate_id: i + 1 for i, candidate in enumerate(ranking)}
    entries = tuple(CandidateSetEntryV2(candidate.candidate_id, policies[candidate.policy_hash].policy_id,
        candidate.key, candidate.side, tuple(sorted(set(scanner_evidence_refs.get(candidate.candidate_id, ())))),
        statuses[candidate.candidate_id], rank_positions.get(candidate.candidate_id),
        "SCANNER_RANK_POLICY_KEY_CANDIDATE_ID" if candidate.candidate_id in rank_positions else None,
        reasons.get(candidate.candidate_id)) for candidate in ordered)
    if uncertain:
        selection_status = CandidateSelectionStatus.NOT_ESTIMABLE
        selected_id = None
    elif ranking:
        selection_status = CandidateSelectionStatus.SELECTED
        selected_id = ranking[0].candidate_id
    else:
        selection_status = CandidateSelectionStatus.NO_CANDIDATE
        selected_id = None
    all_refs.update(ref for refs in scanner_evidence_refs.values() for ref in refs)
    identity = {"decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": SELECTION_POLICY_HASH, "candidate_refs": sorted(candidate.content_hash for candidate in ordered),
        "selection_evidence_refs": sorted(all_refs - {SELECTION_POLICY_HASH, universe.content_hash} -
            {candidate.content_hash for candidate in ordered}),
        "cutoff_ns": cutoff_ns, "producer_version": PRODUCER_VERSION}
    artifact_id = sha256_json(identity)
    envelope = ArtifactEnvelope(1, artifact_id, cutoff_ns, cutoff_ns, PRODUCER_VERSION, tuple(sorted(all_refs)))
    result = CandidateSetV2(envelope, decision_event_id, universe.content_hash, SELECTION_POLICY_HASH,
                            entries, selected_id, TIE_BREAK_RULE, selection_status)
    decision_index_ref = sha256_json({"artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": SELECTION_POLICY_HASH})
    decision_index_body = {"decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": SELECTION_POLICY_HASH, "cutoff_ns": cutoff_ns,
        "candidate_set_ref": result.content_hash}
    existing_index = repository.get_artifact(decision_index_ref)
    if existing_index is not None and (existing_index.artifact_type != "CandidateSetDecisionIndexV1"
                                      or canonical_json(existing_index.metadata) != canonical_json(decision_index_body)):
        raise ValueError("decision event already has a different immutable CandidateSet")
    repository.register_artifact(ArtifactIndexEntryV2(result.content_hash, "CandidateSetV2", result.content_hash,
        cutoff_ns, cutoff_ns, {"candidate_set": result.to_dict(), "identity": identity}))
    repository.register_artifact(ArtifactIndexEntryV2(decision_index_ref, "CandidateSetDecisionIndexV1",
        sha256_json(decision_index_body), cutoff_ns, cutoff_ns, decision_index_body))
    return result


def accept_research_candidates(repository: OpsRepository, candidate_set: CandidateSetV2,
                               candidates: Sequence[CandidateActionV2], *, accepted_at_ns: int) -> dict[str, str]:
    """Index an acceptance for every generated candidate incorporated in the set."""
    indexed_set = repository.get_artifact(candidate_set.content_hash)
    if indexed_set is None or indexed_set.artifact_type != "CandidateSetV2" or canonical_json(indexed_set.metadata.get("candidate_set")) != candidate_set.to_canonical_json():
        raise ValueError("CandidateSet must be durably indexed before acceptance")
    by_id = {candidate.candidate_id: candidate for candidate in candidates}
    if len(by_id) != len(candidates) or set(by_id) != {entry.candidate_id for entry in candidate_set.candidates}:
        raise ValueError("acceptance candidates must exactly match CandidateSet")
    if accepted_at_ns < candidate_set.envelope.available_at_ns:
        raise ValueError("acceptance cannot precede CandidateSet availability")
    accepted: dict[str, str] = {}
    for entry in candidate_set.candidates:
        candidate = by_id[entry.candidate_id]
        if (candidate.content_hash not in candidate_set.envelope.input_refs or candidate.key != entry.key
                or candidate.side != entry.side):
            raise ValueError("acceptance candidate does not match CandidateSet evidence")
        indexed = repository.get_artifact(candidate.content_hash)
        if indexed is None or canonical_json(indexed.metadata.get("candidate")) != candidate.to_canonical_json():
            raise ValueError("candidate artifact unavailable for acceptance")
        body = {"status": "ACCEPTED", "candidate_ref": candidate.content_hash,
            "candidate_set_ref": candidate_set.content_hash, "decision_event_id": candidate_set.decision_event_id,
            "feature_hash": candidate.snapshot_hash, "selection_status": candidate_set.selection_status.value,
            "selected": candidate_set.selected_candidate_id == candidate.candidate_id,
            "pipeline_version": PRODUCER_VERSION, "accepted_at_ns": accepted_at_ns,
            "watch_id": indexed.metadata.get("watch_id")}
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(ref, "ResearchCandidateAcceptanceV1", ref,
            accepted_at_ns, accepted_at_ns, body))
        accepted[candidate.candidate_id] = ref
    return accepted
