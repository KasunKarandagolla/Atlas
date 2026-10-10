"""Additive multi-sleeve research selection; the accepted S1/S2 path is untouched."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json
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
from atlas.v2.selection import (
    ORDERING as BASELINE_ORDERING,
)
from atlas.v2.selection import (
    RANK_MAX_AGE_NS,
    ScannerRankEvidenceV1,
    _indexed_causal,
    _matching_scanner_source,
)
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY
from atlas.v2.strategies.s6_cross_section import S6_ACTION_POLICY, S6_POLICY

MULTI_SLEEVE_SELECTION_ID = "MULTI_SLEEVE_RESEARCH_SELECTION_V1"
MULTI_SLEEVE_ORDERING = BASELINE_ORDERING

# Frozen new-run selector accepted for S1-S3. Keep this body and digest exact;
# registry metadata is intentionally kept outside the hashed policy body.
NEW_RUN_SELECTION_VERSION = "1.0.0-research"
NEW_RUN_SELECTION_BODY = {
    "selection_policy_id": MULTI_SLEEVE_SELECTION_ID,
    "version": NEW_RUN_SELECTION_VERSION,
    "capital_status": "SHADOW_ONLY",
    "ordering": list(MULTI_SLEEVE_ORDERING),
    "scanner_evidence_max_age_ns": RANK_MAX_AGE_NS,
    "candidate_eligibility": "COMPLETE_IMMUTABLE_SINGLE_ACTION_CONTRACT_ONLY",
    "outcome_models_in_selection": False,
    "all_exact_action_competitors_retained": True,
}
NEW_RUN_SELECTION_HASH = sha256_json(NEW_RUN_SELECTION_BODY)

# The paused S41 selector remains addressable for old artifacts and replay.
# These literal values preserve its original body and exact content hash.
S41_REPLAY_SELECTION_VERSION = "1.1.0-research"
S41_REPLAY_SELECTION_BODY = {
    "selection_policy_id": MULTI_SLEEVE_SELECTION_ID,
    "version": S41_REPLAY_SELECTION_VERSION,
    "capital_status": "SHADOW_ONLY",
    "ordering": list(MULTI_SLEEVE_ORDERING),
    "scanner_evidence_max_age_ns": RANK_MAX_AGE_NS,
    "candidate_eligibility": "COMPLETE_IMMUTABLE_SINGLE_ACTION_CONTRACT_ONLY",
    "outcome_models_in_selection": False,
    "all_exact_action_competitors_retained": True,
}
S41_REPLAY_SELECTION_HASH = sha256_json(S41_REPLAY_SELECTION_BODY)

_NEW_RUN_ACTION_HASHES = (
    ("S1_MTF_TREND_PULLBACK", "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"),
    ("S2_COMPRESSION_BREAKOUT", "fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd"),
    ("S3_VWAP_STAT_MEAN_REVERSION", "b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6"),
)
_S41_REPLAY_ACTION_HASHES = (*_NEW_RUN_ACTION_HASHES,
    ("S6_CROSS_SECTIONAL_RELATIVE_STRENGTH", "4cf5797068874ae5f0f2958243fcb1b53f4aa79a25cfb9a18c972e052ed44a3d"))
_CURRENT_ACTION_HASHES = {policy.policy_id: policy.policy_hash
    for policy in (S1_POLICY, S2_POLICY, S3_POLICY, S6_ACTION_POLICY)}
for _policy_id, _policy_hash in _S41_REPLAY_ACTION_HASHES:
    if _CURRENT_ACTION_HASHES.get(_policy_id) != _policy_hash:
        raise RuntimeError(f"research selector registry policy hash drift: {_policy_id}")
if NEW_RUN_SELECTION_HASH != "37355c64d45a67bdce3a271a63db377b953c05847561bcda33847c27cecb0dac":
    raise RuntimeError("accepted 1.0.0 research selector hash drift")
if S41_REPLAY_SELECTION_HASH != "9bd4d8e41e0199a1e1f3ee0e5f72f824661ad346ef056f6a88d32356f7663795":
    raise RuntimeError("immutable S41 replay selector hash drift")


@dataclass(frozen=True)
class ResearchSelectionRegistryEntryV1:
    selection_id: str
    version: str
    selection_hash: str
    body_json: str
    disposition: str
    action_policy_hashes: tuple[tuple[str, str], ...]

    @property
    def body(self) -> dict[str, Any]:
        return json_value(json.loads(self.body_json))


RESEARCH_SELECTION_REGISTRY_V1 = (
    ResearchSelectionRegistryEntryV1(MULTI_SLEEVE_SELECTION_ID, NEW_RUN_SELECTION_VERSION,
        NEW_RUN_SELECTION_HASH, canonical_json(NEW_RUN_SELECTION_BODY), "NEW_RUN_AND_REPLAY",
        _NEW_RUN_ACTION_HASHES),
    ResearchSelectionRegistryEntryV1(MULTI_SLEEVE_SELECTION_ID, S41_REPLAY_SELECTION_VERSION,
        S41_REPLAY_SELECTION_HASH, canonical_json(S41_REPLAY_SELECTION_BODY), "REPLAY_ONLY",
        _S41_REPLAY_ACTION_HASHES),
)

# Existing production imports deliberately resolve to the accepted selector.
MULTI_SLEEVE_SELECTION_VERSION = NEW_RUN_SELECTION_VERSION
MULTI_SLEEVE_SELECTION_BODY = NEW_RUN_SELECTION_BODY
MULTI_SLEEVE_SELECTION_HASH = NEW_RUN_SELECTION_HASH
EXACT_ACTION_POLICY_IDS = frozenset(policy_id for policy_id, _ in _NEW_RUN_ACTION_HASHES)
EXACT_ACTION_POLICIES = {p.policy_id: p for p in (S1_POLICY, S2_POLICY, S3_POLICY)}

S6_ACTION_APPROVAL_PENDING = "S6_ACTION_APPROVAL_PENDING"
S6_EXCLUDED_STATUS = "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"


def resolve_research_selection_registry(version: str, selection_hash: str, *, purpose: str,
        selection_id: str = MULTI_SLEEVE_SELECTION_ID) -> ResearchSelectionRegistryEntryV1:
    """Resolve an exact selector identity without changing stored artifacts."""
    if purpose not in {"NEW_RUN", "REPLAY"}:
        raise ValueError("research selector purpose must be NEW_RUN or REPLAY")
    entry = next((item for item in RESEARCH_SELECTION_REGISTRY_V1
                  if (item.selection_id, item.version, item.selection_hash)
                  == (selection_id, version, selection_hash)), None)
    if purpose == "NEW_RUN":
        if entry is not None and entry.disposition == "NEW_RUN_AND_REPLAY":
            return entry
        if version == S41_REPLAY_SELECTION_VERSION or selection_hash == S41_REPLAY_SELECTION_HASH:
            raise ValueError(S6_ACTION_APPROVAL_PENDING)
        raise ValueError("research selector is not registered for new runs")
    if entry is None:
        raise ValueError("research selector replay requires the exact registered version and hash")
    return entry


def research_selection_universe(universe: UniverseContractV2, *, version: str | None = None,
        selection_hash: str | None = None) -> UniverseContractV2:
    """Derive an additive research universe without rewriting the accepted population."""
    requested_version = NEW_RUN_SELECTION_VERSION if version is None else version
    requested_hash = NEW_RUN_SELECTION_HASH if selection_hash is None else selection_hash
    registry = resolve_research_selection_registry(requested_version, requested_hash, purpose="NEW_RUN")
    if universe.selection_policy_hash == S41_REPLAY_SELECTION_HASH:
        raise ValueError(S6_ACTION_APPROVAL_PENDING)
    identity = sha256_json({"source_universe": universe.content_hash,
        "research_selector": registry.selection_hash})
    return replace(universe, selection_policy_hash=registry.selection_hash,
        envelope=replace(universe.envelope, content_hash="", artifact_id=identity,
            producer_version="MULTI_SLEEVE_RESEARCH_UNIVERSE_V1",
            input_refs=tuple(sorted({*universe.envelope.input_refs, universe.content_hash}))))

SLEEVE_AVAILABILITY = (
    ("S1", "ELIGIBLE", "COMPLETE CandidateActionV2 contract exists"),
    ("S2", "ELIGIBLE", "COMPLETE CandidateActionV2 contract exists"),
    ("S3", "ELIGIBLE", "COMPLETE exact stop, horizon and management contract exists"),
    ("S4", "EXCLUDED", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"),
    ("S5", "EXCLUDED", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"),
    ("S6", "EXCLUDED", f"{S6_EXCLUDED_STATUS} / {S6_ACTION_APPROVAL_PENDING}"),
    ("S7", "EXCLUDED", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"),
    ("S8", "EXCLUDED", "RESEARCH_BASKET_ONLY_NO_SINGLE_ACTION_CONTRACT"),
)


@dataclass(frozen=True)
class ResearchSleeveSelectionAuditV2:
    selection_policy_hash: str
    available_at_ns: int
    sleeves: tuple[tuple[str, str, str], ...]
    exact_action_policy_ids: tuple[str, ...]
    excluded_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": "RESEARCH_SLEEVE_SELECTION_AUDIT_V2_V1",
            "selection_policy_hash": self.selection_policy_hash, "available_at_ns": self.available_at_ns,
            "sleeves": [list(row) for row in self.sleeves],
            "exact_action_policy_ids": list(self.exact_action_policy_ids),
            "excluded_reason": self.excluded_reason}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def research_sleeve_audit(available_at_ns: int) -> ResearchSleeveSelectionAuditV2:
    return ResearchSleeveSelectionAuditV2(MULTI_SLEEVE_SELECTION_HASH, available_at_ns,
        SLEEVE_AVAILABILITY, tuple(sorted(EXACT_ACTION_POLICY_IDS)), "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT")


def persist_research_sleeve_audit(repo: OpsRepository, *, available_at_ns: int) -> str:
    audit = research_sleeve_audit(available_at_ns)
    body = audit.to_dict()
    repo.register_artifact(ArtifactIndexEntryV2(audit.content_hash, "ResearchSleeveSelectionAuditV2",
        audit.content_hash, available_at_ns, available_at_ns, {"selection_audit": body}))
    return audit.content_hash


def _persist_s6_observation_sidecar(repo: OpsRepository, *, candidate: CandidateActionV2,
        universe: UniverseContractV2, decision_event_id: str, cutoff_ns: int,
        scanner_refs: Sequence[str]) -> str:
    """Persist S6 shadow/rank evidence separately from the new-run CandidateSet."""
    identity = {"version": "S6_EXCLUDED_CANDIDATE_OBSERVATION_ID_V1",
        "decision_event_id": decision_event_id, "candidate_ref": candidate.content_hash,
        "selection_policy_hash": NEW_RUN_SELECTION_HASH,
        "universe_ref": universe.content_hash, "cutoff_ns": cutoff_ns}
    identity_ref = sha256_json(identity)
    event_identity = {"version": "S6_EXCLUDED_CANDIDATE_EVENT_ID_V1",
        "decision_event_id": decision_event_id, "candidate_ref": candidate.content_hash,
        "selection_policy_hash": NEW_RUN_SELECTION_HASH}
    event_identity_ref = sha256_json(event_identity)
    event_identity_entry = repo.get_artifact(event_identity_ref)
    if event_identity_entry is not None:
        if (event_identity_entry.artifact_type != "S6ExcludedCandidateEventIndexV1"
                or event_identity_entry.content_hash != event_identity_ref
                or canonical_json(event_identity_entry.metadata.get("event_identity"))
                    != canonical_json(event_identity)
                or canonical_json(event_identity_entry.metadata.get("identity"))
                    != canonical_json(identity)):
            raise ValueError("S6 event retry conflicts with its immutable universe/cutoff identity")
    identity_entry = repo.get_artifact(identity_ref)
    if identity_entry is not None:
        if (identity_entry.artifact_type != "S6ExcludedCandidateObservationIndexV1"
                or identity_entry.content_hash != identity_ref
                or canonical_json(identity_entry.metadata.get("identity")) != canonical_json(identity)
                or identity_entry.metadata.get("scanner_refs") != tuple(sorted(set(scanner_refs)))):
            raise ValueError("S6 observation retry conflicts with its immutable sidecar index")
        if event_identity_entry is None:
            raise ValueError("S6 observation index is missing its immutable event identity")
        prior_ref = identity_entry.metadata.get("observation_ref")
        prior = repo.get_artifact(str(prior_ref)) if isinstance(prior_ref, str) else None
        if (prior is None or prior.artifact_type != "S6ExcludedCandidateObservationV1"
                or prior.content_hash != prior_ref
                or prior.metadata.get("observation") is None):
            raise ValueError("S6 observation index points to a missing or invalid sidecar")
        prior_observation = prior.metadata["observation"]
        if (prior_observation.get("candidate_ref") != candidate.content_hash
                or prior_observation.get("selection_policy_hash") != NEW_RUN_SELECTION_HASH
                or prior_observation.get("universe_ref") != universe.content_hash
                or prior_observation.get("cutoff_ns") != cutoff_ns
                or prior_observation.get("eligibility_status") != S6_EXCLUDED_STATUS
                or prior_observation.get("reason") != S6_ACTION_APPROVAL_PENDING):
            raise ValueError("S6 observation retry found a conflicting immutable sidecar")
        return str(prior_ref)

    candidate_entry = repo.get_artifact(candidate.content_hash)
    if (candidate.policy_hash != S6_ACTION_POLICY.policy_hash or candidate.quantity is not None
            or candidate_entry is None or candidate_entry.artifact_type != "CandidateActionV2"
            or candidate_entry.content_hash != candidate.content_hash
            or canonical_json(candidate_entry.metadata.get("candidate")) != candidate.to_canonical_json()):
        raise ValueError("S6 pending observation must be the exact indexed unsized shadow candidate")
    if candidate.decision_at_ns != cutoff_ns:
        raise ValueError("S6 pending observation has a mismatched cutoff")
    if candidate.key not in {row.key for row in universe.entries}:
        raise ValueError("S6 pending observation instrument is outside the point-in-time universe")

    metadata = candidate_entry.metadata
    hypothesis_ref = metadata.get("hypothesis_ref")
    state_ref = metadata.get("state_ref")
    trigger_ref = metadata.get("action_trigger_ref")
    hypothesis_entry = repo.get_artifact(str(hypothesis_ref)) if isinstance(hypothesis_ref, str) else None
    state_entry = repo.get_artifact(str(state_ref)) if isinstance(state_ref, str) else None
    hypothesis = hypothesis_entry.metadata.get("hypothesis") if hypothesis_entry is not None else None
    state = state_entry.metadata.get("state") if state_entry is not None else None

    rank_row: Mapping[str, Any] | None = None
    denominator: dict[str, Any] = {}
    s6_rank_refs: set[str] = set()
    if isinstance(state, Mapping):
        rows = state.get("rows")
        if isinstance(rows, (tuple, list)):
            rank_row = next((row for row in rows if isinstance(row, Mapping)
                and canonical_json(row.get("key")) == candidate.key.to_canonical_json()), None)
            denominator = {
                "state_status": state.get("status"),
                "state_reason": state.get("reason"),
                "eligible_breadth": state.get("eligible_breadth"),
                "rankable_breadth": sum(1 for row in rows if isinstance(row, Mapping) and row.get("rank") is not None),
                "decile_size": state.get("decile_size"),
                "denominator_ref": state_ref,
            }
            if rank_row is not None:
                s6_rank_refs.update(ref for ref in (*rank_row.get("hourly_refs", ()),
                    *rank_row.get("context_refs", ()), rank_row.get("evidence_ref")) if isinstance(ref, str))

    attempted_scanner_refs = tuple(sorted(set(scanner_refs)))
    scanner_rank_observations: list[dict[str, Any]] = []
    for ref in attempted_scanner_refs:
        entry = repo.get_artifact(ref)
        if entry is not None and entry.artifact_type == "ScannerRankEvidenceV1":
            scanner_rank_observations.append({"ref": ref, "available_at_ns": entry.available_at_ns,
                "evidence": json_value(entry.metadata)})

    trigger_refs: set[str] = set()
    trigger_observation: Mapping[str, Any] | None = None
    if isinstance(trigger_ref, str):
        trigger_refs.add(trigger_ref)
        trigger_entry = repo.get_artifact(trigger_ref)
        if trigger_entry is not None:
            body = trigger_entry.metadata.get("trigger")
            if isinstance(body, Mapping):
                trigger_observation = body
                for name in ("previous_15m_ref", "trigger_15m_ref", "hypothesis_ref"):
                    ref = body.get(name)
                    if isinstance(ref, str):
                        trigger_refs.add(ref)
    for ref in candidate.envelope.input_refs:
        entry = repo.get_artifact(ref)
        if entry is not None and entry.artifact_type in {"S6ActionTriggerEligibilityV1", "S6ConfirmedTriggerV2"}:
            trigger_refs.add(ref)

    hypothesis_refs = {ref for ref in (hypothesis_ref, state_ref) if isinstance(ref, str)}
    watch_id = hypothesis.get("watch_id") if isinstance(hypothesis, Mapping) else None
    watch = repo.get_watch(watch_id) if isinstance(watch_id, str) and watch_id else None
    if watch is None:
        watch_refs: list[str] = []
        watch_observation: dict[str, Any] = {
            "status": "MISSING",
            "watch_id": watch_id if isinstance(watch_id, str) and watch_id else None,
            "reason": "PERSISTED_WATCH_NOT_FOUND" if isinstance(watch_id, str) and watch_id
                else "WATCH_ID_UNAVAILABLE",
        }
    else:
        watch_refs = [watch.watch_id]
        watch_observation = {
            "status": "PRESENT",
            "watch_id": watch.watch_id,
            "state": watch.state.value,
            "state_version": watch.state_version,
            "available_at_ns": watch.updated_at_ns,
            "content_hash": watch.content_hash,
            "watch": watch.to_dict(),
        }
    source_refs = tuple(sorted(candidate.envelope.input_refs))

    # Every artifact reference copied into the sidecar contributes to its
    # deterministic chronology, even when the index is missing. Missing refs
    # remain visible in the observation and never acquire invented timestamps.
    copied_refs = {candidate.content_hash, universe.content_hash, *source_refs, *attempted_scanner_refs,
        *s6_rank_refs, *trigger_refs, *hypothesis_refs}
    for observation in (hypothesis, rank_row, trigger_observation, watch_observation,
            *(item.get("evidence") for item in scanner_rank_observations)):
        pending_values = [observation]
        while pending_values:
            value = pending_values.pop()
            if isinstance(value, Mapping):
                for name, nested in value.items():
                    if isinstance(name, str) and name.endswith("_ref") and isinstance(nested, str):
                        copied_refs.add(nested)
                    elif isinstance(name, str) and name.endswith("_refs") and isinstance(nested, (tuple, list)):
                        copied_refs.update(ref for ref in nested if isinstance(ref, str))
                    pending_values.append(nested)
            elif isinstance(value, (tuple, list)):
                pending_values.extend(value)
    copied_entries = {ref: repo.get_artifact(ref) for ref in sorted(copied_refs)}
    missing_refs = sorted(ref for ref, entry in copied_entries.items() if entry is None)
    evidence_available_at_ns = [entry.available_at_ns for entry in copied_entries.values()
        if entry is not None]
    if watch is not None:
        evidence_available_at_ns.append(watch.updated_at_ns)
    if not evidence_available_at_ns:
        raise ValueError("S6 observation sidecar has no indexed evidence availability")
    body = {
        "schema_version": 1,
        "decision_event_id": decision_event_id,
        "cutoff_ns": cutoff_ns,
        "universe_ref": universe.content_hash,
        "selection_policy_id": MULTI_SLEEVE_SELECTION_ID,
        "selection_policy_version": NEW_RUN_SELECTION_VERSION,
        "selection_policy_hash": NEW_RUN_SELECTION_HASH,
        "candidate_id": candidate.candidate_id,
        "candidate_ref": candidate.content_hash,
        "key": candidate.key.to_dict(),
        "side": candidate.side.value,
        "eligibility_status": S6_EXCLUDED_STATUS,
        "reason": S6_ACTION_APPROVAL_PENDING,
        "proposed_action_policy_hash": S6_ACTION_POLICY.policy_hash,
        "rank_policy_hash": S6_POLICY.policy_hash,
        "rank_refs": sorted({*s6_rank_refs, *attempted_scanner_refs}),
        "rank_observation": json_value(rank_row) if rank_row is not None else None,
        "scanner_rank_observations": scanner_rank_observations,
        "trigger_refs": sorted(trigger_refs),
        "trigger_observation": json_value(trigger_observation) if trigger_observation is not None else None,
        "hypothesis_refs": sorted(hypothesis_refs),
        "hypothesis_observation": json_value(hypothesis) if isinstance(hypothesis, Mapping) else None,
        "watch_refs": watch_refs,
        "watch_observation": watch_observation,
        "shadow_refs": [candidate.content_hash],
        "source_refs": list(source_refs),
        "missing_refs": missing_refs,
        "denominator": denominator,
        "missing_action": {
            "status": S6_EXCLUDED_STATUS,
            "reason": S6_ACTION_APPROVAL_PENDING,
            "existing_missing_contract": (hypothesis.get("missing_contract")
                if isinstance(hypothesis, Mapping) else "S6_EXACT_ACTION_CONTRACT_PENDING"),
        },
        "selector_influence": "ZERO",
        "capital_authority": "ZERO",
    }
    sidecar_ref = sha256_json({"artifact_type": "S6ExcludedCandidateObservationV1",
        "identity": identity, "observation": body})
    sidecar_available_at_ns = max(evidence_available_at_ns)
    sidecar_entry = ArtifactIndexEntryV2(sidecar_ref, "S6ExcludedCandidateObservationV1",
        sidecar_ref, sidecar_available_at_ns, sidecar_available_at_ns, {"observation": body})
    index_body = {"identity": identity, "scanner_refs": list(attempted_scanner_refs),
        "observation_ref": sidecar_ref}
    index_entry = ArtifactIndexEntryV2(identity_ref, "S6ExcludedCandidateObservationIndexV1",
        identity_ref, sidecar_available_at_ns, sidecar_available_at_ns, index_body)
    event_index_body = {"event_identity": event_identity, "identity": identity,
        "identity_ref": identity_ref, "observation_ref": sidecar_ref}
    event_index_entry = ArtifactIndexEntryV2(event_identity_ref, "S6ExcludedCandidateEventIndexV1",
        event_identity_ref, sidecar_available_at_ns, sidecar_available_at_ns, event_index_body)
    repo.register_artifacts((sidecar_entry, index_entry, event_index_entry))
    return sidecar_ref


def _candidate_is_indexed(repo: OpsRepository, candidate: CandidateActionV2, policy: PolicySpecV2,
        universe: UniverseContractV2, cutoff_ns: int, consumer_at_ns: int | None = None) -> ArtifactIndexEntryV2:
    from atlas.v2.chronology import causal_artifact
    consumer_at_ns = cutoff_ns if consumer_at_ns is None else consumer_at_ns
    entry = repo.get_artifact(candidate.content_hash)
    if (entry is None or entry.artifact_type != "CandidateActionV2" or entry.content_hash != candidate.content_hash
        or canonical_json(entry.metadata.get("candidate")) != candidate.to_canonical_json()):
        raise ValueError("multi-sleeve exact candidate must already be durably indexed")
    accepted_policy = EXACT_ACTION_POLICIES.get(policy.policy_id)
    if accepted_policy is None or policy.policy_hash != accepted_policy.policy_hash or policy.policy_hash != candidate.policy_hash:
        raise ValueError("multi-sleeve candidate lacks a declared complete action policy")
    duration = policy.time_exit_rule.get("after_ns", policy.time_exit_rule.get("max_hold_ns"))
    if candidate.horizon_end_ns - candidate.decision_at_ns != duration:
        raise ValueError("multi-sleeve candidate horizon differs from its complete action policy")
    if (candidate.side.value == "LONG" and candidate.stop_price >= candidate.entry_reference) or (
        candidate.side.value == "SHORT" and candidate.stop_price <= candidate.entry_reference):
        raise ValueError("multi-sleeve candidate stop is on the wrong side")
    if candidate.quantity is not None:
        raise ValueError("CandidateSet selection accepts only unsized exact action hypotheses")
    if candidate.decision_at_ns != cutoff_ns or candidate.envelope.available_at_ns > consumer_at_ns:
        raise ValueError("multi-sleeve candidate decision/cutoff identity mismatch")
    if not causal_artifact(repo, candidate.content_hash, cutoff_ns=cutoff_ns,
                           consumer_at_ns=consumer_at_ns, deadline_ns=candidate.deadline_ns):
        raise ValueError("multi-sleeve candidate artifact is unavailable at selection cutoff")
    if candidate.key not in {row.key for row in universe.entries}:
        raise ValueError("exact action candidate instrument absent from point-in-time universe")
    source_universe = entry.metadata.get("universe_ref")
    if source_universe is not None and source_universe not in (universe.content_hash, *universe.envelope.input_refs):
        raise ValueError("multi-sleeve candidate universe lineage mismatch")
    return entry


def _selection_stage_inputs_only(repo: OpsRepository, source_ref: str, cutoff_ns: int) -> bool:
    pending = [source_ref]
    seen: set[str] = set()
    forbidden = {"MaturedOutcomeV2", "EvaluationArtifactV2", "PayoffOutcomeV2",
        "FrozenActionModelComparisonV2"}
    while pending:
        ref = pending.pop()
        if ref in seen:
            continue
        seen.add(ref)
        if len(seen) > 256:
            return False
        entry = repo.get_artifact(ref)
        if (entry is None or entry.available_at_ns > cutoff_ns or entry.artifact_type in forbidden
            or entry.artifact_type.endswith("OutcomeV2")
            or entry.artifact_type.startswith(("M0", "M1", "Analogue"))):
            return False
        nested: list[Any] = [entry.metadata]
        while nested:
            value = nested.pop()
            if isinstance(value, Mapping):
                pending.extend(str(ref) for ref in value.get("input_refs", ()))
                nested.extend(item for name, item in value.items() if name != "input_refs")
            elif isinstance(value, (tuple, list)):
                nested.extend(value)
    return True


def assemble_multisleeve_research_candidate_set(repo: OpsRepository, *, universe: UniverseContractV2,
        decision_event_id: str, cutoff_ns: int, candidates: Sequence[CandidateActionV2],
        policies: Mapping[str, PolicySpecV2], scanner_evidence_refs: Mapping[str, Sequence[str]],
        generation_missing_reasons: Sequence[str] = (), clock_ns: Callable[[], int] | None = None,
        deadline_ns: int | None = None) -> CandidateSetV2:
    """Build the separately versioned research set using selection-stage data only.

    M0/M1/analogue values are intentionally absent from this function's inputs.
    """
    from atlas.v2.chronology import causal_artifact, record_computation, sample
    if any(not isinstance(item, CandidateActionV2) for item in candidates):
        raise TypeError("only complete single-action CandidateActionV2 competitors may enter the research set")
    selector_inputs = tuple(item for item in candidates if item.policy_hash != S6_ACTION_POLICY.policy_hash)
    selector_floor = max(cutoff_ns, universe.envelope.available_at_ns,
        *(item.envelope.available_at_ns for item in selector_inputs))
    started = sample(clock_ns, floor_ns=selector_floor) if clock_ns else cutoff_ns
    deadline = deadline_ns if deadline_ns is not None else min(
        (item.deadline_ns for item in selector_inputs), default=cutoff_ns + 5_000_000_000)
    if not decision_event_id or type(cutoff_ns) is not int or cutoff_ns < 0:
        raise ValueError("multi-sleeve decision identity/cutoff is required")
    missing_reasons = tuple(sorted(set(generation_missing_reasons)))
    if len(missing_reasons) > 32 or any(not isinstance(reason, str) or not 1 <= len(reason) <= 192
                                       for reason in missing_reasons):
        raise ValueError("candidate generation missingness must be bounded explicit reasons")
    if universe.selection_policy_hash == S41_REPLAY_SELECTION_HASH:
        raise ValueError(S6_ACTION_APPROVAL_PENDING)
    if universe.selection_policy_hash != MULTI_SLEEVE_SELECTION_HASH:
        raise ValueError("multi-sleeve selection requires its separately versioned research universe")
    if ((clock_ns is not None and not causal_artifact(repo, universe.content_hash, cutoff_ns=cutoff_ns,
                           consumer_at_ns=started, deadline_ns=deadline))
            or universe.envelope.available_at_ns > started or cutoff_ns > universe.decision_slot_ns):
        raise ValueError("multi-sleeve universe is not causal at the selection cutoff")
    candidate_ids = [item.candidate_id for item in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("duplicate multi-sleeve candidate ID")
    if set(scanner_evidence_refs) - set(candidate_ids):
        raise ValueError("multi-sleeve scanner evidence references an absent candidate")
    all_ordered = tuple(sorted(candidates, key=lambda item: item.candidate_id))
    s6_observations = tuple(item for item in all_ordered if item.policy_hash == S6_ACTION_POLICY.policy_hash)
    ordered = tuple(item for item in all_ordered if item.policy_hash != S6_ACTION_POLICY.policy_hash)
    for candidate in ordered:
        policy = policies.get(candidate.policy_hash)
        if policy is None:
            raise ValueError("multi-sleeve policy specification missing")
        _candidate_is_indexed(repo, candidate, policy, universe, cutoff_ns, started)
    repo.register_artifact(ArtifactIndexEntryV2(MULTI_SLEEVE_SELECTION_HASH,
        "ResearchSelectionPolicyV2", MULTI_SLEEVE_SELECTION_HASH, 0, 0, MULTI_SLEEVE_SELECTION_BODY))
    repo.register_artifact(ArtifactIndexEntryV2(universe.content_hash, "UniverseContractV2",
        universe.content_hash, universe.envelope.created_at_ns, universe.envelope.available_at_ns,
        {"universe": universe.to_dict()}))
    statuses: dict[str, EligibilityStatusV2] = {}
    reasons: dict[str, str] = {}
    ranks: dict[str, int] = {}
    selected_refs: dict[str, tuple[str, ...]] = {}
    causal_refs = {MULTI_SLEEVE_SELECTION_HASH, universe.content_hash}
    attempted = {item.candidate_id: tuple(sorted(scanner_evidence_refs.get(item.candidate_id, ()))) for item in ordered}
    uncertain = False
    for candidate in ordered:
        causal_refs.add(candidate.content_hash)
        policy = policies[candidate.policy_hash]
        universe_rows = [row for row in universe.entries if row.key == candidate.key]
        if len(universe_rows) != 1:
            statuses[candidate.candidate_id] = EligibilityStatusV2.INELIGIBLE
            reasons[candidate.candidate_id] = "UNIVERSE_INELIGIBLE_OR_AMBIGUOUS"
            continue
        universe_row = universe_rows[0]
        strategy = universe_row.strategy_eligibility.get(policy.policy_id)
        if (not universe_row.data_eligible or not universe_row.scanner_eligible or
                (strategy is not None and strategy.status == EligibilityStatusV2.INELIGIBLE)):
            statuses[candidate.candidate_id] = EligibilityStatusV2.INELIGIBLE
            reasons[candidate.candidate_id] = (strategy.reason if strategy is not None and strategy.reason
                else "UNIVERSE_INELIGIBLE")
            continue
        if strategy is None or strategy.status == EligibilityStatusV2.NOT_ESTIMABLE:
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "STRATEGY_ELIGIBILITY_NOT_ESTIMABLE"
            uncertain = True
            continue
        if candidate.deadline_ns < cutoff_ns:
            statuses[candidate.candidate_id] = EligibilityStatusV2.INELIGIBLE
            reasons[candidate.candidate_id] = "CANDIDATE_EXPIRED_BEFORE_SELECTION"
            continue
        refs = attempted[candidate.candidate_id]
        if len(refs) != 1:
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "SCANNER_EVIDENCE_MISSING_OR_CONTRADICTORY"
            uncertain = True
            continue
        evidence_ref = refs[0]
        evidence_entry = _indexed_causal(repo, evidence_ref, started)
        if evidence_entry is not None and not causal_artifact(repo, evidence_ref, cutoff_ns=cutoff_ns,
                                                              consumer_at_ns=started, deadline_ns=deadline):
            evidence_entry = None
        if evidence_entry is None or evidence_entry.artifact_type != "ScannerRankEvidenceV1":
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "SCANNER_EVIDENCE_UNAVAILABLE"
            uncertain = True
            continue
        evidence = ScannerRankEvidenceV1(**{name: evidence_entry.metadata[name]
            for name in ScannerRankEvidenceV1.__dataclass_fields__})
        if not _selection_stage_inputs_only(repo, evidence.source_artifact_ref, started):
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "MODEL_OR_OUTCOME_EVIDENCE_FORBIDDEN_AT_SELECTION"
            uncertain = True
            continue
        if (evidence.content_hash != evidence_ref or evidence.candidate_id != candidate.candidate_id
            or evidence.universe_ref != universe.content_hash or evidence.decision_event_id != decision_event_id
            or evidence.available_at_ns > started or started - evidence.available_at_ns > RANK_MAX_AGE_NS
            or not _matching_scanner_source(repo, evidence.source_artifact_ref, evidence.to_dict(), key=candidate.key)):
            statuses[candidate.candidate_id] = EligibilityStatusV2.NOT_ESTIMABLE
            reasons[candidate.candidate_id] = "SCANNER_EVIDENCE_STALE_OR_CONTRADICTORY"
            uncertain = True
            continue
        statuses[candidate.candidate_id] = EligibilityStatusV2.ELIGIBLE
        ranks[candidate.candidate_id] = evidence.scanner_rank
        selected_refs[candidate.candidate_id] = tuple(sorted((evidence_ref, evidence.source_artifact_ref)))
        causal_refs.update((evidence_ref, evidence.source_artifact_ref))
    ranking = sorted((item for item in ordered if item.candidate_id in ranks),
        key=lambda item: (ranks[item.candidate_id], policies[item.policy_hash].policy_id,
            item.key.to_canonical_json(), item.candidate_id))
    positions = {item.candidate_id: index + 1 for index, item in enumerate(ranking)}
    entries = tuple(CandidateSetEntryV2(item.candidate_id, policies[item.policy_hash].policy_id, item.key,
        item.side, selected_refs.get(item.candidate_id, ()), statuses[item.candidate_id],
        positions.get(item.candidate_id), "SCANNER_RANK_POLICY_KEY_CANDIDATE_ID" if item.candidate_id in positions else None,
        reasons.get(item.candidate_id)) for item in ordered)
    if uncertain or missing_reasons:
        selection_state, selected_id = CandidateSelectionStatus.NOT_ESTIMABLE, None
    elif ranking:
        selection_state, selected_id = CandidateSelectionStatus.SELECTED, ranking[0].candidate_id
    else:
        selection_state, selected_id = CandidateSelectionStatus.NO_CANDIDATE, None
    identity = {"decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        "candidate_refs": sorted(item.content_hash for item in ordered),
        "causal_input_refs": sorted(causal_refs),
        "attempted_scanner_evidence_refs": {key: list(value) for key, value in attempted.items()},
        "cutoff_ns": cutoff_ns, "producer_version": "MULTI_SLEEVE_RESEARCH_CANDIDATE_PIPELINE_V1"}
    if missing_reasons:
        identity["generation_missing_reasons"] = list(missing_reasons)
    identity_ref = sha256_json(identity)
    decision_index_ref = sha256_json({"artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH})
    prior_index = repo.get_artifact(decision_index_ref)
    if prior_index is not None:
        prior = repo.get_artifact(str(prior_index.metadata.get("candidate_set_ref")))
        if (prior is None or canonical_json(prior.metadata.get("identity")) != canonical_json(identity)
                or not causal_artifact(repo, prior.artifact_ref, cutoff_ns=cutoff_ns,
                                       consumer_at_ns=started, deadline_ns=deadline)):
            raise ValueError("research decision conflicts with sealed CandidateSet")
        prior_candidate_set = CandidateSetV2.from_dict(json_value(prior.metadata["candidate_set"]))
        for candidate in s6_observations:
            _persist_s6_observation_sidecar(repo, candidate=candidate, universe=universe,
                decision_event_id=decision_event_id, cutoff_ns=cutoff_ns,
                scanner_refs=scanner_evidence_refs.get(candidate.candidate_id, ()))
        return prior_candidate_set
    finished = sample(clock_ns, floor_ns=started) if clock_ns else cutoff_ns
    available = sample(clock_ns, floor_ns=finished) if clock_ns else finished
    if available > deadline:
        raise ValueError("CandidateSet computation exceeded decision deadline")
    envelope = ArtifactEnvelope(1, identity_ref, finished, available,
        "MULTI_SLEEVE_RESEARCH_CANDIDATE_PIPELINE_V1", tuple(sorted(causal_refs)))
    result = CandidateSetV2(envelope, decision_event_id, universe.content_hash,
        MULTI_SLEEVE_SELECTION_HASH, entries, selected_id,
        "scanner_rank_ascending > policy_id_ascending > canonical_InstrumentKeyV2_ascending > candidate_id_ascending",
        selection_state)
    decision_index_ref = sha256_json({"artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": decision_event_id, "universe_ref": universe.content_hash,
        "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH})
    index_body = {"candidate_set_ref": result.content_hash, "decision_event_id": decision_event_id,
        "universe_ref": universe.content_hash, "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        "cutoff_ns": cutoff_ns}
    existing_index = repo.get_artifact(decision_index_ref)
    if existing_index is not None and canonical_json(existing_index.metadata) != canonical_json(index_body):
        raise ValueError("research decision cannot be rewritten with a different immutable CandidateSet")
    if any(not causal_artifact(repo, ref, cutoff_ns=cutoff_ns,
                               consumer_at_ns=started, deadline_ns=deadline) for ref in causal_refs):
        raise ValueError("multi-sleeve CandidateSet contains unavailable causal references")
    repo.register_artifact(ArtifactIndexEntryV2(result.content_hash, "CandidateSetV2", result.content_hash,
        finished, available, {"candidate_set": result.to_dict(), "identity": identity}))
    repo.register_artifact(ArtifactIndexEntryV2(decision_index_ref, "CandidateSetDecisionIndexV1",
        sha256_json(index_body), finished, available, index_body))
    if clock_ns:
        record_computation(repo, artifact_ref=result.content_hash, information_cutoff_ns=cutoff_ns,
            started_ns=started, finished_ns=finished, available_ns=available,
            input_refs=tuple(causal_refs), deadline_ns=deadline)
    for candidate in s6_observations:
        _persist_s6_observation_sidecar(repo, candidate=candidate, universe=universe,
            decision_event_id=decision_event_id, cutoff_ns=cutoff_ns,
            scanner_refs=scanner_evidence_refs.get(candidate.candidate_id, ()))
    return result
