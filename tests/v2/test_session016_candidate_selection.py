"""CandidateSet durability, causal rank evidence, ties and fail-closed inputs."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    CandidateSelectionStatus,
    EligibilityStatusV2,
    V2Side,
)
from atlas.v2.instruments import InstrumentKeyV2, StrategyEligibilityV2, UniverseContractV2, UniverseEntryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.selection import (
    SELECTION_POLICY_HASH,
    ScannerRankEvidenceV1,
    accept_research_candidates,
    assemble_candidate_set,
    register_scanner_rank,
)
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY

from .test_session014_core import KEY

CUTOFF = 1_800_000_000_000_000
EVENT = "confirmed-15m-016"
POLICIES = {p.policy_hash: p for p in (S1_POLICY, S2_POLICY)}


def alternate_key(symbol: str = "ETHUSDT", venue=None) -> InstrumentKeyV2:
    return replace(KEY, native_symbol=symbol, base_asset_id=symbol[:-4].lower(),
                   venue=venue or KEY.venue, contract_revision=sha256_json({"symbol": symbol, "venue": str(venue)}))


def universe(keys=(KEY,), *, ineligible_policy: str | None = None):
    entries = []
    for key in keys:
        product_ref = sha256_json({"product": key.to_dict()})
        strategy = {policy.policy_id: StrategyEligibilityV2(
            EligibilityStatusV2.INELIGIBLE, "KNOWN_REJECTION") if policy.policy_id == ineligible_policy
            else StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE) for policy in POLICIES.values()}
        entries.append(UniverseEntryV2(key, product_ref, True, True, True, True, False, FrozenMap(strategy), ()))
    entries.sort(key=lambda item: item.key.to_canonical_json())
    return UniverseContractV2(ArtifactEnvelope(1, "u-selection", CUTOFF, CUTOFF,
        "fixture", tuple(sorted(item.product_ref for item in entries))),
        "fixture", CUTOFF, SELECTION_POLICY_HASH, tuple(entries))


def candidate(policy=S1_POLICY, key=KEY, *, serial=0, decision_at_ns=CUTOFF,
              deadline_ns=CUTOFF + 5_000_000_000):
    candidate_id = sha256_json({"policy": policy.policy_hash, "key": key.to_dict(), "serial": serial})
    snapshot = sha256_json({"snapshot": candidate_id})
    envelope = ArtifactEnvelope(1, candidate_id, decision_at_ns, decision_at_ns, "fixture", (snapshot,))
    return CandidateActionV2(envelope, candidate_id, key, policy.policy_hash, snapshot, V2Side.LONG,
        decision_at_ns, deadline_ns, decision_at_ns + 2 * 3_600_000_000_000,
        Decimal("100"), Decimal("100.05"), Decimal("99"), 0, "UNESTIMATED", quantity=None)


def index(repository, item, **metadata):
    repository.register_artifact(ArtifactIndexEntryV2(item.content_hash, "CandidateActionV2",
        item.content_hash, item.envelope.created_at_ns, item.envelope.available_at_ns,
        {"candidate": item.to_dict(), "feature_hash": item.snapshot_hash, **metadata}))


def evidence(repository, item, u, rank, *, available_at_ns=CUTOFF, event=EVENT):
    record = ScannerRankEvidenceV1(item.candidate_id, rank, "SCANNER_V1", "1.0", u.content_hash,
        event, available_at_ns, sha256_json({"scanner": item.candidate_id, "rank": rank}))
    return register_scanner_rank(repository, record)


def assemble(repository, u, items, refs):
    return assemble_candidate_set(repository, universe=u, decision_event_id=EVENT,
        cutoff_ns=CUTOFF, candidates=items, policies=POLICIES, scanner_evidence_refs=refs)


def test_s1_s2_rank_shuffle_and_acceptance(tmp_path):
    u = universe()
    s1, s2 = candidate(), candidate(S2_POLICY)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        for item in (s1, s2):
            index(repository, item)
        refs = {s1.candidate_id: (evidence(repository, s1, u, 2),),
                s2.candidate_id: (evidence(repository, s2, u, 1),)}
        first = assemble(repository, u, (s1, s2), refs)
        second = assemble(repository, u, (s2, s1), refs)
        assert first.content_hash == second.content_hash
        changed = {s1.candidate_id: (evidence(repository, s1, u, 1),),
                   s2.candidate_id: refs[s2.candidate_id]}
        with pytest.raises(ValueError, match="different immutable CandidateSet"):
            assemble(repository, u, (s1, s2), changed)
        assert first.selection_status == CandidateSelectionStatus.SELECTED
        assert first.selected_candidate_id == s2.candidate_id
        assert [entry.candidate_id for entry in first.candidates] == sorted((s1.candidate_id, s2.candidate_id))
        assert {entry.rank for entry in first.candidates} == {1, 2}
        assert repository.get_artifact(first.content_hash) is not None
        accepted = accept_research_candidates(repository, first, (s1, s2), accepted_at_ns=CUTOFF)
        assert len(accepted) == 2
        assert repository.get_artifact(accepted[s1.candidate_id]).metadata["selected"] is False
        assert s2.quantity is None
        for forbidden in ("EvaluationArtifactV2", "TradePlanEnvelopeV2", "Approval", "Reservation", "Order"):
            assert repository.artifact_entries(forbidden) == ()


def test_ties_policy_instrument_candidate_id(tmp_path):
    other = alternate_key()
    u = universe((KEY, other))
    items = (candidate(S1_POLICY, KEY, serial=2), candidate(S1_POLICY, KEY, serial=1),
             candidate(S1_POLICY, other), candidate(S2_POLICY, KEY))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        for item in items:
            index(repository, item)
        refs = {item.candidate_id: (evidence(repository, item, u, 1),) for item in items}
        result = assemble(repository, u, items[::-1], refs)
        ordered = sorted(items, key=lambda item: (1, POLICIES[item.policy_hash].policy_id,
            item.key.to_canonical_json(), item.candidate_id))
        ranks = {entry.candidate_id: entry.rank for entry in result.candidates}
        assert [ranks[item.candidate_id] for item in ordered] == list(range(1, len(items) + 1))
        assert result.selected_candidate_id == ordered[0].candidate_id


def test_missing_stale_future_and_contradictory_evidence_preserve_all(tmp_path):
    u = universe()
    s1, s2 = candidate(), candidate(S2_POLICY)
    for i, kind in enumerate(("missing", "stale", "future", "contradictory", "unindexed")):
        with OpsRepository(tmp_path / f"ops-{i}.sqlite") as repository:
            index(repository, s1)
            index(repository, s2)
            valid = evidence(repository, s1, u, 1)
            stale = evidence(repository, s2, u, 2, available_at_ns=CUTOFF - 6_000_000_000)
            future = evidence(repository, s2, u, 2, available_at_ns=CUTOFF + 1)
            bad = {"missing": (), "stale": (stale,), "future": (future,),
                   "contradictory": (valid, stale), "unindexed": (sha256_json({"missing": 1}),)}[kind]
            result = assemble(repository, u, (s1, s2), {s1.candidate_id: (valid,), s2.candidate_id: bad})
            assert result.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE
            assert len(result.candidates) == 2 and result.selected_candidate_id is None
            assert repository.get_artifact(result.content_hash) is not None
            assert len(repository.artifact_entries("CandidateSetDecisionIndexV1")) == 1


def test_empty_known_rejection_and_invalid_candidate_refs(tmp_path):
    u = universe(ineligible_policy=S2_POLICY.policy_id)
    s2 = candidate(S2_POLICY)
    with OpsRepository(tmp_path / "empty.sqlite") as repository:
        empty = assemble(repository, u, (), {})
        assert empty.selection_status == CandidateSelectionStatus.NO_CANDIDATE
        assert repository.get_artifact(empty.content_hash) is not None
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        with pytest.raises(ValueError, match="indexed"):
            assemble(repository, u, (s2,), {})
        index(repository, s2)
        rejected = assemble(repository, u, (s2,), {})
        assert rejected.selection_status == CandidateSelectionStatus.NO_CANDIDATE
        assert rejected.candidates[0].rejection_reason == "KNOWN_REJECTION"
        with pytest.raises(ValueError, match="duplicate"):
            assemble(repository, u, (s2, s2), {})
        with pytest.raises(ValueError, match="policy"):
            assemble_candidate_set(repository, universe=u, decision_event_id=EVENT, cutoff_ns=CUTOFF,
                candidates=(s2,), policies={S2_POLICY.policy_hash: S1_POLICY}, scanner_evidence_refs={})
        wrong_event = candidate(S1_POLICY, serial=99, decision_at_ns=CUTOFF - 1,
                                deadline_ns=CUTOFF + 5_000_000_000)
        index(repository, wrong_event)
        with pytest.raises(ValueError, match="decision event"):
            assemble(repository, u, (wrong_event,), {})
        expired = candidate(S1_POLICY, serial=98, decision_at_ns=CUTOFF - 10_000_000_000,
                            deadline_ns=CUTOFF - 5_000_000_000)
        index(repository, expired)
        with pytest.raises(ValueError, match="expired"):
            assemble(repository, u, (expired,), {})
        wrong_universe = candidate(S1_POLICY, serial=97)
        index(repository, wrong_universe, universe_ref=sha256_json({"wrong_universe": True}))
        with pytest.raises(ValueError, match="universe reference"):
            assemble(repository, u, (wrong_universe,), {})
        wrong_key = candidate(S1_POLICY, alternate_key())
        index(repository, wrong_key)
        with pytest.raises(ValueError, match="universe"):
            assemble(repository, u, (wrong_key,), {})
