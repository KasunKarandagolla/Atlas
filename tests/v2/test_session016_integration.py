"""Bounded public-style S1/S2 candidate competition and research handoff."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ArtifactEnvelope, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.selection import (
    SELECTION_POLICY_HASH,
    ScannerRankEvidenceV1,
    ScannerSelectionSourceV1,
    accept_research_candidates,
    assemble_candidate_set,
    register_scanner_rank,
    register_scanner_source,
)
from atlas.v2.strategies.s1_trend import (
    S1_POLICY,
    MarkIndexEvidence,
    S1ShadowCoordinator,
)
from atlas.v2.strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator

from .test_session014_core import KEY, bar
from .test_session014_s1 import clear, health
from .test_session016_s2 import fixture


def test_bounded_s1_s2_candidate_set_acceptance_and_unselected_handoff(tmp_path):
    _, s2_join, s2_feature, original_universe, quote = fixture()
    cutoff = s2_join.cutoff_ns
    setup_at = cutoff - BarIntervalV2.M15.duration_ns
    universe = UniverseContractV2(ArtifactEnvelope(1, "u016-integration", setup_at, setup_at,
        "fixture", original_universe.envelope.input_refs), "fixture", cutoff, SELECTION_POLICY_HASH,
        original_universe.entries)
    h4 = tuple(bar(i, interval=BarIntervalV2.H4,
        close=str(Decimal(100) + Decimal(i - 131) * Decimal("0.1"))) for i in range(131, 181))
    h1 = []
    for i in range(673, 725):
        close = Decimal(100) + Decimal(i - 675) * Decimal("0.1")
        low = Decimal("99.5") if i == 722 else close - Decimal("0.3")
        h1.append(bar(i, interval=BarIntervalV2.H1, close=str(close),
                      high=str(close + Decimal("0.3")), low=str(low)))
    h1 = tuple(h1)
    setup_join = JoinedBars(KEY, setup_at, h4, h1, s2_join.m15[-61:-1],
                            "AVAILABLE", None, health(setup_at).content_hash)
    trigger_join = replace(setup_join, cutoff_ns=cutoff, m15=s2_join.m15[-60:],
                           source_health_ref=health(cutoff).content_hash)
    setup_feature = feature_snapshot(setup_join)
    trigger_feature = feature_snapshot(trigger_join)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        s1 = S1ShadowCoordinator(repository)
        watch = s1.create_watch(setup_join, setup_feature, event_gate=clear(setup_at), universe=universe)
        assert watch.status == "WATCH", watch.reason
        assert watch.watch is not None
        mark = MarkIndexEvidence(KEY, quote.ask, quote.ask, cutoff, sha256_json({"mark": cutoff}))
        s1_decision = s1.on_bar(watch.watch.watch_id, trigger_join, trigger_feature,
                                event_gate=clear(cutoff), bbo=quote, mark_index=mark)
        assert s1_decision.status == "CANDIDATE", s1_decision.reason
        s2_decision = S2ShadowCoordinator(repository).on_trigger_close(
            s2_join, s2_feature, universe=universe, bbo=quote)
        assert s2_decision.status == "CANDIDATE", s2_decision.reason
        assert s1_decision.candidate is not None and s2_decision.candidate is not None
        candidates = (s1_decision.candidate, s2_decision.candidate)
        refs = {}
        for rank, candidate in ((2, candidates[0]), (1, candidates[1])):
            input_body = {"scanner_fixture": candidate.candidate_id, "rank": rank, "cutoff": cutoff}
            input_ref = sha256_json(input_body)
            repository.register_artifact(ArtifactIndexEntryV2(input_ref, "ScannerInputFixtureV1", input_ref,
                cutoff, cutoff, input_body))
            source_ref = register_scanner_source(repository, ScannerSelectionSourceV1(
                candidate.candidate_id, candidate.key, rank, "SCANNER_V1", "1.0",
                universe.content_hash, "event-016-integration", cutoff, (input_ref,)))
            evidence = ScannerRankEvidenceV1(candidate.candidate_id, rank, "SCANNER_V1", "1.0",
                universe.content_hash, "event-016-integration", cutoff, source_ref)
            refs[candidate.candidate_id] = (register_scanner_rank(repository, evidence),)
        result = assemble_candidate_set(repository, universe=universe,
            decision_event_id="event-016-integration", cutoff_ns=cutoff,
            candidates=candidates, policies={p.policy_hash: p for p in (S1_POLICY, S2_POLICY)},
            scanner_evidence_refs=refs)
        assert result.selected_candidate_id == candidates[1].candidate_id
        assert {entry.candidate_id for entry in result.candidates} == {c.candidate_id for c in candidates}
        receipts = accept_research_candidates(repository, result, candidates, accepted_at_ns=cutoff)
        handed = s1.accept_handoff(watch.watch.watch_id, candidates[0].content_hash,
                                   pipeline_acceptance_ref=receipts[candidates[0].candidate_id],
                                   accepted_at_ns=cutoff)
        assert handed.state == WatchStateV2.HANDED_OFF
        assert repository.get_artifact(receipts[candidates[0].candidate_id]).metadata["selected"] is False
        assert all(candidate.quantity is None for candidate in candidates)
        for forbidden in ("EvaluationArtifactV2", "TradePlanEnvelopeV2", "Approval", "Reservation", "Order"):
            assert repository.artifact_entries(forbidden) == ()
        assert S1_POLICY.policy_hash == "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"
