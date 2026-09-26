"""Deterministic Session-020 integration from causal strategy evidence to desktop maturity."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import replace
from decimal import Decimal

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import InstrumentRegistryV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
    SelectionStateV2,
    index_decision_calendar_entry,
    index_matured_outcome,
)
from atlas.v2.science.replay import HOUR_NS, ReplayStatusV2
from atlas.v2.selection import (
    ScannerRankEvidenceV1,
    ScannerSelectionSourceV1,
    accept_research_candidates,
    assemble_candidate_set,
    register_scanner_rank,
    register_scanner_source,
)
from atlas.v2.strategies.s1_trend import S1_POLICY, MarkIndexEvidence, S1ShadowCoordinator
from atlas.v2.strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator, failed_break_exit

from . import test_session017_replay as replay_module
from . import test_session017_risk as risk_module
from . import test_session019_evaluation_integration as evaluation_module
from .test_session014_core import KEY, bar
from .test_session014_s1 import clear, health
from .test_session016_s2 import TRIGGER_INDEX
from .test_session016_s2 import fixture as s2_fixture
from .test_session017_replay import run as replay_action
from .test_session020_desktop_ipc import _initialized_db


def _fixture_payload(item) -> bytes:
    index = item.open_at_ns // item.interval.duration_ns
    payload = {"i": index, "close": str(item.close), "high": str(item.high), "low": str(item.low)}
    encoded = canonical_json(payload).encode("utf-8")
    assert hashlib.sha256(encoded).hexdigest() == item.raw.raw_payload_hash
    return encoded


def _archive_fixture_bars(repo: OpsRepository, archive_root, product, bars, cutoff_ns: int) -> None:
    registry = InstrumentRegistryV2()
    registry.register(product)
    archive = ParquetObservationArchiveV2(archive_root)
    collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: cutoff_ns, archive=archive)
    distinct = {item.raw.record_id: item for item in bars}
    for item in sorted(distinct.values(), key=lambda value: (value.open_at_ns, value.interval.value)):
        collector.ingest(item.raw, raw_payload=_fixture_payload(item), bar=item)
    assert collector.flush_archive() is not None


def _build_s1_candidate(repo: OpsRepository, universe: UniverseContractV2, cutoff_ns: int, state: dict):
    joined = state["s2_join"]
    quote = state["quote"]
    assert joined.cutoff_ns == cutoff_ns
    setup_at = cutoff_ns - joined.m15[-1].interval.duration_ns
    setup_join = JoinedBars(
        KEY, setup_at, state["setup_h4"], state["setup_h1"], joined.m15[-61:-1],
        "AVAILABLE", None, health(setup_at).content_hash,
    )
    trigger_join = replace(setup_join, cutoff_ns=cutoff_ns, m15=joined.m15[-60:],
                           source_health_ref=health(cutoff_ns).content_hash)
    setup_feature = feature_snapshot(setup_join)
    trigger_feature = feature_snapshot(trigger_join)
    coordinator = S1ShadowCoordinator(repo)
    watch_result = coordinator.create_watch(
        setup_join, setup_feature, event_gate=clear(setup_at), universe=universe,
    )
    assert watch_result.status == "WATCH", watch_result.reason
    assert watch_result.watch is not None
    mark = MarkIndexEvidence(KEY, quote.ask, quote.ask, cutoff_ns, sha256_json({"mark": cutoff_ns}))
    decision = coordinator.on_bar(watch_result.watch.watch_id, trigger_join, trigger_feature,
        event_gate=clear(cutoff_ns), bbo=quote, mark_index=mark)
    assert decision.status == "CANDIDATE", decision.reason
    assert decision.candidate is not None
    state.update({
        "coordinator": coordinator, "watch": decision.watch, "watch_id": watch_result.watch.watch_id,
        "setup_join": setup_join, "trigger_join": trigger_join, "setup_feature": setup_feature,
        "trigger_feature": trigger_feature, "s2_feature": state["s2_feature"],
        "quote": quote, "s2_join": joined,
    })
    return decision.candidate


def test_phase2_s1_s2_persistence_ipc_and_matured_outcome(tmp_path, monkeypatch):
    from atlas.v2.desktop.ipc import ProjectionClient, ProjectionService
    from atlas.v2.desktop.projection import DesktopChartSeriesV2, DesktopSnapshotV2, project_snapshot

    db = tmp_path / "ops.sqlite"
    _initialized_db(db)
    state = {}
    _, initial_s2, s2_feature, _, quote = s2_fixture()
    state.update({"s2_join": initial_s2, "s2_feature": s2_feature, "quote": quote})
    target_cutoff = initial_s2.cutoff_ns
    setup_at = target_cutoff - initial_s2.m15[-1].interval.duration_ns
    h4 = tuple(bar(i, interval=initial_s2.h4[-1].interval,
        close=str(Decimal(100) + Decimal(i - 131) * Decimal("0.1"))) for i in range(131, 181))
    h1 = tuple(bar(i, interval=initial_s2.h1[-1].interval,
        close=str(Decimal(100) + Decimal(i - 675) * Decimal("0.1")),
        high=str(Decimal(100) + Decimal(i - 675) * Decimal("0.1") + Decimal("0.3")),
        low="99.5" if i == 722 else str(Decimal(100) + Decimal(i - 675) * Decimal("0.1") - Decimal("0.3")))
        for i in range(673, 725))
    state.update({"setup_h4": h4, "setup_h1": h1})
    with OpsRepository(db) as repo:
        case = risk_module.risk_case(
            repo, cutoff_ns=target_cutoff, universe_available_at_ns=setup_at,
            candidate_factory=lambda repository, universe, product:
                _build_s1_candidate(repository, universe, target_cutoff, state),
        )
        state["universe"] = case.universe
        assert case.candidate.policy_hash == S1_POLICY.policy_hash
        assert case.candidate_set.selected_candidate_id == case.candidate.candidate_id
        assert repo.get_artifact(case.candidate.snapshot_hash).artifact_type == "FeatureArtifactV2"

        # The production public collector writes only final fixture candles and their immutable index refs.
        _archive_fixture_bars(repo, tmp_path / "archive", case.product,
            state["trigger_join"].m15 + state["trigger_join"].h1 + state["trigger_join"].h4,
            target_cutoff)
        receipt = accept_research_candidates(repo, case.candidate_set, (case.candidate,),
            accepted_at_ns=target_cutoff)[case.candidate.candidate_id]
        handed = state["coordinator"].accept_handoff(
            state["watch_id"], case.candidate.content_hash, pipeline_acceptance_ref=receipt,
            accepted_at_ns=target_cutoff,
        )
        assert handed.state.value == "HANDED_OFF"

        # S2 uses its own coordinator/policy and management evidence, then the same CandidateSet seam.
        s2_result = S2ShadowCoordinator(repo).on_trigger_close(
            state["s2_join"], state["s2_feature"], universe=case.universe, bbo=state["quote"],
        )
        assert s2_result.status == "CANDIDATE" and s2_result.candidate is not None
        assert s2_result.candidate.policy_hash == S2_POLICY.policy_hash
        setup_entry = repo.get_artifact(s2_result.setup_ref)
        assert setup_entry is not None and setup_entry.artifact_type == "S2SetupEvidenceV1"
        later_bars = (bar(TRIGGER_INDEX + 1, close="100", high="100.4", low="99.6"),
                      bar(TRIGGER_INDEX + 2, close="100.2", high="100.4", low="99.8"))
        failed_ref = failed_break_exit(s2_result.candidate, setup_entry.metadata.to_dict(), later_bars)
        assert failed_ref == later_bars[0].content_hash
        assert canonical_json(S2_POLICY.management_rule) != canonical_json(S1_POLICY.management_rule)

        policies = {spec.policy_hash: spec for spec in (S1_POLICY, S2_POLICY)}
        rank_refs = {}
        for rank, candidate_item in ((1, case.candidate), (2, s2_result.candidate)):
            input_body = {"synthetic_fixture": "SESSION020_PHASE2", "candidate_id": candidate_item.candidate_id,
                          "rank": rank, "cutoff_ns": target_cutoff}
            input_ref = sha256_json(input_body)
            repo.register_artifact(ArtifactIndexEntryV2(input_ref, "ScannerInputFixtureV1", input_ref,
                target_cutoff, target_cutoff, input_body))
            source_ref = register_scanner_source(repo, ScannerSelectionSourceV1(
                candidate_item.candidate_id, candidate_item.key, rank, "SCANNER_V1", "1.0",
                case.universe.content_hash, "session020-shared-candidates", target_cutoff, (input_ref,),
            ))
            rank_refs[candidate_item.candidate_id] = (register_scanner_rank(repo, ScannerRankEvidenceV1(
                candidate_item.candidate_id, rank, "SCANNER_V1", "1.0", case.universe.content_hash,
                "session020-shared-candidates", target_cutoff, source_ref,
            )),)
        shared_set = assemble_candidate_set(repo, universe=case.universe,
            decision_event_id="session020-shared-candidates", cutoff_ns=target_cutoff,
            candidates=(case.candidate, s2_result.candidate), policies=policies,
            scanner_evidence_refs=rank_refs)
        assert shared_set.selected_candidate_id == case.candidate.candidate_id
        s2_member = next(item for item in shared_set.candidates if item.candidate_id == s2_result.candidate.candidate_id)
        s2_calendar = DecisionCalendarEntryV2(
            shared_set.content_hash, s2_result.candidate.content_hash, S2_POLICY.policy_id, S2_POLICY.version,
            S2_POLICY.policy_hash, target_cutoff, SelectionStateV2.UNSELECTED, AdmissionStateV2.NOT_APPLICABLE,
            None, None, DecisionSourceStageV2.CANDIDATE_SET, (), shared_set.content_hash,
            shared_set.envelope.available_at_ns, shared_set.envelope.available_at_ns,
        )
        assert s2_member.policy_id == S2_POLICY.policy_id

        # Reuse the production Session-019 evaluator path with this exact S1 candidate and universe.
        original_risk_case = risk_module.risk_case
        external_candidate = case.candidate
        external_universe = case.universe
        monkeypatch.setattr(risk_module, "CUTOFF", target_cutoff)
        monkeypatch.setattr(replay_module, "CUTOFF", target_cutoff)
        monkeypatch.setattr(evaluation_module, "CUTOFF", target_cutoff)
        monkeypatch.setattr(risk_module, "risk_case", lambda repository: original_risk_case(
            repository, cutoff_ns=target_cutoff, universe_override=external_universe,
            candidate_override=external_candidate,
        ))
        monkeypatch.setattr(risk_module, "candidate", lambda *args, **kwargs: external_candidate)
        evaluation_module.test_unestimable_exact_action_persists_amended_evaluation_and_terminal_calendar(
            tmp_path, monkeypatch,
        )
        index_decision_calendar_entry(repo, s2_calendar)
        evaluation_entry = repo.artifact_entries("EvaluationArtifactV2")[-1]
        calendar_entry = next(item for item in repo.artifact_entries("DecisionCalendarEntryV2")
            if item.metadata.get("decision_entry", {}).get("source_stage") == "ECONOMIC_EVALUATION")
        evaluation_body = evaluation_entry.metadata["evaluation"]
        assert evaluation_body["decision"] == "NOT_ESTIMABLE"
        assert evaluation_body["reason_codes"]
        original_calendar_wire = canonical_json(calendar_entry.metadata["decision_entry"])
        assert evaluation_body["candidate_ref"] == case.candidate.content_hash

        context = replay_module.replay_context(repo, case, minutes=(
            replay_module.minute(target_cutoff),
            replay_module.minute(target_cutoff + 4 * HOUR_NS, bid="110", ask="110",
                mark_low="110", mark_high="110", last_low="110", last_high="110"),
        ))
        action = context[0]
        payoff = replay_action(repo, case, context)
        assert payoff.status == ReplayStatusV2.FULL_FILL and payoff.payoff is not None
        assert action.content_hash == calendar_entry.metadata["decision_entry"]["action_artifact_ref"]
        fees = (payoff.entry.fee if payoff.entry else Decimal(0)) + sum((item.fee for item in payoff.exits), Decimal(0))
        funding = sum((cash for _, cash in payoff.funding_cashflows), Decimal(0))
        outcome = MaturedOutcomeV2(
            decision_ref=calendar_entry.artifact_ref, candidate_set_ref=case.candidate_set.content_hash,
            candidate_ref=case.candidate.content_hash, policy_id=action.action.policy_id,
            policy_version=action.action.policy_version, policy_hash=action.action.policy_hash,
            action_hash=action.action.action_hash, action_artifact_ref=action.content_hash, action_absence_reason=None,
            instrument_revision=case.candidate.key.contract_revision, venue=case.candidate.key.venue.value,
            product=case.candidate.key.product.value, decision_at_ns=target_cutoff,
            horizon_end_ns=case.candidate.horizon_end_ns, matured_at_ns=payoff.available_at_ns,
            available_at_ns=payoff.available_at_ns + 1, label_definition="net_action_value_v2",
            label_view="RECONSTRUCTED_MARKET", selection_state=SelectionStateV2.SELECTED,
            admission_state=AdmissionStateV2.NOT_ESTIMABLE,
            execution_state=ExecutionOutcomeStateV2(payoff.status.value), label_state=LabelStateV2.MATURED,
            provenance=OutcomeProvenanceV2.SIMULATED, payoff_unit="USDT", quantity_unit="CONTRACTS",
            gross_payoff=payoff.payoff + fees - funding, fees=fees, funding_cashflow=funding,
            net_payoff=payoff.payoff, fill_quantity=payoff.filled_quantity,
            requested_quantity=action.action.quantity, mfe=None, mae=None,
            evidence_refs=(payoff.content_hash,), execution_evidence_ref=payoff.content_hash,
            extrema_evidence_ref=None, actual_closed_source_ref=None, evidence_resolution="MINUTE",
            evidence_quality="REPLAY_BOUND", ambiguity=(),
            outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE, diagnostic_value=None,
            diagnostic_unit=None, diagnostic_evidence_ref=None, actual_action_binding_ref=None,
        )
        outcome_ref = index_matured_outcome(repo, outcome)
        assert outcome_ref == outcome.content_hash
        assert canonical_json(repo.get_artifact(calendar_entry.artifact_ref).metadata["decision_entry"]) == original_calendar_wire
        assert outcome.action_hash == action.action.action_hash

        # The fixture marker makes synthetic status explicit across scanner, chart, and evidence views.
        subject_refs = {
            case.universe.content_hash, case.product.content_hash, case.candidate.content_hash,
            case.candidate.snapshot_hash, case.candidate_set.content_hash, shared_set.content_hash,
            s2_result.candidate.content_hash, s2_result.setup_ref, s2_result.trigger_ref,
            action.content_hash, evaluation_entry.artifact_ref, calendar_entry.artifact_ref,
            shared_set.content_hash, outcome_ref, payoff.content_hash,
        }
        subject_refs.update(item.content_hash for item in state["trigger_join"].m15 + state["trigger_join"].h1)
        subject_refs.update(sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": item.raw.record_id})
                            for item in state["trigger_join"].m15 + state["trigger_join"].h1)
        marker_body = {"synthetic_fixture": True, "fixture_name": "SESSION020_PHASE2_E2E",
                       "subject_refs": sorted(subject_refs)}
        marker_ref = sha256_json(marker_body)
        repo.register_artifact(ArtifactIndexEntryV2(marker_ref, "SyntheticIntegrationFixtureV1",
            marker_ref, target_cutoff, target_cutoff, marker_body))

        snapshot = project_snapshot(repo)
        scanner_by_policy = {row.policy_hash: row for row in snapshot.scanner_rows if row.policy_hash}
        assert S1_POLICY.policy_hash in scanner_by_policy and S2_POLICY.policy_hash in scanner_by_policy
        s1_row = scanner_by_policy[S1_POLICY.policy_hash]
        s2_row = scanner_by_policy[S2_POLICY.policy_hash]
        assert s1_row.evaluation_decision == "NOT_ESTIMABLE"
        assert s1_row.reason_codes == tuple(evaluation_body["reason_codes"])
        assert s1_row.frozen_action_ref == action.content_hash and s1_row.synthetic_fixture
        assert s2_row.selection_state == "UNSELECTED"
        assert s2_row.evaluation_decision == AdmissionStateV2.NOT_APPLICABLE.value
        assert s2_row.synthetic_fixture
        evidence_types = {row.artifact_type for row in snapshot.evidence}
        assert {"UniverseContractV2", "ProductContractV2", "FeatureArtifactV2", "CandidateSetV2",
                "SizingDecisionV2", "ActionArtifactV2", "EvaluationArtifactV2", "DecisionCalendarEntryV2",
                "MaturedOutcomeV2", "S2SetupEvidenceV1", "S2TriggerEvidenceV1"}.issubset(evidence_types)
        assert any(row.content_ref == outcome_ref and row.synthetic_fixture for row in snapshot.evidence)
        assert next(row for row in snapshot.evidence if row.content_ref == outcome_ref).label_target == (
            OutcomeTargetV2.EXECUTABLE_ACTION_VALUE.value
        )
        chart = __import__("atlas.v2.desktop.projection", fromlist=["project_chart_series"]).project_chart_series(
            repo, key_json=case.candidate.key.to_canonical_json(), interval="1H",
            information_cutoff_ns=snapshot.generated_at_ns, archive_root=tmp_path / "archive", limit=3,
        )
        assert chart.state == "AVAILABLE" and chart.bars and chart.synthetic_fixture
        assert chart.reason_code == "SERIES_LIMIT_APPLIED"

        token = secrets.token_urlsafe(48)
        service = ProjectionService(db, token, archive_root=tmp_path / "archive")
        service.start()
        host, port = service.address
        try:
            ipc = ProjectionClient(host, port, token)
            projected = DesktopSnapshotV2.from_dict(ipc.request("snapshot"))
            assert projected.freshness_state == "CURRENT"
            assert any(row.content_ref == outcome_ref for row in projected.evidence)
            assert token not in projected.to_canonical_json()
            desktop_chart = ipc.request("chart", {
                "key_json": case.candidate.key.to_canonical_json(), "interval": "1H",
                "information_cutoff_ns": projected.generated_at_ns, "availability_view": "ACTUAL_SYSTEM",
                "limit": 2000,
            })
            assert desktop_chart["state"] == "AVAILABLE" and desktop_chart["synthetic_fixture"] is True
            monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
            from PySide6.QtWidgets import QApplication

            from atlas.desktop.app import AtlasDesktop

            app = QApplication.instance() or QApplication([])
            desktop = AtlasDesktop(ipc)
            desktop._render_chart(DesktopChartSeriesV2.from_dict(desktop_chart))
            app.processEvents()
            assert desktop.chart.getPlotItem().items
            assert "information cutoff" in desktop.chart_title.text()
            desktop.close()
        finally:
            service.close()
