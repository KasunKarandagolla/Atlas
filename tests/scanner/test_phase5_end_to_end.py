from __future__ import annotations

from support.phase4_factory import SLOT, decision_input
from support.scanner_fixture import EPOCH_NS, HOUR_NS, phase4_trade_plan, scanner_fixture

from atlas.scanner import (
    BlindSpotStatus,
    CheapScanInput,
    EligibilityStatus,
    ListingStatus,
    Phase4HandoffRequest,
    RecordingAlertTransport,
    UniverseEntry,
    WarmupEvidence,
    WarmupState,
    blindspot_metrics,
    build_universe_snapshot,
    evaluator_from_phase4,
    run_scan_slot,
)
from atlas.science.evaluation import DecisionStatus
from atlas.science.phase4_engine import evaluate_phase4
from atlas.science.research_archive import ResearchArtifactArchive


def test_phase5_end_to_end_scanner_orchestration_and_persistence(tmp_path):
    fixture = scanner_fixture()
    archive = ResearchArtifactArchive(tmp_path / "research")
    transport = RecordingAlertTransport()
    calls: list[tuple[int, str]] = []

    def evaluator(request):
        calls.append((request.slot_at_ns, request.instrument))
        return fixture.evaluator(request)

    results = [
        run_scan_slot(slot_at_ns=slot, universe=fixture.universes[index], cheap_inputs=fixture.cheap_inputs[index],
                      policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[index],
                      evaluator=evaluator, alert_transport=transport, archive=archive)
        for index, slot in enumerate(fixture.slots)
    ]

    # Point-in-time universe + cheap scan + deterministic ranking + selection.
    first = results[0]
    assert [item.instrument for item in first.ranked[:3]] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert first.exploration.instrument is not None
    assert first.exploration.inclusion_probability > 0
    assert all(row.counterfactual_value is None and row.matured_counterfactual_label_id is None
               for row in first.calendar_rows)
    assert all(item.counterfactual_value is None for item in first.blind_observations)
    assert first.blindspots.status is BlindSpotStatus.INCONCLUSIVE

    # Phase-4 is reused only for capital-enabled, deep-warm BTC/ETH.
    assert sorted(instrument for _, instrument in calls) == ["BTCUSDT", "BTCUSDT", "BTCUSDT",
                                                             "ETHUSDT", "ETHUSDT", "ETHUSDT"]
    btc = next(row for row in first.calendar_rows if row.instrument == "BTCUSDT")
    eth = next(row for row in first.calendar_rows if row.instrument == "ETHUSDT")
    sol = next(row for row in first.calendar_rows if row.instrument == "SOLUSDT")
    assert btc.plan_status == DecisionStatus.TRADE_CANDIDATE.value
    assert btc.trade_plan_id is not None and btc.trade_plan_hash is not None
    assert btc.trade_plan_hash == phase4_trade_plan(fixture.slots[0]).plan_hash()
    assert btc.phase4_evaluation_ref is not None and btc.phase4_evaluation_ref.startswith("phase4-eval")
    assert eth.plan_status == DecisionStatus.NO_SIGNAL.value
    assert sol.plan_status == "NOT_APPLICABLE"

    # Missing warmup and deadline misses stay visible and generate alerts.
    xrp = next(row for row in first.calendar_rows if row.instrument == "XRPUSDT")
    assert xrp.warmup_state.value == "NOT_ESTIMABLE_WARMUP"
    assert xrp.model_deadline_status.value == "MISSED"
    assert any(alert.instrument == "XRPUSDT" for alert in first.alerts)

    # No approval/execution side effects.
    assert all(row.approval_requested_at_ns is None and row.approved_at_ns is None
               and row.entry_attempted is None and row.filled_qty is None for row in first.calendar_rows)

    # Complete calendar persistence across all artifact families.
    for artifact_type in ("universe_snapshot", "cheap_scan", "scanner_rank", "scanner_selection",
                          "exploration_selection", "warmup_status", "scanner_decision_calendar",
                          "scanner_health", "scanner_alert", "blindspot_metrics"):
        assert list((tmp_path / "research" / artifact_type).glob("part-*.parquet"))

    # Health and blind-spot measurement.
    assert results[-1].health.last_completed_scan_slot == fixture.slots[-1]
    assert results[-1].health.assisted_enabled is False
    assert results[-1].health.bybit_capabilities == "UNVERIFIED"
    combined_blindspots = blindspot_metrics(
        tuple(observation for result in results for observation in result.blind_observations),
        tolerance=fixture.policy.blindspot_tolerance)
    assert combined_blindspots.support_slots == len(fixture.slots)
    assert combined_blindspots.status is not None


def test_future_slots_and_sequence_order_do_not_change_an_earlier_scan():
    fixture = scanner_fixture()
    baseline = run_scan_slot(slot_at_ns=fixture.slots[0], universe=fixture.universes[0],
                             cheap_inputs=fixture.cheap_inputs[0], policy=fixture.policy,
                             warmup_evidence=fixture.warmup_evidence[0], evaluator=fixture.evaluator,
                             persist=False)
    for index in (1, 2):
        run_scan_slot(slot_at_ns=fixture.slots[index], universe=fixture.universes[index],
                      cheap_inputs=fixture.cheap_inputs[index], policy=fixture.policy,
                      warmup_evidence=fixture.warmup_evidence[index], evaluator=fixture.evaluator,
                      persist=False)
    repeated = run_scan_slot(slot_at_ns=fixture.slots[0], universe=fixture.universes[0],
                             cheap_inputs=fixture.cheap_inputs[0], policy=fixture.policy,
                             warmup_evidence=fixture.warmup_evidence[0], evaluator=fixture.evaluator,
                             persist=False)
    assert tuple(row.hash() for row in repeated.calendar_rows) == tuple(row.hash() for row in baseline.calendar_rows)
    assert tuple(item.hash() for item in repeated.ranked) == tuple(item.hash() for item in baseline.ranked)


def test_phase4_handoff_adapter_reuses_the_existing_evaluator():
    request = Phase4HandoffRequest(SLOT, "BTCUSDT", SLOT + 1, "universe", "cheap",
                                   WarmupState.WARM_AVAILABLE)
    result = evaluator_from_phase4(lambda _: decision_input())(request)
    baseline = evaluate_phase4(decision_input())
    assert result.status is DecisionStatus.TRADE_CANDIDATE
    assert result.trade_plan is not None
    assert result.evaluation_ref == baseline.evidence.artifact_hash()


def test_exploration_candidate_has_no_phase4_capital_authority():
    slot = EPOCH_NS
    entries = [
        UniverseEntry(slot - HOUR_NS, slot - HOUR_NS // 2, slot - HOUR_NS // 2, "BYBIT", instrument,
                      "PERP", ListingStatus.LISTED, EligibilityStatus.ELIGIBLE, None, f"ref-{instrument}",
                      False, f"contract-{instrument}", f"contract-hash-{instrument}", slot - HOUR_NS,
                      (0.01, 0.0, -0.01))
        for instrument in ("AAAUSDT", "BBBUSDT", "CCCUSDT")
    ]
    entries.append(UniverseEntry(slot - HOUR_NS, slot - HOUR_NS // 2, slot - HOUR_NS // 2, "BYBIT",
                                 "BTCUSDT", "PERP", ListingStatus.LISTED, EligibilityStatus.ELIGIBLE,
                                 None, "ref-BTC", True, "contract-BTC", "contract-hash-BTC", slot - HOUR_NS,
                                 (0.01, 0.0, -0.01)))
    universe = build_universe_snapshot(snapshot_id="u", observed_at_ns=slot - HOUR_NS,
                                       available_at_ns=slot - HOUR_NS // 2, venue="BYBIT", version="V1",
                                       entries=tuple(entries), source_ref="u")
    inputs = tuple(
        CheapScanInput(instrument, slot, slot, return_24h, 0.01, None)
        for instrument, return_24h in (("AAAUSDT", 0.03), ("BBBUSDT", 0.02), ("CCCUSDT", 0.01),
                                       ("BTCUSDT", 0.001))
    )
    fixture = scanner_fixture()
    result = run_scan_slot(slot_at_ns=slot, universe=universe, cheap_inputs=inputs, policy=fixture.policy,
                           warmup_evidence=(WarmupEvidence("BTCUSDT", slot, True, slot - HOUR_NS,
                                                           slot - HOUR_NS // 2, slot - 60_000_000_000),),
                           evaluator=lambda request: (_ for _ in ()).throw(AssertionError("must not evaluate")),
                           persist=False)
    btc = next(row for row in result.calendar_rows if row.instrument == "BTCUSDT")
    assert result.exploration.instrument == "BTCUSDT"
    assert btc.top_k_selected is False
    assert btc.deep_selected is True
    assert btc.warmup_state is WarmupState.WARM_AVAILABLE
    assert btc.plan_status == "NOT_APPLICABLE"
    assert btc.trade_plan_id is None
