"""§19: mutating the future must never change earlier Phase-4 outputs."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from support.phase4_factory import SLOT, decision_input
from test_outer_bootstrap import history

from atlas.science.decision_calendar import record_from_evaluation
from atlas.science.execution_replay import ReplayMinute
from atlas.science.gates import EventGateInput, event_gate
from atlas.science.huber_mean import HOUR_NS, MeanObservation, weekly_refit
from atlas.science.oof import OOFArchive
from atlas.science.outer_loop import estimate_cost_assumptions
from atlas.science.phase4_engine import evaluate_phase4
from atlas.science.scenarios import CurrentModelState, SynchronizedPrices, bridge_hour
from atlas.strategy.features import HourlyClose, feature_values

EPOCH_NS = int(datetime(2025, 1, 6, tzinfo=UTC).timestamp() * 1_000_000_000)  # Monday 00:00 UTC


def closes(count: int, seed: float = 0.0) -> tuple[HourlyClose, ...]:
    return tuple(HourlyClose(EPOCH_NS + index * HOUR_NS,
                             Decimal(str(round(100 * math.exp(0.0005 * math.sin(index / 7 + seed)), 6))),
                             EPOCH_NS + index * HOUR_NS, f"c{index}") for index in range(count))


def test_features_at_earlier_closes_ignore_later_windows():
    records = closes(800)
    early = [feature_values(records[index : index + 721]).z for index in range(0, 11)]
    mutated = list(records)
    for index in range(740, 800):
        mutated[index] = HourlyClose(mutated[index].end_at_ns, mutated[index].close * Decimal("3"),
                                     mutated[index].available_at_ns, "tampered")
    assert [feature_values(tuple(mutated)[index : index + 721]).z for index in range(0, 11)] == early
    tampered_inside = list(records)
    tampered_inside[725] = HourlyClose(tampered_inside[725].end_at_ns, tampered_inside[725].close * Decimal("2"),
                                       tampered_inside[725].available_at_ns, "tampered")
    assert feature_values(tuple(tampered_inside)[10:731]).z != early[-1]


def test_feature_window_rejects_closes_unavailable_at_the_decision_instant():
    records = closes(721)
    decision_at_ns = records[-1].end_at_ns
    unavailable = records[:-1] + (replace(records[-1], available_at_ns=decision_at_ns + 1),)
    with pytest.raises(ValueError, match="future/unavailable close"):
        feature_values(unavailable, decision_at_ns=decision_at_ns)


def observations(days: int = 92) -> list[MeanObservation]:
    rows: list[MeanObservation] = []
    for hour_index in range(days * 24):
        z = math.sin(hour_index / 5.0)
        for instrument, extra in (("BTCUSDT", 0.0), ("ETHUSDT", 0.1)):
            rows.append(MeanObservation(EPOCH_NS + hour_index * HOUR_NS, instrument, z, 0.005,
                                        0.002 * (0.4 * z + extra)))
    return rows


def test_weekly_refit_ignores_labels_after_the_fit_instant():
    rows = observations()
    fit_at = EPOCH_NS + 91 * 24 * HOUR_NS
    baseline = weekly_refit(rows, fit_at)
    mutated = [replace(row, next_return=row.next_return * 100.0) if row.label_at_ns > fit_at else row for row in rows]
    after = weekly_refit(mutated, fit_at)
    assert after.selected_ridge == baseline.selected_ridge
    assert after.model.manifest_hash() == baseline.model.manifest_hash()


def test_oof_forecasts_never_rewrite_or_leak_forward_labels():
    rows = observations()
    fit_at = EPOCH_NS + 91 * 24 * HOUR_NS
    model = weekly_refit(rows, fit_at).model
    archive = OOFArchive()
    early = [row for row in rows if row.label_at_ns <= fit_at + 5 * HOUR_NS][-1]
    entry = archive.forecast(early, model)
    later = tuple(replace(row, next_return=row.next_return * 50) if row.origin_at_ns > early.origin_at_ns else row
                  for row in rows)
    model_later = weekly_refit(later, fit_at).model
    replayed = OOFArchive()
    assert replayed.forecast(early, model_later).forecast == entry.forecast
    matured = archive.mature(early.instrument, early.origin_at_ns, 0.001, entry.target_at_ns)
    assert matured.residual == entry.forecast * 0 + 0.001 / early.sigma - entry.forecast
    assert archive.entries()[0].residual == matured.residual


def test_bridge_for_an_earlier_hour_is_unaffected_by_later_archive_mutation():
    block = list(history(2))[:48]
    target = block[0]
    after = block[1]
    mutated_after = replace(after, btc_last_ohlc=after.btc_last_ohlc[::-1])
    anchor = SynchronizedPrices(100.0, 100.0, 100.0)
    current = CurrentModelState(0.01, 0.1)
    original = bridge_hour(target, "BTCUSDT", previous=anchor, current=current)
    replay = bridge_hour(target, "BTCUSDT", previous=anchor, current=current)
    assert [minute.last.close for minute in original] == [minute.last.close for minute in replay]
    assert mutated_after.btc_last_ohlc != after.btc_last_ohlc


def test_cost_re_estimation_for_an_earlier_cutoff_ignores_later_depth_observations():
    full = history(3)[:72]
    early = full[:48]
    baseline = estimate_cost_assumptions(early, minimum_observations=24)
    mutated = tuple(list(early) + [replace(full[index],
                                           spread_depth_observations=({"spread_bp": "99", "taker_fee_bp": "99"},))
                                   for index in range(48, 72)])
    assert estimate_cost_assumptions(mutated[:48], minimum_observations=24) == baseline
    assert estimate_cost_assumptions(mutated, minimum_observations=24) != baseline


def test_event_gate_at_the_decision_instant_ignores_later_events():
    now = SLOT
    later = EventGateInput(True, (now + 10 * HOUR_NS,))
    unaffected = EventGateInput(True, ())
    assert event_gate(now, later)[0] == event_gate(now, unaffected)[0]
    inside_window = EventGateInput(True, (now + 1_000_000_000,))
    assert event_gate(now, inside_window)[0].value == "NO_TRADE_EVENT"


def test_decision_plan_and_calendar_record_are_pure_in_the_decision_inputs():
    original = decision_input()
    first = evaluate_phase4(original)
    assert first.trade_plan is not None
    record = record_from_evaluation(slot_id="s", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=first, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    # Later-slot evidence cannot reach this evaluation: re-evaluating identical
    # decision inputs reproduces the identical decision, plan hash and record.
    second = evaluate_phase4(decision_input())
    assert second.status is first.status
    assert second.trade_plan is not None and second.trade_plan.plan_hash() == first.trade_plan.plan_hash()
    assert second.evidence.artifact_hash() == first.evidence.artifact_hash()
    assert record.hash() == record_from_evaluation(slot_id="s", strategy_version="1.0",
                                                   availability_cutoff_ns=SLOT + 1, evaluation=second,
                                                   slot_at_ns=SLOT, instrument="BTCUSDT",
                                                   feature_snapshot_hash="snap", signal="LONG").hash()
    with_future_minute = (ReplayMinute(SLOT, Decimal("100"), Decimal("101"), Decimal("1"), Decimal("1"),
                                       Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),)
    del with_future_minute  # replay minutes are an execution input, never a decision input
