from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import HOUR_NS, scanner_fixture

from atlas.scanner import (
    BlindSpotStatus,
    ScannerCalendar,
    blindspot_metrics,
    observations_from_matured,
    run_scan_slot,
)


def _run(fixture, slot_index: int, slot):
    return run_scan_slot(
        slot_at_ns=slot,
        universe=fixture.universes[slot_index],
        cheap_inputs=fixture.cheap_inputs[slot_index],
        policy=fixture.policy,
        warmup_evidence=fixture.warmup_evidence[slot_index],
        evaluator=fixture.evaluator,
        persist=False,
    )


def test_every_universe_instrument_gets_an_immutable_calendar_row():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    result = _run(fixture, 0, slot)
    universe_instruments = {entry.instrument for entry in fixture.universes[0].entries}
    assert {row.instrument for row in result.calendar_rows} == universe_instruments
    calendar = ScannerCalendar()
    for row in result.calendar_rows:
        calendar.append(row)
    calendar.assert_complete(slot, tuple(entry.instrument for entry in fixture.universes[0].entries))
    assert calendar.missing(slot, ("BTCUSDT", "NEWUSDT")) == ("NEWUSDT",)
    btc = next(row for row in result.calendar_rows if row.instrument == "BTCUSDT")
    assert btc.selection_reason == "CAPITAL_WARM_AND_TOP_K"


def test_excluded_instruments_are_retained_with_reason():
    fixture = scanner_fixture()
    result = _run(fixture, 0, fixture.slots[0])
    excluded = next(row for row in result.calendar_rows if row.instrument == "LUNAUSDT")
    assert excluded.exclusion_reason == "DELISTED_RETAINED_IN_HISTORICAL_UNIVERSE"
    assert excluded.cheap_score is None
    assert excluded.rank is None


def test_calendar_rejects_conflicting_rewrite_of_one_slot_instrument():
    fixture = scanner_fixture()
    result = _run(fixture, 0, fixture.slots[0])
    calendar = ScannerCalendar()
    row = result.calendar_rows[0]
    calendar.append(row)
    with pytest.raises(ValueError, match="immutable"):
        calendar.append(replace(row, plan_status="TRADE_CANDIDATE"))


def test_maturation_is_horizon_bound_idempotent_and_append_only():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    result = _run(fixture, 0, slot)
    calendar = ScannerCalendar()
    for row in result.calendar_rows:
        calendar.append(row)
    decision_hashes = tuple(row.hash() for row in calendar.records())
    with pytest.raises(ValueError, match="before the outcome horizon"):
        calendar.mature(slot, "BTCUSDT", matured_at_ns=slot + HOUR_NS,
                        matured_counterfactual_label_id="label", realized_policy_outcome_id="outcome",
                        counterfactual_value=1.0, outcome_status="MATURED", evidence_ref="ref",
                        evidence_hash="hash")
    first = calendar.mature(slot, "BTCUSDT", matured_at_ns=slot + 24 * HOUR_NS,
                            matured_counterfactual_label_id="label", realized_policy_outcome_id="outcome",
                            counterfactual_value=1.23, outcome_status="MATURED", evidence_ref="ref",
                            evidence_hash="hash")
    assert calendar.mature(slot, "BTCUSDT", matured_at_ns=slot + 24 * HOUR_NS,
                           matured_counterfactual_label_id="label", realized_policy_outcome_id="outcome",
                           counterfactual_value=1.23, outcome_status="MATURED", evidence_ref="ref",
                           evidence_hash="hash") == first
    with pytest.raises(ValueError, match="conflicting"):
        calendar.mature(slot, "BTCUSDT", matured_at_ns=slot + 24 * HOUR_NS,
                        matured_counterfactual_label_id="label", realized_policy_outcome_id="outcome",
                        counterfactual_value=9.99, outcome_status="MATURED", evidence_ref="ref",
                        evidence_hash="hash")
    assert first.original_row_hash == next(row for row in result.calendar_rows
                                           if row.instrument == "BTCUSDT").hash()
    assert all(row.counterfactual_value is None for row in calendar.records())
    assert tuple(row.hash() for row in calendar.records()) == decision_hashes


def test_blindspot_inputs_are_inconclusive_until_outcomes_mature():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    result = _run(fixture, 0, slot)
    calendar = ScannerCalendar()
    for row in result.calendar_rows:
        calendar.append(row)
    decision_time = observations_from_matured(calendar.records(), calendar.maturations())
    assert decision_time == ()
    assert blindspot_metrics(decision_time).status is BlindSpotStatus.INCONCLUSIVE
    calendar.mature(slot, "BTCUSDT", matured_at_ns=slot + 24 * HOUR_NS,
                    matured_counterfactual_label_id="label", realized_policy_outcome_id="outcome",
                    counterfactual_value=1.23, outcome_status="MATURED", evidence_ref="ref",
                    evidence_hash="hash")
    matured = observations_from_matured(calendar.records(), calendar.maturations())
    btc = next(item for item in matured if item.instrument == "BTCUSDT")
    assert btc.counterfactual_value == 1.23
