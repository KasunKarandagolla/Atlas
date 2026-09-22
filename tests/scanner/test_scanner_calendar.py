from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import scanner_fixture

from atlas.scanner import ScannerCalendar, run_scan_slot


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
