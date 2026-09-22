from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import HOUR_NS, SLOT_NS, scanner_fixture

from atlas.scanner import (
    ScannerRunner,
)


def _runner(fixture, calls: dict[str, int]) -> ScannerRunner:
    def universe_provider(slot_at_ns: int):
        calls["universe"] += 1
        return fixture.universes[fixture.slots.index(slot_at_ns)] if slot_at_ns in fixture.slots else fixture.universes[0]

    def cheap_provider(slot_at_ns: int):
        calls["cheap"] += 1
        return fixture.cheap_inputs[0]

    def warmup_provider(slot_at_ns: int):
        calls["warmup"] += 1
        return fixture.warmup_evidence[0]

    return ScannerRunner(policy=fixture.policy, universe_provider=universe_provider,
                         cheap_input_provider=cheap_provider,
                         warmup_evidence_provider=warmup_provider, evaluator=fixture.evaluator)


def test_runner_processes_one_due_slot_once_through_existing_run_scan_slot():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    now = fixture.slots[0] + HOUR_NS
    first = runner.run_due_slot(now_ns=now)
    assert first.status == "COMPLETED"
    assert first.mode == "LIVE"
    assert first.result is not None and len(first.result.calendar_rows) == len(fixture.universes[0].entries)
    assert calls == {"universe": 1, "cheap": 1, "warmup": 1}
    second = runner.run_due_slot(now_ns=now)
    assert second.status == "ALREADY_COMPLETED"
    assert second.result is None
    assert calls == {"universe": 1, "cheap": 1, "warmup": 1}


def test_runner_delegates_to_existing_run_scan_slot(monkeypatch):
    import atlas.scanner.runner as runner_module

    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    original = runner_module.run_scan_slot
    seen: dict[str, object] = {}

    def wrapped(**kwargs):
        seen.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(runner_module, "run_scan_slot", wrapped)
    receipt = runner.run_due_slot(now_ns=fixture.slots[0] + HOUR_NS)
    assert receipt.status == "COMPLETED"
    assert seen["slot_at_ns"] == fixture.slots[0]
    assert seen["policy"] is fixture.policy


def test_runner_rejects_future_universe_cheap_and_warmup_evidence():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    slot = fixture.slots[0]
    late_universe = replace(fixture.universes[0], available_at_ns=slot + HOUR_NS)
    runner.universe_provider = lambda slot_at_ns: late_universe
    with pytest.raises(ValueError, match="future universe snapshot"):
        runner.run_slot(slot, mode="CATCH_UP")

    runner.universe_provider = lambda slot_at_ns: fixture.universes[0]
    runner.cheap_input_provider = lambda slot_at_ns: (
        replace(fixture.cheap_inputs[0][0], availability_cutoff_ns=slot + 1),
    )
    with pytest.raises(ValueError, match="future cheap input"):
        runner.run_slot(slot, mode="CATCH_UP")

    runner.cheap_input_provider = lambda slot_at_ns: fixture.cheap_inputs[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: (
        replace(fixture.warmup_evidence[0][0], job_finished_at_ns=slot + 1),
    )
    with pytest.raises(ValueError, match="future warmup evidence"):
        runner.run_slot(slot, mode="CATCH_UP")


def test_missed_slot_diagnostics_never_reconstruct_with_later_provider_data():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    slot = fixture.slots[0]
    runner.run_slot(slot, mode="LIVE", now_ns=slot + HOUR_NS)
    assert runner.missed_slots(slot, now_ns=slot + 3 * SLOT_NS + HOUR_NS) == (slot + SLOT_NS,
                                                                             slot + 2 * SLOT_NS)

    runner.universe_provider = lambda slot_at_ns: replace(fixture.universes[0],
                                                          available_at_ns=slot_at_ns + HOUR_NS)
    with pytest.raises(ValueError, match="future universe snapshot"):
        runner.run_catch_up(slot + SLOT_NS)


def test_runner_has_no_approval_reservation_or_execution_surface():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    for forbidden in ("approve", "reserve", "submit_order", "place_order", "cancel", "flatten"):
        assert not hasattr(runner, forbidden)
    receipt = runner.run_due_slot(now_ns=fixture.slots[0] + HOUR_NS)
    assert receipt.result is not None
    assert all(row.approval_requested_at_ns is None and row.entry_attempted is None
               for row in receipt.result.calendar_rows)
