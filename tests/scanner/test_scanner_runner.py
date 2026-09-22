from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import HOUR_NS, SLOT_NS, scanner_fixture

from atlas.scanner import DeadlineStatus, RecordingAlertTransport, ScannerRunner, WarmupState
from atlas.science.research_archive import ResearchArtifactArchive


def _runner(fixture, calls: dict[str, int], **kwargs) -> ScannerRunner:
    def index(slot_at_ns: int) -> int:
        return fixture.slots.index(slot_at_ns) if slot_at_ns in fixture.slots else 0

    def universe_provider(slot_at_ns: int):
        calls["universe"] += 1
        return fixture.universes[index(slot_at_ns)]

    def cheap_provider(slot_at_ns: int):
        calls["cheap"] += 1
        return fixture.cheap_inputs[index(slot_at_ns)]

    def warmup_provider(slot_at_ns: int):
        calls["warmup"] += 1
        return fixture.warmup_evidence[index(slot_at_ns)]

    return ScannerRunner(policy=fixture.policy, universe_provider=universe_provider,
                         cheap_input_provider=cheap_provider,
                         warmup_evidence_provider=warmup_provider, evaluator=fixture.evaluator, **kwargs)


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
    now = slot + HOUR_NS
    late_universe = replace(fixture.universes[0], available_at_ns=slot + HOUR_NS)
    runner.universe_provider = lambda slot_at_ns: late_universe
    with pytest.raises(ValueError, match="future universe snapshot"):
        runner.run_slot(slot, mode="CATCH_UP", now_ns=now)

    runner.universe_provider = lambda slot_at_ns: fixture.universes[0]
    runner.cheap_input_provider = lambda slot_at_ns: (
        replace(fixture.cheap_inputs[0][0], availability_cutoff_ns=slot + 1),
    )
    with pytest.raises(ValueError, match="future cheap input"):
        runner.run_slot(slot, mode="CATCH_UP", now_ns=now)

    runner.cheap_input_provider = lambda slot_at_ns: fixture.cheap_inputs[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: (
        replace(fixture.warmup_evidence[0][0], job_finished_at_ns=now + 1),
    )
    with pytest.raises(ValueError, match="future warmup evidence"):
        runner.run_slot(slot, mode="CATCH_UP", now_ns=now)

    runner.warmup_evidence_provider = lambda slot_at_ns: (
        replace(fixture.warmup_evidence[0][0], job_enqueued_at_ns=slot + 21_000_000_000,
                job_started_at_ns=slot + 20_000_000_000, job_finished_at_ns=slot + 22_000_000_000),
    )
    with pytest.raises(ValueError, match="invalid warmup timestamp ordering"):
        runner.run_slot(slot, mode="CATCH_UP", now_ns=now)


def _post_slot_warmup(fixture, slot: int, finished_offset_ns: int):
    return tuple(replace(item, job_enqueued_at_ns=slot + 1_000_000_000,
                         job_started_at_ns=slot + 10_000_000_000,
                         job_finished_at_ns=slot + finished_offset_ns)
                 for item in fixture.warmup_evidence[0])


def test_post_slot_warmup_within_deadline_is_met():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    slot = fixture.slots[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: _post_slot_warmup(fixture, slot, 20_000_000_000)
    receipt = runner.run_due_slot(now_ns=slot + 60_000_000_000)
    assert receipt.status == "COMPLETED"
    assert receipt.result is not None
    btc = next(row for row in receipt.result.calendar_rows if row.instrument == "BTCUSDT")
    assert btc.warmup_state is WarmupState.WARM_AVAILABLE
    assert btc.model_deadline_status is DeadlineStatus.MET


def test_post_slot_warmup_after_deadline_is_visible_as_missed():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)
    slot = fixture.slots[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: _post_slot_warmup(fixture, slot, 40_000_000_000)
    receipt = runner.run_due_slot(now_ns=slot + 60_000_000_000)
    assert receipt.status == "COMPLETED"
    assert receipt.result is not None
    btc = next(row for row in receipt.result.calendar_rows if row.instrument == "BTCUSDT")
    assert btc.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP
    assert btc.model_deadline_status is DeadlineStatus.MISSED


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


def test_catch_up_is_diagnostic_only_and_does_not_touch_authoritative_state(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    archive = ResearchArtifactArchive(tmp_path / "research")
    external = RecordingAlertTransport()
    runner = _runner(fixture, calls, archive=archive, alert_transport=external)
    live = runner.run_due_slot(now_ns=fixture.slots[0] + HOUR_NS)
    assert live.status == "COMPLETED"
    authoritative = tuple(row.hash() for row in runner.calendar.records())
    archive_paths = set(archive.root.rglob("part-*.parquet"))
    external_deliveries = tuple(external.deliveries)

    diagnostic = runner.run_catch_up(fixture.slots[1], now_ns=fixture.slots[1] + HOUR_NS)
    assert diagnostic.status == "DIAGNOSTIC_ONLY"
    assert diagnostic.result is not None
    assert tuple(row.hash() for row in runner.calendar.records()) == authoritative
    assert not any(row.scan_slot_at_ns == fixture.slots[1] for row in runner.calendar.records())
    assert set(archive.root.rglob("part-*.parquet")) == archive_paths
    assert tuple(external.deliveries) == external_deliveries
    assert diagnostic.result.alert_deliveries
    assert all(delivery.transport == "recording-stub" for delivery in diagnostic.result.alert_deliveries)
