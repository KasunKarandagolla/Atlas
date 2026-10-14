from __future__ import annotations

import json
from dataclasses import replace

import pytest
from support.scanner_fixture import HOUR_NS, SLOT_NS, scanner_fixture

from atlas.scanner import DeadlineStatus, RecordingAlertTransport, ScannerRunner, WarmupState
from atlas.scanner.slot_ledger import ScannerSlotLedger
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


def test_runner_processes_one_due_slot_once_through_existing_run_scan_slot(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
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


def test_due_slot_is_durably_registered_before_provider_and_recovered_after_failure(tmp_path):
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    now = slot + HOUR_NS
    archive = ResearchArtifactArchive(tmp_path / "research")
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=archive)

    def unavailable_universe(slot_at_ns: int):
        assert slot_at_ns == slot
        # This assertion is the defect reproducer: durable expected-slot
        # registration must already exist before the first provider runs.
        assert len(ScannerSlotLedger(archive).registrations()) == 1
        assert ScannerSlotLedger(archive).latest_progress(slot)["status"] == "STARTED"
        raise OSError("fixture source outage")

    runner.universe_provider = unavailable_universe
    with pytest.raises(OSError, match="fixture source outage"):
        runner.run_due_slot(now_ns=now)

    ledger = ScannerSlotLedger(archive)
    registrations = ledger.registrations()
    assert len(registrations) == 1
    assert registrations[0]["scan_slot_id"] == f"scan-{slot}"
    assert registrations[0]["scanner_policy_hash"] == fixture.policy.hash()
    assert registrations[0]["deadline_at_ns"] == slot + SLOT_NS
    assert ledger.latest_progress(slot)["status"] == "SOURCE_UNAVAILABLE"
    assert ledger.latest_progress(slot)["reason_code"] == "UNIVERSE_SOURCE_UNAVAILABLE"
    registration_ref = ledger.register(slot_at_ns=slot, policy=fixture.policy, registered_at_ns=now + 1)
    assert ledger.register(slot_at_ns=slot, policy=fixture.policy, registered_at_ns=now + 2) == registration_ref
    assert len(ledger.registrations()) == 1

    changed_policy = replace(fixture.policy, policy_version="SCANNER_CHANGED_POLICY_V1")
    changed_runner = ScannerRunner(policy=changed_policy, universe_provider=runner.universe_provider,
        cheap_input_provider=runner.cheap_input_provider,
        warmup_evidence_provider=runner.warmup_evidence_provider,
        archive=archive, evaluator=fixture.evaluator)
    with pytest.raises(ValueError, match="registration conflicts"):
        changed_runner.run_due_slot(now_ns=now)

    recovered = _runner(fixture, calls, archive=archive)
    recovered_receipt = recovered.run_due_slot(now_ns=now + 2)
    assert recovered_receipt.status == "COMPLETED"
    assert recovered_receipt.registration_ref == registration_ref
    assert len(ledger.registrations()) == 1
    assert ledger.latest_progress(slot)["status"] == "COMPLETED"

    after_restart_calls = dict(calls)
    restarted = _runner(fixture, calls, archive=archive)
    restarted.universe_provider = lambda _slot: pytest.fail("completed slot must not be scanned twice")
    duplicate = restarted.run_due_slot(now_ns=now)
    assert duplicate.status == "ALREADY_COMPLETED"
    assert calls == after_restart_calls


def test_runner_delegates_to_existing_run_scan_slot(monkeypatch, tmp_path):
    import atlas.scanner.runner as runner_module

    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
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


def test_post_slot_warmup_within_deadline_is_met(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
    slot = fixture.slots[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: _post_slot_warmup(fixture, slot, 20_000_000_000)
    receipt = runner.run_due_slot(now_ns=slot + 60_000_000_000)
    assert receipt.status == "COMPLETED"
    assert receipt.result is not None
    btc = next(row for row in receipt.result.calendar_rows if row.instrument == "BTCUSDT")
    assert btc.warmup_state is WarmupState.WARM_AVAILABLE
    assert btc.model_deadline_status is DeadlineStatus.MET


def test_post_slot_warmup_after_deadline_is_visible_as_missed(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
    slot = fixture.slots[0]
    runner.warmup_evidence_provider = lambda slot_at_ns: _post_slot_warmup(fixture, slot, 40_000_000_000)
    receipt = runner.run_due_slot(now_ns=slot + 60_000_000_000)
    assert receipt.status == "COMPLETED"
    assert receipt.result is not None
    btc = next(row for row in receipt.result.calendar_rows if row.instrument == "BTCUSDT")
    assert btc.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP
    assert btc.model_deadline_status is DeadlineStatus.MISSED


def test_missed_slot_diagnostics_never_reconstruct_with_later_provider_data(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
    slot = fixture.slots[0]
    runner.run_slot(slot, mode="LIVE", now_ns=slot + HOUR_NS)
    assert runner.missed_slots(slot, now_ns=slot + 3 * SLOT_NS + HOUR_NS) == (slot + SLOT_NS,
                                                                             slot + 2 * SLOT_NS)

    runner.universe_provider = lambda slot_at_ns: replace(fixture.universes[0],
                                                          available_at_ns=slot_at_ns + HOUR_NS)
    with pytest.raises(ValueError, match="future universe snapshot"):
        runner.run_catch_up(slot + SLOT_NS)


def test_prospective_calendar_reconstructs_only_registered_slots_and_marks_overdue_after_restart(tmp_path):
    fixture = scanner_fixture()
    archive = ResearchArtifactArchive(tmp_path / "research")
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=archive)
    now = fixture.slots[0] + HOUR_NS

    refs = runner.schedule_future_slots(now_ns=now, count=3)
    expected = tuple(fixture.slots[0] + offset * SLOT_NS for offset in (1, 2, 3))
    assert tuple(row["due_at_ns"] for row in runner.slot_ledger.registrations()) == expected
    assert len(set(refs)) == 3
    assert all(row["registered_at_ns"] == now for row in runner.slot_ledger.registrations())
    assert calls == {"universe": 0, "cheap": 0, "warmup": 0}
    with pytest.raises(ValueError, match="must be registered before its due time"):
        runner.slot_ledger.register_prospective(slot_at_ns=expected[0], policy=fixture.policy,
                                                registered_at_ns=expected[0])
    with pytest.raises(ValueError, match="durable slot registration"):
        runner.slot_ledger.record_progress(
            registration_ref="unbound", slot_at_ns=expected[0], attempt=1, status="STARTED",
            observed_at_ns=expected[0],
        )

    restarted = _runner(fixture, calls, archive=archive)
    assert restarted.schedule_future_slots(now_ns=now + 1, count=3) == refs
    assert len(ScannerSlotLedger(archive).registrations()) == 3

    # Only the registered first slot is overdue at this cutoff. The second
    # registered slot is due but inside its deadline; the third is future.
    missed = restarted.reconcile_missed_slots(now_ns=expected[0] + SLOT_NS)
    assert missed == (expected[0],)
    ledger = ScannerSlotLedger(archive)
    assert ledger.latest_progress(expected[0])["status"] == "MISSED"
    assert ledger.latest_progress(expected[1]) is None
    summary = ledger.expected_slot_summary(as_of_ns=expected[0] + SLOT_NS)
    assert summary == {
        "as_of_ns": expected[0] + SLOT_NS,
        "expected_due_slots": 2,
        "completed_slots": 0,
        "missed_slots": 1,
        "failed_slots": 0,
        "source_unavailable_slots": 0,
        "expired_slots": 0,
        "incomplete_slots": 1,
        "future_registered_slots": 1,
    }
    assert restarted.reconcile_missed_slots(now_ns=expected[0] + SLOT_NS) == ()
    assert len(ledger.progress_records()) == len({
        json.dumps(row, sort_keys=True) for row in ledger.progress_records()
    })


def test_runner_has_no_approval_reservation_or_execution_surface(tmp_path):
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls, archive=ResearchArtifactArchive(tmp_path / "research"))
    for forbidden in ("approve", "reserve", "submit_order", "place_order", "cancel", "flatten"):
        assert not hasattr(runner, forbidden)
    receipt = runner.run_due_slot(now_ns=fixture.slots[0] + HOUR_NS)
    assert receipt.result is not None
    assert all(row.approval_requested_at_ns is None and row.entry_attempted is None
               for row in receipt.result.calendar_rows)


def test_live_slot_fails_closed_without_archive_before_calling_providers():
    fixture = scanner_fixture()
    calls = {"universe": 0, "cheap": 0, "warmup": 0}
    runner = _runner(fixture, calls)

    with pytest.raises(ValueError, match="requires an archive for durable slot registration"):
        runner.run_due_slot(now_ns=fixture.slots[0] + HOUR_NS)

    assert calls == {"universe": 0, "cheap": 0, "warmup": 0}


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
