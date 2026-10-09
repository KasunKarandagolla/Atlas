"""S40 operational guidance and lifetime run-failure evidence; zero network."""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import replace

import pytest

from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.runtime.live_health import (
    LATCH_FILENAME,
    LATCH_TEMP_FILENAME,
    MAX_LATCH_BYTES,
    NS,
    LiveHealthControllerV1,
    LiveHealthFactsV1,
    LiveHealthPolicyV1,
    QualificationFailureV1,
    QualificationLatchError,
    QualificationLatchV1,
    assess_live_health,
    database_exception_is_integrity_failure_v1,
    terminal_database_integrity_failure_v1,
)

NOW = 1_800_000_000 * NS
RUN = "s40-run-001"
CONFIG = "a" * 64


def healthy(**values):
    baseline = LiveHealthFactsV1(RUN, CONFIG, NOW, NOW, NOW, "RUNNING", True,
        PublicSourceStateV2.HEALTHY_CURRENT, False, 32, 512, 64,
        arrival_frames_per_second=160, drain_frames_per_second=160,
        service_gap_seconds=.2, service_duration_seconds=.05, persistence_seconds=.05,
        free_disk_bytes=500_000_000_000, current_footprint_bytes=2_000_000_000,
        growth_bytes_per_second=100_000, disk_reserve_bytes=10_000_000_000,
        wal_bytes=1_000_000, wal_uncheckpointed_frames=100, wal_growth_bytes_per_second=0,
        wal_last_progress_at_ns=NOW, report_state="SUCCEEDED", last_export_success_at_ns=NOW,
        active_workers=1, max_active_workers=2, evidence_refs=("b" * 64,))
    return replace(baseline, **values)


def test_healthy_guidance_is_separate_from_live_qualification():
    facts = healthy()
    result = assess_live_health(facts)
    assert (result.colour, result.action, result.status) == ("GREEN", "CONTINUE", "TESTED")
    assert result.reasons == () and not result.qualification_failed
    assert result.qualification_latch_ref is None
    assert result.source_state == PublicSourceStateV2.HEALTHY_CURRENT
    assert "qualification remains a separate gate" in result.guidance
    assert result.to_dict()["authority"] == "ZERO"
    assert "capital" not in result.to_dict()


@pytest.mark.parametrize(("values", "reason"), [
    ({"queue_items": 256, "queue_high_water": 256}, "QUEUE_PRESSURE_RISING"),
    ({"queue_items": 480, "queue_high_water": 480}, "QUEUE_PRESSURE_RISING"),
    ({"queue_items": 240, "queue_high_water": 240, "queue_growth_frames_per_second": 400}, "QUEUE_PRESSURE_RISING"),
    ({"queue_items": 240, "queue_high_water": 240, "arrival_frames_per_second": 560,
      "drain_frames_per_second": 160}, "QUEUE_PRESSURE_RISING"),
    ({"capture_pending_batches": 32, "capture_max_pending_batches": 64}, "CAPTURE_BACKLOG_PRESSURE"),
    ({"service_gap_seconds": 1}, "STREAM_SERVICE_STALL"),
    ({"service_duration_seconds": 4.2}, "STREAM_SERVICE_STALL"),
    ({"persistence_seconds": 5}, "PERSISTENCE_STALL"),
    ({"runtime_heartbeat_at_ns": NOW - 11 * NS}, "RUNTIME_HEARTBEAT_STALE"),
    ({"controller_heartbeat_at_ns": NOW - 5 * NS}, "CONTROLLER_HEARTBEAT_STALE"),
    ({"runtime_heartbeat_at_ns": None}, "HEARTBEAT_UNAVAILABLE"),
    ({"connected": False}, "PUBLIC_STREAM_RECOVERING"),
    ({"recovery_required": True}, "PUBLIC_STREAM_RECOVERING"),
    ({"unresolved_gap": True}, "PUBLIC_STREAM_RECOVERING"),
    ({"source_state": PublicSourceStateV2.INCOMPLETE_SNAPSHOT}, "SOURCE_NOT_CURRENT"),
    ({"free_disk_bytes": 20_000_000_000}, "DISK_HEADROOM_INSUFFICIENT"),
    ({"wal_last_progress_at_ns": NOW - 31 * NS, "wal_growth_bytes_per_second": 100}, "WAL_CHECKPOINT_NOT_PROGRESSING"),
    ({"report_state": "FAILED"}, "REPORT_EXPORT_FAILED"),
    ({"export_failure_count": 1}, "REPORT_EXPORT_FAILED"),
    ({"report_state": "RUNNING", "report_started_at_ns": NOW - 11 * NS}, "REPORT_EXPORT_DELAYED"),
    ({"evidence_validation_failures": 1}, "EVIDENCE_VALIDATION_FAILED"),
    ({"resource_pressure": True}, "RESOURCE_PRESSURE"),
    ({"rss_bytes": 1_500_000_000}, "RESOURCE_PRESSURE"),
    ({"host_observed_at_ns": NOW - 4 * NS}, "HOST_HEALTH_OBSERVATION_STALE"),
    ({"active_workers": 3}, "RESOURCE_PRESSURE"),
])
def test_leading_operational_warnings_are_visible_but_do_not_invent_loss(values, reason):
    result = assess_live_health(healthy(**values))
    assert result.colour == "AMBER" and result.action == "ATTENTION" and result.status == "TEST GATE"
    assert reason in result.reasons and not result.qualification_failed


def test_warning_derivation_and_recovering_headroom_are_explicit():
    policy = LiveHealthPolicyV1()
    assert policy.service_warning_seconds == .8  # 512 / 320 / 2
    result = assess_live_health(healthy(queue_items=240, queue_high_water=240,
                                      queue_growth_frames_per_second=400), policy=policy)
    assert result.queue_headroom_seconds == .68 and result.colour == "AMBER"
    recovering = assess_live_health(healthy(queue_items=100, queue_high_water=480,
                                            queue_growth_frames_per_second=-50), policy=policy)
    assert recovering.colour == "GREEN" and recovering.queue_headroom_seconds is None
    assert result.policy_hash == policy.content_hash


def test_harmless_transient_warning_clears_without_resetting_historical_high_water(tmp_path):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    assert controller.observe(healthy()).colour == "GREEN"
    assert controller.observe(healthy(queue_items=480, queue_high_water=480,
                                     persistence_seconds=1)).colour == "AMBER"
    assert not (tmp_path / LATCH_FILENAME).exists()
    recovered = controller.observe(healthy(queue_high_water=480))
    assert recovered.colour == "GREEN" and not recovered.qualification_failed


@pytest.mark.parametrize(("values", "reason"), [
    ({"queue_overflowed": True}, "QUEUE_OVERFLOW_LOCAL_DATA_LOSS"),
    ({"frames_rejected": 1}, "PUBLIC_STREAM_LOCAL_FRAME_REJECTION"),
    ({"capture_failed": True}, "RAW_CAPTURE_TERMINAL_FAILURE"),
    ({"producer_state": "FAILED"}, "PUBLIC_STREAM_TERMINAL_FAILURE"),
    ({"evidence_integrity_failure": True}, "EVIDENCE_INTEGRITY_FAILURE"),
    ({"database_integrity_failed": True}, "EVIDENCE_INTEGRITY_FAILURE"),
    ({"clock_integrity_failure": True}, "EVIDENCE_CLOCK_INTEGRITY_FAILURE"),
])
def test_irreversible_failure_latches_red_despite_http_recovery_and_restart(tmp_path, values, reason):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    first = controller.observe(healthy(**values))
    assert (first.colour, first.action, first.status) == ("RED", "STOP & EXPORT", "TEST GATE")
    assert first.qualification_failed and first.reasons[0] == reason
    original_bytes = (tmp_path / LATCH_FILENAME).read_bytes()
    assert len(original_bytes) < MAX_LATCH_BYTES
    assert first.qualification_latch_ref is not None
    recovered = replace(healthy(), observed_at_ns=NOW + NS,
        runtime_heartbeat_at_ns=NOW + NS, controller_heartbeat_at_ns=NOW + NS)
    restarted = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    result = restarted.observe(recovered)
    assert result.colour == "RED" and result.source_state == PublicSourceStateV2.HEALTHY_CURRENT
    assert result.qualification_latch_ref == first.qualification_latch_ref
    assert (tmp_path / LATCH_FILENAME).read_bytes() == original_bytes
    failure = restarted.latch.read()
    assert failure.first_cause == reason and failure.failed_at_ns == NOW
    assert failure.facts.evidence_refs == ("b" * 64,)
    assert failure.facts.content_hash == healthy(**values).content_hash


def test_green_amber_red_sequence_precedes_512_and_keeps_first_cause(tmp_path):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    assert controller.observe(healthy()).colour == "GREEN"
    amber = controller.observe(healthy(queue_items=256, queue_high_water=256))
    assert amber.colour == "AMBER" and not amber.qualification_failed
    failure = controller.observe(healthy(queue_items=512, queue_high_water=512,
                                         queue_overflowed=True, frames_rejected=1))
    assert failure.colour == "RED"
    second = controller.observe(healthy(capture_failed=True, producer_state="FAILED", observed_at_ns=NOW + NS))
    assert second.reasons[0] == "QUEUE_OVERFLOW_LOCAL_DATA_LOSS"
    assert second.qualification_latch_ref == failure.qualification_latch_ref


def test_a_new_clean_run_gets_a_fresh_lifetime_identity(tmp_path):
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    assert LiveHealthControllerV1(old, run_id=RUN, config_hash=CONFIG).observe(healthy(queue_overflowed=True)).colour == "RED"
    clean = LiveHealthControllerV1(new, run_id="s40-run-002", config_hash=CONFIG)
    assert clean.observe(healthy(run_id="s40-run-002")).colour == "GREEN"
    assert not (new / LATCH_FILENAME).exists()
    assert (old / LATCH_FILENAME).exists()


def test_latch_has_one_explicit_controller_thread_and_read_only_ui(tmp_path):
    reader = QualificationLatchV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    facts = healthy(queue_overflowed=True)
    failure = QualificationFailureV1(RUN, CONFIG, NOW, "QUEUE_OVERFLOW_LOCAL_DATA_LOSS", facts,
                                     LiveHealthPolicyV1().content_hash)
    with pytest.raises(PermissionError):
        reader.publish(failure)
    writer = QualificationLatchV1(tmp_path, run_id=RUN, config_hash=CONFIG, controller=True)
    errors = []

    def mutate_from_wrong_thread():
        try:
            writer.publish(failure)
        except Exception as exc:
            errors.append(type(exc).__name__)

    wrong = threading.Thread(target=mutate_from_wrong_thread)
    wrong.start()
    wrong.join(timeout=2)
    assert not wrong.is_alive() and errors == ["PermissionError"]
    assert writer.publish(failure) == failure
    assert reader.read() == failure


def test_watchdog_can_own_latch_without_a_sqlite_connection(tmp_path):
    outputs = []

    def watchdog():
        controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
        outputs.append(controller.observe(healthy(queue_overflowed=True)))

    thread = threading.Thread(target=watchdog)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and outputs[0].colour == "RED"
    assert not list(tmp_path.glob("*.sqlite*"))


@pytest.mark.parametrize("damage", ["hash", "identity", "duplicate", "oversized", "schema", "facts"])
def test_corrupt_or_mismatched_latch_remains_fail_closed_and_is_never_overwritten(tmp_path, damage):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    controller.observe(healthy(queue_overflowed=True))
    path = tmp_path / LATCH_FILENAME
    body = json.loads(path.read_text())
    if damage == "hash":
        body["content_hash"] = "0" * 64
    elif damage == "identity":
        # Valid first-failure file copied from another run, not an invalid hash.
        other = tmp_path / "other"
        other.mkdir()
        LiveHealthControllerV1(other, run_id="other", config_hash=CONFIG).observe(healthy(run_id="other", queue_overflowed=True))
        path.write_bytes((other / LATCH_FILENAME).read_bytes())
    elif damage == "duplicate":
        path.write_text('{"failure":{},"failure":{},"content_hash":"' + "0" * 64 + '"}')
    elif damage == "oversized":
        path.write_bytes(b" " * (MAX_LATCH_BYTES + 1))
    elif damage == "schema":
        body["failure"]["schema_version"] = True
    elif damage == "facts":
        body["failure"]["facts"]["queue_overflowed"] = False
    if damage in ("hash", "schema", "facts"):
        path.write_text(json.dumps(body))
    retained = path.read_bytes()
    # The live controller caches its immutable first failure; independent
    # readers and a restarted controller must reject externally changed bytes.
    restarted = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    result = restarted.observe(healthy())
    assert result.colour == "RED" and result.qualification_failed
    assert result.reasons[0].startswith("QUALIFICATION_LATCH_")
    assert path.read_bytes() == retained


def test_latch_publication_failure_cannot_be_presented_as_durable_success(tmp_path, monkeypatch):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)

    def unavailable(*args, **kwargs):
        raise OSError("injected host condition; not exported")

    monkeypatch.setattr("atlas.v2.runtime.live_health.os.link", unavailable)
    result = controller.observe(healthy(queue_overflowed=True))
    assert result.colour == "RED" and result.qualification_failed
    assert result.qualification_latch_ref is None
    assert result.reasons[0] == "QUALIFICATION_LATCH_PUBLICATION_FAILED"
    assert not (tmp_path / LATCH_FILENAME).exists()
    assert not (tmp_path / LATCH_TEMP_FILENAME).exists()
    assert "injected" not in json.dumps(result.to_dict())
    recovered_but_unpublished = controller.observe(healthy())
    assert recovered_but_unpublished.colour == "RED" and recovered_but_unpublished.qualification_failed
    assert recovered_but_unpublished.qualification_latch_ref is None


def test_failed_publication_retries_original_failure_and_never_backdates_current_facts(tmp_path, monkeypatch):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    import atlas.v2.runtime.live_health as module

    original = module.os.link

    def unavailable(*args, **kwargs):
        raise OSError("bounded injected failure")

    monkeypatch.setattr(module.os, "link", unavailable)
    assert controller.observe(healthy(queue_overflowed=True)).colour == "RED"
    monkeypatch.setattr(module.os, "link", original)
    latest = healthy(observed_at_ns=NOW + NS, runtime_heartbeat_at_ns=NOW + NS,
                     controller_heartbeat_at_ns=NOW + NS)
    result = controller.observe(latest)
    assert result.colour == "RED" and result.observed_at_ns == NOW + NS
    retained = controller.latch.read()
    assert retained.failed_at_ns == NOW and retained.facts.observed_at_ns == NOW


def test_existing_first_failure_is_never_replaced_and_reader_validates_exact_binding(tmp_path):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    controller.observe(healthy(queue_overflowed=True))
    first = controller.latch.read()
    later = QualificationFailureV1(RUN, CONFIG, NOW + NS, "RAW_CAPTURE_TERMINAL_FAILURE",
        healthy(capture_failed=True, observed_at_ns=NOW + NS), controller.policy.content_hash)
    assert controller.latch.publish(later) == first
    with pytest.raises(QualificationLatchError, match="IDENTITY_MISMATCH"):
        QualificationLatchV1(tmp_path, run_id=RUN, config_hash="c" * 64).read()
    with pytest.raises(ValueError, match="another run"):
        controller.observe(healthy(run_id="other"))


def test_unknown_and_future_clock_observations_cannot_display_green():
    future = assess_live_health(healthy(), at_ns=NOW - NS)
    assert future.colour == "RED" and "HEALTH_OBSERVATION_CLOCK_CONFLICT" in future.reasons
    assert not future.qualification_failed  # Conflicting UI clock does not rewrite evidence.
    stale_snapshot = assess_live_health(healthy(), at_ns=NOW + 20 * NS)
    assert stale_snapshot.colour == "AMBER" and "RUNTIME_HEARTBEAT_STALE" in stale_snapshot.reasons


def test_observed_clock_conflict_latches_but_transient_database_errors_do_not(tmp_path):
    controller = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    result = controller.observe(healthy(runtime_heartbeat_at_ns=NOW + 1))
    assert result.colour == "RED" and result.qualification_failed
    assert result.reasons[0] == "HEALTH_OBSERVATION_CLOCK_CONFLICT"
    assert terminal_database_integrity_failure_v1(("IntegrityError",))
    assert terminal_database_integrity_failure_v1(("DatabaseError",))
    assert not terminal_database_integrity_failure_v1(("OperationalError",))
    assert not terminal_database_integrity_failure_v1(("TimeoutError",))
    corrupt = sqlite3.OperationalError("closed diagnostic text")
    corrupt.sqlite_errorcode = sqlite3.SQLITE_CORRUPT
    busy = sqlite3.OperationalError("closed diagnostic text")
    busy.sqlite_errorcode = sqlite3.SQLITE_BUSY
    assert database_exception_is_integrity_failure_v1(corrupt)
    assert not database_exception_is_integrity_failure_v1(busy)


def test_disk_estimate_is_measured_reserve_not_an_endurance_claim():
    result = assess_live_health(healthy())
    assert result.estimated_disk_reserve_bytes == 2_000_000_000 + 100_000 * 48 * 3600 * 2
    unmeasured = assess_live_health(healthy(growth_bytes_per_second=None, current_footprint_bytes=None,
                                           disk_reserve_bytes=None))
    assert unmeasured.estimated_disk_reserve_bytes is None
    # No missing measurement is silently manufactured as zero growth.
    assert healthy(growth_bytes_per_second=None).to_dict()["growth_bytes_per_second"] is None


def test_wal_reader_pressure_warns_only_on_growing_uncheckpointed_backlog():
    benign = assess_live_health(healthy(wal_last_progress_at_ns=NOW - 60 * NS,
                                       wal_growth_bytes_per_second=0, wal_uncheckpointed_frames=0))
    assert benign.colour == "GREEN"
    stalled = assess_live_health(healthy(wal_last_progress_at_ns=NOW - 31 * NS,
                                         wal_growth_bytes_per_second=100, wal_uncheckpointed_frames=20))
    assert stalled.colour == "AMBER" and "WAL_CHECKPOINT_NOT_PROGRESSING" in stalled.reasons
    assert not stalled.qualification_failed


def test_expected_worker_capacity_and_resolved_report_failure_are_not_permanent_warnings():
    baseline = assess_live_health(healthy(active_workers=1, max_active_workers=1))
    assert baseline.colour == "GREEN"
    recovered = assess_live_health(healthy(export_failure_count=1, last_export_failure_at_ns=NOW - NS,
                                            last_export_success_at_ns=NOW, report_state="SUCCEEDED"))
    assert recovered.colour == "GREEN"
    retained_failure = assess_live_health(healthy(export_failure_count=1,
        last_export_failure_at_ns=NOW, last_export_success_at_ns=NOW - NS, report_state="IDLE"))
    assert retained_failure.colour == "AMBER" and "REPORT_EXPORT_FAILED" in retained_failure.reasons


@pytest.mark.parametrize("values", [
    {"queue_items": 513}, {"queue_high_water": 20}, {"queue_items": True}, {"connected": 1},
    {"arrival_frames_per_second": float("nan")}, {"persistence_seconds": -1},
    {"queue_growth_frames_per_second": float("inf")}, {"frames_rejected": -1},
    {"growth_bytes_per_second": 1e308}, {"arrival_frames_per_second": 1 << 10000},
    {"free_disk_bytes": 1 << 64},
    {"evidence_refs": ("x",)}, {"evidence_refs": ("b" * 64,) * 17}, {"producer_state": "SECRET_UNEXPECTED"},
    {"capture_pending_batches": 2}, {"capture_pending_batches": 65, "capture_max_pending_batches": 64},
    {"active_workers": 0, "max_active_workers": 0}, {"free_disk_bytes": True},
])
def test_typed_facts_reject_invalid_unbounded_or_nonfinite_inputs(values):
    with pytest.raises(ValueError):
        healthy(**values)


def test_facts_round_trip_is_strict_and_healthy_facts_cannot_form_a_failure():
    facts = healthy()
    assert LiveHealthFactsV1.from_dict(facts.to_dict()) == facts
    invalid = facts.to_dict()
    invalid["unknown"] = 1
    with pytest.raises(ValueError):
        LiveHealthFactsV1.from_dict(invalid)
    invalid = facts.to_dict()
    invalid["schema_version"] = True
    with pytest.raises(ValueError):
        LiveHealthFactsV1.from_dict(invalid)
    with pytest.raises(ValueError):
        QualificationFailureV1(RUN, CONFIG, NOW, None, facts, LiveHealthPolicyV1().content_hash)


def test_latch_symlink_is_rejected_without_following_or_repairing_it(tmp_path):
    target = tmp_path / "unrelated.json"
    target.write_text("untouched")
    try:
        (tmp_path / LATCH_FILENAME).symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("host does not permit symlinks")
    result = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG).observe(healthy())
    assert result.colour == "RED" and result.reasons[0] == "QUALIFICATION_LATCH_INVALID"
    assert target.read_text() == "untouched"


def test_non_regular_latch_is_rejected_without_opening_a_blocking_file(tmp_path):
    (tmp_path / LATCH_FILENAME).mkdir()
    result = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG).observe(healthy())
    assert result.colour == "RED" and result.reasons[0] == "QUALIFICATION_LATCH_INVALID"


def test_interrupted_failure_publication_remains_visible_across_restart(tmp_path):
    # The sole fixed temporary marker is not silently wiped or treated as a
    # healthy run after crash, whether the interrupted body is partial or whole.
    marker = tmp_path / LATCH_TEMP_FILENAME
    marker.write_bytes(b'{"failure":')
    original = marker.read_bytes()
    restarted = LiveHealthControllerV1(tmp_path, run_id=RUN, config_hash=CONFIG)
    result = restarted.observe(healthy())
    assert result.colour == "RED" and result.qualification_failed
    assert result.reasons[0] == "QUALIFICATION_LATCH_PUBLICATION_INCOMPLETE"
    assert marker.read_bytes() == original
