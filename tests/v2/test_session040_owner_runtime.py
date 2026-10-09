"""S40 installed owner boundary and watchdog integration; no live providers.

Storage measurements are covered by the actual temporary-store preflight tests.
These tests exercise the parent/child identity boundary and the independently
serviced operator evidence while the sole operational writer is blocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from atlas.v2 import product
from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.memory.writer_lock import OpsWriterAlreadyActive, OpsWriterLock
from atlas.v2.runtime.live_health import (
    LATCH_FILENAME,
    LiveHealthControllerV1,
    LiveHealthFactsV1,
    LiveHealthPolicyV1,
    QualificationLatchV1,
    assess_live_health,
)
from atlas.v2.runtime.owner_health_monitor import OwnerHealthMonitorV1
from atlas.v2.runtime.storage_preflight import StoragePreflightLimitsV1, StoragePreflightResultV1


@pytest.fixture
def build(monkeypatch):
    identity = {"source_sha": "a" * 40, "version": "2.0.40.0", "runtime_lock_sha256": "b" * 64}
    monkeypatch.setattr(product, "build_identity", lambda: dict(identity))
    return identity


def _run(tmp_path):
    return product.create_run(tmp_path, product.ResearchRunConfigV1())


def _facts(identity, *, at=None, **changes):
    now = time.time_ns() if at is None else at
    facts = LiveHealthFactsV1(identity["run_id"], identity["config_hash"], now, now, now,
        "RUNNING", True, PublicSourceStateV2.HEALTHY_CURRENT, False, 0, 512, 0,
        report_state="SUCCEEDED", last_export_success_at_ns=now,
        free_disk_bytes=500_000_000_000, disk_reserve_bytes=64_000_000_000)
    return replace(facts, **changes)


def _projection(run, facts):
    policy = LiveHealthPolicyV1()
    product._publish(run / "live-health.json", {"version": "OWNER_LIVE_HEALTH_PROJECTION_V1",
        "facts": facts.to_dict(), "assessment": assess_live_health(facts, policy=policy).to_dict(),
        "policy": policy.to_dict(), "authority": "ZERO"})


def _preflight_result(run, identity, *, status="TESTED", reasons=()):
    now = time.time_ns()
    return StoragePreflightResultV1(str(run.resolve()), identity["content_hash"], now, now, 0,
        status, reasons, StoragePreflightLimitsV1(), (), FrozenMap({"real_run_database_opened": False})).as_dict()


def test_preflight_child_is_isolated_identity_bound_and_holds_exclusive_probe_lease(tmp_path, build, monkeypatch):
    run = _run(tmp_path)
    identity = product.load_run(run)
    original = (run / "run.json").read_bytes()
    calls = []
    monkeypatch.setenv("DEEPSEEK_API_KEY", "must-not-reach-probe")
    monkeypatch.setenv("CLOUD_SECRET_ACCESS_KEY", "must-not-reach-probe-either")

    def child(command, **options):
        assert "--component" in command and command[command.index("--component") + 1] == "preflight"
        assert options["cwd"] == run and options["timeout"] == 45
        assert options["stdin"] == subprocess.DEVNULL and options["stderr"] == subprocess.DEVNULL
        assert options["env"]["QT_QPA_PLATFORM"] == "offscreen"
        assert "DEEPSEEK_API_KEY" not in options["env"] and "CLOUD_SECRET_ACCESS_KEY" not in options["env"]
        assert not (run / "ops.sqlite").exists()
        with pytest.raises(OpsWriterAlreadyActive):
            OpsWriterLock(run / "ops.sqlite").acquire()
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(_preflight_result(run, identity)).encode())

    monkeypatch.setattr(product.subprocess, "run", child)
    result = product.preflight_run(run)
    assert result["allowed"] and result["status"] == "TESTED"
    assert result["identity_sha256"] == identity["content_hash"] and len(calls) == 1
    assert not (run / "ops.sqlite").exists()
    assert (run / "run.json").read_bytes() == original
    attempts = list((run / "preflight-results").glob("*.json"))
    assert len(attempts) == 1 and json.loads(attempts[0].read_text()) == result
    assert json.loads((run / "preflight.json").read_text()) == result
    lease = OpsWriterLock(run / "ops.sqlite")
    lease.acquire()  # The probe lease is released, including after child completion.
    lease.close()


@pytest.mark.parametrize("reason", ["INSUFFICIENT_DISK_HEADROOM", "PATH_CREATE_FAILED_PermissionError",
    "SQLITE_WAL_UNAVAILABLE", "SQLITE_FULL_DURABILITY_UNAVAILABLE",
    "RUNTIME_DEPENDENCIES_FAILED_ImportError", "SQLITE_TRANSACTION_COMMIT_EXCEEDS_QUEUE_HEADROOM"])
def test_preflight_child_rejection_remains_visible_without_a_real_store(tmp_path, build, monkeypatch, reason):
    run = _run(tmp_path)
    identity = product.load_run(run)
    result = _preflight_result(run, identity, status="TEST GATE", reasons=(reason,))
    monkeypatch.setattr(product.subprocess, "run", lambda *args, **kwargs:
        SimpleNamespace(returncode=2, stdout=json.dumps(result).encode()))
    returned = product.preflight_run(run)
    assert not returned["allowed"] and returned["reasons"] == [reason]
    guidance = product.owner_live_indicator(run, now_ns=time.time_ns())
    assert guidance["colour"] == "AMBER" and reason in guidance["guidance"]
    assert not (run / "ops.sqlite").exists()


@pytest.mark.parametrize("damage", ["hash", "identity", "path", "mode", "authority", "returncode", "oversize",
    "schema-bool", "version", "allowed-int", "capital", "assisted", "source-promotion", "endurance-promotion",
    "status", "status-contradiction", "limits", "reasons-type", "reason-oversize", "reason-empty",
    "timestamp-bool", "negative-duration", "regressed-clock", "unknown-field", "missing-field", "unknown-exit"])
def test_preflight_untrusted_child_result_cannot_authorize_start(tmp_path, build, monkeypatch, damage):
    run = _run(tmp_path)
    identity = product.load_run(run)
    result = _preflight_result(run, identity)
    returncode = 0
    if damage == "hash":
        result["content_hash"] = "0" * 64
    elif damage == "identity":
        result["identity_sha256"] = "c" * 64
    elif damage == "path":
        result["selected_path"] = str(tmp_path)
    elif damage == "mode":
        result["probe_mode"] = "FAULT_INJECTION"
    elif damage == "authority":
        result["authority"] = "CAPITAL"
    elif damage == "returncode":
        returncode = 2
    elif damage == "schema-bool":
        result["schema_version"] = True
    elif damage == "version":
        result["version"] = "UNREGISTERED_PREFLIGHT_V9"
    elif damage == "allowed-int":
        result["allowed"] = 1
    elif damage == "capital":
        result["capital_enabled"] = True
    elif damage == "assisted":
        result["assisted_enabled"] = True
    elif damage == "source-promotion":
        result["live_source_qualification"] = "TESTED"
    elif damage == "endurance-promotion":
        result["endurance_qualification"] = "TESTED"
    elif damage == "status":
        result["status"] = "IMPLEMENTED"
    elif damage == "status-contradiction":
        result["status"] = "TEST GATE"
    elif damage == "limits":
        result["limits"]["safety_margin"] = 1
    elif damage == "reasons-type":
        result["reasons"] = "opaque error"
    elif damage == "reason-oversize":
        result["reasons"] = ["x" * 257]
    elif damage == "reason-empty":
        result["reasons"] = [""]
    elif damage == "timestamp-bool":
        result["started_at_ns"] = True
    elif damage == "negative-duration":
        result["elapsed_ns"] = -1
    elif damage == "regressed-clock":
        result["completed_at_ns"] = result["started_at_ns"] - 1
    elif damage == "unknown-field":
        result["unexpected_override"] = True
    elif damage == "missing-field":
        del result["limits"]
    elif damage == "unknown-exit":
        returncode = 1
    if damage not in {"hash", "oversize"}:
        result["content_hash"] = sha256_json({key: value for key, value in result.items() if key != "content_hash"})
    raw = b"x" * (product.MAX_CONFIG_BYTES + 1) if damage == "oversize" else json.dumps(result).encode()
    monkeypatch.setattr(product.subprocess, "run", lambda *args, **kwargs:
        SimpleNamespace(returncode=returncode, stdout=raw))
    returned = product.preflight_run(run)
    assert returned["status"] == "TEST GATE" and returned["allowed"] is False
    assert returned["reasons"] == ["PREFLIGHT_HOST_WORKER_FAILED_ValueError"]
    assert not (run / "ops.sqlite").exists()


def test_preflight_worker_timeout_is_bounded_sanitized_and_releases_lease(tmp_path, build, monkeypatch):
    run = _run(tmp_path)

    def hang(command, **options):
        assert options["timeout"] == 45
        raise subprocess.TimeoutExpired(command, options["timeout"], output=b"private-provider-body")

    monkeypatch.setattr(product.subprocess, "run", hang)
    result = product.preflight_run(run)
    assert result["reasons"] == ["PREFLIGHT_HOST_WORKER_DEADLINE_EXCEEDED"]
    assert result["allowed"] is False and "private-provider" not in json.dumps(result)
    lease = OpsWriterLock(run / "ops.sqlite")
    lease.acquire()
    lease.close()
    assert not (run / "ops.sqlite").exists()


def test_preflight_cannot_probe_or_replace_status_of_an_active_writer(tmp_path, build, monkeypatch):
    run = _run(tmp_path)
    product._publish(run / "status.json", {"run_id": run.name, "epoch_id": "existing", "status": "IMPLEMENTED"})
    old_status = (run / "status.json").read_bytes()
    calls = []
    monkeypatch.setattr(product.subprocess, "run", lambda *args, **kwargs: calls.append(args))
    with OpsRepository(run / "ops.sqlite") as writer:
        with pytest.raises(OpsWriterAlreadyActive):
            product.preflight_run(run)
        assert writer._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert not calls and not (run / "preflight.json").exists()
    assert not list((run / "preflight-results").glob("*.json"))
    assert (run / "status.json").read_bytes() == old_status


@pytest.mark.parametrize("damage", ["missing", "invalid-json", "oversize", "wrong-identity"])
def test_red_latch_outranks_missing_or_corrupt_ui_projection_and_broad_http_health(tmp_path, build, damage):
    run = _run(tmp_path)
    identity = product.load_run(run)
    facts = _facts(identity, queue_overflowed=True)
    first = LiveHealthControllerV1(run, run_id=run.name, config_hash=identity["config_hash"]).observe(facts)
    original = (run / LATCH_FILENAME).read_bytes()
    if damage == "invalid-json":
        (run / "live-health.json").write_text("not JSON")
    elif damage == "oversize":
        (run / "live-health.json").write_bytes(b" " * 65_537)
    elif damage == "wrong-identity":
        _projection(run, _facts(identity, run_id="another-run"))
    product._publish(run / "status.json", {"run_id": run.name, "observed_at_ns": time.time_ns(),
        "source_health": "HEALTHY_CURRENT", "status": "IMPLEMENTED"})
    result = product.owner_live_indicator(run, now_ns=time.time_ns())
    assert result["colour"] == "RED" and result["action"] == "STOP & EXPORT"
    assert result["qualification_failed"] and result["qualification_latch_ref"] == first.qualification_latch_ref
    assert result["reasons"][0] == "QUEUE_OVERFLOW_LOCAL_DATA_LOSS"
    assert "Current public HTTP/context: HEALTHY_CURRENT" in product._runtime_status_text(
        product._read_json(run / "status.json"), now_ns=time.time_ns())
    assert (run / LATCH_FILENAME).read_bytes() == original
    assert not list(run.glob("*.sqlite*"))


def test_corrupt_red_latch_is_not_hidden_by_absent_projection(tmp_path, build):
    run = _run(tmp_path)
    (run / LATCH_FILENAME).write_text("corrupt first-failure evidence")
    original = (run / LATCH_FILENAME).read_bytes()
    result = product.owner_live_indicator(run, now_ns=time.time_ns())
    assert result["colour"] == "RED" and result["qualification_failed"]
    assert result["reasons"][0] == "QUALIFICATION_LATCH_INVALID"
    assert (run / LATCH_FILENAME).read_bytes() == original


def test_desktop_reassesses_old_green_snapshot_and_preserves_immutable_identity(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    at = time.time_ns()
    _projection(run, _facts(identity, at=at))
    projection = (run / "live-health.json").read_bytes()
    fresh = product.owner_live_indicator(run, now_ns=at)
    assert fresh["colour"] == "GREEN" and fresh["action"] == "CONTINUE"
    stale = product.owner_live_indicator(run, now_ns=at + 11_000_000_000)
    assert stale["colour"] == "AMBER" and not stale["qualification_failed"]
    assert {"RUNTIME_HEARTBEAT_STALE", "CONTROLLER_HEARTBEAT_STALE"} <= set(stale["reasons"])
    assert (run / "live-health.json").read_bytes() == projection
    assert not (run / LATCH_FILENAME).exists()


def test_failed_run_cannot_be_relaunched_but_new_identity_starts_fresh(tmp_path, build, monkeypatch):
    failed = _run(tmp_path)
    identity = product.load_run(failed)
    LiveHealthControllerV1(failed, run_id=failed.name, config_hash=identity["config_hash"]).observe(
        _facts(identity, unclean_capture_stop=True))
    original = (failed / LATCH_FILENAME).read_bytes()
    calls = []
    monkeypatch.setattr(product.subprocess, "Popen", lambda *args, **kwargs: calls.append(args))
    with pytest.raises(RuntimeError, match="RUN_QUALIFICATION_FAILED_CREATE_NEW_RUN"):
        product.launch_run(failed)
    assert not calls and not list((failed / "launches").iterdir())
    clean = _run(tmp_path)
    clean_identity = product.load_run(clean)
    _projection(clean, _facts(clean_identity))
    product.launch_run(clean)
    assert len(calls) == 1
    assert product.owner_live_indicator(clean, now_ns=time.time_ns())["colour"] == "GREEN"
    assert (failed / LATCH_FILENAME).read_bytes() == original


def test_owner_indicator_shows_per_source_health_pressure_and_run_validity(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    facts = _facts(identity, queue_items=16, queue_high_water=32, queue_bytes=4096,
                   queue_capacity_bytes=16_777_216, queue_high_water_bytes=4096, capture_pending_batches=3,
                   capture_max_pending_batches=64, free_disk_bytes=80_000_000_000,
                   disk_reserve_bytes=64_000_000_000, wal_bytes=2_000_000,
                   report_state="SUCCEEDED", evidence_validation_failures=0)
    policy = LiveHealthPolicyV1()
    product._publish(run / "live-health.json", {
        "version": "OWNER_LIVE_HEALTH_PROJECTION_V1", "facts": facts.to_dict(),
        "assessment": assess_live_health(facts, policy=policy).to_dict(),
        "policy": policy.to_dict(),
        "source_states": {"BYBIT_DEPTH": "HEALTHY_CURRENT", "BINANCE_MARKET": "DISCONNECTED"},
        "authority": "ZERO",
    })
    product._publish(run / "status.json", {"provider_health": "TEST GATE",
        "provider_reason": "CONFIGURED_PROVIDER_BROKER_LOST"})
    details = "\n".join(product.owner_live_indicator(run, now_ns=time.time_ns())["operator_details"])
    assert "Process/watchdog: responding" in details
    assert "Run validity: no permanent failure observed" in details
    assert "BYBIT_DEPTH HEALTHY_CURRENT" in details and "BINANCE_MARKET DISCONNECTED" in details
    assert "Queue/capture: 16/512 items, 4096/16777216 bytes, capture 3/64 batches" in details
    assert "Storage: free 80000000000 bytes" in details and "WAL 2000000 bytes" in details
    assert "Provider/critic: TEST GATE (CONFIGURED_PROVIDER_BROKER_LOST)" in details


class _Source:
    """Measured-status fixture; pressure stop never invents frame rejection."""

    def __init__(self):
        self.queue = 0
        self.high_water = 0
        self.received = 0
        self.captured = 0
        self.overflowed = False
        self.rejected = 0
        self.connected = True
        self.state = "RUNNING"
        self.terminal = None
        self.stop_calls = 0
        self.observation_threads = set()

    def status(self):
        self.observation_threads.add(threading.get_ident())
        self.high_water = max(self.high_water, self.queue)
        return SimpleNamespace(state=self.state, handoff=SimpleNamespace(connected=self.connected,
            queue_items=self.queue, max_queue_items=512, high_water_items=self.high_water,
            queue_bytes=self.queue * 100, max_queue_bytes=16_777_216,
            high_water_bytes=self.high_water * 100, frames_received=self.received,
            frames_rejected=self.rejected, overflowed=self.overflowed), capture={
                "captured_frames": self.captured, "pending_batches": 0, "max_pending_batches": 64,
                "terminal_error": self.terminal})

    def request_pressure_stop(self):
        if self.terminal is not None:
            return
        self.stop_calls += 1
        self.terminal = "PREVENTIVE_CAPTURE_PRESSURE_STOP"
        self.connected = False


def _wait(predicate, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError("bounded watchdog observation deadline expired")


def _monitor(run, identity, source, *, persistence=lambda: {}, resources=None, report=None, progress=None):
    return OwnerHealthMonitorV1(run, run_id=run.name, config_hash=identity["config_hash"], source=source,
        progress=progress or (lambda: {"observed_at_ns": time.time_ns() - 1_000_000,
            "stream_source_state": "HEALTHY_CURRENT", "stream_recovery_required": False}),
        persistence=persistence, resources=resources or (lambda: {"disk_free_bytes": 500_000_000_000,
            "disk_reserve_bytes": 64_000_000_000, "current_footprint_bytes": 1_000_000, "wal_bytes": 0}),
        report=report or (lambda: {"state": "SUCCEEDED", "completed_at_ns": time.time_ns()}),
        publish=product._publish)


def test_watchdog_warns_and_latches_while_the_sole_sqlite_commit_is_still_blocked(tmp_path, build, monkeypatch):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    entered = threading.Event()
    release = threading.Event()
    errors = []
    with OpsRepository(run / "ops.sqlite") as repository:
        repository._connection.execute("CREATE TABLE stall_fixture(value INTEGER)")
        connection = repository._connection

        class BlockingCommit:
            def __getattr__(self, name):
                return getattr(connection, name)

            def commit(self):
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("injected commit was not released")
                connection.commit()

        monkeypatch.setattr(repository, "_connection", BlockingCommit())
        monitor = _monitor(run, identity, source, persistence=repository.persistence_metrics)
        monitor.start()
        writer = None
        try:
            _wait(lambda: monitor.latest is not None and monitor.latest["assessment"]["colour"] == "GREEN")

            def write_once():
                try:
                    with repository.atomic_composition():
                        repository._connection.execute("INSERT INTO stall_fixture VALUES (1)")
                except BaseException as error:
                    errors.append(type(error).__name__)

            writer = threading.Thread(target=write_once, name="fixture-sole-ops-writer")
            writer.start()
            assert entered.wait(2)
            _wait(lambda: monitor.latest["assessment"]["colour"] == "AMBER"
                and "PERSISTENCE_STALL" in monitor.latest["assessment"]["reasons"])
            assert writer.is_alive() and not release.is_set()
            assert not (run / LATCH_FILENAME).exists()
            source.overflowed = True
            _wait(lambda: monitor.latest["assessment"]["qualification_failed"])
            assert writer.is_alive() and (run / LATCH_FILENAME).exists()
            failure = QualificationLatchV1(run, run_id=run.name, config_hash=identity["config_hash"]).read()
            assert failure is not None and failure.first_cause == "QUEUE_OVERFLOW_LOCAL_DATA_LOSS"
            source.overflowed = False  # Later component recovery does not reset lifetime evidence.
            _wait(lambda: monitor.latest["facts"]["queue_overflowed"] is False)
            assert monitor.latest["assessment"]["colour"] == "RED"
            assert product.owner_live_indicator(run, now_ns=time.time_ns())["colour"] == "RED"
            assert source.observation_threads == {monitor._thread.ident}
        finally:
            release.set()
            if writer is not None:
                writer.join(timeout=3)
            monitor.close()
        assert not errors and writer is not None and not writer.is_alive()
        assert repository._connection.execute("SELECT count(*) FROM stall_fixture").fetchone()[0] == 1
        assert repository._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert repository.persistence_metrics()["transaction_count"] == 1
        assert monitor.error_type is None


def test_watchdog_harmless_pressure_recovers_but_stalled_capture_stops_before_overflow(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    monitor = _monitor(run, identity, source)
    monitor.start()
    try:
        _wait(lambda: monitor.latest is not None and monitor.latest["assessment"]["colour"] == "GREEN")
        source.queue, source.received, source.captured = 400, 500, 100
        _wait(lambda: monitor.latest["assessment"]["colour"] == "AMBER")
        # A short burst with demonstrated capture progress is not permanent loss.
        source.queue, source.captured = 0, 500
        _wait(lambda: monitor.latest["assessment"]["colour"] == "GREEN")
        assert source.stop_calls == 0 and not (run / LATCH_FILENAME).exists()
        # No capture progress for >0.8s with 400/512 queued is a different case.
        source.queue, source.received = 400, 900
        _wait(lambda: source.stop_calls > 0, timeout=3)
        _wait(lambda: monitor.latest["assessment"]["colour"] == "RED")
        assert source.stop_calls == 1 and not source.overflowed and source.rejected == 0
        assert monitor.latest["facts"]["queue_items"] < 512
        assert monitor.latest["assessment"]["reasons"][0] == "PREVENTIVE_PUBLIC_CAPTURE_STOP"
        source.queue, source.captured, source.terminal, source.connected = 0, 900, None, True
        _wait(lambda: monitor.latest["facts"]["capture_pressure_stop"] is False)
        assert monitor.latest["assessment"]["colour"] == "RED"
    finally:
        monitor.close()
    assert not list(run.glob("*.sqlite*")) and monitor.error_type is None


def test_report_failure_and_service_delay_are_visible_and_recover_without_an_integrity_latch(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    report = {"state": "FAILED", "completed_at_ns": time.time_ns()}
    progress = {"stream_service_gap_seconds": 4.8, "stream_service_duration_seconds": 4.2}
    monitor = _monitor(run, identity, source, report=lambda: dict(report), progress=lambda: {
        "observed_at_ns": time.time_ns() - 1_000_000, "stream_source_state": "HEALTHY_CURRENT",
        "stream_recovery_required": False, **progress})
    monitor.start()
    try:
        _wait(lambda: monitor.latest is not None
            and "REPORT_EXPORT_FAILED" in monitor.latest["assessment"]["reasons"])
        result = monitor.latest["assessment"]
        assert result["colour"] == "AMBER"
        assert {"REPORT_EXPORT_FAILED", "STREAM_SERVICE_STALL"} <= set(result["reasons"])
        assert monitor.latest["facts"]["service_gap_seconds"] == 4.8
        assert not (run / LATCH_FILENAME).exists()
        report.update(state="SUCCEEDED", completed_at_ns=time.time_ns())
        progress.update(stream_service_gap_seconds=.2, stream_service_duration_seconds=.05)
        _wait(lambda: monitor.latest["assessment"]["colour"] == "GREEN")
        facts = LiveHealthFactsV1.from_dict(monitor.latest["facts"])
        assert facts.evidence_refs == () and len(json.dumps(monitor.latest).encode()) < 16_384
    finally:
        monitor.close()
    assert monitor.error_type is None


def test_watchdog_close_publishes_final_terminal_source_facts(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    monitor = _monitor(run, identity, source)
    monitor.start()
    _wait(lambda: monitor.latest is not None)
    source.state = "FAILED"
    monitor.close()
    assert monitor.latest["assessment"]["colour"] == "RED"
    assert monitor.latest["assessment"]["reasons"][0] == "PUBLIC_STREAM_TERMINAL_FAILURE"
    reopened = product.owner_live_indicator(run, now_ns=time.time_ns())
    assert reopened["colour"] == "RED" and reopened["qualification_failed"]


def test_watchdog_wal_warning_uses_measured_growth_and_actual_checkpoint_progress(tmp_path, build):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    start = time.monotonic()
    stale_checkpoint = time.time_ns() - 40_000_000_000
    monitor = _monitor(run, identity, source,
        persistence=lambda: {"wal_frames": 100, "checkpointed_frames": 1,
            "checkpoint_progress_at_ns": stale_checkpoint, "checkpoint_observed_at_ns": time.time_ns()},
        resources=lambda: {"disk_free_bytes": 500_000_000_000, "disk_reserve_bytes": 64_000_000_000,
            "current_footprint_bytes": 1_000_000 + int((time.monotonic() - start) * 4096),
            "wal_bytes": int((time.monotonic() - start) * 4096)})
    monitor.start()
    try:
        _wait(lambda: monitor.latest is not None)
        assert monitor.latest["facts"]["wal_growth_bytes_per_second"] is None
        _wait(lambda: "WAL_CHECKPOINT_NOT_PROGRESSING" in monitor.latest["assessment"]["reasons"], timeout=15)
        assert monitor.latest["assessment"]["colour"] == "AMBER"
        assert monitor.latest["facts"]["wal_growth_bytes_per_second"] > 0
        assert monitor.latest["facts"]["wal_last_progress_at_ns"] == stale_checkpoint
        assert not (run / LATCH_FILENAME).exists()
    finally:
        monitor.close()


@pytest.mark.parametrize("blocked_callback", ["projection", "resources"])
def test_five_second_host_observation_or_projection_stall_cannot_block_preventive_capture_stop(
        tmp_path, build, blocked_callback):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    entered = threading.Event()
    release = threading.Event()
    blocked_at = []
    publisher_threads = set()

    def stall_once():
        publisher_threads.add(threading.get_ident())
        if not entered.is_set():
            blocked_at.append(time.monotonic())
            entered.set()
            assert release.wait(6), "fault injection was not released"

    def resources():
        if blocked_callback == "resources":
            stall_once()
        return {"disk_free_bytes": 500_000_000_000, "disk_reserve_bytes": 64_000_000_000,
            "current_footprint_bytes": 1_000_000, "wal_bytes": 0}

    def publish(path, body):
        if blocked_callback == "projection":
            stall_once()
        product._publish(path, body)

    monitor = OwnerHealthMonitorV1(run, run_id=run.name, config_hash=identity["config_hash"], source=source,
        progress=lambda: {"observed_at_ns": time.time_ns() - 1_000_000,
            "stream_source_state": "HEALTHY_CURRENT", "stream_recovery_required": False},
        persistence=lambda: {}, resources=resources, report=lambda: {"state": "IDLE"}, publish=publish)
    monitor.start()
    try:
        assert entered.wait(2)
        _wait(lambda: monitor.latest is not None)
        source.queue, source.received = 400, 400
        _wait(lambda: source.stop_calls == 1, timeout=2)
        assert not release.is_set() and monitor._publisher.is_alive()
        _wait(lambda: (run / LATCH_FILENAME).exists())
        _wait(lambda: monitor.latest["assessment"]["colour"] == "RED")
        result = monitor.latest["assessment"]
        assert result["colour"] == "RED" and result["reasons"][0] == "PREVENTIVE_PUBLIC_CAPTURE_STOP"
        assert not source.overflowed and source.rejected == 0 and source.high_water == 400
        assert source.observation_threads == {monitor._thread.ident}
        assert publisher_threads == {monitor._publisher.ident}
        assert monitor._pending_projection is None or isinstance(monitor._pending_projection, dict)
        # Keep the actual callback blocked for five seconds, independently of
        # the already completed pressure stop and immutable first-failure latch.
        time.sleep(max(0, blocked_at[0] + 5 - time.monotonic()))
        assert monitor.latest["assessment"]["colour"] == "RED"
    finally:
        release.set()
        monitor.close()
    assert monitor.publication_error_type is None and monitor.error_type is None
    assert not list(run.glob("*.sqlite*"))
    assert product.owner_live_indicator(run, now_ns=time.time_ns())["colour"] == "RED"


def test_actual_desktop_cli_opens_widgets_and_exits_without_starting_a_run(tmp_path):
    environment = {key: value for key, value in os.environ.items()
        if not any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}
    environment.update(QT_QPA_PLATFORM="offscreen", PYTHONPATH=str(product.resource_file("src")))
    completed = subprocess.run([sys.executable, "-c", "from atlas.v2.product import main; raise SystemExit(main())",
        "--desktop-smoke", "--data-root", str(tmp_path / "temporary-desktop")],
        stdin=subprocess.DEVNULL, capture_output=True,
        env=environment, timeout=30, check=False)
    assert completed.returncode == 0, completed.stderr.decode(errors="replace")[-1000:]
    result = json.loads(completed.stdout.splitlines()[-1])
    assert result["status"] == "TESTED" and result["check"] == "DESKTOP_STARTUP_AND_OWNER_GUIDANCE"
    assert result["window_visible"] and result["preflight_control_present"] and result["health_indicator_present"]
    assert result["live_run_started"] is result["capital_enabled"] is result["assisted_enabled"] is False
    assert not list(tmp_path.rglob("ops.sqlite")) and not list(tmp_path.rglob("run.json"))


def test_repeated_healthy_latch_metadata_reads_cannot_stall_pressure_detection(tmp_path, build, monkeypatch):
    run = _run(tmp_path)
    identity = product.load_run(run)
    source = _Source()
    armed = threading.Event()
    release = threading.Event()
    blocked = threading.Event()
    original_read = QualificationLatchV1.read

    def slow_latch_metadata(latch):
        # Initial durable identity validation remains real. Only repeated
        # health observations encounter the injected file-metadata latency.
        if armed.is_set() and source.stop_calls == 0 and latch._owner is not None:
            blocked.set()
            release.wait(5)
        return original_read(latch)

    monkeypatch.setattr(QualificationLatchV1, "read", slow_latch_metadata)
    monitor = _monitor(run, identity, source)
    monitor.start()
    try:
        _wait(lambda: monitor.latest is not None and monitor.latest["assessment"]["colour"] == "GREEN")
        armed.set()
        source.queue, source.received = 400, 400
        _wait(lambda: source.stop_calls == 1, timeout=2)
        assert not blocked.is_set(), "watchdog reread the absent immutable latch on its critical path"
        assert source.high_water < 512 and not source.overflowed and source.rejected == 0
        _wait(lambda: (run / LATCH_FILENAME).exists())
        _wait(lambda: monitor.latest["assessment"]["colour"] == "RED")
        assert monitor.latest["assessment"]["colour"] == "RED"
    finally:
        release.set()
        monitor.close()


def test_failed_begin_clears_pending_phase_without_fabricating_success(tmp_path, build, monkeypatch):
    run = _run(tmp_path)
    with OpsRepository(run / "ops.sqlite") as repository:
        connection = repository._connection

        class FailedBegin:
            def __getattr__(self, name):
                return getattr(connection, name)

            def execute(self, statement, *args):
                if statement == "BEGIN IMMEDIATE":
                    raise sqlite3.OperationalError("injected busy begin")
                return connection.execute(statement, *args)

        monkeypatch.setattr(repository, "_connection", FailedBegin())
        with pytest.raises(sqlite3.OperationalError, match="injected busy begin"), repository.atomic_composition():
            pytest.fail("failed begin must not execute the transaction body")
        metrics = repository.persistence_metrics()
        assert metrics["active_phase"] == "IDLE" and metrics["begin_failed"] and metrics["transaction_failed"]
        assert metrics["transaction_count"] == 0 and metrics["begin_duration_ns"] >= 0
        assert metrics["transaction_observed_at_ns"] > 0 and not connection.in_transaction
        monkeypatch.setattr(repository, "_connection", connection)
        with repository.atomic_composition():
            connection.execute("CREATE TABLE after_failed_begin(value INTEGER)")
        recovered = repository.persistence_metrics()
        assert recovered["active_phase"] == "IDLE" and recovered["transaction_count"] == 1
        assert not recovered["begin_failed"] and not recovered["transaction_failed"]
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_source_close_tolerates_an_already_closed_producer_event_loop():
    from atlas.v2.data.public_microstructure_ws import bybit_btc_eth_linear_topics
    from atlas.v2.data.public_stream_source import PublicStreamSourceV2
    from atlas.v2.instruments import VenueV2

    source = PublicStreamSourceV2(venue=VenueV2.BYBIT, topics=bybit_btc_eth_linear_topics())
    loop = asyncio.new_event_loop()
    loop.close()
    source._loop = loop
    source._task = SimpleNamespace(done=lambda: False, cancel=lambda: None)
    source.request_close()
    source.close()
    assert source.status().state == "CLOSED" and not source.status().handoff.connected
    assert source._close_requested.is_set() and source.status().attempt_count == 0
