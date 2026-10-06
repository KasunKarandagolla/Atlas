"""S40 storage-fault, snapshot, FIFO and restart assertions without networking.

The separate script is the actual-wall native rate/soak gate. These short tests
prove fault/evidence semantics and never claim native throughput or endurance.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import replace
from typing import Any

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.data.public_archive_extents import PublicArchiveSegmentWriterV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime.live_health import QualificationLatchV1
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.owner_health_monitor import OwnerHealthMonitorV1
from scripts.session040_native_resilience import StablePublicSource, raw_digest_update, replay_raw_digest

from .test_s38_sustained_public_stream import MixedWorkload, QueueStream


def wait_for(predicate: Any, *, timeout: float = 12) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError("bounded test wait expired")


def test_long_read_snapshot_pins_wal_without_blocking_sole_full_writer_and_recovers(tmp_path):
    ready, release = threading.Event(), threading.Event()
    failures = []
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        def entry(index):
            ref = sha256_json(["s40-reader-pressure", index])
            return ArtifactIndexEntryV2(ref, "Session040WalFaultFixtureV1", ref, index + 1, index + 1,
                                        {"fixture": "x" * 8192, "ordinal": index})

        writer.register_artifact(entry(0))
        assert writer.checkpoint()[0] == 0

        def reader():
            try:
                with OpsRepository(database, read_only=True) as readonly, readonly.read_snapshot():
                    assert readonly._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == 1
                    ready.set()
                    assert release.wait(10)
                    # Exact prior snapshot does not change as the writer advances.
                    assert readonly._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == 1
            except Exception as error:
                failures.append(type(error).__name__)

        worker = threading.Thread(target=reader, name="s40-pinned-read-only", daemon=True)
        worker.start()
        try:
            assert ready.wait(3)
            for index in range(1, 17):
                writer.register_artifact(entry(index))
            before_release = writer.checkpoint()
            assert before_release[0] == 0 and before_release[1] > before_release[2]
            assert writer._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == 17
            assert writer._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert writer.persistence_metrics()["checkpoint_busy"] == 0
        finally:
            release.set()
            worker.join(timeout=3)
        assert not worker.is_alive() and not failures
        after_release = writer.checkpoint()
        assert after_release[0] == 0 and after_release[1] == after_release[2]
        assert writer._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.parametrize("storage_stall", [.1, .5, 5], ids=["100ms", "500ms", "5s-preventive-stop"])
def test_raw_archive_stall_is_tolerated_or_stops_before_loss_and_retains_exact_fifo(
        tmp_path, monkeypatch, storage_stall):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    capture.configure_capture(tmp_path)
    expected = hashlib.sha256()
    accepted = 0
    producer_errors = []
    original = PublicArchiveSegmentWriterV1.seal
    armed = True

    def seal(writer, *args, **kwargs):
        nonlocal armed
        if armed:
            armed = False
            time.sleep(storage_stall)
        return original(writer, *args, **kwargs)

    monkeypatch.setattr(PublicArchiveSegmentWriterV1, "seal", seal)

    def produce():
        nonlocal accepted
        workload = MixedWorkload()
        begun = time.monotonic()
        try:
            for ordinal in range(960 if storage_stall == 5 else 160):
                time.sleep(max(0, begun + ordinal / 160 - time.monotonic()))
                if source.closed:
                    return  # Explicit preventive stop: no claim of uninterrupted source capture.
                frame = workload.frame(time.time_ns())
                if source.handoff.offer(frame):
                    accepted += 1
                    raw_digest_update(expected, frame)
                elif not source.closed:
                    producer_errors.append("REJECTED_WITHOUT_EXPLICIT_STOP")
                    return
        except Exception as error:
            producer_errors.append(type(error).__name__)

    guidance = []

    def publish(path, body):
        del path
        guidance.append(body["assessment"]["action"])

    with OpsRepository(tmp_path / "ops.sqlite") as writer:
        capture.recover_controller_capture(writer)
        capture.start()
        monitor = OwnerHealthMonitorV1(tmp_path, run_id="standalone-capture-fixture", config_hash="0" * 64,
            source=capture, progress=lambda: {"observed_at_ns": time.time_ns(),
                "stream_source_state": "HEALTHY_CURRENT", "stream_recovery_required": False},
            persistence=writer.persistence_metrics, resources=lambda: {}, report=lambda: {"state": "IDLE"},
            publish=publish)
        monitor.start()
        producer = threading.Thread(target=produce, daemon=True)
        producer.start()
        try:
            producer.join(timeout=9)
            assert not producer.is_alive() and not producer_errors
            wait_for(lambda: capture.status().capture["captured_frames"] == accepted)
            capture.close()
            while (sealed := capture.drain_sealed_transport()) is not None:
                with writer.atomic_composition():
                    sealed.adopt(writer)
            status = capture.status()
            assert status.handoff.frames_rejected == 0 and not status.handoff.overflowed
            assert status.handoff.high_water_items < 512 and status.handoff.queue_items == 0
            assert replay_raw_digest(writer) == (accepted, expected.hexdigest())
            if storage_stall == 5:
                assert status.capture["terminal_error"] == "PREVENTIVE_CAPTURE_PRESSURE_STOP"
                wait_for(lambda: "STOP & EXPORT" in guidance)
                failure = QualificationLatchV1(tmp_path, run_id="standalone-capture-fixture",
                                                config_hash="0" * 64).read()
                assert failure is not None and failure.first_cause == "PREVENTIVE_PUBLIC_CAPTURE_STOP"
                assert accepted < 960
                with pytest.raises(RuntimeError, match="CLEAN_STOP_NOT_PROVEN"):
                    capture.mark_controller_capture_clean(writer)
            else:
                assert status.capture["terminal_error"] is None
                capture.mark_controller_capture_clean(writer)
                assert QualificationLatchV1(tmp_path, run_id="standalone-capture-fixture",
                                            config_hash="0" * 64).read() is None
        finally:
            monitor.close()
            capture.close()
            producer.join(timeout=1)


def test_clean_reopen_keeps_raw_identity_and_requires_new_book_snapshot(tmp_path):
    metadata = StablePublicSource()
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    port = production.create_bybit_public_ws_port(public_source=metadata, public_stream_source=capture)
    expected = hashlib.sha256()
    workload = MixedWorkload()
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port) as supervisor:
        supervisor.run_once()
        assert supervisor.repository is not None
        for _ in range(32):
            frame = workload.frame(time.time_ns())
            assert source.handoff.offer(frame)
            raw_digest_update(expected, frame)
        wait_for(lambda: capture.status().pending_frames == 32)
        port._collect_public_stream_evidence(supervisor.repository, now_ns=time.time_ns())
    assert json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())["state"] == "CLEAN"
    reopened_capture = DurablePublicCaptureV1(QueueStream(time.time_ns))
    reopened_port = production.create_bybit_public_ws_port(public_source=metadata, public_stream_source=reopened_capture)
    with OpsSupervisorV2(tmp_path / "ops.sqlite", reopened_port) as supervisor:
        supervisor.run_once()
        assert supervisor.repository is not None
        assert replay_raw_digest(supervisor.repository) == (32, expected.hexdigest())
        latest = reopened_port._owner_stream_reports.values()
        assert latest and all(not report.book_sequence_valid for report in latest)


def test_reconnect_with_snapshot_preserves_transport_epoch_without_fabricating_gap_repair(tmp_path):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    port = production.create_bybit_public_ws_port(public_source=StablePublicSource(), public_stream_source=capture)
    expected = hashlib.sha256()
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port) as supervisor:
        supervisor.run_once()
        assert supervisor.repository is not None
        for epoch in (1, 2):
            if epoch == 2:
                source.handoff.observe_disconnected(time.time_ns())
                port._collect_public_stream_evidence(supervisor.repository, now_ns=time.time_ns())
                source.handoff.observe_connected(time.time_ns())
            workload = MixedWorkload()  # Each new epoch starts real book snapshots.
            workload.ordinal = (epoch - 1) * 32  # Venue trade IDs remain globally unique across reconnect.
            for _ in range(32):
                frame = replace(workload.frame(time.time_ns()), connection_epoch=epoch)
                assert source.handoff.offer(frame)
                raw_digest_update(expected, frame)
            wait_for(lambda: capture.status().pending_frames == 32)
            port._collect_public_stream_evidence(supervisor.repository, now_ns=time.time_ns())
        assert replay_raw_digest(supervisor.repository) == (64, expected.hexdigest())
        reports = supervisor.repository.artifact_entries("PublicStreamContinuityReportV1")
        assert any(entry.metadata["report"]["gap_count"] > 0 for entry in reports)
        assert not any("TRADE_ID_REUSED_WITH_CONFLICTING_PAYLOAD" in entry.metadata["report"]["gap_reason_codes"]
                       for entry in reports)
        assert source.handoff.snapshot().disconnect_count == 1
        assert not source.handoff.snapshot().overflowed and source.handoff.snapshot().frames_rejected == 0
