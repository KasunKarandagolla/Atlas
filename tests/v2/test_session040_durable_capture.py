"""S40 raw-capture/sole-writer isolation; no public/provider network calls."""
from __future__ import annotations

import json
import threading
import time

import pytest

from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.data.public_archive_extents import PublicArchiveSegmentWriterV1
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.production import create_bybit_public_ws_port

from .test_s38_sustained_public_stream import FixturePublicSource, MixedWorkload, QueueStream, transport_rows


def wait_for(predicate, *, timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError("bounded capture wait expired")


@pytest.mark.parametrize("stall", [.1, .5, 1, 3, 5])
def test_capture_continues_during_blocked_sole_sqlite_commit(tmp_path, monkeypatch, stall):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    port = create_bybit_public_ws_port(public_source=FixturePublicSource(), public_stream_source=capture)
    expected = []
    worker_errors = []
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port) as supervisor:
        supervisor.run_once()
        repo = supervisor.repository
        assert repo is not None
        original_connection = repo._connection
        original_register = repo.register_artifacts
        writer_threads = set()
        armed = [True]
        injected_stall_ns = []

        def register(entries):
            writer_threads.add(threading.get_ident())
            return original_register(entries)

        monkeypatch.setattr(repo, "register_artifacts", register)

        class StalledConnection:
            def __getattr__(self, name):
                return getattr(original_connection, name)

            def commit(self):
                if armed[0]:
                    armed[0] = False
                    started = time.perf_counter_ns()
                    time.sleep(stall)
                    injected_stall_ns.append(time.perf_counter_ns() - started)
                return original_connection.commit()

        monkeypatch.setattr(repo, "_connection", StalledConnection())
        count = int((stall + .5) * 160)

        def produce():
            try:
                workload = MixedWorkload()
                start = time.monotonic()
                for ordinal in range(count):
                    time.sleep(max(0, start + ordinal / 160 - time.monotonic()))
                    frame = workload.frame(time.time_ns())
                    expected.append(frame)
                    assert source.handoff.offer(frame)
            except Exception as exc:
                worker_errors.append(type(exc).__name__)

        producer = threading.Thread(target=produce)
        producer.start()
        wait_for(lambda: capture.status().pending_frames > 0)
        port._collect_public_stream_evidence(repo, now_ns=time.time_ns())
        producer.join(timeout=8)
        assert not producer.is_alive() and not worker_errors
        wait_for(lambda: capture.status().capture["captured_frames"] == count)
        while capture.status().pending_frames:
            port._collect_public_stream_evidence(repo, now_ns=time.time_ns())
        status = capture.status()
        assert status.handoff.high_water_items < 512
        assert not status.handoff.overflowed and status.handoff.frames_rejected == 0
        assert status.capture["pending_batches"] == 0
        assert status.capture["high_water_batches"] <= 64
        assert status.capture["captured_frames"] == count
        assert writer_threads == {threading.get_ident()}
        assert repo._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert len(injected_stall_ns) == 1
        assert injected_stall_ns[0] >= stall * 1e9
        # Python 3.12 on Windows may quantize monotonic_ns to the system tick.
        # Verify the actual fault with the high-resolution performance clock,
        # then allow only the measured monotonic clock's endpoint resolution.
        quantization_ns = 2 * time.get_clock_info("monotonic").resolution * 1e9
        assert repo.persistence_metrics()["max_commit_duration_ns"] >= injected_stall_ns[0] - quantization_ns
        rows = transport_rows(repo, tmp_path)
        assert [row["raw_payload_bytes"] for row in rows] == [f.raw_payload_bytes for f in expected]
        assert [row["received_at_ns"] for row in rows] == [f.received_at_ns for f in expected]
        assert list((tmp_path / "ops-public-capture").glob("capture-*.bin"))
        print({"stall_seconds": stall, "frames": count, "queue_high_water": status.handoff.high_water_items,
               "capture_high_water_batches": status.capture["high_water_batches"], "rejected": 0,
               "injected_stall_ns": injected_stall_ns[0], "monotonic_resolution_ns": quantization_ns / 2})


def test_capture_failure_preserves_raw_receipts_and_is_terminal(tmp_path, monkeypatch):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    capture.configure_capture(tmp_path)
    capture.start()
    workload = MixedWorkload()
    first = workload.frame(time.time_ns())
    assert source.handoff.offer(first)
    wait_for(lambda: capture.status().pending_frames == 1)

    def failed_seal(*args, **kwargs):
        raise OSError("offline injected storage failure")

    monkeypatch.setattr(PublicArchiveSegmentWriterV1, "seal", failed_seal)
    assert source.handoff.offer(workload.frame(time.time_ns()))
    wait_for(lambda: capture.status().state == "FAILED")
    assert capture.status().capture["terminal_error"] == "RAW_CAPTURE_FAILED_OSERROR"
    assert list((tmp_path / "ops-public-capture").glob("capture-*.bin"))
    assert capture.status().pending_frames == 1
    capture.close()
    assert capture.status().state == "FAILED"


def test_no_direct_drain_or_unbound_start_can_compete_with_capture(tmp_path):
    capture = DurablePublicCaptureV1(QueueStream(time.time_ns))
    with pytest.raises(RuntimeError, match="controller-bound"):
        capture.start()
    with pytest.raises(RuntimeError, match="one consumer"):
        capture.drain(max_items=32)


def test_clean_stop_adopts_sealed_backlog_before_database_closes(tmp_path):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    port = create_bybit_public_ws_port(public_source=FixturePublicSource(), public_stream_source=capture)
    frame = MixedWorkload().frame(time.time_ns())
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port) as supervisor:
        supervisor.run_once()
        assert source.handoff.offer(frame)
        wait_for(lambda: capture.status().pending_frames == 1)
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as repo:
        rows = transport_rows(repo, tmp_path)
        assert len(rows) == 1 and rows[0]["raw_payload_bytes"] == frame.raw_payload_bytes
        assert not repo._connection.in_transaction
    assert not capture._thread.is_alive()


def test_sealed_corrupt_bytes_fail_closed_and_leave_raw_receipt(tmp_path):
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    capture.configure_capture(tmp_path)
    capture.start()
    assert source.handoff.offer(MixedWorkload().frame(time.time_ns()))
    wait_for(lambda: capture.status().pending_frames == 1)
    capture.close()
    batch = capture.drain_sealed_transport()
    path = tmp_path / "ops-public-extents" / batch.extent.metadata["extent"]["segment_name"]
    with path.open("r+b") as file:
        first = file.read(1)
        file.seek(0)
        file.write(bytes([first[0] ^ 1]))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        with pytest.raises(ValueError, match="bytes missing or changed"), repo.atomic_composition():
            batch.adopt(repo)
        assert repo.get_artifact(batch.extent.artifact_ref) is None
        assert repo.get_artifact(batch.batch.artifact_ref) is None
    assert list((tmp_path / "ops-public-capture").glob("capture-*.bin"))


def test_clean_capture_reopen_verifies_bounded_tail_and_preserves_prior_raw(tmp_path):
    class RestartPublicSource(FixturePublicSource):
        def bootstrap_products(self, *, now_ns):
            # The S38 convenience fixture changes observation time while fixing
            # effective time, which creates a conflicting product on reopen.
            # Keep the exact first immutable fixture metadata for this recovery.
            if not hasattr(self, "bootstrapped"):
                super().bootstrap_products(now_ns=now_ns)
                self.bootstrapped = True
            return self.products

    public = RestartPublicSource()
    for epoch in range(2):
        source = QueueStream(time.time_ns)
        capture = DurablePublicCaptureV1(source)
        port = create_bybit_public_ws_port(public_source=public, public_stream_source=capture)
        with OpsSupervisorV2(tmp_path / "ops.sqlite", port) as supervisor:
            supervisor.run_once()
            assert capture._thread is not None and capture._thread.is_alive(), (epoch, capture.status().capture, port._stream_ingestion_failed)
            assert source.handoff.offer(MixedWorkload().frame(time.time_ns()))
            wait_for(lambda capture=capture: capture.status().pending_frames == 1 or capture.status().state == "FAILED")
            assert capture.status().pending_frames == 1, capture.status().capture
        head = json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())
        assert head["state"] == "CLEAN" and head["last_batch_ref"] is not None
        assert len(list((tmp_path / "ops-public-capture").glob("capture-*.bin"))) == epoch + 1
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        assert len(transport_rows(reader, tmp_path)) == 2


def test_interrupted_capture_latches_failure_before_any_restarted_producer(tmp_path):
    from atlas.v2.runtime.live_health import QualificationLatchV1

    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    capture.configure_capture(tmp_path, capture_epoch="a" * 64)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        capture.recover_controller_capture(repository)
    # Equivalent to process death after the ACTIVE durability marker: no
    # clean-stop certificate or recovered source continuity is invented.
    restarted = DurablePublicCaptureV1(QueueStream(time.time_ns))
    restarted.configure_capture(tmp_path, capture_epoch="b" * 64)
    with OpsRepository(tmp_path / "ops.sqlite") as repository, pytest.raises(RuntimeError, match="NEW_RUN_REQUIRED"):
        restarted.recover_controller_capture(repository)
    latch = QualificationLatchV1(tmp_path, run_id="standalone-capture-fixture", config_hash="0" * 64).read()
    assert latch is not None and latch.first_cause == "UNCLEAN_PUBLIC_CAPTURE_STOP"
    assert restarted._thread is None
    assert json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())["state"] == "ACTIVE"


def test_unindexed_capture_tail_cannot_be_reopened_as_clean(tmp_path):
    from atlas.v2.runtime.live_health import QualificationLatchV1

    capture = DurablePublicCaptureV1(QueueStream(time.time_ns))
    capture.configure_capture(tmp_path)
    capture.start()
    assert capture.source.handoff.offer(MixedWorkload().frame(time.time_ns()))
    wait_for(lambda: capture.status().pending_frames == 1)
    capture.close()
    restarted = DurablePublicCaptureV1(QueueStream(time.time_ns))
    restarted.configure_capture(tmp_path)
    with OpsRepository(tmp_path / "ops.sqlite") as repository, pytest.raises(ValueError, match="exactly indexed"):
        restarted.recover_controller_capture(repository)
    latch = QualificationLatchV1(tmp_path, run_id="standalone-capture-fixture", config_hash="0" * 64).read()
    assert latch is not None and latch.first_cause == "EVIDENCE_INTEGRITY_FAILURE"
    assert list((tmp_path / "ops-public-capture").glob("capture-*.bin"))


def test_capture_cannot_raise_availability_floor_to_hide_a_regressed_clock(tmp_path):
    now = time.time_ns()
    source = QueueStream(lambda: now)
    capture = DurablePublicCaptureV1(source, clock_ns=lambda: now - 1)
    capture.configure_capture(tmp_path)
    capture.start()
    assert source.handoff.offer(MixedWorkload().frame(now))
    wait_for(lambda: capture.status().state == "FAILED")
    assert capture.status().capture["terminal_error"] == "RAW_CAPTURE_FAILED_VALUEERROR"
    assert capture.status().pending_frames == 0
    assert list((tmp_path / "ops-public-extents").glob("*.arrow"))  # orphan bytes retained
    capture.close()
