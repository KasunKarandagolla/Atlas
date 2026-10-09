"""Bounded stop reserve preserves accepted raw when the controller cannot drain."""

from __future__ import annotations

import json
import threading
import time

import pytest

from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.live_health import QualificationLatchV1

from .test_s38_sustained_public_stream import MixedWorkload, QueueStream, transport_rows


def wait_for(predicate, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("bounded capture-pressure wait expired")


def test_pending_descriptor_reserve_stops_before_exhaustion_and_seals_accepted_queue(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    capture.configure_capture(tmp_path)
    workload = MixedWorkload()
    accepted = []

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        capture.recover_controller_capture(repository)
        capture.start()
        initial = capture.status().capture
        threshold = initial["pressure_stop_threshold_batches"]
        reserve = initial["pressure_reserve_batches"]
        assert threshold == initial["max_pending_batches"] - reserve
        assert 0 < threshold < initial["max_pending_batches"]

        def offer(count: int) -> None:
            for _ in range(count):
                frame = workload.frame(time.time_ns() + len(accepted))
                assert source.handoff.offer(frame)
                accepted.append(frame)

        # Keep the sole controller consumer blocked until the capture queue has
        # accumulated to the calculated stop threshold.
        for expected_pending in range(1, threshold):
            offer(64)
            wait_for(lambda expected_pending=expected_pending:
                capture.status().capture["pending_batches"] >= expected_pending)
            assert capture.status().capture["terminal_error"] is None

        entered_seal = threading.Event()
        release_seal = threading.Event()
        original_seal = capture._seal
        stall_once = True

        def stall_threshold_seal(writer, frames):
            nonlocal stall_once
            if stall_once and capture.status().capture["pending_batches"] == threshold - 1:
                stall_once = False
                entered_seal.set()
                if not release_seal.wait(5.0):
                    raise TimeoutError("test did not release the in-flight capture seal")
            return original_seal(writer, frames)

        monkeypatch.setattr(capture, "_seal", stall_threshold_seal)
        offer(64)
        assert entered_seal.wait(5.0)
        assert source.handoff.snapshot().queue_items == 0
        offer(source.handoff.snapshot().max_queue_items)
        assert source.handoff.snapshot().queue_items == source.handoff.snapshot().max_queue_items
        release_seal.set()

        wait_for(lambda: capture.status().capture["terminal_error"] == "PREVENTIVE_CAPTURE_PRESSURE_STOP")
        wait_for(lambda: not capture._thread.is_alive())
        stopped = capture.status()
        assert stopped.state == "FAILED"
        assert stopped.handoff.queue_items == 0
        assert stopped.handoff.frames_received == len(accepted)
        assert stopped.handoff.frames_rejected == 0
        assert stopped.handoff.overflowed is False
        assert stopped.capture["captured_frames"] == len(accepted)
        assert stopped.capture["high_water_batches"] <= stopped.capture["max_pending_batches"]
        assert stopped.capture["pending_batches"] < stopped.capture["max_pending_batches"]

        # The controller can later index each already durable capture extent;
        # the stop remains terminal and the lifecycle is not marked CLEAN.
        adopted = []
        while (batch := capture.drain_sealed_transport()) is not None:
            adopted.extend(batch.adopt(repository))
        assert adopted == accepted
        rows = transport_rows(repository, tmp_path)
        assert [row["raw_payload_bytes"] for row in rows] == [frame.raw_payload_bytes for frame in accepted]
        assert [row["received_at_ns"] for row in rows] == [frame.received_at_ns for frame in accepted]
        assert repository._connection.in_transaction is False
        assert capture.status().capture["terminal_error"] == "PREVENTIVE_CAPTURE_PRESSURE_STOP"

    capture.close()
    lifecycle = json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())
    assert lifecycle["state"] == "ACTIVE"

    restarted = DurablePublicCaptureV1(QueueStream(time.time_ns))
    restarted.configure_capture(tmp_path, capture_epoch="b" * 64)
    with OpsRepository(tmp_path / "ops.sqlite") as repository, pytest.raises(
            RuntimeError, match="UNCLEAN_PUBLIC_CAPTURE_STOP_NEW_RUN_REQUIRED"):
        restarted.recover_controller_capture(repository)
    latch = QualificationLatchV1(tmp_path, run_id="standalone-capture-fixture", config_hash="0" * 64).read()
    assert latch is not None and latch.first_cause == "UNCLEAN_PUBLIC_CAPTURE_STOP"
