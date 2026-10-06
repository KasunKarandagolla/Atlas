"""S40 offline native capacity/soak runner, using the existing public fixtures.

There is no market/provider network and no capital authority. The default short
probe measures the named host; --native-gate requires Windows and >=20 minutes.
Failures retain the database, archives, reports and result instead of repairing
the evidence into a pass. The expected raw FIFO is a streaming digest, not an
ever-growing in-memory list of incoming frames.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import multiprocessing
import os
import platform
import queue
import sqlite3
import struct
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json
from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.bybit_source import BybitPublicSnapshotV1
from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.data.microstructure import LIVE_BOOK_MAX_FRAMES, LIVE_BOOK_MAX_LEVELS
from atlas.v2.data.public_archive_extents import read_extent
from atlas.v2.data.public_stream_continuity import MAX_OBSERVATION_REPLAY_CACHE, MAX_TRADE_ID_CACHE
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.product import resource_sample
from atlas.v2.runtime import active_history, production
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.owner_health_monitor import OwnerHealthMonitorV1
from atlas.v2.runtime.serviced_acquisition import ServicedPublicAcquisitionV1
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

# Reuse S32 metadata and S38 source/traffic contracts. This runner is shipped as
# validation source, never imported by the installed runtime.
from tests.v2.test_s38_sustained_public_stream import FixturePublicSource, MixedWorkload, QueueStream
from tests.v2.test_session037_active_history import KEY, indexed


class StablePublicSource(FixturePublicSource):
    """The exact original metadata is returned when a stopped fixture reopens."""

    def __init__(self) -> None:
        super().__init__()
        self._bound = False

    def bootstrap_products(self, *, now_ns: int) -> Any:
        if not self._bound:
            self.products = tuple(replace(product, observed_at_ns=now_ns, available_at_ns=now_ns)
                                  for product in self.products)
            self._bound = True
        return self.products


class SlowREST:
    def acquire_snapshot(self, *, now_ns: int) -> BybitPublicSnapshotV1:
        del now_ns
        time.sleep(4.9)
        return BybitPublicSnapshotV1((), False, "INCOMPLETE", "OFFLINE_FIXTURE_NO_MARKET_ASSERTION",
                                    0, time.time_ns())


def raw_digest_update(digest: Any, row: Any) -> None:
    payload = row.raw_payload_bytes if hasattr(row, "raw_payload_bytes") else row["raw_payload_bytes"]
    receipt = row.received_at_ns if hasattr(row, "received_at_ns") else row["received_at_ns"]
    available = row.available_at_ns if hasattr(row, "available_at_ns") else row["available_at_ns"]
    epoch = row.connection_epoch if hasattr(row, "connection_epoch") else row["connection_epoch"]
    digest.update(struct.pack("!QQQQ", len(payload), receipt, available, epoch))
    digest.update(payload)


def replay_raw_digest(repository: OpsRepository) -> tuple[int, str]:
    """Replay bounded extents in exact batch insertion order, with strict hashes."""
    digest = hashlib.sha256()
    count = 0
    cursor = repository._connection.execute(
        "SELECT artifact_ref FROM artifact_index NOT INDEXED "
        "WHERE artifact_type='PublicStreamTransportBatchV2' ORDER BY rowid")
    for row in cursor:
        entry = repository.get_artifact(row[0])
        assert entry is not None
        table = read_extent(repository, entry.metadata["batch"]["archive_extent_ref"])
        for frame in table.to_pylist():
            raw_digest_update(digest, frame)
            count += 1
    cursor.close()
    return count, digest.hexdigest()


def _export_worker(database: str, root: str, identity: TuningRunIdentityV1,
                   requests: Any, completions: Any) -> None:
    # Warm imports before the independent producer starts. Every actual export
    # still opens its own strict read-only snapshot at the unchanged 10s budget.
    import duckdb
    import pyarrow

    del duckdb, pyarrow
    original_snapshot = OpsRepository.read_snapshot
    snapshot_durations: list[float] = []

    @contextlib.contextmanager
    def timed_snapshot(self: OpsRepository) -> Any:
        started = time.monotonic()
        try:
            with original_snapshot(self) as connection:
                yield connection
        finally:
            snapshot_durations.append(time.monotonic() - started)

    OpsRepository.read_snapshot = timed_snapshot  # type: ignore[method-assign]
    completions.put({"worker_ready": True, "pid": os.getpid()})
    while True:
        request = requests.get()
        if request is None:
            return
        started = time.monotonic()
        pages = []
        try:
            for _ in range(request["max_pages"]):
                page_started = time.monotonic()
                snapshot_durations.clear()
                result = export_tuning_snapshot(database, root, identity, cutoff_ns=request["cutoff_ns"])
                pages.append({"elapsed_seconds": time.monotonic() - page_started,
                              "read_snapshot_seconds": sum(snapshot_durations),
                              "rows_written": result["rows_written"],
                              "after_rowid": result["after_rowid"], "through_rowid": result["through_rowid"],
                              "source_window_row_limit": result["source_window_row_limit"],
                              "source_window_scope": result["source_window_scope"],
                              "validation_failures": result["validation_failures"],
                              "budget_yielded": result["budget_yielded"], "has_more": result["has_more"]})
                if not result["has_more"] or result["blocked_future_evidence"]:
                    break
            completions.put({"request_id": request["request_id"], "state": "SUCCEEDED",
                "elapsed_seconds": time.monotonic() - started, "pages": pages,
                "validation_failures": result["validation_failures"], "has_more": result["has_more"],
                "blocked_future_evidence": result["blocked_future_evidence"],
                "through_rowid": result["through_rowid"], "counts": result["counts"],
                "status": result["report"]["status"]})
        except Exception as error:
            completions.put({"request_id": request["request_id"], "state": "FAILED",
                "error_type": type(error).__name__, "elapsed_seconds": time.monotonic() - started,
                "pages": pages})


def _bytes(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _json(path: Path, body: Any) -> None:
    # Synthetic telemetry/projections only. The actual capture journal and SQL
    # persistence retain their normal FULL/fsync paths unchanged.
    temporary = path.with_suffix(".tmp")
    temporary.write_text(canonical_json(body) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _source_sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def run_probe(root: Path, *, seconds: float, normal_rate: int = 160, burst_rate: int = 320,
              burst_seconds: float = 1.0, burst_period_seconds: float = 15,
              commit_stalls: tuple[float, ...] = (.1, .5, 1, 3, 5),
              report_interval_seconds: float = 30, native_gate: bool = False) -> dict[str, Any]:
    if (not 2 <= seconds <= 3600 or not 1 <= normal_rate <= burst_rate <= 320
            or not 0 < burst_seconds < burst_period_seconds or not 1 <= report_interval_seconds <= 300
            or len(commit_stalls) > 32 or any(not 0 <= stall <= 5 for stall in commit_stalls)):
        raise ValueError("probe exceeds its explicit bounded validation envelope")
    if native_gate and (sys.platform != "win32" or seconds < 1200):
        raise ValueError("native qualification fixture requires Windows and >=1200 wall-clock seconds")
    root.mkdir(parents=True, exist_ok=False)
    started_at = time.time_ns()
    identity = TuningRunIdentityV1(root.name, "a" * 64, _source_sha(), started_at)
    source, metadata = QueueStream(time.time_ns), StablePublicSource()
    capture = DurablePublicCaptureV1(source)
    port = production.create_bybit_public_ws_port(public_source=metadata, public_stream_source=capture)
    helper = ServicedPublicAcquisitionV1(SlowREST())
    expected_digest = hashlib.sha256()
    producer_done, stop = threading.Event(), threading.Event()
    expected_count = 0
    max_lateness = 0.0
    producer_error: str | None = None
    writer_threads: set[int] = set()
    samples: list[dict[str, Any]] = []
    stalls: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    exports: list[dict[str, Any]] = []
    reader_errors: list[str] = []
    readers_started = 0
    report_state: dict[str, Any] = {"state": "IDLE"}
    progress: dict[str, Any] = {"observed_at_ns": started_at}
    resources: dict[str, Any] = {}
    guidances: dict[str, int] = {}
    context = multiprocessing.get_context("spawn")
    requests, completions = context.Queue(maxsize=1), context.Queue(maxsize=1)
    report_process = context.Process(target=_export_worker,
        args=(str(root / "ops.sqlite"), str(root / "reports"), identity, requests, completions),
        name="s40-read-only-report-worker", daemon=True)
    reader: threading.Thread | None = None
    monitor: OwnerHealthMonitorV1 | None = None
    producer: threading.Thread | None = None
    outcome: dict[str, Any] = {"version": "SESSION040_NATIVE_RESILIENCE_PROBE_V1", "status": "TEST GATE",
        "source_sha": identity.source_sha, "platform": platform.platform(), "python": sys.version,
        "sqlite_version": sqlite3.sqlite_version, "root": str(root), "scope": "OFFLINE_ACTUAL_WALL_NATIVE_FIXTURE",
        "native_windows_gate": native_gate, "requested_seconds": seconds,
        "declared_rate": normal_rate, "declared_burst_rate": burst_rate, "burst_seconds": burst_seconds,
        "burst_period_seconds": burst_period_seconds, "capital_enabled": False, "assisted_enabled": False,
        "authority": "ZERO", "owner_live_source_qualification": "TEST GATE", "economics": "NOT ESTIMABLE"}
    outcome["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    outcome["worktree_dirty"] = bool(subprocess.check_output(
        ["git", "status", "--porcelain"], text=True).strip())
    report_process.start()
    wall_started = time.monotonic()

    def produce() -> None:
        nonlocal expected_count, max_lateness, producer_error
        workload = MixedWorkload()
        start = target = time.monotonic()
        try:
            while target - start < seconds and not stop.is_set() and not source.closed:
                if stop.wait(max(0, target - time.monotonic())):
                    break
                max_lateness = max(max_lateness, time.monotonic() - target)
                frame = workload.frame(time.time_ns())
                if not source.handoff.offer(frame):
                    producer_error = "PUBLIC_FRAME_REJECTED_OR_SOURCE_CLOSED"
                    break
                raw_digest_update(expected_digest, frame)
                expected_count += 1
                burst = (target - start) % burst_period_seconds < burst_seconds
                target += 1 / (burst_rate if burst else normal_rate)
        except Exception as error:
            producer_error = type(error).__name__
        finally:
            producer_done.set()

    def report_finished(*, wait: float = 0) -> bool:
        nonlocal report_state
        try:
            result = completions.get(timeout=wait) if wait else completions.get_nowait()
        except queue.Empty:
            return False
        if "worker_ready" in result:
            return True
        exports.append(result)
        report_state = {"state": result["state"], "completed_at_ns": time.time_ns(),
                        "validation_failures": sum(result.get("validation_failures", {}).values())}
        return True

    def request_report(request_id: str, *, final: bool = False) -> None:
        nonlocal report_state
        if report_state.get("state") == "RUNNING":
            return
        if not report_process.is_alive():
            raise RuntimeError("read-only report worker died")
        requests.put_nowait({"request_id": request_id, "cutoff_ns": time.time_ns(),
                             "max_pages": 64 if final else 8})
        report_state = {"state": "RUNNING", "started_at_ns": time.time_ns()}

    try:
        ready = completions.get(timeout=60)
        assert ready.get("worker_ready"), "report worker readiness missing"
        # Prepare the same serialized exact history seam used by S38, before
        # arrivals, then restore while raw traffic continues.
        history = None
        for offset in range(0, 1200, 128):
            history = advance(history, tuple(indexed(i) for i in range(offset, min(offset + 128, 1200))),
                              key=KEY, interval=BarIntervalV2.M15)
        assert history is not None
        history_head = {"state_json": json.dumps(history.to_dict()), "state_ref": history.content_hash}
        with OpsSupervisorV2(root / "ops.sqlite", port) as supervisor:
            supervisor.run_once()
            repository = supervisor.repository
            assert repository is not None
            assert repository._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert repository._connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
            original_connection = repository._connection
            original_register = repository.register_artifacts

            def register(entries: Any) -> Any:
                writer_threads.add(threading.get_ident())
                return original_register(entries)

            repository.register_artifacts = register  # type: ignore[method-assign]
            begun = time.monotonic()
            # Space the bounded fault list across the run; repeat the whole
            # list at each 300s mark in the long native soak.
            scheduled_stalls = [(2 + index * max(1, min(10, seconds / max(1, len(commit_stalls)))), delay)
                                for index, delay in enumerate(commit_stalls)]
            if seconds >= 1200:
                scheduled_stalls = [(offset + when, delay) for offset in range(0, int(seconds), 300)
                                    for when, delay in scheduled_stalls if offset + when < seconds - 10]
            next_stall = 0

            class FaultConnection:
                def __getattr__(self, name: str) -> Any:
                    return getattr(original_connection, name)

                def commit(self) -> Any:
                    nonlocal next_stall
                    elapsed = time.monotonic() - begun
                    if next_stall < len(scheduled_stalls) and elapsed >= scheduled_stalls[next_stall][0]:
                        _, delay = scheduled_stalls[next_stall]
                        next_stall += 1
                        before = capture.status()
                        arrivals_active = not producer_done.is_set()
                        fault_start = time.monotonic()
                        time.sleep(delay)
                        after = capture.status()
                        stalls.append({"requested_seconds": delay, "actual_seconds": time.monotonic() - fault_start,
                            "elapsed_seconds": elapsed, "queue_before": before.handoff.queue_items,
                            "queue_after": after.handoff.queue_items,
                            "captured_before": before.capture["captured_frames"],
                            "captured_after": after.capture["captured_frames"],
                            "pending_batches_after": after.capture["pending_batches"],
                            "producer_active_at_stall_start": arrivals_active,
                            "rejected_after": after.handoff.frames_rejected})
                    return original_connection.commit()

            repository._connection = FaultConnection()  # type: ignore[assignment]

            def sample() -> None:
                nonlocal resources
                status = capture.status()
                resources = {**resource_sample(root), "wal_bytes": _bytes(root / "ops.sqlite-wal"),
                    "current_footprint_bytes": _bytes(root / "ops.sqlite") + _bytes(root / "ops.sqlite-wal"),
                    "disk_reserve_bytes": 0}
                archived = sum(path.stat().st_size for path in (root / "ops-public-extents").glob("*.arrow"))
                latest = tuple(port._owner_stream_reports.values())
                samples.append({"elapsed_seconds": time.monotonic() - begun, "offered_frames": expected_count,
                    "queue_items": status.handoff.queue_items, "queue_high_water": status.handoff.high_water_items,
                    "frames_rejected": status.handoff.frames_rejected, "capture": status.capture,
                    "sqlite_bytes": _bytes(root / "ops.sqlite"), "wal_bytes": _bytes(root / "ops.sqlite-wal"),
                    "archive_bytes": archived, "receipt_journal_bytes": sum(
                        path.stat().st_size for path in (root / "ops-public-capture").glob("capture-*.bin")),
                    "reports_bytes": sum(path.stat().st_size for path in (root / "reports").rglob("*") if path.is_file()),
                    "resources": resources, "persistence": repository.persistence_metrics(),
                    "service_duration_ns": port._stream_max_service_duration_ns,
                    "service_gap_ns": port._stream_max_service_gap_ns,
                    "book_frames": [len(book._frames) for book in port._stream_books.values() if book is not None],
                    "book_levels": [[len(book.bids), len(book.asks)] for book in port._stream_books.values() if book is not None],
                    "trade_cache_items": [len(tracker.state.trade_identity_cache) for tracker in port._stream_trackers.values()],
                    "replay_cache_items": [len(tracker.state.observation_replay_cache) for tracker in port._stream_trackers.values()],
                    "current_channels": sum(report.source_current for report in latest),
                    "continuity_gaps": sum(report.gap_count for report in latest),
                    "report_worker_alive": report_process.is_alive()})

            sample()
            monitor = OwnerHealthMonitorV1(root, run_id="standalone-capture-fixture", config_hash="0" * 64,
                source=capture, progress=lambda: progress, persistence=repository.persistence_metrics,
                resources=lambda: resources, report=lambda: report_state, publish=_json)
            monitor.start()
            producer = threading.Thread(target=produce, name="s40-native-fixture-producer", daemon=True)
            producer.start()
            next_rest, next_export, next_history = 0.0, min(2, seconds / 2), min(1, seconds / 3)
            next_sample, next_reader, next_checkpoint = 5.0, min(3, seconds / 2), min(4, seconds * .75)
            history_restores = 0
            while not producer_done.is_set():
                elapsed = time.monotonic() - begun
                report_finished()
                if elapsed >= next_export:
                    request_report(f"live-{len(exports)}")
                    next_export = elapsed + report_interval_seconds
                if elapsed >= next_rest and seconds - elapsed > 6:
                    helper.acquire(now_ns=time.time_ns(), service=supervisor.service_public_stream)
                    next_rest = time.monotonic() - begun + 10
                supervisor.service_public_stream()
                progress = {"observed_at_ns": time.time_ns(), "stream_ingestion_failed": port._stream_ingestion_failed,
                    "stream_source_state": "HEALTHY_CURRENT" if port._owner_stream_reports and all(
                        report.source_current for report in port._owner_stream_reports.values()) else "INCOMPLETE_SNAPSHOT",
                    "stream_recovery_required": not all(report.book_sequence_valid for channel, report in
                        port._owner_stream_reports.items() if channel.startswith("orderbook.")),
                    "stream_unresolved_gap": any(report.gap_count for report in port._owner_stream_reports.values())}
                if elapsed >= next_history:
                    restored = active_history._decode_head(repository, history_head, key=KEY,
                        interval=BarIntervalV2.M15, service=supervisor.service_public_stream)
                    assert restored is not None and restored.content_hash == history.content_hash
                    history_restores += 1
                    next_history = elapsed + 60
                if elapsed >= next_reader and (reader is None or not reader.is_alive()):
                    def hold_snapshot() -> None:
                        try:
                            with OpsRepository(root / "ops.sqlite", read_only=True) as readonly, readonly.read_snapshot():
                                readonly._connection.execute("SELECT count(*) FROM source_health").fetchone()
                                stop.wait(min(10, max(.5, seconds / 3)))
                        except Exception as error:
                            reader_errors.append(type(error).__name__)

                    reader = threading.Thread(target=hold_snapshot, name="s40-long-read-snapshot", daemon=True)
                    reader.start()
                    readers_started += 1
                    next_reader = elapsed + 60
                if elapsed >= next_checkpoint:
                    at = time.monotonic()
                    values = repository.checkpoint()
                    checkpoints.append({"elapsed_seconds": elapsed, "duration_seconds": time.monotonic() - at,
                                        "result": list(values), "reader_alive": reader is not None and reader.is_alive()})
                    next_checkpoint = elapsed + 5 if reader is not None and reader.is_alive() else elapsed + 30
                if elapsed >= next_sample:
                    sample()
                    next_sample = elapsed + 5
                if monitor.latest is not None:
                    action = monitor.latest["assessment"]["action"]
                    guidances[action] = guidances.get(action, 0) + 1
                status = capture.status()
                if status.state == "FAILED" or status.handoff.overflowed:
                    raise AssertionError("supported capture workload failed; retained run cannot qualify")
                time.sleep(.02)
            producer.join(timeout=1)
            assert not producer.is_alive() and producer_error is None
            # Producer has intentionally ended. Flush all remaining accepted
            # traffic without claiming the stopped source remains current.
            deadline = time.monotonic() + 15
            while (capture.status().pending_frames or capture.status().handoff.queue_items
                   or capture.status().capture.get("active_capture_duration_ns", 0)):
                supervisor.service_public_stream()
                if time.monotonic() >= deadline:
                    raise AssertionError("bounded final capture drain did not recover headroom")
                time.sleep(.01)
            port.finish_public_capture(repository)
            monitor.close()
            monitor = None
            sample()
            stop.set()
            if reader is not None:
                reader.join(timeout=2)
            assert not reader_errors and (reader is None or not reader.is_alive())
            post_reader = repository.checkpoint()
            checkpoints.append({"post_reader": True, "result": list(post_reader)})
            assert post_reader[0] == 0 and post_reader[1] == post_reader[2]
            replay_count, replay_hash = replay_raw_digest(repository)
            assert replay_count == expected_count and replay_hash == expected_digest.hexdigest()
            assert writer_threads == {threading.get_ident()}
            status = capture.status()
            assert not status.handoff.overflowed and status.handoff.frames_rejected == 0
            assert status.handoff.high_water_items < 512 and status.capture["high_water_batches"] <= 64
            assert max_lateness < 1, "host did not deliver the declared independent producer schedule"
            assert all(sample["continuity_gaps"] == 0 for sample in samples)
            assert all(max(sample["book_frames"], default=0) <= LIVE_BOOK_MAX_FRAMES for sample in samples)
            assert all(max((max(levels) for levels in sample["book_levels"]), default=0) <= LIVE_BOOK_MAX_LEVELS
                       for sample in samples)
            assert all(max(sample["trade_cache_items"], default=0) <= MAX_TRADE_ID_CACHE for sample in samples)
            assert all(max(sample["replay_cache_items"], default=0) <= MAX_OBSERVATION_REPLAY_CACHE for sample in samples)
            max_metadata = repository._connection.execute(
                "SELECT max(length(metadata_json)) FROM artifact_index WHERE artifact_type='PublicStreamContinuityReportV1'"
            ).fetchone()[0]
            assert max_metadata < 16_384
            assert repository._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            outcome.update({"frames": expected_count, "raw_replay_frames": replay_count,
                "expected_raw_fifo_sha256": expected_digest.hexdigest(), "replayed_raw_fifo_sha256": replay_hash,
                "max_producer_lateness_seconds": max_lateness, "queue_high_water": status.handoff.high_water_items,
                "capture_high_water_batches": status.capture["high_water_batches"], "frames_rejected": 0,
                "continuity_max_metadata_bytes": max_metadata, "sole_writer_threads": len(writer_threads),
                "history_restores": history_restores, "long_readers_started": readers_started})
            repository._connection = original_connection
        # Separate post-stop export, after the sole writer has closed. Any
        # interrupted earlier snapshot keeps its prior manifest untouched.
        if report_state.get("state") == "RUNNING":
            assert report_finished(wait=180), "active report did not finish within bounded request pages"
        request_report("post-stop-final", final=True)
        assert report_finished(wait=300), "final report did not finish within bounded request pages"
        assert exports and all(result["state"] == "SUCCEEDED" for result in exports)
        assert all(not result["validation_failures"] for result in exports)
        assert all(page["read_snapshot_seconds"] <= 10 for result in exports for page in result["pages"])
        assert all(not page["validation_failures"] for result in exports for page in result["pages"])
        warm_samples = [sample for sample in samples if 35 <= sample["elapsed_seconds"] <= seconds - 6]
        if warm_samples:
            assert sum(sample["current_channels"] == 4 for sample in warm_samples) >= len(warm_samples) * .9, (
                "source-current did not remain current in 90% of post-warmup observed samples")
        if native_gate:
            assert warm_samples and history_restores > 10 and readers_started > 10
            assert len(stalls) == len(scheduled_stalls), "native fixture did not execute the complete scheduled stall matrix"
            assert all(stall["producer_active_at_stall_start"] for stall in stalls)
            assert all(stall["captured_after"] > stall["captured_before"]
                       for stall in stalls if stall["requested_seconds"] >= 1)
            assert all(sample["resources"]["threads"] <= samples[0]["resources"]["threads"] + 8
                       for sample in samples), "bounded worker graph gained unbounded Python threads"
            assert all(sample["resources"]["rss_bytes"] is not None
                       and sample["resources"]["rss_bytes"] < 1_500_000_000 for sample in warm_samples), (
                           "RSS crossed the live-health resource-pressure ceiling")
            assert all(sample["resources"]["handles"] is not None
                       and sample["resources"]["handles"] <= samples[0]["resources"]["handles"] + 128
                       for sample in warm_samples), "native handle growth exceeded the bounded diagnostic allowance"
        assert not exports[-1]["has_more"] and not exports[-1]["blocked_future_evidence"]
        # Exact clean capture identity/tail remains reopenable; a fresh network
        # epoch requires fresh book snapshots and earns no invented continuity.
        reopened_capture = DurablePublicCaptureV1(QueueStream(time.time_ns))
        reopened_port = production.create_bybit_public_ws_port(public_source=metadata, public_stream_source=reopened_capture)
        with OpsSupervisorV2(root / "ops.sqlite", reopened_port) as reopened:
            reopened.run_once()
            assert reopened.repository is not None
            assert replay_raw_digest(reopened.repository) == (expected_count, expected_digest.hexdigest())
        outcome["status"] = "TESTED"
        outcome["clean_reopen"] = "TESTED"
    except Exception as error:
        outcome["failure_type"] = type(error).__name__
        outcome["failure_reason"] = str(error) if isinstance(error, AssertionError) else "SANITIZED_FIXTURE_FAILURE"
    finally:
        cleanup_errors = []
        stop.set()
        if producer is not None:
            producer.join(timeout=2)
        if reader is not None:
            reader.join(timeout=2)
        if monitor is not None:
            try:
                monitor.close()
            except Exception as error:
                cleanup_errors.append("MONITOR_" + type(error).__name__)
        try:
            if not helper.close(timeout_s=.1):
                cleanup_errors.append("PUBLIC_ACQUISITION_STILL_PENDING")
        except Exception as error:
            cleanup_errors.append("ACQUISITION_" + type(error).__name__)
        try:
            requests.put_nowait(None)
        except queue.Full:
            pass
        try:
            report_process.join(timeout=5)
            if report_process.is_alive():
                report_process.terminate()
                report_process.join(timeout=5)
                cleanup_errors.append("REPORT_WORKER_FORCED_TERMINATION")
            requests.close()
            completions.close()
        except Exception as error:
            cleanup_errors.append("REPORT_" + type(error).__name__)
        trends = {}
        if len(samples) >= 2:
            first, last = samples[0], samples[-1]
            span = last["elapsed_seconds"] - first["elapsed_seconds"]
            for name in ("sqlite_bytes", "wal_bytes", "archive_bytes", "receipt_journal_bytes", "reports_bytes"):
                trends[name + "_per_second"] = (last[name] - first[name]) / span if span > 0 else None
            for name in ("rss_bytes", "peak_rss_bytes", "threads", "handles", "cpu_seconds"):
                a, b = first["resources"].get(name), last["resources"].get(name)
                trends[name + "_delta"] = b - a if a is not None and b is not None else None
        if cleanup_errors:
            outcome["status"] = "TEST GATE"
        outcome.update({"completed_at_ns": time.time_ns(), "producer_error_type": producer_error,
            "observed_frames": expected_count, "samples": samples, "commit_stalls": stalls,
            "checkpoints": checkpoints, "exports": exports, "reader_errors": reader_errors,
            "operator_guidance_counts": guidances, "rest_acquisition": helper.status(),
            "wall_clock_seconds": time.monotonic() - wall_started, "resource_growth_trends": trends,
            "cleanup_error_types": cleanup_errors})
        _json(root / "resilience-result.json", outcome)
    return outcome


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="new probe-only evidence directory")
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--native-gate", action="store_true", help="Windows-only >=20min acceptance fixture")
    parser.add_argument("--rate", type=int, default=160)
    parser.add_argument("--burst-rate", type=int, default=320)
    args = parser.parse_args()
    result = run_probe(args.root, seconds=args.seconds, normal_rate=args.rate, burst_rate=args.burst_rate,
                       native_gate=args.native_gate)
    print(json.dumps({key: result[key] for key in ("status", "root", "source_sha", "observed_frames",
        "native_windows_gate")}, sort_keys=True), flush=True)
    return 0 if result["status"] == "TESTED" else 2


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
