"""Bounded mixed broad workload probe; synthetic evidence, not qualification."""
from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from decimal import Decimal

import pyarrow.parquet as pq

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import OpportunityWatchV2, WatchStateV2
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2
from atlas.v2.runtime.broad_universe import DAY_NS, active_products, full_universe, latest_workset
from atlas.v2.runtime.production import BroadProductionOpsCyclePortV2
from atlas.v2.runtime.read_only_report_worker import ReadOnlyReportWorkerV1
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from .test_session041_public_runtime_integration import NOW, _DeterministicBybitSource, _idle_stream


def _contracts() -> tuple[ProductContractV2, ...]:
    result = []
    for venue in (VenueV2.BYBIT, VenueV2.BINANCE):
        for index in range(512):
            symbol = ("BTCUSDT" if index == 0 else "ETHUSDT" if index == 1
                      else f"MIX{index:03d}USDT")
            revision = sha256_json({"venue": venue.value, "symbol": symbol, "revision": 1})
            key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET,
                ProductTypeV2.LINEAR_PERPETUAL, symbol, symbol.removesuffix("USDT"),
                "USDT", "USDT", revision)
            result.append(ProductContractV2(key, NOW, NOW, NOW, Decimal("1"),
                Decimal("0.01"), Decimal("0.001"), Decimal("0.001"),
                TradingStatusV2.TRADING, revision))
    return tuple(result)


def _frame(index: int, key: InstrumentKeyV2) -> CapturedPublicFrameV2:
    at_ns = NOW - 1_000_000_000 + index + 1
    channel = f"publicTrade.{key.native_symbol}"
    body = {"topic": channel, "type": "snapshot", "ts": at_ns // 1_000_000,
        "data": [{"T": at_ns // 1_000_000, "s": key.native_symbol, "S": "Buy",
                  "v": "0.01", "p": "100", "i": f"mixed-{index}"}]}
    raw = json.dumps(body, separators=(",", ":")).encode()
    return CapturedPublicFrameV2(key.venue, "BYBIT_PUBLIC_WS_BROAD_V2",
        channel, raw, hashlib.sha256(raw).hexdigest(),
        at_ns, at_ns, 1)


def _wal_size(path) -> int:
    wal = path.with_name(path.name + "-wal")
    return wal.stat().st_size if wal.exists() else 0


def _frame_identity(frame: CapturedPublicFrameV2) -> tuple:
    return (frame.venue, frame.source_id, frame.channel, frame.raw_payload_bytes,
            frame.raw_payload_hash, frame.received_at_ns, frame.available_at_ns,
            frame.connection_epoch)


def test_mixed_1024_contract_public_workload_under_bounded_stalls(tmp_path, record_property):
    products = _contracts()
    clock = [NOW]
    path = tmp_path / "mixed-ops.sqlite"
    runtime = BroadPublicRuntimeV2(clock_ns=lambda: clock[0],
        stream_factories={"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()})
    source = _DeterministicBybitSource(products, NOW)
    port = BroadProductionOpsCyclePortV2(public_source=source, broad_runtime=runtime,
        clock_ns=lambda: clock[0])
    interpreted: list[CapturedPublicFrameV2] = []
    original_interpreter = runtime._interpret_frames
    interpreter_samples: list[dict[str, int]] = []

    def observe_interpreter(repository, frames, *, now_ns):
        started = time.monotonic_ns()
        interpreted.extend(frames)
        try:
            return original_interpreter(repository, frames, now_ns=now_ns)
        finally:
            interpreter_samples.append({"frames": len(frames), "duration_ns": time.monotonic_ns() - started})

    runtime._interpret_frames = observe_interpreter
    identity = TuningRunIdentityV1("s41-mixed-broad-capacity", "a" * 64, "b" * 40, 0)
    report_dir = tmp_path / "reports"
    offered: list[CapturedPublicFrameV2] = []
    commit_observation: dict[str, float | int] = {}
    archive_stall_seconds: list[float] = []
    reader_snapshots: list[tuple[int, int]] = []
    report_release = threading.Event()
    producer_stop = threading.Event()
    def export_during_stall():
        if not report_release.wait(60.0):
            raise TimeoutError("report export did not reach the injected commit stall")
        return export_tuning_snapshot(path, report_dir, identity, cutoff_ns=NOW + 20_000_000_000)

    exporter = ReadOnlyReportWorkerV1(export_during_stall)
    export_completion = []

    try:
        with OpsRepository(path) as repository:
            # Persist representative active watch state through the repository's
            # domain API. The production recovery/workset code discovers these
            # rows; the stream plan receives no direct active-key override.
            cohort_venue = sorted((VenueV2.BYBIT, VenueV2.BINANCE), key=lambda venue: venue.value)[
                (NOW // DAY_NS) % 2]
            cohort_candidates = sorted((item for item in products if item.key.venue == cohort_venue),
                key=lambda item: (item.key.native_symbol != "BTCUSDT",
                    item.key.native_symbol not in {"BTCUSDT", "ETHUSDT"},
                    item.key.to_canonical_json()))[:20]
            watch_keys = tuple(item.key for item in cohort_candidates[:8])
            for index, key in enumerate(watch_keys):
                watch_id = sha256_json({"session": "041-mixed-watch", "key": key.to_dict()})
                repository.create_watch(OpportunityWatchV2(
                    watch_id, key, "SYNTHETIC_CAPACITY_PROBE", "V1",
                    sha256_json({"policy": index}), WatchStateV2.DETECTED, 0,
                    NOW, NOW, sha256_json({"thesis": index}), (), "TRADE",
                    NOW + 60_000_000_000, NOW))
            recovery = port.recover(repository, now_ns=NOW)
            assert recovery.required_source_ids == ("BINANCE_PUBLIC_V2", "BYBIT_PUBLIC_V2")
            assert port._collector_recovery is not None
            collector = port._collector_recovery.collector
            archive_flush_pending_counts: list[int] = []
            archive_flush_durations_seconds: list[float] = []
            original_flush_archive = collector.flush_archive

            def observe_archive_flush():
                archive_flush_pending_counts.append(collector.pending_archive_count)
                started = time.monotonic()
                try:
                    return original_flush_archive()
                finally:
                    archive_flush_durations_seconds.append(time.monotonic() - started)

            collector.flush_archive = observe_archive_flush
            snapshot = source.acquire_snapshot(now_ns=NOW)
            port._register_refreshed_stream_products(repository, snapshot)
            runtime_service_samples: list[int] = []
            runtime_service_timestamps: list[tuple[int, str]] = []
            runtime_stage_samples: list[dict[str, object]] = []
            service_phase = ["INITIAL_BROAD_SNAPSHOT_ADOPTION"]
            original_port_service = port.service_public_stream

            def observe_port_service(repository):
                caller = sys._getframe(1)
                frames = []
                while caller is not None:
                    frames.append(caller)
                    caller = caller.f_back
                names = {frame.f_code.co_name for frame in frames}
                workset = next((frame for frame in frames
                    if frame.f_code.co_name == "publish_broad_workset"), None)
                if workset is not None:
                    service_phase[0] = f"BROAD_WORKSET_LINE_{workset.f_lineno}"
                elif "reconfigure" in names:
                    service_phase[0] = "BROAD_STREAM_RECONFIGURE"
                elif "finish" in names:
                    service_phase[0] = "BROAD_STREAM_FINISH"
                else:
                    if "<lambda>" in names:
                        service_phase[0] = "SERVICED_ACQUISITION_OR_SNAPSHOT"
                    else:
                        adoption = max((frame for frame in frames
                            if frame.f_code.co_name == "_persist_broad_public_snapshot"),
                            key=lambda frame: frame.f_lineno, default=None)
                        if adoption is not None and adoption.f_lineno >= 6307:
                            if adoption.f_lineno <= 6325:
                                service_phase[0] = "BROAD_SNAPSHOT_AND_WORKSET"
                            elif adoption.f_lineno <= 6333:
                                service_phase[0] = "BROAD_ENRICHMENT_SELECTION"
                            elif adoption.f_lineno <= 6347:
                                service_phase[0] = "BROAD_STREAM_RECONFIGURE"
                            else:
                                service_phase[0] = "BROAD_SCHEDULER_STATE_PUBLISH"
                return original_port_service(repository)

            port.service_public_stream = observe_port_service
            original_runtime_service = runtime.service
            runtime_persistence_samples: list[dict[str, object]] = []

            def observe_runtime_service(repository, *, now_ns):
                started = time.monotonic_ns()
                runtime_service_timestamps.append((started, service_phase[0]))
                try:
                    return original_runtime_service(repository, now_ns=now_ns)
                finally:
                    duration_ns = time.monotonic_ns() - started
                    runtime_service_samples.append(duration_ns)
                    if duration_ns >= 100_000_000:
                        persistence = repository.persistence_metrics()
                        runtime_persistence_samples.append({
                            "service_duration_ns": duration_ns,
                            "transaction_count": persistence.get("transaction_count"),
                            "row_and_archive_duration_ns": persistence.get("row_and_archive_duration_ns"),
                            "commit_duration_ns": persistence.get("commit_duration_ns"),
                            "max_commit_duration_ns": persistence.get("max_commit_duration_ns"),
                        })

            runtime.service = observe_runtime_service
            original_reconfigure = runtime.reconfigure

            def observe_reconfigure(*args, **kwargs):
                started = time.monotonic_ns()
                try:
                    return original_reconfigure(*args, **kwargs)
                finally:
                    runtime_stage_samples.append({"stage": "runtime.reconfigure",
                        "duration_ns": time.monotonic_ns() - started})

            runtime.reconfigure = observe_reconfigure
            archive_write_samples: list[dict[str, int]] = []
            assert runtime._archive is not None
            original_runtime_archive_write = runtime._archive.write_chunks

            def observe_runtime_archive_write(chunks):
                frame_count = sum(len(group) for group in chunks)
                started = time.monotonic_ns()
                try:
                    return original_runtime_archive_write(chunks)
                finally:
                    archive_write_samples.append({"frames": frame_count,
                        "duration_ns": time.monotonic_ns() - started})

            runtime._archive.write_chunks = observe_runtime_archive_write
            # The fixture is synthetically shaped and may be marked incomplete
            # by the public health gate; persistence and receipt identity below
            # remain the measured behavior.
            port._persist_broad_public_snapshot(repository, snapshot, now_ns=NOW)

            service_phase[0] = "INITIAL_UNIVERSE_AND_WORKSET_READ"
            def service_runtime():
                port.service_public_stream(repository)

            universe = full_universe(repository, cutoff_ns=NOW, service=service_runtime)
            body = latest_workset(repository, cutoff_ns=NOW, service=service_runtime)
            selected = active_products(repository, cutoff_ns=NOW, service=service_runtime)
            assert universe is not None and body is not None and selected is not None
            assert len(universe.entries) == 1024
            assert len({item.key for item in universe.entries}) == 1024
            assert {item.key.venue for item in universe.entries} == {VenueV2.BYBIT, VenueV2.BINANCE}
            assert len(selected) <= 24 and len(selected) > 0
            wal_after_first_publication = _wal_size(path)
            assert body["selected_count"] == len(selected)
            assert not any(entry.capital_eligible or entry.scanner_eligible for entry in universe.entries)
            # Stream subscriptions follow the collector's actual workset tiers,
            # repository watch state, and runtime benchmarks.
            actual_tiers = {InstrumentKeyV2.from_dict(json.loads(key)): int(value)
                            for key, value in body["tiers"].items()}
            tier3_keys = {key for key, tier in actual_tiers.items() if tier >= 3}
            deep_keys = {item.key for item in runtime.plan.identities
                         if item.channel.startswith("orderbook.50.") or "@depth" in item.channel}
            assert runtime.plan is not None
            assert len(deep_keys) == 8 and set(watch_keys).issubset(deep_keys)
            assert tier3_keys.issubset(set(runtime.plan.keys))
            assert len(runtime.plan.keys) <= 24

            # Open a separate read-only snapshot while the live database is in WAL mode.
            # Keep the supervisor's normal public-stream service boundary active
            # while diagnostics inspect the published broad universe.
            service_phase[0] = "INITIAL_READ_ONLY_SNAPSHOT_OPEN"
            port.service_public_stream(repository)
            with OpsRepository(path, read_only=True) as reader:
                reader_snapshots.append((len(full_universe(reader, cutoff_ns=NOW,
                    service=service_runtime).entries),
                    reader._connection.execute("PRAGMA data_version").fetchone()[0]))
            port.service_public_stream(repository)

            assert runtime.source is not None and runtime.capture is not None
            bybit_lane = runtime.source._lanes["BYBIT"]
            handoff = bybit_lane._handoff
            stream_key = next(identity.key for identity in runtime.plan.identities
                              if identity.venue == VenueV2.BYBIT
                              and identity.channel.startswith("publicTrade."))

            # A real read-only report worker runs alongside ingestion; its output
            # is polled while capture continues, then the exact same export is
            # checked after the ingestion cutoff.
            # The read-only worker is gated until the active commit stall, so
            # its SQLite snapshot opens while the writer is committing.
            port.service_public_stream(repository)
            assert exporter.start()
            before_artifacts = repository._connection.execute(
                "SELECT count(*) FROM artifact_index").fetchone()[0]

            original_connection = repository._connection
            stalled = False
            stream_go = threading.Event()
            producer_done = threading.Event()
            producer_error: list[BaseException] = []
            captured_during_commit: list[int] = []
            capture_during_commit_samples: list[dict[str, object]] = []

            def producer() -> None:
                try:
                    assert stream_go.wait(2.0)
                    next_due = time.monotonic()
                    index = 0
                    while not producer_stop.is_set():
                        frame = _frame(index, stream_key)
                        if not handoff.offer(frame):
                            capture_status = runtime.capture.status() if runtime.capture is not None else None
                            handoff_status = handoff.snapshot()
                            detail = {
                                "index": index,
                                "queue_items": handoff_status.queue_items,
                                "queue_max": handoff_status.max_queue_items,
                                "queue_high_water": handoff_status.high_water_items,
                                "queue_rejected": handoff_status.frames_rejected,
                                "queue_closed": handoff_status.closed,
                                "queue_overflowed": handoff_status.overflowed,
                                "queue_backpressure": handoff_status.backpressure,
                                "capture_pending_batches": (capture_status.capture["pending_batches"]
                                    if capture_status is not None else None),
                                "capture_max_pending_batches": (capture_status.capture["max_pending_batches"]
                                    if capture_status is not None else None),
                                "capture_pressure_threshold_batches": (capture_status.capture[
                                    "pressure_stop_threshold_batches"] if capture_status is not None else None),
                                "capture_pending_frames": (capture_status.capture["pending_frames"]
                                    if capture_status is not None else None),
                                "capture_captured_frames": (capture_status.capture["captured_frames"]
                                    if capture_status is not None else None),
                                "capture_delivered_frames": (capture_status.capture["delivered_frames"]
                                    if capture_status is not None else None),
                                "capture_terminal_error": (capture_status.capture["terminal_error"]
                                    if capture_status is not None else None),
                            }
                            raise AssertionError(
                                "bounded public handoff rejected a frame: " + json.dumps(detail, sort_keys=True))
                        offered.append(frame)
                        index += 1
                        next_due += 1 / 160
                        producer_stop.wait(max(0.0, next_due - time.monotonic()))
                except BaseException as exc:
                    producer_error.append(exc)
                finally:
                    producer_done.set()

            class CommitStall:
                def __getattr__(self, name):
                    return getattr(original_connection, name)

                def commit(self):
                    nonlocal stalled
                    if not stalled:
                        stalled = True
                        stream_go.set()
                        report_release.set()
                        before = time.monotonic()
                        assert runtime.capture is not None
                        deadline = before + 3.0
                        while time.monotonic() < deadline:
                            capture_status = runtime.capture.status()
                            captured = int(capture_status.capture["captured_frames"])
                            capture_during_commit_samples.append({
                                "captured_frames": captured,
                                "queued_frames": capture_status.handoff.queue_items,
                                "pending_frames": capture_status.capture["pending_frames"],
                                "terminal_error": capture_status.capture["terminal_error"],
                                "archive_phase": capture_status.capture["archive"].get("active_phase"),
                                "capture_thread_alive": bool(runtime.capture._capture._thread
                                    and runtime.capture._capture._thread.is_alive()),
                            })
                            if captured and time.monotonic() - before >= 0.5:
                                captured_during_commit.append(captured)
                                break
                            time.sleep(0.01)
                        commit_observation["stall_seconds"] = time.monotonic() - before
                        commit_observation["captured_during_stall"] = max(captured_during_commit, default=0)
                        commit_observation["samples"] = capture_during_commit_samples
                    return original_connection.commit()

            producer_thread = threading.Thread(target=producer, name="s41-mixed-producer")
            producer_thread.start()
            repository._connection = CommitStall()
            try:
                marker = {"mixed_stall_marker": NOW}
                marker_ref = sha256_json(marker)
                repository.register_artifact(ArtifactIndexEntryV2(marker_ref,
                    "Session041MixedStallMarkerV1", marker_ref, NOW, NOW, marker))
            finally:
                repository._connection = original_connection
            marker_count = repository._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
            completion = None
            report_deadline = time.monotonic() + 60.0
            report_service_calls = 0
            service_phase[0] = "CONCURRENT_REPORT_EXPORT"
            while completion is None and time.monotonic() < report_deadline:
                try:
                    port.service_public_stream(repository)
                except Exception as exc:
                    capture_status = runtime.capture.status() if runtime.capture is not None else None
                    detail = {
                        "failure": f"{type(exc).__name__}:{exc}",
                        "captured_frames": (capture_status.capture.get("captured_frames")
                            if capture_status is not None else None),
                        "delivered_frames": (capture_status.capture.get("delivered_frames")
                            if capture_status is not None else None),
                        "pending_batches": (capture_status.capture.get("pending_batches")
                            if capture_status is not None else None),
                        "pending_frames": (capture_status.capture.get("pending_frames")
                            if capture_status is not None else None),
                        "handoff_items": (capture_status.handoff.queue_items
                            if capture_status is not None else None),
                        "handoff_overflowed": (capture_status.handoff.overflowed
                            if capture_status is not None else None),
                        "capture_terminal_error": (capture_status.capture.get("terminal_error")
                            if capture_status is not None else None),
                        "service_calls": len(runtime_service_samples),
                        "service_max_ns": max(runtime_service_samples, default=0),
                        "service_recent_ns": runtime_service_samples[-12:],
                        "interpreter_recent_ms": [round(row["duration_ns"] / 1_000_000, 2)
                            for row in interpreter_samples[-12:]],
                        "archive_recent_ms": [round(row["duration_ns"] / 1_000_000, 2)
                            for row in archive_write_samples[-12:]],
                        "capture_archive": (capture_status.capture.get("archive")
                            if capture_status is not None else None),
                        "service_persistence_slow_samples": runtime_persistence_samples[-5:],
                    }
                    raise AssertionError("mixed capacity service failed: " +
                                         json.dumps(detail, sort_keys=True)) from exc
                report_service_calls += 1
                completion = exporter.poll()
                if completion is None:
                    time.sleep(0.01)
            assert completion is not None
            producer_stop.set()
            producer_thread.join(timeout=3.0)
            assert producer_done.is_set() and not producer_error
            export_completion.append(completion)
            artifact_count_after_concurrent_service = repository._connection.execute(
                "SELECT count(*) FROM artifact_index").fetchone()[0]
            assert artifact_count_after_concurrent_service >= marker_count
            artifact_rows_added_during_report = artifact_count_after_concurrent_service - marker_count
            assert artifact_rows_added_during_report > 0
            # Continue the same real collector, metadata and workset path after
            # the commit/export overlap while stream capture is still active.
            assert runtime._archive is not None
            original_write_chunks = runtime._archive.write_chunks
            archive_started = threading.Event()
            during_archive: list[tuple[int, int]] = []

            # Keep one captured trade queued until the snapshot-adoption loop
            # services the stream. That service is what reaches the archive
            # writer; the following burst then measures capture during its stall.
            archive_seed = _frame(2_000_000, stream_key)
            assert handoff.offer(archive_seed)
            offered.append(archive_seed)

            def stalled_archive_write(chunks):
                before = time.monotonic()
                assert runtime.capture is not None
                frames_before = int(runtime.capture.status().capture["captured_frames"])
                archive_started.set()
                time.sleep(1.0)
                archive_stall_seconds.append(time.monotonic() - before)
                frames_after = int(runtime.capture.status().capture["captured_frames"])
                during_archive.append((frames_before, frames_after))
                return original_write_chunks(chunks)

            burst_errors: list[BaseException] = []

            def archive_stall_burst() -> None:
                try:
                    assert archive_started.wait(10.0)
                    for index in range(2_000_001, 2_000_011):
                        frame = _frame(index, stream_key)
                        if not handoff.offer(frame):
                            raise AssertionError("bounded archive-stall burst was rejected")
                        offered.append(frame)
                        time.sleep(1 / 320)
                except BaseException as exc:
                    burst_errors.append(exc)

            burst_thread = threading.Thread(target=archive_stall_burst, name="s41-mixed-archive-burst")
            burst_thread.start()
            runtime._archive.write_chunks = stalled_archive_write
            service_phase[0] = "REFRESHED_BROAD_SNAPSHOT_AND_WORKSET"
            clock[0] = NOW + 10_000_000_000
            source.at_ns = clock[0]
            # Exercise the production bounded acquisition worker: the sole
            # writer must continue servicing the stream while REST work builds
            # the immutable broad snapshot in its isolated worker.
            mixed_snapshot = port._serviced_acquisition.acquire(now_ns=clock[0],
                service=lambda: port.service_public_stream(repository))
            port._register_refreshed_stream_products(repository, mixed_snapshot)
            try:
                port._persist_broad_public_snapshot(repository, mixed_snapshot, now_ns=clock[0])
            finally:
                runtime._archive.write_chunks = original_write_chunks
            burst_thread.join(timeout=2.0)
            assert not burst_thread.is_alive() and not burst_errors
            assert archive_started.is_set()
            assert archive_stall_seconds and archive_stall_seconds[0] >= 1.0
            assert during_archive and during_archive[0][1] > during_archive[0][0]
            wal_after_mixed_ingestion = _wal_size(path)
            assert wal_after_first_publication > 0 and wal_after_mixed_ingestion > 0
            record_property("session041_capture_during_commit", json.dumps(commit_observation, sort_keys=True))
            assert commit_observation["stall_seconds"] >= 0.5
            assert commit_observation["captured_during_stall"] > 0
            body = latest_workset(repository, cutoff_ns=clock[0], service=service_runtime)
            selected = active_products(repository, cutoff_ns=clock[0], service=service_runtime)
            assert body is not None and selected is not None

            # Boundedly drain all queued batches and close the durable capture.
            service_phase[0] = "FINAL_CAPTURE_DRAIN"
            for _ in range(40):
                port.service_public_stream(repository)
                if runtime.capture.status().capture["pending_frames"] == 0:
                    break
                time.sleep(0.01)
            port.finish_public_capture(repository)
            final = runtime.status()
            assert final.capture["pending_frames"] == 0
            assert final.capture["pending_batches"] == 0
            assert final.handoff.frames_received == len(offered)
            assert final.handoff.frames_drained == len(offered)
            assert final.handoff.frames_rejected == 0
            assert not final.handoff.overflowed
            assert final.handoff.high_water_items <= final.handoff.max_queue_items
            assert final.handoff.high_water_bytes <= final.handoff.max_queue_bytes
            assert len(interpreted) == len(offered)
            assert [_frame_identity(item) for item in interpreted] == [
                _frame_identity(item) for item in offered]
            assert final.capture["terminal_error"] is None

            receipt = repository.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=NOW + 20_000_000_000, limit=1).entries[0]
            receipt_refs = {ref for refs in receipt.metadata["receipt"]["source_observation_refs"].values()
                            for ref in refs}
            assert len(receipt_refs) == 1024
            assert not receipt.metadata["receipt"]["rejected_record_indexes"]
            assert archive_flush_pending_counts
            assert max(archive_flush_pending_counts) <= 64
            expected_refs = {sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": record.observation.record_id}) for record in mixed_snapshot.records}
            assert receipt_refs == expected_refs
            assert repository._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] >= before_artifacts

            after_artifacts = repository._connection.execute(
                "SELECT count(*) FROM artifact_index").fetchone()[0]
            assert after_artifacts >= marker_count
            manifest = completion.result
            report_validation_failures = (manifest.get("validation_failures")
                if isinstance(manifest, dict) else None)
            assert exporter.close(timeout_s=30.0)

            service_gap_rows = [
                {"index": index, "gap_ns": current[0] - prior[0],
                 "previous_call_duration_ns": runtime_service_samples[index],
                 "from_phase": prior[1], "to_phase": current[1]}
                for index, (prior, current) in enumerate(zip(
                    runtime_service_timestamps, runtime_service_timestamps[1:], strict=False))
            ]
            longest_service_gap = max(service_gap_rows, key=lambda row: row["gap_ns"], default=None)
            service_gap_topology = sorted(service_gap_rows, key=lambda row: row["gap_ns"], reverse=True)[:5]
            record_property("session041_stream_fairness", json.dumps({
                "runtime_max_service_gap_ns": final.max_service_gap_ns,
                "runtime_max_gap_phases": longest_service_gap,
                "runtime_service_gap_topology": service_gap_topology,
                "service_gap_budget_ns": 1_500_000_000,
            }, sort_keys=True))

            # The exporter has a fixed per-snapshot budget. Continue its exact
            # immutable rowid checkpoints until the stopped-run report is whole;
            # every yielded page remains manifested and strict validation stays
            # enabled. Never expand a page's time budget to force completion.
            final_before = repository._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
            stopped_manifest = None
            stopped_export_page_seconds: list[float] = []
            prior_through_rowid = -1
            for _ in range(32):
                page_started = time.monotonic()
                stopped_manifest = export_tuning_snapshot(path, report_dir, identity,
                    cutoff_ns=NOW + 20_000_000_000)
                stopped_export_page_seconds.append(time.monotonic() - page_started)
                assert stopped_manifest["through_rowid"] > prior_through_rowid or not stopped_manifest["has_more"]
                prior_through_rowid = stopped_manifest["through_rowid"]
                if not stopped_manifest["has_more"]:
                    break
            assert stopped_manifest is not None and not stopped_manifest["has_more"], (
                "stopped export did not reach the immutable source tail within 32 bounded pages"
            )
            final_after = repository._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
            assert final_before == final_after
            assert stopped_manifest["validation_failures"] == {}
            report_root = report_dir / identity.run_id
            rows = []
            for manifest_path in (report_root / "manifests").glob("*.json"):
                page = json.loads(manifest_path.read_text())
                rows.extend(pq.read_table(report_root / page["partition"]).to_pylist())
            exported_workset = next(row for row in rows if row["artifact_type"] == "BroadUniverseWorksetV2")
            exported_body = json.loads(exported_workset["evidence_payload_json"])
            assert set(exported_body["active_product_refs"]) == {product.content_hash for product in selected}

            report_export_seconds = (completion.completed_at_ns - completion.started_at_ns) / 1_000_000_000
            wal_bytes = _wal_size(path)
            metrics = {
                "scope": "BOUNDED_SYNTHETIC_MIXED_PROBE_ONLY",
                "capacity_qualified": False,
                "contracts": 1024,
                "contracts_per_venue": 512,
                "workset_history_keys": len(selected),
                "runtime_plan_keys": len(runtime.plan.keys),
                "runtime_deep_keys_from_durable_watch_or_tier3": len(deep_keys),
                "runtime_tier3_keys_from_workset": len(tier3_keys),
                "durable_watch_keys": len(watch_keys),
                "stream_frames": len(offered),
                "stream_frame_venues": {"BYBIT": len(offered), "BINANCE": 0},
                "global_paced_base_target_fps": 160,
                "bounded_burst_target_fps": 320,
                "commit_stall_seconds": commit_observation["stall_seconds"],
                "frames_captured_during_commit_stall": commit_observation["captured_during_stall"],
                "archive_stall_seconds": archive_stall_seconds[0],
                "archive_stall_capture_frames_before_after": list(during_archive[0]),
                "handoff_high_water_items": final.handoff.high_water_items,
                "handoff_max_queue_items": final.handoff.max_queue_items,
                "capture_high_water_batches": final.capture["high_water_batches"],
                "capture_max_pending_batches": final.capture["max_pending_batches"],
                "capture_batch_frame_limit": final.capture["batch_frame_limit"],
                "max_interpreted_frames_per_batch": max(
                    (row["frames"] for row in interpreter_samples), default=0),
                "runtime_max_service_gap_ns": final.max_service_gap_ns,
                "runtime_max_gap_phases": longest_service_gap,
                "runtime_service_gap_topology": service_gap_topology,
                "service_gap_budget_ns": 1_500_000_000,
                "service_gap_budget_basis": (
                    "At the declared 160-frame/s sustained input rate, 1.5 seconds is 240 frames; "
                    "the 512-frame handoff retains 272 slots for burst and scheduling variance."
                ),
                "read_only_snapshot_universe_rows": reader_snapshots[0][0],
                "wal_bytes_at_stop": wal_bytes,
                "report_worker_status": export_completion[0].status,
                "report_worker_error_type": export_completion[0].error_type,
                "report_validation_failures": report_validation_failures,
                "report_worker_export_seconds": report_export_seconds,
                "stopped_export_pages": len(stopped_export_page_seconds),
                "stopped_export_page_seconds": stopped_export_page_seconds,
                "stopped_export_has_more": stopped_manifest["has_more"],
                "stopped_export_read_only_artifact_count_unchanged": True,
                "artifact_rows_added_during_concurrent_stream_service": artifact_rows_added_during_report,
                "stream_service_calls_during_report_export": report_service_calls,
                "wal_bytes_after_first_publication": wal_after_first_publication,
                "wal_bytes_after_mixed_ingestion": wal_after_mixed_ingestion,
                "collector_archive_flush_pending_rows": archive_flush_pending_counts,
                "collector_archive_flush_max_rows": max(archive_flush_pending_counts),
                "collector_archive_flush_durations_seconds": archive_flush_durations_seconds,
                "collector_archive_flush_max_seconds": max(archive_flush_durations_seconds),
                "runtime_service_call_durations_ns": runtime_service_samples,
                "runtime_persistence_samples_over_100ms": runtime_persistence_samples,
                "runtime_stage_samples": runtime_stage_samples,
                "runtime_interpreter_batches": interpreter_samples,
                "runtime_archive_writes": archive_write_samples,
                "capital_enabled": False,
                "assisted_enabled": False,
                "qualification_limits": [
                    "Synthetic source rows and deterministic local stream frames; no venue network",
                    "Eight synthetic durable DETECTED watches seed the bounded deep cohort; this is not live prevalence",
                    "Benchmark runtime plan and workset selection do not represent live eligibility prevalence",
                    "One bounded acquisition and one report-overlap stream interval are not endurance or full broad qualification",
                    "Synthetic persistence and archive stalls represent one bounded overlap, not live venue behaviour",
                ],
            }
            record_property("session041_mixed_capacity", json.dumps(metrics, sort_keys=True))
            print("SESSION041_MIXED_BROAD_CAPACITY=" + json.dumps(metrics, sort_keys=True))
            assert metrics["capacity_qualified"] is False
            assert metrics["capital_enabled"] is False
            assert metrics["assisted_enabled"] is False
            assert metrics["capture_batch_frame_limit"] == 16
            assert metrics["max_interpreted_frames_per_batch"] <= 64
            # The SLO preserves at least 272 of 512 queue slots at the declared
            # 160-frame/s sustained rate for bursts and scheduling variance.
            assert final.max_service_gap_ns <= 1_500_000_000, json.dumps(metrics, sort_keys=True)
            assert metrics["report_worker_status"] == "IMPLEMENTED", json.dumps(metrics, sort_keys=True)
            assert metrics["report_worker_error_type"] is None
            assert metrics["report_validation_failures"] == {}

        # Recovery fencing intentionally allows one active DB writer. Close the
        # ingestion writer above before opening the replacement writer here.
        restarted_runtime = BroadPublicRuntimeV2(
            clock_ns=lambda: NOW + 30_000_000_000,
            stream_factories={"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()})
        try:
            with OpsRepository(path) as restarted:
                restart_watches = restarted.list_active_watches(limit=32)
                restart_watch_keys = tuple(watch.key for watch in restart_watches)
                restart_tiers = {
                    InstrumentKeyV2.from_dict(json.loads(key)): int(tier)
                    for key, tier in body["tiers"].items()
                }
                restart_benchmarks = tuple(product.key for product in products
                    if product.key.native_symbol in {"BTCUSDT", "ETHUSDT"})
                restart_factories = {"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()}
                if any(key.venue == VenueV2.BINANCE for key in restart_watch_keys):
                    restart_factories["BINANCE_DEPTH"] = _idle_stream()
                restarted_runtime.stream_factories = restart_factories
                restarted_runtime.recover(restarted, run_root=tmp_path, products=products,
                    tiers=restart_tiers, now_ns=NOW + 30_000_000_000,
                    benchmark_keys=restart_benchmarks, active_watch_keys=restart_watch_keys)
                restarted_body = latest_workset(restarted, cutoff_ns=NOW + 30_000_000_000)
                assert restarted_body is not None
                assert canonical_json(restarted_body) == canonical_json(body)
                assert restarted_runtime.plan is not None
                assert runtime.plan is not None
                assert restarted_runtime.plan.keys == runtime.plan.keys
                assert restarted_runtime.plan.identities == runtime.plan.identities
                assert restarted_runtime.status().capture["terminal_error"] is None
                restarted_runtime.finish(restarted)
        finally:
            restarted_runtime.close()
    finally:
        producer_stop.set()
        if not exporter.close(timeout_s=30.0):
            raise AssertionError("read-only report worker did not stop")
        port.close()
