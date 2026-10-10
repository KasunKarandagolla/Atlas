"""Short, attributable S41 recovery capacity checkpoints.

These checkpoints are engineering probes only. They never create a capacity
admission certificate or assert long-run endurance.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from atlas.v2._serialization import sha256_json  # noqa: E402
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2  # noqa: E402
from atlas.v2.instruments import (  # noqa: E402
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository  # noqa: E402
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2  # noqa: E402

FROZEN_QUEUE_ITEMS = 512
FROZEN_QUEUE_BYTES = 16_000_000
FROZEN_SERVICE_GAP_NS = 1_500_000_000
FROZEN_CAPTURE_BATCH_FRAMES = 16
F1_SECONDS = 60
F1_MICRO_SECONDS = 12
F1_LANE_RATE = 80
F1_BURST_LANE_RATE = 160
F1_BURST_START_SECONDS = 30
F1_MICRO_BURST_START_SECONDS = 6


def _idle_stream():
    async def stream():
        await asyncio.Event().wait()
        if False:
            yield None
    return stream


def _products(now_ns: int) -> tuple[ProductContractV2, ProductContractV2]:
    result = []
    for venue in (VenueV2.BYBIT, VenueV2.BINANCE):
        symbol = "BTCUSDT"
        revision = sha256_json({"venue": venue.value, "symbol": symbol, "revision": 1})
        key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
            symbol, "BTC", "USDT", "USDT", revision)
        result.append(ProductContractV2(key, now_ns, now_ns, now_ns, Decimal("1"),
            Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING,
            revision))
    return tuple(result)  # type: ignore[return-value]


def _frame(venue: VenueV2, index: int, received_at_ns: int) -> CapturedPublicFrameV2:
    at_ms = received_at_ns // 1_000_000
    if venue == VenueV2.BYBIT:
        source_id = "BYBIT_PUBLIC_WS_BROAD_V2"
        channel = "publicTrade.BTCUSDT"
        body = {"topic": channel, "type": "snapshot", "ts": at_ms,
                "data": [{"T": at_ms, "s": "BTCUSDT", "S": "Buy", "v": "0.01",
                          "p": "100", "i": f"f1-bybit-{index}"}]}
    else:
        source_id = "BINANCE_MARKET_PUBLIC_WS_BROAD_V2"
        channel = "btcusdt@aggTrade"
        body = {"stream": channel, "data": {"e": "aggTrade", "E": at_ms,
                "s": "BTCUSDT", "a": index, "p": "100", "q": "0.01",
                "f": index, "l": index, "T": at_ms, "m": True}}
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    return CapturedPublicFrameV2(venue, source_id, channel, raw, hashlib.sha256(raw).hexdigest(),
                                 received_at_ns, received_at_ns, 1)


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)]


def _slope(samples: list[tuple[float, int]]) -> float | None:
    if len(samples) < 3:
        return None
    mean_x = statistics.fmean(x for x, _ in samples)
    mean_y = statistics.fmean(y for _, y in samples)
    denominator = sum((x - mean_x) ** 2 for x, _ in samples)
    if denominator == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in samples) / denominator


def _maximum_window_count(values: list[float], width_seconds: float) -> int:
    """Return the greatest event count in any half-open window of this width."""
    ordered = sorted(values)
    left = best = 0
    for right, value in enumerate(ordered):
        while value - ordered[left] >= width_seconds:
            left += 1
        best = max(best, right - left + 1)
    return best


def _burst_recovery(
    samples: list[tuple[float, int]], *, burst_start_seconds: float,
    recovery_seconds: float = 20.0, baseline_seconds: float = 5.0,
) -> dict[str, Any]:
    """Require a measured one-second burst to drain back to its quiet baseline."""
    baseline_samples = [pending for elapsed, pending in samples
                        if burst_start_seconds - baseline_seconds <= elapsed < burst_start_seconds]
    baseline = max(baseline_samples, default=None)
    deadline = burst_start_seconds + 1 + recovery_seconds
    recovered_at = next((elapsed for elapsed, pending in samples
                         if elapsed >= burst_start_seconds + 1 and elapsed <= deadline
                         and baseline is not None and pending <= baseline), None)
    return {
        "baseline_window_seconds": [burst_start_seconds - baseline_seconds, burst_start_seconds],
        "baseline_max_pending_frames": baseline,
        "baseline_max_allowed_frames": FROZEN_CAPTURE_BATCH_FRAMES,
        "deadline_elapsed_seconds": deadline,
        "recovery_seconds_after_burst": recovery_seconds,
        "recovered_at_elapsed_seconds": recovered_at,
        "recovered_to_preburst_baseline": (baseline is not None and baseline <= FROZEN_CAPTURE_BATCH_FRAMES
                                             and recovered_at is not None),
    }


def _git_sha(root: Path) -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
        capture_output=True, text=True).stdout.strip()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _proc_cpu_ns(pid: int | None) -> int | None:
    """Read per-process user+system CPU without adding a benchmark dependency."""
    if pid is None or not Path(f"/proc/{pid}/stat").is_file():
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks * 1_000_000_000 // int(os.sysconf("SC_CLK_TCK"))
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def _thread_cpu_ns(native_id: int | None) -> int | None:
    """Read one thread's user+system CPU on Linux; other hosts report unavailable."""
    if native_id is None or not Path(f"/proc/self/task/{native_id}/stat").is_file():
        return None
    try:
        fields = Path(f"/proc/self/task/{native_id}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks * 1_000_000_000 // int(os.sysconf("SC_CLK_TCK"))
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def _source_files(root: Path) -> dict[str, str]:
    names = (
        "src/atlas/v2/data/public_microstructure_ws.py",
        "src/atlas/v2/data/durable_public_capture.py",
        "src/atlas/v2/data/public_archive_extents.py",
        "src/atlas/v2/data/microstructure_archive.py",
        "src/atlas/v2/data/public_evidence_preparation.py",
        "src/atlas/v2/data/public_stream_continuity.py",
        "src/atlas/v2/data/public_stream_evidence_v1.py",
        "src/atlas/v2/data/broad_stream_source.py",
        "src/atlas/v2/runtime/broad_public_runtime.py",
        "src/atlas/v2/memory/repository.py",
        "src/atlas/v2/memory/schema.py",
        "tests/v2/test_session041_public_runtime_integration.py",
        "tests/v2/test_session041_recovery_adoption.py",
        "tests/v2/test_session032_public_stream_continuity.py",
        "tests/v2/test_session038_stream_identity_cache.py",
        "scripts/session041_recovery_capacity.py",
        "pyproject.toml", "uv.lock",
    )
    return {name: _file_hash(root / name) for name in names if (root / name).is_file()}


def _working_tree_source_hash(root: Path) -> str:
    tracked = subprocess.run(["git", "diff", "--binary", "HEAD", "--"], cwd=root,
        check=True, capture_output=True).stdout
    untracked = subprocess.run(["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=root, check=True, capture_output=True).stdout.split(b"\0")
    digest = hashlib.sha256()
    digest.update(tracked)
    for encoded in sorted(path for path in untracked if path):
        relative = encoded.decode("utf-8", errors="strict")
        path = root / relative
        if not path.is_file():
            continue
        digest.update(b"\0UNTRACKED\0")
        digest.update(encoded)
        digest.update(bytes.fromhex(_file_hash(path)))
    return digest.hexdigest()


def _f1_profile(*, checkpoint: str = "F1", seconds: int = F1_SECONDS,
                 burst_at: int = F1_BURST_START_SECONDS) -> dict[str, Any]:
    if checkpoint == "F1-micro" and (seconds != F1_MICRO_SECONDS
                                      or burst_at != F1_MICRO_BURST_START_SECONDS):
        raise ValueError("source-approved F1-micro remains fixed at 12 seconds with a burst at 6")
    return {"schema_version": 1, "checkpoint": checkpoint, "duration_seconds": seconds,
        "venues": ["BYBIT", "BINANCE"],
        "sustained_frames_per_second": 160, "lane_frames_per_second": 80,
        "burst_frames_per_second": 320, "burst_seconds": 1, "burst_start_seconds": burst_at,
        "queue_items": FROZEN_QUEUE_ITEMS, "queue_bytes": FROZEN_QUEUE_BYTES,
        "service_gap_ns": FROZEN_SERVICE_GAP_NS, "capture_batch_frames": 16,
        "trade_observations_per_frame": 1}


def _f1(root: Path, report_root: Path, output: Path, *, checkpoint: str = "F1",
        seconds: int = F1_SECONDS, burst_at: int = F1_BURST_START_SECONDS) -> dict[str, Any]:
    if (checkpoint not in {"F1", "F1-micro"} or type(seconds) is not int or seconds < 1
            or type(burst_at) is not int or not 5 <= burst_at < seconds - 5):
        raise ValueError("F1 measurement window or burst placement is outside the supported bounded profile")
    if checkpoint == "F1" and (seconds != F1_SECONDS or burst_at != F1_BURST_START_SECONDS):
        raise ValueError("the full F1 checkpoint remains fixed at 60 seconds with a burst at 30")
    if checkpoint == "F1-micro" and (seconds != F1_MICRO_SECONDS
                                      or burst_at != F1_MICRO_BURST_START_SECONDS):
        raise ValueError("source-approved F1-micro remains fixed at 12 seconds with a burst at 6")
    now_ns = time.time_ns()
    products = _products(now_ns)
    keys = tuple(item.key for item in products)
    db_path = report_root / "ops.sqlite"
    # Capture lifecycle and public extents share the repository parent path;
    # a nested fixture root would make durable bytes unreadable by adoption.
    run_root = report_root
    if report_root.exists() and any(report_root.iterdir()):
        raise ValueError("capacity checkpoint root must be new or empty")
    report_root.mkdir(parents=True, exist_ok=True)
    runtime = BroadPublicRuntimeV2(stream_factories={
        "BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream(),
    })
    offered = {VenueV2.BYBIT: 0, VenueV2.BINANCE: 0}
    offered_at_seconds: dict[VenueV2, list[float]] = {
        VenueV2.BYBIT: [], VenueV2.BINANCE: [],
    }
    offered_bytes: dict[VenueV2, list[int]] = {VenueV2.BYBIT: [], VenueV2.BINANCE: []}
    schedule_error_seconds: dict[VenueV2, list[float]] = {VenueV2.BYBIT: [], VenueV2.BINANCE: []}
    rejected: list[dict[str, Any]] = []
    producer_stop = threading.Event()
    interpreter_ns: list[int] = []
    interpreter_batches: list[tuple[int, int]] = []
    service_ns: list[int] = []
    service_gaps_ns: list[int] = []
    samples: list[dict[str, Any]] = []
    pending_series: list[tuple[float, int]] = []
    last_service_start: int | None = None
    processed_before_stop = 0
    failure: str | None = None
    production_started: float | None = None

    def start_producer(venue: VenueV2) -> None:
        def produce() -> None:
            index = 0
            if production_started is None:
                raise RuntimeError("producer epoch was not initialized")
            start = production_started
            while not producer_stop.is_set():
                elapsed = time.monotonic() - start
                extra = min(max(elapsed - burst_at, 0.0), 1.0)
                target = int(F1_LANE_RATE * elapsed + F1_LANE_RATE * extra)
                while index < target and not producer_stop.is_set():
                    actual_offer_at = time.monotonic()
                    received_at = time.time_ns()
                    frame = _frame(venue, index, received_at)
                    source = runtime.source
                    lane = source._lanes["BYBIT" if venue == VenueV2.BYBIT else "BINANCE_MARKET"] if source else None
                    if lane is None or not lane._handoff.offer(frame):
                        status = runtime.status()
                        rejected.append({"venue": venue.value, "index": index,
                            "queue_items": status.handoff.queue_items,
                            "queue_bytes": status.handoff.queue_bytes,
                            "loss_latched": getattr(status.handoff, "loss_latched", None),
                            "overflowed": status.handoff.overflowed})
                        producer_stop.set()
                        return
                    offered[venue] += 1
                    offered_bytes[venue].append(len(frame.raw_payload_bytes))
                    actual_elapsed = actual_offer_at - start
                    offered_at_seconds[venue].append(actual_elapsed)
                    before_burst = F1_LANE_RATE * burst_at
                    during_burst_end = before_burst + F1_BURST_LANE_RATE
                    if index < before_burst:
                        expected_elapsed = (index + 1) / F1_LANE_RATE
                    elif index < during_burst_end:
                        expected_elapsed = burst_at + (index - before_burst + 1) / F1_BURST_LANE_RATE
                    else:
                        expected_elapsed = (burst_at + 1
                            + (index - during_burst_end + 1) / F1_LANE_RATE)
                    schedule_error_seconds[venue].append(actual_elapsed - expected_elapsed)
                    index += 1
                base_before_burst = F1_LANE_RATE * burst_at
                base_after_burst = base_before_burst + F1_BURST_LANE_RATE
                if index < base_before_burst:
                    target_time = start + (index + 1) / F1_LANE_RATE
                elif index < base_after_burst:
                    target_time = (start + burst_at
                                   + (index - base_before_burst + 1) / F1_BURST_LANE_RATE)
                else:
                    target_time = (start + burst_at + 1
                                   + (index - base_after_burst + 1) / F1_LANE_RATE)
                producer_stop.wait(max(0.0, min(0.005, target_time - time.monotonic())))
        thread = threading.Thread(target=produce, name=f"s41-f1-{venue.value.lower()}", daemon=True)
        thread.start()
        producers.append(thread)

    producers: list[threading.Thread] = []
    run_root.mkdir(parents=True, exist_ok=True)
    with OpsRepository(db_path) as repository:
        runtime.recover(repository, run_root=run_root, products=products, tiers={}, now_ns=now_ns,
                        benchmark_keys=keys)
        writer_thread_id = threading.get_native_id()
        capture_impl = runtime.capture._capture if runtime.capture is not None else None
        capture_thread_id = (capture_impl._thread.native_id if capture_impl is not None
                             and capture_impl._thread is not None else None)
        worker_process = (runtime._preparation_worker._process if runtime._preparation_worker is not None else None)
        writer_cpu_start = _thread_cpu_ns(writer_thread_id)
        capture_cpu_start = _thread_cpu_ns(capture_thread_id)
        worker_cpu_start = _proc_cpu_ns(worker_process.pid if worker_process is not None else None)
        original_interpreter = runtime._interpret_frames

        def observe_interpreter(repository: OpsRepository, frames: tuple[Any, ...], *, now_ns: int) -> Any:
            started = time.monotonic_ns()
            try:
                return original_interpreter(repository, frames, now_ns=now_ns)
            finally:
                duration_ns = time.monotonic_ns() - started
                interpreter_ns.append(duration_ns)
                interpreter_batches.append((duration_ns, len(frames)))

        runtime._interpret_frames = observe_interpreter  # type: ignore[method-assign]
        started = time.monotonic()
        started_ns = time.monotonic_ns()
        production_started = started
        start_producer(VenueV2.BYBIT)
        start_producer(VenueV2.BINANCE)
        next_sample = started
        while time.monotonic() - started < seconds and not producer_stop.is_set():
            service_start = time.monotonic_ns()
            if last_service_start is not None:
                service_gaps_ns.append(service_start - last_service_start)
            last_service_start = service_start
            try:
                runtime.service(repository, now_ns=time.time_ns())
            except Exception as exc:
                failure = f"{type(exc).__name__}:{exc}"
                producer_stop.set()
                break
            service_ns.append(time.monotonic_ns() - service_start)
            if runtime._terminal_error:
                failure = runtime._terminal_error
                producer_stop.set()
                break
            at = time.monotonic()
            if at >= next_sample:
                status = runtime.status()
                sample_ns = time.monotonic_ns()
                elapsed_ns = sample_ns - started_ns
                accepted_at_sample = sum(
                    1 for lane_times in offered_at_seconds.values()
                    for offered_at in lane_times if offered_at * 1_000_000_000 <= elapsed_ns
                )
                indexed_at_sample = sum(count for completed_at, count in runtime._descriptor_completion_events
                                        if completed_at <= sample_ns)
                pending = accepted_at_sample - indexed_at_sample
                pending_series.append((at - started, pending))
                metrics = repository.persistence_metrics()
                lane_samples = {
                    name: {
                        "queue_items": int(lane.handoff.queue_items),
                        "queue_bytes": int(lane.handoff.queue_bytes),
                        "high_water_items": int(lane.handoff.high_water_items),
                        "high_water_bytes": int(lane.handoff.high_water_bytes),
                        "frames_received": int(lane.handoff.frames_received),
                        "frames_drained": int(lane.handoff.frames_drained),
                        "frames_rejected": int(lane.handoff.frames_rejected),
                        "overflowed": bool(lane.handoff.overflowed),
                    }
                    for name, lane in status.lanes.items()
                }
                samples.append({"elapsed_seconds": round(at - started, 3),
                    "sample_monotonic_ns": sample_ns,
                    "accepted_frames_by_sample": accepted_at_sample,
                    "observation_qualified_indexed_frames_by_sample": indexed_at_sample,
                    "pending_frames": pending,
                    "handoff_items": int(status.handoff.queue_items),
                    "handoff_bytes": int(status.handoff.queue_bytes),
                    "lane_occupancy": lane_samples,
                    "capture_pending_frames": int(status.capture["pending_frames"]),
                    "capture_in_progress_frames": int(status.capture.get("capture_in_progress_frames", 0)),
                    "capture_batches": int(status.capture["pending_batches"]),
                    "captured_frames": int(status.capture["captured_frames"]),
                    "delivered_frames": int(status.capture["delivered_frames"]),
                    "indexed_frames": int(runtime._service_frames),
                    "service_calls": int(runtime._service_calls),
                    "service_last_duration_ns": int(runtime._last_service_duration_ns),
                    "service_max_duration_ns": int(runtime._max_service_duration_ns),
                    "service_max_gap_ns": int(runtime._max_service_gap_ns),
                    "phase_timings_ns": status.phase_timings_ns,
                    "capture_archive_metrics": dict(status.capture.get("archive", {})),
                    "capture_max_duration_ns": int(status.capture.get("max_capture_duration_ns", 0)),
                    "capture_receipt_bytes": int(status.capture.get("receipt_bytes_written", 0)),
                    "persistence": metrics})
                next_sample += 1.0
            time.sleep(0.0005)
        elapsed = max(0.0, time.monotonic() - started)
        producer_end_ns = time.monotonic_ns()
        elapsed = max(0.0, (producer_end_ns - started_ns) / 1_000_000_000)
        producer_stop.set()
        for thread in producers:
            thread.join(timeout=2.0)
        accepted_at_end = sum(1 for lane_times in offered_at_seconds.values()
                              for offered_at in lane_times if offered_at < elapsed)
        processed_before_stop = sum(count for completed_at, count in runtime._descriptor_completion_events
                                    if completed_at <= producer_end_ns)
        producer_capture_sealed = (runtime.capture.captured_frames_at_monotonic_ns(producer_end_ns)
                                   if runtime.capture is not None else 0)
        producer_window_pending = accepted_at_end - processed_before_stop
        if not pending_series or abs(pending_series[-1][0] - elapsed) > 0.05:
            pending_series.append((elapsed, producer_window_pending))
        producer_end_status = runtime.status()
        samples.append({"elapsed_seconds": round(elapsed, 3), "sample_monotonic_ns": producer_end_ns,
            "producer_window_end": True, "accepted_frames_by_sample": accepted_at_end,
            "observation_qualified_indexed_frames_by_sample": processed_before_stop,
            "capture_sealed_frames_by_sample": producer_capture_sealed,
            "pending_frames": producer_window_pending,
            "capture_pending_frames_observed_after_cutoff": int(producer_end_status.capture["pending_frames"]),
            "capture_in_progress_frames_observed_after_cutoff": int(
                producer_end_status.capture.get("capture_in_progress_frames", 0)),
            "indexed_frames": int(runtime._service_frames),
            "service_max_gap_ns": int(runtime._max_service_gap_ns),
            "phase_timings_ns": producer_end_status.phase_timings_ns,
            "capture_archive_metrics": dict(producer_end_status.capture.get("archive", {})),
            "capture_max_duration_ns": int(producer_end_status.capture.get("max_capture_duration_ns", 0)),
            "capture_receipt_bytes": int(producer_end_status.capture.get("receipt_bytes_written", 0))})
        writer_cpu_end = _thread_cpu_ns(writer_thread_id)
        capture_cpu_end = _thread_cpu_ns(capture_thread_id)
        worker_cpu_end = _proc_cpu_ns(worker_process.pid if worker_process is not None else None)
        status = runtime.status()
        try:
            if failure is None and not rejected:
                runtime.finish(repository)
            else:
                runtime.close()
        except Exception as exc:
            failure = failure or f"{type(exc).__name__}:{exc}"
        final_status = runtime.status()
        captured = int(final_status.capture["captured_frames"])
        delivered = int(final_status.capture["delivered_frames"])
        lost = int(final_status.handoff.frames_rejected)
        processed = int(runtime._service_frames)
        full_batches_ns = [duration for duration, frame_count in interpreter_batches if frame_count == 16]
        mean_16_frame_ns = int(statistics.fmean(full_batches_ns)) if full_batches_ns else None
        tail = [(elapsed_s, pending) for elapsed_s, pending in pending_series if elapsed_s >= max(0, elapsed - 20)]
        burst_window_counts = {
            venue.value: _maximum_window_count(
                [at for at in offered_at_seconds[venue] if burst_at <= at < burst_at + 1], 1.0
            ) for venue in offered_at_seconds
        }
        sustained_by_lane = {
            venue.value: {
                "preburst_fps": sum(1 for at in offered_at_seconds[venue] if 0 <= at < burst_at) / burst_at,
                "postburst_fps": sum(1 for at in offered_at_seconds[venue] if burst_at + 1 <= at < elapsed)
                    / max(0.001, elapsed - burst_at - 1),
            } for venue in offered_at_seconds
        }
        recovery_limit = 5.0 if checkpoint == "F1-micro" else 20.0
        burst_recovery = _burst_recovery(
            pending_series, burst_start_seconds=burst_at, recovery_seconds=recovery_limit,
            baseline_seconds=3.0 if checkpoint == "F1-micro" else 5.0)
        max_handoff_items = max((sample["handoff_items"] for sample in samples), default=0)
        max_handoff_bytes = max((sample["handoff_bytes"] for sample in samples), default=0)
        baseline = burst_recovery["baseline_max_pending_frames"]
        backlogged_samples = [sample for sample in samples
            if burst_at <= sample["elapsed_seconds"] <= burst_at + 1 + recovery_limit
            and sample["pending_frames"] > 0]
        backlogged_rate = None
        backlogged_duration = 0.0
        if len(backlogged_samples) >= 2:
            first, last = backlogged_samples[0], backlogged_samples[-1]
            backlogged_duration = float(last["elapsed_seconds"] - first["elapsed_seconds"])
            if backlogged_duration > 0:
                backlogged_rate = (
                    int(last["observation_qualified_indexed_frames_by_sample"])
                    - int(first["observation_qualified_indexed_frames_by_sample"])
                ) / backlogged_duration
        full_descriptor_cycles = [duration for duration, count in zip(
            runtime._descriptor_cycle_ns, runtime._descriptor_cycle_frames, strict=True) if count == 16]
        mean_full_descriptor_cycle_ns = (int(statistics.fmean(full_descriptor_cycles))
                                         if full_descriptor_cycles else None)
        producer_window_capture_rate = producer_capture_sealed / elapsed if elapsed else 0.0
        producer_window_indexed_rate = processed_before_stop / elapsed if elapsed else 0.0
        queue_and_loss_ok = (not rejected and lost == 0 and max_handoff_items <= FROZEN_QUEUE_ITEMS
                             and max_handoff_bytes <= FROZEN_QUEUE_BYTES
                             and max(service_gaps_ns, default=0) <= FROZEN_SERVICE_GAP_NS)
        micro_pass = (failure is None and queue_and_loss_ok and elapsed >= seconds - 1.0
            and producer_window_capture_rate >= 240 and producer_window_indexed_rate >= 160
            and backlogged_rate is not None and backlogged_rate >= 240 and backlogged_duration >= 1.0
            and mean_full_descriptor_cycle_ns is not None and mean_full_descriptor_cycle_ns <= 66_700_000
            and burst_recovery["recovered_to_preburst_baseline"]
            and baseline is not None and baseline <= 16
            and producer_window_pending >= 0
            and int(final_status.capture["pending_frames"]) + int(final_status.handoff.queue_items) == 0
            and sum(offered.values()) == captured == delivered == processed)
        f1_pass = (failure is None and not rejected and lost == 0 and
            sum(offered.values()) == captured == delivered == processed and
            (sum(offered.values()) / elapsed >= 160 if elapsed else False) and
            producer_window_indexed_rate >= 160 and
            all(76 <= rates["preburst_fps"] <= 84 and 76 <= rates["postburst_fps"] <= 84
                for rates in sustained_by_lane.values()) and
            burst_window_counts[VenueV2.BYBIT.value] >= 152 and
            burst_window_counts[VenueV2.BINANCE.value] >= 152 and
            sum(burst_window_counts.values()) >= 304 and
            burst_window_counts[VenueV2.BYBIT.value] <= 168 and
            burst_window_counts[VenueV2.BINANCE.value] <= 168 and
            sum(burst_window_counts.values()) <= 336 and
            burst_recovery["recovered_to_preburst_baseline"] and
            max_handoff_items <= FROZEN_QUEUE_ITEMS and
            max_handoff_bytes <= FROZEN_QUEUE_BYTES and
            max(service_gaps_ns, default=0) <= FROZEN_SERVICE_GAP_NS and
            len(samples) >= seconds - 2 and
            mean_16_frame_ns is not None and mean_16_frame_ns <= 100_000_000 and
            (not tail or (_slope(tail) is not None and _slope(tail) <= 0)))
        result = {
            "schema_version": 1,
            "checkpoint": checkpoint,
            "scope": "SHORT_SYNTHETIC_ENGINEERING_PROBE",
            "capacity_certificate": False,
            "base_commit": _git_sha(root),
            "working_tree_source_hash": _working_tree_source_hash(root),
            "profile": _f1_profile(checkpoint=checkpoint, seconds=seconds, burst_at=burst_at),
            "profile_hash": sha256_json(_f1_profile(checkpoint=checkpoint, seconds=seconds, burst_at=burst_at)),
            "working_tree_source_hashes": {
                **_source_files(root),
            },
            "host": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                     "device_id": os.stat(report_root).st_dev,
                     "free_bytes_at_end": shutil.disk_usage(report_root).free},
            "workload": {"duration_seconds": elapsed, "target_seconds": seconds,
                "sustained_total_fps": 2 * F1_LANE_RATE, "burst_total_fps": 2 * F1_BURST_LANE_RATE,
                "burst_seconds": 1, "burst_start_seconds": burst_at,
                "queue_items_limit": FROZEN_QUEUE_ITEMS, "queue_bytes_limit": FROZEN_QUEUE_BYTES,
                "service_gap_limit_ns": FROZEN_SERVICE_GAP_NS,
                "frame_bytes_by_lane": {
                    venue.value: {"count": len(values), "minimum": min(values, default=0),
                        "maximum": max(values, default=0),
                        "mean": statistics.fmean(values) if values else 0}
                    for venue, values in offered_bytes.items()
                },
            },
            "arrivals": {"offered_by_lane": {venue.value: count for venue, count in offered.items()},
                "offered_frames": sum(offered.values()), "captured_frames": captured,
                "capture_sealed_frames_at_producer_end": producer_capture_sealed,
                "descriptors_dequeued_frames": delivered, "indexed_frames": processed,
                "indexed_frames_at_stop": processed_before_stop,
                "producer_window_seconds": elapsed,
                "capture_sealed_per_second_during_producer_window": producer_window_capture_rate,
                "observation_qualified_indexed_per_second_during_producer_window": producer_window_indexed_rate,
                "trade_observations_per_second": processed_before_stop / elapsed if elapsed else 0,
                "measured_sustained_frames_per_second_by_lane": sustained_by_lane,
                "measured_peak_one_second_burst_by_lane": burst_window_counts,
                "measured_peak_one_second_burst_aggregate": sum(burst_window_counts.values()),
                "rejected_frames": lost, "rejections": rejected,
                "capture_terminal_error": final_status.capture["terminal_error"],
                "runtime_error": failure},
            "timing_ns": {"service_mean": int(statistics.fmean(service_ns)) if service_ns else 0,
                "service_p95": _percentile(service_ns, .95), "service_max": max(service_ns, default=0),
                "service_gap_max": max(service_gaps_ns, default=0),
                "interpreter_mean": int(statistics.fmean(interpreter_ns)) if interpreter_ns else 0,
                "interpreter_p95": _percentile(interpreter_ns, .95),
                "interpreter_max": max(interpreter_ns, default=0),
                "interpreter_batch_count": len(interpreter_batches),
                "interpreter_16_frame_batch_count": len(full_batches_ns),
                "mean_16_frame_processing_ns": mean_16_frame_ns,
                "complete_descriptor_cycle_count": len(runtime._descriptor_cycle_ns),
                "complete_16_frame_descriptor_cycle_mean_ns": mean_full_descriptor_cycle_ns,
                "complete_descriptor_cycle_ns": list(runtime._descriptor_cycle_ns),
                "complete_descriptor_frame_counts": list(runtime._descriptor_cycle_frames),
                "observation_qualified_rate_while_backlogged_fps": backlogged_rate,
                "backlogged_measurement_seconds": backlogged_duration},
            "cpu_ns": {"writer_thread": (writer_cpu_end - writer_cpu_start
                           if writer_cpu_end is not None and writer_cpu_start is not None else None),
                "capture_thread": (capture_cpu_end - capture_cpu_start
                           if capture_cpu_end is not None and capture_cpu_start is not None else None),
                "preparation_worker_process": (worker_cpu_end - worker_cpu_start
                           if worker_cpu_end is not None and worker_cpu_start is not None else None),
                "measurement_method": "LINUX_PROCFS_THREAD_AND_PROCESS_TICKS"},
            "producer_window": {"start_monotonic_ns": started_ns, "end_monotonic_ns": producer_end_ns,
                "accepted_frames": accepted_at_end, "capture_sealed_frames": producer_capture_sealed,
                "observation_qualified_indexed_frames": processed_before_stop,
                "accepted_not_observation_qualified_frames": producer_window_pending,
                "actual_offer_timestamps_seconds_by_lane": {
                    venue.value: list(offered_at_seconds[venue]) for venue in offered_at_seconds},
                "sample_count": len(samples)},
            "workload_binding": {"fixture_recipe_hash": sha256_json({"profile": _f1_profile(
                    checkpoint=checkpoint, seconds=seconds, burst_at=burst_at),
                    "fixture": "one exact trade row per frame; Bybit publicTrade; Binance aggTrade; canonical JSON"}),
                "producer_schedule": "two independent monotonic 80fps lanes; each 160fps during the configured 1s burst",
                "schedule_error_seconds_by_lane": {
                    venue.value: {"count": len(values), "mean": statistics.fmean(values) if values else 0.0,
                        "maximum_positive": max(values, default=0.0),
                        "maximum_negative": min(values, default=0.0),
                        "catch_up_gaps_over_5ms": sum(1 for value in values if value > 0.005)}
                    for venue, values in schedule_error_seconds.items()
                },
                "root_device_id": os.stat(report_root).st_dev},
            "phase_timings_ns": final_status.phase_timings_ns,
            "backlog": {"samples": pending_series,
                "measured_one_second_burst_recovery": burst_recovery,
                "tail_20_second_least_squares_frames_per_second": _slope(tail),
                "max_sampled_handoff_items": max_handoff_items,
                "max_sampled_handoff_bytes": max_handoff_bytes,
                "final_pending_frames": int(final_status.capture["pending_frames"])
                    + int(final_status.handoff.queue_items),
                "zero_after_bounded_drain": (failure is None and
                    int(final_status.capture["pending_frames"]) == 0 and
                    int(final_status.handoff.queue_items) == 0)},
            "resource_and_storage_samples": samples,
            "sqlite_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "wal_bytes": db_path.with_name(db_path.name + "-wal").stat().st_size
                if db_path.with_name(db_path.name + "-wal").exists() else 0,
            "micro_gates": {"capture_rate_240_fps": producer_window_capture_rate >= 240,
                "backlogged_index_rate_240_fps": backlogged_rate is not None and backlogged_rate >= 240,
                "mean_complete_descriptor_66_7ms": (mean_full_descriptor_cycle_ns is not None
                    and mean_full_descriptor_cycle_ns <= 66_700_000),
                "service_gap_1_5s": max(service_gaps_ns, default=0) <= FROZEN_SERVICE_GAP_NS,
                "zero_loss_or_reject": lost == 0 and not rejected,
                "preburst_backlog_16": baseline is not None and baseline <= 16,
                "burst_recovery_5s": burst_recovery["recovered_to_preburst_baseline"],
                "bounded_final_drain_zero": (int(final_status.capture["pending_frames"])
                    + int(final_status.handoff.queue_items) == 0)},
            "passed": micro_pass if checkpoint == "F1-micro" else f1_pass,
            "limits_unchanged": True,
            "claim": "This short synthetic checkpoint does not qualify a host, profile, venue source, or 48-hour endurance.",
        }
    output.parent.mkdir(parents=True, exist_ok=True)
    result["report_content_sha256"] = sha256_json(result)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    os.replace(temporary, output)
    return result


F1_PRESEALED_DESCRIPTOR_COUNT = 4


def _f1_presealed_profile() -> dict[str, Any]:
    return {"schema_version": 1, "checkpoint": "F1-presealed",
        "descriptors": F1_PRESEALED_DESCRIPTOR_COUNT, "frames_per_descriptor": 16,
        "venues": ["BYBIT", "BINANCE"], "trade_observations_per_frame": 1,
        "queue_items": FROZEN_QUEUE_ITEMS, "queue_bytes": FROZEN_QUEUE_BYTES,
        "service_gap_ns": FROZEN_SERVICE_GAP_NS,
        "durability": "CAPTURE_EXTENT_AND_RECEIPT_FSYNC_SQLITE_WAL_FULL_POST_COMMIT_OBSERVATION",
        "measurement": "capture_presealed_to_observation_qualified_publication"}


def _phase_delta(before: dict[str, dict[str, int]], after: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    return {phase: {name: after[phase][name] - before[phase][name]
                    for name in ("count", "sum_ns")}
            | {"max_ns": after[phase]["max_ns"]}
            for phase in after}


def _f1_presealed(root: Path, report_root: Path, output: Path) -> dict[str, Any]:
    """Time complete 16-frame descriptors after capture extent/receipt are durable."""
    if report_root.exists() and any(report_root.iterdir()):
        raise ValueError("presealed checkpoint root must be new or empty")
    report_root.mkdir(parents=True, exist_ok=True)
    now_ns = time.time_ns()
    products = _products(now_ns)
    runtime = BroadPublicRuntimeV2(stream_factories={
        "BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream(),
    })
    expected_raw: dict[str, bytes] = {}
    cycles: list[dict[str, Any]] = []
    failure: str | None = None
    db_path = report_root / "ops.sqlite"
    try:
        with OpsRepository(db_path) as repository:
            runtime.recover(repository, run_root=report_root, products=products, tiers={}, now_ns=now_ns,
                            benchmark_keys=tuple(item.key for item in products))
            if runtime.source is None or runtime.capture is None:
                raise RuntimeError("F1_PRESEALED_RUNTIME_NOT_STARTED")
            journal_mode = str(repository._connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            synchronous = int(repository._connection.execute("PRAGMA synchronous").fetchone()[0])
            offered_by_lane = {"BYBIT": 0, "BINANCE_MARKET": 0}
            for descriptor_index in range(F1_PRESEALED_DESCRIPTOR_COUNT):
                target_captured = (descriptor_index + 1) * FROZEN_CAPTURE_BATCH_FRAMES
                for frame_offset in range(FROZEN_CAPTURE_BATCH_FRAMES):
                    venue = VenueV2.BYBIT if frame_offset % 2 == 0 else VenueV2.BINANCE
                    lane_name = "BYBIT" if venue == VenueV2.BYBIT else "BINANCE_MARKET"
                    frame_index = offered_by_lane[lane_name]
                    frame = _frame(venue, frame_index, time.time_ns())
                    lane = runtime.source._lanes[lane_name]
                    if not lane._handoff.offer(frame):
                        raise RuntimeError("F1_PRESEALED_QUEUE_REJECTED_FRAME")
                    offered_by_lane[lane_name] += 1
                    expected_raw[frame.raw_payload_hash] = frame.raw_payload_bytes
                capture_deadline = time.monotonic() + 5.0
                sealed_status = runtime.capture.status()
                while (int(sealed_status.capture["captured_frames"]) < target_captured
                       or int(sealed_status.capture["pending_batches"]) == 0
                       or int(sealed_status.capture.get("capture_in_progress_frames", 0)) != 0):
                    if time.monotonic() >= capture_deadline:
                        raise TimeoutError("F1_PRESEALED_CAPTURE_DURABILITY_TIMEOUT")
                    time.sleep(0.001)
                    sealed_status = runtime.capture.status()
                if int(sealed_status.capture["captured_frames"]) != target_captured:
                    raise RuntimeError("F1_PRESEALED_CAPTURE_BATCH_BOUNDARY_MISMATCH")
                phase_before = runtime._phase_timing_summary_ns()
                cycle_start_ns = time.monotonic_ns()
                prior_cycles = len(runtime._descriptor_cycle_ns)
                publication_deadline = time.monotonic() + 10.0
                while len(runtime._descriptor_cycle_ns) == prior_cycles:
                    runtime.service(repository, now_ns=time.time_ns())
                    if runtime._terminal_error is not None:
                        raise RuntimeError(runtime._terminal_error)
                    if time.monotonic() >= publication_deadline:
                        raise TimeoutError("F1_PRESEALED_PUBLICATION_TIMEOUT")
                    if runtime._pending_adoption is not None:
                        time.sleep(0.001)
                cycle_end_ns = time.monotonic_ns()
                descriptor_frames = runtime._descriptor_cycle_frames[-1]
                if descriptor_frames != FROZEN_CAPTURE_BATCH_FRAMES:
                    raise RuntimeError("F1_PRESEALED_DESCRIPTOR_NOT_16_FRAMES")
                cycles.append({"descriptor_index": descriptor_index,
                    "frames": descriptor_frames,
                    "capture_sealed_at_monotonic_ns": sealed_status.capture.get("last_sealed_monotonic_ns"),
                    "writer_start_monotonic_ns": cycle_start_ns,
                    "observation_qualified_at_monotonic_ns": cycle_end_ns,
                    "complete_descriptor_cycle_ns": runtime._descriptor_cycle_ns[-1],
                    "service_wrapper_ns": cycle_end_ns - cycle_start_ns,
                    "phase_delta_ns": _phase_delta(phase_before, runtime._phase_timing_summary_ns()),
                    "persistence": repository.persistence_metrics()})
            runtime.finish(repository)
            checkpoints = repository.artifact_entries("L2FrameArchiveCheckpointV3")
            archive_rows = []
            if runtime._archive is None:
                raise RuntimeError("F1_PRESEALED_ARCHIVE_NOT_BOUND")
            for checkpoint in checkpoints:
                archive_rows.extend(runtime._archive.read_chunk(str(checkpoint.metadata["chunk_id"])))
            reconstructed = {str(row["raw_payload_hash"]): bytes(row["raw_payload_bytes"])
                             for row in archive_rows}
            if reconstructed != expected_raw:
                raise ValueError("F1_PRESEALED_FRAME_VIEW_RECONSTRUCTION_MISMATCH")
            run_id = runtime._run_id
            if run_id is None:
                raise RuntimeError("F1_PRESEALED_RUN_ID_MISSING")
            publication_rows = repository._connection.execute(
                "SELECT p.publication_id,p.logical_ready_at_ns,o.observed_at_ns "
                "FROM public_evidence_publication_v2 p LEFT JOIN public_evidence_publication_observation_v2 o "
                "ON o.publication_id=p.publication_id WHERE p.run_id=? ORDER BY p.publication_id", (run_id,),
            ).fetchall()
            if len(publication_rows) != F1_PRESEALED_DESCRIPTOR_COUNT or any(
                    row["observed_at_ns"] is None
                    or int(row["observed_at_ns"]) < int(row["logical_ready_at_ns"])
                    for row in publication_rows):
                raise ValueError("F1_PRESEALED_PUBLICATION_OBSERVATION_OR_CHRONOLOGY_INVALID")
            indexed_entries = repository.artifact_entries("BroadPublicStreamContinuityV3")
            if len(indexed_entries) < 2 * target_captured:
                raise ValueError("F1_PRESEALED_CONTINUITY_EVIDENCE_POPULATION_INVALID")
            if any(repository.effective_available_at_ns(entry.artifact_ref) is None
                   for entry in indexed_entries):
                raise ValueError("F1_PRESEALED_CAUSAL_CONTINUITY_VISIBILITY_INVALID")
            clean_capture = runtime.capture.status()
            if (int(clean_capture.capture["pending_frames"]) != 0
                    or int(clean_capture.capture["pending_batches"]) != 0
                    or int(clean_capture.handoff.queue_items) != 0
                    or int(clean_capture.capture["captured_frames"]) != target_captured
                    or int(clean_capture.capture["delivered_frames"]) != target_captured
                    or int(runtime._service_frames) != target_captured):
                raise ValueError("F1_PRESEALED_FINAL_CAPTURE_ACCOUNTING_INVALID")
            final_capture = clean_capture
            capture_metrics = dict(clean_capture.capture.get("archive", {}))
            phase_timings = runtime._phase_timing_summary_ns()
    except Exception as exc:
        failure = f"{type(exc).__name__}:{exc}"
        clean_capture = runtime.capture.status() if runtime.capture is not None else None
        capture_metrics = dict(clean_capture.capture.get("archive", {})) if clean_capture is not None else {}
        phase_timings = runtime._phase_timing_summary_ns()
        publication_rows = []
        final_capture = clean_capture
        journal_mode = None
        synchronous = None
        archive_rows = []
        indexed_entries = ()
    finally:
        runtime.close()
    cycle_ns = [int(item["complete_descriptor_cycle_ns"]) for item in cycles]
    mean_cycle_ns = int(statistics.fmean(cycle_ns)) if cycle_ns else None
    maximum_service_gap_ns = int(runtime._max_service_gap_ns)
    passed = (failure is None and len(cycles) == F1_PRESEALED_DESCRIPTOR_COUNT
        and all(int(cycle["frames"]) == FROZEN_CAPTURE_BATCH_FRAMES for cycle in cycles)
        and mean_cycle_ns is not None and mean_cycle_ns <= 66_700_000
        and maximum_service_gap_ns <= FROZEN_SERVICE_GAP_NS
        and journal_mode == "wal" and synchronous == 2
        and len(archive_rows) == F1_PRESEALED_DESCRIPTOR_COUNT * FROZEN_CAPTURE_BATCH_FRAMES
        and len(indexed_entries) >= 2 * F1_PRESEALED_DESCRIPTOR_COUNT * FROZEN_CAPTURE_BATCH_FRAMES
        and final_capture is not None
        and int(final_capture.capture["pending_frames"]) == 0
        and int(final_capture.handoff.queue_items) == 0)
    profile = _f1_presealed_profile()
    result = {"schema_version": 1, "checkpoint": "F1-presealed",
        "scope": "SHORT_SYNTHETIC_DESCRIPTOR_FEASIBILITY", "capacity_certificate": False,
        "base_commit": _git_sha(root), "working_tree_source_hash": _working_tree_source_hash(root),
        "working_tree_source_hashes": _source_files(root), "profile": profile,
        "profile_hash": sha256_json(profile), "host": {"platform": platform.platform(),
            "cpu_count": os.cpu_count(), "device_id": os.stat(report_root).st_dev,
            "free_bytes_at_end": shutil.disk_usage(report_root).free},
        "durability": {"sqlite_journal_mode": journal_mode, "sqlite_synchronous": synchronous,
            "capture_archive_metrics": capture_metrics,
            "publication_count": len(publication_rows),
            "publication_observed_after_logical_ready": bool(publication_rows) and all(
                int(row["observed_at_ns"]) >= int(row["logical_ready_at_ns"])
                for row in publication_rows if row["observed_at_ns"] is not None)},
        "workload": {"offered_frames": F1_PRESEALED_DESCRIPTOR_COUNT * FROZEN_CAPTURE_BATCH_FRAMES,
            "offered_by_lane": offered_by_lane if failure is None else {},
            "expected_trade_rows_per_frame": 1, "queue_items_limit": FROZEN_QUEUE_ITEMS,
            "queue_bytes_limit": FROZEN_QUEUE_BYTES, "service_gap_limit_ns": FROZEN_SERVICE_GAP_NS},
        "cycles": cycles, "timing_ns": {"complete_16_frame_descriptor_cycle_mean": mean_cycle_ns,
            "complete_16_frame_descriptor_cycle_p95": _percentile(cycle_ns, .95),
            "complete_16_frame_descriptor_cycle_max": max(cycle_ns, default=0),
            "stream_service_gap_max": maximum_service_gap_ns, "phase_timings": phase_timings},
        "integrity": {"archive_reconstructed_frames": len(archive_rows),
            "archive_raw_payload_match": len(archive_rows) == len(expected_raw),
            "observation_qualified_continuity_entries": len(indexed_entries),
            "zero_final_backlog": (final_capture is not None
                and int(final_capture.capture["pending_frames"]) == 0
                and int(final_capture.handoff.queue_items) == 0)},
        "failure": failure, "passed": passed,
        "claim": "Feasibility only; does not qualify F1 capacity, a host profile, or endurance."}
    result["report_content_sha256"] = sha256_json(result)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    os.replace(temporary, output)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", required=True, choices=("F1", "F1-micro", "F1-presealed"))
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--burst-at", type=int)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    repository_root = Path(__file__).resolve().parents[1]
    if args.checkpoint == "F1-presealed":
        if args.seconds is not None or args.burst_at is not None:
            raise SystemExit("F1-presealed uses its fixed four-descriptor profile")
        if args.profile is not None and json.loads(args.profile.read_text()) != _f1_presealed_profile():
            raise SystemExit("profile does not match the source-approved presealed descriptor workload")
        result = _f1_presealed(repository_root, args.root.resolve(), args.output.resolve())
        print(json.dumps({**result, "report_file_sha256": _file_hash(args.output.resolve())}, sort_keys=True))
        return 0 if result["passed"] else 1
    if args.checkpoint == "F1-micro":
        if args.seconds is None or args.burst_at is None:
            raise SystemExit("F1-micro requires --seconds and --burst-at")
        seconds, burst_at = args.seconds, args.burst_at
        if (seconds, burst_at) != (F1_MICRO_SECONDS, F1_MICRO_BURST_START_SECONDS):
            raise SystemExit("F1-micro is fixed at --seconds 12 --burst-at 6")
    else:
        seconds, burst_at = F1_SECONDS, F1_BURST_START_SECONDS
        if args.seconds is not None or args.burst_at is not None:
            raise SystemExit("full F1 remains fixed at 60 seconds with --burst-at 30")
    if args.profile is not None:
        body = json.loads(args.profile.read_text())
        expected = _f1_profile(checkpoint=args.checkpoint, seconds=seconds, burst_at=burst_at)
        if body != expected:
            raise SystemExit("profile does not match the source-approved F1 workload parameters")
    result = _f1(repository_root, args.root.resolve(), args.output.resolve(),
                 checkpoint=args.checkpoint, seconds=seconds, burst_at=burst_at)
    print(json.dumps({**result, "report_file_sha256": _file_hash(args.output.resolve())}, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
