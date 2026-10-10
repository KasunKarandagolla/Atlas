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
F1_LANE_RATE = 80
F1_BURST_LANE_RATE = 160
F1_BURST_START_SECONDS = 30


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
) -> dict[str, Any]:
    """Require a measured one-second burst to drain back to its quiet baseline."""
    baseline_samples = [pending for elapsed, pending in samples
                        if burst_start_seconds - 5 <= elapsed < burst_start_seconds]
    baseline = max(baseline_samples, default=None)
    deadline = burst_start_seconds + 1 + 20
    recovered_at = next((elapsed for elapsed, pending in samples
                         if elapsed >= burst_start_seconds + 1 and elapsed <= deadline
                         and baseline is not None and pending <= baseline), None)
    return {
        "baseline_window_seconds": [burst_start_seconds - 5, burst_start_seconds],
        "baseline_max_pending_frames": baseline,
        "baseline_max_allowed_frames": FROZEN_CAPTURE_BATCH_FRAMES,
        "deadline_elapsed_seconds": deadline,
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


def _f1_profile() -> dict[str, Any]:
    return {"schema_version": 1, "checkpoint": "F1", "venues": ["BYBIT", "BINANCE"],
        "sustained_frames_per_second": 160, "lane_frames_per_second": 80,
        "burst_frames_per_second": 320, "burst_seconds": 1, "burst_start_seconds": 30,
        "queue_items": FROZEN_QUEUE_ITEMS, "queue_bytes": FROZEN_QUEUE_BYTES,
        "service_gap_ns": FROZEN_SERVICE_GAP_NS, "capture_batch_frames": 16,
        "trade_observations_per_frame": 1}


def _f1(root: Path, report_root: Path, output: Path) -> dict[str, Any]:
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
                extra = min(max(elapsed - F1_BURST_START_SECONDS, 0.0), 1.0)
                target = int(F1_LANE_RATE * elapsed + F1_LANE_RATE * extra)
                while index < target and not producer_stop.is_set():
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
                    offered_at_seconds[venue].append(time.monotonic() - start)
                    index += 1
                base_before_burst = F1_LANE_RATE * F1_BURST_START_SECONDS
                base_after_burst = base_before_burst + F1_BURST_LANE_RATE
                if index < base_before_burst:
                    target_time = start + (index + 1) / F1_LANE_RATE
                elif index < base_after_burst:
                    target_time = (start + F1_BURST_START_SECONDS
                                   + (index - base_before_burst + 1) / F1_BURST_LANE_RATE)
                else:
                    target_time = (start + F1_BURST_START_SECONDS + 1
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
        production_started = started
        start_producer(VenueV2.BYBIT)
        start_producer(VenueV2.BINANCE)
        next_sample = started
        while time.monotonic() - started < F1_SECONDS and not producer_stop.is_set():
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
                pending = int(status.handoff.queue_items) + int(status.capture["pending_frames"])
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
                    "pending_frames": pending,
                    "handoff_items": int(status.handoff.queue_items),
                    "handoff_bytes": int(status.handoff.queue_bytes),
                    "lane_occupancy": lane_samples,
                    "capture_pending_frames": int(status.capture["pending_frames"]),
                    "capture_batches": int(status.capture["pending_batches"]),
                    "captured_frames": int(status.capture["captured_frames"]),
                    "delivered_frames": int(status.capture["delivered_frames"]),
                    "indexed_frames": int(runtime._service_frames),
                    "service_calls": int(runtime._service_calls),
                    "service_last_duration_ns": int(runtime._last_service_duration_ns),
                    "service_max_duration_ns": int(runtime._max_service_duration_ns),
                    "service_max_gap_ns": int(runtime._max_service_gap_ns),
                    "phase_timings_ns": status.phase_timings_ns,
                    "persistence": metrics})
                next_sample += 1.0
            time.sleep(0.0005)
        elapsed = max(0.0, time.monotonic() - started)
        processed_before_stop = runtime._service_frames
        producer_stop.set()
        for thread in producers:
            thread.join(timeout=2.0)
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
                [at for at in offered_at_seconds[venue] if 30 <= at < 31], 1.0
            ) for venue in offered_at_seconds
        }
        sustained_by_lane = {
            venue.value: {
                "preburst_fps": sum(1 for at in offered_at_seconds[venue] if 0 <= at < 30) / 30,
                "postburst_fps": sum(1 for at in offered_at_seconds[venue] if 31 <= at < elapsed)
                    / max(0.001, elapsed - 31),
            } for venue in offered_at_seconds
        }
        burst_recovery = _burst_recovery(pending_series, burst_start_seconds=30)
        max_handoff_items = max((sample["handoff_items"] for sample in samples), default=0)
        max_handoff_bytes = max((sample["handoff_bytes"] for sample in samples), default=0)
        result = {
            "schema_version": 1,
            "checkpoint": "F1",
            "scope": "SHORT_SYNTHETIC_ENGINEERING_PROBE",
            "capacity_certificate": False,
            "base_commit": _git_sha(root),
            "working_tree_source_hash": _working_tree_source_hash(root),
            "profile": _f1_profile(),
            "profile_hash": sha256_json(_f1_profile()),
            "working_tree_source_hashes": {
                str(path.relative_to(root)): _file_hash(path)
                for path in (root / "src/atlas/v2/data/public_microstructure_ws.py",
                             root / "src/atlas/v2/data/broad_stream_source.py",
                             root / "src/atlas/v2/runtime/broad_public_runtime.py",
                             root / "scripts/session041_recovery_capacity.py") if path.exists()
            },
            "host": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                     "device_id": os.stat(report_root).st_dev,
                     "free_bytes_at_end": shutil.disk_usage(report_root).free},
            "workload": {"duration_seconds": elapsed, "target_seconds": F1_SECONDS,
                "sustained_total_fps": 2 * F1_LANE_RATE, "burst_total_fps": 2 * F1_BURST_LANE_RATE,
                "burst_seconds": 1, "burst_start_seconds": F1_BURST_START_SECONDS,
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
                "durably_adopted_frames": delivered, "indexed_frames": processed,
                "indexed_frames_at_stop": processed_before_stop,
                "durable_capture_per_second": captured / elapsed if elapsed else 0,
                "indexed_per_second_during_producer_window": processed_before_stop / elapsed if elapsed else 0,
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
                "mean_16_frame_processing_ns": mean_16_frame_ns},
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
            "passed": (failure is None and not rejected and lost == 0 and
                sum(offered.values()) == captured == delivered == processed and
                (sum(offered.values()) / elapsed >= 160 if elapsed else False) and
                (processed_before_stop / elapsed >= F1_LANE_RATE * 2 if elapsed else False) and
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
                len(samples) >= F1_SECONDS - 2 and
                mean_16_frame_ns is not None and mean_16_frame_ns <= 100_000_000 and
                (not tail or (_slope(tail) is not None and _slope(tail) <= 0))),
            "limits_unchanged": True,
            "claim": "This short synthetic checkpoint does not qualify a host, profile, venue source, or 48-hour endurance.",
        }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    os.replace(temporary, output)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", required=True, choices=("F1",))
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    repository_root = Path(__file__).resolve().parents[1]
    if args.profile is not None:
        body = json.loads(args.profile.read_text())
        expected = _f1_profile()
        if body != expected:
            raise SystemExit("profile does not match the frozen F1 workload")
    result = _f1(repository_root, args.root.resolve(), args.output.resolve())
    print(json.dumps(result, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
