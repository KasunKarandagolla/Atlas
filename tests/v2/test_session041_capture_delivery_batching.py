"""Bounded 160-frame/s capture under fixed per-sync latency."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.data.broad_stream_source import (
    BroadDurablePublicCaptureV2,
    BroadPublicStreamPlanV2,
    BroadPublicStreamSourceV2,
)
from atlas.v2.data.capture_receipts import CaptureReceiptJournalV1
from atlas.v2.data.public_archive_extents import PublicArchiveSegmentWriterV1
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.data.universe import ComputeTierV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

PRODUCER_FPS = 160
PRODUCER_FRAMES = 1_280
FIXED_SYNC_LATENCY_SECONDS = 0.08
NOW = 1_800_000_000_000_000_000


def _product(venue: VenueV2, symbol: str) -> ProductContractV2:
    ref = sha256_json({"venue": venue.value, "symbol": symbol})
    key = InstrumentKeyV2(
        venue,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        symbol,
        symbol.removesuffix("USDT"),
        "USDT",
        "USDT",
        ref,
    )
    return ProductContractV2(
        key,
        NOW,
        NOW,
        NOW,
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.001"),
        Decimal("0.001"),
        TradingStatusV2.TRADING,
        ref,
    )


def _idle_stream():
    async def stream():
        await asyncio.Event().wait()
        if False:
            yield None

    return stream


def _frame(
    index: int,
    key: InstrumentKeyV2,
    *,
    trade_id: str | None = None,
    event_at_ns: int | None = None,
) -> CapturedPublicFrameV2:
    at_ns = NOW + index + 1
    event_ns = at_ns if event_at_ns is None else event_at_ns
    if key.venue is VenueV2.BYBIT:
        channel = f"publicTrade.{key.native_symbol}"
        body = {
            "topic": channel,
            "type": "snapshot",
            "ts": event_ns // 1_000_000,
            "data": [
                {
                    "T": event_ns // 1_000_000,
                    "s": key.native_symbol,
                    "S": "Buy",
                    "v": "0.01",
                    "p": "100",
                    "i": trade_id or f"r2-{index}",
                }
            ],
        }
    else:
        channel = f"{key.native_symbol.lower()}@aggTrade"
        body = {
            "stream": channel,
            "data": {"a": index + 1, "p": "100", "q": "0.01", "T": event_ns // 1_000_000, "m": True},
        }
    raw = json.dumps(body, separators=(",", ":")).encode()
    source_id = "BYBIT_PUBLIC_WS_BROAD_V2" if key.venue is VenueV2.BYBIT else "BINANCE_MARKET_PUBLIC_WS_BROAD_V2"
    return CapturedPublicFrameV2(
        key.venue,
        source_id,
        channel,
        raw,
        hashlib.sha256(raw).hexdigest(),
        at_ns,
        at_ns,
        1,
    )


def _identity(frame: CapturedPublicFrameV2) -> tuple:
    return (
        frame.venue,
        frame.source_id,
        frame.channel,
        frame.raw_payload_bytes,
        frame.raw_payload_hash,
        frame.received_at_ns,
        frame.available_at_ns,
        frame.connection_epoch,
    )


def _two_lane_source():
    bybit = _product(VenueV2.BYBIT, "BTCUSDT")
    binance = _product(VenueV2.BINANCE, "BTCUSDT")
    products = (bybit, binance)
    plan = BroadPublicStreamPlanV2.build(
        products,
        {bybit.key: ComputeTierV2.TIER_1, binance.key: ComputeTierV2.TIER_1},
        created_at_ns=NOW,
        benchmark_keys=(bybit.key, binance.key),
    )
    source = BroadPublicStreamSourceV2(
        plan, stream_factories={"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()}
    )
    return bybit, binance, source


def test_busy_lane_reaches_protected_reserve_boundary_and_latches_exact_loss():
    bybit, _binance, source = _two_lane_source()
    handoff = source._lanes["BYBIT"]._handoff
    frame = _frame(0, bybit.key)

    assert all(handoff.offer(frame) for _ in range(480))
    assert not handoff.offer(frame)
    status = source.status().handoff
    budget = source._queue_budget.snapshot_v2()

    assert status.queue_items == status.high_water_items == 480
    assert status.max_queue_items == 512
    assert status.max_queue_bytes == 16_000_000
    assert status.frames_rejected == 1 and status.overflowed
    assert dict(budget.lane_reserved_items) == {"BINANCE_MARKET": 0, "BYBIT": 480}
    assert dict(budget.lane_reservation_floor_items) == {"BINANCE_MARKET": 32, "BYBIT": 32}
    assert dict(budget.rejection_counts) == {"APPEND_FAILURE": 0, "LOCAL_LIMIT": 0, "SHARED_LIMIT": 1}
    assert budget.loss_latched and budget.loss_reasons == ("SHARED_LIMIT",)
    source.close()


def test_two_busy_lanes_share_capacity_and_drain_fairly():
    bybit, binance, source = _two_lane_source()
    bybit_handoff = source._lanes["BYBIT"]._handoff
    binance_handoff = source._lanes["BINANCE_MARKET"]._handoff

    for index in range(256):
        assert bybit_handoff.offer(_frame(index, bybit.key))
        assert binance_handoff.offer(_frame(index, binance.key))
    full = source.status().handoff
    drained = source.drain(max_items=64)
    bybit_count = sum(frame.venue is VenueV2.BYBIT for frame in drained)
    binance_count = sum(frame.venue is VenueV2.BINANCE for frame in drained)
    after = source.status().handoff
    budget = source._queue_budget.snapshot_v2()

    assert full.queue_items == full.high_water_items == 512
    assert full.max_queue_items == 512 and full.max_queue_bytes == 16_000_000
    assert full.high_water_bytes <= full.max_queue_bytes
    assert len(drained) == 64 and (bybit_count, binance_count) == (32, 32)
    assert after.queue_items == 448 and after.frames_rejected == 0
    assert not budget.loss_latched and not budget.loss_reasons
    source.close()


def test_capture_preserves_repeated_trade_ids_late_times_and_partial_flush(tmp_path):
    bybit, _binance, source = _two_lane_source()
    capture = BroadDurablePublicCaptureV2(source, clock_ns=lambda: NOW + 10_000_000_000)
    capture.configure_capture(tmp_path)
    capture.start()
    offered = tuple(
        _frame(index, bybit.key, trade_id="same-trade-id", event_at_ns=NOW + (32 - index) * 1_000_000)
        for index in range(30)
    )
    handoff = source._lanes["BYBIT"]._handoff
    assert all(handoff.offer(frame) for frame in offered)

    capture.close()
    sealed = []
    while (batch := capture.drain_sealed_transport()) is not None:
        assert batch.frame_count <= 16
        sealed.append(batch)
    restored = tuple(frame for batch in sealed for frame in batch.read_unpublished(tmp_path))
    status = capture.status()
    payload_event_times = tuple(json.loads(frame.raw_payload_bytes)["data"][0]["T"] for frame in restored)

    assert status.handoff.frames_received == status.handoff.frames_drained == len(offered)
    assert status.handoff.frames_rejected == 0 and not status.handoff.overflowed
    assert status.capture["captured_frames"] == status.capture["delivered_frames"] == len(offered)
    assert status.capture["pending_batches"] == 0 and status.capture["pending_frames"] == 0
    assert status.capture["terminal_error"] is None and status.capture["batch_frame_limit"] == 16
    assert tuple(batch.frame_count for batch in sealed) == (16, 14)
    assert tuple(_identity(frame) for frame in restored) == tuple(_identity(frame) for frame in offered)
    assert all(json.loads(frame.raw_payload_bytes)["data"][0]["i"] == "same-trade-id" for frame in restored)
    assert payload_event_times == tuple(sorted(payload_event_times, reverse=True))


def test_group_archive_failure_is_terminal_and_not_fabricated_as_delivery(tmp_path, monkeypatch):
    bybit, _binance, source = _two_lane_source()
    capture = BroadDurablePublicCaptureV2(source, clock_ns=lambda: NOW + 10_000_000_000)
    capture.configure_capture(tmp_path)

    failed_group = threading.Event()

    def fail_seal_many(_writer, _chunks):
        failed_group.set()
        raise OSError("injected grouped archive failure")

    monkeypatch.setattr(PublicArchiveSegmentWriterV1, "seal_many", fail_seal_many)
    capture.start()
    handoff = source._lanes["BYBIT"]._handoff
    offered = tuple(_frame(index, bybit.key) for index in range(32))
    assert all(handoff.offer(frame) for frame in offered)

    assert failed_group.wait(timeout=5), "capture never attempted the bounded grouped archive write"
    capture.close()
    status = capture.status()
    budget = source._queue_budget.snapshot_v2()

    assert status.state == "FAILED"
    assert status.last_error_code == "RAW_CAPTURE_FAILED_OSERROR"
    assert status.capture["terminal_error"] == "RAW_CAPTURE_FAILED_OSERROR"
    assert status.capture["captured_frames"] == status.capture["delivered_frames"] == 0
    assert status.capture["pending_batches"] == 0
    assert status.handoff.frames_received == status.handoff.frames_drained == len(offered)
    assert status.handoff.frames_rejected == 0 and not status.handoff.overflowed
    assert not budget.loss_latched and not budget.loss_reasons
    assert capture.drain_sealed_transport() is None


def test_broad_capture_keeps_160_fps_delivery_under_bounded_sync_latency(
    tmp_path,
    monkeypatch,
):
    bybit = _product(VenueV2.BYBIT, "BTCUSDT")
    binance = _product(VenueV2.BINANCE, "BTCUSDT")
    products = (bybit, binance)
    plan = BroadPublicStreamPlanV2.build(
        products,
        {bybit.key: ComputeTierV2.TIER_1, binance.key: ComputeTierV2.TIER_1},
        created_at_ns=NOW,
        benchmark_keys=(bybit.key, binance.key),
    )
    assert set(plan.lane_topics) == {"BYBIT", "BINANCE_MARKET"}
    source = BroadPublicStreamSourceV2(
        plan, stream_factories={"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()}
    )
    # Keep the diagnostic frame chronology inside the injected clock horizon.
    # Capture timing itself remains monotonic and independent from wall time.
    capture = BroadDurablePublicCaptureV2(source, clock_ns=lambda: NOW + 10_000_000_000)
    capture.configure_capture(tmp_path)
    capture.start()

    latency_enabled = threading.Event()
    latency_enabled.set()
    sync_samples_ns: dict[str, list[int]] = {"archive": [], "receipts": []}
    archive_group_extent_counts: list[int] = []
    original_seal = PublicArchiveSegmentWriterV1.seal
    original_seal_many = PublicArchiveSegmentWriterV1.seal_many
    original_append = CaptureReceiptJournalV1.append
    original_append_many = getattr(CaptureReceiptJournalV1, "append_many", None)

    def delay_after_sync(stage: str, original, *args, **kwargs):
        started = time.monotonic_ns()
        result = original(*args, **kwargs)
        if latency_enabled.is_set():
            time.sleep(FIXED_SYNC_LATENCY_SECONDS)
        sync_samples_ns[stage].append(time.monotonic_ns() - started)
        return result

    def delayed_seal(self, *args, **kwargs):
        return delay_after_sync("archive", original_seal, self, *args, **kwargs)

    def delayed_seal_many(self, *args, **kwargs):
        archive_group_extent_counts.append(len(args[0]))
        return delay_after_sync("archive", original_seal_many, self, *args, **kwargs)

    def delayed_append(self, *args, **kwargs):
        return delay_after_sync("receipts", original_append, self, *args, **kwargs)

    monkeypatch.setattr(PublicArchiveSegmentWriterV1, "seal", delayed_seal)
    monkeypatch.setattr(PublicArchiveSegmentWriterV1, "seal_many", delayed_seal_many)
    monkeypatch.setattr(CaptureReceiptJournalV1, "append", delayed_append)
    if original_append_many is not None:

        def delayed_append_many(self, *args, **kwargs):
            return delay_after_sync("receipts", original_append_many, self, *args, **kwargs)

        monkeypatch.setattr(CaptureReceiptJournalV1, "append_many", delayed_append_many)

    handoff = source._lanes["BYBIT"]._handoff
    accepted: list[CapturedPublicFrameV2] = []
    rejection: dict[str, object] | None = None
    producer_done = threading.Event()
    producer_error: list[str] = []
    offer_times_ns: list[int] = []

    def produce() -> None:
        nonlocal rejection
        start = time.monotonic()
        try:
            for index in range(PRODUCER_FRAMES):
                due = start + index / PRODUCER_FPS
                remaining = due - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                frame = _frame(index, bybit.key)
                if not handoff.offer(frame):
                    budget = source._queue_budget.snapshot_v2()
                    status = capture.status()
                    rejection = {
                        "attempted_index": index,
                        "queue_items": status.handoff.queue_items,
                        "queue_bytes": status.handoff.queue_bytes,
                        "queue_high_water_items": status.handoff.high_water_items,
                        "max_queue_items": status.handoff.max_queue_items,
                        "max_queue_bytes": status.handoff.max_queue_bytes,
                        "lane_reserved_items": dict(budget.lane_reserved_items),
                        "lane_reserved_bytes": dict(budget.lane_reserved_bytes),
                        "lane_reservation_floor_items": dict(budget.lane_reservation_floor_items),
                        "rejection_counts": dict(budget.rejection_counts),
                        "loss_latched": budget.loss_latched,
                        "capture": {
                            name: status.capture.get(name)
                            for name in (
                                "pending_batches",
                                "pending_frames",
                                "capture_in_progress_frames",
                                "captured_frames",
                                "delivered_frames",
                                "terminal_error",
                                "archive",
                            )
                        },
                    }
                    break
                accepted.append(frame)
                offer_times_ns.append(time.monotonic_ns())
        except BaseException as exc:
            producer_error.append(f"{type(exc).__name__}:{exc}")
        finally:
            producer_done.set()

    producer = threading.Thread(target=produce, name="r2-160fps-producer")
    producer.start()
    durable_frames: list[CapturedPublicFrameV2] = []

    def collect_sealed() -> None:
        while (sealed := capture.drain_sealed_transport()) is not None:
            assert sealed.frame_count <= 16
            durable_frames.extend(sealed.read_unpublished(tmp_path))

    while not producer_done.wait(0.002):
        collect_sealed()
    producer.join(timeout=2.0)
    latency_enabled.clear()
    capture.close()
    collect_sealed()

    status = capture.status()
    budget = source._queue_budget.snapshot_v2()
    elapsed = (offer_times_ns[-1] - offer_times_ns[0]) / 1_000_000_000 if len(offer_times_ns) > 1 else 0.0
    metrics = {
        "producer_target_fps": PRODUCER_FPS,
        "offered_count": len(offer_times_ns) + int(rejection is not None),
        "accepted_count": len(accepted),
        "rejected_count": status.handoff.frames_rejected,
        "rejection": rejection,
        "capture_drained_count": status.handoff.frames_drained,
        "capture_sealed_count": status.capture["captured_frames"],
        "capture_delivered_count": status.capture["delivered_frames"],
        "durably_read_count": len(durable_frames),
        "actual_offer_span_seconds": elapsed,
        "actual_offer_rate_fps": ((len(offer_times_ns) - 1) / elapsed if elapsed > 0 else 0),
        "queue_high_water_items": status.handoff.high_water_items,
        "queue_limit_items": status.handoff.max_queue_items,
        "queue_high_water_bytes": status.handoff.high_water_bytes,
        "queue_limit_bytes": status.handoff.max_queue_bytes,
        "lane_reserved_items": dict(budget.lane_reserved_items),
        "lane_reservation_floor_items": dict(budget.lane_reservation_floor_items),
        "budget_rejection_counts": dict(budget.rejection_counts),
        "capture_batch_frame_limit": status.capture["batch_frame_limit"],
        "archive_sync_calls": len(sync_samples_ns["archive"]),
        "archive_group_extent_counts": archive_group_extent_counts,
        "receipt_sync_calls": len(sync_samples_ns["receipts"]),
        "archive_sync_durations_ns": sync_samples_ns["archive"],
        "receipt_sync_durations_ns": sync_samples_ns["receipts"],
        "producer_errors": producer_error,
        "loss_latched": budget.loss_latched,
        "source_state": str(source.status().state),
    }
    print("SESSION041_R2_CAPTURE_SYNC_LATENCY=" + json.dumps(metrics, sort_keys=True))

    assert not producer.is_alive() and not producer_error
    assert rejection is None, json.dumps(metrics, sort_keys=True)
    assert len(accepted) == PRODUCER_FRAMES
    assert status.handoff.frames_received == len(accepted)
    assert status.handoff.frames_drained == len(accepted)
    assert status.handoff.frames_rejected == 0 and not status.handoff.overflowed
    assert not budget.loss_latched and not budget.loss_reasons
    assert status.handoff.high_water_items <= 512
    assert status.handoff.high_water_bytes <= 16_000_000
    assert status.handoff.max_drain_items == 64
    assert status.capture["batch_frame_limit"] == 16
    assert status.capture["pending_frames"] == 0
    assert status.capture["terminal_error"] is None
    assert len(durable_frames) == len(accepted)
    assert [_identity(frame) for frame in durable_frames] == [_identity(frame) for frame in accepted]
    assert sum(archive_group_extent_counts) == len(accepted) // 16
    assert all(group_count == 2 for group_count in archive_group_extent_counts)
