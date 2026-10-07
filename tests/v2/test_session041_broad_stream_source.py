from __future__ import annotations

import concurrent.futures
import hashlib
import threading
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.broad_stream_source import (
    BroadDurablePublicCaptureV2,
    BroadPublicStreamPlanV2,
    BroadPublicStreamSourceV2,
)
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.microstructure import SequenceValidBookV2
from atlas.v2.data.microstructure_archive import L2FrameArchiveV2
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2, parse_binance_depth_frame
from atlas.v2.data.public_stream_continuity import PublicStreamContinuityTrackerV1
from atlas.v2.data.universe import ComputeTierV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2


def _product(venue: VenueV2, symbol: str, revision: str = "a") -> ProductContractV2:
    ref = sha256_json({"venue": venue.value, "symbol": symbol, "revision": revision})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, symbol.removesuffix("USDT"), "USDT", "USDT", ref)
    return ProductContractV2(key, 100, 100, 100, Decimal("1"), Decimal("0.01"),
                             Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, ref)


def _plan():
    bybit = _product(VenueV2.BYBIT, "BTCUSDT")
    binance = _product(VenueV2.BINANCE, "BTCUSDT")
    return BroadPublicStreamPlanV2.build(
        (bybit, binance), {bybit.key: ComputeTierV2.TIER_3, binance.key: ComputeTierV2.TIER_3},
        created_at_ns=200, benchmark_keys=(bybit.key, binance.key),
    )


def _frame(venue: VenueV2, source: str, channel: str, payload: bytes):
    return CapturedPublicFrameV2(venue, source, channel, payload, __import__("hashlib").sha256(payload).hexdigest(),
                                 300, 300, 1)


def _lane(now_ns: int, *, epoch: int = 1, activity: int | None = None, connected: bool = True):
    return SimpleNamespace(state="RUNNING", attempt_count=epoch, handoff=SimpleNamespace(
        connected=connected, overflowed=False, backpressure=False, closed=False,
        last_activity_at_ns=now_ns if activity is None else activity,
        queue_items=0, disconnect_count=0,
    ))


def test_stream_plan_is_venue_aware_and_aggregate_queue_keeps_global_caps():
    plan = _plan()
    assert len(plan.keys) == 2
    assert len(plan.identities) == 4
    assert set(plan.lane_topics) == {"BYBIT", "BINANCE_DEPTH", "BINANCE_MARKET"}
    source = BroadPublicStreamSourceV2(plan, stream_factories={name: (lambda: iter(())) for name in plan.lane_topics})
    status = source.status().handoff
    assert status.max_queue_items == 512
    assert status.max_queue_bytes == 16_000_000
    assert status.max_drain_items == 64
    capture = BroadDurablePublicCaptureV2(source).status()
    assert capture.capture["max_pending_batches"] == 64
    assert "lanes" in capture.capture
    bybit_channel = "publicTrade.BTCUSDT"
    binance_channel = "btcusdt@aggTrade"
    bybit_frame = _frame(VenueV2.BYBIT, "BYBIT_PUBLIC_WS_BROAD_V2", bybit_channel,
                         b'{"topic":"publicTrade.BTCUSDT","data":[{"i":"x","S":"Buy","p":"10","v":"2","T":1000}]}')
    binance_frame = _frame(VenueV2.BINANCE, "BINANCE_MARKET_PUBLIC_WS_BROAD_V2", binance_channel,
                           b'{"stream":"btcusdt@aggTrade","data":{"a":7,"p":"10","q":"2","T":1000,"m":true}}')
    assert plan.key_for_frame(bybit_frame).venue == VenueV2.BYBIT
    assert plan.key_for_frame(binance_frame).venue == VenueV2.BINANCE
    bybit_trade = source.plan.parse_frame(bybit_frame, processed_at_ns=301)[0]
    binance_trade = source.plan.parse_frame(binance_frame, processed_at_ns=301)[0]
    assert bybit_trade.aggressor_side == "BUY"
    assert binance_trade.aggressor_side == "SELL"
    assert binance_trade.side_convention == "BINANCE_m_TRUE_BUYER_MAKER_SELLER_AGGRESSOR"


def test_binance_depth_frame_preserves_pu_update_range_without_claiming_book():
    plan = _plan()
    channel = "btcusdt@depth@100ms"
    frame = _frame(VenueV2.BINANCE, "BINANCE_DEPTH_PUBLIC_WS_BROAD_V2", channel,
                   b'{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1000,"U":11,"u":15,"pu":10,"b":[["10","2"]],"a":[["11","3"]]}}')
    delta, = plan.parse_frame(frame, processed_at_ns=301)
    assert delta.sequence_semantics == "BINANCE_U_PU"
    assert delta.first_update_id == 11 and delta.last_update_id == 15 and delta.previous_update_id == 10
    assert not isinstance(delta, type(None))


def test_stream_plan_fails_closed_on_deep_fanout_and_missing_product_revision():
    products = tuple(_product(VenueV2.BYBIT, f"C{i}USDT") for i in range(9))
    tiers = {item.key: ComputeTierV2.TIER_3 for item in products}
    with pytest.raises(ValueError, match="deep subscription population"):
        BroadPublicStreamPlanV2.build(products, tiers, created_at_ns=200)
    with pytest.raises(ValueError, match="active USDT linear"):
        BroadPublicStreamPlanV2.build(products[:1], {products[0].key: ComputeTierV2.TIER_3,
                                                     _product(VenueV2.BYBIT, "OTHERUSDT").key: ComputeTierV2.TIER_3},
                                      created_at_ns=1)


def test_runtime_binance_snapshot_worker_bridges_buffered_actual_diff(tmp_path):
    product = _product(VenueV2.BINANCE, "BTCUSDT")
    now = 1_800_000_000_000
    channel = "btcusdt@depth@100ms"
    source_id = "BINANCE_DEPTH_PUBLIC_WS_BROAD_V2"
    frame = _frame(VenueV2.BINANCE, source_id, channel,
                   b'{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1799999999999,"U":9,"u":11,"pu":8,"b":[["100","2"]],"a":[["101","3"]]}}')
    health = PublicSourceHealthV2(source_id, now - 10, now - 9,
        PublicSourceStateV2.HEALTHY_CURRENT, "health-ref", "fixture current")
    delta = parse_binance_depth_frame(frame, instrument=product.key,
        source_health=health.state.value, source_health_ref=health.content_hash,
        processed_at_ns=now - 9)
    assert not isinstance(delta, tuple)
    plan = BroadPublicStreamPlanV2.build((product,), {product.key: ComputeTierV2.TIER_3},
        created_at_ns=now)
    raw_snapshot = b'{"lastUpdateId":10,"bids":[["100","2"]],"asks":[["101","3"]]}'
    runtime = BroadPublicRuntimeV2(snapshot_reader=lambda _key, _at: (raw_snapshot, now - 5))
    runtime.plan = plan
    runtime.source = SimpleNamespace(status=lambda: SimpleNamespace(lanes={"BINANCE_DEPTH": _lane(now)}))
    runtime._stream_epoch = "fixture-epoch"
    runtime._products = {product.key: product}
    runtime._sequence_books[product.key] = SequenceValidBookV2(
        instrument=product.key, source_id=source_id, channel=channel,
        sequence_semantics="BINANCE_U_PU")
    runtime._trackers[(product.key, channel)] = PublicStreamContinuityTrackerV1(
        instrument=product.key, source_id=source_id, channel=channel,
        metadata_ref=product.metadata_ref, epoch_id="fixture-epoch:BINANCE:" + channel)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        runtime._queue_snapshot_delta(repository, product, frame, delta, now - 9)
        assert runtime._snapshot_future is not None
        runtime._snapshot_future.result(timeout=2)
        grouped = {}
        runtime._poll_snapshot(repository, now_ns=now, grouped=grouped)
        book = runtime.sequence_books[product.key]
        assert book.last_update_id == 11, (runtime._snapshot_failure, book.sequence_state, runtime._snapshot_buffer)
        assert book.sequence_state.state.value in {"WARMING", "VALID"}
        assert any(rows[0].frame_type == "REST_SNAPSHOT" for rows in grouped.values())


def test_runtime_persists_typed_trade_identity_and_receipt_continuity(tmp_path):
    product = _product(VenueV2.BYBIT, "BTCUSDT")
    now = 2_000
    plan = BroadPublicStreamPlanV2.build((product,), {product.key: ComputeTierV2.TIER_3}, created_at_ns=now)
    frame = _frame(VenueV2.BYBIT, "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT",
                   b'{"topic":"publicTrade.BTCUSDT","data":[{"i":"trade-1","S":"Buy","p":"10","v":"2","T":1}]}')
    runtime = BroadPublicRuntimeV2()
    runtime.plan = plan
    runtime._stream_epoch = "fixture-epoch"
    runtime._products = {product.key: product}
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        runtime._archive = L2FrameArchiveV2(tmp_path / "archive", repository)
        runtime._interpret_frames(repository, (frame,), now_ns=now)
        entries = repository.artifact_entries("BroadPublicStreamContinuityV2")
        assert len(entries) == 2
        assert any(entry.metadata["decision"]["classification"] == "TRADE_ACCEPTED"
                   for entry in entries)
        assert any(entry.metadata["observation"]["trade_id"] == "trade-1" for entry in entries)
        trade_row = next(entry.metadata["observation"] for entry in entries
                         if entry.metadata["observation"]["trade_id"] == "trade-1")
        assert trade_row["trade_payload_hash_basis"] == "CANONICALIZED_PER_TRADE_RAW_ROW_BYTES"
        assert trade_row["trade_payload_hash"] == sha256_json(
            {"i": "trade-1", "S": "Buy", "p": "10", "v": "2", "T": 1})


def _runtime_fixture(tmp_path, *, snapshot_reader=None):
    product = _product(VenueV2.BINANCE, "BTCUSDT")
    channel = "btcusdt@depth@100ms"
    runtime = BroadPublicRuntimeV2(snapshot_reader=snapshot_reader)
    runtime.plan = BroadPublicStreamPlanV2.build((product,), {product.key: ComputeTierV2.TIER_3},
                                               created_at_ns=time.time_ns())
    runtime._products = {product.key: product}
    runtime._stream_epoch = "fixture-epoch"
    lane = _lane(time.time_ns())
    runtime.source = SimpleNamespace(status=lambda: SimpleNamespace(lanes={"BINANCE_DEPTH": lane}))
    runtime.capture = SimpleNamespace(status=lambda: SimpleNamespace(
        state="RUNNING", attempt_count=lane.attempt_count, handoff=lane.handoff, capture={},
        pending_frames=0, lanes={"BINANCE_DEPTH": lane}), close=lambda: None)
    at_ns = time.time_ns()
    payload = (b'{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate",'
               b'"E":1,"U":9,"u":11,"pu":8,"b":[["100","2"]],"a":[["101","3"]]}}')
    frame = CapturedPublicFrameV2(VenueV2.BINANCE, "BINANCE_DEPTH_PUBLIC_WS_BROAD_V2", channel,
        payload, hashlib.sha256(payload).hexdigest(), at_ns, at_ns, 1)
    return runtime, product, frame, lane


@pytest.mark.parametrize("stall_seconds", [0.1, 0.5, 1.0, 3.0, 5.0])
def test_snapshot_stall_preserves_actual_service_cadence_and_causal_adoption(tmp_path, stall_seconds):
    released = threading.Event()
    raw_snapshot = b'{"lastUpdateId":10,"bids":[["100","2"]],"asks":[["101","3"]]}'
    def reader(_key, _at):
        released.wait(8)
        return raw_snapshot, time.time_ns()
    elapsed_calls = []
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        runtime, product, frame, lane = _runtime_fixture(tmp_path, snapshot_reader=reader)
        runtime._archive = L2FrameArchiveV2(tmp_path / "archive", repository)
        try:
            runtime._interpret_frames(repository, (frame,), now_ns=time.time_ns())
            assert runtime._snapshot_future is not None
            future = runtime._snapshot_future
            started = time.monotonic()
            while time.monotonic() - started < stall_seconds:
                now = time.time_ns()
                lane.handoff.last_activity_at_ns = now
                call_start = time.monotonic()
                runtime.service(repository, now_ns=now)
                elapsed_calls.append(time.monotonic() - call_start)
                time.sleep(0.01)
            released.set()
            future.result(timeout=2)
            adoption = time.time_ns()
            lane.handoff.last_activity_at_ns = adoption
            runtime.service(repository, now_ns=adoption)
            assert runtime._service_calls >= 5
            assert max(elapsed_calls) < 0.1
            book = runtime.sequence_books[product.key]
            if stall_seconds < 5:
                assert runtime._snapshot_failure is None
                assert book.last_update_id == 11
                assert book.sequence_state.state_changed_at_ns >= adoption
                archive_entries = repository.artifact_entries("L2FrameArchiveCheckpointV2")
                assert archive_entries
            else:
                assert "TIMEOUT" in runtime._snapshot_failure
                assert book.last_update_id is None
        finally:
            released.set()
            runtime.close()
            if runtime._snapshot_executor is not None:
                runtime._snapshot_executor.shutdown(wait=True)


@pytest.mark.parametrize("failure", ["epoch", "overflow", "deadline"])
def test_invalidated_snapshot_cannot_seed_a_book_after_worker_completion(tmp_path, failure):
    pending = concurrent.futures.Future()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        runtime, product, frame, lane = _runtime_fixture(tmp_path)
        runtime._sequence_books[product.key] = SequenceValidBookV2(instrument=product.key,
            source_id=frame.source_id, channel=frame.channel, sequence_semantics="BINANCE_U_PU")
        runtime._trackers[(product.key, frame.channel)] = PublicStreamContinuityTrackerV1(
            instrument=product.key, source_id=frame.source_id, channel=frame.channel,
            metadata_ref=product.metadata_ref, epoch_id="fixture")
        delta = parse_binance_depth_frame(frame, instrument=product.key, processed_at_ns=frame.available_at_ns)
        runtime._snapshot_future = pending
        runtime._snapshot_binding = (runtime.plan.plan_id, product.key, product.metadata_ref, 1)
        runtime._snapshot_buffer = [(product, frame, delta)]
        runtime._snapshot_started_monotonic_ns = time.monotonic_ns()
        if failure == "epoch":
            lane.attempt_count = 2
        elif failure == "overflow":
            runtime._snapshot_failure = "BINANCE_SNAPSHOT_BRIDGE_BUFFER_OVERFLOW"
        else:
            runtime._snapshot_started_monotonic_ns -= 5_000_000_000
        pending.set_result((b'{"lastUpdateId":10,"bids":[["100","2"]],"asks":[["101","3"]]}',
                            time.time_ns()))
        runtime._poll_snapshot(repository, now_ns=time.time_ns(), grouped={})
        assert runtime.sequence_books[product.key].last_update_id is None
        assert runtime._snapshot_failure


def test_progress_reads_actual_lane_health_and_does_not_retain_stale_current_claim(tmp_path):
    runtime, _, _, lane = _runtime_fixture(tmp_path)
    assert runtime.progress_snapshot()["stream_source_state"] == "HEALTHY_CURRENT"
    lane.handoff.last_activity_at_ns = time.time_ns() - 5_000_000_001
    progress = runtime.progress_snapshot()
    assert progress["stream_recovery_required"]
    assert progress["stream_source_states"] == {"BINANCE_DEPTH": "INCOMPLETE_SNAPSHOT"}
    lane.handoff.connected = False
    assert runtime.progress_snapshot()["stream_source_state"] != "HEALTHY_CURRENT"


def test_actual_connection_epoch_change_invalidates_even_apparently_contiguous_book(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        runtime, product, frame, lane = _runtime_fixture(tmp_path,
            snapshot_reader=lambda *_args: (b'{"lastUpdateId":10,"bids":[["100","2"]],"asks":[["101","3"]]}',
                                          time.time_ns()))
        runtime._archive = L2FrameArchiveV2(tmp_path / "archive", repository)
        try:
            runtime._interpret_frames(repository, (frame,), now_ns=time.time_ns())
            runtime._snapshot_future.result(timeout=2)
            runtime.service(repository, now_ns=time.time_ns())
            assert runtime.sequence_books[product.key].last_update_id == 11
            lane.attempt_count = 2
            updated = replace(frame, connection_epoch=2, received_at_ns=time.time_ns(), available_at_ns=time.time_ns())
            runtime._interpret_frames(repository, (updated,), now_ns=time.time_ns())
            assert runtime._snapshot_future is not None
            entries = repository.artifact_entries("BroadPublicStreamContinuityV2")
            assert any(entry.metadata["observation"]["kind"] == "RECONNECT" for entry in entries)
        finally:
            runtime.close()
            if runtime._snapshot_executor is not None:
                runtime._snapshot_executor.shutdown(wait=True)
