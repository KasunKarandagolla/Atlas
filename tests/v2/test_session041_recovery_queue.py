from __future__ import annotations

import hashlib
import threading
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.broad_stream_source import BroadPublicStreamPlanV2, BroadPublicStreamSourceV2
from atlas.v2.data.public_microstructure_ws import (
    BoundedPublicFrameHandoffV2,
    CapturedPublicFrameV2,
    SharedPublicFrameBudgetSnapshotV2,
    SharedPublicFrameBudgetV1,
)
from atlas.v2.data.universe import ComputeTierV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

WAIT_SECONDS = 3.0
BYBIT_TOPIC = "publicTrade.BTCUSDT"


def _await(event: threading.Event) -> None:
    assert event.wait(WAIT_SECONDS), "deterministic queue barrier timed out"


def _run_thread(call):
    result: dict[str, object] = {}
    errors: list[BaseException] = []

    def run() -> None:
        try:
            result["value"] = call()
        except BaseException as exc:  # propagate worker failures in the test thread
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result, errors


def _join_thread(thread: threading.Thread, errors: list[BaseException]) -> None:
    thread.join(timeout=WAIT_SECONDS)
    assert not thread.is_alive(), "queue worker did not finish within the bounded join"
    assert not errors, f"queue worker failed: {errors[0]!r}"


def _frame(payload: bytes = b"x", *, topic: str = BYBIT_TOPIC) -> CapturedPublicFrameV2:
    return CapturedPublicFrameV2(
        VenueV2.BYBIT,
        "BYBIT_PUBLIC_WS_BROAD_V2",
        topic,
        payload,
        hashlib.sha256(payload).hexdigest(),
        1_800_000_000_000,
        1_800_000_000_000,
        1,
    )


def _budget(*, items: int, size: int, floor_items: int = 0, floor_bytes: int = 0):
    return SharedPublicFrameBudgetV1(
        max_items=items,
        max_bytes=size,
        reserve_items_per_lane=floor_items,
        reserve_bytes_per_lane=floor_bytes,
    )


def _handoff(budget: SharedPublicFrameBudgetV1, lane: str, *, items: int | None = None,
             size: int | None = None) -> BoundedPublicFrameHandoffV2:
    return BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT,
        topics=(BYBIT_TOPIC,),
        max_queue_items=budget.max_items if items is None else items,
        max_queue_bytes=budget.max_bytes if size is None else size,
        shared_budget=budget,
        budget_lane=lane,
    )


def _snapshot_lane(snapshot: SharedPublicFrameBudgetSnapshotV2, field: str, lane: str) -> int:
    return dict(getattr(snapshot, field))[lane]


def _rejections(snapshot: SharedPublicFrameBudgetSnapshotV2) -> dict[str, int]:
    return dict(snapshot.rejection_counts)


def _product(venue: VenueV2, symbol: str) -> ProductContractV2:
    revision = sha256_json({"venue": venue.value, "symbol": symbol})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, symbol.removesuffix("USDT"), "USDT", "USDT", revision)
    return ProductContractV2(key, 100, 100, 100, Decimal("1"), Decimal("0.01"),
                             Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)


def _broad_source() -> BroadPublicStreamSourceV2:
    bybit = _product(VenueV2.BYBIT, "BTCUSDT")
    binance = _product(VenueV2.BINANCE, "BTCUSDT")
    plan = BroadPublicStreamPlanV2.build(
        (bybit, binance),
        {bybit.key: ComputeTierV2.TIER_3, binance.key: ComputeTierV2.TIER_3},
        created_at_ns=1_800_000_000_000,
        benchmark_keys=(bybit.key, binance.key),
    )
    return BroadPublicStreamSourceV2(
        plan,
        stream_factories={name: (lambda: iter(())) for name in plan.lane_topics},
    )


def test_snapshot_v2_is_immutable_and_v1_tuple_api_is_preserved():
    budget = _budget(items=8, size=32)
    _handoff(budget, "a")
    snap = budget.snapshot_v2()

    assert isinstance(snap, SharedPublicFrameBudgetSnapshotV2)
    assert snap.generation == 1
    assert snap.sampled_at_ns > 0
    assert snap.reserved_items == snap.reserved_bytes == 0
    assert snap.max_items == 8 and snap.max_bytes == 32
    assert snap.lane_reserved_items == (("a", 0),)
    assert snap.lane_reserved_bytes == (("a", 0),)
    assert snap.lane_reservation_floor_items == (("a", 0),)
    assert snap.lane_reservation_floor_bytes == (("a", 0),)
    assert snap.rejection_counts == (("APPEND_FAILURE", 0), ("LOCAL_LIMIT", 0), ("SHARED_LIMIT", 0))
    assert not snap.loss_latched and snap.loss_reasons == ()
    with pytest.raises(FrozenInstanceError):
        snap.generation = 9  # type: ignore[misc]

    assert budget.snapshot() == (0, 0, 0, 0, 8)


@pytest.mark.parametrize("operation", ["offer", "drain"])
def test_broad_status_does_not_compare_lane_diagnostics_to_budget_snapshot(operation: str):
    source = _broad_source()
    lane = source._lanes["BYBIT"]
    handoff = lane._handoff
    if operation == "drain":
        assert handoff.offer(_frame())
    prior_lane_items = handoff.snapshot().queue_items

    sampled = threading.Event()
    resume = threading.Event()
    original_status = lane.status

    def paused_status():
        status = original_status()
        sampled.set()
        _await(resume)
        return status

    lane.status = paused_status
    thread, result, errors = _run_thread(source.status)
    try:
        _await(sampled)
        if operation == "offer":
            assert handoff.offer(_frame(b"y"))
        else:
            assert len(handoff.drain(max_items=1)) == 1
    finally:
        resume.set()
        _join_thread(thread, errors)

    status = result["value"]
    assert status.lanes["BYBIT"].handoff.queue_items == prior_lane_items
    # Aggregate occupancy is sampled once from the shared budget. It can be
    # just before or just after the concurrent mutation, without an invariant
    # error from independently sampled lane diagnostics.
    expected = (0, 1) if operation == "offer" else (1, 0)
    assert status.handoff.queue_items in expected
    assert status.handoff.queue_items <= status.handoff.max_queue_items
    assert status.handoff.queue_bytes <= status.handoff.max_queue_bytes
    assert status.handoff.generation > 0 and status.handoff.sampled_at_ns > 0
    assert status.handoff.lane_status_is_diagnostic
    assert status.handoff.lane_diagnostic_sampled_at_ns > 0


def test_reservation_is_visible_while_producer_is_paused_before_append():
    budget = _budget(items=4, size=16)
    handoff = _handoff(budget, "a")
    reserved = threading.Event()
    resume = threading.Event()
    original_reserve = budget.reserve

    def paused_reserve(lane: str, frame_bytes: int) -> bool:
        accepted = original_reserve(lane, frame_bytes)
        if accepted:
            reserved.set()
            _await(resume)
        return accepted

    budget.reserve = paused_reserve
    thread, result, errors = _run_thread(lambda: handoff.offer(_frame(b"abc")))
    try:
        _await(reserved)
        snap = budget.snapshot_v2()
        assert snap.reserved_items == 1 and snap.reserved_bytes == 3
        assert _snapshot_lane(snap, "lane_reserved_items", "a") == 1
        assert _snapshot_lane(snap, "lane_reserved_bytes", "a") == 3
        # Reservation is conservative occupancy even though deque append has
        # not run yet and the lane diagnostic lock remains held.
        assert len(handoff._queue) == 0
    finally:
        resume.set()
        _join_thread(thread, errors)

    assert result["value"] is True
    assert handoff.snapshot().queue_items == 1
    assert budget.snapshot_v2().reserved_items == 1


def test_popped_frame_remains_reserved_until_release_completes():
    budget = _budget(items=4, size=16)
    handoff = _handoff(budget, "a")
    assert handoff.offer(_frame(b"abc"))
    releasing = threading.Event()
    resume = threading.Event()
    original_release = budget.release

    def paused_release(lane: str, frame_bytes: int) -> None:
        releasing.set()
        _await(resume)
        original_release(lane, frame_bytes)

    budget.release = paused_release
    thread, result, errors = _run_thread(lambda: handoff.drain(max_items=1))
    try:
        _await(releasing)
        snap = budget.snapshot_v2()
        assert snap.reserved_items == 1 and snap.reserved_bytes == 3
        assert _snapshot_lane(snap, "lane_reserved_items", "a") == 1
        # The physical pop has happened, but release has not. The snapshot
        # reports conservative reservation occupancy, not deque length.
        assert len(handoff._queue) == 0
    finally:
        resume.set()
        _join_thread(thread, errors)

    assert len(result["value"]) == 1
    assert budget.snapshot_v2().reserved_items == 0
    assert handoff.snapshot().queue_items == 0


def test_failed_append_rolls_back_reservation_and_latches_append_loss():
    class FailingAppendQueue:
        def __len__(self) -> int:
            return 0

        def append(self, _frame: CapturedPublicFrameV2) -> None:
            raise OSError("injected append failure")

    budget = _budget(items=4, size=16)
    handoff = _handoff(budget, "a")
    handoff._queue = FailingAppendQueue()  # type: ignore[assignment]

    with pytest.raises(OSError, match="injected append failure"):
        handoff.offer(_frame(b"abc"))

    snap = budget.snapshot_v2()
    assert snap.reserved_items == snap.reserved_bytes == 0
    assert snap.high_water_items == 1 and snap.high_water_bytes == 3
    assert _snapshot_lane(snap, "lane_reserved_items", "a") == 0
    assert _rejections(snap)["APPEND_FAILURE"] == 1
    assert snap.loss_latched and snap.loss_reasons == ("APPEND_FAILURE",)
    lane_status = handoff.snapshot()
    assert lane_status.frames_rejected == 1 and lane_status.overflowed

    # A standalone lane has no shared budget, but its local lifetime-loss
    # signal must still retain an unexpected append failure.
    local = BoundedPublicFrameHandoffV2(venue=VenueV2.BYBIT, topics=(BYBIT_TOPIC,))
    local._queue = FailingAppendQueue()  # type: ignore[assignment]
    with pytest.raises(OSError, match="injected append failure"):
        local.offer(_frame(b"abc"))
    local_status = local.snapshot()
    assert local_status.frames_rejected == 1 and local_status.overflowed


def test_local_and_shared_rejections_are_counted_separately_and_stay_latched_after_drain():
    local_budget = _budget(items=4, size=16)
    local = _handoff(local_budget, "local", items=1)
    assert local.offer(_frame())
    assert not local.offer(_frame(b"y"))
    local_snap = local_budget.snapshot_v2()
    assert _rejections(local_snap)["LOCAL_LIMIT"] == 1
    assert local_snap.loss_latched and local_snap.loss_reasons == ("LOCAL_LIMIT",)
    local.drain(max_items=1)
    after_drain = local_budget.snapshot_v2()
    assert after_drain.loss_latched and after_drain.loss_reasons == ("LOCAL_LIMIT",)
    assert after_drain.reserved_items == 0
    assert after_drain.generation > local_snap.generation

    shared_budget = _budget(items=2, size=16)
    shared = _handoff(shared_budget, "shared", items=4)
    assert shared.offer(_frame())
    assert shared.offer(_frame(b"y"))
    assert not shared.offer(_frame(b"z"))
    shared_snap = shared_budget.snapshot_v2()
    assert _rejections(shared_snap)["SHARED_LIMIT"] == 1
    assert shared_snap.loss_latched and shared_snap.loss_reasons == ("SHARED_LIMIT",)


def test_byte_bound_is_enforced_at_exact_shared_capacity():
    budget = _budget(items=8, size=10, floor_bytes=2)
    lane_a = _handoff(budget, "a")
    lane_b = _handoff(budget, "b")

    assert lane_a.offer(_frame(b"12345678"))
    assert lane_b.offer(_frame(b"12"))
    assert not lane_b.offer(_frame(b"x"))
    snap = budget.snapshot_v2()
    assert snap.reserved_items == 2 and snap.reserved_bytes == 10
    assert snap.reserved_items <= snap.max_items and snap.reserved_bytes <= snap.max_bytes
    assert snap.high_water_bytes == snap.max_bytes
    assert _rejections(snap)["SHARED_LIMIT"] == 1
    assert dict(snap.lane_reservation_floor_bytes) == {"a": 2, "b": 2}


def test_idle_lane_preserves_exact_480_32_item_reserve_at_frozen_global_limit():
    budget = _budget(items=512, size=16_000_000, floor_items=32, floor_bytes=1_000_000)
    busy = _handoff(budget, "busy")
    idle = _handoff(budget, "idle")
    frame = _frame()

    assert all(busy.offer(frame) for _ in range(480))
    assert not busy.offer(frame)
    assert all(idle.offer(frame) for _ in range(32))
    snap = budget.snapshot_v2()

    assert snap.reserved_items == 512 and snap.max_items == 512
    assert snap.reserved_bytes == 512 and snap.max_bytes == 16_000_000
    assert snap.high_water_items == 512 and snap.high_water_bytes == 512
    assert dict(snap.lane_reserved_items) == {"busy": 480, "idle": 32}
    assert dict(snap.lane_reservation_floor_items) == {"busy": 32, "idle": 32}
    assert _rejections(snap)["SHARED_LIMIT"] == 1
    assert snap.loss_latched and snap.loss_reasons == ("SHARED_LIMIT",)


def test_two_lanes_compete_atomically_for_the_last_item_and_byte():
    budget = _budget(items=8, size=8, floor_items=2, floor_bytes=2)
    _handoff(budget, "a")
    _handoff(budget, "b")
    for _ in range(5):
        assert budget.reserve("a", 1)
    for _ in range(2):
        assert budget.reserve("b", 1)

    start = threading.Barrier(3)
    results: list[bool | None] = [None, None]
    errors: list[BaseException] = []

    def contend(index: int, lane: str) -> None:
        try:
            start.wait(timeout=WAIT_SECONDS)
            results[index] = budget.reserve(lane, 1)
        except BaseException as exc:
            errors.append(exc)

    workers = [threading.Thread(target=contend, args=(0, "a"), daemon=True),
               threading.Thread(target=contend, args=(1, "b"), daemon=True)]
    for worker in workers:
        worker.start()
    start.wait(timeout=WAIT_SECONDS)
    for worker in workers:
        _join_thread(worker, errors)

    snap = budget.snapshot_v2()
    assert sorted(results) == [False, True]
    assert snap.reserved_items == snap.reserved_bytes == 8
    assert snap.reserved_items <= snap.max_items and snap.reserved_bytes <= snap.max_bytes
    assert dict(snap.lane_reserved_items)["a"] + dict(snap.lane_reserved_items)["b"] == 8
    assert min(dict(snap.lane_reserved_items).values()) >= 2
    assert _rejections(snap)["SHARED_LIMIT"] == 1
    assert snap.loss_latched and snap.loss_reasons == ("SHARED_LIMIT",)


def test_simultaneous_reserve_and_drain_finish_with_exact_nonnegative_counts():
    budget = _budget(items=4, size=16)
    handoff = _handoff(budget, "a")
    assert handoff.offer(_frame(b"ab"))
    start = threading.Barrier(3)
    result: dict[str, object] = {}
    errors: list[BaseException] = []

    def produce() -> None:
        try:
            start.wait(timeout=WAIT_SECONDS)
            result["offered"] = handoff.offer(_frame(b"ab"))
        except BaseException as exc:
            errors.append(exc)

    def drain() -> None:
        try:
            start.wait(timeout=WAIT_SECONDS)
            result["drained"] = handoff.drain(max_items=1)
        except BaseException as exc:
            errors.append(exc)

    producer = threading.Thread(target=produce, daemon=True)
    drainer = threading.Thread(target=drain, daemon=True)
    producer.start()
    drainer.start()
    start.wait(timeout=WAIT_SECONDS)
    _join_thread(producer, errors)
    _join_thread(drainer, errors)

    snap = budget.snapshot_v2()
    assert result["offered"] is True
    assert len(result["drained"]) == 1
    assert snap.reserved_items == 1 and snap.reserved_bytes == 2
    assert _snapshot_lane(snap, "lane_reserved_items", "a") == 1
    assert _snapshot_lane(snap, "lane_reserved_bytes", "a") == 2
    assert handoff.snapshot().queue_items == 1


def test_budget_underflow_and_corrupt_totals_raise():
    budget = _budget(items=4, size=16)
    _handoff(budget, "a")
    with pytest.raises(RuntimeError, match="underflow"):
        budget.release("a", 1)

    with budget._lock:
        budget._items = 1
    with pytest.raises(RuntimeError, match="invariants"):
        budget.snapshot_v2()
