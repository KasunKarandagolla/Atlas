import asyncio
import queue
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest
from support.assisted_control_fixture import make_plan

from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, make_command
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.binance_demo import BinanceDemoIdentity, DemoCredential
from atlas.runtime.binance_native import BinanceNativeEvent, BinanceNativeNode
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)


@dataclass(frozen=True)
class Snapshot:
    identity_hash: str
    eligible: bool
    captured_at_ns: int


def _product():
    return ProductContractV2(
        InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.DEMO, ProductTypeV2.LINEAR_PERPETUAL,
                        "SOLUSDT", "SOL", "USDT", "USDT", "rev-1"),
        1_800_000_000_000_000_000, 1_800_000_000_000_000_000, 1_800_000_000_000_000_000,
        Decimal("1"), Decimal("0.01"), Decimal("0.01"), Decimal("0.01"), TradingStatusV2.TRADING,
        "fixture", min_notional=Decimal("5"), max_qty=Decimal("100"),
    )


def _node(journal, *, port_factory=None, node_builder_factory=None):
    identity = BinanceDemoIdentity("scope-demo", "cred-demo")
    now = 1_800_000_000_000_000_000
    snapshots = []

    def get_snapshot():
        snapshots.append(now)
        return Snapshot(identity.content_hash, True, now)

    runtime = BinanceNativeNode(
        identity=identity,
        credential=DemoCredential("test-key", "test-secret"),
        product=_product(),
        journal=journal,
        writer_epoch=1,
        assert_writer=lambda: None,
        reconcile_once=lambda: None,
        account_snapshot_getter=get_snapshot,
        market_filters=object(),
        port_factory=port_factory or (lambda *_args: object()),
        node_builder_factory=node_builder_factory,
    )
    return runtime, identity, snapshots


def test_actual_pinned_live_node_builds_without_starting_or_reading_account(journal):
    runtime, _identity, snapshots = _node(journal)
    assert runtime.node.is_running is False
    assert snapshots == []
    assert runtime.command_queue.maxsize == 32
    assert runtime.event_queue.maxsize == 256


def test_close_all_repair_resolves_only_the_exact_open_native_position():
    from nautilus_trader.model import AccountId, InstrumentId, PositionId

    from atlas.runtime.binance_native import _StrategyHost

    instrument_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    account_id = "BINANCE-scope-demo"
    position = SimpleNamespace(
        id=PositionId("P-20261007-000001"), instrument_id=instrument_id,
        account_id=AccountId(account_id), signed_qty=2.0,
    )
    cache = SimpleNamespace(
        positions_open=lambda **kwargs: [position],
        is_position_open=lambda position_id: position_id == position.id,
    )
    host = _StrategyHost(SimpleNamespace(cache=cache))
    assert host.resolve_position_id(
        instrument_id=instrument_id,
        account_id=account_id,
        expected_signed_quantity=Decimal("2"),
    ) == position.id

    mismatch = _StrategyHost(SimpleNamespace(cache=SimpleNamespace(
        positions_open=lambda **kwargs: [position],
        is_position_open=lambda position_id: True,
    )))
    with pytest.raises(PersistenceError, match="IDENTITY_OR_QUANTITY_MISMATCH"):
        mismatch.resolve_position_id(
            instrument_id=instrument_id,
            account_id=account_id,
            expected_signed_quantity=Decimal("1"),
        )


def test_close_all_repair_configures_only_binance_full_position_risk_eligibility(journal):
    captured = {}

    class BuiltNode:
        def add_strategy(self, _strategy):
            return None

        def handle(self):
            return SimpleNamespace()

    class Builder:
        def with_risk_engine_config(self, config):
            captured["risk_config"] = config
            return self

        def add_exec_client(self, *_args):
            return self

        def with_reconciliation(self, _enabled):
            return self

        def build(self):
            return BuiltNode()

    runtime, _, _ = _node(journal, node_builder_factory=lambda *_args: Builder())
    from nautilus_trader.adapters.binance import BINANCE_VENUE

    assert captured["risk_config"].full_position_exit_venues == [BINANCE_VENUE]


def test_journal_command_queue_is_deduplicated_and_opening_refused(journal):
    runtime, identity, _ = _node(journal)
    make_plan(journal, plan_id="plan")
    intent = Intent(
        "native-intent", 0, "plan", "v1", "a" * 32, 1, LifecycleState.SUBMITTING,
        ProtectionStatus.UNCONFIRMED, ReconciliationHealth.STALE, 1_800_000_000_000_000_000,
    )
    journal.create_intent_with_reservation(
        intent,
        Reservation("native-res", intent.intent_id, Decimal("1"), Decimal("0"), Decimal("0"),
                    Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")),
    )
    journal.persist_command(make_command(
        command_id="native-exit", intent_id=intent.intent_id, command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": intent.client_order_id, "symbol": "SOLUSDT"},
        expected_state_version=0, created_at_ns=intent.created_at_ns,
    ))
    runtime.enqueue_command("native-exit")
    runtime.enqueue_command("native-exit")
    assert runtime.command_queue.qsize() == 1
    journal.persist_command(make_command(
        command_id="native-open", intent_id=intent.intent_id, command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"client_order_id": intent.client_order_id},
        expected_state_version=0, created_at_ns=intent.created_at_ns,
    ))
    with pytest.raises(PersistenceError, match="command type refused"):
        runtime.enqueue_command("native-open")
    assert runtime.identity is identity


def test_event_overflow_latches_stop_without_leaking_raw_event_text(journal):
    runtime, _identity, _ = _node(journal)
    row = BinanceNativeEvent("OrderFilled", "a" * 32, "venue-id", "SOLUSDT-PERP.BINANCE", "FILLED",
                             "1", "120", 1_800_000_000_000_000_000)
    for _ in range(256):
        runtime._push_event(row)
    assert runtime.queue_overflow is False
    runtime._push_event(row)
    assert runtime.queue_overflow is True and runtime.stopped is True
    with pytest.raises(PersistenceError, match="runtime is stopped"):
        runtime.enqueue_command("missing")
    assert runtime.event_queue.get_nowait() == row
    assert runtime.event_queue.qsize() == 255


def test_native_dispatch_refuses_missing_typed_proof_before_unknown_or_effect(journal):
    make_plan(journal, plan_id="plan")
    now = 1_800_000_000_000_000_000
    intent = Intent(
        "native-intent", 0, "plan", "v1", "b" * 32, 1, LifecycleState.SUBMITTING,
        ProtectionStatus.UNCONFIRMED, ReconciliationHealth.STALE, now,
    )
    journal.create_intent_with_reservation(
        intent,
        Reservation("native-res", intent.intent_id, Decimal("1"), Decimal("0"), Decimal("0"),
                    Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")),
    )
    command = make_command(
        command_id="native-exit", intent_id=intent.intent_id, command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": intent.client_order_id, "symbol": "SOLUSDT"},
        expected_state_version=0, created_at_ns=now,
    )
    journal.persist_command(command)
    fence_calls = []

    class Port:
        def dispatch(self, command, *, now_ns):
            fence_calls.append(("effect", journal.load_command(command.command_id).outcome.value, now_ns))

    runtime, _identity, _ = _node(journal)
    runtime.assert_writer = lambda: fence_calls.append(("fence", None, None))
    runtime.enqueue_command(command.command_id)
    runtime._dispatch(command.command_id, Port(), now)
    assert [item[0] for item in fence_calls] == ["fence", "fence"]
    assert journal.load_command(command.command_id).outcome.value == "UNSENT"
    assert runtime.readiness_reason == "BINANCE_NATIVE_COMMAND_UNRESOLVED"


def _native_fill(client_id="c" * 32, *, trade_id="77", quantity="0.2", event_time=1_800_000_000_000_000_000):
    from nautilus_trader import model
    from nautilus_trader.core import UUID4

    return model.OrderFilled(
        trader_id=model.TraderId("ATLAS-BINANCE-DEMO"),
        strategy_id=model.StrategyId("BinanceDemoStrategy-000"),
        instrument_id=model.InstrumentId.from_str("SOLUSDT-PERP.BINANCE"),
        client_order_id=model.ClientOrderId(client_id), venue_order_id=model.VenueOrderId("101"),
        account_id=model.AccountId("BINANCE-scope-demo"), trade_id=model.TradeId(trade_id),
        order_side=model.OrderSide.BUY, order_type=model.OrderType.LIMIT,
        last_qty=model.Quantity.from_str(quantity), last_px=model.Price.from_str("120.00"),
        currency=model.Currency.from_str("USDT"), liquidity_side=model.LiquiditySide.TAKER,
        event_id=UUID4(), ts_event=event_time, ts_init=event_time, reconciliation=False,
        commission=model.Money.from_str("0.10 USDT"),
    )


def _persist_native_intent(journal, client_id="c" * 32):
    make_plan(journal, plan_id="plan")
    now = 1_800_000_000_000_000_000
    intent = Intent("typed-intent", 0, "plan", "v1", client_id, 1, LifecycleState.SUBMITTING,
                    ProtectionStatus.UNCONFIRMED, ReconciliationHealth.STALE, now)
    journal.create_intent_with_reservation(intent, Reservation(
        "typed-res", intent.intent_id, Decimal("1"), *(Decimal("0") for _ in range(6)),
    ))
    journal.persist_command(make_command(
        command_id="typed-command", intent_id=intent.intent_id, command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": client_id, "symbol": "SOLUSDT",
                      "identity_hash": BinanceDemoIdentity("scope-demo", "cred-demo").content_hash,
                      "instrument_ref": _product().content_hash},
        expected_state_version=0, created_at_ns=now,
    ))
    return intent


def test_actual_sdk_fill_capture_persists_partial_and_cancel_race_before_publication(journal):
    from nautilus_trader import model
    from nautilus_trader.core import UUID4

    runtime, identity, _ = _node(journal)
    intent = _persist_native_intent(journal)
    now = intent.created_at_ns
    order = SimpleNamespace(status=model.OrderStatus.PARTIALLY_FILLED,
                            filled_qty=model.Quantity.from_str("0.2"), avg_px=model.Price.from_str("120"))
    fill = _native_fill(event_time=now - 10)
    runtime.capture_native_event(fill, now, order=order)
    assert journal.load_execution_evidence() == []
    published = runtime.drain_events_to_journal()
    assert len(published) == 1 and published[0].quantity == "0.2"
    assert published[0].status == "PARTIALLY_FILLED"
    saved = journal.load_execution_evidence()[0]
    assert saved.execution_id == f"BINANCE:{identity.content_hash}:SOLUSDT:77"
    assert saved.qty == Decimal("0.2") and saved.fee == Decimal("0.10")
    # The same broker fill at a different receipt clock remains one execution.
    runtime.capture_native_event(fill, now + 100, order=order)
    runtime.drain_events_to_journal()
    assert len(journal.load_execution_evidence()) == 1 and not runtime.stopped
    canceled = model.OrderCanceled(
        trader_id=model.TraderId("ATLAS-BINANCE-DEMO"), strategy_id=model.StrategyId("BinanceDemoStrategy-000"),
        instrument_id=model.InstrumentId.from_str("SOLUSDT-PERP.BINANCE"), client_order_id=model.ClientOrderId(intent.client_order_id),
        venue_order_id=model.VenueOrderId("101"), account_id=model.AccountId("BINANCE-scope-demo"),
        event_id=UUID4(), ts_event=now, ts_init=now, reconciliation=False,
    )
    runtime.capture_native_event(canceled, now + 200, order=order)
    runtime.drain_events_to_journal()
    final = journal.load_order_status_observations()[-1]
    assert final.status == "CANCELED" and final.cum_exec_qty == Decimal("0.2")
    assert journal.load_intent(intent.intent_id).lifecycle == intent.lifecycle
    assert journal.load_command("typed-command").outcome.value == "UNSENT"


def test_native_conflicting_execution_and_unknown_association_latch_stop(journal):
    runtime, _, _ = _node(journal)
    _persist_native_intent(journal)
    now = 1_800_000_000_000_000_000
    runtime.capture_native_event(_native_fill(event_time=now - 10), now)
    assert len(runtime.drain_events_to_journal()) == 1
    runtime.capture_native_event(_native_fill(quantity="0.3", event_time=now - 10), now + 1)
    assert runtime.drain_events_to_journal() == ()
    assert runtime.stopped and runtime.last_failure_code == "BINANCE_NATIVE_EVENT_PERSISTENCE_FAILED"
    assert journal.load_execution_evidence()[0].qty == Decimal("0.2")


def test_missing_native_intent_records_unknown_receipt_without_fill(journal):
    runtime, _, _ = _node(journal)
    now = 1_800_000_000_000_000_000
    runtime.capture_native_event(_native_fill(event_time=now - 10), now)
    assert len(runtime.drain_events_to_journal()) == 1
    assert journal.count("observations") == 1
    assert journal.load_execution_evidence() == []
    assert runtime.stopped and runtime.last_failure_code == "BINANCE_NATIVE_EVENT_ASSOCIATION_UNRESOLVED"


def test_slow_reconciliation_never_blocks_native_timer_or_queues_workers(journal):
    runtime, _, _ = _node(journal)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def slow_read():
        calls.append(threading.get_ident())
        entered.set()
        release.wait(5)
        return None

    runtime.reconcile_once = slow_read
    runtime.strategy._port = object()
    try:
        runtime.strategy._on_drain_timer(None)
        assert entered.wait(1)
        worker = runtime._reconciliation_worker
        for _ in range(20):
            runtime.strategy._on_drain_timer(None)
        assert runtime._reconciliation_worker is worker
        assert len(calls) == 1 and calls[0] != threading.get_ident()
        assert runtime.reconciliation_ready is False
    finally:
        release.set()
        runtime._reconciliation_worker.join(1)
    assert runtime.reconciliation_ready is False
    assert runtime.readiness_reason == "BINANCE_COMMAND_READINESS_UNAVAILABLE"


def test_incomplete_reconciliation_never_opens_native_command_gate(journal):
    runtime, _, _ = _node(journal)
    runtime.reconcile_once = lambda: False
    runtime.request_reconciliation()
    worker = runtime._reconciliation_worker
    assert worker is not None
    worker.join(2)
    assert not worker.is_alive()
    assert runtime.reconciliation_ready is False
    assert runtime.last_failure_code is None
    assert runtime.readiness_reason == "BINANCE_COMMAND_READINESS_UNAVAILABLE"


def test_native_timer_dispatches_only_command_bound_to_current_proof(journal):
    runtime, identity, _ = _node(journal)
    runtime.strategy._port = object()
    runtime.reconciliation_ready = True
    runtime.account_snapshot_getter = lambda: Snapshot(identity.content_hash, True, runtime.strategy.clock.timestamp_ns())
    runtime._readiness_proof = SimpleNamespace(command_id="0")
    runtime._last_reconciliation_request_ns = time.monotonic_ns()
    sent = []
    runtime._dispatch = lambda command_id, _port, _now: sent.append(command_id)
    for index in range(10):
        runtime.command_queue.put_nowait(str(index))
        runtime._queued_command_ids.add(str(index))
    runtime.strategy._on_drain_timer(None)
    assert sent == ["0"] and runtime.command_queue.qsize() == 9
    assert runtime._reconciliation_worker is None


def test_hosted_lifecycle_accepts_future_and_stops_through_captured_handle():
    async def hosted():
        runtime = BinanceNativeNode.__new__(BinanceNativeNode)
        runtime.assert_writer = lambda: None
        runtime.hydrate_unsent_command = lambda: None
        runtime._run_lock = asyncio.Lock()
        runtime._run_task = None
        runtime._reconciliation_worker = None
        runtime.event_queue = queue.Queue(maxsize=256)
        runtime.stopped = False
        runtime.last_failure_code = None
        future = asyncio.get_running_loop().create_future()
        calls = []

        class TakenWrapper:
            def run_async(self):
                calls.append("run_async")
                return future

            def stop(self):
                raise AssertionError("rc5 wrapper was consumed")

            def dispose(self):
                raise AssertionError("rc5 wrapper was consumed")

        class Handle:
            def stop(self):
                calls.append("handle_stop")
                future.set_result(None)

        runtime.node = TakenWrapper()
        runtime._node_handle = Handle()
        run = asyncio.create_task(runtime.run())
        await asyncio.sleep(0)
        assert runtime._run_task is future
        await runtime.stop()
        await run
        assert calls == ["run_async", "handle_stop"] and runtime.stopped

    asyncio.run(hosted())


def test_overflow_between_fences_preserves_unknown_and_blocks_effect(journal):
    runtime, _, _ = _node(journal)
    _persist_native_intent(journal)
    calls = []

    def fence():
        calls.append("fence")
        if len(calls) == 2:
            runtime._trip_overflow()

    class Port:
        def dispatch(self, *_args, **_kwargs):
            calls.append("effect")

    runtime.assert_writer = fence
    runtime._dispatch("typed-command", Port(), 1_800_000_000_000_000_000)
    assert calls == ["fence", "fence"]
    assert journal.load_command("typed-command").outcome.value == "UNSENT"
    assert runtime.stopped and runtime.queue_overflow


def test_stop_timeout_preserves_active_native_future_for_writer_lease_owner():
    async def check():
        runtime = BinanceNativeNode.__new__(BinanceNativeNode)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime._run_task = asyncio.get_running_loop().create_future()
        runtime._reconciliation_worker = None
        runtime._node_handle = SimpleNamespace(stop=lambda: None)
        runtime.event_queue = queue.Queue(maxsize=256)
        with pytest.raises(PersistenceError, match="writer must remain held"):
            await runtime.stop(timeout_s=0.01)
        assert not runtime._run_task.done() and not runtime._run_task.cancelled()
        assert runtime.stopped and runtime.last_failure_code == "BINANCE_NATIVE_NODE_STOP_TIMEOUT"
        runtime._run_task.set_result(None)
        await runtime.stop(timeout_s=0.01)
    asyncio.run(check())


def test_stop_timeout_keeps_live_reconciliation_worker_visible():
    async def check():
        runtime = BinanceNativeNode.__new__(BinanceNativeNode)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime._run_task = asyncio.get_running_loop().create_future()
        runtime._run_task.set_result(None)
        runtime._node_handle = SimpleNamespace(stop=lambda: None)
        runtime.event_queue = queue.Queue(maxsize=256)
        finish = threading.Event()
        worker = threading.Thread(target=lambda: finish.wait(1), daemon=True)
        runtime._reconciliation_worker = worker
        worker.start()
        try:
            with pytest.raises(PersistenceError, match="writer must remain held"):
                await runtime.stop(timeout_s=0.01)
            assert worker.is_alive() and runtime.last_failure_code == "BINANCE_RECONCILIATION_STOP_TIMEOUT"
        finally:
            finish.set()
            worker.join(1)
        await runtime.stop(timeout_s=0.01)
    asyncio.run(check())


def test_cancelling_run_does_not_cancel_native_future_and_requests_handle_stop():
    async def check():
        runtime = BinanceNativeNode.__new__(BinanceNativeNode)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime.assert_writer = lambda: None
        runtime.hydrate_unsent_command = lambda: None
        runtime._run_lock = asyncio.Lock()
        runtime._run_task = None
        runtime._reconciliation_worker = None
        runtime.event_queue = queue.Queue(maxsize=256)
        future = asyncio.get_running_loop().create_future()
        calls = []
        runtime.node = SimpleNamespace(run_async=lambda: future)
        runtime._node_handle = SimpleNamespace(stop=lambda: calls.append("stop"))
        caller = asyncio.create_task(runtime.run())
        await asyncio.sleep(0)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert calls == ["stop"] and not future.done()
        future.set_result(None)
        await runtime.stop(timeout_s=0.01)
    asyncio.run(check())


def test_unstarted_disposal_is_idempotent_and_consumed_wrapper_refused():
    runtime = BinanceNativeNode.__new__(BinanceNativeNode)
    runtime._run_task = None
    calls = []
    runtime.node = SimpleNamespace(dispose=lambda: calls.append("dispose"))
    runtime.dispose_unstarted()
    runtime.dispose_unstarted()
    assert calls == ["dispose"]
    runtime._run_task = object()
    with pytest.raises(PersistenceError, match="consumed"):
        runtime.dispose_unstarted()
