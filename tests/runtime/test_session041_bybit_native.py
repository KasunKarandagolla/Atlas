import asyncio
import json
import queue
import threading
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest
from support.assisted_control_fixture import make_plan

from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, make_command
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.bybit_demo import BybitDemoIdentity, BybitDemoSnapshot, DemoCredential
from atlas.runtime.bybit_native import BybitNativeEvent, BybitNativeNode
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
        InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.DEMO, ProductTypeV2.LINEAR_PERPETUAL,
                        "SOLUSDT", "SOL", "USDT", "USDT", "rev-1"),
        1_800_000_000_000_000_000, 1_800_000_000_000_000_000, 1_800_000_000_000_000_000,
        Decimal("1"), Decimal("0.01"), Decimal("0.01"), Decimal("0.01"), TradingStatusV2.TRADING,
        "fixture", min_notional=Decimal("5"), max_qty=Decimal("100"),
    )


def _node(journal, *, port_factory=None):
    identity = BybitDemoIdentity("scope-demo", "cred-demo")
    now = 1_800_000_000_000_000_000
    snapshots = []

    def get_snapshot():
        snapshots.append(now)
        return BybitDemoSnapshot(identity.content_hash, now, True, (), "f" * 64,
                                 "BYBIT_CURRENT_PROFILE_QUALIFIED", (), profile_qualified=True, native_symbol="SOLUSDT")

    runtime = BybitNativeNode(
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
    )
    return runtime, identity, snapshots


def test_actual_pinned_live_node_builds_without_starting_or_reading_account(journal):
    runtime, _identity, snapshots = _node(journal)
    assert runtime.node.is_running is False
    assert snapshots == []
    assert runtime.command_queue.maxsize == 32
    assert runtime.event_queue.maxsize == 256


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
    row = BybitNativeEvent("OrderFilled", "a" * 32, "venue-id", "SOLUSDT-LINEAR.BYBIT", "FILLED",
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


def test_command_dispatch_checks_fence_before_unknown_and_immediately_before_effect(journal):
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
    assert [item[0] for item in fence_calls] == ["fence", "fence", "effect"]
    assert fence_calls[-1][1] == "UNKNOWN"
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"


def _native_fill(client_id="c" * 32, *, trade_id="77", quantity="0.2", event_time=1_800_000_000_000_000_000):
    from nautilus_trader import model
    from nautilus_trader.core import UUID4

    return model.OrderFilled(
        trader_id=model.TraderId("ATLAS-BYBIT-DEMO"),
        strategy_id=model.StrategyId("BybitDemoStrategy-000"),
        instrument_id=model.InstrumentId.from_str("SOLUSDT-LINEAR.BYBIT"),
        client_order_id=model.ClientOrderId(client_id), venue_order_id=model.VenueOrderId("101"),
        account_id=model.AccountId("BYBIT-scope-demo"), trade_id=model.TradeId(trade_id),
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
                      "instrument_ref": _product().content_hash,
                      "identity_hash": BybitDemoIdentity("scope-demo", "cred-demo").content_hash},
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
    assert saved.execution_id == f"BYBIT:{identity.content_hash}:SOLUSDT:77"
    assert saved.qty == Decimal("0.2") and saved.fee == Decimal("0.10")
    # The same broker fill at a different receipt clock remains one execution.
    runtime.capture_native_event(fill, now + 100, order=order)
    runtime.drain_events_to_journal()
    assert len(journal.load_execution_evidence()) == 1 and not runtime.stopped
    canceled = model.OrderCanceled(
        trader_id=model.TraderId("ATLAS-BYBIT-DEMO"), strategy_id=model.StrategyId("BybitDemoStrategy-000"),
        instrument_id=model.InstrumentId.from_str("SOLUSDT-LINEAR.BYBIT"), client_order_id=model.ClientOrderId(intent.client_order_id),
        venue_order_id=model.VenueOrderId("101"), account_id=model.AccountId("BYBIT-scope-demo"),
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
    assert runtime.stopped and runtime.last_failure_code == "BYBIT_NATIVE_EVENT_PERSISTENCE_FAILED"
    assert journal.load_execution_evidence()[0].qty == Decimal("0.2")


def test_missing_native_intent_records_unknown_receipt_without_fill(journal):
    runtime, _, _ = _node(journal)
    now = 1_800_000_000_000_000_000
    runtime.capture_native_event(_native_fill(event_time=now - 10), now)
    assert len(runtime.drain_events_to_journal()) == 1
    assert journal.count("observations") == 1
    assert journal.load_execution_evidence() == []
    assert runtime.stopped and runtime.last_failure_code == "BYBIT_NATIVE_EVENT_ASSOCIATION_UNRESOLVED"


def test_slow_reconciliation_never_blocks_native_timer_or_queues_workers(journal):
    runtime, _, _ = _node(journal)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def slow_read():
        calls.append(threading.get_ident())
        entered.set()
        release.wait(5)

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
    assert runtime.reconciliation_ready is True


def test_native_timer_dispatches_at_most_eight_commands(journal):
    runtime, identity, _ = _node(journal)
    runtime.strategy._port = object()
    runtime.reconciliation_ready = True
    runtime.account_snapshot_getter = lambda: BybitDemoSnapshot(
        identity.content_hash, runtime.strategy.clock.timestamp_ns(), True, (), "f" * 64,
        "BYBIT_CURRENT_PROFILE_QUALIFIED", (), profile_qualified=True, native_symbol="SOLUSDT")
    sent = []
    runtime._dispatch = lambda command_id, _port, _now: sent.append(command_id)
    for index in range(10):
        runtime.command_queue.put_nowait(str(index))
        runtime._queued_command_ids.add(str(index))
    runtime.strategy._on_drain_timer(None)
    assert len(sent) == 8 and runtime.command_queue.qsize() == 2
    runtime._reconciliation_worker.join(1)


def test_hosted_lifecycle_accepts_future_and_stops_through_captured_handle():
    async def hosted():
        runtime = BybitNativeNode.__new__(BybitNativeNode)
        runtime.assert_writer = lambda: None
        runtime._run_lock = asyncio.Lock()
        runtime._run_task = None
        runtime._reconciliation_worker = None
        runtime.event_queue = queue.Queue(maxsize=256)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime.assert_writer = lambda: None
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
    assert journal.load_command("typed-command").outcome.value == "UNKNOWN"
    assert runtime.stopped and runtime.queue_overflow


T0 = 1_800_000_000_000_000_000


def _real_port(journal, *, size="2", side="Buy", received=T0):
    import json

    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.model import StrategyId, TraderId

    from atlas.runtime.bybit_demo import BybitDemoReadReceipt, BybitMarketFilters, BybitNautilusDemoPort
    identity = BybitDemoIdentity("scope-demo", "cred-demo")
    product = _product()
    body = {"retCode": 0, "result": {"list": [{"symbol": "SOLUSDT", "positionIdx": 0,
                                                "size": size, "side": side}]}}
    receipt = BybitDemoReadReceipt(identity.content_hash, "/v5/position/list", received,
                                  "a" * 64, "b" * 64, json.dumps(body), received)
    snapshot = BybitDemoSnapshot(identity.content_hash, received, True, (), "f" * 64,
                                "BYBIT_CURRENT_PROFILE_QUALIFIED", (receipt,), positions=(receipt,),
                                profile_qualified=True, native_symbol="SOLUSDT")
    class Host:
        def __init__(self):
            self.order_factory = OrderFactory(TraderId("ATLAS-BYBIT-DEMO"), StrategyId("BybitDemoStrategy-000"), Clock.new_test())
            self.effects = []
            self.orders = {}
        def submit_order(self, order, *, params=None):
            self.effects.append((order, params))
        def find_order(self, client_id):
            return self.orders.get(client_id)
        def cancel_order(self, order):
            self.effects.append(("cancel", order))
    host = Host()
    filters = BybitMarketFilters(product.content_hash, received, Decimal("0.01"), Decimal("100"),
                                 Decimal("0.01"), "a" * 64)
    port = BybitNautilusDemoPort(identity, host, product, account_snapshot_getter=lambda: snapshot,
                               market_filters=filters)
    return port, host


T0 = 1_800_000_000_000_000_000


def _exact_command(journal, kind=CommandType.SUBMIT_EXIT, **changes):
    from conftest import add_intent
    identity = BybitDemoIdentity("scope-demo", "cred-demo")
    intent = add_intent(journal)
    payload = {"client_order_id": intent.client_order_id, "symbol": "SOLUSDT",
               "instrument_ref": _product().content_hash, "identity_hash": identity.content_hash,
               "side": "SELL", "quantity": "1", "price": "120", "stop": "110", "reduce_only": True}
    payload.update(changes)
    command = make_command(command_id="exact-command", intent_id=intent.intent_id, command_type=kind,
                           payload_dict=payload, expected_state_version=0, created_at_ns=T0)
    journal.persist_command(command)
    return command, intent


def test_actual_sdk_offline_entry_ioc_has_exact_attached_full_mark_stop_and_no_effect(journal):
    from nautilus_trader.model import TimeInForce
    port, host = _real_port(journal)
    command, _ = _exact_command(journal, CommandType.SUBMIT_ENTRY, side="BUY", reduce_only=False)
    order, params = port.compile_entry_ioc(command, now_ns=T0)
    assert str(order.instrument_id) == "SOLUSDT-LINEAR.BYBIT"
    assert order.quantity.as_decimal() == Decimal("1") and order.price.as_decimal() == Decimal("120")
    assert order.time_in_force == TimeInForce.IOC and not order.is_reduce_only
    assert params == {"position_idx": 0, "stop_loss": "110", "sl_trigger_by": "MarkPrice",
                      "sl_order_type": "Market", "tpsl_mode": "Full"}
    assert not host.effects
    from atlas.runtime.bybit_demo import dispatch_persisted_bybit_command
    with pytest.raises(PersistenceError, match="FILL_TIME_READBACK_UNQUALIFIED"):
        dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                         now_ns=T0, writer_epoch=1, assert_writer=lambda: None)
    assert journal.load_command(command.command_id).outcome.value == "UNSENT"
    assert journal.load_command(command.command_id).send_started_at_ns is None
    assert not host.effects


@pytest.mark.parametrize("price", [None, "120"])
def test_reduce_only_native_ioc_or_market_unknown_and_no_replay(journal, price):
    from nautilus_trader.model import TimeInForce

    from atlas.runtime.bybit_demo import dispatch_persisted_bybit_command
    port, host = _real_port(journal)
    command, intent = _exact_command(journal, price=price)
    fences = []
    saved = dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                            now_ns=T0, writer_epoch=1, assert_writer=lambda: fences.append("fence"))
    assert saved.outcome.value == "UNKNOWN" and saved.send_started_at_ns == T0
    assert len(host.effects) == 1 and fences == ["fence", "fence"]
    order, params = host.effects[0]
    assert order.is_reduce_only and order.time_in_force == TimeInForce.IOC and params == {"position_idx": 0}
    with pytest.raises(PersistenceError, match="replay refused"):
        dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                         now_ns=T0 + 1, writer_epoch=1, assert_writer=lambda: None)
    assert len(host.effects) == 1 and journal.load_intent(intent.intent_id).lifecycle == intent.lifecycle
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")


@pytest.mark.parametrize("change", [{"side": "BUY"}, {"quantity": "3"}, {"reduce_only": False},
                                     {"symbol": "BTCUSDT"}, {"price": "120.005"}])
def test_reduction_refuses_wrong_position_or_exact_filters_without_effect(journal, change):
    port, host = _real_port(journal)
    command, _ = _exact_command(journal, **change)
    with pytest.raises(ValueError):
        port.dispatch(command, now_ns=T0)
    assert not host.effects


def test_stale_profile_and_stale_writer_never_create_external_effect(journal):
    from atlas.runtime.bybit_demo import dispatch_persisted_bybit_command
    port, host = _real_port(journal, received=T0 - 3_000_000_000)
    command, _ = _exact_command(journal)
    with pytest.raises(ValueError, match="STALE"):
        port.dispatch(command, now_ns=T0)
    assert not host.effects
    def fence():
        if journal.load_command(command.command_id).outcome.value == "UNKNOWN":
            raise PersistenceError("stale writer")
    with pytest.raises(PersistenceError, match="stale writer"):
        dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                         now_ns=T0, writer_epoch=1, assert_writer=fence)
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN" and not host.effects


def test_full_stop_repair_is_precisely_gated_and_cannot_be_independent_conditional(journal):
    from atlas.runtime.bybit_demo import dispatch_persisted_bybit_command
    port, host = _real_port(journal)
    command, _ = _exact_command(journal, CommandType.REPAIR_STOP)
    with pytest.raises(PersistenceError, match="FULL_POSITION_PROTECTION_PORT_UNQUALIFIED"):
        dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                         now_ns=T0, writer_epoch=1, assert_writer=lambda: None)
    assert journal.load_command(command.command_id).outcome.value == "UNSENT" and not host.effects


def test_native_cancel_only_exact_cached_order_and_keeps_partial_fill_unknown(journal):
    from nautilus_trader.model import OrderSide, TimeInForce

    from atlas.runtime.bybit_demo import dispatch_persisted_bybit_command
    port, host = _real_port(journal)
    command, _ = _exact_command(journal, CommandType.CANCEL_ENTRY)
    # A local cached opening order is the exact native object being canceled.
    opening, _ = port.compile_entry_ioc(
        make_command(command_id="compile-only", intent_id=command.intent_id, command_type=CommandType.SUBMIT_ENTRY,
                     payload_dict=json.loads(command.payload) | {"side": "BUY", "reduce_only": False},
                     expected_state_version=0, created_at_ns=T0), now_ns=T0)
    host.orders[str(opening.client_order_id)] = opening
    saved = dispatch_persisted_bybit_command(journal=journal, command_id=command.command_id, port=port,
                                            now_ns=T0, writer_epoch=1, assert_writer=lambda: None)
    assert saved.outcome.value == "UNKNOWN" and host.effects == [("cancel", opening)]
    assert opening.side == OrderSide.BUY and opening.time_in_force == TimeInForce.IOC


def test_selected_metadata_read_and_market_filters_are_exact(journal):
    from atlas.runtime.bybit_demo import BybitMarketFilters
    product = _product()
    row = {"symbol": "SOLUSDT", "status": "Trading", "lotSizeFilter": {
           "minOrderQty": "0.01", "maxMktOrderQty": "10", "maxOrderQty": "100", "qtyStep": "0.01"}}
    filters = BybitMarketFilters.from_metadata(row, product=product, received_at_ns=T0)
    filters.validate(product_ref=product.content_hash, quantity=Decimal("10"), now_ns=T0)
    with pytest.raises(ValueError, match="market quantity"):
        filters.validate(product_ref=product.content_hash, quantity=Decimal("11"), now_ns=T0)
    with pytest.raises(KeyError):
        BybitMarketFilters.from_metadata(row | {"lotSizeFilter": {"minOrderQty": "0.01", "maxOrderQty": "100", "qtyStep": "0.01"}},
                                        product=product, received_at_ns=T0)


def test_stop_timeout_preserves_active_native_future_for_writer_lease_owner():
    async def check():
        runtime = BybitNativeNode.__new__(BybitNativeNode)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime._run_task = asyncio.get_running_loop().create_future()
        runtime._reconciliation_worker = None
        runtime._node_handle = SimpleNamespace(stop=lambda: None)
        runtime.event_queue = queue.Queue(maxsize=256)
        with pytest.raises(PersistenceError, match="writer must remain held"):
            await runtime.stop(timeout_s=0.01)
        assert not runtime._run_task.done() and not runtime._run_task.cancelled()
        assert runtime.stopped and runtime.last_failure_code == "BYBIT_NATIVE_NODE_STOP_TIMEOUT"
        runtime._run_task.set_result(None)
        await runtime.stop(timeout_s=0.01)
    asyncio.run(check())


def test_stop_timeout_keeps_live_reconciliation_worker_visible():
    async def check():
        runtime = BybitNativeNode.__new__(BybitNativeNode)
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
            assert worker.is_alive() and runtime.last_failure_code == "BYBIT_RECONCILIATION_STOP_TIMEOUT"
        finally:
            finish.set()
            worker.join(1)
        await runtime.stop(timeout_s=0.01)
    asyncio.run(check())


def test_cancelling_run_does_not_cancel_native_future_and_requests_handle_stop():
    async def check():
        runtime = BybitNativeNode.__new__(BybitNativeNode)
        runtime.stopped = False
        runtime.last_failure_code = None
        runtime.assert_writer = lambda: None
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
    runtime = BybitNativeNode.__new__(BybitNativeNode)
    runtime._run_task = None
    calls = []
    runtime.node = SimpleNamespace(dispose=lambda: calls.append("dispose"))
    runtime.dispose_unstarted()
    runtime.dispose_unstarted()
    assert calls == ["dispose"]
    runtime._run_task = object()
    with pytest.raises(PersistenceError, match="consumed"):
        runtime.dispose_unstarted()
