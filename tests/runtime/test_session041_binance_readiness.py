from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from decimal import Decimal

import pytest

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth, Side
from atlas.domain.execution import Intent, Reservation, make_command
from atlas.domain.trade_plan import TradePlan
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.assisted_control import AssistedControlShell
from atlas.runtime.binance_demo import (
    BinanceDemoIdentity,
    BinanceDemoReadReceipt,
    BinanceMarketFilters,
    BinanceNautilusDemoPort,
    DemoCredential,
    dispatch_persisted_demo_command,
)
from atlas.runtime.binance_native import BinanceNativeNode
from atlas.runtime.binance_readiness import (
    binance_command_client_order_id,
    build_binance_command_readiness,
    validate_binance_command_readiness,
)
from atlas.runtime.binance_reconciliation import BinanceAccountSnapshot, BinanceObservedRow
from atlas.runtime.fill_dedup import OrderStatusRecord
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

T0 = 1_800_000_000_000_000_000


def product(now: int = T0) -> ProductContractV2:
    return ProductContractV2(
        InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.DEMO, ProductTypeV2.LINEAR_PERPETUAL,
                        "SOLUSDT", "SOL", "USDT", "USDT", "rev-1"),
        now, now, now, Decimal("1"), Decimal("0.01"), Decimal("0.01"), Decimal("0.01"),
        TradingStatusV2.TRADING, "fixture", min_notional=Decimal("5"), max_qty=Decimal("100"),
    )


def identity() -> BinanceDemoIdentity:
    return BinanceDemoIdentity("scope-demo", "credential-demo")


def plan(journal, *, side: Side = Side.LONG, now: int = T0) -> TradePlan:
    stop, reference = (Decimal("120"), Decimal("130")) if side is Side.LONG else (
        Decimal("140"), Decimal("130"))
    value = TradePlan(
        plan_id="binance-plan", version="v1", policy_hash="policy", snapshot_hash="snapshot",
        expires_at_ns=now + 60_000_000_000, market="BINANCE", account_scope="scope-demo",
        instrument="SOLUSDT", side=side, qty_limit=Decimal("2"), entry_policy="IOC_LIMIT_FULL_STOP",
        collar=Decimal("130"), stop=stop, stop_trigger_basis="MarkPrice", management_policy="fixed",
        horizon_end_ns=now + 24 * 3_600_000_000_000, cost_distribution_ref="cost", normal_risk=Decimal("1"),
        stress_risk=Decimal("2"), margin=Decimal("3"), leverage_bound=Decimal("2"), risk_config_hash="risk",
        created_at_ns=now, available_at_ns=now, reference_price=reference,
    )
    journal.create_trade_plan(value)
    return value


def snapshot(identity_value: BinanceDemoIdentity, *, signed: str = "2", now: int = T0) -> BinanceAccountSnapshot:
    def row(values):
        frozen = tuple(sorted(values.items()))
        body = json.dumps(values, sort_keys=True, separators=(",", ":"))
        return BinanceObservedRow(frozen, now, hashlib.sha256(body.encode()).hexdigest())

    return BinanceAccountSnapshot(
        identity_value.content_hash, identity_value.account_scope_ref, identity_value.credential_ref,
        "DEMO", identity_value.product, now, row({"canTrade": True}), row({"dualSidePosition": False}),
        row({"multiAssetsMargin": False}), (row({"symbol": "SOLUSDT", "positionAmt": signed,
                                                 "positionSide": "BOTH"}),), "a" * 64,
        "VERIFIED_STABLE_UID", True, (), (row({"symbol": "SOLUSDT", "marginType": "ISOLATED"}),),
    )


class EmptyOpenViews:
    def __init__(self, identity_value, now: int | Callable[[], int] = T0):
        self.identity = identity_value
        self.now = now

    def _clock(self) -> int:
        return self.now() if callable(self.now) else int(self.now)

    def clock_ns(self):
        return self._clock()

    def read_with_receipt(self, endpoint, _params):
        payload = "[]"
        digest = hashlib.sha256(payload.encode()).hexdigest()
        return BinanceDemoReadReceipt(self.identity.content_hash, endpoint, self._clock(), digest, digest, payload)


def active_intent(journal, *, lifecycle=LifecycleState.OPEN_PROTECTED, writer_epoch=1, now: int = T0):
    intent = Intent("binance-intent", 7, "binance-plan", "v1", "a" * 32, writer_epoch, lifecycle,
                    ProtectionStatus.CONFIRMED, ReconciliationHealth.CURRENT, now, 0)
    journal.create_intent_with_reservation(
        intent, Reservation("binance-reservation", intent.intent_id, Decimal("2"), Decimal("1"),
                            Decimal("2"), Decimal("260"), Decimal("260"), Decimal("130"), Decimal("2")),
    )
    return intent


def command_payload(identity_value, product_value, intent, command_id, *, stop="120", side="SELL"):
    return {
        "identity_hash": identity_value.content_hash,
        "instrument_ref": product_value.content_hash,
        "symbol": "SOLUSDT",
        "client_order_id": binance_command_client_order_id(
            identity_value, intent_id=intent.intent_id, command_id=command_id, writer_epoch=intent.writer_epoch),
        "side": side,
        "quantity": "1",
        "price": "125",
        "stop": stop,
        "reduce_only": True,
    }


def prepared_exit(journal, *, stop="120", writer_epoch=1, now: int = T0):
    identity_value, product_value = identity(), product(now)
    plan(journal, now=now)
    intent = active_intent(journal, writer_epoch=writer_epoch, now=now)
    command_id = "reduce-command"
    command = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id=command_id, command_type=CommandType.SUBMIT_EXIT,
        payload_dict=command_payload(identity_value, product_value, intent, command_id, stop=stop),
        created_at_ns=now, next_lifecycle=LifecycleState.EXIT_PENDING,
    )
    return identity_value, product_value, intent, command


def prepared_repair(journal, *, stop="120", writer_epoch=1, now: int = T0):
    identity_value, product_value = identity(), product(now)
    plan(journal, now=now)
    intent = active_intent(journal, lifecycle=LifecycleState.OPEN_UNPROTECTED,
                           writer_epoch=writer_epoch, now=now)
    command_id = "repair-command"
    command = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id=command_id, command_type=CommandType.REPAIR_STOP,
        payload_dict=command_payload(identity_value, product_value, intent, command_id, stop=stop,
                                     side="SELL") | {"quantity": "2", "price": None},
        created_at_ns=now, next_lifecycle=LifecycleState.OPEN_UNPROTECTED,
    )
    return identity_value, product_value, intent, command


def test_typed_readiness_binds_approved_action_and_restarts_writer(journal):
    identity_value, product_value, intent, command = prepared_exit(journal)
    account = snapshot(identity_value)
    proof = build_binance_command_readiness(
        EmptyOpenViews(identity_value), journal, identity=identity_value, product=product_value,
        snapshot=account, writer_epoch=2, native_generation=0, assert_writer=lambda: None,
    )
    assert proof is not None and proof.command_id == command.command_id
    assert proof.client_order_id == json.loads(command.payload)["client_order_id"]
    assert proof.writer_epoch == 2
    validate_binance_command_readiness(
        proof, journal, identity=identity_value, product=product_value, snapshot=account,
        command_id=command.command_id, writer_epoch=2, native_generation=0,
        now_ns=T0 + 1,
    )


def test_real_dispatch_persists_unknown_before_effect_and_never_retries(journal):
    from unittest.mock import Mock

    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.model import StrategyId, TraderId

    identity_value, product_value, _intent, command = prepared_exit(journal)
    account = snapshot(identity_value)
    host = Mock()
    host.order_factory = OrderFactory(TraderId("ATLAS-TEST"), StrategyId("BINANCE-TEST"), Clock.new_test())
    host.find_order.return_value = None
    market_filters = BinanceMarketFilters(
        product_value.content_hash, T0, Decimal("0.01"), Decimal("100"), Decimal("0.01"), "b" * 64,
    )
    port = BinanceNautilusDemoPort(
        identity_value, host, product_value, account_snapshot_getter=lambda: account,
        market_filters=market_filters,
    )
    proof = build_binance_command_readiness(
        EmptyOpenViews(identity_value), journal, identity=identity_value, product=product_value,
        snapshot=account, writer_epoch=2, native_generation=0, assert_writer=lambda: None,
    )
    assert proof is not None

    def effect(_order, **_kwargs):
        assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
        raise TimeoutError("transport detail must stay private")

    host.submit_order.side_effect = effect
    kwargs = {
        "journal": journal, "command_id": command.command_id, "port": port, "now_ns": T0 + 1,
        "writer_epoch": 2, "assert_writer": lambda: None, "readiness_proof": proof,
        "snapshot_getter": lambda: account, "generation_getter": lambda: 0,
        "clock_ns": lambda: T0 + 1,
    }
    result = dispatch_persisted_demo_command(**kwargs)
    assert result.outcome.value == "UNKNOWN"
    assert host.submit_order.call_count == 1
    with pytest.raises(PersistenceError, match="redispatch refused"):
        dispatch_persisted_demo_command(**kwargs)
    assert host.submit_order.call_count == 1
    validate_binance_command_readiness(
        proof, journal, identity=identity_value, product=product_value, snapshot=account,
        command_id=command.command_id, writer_epoch=2, native_generation=0,
        now_ns=T0 + 1, send_started=True,
    )


def test_plan_bound_repair_dispatches_full_position_mark_price_stop(journal):
    from unittest.mock import Mock

    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.model import StrategyId, TraderId

    identity_value, product_value, _intent, command = prepared_repair(journal)
    account = snapshot(identity_value)
    host = Mock()
    host.order_factory = OrderFactory(TraderId("ATLAS-REPAIR"), StrategyId("BINANCE-REPAIR"), Clock.new_test())
    host.find_order.return_value = None
    port = BinanceNautilusDemoPort(
        identity_value, host, product_value, account_snapshot_getter=lambda: account,
        market_filters=BinanceMarketFilters(product_value.content_hash, T0, Decimal("0.01"),
            Decimal("100"), Decimal("0.01"), "b" * 64),
    )
    proof = build_binance_command_readiness(
        EmptyOpenViews(identity_value), journal, identity=identity_value, product=product_value,
        snapshot=account, writer_epoch=2, native_generation=0, assert_writer=lambda: None,
    )
    assert proof is not None and proof.command_type == CommandType.REPAIR_STOP.value
    result = dispatch_persisted_demo_command(
        journal=journal, command_id=command.command_id, port=port, now_ns=T0 + 1,
        writer_epoch=2, assert_writer=lambda: None, readiness_proof=proof,
        snapshot_getter=lambda: account, generation_getter=lambda: 0, clock_ns=lambda: T0 + 1,
    )
    assert result.outcome.value == "UNKNOWN"
    order = host.submit_order.call_args.args[0]
    assert order.is_reduce_only is True
    assert str(order.trigger_type) == "MARK_PRICE"
    assert host.submit_order.call_args.kwargs["params"] == {"close_position": True}
    assert str(order.quantity) == "2"
    assert Decimal(str(order.trigger_price)) == Decimal("120")


def test_repair_readiness_rejects_stop_that_differs_from_immutable_plan(journal):
    identity_value, product_value = identity(), product()
    plan(journal)
    intent = active_intent(journal, lifecycle=LifecycleState.OPEN_UNPROTECTED)
    command_id = "wrong-stop-repair"
    journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id=command_id, command_type=CommandType.REPAIR_STOP,
        payload_dict=command_payload(identity_value, product_value, intent, command_id,
                                     stop="119", side="SELL") | {"price": None, "quantity": "2"},
        created_at_ns=T0, next_lifecycle=LifecycleState.OPEN_UNPROTECTED,
    )
    with pytest.raises(PersistenceError, match="approved plan"):
        build_binance_command_readiness(
            EmptyOpenViews(identity_value), journal, identity=identity_value, product=product_value,
            snapshot=snapshot(identity_value), writer_epoch=2, native_generation=0,
            assert_writer=lambda: None,
        )


def test_native_payload_builder_uses_deterministic_child_identity_and_plan_stop(journal):
    identity_value, product_value, _intent, _command = prepared_exit(journal)
    runtime = object.__new__(BinanceNativeNode)
    runtime.identity = identity_value
    runtime._product = product_value
    intent = journal.load_intent("binance-intent")
    saved_plan = journal.load_trade_plan(intent.plan_id)
    payload = BinanceNativeNode.build_reduction_payload(
        runtime, intent=intent, plan=saved_plan, command_id="child-one",
        command_type=CommandType.REPAIR_STOP, side="SELL", quantity=Decimal("2"),
        price=None, stop=saved_plan.stop,
    )
    assert payload["client_order_id"] == binance_command_client_order_id(
        identity_value, intent_id=intent.intent_id, command_id="child-one", writer_epoch=1)
    assert payload["stop"] == "120"
    assert payload["reduce_only"] is True
    with pytest.raises(PersistenceError, match="approved plan"):
        BinanceNativeNode.build_reduction_payload(
            runtime, intent=intent, plan=saved_plan, command_id="child-two",
            command_type=CommandType.REPAIR_STOP, side="SELL", quantity=Decimal("2"),
            price=None, stop=Decimal("119"),
        )


def test_selected_binance_shell_persists_then_queues_exact_reduction(journal):
    from atlas.runtime.binance_demo import DemoCredential

    identity_value, product_value = identity(), product()
    plan(journal)
    intent = active_intent(journal)
    runtime = BinanceNativeNode(
        identity=identity_value, credential=DemoCredential("test-key", "test-secret"),
        product=product_value, journal=journal, writer_epoch=2, assert_writer=lambda: None,
        reconcile_once=lambda: None, account_snapshot_getter=lambda: snapshot(identity_value),
        market_filters=object(), port_factory=lambda *_args: object(),
        node_builder_factory=lambda *_args: _NativeBuilder(),
    )
    shell = AssistedControlShell(
        journal=journal, runtime_instance_id="writer-2", writer_id="writer-2", writer_epoch=2,
        assisted_enabled=False, nautilus_port=runtime,
    )
    result = shell.close(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("2"),
                         quantity=Decimal("1"), exit_price=Decimal("125"), now_ns=T0 + 1)
    assert result.status == "RISK_REDUCTION_QUEUED"
    assert result.command is not None and result.command.send_started_at_ns is None
    payload = json.loads(result.command.payload)
    assert set(payload) == {"identity_hash", "instrument_ref", "symbol", "client_order_id", "side",
                            "quantity", "price", "stop", "reduce_only"}
    assert payload["side"] == "SELL" and payload["quantity"] == "1"
    assert payload["stop"] == "120" and payload["reduce_only"] is True
    assert runtime.command_queue.get_nowait() == result.command.command_id


def test_native_timer_carries_readiness_proof_through_to_real_order_port(journal):
    from unittest.mock import Mock

    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.model import StrategyId, TraderId

    now = time.time_ns()
    identity_value, product_value = identity(), product(now)
    plan(journal, now=now)
    intent = active_intent(journal, writer_epoch=2, now=now)
    account = snapshot(identity_value, now=now)
    runtime = BinanceNativeNode(
        identity=identity_value, credential=DemoCredential("test-key", "test-secret"),
        product=product_value, journal=journal, writer_epoch=2, assert_writer=lambda: None,
        reconcile_once=lambda: None, account_snapshot_getter=lambda: account,
        market_filters=BinanceMarketFilters(product_value.content_hash, now, Decimal("0.01"),
            Decimal("100"), Decimal("0.01"), "b" * 64),
    )
    shell = AssistedControlShell(
        journal=journal, runtime_instance_id="writer-2", writer_id="writer-2", writer_epoch=2,
        assisted_enabled=False, nautilus_port=runtime,
    )
    result = shell.close(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("2"),
                         quantity=Decimal("1"), exit_price=Decimal("125"), now_ns=now + 1)
    assert result.command is not None

    host = Mock()
    host.order_factory = OrderFactory(TraderId("ATLAS-TIMER"), StrategyId("BINANCE-TIMER"), Clock.new_test())
    host.find_order.return_value = None
    runtime.strategy._port = BinanceNautilusDemoPort(
        identity_value, host, product_value, account_snapshot_getter=lambda: account,
        market_filters=runtime.market_filters,
    )
    reader = EmptyOpenViews(identity_value, time.time_ns)
    runtime.reconcile_once = lambda: build_binance_command_readiness(
        reader, journal, identity=identity_value, product=product_value, snapshot=account,
        writer_epoch=runtime.writer_epoch, native_generation=runtime.state_generation,
        assert_writer=runtime.assert_writer,
    )
    runtime.strategy._on_drain_timer(None)
    worker = runtime._reconciliation_worker
    assert worker is not None
    worker.join(3)
    assert not worker.is_alive() and runtime.reconciliation_ready
    runtime.strategy._on_drain_timer(None)

    command = journal.load_command(result.command.command_id)
    assert command.outcome.value == "UNKNOWN"
    assert host.submit_order.call_count == 1
    assert runtime.command_queue.empty()
    assert runtime.state_generation == 1


def test_parent_order_cancel_is_durable_and_uses_parent_identity(journal):

    identity_value, product_value = identity(), product()
    saved_plan = plan(journal)
    intent = Intent("cancel-intent", 8, saved_plan.plan_id, saved_plan.version, "c" * 32, 2,
                    LifecycleState.ENTRY_WORKING, ProtectionStatus.NONE, ReconciliationHealth.CURRENT, T0, 0)
    journal.create_intent_with_reservation(
        intent, Reservation("cancel-reservation", intent.intent_id, Decimal("2"), Decimal("1"),
                            Decimal("2"), Decimal("260"), Decimal("260"), Decimal("130"), Decimal("2")),
    )
    entry = make_command(
        command_id="entry-command", intent_id=intent.intent_id, command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"identity_hash": identity_value.content_hash, "instrument_ref": product_value.content_hash,
            "symbol": "SOLUSDT", "client_order_id": intent.client_order_id, "side": "BUY",
            "quantity": "2", "price": "130", "stop": "120", "reduce_only": False},
        expected_state_version=0, created_at_ns=T0,
    )
    journal.persist_command(entry)
    journal.mark_send_started(entry.command_id, T0 + 1)
    journal.update_command_outcome(entry.command_id, CommandOutcome.RECONCILED)
    journal.append_order_status_observation(OrderStatusRecord(
        "venue-order", intent.client_order_id, intent.intent_id, "NEW", Decimal("0"), Decimal("0"),
        Decimal("0"), None, T0 + 2, "fixture", "d" * 64,
    ))
    node = object.__new__(BinanceNativeNode)
    node.identity, node._product = identity_value, product_value
    queued = []

    class Port:
        risk_reduction_profile = "BINANCE_DEMO_REDUCE_ONLY_V1"

        def build_reduction_payload(self, **kwargs):
            return BinanceNativeNode.build_reduction_payload(node, **kwargs)

        def enqueue_persisted_command(self, command_id):
            queued.append(command_id)

    shell = AssistedControlShell(
        journal=journal, runtime_instance_id="writer-2", writer_id="writer-2", writer_epoch=2,
        nautilus_port=Port(), assisted_enabled=False,
    )
    result = shell.cancel_entry(intent_id=intent.intent_id, now_ns=T0 + 3)
    assert result.status == "CANCEL_QUEUED" and result.command is not None
    assert result.command.command_type == CommandType.CANCEL_ENTRY
    assert result.command.outcome == CommandOutcome.UNSENT and result.command.send_started_at_ns is None
    payload = json.loads(result.command.payload)
    assert payload["client_order_id"] == intent.client_order_id
    assert payload["side"] == "BUY" and payload["reduce_only"] is False
    assert queued == [result.command.command_id]


def test_selected_binance_protection_repair_stays_plan_bound_and_queued(journal):
    identity_value, product_value = identity(), product()
    plan(journal)
    intent = active_intent(journal, lifecycle=LifecycleState.OPEN_UNPROTECTED)
    node = object.__new__(BinanceNativeNode)
    node.identity, node._product = identity_value, product_value
    queued = []

    class Port:
        risk_reduction_profile = "BINANCE_DEMO_REDUCE_ONLY_V1"

        def build_reduction_payload(self, **kwargs):
            return BinanceNativeNode.build_reduction_payload(node, **kwargs)

        def enqueue_persisted_command(self, command_id):
            queued.append(command_id)

    shell = AssistedControlShell(
        journal=journal, runtime_instance_id="writer-2", writer_id="writer-2", writer_epoch=2,
        assisted_enabled=False, nautilus_port=Port(),
    )
    result = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("2"),
                           expected_signed_qty=Decimal("2"), stop_price=Decimal("120"),
                           protection_port=None, now_ns=T0 + 1)
    assert result.status == "PROTECTION_QUEUED" and result.command is not None
    payload = json.loads(result.command.payload)
    assert payload["stop"] == "120" and payload["side"] == "SELL"
    assert payload["quantity"] == "2" and payload["reduce_only"] is True
    assert queued == [result.command.command_id]


class _NativeBuilder:
    def with_risk_engine_config(self, _config):
        return self

    def add_exec_client(self, *_args):
        return self

    def with_reconciliation(self, _enabled):
        return self

    def build(self):
        return _BuiltNode()


class _BuiltNode:
    def add_strategy(self, _strategy):
        return None

    def handle(self):
        return object()
