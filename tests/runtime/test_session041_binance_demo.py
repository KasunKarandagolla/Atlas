from __future__ import annotations

import io
import json
from dataclasses import replace
from decimal import Decimal
from unittest.mock import Mock

import pytest
from conftest import T0, add_intent

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.domain.execution import make_command
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.binance_demo import (
    DEMO_REST,
    MAX_READ_BYTES,
    BinanceDemoIdentity,
    BinanceDemoReader,
    BinanceMarketFilters,
    BinanceNautilusDemoPort,
    DemoCredential,
    DemoReadError,
    build_binance_demo_config,
    classify_negative_order_lookup,
    dispatch_persisted_demo_command,
    verify_binance_protection,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)


def product():
    return ProductContractV2(
        InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.DEMO, ProductTypeV2.LINEAR_PERPETUAL,
                        "SOLUSDT", "SOL", "USDT", "USDT", "revision-1"),
        T0, T0, T0, Decimal("1"), Decimal("0.01"), Decimal("0.01"), Decimal("0.01"),
        TradingStatusV2.TRADING, "metadata-fixture", min_notional=Decimal("5"), max_qty=Decimal("100"),
    )


def identity():
    return BinanceDemoIdentity("owner-demo", "binance-demo-key")


def host():
    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.model import StrategyId, TraderId

    result = Mock()
    result.order_factory = OrderFactory(TraderId("ATLAS-001"), StrategyId("DEMO-001"), Clock.new_test())
    return result


def payload(p, i, client_id):
    return {"identity_hash": i.content_hash, "instrument_ref": p.content_hash, "symbol": "SOLUSDT",
                "client_order_id": client_id, "side": "SELL", "quantity": "1.00", "price": None,
                "stop": "120.00", "reduce_only": True}


def qualified_port(p, i, h, *, snapshot=None):
    from atlas.runtime.binance_reconciliation import BinanceAccountSnapshot, BinanceObservedRow

    def row(values):
        return BinanceObservedRow(tuple(sorted(values.items())), T0, "a" * 64)
    account = snapshot or BinanceAccountSnapshot(i.content_hash, i.account_scope_ref, i.credential_ref,
        i.environment, i.product, T0, row({"canTrade": True}), row({"dualSidePosition": False}),
        row({"multiAssetsMargin": False}), (row({"symbol": "SOLUSDT", "positionAmt": "2",
        "positionSide": "BOTH"}),), "a" * 64, "VERIFIED_STABLE_UID", True, (),
        (row({"symbol": "SOLUSDT", "marginType": "ISOLATED"}),))
    filters = BinanceMarketFilters(p.content_hash, T0, Decimal("0.01"), Decimal("100"), Decimal("0.01"), "b" * 64)
    return BinanceNautilusDemoPort(i, h, p, account_snapshot_getter=lambda: account, market_filters=filters)


@pytest.mark.parametrize("environment", ["LIVE", "MAINNET", "demo", "UNKNOWN"])
def test_no_production_identity(environment):
    with pytest.raises(ValueError):
        BinanceDemoIdentity("owner-demo", "key-ref", environment=environment)


def test_pinned_native_client_configuration_and_secret_repr():
    credential = DemoCredential("fixture-key-material", "fixture-secret-material")
    config = build_binance_demo_config(identity(), credential)
    assert str(config.environment) == "DEMO"
    assert config.max_retries == 0
    assert config.treat_expired_as_canceled is False
    assert "fixture" not in repr(credential)


def test_environment_identity_does_not_alias_demo_testnet():
    p = product()
    assert replace(p.key, environment=EnvironmentV2.TESTNET).content_hash != p.key.content_hash
    with pytest.raises(ValueError):
        BinanceNautilusDemoPort(replace(identity(), environment="TESTNET"), host(), p)


@pytest.mark.parametrize("kind", [CommandType.FLATTEN, CommandType.SUBMIT_EXIT, CommandType.REPAIR_STOP])
def test_real_order_factory_uses_same_identity_reduce_only_mark_stop(kind):
    p, i, h = product(), identity(), host()
    cmd = make_command(command_id="cmd", intent_id="intent", command_type=kind,
                       payload_dict={**payload(p, i, "a" * 32), "quantity": "2" if kind == CommandType.REPAIR_STOP else "1"},
                       expected_state_version=0, created_at_ns=T0)
    qualified_port(p, i, h).dispatch(cmd, now_ns=T0 + 1)
    order = h.submit_order.call_args.args[0]
    assert str(order.client_order_id) == "a" * 32
    assert order.is_reduce_only is True
    assert "BINANCE" in str(order.instrument_id)
    if kind == CommandType.REPAIR_STOP:
        assert str(order.trigger_type) == "MARK_PRICE"
        assert h.submit_order.call_args.kwargs["params"] == {"close_position": True}


def test_durable_unknown_before_effect_and_no_second_send(journal):
    intent = add_intent(journal)
    p, i, h = product(), identity(), host()
    cmd = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="close", command_type=CommandType.SUBMIT_EXIT,
        payload_dict=payload(p, i, intent.client_order_id), created_at_ns=T0)
    def external_effect(order, **kwargs):
        assert journal.load_command(cmd.command_id).outcome == CommandOutcome.UNKNOWN
        raise TimeoutError("sensitive transport text must not escape")
    h.submit_order.side_effect = external_effect
    kwargs = {"journal": journal, "command_id": cmd.command_id,
                  "port": qualified_port(p, i, h), "now_ns": T0 + 1,
                  "writer_epoch": 1, "assert_writer": lambda: None}
    result = dispatch_persisted_demo_command(**kwargs)
    assert result.outcome == CommandOutcome.UNKNOWN
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")
    with pytest.raises(PersistenceError, match="redispatch refused"):
        dispatch_persisted_demo_command(**kwargs)
    assert h.submit_order.call_count == 1


def test_opening_stale_writer_and_second_writer_fail_before_send(journal):
    intent = add_intent(journal)
    p, i, h = product(), identity(), host()
    cmd = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="open", command_type=CommandType.SUBMIT_ENTRY,
        payload_dict=payload(p, i, intent.client_order_id), created_at_ns=T0)
    kwargs = {"journal": journal, "command_id": cmd.command_id,
                  "port": BinanceNautilusDemoPort(i, h, p), "now_ns": T0 + 1,
                  "writer_epoch": 1, "assert_writer": lambda: None}
    with pytest.raises(PersistenceError, match="FILL_TIME_PROTECTION"):
        dispatch_persisted_demo_command(**kwargs)
    with pytest.raises(PersistenceError, match="stale"):
        dispatch_persisted_demo_command(**(kwargs | {"writer_epoch": 2}))
    with pytest.raises(PersistenceError, match="second writer"):
        dispatch_persisted_demo_command(**(kwargs | {"assert_writer": Mock(side_effect=PersistenceError("second writer"))}))
    assert journal.load_command(cmd.command_id).send_started_at_ns is None
    h.submit_order.assert_not_called()


@pytest.mark.parametrize("sent,found,complete,expected", [
    (True, False, True, CommandOutcome.UNKNOWN),
    (True, False, False, CommandOutcome.UNKNOWN),
    (False, False, False, CommandOutcome.UNKNOWN),
    (False, True, True, CommandOutcome.UNKNOWN),
    (False, False, True, CommandOutcome.UNSENT),
])
def test_negative_lookup_does_not_release_uncertain_send(sent, found, complete, expected):
    assert classify_negative_order_lookup(sent_started=sent, lookup_found=found,
                                          history_complete=complete) == expected


def test_conditional_protection_readback_is_exact_and_fresh():
    row = {"symbol": "SOLUSDT", "clientAlgoId": "a" * 32, "algoId": 123,
               "orderType": "STOP_MARKET", "workingType": "MARK_PRICE", "positionSide": "BOTH",
               "side": "SELL", "closePosition": True, "algoStatus": "NEW", "triggerPrice": "120.00"}
    kwargs = {"symbol": "SOLUSDT", "client_order_id": "a" * 32, "stop": Decimal("120"),
                  "signed_position": Decimal("1"), "now_ns": T0 + 1, "received_at_ns": T0}
    assert verify_binance_protection(row, **kwargs).verified
    for key, bad in {"side": "BUY", "workingType": "CONTRACT_PRICE", "closePosition": False,
                     "triggerPrice": "121", "algoStatus": "CANCELED", "positionSide": "LONG",
                     "symbol": "BTCUSDT", "clientAlgoId": "b" * 32}.items():
        assert not verify_binance_protection(row | {key: bad}, **kwargs).verified
    assert not verify_binance_protection(row, **(kwargs | {"now_ns": T0 + 3_000_000_000})).verified


class Response(io.BytesIO):
    def geturl(self):
        return f"{DEMO_REST}/fapi/v1/order"


def test_signed_reader_is_get_only_demo_bounded_and_sanitized():
    opener = Mock()
    opener.open.return_value = Response(json.dumps({"orderId": 123}).encode())
    reader = BinanceDemoReader(identity=identity(), credential=DemoCredential("fixture-key", "fixture-secret"),
                               clock_ns=lambda: T0, opener=opener)
    assert reader.read("/fapi/v1/order", {"symbol": "SOLUSDT", "origClientOrderId": "a" * 32}) == {"orderId": 123}
    request = opener.open.call_args.args[0]
    assert request.method == "GET"
    assert request.full_url.startswith(DEMO_REST + "/fapi/v1/order?")
    assert "signature=" in request.full_url
    assert opener.open.call_args.kwargs["timeout"] == 2.0
    for path in ("/fapi/v1/withdraw", "https://fapi.binance.com/fapi/v1/order"):
        with pytest.raises(ValueError):
            reader.read(path)
    opener.open.side_effect = OSError("credential fixture-secret")
    with pytest.raises(DemoReadError) as failure:
        reader.read("/fapi/v1/order")
    assert "fixture-secret" not in str(failure.value)
    assert failure.value.__suppress_context__


def test_response_budget_and_history_window_fail_closed():
    opener = Mock()
    reader = BinanceDemoReader(identity=identity(), credential=DemoCredential("fixture-key", "fixture-secret"),
                               clock_ns=lambda: T0, opener=opener)
    with pytest.raises(ValueError):
        reader.read("/fapi/v1/userTrades", {"startTime": 0, "endTime": 8 * 24 * 3600 * 1000})
    opener.open.return_value = Response(b" " * (MAX_READ_BYTES + 1))
    with pytest.raises(DemoReadError, match="BUDGET"):
        reader.read("/fapi/v1/order")


def test_exact_ioc_compilation_does_not_grant_opening_authority():
    p, i, h = product(), identity(), host()
    values = payload(p, i, "a" * 32) | {"price": "100.00", "stop": "120.00", "reduce_only": False}
    command = make_command(command_id="compile", intent_id="intent", command_type=CommandType.SUBMIT_ENTRY,
        payload_dict=values, expected_state_version=0, created_at_ns=T0)
    port = qualified_port(p, i, h)
    order = port.compile_entry_ioc(command, now_ns=T0)
    assert str(order.time_in_force) == "IOC"
    assert str(order.client_order_id) == "a" * 32
    assert Decimal(str(order.quantity)) == Decimal(values["quantity"])
    assert Decimal(str(order.price)) == Decimal(values["price"])
    h.submit_order.assert_not_called()
    with pytest.raises(ValueError, match="FILL_TIME_PROTECTION"):
        port.dispatch(command, now_ns=T0)


@pytest.mark.parametrize("changes", [{"quantity": "200"}, {"price": "0"}, {"stop": "90"},
                                      {"reduce_only": True}, {"symbol": "BTCUSDT"}])
def test_entry_compiler_rejects_invalid_exact_action(changes):
    p, i, h = product(), identity(), host()
    values = payload(p, i, "a" * 32) | {"price": "100", "stop": "120", "reduce_only": False} | changes
    command = make_command(command_id="compile", intent_id="intent", command_type=CommandType.SUBMIT_ENTRY,
        payload_dict=values, expected_state_version=0, created_at_ns=T0)
    with pytest.raises(ValueError):
        qualified_port(p, i, h).compile_entry_ioc(command, now_ns=T0)
    h.submit_order.assert_not_called()


def test_unverified_account_stale_account_wrong_reduction_and_market_filters_fail_closed():
    p, i, h = product(), identity(), host()
    command = make_command(command_id="close", intent_id="intent", command_type=CommandType.FLATTEN,
        payload_dict=payload(p, i, "a" * 32), expected_state_version=0, created_at_ns=T0)
    with pytest.raises(ValueError, match="PROFILE_NOT_VERIFIED"):
        BinanceNautilusDemoPort(i, h, p).dispatch(command, now_ns=T0)
    with pytest.raises(ValueError, match="PROFILE_STALE"):
        qualified_port(p, i, h).dispatch(command, now_ns=T0 + 3_000_000_000)
    port = qualified_port(p, i, h)
    port.market_filters = replace(port.market_filters, max_qty=Decimal("0.5"))
    with pytest.raises(ValueError, match="market quantity"):
        port.dispatch(command, now_ns=T0)
    command = make_command(command_id="wrong-side", intent_id="intent", command_type=CommandType.FLATTEN,
        payload_dict=payload(p, i, "a" * 32) | {"side": "BUY"}, expected_state_version=0, created_at_ns=T0)
    with pytest.raises(ValueError, match="POSITION_SCOPE"):
        qualified_port(p, i, h).dispatch(command, now_ns=T0)
    h.submit_order.assert_not_called()
