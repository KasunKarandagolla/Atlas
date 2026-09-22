from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.enums import CommandType, Side
from atlas.domain.execution import make_command
from atlas.domain.trade_plan import TradePlan
from atlas.runtime.wire_contract import (
    build_entry_wire_contract,
    build_exit_wire_contract,
    build_market_exit_wire_contract,
)


def _plan() -> TradePlan:
    return TradePlan(
        plan_id="p",
        version="v1",
        policy_hash="p",
        snapshot_hash="s",
        expires_at_ns=1_700_000_060_000_000_000,
        market="BYBIT",
        account_scope="offline",
        instrument="BTCUSDT",
        side=Side.LONG,
        qty_limit=Decimal("0.01"),
        entry_policy="IOC_LIMIT_FULL_STOP",
        collar=Decimal("50000"),
        stop=Decimal("48000"),
        stop_trigger_basis="MarkPrice",
        management_policy="exit",
        horizon_end_ns=1_700_100_000_000_000_000,
        cost_distribution_ref="c",
        normal_risk=Decimal("1"),
        stress_risk=Decimal("2"),
        margin=Decimal("3"),
        leverage_bound=Decimal("2"),
        risk_config_hash="r",
        created_at_ns=1_700_000_000_000_000_000,
        available_at_ns=1_700_000_000_000_000_000,
        reference_price=Decimal("49000"),
    )


def test_entry_contract_has_frozen_fields():
    client_order_id = "a" * 32
    entry = build_entry_wire_contract(_plan(), Decimal("50000"), Decimal("48000"), Decimal("0.01"),
                                      client_order_id)
    assert entry.to_bybit_params() == {
        "symbol": "BTCUSDT-LINEAR.BYBIT",
        "side": "Buy",
        "orderType": "LIMIT",
        "timeInForce": "IOC",
        "qty": "0.01",
        "price": "50000",
        "reduceOnly": False,
        "positionIdx": 0,
        "stopLoss": "48000",
        "orderLinkId": client_order_id,
        "slTriggerBy": "MarkPrice",
        "slOrderType": "Market",
        "tpslMode": "Full",
    }


def test_entry_payload_identity_changes_with_client_order_id():
    first = build_entry_wire_contract(_plan(), Decimal("50000"), Decimal("48000"), Decimal("0.01"), "a" * 32)
    second = build_entry_wire_contract(_plan(), Decimal("50000"), Decimal("48000"), Decimal("0.01"), "b" * 32)
    assert first.to_bybit_params()["orderLinkId"] != second.to_bybit_params()["orderLinkId"]
    with pytest.raises(ValueError):
        build_entry_wire_contract(_plan(), Decimal("50000"), Decimal("48000"), Decimal("0.01"), "NOT-HEX")


def test_entry_command_hash_binds_client_order_id():
    first = make_command(command_id="c1", intent_id="i1", command_type=CommandType.SUBMIT_ENTRY,
                         payload_dict={"orderLinkId": "a" * 32, "qty": "0.01"}, expected_state_version=0,
                         created_at_ns=1)
    second = make_command(command_id="c2", intent_id="i1", command_type=CommandType.SUBMIT_ENTRY,
                          payload_dict={"orderLinkId": "b" * 32, "qty": "0.01"}, expected_state_version=0,
                          created_at_ns=1)
    assert first.exact_payload_hash != second.exact_payload_hash


def test_exit_has_explicit_quantity_and_market_has_no_fake_price():
    normal = build_exit_wire_contract(_plan(), Decimal("48900"), Decimal("0.01"))
    emergency = build_market_exit_wire_contract(_plan(), Decimal("0.01"))
    assert normal.to_bybit_params()["reduceOnly"] is True
    assert normal.to_bybit_params()["positionIdx"] == 0
    assert emergency.price is None
    assert "price" not in emergency.to_bybit_params()
    with pytest.raises(ValueError):
        build_market_exit_wire_contract(_plan(), Decimal("0"))
