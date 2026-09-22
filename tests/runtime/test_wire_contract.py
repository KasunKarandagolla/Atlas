from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.enums import Side
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
    entry = build_entry_wire_contract(_plan(), Decimal("50000"), Decimal("48000"), Decimal("0.01"))
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
        "slTriggerBy": "MarkPrice",
        "slOrderType": "Market",
        "tpslMode": "Full",
    }


def test_exit_has_explicit_quantity_and_market_has_no_fake_price():
    normal = build_exit_wire_contract(_plan(), Decimal("48900"), Decimal("0.01"))
    emergency = build_market_exit_wire_contract(_plan(), Decimal("0.01"))
    assert normal.to_bybit_params()["reduceOnly"] is True
    assert normal.to_bybit_params()["positionIdx"] == 0
    assert emergency.price is None
    assert "price" not in emergency.to_bybit_params()
    with pytest.raises(ValueError):
        build_market_exit_wire_contract(_plan(), Decimal("0"))
