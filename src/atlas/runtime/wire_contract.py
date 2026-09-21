"""Logical wire contract builder (freeze §1.3).

Deterministic builder for expected entry/exit wire fields from immutable plan data.
NOT transport - produces expected-wire evidence for later real adapter qualification.

Entry expectation (freeze §1.3):
- LIMIT, IOC, position_idx=0
- stop_loss = approved absolute stop
- sl_trigger_by = MarkPrice
- sl_order_type = Market
- tpsl_mode = Full
- reduce_only = false

Exit contract expectation:
- Explicit quantity, opposite side
- reduce_only = true
- Never zero-quantity "close all"
- No reversal semantics
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.enums import Side
from atlas.domain.money import canonical_decimal_str
from atlas.domain.trade_plan import TradePlan


@dataclass(frozen=True)
class EntryWireContract:
    """Expected logical entry order fields for wire qualification."""

    instrument: str
    order_type: str = "LIMIT"
    time_in_force: str = "IOC"
    side: str = ""  # "Buy" | "Sell"
    quantity: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    reduce_only: bool = False
    position_idx: int = 0
    stop_loss: Decimal = Decimal("0")
    sl_trigger_by: str = "MarkPrice"
    sl_order_type: str = "Market"
    tpsl_mode: str = "Full"

    def __post_init__(self) -> None:
        if not self.instrument or not self.instrument.strip():
            raise ValueError("instrument must be non-blank")
        if self.order_type not in ("LIMIT", "MARKET"):
            raise ValueError("order_type must be LIMIT or MARKET")
        if self.time_in_force not in ("IOC", "GTC", "FOK"):
            raise ValueError("time_in_force must be IOC, GTC, or FOK")
        if self.side not in ("Buy", "Sell"):
            raise ValueError("side must be Buy or Sell")
        if not isinstance(self.quantity, Decimal) or self.quantity <= 0:
            raise ValueError("quantity must be positive Decimal")
        if not isinstance(self.price, Decimal) or self.price <= 0:
            raise ValueError("price must be positive Decimal")
        if not isinstance(self.reduce_only, bool):
            raise ValueError("reduce_only must be bool")
        if not isinstance(self.position_idx, int) or isinstance(self.position_idx, bool) or self.position_idx < 0:
            raise ValueError("position_idx must be int >= 0")
        if not isinstance(self.stop_loss, Decimal) or self.stop_loss <= 0:
            raise ValueError("stop_loss must be positive Decimal")
        if self.sl_trigger_by not in ("MarkPrice", "LastPrice", "IndexPrice"):
            raise ValueError("sl_trigger_by must be MarkPrice, LastPrice, or IndexPrice")
        if self.sl_order_type not in ("Market", "Limit"):
            raise ValueError("sl_order_type must be Market or Limit")
        if self.tpsl_mode not in ("Full", "Partial"):
            raise ValueError("tpsl_mode must be Full or Partial")

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "side": self.side,
            "quantity": canonical_decimal_str(self.quantity),
            "price": canonical_decimal_str(self.price),
            "reduce_only": self.reduce_only,
            "position_idx": self.position_idx,
            "stop_loss": canonical_decimal_str(self.stop_loss),
            "sl_trigger_by": self.sl_trigger_by,
            "sl_order_type": self.sl_order_type,
            "tpsl_mode": self.tpsl_mode,
        }

    def to_bybit_params(self) -> dict[str, Any]:
        """Format as expected Bybit V5 order parameters."""
        return {
            "symbol": self.instrument,
            "side": self.side,
            "orderType": self.order_type,
            "timeInForce": self.time_in_force,
            "qty": canonical_decimal_str(self.quantity),
            "price": canonical_decimal_str(self.price),
            "reduceOnly": self.reduce_only,
            "positionIdx": self.position_idx,
            "stopLoss": canonical_decimal_str(self.stop_loss),
            "slTriggerBy": self.sl_trigger_by,
            "slOrderType": self.sl_order_type,
            "tpslMode": self.tpsl_mode,
        }


@dataclass(frozen=True)
class ExitWireContract:
    """Expected logical exit order fields for wire qualification."""

    instrument: str
    order_type: str = "LIMIT"
    time_in_force: str = "IOC"
    side: str = ""  # "Buy" | "Sell" (opposite of entry)
    quantity: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    reduce_only: bool = True

    def __post_init__(self) -> None:
        if not self.instrument or not self.instrument.strip():
            raise ValueError("instrument must be non-blank")
        if self.order_type not in ("LIMIT", "MARKET"):
            raise ValueError("order_type must be LIMIT or MARKET")
        if self.time_in_force not in ("IOC", "GTC", "FOK"):
            raise ValueError("time_in_force must be IOC, GTC, or FOK")
        if self.side not in ("Buy", "Sell"):
            raise ValueError("side must be Buy or Sell")
        if not isinstance(self.quantity, Decimal) or self.quantity <= 0:
            raise ValueError("quantity must be positive Decimal (explicit, never zero)")
        if not isinstance(self.price, Decimal) or self.price <= 0:
            raise ValueError("price must be positive Decimal")
        if not isinstance(self.reduce_only, bool) or not self.reduce_only:
            raise ValueError("reduce_only must be True for exit")

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "side": self.side,
            "quantity": canonical_decimal_str(self.quantity),
            "price": canonical_decimal_str(self.price),
            "reduce_only": self.reduce_only,
        }

    def to_bybit_params(self) -> dict[str, Any]:
        """Format as expected Bybit V5 order parameters."""
        return {
            "symbol": self.instrument,
            "side": self.side,
            "orderType": self.order_type,
            "timeInForce": self.time_in_force,
            "qty": canonical_decimal_str(self.quantity),
            "price": canonical_decimal_str(self.price),
            "reduceOnly": self.reduce_only,
        }


def build_entry_wire_contract(
    plan: TradePlan,
    collar_price: Decimal,
    stop_price: Decimal,
    quantity: Decimal,
) -> EntryWireContract:
    """Build entry wire contract from approved plan data."""
    side_str = "Buy" if plan.side == Side.LONG else "Sell"
    return EntryWireContract(
        instrument=f"{plan.instrument}-LINEAR.BYBIT",
        side=side_str,
        quantity=quantity,
        price=collar_price,
        stop_loss=stop_price,
    )


def build_exit_wire_contract(
    plan: TradePlan,
    exit_price: Decimal,
    quantity: Decimal,
) -> ExitWireContract:
    """Build exit wire contract from approved plan data.

    Exit side is opposite of entry side.
    """
    exit_side = "Sell" if plan.side == Side.LONG else "Buy"
    return ExitWireContract(
        instrument=f"{plan.instrument}-LINEAR.BYBIT",
        side=exit_side,
        quantity=quantity,
        price=exit_price,
    )


def build_market_exit_wire_contract(
    plan: TradePlan,
    quantity: Decimal,
) -> ExitWireContract:
    """Build market exit wire contract (no price, MARKET order type)."""
    exit_side = "Sell" if plan.side == Side.LONG else "Buy"
    return ExitWireContract(
        instrument=f"{plan.instrument}-LINEAR.BYBIT",
        order_type="MARKET",
        time_in_force="IOC",
        side=exit_side,
        quantity=quantity,
        price=Decimal("0"),  # Not used for market orders
    )
