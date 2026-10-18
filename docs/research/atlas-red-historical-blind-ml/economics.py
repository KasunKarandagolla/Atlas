"""Explicit unit-notional arithmetic sensitivities, not ATLAS action receipts."""

from __future__ import annotations

from decimal import Decimal

import common

from atlas.v2.science.costs import fee_cash

D = Decimal
PROGRAMME_EXPENSE = D("0.02") / D(10000)


def require_frozen_deadline(*, ready_ns, fill_ns, deadline_ns):
    if ready_ns > deadline_ns or fill_ns >= deadline_ns:
        raise ValueError("FROZEN_DEADLINE_CANNOT_INHERIT_DELAYED_REFERENCE")


def ambiguous_bar_bounds(*, stop_cash, target_cash, stop_seen, target_seen):
    if stop_seen and target_seen:
        return min(stop_cash, target_cash), max(stop_cash, target_cash), "UNRESOLVED_ORDERING"
    value = stop_cash if stop_seen else target_cash if target_seen else None
    return value, value, "OBSERVED_SINGLE_BOUNDARY" if value is not None else "NO_BOUNDARY"


def unit_cash(
    *,
    side,
    reference,
    endpoint,
    fees_bps,
    spread_bps,
    slippage_bps,
    funding_charge_bps=None,
    filled=True,
    price_includes_spread_slippage=False,
):
    """One hypothetical USDT reference notional; all charges explicitly assumed.

    Unknown funding keeps net cash null. Explicit numeric funding is a scenario
    assumption, never an inferred historical payment. Programme expense is per
    original opportunity, including rejections and no-fill, and charged once.
    """
    if side not in ("LONG", "SHORT"):
        raise ValueError("INVALID_SIDE")
    if price_includes_spread_slippage and (spread_bps or slippage_bps):
        raise ValueError("SOURCE_COST_DOUBLE_CHARGE")
    if any(D(str(v)) < 0 for v in (fees_bps, spread_bps, slippage_bps)):
        raise ValueError("INVALID_COST_ASSUMPTION")
    gross = fee = spread = slippage = funding = D(0)
    if filled:
        if reference is None or endpoint is None:
            raise ValueError("MISSING_PRICE_CANNOT_BECOME_ZERO")
        a, b = D(str(reference)), D(str(endpoint))
        if min(a, b) <= 0 or not a.is_finite() or not b.is_finite():
            raise ValueError("INVALID_PRICE")
        q = D(1) / a
        gross = (D(1) if side == "LONG" else D(-1)) * q * (b - a)
        fee = fee_cash(q, a, D(1), D(str(fees_bps)) / 10000) + fee_cash(q, b, D(1), D(str(fees_bps)) / 10000)
        spread = D(str(spread_bps)) / 10000
        slippage = D(str(slippage_bps)) / 10000
        funding = None if funding_charge_bps is None else D(str(funding_charge_bps)) / 10000
    before = gross - fee - spread - slippage - PROGRAMME_EXPENSE
    return {
        "gross_cash": gross,
        "fees_cash": fee,
        "spread_cash": spread,
        "slippage_cash": slippage,
        "funding_charge_cash": funding,
        "programme_expense_cash": PROGRAMME_EXPENSE,
        "before_unknown_funding_cash": before,
        "conditional_net_cash": None if funding is None else before - funding,
        "qualification": "ASSUMED_UNIT_NOTIONAL_RESEARCH; not exact-action or actual trading cash",
    }


def fixed_test_threshold(value=12.0):
    if value != 12.0:
        raise ValueError("TEST_INFORMED_THRESHOLD_CHANGE_FORBIDDEN")
    if common.protocol()["arms"]["RIDGE_CONTEXT"].find(">12 bps") < 0:
        raise ValueError("PROTOCOL_THRESHOLD_IDENTITY")
    return value
