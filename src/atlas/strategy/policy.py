"""Decimal exchange policy construction; this is a research representation only."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from math import exp, sqrt

from atlas.domain.enums import Side
from atlas.domain.money import ensure_positive_decimal

from .crypto_trend_24h_v1 import HORIZON_NS


class PolicyRejected(ValueError):
    pass


def round_down(value: Decimal, increment: Decimal) -> Decimal:
    return (value / increment).to_integral_value(rounding=ROUND_DOWN) * increment


def round_up(value: Decimal, increment: Decimal) -> Decimal:
    return (value / increment).to_integral_value(rounding=ROUND_UP) * increment


@dataclass(frozen=True)
class VenueFilters:
    tick: Decimal
    lot: Decimal
    min_qty: Decimal

    def __post_init__(self) -> None:
        for x in (self.tick, self.lot, self.min_qty):
            ensure_positive_decimal(x)


@dataclass(frozen=True)
class FixedPolicy:
    side: Side
    quantity: Decimal
    entry_collar: Decimal
    stop: Decimal
    mark_reference: Decimal
    h: float
    decision_slot_at_ns: int
    created_at_ns: int
    horizon_end_ns: int
    time_exit_bps: Decimal = Decimal("0.0025")
    stop_trigger_basis: str = "MarkPrice"
    entry_policy: str = "IOC_LIMIT"
    management_policy: str = "ABSOLUTE_FIXED_STOP_NO_TP_NO_PYRAMID"


def entry_collar(side: Side, bid: Decimal, ask: Decimal, tick: Decimal) -> Decimal:
    if side is Side.LONG:
        return round_down(ask * Decimal("1.001"), tick)
    return round_up(bid * Decimal("0.999"), tick)


def stop_distance(sigma: float) -> float:
    return 2 * sigma * sqrt(24)


def fixed_policy(
    side: Side, qty: Decimal, bid: Decimal, ask: Decimal, mark: Decimal, sigma: float,
    filters: VenueFilters, decision_slot_at_ns: int, created_at_ns: int,
) -> FixedPolicy:
    if created_at_ns < decision_slot_at_ns or created_at_ns > decision_slot_at_ns + 60_000_000_000:
        raise PolicyRejected("plan creation must be in frozen decision-slot TTL")
    h = stop_distance(sigma)
    if h < 0.0025 or h > 0.10:
        raise PolicyRejected("stop distance outside frozen eligibility")
    quantity = round_down(qty, filters.lot)
    if quantity < filters.min_qty:
        raise PolicyRejected("MIN_SIZE")
    if side is Side.LONG:
        stop = round_up(mark * Decimal(str(exp(-h))), filters.tick)
        if stop >= mark:
            raise PolicyRejected("rounded long stop not protective")
    else:
        stop = round_down(mark * Decimal(str(exp(h))), filters.tick)
        if stop <= mark:
            raise PolicyRejected("rounded short stop not protective")
    return FixedPolicy(
        side, quantity, entry_collar(side, bid, ask, filters.tick), stop, mark, h,
        decision_slot_at_ns, created_at_ns, decision_slot_at_ns + HORIZON_NS,
    )


def time_exit_collar(side: Side, bid: Decimal, ask: Decimal, tick: Decimal) -> Decimal:
    """Protective 25 bp IOC exit collar beyond the current opposite quote."""
    if side is Side.LONG:
        return round_up(bid * Decimal("0.9975"), tick)
    return round_down(ask * Decimal("1.0025"), tick)
