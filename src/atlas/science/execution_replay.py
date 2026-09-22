"""Conservative offline IOC/stop/time-exit replay.  It has no transport dependency."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.enums import Side


class FillStatus(StrEnum):
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"
    STOP_EXIT = "STOP_EXIT"
    TIME_EXIT = "TIME_EXIT"
    EXTENDED_EXIT = "EXTENDED_EXIT"


class ExecutionEvidenceStatus(StrEnum):
    EXECUTABLE = "EXECUTABLE"
    NO_EXECUTION_DATA = "NO_EXECUTION_DATA"
    BOUND_ONLY = "BOUND_ONLY"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class Fill:
    quantity: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")


@dataclass(frozen=True)
class ReplayMinute:
    at_ns: int
    bid: Decimal | None
    ask: Decimal | None
    bid_depth: Decimal | None
    ask_depth: Decimal | None
    mark_low: Decimal
    mark_high: Decimal
    last_low: Decimal
    last_high: Decimal
    available: bool = True


@dataclass(frozen=True)
class ReplayResult:
    entry_status: FillStatus | None
    entry: Fill | None
    exit: Fill | None
    remaining_qty: Decimal
    exit_status: FillStatus | None
    reason: str = ""
    evidence_status: ExecutionEvidenceStatus = ExecutionEvidenceStatus.EXECUTABLE


def ioc_entry(side: Side, qty: Decimal, collar: Decimal, minute: ReplayMinute, participation: Decimal = Decimal("0.10"), taker_fee_rate: Decimal = Decimal("0")) -> ReplayResult:
    if not minute.available or minute.bid is None or minute.ask is None or minute.bid_depth is None or minute.ask_depth is None:
        # Missing causal quote/depth support is not an economically benign IOC.
        return ReplayResult(None, None, None, qty, None, "NO_EXECUTION_DATA", ExecutionEvidenceStatus.NO_EXECUTION_DATA)
    price, depth = (minute.ask, minute.ask_depth) if side is Side.LONG else (minute.bid, minute.bid_depth)
    permitted = price <= collar if side is Side.LONG else price >= collar
    if not permitted:
        return ReplayResult(FillStatus.NO_FILL, None, None, qty, None, "outside collar")
    filled = min(qty, depth * participation)
    if filled <= 0:
        return ReplayResult(FillStatus.NO_FILL, None, None, qty, None, "depth exhausted")
    fee = filled * price * taker_fee_rate
    return ReplayResult(FillStatus.FULL_FILL if filled == qty else FillStatus.PARTIAL_FILL, Fill(filled, price, fee), None, qty - filled, None)


def stop_triggered(side: Side, stop: Decimal, minute: ReplayMinute) -> bool:
    return minute.mark_low <= stop if side is Side.LONG else minute.mark_high >= stop


@dataclass(frozen=True)
class StopBounds:
    adverse: Fill | None
    favorable: Fill | None
    evidence_status: ExecutionEvidenceStatus
    triggered: bool


def executable_stop_bounds(
    side: Side, qty: Decimal, stop: Decimal, minute: ReplayMinute, *, spread_impact: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"), latency_minutes: int = 0,
) -> StopBounds:
    """Replay a triggered mark stop after the trigger, retaining bar-order bounds."""
    if not stop_triggered(side, stop, minute):
        return StopBounds(None, None, ExecutionEvidenceStatus.EXECUTABLE, False)
    if not minute.available or minute.bid is None or minute.ask is None:
        return StopBounds(None, None, ExecutionEvidenceStatus.NOT_ESTIMABLE, True)
    if spread_impact < 0:
        raise ValueError("spread/impact cannot be negative")
    # A gap is caught because the mark crossing is tested before executable price.
    # The adverse side explicitly deducts/adds spread-impact after the trigger.
    if side is Side.LONG:
        adverse_price = min(minute.bid, minute.last_low) - spread_impact
        favorable_price = minute.bid
    else:
        adverse_price = max(minute.ask, minute.last_high) + spread_impact
        favorable_price = minute.ask
    if adverse_price <= 0 or favorable_price <= 0:
        return StopBounds(None, None, ExecutionEvidenceStatus.NOT_ESTIMABLE, True)
    return StopBounds(
        Fill(qty, adverse_price, qty * adverse_price * taker_fee_rate),
        Fill(qty, favorable_price, qty * favorable_price * taker_fee_rate),
        ExecutionEvidenceStatus.EXECUTABLE, True,
    )


def executable_stop(side: Side, qty: Decimal, minute: ReplayMinute, taker_fee_rate: Decimal = Decimal("0")) -> Fill | None:
    """Compatibility primitive returning only the conservative mark-stop bound."""
    # A caller of this legacy helper must provide a triggered minute; its stop is
    # irrelevant to the previous primitive, so use the observed crossing bound.
    if not minute.available or minute.bid is None or minute.ask is None:
        return None
    price = min(minute.bid, minute.last_low) if side is Side.LONG else max(minute.ask, minute.last_high)
    return Fill(qty, price, qty * price * taker_fee_rate)


def linear_pnl(side: Side, entries: tuple[Fill, ...], exits: tuple[Fill, ...], funding: tuple[Decimal, ...] = ()) -> Decimal:
    direction = Decimal("1") if side is Side.LONG else Decimal("-1")
    gross = sum((direction * x.quantity * x.price for x in exits), Decimal("0")) - sum((direction * x.quantity * x.price for x in entries), Decimal("0"))
    fees = sum((x.fee for x in entries + exits), Decimal("0"))
    return gross - fees - sum(funding, Decimal("0"))
