"""Pure local no-reversal invariant (venue enforcement remains TEST GATE)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.money import ensure_decimal


@dataclass(frozen=True)
class ReversalCheckResult:
    allowed: bool
    original_qty: Decimal
    close_qty: Decimal
    resulting_qty: Decimal
    violations: tuple[str, ...]


def check_no_reversal(
    original_signed_qty: Decimal, close_execution_qty: Decimal, close_side: str
) -> ReversalCheckResult:
    q = ensure_decimal(original_signed_qty, field="original_signed_qty")
    qty = ensure_decimal(close_execution_qty, field="close_execution_qty")
    if qty <= 0:
        return ReversalCheckResult(False, q, qty, q, ("close_execution_qty must be positive",))
    if close_side not in ("Buy", "Sell"):
        return ReversalCheckResult(False, q, qty, q, ("invalid close_side",))
    dq = qty if close_side == "Buy" else -qty
    v = []
    if q * dq > 0:
        v.append("closing execution same sign as position")
    if abs(dq) > abs(q):
        v.append("close exceeds current position")
    resulting = q + dq
    if resulting and ((q > 0 > resulting) or (q < 0 < resulting)):
        v.append("reversal")
    return ReversalCheckResult(not v, q, qty, resulting, tuple(v))


def check_no_reversal_partial(
    original_signed_qty: Decimal, close_executions: list[tuple[Decimal, str]]
) -> list[ReversalCheckResult]:
    results = []
    current = original_signed_qty
    for qty, side in close_executions:
        result = check_no_reversal(current, qty, side)
        results.append(result)
        if result.allowed:
            current = result.resulting_qty
    return results


@dataclass(frozen=True)
class StopCloseRaceResult:
    stop_execution: ReversalCheckResult | None
    manual_close_execution: ReversalCheckResult | None
    final_qty: Decimal
    race_detected: bool
    race_resolved: bool


def simulate_stop_close_race(
    original_signed_qty: Decimal,
    stop_trigger_price: Decimal,
    manual_close_qty: Decimal,
    manual_close_side: str,
    stop_fills_first: bool,
) -> StopCloseRaceResult:
    del stop_trigger_price
    q = ensure_decimal(original_signed_qty, field="original_signed_qty")
    manual = ensure_decimal(manual_close_qty, field="manual_close_qty")
    if stop_fills_first:
        stop_side = "Sell" if q > 0 else "Buy"
        stop = check_no_reversal(q, abs(q), stop_side)
        manual_result = check_no_reversal(Decimal("0"), manual, manual_close_side)
        return StopCloseRaceResult(stop, manual_result, Decimal("0"), True, stop.allowed and not manual_result.allowed)
    manual_result = check_no_reversal(q, manual, manual_close_side)
    if not manual_result.allowed:
        return StopCloseRaceResult(None, manual_result, q, True, False)
    remaining = manual_result.resulting_qty
    if remaining == 0:
        return StopCloseRaceResult(None, manual_result, Decimal("0"), True, True)
    stop_side = "Sell" if remaining > 0 else "Buy"
    stop = check_no_reversal(remaining, abs(remaining), stop_side)
    return StopCloseRaceResult(
        stop, manual_result, stop.resulting_qty if stop.allowed else remaining, True, stop.allowed
    )


def check_late_entry_reopening(
    original_signed_qty: Decimal | None = None,
    close_executions: list[tuple[Decimal, str]] | None = None,
    late_entry_qty: Decimal | None = None,
    late_entry_side: str | None = None,
    *,
    prior_epoch_closed: bool = False,
) -> tuple[bool, str]:
    if not prior_epoch_closed:
        return False, "late fill belongs to prior unresolved epoch/recovery incident"
    if original_signed_qty is None or close_executions is None or late_entry_qty is None or late_entry_side is None:
        return True, "new epoch allowed only after certified CLOSED"
    results = check_no_reversal_partial(original_signed_qty, close_executions)
    final_qty = results[-1].resulting_qty if results else original_signed_qty
    original_side = "Buy" if original_signed_qty > 0 else "Sell"
    if late_entry_side == original_side:
        return True, "new epoch (same side as original)"
    result = check_no_reversal(final_qty, late_entry_qty, late_entry_side)
    if (
        result.allowed
        and result.resulting_qty != 0
        and ((final_qty > 0 > result.resulting_qty) or (final_qty < 0 < result.resulting_qty))
    ):
        return False, "reversal detected"
    return True, "partial close or new epoch"
