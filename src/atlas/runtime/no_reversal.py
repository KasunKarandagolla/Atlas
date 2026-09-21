"""No-reversal invariant (freeze §1.7).

For original signed position q and close execution Δq enforce:
  q * Δq <= 0
  abs(Δq) <= abs(q)
  q + Δq == 0 OR sign(q + Δq) == sign(q)

Use Decimal/quantized final quantities.
This is local validation plus future venue qualification; do not claim venue enforcement.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.money import ensure_decimal


@dataclass(frozen=True)
class ReversalCheckResult:
    """Result of no-reversal invariant check."""
    allowed: bool
    original_qty: Decimal
    close_qty: Decimal
    resulting_qty: Decimal
    violations: tuple[str, ...]

    def __bool__(self) -> bool:
        return self.allowed


def check_no_reversal(
    original_signed_qty: Decimal,
    close_execution_qty: Decimal,
    close_side: str,  # "Buy" | "Sell" - side of the closing execution
) -> ReversalCheckResult:
    """Check no-reversal invariant for a close execution.

    Args:
        original_signed_qty: Original position (positive = long, negative = short)
        close_execution_qty: Quantity of the closing execution (always positive)
        close_side: Side of the closing execution ("Buy" to close short, "Sell" to close long)

    Returns:
        ReversalCheckResult with allowed=True if invariant holds.
    """
    q = ensure_decimal(original_signed_qty, field="original_signed_qty")
    dq = ensure_decimal(close_execution_qty, field="close_execution_qty")

    if dq <= 0:
        return ReversalCheckResult(
            allowed=False,
            original_qty=q,
            close_qty=dq,
            resulting_qty=q,
            violations=("close_execution_qty must be positive",),
        )

    # Determine Δq (signed change from close execution)
    # Sell execution reduces long (negative Δq), Buy execution reduces short (positive Δq)
    if close_side == "Sell":
        delta_q = -dq  # Selling reduces position
    elif close_side == "Buy":
        delta_q = dq   # Buying increases position (reduces short)
    else:
        return ReversalCheckResult(
            allowed=False,
            original_qty=q,
            close_qty=dq,
            resulting_qty=q,
            violations=(f"invalid close_side: {close_side}",),
        )

    violations: list[str] = []

    # Invariant 1: q * Δq <= 0 (must be opposite sign or zero)
    if q * delta_q > 0:
        violations.append(f"q * Δq > 0: {q} * {delta_q} = {q * delta_q} (must be <= 0)")

    # Invariant 2: |Δq| <= |q| (cannot close more than exists)
    if abs(delta_q) > abs(q):
        violations.append(f"|Δq| > |q|: {abs(delta_q)} > {abs(q)}")

    # Invariant 3: q + Δq == 0 OR sign(q + Δq) == sign(q)
    resulting = q + delta_q
    if resulting != 0:
        # Check if sign is preserved (allow partial close)
        if q > 0 and resulting < 0:
            violations.append(f"reversal: long {q} -> short {resulting}")
        elif q < 0 and resulting > 0:
            violations.append(f"reversal: short {q} -> long {resulting}")
        # If q == 0, any close is invalid (handled by |Δq| <= |q|)

    allowed = len(violations) == 0

    return ReversalCheckResult(
        allowed=allowed,
        original_qty=q,
        close_qty=dq,
        resulting_qty=resulting,
        violations=tuple(violations),
    )


def check_no_reversal_partial(
    original_signed_qty: Decimal,
    close_executions: list[tuple[Decimal, str]],  # (qty, side)
) -> list[ReversalCheckResult]:
    """Check no-reversal for a sequence of partial close executions.

    Each execution is checked against the remaining position at that point.
    """
    results: list[ReversalCheckResult] = []
    remaining = ensure_decimal(original_signed_qty, field="original_signed_qty")

    for close_qty, close_side in close_executions:
        result = check_no_reversal(remaining, close_qty, close_side)
        results.append(result)
        if not result.allowed:
            break
        remaining = result.resulting_qty

    return results


@dataclass(frozen=True)
class StopCloseRaceResult:
    """Result of stop/manual-close race bookkeeping."""
    stop_execution: ReversalCheckResult | None
    manual_close_execution: ReversalCheckResult | None
    final_qty: Decimal
    race_detected: bool
    race_resolved: bool  # True if exchange clamped/rejected surplus


def simulate_stop_close_race(
    original_signed_qty: Decimal,
    stop_trigger_price: Decimal,
    manual_close_qty: Decimal,
    manual_close_side: str,
    stop_fills_first: bool,
) -> StopCloseRaceResult:
    """Simulate stop and manual close race.

    The exchange must clamp/reject the surplus rather than reverse.
    This simulates the expected venue behavior.
    """
    q = ensure_decimal(original_signed_qty, field="original_signed_qty")
    manual_qty = ensure_decimal(manual_close_qty, field="manual_close_qty")

    if stop_fills_first:
        # Stop executes first - closes entire position
        stop_side = "Sell" if q > 0 else "Buy"
        stop_result = check_no_reversal(q, abs(q), stop_side)
        if not stop_result.allowed:
            return StopCloseRaceResult(
                stop_execution=stop_result,
                manual_close_execution=None,
                final_qty=q,
                race_detected=True,
                race_resolved=False,
            )
        # Position now flat - manual close should be rejected by exchange
        manual_result = check_no_reversal(Decimal("0"), manual_qty, manual_close_side)
        final_qty = Decimal("0")
        race_resolved = not manual_result.allowed  # Exchange rejects surplus
    else:
        # Manual close executes first
        manual_result = check_no_reversal(q, manual_qty, manual_close_side)
        if not manual_result.allowed:
            return StopCloseRaceResult(
                stop_execution=None,
                manual_close_execution=manual_result,
                final_qty=q,
                race_detected=True,
                race_resolved=False,
            )
        # Stop fires on remaining position
        remaining = manual_result.resulting_qty
        if remaining == 0:
            # Position flat - stop should be cancelled by exchange
            stop_result = None
            final_qty = Decimal("0")
            race_resolved = True
        else:
            stop_side = "Sell" if remaining > 0 else "Buy"
            stop_result = check_no_reversal(remaining, abs(remaining), stop_side)
            final_qty = stop_result.resulting_qty if stop_result.allowed else remaining
            race_resolved = stop_result.allowed

    return StopCloseRaceResult(
        stop_execution=stop_result,
        manual_close_execution=manual_result if not stop_fills_first else None,
        final_qty=final_qty,
        race_detected=True,
        race_resolved=race_resolved,
    )


def check_late_entry_reopening(
    original_signed_qty: Decimal,
    close_executions: list[tuple[Decimal, str]],
    late_entry_qty: Decimal,
    late_entry_side: str,
) -> tuple[bool, str]:
    """Check late-entry reopening prevention.

    After close executions, a late entry on the same side as original
    should be treated as new epoch, not reversal.
    """
    results = check_no_reversal_partial(original_signed_qty, close_executions)
    final_qty = results[-1].resulting_qty if results else original_signed_qty

    # Late entry must be on opposite side of original to be reversal
    # If same side as original, it's a new position (new epoch)
    # If opposite side, it could be reversal
    original_side = "Buy" if original_signed_qty > 0 else "Sell"
    if late_entry_side == original_side:
        return True, "new epoch (same side as original)"
    else:
        # Opposite side - check if it would reverse
        result = check_no_reversal(final_qty, late_entry_qty, late_entry_side)
        if result.allowed and result.resulting_qty != 0 and (
            (final_qty > 0 and result.resulting_qty < 0) or
            (final_qty < 0 and result.resulting_qty > 0)
        ):
            return False, "reversal detected"
        return True, "partial close or new epoch"
