"""Complete pure offline replay of the frozen IOC/stop/T+24h policy.

Frozen replay conventions for minute-only history:

* entry may only use the first complete minute after arrival (a bar already in
  progress at send/arrival is never fillable);
* a triggered MarkPrice stop can only execute in a later minute bar, at or after
  ``trigger + resolution + supplied latency``;
* the T+24h exit escalates as: bounded 25bp IOC, refreshed bounded retry,
  two-second market escalation, then extended exposure / declared bound /
  ``NOT_ESTIMABLE``.

The replay never chases, reverses, pyramids, moves the stop or resets the
horizon, and quantity is conserved at every step.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.enums import Side
from atlas.science.execution_replay import (
    MINUTE_NS,
    ExecutionEvidenceStatus,
    Fill,
    FillStatus,
    ReplayMinute,
    executable_stop_bounds,
    first_complete_minute,
    ioc_entry,
    linear_pnl,
    stop_execution_minute,
)
from atlas.science.funding import FundingSettlement, funding_cashflow
from atlas.strategy.policy import FixedPolicy, time_exit_collar


@dataclass(frozen=True)
class ReplayAssumptions:
    decision_to_venue_ns: int
    human_delay_ns: int
    tick: Decimal
    taker_fee_rate: Decimal
    stop_spread_impact: Decimal
    time_exit_market_escalation_supported: bool
    extension_bound_supported: bool
    escalation_delay_ns: int = 2_000_000_000
    stop_latency_ns: int = 0
    minute_ns: int = MINUTE_NS


@dataclass(frozen=True)
class PolicyReplayResult:
    status: FillStatus | None
    evidence_status: ExecutionEvidenceStatus
    entry_fill: Fill | None
    exit_fills: tuple[Fill, ...]
    stop_fills: tuple[Fill, ...]
    time_exit_fills: tuple[Fill, ...]
    time_exit_retry_fills: tuple[Fill, ...]
    escalation_fills: tuple[Fill, ...]
    funding_costs: tuple[Decimal, ...]
    filled_qty: Decimal
    remaining_qty: Decimal
    pnl: Decimal | None
    reason: str
    bounded_extension: bool = False

    @property
    def closed(self) -> bool:
        return self.remaining_qty == 0

    def outcome_status(self) -> FillStatus | None:
        """Matured execution outcome used by the decision calendar."""
        if self.entry_fill is None:
            return self.status
        if self.remaining_qty > 0 or self.bounded_extension:
            return FillStatus.EXTENDED_EXIT
        stop_qty = sum((fill.quantity for fill in self.stop_fills), Decimal("0"))
        if stop_qty == self.filled_qty:
            return FillStatus.STOP_EXIT
        if self.escalation_fills:
            return FillStatus.EXTENDED_EXIT
        return FillStatus.TIME_EXIT


def _bounded_exit(side: Side, qty: Decimal, minute: ReplayMinute, collar: Decimal | None,
                  fee_rate: Decimal) -> Fill | None:
    """One IOC lot bounded by the opposite displayed depth and the protective collar."""
    if qty <= 0 or not minute.available or minute.bid is None or minute.ask is None:
        return None
    price, depth = (minute.bid, minute.bid_depth) if side is Side.LONG else (minute.ask, minute.ask_depth)
    if depth is None or depth <= 0:
        return None
    if collar is not None and ((side is Side.LONG and price < collar) or (side is Side.SHORT and price > collar)):
        return None
    filled = min(qty, depth)
    return None if filled <= 0 else Fill(filled, price, filled * price * fee_rate, minute.at_ns)


def replay_policy(policy: FixedPolicy, minutes: tuple[ReplayMinute, ...], settlements: tuple[FundingSettlement, ...],
                  assumptions: ReplayAssumptions) -> PolicyReplayResult:
    """Replay the complete frozen lifecycle for one already-frozen policy."""
    empty: tuple[Decimal, ...] = ()
    if not minutes:
        return PolicyReplayResult(None, ExecutionEvidenceStatus.NOT_ESTIMABLE, None, (), (), (), (), (), empty,
                                  Decimal("0"), Decimal("0"), None, "empty replay path")
    arrival = policy.created_at_ns + assumptions.decision_to_venue_ns + assumptions.human_delay_ns
    entry_minute = first_complete_minute(minutes, arrival_ns=arrival, minute_ns=assumptions.minute_ns)
    if entry_minute is None:
        return PolicyReplayResult(None, ExecutionEvidenceStatus.NOT_ESTIMABLE, None, (), (), (), (), (), empty,
                                  Decimal("0"), Decimal("0"), None, "no complete minute after arrival")
    entry = ioc_entry(policy.side, policy.quantity, policy.entry_collar, entry_minute,
                      taker_fee_rate=assumptions.taker_fee_rate)
    if entry.evidence_status is not ExecutionEvidenceStatus.EXECUTABLE:
        return PolicyReplayResult(None, entry.evidence_status, None, (), (), (), (), (), empty,
                                  Decimal("0"), Decimal("0"), None, entry.reason)
    if entry.entry is None:
        return PolicyReplayResult(FillStatus.NO_FILL, ExecutionEvidenceStatus.EXECUTABLE, None, (), (), (), (), (),
                                  empty, Decimal("0"), Decimal("0"), Decimal("0"), "IOC no fill")

    filled_qty = entry.entry.quantity
    remaining = filled_qty
    stop_fills: list[Fill] = []
    time_fills: list[Fill] = []
    retry_fills: list[Fill] = []
    escalation_fills: list[Fill] = []
    evidence = ExecutionEvidenceStatus.EXECUTABLE
    reason = "closed"

    def build(status: FillStatus | None = None, evidence_status: ExecutionEvidenceStatus = ExecutionEvidenceStatus.EXECUTABLE,
              why: str = "closed", pnl: Decimal | None = None, bounded: bool = False) -> PolicyReplayResult:
        exits = tuple(stop_fills + time_fills + retry_fills + escalation_fills)
        return PolicyReplayResult(status, evidence_status, entry.entry, exits, tuple(stop_fills), tuple(time_fills),
                                  tuple(retry_fills), tuple(escalation_fills), empty, filled_qty, remaining, pnl, why,
                                  bounded)

    # 1. Fixed absolute MarkPrice stop, executed in a later bar with latency applied.
    for minute in minutes:
        if minute.at_ns < entry_minute.at_ns or minute.at_ns >= policy.horizon_end_ns or remaining <= 0:
            continue
        probe = executable_stop_bounds(policy.side, remaining, policy.stop, minute,
                                       spread_impact=assumptions.stop_spread_impact,
                                       taker_fee_rate=assumptions.taker_fee_rate)
        if not probe.triggered:
            continue
        execution = stop_execution_minute(minutes, trigger=minute, latency_ns=assumptions.stop_latency_ns,
                                          minute_ns=assumptions.minute_ns)
        if execution is None or execution.at_ns >= policy.horizon_end_ns:
            # The trigger cannot execute before the horizon: the position proceeds
            # to the frozen T+24h exit rather than inventing a trigger-minute fill.
            break
        bounds = executable_stop_bounds(policy.side, remaining, policy.stop, execution,
                                        spread_impact=assumptions.stop_spread_impact,
                                        taker_fee_rate=assumptions.taker_fee_rate)
        if bounds.adverse is None:
            return build(None, bounds.evidence_status, "stop execution unsupported")
        depth = execution.bid_depth if policy.side is Side.LONG else execution.ask_depth
        if depth is None:
            return build(None, ExecutionEvidenceStatus.NOT_ESTIMABLE, "stop depth unavailable")
        quantity = min(remaining, depth)
        if quantity <= 0:
            continue
        stop_fills.append(Fill(quantity, bounds.adverse.price,
                               quantity * bounds.adverse.price * assumptions.taker_fee_rate, execution.at_ns))
        remaining -= quantity

    # 2. Frozen T+24h time exit: bounded IOC, then a refreshed bounded retry.
    first_attempt = next((x for x in minutes if x.at_ns >= policy.horizon_end_ns), None)
    if remaining > 0:
        if first_attempt is None or first_attempt.bid is None or first_attempt.ask is None:
            return build(None, ExecutionEvidenceStatus.NOT_ESTIMABLE, "time-exit evidence unavailable")
        collar = time_exit_collar(policy.side, first_attempt.bid, first_attempt.ask, assumptions.tick)
        first = _bounded_exit(policy.side, remaining, first_attempt, collar, assumptions.taker_fee_rate)
        if first is not None:
            time_fills.append(first)
            remaining -= first.quantity
    retry: ReplayMinute | None = None
    if remaining > 0 and first_attempt is not None:
        retry = next((x for x in minutes if x.at_ns > first_attempt.at_ns), None)
        if retry is not None and retry.bid is not None and retry.ask is not None:
            collar = time_exit_collar(policy.side, retry.bid, retry.ask, assumptions.tick)
            refreshed = _bounded_exit(policy.side, remaining, retry, collar, assumptions.taker_fee_rate)
            if refreshed is not None:
                retry_fills.append(refreshed)
                remaining -= refreshed.quantity

    # 3. Frozen two-second escalation, then extended exposure.
    escalation_anchor = policy.horizon_end_ns + assumptions.escalation_delay_ns
    if retry is not None:
        escalation_anchor = max(escalation_anchor, retry.at_ns + assumptions.minute_ns)
    if remaining > 0:
        escalation = next((x for x in minutes if x.at_ns >= escalation_anchor), None)
        if assumptions.time_exit_market_escalation_supported and escalation is not None:
            market = _bounded_exit(policy.side, remaining, escalation, None, assumptions.taker_fee_rate)
            if market is not None:
                escalation_fills.append(market)
                remaining -= market.quantity
        if remaining > 0 and not assumptions.extension_bound_supported:
            return build(FillStatus.EXTENDED_EXIT, ExecutionEvidenceStatus.NOT_ESTIMABLE, "unbounded exit extension")

    exits = tuple(stop_fills + time_fills + retry_fills + escalation_fills)
    total_exit = sum((fill.quantity for fill in exits), Decimal("0"))
    if total_exit > filled_qty or remaining != filled_qty - total_exit or remaining < 0:
        raise ValueError("quantity conservation/reversal violation")

    close_at = minutes[-1].at_ns + 1
    if remaining == 0:
        close_at = max((fill.at_ns for fill in exits if fill.at_ns is not None), default=policy.horizon_end_ns) + 1

    def quantity_at(at_ns: int) -> Decimal:
        exited = sum((fill.quantity for fill in exits if fill.at_ns is not None and fill.at_ns <= at_ns), Decimal("0"))
        return filled_qty - exited

    funding = tuple(funding_cashflow(policy.side, quantity_at(settlement.at_ns), settlement)
                    for settlement in settlements
                    if entry_minute.at_ns <= settlement.at_ns < close_at and quantity_at(settlement.at_ns) > 0)
    bounded_extension = False
    if remaining > 0:
        # Explicit declared conservative bound for still-open exposure, valued at
        # the worst observed executable price of the remaining evidence.
        tail = [x for x in minutes if x.at_ns >= escalation_anchor]
        tail_bounds = [x.last_low if policy.side is Side.LONG else x.last_high for x in tail] or [
            first_attempt.last_low if first_attempt else policy.mark_reference
        ]
        bound_price = min(tail_bounds) if policy.side is Side.LONG else max(tail_bounds)
        exits = exits + (Fill(remaining, bound_price, remaining * bound_price * assumptions.taker_fee_rate,
                              minutes[-1].at_ns),)
        remaining = Decimal("0")
        bounded_extension = True
        reason = "extended exposure conservatively bounded"
    pnl = linear_pnl(policy.side, (entry.entry,), exits, funding)
    return PolicyReplayResult(entry.entry_status, evidence, entry.entry, exits, tuple(stop_fills), tuple(time_fills),
                              tuple(retry_fills), tuple(escalation_fills), funding, filled_qty, remaining, pnl, reason,
                              bounded_extension)
