"""Research action sealing and full-lifecycle research evaluation.

These are *research* actions. No order is created, no account is touched, no
position is opened. An action is sealed only from qualified original-time
evidence; an incomplete exact action stays an incomplete research candidate and
says so. Lifecycle evaluation is a modelled path over genuinely observed future
bars, with explicit uncertainty where the live order queue is unobservable.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ...data.bars import CausalBarV2
from ...science.costs import FeeScheduleV2, fee_cash, funding_cash

ACTIONS_VERSION = "ATLAS_RED_FULL_CYCLE_RESEARCH_ACTION_V1"

# Binance USD-M published taker fee for the base (VIP 0) tier.
TAKER_FEE_RATE = Decimal("0.0005")
# Explicitly labelled research risk profile. It is not an account and it does
# not authorise any capital.
VIRTUAL_EQUITY_USDT = Decimal("10000")
RISK_FRACTION_PER_TRADE = Decimal("0.005")
RESEARCH_SLIPPAGE_BPS = Decimal("2.0")


@dataclass(frozen=True)
class ResearchActionV1:
    action_id: str
    cycle_id: str
    symbol: str
    strategy: str
    policy_id: str
    policy_hash: str
    side: str
    direction_origin: str
    entry_reference: Decimal
    entry_collar_bps: Decimal
    stop_price: Decimal
    stop_basis: str
    quantity: Decimal
    notional_usd: Decimal
    risk_profile: str
    horizon_end_ns: int
    deadline_ns: int
    take_profit_price: Decimal | None
    take_profit_policy: str
    fee_bps: Decimal
    expected_spread_bps: Decimal
    funding_assumption: str
    slippage_bps: Decimal
    hard_refusal_reason: str | None
    evidence_refs: tuple[str, ...]
    sealed_at_ns: int

    def as_row(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "cycle_id": self.cycle_id,
            "symbol": self.symbol,
            "strategy": self.strategy,
            "policy_id": self.policy_id,
            "policy_hash": self.policy_hash,
            "side": self.side,
            "direction_origin": self.direction_origin,
            "entry_reference": str(self.entry_reference),
            "entry_collar_bps": str(self.entry_collar_bps),
            "stop_price": str(self.stop_price),
            "stop_basis": self.stop_basis,
            "quantity": str(self.quantity),
            "notional_usd": str(self.notional_usd),
            "risk_profile": self.risk_profile,
            "horizon_end_ns": self.horizon_end_ns,
            "deadline_ns": self.deadline_ns,
            "take_profit_price": None if self.take_profit_price is None else str(self.take_profit_price),
            "take_profit_levels": [],
            "fee_bps": str(self.fee_bps),
            "expected_spread_bps": str(self.expected_spread_bps),
            "funding_assumption": self.funding_assumption,
            "slippage_bps": str(self.slippage_bps),
            "hard_refusal_reason": self.hard_refusal_reason,
            "evidence_refs": list(self.evidence_refs),
            "code_hash": ACTIONS_VERSION,
            "sealed_at_ns": self.sealed_at_ns,
        }


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("quantity step must be positive")
    return (value / step).to_integral_value(rounding="ROUND_FLOOR") * step


def seal_research_action(
    *, cycle_id: str, symbol: str, strategy: str, policy_id: str, policy_hash: str,
    side: str, entry_reference: Decimal, stop_price: Decimal, stop_basis: str,
    qty_step: Decimal, tick_size: Decimal, min_notional: Decimal | None,
    spread_bps: Decimal | None, atr_value: Decimal, max_hold_ns: int,
    decision_at_ns: int, deadline_offset_ns: int, evidence_refs: Sequence[str],
    take_profit_price: Decimal | None = None, take_profit_policy: str = "NOT_IN_REGISTERED_POLICY",
    funding_assumption: str = "NONE_OBSERVED_AT_DECISION_TIME",
    sealed_at_ns: int = 0,
) -> ResearchActionV1:
    """Seal one research action from qualified original-time evidence."""

    risk_per_unit = abs(entry_reference - stop_price)
    if risk_per_unit <= 0:
        raise ValueError("research action requires a positive stop distance")
    risk_budget = VIRTUAL_EQUITY_USDT * RISK_FRACTION_PER_TRADE
    raw_quantity = risk_budget / risk_per_unit
    quantity = _floor_step(raw_quantity, qty_step)
    notional = quantity * entry_reference
    refusal: str | None = None
    if quantity <= 0:
        refusal = "QUANTITY_BELOW_REGISTERED_STEP"
    elif min_notional is not None and notional < min_notional:
        refusal = "NOTIONAL_BELOW_REGISTERED_MINIMUM"
    action_id = hashlib.sha256(
        f"{cycle_id}|{symbol}|{strategy}|{side}|{entry_reference}|{stop_price}|{decision_at_ns}".encode()
    ).hexdigest()
    return ResearchActionV1(
        action_id=action_id,
        cycle_id=cycle_id,
        symbol=symbol,
        strategy=strategy,
        policy_id=policy_id,
        policy_hash=policy_hash,
        side=side,
        direction_origin="REGISTERED_STRATEGY_PREDICATE",
        entry_reference=entry_reference,
        entry_collar_bps=Decimal("5"),
        stop_price=stop_price,
        stop_basis=stop_basis,
        quantity=quantity,
        notional_usd=notional,
        risk_profile="RESEARCH_VIRTUAL_10000_USDT_0.5PCT_PER_TRADE",
        horizon_end_ns=decision_at_ns + max_hold_ns,
        deadline_ns=decision_at_ns + deadline_offset_ns,
        take_profit_price=take_profit_price,
        take_profit_policy=take_profit_policy,
        fee_bps=TAKER_FEE_RATE * Decimal(10_000),
        expected_spread_bps=spread_bps if spread_bps is not None else Decimal("0"),
        funding_assumption=funding_assumption,
        slippage_bps=RESEARCH_SLIPPAGE_BPS,
        hard_refusal_reason=refusal,
        evidence_refs=tuple(evidence_refs),
        sealed_at_ns=sealed_at_ns,
    )


@dataclass(frozen=True)
class LifecycleOutcomeV1:
    management_kind: str
    path_id: str
    entry_state: str
    exit_state: str
    exit_reason: str
    gross_cash: Decimal
    cost_cash: Decimal
    net_cash: Decimal
    horizon_end_ns: int
    uncertainty: str
    detail: Mapping[str, Any] = field(default_factory=dict)


def _entry_fill(action: ResearchActionV1, next_bar: CausalBarV2, spread_bps: Decimal) -> tuple[str, Decimal, str]:
    """Model the entry fill of a limit order placed at the decision close."""

    direction = Decimal("1") if action.side == "LONG" else Decimal("-1")
    slippage = action.entry_reference * RESEARCH_SLIPPAGE_BPS / Decimal(10_000)
    reference = action.entry_reference + direction * slippage
    open_price = next_bar.open
    collar = action.entry_reference * action.entry_collar_bps / Decimal(10_000)
    if direction > 0:
        if open_price > reference + collar:
            return "NO_FILL", reference, "OPEN_BEYOND_ADVERSE_COLLAR"
        fill = max(open_price, reference)
    else:
        if open_price < reference - collar:
            return "NO_FILL", reference, "OPEN_BEYOND_ADVERSE_COLLAR"
        fill = min(open_price, reference)
    return "FILLED", fill, "LIMIT_FILLED_WITHIN_COLLAR"


def _funding_between(action: ResearchActionV1, start_ns: int, end_ns: int,
                     funding_rate: Decimal | None, mark_price: Decimal | None) -> Decimal:
    if funding_rate is None or mark_price is None:
        return Decimal("0")
    # Binance USD-M settles funding every 8 hours at 00:00/08:00/16:00 UTC.
    settlements = []
    step = 8 * 3600 * 1_000_000_000
    cursor = (start_ns // step + 1) * step
    while cursor <= end_ns:
        settlements.append(cursor)
        cursor += step
    total = Decimal("0")
    for at_ns in settlements:
        settlement = _FundingStub(at_ns=at_ns, signed_rate=funding_rate, mark_price=mark_price)
        total += funding_cash(action.side, action.quantity, Decimal("1"), settlement)
    return total


class _FundingStub:
    """Minimal carrier for the production funding_cash contract."""

    def __init__(self, *, at_ns: int, signed_rate: Decimal, mark_price: Decimal) -> None:
        self.at_ns = at_ns
        self.signed_rate = signed_rate
        self.mark_price = mark_price
        self.available_at_ns = at_ns


def evaluate_lifecycle(
    action: ResearchActionV1, *, future_bars: Sequence[CausalBarV2], spread_bps: Decimal,
    funding_rate: Decimal | None = None, mark_price: Decimal | None = None,
    atr_value: Decimal | None = None, take_profit_price: Decimal | None = None,
) -> tuple[LifecycleOutcomeV1, ...]:
    """Evaluate the registered management alternatives over one observed path."""

    if action.hard_refusal_reason:
        return ()
    if not future_bars:
        return ()
    direction = Decimal("1") if action.side == "LONG" else Decimal("-1")
    entry_state, entry_price, entry_reason = _entry_fill(action, future_bars[0], spread_bps)
    if entry_state != "FILLED":
        return (LifecycleOutcomeV1(
            management_kind="HOLD", path_id=action.action_id, entry_state="NO_FILL", exit_state="NO_POSITION",
            exit_reason=entry_reason, gross_cash=Decimal("0"), cost_cash=Decimal("0"), net_cash=Decimal("0"),
            horizon_end_ns=action.horizon_end_ns, uncertainty="LIVE_QUEUE_UNOBSERVABLE",
            detail={"entry_reference": str(action.entry_reference)}),)
    fee_schedule = FeeScheduleV2(
        key=action_action_key(action), available_at_ns=action.sealed_at_ns,
        entry_taker_rate=TAKER_FEE_RATE, exit_taker_rate=TAKER_FEE_RATE,
        source_ref=hashlib.sha256(f"{ACTIONS_VERSION}|RESEARCH_TAKER_SCHEDULE".encode()).hexdigest())
    entry_fee = fee_cash(action.quantity, entry_price, Decimal("1"), fee_schedule.entry_taker_rate)
    results: list[LifecycleOutcomeV1] = []

    def _run(kind: str, *, tighten_after_bars: int | None = None,
             partial_fraction: Decimal | None = None, trail: bool = False,
             close_after_bars: int | None = None) -> LifecycleOutcomeV1:
        stop = action.stop_price
        remaining = action.quantity
        gross = Decimal("0")
        exit_price: Decimal | None = None
        exit_reason = "TIME_EXIT"
        exit_state = "CLOSED"
        partial_done = False
        bars_seen = 0
        for bar in future_bars:
            if bar.close_at_ns > action.horizon_end_ns:
                break
            bars_seen += 1
            open_price, high, low, close = bar.open, bar.high, bar.low, bar.close
            if direction > 0:
                hit_stop = low <= stop
                gap_through = open_price < stop
            else:
                hit_stop = high >= stop
                gap_through = open_price > stop
            if hit_stop:
                exit_price = open_price if gap_through else stop
                exit_reason = "STOP_GAP" if gap_through else "STOP"
                break
            if take_profit_price is not None and not partial_done:
                tp_hit = (high >= take_profit_price) if direction > 0 else (low <= take_profit_price)
                if tp_hit and partial_fraction is not None:
                    partial_qty = _floor_step(remaining * partial_fraction, action.qty_step)
                    if partial_qty > 0:
                        gross += direction * (take_profit_price - entry_price) * partial_qty
                        remaining -= partial_qty
                        partial_done = True
                        exit_reason = "PARTIAL_TP"
                elif tp_hit:
                    exit_price = take_profit_price
                    exit_reason = "TAKE_PROFIT"
                    break
            if close_after_bars is not None and bars_seen >= close_after_bars:
                exit_price = close
                exit_reason = "MANAGED_CLOSE"
                break
            if tighten_after_bars is not None and bars_seen >= tighten_after_bars:
                stop = max(stop, entry_price) if direction > 0 else min(stop, entry_price)
            if trail and atr_value:
                trail_distance = atr_value
                stop = max(stop, close - trail_distance) if direction > 0 else min(stop, close + trail_distance)
        if exit_price is None:
            last = future_bars[min(bars_seen, len(future_bars)) - 1] if future_bars else None
            exit_price = last.close if last is not None else entry_price
            exit_reason = "TIME_EXIT" if exit_reason == "TIME_EXIT" else exit_reason
        gross += direction * (exit_price - entry_price) * remaining
        exit_fee = fee_cash(action.quantity, exit_price, Decimal("1"), fee_schedule.exit_taker_rate)
        funding = _funding_between(action, action.deadline_ns, action.horizon_end_ns,
                                   funding_rate, mark_price)
        cost = entry_fee + exit_fee + funding
        return LifecycleOutcomeV1(
            management_kind=kind, path_id=action.action_id, entry_state="FILLED", exit_state=exit_state,
            exit_reason=exit_reason, gross_cash=gross, cost_cash=cost, net_cash=gross - cost,
            horizon_end_ns=action.horizon_end_ns, uncertainty="MODELLED_PATH_INTRABAR_SEQUENCE_UNOBSERVABLE",
            detail={"entry_price": str(entry_price), "exit_price": str(exit_price),
                    "entry_fee": str(entry_fee), "exit_fee": str(exit_fee), "funding_cash": str(funding),
                    "quantity": str(action.quantity), "remaining_quantity": str(remaining)})

    results.append(_run("HOLD"))
    results.append(_run("CLOSE", close_after_bars=1))
    results.append(_run("TIGHTEN", tighten_after_bars=1))
    if take_profit_price is not None:
        results.append(_run("PARTIAL_TP", partial_fraction=Decimal("0.5")))
    if atr_value is not None:
        results.append(_run("REGISTERED_TRAIL", trail=True))
    return tuple(results)


def action_action_key(action: ResearchActionV1) -> Any:
    """The fee schedule needs an instrument key; a research stand-in is used."""

    from ...instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

    return InstrumentKeyV2(
        VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        action.symbol, action.symbol.replace("USDT", ""), "USDT", "USDT",
        hashlib.sha256(f"{ACTIONS_VERSION}|{action.symbol}".encode()).hexdigest())


def maturity_status(*, action: ResearchActionV1, future_bars: Sequence[CausalBarV2],
                    evaluated_at_ns: int) -> tuple[str, str | None, int]:
    """Return (maturity_status, censoring_reason, horizon_ns)."""

    if not future_bars:
        return "PENDING", "NO_FUTURE_OBSERVATION_YET", action.horizon_end_ns
    last_close = future_bars[-1].close_at_ns
    if last_close < action.horizon_end_ns:
        return "PENDING", "HORIZON_NOT_YET_MATURED", action.horizon_end_ns
    return "MATURED", None, action.horizon_end_ns
