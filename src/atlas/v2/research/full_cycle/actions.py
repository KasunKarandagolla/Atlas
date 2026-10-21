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
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from ...data.bars import CausalBarV2
from ...science.costs import fee_cash, funding_cash
from .causality import CausalFact, EvidenceGrade, prospective_refusal

ACTIONS_VERSION = "ATLAS_RED_FULL_CYCLE_RESEARCH_ACTION_V2"
ACTIONS_CODE_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

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

    qty_step: Decimal = Decimal("0.001")
    tick_size: Decimal = Decimal("0.01")
    min_notional: Decimal | None = None
    decision_at_ns: int = 0
    setup_at_ns: int = 0
    trigger_at_ns: int = 0
    opportunity_id: str = ""
    instrument_revision: str = ""
    evidence_grade: str = EvidenceGrade.RECONSTRUCTED
    provenance: Mapping[str, Any] = field(default_factory=dict)
    execution_plan: Mapping[str, Any] = field(default_factory=dict)
    code_hash: str = ""

    def as_row(self) -> dict[str, Any]:
        row = asdict(self)
        for name in ("qty_step", "tick_size", "min_notional", "take_profit_policy"):
            row.pop(name)
        for name, value in list(row.items()):
            if isinstance(value, Decimal):
                row[name] = str(value)
        row.update(
            take_profit_levels=[], evidence_refs=list(self.evidence_refs), code_hash=self.code_hash or ACTIONS_CODE_HASH
        )
        return row


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("quantity step must be positive")
    return (value / step).to_integral_value(rounding="ROUND_FLOOR") * step


def seal_research_action(
    *,
    cycle_id: str,
    symbol: str,
    strategy: str,
    policy_id: str,
    policy_hash: str,
    side: str,
    entry_reference: Decimal,
    stop_price: Decimal,
    stop_basis: str,
    qty_step: Decimal,
    tick_size: Decimal,
    min_notional: Decimal | None,
    spread_bps: Decimal | None,
    atr_value: Decimal,
    max_hold_ns: int,
    decision_at_ns: int,
    deadline_offset_ns: int,
    evidence_refs: Sequence[str],
    take_profit_price: Decimal | None = None,
    take_profit_policy: str = "NOT_IN_REGISTERED_POLICY",
    funding_assumption: str = "NONE_OBSERVED_AT_DECISION_TIME",
    sealed_at_ns: int = 0,
    setup_at_ns: int | None = None,
    trigger_at_ns: int | None = None,
    instrument_revision: str = "",
    evidence_grade: str = EvidenceGrade.RECONSTRUCTED,
    facts: Sequence[CausalFact] = (),
    opportunity_id: str = "",
    min_qty: Decimal = Decimal("0"),
    max_qty: Decimal | None = None,
    execution_plan: Mapping[str, Any] | None = None,
) -> ResearchActionV1:
    """Seal one research action from qualified original-time evidence."""

    if not policy_id or not policy_hash:
        raise ValueError("POLICY_IDENTITY_REQUIRED")
    if side not in ("LONG", "SHORT"):
        raise ValueError("SIDE_INVALID")
    if not all(v.is_finite() and v > 0 for v in (entry_reference, stop_price, qty_step, tick_size, atr_value)):
        raise ValueError("NONFINITE_OR_NONPOSITIVE_ACTION")
    if (side == "LONG" and stop_price >= entry_reference) or (side == "SHORT" and stop_price <= entry_reference):
        raise ValueError("STOP_WRONG_SIDE")
    setup_at_ns = decision_at_ns if setup_at_ns is None else setup_at_ns
    trigger_at_ns = decision_at_ns if trigger_at_ns is None else trigger_at_ns
    if not (setup_at_ns <= trigger_at_ns <= decision_at_ns) or max_hold_ns <= 0 or deadline_offset_ns <= 0:
        raise ValueError("TRIGGER_DECISION_CHRONOLOGY_INVALID")
    stop_price = (stop_price / tick_size).to_integral_value(
        rounding="ROUND_CEILING" if side == "LONG" else "ROUND_FLOOR"
    ) * tick_size
    if (side == "LONG" and stop_price >= entry_reference) or (side == "SHORT" and stop_price <= entry_reference):
        raise ValueError("ROUNDED_STOP_WRONG_SIDE")
    risk_per_unit = abs(entry_reference - stop_price)
    if risk_per_unit <= 0:
        raise ValueError("research action requires a positive stop distance")
    risk_budget = VIRTUAL_EQUITY_USDT * RISK_FRACTION_PER_TRADE
    raw_quantity = risk_budget / risk_per_unit
    if max_qty is not None:
        raw_quantity = min(raw_quantity, max_qty)
    quantity = _floor_step(raw_quantity, qty_step)
    notional = quantity * entry_reference
    refusal: str | None = None
    if quantity <= 0 or quantity < min_qty:
        refusal = "QUANTITY_BELOW_REGISTERED_STEP"
    elif min_notional is not None and notional < min_notional:
        refusal = "NOTIONAL_BELOW_REGISTERED_MINIMUM"
    if not sealed_at_ns:
        refusal = "SEAL_TIME_MISSING"
    elif sealed_at_ns < decision_at_ns:
        refusal = "SEAL_PRECEDES_DECISION"
    elif sealed_at_ns > trigger_at_ns + deadline_offset_ns:
        refusal = "MISSED_ORIGINAL_DEADLINE"
    if evidence_grade == EvidenceGrade.PROSPECTIVE:
        refusal = refusal or prospective_refusal(
            facts,
            cutoff_ns=decision_at_ns, derived_cutoff_ns=sealed_at_ns,
            instrument_revision=instrument_revision,
            required_kinds=("M15", "H1", "H4", "PRODUCT", "UNIVERSE", "FEATURE", "BBO", "EVENT_GATE", "MARK_INDEX"),
        )
        if spread_bps is None:
            refusal = refusal or "ORIGINAL_SPREAD_MISSING"
    import json

    plan = dict(execution_plan or {})
    plan.update(
        order_type="LIMIT",
        time_in_force="IOC",
        qty_step=str(qty_step),
        tick_size=str(tick_size),
        min_qty=str(min_qty),
        min_notional=None if min_notional is None else str(min_notional),
        take_profit_policy=take_profit_policy,
    )
    opportunity_id = (
        opportunity_id
        or hashlib.sha256(f"{symbol}|{strategy}|{policy_hash}|{setup_at_ns}|{trigger_at_ns}".encode()).hexdigest()
    )
    code_hash = ACTIONS_CODE_HASH
    identity = {k: v for k, v in locals().items() if k not in {"json", "identity", "facts"}}
    identity["facts"] = [f.as_dict() for f in facts]
    action_id = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
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
        deadline_ns=trigger_at_ns + deadline_offset_ns,
        take_profit_price=take_profit_price,
        take_profit_policy=take_profit_policy,
        fee_bps=TAKER_FEE_RATE * Decimal(10_000),
        expected_spread_bps=spread_bps if spread_bps is not None else Decimal("0"),
        funding_assumption=funding_assumption,
        slippage_bps=RESEARCH_SLIPPAGE_BPS,
        hard_refusal_reason=refusal,
        evidence_refs=tuple(evidence_refs),
        sealed_at_ns=sealed_at_ns,
        qty_step=qty_step,
        tick_size=tick_size,
        min_notional=min_notional,
        decision_at_ns=decision_at_ns,
        setup_at_ns=setup_at_ns,
        trigger_at_ns=trigger_at_ns,
        opportunity_id=opportunity_id,
        instrument_revision=instrument_revision,
        evidence_grade=evidence_grade,
        provenance={"facts": [f.as_dict() for f in facts]},
        execution_plan=plan,
        code_hash=code_hash,
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


@dataclass(frozen=True)
class FundingSettlement:
    at_ns: int
    signed_rate: Decimal
    mark_price: Decimal
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        if (
            self.available_at_ns < self.at_ns
            or not self.evidence_ref
            or not self.signed_rate.is_finite()
            or not self.mark_price.is_finite()
            or self.mark_price <= 0
        ):
            raise ValueError("FUNDING_EVIDENCE_INVALID")


def _entry_fill(action: ResearchActionV1, next_bar: CausalBarV2, spread_bps: Decimal) -> tuple[str, Decimal, str]:
    direction = Decimal(1) if action.side == "LONG" else Decimal(-1)
    # OHLC-only executable-price assumption is explicit, bounded and never venue truth.
    adjustment = (spread_bps / 2 + action.slippage_bps) / Decimal(10000)
    price = next_bar.open * (1 + direction * adjustment)
    price = (price / action.tick_size).to_integral_value(
        rounding="ROUND_CEILING" if direction > 0 else "ROUND_FLOOR"
    ) * action.tick_size
    collar = action.entry_reference * (1 + direction * action.entry_collar_bps / Decimal(10000))
    collar = (collar / action.tick_size).to_integral_value(
        rounding="ROUND_FLOOR" if direction > 0 else "ROUND_CEILING"
    ) * action.tick_size
    if (direction > 0 and price > collar) or (direction < 0 and price < collar):
        return "NO_FILL", price, "OPEN_BEYOND_ADVERSE_COLLAR"
    return "FILLED", price, "MODELED_IOC_PRICE_WITHIN_COLLAR"


def evaluate_lifecycle(
    action: ResearchActionV1,
    *,
    future_bars: Sequence[CausalBarV2],
    spread_bps: Decimal,
    funding_rate: Decimal | None = None,
    mark_price: Decimal | None = None,
    atr_value: Decimal | None = None,
    take_profit_price: Decimal | None = None,
    funding_settlements: Sequence[FundingSettlement] = (),
    funding_coverage: tuple[int, int] | None = None,
    filled_quantity: Decimal | None = None,
    evaluated_at_ns: int | None = None,
) -> tuple[LifecycleOutcomeV1, ...]:
    """OHLC bounds on a sealed plan; unobservable fill/ordering/funding stays censored.

    Deprecated scalar funding inputs are intentionally insufficient: they contain
    neither the venue settlement schedule nor an exposure-specific rate history.
    """
    if action.hard_refusal_reason or not future_bars:
        return ()
    if take_profit_price is not None and take_profit_price != action.take_profit_price:
        raise ValueError("TAKE_PROFIT_NOT_BOUND_TO_ACTION")
    if spread_bps < 0 or not spread_bps.is_finite():
        raise ValueError("SPREAD_INVALID")
    bars = sorted(future_bars, key=lambda b: b.open_at_ns)
    # A complete bar beginning before seal cannot supply a fill after the seal.
    bars = [
        b
        for b in bars
        if b.open_at_ns >= action.sealed_at_ns
        and b.open_at_ns < action.horizon_end_ns
        and (evaluated_at_ns is None or b.raw.available_at_ns <= evaluated_at_ns)
    ]
    if not bars:
        return ()
    if bars[0].open_at_ns > action.deadline_ns:
        return ()
    direction = Decimal(1) if action.side == "LONG" else Decimal(-1)
    state, entry_price, reason = _entry_fill(action, bars[0], spread_bps)
    kinds = ["HOLD", "CLOSE", "TIGHTEN"]
    # Only explicitly registered management comparisons may claim these names.
    if take_profit_price is not None and action.take_profit_policy == "HALF_TP_AT_2R_COMPARATOR_V1":
        kinds.append("PARTIAL_TP")
    if atr_value is not None and action.execution_plan.get("trail_policy") == "ATR_TRAIL_COMPARATOR_V1":
        kinds.append("REGISTERED_TRAIL")
    qty = action.quantity if filled_quantity is None else filled_quantity
    if not qty.is_finite() or qty < 0 or qty > action.quantity or qty % action.qty_step:
        raise ValueError("FILLED_QUANTITY_INVALID")
    if qty == 0:
        state, reason = "NO_FILL", "MODELED_ZERO_QUANTITY"
    if state == "NO_FILL":
        return tuple(
            LifecycleOutcomeV1(
                k,
                action.action_id,
                "NO_FILL",
                "NO_POSITION",
                reason,
                Decimal(0),
                Decimal(0),
                Decimal(0),
                action.horizon_end_ns,
                "MODELED_NO_FILL_QUEUE_UNOBSERVABLE",
                {
                    "entry_at_ns": bars[0].open_at_ns,
                    "exit_at_ns": bars[0].open_at_ns,
                    "label_available_at_ns": bars[0].raw.available_at_ns,
                    "remaining_quantity": "0",
                    "evidence_grade": str(EvidenceGrade.MODELED),
                },
            )
            for k in kinds
        )
    entry_at = bars[0].open_at_ns
    rate = action.fee_bps / Decimal(10000)
    results = []
    for kind in kinds:
        remaining, stop = qty, action.stop_price
        entry_fee = fee_cash(qty, entry_price, Decimal(1), rate)
        exits: list[tuple[int, Decimal, Decimal]] = []
        exit_reason, ambiguity, complete = "FUTURE_PATH_INCOMPLETE", [], False

        def append_exit(
            at_ns: int, closing_qty: Decimal, closing_price: Decimal, recorded_exits=exits, recorded_ambiguity=ambiguity
        ) -> bool:
            price = (closing_price / action.tick_size).to_integral_value(
                rounding="ROUND_FLOOR" if direction > 0 else "ROUND_CEILING"
            ) * action.tick_size
            minimum_qty = Decimal(str(action.execution_plan.get("min_qty", "0")))
            if closing_qty < minimum_qty or (
                action.min_notional is not None
                and closing_qty * price < action.min_notional
                and not action.execution_plan.get("closing_min_notional_exempt", False)
            ):
                recorded_ambiguity.append("EXIT_FILTERS_OR_DUST_UNOBSERVABLE")
                return False
            recorded_exits.append((at_ns, closing_qty, price))
            return True

        expected_open = entry_at
        label_available = 0
        for i, bar in enumerate(bars):
            if bar.open_at_ns != expected_open:
                ambiguity.append("FUTURE_PATH_GAP")
                break
            expected_open = bar.close_at_ns
            label_available = max(label_available, bar.raw.available_at_ns)
            hit_stop = bar.low <= stop if direction > 0 else bar.high >= stop
            tp_hit = (
                (bar.high >= take_profit_price if direction > 0 else bar.low <= take_profit_price)
                if take_profit_price is not None
                else False
            )
            if hit_stop and tp_hit and kind == "PARTIAL_TP":
                ambiguity.append("STOP_TP_ORDER_UNOBSERVABLE_ADVERSE_BOUND")
            if hit_stop:
                # Adverse executable bound includes gaps and the trigger bar excursion.
                price = min(stop, bar.open, bar.low) if direction > 0 else max(stop, bar.open, bar.high)
                price *= 1 - direction * (spread_bps / 2 + action.slippage_bps) / Decimal(10000)
                if not append_exit(bar.close_at_ns, remaining, price):
                    exit_reason = "EXIT_FILTER_REFUSAL"
                    break
                exit_reason = "STOP_GAP" if (bar.open < stop if direction > 0 else bar.open > stop) else "STOP"
                ambiguity.append("STOP_TIME_WITHIN_BAR_UNOBSERVABLE")
                remaining, complete = Decimal(0), True
                break
            if kind == "PARTIAL_TP" and tp_hit and not exits:
                partial = _floor_step(remaining / 2, action.qty_step)
                residual = remaining - partial
                if partial and (
                    action.min_notional is None or min(partial, residual) * take_profit_price >= action.min_notional
                ):
                    if append_exit(bar.close_at_ns, partial, take_profit_price):
                        remaining = residual
            range_low, range_high = action.execution_plan.get("range_low"), action.execution_plan.get("range_high")
            failed_break = (
                action.strategy == "S2"
                and i < 2
                and range_low is not None
                and range_high is not None
                and Decimal(range_low) <= bar.close <= Decimal(range_high)
            )
            if kind == "CLOSE" or failed_break or bar.close_at_ns == action.horizon_end_ns:
                price = bar.close * (1 - direction * (spread_bps / 2 + action.slippage_bps) / Decimal(10000))
                if not append_exit(bar.close_at_ns, remaining, price):
                    exit_reason = "EXIT_FILTER_REFUSAL"
                    break
                exit_reason = "FAILED_BREAK" if failed_break else "MANAGED_CLOSE" if kind == "CLOSE" else "TIME_EXIT"
                remaining, complete = Decimal(0), True
                break
            if kind == "TIGHTEN":
                stop = max(stop, entry_price) if direction > 0 else min(stop, entry_price)
            if kind == "REGISTERED_TRAIL" and atr_value:
                proposed = bar.close - direction * atr_value
                stop = max(stop, proposed) if direction > 0 else min(stop, proposed)
        end = exits[-1][0] if complete else expected_open
        gross = sum((direction * q * (p - entry_price) for _, q, p in exits), Decimal(0))
        exit_fee = sum((fee_cash(q, p, Decimal(1), rate) for _, q, p in exits), Decimal(0))
        funding = Decimal(0)
        funding_known = funding_coverage is not None and funding_coverage[0] <= entry_at and funding_coverage[1] >= end
        for settlement in sorted(funding_settlements, key=lambda f: f.at_ns):
            if entry_at < settlement.at_ns <= end:
                if evaluated_at_ns is not None and settlement.available_at_ns > evaluated_at_ns:
                    funding_known = False
                    continue
                # At an OHLC-ambiguous same-bar exit, conservatively retain exposure at settlement.
                held = qty - sum((q for t, q, _ in exits if t < settlement.at_ns), Decimal(0))
                funding += funding_cash(action.side, held, Decimal(1), settlement)
        uncertainty = ["MODELED_SHADOW_EXECUTION", "OHLC_EXECUTION_BOUND_QUEUE_UNOBSERVABLE", *ambiguity]
        if not funding_known:
            uncertainty.append("FUNDING_UNOBSERVABLE")
        if qty < action.quantity:
            uncertainty.append("PARTIAL_FILL_MODELED_QUANTITY")
        if not complete:
            uncertainty.append("RESIDUAL_EXPOSURE_UNVALUED")
        cost = entry_fee + exit_fee - funding
        results.append(
            LifecycleOutcomeV1(
                kind,
                action.action_id,
                "PARTIAL_FILL" if qty < action.quantity else state,
                "CLOSED" if complete else "CENSORED",
                exit_reason,
                gross,
                cost,
                gross - cost,
                action.horizon_end_ns,
                "|".join(uncertainty),
                {
                    "entry_price": str(entry_price),
                    "entry_at_ns": entry_at,
                    "exit_at_ns": end,
                    "label_available_at_ns": label_available,
                    "entry_fee": str(entry_fee),
                    "exit_fee": str(exit_fee),
                    "funding_cash": str(funding),
                    "quantity": str(qty),
                    "remaining_quantity": str(remaining),
                    "funding_qualified": funding_known,
                    "exits": [[t, str(q), str(p)] for t, q, p in exits],
                    "model_version": ACTIONS_VERSION,
                    "evidence_grade": str(EvidenceGrade.MODELED),
                },
            )
        )
    return tuple(results)


def action_action_key(action: ResearchActionV1) -> Any:
    """The fee schedule needs an instrument key; a research stand-in is used."""

    from ...instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

    return InstrumentKeyV2(
        VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        action.symbol, action.symbol.replace("USDT", ""), "USDT", "USDT",
        hashlib.sha256(f"{ACTIONS_VERSION}|{action.symbol}".encode()).hexdigest())


def maturity_status(
    *, action: ResearchActionV1, future_bars: Sequence[CausalBarV2], evaluated_at_ns: int
) -> tuple[str, str | None, int]:
    """Return (maturity_status, censoring_reason, horizon_ns)."""

    if not future_bars:
        return "PENDING", "NO_FUTURE_OBSERVATION_YET", action.horizon_end_ns
    bars = sorted(future_bars, key=lambda b: b.open_at_ns)
    if evaluated_at_ns < action.horizon_end_ns or any(
        b.raw.available_at_ns > evaluated_at_ns for b in bars if b.close_at_ns <= action.horizon_end_ns
    ):
        return "PENDING", "LABEL_NOT_AVAILABLE", action.horizon_end_ns
    expected = bars[0].open_at_ns
    if expected < action.sealed_at_ns:
        return "CENSORED", "PATH_PRECEDES_ACTION_SEAL", action.horizon_end_ns
    for bar in bars:
        if bar.open_at_ns > action.horizon_end_ns:
            break
        if bar.open_at_ns != expected:
            return "CENSORED", "FUTURE_PATH_GAP", action.horizon_end_ns
        expected = bar.close_at_ns
        if expected >= action.horizon_end_ns:
            return "MATURED", None, action.horizon_end_ns
    return "PENDING", "HORIZON_NOT_YET_MATURED", action.horizon_end_ns
