"""Evidence-valued deterministic Phase-4 stresses, separate from statistical P&L.

Every frozen stress is valued *only* from explicit, versioned inputs supplied by
the caller.  No exchange mechanic (maintenance-margin tier, liquidation
threshold, liquidation cost, exit path, funding rate) is invented here: a stress
whose required evidence is absent returns ``NOT_ESTIMABLE`` instead of a
convenient number.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from atlas.domain.enums import Side


class StressName(StrEnum):
    JUMP_5_LIQUIDITY = "JUMP_5_LIQUIDITY"
    JUMP_10_LIQUIDITY = "JUMP_10_LIQUIDITY"
    MARK_LAST_DIVERGENCE_1 = "MARK_LAST_DIVERGENCE_1"
    SPREAD_10X = "SPREAD_10X"
    DEPTH_MINUS_90 = "DEPTH_MINUS_90"
    NO_EXIT_60_SECONDS = "NO_EXIT_60_SECONDS"
    NO_EXIT_15_MINUTES = "NO_EXIT_15_MINUTES"
    BTC_ETH_CORRELATED_SHOCK = "BTC_ETH_CORRELATED_SHOCK"
    FUNDING_DEBIT_5X = "FUNDING_DEBIT_5X"
    MAINTENANCE_MARGIN_TIER = "MAINTENANCE_MARGIN_TIER"
    USDT_HAIRCUT_10 = "USDT_HAIRCUT_10"
    VENUE_COLLATERAL_LOSS = "VENUE_COLLATERAL_LOSS"


class StressStatus(StrEnum):
    ESTIMATED = "ESTIMATED"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class StressCase:
    """Frozen stress parameterisation; numeric mechanics live in the inputs."""

    name: StressName
    adverse_price_jump: Decimal = Decimal("0")
    spread_multiplier: Decimal = Decimal("1")
    depth_multiplier: Decimal = Decimal("1")
    exit_delay_seconds: int = 0
    collateral_haircut: Decimal = Decimal("0")
    venue_collateral_loss: bool = False
    mark_last_divergence: Decimal = Decimal("0")
    correlation: Decimal | None = None

    @property
    def requires_depth(self) -> bool:
        return self.depth_multiplier != Decimal("1")

    @property
    def requires_spread(self) -> bool:
        return self.spread_multiplier != Decimal("1")

    @property
    def requires_exit_delay_path(self) -> bool:
        return self.exit_delay_seconds > 0

    @property
    def requires_divergence(self) -> bool:
        return self.mark_last_divergence != Decimal("0")

    @property
    def requires_correlation(self) -> bool:
        return self.correlation is not None


FROZEN_STRESSES = (
    StressCase(StressName.JUMP_5_LIQUIDITY, Decimal("0.05"), Decimal("10"), Decimal("0.10")),
    StressCase(StressName.JUMP_10_LIQUIDITY, Decimal("0.10"), Decimal("10"), Decimal("0.10")),
    StressCase(StressName.MARK_LAST_DIVERGENCE_1, mark_last_divergence=Decimal("0.01")),
    StressCase(StressName.SPREAD_10X, spread_multiplier=Decimal("10")),
    StressCase(StressName.DEPTH_MINUS_90, depth_multiplier=Decimal("0.10")),
    StressCase(StressName.NO_EXIT_60_SECONDS, exit_delay_seconds=60),
    StressCase(StressName.NO_EXIT_15_MINUTES, exit_delay_seconds=900),
    StressCase(StressName.BTC_ETH_CORRELATED_SHOCK, Decimal("0.10"), Decimal("10"), Decimal("0.10"), correlation=Decimal("1")),
    StressCase(StressName.FUNDING_DEBIT_5X),
    StressCase(StressName.MAINTENANCE_MARGIN_TIER),
    StressCase(StressName.USDT_HAIRCUT_10, collateral_haircut=Decimal("0.10")),
    StressCase(StressName.VENUE_COLLATERAL_LOSS, venue_collateral_loss=True),
)


@dataclass(frozen=True)
class StressExecutionAssumptions:
    """Stressed executable prices/spreads/depths supplied as versioned evidence."""

    stressed_executable_prices: dict[StressName, tuple[Decimal, ...]] = field(default_factory=dict)
    stressed_available_depth: dict[StressName, tuple[Decimal, ...]] = field(default_factory=dict)
    stressed_spreads: dict[StressName, Decimal] = field(default_factory=dict)
    exit_delay_path_prices: dict[StressName, tuple[Decimal, ...]] = field(default_factory=dict)
    mark_last_divergences: dict[StressName, Decimal] = field(default_factory=dict)
    correlation_values: dict[StressName, Decimal] = field(default_factory=dict)
    paired_instrument_paths: dict[StressName, dict[str, tuple[Decimal, ...]]] = field(default_factory=dict)


@dataclass(frozen=True)
class StressMarginAssumptions:
    """Supplied maintenance-margin tier schedule and liquidation mechanics."""

    maintenance_margin_tiers: dict[StressName, tuple[Decimal, ...]] = field(default_factory=dict)
    liquidation_thresholds: dict[StressName, Decimal] = field(default_factory=dict)
    liquidation_costs: dict[StressName, Decimal] = field(default_factory=dict)
    liquidation_mechanics: dict[StressName, str] = field(default_factory=dict)


@dataclass(frozen=True)
class StressFundingAssumptions:
    current_abs_rate: Decimal
    training_p99_abs_rate: Decimal
    settlement_notional: Decimal


@dataclass(frozen=True)
class StressCollateralAssumptions:
    venue_collateral: Decimal
    valuation_currency: str = "USDT"


def _zero_funding() -> StressFundingAssumptions:
    return StressFundingAssumptions(Decimal("0"), Decimal("0"), Decimal("0"))


def _zero_collateral() -> StressCollateralAssumptions:
    return StressCollateralAssumptions(Decimal("0"))


@dataclass(frozen=True)
class StressInput:
    side: Side
    quantity: Decimal
    current_mark: Decimal
    shock_at_ns: int
    execution: StressExecutionAssumptions = field(default_factory=StressExecutionAssumptions)
    margin: StressMarginAssumptions = field(default_factory=StressMarginAssumptions)
    funding: StressFundingAssumptions = field(default_factory=_zero_funding)
    collateral: StressCollateralAssumptions = field(default_factory=_zero_collateral)


@dataclass(frozen=True)
class StressResult:
    case: StressCase
    status: StressStatus
    trade_loss: Decimal | None
    collateral_loss: Decimal
    venue_collateral_loss: Decimal
    margin_path: tuple[Decimal, ...]
    liquidated: bool | None
    liquidation_cost: Decimal | None
    shock_at_ns: int
    shock_values: tuple[str, ...]
    valuation_currency: str
    mechanics: str
    reason: str = ""

    @property
    def estimable(self) -> bool:
        return self.status is StressStatus.ESTIMATED


def _not_estimable(case: StressCase, state: StressInput, reason: str) -> StressResult:
    return StressResult(case=case, status=StressStatus.NOT_ESTIMABLE, trade_loss=None,
                        collateral_loss=Decimal("0"), venue_collateral_loss=Decimal("0"), margin_path=(),
                        liquidated=None, liquidation_cost=None, shock_at_ns=state.shock_at_ns, shock_values=(),
                        valuation_currency=state.collateral.valuation_currency, mechanics="", reason=reason)


def _weighted_exit_price(path: tuple[Decimal, ...], depth: tuple[Decimal, ...] | None, quantity: Decimal) -> Decimal | None:
    """Quantity-weighted executable exit price; ``None`` when support is not bounded."""
    if not path or any(price <= 0 for price in path):
        return None
    if depth is None:
        return path[-1]
    if len(depth) != len(path) or any(step < 0 for step in depth):
        return None
    remaining = quantity
    notional = Decimal("0")
    for price, available in zip(path, depth, strict=True):
        filled = min(remaining, available)
        if filled <= 0:
            continue
        notional += filled * price
        remaining -= filled
        if remaining <= 0:
            break
    if remaining > 0:
        return None
    return notional / quantity


def _margin_liquidation(case: StressCase, state: StressInput) -> tuple[tuple[Decimal, ...], bool, Decimal] | None:
    tiers = state.margin.maintenance_margin_tiers.get(case.name)
    threshold = state.margin.liquidation_thresholds.get(case.name)
    if tiers is None or threshold is None or not state.margin.liquidation_mechanics.get(case.name, "").strip():
        return None
    liquidated = any(tier >= threshold for tier in tiers)
    cost = state.margin.liquidation_costs.get(case.name, Decimal("0")) if liquidated else Decimal("0")
    return tiers, liquidated, cost


def evaluate_stress(case: StressCase, state: StressInput) -> StressResult:
    """Value a frozen stress only from explicit versioned evidence/configuration."""
    if case.venue_collateral_loss:
        # Account/venue failure, not an ordinary trade stop-out: the trade loss is
        # zero and the whole venue collateral balance is at risk.
        return StressResult(case=case, status=StressStatus.ESTIMATED, trade_loss=Decimal("0"),
                            collateral_loss=Decimal("0"), venue_collateral_loss=state.collateral.venue_collateral,
                            margin_path=(), liquidated=True, liquidation_cost=Decimal("0"),
                            shock_at_ns=state.shock_at_ns, shock_values=("loss_of_all_venue_collateral",),
                            valuation_currency=state.collateral.valuation_currency, mechanics="venue collateral unavailable")
    if case.name is StressName.USDT_HAIRCUT_10:
        if state.collateral.venue_collateral <= 0:
            return _not_estimable(case, state, "missing venue collateral evidence")
        margin = _margin_liquidation(case, state)
        if margin is None:
            return _not_estimable(case, state, "missing haircut margin/liquidation mechanics")
        tiers, liquidated, cost = margin
        loss = state.collateral.venue_collateral * case.collateral_haircut
        return StressResult(case=case, status=StressStatus.ESTIMATED, trade_loss=Decimal("0"), collateral_loss=loss,
                            venue_collateral_loss=Decimal("0"), margin_path=tiers, liquidated=liquidated,
                            liquidation_cost=cost, shock_at_ns=state.shock_at_ns,
                            shock_values=(f"collateral_haircut={case.collateral_haircut}",),
                            valuation_currency=state.collateral.valuation_currency,
                            mechanics=state.margin.liquidation_mechanics[case.name])
    if case.name is StressName.FUNDING_DEBIT_5X:
        if state.funding.settlement_notional <= 0:
            return _not_estimable(case, state, "missing funding settlement notional")
        rate = Decimal("5") * max(state.funding.current_abs_rate, state.funding.training_p99_abs_rate)
        loss = state.funding.settlement_notional * rate
        return StressResult(case=case, status=StressStatus.ESTIMATED, trade_loss=loss, collateral_loss=Decimal("0"),
                            venue_collateral_loss=Decimal("0"), margin_path=(), liquidated=False,
                            liquidation_cost=Decimal("0"), shock_at_ns=state.shock_at_ns,
                            shock_values=(f"funding_rate={rate}",),
                            valuation_currency=state.collateral.valuation_currency,
                            mechanics="funding debit from supplied rates")
    if case.name is StressName.MAINTENANCE_MARGIN_TIER:
        margin = _margin_liquidation(case, state)
        if margin is None:
            return _not_estimable(case, state, "missing maintenance-margin tier schedule")
        tiers, liquidated, cost = margin
        return StressResult(case=case, status=StressStatus.ESTIMATED, trade_loss=Decimal("0"),
                            collateral_loss=Decimal("0"), venue_collateral_loss=Decimal("0"), margin_path=tiers,
                            liquidated=liquidated, liquidation_cost=cost, shock_at_ns=state.shock_at_ns,
                            shock_values=("maintenance_margin_tier_increase",),
                            valuation_currency=state.collateral.valuation_currency,
                            mechanics=state.margin.liquidation_mechanics[case.name])
    if case.requires_correlation:
        paired = state.execution.paired_instrument_paths.get(case.name)
        observed = state.execution.correlation_values.get(case.name)
        if not paired or len(paired) < 2 or any(not path for path in paired.values()):
            return _not_estimable(case, state, "missing paired stressed instrument paths")
        if observed is None or observed != case.correlation:
            return _not_estimable(case, state, "missing correlation evidence for paired shock")
    margin = _margin_liquidation(case, state)
    if margin is None:
        return _not_estimable(case, state, "missing margin/liquidation mechanics")
    tiers, liquidated, cost = margin
    if case.requires_exit_delay_path:
        path = state.execution.exit_delay_path_prices.get(case.name)
    else:
        path = state.execution.stressed_executable_prices.get(case.name)
    if not path:
        return _not_estimable(case, state, "missing stressed executable price/path evidence")
    depth: tuple[Decimal, ...] | None = None
    if case.requires_depth:
        depth = state.execution.stressed_available_depth.get(case.name)
        if depth is None:
            return _not_estimable(case, state, "missing stressed available depth evidence")
    if case.requires_spread and state.execution.stressed_spreads.get(case.name) is None:
        return _not_estimable(case, state, "missing stressed spread evidence")
    if case.requires_divergence and state.execution.mark_last_divergences.get(case.name) is None:
        return _not_estimable(case, state, "missing mark/last divergence evidence")
    exit_price = _weighted_exit_price(path, depth, state.quantity)
    if exit_price is None:
        return _not_estimable(case, state, "insufficient stressed path/depth to bound the exit")
    direction = Decimal("1") if state.side is Side.LONG else Decimal("-1")
    trade_loss = max(Decimal("0"), -direction * state.quantity * (exit_price - state.current_mark))
    shocks = (f"price_jump={case.adverse_price_jump}", f"spread_multiplier={case.spread_multiplier}",
              f"depth_multiplier={case.depth_multiplier}", f"exit_delay_seconds={case.exit_delay_seconds}",
              f"mark_last_divergence={case.mark_last_divergence}",
              f"correlation={case.correlation}", f"stressed_exit_price={exit_price}")
    return StressResult(case=case, status=StressStatus.ESTIMATED, trade_loss=trade_loss,
                        collateral_loss=Decimal("0"), venue_collateral_loss=Decimal("0"), margin_path=tiers,
                        liquidated=liquidated, liquidation_cost=cost, shock_at_ns=state.shock_at_ns,
                        shock_values=tuple(shocks), valuation_currency=state.collateral.valuation_currency,
                        mechanics=state.margin.liquidation_mechanics[case.name])


def evaluate_stress_suite(state: StressInput) -> tuple[StressResult, ...]:
    return tuple(evaluate_stress(case, state) for case in FROZEN_STRESSES)


def suite_is_estimable(results: tuple[StressResult, ...]) -> bool:
    """A required stress that cannot be valued is never silently treated as benign."""
    return all(result.estimable for result in results)


def stressed_venue_loss(results: tuple[StressResult, ...]) -> Decimal:
    """Venue collateral loss is account-level, never folded into trade stop loss."""
    return sum((result.venue_collateral_loss for result in results), Decimal("0"))


def total_collateral_stress_loss(results: tuple[StressResult, ...]) -> Decimal:
    """Collateral impairment (haircut) plus full venue collateral loss."""
    return sum((result.collateral_loss + result.venue_collateral_loss for result in results), Decimal("0"))


def max_trade_stress_loss(results: tuple[StressResult, ...]) -> Decimal:
    values = [result.trade_loss for result in results if result.trade_loss is not None]
    return max(values, default=Decimal("0"))


def max_liquidation_cost(results: tuple[StressResult, ...]) -> Decimal:
    values = [result.liquidation_cost for result in results if result.liquidation_cost is not None]
    return max(values, default=Decimal("0"))


def stress_path_is_coherent(path: tuple[Decimal, ...], reference: Decimal, *, jump: Decimal, side: Side) -> bool:
    """Evidence coherence: the supplied path must reach the declared adverse jump."""
    if not path or reference <= 0 or jump <= 0:
        return False
    worst = min(path) if side is Side.LONG else max(path)
    magnitude = (reference - worst) / reference if side is Side.LONG else (worst - reference) / reference
    return math.isfinite(float(magnitude)) and magnitude >= float(jump) - 1e-9
