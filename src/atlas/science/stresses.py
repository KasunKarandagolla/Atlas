"""Typed deterministic, non-probabilistic Phase-4 stress suite."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


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


@dataclass(frozen=True)
class StressCase:
    name: StressName
    adverse_price_jump: Decimal = Decimal("0")
    spread_multiplier: Decimal = Decimal("1")
    depth_multiplier: Decimal = Decimal("1")
    exit_delay_seconds: int = 0
    collateral_haircut: Decimal = Decimal("0")
    venue_collateral_loss: bool = False
    mark_last_divergence: Decimal = Decimal("0")
    correlation: Decimal | None = None


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
class StressResult:
    case: StressCase
    loss: Decimal
    margin_path: tuple[Decimal, ...]
    liquidated: bool
    liquidation_cost: Decimal = Decimal("0")
    shock_at_ns: int = 0
    valuation_currency: str = "USDT"
    venue_collateral_loss: Decimal = Decimal("0")
    mechanics: str = ""


@dataclass(frozen=True)
class StressInput:
    notional: Decimal
    current_mark: Decimal
    initial_margin: Decimal
    maintenance_margin: Decimal
    available_collateral: Decimal
    current_funding_abs_rate: Decimal = Decimal("0")
    training_funding_p99_abs_rate: Decimal = Decimal("0")
    shock_at_ns: int = 0
    valuation_currency: str = "USDT"


def evaluate_stress(case: StressCase, state: StressInput) -> StressResult:
    """Apply each deterministic stress and inspect every margin-path waypoint.

    Venue collateral loss intentionally has no trade stop-loss value: it is an
    account/venue-capital failure reported separately from `loss`.
    """
    collateral = state.available_collateral * (Decimal("1") - case.collateral_haircut)
    loss = state.notional * case.adverse_price_jump
    if case.mark_last_divergence:
        loss += state.notional * case.mark_last_divergence
    if case.name is StressName.FUNDING_DEBIT_5X:
        loss += state.notional * Decimal("5") * max(state.current_funding_abs_rate, state.training_funding_p99_abs_rate)
    if case.name is StressName.MAINTENANCE_MARGIN_TIER:
        maintenance = state.maintenance_margin * Decimal("1.5")
    else:
        maintenance = state.maintenance_margin
    if case.exit_delay_seconds:
        # Deterministic impairment is represented by retaining an adverse price
        # move for the delayed period; it is deliberately separate from mean P&L.
        loss += state.notional * Decimal(case.exit_delay_seconds) / Decimal("100000")
    margin_path = (state.initial_margin, maintenance, maintenance + loss)
    venue_loss = state.available_collateral if case.venue_collateral_loss else Decimal("0")
    liquidated = any(x > collateral for x in margin_path) or case.venue_collateral_loss
    mechanics = "venue collateral unavailable" if case.venue_collateral_loss else ("margin liquidation before intended stop" if liquidated else "survives")
    return StressResult(case, loss, margin_path, liquidated, loss if liquidated and not case.venue_collateral_loss else Decimal("0"),
                        state.shock_at_ns, state.valuation_currency, venue_loss, mechanics)


def stress_price_loss(notional: Decimal, case: StressCase, *, maintenance_margin: Decimal = Decimal("0"), collateral: Decimal | None = None) -> StressResult:
    """Backward-compatible convenience wrapper for tests/diagnostics."""
    return evaluate_stress(case, StressInput(notional, Decimal("1"), Decimal("0"), maintenance_margin, collateral or Decimal("0")))
