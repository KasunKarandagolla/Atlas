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


def stress_price_loss(notional: Decimal, case: StressCase, *, maintenance_margin: Decimal = Decimal("0"), collateral: Decimal | None = None) -> StressResult:
    loss = notional * case.adverse_price_jump
    if case.venue_collateral_loss:
        loss = notional if collateral is None else max(notional, collateral)
    if case.collateral_haircut and collateral is not None:
        loss += collateral * case.collateral_haircut
    path = (maintenance_margin, maintenance_margin + loss)
    liquidated = collateral is not None and path[-1] > collateral
    return StressResult(case, loss, path, liquidated, loss if liquidated else Decimal("0"))
