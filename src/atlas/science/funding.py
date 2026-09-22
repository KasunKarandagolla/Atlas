"""Signed settlement-time funding accounting and conservative planning reserve."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.enums import Side


@dataclass(frozen=True)
class FundingSettlement:
    at_ns: int
    rate: Decimal
    mark: Decimal


@dataclass(frozen=True)
class FundingForecast:
    next_settlement_at_ns: int
    rates: tuple[Decimal, ...]
    anchor_kind: str
    downgrade: str | None = None


def forecast_funding(
    *, next_settlement_at_ns: int, latest_predicted_rate: Decimal | None, latest_settled_rate: Decimal | None,
    historical_settlement_changes: tuple[Decimal, ...], horizon_settlements: int,
) -> FundingForecast:
    """Causal settlement-change forecast; never accepts realized future rates."""
    if horizon_settlements < 1 or next_settlement_at_ns < 0:
        raise ValueError("known next settlement and positive horizon required")
    if latest_predicted_rate is not None:
        anchor, kind, downgrade = latest_predicted_rate, "PREDICTED", None
    elif latest_settled_rate is not None:
        anchor, kind, downgrade = latest_settled_rate, "SETTLED", "PREDICTED_HISTORY_UNAVAILABLE"
    else:
        raise ValueError("NOT_ESTIMABLE: no observable funding anchor")
    mean_change = sum(historical_settlement_changes, Decimal("0")) / Decimal(len(historical_settlement_changes)) if historical_settlement_changes else Decimal("0")
    return FundingForecast(next_settlement_at_ns, tuple(anchor + Decimal(i) * mean_change for i in range(horizon_settlements)), kind, downgrade)


def funding_cashflow(side: Side, quantity: Decimal, settlement: FundingSettlement) -> Decimal:
    """Positive result is a cost; positive funding costs a long and credits a short."""
    sign = Decimal("1") if side is Side.LONG else Decimal("-1")
    return sign * quantity * settlement.mark * settlement.rate


def funding_costs(side: Side, quantity_at: Callable[[int], Decimal], settlements: tuple[FundingSettlement, ...], opened_at_ns: int, closed_at_ns: int) -> tuple[Decimal, ...]:
    return tuple(funding_cashflow(side, quantity_at(x.at_ns), x) for x in settlements if opened_at_ns <= x.at_ns < closed_at_ns)


def adverse_funding_reserve(price_distance_stop_budget: Decimal, projected_costs: tuple[Decimal, ...]) -> Decimal:
    reserve = sum((max(Decimal("0"), x) for x in projected_costs), Decimal("0"))
    if reserve > price_distance_stop_budget * Decimal("0.25"):
        raise ValueError("adverse funding exceeds 25% stop budget")
    return reserve
