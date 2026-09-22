"""§9: causal funding forecasting, settlement-change model, and reserve limits."""

from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.enums import Side
from atlas.science.funding import (
    FundingSettlement,
    adverse_funding_reserve,
    forecast_funding,
    funding_cashflow,
    funding_costs,
)


def test_forecast_uses_the_latest_observable_predicted_rate_and_known_settlement():
    forecast = forecast_funding(next_settlement_at_ns=1_000, latest_predicted_rate=Decimal("0.01"),
                                latest_settled_rate=Decimal("0.5"),
                                historical_settlement_changes=(Decimal("0.001"), Decimal("0.002")),
                                horizon_settlements=3)
    assert forecast.anchor_kind == "PREDICTED" and forecast.downgrade is None
    assert forecast.rates == (Decimal("0.01"), Decimal("0.0115"), Decimal("0.013"))
    assert forecast.next_settlement_at_ns == 1_000


def test_missing_predicted_history_downgrades_to_settled_anchor_explicitly():
    forecast = forecast_funding(next_settlement_at_ns=1_000, latest_predicted_rate=None,
                                latest_settled_rate=Decimal("0.02"), historical_settlement_changes=(),
                                horizon_settlements=2)
    assert forecast.anchor_kind == "SETTLED"
    assert forecast.downgrade == "PREDICTED_HISTORY_UNAVAILABLE"
    assert forecast.rates == (Decimal("0.02"), Decimal("0.02"))


def test_no_observable_anchor_is_not_estimable():
    with pytest.raises(ValueError, match="NOT_ESTIMABLE"):
        forecast_funding(next_settlement_at_ns=1, latest_predicted_rate=None, latest_settled_rate=None,
                         historical_settlement_changes=(), horizon_settlements=1)
    with pytest.raises(ValueError, match="known next settlement"):
        forecast_funding(next_settlement_at_ns=-1, latest_predicted_rate=Decimal("0.01"), latest_settled_rate=None,
                         historical_settlement_changes=(), horizon_settlements=1)


def test_signed_funding_accounting_and_surviving_quantity():
    long_cost = funding_cashflow(Side.LONG, Decimal("2"), FundingSettlement(0, Decimal("0.01"), Decimal("100")))
    short_credit = funding_cashflow(Side.SHORT, Decimal("2"), FundingSettlement(0, Decimal("0.01"), Decimal("100")))
    assert long_cost == Decimal("2") and short_credit == Decimal("-2")
    settlements = (FundingSettlement(10, Decimal("0.01"), Decimal("100")),
                   FundingSettlement(20, Decimal("0.01"), Decimal("100")),
                   FundingSettlement(30, Decimal("0.01"), Decimal("100")))
    charged = funding_costs(Side.LONG, lambda at_ns: Decimal("1") if at_ns < 25 else Decimal("0"), settlements, 0, 30)
    assert charged == (Decimal("1"), Decimal("1"))


def test_adverse_funding_reserve_is_bounded_by_25_percent_of_stop_budget():
    reserve = adverse_funding_reserve(Decimal("100"), (Decimal("10"), Decimal("-50"), Decimal("5")))
    assert reserve == Decimal("15")
    with pytest.raises(ValueError, match="25% stop budget"):
        adverse_funding_reserve(Decimal("100"), (Decimal("26"),))
    with pytest.raises(ValueError, match="25% stop budget"):
        adverse_funding_reserve(Decimal("10"), (Decimal("2.6"),))
