"""§11/§12: common-path portfolio ES/J and deterministic hard-constraint sizing."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from support.phase4_factory import account

from atlas.domain.risk import engineering_default_policy
from atlas.risk.engine import AccountState, RiskVector
from atlas.risk.portfolio import MATERIALITY_FLOOR, common_path_portfolio_decision, decision_value, empirical_es
from atlas.risk.sizing import (
    PerUnitRisk,
    allocate_serial_btc_then_eth,
    deterministic_risk_quantity,
    largest_feasible_quantity,
)


def per_unit(*, normal: Decimal = Decimal("2"), stress: Decimal = Decimal("4"),
             mark: Decimal = Decimal("100"), margin: Decimal = Decimal("20")) -> PerUnitRisk:
    return PerUnitRisk(normal, stress, mark, mark, margin, Decimal("0"), Decimal("10"))


def test_empirical_es_is_the_frozen_rockafellar_uryasev_minimisation():
    assert empirical_es([0.0, 1.0, 2.0, 3.0], 0.75) == pytest.approx(3.0)
    assert empirical_es([1.0], 0.975) == pytest.approx(1.0)
    with pytest.raises(ValueError):
        empirical_es([], 0.975)
    with pytest.raises(ValueError):
        empirical_es([1.0], 1.0)


def test_common_path_portfolio_decision_uses_pi0_and_incremental_es():
    pi0 = (0.0, 0.0, 0.0, 0.0, 0.0, -500.0)
    candidate = (100.0, 100.0, 100.0, 100.0, 100.0, 600.0)
    decision = common_path_portfolio_decision(50.0, 10_000.0, pi0, candidate, 0.975)
    assert decision.es_before > decision.es_after
    assert decision.incremental_es < 0
    assert decision.j == pytest.approx(50.0 / 10_000.0 - decision.incremental_es)
    assert decision.qualified is True
    with pytest.raises(ValueError, match="paired common paths"):
        common_path_portfolio_decision(1.0, 100.0, (0.0,), (0.0, 1.0), 0.975)
    with pytest.raises(ValueError, match="positive equity"):
        common_path_portfolio_decision(1.0, 0.0, (0.0,), (0.0,), 0.975)


def test_negative_alpha_action_cannot_qualify_only_by_reducing_es():
    pi0 = (0.0, -1000.0)
    candidate = (0.0, 900.0)  # strongly lowers tail loss but has non-positive expected edge
    decision = common_path_portfolio_decision(-1.0, 10_000.0, pi0, candidate, 0.975)
    assert decision.incremental_es < 0
    assert decision.qualified is False and decision.reason == "LCB_NONPOSITIVE"
    marginal = common_path_portfolio_decision(1e-9, 10_000.0, pi0, candidate, 0.975)
    assert marginal.qualified is (marginal.j > MATERIALITY_FLOOR)


def test_decision_value_matches_the_frozen_lambda_one_formula():
    value = decision_value(100.0, 10_000.0, (0.0, 0.0), (10.0, 20.0))
    assert value == pytest.approx(100.0 / 10_000.0 - (empirical_es([-10 / 10_000, -20 / 10_000]) - empirical_es([0.0, 0.0])))
    with pytest.raises(ValueError):
        decision_value(1.0, 100.0, (0.0,), (0.0, 1.0))


def test_largest_feasible_quantity_is_lot_rounded_and_stops_at_minimum():
    assert largest_feasible_quantity(Decimal("5.37"), Decimal("0.1"), Decimal("0.2"), lambda q: True) == Decimal("5.3")
    assert largest_feasible_quantity(Decimal("0.15"), Decimal("0.1"), Decimal("0.2"), lambda q: True) is None
    assert largest_feasible_quantity(Decimal("1"), Decimal("0.25"), Decimal("0.25"),
                                     lambda q: q <= Decimal("0.5")) == Decimal("0.5")


def test_deterministic_risk_quantity_solves_hard_constraints_before_any_lcb():
    selection = deterministic_risk_quantity(policy=engineering_default_policy(), account=account(Decimal("10000")),
                                            pending=(), per_unit=per_unit(normal=Decimal("2")),
                                            venue_maximum=Decimal("10"), lot=Decimal("0.1"),
                                            minimum=Decimal("0.1"), leverage=Decimal("1"))
    assert selection.quantity is not None and selection.reason == ""
    assert selection.quantity % Decimal("0.1") == 0
    # The per-trade normal-loss cap is 0.1% of 10,000 = 10, so at 2 per unit the cap binds at 5.
    assert selection.quantity == Decimal("5.0")
    assert selection.risk is not None and selection.risk.normal_loss <= Decimal("10")


def test_no_feasible_quantity_yields_a_typed_rejection():
    selection = deterministic_risk_quantity(policy=engineering_default_policy(), account=account(Decimal("10")),
                                            pending=(), per_unit=per_unit(), venue_maximum=Decimal("1"),
                                            lot=Decimal("1"), minimum=Decimal("1"), leverage=Decimal("1"))
    assert selection.quantity is None and selection.risk is None and selection.reason


def test_serial_btc_then_eth_allocation_updates_reservations_in_order():
    requests = {
        "BTCUSDT": (per_unit(normal=Decimal("2")), Decimal("5"), Decimal("1"), Decimal("1"), Decimal("1")),
        "ETHUSDT": (per_unit(normal=Decimal("2")), Decimal("5"), Decimal("1"), Decimal("1"), Decimal("1")),
    }
    state = AccountState(Decimal("2000"), Decimal("2000"), Decimal("0"), Decimal("0"), Decimal("0"),
                         new_risk_intents=0)
    results = allocate_serial_btc_then_eth(policy=engineering_default_policy(), account=state, requests=requests)
    assert set(results) == {"BTCUSDT", "ETHUSDT"}
    assert results["BTCUSDT"].quantity == Decimal("1")  # 0.1% of 2,000 at 2 per unit
    # The single serial new-risk slot is consumed by BTC, so ETH cannot add risk.
    assert results["ETHUSDT"].reason == "MIN_SIZE_OR_HARD_RISK"


@settings(max_examples=25, deadline=None)
@given(size=st.integers(min_value=1, max_value=20))
def test_es_is_monotone_in_added_loss(size: int):
    losses = [float(index) for index in range(size)]
    assert empirical_es(losses, 0.9) <= empirical_es([value + 1.0 for value in losses], 0.9) + 1e-9


def test_risk_vector_scales_with_quantity_and_keeps_venue_collateral_separate():
    unit = PerUnitRisk(Decimal("1"), Decimal("2"), Decimal("100"), Decimal("100"), Decimal("50"), Decimal("0"),
                       Decimal("10"))
    vector = unit.vector(Decimal("3"))
    assert isinstance(vector, RiskVector)
    assert vector.normal_loss == Decimal("3") and vector.stress_loss == Decimal("6")
    assert vector.venue_collateral == Decimal("30")
