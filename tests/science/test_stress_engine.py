"""§4: deterministic stresses valued only from explicit, versioned evidence."""

from __future__ import annotations

from decimal import Decimal

import pytest
from support.phase4_factory import complete_stress_input

from atlas.domain.enums import Side
from atlas.science.stresses import (
    FROZEN_STRESSES,
    StressCollateralAssumptions,
    StressExecutionAssumptions,
    StressInput,
    StressMarginAssumptions,
    StressName,
    StressStatus,
    evaluate_stress,
    evaluate_stress_suite,
    max_liquidation_cost,
    max_trade_stress_loss,
    stress_path_is_coherent,
    stressed_venue_loss,
    suite_is_estimable,
)


def case(name: StressName):
    return next(item for item in FROZEN_STRESSES if item.name is name)


def test_every_frozen_stress_is_represented():
    assert {item.name for item in FROZEN_STRESSES} == set(StressName)
    assert len(FROZEN_STRESSES) == 12
    assert {item.name for item in FROZEN_STRESSES if item.adverse_price_jump} == {
        StressName.JUMP_5_LIQUIDITY, StressName.JUMP_10_LIQUIDITY, StressName.BTC_ETH_CORRELATED_SHOCK}


def test_complete_evidence_produces_estimable_trade_and_account_losses():
    state = complete_stress_input(side=Side.LONG, quantity=Decimal("2"), mark=Decimal("100"))
    results = evaluate_stress_suite(state)
    assert suite_is_estimable(tuple(results))
    values = {result.case.name: result for result in results}
    # Depth is supplied per step, so the full quantity exits at the first stressed price.
    assert values[StressName.JUMP_5_LIQUIDITY].trade_loss == Decimal("10")  # 2 * (100 - 95)
    assert values[StressName.JUMP_10_LIQUIDITY].trade_loss == Decimal("20")  # 2 * (100 - 90)
    assert values[StressName.VENUE_COLLATERAL_LOSS].trade_loss == 0
    assert stressed_venue_loss(tuple(results)) == Decimal("500")
    assert max_trade_stress_loss(tuple(results)) == Decimal("20")
    assert max_liquidation_cost(tuple(results)) == Decimal("0")


def test_short_direction_inverts_the_stressed_loss_sign():
    state = complete_stress_input(side=Side.SHORT, quantity=Decimal("1"), mark=Decimal("100"))
    results = {result.case.name: result for result in evaluate_stress_suite(state)}
    assert results[StressName.JUMP_10_LIQUIDITY].trade_loss == 0
    rise = StressInput(Side.SHORT, Decimal("1"), Decimal("100"), 0,
                       StressExecutionAssumptions(stressed_executable_prices={StressName.JUMP_10_LIQUIDITY: (Decimal("115"),)},
                                                  stressed_available_depth={StressName.JUMP_10_LIQUIDITY: (Decimal("1"),)},
                                                  stressed_spreads={StressName.JUMP_10_LIQUIDITY: Decimal("0.5")}),
                       StressMarginAssumptions(maintenance_margin_tiers={StressName.JUMP_10_LIQUIDITY: (Decimal("1"),)},
                                               liquidation_thresholds={StressName.JUMP_10_LIQUIDITY: Decimal("1000")},
                                               liquidation_mechanics={StressName.JUMP_10_LIQUIDITY: "supplied"}))
    assert evaluate_stress(case(StressName.JUMP_10_LIQUIDITY), rise).trade_loss == Decimal("15")


def test_missing_venue_mechanics_yield_not_estimable_and_never_invented_numbers():
    bare = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0)
    results = evaluate_stress_suite(bare)
    assert not suite_is_estimable(tuple(results))
    for result in results:
        if result.case.name is not StressName.VENUE_COLLATERAL_LOSS:
            assert result.status is StressStatus.NOT_ESTIMABLE
            assert result.trade_loss is None
    jump = evaluate_stress(case(StressName.JUMP_5_LIQUIDITY), bare)
    assert jump.reason == "missing margin/liquidation mechanics"
    # A price path alone must not be turned into a loss without margin/liquidation evidence.
    prices_only = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0,
                              StressExecutionAssumptions(stressed_executable_prices={
                                  StressName.JUMP_5_LIQUIDITY: (Decimal("95"),)},
                                  stressed_available_depth={StressName.JUMP_5_LIQUIDITY: (Decimal("1"),)},
                                  stressed_spreads={StressName.JUMP_5_LIQUIDITY: Decimal("0.5")}))
    assert evaluate_stress(case(StressName.JUMP_5_LIQUIDITY), prices_only).status is StressStatus.NOT_ESTIMABLE


def test_depth_and_spread_stresses_require_their_own_evidence():
    depth_case = case(StressName.DEPTH_MINUS_90)
    without_depth = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0,
                                StressExecutionAssumptions(stressed_executable_prices={depth_case.name: (Decimal("99"),)}),
                                StressMarginAssumptions(maintenance_margin_tiers={depth_case.name: (Decimal("1"),)},
                                                        liquidation_thresholds={depth_case.name: Decimal("1000")},
                                                        liquidation_mechanics={depth_case.name: "supplied"}))
    assert evaluate_stress(depth_case, without_depth).reason == "missing stressed available depth evidence"
    spread_case = case(StressName.SPREAD_10X)
    without_spread = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0,
                                 StressExecutionAssumptions(stressed_executable_prices={spread_case.name: (Decimal("99"),)}),
                                 StressMarginAssumptions(maintenance_margin_tiers={spread_case.name: (Decimal("1"),)},
                                                         liquidation_thresholds={spread_case.name: Decimal("1000")},
                                                         liquidation_mechanics={spread_case.name: "supplied"}))
    assert evaluate_stress(spread_case, without_spread).reason == "missing stressed spread evidence"


def test_depth_limited_path_cannot_bound_the_exit_and_is_not_estimable():
    name = StressName.JUMP_5_LIQUIDITY
    shallow = StressInput(Side.LONG, Decimal("5"), Decimal("100"), 0,
                          StressExecutionAssumptions(stressed_executable_prices={name: (Decimal("95"), Decimal("94"))},
                                                     stressed_available_depth={name: (Decimal("1"), Decimal("1"))},
                                                     stressed_spreads={name: Decimal("0.5")}),
                          StressMarginAssumptions(maintenance_margin_tiers={name: (Decimal("1"),)},
                                                  liquidation_thresholds={name: Decimal("1000")},
                                                  liquidation_mechanics={name: "supplied"}))
    result = evaluate_stress(case(name), shallow)
    assert result.status is StressStatus.NOT_ESTIMABLE
    assert result.reason == "insufficient stressed path/depth to bound the exit"


def test_correlated_shock_requires_paired_paths_and_correlation_evidence():
    name = StressName.BTC_ETH_CORRELATED_SHOCK
    margin = StressMarginAssumptions(maintenance_margin_tiers={name: (Decimal("1"),)},
                                     liquidation_thresholds={name: Decimal("1000")},
                                     liquidation_mechanics={name: "supplied"})
    unpaired = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0,
                           StressExecutionAssumptions(stressed_executable_prices={name: (Decimal("90"),)},
                                                      stressed_available_depth={name: (Decimal("1"),)},
                                                      stressed_spreads={name: Decimal("0.5")}), margin)
    assert evaluate_stress(case(name), unpaired).reason == "missing paired stressed instrument paths"
    paired = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0,
                         StressExecutionAssumptions(stressed_executable_prices={name: (Decimal("90"),)},
                                                    stressed_available_depth={name: (Decimal("1"),)},
                                                    stressed_spreads={name: Decimal("0.5")},
                                                    correlation_values={name: Decimal("1")},
                                                    paired_instrument_paths={name: {"BTCUSDT": (Decimal("90"),),
                                                                                   "ETHUSDT": (Decimal("90"),)}}), margin)
    assert evaluate_stress(case(name), paired).status is StressStatus.ESTIMATED


def test_funding_haircut_and_maintenance_tier_stresses_use_supplied_values():
    state = complete_stress_input()
    funding = evaluate_stress(case(StressName.FUNDING_DEBIT_5X), state)
    assert funding.trade_loss == Decimal("5") * Decimal("0.0003") * Decimal("100")
    haircut = evaluate_stress(case(StressName.USDT_HAIRCUT_10), state)
    assert haircut.trade_loss == 0 and haircut.collateral_loss == Decimal("50")  # 10% of 500 venue collateral
    tier = case(StressName.MAINTENANCE_MARGIN_TIER)
    breaching = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0, state.execution,
                            StressMarginAssumptions(maintenance_margin_tiers={tier.name: (Decimal("2000"),)},
                                                    liquidation_thresholds={tier.name: Decimal("1000")},
                                                    liquidation_costs={tier.name: Decimal("7")},
                                                    liquidation_mechanics={tier.name: "supplied-tier-schedule-v1"}),
                            state.funding, stress_collateral())
    result = evaluate_stress(tier, breaching)
    assert result.liquidated is True and result.liquidation_cost == Decimal("7")


def stress_collateral() -> StressCollateralAssumptions:
    return StressCollateralAssumptions(Decimal("500"))


def test_stress_path_coherence_requires_the_declared_adverse_jump():
    assert stress_path_is_coherent((Decimal("95"),), Decimal("100"), jump=Decimal("0.05"), side=Side.LONG)
    assert not stress_path_is_coherent((Decimal("99"),), Decimal("100"), jump=Decimal("0.05"), side=Side.LONG)
    assert stress_path_is_coherent((Decimal("105"),), Decimal("100"), jump=Decimal("0.05"), side=Side.SHORT)
    assert not stress_path_is_coherent((), Decimal("100"), jump=Decimal("0.05"), side=Side.LONG)


def test_venue_collateral_loss_is_never_folded_into_trade_stop_loss():
    state = complete_stress_input()
    results = evaluate_stress_suite(state)
    venue = next(result for result in results if result.case.name is StressName.VENUE_COLLATERAL_LOSS)
    assert venue.trade_loss == 0 and venue.venue_collateral_loss == Decimal("500")
    assert max_trade_stress_loss(tuple(results)) < venue.venue_collateral_loss
    with pytest.raises(AttributeError):
        _ = venue.loss  # the invented legacy field must not exist
