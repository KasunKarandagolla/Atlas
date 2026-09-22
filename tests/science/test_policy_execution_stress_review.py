from decimal import Decimal

from atlas.domain.enums import Side
from atlas.science.execution_replay import ExecutionEvidenceStatus, ReplayMinute, executable_stop_bounds, ioc_entry
from atlas.science.gates import EventGateInput, GateStatus, MarketGateInput, event_gate, market_gate
from atlas.science.stresses import (
    FROZEN_STRESSES,
    StressCollateralAssumptions,
    StressInput,
    StressName,
    StressStatus,
    evaluate_stress,
)
from atlas.strategy.policy import VenueFilters, fixed_policy, time_exit_collar


def test_horizon_is_decision_slot_not_late_construction_time_and_exit_rounding_is_protective():
    tick = Decimal(".1")
    p = fixed_policy(Side.LONG, Decimal("1"), Decimal("100"), Decimal("100.1"), Decimal("100"), .01,
                     VenueFilters(tick, Decimal(".1"), Decimal(".1")), 0, 20_000_000_000)
    assert p.horizon_end_ns == 24 * 3_600_000_000_000
    assert time_exit_collar(Side.LONG, Decimal("100.03"), Decimal("101"), tick) == Decimal("99.8")
    assert time_exit_collar(Side.SHORT, Decimal("100"), Decimal("101.03"), tick) == Decimal("101.2")


def test_missing_depth_is_not_a_benign_no_fill_and_stop_has_adverse_bound():
    missing = ReplayMinute(0, Decimal("100"), Decimal("101"), None, None, Decimal("98"), Decimal("102"), Decimal("97"), Decimal("103"))
    no_evidence = ioc_entry(Side.LONG, Decimal("1"), Decimal("102"), missing)
    assert no_evidence.entry_status is None and no_evidence.evidence_status is ExecutionEvidenceStatus.NO_EXECUTION_DATA
    minute = ReplayMinute(0, Decimal("100"), Decimal("101"), Decimal("10"), Decimal("10"), Decimal("98"), Decimal("102"), Decimal("97"), Decimal("103"))
    bounds = executable_stop_bounds(Side.LONG, Decimal("1"), Decimal("99"), minute, spread_impact=Decimal(".5"))
    assert bounds.triggered and bounds.adverse is not None and bounds.favorable is not None
    assert bounds.adverse.price == Decimal("96.5") < bounds.favorable.price


def test_all_stresses_exist_and_venue_loss_is_not_trade_stop_loss():
    assert len(FROZEN_STRESSES) == 12
    state = StressInput(
        Side.LONG, Decimal("1"), Decimal("100"), 0,
        collateral=StressCollateralAssumptions(Decimal("50")),
    )
    venue = evaluate_stress(next(x for x in FROZEN_STRESSES if x.name is StressName.VENUE_COLLATERAL_LOSS), state)
    assert venue.trade_loss == 0 and venue.venue_collateral_loss == Decimal("50") and venue.liquidated
    # Missing versioned mechanics are never replaced by an invented formula.
    jump = evaluate_stress(next(x for x in FROZEN_STRESSES if x.name is StressName.JUMP_5_LIQUIDITY), state)
    assert jump.status is StressStatus.NOT_ESTIMABLE and jump.trade_loss is None


def test_market_and_event_gates_fail_closed():
    value = MarketGateInput(100, 100, 100, 100, Decimal("100"), Decimal("100.02"), Decimal("100"), Decimal("100"), None, Decimal("1"), Decimal("0"), True, True, True, True)
    assert market_gate(value)[0] is GateStatus.NOT_ESTIMABLE
    assert event_gate(0, EventGateInput(False))[0] is GateStatus.NOT_ESTIMABLE
    assert event_gate(0, EventGateInput(True, (1_000_000_000_000,)))[0] is GateStatus.NO_TRADE_EVENT
