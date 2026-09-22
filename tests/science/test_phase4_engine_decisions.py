"""§13/§14/§15: complete A0/B0 orchestration and immutable TradePlan mapping."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from support.phase4_factory import (
    SLOT,
    account,
    bootstrap,
    complete_stress_input,
    decision_input,
    market_inputs,
    policy,
    signal_snapshot,
)

from atlas.domain.enums import Side
from atlas.domain.risk import engineering_default_policy
from atlas.science.evaluation import DecisionStatus
from atlas.science.gates import EventGateInput, MarketGateInput
from atlas.science.phase4_engine import evaluate_phase4, unit_per_unit_risk
from atlas.science.stresses import FROZEN_STRESSES, evaluate_stress_suite
from atlas.strategy.crypto_trend_24h_v1 import Signal


def test_qualified_candidate_maps_to_an_immutable_trade_plan():
    evaluation = evaluate_phase4(decision_input())
    assert evaluation.status is DecisionStatus.TRADE_CANDIDATE
    plan = evaluation.trade_plan
    assert plan is not None
    snapshot = signal_snapshot(Signal.LONG)
    assert plan.snapshot_hash == snapshot.snapshot_hash()
    assert plan.version.startswith("CRYPTO_TREND_24H_V1")
    assert plan.side is Side.LONG
    assert plan.stop == policy().stop and plan.stop_trigger_basis == "MarkPrice"
    assert plan.horizon_end_ns == snapshot.horizon_end_ns
    assert plan.expires_at_ns == snapshot.expires_at_ns
    assert plan.qty_limit == evaluation.quantity
    assert plan.normal_risk == evaluation.risk.normal_loss
    assert plan.stress_risk >= Decimal("0")
    assert plan.risk_config_hash == engineering_default_policy().policy_hash()
    assert plan.cost_distribution_ref == "cost-1"
    assert evaluation.evidence.artifact_hash() == evaluation.evidence.artifact_hash()
    assert evaluation.evidence.lcb() == 1.0
    assert evaluation.evidence.portfolio is not None


def test_b0_has_no_statistical_veto_but_a0_does():
    evaluation = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-5.0),
                                               candidate_path_pnl=(-10.0, -10.0)))
    assert evaluation.b0.status is DecisionStatus.TRADE_CANDIDATE
    assert evaluation.a0.status is DecisionStatus.NO_TRADE_NO_EDGE
    assert evaluation.status is DecisionStatus.NO_TRADE_NO_EDGE
    assert evaluation.trade_plan is None


def test_every_frozen_status_is_reachable_and_fail_closed():
    assert evaluate_phase4(decision_input(snapshot=None, policy=None)).status is DecisionStatus.SKIP_DATA
    assert evaluate_phase4(decision_input(snapshot=signal_snapshot(Signal.FLAT))).status is DecisionStatus.NO_SIGNAL
    stale = replace(market_inputs(), quote_at_ns=SLOT - 5_000_000_000)
    assert evaluate_phase4(decision_input(market_inputs=stale)).status is DecisionStatus.NO_TRADE_GATE
    event = EventGateInput(True, (SLOT + 1_000,))
    assert evaluate_phase4(decision_input(event_inputs=event)).status is DecisionStatus.NO_TRADE_EVENT
    missing_calendar = EventGateInput(False)
    assert evaluate_phase4(decision_input(event_inputs=missing_calendar)).status is DecisionStatus.NOT_ESTIMABLE
    assert evaluate_phase4(decision_input(stress_input=None)).status is DecisionStatus.NOT_ESTIMABLE
    assert evaluate_phase4(decision_input(bootstrap=None)).status is DecisionStatus.NOT_ESTIMABLE
    no_room = MarketGateInput(SLOT + 1_000, SLOT, SLOT, SLOT, Decimal("100"), Decimal("100.02"), Decimal("100"),
                              Decimal("100"), Decimal("0.5"), Decimal("100"), Decimal("0.0001"), True, True, True, True)
    assert evaluate_phase4(decision_input(market_inputs=no_room)).status is DecisionStatus.NO_TRADE_GATE
    tiny = evaluate_phase4(decision_input(account=account(Decimal("1")), venue_maximum_quantity=Decimal("1")))
    assert tiny.status is DecisionStatus.NO_TRADE_RISK
    numerical = evaluate_phase4(decision_input(bootstrap=bootstrap(unstable=True)))
    assert numerical.status is DecisionStatus.NO_TRADE_NUMERICAL
    assert numerical.b0.status is DecisionStatus.TRADE_CANDIDATE


def test_direction_is_never_searched_only_the_frozen_signal_is_used():
    long_case = evaluate_phase4(decision_input(snapshot=signal_snapshot(Signal.LONG)))
    short_case = evaluate_phase4(decision_input(
        snapshot=signal_snapshot(Signal.SHORT),
        policy=policy(side=Side.SHORT),
        market_inputs=market_inputs(quantity=Decimal("5")),
        per_unit_risk=None,
    ))
    assert long_case.b0.action is Signal.LONG and long_case.trade_plan is not None
    assert long_case.trade_plan.side is Side.LONG
    assert short_case.b0.action is Signal.SHORT and short_case.quantity is None


def test_risk_sizing_happens_before_a0_and_is_never_optimised_against_the_lcb():
    sized = evaluate_phase4(decision_input())
    # The frozen policy quantity caps the venue maximum; risk sizing only scales down.
    assert sized.quantity == Decimal("1") == policy().quantity
    assert sized.risk is not None and sized.risk.remaining_open_qty == sized.quantity
    hopeless = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-1e9)))
    assert hopeless.quantity == sized.quantity
    assert hopeless.status is DecisionStatus.NO_TRADE_NO_EDGE


def test_evidence_artifact_records_gates_stress_and_not_estimable_reasons():
    evaluation = evaluate_phase4(decision_input(stress_input=complete_stress_input()))
    evidence = evaluation.evidence
    assert len(evidence.stress_results) == len(FROZEN_STRESSES)
    assert evidence.not_estimable_reasons == ()
    assert evidence.rejection_reasons == ()
    assert evidence.selected_block == 24 and evidence.scenario_paths == 8
    assert evidence.portfolio is not None and evidence.bootstrap is not None

    rejected = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-1.0)))
    assert rejected.evidence.rejection_reasons and rejected.evidence.not_estimable_reasons == ()
    not_estimable = evaluate_phase4(decision_input(stress_input=None))
    assert not_estimable.evidence.not_estimable_reasons


def test_unit_per_unit_risk_separates_venue_and_trade_losses():
    state = complete_stress_input(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    suite = evaluate_stress_suite(state)
    jump = next(result for result in suite if result.case.name.value == "JUMP_10_LIQUIDITY")
    venue = next(result for result in suite if result.case.name.value == "VENUE_COLLATERAL_LOSS")
    unit = unit_per_unit_risk(policy=policy(), stress=jump, beta=Decimal("1"), margin_per_unit=Decimal("20"),
                              es_contribution=Decimal("0"), venue_collateral=Decimal("10"),
                              taker_fee_rate=Decimal("0.0005"))
    assert unit.stress_loss == jump.trade_loss
    venue_unit = unit_per_unit_risk(policy=policy(), stress=venue, beta=Decimal("1"),
                                    margin_per_unit=Decimal("20"), es_contribution=Decimal("0"),
                                    venue_collateral=Decimal("10"), taker_fee_rate=Decimal("0.0005"))
    assert venue_unit.stress_loss == 0
    assert unit.normal_loss == abs(policy().mark_reference - policy().stop) + Decimal("100") * Decimal("0.001")
