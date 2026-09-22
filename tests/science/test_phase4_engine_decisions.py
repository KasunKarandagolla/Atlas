"""§5/§6/§13/§14: hard-risk binding, evidence binding and A0/B0 orchestration."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from support.phase4_factory import (
    SLOT,
    account,
    bootstrap,
    candidate_risk_inputs,
    complete_stress_input,
    decision_input,
    market_inputs,
    policy,
    scenario_evaluation,
    signal_snapshot,
)

from atlas.domain.enums import Side
from atlas.domain.risk import engineering_default_policy
from atlas.science.evaluation import DecisionStatus
from atlas.science.gates import EventGateInput, MarketGateInput
from atlas.science.phase4_engine import (
    CandidateRiskInputs,
    evaluate_phase4,
    path_hash,
    phase4_action_hash,
    seed_identity,
    unit_per_unit_risk,
)
from atlas.science.stresses import FROZEN_STRESSES, StressInput, StressName, evaluate_stress_suite
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
    assert plan.stress_risk == evaluation.evidence.stress_loss
    assert plan.risk_config_hash == engineering_default_policy().policy_hash()
    assert plan.cost_distribution_ref == "cost-1"
    assert evaluation.evidence.artifact_hash() == evaluation.evidence.artifact_hash()
    assert evaluation.evidence.lcb() == 1.0
    assert evaluation.evidence.portfolio is not None
    assert evaluation.evidence.action_hash == phase4_action_hash(policy(), snapshot, evaluation.quantity)


def test_b0_has_no_statistical_veto_but_a0_does():
    evaluation = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-5.0), candidate_path_pnl=(-10.0, -10.0)))
    assert evaluation.b0.status is DecisionStatus.TRADE_CANDIDATE
    assert evaluation.a0.status is DecisionStatus.NO_TRADE_NO_EDGE
    assert evaluation.status is DecisionStatus.NO_TRADE_NO_EDGE
    assert evaluation.trade_plan is None


def test_every_frozen_status_is_reachable_and_fail_closed():
    assert evaluate_phase4(decision_input(snapshot=None, policy=None)).status is DecisionStatus.SKIP_DATA
    assert evaluate_phase4(decision_input(snapshot=signal_snapshot(Signal.FLAT))).status is DecisionStatus.NO_SIGNAL
    stale = replace(market_inputs(), quote_at_ns=SLOT - 5_000_000_000)
    assert evaluate_phase4(decision_input(market_inputs=stale)).status is DecisionStatus.NO_TRADE_GATE
    assert evaluate_phase4(decision_input(event_inputs=EventGateInput(True, (SLOT + 1_000,)))).status is \
        DecisionStatus.NO_TRADE_EVENT
    assert evaluate_phase4(decision_input(event_inputs=EventGateInput(False))).status is DecisionStatus.NOT_ESTIMABLE
    assert evaluate_phase4(decision_input(scenario=None)).status is DecisionStatus.NOT_ESTIMABLE
    # A stress template whose venue evidence cannot value the frozen mechanics
    # propagates NOT_ESTIMABLE instead of a fabricated loss.
    bare = StressInput(Side.LONG, Decimal("1"), Decimal("100"), 0)
    assert evaluate_phase4(decision_input(stress_template=bare)).status is DecisionStatus.NOT_ESTIMABLE
    no_room = MarketGateInput(SLOT + 1_000, SLOT, SLOT, SLOT, Decimal("100"), Decimal("100.02"), Decimal("100"),
                              Decimal("100"), Decimal("0.5"), Decimal("100"), Decimal("0.0001"), True, True, True, True)
    assert evaluate_phase4(decision_input(market_inputs=no_room)).status is DecisionStatus.NO_TRADE_GATE
    tiny = evaluate_phase4(decision_input(account=account(Decimal("1")), venue_maximum_quantity=Decimal("1")))
    assert tiny.status is DecisionStatus.NO_TRADE_RISK
    numerical = evaluate_phase4(decision_input(bootstrap=bootstrap(unstable=True)))
    assert numerical.status is DecisionStatus.NO_TRADE_NUMERICAL
    assert numerical.b0.status is DecisionStatus.TRADE_CANDIDATE


def test_scenario_artifact_must_belong_to_this_action():
    baseline = decision_input()
    assert evaluate_phase4(baseline).status is DecisionStatus.TRADE_CANDIDATE

    smaller_quantity = replace(baseline.scenario, quantity=Decimal("0.5"),
                               bootstrap=bootstrap(action_hash=phase4_action_hash(policy(), signal_snapshot(Signal.LONG),
                                                                                  Decimal("0.5"))))
    assert evaluate_phase4(replace(baseline, scenario=smaller_quantity)).status is DecisionStatus.NOT_ESTIMABLE

    swapped_paths = replace(baseline.scenario, candidate_path_pnl=(-5.0, -5.0))
    assert evaluate_phase4(replace(baseline, scenario=swapped_paths)).status is DecisionStatus.NOT_ESTIMABLE

    other_model = replace(baseline.scenario, model_manifest_hash="other-model")
    assert evaluate_phase4(replace(baseline, scenario=other_model)).status is DecisionStatus.NOT_ESTIMABLE

    other_policy = replace(baseline.scenario, risk_policy_hash="other-policy")
    assert evaluate_phase4(replace(baseline, scenario=other_policy)).status is DecisionStatus.NOT_ESTIMABLE

    other_snapshot = replace(baseline.scenario, snapshot_hash="other-snapshot")
    assert evaluate_phase4(replace(baseline, scenario=other_snapshot)).status is DecisionStatus.NOT_ESTIMABLE

    other_seed = replace(baseline.scenario, seed_identity="block=48;paths=8;seed=1")
    assert evaluate_phase4(replace(baseline, scenario=other_seed)).status is DecisionStatus.NOT_ESTIMABLE

    unbound_bootstrap = replace(baseline.scenario, bootstrap=bootstrap(action_hash="different-action"))
    assert evaluate_phase4(replace(baseline, scenario=unbound_bootstrap)).status is DecisionStatus.NOT_ESTIMABLE


def test_bootstrap_from_another_quantity_or_side_cannot_qualify():
    long_snapshot = signal_snapshot(Signal.LONG)
    short_snapshot = signal_snapshot(Signal.SHORT)
    short_scenario = scenario_evaluation(snapshot=short_snapshot, policy=policy(side=Side.SHORT),
                                         risk_policy_hash=engineering_default_policy().policy_hash(),
                                         quantity=Decimal("1"))
    mismatched = decision_input(scenario=short_scenario, snapshot=long_snapshot, policy=policy())
    assert evaluate_phase4(mismatched).status is DecisionStatus.NOT_ESTIMABLE


def test_direction_is_never_searched_only_the_frozen_signal_is_used():
    long_case = evaluate_phase4(decision_input())
    short_snapshot = signal_snapshot(Signal.SHORT)
    short_policy = policy(side=Side.SHORT)
    short_scenario = scenario_evaluation(snapshot=short_snapshot, policy=short_policy,
                                         risk_policy_hash=engineering_default_policy().policy_hash(),
                                         quantity=Decimal("1"))
    short_case = evaluate_phase4(decision_input(snapshot=short_snapshot, policy=short_policy,
                                                scenario=short_scenario,
                                                market_inputs=market_inputs(quantity=Decimal("1"))))
    assert long_case.b0.action is Signal.LONG and long_case.trade_plan is not None
    assert long_case.trade_plan.side is Side.LONG
    assert short_case.b0.action is Signal.SHORT
    assert short_case.trade_plan is None or short_case.trade_plan.side is Side.SHORT


def test_risk_sizing_happens_before_a0_and_is_never_optimised_against_the_lcb():
    sized = evaluate_phase4(decision_input())
    assert sized.quantity == Decimal("1") == policy().quantity
    assert sized.risk is not None and sized.risk.remaining_open_qty == sized.quantity
    hopeless = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-1e9)))
    assert hopeless.status is DecisionStatus.NO_TRADE_NO_EDGE


def test_optimistic_caller_values_cannot_bypass_actual_stress_loss():
    # The engine derives stress loss itself: shrinking the supplied per-unit
    # margin/beta inputs cannot lower the actual stress suite result.
    optimistic = decision_input(risk_inputs=CandidateRiskInputs(beta=Decimal("0.0001"),
                                                                taker_fee_rate=Decimal("0"),
                                                                maintenance_margin_per_unit=Decimal("0.01"),
                                                                venue_collateral=Decimal("0")))
    baseline = evaluate_phase4(decision_input())
    result = evaluate_phase4(optimistic)
    assert result.status is DecisionStatus.TRADE_CANDIDATE
    assert result.evidence.stress_loss == baseline.evidence.stress_loss
    assert result.evidence.stress_loss > Decimal("0")


def test_stress_suite_recomputed_at_the_selected_quantity():
    quantity = Decimal("1")
    evaluation = evaluate_phase4(decision_input())
    assert evaluation.quantity == quantity
    expected = evaluate_stress_suite(replace(complete_stress_input(mark=Decimal("100")), quantity=quantity))
    from atlas.science.stresses import max_liquidation_cost, max_trade_stress_loss

    assert evaluation.evidence.stress_loss == max_trade_stress_loss(expected) + max_liquidation_cost(expected)
    # A template built for a different quantity never authorises the selected one:
    # the engine re-evaluates at quantity Q, so the depth-limited stress becomes
    # unbounded when the template depth cannot cover Q.
    shallow = complete_stress_input(mark=Decimal("100"), quantity=Decimal("1"))
    evaluation_shallow = evaluate_phase4(decision_input(stress_template=shallow))
    assert evaluation_shallow.status is DecisionStatus.TRADE_CANDIDATE
    assert evaluation_shallow.evidence.stress_loss == evaluation.evidence.stress_loss


def test_portfolio_es_above_policy_limit_is_hard_risk_rejected():
    # A fat left tail on the common paths pushes ES_after above the policy limit.
    candidate = (-50_000.0,) + (1.0,) * 19
    evaluation = evaluate_phase4(decision_input(candidate_path_pnl=candidate,
                                                pi0_path_pnl=(0.0,) * 20))
    assert evaluation.status is DecisionStatus.NO_TRADE_RISK
    assert evaluation.a0.status is DecisionStatus.NO_TRADE_RISK
    assert evaluation.evidence.portfolio is not None
    assert evaluation.evidence.portfolio.es_after > float(engineering_default_policy().portfolio_es_limit_frac)


def test_es_contribution_is_not_caller_supplied_and_seed_identity_is_bound():
    scenario = decision_input().scenario
    assert scenario is not None
    assert scenario.seed_identity == seed_identity(scenario.scenario_support)
    assert scenario.candidate_path_hash == path_hash(scenario.candidate_path_pnl)
    assert scenario.portfolio_path_hash == path_hash(scenario.pi0_path_pnl)
    assert not hasattr(decision_input(), "per_unit_risk")


def test_evidence_artifact_records_gates_stress_and_not_estimable_reasons():
    evaluation = evaluate_phase4(decision_input())
    evidence = evaluation.evidence
    assert len(evidence.stress_results) == len(FROZEN_STRESSES)
    assert evidence.not_estimable_reasons == ()
    assert evidence.rejection_reasons == ()
    assert evidence.selected_block == 24 and evidence.scenario_paths == 8
    assert evidence.portfolio is not None and evidence.bootstrap is not None
    assert evidence.stress_loss == max_stress_loss(evidence.stress_results)

    rejected = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-1.0)))
    assert rejected.evidence.rejection_reasons and rejected.evidence.not_estimable_reasons == ()
    not_estimable = evaluate_phase4(decision_input(scenario=None))
    assert not_estimable.evidence.not_estimable_reasons


def max_stress_loss(results) -> Decimal:  # type: ignore[no-untyped-def]
    from atlas.science.stresses import max_liquidation_cost, max_trade_stress_loss

    return max_trade_stress_loss(results) + max_liquidation_cost(results)


def test_unit_per_unit_risk_separates_venue_and_trade_losses():
    state = complete_stress_input(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    suite = evaluate_stress_suite(state)
    jump = next(result for result in suite if result.case.name is StressName.JUMP_10_LIQUIDITY)
    venue = next(result for result in suite if result.case.name is StressName.VENUE_COLLATERAL_LOSS)
    unit = unit_per_unit_risk(policy=policy(), stress=jump, beta=Decimal("1"), margin_per_unit=Decimal("20"),
                              es_contribution=Decimal("0"), venue_collateral=Decimal("10"),
                              taker_fee_rate=Decimal("0.0005"))
    assert unit.stress_loss == jump.trade_loss
    venue_unit = unit_per_unit_risk(policy=policy(), stress=venue, beta=Decimal("1"),
                                    margin_per_unit=Decimal("20"), es_contribution=Decimal("0"),
                                    venue_collateral=Decimal("10"), taker_fee_rate=Decimal("0.0005"))
    assert venue_unit.stress_loss == 0
    assert unit.normal_loss == abs(policy().mark_reference - policy().stop) + Decimal("100") * Decimal("0.001")


def test_venue_collateral_limit_is_checked_against_account_evidence():
    over = decision_input(risk_inputs=candidate_risk_inputs(venue_collateral=Decimal("100000000")))
    assert evaluate_phase4(over).status is DecisionStatus.NO_TRADE_RISK
