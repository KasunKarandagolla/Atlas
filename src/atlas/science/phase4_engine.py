"""Complete pure offline A0/B0 orchestration for the frozen Phase-4 policy.

Frozen decision order (deterministic, fail-closed)::

    SKIP_DATA -> gate (NO_TRADE_GATE / NO_TRADE_EVENT / NOT_ESTIMABLE)
    -> NO_SIGNAL -> NOT_ESTIMABLE(scenario/stress/uncertainty)
    -> NO_TRADE_RISK -> NO_TRADE_NUMERICAL -> NO_TRADE_NO_EDGE -> TRADE_CANDIDATE

Risk quantity is solved once against the deterministic hard constraints; the
statistical A0 evaluation then runs *once* at that quantity.  Neither direction
nor size is ever optimised against a noisy LCB.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.risk import RiskPolicy
from atlas.domain.trade_plan import TradePlan
from atlas.risk.engine import AccountState, RiskDecision, RiskVector, evaluate_reservation
from atlas.risk.portfolio import PortfolioDecision, common_path_portfolio_decision
from atlas.risk.sizing import PerUnitRisk, QuantitySelection, deterministic_risk_quantity
from atlas.science.evaluation import DecisionStatus, EvaluationResult, evaluate_actions_and_portfolio
from atlas.science.gates import EventGateInput, GateStatus, MarketGateInput, event_gate, market_gate
from atlas.science.stresses import (
    StressInput,
    StressResult,
    StressStatus,
    evaluate_stress_suite,
    max_liquidation_cost,
    max_trade_stress_loss,
    suite_is_estimable,
)
from atlas.science.trade_plan_mapping import candidate_trade_plan
from atlas.science.uncertainty import OuterBootstrapResult
from atlas.strategy.crypto_trend_24h_v1 import FeatureSnapshot, Signal
from atlas.strategy.policy import FixedPolicy


@dataclass(frozen=True)
class ScenarioSupport:
    """Support verdict for the sampled joint block/scenario construction."""

    estimable: bool
    reasons: tuple[str, ...] = ()
    paths: int = 0
    selected_block: int | None = None
    seed: int = 0


@dataclass(frozen=True)
class EvaluationEvidence:
    market_gate_status: GateStatus
    market_gate_reasons: tuple[str, ...]
    event_gate_status: GateStatus
    event_gate_reasons: tuple[str, ...]
    model_manifest_hash: str
    block_manifest_hash: str
    scenario_config_hash: str
    selected_block: int | None
    scenario_paths: int
    bootstrap: OuterBootstrapResult | None
    portfolio: PortfolioDecision | None
    stress_results: tuple[StressResult, ...]
    rejection_reasons: tuple[str, ...]
    not_estimable_reasons: tuple[str, ...]
    risk_reasons: tuple[str, ...] = ()

    def lcb(self) -> float | None:
        return self.bootstrap.lcb if self.bootstrap is not None else None

    def artifact_hash(self) -> str:
        return _canonical_hash(self)


@dataclass(frozen=True)
class Phase4DecisionInput:
    now_ns: int
    snapshot: FeatureSnapshot | None
    policy: FixedPolicy | None
    risk_policy: RiskPolicy
    account: AccountState
    pending_reservations: tuple[RiskVector, ...]
    market_inputs: MarketGateInput | None
    event_inputs: EventGateInput | None
    scenario_support: ScenarioSupport
    bootstrap: OuterBootstrapResult | None
    pi0_path_pnl: tuple[float, ...]
    candidate_path_pnl: tuple[float, ...]
    per_unit_risk: PerUnitRisk | None
    venue_maximum_quantity: Decimal
    lot: Decimal
    minimum_quantity: Decimal
    leverage: Decimal
    model_manifest_hash: str
    block_manifest_hash: str
    scenario_config_hash: str
    cost_evidence_ref: str
    account_scope: str
    plan_id: str
    availability_cutoff_ns: int
    stress_input: StressInput | None = None
    not_estimable_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class Phase4Evaluation:
    status: DecisionStatus
    b0: EvaluationResult
    a0: EvaluationResult
    evidence: EvaluationEvidence
    trade_plan: TradePlan | None
    quantity: Decimal | None
    risk: RiskVector | None
    risk_decision: RiskDecision | None
    reason: str


def _canonical_hash(value: object) -> str:
    def default(item: object) -> object:
        if hasattr(item, "value"):
            return item.value
        if hasattr(item, "__dataclass_fields__"):
            return {name: getattr(item, name) for name in item.__dataclass_fields__}  # type: ignore[attr-defined]
        return str(item)

    return hashlib.sha256(json.dumps(value, default=default, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _not_estimable(*values: str | None) -> tuple[str, ...]:
    return tuple(value for value in values if value)


def unit_per_unit_risk(*, policy: FixedPolicy, stress: StressResult, beta: Decimal,
                       margin_per_unit: Decimal, es_contribution: Decimal,
                       venue_collateral: Decimal, taker_fee_rate: Decimal) -> PerUnitRisk:
    """Per-unit hard-risk vector; the stress loss is the frozen suite bound at unit size."""
    stop_loss = abs(policy.mark_reference - policy.stop)
    normal = stop_loss + policy.mark_reference * taker_fee_rate * Decimal("2")
    stress_loss = stress.trade_loss if stress.trade_loss is not None else Decimal("0")
    stress_loss += stress.liquidation_cost or Decimal("0")
    return PerUnitRisk(normal, stress_loss, policy.mark_reference, policy.mark_reference * beta,
                       margin_per_unit, es_contribution, venue_collateral)


def size_phase4_quantity(value: Phase4DecisionInput) -> QuantitySelection:
    """Largest hard-constraint quantity, before any statistical acceptance."""
    if value.per_unit_risk is None or value.policy is None:
        return QuantitySelection(None, None, "missing per-unit risk evidence")
    # The frozen policy quantity is an upper bound; risk sizing may only scale down.
    maximum = min(value.venue_maximum_quantity, value.policy.quantity)
    return deterministic_risk_quantity(policy=value.risk_policy, account=value.account,
                                       pending=value.pending_reservations, per_unit=value.per_unit_risk,
                                       venue_maximum=maximum, lot=value.lot,
                                       minimum=value.minimum_quantity, leverage=value.leverage)


def evaluate_phase4(value: Phase4DecisionInput) -> Phase4Evaluation:
    """Evaluate one already-frozen action once; never search direction or size."""
    not_estimable = list(value.not_estimable_reasons)
    if value.market_inputs is None:
        market_status, market_reason = GateStatus.NOT_ESTIMABLE, "missing market gate inputs"
    else:
        market_status, market_reason = market_gate(value.market_inputs)
    if value.event_inputs is None:
        event_status, event_reason = GateStatus.NOT_ESTIMABLE, "missing event gate inputs"
    else:
        event_status, event_reason = event_gate(value.now_ns, value.event_inputs)

    def finish(status: DecisionStatus, reason: str, *, b0: EvaluationResult | None = None,
               a0: EvaluationResult | None = None, plan: TradePlan | None = None,
               quantity: Decimal | None = None, risk: RiskVector | None = None,
               risk_decision: RiskDecision | None = None,
               stress_results: tuple[StressResult, ...] = ()) -> Phase4Evaluation:
        signal = value.snapshot.signal if value.snapshot is not None else Signal.FLAT
        fallback_b0 = EvaluationResult("B0", status, reason, signal)
        fallback_a0 = EvaluationResult("A0", status, reason, signal)
        rejection = () if status is DecisionStatus.TRADE_CANDIDATE else (reason,)
        evidence = EvaluationEvidence(market_status, tuple([market_reason] if market_reason else []), event_status,
                                      tuple([event_reason] if event_reason else []), value.model_manifest_hash,
                                      value.block_manifest_hash, value.scenario_config_hash,
                                      value.scenario_support.selected_block, value.scenario_support.paths,
                                      value.bootstrap, None, stress_results, rejection,
                                      tuple(dict.fromkeys(not_estimable)),
                                      risk_decision.reasons if risk_decision is not None else ())
        return Phase4Evaluation(status, b0 or fallback_b0, a0 or fallback_a0, evidence, plan, quantity, risk,
                                risk_decision, reason)

    if value.snapshot is None or value.policy is None:
        return finish(DecisionStatus.SKIP_DATA, "missing causal snapshot/policy for the decision slot")
    if market_status in {GateStatus.NOT_ESTIMABLE, GateStatus.GATE_DISABLED_DIAGNOSTIC}:
        not_estimable.append(market_reason or "market gate not estimable")
    if event_status in {GateStatus.NOT_ESTIMABLE, GateStatus.GATE_DISABLED_DIAGNOSTIC}:
        not_estimable.append(event_reason or "event gate not estimable")
    if market_status is GateStatus.NO_TRADE_GATE:
        return finish(DecisionStatus.NO_TRADE_GATE, market_reason)
    if event_status is GateStatus.NO_TRADE_EVENT:
        return finish(DecisionStatus.NO_TRADE_EVENT, event_reason)
    if value.snapshot.signal is Signal.FLAT:
        return finish(DecisionStatus.NO_SIGNAL, "frozen trend signal is flat")
    if not value.scenario_support.estimable:
        not_estimable.extend(value.scenario_support.reasons)
    stress_results: tuple[StressResult, ...] = ()
    if value.stress_input is not None:
        stress_results = evaluate_stress_suite(value.stress_input)
        if not suite_is_estimable(stress_results):
            not_estimable.extend(result.reason for result in stress_results if result.status is StressStatus.NOT_ESTIMABLE)
    else:
        not_estimable.append("stress assumptions unavailable")
    if not_estimable:
        return finish(DecisionStatus.NOT_ESTIMABLE, not_estimable[0])
    assert value.policy is not None and value.stress_input is not None
    selection = size_phase4_quantity(value)
    if selection.quantity is None or selection.risk is None:
        return finish(DecisionStatus.NO_TRADE_RISK, selection.reason)
    # Stress and risk are re-evaluated once at the deterministic sized quantity.
    sized = evaluate_stress_suite(value.stress_input)
    if not suite_is_estimable(sized):
        not_estimable.extend(result.reason for result in sized if result.status is StressStatus.NOT_ESTIMABLE)
        return finish(DecisionStatus.NOT_ESTIMABLE, not_estimable[0])
    stress_limit_breach = any(result.liquidated and not result.case.venue_collateral_loss for result in sized)
    risk_decision = evaluate_reservation(value.risk_policy, value.account, value.pending_reservations,
                                         selection.risk, leverage=value.leverage)
    if stress_limit_breach:
        risk_decision = RiskDecision(False, ("stress liquidation mechanics breach",), risk_decision.scaled_normal_budget)
    if not risk_decision.accepted:
        return finish(DecisionStatus.NO_TRADE_RISK, risk_decision.reasons[0] if risk_decision.reasons else "hard risk",
                      quantity=selection.quantity, risk=selection.risk, risk_decision=risk_decision,
                      stress_results=sized)
    # B0 has no statistical expected-alpha veto: the frozen trend direction with
    # identical execution mechanics and identical hard capital/risk constraints.
    b0 = EvaluationResult("B0", DecisionStatus.TRADE_CANDIDATE, "raw frozen trend policy", value.snapshot.signal)
    if value.bootstrap is None:
        return finish(DecisionStatus.NOT_ESTIMABLE, "outer bootstrap evidence unavailable", b0=b0, a0=None,
                      quantity=selection.quantity, risk=selection.risk, risk_decision=risk_decision,
                      stress_results=sized)
    assert value.bootstrap is not None
    portfolio: PortfolioDecision | None = None
    j: float | None = None
    lcb: float | None = value.bootstrap.lcb
    if value.pi0_path_pnl and len(value.pi0_path_pnl) == len(value.candidate_path_pnl) and lcb is not None:
        portfolio = common_path_portfolio_decision(lcb, float(value.account.eligible_equity), value.pi0_path_pnl,
                                                   value.candidate_path_pnl, float(value.risk_policy.portfolio_es_alpha))
        j = portfolio.j
    numerical = value.bootstrap.numerical_unstable
    _, a0, _ = evaluate_actions_and_portfolio(signal=value.snapshot.signal, gates_ok=True, scenario_support_ok=True,
                                              hard_risk_ok=True, a0_lcb=lcb, a0_j=j)
    status = a0.status
    reason = a0.reason
    if status is DecisionStatus.TRADE_CANDIDATE and numerical:
        status = DecisionStatus.NO_TRADE_NUMERICAL
        reason = "independent inner seeds flip qualification"
        a0 = EvaluationResult("A0", status, reason, value.snapshot.signal, lcb, j)
    stress_risk = max_trade_stress_loss(sized) + max_liquidation_cost(sized)
    plan = None
    if status is DecisionStatus.TRADE_CANDIDATE:
        assert selection.quantity is not None
        plan = candidate_trade_plan(plan_id=value.plan_id, snapshot=value.snapshot, policy=value.policy,
                                    policy_hash=value.risk_policy.policy_hash(),
                                    normal_risk=selection.risk.normal_loss, stress_risk=stress_risk,
                                    margin=selection.risk.margin, leverage=value.leverage,
                                    cost_evidence_ref=value.cost_evidence_ref, account_scope=value.account_scope,
                                    quantity=selection.quantity)
    rejection = () if status is DecisionStatus.TRADE_CANDIDATE else (reason,)
    evidence = EvaluationEvidence(market_status, tuple([market_reason] if market_reason else []), event_status,
                                  tuple([event_reason] if event_reason else []), value.model_manifest_hash,
                                  value.block_manifest_hash, value.scenario_config_hash,
                                  value.scenario_support.selected_block, value.scenario_support.paths, value.bootstrap,
                                  portfolio, sized, rejection, (), risk_decision.reasons)
    return Phase4Evaluation(status, b0, a0, evidence, plan, selection.quantity, selection.risk, risk_decision, reason)
