"""Complete pure offline A0/B0 orchestration for the frozen Phase-4 policy.

Frozen decision order (deterministic, fail-closed)::

    SKIP_DATA -> gate (NO_TRADE_GATE / NO_TRADE_EVENT / NOT_ESTIMABLE)
    -> NO_SIGNAL -> NOT_ESTIMABLE(scenario/stress/uncertainty/evidence conflict)
    -> NO_TRADE_RISK -> NO_TRADE_NUMERICAL -> NO_TRADE_NO_EDGE -> TRADE_CANDIDATE

Capital control is evidence-bound, not caller-declared:

* per-unit normal/stress/notional/margin quantities are derived by this module;
* the actual stress suite is re-evaluated at the SELECTED quantity (a stress
  template built for another quantity can never authorise it);
* the portfolio ES hard limit uses ``ES_after`` from the common-path
  calculation, never a caller-supplied contribution;
* every scenario artifact must prove it belongs to this snapshot, direction,
  policy, RiskPolicy, quantity, model/block/scenario identity and path set.

Risk quantity is solved once against the deterministic hard constraints; the
statistical A0 evaluation then runs *once* at that quantity.  Neither direction
nor size is ever optimised against a noisy LCB.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
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

UNIT_QUANTITY = Decimal("1")


def canonical_hash(value: object) -> str:
    def default(item: object) -> object:
        if hasattr(item, "value"):
            return item.value  # type: ignore[attr-defined]
        if hasattr(item, "__dataclass_fields__"):
            return {name: getattr(item, name) for name in item.__dataclass_fields__}  # type: ignore[attr-defined]
        return str(item)

    return hashlib.sha256(json.dumps(value, default=default, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def phase4_action_hash(policy: FixedPolicy, snapshot: FeatureSnapshot, quantity: Decimal) -> str:
    """Immutable identity of the frozen action the evidence must belong to."""
    return canonical_hash({"strategy": "CRYPTO_TREND_24H_V1", "instrument": snapshot.instrument,
                           "slot": snapshot.slot_at_ns, "snapshot": snapshot.snapshot_hash(),
                           "side": policy.side.value, "quantity": str(quantity), "entry_collar": str(policy.entry_collar),
                           "stop": str(policy.stop), "stop_basis": policy.stop_trigger_basis,
                           "management": policy.management_policy, "horizon_end_ns": policy.horizon_end_ns,
                           "created_at_ns": policy.created_at_ns})


def path_hash(paths: tuple[float, ...]) -> str:
    return canonical_hash(list(paths))


def seed_identity(support: ScenarioSupport) -> str:
    return f"block={support.selected_block};paths={support.paths};seed={support.seed}"


@dataclass(frozen=True)
class ScenarioSupport:
    """Support verdict for the sampled joint block/scenario construction."""

    estimable: bool
    reasons: tuple[str, ...] = ()
    paths: int = 0
    selected_block: int | None = None
    seed: int = 0


@dataclass(frozen=True)
class CandidateRiskInputs:
    """Deterministic, scalable per-unit risk inputs supplied as venue evidence."""

    beta: Decimal
    taker_fee_rate: Decimal
    maintenance_margin_per_unit: Decimal
    venue_collateral: Decimal
    adverse_funding_reserve_per_unit: Decimal = Decimal("0")

    def evidence_hash(self) -> str:
        return canonical_hash({name: str(getattr(self, name)) for name in self.__dataclass_fields__})


def candidate_risk_inputs_hash(risk_inputs: CandidateRiskInputs) -> str:
    return risk_inputs.evidence_hash()


@dataclass(frozen=True)
class Phase4ScenarioEvaluation:
    """Immutable binding contract: scenario evidence for one frozen action."""

    snapshot_hash: str
    action_hash: str
    quantity: Decimal
    risk_policy_hash: str
    model_manifest_hash: str
    block_manifest_hash: str
    scenario_config_hash: str
    seed_identity: str
    candidate_path_hash: str
    portfolio_path_hash: str
    risk_inputs_hash: str
    bootstrap: OuterBootstrapResult
    pi0_path_pnl: tuple[float, ...]
    candidate_path_pnl: tuple[float, ...]
    scenario_support: ScenarioSupport
    stress_template: StressInput

    def conflicts(self) -> tuple[str, ...]:
        problems: list[str] = []
        if self.quantity <= 0:
            problems.append("scenario quantity must be positive")
        if self.candidate_path_hash != path_hash(self.candidate_path_pnl):
            problems.append("candidate path hash does not match its paths")
        if self.portfolio_path_hash != path_hash(self.pi0_path_pnl):
            problems.append("portfolio path hash does not match its paths")
        if len(self.pi0_path_pnl) != len(self.candidate_path_pnl):
            problems.append("portfolio and candidate paths are not common paths")
        if self.bootstrap.action_hash != self.action_hash:
            problems.append("bootstrap belongs to a different action")
        if self.seed_identity != seed_identity(self.scenario_support):
            problems.append("scenario seed/config identity mismatch")
        return tuple(problems)


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
    action_hash: str = ""
    quantity: Decimal = Decimal("0")
    stress_loss: Decimal = Decimal("0")

    def lcb(self) -> float | None:
        return self.bootstrap.lcb if self.bootstrap is not None else None

    def artifact_hash(self) -> str:
        return canonical_hash(self)


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
    scenario: Phase4ScenarioEvaluation | None
    risk_inputs: CandidateRiskInputs | None
    model_manifest_hash: str
    block_manifest_hash: str
    scenario_config_hash: str
    venue_maximum_quantity: Decimal
    lot: Decimal
    minimum_quantity: Decimal
    leverage: Decimal
    cost_evidence_ref: str
    account_scope: str
    plan_id: str
    availability_cutoff_ns: int
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


def unit_per_unit_risk(*, policy: FixedPolicy, stress: StressResult, beta: Decimal,
                       margin_per_unit: Decimal, es_contribution: Decimal,
                       venue_collateral: Decimal, taker_fee_rate: Decimal,
                       adverse_funding_reserve_per_unit: Decimal = Decimal("0")) -> PerUnitRisk:
    """Per-unit hard-risk vector; the stress loss is the frozen suite bound at unit size."""
    stop_loss = abs(policy.mark_reference - policy.stop)
    normal = stop_loss + policy.mark_reference * taker_fee_rate * Decimal("2") + adverse_funding_reserve_per_unit
    stress_loss = stress.trade_loss if stress.trade_loss is not None else Decimal("0")
    stress_loss += stress.liquidation_cost or Decimal("0")
    return PerUnitRisk(normal, stress_loss, policy.mark_reference, policy.mark_reference * beta,
                       margin_per_unit, es_contribution, venue_collateral)


def _unit_stress(template: StressInput) -> tuple[StressResult, ...]:
    return evaluate_stress_suite(replace(template, quantity=UNIT_QUANTITY))


def derive_per_unit_risk(value: Phase4DecisionInput, stress_template: StressInput) -> PerUnitRisk:
    """Derive scalable per-unit risk from evidence; never from caller declarations."""
    assert value.policy is not None and value.risk_inputs is not None
    unit = _unit_stress(stress_template)
    if not suite_is_estimable(unit):
        raise ValueError("NOT_ESTIMABLE: stress suite cannot be valued at unit size")
    worst = max(unit, key=lambda result: (result.trade_loss or Decimal("0")) + (result.liquidation_cost or Decimal("0")))
    return unit_per_unit_risk(policy=value.policy, stress=worst, beta=value.risk_inputs.beta,
                              margin_per_unit=value.risk_inputs.maintenance_margin_per_unit,
                              es_contribution=Decimal("0"), venue_collateral=Decimal("0"),
                              taker_fee_rate=value.risk_inputs.taker_fee_rate,
                              adverse_funding_reserve_per_unit=value.risk_inputs.adverse_funding_reserve_per_unit)


def size_phase4_quantity(value: Phase4DecisionInput, per_unit: PerUnitRisk) -> QuantitySelection:
    """Largest hard-constraint quantity, before any statistical acceptance."""
    if value.policy is None:
        return QuantitySelection(None, None, "missing frozen policy")
    maximum = min(value.venue_maximum_quantity, value.policy.quantity)
    return deterministic_risk_quantity(policy=value.risk_policy, account=value.account,
                                       pending=value.pending_reservations, per_unit=per_unit,
                                       venue_maximum=maximum, lot=value.lot,
                                       minimum=value.minimum_quantity, leverage=value.leverage)


def _final_risk_vector(*, per_unit: PerUnitRisk, quantity: Decimal, actual_stress_loss: Decimal) -> RiskVector:
    vector = per_unit.vector(quantity)
    return replace(vector, stress_loss=actual_stress_loss, venue_collateral=Decimal("0"), es_contribution=Decimal("0"))


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
               risk_decision: RiskDecision | None = None, portfolio: PortfolioDecision | None = None,
               stress_results: tuple[StressResult, ...] = (), stress_loss: Decimal = Decimal("0"),
               action_hash: str = "") -> Phase4Evaluation:
        signal = value.snapshot.signal if value.snapshot is not None else Signal.FLAT
        fallback_b0 = EvaluationResult("B0", status, reason, signal)
        fallback_a0 = EvaluationResult("A0", status, reason, signal)
        rejection = () if status is DecisionStatus.TRADE_CANDIDATE else (reason,)
        evidence = EvaluationEvidence(market_status, tuple([market_reason] if market_reason else []), event_status,
                                      tuple([event_reason] if event_reason else []), value.model_manifest_hash,
                                      value.block_manifest_hash, value.scenario_config_hash,
                                      value.scenario.scenario_support.selected_block if value.scenario else None,
                                      value.scenario.scenario_support.paths if value.scenario else 0,
                                      value.scenario.bootstrap if value.scenario else None, portfolio, stress_results,
                                      rejection, tuple(dict.fromkeys(not_estimable)),
                                      risk_decision.reasons if risk_decision is not None else (), action_hash,
                                      quantity or Decimal("0"), stress_loss)
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
    if value.scenario is None or value.risk_inputs is None:
        not_estimable.append("missing scenario or risk evidence artifact")
        return finish(DecisionStatus.NOT_ESTIMABLE, not_estimable[0])

    # §6 evidence binding: the artifact must belong to this snapshot, direction,
    # policy, RiskPolicy and manifest identity before any quantity is trusted.
    scenario = value.scenario
    snapshot_hash = value.snapshot.snapshot_hash()
    conflicts = list(scenario.conflicts())
    if scenario.snapshot_hash != snapshot_hash:
        conflicts.append("scenario belongs to a different snapshot")
    if scenario.risk_policy_hash != value.risk_policy.policy_hash():
        conflicts.append("scenario belongs to a different RiskPolicy")
    if scenario.model_manifest_hash != value.model_manifest_hash:
        conflicts.append("scenario belongs to a different model manifest")
    if scenario.block_manifest_hash != value.block_manifest_hash:
        conflicts.append("scenario belongs to a different block manifest")
    if scenario.scenario_config_hash != value.scenario_config_hash:
        conflicts.append("scenario belongs to a different scenario configuration")
    if scenario.risk_inputs_hash != candidate_risk_inputs_hash(value.risk_inputs):
        conflicts.append("scenario belongs to different candidate risk inputs")
    if scenario.stress_template.side is not value.policy.side:
        conflicts.append("stress evidence belongs to the opposite direction")
    if scenario.stress_template.current_mark != value.policy.mark_reference:
        conflicts.append("stress evidence belongs to a different mark reference")
    if scenario.stress_template.quantity != scenario.quantity:
        conflicts.append("stress evidence quantity does not match the bound quantity")
    if conflicts:
        not_estimable.extend(conflicts)
        return finish(DecisionStatus.NOT_ESTIMABLE, conflicts[0])

    support = scenario.scenario_support
    if not support.estimable:
        not_estimable.extend(support.reasons)
    unit_stress = _unit_stress(scenario.stress_template)
    if not suite_is_estimable(unit_stress):
        not_estimable.extend(result.reason for result in unit_stress if result.status is StressStatus.NOT_ESTIMABLE)
    if not_estimable:
        return finish(DecisionStatus.NOT_ESTIMABLE, not_estimable[0])
    # The frozen action identity is fixed before sizing; the artifact must match it.
    action_hash = phase4_action_hash(value.policy, value.snapshot, scenario.quantity)
    if scenario.action_hash != action_hash:
        return finish(DecisionStatus.NOT_ESTIMABLE, "scenario belongs to a different action", action_hash=action_hash)

    per_unit = derive_per_unit_risk(value, scenario.stress_template)
    selection = size_phase4_quantity(value, per_unit)
    if selection.quantity is None or selection.risk is None:
        return finish(DecisionStatus.NO_TRADE_RISK, selection.reason, action_hash=action_hash)
    quantity = selection.quantity
    if scenario.quantity != quantity:
        return finish(DecisionStatus.NOT_ESTIMABLE, "scenario evidence is not bound to the selected quantity",
                      quantity=quantity, action_hash=action_hash)

    # §5 hard risk: the ACTUAL stress suite at the selected quantity decides.
    sized_stress = evaluate_stress_suite(replace(scenario.stress_template, quantity=quantity))
    if not suite_is_estimable(sized_stress):
        not_estimable.extend(result.reason for result in sized_stress if result.status is StressStatus.NOT_ESTIMABLE)
        return finish(DecisionStatus.NOT_ESTIMABLE, not_estimable[0], quantity=quantity, stress_results=sized_stress,
                      action_hash=action_hash)
    actual_stress_loss = max_trade_stress_loss(sized_stress) + max_liquidation_cost(sized_stress)
    final_risk = _final_risk_vector(per_unit=per_unit, quantity=quantity, actual_stress_loss=actual_stress_loss)
    stress_limit_breach = any(result.liquidated and not result.case.venue_collateral_loss for result in sized_stress)
    risk_decision = evaluate_reservation(value.risk_policy, value.account, value.pending_reservations, final_risk,
                                         leverage=value.leverage)
    if stress_limit_breach:
        risk_decision = RiskDecision(False, ("stress liquidation mechanics breach",), risk_decision.scaled_normal_budget)
    venue_total = value.account.venue_collateral + value.risk_inputs.venue_collateral
    if venue_total > value.risk_policy.venue_collateral_limit * value.account.eligible_equity:
        risk_decision = RiskDecision(False, ("venue-collateral",), risk_decision.scaled_normal_budget)
    if not risk_decision.accepted:
        reason = risk_decision.reasons[0] if risk_decision.reasons else "hard risk"
        return finish(DecisionStatus.NO_TRADE_RISK, reason, quantity=quantity, risk=final_risk,
                      risk_decision=risk_decision, stress_results=sized_stress, stress_loss=actual_stress_loss,
                      action_hash=action_hash)

    lcb = scenario.bootstrap.lcb
    portfolio = common_path_portfolio_decision(lcb, float(value.account.eligible_equity), scenario.pi0_path_pnl,
                                               scenario.candidate_path_pnl,
                                               float(value.risk_policy.portfolio_es_alpha))
    # ES_after is an equity fraction, so the policy limit is compared in the same units.
    es_limit = value.risk_policy.portfolio_es_limit_frac * value.risk_policy.scaling_at(value.account.drawdown)
    if portfolio.es_after > float(es_limit):
        risk_decision = RiskDecision(False, ("portfolio-es-after",), risk_decision.scaled_normal_budget)
        return finish(DecisionStatus.NO_TRADE_RISK, "portfolio-es-after", quantity=quantity, risk=final_risk,
                      risk_decision=risk_decision, portfolio=portfolio, stress_results=sized_stress,
                      stress_loss=actual_stress_loss, action_hash=action_hash)

    b0 = EvaluationResult("B0", DecisionStatus.TRADE_CANDIDATE, "raw frozen trend policy", value.snapshot.signal)
    _, a0, _ = evaluate_actions_and_portfolio(signal=value.snapshot.signal, gates_ok=True, scenario_support_ok=True,
                                              hard_risk_ok=True, a0_lcb=lcb, a0_j=portfolio.j)
    status = a0.status
    reason = a0.reason
    if status is DecisionStatus.TRADE_CANDIDATE and scenario.bootstrap.numerical_unstable:
        status = DecisionStatus.NO_TRADE_NUMERICAL
        reason = "independent inner seeds flip qualification"
        a0 = EvaluationResult("A0", status, reason, value.snapshot.signal, lcb, portfolio.j)
    plan = None
    if status is DecisionStatus.TRADE_CANDIDATE:
        plan = candidate_trade_plan(plan_id=value.plan_id, snapshot=value.snapshot, policy=value.policy,
                                    policy_hash=value.risk_policy.policy_hash(),
                                    normal_risk=final_risk.normal_loss, stress_risk=actual_stress_loss,
                                    margin=final_risk.margin, leverage=value.leverage,
                                    cost_evidence_ref=value.cost_evidence_ref, account_scope=value.account_scope,
                                    quantity=quantity)
    rejection = () if status is DecisionStatus.TRADE_CANDIDATE else (reason,)
    evidence = EvaluationEvidence(market_status, tuple([market_reason] if market_reason else []), event_status,
                                  tuple([event_reason] if event_reason else []), value.model_manifest_hash,
                                  value.block_manifest_hash, value.scenario_config_hash, support.selected_block,
                                  support.paths, scenario.bootstrap, portfolio, sized_stress, rejection, (),
                                  risk_decision.reasons, action_hash, quantity, actual_stress_loss)
    return Phase4Evaluation(status, b0, a0, evidence, plan, quantity, final_risk, risk_decision, reason)
