"""Explicit caller-owned Session-019/Phase-2 economic evaluation seam."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json
from ..contracts import CandidateActionV2, CandidateSetV2
from ..instruments import ProductContractV2
from ..memory.repository import OpsRepository
from ..risk import AccountRiskSnapshotV2, FeeScheduleV2, RiskPolicyV2, SizingDecisionV2, SizingStatus
from .action import ActionArtifactV2
from .admission import (
    ADMISSION_POLICY_VERSION,
    AdmissionPolicyV2,
    AdmissionResultV2,
    AmendedEvaluationArtifactV2,
    DecisionTimePortfolioCompletenessV2,
    DecisionTimePortfolioScenariosV2,
    DeterministicStressV2,
    EstimationUncertaintyV2,
    ExecutionModelUncertaintyV2,
    InferenceSupportV2,
    LCBMethodV2,
    M0CalibrationV2,
    M0SupportV2,
    NumericalErrorV2,
    OutcomeDistributionV2,
    PortfolioESV2,
    PretradeExecutionScenarioV2,
    PretradePathPayoffV2,
    ScenarioSupportV2,
    VenueCapabilitySnapshotV2,
    build_decision_time_portfolio_scenarios,
    decide_admission,
    evaluate_deterministic_stress,
    index_admission_evidence,
    index_lcb_method,
    index_portfolio_completeness,
    index_venue_capability_snapshot,
    make_amended_evaluation,
    make_estimation_uncertainty,
    make_execution_uncertainty,
    make_inference_support,
    make_numerical_error,
    make_outcome_distribution,
    make_portfolio_es,
    make_scenario_support,
    persist_economic_decision,
)
from .m0 import M0OODV2, M0PredictionV2, fit_m0
from .pretrade import CausalInputV2
from .scenario_engine import generate_pretrade_scenarios


@dataclass(frozen=True)
class Phase2EvaluationResultV2:
    prediction: M0PredictionV2
    m0_support: M0SupportV2
    calibration: M0CalibrationV2
    ood: M0OODV2
    scenario: PretradeExecutionScenarioV2
    payoffs: tuple[PretradePathPayoffV2, ...]
    scenario_support: ScenarioSupportV2
    inference_support: InferenceSupportV2
    distribution: OutcomeDistributionV2
    estimation: EstimationUncertaintyV2
    execution: ExecutionModelUncertaintyV2
    numerical: NumericalErrorV2
    stress: DeterministicStressV2
    completeness: DecisionTimePortfolioCompletenessV2
    portfolio: DecisionTimePortfolioScenariosV2
    portfolio_es: PortfolioESV2
    admission: AdmissionResultV2
    admission_policy: AdmissionPolicyV2
    lcb_method_ref: str
    capability_ref: str
    evaluation: AmendedEvaluationArtifactV2
    evaluation_ref: str
    calendar_ref: str


def run_phase2_economic_evaluation(
    repository: OpsRepository,
    *,
    action: ActionArtifactV2,
    candidate: CandidateActionV2,
    candidate_set: CandidateSetV2,
    sizing: SizingDecisionV2,
    product: ProductContractV2,
    risk_policy: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    account: AccountRiskSnapshotV2,
    fee: FeeScheduleV2,
    admission_policy: AdmissionPolicyV2,
    capability: VenueCapabilitySnapshotV2,
    model_input: CausalInputV2,
    calibration_input: CausalInputV2,
    execution_model_input: CausalInputV2,
    available_at_ns: int,
    scenario_seed: int,
    scenario_count: int = 100,
) -> Phase2EvaluationResultV2:
    """Run M0 through terminal economic calendar using the supplied ops writer.

    Hard-risk sizing and exact action freezing happen before this seam. The
    indexed sizing/action/candidate graph is verified here before any economic
    evaluation begins. This service never opens an OpsRepository itself.
    """
    if repository.read_only:
        raise ValueError("Phase-2 economic evaluation requires the caller's writable OpsRepository")
    if admission_policy.version != ADMISSION_POLICY_VERSION:
        raise ValueError("unsupported Phase-2 admission policy version")
    if (
        candidate_set.selected_candidate_id != candidate.candidate_id
        or action.candidate_ref != candidate.content_hash
        or action.candidate_set_ref != candidate_set.content_hash
        or action.action.key != candidate.key
        or action.action.product_ref != product.content_hash
        or action.sizing_ref != sizing.content_hash
        or sizing.status != SizingStatus.SIZED
        or sizing.quantity is None
        or action.action.quantity != sizing.quantity
        or sizing.candidate_ref != candidate.content_hash
        or sizing.candidate_set_ref != candidate_set.content_hash
        or sizing.risk_policy_hash != risk_policy.policy_hash()
        or sizing.risk_policy_v2_hash != risk_policy_v2.policy_hash
    ):
        raise ValueError("hard-risk sizing and frozen action are not an exact selected candidate binding")
    sizing_entry = repository.get_artifact(sizing.content_hash)
    if (
        sizing_entry is None
        or sizing_entry.artifact_type != "SizingDecisionV2"
        or canonical_json(sizing_entry.metadata.get("sizing")) != canonical_json(sizing.to_dict())
    ):
        raise ValueError("exact hard-risk sizing evidence must be persisted before economic evaluation")
    if available_at_ns >= candidate.deadline_ns:
        raise ValueError("economic evaluation must finish before the selected action deadline")

    _, prediction, m0_support, calibration, _ = fit_m0(
        repository,
        action=action,
        candidate=candidate,
        candidate_set=candidate_set,
        cutoff_ns=candidate.decision_at_ns,
        available_at_ns=available_at_ns,
    )
    scenario, payoffs = generate_pretrade_scenarios(
        repository,
        action=action,
        model_input=model_input,
        calibration_input=calibration_input,
        execution_model_input=execution_model_input,
        source_inputs=(),
        joint_data_refs=(),
        fee=fee,
        base_units_per_contract=product.base_units_per_contract,
        cutoff_ns=candidate.decision_at_ns,
        created_at_ns=available_at_ns - 2,
        computed_at_ns=available_at_ns - 1,
        available_at_ns=available_at_ns,
        expires_at_ns=candidate.deadline_ns,
        seed=scenario_seed,
        scenario_count=scenario_count,
    )

    scenario_support = make_scenario_support(repository, action=action, scenario=scenario, support_unit_refs=())
    index_admission_evidence(repository, "ScenarioSupportV2", scenario_support.to_dict(), available_at_ns)
    support = make_inference_support(action.action.action_hash, m0_support, scenario_support)
    index_admission_evidence(repository, "InferenceSupportV2", support.to_dict(), available_at_ns)
    distribution = make_outcome_distribution(scenario, payoffs)
    distribution_ref = index_admission_evidence(
        repository, "OutcomeDistributionV2", distribution.to_dict(), available_at_ns
    )

    estimation = make_estimation_uncertainty(
        prediction,
        training_refs=m0_support.training_outcome_refs,
        confidence_multiplier=admission_policy.confidence_multiplier,
    )
    estimation_ref = index_admission_evidence(
        repository, "EstimationUncertaintyV2", estimation.to_dict(), available_at_ns
    )
    execution = make_execution_uncertainty(
        action.action.action_hash,
        execution_model_input.ref,
        (),
        compatibility_key=m0_support.compatibility_key,
        cutoff_ns=candidate.decision_at_ns,
        minimum_support=admission_policy.minimum_execution_calibration,
    )
    execution_ref = index_admission_evidence(
        repository, "ExecutionModelUncertaintyV2", execution.to_dict(), available_at_ns
    )
    numerical = make_numerical_error(repository, action=action, scenario=scenario, prediction=prediction)
    numerical_ref = index_admission_evidence(repository, "NumericalErrorV2", numerical.to_dict(), available_at_ns)
    stress = evaluate_deterministic_stress(
        repository,
        action=action,
        risk_policy=risk_policy,
        risk_policy_ref=risk_policy.policy_hash(),
        eligible_equity=account.eligible_equity,
        drawdown=account.drawdown,
        product_base_units=product.base_units_per_contract,
        stress_input=None,
        stress_evidence_ref=None,
        cutoff_ns=candidate.decision_at_ns,
    )
    index_admission_evidence(repository, "DeterministicStressV2", stress.to_dict(), available_at_ns)

    completeness = DecisionTimePortfolioCompletenessV2(
        account.content_hash,
        scenario.common_scenario_set_id,
        tuple(sorted(account.existing_exposure_refs)),
        tuple(sorted(account.pending_risk_refs)),
        (),
        (),
        account.eligible_equity,
        account.drawdown,
        candidate.decision_at_ns,
        "NOT_ESTIMABLE",
    )
    index_portfolio_completeness(repository, completeness)
    portfolio = build_decision_time_portfolio_scenarios(
        action=action,
        repo=repository,
        scenario=scenario,
        payoffs=payoffs,
        existing_paths=(),
        completeness=completeness,
    )
    index_admission_evidence(repository, "DecisionTimePortfolioScenariosV2", portfolio.to_dict(), available_at_ns)
    portfolio_es = make_portfolio_es(portfolio, risk_policy=risk_policy, risk_policy_ref=risk_policy.policy_hash())
    index_admission_evidence(repository, "PortfolioESV2", portfolio_es.to_dict(), available_at_ns)

    capability_ref = index_venue_capability_snapshot(repository, capability)
    lcb_method_ref = index_lcb_method(repository, LCBMethodV2(), available_at_ns=available_at_ns)
    admission_policy_ref = index_admission_evidence(
        repository,
        "AdmissionPolicyV2",
        admission_policy.to_dict(),
        available_at_ns,
    )
    ood_entry = repository.get_artifact(prediction.ood_ref)
    ood_body = ood_entry.metadata.get("ood") if ood_entry is not None else None
    if not isinstance(ood_body, Mapping):
        raise ValueError("M0 OOD evidence is missing from the persisted prediction graph")
    ood_wire = dict(ood_body)
    if ood_wire.pop("version", None) != "M0_ROBUST_OOD_V1":
        raise ValueError("unsupported M0 OOD evidence")
    ood = M0OODV2(
        ood_wire["action_hash"],
        ood_wire["feature_vector_ref"],
        tuple(ood_wire["training_row_refs"]),
        Decimal(ood_wire["robust_z_limit"]),
        Decimal(ood_wire["maximum_absolute_robust_z"]) if ood_wire["maximum_absolute_robust_z"] is not None else None,
        ood_wire["out_of_distribution"],
        ood_wire["status"],
    )
    if (
        ood_entry is None
        or ood_entry.artifact_type != "M0OODV2"
        or ood_entry.content_hash != prediction.ood_ref
        or ood.content_hash != prediction.ood_ref
        or canonical_json(ood_entry.metadata.get("ood")) != canonical_json(ood.to_dict())
    ):
        raise ValueError("M0 OOD evidence identity does not match the persisted prediction graph")
    admission = decide_admission(
        action=action,
        prediction=prediction,
        m0_support=m0_support,
        calibration=calibration,
        ood=ood,
        scenario=scenario,
        scenario_support=scenario_support,
        outcome_distribution=distribution,
        estimation=estimation,
        execution=execution,
        numerical=numerical,
        stress=stress,
        portfolio=portfolio_es,
        policy=admission_policy,
        capability=capability,
        account_scope=account.account_scope,
    )
    evaluation = make_amended_evaluation(
        action=action,
        candidate=candidate,
        candidate_set=candidate_set,
        prediction=prediction,
        scenario=scenario,
        stress=stress,
        portfolio=portfolio,
        portfolio_es=portfolio_es,
        support=support,
        estimation_ref=estimation_ref,
        execution_ref=execution_ref,
        numerical_ref=numerical_ref,
        calibration_ref=prediction.calibration_ref,
        ood_ref=prediction.ood_ref,
        outcome_distribution_ref=distribution_ref,
        admission_policy_ref=admission_policy_ref,
        capability_evidence_ref=capability_ref,
        lcb_method_ref=lcb_method_ref,
        account_snapshot_ref=account.content_hash,
        risk_policy_ref=risk_policy.policy_hash(),
        risk_policy_v2_ref=risk_policy_v2.policy_hash,
        causal_state_ref=candidate.snapshot_hash,
        available_at_ns=available_at_ns,
        result=admission,
    )
    evaluation_ref, calendar_ref = persist_economic_decision(
        repository,
        evaluation,
        policy_id=action.action.policy_id,
        policy_version=action.action.policy_version,
        created_at_ns=available_at_ns,
    )
    return Phase2EvaluationResultV2(
        prediction,
        m0_support,
        calibration,
        ood,
        scenario,
        tuple(payoffs),
        scenario_support,
        support,
        distribution,
        estimation,
        execution,
        numerical,
        stress,
        completeness,
        portfolio,
        portfolio_es,
        admission,
        admission_policy,
        lcb_method_ref,
        capability_ref,
        evaluation,
        evaluation_ref,
        calendar_ref,
    )
