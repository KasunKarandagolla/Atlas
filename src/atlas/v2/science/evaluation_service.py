"""Explicit caller-owned Session-019/Phase-2 economic evaluation seam."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json, json_value, sha256_json, timestamp
from ..chronology import record_computation
from ..contracts import CandidateActionV2, CandidateSetV2
from ..instruments import ProductContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
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
    ExecutionCalibrationResidualV2,
    ExecutionModelUncertaintyV2,
    ExistingPortfolioPathV2,
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
    index_execution_calibration_residual,
    index_lcb_method,
    index_numerical_convergence_run,
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
    stress_input_from_wire,
)
from .admission import (
    index_admission_evidence as _index_admission_evidence,
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
    clock_ns: Callable[[], int] | None = None,
    source_inputs: Sequence[CausalInputV2] = (),
    joint_data_refs: Sequence[str] = (),
    support_unit_refs: Sequence[str] = (),
    execution_residual_refs: Sequence[str] = (),
    stress_input_ref: str | None = None,
    existing_portfolio_path_refs: Sequence[str] = (),
) -> Phase2EvaluationResultV2:
    """Run M0 through terminal economic calendar using the supplied ops writer.

    Hard-risk sizing and exact action freezing happen before this seam. The
    indexed sizing/action/candidate graph is verified here before any economic
    evaluation begins. This service never opens an OpsRepository itself.
    """
    if repository.read_only:
        raise ValueError("Phase-2 economic evaluation requires the caller's writable OpsRepository")
    supplied_refs = (*joint_data_refs, *support_unit_refs, *execution_residual_refs,
        *existing_portfolio_path_refs, *(item.ref for item in source_inputs))
    if len(supplied_refs) + (stress_input_ref is not None) > 128:
        raise ValueError("economic evaluation source population exceeds its declared bound")
    for refs in (joint_data_refs, support_unit_refs, execution_residual_refs, existing_portfolio_path_refs):
        if tuple(refs) != tuple(sorted(set(refs))):
            raise ValueError("economic evaluation source refs must be sorted and unique")
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

    def sample_time() -> int:
        nonlocal available_at_ns
        if clock_ns is not None:
            observed = timestamp(clock_ns(), field="economic computation clock")
            if observed < available_at_ns:
                raise ValueError("economic clock regressed during computation")
            available_at_ns = observed
        if available_at_ns >= candidate.deadline_ns:
            raise ValueError("economic computation missed the decision deadline")
        return available_at_ns

    def index_admission_evidence(
        repo: OpsRepository, kind: str, body: Mapping[str, object], supplied_time: int,
    ) -> str:
        # Supplied time is the latest stage boundary in explicit offline callers;
        # production samples the clock after the body has been computed.
        if supplied_time > available_at_ns:
            raise ValueError("economic evidence cannot be published from a future stage")
        at_ns = sample_time()
        ref = sha256_json(body)
        existing = repo.get_artifact(ref)
        if existing is not None:
            if (existing.artifact_type != kind or existing.content_hash != ref
                    or canonical_json(existing.metadata) != canonical_json({"evidence": body})
                    or existing.available_at_ns > at_ns):
                raise ValueError("economic immutable artifact conflicts with an existing publication")
            return ref
        return _index_admission_evidence(repo, kind, body, at_ns)

    sample_time()

    _, prediction, m0_support, calibration, _ = fit_m0(
        repository,
        action=action,
        candidate=candidate,
        candidate_set=candidate_set,
        cutoff_ns=candidate.decision_at_ns,
        available_at_ns=available_at_ns,
        clock_ns=clock_ns,
    )
    available_at_ns = prediction.available_at_ns
    sample_time()
    scenario, payoffs = generate_pretrade_scenarios(
        repository,
        action=action,
        model_input=model_input,
        calibration_input=calibration_input,
        execution_model_input=execution_model_input,
        source_inputs=source_inputs,
        joint_data_refs=joint_data_refs,
        fee=fee,
        base_units_per_contract=product.base_units_per_contract,
        cutoff_ns=candidate.decision_at_ns,
        created_at_ns=available_at_ns,
        computed_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        expires_at_ns=candidate.deadline_ns,
        seed=scenario_seed,
        scenario_count=scenario_count,
        clock_ns=clock_ns,
    )
    available_at_ns = scenario.available_at_ns

    scenario_support = make_scenario_support(repository, action=action, scenario=scenario,
        support_unit_refs=support_unit_refs)
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
    residuals: list[ExecutionCalibrationResidualV2] = []
    for ref in execution_residual_refs:
        entry = repository.get_artifact(ref)
        body = entry.metadata.get("residual") if entry is not None else None
        if (entry is None or entry.artifact_type != "ExecutionCalibrationResidualV2"
                or entry.content_hash != ref or not isinstance(body, Mapping)
                or sha256_json(body) != ref or entry.available_at_ns > candidate.decision_at_ns):
            raise ValueError("economic execution calibration is missing, future or invalid")
        residual = ExecutionCalibrationResidualV2.from_dict(json_value(body), evidence_ref=ref)
        if residual.available_at_ns != entry.available_at_ns:
            raise ValueError("economic execution calibration publication mismatch")
        index_execution_calibration_residual(repository, residual)
        residuals.append(residual)
    execution = make_execution_uncertainty(
        action.action.action_hash,
        execution_model_input.ref,
        residuals,
        compatibility_key=m0_support.compatibility_key,
        cutoff_ns=candidate.decision_at_ns,
        minimum_support=admission_policy.minimum_execution_calibration,
    )
    execution_ref = index_admission_evidence(
        repository, "ExecutionModelUncertaintyV2", execution.to_dict(), available_at_ns
    )
    # Two fixed independent seeds diagnose Monte Carlo error; they are not new
    # historical observations or hidden attempts to rescue an unsupported model.
    run_refs: list[str] = []
    if scenario.status.value == "AVAILABLE" and not scenario.synthetic_fixture and prediction.numerical_conversion_error is not None:
        run_refs.append(index_numerical_convergence_run(repository, action=action, scenario_ref=scenario.content_hash))
        sample_time()
        alternate, _ = generate_pretrade_scenarios(repository, action=action,
            model_input=model_input, calibration_input=calibration_input,
            execution_model_input=execution_model_input, source_inputs=source_inputs,
            joint_data_refs=joint_data_refs, fee=fee, base_units_per_contract=product.base_units_per_contract,
            cutoff_ns=candidate.decision_at_ns, created_at_ns=available_at_ns, computed_at_ns=available_at_ns,
            available_at_ns=available_at_ns, expires_at_ns=candidate.deadline_ns,
            seed=scenario_seed ^ 0xA71A537, scenario_count=scenario_count, clock_ns=clock_ns)
        available_at_ns = alternate.available_at_ns
        run_refs.append(index_numerical_convergence_run(repository, action=action, scenario_ref=alternate.content_hash))
    numerical = make_numerical_error(repository, action=action, scenario=scenario, prediction=prediction,
        run_a_ref=run_refs[0] if run_refs else None, run_b_ref=run_refs[1] if run_refs else None)
    numerical_ref = index_admission_evidence(repository, "NumericalErrorV2", numerical.to_dict(), available_at_ns)
    stress_input = None
    if stress_input_ref is not None:
        entry = repository.get_artifact(stress_input_ref)
        if (entry is None or entry.artifact_type != "StressSuiteEvidenceV2"
                or entry.content_hash != stress_input_ref or sha256_json(entry.metadata) != stress_input_ref
                or entry.available_at_ns > candidate.decision_at_ns):
            raise ValueError("economic stress source is missing, future or invalid")
        stress_input = stress_input_from_wire(json_value(entry.metadata["stress_input"]))
    stress = evaluate_deterministic_stress(
        repository,
        action=action,
        risk_policy=risk_policy,
        risk_policy_ref=risk_policy.policy_hash(),
        eligible_equity=account.eligible_equity,
        drawdown=account.drawdown,
        product_base_units=product.base_units_per_contract,
        stress_input=stress_input,
        stress_evidence_ref=stress_input_ref,
        cutoff_ns=candidate.decision_at_ns,
    )
    index_admission_evidence(repository, "DeterministicStressV2", stress.to_dict(), available_at_ns)

    existing_paths: list[ExistingPortfolioPathV2] = []
    for ref in existing_portfolio_path_refs:
        entry = repository.get_artifact(ref)
        body = entry.metadata.get("evidence") if entry is not None else None
        if (entry is None or entry.artifact_type != "ExistingPortfolioPathV2" or entry.content_hash != ref
                or not isinstance(body, Mapping) or sha256_json(body) != ref
                or entry.available_at_ns > candidate.decision_at_ns):
            raise ValueError("economic portfolio source is missing, future or invalid")
        existing_paths.append(ExistingPortfolioPathV2.from_dict(json_value(body)))
    existing_paths.sort(key=lambda item: item.common_path_id)
    flat = not account.existing_exposure_refs and not account.pending_risk_refs
    if flat and scenario.status.value == "AVAILABLE" and not existing_paths:
        existing_paths = [ExistingPortfolioPathV2(path_id, scenario.common_scenario_set_id, probability, Decimal(0))
            for path_id, probability, _ in scenario.rows]
    completeness_started = sample_time()
    completeness_available = sample_time()
    completeness = DecisionTimePortfolioCompletenessV2(
        account.content_hash,
        scenario.common_scenario_set_id,
        tuple(sorted(account.existing_exposure_refs)),
        tuple(sorted(account.pending_risk_refs)),
        (),
        tuple(path_id for path_id, _, _ in scenario.rows),
        account.eligible_equity,
        account.drawdown,
        completeness_available,
        "COMPLETE" if scenario.status.value == "AVAILABLE" and not scenario.synthetic_fixture
            and (flat or bool(existing_paths)) else "NOT_ESTIMABLE",
    )
    completeness_inputs = tuple(sorted({account.content_hash, account.exposure_completeness_ref,
        scenario.content_hash, *account.existing_exposure_refs, *account.pending_risk_refs,
        *existing_portfolio_path_refs}))
    repository.register_artifact(ArtifactIndexEntryV2(completeness.content_hash,
        "DecisionTimePortfolioCompletenessV2", completeness.content_hash, completeness_started,
        completeness_available, {"evidence": completeness.to_dict(), "input_refs": list(completeness_inputs)}))
    record_computation(repository, artifact_ref=completeness.content_hash,
        information_cutoff_ns=candidate.decision_at_ns, started_ns=completeness_started,
        finished_ns=completeness_available, available_ns=completeness_available,
        input_refs=completeness_inputs, deadline_ns=candidate.deadline_ns)
    portfolio = build_decision_time_portfolio_scenarios(
        action=action,
        repo=repository,
        scenario=scenario,
        payoffs=payoffs,
        existing_paths=existing_paths,
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
    evaluation_started = sample_time()
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
        created_at_ns=evaluation_started,
        clock_ns=clock_ns,
    )
    sealed_entry = repository.get_artifact(evaluation_ref)
    if sealed_entry is None:
        raise ValueError("terminal evaluation publication missing")
    evaluation = AmendedEvaluationArtifactV2.from_dict(json_value(sealed_entry.metadata["evaluation"]))
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
