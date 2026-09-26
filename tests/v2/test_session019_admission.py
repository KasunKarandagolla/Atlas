"""Admission uncertainty, ES and amended Evaluation wire checks."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import DecisionStatusV2
from atlas.v2.science.admission import (
    EVALUATION_VERSION,
    AmendedEvaluationArtifactV2,
    DecisionTimePortfolioScenariosV2,
    EstimationUncertaintyV2,
    ExecutionModelUncertaintyV2,
    NumericalErrorV2,
    OutcomeDistributionV2,
    PortfolioPathV2,
    ReservationSnapshotV2,
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
    create_shadow_trade_plan,
    expected_value_lcb,
    index_shadow_plan_snapshots,
    make_execution_uncertainty,
    make_portfolio_es,
)


def _ref(name: str) -> str:
    return sha256_json({"test-ref": name})


def _policy(threshold: Decimal = Decimal("0.5")):
    from atlas.v2.science.admission import ADMISSION_POLICY_VERSION, AdmissionPolicyV2

    return AdmissionPolicyV2(ADMISSION_POLICY_VERSION, threshold, 30, 30, 20, Decimal(1),
        "ISOLATED", "ONE_WAY", "nautilus_trader", "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e", _ref("nautilus-artifact"),
        _ref("execution-profile"), _ref("protection-profile"), "TEST_QUALIFICATION_V1")


def _capability(action, *, account_scope: str, policy, synthetic: bool = True,
        evidence_ref: str | None = None, available_at_ns: int = 100):
    return VenueCapabilitySnapshotV2(action.action.key.venue, action.action.key.environment,
        account_scope, action.action.product_ref, action.action.key.content_hash,
        policy.required_margin_mode, policy.required_position_mode,
        policy.required_nautilus_distribution, policy.required_nautilus_version,
        policy.required_nautilus_source_commit, policy.required_nautilus_artifact_ref,
        policy.required_execution_profile_ref, policy.required_protection_profile_ref,
        policy.required_qualification_version, VenueCapabilityStatusV2.SUPPORTED,
        (evidence_ref or _ref("synthetic-capability-source"),), available_at_ns, synthetic)


def _numerical(action_hash: str, scenario_ref: str, prediction_ref: str, *,
        seed_a: int, seed_b: int, path_count_a: int, path_count_b: int,
        estimate_a: Decimal, estimate_b: Decimal, conversion_error: Decimal) -> NumericalErrorV2:
    return NumericalErrorV2(action_hash, scenario_ref, _ref("template-manifest"),
        _ref("execution-model"), _ref("fee-model"), prediction_ref,
        _ref("run-a"), _ref("run-b"), seed_a, seed_b, path_count_a, path_count_b,
        conversion_error, estimate_a, estimate_b,
        abs(estimate_a - estimate_b) + conversion_error, 101, 102, "AVAILABLE")


def _evaluation(decision: DecisionStatusV2 = DecisionStatusV2.CANDIDATE) -> AmendedEvaluationArtifactV2:
    refs = {name: _ref(name) for name in (
        "action", "action-artifact", "candidate", "policy", "risk-v1-ref", "risk-v1-hash",
        "risk-v2-ref", "risk-v2-hash", "account", "universe", "candidate-set", "selection",
        "causal-state", "feature", "model", "prediction", "scenario", "stress", "portfolio",
        "lcb", "estimation", "execution", "numerical", "support", "calibration", "ood",
        "distribution", "admission-policy", "capability")}
    reasons = () if decision == DecisionStatusV2.CANDIDATE else ("FIXTURE_REJECTION",)
    return AmendedEvaluationArtifactV2(
        action_hash=refs["action"], action_artifact_ref=refs["action-artifact"], candidate_ref=refs["candidate"],
        quantity=Decimal("2"), policy_hash=refs["policy"], risk_policy_ref=refs["risk-v1-ref"],
        risk_policy_hash=refs["risk-v1-hash"], risk_policy_v2_ref=refs["risk-v2-ref"],
        risk_policy_v2_hash=refs["risk-v2-hash"], account_snapshot_ref=refs["account"],
        universe_ref=refs["universe"], candidate_set_ref=refs["candidate-set"],
        selection_policy_hash=refs["selection"], causal_state_ref=refs["causal-state"],
        feature_artifact_ref=refs["feature"], m0_model_ref=refs["model"],
        m0_prediction_ref=refs["prediction"], meta_version="M0_HUBER_RIDGE_ACTION_VALUE_V1",
        pretrade_scenario_ref=refs["scenario"], deterministic_stress_ref=refs["stress"],
        existing_portfolio_ref=refs["portfolio"], expected_net_value=Decimal("1.2"),
        expected_pnl_lcb=Decimal("0.5"), lcb_method_ref=refs["lcb"],
        estimation_uncertainty_ref=refs["estimation"], execution_uncertainty_ref=refs["execution"],
        numerical_error_ref=refs["numerical"], support_ref=refs["support"],
        calibration_ref=refs["calibration"], ood_ref=refs["ood"],
        outcome_distribution_ref=refs["distribution"], admission_policy_ref=refs["admission-policy"],
        capability_evidence_ref=refs["capability"],
        es_before=Decimal("0.01"), es_after=Decimal("0.02"), decision=decision,
        reason_codes=reasons, decision_at_ns=100, available_at_ns=101, action_expiry_ns=200)


def test_amended_evaluation_strict_roundtrip_version_and_content_hash():
    evaluation = _evaluation()
    wire = evaluation.to_dict()
    assert wire["version"] == EVALUATION_VERSION
    assert AmendedEvaluationArtifactV2.from_dict(wire) == evaluation
    assert evaluation.content_hash == sha256_json(wire)
    with pytest.raises(ValueError, match="unknown or pre-amendment"):
        AmendedEvaluationArtifactV2.from_dict(wire | {"version": "EVALUATION_ARTIFACT_V2_V1"})
    with pytest.raises(ValueError, match="unknown fields"):
        AmendedEvaluationArtifactV2.from_dict(wire | {"legacy_meta_score": "0.9"})


def test_old_pre_amendment_wire_is_not_silently_accepted():
    old = {"artifact_type": "EvaluationArtifactV2", "schema_version": 1,
           "decision": "CANDIDATE", "action_hash": _ref("old")}
    with pytest.raises(ValueError):
        AmendedEvaluationArtifactV2.from_dict(old)


def test_lcb_subtracts_three_uncertainty_terms_not_a_pnl_tail_quantile():
    action_hash = _ref("action")
    estimation = EstimationUncertaintyV2(action_hash, _ref("prediction"), _ref("oof"),
        (_ref("outcome-1"), _ref("outcome-2")), Decimal("2"), Decimal("1.96"),
        Decimal("3.92"), "AVAILABLE")
    execution = ExecutionModelUncertaintyV2(action_hash, _ref("execution-model"),
        (_ref("exec-1"), _ref("exec-2")), 2, Decimal("1.5"), "AVAILABLE")
    numerical = _numerical(action_hash, _ref("scenario"), _ref("prediction"), seed_a=11,
        seed_b=29, path_count_a=1_000, path_count_b=10_000,
        estimate_a=Decimal("8.0"), estimate_b=Decimal("8.2"), conversion_error=Decimal("0"))
    assert numerical.error_bound == Decimal("0.2")
    assert expected_value_lcb(Decimal("10"), estimation, execution, numerical) == Decimal("4.38")
    assert expected_value_lcb(Decimal("10"), estimation,
        ExecutionModelUncertaintyV2(action_hash, _ref("exec-model-unsupported"), (), 0, None, "NOT_ESTIMABLE"),
        numerical) is None


def test_m0_decimal_conversion_error_is_recorded_and_subtracted_separately():
    numerical = _numerical(_ref("action"), _ref("scenario"), _ref("prediction"), seed_a=2,
        seed_b=7, path_count_a=100, path_count_b=1000,
        estimate_a=Decimal("1"), estimate_b=Decimal("1.1"), conversion_error=Decimal("0.001"))
    assert numerical.m0_conversion_error == Decimal("0.001")
    assert numerical.error_bound == Decimal("0.101")


def test_deterministic_admission_requires_every_gate_and_respects_materiality(tmp_path):
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.admission import (
        M0OODV2,
        DeterministicStressV2,
        M0CalibrationV2,
        M0SupportV2,
        PortfolioESV2,
        ScenarioSupportV2,
        decide_admission,
    )
    from atlas.v2.science.m0 import M0PredictionV2
    from atlas.v2.science.pretrade import CausalInputV2
    from atlas.v2.science.scenario_engine import (
        SCENARIO_GENERATOR_VERSION,
        PretradeExecutionScenarioV2,
        ScenarioGenerationStatusV2,
    )
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .test_session016_candidate_selection import CUTOFF
    from .test_session017_risk import risk_case, size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)

    def causal(name: str) -> CausalInputV2:
        return CausalInputV2(_ref(f"decision-{name}"), f"DecisionFixture{name}V1", CUTOFF, CUTOFF)

    model_input, calibration_input, execution_input, source_input = (
        causal("model"), causal("calibration"), causal("execution"), causal("source"))
    template_ref, joint_data_ref = _ref("template"), _ref("joint-data")
    path_id, payload_ref, payoff_ref = _ref("common-path"), _ref("joint-payload"), _ref("path-payoff")
    scenario = PretradeExecutionScenarioV2(action.action.action_hash, action.content_hash, CUTOFF,
        model_input, calibration_input, execution_input, (source_input,), (template_ref,),
        (joint_data_ref,), ((path_id, Decimal(1), payload_ref),), (),
        SCENARIO_GENERATOR_VERSION, _ref("common-set"), CUTOFF + 1, CUTOFF + 2, CUTOFF + 3,
        case.candidate.deadline_ns, ScenarioGenerationStatusV2.AVAILABLE, seed=19, scenario_count=1)
    support = M0SupportV2(action.action.action_hash, CUTOFF, 40, 40, 1, (("ACTUAL", 40),),
        (("FULL_FILL", 40),), (("__any_missing__", Decimal(0)),), CUTOFF - 10,
        CUTOFF - 1, (_ref("training-outcome"),), _ref("compatibility"), "SUPPORTED")
    prediction = M0PredictionV2(action.action.action_hash, action.content_hash, _ref("features"),
        _ref("model"), CUTOFF, CUTOFF + 3, Decimal(2), Decimal("0.1"), Decimal(0),
        support.content_hash, _ref("oof"), _ref("calibration"), _ref("ood"), "AVAILABLE", ())
    calibration = M0CalibrationV2(action.action.action_hash, CUTOFF, prediction.oof_archive_ref,
        40, Decimal("0.3"), "OOF_CALIBRATED")
    ood = M0OODV2(action.action.action_hash, prediction.feature_vector_ref,
        support.training_outcome_refs, Decimal(5), Decimal("0.2"), False, "IN_DISTRIBUTION")
    unit_refs = tuple(sorted(_ref(f"support-unit-{i}") for i in range(40)))
    scenario_support = ScenarioSupportV2(action.action.action_hash, scenario.content_hash, 40,
        unit_refs, unit_refs, True, True, True, True, True, "SUPPORTED", (template_ref,))
    outcome = OutcomeDistributionV2(action.action.action_hash, scenario.content_hash, (payoff_ref,),
        (path_id,), (Decimal(1),), (Decimal(2),), Decimal(2), Decimal(2), "AVAILABLE")
    estimation = EstimationUncertaintyV2(action.action.action_hash, prediction.content_hash,
        prediction.oof_archive_ref, support.training_outcome_refs, Decimal("0.05"), Decimal(1),
        Decimal("0.05"), "AVAILABLE")
    execution = ExecutionModelUncertaintyV2(action.action.action_hash, execution_input.ref,
        (_ref("execution-residual"),), 40, Decimal("0.05"), "AVAILABLE")
    numerical = _numerical(action.action.action_hash, scenario.content_hash, prediction.content_hash,
        seed_a=1901, seed_b=1902, path_count_a=100, path_count_b=1000,
        estimate_a=Decimal("1.9"), estimate_b=Decimal("1.9"), conversion_error=Decimal(0))
    stress = DeterministicStressV2(action.action.action_hash, action.content_hash,
        action.action.quantity, case.v1.policy_hash(), case.v1.policy_hash(), Decimal(10_000),
        Decimal(0), Decimal(50), CUTOFF, "STRESS_FIXTURE_V1", _ref("stress-evidence"),
        (_ref("stress-case"),), Decimal(10), False, "AVAILABLE", ())
    portfolio = PortfolioESV2(action.action.action_hash, _ref("portfolio"), Decimal("0.95"),
        Decimal("0.05"), Decimal("0.01"), Decimal("0.02"), "AVAILABLE", False)

    def result(*, expected: Decimal = Decimal(2), threshold: Decimal = Decimal("0.5"),
            supported: bool = True, stress_breach: bool = False, es_breach: bool = False,
            materially_ood: bool = False, capability_override=None,
            allow_fixture: bool = True, legacy_capability_qualified: bool | None = None):
        admission_policy = _policy(threshold)
        capability = capability_override or _capability(action, account_scope="SYNTHETIC_ACCOUNT",
            policy=admission_policy)
        kwargs = {
            "action": action,
            "prediction": replace(prediction, expected_net_value=expected),
            "m0_support": support,
            "calibration": calibration,
            "ood": replace(ood, out_of_distribution=materially_ood,
                status="OOD" if materially_ood else "IN_DISTRIBUTION"),
            "scenario": scenario,
            "scenario_support": scenario_support,
            "outcome_distribution": outcome,
            "estimation": estimation,
            "execution": execution if supported else replace(execution, residual_refs=(),
                independent_support_count=0, absolute_cost_error_q90=None, status="NOT_ESTIMABLE"),
            "numerical": numerical,
            "stress": replace(stress, breach=stress_breach),
            "portfolio": replace(portfolio, breach=es_breach),
            "policy": admission_policy,
            "capability": capability,
            "account_scope": "SYNTHETIC_ACCOUNT",
            "allow_synthetic_fixtures": allow_fixture,
        }
        if legacy_capability_qualified is not None:
            kwargs["venue_capability_qualified"] = legacy_capability_qualified
        return decide_admission(**kwargs)

    assert result().decision == DecisionStatusV2.CANDIDATE
    assert result(expected=Decimal("-1")).decision == DecisionStatusV2.NO_TRADE
    assert result(threshold=Decimal("2")).decision == DecisionStatusV2.NO_TRADE
    assert result(supported=False).decision == DecisionStatusV2.NOT_ESTIMABLE
    assert result(materially_ood=True).decision == DecisionStatusV2.NOT_ESTIMABLE
    assert result(stress_breach=True).decision == DecisionStatusV2.NO_TRADE
    assert result(es_breach=True).decision == DecisionStatusV2.NO_TRADE
    assert result(allow_fixture=False).decision == DecisionStatusV2.NOT_ESTIMABLE
    with pytest.raises(TypeError, match="venue_capability_qualified"):
        result(legacy_capability_qualified=True)
    wrong_venue = replace(_capability(action, account_scope="SYNTHETIC_ACCOUNT", policy=_policy()),
        venue="BINANCE")
    wrong_account = replace(_capability(action, account_scope="SYNTHETIC_ACCOUNT", policy=_policy()),
        account_scope="OTHER_ACCOUNT")
    wrong_product = replace(_capability(action, account_scope="SYNTHETIC_ACCOUNT", policy=_policy()),
        product_ref=_ref("wrong-product"))
    wrong_profile = replace(_capability(action, account_scope="SYNTHETIC_ACCOUNT", policy=_policy()),
        protection_profile_ref=_ref("wrong-protection-profile"))
    assert result(capability_override=wrong_venue).decision == DecisionStatusV2.NOT_ESTIMABLE
    assert result(capability_override=wrong_account).decision == DecisionStatusV2.NOT_ESTIMABLE
    assert result(capability_override=wrong_product).decision == DecisionStatusV2.NOT_ESTIMABLE
    assert result(capability_override=wrong_profile).decision == DecisionStatusV2.NOT_ESTIMABLE
    with pytest.raises(ValueError, match="unknown fields"):
        _policy().from_dict(_policy().to_dict() | {"venue_capability_qualified": True})


def test_outcome_dispersion_and_mean_estimation_uncertainty_are_separate_artifacts():
    action_hash, scenario_ref = _ref("distribution-action"), _ref("distribution-scenario")
    payoff_refs = tuple(sorted((_ref("payoff-down"), _ref("payoff-up"))))
    path_ids = tuple(sorted((_ref("path-down"), _ref("path-up"))))
    distribution = OutcomeDistributionV2(action_hash, scenario_ref, payoff_refs, path_ids,
        (Decimal("0.5"), Decimal("0.5")), (Decimal("-100"), Decimal("100")),
        Decimal("0"), Decimal("-100"), "AVAILABLE")
    estimation = EstimationUncertaintyV2(action_hash, _ref("prediction"), _ref("oof"),
        (_ref("outcome-1"), _ref("outcome-2")), Decimal("2"), Decimal("1.96"),
        Decimal("3.92"), "AVAILABLE")
    assert distribution.downside_q05 == Decimal("-100")
    assert estimation.uncertainty_amount == Decimal("3.92")
    assert distribution.content_hash != estimation.content_hash


def test_execution_uncertainty_needs_historical_compatible_support_not_path_count():
    from atlas.v2.science.admission import ExecutionCalibrationResidualV2

    key = _ref("compatibility")
    model = _ref("execution-model")
    residual = ExecutionCalibrationResidualV2(_ref("actual-exec"), key,
        Decimal("1"), Decimal("3"), 100, 150)
    unsupported = make_execution_uncertainty(_ref("action"), model, (residual,),
        compatibility_key=key, cutoff_ns=149, minimum_support=1)
    supported = make_execution_uncertainty(_ref("action"), model, (residual,),
        compatibility_key=key, cutoff_ns=150, minimum_support=1)
    assert unsupported.status == "NOT_ESTIMABLE"
    assert supported.status == "AVAILABLE"
    assert supported.absolute_cost_error_q90 == Decimal("2")
    overlapping = replace(residual, evidence_ref=_ref("overlapping-exec"), decision_at_ns=110)
    repeated = make_execution_uncertainty(_ref("action"), model, (residual, overlapping),
        compatibility_key=key, cutoff_ns=150, minimum_support=2)
    assert repeated.independent_support_count == 1 and repeated.status == "NOT_ESTIMABLE"


def test_portfolio_es_uses_common_units_and_existing_drawdown_scaled_risk_limit():
    from atlas.domain.risk import engineering_default_policy

    action_hash = _ref("es-action")
    p0, p1 = sorted((_ref("common-path-a"), _ref("common-path-b")))
    portfolio = DecisionTimePortfolioScenariosV2(action_hash, _ref("action-artifact"),
        _ref("scenario"), _ref("common-set"), _ref("account"), _ref("completeness"),
        (), (), False, 100, 100 + 24 * 3_600_000_000_000, Decimal("100000"), Decimal("0.075"),
        (PortfolioPathV2(p0, Decimal("0.5"), Decimal("0"), Decimal("0"), _ref("payoff-a"), True),
         PortfolioPathV2(p1, Decimal("0.5"), Decimal("0"), Decimal("-900"), _ref("payoff-b"), True)),
        "AVAILABLE")
    result = make_portfolio_es(portfolio,
        risk_policy=engineering_default_policy(policy_version="ES_FIXTURE"),
        risk_policy_ref=_ref("risk-policy"))
    assert result.es_before_fraction == Decimal("0")
    assert result.es_after_fraction == Decimal("0.009")
    assert result.limit_fraction == Decimal("0.0050")
    assert result.breach is True


def test_missing_stress_evidence_is_not_estimable_and_keeps_frozen_quantity(tmp_path):
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.admission import evaluate_deterministic_stress
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .test_session017_risk import CUTOFF, risk_case, size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        stress = evaluate_deterministic_stress(repo, action=action, risk_policy=case.v1,
            risk_policy_ref=case.v1.policy_hash(), eligible_equity=case.account.eligible_equity,
            drawdown=None, product_base_units=case.product.base_units_per_contract,
            stress_input=None, stress_evidence_ref=None, cutoff_ns=CUTOFF)
        assert stress.status == "NOT_ESTIMABLE"
        assert stress.breach is None and stress.maximum_loss is None
        assert stress.loss_limit is None
        assert stress.action_hash == action.action.action_hash
        assert stress.quantity == action.action.quantity


def test_typed_stress_input_roundtrip_exact_quantity_and_supported_breach(tmp_path):
    from support.phase4_factory import complete_stress_input

    from atlas.domain.enums import Side
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.admission import (
        evaluate_deterministic_stress,
        index_stress_input,
        stress_input_from_wire,
        stress_input_wire,
    )
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .test_session017_risk import CUTOFF, risk_case, size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        state = complete_stress_input(side=Side(action.action.side),
            quantity=action.action.quantity * case.product.base_units_per_contract,
            mark=action.action.entry_reference)
        assert stress_input_from_wire(stress_input_wire(state)) == state
        with pytest.raises(ValueError, match="unsupported"):
            stress_input_from_wire(stress_input_wire(state) | {"version": "V2_DETERMINISTIC_STRESS_INPUT_V999"})
        ref = index_stress_input(repo, state, source_refs=(case.product.content_hash,),
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF)
        result = evaluate_deterministic_stress(repo, action=action, risk_policy=case.v1,
            risk_policy_ref=case.v1.policy_hash(), eligible_equity=Decimal(1), drawdown=Decimal(0),
            product_base_units=case.product.base_units_per_contract, stress_input=state,
            stress_evidence_ref=ref, cutoff_ns=CUTOFF)
        assert result.status == "AVAILABLE" and result.breach is True
        assert result.quantity == action.action.quantity
        wrong = evaluate_deterministic_stress(repo, action=action, risk_policy=case.v1,
            risk_policy_ref=case.v1.policy_hash(), eligible_equity=Decimal(1), drawdown=Decimal(0),
            product_base_units=case.product.base_units_per_contract,
            stress_input=replace(state, quantity=state.quantity / 2), stress_evidence_ref=ref, cutoff_ns=CUTOFF)
        assert wrong.status == "NOT_ESTIMABLE" and wrong.maximum_loss is None


def test_execution_calibration_residual_strict_wire_and_bound_sources(tmp_path):
    from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
    from atlas.v2.science.admission import ExecutionCalibrationResidualV2, index_execution_calibration_residual

    body = {"version": "SYNTHETIC_EXECUTION_FIXTURE_V1", "available_at_ns": 100}
    source_ref = sha256_json(body)
    row = ExecutionCalibrationResidualV2(_ref("placeholder"), _ref("compatibility"),
        Decimal(1), Decimal(3), 100, 150, "SIMULATED", (source_ref,))
    row = replace(row, evidence_ref=row.content_hash)
    assert ExecutionCalibrationResidualV2.from_dict(row.to_dict(), evidence_ref=row.content_hash) == row
    with pytest.raises(ValueError, match="unsupported"):
        ExecutionCalibrationResidualV2.from_dict(row.to_dict() | {"version": "EXECUTION_CALIBRATION_RESIDUAL_V999"},
            evidence_ref=row.content_hash)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        with pytest.raises(ValueError, match="unavailable"):
            index_execution_calibration_residual(repo, row)
        repo.register_artifact(ArtifactIndexEntryV2(source_ref, "ExecutionFixtureV1", source_ref, 100, 100, body))
        assert index_execution_calibration_residual(repo, row) == row.content_hash


def test_existing_portfolio_valuation_needs_causal_models_inventory_and_24h_evidence(tmp_path):
    from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
    from atlas.v2.science.admission import ExistingPortfolioPathV2, index_existing_portfolio_path

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        refs = []
        for kind in ("ExposureV2", "PortfolioModelV2", "PortfolioCalibrationV2", "CutoffMarketEvidenceV2"):
            body = {"version": "SYNTHETIC_PORTFOLIO_FIXTURE_V1", "kind": kind}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, 100, 100, body))
            refs.append(ref)
        path = ExistingPortfolioPathV2(_ref("portfolio-common-path"), _ref("portfolio-common-set"),
            Decimal(1), Decimal("-1"), 100, 100 + 24 * 3_600_000_000_000, 101,
            (refs[0],), (refs[3],), refs[1], refs[2])
        assert ExistingPortfolioPathV2.from_dict(path.to_dict()) == path
        assert index_existing_portfolio_path(repo, path) == path.content_hash
        with pytest.raises(ValueError, match="common-horizon"):
            index_existing_portfolio_path(repo, replace(path, horizon_end_ns=200))
        with pytest.raises(ValueError, match="source evidence"):
            index_existing_portfolio_path(repo, replace(path, source_refs=()))
        with pytest.raises(ValueError, match="unsupported"):
            ExistingPortfolioPathV2.from_dict(path.to_dict() | {"version": "DECISION_TIME_EXISTING_PORTFOLIO_PATH_V999"})
        body = {"version": "SYNTHETIC_RETROSPECTIVE_FIXTURE_V1"}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "PairedPortfolioPayoffV2", ref, 100, 100, body))
        with pytest.raises(ValueError, match="retrospective"):
            index_existing_portfolio_path(repo, replace(path, model_ref=ref))


@pytest.mark.parametrize("decision", [DecisionStatusV2.NO_TRADE, DecisionStatusV2.NOT_ESTIMABLE])
def test_rejected_evaluation_has_terminal_reason_and_is_not_candidate(decision):
    result = _evaluation(decision)
    assert result.decision == decision
    assert result.reason_codes == ("FIXTURE_REJECTION",)
    assert AmendedEvaluationArtifactV2.from_dict(result.to_dict()) == result


def test_synthetic_candidate_fixture_creates_one_read_only_plan_from_frozen_sizing(tmp_path, monkeypatch):
    from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.admission import index_admission_evidence
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from . import test_session016_candidate_selection as selection_module
    from . import test_session017_risk as risk_module

    original_candidate = selection_module.candidate

    def scoped_candidate(*args, **kwargs):
        item = original_candidate(*args, **kwargs)
        return replace(item, envelope=replace(item.envelope, content_hash=""),
            account_scope="SHADOW_FAKE_ACCOUNT")

    monkeypatch.setattr(risk_module, "candidate", scoped_candidate)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_module.risk_case(repo)
        sizing = risk_module.size(repo, case)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        admission_policy = _policy()
        admission_policy_ref = index_admission_evidence(repo, "AdmissionPolicyV2",
            admission_policy.to_dict(), case.candidate.decision_at_ns, metadata_key="evidence")
        capability_source_ref = index_admission_evidence(repo, "SyntheticVenueCapabilitySourceV2",
            {"version": "SYNTHETIC_VENUE_CAPABILITY_SOURCE_V1", "scope": "SHADOW_FAKE_ACCOUNT"},
            case.candidate.decision_at_ns, metadata_key="evidence")
        capability = _capability(action, account_scope="SHADOW_FAKE_ACCOUNT",
            policy=admission_policy, evidence_ref=capability_source_ref,
            available_at_ns=case.candidate.decision_at_ns)
        refs = {name: _ref(f"plan-{name}") for name in (
            "model", "prediction", "scenario", "stress", "portfolio", "lcb", "estimation",
            "execution", "numerical", "support", "calibration", "ood", "distribution")}
        evaluation = AmendedEvaluationArtifactV2(
            action.action.action_hash, action.content_hash, case.candidate.content_hash,
            action.action.quantity, S1_POLICY.policy_hash, case.v1.policy_hash(), case.v1.policy_hash(),
            case.v2.policy_hash, case.v2.policy_hash, case.account.content_hash, case.universe.content_hash,
            case.candidate_set.content_hash, case.candidate_set.selection_policy_hash,
            case.candidate.snapshot_hash, case.candidate.snapshot_hash, refs["model"], refs["prediction"],
            "M0_HUBER_RIDGE_ACTION_VALUE_V1", refs["scenario"], refs["stress"], refs["portfolio"],
            Decimal("1"), Decimal("0.5"), refs["lcb"], refs["estimation"], refs["execution"],
            refs["numerical"], refs["support"], refs["calibration"], refs["ood"], refs["distribution"],
            admission_policy_ref, capability.content_hash, Decimal("0"), Decimal("0"), DecisionStatusV2.CANDIDATE, (),
            case.candidate.decision_at_ns, case.candidate.decision_at_ns + 100, case.candidate.deadline_ns)
        repo.register_artifact(ArtifactIndexEntryV2(evaluation.content_hash, "EvaluationArtifactV2",
            evaluation.content_hash, evaluation.decision_at_ns, evaluation.available_at_ns,
            {"evaluation": evaluation.to_dict()}))
        reservation = ReservationSnapshotV2("SHADOW_FAKE_ACCOUNT", evaluation.available_at_ns, 1, True)
        index_shadow_plan_snapshots(repo, capability=capability, reservation=reservation)
        plan = create_shadow_trade_plan(repo, action=action, evaluation=evaluation,
            capability=capability, reservation=reservation, allow_synthetic_fixtures=True)
        again = create_shadow_trade_plan(repo, action=action, evaluation=evaluation,
            capability=capability, reservation=reservation, allow_synthetic_fixtures=True)
        assert plan is not None and again is not None and plan.content_hash == again.content_hash
        assert plan.qty_limit == action.action.quantity == sizing.quantity
        assert plan.stop == action.action.stop_price
        assert plan.collar == action.action.entry_collar
        assert plan.entry_policy == canonical_json({"entry_rule": action.action.entry_rule.to_dict(),
            "trigger_basis": action.action.entry_trigger_basis})
        assert plan.stop_trigger_basis == "MARK_PRICE"
        assert plan.horizon_end_ns == action.action.horizon_end_ns
        assert plan.leverage_bound == sizing.leverage
        assert plan.normal_risk == sizing.normal_risk and plan.stress_risk == sizing.stress_risk
        assert plan.margin == sizing.margin
        plan_entry = repo.get_artifact(plan.content_hash)
        assert plan_entry is not None
        assert plan_entry.metadata["shadow_plan_evidence"]["shadow_read_only"] is True
        assert len(repo.artifact_entries("TradePlanEnvelopeV2")) == 1
        assert create_shadow_trade_plan(repo, action=action, evaluation=evaluation,
            capability=capability, reservation=reservation) is None
        real_source_ref = index_admission_evidence(repo, "VenueCapabilityQualificationEvidenceV2",
            {"version": "VENUE_CAPABILITY_QUALIFICATION_FIXTURE_V1", "scope": "SHADOW_FAKE_ACCOUNT"},
            case.candidate.decision_at_ns, metadata_key="evidence")
        real_capability = replace(capability, synthetic_fixture=False, evidence_refs=(real_source_ref,))
        with pytest.raises(ValueError, match="CapabilityContractV1"):
            index_shadow_plan_snapshots(repo, capability=real_capability, reservation=reservation)
        assert create_shadow_trade_plan(repo, action=action,
            evaluation=replace(evaluation, decision=DecisionStatusV2.NO_TRADE,
                reason_codes=("EXPECTED_NET_VALUE_LCB_NOT_POSITIVE",)),
            capability=capability, reservation=reservation,
            allow_synthetic_fixtures=True) is None
        assert create_shadow_trade_plan(repo, action=action,
            evaluation=replace(evaluation, decision=DecisionStatusV2.NOT_ESTIMABLE,
                reason_codes=("M0_NOT_ESTIMABLE",)),
            capability=capability, reservation=reservation,
            allow_synthetic_fixtures=True) is None
        altered = replace(action, action=replace(action.action,
            quantity=action.action.quantity / Decimal("2"), stop_price=action.action.stop_price + Decimal("1")))
        with pytest.raises(ValueError, match="identity/capability/reservation mismatch"):
            create_shadow_trade_plan(repo, action=altered, evaluation=evaluation,
                capability=capability, reservation=reservation, allow_synthetic_fixtures=True)
