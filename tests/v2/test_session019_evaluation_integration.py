"""Strict persistence path for a decision-time economic admission."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import freeze_action
from atlas.v2.science.admission import (
    ADMISSION_POLICY_VERSION,
    AdmissionPolicyV2,
    DecisionTimePortfolioCompletenessV2,
    LCBMethodV2,
    build_decision_time_portfolio_scenarios,
    decide_admission,
    evaluate_deterministic_stress,
    index_admission_evidence,
    index_amended_evaluation,
    index_lcb_method,
    index_portfolio_completeness,
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
from atlas.v2.science.m0 import M0OODV2, fit_m0
from atlas.v2.science.outcomes import AdmissionStateV2, DecisionCalendarEntryV2
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.scenario_engine import generate_pretrade_scenarios
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session014_core import KEY
from .test_session017_risk import CUTOFF, risk_case, size


def test_unestimable_exact_action_persists_amended_evaluation_and_terminal_calendar(tmp_path, monkeypatch):
    from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2

    from . import test_session017_risk as risk_module

    original_candidate = risk_module.candidate
    features: dict[str, FeatureArtifactV2] = {}

    def candidate_with_feature(policy=S1_POLICY, key=KEY, **kwargs):
        item = original_candidate(policy, key, **kwargs)
        feature = FeatureArtifactV2(
            ArtifactEnvelope(1, f"session019-admission-feature-{item.candidate_id}", CUTOFF,
                CUTOFF, "session019-fixture", ()), item.key, "SESSION019_FIXTURE_V1",
            CUTOFF, CUTOFF, {"h4.ema20": FeatureValueV2(Decimal("100"), "PRICE")},
            sha256_json("fixture-source-health"), ReplayViewV2.ACTUAL_SYSTEM)
        item = replace(item, snapshot_hash=feature.content_hash,
            envelope=replace(item.envelope, content_hash="", input_refs=(feature.content_hash,)))
        features[feature.content_hash] = feature
        return item

    monkeypatch.setattr(risk_module, "candidate", candidate_with_feature)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        for ref, feature in features.items():
            repo.register_artifact(ArtifactIndexEntryV2(ref, "FeatureArtifactV2", ref,
                CUTOFF, CUTOFF, {"feature": feature.to_dict()}))
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        _, prediction, m0_support, calibration, _ = fit_m0(repo, action=action,
            candidate=case.candidate, candidate_set=case.candidate_set, cutoff_ns=CUTOFF,
            available_at_ns=CUTOFF + 1)
        assert prediction.status == "NOT_ESTIMABLE"

        def causal(kind: str) -> CausalInputV2:
            body = {"kind": kind, "decision_cutoff_ns": CUTOFF}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, CUTOFF, CUTOFF, body))
            return CausalInputV2(ref, kind, CUTOFF, CUTOFF)

        model_input = causal("Session019ModelFixtureV1")
        calibration_input = causal("Session019CalibrationFixtureV1")
        execution_input = causal("Session019ExecutionModelFixtureV1")
        scenario, payoffs = generate_pretrade_scenarios(repo, action=action,
            model_input=model_input, calibration_input=calibration_input,
            execution_model_input=execution_input, source_inputs=(), joint_data_refs=(),
            fee=case.fee, base_units_per_contract=case.product.base_units_per_contract,
            cutoff_ns=CUTOFF, created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2,
            available_at_ns=CUTOFF + 3, expires_at_ns=case.candidate.deadline_ns,
            seed=1901, scenario_count=100)
        assert repo.get_artifact(scenario.content_hash) is not None

        scenario_support = make_scenario_support(scenario)
        index_admission_evidence(repo, "ScenarioSupportV2",
            scenario_support.to_dict(), CUTOFF + 3)
        support = make_inference_support(action.action.action_hash, m0_support, scenario_support)
        index_admission_evidence(repo, "InferenceSupportV2", support.to_dict(), CUTOFF + 3)
        distribution = make_outcome_distribution(scenario, payoffs)
        distribution_ref = index_admission_evidence(repo, "OutcomeDistributionV2",
            distribution.to_dict(), CUTOFF + 3)

        estimation = make_estimation_uncertainty(prediction,
            training_refs=m0_support.training_outcome_refs, confidence_multiplier=Decimal("1.96"))
        estimation_ref = index_admission_evidence(repo, "EstimationUncertaintyV2",
            estimation.to_dict(), CUTOFF + 3)
        execution = make_execution_uncertainty(action.action.action_hash, execution_input.ref, (),
            compatibility_key=m0_support.compatibility_key, cutoff_ns=CUTOFF, minimum_support=1)
        execution_ref = index_admission_evidence(repo, "ExecutionModelUncertaintyV2",
            execution.to_dict(), CUTOFF + 3)
        numerical = make_numerical_error(action_hash=action.action.action_hash,
            scenario_ref=scenario.content_hash, seed_a=19, seed_b=23,
            path_count_a=100, path_count_b=1000, estimate_a=None, estimate_b=None,
            m0_conversion_error=prediction.numerical_conversion_error)
        numerical_ref = index_admission_evidence(repo, "NumericalErrorV2", numerical.to_dict(), CUTOFF + 3)
        stress = evaluate_deterministic_stress(repo, action=action, risk_policy=case.v1,
            risk_policy_ref=case.v1.policy_hash(), eligible_equity=case.account.eligible_equity,
            drawdown=case.account.drawdown, product_base_units=case.product.base_units_per_contract,
            stress_input=None, stress_evidence_ref=None, cutoff_ns=CUTOFF)
        index_admission_evidence(repo, "DeterministicStressV2", stress.to_dict(), CUTOFF + 3)

        completeness = DecisionTimePortfolioCompletenessV2(case.account.content_hash,
            scenario.common_scenario_set_id, tuple(sorted(case.account.existing_exposure_refs)),
            tuple(sorted(case.account.pending_risk_refs)), (), (), case.account.eligible_equity,
            case.account.drawdown, CUTOFF, "NOT_ESTIMABLE")
        index_portfolio_completeness(repo, completeness)
        portfolio = build_decision_time_portfolio_scenarios(action=action, repo=repo,
            scenario=scenario, payoffs=payoffs, existing_paths=(), completeness=completeness)
        index_admission_evidence(repo, "DecisionTimePortfolioScenariosV2", portfolio.to_dict(), CUTOFF + 3)
        portfolio_es = make_portfolio_es(portfolio, risk_policy=case.v1,
            risk_policy_ref=case.v1.policy_hash())
        index_admission_evidence(repo, "PortfolioESV2", portfolio_es.to_dict(), CUTOFF + 3)

        policy = AdmissionPolicyV2(ADMISSION_POLICY_VERSION, Decimal("1"), 30, 30, 20,
            Decimal("1.96"), False)
        policy_ref = index_admission_evidence(repo, "AdmissionPolicyV2", policy.to_dict(), CUTOFF + 4)
        lcb_method_ref = index_lcb_method(repo, LCBMethodV2(), available_at_ns=CUTOFF + 4)
        ood_entry = repo.get_artifact(prediction.ood_ref)
        assert ood_entry is not None
        ood_body = json_value(ood_entry.metadata["ood"])
        ood = M0OODV2(ood_body["action_hash"], ood_body["feature_vector_ref"],
            tuple(ood_body["training_row_refs"]), Decimal(ood_body["robust_z_limit"]),
            Decimal(ood_body["maximum_absolute_robust_z"])
                if ood_body["maximum_absolute_robust_z"] is not None else None,
            ood_body["out_of_distribution"], ood_body["status"])
        result = decide_admission(action=action, prediction=prediction, m0_support=m0_support,
            calibration=calibration, ood=ood,
            scenario=scenario, scenario_support=scenario_support, outcome_distribution=distribution,
            estimation=estimation, execution=execution, numerical=numerical, stress=stress,
            portfolio=portfolio_es, policy=policy)
        assert result.decision.value == "NOT_ESTIMABLE"
        evaluation = make_amended_evaluation(action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, prediction=prediction, scenario=scenario,
            stress=stress, portfolio=portfolio, portfolio_es=portfolio_es, support=support,
            estimation_ref=estimation_ref, execution_ref=execution_ref,
            numerical_ref=numerical_ref, calibration_ref=prediction.calibration_ref,
            ood_ref=prediction.ood_ref, outcome_distribution_ref=distribution_ref,
            admission_policy_ref=policy_ref, lcb_method_ref=lcb_method_ref,
            account_snapshot_ref=case.account.content_hash,
            risk_policy_ref=case.v1.policy_hash(), risk_policy_v2_ref=case.v2.policy_hash,
            causal_state_ref=case.candidate.snapshot_hash, available_at_ns=CUTOFF + 10,
            result=result)
        evaluation_ref, calendar_ref = persist_economic_decision(repo, evaluation,
            policy_id=S1_POLICY.policy_id, policy_version=S1_POLICY.version, created_at_ns=CUTOFF + 10)
        indexed_eval = repo.get_artifact(evaluation_ref)
        indexed_calendar = repo.get_artifact(calendar_ref)
        assert indexed_eval is not None and canonical_json(indexed_eval.metadata["evaluation"]) == canonical_json(evaluation.to_dict())
        assert indexed_calendar is not None
        calendar = DecisionCalendarEntryV2.from_dict(json_value(indexed_calendar.metadata["decision_entry"]))
        assert calendar.source_artifact_ref == evaluation_ref
        assert calendar.action_hash == action.action.action_hash
        assert calendar.action_artifact_ref == action.content_hash
        assert calendar.selection_state.value == "SELECTED"
        assert calendar.admission_state == AdmissionStateV2.NOT_ESTIMABLE
        assert calendar.reason_codes == evaluation.reason_codes
        for field in ("action_hash", "candidate_ref", "candidate_set_ref", "risk_policy_ref",
                "pretrade_scenario_ref", "m0_prediction_ref", "support_ref", "ood_ref"):
            with pytest.raises(ValueError):
                index_amended_evaluation(repo, replace(evaluation,
                    **{field: sha256_json({"wrong-session019-ref": field})}))
        forged_estimation = replace(estimation, status="AVAILABLE", standard_error=Decimal(0),
            uncertainty_amount=Decimal(0))
        forged_ref = index_admission_evidence(repo, "EstimationUncertaintyV2",
            forged_estimation.to_dict(), CUTOFF + 3)
        with pytest.raises(ValueError, match="estimation uncertainty does not reproduce"):
            index_amended_evaluation(repo, replace(evaluation, estimation_uncertainty_ref=forged_ref))
        assert len(repo.artifact_entries("EvaluationArtifactV2")) == 1
        assert len(repo.artifact_entries("DecisionCalendarEntryV2")) == 1
