"""Clock observations cannot be replaced by invented early publication times."""

from pathlib import Path

import pytest

from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.m0 import fit_m0
from atlas.v2.science.scenario_engine import generate_pretrade_scenarios

from .test_session019_scenarios import _evidence
from .test_session023_analogue import evidence_bound_action


def test_m0_publications_follow_actual_computation_and_reuse_sealed_inputs(tmp_path: Path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        cutoff = case.candidate.decision_at_ns
        now = cutoff + 10

        def clock() -> int:
            nonlocal now
            now += 10
            return now

        args = {"action": action, "candidate": case.candidate, "candidate_set": case.candidate_set,
                "cutoff_ns": cutoff, "available_at_ns": cutoff + 1, "clock_ns": clock}
        _, prediction, _, _, _ = fit_m0(repo, **args)
        assert prediction.available_at_ns == now
        assert prediction.available_at_ns > cutoff + 1
        feature = repo.get_artifact(prediction.feature_vector_ref)
        model = repo.get_artifact(prediction.model_ref)
        assert feature is not None and model is not None
        assert cutoff < feature.available_at_ns < model.available_at_ns <= prediction.available_at_ns
        original_feature_time = feature.available_at_ns
        _, second, _, _, _ = fit_m0(repo, **args)
        assert second.available_at_ns > prediction.available_at_ns
        assert repo.get_artifact(prediction.feature_vector_ref).available_at_ns == original_feature_time


def test_m0_deadline_or_clock_regression_cannot_publish_a_usable_prediction(tmp_path: Path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        for observed, message in ((case.candidate.deadline_ns, "deadline"),
                                  (case.candidate.decision_at_ns, "regressed")):
            with pytest.raises(ValueError, match=message):
                fit_m0(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
                       cutoff_ns=case.candidate.decision_at_ns,
                       available_at_ns=case.candidate.decision_at_ns + 1, clock_ns=lambda at=observed: at)
        assert repo.artifact_entries("M0PredictionV2") == ()


def test_scenario_records_sampled_start_finish_and_rejects_late_completion(tmp_path: Path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        cutoff = case.candidate.decision_at_ns
        args = {"action": action, "model_input": _evidence(repo, "PretradeModelV1", "clock-model"),
                "calibration_input": _evidence(repo, "PretradeCalibrationV1", "clock-calibration"),
                "execution_model_input": _evidence(repo, "ExecutionModelV1", "clock-execution"),
                "source_inputs": (), "joint_data_refs": (), "fee": case.fee,
                "base_units_per_contract": case.product.base_units_per_contract, "cutoff_ns": cutoff,
                "created_at_ns": cutoff + 1, "computed_at_ns": cutoff + 1, "available_at_ns": cutoff + 1,
                "expires_at_ns": case.candidate.deadline_ns, "seed": 1, "scenario_count": 10}
        observations = iter((cutoff + 20, cutoff + 40))
        scenario, _ = generate_pretrade_scenarios(repo, **args, clock_ns=lambda: next(observations))
        assert scenario.created_at_ns == cutoff + 20
        assert scenario.computed_at_ns == scenario.available_at_ns == cutoff + 40
        count = len(repo.artifact_entries("PretradeExecutionScenarioV2"))
        observations = iter((cutoff + 50, case.candidate.deadline_ns))
        with pytest.raises(ValueError, match="deadline"):
            generate_pretrade_scenarios(repo, **args, clock_ns=lambda: next(observations))
        assert len(repo.artifact_entries("PretradeExecutionScenarioV2")) == count
