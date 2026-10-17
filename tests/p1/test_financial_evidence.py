from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.science.mean_calibration import (
    MeanCalibrationProtocolV1,
    MeanDependenceQualificationV1,
    MeanOOFPredictionV1,
    MeanSelectionCalibrationV2,
    SelectedPolicyResidualV2,
    calibrate_verified_selected_policy,
    verify_mean_calibration,
)
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import index_mean_artifact, read_mean_artifact
from atlas.v2.science.mean_models import MeanModelV1, MeanTrainingRowV1, fit_m0_mean
from atlas.v2.science.mean_research import SelectedPolicyResidualV1, calibrate_selected_policy
from atlas.v2.science.mean_scenario_evidence import (
    MeanScenarioSetV2,
    ScenarioMarketEvidenceV1,
    ScenarioRiskSupportV1,
    ScenarioSourceV1,
    verify_scenario_evidence,
)
from atlas.v2.science.mean_scenarios import MeanScenarioSetV1

from .conftest import H, T
from .test_mean_evaluation import run
from .test_mean_selection import request_response, validate


def test_a_neg_01_legacy_declared_zero_residuals_cannot_qualify():
    rows = tuple(
        SelectedPolicyResidualV1(
            H("model"), H("policy"), H("cell"), H(str(k)), k * 7 * DAY_NS, (k * 7 + 1) * DAY_NS, D("5"), D("5"), False
        )
        for k in range(30)
    )
    result = calibrate_selected_policy(
        rows,
        model_ref=H("model"),
        policy_ref=H("policy"),
        compatibility_ref=H("cell"),
        source_ref=H("invented"),
        cutoff_ns=T,
        expires_at_ns=T + DAY_NS,
        dependence_qualified=True,
        resamples=100,
    )
    assert result.status == "NOT ESTIMABLE" and result.allowance_cash is None


def test_b_neg_01_boolean_scenarios_without_evidence_cannot_enter_e(case):
    for ref, inp in tuple(case.inputs.items()):
        wire = inp.scenarios.to_dict()
        wire.pop("qualification_ref", None)
        wire["version"] = MeanScenarioSetV1.VERSION
        scenarios = MeanScenarioSetV1.from_dict(wire)
        scenarios = replace(scenarios, source_ref=H("invented-source"), probability_support=True, es_qualified=True)
        index_mean_artifact(case.repo, scenarios, available_at_ns=T - 1)
        case.inputs[ref] = replace(inp, scenarios=scenarios)
    assert not run(case).members


def read(case, ref, cls):
    return read_mean_artifact(case.repo, ref, cls, consumer_ns=T + 12)


def a_chain(case):
    inp = list(case.inputs.values())[1]
    proof = read(case, inp.calibration.qualification_ref, MeanDependenceQualificationV1)
    protocol = read(case, proof.protocol_ref, MeanCalibrationProtocolV1)
    residuals = tuple(read(case, r, SelectedPolicyResidualV2) for r in proof.residual_refs)
    return inp, proof, protocol, residuals


def bind_residual(case, inp, proof, residuals):
    proof = replace(proof, residual_refs=tuple(r.content_hash for r in residuals))
    index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
    calibration = replace(inp.calibration, qualification_ref=proof.content_hash, calibration_refs=proof.residual_refs)
    index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=calibration)
    return calibration


def assert_a_refuses(case, inp, proof, protocol, residuals):
    with pytest.raises(ValueError):
        calibrate_verified_selected_policy(
            case.repo,
            model=inp.model,
            protocol=protocol,
            residuals=residuals,
            cutoff_ns=inp.calibration.available_at_ns,
            expires_at_ns=inp.calibration.expires_at_ns,
        )
    forged = bind_residual(case, inp, proof, residuals)
    with pytest.raises(ValueError):
        verify_mean_calibration(
            case.repo,
            forged,
            model=inp.model,
            policy_ref=case.policy.content_hash,
            product_ref=inp.product.content_hash,
            cutoff_ns=T,
            now_ns=T + 10,
        )
    result = run(case)
    assert inp.candidate.content_hash not in {e.candidate_ref for e in result.members}


def test_a_neg_01_thirty_actual_in_sample_zero_residual_models(case):
    inp, proof, protocol, residuals = a_chain(case)
    forged = []
    for residual in residuals:
        prediction = read(case, residual.prediction_ref, MeanOOFPredictionV1)
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        training = tuple(read(case, r, MeanTrainingRowV1) for r in prediction.training_row_refs) + (row,)
        cutoff = row.label_available_at_ns + 1
        model = fit_m0_mean(
            training,
            compatibility_ref=inp.model.compatibility_ref,
            cutoff_ns=cutoff,
            available_at_ns=cutoff,
            expires_at_ns=T + DAY_NS,
            provenance="CAUSAL_REPLAY",
        )
        index_mean_artifact(case.repo, model, available_at_ns=model.available_at_ns)
        prediction = replace(
            prediction,
            model_ref=model.content_hash,
            training_row_refs=tuple(r.content_hash for r in training),
            prediction_cash=row.target_cash,
        )
        index_mean_artifact(case.repo, prediction, available_at_ns=prediction.available_at_ns)
        changed = replace(residual, prediction_ref=prediction.content_hash)
        index_mean_artifact(case.repo, changed, available_at_ns=changed.available_at_ns)
        forged.append(changed)
    assert len(forged) == 30
    assert_a_refuses(case, inp, proof, protocol, tuple(forged))


@pytest.mark.parametrize(
    "fault",
    [
        "future_model",
        "self_label",
        "overlap",
        "missing_model",
        "missing_outcome",
        "wrong_cash",
        "wrong_features",
        "wrong_cell",
        "protected",
        "late_prediction",
    ],
)
def test_a_neg_02_03_04_06_prediction_membership_and_chronology(case, fault):
    inp, proof, protocol, residuals = a_chain(case)
    first = residuals[0]
    prediction = read(case, first.prediction_ref, MeanOOFPredictionV1)
    model = read(case, prediction.model_ref, MeanModelV1)
    row = read(case, first.target_row_ref, MeanTrainingRowV1)
    if fault == "future_model":
        model = replace(model, available_at_ns=row.origin_ns + 1)
        index_mean_artifact(case.repo, model, available_at_ns=model.available_at_ns)
        prediction = replace(prediction, model_ref=model.content_hash)
    elif fault in ["self_label", "overlap"]:
        # Own target necessarily violates the causal fit membership/embargo.
        model = replace(model, training_refs=model.training_refs + (row.outcome_ref,))
        index_mean_artifact(case.repo, model, available_at_ns=model.available_at_ns)
        prediction = replace(
            prediction,
            model_ref=model.content_hash,
            training_row_refs=prediction.training_row_refs + (row.content_hash,),
        )
    elif fault == "missing_model":
        prediction = replace(prediction, model_ref=H("missing-model"))
    elif fault == "missing_outcome":
        row = replace(row, outcome_ref=H("unindexed-outcome"))
        index_mean_artifact(case.repo, row, available_at_ns=row.label_available_at_ns)
        first = replace(first, target_row_ref=row.content_hash)
    elif fault == "wrong_cash":
        prediction = replace(prediction, prediction_cash=D("999"))
    elif fault == "wrong_features":
        prediction = replace(prediction, features=(1.0, 7.0, 0.02, 4.0, 2.0, 0.5))
    elif fault == "wrong_cell":
        prediction = replace(prediction, scope_ref=H("different-size-liquidity-cell"))
    elif fault == "protected":
        from atlas.v2.science.mean_research import ProtectedMeanHoldoutV1

        hold = read(case, protocol.holdout_ref, ProtectedMeanHoldoutV1)
        # Changing the first reservation is not permitted even if a new object hashes correctly.
        hold = replace(hold, outcome_refs=(row.outcome_ref,))
        index_mean_artifact(case.repo, hold, available_at_ns=hold.reserved_at_ns)
        protocol = replace(protocol, holdout_ref=hold.content_hash)
        index_mean_artifact(case.repo, protocol, available_at_ns=protocol.registered_at_ns)
        prediction = replace(prediction, protocol_ref=protocol.content_hash)
    else:
        prediction = replace(prediction, available_at_ns=prediction.origin_ns + 1)
    index_mean_artifact(case.repo, prediction, available_at_ns=prediction.available_at_ns)
    changed = replace(first, prediction_ref=prediction.content_hash)
    index_mean_artifact(case.repo, changed, available_at_ns=changed.available_at_ns)
    assert_a_refuses(case, inp, proof, protocol, (changed,) + residuals[1:])


@pytest.mark.parametrize("fault", ["missing", "stale", "future", "membership", "blocks", "acf", "allowance"])
def test_a_neg_05_dependence_is_verified_not_declared(case, fault):
    inp, proof, protocol, residuals = a_chain(case)
    changes = {"missing": {"qualification_ref": H("unindexed-dependence")}}
    if fault == "missing":
        calibration = replace(inp.calibration, **changes[fault])
    else:
        delta = {
            "stale": {"expires_at_ns": T},
            "future": {"available_at_ns": T + 1},
            "membership": {"residual_refs": proof.residual_refs[:-1]},
            "blocks": {"effective_blocks": 10000},
            "acf": {"lag_correlations": (0.9, 0.9, 0.9)},
            "allowance": {"allowance_cash": D("100")},
        }[fault]
        proof = replace(proof, **delta)
        index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
        calibration = replace(inp.calibration, qualification_ref=proof.content_hash)
    index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
    with pytest.raises(ValueError):
        verify_mean_calibration(
            case.repo,
            calibration,
            model=inp.model,
            policy_ref=case.policy.content_hash,
            product_ref=inp.product.content_hash,
            cutoff_ns=T,
            now_ns=T + 10,
        )
    # A truncated selected cohort now fails completeness before statistical support.
    with pytest.raises(ValueError, match="INCOMPLETE_OR_OUTCOME_CHOSEN_SELECTED_POPULATION"):
        calibrate_verified_selected_policy(
            case.repo,
            model=inp.model,
            protocol=protocol,
            residuals=residuals[:19],
            cutoff_ns=inp.calibration.available_at_ns,
            expires_at_ns=inp.calibration.expires_at_ns,
        )


def test_a_pos_01_real_oof_different_fold_models_and_reopen(case):
    from atlas.v2.memory.repository import OpsRepository

    inp, proof, protocol, residuals = a_chain(case)
    origins = []
    folds = []
    for residual in residuals:
        prediction = read(case, residual.prediction_ref, MeanOOFPredictionV1)
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        model = read(case, prediction.model_ref, MeanModelV1)
        assert row.outcome_ref not in model.training_refs
        assert model.available_at_ns < prediction.origin_ns
        assert prediction.prediction_cash == row.target_cash
        origins.append(prediction.origin_ns)
        folds.append(model.content_hash)
    assert len(set(folds)) == 30 and inp.model.available_at_ns > max(origins)
    assert inp.model.content_hash not in folds
    e, q, r = request_response(case)
    path = case.repo.path
    case.repo.close()
    case.repo = OpsRepository(path)
    verify_mean_calibration(
        case.repo,
        inp.calibration,
        model=inp.model,
        policy_ref=case.policy.content_hash,
        product_ref=inp.product.content_hash,
        cutoff_ns=T,
        now_ns=T + 12,
    )
    assert validate(case, e, q, r).action_ref == r.action_ref
    assert MeanSelectionCalibrationV2.from_json(__import__("json").dumps(inp.calibration.to_dict())) == inp.calibration
    with pytest.raises(ValueError):
        from atlas.v2.science.mean_research import MeanSelectionCalibrationV1

        MeanSelectionCalibrationV1.from_dict(inp.calibration.to_dict())


@pytest.mark.parametrize(
    "fault",
    [
        "missing_source",
        "missing_execution",
        "missing_calibration",
        "stale",
        "future",
        "wrong_scope",
        "weights",
        "path_id",
        "background",
        "realised_future",
        "boolean_only",
        "missing_risk",
        "tail_count",
        "common_account",
    ],
)
def test_b_neg_01_through_08_qualified_sources_required_even_without_es(case, fault):
    inp = list(case.inputs.values())[1]
    s = inp.scenarios
    if fault in ["missing_source", "missing_execution", "missing_calibration"]:
        field = {
            "missing_source": "source_ref",
            "missing_execution": "execution_ref",
            "missing_calibration": "calibration_ref",
        }[fault]
        s = replace(s, **{field: H(fault)})
    elif fault in ["stale", "future", "wrong_scope"]:
        source = read(case, s.source_ref, ScenarioSourceV1)
        source = replace(
            source,
            **{
                "stale": {"expires_at_ns": T},
                "future": {"available_at_ns": T + 1},
                "wrong_scope": {"scope_ref": H("unqualified-scope")},
            }[fault],
        )
        index_mean_artifact(case.repo, source, available_at_ns=source.available_at_ns)
        s = replace(s, source_ref=source.content_hash)
    elif fault in ["weights", "path_id", "background"]:
        path = s.paths[0]
        if fault == "weights":
            # Same one-unit mass, different population and empirical weights.
            s = replace(
                s,
                paths=(
                    replace(path, probability=D(".5")),
                    replace(path, common_path_id=H("other-path"), probability=D(".5")),
                ),
            )
        elif fault == "path_id":
            s = replace(s, paths=(replace(path, common_path_id=H("other-path")),))
        else:
            s = replace(s, paths=(replace(path, background_pnl_cash=D("999")),))
    elif fault == "realised_future":
        source = read(case, s.source_ref, ScenarioSourceV1)
        point = source.templates[0].points[0]
        raw = read(case, point.source_ref, ScenarioMarketEvidenceV1)
        raw = replace(raw, observed_at_ns=T + 1, available_at_ns=T + 2)
        index_mean_artifact(case.repo, raw, available_at_ns=raw.available_at_ns)
        badpoint = replace(point, source_ref=raw.content_hash)
        source = replace(
            source, templates=(replace(source.templates[0], points=(badpoint,) + source.templates[0].points[1:]),)
        )
        index_mean_artifact(case.repo, source, available_at_ns=source.available_at_ns)
        s = replace(s, source_ref=source.content_hash)
    elif fault == "boolean_only":
        s = replace(s, qualification_ref=H("no-proof"), probability_support=True, es_qualified=True)
    elif fault == "missing_risk":
        s = replace(s, qualification_ref=H("missing-risk-support"))
    elif fault == "tail_count":
        risk = read(case, s.qualification_ref, ScenarioRiskSupportV1)
        risk = replace(risk, tail_observations=999)
        index_mean_artifact(case.repo, risk, available_at_ns=risk.available_at_ns)
        s = replace(s, qualification_ref=risk.content_hash)
    else:
        s = replace(s, account_snapshot_ref=H("different-common-portfolio"))
    index_mean_artifact(case.repo, s, available_at_ns=T - 1)
    case.inputs[inp.candidate.content_hash] = replace(inp, scenarios=s)
    assert inp.candidate.content_hash not in {e.candidate_ref for e in run(case).members}


def test_b_pos_01_verified_synthetic_paths_common_es_and_reopen(case):
    from atlas.v2.memory.repository import OpsRepository

    from .test_mean_evaluation import rebind_policy

    rebind_policy(case, replace(case.policy, require_es=True, risk_lambda=D("1")))
    inp = list(case.inputs.values())[1]
    risk = verify_scenario_evidence(
        case.repo,
        inp.scenarios,
        product_ref=inp.product.content_hash,
        policy=case.policy,
        snapshot=case.snapshot,
        compatibility_ref=inp.model.compatibility_ref,
        cutoff_ns=T,
        now_ns=T + 10,
    )
    assert risk.tail_observations == 5 and risk.effective_blocks >= 20
    source = read(case, inp.scenarios.source_ref, ScenarioSourceV1)
    assert all(p.at_ns < T for t in source.templates for p in t.points)
    assert all(p.at_ns > T for path in inp.scenarios.paths for p in path.points)
    path = case.repo.path
    case.repo.close()
    case.repo = OpsRepository(path)
    result = run(case)
    assert len(result.members) == 2
    assert all(e.risk_increment_cash < 0 and e.j_cash == e.score_cash - e.risk_increment_cash for e in result.members)
    assert MeanScenarioSetV2.from_dict(inp.scenarios.to_dict()) == inp.scenarios
    with pytest.raises(ValueError):
        MeanScenarioSetV1.from_dict(inp.scenarios.to_dict())


def test_evidence_body_corruption_and_late_expiry_reject_saved_b_decision(case, monkeypatch):
    e, q, r = request_response(case)
    inp = list(case.inputs.values())[1]
    source = case.repo.get_artifact(inp.scenarios.source_ref)
    original = case.repo.get_artifact
    body = dict(source.metadata)
    body["mean_artifact"] = dict(
        body["mean_artifact"], expires_at_ns=source.metadata["mean_artifact"]["expires_at_ns"] + 1
    )
    altered = replace(source, metadata=body)
    monkeypatch.setattr(case.repo, "get_artifact", lambda ref: altered if ref == source.artifact_ref else original(ref))
    with pytest.raises(ValueError):
        validate(case, e, q, r)


@pytest.mark.parametrize(
    "fault",
    ["late_calibration", "stale_calibration", "overlap", "wrong_weights", "wrong_execution_scope", "unqualified_delay"],
)
def test_b_calibration_and_execution_support_references_are_verified(case, fault):
    from atlas.v2.science.mean_scenario_evidence import ScenarioExecutionModelV1, ScenarioProbabilityCalibrationV1

    inp = list(case.inputs.values())[1]
    s = inp.scenarios
    if fault in ["wrong_execution_scope", "unqualified_delay"]:
        execution = read(case, s.execution_ref, ScenarioExecutionModelV1)
        execution = replace(
            execution,
            **(
                {"scope_ref": H("wrong-product-or-regime")}
                if fault == "wrong_execution_scope"
                else {"entry_delay_ns": 1}
            ),
        )
        index_mean_artifact(case.repo, execution, available_at_ns=execution.available_at_ns)
        s = replace(s, execution_ref=execution.content_hash)
    else:
        probability = read(case, s.calibration_ref, ScenarioProbabilityCalibrationV1)
        change = {
            "late_calibration": {"available_at_ns": T + 1},
            "stale_calibration": {"expires_at_ns": T},
            "overlap": {"validation_refs": probability.training_refs + probability.validation_refs},
            "wrong_weights": {"probabilities": (D(".1"),)},
        }[fault]
        probability = replace(probability, **change)
        index_mean_artifact(case.repo, probability, available_at_ns=probability.available_at_ns)
        s = replace(s, calibration_ref=probability.content_hash)
    index_mean_artifact(case.repo, s, available_at_ns=T - 1)
    with pytest.raises(ValueError):
        verify_scenario_evidence(
            case.repo,
            s,
            product_ref=inp.product.content_hash,
            policy=case.policy,
            snapshot=case.snapshot,
            compatibility_ref=inp.model.compatibility_ref,
            cutoff_ns=T,
            now_ns=T + 10,
        )


def test_weighted_selected_action_bootstrap_preserves_unequal_block_counts():
    from atlas.v2.science.mean_calibration import selected_policy_upper_cash

    groups = {0: [D("10")] * 100, **{k: [D("0")] for k in range(1, 30)}}
    result = selected_policy_upper_cash(groups, alpha=0.05, resamples=10000)
    # Unweighted block mean is 1/3; actual per-selected-action mean is 1000/129.
    assert result >= D("1000") / 129 and result <= 10


def test_nonzero_allowance_from_actual_folds_not_declared_predictions(case):
    from .evidence_fixtures import verified_calibration

    inp = list(case.inputs.values())[1]
    model, calibration, *_ = verified_calibration(case.repo, product=inp.product, policy=case.policy,
        compatibility_ref=H("original-nonzero-calibration-cell"), cash="5", now_ns=T,
        residual_errors=(D("1"),) * 30)
    assert calibration.allowance_cash == 1
    verify_mean_calibration(
        case.repo,
        calibration,
        model=model,
        policy_ref=case.policy.content_hash,
        product_ref=inp.product.content_hash,
        cutoff_ns=T,
        now_ns=T + 10,
    )
    proof = read(case, calibration.qualification_ref, MeanDependenceQualificationV1)
    # Even two mutually consistent understated payloads cannot beat mean error.
    proof = replace(proof, allowance_cash=D("0"))
    index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
    forged = replace(calibration, allowance_cash=D("0"), qualification_ref=proof.content_hash)
    index_mean_artifact(case.repo, forged, available_at_ns=forged.available_at_ns)
    with pytest.raises(ValueError, match="VALIDATION_UNAVAILABLE"):
        verify_mean_calibration(
            case.repo,
            forged,
            model=model,
            policy_ref=case.policy.content_hash,
            product_ref=inp.product.content_hash,
            cutoff_ns=T,
            now_ns=T + 10,
        )


def test_legacy_saved_capsule_stays_readable_but_cannot_grant_new_support():
    import json
    from pathlib import Path

    from atlas.v2.contracts import EligibleSetV1
    from atlas.v2.science.mean_research import MeanSelectionCalibrationV1

    capsule = json.loads(Path("docs/v2/transition-p1-quant-evidence/review-capsule.json").read_text())
    assert EligibleSetV1.from_dict(capsule["eligible_set"]).to_dict() == capsule["eligible_set"]
    for values in capsule["inputs"].values():
        assert MeanSelectionCalibrationV1.from_dict(values["calibration"]).to_dict() == values["calibration"]
        assert MeanScenarioSetV1.from_dict(values["scenarios"]).to_dict() == values["scenarios"]
