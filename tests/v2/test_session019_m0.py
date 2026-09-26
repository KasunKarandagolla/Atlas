"""Chronological M0 causal guarantees and versioned inference records."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import freeze_action
from atlas.v2.science.m0 import (
    FEATURE_ORDER,
    M0ArtifactV2,
    M0FeatureVectorV2,
    M0PredictionV2,
    M0TrainingRowV2,
    action_features,
    chronological_oof,
    chronological_oof_calibration,
    eligible_m0_targets,
    fit_m0,
)
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    OutcomeTargetV2,
    index_matured_outcome,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session014_core import KEY
from .test_session017_risk import CUTOFF, risk_case, size
from .test_session018_remediation import _payoff_case


def _row(decision: int, available: int, target: str, *, feature: float, salt: str) -> M0TrainingRowV2:
    refs = {name: sha256_json({salt: name}) for name in (
        "outcome", "action", "action-artifact", "policy", "compatibility")}
    features = (feature,) + (0.0,) * (len(FEATURE_ORDER) - 1)
    return M0TrainingRowV2(refs["outcome"], decision, decision + 10, available,
        refs["action"], refs["action-artifact"], refs["policy"], refs["compatibility"],
        features, Decimal(target), "COUNTERFACTUAL", "FULL_FILL")


def test_expanding_oof_uses_only_labels_available_strictly_before_each_cutoff():
    first = _row(100, 120, "1", feature=1.0, salt="one")
    late = _row(140, 250, "1000000", feature=2.0, salt="late")
    second = _row(200, 220, "2", feature=3.0, salt="two")
    third = _row(300, 320, "3", feature=4.0, salt="three")
    oof = chronological_oof((first, late, second, third), min_training_samples=1)
    by_ref = {row.outcome_ref: row for row in oof}
    assert by_ref[first.outcome_ref].training_row_refs == ()
    assert by_ref[second.outcome_ref].training_row_refs == (first.outcome_ref,)
    assert late.outcome_ref not in by_ref[second.outcome_ref].training_row_refs
    assert late.outcome_ref in by_ref[third.outcome_ref].training_row_refs
    assert all(ref != row.outcome_ref for row in oof for ref in row.training_row_refs)


def test_future_labels_do_not_rewrite_older_oof_predictions_or_residuals():
    rows = tuple(_row(i * 100, i * 100 + 20, str(i), feature=float(i), salt=str(i))
        for i in range(1, 7))
    original = chronological_oof(rows, min_training_samples=2)
    future = _row(10_000, 10_020, "-999999999999", feature=1e12, salt="future")
    appended = chronological_oof(rows + (future,), min_training_samples=2)
    original_rows = {row.outcome_ref: row for row in original}
    appended_rows = {row.outcome_ref: row for row in appended}
    assert all(original_rows[ref] == appended_rows[ref] for ref in original_rows)


def test_future_labels_do_not_improve_earlier_oof_calibration():
    rows = tuple(_row(i * 100, i * 100 + 20, str(i), feature=float(i), salt=f"cal-{i}")
        for i in range(1, 40))
    oof = chronological_oof(rows, min_training_samples=1)
    action_hash = rows[-1].action_hash
    archive_ref = sha256_json("calibration-archive")
    baseline = chronological_oof_calibration(action_hash=action_hash, training_cutoff_ns=10_000,
        oof_archive_ref=archive_ref, rows=oof, minimum_samples=10)
    future = _row(20_000, 20_020, "1e40", feature=1e40, salt="cal-future")
    extended = chronological_oof(rows + (future,), min_training_samples=1)
    after_append = chronological_oof_calibration(action_hash=action_hash, training_cutoff_ns=10_000,
        oof_archive_ref=archive_ref, rows=extended, minimum_samples=10)
    assert baseline.status == "OOF_CALIBRATED"
    assert baseline.chronological_oof_count == after_append.chronological_oof_count
    assert baseline.absolute_residual_q90 == after_append.absolute_residual_q90


def test_oof_rows_are_out_of_fold_and_insufficient_history_is_explicit():
    rows = (_row(100, 120, "10", feature=1.0, salt="one"),
            _row(200, 220, "100", feature=2.0, salt="two"))
    too_early = chronological_oof(rows, min_training_samples=3)
    assert all(row.prediction is None and row.residual is None and row.status.startswith("NOT_ESTIMABLE")
               for row in too_early)
    ready = chronological_oof(rows, min_training_samples=1)
    assert ready[1].training_row_refs == (rows[0].outcome_ref,)
    assert ready[1].prediction is not None
    assert ready[1].residual == ready[1].target - ready[1].prediction
    assert rows[1].outcome_ref not in ready[1].training_row_refs


def test_feature_schema_is_explicit_stable_and_admission_state_is_not_a_feature():
    refs = [sha256_json({"feature-ref": item}) for item in range(4)]
    vector = M0FeatureVectorV2("M0_ACTION_VALUE_FEATURES_V1", *refs[:4], 100,
        FEATURE_ORDER, (0.0,) * len(FEATURE_ORDER), (), refs[0], refs[1])
    assert vector.to_dict()["feature_order"] == list(FEATURE_ORDER)
    assert vector.content_hash == sha256_json(vector.to_dict())
    assert M0FeatureVectorV2.from_dict(vector.to_dict()) == vector
    assert not any("admission" in feature.lower() or "payoff" in feature.lower() for feature in FEATURE_ORDER)
    changed = M0FeatureVectorV2(vector.schema_version, vector.action_hash, vector.action_artifact_ref,
        vector.candidate_ref, vector.feature_artifact_ref, vector.information_cutoff_ns,
        vector.feature_order, (1.0,) + vector.values[1:], vector.missing_reasons,
        vector.policy_hash, vector.compatibility_key)
    assert changed.content_hash != vector.content_hash


def test_current_prediction_and_oof_hashes_bind_feature_schema_and_rows():
    refs = [sha256_json({"model-ref": item}) for item in range(6)]
    base = M0ArtifactV2(refs[0], refs[1], "M0_ACTION_VALUE_FEATURES_V1", "M0_FIXED_HUBER_RIDGE_CONFIG_V1",
        "M0_HUBER_RIDGE_ACTION_VALUE_V1", 100, (refs[2], refs[3]), "AVAILABLE_AT_DECISION_AT_OUTCOME_REF_ASC",
        (), (), (), 0.0, (("ridge", 1.0),), refs[4], refs[5], refs[0], 2, 120, "AVAILABLE", ())
    different_training = replace(base, training_row_refs=(refs[2], refs[3], refs[4]), sample_count=3)
    different_schema = replace(base, feature_schema_version="M0_ACTION_VALUE_FEATURES_V2")
    assert len({base.content_hash, different_training.content_hash, different_schema.content_hash}) == 3
    prediction_refs = [sha256_json({"prediction-ref": item}) for item in range(8)]
    prediction = M0PredictionV2(prediction_refs[0], prediction_refs[1], prediction_refs[2],
        prediction_refs[3], 100, 120, Decimal("1.5"), Decimal("0.4"), Decimal("0.001"),
        prediction_refs[4], prediction_refs[5], prediction_refs[6], prediction_refs[7], "AVAILABLE", ())
    changed_model = replace(prediction, model_ref=sha256_json("changed-real-training-fit"))
    changed_features = replace(prediction, feature_vector_ref=sha256_json("changed-current-feature-schema"))
    assert prediction.content_hash != changed_model.content_hash
    assert prediction.content_hash != changed_features.content_hash


@pytest.mark.parametrize(("depth", "state"), [("0", "NO_FILL"), ("10", "PARTIAL_FILL"), ("100", "FULL_FILL")])
def test_only_matured_executable_fill_states_are_m0_targets(tmp_path, depth, state):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, payoff, outcome = _payoff_case(repo, entry_depth=depth)
        assert payoff.status.value == state
        assert index_matured_outcome(repo, outcome) == outcome.content_hash
        assert eligible_m0_targets((outcome,), outcome.available_at_ns) == (outcome,)
        assert eligible_m0_targets((outcome,), outcome.available_at_ns - 1) == ()


@pytest.mark.parametrize("admission", [AdmissionStateV2.NO_TRADE, AdmissionStateV2.NOT_ESTIMABLE])
def test_exact_frozen_actions_rejected_economically_can_mature_as_counterfactual_targets(tmp_path, admission):
    with OpsRepository(tmp_path / f"{admission.value}.sqlite") as repo:
        _, action, payoff, outcome = _payoff_case(repo, admission_state=admission)
        assert action.action.action_hash == outcome.action_hash
        assert payoff.payoff is not None
        assert index_matured_outcome(repo, outcome) == outcome.content_hash
        assert eligible_m0_targets((outcome,), outcome.available_at_ns) == (outcome,)


def test_non_executable_diagnostic_is_never_an_m0_target(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, _, executable = _payoff_case(repo)
        diagnostic_ref = sha256_json("diagnostic-evidence")
        diagnostic = replace(executable, action_hash=None, action_artifact_ref=None,
            action_absence_reason="NON_EXECUTABLE_DIAGNOSTIC", admission_state=AdmissionStateV2.NOT_APPLICABLE,
            execution_state=ExecutionOutcomeStateV2.NOT_APPLICABLE, gross_payoff=None, fees=None,
            funding_cashflow=None, net_payoff=None, fill_quantity=None, requested_quantity=None,
            evidence_refs=(diagnostic_ref,), execution_evidence_ref=None, outcome_target=OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
            diagnostic_value=Decimal("0.02"), diagnostic_unit="FRACTION", diagnostic_evidence_ref=diagnostic_ref,
            actual_action_binding_ref=None)
        assert diagnostic.label_state == LabelStateV2.MATURED
        assert eligible_m0_targets((diagnostic,), diagnostic.available_at_ns) == ()


def test_fit_m0_persists_cutoff_feature_schema_and_returns_not_estimable_without_history(tmp_path, monkeypatch):
    from . import test_session017_risk as risk_module

    original_candidate = risk_module.candidate
    features = {}

    def candidate_with_feature(policy=S1_POLICY, key=KEY, **kwargs):
        item = original_candidate(policy, key, **kwargs)
        feature = FeatureArtifactV2(
            ArtifactEnvelope(1, f"m0-feature-{item.candidate_id}", CUTOFF, CUTOFF, "m0-fixture", ()),
            item.key, "M0_FIXTURE_FEATURES_V1", CUTOFF, CUTOFF,
            {"h4.ema20": FeatureValueV2(Decimal("100"), "PRICE"),
             "h1.ema20": FeatureValueV2(Decimal("100"), "PRICE"),
             "m15.ema20": FeatureValueV2(Decimal("100"), "PRICE")},
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
                feature.envelope.created_at_ns, feature.envelope.available_at_ns, {"feature": feature.to_dict()}))
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        earlier_vector = action_features(repo, action.content_hash, cutoff_ns=CUTOFF)
        original = features[case.candidate.snapshot_hash]
        future = replace(original, envelope=replace(original.envelope, content_hash="",
                artifact_id="m0-future-feature", created_at_ns=CUTOFF + 10, available_at_ns=CUTOFF + 10),
            information_cutoff_ns=CUTOFF + 10, confirmed_at_ns=CUTOFF + 10,
            values={**dict(original.values), "h4.ema20": FeatureValueV2(Decimal("1e100"), "PRICE")})
        repo.register_artifact(ArtifactIndexEntryV2(future.content_hash, "FeatureArtifactV2", future.content_hash,
            future.envelope.created_at_ns, future.envelope.available_at_ns, {"feature": future.to_dict()}))
        assert action_features(repo, action.content_hash, cutoff_ns=CUTOFF).content_hash == earlier_vector.content_hash
        model, prediction, support, calibration, oof = fit_m0(repo, action=action,
            candidate=case.candidate, candidate_set=case.candidate_set, cutoff_ns=CUTOFF,
            available_at_ns=CUTOFF + 1)
        assert model.action_hash == action.action.action_hash
        assert prediction.action_hash == action.action.action_hash
        assert prediction.status == "NOT_ESTIMABLE"
        assert prediction.expected_net_value is None
        assert support.eligible_sample_count == 0
        assert calibration.status == "NOT_ESTIMABLE"
        assert not oof
        assert repo.get_artifact(prediction.content_hash) is not None
        with pytest.raises(ValueError, match="support floors"):
            fit_m0(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
                cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1, min_training_samples=1,
                min_independent_support=1, min_oof_training_samples=1, min_oof_calibration_samples=1)
