import json
from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_models import (
    MeanModelV1,
    MeanSplitV1,
    eligible_mean_rows,
    fit_empirical_mean,
    fit_m0_mean,
    fit_m1_mean,
    predict_mean,
)

from .conftest import H, T, training_rows


@pytest.mark.parametrize("estimator", [fit_empirical_mean, fit_m0_mean])
def test_tail_mean_99_plus_one_extreme_loss(estimator):
    compat = H("tail-cell")
    rows = list(training_rows(compat, cash=D("1"), count=100, constant=True))
    rows[-1] = replace(rows[-1], target_cash=D("-200"))
    model = estimator(
        rows,
        compatibility_ref=compat,
        cutoff_ns=T - 100,
        available_at_ns=T - 99,
        expires_at_ns=T + DAY_NS,
        provenance="CAUSAL_REPLAY",
    )
    value = predict_mean(model, rows[0].features, compatibility_ref=compat, cutoff_ns=T, now_ns=T)
    assert value == D("-1.01")
    assert model.target.startswith("EXACT_ACTION_INCREMENTAL_TRADING_CASH")
    assert MeanModelV1.from_dict(json.loads(json.dumps(model.to_dict()))) == model


def test_actual_lightgbm_l2_fit_compact_inference_and_serialization(tmp_path):
    compat = H("m1-cell")
    rows = list(training_rows(compat, cash=D("1"), count=100, constant=True))
    rows[-1] = replace(rows[-1], target_cash=D("-200"))
    validation = training_rows(compat, cash=D("-1.01"), count=10, start_day=200, constant=True)
    model = fit_m1_mean(
        rows,
        compatibility_ref=compat,
        cutoff_ns=T - 100,
        available_at_ns=T - 99,
        expires_at_ns=T + DAY_NS,
        provenance="CAUSAL_REPLAY",
        validation_rows=validation,
    )
    prediction = predict_mean(model, rows[0].features, compatibility_ref=compat, cutoff_ns=T, now_ns=T)
    assert prediction == pytest.approx(D("-1.01"), abs=D(".000001"))
    assert model.parameters["objective"] == "regression_l2"
    assert model.library_identity == "lightgbm==4.7.0"
    path = tmp_path / "compact-model.json"
    path.write_text(json.dumps(model.to_dict()))
    restored = MeanModelV1.from_dict(json.loads(path.read_text()))
    assert predict_mean(restored, rows[0].features, compatibility_ref=compat, cutoff_ns=T, now_ns=T) == prediction
    repeat = fit_m1_mean(
        rows,
        compatibility_ref=compat,
        cutoff_ns=T - 100,
        available_at_ns=T - 99,
        expires_at_ns=T + DAY_NS,
        provenance="CAUSAL_REPLAY",
        validation_rows=validation,
    )
    assert repeat.content_hash == model.content_hash


def test_m1_nonlinear_prediction_matches_native_booster():
    import lightgbm as lgb
    import numpy as np

    compat = H("nonlinear")
    train = tuple(
        replace(r, target_cash=D("10") if r.features[1] > 7 else D("-3")) for r in training_rows(compat, count=100)
    )
    validation = tuple(
        replace(r, target_cash=D("10") if r.features[1] > 7 else D("-3"))
        for r in training_rows(compat, count=20, start_day=200)
    )
    model = fit_m1_mean(
        train,
        compatibility_ref=compat,
        cutoff_ns=T - 100,
        available_at_ns=T - 99,
        expires_at_ns=T + DAY_NS,
        provenance="CAUSAL_REPLAY",
        validation_rows=validation,
    )
    native = lgb.train(
        {
            "objective": "regression_l2",
            "num_leaves": model.parameters["num_leaves"],
            "max_depth": 4,
            "learning_rate": 0.1,
            "min_child_samples": 5,
            "seed": 6101,
            "num_threads": 1,
            "verbosity": -1,
            "deterministic": True,
            "force_col_wise": True,
        },
        lgb.Dataset(np.array([r.features for r in train]), label=np.array([float(r.target_cash) for r in train])),
        num_boost_round=32,
    )
    expected = native.predict(np.array([r.features for r in validation]), num_threads=1)
    actual = [
        float(predict_mean(model, r.features, compatibility_ref=compat, cutoff_ns=T, now_ns=T)) for r in validation
    ]
    assert actual == pytest.approx(expected)
    assert max(actual) > 8 and min(actual) < -2


def test_historical_artifact_identity_separation():
    from atlas.v2.science.m0 import M0_MODEL_VERSION
    from atlas.v2.science.m1 import OBJECTIVE

    assert "HUBER" in M0_MODEL_VERSION and OBJECTIVE == "regression_l1"
    with pytest.raises(ValueError):
        MeanModelV1.from_dict({"version": M0_MODEL_VERSION})


def test_future_immature_censored_diagnostic_and_incompatible_rows():
    compat = H("chronology")
    rows = training_rows(compat, count=6)
    rows = rows + (
        replace(rows[0], outcome_ref=H("future"), label_available_at_ns=T + 1),
        replace(rows[0], outcome_ref=H("diag"), provenance="DIAGNOSTIC"),
        replace(rows[0], outcome_ref=H("censor"), execution_state="CENSORED"),
        replace(rows[0], outcome_ref=H("wrongcell"), compatibility_ref=H("wrong")),
    )
    valid = eligible_mean_rows(rows, cutoff_ns=T, compatibility_ref=compat, provenance="CAUSAL_REPLAY")
    assert len(valid) == 6
    with pytest.raises(ValueError, match="duplicate"):
        eligible_mean_rows((rows[0], rows[0]), cutoff_ns=T, compatibility_ref=compat, provenance="CAUSAL_REPLAY")
    with pytest.raises(ValueError, match="revision"):
        replace(rows[0], revision=1)
    with pytest.raises(ValueError, match="chronology"):
        replace(rows[0], feature_available_at_ns=rows[0].origin_ns + 1)


def test_purged_calendar_embargo_and_fixed_holdout():
    rows = training_rows(H("split"), count=100)
    split = MeanSplitV1(30 * DAY_NS, 60 * DAY_NS, 90 * DAY_NS, 90 * DAY_NS)
    train, cal, test = split.partition(rows, fit_cutoff_ns=T)
    assert max(r.label_end_ns for r in train) < 30 * DAY_NS
    assert min(r.origin_ns for r in cal) >= 31 * DAY_NS
    assert min(r.origin_ns for r in test) >= 61 * DAY_NS
    assert all(r.origin_ns < split.holdout_start_ns for r in (*train, *cal, *test))
    assert split.partition(rows + training_rows(H("split"), start_day=500, count=10), fit_cutoff_ns=T) == (
        train,
        cal,
        test,
    )
    with pytest.raises(ValueError, match="preregistration"):
        replace(split, embargo_ns=0)


def test_support_and_stale_inference_refusal():
    compat = H("support")
    rows = training_rows(compat)
    model = fit_m0_mean(
        rows,
        compatibility_ref=compat,
        cutoff_ns=T - 100,
        available_at_ns=T - 99,
        expires_at_ns=T + 1,
        provenance="CAUSAL_REPLAY",
    )
    for features, cell, now in (
        (tuple(100.0 for _ in range(6)), compat, T),
        (rows[0].features, H("other"), T),
        (rows[0].features, compat, T + 1),
    ):
        with pytest.raises(ValueError, match="NOT ESTIMABLE"):
            predict_mean(model, features, compatibility_ref=cell, cutoff_ns=T, now_ns=now)
