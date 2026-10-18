"""Quarantined price-target numerical reuse; never production MeanModelV1."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import gc
import math
import pathlib
import statistics
import time

import common
from causal_replay import BASE_FEATURES, CONTEXT_FEATURES, Market, feature_values

from atlas.v2.science.m0 import _solve

TARGET = "SIDE_SIGNED_DELAYED_4H_SIMPLE_PRICE_CHANGE_BPS"
MODEL_VERSION = "QUARANTINED_PRICE_RIDGE_V1"


def partition(row):
    p = common.protocol()["partitions"]
    at, available = int(row["cutoff_ns"]), int(row["label_available_ns"])
    if str(row["features_complete"]) != "1" or row["label_status"] != "OBSERVED":
        return "UNAVAILABLE"
    for left, right in p["train_origins"]:
        a, b = common.ns(left), common.ns(right)
        if a <= at < b - 48 * common.HOUR and available < b:
            return "TRAIN"
    a, b = map(common.ns, p["validation_origins"])
    if a <= at < b - 48 * common.HOUR and available < b:
        return "VALIDATION"
    return "PURGED_OR_OUTSIDE"


def assert_training_boundary(rows, fit_cutoff, maximum_origin):
    if len({r["event_id"] for r in rows}) != len(rows):
        raise ValueError("DUPLICATE_TRAINING_EPISODE_WEIGHT")
    for r in rows:
        if int(r["label_available_ns"]) >= fit_cutoff or int(r["cutoff_ns"]) >= maximum_origin:
            raise ValueError("IMMATURE_OR_TEST_TRAINING_LABEL")
        if r.get("evidence_grade") != common.GRADE:
            raise ValueError("DIAGNOSTIC_TARGET_PROVENANCE_REQUIRED")


def fit_numeric(rows, names, *, estimator, fit_cutoff):
    """Same L2/scaler/solver as existing mean_models; separate price target."""
    assert_training_boundary(rows, fit_cutoff, common.ns("2025-05-01T00:00:00Z"))
    if estimator not in ("EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT") or len(rows) < 30:
        raise ValueError("FIXED_ESTIMATOR_OR_TRAINING_SUPPORT")
    expected = (*BASE_FEATURES, *CONTEXT_FEATURES) if estimator == "RIDGE_CONTEXT" else BASE_FEATURES
    if tuple(names) != expected:
        raise ValueError("UNREGISTERED_PRICE_FEATURE_ORDER")
    x = [tuple(float(r[n]) for n in names) for r in rows]
    y = [float(r["target_4h_bps"]) for r in rows]
    centers = [statistics.mean(v[j] for v in x) for j in range(len(names))]
    scales = [max(1e-9, statistics.pstdev(v[j] for v in x)) for j in range(len(names))]
    model = {
        "version": MODEL_VERSION,
        "estimator": estimator,
        "target": TARGET,
        "evidence_grade": common.GRADE,
        "feature_order": list(names),
        "centers": centers,
        "scales": scales,
        "support_min": [min(v[j] for v in x) for j in range(len(names))],
        "support_max": [max(v[j] for v in x) for j in range(len(names))],
        "training_count": len(rows),
        "training_refs_sha256": common.digest([r["event_id"] for r in rows]),
        "training_latest_label_available_ns": max(int(r["label_available_ns"]) for r in rows),
        "fit_replay_cutoff_ns": fit_cutoff,
        "mean": math.fsum(y) / len(y),
        "ridge": 1.0,
        "library_identity": "stdlib float64; existing atlas.v2.science.m0._solve",
        "serving_available_ns": common.ns("2025-06-29T00:00:01Z"),
        "expires_ns": common.ns("2025-10-01T00:00:00Z"),
        "bias_adjustment_bps": 0.0,
    }
    if estimator != "EMPIRICAL":
        xs = [(1.0, *[(v - c) / s for v, c, s in zip(row, centers, scales, strict=True)]) for row in x]
        n, width = len(xs), len(xs[0])
        matrix = [
            [sum(v[i] * v[j] for v in xs) / n + (1.0 if i == j and i > 0 else 0.0) for j in range(width)]
            for i in range(width)
        ]
        vector = [sum(v[i] * target for v, target in zip(xs, y, strict=True)) / n for i in range(width)]
        model["coefficients"] = _solve(matrix, vector)
    return model


def predict_numeric(model, values, *, at, validation=False):
    if model.get("target") != TARGET or model.get("evidence_grade") != common.GRADE:
        raise ValueError("PRICE_TARGET_MUST_NOT_BECOME_CASH_MODEL")
    if not validation and not model["serving_available_ns"] <= at < model["expires_ns"]:
        raise ValueError("MODEL_NOT_YET_AVAILABLE_OR_EXPIRED")
    if model["training_latest_label_available_ns"] >= at:
        raise ValueError("MODEL_LABEL_NOT_MATURE_AT_PREDICTION")
    if len(values) != len(model["feature_order"]) or any(v is None or not math.isfinite(v) for v in values):
        return None, "MISSING_OR_INVALID_FEATURES"
    if any(x < lo or x > hi for x, lo, hi in zip(values, model["support_min"], model["support_max"], strict=True)):
        return None, "OOD_TRAIN_ONLY_ENVELOPE"
    if model["estimator"] == "EMPIRICAL":
        prediction = model["mean"]
    else:
        x = (1.0, *[(v - c) / s for v, c, s in zip(values, model["centers"], model["scales"], strict=True)])
        prediction = sum(a * b for a, b in zip(x, model["coefficients"], strict=True))
    return prediction + model["bias_adjustment_bps"], "SUPPORTED"


def calibrate(model, rows):
    residuals, states = [], collections.Counter()
    available = common.ns("2025-06-29T00:00:01Z")
    for r in rows:
        if partition(r) != "VALIDATION" or int(r["label_available_ns"]) >= available:
            raise ValueError("INVALID_CALIBRATION_CHRONOLOGY")
        values = tuple(float(r[n]) for n in model["feature_order"])
        prediction, status = predict_numeric(model, values, at=int(r["cutoff_ns"]) + 6 * common.SECOND, validation=True)
        states[status] += 1
        if prediction is not None:
            residuals.append(float(r["target_4h_bps"]) - prediction)
    if len(residuals) < 30:
        raise ValueError("INSUFFICIENT_CALIBRATION_SUPPORT")
    bias = math.fsum(residuals) / len(residuals)
    centered = sorted(r - bias for r in residuals)

    def quantile(q):
        index = (len(centered) - 1) * q
        a, b = math.floor(index), math.ceil(index)
        return centered[a] + (centered[b] - centered[a]) * (index - a)

    model.update(
        bias_adjustment_bps=bias,
        residual_q05_bps=quantile(0.05),
        residual_q95_bps=quantile(0.95),
        validation_count=len(rows),
        validation_supported_count=len(residuals),
        validation_support_counts=dict(states),
        validation_refs_sha256=common.digest([r["event_id"] for r in rows]),
        residual_rmse_bps=math.sqrt(math.fsum(r * r for r in centered) / len(centered)),
        calibration_interpretation="Chronological additive bias/residual diagnostic; no probability or conditional coverage qualification",
    )
    return model


def development_rows(old_cache, new_cache):
    all_rows = []
    timing = []
    for window, year in (("A", 2024), ("B", 2025)):
        started = time.monotonic()
        market = Market(year, 6, range(2, 7), old_cache, new_cache)
        for old in common.read_csv(common.OLD / "v3-corrected" / (window + "_PRICE_PROXIES.csv.gz")):
            if old["study_status"] != "SOURCE_QUALIFIED_ALT_EVENT":
                continue
            at = int(old["cutoff_ns"])
            snapshot = market.snapshot(old["symbol"], at)
            try:
                base, context = feature_values(snapshot, old["side"], int(old["setup_cutoff_ns"]), old["setup_extreme"])
            except ValueError:
                base, context = (), None
            row = {
                "window": window,
                "symbol": old["symbol"],
                "event_id": old["event_id"],
                "cutoff_ns": at,
                "features_complete": int(bool(base and context)),
                "label_status": old["outcome_4h_status"],
                "target_4h_bps": old["proxy_4h_bps"],
                "label_available_ns": int(old["reference_ns"]) + 4 * common.HOUR + 6 * common.SECOND,
                "source_ref": snapshot.source_ref,
                "evidence_grade": common.GRADE,
            }
            row.update(dict.fromkeys((*BASE_FEATURES, *CONTEXT_FEATURES), ""))
            row.update(zip(BASE_FEATURES, base, strict=bool(base)))
            if context:
                row.update(zip(CONTEXT_FEATURES, context, strict=True))
            all_rows.append(row)
        timing.append({"window": window, "wall_seconds": time.monotonic() - started})
        market.state.cache_clear()
        market.s6.cache_clear()
        del market
        gc.collect()
    return all_rows, timing


def fit_all(old_cache, new_cache):
    common.verify_prior_inputs()
    started = time.monotonic()
    rows, timing = development_rows(old_cache, new_cache)
    for r in rows:
        r["partition"] = partition(r)
    common.write_csv(common.HERE / "DEVELOPMENT_ROWS.csv.gz", rows)
    train = [r for r in rows if r["partition"] == "TRAIN"]
    validation = [r for r in rows if r["partition"] == "VALIDATION"]
    models = {}
    for name, names in (
        ("EMPIRICAL", BASE_FEATURES),
        ("RIDGE_S1", BASE_FEATURES),
        ("RIDGE_CONTEXT", (*BASE_FEATURES, *CONTEXT_FEATURES)),
    ):
        model = fit_numeric(train, names, estimator=name, fit_cutoff=common.ns("2025-05-01T00:00:00Z"))
        models[name] = calibrate(model, validation)
    result = {
        "version": MODEL_VERSION,
        "target": TARGET,
        "evidence_grade": common.GRADE,
        "protocol_sha256": common.PROTOCOL_SHA,
        "development_rows_sha256": common.sha(common.HERE / "DEVELOPMENT_ROWS.csv.gz"),
        "counts": dict(collections.Counter(r["partition"] for r in rows)),
        "models": models,
        "training_volatility_median": statistics.median(float(r["own_24h_return_volatility"]) for r in train),
        "price_diagnostics_not_production_cash_models": True,
        "M1": "BLOCKED BY ENVIRONMENT: locked lightgbm==4.7.0 absent; not fitted",
        "action_cash_ML": "NOT ESTIMABLE: no qualified exact-action cash rows",
    }
    common.save(common.HERE / "PRICE_MODELS.json", result)
    common.save(
        common.HERE / "TRAINING_RESOURCES.json",
        {
            "actual_training_created_at": dt.datetime.now(dt.UTC).isoformat(),
            "wall_seconds": time.monotonic() - started,
            "stages": timing,
        },
    )
    return {
        "counts": result["counts"],
        "model_sha256": common.sha(common.HERE / "PRICE_MODELS.json"),
        "wall_seconds": time.monotonic() - started,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--old-cache", type=pathlib.Path, required=True)
    p.add_argument("--new-cache", type=pathlib.Path, required=True)
    args = p.parse_args()
    print(common.canonical(fit_all(args.old_cache, args.new_cache)))
