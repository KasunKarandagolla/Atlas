"""Offline squared-loss estimators and compact, framework-free mean inference.

Historical m0/m1 artifacts remain Huber/L1. Every new fit includes the exact
cash target, compatibility cell, causal training refs and training-only scaler.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.science.mean_common import DAY_NS, MEAN_TARGET, ZERO, OfflineArtifact, unique

FEATURES = ("side_long", "log_notional", "stop_fraction", "horizon_hours", "origin_rank", "market_state")


@dataclass(frozen=True)
class MeanTrainingRowV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_TRAINING_ROW_V1"
    outcome_ref: str
    action_ref: str
    compatibility_ref: str
    feature_available_at_ns: int
    origin_ns: int
    label_end_ns: int
    label_available_at_ns: int
    features: tuple[float, ...]
    target_cash: Decimal
    provenance: str
    execution_state: str
    source_grade: str
    revision: int = 0
    target: str = MEAN_TARGET

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            len(self.features) != len(FEATURES)
            or not self.feature_available_at_ns <= self.origin_ns < self.label_end_ns <= self.label_available_at_ns
            or self.revision != 0
            or self.target != MEAN_TARGET
        ):
            raise ValueError("mean training chronology/target/revision invalid")
        if self.provenance not in (
            "ACTUAL_RECONCILED",
            "PROSPECTIVE_SHADOW",
            "CAUSAL_REPLAY",
            "DIAGNOSTIC",
            "CENSORED",
        ):
            raise ValueError("unknown training provenance")
        if self.execution_state not in ("NO_FILL", "PARTIAL_FILL", "FULL_FILL", "CENSORED"):
            raise ValueError("unknown fill evidence")
        if self.execution_state == "NO_FILL" and self.target_cash != 0:
            raise ValueError("no fill has zero trading cash; programme costs are separate")


def eligible_mean_rows(
    rows: Sequence[MeanTrainingRowV1], *, cutoff_ns: int, compatibility_ref: str, provenance: str
) -> tuple[MeanTrainingRowV1, ...]:
    if provenance not in ("ACTUAL_RECONCILED", "PROSPECTIVE_SHADOW", "CAUSAL_REPLAY"):
        raise ValueError("diagnostic/censored data cannot establish mean economics")
    unique(tuple(r.outcome_ref for r in rows))
    return tuple(
        sorted(
            (
                r
                for r in rows
                if r.compatibility_ref == compatibility_ref
                and r.label_available_at_ns < cutoff_ns
                and r.provenance == provenance
                and r.execution_state != "CENSORED"
                and r.source_grade in ("COMPLETE_ACTUAL", "QUALIFIED_MODELLED")
            ),
            key=lambda r: (r.origin_ns, r.outcome_ref),
        )
    )


@dataclass(frozen=True)
class MeanSplitV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_PURGED_CALENDAR_SPLIT_V1"
    train_end_ns: int
    calibration_end_ns: int
    test_end_ns: int
    holdout_start_ns: int
    embargo_ns: int = DAY_NS
    embargo_justification_ref: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.train_end_ns < self.calibration_end_ns < self.test_end_ns <= self.holdout_start_ns:
            raise ValueError("chronological calendar split required")
        if self.embargo_ns != DAY_NS and self.embargo_justification_ref is None:
            raise ValueError("nonbaseline embargo needs preregistration")

    def partition(
        self, rows: Sequence[MeanTrainingRowV1], *, fit_cutoff_ns: int
    ) -> tuple[tuple[MeanTrainingRowV1, ...], ...]:
        # All symbols use the same calendar. Labels crossing a boundary or
        # unavailable before that boundary are purged; holdout is never returned.
        bounds = (
            (0, self.train_end_ns),
            (self.train_end_ns + self.embargo_ns, self.calibration_end_ns),
            (self.calibration_end_ns + self.embargo_ns, self.test_end_ns),
        )
        return tuple(
            tuple(
                r
                for r in rows
                if start <= r.origin_ns
                and r.label_end_ns < end
                and r.label_available_at_ns < min(end, fit_cutoff_ns)
                and r.origin_ns < self.holdout_start_ns
            )
            for start, end in bounds
        )


@dataclass(frozen=True)
class MeanModelV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_PREFIT_MEAN_MODEL_V1"
    estimator: str
    compatibility_ref: str
    training_cutoff_ns: int
    available_at_ns: int
    expires_at_ns: int
    provenance: str
    training_refs: tuple[str, ...]
    feature_order: tuple[str, ...]
    centers: tuple[float, ...]
    scales: tuple[float, ...]
    support_min: tuple[float, ...]
    support_max: tuple[float, ...]
    parameters: FrozenMap
    library_identity: str
    target: str = MEAN_TARGET

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            self.estimator not in ("EMPIRICAL_MEAN_V1", "M0_MEAN_RIDGE_V1", "M1_MEAN_LIGHTGBM_L2_V1")
            or self.target != MEAN_TARGET
            or self.feature_order != FEATURES
            or not self.training_refs
            or not self.training_cutoff_ns <= self.available_at_ns < self.expires_at_ns
        ):
            raise ValueError("invalid prefit mean-model identity")
        unique(self.training_refs)
        if any(len(x) != len(FEATURES) for x in (self.centers, self.scales, self.support_min, self.support_max)):
            raise ValueError("model feature width mismatch")
        if any(x <= 0 for x in self.scales) or any(
            a > b for a, b in zip(self.support_min, self.support_max, strict=True)
        ):
            raise ValueError("invalid train-only scaler/support envelope")
        if self.provenance not in ("ACTUAL_RECONCILED", "PROSPECTIVE_SHADOW", "CAUSAL_REPLAY"):
            raise ValueError("unqualified model training provenance")
        if self.estimator == "M0_MEAN_RIDGE_V1":
            coefficients = self.parameters.get("coefficients", ())
            if (
                len(coefficients) != len(FEATURES) + 1
                or any(type(v) not in (int, float) or not math.isfinite(v) for v in coefficients)
                or self.parameters.get("objective") != "squared_loss"
            ):
                raise ValueError("invalid compact squared-loss coefficients")
        if self.estimator == "EMPIRICAL_MEAN_V1":
            if not Decimal(self.parameters.get("mean_cash", "NaN")).is_finite():
                raise ValueError("invalid empirical arithmetic mean")
        if self.estimator == "M1_MEAN_LIGHTGBM_L2_V1":
            trees = self.parameters.get("trees", ())
            if not 0 < len(trees) <= 64 or self.parameters.get("objective") != "regression_l2":
                raise ValueError("bounded L2 trees required")
            for tree in trees:
                _validate_tree(tree, 0)


def _validate_tree(node: FrozenMap, depth: int) -> None:
    if depth > 8:
        raise ValueError("compact tree exceeds inference depth budget")
    if "leaf_value" in node:
        if not math.isfinite(float(node["leaf_value"])):
            raise ValueError("invalid tree leaf")
        return
    if node.get("decision_type") != "<=" or not 0 <= node["split_feature"] < len(FEATURES):
        raise ValueError("unsupported categorical/tree split")
    if not math.isfinite(float(node["threshold"])):
        raise ValueError("invalid tree threshold")
    _validate_tree(node["left_child"], depth + 1)
    _validate_tree(node["right_child"], depth + 1)


def _fit_inputs(
    rows: Sequence[MeanTrainingRowV1], *, compatibility_ref: str, cutoff_ns: int, provenance: str, minimum_rows: int
) -> tuple[MeanTrainingRowV1, ...]:
    from atlas.v2.science.active_training import require_row_budget

    require_row_budget(None, rows, cutoff_ns=cutoff_ns)
    valid = eligible_mean_rows(rows, cutoff_ns=cutoff_ns, compatibility_ref=compatibility_ref, provenance=provenance)
    if minimum_rows < 2 or len(valid) < minimum_rows:
        raise ValueError("NOT ESTIMABLE: insufficient compatible mature training rows")
    return valid


def _artifact(
    rows: tuple[MeanTrainingRowV1, ...],
    estimator: str,
    compatibility_ref: str,
    cutoff_ns: int,
    available_at_ns: int,
    expires_at_ns: int,
    parameters: FrozenMap,
    library: str,
) -> MeanModelV1:
    centers = tuple(statistics.mean(r.features[i] for r in rows) for i in range(len(FEATURES)))
    scales = tuple(max(1e-9, statistics.pstdev(r.features[i] for r in rows)) for i in range(len(FEATURES)))
    return MeanModelV1(
        estimator,
        compatibility_ref,
        cutoff_ns,
        available_at_ns,
        expires_at_ns,
        rows[0].provenance,
        tuple(r.outcome_ref for r in rows),
        FEATURES,
        centers,
        scales,
        tuple(min(r.features[i] for r in rows) for i in range(len(FEATURES))),
        tuple(max(r.features[i] for r in rows) for i in range(len(FEATURES))),
        parameters,
        library,
    )


def fit_empirical_mean(
    rows: Sequence[MeanTrainingRowV1],
    *,
    compatibility_ref: str,
    cutoff_ns: int,
    available_at_ns: int,
    expires_at_ns: int,
    provenance: str,
    minimum_rows: int = 30,
) -> MeanModelV1:
    valid = _fit_inputs(
        rows, compatibility_ref=compatibility_ref, cutoff_ns=cutoff_ns, provenance=provenance, minimum_rows=minimum_rows
    )
    mean = sum((r.target_cash for r in valid), ZERO) / len(valid)
    return _artifact(
        valid,
        "EMPIRICAL_MEAN_V1",
        compatibility_ref,
        cutoff_ns,
        available_at_ns,
        expires_at_ns,
        FrozenMap({"mean_cash": str(mean)}),
        "stdlib-decimal",
    )


def fit_m0_mean(
    rows: Sequence[MeanTrainingRowV1],
    *,
    compatibility_ref: str,
    cutoff_ns: int,
    available_at_ns: int,
    expires_at_ns: int,
    provenance: str,
    ridge: float = 1.0,
    minimum_rows: int = 30,
) -> MeanModelV1:
    from atlas.v2.science.m0 import _solve

    if not math.isfinite(ridge) or ridge <= 0:
        raise ValueError("positive preregistered ridge required")
    valid = _fit_inputs(
        rows, compatibility_ref=compatibility_ref, cutoff_ns=cutoff_ns, provenance=provenance, minimum_rows=minimum_rows
    )
    blank = _artifact(
        valid,
        "M0_MEAN_RIDGE_V1",
        compatibility_ref,
        cutoff_ns,
        available_at_ns,
        expires_at_ns,
        FrozenMap({"coefficients": [0.0] * (len(FEATURES) + 1), "objective": "squared_loss"}),
        "stdlib-float64-pivoted-solver",
    )
    xs = [
        (1.0, *tuple((v - c) / s for v, c, s in zip(r.features, blank.centers, blank.scales, strict=True)))
        for r in valid
    ]
    n, width = len(xs), len(xs[0])
    matrix = [
        [sum(x[i] * x[j] for x in xs) / n + (ridge if i == j and i > 0 else 0) for j in range(width)]
        for i in range(width)
    ]
    vector = [sum(x[i] * float(r.target_cash) for x, r in zip(xs, valid, strict=True)) / n for i in range(width)]
    coefficients = _solve(matrix, vector)
    from dataclasses import replace

    return replace(
        blank, parameters=FrozenMap({"coefficients": coefficients, "ridge": ridge, "objective": "squared_loss"})
    )


def fit_m1_mean(
    rows: Sequence[MeanTrainingRowV1],
    *,
    compatibility_ref: str,
    cutoff_ns: int,
    available_at_ns: int,
    expires_at_ns: int,
    provenance: str,
    validation_rows: Sequence[MeanTrainingRowV1],
    minimum_rows: int = 30,
) -> MeanModelV1:
    import importlib.metadata

    if importlib.metadata.version("lightgbm") != "4.7.0":
        raise RuntimeError("BLOCKED BY ENVIRONMENT: locked lightgbm==4.7.0 required")
    import numpy as np

    from atlas.v2.science.m1 import _NativeLightGBMEstimator

    valid = _fit_inputs(
        rows, compatibility_ref=compatibility_ref, cutoff_ns=cutoff_ns, provenance=provenance, minimum_rows=minimum_rows
    )
    validation = _fit_inputs(
        validation_rows, compatibility_ref=compatibility_ref, cutoff_ns=cutoff_ns, provenance=provenance, minimum_rows=2
    )
    if max(r.label_available_at_ns for r in valid) + DAY_NS >= min(r.origin_ns for r in validation):
        raise ValueError("validation must follow mature training plus historical embargo")
    # Two preregistered configurations, chronological validation only. No outer
    # holdout, random folds, winsorization, Huber or absolute-loss objective.
    x = np.array([r.features for r in valid], dtype=np.float64)
    y = np.array([float(r.target_cash) for r in valid], dtype=np.float64)
    vx = np.array([r.features for r in validation], dtype=np.float64)
    vy = np.array([float(r.target_cash) for r in validation], dtype=np.float64)
    candidates = []
    for leaves in (3, 7):
        model = _NativeLightGBMEstimator(
            {
                "objective": "regression_l2",
                "n_estimators": 32,
                "num_leaves": leaves,
                "max_depth": 4,
                "learning_rate": 0.1,
                "min_child_samples": 5,
                "seed": 6101,
                "num_threads": 1,
                "verbosity": -1,
                "deterministic": True,
                "force_col_wise": True,
            }
        )
        model.fit(x.tolist(), y.tolist())
        mse = float(np.mean((model.predict(vx.tolist()) - vy) ** 2))
        candidates.append((mse, leaves, model))
    mse, leaves, best = min(candidates, key=lambda c: (c[0], c[1]))
    trees = [t["tree_structure"] for t in best.booster_.dump_model()["tree_info"]]
    return _artifact(
        valid,
        "M1_MEAN_LIGHTGBM_L2_V1",
        compatibility_ref,
        cutoff_ns,
        available_at_ns,
        expires_at_ns,
        FrozenMap(
            {
                "trees": trees,
                "objective": "regression_l2",
                "num_leaves": leaves,
                "validation_mse": mse,
                "validation_refs": [r.outcome_ref for r in validation],
                "serialized_booster_hash": sha256_json(best.booster_.model_to_string()),
            }
        ),
        "lightgbm==4.7.0",
    )


def predict_mean(
    model: MeanModelV1, features: tuple[float, ...], *, compatibility_ref: str, cutoff_ns: int, now_ns: int
) -> Decimal:
    if (
        compatibility_ref != model.compatibility_ref
        or model.available_at_ns > cutoff_ns
        or model.training_cutoff_ns >= cutoff_ns
        or now_ns >= model.expires_at_ns
        or len(features) != len(FEATURES)
        or any(type(x) is not float or not math.isfinite(x) for x in features)
    ):
        raise ValueError("NOT ESTIMABLE: incompatible/stale model or features")
    if any(x < lo or x > hi for x, lo, hi in zip(features, model.support_min, model.support_max, strict=True)):
        raise ValueError("NOT ESTIMABLE: OOD action/size/liquidity support")
    if model.estimator == "EMPIRICAL_MEAN_V1":
        return Decimal(model.parameters["mean_cash"])
    if model.estimator == "M0_MEAN_RIDGE_V1":
        xs = (1.0, *tuple((v - c) / s for v, c, s in zip(features, model.centers, model.scales, strict=True)))
        prediction = sum(x * b for x, b in zip(xs, model.parameters["coefficients"], strict=True))
    else:
        prediction = 0.0
        for tree in model.parameters["trees"]:
            node = tree
            for _ in range(9):
                if "leaf_value" in node:
                    prediction += node["leaf_value"]
                    break
                node = (
                    node["left_child"] if features[node["split_feature"]] <= node["threshold"] else node["right_child"]
                )
    if not math.isfinite(prediction):
        raise ValueError("NOT ESTIMABLE: nonfinite mean prediction")
    return Decimal(str(prediction))
