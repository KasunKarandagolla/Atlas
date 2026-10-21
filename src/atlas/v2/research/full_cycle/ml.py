"""ML inventory, ML-ready research tables and chronological model comparison.

The ML work is bounded by what the evidence can support. Where a matured
after-cost label does not exist, the row is persisted as an unlabelled
opportunity with its features, and no cash target is invented. Model
acquisition and inference qualification are recorded separately from model
quality.
"""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ...science.m0 import FEATURE_ORDER  # noqa: F401  (documented feature contract reference)
from ..full_cycle.models import (
    SELECTED_MODELS,
    download_checkpoint,
    resolve_checkpoint_identity,
)
from .causality import EvidenceGrade, action_qualification

ML_VERSION = "ATLAS_RED_FULL_CYCLE_ML_V2"

# Research features are computed only from information available at the
# original decision time. Units are explicit.
FEATURE_NAMES: tuple[str, ...] = (
    "atr14_bps",
    "realized_vol_bps",
    "adx14",
    "trend_score",
    "compression_ratio",
    "spread_bps",
    "turnover_log10_usd",
    "funding_rate_bps",
    "corr_btc",
    "beta_btc",
    "s6_relative_strength_score",
    "hour_of_day_utc",
    "return_bps",
    "regime_ema_separation",
)

TARGET_NAME = "net_cash_per_notional_bps"


def _f(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def build_ml_rows(store: Any) -> list[dict[str, Any]]:
    """Join actions, context, applicability and outcomes into ML-ready rows."""

    contexts: dict[str, list[Mapping[str, Any]]] = {}
    for row in store.query("SELECT * FROM market_context ORDER BY computed_at_ns"):
        contexts.setdefault(row["symbol"], []).append(row)
    actions = store.query("SELECT * FROM research_action")
    outcomes = {row["action_id"]: row for row in store.query("SELECT * FROM effective_outcome")}
    rows: list[dict[str, Any]] = []
    for action in actions:
        decision_ns = int(action.get("decision_at_ns") or int(action["deadline_ns"]) - 5_000_000_000)
        context = None
        best_cutoff = -1
        for candidate_row in contexts.get(action["symbol"], []):
            cutoff = candidate_row.get("information_cutoff_ns")
            available = candidate_row.get("available_at_ns")
            if (
                cutoff is not None
                and available is not None
                and int(cutoff) <= decision_ns
                and int(available) <= int(action["sealed_at_ns"])
                and int(candidate_row["computed_at_ns"]) <= int(available)
                and candidate_row.get("instrument_revision") == action.get("instrument_revision")
                and candidate_row.get("source_grade") == EvidenceGrade.PROSPECTIVE
                and int(cutoff) > best_cutoff
            ):
                best_cutoff, context = int(cutoff), candidate_row
        context = context or {}
        outcome = outcomes.get(action["action_id"])
        features = {
            "atr14_bps": _f(context.get("atr14_bps")),
            "realized_vol_bps": _f(context.get("realized_vol_bps")),
            "adx14": _f(context.get("adx14")),
            "trend_score": _f(context.get("trend_score")),
            "compression_ratio": _f(context.get("compression_ratio")),
            "spread_bps": _f(context.get("spread_bps")),
            "turnover_log10_usd": (
                math.log10(float(context["turnover_24h_usd"])) if _f(context.get("turnover_24h_usd")) else None
            ),
            "funding_rate_bps": (
                _f(context.get("funding_rate")) * 10_000 if _f(context.get("funding_rate")) is not None else None
            ),
            "corr_btc": _f(context.get("corr_btc")),
            "beta_btc": _f(context.get("beta_btc")),
            "s6_relative_strength_score": None,
            "hour_of_day_utc": (decision_ns % 86_400_000_000_000) / 3_600_000_000_000.0,
            "return_bps": _f(context.get("return_bps")),
            "regime_ema_separation": _f(context.get("trend_score")),
        }
        label = None
        label_state = "UNLABELLED"
        if outcome is not None:
            if (
                outcome["maturity_status"] == "MATURED"
                and outcome.get("net_cash") is not None
                and outcome.get("evidence_grade") == EvidenceGrade.MODELED
                and action_qualification(action)
                and context
                and outcome.get("label_available_at_ns") is not None
            ):
                notional = float(action["notional_usd"])
                label = float(outcome["net_cash"]) / notional * 10_000 if notional > 0 else None
                label_state = "MATURED_NET_CASH"
            else:
                label_state = f"CENSORED_{outcome['maturity_status']}"
        rows.append(
            {
                "action_id": action["action_id"],
                "cycle_id": action["cycle_id"],
                "symbol": action["symbol"],
                "strategy": action["strategy"],
                "side": action["side"],
                "decision_at_ns": decision_ns,
                "decision_seal_at_ns": int(action["sealed_at_ns"]),
                "information_cutoff_ns": decision_ns,
                "horizon_end_ns": int(action["horizon_end_ns"]),
                "label_available_at_ns": None if outcome is None else outcome.get("label_available_at_ns"),
                "opportunity_id": action.get("opportunity_id") or action["action_id"],
                "instrument_revision": action.get("instrument_revision"),
                "evidence_grade": action.get("evidence_grade", EvidenceGrade.LEGACY),
                "feature_available_at_ns": context.get("available_at_ns"),
                "feature_missing_reason": None if context else "NO_ORIGINAL_TIME_FEATURE_PROVENANCE",
                "features": features,
                "label_bps": label,
                "label_state": label_state,
                "policy_hash": action["policy_hash"],
                "action_hash": action["action_id"],
            }
        )
    return rows


def chronological_split(
    rows: Sequence[Mapping[str, Any]], *, embargo_ns: int = 86_400_000_000_000
) -> dict[str, list[Mapping[str, Any]]]:
    """Causal chronological split with a horizon embargo between train and test."""

    if embargo_ns < 0:
        raise ValueError("EMBARGO_MUST_BE_NONNEGATIVE")
    ordered = sorted(rows, key=lambda row: int(row["information_cutoff_ns"]))
    if len(ordered) < 5:
        return {"train": [], "validation": [], "test": [], "embargo_ns": embargo_ns}
    # Boundaries are market-time blocks, so simultaneous symbols stay together.
    times = sorted({int(r["information_cutoff_ns"]) for r in ordered})
    vb, tb = times[int(len(times) * 0.6)], times[int(len(times) * 0.8)]
    folds = {"train": [], "validation": [], "test": []}
    for row in ordered:
        t = int(row["information_cutoff_ns"])
        name = "train" if t < vb else "validation" if t < tb else "test"
        end = row.get("label_available_at_ns")
        horizon = row.get("horizon_end_ns")
        if end is None or horizon is None:
            continue
        end = max(int(end), int(horizon))
        boundary = vb if name == "train" else tb if name == "validation" else None
        if boundary is not None and end + embargo_ns >= boundary:
            continue
        folds[name].append(row)
    # Remove all cross-boundary repeated opportunities; never choose a convenient variant.
    owners = {}
    for name, part in folds.items():
        for row in part:
            group = row.get("opportunity_id") or row.get("action_id")
            if not group:
                continue
            owners.setdefault(group, set()).add(name)
    for name, part in folds.items():
        folds[name] = [r for r in part if len(owners.get(r.get("opportunity_id") or r.get("action_id"), set())) == 1]
    return {**folds, "embargo_ns": embargo_ns}


def fit_and_compare(rows: Sequence[Mapping[str, Any]], *, seed: int = 23017) -> dict[str, Any]:
    """Compare a ridge mean model, a gradient-boosted challenger and the empirical mean.

    Only matured rows with a real after-cost label may be scored. A model that
    cannot be fitted because there is not enough matured support is reported as
    NOT ESTIMABLE, never as a zero.
    """

    matured = [
        row
        for row in rows
        if row["label_state"] == "MATURED_NET_CASH"
        and _f(row.get("label_bps")) is not None
        and row.get("evidence_grade") == EvidenceGrade.PROSPECTIVE
        and row.get("policy_hash")
        and row.get("instrument_revision")
        and row.get("feature_available_at_ns") is not None
        and int(row["feature_available_at_ns"]) <= int(row.get("decision_seal_at_ns", row["information_cutoff_ns"]))
    ]
    # One target per opportunity; variants never manufacture independent support.
    unique = {row["opportunity_id"]: row for row in matured}
    matured = list(unique.values())
    result: dict[str, Any] = {
        "version": ML_VERSION,
        "n_rows": len(rows),
        "n_matured": len(matured),
        "label_definition": TARGET_NAME,
        "split": None,
        "models": {},
    }
    if len(matured) < 30 or len({r.get("symbol") for r in matured}) < 3:
        result["status"] = "NOT_ESTIMABLE"
        result["reason"] = "INSUFFICIENT_MATURED_AFTER_COST_LABELS"
        return result
    split = chronological_split(matured)
    result["split"] = {name: len(value) for name, value in split.items() if isinstance(value, list)}

    def _matrix(part: Sequence[Mapping[str, Any]]) -> tuple[list[list[float]], list[float]]:
        matrix: list[list[float]] = []
        target: list[float] = []
        for row in part:
            vector: list[float] = []
            for name in FEATURE_NAMES:
                value = row["features"].get(name)
                vector.append(float("nan") if value is None else float(value))
            matrix.append(vector)
            target.append(float(row["label_bps"]))
        return matrix, target

    import numpy as np

    train_x, train_y = _matrix(split["train"])
    test_x, test_y = _matrix(split["test"])
    if len(test_x) < 5 or len(train_x) < 10 or len(split["validation"]) < 5:
        result["status"] = "NOT_ESTIMABLE"
        result["reason"] = "CHRONOLOGICAL_SPLIT_HAS_NO_TEST_OR_TRAIN_SUPPORT"
        return result
    train_mean = sum(train_y) / len(train_y)
    empirical_rmse = math.sqrt(sum((value - train_mean) ** 2 for value in test_y) / len(test_y))
    result["models"]["EMPIRICAL_MEAN"] = {
        "kind": "BASELINE",
        "train_mean_bps": train_mean,
        "test_rmse_bps": empirical_rmse,
        "test_mean_bps": sum(test_y) / len(test_y),
    }
    result["models"]["M0_ARITHMETIC_MEAN_NET_ACTION"] = dict(result["models"]["EMPIRICAL_MEAN"])
    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge  # noqa: PLC0415
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception:
        Ridge = None
    if Ridge is not None:
        model = make_pipeline(
            SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True),
            StandardScaler(),
            Ridge(alpha=1.0),
        )
        model.fit(np.array(train_x), np.array(train_y))
        predictions = model.predict(np.array(test_x))
        ridge_rmse = math.sqrt(float(np.mean((np.array(test_y) - predictions) ** 2)))
        result["models"]["RIDGE_MEAN"] = {
            "kind": "CHALLENGER",
            "test_rmse_bps": ridge_rmse,
            "test_mean_bps": float(np.mean(predictions)),
            "delta_rmse_vs_empirical": ridge_rmse - empirical_rmse,
        }
    try:
        import lightgbm  # noqa: PLC0415

        booster = lightgbm.LGBMRegressor(
            objective="regression_l2",
            n_estimators=120,
            learning_rate=0.05,
            num_leaves=15,
            min_child_samples=5,
            random_state=seed,
            n_jobs=1,
            verbose=-1,
        )
        booster.fit(np.array(train_x), np.array(train_y))
        predictions = booster.predict(np.array(test_x))
        lgbm_rmse = math.sqrt(float(np.mean((np.array(test_y) - predictions) ** 2)))
        result["models"]["LIGHTGBM_L2_CASH_MEAN"] = {
            "kind": "CHALLENGER",
            "test_rmse_bps": lgbm_rmse,
            "test_mean_bps": float(np.mean(predictions)),
            "delta_rmse_vs_empirical": lgbm_rmse - empirical_rmse,
            "feature_importance": {
                name: float(value)
                for name, value in zip(FEATURE_NAMES, booster.feature_importances_.tolist(), strict=True)
            },
        }
    except Exception as exc:
        result["models"]["LIGHTGBM_L2_CASH_MEAN"] = {
            "kind": "CHALLENGER",
            "status": "BLOCKED_BY_ENVIRONMENT",
            "reason": f"{type(exc).__name__}: {exc}",
        }
    result["status"] = "HISTORICAL_DIAGNOSTIC"
    result["economic_authority"] = "NOT ESTIMABLE"
    result["dependence_unit"] = "UNIQUE_OPPORTUNITY_AND_COMMON_MARKET_TIME"
    result["note"] = "Predictive error does not establish after-cost edge or independent economic support."
    result["generated_at_ns"] = time.time_ns()
    return result


def foundation_inventory(cache_root: Path, *, download: bool = False) -> list[dict[str, Any]]:
    """Record the exact identity, revision and availability of the selected checkpoints."""

    rows: list[dict[str, Any]] = []
    for entry in SELECTED_MODELS:
        if entry["checkpoint"] is None:
            rows.append({
                "model_id": entry["model_id"], "checkpoint": None, "revision": None,
                "role": entry["role"], "load_status": "IN_REPOSITORY", "inference_status": "NOT_APPLICABLE",
                "detail": {"owner": entry["owner"], "kind": entry["kind"]},
            })
            continue
        checkpoint = entry["checkpoint"]
        identity = resolve_checkpoint_identity(checkpoint, cache_root)
        if download and not identity.weight_files:
            outcome = download_checkpoint(checkpoint, cache_root)
            identity = resolve_checkpoint_identity(checkpoint, cache_root)
            detail = {"download": outcome}
        else:
            detail = {"download": "NOT_ATTEMPTED"}
        rows.append({
            "model_id": entry["model_id"], "checkpoint": checkpoint,
            "revision": identity.revision or None, "role": entry["role"],
            "load_status": "ACQUIRED" if identity.weight_files else "NOT_ACQUIRED",
            "inference_status": "NOT_RUN",
            "detail": {**detail, "identity": identity.as_dict()},
        })
    return rows
