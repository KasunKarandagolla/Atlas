"""Offline LightGBM action-value challenger with chronological evidence only.

The module deliberately has no module-level LightGBM import.  The optional
``offline-research`` environment owns that dependency; the live writer can
import the rest of ATLAS without importing or installing LightGBM.
"""

from __future__ import annotations

import importlib.metadata
import math
import platform
import statistics
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import json_value, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import ActionArtifactV2
from atlas.v2.science.m0 import (
    FEATURE_ORDER as M0_FEATURE_ORDER,
)
from atlas.v2.science.m0 import (
    M0FeatureVectorV2,
    M0OOFRowV2,
    action_features,
)
from atlas.v2.science.outcomes import (
    ExecutionOutcomeStateV2,
    MaturedOutcomeV2,
    executable_action_value_training_eligible,
    index_matured_outcome,
)

M1_POLICY_ID = "M1_LIGHTGBM_ACTION_VALUE_V1"
M1_POLICY_VERSION = "1.0.0-offline-research"
M1_FEATURE_SCHEMA_VERSION = "M1_FEATURE_VECTOR_V2_V1"
M1_TRAINING_ROW_VERSION = "M1_TRAINING_ROW_V2_V1"
M1_OOF_ROW_VERSION = "M1_OOF_ROW_V2_V1"
M1_MODEL_FIT_VERSION = "M1_MODEL_FIT_V2_V1"
M1_PREDICTION_VERSION = "M1_PREDICTION_V2_V1"
M1_CALIBRATION_VERSION = "M1_CALIBRATION_V2_V1"
M1_SUPPORT_VERSION = "M1_SUPPORT_V2_V1"
M1_OOD_VERSION = "M1_OOD_V2_V1"
M1_COMPARISON_VERSION = "M1_INCREMENTAL_COMPARISON_V2_V1"
M1_OOF_ARCHIVE_VERSION = "M1_CHRONOLOGICAL_OOF_ARCHIVE_V1"
M1_CHRONOLOGY_VERSION = "M1_CHRONOLOGY_V1"
M1_HOLDOUT_VERSION = "M1_FINAL_HOLDOUT_RESERVATION_V1"
DAY_NS = 86_400_000_000_000
HOUR_NS = 3_600_000_000_000
MIN_TRAINING_ROWS = 30
MIN_INDEPENDENT_SUPPORT = 20
MIN_CALIBRATION_ROWS = 30
OOF_EMBARGO_NS = 24 * HOUR_NS
SEED = 23017
THREAD_COUNT = 1
OBJECTIVE = "regression_l1"
VALIDATION_METRIC = "mean_absolute_error"
TIE_BREAK = "metric_ascending_then_num_leaves_ascending_then_n_estimators_ascending_then_canonical_params"

# A small declared subset of M0's action-aligned causal feature schema.  Each
# value has its M0 missingness bit; research/context extras are not auto-added.
M1_SOURCE_FEATURES = (
    "h4.ema20", "h4.ema50", "h4.adx14", "h1.roc10",
    "h1.realized_variance20", "h1.ewma_variance", "m15.rsi14", "m15.atr14",
    "candle.signed_body_atr", "candle.close_position",
    "regime.trend_state", "regime.volatility_state",
    "action.side_long", "action.quantity_log_notional", "action.entry_collar_fraction",
    "action.stop_distance_fraction", "action.horizon_hours", "action.policy_s1",
    "action.policy_s2", "action.selection_rank", "action.selection_rank_missing",
    "action.venue_bybit", "action.venue_binance",
)
M0_INDEX = {name: index for index, name in enumerate(M0_FEATURE_ORDER)}
M1_FEATURE_ORDER = tuple(
    name
    for feature in M1_SOURCE_FEATURES
    for name in (f"value:{feature}", f"missing:{feature}")
)
M1_FEATURE_POLICY_BODY = {
    "policy_id": M1_POLICY_ID,
    "version": M1_POLICY_VERSION,
    "feature_schema_version": M1_FEATURE_SCHEMA_VERSION,
    "source_feature_schema": "M0_ACTION_VALUE_FEATURES_V1",
    "feature_order": list(M1_FEATURE_ORDER),
    "feature_families": ["technical", "candle_geometry", "regime", "frozen_action"],
    "missingness": "M0_ZERO_IMPUTATION_WITH_EXPLICIT_PAIRED_MISSING_FLAG",
    "capital_authority": "ZERO",
}
M1_FEATURE_POLICY_HASH = sha256_json(M1_FEATURE_POLICY_BODY)

# Four preregistered configurations.  No random CV and no outer-test tuning.
M1_PARAMETER_GRID = (
    {"num_leaves": 7, "max_depth": -1, "n_estimators": 48, "learning_rate": 0.05, "min_child_samples": 5},
    {"num_leaves": 7, "max_depth": -1, "n_estimators": 96, "learning_rate": 0.05, "min_child_samples": 5},
    {"num_leaves": 15, "max_depth": -1, "n_estimators": 48, "learning_rate": 0.05, "min_child_samples": 5},
    {"num_leaves": 15, "max_depth": -1, "n_estimators": 96, "learning_rate": 0.05, "min_child_samples": 5},
)
M1_POLICY_BODY = {
    "policy_id": M1_POLICY_ID, "version": M1_POLICY_VERSION,
    "feature_policy_hash": M1_FEATURE_POLICY_HASH, "parameter_grid": list(M1_PARAMETER_GRID),
    "search_budget": len(M1_PARAMETER_GRID), "seed": SEED, "thread_count": THREAD_COUNT,
    "objective": OBJECTIVE, "validation_metric": VALIDATION_METRIC, "tie_break": TIE_BREAK,
    "chronology": "180D_TRAIN_30D_INNER_30D_OUTER_MONTHLY_THREE_WINDOWS_30D_UNTOUCHED_HOLDOUT",
    "preprocessing": "TRAINING_FOLD_MEDIAN_MAD_WITH_EXPLICIT_MISSINGNESS",
    "holdout": "IMMUTABLE_FIRST_RESEARCH_RESERVATION_NEVER_ROLLED_OR_REUSED_AFTER_SPENDING",
    "capital_authority": "ZERO", "model_voting": False,
}
M1_POLICY_HASH = sha256_json(M1_POLICY_BODY)


def _median_scale(rows: Sequence[Sequence[float]]) -> tuple[tuple[float, ...], tuple[float, ...]]:
    if not rows or any(len(row) != len(M1_FEATURE_ORDER) for row in rows):
        raise ValueError("M1 preprocessing requires non-empty, fixed-width training rows")
    centers = tuple(statistics.median(row[i] for row in rows) for i in range(len(M1_FEATURE_ORDER)))
    scales = tuple(
        max(1e-9, statistics.median(abs(row[i] - centers[i]) for row in rows) * 1.4826)
        for i in range(len(M1_FEATURE_ORDER))
    )
    return centers, scales


def _transform(row: Sequence[float], centers: Sequence[float], scales: Sequence[float]) -> list[float]:
    if len(row) != len(centers) or len(centers) != len(scales):
        raise ValueError("M1 feature/preprocessor width mismatch")
    return [(float(x) - float(m)) / float(s) for x, m, s in zip(row, centers, scales, strict=True)]


def _independent_rows(rows: Sequence[M1TrainingRowV2]) -> tuple[M1TrainingRowV2, ...]:
    chosen: list[M1TrainingRowV2] = []
    end = -1
    for row in sorted(rows, key=lambda item: (item.decision_at_ns, item.horizon_end_ns, item.outcome_ref)):
        if row.decision_at_ns >= end:
            chosen.append(row)
            end = row.horizon_end_ns
    return tuple(chosen)


@dataclass(frozen=True)
class M1FeatureVectorV2:
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    source_feature_ref: str
    information_cutoff_ns: int
    feature_order: tuple[str, ...]
    values: tuple[float, ...]
    missing_features: tuple[str, ...]
    policy_hash: str
    compatibility_key: str

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
                     "source_feature_ref", "policy_hash", "compatibility_key"):
            sha256_ref(getattr(self, name), field=name)
        if self.feature_order != M1_FEATURE_ORDER or len(self.values) != len(M1_FEATURE_ORDER):
            raise ValueError("M1 fixed feature schema/order mismatch")
        if any(not math.isfinite(value) for value in self.values):
            raise ValueError("M1 feature values must be finite")
        if tuple(sorted(set(self.missing_features))) != self.missing_features:
            raise ValueError("M1 missing feature names must be sorted and unique")
        if type(self.information_cutoff_ns) is not int or self.information_cutoff_ns < 0:
            raise ValueError("M1 information cutoff is invalid")

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_FEATURE_SCHEMA_VERSION, "action_hash": self.action_hash,
                "action_artifact_ref": self.action_artifact_ref, "candidate_ref": self.candidate_ref,
                "candidate_set_ref": self.candidate_set_ref, "source_feature_ref": self.source_feature_ref,
                "information_cutoff_ns": self.information_cutoff_ns,
                "feature_order": list(self.feature_order), "values": list(self.values),
                "missing_features": list(self.missing_features), "policy_hash": self.policy_hash,
                "compatibility_key": self.compatibility_key, "feature_policy_hash": M1_FEATURE_POLICY_HASH}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def project_m0_features(vector: M0FeatureVectorV2, *, candidate_set_ref: str) -> M1FeatureVectorV2:
    """Project the preregistered subset, retaining one missingness bit per value."""
    sha256_ref(candidate_set_ref, field="candidate_set_ref")
    index = {name: i for i, name in enumerate(M0_FEATURE_ORDER)}
    output: list[float] = []
    missing: list[str] = []
    for feature in M1_SOURCE_FEATURES:
        value_index = index[feature if feature.startswith("action.") else f"value:{feature}"]
        missing_value = 0.0 if feature.startswith("action.") else vector.values[index[f"missing:{feature}"]]
        output.extend((vector.values[value_index], missing_value))
        if missing_value != 0:
            missing.append(feature)
    return M1FeatureVectorV2(vector.action_hash, vector.action_artifact_ref, vector.candidate_ref,
        candidate_set_ref, vector.feature_artifact_ref, vector.information_cutoff_ns, M1_FEATURE_ORDER,
        tuple(output), tuple(sorted(missing)), vector.policy_hash, vector.compatibility_key)


@dataclass(frozen=True)
class M1TrainingRowV2:
    outcome_ref: str
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    feature_ref: str
    source_feature_ref: str
    policy_hash: str
    compatibility_key: str
    venue: str
    product: str
    decision_at_ns: int
    horizon_end_ns: int
    label_available_at_ns: int
    features: tuple[float, ...]
    missing_features: tuple[str, ...]
    target_net_value: Decimal
    provenance: str
    execution_state: str
    requested_quantity: Decimal
    fill_quantity: Decimal
    gross_payoff: Decimal
    fees: Decimal
    funding_cashflow: Decimal
    execution_evidence_ref: str

    def __post_init__(self) -> None:
        for name in ("outcome_ref", "action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
                     "feature_ref", "source_feature_ref", "policy_hash", "compatibility_key", "execution_evidence_ref"):
            sha256_ref(getattr(self, name), field=name)
        if not (self.decision_at_ns < self.horizon_end_ns <= self.label_available_at_ns):
            raise ValueError("M1 label is not fully matured/available")
        if len(self.features) != len(M1_FEATURE_ORDER) or any(not math.isfinite(x) for x in self.features):
            raise ValueError("M1 training feature width/non-finite value")
        if self.execution_state not in {ExecutionOutcomeStateV2.NO_FILL.value,
            ExecutionOutcomeStateV2.PARTIAL_FILL.value, ExecutionOutcomeStateV2.FULL_FILL.value}:
            raise ValueError("M1 requires an explicit matured fill/no-fill state")
        if self.provenance not in {"ACTUAL", "SIMULATED", "COUNTERFACTUAL"}:
            raise ValueError("M1 requires honest actual/simulated/counterfactual provenance")
        if any(not value.is_finite() for value in (self.target_net_value, self.gross_payoff, self.fees,
            self.funding_cashflow, self.fill_quantity, self.requested_quantity)) or self.fees < 0:
            raise ValueError("M1 requires finite target/cost/quantity components")
        if self.target_net_value != self.gross_payoff - self.fees + self.funding_cashflow:
            raise ValueError("M1 target must preserve gross, fee and funding components")
        if self.requested_quantity <= 0 or not 0 <= self.fill_quantity <= self.requested_quantity:
            raise ValueError("M1 fill quantities are inconsistent")
        if ((self.execution_state == "NO_FILL" and self.fill_quantity != 0)
            or (self.execution_state == "FULL_FILL" and self.fill_quantity != self.requested_quantity)
            or (self.execution_state == "PARTIAL_FILL" and not 0 < self.fill_quantity < self.requested_quantity)):
            raise ValueError("M1 explicit fill state contradicts its actual fill quantity")

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_TRAINING_ROW_VERSION, **{
            name: ([*value] if name in ("features", "missing_features") else
                   canonical_decimal_str(value) if isinstance(value, Decimal) else value)
            for name, value in self.__dict__.items()}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_m1_training_rows(repo: OpsRepository, *, cutoff_ns: int,
        compatibility_key: str | None = None, exclude_action_hash: str | None = None,
        decision_before_ns: int | None = None) -> tuple[M1TrainingRowV2, ...]:
    """Resolve only honest exact-action labels available by this training cutoff."""
    rows: list[M1TrainingRowV2] = []
    if exclude_action_hash is not None:
        sha256_ref(exclude_action_hash, field="exclude_action_hash")
    for entry in repo.artifact_entries("MaturedOutcomeV2"):
        if entry.available_at_ns > cutoff_ns:
            continue
        raw = entry.metadata.get("outcome")
        if not isinstance(raw, Mapping):
            continue
        if decision_before_ns is not None and int(raw.get("decision_at_ns", -1)) >= decision_before_ns:
            continue
        outcome = MaturedOutcomeV2.from_dict(json_value(raw))
        if entry.content_hash != outcome.content_hash or entry.available_at_ns != outcome.available_at_ns:
            raise ValueError("M1 matured outcome index/body mismatch")
        if not executable_action_value_training_eligible(outcome, cutoff_ns):
            continue
        if outcome.action_hash == exclude_action_hash:
            continue
        if index_matured_outcome(repo, outcome) != outcome.content_hash:
            raise ValueError("M1 label failed exact matured action/outcome validation")
        assert outcome.action_artifact_ref and outcome.action_hash and outcome.candidate_ref
        assert outcome.candidate_set_ref and outcome.execution_evidence_ref and outcome.net_payoff is not None
        assert outcome.requested_quantity is not None and outcome.fill_quantity is not None
        assert outcome.gross_payoff is not None and outcome.fees is not None and outcome.funding_cashflow is not None
        action_entry = repo.get_artifact(outcome.action_artifact_ref)
        identity = action_entry.metadata.get("action_identity") if action_entry else None
        key_data = identity.get("key") if isinstance(identity, Mapping) else None
        if not isinstance(key_data, Mapping):
            raise ValueError("M1 exact action instrument identity unavailable")
        key = InstrumentKeyV2.from_dict(key_data)
        m0_vector = action_features(repo, outcome.action_artifact_ref, cutoff_ns=cutoff_ns)
        if m0_vector.information_cutoff_ns != outcome.decision_at_ns or m0_vector.action_hash != outcome.action_hash:
            raise ValueError("M1 source feature evidence changed from the original decision cutoff")
        projected = project_m0_features(m0_vector, candidate_set_ref=outcome.candidate_set_ref)
        if compatibility_key is not None and projected.compatibility_key != compatibility_key:
            continue
        rows.append(M1TrainingRowV2(outcome.content_hash, outcome.action_hash, outcome.action_artifact_ref,
            outcome.candidate_ref, outcome.candidate_set_ref, projected.content_hash,
            projected.source_feature_ref, outcome.policy_hash,
            projected.compatibility_key, key.venue.value, key.product.value, outcome.decision_at_ns,
            outcome.horizon_end_ns, outcome.available_at_ns, projected.values, projected.missing_features,
            outcome.net_payoff, outcome.provenance.value, outcome.execution_state.value,
            outcome.requested_quantity, outcome.fill_quantity, outcome.gross_payoff, outcome.fees,
            outcome.funding_cashflow, outcome.execution_evidence_ref))
    return tuple(sorted(rows, key=lambda item: (item.label_available_at_ns, item.decision_at_ns, item.outcome_ref)))


@dataclass(frozen=True)
class M1OOFRowV2:
    outcome_ref: str
    action_hash: str
    candidate_ref: str
    candidate_set_ref: str
    fold_id: str
    training_cutoff_ns: int
    prediction_at_ns: int
    horizon_end_ns: int
    label_available_at_ns: int
    training_row_refs: tuple[str, ...]
    validation_row_refs: tuple[str, ...]
    prediction: Decimal | None
    target_net_value: Decimal
    residual: Decimal | None
    status: str

    def __post_init__(self) -> None:
        for name in ("outcome_ref", "action_hash", "candidate_ref", "candidate_set_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.training_cutoff_ns > self.prediction_at_ns or not (
            self.prediction_at_ns < self.horizon_end_ns <= self.label_available_at_ns
        ):
            raise ValueError("M1 OOF chronology invalid")
        if tuple(sorted(set(self.training_row_refs))) != self.training_row_refs:
            raise ValueError("M1 OOF training refs must be sorted/unique")
        if tuple(sorted(set(self.validation_row_refs))) != self.validation_row_refs:
            raise ValueError("M1 OOF validation refs must be sorted/unique")
        if (self.prediction is None) != (self.residual is None):
            raise ValueError("M1 OOF prediction/residual availability differs")
        if self.prediction is not None and self.residual != self.target_net_value - self.prediction:
            raise ValueError("M1 OOF residual does not match target")

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_OOF_ROW_VERSION, "outcome_ref": self.outcome_ref,
            "action_hash": self.action_hash, "candidate_ref": self.candidate_ref,
            "candidate_set_ref": self.candidate_set_ref, "fold_id": self.fold_id,
            "training_cutoff_ns": self.training_cutoff_ns, "prediction_at_ns": self.prediction_at_ns,
            "horizon_end_ns": self.horizon_end_ns, "label_available_at_ns": self.label_available_at_ns,
            "training_row_refs": list(self.training_row_refs), "validation_row_refs": list(self.validation_row_refs),
            "prediction": canonical_decimal_str(self.prediction) if self.prediction is not None else None,
            "target_net_value": canonical_decimal_str(self.target_net_value),
            "residual": canonical_decimal_str(self.residual) if self.residual is not None else None,
            "status": self.status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1CalibrationV2:
    action_hash: str
    cutoff_ns: int
    oof_archive_ref: str
    oof_row_refs: tuple[str, ...]
    independent_support: int
    absolute_residual_q90: Decimal | None
    status: str
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_CALIBRATION_VERSION, "action_hash": self.action_hash,
            "cutoff_ns": self.cutoff_ns, "oof_archive_ref": self.oof_archive_ref,
            "oof_row_refs": list(self.oof_row_refs), "independent_support": self.independent_support,
            "absolute_residual_q90": canonical_decimal_str(self.absolute_residual_q90)
                if self.absolute_residual_q90 is not None else None,
            "status": self.status, "reason": self.reason}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1SupportV2:
    action_hash: str
    cutoff_ns: int
    compatible_training_row_refs: tuple[str, ...]
    independent_training_row_refs: tuple[str, ...]
    provenance_counts: tuple[tuple[str, int], ...]
    execution_state_counts: tuple[tuple[str, int], ...]
    missing_feature_counts: tuple[tuple[str, int], ...]
    start_ns: int | None
    end_ns: int | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_SUPPORT_VERSION, "action_hash": self.action_hash,
            "cutoff_ns": self.cutoff_ns, "compatible_training_row_refs": list(self.compatible_training_row_refs),
            "independent_training_row_refs": list(self.independent_training_row_refs),
            "provenance_counts": [[key, value] for key, value in self.provenance_counts],
            "execution_state_counts": [[key, value] for key, value in self.execution_state_counts],
            "missing_feature_counts": [[key, value] for key, value in self.missing_feature_counts],
            "start_ns": self.start_ns, "end_ns": self.end_ns, "status": self.status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1OODV2:
    action_hash: str
    feature_ref: str
    training_row_refs: tuple[str, ...]
    fitted_center: tuple[float, ...]
    fitted_scale: tuple[float, ...]
    maximum_absolute_robust_z: float | None
    threshold: float
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_OOD_VERSION, "action_hash": self.action_hash, "feature_ref": self.feature_ref,
            "training_row_refs": list(self.training_row_refs), "fitted_center": list(self.fitted_center),
            "fitted_scale": list(self.fitted_scale), "maximum_absolute_robust_z": self.maximum_absolute_robust_z,
            "threshold": self.threshold, "status": self.status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1ModelFitV2:
    model_id: str
    model_version: str
    feature_policy_hash: str
    compatibility_key: str
    fit_cutoff_ns: int
    available_at_ns: int
    training_row_refs: tuple[str, ...]
    scaler_fit_row_refs: tuple[str, ...]
    validation_row_refs: tuple[str, ...]
    selected_parameters: tuple[tuple[str, int | float], ...]
    search_results: tuple[tuple[str, str, str | None], ...]
    centers: tuple[float, ...]
    scales: tuple[float, ...]
    booster_ref: str | None
    lightgbm_version: str | None
    python_version: str
    platform_id: str
    dependency_lock_hash: str
    seed: int
    thread_count: int
    objective: str
    validation_metric: str
    tie_break: str
    status: str
    reasons: tuple[str, ...]
    final_holdout_reservation_ref: str | None = None

    def __post_init__(self) -> None:
        if self.model_id != M1_POLICY_ID or self.model_version != M1_POLICY_VERSION:
            raise ValueError("M1 model identity changed")
        for name in ("feature_policy_hash", "compatibility_key", "dependency_lock_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.final_holdout_reservation_ref is not None:
            sha256_ref(self.final_holdout_reservation_ref, field="final_holdout_reservation_ref")
        if self.thread_count != 1 or self.seed != SEED or self.objective != OBJECTIVE:
            raise ValueError("M1 deterministic LightGBM configuration identity changed")
        if self.centers and (len(self.centers) != len(M1_FEATURE_ORDER) or len(self.scales) != len(M1_FEATURE_ORDER)):
            raise ValueError("M1 model preprocessor width mismatch")
        if tuple(sorted(set(self.reasons))) != self.reasons:
            raise ValueError("M1 reasons must be sorted and unique")

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_MODEL_FIT_VERSION, "model_id": self.model_id, "model_version": self.model_version,
            "feature_policy_hash": self.feature_policy_hash, "feature_policy_body": M1_FEATURE_POLICY_BODY,
            "model_policy_hash": M1_POLICY_HASH, "model_policy_body": M1_POLICY_BODY,
            "compatibility_key": self.compatibility_key, "fit_cutoff_ns": self.fit_cutoff_ns,
            "available_at_ns": self.available_at_ns, "training_row_refs": list(self.training_row_refs),
            "scaler_fit_row_refs": list(self.scaler_fit_row_refs), "validation_row_refs": list(self.validation_row_refs),
            "selected_parameters": [[key, value] for key, value in self.selected_parameters],
            "search_results": [[key, status, metric] for key, status, metric in self.search_results],
            "centers": list(self.centers), "scales": list(self.scales), "booster_ref": self.booster_ref,
            "lightgbm_version": self.lightgbm_version, "python_version": self.python_version,
            "platform_id": self.platform_id, "dependency_lock_hash": self.dependency_lock_hash,
            "seed": self.seed, "thread_count": self.thread_count, "objective": self.objective,
            "validation_metric": self.validation_metric, "tie_break": self.tie_break,
            "status": self.status, "reasons": list(self.reasons),
            "final_holdout_reservation_ref": self.final_holdout_reservation_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1PredictionV2:
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    information_cutoff_ns: int
    available_at_ns: int
    model_fit_ref: str
    feature_vector_ref: str
    training_row_refs: tuple[str, ...]
    oof_archive_ref: str
    calibration_ref: str
    support_ref: str
    ood_ref: str
    compatibility_key: str
    expected_net_value: Decimal | None
    status: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref", "model_fit_ref",
                     "feature_vector_ref", "oof_archive_ref", "calibration_ref", "support_ref", "ood_ref",
                     "compatibility_key"):
            sha256_ref(getattr(self, name), field=name)
        for ref in self.training_row_refs:
            sha256_ref(ref, field="training_row_refs")
        if self.information_cutoff_ns >= self.available_at_ns:
            raise ValueError("M1 prediction availability must follow its exact action cutoff")
        if tuple(sorted(set(self.reasons))) != self.reasons:
            raise ValueError("M1 prediction reasons must be sorted and unique")

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_PREDICTION_VERSION, **{
            name: canonical_decimal_str(value) if isinstance(value, Decimal) else
                  list(value) if isinstance(value, tuple) else value
            for name, value in self.__dict__.items()}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1IncrementalComparisonV2:
    action_hash: str
    m0_prediction_ref: str | None
    m1_prediction_ref: str
    common_outer_outcome_refs: tuple[str, ...]
    m0_outer_mae: Decimal | None
    m1_outer_mae: Decimal | None
    paired_error_difference: Decimal | None
    block_uncertainty_low: Decimal | None
    block_uncertainty_high: Decimal | None
    incremental_after_cost_policy_value: Decimal | None
    status: str
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"version": M1_COMPARISON_VERSION, **{
            name: canonical_decimal_str(value) if isinstance(value, Decimal) else list(value)
            if isinstance(value, tuple) else value for name, value in self.__dict__.items()}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M1WalkForwardWindowV2:
    fold_id: str
    training_start_ns: int
    training_end_ns: int
    validation_start_ns: int
    validation_end_ns: int
    outer_start_ns: int
    outer_end_ns: int
    training_refs: tuple[str, ...]
    validation_refs: tuple[str, ...]
    outer_refs: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {name: list(value) if isinstance(value, tuple) else value
                for name, value in self.__dict__.items()}


@dataclass(frozen=True)
class M1ChronologyV2:
    as_of_ns: int
    embargo_ns: int
    training_window_ns: int
    validation_window_ns: int
    outer_window_ns: int
    advance_ns: int
    windows: tuple[M1WalkForwardWindowV2, ...]
    final_holdout_refs: tuple[str, ...]
    holdout_state: str
    status: str
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"version": "M1_CHRONOLOGY_V1", "as_of_ns": self.as_of_ns, "embargo_ns": self.embargo_ns,
            "training_window_ns": self.training_window_ns, "validation_window_ns": self.validation_window_ns,
            "outer_window_ns": self.outer_window_ns, "advance_ns": self.advance_ns,
            "windows": [window.to_dict() for window in self.windows],
            "final_holdout_refs": list(self.final_holdout_refs), "holdout_state": self.holdout_state,
            "status": self.status, "reason": self.reason}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_walk_forward_chronology(rows: Sequence[M1TrainingRowV2], *, as_of_ns: int,
        embargo_ns: int = OOF_EMBARGO_NS, reserved_holdout_refs: Sequence[str] = ()) -> M1ChronologyV2:
    """Declare 180/30/30 monthly windows and reserve an untouched 30-day tail."""
    matured = tuple(row for row in rows if row.label_available_at_ns <= as_of_ns)
    if embargo_ns < max((row.horizon_end_ns - row.decision_at_ns for row in matured), default=0):
        raise ValueError("M1 embargo must be at least the maximum compatible policy holding horizon")
    training_ns, validation_ns, outer_ns = 180 * DAY_NS, 30 * DAY_NS, 30 * DAY_NS
    step_ns = 30 * DAY_NS
    total_ns = training_ns + validation_ns + 4 * outer_ns
    start = as_of_ns - total_ns
    windows: list[M1WalkForwardWindowV2] = []
    for index in range(3):
        train_start = start + index * step_ns
        train_end = train_start + training_ns
        val_start = train_end
        val_end = val_start + validation_ns
        outer_start = val_end
        outer_end = outer_start + outer_ns
        train = tuple(sorted((r for r in matured if train_start <= r.decision_at_ns < train_end
            and r.horizon_end_ns <= train_end - embargo_ns and r.label_available_at_ns <= train_end),
            key=lambda r: (r.decision_at_ns, r.outcome_ref)))
        validation = tuple(sorted((r for r in matured if val_start <= r.decision_at_ns < val_end
            and r.horizon_end_ns <= val_end - embargo_ns and r.label_available_at_ns <= outer_start),
            key=lambda r: (r.decision_at_ns, r.outcome_ref)))
        outer = tuple(sorted((r for r in matured if outer_start + embargo_ns <= r.decision_at_ns < outer_end
            and r.horizon_end_ns <= outer_end and r.label_available_at_ns <= outer_end),
            key=lambda r: (r.decision_at_ns, r.outcome_ref)))
        windows.append(M1WalkForwardWindowV2(f"OUTER_{index + 1}", train_start, train_end, val_start,
            val_end, outer_start, outer_end, tuple(r.outcome_ref for r in train),
            tuple(r.outcome_ref for r in validation), tuple(r.outcome_ref for r in outer)))
    holdout_start = as_of_ns - outer_ns
    holdout = tuple(sorted(set(reserved_holdout_refs) | {r.outcome_ref for r in matured
        if holdout_start <= r.decision_at_ns < as_of_ns and r.horizon_end_ns <= as_of_ns}))
    sufficient = all(len(w.training_refs) >= MIN_TRAINING_ROWS and len(w.validation_refs) >= 5
        and len(w.outer_refs) >= 5 for w in windows) and bool(holdout)
    return M1ChronologyV2(as_of_ns, embargo_ns, training_ns, validation_ns, outer_ns, step_ns,
        tuple(windows), holdout, "UNTOUCHED", "WALK_FORWARD_READY" if sufficient else "NOT_ESTIMABLE",
        None if sufficient else "INSUFFICIENT_180_30_30_OUTER_WINDOWS_OR_HOLDOUT")


def chronological_oof(rows: Sequence[M1TrainingRowV2], *, embargo_ns: int = OOF_EMBARGO_NS,
        minimum_training_rows: int = MIN_TRAINING_ROWS) -> tuple[M1OOFRowV2, ...]:
    """Expanding strictly-prior OOF. There is intentionally no shuffle/CV option."""
    if minimum_training_rows < MIN_TRAINING_ROWS or embargo_ns < max(
        (row.horizon_end_ns - row.decision_at_ns for row in rows), default=0):
        raise ValueError("M1 OOF cannot lower support floors or holding-horizon embargo")
    by_ref: dict[str, M1TrainingRowV2] = {}
    for row in rows:
        if row.outcome_ref in by_ref and by_ref[row.outcome_ref] != row:
            raise ValueError("M1 immutable outcome row cannot be revised in an OOF population")
        by_ref[row.outcome_ref] = row
    ordered = tuple(sorted(by_ref.values(), key=lambda row: (row.decision_at_ns, row.horizon_end_ns, row.outcome_ref)))
    output: list[M1OOFRowV2] = []
    for query in ordered:
        train = tuple(row for row in ordered if row.outcome_ref != query.outcome_ref
            and row.action_hash != query.action_hash
            and row.label_available_at_ns < query.decision_at_ns
            and row.horizon_end_ns <= query.decision_at_ns - embargo_ns
            and row.compatibility_key == query.compatibility_key)
        validation: tuple[M1TrainingRowV2, ...] = ()
        if len(train) < minimum_training_rows:
            output.append(M1OOFRowV2(query.outcome_ref, query.action_hash, query.candidate_ref,
                query.candidate_set_ref, "EXPANDING_OOF", query.decision_at_ns, query.decision_at_ns,
                query.horizon_end_ns, query.label_available_at_ns,
                tuple(sorted(row.outcome_ref for row in train)), (), None, query.target_net_value, None,
                "NOT_ESTIMABLE_INSUFFICIENT_CHRONOLOGICAL_HISTORY"))
            continue
        # A deterministic latest-period validation block is used only inside
        # the earlier prefix. The outer windows use the frozen 180/30/30 schedule.
        ordered_train = tuple(sorted(train, key=lambda row: (row.decision_at_ns, row.outcome_ref)))
        split = max(minimum_training_rows, int(len(ordered_train) * 0.8))
        val_start = ordered_train[split].decision_at_ns if split < len(ordered_train) else query.decision_at_ns
        earlier = tuple(row for row in ordered_train[:split]
            if row.horizon_end_ns <= val_start - embargo_ns)
        validation = tuple(row for row in ordered_train[split:]
            if row.label_available_at_ns < query.decision_at_ns and row.horizon_end_ns <= query.decision_at_ns - embargo_ns)
        if len(earlier) < minimum_training_rows or not validation:
            output.append(M1OOFRowV2(query.outcome_ref, query.action_hash, query.candidate_ref,
                query.candidate_set_ref, "EXPANDING_OOF", query.decision_at_ns, query.decision_at_ns,
                query.horizon_end_ns, query.label_available_at_ns,
                tuple(sorted(row.outcome_ref for row in train)), tuple(sorted(row.outcome_ref for row in validation)),
                None, query.target_net_value, None, "NOT_ESTIMABLE_PURGED_INNER_VALIDATION"))
            continue
        try:
            params, _, _, search = choose_parameters(earlier, validation)
            predictor, centers, scales = _fit_predictor(earlier, params)
            prediction = Decimal(str(float(predictor.predict([_transform(query.features, centers, scales)])[0])))
            output.append(M1OOFRowV2(query.outcome_ref, query.action_hash, query.candidate_ref,
                query.candidate_set_ref, "EXPANDING_OOF", query.decision_at_ns, query.decision_at_ns,
                query.horizon_end_ns, query.label_available_at_ns,
                tuple(sorted(row.outcome_ref for row in earlier)), tuple(sorted(row.outcome_ref for row in validation)),
                prediction, query.target_net_value, query.target_net_value - prediction, "OOF"))
        except ImportError:
            output.append(M1OOFRowV2(query.outcome_ref, query.action_hash, query.candidate_ref,
                query.candidate_set_ref, "EXPANDING_OOF", query.decision_at_ns, query.decision_at_ns,
                query.horizon_end_ns, query.label_available_at_ns,
                tuple(sorted(row.outcome_ref for row in earlier)), tuple(sorted(row.outcome_ref for row in validation)),
                None, query.target_net_value, None, "BLOCKED_BY_ENVIRONMENT_LIGHTGBM_UNAVAILABLE"))
        except Exception as exc:
            output.append(M1OOFRowV2(query.outcome_ref, query.action_hash, query.candidate_ref,
                query.candidate_set_ref, "EXPANDING_OOF", query.decision_at_ns, query.decision_at_ns,
                query.horizon_end_ns, query.label_available_at_ns,
                tuple(sorted(row.outcome_ref for row in earlier)), tuple(sorted(row.outcome_ref for row in validation)),
                None, query.target_net_value, None, f"FIT_FAILED:{type(exc).__name__}"))
    return tuple(output)


def walk_forward_oof(rows: Sequence[M1TrainingRowV2], chronology: M1ChronologyV2
        ) -> tuple[tuple[M1OOFRowV2, ...], tuple[Mapping[str, Any], ...]]:
    """Run the preregistered 180/30/30 outer folds; never fit on outer targets."""
    by_ref = {row.outcome_ref: row for row in rows}
    output: list[M1OOFRowV2] = []
    fits: list[Mapping[str, Any]] = []
    if chronology.status != "WALK_FORWARD_READY":
        return (), ({"status": "NOT_ESTIMABLE", "reason": chronology.reason},)
    for window in chronology.windows:
        train = tuple(by_ref[ref] for ref in window.training_refs)
        validation = tuple(by_ref[ref] for ref in window.validation_refs)
        outer = tuple(by_ref[ref] for ref in window.outer_refs)
        fit_rows = train + validation
        if any(row.label_available_at_ns > window.outer_start_ns or
            row.horizon_end_ns > window.outer_start_ns - chronology.embargo_ns for row in fit_rows):
            raise ValueError("M1 outer fold contains overlapping or future training labels")
        predictions: list[Decimal | None] = [None] * len(outer)
        fit_body: dict[str, Any] = {"fold_id": window.fold_id, "training_cutoff_ns": window.outer_start_ns,
            "training_refs": list(window.training_refs), "validation_refs": list(window.validation_refs),
            "outer_refs": list(window.outer_refs), "parameter_grid": list(M1_PARAMETER_GRID),
            "seed": SEED, "thread_count": THREAD_COUNT, "holdout_state": "UNTOUCHED"}
        try:
            parameters, _, _, search = choose_parameters(train, validation)
            model, centers, scales = _fit_predictor(fit_rows, parameters)
            raw = model.predict([_transform(row.features, centers, scales) for row in outer])
            predictions = [Decimal(str(float(value))) for value in raw]
            fit_body.update({"status": "TESTED", "parameters": parameters, "search_results": list(search),
                "scaler_fit_refs": [row.outcome_ref for row in fit_rows],
                "centers": list(centers), "scales": list(scales),
                "booster_sha256": sha256_json(model.booster_.model_to_string())})
        except ImportError:
            fit_body.update({"status": "BLOCKED BY ENVIRONMENT", "failure": "LIGHTGBM_UNAVAILABLE"})
        except Exception as exc:
            fit_body.update({"status": "NOT_ESTIMABLE", "failure": type(exc).__name__})
            if isinstance(exc, M1SearchFailure):
                fit_body["search_results"] = list(exc.results)
        fits.append(fit_body)
        for row, prediction in zip(outer, predictions, strict=True):
            output.append(M1OOFRowV2(row.outcome_ref, row.action_hash, row.candidate_ref,
                row.candidate_set_ref, window.fold_id, window.outer_start_ns, row.decision_at_ns,
                row.horizon_end_ns, row.label_available_at_ns,
                tuple(sorted(item.outcome_ref for item in fit_rows)), tuple(sorted(window.validation_refs)),
                prediction, row.target_net_value, row.target_net_value - prediction if prediction is not None else None,
                "OOF" if prediction is not None else "NOT_ESTIMABLE"))
    return tuple(output), tuple(fits)


def _lightgbm() -> Any:
    try:
        import lightgbm
    except ImportError as exc:  # pragma: no cover - depends on optional offline environment
        raise ImportError("BLOCKED BY ENVIRONMENT: LightGBM offline-research extra is not installed") from exc
    if lightgbm.__version__ != "4.7.0":
        raise RuntimeError(f"unsupported LightGBM version {lightgbm.__version__}; expected 4.7.0")
    return lightgbm


def _make_estimator(params: Mapping[str, int | float], *, seed: int = SEED) -> Any:
    fixed = {"objective": OBJECTIVE, "seed": seed, "num_threads": THREAD_COUNT,
        "deterministic": True, "force_col_wise": True, "verbosity": -1,
        "bagging_seed": seed, "feature_fraction_seed": seed, "data_random_seed": seed,
        "extra_seed": seed, "subsample": 1.0, "colsample_bytree": 1.0, "reg_lambda": 1.0}
    return _NativeLightGBMEstimator({**fixed, **dict(params)})


class _NativeLightGBMEstimator:
    """Small wrapper around LightGBM's native CPU API; no sklearn dependency."""

    def __init__(self, params: Mapping[str, Any]) -> None:
        self.params = dict(params)
        self.booster_: Any = None

    def fit(self, features: Sequence[Sequence[float]], targets: Sequence[float]) -> None:
        import numpy as np

        library = _lightgbm()
        params = dict(self.params)
        rounds = int(params.pop("n_estimators"))
        dataset = library.Dataset(np.asarray(features, dtype=np.float64), label=np.asarray(targets, dtype=np.float64))
        self.booster_ = library.train(params, dataset, num_boost_round=rounds)

    def predict(self, features: Sequence[Sequence[float]]) -> Any:
        import numpy as np

        return self.booster_.predict(np.asarray(features, dtype=np.float64), num_threads=THREAD_COUNT)


def _fit_predictor(rows: Sequence[M1TrainingRowV2], params: Mapping[str, int | float]) -> tuple[Any, tuple[float, ...], tuple[float, ...]]:
    centers, scales = _median_scale([row.features for row in rows])
    X = [_transform(row.features, centers, scales) for row in rows]
    y = [float(row.target_net_value) for row in rows]
    predictor = _make_estimator(params)
    predictor.fit(X, y)
    return predictor, centers, scales


class M1SearchFailure(RuntimeError):
    """Keep the complete bounded search ledger even when every fit fails."""

    def __init__(self, results: tuple[tuple[str, str, str | None], ...]) -> None:
        super().__init__("all preregistered LightGBM configurations failed")
        self.results = results


def choose_parameters(train_rows: Sequence[M1TrainingRowV2], validation_rows: Sequence[M1TrainingRowV2]
        ) -> tuple[dict[str, int | float], tuple[float, ...], tuple[float, ...], tuple[tuple[str, str, str | None], ...]]:
    """Run the exact small chronological search; scaler fits on train rows only."""
    if len(train_rows) < MIN_TRAINING_ROWS or not validation_rows:
        raise ValueError("M1 chronological search requires training and later validation rows")
    if max(row.decision_at_ns for row in train_rows) >= min(row.decision_at_ns for row in validation_rows):
        raise ValueError("M1 validation must follow every training decision")
    if any(row.label_available_at_ns >= min(row.decision_at_ns for row in validation_rows)
        for row in train_rows):
        raise ValueError("M1 training labels cross the validation fold boundary")
    centers, scales = _median_scale([row.features for row in train_rows])
    train_x = [_transform(row.features, centers, scales) for row in train_rows]
    val_x = [_transform(row.features, centers, scales) for row in validation_rows]
    y_train = [float(row.target_net_value) for row in train_rows]
    y_val = [float(row.target_net_value) for row in validation_rows]
    results: list[tuple[float, int, int, str, tuple[str, str, str | None]]] = []
    for config in M1_PARAMETER_GRID:
        config_id = sha256_json(dict(config))
        try:
            model = _make_estimator(config)
            model.fit(train_x, y_train)
            predictions = [float(value) for value in model.predict(val_x)]
            metric = sum(abs(actual - predicted) for actual, predicted in zip(y_val, predictions, strict=True)) / len(y_val)
            if not math.isfinite(metric):
                raise ValueError("non-finite validation metric")
            results.append((metric, int(config["num_leaves"]), int(config["n_estimators"]), config_id,
                (config_id, "PASS", format(metric, ".17g"))))
        except ImportError:
            raise
        except Exception as exc:
            results.append((math.inf, int(config["num_leaves"]), int(config["n_estimators"]), config_id,
                (config_id, f"FAILED:{type(exc).__name__}", None)))
    passed = [row for row in results if math.isfinite(row[0])]
    if not passed:
        raise M1SearchFailure(tuple(sorted(row[4] for row in results)))
    selected = min(passed, key=lambda row: (row[0], row[1], row[2], row[3]))
    params = next(dict(config) for config in M1_PARAMETER_GRID if sha256_json(dict(config)) == selected[3])
    return params, centers, scales, tuple(sorted(row[4] for row in results))


def chronological_oof_calibration(action_hash: str, cutoff_ns: int, archive_ref: str,
        rows: Sequence[M1OOFRowV2], *, minimum_rows: int = MIN_CALIBRATION_ROWS) -> M1CalibrationV2:
    eligible = tuple(row for row in rows if row.status == "OOF" and row.residual is not None
        and row.label_available_at_ns <= cutoff_ns and row.prediction_at_ns < cutoff_ns)
    independent_refs: list[str] = []
    last_end = -1
    for row in sorted(eligible, key=lambda item: (item.prediction_at_ns, item.horizon_end_ns, item.outcome_ref)):
        if row.prediction_at_ns >= last_end:
            independent_refs.append(row.content_hash)
            last_end = row.horizon_end_ns
    residuals = sorted(abs(row.residual) for row in eligible if row.content_hash in independent_refs and row.residual is not None)
    if len(residuals) < minimum_rows:
        return M1CalibrationV2(action_hash, cutoff_ns, archive_ref, tuple(independent_refs), len(residuals), None,
            "NOT_ESTIMABLE", "INSUFFICIENT_INDEPENDENT_CHRONOLOGICAL_OOF_RESIDUALS")
    q90 = residuals[math.ceil(0.90 * len(residuals)) - 1]
    return M1CalibrationV2(action_hash, cutoff_ns, archive_ref, tuple(independent_refs), len(residuals), q90,
        "OOF_CALIBRATED", None)


def persist_m1_artifact(repo: OpsRepository, kind: str, body: Mapping[str, Any], *, available_at_ns: int,
        key: str) -> str:
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, available_at_ns, available_at_ns,
        {key: json_value(body)}))
    return ref


def fit_lightgbm_fixture(rows: Sequence[M1TrainingRowV2], *, params_index: int = 0) -> tuple[str, tuple[float, ...], tuple[float, ...]]:
    """Fit the actual pinned LightGBM implementation; suitable for engineering fixtures only."""
    if params_index < 0 or params_index >= len(M1_PARAMETER_GRID):
        raise ValueError("M1 preregistered grid index out of range")
    predictor, centers, scales = _fit_predictor(rows, M1_PARAMETER_GRID[params_index])
    # The exact model representation is persisted by caller; returning it here
    # also makes deterministic round-trip behavior directly testable.
    return predictor.booster_.model_to_string(), centers, scales


def predict_lightgbm_fixture(model_text: str, row: Sequence[float]) -> float:
    library = _lightgbm()
    booster = library.Booster(model_str=model_text)
    import numpy as np

    value = float(booster.predict(np.asarray([list(row)], dtype=np.float64), num_threads=THREAD_COUNT)[0])
    if not math.isfinite(value):
        raise ValueError("LightGBM produced a non-finite action-value prediction")
    return value


def m1_environment(dependency_lock_hash: str) -> tuple[str, str, str]:
    sha256_ref(dependency_lock_hash, field="dependency_lock_hash")
    try:
        version = importlib.metadata.version("lightgbm")
    except importlib.metadata.PackageNotFoundError:
        version = "UNAVAILABLE"
    return version, sys.version.split()[0], f"{platform.system()}-{platform.machine()}"


@dataclass(frozen=True)
class M1RunV2:
    model_fit: M1ModelFitV2
    prediction: M1PredictionV2
    support: M1SupportV2
    calibration: M1CalibrationV2
    ood: M1OODV2
    chronology: M1ChronologyV2
    oof_rows: tuple[M1OOFRowV2, ...]


def _persist_training_feature(repo: OpsRepository, row: M1TrainingRowV2, *, available_at_ns: int) -> str:
    feature = M1FeatureVectorV2(row.action_hash, row.action_artifact_ref, row.candidate_ref,
        row.candidate_set_ref, row.source_feature_ref, row.decision_at_ns, M1_FEATURE_ORDER,
        row.features, row.missing_features, row.policy_hash, row.compatibility_key)
    if feature.content_hash != row.feature_ref:
        raise ValueError("M1 training feature ref does not reproduce from its immutable row")
    return persist_m1_artifact(repo, "M1FeatureVectorV2", feature.to_dict(),
        available_at_ns=available_at_ns, key="feature_vector")


def _current_support(rows: Sequence[M1TrainingRowV2], *, action_hash: str, cutoff_ns: int,
        training_artifact_refs: Mapping[str, str]) -> M1SupportV2:
    independent = _independent_rows(rows)
    provenance = {value: sum(row.provenance == value for row in rows) for value in {row.provenance for row in rows}}
    execution = {value: sum(row.execution_state == value for row in rows) for value in {row.execution_state for row in rows}}
    missing = {name: sum(name in row.missing_features for row in rows)
        for name in sorted({name for row in rows for name in row.missing_features})}
    refs = tuple(sorted(training_artifact_refs[row.outcome_ref] for row in rows))
    independent_refs = tuple(sorted(training_artifact_refs[row.outcome_ref] for row in independent))
    return M1SupportV2(action_hash, cutoff_ns, refs, independent_refs,
        tuple(sorted(provenance.items())), tuple(sorted(execution.items())), tuple(sorted(missing.items())),
        min((row.decision_at_ns for row in rows), default=None),
        max((row.decision_at_ns for row in rows), default=None),
        "SUPPORTED" if len(rows) >= MIN_TRAINING_ROWS and len(independent) >= MIN_INDEPENDENT_SUPPORT
        else "INSUFFICIENT")


def reserve_m1_final_holdout(repo: OpsRepository, *, compatibility_key: str,
        cutoff_ns: int, available_at_ns: int) -> tuple[int, int, str]:
    """Reserve once per model family/population; future calls cannot roll the tail."""
    sha256_ref(compatibility_key, field="compatibility_key")
    if cutoff_ns < 0 or available_at_ns < cutoff_ns:
        raise ValueError("M1 holdout reservation chronology invalid")
    index_ref = sha256_json({"version": "M1_HOLDOUT_DECISION_INDEX_V1", "policy_hash": M1_POLICY_HASH,
        "compatibility_key": compatibility_key})
    existing = repo.get_artifact(index_ref)
    if existing is not None:
        if existing.available_at_ns > available_at_ns:
            raise ValueError("M1 cannot use a future holdout reservation for an earlier fit")
        reservation_ref = str(existing.metadata["reservation_ref"])
        indexed = repo.get_artifact(reservation_ref)
        body = indexed.metadata.get("reservation") if indexed else None
        if not isinstance(body, Mapping) or sha256_json(body) != reservation_ref:
            raise ValueError("M1 immutable holdout reservation is missing or revised")
        if int(body["end_ns"]) > cutoff_ns:
            raise ValueError("M1 cannot use a future holdout plan for an earlier cutoff")
    else:
        body = {"version": M1_HOLDOUT_VERSION, "model_policy_hash": M1_POLICY_HASH,
            "compatibility_key": compatibility_key, "start_ns": max(0, cutoff_ns - 30 * DAY_NS),
            "end_ns": cutoff_ns, "state": "UNTOUCHED", "reserved_at_ns": available_at_ns}
        reservation_ref = persist_m1_artifact(repo, "M1FinalHoldoutReservationV2", body,
            available_at_ns=available_at_ns, key="reservation")
        index_body = {"reservation_ref": reservation_ref, "model_policy_hash": M1_POLICY_HASH,
            "compatibility_key": compatibility_key}
        repo.register_artifact(ArtifactIndexEntryV2(index_ref, "M1HoldoutDecisionIndexV1", sha256_json(index_body),
            available_at_ns, available_at_ns, index_body))
    if any(isinstance((state := entry.metadata.get("holdout_state")), Mapping)
        and state.get("holdout_ref") == reservation_ref and state.get("state") == "SPENT"
        for entry in repo.artifact_entries("DiscoveryHoldoutStateV2")):
        raise ValueError("M1 final holdout is SPENT; redesign requires a new declared family and fresh future evidence")
    return int(body["start_ns"]), int(body["end_ns"]), reservation_ref


def fit_m1(repo: OpsRepository, *, action: ActionArtifactV2, candidate: CandidateActionV2,
        candidate_set: CandidateSetV2, cutoff_ns: int, available_at_ns: int,
        dependency_lock_hash: str, historical_evidence_ref: str | None = None) -> M1RunV2:
    """Fit and persist the offline M1 challenger for an already frozen exact action.

    The function has no candidate selection, sizing, or action mutation input.
    The first declared final 30-day evidence tail remains fixed and untouched.
    """
    sha256_ref(dependency_lock_hash, field="dependency_lock_hash")
    if (action.action.action_hash != sha256_json(action.action.to_dict())
        or candidate.content_hash != action.candidate_ref
        or candidate_set.content_hash != action.candidate_set_ref
        or candidate_set.selected_candidate_id != candidate.candidate_id
        or candidate.decision_at_ns != cutoff_ns or action.available_at_ns > cutoff_ns
        or available_at_ns <= cutoff_ns or available_at_ns >= candidate.deadline_ns):
        raise ValueError("M1 requires the exact selected frozen action at its original decision cutoff")
    query_m0 = action_features(repo, action.content_hash, cutoff_ns=cutoff_ns)
    if query_m0.action_hash != action.action.action_hash:
        raise ValueError("M1 current feature evidence differs from the frozen action")
    query_feature = project_m0_features(query_m0, candidate_set_ref=candidate_set.content_hash)
    query_feature_ref = persist_m1_artifact(repo, "M1FeatureVectorV2", query_feature.to_dict(),
        available_at_ns=available_at_ns, key="feature_vector")

    holdout_start, holdout_end, holdout_ref = reserve_m1_final_holdout(repo,
        compatibility_key=query_feature.compatibility_key, cutoff_ns=cutoff_ns, available_at_ns=available_at_ns)
    rows_all = build_m1_training_rows(repo, cutoff_ns=holdout_start,
        compatibility_key=query_feature.compatibility_key, exclude_action_hash=action.action.action_hash,
        decision_before_ns=holdout_start)
    current_horizon_ns = action.action.horizon_end_ns - cutoff_ns
    embargo_ns = max(OOF_EMBARGO_NS, current_horizon_ns,
        max((row.horizon_end_ns - row.decision_at_ns for row in rows_all), default=0))
    compatible = tuple(row for row in rows_all if row.label_available_at_ns <= holdout_start
        and row.horizon_end_ns <= holdout_start - embargo_ns)
    current_training_rows = tuple(row for row in compatible if row.decision_at_ns >= holdout_start - 210 * DAY_NS)
    reserved = tuple(entry.content_hash for entry in repo.artifact_entries("MaturedOutcomeV2")
        if isinstance((body := entry.metadata.get("outcome")), Mapping)
        and body.get("policy_hash") == candidate.policy_hash
        and holdout_start <= int(body.get("decision_at_ns", -1)) < holdout_end
        and entry.available_at_ns <= cutoff_ns)
    chronology = build_walk_forward_chronology(rows_all, as_of_ns=holdout_end, embargo_ns=embargo_ns,
        reserved_holdout_refs=reserved)
    chronology_ref = persist_m1_artifact(repo, "M1ChronologyV2", chronology.to_dict(),
        available_at_ns=available_at_ns, key="chronology")
    _ = chronology_ref

    training_artifact_refs: dict[str, str] = {}
    for row in rows_all:
        _persist_training_feature(repo, row, available_at_ns=available_at_ns)
        training_artifact_refs[row.outcome_ref] = persist_m1_artifact(repo, "M1TrainingRowV2", row.to_dict(),
            available_at_ns=available_at_ns, key="training_row")
    support = _current_support(current_training_rows, action_hash=action.action.action_hash, cutoff_ns=cutoff_ns,
        training_artifact_refs=training_artifact_refs)
    support_ref = persist_m1_artifact(repo, "M1SupportV2", support.to_dict(),
        available_at_ns=available_at_ns, key="support")

    # OOF calibration sees only the pre-holdout prefix. No row from the final
    # 30-day tail is evaluated, selected, or used to fit preprocessing.
    oof_rows, outer_fits = walk_forward_oof(rows_all, chronology)
    oof_body = {"version": M1_OOF_ARCHIVE_VERSION, "feature_policy_hash": M1_FEATURE_POLICY_HASH,
        "rows": [row.to_dict() for row in oof_rows], "chronology_ref": chronology.content_hash,
        "holdout_state": "UNTOUCHED", "final_holdout_reservation_ref": holdout_ref, "outer_fold_fits": list(outer_fits)}
    oof_ref = persist_m1_artifact(repo, "M1OOFArchiveV2", oof_body,
        available_at_ns=available_at_ns, key="archive")
    calibration = chronological_oof_calibration(action.action.action_hash, holdout_start, oof_ref, oof_rows)
    calibration_ref = persist_m1_artifact(repo, "M1CalibrationV2", calibration.to_dict(),
        available_at_ns=available_at_ns, key="calibration")

    centers: tuple[float, ...] = ()
    scales: tuple[float, ...] = ()
    selected_parameters: tuple[tuple[str, int | float], ...] = ()
    search_results: tuple[tuple[str, str, str | None], ...] = ()
    booster_ref: str | None = None
    lightgbm_version, python_version, platform_id = m1_environment(dependency_lock_hash)
    reasons: list[str] = []
    expected_value: Decimal | None = None
    fit_status = "NOT_ESTIMABLE"
    historical = repo.get_artifact(historical_evidence_ref) if historical_evidence_ref is not None else None
    if (historical is None or historical.artifact_type != "QualifiedHistoricalEvidenceV2"
        or historical.available_at_ns > cutoff_ns or historical.metadata.get("synthetic") is not False
        or not {row.outcome_ref for row in rows_all}.issubset(set(historical.metadata.get("outcome_refs", ())))):
        reasons.append("GENUINE_HISTORICAL_CHRONOLOGY_UNVERIFIED")
    val_start = holdout_start - 30 * DAY_NS
    validation_rows = tuple(row for row in compatible if val_start <= row.decision_at_ns < holdout_start
        and row.horizon_end_ns <= holdout_start - embargo_ns)
    search_train_rows = tuple(row for row in compatible if holdout_start - 210 * DAY_NS <= row.decision_at_ns < val_start
        and row.horizon_end_ns <= val_start - embargo_ns and row.label_available_at_ns < val_start)
    if len(current_training_rows) < MIN_TRAINING_ROWS or len(_independent_rows(current_training_rows)) < MIN_INDEPENDENT_SUPPORT:
        reasons.append("INSUFFICIENT_MATURED_OR_INDEPENDENT_COMPATIBLE_ACTION_VALUE_SUPPORT")
    if chronology.status != "WALK_FORWARD_READY":
        reasons.append(chronology.reason or "INSUFFICIENT_REQUIRED_CHRONOLOGICAL_OUTER_WINDOWS")
    if len(outer_fits) < 3 or any(fold.get("status") != "TESTED" for fold in outer_fits):
        reasons.append("INSUFFICIENT_SUCCESSFUL_CHRONOLOGICAL_OUTER_FITS")
    if calibration.status != "OOF_CALIBRATED":
        reasons.append("INSUFFICIENT_CHRONOLOGICAL_OOF_CALIBRATION")
    if len(search_train_rows) < MIN_TRAINING_ROWS or len(validation_rows) < 5:
        reasons.append("INSUFFICIENT_180D_TRAIN_30D_VALIDATION_HISTORY_BEFORE_HOLDOUT")
    elif not reasons:
        try:
            params, _, _, search_results = choose_parameters(search_train_rows, validation_rows)
            final_rows = current_training_rows
            predictor, centers, scales = _fit_predictor(final_rows, params)
            raw_prediction = float(predictor.predict([_transform(query_feature.values, centers, scales)])[0])
            if not math.isfinite(raw_prediction):
                raise ValueError("non-finite M1 action-value prediction")
            expected_value = Decimal(str(raw_prediction))
            booster_text = predictor.booster_.model_to_string()
            booster_ref = persist_m1_artifact(repo, "M1LightGBMBoosterV2",
                {"version": "M1_LIGHTGBM_BOOSTER_V1", "model_text": booster_text,
                 "lightgbm_version": lightgbm_version, "seed": SEED, "thread_count": THREAD_COUNT,
                 "deterministic": True}, available_at_ns=available_at_ns, key="booster")
            selected_parameters = tuple(sorted((str(key), value) for key, value in params.items()))
            fit_status = "HISTORICAL_DIAGNOSTIC"
        except ImportError:
            reasons.append("BLOCKED_BY_ENVIRONMENT_LIGHTGBM_UNAVAILABLE")
            fit_status = "BLOCKED BY ENVIRONMENT"
        except Exception as exc:
            reasons.append(f"M1_FIT_FAILED_{type(exc).__name__}")
            if isinstance(exc, M1SearchFailure):
                search_results = exc.results
    if current_training_rows and not centers:
        centers, scales = _median_scale([row.features for row in current_training_rows])
    query_z = (max((abs((value - center) / scale) for value, center, scale in
        zip(query_feature.values, centers, scales, strict=True)), default=None) if centers else None)
    ood_status = ("NOT_ESTIMABLE" if query_z is None else "OOD" if query_z > 6.0 else "IN_DISTRIBUTION")
    ood = M1OODV2(action.action.action_hash, query_feature_ref,
        tuple(sorted(training_artifact_refs[row.outcome_ref] for row in current_training_rows)), centers, scales,
        query_z, 6.0, ood_status)
    ood_ref = persist_m1_artifact(repo, "M1OODV2", ood.to_dict(), available_at_ns=available_at_ns, key="ood")
    if calibration.status != "OOF_CALIBRATED":
        reasons.append("CHRONOLOGICAL_OOF_CALIBRATION_UNSUPPORTED")
    if ood_status != "IN_DISTRIBUTION":
        reasons.append("M1_CURRENT_ACTION_FEATURE_SUPPORT_UNAVAILABLE_OR_OOD")
    if support.status != "SUPPORTED":
        reasons.append("M1_COMPATIBLE_INDEPENDENT_SUPPORT_BELOW_FLOOR")
    reasons = sorted(set(reasons))
    if fit_status == "HISTORICAL_DIAGNOSTIC" and reasons:
        fit_status = "NOT_ESTIMABLE"
        expected_value = None
    if not reasons and fit_status == "HISTORICAL_DIAGNOSTIC":
        reasons = []
    final_fit_rows = current_training_rows
    model_fit = M1ModelFitV2(M1_POLICY_ID, M1_POLICY_VERSION, M1_FEATURE_POLICY_HASH,
        query_feature.compatibility_key, cutoff_ns, available_at_ns,
        tuple(sorted(training_artifact_refs[row.outcome_ref] for row in final_fit_rows)),
        tuple(sorted(training_artifact_refs[row.outcome_ref] for row in final_fit_rows)),
        tuple(sorted(training_artifact_refs[row.outcome_ref] for row in validation_rows)), selected_parameters,
        search_results, centers, scales, booster_ref, lightgbm_version, python_version, platform_id,
        dependency_lock_hash, SEED, THREAD_COUNT, OBJECTIVE, VALIDATION_METRIC, TIE_BREAK,
        fit_status, tuple(reasons), holdout_ref)
    model_fit_ref = persist_m1_artifact(repo, "M1ModelFitV2", model_fit.to_dict(),
        available_at_ns=available_at_ns, key="model_fit")
    prediction = M1PredictionV2(action.action.action_hash, action.content_hash, candidate.content_hash,
        candidate_set.content_hash, cutoff_ns, available_at_ns, model_fit_ref, query_feature_ref,
        model_fit.training_row_refs, oof_ref, calibration_ref, support_ref, ood_ref,
        query_feature.compatibility_key, expected_value, "AVAILABLE" if expected_value is not None else "NOT_ESTIMABLE",
        tuple(reasons))
    persist_m1_artifact(repo, "M1PredictionV2", prediction.to_dict(),
        available_at_ns=available_at_ns, key="prediction")
    return M1RunV2(model_fit, prediction, support, calibration, ood, chronology, oof_rows)


def compare_m1_to_m0(*, action_hash: str, m1_prediction_ref: str, m0_prediction_ref: str | None,
        m1_oof: Sequence[M1OOFRowV2], m0_oof: Sequence[M0OOFRowV2],
        outer_outcome_refs: Sequence[str]) -> M1IncrementalComparisonV2:
    """Paired forecast diagnostic; no inferred policy-value or model voting."""
    sha256_ref(action_hash, field="action_hash")
    sha256_ref(m1_prediction_ref, field="m1_prediction_ref")
    if m0_prediction_ref is not None:
        sha256_ref(m0_prediction_ref, field="m0_prediction_ref")
    outer = set(outer_outcome_refs)
    m1_by_ref = {row.outcome_ref: row for row in m1_oof if row.status == "OOF" and row.prediction is not None}
    m0_by_ref = {row.outcome_ref: row for row in m0_oof if row.status == "OOF" and row.prediction is not None}
    common = tuple(sorted(ref for ref in outer if ref in m1_by_ref and ref in m0_by_ref))
    if len(common) < 30 or len(common) != len(outer):
        return M1IncrementalComparisonV2(action_hash, m0_prediction_ref, m1_prediction_ref, common,
            None, None, None, None, None, None, "NOT_ESTIMABLE", "INSUFFICIENT_COMMON_CHRONOLOGICAL_OUTER_EVIDENCE")
    m1_error = [abs(m1_by_ref[ref].residual or Decimal(0)) for ref in common]
    m0_error = [abs(m0_by_ref[ref].target - (m0_by_ref[ref].target - (m0_by_ref[ref].residual or Decimal(0)))) for ref in common]
    m1_mae = sum(m1_error, Decimal(0)) / len(common)
    m0_mae = sum(m0_error, Decimal(0)) / len(common)
    # Forecast loss may be compared, but a change in this loss is not an
    # after-cost whole-policy value estimate. Its confidence interval remains
    # unavailable until the registered block evaluation is run.
    return M1IncrementalComparisonV2(action_hash, m0_prediction_ref, m1_prediction_ref, common,
        m0_mae, m1_mae, m1_mae - m0_mae, None, None, None, "HISTORICAL_DIAGNOSTIC",
        "FORECAST_LOSS_ONLY_POLICY_VALUE_NOT_ESTIMABLE")
