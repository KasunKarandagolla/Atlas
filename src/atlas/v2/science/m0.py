"""Chronological action-value baseline (M0); research-only and fail-closed."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import canonical_json, decimal_value, json_value, sha256_json, sha256_ref, strict_fields
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2, FeatureArtifactV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2
from atlas.v2.science.outcomes import (
    ExecutionOutcomeStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeTargetV2,
    executable_action_value_training_eligible,
    index_matured_outcome,
)

M0_FEATURE_SCHEMA_VERSION = "M0_ACTION_VALUE_FEATURES_V1"
M0_MODEL_VERSION = "M0_HUBER_RIDGE_ACTION_VALUE_V1"
M0_CONFIG_VERSION = "M0_FIXED_HUBER_RIDGE_CONFIG_V2"
M0_ESTIMATION_METHOD_VERSION = "INDEPENDENT_OOF_RESIDUAL_RIDGE_MEAN_SE_V2"
M0_RESIDUAL_ARCHIVE_VERSION = "M0_CHRONOLOGICAL_OOF_RESIDUAL_ARCHIVE_V2"
HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS

_TECH = ("ema20", "ema50", "robust_slope20", "adx14", "rsi14", "roc10",
         "realized_variance20", "ewma_variance", "bollinger_width20", "atr14")
_FRAMES = ("h4", "h1", "m15")
_OTHER = (
    "candle.signed_body_atr", "candle.upper_wick_atr", "candle.lower_wick_atr",
    "candle.range_atr", "candle.close_position", "candle.volume_z",
    "location.prior_day_high", "location.prior_day_low", "location.utc_day_trade_vwap",
    "structure.last_confirmed_high", "structure.last_confirmed_low",
    "structure.bos_up_at_close", "structure.bos_down_at_close",
    "structure.choch_up_at_close", "structure.choch_down_at_close",
    "structure.fvg_bull_at_close", "structure.fvg_bear_at_close",
    "structure.sweep_up_at_close", "structure.sweep_down_at_close",
    "structure.zone_version_count", "fibonacci.0.382", "fibonacci.0.5", "fibonacci.0.618",
    "regime.trend_state", "regime.volatility_state",
)
_MARKET_FEATURES = tuple(f"{frame}.{name}" for frame in _FRAMES for name in _TECH) + _OTHER
# Value/missingness columns are deliberately ordered and versioned. Hashes are never numeric features.
FEATURE_ORDER = tuple(item for name in _MARKET_FEATURES for item in (f"value:{name}", f"missing:{name}")) + (
    "action.side_long", "action.quantity_log_notional", "action.entry_collar_fraction",
    "action.stop_distance_fraction", "action.horizon_hours",
    "action.policy_s1", "action.policy_s2", "action.selection_rank", "action.selection_rank_missing",
    "action.venue_bybit", "action.venue_binance",
)


def _positive_count(value: Any) -> bool:
    return type(value) is int and value > 0


@dataclass(frozen=True)
class M0FeatureVectorV2:
    schema_version: str
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    feature_artifact_ref: str
    information_cutoff_ns: int
    feature_order: tuple[str, ...]
    values: tuple[float, ...]
    missing_reasons: tuple[tuple[str, str], ...]
    policy_hash: str
    compatibility_key: str

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "feature_artifact_ref", "policy_hash", "compatibility_key"):
            sha256_ref(getattr(self, name), field=name)
        if self.schema_version != M0_FEATURE_SCHEMA_VERSION or self.feature_order != FEATURE_ORDER:
            raise ValueError("unsupported M0 feature schema/order")
        if len(self.values) != len(FEATURE_ORDER) or any(not math.isfinite(x) for x in self.values):
            raise ValueError("M0 feature vector must be finite and fixed width")
        if type(self.information_cutoff_ns) is not int or self.information_cutoff_ns < 0:
            raise ValueError("M0 feature cutoff invalid")
        if self.missing_reasons != tuple(sorted(self.missing_reasons)):
            raise ValueError("M0 missingness must be sorted")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.schema_version, "action_hash": self.action_hash,
                "action_artifact_ref": self.action_artifact_ref, "candidate_ref": self.candidate_ref,
                "feature_artifact_ref": self.feature_artifact_ref, "information_cutoff_ns": self.information_cutoff_ns,
                "feature_order": list(self.feature_order), "values": list(self.values),
                "missing_reasons": [[name, reason] for name, reason in self.missing_reasons],
                "policy_hash": self.policy_hash, "compatibility_key": self.compatibility_key}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> M0FeatureVectorV2:
        fields = (set(cls.__dataclass_fields__) - {"schema_version"}) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="M0FeatureVectorV2"))
        d["schema_version"] = d.pop("version")
        if (d["schema_version"] != M0_FEATURE_SCHEMA_VERSION or
                not isinstance(d["feature_order"], list) or not isinstance(d["values"], list) or
                not isinstance(d["missing_reasons"], list)):
            raise ValueError("unsupported M0 feature-vector wire")
        d["feature_order"] = tuple(d["feature_order"])
        if any(type(value) not in (int, float) for value in d["values"]):
            raise ValueError("M0 wire feature values must be numeric")
        d["values"] = tuple(float(value) for value in d["values"])
        if any(not isinstance(row, list) or len(row) != 2 for row in d["missing_reasons"]):
            raise ValueError("M0 wire missingness must be name/reason pairs")
        d["missing_reasons"] = tuple((row[0], row[1]) for row in d["missing_reasons"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class M0TrainingRowV2:
    outcome_ref: str
    decision_at_ns: int
    horizon_end_ns: int
    available_at_ns: int
    action_hash: str
    action_artifact_ref: str
    policy_hash: str
    compatibility_key: str
    features: tuple[float, ...]
    target: Decimal
    provenance: str
    execution_state: str

    def __post_init__(self) -> None:
        for name in ("outcome_ref", "action_hash", "action_artifact_ref", "policy_hash", "compatibility_key"):
            sha256_ref(getattr(self, name), field=name)
        if len(self.features) != len(FEATURE_ORDER) or any(not math.isfinite(x) for x in self.features):
            raise ValueError("training feature width/non-finite value")
        if not isinstance(self.target, Decimal) or not self.target.is_finite():
            raise ValueError("M0 target requires finite Decimal")
        if self.execution_state not in {x.value for x in ExecutionOutcomeStateV2 if x != ExecutionOutcomeStateV2.NOT_APPLICABLE}:
            raise ValueError("M0 target is not an executable outcome")
        if not self.decision_at_ns < self.horizon_end_ns <= self.available_at_ns:
            raise ValueError("M0 training label chronology is invalid")


@dataclass(frozen=True)
class M0OOFRowV2:
    outcome_ref: str
    action_hash: str
    training_cutoff_ns: int
    horizon_end_ns: int
    label_available_at_ns: int
    training_row_refs: tuple[str, ...]
    prediction: Decimal | None
    target: Decimal
    residual: Decimal | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {"outcome_ref": self.outcome_ref, "action_hash": self.action_hash,
                "training_cutoff_ns": self.training_cutoff_ns,
                "horizon_end_ns": self.horizon_end_ns,
                "label_available_at_ns": self.label_available_at_ns,
                "training_row_refs": list(self.training_row_refs),
                "prediction": canonical_decimal_str(self.prediction) if self.prediction is not None else None,
                "target": canonical_decimal_str(self.target),
                "residual": canonical_decimal_str(self.residual) if self.residual is not None else None,
                "status": self.status}


@dataclass(frozen=True)
class M0SupportV2:
    action_hash: str
    information_cutoff_ns: int
    eligible_sample_count: int
    independent_support_count: int
    compatible_policy_count: int
    provenance_counts: tuple[tuple[str, int], ...]
    execution_state_counts: tuple[tuple[str, int], ...]
    missing_feature_coverage: tuple[tuple[str, Decimal], ...]
    training_start_ns: int | None
    training_end_ns: int | None
    training_outcome_refs: tuple[str, ...]
    compatibility_key: str
    evidence_quality: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": "M0_SUPPORT_V2_V1", "action_hash": self.action_hash,
                "information_cutoff_ns": self.information_cutoff_ns,
                "eligible_sample_count": self.eligible_sample_count,
                "independent_support_count": self.independent_support_count,
                "compatible_policy_count": self.compatible_policy_count,
                "provenance_counts": [[k, v] for k, v in self.provenance_counts],
                "execution_state_counts": [[k, v] for k, v in self.execution_state_counts],
                "missing_feature_coverage": [[k, canonical_decimal_str(v)] for k, v in self.missing_feature_coverage],
                "training_start_ns": self.training_start_ns, "training_end_ns": self.training_end_ns,
                "training_outcome_refs": list(self.training_outcome_refs),
                "compatibility_key": self.compatibility_key, "evidence_quality": self.evidence_quality}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M0CalibrationV2:
    action_hash: str
    training_cutoff_ns: int
    oof_archive_ref: str
    chronological_oof_count: int
    absolute_residual_q90: Decimal | None
    status: str
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"version": "M0_CHRONOLOGICAL_CALIBRATION_V1", "action_hash": self.action_hash,
                "training_cutoff_ns": self.training_cutoff_ns, "oof_archive_ref": self.oof_archive_ref,
                "chronological_oof_count": self.chronological_oof_count,
                "absolute_residual_q90": canonical_decimal_str(self.absolute_residual_q90) if self.absolute_residual_q90 is not None else None,
                "status": self.status, "reason": self.reason}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M0OODV2:
    action_hash: str
    feature_vector_ref: str
    training_row_refs: tuple[str, ...]
    robust_z_limit: Decimal
    maximum_absolute_robust_z: Decimal | None
    out_of_distribution: bool | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": "M0_ROBUST_OOD_V1", "action_hash": self.action_hash,
                "feature_vector_ref": self.feature_vector_ref, "training_row_refs": list(self.training_row_refs),
                "robust_z_limit": canonical_decimal_str(self.robust_z_limit),
                "maximum_absolute_robust_z": canonical_decimal_str(self.maximum_absolute_robust_z)
                if self.maximum_absolute_robust_z is not None else None,
                "out_of_distribution": self.out_of_distribution, "status": self.status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class M0PredictionV2:
    action_hash: str
    action_artifact_ref: str
    feature_vector_ref: str
    model_ref: str
    training_cutoff_ns: int
    available_at_ns: int
    expected_net_value: Decimal | None
    estimation_uncertainty: Decimal | None
    numerical_conversion_error: Decimal | None
    support_ref: str
    oof_archive_ref: str
    calibration_ref: str
    ood_ref: str
    status: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"version": "M0_ACTION_VALUE_PREDICTION_V1", "action_hash": self.action_hash,
                "action_artifact_ref": self.action_artifact_ref, "feature_vector_ref": self.feature_vector_ref,
                "model_ref": self.model_ref, "training_cutoff_ns": self.training_cutoff_ns,
                "available_at_ns": self.available_at_ns,
                "expected_net_value": canonical_decimal_str(self.expected_net_value) if self.expected_net_value is not None else None,
                "estimation_uncertainty": canonical_decimal_str(self.estimation_uncertainty) if self.estimation_uncertainty is not None else None,
                "numerical_conversion_error": canonical_decimal_str(self.numerical_conversion_error) if self.numerical_conversion_error is not None else None,
                "support_ref": self.support_ref, "oof_archive_ref": self.oof_archive_ref,
                "calibration_ref": self.calibration_ref, "ood_ref": self.ood_ref,
                "status": self.status, "reasons": list(self.reasons)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> M0PredictionV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="M0PredictionV2"))
        if d["version"] != "M0_ACTION_VALUE_PREDICTION_V1" or not isinstance(d["reasons"], list):
            raise ValueError("unsupported M0 prediction wire")
        for name in ("expected_net_value", "estimation_uncertainty", "numerical_conversion_error"):
            d[name] = decimal_value(d[name], field=name, wire=True) if d[name] is not None else None
        d["reasons"] = tuple(d["reasons"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class M0ArtifactV2:
    action_hash: str
    action_artifact_ref: str
    feature_schema_version: str
    config_version: str
    model_version: str
    training_cutoff_ns: int
    training_row_refs: tuple[str, ...]
    training_ordering: str
    centers: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    hyperparameters: tuple[tuple[str, float], ...]
    oof_archive_ref: str
    current_feature_vector_ref: str
    current_prediction_ref: str
    sample_count: int
    available_at_ns: int
    status: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"version": M0_MODEL_VERSION, "action_hash": self.action_hash,
                "action_artifact_ref": self.action_artifact_ref, "feature_schema_version": self.feature_schema_version,
                "config_version": self.config_version, "model_version": self.model_version,
                "training_cutoff_ns": self.training_cutoff_ns, "training_row_refs": list(self.training_row_refs),
                "training_ordering": self.training_ordering, "centers": list(self.centers), "scales": list(self.scales),
                "coefficients": list(self.coefficients), "intercept": self.intercept,
                "hyperparameters": [[k, v] for k, v in self.hyperparameters],
                "oof_archive_ref": self.oof_archive_ref, "current_feature_vector_ref": self.current_feature_vector_ref,
                "current_prediction_ref": self.current_prediction_ref, "sample_count": self.sample_count,
                "available_at_ns": self.available_at_ns, "status": self.status, "reasons": list(self.reasons)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _index(repo: OpsRepository, kind: str, ref: str, at_ns: int, key: str, body: Mapping[str, Any]) -> None:
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at_ns, at_ns, {key: body}))


def _body(repo: OpsRepository, ref: str, kind: str, key: str) -> Mapping[str, Any]:
    entry = repo.get_artifact(ref)
    value = entry.metadata.get(key) if entry is not None else None
    if entry is None or entry.artifact_type != kind or entry.content_hash != ref or not isinstance(value, Mapping):
        raise ValueError(f"indexed {kind} required")
    normalized = json_value(value)
    if not isinstance(normalized, Mapping):
        raise ValueError(f"indexed {kind} body is malformed")
    return normalized


def _numeric(value: Any, *, price_scale: float = 1.0) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError("M0 feature values must be numeric or explicitly missing")
    result = float(value) / price_scale
    if not math.isfinite(result):
        raise ValueError("M0 feature must be finite")
    return result


def _compatibility(action: FrozenActionV2, product: ProductContractV2, decision_at_ns: int) -> str:
    return sha256_json({"policy_hash": action.policy_hash, "venue": action.key.venue.value,
        "product": action.key.product.value, "trigger_basis": action.stop_trigger_basis,
        "horizon_duration_ns": action.horizon_end_ns - decision_at_ns, "management_rule": action.management_rule.to_dict(),
        "entry_rule": action.entry_rule.to_dict(), "collar_rule": action.collar_rule.to_dict(),
        "base_units_per_contract": canonical_decimal_str(product.base_units_per_contract)})


def action_features(repo: OpsRepository, action_artifact_ref: str, *, cutoff_ns: int) -> M0FeatureVectorV2:
    artifact_body = _body(repo, action_artifact_ref, "ActionArtifactV2", "action_artifact")
    identity_body = _body(repo, action_artifact_ref, "ActionArtifactV2", "action_identity")
    action_entry = repo.get_artifact(action_artifact_ref)
    if action_entry is None or action_entry.available_at_ns > cutoff_ns:
        raise ValueError("frozen action artifact unavailable by M0 cutoff")
    candidate_ref = artifact_body.get("candidate_ref")
    candidate_body = _body(repo, str(candidate_ref), "CandidateActionV2", "candidate")
    candidate = CandidateActionV2.from_dict(candidate_body)
    if (sha256_json(artifact_body) != action_artifact_ref or
            artifact_body.get("action_hash") != sha256_json(identity_body)):
        raise ValueError("M0 action artifact identity mismatch")
    if (candidate.content_hash != candidate_ref or candidate.decision_at_ns > cutoff_ns or
            candidate.policy_hash != identity_body.get("policy_hash")):
        raise ValueError("candidate feature cutoff mismatch")
    candidate_set_ref = artifact_body.get("candidate_set_ref")
    candidate_set_entry = repo.get_artifact(str(candidate_set_ref))
    candidate_set_body = candidate_set_entry.metadata.get("candidate_set") if candidate_set_entry is not None else None
    if (candidate_set_entry is None or candidate_set_entry.artifact_type != "CandidateSetV2" or
            candidate_set_entry.content_hash != candidate_set_ref or not isinstance(candidate_set_body, Mapping)):
        raise ValueError("M0 exact CandidateSet is unavailable")
    candidate_set = CandidateSetV2.from_dict(json_value(candidate_set_body))
    if (candidate_set.content_hash != candidate_set_ref or
            candidate_set.selected_candidate_id != candidate.candidate_id or
            candidate_set.envelope.available_at_ns > candidate.decision_at_ns):
        raise ValueError("M0 candidate selection identity mismatch")
    feature_entry = repo.get_artifact(candidate.snapshot_hash)
    feature_body = feature_entry.metadata.get("feature") if feature_entry is not None else None
    if (feature_entry is None or feature_entry.artifact_type != "FeatureArtifactV2" or
            feature_entry.content_hash != candidate.snapshot_hash or not isinstance(feature_body, Mapping) or
            feature_entry.available_at_ns > candidate.decision_at_ns):
        raise ValueError("original cutoff FeatureArtifactV2 unavailable")
    feature = FeatureArtifactV2.from_dict(json_value(feature_body))
    key = InstrumentKeyV2.from_dict(identity_body["key"])
    if (feature.content_hash != candidate.snapshot_hash or feature.key != key or
            feature.information_cutoff_ns > candidate.decision_at_ns or
            feature.envelope.available_at_ns > candidate.decision_at_ns or
            feature.replay_view.value not in {"ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"}):
        raise ValueError("M0 refuses future/revised feature evidence")
    product_entry = repo.get_artifact(str(artifact_body.get("product_ref")))
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2" or
            product_entry.content_hash != artifact_body.get("product_ref") or not isinstance(product_body, Mapping)):
        raise ValueError("action ProductContractV2 missing")
    product = ProductContractV2.from_dict(json_value(product_body))
    if product.key != key or product_entry.available_at_ns > candidate.decision_at_ns:
        raise ValueError("M0 product identity/availability mismatch")
    values_by_id = feature.values
    missing: list[tuple[str, str]] = []
    market: list[float] = []
    reference = float(candidate.entry_reference)
    for name in _MARKET_FEATURES:
        prefix, _, feature_name = name.partition(".")
        value: Any = None
        reason = "FEATURE_NOT_PRESENT_IN_ORIGINAL_SNAPSHOT"
        if name.startswith(("h4.", "h1.", "m15.")):
            v = values_by_id.get(name)
            if v is not None:
                value = v.value
                reason = v.missing_reason or "EXPLICIT_MISSING"
        else:
            v = values_by_id.get(name)
            if v is not None:
                value = v.value
                reason = v.missing_reason or "EXPLICIT_MISSING"
        price_scale = reference if name.endswith(("ema20", "ema50", "atr14", "prior_day_high", "prior_day_low", "utc_day_trade_vwap", "last_confirmed_high", "last_confirmed_low")) else 1.0
        numeric = _numeric(value, price_scale=price_scale)
        if numeric is None:
            missing.append((name, reason))
            market.extend((0.0, 1.0))
        else:
            market.extend((numeric, 0.0))
    selected_entries = [entry for entry in candidate_set.candidates if entry.candidate_id == candidate.candidate_id]
    if len(selected_entries) != 1:
        raise ValueError("M0 candidate must have one exact CandidateSet entry")
    rank: float | None = float(selected_entries[0].rank) if selected_entries[0].rank is not None else None
    policy_id = str(identity_body["policy_id"])
    action = FrozenActionV2(
        key, str(identity_body["side"]), decimal_value(identity_body["quantity"], field="quantity", wire=True),
        str(identity_body["product_ref"]), identity_body["entry_rule"], identity_body["collar_rule"],
        decimal_value(identity_body["entry_reference"], field="entry_reference", wire=True),
        decimal_value(identity_body["entry_collar"], field="entry_collar", wire=True),
        decimal_value(identity_body["stop_price"], field="stop_price", wire=True),
        str(identity_body.get("entry_trigger_basis", identity_body["stop_trigger_basis"])),
        str(identity_body["stop_trigger_basis"]), identity_body["management_rule"], identity_body["time_exit_rule"],
        int(identity_body["horizon_end_ns"]), policy_id, str(identity_body["policy_version"]),
        str(identity_body["policy_hash"]), str(identity_body["risk_policy_hash"]), str(identity_body["risk_policy_v2_hash"]),
    )
    notion = float(action.quantity * action.entry_reference * product.base_units_per_contract)
    action_values = [1.0 if action.side == "LONG" else 0.0,
        math.log1p(max(0.0, notion)), float(action.entry_collar / action.entry_reference - Decimal(1)),
        float(abs(action.entry_reference - action.stop_price) / action.entry_reference),
        (action.horizon_end_ns - candidate.decision_at_ns) / HOUR_NS,
        1.0 if policy_id == "S1_MTF_TREND_PULLBACK" else 0.0,
        1.0 if policy_id == "S2_COMPRESSION_BREAKOUT" else 0.0,
        rank if rank is not None else 0.0, 1.0 if rank is None else 0.0,
        1.0 if key.venue.value == "BYBIT" else 0.0,
        1.0 if key.venue.value == "BINANCE" else 0.0]
    vector = M0FeatureVectorV2(M0_FEATURE_SCHEMA_VERSION, str(artifact_body["action_hash"]),
        action_artifact_ref, str(candidate_ref), candidate.snapshot_hash, candidate.decision_at_ns,
        FEATURE_ORDER, tuple(market + action_values), tuple(sorted(missing)), action.policy_hash,
        _compatibility(action, product, candidate.decision_at_ns))
    if vector.action_hash != sha256_json(identity_body):
        raise ValueError("M0 action identity hash mismatch")
    return vector


def _training_rows(repo: OpsRepository, cutoff_ns: int, *, compatibility_key: str | None = None) -> tuple[M0TrainingRowV2, ...]:
    rows: list[M0TrainingRowV2] = []
    for entry in repo.artifact_entries("MaturedOutcomeV2"):
        raw = entry.metadata.get("outcome")
        if not isinstance(raw, Mapping):
            continue
        outcome = MaturedOutcomeV2.from_dict(json_value(raw))
        if entry.content_hash != outcome.content_hash or entry.available_at_ns != outcome.available_at_ns:
            raise ValueError("matured outcome index/body mismatch")
        if not executable_action_value_training_eligible(outcome, cutoff_ns):
            continue
        # Re-resolve the accepted Session-018 executable label contract from
        # its exact calendar/action/replay evidence before using it as a target.
        if index_matured_outcome(repo, outcome) != outcome.content_hash:
            raise ValueError("M0 target failed exact matured executable-action validation")
        assert outcome.action_artifact_ref is not None and outcome.action_hash is not None and outcome.net_payoff is not None
        action = repo.get_artifact(outcome.action_artifact_ref)
        identity = action.metadata.get("action_identity") if action is not None else None
        if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > outcome.horizon_end_ns
                or not isinstance(identity, Mapping) or sha256_json(identity) != outcome.action_hash
                or action.metadata.get("action_artifact", {}).get("candidate_ref") != outcome.candidate_ref
                or action.metadata.get("action_artifact", {}).get("candidate_set_ref") != outcome.candidate_set_ref):
            raise ValueError("M0 target does not resolve to exact matured frozen action")
        if outcome.label_state != LabelStateV2.MATURED or outcome.outcome_target != OutcomeTargetV2.EXECUTABLE_ACTION_VALUE:
            continue
        # The outcome must be known by this fit cutoff; its frozen action record may
        # have been indexed after the original decision, while its feature snapshot
        # remains pinned to the original decision cutoff inside action_features.
        vector = action_features(repo, outcome.action_artifact_ref, cutoff_ns=cutoff_ns)
        if vector.information_cutoff_ns != outcome.decision_at_ns or vector.action_hash != outcome.action_hash:
            raise ValueError("M0 historical action features changed or were revised")
        if compatibility_key is not None and vector.compatibility_key != compatibility_key:
            continue
        rows.append(M0TrainingRowV2(outcome.content_hash, outcome.decision_at_ns, outcome.horizon_end_ns,
            outcome.available_at_ns, outcome.action_hash, outcome.action_artifact_ref, outcome.policy_hash,
            vector.compatibility_key, vector.values, outcome.net_payoff, outcome.provenance.value,
            outcome.execution_state.value))
    # Stable chronological ordering retains exact label-availability ordering.
    return tuple(sorted(rows, key=lambda row: (row.available_at_ns, row.decision_at_ns, row.outcome_ref)))


def eligible_m0_targets(outcomes: Sequence[MaturedOutcomeV2], cutoff_ns: int) -> tuple[MaturedOutcomeV2, ...]:
    """Return only matured executable frozen-action labels actually available by cutoff."""
    return tuple(sorted((outcome for outcome in outcomes
                         if executable_action_value_training_eligible(outcome, cutoff_ns)),
                        key=lambda outcome: (outcome.available_at_ns, outcome.decision_at_ns, outcome.content_hash)))


def chronological_oof(rows: Sequence[M0TrainingRowV2], *, ridge: float = 1.0,
                      huber_delta: float = 1.345, min_training_samples: int = 5) -> tuple[M0OOFRowV2, ...]:
    """Expanding OOF archive; every row's labels were available strictly before its cutoff."""
    ordered = tuple(sorted(rows, key=lambda row: (row.available_at_ns, row.decision_at_ns, row.outcome_ref)))
    output: list[M0OOFRowV2] = []
    for row in ordered:
        earlier = tuple(item for item in ordered if item.available_at_ns < row.decision_at_ns)
        refs = tuple(item.outcome_ref for item in earlier)
        if any(item.available_at_ns >= row.decision_at_ns for item in earlier):
            raise AssertionError("chronological OOF training cutoff leaked a late label")
        if len(earlier) < min_training_samples:
            output.append(M0OOFRowV2(row.outcome_ref, row.action_hash, row.decision_at_ns,
                row.horizon_end_ns, row.available_at_ns, refs, None, row.target, None,
                "NOT_ESTIMABLE_INSUFFICIENT_CHRONOLOGICAL_HISTORY"))
            continue
        centers, scales, coefficients, intercept = _fit(earlier, ridge=ridge, huber_delta=huber_delta)
        prediction = Decimal(str(_predict(row.features, centers, scales, coefficients, intercept)))
        output.append(M0OOFRowV2(row.outcome_ref, row.action_hash, row.decision_at_ns,
            row.horizon_end_ns, row.available_at_ns, refs, prediction, row.target, row.target - prediction, "OOF"))
    return tuple(output)


def chronological_oof_calibration(*, action_hash: str, training_cutoff_ns: int,
        oof_archive_ref: str, rows: Sequence[M0OOFRowV2], minimum_samples: int = 30) -> M0CalibrationV2:
    eligible = tuple(row for row in rows if row.label_available_at_ns <= training_cutoff_ns and
        row.training_cutoff_ns <= training_cutoff_ns and
        row.residual is not None and row.status == "OOF")
    independent: list[M0OOFRowV2] = []
    last_horizon_end = -1
    for row in sorted(eligible, key=lambda item: (item.training_cutoff_ns, item.horizon_end_ns, item.outcome_ref)):
        if row.training_cutoff_ns >= last_horizon_end:
            independent.append(row)
            last_horizon_end = row.horizon_end_ns
    residuals = sorted(abs(row.residual) for row in independent if row.residual is not None)
    if minimum_samples <= 0:
        raise ValueError("minimum chronological calibration support must be positive")
    if len(residuals) < minimum_samples:
        return M0CalibrationV2(action_hash, training_cutoff_ns, oof_archive_ref,
            len(residuals), None, "NOT_ESTIMABLE", "INSUFFICIENT_CHRONOLOGICAL_OOF_RESIDUALS")
    q90 = residuals[math.ceil(0.90 * len(residuals)) - 1]
    return M0CalibrationV2(action_hash, training_cutoff_ns, oof_archive_ref,
        len(residuals), q90, "OOF_CALIBRATED")


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    n = len(vector)
    a = [matrix[i][:] + [vector[i]] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(a[row][col]))
        if abs(a[pivot][col]) < 1e-14:
            raise ValueError("M0 regularized normal equation is singular")
        a[col], a[pivot] = a[pivot], a[col]
        divisor = a[col][col]
        for j in range(col, n + 1):
            a[col][j] /= divisor
        for row in range(n):
            if row == col:
                continue
            factor = a[row][col]
            if factor:
                for j in range(col, n + 1):
                    a[row][j] -= factor * a[col][j]
    return [a[i][n] for i in range(n)]


def _fit(rows: Sequence[M0TrainingRowV2], *, ridge: float = 1.0, huber_delta: float = 1.345,
         iterations: int = 25) -> tuple[tuple[float, ...], tuple[float, ...], tuple[float, ...], float]:
    if not rows:
        raise ValueError("M0 fit requires training rows")
    width = len(FEATURE_ORDER)
    xs = [row.features for row in rows]
    centers = [statistics.median(x[j] for x in xs) for j in range(width)]
    scales = []
    for j in range(width):
        mad = statistics.median(abs(x[j] - centers[j]) for x in xs) * 1.4826
        scales.append(mad if mad > 1e-10 else 1.0)
    active = [j for j in range(width) if max(x[j] for x in xs) - min(x[j] for x in xs) > 1e-12]
    z = [[(x[j] - centers[j]) / scales[j] for j in active] for x in xs]
    ys = [float(row.target) for row in rows]
    beta = [0.0] * (len(active) + 1)
    weights = [1.0] * len(rows)
    for _ in range(iterations):
        dim = len(active) + 1
        matrix = [[0.0] * dim for _ in range(dim)]
        rhs = [0.0] * dim
        for x, y, w in zip(z, ys, weights, strict=True):
            row = [1.0, *x]
            for i in range(dim):
                rhs[i] += w * row[i] * y
                for j in range(dim):
                    matrix[i][j] += w * row[i] * row[j]
        for i in range(1, dim):
            matrix[i][i] += ridge
        updated = _solve(matrix, rhs)
        residuals = [y - sum(c * v for c, v in zip(updated, [1.0, *x], strict=True)) for x, y in zip(z, ys, strict=True)]
        scale = max(1e-9, statistics.median(abs(r) for r in residuals) * 1.4826)
        next_weights = [1.0 if abs(r) <= huber_delta * scale else huber_delta * scale / abs(r) for r in residuals]
        if max(abs(a - b) for a, b in zip(beta, updated, strict=True)) < 1e-10:
            beta, weights = updated, next_weights
            break
        beta, weights = updated, next_weights
    coefficients = [0.0] * width
    for index, feature_index in enumerate(active):
        coefficients[feature_index] = beta[index + 1]
    return tuple(centers), tuple(scales), tuple(coefficients), beta[0]


def _predict(features: Sequence[float], centers: Sequence[float], scales: Sequence[float],
             coefficients: Sequence[float], intercept: float) -> float:
    return intercept + sum(c * ((x - m) / s) for x, m, s, c in zip(features, centers, scales, coefficients, strict=True))


def _mean_prediction_leverage(rows: Sequence[M0TrainingRowV2], features: Sequence[float],
        centers: Sequence[float], scales: Sequence[float], *, ridge: float) -> float:
    width = len(FEATURE_ORDER)
    active = [j for j in range(width) if max(row.features[j] for row in rows) - min(row.features[j] for row in rows) > 1e-12]
    design = [[1.0, *[(row.features[j] - centers[j]) / scales[j] for j in active]] for row in rows]
    query = [1.0, *[(features[j] - centers[j]) / scales[j] for j in active]]
    dimension = len(query)
    gram = [[sum(row[i] * row[j] for row in design) for j in range(dimension)] for i in range(dimension)]
    for index in range(1, dimension):
        gram[index][index] += ridge
    solved = _solve(gram, query)
    return max(0.0, sum(a * b for a, b in zip(query, solved, strict=True)))


def _independent_rows(rows: Sequence[M0TrainingRowV2]) -> tuple[M0TrainingRowV2, ...]:
    selected: list[M0TrainingRowV2] = []
    last_end = -1
    for row in sorted(rows, key=lambda r: (r.decision_at_ns, r.horizon_end_ns, r.outcome_ref)):
        if row.decision_at_ns >= last_end:
            selected.append(row)
            last_end = row.horizon_end_ns
    return tuple(selected)


def _independent_count(rows: Sequence[M0TrainingRowV2]) -> int:
    return len(_independent_rows(rows))


def _persist(repo: OpsRepository, kind: str, at_ns: int, body: Mapping[str, Any], key: str) -> str:
    ref = sha256_json(body)
    _index(repo, kind, ref, at_ns, key, body)
    return ref


def fit_m0(repo: OpsRepository, *, action: ActionArtifactV2, candidate: CandidateActionV2,
           candidate_set: CandidateSetV2, cutoff_ns: int, available_at_ns: int,
           min_training_samples: int = 30, min_independent_support: int = 20,
           min_oof_training_samples: int = 5, min_oof_calibration_samples: int = 30,
           ridge: float = 1.0, huber_delta: float = 1.345) -> tuple[M0ArtifactV2, M0PredictionV2, M0SupportV2, M0CalibrationV2, tuple[M0OOFRowV2, ...]]:
    if (min_training_samples < 30 or min_independent_support < 20 or
            min_oof_training_samples < 5 or min_oof_calibration_samples < 30 or
            not math.isfinite(ridge) or ridge <= 0 or
            not math.isfinite(huber_delta) or huber_delta <= 0):
        raise ValueError("M0 configuration cannot lower versioned support floors or use invalid Huber/ridge parameters")
    if (action.action.action_hash != sha256_json(action.action.to_dict()) or candidate.content_hash != action.candidate_ref
            or candidate_set.content_hash != action.candidate_set_ref or candidate.decision_at_ns != cutoff_ns
            or action.available_at_ns > cutoff_ns or available_at_ns < cutoff_ns or available_at_ns >= candidate.deadline_ns):
        raise ValueError("M0 training cutoff must equal the exact frozen action decision cutoff")
    current_vector = action_features(repo, action.content_hash, cutoff_ns=cutoff_ns)
    if current_vector.action_hash != action.action.action_hash:
        raise ValueError("M0 current prediction action identity mismatch")
    current_feature_ref = index_m0_feature_vector(repo, current_vector, available_at_ns)
    all_rows = _training_rows(repo, cutoff_ns)
    compatible = tuple(row for row in all_rows if row.compatibility_key == current_vector.compatibility_key
                       and row.available_at_ns <= cutoff_ns)
    # OOF fits are expanding and each label must be available strictly before that row's own decision cutoff.
    oof = chronological_oof(compatible, ridge=ridge, huber_delta=huber_delta,
                            min_training_samples=min_oof_training_samples)
    oof_ref_body = {"version": M0_RESIDUAL_ARCHIVE_VERSION, "feature_schema_version": M0_FEATURE_SCHEMA_VERSION,
        "model_version": M0_MODEL_VERSION, "rows": [row.to_dict() for row in oof]}
    oof_ref = _persist(repo, "M0OOFResidualArchiveV2", available_at_ns, oof_ref_body, "oof_archive")
    missing_indices = tuple(i for i, name in enumerate(FEATURE_ORDER) if name.startswith("missing:"))
    missing_count = sum(any(row.features[i] != 0.0 for i in missing_indices) for row in compatible)
    missing_coverage = Decimal(missing_count) / Decimal(len(compatible)) if compatible else Decimal(1)
    support = M0SupportV2(action.action.action_hash, cutoff_ns, len(compatible), _independent_count(compatible),
        len({row.policy_hash for row in compatible}),
        tuple(sorted((p, sum(row.provenance == p for row in compatible)) for p in {r.provenance for r in compatible})),
        tuple(sorted((s, sum(row.execution_state == s for row in compatible)) for s in {r.execution_state for r in compatible})),
        (("__any_missing__", missing_coverage),),
        min((r.decision_at_ns for r in compatible), default=None), max((r.decision_at_ns for r in compatible), default=None),
        tuple(r.outcome_ref for r in compatible), current_vector.compatibility_key,
        "SUPPORTED" if len(compatible) >= min_training_samples and _independent_count(compatible) >= min_independent_support else "INSUFFICIENT")
    support_ref = _persist(repo, "M0SupportV2", available_at_ns, support.to_dict(), "support")
    calibration = chronological_oof_calibration(action_hash=action.action.action_hash,
        training_cutoff_ns=cutoff_ns, oof_archive_ref=oof_ref, rows=oof,
        minimum_samples=min_oof_calibration_samples)
    cal_status = calibration.status
    calibration_ref = _persist(repo, "M0CalibrationV2", available_at_ns, calibration.to_dict(), "calibration")
    ood_limit = Decimal("6")
    ood_max: Decimal | None = None
    ood_state: bool | None = None
    ood_status = "NOT_ESTIMABLE"
    if compatible:
        maxima: list[float] = []
        for index in range(len(FEATURE_ORDER)):
            values = [row.features[index] for row in compatible]
            center = statistics.median(values)
            scale = statistics.median(abs(value - center) for value in values) * 1.4826
            scale = scale if scale > 1e-10 else 1.0
            maxima.append(abs((current_vector.values[index] - center) / scale))
        raw_max = max(maxima, default=0.0)
        ood_max = Decimal(str(raw_max))
        ood_state = raw_max > float(ood_limit)
        ood_status = "OOD" if ood_state else "IN_DISTRIBUTION"
    ood = M0OODV2(action.action.action_hash, current_vector.content_hash,
        tuple(row.outcome_ref for row in compatible), ood_limit, ood_max, ood_state, ood_status)
    ood_ref = _persist(repo, "M0OODV2", available_at_ns, ood.to_dict(), "ood")
    reasons: list[str] = []
    if len(compatible) < min_training_samples:
        reasons.append("INSUFFICIENT_MATURED_EXECUTABLE_ACTION_VALUE_HISTORY")
    if _independent_count(compatible) < min_independent_support:
        reasons.append("INSUFFICIENT_INDEPENDENT_ACTION_SUPPORT")
    if cal_status != "OOF_CALIBRATED":
        reasons.append("CHRONOLOGICAL_OOF_CALIBRATION_UNSUPPORTED")
    if ood_state is None:
        reasons.append("M0_OOD_SUPPORT_UNAVAILABLE")
    elif ood_state:
        reasons.append("M0_CURRENT_ACTION_STATE_MATERIALLY_OOD")
    maximum_missing_fraction = 0.5
    current_missing = len(current_vector.missing_reasons) / max(1, len(_MARKET_FEATURES))
    if current_missing > maximum_missing_fraction:
        reasons.append("M0_REQUIRED_FEATURE_COVERAGE_INSUFFICIENT")
    oof_by_ref = {row.outcome_ref: row for row in oof if row.residual is not None}
    independent_oof = tuple(oof_by_ref[row.outcome_ref] for row in _independent_rows(compatible)
                            if row.outcome_ref in oof_by_ref)
    if len(independent_oof) < min_independent_support:
        reasons.append("INSUFFICIENT_INDEPENDENT_OOF_RESIDUAL_SUPPORT")
    centers: tuple[float, ...] = ()
    scales: tuple[float, ...] = ()
    coefficients: tuple[float, ...] = ()
    intercept = 0.0
    expected: Decimal | None = None
    estimate_se: Decimal | None = None
    conv_error: Decimal | None = None
    if not reasons:
        centers, scales, coefficients, intercept = _fit(compatible, ridge=ridge, huber_delta=huber_delta)
        raw_prediction = _predict(current_vector.values, centers, scales, coefficients, intercept)
        expected = Decimal(str(raw_prediction))
        conv_error = abs(Decimal.from_float(raw_prediction) - expected)
        # Parameter uncertainty is derived from OOF residual variance and the current action leverage.
        residual_values = [float(row.residual) for row in independent_oof if row.residual is not None]
        sigma = statistics.pstdev(residual_values)
        leverage = _mean_prediction_leverage(_independent_rows(compatible), current_vector.values,
            centers, scales, ridge=ridge)
        estimate_se = Decimal(str(sigma * math.sqrt(leverage)))
    # Prediction and model hashes include real training refs and exact feature schema.
    model_body = {"version": M0_MODEL_VERSION, "feature_schema_version": M0_FEATURE_SCHEMA_VERSION,
        "config_version": M0_CONFIG_VERSION, "training_cutoff_ns": cutoff_ns,
        "current_action_artifact_ref": action.content_hash,
        "training_row_refs": [r.outcome_ref for r in compatible], "training_ordering": "AVAILABLE_AT_DECISION_AT_OUTCOME_REF_ASC",
        "centers": list(centers), "scales": list(scales), "coefficients": list(coefficients), "intercept": intercept,
        "hyperparameters": {"ridge": ridge, "huber_delta": huber_delta, "iterations": 25},
        "support_config": {"minimum_training_samples": min_training_samples,
            "minimum_independent_support": min_independent_support,
            "minimum_oof_training_samples": min_oof_training_samples,
            "minimum_oof_calibration_samples": min_oof_calibration_samples,
            "maximum_missing_feature_fraction": maximum_missing_fraction,
            "robust_ood_z_limit": canonical_decimal_str(ood_limit),
            "estimation_method": M0_ESTIMATION_METHOD_VERSION},
        "oof_archive_ref": oof_ref, "current_feature_vector_ref": current_feature_ref,
        "sample_count": len(compatible), "current_action_hash": action.action.action_hash,
        "status": "AVAILABLE" if not reasons else "NOT_ESTIMABLE", "reasons": sorted(set(reasons))}
    # This is the independently resolvable fitted parameter artifact. The run
    # artifact below points to the prediction; prediction points back here, so
    # immutable references remain acyclic.
    model_ref = _persist(repo, "M0ModelFitV2", available_at_ns, model_body, "model_fit")
    prediction = M0PredictionV2(action.action.action_hash, action.content_hash, current_vector.content_hash,
        model_ref, cutoff_ns, available_at_ns, expected, estimate_se, conv_error, support_ref, oof_ref,
        calibration_ref, ood_ref, "AVAILABLE" if not reasons else "NOT_ESTIMABLE", tuple(sorted(set(reasons))))
    prediction_ref = _persist(repo, "M0PredictionV2", available_at_ns, prediction.to_dict(), "prediction")
    model = M0ArtifactV2(action.action.action_hash, action.content_hash, M0_FEATURE_SCHEMA_VERSION,
        M0_CONFIG_VERSION, M0_MODEL_VERSION, cutoff_ns, tuple(r.outcome_ref for r in compatible),
        "AVAILABLE_AT_DECISION_AT_OUTCOME_REF_ASC", centers, scales, coefficients, intercept,
        (("ridge", ridge), ("huber_delta", huber_delta), ("iterations", 25.0)), oof_ref,
        current_vector.content_hash, prediction_ref, len(compatible), available_at_ns,
        "AVAILABLE" if not reasons else "NOT_ESTIMABLE", tuple(sorted(set(reasons))))
    _persist(repo, "M0PredictionV2", available_at_ns, prediction.to_dict(), "prediction")
    _persist(repo, "M0ModelRunV2", available_at_ns, model.to_dict(), "model_run")
    return model, prediction, support, calibration, tuple(oof)


def validate_m0_fit_evidence(repo: OpsRepository, *, action: ActionArtifactV2,
        prediction: M0PredictionV2, feature_vector: M0FeatureVectorV2,
        model_body: Mapping[str, Any], oof_body: Mapping[str, Any],
        support_body: Mapping[str, Any], calibration_body: Mapping[str, Any],
        ood_body: Mapping[str, Any]) -> None:
    """Reproduce the stored M0 fit and every support/calibration/OOD result."""
    cutoff_ns = feature_vector.information_cutoff_ns
    current = action_features(repo, action.content_hash, cutoff_ns=cutoff_ns)
    if (current.content_hash != feature_vector.content_hash or prediction.action_hash != action.action.action_hash or
            prediction.action_artifact_ref != action.content_hash or prediction.feature_vector_ref != feature_vector.content_hash or
            prediction.training_cutoff_ns != cutoff_ns or prediction.model_ref is None):
        raise ValueError("M0 reproduction inputs do not bind exact cutoff action/feature evidence")
    if (model_body.get("version") != M0_MODEL_VERSION or
            model_body.get("feature_schema_version") != M0_FEATURE_SCHEMA_VERSION or
            model_body.get("config_version") != M0_CONFIG_VERSION or
            model_body.get("current_action_hash") != action.action.action_hash or
            model_body.get("current_action_artifact_ref") != action.content_hash or
            model_body.get("current_feature_vector_ref") != feature_vector.content_hash or
            model_body.get("training_cutoff_ns") != cutoff_ns or
            model_body.get("training_ordering") != "AVAILABLE_AT_DECISION_AT_OUTCOME_REF_ASC" or
            model_body.get("oof_archive_ref") != prediction.oof_archive_ref or
            tuple(model_body.get("reasons", ())) != prediction.reasons or
            model_body.get("status") != prediction.status):
        raise ValueError("M0 fitted model identity/configuration differs from prediction")
    hyperparameters = model_body.get("hyperparameters")
    config = model_body.get("support_config")
    if (not isinstance(hyperparameters, Mapping) or not isinstance(config, Mapping) or
            hyperparameters.get("iterations") != 25 or
            config.get("estimation_method") != M0_ESTIMATION_METHOD_VERSION or
            config.get("maximum_missing_feature_fraction") != 0.5 or
            config.get("robust_ood_z_limit") != "6" or
            any(not _positive_count(config.get(name)) for name in (
                "minimum_training_samples", "minimum_independent_support",
                "minimum_oof_training_samples", "minimum_oof_calibration_samples"))):
        raise ValueError("M0 model contains an unsupported versioned fit/support configuration")
    if (config["minimum_training_samples"] < 30 or config["minimum_independent_support"] < 20 or
            config["minimum_oof_training_samples"] < 5 or config["minimum_oof_calibration_samples"] < 30):
        raise ValueError("M0 model configuration lowers versioned support floors")
    ridge = hyperparameters.get("ridge")
    huber_delta = hyperparameters.get("huber_delta")
    if (isinstance(ridge, bool) or not isinstance(ridge, (int, float)) or ridge <= 0 or
            isinstance(huber_delta, bool) or not isinstance(huber_delta, (int, float)) or huber_delta <= 0):
        raise ValueError("M0 regularization parameters are invalid")
    training_rows = _training_rows(repo, cutoff_ns, compatibility_key=feature_vector.compatibility_key)
    training_refs = tuple(row.outcome_ref for row in training_rows)
    if (tuple(model_body.get("training_row_refs", ())) != training_refs or
            model_body.get("sample_count") != len(training_rows) or
            tuple(model_body.get("training_row_refs", ())) != tuple(support_body.get("training_outcome_refs", ())) or
            sha256_json(support_body) != prediction.support_ref or
            sha256_json(calibration_body) != prediction.calibration_ref or
            sha256_json(ood_body) != prediction.ood_ref):
        raise ValueError("M0 outcome/support/calibration/OOD refs do not resolve to exact training inputs")
    oof_rows = chronological_oof(training_rows, ridge=float(ridge), huber_delta=float(huber_delta),
        min_training_samples=config["minimum_oof_training_samples"])
    expected_oof = {"version": M0_RESIDUAL_ARCHIVE_VERSION,
        "feature_schema_version": M0_FEATURE_SCHEMA_VERSION, "model_version": M0_MODEL_VERSION,
        "rows": [row.to_dict() for row in oof_rows]}
    if canonical_json(oof_body) != canonical_json(expected_oof):
        raise ValueError("M0 chronological OOF residual archive does not reproduce")
    centers: tuple[float, ...] = ()
    scales: tuple[float, ...] = ()
    coefficients: tuple[float, ...] = ()
    intercept = 0.0
    if prediction.status == "AVAILABLE":
        centers, scales, coefficients, intercept = _fit(training_rows,
            ridge=float(ridge), huber_delta=float(huber_delta))
    elif prediction.status != "NOT_ESTIMABLE":
        raise ValueError("unknown M0 prediction availability state")
    if (tuple(model_body.get("centers", ())) != centers or tuple(model_body.get("scales", ())) != scales or
            tuple(model_body.get("coefficients", ())) != coefficients or model_body.get("intercept") != intercept):
        raise ValueError("M0 fitted coefficients/intercept do not reproduce from exact labels")
    independent_count = _independent_count(training_rows)
    missing_indices = tuple(i for i, name in enumerate(FEATURE_ORDER) if name.startswith("missing:"))
    missing_count = sum(any(row.features[i] != 0.0 for i in missing_indices) for row in training_rows)
    missing_coverage = Decimal(missing_count) / Decimal(len(training_rows)) if training_rows else Decimal(1)
    expected_support = M0SupportV2(action.action.action_hash, cutoff_ns, len(training_rows), independent_count,
        len({row.policy_hash for row in training_rows}),
        tuple(sorted((name, sum(row.provenance == name for row in training_rows))
            for name in {row.provenance for row in training_rows})),
        tuple(sorted((name, sum(row.execution_state == name for row in training_rows))
            for name in {row.execution_state for row in training_rows})),
        (("__any_missing__", missing_coverage),),
        min((row.decision_at_ns for row in training_rows), default=None),
        max((row.decision_at_ns for row in training_rows), default=None), training_refs,
        feature_vector.compatibility_key,
        "SUPPORTED" if len(training_rows) >= config["minimum_training_samples"] and
            independent_count >= config["minimum_independent_support"] else "INSUFFICIENT")
    if canonical_json(support_body) != canonical_json(expected_support.to_dict()):
        raise ValueError("M0 typed sample/support summary does not reproduce from exact matured labels")
    expected_calibration = chronological_oof_calibration(action_hash=action.action.action_hash,
        training_cutoff_ns=cutoff_ns, oof_archive_ref=prediction.oof_archive_ref,
        rows=oof_rows, minimum_samples=config["minimum_oof_calibration_samples"])
    if canonical_json(calibration_body) != canonical_json(expected_calibration.to_dict()):
        raise ValueError("M0 chronological OOF calibration result does not reproduce")
    if training_rows:
        maxima: list[float] = []
        for index in range(len(FEATURE_ORDER)):
            values = [row.features[index] for row in training_rows]
            center = statistics.median(values)
            scale = statistics.median(abs(value - center) for value in values) * 1.4826
            scale = scale if scale > 1e-10 else 1.0
            maxima.append(abs((feature_vector.values[index] - center) / scale))
        raw_max = max(maxima, default=0.0)
        expected_ood = M0OODV2(action.action.action_hash, feature_vector.content_hash, training_refs,
            Decimal("6"), Decimal(str(raw_max)), raw_max > 6.0, "OOD" if raw_max > 6.0 else "IN_DISTRIBUTION")
    else:
        expected_ood = M0OODV2(action.action.action_hash, feature_vector.content_hash, (),
            Decimal("6"), None, None, "NOT_ESTIMABLE")
    if canonical_json(ood_body) != canonical_json(expected_ood.to_dict()):
        raise ValueError("M0 robust OOD result does not reproduce from training-only feature bounds")
    reasons: list[str] = []
    if len(training_rows) < config["minimum_training_samples"]:
        reasons.append("INSUFFICIENT_MATURED_EXECUTABLE_ACTION_VALUE_HISTORY")
    if independent_count < config["minimum_independent_support"]:
        reasons.append("INSUFFICIENT_INDEPENDENT_ACTION_SUPPORT")
    if expected_calibration.status != "OOF_CALIBRATED":
        reasons.append("CHRONOLOGICAL_OOF_CALIBRATION_UNSUPPORTED")
    if expected_ood.out_of_distribution is None:
        reasons.append("M0_OOD_SUPPORT_UNAVAILABLE")
    elif expected_ood.out_of_distribution:
        reasons.append("M0_CURRENT_ACTION_STATE_MATERIALLY_OOD")
    current_missing = len(feature_vector.missing_reasons) / max(1, len(_MARKET_FEATURES))
    if current_missing > config["maximum_missing_feature_fraction"]:
        reasons.append("M0_REQUIRED_FEATURE_COVERAGE_INSUFFICIENT")
    oof_by_ref = {row.outcome_ref: row for row in oof_rows if row.residual is not None}
    independent_oof = tuple(oof_by_ref[row.outcome_ref] for row in _independent_rows(training_rows)
                            if row.outcome_ref in oof_by_ref)
    if len(independent_oof) < config["minimum_independent_support"]:
        reasons.append("INSUFFICIENT_INDEPENDENT_OOF_RESIDUAL_SUPPORT")
    expected_status = "AVAILABLE" if not reasons else "NOT_ESTIMABLE"
    if prediction.status != expected_status or prediction.reasons != tuple(sorted(set(reasons))):
        raise ValueError("M0 prediction availability/reasons do not reproduce from evidence gates")
    if expected_status == "AVAILABLE":
        raw_prediction = _predict(feature_vector.values, centers, scales, coefficients, intercept)
        expected_value = Decimal(str(raw_prediction))
        conversion_error = abs(Decimal.from_float(raw_prediction) - expected_value)
        residuals = [float(row.residual) for row in independent_oof if row.residual is not None]
        sigma = statistics.pstdev(residuals)
        leverage = _mean_prediction_leverage(_independent_rows(training_rows), feature_vector.values,
            centers, scales, ridge=float(ridge))
        expected_se = Decimal(str(sigma * math.sqrt(leverage)))
        if (prediction.expected_net_value != expected_value or
                prediction.estimation_uncertainty != expected_se or
                prediction.numerical_conversion_error != conversion_error):
            raise ValueError("M0 exact action prediction, standard error or numeric conversion error differs")
    elif any(value is not None for value in (prediction.expected_net_value,
            prediction.estimation_uncertainty, prediction.numerical_conversion_error)):
        raise ValueError("unestimable M0 prediction cannot carry an inferred value/uncertainty")


def index_m0_feature_vector(repo: OpsRepository, vector: M0FeatureVectorV2, available_at_ns: int) -> str:
    if available_at_ns < vector.information_cutoff_ns:
        raise ValueError("M0 vector cannot be available before cutoff")
    _index(repo, "M0FeatureVectorV2", vector.content_hash, available_at_ns, "feature_vector", vector.to_dict())
    return vector.content_hash
