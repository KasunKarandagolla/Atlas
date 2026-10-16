"""Verified OOF evidence on the existing immutable Ops artifact owner.

Only the explicitly synthetic offline qualification is implemented here. It
cannot label synthetic data as actual market calibration or grant capital.
Training/bootstrap are offline; consumption verifies the sealed causal chain.
"""

from __future__ import annotations

import math
import random
import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import sha256_json
from atlas.v2.science.mean_common import DAY_NS, MEAN_TARGET, ZERO, OfflineArtifact, unique
from atlas.v2.science.mean_models import FEATURES, MeanModelV1, MeanTrainingRowV1, predict_mean
from atlas.v2.science.mean_research import MeanSelectionCalibrationV1, ProtectedMeanHoldoutV1

GRADE = "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT"
FEATURE_REF = sha256_json({"features": FEATURES, "target": MEAN_TARGET})


@dataclass(frozen=True)
class MeanEvidenceScopeV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_EVIDENCE_SCOPE_V1"
    product_ref: str
    policy_ref: str
    compatibility_ref: str
    feature_ref: str
    horizon_ns: int
    support_min: tuple[float, ...]
    support_max: tuple[float, ...]
    registered_at_ns: int
    expires_at_ns: int
    provenance: str
    evidence_grade: str

    def __post_init__(self):
        super().__post_init__()
        if (
            self.feature_ref != FEATURE_REF
            or len(self.support_min) != len(FEATURES)
            or len(self.support_max) != len(FEATURES)
            or any(a > b for a, b in zip(self.support_min, self.support_max, strict=True))
            or self.horizon_ns != DAY_NS
            or self.registered_at_ns >= self.expires_at_ns
            or self.provenance != "CAUSAL_REPLAY"
            or self.evidence_grade != GRADE
        ):
            raise ValueError("unsupported evidence scope/grade; genuine market qualification remains separate")


class EvidenceReader:
    """Per-consumption cache, never reused after return or across revisions."""

    def __init__(self, repo, *, cutoff_ns, now_ns):
        self.repo, self.cutoff_ns, self.now_ns = repo, cutoff_ns, now_ns
        self.cache = {}
        self.verified_rows = set()

    def read(self, ref, cls, *, by_ns=None, valid_at_ns=None):
        from atlas.v2.science.mean_evaluation import read_mean_artifact

        key = (ref, cls)
        if key not in self.cache:
            self.cache[key] = (
                read_mean_artifact(self.repo, ref, cls, consumer_ns=self.now_ns),
                self.repo.get_artifact(ref),
            )
        obj, entry = self.cache[key]
        bound = self.cutoff_ns if by_ns is None else by_ns
        available = getattr(
            obj, "available_at_ns", getattr(obj, "registered_at_ns", getattr(obj, "label_available_at_ns", None))
        )
        if (
            entry.available_at_ns > bound
            or (available is not None and (available > bound or entry.available_at_ns != available))
            or (
                hasattr(obj, "expires_at_ns")
                and (self.now_ns if valid_at_ns is None else valid_at_ns) >= obj.expires_at_ns
            )
        ):
            raise ValueError("EVIDENCE_STALE_EXPIRED_OR_FUTURE")
        return obj


@dataclass(frozen=True)
class MeanOriginEvidenceV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_HISTORICAL_ORIGIN_EVIDENCE_V1"
    scope_ref: str
    action_id: str
    origin_ns: int
    available_at_ns: int
    features: tuple[float, ...]

    def __post_init__(self):
        super().__post_init__()
        if self.available_at_ns > self.origin_ns or not self.action_id or len(self.features) != len(FEATURES):
            raise ValueError("HISTORICAL_ORIGIN_FEATURES_UNAVAILABLE")


@dataclass(frozen=True)
class MeanOutcomeEvidenceV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_OUTCOME_EVIDENCE_V1"
    scope_ref: str
    action_ref: str
    origin_ns: int
    label_end_ns: int
    available_at_ns: int
    features: tuple[float, ...]
    target_cash: Decimal
    execution_state: str

    def training_row(self, scope):
        if self.scope_ref != scope.content_hash or self.label_end_ns != self.origin_ns + scope.horizon_ns:
            raise ValueError("OUTCOME_SCOPE_CHANGED")
        return MeanTrainingRowV1(
            self.content_hash,
            self.action_ref,
            scope.compatibility_ref,
            self.origin_ns,
            self.origin_ns,
            self.label_end_ns,
            self.available_at_ns,
            self.features,
            self.target_cash,
            scope.provenance,
            self.execution_state,
            "QUALIFIED_MODELLED",
        )

    def __post_init__(self):
        super().__post_init__()
        if not self.origin_ns < self.label_end_ns <= self.available_at_ns or len(self.features) != len(FEATURES):
            raise ValueError("OUTCOME_CHRONOLOGY_OR_FEATURES")


@dataclass(frozen=True)
class MeanCalibrationProtocolV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_OOF_CALIBRATION_PROTOCOL_V1"
    scope_ref: str
    holdout_ref: str
    registered_at_ns: int
    start_ns: int
    end_ns: int
    block_ns: int = 7 * DAY_NS
    embargo_ns: int = DAY_NS
    minimum_blocks: int = 20
    alpha: float = 0.05
    resamples: int = 10000
    estimator: str = "M0_MEAN_RIDGE_V1"
    ridge: float = 1.0
    dependence_assumption: str = "INDEPENDENT_SYNTHETIC_CALENDAR_BLOCKS_V1"

    def __post_init__(self):
        super().__post_init__()
        if (
            not self.registered_at_ns < self.start_ns < self.end_ns
            or self.block_ns < 7 * DAY_NS
            or self.embargo_ns != DAY_NS
            or self.estimator != "M0_MEAN_RIDGE_V1"
            or self.ridge <= 0
            or self.minimum_blocks < 20
            or not 0 < self.alpha < 0.5
            or not 100 <= self.resamples <= 100000
            or self.dependence_assumption != "INDEPENDENT_SYNTHETIC_CALENDAR_BLOCKS_V1"
        ):
            raise ValueError("PREREGISTERED_CALIBRATION_RULES_REQUIRED")


@dataclass(frozen=True)
class MeanOOFPredictionV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_VERIFIABLE_OOF_PREDICTION_V1"
    protocol_ref: str
    scope_ref: str
    model_ref: str
    training_row_refs: tuple[str, ...]
    action_ref: str
    origin_ns: int
    available_at_ns: int
    features: tuple[float, ...]
    prediction_cash: Decimal


@dataclass(frozen=True)
class SelectedPolicyResidualV2(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SELECTED_POLICY_MEAN_RESIDUAL_V2"
    prediction_ref: str
    target_row_ref: str
    available_at_ns: int


@dataclass(frozen=True)
class MeanDependenceQualificationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_OOF_DEPENDENCE_QUALIFICATION_V1"
    protocol_ref: str
    serving_model_ref: str
    residual_refs: tuple[str, ...]
    available_at_ns: int
    expires_at_ns: int
    effective_blocks: int
    lag_correlations: tuple[float, ...]
    allowance_cash: Decimal


@dataclass(frozen=True)
class MeanSelectionCalibrationV2(MeanSelectionCalibrationV1):
    VERSION: ClassVar[str] = "ATLAS_RED_SELECTED_POLICY_MEAN_CALIBRATION_V2"
    qualification_ref: str


def _holdout(reader, protocol, scope):
    hold = reader.read(protocol.holdout_ref, ProtectedMeanHoldoutV1, by_ns=protocol.registered_at_ns)
    identity = sha256_json({"first_mean_holdout": hold.family_ref, "cell": hold.compatibility_ref})
    marker = reader.repo.get_artifact(identity)
    if (
        hold.family_ref != scope.policy_ref
        or hold.compatibility_ref != scope.compatibility_ref
        or hold.source_ref != scope.content_hash
        or protocol.end_ns > hold.start_ns
        or marker is None
        or marker.metadata.get("holdout_ref") != hold.content_hash
        or marker.content_hash != sha256_json(marker.metadata)
    ):
        raise ValueError("PROTECTED_HOLDOUT_BINDING")
    return hold


def _row(reader, ref, scope, hold, *, by_ns):
    row = reader.read(ref, MeanTrainingRowV1, by_ns=by_ns)
    key = (ref, scope.content_hash, hold.content_hash)
    if key in reader.verified_rows:
        return row
    evidence = reader.read(row.outcome_ref, MeanOutcomeEvidenceV1, by_ns=by_ns)
    origin = reader.read(row.action_ref, MeanOriginEvidenceV1, by_ns=row.origin_ns)
    if origin.scope_ref != scope.content_hash or origin.features != row.features or origin.origin_ns != row.origin_ns:
        raise ValueError("HISTORICAL_ACTION_OR_FEATURE_PREFIX_CHANGED")
    if (
        row != evidence.training_row(scope)
        or any(not lo <= x <= hi for x, lo, hi in zip(row.features, scope.support_min, scope.support_max, strict=True))
        or row.outcome_ref in hold.outcome_refs
        or row.origin_ns >= hold.start_ns
        or row.label_end_ns >= hold.start_ns
    ):
        raise ValueError("UNVERIFIED_OR_PROTECTED_OUTCOME")
    reader.verified_rows.add(key)
    return row


def verify_oof_residuals(reader, protocol, residual_refs):
    scope = reader.read(protocol.scope_ref, MeanEvidenceScopeV1, by_ns=protocol.registered_at_ns)
    hold = _holdout(reader, protocol, scope)
    unique(residual_refs)
    if not 1 <= len(residual_refs) <= 512:
        raise ValueError("BOUNDED_RESIDUAL_MEMBERSHIP_REQUIRED")
    result = []
    outcomes = []
    for ref in residual_refs:
        residual = reader.read(ref, SelectedPolicyResidualV2)
        prediction = reader.read(residual.prediction_ref, MeanOOFPredictionV1)
        row = _row(reader, residual.target_row_ref, scope, hold, by_ns=reader.cutoff_ns)
        model = reader.read(
            prediction.model_ref, MeanModelV1, by_ns=prediction.origin_ns, valid_at_ns=prediction.origin_ns
        )
        if (
            prediction.protocol_ref != protocol.content_hash
            or prediction.scope_ref != scope.content_hash
            or not protocol.start_ns <= row.origin_ns < protocol.end_ns
            or (prediction.action_ref, prediction.origin_ns, prediction.features)
            != (row.action_ref, row.origin_ns, row.features)
            or prediction.available_at_ns > row.origin_ns
            or residual.available_at_ns < row.label_available_at_ns
            or row.label_available_at_ns >= reader.cutoff_ns
            or model.estimator != protocol.estimator
            or model.parameters.get("ridge") != protocol.ridge
            or model.compatibility_ref != scope.compatibility_ref
            or model.provenance != scope.provenance
            or model.available_at_ns >= row.origin_ns
            or model.training_cutoff_ns > row.origin_ns - protocol.embargo_ns
            or row.outcome_ref in model.training_refs
        ):
            raise ValueError("OOF_MODEL_TARGET_OR_CHRONOLOGY_INVALID")
        unique(prediction.training_row_refs)
        training = tuple(
            _row(reader, t, scope, hold, by_ns=model.training_cutoff_ns) for t in prediction.training_row_refs
        )
        if (
            not training
            or tuple(r.outcome_ref for r in training) != model.training_refs
            or any(
                r.label_available_at_ns >= model.training_cutoff_ns
                or r.label_end_ns >= row.origin_ns - protocol.embargo_ns
                for r in training
            )
        ):
            raise ValueError("OOF_TRAINING_MEMBERSHIP_OVERLAP_OR_EMBARGO")
        value = predict_mean(
            model,
            row.features,
            compatibility_ref=scope.compatibility_ref,
            cutoff_ns=row.origin_ns,
            now_ns=row.origin_ns,
        )
        if value != prediction.prediction_cash:
            raise ValueError("OOF_ORIGINAL_PREDICTION_CHANGED")
        outcomes.append(row.outcome_ref)
        result.append((row.origin_ns, value - row.target_cash))
    unique(tuple(outcomes))
    return scope, tuple(result)


def dependence_diagnostics(values, protocol):
    groups = {}
    for at, value in values:
        groups.setdefault(at // protocol.block_ns, []).append(value)
    # Missing calendar blocks are not manufactured into independent observations.
    means = [float(sum(v, ZERO) / len(v)) for _, v in sorted(groups.items())]
    center = statistics.mean(means)
    denominator = sum((x - center) ** 2 for x in means)
    lags = tuple(
        0.0
        if not denominator
        else sum((means[i] - center) * (means[i - lag] - center) for i in range(lag, len(means))) / denominator
        for lag in range(1, min(4, len(means)))
    )
    effective = math.floor(len(means) / (1 + 2 * sum(max(0.0, x) for x in lags)))
    if effective < protocol.minimum_blocks or any(abs(x) > 0.2 for x in lags):
        raise ValueError("DEPENDENCE_EFFECTIVE_SUPPORT_NOT_ESTIMABLE")
    return groups, effective, lags


def verify_serving_training(reader, model, scope, protocol):
    stored = reader.read(model.content_hash, MeanModelV1)
    if model != stored or not 1 <= len(model.training_refs) <= 512:
        raise ValueError("SERVING_MODEL_MEMBERSHIP_INVALID")
    hold = _holdout(reader, protocol, scope)
    for ref in model.training_refs:
        evidence = reader.read(ref, MeanOutcomeEvidenceV1, by_ns=model.training_cutoff_ns)
        row = evidence.training_row(scope)
        verified = _row(reader, row.content_hash, scope, hold, by_ns=model.training_cutoff_ns)
        if verified.label_available_at_ns >= model.training_cutoff_ns:
            raise ValueError("SERVING_TRAINING_IMMATURE_OR_FUTURE")


def selected_policy_upper_cash(groups, *, alpha, resamples):
    """Offline complete-group resampling, weighted by selected-action count."""
    if not 100 <= resamples <= 100000 or not 0 < alpha < 0.5 or not groups:
        raise ValueError("REGISTERED_BOOTSTRAP_PRECISION_REQUIRED")
    rng = random.Random(6101)
    totals = tuple((sum(v, ZERO), len(v)) for _, v in sorted(groups.items()))
    estimates = []
    for _ in range(resamples):
        sample = [rng.choice(totals) for _ in totals]
        estimates.append(sum((s for s, _ in sample), ZERO) / sum(n for _, n in sample))
    estimates.sort()
    allowance = max(
        ZERO,
        sum((sum(v, ZERO) for v in groups.values()), ZERO) / sum(len(v) for v in groups.values()),
        estimates[min(len(estimates) - 1, math.ceil((1 - alpha) * len(estimates)) - 1)],
    )
    return allowance


def calibrate_verified_selected_policy(repo, *, model, protocol, residuals, cutoff_ns, expires_at_ns):
    from atlas.v2.science.mean_evaluation import index_mean_artifact

    reader = EvidenceReader(repo, cutoff_ns=cutoff_ns, now_ns=cutoff_ns)
    protocol = reader.read(protocol.content_hash, MeanCalibrationProtocolV1)
    scope, values = verify_oof_residuals(reader, protocol, tuple(r.content_hash for r in residuals))
    reader.read(model.content_hash, MeanModelV1)
    if (
        model.compatibility_ref != scope.compatibility_ref
        or model.provenance != scope.provenance
        or model.estimator != protocol.estimator
        or model.parameters.get("ridge") != protocol.ridge
    ):
        raise ValueError("SERVING_MODEL_CELL_CHANGED")
    verify_serving_training(reader, model, scope, protocol)
    groups, effective, lags = dependence_diagnostics(values, protocol)
    allowance = selected_policy_upper_cash(groups, alpha=protocol.alpha, resamples=protocol.resamples)
    qualification = MeanDependenceQualificationV1(
        protocol.content_hash,
        model.content_hash,
        tuple(r.content_hash for r in residuals),
        cutoff_ns,
        expires_at_ns,
        effective,
        lags,
        allowance,
    )
    index_mean_artifact(repo, qualification, available_at_ns=cutoff_ns)
    calibration = MeanSelectionCalibrationV2(
        model.content_hash,
        scope.policy_ref,
        scope.compatibility_ref,
        scope.content_hash,
        cutoff_ns,
        expires_at_ns,
        protocol.block_ns,
        effective,
        qualification.residual_refs,
        allowance,
        "SUPPORTED_OFFLINE",
        "REGISTERED_POLICY_COMPATIBLE_ACTION_SIZE_LIQUIDITY_CELL",
        qualification.content_hash,
    )
    index_mean_artifact(repo, calibration, available_at_ns=cutoff_ns)
    return calibration


def verify_mean_calibration(repo, calibration, *, model, policy_ref, product_ref, cutoff_ns, now_ns):
    if not isinstance(calibration, MeanSelectionCalibrationV2):
        raise ValueError("UNVERIFIED_LEGACY_CALIBRATION")
    reader = EvidenceReader(repo, cutoff_ns=cutoff_ns, now_ns=now_ns)
    stored = reader.read(calibration.content_hash, MeanSelectionCalibrationV2)
    proof = reader.read(stored.qualification_ref, MeanDependenceQualificationV1)
    protocol = reader.read(proof.protocol_ref, MeanCalibrationProtocolV1)
    scope, values = verify_oof_residuals(reader, protocol, proof.residual_refs)
    verify_serving_training(reader, model, scope, protocol)
    _, effective, lags = dependence_diagnostics(values, protocol)
    empirical_error = sum((value for _, value in values), ZERO) / len(values)
    if proof.allowance_cash < max(ZERO, empirical_error):
        raise ValueError("CALIBRATION_ALLOWANCE_BELOW_VERIFIED_MEAN_ERROR")
    if (
        calibration != stored
        or calibration.model_ref != model.content_hash
        or proof.serving_model_ref != model.content_hash
        or scope.policy_ref != policy_ref
        or scope.product_ref != product_ref
        or scope.compatibility_ref != model.compatibility_ref
        or scope.provenance != model.provenance
        or model.estimator != protocol.estimator
        or model.parameters.get("ridge") != protocol.ridge
        or model.support_min != scope.support_min
        or model.support_max != scope.support_max
        or calibration.source_ref != scope.content_hash
        or calibration.policy_ref != policy_ref
        or calibration.compatibility_ref != scope.compatibility_ref
        or calibration.block_ns != protocol.block_ns
        or calibration.calibration_refs != proof.residual_refs
        or proof.effective_blocks != effective
        or calibration.effective_blocks != effective
        or proof.lag_correlations != lags
        or calibration.allowance_cash != proof.allowance_cash
        or calibration.status != "SUPPORTED_OFFLINE"
        or proof.available_at_ns != calibration.available_at_ns
        or proof.expires_at_ns != calibration.expires_at_ns
    ):
        raise ValueError("CALIBRATION_PROOF_REVISIONS_CHANGED")
    return scope


def produce_verified_mean_oof(repo, rows, *, protocol, provenance="CAUSAL_REPLAY", minimum_rows=30):
    """Persist actual models/rows/predictions produced by the existing OOF loop."""
    from atlas.v2.science.mean_evaluation import index_mean_artifact
    from atlas.v2.science.mean_research import chronological_mean_oof

    reader = EvidenceReader(repo, cutoff_ns=protocol.end_ns, now_ns=protocol.end_ns)
    scope = reader.read(protocol.scope_ref, MeanEvidenceScopeV1, by_ns=protocol.registered_at_ns)
    hold = _holdout(reader, protocol, scope)
    for row in rows:
        _row(reader, row.content_hash, scope, hold, by_ns=protocol.end_ns)
    residuals = []

    def record(row, model, training, value):
        if not protocol.start_ns <= row.origin_ns < protocol.end_ns:
            return
        index_mean_artifact(repo, model, available_at_ns=model.available_at_ns)
        prediction = MeanOOFPredictionV1(
            protocol.content_hash,
            scope.content_hash,
            model.content_hash,
            tuple(r.content_hash for r in training),
            row.action_ref,
            row.origin_ns,
            row.origin_ns,
            row.features,
            value,
        )
        index_mean_artifact(repo, prediction, available_at_ns=row.origin_ns)
        residual = SelectedPolicyResidualV2(prediction.content_hash, row.content_hash, row.label_available_at_ns)
        index_mean_artifact(repo, residual, available_at_ns=row.label_available_at_ns)
        residuals.append(residual)

    chronological_mean_oof(
        rows,
        compatibility_ref=scope.compatibility_ref,
        provenance=provenance,
        embargo_ns=protocol.embargo_ns,
        minimum_rows=minimum_rows,
        protected_outcome_refs=hold.outcome_refs,
        on_prediction=record,
    )
    return tuple(residuals)
