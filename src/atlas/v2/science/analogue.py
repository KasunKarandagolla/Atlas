"""Causal, compatibility-first numerical analogue retrieval baseline."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import json_value, sha256_json, sha256_ref
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.m0 import action_features
from atlas.v2.science.outcomes import MaturedOutcomeV2, executable_action_value_training_eligible, index_matured_outcome

ANALOGUE_POLICY_ID = "CAUSAL_ANALOGUE_ACTION_VALUE_V1"
ANALOGUE_POLICY_VERSION = "1.0.0-research"
ANALOGUE_RESULT_VERSION = "ANALOGUE_ACTION_VALUE_V2_V1"
ANALOGUE_SUPPORT_VERSION = "ANALOGUE_SUPPORT_V2_V1"
DEFAULT_NEIGHBORS = 30
MIN_INDEPENDENT_SUPPORT = 20
DEFAULT_EMBARGO_NS = 24 * 60 * 60 * 1_000_000_000
OOD_DISTANCE_LIMIT = 8.0
ANALOGUE_POLICY_BODY = {
    "policy_id": ANALOGUE_POLICY_ID, "version": ANALOGUE_POLICY_VERSION,
    "compatibility_before_distance": ["policy", "action_semantics", "side", "horizon", "venue", "product",
        "execution_mode", "quantity_participation", "liquidity", "feature_schema", "availability", "costs"],
    "distance": "TRAINING_ONLY_MEDIAN_MAD_EUCLIDEAN_AVAILABLE_DIMENSIONS_V1",
    "support": "OVERLAPPING_LABEL_AND_EPISODE_COMPONENTS_AT_LEAST_20",
    "effective_support_floor": MIN_INDEPENDENT_SUPPORT,
    "neighbors": DEFAULT_NEIGHBORS, "capital_authority": "ZERO", "independent_vote": False,
}
ANALOGUE_POLICY_HASH = sha256_json(ANALOGUE_POLICY_BODY)


def _number(value: float | None) -> float:
    if value is None:
        raise ValueError("analogue distance cannot impute an unavailable required dimension")
    return float(value)


@dataclass(frozen=True)
class AnalogueCompatibilityV2:
    policy_hash: str
    side: str
    action_representation_hash: str
    holding_horizon_ns: int
    venue: str
    product: str
    execution_mode: str
    quantity_participation_key: str
    liquidity_regime_key: str
    feature_schema_hash: str
    feature_availability: tuple[bool, ...]
    cost_semantics_hash: str

    def __post_init__(self) -> None:
        for name in ("policy_hash", "action_representation_hash", "feature_schema_hash", "cost_semantics_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.side not in ("LONG", "SHORT") or self.holding_horizon_ns <= 0:
            raise ValueError("analogue action side/horizon invalid")
        if not all((self.venue, self.product, self.execution_mode, self.quantity_participation_key,
                    self.liquidity_regime_key)):
            raise ValueError("analogue compatibility dimensions must be explicit")
        if not self.feature_availability or any(type(bit) is not bool for bit in self.feature_availability):
            raise ValueError("analogue feature availability signature must be explicit")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ANALOGUE_COMPATIBILITY_V1", **{
            name: list(value) if isinstance(value, tuple) else value for name, value in self.__dict__.items()}}

    @property
    def compatibility_key(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AnalogueTrainingObservationV2:
    outcome_ref: str
    action_hash: str
    candidate_ref: str
    candidate_set_ref: str
    compatibility_key: str
    feature_names: tuple[str, ...]
    values: tuple[float | None, ...]
    missing_features: tuple[str, ...]
    decision_at_ns: int
    horizon_end_ns: int
    label_available_at_ns: int
    episode_id: str
    regime_id: str
    net_payoff: Decimal
    provenance: str
    execution_state: str
    eligibility_ref: str

    def __post_init__(self) -> None:
        for name in ("outcome_ref", "action_hash", "candidate_ref", "candidate_set_ref",
                     "compatibility_key", "episode_id", "eligibility_ref"):
            sha256_ref(getattr(self, name), field=name)
        if tuple(sorted(set(self.feature_names))) != self.feature_names or len(self.values) != len(self.feature_names):
            raise ValueError("analogue feature names/values must be ordered and fixed width")
        if tuple(sorted(set(self.missing_features))) != self.missing_features:
            raise ValueError("analogue missingness must be sorted and unique")
        if any(value is not None and not math.isfinite(value) for value in self.values):
            raise ValueError("analogue feature value must be finite or explicitly missing")
        if not (self.decision_at_ns < self.horizon_end_ns <= self.label_available_at_ns):
            raise ValueError("analogue label is not fully matured")
        if not self.regime_id:
            raise ValueError("analogue regime identity is required")
        if self.missing_features != tuple(name for name, value in zip(self.feature_names, self.values, strict=True) if value is None):
            raise ValueError("analogue observation missingness contradicts values")
        if self.provenance not in {"ACTUAL", "SIMULATED", "COUNTERFACTUAL"} or self.execution_state not in {
            "NO_FILL", "PARTIAL_FILL", "FULL_FILL"}:
            raise ValueError("analogue observations require explicit eligible provenance and fill states")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ANALOGUE_TRAINING_OBSERVATION_V1", "outcome_ref": self.outcome_ref,
            "action_hash": self.action_hash, "candidate_ref": self.candidate_ref,
            "candidate_set_ref": self.candidate_set_ref, "compatibility_key": self.compatibility_key,
            "feature_names": list(self.feature_names), "values": list(self.values),
            "missing_features": list(self.missing_features), "decision_at_ns": self.decision_at_ns,
            "horizon_end_ns": self.horizon_end_ns, "label_available_at_ns": self.label_available_at_ns,
            "episode_id": self.episode_id, "regime_id": self.regime_id,
            "net_payoff": canonical_decimal_str(self.net_payoff), "provenance": self.provenance,
            "execution_state": self.execution_state, "eligibility_ref": self.eligibility_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AnalogueQueryV2:
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    information_cutoff_ns: int
    target_horizon_end_ns: int
    compatibility: AnalogueCompatibilityV2
    feature_names: tuple[str, ...]
    values: tuple[float | None, ...]
    feature_available_at_ns: tuple[int | None, ...]
    missing_features: tuple[str, ...]
    regime_id: str

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.target_horizon_end_ns <= self.information_cutoff_ns:
            raise ValueError("analogue query target horizon must follow its cutoff")
        if len(self.feature_names) != len(self.values) or len(self.values) != len(self.feature_available_at_ns):
            raise ValueError("analogue query feature vectors differ in width")
        if any(value is not None and not math.isfinite(value) for value in self.values):
            raise ValueError("analogue query feature values must be finite or explicitly missing")
        if self.feature_names != tuple(sorted(set(self.feature_names))):
            raise ValueError("analogue query feature names must be sorted/unique")
        if self.compatibility.feature_availability != tuple(value is not None for value in self.values):
            raise ValueError("analogue query availability mask contradicts feature values")
        if any(at is not None and at > self.information_cutoff_ns for at in self.feature_available_at_ns):
            raise ValueError("analogue query contains future/unavailable feature evidence")
        if any((value is None) != (at is None) for value, at in
            zip(self.values, self.feature_available_at_ns, strict=True)):
            raise ValueError("analogue present values require causal availability")
        if self.missing_features != tuple(name for name, value in zip(self.feature_names, self.values, strict=True) if value is None):
            raise ValueError("analogue query missingness contradicts values")
        if not self.regime_id:
            raise ValueError("analogue current regime must be named")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ANALOGUE_QUERY_V2_V1", **{name: list(value) if isinstance(value, tuple)
            else value for name, value in self.__dict__.items() if name != "compatibility"},
            "compatibility": self.compatibility.to_dict(), "policy_hash": ANALOGUE_POLICY_HASH}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AnalogueNeighborV2:
    outcome_ref: str
    action_hash: str
    episode_id: str
    decision_at_ns: int
    distance: float
    weight: float
    net_payoff: Decimal
    provenance: str
    execution_state: str

    def to_dict(self) -> dict[str, Any]:
        return {"outcome_ref": self.outcome_ref, "action_hash": self.action_hash,
            "episode_id": self.episode_id, "decision_at_ns": self.decision_at_ns,
            "distance": self.distance, "weight": self.weight,
            "net_payoff": canonical_decimal_str(self.net_payoff), "provenance": self.provenance,
            "execution_state": self.execution_state}


@dataclass(frozen=True)
class AnalogueActionValueV2:
    query_action_hash: str
    query_action_ref: str
    query_candidate_ref: str
    query_candidate_set_ref: str
    information_cutoff_ns: int
    compatibility_key: str
    compatible_population_count: int
    neighbor_refs: tuple[str, ...]
    neighbors: tuple[AnalogueNeighborV2, ...]
    weighted_estimate: Decimal | None
    payoff_dispersion: Decimal | None
    effective_sample_size: Decimal | None
    independent_support_count: int
    temporal_concentration: Decimal | None
    regime_coverage: tuple[tuple[str, int], ...]
    missing_features: tuple[str, ...]
    explanation_fields: tuple[tuple[str, Decimal], ...]
    nearest_distance: float | None
    ood_status: str
    support_status: str
    reasons: tuple[str, ...]
    scaler_training_refs: tuple[str, ...] = ()
    scaler_feature_names: tuple[str, ...] = ()
    scaler_centers: tuple[float, ...] = ()
    scaler_scales: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        for name in ("query_action_hash", "query_action_ref", "query_candidate_ref",
                     "query_candidate_set_ref", "compatibility_key"):
            sha256_ref(getattr(self, name), field=name)
        if self.compatible_population_count < 0 or self.independent_support_count < 0:
            raise ValueError("analogue support counts cannot be negative")
        if self.neighbor_refs != tuple(neighbor.outcome_ref for neighbor in self.neighbors):
            raise ValueError("analogue neighbor refs/order mismatch")
        if any(self.neighbors[i].distance > self.neighbors[i + 1].distance
            for i in range(len(self.neighbors) - 1)):
            raise ValueError("analogue neighbors must be sorted by deterministic distance")

    def to_dict(self) -> dict[str, Any]:
        return {"version": ANALOGUE_RESULT_VERSION, "policy_id": ANALOGUE_POLICY_ID,
            "policy_hash": ANALOGUE_POLICY_HASH,
            "policy_version": ANALOGUE_POLICY_VERSION, "query_action_hash": self.query_action_hash,
            "query_action_ref": self.query_action_ref, "query_candidate_ref": self.query_candidate_ref,
            "query_candidate_set_ref": self.query_candidate_set_ref,
            "information_cutoff_ns": self.information_cutoff_ns, "compatibility_key": self.compatibility_key,
            "compatible_population_count": self.compatible_population_count,
            "neighbor_refs": list(self.neighbor_refs), "neighbors": [n.to_dict() for n in self.neighbors],
            "weighted_estimate": canonical_decimal_str(self.weighted_estimate) if self.weighted_estimate is not None else None,
            "payoff_dispersion": canonical_decimal_str(self.payoff_dispersion) if self.payoff_dispersion is not None else None,
            "effective_sample_size": canonical_decimal_str(self.effective_sample_size) if self.effective_sample_size is not None else None,
            "independent_support_count": self.independent_support_count,
            "temporal_concentration": canonical_decimal_str(self.temporal_concentration)
                if self.temporal_concentration is not None else None,
            "regime_coverage": [[name, count] for name, count in self.regime_coverage],
            "missing_features": list(self.missing_features),
            "explanation_fields": [[name, canonical_decimal_str(value)] for name, value in self.explanation_fields],
            "nearest_distance": self.nearest_distance, "ood_status": self.ood_status,
            "support_status": self.support_status, "reasons": list(self.reasons),
            "scaler_training_refs": list(self.scaler_training_refs),
            "scaler_feature_names": list(self.scaler_feature_names),
            "scaler_centers": list(self.scaler_centers), "scaler_scales": list(self.scaler_scales)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _robust_scaler(rows: Sequence[AnalogueTrainingObservationV2], dimensions: Sequence[int]
        ) -> tuple[tuple[float, ...], tuple[float, ...]]:
    centers: list[float] = []
    scales: list[float] = []
    for index in dimensions:
        values = [_number(row.values[index]) for row in rows if row.values[index] is not None]
        if not values:
            centers.append(0.0)
            scales.append(1.0)
            continue
        center = statistics.median(values)
        scale = statistics.median(abs(value - center) for value in values) * 1.4826
        centers.append(center)
        scales.append(scale if scale > 1e-9 else 1.0)
    return tuple(centers), tuple(scales)


def estimate_analogue(query: AnalogueQueryV2, observations: Sequence[AnalogueTrainingObservationV2], *,
        neighbors: int = DEFAULT_NEIGHBORS, minimum_independent_support: int = MIN_INDEPENDENT_SUPPORT,
        embargo_ns: int = DEFAULT_EMBARGO_NS) -> AnalogueActionValueV2:
    """Retrieve only compatible, cutoff- and maturity-valid exact-action labels."""
    if neighbors < 1 or minimum_independent_support < MIN_INDEPENDENT_SUPPORT or embargo_ns < query.target_horizon_end_ns - query.information_cutoff_ns:
        raise ValueError("analogue neighbor/support/embargo configuration invalid")
    if any(tuple(row.feature_names) != query.feature_names for row in observations):
        raise ValueError("analogue feature schema mismatch")
    unique: dict[str, AnalogueTrainingObservationV2] = {}
    for row in observations:
        if row.outcome_ref in unique and unique[row.outcome_ref] != row:
            raise ValueError("analogue outcome cannot have contradictory revised features")
        unique[row.outcome_ref] = row
    compatible = tuple(sorted((row for row in unique.values()
        if row.compatibility_key == query.compatibility.compatibility_key
        and row.decision_at_ns < query.information_cutoff_ns
        and row.label_available_at_ns <= query.information_cutoff_ns
        and row.horizon_end_ns <= query.information_cutoff_ns - embargo_ns
        and row.action_hash != query.action_hash
        and row.horizon_end_ns - row.decision_at_ns == query.compatibility.holding_horizon_ns
        and len(row.values) == len(query.values)
        and tuple(value is not None for value in row.values) == query.compatibility.feature_availability),
        key=lambda row: (row.decision_at_ns, row.outcome_ref)))
    dimensions = tuple(i for i, value in enumerate(query.values) if value is not None)
    centers, scales = _robust_scaler(compatible, dimensions) if compatible and dimensions else ((), ())
    distances: list[tuple[float, AnalogueTrainingObservationV2]] = []
    for row in compatible:
        components = [(_number(row.values[index]) - _number(query.values[index])) / scale
            for index, scale in zip(dimensions, scales, strict=True)]
        distance = math.sqrt(sum(value * value for value in components) / max(1, len(components)))
        distances.append((distance, row))
    distances.sort(key=lambda item: (item[0], item[1].decision_at_ns, item[1].action_hash, item[1].outcome_ref))
    selected = distances[:neighbors]
    raw_weights = [1.0 / (distance + 1e-6) for distance, _ in selected]
    weight_sum = sum(raw_weights)
    normalized = [weight / weight_sum for weight in raw_weights] if weight_sum > 0 else []
    neighbor_rows = tuple(AnalogueNeighborV2(row.outcome_ref, row.action_hash, row.episode_id,
        row.decision_at_ns, distance, weight, row.net_payoff, row.provenance, row.execution_state)
        for (distance, row), weight in zip(selected, normalized, strict=True))
    # Aggregate weights by independent episode before computing effective N;
    # repeated rows from one episode cannot masquerade as independent support.
    parents = list(range(len(selected)))

    def root(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for i, (_, left) in enumerate(selected):
        for j, (_, right) in enumerate(selected[:i]):
            if left.episode_id == right.episode_id or max(left.decision_at_ns, right.decision_at_ns) < min(
                left.horizon_end_ns, right.horizon_end_ns):
                parents[root(i)] = root(j)
    episode_weights: dict[str, float] = defaultdict(float)
    episode_payoffs: dict[str, list[tuple[float, float]]] = defaultdict(list)
    episode_regimes: dict[str, str] = {}
    for index, (neighbor, (_, row)) in enumerate(zip(neighbor_rows, selected, strict=True)):
        component = str(root(index))
        episode_weights[component] += neighbor.weight
        episode_payoffs[component].append((neighbor.weight, float(row.net_payoff)))
        episode_regimes[component] = row.regime_id
    episode_sum = sum(episode_weights.values())
    episode_weight_values = [weight / episode_sum for weight in episode_weights.values()] if episode_sum else []
    ess = (1.0 / sum(weight * weight for weight in episode_weight_values)) if episode_weight_values else None
    independent_count = len(episode_weights)
    weighted_estimate: Decimal | None = None
    dispersion: Decimal | None = None
    if neighbor_rows:
        estimate = sum(neighbor.weight * float(neighbor.net_payoff) for neighbor in neighbor_rows)
        variance = sum(neighbor.weight * (float(neighbor.net_payoff) - estimate) ** 2 for neighbor in neighbor_rows)
        weighted_estimate = Decimal(str(estimate))
        dispersion = Decimal(str(math.sqrt(max(0.0, variance))))
    temporal: Decimal | None = None
    if neighbor_rows:
        by_week: dict[int, float] = defaultdict(float)
        for neighbor in neighbor_rows:
            by_week[neighbor.decision_at_ns // (7 * 24 * 60 * 60 * 1_000_000_000)] += neighbor.weight
        temporal = Decimal(str(max(by_week.values(), default=0.0)))
    regimes: dict[str, int] = defaultdict(int)
    for regime in episode_regimes.values():
        regimes[regime] += 1
    missing = tuple(sorted(name for i, name in enumerate(query.feature_names) if query.values[i] is None))
    diffs = []
    for index, center, scale in zip(dimensions if compatible else (), centers, scales, strict=True):
        delta = abs((_number(query.values[index]) - center) / scale)
        diffs.append((delta, query.feature_names[index]))
    explanation = tuple((name, Decimal(str(value))) for value, name in sorted(diffs, reverse=True)[:5])
    nearest = selected[0][0] if selected else None
    ood = "NOT_ESTIMABLE" if nearest is None else "OOD" if nearest > OOD_DISTANCE_LIMIT else "IN_DISTRIBUTION"
    reasons: list[str] = []
    if len(compatible) < neighbors:
        reasons.append("COMPATIBLE_POPULATION_BELOW_NEIGHBOR_BUDGET")
    if independent_count < minimum_independent_support:
        reasons.append("INSUFFICIENT_INDEPENDENT_EPISODE_SUPPORT")
    if ess is None or ess + 1e-9 < minimum_independent_support:
        reasons.append("INSUFFICIENT_EFFECTIVE_EPISODE_WEIGHT_SUPPORT")
    if temporal is not None and temporal > Decimal("0.50"):
        reasons.append("TEMPORAL_CONCENTRATION_TOO_HIGH")
    if not dimensions:
        reasons.append("NO_COMMON_AVAILABLE_NUMERIC_FEATURES")
    if query.compatibility.liquidity_regime_key in {"UNKNOWN", "UNAVAILABLE"}:
        reasons.append("LIQUIDITY_COMPATIBILITY_NOT_ESTIMABLE")
    if ood in ("OOD", "NOT_ESTIMABLE"):
        reasons.append("LOCAL_ANALOGUE_STATE_OOD_OR_UNSUPPORTED")
    support_status = "SUPPORTED" if not reasons else "NOT_ESTIMABLE"
    if support_status != "SUPPORTED":
        weighted_estimate = None
        dispersion = None
    return AnalogueActionValueV2(query.action_hash, query.action_artifact_ref, query.candidate_ref,
        query.candidate_set_ref, query.information_cutoff_ns, query.compatibility.compatibility_key,
        len(compatible), tuple(row.outcome_ref for row in neighbor_rows), neighbor_rows,
        weighted_estimate, dispersion, Decimal(str(ess)) if ess is not None else None,
        independent_count, temporal, tuple(sorted(regimes.items())), missing, explanation, nearest,
        ood, support_status, tuple(sorted(reasons)), tuple(sorted(row.outcome_ref for row in compatible)),
        tuple(query.feature_names[index] for index in dimensions) if compatible else (), centers, scales)


def action_semantics_hash(identity: Mapping[str, Any]) -> str:
    return sha256_json({name: identity[name] for name in (
        "policy_hash", "entry_rule", "collar_rule", "management_rule", "time_exit_rule",
        "entry_trigger_basis", "stop_trigger_basis")})


def _causal_values(repo: OpsRepository, action_ref: str, feature_names: tuple[str, ...], cutoff_ns: int
        ) -> tuple[tuple[float | None, ...], str]:
    vector = action_features(repo, action_ref, cutoff_ns=cutoff_ns)
    indexed = dict(zip(vector.feature_order, vector.values, strict=True))
    if any(name not in indexed or name.startswith("missing:") for name in feature_names):
        raise ValueError("analogue features must be a declared subset of causal M0 action evidence")
    values = tuple(None if name.startswith("value:") and indexed.get("missing:" + name[6:]) else indexed[name]
        for name in feature_names)
    return values, vector.content_hash


def _validate_action_compatibility(repo: OpsRepository, action_ref: str, *, decision_at_ns: int,
        compatibility: AnalogueCompatibilityV2, feature_names: tuple[str, ...], values: tuple[float | None, ...]) -> None:
    entry = repo.get_artifact(action_ref)
    identity = entry.metadata.get("action_identity") if entry else None
    if not isinstance(identity, Mapping):
        raise ValueError("analogue exact action identity is unavailable")
    key = InstrumentKeyV2.from_dict(identity["key"])
    if (identity["policy_hash"] != compatibility.policy_hash or identity["side"] != compatibility.side
        or int(identity["horizon_end_ns"]) - decision_at_ns != compatibility.holding_horizon_ns
        or key.venue.value != compatibility.venue or key.product.value != compatibility.product
        or action_semantics_hash(identity) != compatibility.action_representation_hash
        or sha256_json(list(feature_names)) != compatibility.feature_schema_hash
        or tuple(value is not None for value in values) != compatibility.feature_availability):
        raise ValueError("analogue compatibility contradicts its frozen action or causal feature schema")


def build_analogue_query(repo: OpsRepository, *, action_ref: str, candidate_ref: str, candidate_set_ref: str,
        cutoff_ns: int, compatibility: AnalogueCompatibilityV2, feature_names: tuple[str, ...],
        regime_id: str) -> AnalogueQueryV2:
    entry = repo.get_artifact(action_ref)
    body = entry.metadata.get("action_artifact") if entry else None
    identity = entry.metadata.get("action_identity") if entry else None
    if not isinstance(body, Mapping) or not isinstance(identity, Mapping) or body.get("candidate_ref") != candidate_ref or body.get("candidate_set_ref") != candidate_set_ref:
        raise ValueError("analogue query must bind the same selected frozen action")
    values, _ = _causal_values(repo, action_ref, feature_names, cutoff_ns)
    _validate_action_compatibility(repo, action_ref, decision_at_ns=cutoff_ns,
        compatibility=compatibility, feature_names=feature_names, values=values)
    return AnalogueQueryV2(str(body["action_hash"]), action_ref, candidate_ref, candidate_set_ref,
        cutoff_ns, int(identity["horizon_end_ns"]), compatibility, feature_names, values,
        tuple(cutoff_ns if value is not None else None for value in values),
        tuple(name for name, value in zip(feature_names, values, strict=True) if value is None), regime_id)


def observation_from_matured_outcome(repo: OpsRepository, *, outcome_ref: str, cutoff_ns: int,
        compatibility: AnalogueCompatibilityV2, feature_names: tuple[str, ...], episode_id: str,
        regime_id: str) -> AnalogueTrainingObservationV2:
    entry = repo.get_artifact(outcome_ref)
    raw = entry.metadata.get("outcome") if entry else None
    if entry is None or entry.artifact_type != "MaturedOutcomeV2" or not isinstance(raw, Mapping):
        raise ValueError("analogue source must be an indexed honest matured outcome")
    outcome = MaturedOutcomeV2.from_dict(json_value(raw))
    if outcome.content_hash != outcome_ref or not executable_action_value_training_eligible(outcome, cutoff_ns):
        raise ValueError("analogue source label is future, unmatured or ineligible")
    index_matured_outcome(repo, outcome)
    required_mode = "ACTUAL" if outcome.provenance.value == "ACTUAL" else "MINUTE_REPLAY"
    if compatibility.execution_mode != required_mode:
        raise ValueError("analogue execution mode contradicts the honest outcome provenance")
    assert outcome.action_artifact_ref and outcome.action_hash and outcome.candidate_ref
    assert outcome.net_payoff is not None
    values, feature_ref = _causal_values(repo, outcome.action_artifact_ref, feature_names, cutoff_ns)
    _validate_action_compatibility(repo, outcome.action_artifact_ref, decision_at_ns=outcome.decision_at_ns,
        compatibility=compatibility, feature_names=feature_names, values=values)
    witness = sha256_json({"version": "ANALOGUE_HONEST_TRAINING_ELIGIBILITY_V1", "outcome_ref": outcome_ref,
        "execution_evidence_ref": outcome.execution_evidence_ref, "feature_ref": feature_ref,
        "compatibility_key": compatibility.compatibility_key})
    return AnalogueTrainingObservationV2(outcome_ref, outcome.action_hash, outcome.candidate_ref,
        outcome.candidate_set_ref, compatibility.compatibility_key, feature_names, values,
        tuple(name for name, value in zip(feature_names, values, strict=True) if value is None),
        outcome.decision_at_ns, outcome.horizon_end_ns, outcome.available_at_ns, episode_id, regime_id,
        outcome.net_payoff, outcome.provenance.value, outcome.execution_state.value, witness)


def estimate_causal_analogue(repo: OpsRepository, query: AnalogueQueryV2,
        observations: Sequence[AnalogueTrainingObservationV2], *,
        compatibility_contracts: Mapping[str, AnalogueCompatibilityV2]) -> AnalogueActionValueV2:
    """Repository seam: revalidate exact labels/features before numerical retrieval."""
    reproduced = build_analogue_query(repo, action_ref=query.action_artifact_ref, candidate_ref=query.candidate_ref,
        candidate_set_ref=query.candidate_set_ref, cutoff_ns=query.information_cutoff_ns,
        compatibility=query.compatibility, feature_names=query.feature_names, regime_id=query.regime_id)
    if reproduced != query:
        raise ValueError("analogue query does not reproduce its original cutoff features")
    eligible = []
    for observation in observations:
        if observation.decision_at_ns >= query.information_cutoff_ns or observation.label_available_at_ns > query.information_cutoff_ns:
            continue
        compatibility = compatibility_contracts.get(observation.compatibility_key)
        if compatibility is None:
            raise ValueError("analogue source compatibility body is missing")
        source = observation_from_matured_outcome(repo, outcome_ref=observation.outcome_ref,
            cutoff_ns=query.information_cutoff_ns, compatibility=compatibility,
            feature_names=observation.feature_names, episode_id=observation.episode_id, regime_id=observation.regime_id)
        if source != observation:
            raise ValueError("analogue source was revised or does not bind the honest label/features")
        eligible.append(source)
    return estimate_analogue(query, eligible)


def persist_analogue(repo: Any, result: AnalogueActionValueV2, *, available_at_ns: int) -> str:
    """Index the immutable result using the standard ops artifact envelope/index seam."""
    from atlas.v2.memory.repository import ArtifactIndexEntryV2

    if available_at_ns < result.information_cutoff_ns:
        raise ValueError("analogue output cannot precede its action cutoff")
    body = result.to_dict()
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "AnalogueActionValueV2", ref,
        available_at_ns, available_at_ns, {"analogue": body}))
    return ref
