"""Causal, compatibility-first numerical analogue retrieval baseline."""

from __future__ import annotations

import hashlib
import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import json_value, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateActionV2, FeatureArtifactV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.costs import ActionCostContractV2, FeeScheduleV2, FundingScheduleV2
from atlas.v2.science.m0 import action_features
from atlas.v2.science.outcomes import MaturedOutcomeV2, executable_action_value_training_eligible, index_matured_outcome
from atlas.v2.science.replay import ReplayAssumptionsV2

ANALOGUE_POLICY_ID = "CAUSAL_ANALOGUE_ACTION_VALUE_V1"
ANALOGUE_POLICY_VERSION = "2.0.0-research"
ANALOGUE_RESULT_VERSION = "ANALOGUE_ACTION_VALUE_V2_V2"
ANALOGUE_SUPPORT_VERSION = "ANALOGUE_SUPPORT_V2_V1"
DEFAULT_NEIGHBORS = 30
MIN_INDEPENDENT_SUPPORT = 20
DEFAULT_EMBARGO_NS = 24 * 60 * 60 * 1_000_000_000
OOD_DISTANCE_LIMIT = 8.0


class AnalogueNotEstimableError(ValueError):
    """A named required compatibility input is missing or cannot be reconstructed."""

    def __init__(self, reason: str) -> None:
        if not reason.startswith("NOT_ESTIMABLE_"):
            raise ValueError("analogue not-estimable reasons must be named")
        self.reason = reason
        super().__init__(reason)


ANALOGUE_POLICY_BODY = {
    "policy_id": ANALOGUE_POLICY_ID, "version": ANALOGUE_POLICY_VERSION,
    "compatibility_before_distance": ["policy", "action_semantics", "side", "horizon", "venue", "product",
        "execution_contract", "quantity_participation", "liquidity_evidence", "feature_schema",
        "feature_availability", "cost_contract"],
    "compatibility_key": "DERIVED_FROM_FROZEN_ACTION_AND_CUTOFF_KNOWN_TYPED_EVIDENCE_V2",
    "execution_provenance": "OUTCOME_ONLY_NOT_ACTION_EXECUTION_SEMANTICS",
    "liquidity_evidence": "SEQUENCE_VALID_CUTOFF_KNOWN_S4_BOOK_V1",
    "cost_evidence": "PRODUCT_BOUND_VERSIONED_FEE_AND_FUNDING_CONTRACTS_V1",
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
    instrument_key_hash: str
    execution_contract_hash: str
    quantity_participation_key: str
    liquidity_regime_key: str
    feature_schema_hash: str
    feature_availability: tuple[bool, ...]
    cost_semantics_hash: str

    def __post_init__(self) -> None:
        for name in ("policy_hash", "action_representation_hash", "instrument_key_hash",
                     "execution_contract_hash", "quantity_participation_key", "liquidity_regime_key",
                     "feature_schema_hash", "cost_semantics_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.side not in ("LONG", "SHORT") or self.holding_horizon_ns <= 0:
            raise ValueError("analogue action side/horizon invalid")
        if not all((self.venue, self.product, self.quantity_participation_key,
                    self.liquidity_regime_key)):
            raise ValueError("analogue compatibility dimensions must be explicit")
        if not self.feature_availability or any(type(bit) is not bool for bit in self.feature_availability):
            raise ValueError("analogue feature availability signature must be explicit")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ANALOGUE_COMPATIBILITY_V2", **{
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
    compatibility_key: str | None
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
        for name in ("query_action_hash", "query_action_ref", "query_candidate_ref", "query_candidate_set_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.compatibility_key is not None:
            sha256_ref(self.compatibility_key, field="compatibility_key")
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
        and len(row.values) == len(query.values)),
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
    fields = ("key", "side", "quantity", "product_ref", "entry_rule", "collar_rule", "entry_reference",
        "entry_collar", "stop_price", "entry_trigger_basis", "stop_trigger_basis", "management_rule",
        "time_exit_rule", "horizon_end_ns", "policy_id", "policy_version", "policy_hash",
        "risk_policy_hash", "risk_policy_v2_hash")
    if any(name not in identity for name in fields):
        raise ValueError("frozen action execution contract is incomplete")
    return sha256_json({"version": "ANALOGUE_FROZEN_ACTION_SEMANTICS_V2",
        **{name: identity[name] for name in fields}})


def _indexed_body(repo: OpsRepository, ref: str, artifact_type: str,
        body_name: str | None) -> tuple[Any, Mapping[str, Any]]:
    entry = repo.get_artifact(ref)
    body = entry.metadata.get(body_name) if entry is not None and body_name else entry.metadata if entry is not None else None
    if entry is None or entry.artifact_type != artifact_type or not isinstance(body, Mapping):
        raise AnalogueNotEstimableError(f"NOT_ESTIMABLE_MISSING_TYPED_{artifact_type.upper()}")
    return entry, body


def _feature_schema(feature: FeatureArtifactV2) -> tuple[str, tuple[bool, ...]]:
    names = tuple(sorted(feature.values))
    schema = sha256_json({"version": "ANALOGUE_CAUSAL_FEATURE_SCHEMA_V2",
        "feature_set_version": feature.feature_set_version,
        "features": [[name, feature.values[name].unit] for name in names]})
    availability = tuple(feature.values[name].value is not None and not feature.values[name].missing_reason
        for name in names)
    return schema, availability


def _indexed_l2_payload(repo: OpsRepository, raw_ref: str, *, key: InstrumentKeyV2,
        decision_at_ns: int) -> bool:
    """Resolve S4 raw payload hashes to immutable archived frame records."""
    candidates = tuple(entry for entry in repo.artifact_entries_by_types(("L2RawFrameV2",), limit=10_000)
        if entry.metadata.get("raw_payload_hash") == raw_ref)
    for entry in candidates:
        body = entry.metadata
        payload_hex = body.get("raw_payload_hex")
        try:
            payload = bytes.fromhex(payload_hex) if isinstance(payload_hex, str) else b""
            frame_key = InstrumentKeyV2.from_dict(body["instrument"])
        except (KeyError, TypeError, ValueError):
            continue
        sequence = body.get("last_update_id")
        identity = {"instrument": body.get("instrument"), "source_id": body.get("source_id"),
            "channel": body.get("channel"), "frame_type": body.get("frame_type"), "sequence": sequence,
            "event_at_ns": body.get("event_at_ns") if sequence is None else None}
        if (entry.content_hash != sha256_json(body) or entry.artifact_ref != sha256_json(identity)
                or hashlib.sha256(payload).hexdigest() != raw_ref or frame_key != key
                or body.get("available_at_ns", decision_at_ns + 1) > decision_at_ns
                or entry.available_at_ns > decision_at_ns or body.get("source_health") != "HEALTHY_CURRENT"
                or body.get("availability_class") != "ACTUAL_SYSTEM"
                or body.get("frame_type") not in {"SNAPSHOT", "DELTA", "DEPTH_SNAPSHOT", "DEPTH_DELTA"}):
            continue
        return True
    return False


def _liquidity_evidence(repo: OpsRepository, feature: FeatureArtifactV2, *, key: InstrumentKeyV2,
        decision_at_ns: int, quantity: Decimal, product: ProductContractV2, side: str) -> tuple[str, str]:
    """Derive compatibility from the exact sequence-valid book embedded in the action snapshot lineage."""
    s4_refs: list[tuple[str, Any, Mapping[str, Any]]] = []
    for ref in feature.envelope.input_refs:
        entry = repo.get_artifact(ref)
        if entry is not None and entry.artifact_type == "S4FeatureArtifactV2":
            body = entry.metadata.get("feature")
            if isinstance(body, Mapping):
                s4_refs.append((ref, entry, body))
    if len(s4_refs) != 1:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE")
    from atlas.v2.data.microstructure import (
        S4_FEATURE_POLICY_HASH,
        S4_FEATURE_VERSION,
        AvailabilityViewV2,
        BookStateV2,
        S4FeatureArtifactV2,
    )

    ref, entry, body = s4_refs[0]
    try:
        s4 = S4FeatureArtifactV2.from_dict(dict(body))
    except (TypeError, ValueError, KeyError) as exc:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE") from exc
    if (entry.available_at_ns > decision_at_ns or s4.content_hash != ref
            or s4.producer_version != S4_FEATURE_VERSION or body.get("producer_policy_hash") != S4_FEATURE_POLICY_HASH
            or s4.cutoff_ns != decision_at_ns or s4.instrument != key
            or s4.availability_view != AvailabilityViewV2.ACTUAL_RECEIPT
            or s4.sequence_state != BookStateV2.VALID or s4.source_health != "HEALTHY_CURRENT"
            or s4.missing_reason is not None or s4.spread is None or not s4.depth_bands):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE")
    raw_refs = s4.input_refs
    if not raw_refs:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_LIQUIDITY_SOURCE_LINEAGE_MISSING")
    raw_book_count = 0
    for raw_ref in raw_refs:
        raw_entry = repo.get_artifact(str(raw_ref))
        if raw_entry is None:
            if not _indexed_l2_payload(repo, str(raw_ref), key=key, decision_at_ns=decision_at_ns):
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_LIQUIDITY_SOURCE_UNAVAILABLE_AT_CUTOFF")
            raw_book_count += 1
            continue
        if raw_entry.available_at_ns > decision_at_ns:
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_LIQUIDITY_SOURCE_UNAVAILABLE_AT_CUTOFF")
        if raw_entry.artifact_type in {"L2SnapshotV2", "L2DeltaV2"}:
            raw_book_count += 1
            raw_body = raw_entry.metadata.get("snapshot", raw_entry.metadata.get("delta", raw_entry.metadata))
            if not isinstance(raw_body, Mapping):
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_LIQUIDITY_SOURCE_LINEAGE")
            raw_key = raw_body.get("instrument")
            raw_available = raw_body.get("available_at_ns")
            if (raw_entry.content_hash != raw_ref or sha256_json(raw_body) != raw_ref
                    or json_value(raw_key) != key.to_dict() or type(raw_available) is not int or raw_available > decision_at_ns
                    or raw_body.get("source_health") != "HEALTHY_CURRENT"
                    or raw_body.get("availability_class") != "ACTUAL_SYSTEM"):
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_LIQUIDITY_SOURCE_LINEAGE")
        elif raw_entry.content_hash != raw_ref:
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_LIQUIDITY_SOURCE_LINEAGE")
    if raw_book_count == 0:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_LIQUIDITY_SOURCE_LINEAGE_MISSING")
    # The caller cannot choose a liquidity bucket. Keep the exact sequence-valid measurements.
    try:
        depth = tuple(tuple(str(part) for part in row) for row in body["depth_bands"])
        spread = str(body["spread"])
        spread_value = Decimal(spread)
        opposing = 2 if side == "LONG" else 1
        ratios = tuple([row[0], str(quantity / Decimal(row[opposing])) if Decimal(row[opposing]) > 0 else "NO_DEPTH"]
            for row in depth)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError, IndexError) as exc:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_LIQUIDITY_MEASUREMENTS") from exc
    if not spread_value.is_finite() or spread_value < 0 or not depth:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_LIQUIDITY_MEASUREMENTS")
    liquidity_ref = sha256_json({"version": "ANALOGUE_LIQUIDITY_EVIDENCE_V1", "source_ref": ref,
        "cutoff_ns": decision_at_ns, "key": key.to_dict(), "spread": spread, "depth_bands": depth})
    participation_key = sha256_json({"version": "ANALOGUE_QUANTITY_PARTICIPATION_V2",
        "quantity_contracts": str(quantity), "qty_step": str(product.qty_step),
        "base_units_per_contract": str(product.base_units_per_contract), "side": body.get("opposing_liquidity_side"),
        "depth_participation_by_band": ratios, "liquidity_evidence_ref": liquidity_ref})
    return liquidity_ref, participation_key


def _cost_semantics(repo: OpsRepository, product: ProductContractV2, key: InstrumentKeyV2,
        decision_at_ns: int, policy_hash: str, candidate_cost_ref: str) -> str:
    existing_cost = repo.get_artifact(candidate_cost_ref)
    if existing_cost is None or existing_cost.artifact_type != "ActionCostContractV2":
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_COST_EVIDENCE")
    cost_entry, contract = _indexed_body(repo, candidate_cost_ref, "ActionCostContractV2", "cost_contract")
    execution_ref = contract.get("execution_assumptions_ref")
    execution = repo.get_artifact(str(execution_ref))
    execution_body = execution.metadata if execution is not None else None
    fee_ref, funding_ref = product.fee_schedule_ref, product.funding_schedule_ref
    if fee_ref is None:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_FEE_EVIDENCE")
    if funding_ref is None:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_FUNDING_EVIDENCE")
    fee_entry, fee = _indexed_body(repo, fee_ref, "FeeScheduleV2", None)
    funding_entry, funding = _indexed_body(repo, funding_ref, "FundingScheduleV2", None)
    fee_source = repo.get_artifact(str(fee.get("source_ref", "")))
    funding_source = repo.get_artifact(str(funding.get("source_ref", "")))
    contract_source = repo.get_artifact(str(contract.get("source_ref", "")))
    if not isinstance(execution_body, Mapping):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_CUTOFF_KNOWN_EXECUTION_EVIDENCE")
    try:
        action_cost_contract = ActionCostContractV2(InstrumentKeyV2.from_dict(contract["key"]),
            str(contract["policy_hash"]), contract["available_at_ns"], str(contract["fee_schedule_ref"]),
            str(contract["funding_schedule_ref"]), str(contract["execution_assumptions_ref"]),
            str(contract["source_ref"]))
        fee_key = InstrumentKeyV2.from_dict(fee["key"])
        fee_schedule = FeeScheduleV2(fee_key, fee["available_at_ns"],
            Decimal(str(fee["entry_taker_rate"])), Decimal(str(fee["exit_taker_rate"])), str(fee["source_ref"]))
        funding_schedule = FundingScheduleV2(funding["available_at_ns"],
            tuple(funding["expected_settlement_times_ns"]), funding["explicit_zero_funding"],
            str(funding["source_ref"]))
        replay_assumptions = ReplayAssumptionsV2(execution_body["decision_to_arrival_ns"],
            execution_body["human_delay_ns"], execution_body["stop_latency_ns"],
            execution_body["exit_latency_ns"], Decimal(str(execution_body["participation"])),
            Decimal(str(execution_body["exit_impact"])))
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_CUTOFF_KNOWN_COST_EVIDENCE") from exc
    checks = {
        "cost_contract_identity": cost_entry.content_hash == candidate_cost_ref and sha256_json(contract) == candidate_cost_ref,
        "cost_contract_typed": json_value(action_cost_contract.to_dict()) == json_value(contract),
        "cost_contract_cutoff": type(contract.get("available_at_ns")) is int
            and cost_entry.available_at_ns <= decision_at_ns
            and contract["available_at_ns"] <= decision_at_ns,
        "cost_contract_source": contract_source is not None
            and contract_source.content_hash == contract.get("source_ref")
            and contract_source.available_at_ns <= contract.get("available_at_ns", -1),
        "cost_contract_version": contract.get("version") == "V2_ACTION_COST_CONTRACT_V1",
        "cost_contract_key": json_value(contract.get("key")) == key.to_dict(),
        "cost_contract_policy": contract.get("policy_hash") == policy_hash,
        "cost_contract_fee": contract.get("fee_schedule_ref") == fee_ref,
        "cost_contract_funding": contract.get("funding_schedule_ref") == funding_ref,
        "execution_contract": execution is not None and execution.artifact_type == "ReplayAssumptionsV2"
            and execution.content_hash == execution_ref and isinstance(execution_body, Mapping)
            and sha256_json(execution_body) == execution_ref and execution.available_at_ns <= decision_at_ns
            and execution_body.get("version") == "V2_REPLAY_ASSUMPTIONS_V1"
            and json_value(replay_assumptions.to_dict()) == json_value(execution_body),
        "fee_schedule": fee_entry.content_hash == fee_ref and sha256_json(fee) == fee_ref
            and fee.get("version") == "V2_TAKER_FEES_V1" and fee_key == key
            and json_value(fee_schedule.to_dict()) == json_value(fee)
            and fee.get("available_at_ns", decision_at_ns + 1) <= decision_at_ns,
        "fee_source": fee_source is not None and fee_source.content_hash == fee.get("source_ref")
            and fee_source.available_at_ns <= fee_entry.available_at_ns,
        "funding_schedule": funding_entry.content_hash == funding_ref and sha256_json(funding) == funding_ref
            and funding.get("version") == "V2_FUNDING_SCHEDULE_V1"
            and type(funding.get("explicit_zero_funding")) is bool
            and json_value(funding_schedule.to_dict()) == json_value(funding)
            and funding.get("available_at_ns", decision_at_ns + 1) <= decision_at_ns,
        "funding_source": funding_source is not None and funding_source.content_hash == funding.get("source_ref")
            and funding_source.available_at_ns <= funding_entry.available_at_ns,
    }
    failed = tuple(name for name, valid in checks.items() if not valid)
    if failed:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_CUTOFF_KNOWN_COST_EVIDENCE:" + ",".join(failed))
    return sha256_json({"version": "ANALOGUE_COST_SEMANTICS_V2", "cost_contract_ref": candidate_cost_ref,
        "fee_ref": fee_ref, "funding_schedule_ref": funding_ref, "execution_assumptions_ref": execution_ref})


def build_analogue_compatibility(repo: OpsRepository, *, action_ref: str,
        consumer_at_ns: int | None = None) -> AnalogueCompatibilityV2:
    """Derive the only repository-backed compatibility key from the frozen action and cutoff evidence."""
    action_entry, action_body = _indexed_body(repo, action_ref, "ActionArtifactV2", "action_artifact")
    identity = action_entry.metadata.get("action_identity")
    if (not isinstance(identity, Mapping) or action_entry.content_hash != action_ref
            or sha256_json(action_body) != action_ref or sha256_json(identity) != action_body.get("action_hash")
            or identity.get("version") is None or action_body.get("version") != "V2_SHADOW_ACTION_ARTIFACT_V1"):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_FROZEN_ACTION_ARTIFACT")
    candidate_ref = action_body.get("candidate_ref")
    candidate_entry, candidate_body = _indexed_body(repo, str(candidate_ref), "CandidateActionV2", "candidate")
    from atlas.v2.contracts import CandidateActionV2, CandidateSetV2

    candidate = CandidateActionV2.from_dict(json_value(candidate_body))
    if candidate_entry.content_hash != candidate_ref or candidate.content_hash != candidate_ref:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_FROZEN_CANDIDATE")
    decision_at_ns = candidate.decision_at_ns
    consumer_at_ns = action_entry.available_at_ns if consumer_at_ns is None else consumer_at_ns
    if consumer_at_ns < action_entry.available_at_ns or consumer_at_ns > candidate.deadline_ns:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_FROZEN_ACTION_OUTSIDE_CONSUMER_WINDOW")
    from atlas.v2.chronology import causal_artifact

    # Candidates, selection, sizing and the frozen action are computations over
    # the decision-time market prefix. Their publication may therefore follow
    # T0, but only a sealed, recursively causal receipt can establish that.
    for derived_ref in (candidate_ref, str(action_body.get("candidate_set_ref")),
            str(action_body.get("sizing_ref")), action_ref, candidate.snapshot_hash):
        if not causal_artifact(repo, derived_ref, cutoff_ns=decision_at_ns,
                consumer_at_ns=consumer_at_ns, deadline_ns=candidate.deadline_ns):
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_INVALID_FROZEN_ACTION_DERIVED_CHRONOLOGY")
    candidate_set_ref = action_body.get("candidate_set_ref")
    set_entry, set_body = _indexed_body(repo, str(candidate_set_ref), "CandidateSetV2", "candidate_set")
    candidate_set = CandidateSetV2.from_dict(json_value(set_body))
    if (set_entry.content_hash != candidate_set_ref or candidate_set.content_hash != candidate_set_ref
            or candidate_set.selected_candidate_id != candidate.candidate_id
            or candidate_set.envelope.available_at_ns > consumer_at_ns
            or candidate.content_hash not in candidate_set.envelope.input_refs
            or action_entry.available_at_ns > consumer_at_ns
            or candidate.envelope.available_at_ns > consumer_at_ns
            or action_body.get("action_hash") != sha256_json(identity)
            or identity.get("policy_hash") != candidate.policy_hash
            or identity.get("horizon_end_ns") != candidate.horizon_end_ns
            or identity.get("side") != candidate.side.value):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_FROZEN_ACTION_CUTOFF_OR_BINDING_MISMATCH")
    key = InstrumentKeyV2.from_dict(identity["key"])
    product_ref = action_body.get("product_ref")
    product_entry, product_body = _indexed_body(repo, str(product_ref), "ProductContractV2", "product")
    product = ProductContractV2.from_dict(json_value(product_body))
    if (product_entry.content_hash != product_ref or product.content_hash != product_ref or product.key != key
            or product.available_at_ns > decision_at_ns or product.effective_at_ns > decision_at_ns
            or identity.get("product_ref") != product_ref):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_FROZEN_PRODUCT_UNAVAILABLE_OR_MISMATCHED")
    sizing_ref = action_body.get("sizing_ref")
    sizing_entry, sizing_body = _indexed_body(repo, str(sizing_ref), "SizingDecisionV2", "sizing")
    action_inputs = action_entry.metadata.get("input_refs")
    if (sizing_entry.content_hash != sizing_ref or sizing_body.get("status") != "SIZED"
            or sizing_body.get("candidate_ref") != candidate_ref
            or sizing_body.get("candidate_set_ref") != candidate_set_ref
            or sizing_body.get("product_ref") != product_ref
            or sizing_body.get("quantity") != str(identity.get("quantity"))
            or sizing_body.get("risk_policy_hash") != identity.get("risk_policy_hash")
            or sizing_body.get("risk_policy_v2_hash") != identity.get("risk_policy_v2_hash")
            or sizing_entry.available_at_ns > consumer_at_ns
            or not isinstance(action_inputs, (list, tuple))
            or not {candidate_ref, candidate_set_ref, sizing_ref, product_ref}.issubset(set(action_inputs))):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_FROZEN_ACTION_SIZING_MISMATCH")
    feature_entry, feature_body = _indexed_body(repo, candidate.snapshot_hash, "FeatureArtifactV2", "feature")
    feature = FeatureArtifactV2.from_dict(json_value(feature_body))
    if (feature_entry.content_hash != candidate.snapshot_hash or feature.content_hash != candidate.snapshot_hash
            or feature_entry.available_at_ns > consumer_at_ns or feature.envelope.available_at_ns > consumer_at_ns
            or feature.information_cutoff_ns > decision_at_ns or feature.key != key
            or feature.replay_view.value not in {"ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"}):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_CAUSAL_FEATURE_ARTIFACT_MISMATCH")
    liquidity_ref, participation_key = _liquidity_evidence(repo, feature, key=key,
        decision_at_ns=decision_at_ns, quantity=Decimal(str(identity["quantity"])), product=product,
        side=str(identity["side"]))
    costs_hash = _cost_semantics(repo, product, key, decision_at_ns, candidate.policy_hash, candidate.cost_model_ref)
    schema_hash, availability = _feature_schema(feature)
    action_semantics = action_semantics_hash(identity)
    execution_contract = sha256_json({"version": "ANALOGUE_EXECUTION_CONTRACT_V2",
        "frozen_action_semantics_hash": action_semantics,
        "execution_assumptions_ref": _indexed_body(repo, candidate.cost_model_ref,
            "ActionCostContractV2", "cost_contract")[1]["execution_assumptions_ref"],
        "cost_contract_ref": candidate.cost_model_ref})
    return AnalogueCompatibilityV2(str(identity["policy_hash"]), str(identity["side"]),
        action_semantics, int(identity["horizon_end_ns"]) - decision_at_ns,
        key.venue.value, key.product.value, sha256_json(key.to_dict()), execution_contract,
        participation_key, liquidity_ref, schema_hash, availability, costs_hash)


def _causal_values(repo: OpsRepository, action_ref: str, feature_names: tuple[str, ...], cutoff_ns: int,
        consumer_at_ns: int | None = None
        ) -> tuple[tuple[float | None, ...], str]:
    vector = action_features(repo, action_ref, cutoff_ns=cutoff_ns, consumer_at_ns=consumer_at_ns)
    indexed = dict(zip(vector.feature_order, vector.values, strict=True))
    if any(name not in indexed or name.startswith("missing:") for name in feature_names):
        raise ValueError("analogue features must be a declared subset of causal M0 action evidence")
    values = tuple(None if name.startswith("value:") and indexed.get("missing:" + name[6:]) else indexed[name]
        for name in feature_names)
    return values, vector.content_hash


def _validate_action_compatibility(repo: OpsRepository, action_ref: str, *, decision_at_ns: int,
        compatibility: AnalogueCompatibilityV2, feature_names: tuple[str, ...], values: tuple[float | None, ...],
        consumer_at_ns: int | None = None) -> None:
    entry = repo.get_artifact(action_ref)
    identity = entry.metadata.get("action_identity") if entry else None
    if not isinstance(identity, Mapping):
        raise ValueError("analogue exact action identity is unavailable")
    derived = build_analogue_compatibility(repo, action_ref=action_ref, consumer_at_ns=consumer_at_ns)
    if (identity["horizon_end_ns"] <= decision_at_ns or derived != compatibility
        or not feature_names or tuple(sorted(set(feature_names))) != feature_names
        or len(values) != len(feature_names)):
        raise ValueError("analogue compatibility contradicts its frozen action or causal feature schema")


def build_analogue_query(repo: OpsRepository, *, action_ref: str, candidate_ref: str, candidate_set_ref: str,
        cutoff_ns: int, compatibility: AnalogueCompatibilityV2, feature_names: tuple[str, ...],
        regime_id: str, consumer_at_ns: int | None = None) -> AnalogueQueryV2:
    entry = repo.get_artifact(action_ref)
    body = entry.metadata.get("action_artifact") if entry else None
    identity = entry.metadata.get("action_identity") if entry else None
    if not isinstance(body, Mapping) or not isinstance(identity, Mapping) or body.get("candidate_ref") != candidate_ref or body.get("candidate_set_ref") != candidate_set_ref:
        raise ValueError("analogue query must bind the same selected frozen action")
    candidate_entry = repo.get_artifact(candidate_ref)
    candidate_raw = candidate_entry.metadata.get("candidate") if candidate_entry is not None else None
    if not isinstance(candidate_raw, Mapping) or CandidateActionV2.from_dict(json_value(candidate_raw)).decision_at_ns != cutoff_ns:
        raise ValueError("analogue query cutoff must equal the exact frozen action decision time")
    entry = repo.get_artifact(action_ref)
    if entry is None:
        raise ValueError("analogue query action is not indexed")
    consumer_at_ns = entry.available_at_ns if consumer_at_ns is None else consumer_at_ns
    values, _ = _causal_values(repo, action_ref, feature_names, cutoff_ns, consumer_at_ns)
    _validate_action_compatibility(repo, action_ref, decision_at_ns=cutoff_ns,
        compatibility=compatibility, feature_names=feature_names, values=values,
        consumer_at_ns=consumer_at_ns)
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
    assert outcome.action_artifact_ref and outcome.action_hash and outcome.candidate_ref
    assert outcome.net_payoff is not None
    # Compatibility and state features are always reconstructed at the action's original cutoff.
    action_entry = repo.get_artifact(outcome.action_artifact_ref)
    if action_entry is None:
        raise ValueError("matured outcome source action is not indexed")
    values, feature_ref = _causal_values(repo, outcome.action_artifact_ref, feature_names,
        outcome.decision_at_ns, action_entry.available_at_ns)
    _validate_action_compatibility(repo, outcome.action_artifact_ref, decision_at_ns=outcome.decision_at_ns,
        compatibility=compatibility, feature_names=feature_names, values=values,
        consumer_at_ns=action_entry.available_at_ns)
    if outcome.provenance.value != "ACTUAL":
        payoff_entry, payoff = _indexed_body(repo, str(outcome.execution_evidence_ref), "PolicyPayoffV2", "payoff")
        action_entry, action_body = _indexed_body(repo, outcome.action_artifact_ref, "ActionArtifactV2", "action_artifact")
        product_entry, product_body = _indexed_body(repo, str(action_body.get("product_ref")), "ProductContractV2", "product")
        product = ProductContractV2.from_dict(json_value(product_body))
        if (payoff_entry.available_at_ns > outcome.available_at_ns or product_entry.available_at_ns > outcome.decision_at_ns
                or payoff.get("fee_ref") != product.fee_schedule_ref
                or payoff.get("funding_schedule_ref") != product.funding_schedule_ref):
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_OUTCOME_COSTS_DO_NOT_MATCH_FROZEN_ACTION_CONTRACT")
    witness = sha256_json({"version": "ANALOGUE_HONEST_TRAINING_ELIGIBILITY_V1", "outcome_ref": outcome_ref,
        "execution_evidence_ref": outcome.execution_evidence_ref, "feature_ref": feature_ref,
        "compatibility_key": compatibility.compatibility_key, "outcome_provenance": outcome.provenance.value})
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
        if compatibility.compatibility_key != observation.compatibility_key:
            raise ValueError("analogue source compatibility key mismatch")
        source = observation_from_matured_outcome(repo, outcome_ref=observation.outcome_ref,
            cutoff_ns=query.information_cutoff_ns, compatibility=compatibility,
            feature_names=observation.feature_names, episode_id=observation.episode_id, regime_id=observation.regime_id)
        if source != observation:
            raise ValueError("analogue source was revised or does not bind the honest label/features")
        eligible.append(source)
    return estimate_analogue(query, eligible)


def not_estimable_analogue(*, action_ref: str, candidate_ref: str, candidate_set_ref: str,
        action_hash: str, information_cutoff_ns: int, reason: str) -> AnalogueActionValueV2:
    """Persist an explicit result when required repository evidence has no compatibility key."""
    if not reason.startswith("NOT_ESTIMABLE_"):
        raise ValueError("analogue not-estimable result requires a named evidence reason")
    return AnalogueActionValueV2(action_hash, action_ref, candidate_ref, candidate_set_ref,
        information_cutoff_ns, None, 0, (), (), None, None, None, 0, None, (), (), (), None,
        "NOT_ESTIMABLE", "NOT_ESTIMABLE", (reason,))


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
