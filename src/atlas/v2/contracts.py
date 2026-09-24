"""Immutable, strict and content-addressed ATLAS V2 boundary contracts.

These types describe research and operations evidence only. None dispatches
orders or grants capital authority.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from atlas.domain.money import ensure_decimal, ensure_positive_decimal

from ._serialization import (
    FrozenMap,
    artifact_wire,
    canonical_json,
    decimal_value,
    nonblank,
    seal_envelope,
    sha256_json,
    sha256_ref,
    strict_fields,
    string_tuple,
    timestamp,
)

if TYPE_CHECKING:
    from .instruments import InstrumentKeyV2

ARTIFACT_SCHEMA_VERSION = 1
TRADE_PLAN_VERSION_V2 = "2.0"


class V2Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class ReplayViewV2(StrEnum):
    ACTUAL_SYSTEM = "ACTUAL_SYSTEM"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"


class EligibilityStatusV2(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


class DecisionStatusV2(StrEnum):
    CANDIDATE = "CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


class CandidateSelectionStatus(StrEnum):
    SELECTED = "SELECTED"
    NO_CANDIDATE = "NO_CANDIDATE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


class ForecastStatusV2(StrEnum):
    AVAILABLE = "AVAILABLE"
    PARTIAL = "PARTIAL"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    FAILED = "FAILED"


class WatchStateV2(StrEnum):
    DETECTED = "DETECTED"
    WAITING_FOR_EVENT = "WAITING_FOR_EVENT"
    READY_FOR_RECHECK = "READY_FOR_RECHECK"
    CONFIRMED = "CONFIRMED"
    HANDED_OFF = "HANDED_OFF"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


def _enum(enum_type: type[StrEnum], value: Any, *, field_name: str) -> Any:
    try:
        return enum_type(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


def _artifact_methods(envelope: ArtifactEnvelope, body: Mapping[str, Any], *, artifact_type: str) -> tuple[str, str]:
    wire = artifact_wire(envelope, body, artifact_type=artifact_type, include_hash=True)
    return canonical_json(wire), envelope.content_hash


@dataclass(frozen=True)
class ArtifactEnvelope:
    schema_version: int
    artifact_id: str
    created_at_ns: int
    available_at_ns: int
    producer_version: str
    input_refs: tuple[str, ...]
    content_hash: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != ARTIFACT_SCHEMA_VERSION:
            raise ValueError(f"unsupported artifact schema_version {self.schema_version!r}")
        nonblank(self.artifact_id, field="artifact_id")
        nonblank(self.producer_version, field="producer_version")
        timestamp(self.created_at_ns, field="created_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("available_at_ns must be >= created_at_ns")
        refs = string_tuple(self.input_refs, field="input_refs", sorted_unique=True)
        object.__setattr__(self, "input_refs", refs)
        if self.content_hash and len(self.content_hash) != 64:
            raise ValueError("content_hash must be a lowercase SHA-256 digest")
        if self.content_hash:
            sha256_ref(self.content_hash, field="content_hash")

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "created_at_ns": self.created_at_ns,
            "available_at_ns": self.available_at_ns,
            "producer_version": self.producer_version,
            "input_refs": list(self.input_refs),
        }
        if include_hash:
            if not self.content_hash:
                raise ValueError("unsealed envelope cannot be serialized")
            result["content_hash"] = self.content_hash
        return result

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ArtifactEnvelope:
        fields = {"schema_version", "artifact_id", "created_at_ns", "available_at_ns", "producer_version", "input_refs", "content_hash"}
        d = strict_fields(data, expected=fields, required=fields, name="ArtifactEnvelope")
        if not isinstance(d["input_refs"], list):
            raise ValueError("input_refs must be an array")
        return cls(
            d["schema_version"],
            d["artifact_id"],
            d["created_at_ns"],
            d["available_at_ns"],
            d["producer_version"],
            tuple(d["input_refs"]),
            d["content_hash"],
        )


@dataclass(frozen=True)
class FeatureValueV2:
    value: Decimal | int | float | None
    unit: str
    missing_reason: str | None = None

    def __post_init__(self) -> None:
        nonblank(self.unit, field="unit")
        if self.value is None:
            if self.missing_reason is None:
                raise ValueError("missing feature value requires missing_reason")
            nonblank(self.missing_reason, field="missing_reason")
        else:
            if self.missing_reason is not None:
                raise ValueError("present feature value cannot have missing_reason")
            if isinstance(self.value, bool) or not isinstance(self.value, (Decimal, int, float)):
                raise ValueError("feature values must be finite numeric values or explicit missingness")
            if isinstance(self.value, Decimal):
                ensure_decimal(self.value, field="feature value")
            elif isinstance(self.value, float) and not math.isfinite(self.value):
                raise ValueError("feature value must be finite")

    def to_dict(self) -> dict[str, Any]:
        value = self.value
        if isinstance(value, Decimal):
            value = decimal_value(value, field="feature value")
        return {"value": value, "unit": self.unit, "missing_reason": self.missing_reason}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureValueV2:
        fields = {"value", "unit", "missing_reason"}
        d = strict_fields(data, expected=fields, required=fields, name="FeatureValueV2")
        value = d["value"]
        if isinstance(value, str):
            value = decimal_value(value, field="feature value", wire=True)
        return cls(value, d["unit"], d["missing_reason"])


@dataclass(frozen=True)
class FeatureArtifactV2:
    envelope: ArtifactEnvelope
    key: InstrumentKeyV2
    feature_set_version: str
    information_cutoff_ns: int
    confirmed_at_ns: int
    values: FrozenMap
    source_health_ref: str
    replay_view: ReplayViewV2
    state_ref: str | None = None

    ARTIFACT_TYPE: ClassVar[str] = "FeatureArtifactV2"

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        nonblank(self.feature_set_version, field="feature_set_version")
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        timestamp(self.confirmed_at_ns, field="confirmed_at_ns")
        if self.information_cutoff_ns > self.confirmed_at_ns or self.confirmed_at_ns > self.envelope.created_at_ns:
            raise ValueError("feature causal timestamps must satisfy cutoff <= confirmed <= created")
        nonblank(self.source_health_ref, field="source_health_ref")
        if self.state_ref is not None:
            nonblank(self.state_ref, field="state_ref")
        try:
            replay = _enum(ReplayViewV2, self.replay_view, field_name="replay_view")
        except ValueError:
            raise
        object.__setattr__(self, "replay_view", replay)
        values = self.values if isinstance(self.values, FrozenMap) else FrozenMap(self.values)
        if any(not isinstance(name, str) or not name for name in values):
            raise ValueError("feature_id values require non-empty string keys")
        if any(not isinstance(value, FeatureValueV2) for value in values.values()):
            raise ValueError("values must map feature IDs to FeatureValueV2")
        object.__setattr__(self, "values", values)
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "key": self.key.to_dict(),
            "feature_set_version": self.feature_set_version,
            "information_cutoff_ns": self.information_cutoff_ns,
            "confirmed_at_ns": self.confirmed_at_ns,
            "values": {name: value.to_dict() for name, value in self.values.items()},
            "state_ref": self.state_ref,
            "source_health_ref": self.source_health_ref,
            "replay_view": self.replay_view.value,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureArtifactV2:
        expected = {"envelope", "key", "feature_set_version", "information_cutoff_ns", "confirmed_at_ns", "values", "state_ref", "source_health_ref", "replay_view"}
        required = expected - {"state_ref"}
        d = strict_fields(data, expected=expected, required=required, name=cls.ARTIFACT_TYPE)
        if not isinstance(d["values"], Mapping):
            raise ValueError("values must be an object")
        from .instruments import InstrumentKeyV2

        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]),
            InstrumentKeyV2.from_dict(d["key"]),
            d["feature_set_version"],
            d["information_cutoff_ns"],
            d["confirmed_at_ns"],
            FrozenMap({k: FeatureValueV2.from_dict(v) for k, v in d["values"].items()}),
            d["source_health_ref"],
            _enum(ReplayViewV2, d["replay_view"], field_name="replay_view"),
            d.get("state_ref"),
        )


@dataclass(frozen=True)
class PolicySpecV2:
    policy_id: str
    version: str
    policy_hash: str
    strategy_family: str
    capital_status: str
    decision_event: str
    required_features: tuple[str, ...]
    optional_features: tuple[str, ...]
    timeframe_rules: FrozenMap
    setup_parameters: FrozenMap
    direction_rule: FrozenMap
    entry_rule: FrozenMap
    collar_rule: FrozenMap
    stop_rule: FrozenMap
    trigger_basis: str
    management_rule: FrozenMap
    time_exit_rule: FrozenMap
    max_hold_ns: int
    expiry_rule: FrozenMap
    model_requirements: tuple[str, ...]
    fallback_policy_id: str | None = None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        for field_name in ("policy_id", "version", "strategy_family", "capital_status", "decision_event", "trigger_basis"):
            nonblank(getattr(self, field_name), field=field_name)
        timestamp(self.max_hold_ns, field="max_hold_ns")
        if self.max_hold_ns <= 0:
            raise ValueError("max_hold_ns must be positive")
        for field_name in ("required_features", "optional_features", "model_requirements"):
            values = string_tuple(getattr(self, field_name), field=field_name, sorted_unique=True)
            object.__setattr__(self, field_name, values)
        if set(self.required_features) & set(self.optional_features):
            raise ValueError("required and optional features must be disjoint")
        for field_name in (
            "timeframe_rules", "setup_parameters", "direction_rule", "entry_rule", "collar_rule",
            "stop_rule", "management_rule", "time_exit_rule", "expiry_rule",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, FrozenMap):
                value = FrozenMap(value)
                object.__setattr__(self, field_name, value)
        if self.fallback_policy_id is not None:
            nonblank(self.fallback_policy_id, field="fallback_policy_id")
        supplied = self.policy_hash
        if supplied:
            sha256_ref(supplied, field="policy_hash")
        digest = sha256_json({"contract_type": "PolicySpecV2", "policy": self._body(include_hash=False)})
        if supplied and supplied != digest:
            raise ValueError("PolicySpecV2 policy_hash mismatch")
        object.__setattr__(self, "policy_hash", digest)

    def _body(self, *, include_hash: bool = True) -> dict[str, Any]:
        result = {
            "policy_id": self.policy_id,
            "version": self.version,
            "strategy_family": self.strategy_family,
            "capital_status": self.capital_status,
            "decision_event": self.decision_event,
            "required_features": list(self.required_features),
            "optional_features": list(self.optional_features),
            "timeframe_rules": self.timeframe_rules.to_dict(),
            "setup_parameters": self.setup_parameters.to_dict(),
            "direction_rule": self.direction_rule.to_dict(),
            "entry_rule": self.entry_rule.to_dict(),
            "collar_rule": self.collar_rule.to_dict(),
            "stop_rule": self.stop_rule.to_dict(),
            "trigger_basis": self.trigger_basis,
            "management_rule": self.management_rule.to_dict(),
            "time_exit_rule": self.time_exit_rule.to_dict(),
            "max_hold_ns": self.max_hold_ns,
            "expiry_rule": self.expiry_rule.to_dict(),
            "model_requirements": list(self.model_requirements),
            "fallback_policy_id": self.fallback_policy_id,
        }
        if include_hash:
            result["policy_hash"] = self.policy_hash
        return result

    @property
    def content_hash(self) -> str:
        return self.policy_hash

    def to_dict(self) -> dict[str, Any]:
        return self._body()

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def build(cls, **values: Any) -> PolicySpecV2:
        return cls(policy_hash="", **values)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PolicySpecV2:
        expected = {"policy_id", "version", "policy_hash", "strategy_family", "capital_status", "decision_event", "required_features", "optional_features", "timeframe_rules", "setup_parameters", "direction_rule", "entry_rule", "collar_rule", "stop_rule", "trigger_basis", "management_rule", "time_exit_rule", "max_hold_ns", "expiry_rule", "model_requirements", "fallback_policy_id"}
        required = expected - {"fallback_policy_id"}
        d = strict_fields(data, expected=expected, required=required, name=cls.__name__)
        for key in ("required_features", "optional_features", "model_requirements"):
            if not isinstance(d[key], list):
                raise ValueError(f"{key} must be an array")
        return cls(
            d["policy_id"], d["version"], d["policy_hash"], d["strategy_family"], d["capital_status"], d["decision_event"],
            tuple(d["required_features"]), tuple(d["optional_features"]), FrozenMap(d["timeframe_rules"]),
            FrozenMap(d["setup_parameters"]), FrozenMap(d["direction_rule"]), FrozenMap(d["entry_rule"]),
            FrozenMap(d["collar_rule"]), FrozenMap(d["stop_rule"]), d["trigger_basis"], FrozenMap(d["management_rule"]),
            FrozenMap(d["time_exit_rule"]), d["max_hold_ns"], FrozenMap(d["expiry_rule"]), tuple(d["model_requirements"]),
            d.get("fallback_policy_id"),
        )


@dataclass(frozen=True)
class CandidateActionV2:
    envelope: ArtifactEnvelope
    candidate_id: str
    key: InstrumentKeyV2
    policy_hash: str
    snapshot_hash: str
    side: V2Side
    decision_at_ns: int
    deadline_ns: int
    horizon_end_ns: int
    entry_reference: Decimal
    entry_collar: Decimal
    stop_price: Decimal
    state_version: int
    cost_model_ref: str
    quantity: Decimal | None = None
    account_scope: str | None = None

    ARTIFACT_TYPE: ClassVar[str] = "CandidateActionV2"

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        nonblank(self.candidate_id, field="candidate_id")
        sha256_ref(self.policy_hash, field="policy_hash")
        sha256_ref(self.snapshot_hash, field="snapshot_hash")
        object.__setattr__(self, "side", _enum(V2Side, self.side, field_name="side"))
        timestamp(self.decision_at_ns, field="decision_at_ns")
        timestamp(self.deadline_ns, field="deadline_ns")
        timestamp(self.horizon_end_ns, field="horizon_end_ns")
        if not (self.decision_at_ns <= self.envelope.created_at_ns <= self.envelope.available_at_ns <= self.deadline_ns < self.horizon_end_ns):
            raise ValueError("candidate causal/deadline timestamps are inconsistent")
        for field_name in ("entry_reference", "entry_collar", "stop_price"):
            object.__setattr__(self, field_name, ensure_positive_decimal(getattr(self, field_name), field=field_name))
        if self.quantity is not None:
            object.__setattr__(self, "quantity", ensure_positive_decimal(self.quantity, field="quantity"))
        if type(self.state_version) is not int or self.state_version < 0:
            raise ValueError("state_version must be a nonnegative integer")
        nonblank(self.cost_model_ref, field="cost_model_ref")
        if self.account_scope is not None:
            nonblank(self.account_scope, field="account_scope")
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "key": self.key.to_dict(),
            "account_scope": self.account_scope,
            "policy_hash": self.policy_hash,
            "snapshot_hash": self.snapshot_hash,
            "side": self.side.value,
            "decision_at_ns": self.decision_at_ns,
            "deadline_ns": self.deadline_ns,
            "horizon_end_ns": self.horizon_end_ns,
            "entry_reference": self.entry_reference,
            "entry_collar": self.entry_collar,
            "stop_price": self.stop_price,
            "quantity": self.quantity,
            "state_version": self.state_version,
            "cost_model_ref": self.cost_model_ref,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CandidateActionV2:
        expected = {"envelope", "candidate_id", "key", "account_scope", "policy_hash", "snapshot_hash", "side", "decision_at_ns", "deadline_ns", "horizon_end_ns", "entry_reference", "entry_collar", "stop_price", "quantity", "state_version", "cost_model_ref"}
        required = expected - {"account_scope", "quantity"}
        d = strict_fields(data, expected=expected, required=required, name=cls.ARTIFACT_TYPE)
        if d.get("quantity") is not None and not isinstance(d["quantity"], str):
            raise ValueError("quantity wire value must be a canonical decimal string")
        from .instruments import InstrumentKeyV2

        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]), d["candidate_id"], InstrumentKeyV2.from_dict(d["key"]),
            d["policy_hash"], d["snapshot_hash"], _enum(V2Side, d["side"], field_name="side"),
            d["decision_at_ns"], d["deadline_ns"], d["horizon_end_ns"],
            decimal_value(d["entry_reference"], field="entry_reference", wire=True),
            decimal_value(d["entry_collar"], field="entry_collar", wire=True),
            decimal_value(d["stop_price"], field="stop_price", wire=True), d["state_version"], d["cost_model_ref"],
            decimal_value(d["quantity"], field="quantity", wire=True) if d.get("quantity") is not None else None,
            d.get("account_scope"),
        )


@dataclass(frozen=True)
class StrategyForecastV2:
    envelope: ArtifactEnvelope
    forecast_id: str
    candidate_id: str
    key: InstrumentKeyV2
    strategy_id: str
    strategy_version: str
    policy_hash: str
    horizon_ns: int
    expires_at_ns: int
    target_definition: str
    status: ForecastStatusV2
    regime_support_ref: str
    distribution_ref: str | None = None
    mean: Decimal | float | None = None
    q05: Decimal | float | None = None
    q50: Decimal | float | None = None
    q95: Decimal | float | None = None
    p_net_positive: Decimal | float | None = None
    cost_ref: str | None = None
    mae_ref: str | None = None
    mfe_ref: str | None = None
    calibration_ref: str | None = None
    capacity_ref: str | None = None
    contradictions: tuple[str, ...] = ()

    ARTIFACT_TYPE: ClassVar[str] = "StrategyForecastV2"

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        for field_name in ("forecast_id", "candidate_id", "strategy_id", "strategy_version", "target_definition"):
            nonblank(getattr(self, field_name), field=field_name)
        sha256_ref(self.policy_hash, field="policy_hash")
        timestamp(self.horizon_ns, field="horizon_ns")
        timestamp(self.expires_at_ns, field="expires_at_ns")
        if self.horizon_ns <= 0 or self.expires_at_ns < self.envelope.available_at_ns:
            raise ValueError("forecast horizon/expiry is invalid")
        object.__setattr__(self, "status", _enum(ForecastStatusV2, self.status, field_name="status"))
        for field_name in ("mean", "q05", "q50", "q95", "p_net_positive"):
            value = getattr(self, field_name)
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
                    raise ValueError(f"{field_name} must be finite numeric")
                if isinstance(value, Decimal):
                    ensure_decimal(value, field=field_name)
                elif isinstance(value, float) and not math.isfinite(value):
                    raise ValueError(f"{field_name} must be finite")
        if self.p_net_positive is not None and not (0 <= self.p_net_positive <= 1):
            raise ValueError("p_net_positive must be in [0, 1]")
        for field_name in ("distribution_ref", "cost_ref", "mae_ref", "mfe_ref", "calibration_ref", "capacity_ref"):
            value = getattr(self, field_name)
            if value is not None:
                nonblank(value, field=field_name)
        nonblank(self.regime_support_ref, field="regime_support_ref")
        contradictions = string_tuple(self.contradictions, field="contradictions", sorted_unique=True)
        object.__setattr__(self, "contradictions", contradictions)
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "forecast_id": self.forecast_id,
            "candidate_id": self.candidate_id,
            "key": self.key.to_dict(),
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "policy_hash": self.policy_hash,
            "horizon_ns": self.horizon_ns,
            "expires_at_ns": self.expires_at_ns,
            "target_definition": self.target_definition,
            "distribution_ref": self.distribution_ref,
            "mean": self.mean,
            "q05": self.q05,
            "q50": self.q50,
            "q95": self.q95,
            "p_net_positive": self.p_net_positive,
            "cost_ref": self.cost_ref,
            "mae_ref": self.mae_ref,
            "mfe_ref": self.mfe_ref,
            "calibration_ref": self.calibration_ref,
            "capacity_ref": self.capacity_ref,
            "regime_support_ref": self.regime_support_ref,
            "contradictions": list(self.contradictions),
            "status": self.status.value,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StrategyForecastV2:
        expected = {"envelope", "forecast_id", "candidate_id", "key", "strategy_id", "strategy_version", "policy_hash", "horizon_ns", "expires_at_ns", "target_definition", "distribution_ref", "mean", "q05", "q50", "q95", "p_net_positive", "cost_ref", "mae_ref", "mfe_ref", "calibration_ref", "capacity_ref", "regime_support_ref", "contradictions", "status"}
        required = expected - {"distribution_ref", "mean", "q05", "q50", "q95", "p_net_positive", "cost_ref", "mae_ref", "mfe_ref", "calibration_ref", "capacity_ref"}
        d = strict_fields(data, expected=expected, required=required, name=cls.ARTIFACT_TYPE)
        if not isinstance(d["contradictions"], list):
            raise ValueError("contradictions must be an array")
        from .instruments import InstrumentKeyV2

        numbers: dict[str, Any] = {}
        for field_name in ("mean", "q05", "q50", "q95", "p_net_positive"):
            value = d.get(field_name)
            numbers[field_name] = decimal_value(value, field=field_name, wire=True) if isinstance(value, str) else value
        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]), d["forecast_id"], d["candidate_id"], InstrumentKeyV2.from_dict(d["key"]),
            d["strategy_id"], d["strategy_version"], d["policy_hash"], d["horizon_ns"], d["expires_at_ns"],
            d["target_definition"], _enum(ForecastStatusV2, d["status"], field_name="status"),
            d["regime_support_ref"], d.get("distribution_ref"), numbers["mean"], numbers["q05"], numbers["q50"], numbers["q95"], numbers["p_net_positive"],
            d.get("cost_ref"), d.get("mae_ref"), d.get("mfe_ref"), d.get("calibration_ref"), d.get("capacity_ref"),
            tuple(d["contradictions"]),
        )


@dataclass(frozen=True)
class CandidateSetEntryV2:
    candidate_id: str
    policy_id: str
    key: InstrumentKeyV2
    side: V2Side
    selection_feature_refs: tuple[str, ...]
    eligibility_status: EligibilityStatusV2
    rank: int | None = None
    rank_reason: str | None = None
    rejection_reason: str | None = None

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        nonblank(self.candidate_id, field="candidate_id")
        nonblank(self.policy_id, field="policy_id")
        object.__setattr__(self, "side", _enum(V2Side, self.side, field_name="side"))
        object.__setattr__(self, "eligibility_status", _enum(EligibilityStatusV2, self.eligibility_status, field_name="eligibility_status"))
        refs = string_tuple(self.selection_feature_refs, field="selection_feature_refs", sorted_unique=True)
        object.__setattr__(self, "selection_feature_refs", refs)
        if self.rank is not None and (type(self.rank) is not int or self.rank < 1):
            raise ValueError("rank must be a positive integer")
        for field_name in ("rank_reason", "rejection_reason"):
            value = getattr(self, field_name)
            if value is not None:
                nonblank(value, field=field_name)
        if self.eligibility_status == EligibilityStatusV2.INELIGIBLE and not self.rejection_reason:
            raise ValueError("ineligible candidate requires rejection_reason")

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "policy_id": self.policy_id,
            "key": self.key.to_dict(),
            "side": self.side.value,
            "selection_feature_refs": list(self.selection_feature_refs),
            "eligibility_status": self.eligibility_status.value,
            "rank": self.rank,
            "rank_reason": self.rank_reason,
            "rejection_reason": self.rejection_reason,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CandidateSetEntryV2:
        fields = {"candidate_id", "policy_id", "key", "side", "selection_feature_refs", "eligibility_status", "rank", "rank_reason", "rejection_reason"}
        d = strict_fields(data, expected=fields, required=fields - {"rank", "rank_reason", "rejection_reason"}, name=cls.__name__)
        if not isinstance(d["selection_feature_refs"], list):
            raise ValueError("selection_feature_refs must be an array")
        from .instruments import InstrumentKeyV2

        return cls(
            d["candidate_id"], d["policy_id"], InstrumentKeyV2.from_dict(d["key"]),
            _enum(V2Side, d["side"], field_name="side"), tuple(d["selection_feature_refs"]),
            _enum(EligibilityStatusV2, d["eligibility_status"], field_name="eligibility_status"),
            d.get("rank"), d.get("rank_reason"), d.get("rejection_reason"),
        )


@dataclass(frozen=True)
class CandidateSetV2:
    envelope: ArtifactEnvelope
    decision_event_id: str
    universe_ref: str
    selection_policy_hash: str
    candidates: tuple[CandidateSetEntryV2, ...]
    selected_candidate_id: str | None
    tie_break_rule: str
    selection_status: CandidateSelectionStatus

    ARTIFACT_TYPE: ClassVar[str] = "CandidateSetV2"

    def __post_init__(self) -> None:
        nonblank(self.decision_event_id, field="decision_event_id")
        sha256_ref(self.universe_ref, field="universe_ref")
        sha256_ref(self.selection_policy_hash, field="selection_policy_hash")
        nonblank(self.tie_break_rule, field="tie_break_rule")
        object.__setattr__(self, "selection_status", _enum(CandidateSelectionStatus, self.selection_status, field_name="selection_status"))
        candidates = tuple(self.candidates)
        if any(not isinstance(candidate, CandidateSetEntryV2) for candidate in candidates):
            raise ValueError("candidates must contain CandidateSetEntryV2 values")
        ids = tuple(candidate.candidate_id for candidate in candidates)
        if ids != tuple(sorted(ids)) or len(set(ids)) != len(ids):
            raise ValueError("candidate IDs must be unique and sorted")
        if self.selection_status == CandidateSelectionStatus.SELECTED:
            if self.selected_candidate_id is None or self.selected_candidate_id not in ids:
                raise ValueError("selected_candidate_id must exist in the persisted candidate set")
        elif self.selected_candidate_id is not None:
            raise ValueError("NO_CANDIDATE/NOT_ESTIMABLE cannot contain a selected candidate")
        if self.selected_candidate_id is not None:
            nonblank(self.selected_candidate_id, field="selected_candidate_id")
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "decision_event_id": self.decision_event_id,
            "universe_ref": self.universe_ref,
            "selection_policy_hash": self.selection_policy_hash,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "selected_candidate_id": self.selected_candidate_id,
            "tie_break_rule": self.tie_break_rule,
            "selection_status": self.selection_status.value,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CandidateSetV2:
        fields = {"envelope", "decision_event_id", "universe_ref", "selection_policy_hash", "candidates", "selected_candidate_id", "tie_break_rule", "selection_status"}
        d = strict_fields(data, expected=fields, required=fields, name=cls.ARTIFACT_TYPE)
        if not isinstance(d["candidates"], list):
            raise ValueError("candidates must be an array")
        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]), d["decision_event_id"], d["universe_ref"], d["selection_policy_hash"],
            tuple(CandidateSetEntryV2.from_dict(item) for item in d["candidates"]), d["selected_candidate_id"],
            d["tie_break_rule"], _enum(CandidateSelectionStatus, d["selection_status"], field_name="selection_status"),
        )


@dataclass(frozen=True)
class EvaluationArtifactV2:
    envelope: ArtifactEnvelope
    action_hash: str
    quantity: Decimal
    risk_policy_hash: str
    account_snapshot_ref: str
    universe_ref: str
    candidate_set_ref: str
    selection_policy_hash: str
    meta_version: str
    scenario_manifest_ref: str
    common_path_ids_ref: str
    existing_portfolio_ref: str
    stress_ref: str
    outcome_distribution_ref: str
    decision: DecisionStatusV2
    reasons: tuple[str, ...]
    expires_at_ns: int
    expected_pnl_lcb: Decimal | None = None
    lcb_method_ref: str | None = None
    es_before: Decimal | None = None
    es_after: Decimal | None = None
    numerical_error_ref: str | None = None

    ARTIFACT_TYPE: ClassVar[str] = "EvaluationArtifactV2"

    def __post_init__(self) -> None:
        for field_name in (
            "action_hash", "risk_policy_hash", "selection_policy_hash", "account_snapshot_ref", "universe_ref",
            "candidate_set_ref", "scenario_manifest_ref", "common_path_ids_ref", "existing_portfolio_ref",
            "stress_ref", "outcome_distribution_ref",
        ):
            sha256_ref(getattr(self, field_name), field=field_name)
        nonblank(self.meta_version, field="meta_version")
        object.__setattr__(self, "quantity", ensure_positive_decimal(self.quantity, field="quantity"))
        object.__setattr__(self, "decision", _enum(DecisionStatusV2, self.decision, field_name="decision"))
        timestamp(self.expires_at_ns, field="expires_at_ns")
        if self.expires_at_ns < self.envelope.available_at_ns:
            raise ValueError("evaluation expiry must not precede availability")
        reasons = string_tuple(self.reasons, field="reasons")
        object.__setattr__(self, "reasons", reasons)
        if self.decision in (DecisionStatusV2.NO_TRADE, DecisionStatusV2.NOT_ESTIMABLE) and not reasons:
            raise ValueError(f"{self.decision.value} evaluation requires a reason")
        for field_name in ("expected_pnl_lcb", "es_before", "es_after"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, ensure_decimal(value, field=field_name))
        for field_name in ("lcb_method_ref", "numerical_error_ref"):
            value = getattr(self, field_name)
            if value is not None:
                nonblank(value, field=field_name)
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "action_hash": self.action_hash,
            "quantity": self.quantity,
            "risk_policy_hash": self.risk_policy_hash,
            "account_snapshot_ref": self.account_snapshot_ref,
            "universe_ref": self.universe_ref,
            "candidate_set_ref": self.candidate_set_ref,
            "selection_policy_hash": self.selection_policy_hash,
            "meta_version": self.meta_version,
            "scenario_manifest_ref": self.scenario_manifest_ref,
            "common_path_ids_ref": self.common_path_ids_ref,
            "existing_portfolio_ref": self.existing_portfolio_ref,
            "stress_ref": self.stress_ref,
            "outcome_distribution_ref": self.outcome_distribution_ref,
            "expected_pnl_lcb": self.expected_pnl_lcb,
            "lcb_method_ref": self.lcb_method_ref,
            "es_before": self.es_before,
            "es_after": self.es_after,
            "numerical_error_ref": self.numerical_error_ref,
            "decision": self.decision.value,
            "reasons": list(self.reasons),
            "expires_at_ns": self.expires_at_ns,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EvaluationArtifactV2:
        fields = {"envelope", "action_hash", "quantity", "risk_policy_hash", "account_snapshot_ref", "universe_ref", "candidate_set_ref", "selection_policy_hash", "meta_version", "scenario_manifest_ref", "common_path_ids_ref", "existing_portfolio_ref", "stress_ref", "outcome_distribution_ref", "expected_pnl_lcb", "lcb_method_ref", "es_before", "es_after", "numerical_error_ref", "decision", "reasons", "expires_at_ns"}
        optional = {"expected_pnl_lcb", "lcb_method_ref", "es_before", "es_after", "numerical_error_ref"}
        d = strict_fields(data, expected=fields, required=fields - optional, name=cls.ARTIFACT_TYPE)
        if not isinstance(d["reasons"], list):
            raise ValueError("reasons must be an array")
        decimals: dict[str, Any] = {}
        for field_name in ("quantity", "expected_pnl_lcb", "es_before", "es_after"):
            value = d.get(field_name)
            decimals[field_name] = decimal_value(value, field=field_name, wire=True) if value is not None else None
        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]), d["action_hash"], decimals["quantity"], d["risk_policy_hash"],
            d["account_snapshot_ref"], d["universe_ref"], d["candidate_set_ref"], d["selection_policy_hash"], d["meta_version"],
            d["scenario_manifest_ref"], d["common_path_ids_ref"], d["existing_portfolio_ref"], d["stress_ref"], d["outcome_distribution_ref"],
            _enum(DecisionStatusV2, d["decision"], field_name="decision"), tuple(d["reasons"]), d["expires_at_ns"],
            decimals["expected_pnl_lcb"], d.get("lcb_method_ref"), decimals["es_before"], decimals["es_after"], d.get("numerical_error_ref"),
        )


@dataclass(frozen=True)
class TradePlanEnvelopeV2:
    envelope: ArtifactEnvelope
    plan_id: str
    plan_version: str
    execution_contract_version: str
    key: InstrumentKeyV2
    product_ref: str
    account_scope: str
    policy_hash: str
    action_hash: str
    evaluation_ref: str
    risk_policy_hash: str
    capability_manifest_hash: str
    reservation_snapshot_version: int
    side: V2Side
    qty_limit: Decimal
    entry_policy: str
    collar: Decimal
    stop: Decimal
    stop_trigger_basis: str
    management_policy: str
    horizon_end_ns: int
    normal_risk: Decimal
    stress_risk: Decimal
    margin: Decimal
    leverage_bound: Decimal
    reference_price: Decimal
    expires_at_ns: int

    ARTIFACT_TYPE: ClassVar[str] = "TradePlanEnvelopeV2"
    SUPPORTED_PLAN_VERSIONS: ClassVar[frozenset[str]] = frozenset({TRADE_PLAN_VERSION_V2})

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        for field_name in ("plan_id", "execution_contract_version", "account_scope", "entry_policy", "stop_trigger_basis", "management_policy"):
            nonblank(getattr(self, field_name), field=field_name)
        if self.plan_version not in self.SUPPORTED_PLAN_VERSIONS:
            raise ValueError(f"unsupported TradePlanEnvelopeV2 plan_version {self.plan_version!r}")
        if self.execution_contract_version != TRADE_PLAN_VERSION_V2:
            raise ValueError("unsupported execution_contract_version")
        for field_name in ("product_ref", "policy_hash", "action_hash", "evaluation_ref", "risk_policy_hash", "capability_manifest_hash"):
            sha256_ref(getattr(self, field_name), field=field_name)
        if type(self.reservation_snapshot_version) is not int or self.reservation_snapshot_version < 0:
            raise ValueError("reservation_snapshot_version must be nonnegative")
        object.__setattr__(self, "side", _enum(V2Side, self.side, field_name="side"))
        for field_name in ("qty_limit", "collar", "stop", "leverage_bound", "reference_price"):
            object.__setattr__(self, field_name, ensure_positive_decimal(getattr(self, field_name), field=field_name))
        for field_name in ("normal_risk", "stress_risk", "margin"):
            value = ensure_decimal(getattr(self, field_name), field=field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be nonnegative")
            object.__setattr__(self, field_name, value)
        timestamp(self.horizon_end_ns, field="horizon_end_ns")
        timestamp(self.expires_at_ns, field="expires_at_ns")
        if not self.envelope.available_at_ns < self.expires_at_ns <= self.horizon_end_ns:
            raise ValueError("plan expiry must follow availability and not exceed horizon")
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "plan_version": self.plan_version,
            "execution_contract_version": self.execution_contract_version,
            "key": self.key.to_dict(),
            "product_ref": self.product_ref,
            "account_scope": self.account_scope,
            "policy_hash": self.policy_hash,
            "action_hash": self.action_hash,
            "evaluation_ref": self.evaluation_ref,
            "risk_policy_hash": self.risk_policy_hash,
            "capability_manifest_hash": self.capability_manifest_hash,
            "reservation_snapshot_version": self.reservation_snapshot_version,
            "side": self.side.value,
            "qty_limit": self.qty_limit,
            "entry_policy": self.entry_policy,
            "collar": self.collar,
            "stop": self.stop,
            "stop_trigger_basis": self.stop_trigger_basis,
            "management_policy": self.management_policy,
            "horizon_end_ns": self.horizon_end_ns,
            "normal_risk": self.normal_risk,
            "stress_risk": self.stress_risk,
            "margin": self.margin,
            "leverage_bound": self.leverage_bound,
            "reference_price": self.reference_price,
            "expires_at_ns": self.expires_at_ns,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TradePlanEnvelopeV2:
        fields = {"envelope", "plan_id", "plan_version", "execution_contract_version", "key", "product_ref", "account_scope", "policy_hash", "action_hash", "evaluation_ref", "risk_policy_hash", "capability_manifest_hash", "reservation_snapshot_version", "side", "qty_limit", "entry_policy", "collar", "stop", "stop_trigger_basis", "management_policy", "horizon_end_ns", "normal_risk", "stress_risk", "margin", "leverage_bound", "reference_price", "expires_at_ns"}
        d = strict_fields(data, expected=fields, required=fields, name=cls.ARTIFACT_TYPE)
        from .instruments import InstrumentKeyV2

        decimals = {key: decimal_value(d[key], field=key, wire=True) for key in ("qty_limit", "collar", "stop", "normal_risk", "stress_risk", "margin", "leverage_bound", "reference_price")}
        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]), d["plan_id"], d["plan_version"], d["execution_contract_version"],
            InstrumentKeyV2.from_dict(d["key"]), d["product_ref"], d["account_scope"], d["policy_hash"], d["action_hash"],
            d["evaluation_ref"], d["risk_policy_hash"], d["capability_manifest_hash"], d["reservation_snapshot_version"],
            _enum(V2Side, d["side"], field_name="side"), decimals["qty_limit"], d["entry_policy"], decimals["collar"],
            decimals["stop"], d["stop_trigger_basis"], d["management_policy"], d["horizon_end_ns"], decimals["normal_risk"],
            decimals["stress_risk"], decimals["margin"], decimals["leverage_bound"], decimals["reference_price"], d["expires_at_ns"],
        )


_WATCH_NEXT: dict[WatchStateV2, frozenset[WatchStateV2]] = {
    WatchStateV2.DETECTED: frozenset({WatchStateV2.WAITING_FOR_EVENT, WatchStateV2.INVALIDATED, WatchStateV2.EXPIRED}),
    WatchStateV2.WAITING_FOR_EVENT: frozenset({WatchStateV2.READY_FOR_RECHECK, WatchStateV2.INVALIDATED, WatchStateV2.EXPIRED}),
    WatchStateV2.READY_FOR_RECHECK: frozenset({WatchStateV2.CONFIRMED, WatchStateV2.INVALIDATED, WatchStateV2.EXPIRED}),
    WatchStateV2.CONFIRMED: frozenset({WatchStateV2.HANDED_OFF, WatchStateV2.INVALIDATED, WatchStateV2.EXPIRED}),
    WatchStateV2.HANDED_OFF: frozenset(),
    WatchStateV2.INVALIDATED: frozenset(),
    WatchStateV2.EXPIRED: frozenset(),
}


@dataclass(frozen=True)
class OpportunityWatchV2:
    watch_id: str
    key: InstrumentKeyV2
    strategy_id: str
    strategy_version: str
    policy_hash: str
    state: WatchStateV2
    state_version: int
    created_at_ns: int
    updated_at_ns: int
    thesis_hash: str
    evidence_refs: tuple[str, ...]
    required_next_event: str
    expires_at_ns: int
    last_evaluated_at_ns: int
    wake_at_ns: int | None = None
    invalidators: tuple[str, ...] = ()
    last_event_id: str | None = None
    parent_watch_id: str | None = None
    handoff_receipt: str | None = None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        from .instruments import InstrumentKeyV2

        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        for field_name in ("watch_id", "strategy_id", "strategy_version", "required_next_event"):
            nonblank(getattr(self, field_name), field=field_name)
        sha256_ref(self.policy_hash, field="policy_hash")
        sha256_ref(self.thesis_hash, field="thesis_hash")
        object.__setattr__(self, "state", _enum(WatchStateV2, self.state, field_name="state"))
        if type(self.state_version) is not int or self.state_version < 0:
            raise ValueError("state_version must be a nonnegative integer")
        for field_name in ("created_at_ns", "updated_at_ns", "expires_at_ns", "last_evaluated_at_ns"):
            timestamp(getattr(self, field_name), field=field_name)
        expiry_state = self.state == WatchStateV2.EXPIRED and self.updated_at_ns >= self.expires_at_ns
        temporal_state_ok = (
            self.created_at_ns <= self.last_evaluated_at_ns <= self.updated_at_ns
            and (self.updated_at_ns < self.expires_at_ns or expiry_state)
        )
        if not temporal_state_ok:
            raise ValueError("watch timestamps are inconsistent")
        if self.wake_at_ns is not None:
            timestamp(self.wake_at_ns, field="wake_at_ns")
            if not self.created_at_ns <= self.wake_at_ns < self.expires_at_ns:
                raise ValueError("wake_at_ns must fall within the watch lifetime")
        refs = string_tuple(self.evidence_refs, field="evidence_refs", sorted_unique=True)
        invalidators = string_tuple(self.invalidators, field="invalidators", sorted_unique=True)
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "invalidators", invalidators)
        if self.last_event_id is not None:
            nonblank(self.last_event_id, field="last_event_id")
        if self.state_version == 0 and (self.state != WatchStateV2.DETECTED or self.last_event_id is not None):
            raise ValueError("initial watch state must be DETECTED without a prior event")
        if self.state_version > 0 and self.last_event_id is None:
            raise ValueError("advanced watch state requires last_event_id")
        if self.parent_watch_id is not None:
            nonblank(self.parent_watch_id, field="parent_watch_id")
            if self.parent_watch_id == self.watch_id:
                raise ValueError("watch cannot be its own parent")
        if self.handoff_receipt is not None:
            nonblank(self.handoff_receipt, field="handoff_receipt")
        if self.state == WatchStateV2.HANDED_OFF and self.handoff_receipt is None:
            raise ValueError("HANDED_OFF requires an accepted-pipeline handoff_receipt")
        if self.state != WatchStateV2.HANDED_OFF and self.handoff_receipt is not None:
            raise ValueError("handoff_receipt is only valid in HANDED_OFF")

    def transition_to(
        self,
        state: WatchStateV2,
        *,
        event_id: str,
        event_at_ns: int,
        transition_at_ns: int,
        required_next_event: str | None = None,
        wake_at_ns: int | None = None,
        reason: str | None = None,
        handoff_receipt: str | None = None,
    ) -> OpportunityWatchV2:
        target = _enum(WatchStateV2, state, field_name="state")
        if target not in _WATCH_NEXT[self.state]:
            raise ValueError(f"illegal watch transition {self.state.value}->{target.value}")
        nonblank(event_id, field="event_id")
        timestamp(event_at_ns, field="event_at_ns")
        timestamp(transition_at_ns, field="transition_at_ns")
        if event_at_ns < self.last_evaluated_at_ns or transition_at_ns < event_at_ns or transition_at_ns < self.updated_at_ns:
            raise ValueError("watch recheck must be chronological")
        if target == WatchStateV2.EXPIRED and transition_at_ns < self.expires_at_ns:
            raise ValueError("watch cannot expire before expires_at_ns")
        if target not in (WatchStateV2.EXPIRED, WatchStateV2.INVALIDATED) and transition_at_ns >= self.expires_at_ns:
            raise ValueError("expired watch cannot advance")
        invalidators = self.invalidators
        if target == WatchStateV2.INVALIDATED:
            if reason is None:
                raise ValueError("INVALIDATED transition requires reason")
            invalidators = tuple(sorted({*invalidators, nonblank(reason, field="reason")}))
        if target == WatchStateV2.HANDED_OFF:
            if handoff_receipt is None:
                raise ValueError("HANDED_OFF requires pipeline receipt")
            nonblank(handoff_receipt, field="handoff_receipt")
        next_event = "TERMINAL" if target in (WatchStateV2.EXPIRED, WatchStateV2.INVALIDATED, WatchStateV2.HANDED_OFF) else required_next_event
        if next_event is None:
            next_event = self.required_next_event
        nonblank(next_event, field="required_next_event")
        return replace(
            self,
            state=target,
            state_version=self.state_version + 1,
            updated_at_ns=transition_at_ns,
            last_event_id=event_id,
            last_evaluated_at_ns=event_at_ns,
            required_next_event=next_event,
            wake_at_ns=wake_at_ns,
            invalidators=invalidators,
            handoff_receipt=handoff_receipt if target == WatchStateV2.HANDED_OFF else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "watch_id": self.watch_id,
            "key": self.key.to_dict(),
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "policy_hash": self.policy_hash,
            "state": self.state.value,
            "state_version": self.state_version,
            "created_at_ns": self.created_at_ns,
            "updated_at_ns": self.updated_at_ns,
            "thesis_hash": self.thesis_hash,
            "evidence_refs": list(self.evidence_refs),
            "required_next_event": self.required_next_event,
            "wake_at_ns": self.wake_at_ns,
            "invalidators": list(self.invalidators),
            "expires_at_ns": self.expires_at_ns,
            "last_event_id": self.last_event_id,
            "last_evaluated_at_ns": self.last_evaluated_at_ns,
            "parent_watch_id": self.parent_watch_id,
            "handoff_receipt": self.handoff_receipt,
        }

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def content_hash(self) -> str:
        return sha256_json({"contract_type": "OpportunityWatchV2", "watch": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OpportunityWatchV2:
        fields = {"schema_version", "watch_id", "key", "strategy_id", "strategy_version", "policy_hash", "state", "state_version", "created_at_ns", "updated_at_ns", "thesis_hash", "evidence_refs", "required_next_event", "wake_at_ns", "invalidators", "expires_at_ns", "last_event_id", "last_evaluated_at_ns", "parent_watch_id", "handoff_receipt"}
        required = fields - {"wake_at_ns", "last_event_id", "parent_watch_id", "handoff_receipt"}
        d = strict_fields(data, expected=fields, required=required, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported OpportunityWatchV2 schema_version")
        for field_name in ("evidence_refs", "invalidators"):
            if not isinstance(d[field_name], list):
                raise ValueError(f"{field_name} must be an array")
        from .instruments import InstrumentKeyV2

        return cls(
            d["watch_id"], InstrumentKeyV2.from_dict(d["key"]), d["strategy_id"], d["strategy_version"], d["policy_hash"],
            _enum(WatchStateV2, d["state"], field_name="state"), d["state_version"], d["created_at_ns"], d["updated_at_ns"],
            d["thesis_hash"], tuple(d["evidence_refs"]), d["required_next_event"], d["expires_at_ns"], d["last_evaluated_at_ns"],
            d.get("wake_at_ns"), tuple(d["invalidators"]), d.get("last_event_id"), d.get("parent_watch_id"), d.get("handoff_receipt"),
        )
