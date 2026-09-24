"""Immutable model request and evidence types. No provider or worker is defined."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from atlas.domain.money import canonical_decimal_str

from .._serialization import (
    FrozenMap,
    canonical_json,
    decimal_value,
    nonblank,
    sha256_json,
    sha256_ref,
    strict_fields,
    string_tuple,
    timestamp,
)
from ..contracts import ForecastStatusV2
from ..instruments import InstrumentKeyV2


class PromotionStatusV2(StrEnum):
    INTEGRATED = "INTEGRATED"
    ENGINEERING_PASS = "ENGINEERING_PASS"
    HISTORICAL_DIAGNOSTIC = "HISTORICAL_DIAGNOSTIC"
    PROSPECTIVE_SHADOW = "PROSPECTIVE_SHADOW"
    INCREMENTAL_VALUE_PASS = "INCREMENTAL_VALUE_PASS"
    DECISION_ELIGIBLE = "DECISION_ELIGIBLE"


def _enum(enum_type: type[StrEnum], value: Any, *, field_name: str) -> Any:
    try:
        return enum_type(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


@dataclass(frozen=True)
class ModelManifestV2:
    provider: str
    source_repository: str
    source_commit: str
    checkpoint_id: str
    checkpoint_revision: str
    weight_sha256: tuple[str, ...]
    code_license_ref: str
    weight_license_ref: str
    allowed_use_status: str
    preprocessing_hash: str
    postprocessing_hash: str
    environment_lock_hash: str
    device: str
    precision: str
    supported_inputs: tuple[str, ...]
    supported_outputs: tuple[str, ...]
    context_limit: int
    contamination_class: str
    promotion_status: PromotionStatusV2
    tokenizer_id: str | None = None
    tokenizer_revision: str | None = None
    tokenizer_sha256: tuple[str, ...] = ()
    training_cutoff_ns: int | None = None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        for name in (
            "provider", "source_repository", "source_commit", "checkpoint_id",
            "checkpoint_revision", "code_license_ref", "weight_license_ref",
            "allowed_use_status", "device", "precision", "contamination_class",
        ):
            nonblank(getattr(self, name), field=name)
        for name in ("preprocessing_hash", "postprocessing_hash", "environment_lock_hash"):
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "weight_sha256", tuple(sha256_ref(v, field="weight_sha256") for v in self.weight_sha256))
        object.__setattr__(self, "tokenizer_sha256", tuple(sha256_ref(v, field="tokenizer_sha256") for v in self.tokenizer_sha256))
        if len(set(self.weight_sha256)) != len(self.weight_sha256) or len(set(self.tokenizer_sha256)) != len(self.tokenizer_sha256):
            raise ValueError("weight and tokenizer hashes must be unique")
        for name in ("tokenizer_id", "tokenizer_revision"):
            value = getattr(self, name)
            if value is not None:
                nonblank(value, field=name)
        if (self.tokenizer_id is None) != (self.tokenizer_revision is None):
            raise ValueError("tokenizer_id and tokenizer_revision must be supplied together")
        if self.tokenizer_sha256 and self.tokenizer_id is None:
            raise ValueError("tokenizer hashes require tokenizer identity")
        if type(self.context_limit) is not int or self.context_limit <= 0:
            raise ValueError("context_limit must be a positive integer")
        if self.training_cutoff_ns is not None:
            timestamp(self.training_cutoff_ns, field="training_cutoff_ns")
        object.__setattr__(self, "supported_inputs", string_tuple(self.supported_inputs, field="supported_inputs", sorted_unique=True))
        object.__setattr__(self, "supported_outputs", string_tuple(self.supported_outputs, field="supported_outputs", sorted_unique=True))
        object.__setattr__(self, "promotion_status", _enum(PromotionStatusV2, self.promotion_status, field_name="promotion_status"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "provider": self.provider,
            "source_repository": self.source_repository,
            "source_commit": self.source_commit,
            "checkpoint_id": self.checkpoint_id,
            "checkpoint_revision": self.checkpoint_revision,
            "weight_sha256": list(self.weight_sha256),
            "tokenizer_id": self.tokenizer_id,
            "tokenizer_revision": self.tokenizer_revision,
            "tokenizer_sha256": list(self.tokenizer_sha256),
            "code_license_ref": self.code_license_ref,
            "weight_license_ref": self.weight_license_ref,
            "allowed_use_status": self.allowed_use_status,
            "preprocessing_hash": self.preprocessing_hash,
            "postprocessing_hash": self.postprocessing_hash,
            "environment_lock_hash": self.environment_lock_hash,
            "device": self.device,
            "precision": self.precision,
            "supported_inputs": list(self.supported_inputs),
            "supported_outputs": list(self.supported_outputs),
            "context_limit": self.context_limit,
            "training_cutoff_ns": self.training_cutoff_ns,
            "contamination_class": self.contamination_class,
            "promotion_status": self.promotion_status.value,
        }

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def manifest_hash(self) -> str:
        return sha256_json({"contract_type": "ModelManifestV2", "manifest": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ModelManifestV2:
        names = {item.name for item in fields(cls)} | {"schema_version"}
        d = strict_fields(data, expected=names, required=names, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported ModelManifestV2 schema_version")
        array_fields = ("weight_sha256", "tokenizer_sha256", "supported_inputs", "supported_outputs")
        if any(not isinstance(d[name], list) for name in array_fields):
            raise ValueError("manifest list fields must be arrays")
        args = {item.name: d[item.name] for item in fields(cls)}
        for name in array_fields:
            args[name] = tuple(d[name])
        return cls(**args)


@dataclass(frozen=True)
class ModelRequestV2:
    request_id: str
    input_artifact_refs: tuple[str, ...]
    input_hash: str
    instrument_key: InstrumentKeyV2
    policy_context_ref: str
    model_manifest_hash: str
    information_cutoff_ns: int
    requested_targets: tuple[str, ...]
    requested_horizons: tuple[int, ...]
    requested_quantiles: tuple[Decimal, ...]
    deadline_ns: int
    seed: int
    resource_budget: FrozenMap

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_key, InstrumentKeyV2):
            raise ValueError("instrument_key must be InstrumentKeyV2")
        object.__setattr__(self, "input_artifact_refs", string_tuple(self.input_artifact_refs, field="input_artifact_refs", sorted_unique=True))
        for name in ("input_hash", "model_manifest_hash"):
            sha256_ref(getattr(self, name), field=name)
        nonblank(self.request_id, field="request_id")
        nonblank(self.policy_context_ref, field="policy_context_ref")
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        timestamp(self.deadline_ns, field="deadline_ns")
        if self.deadline_ns <= self.information_cutoff_ns:
            raise ValueError("deadline_ns must be after information_cutoff_ns")
        object.__setattr__(self, "requested_targets", string_tuple(self.requested_targets, field="requested_targets", sorted_unique=True))
        horizons = tuple(self.requested_horizons)
        if any(type(h) is not int or h <= 0 for h in horizons) or horizons != tuple(sorted(set(horizons))):
            raise ValueError("requested_horizons must be sorted, unique positive integers")
        object.__setattr__(self, "requested_horizons", horizons)
        quantiles = tuple(decimal_value(q, field="requested_quantiles") for q in self.requested_quantiles)
        if any(q < 0 or q > 1 for q in quantiles) or quantiles != tuple(sorted(set(quantiles))):
            raise ValueError("requested_quantiles must be sorted, unique values in [0, 1]")
        object.__setattr__(self, "requested_quantiles", quantiles)
        if type(self.seed) is not int or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        budget = self.resource_budget if isinstance(self.resource_budget, FrozenMap) else FrozenMap(self.resource_budget)
        for key, value in budget.items():
            if not key or isinstance(value, bool) or not isinstance(value, (int, Decimal)) or value < 0:
                raise ValueError("resource_budget values must be nonnegative numeric limits")
        object.__setattr__(self, "resource_budget", budget)
        expected_id = self.expected_request_id()
        if self.request_id != expected_id:
            raise ValueError("request_id does not match deterministic request identity")

    def _identity_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "input_artifact_refs": list(self.input_artifact_refs),
            "input_hash": self.input_hash,
            "instrument_key": self.instrument_key.to_dict(),
            "policy_context_ref": self.policy_context_ref,
            "model_manifest_hash": self.model_manifest_hash,
            "information_cutoff_ns": self.information_cutoff_ns,
            "requested_targets": list(self.requested_targets),
            "requested_horizons": list(self.requested_horizons),
            "requested_quantiles": [canonical_decimal_str(value) for value in self.requested_quantiles],
            "deadline_ns": self.deadline_ns,
            "seed": self.seed,
            "resource_budget": self.resource_budget.to_dict(),
        }

    def expected_request_id(self) -> str:
        return "req_" + sha256_json({"contract_type": "ModelRequestV2", "request": self._identity_dict()})

    def to_dict(self) -> dict[str, Any]:
        result = self._identity_dict()
        result["request_id"] = self.request_id
        return result

    @property
    def request_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def build(cls, **values: Any) -> ModelRequestV2:
        if "request_id" in values:
            raise ValueError("build derives request_id; omit it")
        normalized = dict(values)
        normalized["input_artifact_refs"] = tuple(normalized["input_artifact_refs"])
        normalized["requested_targets"] = tuple(normalized["requested_targets"])
        normalized["requested_horizons"] = tuple(normalized["requested_horizons"])
        normalized["requested_quantiles"] = tuple(decimal_value(v, field="requested_quantiles") for v in normalized["requested_quantiles"])
        normalized["resource_budget"] = normalized["resource_budget"] if isinstance(normalized["resource_budget"], FrozenMap) else FrozenMap(normalized["resource_budget"])
        identity = {
            "schema_version": cls.SCHEMA_VERSION,
            "input_artifact_refs": list(normalized["input_artifact_refs"]),
            "input_hash": normalized["input_hash"],
            "instrument_key": normalized["instrument_key"].to_dict(),
            "policy_context_ref": normalized["policy_context_ref"],
            "model_manifest_hash": normalized["model_manifest_hash"],
            "information_cutoff_ns": normalized["information_cutoff_ns"],
            "requested_targets": list(normalized["requested_targets"]),
            "requested_horizons": list(normalized["requested_horizons"]),
            "requested_quantiles": list(normalized["requested_quantiles"]),
            "deadline_ns": normalized["deadline_ns"],
            "seed": normalized["seed"],
            "resource_budget": normalized["resource_budget"].to_dict(),
        }
        request_id = "req_" + sha256_json({"contract_type": "ModelRequestV2", "request": identity})
        return cls(request_id=request_id, **normalized)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ModelRequestV2:
        names = {item.name for item in fields(cls)} | {"schema_version"}
        d = strict_fields(data, expected=names, required=names, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported ModelRequestV2 schema_version")
        for name in ("input_artifact_refs", "requested_targets", "requested_horizons", "requested_quantiles"):
            if not isinstance(d[name], list):
                raise ValueError(f"{name} must be an array")
        if not isinstance(d["resource_budget"], Mapping):
            raise ValueError("resource_budget must be an object")
        resource_budget: dict[str, Any] = {}
        for name, value in d["resource_budget"].items():
            if isinstance(value, str):
                value = decimal_value(value, field=f"resource_budget.{name}", wire=True)
            resource_budget[name] = value
        return cls(
            request_id=d["request_id"],
            input_artifact_refs=tuple(d["input_artifact_refs"]),
            input_hash=d["input_hash"],
            instrument_key=InstrumentKeyV2.from_dict(d["instrument_key"]),
            policy_context_ref=d["policy_context_ref"],
            model_manifest_hash=d["model_manifest_hash"],
            information_cutoff_ns=d["information_cutoff_ns"],
            requested_targets=tuple(d["requested_targets"]),
            requested_horizons=tuple(d["requested_horizons"]),
            requested_quantiles=tuple(decimal_value(v, field="requested_quantiles", wire=True) for v in d["requested_quantiles"]),
            deadline_ns=d["deadline_ns"],
            seed=d["seed"],
            resource_budget=FrozenMap(resource_budget),
        )


@dataclass(frozen=True)
class ForecastArtifactV2:
    request_id: str
    model_manifest_hash: str
    input_hash: str
    inference_started_ns: int
    completed_ns: int
    received_ns: int
    expires_ns: int
    targets: tuple[str, ...]
    horizons: tuple[int, ...]
    native_quantiles: tuple[Decimal, ...]
    values_ref: str | None
    samples_ref: str | None
    missing_outputs: tuple[str, ...]
    units: FrozenMap
    resource_metrics: FrozenMap
    status: ForecastStatusV2

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        nonblank(self.request_id, field="request_id")
        for name in ("model_manifest_hash", "input_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("inference_started_ns", "completed_ns", "received_ns", "expires_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.inference_started_ns <= self.completed_ns <= self.received_ns:
            raise ValueError("forecast timing must satisfy started <= completed <= received")
        if self.expires_ns < self.inference_started_ns:
            raise ValueError("expires_ns cannot precede inference start")
        object.__setattr__(self, "targets", string_tuple(self.targets, field="targets", sorted_unique=True))
        horizons = tuple(self.horizons)
        if any(type(value) is not int or value <= 0 for value in horizons) or horizons != tuple(sorted(set(horizons))):
            raise ValueError("horizons must be sorted, unique positive integers")
        object.__setattr__(self, "horizons", horizons)
        object.__setattr__(self, "native_quantiles", tuple(decimal_value(v, field="native_quantiles") for v in self.native_quantiles))
        for name in ("values_ref", "samples_ref"):
            value = getattr(self, name)
            if value is not None:
                nonblank(value, field=name)
        object.__setattr__(self, "missing_outputs", string_tuple(self.missing_outputs, field="missing_outputs", sorted_unique=True))
        for name in ("units", "resource_metrics"):
            value = getattr(self, name)
            object.__setattr__(self, name, value if isinstance(value, FrozenMap) else FrozenMap(value))
        object.__setattr__(self, "status", _enum(ForecastStatusV2, self.status, field_name="status"))

    def is_usable_at(self, decision_deadline_ns: int) -> bool:
        timestamp(decision_deadline_ns, field="decision_deadline_ns")
        return (
            self.status in (ForecastStatusV2.AVAILABLE, ForecastStatusV2.PARTIAL)
            and self.completed_ns <= self.expires_ns
            and self.received_ns <= self.expires_ns
            and self.received_ns <= decision_deadline_ns
            and decision_deadline_ns <= self.expires_ns
            and not self.missing_outputs
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "request_id": self.request_id,
            "model_manifest_hash": self.model_manifest_hash,
            "input_hash": self.input_hash,
            "inference_started_ns": self.inference_started_ns,
            "completed_ns": self.completed_ns,
            "received_ns": self.received_ns,
            "expires_ns": self.expires_ns,
            "targets": list(self.targets),
            "horizons": list(self.horizons),
            "native_quantiles": [canonical_decimal_str(value) for value in self.native_quantiles],
            "values_ref": self.values_ref,
            "samples_ref": self.samples_ref,
            "missing_outputs": list(self.missing_outputs),
            "units": self.units.to_dict(),
            "resource_metrics": self.resource_metrics.to_dict(),
            "status": self.status.value,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json({"contract_type": "ForecastArtifactV2", "artifact": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastArtifactV2:
        names = {item.name for item in fields(cls)} | {"schema_version"}
        d = strict_fields(data, expected=names, required=names, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported ForecastArtifactV2 schema_version")
        for name in ("targets", "horizons", "native_quantiles", "missing_outputs"):
            if not isinstance(d[name], list):
                raise ValueError(f"{name} must be an array")
        return cls(
            request_id=d["request_id"], model_manifest_hash=d["model_manifest_hash"], input_hash=d["input_hash"],
            inference_started_ns=d["inference_started_ns"], completed_ns=d["completed_ns"], received_ns=d["received_ns"], expires_ns=d["expires_ns"],
            targets=tuple(d["targets"]), horizons=tuple(d["horizons"]),
            native_quantiles=tuple(decimal_value(v, field="native_quantiles", wire=True) for v in d["native_quantiles"]),
            values_ref=d["values_ref"], samples_ref=d["samples_ref"], missing_outputs=tuple(d["missing_outputs"]),
            units=FrozenMap(d["units"]), resource_metrics=FrozenMap(d["resource_metrics"]), status=d["status"],
        )
