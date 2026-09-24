"""Shared causal adapter input/output records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..._serialization import FrozenMap, decimal_value, nonblank, sha256_json, timestamp
from ..protocol import ModelRequestV2
from ..worker_protocol import ModelProviderError


@dataclass(frozen=True)
class CausalFeatureInputV2:
    feature_id: str
    value: Decimal
    available_at_ns: int
    unit: str
    known_ahead_at_origin: bool = False

    def __post_init__(self) -> None:
        nonblank(self.feature_id, field="feature_id")
        nonblank(self.unit, field="unit")
        value = decimal_value(self.value, field="feature value")
        if not value.is_finite():
            raise ValueError("model feature must be finite")
        if type(self.known_ahead_at_origin) is not bool:
            raise ValueError("known_ahead_at_origin must be an explicit boolean")
        object.__setattr__(self, "value", value)
        timestamp(self.available_at_ns, field="available_at_ns")


@dataclass(frozen=True)
class CausalModelInputV2:
    information_cutoff_ns: int
    features: tuple[CausalFeatureInputV2, ...]

    def __post_init__(self) -> None:
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        features = tuple(self.features)
        if any(not isinstance(item, CausalFeatureInputV2) for item in features):
            raise ValueError("features must contain CausalFeatureInputV2")
        ids = tuple(item.feature_id for item in features)
        if ids != tuple(sorted(set(ids))):
            raise ValueError("causal model features must be sorted and unique")
        if any(item.available_at_ns > self.information_cutoff_ns for item in features):
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "model input contains data unavailable at origin")
        object.__setattr__(self, "features", features)

    @property
    def content_hash(self) -> str:
        return sha256_json(
            {
                "information_cutoff_ns": self.information_cutoff_ns,
                "features": [
                    {
                        "feature_id": item.feature_id,
                        "value": str(item.value),
                        "available_at_ns": item.available_at_ns,
                        "unit": item.unit,
                        "known_ahead_at_origin": item.known_ahead_at_origin,
                    }
                    for item in self.features
                ],
            }
        )


@dataclass(frozen=True)
class AdapterPredictionV2:
    manifest_hash: str
    input_hash: str
    values: FrozenMap
    missing_outputs: tuple[str, ...]


def normalize_quantile_outputs(
    outputs: Mapping[str, Any], request: ModelRequestV2, *, manifest_hash: str, input_hash: str
) -> AdapterPredictionV2:
    requested_names = {
        f"{target}:{horizon}:q{quantile}"
        for target in request.requested_targets
        for horizon in request.requested_horizons
        for quantile in request.requested_quantiles
    }
    unknown = set(outputs) - requested_names
    if unknown:
        raise ModelProviderError("ADAPTER_OUTPUT_INVALID", f"model returned unrequested output keys: {sorted(unknown)}")
    normalized: dict[str, Decimal] = {}
    missing: list[str] = []
    for target in request.requested_targets:
        for horizon in request.requested_horizons:
            for quantile in request.requested_quantiles:
                name = f"{target}:{horizon}:q{quantile}"
                if name not in outputs:
                    missing.append(name)
                    continue
                try:
                    value = Decimal(str(outputs[name]))
                except Exception as exc:
                    raise ModelProviderError("ADAPTER_OUTPUT_INVALID", f"model output {name} is not numeric") from exc
                if not value.is_finite():
                    raise ModelProviderError("ADAPTER_OUTPUT_INVALID", f"model output {name} is non-finite")
                normalized[name] = value
    return AdapterPredictionV2(manifest_hash, input_hash, FrozenMap(normalized), tuple(sorted(missing)))
