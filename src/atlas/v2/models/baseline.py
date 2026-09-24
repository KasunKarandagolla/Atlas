"""Small deterministic causal empirical baseline for Model Arena plumbing."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .._serialization import FrozenMap, decimal_value, sha256_json, strict_fields, timestamp
from ..contracts import ForecastStatusV2
from ..instruments import InstrumentKeyV2
from .protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2
from .provider import ModelProvider
from .worker_protocol import ModelProviderError

_HORIZONS_NS = {900_000_000_000, 3_600_000_000_000, 14_400_000_000_000}


@dataclass(frozen=True)
class CausalCloseV2:
    close_at_ns: int
    available_at_ns: int
    close: Decimal

    def __post_init__(self) -> None:
        timestamp(self.close_at_ns, field="close_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.close_at_ns:
            raise ValueError("causal close cannot be available before its bar closes")
        close = decimal_value(self.close, field="close")
        object.__setattr__(self, "close", close)
        if not close.is_finite() or close <= 0:
            raise ValueError("causal close must be positive and finite")


@dataclass(frozen=True)
class BaselineInputsV2:
    instrument_key: InstrumentKeyV2
    information_cutoff_ns: int
    closes: tuple[CausalCloseV2, ...]

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        closes = tuple(self.closes)
        if any(not isinstance(item, CausalCloseV2) for item in closes):
            raise ValueError("baseline closes must be CausalCloseV2")
        if tuple(sorted(closes, key=lambda item: (item.close_at_ns, item.available_at_ns))) != closes:
            raise ValueError("baseline closes must be sorted chronologically")
        if len({item.close_at_ns for item in closes}) != len(closes):
            raise ValueError("baseline closes cannot contain conflicting duplicate close times")
        if any(item.available_at_ns > self.information_cutoff_ns or item.close_at_ns > self.information_cutoff_ns for item in closes):
            raise ValueError("baseline input contains a close unavailable at information cutoff")
        object.__setattr__(self, "closes", closes)

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "instrument_key": self.instrument_key.to_dict(),
            "information_cutoff_ns": self.information_cutoff_ns,
            "closes": [
                {"close_at_ns": item.close_at_ns, "available_at_ns": item.available_at_ns, "close": str(item.close)}
                for item in self.closes
            ],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BaselineInputsV2:
        value = strict_fields(
            data,
            expected={"schema_version", "instrument_key", "information_cutoff_ns", "closes"},
            required={"schema_version", "instrument_key", "information_cutoff_ns", "closes"},
            name=cls.__name__,
        )
        if type(value["schema_version"]) is not int or value["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported BaselineInputsV2 schema_version")
        rows = value["closes"]
        if not isinstance(rows, list):
            raise ValueError("baseline closes must be an array")
        closes: list[CausalCloseV2] = []
        for row in rows:
            item = strict_fields(
                row,
                expected={"close_at_ns", "available_at_ns", "close"},
                required={"close_at_ns", "available_at_ns", "close"},
                name="BaselineCloseV2",
            )
            closes.append(
                CausalCloseV2(
                    item["close_at_ns"], item["available_at_ns"],
                    decimal_value(item["close"], field="baseline.close", wire=True),
                )
            )
        return cls(
            InstrumentKeyV2.from_dict(value["instrument_key"]),
            value["information_cutoff_ns"],
            tuple(closes),
        )


@dataclass(frozen=True)
class BaselineForecastV2:
    artifact: ForecastArtifactV2
    values: FrozenMap


def _empirical_quantile(values: list[float], q: Decimal) -> float:
    ordered = sorted(values)
    # Nearest lower order statistic is deterministic and makes no parametric claim.
    index = int((q * Decimal(len(ordered) - 1)).to_integral_value(rounding="ROUND_FLOOR"))
    return ordered[index]


def _format(value: float) -> str:
    return format(value, ".17g")


class StatisticalBaselineV2(ModelProvider):
    """Empirical horizon-return control path. This provider makes no alpha claim."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns) -> None:
        self.clock_ns = clock_ns

    def infer(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        started_at_ns: int,
    ) -> ForecastArtifactV2:
        del started_at_ns
        try:
            typed_inputs = BaselineInputsV2.from_dict(inputs)
        except (TypeError, ValueError, KeyError) as exc:
            raise ModelProviderError("BASELINE_INPUT_INVALID", "statistical baseline inputs are invalid") from exc
        return self.predict(request, manifest, typed_inputs, completed_at_ns=self.clock_ns()).artifact

    def predict(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: BaselineInputsV2,
        *,
        completed_at_ns: int,
    ) -> BaselineForecastV2:
        if request.instrument_key != inputs.instrument_key:
            raise ModelProviderError("INSTRUMENT_MISMATCH", "baseline input instrument does not match request")
        if request.model_manifest_hash != manifest.manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "baseline manifest hash does not match request")
        if request.input_hash != inputs.content_hash:
            raise ModelProviderError("INPUT_HASH_MISMATCH", "baseline input content hash does not match request")
        if inputs.information_cutoff_ns > request.information_cutoff_ns:
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "baseline inputs exceed request information cutoff")
        if completed_at_ns >= request.deadline_ns:
            raise ModelProviderError("DEADLINE_EXPIRED", "baseline result missed original request deadline")
        close_by_time = {item.close_at_ns: item.close for item in inputs.closes}
        outputs: dict[str, str] = {}
        missing: list[str] = []
        sample_counts: dict[str, int] = {}
        targets = set(request.requested_targets)
        for target in request.requested_targets:
            for horizon in request.requested_horizons:
                output_prefix = f"{target}:{horizon}"
                if target != "log_return" or horizon not in _HORIZONS_NS:
                    missing.append(f"{output_prefix}:UNSUPPORTED")
                    continue
                sample: list[float] = []
                for at_ns, close in close_by_time.items():
                    earlier = close_by_time.get(at_ns - horizon)
                    if earlier is None:
                        continue
                    sample.append(math.log(float(close / earlier)))
                if not sample:
                    missing.append(f"{output_prefix}:INSUFFICIENT_CAUSAL_HISTORY")
                    continue
                sample_counts[output_prefix] = len(sample)
                outputs[f"{output_prefix}:mean"] = _format(sum(sample) / len(sample))
                for quantile in request.requested_quantiles:
                    outputs[f"{output_prefix}:q{quantile}"] = _format(_empirical_quantile(sample, quantile))
        missing = sorted(missing)
        if not outputs:
            status = ForecastStatusV2.NOT_ESTIMABLE
        elif missing:
            status = ForecastStatusV2.PARTIAL
        else:
            status = ForecastStatusV2.AVAILABLE
        value_hash = sha256_json(outputs)
        artifact = ForecastArtifactV2(
            request_id=request.request_id,
            model_manifest_hash=manifest.manifest_hash,
            input_hash=inputs.content_hash,
            inference_started_ns=completed_at_ns,
            completed_ns=completed_at_ns,
            received_ns=completed_at_ns,
            expires_ns=request.deadline_ns,
            targets=tuple(sorted(targets)),
            horizons=request.requested_horizons,
            native_quantiles=request.requested_quantiles,
            values_ref=value_hash,
            samples_ref=None,
            missing_outputs=tuple(missing),
            units=FrozenMap(dict.fromkeys(targets, "log_return")),
            resource_metrics=FrozenMap({"provider": "statistical-baseline-v1", "sample_counts": sample_counts}),
            status=status,
        )
        return BaselineForecastV2(artifact, FrozenMap(outputs))
