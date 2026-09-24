"""Kronos-mini OHLC adapter boundary with explicit units and consistency checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..._serialization import decimal_value, nonblank, sha256_json, timestamp
from ..protocol import ModelManifestV2, ModelRequestV2
from ..worker_protocol import ModelProviderError
from .common import AdapterPredictionV2, normalize_quantile_outputs


@dataclass(frozen=True)
class OHLCVRowV2:
    event_at_ns: int
    available_at_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self) -> None:
        timestamp(self.event_at_ns, field="event_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.event_at_ns:
            raise ValueError("OHLC bar cannot be available before interval close")
        for name in ("open", "high", "low", "close", "volume"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), field=name))
        prices = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() for value in prices) or not self.volume.is_finite():
            raise ValueError("OHLC prices and volume must be finite")
        if min(prices) <= 0 or self.volume < 0:
            raise ValueError("OHLC prices must be positive and volume nonnegative")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("OHLC invariants failed")


@dataclass(frozen=True)
class KronosPreparedInputV2:
    rows: tuple[tuple[str, str, str, str, str], ...]
    input_unit: str
    price_transform: str
    sampling_version: str
    information_cutoff_ns: int
    preprocessing_hash: str


class KronosMiniAdapter:
    def __init__(
        self,
        manifest: ModelManifestV2,
        *,
        expected_manifest_hash: str,
        preprocessing_version: str,
        postprocessing_version: str,
        sampling_version: str,
        inference: Callable[[KronosPreparedInputV2, ModelRequestV2], Mapping[str, Any]] | None = None,
    ) -> None:
        if manifest.manifest_hash != expected_manifest_hash:
            raise ValueError("Kronos-mini requires the exact configured ModelManifestV2 hash")
        if not manifest.checkpoint_id or not manifest.checkpoint_revision or not manifest.weight_sha256:
            raise ValueError("Kronos-mini manifest must identify exact checkpoint and weights")
        if not manifest.tokenizer_id or not manifest.tokenizer_revision or not manifest.tokenizer_sha256:
            raise ValueError("Kronos-mini manifest must pin tokenizer identity, revision and hashes")
        if manifest.promotion_status.value == "INTEGRATED":
            raise ValueError("Session-012 adapter fixtures cannot assert INTEGRATED checkpoint status")
        preprocessing_version = nonblank(preprocessing_version, field="preprocessing_version")
        postprocessing_version = nonblank(postprocessing_version, field="postprocessing_version")
        sampling_version = nonblank(sampling_version, field="sampling_version")
        self.manifest = manifest
        self.preprocessing_version = preprocessing_version
        self.postprocessing_version = postprocessing_version
        self.sampling_version = sampling_version
        self.preprocessing_hash = sha256_json(
            {"adapter": "kronos-mini", "preprocessing_version": preprocessing_version, "sampling_version": sampling_version}
        )
        self.postprocessing_hash = sha256_json({"adapter": "kronos-mini", "postprocessing_version": postprocessing_version})
        if manifest.preprocessing_hash != self.preprocessing_hash or manifest.postprocessing_hash != self.postprocessing_hash:
            raise ValueError("Kronos-mini preprocessing/postprocessing manifest hashes mismatch")
        self.inference = inference
        self.artifact_status = "UNVERIFIED" if inference is None else "TESTED"

    def preprocess(self, rows: tuple[OHLCVRowV2, ...], *, information_cutoff_ns: int) -> KronosPreparedInputV2:
        timestamp(information_cutoff_ns, field="information_cutoff_ns")
        if any(not isinstance(row, OHLCVRowV2) for row in rows):
            raise ValueError("Kronos-mini accepts only validated OHLCVRowV2 rows")
        if any(row.available_at_ns > information_cutoff_ns or row.event_at_ns > information_cutoff_ns for row in rows):
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "Kronos-mini prefix includes data unavailable at origin")
        if tuple(sorted(rows, key=lambda row: row.event_at_ns)) != rows:
            raise ValueError("Kronos-mini causal prefix must be ordered by event time")
        if len({row.event_at_ns for row in rows}) != len(rows):
            raise ValueError("Kronos-mini causal prefix cannot contain duplicate bar boundaries")
        normalized = tuple(
            (str(row.open), str(row.high), str(row.low), str(row.close), str(row.volume)) for row in rows
        )
        return KronosPreparedInputV2(
            normalized,
            "quote_currency_price_and_base_volume",
            "raw_decimal_OHLCV_no_future_normalization",
            self.sampling_version,
            information_cutoff_ns,
            self.preprocessing_hash,
        )

    def postprocess(self, outputs: Mapping[str, Any], request: ModelRequestV2, *, input_hash: str) -> AdapterPredictionV2:
        return normalize_quantile_outputs(outputs, request, manifest_hash=self.manifest.manifest_hash, input_hash=input_hash)

    def infer_fixture_or_fail(
        self, rows: tuple[OHLCVRowV2, ...], *, information_cutoff_ns: int, request: ModelRequestV2
    ) -> AdapterPredictionV2:
        if self.inference is None:
            raise ModelProviderError("MODEL_ARTIFACT_UNAVAILABLE", "Kronos-mini checkpoint/package is unavailable; status UNVERIFIED")
        if request.model_manifest_hash != self.manifest.manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "Kronos-mini request references a different manifest")
        if information_cutoff_ns > request.information_cutoff_ns:
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "Kronos-mini input cutoff exceeds the request information cutoff")
        prepared = self.preprocess(rows, information_cutoff_ns=information_cutoff_ns)
        input_hash = sha256_json(
            {"rows": prepared.rows, "unit": prepared.input_unit, "transform": prepared.price_transform, "cutoff": information_cutoff_ns}
        )
        if request.input_hash != input_hash:
            raise ModelProviderError("INPUT_HASH_MISMATCH", "Kronos-mini causal input hash does not match request")
        result = self.inference(prepared, request)
        return self.postprocess(result, request, input_hash=input_hash)
