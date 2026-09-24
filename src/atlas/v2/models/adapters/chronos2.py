"""Chronos-2 causal adapter boundary; checkpoint/package remains UNVERIFIED.

Preparation is pure. Real inference can run only through LocalProcessProvider's
qualified isolated worker. Fixture postprocessing never grants decision authority.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..._serialization import sha256_json
from ..local_process import LocalProcessProvider
from ..protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2
from ..worker_protocol import ModelProviderError
from .common import AdapterPredictionV2, CausalModelInputV2, normalize_quantile_outputs


@dataclass(frozen=True)
class ChronosPreparedV2:
    values: tuple[str, ...]
    observed_covariates: tuple[tuple[str, str], ...]
    known_covariates: tuple[tuple[str, str], ...]
    information_cutoff_ns: int
    preprocessing_hash: str


class Chronos2Adapter:
    def __init__(self, manifest: ModelManifestV2, *, expected_manifest_hash: str,
                 expected_weight_sha256: tuple[str, ...], expected_tokenizer_sha256: tuple[str, ...],
                 preprocessing_version: str = "CHRONOS2_PRE_V1", postprocessing_version: str = "CHRONOS2_POST_V1") -> None:
        if manifest.manifest_hash != expected_manifest_hash or manifest.weight_sha256 != expected_weight_sha256:
            raise ValueError("Chronos-2 requires exact manifest and checkpoint hashes")
        if not manifest.weight_sha256 or not manifest.checkpoint_id or not manifest.checkpoint_revision:
            raise ValueError("Chronos-2 requires exact checkpoint identity")
        if manifest.tokenizer_sha256 != expected_tokenizer_sha256:
            raise ValueError("Chronos-2 tokenizer hash mismatch")
        if manifest.tokenizer_id and not manifest.tokenizer_sha256:
            raise ValueError("Chronos-2 tokenizer identity requires exact hashes")
        # Manifest promotion records registry workflow; the unavailable real
        # checkpoint remains independently UNVERIFIED at this adapter boundary.
        self.manifest = manifest
        self.preprocessing_hash = sha256_json({"adapter": "chronos2", "version": preprocessing_version})
        self.postprocessing_hash = sha256_json({"adapter": "chronos2", "version": postprocessing_version})
        if manifest.preprocessing_hash != self.preprocessing_hash or manifest.postprocessing_hash != self.postprocessing_hash:
            raise ValueError("Chronos-2 preprocessing/postprocessing manifest hashes mismatch")
        self.artifact_status = "UNVERIFIED"
        self.decision_influence = Decimal(0)

    def preprocess(self, inputs: CausalModelInputV2, request: ModelRequestV2) -> ChronosPreparedV2:
        if request.model_manifest_hash != self.manifest.manifest_hash or request.input_hash != inputs.content_hash:
            raise ModelProviderError("INPUT_OR_MANIFEST_MISMATCH", "Chronos-2 request does not bind exact inputs")
        if inputs.information_cutoff_ns > request.information_cutoff_ns or len(inputs.features) > self.manifest.context_limit:
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "Chronos-2 prefix exceeds cutoff or context")
        unsupported = [target for target in request.requested_targets if target not in self.manifest.supported_outputs]
        if unsupported or any(h not in (900_000_000_000, 3_600_000_000_000, 14_400_000_000_000, 86_400_000_000_000) for h in request.requested_horizons):
            raise ModelProviderError("UNSUPPORTED_OUTPUT", "Chronos-2 requested target or horizon unsupported")
        if any(q not in (Decimal("0.05"), Decimal("0.5"), Decimal("0.95")) for q in request.requested_quantiles):
            raise ModelProviderError("UNSUPPORTED_OUTPUT", "Chronos-2 requested quantile unsupported")
        observed: list[tuple[str, str]] = []
        known: list[tuple[str, str]] = []
        values: list[str] = []
        for feature in inputs.features:
            if feature.available_at_ns > request.information_cutoff_ns:
                raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "Chronos-2 feature unavailable at origin")
            if feature.known_ahead_at_origin and not feature.feature_id.startswith(("calendar_", "scheduled_", "time_", "holiday_")):
                raise ModelProviderError("FUTURE_REALIZED_COVARIATE", "only deterministic calendar features may be known covariates")
            pair = (feature.feature_id, str(feature.value))
            (known if feature.known_ahead_at_origin else observed).append(pair)
            values.append(str(feature.value))
        return ChronosPreparedV2(tuple(values), tuple(observed), tuple(known), inputs.information_cutoff_ns, self.preprocessing_hash)

    def postprocess_fixture(self, outputs: Mapping[str, Any], request: ModelRequestV2, *, input_hash: str) -> AdapterPredictionV2:
        """Schema/parity fixture only; no checkpoint load or promotion."""
        if request.model_manifest_hash != self.manifest.manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "wrong Chronos-2 manifest")
        return normalize_quantile_outputs(outputs, request, manifest_hash=self.manifest.manifest_hash, input_hash=input_hash)

    def infer_isolated(self, provider: LocalProcessProvider, inputs: CausalModelInputV2,
                       request: ModelRequestV2, *, started_at_ns: int) -> ForecastArtifactV2:
        if not isinstance(provider, LocalProcessProvider):
            raise ModelProviderError("WORKER_ISOLATION_REQUIRED", "Chronos-2 requires isolated LocalProcessProvider")
        prepared = self.preprocess(inputs, request)
        return provider.infer(request, self.manifest, {"chronos2_prepared_v1": {
            "values": prepared.values, "observed_covariates": prepared.observed_covariates,
            "known_covariates": prepared.known_covariates, "information_cutoff_ns": prepared.information_cutoff_ns,
            "preprocessing_hash": prepared.preprocessing_hash}}, started_at_ns=started_at_ns)
