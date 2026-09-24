"""TiRex-2 adapter boundary with causal fixtures and honest artifact status."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ..._serialization import nonblank, sha256_json
from ..protocol import ModelManifestV2, ModelRequestV2
from ..worker_protocol import ModelProviderError
from .common import AdapterPredictionV2, CausalModelInputV2, normalize_quantile_outputs


@dataclass(frozen=True)
class TiRexPreparedInputV2:
    feature_ids: tuple[str, ...]
    values: tuple[str, ...]
    units: tuple[str, ...]
    information_cutoff_ns: int
    preprocessing_hash: str


class TiRex2Adapter:
    def __init__(
        self,
        manifest: ModelManifestV2,
        *,
        expected_manifest_hash: str,
        preprocessing_version: str,
        postprocessing_version: str,
        inference: Callable[[TiRexPreparedInputV2, ModelRequestV2], Mapping[str, Any]] | None = None,
    ) -> None:
        if manifest.manifest_hash != expected_manifest_hash:
            raise ValueError("TiRex-2 requires the exact configured ModelManifestV2 hash")
        if not manifest.checkpoint_id or not manifest.checkpoint_revision or not manifest.weight_sha256:
            raise ValueError("TiRex-2 manifest must identify the exact checkpoint and weight hashes")
        if manifest.promotion_status.value == "INTEGRATED":
            raise ValueError("Session-012 adapter fixtures cannot assert INTEGRATED checkpoint status")
        preprocessing_version = nonblank(preprocessing_version, field="preprocessing_version")
        postprocessing_version = nonblank(postprocessing_version, field="postprocessing_version")
        self.manifest = manifest
        self.preprocessing_version = preprocessing_version
        self.postprocessing_version = postprocessing_version
        self.preprocessing_hash = sha256_json({"adapter": "tirex2", "preprocessing_version": preprocessing_version})
        self.postprocessing_hash = sha256_json({"adapter": "tirex2", "postprocessing_version": postprocessing_version})
        if manifest.preprocessing_hash != self.preprocessing_hash or manifest.postprocessing_hash != self.postprocessing_hash:
            raise ValueError("TiRex-2 preprocessing/postprocessing manifest hashes mismatch")
        self.inference = inference
        self.artifact_status = "UNVERIFIED" if inference is None else "TESTED"

    def preprocess(self, inputs: CausalModelInputV2) -> TiRexPreparedInputV2:
        if any(feature.available_at_ns > inputs.information_cutoff_ns for feature in inputs.features):
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "TiRex-2 input contains a future-available feature")
        # Known covariates are carried only when already available at the origin.
        return TiRexPreparedInputV2(
            tuple(item.feature_id for item in inputs.features),
            tuple(str(item.value) for item in inputs.features),
            tuple(item.unit for item in inputs.features),
            inputs.information_cutoff_ns,
            self.preprocessing_hash,
        )

    def postprocess(self, outputs: Mapping[str, Any], request: ModelRequestV2, *, input_hash: str) -> AdapterPredictionV2:
        return normalize_quantile_outputs(outputs, request, manifest_hash=self.manifest.manifest_hash, input_hash=input_hash)

    def infer_fixture_or_fail(
        self, inputs: CausalModelInputV2, request: ModelRequestV2
    ) -> AdapterPredictionV2:
        if self.inference is None:
            raise ModelProviderError("MODEL_ARTIFACT_UNAVAILABLE", "TiRex-2 checkpoint/package is unavailable; status UNVERIFIED")
        if request.model_manifest_hash != self.manifest.manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "TiRex-2 request references a different manifest")
        if inputs.information_cutoff_ns > request.information_cutoff_ns:
            raise ModelProviderError("CAUSAL_INPUT_VIOLATION", "TiRex-2 input cutoff exceeds the request information cutoff")
        if request.input_hash != inputs.content_hash:
            raise ModelProviderError("INPUT_HASH_MISMATCH", "TiRex-2 causal input hash does not match request")
        prepared = self.preprocess(inputs)
        result = self.inference(prepared, request)
        return self.postprocess(result, request, input_hash=inputs.content_hash)
