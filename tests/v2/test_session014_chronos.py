"""Chronos-2 adapter contract fixtures; no checkpoint is loaded."""

from __future__ import annotations

import ast
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.models.adapters.chronos2 import Chronos2Adapter
from atlas.v2.models.adapters.common import CausalFeatureInputV2, CausalModelInputV2
from atlas.v2.models.protocol import ModelManifestV2, ModelRequestV2, PromotionStatusV2
from atlas.v2.models.worker_protocol import ModelProviderError

from .test_model_runtime import key

WEIGHT = "d" * 64
TOKENIZER = "e" * 64


def fixture() -> tuple[Chronos2Adapter, ModelManifestV2, CausalModelInputV2]:
    pre = sha256_json({"adapter": "chronos2", "version": "CHRONOS2_PRE_V1"})
    post = sha256_json({"adapter": "chronos2", "version": "CHRONOS2_POST_V1"})
    manifest = ModelManifestV2("chronos2-fixture", "fixture://chronos2", "source-revision-v1",
        "fixture-checkpoint", "checkpoint-revision-v1", (WEIGHT,), "code-license-fixture", "weight-license-fixture",
        "RESEARCH_ONLY", pre, post, "a" * 64, "cpu", "fp32", ("causal-prefix-v1",), ("log_return",),
        128, "UNKNOWN", PromotionStatusV2.INTEGRATED, "fixture-tokenizer", "tokenizer-revision-v1", (TOKENIZER,))
    adapter = Chronos2Adapter(manifest, expected_manifest_hash=manifest.manifest_hash,
        expected_weight_sha256=(WEIGHT,), expected_tokenizer_sha256=(TOKENIZER,))
    inputs = CausalModelInputV2(100, (CausalFeatureInputV2("calendar_hour", Decimal(1), 99, "hour", True),
                                     CausalFeatureInputV2("close", Decimal(100), 100, "price")))
    return adapter, manifest, inputs


def request(manifest: ModelManifestV2, inputs: CausalModelInputV2, *, target: str = "log_return",
            quantile: str = "0.5") -> ModelRequestV2:
    return ModelRequestV2.build(input_artifact_refs=("a" * 64,), input_hash=inputs.content_hash,
        instrument_key=key(), policy_context_ref="fixture-policy", model_manifest_hash=manifest.manifest_hash,
        information_cutoff_ns=100, requested_targets=(target,), requested_horizons=(900_000_000_000,),
        requested_quantiles=(Decimal(quantile),), deadline_ns=1_000_000_000_000, seed=1,
        resource_budget={"latency_ms": 1000})


def test_chronos_manifest_preprocess_and_fixture_output() -> None:
    adapter, manifest, inputs = fixture()
    prepared = adapter.preprocess(inputs, request(manifest, inputs))
    assert prepared.preprocessing_hash == manifest.preprocessing_hash
    assert prepared.known_covariates == (("calendar_hour", "1"),)
    output_key = "log_return:900000000000:q0.5"
    prediction = adapter.postprocess_fixture({output_key: "0.01"}, request(manifest, inputs), input_hash=inputs.content_hash)
    assert prediction.values[output_key] == Decimal("0.01") and prediction.missing_outputs == ()
    assert adapter.artifact_status == "UNVERIFIED" and adapter.decision_influence == 0
    assert adapter.postprocess_fixture({}, request(manifest, inputs), input_hash=inputs.content_hash).missing_outputs == (output_key,)
    with pytest.raises(ModelProviderError, match="unrequested"):
        adapter.postprocess_fixture({"unrequested": 1}, request(manifest, inputs), input_hash=inputs.content_hash)


def test_chronos_rejects_wrong_manifest_output_and_future_realized() -> None:
    adapter, manifest, inputs = fixture()
    with pytest.raises(ValueError, match="manifest"):
        Chronos2Adapter(manifest, expected_manifest_hash="b" * 64, expected_weight_sha256=(WEIGHT,), expected_tokenizer_sha256=(TOKENIZER,))
    with pytest.raises(ValueError, match="tokenizer"):
        Chronos2Adapter(manifest, expected_manifest_hash=manifest.manifest_hash, expected_weight_sha256=(WEIGHT,), expected_tokenizer_sha256=("b" * 64,))
    with pytest.raises(ValueError, match="preprocessing"):
        Chronos2Adapter(replace(manifest, preprocessing_hash="b" * 64),
            expected_manifest_hash=replace(manifest, preprocessing_hash="b" * 64).manifest_hash,
            expected_weight_sha256=(WEIGHT,), expected_tokenizer_sha256=(TOKENIZER,))
    with pytest.raises(ModelProviderError, match="unsupported"):
        adapter.preprocess(inputs, request(manifest, inputs, target="unknown_target"))
    with pytest.raises(ModelProviderError, match="quantile"):
        adapter.preprocess(inputs, request(manifest, inputs, quantile="0.1"))
    future = CausalModelInputV2(100, (CausalFeatureInputV2("future_funding", Decimal(1), 100, "rate", True),))
    with pytest.raises(ModelProviderError, match="known covariates"):
        adapter.preprocess(future, request(manifest, future))
    late = CausalModelInputV2(101, (CausalFeatureInputV2("close", Decimal(100), 101, "price"),))
    with pytest.raises(ModelProviderError, match="cutoff"):
        adapter.preprocess(late, request(manifest, late))
    with pytest.raises(ModelProviderError, match="isolated"):
        adapter.infer_isolated(object(), inputs, request(manifest, inputs), started_at_ns=100)  # type: ignore[arg-type]


def test_chronos_boundary_has_no_heavy_runtime_import() -> None:
    source = Path(__file__).resolve().parents[2] / "src/atlas/v2/models/adapters/chronos2.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported.update(name.name for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names)
    assert not any(name and name.split(".")[0] in {"torch", "chronos", "transformers", "tensorflow"} for name in imported)
