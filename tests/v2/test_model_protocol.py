from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap
from atlas.v2.contracts import ForecastStatusV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.models.protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2, PromotionStatusV2

H = "a" * 64
H2 = "b" * 64


def key() -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", "r1")


def manifest() -> ModelManifestV2:
    return ModelManifestV2(
        "provider", "https://example.invalid/repo", "commit", "checkpoint", "revision", (H,),
        "code-license", "weight-license", "RESEARCH_ONLY", H2, H, H2, "cpu", "fp32",
        ("features.v1",), ("return.quantiles",), 4096, "UNKNOWN", PromotionStatusV2.INTEGRATED,
    )


def request() -> ModelRequestV2:
    return ModelRequestV2.build(
        input_artifact_refs=(H,), input_hash=H2, instrument_key=key(), policy_context_ref="policy-ref",
        model_manifest_hash=manifest().manifest_hash, information_cutoff_ns=100,
        requested_targets=("net_return",), requested_horizons=(60,),
        requested_quantiles=(Decimal("0.05"), Decimal("0.5"), Decimal("0.95")),
        deadline_ns=200, seed=4, resource_budget={"memory_mb": 512, "latency_ms": 1000},
    )


def test_manifest_is_content_addressed_and_promotion_vocabulary_is_exact() -> None:
    first = manifest()
    assert first.manifest_hash == manifest().manifest_hash
    assert ModelManifestV2.from_dict(first.to_dict()).manifest_hash == first.manifest_hash
    assert {item.value for item in PromotionStatusV2} == {
        "INTEGRATED", "ENGINEERING_PASS", "HISTORICAL_DIAGNOSTIC", "PROSPECTIVE_SHADOW",
        "INCREMENTAL_VALUE_PASS", "DECISION_ELIGIBLE",
    }
    with pytest.raises(ValueError, match="unknown fields"):
        ModelManifestV2.from_dict({**first.to_dict(), "worker": "launch"})


def test_request_deterministic_identity_and_temporal_constraints() -> None:
    first = request()
    second = request()
    assert first.request_id == second.request_id
    assert ModelRequestV2.from_dict(first.to_dict()) == first
    decimal_budget = ModelRequestV2.build(
        input_artifact_refs=(H,), input_hash=H2, instrument_key=key(), policy_context_ref="policy-ref",
        model_manifest_hash=manifest().manifest_hash, information_cutoff_ns=100,
        requested_targets=("net_return",), requested_horizons=(60,), requested_quantiles=(Decimal("0.5"),),
        deadline_ns=200, seed=4, resource_budget={"accelerator_gb": Decimal("1.5")},
    )
    assert ModelRequestV2.from_dict(decimal_budget.to_dict()) == decimal_budget
    with pytest.raises(ValueError, match="deadline_ns"):
        ModelRequestV2.build(
            input_artifact_refs=(H,), input_hash=H2, instrument_key=key(), policy_context_ref="p",
            model_manifest_hash=manifest().manifest_hash, information_cutoff_ns=100, requested_targets=("x",),
            requested_horizons=(1,), requested_quantiles=(Decimal("0.5"),), deadline_ns=100, seed=0, resource_budget={},
        )
    with pytest.raises(ValueError, match="unknown fields"):
        ModelRequestV2.from_dict({**first.to_dict(), "future_covariate": 9})


def test_late_forecast_is_archived_but_unusable_and_missingness_is_explicit() -> None:
    artifact = ForecastArtifactV2(
        request().request_id, manifest().manifest_hash, H2, 110, 180, 220, 200,
        ("net_return",), (60,), (Decimal("0.05"), Decimal("0.5")), "values-ref", None,
        ("net_return.q95",), FrozenMap({"net_return": "fraction"}), FrozenMap({"elapsed_ms": 110}), ForecastStatusV2.PARTIAL,
    )
    assert artifact.received_ns > artifact.expires_ns
    assert not artifact.is_usable_at(190)
    assert not artifact.is_usable_at(201)
    assert artifact.missing_outputs == ("net_return.q95",)
    assert ForecastArtifactV2.from_dict(artifact.to_dict()).content_hash == artifact.content_hash
    complete = ForecastArtifactV2(
        request().request_id, manifest().manifest_hash, H2, 110, 150, 160, 200,
        ("net_return",), (60,), (Decimal("0.5"),), "values-ref", None,
        (), FrozenMap({"net_return": "fraction"}), FrozenMap(), ForecastStatusV2.AVAILABLE,
    )
    assert complete.is_usable_at(170)
    assert not complete.is_usable_at(155)
    with pytest.raises(ValueError, match="started <= completed <= received"):
        ForecastArtifactV2(
            "req", H, H2, 111, 110, 120, 200, (), (), (), None, None, (), FrozenMap(), FrozenMap(), ForecastStatusV2.AVAILABLE,
        )
