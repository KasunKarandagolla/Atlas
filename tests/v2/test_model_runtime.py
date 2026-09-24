from __future__ import annotations

import ast
import json
import os
import time
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ForecastStatusV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.models.adapters.common import CausalFeatureInputV2, CausalModelInputV2
from atlas.v2.models.adapters.kronos_mini import KronosMiniAdapter, OHLCVRowV2
from atlas.v2.models.adapters.tirex2 import TiRex2Adapter
from atlas.v2.models.baseline import BaselineInputsV2, CausalCloseV2, StatisticalBaselineV2
from atlas.v2.models.local_process import LocalProcessProvider
from atlas.v2.models.protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2, PromotionStatusV2
from atlas.v2.models.provider import DeterministicFakeProvider, ForecastArchiveV2, ModelArenaV2, ModelProvider
from atlas.v2.models.remote_http import RemoteHTTPProvider
from atlas.v2.models.worker_protocol import ModelProviderError, ProviderStateV2, WorkerRequestV2, strict_worker_response

H = "a" * 64
H2 = "b" * 64
H3 = "c" * 64
WEIGHT = "d" * 64
TOKENIZER = "e" * 64
NS = 1_000_000_000


def key() -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                           "BTCUSDT", "BTC", "USDT", "USDT", H)


def manifest(*, preprocessing: str = H2, postprocessing: str = H3,
             tokenizer: bool = False, status: PromotionStatusV2 = PromotionStatusV2.ENGINEERING_PASS) -> ModelManifestV2:
    return ModelManifestV2(
        "fixture-provider", "fixture://model", "commit-fixture", "checkpoint-exact-fixture", "rev-1",
        (WEIGHT,), "code-license-ref", "weight-license-ref", "RESEARCH_ONLY", preprocessing, postprocessing,
        H, "cpu", "fp32", ("causal-bars.v1",), ("log-return-quantiles.v1",), 4096,
        "UNKNOWN", status,
        "fixture-tokenizer" if tokenizer else None,
        "revision-1" if tokenizer else None,
        (TOKENIZER,) if tokenizer else (),
    )


def request(model: ModelManifestV2, *, now_ns: int = 100, input_hash: str = H2,
            cutoff: int | None = None, deadline: int | None = None,
            targets: tuple[str, ...] = ("log_return",), horizons: tuple[int, ...] = (900 * NS,),
            policy_context_ref: str = "policy-context-v1") -> ModelRequestV2:
    cutoff = now_ns - 1 if cutoff is None else cutoff
    deadline = now_ns + 100 * NS if deadline is None else deadline
    return ModelRequestV2.build(
        input_artifact_refs=(H,), input_hash=input_hash, instrument_key=key(), policy_context_ref=policy_context_ref,
        model_manifest_hash=model.manifest_hash, information_cutoff_ns=cutoff,
        requested_targets=targets, requested_horizons=horizons, requested_quantiles=(Decimal("0.5"),),
        deadline_ns=deadline, seed=7, resource_budget={"latency_ms": 1000, "memory_mb": 256},
    )


def test_arena_idempotency_queue_bound_stale_and_original_retry_deadline() -> None:
    model = manifest()
    now = [10]

    def clock() -> int:
        return now[0]

    request_a = request(model, now_ns=10, cutoff=1, deadline=20)
    arena = ModelArenaV2(max_queue_size=1, clock_ns=clock)
    payload = {"closes": [100, 101]}
    assert arena.enqueue(request_a, model, payload, provider_key="fixture") == request_a.request_id
    assert arena.enqueue(request_a, model, payload, provider_key="fixture") == request_a.request_id
    request_b = request(model, now_ns=10, cutoff=1, deadline=30, input_hash=H3)
    with pytest.raises(ModelProviderError, match="queue is full"):
        arena.enqueue(request_b, model, {"other": True}, provider_key="fixture")
    now[0] = 20
    provider = DeterministicFakeProvider(clock_ns=clock)
    stale = arena.run_next(provider, provider_key="fixture")
    assert stale is not None and stale.state == ProviderStateV2.STALE_DISCARDED and not provider.calls

    now[0] = 10
    retry_arena = ModelArenaV2(max_queue_size=1, clock_ns=clock)
    retry_request = request(model, now_ns=10, cutoff=1, deadline=20)
    retry_arena.enqueue(retry_request, model, payload, provider_key="retry")

    class RetryOnce(ModelProvider):
        def __init__(self) -> None:
            self.calls = 0

        def infer(self, *args, **kwargs):
            self.calls += 1
            raise ModelProviderError("TRANSIENT", "fixture transport failure", retryable=True)

    retry_provider = RetryOnce()
    result = retry_arena.run_next(
        retry_provider, provider_key="retry", retries=3, retry_wait=lambda attempt: now.__setitem__(0, 20)
    )
    assert result is not None and result.failure_code == "RETRY_DEADLINE_EXPIRED"
    assert retry_provider.calls == 1 and request_a.deadline_ns == 20


def test_late_response_is_archived_but_unusable_and_hash_mismatch_fails_closed() -> None:
    model = manifest()
    now = [10]
    req = request(model, now_ns=10, cutoff=1, deadline=30)
    arena = ModelArenaV2(max_queue_size=2, clock_ns=lambda: now[0])
    arena.enqueue(req, model, {"bars": [1]}, provider_key="fake")
    late = arena.run_next(
        DeterministicFakeProvider(clock_ns=lambda: now[0], receive_lag_ns=21), provider_key="fake"
    )
    assert late is not None and not late.usable and late.failure_code == "LATE_OR_EXPIRED"
    assert arena.archive.get(req.request_id) == late.artifact

    now[0] = 10
    req2 = request(model, now_ns=10, cutoff=1, deadline=30, input_hash=H3)
    arena.enqueue(req2, model, {"bars": [2]}, provider_key="bad-hash")

    class WrongInput(DeterministicFakeProvider):
        def infer(self, request_value, manifest_value, inputs, *, started_at_ns):
            correct = super().infer(request_value, manifest_value, inputs, started_at_ns=started_at_ns)
            return ForecastArtifactV2(
                correct.request_id, correct.model_manifest_hash, H2, correct.inference_started_ns,
                correct.completed_ns, correct.received_ns, correct.expires_ns, correct.targets, correct.horizons,
                correct.native_quantiles, correct.values_ref, correct.samples_ref, correct.missing_outputs,
                correct.units, correct.resource_metrics, correct.status,
            )

    mismatch = arena.run_next(WrongInput(clock_ns=lambda: now[0]), provider_key="bad-hash")
    assert mismatch is not None and mismatch.failure_code == "INPUT_HASH_MISMATCH"
    assert arena.archive.get(req2.request_id) is None

    unavailable_arena = ModelArenaV2(max_queue_size=1, clock_ns=lambda: now[0])
    unavailable_arena.enqueue(req2, model, {"bars": [2]}, provider_key="missing-model")
    unavailable = unavailable_arena.run_next(DeterministicFakeProvider(clock_ns=lambda: now[0]), provider_key="unavailable")
    assert unavailable is not None and unavailable.failure_code == "PROVIDER_UNAVAILABLE"


def test_arena_rejects_unrequested_forecast_dimensions() -> None:
    model = manifest()
    req = request(model, now_ns=10, cutoff=1, deadline=30)
    arena = ModelArenaV2(max_queue_size=1, clock_ns=lambda: 10)
    arena.enqueue(req, model, {"bars": []}, provider_key="wrong-dimension")

    class WrongHorizon(DeterministicFakeProvider):
        def infer(self, request_value, manifest_value, inputs, *, started_at_ns):
            correct = super().infer(request_value, manifest_value, inputs, started_at_ns=started_at_ns)
            return ForecastArtifactV2(
                correct.request_id, correct.model_manifest_hash, correct.input_hash,
                correct.inference_started_ns, correct.completed_ns, correct.received_ns, correct.expires_ns,
                correct.targets, (1_800 * NS,), correct.native_quantiles, correct.values_ref, correct.samples_ref,
                correct.missing_outputs, correct.units, correct.resource_metrics, correct.status,
            )

    result = arena.run_next(WrongHorizon(clock_ns=lambda: 10), provider_key="wrong-dimension")
    assert result is not None and result.failure_code == "HORIZONS_MISMATCH"
    assert arena.archive.get(req.request_id) is None


def test_late_forecast_archive_survives_provider_process_restart(tmp_path) -> None:
    model = manifest()
    now = [10]
    req = request(model, now_ns=10, cutoff=1, deadline=30)
    archive_path = tmp_path / "forecasts"
    first_archive = ForecastArchiveV2(archive_path)
    arena = ModelArenaV2(max_queue_size=1, clock_ns=lambda: now[0], archive=first_archive)
    arena.enqueue(req, model, {"features": []}, provider_key="late")
    result = arena.run_next(
        DeterministicFakeProvider(clock_ns=lambda: now[0], receive_lag_ns=21), provider_key="late"
    )
    assert result is not None and not result.usable and result.artifact is not None
    restored = ForecastArchiveV2(archive_path)
    assert restored.get(req.request_id) == result.artifact


def test_worker_protocol_rejects_unknown_schema_and_sensitive_path_fields() -> None:
    model = manifest()
    req = request(model)
    for sensitive_field in ("v1_live_control_db_path", "liveControlDbPath", "riskPolicy", "orderApi"):
        with pytest.raises(ValueError, match="sensitive field"):
            WorkerRequestV2(req, model, {"features": [1], sensitive_field: "/tmp/live.sqlite"})
    with pytest.raises(ValueError, match="filesystem path data"):
        WorkerRequestV2(req, model, {"artifact_data": {"context": "/tmp/live.sqlite"}})
    with pytest.raises(ValueError, match="filesystem path data"):
        WorkerRequestV2(request(model, policy_context_ref="/tmp/live.sqlite"), model, {"features": []})
    artifact = DeterministicFakeProvider(clock_ns=lambda: 100).infer(req, model, {}, started_at_ns=100)
    with pytest.raises(ValueError, match="schema_version"):
        strict_worker_response({**artifact.to_dict(), "schema_version": 77})
    assert WorkerRequestV2(req, model, {"features": [1], "input_units": "fraction"}).request == req


def test_local_worker_allowlisted_environment_success_crash_timeout_and_no_runtime_authority() -> None:
    model = manifest()
    now = time.time_ns()
    req = request(model, now_ns=now, cutoff=now - 1, deadline=now + 10 * NS)
    env = LocalProcessProvider.allowlisted_environment({
        "PATH": os.environ.get("PATH", "/usr/bin"),
        "BYBIT_API_KEY": "redacted-test-value", "BINANCE_API_SECRET": "redacted-test-value",
        "ATLAS_LIVE_CONTROL_DB": "/tmp/v1.sqlite", "ATLAS_ACCOUNT_ID": "test-account",
        "FUTURE_PRIVATE_TOKEN": "redacted-test-value",
    })
    assert set(env) <= {"PATH", "LANG", "LC_ALL", "TZ", "SYSTEMROOT", "WINDIR", "TMPDIR", "TEMP", "TMP", "PYTHONIOENCODING"}
    probe = LocalProcessProvider.probe_environment_keys({
        "PATH": os.environ.get("PATH", "/usr/bin"), "BYBIT_API_KEY": "should-not-pass",
        "ATLAS_LIVE_CONTROL_DB": "/tmp/v1.sqlite", "ATLAS_ACCOUNT_ID": "should-not-pass",
    })
    assert "BYBIT_API_KEY" not in probe and "ATLAS_LIVE_CONTROL_DB" not in probe
    provider = LocalProcessProvider()
    artifact = provider.infer(req, model, {"causal_features": ["close_return"]}, started_at_ns=now)
    assert artifact.model_manifest_hash == model.manifest_hash and artifact.input_hash == req.input_hash

    with pytest.raises(ModelProviderError, match="status 42"):
        LocalProcessProvider(worker_program="raise SystemExit(42)").infer(
            req, model, {"causal_features": []}, started_at_ns=now
        )
    with pytest.raises(ModelProviderError, match="exceeded"):
        LocalProcessProvider(timeout_s=0.05, worker_program="import time;time.sleep(1)").infer(
            req, model, {"causal_features": []}, started_at_ns=now
        )
    with pytest.raises(ModelProviderError, match="output exceeded"):
        LocalProcessProvider(max_output_bytes=100, worker_program="import sys;sys.stdout.write('x'*1000)").infer(
            req, model, {"causal_features": []}, started_at_ns=now
        )


def test_remote_provider_deadline_timeout_request_identity_and_schema_checks() -> None:
    model = manifest()
    now = [10]
    req = request(model, now_ns=10, cutoff=1, deadline=30)
    generated = DeterministicFakeProvider(clock_ns=lambda: now[0]).infer(
        req, model, {"features": []}, started_at_ns=10
    )

    def poster(url, body, timeout_s, headers):
        assert url == "https://worker.example/infer"
        assert timeout_s <= 3.0
        assert headers["Idempotency-Key"] == req.request_id
        assert not any("auth" in name.lower() or "token" in name.lower() for name in headers)
        wire = json.loads(body)
        assert wire["request"]["request_id"] == req.request_id
        return 200, json.dumps(generated.to_dict()).encode()

    remote = RemoteHTTPProvider("https://worker.example/infer", clock_ns=lambda: now[0], poster=poster)
    response = remote.infer(req, model, {"features": []}, started_at_ns=10)
    assert response.received_ns == 10

    def timeout_poster(*args, **kwargs):
        raise ModelProviderError("REMOTE_TIMEOUT", "fixture timed out", retryable=True)

    with pytest.raises(ModelProviderError, match="timed out"):
        RemoteHTTPProvider("https://worker.example/infer", clock_ns=lambda: now[0], poster=timeout_poster).infer(
            req, model, {"features": []}, started_at_ns=10
        )
    with pytest.raises(ValueError, match="HTTPS"):
        RemoteHTTPProvider("http://worker.example/infer")

    with pytest.raises(ModelProviderError, match="manifest hash mismatch"):
        RemoteHTTPProvider("https://worker.example/infer", clock_ns=lambda: now[0], poster=poster).infer(
            req, manifest(preprocessing=H3), {"features": []}, started_at_ns=10
        )

    wrong_input = ForecastArtifactV2(
        generated.request_id, generated.model_manifest_hash, H3, generated.inference_started_ns,
        generated.completed_ns, generated.received_ns, generated.expires_ns, generated.targets,
        generated.horizons, generated.native_quantiles, generated.values_ref, generated.samples_ref,
        generated.missing_outputs, generated.units, generated.resource_metrics, generated.status,
    )
    bad_input_remote = RemoteHTTPProvider(
        "https://worker.example/infer", clock_ns=lambda: now[0],
        poster=lambda url, body, timeout_s, headers: (200, json.dumps(wrong_input.to_dict()).encode()),
    )
    with pytest.raises(ModelProviderError, match="input hash mismatch"):
        bad_input_remote.infer(req, model, {"features": []}, started_at_ns=10)

    wrong_request = ForecastArtifactV2(
        "req_wrong_worker_response", generated.model_manifest_hash, generated.input_hash,
        generated.inference_started_ns, generated.completed_ns, generated.received_ns, generated.expires_ns,
        generated.targets, generated.horizons, generated.native_quantiles, generated.values_ref,
        generated.samples_ref, generated.missing_outputs, generated.units, generated.resource_metrics,
        generated.status,
    )
    bad_request_remote = RemoteHTTPProvider(
        "https://worker.example/infer", clock_ns=lambda: now[0],
        poster=lambda url, body, timeout_s, headers: (200, json.dumps(wrong_request.to_dict()).encode()),
    )
    with pytest.raises(ModelProviderError, match="request_id mismatch"):
        bad_request_remote.infer(req, model, {"features": []}, started_at_ns=10)

    malformed_remote = RemoteHTTPProvider(
        "https://worker.example/infer", clock_ns=lambda: now[0],
        poster=lambda url, body, timeout_s, headers: (200, b'{"schema_version": 99}'),
    )
    with pytest.raises(ModelProviderError, match="unknown schema"):
        malformed_remote.infer(req, model, {"features": []}, started_at_ns=10)


def test_statistical_baseline_is_deterministic_causal_and_explicit_about_missing_outputs() -> None:
    model = manifest()
    step = 900 * NS
    closes = tuple(CausalCloseV2(i * step, i * step + 1, Decimal(str(100 + i))) for i in range(4))
    inputs = BaselineInputsV2(key(), information_cutoff_ns=4 * step, closes=closes)
    req = request(model, now_ns=4 * step + 10, input_hash=inputs.content_hash, cutoff=4 * step,
                  deadline=4 * step + 1000, horizons=(step, 3_600 * NS))
    baseline = StatisticalBaselineV2()
    first = baseline.predict(req, model, inputs, completed_at_ns=4 * step + 20)
    second = baseline.predict(req, model, inputs, completed_at_ns=4 * step + 20)
    assert first.artifact.content_hash == second.artifact.content_hash
    assert first.values == second.values
    assert any("INSUFFICIENT_CAUSAL_HISTORY" in item for item in first.artifact.missing_outputs)
    assert first.artifact.status == ForecastStatusV2.PARTIAL
    assert BaselineInputsV2.from_dict(inputs.to_dict()) == inputs
    with pytest.raises(ValueError, match="schema_version"):
        BaselineInputsV2.from_dict({**inputs.to_dict(), "schema_version": 88})
    malformed_inputs = inputs.to_dict()
    malformed_inputs["closes"][0]["close"] = 100  # type: ignore[index]
    with pytest.raises(ValueError, match="wire value must be a canonical decimal string"):
        BaselineInputsV2.from_dict(malformed_inputs)
    arena = ModelArenaV2(max_queue_size=1, clock_ns=lambda: 4 * step + 20)
    arena.enqueue(req, model, inputs.to_dict(), provider_key="statistical-baseline")
    runtime_result = arena.run_next(
        StatisticalBaselineV2(clock_ns=lambda: 4 * step + 20), provider_key="statistical-baseline"
    )
    assert runtime_result is not None and runtime_result.artifact == first.artifact
    with pytest.raises(ValueError, match="information cutoff"):
        future = BaselineInputsV2(key(), information_cutoff_ns=4 * step + 1,
                                  closes=closes + (CausalCloseV2(4 * step + 1, 4 * step + 2, Decimal("105")),))
        baseline.predict(req, model, future, completed_at_ns=4 * step + 20)


def test_tirex_and_kronos_fixture_boundaries_and_unavailable_artifacts_are_honest() -> None:
    preprocessing_version = "tirex-pre-v1"
    postprocessing_version = "tirex-post-v1"
    tirex_pre = sha256_json({"adapter": "tirex2", "preprocessing_version": preprocessing_version})
    tirex_post = sha256_json({"adapter": "tirex2", "postprocessing_version": postprocessing_version})
    tirex_manifest = manifest(preprocessing=tirex_pre, postprocessing=tirex_post)
    feature_input = CausalModelInputV2(100, (CausalFeatureInputV2("return", Decimal("0.01"), 99, "fraction"),))
    feature_req = request(tirex_manifest, input_hash=feature_input.content_hash, cutoff=100, deadline=200)
    tirex = TiRex2Adapter(
        tirex_manifest, expected_manifest_hash=tirex_manifest.manifest_hash,
        preprocessing_version=preprocessing_version, postprocessing_version=postprocessing_version,
        inference=lambda prepared, req: {"log_return:900000000000:q0.5": "0.02"},
    )
    prediction = tirex.infer_fixture_or_fail(feature_input, feature_req)
    assert prediction.values["log_return:900000000000:q0.5"] == Decimal("0.02")
    with pytest.raises(ModelProviderError, match="unrequested output keys"):
        tirex.postprocess(
            {"log_return:900000000000:q0.5": "0.02", "future_target:900000000000:q0.5": "1"},
            feature_req,
            input_hash=feature_input.content_hash,
        )
    with pytest.raises(ModelProviderError, match="unavailable at origin"):
        tirex.preprocess(CausalModelInputV2(100, (CausalFeatureInputV2("return", Decimal("0.01"), 101, "fraction"),)))
    unavailable_tirex = TiRex2Adapter(
        tirex_manifest, expected_manifest_hash=tirex_manifest.manifest_hash,
        preprocessing_version=preprocessing_version, postprocessing_version=postprocessing_version,
    )
    assert unavailable_tirex.artifact_status == "UNVERIFIED"
    with pytest.raises(ModelProviderError, match="unavailable"):
        unavailable_tirex.infer_fixture_or_fail(feature_input, feature_req)

    pre_version, post_version, sample_version = "kronos-pre-v1", "kronos-post-v1", "sampling-v1"
    kronos_pre = sha256_json({"adapter": "kronos-mini", "preprocessing_version": pre_version,
                              "sampling_version": sample_version})
    kronos_post = sha256_json({"adapter": "kronos-mini", "postprocessing_version": post_version})
    kronos_manifest = manifest(preprocessing=kronos_pre, postprocessing=kronos_post, tokenizer=True)
    ohlcv = (OHLCVRowV2(90, 91, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"), Decimal("5")),)
    kronos = KronosMiniAdapter(
        kronos_manifest, expected_manifest_hash=kronos_manifest.manifest_hash,
        preprocessing_version=pre_version, postprocessing_version=post_version, sampling_version=sample_version,
        inference=lambda prepared, req: {"log_return:900000000000:q0.5": "0.03"},
    )
    prepared = kronos.preprocess(ohlcv, information_cutoff_ns=100)
    input_hash = sha256_json({"rows": prepared.rows, "unit": prepared.input_unit,
                              "transform": prepared.price_transform, "cutoff": 100})
    kronos_req = request(kronos_manifest, input_hash=input_hash, cutoff=100, deadline=200)
    assert kronos.infer_fixture_or_fail(ohlcv, information_cutoff_ns=100, request=kronos_req).values[
        "log_return:900000000000:q0.5"
    ] == Decimal("0.03")
    with pytest.raises(ValueError, match="OHLC invariants"):
        OHLCVRowV2(90, 91, Decimal("100"), Decimal("98"), Decimal("99"), Decimal("101"), Decimal("5"))
    with pytest.raises(ModelProviderError, match="unavailable at origin"):
        kronos.preprocess((OHLCVRowV2(110, 111, Decimal("100"), Decimal("102"), Decimal("99"),
                                      Decimal("101"), Decimal("5")),), information_cutoff_ns=100)


def test_models_package_has_no_execution_runtime_or_heavy_model_imports() -> None:
    root = Path(__file__).resolve().parents[2] / "src/atlas/v2/models"
    forbidden = {"atlas.execution", "atlas.runtime", "torch", "tirex", "kronos", "transformers"}
    for source in root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                imports.add(node.module)
            elif isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
        assert not any(any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden) for name in imports)
