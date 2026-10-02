"""Installed run identity, failure visibility and public-only lifecycle seams."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from atlas.v2 import product
from atlas.v2._serialization import sha256_json
from atlas.v2.data import public_http
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.memory.writer_lock import OpsWriterAlreadyActive
from atlas.v2.resources import resource_file


@pytest.fixture
def build(monkeypatch):
    identity = {"source_sha": "a" * 40, "version": "2.0.0.36", "runtime_lock_sha256": "b" * 64}
    monkeypatch.setattr(product, "build_identity", lambda: dict(identity))
    return identity


def test_manifest_freezes_config_build_and_disabled_authority(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    manifest = product.load_run(run)
    assert manifest["source_sha"] == build["source_sha"]
    assert manifest["capital_enabled"] is manifest["assisted_enabled"] is manifest["holdout_access"] is False
    # A correctly rehashed file still cannot turn on authority or a provider.
    manifest["capital_enabled"] = True
    manifest["content_hash"] = sha256_json({k: v for k, v in manifest.items() if k != "content_hash"})
    (run / "run.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="drift"):
        product.load_run(run)
    newer = dict(build, source_sha="c" * 40)
    monkeypatch.setattr(product, "build_identity", lambda: newer)
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    monkeypatch.setattr(product, "build_identity", lambda: build)
    with pytest.raises(ValueError, match="exact original build"):
        product.load_run(run)


def test_public_profile_rejects_undeclared_provider_and_unbounded_limits():
    with pytest.raises(ValueError, match="unregistered"):
        product.ResearchRunConfigV1(profile="ADAPTIVE")
    with pytest.raises(ValueError, match="provider dispatch"):
        product.ResearchRunConfigV1(provider_profile="FALLBACK")
    with pytest.raises(ValueError, match="report interval"):
        product.ResearchRunConfigV1(report_interval_seconds=True)
    with pytest.raises(ValueError, match="disk reserve"):
        product.ResearchRunConfigV1(minimum_free_disk_bytes=0)


def test_offline_real_composition_reopens_db_new_epoch_and_exports(tmp_path, build):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    assert product.run_component(run, smoke=True) == 0
    first = json.loads((run / "status.json").read_text())
    assert first["reason"] == "OFFLINE_COMPOSITION_FIXTURE_ONLY"
    assert first["live_qualification"] == "TEST GATE"
    assert product.run_component(run, smoke=True) == 0
    second = json.loads((run / "status.json").read_text())
    assert first["epoch_id"] != second["epoch_id"]
    assert len(list((run / "epochs").glob("*-start.json"))) == 2
    assert len(list((run / "epochs").glob("*-stop.json"))) == 2
    reports = list((run / "reports").glob("*/report-*.json"))
    assert reports
    with OpsRepository(run / "ops.sqlite", read_only=True) as reader:
        assert reader.artifact_entries("OpsSupervisorCycleReceiptV1")


def test_duplicate_writer_does_not_replace_existing_status(tmp_path, build):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    state = {"run_id": run.name, "epoch_id": "owner-epoch", "status": "IMPLEMENTED"}
    product._publish(run / "status.json", state)
    with OpsRepository(run / "ops.sqlite"), pytest.raises(OpsWriterAlreadyActive):
        product.run_component(run, smoke=True)
    assert json.loads((run / "status.json").read_text()) == state
    assert not list((run / "epochs").iterdir())


def test_disk_reserve_and_process_stop_persist_explicit_terminal_state(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    monkeypatch.setattr(product.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
    assert product.run_component(run, smoke=True) == 2
    assert json.loads((run / "status.json").read_text())["reason"] == "DISK_RESERVE_EXHAUSTED"
    other = product.create_run(tmp_path, product.ResearchRunConfigV1())
    assert product.run_component(other, smoke=True, stop_requested=lambda: True) == 0
    assert json.loads((other / "status.json").read_text())["reason"] == "PROCESS_STOP_REQUESTED"


def test_report_failure_is_sanitized_and_observable(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())

    def failed_report(path):
        raise RuntimeError("secret-value-provider-response")

    monkeypatch.setattr(product, "export_run", failed_report)
    assert product.run_component(run, smoke=True) == 2
    failure = (run / "report-failure.json").read_text()
    assert "REPORT_UNAVAILABLE" in failure and "RuntimeError" in failure
    assert "secret-value" not in failure


def test_stop_request_binds_exact_epoch_and_launch_omits_ambient_secrets(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    product._publish(run / "status.json", {"run_id": run.name, "epoch_id": "exact-epoch"})
    product.request_stop(run)
    assert json.loads((run / "stop.request").read_text()) == {"run_id": run.name, "epoch_id": "exact-epoch"}
    monkeypatch.setenv("DEEPSEEK_API_KEY", "never-in-child")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "never-in-child-either")
    calls = []
    monkeypatch.setattr(product.subprocess, "Popen", lambda *args, **kwargs: calls.append((args, kwargs)))
    product.launch_run(run)
    command, options = calls[0][0][0], calls[0][1]
    assert "--launch-id" in command
    assert "DEEPSEEK_API_KEY" not in options["env"] and "AWS_SECRET_ACCESS_KEY" not in options["env"]
    assert "never-in-child" not in repr(command)


def test_secret_cipher_is_separate_from_run_and_no_insecure_fallback(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    store = product.WindowsSecretStore(tmp_path / "secrets")
    monkeypatch.setattr(store, "_crypt", lambda raw, decrypt: b"DPAPI-cipher-fixture")
    store.put_provider_key("deepseek-v41-action-critic-v1", "never-in-run")
    assert (tmp_path / "secrets" / "deepseek-action-critic.dpapi").read_bytes() == b"DPAPI-cipher-fixture"
    assert "never-in-run" not in (run / "run.json").read_text()
    if product.os.name != "nt":
        with pytest.raises(RuntimeError, match="unavailable"):
            product.WindowsSecretStore._crypt(b"secret", decrypt=False)


def test_frozen_manifest_is_adjacent_executable_and_resources_cannot_escape(tmp_path, monkeypatch):
    executable = tmp_path / "atlas-product.exe"
    monkeypatch.setattr(product.sys, "frozen", True, raising=False)
    monkeypatch.setattr(product.sys, "executable", str(executable))
    manifest = {"schema_version": 1, "source_sha": "a" * 40, "version": "2.0.0.36",
                "runtime_lock_sha256": "b" * 64, "dependency_locks": {}, "payload_tree_sha256": "c" * 64,
                "python_version": "3.12.10", "runtime_platform": {"system": "Windows", "architecture": "x64", "implementation": "CPython"},
                "capital_enabled": False, "assisted_enabled": False, "payload": ["x" * 20000]}
    (tmp_path / "build-manifest.json").write_text(json.dumps(manifest))
    assert product.build_identity()["source_sha"] == "a" * 40
    assert "payload" not in product.build_identity()
    for unsafe in ("../secret", "/outside"):
        with pytest.raises(ValueError):
            resource_file(unsafe)


def test_public_http_never_consults_ambient_proxy(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://credential-bearing-proxy.invalid")
    calls = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, size):
            return b"{}"

    class Opener:
        def open(self, request, timeout):
            assert request.full_url == "https://api.bybit.com/v5/market/time"
            return Response()

    def build(handler):
        calls.append(handler.proxies)
        return Opener()

    monkeypatch.setattr(public_http, "build_opener", build)
    assert public_http._stdlib_get("https://api.bybit.com/v5/market/time", 1) == (200, b"{}")
    assert calls == [{}]


def test_declared_provider_configuration_is_sealed_and_secret_free(tmp_path, build):
    config = product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1")
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)
    assert manifest["provider_configuration"] == product.provider_configuration(config.provider_profile)
    assert manifest["provider_configuration"] is not None
    manifest["provider_configuration"]["requested_model_id"] = "undeclared-alias"
    manifest["content_hash"] = sha256_json({k: v for k, v in manifest.items() if k != "content_hash"})
    (run / "run.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="provider configuration drift"):
        product.load_run(run)


def test_broker_context_binds_run_epoch_and_has_bounded_protected_bytes(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    epoch = "e" * 32
    path = run / "epochs" / (epoch + "-broker.dpapi")
    context = {"schema_version": 1, "run_id": run.name, "epoch_id": epoch,
               "pipe_name": r"\\.\pipe\atlas-critic-fixture",
               "authentication_key": "a" * 64, "signing_key": "b" * 64}
    path.write_bytes(b"cipher-fixture")
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", lambda raw, decrypt: json.dumps(context).encode())
    assert product._load_broker_context(run, path) == context
    context["run_id"] = "another-run"
    with pytest.raises(ValueError, match="run and epoch"):
        product._load_broker_context(run, path)
    path.write_bytes(b"x" * 65537)
    with pytest.raises(ValueError, match="bound"):
        product._load_broker_context(run, path)


def test_shadow_shutdown_failure_still_stops_credential_process(tmp_path, build):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    waits = []

    class BrokenShadow:
        def close(self):
            raise RuntimeError("closed failure")

    child = SimpleNamespace(wait=lambda timeout: waits.append(timeout))
    runtime = product._InstalledCriticRuntime(child, BrokenShadow(), run, "e" * 32)
    with pytest.raises(RuntimeError, match="closed failure"):
        runtime.close()
    assert waits == [5]
    stop = json.loads((run / "epochs" / ("e" * 32 + "-broker-stop.json")).read_text())
    assert stop == {"run_id": run.name, "epoch_id": "e" * 32}


def test_configured_broker_loss_stops_run_without_fallback(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    closed = []
    critic = SimpleNamespace(process=SimpleNamespace(poll=lambda: 2), shadow=lambda *args: None,
                             close=lambda: closed.append(True))
    monkeypatch.setattr(product, "_start_installed_critic", lambda *args: critic)
    assert product.run_component(run, smoke=True) == 2
    status = json.loads((run / "status.json").read_text())
    assert status["reason"] == "CONFIGURED_PROVIDER_BROKER_LOST"
    assert status["provider_health"] == "TEST GATE" and closed == [True]
