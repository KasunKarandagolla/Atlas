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


def test_read_only_export_preserves_original_identity_after_upgrade(tmp_path, build, monkeypatch):
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    with OpsRepository(run / "ops.sqlite"):
        pass
    original = (run / "run.json").read_bytes()
    monkeypatch.setattr(product, "build_identity", lambda: dict(build, source_sha="c" * 40))
    with pytest.raises(ValueError, match="exact original build"):
        product.load_run(run)
    assert product.load_run(run, require_current_build=False)["source_sha"] == "a" * 40
    result = product.export_run(run)
    assert result["report"]["status"] in {"TESTED", "TEST GATE", "IMPLEMENTED", "NOT ESTIMABLE"}
    assert (run / "run.json").read_bytes() == original
    manifest = json.loads(original)
    manifest["source_sha"] = "d" * 40
    manifest["content_hash"] = sha256_json({k: v for k, v in manifest.items() if k != "content_hash"})
    (run / "run.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="model configuration drift"):
        product.load_run(run, require_current_build=False)


def test_run_selector_selects_new_run_and_preserves_explicit_selection(tmp_path, build, monkeypatch):
    class Selector:
        def __init__(self):
            self.items = []
            self.index = -1

        def currentData(self):
            return self.items[self.index][1] if self.index >= 0 else None

        def clear(self):
            self.items, self.index = [], -1

        def addItem(self, label, data):
            self.items.append((label, data))
            if self.index == -1:
                self.index = 0

        def findData(self, data):
            return next((i for i, item in enumerate(self.items) if item[1] == data), -1)

        def setCurrentIndex(self, index):
            self.index = index

    selector = Selector()
    first = product.create_run(tmp_path, product.ResearchRunConfigV1())
    product._refresh_run_selector(selector, tmp_path, preferred_run=first)
    second = product.create_run(tmp_path, product.ResearchRunConfigV1())
    product._refresh_run_selector(selector, tmp_path, preferred_run=second)
    assert selector.currentData() == str(second)
    selector.setCurrentIndex(selector.findData(str(first)))
    monkeypatch.setattr(product, "build_identity", lambda: dict(build, source_sha="c" * 40))
    product._refresh_run_selector(selector, tmp_path)
    assert selector.currentData() == str(first)
    assert len(selector.items) == 2


@pytest.mark.parametrize("heartbeat,stopped,reason", [
    (99_000_000_000, False, "READY"),
    (60_000_000_000, False, "RUNTIME_HEARTBEAT_STALE"),
    (101_000_000_000, False, "RUNTIME_CLOCK_REGRESSION"),
    (None, False, "RUNTIME_HEARTBEAT_UNAVAILABLE"),
    (60_000_000_000, True, "READY"),
])
def test_runtime_status_exposes_stale_missing_and_future_heartbeats(heartbeat, stopped, reason):
    state = {"run_id": "run", "observed_at_ns": heartbeat, "status": "IMPLEMENTED", "reason": "READY"}
    if stopped:
        state["stopped_at_ns"] = heartbeat
    text = product._runtime_status_text(state, now_ns=100_000_000_000)
    assert f"Reason: {reason}" in text
    assert ("Runtime: TEST GATE" in text) == (reason != "READY")
    assert "Nones" not in text and "-1s" not in text


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
    context = {"schema_version": 2, "run_id": run.name, "epoch_id": epoch,
               "owner_process": {"pid": 123, "creation_filetime_ticks": 456},
               "pipe_name": r"\\.\pipe\AtlasCritic-12345678-1234-1234-1234-123456789abc",
               "authentication_key": "a" * 64, "signing_key": "b" * 64}
    path.write_bytes(b"cipher-fixture")
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", lambda raw, decrypt: json.dumps(context).encode())
    assert product._load_broker_context(run, path) == context
    context["owner_process"]["pid"] = True
    with pytest.raises(ValueError, match="owner or endpoint"):
        product._load_broker_context(run, path)
    context["owner_process"]["pid"] = 123
    context["pipe_name"] = r"\\.\pipe\atlas-critic-wrong-format"
    with pytest.raises(ValueError, match="owner or endpoint"):
        product._load_broker_context(run, path)
    context["pipe_name"] = r"\\.\pipe\AtlasCritic-12345678-1234-1234-1234-123456789abc"
    context["schema_version"] = 1
    with pytest.raises(ValueError, match="context contract"):
        product._load_broker_context(run, path)
    context["schema_version"] = 2
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


def test_broker_binds_live_controller_before_accessing_secret(tmp_path, build, monkeypatch):
    from atlas.v2.agent_intelligence import windows_broker

    run = product.create_run(tmp_path, product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    context = {"owner_process": {"pid": 123, "creation_filetime_ticks": 456}}
    monkeypatch.setattr(product, "_load_broker_context", lambda *args: context)
    calls = []

    class Owner:
        def __init__(self, identity):
            assert identity == context["owner_process"]
            calls.append("bind")

        def __enter__(self):
            return self

        def alive(self):
            return False

        def __exit__(self, *args):
            calls.append("close")

    monkeypatch.setattr(windows_broker, "WindowsOwnerProcessV1", Owner)
    monkeypatch.setattr(product, "_serve_critic_broker", lambda *args, stop_requested: 0 if stop_requested() else 2)
    assert product.run_critic_broker(run, run / "unused", stop_requested=lambda: False) == 0
    assert calls == ["bind", "close"]


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


def test_installed_critic_generates_transport_valid_owner_bound_context(tmp_path, build, monkeypatch):
    from atlas.v2.agent_intelligence import windows_broker
    from atlas.v2.runtime import action_critic_shadow

    run = product.create_run(tmp_path, product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    epoch = "e" * 32
    owner = {"pid": 123, "creation_filetime_ticks": 456}
    monkeypatch.setattr(windows_broker, "current_owner_identity", lambda: owner)
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", lambda raw, decrypt: raw)
    shadow = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(action_critic_shadow, "create_action_assessment_shadow", lambda *args, **kwargs: shadow)
    child = SimpleNamespace(poll=lambda: None, wait=lambda timeout: None)

    def start(*args, **kwargs):
        product._publish(run / "epochs" / (epoch + "-broker-status.json"),
            {"run_id": run.name, "epoch_id": epoch, "health": {"status": "IMPLEMENTED"}})
        return child

    monkeypatch.setattr(product.subprocess, "Popen", start)
    runtime = product._start_installed_critic(run, epoch)
    context = product._load_broker_context(run, run / "epochs" / (epoch + "-broker.dpapi"))
    assert context["owner_process"] == owner and context["schema_version"] == 2
    assert windows_broker.PIPE_NAME_RE.fullmatch(context["pipe_name"])
    runtime.close()
