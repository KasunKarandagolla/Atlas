"""V2 owner configuration is immutable, bounded and non-authoritative."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

from atlas.v2 import product


@pytest.fixture
def build(monkeypatch):
    identity = {"source_sha": "a" * 40, "version": "2.0.0.41", "runtime_lock_sha256": "b" * 64}
    monkeypatch.setattr(product, "build_identity", lambda: dict(identity))
    return identity


def v2_config(root, **changes):
    values = {
        "data_root": product.canonical_data_root(root),
        "public_venues": ("BYBIT", "BINANCE"),
        "selected_execution_venue": "BINANCE",
        "execution_environment": "DEMO",
        "account_scope_ref": "ref_owner_demo",
        "capability_profile_ref": "sha256:" + "c" * 64,
        "credential_ref": "ref_demo_exchange_1",
    }
    values.update(changes)
    return product.ResearchRunConfigV2(**values)


def test_v2_manifest_freezes_registered_selection_and_zero_authority(tmp_path, build):
    config = v2_config(tmp_path)
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)

    assert manifest["configuration"]["public_venues"] == ["BYBIT", "BINANCE"]
    assert manifest["configuration"]["selected_execution_venue"] == "BINANCE"
    assert manifest["configuration"]["execution_environment"] == "DEMO"
    assert manifest["configuration"]["scope"] == "BOUNDED_UNIVERSE_V2"
    assert manifest["configuration"]["resource_profile_id"] == "bounded-v2"
    assert manifest["config_hash"] == config.content_hash
    assert manifest["capital_enabled"] is False
    assert manifest["assisted_enabled"] is False
    assert manifest["holdout_access"] is False
    assert "api_key" not in json.dumps(manifest).casefold()
    assert "api_secret" not in json.dumps(manifest).casefold()


def test_selected_run_summary_shows_immutable_venue_identity_without_secrets(tmp_path):
    config = v2_config(tmp_path, execution_profile="DEMO_NATIVE_OMS_QUALIFICATION",
        execution_instrument_symbol="ETHUSDT")
    text = product._selected_run_configuration_text("run-041", config,
        provider_key_status="NOT REQUIRED", demo_credential_status="PROTECTED RECORD PRESENT")

    assert "public venues: BYBIT + BINANCE" in text
    assert "authenticated execution: BINANCE / DEMO" in text
    assert "execution profile: DEMO_NATIVE_OMS_QUALIFICATION" in text
    assert "native instrument: ETHUSDT" in text
    assert "account scope ref: ref_owner_demo" in text
    assert "capability profile ref: sha256:" + "c" * 64 in text
    assert "demo/test credential: PROTECTED RECORD PRESENT" in text
    assert "Capital: OFF; assisted execution: OFF" in text
    assert "controls above set choices for a new run" in text
    assert "fixture-key" not in text and "fixture-secret" not in text


def test_v1_manifest_remains_readable_with_original_config_hash(tmp_path, build):
    config = product.ResearchRunConfigV1()
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)
    assert manifest["configuration"] == {
        "profile": "PUBLIC_BYBIT_BASELINE_V1",
        "report_interval_seconds": 60,
        "minimum_free_disk_bytes": 1_073_741_824,
        "provider_profile": "DISABLED",
    }
    assert manifest["config_hash"] == config.content_hash


def test_s7_provider_profile_is_v2_only():
    with pytest.raises(ValueError, match="unregistered provider dispatch profile"):
        product.ResearchRunConfigV1(provider_profile="openai-gpt6-astra-s7-event-extraction-v1")


def test_v2_root_must_match_create_location_and_relocation_is_rejected(tmp_path, build):
    selected_root = tmp_path / "selected"
    selected_root.mkdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    config = v2_config(selected_root)
    with pytest.raises(ValueError, match="differs"):
        product.create_run(elsewhere, config)

    run = product.create_run(selected_root, config)
    relocated_root = tmp_path / "relocated"
    relocated_root.mkdir()
    relocated_run = relocated_root / run.name
    relocated_run.mkdir()
    (relocated_run / "run.json").write_bytes((run / "run.json").read_bytes())
    with pytest.raises(ValueError, match="relocated"):
        product.load_run(relocated_run, require_current_build=False)


def test_v2_root_path_cannot_be_rebound_to_a_relocated_directory(tmp_path, build):
    selected_root = tmp_path / "selected"
    selected_root.mkdir()
    config = v2_config(selected_root)
    run = product.create_run(selected_root, config)
    moved_root = tmp_path / "moved"
    selected_root.rename(moved_root)
    selected_root.symlink_to(moved_root, target_is_directory=True)
    with pytest.raises(ValueError, match="canonical|relocated"):
        product.load_run(run, require_current_build=False)


@pytest.mark.parametrize("changes, message", [
    ({"execution_environment": "MAINNET"}, "DEMO or TESTNET"),
    ({"execution_environment": []}, "DEMO or TESTNET"),
    ({"selected_execution_venue": "KRAKEN"}, "unsupported selected execution venue"),
    ({"selected_execution_venue": []}, "unsupported selected execution venue"),
    ({"public_venues": ("BYBIT", "BYBIT")}, "unique registered venue tuple"),
    ({"public_venues": (["BYBIT"],)}, "unique registered venue tuple"),
    ({"public_venues": ()}, "unique registered venue tuple"),
    ({"scope": "ALL_MARKETS"}, "bounded universe scope"),
    ({"resource_profile_id": "unbounded"}, "resource profile"),
    ({"account_scope_ref": "123456789"}, "opaque registered reference"),
    ({"credential_ref": "sk-live-secret-value"}, "opaque registered reference"),
    ({"minimum_free_disk_bytes": True}, "disk reserve"),
    ({"provider_profile": []}, "unregistered provider dispatch profile"),
])
def test_v2_rejects_unregistered_scope_environment_and_references(tmp_path, changes, message):
    with pytest.raises(ValueError, match=message):
        v2_config(tmp_path, **changes)


def test_v2_canonical_root_is_required_before_run_creation(tmp_path):
    noncanonical = str(tmp_path / "child" / "..")
    with pytest.raises(ValueError, match="canonical"):
        product.ResearchRunConfigV2(data_root=noncanonical)


@pytest.mark.parametrize("public_venues, execution_venue", [
    (("BYBIT",), "BINANCE"),
    (("BINANCE",), "BYBIT"),
])
def test_public_collection_and_execution_selection_are_independent(
    tmp_path, public_venues, execution_venue,
):
    config = v2_config(tmp_path, public_venues=public_venues,
                       selected_execution_venue=execution_venue)
    assert config.public_venues == public_venues
    assert config.selected_execution_venue == execution_venue


def test_demo_credential_reference_is_bound_and_secret_is_not_in_filename(tmp_path, monkeypatch):
    store = product.WindowsSecretStore(tmp_path / "secrets")
    if product.os.name != "nt":
        with pytest.raises(RuntimeError, match="Windows protected secrets"):
            store.put_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO", "fixture-key", "fixture-secret")
        assert not store.has_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO")
    else:
        # On the Windows runner this is a real current-user DPAPI roundtrip.
        store.put_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO", "fixture-key", "fixture-secret")
        assert store.has_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO")
        credential = store.get_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO")
        assert (credential.api_key, credential.api_secret) == ("fixture-key", "fixture-secret")
        native_record = next((tmp_path / "secrets").glob("venue-*.dpapi")).read_bytes()
        assert b"fixture-key" not in native_record and b"fixture-secret" not in native_record

    # Exercise encrypted-file naming/writes without invoking any OS credential
    # API. The fake cipher marks bytes as ciphertext for this filesystem test.
    monkeypatch.setattr(product.os, "name", "nt")
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", staticmethod(
        lambda raw, *, decrypt: raw[::-1]))
    store.put_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO", "fixture-key", "fixture-secret")
    assert store.has_demo_exchange_credentials("ref_demo", "BYBIT", "DEMO")
    assert not store.has_demo_exchange_credentials("ref_demo", "BINANCE", "DEMO")
    assert not store.has_demo_exchange_credentials("ref_demo", "BYBIT", "TESTNET")
    record = next((tmp_path / "secrets").glob("venue-*.dpapi"))
    assert "key-value" not in record.name and "secret-value" not in record.name
    assert b"fixture-key" not in record.read_bytes() and b"fixture-secret" not in record.read_bytes()
    assert not store.has_demo_exchange_credentials("ref_other", "BYBIT", "DEMO")


def test_v2_provider_secret_root_is_bound_to_configured_data_root(tmp_path, build, monkeypatch):
    configured_root = tmp_path / "configured"
    configured_root.mkdir()
    run = product.create_run(configured_root, v2_config(configured_root, provider_profile="deepseek-v41-action-critic-v1"))
    manifest = product.load_run(run)
    store = product.secret_store_for_run(manifest)
    assert store.root == configured_root / "secrets"


def test_s7_provider_profile_is_immutable_and_survives_run_reload(tmp_path, build):
    profile = "openai-gpt6-astra-s7-event-extraction-v1"
    config = v2_config(tmp_path, provider_profile=profile)
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)

    assert manifest["configuration"]["provider_profile"] == profile
    assert manifest["config_hash"] == config.content_hash
    assert manifest["provider_configuration"] == product.provider_configuration(profile)
    assert manifest["provider_configuration"] == {
        "version": "AtlasEventExtractionProviderProfileV1", "provider": "openai",
        "model": "gpt-6-astra", "api": "responses", "reasoning_effort": "medium",
        "structured_output": "json_schema_strict", "max_model_calls_per_request": 1,
        "tools": [], "fallback": False,
    }
    assert product._run_config(manifest["configuration"]).content_hash == config.content_hash
    assert manifest["capital_enabled"] is False and manifest["assisted_enabled"] is False
    assert profile in product.PROVIDER_PROFILES


def test_provider_secret_profiles_use_separate_protected_files_and_bindings(tmp_path, monkeypatch):
    monkeypatch.setattr(product.os, "name", "nt")
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", staticmethod(
        lambda raw, *, decrypt: raw[::-1]))
    store = product.WindowsSecretStore(tmp_path / "secrets")
    deepseek = "deepseek-v41-action-critic-v1"
    event = "openai-gpt6-astra-s7-event-extraction-v1"
    store.put_provider_key(deepseek, "deepseek-test-key")
    store.put_provider_key(event, "openai-test-key")

    assert (store.root / "deepseek-action-critic.dpapi").is_file()
    event_record = store.root / "openai-s7-event-extraction.dpapi"
    assert event_record.is_file()
    assert store.get_provider_key(deepseek) == "deepseek-test-key"
    assert store.get_provider_key(event) == "openai-test-key"
    assert b"openai-test-key" not in event_record.read_bytes()
    assert b"deepseek-test-key" not in (store.root / "deepseek-action-critic.dpapi").read_bytes()
    with pytest.raises(ValueError, match="unsupported provider secret profile"):
        store.put_provider_key("openai-other-profile", "unused")


def test_s7_broker_context_is_dpapi_protected_and_epoch_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(product.os, "name", "nt")
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", staticmethod(
        lambda raw, *, decrypt: raw[::-1]))
    run = tmp_path / "runid"
    epochs = run / "epochs"
    epochs.mkdir(parents=True)
    epoch = "a" * 32
    context = {"schema_version": 1, "run_id": run.name, "epoch_id": epoch,
        "pipe_name": r"\\.\pipe\AtlasEventExtract-00000000-0000-0000-0000-000000000000",
        "authentication_key": "1" * 64, "signing_key": "2" * 64,
        "owner_process": {"pid": 123, "creation_filetime_ticks": 456}}
    path = epochs / (epoch + "-event-extraction-broker.dpapi")
    cipher = product.WindowsSecretStore._crypt(product.canonical_json(context).encode(), decrypt=False)
    path.write_bytes(cipher)

    loaded = product._load_event_extraction_broker_context(run, path)
    assert loaded == context
    assert b"authentication_key" not in path.read_bytes()
    wrong_epoch_path = epochs / ("b" * 32 + "-event-extraction-broker.dpapi")
    wrong_epoch_path.write_bytes(cipher)
    with pytest.raises(ValueError, match="context does not bind"):
        product._load_event_extraction_broker_context(run, wrong_epoch_path)


def test_s7_openai_client_ignores_ambient_proxy_and_disables_sdk_retries(monkeypatch):
    captured = {}

    class FakeHTTPClient:
        def __init__(self, **kwargs):
            captured["httpx"] = kwargs

    class FakeOpenAI:
        def __init__(self, **kwargs):
            captured["openai"] = kwargs

    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(Client=FakeHTTPClient))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAI))
    client = product._openai_event_extraction_client("fixture-only-key")
    assert isinstance(client, FakeOpenAI)
    assert captured["httpx"] == {"trust_env": False}
    assert captured["openai"]["max_retries"] == 0
    assert captured["openai"]["http_client"].__class__ is FakeHTTPClient

    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.invalid")
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("SAFE_FIXTURE", "kept")
    environment = product._provider_child_environment()
    assert not any("PROXY" in key.upper() for key in environment)
    assert "SAFE_FIXTURE" in environment


def test_v1_provider_secret_root_keeps_legacy_default(tmp_path, build, monkeypatch):
    legacy_root = tmp_path / "legacy-default"
    monkeypatch.setattr(product, "default_data_root", lambda: legacy_root)
    manifest = {"configuration": product.asdict(product.ResearchRunConfigV1())}
    assert product.secret_store_for_run(manifest).root == legacy_root / "secrets"


def test_v2_rejects_symlink_spelling_of_selected_root(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="canonical"):
        product.ResearchRunConfigV2(data_root=str(alias))
