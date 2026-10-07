"""Immutable selected demo bindings, fencing and protected retrieval."""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.binance_demo import BinanceDemoReadReceipt, DemoCredential
from atlas.runtime.selected_demo import SelectedDemoQualification
from atlas.runtime.writer_lock import WriterAlreadyActive
from atlas.v2.product import ResearchRunConfigV2, WindowsSecretStore

T0 = 1_800_000_000_000_000_000


def configuration(root, **changes):
    values = {"data_root": str(root.resolve()), "public_venues": ("BYBIT", "BINANCE"),
        "selected_execution_venue": "BINANCE", "execution_environment": "DEMO",
        "account_scope_ref": "ref_owner", "capability_profile_ref": "sha256:" + "a" * 64,
        "credential_ref": "ref_credential", "execution_profile": "DEMO_READ_ONLY_QUALIFICATION"}
    values.update(changes)
    return ResearchRunConfigV2(**values)


class Reader:
    alias = "fixture-alias"

    def __init__(self, *, identity, credential, clock_ns):
        self.identity = identity
        self.clock_ns = clock_ns
        self.calls = []

    def read_with_receipt(self, path):
        self.calls.append(path)
        payloads = {
            "/fapi/v3/account": {"totalWalletBalance": "100", "assets": [{"asset": "USDT"}]},
            "/fapi/v1/accountConfig": {"canTrade": True, "dualSidePosition": False, "multiAssetsMargin": False},
            "/fapi/v3/positionRisk": [],
            "/fapi/v1/symbolConfig": [{"symbol": "SOLUSDT", "marginType": "ISOLATED"}],
            "/fapi/v3/balance": [{"asset": "USDT", "accountAlias": self.alias}],
            "/fapi/v1/exchangeInfo": {"symbols": [{"symbol": "SOLUSDT", "baseAsset": "SOL",
                "quoteAsset": "USDT", "marginAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING",
                "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100", "stepSize": "0.01"},
                    {"filterType": "MARKET_LOT_SIZE", "minQty": "0.01", "maxQty": "100", "stepSize": "0.01"}]}]},
        }
        body = json.dumps(payloads[path], sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(body.encode()).hexdigest()
        return BinanceDemoReadReceipt(self.identity.content_hash, path, self.clock_ns(), digest, digest, body)


def host(root, lease_root, **changes):
    run = root / "run"
    run.mkdir(exist_ok=True)
    store = Mock()
    store.get_demo_exchange_credentials.return_value = DemoCredential("fixture-key", "fixture-material")
    return SelectedDemoQualification(run=run, configuration=configuration(root, **changes),
        secret_store=store, lease_root=lease_root, clock_ns=lambda: T0, reader_factory=Reader)


def test_constructor_does_not_read_credentials_or_connect_and_open_binds_exact_run(tmp_path):
    runtime = host(tmp_path, tmp_path / "lease")
    runtime.secret_store.get_demo_exchange_credentials.assert_not_called()
    assert runtime.journal is None
    try:
        result = runtime.open()
        runtime.secret_store.get_demo_exchange_credentials.assert_called_once_with("ref_credential", "BINANCE", "DEMO")
        assert result["account_profile"] == "TESTED"
        assert runtime.journal.load_unresolved_commands() == []
        assert result["capital_enabled"] is result["assisted_enabled"] is False
        assert runtime.account_snapshot().account_fingerprint_status == "VERIFIED_ACCOUNT_ALIAS_RECEIPT"
        assert "fixture-alias" not in (runtime.run / "demo-account-binding.json").read_text()
    finally:
        runtime.close()


def test_common_writer_lease_blocks_second_venue_and_data_root(tmp_path):
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first = host(first_root, tmp_path / "lease")
    second = host(second_root, tmp_path / "lease", selected_execution_venue="BYBIT")
    first.open()
    try:
        with pytest.raises(WriterAlreadyActive):
            second.open()
        second.secret_store.get_demo_exchange_credentials.assert_not_called()
    finally:
        first.close()
        second.close()


def test_restart_changed_account_remains_ineligible_and_preserves_first_binding(tmp_path, monkeypatch):
    first = host(tmp_path, tmp_path / "lease")
    first.open()
    initial = (first.run / "demo-account-binding.json").read_bytes()
    first.close()
    monkeypatch.setattr(Reader, "alias", "different-fixture-alias")
    second = host(tmp_path, tmp_path / "lease")
    try:
        result = second.open()
        assert result["account_profile"] == "TEST GATE"
        assert "ACCOUNT_FINGERPRINT_CHANGED" in result["account_profile_reasons"]
        assert (second.run / "demo-account-binding.json").read_bytes() == initial
    finally:
        second.close()


def test_stale_writer_fails_before_native_effect(tmp_path):
    runtime = host(tmp_path, tmp_path / "lease")
    runtime.open()
    try:
        runtime.lease.path.write_text("999:stale-owner")
        with pytest.raises(PersistenceError, match="epoch is stale"):
            runtime.assert_writer()
    finally:
        runtime.close()


def test_native_profile_builds_exact_observed_demo_product_without_running(tmp_path):
    runtime = host(tmp_path, tmp_path / "lease", execution_profile="DEMO_NATIVE_OMS_QUALIFICATION",
                   execution_instrument_symbol="SOLUSDT")
    native = SimpleNamespace(node=Mock(), _run_task=None)
    runtime.node_factory = Mock(return_value=native)
    try:
        result = runtime.open()
        assert result["native_oms_constructed"] is True
        values = runtime.node_factory.call_args.kwargs
        assert values["product"].key.environment.value == "DEMO"
        assert values["product"].key.native_symbol == "SOLUSDT"
        assert values["writer_epoch"] == runtime.ownership.writer_epoch
        assert "BINANCE_FILL_TIME_PROTECTION" in result["opening_gate"]
        assert runtime.journal.load_unresolved_intents() == []
    finally:
        runtime.close()
    native.node.dispose.assert_called_once()


def test_config_hash_changes_for_qualification_profile_and_symbol(tmp_path):
    config = configuration(tmp_path)
    native = replace(config, execution_profile="DEMO_NATIVE_OMS_QUALIFICATION", execution_instrument_symbol="SOLUSDT")
    assert native.content_hash != config.content_hash
    assert replace(native, execution_instrument_symbol="ETHUSDT").content_hash != native.content_hash
    with pytest.raises(ValueError):
        replace(config, execution_profile="REAL_MONEY")


def test_protected_getter_exact_binding_and_sanitized_tamper(tmp_path, monkeypatch):
    store = WindowsSecretStore(tmp_path)
    name = store._venue_credentials_name("ref_one", "BINANCE", "DEMO")
    body = {"schema_version": 1, "reference": "ref_one", "venue": "BINANCE", "environment": "DEMO",
            "api_key": "fixture-key", "api_secret": "fixture-private-material"}
    (tmp_path / name).write_bytes(b"encrypted-fixture")
    monkeypatch.setattr(store, "_crypt", lambda *_args, **_kwargs: json.dumps(body).encode())
    credential = store.get_demo_exchange_credentials("ref_one", "BINANCE", "DEMO")
    assert credential.api_key == "fixture-key"
    assert "fixture" not in repr(credential)
    body["environment"] = "TESTNET"
    with pytest.raises(ValueError) as failure:
        store.get_demo_exchange_credentials("ref_one", "BINANCE", "DEMO")
    assert "fixture-private-material" not in str(failure.value)


def test_protected_getter_rejects_symlinks_and_excess_size(tmp_path):
    store = WindowsSecretStore(tmp_path)
    name = store._venue_credentials_name("ref_one", "BINANCE", "DEMO")
    target = tmp_path / name
    other = tmp_path / "other"
    other.write_bytes(b"fixture")
    target.symlink_to(other)
    with pytest.raises(ValueError, match="symlink"):
        store.get_demo_exchange_credentials("ref_one", "BINANCE", "DEMO")
    target.unlink()
    target.write_bytes(b"x" * 65_537)
    with pytest.raises(ValueError, match="size"):
        store.get_demo_exchange_credentials("ref_one", "BINANCE", "DEMO")


class BybitReader:
    def __init__(self, *, identity, credential, clock_ns):
        self.identity, self._credential, self.clock_ns = identity, credential, clock_ns

    def read_with_receipt(self, path, params=None):
        from atlas.runtime.bybit_demo import BybitDemoReadReceipt

        payloads = {
            "/v5/account/info": {"marginMode": "ISOLATED_MARGIN", "unifiedMarginStatus": 6},
            "/v5/account/wallet-balance": {"list": [{"accountType": "UNIFIED"}]},
            "/v5/position/list": {"list": [{"symbol": "SOLUSDT", "positionIdx": 0, "size": "0", "side": ""}]},
            "/v5/market/instruments-info": {"list": [{"symbol": "SOLUSDT", "baseCoin": "SOL",
                "quoteCoin": "USDT", "settleCoin": "USDT", "contractType": "LinearPerpetual",
                "status": "Trading", "priceFilter": {"tickSize": "0.01"},
                "lotSizeFilter": {"qtyStep": "0.01", "minOrderQty": "0.01", "maxOrderQty": "100", "maxMktOrderQty": "100"}}]},
        }
        body = json.dumps({"retCode": 0, "result": payloads[path]}, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(body.encode()).hexdigest()
        return BybitDemoReadReceipt(self.identity.content_hash, path, self.clock_ns(), digest, digest, body)

    def read_pages(self, path, params):
        assert params == {"category": "linear", "symbol": "SOLUSDT"}
        return (self.read_with_receipt(path, params),)


def test_selected_bybit_uses_its_exact_profile_and_native_metadata_without_fallback(tmp_path):
    runtime = host(tmp_path, tmp_path / "lease", selected_execution_venue="BYBIT",
        execution_profile="DEMO_NATIVE_OMS_QUALIFICATION", execution_instrument_symbol="SOLUSDT")
    runtime.reader_factory = BybitReader
    native = SimpleNamespace(node=Mock(), _run_task=None)
    runtime.node_factory = Mock(return_value=native)
    try:
        status = runtime.open()
        assert status["selected_execution_venue"] == "BYBIT"
        assert status["account_identity"] == "UNVERIFIED_ACCOUNT_IDENTITY"
        assert "BYBIT_FULL_MARK_PRICE" in status["opening_gate"]
        assert runtime.product.key.venue.value == "BYBIT"
        assert runtime.product.key.native_symbol == "SOLUSDT"
        runtime.reconcile_once()
        assert runtime.journal.load_unresolved_commands() == []
        with pytest.raises(PersistenceError, match="does not use Binance"):
            runtime.capture_recovery(history_start_ns=T0 - 1)
    finally:
        runtime.close()


def test_recovery_capture_rejects_live_native_host_before_signed_read(tmp_path):
    runtime = host(tmp_path, tmp_path / "lease")
    try:
        runtime.open()
        calls = list(runtime.reader.calls)
        runtime.node = SimpleNamespace(_run_task=SimpleNamespace(done=lambda: False))
        with pytest.raises(PersistenceError, match="stop and drain"):
            runtime.capture_recovery(history_start_ns=T0 - 1)
        assert runtime.reader.calls == calls
    finally:
        runtime.node = None
        runtime.close()
