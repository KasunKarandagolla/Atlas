"""Strict Binance Futures Demo boundary tests. Exchange replies are fixtures."""

from __future__ import annotations

import hashlib
import hmac
import urllib.parse
from contextlib import closing
from decimal import Decimal

import pytest

from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.binance_demo import external_client_order_id, verify_binance_protection
from atlas.runtime.binance_futures_demo_r4 import (
    BINANCE_FUTURES_DEMO_REST,
    BINANCE_FUTURES_DEMO_WS,
    DemoOnlyAdapter,
    DemoOnlyCredentials,
    DemoOnlyIdentity,
    DemoOnlyPaths,
    DemoOrderState,
    DemoTransportFailure,
    build_demo_order_request,
    normalize_demo_account,
    normalize_demo_fills,
    normalize_demo_instrument,
    normalize_demo_margin,
    normalize_demo_order,
    normalize_demo_order_state,
    normalize_demo_protection,
    normalize_demo_transport_failure,
)
from atlas.runtime.fill_dedup import FillDeduplicator, FillRecord


def credentials() -> DemoOnlyCredentials:
    return DemoOnlyCredentials("fixture-demo-key", "fixture-demo-secret")


def test_demo_only_endpoint_identity_and_state_paths_are_distinct_and_fixed(tmp_path):
    identity = DemoOnlyIdentity("owner-demo", "demo-key-ref")
    adapter = DemoOnlyAdapter(identity=identity, credentials=credentials(), clock_ms=lambda: 1_800_000_000_123,
                              allow_fixture_credentials=True)
    assert identity.environment == "DEMO_ONLY"
    assert adapter.base_url == BINANCE_FUTURES_DEMO_REST
    assert adapter.websocket_url == BINANCE_FUTURES_DEMO_WS
    with pytest.raises(AttributeError):
        adapter.base_url = "https://fapi.binance.com"
    with pytest.raises(ValueError, match="DEMO_ONLY"):
        DemoOnlyAdapter(identity=identity, credentials=credentials(), base_url="https://fapi.binance.com",
                        clock_ms=lambda: 1_800_000_000_123, allow_fixture_credentials=True)
    for other_environment in ("LIVE", "TESTNET", "SHADOW"):
        with pytest.raises(ValueError, match="environment"):
            DemoOnlyIdentity("owner-demo", "demo-key-ref", environment=other_environment)
    paths = DemoOnlyPaths.under(tmp_path)
    assert paths.database.parent == paths.root
    assert "DEMO_ONLY" in str(paths.root)
    assert paths.database.name == "binance-futures-demo.sqlite3"
    assert paths.logs.parent == paths.root
    assert paths.orders.parent == paths.root
    assert paths.account_snapshots.parent == paths.root


def test_demo_environment_loader_rejects_live_credential_aliases():
    env = {"ATLAS_BINANCE_FUTURES_ENV": "DEMO_ONLY",
           "BINANCE_FUTURES_DEMO_API_KEY": "demo-key", "BINANCE_FUTURES_DEMO_API_SECRET": "demo-secret"}
    credentials_from_env = DemoOnlyCredentials.from_environment(env)
    assert credentials_from_env.api_key == "demo-key"
    assert credentials_from_env.source == "BINANCE_FUTURES_DEMO_ENV"
    for name in ("BINANCE_API_KEY", "BINANCE_SECRET_KEY", "BINANCE_LIVE_API_KEY"):
        with pytest.raises(ValueError, match="live credential"):
            DemoOnlyCredentials.from_environment(env | {name: "live-material"})
    for name in ("ATLAS_BINANCE_FUTURES_ENDPOINT", "BINANCE_FUTURES_TESTNET_BASE_URL"):
        with pytest.raises(ValueError, match="override"):
            DemoOnlyCredentials.from_environment(env | {name: "https://fapi.binance.com"})
    assert "fixture-demo-secret" not in repr(credentials())
    with pytest.raises(ValueError, match="environment identity"):
        DemoOnlyCredentials.from_environment(env | {"ATLAS_BINANCE_FUTURES_ENV": "TESTNET"})


def test_demo_error_normalization_discards_exchange_text_and_rejects_bad_statuses():
    # The venue's free-form msg may contain account or request details; only a
    # registered status code crosses the boundary.
    assert normalize_demo_transport_failure(400, {"code": -1022, "msg": "sensitive signature"}) is \
        DemoTransportFailure.INVALID_SIGNATURE
    assert normalize_demo_transport_failure(400, {"code": -1021, "msg": "sensitive timestamp"}) is \
        DemoTransportFailure.TIMESTAMP_OUTSIDE_WINDOW
    assert normalize_demo_transport_failure(401, {"code": -2015, "msg": "sensitive credential"}) is \
        DemoTransportFailure.AUTHENTICATION_REJECTED
    assert normalize_demo_transport_failure(429, {"msg": "sensitive quota"}) is DemoTransportFailure.RATE_LIMITED
    assert normalize_demo_transport_failure(503, {"msg": "sensitive server text"}) is \
        DemoTransportFailure.VENUE_UNAVAILABLE
    assert normalize_demo_transport_failure(302, {"location": "https://fapi.binance.com"}) is \
        DemoTransportFailure.UNSAFE_REDIRECT
    with pytest.raises(ValueError, match="status"):
        normalize_demo_transport_failure(99, {"msg": "ignored"})
    with pytest.raises(ValueError, match="successful"):
        normalize_demo_transport_failure(200, {})


def test_test_fixture_credentials_are_rejected_by_default_runner_constructor():
    with pytest.raises(ValueError, match="fixture credentials"):
        DemoOnlyAdapter(identity=DemoOnlyIdentity("owner-demo", "demo-key-ref"),
                        credentials=credentials(), clock_ms=lambda: 1_800_000_000_123)


def test_signed_read_uses_exact_timestamp_signature_and_fixed_demo_host():
    adapter = DemoOnlyAdapter(identity=DemoOnlyIdentity("owner-demo", "demo-key-ref"),
                              credentials=credentials(), clock_ms=lambda: 1_800_000_000_123,
                              allow_fixture_credentials=True)
    request = adapter.build_signed_read("/fapi/v3/account", {"recvWindow": 5000})
    parsed = urllib.parse.urlsplit(request.full_url)
    assert parsed.scheme == "https" and parsed.hostname == "demo-fapi.binance.com"
    values = urllib.parse.parse_qs(parsed.query)
    unsigned = urllib.parse.urlencode(sorted((key, value[0]) for key, value in values.items() if key != "signature"))
    expected = hmac.new(b"fixture-demo-secret", unsigned.encode(), hashlib.sha256).hexdigest()
    assert values["timestamp"] == ["1800000000123"]
    assert values["signature"] == [expected]
    assert request.get_header("X-mbx-apikey") == "fixture-demo-key"
    for path in ("/api/v3/account", "https://fapi.binance.com/fapi/v3/account", "/fapi/v1/withdraw"):
        with pytest.raises(ValueError):
            adapter.build_signed_read(path)


def test_demo_order_is_typed_offline_and_unknown_exchange_status_is_not_success():
    request = build_demo_order_request(symbol="BTCUSDT", side="BUY", quantity="0.001",
                                      client_order_id="r4-fixture-order", reduce_only=False)
    assert request.environment == "DEMO_ONLY"
    assert request.symbol == "BTCUSDT" and request.side == "BUY"
    assert request.reduce_only is False
    assert request.venue_effect == "REQUEST_GENERATION_ONLY"
    assert normalize_demo_order_state({"status": "PARTIALLY_FILLED"}) is DemoOrderState.PARTIALLY_FILLED
    assert normalize_demo_order_state({"status": "FILLED"}) is DemoOrderState.FILLED
    with pytest.raises(ValueError, match="unknown order state"):
        normalize_demo_order_state({"status": "PENDING_EXCHANGE_MAGIC"})
    with pytest.raises(ValueError, match="required"):
        normalize_demo_order_state({})


def test_fake_account_instrument_margin_order_fill_cancel_and_protection_contracts():
    identity = DemoOnlyIdentity("fixture-account", "fixture-credential")
    account = normalize_demo_account(identity, {
        "canTrade": True,
        "assets": [{"asset": "USDT", "walletBalance": "1000.00", "availableBalance": "900.00"}],
        "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.001", "entryPrice": "50000",
                       "unRealizedProfit": "1.25"}],
    }, captured_at_ns=1_800_000_000_123_000_000)
    assert account.environment == "DEMO_ONLY"
    assert account.balances[0].wallet_balance == Decimal("1000.00")
    assert account.positions[0].position_amount == Decimal("0.001")

    instrument = normalize_demo_instrument(identity, {
        "symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT",
        "filters": [{"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "100",
                     "stepSize": "0.001"}],
    })
    assert instrument.market_min_quantity == Decimal("0.001")
    margin = normalize_demo_margin(identity, {"multiAssetsMargin": False}, [
        {"symbol": "BTCUSDT", "marginType": "isolated", "isAutoAddMargin": False, "leverage": 2},
    ])
    assert margin.multi_assets_margin is False and margin.symbols[0].leverage == 2

    order = normalize_demo_order(identity, {
        "orderId": 123, "clientOrderId": "r4-fixture", "symbol": "BTCUSDT", "side": "BUY",
        "type": "MARKET", "status": "PARTIALLY_FILLED", "origQty": "0.002", "executedQty": "0.001",
    })
    assert order.status is DemoOrderState.PARTIALLY_FILLED
    canceled = normalize_demo_order(identity, {
        "orderId": 123, "clientOrderId": "r4-fixture", "symbol": "BTCUSDT", "side": "BUY",
        "type": "MARKET", "status": "CANCELED", "origQty": "0.002", "executedQty": "0.001",
    })
    rejected = normalize_demo_order(identity, {
        "orderId": 124, "clientOrderId": "r4-rejected", "symbol": "BTCUSDT", "side": "BUY",
        "type": "MARKET", "status": "REJECTED", "origQty": "0.002", "executedQty": "0",
    })
    assert canceled.status is DemoOrderState.CANCELED  # Cancellation state is observed, never sent.
    assert rejected.status is DemoOrderState.REJECTED

    fills = normalize_demo_fills(identity, [{
        "id": 77, "orderId": 123, "symbol": "BTCUSDT", "side": "BUY", "price": "50000",
        "qty": "0.001", "commission": "0.02", "commissionAsset": "USDT", "time": 1_800_000_000_000,
    }])
    assert fills[0].execution_id == "77" and fills[0].quantity == Decimal("0.001")

    protection_row = {
        "algoId": 45, "algoType": "CONDITIONAL", "symbol": "BTCUSDT", "side": "SELL", "algoStatus": "NEW",
        "orderType": "STOP_MARKET", "workingType": "MARK_PRICE", "triggerPrice": "49000",
        "positionSide": "BOTH", "clientAlgoId": external_client_order_id("a" * 32),
        "closePosition": True, "reduceOnly": False, "quantity": "0",
    }
    protection = normalize_demo_protection(identity, protection_row)
    assert protection.status == "NEW" and protection.is_close_position
    assert protection.is_stop_market is True
    valid = verify_binance_protection(
        protection_row, symbol="BTCUSDT", client_order_id="a" * 32, stop=Decimal("49000"),
        signed_position=Decimal("0.001"), now_ns=1_800_000_000_123_000_000,
        received_at_ns=1_800_000_000_123_000_000,
    )
    assert valid.verified
    invalid = verify_binance_protection(
        protection_row | {"workingType": "CONTRACT_PRICE"}, symbol="BTCUSDT", client_order_id="a" * 32,
        stop=Decimal("49000"), signed_position=Decimal("0.001"), now_ns=1_800_000_000_123_000_000,
        received_at_ns=1_800_000_000_123_000_000,
    )
    assert not invalid.verified
    wrong_stop = normalize_demo_protection(identity, protection_row | {"workingType": "CONTRACT_PRICE"})
    assert wrong_stop.is_stop_market is False
    with pytest.raises(ValueError, match="unknown"):
        normalize_demo_protection(identity, {"algoId": 45, "algoStatus": "UNKNOWN_FUTURE_STATE"})


def test_demo_only_restart_reconciliation_deduplicates_exchange_execution_events(tmp_path):
    paths = DemoOnlyPaths.under(tmp_path)
    paths.root.mkdir(parents=True)
    record = FillRecord(
        execution_id="77", order_id="123", client_order_id="r4-fixture", intent_id="r4-intent",
        instrument="BTCUSDT", side="Buy", qty=Decimal("0.001"), price=Decimal("50000"),
        fee=Decimal("0.02"), fee_currency="USDT", trade_time_ns=1_800_000_000_000_000_000,
        receive_time_ns=1_800_000_000_001_000_000, source="BINANCE_FUTURES_DEMO_ONLY_FIXTURE",
        raw_hash=hashlib.sha256(b"fake-fill-response").hexdigest(),
    )
    with closing(SQLiteJournal(paths.database)) as journal:
        assert FillDeduplicator(journal).try_record_fill(record).accepted
    with closing(SQLiteJournal(paths.database)) as restarted:
        deduplicator = FillDeduplicator(restarted)
        duplicate = deduplicator.try_record_fill(record)
        assert not duplicate.accepted
        assert duplicate.duplicate_of == "77"
        assert deduplicator.get_cumulative_qty("r4-intent") == Decimal("0.001")


def test_adapter_has_no_order_submission_and_fake_response_normalization_is_sanitized():
    adapter = DemoOnlyAdapter(identity=DemoOnlyIdentity("owner-demo", "demo-key-ref"),
                              credentials=credentials(), clock_ms=lambda: 1_800_000_000_123,
                              allow_fixture_credentials=True)
    assert not hasattr(adapter, "submit_order")
    with pytest.raises(ValueError, match="GET only"):
        adapter.request("POST", "/fapi/v1/order", {})
    with pytest.raises(ValueError, match="DEMO_ONLY"):
        adapter.request("GET", "https://fapi.binance.com/fapi/v2/account", {})
