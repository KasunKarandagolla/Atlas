from __future__ import annotations

import io
import json
from decimal import Decimal

import pytest

from atlas.runtime.binance_demo import DemoCredential
from atlas.runtime.bybit_demo import (
    DEMO_REST,
    MAX_READ_BYTES,
    BybitDemoIdentity,
    BybitDemoReader,
    BybitDemoReadError,
    build_bybit_demo_config,
    capture_bybit_account_snapshot,
    verify_bybit_protection,
)

T0 = 1_800_000_000_000_000_000


def identity(**changes):
    return BybitDemoIdentity("acct-ref", "cred-ref", **changes)


class Response(io.BytesIO):
    def __init__(self, body, url):
        super().__init__(body)
        self.url = url

    def geturl(self):
        return self.url


class JsonOpener:
    def __init__(self, bodies):
        self.bodies = iter(bodies)
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        path = request.full_url.split("?", 1)[0].removeprefix(DEMO_REST)
        body = next(self.bodies)
        if isinstance(body, Exception):
            raise body
        return Response(json.dumps(body).encode(), f"{DEMO_REST}{path}")


def result(value):
    return {"retCode": 0, "retMsg": "OK", "result": value, "time": T0 // 1_000_000}


def test_demo_identity_is_exact_and_opaque():
    assert identity().content_hash != identity(environment="TESTNET").content_hash
    with pytest.raises(ValueError):
        identity(environment="MAINNET")
    with pytest.raises(ValueError):
        BybitDemoIdentity("account uid 123", "cred-ref")


def test_pinned_native_config_is_demo_linear_isolated_zero_retry():
    credential = DemoCredential("demo-fixture-key", "demo-fixture-secret")
    cfg = build_bybit_demo_config(identity(), credential)
    assert cfg.environment.name.upper() == "DEMO"
    assert [value.name.upper() for value in cfg.product_types] == ["LINEAR"]
    assert cfg.margin_mode is None  # Observe isolated mode; connect must not mutate it.
    assert cfg.max_retries == 0
    assert "demo-fixture-secret" not in repr(credential)


def test_reader_signs_get_only_and_uses_fixed_demo_host():
    opener = JsonOpener([result({"uid": "private-account-id", "unifiedMarginStatus": 3,
                                 "marginMode": "ISOLATED_MARGIN"})])
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                             clock_ns=lambda: T0, opener=opener)
    receipt = reader.read_with_receipt("/v5/account/info")
    req, timeout = opener.requests[0]
    assert req.method == "GET"
    assert req.full_url == DEMO_REST + "/v5/account/info"
    assert req.get_header("X-bapi-api-key") == "key"
    assert req.get_header("X-bapi-sign")
    assert timeout == 2.0
    assert receipt.identity_hash == identity().content_hash
    assert receipt.raw_payload_hash and receipt.canonical_payload_hash
    assert "private-account-id" not in repr(receipt)
    assert "secret" not in repr(receipt)
    with pytest.raises(ValueError):
        reader.read_with_receipt("/v5/order/create")
    with pytest.raises(ValueError):
        reader.read_with_receipt("https://api.bybit.com/v5/account/info")


def test_reader_sanitizes_failures_redirects_and_payload_overflow():
    credential = DemoCredential("fixture-key", "fixture-secret")
    reader = BybitDemoReader(identity=identity(), credential=credential, clock_ns=lambda: T0,
                             opener=JsonOpener([OSError("fixture-secret leaked by server")]))
    with pytest.raises(BybitDemoReadError) as err:
        reader.read_with_receipt("/v5/account/info")
    assert "fixture-secret" not in str(err.value)
    assert err.value.__suppress_context__

    class HugeOpener:
        def open(self, request, *, timeout):
            path = request.full_url.split("?", 1)[0].removeprefix(DEMO_REST)
            return Response(b" " * (MAX_READ_BYTES + 1), f"{DEMO_REST}{path}")

    reader = BybitDemoReader(identity=identity(), credential=credential, clock_ns=lambda: T0,
                             opener=HugeOpener())
    with pytest.raises(BybitDemoReadError, match="BUDGET"):
        reader.read_with_receipt("/v5/account/info")

    class RedirectOpener:
        def open(self, request, *, timeout):
            return Response(b"{}", "https://api.bybit.com/v5/account/info")

    reader = BybitDemoReader(identity=identity(), credential=credential, clock_ns=lambda: T0,
                             opener=RedirectOpener())
    with pytest.raises(BybitDemoReadError, match="ENDPOINT"):
        reader.read_with_receipt("/v5/account/info")


def test_list_reader_bounds_endpoint_page_size_and_cursor_walk():
    opener = JsonOpener([
        result({"list": [{"orderId": "first"}], "nextPageCursor": "cursor-1"}),
        result({"list": [{"orderId": "second"}], "nextPageCursor": ""}),
    ])
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                             clock_ns=lambda: T0, opener=opener)
    receipts = reader.read_pages("/v5/order/history", {"category": "linear"})
    assert len(receipts) == 2
    assert "limit=50" in opener.requests[0][0].full_url
    assert "cursor=cursor-1" in opener.requests[1][0].full_url
    with pytest.raises(ValueError, match="limit"):
        reader.read_with_receipt("/v5/order/history", {"limit": 51})
    with pytest.raises(ValueError, match="pagination"):
        reader.read_pages("/v5/account/wallet-balance")


@pytest.mark.parametrize("unified_status", [3, 4, 5, 6])
def test_snapshot_current_profile_qualifies_all_supported_unified_modes(unified_status):
    records = [
        result({"unifiedMarginStatus": unified_status, "marginMode": "ISOLATED_MARGIN"}),
        result({"list": [{"accountType": "UNIFIED", "coin": []}]}),
        result({"list": [{"symbol": "BTCUSDT", "positionIdx": 0, "size": "0",
                          "side": "", "tradeMode": 0}], "nextPageCursor": ""}),
    ]
    opener = JsonOpener(records)
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                             clock_ns=lambda: T0, opener=opener)
    snapshot = capture_bybit_account_snapshot(reader, native_symbol="BTCUSDT")
    assert snapshot.eligible and snapshot.profile_qualified
    assert snapshot.status == "BYBIT_CURRENT_PROFILE_QUALIFIED"
    assert snapshot.account_fingerprint
    assert snapshot.account_fingerprint_status == "DECLARED_SCOPE_CREDENTIAL_BINDING"
    assert snapshot.account_identity_status == "UNVERIFIED_ACCOUNT_IDENTITY"
    assert len(snapshot.receipts) == 3
    paths = [request.full_url for request, _ in opener.requests]
    assert all("history" not in path and "execution" not in path for path in paths)
    assert "symbol=BTCUSDT" in paths[-1]


@pytest.mark.parametrize("position", [[], [{"symbol": "BTCUSDT", "positionIdx": 1, "size": "1", "side": "Buy"}],
                                     [{"symbol": "ETHUSDT", "positionIdx": 0, "size": "0", "side": ""}]])
def test_empty_or_wrong_selected_symbol_does_not_prove_one_way_mode(position):
    records = [result({"unifiedMarginStatus": 5, "marginMode": "ISOLATED_MARGIN"}),
               result({"list": [{"accountType": "UNIFIED"}]}),
               result({"list": position, "nextPageCursor": ""})]
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                            clock_ns=lambda: T0, opener=JsonOpener(records))
    snapshot = capture_bybit_account_snapshot(reader)
    assert not snapshot.eligible and "selected symbol one-way mode unconfirmed" in snapshot.reasons


def test_snapshot_incomplete_read_set_fails_closed():
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                             clock_ns=lambda: T0, opener=JsonOpener([OSError("offline")]))
    snapshot = capture_bybit_account_snapshot(reader)
    assert not snapshot.eligible
    assert snapshot.account_fingerprint is None
    assert snapshot.status == "BYBIT_READ_UNRESOLVED"


def test_bybit_full_mark_price_protection_readback_is_exact_and_fresh():
    row = {"symbol": "BTCUSDT", "positionIdx": 0, "tpslMode": "Full",
           "slTriggerBy": "MarkPrice", "side": "Buy", "size": "2", "stopLoss": "95000"}
    kwargs = {"symbol": "BTCUSDT", "expected_signed_qty": Decimal("2"),
              "stop_price": Decimal("95000"), "now_ns": T0 + 1, "received_at_ns": T0}
    # Position/list does not expose triggerBy; a made-up slTriggerBy cannot verify.
    assert not verify_bybit_protection(row, **kwargs).verified
    stop_order = {"symbol": "BTCUSDT", "positionIdx": 0, "stopOrderType": "StopLoss",
                  "orderType": "Market", "triggerBy": "MarkPrice", "tpslMode": "Full",
                  "orderStatus": "Untriggered", "reduceOnly": True, "closeOnTrigger": True,
                  "side": "Sell", "triggerPrice": "95000", "qty": "2"}
    kwargs |= {"stop_order": stop_order, "stop_order_received_at_ns": T0}
    assert verify_bybit_protection(row, **kwargs).verified
    for key, value in {"positionIdx": 1, "tpslMode": "Partial",
                       "side": "Sell", "size": "1", "stopLoss": "94999", "symbol": "ETHUSDT"}.items():
        assert not verify_bybit_protection(row | {key: value}, **kwargs).verified
    assert not verify_bybit_protection(row, **(kwargs | {"now_ns": T0 + 3_000_000_000})).verified


def test_position_stop_delay_and_wrong_trigger_keep_protection_unconfirmed():
    row = {"symbol": "BTCUSDT", "positionIdx": 0, "tpslMode": "Full", "side": "Buy",
           "size": "2", "stopLoss": "95000"}
    order = {"symbol": "BTCUSDT", "positionIdx": 0, "stopOrderType": "StopLoss",
             "orderType": "Market", "triggerBy": "LastPrice", "tpslMode": "Full",
             "orderStatus": "Untriggered", "reduceOnly": True, "closeOnTrigger": True,
             "side": "Sell", "triggerPrice": "95000", "qty": "2"}
    kwargs = {"symbol": "BTCUSDT", "expected_signed_qty": Decimal("2"), "stop_price": Decimal("95000"),
              "now_ns": T0, "received_at_ns": T0, "stop_order_received_at_ns": T0}
    assert not verify_bybit_protection(row, stop_order=order, **kwargs).verified
    assert not verify_bybit_protection(row, stop_order=order | {"triggerBy": "MarkPrice"},
                                      **(kwargs | {"stop_order_received_at_ns": T0 - 3_000_000_000})).verified


def test_historical_diagnostics_have_explicit_window_and_actual_receipt_clocks():
    from atlas.runtime.bybit_demo import capture_bybit_historical_diagnostics
    opener = JsonOpener([result({"list": [], "nextPageCursor": ""}) for _ in range(4)])
    reader = BybitDemoReader(identity=identity(), credential=DemoCredential("key", "secret"),
                            clock_ns=lambda: T0, opener=opener)
    end_ms = T0 // 1_000_000
    diagnostic = capture_bybit_historical_diagnostics(reader, native_symbol="BTCUSDT",
                                                      start_ms=end_ms - 1000, end_ms=end_ms)
    assert len(diagnostic.receipts) == 4
    assert diagnostic.status == "BOUNDED_DIAGNOSTICS_NOT_RECOVERY_COMPLETENESS"
    assert all(item.requested_at_ns == item.received_at_ns == T0 for item in diagnostic.receipts)
    assert all(f"startTime={end_ms - 1000}" in req.full_url and f"endTime={end_ms}" in req.full_url
               for req, _ in opener.requests)
    with pytest.raises(ValueError, match="window"):
        capture_bybit_historical_diagnostics(reader, native_symbol="BTCUSDT", start_ms=1, end_ms=end_ms)
