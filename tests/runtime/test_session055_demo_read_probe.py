"""Safety checks for the approved read-only Demo connectivity probe."""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request
from types import SimpleNamespace
from urllib.parse import urlsplit

from scripts import atlas_red_demo_read_probe as probe


def test_blocked_demo_clock_read_is_sanitized_and_stops_before_account_get(monkeypatch, capsys):
    monkeypatch.setenv("ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED", "YES")
    monkeypatch.setenv("ATLAS_BINANCE_FUTURES_ENV", "DEMO_ONLY")
    monkeypatch.setattr(
        probe.DemoOnlyCredentials, "from_environment",
        classmethod(lambda cls, env: object()),
    )
    monkeypatch.setattr(probe, "DemoOnlyAdapter", lambda **kwargs: _NeverSignedAdapter())

    expected_proxy = {"https": "http://managed-proxy.invalid:8080"}
    monkeypatch.setattr(probe.urllib.request, "getproxies", lambda: expected_proxy)

    class BlockedOpener:
        def open(self, request, timeout):
            assert request.full_url == probe.BINANCE_FUTURES_DEMO_REST + "/fapi/v1/time"
            assert timeout == 5.0
            raise urllib.error.HTTPError(request.full_url, 451, "Unknown", {}, io.BytesIO())

    def build_opener(*handlers):
        proxy_handlers = [handler for handler in handlers
                          if isinstance(handler, urllib.request.ProxyHandler)]
        assert len(proxy_handlers) == 1
        assert proxy_handlers[0].proxies == expected_proxy
        return BlockedOpener()

    monkeypatch.setattr(probe.urllib.request, "build_opener", build_opener)

    assert probe.main() == 2
    observed = json.loads(capsys.readouterr().out)
    assert {key: value for key, value in observed.items()
            if key not in {"request_started_at_utc", "response_observed_at_utc"}} == {
        "environment": "DEMO_ONLY",
        "endpoint": "demo-fapi.binance.com",
        "operation": "PUBLIC_SERVER_TIME_GET",
        "status": "HTTP_FAILURE",
        "http_status": 451,
        "account_payload_printed": False,
        "credentials_printed": False,
        "state_persisted": False,
        "orders_submitted": False,
        "signed_account_get_attempted": False,
    }
    assert observed["request_started_at_utc"].endswith("Z")
    assert observed["response_observed_at_utc"].endswith("Z")
    assert observed["request_started_at_utc"] <= observed["response_observed_at_utc"]


def test_demo_network_failure_class_is_sanitized(monkeypatch, capsys):
    monkeypatch.setenv("ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED", "YES")
    monkeypatch.setenv("ATLAS_BINANCE_FUTURES_ENV", "DEMO_ONLY")
    monkeypatch.setattr(
        probe.DemoOnlyCredentials, "from_environment",
        classmethod(lambda cls, env: object()),
    )
    monkeypatch.setattr(probe, "DemoOnlyAdapter", lambda **kwargs: _NeverSignedAdapter())

    class TimedOutOpener:
        def open(self, request, timeout):
            raise urllib.error.URLError(TimeoutError("sensitive-key-and-response"))

    monkeypatch.setattr(probe.urllib.request, "build_opener", lambda *handlers: TimedOutOpener())

    assert probe.main() == 2
    observed = json.loads(capsys.readouterr().out)
    assert observed["status"] == "NETWORK_FAILURE"
    assert observed["network_failure_class"] == "TimeoutError"
    assert observed["signed_account_get_attempted"] is False
    assert "sensitive-key-and-response" not in json.dumps(observed)


def test_exchange_info_summary_distinguishes_status_and_required_trade_filters():
    rows = [
        {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
         "quoteAsset": "USDT", "marginAsset": "USDT", "orderTypes": ["LIMIT", "MARKET"],
         "filters": [
             {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
             {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
             {"filterType": "MIN_NOTIONAL", "notional": "50"},
         ]},
        {"symbol": "ETHUSDT", "status": "SETTLING", "contractType": "PERPETUAL",
         "quoteAsset": "USDT", "marginAsset": "USDT", "filters": []},
        {"symbol": "ALTUSDT", "status": "TRADING", "contractType": "PERPETUAL",
         "quoteAsset": "USDT", "marginAsset": "USDT", "filters": [
             {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
         ]},
        {"symbol": "BTCUSD", "status": "TRADING", "contractType": "PERPETUAL",
         "quoteAsset": "USD", "marginAsset": "USD", "filters": []},
    ]

    result = probe._exchange_filter_summary(rows)

    assert result["status_counts"] == {"SETTLING": 1, "TRADING": 3}
    assert result["trading_usdt_perpetual_count"] == 2
    assert result["trading_usdt_perpetual_with_price_and_quantity_filters"] == 1
    assert result["trading_usdt_perpetual_with_minimum_notional_filter"] == 1
    assert result["sample_instruments"]["BTCUSDT"]["minimum_notional"] == "50"
    assert result["sample_instruments"]["ALTUSDT"]["quantity_filter_present"] is False


def test_successful_probe_reads_demo_clock_account_and_public_instrument_info(monkeypatch, capsys):
    monkeypatch.setenv("ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED", "YES")
    monkeypatch.setenv("ATLAS_BINANCE_FUTURES_ENV", "DEMO_ONLY")
    monkeypatch.setattr(
        probe.DemoOnlyCredentials, "from_environment",
        classmethod(lambda cls, env: object()),
    )
    monkeypatch.setattr(probe.time, "time_ns", lambda: 1_800_000_000_000_000_000)
    monkeypatch.setattr(probe, "DemoOnlyAdapter", lambda **kwargs: _FixtureSignedAdapter())
    responses = {
        "/fapi/v1/time": {"serverTime": 1_800_000_000_000},
        "/fapi/v3/account": {"assets": []},
        "/fapi/v1/exchangeInfo": {"symbols": [
            {"symbol": f"PAIR{index:03d}", "status": "TRADING", "detail": "x" * 900}
            for index in range(100)
        ]},
    }

    class Response:
        status = 200

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return "https://demo-fapi.binance.com" + self.path

        def read(self, limit):
            return io.BytesIO(json.dumps(responses[self.path]).encode()).read(limit)

    class SuccessfulOpener:
        requests = []

        def open(self, request, timeout):
            assert timeout == 5.0
            self.requests.append(request)
            return Response(urlsplit(request.full_url).path)

    opener = SuccessfulOpener()
    monkeypatch.setattr(probe.urllib.request, "build_opener", lambda *handlers: opener)

    assert probe.main() == 0
    record = json.loads(capsys.readouterr().out)
    assert [item.get_header("X-mbx-apikey") for item in opener.requests] == [None, "fixture-demo-key", None]
    assert [urlsplit(item.full_url).path for item in opener.requests] == [
        "/fapi/v1/time", "/fapi/v3/account", "/fapi/v1/exchangeInfo",
    ]
    assert record["request_count"] == 3
    assert record["instrument_symbol_count"] == 100
    assert record["orders_submitted"] is False
    assert "assets" not in json.dumps(record)
    assert "fixture-demo-key" not in json.dumps(record)
    assert all(item["request_started_at_utc"].endswith("Z")
               and item["response_observed_at_utc"].endswith("Z") for item in record["requests"])


def test_oversized_instrument_response_returns_sanitized_bounded_failure(monkeypatch, capsys):
    monkeypatch.setenv("ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED", "YES")
    monkeypatch.setenv("ATLAS_BINANCE_FUTURES_ENV", "DEMO_ONLY")
    monkeypatch.setattr(
        probe.DemoOnlyCredentials, "from_environment",
        classmethod(lambda cls, env: object()),
    )
    monkeypatch.setattr(probe.time, "time_ns", lambda: 1_800_000_000_000_000_000)
    monkeypatch.setattr(probe, "DemoOnlyAdapter", lambda **kwargs: _FixtureSignedAdapter())

    class Response:
        status = 200

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return "https://demo-fapi.binance.com" + self.path

        def read(self, limit):
            if self.path == "/fapi/v1/time":
                return json.dumps({"serverTime": 1_800_000_000_000}).encode()[:limit]
            if self.path == "/fapi/v3/account":
                return b'{"assets": []}'[:limit]
            return b"x" * limit

    class Opener:
        requests = []

        def open(self, request, timeout):
            self.requests.append(request)
            return Response(urlsplit(request.full_url).path)

    opener = Opener()
    monkeypatch.setattr(probe.urllib.request, "build_opener", lambda *handlers: opener)

    assert probe.main() == 2
    observed = json.loads(capsys.readouterr().out)
    assert observed["operation"] == "PUBLIC_INSTRUMENT_INFO_GET"
    assert observed["status"] == "RESPONSE_LIMIT_EXCEEDED"
    assert observed["response_body_bytes_read"] == probe.MAX_INSTRUMENT_RESPONSE_BYTES + 1
    assert observed["orders_submitted"] is False
    assert observed["account_payload_printed"] is False
    assert observed["credentials_printed"] is False
    assert "xxxxxxxx" not in json.dumps(observed)
    assert len(opener.requests) == 3


class _NeverSignedAdapter:
    def build_signed_read(self, *args, **kwargs):
        raise AssertionError("account GET must not follow a denied server-time request")


class _FixtureSignedAdapter:
    def build_signed_read(self, path, params):
        assert path == "/fapi/v3/account"
        assert params == {"recvWindow": 5000}
        return SimpleNamespace(
            full_url="https://demo-fapi.binance.com/fapi/v3/account?timestamp=1800000000000&signature=fixture",
            api_key="fixture-demo-key",
        )
