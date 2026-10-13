#!/usr/bin/env python3
"""Trusted-runner, authenticated Demo GET probe. This module has no order path."""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime

from atlas.runtime.binance_futures_demo_r4 import (
    BINANCE_FUTURES_DEMO_REST,
    DemoOnlyAdapter,
    DemoOnlyCredentials,
    DemoOnlyIdentity,
)

MAX_RESPONSE_BYTES = 65_536
MAX_INSTRUMENT_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_CLOCK_SKEW_MS = 2_000


class _ResponseLimitExceeded(Exception):
    def __init__(self, bytes_read: int) -> None:
        self.bytes_read = bytes_read
        super().__init__("DEMO_ONLY_RESPONSE_LIMIT")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _failure(operation: str, status: str, *, request_started_at_utc: str,
             http_status: int | None = None, response_body_bytes: int | None = None) -> int:
    record = {
        "environment": "DEMO_ONLY",
        "endpoint": "demo-fapi.binance.com",
        "operation": operation,
        "status": status,
        "request_started_at_utc": request_started_at_utc,
        "response_observed_at_utc": _utc_timestamp(),
        "account_payload_printed": False,
        "credentials_printed": False,
        "state_persisted": False,
        "orders_submitted": False,
    }
    if http_status is not None:
        record["http_status"] = http_status
    if response_body_bytes is not None:
        record["response_body_bytes_read"] = response_body_bytes
    if operation == "PUBLIC_SERVER_TIME_GET":
        record["signed_account_get_attempted"] = False
    print(json.dumps(record, sort_keys=True))
    return 2


def _read(opener, request: urllib.request.Request, *, max_response_bytes: int = MAX_RESPONSE_BYTES
          ) -> tuple[int, bytes, str]:
    with opener.open(request, timeout=5.0) as response:
        final = urllib.parse.urlsplit(response.geturl())
        if final.scheme != "https" or final.hostname != "demo-fapi.binance.com":
            raise RuntimeError("DEMO_ONLY_ENDPOINT_IDENTITY_FAILED")
        body = response.read(max_response_bytes + 1)
        if len(body) > max_response_bytes:
            raise _ResponseLimitExceeded(len(body))
        return response.status, body, response.geturl()


def main() -> int:
    if os.environ.get("ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED") != "YES":
        raise SystemExit("Set ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED=YES after owner approval.")
    if os.environ.get("ATLAS_BINANCE_FUTURES_ENV") != "DEMO_ONLY":
        raise SystemExit("ATLAS_BINANCE_FUTURES_ENV must equal DEMO_ONLY.")

    credentials = DemoOnlyCredentials.from_environment(os.environ)
    identity = DemoOnlyIdentity("trusted-runner-demo", "trusted-runner-demo-credential")
    adapter = DemoOnlyAdapter(
        identity=identity,
        credentials=credentials,
        clock_ms=lambda: time.time_ns() // 1_000_000,
    )
    # Preserve the managed environment's configured proxy policy. An empty
    # ProxyHandler would bypass it and make this probe fail closed by policy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(), _NoRedirect())

    time_request = urllib.request.Request(
        BINANCE_FUTURES_DEMO_REST + "/fapi/v1/time", method="GET"
    )
    time_request_started_at = _utc_timestamp()
    try:
        status, time_body, _ = _read(opener, time_request)
    except urllib.error.HTTPError as error:
        return _failure("PUBLIC_SERVER_TIME_GET", "HTTP_FAILURE",
                        request_started_at_utc=time_request_started_at, http_status=error.code)
    except _ResponseLimitExceeded as error:
        return _failure("PUBLIC_SERVER_TIME_GET", "RESPONSE_LIMIT_EXCEEDED",
                        request_started_at_utc=time_request_started_at,
                        response_body_bytes=error.bytes_read)
    except (urllib.error.URLError, TimeoutError):
        return _failure("PUBLIC_SERVER_TIME_GET", "NETWORK_FAILURE",
                        request_started_at_utc=time_request_started_at)
    time_response_observed_at = _utc_timestamp()
    if status != 200:
        return _failure("PUBLIC_SERVER_TIME_GET", "HTTP_FAILURE",
                        request_started_at_utc=time_request_started_at, http_status=status,
                        response_body_bytes=len(time_body))
    try:
        time_payload = json.loads(time_body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _failure("PUBLIC_SERVER_TIME_GET", "INVALID_RESPONSE",
                        request_started_at_utc=time_request_started_at,
                        response_body_bytes=len(time_body))
    server_time_ms = time_payload.get("serverTime") if isinstance(time_payload, dict) else None
    local_time_ms = time.time_ns() // 1_000_000
    if type(server_time_ms) is not int or abs(local_time_ms - server_time_ms) > MAX_CLOCK_SKEW_MS:
        return _failure("PUBLIC_SERVER_TIME_GET", "CLOCK_VALIDATION_FAILED",
                        request_started_at_utc=time_request_started_at,
                        response_body_bytes=len(time_body))

    signed = adapter.build_signed_read("/fapi/v3/account", {"recvWindow": 5000})
    signed_url = urllib.parse.urlsplit(signed.full_url)
    if (signed_url.scheme != "https" or signed_url.hostname != "demo-fapi.binance.com"
            or signed_url.path != "/fapi/v3/account"):
        raise RuntimeError("DEMO_ONLY_ENDPOINT_IDENTITY_FAILED")
    request = urllib.request.Request(
        signed.full_url,
        headers={"X-MBX-APIKEY": signed.api_key, "Accept": "application/json"},
        method="GET",
    )
    account_request_started_at = _utc_timestamp()
    try:
        status, body, _ = _read(opener, request)
    except urllib.error.HTTPError as error:
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "HTTP_FAILURE",
                        request_started_at_utc=account_request_started_at, http_status=error.code)
    except _ResponseLimitExceeded as error:
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "RESPONSE_LIMIT_EXCEEDED",
                        request_started_at_utc=account_request_started_at,
                        response_body_bytes=error.bytes_read)
    except (urllib.error.URLError, TimeoutError):
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "NETWORK_FAILURE",
                        request_started_at_utc=account_request_started_at)
    account_response_observed_at = _utc_timestamp()
    if status != 200:
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "HTTP_FAILURE",
                        request_started_at_utc=account_request_started_at, http_status=status,
                        response_body_bytes=len(body))
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "INVALID_RESPONSE",
                        request_started_at_utc=account_request_started_at,
                        response_body_bytes=len(body))
    if not isinstance(payload, dict):
        return _failure("SIGNED_READ_ONLY_ACCOUNT_GET", "INVALID_RESPONSE",
                        request_started_at_utc=account_request_started_at,
                        response_body_bytes=len(body))

    instrument_request = urllib.request.Request(
        BINANCE_FUTURES_DEMO_REST + "/fapi/v1/exchangeInfo", method="GET",
        headers={"Accept": "application/json"},
    )
    instrument_request_started_at = _utc_timestamp()
    try:
        instrument_status, instrument_body, _ = _read(
            opener, instrument_request, max_response_bytes=MAX_INSTRUMENT_RESPONSE_BYTES,
        )
    except urllib.error.HTTPError as error:
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "HTTP_FAILURE",
                        request_started_at_utc=instrument_request_started_at,
                        http_status=error.code)
    except _ResponseLimitExceeded as error:
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "RESPONSE_LIMIT_EXCEEDED",
                        request_started_at_utc=instrument_request_started_at,
                        response_body_bytes=error.bytes_read)
    except (urllib.error.URLError, TimeoutError):
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "NETWORK_FAILURE",
                        request_started_at_utc=instrument_request_started_at)
    instrument_response_observed_at = _utc_timestamp()
    if instrument_status != 200:
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "HTTP_FAILURE",
                        request_started_at_utc=instrument_request_started_at,
                        http_status=instrument_status, response_body_bytes=len(instrument_body))
    try:
        instrument_payload = json.loads(instrument_body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "INVALID_RESPONSE",
                        request_started_at_utc=instrument_request_started_at,
                        response_body_bytes=len(instrument_body))
    symbols = instrument_payload.get("symbols") if isinstance(instrument_payload, dict) else None
    if not isinstance(symbols, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("symbol"), str) for item in symbols
    ):
        return _failure("PUBLIC_INSTRUMENT_INFO_GET", "INVALID_RESPONSE",
                        request_started_at_utc=instrument_request_started_at,
                        response_body_bytes=len(instrument_body))
    print(json.dumps({
        "environment": "DEMO_ONLY",
        "endpoint": "demo-fapi.binance.com",
        "operation": "BOUNDED_READ_ONLY_CONNECTIVITY_SEQUENCE",
        "authorization_reference": "owner-authorized Demo read-only sequence; process-local approval marker",
        "trade_plan_identity_sha256": None,
        "request_count": 3,
        "requests": [
            {"operation": "PUBLIC_SERVER_TIME_GET", "request_started_at_utc": time_request_started_at,
             "response_observed_at_utc": time_response_observed_at},
            {"operation": "SIGNED_READ_ONLY_ACCOUNT_GET", "request_started_at_utc": account_request_started_at,
             "response_observed_at_utc": account_response_observed_at},
            {"operation": "PUBLIC_INSTRUMENT_INFO_GET", "request_started_at_utc": instrument_request_started_at,
             "response_observed_at_utc": instrument_response_observed_at},
        ],
        "status": "READ_OK",
        "exchange_clock_skew_ms": local_time_ms - server_time_ms,
        "account_response_bytes": len(body),
        "account_response_sha256": hashlib.sha256(body).hexdigest(),
        "instrument_response_bytes": len(instrument_body),
        "instrument_response_sha256": hashlib.sha256(instrument_body).hexdigest(),
        "instrument_symbol_count": len(symbols),
        "account_payload_printed": False,
        "credentials_printed": False,
        "state_persisted": False,
        "orders_submitted": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
