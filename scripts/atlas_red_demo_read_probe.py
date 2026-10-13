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

from atlas.runtime.binance_futures_demo_r4 import (
    BINANCE_FUTURES_DEMO_REST,
    DemoOnlyAdapter,
    DemoOnlyCredentials,
    DemoOnlyIdentity,
)

MAX_RESPONSE_BYTES = 65_536
MAX_CLOCK_SKEW_MS = 2_000


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _read(opener, request: urllib.request.Request) -> tuple[int, bytes, str]:
    with opener.open(request, timeout=5.0) as response:
        final = urllib.parse.urlsplit(response.geturl())
        if final.scheme != "https" or final.hostname != "demo-fapi.binance.com":
            raise RuntimeError("DEMO_ONLY_ENDPOINT_IDENTITY_FAILED")
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise RuntimeError("DEMO_ONLY_RESPONSE_LIMIT")
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
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    time_request = urllib.request.Request(
        BINANCE_FUTURES_DEMO_REST + "/fapi/v1/time", method="GET"
    )
    status, time_body, _ = _read(opener, time_request)
    if status != 200:
        raise RuntimeError("DEMO_ONLY_EXCHANGE_CLOCK_UNAVAILABLE")
    time_payload = json.loads(time_body)
    server_time_ms = time_payload.get("serverTime") if isinstance(time_payload, dict) else None
    local_time_ms = time.time_ns() // 1_000_000
    if type(server_time_ms) is not int or abs(local_time_ms - server_time_ms) > MAX_CLOCK_SKEW_MS:
        raise RuntimeError("DEMO_ONLY_EXCHANGE_CLOCK_SKEW")

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
    try:
        status, body, _ = _read(opener, request)
    except urllib.error.HTTPError as error:
        print(json.dumps({"environment": "DEMO_ONLY", "status": "HTTP_FAILURE", "http_status": error.code}))
        return 2
    if status != 200:
        print(json.dumps({"environment": "DEMO_ONLY", "status": "HTTP_FAILURE", "http_status": status}))
        return 2
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise RuntimeError("DEMO_ONLY_ACCOUNT_RESPONSE_INVALID")
    print(json.dumps({
        "environment": "DEMO_ONLY",
        "endpoint": "demo-fapi.binance.com",
        "operation": "SIGNED_READ_ONLY_ACCOUNT_GET",
        "status": "READ_OK",
        "exchange_clock_skew_ms": local_time_ms - server_time_ms,
        "response_bytes": len(body),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "account_payload_printed": False,
        "credentials_printed": False,
        "state_persisted": False,
        "orders_submitted": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
