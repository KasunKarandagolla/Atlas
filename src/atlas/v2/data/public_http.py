"""Minimal credential-free public HTTP transport with a strict endpoint allowlist."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class PublicVenueV2(StrEnum):
    BYBIT = "BYBIT"
    BINANCE = "BINANCE"


_BASE_URLS = {
    PublicVenueV2.BYBIT: "https://api.bybit.com",
    PublicVenueV2.BINANCE: "https://fapi.binance.com",
}
_PATHS = {
    PublicVenueV2.BYBIT: frozenset(
        {
            "/v5/market/time",
            "/v5/market/instruments-info",
            "/v5/market/kline",
            "/v5/market/recent-trade",
            "/v5/market/tickers",
            "/v5/market/funding/history",
            "/v5/market/open-interest",
        }
    ),
    PublicVenueV2.BINANCE: frozenset(
        {
            "/fapi/v1/time",
            "/fapi/v1/exchangeInfo",
            "/fapi/v1/klines",
            "/fapi/v1/aggTrades",
            "/fapi/v1/ticker/bookTicker",
            "/fapi/v1/premiumIndex",
            "/fapi/v1/fundingRate",
            "/fapi/v1/openInterest",
            "/futures/data/openInterestHist",
        }
    ),
}
_QUERY_FIELDS = {
    "/v5/market/time": frozenset(),
    "/v5/market/instruments-info": frozenset({"category", "limit", "cursor"}),
    "/v5/market/kline": frozenset({"category", "symbol", "interval", "limit", "start", "end"}),
    "/v5/market/recent-trade": frozenset({"category", "symbol", "limit"}),
    "/v5/market/tickers": frozenset({"category", "symbol"}),
    "/v5/market/funding/history": frozenset({"category", "symbol", "limit", "startTime", "endTime"}),
    "/v5/market/open-interest": frozenset({"category", "symbol", "intervalTime", "limit", "startTime", "endTime"}),
    "/fapi/v1/time": frozenset(),
    "/fapi/v1/exchangeInfo": frozenset(),
    "/fapi/v1/klines": frozenset({"symbol", "interval", "limit", "startTime", "endTime"}),
    "/fapi/v1/aggTrades": frozenset({"symbol", "fromId", "startTime", "endTime", "limit"}),
    "/fapi/v1/ticker/bookTicker": frozenset({"symbol"}),
    "/fapi/v1/premiumIndex": frozenset({"symbol"}),
    "/fapi/v1/fundingRate": frozenset({"symbol", "startTime", "endTime", "limit"}),
    "/fapi/v1/openInterest": frozenset({"symbol"}),
    "/futures/data/openInterestHist": frozenset({"symbol", "period", "limit", "startTime", "endTime"}),
}


class PublicDataError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

    @property
    def rate_limited(self) -> bool:
        return self.status_code == 429


@dataclass(frozen=True)
class PublicHttpResponseV2:
    venue: PublicVenueV2
    path: str
    payload: Any
    raw_body: bytes
    received_at_ns: int
    status_code: int


class HttpGetter(Protocol):
    def __call__(self, url: str, timeout: float) -> tuple[int, bytes]: ...


def _stdlib_get(url: str, timeout: float) -> tuple[int, bytes]:
    request = Request(url, method="GET", headers={"Accept": "application/json", "User-Agent": "ATLAS-V2-public-research/1"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(2_000_001)
    except HTTPError as exc:
        body = exc.read(16_384)
        raise PublicDataError(f"public endpoint returned HTTP {exc.code}: {body[:256]!r}", status_code=exc.code) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise PublicDataError(f"credential-free public request failed: {type(exc).__name__}") from exc


class PublicHttpClientV2:
    """GET-only reader for explicitly public venue endpoints; no auth surface exists."""

    def __init__(
        self,
        venue: PublicVenueV2,
        *,
        timeout_s: float = 5.0,
        clock_ns: Callable[[], int] = time.time_ns,
        getter: HttpGetter = _stdlib_get,
    ) -> None:
        self.venue = PublicVenueV2(venue)
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        self.timeout_s = timeout_s
        self.clock_ns = clock_ns
        self.getter = getter

    def get(self, path: str, params: Mapping[str, str | int] | None = None) -> PublicHttpResponseV2:
        if path not in _PATHS[self.venue]:
            raise ValueError(f"endpoint is not in the {self.venue.value} public GET allowlist")
        unknown_params = set(params or ()) - _QUERY_FIELDS[path]
        if unknown_params:
            raise ValueError(f"query parameters are not in the public endpoint allowlist: {sorted(unknown_params)}")
        query = urlencode(sorted((str(key), str(value)) for key, value in (params or {}).items()))
        url = _BASE_URLS[self.venue] + path + (f"?{query}" if query else "")
        try:
            status, body = self.getter(url, self.timeout_s)
        except PublicDataError:
            raise
        if status < 200 or status >= 300:
            raise PublicDataError(f"public endpoint returned HTTP {status}", status_code=status)
        if len(body) > 2_000_000:
            raise PublicDataError("public response exceeds the bounded 2 MB response limit")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PublicDataError("public endpoint returned malformed JSON") from exc
        return PublicHttpResponseV2(self.venue, path, payload, body, self.clock_ns(), status)

    def server_time_ns(self) -> int:
        path = "/v5/market/time" if self.venue == PublicVenueV2.BYBIT else "/fapi/v1/time"
        payload = self.get(path).payload
        if self.venue == PublicVenueV2.BYBIT:
            if not isinstance(payload, Mapping) or payload.get("retCode") != 0:
                raise PublicDataError("Bybit server time response is not successful")
            result = payload.get("result")
            if isinstance(result, Mapping) and result.get("timeNano") is not None:
                return int(result["timeNano"])
            if isinstance(result, Mapping) and result.get("timeSecond") is not None:
                return int(result["timeSecond"]) * 1_000_000_000
            raise PublicDataError("Bybit server time response lacks a recognized timestamp")
        if not isinstance(payload, Mapping) or payload.get("serverTime") is None:
            raise PublicDataError("Binance server time response lacks serverTime")
        return int(payload["serverTime"]) * 1_000_000
