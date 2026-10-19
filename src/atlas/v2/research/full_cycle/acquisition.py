"""Bounded credential-free public-market acquisition for ATLAS RED research.

The pipeline reuses the production owners: `PublicHttpClientV2` (allowlisted,
proxy-free, credential-free transport), `BinanceUsdMPublicReaderV2` (typed
endpoints) and `atlas.v2.data.binance` translators (canonical observation
records). It adds only what the repository does not own: an explicit request
budget, a fair-rotation deep cohort, and durable research projection writes.

It never places an order, never reads a private endpoint and never mutates an
account. The full ATLAS ten-second mandatory observer is NOT qualified on this
host (see the G1 storage preflight gate); this pipeline is an explicitly
limited, labelled research probe.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ...data.bars import BarIntervalV2
from ...data.binance import SOURCE_ID, BinanceUsdMPublicReaderV2, translate_exchange_info
from ...data.public_http import (
    PublicDataError,
    PublicHttpClientV2,
    PublicHttpsSessionV1,
    PublicVenueV2,
)
from ...instruments import EnvironmentV2
from ..full_cycle.store import ResearchStoreV1, compress_payload

ACQUISITION_VERSION = "ATLAS_RED_FULL_CYCLE_ACQUISITION_V1"

# Binance USD-M documented request weights (2400 weight/minute per IP).
ENDPOINT_WEIGHT: Mapping[str, int] = {
    "/fapi/v1/time": 1,
    "/fapi/v1/exchangeInfo": 1,
    "/fapi/v1/ticker/24hr": 40,
    "/fapi/v1/premiumIndex": 10,
    "/fapi/v1/ticker/bookTicker": 5,
    "/fapi/v1/klines": 2,
    "/fapi/v1/aggTrades": 1,
    "/fapi/v1/depth": 5,
    "/fapi/v1/openInterest": 1,
    "/fapi/v1/fundingRate": 1,
}


class RequestBudgetExceeded(RuntimeError):
    """Raised when the registered public request budget for the window is spent."""


@dataclass
class RequestBudgetV1:
    """Explicit per-minute public request budget (the repository owns no quota module)."""

    weight_per_minute: int = 1800
    _window_start: float = field(default_factory=time.monotonic)
    _used: int = 0

    def _roll(self) -> None:
        now = time.monotonic()
        if now - self._window_start >= 60.0:
            self._window_start = now
            self._used = 0

    def consume(self, endpoint: str, weight: int | None = None) -> None:
        self._roll()
        cost = ENDPOINT_WEIGHT.get(endpoint, 1) if weight is None else weight
        if self._used + cost > self.weight_per_minute:
            raise RequestBudgetExceeded(
                f"public request budget exhausted: used={self._used} cost={cost}")
        self._used += cost

    @property
    def used(self) -> int:
        self._roll()
        return self._used


@dataclass(frozen=True)
class CohortSelectionV1:
    cycle_id: str
    symbols: tuple[str, ...]
    selection_rule: str
    selection_probability: float
    universe_size: int
    deep_population_size: int


@dataclass
class AcquisitionStatsV1:
    requests: int = 0
    failures: int = 0
    bytes_received: int = 0
    observations: int = 0
    bars: int = 0
    products: int = 0
    missingness: list[dict[str, Any]] = field(default_factory=list)


class BoundedPublicResearchAcquisitionV1:
    """One bounded research acquisition cycle over the Binance public REST surface."""

    def __init__(
        self,
        store: ResearchStoreV1,
        *,
        client: PublicHttpClientV2 | None = None,
        environment: EnvironmentV2 = EnvironmentV2.MAINNET,
        budget: RequestBudgetV1 | None = None,
        kline_limit: int = 500,
        deep_symbols_per_cycle: int = 24,
        rotation_offset: int = 0,
        clock_ns=time.time_ns,
    ) -> None:
        self.store = store
        self.client = client or PublicHttpClientV2(
            PublicVenueV2.BINANCE, timeout_s=15.0,
            getter=PublicHttpsSessionV1(PublicVenueV2.BINANCE))
        self.reader = BinanceUsdMPublicReaderV2(self.client)
        self.environment = environment
        self.budget = budget or RequestBudgetV1()
        self.kline_limit = kline_limit
        self.deep_symbols_per_cycle = deep_symbols_per_cycle
        self.rotation_offset = rotation_offset
        self.clock_ns = clock_ns

    # -- transport ----------------------------------------------------
    def _get(self, path: str, params: Mapping[str, Any] | None, *, symbol: str | None,
             event_type: str) -> tuple[Any, bytes, int, str, int]:
        """One allowlisted public GET with a recorded raw receipt."""

        self.budget.consume(path)
        try:
            response = self.client.get(path, dict(params or {}))
        except PublicDataError:
            raise
        return response.payload, response.raw_body, response.received_at_ns, path, response.status_code

    @staticmethod
    def _request_hash(path: str, params: Mapping[str, Any]) -> str:
        canonical = json.dumps({"path": path, "params": dict(sorted(params.items()))},
                               sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _record(self, *, cycle_id: str, path: str, params: Mapping[str, Any], symbol: str | None,
                event_type: str, payload: Any, raw_body: bytes, received_at_ns: int,
                http_status: int, availability_class: str = "ACTUAL_SYSTEM",
                event_at_ns: int | None = None, published_at_ns: int | None = None,
                sequence: str | None = None, quality_flags: Sequence[str] = (),
                error_kind: str | None = None) -> str:
        payload_hash = hashlib.sha256(raw_body).hexdigest()
        observation_id = hashlib.sha256(
            json.dumps({
                "cycle": cycle_id,
                "endpoint": path,
                "symbol": symbol,
                "event_type": event_type,
                "payload": payload_hash,
                "received_at_ns": received_at_ns,
            }, sort_keys=True).encode("utf-8")).hexdigest()
        self.store.record_observation({
            "observation_id": observation_id,
            "cycle_id": cycle_id,
            "venue": "BINANCE",
            "source_id": SOURCE_ID,
            "endpoint": path,
            "instrument_symbol": symbol,
            "event_type": event_type,
            "request_sha256": self._request_hash(path, params),
            "payload_sha256": payload_hash,
            "payload_bytes": len(raw_body),
            "payload_zlib": compress_payload(raw_body),
            "event_at_ns": event_at_ns,
            "published_at_ns": published_at_ns,
            "received_at_ns": received_at_ns,
            "available_at_ns": received_at_ns,
            "http_status": http_status,
            "availability_class": availability_class,
            "sequence": sequence,
            "quality_flags": list(quality_flags),
            "error_kind": error_kind,
        })
        return observation_id

    # -- cycle steps --------------------------------------------------
    def fetch_server_time(self, cycle_id: str) -> int:
        payload, body, received, path, status = self._get("/fapi/v1/time", {}, symbol=None,
                                                           event_type="SERVER_TIME")
        self._record(cycle_id=cycle_id, path=path, params={}, symbol=None,
                     event_type="SERVER_TIME", payload=payload, raw_body=body,
                     received_at_ns=received, http_status=status, event_at_ns=int(payload["serverTime"]) * 1_000_000)
        return int(payload["serverTime"]) * 1_000_000

    def fetch_products(self, cycle_id: str, *, server_time_ns: int) -> list[dict[str, Any]]:
        payload, body, received, path, status = self._get("/fapi/v1/exchangeInfo", {}, symbol=None,
                                                           event_type="EXCHANGE_INFO")
        self._record(cycle_id=cycle_id, path=path, params={}, symbol=None, event_type="EXCHANGE_INFO",
                     payload=payload, raw_body=body, received_at_ns=received, http_status=status,
                     event_at_ns=server_time_ns)
        contracts = translate_exchange_info(
            payload, environment=self.environment, observed_at_ns=server_time_ns, available_at_ns=received)
        rows: list[dict[str, Any]] = []
        for contract in contracts:
            rows.append({
                "product_ref": contract.key.content_hash,
                "observed_at_ns": contract.observed_at_ns,
                "cycle_id": cycle_id,
                "venue": "BINANCE",
                "symbol": contract.key.native_symbol,
                "base_asset": contract.key.base_asset_id,
                "quote_asset": contract.key.quote_asset,
                "contract_type": contract.key.product.value,
                "trading_status": contract.trading_status.value,
                "tick_size": str(contract.tick_size),
                "qty_step": str(contract.qty_step),
                "min_qty": str(contract.min_qty),
                "max_qty": None if contract.max_qty is None else str(contract.max_qty),
                "min_notional": None if contract.min_notional is None else str(contract.min_notional),
                "listing_at_ns": contract.listing_at_ns,
                "delisting_at_ns": contract.delisting_at_ns,
                "metadata_ref": contract.metadata_ref,
            })
        self.store.record_products(rows)
        return rows

    def fetch_ticker_24h(self, cycle_id: str) -> dict[str, dict[str, Any]]:
        payload, body, received, path, status = self._get("/fapi/v1/ticker/24hr", {}, symbol=None,
                                                           event_type="TICKER_24H")
        self._record(cycle_id=cycle_id, path=path, params={}, symbol=None, event_type="TICKER_24H",
                     payload=payload, raw_body=body, received_at_ns=received, http_status=status)
        result: dict[str, dict[str, Any]] = {}
        if not isinstance(payload, list):
            return result
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            result[symbol] = {
                "last_price": row.get("lastPrice"),
                "price_change_pct_24h": row.get("priceChangePercent"),
                "quote_volume_24h_usd": row.get("quoteVolume"),
                "volume_24h": row.get("volume"),
                "trades_24h": row.get("count"),
                "high_24h": row.get("highPrice"),
                "low_24h": row.get("lowPrice"),
            }
        return result

    def fetch_premium_index(self, cycle_id: str) -> dict[str, dict[str, Any]]:
        payload, body, received, path, status = self._get("/fapi/v1/premiumIndex", {}, symbol=None,
                                                           event_type="PREMIUM_INDEX")
        self._record(cycle_id=cycle_id, path=path, params={}, symbol=None, event_type="PREMIUM_INDEX",
                     payload=payload, raw_body=body, received_at_ns=received, http_status=status)
        result: dict[str, dict[str, Any]] = {}
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            next_funding = row.get("nextFundingTime")
            result[symbol] = {
                "mark_price": row.get("markPrice"),
                "index_price": row.get("indexPrice"),
                "funding_rate": row.get("lastFundingRate"),
                "next_funding_at_ns": int(next_funding) * 1_000_000 if next_funding else None,
            }
        return result

    def fetch_book_ticker(self, cycle_id: str) -> dict[str, dict[str, Any]]:
        payload, body, received, path, status = self._get("/fapi/v1/ticker/bookTicker", {}, symbol=None,
                                                           event_type="BOOK_TICKER")
        self._record(cycle_id=cycle_id, path=path, params={}, symbol=None, event_type="BOOK_TICKER",
                     payload=payload, raw_body=body, received_at_ns=received, http_status=status)
        result: dict[str, dict[str, Any]] = {}
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            try:
                bid = Decimal(str(row.get("bidPrice")))
                ask = Decimal(str(row.get("askPrice")))
                mid = (bid + ask) / 2
                spread_bps = (ask - bid) / mid * Decimal(10_000) if mid > 0 else None
            except Exception:
                bid = ask = mid = spread_bps = None
            result[symbol] = {
                "bid_price": str(bid) if bid is not None else None,
                "ask_price": str(ask) if ask is not None else None,
                "mid_price": str(mid) if mid is not None else None,
                "spread_bps": str(spread_bps) if spread_bps is not None else None,
            }
        return result

    def fetch_klines(self, cycle_id: str, symbol: str, interval: BarIntervalV2, *,
                     server_time_ns: int, limit: int | None = None,
                     end_time_ms: int | None = None) -> list[dict[str, Any]]:
        text = {
            BarIntervalV2.M1: "1m", BarIntervalV2.M5: "5m", BarIntervalV2.M15: "15m",
            BarIntervalV2.H1: "1h", BarIntervalV2.H4: "4h",
        }[interval]
        count = int(limit if limit is not None else self.kline_limit)
        params: dict[str, Any] = {"symbol": symbol, "interval": text, "limit": count}
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        payload, body, received, path, status = self._get(path="/fapi/v1/klines", params=params, symbol=symbol,
                                                           event_type=f"BAR_{interval.value}")
        observation_id = self._record(cycle_id=cycle_id, path=path, params=params, symbol=symbol,
                                      event_type=f"BAR_{interval.value}", payload=payload, raw_body=body,
                                      received_at_ns=received, http_status=status)
        if not isinstance(payload, list):
            return []
        rows: list[dict[str, Any]] = []
        for raw_row in payload:
            if not isinstance(raw_row, (list, tuple)) or len(raw_row) < 7:
                continue
            open_at_ns = int(raw_row[0]) * 1_000_000
            close_at_ns = int(raw_row[6]) * 1_000_000 + 1_000_000
            if server_time_ns < close_at_ns:
                continue  # forming bar is not a policy-ready observation
            bar_id = hashlib.sha256(
                f"{symbol}|{interval.value}|{open_at_ns}".encode()).hexdigest()
            rows.append({
                "bar_id": bar_id,
                "cycle_id": cycle_id,
                "symbol": symbol,
                "interval": interval.value,
                "open_at_ns": open_at_ns,
                "close_at_ns": close_at_ns,
                "open": str(raw_row[1]),
                "high": str(raw_row[2]),
                "low": str(raw_row[3]),
                "close": str(raw_row[4]),
                "volume": str(raw_row[5]),
                "quote_volume": str(raw_row[7]) if len(raw_row) > 7 else None,
                "trades": int(raw_row[8]) if len(raw_row) > 8 else None,
                "available_at_ns": max(received, close_at_ns),
                "received_at_ns": received,
                "raw_row_json": json.dumps(list(raw_row), separators=(",", ":")),
                "source_observation_id": observation_id,
            })
        self.store.record_bars(rows)
        return rows

    def fetch_deep_source(self, cycle_id: str, symbol: str, kind: str) -> dict[str, Any] | None:
        """One bounded deep source receipt: depth, aggTrades, OI or funding history."""

        spec = {
            "DEPTH": ("/fapi/v1/depth", {"symbol": symbol, "limit": 100}),
            "AGG_TRADES": ("/fapi/v1/aggTrades", {"symbol": symbol, "limit": 100}),
            "OPEN_INTEREST": ("/fapi/v1/openInterest", {"symbol": symbol}),
            "FUNDING_HISTORY": ("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 20}),
        }[kind]
        path, params = spec
        try:
            payload, body, received, _, status = self._get(path=path, params=params, symbol=symbol,
                                                            event_type=kind)
        except PublicDataError as exc:
            self.store.record_missingness({
                "missing_id": hashlib.sha256(f"{cycle_id}|{symbol}|{kind}".encode()).hexdigest(),
                "cycle_id": cycle_id, "symbol": symbol, "scope": "DEEP_SOURCE",
                "reason_code": "SOURCE_UNAVAILABLE", "detail": {"kind": kind, "error": str(exc)},
                "recorded_at_ns": self.clock_ns(),
            })
            return None
        self._record(cycle_id=cycle_id, path=path, params=params, symbol=symbol, event_type=kind,
                     payload=payload, raw_body=body, received_at_ns=received, http_status=status)
        return payload if isinstance(payload, (list, dict)) else None
