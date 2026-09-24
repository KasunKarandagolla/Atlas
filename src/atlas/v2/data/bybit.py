"""Credential-free Bybit linear-perpetual public payload translation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

from .._serialization import sha256_json
from ..instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from .bars import BarIntervalV2, close_boundary_ns
from .public_http import PublicHttpClientV2, PublicHttpResponseV2, PublicVenueV2
from .raw import RawObservationV2

TRANSLATION_VERSION = "bybit-public-v1"
SOURCE_ID = "BYBIT_PUBLIC_HTTP"


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return value


def _require_bybit_key(key: InstrumentKeyV2) -> None:
    if key.venue != VenueV2.BYBIT or key.product != ProductTypeV2.LINEAR_PERPETUAL:
        raise ValueError("Bybit payload must bind a Bybit linear-perpetual InstrumentKeyV2")


def _ns_from_ms(value: Any) -> int:
    return int(value) * 1_000_000


def _optional_ns_from_ms(value: Any) -> int | None:
    if value in (None, "", 0, "0"):
        return None
    return _ns_from_ms(value)


def _result_rows(payload: Mapping[str, Any], *, name: str) -> list[Mapping[str, Any]]:
    if payload.get("retCode") != 0:
        raise ValueError(f"Bybit {name} returned retCode={payload.get('retCode')!r}")
    result = _mapping(payload.get("result"), name=f"{name}.result")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise ValueError(f"Bybit {name}.result.list must be an array")
    return [_mapping(row, name=f"{name}.row") for row in rows]


def translate_instrument_info(
    payload: Mapping[str, Any], *, environment: EnvironmentV2, observed_at_ns: int, available_at_ns: int
) -> tuple[ProductContractV2, ...]:
    """Translate USDT-settled linear perpetual metadata without ticker joins."""
    result: list[ProductContractV2] = []
    for row in _result_rows(payload, name="instrument-info"):
        if row.get("contractType") not in ("LinearPerpetual", "LinearPerpetualContract"):
            continue
        if row.get("quoteCoin") != "USDT" or row.get("settleCoin") != "USDT":
            continue
        symbol = str(row.get("symbol", ""))
        base = str(row.get("baseCoin", ""))
        if not symbol or not base:
            raise ValueError("Bybit instrument metadata lacks symbol/baseCoin")
        metadata_ref = sha256_json(dict(row))
        status = {
            "Trading": TradingStatusV2.TRADING,
            "PreLaunch": TradingStatusV2.PRELISTING,
            "Suspended": TradingStatusV2.SUSPENDED,
            "Settling": TradingStatusV2.DELISTED,
            "Delivering": TradingStatusV2.DELISTED,
        }.get(str(row.get("status")), TradingStatusV2.SUSPENDED)
        price_filter = _mapping(row.get("priceFilter", {}), name="priceFilter")
        lot_filter = _mapping(row.get("lotSizeFilter", {}), name="lotSizeFilter")
        listing = row.get("launchTime")
        delivery = row.get("deliveryTime")
        key = InstrumentKeyV2(
            VenueV2.BYBIT, EnvironmentV2(environment), ProductTypeV2.LINEAR_PERPETUAL,
            symbol, base, "USDT", "USDT", metadata_ref,
        )
        result.append(
            ProductContractV2(
                key=key,
                effective_at_ns=observed_at_ns,
                observed_at_ns=observed_at_ns,
                available_at_ns=available_at_ns,
                base_units_per_contract=Decimal("1"),
                tick_size=Decimal(str(price_filter["tickSize"])),
                qty_step=Decimal(str(lot_filter["qtyStep"])),
                min_qty=Decimal(str(lot_filter.get("minOrderQty", "0"))),
                min_notional=Decimal(str(lot_filter["minNotionalValue"])) if lot_filter.get("minNotionalValue") not in (None, "") else None,
                max_qty=Decimal(str(lot_filter["maxOrderQty"])) if lot_filter.get("maxOrderQty") not in (None, "") else None,
                trading_status=status,
                metadata_ref=metadata_ref,
                listing_at_ns=_optional_ns_from_ms(listing),
                delisting_at_ns=_optional_ns_from_ms(delivery),
            )
        )
    return tuple(sorted(result, key=lambda item: item.key.to_canonical_json()))


def translate_ticker(
    row: Mapping[str, Any], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> RawObservationV2:
    _require_bybit_key(key)
    if row.get("symbol") != key.native_symbol:
        raise ValueError("Bybit ticker symbol does not match canonical InstrumentKeyV2")
    event_at = _ns_from_ms(row["ts"]) if row.get("ts") is not None else received_at_ns
    return RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type="TICKER_MARK_INDEX_FUNDING_OI",
        event_at_ns=event_at,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=max(received_at_ns, event_at),
        payload=row,
        translation_version=TRANSLATION_VERSION,
    )


def translate_kline(
    row: Sequence[Any],
    *,
    key: InstrumentKeyV2,
    interval: BarIntervalV2,
    received_at_ns: int,
    server_time_ns: int,
    source_id: str = SOURCE_ID,
    revision_of: str | None = None,
) -> tuple[RawObservationV2, dict[str, str], int, bool]:
    _require_bybit_key(key)
    if len(row) < 7:
        raise ValueError("Bybit kline row must contain start, OHLC, volume and turnover")
    open_at_ns = _ns_from_ms(row[0])
    close_at_ns = close_boundary_ns(open_at_ns, interval)
    # REST klines have no final flag; a bar is final only after its exclusive close.
    final = server_time_ns >= close_at_ns
    values = {name: str(row[index]) for name, index in (("open", 1), ("high", 2), ("low", 3), ("close", 4), ("volume", 5))}
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type=f"BAR_{interval.value}",
        event_at_ns=close_at_ns,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=max(received_at_ns, close_at_ns) if final else received_at_ns,
        payload=list(row),
        translation_version=TRANSLATION_VERSION,
        sequence=str(open_at_ns),
        revision_of=revision_of,
        quality_flags=() if final else ("FORMING",),
    )
    return raw, values, open_at_ns, final


def translate_recent_trades(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_bybit_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("symbol") not in (None, key.native_symbol):
            raise ValueError("Bybit trade symbol does not match canonical instrument")
        identity = row.get("execId", row.get("i"))
        if identity is None:
            raise ValueError("Bybit trade lacks a stable execution identity")
        result.append(
            RawObservationV2.build(
                instrument_revision=key.contract_revision,
                source_id=source_id,
                event_type="TRADE",
                event_at_ns=_ns_from_ms(row.get("time", row.get("T"))),
                received_at_ns=received_at_ns,
                ingested_at_ns=received_at_ns,
                available_at_ns=received_at_ns,
                payload=row,
                translation_version=TRANSLATION_VERSION,
                sequence=str(identity),
            )
        )
    return tuple(result)


def translate_funding_history(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_bybit_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("symbol") not in (None, key.native_symbol):
            raise ValueError("Bybit funding symbol does not match canonical instrument")
        event_ns = _ns_from_ms(row.get("fundingRateTimestamp", row.get("fundingTime")))
        result.append(
            RawObservationV2.build(
                instrument_revision=key.contract_revision,
                source_id=source_id,
                event_type="FUNDING_HISTORY",
                event_at_ns=event_ns,
                received_at_ns=received_at_ns,
                ingested_at_ns=received_at_ns,
                available_at_ns=received_at_ns,
                payload=row,
                translation_version=TRANSLATION_VERSION,
                sequence=str(event_ns),
            )
        )
    return tuple(result)


def translate_open_interest_history(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_bybit_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("symbol") not in (None, key.native_symbol):
            raise ValueError("Bybit OI symbol does not match canonical instrument")
        event_ns = _ns_from_ms(row.get("timestamp", row.get("ts")))
        result.append(
            RawObservationV2.build(
                instrument_revision=key.contract_revision,
                source_id=source_id,
                event_type="OPEN_INTEREST_HISTORY",
                event_at_ns=event_ns,
                received_at_ns=received_at_ns,
                ingested_at_ns=received_at_ns,
                available_at_ns=received_at_ns,
                payload=row,
                translation_version=TRANSLATION_VERSION,
                sequence=str(event_ns),
                quality_flags=("PUBLICATION_TIME_UNKNOWN",),
            )
        )
    return tuple(result)


def translate_ticker_fields(row: Mapping[str, Any]) -> dict[str, str | None]:
    """Preserve available current mark/index/funding/OI values with explicit absence."""
    return {
        field: str(row[field]) if row.get(field) not in (None, "") else None
        for field in ("bid1Price", "ask1Price", "bid1Size", "ask1Size", "markPrice", "indexPrice", "fundingRate", "nextFundingTime", "openInterest")
    }


class BybitPublicReaderV2:
    """Strict read-only V5 subset. No credential, account, or mutation methods are exposed."""

    def __init__(self, client: PublicHttpClientV2 | None = None) -> None:
        self.client = client or PublicHttpClientV2(PublicVenueV2.BYBIT)
        if self.client.venue != PublicVenueV2.BYBIT:
            raise ValueError("BybitPublicReaderV2 requires a Bybit public HTTP client")

    def instruments(self, *, limit: int = 1000, cursor: str | None = None) -> PublicHttpResponseV2:
        params: dict[str, str | int] = {"category": "linear", "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self.client.get("/v5/market/instruments-info", params)

    def klines(self, symbol: str, interval: BarIntervalV2, *, limit: int = 200) -> PublicHttpResponseV2:
        interval_text = {BarIntervalV2.M15: "15", BarIntervalV2.H1: "60", BarIntervalV2.H4: "240"}[BarIntervalV2(interval)]
        return self.client.get("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": interval_text, "limit": limit})

    def recent_trades(self, symbol: str, *, limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get("/v5/market/recent-trade", {"category": "linear", "symbol": symbol, "limit": limit})

    def ticker(self, symbol: str) -> PublicHttpResponseV2:
        return self.client.get("/v5/market/tickers", {"category": "linear", "symbol": symbol})

    def funding_history(self, symbol: str, *, limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get("/v5/market/funding/history", {"category": "linear", "symbol": symbol, "limit": limit})

    def open_interest_history(self, symbol: str, *, interval: str = "5min", limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get(
            "/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": limit}
        )

    def server_time_ns(self) -> int:
        return self.client.server_time_ns()
