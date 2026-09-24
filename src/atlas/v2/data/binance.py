"""Credential-free Binance USD-M public payload translation."""

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

TRANSLATION_VERSION = "binance-usdm-public-v1"
SOURCE_ID = "BINANCE_USDM_PUBLIC_HTTP"


def _ns_ms(value: Any) -> int:
    return int(value) * 1_000_000


def _optional_ns_ms(value: Any) -> int | None:
    if value in (None, "", 0, "0"):
        return None
    return _ns_ms(value)


def _object(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return value


def _require_binance_key(key: InstrumentKeyV2) -> None:
    if key.venue != VenueV2.BINANCE or key.product != ProductTypeV2.LINEAR_PERPETUAL:
        raise ValueError("Binance payload must bind a Binance linear-perpetual InstrumentKeyV2")


def translate_exchange_info(
    payload: Mapping[str, Any], *, environment: EnvironmentV2, observed_at_ns: int, available_at_ns: int
) -> tuple[ProductContractV2, ...]:
    symbols = payload.get("symbols")
    if not isinstance(symbols, list):
        raise ValueError("Binance exchangeInfo.symbols must be an array")
    result: list[ProductContractV2] = []
    for raw in symbols:
        row = _object(raw, name="exchangeInfo.symbol")
        if row.get("contractType") != "PERPETUAL" or row.get("quoteAsset") != "USDT":
            continue
        if row.get("marginAsset", "USDT") != "USDT":
            continue
        symbol = str(row.get("symbol", ""))
        base = str(row.get("baseAsset", ""))
        if not symbol or not base:
            raise ValueError("Binance contract metadata lacks symbol/baseAsset")
        filters_raw = row.get("filters")
        if not isinstance(filters_raw, list):
            raise ValueError("Binance symbol filters must be an array")
        filters = {str(item.get("filterType")): item for item in filters_raw if isinstance(item, Mapping)}
        price_filter = filters.get("PRICE_FILTER")
        lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE")
        if not isinstance(price_filter, Mapping) or not isinstance(lot_filter, Mapping):
            raise ValueError(f"Binance {symbol} lacks PRICE_FILTER or LOT_SIZE")
        minimum = filters.get("MIN_NOTIONAL", filters.get("NOTIONAL", {}))
        metadata_ref = sha256_json(dict(row))
        status = {
            "TRADING": TradingStatusV2.TRADING,
            "PENDING_TRADING": TradingStatusV2.PRELISTING,
            "SETTLING": TradingStatusV2.SUSPENDED,
            "CLOSE": TradingStatusV2.DELISTED,
            "PRE_DELIVERING": TradingStatusV2.SUSPENDED,
            "DELIVERING": TradingStatusV2.DELISTED,
        }.get(str(row.get("status")), TradingStatusV2.SUSPENDED)
        listing = row.get("onboardDate")
        delivery = row.get("deliveryDate")
        key = InstrumentKeyV2(
            VenueV2.BINANCE,
            EnvironmentV2(environment),
            ProductTypeV2.LINEAR_PERPETUAL,
            symbol,
            base,
            "USDT",
            "USDT",
            metadata_ref,
        )
        result.append(
            ProductContractV2(
                key=key,
                effective_at_ns=observed_at_ns,
                observed_at_ns=observed_at_ns,
                available_at_ns=available_at_ns,
                base_units_per_contract=Decimal("1"),
                tick_size=Decimal(str(price_filter["tickSize"])),
                qty_step=Decimal(str(lot_filter["stepSize"])),
                min_qty=Decimal(str(lot_filter["minQty"])),
                max_qty=Decimal(str(lot_filter["maxQty"])) if lot_filter.get("maxQty") else None,
                min_notional=Decimal(str(minimum["notional"])) if isinstance(minimum, Mapping) and minimum.get("notional") else None,
                trading_status=status,
                metadata_ref=metadata_ref,
                listing_at_ns=_optional_ns_ms(listing),
                delisting_at_ns=_optional_ns_ms(delivery),
            )
        )
    return tuple(sorted(result, key=lambda item: item.key.to_canonical_json()))


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
    _require_binance_key(key)
    if len(row) < 7:
        raise ValueError("Binance kline row must contain open time, OHLC, volume and close time")
    open_at_ns = _ns_ms(row[0])
    venue_close_at_ns = _ns_ms(row[6]) + 1_000_000
    interval_close_ns = close_boundary_ns(open_at_ns, interval)
    if venue_close_at_ns != interval_close_ns:
        raise ValueError("Binance kline close timestamp disagrees with requested interval")
    final = server_time_ns >= venue_close_at_ns
    values = {name: str(row[index]) for name, index in (("open", 1), ("high", 2), ("low", 3), ("close", 4), ("volume", 5))}
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type=f"BAR_{interval.value}",
        event_at_ns=interval_close_ns,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=max(received_at_ns, interval_close_ns) if final else received_at_ns,
        payload=list(row),
        translation_version=TRANSLATION_VERSION,
        sequence=str(open_at_ns),
        revision_of=revision_of,
        quality_flags=() if final else ("FORMING",),
    )
    return raw, values, open_at_ns, final


def translate_agg_trades(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_binance_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("s") not in (None, key.native_symbol):
            raise ValueError("Binance aggTrade symbol does not match canonical instrument")
        agg_id = row.get("a")
        if agg_id is None:
            raise ValueError("Binance aggTrade lacks aggregate trade identity")
        result.append(
            RawObservationV2.build(
                instrument_revision=key.contract_revision,
                source_id=source_id,
                event_type="AGG_TRADE",
                event_at_ns=_ns_ms(row["T"]),
                received_at_ns=received_at_ns,
                ingested_at_ns=received_at_ns,
                available_at_ns=received_at_ns,
                payload=row,
                translation_version=TRANSLATION_VERSION,
                sequence=str(agg_id),
            )
        )
    return tuple(result)


def translate_book_ticker(
    row: Mapping[str, Any], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> RawObservationV2:
    _require_binance_key(key)
    if row.get("symbol", row.get("s")) != key.native_symbol:
        raise ValueError("Binance bookTicker symbol does not match canonical instrument")
    update_id = row.get("u")
    return RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type="BOOK_TICKER",
        event_at_ns=_ns_ms(row["E"]) if row.get("E") is not None else received_at_ns,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=received_at_ns,
        payload=row,
        translation_version=TRANSLATION_VERSION,
        sequence=str(update_id) if update_id is not None else None,
    )


def translate_premium_index(
    row: Mapping[str, Any], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> RawObservationV2:
    _require_binance_key(key)
    if row.get("symbol") != key.native_symbol:
        raise ValueError("Binance premiumIndex symbol does not match canonical instrument")
    event_at = _ns_ms(row["time"]) if row.get("time") is not None else received_at_ns
    return RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type="MARK_INDEX_CURRENT_FUNDING",
        event_at_ns=event_at,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=received_at_ns,
        payload=row,
        translation_version=TRANSLATION_VERSION,
    )


def translate_funding_history(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_binance_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("symbol") != key.native_symbol:
            raise ValueError("Binance funding history symbol does not match canonical instrument")
        event_ns = _ns_ms(row["fundingTime"])
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


def translate_open_interest(
    row: Mapping[str, Any], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> RawObservationV2:
    _require_binance_key(key)
    if row.get("symbol") != key.native_symbol:
        raise ValueError("Binance openInterest symbol does not match canonical instrument")
    event_ns = _ns_ms(row["time"]) if row.get("time") is not None else received_at_ns
    return RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type="OPEN_INTEREST_CURRENT",
        event_at_ns=event_ns,
        received_at_ns=received_at_ns,
        ingested_at_ns=received_at_ns,
        available_at_ns=received_at_ns,
        payload=row,
        translation_version=TRANSLATION_VERSION,
    )


def translate_open_interest_history(
    rows: Sequence[Mapping[str, Any]], *, key: InstrumentKeyV2, received_at_ns: int, source_id: str = SOURCE_ID
) -> tuple[RawObservationV2, ...]:
    _require_binance_key(key)
    result: list[RawObservationV2] = []
    for row in rows:
        if row.get("symbol") != key.native_symbol:
            raise ValueError("Binance OI history symbol does not match canonical instrument")
        event_ns = _ns_ms(row["timestamp"])
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


class BinanceUsdMPublicReaderV2:
    """Strict credential-free USD-M public REST subset; no order/trading routes exist."""

    def __init__(self, client: PublicHttpClientV2 | None = None) -> None:
        self.client = client or PublicHttpClientV2(PublicVenueV2.BINANCE)
        if self.client.venue != PublicVenueV2.BINANCE:
            raise ValueError("BinanceUsdMPublicReaderV2 requires a Binance public HTTP client")

    def exchange_info(self) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/exchangeInfo")

    def klines(self, symbol: str, interval: BarIntervalV2, *, limit: int = 200) -> PublicHttpResponseV2:
        interval_text = {BarIntervalV2.M15: "15m", BarIntervalV2.H1: "1h", BarIntervalV2.H4: "4h"}[BarIntervalV2(interval)]
        return self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval_text, "limit": limit})

    def aggregate_trades(self, symbol: str, *, limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/aggTrades", {"symbol": symbol, "limit": limit})

    def book_ticker(self, symbol: str) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def premium_index(self, symbol: str) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/premiumIndex", {"symbol": symbol})

    def funding_history(self, symbol: str, *, limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def open_interest(self, symbol: str) -> PublicHttpResponseV2:
        return self.client.get("/fapi/v1/openInterest", {"symbol": symbol})

    def open_interest_history(self, symbol: str, *, period: str = "5m", limit: int = 100) -> PublicHttpResponseV2:
        return self.client.get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})

    def server_time_ns(self) -> int:
        return self.client.server_time_ns()
