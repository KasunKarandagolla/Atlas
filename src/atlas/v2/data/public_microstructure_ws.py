"""Narrow credential-free public WebSocket capture and venue translators.

No authentication, account, order, or capital-control endpoint is present.
Live feed qualification remains a separate gate.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast
from urllib.parse import urlencode

from .._serialization import sha256_ref, timestamp
from ..instruments import InstrumentKeyV2, VenueV2
from .capabilities import capability_for_public_channel_v2, default_evidence_capability_matrix_v2
from .derivatives import DerivativeAvailabilityV2, LiquidationCoverageV2, LiquidationObservationV2
from .microstructure import AggressiveTradeV2, BookLevelV2, L2DeltaV2, L2SequenceFaultV2, L2SnapshotV2
from .microstructure_archive import L2RawFrameV2

MAX_PUBLIC_FRAME_BYTES = 2_000_000
_BYBIT_WS_HOST = "stream.bybit.com"
_BINANCE_WS_HOST = "fstream.binance.com"


@dataclass(frozen=True)
class CapturedPublicFrameV2:
    venue: VenueV2
    source_id: str
    channel: str
    raw_payload_bytes: bytes
    raw_payload_hash: str
    received_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", VenueV2(self.venue))
        if not self.source_id or not self.channel or not isinstance(self.raw_payload_bytes, bytes):
            raise ValueError("captured public frame identity and exact bytes are required")
        sha256_ref(self.raw_payload_hash, field="raw_payload_hash")
        if hashlib.sha256(self.raw_payload_bytes).hexdigest() != self.raw_payload_hash:
            raise ValueError("captured public frame hash does not match exact bytes")
        timestamp(self.received_at_ns, field="received_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("frame availability cannot precede receipt")


def _topic_allowed(venue: VenueV2, topic: str) -> bool:
    if venue == VenueV2.BYBIT:
        return topic.startswith(("orderbook.50.", "publicTrade.", "allLiquidation."))
    return _binance_route(topic) is not None


def _binance_route(topic: str) -> str | None:
    if topic.endswith(("@depth@100ms", "@depth@250ms")):
        return "public"
    if topic.endswith("@aggTrade"):
        return "market"
    return None


def _venue_url(venue: VenueV2, topics: tuple[str, ...]) -> str:
    if not topics or any(not _topic_allowed(venue, topic) for topic in topics):
        raise ValueError("only allowlisted public L2/trade/liquidation channels are accepted")
    if venue == VenueV2.BYBIT:
        return "wss://stream.bybit.com/v5/public/linear"
    routes = {_binance_route(topic) for topic in topics}
    if len(routes) != 1:
        raise ValueError("Binance public depth and market trades require separate WebSocket routes")
    route = next(iter(routes))
    return f"wss://fstream.binance.com/{route}/stream?" + urlencode({"streams": "/".join(topics)})


async def capture_public_frames(*, venue: VenueV2, topics: tuple[str, ...],
                                source_id: str | None = None,
                                clock_ns: Callable[[], int] = time.time_ns) -> AsyncIterator[CapturedPublicFrameV2]:
    """Capture raw frames from one allowlisted public venue socket, with receipt timestamps.

    The function requires ``websockets==17.0.1`` from the lock. It never accepts
    headers, tokens or authentication payloads.
    """
    venue = VenueV2(venue)
    url = _venue_url(venue, topics)
    expected_host = _BYBIT_WS_HOST if venue == VenueV2.BYBIT else _BINANCE_WS_HOST
    if expected_host not in url:
        raise ValueError("public WebSocket endpoint escaped venue host allowlist")
    try:
        from websockets.asyncio.client import connect
    except ImportError as exc:  # pragma: no cover - exercised by installation gate
        raise RuntimeError("locked websockets public transport dependency is unavailable") from exc
    name = source_id or f"{venue.value}_PUBLIC_WS"
    async with connect(url, open_timeout=10, ping_interval=20, ping_timeout=20,
                       close_timeout=5, max_size=MAX_PUBLIC_FRAME_BYTES) as socket:
        if venue == VenueV2.BYBIT:
            await socket.send(json.dumps({"op": "subscribe", "args": list(topics)}, separators=(",", ":")))
        async for message in socket:
            received = clock_ns()
            raw = message if isinstance(message, bytes) else message.encode("utf-8")
            if len(raw) > MAX_PUBLIC_FRAME_BYTES:
                raise ValueError("public WebSocket frame exceeds strict size bound")
            payload_hash = hashlib.sha256(raw).hexdigest()
            try:
                parsed = json.loads(raw)
                channel = str(parsed.get("topic", parsed.get("stream", "UNKNOWN"))) if isinstance(parsed, dict) else "UNKNOWN"
            except (UnicodeDecodeError, json.JSONDecodeError):
                channel = "UNPARSEABLE"
            available = clock_ns()
            yield CapturedPublicFrameV2(venue, name, channel, raw, payload_hash, received, available)


def _parsed_availability(frame: CapturedPublicFrameV2, processed_at_ns: int | None) -> int:
    processed = time.time_ns() if processed_at_ns is None else timestamp(processed_at_ns, field="processed_at_ns")
    return max(frame.available_at_ns, processed)


def _millis_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("exchange timestamp must be integer milliseconds")
    try:
        result = int(value) * 1_000_000
    except (TypeError, ValueError) as exc:
        raise ValueError("exchange timestamp must be integer milliseconds") from exc
    timestamp(result, field="exchange_timestamp")
    return result


def _book_levels(values: Any) -> tuple[BookLevelV2, ...]:
    if not isinstance(values, list):
        raise ValueError("book levels must be an array")
    return tuple(BookLevelV2(Decimal(str(row[0])), Decimal(str(row[1]))) for row in values)


def parse_bybit_orderbook_frame(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                                declared_depth: int = 50, source_health: str = "UNKNOWN",
                                source_health_ref: str | None = None, processed_at_ns: int | None = None,
                                availability_class: str = "ACTUAL_SYSTEM") -> L2SnapshotV2 | L2DeltaV2 | L2SequenceFaultV2:
    if frame.venue != VenueV2.BYBIT:
        raise ValueError("Bybit parser received a different venue")
    payload = json.loads(frame.raw_payload_bytes)
    data = payload.get("data")
    expected_topic = f"orderbook.{declared_depth}.{instrument.native_symbol}"
    if (not isinstance(data, dict) or payload.get("topic") != expected_topic
            or frame.channel != expected_topic):
        raise ValueError("Bybit frame topic/data does not match the requested instrument and depth")
    kind = str(payload.get("type", "")).lower()
    update_id = data.get("u")
    if type(update_id) is not int:
        available = _parsed_availability(frame, processed_at_ns)
        return L2SequenceFaultV2(instrument, frame.source_id, frame.channel, "MISSING_UPDATE_ID_U",
                                 _millis_ns(payload.get("ts")), frame.received_at_ns,
                                 available, frame.raw_payload_hash, source_health,
                                 source_health_ref)
    seq = data.get("seq")
    # cts is matching-engine production time; ts remains in the immutable raw
    # payload as the exchange message-generation timestamp.
    event_at = _millis_ns(payload.get("cts", payload.get("ts")))
    bids = _book_levels(data.get("b", []))
    asks = _book_levels(data.get("a", []))
    available = _parsed_availability(frame, processed_at_ns)
    common: dict[str, Any] = {
        "instrument": instrument, "source_id": frame.source_id, "channel": frame.channel,
        "sequence_semantics": "BYBIT_U", "last_update_id": update_id, "event_at_ns": event_at,
        "received_at_ns": frame.received_at_ns, "available_at_ns": available,
        "raw_content_ref": frame.raw_payload_hash, "source_health": source_health,
        "availability_class": availability_class, "source_health_ref": source_health_ref,
    }
    if kind == "snapshot":
        return L2SnapshotV2(**common, bids=bids, asks=asks, declared_depth=declared_depth,
                            snapshot_token=str(seq) if seq is not None else None)
    if kind != "delta":
        raise ValueError("Bybit orderbook message must declare snapshot or delta")
    return L2DeltaV2(**common, first_update_id=None, previous_update_id=None,
                     bids=bids, asks=asks, reset=update_id == 1)


def parse_binance_depth_frame(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                              source_health: str = "UNKNOWN",
                              source_health_ref: str | None = None, processed_at_ns: int | None = None,
                              availability_class: str = "ACTUAL_SYSTEM") -> L2DeltaV2 | L2SequenceFaultV2:
    if frame.venue != VenueV2.BINANCE:
        raise ValueError("Binance parser received a different venue")
    wrapper = json.loads(frame.raw_payload_bytes)
    payload = wrapper.get("data", wrapper)
    stream = wrapper.get("stream", "")
    if (not isinstance(payload, dict) or not stream
            or not stream.startswith(instrument.native_symbol.lower() + "@depth")
            or frame.channel != stream):
        raise ValueError("Binance depth stream does not match the requested instrument")
    first, last, previous = payload.get("U"), payload.get("u"), payload.get("pu")
    if any(type(value) is not int for value in (first, last, previous)):
        available = _parsed_availability(frame, processed_at_ns)
        return L2SequenceFaultV2(instrument, frame.source_id, frame.channel, "MISSING_OR_INVALID_U_U_PU",
                                 _millis_ns(payload.get("E")), frame.received_at_ns,
                                 available, frame.raw_payload_hash, source_health,
                                 source_health_ref)
    event = _millis_ns(payload.get("E"))
    bids = _book_levels(payload.get("b", []))
    asks = _book_levels(payload.get("a", []))
    available = _parsed_availability(frame, processed_at_ns)
    return L2DeltaV2(
        instrument, frame.source_id, frame.channel, "BINANCE_U_PU",
        cast(int, first), cast(int, last), cast(int, previous), event,
        frame.received_at_ns, available, bids, asks, frame.raw_payload_hash, source_health,
        availability_class, False, source_health_ref,
    )


def parse_binance_rest_snapshot(raw_payload_bytes: bytes, *, instrument: InstrumentKeyV2,
                                source_id: str, channel: str, received_at_ns: int,
                                available_at_ns: int, declared_depth: int,
                                source_health: str, source_health_ref: str | None = None,
                                processed_at_ns: int | None = None,
                                availability_class: str = "ACTUAL_SYSTEM") -> L2SnapshotV2:
    capability = capability_for_public_channel_v2(
        default_evidence_capability_matrix_v2(), instrument, channel,
    )
    if capability is None or "snapshot initialization input" not in " ".join(capability.permitted_uses):
        raise ValueError("Binance REST snapshot channel is absent from the capability matrix")
    payload = json.loads(raw_payload_bytes)
    last = payload.get("lastUpdateId")
    if type(last) is not int:
        raise ValueError("Binance depth snapshot requires lastUpdateId")
    bids = _book_levels(payload.get("bids", []))
    asks = _book_levels(payload.get("asks", []))
    processed = (time.time_ns() if processed_at_ns is None
                 else timestamp(processed_at_ns, field="processed_at_ns"))
    available = max(timestamp(available_at_ns, field="available_at_ns"), processed)
    return L2SnapshotV2(instrument, source_id, channel, "BINANCE_U_PU", last, None,
                        received_at_ns, available, bids, asks, hashlib.sha256(raw_payload_bytes).hexdigest(),
                        source_health, declared_depth, availability_class, str(last), source_health_ref)


def parse_bybit_trades(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                       source_health: str = "UNKNOWN",
                       source_health_ref: str | None = None, processed_at_ns: int | None = None,
                       availability_class: str = "ACTUAL_SYSTEM") -> tuple[AggressiveTradeV2, ...]:
    if frame.venue != VenueV2.BYBIT:
        raise ValueError("Bybit trade parser received a different venue")
    payload = json.loads(frame.raw_payload_bytes)
    if payload.get("topic") != f"publicTrade.{instrument.native_symbol}" or not isinstance(payload.get("data"), list):
        raise ValueError("Bybit trade topic/data does not match instrument")
    result = []
    for row in payload["data"]:
        side = row.get("S")
        aggressor = "BUY" if side == "Buy" else "SELL" if side == "Sell" else None
        price, quantity = Decimal(str(row["p"])), Decimal(str(row["v"]))
        event_at = _millis_ns(row.get("T"))
        available = _parsed_availability(frame, processed_at_ns)
        result.append(AggressiveTradeV2(
            instrument, frame.source_id, frame.channel, str(row.get("i", "")), aggressor,
            price, quantity, event_at,
            frame.received_at_ns, available, frame.raw_payload_hash, source_health,
            "BYBIT_S_IS_TAKER_SIDE", availability_class, source_health_ref,
        ))
    return tuple(result)


def parse_binance_aggtrade(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                           source_health: str = "UNKNOWN",
                           source_health_ref: str | None = None, processed_at_ns: int | None = None,
                           availability_class: str = "ACTUAL_SYSTEM") -> AggressiveTradeV2:
    if frame.venue != VenueV2.BINANCE:
        raise ValueError("Binance aggTrade parser received a different venue")
    wrapper = json.loads(frame.raw_payload_bytes)
    row = wrapper.get("data", wrapper)
    if not isinstance(row, dict):
        raise ValueError("Binance aggregate trade body is malformed")
    stream = wrapper.get("stream", "")
    if not stream or not stream.startswith(instrument.native_symbol.lower() + "@aggTrade") or frame.channel != stream:
        raise ValueError("Binance trade stream does not match instrument")
    buyer_maker = row.get("m")
    if type(buyer_maker) is not bool:
        raise ValueError("Binance aggTrade buyer-maker convention is missing")
    price, quantity = Decimal(str(row["p"])), Decimal(str(row["q"]))
    event_at = _millis_ns(row.get("T"))
    available = _parsed_availability(frame, processed_at_ns)
    return AggressiveTradeV2(
        instrument, frame.source_id, frame.channel, str(row.get("a", "")), "SELL" if buyer_maker else "BUY",
        price, quantity, event_at,
        frame.received_at_ns, available, frame.raw_payload_hash, source_health,
        "BINANCE_m_TRUE_BUYER_MAKER_SELLER_AGGRESSOR", availability_class, source_health_ref,
    )


def parse_bybit_liquidations(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                            source_health: str = "UNKNOWN",
                            source_health_ref: str | None = None, processed_at_ns: int | None = None,
                            availability: DerivativeAvailabilityV2 = DerivativeAvailabilityV2.ACTUAL_RECEIPT
                            ) -> tuple[LiquidationObservationV2, ...]:
    """Parse Bybit public liquidation records as censored observations.

    ``S`` is the liquidated position side, never the aggressor side; public
    feed completeness remains unknown even when the transport is healthy.
    """
    if frame.venue != VenueV2.BYBIT:
        raise ValueError("Bybit liquidation parser received a different venue")
    payload = json.loads(frame.raw_payload_bytes)
    if payload.get("topic") != f"allLiquidation.{instrument.native_symbol}" or not isinstance(payload.get("data"), list):
        raise ValueError("Bybit liquidation topic/data does not match instrument")
    result = []
    for row in payload["data"]:
        side = row.get("S")
        position_side = "BUY_POSITION_LIQUIDATED" if side == "Buy" else "SELL_POSITION_LIQUIDATED" if side == "Sell" else "UNKNOWN"
        event_at = _millis_ns(row.get("T", row.get("updatedTime")))
        row_hash = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        price = Decimal(str(row["p"])) if row.get("p") is not None else None
        quantity = Decimal(str(row["v"])) if row.get("v") is not None else None
        available = _parsed_availability(frame, processed_at_ns)
        result.append(LiquidationObservationV2(
            instrument, frame.source_id, row_hash, position_side, "BYBIT_S_IS_LIQUIDATED_POSITION_SIDE",
            price, quantity,
            "BYBIT_SOURCE_CONTRACT_QUANTITY_UNIT_UNQUALIFIED" if row.get("v") is not None else None,
            event_at, frame.received_at_ns, available, LiquidationCoverageV2.CENSORED,
            source_health, frame.raw_payload_hash, availability,
            source_health_ref, frame.channel,
        ))
    return tuple(result)


def raw_archive_record(frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                       frame_type: str, sequence_semantics: str,
                       first_update_id: int | None = None, last_update_id: int | None = None,
                       previous_update_id: int | None = None, event_at_ns: int | None = None,
                       source_health: str = "UNKNOWN", source_health_ref: str | None = None) -> L2RawFrameV2:
    return L2RawFrameV2(instrument, frame.source_id, frame.channel, frame_type,
                         frame.raw_payload_bytes, frame.raw_payload_hash, event_at_ns,
                         frame.received_at_ns, frame.available_at_ns,
                         first_update_id, last_update_id, previous_update_id,
                         sequence_semantics, source_health, "ACTUAL_SYSTEM", source_health_ref)
