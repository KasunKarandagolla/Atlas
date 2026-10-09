"""Narrow credential-free public WebSocket capture and venue translators.

No authentication, account, order, or capital-control endpoint is present.
Live feed qualification remains a separate gate.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace
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
MAX_PUBLIC_SUBSCRIPTION_TOPICS = 32
DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS = 512
DEFAULT_PUBLIC_FRAME_QUEUE_BYTES = 16_000_000
DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS = 64
PUBLIC_WS_RECEIVE_QUEUE_ITEMS = 16
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
    connection_epoch: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", VenueV2(self.venue))
        if (not self.source_id or len(self.source_id) > 128 or not self.channel or len(self.channel) > 256
                or not isinstance(self.raw_payload_bytes, bytes)):
            raise ValueError("captured public frame identity and exact bytes are required")
        if len(self.raw_payload_bytes) > MAX_PUBLIC_FRAME_BYTES:
            raise ValueError("public WebSocket frame exceeds strict size bound")
        sha256_ref(self.raw_payload_hash, field="raw_payload_hash")
        if hashlib.sha256(self.raw_payload_bytes).hexdigest() != self.raw_payload_hash:
            raise ValueError("captured public frame hash does not match exact bytes")
        timestamp(self.received_at_ns, field="received_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("frame availability cannot precede receipt")
        if self.connection_epoch is not None and (type(self.connection_epoch) is not int or self.connection_epoch <= 0):
            raise ValueError("connection_epoch must be a positive integer when present")


@dataclass(frozen=True)
class PublicFrameHandoffStatusV2:
    """Bounded, read-only observations for a single public stream handoff."""

    venue: VenueV2
    topics: tuple[str, ...]
    queue_items: int
    queue_bytes: int
    max_queue_items: int
    max_queue_bytes: int
    max_drain_items: int
    high_water_items: int
    high_water_bytes: int
    frames_received: int
    frames_drained: int
    controls_received: int
    frames_rejected: int
    closed_rejections: int
    overflowed: bool
    backpressure: bool
    connected: bool
    closed: bool
    disconnect_count: int
    last_disconnect_at_ns: int | None
    heartbeat_count: int
    last_heartbeat_at_ns: int | None
    last_activity_at_ns: int | None
    last_error_code: str | None
    last_error_at_ns: int | None


class PublicFrameHandoffOverflowV2(RuntimeError):
    """Raised by the stream pump after the bounded queue rejects a frame."""


class SharedPublicFrameBudgetV1:
    """One aggregate bounded budget shared by independent public stream lanes.

    Each lane retains a small guaranteed reserve while idle capacity may be
    borrowed by busy lanes. The sum of queued frames and bytes never exceeds
    the existing broad-stream limits.
    """

    def __init__(self, *, max_items: int, max_bytes: int,
                 reserve_items_per_lane: int, reserve_bytes_per_lane: int) -> None:
        values = (max_items, max_bytes, reserve_items_per_lane, reserve_bytes_per_lane)
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("shared public frame budget values must be non-negative integers")
        if max_items <= 0 or max_bytes <= 0 or reserve_items_per_lane > max_items or reserve_bytes_per_lane > max_bytes:
            raise ValueError("shared public frame budget requires positive capacities and bounded reserves")
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.reserve_items_per_lane = reserve_items_per_lane
        self.reserve_bytes_per_lane = reserve_bytes_per_lane
        self._lock = threading.Lock()
        self._lanes: dict[str, tuple[int, int]] = {}
        self._items = 0
        self._bytes = 0
        self._high_water_items = 0
        self._high_water_bytes = 0

    def register_lane(self, lane: str) -> None:
        if not lane or len(lane) > 64:
            raise ValueError("shared public frame budget lane identity is invalid")
        with self._lock:
            if lane in self._lanes:
                raise ValueError("shared public frame budget lane is duplicated")
            if ((len(self._lanes) + 1) * self.reserve_items_per_lane > self.max_items
                    or (len(self._lanes) + 1) * self.reserve_bytes_per_lane > self.max_bytes):
                raise ValueError("shared public frame budget cannot preserve all lane reserves")
            self._lanes[lane] = (0, 0)

    def reserve(self, lane: str, frame_bytes: int) -> bool:
        if type(frame_bytes) is not int or frame_bytes < 0:
            raise ValueError("shared public frame reservation size is invalid")
        with self._lock:
            if lane not in self._lanes:
                raise RuntimeError("unregistered public stream lane requested shared capacity")
            if self._items + 1 > self.max_items or self._bytes + frame_bytes > self.max_bytes:
                return False
            # Keep each other lane's unused floor available. The current lane
            # may fill its own reserve, then borrow only genuinely idle space.
            protected_items = sum(max(0, self.reserve_items_per_lane - used[0])
                                  for name, used in self._lanes.items() if name != lane)
            protected_bytes = sum(max(0, self.reserve_bytes_per_lane - used[1])
                                  for name, used in self._lanes.items() if name != lane)
            if (self._items + 1 + protected_items > self.max_items
                    or self._bytes + frame_bytes + protected_bytes > self.max_bytes):
                return False
            items, size = self._lanes[lane]
            self._lanes[lane] = (items + 1, size + frame_bytes)
            self._items += 1
            self._bytes += frame_bytes
            self._high_water_items = max(self._high_water_items, self._items)
            self._high_water_bytes = max(self._high_water_bytes, self._bytes)
            return True

    def release(self, lane: str, frame_bytes: int) -> None:
        with self._lock:
            current = self._lanes.get(lane)
            if current is None or current[0] <= 0 or current[1] < frame_bytes:
                raise RuntimeError("shared public frame budget accounting underflow")
            self._lanes[lane] = (current[0] - 1, current[1] - frame_bytes)
            self._items -= 1
            self._bytes -= frame_bytes

    def snapshot(self) -> tuple[int, int, int, int, int]:
        with self._lock:
            return (self._items, self._bytes, self._high_water_items,
                    self._high_water_bytes, self.max_items)


def bybit_btc_eth_linear_topics() -> tuple[str, ...]:
    """Return the explicit S32 Bybit USDT-linear BTC/ETH book and trade set."""
    return (
        "orderbook.50.BTCUSDT", "publicTrade.BTCUSDT",
        "orderbook.50.ETHUSDT", "publicTrade.ETHUSDT",
    )


def _validate_subscription(venue: VenueV2, topics: tuple[str, ...]) -> None:
    if (not topics or len(topics) > MAX_PUBLIC_SUBSCRIPTION_TOPICS
            or len(set(topics)) != len(topics)
            or any(not isinstance(topic, str) or len(topic) > 256 for topic in topics)):
        raise ValueError("public WebSocket subscription must be a small, unique explicit topic set")
    if any(not _topic_allowed(venue, topic) for topic in topics):
        raise ValueError("only allowlisted public L2/trade/liquidation channels are accepted")


class BoundedPublicFrameHandoffV2:
    """Thread-safe, nonblocking bounded queue from one network producer to a controller.

    Queue overflow is observable and sticky. ``offer`` never waits for a drain;
    a rejected data frame returns ``False`` and increments the loss counters.
    Control acknowledgements are observed and counted without entering the
    market-data queue. No persistence or venue state is changed here.
    """

    def __init__(self, *, venue: VenueV2, topics: tuple[str, ...],
                 max_queue_items: int = DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
                 max_queue_bytes: int = DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
                 max_drain_items: int = DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
                 shared_budget: SharedPublicFrameBudgetV1 | None = None,
                 budget_lane: str | None = None) -> None:
        self.venue = VenueV2(venue)
        _validate_subscription(self.venue, topics)
        for value, name in ((max_queue_items, "max_queue_items"),
                            (max_queue_bytes, "max_queue_bytes"),
                            (max_drain_items, "max_drain_items")):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if max_queue_bytes > 2_000_000_000 or max_queue_items > 100_000 or max_drain_items > 10_000:
            raise ValueError("public frame handoff bounds exceed hard safety ceilings")
        self.topics = tuple(topics)
        self.max_queue_items = max_queue_items
        self.max_queue_bytes = max_queue_bytes
        self.max_drain_items = max_drain_items
        if (shared_budget is None) != (budget_lane is None):
            raise ValueError("shared public frame budget and lane identity must be supplied together")
        self._shared_budget = shared_budget
        self._budget_lane = budget_lane
        if shared_budget is not None:
            assert budget_lane is not None
            # Registration is idempotence-protected by the budget; duplicate
            # stream lane construction therefore fails closed.
            shared_budget.register_lane(budget_lane)
        self._queue: deque[CapturedPublicFrameV2] = deque()
        self._queue_bytes = 0
        self._lock = threading.Lock()
        self._high_water_items = 0
        self._high_water_bytes = 0
        self._frames_received = 0
        self._frames_drained = 0
        self._controls_received = 0
        self._frames_rejected = 0
        self._closed_rejections = 0
        self._overflowed = False
        self._backpressure = False
        self._connected = False
        self._closed = False
        self._disconnect_count = 0
        self._last_disconnect_at_ns: int | None = None
        self._heartbeat_count = 0
        self._last_heartbeat_at_ns: int | None = None
        self._last_activity_at_ns: int | None = None
        self._last_error_code: str | None = None
        self._last_error_at_ns: int | None = None

    def offer(self, frame: CapturedPublicFrameV2) -> bool:
        """Offer one immutable frame without blocking; return false on loss/closure."""
        if frame.venue != self.venue:
            self.observe_error("WRONG_VENUE", frame.received_at_ns)
            raise ValueError("public frame venue does not match the handoff")
        if frame.channel == "CONTROL":
            try:
                control = json.loads(frame.raw_payload_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                self.observe_error("MALFORMED_CONTROL", frame.received_at_ns)
                raise ValueError("malformed public WebSocket control frame") from exc
            if self.venue != VenueV2.BYBIT or not isinstance(control, dict) or control.get("op") not in {
                "ping", "pong", "subscribe",
            }:
                self.observe_error("UNEXPECTED_CONTROL", frame.received_at_ns)
                raise ValueError("unrecognized public WebSocket control frame")
            with self._lock:
                if self._closed:
                    self._closed_rejections += 1
                    return False
                self._controls_received += 1
                self._last_activity_at_ns = frame.received_at_ns
                if control.get("op") in {"ping", "pong"} or control.get("ret_msg") == "pong":
                    self._heartbeat_count += 1
                    self._last_heartbeat_at_ns = frame.received_at_ns
                if control.get("success") is False:
                    self._set_error_locked("VENUE_CONTROL_ERROR", frame.received_at_ns)
            return True
        if frame.channel not in self.topics:
            self.observe_error("UNEXPECTED_TOPIC", frame.received_at_ns)
            raise ValueError("public frame topic does not match the explicit subscription")
        size = len(frame.raw_payload_bytes)
        with self._lock:
            self._last_activity_at_ns = frame.received_at_ns
            if self._closed:
                self._closed_rejections += 1
                return False
            if size > self.max_queue_bytes or len(self._queue) >= self.max_queue_items \
                    or self._queue_bytes + size > self.max_queue_bytes:
                self._frames_rejected += 1
                self._overflowed = True
                self._backpressure = True
                self._set_error_locked("FRAME_QUEUE_OVERFLOW", frame.received_at_ns)
                return False
            reserved = False
            if self._shared_budget is not None:
                assert self._budget_lane is not None
                if not self._shared_budget.reserve(self._budget_lane, size):
                    self._frames_rejected += 1
                    self._overflowed = True
                    self._backpressure = True
                    self._set_error_locked("FRAME_QUEUE_OVERFLOW", frame.received_at_ns)
                    return False
                reserved = True
            try:
                self._queue.append(frame)
            except BaseException:
                if reserved:
                    assert self._shared_budget is not None and self._budget_lane is not None
                    self._shared_budget.release(self._budget_lane, size)
                raise
            self._queue_bytes += size
            self._frames_received += 1
            self._high_water_items = max(self._high_water_items, len(self._queue))
            self._high_water_bytes = max(self._high_water_bytes, self._queue_bytes)
            if (len(self._queue) * 4 >= self.max_queue_items * 3
                    or self._queue_bytes * 4 >= self.max_queue_bytes * 3):
                self._backpressure = True
        return True

    def drain(self, *, max_items: int | None = None) -> tuple[CapturedPublicFrameV2, ...]:
        """Remove at most the configured per-call work bound, preserving FIFO order."""
        limit = self.max_drain_items if max_items is None else max_items
        if type(limit) is not int or limit <= 0 or limit > self.max_drain_items:
            raise ValueError("drain request must be positive and no greater than max_drain_items")
        with self._lock:
            count = min(limit, len(self._queue))
            rows = tuple(self._queue.popleft() for _ in range(count))
            released_bytes = sum(len(frame.raw_payload_bytes) for frame in rows)
            self._queue_bytes -= released_bytes
            if self._shared_budget is not None:
                assert self._budget_lane is not None
                for frame in rows:
                    self._shared_budget.release(self._budget_lane, len(frame.raw_payload_bytes))
            self._frames_drained += len(rows)
            return rows

    def observe_connected(self, at_ns: int) -> None:
        at_ns = timestamp(at_ns, field="connected_at_ns")
        with self._lock:
            if not self._closed:
                self._connected = True
                self._last_activity_at_ns = at_ns

    def observe_disconnected(self, at_ns: int) -> None:
        at_ns = timestamp(at_ns, field="disconnected_at_ns")
        with self._lock:
            if self._connected:
                self._disconnect_count += 1
                self._last_disconnect_at_ns = at_ns
            self._connected = False
            self._last_activity_at_ns = at_ns

    def observe_heartbeat(self, at_ns: int) -> None:
        at_ns = timestamp(at_ns, field="heartbeat_at_ns")
        with self._lock:
            self._heartbeat_count += 1
            self._last_heartbeat_at_ns = at_ns
            self._last_activity_at_ns = at_ns

    def observe_error(self, code: str, at_ns: int) -> None:
        at_ns = timestamp(at_ns, field="error_at_ns")
        if not code or len(code) > 64 or not code.replace("_", "").isalnum():
            raise ValueError("public stream error code must be a short stable identifier")
        with self._lock:
            self._set_error_locked(code, at_ns)

    def _set_error_locked(self, code: str, at_ns: int) -> None:
        self._last_error_code = code
        self._last_error_at_ns = at_ns

    def close(self, at_ns: int) -> None:
        at_ns = timestamp(at_ns, field="closed_at_ns")
        with self._lock:
            self._closed = True
            self._connected = False
            self._last_activity_at_ns = at_ns

    def snapshot(self) -> PublicFrameHandoffStatusV2:
        with self._lock:
            return PublicFrameHandoffStatusV2(
                self.venue, self.topics, len(self._queue), self._queue_bytes,
                self.max_queue_items, self.max_queue_bytes, self.max_drain_items,
                self._high_water_items, self._high_water_bytes, self._frames_received,
                self._frames_drained, self._controls_received, self._frames_rejected,
                self._closed_rejections, self._overflowed, self._backpressure,
                self._connected, self._closed, self._disconnect_count,
                self._last_disconnect_at_ns, self._heartbeat_count,
                self._last_heartbeat_at_ns, self._last_activity_at_ns,
                self._last_error_code, self._last_error_at_ns,
            )


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
    _validate_subscription(venue, topics)
    if venue == VenueV2.BYBIT:
        return "wss://stream.bybit.com/v5/public/linear"
    routes = {_binance_route(topic) for topic in topics}
    if len(routes) != 1:
        raise ValueError("Binance public depth and market trades require separate WebSocket routes")
    route = next(iter(routes))
    return f"wss://fstream.binance.com/{route}/stream?" + urlencode({"streams": "/".join(topics)})


def capture_public_frame_message(*, venue: VenueV2, topics: tuple[str, ...], message: str | bytes,
                                 source_id: str, clock_ns: Callable[[], int] = time.time_ns
                                 ) -> CapturedPublicFrameV2:
    """Validate and capture one received message, useful for fake stream adapters too."""
    venue = VenueV2(venue)
    _validate_subscription(venue, topics)
    if not source_id or len(source_id) > 128:
        raise ValueError("public WebSocket source identity must be bounded and non-empty")
    received = timestamp(clock_ns(), field="received_at_ns")
    if isinstance(message, bytes):
        raw = message
    elif isinstance(message, str):
        raw = message.encode("utf-8")
    else:
        raise ValueError("public WebSocket message must be text or exact bytes")
    if len(raw) > MAX_PUBLIC_FRAME_BYTES:
        raise ValueError("public WebSocket frame exceeds strict size bound")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("public WebSocket frame is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("public WebSocket frame must be a JSON object")
    if venue == VenueV2.BYBIT and "topic" not in payload and "stream" not in payload:
        if payload.get("op") not in {"ping", "pong", "subscribe"}:
            raise ValueError("Bybit public frame is neither subscribed data nor a recognized control")
        channel = "CONTROL"
    else:
        channel_value = payload.get("topic", payload.get("stream"))
        if not isinstance(channel_value, str) or channel_value not in topics:
            raise ValueError("public WebSocket frame topic does not match the explicit subscription")
        channel = channel_value
    available = max(received, timestamp(clock_ns(), field="available_at_ns"))
    return CapturedPublicFrameV2(
        venue, source_id, channel, raw, hashlib.sha256(raw).hexdigest(), received, available,
    )


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
    async with connect(url, proxy=None, open_timeout=10, ping_interval=20, ping_timeout=20,
                       close_timeout=5, max_size=MAX_PUBLIC_FRAME_BYTES,
                       max_queue=PUBLIC_WS_RECEIVE_QUEUE_ITEMS) as socket:
        if venue == VenueV2.BYBIT:
            await socket.send(json.dumps({"op": "subscribe", "args": list(topics)}, separators=(",", ":")))
        async for message in socket:
            yield capture_public_frame_message(
                venue=venue, topics=topics, message=message, source_id=name, clock_ns=clock_ns,
            )


async def handoff_public_frames(frames: AsyncIterator[CapturedPublicFrameV2], *,
                                handoff: BoundedPublicFrameHandoffV2,
                                connection_epoch: int | None = None,
                                clock_ns: Callable[[], int] = time.time_ns) -> None:
    """Pump one already-open stream into a handoff; never retries or persists."""
    if connection_epoch is not None and (type(connection_epoch) is not int or connection_epoch <= 0):
        raise ValueError("connection_epoch must be a positive integer when present")
    handoff.observe_connected(clock_ns())
    try:
        async for frame in frames:
            if connection_epoch is not None:
                frame = replace(frame, connection_epoch=connection_epoch)
            if not handoff.offer(frame):
                raise PublicFrameHandoffOverflowV2("bounded public frame handoff rejected a frame")
    except Exception as exc:
        error_code = "FRAME_QUEUE_OVERFLOW" if isinstance(exc, PublicFrameHandoffOverflowV2) else (
            "STREAM_" + type(exc).__name__.upper()[:52]
        )
        handoff.observe_error(error_code, clock_ns())
        raise
    finally:
        handoff.observe_disconnected(clock_ns())


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
