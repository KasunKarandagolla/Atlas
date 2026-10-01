from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from dataclasses import replace

import pytest

from atlas.v2.data.public_microstructure_ws import (
    MAX_PUBLIC_FRAME_BYTES,
    BoundedPublicFrameHandoffV2,
    CapturedPublicFrameV2,
    PublicFrameHandoffOverflowV2,
    bybit_btc_eth_linear_topics,
    capture_public_frame_message,
    capture_public_frames,
    handoff_public_frames,
)
from atlas.v2.instruments import VenueV2

TOPICS = bybit_btc_eth_linear_topics()


def raw_trade(topic: str, trade_id: str = "t-1") -> bytes:
    return json.dumps({
        "topic": topic,
        "type": "snapshot",
        "ts": 1_700_000_000_000,
        "data": [{"i": trade_id, "T": 1_700_000_000_000, "S": "Buy", "p": "100", "v": "0.1"}],
    }, separators=(",", ":")).encode()


def capture(topic: str = TOPICS[1], *, source_id: str = "BYBIT_PUBLIC_WS",
            received: int = 100, available: int = 101, raw: bytes | None = None) -> CapturedPublicFrameV2:
    payload = raw if raw is not None else raw_trade(topic)
    return CapturedPublicFrameV2(
        VenueV2.BYBIT, source_id, topic, payload, hashlib.sha256(payload).hexdigest(), received, available,
    )


def test_two_symbol_fake_stream_preserves_exact_bytes_receipt_and_bounds_transport(monkeypatch) -> None:
    rows = [raw_trade(topic).decode() for topic in TOPICS]
    sent: list[str] = []
    connect_options: dict[str, object] = {}

    class FakeSocket:
        def __init__(self) -> None:
            self.messages = iter(rows)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message: str) -> None:
            sent.append(message)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.messages)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    def fake_connect(_url: str, **kwargs):
        connect_options.update(kwargs)
        return FakeSocket()

    from websockets.asyncio import client

    monkeypatch.setattr(client, "connect", fake_connect)
    clock_values = iter((200, 201, 202, 203, 204, 205, 206, 207))

    async def collect():
        return [
            frame async for frame in capture_public_frames(
                venue=VenueV2.BYBIT, topics=TOPICS, clock_ns=lambda: next(clock_values),
            )
        ]

    frames = asyncio.run(collect())
    assert tuple(frame.channel for frame in frames) == TOPICS
    assert [frame.received_at_ns for frame in frames] == [200, 202, 204, 206]
    assert [frame.available_at_ns for frame in frames] == [201, 203, 205, 207]
    assert all(frame.connection_epoch is None for frame in frames)
    assert all(frame.raw_payload_bytes == original.encode() for frame, original in zip(frames, rows, strict=True))
    assert all(frame.raw_payload_hash == hashlib.sha256(frame.raw_payload_bytes).hexdigest() for frame in frames)
    assert json.loads(sent[0]) == {"op": "subscribe", "args": list(TOPICS)}
    assert connect_options["max_size"] == MAX_PUBLIC_FRAME_BYTES
    assert connect_options["max_queue"] == 16
    assert connect_options["ping_interval"] == 20 and connect_options["ping_timeout"] == 20
    assert connect_options["proxy"] is None


def test_capture_rejects_wrong_topic_malformed_and_oversized_messages() -> None:
    with pytest.raises(ValueError, match="topic does not match"):
        capture_public_frame_message(
            venue=VenueV2.BYBIT, topics=TOPICS, message=raw_trade("publicTrade.XRPUSDT"),
            source_id="fake", clock_ns=lambda: 100,
        )
    with pytest.raises(ValueError, match="valid JSON"):
        capture_public_frame_message(
            venue=VenueV2.BYBIT, topics=TOPICS, message=b"not-json", source_id="fake", clock_ns=lambda: 100,
        )
    with pytest.raises(ValueError, match="size bound"):
        capture_public_frame_message(
            venue=VenueV2.BYBIT, topics=TOPICS, message=b" " * (MAX_PUBLIC_FRAME_BYTES + 1),
            source_id="fake", clock_ns=lambda: 100,
        )


def test_handoff_fifo_drain_work_bound_and_sticky_item_overflow() -> None:
    handoff = BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT, topics=TOPICS, max_queue_items=3, max_queue_bytes=10_000,
        max_drain_items=2,
    )
    frames = [capture(received=i, available=i) for i in (100, 101, 102, 103)]
    assert [handoff.offer(frame) for frame in frames[:3]] == [True, True, True]
    assert handoff.offer(frames[3]) is False
    state = handoff.snapshot()
    assert state.queue_items == state.high_water_items == 3
    assert state.overflowed and state.backpressure
    assert state.frames_rejected == 1 and state.last_error_code == "FRAME_QUEUE_OVERFLOW"
    assert [row.received_at_ns for row in handoff.drain(max_items=2)] == [100, 101]
    assert [row.received_at_ns for row in handoff.drain()] == [102]
    assert handoff.snapshot().overflowed and handoff.snapshot().frames_drained == 3
    with pytest.raises(ValueError, match="max_drain_items"):
        handoff.drain(max_items=3)


def test_handoff_enforces_aggregate_bytes_and_closed_rejections() -> None:
    frame = capture()
    handoff = BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT, topics=TOPICS, max_queue_items=8,
        max_queue_bytes=len(frame.raw_payload_bytes) - 1, max_drain_items=2,
    )
    assert not handoff.offer(frame)
    state = handoff.snapshot()
    assert state.queue_items == state.queue_bytes == 0
    assert state.overflowed and state.frames_rejected == 1
    assert state.high_water_bytes == 0
    handoff.close(150)
    assert not handoff.offer(frame)
    state = handoff.snapshot()
    assert state.closed and state.closed_rejections == 1 and state.queue_items == 0


def test_connection_epoch_is_optional_but_must_be_positive_integer() -> None:
    assert capture().connection_epoch is None
    with pytest.raises(ValueError, match="positive integer"):
        replace(capture(), connection_epoch=0)
    with pytest.raises(ValueError, match="positive integer"):
        replace(capture(), connection_epoch=True)


def test_handoff_rejects_wrong_instrument_topic_and_fake_stream_records_control_and_disconnect() -> None:
    handoff = BoundedPublicFrameHandoffV2(venue=VenueV2.BYBIT, topics=TOPICS)
    wrong = capture("publicTrade.XRPUSDT")
    with pytest.raises(ValueError, match="explicit subscription"):
        handoff.offer(wrong)
    assert handoff.snapshot().last_error_code == "UNEXPECTED_TOPIC"

    controls = [
        capture("CONTROL", raw=b'{"op":"subscribe","success":true,"ret_msg":""}', received=300, available=301),
        capture("CONTROL", raw=b'{"op":"pong","success":true,"ret_msg":"pong"}', received=302, available=303),
    ]

    async def fake_stream():
        yield capture(received=250, available=251)
        yield controls[0]
        yield controls[1]

    asyncio.run(handoff_public_frames(fake_stream(), handoff=handoff, clock_ns=lambda: 400))
    state = handoff.snapshot()
    assert state.queue_items == 1 and state.controls_received == 2
    assert state.heartbeat_count == 1 and state.last_heartbeat_at_ns == 302
    assert state.disconnect_count == 1 and state.last_disconnect_at_ns == 400
    assert not state.connected and state.last_error_code == "UNEXPECTED_TOPIC"


def test_fake_stream_stops_on_overflow_and_exposes_transport_error() -> None:
    handoff = BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT, topics=TOPICS, max_queue_items=1, max_queue_bytes=10_000,
    )

    async def fake_stream():
        yield capture(received=500, available=500)
        yield capture(received=501, available=501)

    with pytest.raises(PublicFrameHandoffOverflowV2):
        asyncio.run(handoff_public_frames(fake_stream(), handoff=handoff, clock_ns=lambda: 600))
    state = handoff.snapshot()
    assert state.queue_items == 1 and state.frames_rejected == 1
    assert state.overflowed and state.last_error_code == "FRAME_QUEUE_OVERFLOW"
    assert not state.connected and state.disconnect_count == 1

    async def broken_stream():
        yield capture(received=700, available=700)
        raise OSError("details deliberately not copied to status")

    other = BoundedPublicFrameHandoffV2(venue=VenueV2.BYBIT, topics=TOPICS)
    with pytest.raises(OSError):
        asyncio.run(handoff_public_frames(broken_stream(), handoff=other, clock_ns=lambda: 800))
    assert other.snapshot().last_error_code == "STREAM_OSERROR"
    assert other.snapshot().disconnect_count == 1


def test_transport_and_handoff_have_no_credential_or_storage_parameters() -> None:
    forbidden = {"headers", "additional_headers", "api_key", "apiKey", "token", "credentials", "ops_db"}
    assert forbidden.isdisjoint(inspect.signature(capture_public_frames).parameters)
    assert forbidden.isdisjoint(inspect.signature(capture_public_frame_message).parameters)
    assert forbidden.isdisjoint(inspect.signature(handoff_public_frames).parameters)
