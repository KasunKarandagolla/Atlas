from __future__ import annotations

import asyncio
import hashlib
import threading
import time

from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2, bybit_btc_eth_linear_topics
from atlas.v2.data.public_stream_source import PublicStreamSourceV2
from atlas.v2.instruments import VenueV2

TOPICS = bybit_btc_eth_linear_topics()


def frame(received_at_ns: int = 100) -> CapturedPublicFrameV2:
    raw = b'{"topic":"publicTrade.BTCUSDT","data":[]}'
    return CapturedPublicFrameV2(
        VenueV2.BYBIT, "FAKE", TOPICS[1], raw, hashlib.sha256(raw).hexdigest(),
        received_at_ns, received_at_ns,
    )


def wait_for_finish(source: PublicStreamSourceV2) -> None:
    deadline = time.monotonic() + 3.0
    while not source.status().handoff.closed and time.monotonic() < deadline:
        threading.Event().wait(0.005)
    assert source.status().state != "RUNNING" and source.status().handoff.closed


def test_fake_source_has_finite_reconnects_and_immutable_controller_drain() -> None:
    calls = 0

    async def stream_factory():
        nonlocal calls
        calls += 1
        if calls == 1:
            yield frame(150)
            raise OSError("private detail is never copied into status")
        yield frame(200)

    source = PublicStreamSourceV2(
        venue=VenueV2.BYBIT, topics=TOPICS, stream_factory=stream_factory,
        source_id="FAKE", max_attempts=2, initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    source.start()
    source.start()  # idempotent: it must not create a second producer.
    wait_for_finish(source)
    status = source.status()
    drained = source.drain(max_items=2)
    assert calls == 2
    assert status.state == "EXHAUSTED"
    assert status.attempt_count == 2 and status.reconnect_count == 1
    assert status.handoff.frames_received == 2 and status.handoff.closed
    assert [row.connection_epoch for row in drained] == [1, 2]
    assert [row.received_at_ns for row in drained] == [150, 200]
    assert isinstance(drained, tuple)
    source.close()
    assert source.status().state == "CLOSED"


def test_source_close_cancels_one_blocked_fake_stream_and_closes_handoff() -> None:
    entered = threading.Event()

    async def blocked_stream():
        entered.set()
        await asyncio.Event().wait()
        yield frame(300)

    source = PublicStreamSourceV2(
        venue=VenueV2.BYBIT, topics=TOPICS, stream_factory=blocked_stream,
        max_attempts=5, initial_backoff_seconds=0, max_backoff_seconds=0,
    )
    source.start()
    assert entered.wait(timeout=2.0)
    source.close(timeout_seconds=2.0)
    status = source.status()
    assert status.state == "CLOSED" and status.handoff.closed
    assert not status.handoff.connected
    assert status.attempt_count == 1


def test_source_overflow_fails_closed_without_retry() -> None:
    calls = 0

    async def overflowing_stream():
        nonlocal calls
        calls += 1
        yield frame(400)
        yield frame(401)

    source = PublicStreamSourceV2(
        venue=VenueV2.BYBIT, topics=TOPICS, stream_factory=overflowing_stream,
        max_attempts=4, initial_backoff_seconds=0, max_backoff_seconds=0,
        max_queue_items=1, max_queue_bytes=1000,
    )
    source.start()
    wait_for_finish(source)
    status = source.status()
    assert calls == 1 and status.attempt_count == 1
    assert status.state == "FAILED" and status.last_error_code == "FRAME_QUEUE_OVERFLOW"
    assert status.handoff.overflowed and status.handoff.frames_rejected == 1
    assert status.handoff.closed
    source.close()


def test_source_rejects_unbounded_retry_or_backoff_configuration() -> None:
    import pytest

    with pytest.raises(ValueError, match="max_attempts"):
        PublicStreamSourceV2(venue=VenueV2.BYBIT, topics=TOPICS, max_attempts=9)
    with pytest.raises(ValueError, match="backoff"):
        PublicStreamSourceV2(venue=VenueV2.BYBIT, topics=TOPICS, max_backoff_seconds=31)
