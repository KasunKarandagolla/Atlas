"""Opt-in, bounded lifecycle adapter for the existing public WebSocket capture.

This component owns one producer thread and event loop. Its only output is a
bounded in-memory frame handoff; durable writes remain with the controller.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Literal

from ..instruments import VenueV2
from .public_microstructure_ws import (
    DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
    DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
    DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
    BoundedPublicFrameHandoffV2,
    CapturedPublicFrameV2,
    PublicFrameHandoffOverflowV2,
    PublicFrameHandoffStatusV2,
    capture_public_frames,
    handoff_public_frames,
)

MAX_PUBLIC_STREAM_ATTEMPTS = 8
MAX_PUBLIC_STREAM_BACKOFF_SECONDS = 30.0


@dataclass(frozen=True)
class PublicStreamSourceStatusV2:
    state: Literal["CREATED", "RUNNING", "EXHAUSTED", "FAILED", "CLOSED"]
    attempt_count: int
    reconnect_count: int
    last_error_code: str | None
    handoff: PublicFrameHandoffStatusV2


class PublicStreamSourceV2:
    """Run a finite-retry public stream producer and expose immutable drains/status.

    The source never writes archives or repositories. A caller may inject an
    async frame factory for deterministic tests or use the existing allowlisted
    ``capture_public_frames`` transport by omitting ``stream_factory``.
    """

    def __init__(self, *, venue: VenueV2, topics: tuple[str, ...],
                 stream_factory: Callable[[], AsyncIterator[CapturedPublicFrameV2]] | None = None,
                 source_id: str | None = None, max_attempts: int = 3,
                 initial_backoff_seconds: float = 0.25, max_backoff_seconds: float = 2.0,
                 max_queue_items: int = DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
                 max_queue_bytes: int = DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
                 max_drain_items: int = DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
                 clock_ns: Callable[[], int] = time.time_ns) -> None:
        self.venue = VenueV2(venue)
        self.topics = tuple(topics)
        if type(max_attempts) is not int or not 1 <= max_attempts <= MAX_PUBLIC_STREAM_ATTEMPTS:
            raise ValueError("max_attempts must be between 1 and the fixed public stream ceiling")
        if (not math.isfinite(initial_backoff_seconds) or not math.isfinite(max_backoff_seconds)
                or initial_backoff_seconds < 0 or max_backoff_seconds < initial_backoff_seconds
                or max_backoff_seconds > MAX_PUBLIC_STREAM_BACKOFF_SECONDS):
            raise ValueError("public stream backoff must be finite and within the fixed safety ceiling")
        if source_id is not None and (not source_id or len(source_id) > 128):
            raise ValueError("public stream source identity must be bounded and non-empty")
        self._clock_ns = clock_ns
        self._source_id = source_id or f"{self.venue.value}_PUBLIC_WS"
        self._max_attempts = max_attempts
        self._initial_backoff_seconds = initial_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._factory = stream_factory or self._default_factory
        self._handoff = BoundedPublicFrameHandoffV2(
            venue=self.venue, topics=self.topics,
            max_queue_items=max_queue_items, max_queue_bytes=max_queue_bytes,
            max_drain_items=max_drain_items,
        )
        self._lock = threading.Lock()
        self._state: Literal["CREATED", "RUNNING", "EXHAUSTED", "FAILED", "CLOSED"] = "CREATED"
        self._attempt_count = 0
        self._reconnect_count = 0
        self._last_error_code: str | None = None
        self._close_requested = threading.Event()
        self._thread: threading.Thread | None = None
        self._thread_ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None

    def _default_factory(self) -> AsyncIterator[CapturedPublicFrameV2]:
        return capture_public_frames(
            venue=self.venue, topics=self.topics, source_id=self._source_id,
            clock_ns=self._clock_ns,
        )

    def start(self) -> None:
        """Start once; repeated calls while running are harmless and never add threads."""
        with self._lock:
            if self._state == "CLOSED":
                raise RuntimeError("closed public stream source cannot be restarted")
            if self._thread is not None:
                return
            self._state = "RUNNING"
            self._thread = threading.Thread(target=self._thread_main, name="atlas-public-stream", daemon=True)
            self._thread.start()
        if not self._thread_ready.wait(timeout=5.0):
            self.close(timeout_seconds=5.0)
            raise TimeoutError("public stream event loop did not start within its bounded startup wait")

    def drain(self, *, max_items: int | None = None) -> tuple[CapturedPublicFrameV2, ...]:
        """Return a bounded immutable FIFO batch for controller-owned persistence."""
        return self._handoff.drain(max_items=max_items)

    def status(self) -> PublicStreamSourceStatusV2:
        """Return one immutable source lifecycle and queue-state snapshot."""
        with self._lock:
            state = self._state
            attempts = self._attempt_count
            reconnects = self._reconnect_count
            error = self._last_error_code
        return PublicStreamSourceStatusV2(state, attempts, reconnects, error, self._handoff.snapshot())

    def request_close(self) -> None:
        """Request a research capture stop without waiting on the producer.

        Used by the bounded owner watchdog when finite queue headroom is being
        consumed. The explicit close boundary and any late rejection remain
        observable; no reconnect or continuity qualification is manufactured.
        """
        self._close_requested.set()
        with self._lock:
            loop, task = self._loop, self._task
        self._handoff.close(self._clock_ns())
        if loop is not None and task is not None and not task.done():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                if not loop.is_closed():
                    raise

    def close(self, *, timeout_seconds: float = 5.0) -> None:
        """Cancel the sole producer, close its bounded handoff, and join for a bounded time."""
        if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 30.0:
            raise ValueError("close timeout must be finite and no greater than thirty seconds")
        self.request_close()
        with self._lock:
            thread = self._thread
            if thread is None:
                self._state = "CLOSED"
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout_seconds)
            if thread.is_alive():
                raise TimeoutError("public stream producer did not stop within its bounded close wait")
        self._handoff.close(self._clock_ns())
        with self._lock:
            self._state = "CLOSED"

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with self._lock:
            self._loop = loop
            self._task = loop.create_task(self._run())
        self._thread_ready.set()
        try:
            loop.run_until_complete(self._task)
        except asyncio.CancelledError:
            pass
        except Exception:
            with self._lock:
                self._state = "FAILED"
                self._last_error_code = "PRODUCER_LOOP_FAILURE"
            self._handoff.observe_error("PRODUCER_LOOP_FAILURE", self._clock_ns())
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            with self._lock:
                self._loop = None
                self._task = None
                if self._close_requested.is_set():
                    self._state = "CLOSED"
                elif self._state == "RUNNING":
                    self._state = "EXHAUSTED"
            self._handoff.close(self._clock_ns())

    async def _run(self) -> None:
        for attempt_index in range(self._max_attempts):
            if self._close_requested.is_set():
                return
            with self._lock:
                self._attempt_count += 1
                connection_epoch = self._attempt_count
                if attempt_index:
                    self._reconnect_count += 1
            try:
                await handoff_public_frames(
                    self._factory(), handoff=self._handoff,
                    connection_epoch=connection_epoch, clock_ns=self._clock_ns,
                )
                outcome = "STREAM_ENDED"
                self._handoff.observe_error(outcome, self._clock_ns())
            except asyncio.CancelledError:
                raise
            except PublicFrameHandoffOverflowV2:
                with self._lock:
                    self._state = "FAILED"
                    self._last_error_code = "FRAME_QUEUE_OVERFLOW"
                return
            except Exception as exc:
                error_code = self._error_code_for_exception(exc)
                with self._lock:
                    self._last_error_code = error_code
                # Refresh status for factory failures and retain only stable codes.
                self._handoff.observe_error(error_code, self._clock_ns())
            if attempt_index + 1 < self._max_attempts:
                delay = min(self._initial_backoff_seconds * (2 ** attempt_index), self._max_backoff_seconds)
                await asyncio.sleep(delay)
        with self._lock:
            if self._state == "RUNNING":
                self._last_error_code = "RECONNECT_EXHAUSTED"
        self._handoff.observe_error("RECONNECT_EXHAUSTED", self._clock_ns())

    @staticmethod
    def _error_code_for_exception(exc: Exception) -> str:
        return "STREAM_" + type(exc).__name__.upper()[:52]
