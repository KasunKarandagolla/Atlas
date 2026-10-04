"""Bounded public REST acquisition while the sole writer services stream evidence."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from typing import Any

from ..data.bybit_source import BybitPublicSnapshotV1


class ServicedPublicAcquisitionV1:
    """One read-only acquisition worker and one retained immutable completion.

    The caller remains the sole persistence owner. Its service callback runs on
    that caller's thread, never on the acquisition thread. A timed-out worker
    stays pending: subsequent calls service the stream while waiting for that
    same request instead of creating additional workers. Completion timestamps
    are returned unchanged even when their consumption occurs in a later cycle.
    """

    def __init__(
        self,
        source: Any,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        max_wait_s: float = 5.0,
        poll_interval_s: float = 0.02,
    ) -> None:
        if not math.isfinite(max_wait_s) or not 0 < max_wait_s <= 5.0:
            raise ValueError("public acquisition wait must be positive and at most five seconds")
        if not math.isfinite(poll_interval_s) or not 0 < poll_interval_s <= 0.02:
            raise ValueError("public acquisition service interval must be positive and at most twenty milliseconds")
        if not callable(getattr(source, "acquire_snapshot", None)):
            raise ValueError("public acquisition source must expose acquire_snapshot")
        self.source = source
        self.clock_ns = clock_ns
        self.monotonic = monotonic
        self.max_wait_s = max_wait_s
        self.poll_interval_s = poll_interval_s
        self._lock = threading.Lock()
        self._completed = threading.Event()
        self._worker: threading.Thread | None = None
        self._result: BybitPublicSnapshotV1 | None = None
        self._closed = False
        self._source_closed = False
        self._started_count = 0
        self._consumed_count = 0
        self._wait_timeout_count = 0

    def _empty_snapshot(
        self, *, now_ns: int, started_at: float, failure_kind: str, reason: str,
    ) -> BybitPublicSnapshotV1:
        return BybitPublicSnapshotV1(
            records=(), complete=False, failure_kind=failure_kind, failure_reason=reason,
            latest_received_at_ns=0, observed_at_ns=max(now_ns, self.clock_ns()),
            acquisition_duration_ns=max(0, int((self.monotonic() - started_at) * 1_000_000_000)),
        )

    def _run(self, *, now_ns: int) -> None:
        started_at = self.monotonic()
        try:
            begin = getattr(self.source, "begin_collection_cycle", None)
            if callable(begin):
                begin(now_ns=now_ns)
            result = self.source.acquire_snapshot(now_ns=now_ns)
            if not isinstance(result, BybitPublicSnapshotV1):
                result = self._empty_snapshot(
                    now_ns=now_ns, started_at=started_at, failure_kind="MALFORMED",
                    reason="BYBIT_PUBLIC_ACQUISITION_WORKER_INVALID_RESULT",
                )
        except Exception:
            # Neither exception messages nor provider response values cross this
            # boundary. Actual receipt-bearing snapshots keep their own identity.
            result = self._empty_snapshot(
                now_ns=now_ns, started_at=started_at, failure_kind="MALFORMED",
                reason="BYBIT_PUBLIC_ACQUISITION_WORKER_FAILED",
            )
        with self._lock:
            self._result = result
            self._completed.set()
            closed = self._closed
        if closed:
            self._close_source()

    def _close_source(self) -> None:
        with self._lock:
            if self._source_closed:
                return
            self._source_closed = True
        close = getattr(self.source, "close", None)
        if callable(close):
            close()

    def acquire(self, *, now_ns: int, service: Callable[[], None]) -> BybitPublicSnapshotV1:
        """Wait a bounded time, servicing FIFO stream evidence between polls.

        The callback's own work must be bounded by its caller. Callback failures
        propagate without consuming or discarding an acquisition completion.
        """
        started_at = self.monotonic()
        deadline = started_at + self.max_wait_s
        with self._lock:
            if self._closed:
                raise RuntimeError("public acquisition worker is closed")
            if self._worker is None:
                self._completed.clear()
                self._worker = threading.Thread(
                    target=self._run, kwargs={"now_ns": now_ns},
                    name="atlas-public-acquisition", daemon=True,
                )
                self._started_count += 1
                self._worker.start()
        while True:
            service()
            with self._lock:
                if self._completed.is_set():
                    result = self._result
                    if result is None:
                        raise RuntimeError("public acquisition completion is absent")
                    self._result = None
                    self._worker = None
                    self._completed.clear()
                    self._consumed_count += 1
                    return result
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                with self._lock:
                    self._wait_timeout_count += 1
                return self._empty_snapshot(
                    now_ns=now_ns, started_at=started_at, failure_kind="INCOMPLETE",
                    reason="BYBIT_PUBLIC_ACQUISITION_WORKER_PENDING",
                )
            self._completed.wait(min(self.poll_interval_s, remaining))

    def status(self) -> dict[str, int | bool]:
        """Return bounded operational counters without provider response values."""
        with self._lock:
            return {
                "closed": self._closed,
                "pending": self._worker is not None and not self._completed.is_set(),
                "completion_ready": self._completed.is_set(),
                "active_workers": int(self._worker is not None and self._worker.is_alive()),
                "started_count": self._started_count,
                "consumed_count": self._consumed_count,
                "wait_timeout_count": self._wait_timeout_count,
            }

    def close(self, *, timeout_s: float = 0.1) -> bool:
        """Reject new work and join for at most 100ms; a blocked worker is daemonized."""
        if not math.isfinite(timeout_s) or not 0 <= timeout_s <= 0.1:
            raise ValueError("public acquisition close wait must be between zero and one hundred milliseconds")
        with self._lock:
            self._closed = True
            worker = self._worker
        if worker is not None:
            worker.join(timeout=timeout_s)
        completed = worker is None or not worker.is_alive()
        if completed:
            self._close_source()
        return completed
