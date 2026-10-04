"""A single bounded background slot for the installed read-only report export.

The caller supplies a zero-argument export over a run path, never the live
writer repository. Completed results occupy the same slot until polled. A
slow export cannot accumulate queued jobs or block public-stream servicing.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ReportWorkerCompletionV1:
    status: str
    result: object | None
    error_type: str | None
    started_at_ns: int
    completed_at_ns: int


class ReadOnlyReportWorkerV1:
    """One daemon worker and one result; no queued reruns or database writer."""

    def __init__(self, export: Callable[[], object]) -> None:
        if not callable(export):
            raise ValueError("report export must be callable")
        self._export = export
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._completion: ReportWorkerCompletionV1 | None = None
        self._closed = False

    def start(self) -> bool:
        """Start one export, refusing requests while its slot is occupied."""
        with self._lock:
            if (self._closed or self._completion is not None
                    or (self._thread is not None and self._thread.is_alive())):
                return False
            self._thread = threading.Thread(target=self._run,
                name="atlas-read-only-report", daemon=True)
            self._thread.start()
            return True

    def _run(self) -> None:
        started = time.time_ns()
        result: object | None = None
        error_type: str | None = None
        try:
            result = self._export()
        except Exception as exc:
            candidate = type(exc).__name__
            error_type = (candidate if candidate.isascii() and candidate.isidentifier()
                          and len(candidate) <= 64 else "Exception")
        completion = ReportWorkerCompletionV1(
            "IMPLEMENTED" if error_type is None else "TEST GATE",
            result, error_type, started, max(started, time.time_ns()),
        )
        with self._lock:
            self._completion = completion

    def poll(self) -> ReportWorkerCompletionV1 | None:
        """Return the completed export once; do not wait for active work."""
        with self._lock:
            result, self._completion = self._completion, None
            return result

    def close(self, timeout_s: float = 1.0) -> bool:
        """Refuse future exports and join for at most the supplied allowance.

        False means an export is still running. The caller must not start a
        synchronous final export over the same destination in that case.
        The daemon can finish its existing read-only job; it is not killed.
        """
        if not math.isfinite(timeout_s) or not 0 <= timeout_s <= 30:
            raise ValueError("report close timeout must be between zero and thirty seconds")
        with self._lock:
            self._closed = True
            thread = self._thread
        if thread is not None:
            thread.join(timeout_s)
            return not thread.is_alive()
        return True
