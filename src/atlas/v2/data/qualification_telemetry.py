"""Bounded event-time counters used only by the public qualification diagnostic."""

from __future__ import annotations

from typing import Any

NANOSECONDS_PER_SECOND = 1_000_000_000
MAX_TRAFFIC_WINDOW_BUCKETS = 512


class QualificationTrafficWindowV1:
    """Aggregate exact accepted events into a fixed, one-second window.

    The owning handoff/capture/runtime provides synchronization. No per-event
    records are retained; memory is bounded by 512 seconds and the stream's
    existing channel cap.
    """

    def __init__(self) -> None:
        self._start_ns: int | None = None
        self._end_ns: int | None = None
        self._buckets: dict[int, dict[tuple[str, str], list[int | None]]] = {}
        self._totals: dict[tuple[str, str], list[int | None]] = {}
        self._history_truncated = False

    def begin_window(self, start_ns: int, end_ns: int) -> None:
        if (type(start_ns) is not int or type(end_ns) is not int or start_ns < 0
                or end_ns <= start_ns
                or end_ns - start_ns > MAX_TRAFFIC_WINDOW_BUCKETS * NANOSECONDS_PER_SECOND):
            raise ValueError("qualification traffic window is outside its fixed bound")
        self._start_ns, self._end_ns = start_ns, end_ns
        self._buckets.clear()
        self._totals.clear()
        self._history_truncated = False

    def record(self, *, at_ns: int, lane: str, channel: str, payload_bytes: int) -> None:
        if (type(at_ns) is not int or type(payload_bytes) is not int or payload_bytes < 0
                or not lane or len(lane) > 128 or not channel or len(channel) > 256):
            raise ValueError("qualification traffic event is invalid")
        start, end = self._start_ns, self._end_ns
        if start is None or end is None or at_ns < start or at_ns >= end:
            return
        bucket_index = (at_ns - start) // NANOSECONDS_PER_SECOND
        identity = lane, channel
        self._increment(self._totals, identity, payload_bytes)
        bucket = self._buckets.setdefault(bucket_index, {})
        self._increment(bucket, identity, payload_bytes)
        if len(self._buckets) > MAX_TRAFFIC_WINDOW_BUCKETS:
            oldest = min(self._buckets)
            del self._buckets[oldest]
            self._history_truncated = True

    @staticmethod
    def _increment(target: dict[tuple[str, str], list[int | None]],
                    identity: tuple[str, str], payload_bytes: int) -> None:
        values = target.get(identity)
        if values is None:
            target[identity] = [1, payload_bytes, payload_bytes, payload_bytes]
            return
        values[0] = int(values[0] or 0) + 1
        values[1] = int(values[1] or 0) + payload_bytes
        values[2] = min(int(values[2]) if values[2] is not None else payload_bytes, payload_bytes)
        values[3] = max(int(values[3]) if values[3] is not None else payload_bytes, payload_bytes)

    @staticmethod
    def _wire(values: dict[tuple[str, str], list[int | None]]) -> list[dict[str, Any]]:
        return [{"lane": lane, "channel": channel, "frames": int(count or 0),
                 "payload_bytes": int(size or 0), "payload_min_bytes": minimum,
                 "payload_max_bytes": maximum}
                for (lane, channel), (count, size, minimum, maximum)
                in sorted(values.items())]

    def snapshot(self) -> dict[str, Any]:
        start, end = self._start_ns, self._end_ns
        if start is None or end is None:
            return {"window_started": False, "history_complete": False,
                    "window_start_monotonic_ns": None, "window_end_monotonic_ns": None,
                    "identities": [], "buckets": []}
        expected_buckets = (end - start + NANOSECONDS_PER_SECOND - 1) // NANOSECONDS_PER_SECOND
        return {
            "window_started": True,
            "history_complete": not self._history_truncated,
            "window_start_monotonic_ns": start,
            "window_end_monotonic_ns": end,
            "expected_bucket_count": expected_buckets,
            "identities": self._wire(self._totals),
            "buckets": [{"second": index, "identities": self._wire(values)}
                for index, values in sorted(self._buckets.items())],
        }


class QualificationPhaseWindowV1:
    """Keep fixed-size count/sum/max timing totals for one diagnostic window."""

    def __init__(self, phase_names: tuple[str, ...]) -> None:
        if not phase_names or len(set(phase_names)) != len(phase_names):
            raise ValueError("qualification phase names must be unique")
        self._phase_names = phase_names
        self._start_ns: int | None = None
        self._end_ns: int | None = None
        self._totals: dict[str, list[int]] = {name: [0, 0, 0] for name in phase_names}

    def begin_window(self, start_ns: int, end_ns: int) -> None:
        if (type(start_ns) is not int or type(end_ns) is not int or start_ns < 0
                or end_ns <= start_ns or end_ns - start_ns > 300_000_000_000):
            raise ValueError("qualification phase window is outside its fixed bound")
        self._start_ns, self._end_ns = start_ns, end_ns
        self._totals = {name: [0, 0, 0] for name in self._phase_names}

    def record(self, *, at_ns: int, phase: str, duration_ns: int) -> None:
        if type(at_ns) is not int or type(duration_ns) is not int or duration_ns < 0:
            raise ValueError("qualification phase timing is invalid")
        if phase not in self._totals:
            raise ValueError("qualification phase timing name is unknown")
        if self._start_ns is None or self._end_ns is None or not self._start_ns <= at_ns < self._end_ns:
            return
        stats = self._totals[phase]
        stats[0] += 1
        stats[1] += duration_ns
        stats[2] = max(stats[2], duration_ns)

    def snapshot(self) -> dict[str, object]:
        if self._start_ns is None or self._end_ns is None:
            return {"window_started": False, "history_complete": False, "phases": {}}
        return {"window_started": True, "history_complete": True,
            "window_start_monotonic_ns": self._start_ns,
            "window_end_monotonic_ns": self._end_ns,
            "phases": {name: {"count": values[0], "sum_ns": values[1], "max_ns": values[2]}
                for name, values in self._totals.items()}}
