"""Public REST worker scheduling regressions; no network or repository writer."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from atlas.v2.data.bybit_source import BybitPublicSnapshotV1
from atlas.v2.runtime.serviced_acquisition import ServicedPublicAcquisitionV1


def snapshot(observed_at_ns: int = 200) -> BybitPublicSnapshotV1:
    return BybitPublicSnapshotV1((), True, None, None, 190, observed_at_ns)


class ControlledSource:
    def __init__(self, result: Any = None, *, delay_s: float = 0) -> None:
        self.result = snapshot() if result is None else result
        self.delay_s = delay_s
        self.release = threading.Event()
        self.started = threading.Event()
        self.block = False
        self.call_count = 0
        self.begin_threads: list[int] = []
        self.acquire_threads: list[int] = []
        self.close_threads: list[int] = []
        self.closed = threading.Event()

    def close(self) -> None:
        self.close_threads.append(threading.get_ident())
        self.closed.set()

    def begin_collection_cycle(self, *, now_ns: int) -> None:
        del now_ns
        self.begin_threads.append(threading.get_ident())

    def acquire_snapshot(self, *, now_ns: int) -> Any:
        del now_ns
        self.call_count += 1
        self.acquire_threads.append(threading.get_ident())
        self.started.set()
        if self.block:
            self.release.wait()
        if self.delay_s:
            time.sleep(self.delay_s)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_slow_rest_services_stream_on_the_calling_thread() -> None:
    source = ControlledSource(delay_s=0.15)
    helper = ServicedPublicAcquisitionV1(source, max_wait_s=0.5)
    services: list[tuple[int, float]] = []
    caller = threading.get_ident()
    result = helper.acquire(now_ns=100, service=lambda: services.append((threading.get_ident(), time.monotonic())))
    assert result is source.result
    assert len(services) >= 5
    assert {thread_id for thread_id, _ in services} == {caller}
    assert source.begin_threads == source.acquire_threads
    assert source.acquire_threads[0] != caller
    assert helper.status()["consumed_count"] == 1
    assert helper.close()


def test_timeout_retains_one_worker_and_original_receipt_for_later_consumption() -> None:
    source = ControlledSource()
    source.block = True
    helper = ServicedPublicAcquisitionV1(source, clock_ns=lambda: 500, max_wait_s=0.03)
    services: list[int] = []
    try:
        for _ in range(3):
            timed_out = helper.acquire(now_ns=300, service=lambda: services.append(1))
            assert not timed_out.complete
            assert timed_out.failure_reason == "BYBIT_PUBLIC_ACQUISITION_WORKER_PENDING"
            assert timed_out.latest_received_at_ns == 0
            assert timed_out.observed_at_ns == 500
            assert timed_out.acquisition_duration_ns >= 30_000_000
        assert source.call_count == 1
        assert helper.status()["active_workers"] == 1
        assert helper.status()["started_count"] == 1
        assert helper.status()["wait_timeout_count"] == 3
        assert len(services) >= 6
        source.release.set()
        result = helper.acquire(now_ns=600, service=lambda: services.append(1))
        assert result is source.result
        assert result.observed_at_ns == 200
        assert result.latest_received_at_ns == 190
        assert helper.status()["consumed_count"] == 1
        assert not helper.status()["pending"]
        # Only consuming the retained completion permits a subsequent request.
        next_result = helper.acquire(now_ns=700, service=lambda: None)
        for _ in range(20):
            if next_result.complete:
                break
            next_result = helper.acquire(now_ns=700, service=lambda: None)
        assert next_result is source.result
        assert source.call_count == 2
    finally:
        source.release.set()
        helper.close()


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        (ValueError("private response value must not escape"), "BYBIT_PUBLIC_ACQUISITION_WORKER_FAILED"),
        ({"untrusted": "response value"}, "BYBIT_PUBLIC_ACQUISITION_WORKER_INVALID_RESULT"),
    ],
)
def test_worker_failures_are_empty_sanitized_snapshots(result: Any, reason: str) -> None:
    helper = ServicedPublicAcquisitionV1(ControlledSource(result), clock_ns=lambda: 900)
    failed = helper.acquire(now_ns=700, service=lambda: None)
    assert failed.failure_kind == "MALFORMED"
    assert failed.failure_reason == reason
    assert failed.records == ()
    assert failed.latest_received_at_ns == 0
    assert failed.observed_at_ns == 900
    assert "private" not in repr(failed)
    assert "response value" not in repr(failed)
    assert helper.close()


def test_service_exception_leaves_completion_available_once() -> None:
    source = ControlledSource()
    helper = ServicedPublicAcquisitionV1(source, max_wait_s=0.5)

    def broken_service() -> None:
        assert source.started.wait(0.2)
        raise ValueError("service failed")

    with pytest.raises(ValueError, match="service failed"):
        helper.acquire(now_ns=100, service=broken_service)
    assert helper.status()["consumed_count"] == 0
    assert helper.acquire(now_ns=300, service=lambda: None) is source.result
    assert source.call_count == 1
    assert helper.status()["consumed_count"] == 1
    assert helper.close()


def test_close_is_bounded_with_blocked_worker_and_rejects_new_requests() -> None:
    source = ControlledSource()
    source.block = True
    helper = ServicedPublicAcquisitionV1(source, max_wait_s=0.01)
    try:
        assert not helper.acquire(now_ns=100, service=lambda: None).complete
        started = time.monotonic()
        assert not helper.close(timeout_s=0.01)
        assert time.monotonic() - started < 0.2
        assert helper.status()["closed"]
        with pytest.raises(RuntimeError, match="closed"):
            helper.acquire(now_ns=200, service=lambda: None)
        assert source.call_count == 1
        assert not source.closed.is_set()
    finally:
        source.release.set()
        assert helper.close(timeout_s=0.1)
        assert source.closed.wait(0.2)
        assert len(source.close_threads) == 1


@pytest.mark.parametrize("wait_s", [0, -1, 5.01, float("nan"), float("inf")])
def test_wait_bound_is_enforced(wait_s: float) -> None:
    with pytest.raises(ValueError):
        ServicedPublicAcquisitionV1(ControlledSource(), max_wait_s=wait_s)


@pytest.mark.parametrize("poll_s", [0, -1, 0.021, float("nan")])
def test_service_interval_bound_is_enforced(poll_s: float) -> None:
    with pytest.raises(ValueError):
        ServicedPublicAcquisitionV1(ControlledSource(), poll_interval_s=poll_s)
