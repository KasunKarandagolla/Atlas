"""Installed report scheduling must not pause the sole public evidence writer."""

from __future__ import annotations

import threading
import time

import pytest

from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.read_only_report_worker import ReadOnlyReportWorkerV1


def test_slow_report_has_one_slot_and_does_not_block_caller():
    entered, release = threading.Event(), threading.Event()
    calls = []

    def export():
        calls.append(threading.get_ident())
        entered.set()
        assert release.wait(3)
        return {"has_more": False}

    worker = ReadOnlyReportWorkerV1(export)
    try:
        assert worker.start()
        assert entered.wait(1)
        for _ in range(100):
            assert not worker.start()
            assert worker.poll() is None
        started = time.monotonic()
        assert not worker.close(0)
        assert time.monotonic() - started < 0.25
        release.set()
        assert worker.close(1)
        completion = worker.poll()
        assert completion is not None
        assert completion.status == "IMPLEMENTED"
        assert completion.result == {"has_more": False}
        assert completion.error_type is None
        assert completion.completed_at_ns >= completion.started_at_ns
        assert worker.poll() is None
        assert not worker.start()
        assert len(calls) == 1 and calls[0] != threading.get_ident()
    finally:
        release.set()
        worker.close(1)


def test_completed_report_refuses_rerun_until_polled():
    done = threading.Event()
    worker = ReadOnlyReportWorkerV1(lambda: done.set())
    assert worker.start()
    assert done.wait(1)
    # Join the existing job without closing the reusable worker.
    assert worker._thread is not None
    worker._thread.join(1)
    assert not worker.start()
    assert worker.poll() is not None
    assert worker.start()
    assert worker.close(1)


def test_export_failure_retains_only_safe_error_class():
    def export():
        raise ValueError("credential-or-provider-content-must-never-be-returned")

    worker = ReadOnlyReportWorkerV1(export)
    assert worker.start()
    assert worker.close(1)
    completion = worker.poll()
    assert completion is not None
    assert completion.status == "TEST GATE"
    assert completion.error_type == "ValueError" and completion.result is None
    assert "credential-or-provider" not in repr(completion)


def test_non_ascii_error_class_is_sanitized():
    unsafe_error = type("Unexpected provider text\N{SNOWMAN}", (Exception,), {})

    def export():
        raise unsafe_error("private content")

    worker = ReadOnlyReportWorkerV1(export)
    assert worker.start() and worker.close(1)
    completion = worker.poll()
    assert completion is not None and completion.error_type == "Exception"


@pytest.mark.parametrize("timeout", [-1, 31, float("inf"), float("nan")])
def test_shutdown_timeout_is_bounded(timeout):
    worker = ReadOnlyReportWorkerV1(lambda: None)
    with pytest.raises(ValueError, match="timeout"):
        worker.close(timeout)
    assert worker.close(0)


def test_report_opens_read_only_connection_while_writer_keeps_ownership(tmp_path):
    database = tmp_path / "ops.sqlite"
    inspected, release = threading.Event(), threading.Event()
    identity = "a" * 64
    with OpsRepository(database) as writer:
        writer.register_artifact(ArtifactIndexEntryV2(identity, "ReportWorkerFixtureV1",
            identity, 1, 1, {"authority": "ZERO"}))

        def export():
            with OpsRepository(database, read_only=True) as reader:
                assert reader.get_artifact(identity) is not None
                with pytest.raises(RuntimeError, match="read-only"):
                    reader.register_artifact(ArtifactIndexEntryV2(identity,
                        "ReportWorkerFixtureV1", identity, 1, 1, {"authority": "ZERO"}))
                inspected.set()
                assert release.wait(3)
                return "TESTED"

        worker = ReadOnlyReportWorkerV1(export)
        try:
            assert worker.start() and inspected.wait(1)
            assert writer.get_artifact(identity) is not None
            with pytest.raises(RuntimeError):
                OpsRepository(database)
            release.set()
            assert worker.close(1)
            completion = worker.poll()
            assert completion is not None and completion.error_type is None
        finally:
            release.set()
            worker.close(1)
