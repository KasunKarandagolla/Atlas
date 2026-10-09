from __future__ import annotations

import threading
import time

import pytest

from atlas.v2._serialization import FrozenMap
from atlas.v2.agent_intelligence.contracts import AgentEvidenceRefV1, EventExtractionRequestV1, EventExtractionV1
from atlas.v2.agent_intelligence.event_extraction import EventExtractionError, EventExtractionRunV1
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime import public_context
from atlas.v2.runtime.public_context import PublicContextCycleReportV1, PublicContextMaintenanceV1
from tests.v2.test_session037_public_context import SOURCE, _Clock, _complete, _Transport


class _SlowProvider:
    def __init__(self, repository: OpsRepository) -> None:
        self.repository = repository
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def extract(self, request, evidence):
        self.calls += 1
        assert self.repository.get_artifact(request.content_hash) is not None
        self.entered.set()
        self.release.wait()
        return EventExtractionV1(request.request_id, request.source_artifact_ref, (FrozenMap({
            "item_index": 0, "event_type": "SECURITY_INCIDENT", "severity": "HIGH",
            "event_time_text": None, "asset_mentions": ["Bitcoin"],
            "supporting_spans": ["Bitcoin security incident"], "unknown_fields": ["EVENT_TIME"],
        }),))


def _start_collection(maintenance: PublicContextMaintenanceV1, repository: OpsRepository,
                      clock: _Clock) -> None:
    maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
    _complete(maintenance)
    clock.at += 10
    report = maintenance.run_cycle(repository, information_cutoff_ns=100)
    assert report.status == "COLLECTED"


def test_slow_provider_does_not_block_public_cycle_and_result_persists_on_next_cycle(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        provider = _SlowProvider(repository)
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock),
            sources=(SOURCE,), event_extraction_provider=provider)
        _start_collection(maintenance, repository, clock)
        assert provider.entered.wait(1)
        started = time.monotonic()
        pending = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert time.monotonic() - started < 0.25
        assert pending.extraction_status == "PENDING"
        assert repository.latest_artifact_entries("EventExtractionArtifactV1", as_of_ns=clock.at,
                                                  limit=1).entries == ()

        provider.release.set()
        maintenance._extraction_thread.join(timeout=1)
        assert not maintenance._extraction_thread.is_alive()
        assert repository.latest_artifact_entries("EventExtractionArtifactV1", as_of_ns=clock.at,
                                                  limit=1).entries == ()
        completed = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert completed.extraction_status == "VALIDATED"
        assert repository.get_artifact(completed.extraction_result_ref) is not None
        maintenance.close()


def test_provider_error_is_stable_and_never_retried(tmp_path) -> None:
    clock = _Clock()

    class BrokenProvider:
        calls = 0

        def extract(self, _request, _evidence):
            self.calls += 1
            raise RuntimeError("private provider details")

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        provider = BrokenProvider()
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock),
            sources=(SOURCE,), event_extraction_provider=provider)
        _start_collection(maintenance, repository, clock)
        maintenance._extraction_thread.join(timeout=1)
        failed = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert failed.extraction_status == "UNAVAILABLE"
        assert failed.extraction_validation_ref
        for _ in range(3):
            maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert provider.calls == 1
        assert "private provider details" not in repr(failed.to_dict())
        maintenance.close()


def test_timeout_is_durable_no_retry_and_close_is_bounded(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(public_context, "PUBLIC_CONTEXT_EXTRACTION_TIMEOUT_S_V1", 0.01)
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        provider = _SlowProvider(repository)
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock),
            sources=(SOURCE,), event_extraction_provider=provider)
        _start_collection(maintenance, repository, clock)
        assert provider.entered.wait(1)
        time.sleep(0.02)
        failed = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert failed.extraction_status == "UNAVAILABLE"
        assert failed.extraction_validation_ref
        provider.release.set()
        maintenance._extraction_thread.join(timeout=1)
        for _ in range(2):
            maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert provider.calls == 1
        maintenance.close()

    with OpsRepository(tmp_path / "shutdown.sqlite") as repository:
        clock2 = _Clock()
        provider2 = _SlowProvider(repository)
        maintenance2 = PublicContextMaintenanceV1(clock_ns=clock2, transport=_Transport(clock2),
            sources=(SOURCE,), event_extraction_provider=provider2)
        _start_collection(maintenance2, repository, clock2)
        assert provider2.entered.wait(1)
        worker = maintenance2._extraction_thread
        assert worker is not None
        join_timeouts: list[float | None] = []
        original_join = worker.join

        def record_join_timeout(timeout: float | None = None) -> None:
            join_timeouts.append(timeout)
            original_join(timeout)

        worker.join = record_join_timeout
        maintenance2.close(timeout_s=0.02)
        assert join_timeouts and all(timeout is not None and 0 <= timeout <= 0.02 for timeout in join_timeouts)
        assert worker.is_alive()
        provider2.release.set()


def test_extraction_can_outlive_short_public_fetch_timeout(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(public_context, "PUBLIC_CONTEXT_TIMEOUT_S_V1", 0.01)
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        provider = _SlowProvider(repository)
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock),
            sources=(SOURCE,), event_extraction_provider=provider)
        _start_collection(maintenance, repository, clock)
        assert provider.entered.wait(1)
        time.sleep(0.02)
        provider.release.set()
        maintenance._extraction_thread.join(timeout=1)
        assert not maintenance._extraction_thread.is_alive()
        completed = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert completed.extraction_status == "VALIDATED"
        assert provider.calls == 1
        maintenance.close()


def test_close_records_unknown_outcome_without_fabricating_or_retrying(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        provider = _SlowProvider(repository)
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock),
            sources=(SOURCE,), event_extraction_provider=provider)
        _start_collection(maintenance, repository, clock)
        assert provider.entered.wait(1)
        report = maintenance._last_report
        assert report is not None and report.extraction_dispatch_ref
        worker = maintenance._extraction_thread
        assert worker is not None
        join_timeouts: list[float | None] = []
        original_join = worker.join

        def record_join_timeout(timeout: float | None = None) -> None:
            join_timeouts.append(timeout)
            original_join(timeout)

        worker.join = record_join_timeout
        maintenance.close(timeout_s=0)
        assert join_timeouts == [0.0]
        assert worker.is_alive()
        closed_report = maintenance._last_report
        assert closed_report is not None
        assert closed_report.extraction_status == "INDETERMINATE"
        assert closed_report.extraction_validation_ref
        receipt_entry = repository.get_artifact(closed_report.extraction_validation_ref)
        assert receipt_entry is not None
        receipt = receipt_entry.metadata["receipt"]
        assert receipt["status"] == "INDETERMINATE"
        assert receipt["reason"] == "CALL_OUTCOME_UNKNOWN"
        assert receipt["result_ref"] is None and receipt["candidate_hash"] is None
        validation_ref = closed_report.extraction_validation_ref
        maintenance.close(timeout_s=0)
        assert maintenance._last_report.extraction_validation_ref == validation_ref

        provider.release.set()
        maintenance._extraction_thread.join(timeout=1)
        assert not maintenance._extraction_thread.is_alive()
        assert provider.calls == 1
        assert repository.latest_artifact_entries("EventExtractionArtifactV1", as_of_ns=clock.at,
                                                  limit=1).entries == ()
        assert repository.get_artifact(validation_ref).metadata["receipt"]["reason"] == "CALL_OUTCOME_UNKNOWN"


def test_provider_can_be_supplied_after_archived_item(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock), sources=(SOURCE,))
        _start_collection(maintenance, repository, clock)
        assert maintenance._last_report is not None
        assert maintenance._last_report.extraction_status == "DISABLED"
        provider = _SlowProvider(repository)
        provider.release.set()
        maintenance.event_extraction_provider = provider
        maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert provider.entered.wait(1)
        pending = maintenance._last_report
        assert pending is not None
        pending_entry = repository.get_artifact(pending.content_hash)
        assert pending_entry is not None
        assert pending.available_at_ns == pending_entry.available_at_ns
        maintenance._extraction_thread.join(timeout=1)
        complete = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert complete.extraction_status == "VALIDATED"
        assert provider.calls == 1
        maintenance.close()


def test_report_publication_time_cannot_precede_terminal_completion(tmp_path) -> None:
    clock = _Clock(100)
    request = EventExtractionRequestV1(
        "00000000-0000-4000-8000-000000000012", "a" * 64,
        (AgentEvidenceRefV1("get_registered_artifact", "a" * 64, 10),
         AgentEvidenceRefV1("get_registered_artifact", "b" * 64, 20)),
        1_000, "c" * 64,
    )
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock), sources=(SOURCE,))
    maintenance._last_report = PublicContextCycleReportV1(
        100, 100, 100, "d" * 64, None, None, "COLLECTED", (),
        extraction_status="INDETERMINATE", extraction_request_ref=request.content_hash,
    )
    run = EventExtractionRunV1(request, "INDETERMINATE", request.content_hash, "e" * 64,
                               None, None, "f" * 64, 200)
    with (OpsRepository(tmp_path / "ops.sqlite") as repository,
          pytest.raises(EventExtractionError, match="EXTRACTION_PUBLICATION_CLOCK_REGRESSION")):
        maintenance._publish_extraction_result(repository, run)
    maintenance.close()
