"""Installed context maintenance: bounded acquisition, one writer, exact evidence."""

from __future__ import annotations

import json
import threading

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    FetchedDocumentV2,
    NewsCollectionBoundedError,
    NewsCollectionPipelineV2,
    NewsSourceClassV2,
    NewsSourceConfigV2,
    _event_from_dict,
)
from atlas.v2.runtime.public_context import PUBLIC_CONTEXT_CADENCE_NS_V1, PublicContextMaintenanceV1

SOURCE = NewsSourceConfigV2("FIXTURE", NewsSourceClassV2.OFFICIAL_MACRO,
                           "https://news.example.org/feed", ("news.example.org",))
OTHER = NewsSourceConfigV2("OTHER", NewsSourceClassV2.REPUTABLE_NEWS,
                          "https://news.example.org/other", ("news.example.org",))
BODY = b'{"articles":[{"title":"Bitcoin security incident","url":"https://news.example.org/a","description":"BTC exploit"}]}'


class _Clock:
    def __init__(self, at: int = 100) -> None:
        self.at = at

    def __call__(self) -> int:
        return self.at


class _Transport:
    def __init__(self, clock: _Clock, body: bytes = BODY, *, block: threading.Event | None = None,
                 error: Exception | None = None) -> None:
        self.clock, self.body, self.block, self.error = clock, body, block, error
        self.calls: list[tuple[str, int]] = []
        self.started = threading.Event()

    def fetch(self, source: NewsSourceConfigV2) -> FetchedDocumentV2:
        self.calls.append((source.source_id, threading.get_ident()))
        self.started.set()
        if self.block is not None:
            assert self.block.wait(timeout=2)
        if self.error is not None:
            raise self.error
        return FetchedDocumentV2(source.feed_url, source.feed_url, 200, self.body, self.clock(), {})


def _complete(maintenance: PublicContextMaintenanceV1) -> None:
    assert maintenance._thread is not None
    maintenance._thread.join(timeout=2)
    assert not maintenance._thread.is_alive()


def _pipeline(repository: OpsRepository, clock: _Clock, body: bytes = BODY,
              *, publication_clock=None) -> NewsCollectionPipelineV2:
    return NewsCollectionPipelineV2(repository, clock_ns=clock, transport=_Transport(clock, body),
                                    sources=(SOURCE,), publication_clock_ns=publication_clock)


def test_constructor_is_offline_and_worker_never_writes_repository(tmp_path, monkeypatch) -> None:
    clock, writer = _Clock(), threading.get_ident()
    transport = _Transport(clock)
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=transport, sources=(SOURCE,))
    assert transport.calls == []
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        writes: list[int] = []
        original = repository.register_artifacts

        def checked(entries):
            writes.append(threading.get_ident())
            return original(entries)

        monkeypatch.setattr(repository, "register_artifacts", checked)
        monkeypatch.setattr(repository, "artifact_entries", lambda *_args: pytest.fail("unbounded scan"))
        first = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert first.status == "FETCH_STARTED"
        _complete(maintenance)
        clock.at = 110
        result = maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert result.status == "COLLECTED" and len(result.event_refs) == 1
        assert result.fetch_completed_at_ns == 100 and result.available_at_ns == 110
        event = repository.get_artifact(result.event_refs[0])
        assert event is not None and event.available_at_ns == 110
        assert event.metadata["event"]["authentication_state"] == "UNKNOWN"
        assert event.metadata["event"]["source_health_state"] == "UNKNOWN"
        assert result.alert_refs == ()
        assert writes and set(writes) == {writer}
        assert transport.calls[0][1] != writer
        count = len(writes)
        assert maintenance.run_cycle(repository, information_cutoff_ns=110) == result
        assert len(writes) == count
    maintenance.close()


def test_asset_mapping_configuration_is_bound_to_policy_identity() -> None:
    first = PublicContextMaintenanceV1(sources=(SOURCE,), asset_aliases={"bitcoin": "BTC"})
    second = PublicContextMaintenanceV1(sources=(SOURCE,), asset_aliases={"bitcoin": "OTHER"})
    assert first.policy_hash != second.policy_hash
    assert first.policy["asset_aliases"] == {"bitcoin": "BTC"}
    first.close()
    second.close()


def test_future_completion_slot_stays_pending_until_cutoff_visible(tmp_path) -> None:
    clock = _Clock()
    transport = _Transport(clock)
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=transport, sources=(SOURCE,))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        first = maintenance.run_cycle(repository, information_cutoff_ns=99)
        _complete(maintenance)
        assert maintenance.run_cycle(repository, information_cutoff_ns=99) is first
        assert maintenance._slot is not None
        assert repository.latest_artifact_entries("NewsRawPayloadV2", as_of_ns=100, limit=1).entries == ()
        assert len(transport.calls) == 1
        clock.at = 101
        assert maintenance.run_cycle(repository, information_cutoff_ns=100).status == "COLLECTED"
    maintenance.close()


def test_inflight_timeout_is_visible_once_and_never_starts_second_worker(tmp_path) -> None:
    clock, release = _Clock(), threading.Event()
    transport = _Transport(clock, block=release)
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=transport, sources=(SOURCE,))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance.run_cycle(repository, information_cutoff_ns=100)
        assert transport.started.wait(timeout=1)
        clock.at += 3_000_000_000
        pressure = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert pressure.status == "PRESSURE" and pressure.inflight_count == 1
        assert maintenance.run_cycle(repository, information_cutoff_ns=clock.at) is pressure
        assert len(transport.calls) == 1
        maintenance.close(timeout_s=0)
        release.set()
        _complete(maintenance)
        assert maintenance._slot is None
        closed = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert closed.status == "CLOSED"
        assert maintenance.run_cycle(repository, information_cutoff_ns=clock.at) is closed
        assert len(transport.calls) == 1


def test_round_robin_cadence_and_restart_dedupe_are_explicit(tmp_path) -> None:
    clock, transport = _Clock(), None
    transport = _Transport(clock)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=transport, sources=(SOURCE, OTHER))
        maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(maintenance)
        first = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        clock.at += PUBLIC_CONTEXT_CADENCE_NS_V1
        maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(maintenance)
        second = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert [call[0] for call in transport.calls] == ["FIXTURE", "OTHER"]
        assert second.status == "COLLECTED"
        assert maintenance.policy["restart_cursor"] == "FIRST_CONFIGURED_SOURCE"
        maintenance.close()
        restarted = PublicContextMaintenanceV1(clock_ns=clock, transport=transport, sources=(SOURCE, OTHER))
        restarted.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(restarted)
        duplicate = restarted.run_cycle(repository, information_cutoff_ns=clock.at)
        assert duplicate.event_refs == first.event_refs and duplicate.duplicate_count == 1
        assert duplicate.raw_ref == first.raw_ref and duplicate.alert_refs == ()
        restarted.close()


def test_parse_bound_refuses_whole_batch_after_raw_archive(tmp_path) -> None:
    clock = _Clock()
    body = json.dumps({"articles": [{"title": f"item {i}", "url": f"https://news.example.org/{i}"}
                                    for i in range(129)]}).encode()
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock, body), sources=(SOURCE,))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(maintenance)
        report = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert report.status == "NOT_ESTIMABLE" and report.reasons == ("NEWS_PARSE_ITEM_LIMIT_EXCEEDED",)
        assert report.raw_ref is not None and repository.get_artifact(report.raw_ref) is not None
        assert repository.latest_artifact_entries("NewsEventV2", as_of_ns=clock.at, limit=1).entries == ()
    maintenance.close()


@pytest.mark.parametrize("body", [b"unsupported HTML", b"<broken-xml"])
def test_unsupported_feed_keeps_raw_and_structured_failure(tmp_path, body) -> None:
    clock = _Clock()
    maintenance = PublicContextMaintenanceV1(clock_ns=clock, transport=_Transport(clock, body), sources=(SOURCE,))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(maintenance)
        report = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert report.status == "NOT_ESTIMABLE" and report.raw_ref is not None
        assert report.reasons == ("PUBLIC_CONTEXT_COLLECTION_FAILED",)
    maintenance.close()


def test_fetch_failure_report_does_not_copy_sensitive_exception_strings(tmp_path) -> None:
    clock = _Clock()
    maintenance = PublicContextMaintenanceV1(clock_ns=clock,
        transport=_Transport(clock, error=RuntimeError("test-private-token=DO_NOT_LOG")), sources=(SOURCE,))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        _complete(maintenance)
        report = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert report.status == "FAILED" and report.reasons == ("PUBLIC_CONTEXT_FETCH_FAILED",)
        assert "DO_NOT_LOG" not in json.dumps(report.to_dict())
        assert report.raw_ref is None
    maintenance.close()


def test_url_history_overflow_is_explicit_and_never_silently_selects_latest(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        original = _pipeline(repository, clock).collect(SOURCE.source_id)
        entry = repository.get_artifact(original.event_refs[0])
        assert entry is not None
        entries = []
        for index in range(128):
            body = dict(entry.metadata["event"])
            body["event_id"] = sha256_json({"historical-version": index})
            event = _event_from_dict(body)
            entries.append(ArtifactIndexEntryV2(event.event_id, "NewsEventV2", event.semantic_hash,
                                                clock.at, event.available_at_ns, {"event": event.to_dict()}))
        repository.register_artifacts(tuple(entries))
        clock.at += 1
        with pytest.raises(NewsCollectionBoundedError, match="NEWS_IDENTITY_HISTORY_OVERFLOW_OR_CORRUPT") as error:
            _pipeline(repository, clock).collect(SOURCE.source_id)
        assert repository.get_artifact(error.value.raw_ref) is not None


def test_corrupt_selected_event_fails_closed_even_when_body_parses(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        first = _pipeline(repository, clock).collect(SOURCE.source_id)
        entry = repository.get_artifact(first.event_refs[0])
        assert entry is not None
        repository._connection.execute("UPDATE artifact_index SET content_hash=? WHERE artifact_ref=?",
                                       ("a" * 64, entry.artifact_ref))
        clock.at += 1
        with pytest.raises(NewsCollectionBoundedError, match="NEWS_SELECTED_EVENT_CORRUPT"):
            _pipeline(repository, clock).collect(SOURCE.source_id)


def test_actual_item_completion_is_sampled_after_classification_and_mapping(tmp_path) -> None:
    clock = _Clock()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = _pipeline(repository, clock, publication_clock=lambda: 150).collect(SOURCE.source_id)
        entry = repository.get_artifact(result.event_refs[0])
        assert entry is not None and entry.available_at_ns == 150
        assert entry.metadata["event"]["received_at_ns"] == 100
        assert entry.metadata["event"]["extraction_completed_ns"] == 150
        clock.at = 160
        duplicate = _pipeline(repository, clock, publication_clock=lambda: pytest.fail("dedupe must preserve publication")).collect(SOURCE.source_id)
        assert duplicate.duplicate_count == 1 and duplicate.event_refs == result.event_refs


def test_repeated_item_in_same_response_reuses_its_actual_publication(tmp_path) -> None:
    clock = _Clock()
    item = json.loads(BODY)["articles"][0]
    body = json.dumps({"articles": [item, item]}).encode()
    publications = iter((150, 160))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = _pipeline(repository, clock, body, publication_clock=lambda: next(publications)).collect(SOURCE.source_id)
        assert result.duplicate_count == 1
        assert result.event_refs[0] == result.event_refs[1]
        event = repository.get_artifact(result.event_refs[0])
        assert event is not None and event.available_at_ns == 150
        assert next(publications) == 160


def test_publication_clock_cannot_regress_between_items(tmp_path) -> None:
    clock = _Clock()
    item = json.loads(BODY)["articles"][0]
    second = dict(item, url="https://news.example.org/b")
    body = json.dumps({"articles": [item, second]}).encode()
    publications = iter((150, 140))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        with pytest.raises(NewsCollectionBoundedError, match="NEWS_EXTRACTION_CLOCK_REGRESSION") as error:
            _pipeline(repository, clock, body, publication_clock=lambda: next(publications)).collect(SOURCE.source_id)
        assert repository.get_artifact(error.value.raw_ref) is not None
        events = repository.latest_artifact_entries("NewsEventV2", as_of_ns=150, limit=2).entries
        assert len(events) == 1 and events[0].available_at_ns == 150
