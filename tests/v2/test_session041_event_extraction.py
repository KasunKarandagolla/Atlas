"""Bounded zero-authority S7 extraction over immutable news evidence."""

from __future__ import annotations

import sqlite3

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.agent_intelligence.contracts import AgentEvidenceRefV1, EventExtractionRequestV1, EventExtractionV1
from atlas.v2.agent_intelligence.event_extraction import (
    EVENT_EXTRACTION_SCHEMA_HASH_V1,
    EventExtractionError,
    EventExtractionProviderOutcomeV1,
    build_event_extraction_request_v1,
    complete_event_extraction_v1,
    find_event_extraction_terminal_v1,
    prepare_event_extraction_v1,
    record_event_extraction_indeterminate_v1,
    run_event_extraction_v1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    FetchedDocumentV2,
    NewsCollectionPipelineV2,
    NewsSourceClassV2,
    NewsSourceConfigV2,
)

SOURCE = NewsSourceConfigV2("FIXTURE_NEWS", NewsSourceClassV2.OFFICIAL_MACRO,
                            "https://news.example.org/feed", ("news.example.org",))
BODY = (b'{"articles":[{"title":"Bitcoin security incident","url":"https://news.example.org/a",'
        b'"description":"Officials confirmed a security incident at 12:00 UTC."}]}')


class _Clock:
    def __init__(self, *times: int) -> None:
        self.times = list(times)

    def __call__(self) -> int:
        if len(self.times) > 1:
            return self.times.pop(0)
        return self.times[0]


class _Transport:
    def fetch(self, _source: NewsSourceConfigV2) -> FetchedDocumentV2:
        return FetchedDocumentV2(SOURCE.feed_url, SOURCE.feed_url, 200, BODY, 1_000, {})


def _archive(repository: OpsRepository) -> tuple[str, str, str]:
    result = NewsCollectionPipelineV2(repository, clock_ns=lambda: 1_000,
        transport=_Transport(), sources=(SOURCE,)).collect(SOURCE.source_id)
    receipts = repository.latest_artifact_entries("NewsReceiptV2", as_of_ns=1_000, limit=4)
    assert len(receipts.entries) == 1
    receipt = receipts.entries[0]
    assert receipt.metadata["raw_ref"] == result.raw_ref
    event = repository.get_artifact(result.event_refs[0])
    assert event is not None and event.artifact_type == "NewsEventV2"
    return result.raw_ref, receipt.artifact_ref, event.artifact_ref


def _event(index: int = 0, *, unknown: tuple[str, ...] = ()) -> FrozenMap:
    return FrozenMap({
        "item_index": index,
        "event_type": None if "EVENT_TYPE" in unknown else "SECURITY_INCIDENT",
        "severity": None if "SEVERITY" in unknown else "HIGH",
        "event_time_text": None if "EVENT_TIME" in unknown else "12:00 UTC",
        "asset_mentions": [] if "ASSET_MENTIONS" in unknown else ["Bitcoin"],
        "supporting_spans": ["Bitcoin security incident", "12:00 UTC"],
        "unknown_fields": list(unknown),
    })


class _Provider:
    def __init__(self, repository: OpsRepository, extracted: EventExtractionV1 | None = None,
                 *, error: Exception | None = None) -> None:
        self.repository = repository
        self.extracted = extracted
        self.error = error
        self.calls = 0

    def extract(self, request, evidence):
        self.calls += 1
        # The immutable request and no-repeat dispatch fence are durable before
        # provider code can cause an external effect.
        assert self.repository.get_artifact(request.content_hash) is not None
        rows = self.repository.latest_artifact_entries("EventExtractionDispatchV1",
                                                       as_of_ns=2_000, limit=2)
        assert len(rows.entries) == 1
        assert len(evidence) == 2 and evidence[0]["status"] == "PRESENT"
        assert evidence[0]["rows"][0]["raw_ref"] == request.source_artifact_ref
        assert "tools" not in evidence[0] and "url_fetch" not in evidence[0]
        if self.error is not None:
            raise self.error
        assert self.extracted is not None
        return self.extracted


def test_valid_extraction_is_separate_causal_artifact_and_does_not_edit_news_event(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, event_ref = _archive(repository)
        before = repository.get_artifact(event_ref)
        assert before is not None
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000001",
            deadline_ns=2_000)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        result = run_event_extraction_v1(repository, provider, request=request,
                                         clock_ns=_Clock(1_100, 1_250))
        assert result.status == "VALIDATED" and result.completed_at_ns == 1_250
        output = repository.get_artifact(result.result_ref)
        assert output is not None and output.artifact_type == "EventExtractionArtifactV1"
        assert output.created_at_ns == output.available_at_ns == 1_250
        assert output.metadata["completion_time_source"] == "provider_return_observed_by_ATLAS"
        after = repository.get_artifact(event_ref)
        assert after == before
        assert repository.latest_artifact_entries("EventExtractionValidationReceiptV1",
                                                  as_of_ns=1_250, limit=2).entries


def test_ambiguous_fields_remain_unknown_in_sidecar(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000002",
            deadline_ns=2_000)
        fields = ("EVENT_TYPE", "SEVERITY", "EVENT_TIME", "ASSET_MENTIONS")
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref,
                                                           (_event(unknown=fields),)))
        result = run_event_extraction_v1(repository, provider, request=request,
                                         clock_ns=_Clock(1_100, 1_200))
        assert result.status == "VALIDATED"
        output = repository.get_artifact(result.result_ref)
        assert output is not None
        row = output.metadata["extraction"]["extracted_events"][0]
        assert row["event_type"] is None and row["severity"] is None
        assert row["event_time_text"] is None and row["asset_mentions"] == ()


@pytest.mark.parametrize("bad_row, expected", [
    (FrozenMap({**_event().to_dict(), "supporting_spans": ["not in source"]}),
     "EXTRACTION_SPAN_NOT_IN_SOURCE"),
    (FrozenMap({**_event().to_dict(), "event_time_text": "tomorrow"}),
     "EXTRACTION_TIME_NOT_IN_SOURCE_OR_AMBIGUOUS"),
    (FrozenMap({**_event().to_dict(), "item_index": 7}),
     "EXTRACTION_SCHEMA_INVALID"),
])
def test_unsupported_or_misbound_claim_is_rejected(tmp_path, bad_row, expected) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000003",
            deadline_ns=2_000)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (bad_row,)))
        result = run_event_extraction_v1(repository, provider, request=request,
                                         clock_ns=_Clock(1_100, 1_200))
        assert result.status == "INVALID" and result.result_ref is None
        receipt = repository.get_artifact(result.validation_ref)
        assert receipt is not None and receipt.metadata["receipt"]["reason"] == expected
        assert repository.latest_artifact_entries("EventExtractionArtifactV1",
                                                  as_of_ns=2_000, limit=1).entries == ()


def test_future_archived_evidence_is_rejected_before_provider_call(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000004",
            deadline_ns=2_000)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        with pytest.raises(EventExtractionError, match="ARCHIVED_EVIDENCE_UNAVAILABLE_OR_FUTURE"):
            run_event_extraction_v1(repository, provider, request=request, clock_ns=lambda: 999)
        assert provider.calls == 0


def test_provider_failure_is_durable_and_not_automatically_retried(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000005",
            deadline_ns=2_000)
        provider = _Provider(repository, error=RuntimeError("provider unavailable"))
        result = run_event_extraction_v1(repository, provider, request=request,
                                         clock_ns=_Clock(1_100, 1_200))
        assert result.status == "UNAVAILABLE" and result.result_ref is None
        with pytest.raises(EventExtractionError, match="EXTRACTION_DISPATCH_ALREADY_STARTED_INDETERMINATE"):
            run_event_extraction_v1(repository, provider, request=request, clock_ns=lambda: 1_300)
        assert provider.calls == 1


def test_indeterminate_shutdown_receipt_is_stable_and_idempotent(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000009",
            deadline_ns=2_000)
        prepare_event_extraction_v1(repository, request=request, clock_ns=lambda: 1_100)
        first = record_event_extraction_indeterminate_v1(repository, request, observed_at_ns=1_200)
        second = record_event_extraction_indeterminate_v1(repository, request, observed_at_ns=1_300)
        assert first.status == second.status == "INDETERMINATE"
        assert first.validation_ref == second.validation_ref
        assert first.result_ref is second.result_ref is None
        entry = repository.get_artifact(first.validation_ref)
        assert entry is not None and entry.artifact_type == "EventExtractionValidationReceiptV1"
        assert entry.metadata["receipt"]["reason"] == "CALL_OUTCOME_UNKNOWN"
        assert entry.metadata["receipt"]["completed_at_ns"] == 1_200


def test_terminal_lookup_uses_exact_identity_after_more_than_4096_unrelated_receipts(
    tmp_path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000010",
            deadline_ns=10_000)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        completed = run_event_extraction_v1(repository, provider, request=request,
                                            clock_ns=_Clock(1_100, 1_200))
        unrelated = []
        for index in range(4_105):
            request_ref = sha256_json({"unrelated-request": index})
            metadata = {"receipt": {"request_ref": request_ref, "dispatch_ref": "f" * 64,
                                    "status": "VALIDATED", "reason": None, "result_ref": None,
                                    "candidate_hash": None, "completed_at_ns": 2_000 + index}}
            ref = sha256_json({"unrelated-validation": index})
            unrelated.append(ArtifactIndexEntryV2(ref, "EventExtractionValidationReceiptV1",
                sha256_json(metadata), 2_000 + index, 2_000 + index, metadata))
        repository.register_artifacts(tuple(unrelated))

        calls: list[tuple[str, tuple[str, ...]]] = []
        original_lookup = repository.artifact_entries_by_metadata_identity

        def exact_lookup(artifact_type, metadata_path, identity_value, **kwargs):
            calls.append((artifact_type, tuple(metadata_path)))
            return original_lookup(artifact_type, metadata_path, identity_value, **kwargs)

        monkeypatch.setattr(repository, "latest_artifact_entries",
                            lambda *_args, **_kwargs: pytest.fail("terminal lookup used a recent-row scan"))
        monkeypatch.setattr(repository, "artifact_entries_by_metadata_identity", exact_lookup)
        terminal = find_event_extraction_terminal_v1(repository, request, as_of_ns=10_000)
        assert terminal is not None and terminal.status == "VALIDATED"
        assert terminal.validation_ref == completed.validation_ref
        assert calls == [("EventExtractionValidationReceiptV1", ("receipt", "request_ref"))]


def test_terminal_receipt_identity_index_is_rebuilt_without_schema_version_change(tmp_path) -> None:
    database = tmp_path / "existing-ops.sqlite"
    index_name = "created_metadata_" + sha256_json([
        "EventExtractionValidationReceiptV1", ["receipt", "request_ref"],
    ])[:16]
    with OpsRepository(database) as repository:
        original_schema_version = repository.schema_version
    connection = sqlite3.connect(database)
    try:
        connection.execute(f"DROP INDEX {index_name}")
        connection.commit()
    finally:
        connection.close()
    with OpsRepository(database) as repository:
        assert repository.schema_version == original_schema_version
        rows = repository._connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='index' AND name=?", (index_name,),
        ).fetchall()
        assert len(rows) == 1


def test_conflicting_terminal_receipts_and_pre_dispatch_observation_fail_closed(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000011",
            deadline_ns=10_000)
        prepared = prepare_event_extraction_v1(repository, request=request, clock_ns=lambda: 1_100)
        with pytest.raises(EventExtractionError, match="EXTRACTION_INDETERMINATE_CLOCK_REGRESSION"):
            record_event_extraction_indeterminate_v1(repository, request, observed_at_ns=1_099)
        valid = EventExtractionV1(request.request_id, raw_ref, (_event(),))
        completed = complete_event_extraction_v1(repository, prepared,
            outcome=EventExtractionProviderOutcomeV1(valid, 1_200), clock_ns=lambda: 1_200)
        record_event_extraction_indeterminate_v1(repository, request, observed_at_ns=1_300)
        with pytest.raises(EventExtractionError, match="EXTRACTION_TERMINAL_RECEIPTS_CONFLICT"):
            find_event_extraction_terminal_v1(repository, request, as_of_ns=2_000)
        assert completed.status == "VALIDATED"


def test_late_provider_result_is_retained_as_ineligible_candidate(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000007",
            deadline_ns=2_000)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        result = run_event_extraction_v1(repository, provider, request=request,
                                         clock_ns=_Clock(1_100, 2_100))
        assert result.status == "LATE_RETROSPECTIVE" and result.result_ref is None
        candidate = repository.get_artifact(result.candidate_ref)
        assert candidate is not None and candidate.artifact_type == "EventExtractionCandidateV1"
        assert candidate.available_at_ns == 2_100
        assert candidate.metadata["eligible"] is False


def test_request_cannot_substitute_an_unregistered_receipt(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        initial = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000008",
            deadline_ns=2_000)
        request = EventExtractionRequestV1(initial.request_id, raw_ref,
            (initial.evidence_manifest[0], AgentEvidenceRefV1("get_registered_artifact", "f" * 64,
                                                               initial.evidence_manifest[1].available_through_ns)),
            initial.deadline_ns, EVENT_EXTRACTION_SCHEMA_HASH_V1)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        with pytest.raises(EventExtractionError, match="ARCHIVED_EVIDENCE_UNAVAILABLE_OR_FUTURE"):
            run_event_extraction_v1(repository, provider, request=request, clock_ns=lambda: 1_100)
        assert provider.calls == 0


def test_request_schema_hash_is_fixed_and_tampering_rejected(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw_ref, receipt_ref, _ = _archive(repository)
        request = build_event_extraction_request_v1(repository, raw_ref=raw_ref,
            receipt_ref=receipt_ref, request_id="00000000-0000-4000-8000-000000000006",
            deadline_ns=2_000, schema_hash="f" * 64)
        provider = _Provider(repository, EventExtractionV1(request.request_id, raw_ref, (_event(),)))
        with pytest.raises(EventExtractionError, match="EXTRACTION_SCHEMA_HASH_MISMATCH"):
            run_event_extraction_v1(repository, provider, request=request, clock_ns=lambda: 1_100)
        assert provider.calls == 0
        assert len(EVENT_EXTRACTION_SCHEMA_HASH_V1) == 64
