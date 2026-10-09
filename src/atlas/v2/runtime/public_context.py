"""Bounded public-context acquisition with persistence only on the caller thread.

The fetch worker owns no repository. A completed response occupies one slot
until its actual receipt/completion is cutoff-visible. Parsing and archiving
use the existing deterministic collector on the installed writer thread.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace

from atlas.v2._serialization import sha256_json, timestamp
from atlas.v2.agent_intelligence.contracts import EventExtractionProvider
from atlas.v2.agent_intelligence.event_extraction import (
    EVENT_EXTRACTION_SCHEMA_HASH_V1,
    EventExtractionError,
    EventExtractionPreparedV1,
    EventExtractionProviderOutcomeV1,
    EventExtractionRunV1,
    build_event_extraction_request_v1,
    complete_event_extraction_v1,
    find_event_extraction_terminal_v1,
    prepare_event_extraction_v1,
    record_event_extraction_indeterminate_v1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    DEFAULT_NEWS_SOURCES_V2,
    NEWS_COLLECTION_MAX_ITEMS_V1,
    PRODUCER_VERSION,
    RAW_BODY_MAX_BYTES,
    FetchedDocumentV2,
    NewsCollectionBoundedError,
    NewsCollectionPipelineV2,
    NewsSourceConfigV2,
    PublicNewsTransportV2,
    canonical_url,
)

PUBLIC_CONTEXT_POLICY_VERSION_V1 = "PublicContextRoundRobinPolicyV1"
PUBLIC_CONTEXT_CADENCE_NS_V1 = 60_000_000_000
PUBLIC_CONTEXT_TIMEOUT_S_V1 = 2.0
PUBLIC_CONTEXT_SOURCE_LIMIT_V1 = 32
PUBLIC_CONTEXT_EXTRACTION_DEADLINE_NS_V1 = 30_000_000_000
PUBLIC_CONTEXT_EXTRACTION_TIMEOUT_S_V1 = (
    PUBLIC_CONTEXT_EXTRACTION_DEADLINE_NS_V1 / 1_000_000_000
)
PUBLIC_CONTEXT_CLOSE_MAX_S_V1 = PUBLIC_CONTEXT_TIMEOUT_S_V1 + 0.1


@dataclass(frozen=True)
class PublicContextCycleReportV1:
    information_cutoff_ns: int
    started_at_ns: int
    available_at_ns: int
    policy_hash: str
    source_id: str | None
    source_ref: str | None
    status: str
    reasons: tuple[str, ...]
    raw_ref: str | None = None
    event_refs: tuple[str, ...] = ()
    alert_refs: tuple[str, ...] = ()
    duplicate_count: int = 0
    fetch_started_at_ns: int | None = None
    fetch_completed_at_ns: int | None = None
    inflight_count: int = 0
    completed_slot_count: int = 0
    extraction_status: str = "DISABLED"
    extraction_request_ref: str | None = None
    extraction_dispatch_ref: str | None = None
    extraction_result_ref: str | None = None
    extraction_candidate_ref: str | None = None
    extraction_validation_ref: str | None = None

    VERSION = "PublicContextCycleReportV1"

    def to_dict(self) -> dict[str, object]:
        return {"version": self.VERSION, "information_cutoff_ns": self.information_cutoff_ns,
                "started_at_ns": self.started_at_ns, "available_at_ns": self.available_at_ns,
                "policy_version": PUBLIC_CONTEXT_POLICY_VERSION_V1, "policy_hash": self.policy_hash,
                "source_id": self.source_id, "source_ref": self.source_ref,
                "status": self.status, "reasons": list(self.reasons), "raw_ref": self.raw_ref,
                "event_refs": list(self.event_refs), "alert_refs": list(self.alert_refs),
                "duplicate_count": self.duplicate_count, "fetch_started_at_ns": self.fetch_started_at_ns,
                "fetch_completed_at_ns": self.fetch_completed_at_ns, "inflight_count": self.inflight_count,
                "completed_slot_count": self.completed_slot_count,
                "extraction_status": self.extraction_status,
                "extraction_request_ref": self.extraction_request_ref,
                "extraction_dispatch_ref": self.extraction_dispatch_ref,
                "extraction_result_ref": self.extraction_result_ref,
                "extraction_candidate_ref": self.extraction_candidate_ref,
                "extraction_validation_ref": self.extraction_validation_ref,
                "capital_authority": "ZERO",
                "source_authentication": "UNKNOWN", "calendar_coverage": "UNKNOWN"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class _FetchedSlot:
    source: NewsSourceConfigV2
    started_at_ns: int
    completed_at_ns: int
    response: FetchedDocumentV2 | None
    error: str | None


@dataclass(frozen=True)
class _ExtractionSlot:
    prepared: EventExtractionPreparedV1
    outcome: EventExtractionProviderOutcomeV1


class _ReplayTransport:
    def __init__(self, response: FetchedDocumentV2) -> None:
        self.response = response

    def fetch(self, _source: NewsSourceConfigV2) -> FetchedDocumentV2:
        return self.response


class PublicContextMaintenanceV1:
    """One public fetch worker, one completion slot, fixed round-robin cadence."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns,
                 transport: object | None = None,
                 sources: Sequence[NewsSourceConfigV2] = DEFAULT_NEWS_SOURCES_V2,
                 asset_aliases: Mapping[str, str] | None = None,
                 event_extraction_provider: EventExtractionProvider | None = None) -> None:
        self.clock_ns = clock_ns
        self.transport = transport if transport is not None else PublicNewsTransportV2(
            clock_ns=clock_ns, timeout_s=PUBLIC_CONTEXT_TIMEOUT_S_V1,
        )
        self.sources = tuple(sources)
        if (not self.sources or len(self.sources) > PUBLIC_CONTEXT_SOURCE_LIMIT_V1
                or len({source.source_id for source in self.sources}) != len(self.sources)):
            raise ValueError("public context sources must be unique and bounded")
        self.asset_aliases = dict(asset_aliases or {})
        self.event_extraction_provider = event_extraction_provider
        policy = {"version": PUBLIC_CONTEXT_POLICY_VERSION_V1,
                  "sources": [source.to_dict() for source in self.sources],
                  "asset_aliases": self.asset_aliases,
                  "event_producer_version": PRODUCER_VERSION,
                  "cadence_ns": PUBLIC_CONTEXT_CADENCE_NS_V1,
                  "timeout_s": str(PUBLIC_CONTEXT_TIMEOUT_S_V1),
                  "maximum_inflight": 1, "maximum_completed_slots": 1,
                  "raw_body_max_bytes": RAW_BODY_MAX_BYTES,
                  "parsed_items_max": NEWS_COLLECTION_MAX_ITEMS_V1,
                  "routing": "FIXED_ROUND_ROBIN", "restart_cursor": "FIRST_CONFIGURED_SOURCE",
                  "capital_authority": "ZERO"}
        self.policy = policy
        self.policy_hash = sha256_json(policy)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._slot: _FetchedSlot | None = None
        self._closed = False
        self._position = 0
        self._next_fetch_at_ns = 0
        self._fetch_started_at_ns: int | None = None
        self._fetch_source: NewsSourceConfigV2 | None = None
        self._timeout_reported = False
        self._last_report: PublicContextCycleReportV1 | None = None
        self._extraction_thread: threading.Thread | None = None
        self._extraction_prepared: EventExtractionPreparedV1 | None = None
        self._extraction_slot: _ExtractionSlot | None = None
        self._extraction_started_monotonic: float | None = None
        self._extraction_deadline_monotonic: float | None = None
        self._extraction_active = False
        self._extraction_terminal = False
        self._pending_extraction_raw_ref: str | None = None
        self._repository: OpsRepository | None = None

    def _fetch(self, source: NewsSourceConfigV2, started_at_ns: int) -> None:
        response: FetchedDocumentV2 | None = None
        error: str | None = None
        try:
            fetch = getattr(self.transport, "fetch", None)
            response = fetch(source) if callable(fetch) else self.transport(source)  # type: ignore[operator]
            if not isinstance(response, FetchedDocumentV2):
                raise TypeError("public news transport returned an invalid response")
            if response.received_at_ns < started_at_ns:
                raise ValueError("news response receipt precedes fetch start")
        except Exception:
            # Do not persist provider strings, URLs with queries, or exception payloads.
            response, error = None, "PUBLIC_CONTEXT_FETCH_FAILED"
        completed = self.clock_ns()
        if completed < started_at_ns or (response is not None and completed < response.received_at_ns):
            response, error = None, "PUBLIC_CONTEXT_FETCH_CLOCK_REGRESSION"
        with self._lock:
            if not self._closed:
                self._slot = _FetchedSlot(source, started_at_ns, completed, response, error)

    def _publish(self, repository: OpsRepository, *, cutoff_ns: int, started_at_ns: int,
                 source: NewsSourceConfigV2 | None, status: str, reasons: tuple[str, ...],
                 slot: _FetchedSlot | None = None, raw_ref: str | None = None,
                 event_refs: tuple[str, ...] = (), alert_refs: tuple[str, ...] = (),
                 duplicate_count: int = 0, extraction_status: str = "DISABLED",
                 extraction_request_ref: str | None = None,
                 extraction_dispatch_ref: str | None = None,
                 extraction_result_ref: str | None = None,
                 extraction_candidate_ref: str | None = None,
                 extraction_validation_ref: str | None = None) -> PublicContextCycleReportV1:
        finished = self.clock_ns()
        if finished < started_at_ns:
            raise ValueError("public context publication clock regressed")
        source_ref = sha256_json(source.to_dict()) if source is not None else None
        with self._lock:
            inflight = int(self._thread is not None and self._thread.is_alive() and self._slot is None)
            completed = int(self._slot is not None)
        report = PublicContextCycleReportV1(
            cutoff_ns, started_at_ns, finished, self.policy_hash,
            source.source_id if source is not None else None, source_ref, status, reasons,
            raw_ref, event_refs, alert_refs, duplicate_count,
            slot.started_at_ns if slot is not None else self._fetch_started_at_ns,
            slot.completed_at_ns if slot is not None else None, inflight, completed,
            extraction_status, extraction_request_ref, extraction_dispatch_ref,
            extraction_result_ref, extraction_candidate_ref, extraction_validation_ref,
        )
        entries = [ArtifactIndexEntryV2(self.policy_hash, PUBLIC_CONTEXT_POLICY_VERSION_V1,
                                       self.policy_hash, started_at_ns, started_at_ns, self.policy)]
        # Reuse the first publication of immutable source/configuration identities.
        if repository.get_artifact(self.policy_hash) is not None:
            entries.clear()
        if source is not None and source_ref is not None and repository.get_artifact(source_ref) is None:
            entries.append(ArtifactIndexEntryV2(source_ref, "NewsSourceConfigV2", source_ref,
                                                started_at_ns, started_at_ns, source.to_dict()))
        entries.append(ArtifactIndexEntryV2(report.content_hash, report.VERSION, report.content_hash,
                                            started_at_ns, finished, report.to_dict()))
        repository.register_artifacts(tuple(entries))
        self._last_report = report
        return report

    def _run_extraction_provider(self, provider: EventExtractionProvider,
                                 prepared: EventExtractionPreparedV1) -> None:
        """Call provider only; repository access and validation stay on caller thread."""
        try:
            candidate = provider.extract(prepared.request, prepared.evidence)
            outcome = EventExtractionProviderOutcomeV1(candidate, self.clock_ns())
        except Exception:
            try:
                completed = self.clock_ns()
            except Exception:
                completed = prepared.started_at_ns
            outcome = EventExtractionProviderOutcomeV1(None, completed, True)
        with self._lock:
            self._extraction_active = False
            if not self._extraction_terminal and not self._closed:
                self._extraction_slot = _ExtractionSlot(prepared, outcome)

    def _extract_archived_response(
        self, repository: OpsRepository, *, raw_ref: str,
    ) -> tuple[str, str | None, str | None, str | None, str | None, str | None]:
        """Durably dispatch one zero-authority attempt, then return immediately."""
        provider = self.event_extraction_provider
        if provider is None:
            self._pending_extraction_raw_ref = raw_ref
            return "DISABLED", None, None, None, None, None
        with self._lock:
            if self._extraction_active or self._extraction_slot is not None:
                self._pending_extraction_raw_ref = raw_ref
                return "PENDING", None, None, None, None, None
        raw = repository.get_artifact(raw_ref)
        if raw is None or raw.artifact_type != "NewsRawPayloadV2":
            return "UNAVAILABLE", None, None, None, None, None
        source_url, source_id = raw.metadata.get("source_url"), raw.metadata.get("source_id")
        if not isinstance(source_url, str) or not isinstance(source_id, str):
            return "UNAVAILABLE", None, None, None, None, None
        receipt_body = {"schema_version": 1, "raw_ref": raw_ref, "source_id": source_id,
                        "source_url": source_url, "received_at_ns": raw.available_at_ns}
        receipt_ref = sha256_json(receipt_body)
        receipt = repository.get_artifact(receipt_ref)
        if receipt is None or receipt.artifact_type != "NewsReceiptV2":
            return "UNAVAILABLE", None, None, None, None, None
        request_id_seed = sha256_json({"version": "PublicContextEventExtractionRequestV1",
                                       "raw_ref": raw_ref, "receipt_ref": receipt_ref,
                                       "schema_hash": EVENT_EXTRACTION_SCHEMA_HASH_V1})
        request_id = str(uuid.UUID(hex=request_id_seed[:32], version=5))
        try:
            request = build_event_extraction_request_v1(
                repository, raw_ref=raw_ref, receipt_ref=receipt_ref, request_id=request_id,
                deadline_ns=raw.available_at_ns + PUBLIC_CONTEXT_EXTRACTION_DEADLINE_NS_V1,
                schema_hash=EVENT_EXTRACTION_SCHEMA_HASH_V1,
            )
        except Exception:
            return "UNAVAILABLE", None, None, None, None, None
        request_ref = request.content_hash
        dispatch_ref = sha256_json({"version": "EventExtractionDispatchV1",
                                    "request_hash": request_ref})
        if repository.get_artifact(dispatch_ref) is not None:
            try:
                terminal = find_event_extraction_terminal_v1(
                    repository, request, as_of_ns=self.clock_ns(),
                )
            except EventExtractionError:
                # Conflicting, malformed, or over-bound terminal evidence is
                # visible as indeterminate; never add a second terminal claim.
                return "INDETERMINATE", request_ref, dispatch_ref, None, None, None
            if terminal is not None:
                status = {"VALIDATED": "VALIDATED", "INVALID": "UNAVAILABLE",
                          "UNAVAILABLE": "UNAVAILABLE", "LATE_RETROSPECTIVE": "LATE",
                          "INDETERMINATE": "INDETERMINATE"}.get(terminal.status, "INDETERMINATE")
                return (status, terminal.request_ref, terminal.dispatch_ref, terminal.result_ref,
                        terminal.candidate_ref, terminal.validation_ref)
            try:
                run = record_event_extraction_indeterminate_v1(
                    repository, request, observed_at_ns=self.clock_ns(),
                )
            except EventExtractionError:
                return "INDETERMINATE", request_ref, dispatch_ref, None, None, None
            return ("INDETERMINATE", request_ref, dispatch_ref, None, None, run.validation_ref)
        try:
            prepared = prepare_event_extraction_v1(repository, request=request, clock_ns=self.clock_ns)
        except EventExtractionError as exc:
            status = "LATE" if exc.code == "EXTRACTION_DEADLINE_EXPIRED" else (
                "INDETERMINATE" if exc.code == "EXTRACTION_DISPATCH_ALREADY_STARTED_INDETERMINATE" else "UNAVAILABLE")
            if status == "INDETERMINATE":
                try:
                    run = record_event_extraction_indeterminate_v1(
                        repository, request, observed_at_ns=self.clock_ns(),
                    )
                    return status, request_ref, dispatch_ref, None, None, run.validation_ref
                except EventExtractionError:
                    pass
            return status, request_ref, dispatch_ref, None, None, None
        except Exception:
            return "UNAVAILABLE", None, None, None, None, None
        with self._lock:
            self._pending_extraction_raw_ref = None
            self._extraction_prepared = prepared
            self._extraction_started_monotonic = time.monotonic()
            remaining_ns = max(0, prepared.request.deadline_ns - timestamp(
                self.clock_ns(), field="public context extraction dispatch time"))
            remaining_s = min(PUBLIC_CONTEXT_EXTRACTION_TIMEOUT_S_V1, remaining_ns / 1_000_000_000)
            self._extraction_deadline_monotonic = self._extraction_started_monotonic + remaining_s
            self._extraction_active = True
            self._extraction_terminal = False
            self._extraction_thread = threading.Thread(
                target=self._run_extraction_provider, args=(provider, prepared),
                name="atlas-event-extraction", daemon=True,
            )
            self._extraction_thread.start()
        return "PENDING", prepared.request_ref, prepared.dispatch_ref, None, None, None

    def _publish_extraction_result(self, repository: OpsRepository, run: EventExtractionRunV1) -> None:
        status = {"VALIDATED": "VALIDATED", "LATE_RETROSPECTIVE": "LATE",
                  "INVALID": "UNAVAILABLE", "UNAVAILABLE": "UNAVAILABLE",
                  "INDETERMINATE": "INDETERMINATE"}.get(run.status, "UNAVAILABLE")
        current = self._last_report
        if current is None or current.extraction_request_ref != run.request_ref:
            return
        published_at_ns = timestamp(self.clock_ns(), field="public context extraction report publication")
        if published_at_ns < run.completed_at_ns:
            raise EventExtractionError("EXTRACTION_PUBLICATION_CLOCK_REGRESSION")
        updated = replace(current, available_at_ns=published_at_ns, extraction_status=status,
                          extraction_dispatch_ref=run.dispatch_ref, extraction_result_ref=run.result_ref,
                          extraction_candidate_ref=run.candidate_ref,
                          extraction_validation_ref=run.validation_ref)
        repository.register_artifacts((ArtifactIndexEntryV2(
            updated.content_hash, updated.VERSION, updated.content_hash, updated.started_at_ns,
            updated.available_at_ns, updated.to_dict()),))
        self._last_report = updated

    def _consume_extraction(self, repository: OpsRepository) -> bool:
        """Finalize provider outcomes on the caller thread; return on a state change."""
        with self._lock:
            slot = self._extraction_slot
            prepared = self._extraction_prepared
            timed_out = (self._extraction_active and not self._extraction_terminal
                         and self._extraction_deadline_monotonic is not None
                         and time.monotonic() >= self._extraction_deadline_monotonic)
            if timed_out and prepared is not None:
                self._extraction_terminal = True
        if timed_out and prepared is not None:
            outcome = EventExtractionProviderOutcomeV1(None, self.clock_ns(), True)
            run = complete_event_extraction_v1(repository, prepared, outcome=outcome, clock_ns=self.clock_ns)
            self._publish_extraction_result(repository, run)
            return True
        if slot is None:
            return False
        with self._lock:
            if self._extraction_slot is slot:
                self._extraction_slot = None
                self._extraction_terminal = True
        run = complete_event_extraction_v1(repository, slot.prepared, outcome=slot.outcome, clock_ns=self.clock_ns)
        self._publish_extraction_result(repository, run)
        return True

    def run_cycle(self, repository: OpsRepository, *, information_cutoff_ns: int) -> PublicContextCycleReportV1:
        source: NewsSourceConfigV2 | None
        cutoff = timestamp(information_cutoff_ns, field="public_context.information_cutoff_ns")
        started = self.clock_ns()
        if started < cutoff:
            raise ValueError("public context computation precedes its cutoff")
        self._repository = repository
        if self._consume_extraction(repository):
            if self._last_report is not None:
                return self._last_report
        if self._pending_extraction_raw_ref is not None and self.event_extraction_provider is not None:
            pending_raw_ref = self._pending_extraction_raw_ref
            pending = self._extract_archived_response(repository, raw_ref=pending_raw_ref)
            current = self._last_report
            if current is not None and current.raw_ref == pending_raw_ref:
                if started < current.available_at_ns:
                    raise EventExtractionError("EXTRACTION_REPORT_PUBLICATION_CLOCK_REGRESSION")
                updated = replace(current, available_at_ns=started,
                                  extraction_status=pending[0], extraction_request_ref=pending[1],
                                  extraction_dispatch_ref=pending[2], extraction_result_ref=pending[3],
                                  extraction_candidate_ref=pending[4], extraction_validation_ref=pending[5])
                repository.register_artifacts((ArtifactIndexEntryV2(
                    updated.content_hash, updated.VERSION, updated.content_hash, updated.started_at_ns,
                    started, updated.to_dict()),))
                self._last_report = updated
        with self._lock:
            closed, slot = self._closed, self._slot
        if closed:
            if self._last_report is not None and self._last_report.status == "CLOSED":
                return self._last_report
            return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                 source=None, status="CLOSED", reasons=("PUBLIC_CONTEXT_CLOSED",))
        if slot is not None and max(slot.completed_at_ns, slot.response.received_at_ns if slot.response else 0) <= cutoff:
            with self._lock:
                self._slot = None
            if slot.error is not None or slot.response is None:
                return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                     source=slot.source, status="FAILED", reasons=(slot.error or "PUBLIC_CONTEXT_FETCH_FAILED",), slot=slot)
            pipeline = NewsCollectionPipelineV2(
                repository, clock_ns=self.clock_ns, transport=_ReplayTransport(slot.response),
                sources=self.sources, asset_aliases=self.asset_aliases, publication_clock_ns=self.clock_ns,
            )
            raw_ref: str | None = None
            try:
                result = pipeline.collect(slot.source.source_id)
            except Exception as exc:
                reason = exc.reason if isinstance(exc, NewsCollectionBoundedError) else "PUBLIC_CONTEXT_COLLECTION_FAILED"
                try:
                    expected_raw_ref = sha256_json({"artifact_type": "NewsRawPayloadV2", "source_id": slot.source.source_id,
                                                   "source_url": canonical_url(slot.response.final_url),
                                                   "raw_payload_hash": hashlib.sha256(slot.response.body).hexdigest()})
                    if repository.get_artifact(expected_raw_ref) is not None:
                        raw_ref = expected_raw_ref
                except ValueError:
                    pass
                return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                     source=slot.source, status="NOT_ESTIMABLE", reasons=(reason,), slot=slot, raw_ref=raw_ref)
            extraction = self._extract_archived_response(
                repository, raw_ref=result.raw_ref,
            )
            return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                 source=slot.source, status="COLLECTED", reasons=("SOURCE_AUTHENTICATION_AND_CALENDAR_COVERAGE_UNKNOWN",),
                                 slot=slot, raw_ref=result.raw_ref, event_refs=result.event_refs,
                                 alert_refs=result.alert_refs, duplicate_count=result.duplicate_count,
                                 extraction_status=extraction[0], extraction_request_ref=extraction[1],
                                 extraction_dispatch_ref=extraction[2], extraction_result_ref=extraction[3],
                                 extraction_candidate_ref=extraction[4], extraction_validation_ref=extraction[5])
        with self._lock:
            inflight = self._thread is not None and self._thread.is_alive()
            if not inflight and self._slot is None and started >= self._next_fetch_at_ns:
                source = self.sources[self._position]
                self._position = (self._position + 1) % len(self.sources)
                self._fetch_started_at_ns, self._fetch_source = started, source
                self._next_fetch_at_ns = started + PUBLIC_CONTEXT_CADENCE_NS_V1
                self._timeout_reported = False
                self._thread = threading.Thread(target=self._fetch, args=(source, started),
                                                name="atlas-public-context-fetch", daemon=True)
                self._thread.start()
                launched = True
            else:
                source, launched = self._fetch_source, False
            timed_out = (inflight and self._fetch_started_at_ns is not None
                         and started - self._fetch_started_at_ns > int(PUBLIC_CONTEXT_TIMEOUT_S_V1 * 1_000_000_000)
                         and not self._timeout_reported)
            if timed_out:
                self._timeout_reported = True
        if launched:
            return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                 source=source, status="FETCH_STARTED", reasons=("PUBLIC_CONTEXT_FETCH_INFLIGHT",))
        if timed_out:
            return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                 source=source, status="PRESSURE", reasons=("PUBLIC_CONTEXT_FETCH_TIMEOUT_NO_SECOND_WORKER",))
        if self._last_report is not None:
            return self._last_report
        return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                             source=source, status="PENDING", reasons=("PUBLIC_CONTEXT_RESPONSE_NOT_CUTOFF_VISIBLE",))

    def close(self, *, timeout_s: float = PUBLIC_CONTEXT_CLOSE_MAX_S_V1,
              repository: OpsRepository | None = None) -> None:
        """Persist caller-owned terminal state, then bound only worker join time.

        SQLite persistence is synchronous and can add storage latency beyond
        ``timeout_s``. The parameter bounds waiting for worker termination.
        """
        if not 0 <= timeout_s <= PUBLIC_CONTEXT_CLOSE_MAX_S_V1:
            raise ValueError("public context close join must remain bounded")
        with self._lock:
            needs_finalization = not self._extraction_terminal
            self._closed = True
            self._slot = None
            worker = self._thread
            extraction_worker = self._extraction_thread
            extraction_slot = self._extraction_slot
            prepared = self._extraction_prepared
            extraction_active = self._extraction_active
            self._extraction_terminal = True
            self._extraction_slot = None
        active_repository = repository or self._repository
        if needs_finalization and active_repository is not None and extraction_slot is not None:
            run = complete_event_extraction_v1(
                active_repository, extraction_slot.prepared, outcome=extraction_slot.outcome,
                clock_ns=self.clock_ns,
            )
            self._publish_extraction_result(active_repository, run)
        elif needs_finalization and active_repository is not None and prepared is not None and extraction_active:
            run = record_event_extraction_indeterminate_v1(
                active_repository, prepared.request, observed_at_ns=self.clock_ns(),
            )
            self._publish_extraction_result(active_repository, run)
        deadline = time.monotonic() + timeout_s
        if worker is not None:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
        if extraction_worker is not None:
            extraction_worker.join(timeout=max(0.0, deadline - time.monotonic()))
