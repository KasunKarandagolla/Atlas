"""Bounded public-context acquisition with persistence only on the caller thread.

The fetch worker owns no repository. A completed response occupies one slot
until its actual receipt/completion is cutoff-visible. Parsing and archiving
use the existing deterministic collector on the installed writer thread.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from atlas.v2._serialization import sha256_json, timestamp
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
                "completed_slot_count": self.completed_slot_count, "capital_authority": "ZERO",
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
                 asset_aliases: Mapping[str, str] | None = None) -> None:
        self.clock_ns = clock_ns
        self.transport = transport if transport is not None else PublicNewsTransportV2(
            clock_ns=clock_ns, timeout_s=PUBLIC_CONTEXT_TIMEOUT_S_V1,
        )
        self.sources = tuple(sources)
        if (not self.sources or len(self.sources) > PUBLIC_CONTEXT_SOURCE_LIMIT_V1
                or len({source.source_id for source in self.sources}) != len(self.sources)):
            raise ValueError("public context sources must be unique and bounded")
        self.asset_aliases = dict(asset_aliases or {})
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
                 duplicate_count: int = 0) -> PublicContextCycleReportV1:
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

    def run_cycle(self, repository: OpsRepository, *, information_cutoff_ns: int) -> PublicContextCycleReportV1:
        source: NewsSourceConfigV2 | None
        cutoff = timestamp(information_cutoff_ns, field="public_context.information_cutoff_ns")
        started = self.clock_ns()
        if started < cutoff:
            raise ValueError("public context computation precedes its cutoff")
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
            return self._publish(repository, cutoff_ns=cutoff, started_at_ns=started,
                                 source=slot.source, status="COLLECTED", reasons=("SOURCE_AUTHENTICATION_AND_CALENDAR_COVERAGE_UNKNOWN",),
                                 slot=slot, raw_ref=result.raw_ref, event_refs=result.event_refs,
                                 alert_refs=result.alert_refs, duplicate_count=result.duplicate_count)
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

    def close(self, *, timeout_s: float = PUBLIC_CONTEXT_TIMEOUT_S_V1 + 0.1) -> None:
        if not 0 <= timeout_s <= PUBLIC_CONTEXT_TIMEOUT_S_V1 + 0.1:
            raise ValueError("public context close join must remain bounded")
        with self._lock:
            self._closed = True
            self._slot = None
            worker = self._thread
        if worker is not None:
            worker.join(timeout=timeout_s)
