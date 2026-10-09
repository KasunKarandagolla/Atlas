"""Bounded, zero-authority extraction over an already archived S7 news item.

This module deliberately stops before provider transport.  It accepts only an
``EventExtractionProvider`` supplied by the caller, reads exact immutable news
artifacts from ``OpsRepository``, records the request and dispatch marker before
calling that provider, and writes a separate sidecar artifact.  It never edits
``NewsEventV2`` or any event gate, source-health, or authentication record.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json, timestamp
from atlas.v2.agent_intelligence.contracts import (
    AgentEvidenceRefV1,
    EventExtractionProvider,
    EventExtractionRequestV1,
    EventExtractionV1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    RAW_BODY_MAX_BYTES,
    ParsedNewsItemV2,
    canonical_url,
    parse_feed_bytes_v2,
)

EVENT_EXTRACTION_SCHEMA_V1: Mapping[str, Any] = {
    "version": "ATLAS_S7_EVENT_EXTRACTION_OUTPUT_V1",
    "items": {
        "required": ["item_index", "event_type", "severity", "event_time_text",
                     "asset_mentions", "supporting_spans", "unknown_fields"],
        "item_index": "integer-index-into-exact-parsed-source-items",
        "event_type": ["US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION", "SECURITY_INCIDENT",
                       "VENUE_INCIDENT", "OFFICIAL_ANNOUNCEMENT", "GENERAL_CONTEXT", None],
        "severity": ["CRITICAL", "HIGH", "MEDIUM", "LOW", None],
        "event_time_text": "exact-source-substring-or-null",
        "asset_mentions": "at-most-eight-exact-source-substrings",
        "supporting_spans": "one-to-eight-exact-source-substrings",
        "unknown_fields": ["EVENT_TYPE", "SEVERITY", "EVENT_TIME", "ASSET_MENTIONS",
                           "SOURCE_CLAIMS_CONFLICT"],
        "maximum_items": 100,
        "authority": "ZERO",
    },
}
EVENT_EXTRACTION_SCHEMA_HASH_V1 = sha256_json(EVENT_EXTRACTION_SCHEMA_V1)
MAX_EVENT_EXTRACTION_INPUT_BYTES = 160_000
MAX_EVENT_EXTRACTION_ITEM_CHARS = 4_000
MAX_EVENT_EXTRACTION_TITLE_CHARS = 512
MAX_EVENT_EXTRACTION_ITEMS = 100
_ALLOWED_EVENT_TYPES = frozenset({
    "US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION", "SECURITY_INCIDENT",
    "VENUE_INCIDENT", "OFFICIAL_ANNOUNCEMENT", "GENERAL_CONTEXT",
})
_ALLOWED_SEVERITIES = frozenset({"CRITICAL", "HIGH", "MEDIUM", "LOW"})
_ALLOWED_UNKNOWN_FIELDS = frozenset({
    "EVENT_TYPE", "SEVERITY", "EVENT_TIME", "ASSET_MENTIONS", "SOURCE_CLAIMS_CONFLICT",
})
_RAW_ARTIFACT_TYPE = "NewsRawPayloadV2"
_RECEIPT_ARTIFACT_TYPE = "NewsReceiptV2"


class EventExtractionError(ValueError):
    """A fail-closed S7 extraction error with a stable machine-readable code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class EventExtractionRunV1:
    request: EventExtractionRequestV1
    status: str
    request_ref: str
    dispatch_ref: str
    result_ref: str | None
    candidate_ref: str | None
    validation_ref: str
    completed_at_ns: int | None


@dataclass(frozen=True)
class EventExtractionPreparedV1:
    """Caller-thread verified evidence and durable dispatch for one attempt."""

    request: EventExtractionRequestV1
    evidence: tuple[Mapping[str, Any], ...]
    items: tuple[ParsedNewsItemV2, ...]
    request_ref: str
    dispatch_ref: str
    started_at_ns: int


@dataclass(frozen=True)
class EventExtractionProviderOutcomeV1:
    """In-memory provider outcome; workers may create this but never persist it."""

    candidate: Any | None
    completed_at_ns: int
    failed: bool = False


def _register(repository: OpsRepository, *, ref: str, artifact_type: str,
              created_at_ns: int, available_at_ns: int, metadata: Mapping[str, Any]) -> None:
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, artifact_type, sha256_json(metadata), created_at_ns, available_at_ns, metadata,
    ))


def _required_mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventExtractionError(code)
    return value


def _load_exact_archived_evidence(repository: OpsRepository, request: EventExtractionRequestV1,
                                  *, as_of_ns: int) -> tuple[Mapping[str, Any], tuple[ParsedNewsItemV2, ...]]:
    """Verify the request names exactly the source bytes and their receipt."""
    if len(request.evidence_manifest) != 2:
        raise EventExtractionError("EVIDENCE_MANIFEST_NOT_EXACT")
    raw_ref, receipt_ref = request.source_artifact_ref, request.evidence_manifest[1].artifact_ref
    manifest = request.evidence_manifest
    if (manifest[0].artifact_ref != raw_ref
            or any(item.tool_name != "get_registered_artifact" or item.cursor is not None for item in manifest)):
        raise EventExtractionError("EVIDENCE_MANIFEST_NOT_EXACT")
    raw_entry = repository.get_artifact(raw_ref)
    receipt_entry = repository.get_artifact(receipt_ref)
    if (raw_entry is None or receipt_entry is None
            or raw_entry.artifact_type != _RAW_ARTIFACT_TYPE
            or receipt_entry.artifact_type != _RECEIPT_ARTIFACT_TYPE
            or raw_entry.available_at_ns > as_of_ns or receipt_entry.available_at_ns > as_of_ns
            or manifest[0].available_through_ns != raw_entry.available_at_ns
            or manifest[1].available_through_ns != receipt_entry.available_at_ns):
        raise EventExtractionError("ARCHIVED_EVIDENCE_UNAVAILABLE_OR_FUTURE")

    raw_meta = _required_mapping(raw_entry.metadata, "RAW_ARTIFACT_INVALID")
    receipt = _required_mapping(receipt_entry.metadata, "NEWS_RECEIPT_INVALID")
    try:
        source_id = raw_meta["source_id"]
        source_class = raw_meta["source_class"]
        source_url = canonical_url(str(raw_meta["source_url"]))
        raw_hash = str(raw_meta["raw_payload_hash"])
        raw_b64 = str(raw_meta["raw_bytes_base64"])
        if len(raw_b64) > ((RAW_BODY_MAX_BYTES + 2) // 3) * 4:
            raise EventExtractionError("RAW_ARTIFACT_SIZE_LIMIT")
        raw_bytes = base64.b64decode(raw_b64, validate=True)
    except (KeyError, TypeError, ValueError) as exc:
        raise EventExtractionError("RAW_ARTIFACT_INVALID") from exc
    if (len(raw_bytes) > RAW_BODY_MAX_BYTES
            or not isinstance(source_id, str) or not source_id or not isinstance(source_class, str)
            or not source_class or hashlib.sha256(raw_bytes).hexdigest() != raw_hash
            or raw_entry.content_hash != raw_hash
            or raw_ref != sha256_json({"artifact_type": _RAW_ARTIFACT_TYPE, "source_id": source_id,
                                       "source_url": source_url, "raw_payload_hash": raw_hash})):
        raise EventExtractionError("RAW_ARTIFACT_HASH_OR_IDENTITY_MISMATCH")
    if (receipt.get("schema_version") != 1 or receipt.get("raw_ref") != raw_ref
            or receipt.get("source_id") != source_id or receipt.get("source_url") != source_url
            or type(receipt.get("received_at_ns")) is not int
            or receipt.get("received_at_ns") != raw_entry.available_at_ns
            or receipt_entry.artifact_ref != sha256_json(receipt)
            or receipt_entry.content_hash != sha256_json(receipt)
            or receipt_entry.available_at_ns != receipt["received_at_ns"]):
        raise EventExtractionError("NEWS_RECEIPT_BINDING_MISMATCH")
    try:
        items = tuple(parse_feed_bytes_v2(raw_bytes, base_url=source_url))
    except Exception as exc:
        raise EventExtractionError("ARCHIVED_SOURCE_PARSE_FAILED") from exc
    if len(items) > MAX_EVENT_EXTRACTION_ITEMS:
        raise EventExtractionError("ARCHIVED_SOURCE_ITEM_BOUND_EXCEEDED")
    for item in items:
        if (len(item.title) > MAX_EVENT_EXTRACTION_TITLE_CHARS
                or len(item.text) > MAX_EVENT_EXTRACTION_ITEM_CHARS):
            raise EventExtractionError("ARCHIVED_SOURCE_ITEM_BOUND_EXCEEDED")
    evidence = {
        "source_id": source_id,
        "source_class": source_class,
        "source_url": source_url,
        "raw_ref": raw_ref,
        "receipt_ref": receipt_ref,
        "received_at_ns": receipt["received_at_ns"],
        "items": [{"item_index": index, "title": item.title, "text": item.text,
                   "url": item.url, "claimed_published_at": item.published_claim}
                  for index, item in enumerate(items)],
    }
    if len(canonical_json(evidence).encode("utf-8")) > MAX_EVENT_EXTRACTION_INPUT_BYTES:
        raise EventExtractionError("ARCHIVED_SOURCE_INPUT_BOUND_EXCEEDED")
    return evidence, items


def build_event_extraction_request_v1(repository: OpsRepository, *, raw_ref: str,
                                      receipt_ref: str, request_id: str,
                                      deadline_ns: int, schema_hash: str = EVENT_EXTRACTION_SCHEMA_HASH_V1
                                      ) -> EventExtractionRequestV1:
    """Create the provider-neutral request bound to the exact raw receipt pair."""
    raw = repository.get_artifact(raw_ref)
    receipt = repository.get_artifact(receipt_ref)
    if raw is None or receipt is None:
        raise EventExtractionError("ARCHIVED_EVIDENCE_UNAVAILABLE")
    return EventExtractionRequestV1(
        request_id=request_id,
        source_artifact_ref=raw_ref,
        evidence_manifest=(
            AgentEvidenceRefV1("get_registered_artifact", raw_ref, raw.available_at_ns),
            AgentEvidenceRefV1("get_registered_artifact", receipt_ref, receipt.available_at_ns),
        ),
        deadline_ns=deadline_ns,
        schema_hash=schema_hash,
    )


def _validate_extraction(request: EventExtractionRequestV1, candidate: EventExtractionV1,
                         items: tuple[ParsedNewsItemV2, ...]) -> EventExtractionV1:
    if (not isinstance(candidate, EventExtractionV1)
            or candidate.request_id != request.request_id
            or candidate.source_artifact_ref != request.source_artifact_ref
            or len(candidate.extracted_events) != len(items)):
        raise EventExtractionError("EXTRACTION_REQUEST_OR_ITEM_BINDING_MISMATCH")
    normalized: list[Mapping[str, Any]] = []
    for index, (row, item) in enumerate(zip(candidate.extracted_events, items, strict=True)):
        body = row.to_dict()
        required = {"item_index", "event_type", "severity", "event_time_text", "asset_mentions",
                    "supporting_spans", "unknown_fields"}
        if set(body) != required or body["item_index"] != index or type(body["item_index"]) is not int:
            raise EventExtractionError("EXTRACTION_SCHEMA_INVALID")
        event_type, severity = body["event_type"], body["severity"]
        if event_type is not None and event_type not in _ALLOWED_EVENT_TYPES:
            raise EventExtractionError("EXTRACTION_SCHEMA_INVALID")
        if severity is not None and severity not in _ALLOWED_SEVERITIES:
            raise EventExtractionError("EXTRACTION_SCHEMA_INVALID")
        unknown = body["unknown_fields"]
        mentions, spans = body["asset_mentions"], body["supporting_spans"]
        if (not isinstance(unknown, (tuple, list))
                or any(not isinstance(value, str) or value not in _ALLOWED_UNKNOWN_FIELDS for value in unknown)
                or len(unknown) != len(set(unknown))
                or not isinstance(mentions, (tuple, list)) or len(mentions) > 8
                or any(not isinstance(value, str) or not value for value in mentions)
                or not isinstance(spans, (tuple, list)) or not 1 <= len(spans) <= 8
                or any(not isinstance(value, str) or not value for value in spans)):
            raise EventExtractionError("EXTRACTION_SCHEMA_INVALID")
        if not isinstance(item.title, str) or not isinstance(item.text, str):
            raise EventExtractionError("ARCHIVED_SOURCE_PARSE_FAILED")
        source_text = f"{item.title}\n{item.text}"
        if any(span not in source_text for span in spans):
            raise EventExtractionError("EXTRACTION_SPAN_NOT_IN_SOURCE")
        if body["event_time_text"] is not None and (
                not isinstance(body["event_time_text"], str)
                or body["event_time_text"] not in source_text
                or "EVENT_TIME" in unknown):
            raise EventExtractionError("EXTRACTION_TIME_NOT_IN_SOURCE_OR_AMBIGUOUS")
        if body["event_time_text"] is None and "EVENT_TIME" not in unknown:
            raise EventExtractionError("EXTRACTION_UNKNOWN_FIELD_NOT_DECLARED")
        if event_type is None and "EVENT_TYPE" not in unknown:
            raise EventExtractionError("EXTRACTION_UNKNOWN_FIELD_NOT_DECLARED")
        if severity is None and "SEVERITY" not in unknown:
            raise EventExtractionError("EXTRACTION_UNKNOWN_FIELD_NOT_DECLARED")
        if any(mention not in source_text for mention in mentions):
            raise EventExtractionError("EXTRACTION_ASSET_MENTION_NOT_IN_SOURCE")
        if "ASSET_MENTIONS" in unknown and mentions:
            raise EventExtractionError("EXTRACTION_AMBIGUITY_CONTRADICTION")
        if "EVENT_TYPE" in unknown and event_type is not None:
            raise EventExtractionError("EXTRACTION_AMBIGUITY_CONTRADICTION")
        if "SEVERITY" in unknown and severity is not None:
            raise EventExtractionError("EXTRACTION_AMBIGUITY_CONTRADICTION")
        if ("SOURCE_CLAIMS_CONFLICT" in unknown
                and (event_type is not None or severity is not None or body["event_time_text"] is not None
                     or mentions)):
            raise EventExtractionError("EXTRACTION_AMBIGUITY_CONTRADICTION")
        normalized.append(body)
    # Reconstruct through the frozen public contract so nested values are immutable.
    return EventExtractionV1(request.request_id, request.source_artifact_ref, tuple(
        FrozenMap(item)
        for item in normalized
    ))


def prepare_event_extraction_v1(repository: OpsRepository, *, request: EventExtractionRequestV1,
                                clock_ns: Callable[[], int]) -> EventExtractionPreparedV1:
    """Verify and durably dispatch an attempt before any provider code runs."""
    if request.schema_hash != EVENT_EXTRACTION_SCHEMA_HASH_V1:
        raise EventExtractionError("EXTRACTION_SCHEMA_HASH_MISMATCH")
    started_at_ns = timestamp(clock_ns(), field="event extraction start")
    if started_at_ns >= request.deadline_ns:
        raise EventExtractionError("EXTRACTION_DEADLINE_EXPIRED")
    evidence, items = _load_exact_archived_evidence(repository, request, as_of_ns=started_at_ns)
    packed_evidence = (
        {"tool_name": "get_registered_artifact", "artifact_ref": request.source_artifact_ref,
         "available_through_ns": request.evidence_manifest[0].available_through_ns,
         "status": "PRESENT", "rows": [evidence]},
        {"tool_name": "get_registered_artifact", "artifact_ref": request.evidence_manifest[1].artifact_ref,
         "available_through_ns": request.evidence_manifest[1].available_through_ns,
         "status": "PRESENT", "rows": [{"receipt_ref": request.evidence_manifest[1].artifact_ref,
                                            "raw_ref": request.source_artifact_ref,
                                            "received_at_ns": request.evidence_manifest[1].available_through_ns}]},
    )
    if len(canonical_json(packed_evidence).encode("utf-8")) > MAX_EVENT_EXTRACTION_INPUT_BYTES:
        raise EventExtractionError("ARCHIVED_SOURCE_INPUT_BOUND_EXCEEDED")
    request_ref = request.content_hash
    dispatch_ref = sha256_json({"version": "EventExtractionDispatchV1", "request_hash": request_ref})
    if repository.get_artifact(dispatch_ref) is not None:
        raise EventExtractionError("EXTRACTION_DISPATCH_ALREADY_STARTED_INDETERMINATE")
    request_metadata = {"request": request.to_dict(), "authority": "ZERO"}
    existing_request = repository.get_artifact(request_ref)
    if existing_request is not None:
        if (existing_request.artifact_type != "EventExtractionRequestV1"
                or existing_request.metadata.get("request") != request.to_dict()):
            raise EventExtractionError("EXTRACTION_REQUEST_IDENTITY_CONFLICT")
    else:
        _register(repository, ref=request_ref, artifact_type="EventExtractionRequestV1",
                  created_at_ns=started_at_ns, available_at_ns=started_at_ns, metadata=request_metadata)
    dispatch_metadata = {"version": "EventExtractionDispatchV1", "request_ref": request_ref,
                         "request_hash": request_ref, "started_at_ns": started_at_ns,
                         "provider_effect_may_follow": True, "authority": "ZERO"}
    _register(repository, ref=dispatch_ref, artifact_type="EventExtractionDispatchV1",
              created_at_ns=started_at_ns, available_at_ns=started_at_ns, metadata=dispatch_metadata)
    return EventExtractionPreparedV1(request, packed_evidence, items, request_ref, dispatch_ref,
                                     started_at_ns)


def complete_event_extraction_v1(repository: OpsRepository, prepared: EventExtractionPreparedV1, *,
                                 outcome: EventExtractionProviderOutcomeV1,
                                 clock_ns: Callable[[], int]) -> EventExtractionRunV1:
    """Validate and persist a provider outcome on the caller thread."""
    request = prepared.request
    request_ref, dispatch_ref, started_at_ns = prepared.request_ref, prepared.dispatch_ref, prepared.started_at_ns
    completed_at_ns = timestamp(outcome.completed_at_ns, field="event extraction provider completion")
    status = "UNAVAILABLE" if outcome.failed else "VALIDATED"
    reason: str | None = "PROVIDER_UNAVAILABLE_OR_INVALID_RESULT" if outcome.failed else None
    candidate = outcome.candidate
    candidate_dict: Mapping[str, Any] | None = None
    extraction: EventExtractionV1 | None = None
    if isinstance(candidate, EventExtractionV1):
        candidate_dict = candidate.to_dict()
    if completed_at_ns < started_at_ns:
        completed_at_ns = started_at_ns
        status, reason = "INVALID", "EXTRACTION_CLOCK_REGRESSION"
    elif completed_at_ns >= request.deadline_ns:
        status, reason = "LATE_RETROSPECTIVE", "EXTRACTION_DEADLINE_EXPIRED"
    elif not outcome.failed and not isinstance(candidate, EventExtractionV1):
        status, reason = "UNAVAILABLE", "PROVIDER_UNAVAILABLE_OR_INVALID_RESULT"
    elif not outcome.failed:
        try:
            assert isinstance(candidate, EventExtractionV1)
            extraction = _validate_extraction(request, candidate, prepared.items)
        except EventExtractionError as exc:
            status, reason = "INVALID", exc.code
        validated_at_ns = timestamp(clock_ns(), field="event extraction validation completion")
        if validated_at_ns < completed_at_ns:
            status, reason, extraction = "INVALID", "EXTRACTION_CLOCK_REGRESSION", None
        elif validated_at_ns >= request.deadline_ns:
            status, reason, extraction = "LATE_RETROSPECTIVE", "EXTRACTION_DEADLINE_EXPIRED", None
        else:
            completed_at_ns = validated_at_ns

    result_ref: str | None = None
    candidate_ref: str | None = None
    if extraction is not None:
        result_ref = sha256_json({"version": "EventExtractionArtifactV1",
                                  "request_ref": request_ref, "result_hash": extraction.content_hash})
        result_metadata = {"extraction": extraction.to_dict(), "request_ref": request_ref,
                           "source_artifact_ref": request.source_artifact_ref,
                           "completion_time_source": "provider_return_observed_by_ATLAS",
                           "completed_at_ns": completed_at_ns, "authority": "ZERO"}
        _register(repository, ref=result_ref, artifact_type="EventExtractionArtifactV1",
                  created_at_ns=completed_at_ns, available_at_ns=completed_at_ns, metadata=result_metadata)
    elif candidate_dict is not None:
        candidate_ref = sha256_json({"version": "EventExtractionCandidateV1", "request_ref": request_ref,
                                     "candidate_hash": sha256_json(candidate_dict), "status": status})
        candidate_metadata = {"version": "EventExtractionCandidateV1", "candidate": candidate_dict,
                              "request_ref": request_ref, "source_artifact_ref": request.source_artifact_ref,
                              "status": status, "reason": reason, "completed_at_ns": completed_at_ns,
                              "authority": "ZERO", "eligible": False}
        _register(repository, ref=candidate_ref, artifact_type="EventExtractionCandidateV1",
                  created_at_ns=completed_at_ns, available_at_ns=completed_at_ns,
                  metadata=candidate_metadata)
    receipt_body = {"version": "EventExtractionValidationReceiptV1", "request_ref": request_ref,
                    "dispatch_ref": dispatch_ref, "result_ref": result_ref, "status": status,
                    "reason": reason, "candidate_hash": sha256_json(candidate_dict) if candidate_dict else None,
                    "completed_at_ns": completed_at_ns, "authority": "ZERO"}
    validation_ref = sha256_json(receipt_body)
    _register(repository, ref=validation_ref, artifact_type="EventExtractionValidationReceiptV1",
              created_at_ns=completed_at_ns, available_at_ns=completed_at_ns,
              metadata={"receipt": receipt_body})
    return EventExtractionRunV1(request, status, request_ref, dispatch_ref, result_ref, candidate_ref,
                                validation_ref, completed_at_ns)


def record_event_extraction_indeterminate_v1(
    repository: OpsRepository, request: EventExtractionRequestV1, *, observed_at_ns: int,
) -> EventExtractionRunV1:
    """Persist a stable terminal receipt when dispatch exists without a finalized result.

    The reference is derived from the request and dispatch identities, so repeated
    shutdown/restart handling reuses the same receipt. No provider output is
    asserted or retained.
    """
    observed_at_ns = timestamp(observed_at_ns, field="event extraction indeterminate observation")
    request_ref = request.content_hash
    dispatch_ref = sha256_json({"version": "EventExtractionDispatchV1", "request_hash": request_ref})
    dispatch = repository.get_artifact(dispatch_ref)
    if (dispatch is None or dispatch.artifact_type != "EventExtractionDispatchV1"
            or dispatch.metadata.get("request_ref") != request_ref):
        raise EventExtractionError("EXTRACTION_DISPATCH_NOT_FOUND")
    if observed_at_ns < dispatch.available_at_ns:
        raise EventExtractionError("EXTRACTION_INDETERMINATE_CLOCK_REGRESSION")
    receipt = {
        "version": "EventExtractionValidationReceiptV1",
        "request_ref": request_ref,
        "dispatch_ref": dispatch_ref,
        "result_ref": None,
        "status": "INDETERMINATE",
        "reason": "CALL_OUTCOME_UNKNOWN",
        "candidate_hash": None,
        "completed_at_ns": observed_at_ns,
        "authority": "ZERO",
    }
    validation_ref = sha256_json({
        "version": "EventExtractionIndeterminateReceiptV1",
        "request_ref": request_ref,
        "dispatch_ref": dispatch_ref,
        "status": receipt["status"],
        "reason": receipt["reason"],
    })
    existing = repository.get_artifact(validation_ref)
    if existing is None:
        _register(repository, ref=validation_ref, artifact_type="EventExtractionValidationReceiptV1",
                  created_at_ns=observed_at_ns, available_at_ns=observed_at_ns,
                  metadata={"receipt": receipt})
    else:
        existing_receipt = existing.metadata.get("receipt")
        if (existing.artifact_type != "EventExtractionValidationReceiptV1"
                or not isinstance(existing_receipt, Mapping)
                or existing_receipt.get("request_ref") != request_ref
                or existing_receipt.get("dispatch_ref") != dispatch_ref
                or existing_receipt.get("status") != "INDETERMINATE"
                or existing_receipt.get("reason") != "CALL_OUTCOME_UNKNOWN"):
            raise EventExtractionError("EXTRACTION_VALIDATION_IDENTITY_CONFLICT")
        prior_completed = existing_receipt.get("completed_at_ns")
        if type(prior_completed) is int:
            observed_at_ns = prior_completed
    return EventExtractionRunV1(request, "INDETERMINATE", request_ref, dispatch_ref,
                                None, None, validation_ref, observed_at_ns)


def find_event_extraction_terminal_v1(repository: OpsRepository, request: EventExtractionRequestV1, *,
                                      as_of_ns: int) -> EventExtractionRunV1 | None:
    """Find terminal outcome by exact indexed request identity; fail on conflicts."""
    cutoff = timestamp(as_of_ns, field="event extraction terminal lookup")
    dispatch_ref = sha256_json({"version": "EventExtractionDispatchV1",
                                "request_hash": request.content_hash})
    try:
        page = repository.artifact_entries_by_metadata_identity(
            "EventExtractionValidationReceiptV1", ("receipt", "request_ref"), request.content_hash,
            as_of_ns=cutoff, limit=8,
        )
    except (ValueError, RuntimeError) as exc:
        raise EventExtractionError("EXTRACTION_TERMINAL_RECEIPT_LOOKUP_FAILED") from exc
    if page.has_more or page.invalid_entry_count:
        raise EventExtractionError("EXTRACTION_TERMINAL_RECEIPT_LOOKUP_OVERFLOW_OR_INVALID")
    matches: list[tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = []
    for entry in page.entries:
        body = entry.metadata.get("receipt")
        if (not isinstance(body, Mapping) or body.get("request_ref") != request.content_hash
                or body.get("dispatch_ref") != dispatch_ref
                or body.get("status") not in {
                    "VALIDATED", "INVALID", "UNAVAILABLE", "LATE_RETROSPECTIVE", "INDETERMINATE",
                }):
            continue
        matches.append((entry, body))
    if not matches:
        return None
    semantic_outcomes = {
        (body.get("status"), body.get("reason"), body.get("result_ref"), body.get("candidate_hash"))
        for _, body in matches
    }
    if len(semantic_outcomes) != 1:
        raise EventExtractionError("EXTRACTION_TERMINAL_RECEIPTS_CONFLICT")
    entry, body = max(matches, key=lambda row: (row[0].available_at_ns, row[0].artifact_ref))
    status = str(body["status"])
    candidate_hash = body.get("candidate_hash")
    candidate_ref = None
    if isinstance(candidate_hash, str) and status in {"INVALID", "UNAVAILABLE", "LATE_RETROSPECTIVE"}:
        candidate_ref = sha256_json({"version": "EventExtractionCandidateV1", "request_ref": request.content_hash,
                                     "candidate_hash": candidate_hash, "status": status})
    completed = body.get("completed_at_ns")
    completed_at_ns = timestamp(completed, field="event extraction terminal completion") if type(completed) is int else entry.available_at_ns
    return EventExtractionRunV1(
        request, status, request.content_hash, dispatch_ref, body.get("result_ref"), candidate_ref,
        entry.artifact_ref, completed_at_ns,
    )


def run_event_extraction_v1(repository: OpsRepository, provider: EventExtractionProvider, *,
                            request: EventExtractionRequestV1,
                            clock_ns: Callable[[], int]) -> EventExtractionRunV1:
    """Synchronous compatibility helper; public cycles use the split async API."""
    prepared = prepare_event_extraction_v1(repository, request=request, clock_ns=clock_ns)
    try:
        candidate = provider.extract(request, prepared.evidence)
        outcome = EventExtractionProviderOutcomeV1(candidate, timestamp(clock_ns(), field="event extraction completion"))
    except Exception:
        outcome = EventExtractionProviderOutcomeV1(None, timestamp(clock_ns(), field="event extraction completion"), True)
    return complete_event_extraction_v1(repository, prepared, outcome=outcome, clock_ns=clock_ns)
