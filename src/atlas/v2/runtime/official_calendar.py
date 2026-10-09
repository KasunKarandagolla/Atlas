"""Bounded, public official macro calendar acquisition.

This adapter preserves source bytes before parsing and only emits scheduled
events when the official source documents an exact, timezone-qualified time.
It has no credential, order, or trading surface.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from atlas.v2._serialization import sha256_json, timestamp
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    RAW_BODY_MAX_BYTES,
    CalendarCoverageV2,
    FetchedDocumentV2,
    NewsSourceClassV2,
    NewsSourceConfigV2,
    PublicNewsTransportV2,
    ScheduledEventV2,
    SourceQualificationV2,
)

CALENDAR_CADENCE_NS = 60_000_000_000
CALENDAR_TIMEOUT_S = 2.0
CALENDAR_SOURCE_ID = "US_OFFICIAL_MACRO_CALENDAR"
CALENDAR_POLICY_VERSION = "OfficialCalendarMaintenanceV1"
CALENDAR_EVENT_TYPES = ("US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION")
FOMC_PARSERS = ("OFFICIAL_ICS_FOMC_V1", "OFFICIAL_FOMC_JSON_V1")


@dataclass(frozen=True)
class OfficialFomcSourceProfileV1:
    """Explicit official source configuration; proof refs never grant authority alone."""

    feed_url: str
    parser: str = "OFFICIAL_ICS_FOMC_V1"
    qualification_evidence_ref: str | None = None
    timezone: str | None = None
    timezone_evidence_ref: str | None = None
    bls_qualification_evidence_ref: str | None = None

    def __post_init__(self) -> None:
        parsed = urlsplit(self.feed_url)
        if (parsed.scheme != "https" or parsed.hostname != "www.federalreserve.gov"
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.port not in (None, 443)):
            raise ValueError("FOMC profile requires an official Fed HTTPS URL without secrets or query")
        if self.parser not in FOMC_PARSERS:
            raise ValueError("unsupported exact FOMC source parser")
        for ref in (self.qualification_evidence_ref, self.timezone_evidence_ref, self.bls_qualification_evidence_ref):
            if ref is not None and (not isinstance(ref, str) or re.fullmatch(r"[0-9a-f]{64}", ref) is None):
                raise ValueError("calendar proof reference must be a SHA-256 artifact reference")
        if (self.timezone is None) != (self.timezone_evidence_ref is None):
            raise ValueError("configured timezone requires its separate official source evidence")
        if self.timezone is not None:
            try:
                ZoneInfo(self.timezone)
            except (ValueError, ZoneInfoNotFoundError) as exc:
                raise ValueError("configured timezone must be an IANA zone") from exc

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> OfficialFomcSourceProfileV1:
        allowed = {"schema_version", "feed_url", "parser", "qualification_evidence_ref",
                   "timezone", "timezone_evidence_ref", "bls_qualification_evidence_ref"}
        if set(value) - allowed or type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
            raise ValueError("invalid official FOMC source profile schema")
        if not isinstance(value.get("feed_url"), str) or not isinstance(value.get("parser", "OFFICIAL_ICS_FOMC_V1"), str):
            raise ValueError("official calendar URL and parser must be strings")
        if value.get("timezone") is not None and not isinstance(value["timezone"], str):
            raise ValueError("official calendar timezone must be a string")
        return cls(**{key: item for key, item in value.items() if key != "schema_version"})  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "feed_url": self.feed_url, "parser": self.parser,
                "qualification_evidence_ref": self.qualification_evidence_ref,
                "timezone": self.timezone, "timezone_evidence_ref": self.timezone_evidence_ref,
                "bls_qualification_evidence_ref": self.bls_qualification_evidence_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def source(self) -> NewsSourceConfigV2:
        return NewsSourceConfigV2("FED_FOMC_EXPLICIT_PROFILE", NewsSourceClassV2.OFFICIAL_MACRO,
            self.feed_url, ("www.federalreserve.gov",), parser=self.parser,
            qualification=SourceQualificationV2.UNVERIFIED)

DEFAULT_CALENDAR_SOURCES = (
    NewsSourceConfigV2("BLS_OFFICIAL_ICS", NewsSourceClassV2.OFFICIAL_MACRO,
        "https://www.bls.gov/schedule/news_release/bls.ics", ("www.bls.gov",),
        parser="OFFICIAL_ICS_V1", qualification=SourceQualificationV2.UNVERIFIED),
    NewsSourceConfigV2("FED_FOMC_OFFICIAL_HTML", NewsSourceClassV2.OFFICIAL_MACRO,
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm", ("www.federalreserve.gov",),
        parser="OFFICIAL_FOMC_HTML_DATE_ONLY_V1", qualification=SourceQualificationV2.UNVERIFIED),
)


def official_calendar_sources_v1(
    fomc_source_profile: OfficialFomcSourceProfileV1 | None = None,
) -> tuple[NewsSourceConfigV2, ...]:
    return DEFAULT_CALENDAR_SOURCES if fomc_source_profile is None else (
        DEFAULT_CALENDAR_SOURCES[0], fomc_source_profile.source())


@dataclass(frozen=True)
class CalendarParseResultV1:
    events: tuple[tuple[str, int, str, int | None], ...]
    missing_times: tuple[Mapping[str, object], ...]
    complete: bool
    reasons: tuple[str, ...]


def _unfold_ical(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if line.startswith((" ", "\t")) and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return lines


def _ics_datetime(property_line: str) -> int | None:
    left, sep, value = property_line.partition(":")
    if not sep:
        return None
    params = {key.upper(): val for key, _, val in
              (piece.partition("=") for piece in left.split(";")[1:]) if key and val}
    value = value.strip()
    try:
        if re.fullmatch(r"\d{8}T\d{6}Z", value):
            dt = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        elif re.fullmatch(r"\d{8}T\d{6}", value):
            tzid = params.get("TZID", "").strip('"')
            if not tzid:
                return None
            dt = datetime.strptime(value, "%Y%m%dT%H%M%S")
            zone = ZoneInfo(tzid)
            first, second = dt.replace(tzinfo=zone, fold=0), dt.replace(tzinfo=zone, fold=1)
            # Ambiguous wall times have two valid UTC instants; source data
            # must disambiguate them instead of inheriting a library default.
            if first.utcoffset() != second.utcoffset():
                return None
            dt = first
            # Round-trip rejects nonexistent local times across the DST gap.
            if dt.astimezone(UTC).astimezone(dt.tzinfo).replace(tzinfo=None) != dt.replace(tzinfo=None):
                return None
        else:
            # All-day dates and floating times do not specify a release instant.
            return None
        return int(dt.astimezone(UTC).timestamp() * 1_000_000_000)
    except (ValueError, OverflowError, OSError, ZoneInfoNotFoundError):
        return None


def _header_publication_ns(headers: Mapping[str, str]) -> int | None:
    raw = next((value for key, value in headers.items() if key.lower() == "last-modified"), None)
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            return None
        return int(parsed.astimezone(UTC).timestamp() * 1_000_000_000)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def parse_bls_ics_v1(body: bytes) -> CalendarParseResultV1:
    """Extract CPI and Employment Situation instants from official BLS ICS."""
    try:
        lines = _unfold_ical(body.decode("utf-8-sig"))
    except UnicodeDecodeError:
        return CalendarParseResultV1((), (), False, ("INVALID_ICS_ENCODING",))
    rows: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in lines:
        upper = line.upper()
        if upper == "BEGIN:VEVENT":
            current = {}
        elif upper == "END:VEVENT":
            if current is not None:
                rows.append(current)
            current = None
        elif current is not None:
            name = line.partition(":")[0].split(";", 1)[0].upper()
            if name in ("UID", "SUMMARY", "DTSTART", "DTSTAMP", "LAST-MODIFIED"):
                current[name] = line
    events: list[tuple[str, int, str, int | None]] = []
    missing: list[Mapping[str, object]] = []
    reasons: set[str] = set()
    seen_uid: set[str] = set()
    for row in rows:
        summary_line = row.get("SUMMARY", "")
        summary = summary_line.partition(":")[2].replace("\\,", ",").replace("\\;", ";").strip()
        text = summary.casefold()
        if "consumer price index" in text or re.search(r"\bcpi\b", text):
            classification = "US_CPI"
        elif "employment situation" in text or "employment situation summary" in text:
            classification = "US_PAYROLL"
        else:
            continue
        uid = row.get("UID", "").partition(":")[2].strip()
        if not uid or uid in seen_uid:
            reasons.add("MISSING_OR_DUPLICATE_ICS_UID")
            continue
        seen_uid.add(uid)
        start = row.get("DTSTART")
        scheduled = _ics_datetime(start) if start else None
        if scheduled is None:
            missing.append({"classification": classification, "uid": uid,
                            "missing_field": "EXACT_TIMEZONE_QUALIFIED_DTSTART", "summary": summary})
            reasons.add("OFFICIAL_SCHEDULE_TIME_NOT_ESTIMABLE")
            continue
        published_line = row.get("DTSTAMP") or row.get("LAST-MODIFIED")
        published = _ics_datetime(published_line) if published_line else None
        events.append((classification, scheduled, uid, published))
    if not rows:
        reasons.add("NO_ICS_EVENTS")
    classes = {item[0] for item in events}
    if not {"US_CPI", "US_PAYROLL"}.issubset(classes):
        reasons.add("REQUIRED_RELEASE_CLASS_MISSING")
    return CalendarParseResultV1(tuple(sorted(events, key=lambda item: (item[1], item[0], item[2]))),
                                 tuple(missing), not reasons, tuple(sorted(reasons)))


def parse_fomc_html_dates_v1(body: bytes) -> CalendarParseResultV1:
    """Recognize FOMC dates, but deliberately withhold events lacking times."""
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return CalendarParseResultV1((), (), False, ("INVALID_HTML_ENCODING",))
    # The official calendar is a list of meeting date ranges. Date-only entries
    # are evidence that a meeting is scheduled, not evidence of a release time.
    dates = sorted(set(re.findall(r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:\s*[–-]\s*\d{1,2})?,?\s+20\d{2}\b", re.sub(r"<[^>]*>", " ", text), re.I)))
    if not dates:
        return CalendarParseResultV1((), (), False, ("NO_OFFICIAL_FOMC_DATES_RECOGNIZED",))
    missing = tuple({"classification": "FOMC_RATE_DECISION", "date_text": value,
                     "missing_field": "OFFICIAL_RELEASE_TIME"} for value in dates)
    return CalendarParseResultV1((), missing, False, ("OFFICIAL_SCHEDULE_TIME_NOT_ESTIMABLE",))


@dataclass(frozen=True)
class _Slot:
    source: NewsSourceConfigV2
    started_ns: int
    completed_ns: int
    response: FetchedDocumentV2 | None
    error: str | None


class OfficialCalendarMaintenanceV1:
    """One fetch worker and completion slot for official public calendars."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns,
                 transport: object | None = None,
                 sources: Sequence[NewsSourceConfigV2] = DEFAULT_CALENDAR_SOURCES,
                 fomc_source_profile: OfficialFomcSourceProfileV1 | None = None) -> None:
        self.clock_ns = clock_ns
        self.transport = transport if transport is not None else PublicNewsTransportV2(
            clock_ns=clock_ns, timeout_s=CALENDAR_TIMEOUT_S)
        if fomc_source_profile is not None and tuple(sources) != DEFAULT_CALENDAR_SOURCES:
            raise ValueError("explicit FOMC profile cannot be combined with custom calendar sources")
        self.fomc_source_profile = fomc_source_profile
        self.sources = official_calendar_sources_v1(fomc_source_profile) if fomc_source_profile else tuple(sources)
        if (not self.sources or len(self.sources) > 8
                or len({item.source_id for item in self.sources}) != len(self.sources)):
            raise ValueError("official calendar sources must be unique and bounded")
        if any(source.parser not in (*FOMC_PARSERS, "OFFICIAL_ICS_V1", "OFFICIAL_FOMC_HTML_DATE_ONLY_V1")
               or source.source_class != NewsSourceClassV2.OFFICIAL_MACRO for source in self.sources):
            raise ValueError("official calendar requires supported official macro source parsers")
        self._lock = threading.Lock()
        self._worker: threading.Thread | None = None
        self._slot: _Slot | None = None
        self._position = 0
        self._next_fetch_ns = 0
        self._closed = False

    def _profile_proof(self, repo: OpsRepository, ref: str | None, artifact_type: str,
                       cutoff: int, source: NewsSourceConfigV2 | None = None) -> Mapping[str, object] | None:
        if ref is None or self.fomc_source_profile is None:
            return None
        entry = repo.get_artifact(ref)
        if entry is None or entry.artifact_type != artifact_type or entry.available_at_ns > cutoff:
            return None
        body = entry.metadata
        source = self.fomc_source_profile.source() if source is None else source
        if entry.artifact_ref != sha256_json(dict(body)) or entry.content_hash != entry.artifact_ref:
            return None
        common = {"schema_version", "source_id", "feed_url", "parser", "received_at_ns", "raw_ref", "raw_payload_hash"}
        expected = common | ({"qualification", "qualification_method", "exact_timezone_qualified_schedule", "required_classes"}
            if artifact_type == "OfficialCalendarSourceQualificationV1" else
            {"timezone", "evidence_scope", "calendar_timezone_statement"})
        if body.get("schema_version") == 2 and artifact_type == "OfficialCalendarSourceQualificationV1":
            expected |= {"timezone", "timezone_evidence_ref"}
        elif body.get("schema_version") != 1:
            return None
        if set(body) != expected:
            return None
        received = body.get("received_at_ns")
        if (type(received) is not int or received > cutoff or received > entry.available_at_ns
                or body.get("feed_url") != source.feed_url
                or body.get("parser") != source.parser
                or body.get("source_id") != source.source_id):
            return None
        raw_ref = body.get("raw_ref")
        raw = repo.get_artifact(raw_ref) if isinstance(raw_ref, str) else None
        if raw is None or raw.artifact_type != "OfficialCalendarRawV1" or raw.available_at_ns > received:
            return None
        raw_url = urlsplit(str(raw.metadata.get("source_url", "")))
        if (raw_url.scheme != "https" or raw_url.hostname not in source.allowed_hosts
                or type(raw.metadata.get("received_at_ns")) is not int
                or raw.metadata["received_at_ns"] > received):
            return None
        try:
            raw_bytes = base64.b64decode(raw.metadata["raw_bytes_base64"], validate=True)
        except (ValueError, TypeError, KeyError):
            return None
        raw_hash = hashlib.sha256(raw_bytes).hexdigest()
        if (len(raw_bytes) > RAW_BODY_MAX_BYTES or raw.metadata.get("raw_payload_hash") != raw_hash
                or body.get("raw_payload_hash") != raw_hash or raw.content_hash != raw_hash):
            return None
        if artifact_type == "OfficialCalendarSourceQualificationV1":
            if raw.metadata.get("source_url") != source.feed_url:
                return None
            qualified_timezone = None
            if source.parser == "OFFICIAL_FOMC_JSON_V1" and body.get("schema_version") == 2:
                if (body.get("timezone") != self.fomc_source_profile.timezone
                        or body.get("timezone_evidence_ref") != self.fomc_source_profile.timezone_evidence_ref):
                    return None
                timezone_proof = self._profile_proof(repo, body.get("timezone_evidence_ref"),
                                                    "OfficialCalendarTimezoneEvidenceV1", cutoff)
                timezone_value = timezone_proof.get("timezone") if timezone_proof is not None else None
                qualified_timezone = timezone_value if isinstance(timezone_value, str) else None
            parsed = parse_official_calendar_v1(raw_bytes, parser=source.parser, qualified_timezone=qualified_timezone)
            if not parsed.complete:
                return None
        else:
            statement = body.get("calendar_timezone_statement")
            if (body.get("evidence_scope") != "FED_OFFICIAL_CALENDAR_TIMEZONE_V1"
                    or not isinstance(statement, str)
                    or statement not in raw_bytes.decode("utf-8", errors="replace")
                    or re.fullmatch(r"All (?:calendar )?times(?: listed)? are Eastern Time\.?", statement, re.I) is None
                    or body.get("timezone") != "America/New_York"):
                return None
        return body

    def _source_qualification(self, repo: OpsRepository, source: NewsSourceConfigV2, cutoff: int) -> str:
        profile = self.fomc_source_profile
        if profile is None:
            return source.qualification.value
        ref = (profile.qualification_evidence_ref if source.source_id == "FED_FOMC_EXPLICIT_PROFILE"
               else profile.bls_qualification_evidence_ref)
        proof = self._profile_proof(repo, ref, "OfficialCalendarSourceQualificationV1", cutoff, source)
        required_classes = proof.get("required_classes") if proof is not None else None
        expected_classes = ("US_CPI", "US_PAYROLL") if source.parser == "OFFICIAL_ICS_V1" else ("FOMC_RATE_DECISION",)
        return "VERIFIED" if (proof is not None and proof.get("qualification") == "VERIFIED"
            and proof.get("exact_timezone_qualified_schedule") is True
            and proof.get("qualification_method") == "EXTERNAL_OFFICIAL_SOURCE_CAPABILITY_REVIEW_V1"
            and isinstance(required_classes, (list, tuple))
            and all(isinstance(item, str) for item in required_classes)
            and tuple(required_classes) == expected_classes) else "UNVERIFIED"

    def _fetch(self, source: NewsSourceConfigV2, started_ns: int) -> None:
        response, error = None, None
        try:
            fetch = getattr(self.transport, "fetch", None)
            response = fetch(source) if callable(fetch) else self.transport(source)  # type: ignore[operator]
            if not isinstance(response, FetchedDocumentV2):
                raise TypeError("calendar transport returned invalid response")
            final = urlsplit(response.final_url)
            if (response.status_code < 200 or response.status_code >= 300 or final.scheme != "https"
                    or final.hostname is None or final.hostname.lower() not in source.allowed_hosts
                    or response.received_at_ns < started_ns or len(response.body) > RAW_BODY_MAX_BYTES):
                raise ValueError("calendar response failed status, receipt, size, or host validation")
        except Exception:
            response, error = None, "OFFICIAL_CALENDAR_FETCH_FAILED"
        done = self.clock_ns()
        if done < started_ns or (response is not None and done < response.received_at_ns):
            response, error = None, "OFFICIAL_CALENDAR_CLOCK_REGRESSION"
        with self._lock:
            if not self._closed:
                self._slot = _Slot(source, started_ns, done, response, error)

    def _persist_response(self, repo: OpsRepository, slot: _Slot, cutoff: int) -> tuple[tuple[ScheduledEventV2, ...], CalendarCoverageV2, str, tuple[str, ...]]:
        assert slot.response is not None
        response, source = slot.response, slot.source
        digest = hashlib.sha256(response.body).hexdigest()
        final_url = response.final_url
        header_publication_ns = _header_publication_ns(response.headers)
        if header_publication_ns is not None and header_publication_ns > response.received_at_ns:
            header_publication_ns = None
        qualification = self._source_qualification(repo, source, cutoff)
        profile = self.fomc_source_profile
        is_fomc_profile = source.source_id == "FED_FOMC_EXPLICIT_PROFILE"
        timezone_proof = self._profile_proof(repo, profile.timezone_evidence_ref if profile and is_fomc_profile else None,
                                            "OfficialCalendarTimezoneEvidenceV1", cutoff)
        qualified_timezone = (profile.timezone if profile and timezone_proof is not None
            and timezone_proof.get("timezone") == profile.timezone else None)
        raw_meta = {"schema_version": 2, "source_id": source.source_id, "source_url": final_url,
                    "status_code": response.status_code, "received_at_ns": response.received_at_ns,
                    "raw_payload_hash": digest, "raw_bytes_base64": base64.b64encode(response.body).decode("ascii"),
                    "source_publication_at_ns": header_publication_ns,
                    "publication_status": "SOURCE_TIMESTAMP" if header_publication_ns is not None else "PUBLICATION_UNKNOWN",
                    "qualification": qualification, "parser": source.parser,
                    "source_profile": profile.to_dict() if profile else None,
                    "source_profile_hash": profile.content_hash if profile else None,
                    "qualification_evidence_ref": (profile.qualification_evidence_ref if profile and is_fomc_profile else
                        profile.bls_qualification_evidence_ref if profile else None),
                    "qualified_timezone": qualified_timezone,
                    "timezone_evidence_ref": profile.timezone_evidence_ref if qualified_timezone and profile else None}
        raw_ref = sha256_json({"artifact_type": "OfficialCalendarRawV1", "schema_version": 2,
            "source_id": source.source_id, "source_url": final_url, "raw_payload_hash": digest,
            "parser": source.parser, "source_profile_hash": raw_meta["source_profile_hash"],
            "qualification": qualification, "qualified_timezone": qualified_timezone,
            "timezone_evidence_ref": raw_meta["timezone_evidence_ref"]})
        if repo.get_artifact(raw_ref) is None:
            repo.register_artifact(ArtifactIndexEntryV2(raw_ref, "OfficialCalendarRawV1", digest,
                response.received_at_ns, response.received_at_ns, raw_meta))
        # Raw bytes and actual receipt become durable before the parser sees them.
        parser = source.parser
        result = parse_official_calendar_v1(response.body, parser=parser,
            received_at_ns=response.received_at_ns, qualified_timezone=qualified_timezone)
        extracted_at = self.clock_ns()
        if extracted_at < response.received_at_ns:
            raise ValueError("calendar extraction cannot precede actual receipt")
        events: list[ScheduledEventV2] = []
        event_entries: list[ArtifactIndexEntryV2] = []
        revision = digest
        for classification, scheduled_ns, uid, source_publication_ns in result.events:
            event_id = sha256_json({"source_id": CALENDAR_SOURCE_ID, "uid": uid, "classification": classification})
            dedup_ref = sha256_json({"artifact_type": "OfficialCalendarEventDedupV1",
                                     "event_id": event_id, "schedule_revision": revision})
            prior_entry = repo.get_artifact(dedup_ref)
            if prior_entry is not None:
                prior_wire = prior_entry.metadata.get("event")
                if isinstance(prior_wire, Mapping):
                    events.append(ScheduledEventV2(**{key: value for key, value in prior_wire.items()
                        if key != "schema_version"}))
                    continue
            if source_publication_ns is not None and source_publication_ns > response.received_at_ns:
                source_publication_ns = None
            source_event_ns = source_publication_ns if source_publication_ns is not None else response.received_at_ns
            evidence_body = {"schema_version": 1, "raw_ref": raw_ref, "source_id": source.source_id,
                "uid": uid, "schedule_revision": revision, "source_event_at_ns": source_event_ns,
                "source_publication_at_ns": source_publication_ns,
                "publication_status": "SOURCE_TIMESTAMP" if source_publication_ns is not None else "PUBLICATION_UNKNOWN",
                "actual_received_at_ns": response.received_at_ns, "available_at_ns": extracted_at}
            evidence_ref = sha256_json(evidence_body)
            event_entries.append(ArtifactIndexEntryV2(evidence_ref, "OfficialCalendarEventEvidenceV1",
                evidence_ref, extracted_at, extracted_at, evidence_body))
            events.append(ScheduledEventV2(event_id, classification, scheduled_ns, revision, CALENDAR_SOURCE_ID,
                source_event_ns, response.received_at_ns, extracted_at, evidence_ref))
            dedup_body = {"schema_version": 1, "event": events[-1].to_dict(), "event_id": event_id,
                          "schedule_revision": revision, "first_received_at_ns": response.received_at_ns}
            event_entries.append(ArtifactIndexEntryV2(dedup_ref, "OfficialCalendarEventDedupV1",
                dedup_ref, extracted_at, extracted_at, dedup_body))
        typed_event_entries = [ArtifactIndexEntryV2(event.content_hash, "ScheduledEventV2", event.content_hash,
            event.available_at_ns, event.available_at_ns, {"evidence": event.to_dict()}) for event in events]
        missing_ref = sha256_json({"artifact_type": "OfficialCalendarNotEstimableV1", "raw_ref": raw_ref,
                                   "source_id": source.source_id, "missing": list(result.missing_times)})
        missing_body = {"schema_version": 1, "raw_ref": raw_ref, "source_id": source.source_id,
                        "status": "NOT_ESTIMABLE", "missing_time_fields": list(result.missing_times),
                        "reasons": list(result.reasons), "received_at_ns": response.received_at_ns}
        entries = event_entries + typed_event_entries + [ArtifactIndexEntryV2(missing_ref, "OfficialCalendarNotEstimableV1",
                    missing_ref, extracted_at, extracted_at, missing_body)]
        repo.register_artifacts(tuple(entry for entry in entries if repo.get_artifact(entry.artifact_ref) is None))
        # This source-local coverage is complete only when its parser found no
        # gaps and an external qualification gate supplied VERIFIED evidence.
        event_times = [item[1] for item in result.events]
        covered_from = min(event_times) if event_times else response.received_at_ns
        covered_through = max(event_times) if event_times else response.received_at_ns
        required_by_parser = {"OFFICIAL_ICS_V1": ["US_CPI", "US_PAYROLL"],
                              "OFFICIAL_FOMC_HTML_DATE_ONLY_V1": ["FOMC_RATE_DECISION"],
                              "OFFICIAL_ICS_FOMC_V1": ["FOMC_RATE_DECISION"],
                              "OFFICIAL_FOMC_JSON_V1": ["FOMC_RATE_DECISION"]}
        event_classes = sorted({item[0] for item in result.events})
        coverage_body = {"schema_version": 2, "source_id": source.source_id, "raw_ref": raw_ref,
            "revision": revision, "complete": result.complete,
            "source_qualification": qualification, "received_at_ns": response.received_at_ns,
            "source_profile_hash": profile.content_hash if profile else None, "parser": parser,
            "qualification_evidence_ref": raw_meta["qualification_evidence_ref"],
            "event_classes": event_classes, "required_classes": required_by_parser.get(parser, []),
            "covered_from_ns": covered_from, "covered_through_ns": covered_through,
            "missing_times": list(result.missing_times), "reasons": list(result.reasons)}
        coverage_ref = sha256_json(coverage_body)
        if repo.get_artifact(coverage_ref) is None:
            repo.register_artifact(ArtifactIndexEntryV2(coverage_ref, "OfficialCalendarCoverageEvidenceV1",
                coverage_ref, extracted_at, extracted_at, coverage_body))
        coverage = self._aggregate_coverage(repo, extracted_at)
        return tuple(events), coverage, raw_ref, result.reasons

    def _aggregate_coverage(self, repo: OpsRepository, available_at_ns: int) -> CalendarCoverageV2:
        """Build fail-closed CPI/payroll/FOMC coverage from latest source records."""
        latest: dict[str, Mapping[str, object]] = {}
        for source in self.sources:
            page = repo.latest_artifact_entries("OfficialCalendarCoverageEvidenceV1",
                as_of_ns=available_at_ns, limit=1, metadata_path=("source_id",), identity_value=source.source_id)
            if page.invalid_entry_count:
                raise ValueError("persisted source-calendar coverage evidence is invalid")
            if page.entries:
                latest[source.source_id] = page.entries[0].metadata
        class_status: dict[str, bool] = {}
        evidence_refs: list[str] = []
        intervals: list[tuple[int, int]] = []
        all_sources_verified = bool(self.sources)
        for source in self.sources:
            parser_requirements = ("US_CPI", "US_PAYROLL") if source.parser == "OFFICIAL_ICS_V1" else (
                ("FOMC_RATE_DECISION",) if source.parser in (*FOMC_PARSERS, "OFFICIAL_FOMC_HTML_DATE_ONLY_V1") else ())
            if not parser_requirements:
                all_sources_verified = False
                continue
            item = latest.get(source.source_id)
            verified = self._source_qualification(repo, source, available_at_ns) == "VERIFIED"
            all_sources_verified = all_sources_verified and verified and item is not None \
                and item.get("source_qualification") == "VERIFIED"
            if item is None:
                for classification in parser_requirements:
                    class_status[classification] = False
                continue
            evidence_refs.append(sha256_json(dict(item)))
            local_ok = item.get("complete") is True and item.get("source_qualification") == "VERIFIED"
            raw_classes = item.get("event_classes", ())
            if not isinstance(raw_classes, (tuple, list)) or any(not isinstance(value, str) for value in raw_classes):
                raise ValueError("source calendar event classes are invalid")
            observed_classes = set(raw_classes)
            for classification in parser_requirements:
                class_status[classification] = class_status.get(classification, True) and local_ok and classification in observed_classes
            start, end = item.get("covered_from_ns"), item.get("covered_through_ns")
            if type(start) is int and type(end) is int:
                intervals.append((start, end))
        complete = (all(class_status.get(item, False) for item in CALENDAR_EVENT_TYPES)
                    and all_sources_verified and len(intervals) == len(self.sources))
        if intervals:
            covered_from = max(item[0] for item in intervals)
            covered_through = min(item[1] for item in intervals)
            if covered_through < covered_from:
                complete = False
                covered_from = covered_through = available_at_ns
        else:
            covered_from = covered_through = available_at_ns
        revision = sha256_json({"source_evidence": sorted(evidence_refs), "classes": class_status})
        receipt_times = []
        for item in latest.values():
            receipt_time = item.get("received_at_ns")
            if type(receipt_time) is int:
                receipt_times.append(receipt_time)
        if not receipt_times:
            raise ValueError("aggregate calendar coverage requires at least one actual source receipt")
        observed_at_ns, received_at_ns = min(receipt_times), max(receipt_times)
        evidence_body = {"schema_version": 2, "source_id": CALENDAR_SOURCE_ID, "revision": revision,
            "complete": complete, "required_classes": list(CALENDAR_EVENT_TYPES),
            "class_status": class_status, "source_evidence_refs": sorted(evidence_refs),
            "source_qualification": "VERIFIED" if all_sources_verified else "UNVERIFIED",
            "covered_from_ns": covered_from, "covered_through_ns": covered_through,
            "observed_at_ns": observed_at_ns, "received_at_ns": received_at_ns,
            "available_at_ns": available_at_ns}
        evidence_ref = sha256_json(evidence_body)
        if repo.get_artifact(evidence_ref) is None:
            repo.register_artifact(ArtifactIndexEntryV2(evidence_ref, "OfficialCalendarAggregateEvidenceV1",
                evidence_ref, available_at_ns, available_at_ns, evidence_body))
        coverage = CalendarCoverageV2(CALENDAR_SOURCE_ID, covered_from, covered_through,
            observed_at_ns, received_at_ns, available_at_ns, complete, revision, evidence_ref,
            "VERIFIED" if all_sources_verified else "UNVERIFIED")
        typed = ArtifactIndexEntryV2(coverage.content_hash, "CalendarCoverageV2", coverage.content_hash,
            available_at_ns, available_at_ns, {"evidence": coverage.to_dict()})
        if repo.get_artifact(typed.artifact_ref) is None:
            repo.register_artifact(typed)
        return coverage

    def run_cycle(self, repo: OpsRepository, *, information_cutoff_ns: int) -> Mapping[str, object]:
        cutoff = timestamp(information_cutoff_ns, field="official_calendar.information_cutoff_ns")
        started = self.clock_ns()
        if started < cutoff:
            raise ValueError("calendar cycle computation precedes its information cutoff")
        with self._lock:
            if self._closed:
                return {"status": "CLOSED", "available_at_ns": started, "coverage": None, "events": ()}
            slot = self._slot
            if slot is not None and slot.completed_ns <= cutoff and (slot.response is None or slot.response.received_at_ns <= cutoff):
                self._slot = None
            else:
                slot = None
            inflight = self._worker is not None and self._worker.is_alive()
            launched = False
            if not inflight and self._slot is None and started >= self._next_fetch_ns:
                source = self.sources[self._position]
                self._position = (self._position + 1) % len(self.sources)
                self._next_fetch_ns = started + CALENDAR_CADENCE_NS
                self._worker = threading.Thread(target=self._fetch, args=(source, started),
                    name="atlas-official-calendar-fetch", daemon=True)
                self._worker.start()
                launched = True
        if slot is None:
            return {"status": "FETCH_STARTED" if launched else "PENDING", "available_at_ns": started,
                    "coverage": None, "events": (), "completed_slot_count": int(self._slot is not None)}
        if slot.response is None:
            return {"status": "FAILED", "reason": slot.error, "available_at_ns": slot.completed_ns,
                    "coverage": None, "events": ()}
        events, coverage, raw_ref, reasons = self._persist_response(repo, slot, cutoff)
        return {"status": "COLLECTED" if coverage.complete else "NOT_ESTIMABLE",
                "available_at_ns": coverage.available_at_ns, "source_id": slot.source.source_id,
                "raw_ref": raw_ref, "coverage": coverage, "events": events,
                "reasons": reasons, "capital_authority": "ZERO", "source_authentication": "UNKNOWN"}

    def close(self, *, timeout_s: float = CALENDAR_TIMEOUT_S + 0.1) -> None:
        if not 0 <= timeout_s <= CALENDAR_TIMEOUT_S + 0.1:
            raise ValueError("calendar worker close join must remain bounded")
        with self._lock:
            self._closed = True
            self._slot = None
            worker = self._worker
        if worker is not None:
            worker.join(timeout=timeout_s)


def _is_fomc_decision_title(summary: str) -> bool:
    title = " ".join(summary.casefold().split())
    return title in {
        "fomc meeting", "fomc rate decision", "fomc statement",
        "federal reserve issues fomc statement", "federal funds rate decision",
        "federal open market committee rate decision", "monetary policy decision",
    }


def parse_exact_fomc_ics_v1(body: bytes) -> CalendarParseResultV1:
    """Extract exact decisions; minutes and press conferences are different events."""
    try:
        lines = _unfold_ical(body.decode("utf-8-sig"))
    except UnicodeDecodeError:
        return CalendarParseResultV1((), (), False, ("INVALID_ICS_ENCODING",))
    rows: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    reasons: set[str] = set()
    if "BEGIN:VCALENDAR" not in lines or "END:VCALENDAR" not in lines:
        reasons.add("INVALID_ICS_STRUCTURE")
    for line in lines:
        if line.upper() == "BEGIN:VEVENT":
            if current is not None:
                reasons.add("INVALID_ICS_STRUCTURE")
            current = {}
        elif line.upper() == "END:VEVENT":
            if current is not None:
                rows.append(current)
            else:
                reasons.add("INVALID_ICS_STRUCTURE")
            current = None
        elif current is not None:
            name = line.partition(":")[0].split(";", 1)[0].upper()
            if name in ("UID", "SUMMARY", "DTSTART", "DTSTAMP", "LAST-MODIFIED", "RRULE", "RDATE"):
                if name in current:
                    reasons.add("DUPLICATE_ICS_PROPERTY")
                current[name] = line
    if current is not None:
        reasons.add("INVALID_ICS_STRUCTURE")
    events: list[tuple[str, int, str, int | None]] = []
    missing: list[Mapping[str, object]] = []
    seen_uid: set[str] = set()
    for row in rows:
        summary = row.get("SUMMARY", "").partition(":")[2]
        if not _is_fomc_decision_title(summary):
            continue
        uid = row.get("UID", "").partition(":")[2].strip()
        if not uid or uid in seen_uid:
            reasons.add("MISSING_OR_DUPLICATE_ICS_UID")
            continue
        seen_uid.add(uid)
        if "RRULE" in row or "RDATE" in row:
            reasons.add("UNSUPPORTED_RECURRING_FOMC_EVENT")
            continue
        scheduled = _ics_datetime(row.get("DTSTART", ""))
        if scheduled is None:
            missing.append({"classification": "FOMC_RATE_DECISION", "uid": uid,
                            "missing_field": "EXACT_TIMEZONE_QUALIFIED_DTSTART"})
            reasons.add("OFFICIAL_SCHEDULE_TIME_NOT_ESTIMABLE")
        else:
            published_line = row.get("DTSTAMP") or row.get("LAST-MODIFIED")
            published = _ics_datetime(published_line) if published_line else None
            events.append(("FOMC_RATE_DECISION", scheduled, uid, published))
    if not events:
        reasons.add("REQUIRED_RELEASE_CLASS_MISSING")
    return CalendarParseResultV1(tuple(sorted(events, key=lambda item: (item[1], item[2]))),
        tuple(missing), bool(events) and not reasons, tuple(sorted(reasons)))


def parse_fomc_json_v1(body: bytes, *, qualified_timezone: str | None = None) -> CalendarParseResultV1:
    """Read the official calendar.json schema without supplying absent timezones.

    A qualified_timezone is supplied only after the runtime resolves separately
    archived official timezone evidence. It is never derived from geography or
    historical statement release times.
    """
    try:
        data = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError):
        return CalendarParseResultV1((), (), False, ("INVALID_FOMC_JSON",))
    rows = data.get("events") if isinstance(data, dict) else None
    if not isinstance(rows, list) or len(rows) > 10_000:
        return CalendarParseResultV1((), (), False, ("INVALID_FOMC_JSON_EVENTS",))
    events: list[tuple[str, int, str, int | None]] = []
    missing: list[Mapping[str, object]] = []
    reasons: set[str] = set()
    seen_uid: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            reasons.add("INVALID_FOMC_JSON_EVENT")
            continue
        if row.get("type") != "FOMC" or not isinstance(row.get("title"), str) or not _is_fomc_decision_title(row["title"]):
            continue
        month, day, clock = row.get("month"), row.get("days"), row.get("time")
        zone = row.get("timezone", qualified_timezone)
        when: int | None = None
        uid = ""
        if (isinstance(month, str) and re.fullmatch(r"20\d{2}-\d{2}", month)
                and isinstance(day, str) and re.fullmatch(r"\d{1,2}", day)):
            uid = f"fomc:{month}-{int(day):02d}"
            match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*([ap])\.m\.", clock.strip(), re.I) if isinstance(clock, str) else None
            if match and isinstance(zone, str) and 1 <= int(match[1]) <= 12 and 0 <= int(match[2]) <= 59:
                hour = int(match[1]) % 12 + (12 if match[3].lower() == "p" else 0)
                value = f"{month.replace('-', '')}{int(day):02d}T{hour:02d}{int(match[2]):02d}00"
                when = _ics_datetime(f"DTSTART;TZID={zone}:{value}")
        if not uid or uid in seen_uid:
            reasons.add("MISSING_OR_DUPLICATE_FOMC_IDENTITY")
            continue
        seen_uid.add(uid)
        if when is None:
            missing.append({"classification": "FOMC_RATE_DECISION", "uid": uid,
                "source_time": clock, "missing_field": "EXACT_TIMEZONE_QUALIFIED_TIME"})
            reasons.add("OFFICIAL_SCHEDULE_TIME_NOT_ESTIMABLE")
        else:
            events.append(("FOMC_RATE_DECISION", when, uid, None))
    if not events:
        reasons.add("REQUIRED_RELEASE_CLASS_MISSING")
    return CalendarParseResultV1(tuple(sorted(events, key=lambda item: (item[1], item[2]))),
        tuple(missing), bool(events) and not reasons, tuple(sorted(reasons)))


def parse_official_calendar_v1(body: bytes, *, parser: str, received_at_ns: int | None = None,
                               qualified_timezone: str | None = None) -> CalendarParseResultV1:
    """Shared acquisition/readback parser policy, including future class coverage."""
    if parser == "OFFICIAL_ICS_V1":
        result = parse_bls_ics_v1(body)
    elif parser == "OFFICIAL_ICS_FOMC_V1":
        result = parse_exact_fomc_ics_v1(body)
    elif parser == "OFFICIAL_FOMC_HTML_DATE_ONLY_V1":
        result = parse_fomc_html_dates_v1(body)
    elif parser == "OFFICIAL_FOMC_JSON_V1":
        result = parse_fomc_json_v1(body, qualified_timezone=qualified_timezone)
    else:
        raise ValueError("unsupported official calendar parser")
    if received_at_ns is not None:
        required = ("US_CPI", "US_PAYROLL") if parser == "OFFICIAL_ICS_V1" else ("FOMC_RATE_DECISION",)
        future = {kind for kind, when, _, _ in result.events if when > received_at_ns}
        if not set(required).issubset(future):
            return CalendarParseResultV1(result.events, result.missing_times, False,
                tuple(sorted(set(result.reasons) | {"FUTURE_REQUIRED_RELEASE_CLASS_MISSING"})))
    return result
