"""Strict, bounded projections of complete broad-universe research evidence."""
from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from .._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from ..instruments import ProductContractV2, UniverseContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository

ROLE_NAMES = (
    "S4ExecutionContext", "S4StandaloneShadow", "S5Continuation", "S5Reversal",
    "S4", "S5", "S6", "S7_DIRECTIONAL_SHADOW", "MODEL_ARENA_OPTIONAL", "S8_RESEARCH_BASKET",
)
SURFACE_TYPE = "FULL_STRATEGY_RESEARCH_SURFACE_V1"
CALENDAR_EXPORT_TYPES = (
    "OfficialCalendarRawV1", "OfficialCalendarEventEvidenceV1", "OfficialCalendarEventDedupV1",
    "OfficialCalendarNotEstimableV1", "OfficialCalendarCoverageEvidenceV1",
    "OfficialCalendarAggregateEvidenceV1", "ScheduledEventV2", "CalendarCoverageV2",
)
CALENDAR_PROFILE_PROOF_EXPORT_TYPES = (
    "OfficialCalendarSourceQualificationV1", "OfficialCalendarTimezoneEvidenceV1",
)
BROAD_EXPORT_TYPES = (
    "UniverseContractV2", "CandidateSetV2", "BroadUniverseWorksetV2",
    "BroadPublicAcquisitionReceiptV2", "BroadPublicSchedulerStateV2",
    "BroadPublicAcquisitionWaitV2", "BroadPublicDuplicateReceiptV2",
    "OpsDecisionSourceScopeV2", SURFACE_TYPE,
    "BroadResearchDeferredV1", "NativeStrategyHistoryBarV1", "S7M5AggregationReceiptV1",
    "S7DerivedM5BarV1", "S7DerivedPreEventBetaV1", "S7DerivedMarketReturn5MV1",
    "S7DerivedReactionSpreadV1", "S8PairOwnerCatalogV1", "S8PairDefinitionV2",
    "ResearchBasketForecastV2",
    "S8ResidualDiagnosticsV1", "S8BasketOutcomeEvidenceV1", "S8BasketOutcomeV1",
    "TradeLocationInputReceiptV1",
    "S4FeatureArtifactV2", "S5CrowdingContextV2", "S6LiquidityFundingEvidenceV2",
    "S4ExpectedResponseBaselineV2", "S4AbsorptionHypothesisV2", "S5StructuralStageEvidenceV1",
    "S5PreparedDiagnosticsV1", "FundingObservationV2", "OpenInterestObservationV2", "OIChangeEvidenceV2",
    "BroadPublicAdoptionRejectionV1",
    *CALENDAR_EXPORT_TYPES,
    *CALENDAR_PROFILE_PROOF_EXPORT_TYPES,
    *(f"{name}ResearchRoleV1" for name in ROLE_NAMES),
)
LARGE_TYPES = ("UniverseContractV2", "CandidateSetV2", "BroadUniverseWorksetV2",
               "BroadPublicAcquisitionReceiptV2")
MAX_BROAD_METADATA_BYTES = 32 * 1024 * 1024
_SECRET_FIELDS = frozenset({"secret", "api_key", "apikey", "api_secret", "apisecret",
                            "password", "authorization", "credential", "credentials", "access_token"})


def _safe_public_payload(value: Any, *, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("broad export nesting exceeds its bound")
    if isinstance(value, Mapping):
        if any(str(key).lower() in _SECRET_FIELDS for key in value):
            raise ValueError("private fields are not public research evidence")
        for child in value.values():
            _safe_public_payload(child, depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _safe_public_payload(child, depth=depth + 1)


def _exact_body(entry: ArtifactIndexEntryV2, field: str) -> dict[str, Any]:
    if set(entry.metadata) != {field} or not isinstance(entry.metadata[field], Mapping):
        raise ValueError("broad export requires its exact typed body")
    body = json_value(entry.metadata[field])
    if entry.artifact_ref != entry.content_hash or sha256_json(body) != entry.content_hash:
        raise ValueError("broad research body hash mismatch")
    _safe_public_payload(body)
    return body


def _refs(repository: OpsRepository, refs: Any, *, available_at_ns: int,
          maximum: int = 16384) -> Mapping[str, Mapping[str, Any]]:
    if (not isinstance(refs, (tuple, list)) or len(refs) > maximum
            or tuple(refs) != tuple(sorted(set(refs)))):
        raise ValueError("broad references must be unique, sorted and bounded")
    entries = repository.get_artifact_metadata_by_refs(refs)
    if any(ref not in entries or entries[ref].get("effective_available_at_ns") is None
           or entries[ref]["effective_available_at_ns"] > available_at_ns for ref in refs):
        raise ValueError("broad evidence dependency missing or unavailable")
    return entries


def _universe(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> UniverseContractV2:
    cache = getattr(repository, "_broad_universe_cache", None)
    cache_key = (entry.artifact_ref, entry.content_hash, entry.created_at_ns, entry.available_at_ns)
    if cache is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            original, universe = cached
            if original == entry:
                cache.move_to_end(cache_key)
                return universe
    if set(entry.metadata) != {"universe"}:
        raise ValueError("universe export has unexpected fields")
    universe = UniverseContractV2.from_dict(json_value(entry.metadata["universe"]))
    if (universe.content_hash != entry.content_hash or entry.artifact_ref != entry.content_hash
            or universe.envelope.available_at_ns != entry.available_at_ns
            or universe.envelope.created_at_ns != entry.created_at_ns or len(universe.entries) > 4096):
        raise ValueError("universe export identity or population mismatch")
    product_refs = tuple(sorted({item.product_ref for item in universe.entries}))
    # Product metadata is also part of the universe envelope input set. Resolve
    # the union once: fetching the products and then the full envelope repeats
    # expensive compact-index decoding for every contract in a broad market.
    # The artifact availability cutoff is no later than the decision slot; keep
    # the explicit product cutoff below as a separate causal assertion.
    envelope_refs = universe.envelope.input_refs
    if len(envelope_refs) > 16384:
        raise ValueError("universe envelope input population exceeds its bound")
    resolved_refs = _refs(repository, tuple(sorted(set(product_refs).union(envelope_refs))),
        available_at_ns=entry.available_at_ns, maximum=16384)
    for item in universe.entries:
        indexed = resolved_refs[item.product_ref]
        if (indexed["available_at_ns"] > universe.decision_slot_ns
                or indexed["artifact_type"] != "ProductContractV2"):
            raise ValueError("universe requires typed product metadata")
        product = ProductContractV2.from_dict(json_value(indexed["metadata"]["product"]))
        if (product.content_hash != item.product_ref or indexed["content_hash"] != item.product_ref
                or product.key != item.key or product.effective_at_ns > universe.decision_slot_ns):
            raise ValueError("universe product revision identity or chronology mismatch")
    if cache is not None:
        cache[cache_key] = (entry, universe)
        cache.move_to_end(cache_key)
        while len(cache) > 8:
            cache.popitem(last=False)
    return universe


def _calendar_evidence(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> dict[str, Any]:
    """Validate the actual calendar archive identities and receipt lineage."""
    from ..news.events import RAW_BODY_MAX_BYTES, CalendarCoverageV2, ScheduledEventV2
    from ..runtime.official_calendar import CALENDAR_EVENT_TYPES, CALENDAR_SOURCE_ID

    kind = entry.artifact_type
    metadata = json_value(entry.metadata)
    if kind in CALENDAR_PROFILE_PROOF_EXPORT_TYPES:
        from ..news.events import NewsSourceClassV2, NewsSourceConfigV2
        from ..runtime.official_calendar import OfficialCalendarMaintenanceV1, OfficialFomcSourceProfileV1

        parser = metadata.get("parser")
        is_bls = parser == "OFFICIAL_ICS_V1"
        zone = metadata.get("timezone")
        zone_ref = (entry.artifact_ref if kind == "OfficialCalendarTimezoneEvidenceV1"
                    else metadata.get("timezone_evidence_ref"))
        profile = OfficialFomcSourceProfileV1(
            "https://www.federalreserve.gov/json/calendar.json" if is_bls else metadata.get("feed_url"),
            parser="OFFICIAL_FOMC_JSON_V1" if is_bls else parser,
            timezone=zone if zone_ref else None, timezone_evidence_ref=zone_ref)
        runtime = OfficialCalendarMaintenanceV1(fomc_source_profile=profile)
        source_config = NewsSourceConfigV2(metadata.get("source_id"), NewsSourceClassV2.OFFICIAL_MACRO,
            metadata.get("feed_url"), ("www.bls.gov",) if is_bls else ("www.federalreserve.gov",), parser=parser)
        proof = runtime._profile_proof(repository, entry.artifact_ref, kind, entry.available_at_ns, source_config)
        if proof is None or entry.created_at_ns != entry.available_at_ns:
            raise ValueError("calendar profile proof is missing exact official raw evidence or scope")
        if kind == "OfficialCalendarSourceQualificationV1" and (
                metadata["qualification"] != "VERIFIED" or metadata["exact_timezone_qualified_schedule"] is not True
                or metadata["qualification_method"] != "EXTERNAL_OFFICIAL_SOURCE_CAPABILITY_REVIEW_V1"
                or metadata["required_classes"] != (["US_CPI", "US_PAYROLL"] if is_bls else ["FOMC_RATE_DECISION"])):
            raise ValueError("calendar qualification proof lacks an external capability review")
        _safe_public_payload(metadata)
        return metadata
    fields = {
        "OfficialCalendarRawV1": {"schema_version", "source_id", "source_url", "status_code",
            "received_at_ns", "raw_payload_hash", "raw_bytes_base64", "source_publication_at_ns",
            "publication_status", "qualification"},
        "OfficialCalendarEventEvidenceV1": {"schema_version", "raw_ref", "source_id", "uid",
            "schedule_revision", "source_event_at_ns", "source_publication_at_ns", "publication_status",
            "actual_received_at_ns", "available_at_ns"},
        "OfficialCalendarEventDedupV1": {"schema_version", "event", "event_id", "schedule_revision",
            "first_received_at_ns"},
        "OfficialCalendarNotEstimableV1": {"schema_version", "raw_ref", "source_id", "status",
            "missing_time_fields", "reasons", "received_at_ns"},
        "OfficialCalendarCoverageEvidenceV1": {"schema_version", "source_id", "raw_ref", "revision",
            "complete", "source_qualification", "received_at_ns", "event_classes", "required_classes",
            "covered_from_ns", "covered_through_ns", "missing_times", "reasons"},
        "OfficialCalendarAggregateEvidenceV1": {"schema_version", "source_id", "revision", "complete",
            "required_classes", "class_status", "source_evidence_refs", "source_qualification",
            "observed_at_ns", "received_at_ns", "available_at_ns"},
    }

    def dependency(ref: str, expected_kind: str) -> tuple[ArtifactIndexEntryV2, dict[str, Any]]:
        target = repository.get_artifact(ref)
        if (target is None or target.artifact_type != expected_kind
                or target.available_at_ns > entry.available_at_ns):
            raise ValueError("calendar source evidence is missing, future, or mistyped")
        return target, _calendar_evidence(repository, target)

    if kind in {"ScheduledEventV2", "CalendarCoverageV2"}:
        body = _exact_body(entry, "evidence")
        cls = ScheduledEventV2 if kind == "ScheduledEventV2" else CalendarCoverageV2
        if body.get("schema_version") != 1:
            raise ValueError("calendar typed schema mismatch")
        value: ScheduledEventV2 | CalendarCoverageV2
        try:
            value = cls(**{name: item for name, item in body.items() if name != "schema_version"})
        except TypeError as error:
            raise ValueError("calendar typed fields mismatch") from error
        if value.to_dict() != body or value.available_at_ns != entry.available_at_ns:
            raise ValueError("calendar typed publication mismatch")
        if isinstance(value, ScheduledEventV2):
            _, source = dependency(value.evidence_ref, "OfficialCalendarEventEvidenceV1")
            raw_entry, raw = dependency(source["raw_ref"], "OfficialCalendarRawV1")
            if (value.source_id != CALENDAR_SOURCE_ID
                    or value.event_id != sha256_json({"source_id": CALENDAR_SOURCE_ID,
                        "uid": source["uid"], "classification": value.classification})
                    or value.schedule_revision != source["schedule_revision"]
                    or value.source_event_at_ns != source["source_event_at_ns"]
                    or value.received_at_ns != source["actual_received_at_ns"]
                    or value.available_at_ns != source["available_at_ns"]):
                raise ValueError("calendar event source binding mismatch")
            from ..runtime.official_calendar import parse_official_calendar_v1

            parser = raw.get("parser", "OFFICIAL_ICS_FOMC_V1" if value.classification == "FOMC_RATE_DECISION" else "OFFICIAL_ICS_V1")
            raw_bytes = base64.b64decode(raw_entry.metadata["raw_bytes_base64"], validate=True)
            if not any(classification == value.classification and scheduled == value.scheduled_at_ns
                       and uid == source["uid"] for classification, scheduled, uid, _ in parse_official_calendar_v1(
                           raw_bytes, parser=parser, qualified_timezone=raw.get("qualified_timezone")).events):
                raise ValueError("calendar event schedule absent from exact source bytes")
        elif isinstance(value, CalendarCoverageV2):
            _, source = dependency(value.evidence_ref, "OfficialCalendarAggregateEvidenceV1")
            if (value.source_id != source["source_id"] or value.revision != source["revision"]
                    or value.complete != source["complete"]
                    or value.source_qualification != source["source_qualification"]
                    or any(getattr(value, name) != source[name] for name in
                           ("observed_at_ns", "received_at_ns", "available_at_ns"))):
                raise ValueError("calendar aggregate coverage binding mismatch")
            sources = [dependency(ref, "OfficialCalendarCoverageEvidenceV1")[1]
                       for ref in source["source_evidence_refs"]]
            covered_from = max(item["covered_from_ns"] for item in sources)
            covered_through = min(item["covered_through_ns"] for item in sources)
            if covered_through < covered_from:
                covered_from = covered_through = value.available_at_ns
            if (value.covered_from_ns, value.covered_through_ns) != (covered_from, covered_through):
                raise ValueError("calendar aggregate coverage interval mismatch")
        return body

    body = metadata
    version = body.get("schema_version")
    expected_fields = fields[kind]
    versioned_fields = {
        "OfficialCalendarRawV1": {"parser", "source_profile", "source_profile_hash",
            "qualification_evidence_ref", "qualified_timezone", "timezone_evidence_ref"},
        "OfficialCalendarCoverageEvidenceV1": {"parser", "source_profile_hash", "qualification_evidence_ref"},
        "OfficialCalendarAggregateEvidenceV1": {"covered_from_ns", "covered_through_ns"},
    }
    if version == 2 and kind in versioned_fields:
        expected_fields = expected_fields | versioned_fields[kind]
    elif version != 1:
        raise ValueError("calendar evidence schema version mismatch")
    if (set(body) != expected_fields
            or entry.created_at_ns != entry.available_at_ns):
        raise ValueError("calendar evidence fields or publication mismatch")
    _safe_public_payload(body)
    for name, at in body.items():
        if name.endswith("_ns") and at is not None:
            timestamp(at, field=f"calendar.{name}")
    if kind == "OfficialCalendarRawV1":
        encoded = body["raw_bytes_base64"]
        if not isinstance(encoded, str) or len(encoded) > 4 * ((RAW_BODY_MAX_BYTES + 2) // 3):
            raise ValueError("calendar source bytes exceed their bound")
        try:
            raw_bytes = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise ValueError("calendar source bytes encoding mismatch") from error
        digest = hashlib.sha256(raw_bytes).hexdigest()
        url = urlsplit(body["source_url"])
        if (len(raw_bytes) > RAW_BODY_MAX_BYTES or digest != body["raw_payload_hash"]
                or entry.content_hash != digest or body["received_at_ns"] != entry.available_at_ns
                or not isinstance(body["source_id"], str) or not body["source_id"].strip()
                or type(body["status_code"]) is not int or not 200 <= body["status_code"] < 300
                or url.scheme != "https" or not url.hostname or url.username or url.password
                or body["qualification"] not in {"VERIFIED", "UNVERIFIED"}):
            raise ValueError("calendar raw identity or actual receipt mismatch")
        identity = {"artifact_type": kind, "source_id": body["source_id"],
            "source_url": body["source_url"], "raw_payload_hash": digest}
        if version == 2:
            from ..runtime.official_calendar import (
                DEFAULT_CALENDAR_SOURCES,
                OfficialCalendarMaintenanceV1,
                OfficialFomcSourceProfileV1,
                parse_official_calendar_v1,
            )

            # Supported parser and exact profile shape are checked before any
            # archived timezone or qualification can affect schedule replay.
            parse_official_calendar_v1(raw_bytes, parser=body["parser"])
            profile_body = body["source_profile"]
            if profile_body is None:
                if any(body[name] is not None for name in ("source_profile_hash", "qualification_evidence_ref",
                                                          "qualified_timezone", "timezone_evidence_ref")):
                    raise ValueError("unconfigured calendar source claims profile proof")
            else:
                if not isinstance(profile_body, Mapping):
                    raise ValueError("calendar source profile must be an exact object")
                profile = OfficialFomcSourceProfileV1.from_dict(profile_body)
                if profile.to_dict() != profile_body or profile.content_hash != body["source_profile_hash"]:
                    raise ValueError("calendar source immutable profile mismatch")
                source_config = profile.source() if body["source_id"] == "FED_FOMC_EXPLICIT_PROFILE" else DEFAULT_CALENDAR_SOURCES[0]
                if (source_config.source_id != body["source_id"] or source_config.parser != body["parser"]
                        or source_config.feed_url != body["source_url"]):
                    raise ValueError("calendar raw source is outside its profile scope")
                runtime = OfficialCalendarMaintenanceV1(fomc_source_profile=profile)
                proof = runtime._profile_proof(repository, profile.timezone_evidence_ref,
                    "OfficialCalendarTimezoneEvidenceV1", entry.available_at_ns) if source_config.parser == "OFFICIAL_FOMC_JSON_V1" else None
                expected_zone = profile.timezone if proof is not None else None
                expected_zone_ref = profile.timezone_evidence_ref if expected_zone else None
                expected_qualification_ref = (profile.qualification_evidence_ref
                    if source_config.source_id == "FED_FOMC_EXPLICIT_PROFILE" else profile.bls_qualification_evidence_ref)
                if (body["qualified_timezone"] != expected_zone or body["timezone_evidence_ref"] != expected_zone_ref
                        or body["qualification_evidence_ref"] != expected_qualification_ref
                        or body["qualification"] != runtime._source_qualification(repository, source_config, entry.available_at_ns)):
                    raise ValueError("calendar qualification or timezone lacks actual source evidence")
            identity.update({"schema_version": 2, "parser": body["parser"],
                "source_profile_hash": body["source_profile_hash"], "qualification": body["qualification"],
                "qualified_timezone": body["qualified_timezone"], "timezone_evidence_ref": body["timezone_evidence_ref"]})
    elif kind == "OfficialCalendarEventDedupV1":
        event_body = body["event"]
        if not isinstance(event_body, Mapping):
            raise ValueError("calendar dedup event is unavailable")
        event_ref = sha256_json(event_body)
        _, event = dependency(event_ref, "ScheduledEventV2")
        if (event != event_body or body["event_id"] != event["event_id"]
                or body["schedule_revision"] != event["schedule_revision"]
                or body["first_received_at_ns"] != event["received_at_ns"]
                or entry.available_at_ns != event["available_at_ns"]):
            raise ValueError("calendar dedup first receipt binding mismatch")
        identity = {"artifact_type": kind, "event_id": body["event_id"],
            "schedule_revision": body["schedule_revision"]}
    elif kind == "OfficialCalendarAggregateEvidenceV1":
        refs = body["source_evidence_refs"]
        _refs(repository, refs, available_at_ns=entry.available_at_ns, maximum=8)
        sources = [dependency(ref, "OfficialCalendarCoverageEvidenceV1")[1] for ref in refs]
        classes = body["class_status"]
        if (not sources or len({source["source_id"] for source in sources}) != len(sources)
                or body["source_id"] != CALENDAR_SOURCE_ID or body["required_classes"] != list(CALENDAR_EVENT_TYPES)
                or not isinstance(classes, Mapping) or set(classes) - set(CALENDAR_EVENT_TYPES)
                or any(type(status) is not bool for status in classes.values())
                or type(body["complete"]) is not bool
                or body["source_qualification"] not in {"VERIFIED", "UNVERIFIED"}
                or body["revision"] != sha256_json({"source_evidence": refs, "classes": classes})
                or body["observed_at_ns"] != min(source["received_at_ns"] for source in sources)
                or body["received_at_ns"] != max(source["received_at_ns"] for source in sources)
                or not body["received_at_ns"] <= body["available_at_ns"] == entry.available_at_ns):
            raise ValueError("calendar aggregate identity or actual receipt mismatch")
        for classification, status in classes.items():
            relevant = [source for source in sources if classification in source["required_classes"]]
            eligible = bool(relevant) and all(source["complete"] and source["source_qualification"] == "VERIFIED"
                and classification in source["event_classes"] for source in relevant)
            if status and not eligible:
                raise ValueError("calendar aggregate promotes unavailable class coverage")
        if ((body["source_qualification"] == "VERIFIED"
                and any(source["source_qualification"] != "VERIFIED" for source in sources))
                or (body["complete"] and (body["source_qualification"] != "VERIFIED"
                    or not all(classes.get(name, False) for name in CALENDAR_EVENT_TYPES)
                    or max(source["covered_from_ns"] for source in sources)
                        > min(source["covered_through_ns"] for source in sources)))):
            raise ValueError("calendar aggregate completeness lacks qualified support")
        if version == 2:
            start = max(source["covered_from_ns"] for source in sources)
            end = min(source["covered_through_ns"] for source in sources)
            if end < start:
                start = end = entry.available_at_ns
            if (body["covered_from_ns"], body["covered_through_ns"]) != (start, end):
                raise ValueError("calendar aggregate evidence interval mismatch")
        identity = body
    else:
        raw_entry, raw = dependency(body["raw_ref"], "OfficialCalendarRawV1")
        received = body.get("actual_received_at_ns", body.get("received_at_ns"))
        if (body["source_id"] != raw["source_id"] or received < raw["received_at_ns"]
                or received > entry.available_at_ns):
            raise ValueError("calendar source identity or receipt chronology mismatch")
        if kind == "OfficialCalendarEventEvidenceV1":
            if (body["schedule_revision"] != raw["raw_payload_hash"]
                    or received != raw["received_at_ns"]
                    or not isinstance(body["uid"], str) or not body["uid"].strip()
                    or body["source_event_at_ns"] > received
                    or body["available_at_ns"] != entry.available_at_ns):
                raise ValueError("calendar event revision or chronology mismatch")
            identity = body
        elif kind == "OfficialCalendarNotEstimableV1":
            if body["status"] != "NOT_ESTIMABLE" or not isinstance(body["missing_time_fields"], list):
                raise ValueError("calendar missing-time status mismatch")
            identity = {"artifact_type": kind, "raw_ref": body["raw_ref"],
                "source_id": body["source_id"], "missing": body["missing_time_fields"]}
        else:
            if (body["revision"] != raw["raw_payload_hash"] or type(body["complete"]) is not bool
                    or body["source_qualification"] != raw["qualification"]
                    or body["covered_from_ns"] > body["covered_through_ns"]
                    or body["event_classes"] != sorted(set(body["event_classes"]))
                    or set(body["event_classes"]) - set(CALENDAR_EVENT_TYPES)
                    or body["required_classes"] not in (["US_CPI", "US_PAYROLL"], ["FOMC_RATE_DECISION"])
                    or not isinstance(body["missing_times"], list)
                    or (body["complete"] and (body["missing_times"] or body["reasons"]))):
                raise ValueError("calendar source coverage contract mismatch")
            from ..runtime.official_calendar import (
                parse_bls_ics_v1,
                parse_exact_fomc_ics_v1,
                parse_fomc_html_dates_v1,
            )

            raw_bytes = base64.b64decode(raw_entry.metadata["raw_bytes_base64"], validate=True)
            if version == 2:
                from ..runtime.official_calendar import parse_official_calendar_v1

                if (body["parser"] != raw.get("parser") or body["source_profile_hash"] != raw.get("source_profile_hash")
                        or body["qualification_evidence_ref"] != raw.get("qualification_evidence_ref")):
                    raise ValueError("calendar source coverage parser or profile binding mismatch")
                parsed = parse_official_calendar_v1(raw_bytes, parser=body["parser"], received_at_ns=received,
                    qualified_timezone=raw.get("qualified_timezone"))
            else:
                parsed = (parse_bls_ics_v1(raw_bytes) if body["required_classes"] == ["US_CPI", "US_PAYROLL"]
                      else parse_exact_fomc_ics_v1(raw_bytes) if b"BEGIN:VCALENDAR" in raw_bytes.upper()
                      else parse_fomc_html_dates_v1(raw_bytes))
            event_times = [item[1] for item in parsed.events]
            if (body["complete"] != parsed.complete
                    or body["event_classes"] != sorted({item[0] for item in parsed.events})
                    or body["missing_times"] != json_value(parsed.missing_times)
                    or body["reasons"] != list(parsed.reasons)
                    or body["covered_from_ns"] != (min(event_times) if event_times else received)
                    or body["covered_through_ns"] != (max(event_times) if event_times else received)):
                raise ValueError("calendar source coverage absent from exact source bytes")
            identity = body
    if kind in {"OfficialCalendarRawV1", "OfficialCalendarEventEvidenceV1"}:
        publication = body["source_publication_at_ns"]
        received = body.get("actual_received_at_ns", body.get("received_at_ns"))
        if (body["publication_status"] != ("PUBLICATION_UNKNOWN" if publication is None else "SOURCE_TIMESTAMP")
                or (publication is not None and publication > received)
                or (kind == "OfficialCalendarEventEvidenceV1"
                    and body["source_event_at_ns"] != (received if publication is None else publication))):
            raise ValueError("calendar publication status mismatch")
    expected_ref = sha256_json(identity)
    if (entry.artifact_ref != expected_ref
            or (kind != "OfficialCalendarRawV1" and entry.content_hash != expected_ref)):
        raise ValueError("calendar evidence hash mismatch")
    return {name: item for name, item in body.items() if name != "raw_bytes_base64"}


def _strategy_evidence(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> dict[str, Any]:
    from ..chronology import causal_artifact
    from ..data.raw import RawObservationV2
    from ..instruments import InstrumentKeyV2
    from ..news.events import _source_evidence_available

    kind = entry.artifact_type
    metadata = json_value(entry.metadata)
    if entry.artifact_ref != entry.content_hash and kind != "S5StructuralStageEvidenceV1":
        raise ValueError("strategy evidence reference/content mismatch")
    if kind in {"NativeStrategyHistoryBarV1", "S7DerivedM5BarV1"}:
        bar = metadata.get("bar")
        if (not isinstance(bar, Mapping) or sha256_json(bar) != entry.content_hash
                or bar.get("final") is not True or type(bar.get("close_at_ns")) is not int):
            raise ValueError("strategy bar is not an exact final history bar")
        if kind == "NativeStrategyHistoryBarV1":
            if set(metadata) != {"bar", "raw", "key", "input_refs", "information_cutoff_ns", "authority"}:
                raise ValueError("native history fields mismatch")
            raw = RawObservationV2.from_dict(metadata["raw"])
            key = InstrumentKeyV2.from_dict(metadata["key"])
            cutoff = metadata["information_cutoff_ns"]
            refs = metadata["input_refs"]
            if (metadata["authority"] != "ZERO" or len(refs) != 1 or bar["record_id"] != raw.record_id
                    or bar["raw_payload_hash"] != raw.raw_payload_hash
                    or bar["instrument_revision"] != key.contract_revision
                    or raw.instrument_revision != key.contract_revision
                    or not bar["close_at_ns"] <= raw.available_at_ns <= cutoff <= entry.available_at_ns
                    or not _source_evidence_available(repository, refs[0], cutoff)):
                raise ValueError("native history causal identity mismatch")
            source = repository.get_artifact(refs[0])
            if (source is None or source.content_hash != raw.content_hash
                    or source.metadata.get("bar_content_hash") != entry.content_hash):
                raise ValueError("native history conflicts with archived bar index")
        else:
            if set(metadata) != {"bar", "aggregation_receipt_ref", "input_refs"}:
                raise ValueError("derived M5 fields mismatch")
            receipt = repository.get_artifact(metadata["aggregation_receipt_ref"])
            if receipt is None or receipt.artifact_type != "S7M5AggregationReceiptV1":
                raise ValueError("derived M5 aggregation receipt missing")
            receipt_body = _exact_body(receipt, "receipt")
            cutoff = receipt_body["information_cutoff_ns"]
            if (receipt_body["bar_ref"] != entry.artifact_ref or bar["interval"] != "5M"
                    or receipt_body["input_refs"] != metadata["input_refs"]
                    or receipt.available_at_ns != entry.available_at_ns):
                raise ValueError("derived M5 aggregation binding mismatch")
            _strategy_evidence(repository, receipt)
        if not causal_artifact(repository, entry.artifact_ref, cutoff_ns=cutoff,
                               consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns):
            raise ValueError("strategy history chronology receipt unavailable")
    elif kind == "S7M5AggregationReceiptV1":
        body = _exact_body(entry, "receipt")
        if (set(body) != {"version", "bar_ref", "key", "information_cutoff_ns", "available_at_ns", "input_refs", "authority"}
                or body["version"] != "S7_M1_TO_M5_AGGREGATION_RECEIPT_V1"
                or body["available_at_ns"] != entry.available_at_ns or body["authority"] != "ZERO"
                or len(body["input_refs"]) != 5
                or body["input_refs"] != sorted(set(body["input_refs"]))
                or any(not _source_evidence_available(repository, ref, body["information_cutoff_ns"])
                       for ref in body["input_refs"])):
            raise ValueError("S7 exact M1 aggregation sources mismatch")
        InstrumentKeyV2.from_dict(body["key"])
    elif kind.startswith("S7Derived"):
        body = metadata.get("evidence")
        if (set(metadata) != {"evidence", "input_refs"} or not isinstance(body, Mapping)
                or set(body) != {"market_information_cutoff_ns", "available_at_ns", "input_refs", "payload", "authority"}
                or sha256_json({"artifact_type": kind, "evidence": body}) != entry.content_hash
                or body["available_at_ns"] != entry.available_at_ns or body["authority"] != "ZERO"
                or metadata["input_refs"] != body["input_refs"] or not 1 <= len(body["input_refs"]) <= 512
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=body["market_information_cutoff_ns"], consumer_at_ns=entry.available_at_ns,
                    deadline_ns=entry.available_at_ns)):
            raise ValueError("S7 numerical evidence chronology/identity mismatch")
    elif kind == "S8PairOwnerCatalogV1":
        body = _exact_body(entry, "catalog")
        if (set(body) != {"version", "owner_id", "available_at_ns", "pairs", "authority", "capital_authority"}
                or body["version"] != "S8_PAIR_OWNER_CATALOG_V1"
                or body["available_at_ns"] != entry.available_at_ns or not isinstance(body["owner_id"], str)
                or not body["owner_id"].strip() or not 1 <= len(body["pairs"]) <= 64
                or body["authority"] != "RESEARCH_CONFIGURATION_ONLY" or body["capital_authority"] != "ZERO"):
            raise ValueError("owner pair catalog identity/authority mismatch")
    elif kind == "S8PairDefinitionV2":
        from ..strategies.s8_pairs import S8PairDefinitionV2

        if set(metadata) != {"pair", "owner_catalog_ref"}:
            raise ValueError("owner pair fields mismatch")
        body = metadata["pair"]
        pair = S8PairDefinitionV2(body["pair_id"], body["economic_pair_definition"],
            InstrumentKeyV2.from_dict(body["key_a"]), InstrumentKeyV2.from_dict(body["key_b"]),
            body["hedge_fit"], body["residual_definition"], body["version"])
        catalog = repository.get_artifact(metadata["owner_catalog_ref"])
        if (pair.content_hash != entry.content_hash or catalog is None
                or catalog.available_at_ns > entry.available_at_ns or catalog.artifact_type != "S8PairOwnerCatalogV1"):
            raise ValueError("owner pair catalog lineage mismatch")
        _strategy_evidence(repository, catalog)
        if pair.to_dict() not in catalog.metadata["catalog"]["pairs"]:
            raise ValueError("owner pair absent from exact catalog")
    elif kind in {"S4ExpectedResponseBaselineV2", "S4AbsorptionHypothesisV2",
                  "S5StructuralStageEvidenceV1", "S5PreparedDiagnosticsV1",
                  "FundingObservationV2", "OpenInterestObservationV2", "OIChangeEvidenceV2"}:
        from ..chronology import VERSION, chronology_ref

        refs = metadata.get("input_refs")
        receipt = repository.get_artifact(chronology_ref(entry.artifact_ref))
        chronology = receipt.metadata.get("chronology") if receipt is not None else None
        legacy_baseline = kind == "S4ExpectedResponseBaselineV2" and set(metadata) == {"baseline", "input_refs"}
        if ((metadata.get("authority") != "ZERO" and not legacy_baseline) or not isinstance(refs, (list, tuple))
                or receipt is None or receipt.artifact_type != VERSION or not isinstance(chronology, Mapping)
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=chronology["market_information_cutoff_ns"],
                    consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns)):
            raise ValueError("typed research derivation lacks exact chronology")
        field = {"S4ExpectedResponseBaselineV2": "baseline", "S4AbsorptionHypothesisV2": "artifact",
            "S5StructuralStageEvidenceV1": "stage", "S5PreparedDiagnosticsV1": "diagnostics",
            "FundingObservationV2": "observation", "OpenInterestObservationV2": "observation",
            "OIChangeEvidenceV2": "evidence"}[kind]
        body = metadata.get(field)
        if not isinstance(body, Mapping):
            raise ValueError("typed research body unavailable")
        expected_fields = ({"policy", "key", "break_at_ns", "stage", "input_refs", "authority"}
            if kind == "S5StructuralStageEvidenceV1" else {field, "input_refs", "authority"})
        if set(metadata) != expected_fields and not legacy_baseline:
            raise ValueError("typed research fields mismatch")
        if kind == "S5StructuralStageEvidenceV1":
            from ..runtime.full_strategy_surface import S5_STRUCTURAL_POLICY

            valid = (sha256_json(metadata) == entry.content_hash
                and metadata["policy"] == S5_STRUCTURAL_POLICY
                and set(body) == {"stage", "event_at_ns", "state"}
                and entry.artifact_ref == sha256_json({"policy": metadata["policy"], "key": metadata["key"],
                    "break_at_ns": metadata["break_at_ns"], "stage": body["stage"],
                    "event_at_ns": body["event_at_ns"]}))
        elif kind in {"FundingObservationV2", "OpenInterestObservationV2"}:
            valid = sha256_json({"artifact_type": kind, "observation": body}) == entry.content_hash
        elif kind in {"S4AbsorptionHypothesisV2", "OIChangeEvidenceV2"}:
            valid = sha256_json({"artifact_type": kind, "artifact": body}) == entry.content_hash
        elif kind == "S4ExpectedResponseBaselineV2":
            valid = sha256_json({"artifact_type": kind, "baseline": body}) == entry.content_hash
        else:
            valid = sha256_json(body) == entry.content_hash
        if not valid:
            raise ValueError("typed research body hash mismatch")
        if kind in {"S4ExpectedResponseBaselineV2", "S5PreparedDiagnosticsV1"}:
            declared_refs = body["input_refs"]
        elif kind in {"FundingObservationV2", "OpenInterestObservationV2"}:
            declared_refs = sorted({body["raw_content_ref"],
                *([body["source_health_ref"]] if body["source_health_ref"] else [])})
        elif kind == "OIChangeEvidenceV2":
            declared_refs = sorted({body["start_ref"], body["end_ref"], *body["source_health_refs"]})
        elif kind == "S4AbsorptionHypothesisV2":
            declared_refs = sorted({body["feature_ref"], *body["evidence_refs"]})
        else:
            declared_refs = refs
        if (list(refs) != list(declared_refs)
                or any(not causal_artifact(repository, ref,
                    cutoff_ns=chronology["market_information_cutoff_ns"],
                    consumer_at_ns=chronology["computation_started_ns"], deadline_ns=entry.available_at_ns)
                    for ref in refs)):
            raise ValueError("typed research declared source chronology mismatch")
        if kind == "S5PreparedDiagnosticsV1" and (
                body.get("authority") != "ZERO" or body.get("capital_authority") != "ZERO"
                or body.get("candidate_action_refs") != [] or body.get("delivered_at_ns") != entry.available_at_ns
                or body.get("information_cutoff_ns") != chronology["market_information_cutoff_ns"]):
            raise ValueError("prepared research authority or publication mismatch")
        _refs(repository, refs, available_at_ns=entry.available_at_ns)
    else:
        field = {"S4FeatureArtifactV2": "feature", "S5CrowdingContextV2": "context",
                 "S6LiquidityFundingEvidenceV2": "evidence"}[kind]
        if kind == "S4FeatureArtifactV2":
            from ..data.microstructure import S4FeatureArtifactV2

            if set(metadata) not in ({"feature"}, {"feature", "instrument_key_json", "input_refs"},
                                    {"feature", "instrument_key_json", "input_refs", "authority"}):
                raise ValueError("S4 feature fields mismatch")
            feature = S4FeatureArtifactV2.from_dict(metadata["feature"])
            body = feature.to_dict()
            if (metadata["feature"] != body or feature.content_hash != entry.content_hash
                    or ("instrument_key_json" in metadata
                        and metadata["instrument_key_json"] != feature.instrument.to_canonical_json())
                    or ("input_refs" in metadata and metadata["input_refs"] != body["input_refs"])
                    or ("authority" in metadata and metadata["authority"] != "ZERO")
                    or not causal_artifact(repository, entry.artifact_ref, cutoff_ns=feature.cutoff_ns,
                        consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns)):
                raise ValueError("S4 feature instrument/source identity mismatch")
        elif kind == "S5CrowdingContextV2":
            body = metadata.get("context")
            if (not isinstance(body, Mapping) or set(metadata) - {"context", "input_refs"}
                    or sha256_json({"artifact_type": kind, "artifact": body}) != entry.content_hash
                    or ("input_refs" in metadata and metadata["input_refs"] != body["input_refs"])):
                raise ValueError("S5 context identity mismatch")
        else:
            body = _exact_body(entry, field)
        if kind == "S5CrowdingContextV2":
            refs, cutoff = body["input_refs"], body["cutoff_ns"]
        elif kind == "S6LiquidityFundingEvidenceV2":
            refs = [body["liquidity_ref"], body["funding_ref"], *([body["turnover_ref"]] if body.get("turnover_ref") else [])]
            cutoff = body["available_at_ns"]
        else:
            refs, cutoff = body["input_refs"], body["cutoff_ns"]
        if kind == "S5CrowdingContextV2":
            valid_sources = causal_artifact(repository, entry.artifact_ref, cutoff_ns=cutoff,
                consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns) and all(
                    causal_artifact(repository, ref, cutoff_ns=cutoff,
                        consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns) for ref in refs)
        else:
            valid_sources = all(_source_evidence_available(repository, ref, cutoff) for ref in refs)
        if cutoff > entry.available_at_ns or not valid_sources:
            raise ValueError("strategy context lacks exact cutoff source evidence")
    _safe_public_payload(metadata)
    return metadata


def project_broad_evidence(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> dict[str, Any]:
    """Return full public evidence only after strict type/lineage validation.

    Complete rejected/unselected universe and CandidateSet members are retained
    in ``evidence_payload_json``. No full raw provider packet is exported.
    """
    result: dict[str, Any] = {"row_kind": "BROAD_RESEARCH"}
    if entry.artifact_type == "UniverseContractV2":
        universe = _universe(repository, entry)
        body = universe.to_dict()
        result.update(row_kind="UNIVERSE", decision_at_ns=universe.decision_slot_ns,
                      information_cutoff_ns=universe.decision_slot_ns,
                      policy_hash=universe.selection_policy_hash)
    elif entry.artifact_type == "CandidateSetV2":
        from .outcomes import _resolve_candidate_set

        candidates, identity = _resolve_candidate_set(repository, entry.artifact_ref)
        body = {"candidate_set": candidates.to_dict(), "identity": json_value(identity)}
        if len(candidates.candidates) > 16384:
            raise ValueError("CandidateSet export population overflow")
        result.update(row_kind="CANDIDATE_SET", candidate_set_ref=entry.artifact_ref,
                      event_id=candidates.decision_event_id,
                      information_cutoff_ns=identity["cutoff_ns"], policy_hash=candidates.selection_policy_hash)
    elif entry.artifact_type == "BroadUniverseWorksetV2":
        body = _exact_body(entry, "workset")
        required = {"version", "available_at_ns", "universe_ref", "product_refs", "active_product_refs",
                    "tiers", "exploration", "coverage", "cheap_quotes", "selected_count",
                    "unselected_observed_count", "source_ref", "prior_workset_ref", "capital_enabled", "authority"}
        optional_fields = {"source_cutoff_ns", "research_cohort_venue", "research_cohort_keys"}
        if (not required.issubset(body) or set(body) - required - optional_fields or body["version"] != entry.artifact_type
                or body["available_at_ns"] != entry.available_at_ns or body["capital_enabled"] is not False
                or type(body.get("source_cutoff_ns", entry.available_at_ns)) is not int
                or not 0 <= body.get("source_cutoff_ns", entry.available_at_ns) <= entry.available_at_ns
                or body["authority"] != "ZERO" or len(body["active_product_refs"]) > 24
                or len(body["active_product_refs"]) != len(set(body["active_product_refs"]))
                or body["selected_count"] != len(body["active_product_refs"])
                or body["unselected_observed_count"] != len(body["product_refs"]) - body["selected_count"]
                or not set(body["active_product_refs"]).issubset(body["product_refs"])):
            raise ValueError("broad workset resource or authority contract mismatch")
        indexed_universe = repository.get_artifact(body["universe_ref"])
        if indexed_universe is None or indexed_universe.available_at_ns > entry.available_at_ns:
            raise ValueError("workset universe unavailable")
        universe = _universe(repository, indexed_universe)
        expected_product_refs = tuple(sorted({item.product_ref for item in universe.entries}))
        if tuple(body["product_refs"]) != expected_product_refs:
            raise ValueError("workset product population differs from validated universe")
        keys = {item.key.to_canonical_json() for item in universe.entries}
        if "research_cohort_keys" in body:
            cohort = body["research_cohort_keys"]
            active_keys = {item.key.to_canonical_json() for item in universe.entries
                           if item.product_ref in body["active_product_refs"]}
            if (len(cohort) > 20 or len(set(cohort)) != len(cohort) or not set(cohort).issubset(active_keys)
                    or any(json_value(item.key.to_dict())["venue"] != body["research_cohort_venue"]
                           for item in universe.entries if item.key.to_canonical_json() in cohort)):
                raise ValueError("broad same-venue cohort identity mismatch")
        if (set(body["tiers"]) != keys or set(body["coverage"]) != keys
                or set(body["cheap_quotes"]) != keys or any(type(tier) is not int or not 0 <= tier <= 4
                                                          for tier in body["tiers"].values())):
            raise ValueError("workset omitted observed instruments")
        for frames in body["coverage"].values():
            if not isinstance(frames, Mapping) or set(frames) - {"M1", "M15", "H1", "H4"}:
                raise ValueError("workset history interval unknown")
            for ranges in frames.values():
                if len(ranges) > 32 or any(len(pair) != 2 or any(type(t) is not int for t in pair)
                                          or pair[0] > pair[1] for pair in ranges):
                    raise ValueError("workset history coverage invalid")
        source_refs = [body["source_ref"], *([body["prior_workset_ref"]] if body["prior_workset_ref"] else [])]
        _refs(repository, sorted(set(source_refs)), available_at_ns=entry.available_at_ns, maximum=2)
    elif entry.artifact_type == "S8ResidualDiagnosticsV1":
        from ..chronology import causal_artifact

        body = _exact_body(entry, "diagnostics")
        expected = {"version", "pair_ref", "forecast_ref", "cutoff_ns", "published_at_ns",
                    "synchronized_price_refs", "half_life", "stationarity", "authority",
                    "capital_authority", "selector_influence"}
        if (set(body) != expected or body["version"] != "S8ResidualDiagnosticsV1"
                or body["authority"] != "ZERO" or body["capital_authority"] != "ZERO"
                or body["selector_influence"] != "ZERO"
                or body["published_at_ns"] != entry.available_at_ns
                or entry.created_at_ns != entry.available_at_ns
                or type(body["cutoff_ns"]) is not int
                or body["published_at_ns"] < body["cutoff_ns"]):
            raise ValueError("S8 residual diagnostic schema or authority mismatch")
        forecast = repository.get_artifact(body["forecast_ref"])
        from ..runtime.research_basket_outcomes import _forecast_from_entry

        forecast_body = forecast.metadata.get("basket") if forecast is not None else None
        if not isinstance(forecast_body, Mapping):
            raise ValueError("S8 residual diagnostic forecast body is unavailable")
        parsed_forecast = _forecast_from_entry(forecast_body)
        pair_entry = repository.get_artifact(body["pair_ref"])
        pair_body = pair_entry.metadata.get("pair") if pair_entry is not None else None
        if (pair_entry is None or pair_entry.artifact_type != "S8PairDefinitionV2"
                or not isinstance(pair_body, Mapping) or sha256_json(pair_body) != body["pair_ref"]):
            raise ValueError("S8 residual diagnostic pair definition is missing or malformed")
        synchronized_refs = body["synchronized_price_refs"]
        if (not isinstance(synchronized_refs, list) or len(synchronized_refs) != 721
                or any(not isinstance(pair, list) or len(pair) != 2
                       or any(not isinstance(ref, str) for ref in pair) for pair in synchronized_refs)
                or tuple(tuple(pair) for pair in synchronized_refs) != parsed_forecast.synchronized_price_refs):
            raise ValueError("S8 residual diagnostic synchronized price lineage mismatch")
        _refs(repository, sorted({body["pair_ref"], body["forecast_ref"],
                                  *(ref for pair in synchronized_refs for ref in pair)}),
              available_at_ns=entry.available_at_ns, maximum=1444)
        if (forecast is None or forecast.artifact_type != "ResearchBasketForecastV2"
                or forecast.available_at_ns > entry.available_at_ns
                or forecast.metadata.get("basket", {}).get("pair_definition_ref") != body["pair_ref"]
                or forecast.metadata.get("basket", {}).get("information_cutoff_ns") != body["cutoff_ns"]
                or parsed_forecast.content_hash != body["forecast_ref"]
                or canonical_json(parsed_forecast.to_dict()) != canonical_json(forecast_body)
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=body["cutoff_ns"], consumer_at_ns=entry.available_at_ns,
                    deadline_ns=entry.available_at_ns)):
            raise ValueError("S8 residual diagnostic forecast lineage mismatch")
        result.update(information_cutoff_ns=body["cutoff_ns"], status="ZERO_AUTHORITY_RESEARCH")
    elif entry.artifact_type == "TradeLocationInputReceiptV1":
        from ..chronology import causal_artifact
        from ..instruments import InstrumentKeyV2
        from ..runtime.trade_location_inputs import MAX_ARCHIVED_TRADE_ROWS, TradeLocationInputReceiptV1

        if set(entry.metadata) != {"receipt", "input_refs"} or not isinstance(entry.metadata["receipt"], Mapping):
            raise ValueError("trade-location receipt requires its exact body and dependency vector")
        body = json_value(entry.metadata["receipt"])
        TradeLocationInputReceiptV1.from_dict(body, artifact_ref=entry.content_hash)
        if (entry.artifact_ref != entry.content_hash
                or body["created_at_ns"] != entry.created_at_ns
                or body["available_at_ns"] != entry.available_at_ns
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=body["information_cutoff_ns"], consumer_at_ns=entry.available_at_ns,
                    deadline_ns=entry.available_at_ns)):
            raise ValueError("trade-location receipt identity or chronology mismatch")
        key = InstrumentKeyV2.from_dict(body["key"])
        config = body["configuration"]
        config_refs = set()
        if isinstance(config, Mapping):
            anchor = config.get("anchor")
            if isinstance(anchor, Mapping) and isinstance(anchor.get("ref"), str):
                config_refs.add(anchor["ref"])
            product_ref = config.get("product_ref")
            if isinstance(product_ref, str):
                config_refs.add(product_ref)
        expected_inputs = tuple(sorted(set(body["source_refs"]) | config_refs))
        declared_inputs = entry.metadata["input_refs"]
        if tuple(declared_inputs) != expected_inputs:
            raise ValueError("trade-location receipt dependency vector differs from its exact body")
        indexed = _refs(repository, expected_inputs, available_at_ns=body["information_cutoff_ns"],
                        maximum=MAX_ARCHIVED_TRADE_ROWS + 8)
        expected_events = {"TRADE"} if key.venue.value == "BYBIT" else {"AGG_TRADE"}
        for ref in body["observed_trade_refs"]:
            item = indexed[ref]
            metadata = item["metadata"]
            try:
                record_id = metadata["record_id"]
                sha256_ref(metadata["raw_payload_hash"], field="raw_payload_hash")
                expected_ref = sha256_json({"artifact_type": item["artifact_type"], "record_id": record_id})
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("trade-location observation identity is malformed") from error
            if (not isinstance(ref, str)
                    or item["artifact_type"] not in {"PublicObservationIndexV2", "PublicStreamTradeObservationIndexV1"}
                    or ref != expected_ref
                    or not isinstance(record_id, str)
                    or metadata.get("instrument_key_json") != key.to_canonical_json()
                    or metadata.get("event_type") not in expected_events
                    or metadata.get("availability_class") != "ACTUAL_SYSTEM"
                    or type(metadata.get("event_at_ns")) is not int
                    or metadata["event_at_ns"] > body["information_cutoff_ns"]
                    or not isinstance(metadata.get("raw_payload_hash"), str)):
                raise ValueError("trade-location observation identity or cutoff is invalid")
        if body["coverage_state"] == "VERIFIED":
            window = body["completeness_window"]
            _refs(repository, sorted({body["completeness_evidence_ref"], window["source_contract_ref"]}),
                  available_at_ns=body["information_cutoff_ns"], maximum=2)
        result.update(information_cutoff_ns=body["information_cutoff_ns"],
                      coverage_state=body["coverage_state"], status=body["reason"])
    elif entry.artifact_type == "S8BasketOutcomeEvidenceV1":
        from ..chronology import causal_artifact
        from ..runtime.research_basket_outcomes import (
            _evidence_from_dict,
            _forecast_from_entry,
            _make_outcome,
            _outcome_evidence_input_refs,
        )

        if set(entry.metadata) != {"evidence", "input_refs"} or not isinstance(entry.metadata["evidence"], Mapping):
            raise ValueError("S8 outcome evidence requires its exact wrapper and dependency vector")
        body = json_value(entry.metadata["evidence"])
        evidence = _evidence_from_dict(body)
        if (entry.artifact_ref != entry.content_hash or evidence.content_hash != entry.content_hash
                or entry.created_at_ns != entry.available_at_ns
                or entry.available_at_ns < evidence.available_at_ns):
            raise ValueError("S8 outcome evidence identity or receipt chronology mismatch")
        forecast_entry = repository.get_artifact(evidence.forecast_ref)
        forecast_body = forecast_entry.metadata.get("basket") if forecast_entry is not None else None
        if (forecast_entry is None or forecast_entry.artifact_type != "ResearchBasketForecastV2"
                or not isinstance(forecast_body, Mapping)):
            raise ValueError("S8 outcome evidence forecast is unavailable")
        evidence_forecast = _forecast_from_entry(forecast_body)
        if evidence_forecast.content_hash != evidence.forecast_ref:
            raise ValueError("S8 outcome evidence forecast hash mismatch")
        input_refs = entry.metadata["input_refs"]
        expected_input_refs = _outcome_evidence_input_refs(repository, evidence)
        if tuple(input_refs) != expected_input_refs:
            raise ValueError("S8 outcome evidence source vector mismatch")
        _refs(repository, input_refs, available_at_ns=evidence.available_at_ns, maximum=256)
        _make_outcome(repository, evidence_forecast, evidence.forecast_ref, evidence,
            evidence.available_at_ns, lambda: entry.available_at_ns)
        if not causal_artifact(repository, entry.artifact_ref,
                cutoff_ns=evidence.available_at_ns, consumer_at_ns=entry.available_at_ns,
                deadline_ns=entry.available_at_ns):
            raise ValueError("S8 outcome evidence lacks source chronology")
        result.update(information_cutoff_ns=evidence.available_at_ns, status="MATURED_RESEARCH_EVIDENCE")
    elif entry.artifact_type == "S8BasketOutcomeV1":
        from ..chronology import causal_artifact
        from ..runtime.research_basket_outcomes import _evidence_from_dict, _forecast_from_entry, _make_outcome

        body = _exact_body(entry, "outcome")
        required = {"version", "forecast_ref", "pair_ref", "decision_at_ns", "maturity_at_ns",
                    "evidence_cutoff_ns",
                    "outcome_status", "economic_status", "reason_codes", "evidence_ref",
                    "price_path_a", "price_path_b", "simulation", "source_refs", "available_at_ns",
                    "capital_authority", "single_action", "trade_plan_allowed"}
        if (set(body) != required or body["version"] != "S8BasketOutcomeV1"
                or body["capital_authority"] != "ZERO" or body["single_action"] is not False
                or body["trade_plan_allowed"] is not False or body["economic_status"] != "NOT_ESTIMABLE"
                or body["outcome_status"] not in {"NOT_ESTIMABLE", "NO_ENTRY", "OBSERVED_RESEARCH_PATH"}
                or type(body["decision_at_ns"]) is not int or type(body["maturity_at_ns"]) is not int
                or type(body["evidence_cutoff_ns"]) is not int
                or type(body["available_at_ns"]) is not int or body["available_at_ns"] != entry.available_at_ns
                or entry.created_at_ns != entry.available_at_ns
                or not body["decision_at_ns"] < body["maturity_at_ns"] <= body["evidence_cutoff_ns"]
                or body["evidence_cutoff_ns"] > body["available_at_ns"]
                or not isinstance(body["reason_codes"], list)
                or body["reason_codes"] != sorted(set(body["reason_codes"]))
                or not isinstance(body["price_path_a"], list) or not isinstance(body["price_path_b"], list)
                or len(body["price_path_a"]) > 5 or len(body["price_path_b"]) > 5):
            raise ValueError("S8 outcome schema, maturity, or authority mismatch")
        forecast = repository.get_artifact(body["forecast_ref"])
        if (forecast is None or forecast.artifact_type != "ResearchBasketForecastV2"
                or forecast.available_at_ns > entry.available_at_ns
                or forecast.metadata.get("basket", {}).get("pair_definition_ref") != body["pair_ref"]
                or forecast.metadata.get("basket", {}).get("decision_at_ns") != body["decision_at_ns"]
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=body["evidence_cutoff_ns"], consumer_at_ns=entry.available_at_ns,
                    deadline_ns=entry.available_at_ns)):
            raise ValueError("S8 outcome forecast lineage or chronology mismatch")
        forecast_body = forecast.metadata.get("basket")
        if not isinstance(forecast_body, Mapping):
            raise ValueError("S8 outcome forecast body is unavailable")
        parsed_forecast = _forecast_from_entry(forecast_body)
        if (parsed_forecast.content_hash != body["forecast_ref"]
                or canonical_json(parsed_forecast.to_dict()) != canonical_json(forecast_body)):
            raise ValueError("S8 outcome forecast content hash mismatch")
        source_refs = body["source_refs"]
        _refs(repository, source_refs, available_at_ns=entry.available_at_ns, maximum=64)
        evidence = None
        if body["evidence_ref"] is not None:
            sha256_ref(body["evidence_ref"], field="evidence_ref")
            if body["evidence_ref"] not in source_refs:
                raise ValueError("S8 outcome evidence ref is absent from complete source refs")
            evidence_entry = repository.get_artifact(body["evidence_ref"])
            if (evidence_entry is None or evidence_entry.artifact_type != "S8BasketOutcomeEvidenceV1"
                    or evidence_entry.available_at_ns > body["available_at_ns"]
                    or evidence_entry.metadata.get("evidence", {}).get("available_at_ns")
                    != body["evidence_cutoff_ns"]
                    or evidence_entry.metadata.get("evidence", {}).get("forecast_ref") != body["forecast_ref"]):
                raise ValueError("S8 outcome aggregate evidence artifact is missing or mismatched")
            project_broad_evidence(repository, evidence_entry)
            evidence_body = evidence_entry.metadata.get("evidence")
            if not isinstance(evidence_body, Mapping):
                raise ValueError("S8 outcome evidence artifact body is unavailable")
            evidence = _evidence_from_dict(json_value(evidence_body))
        if body["simulation"] is not None and not isinstance(body["simulation"], Mapping):
            raise ValueError("S8 outcome simulation body is malformed")
        replayed = _make_outcome(
            repository,
            parsed_forecast,
            body["forecast_ref"],
            evidence,
            body["evidence_cutoff_ns"],
            lambda: body["available_at_ns"],
        )
        if canonical_json(replayed.to_dict()) != canonical_json(body):
            raise ValueError("S8 outcome does not match deterministic replay of its durable evidence")
        pair_keys = (parsed_forecast.leg_a_evidence.instrument_key.to_dict(),
                     parsed_forecast.leg_b_evidence.instrument_key.to_dict())
        for path, expected_key in zip((body["price_path_a"], body["price_path_b"]), pair_keys, strict=True):
            prior_hour = body["decision_at_ns"] - 1
            for row in path:
                if (not isinstance(row, Mapping)
                        or set(row) != {"instrument_key", "hour_end_ns", "available_at_ns", "close", "source_ref"}
                        or row["instrument_key"] != expected_key
                        or type(row["hour_end_ns"]) is not int or type(row["available_at_ns"]) is not int
                        or row["hour_end_ns"] <= prior_hour or row["hour_end_ns"] > body["maturity_at_ns"]
                        or row["available_at_ns"] < row["hour_end_ns"]
                        or row["available_at_ns"] > entry.available_at_ns
                        or not isinstance(row["close"], str) or not row["close"]
                        or row["source_ref"] not in source_refs):
                    raise ValueError("S8 outcome hourly price path identity is invalid")
                prior_hour = row["hour_end_ns"]
        result.update(information_cutoff_ns=body["evidence_cutoff_ns"], status=body["outcome_status"])
    elif entry.artifact_type == "ResearchBasketForecastV2":
        from ..chronology import causal_artifact
        from ..runtime.full_strategy_surface import _s8_source_visible
        from ..runtime.research_basket_outcomes import _forecast_from_entry

        body = _exact_body(entry, "basket")
        basket = _forecast_from_entry(body)
        if (canonical_json(basket.to_dict()) != canonical_json(body)
                or basket.content_hash != entry.content_hash
                or basket.information_cutoff_ns > entry.available_at_ns
                or not causal_artifact(repository, entry.artifact_ref,
                    cutoff_ns=basket.information_cutoff_ns, consumer_at_ns=entry.available_at_ns,
                    deadline_ns=entry.available_at_ns)):
            raise ValueError("S8 forecast exact profile or chronology mismatch")
        refs = {basket.pair_definition_ref,
                *(ref for pair in basket.synchronized_price_refs for ref in pair)}
        for leg in (basket.leg_a_evidence, basket.leg_b_evidence):
            refs.update((*leg.price_refs, *leg.book_execution_refs, *leg.funding_refs))
            if leg.fee_ref is not None:
                refs.add(leg.fee_ref)
        if len(refs) > 4096 or any(not _s8_source_visible(repository, ref,
                basket.information_cutoff_ns, entry.available_at_ns) for ref in refs):
            raise ValueError("S8 forecast source identity or causal availability mismatch")
        result.update(information_cutoff_ns=basket.information_cutoff_ns,
                      decision_at_ns=basket.decision_at_ns, status=basket.economic_status)
    elif entry.artifact_type == SURFACE_TYPE:
        from ..chronology import causal_artifact, chronology_ref

        body = _exact_body(entry, "surface")
        if (set(body) != {"version", "cutoff_ns", "published_at_ns", "universe_ref", "role_status",
                         "role_refs", "missing_reasons", "authority", "capital_authority",
                         "candidate_action_refs", "trade_plan_allowed", "original_universe_receipt_ref",
                         "original_decision_deadline_ns", "selector_influence"}
                or body["version"] != SURFACE_TYPE or body["published_at_ns"] != entry.available_at_ns
                or not 0 <= body["cutoff_ns"] <= body["published_at_ns"]
                or body["authority"] != "ZERO" or body["capital_authority"] != "ZERO"
                or body["selector_influence"] != "ZERO"
                or body["candidate_action_refs"] != []
                or body["trade_plan_allowed"] is not False
                or set(body["role_status"]) != set(body["role_refs"])
                or set(body["role_refs"]) != set(body["missing_reasons"])):
            raise ValueError("strategy surface identity or authority mismatch")
        if set(body["role_refs"]) != set(ROLE_NAMES[4:]):
            raise ValueError("strategy surface requires every research role")
        surface_refs = sorted({body["universe_ref"], *(ref for refs in body["role_refs"].values() for ref in refs)})
        _refs(repository, surface_refs, available_at_ns=entry.available_at_ns, maximum=4096)
        universe_entry = repository.get_artifact(body["universe_ref"])
        if universe_entry is None or universe_entry.artifact_type != "UniverseContractV2":
            raise ValueError("strategy surface universe is mistyped")
        universe = _universe(repository, universe_entry)
        expected_receipt = (chronology_ref(universe.content_hash)
                            if universe.envelope.available_at_ns > body["cutoff_ns"] else None)
        if (body["original_decision_deadline_ns"] != universe.decision_slot_ns
                or body["original_universe_receipt_ref"] != expected_receipt
                or body["cutoff_ns"] > universe.decision_slot_ns
                or not causal_artifact(repository, universe.content_hash, cutoff_ns=body["cutoff_ns"],
                    consumer_at_ns=entry.available_at_ns, deadline_ns=universe.decision_slot_ns)):
            raise ValueError("strategy surface original universe chronology mismatch")
        for role, role_refs in body["role_refs"].items():
            if len(role_refs) != 1:
                raise ValueError("strategy surface role publication mismatch")
            role_entry = repository.get_artifact(role_refs[0])
            if role_entry is None or role_entry.artifact_type != f"{role}ResearchRoleV1":
                raise ValueError("strategy surface role is mistyped")
            project_broad_evidence(repository, role_entry)
            role_body = json_value(role_entry.metadata["role"])
            if (role_body["status"] != body["role_status"][role]
                    or role_body["missing_reasons"] != body["missing_reasons"][role]
                    or role_body["available_at_ns"] != body["published_at_ns"]
                    or role_body["information_cutoff_ns"] != body["cutoff_ns"]):
                raise ValueError("strategy surface role binding mismatch")
        result["information_cutoff_ns"] = body["cutoff_ns"]
    elif entry.artifact_type in (*CALENDAR_EXPORT_TYPES, *CALENDAR_PROFILE_PROOF_EXPORT_TYPES):
        body = _calendar_evidence(repository, entry)
    elif entry.artifact_type.endswith("ResearchRoleV1"):
        from ..chronology import causal_artifact

        body = _exact_body(entry, "role")
        role = body.get("role")
        if (role not in ROLE_NAMES or entry.artifact_type != f"{role}ResearchRoleV1"
                or set(body) != {"version", "role", "status", "information_cutoff_ns", "available_at_ns",
                                 "input_refs", "missing_reasons", "payload", "authority",
                                 "candidate_action_ref", "trade_plan_allowed"}
                or body["version"] != f"{role}_RESEARCH_ROLE_V1"
                or body["available_at_ns"] != entry.available_at_ns
                or not 0 <= body["information_cutoff_ns"] <= entry.available_at_ns
                or body["authority"] != "ZERO" or body["candidate_action_ref"] is not None
                or body["trade_plan_allowed"] is not False
                or body["status"] not in {"AVAILABLE", "NOT_ESTIMABLE"}):
            raise ValueError("research role contract or authority mismatch")
        _refs(repository, body["input_refs"], available_at_ns=entry.available_at_ns, maximum=4096)
        if not causal_artifact(repository, entry.artifact_ref, cutoff_ns=body["information_cutoff_ns"],
                               consumer_at_ns=entry.available_at_ns, deadline_ns=entry.available_at_ns):
            raise ValueError("research role lacks exact cutoff-sealed source chronology")
        result.update(status=body["status"], information_cutoff_ns=body["information_cutoff_ns"])
    elif entry.artifact_type in {"NativeStrategyHistoryBarV1", "S7M5AggregationReceiptV1", "S7DerivedM5BarV1",
            "S7DerivedPreEventBetaV1", "S7DerivedMarketReturn5MV1", "S7DerivedReactionSpreadV1",
            "S8PairOwnerCatalogV1", "S8PairDefinitionV2", "S4FeatureArtifactV2", "S5CrowdingContextV2",
            "S6LiquidityFundingEvidenceV2", "S4ExpectedResponseBaselineV2", "S4AbsorptionHypothesisV2",
            "S5StructuralStageEvidenceV1", "S5PreparedDiagnosticsV1", "FundingObservationV2",
            "OpenInterestObservationV2", "OIChangeEvidenceV2"}:
        body = _strategy_evidence(repository, entry)
    else:
        fields = {"BroadPublicAcquisitionReceiptV2": "receipt", "BroadPublicSchedulerStateV2": "scheduler",
                  "BroadPublicAcquisitionWaitV2": "wait", "BroadPublicDuplicateReceiptV2": "receipt",
                  "OpsDecisionSourceScopeV2": "source_scope", "BroadResearchDeferredV1": "deferred",
                  "BroadPublicAdoptionRejectionV1": "rejection"}
        body = _exact_body(entry, fields[entry.artifact_type])
        if entry.artifact_type == "BroadPublicSchedulerStateV2":
            if body.get("schema_version") not in (1, 2) or len(body.get("enabled_venues", ())) not in (1, 2):
                raise ValueError("broad scheduler contract mismatch")
        elif body.get("authority") != "ZERO" or body.get("capital_enabled", False) is not False:
            raise ValueError("broad receipt must have zero authority")
        for timestamp_name in ("available_at_ns", "observed_at_ns"):
            if timestamp_name in body and body[timestamp_name] != entry.available_at_ns:
                raise ValueError("broad receipt chronology mismatch")
        if entry.artifact_type == "BroadPublicAcquisitionReceiptV2":
            acquisition_refs = sorted({ref for values in body["source_observation_refs"].values() for ref in values})
            _refs(repository, acquisition_refs, available_at_ns=entry.available_at_ns)
    _safe_public_payload(body)
    encoded = canonical_json(body)
    if len(encoded.encode("utf-8")) > MAX_BROAD_METADATA_BYTES:
        raise ValueError("broad export evidence exceeds its fixed byte bound")
    result["evidence_payload_json"] = encoded
    return result
