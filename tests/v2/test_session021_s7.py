"""Session-021 deterministic news, safety-gate, alert and shadow tests."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import V2Side, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2, close_boundary_ns
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    DEFAULT_NEWS_SOURCES_V2,
    AbnormalityEvidenceV2,
    AbnormalityStateV2,
    AssetMappingStateV2,
    CalendarCoverageV2,
    EventGateStateV2,
    EventResolutionV2,
    EventSafetyGateBuilderV2,
    FetchedDocumentV2,
    IncidentStateV2,
    MarketReturn5MV2,
    NewsCollectionPipelineV2,
    NewsEventV2,
    NewsSourceClassV2,
    NewsSourceConfigV2,
    OperationalIncidentV2,
    PreEventBetaV2,
    ReactionSpreadEvidenceV2,
    S7DirectionalShadowV2,
    ScheduledEventV2,
    SourceQualificationV2,
    map_assets_v2,
    parse_feed_bytes_v2,
)

from .test_session014_core import KEY

NS = 1_000_000_000
H = "c" * 64
SOURCE_ID = "OFFICIAL_FIXTURE"
BAR_SOURCE = "VENUE_PUBLIC_FIXTURE"


def _index(repository: OpsRepository, ref: str, kind: str, at_ns: int, body: dict | None = None) -> None:
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at_ns, at_ns, body or {"ref": ref}))


class _QueueTransport:
    def __init__(self, rows: list[FetchedDocumentV2]) -> None:
        self.rows = rows

    def fetch(self, _source: NewsSourceConfigV2) -> FetchedDocumentV2:
        return self.rows.pop(0)


def _feed_doc(body: bytes, at: int, url: str = "https://news.example.org/feed") -> FetchedDocumentV2:
    return FetchedDocumentV2(url, url, 200, body, at, {})


def _coverage(repository: OpsRepository, at: int, *, source_id: str = "FEDERAL_RESERVE") -> CalendarCoverageV2:
    source_ref = sha256_json({"calendar-source": at, "source": source_id})
    _index(repository, source_ref, "CalendarSourceFixtureV2", at)
    return CalendarCoverageV2(source_id, 0, 10**20, at, at, at, True, "schedule-r1",
                              source_ref, "VERIFIED")


def _normal_abnormality(repository: OpsRepository, at: int,
                        state: AbnormalityStateV2 = AbnormalityStateV2.NORMAL) -> AbnormalityEvidenceV2:
    source_ref = sha256_json({"abnormality-source": at, "state": state.value})
    _index(repository, source_ref, "AbnormalitySourceFixtureV2", at)
    return AbnormalityEvidenceV2(state, at, at, source_ref)


def _macro_event(repository: OpsRepository, scheduled_at: int, available_at: int) -> ScheduledEventV2:
    evidence_ref = sha256_json({"macro-schedule": scheduled_at, "available": available_at})
    _index(repository, evidence_ref, "MacroScheduleFixtureV2", available_at)
    return ScheduledEventV2(
        sha256_json({"macro-event": scheduled_at}), "US_CPI", scheduled_at, "calendar-r1",
        "FEDERAL_RESERVE", available_at, available_at, available_at, evidence_ref,
    )


def _event(*, auth: str = "AUTHENTICATED", mapping: AssetMappingStateV2 = AssetMappingStateV2.UNAMBIGUOUS,
           assets: tuple[str, ...] = (KEY.base_asset_id,), source_health_ref: str | None = None,
           received: int = 20, extracted: int = 25) -> NewsEventV2:
    health_ref = source_health_ref or H
    event_id = sha256_json({"news-event": auth, "mapping": mapping.value, "assets": assets})
    raw_body = b"session021 deterministic raw event fixture"
    raw_digest = hashlib.sha256(raw_body).hexdigest()
    feed_url = "https://news.example.org/feed"
    raw_ref = sha256_json({"artifact_type": "NewsRawPayloadV2", "source_id": SOURCE_ID,
                           "source_url": feed_url, "raw_payload_hash": raw_digest})
    receipt = {"schema_version": 1, "raw_ref": raw_ref, "source_id": SOURCE_ID,
               "source_url": feed_url, "received_at_ns": received}
    receipt_ref = sha256_json(receipt)
    return NewsEventV2(
        event_id, SOURCE_ID, NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
        "https://news.example.org/article", sha256_json({"article": event_id}), 0,
        received, extracted, tuple(sorted(assets)), "SECURITY_INCIDENT", "HIGH", Decimal("0.95"),
        ("security incident",), sha256_json({"group": event_id}), raw_ref, receipt_ref, health_ref,
        auth, H if auth in ("AUTHENTICATED", "EXPLICITLY_RESOLVED") else None,
        mapping, None, None, source_health_state="HEALTHY_CURRENT",
    )


def _persist_news_event(repository: OpsRepository, event: NewsEventV2) -> None:
    raw_body = b"session021 deterministic raw event fixture"
    raw_digest = hashlib.sha256(raw_body).hexdigest()
    repository.register_artifact(ArtifactIndexEntryV2(
        event.raw_ref, "NewsRawPayloadV2", raw_digest, event.received_at_ns, event.received_at_ns,
        {"source_id": event.source_id, "source_url": "https://news.example.org/feed",
         "raw_payload_hash": raw_digest, "raw_bytes_base64": base64.b64encode(raw_body).decode("ascii")},
    ))
    repository.register_artifact(ArtifactIndexEntryV2(
        event.receipt_ref, "NewsReceiptV2", event.receipt_ref, event.received_at_ns, event.received_at_ns,
        {"raw_ref": event.raw_ref, "source_id": event.source_id,
         "source_url": "https://news.example.org/feed", "received_at_ns": event.received_at_ns},
    ))
    repository.register_artifact(ArtifactIndexEntryV2(
        event.event_id, "NewsEventV2", event.semantic_hash, event.received_at_ns,
        event.available_at_ns, {"event": event.to_dict()},
    ))


def _bar(key: InstrumentKeyV2, interval: BarIntervalV2, opened: int, open_px: str,
         close_px: str, volume: str, *, source: str = BAR_SOURCE) -> CausalBarV2:
    close_at = close_boundary_ns(opened, interval)
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision, source_id=source, event_type=f"BAR_{interval.value}",
        event_at_ns=close_at, received_at_ns=close_at, ingested_at_ns=close_at,
        available_at_ns=close_at, payload={"open": open_px, "close": close_px, "volume": volume},
        translation_version="s7-fixture", sequence=str(opened),
    )
    op, cl = Decimal(open_px), Decimal(close_px)
    return CausalBarV2(raw, interval, opened, close_at, op, max(op, cl), min(op, cl), cl,
                       Decimal(volume), True)


def test_news_receipt_availability_exact_dedupe_revision_conflict_and_alert(tmp_path) -> None:
    body = b'{"articles":[{"title":"Bitcoin security incident hack","url":"https://news.example.org/a?utm_source=x","publishedAt":"Thu, 01 Jan 1970 00:00:00 GMT","description":"BTC exchange exploit"}]}'
    revised = b'{"articles":[{"title":"Bitcoin security incident hack","url":"https://news.example.org/a","publishedAt":"Thu, 01 Jan 1970 00:00:00 GMT","description":"BTC exchange exploit confirmed"}]}'
    source = NewsSourceConfigV2(SOURCE_ID, NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                                "https://news.example.org/feed", ("news.example.org",),
                                qualification=SourceQualificationV2.UNVERIFIED)
    other = NewsSourceConfigV2("REPUTABLE_FIXTURE", NewsSourceClassV2.REPUTABLE_NEWS,
                               "https://news.example.org/other", ("news.example.org",))
    times = iter([110, 210, 310, 410])
    responses = [_feed_doc(body, 100), _feed_doc(body, 200), _feed_doc(revised, 300),
                 _feed_doc(body, 400, "https://news.example.org/other")]
    def health(source_id: str, at: int) -> PublicSourceHealthV2:
        return PublicSourceHealthV2(
            source_id, at - 1, at - 1, PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json({"health": source_id, "at": at}), "fixture",
        )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        def authenticate(_source: NewsSourceConfigV2, at: int) -> tuple[str, str]:
            ref = sha256_json({"auth": at})
            _index(repository, ref, "NewsAuthenticationFixtureV2", at - 1)
            return "AUTHENTICATED", ref

        pipeline = NewsCollectionPipelineV2(
            repository, clock_ns=lambda: next(times), transport=_QueueTransport(responses),
            sources=(source, other), asset_aliases={"btc": "BTC", "bitcoin": "BTC"},
            source_health=health,
            authentication=authenticate,
        )
        first = pipeline.collect(SOURCE_ID)
        assert len(first.event_refs) == 1 and len(first.alert_refs) == 1
        event_entry = repository.get_artifact(first.event_refs[0])
        assert event_entry is not None
        event = NewsEventV2(
            event_entry.metadata["event"]["event_id"], event_entry.metadata["event"]["source_id"],
            NewsSourceClassV2(event_entry.metadata["event"]["source_class"]),
            event_entry.metadata["event"]["source_url"], event_entry.metadata["event"]["content_hash"],
            event_entry.metadata["event"]["claimed_published_at_ns"],
            event_entry.metadata["event"]["received_at_ns"], event_entry.metadata["event"]["extraction_completed_ns"],
            tuple(event_entry.metadata["event"]["affected_asset_ids"]), event_entry.metadata["event"]["event_type"],
            event_entry.metadata["event"]["severity"], Decimal(event_entry.metadata["event"]["confidence"]),
            tuple(event_entry.metadata["event"]["supporting_spans"]), event_entry.metadata["event"]["duplicate_group"],
            event_entry.metadata["event"]["raw_ref"], event_entry.metadata["event"]["receipt_ref"],
            event_entry.metadata["event"]["source_health_ref"], event_entry.metadata["event"]["authentication_state"],
            event_entry.metadata["event"]["authentication_ref"],
            AssetMappingStateV2(event_entry.metadata["event"]["asset_mapping_state"]),
            event_entry.metadata["event"]["supersedes_id"], event_entry.metadata["event"]["expires_at_ns"],
            event_entry.metadata["event"]["schema_version"], event_entry.metadata["event"]["source_health_state"],
        )
        assert event.claimed_published_at_ns == 0
        assert event.available_at_ns == max(event.received_at_ns, event.extraction_completed_ns) == 110
        assert event.source_health_state == "HEALTHY_CURRENT"
        duplicate = pipeline.collect(SOURCE_ID)
        assert duplicate.duplicate_count == 1
        assert duplicate.event_refs == first.event_refs and duplicate.alert_refs == ()
        revision = pipeline.collect(SOURCE_ID)
        revised_entry = repository.get_artifact(revision.event_refs[0])
        assert revised_entry is not None
        assert revised_entry.metadata["event"]["supersedes_id"] == first.event_refs[0]
        cross_source = pipeline.collect("REPUTABLE_FIXTURE")
        assert len(repository.artifact_entries("NewsSourceConflictV2")) == 1
        assert len(repository.artifact_entries("EventAlertV2")) <= 3
        alert = repository.get_artifact(first.alert_refs[0])
        assert alert is not None and alert.metadata["alert"]["delivery_status"] == "UNVERIFIED"
        assert alert.metadata["alert"]["event_ref"] == first.event_refs[0]
        assert first.raw_ref == duplicate.raw_ref
        assert first.raw_hash == duplicate.raw_hash
        assert cross_source.event_refs


def test_official_announcement_and_status_sources_remain_unverified_and_parse_status_evidence() -> None:
    sources = {item.source_id: item for item in DEFAULT_NEWS_SOURCES_V2}
    assert {"FEDERAL_RESERVE", "BLS", "SEC", "BYBIT_OFFICIAL", "BYBIT_STATUS",
            "BINANCE_OFFICIAL", "BINANCE_STATUS", "GDELT_DISCOVERY", "COIN_METRICS_COMMUNITY"} <= set(sources)
    assert all(item.qualification == SourceQualificationV2.UNVERIFIED for item in sources.values())
    assert sources["FEDERAL_RESERVE"].source_class == NewsSourceClassV2.OFFICIAL_MACRO
    assert sources["BLS"].source_class == NewsSourceClassV2.OFFICIAL_MACRO
    assert sources["SEC"].source_class == NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY
    mapping, assets = map_assets_v2("Bitcoin and Ethereum", {"bitcoin": "BTC", "ethereum": "ETH"})
    assert mapping == AssetMappingStateV2.AMBIGUOUS and assets == ()
    bybit = parse_feed_bytes_v2(
        b'{"retCode":0,"result":{"list":[{"id":"incident-1","title":"API Maintenance",'
        b'"state":"ongoing","begin":"2000000000000"}]}}',
        base_url=sources["BYBIT_STATUS"].feed_url,
    )
    assert len(bybit) == 1
    assert "system status ongoing" in bybit[0].text
    assert bybit[0].url.endswith("id=incident-1")
    binance = parse_feed_bytes_v2(
        b'{"status":1,"msg":"system maintenance"}',
        base_url=sources["BINANCE_STATUS"].feed_url,
    )
    assert len(binance) == 1 and "maintenance" in binance[0].text
    assert parse_feed_bytes_v2(b'{"status":0,"msg":"normal"}',
                               base_url=sources["BINANCE_STATUS"].feed_url) == ()


def test_authentication_resolution_appends_revision_once_and_alerts_that_revision(tmp_path) -> None:
    body = b'{"articles":[{"title":"Bitcoin security incident","url":"https://news.example.org/a","description":"BTC exchange exploit"}]}'
    source = NewsSourceConfigV2(SOURCE_ID, NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                                "https://news.example.org/feed", ("news.example.org",))
    responses = [_feed_doc(body, 100), _feed_doc(body, 200), _feed_doc(body, 300)]
    auth_states = iter(("UNKNOWN", "AUTHENTICATED", "AUTHENTICATED"))
    times = iter((110, 210, 310))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        def authenticate(_source: NewsSourceConfigV2, at: int) -> tuple[str, str | None]:
            state = next(auth_states)
            if state == "UNKNOWN":
                return state, None
            ref = sha256_json({"auth-evidence": at})
            _index(repository, ref, "NewsAuthenticationFixtureV2", at - 1)
            return state, ref

        pipeline = NewsCollectionPipelineV2(
            repository, clock_ns=lambda: next(times), transport=_QueueTransport(responses),
            sources=(source,), asset_aliases={"bitcoin": "BTC"}, authentication=authenticate,
        )
        first = pipeline.collect(SOURCE_ID)
        revised = pipeline.collect(SOURCE_ID)
        duplicate = pipeline.collect(SOURCE_ID)

        assert first.alert_refs == ()
        assert revised.event_refs != first.event_refs
        revised_entry = repository.get_artifact(revised.event_refs[0])
        assert revised_entry is not None
        assert revised_entry.metadata["event"]["supersedes_id"] == first.event_refs[0]
        assert revised_entry.metadata["event"]["authentication_state"] == "AUTHENTICATED"
        assert len(revised.alert_refs) == 1
        alert = repository.get_artifact(revised.alert_refs[0])
        assert alert is not None and alert.metadata["alert"]["event_ref"] == revised.event_refs[0]
        assert duplicate.duplicate_count == 1
        assert duplicate.event_refs == revised.event_refs and duplicate.alert_refs == ()
        assert len(repository.artifact_entries("NewsEventV2")) == 2
        assert len(repository.artifact_entries("EventAlertV2")) == 1


def test_event_gate_calendar_coverage_missing_fails_closed_and_blackout_is_half_open(tmp_path) -> None:
    scheduled_at = 20_000 * NS
    start, end = scheduled_at - 30 * 60 * NS, scheduled_at + 15 * 60 * NS
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        builder = EventSafetyGateBuilderV2(repository)
        unknown = builder.evaluate(key=KEY, cutoff_ns=start, coverage=None, scheduled_events=(),
                                   abnormality=None, incidents=())
        assert unknown.state == EventGateStateV2.UNKNOWN and unknown.blocked
        assert unknown.to_s1_event_gate().state.value == "UNKNOWN"
        for cutoff, expected in ((start - 1, EventGateStateV2.CLEAR),
                                 (start, EventGateStateV2.BLOCKED),
                                 (end - 1, EventGateStateV2.BLOCKED),
                                 (end, EventGateStateV2.CLEAR)):
            coverage = _coverage(repository, cutoff)
            abnormality = _normal_abnormality(repository, cutoff)
            event = _macro_event(repository, scheduled_at, cutoff)
            gate = builder.evaluate(key=KEY, cutoff_ns=cutoff, coverage=coverage,
                                    scheduled_events=(event,), abnormality=abnormality, incidents=())
            assert gate.state == expected
            if expected == EventGateStateV2.BLOCKED:
                assert gate.reasons == ("SCHEDULED_MACRO_BLACKOUT",)
            assert gate.to_s1_event_gate().evidence_ref == gate.content_hash


def test_event_gate_requires_full_decision_blackout_coverage_horizon(tmp_path) -> None:
    cutoff = 100_000 * NS
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        builder = EventSafetyGateBuilderV2(repository)
        abnormality = _normal_abnormality(repository, cutoff)

        def coverage(start: int, end: int) -> CalendarCoverageV2:
            source_ref = sha256_json({"horizon-source": cutoff, "from": start, "through": end})
            _index(repository, source_ref, "CalendarSourceFixtureV2", cutoff)
            return CalendarCoverageV2(
                "FEDERAL_RESERVE", start, end, cutoff, cutoff, cutoff, True,
                "schedule-horizon-r1", source_ref, "VERIFIED",
            )

        required_start = cutoff - 15 * 60 * NS
        required_end = cutoff + 30 * 60 * NS
        for start, end in (
            (required_start, cutoff),
            (required_start, required_end - 1),
            (required_start + 1, required_end),
        ):
            gate = builder.evaluate(
                key=KEY, cutoff_ns=cutoff, coverage=coverage(start, end), scheduled_events=(),
                abnormality=abnormality, incidents=(),
            )
            assert gate.state == EventGateStateV2.UNKNOWN
            assert gate.blocked
            assert "CALENDAR_COVERAGE_MISSING_STALE_OR_INCOMPLETE" in gate.reasons

        complete = builder.evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=coverage(required_start, required_end),
            scheduled_events=(), abnormality=abnormality, incidents=(),
        )
        assert complete.state == EventGateStateV2.CLEAR
        assert not complete.blocked

        missing_source_coverage = CalendarCoverageV2(
            "FEDERAL_RESERVE", required_start, required_end, cutoff, cutoff, cutoff,
            True, "schedule-missing-source-r1", sha256_json({"missing-calendar-source": cutoff}), "VERIFIED",
        )
        missing_source = builder.evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=missing_source_coverage, scheduled_events=(),
            abnormality=abnormality, incidents=(),
        )
        assert missing_source.state == EventGateStateV2.UNKNOWN
        assert "CALENDAR_COVERAGE_MISSING_STALE_OR_INCOMPLETE" in missing_source.reasons


def test_event_gate_horizon_preserves_exact_half_open_blackout_and_missing_event_evidence(tmp_path) -> None:
    cutoff = 200_000 * NS
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        builder = EventSafetyGateBuilderV2(repository)
        coverage = _coverage(repository, cutoff)
        abnormality = _normal_abnormality(repository, cutoff)

        for scheduled_at, expected in (
            (cutoff + 30 * 60 * NS, EventGateStateV2.BLOCKED),
            (cutoff - 15 * 60 * NS, EventGateStateV2.CLEAR),
            (cutoff - 15 * 60 * NS + 1, EventGateStateV2.BLOCKED),
        ):
            event = _macro_event(repository, scheduled_at, cutoff)
            gate = builder.evaluate(
                key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(event,),
                abnormality=abnormality, incidents=(),
            )
            assert gate.state == expected

        missing_source_event = ScheduledEventV2(
            sha256_json({"event-with-missing-source": cutoff}), "US_CPI", cutoff + 2 * 60 * 60 * NS,
            "schedule-r1", "FEDERAL_RESERVE", cutoff - 1, cutoff, cutoff,
            sha256_json({"not-indexed-event-source": cutoff}),
        )
        missing_evidence = builder.evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(missing_source_event,),
            abnormality=abnormality, incidents=(),
        )
        assert missing_evidence.state == EventGateStateV2.UNKNOWN
        assert "SCHEDULE_EVENT_SOURCE_EVIDENCE_UNAVAILABLE" in missing_evidence.reasons


def test_event_gate_abnormality_unknown_abnormal_and_unresolved_incident_do_not_expire(tmp_path) -> None:
    cutoff = 50_000 * NS
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        builder = EventSafetyGateBuilderV2(repository)
        coverage = _coverage(repository, cutoff)
        for state in (AbnormalityStateV2.UNKNOWN, AbnormalityStateV2.ABNORMAL):
            gate = builder.evaluate(key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(),
                                    abnormality=_normal_abnormality(repository, cutoff, state), incidents=())
            assert gate.state == EventGateStateV2.BLOCKED
        open_ref = sha256_json({"incident": "open"})
        _index(repository, open_ref, "IncidentSourceV2", cutoff)
        opened = OperationalIncidentV2("incident-1", KEY.venue.value, KEY.base_asset_id,
                                        IncidentStateV2.OPEN, 1, cutoff, cutoff, open_ref)
        late = cutoff + 365 * 24 * 60 * 60 * NS
        gate = builder.evaluate(key=KEY, cutoff_ns=late, coverage=_coverage(repository, late),
                                scheduled_events=(), abnormality=_normal_abnormality(repository, late),
                                incidents=(opened,))
        assert gate.state == EventGateStateV2.BLOCKED
        assert "UNRESOLVED_VENUE_OR_ASSET_INCIDENT" in gate.reasons
        verified_source = sha256_json({"verified-resolution": "incident-1"})
        resolved_ref = sha256_json({"incident": "verified-resolved", "revision": 2})
        _index(repository, verified_source, "VerifiedResolutionSourceV2", late + 1)
        _index(repository, resolved_ref, "IncidentSourceV2", late + 1)
        verified = OperationalIncidentV2("incident-1", KEY.venue.value, KEY.base_asset_id,
                                         IncidentStateV2.VERIFIED_RESOLVED, 2, late + 1, late + 1,
                                         resolved_ref, open_ref, verified_source)
        before_resolution = builder.evaluate(
            key=KEY, cutoff_ns=late, coverage=_coverage(repository, late), scheduled_events=(),
            abnormality=_normal_abnormality(repository, late), incidents=(opened, verified),
        )
        assert before_resolution.state == EventGateStateV2.BLOCKED
        after_resolution = builder.evaluate(
            key=KEY, cutoff_ns=late + 1, coverage=_coverage(repository, late + 1), scheduled_events=(),
            abnormality=_normal_abnormality(repository, late + 1), incidents=(opened, verified),
        )
        assert after_resolution.state == EventGateStateV2.CLEAR
        resolution_source = sha256_json({"human-review": "resolve incident-1"})
        resolution_at = late + 1
        _index(repository, resolution_source, "HumanReviewSourceV2", resolution_at)
        resolved_ref = sha256_json({"incident": "resolved", "revision": 2})
        _index(repository, resolved_ref, "IncidentSourceV2", resolution_at)
        resolved = OperationalIncidentV2("incident-1", KEY.venue.value, KEY.base_asset_id,
                                         IncidentStateV2.HUMAN_REVIEW_RESOLVED, 2, resolution_at, resolution_at,
                                         resolved_ref, open_ref, resolution_source)
        cleared = builder.evaluate(key=KEY, cutoff_ns=resolution_at, coverage=_coverage(repository, resolution_at),
                                   scheduled_events=(), abnormality=_normal_abnormality(repository, resolution_at),
                                   incidents=(opened, resolved))
        assert cleared.state == EventGateStateV2.CLEAR


def test_event_source_calendar_revision_missing_evidence_and_stale_coverage_block_clear(tmp_path) -> None:
    cutoff = 100_000 * NS
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        builder = EventSafetyGateBuilderV2(repository)
        coverage = CalendarCoverageV2("FEDERAL_RESERVE", 0, 10**16, cutoff, cutoff, cutoff,
                                      True, "r1", H, "UNVERIFIED")
        gate = builder.evaluate(key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(),
                                abnormality=_normal_abnormality(repository, cutoff), incidents=())
        assert gate.state == EventGateStateV2.UNKNOWN
        verified = _coverage(repository, cutoff)
        stale_source = replace(verified, observed_at_ns=cutoff - 25 * 60 * 60 * NS,
                               received_at_ns=cutoff - 25 * 60 * 60 * NS,
                               available_at_ns=cutoff - 25 * 60 * 60 * NS)
        stale = builder.evaluate(key=KEY, cutoff_ns=cutoff, coverage=stale_source, scheduled_events=(),
                                  abnormality=_normal_abnormality(repository, cutoff), incidents=())
        assert stale.state == EventGateStateV2.UNKNOWN


def test_directional_shadow_is_distinct_idempotent_and_exact_action_is_unestimable(tmp_path) -> None:
    event_health = PublicSourceHealthV2(SOURCE_ID, 10, 15, PublicSourceStateV2.HEALTHY_CURRENT,
                                       sha256_json({"news-health-ingest": 1}), "news")
    receipt_health = event_health
    bar_health = PublicSourceHealthV2(BAR_SOURCE, 899 * NS, 899 * NS, PublicSourceStateV2.HEALTHY_CURRENT,
                                      sha256_json({"bar-health": 1}), "bars")
    historical = _bar(KEY, BarIntervalV2.M5, 0, "100", "100", "10")
    first = _bar(KEY, BarIntervalV2.M5, 300 * NS, "100", "101", "10")
    second = _bar(KEY, BarIntervalV2.M5, 600 * NS, "101", "102", "1000")
    beta = PreEventBetaV2(KEY, KEY, 1.0, 20, 25, H)
    btc_return = MarketReturn5MV2(KEY, first.close_at_ns, first.close_at_ns, 0.0, H)
    spread = ReactionSpreadEvidenceV2(KEY, True, second.close_at_ns, second.close_at_ns, H)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        _index(repository, H, "EventEvidenceFixtureV2", 1)
        _index(repository, bar_health.content_hash, "PublicSourceHealthV2", bar_health.available_at_ns,
               {"health": bar_health.to_dict()})
        _index(repository, event_health.content_hash, "PublicSourceHealthV2", event_health.available_at_ns,
               {"health": event_health.to_dict()})
        event_health = PublicSourceHealthV2(SOURCE_ID, second.close_at_ns - 5 * NS,
                                            second.close_at_ns - 5 * NS,
                                            PublicSourceStateV2.HEALTHY_CURRENT, sha256_json({"news-health": 2}), "news")
        current_event_health = event_health
        _index(repository, current_event_health.content_hash, "PublicSourceHealthV2",
               current_event_health.available_at_ns, {"health": current_event_health.to_dict()})
        event = _event(source_health_ref=receipt_health.content_hash, received=20, extracted=25)
        _persist_news_event(repository, event)
        directional = S7DirectionalShadowV2(repository)
        first_result = directional.evaluate(
            event=event, key=KEY, first_bar=first, second_bar=second,
            history_bars=(historical, first, second), beta_to_btc=beta, btc_return_5m=btc_return,
            spread=spread, source_health=current_event_health, bar_source_health=bar_health,
            cutoff_ns=second.close_at_ns,
        )
        repeat = directional.evaluate(
            event=event, key=KEY, first_bar=first, second_bar=second,
            history_bars=(second, first, historical), beta_to_btc=beta, btc_return_5m=btc_return,
            spread=spread, source_health=current_event_health, bar_source_health=bar_health,
            cutoff_ns=second.close_at_ns,
        )
        assert first_result.status == "TRIGGER_CONFIRMED", first_result.reason
        assert first_result.side == V2Side.LONG
        assert first_result.content_hash == repeat.content_hash
        assert len(repository.artifact_entries("EventReactionArtifactV2")) == 1
        watch_id = sha256_json({"event_ref": event.event_id, "key": KEY.to_dict(),
                                "reaction": "S7_DIRECTIONAL_SHADOW", "version": "S7_DIRECTIONAL_REACTION_V1"})
        watch = repository.get_watch(watch_id)
        assert watch is not None and watch.state == WatchStateV2.CONFIRMED
        reaction_entry = repository.get_artifact(first_result.content_hash)
        assert reaction_entry is not None
        assert reaction_entry.metadata["safety_gate_ref"] is None
        assert repository.artifact_entries("CandidateActionV2") == ()
        ambiguous = _event(mapping=AssetMappingStateV2.AMBIGUOUS, assets=(),
                           source_health_ref=receipt_health.content_hash)
        _persist_news_event(repository, ambiguous)
        result = directional.evaluate(
            event=ambiguous, key=KEY, first_bar=first, second_bar=second,
            history_bars=(historical,), beta_to_btc=beta, btc_return_5m=btc_return,
            spread=spread, source_health=current_event_health, bar_source_health=bar_health,
            cutoff_ns=second.close_at_ns,
        )
        assert result.status == "NOT_ESTIMABLE"
        assert result.reason == "ASSET_MAPPING_AMBIGUOUS_OR_MISMATCHED"
        unresolved = _event(auth="UNKNOWN", source_health_ref=receipt_health.content_hash)
        _persist_news_event(repository, unresolved)
        late_resolution = directional.evaluate(
            event=unresolved, key=KEY, first_bar=first, second_bar=second,
            history_bars=(historical,), beta_to_btc=beta, btc_return_5m=btc_return,
            spread=spread, source_health=current_event_health, bar_source_health=bar_health,
            cutoff_ns=second.close_at_ns,
            resolution=EventResolutionV2(unresolved.event_id, second.close_at_ns, H),
        )
        assert late_resolution.status == "NOT_ESTIMABLE"
        assert late_resolution.reason == "EVENT_SOURCE_NOT_AUTHENTICATED_OR_RESOLVED"
