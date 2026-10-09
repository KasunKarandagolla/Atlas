from __future__ import annotations

import base64
import hashlib
import threading

import pytest

from atlas.v2.memory.repository import OpsRepository
from atlas.v2.news.events import FetchedDocumentV2, NewsSourceClassV2, NewsSourceConfigV2, SourceQualificationV2
from atlas.v2.runtime.official_calendar import (
    CALENDAR_CADENCE_NS,
    OfficialCalendarMaintenanceV1,
    parse_bls_ics_v1,
    parse_fomc_html_dates_v1,
)

NOW = 1_800_000_000_000_000_000
ICS = b"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:cpi-2028-11@example.gov
DTSTART;TZID=America/New_York:20281106T083000
DTSTAMP:20260901T120000Z
SUMMARY:Consumer Price Index (CPI)
END:VEVENT
BEGIN:VEVENT
UID:payroll-2028-11@example.gov
DTSTART:20281106T133000Z
SUMMARY:The Employment Situation
END:VEVENT
END:VCALENDAR
"""


class FakeTransport:
    def __init__(self, response: FetchedDocumentV2):
        self.response = response
        self.called = threading.Event()
        self.calls = 0

    def fetch(self, _source: NewsSourceConfigV2) -> FetchedDocumentV2:
        self.calls += 1
        self.called.set()
        return self.response


class PollingTransport:
    def __init__(self, clock):
        self.clock = clock
        self.called = threading.Event()
        self.calls = 0

    def fetch(self, source):
        self.calls += 1
        self.called.set()
        return FetchedDocumentV2(source.feed_url, source.feed_url, 200, ICS, self.clock(), {})


def test_bls_ics_uses_timezone_dst_and_preserves_source_publication_time():
    parsed = parse_bls_ics_v1(ICS)
    assert parsed.complete
    assert [(kind, when) for kind, when, _, _ in parsed.events] == [
            ("US_CPI", 1_857_130_200_000_000_000),
            ("US_PAYROLL", 1_857_130_200_000_000_000),
    ]


def test_unqualified_or_date_only_ics_time_is_not_estimated():
    parsed = parse_bls_ics_v1(b"""BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:x\nDTSTART:20281106T083000\nSUMMARY:CPI\nEND:VEVENT\nEND:VCALENDAR\n""")
    assert not parsed.complete
    assert not parsed.events
    assert parsed.missing_times[0]["missing_field"] == "EXACT_TIMEZONE_QUALIFIED_DTSTART"


@pytest.mark.parametrize("local_time", ("20281105T013000", "20280312T023000"))
def test_ambiguous_or_nonexistent_dst_wall_times_are_not_estimated(local_time):
    body = ("BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:x\nDTSTART;TZID=America/New_York:" + local_time
            + "\nSUMMARY:CPI\nEND:VEVENT\nEND:VCALENDAR\n").encode()
    parsed = parse_bls_ics_v1(body)
    assert not parsed.events
    assert not parsed.complete


def test_fomc_official_dates_are_not_given_invented_release_times():
    parsed = parse_fomc_html_dates_v1(b"<html><p>November 6-7, 2026</p></html>")
    assert not parsed.complete
    assert not parsed.events
    assert parsed.missing_times[0]["missing_field"] == "OFFICIAL_RELEASE_TIME"


def test_calendar_worker_holds_future_receipt_until_cutoff_then_archives_future_events(tmp_path):
    now = [NOW + 10]
    received = NOW + 10
    transport = FakeTransport(FetchedDocumentV2(
        "https://www.bls.gov/schedule/news_release/bls.ics",
        "https://www.bls.gov/schedule/news_release/bls.ics", 200, ICS, received, {}))
    source = NewsSourceConfigV2("BLS_TEST", NewsSourceClassV2.OFFICIAL_MACRO,
        "https://www.bls.gov/schedule/news_release/bls.ics", ("www.bls.gov",),
        parser="OFFICIAL_ICS_V1", qualification=SourceQualificationV2.UNVERIFIED)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: now[0], transport=transport, sources=(source,))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        started = runtime.run_cycle(repo, information_cutoff_ns=NOW)
        assert started["status"] == "FETCH_STARTED"
        assert transport.called.wait(1)
        # Give the fake worker time to publish its single completion slot.
        assert runtime._worker is not None
        runtime._worker.join(timeout=1)
        now[0] = NOW + 20
        hidden = runtime.run_cycle(repo, information_cutoff_ns=NOW + 9)
        assert hidden["status"] == "PENDING"
        assert repo.artifact_entries("OfficialCalendarRawV1") == ()
        visible = runtime.run_cycle(repo, information_cutoff_ns=NOW + 20)
        assert visible["status"] == "NOT_ESTIMABLE"  # source qualification is unverified
        events = visible["events"]
        assert len(events) == 2 and all(item.scheduled_at_ns > NOW + 20 for item in events)
        assert events[0].source_event_at_ns == 1_788_264_000_000_000_000
        assert events[1].source_event_at_ns == received
        stored_events = repo.latest_artifact_entries("ScheduledEventV2", as_of_ns=NOW + 20, limit=2)
        assert {entry.metadata["evidence"]["event_id"] for entry in stored_events.entries} == {
            item.event_id for item in events}
        stored_coverage = repo.latest_artifact_entries("CalendarCoverageV2", as_of_ns=NOW + 20, limit=1)
        assert stored_coverage.entries[0].metadata["evidence"].to_dict() == visible["coverage"].to_dict()
        assert visible["coverage"].observed_at_ns == received
        assert visible["coverage"].received_at_ns == received
        assert visible["coverage"].available_at_ns == NOW + 20
        assert all(item.available_at_ns <= NOW + 20 for item in events)
        raws = repo.artifact_entries("OfficialCalendarRawV1")
        assert len(raws) == 1
        raw = raws[0]
        assert hashlib.sha256(base64.b64decode(raw.metadata["raw_bytes_base64"])).hexdigest() == raw.metadata["raw_payload_hash"]
        assert raw.metadata["publication_status"] == "PUBLICATION_UNKNOWN"
        evidence = repo.get_artifact(events[0].evidence_ref)
        assert evidence is not None
        assert evidence.metadata["source_publication_at_ns"] == 1_788_264_000_000_000_000
        assert evidence.metadata["publication_status"] == "SOURCE_TIMESTAMP"
        unknown_evidence = repo.get_artifact(events[1].evidence_ref)
        assert unknown_evidence is not None
        assert unknown_evidence.metadata["source_publication_at_ns"] is None
        assert unknown_evidence.metadata["publication_status"] == "PUBLICATION_UNKNOWN"
        assert transport.calls == 1
        runtime.close()


def test_repeat_poll_same_revision_reuses_first_typed_event_receipt(tmp_path):
    now = [NOW + 10]
    transport = PollingTransport(lambda: now[0])
    source = NewsSourceConfigV2("BLS_TEST", NewsSourceClassV2.OFFICIAL_MACRO,
        "https://www.bls.gov/schedule/news_release/bls.ics", ("www.bls.gov",),
        parser="OFFICIAL_ICS_V1", qualification=SourceQualificationV2.UNVERIFIED)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: now[0], transport=transport, sources=(source,))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        runtime.run_cycle(repo, information_cutoff_ns=NOW)
        assert transport.called.wait(1)
        assert runtime._worker is not None
        runtime._worker.join(timeout=1)
        now[0] = NOW + 20
        first = runtime.run_cycle(repo, information_cutoff_ns=now[0])
        first_events = first["events"]
        first_refs = {event.content_hash for event in first_events}

        now[0] = NOW + CALENDAR_CADENCE_NS + 10
        transport.called.clear()
        runtime.run_cycle(repo, information_cutoff_ns=now[0])
        assert transport.called.wait(1)
        assert runtime._worker is not None
        runtime._worker.join(timeout=1)
        now[0] += 20
        repeated = runtime.run_cycle(repo, information_cutoff_ns=now[0])
        assert repeated["events"] == first_events
        assert {event.content_hash for event in repeated["events"]} == first_refs
        typed_events = repo.latest_artifact_entries("ScheduledEventV2", as_of_ns=now[0], limit=3)
        assert len(typed_events.entries) == 2
        assert len(repo.artifact_entries("OfficialCalendarRawV1")) == 1
        assert repeated["coverage"].received_at_ns == NOW + CALENDAR_CADENCE_NS + 10
        runtime.close()


def test_fetch_cadence_is_at_least_one_minute_and_https_alone_does_not_verify():
    assert CALENDAR_CADENCE_NS >= 60_000_000_000


def test_verified_bls_source_alone_does_not_complete_cpi_payroll_fomc_coverage(tmp_path):
    now = [NOW + 10]
    url = "https://www.bls.gov/schedule/news_release/bls.ics"
    transport = FakeTransport(FetchedDocumentV2(url, url, 200, ICS, NOW + 10, {}))
    source = NewsSourceConfigV2("BLS_TEST", NewsSourceClassV2.OFFICIAL_MACRO, url,
        ("www.bls.gov",), parser="OFFICIAL_ICS_V1", qualification=SourceQualificationV2.VERIFIED)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: now[0], transport=transport, sources=(source,))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        runtime.run_cycle(repo, information_cutoff_ns=NOW)
        assert transport.called.wait(1)
        assert runtime._worker is not None
        runtime._worker.join(timeout=1)
        now[0] = NOW + 20
        result = runtime.run_cycle(repo, information_cutoff_ns=NOW + 20)
        assert not result["coverage"].complete
        assert result["coverage"].source_qualification == "VERIFIED"
        assert result["coverage"].source_id == "US_OFFICIAL_MACRO_CALENDAR"
        runtime.close()


def _fomc_ics(*rows):
    return ('BEGIN:VCALENDAR\n' + '\n'.join(
        f'BEGIN:VEVENT\nUID:{uid}\nSUMMARY:{title}\n{start}\nEND:VEVENT'
        for uid, title, start in rows) + '\nEND:VCALENDAR\n').encode()


def test_exact_fomc_decisions_use_actual_dst_and_exclude_minutes_and_press_conferences():
    from datetime import UTC, datetime

    from atlas.v2.runtime.official_calendar import parse_exact_fomc_ics_v1
    body = _fomc_ics(
        ('summer', 'FOMC Meeting', 'DTSTART;TZID=America/New_York:20281025T140000'),
        ('winter', 'FOMC Rate Decision', 'DTSTART;TZID=America/New_York:20281213T140000'),
        ('minutes', 'FOMC Minutes', 'DTSTART:20281213T190000Z'),
        ('press', 'FOMC Press Conference', 'DTSTART:20281213T193000Z'))
    result = parse_exact_fomc_ics_v1(body)
    assert result.complete
    assert [(uid, datetime.fromtimestamp(when / 1e9, UTC).hour) for _, when, uid, _ in result.events] == [
        ('summer', 18), ('winter', 19)]
    assert all(published is None for _, _, _, published in result.events)


@pytest.mark.parametrize('start', ('DTSTART:20281025T140000', 'DTSTART;VALUE=DATE:20281025',
    'DTSTART;TZID=America/New_York:20281105T013000', 'DTSTART;TZID=America/New_York:20280312T023000'))
def test_exact_fomc_missing_or_ambiguous_time_fails_closed(start):
    from atlas.v2.runtime.official_calendar import parse_exact_fomc_ics_v1
    result = parse_exact_fomc_ics_v1(_fomc_ics(('x', 'FOMC Meeting', start)))
    assert not result.complete and not result.events
    assert result.missing_times


def test_exact_fomc_duplicate_uid_and_empty_decision_set_fail_closed():
    from atlas.v2.runtime.official_calendar import parse_exact_fomc_ics_v1
    row = ('x', 'FOMC Meeting', 'DTSTART:20281025T180000Z')
    assert 'MISSING_OR_DUPLICATE_ICS_UID' in parse_exact_fomc_ics_v1(_fomc_ics(row, row)).reasons
    assert not parse_exact_fomc_ics_v1(_fomc_ics(('m', 'FOMC Minutes', row[2]))).complete


def test_live_fed_json_schema_does_not_estimate_missing_timezone_or_classify_minutes():
    import json

    from atlas.v2.runtime.official_calendar import parse_fomc_json_v1
    rows = [{"type": 'FOMC', "title": title, "month": '2026-10', "days": '28', "time": clock}
            for title, clock in [('FOMC Meeting', '2:00 p.m.'), ('FOMC Minutes', '2:00 p.m.'),
                                  ('FOMC Press Conference', '2:30 p.m.')]]
    result = parse_fomc_json_v1(json.dumps({'events': rows}).encode())
    assert not result.complete and not result.events and len(result.missing_times) == 1
    rows[0]['timezone'] = 'America/New_York'
    result = parse_fomc_json_v1(json.dumps({'events': rows}).encode())
    assert result.complete and len(result.events) == 1
    assert result.events[0][1] == 1_793_210_400_000_000_000
    assert result.events[0][3] is None


@pytest.mark.parametrize('updates', ({'qualification': 'VERIFIED'}, {'feed_url': 'https://other.example/calendar.ics'},
    {'feed_url': 'https://www.federalreserve.gov/calendar.ics?token=secret'},
    {'timezone': 'America/New_York'}, {'qualification_evidence_ref': 'not-a-hash'}, {'schema_version': True}))
def test_immutable_fomc_profile_rejects_untrusted_qualification_secrets_and_unknown_fields(updates):
    from atlas.v2.runtime.official_calendar import OfficialFomcSourceProfileV1
    body = {'schema_version': 1, 'feed_url': 'https://www.federalreserve.gov/json/calendar.json',
            'parser': 'OFFICIAL_FOMC_JSON_V1', **updates}
    with pytest.raises(ValueError):
        OfficialFomcSourceProfileV1.from_dict(body)


def test_explicit_source_profile_is_immutable_roundtrips_and_reaches_runtime():
    from dataclasses import FrozenInstanceError

    from atlas.v2.runtime.official_calendar import OfficialFomcSourceProfileV1
    profile = OfficialFomcSourceProfileV1('https://www.federalreserve.gov/json/calendar.json',
                                         parser='OFFICIAL_FOMC_JSON_V1')
    assert OfficialFomcSourceProfileV1.from_dict(profile.to_dict()) == profile
    assert OfficialFomcSourceProfileV1.from_dict(profile.to_dict()).content_hash == profile.content_hash
    with pytest.raises(FrozenInstanceError):
        profile.feed_url = 'https://www.federalreserve.gov/other.ics'
    runtime = OfficialCalendarMaintenanceV1(fomc_source_profile=profile)
    assert runtime.sources[1].parser == 'OFFICIAL_FOMC_JSON_V1'
    assert runtime.sources[1].qualification == SourceQualificationV2.UNVERIFIED
    runtime.close()


def test_empty_future_exact_fomc_coverage_is_incomplete_and_replays_from_raw(tmp_path):
    from atlas.v2.runtime.official_calendar import _Slot
    from atlas.v2.science.broad_export import project_broad_evidence
    body = _fomc_ics(('past', 'FOMC Meeting', 'DTSTART:20260901T180000Z'))
    source = NewsSourceConfigV2('FED_TEST', NewsSourceClassV2.OFFICIAL_MACRO,
        'https://www.federalreserve.gov/calendar.ics', ('www.federalreserve.gov',),
        parser='OFFICIAL_ICS_FOMC_V1', qualification=SourceQualificationV2.VERIFIED)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: NOW + 20, sources=(source,))
    response = FetchedDocumentV2(source.feed_url, source.feed_url, 200, body, NOW + 10, {})
    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        events, coverage, _, reasons = runtime._persist_response(repo, _Slot(source, NOW, NOW + 10, response, None), NOW + 20)
        assert len(events) == 1 and not coverage.complete
        assert 'FUTURE_REQUIRED_RELEASE_CLASS_MISSING' in reasons
        for entry in repo.artifact_entries('OfficialCalendarCoverageEvidenceV1'):
            project_broad_evidence(repo, entry)
    runtime.close()


def test_all_three_macro_classes_require_intersecting_actual_source_intervals_and_typed_readback(tmp_path):
    from atlas.v2.runtime.official_calendar import _Slot
    from atlas.v2.science.broad_export import CALENDAR_EXPORT_TYPES, project_broad_evidence
    fomc = _fomc_ics(('before', 'FOMC Meeting', 'DTSTART:20260901T180000Z'),
                     ('after', 'FOMC Meeting', 'DTSTART:20281213T190000Z'))
    bls = NewsSourceConfigV2('BLS_TEST', NewsSourceClassV2.OFFICIAL_MACRO,
        'https://www.bls.gov/bls.ics', ('www.bls.gov',), parser='OFFICIAL_ICS_V1',
        qualification=SourceQualificationV2.VERIFIED)
    fed = NewsSourceConfigV2('FED_TEST', NewsSourceClassV2.OFFICIAL_MACRO,
        'https://www.federalreserve.gov/calendar.ics', ('www.federalreserve.gov',),
        parser='OFFICIAL_ICS_FOMC_V1', qualification=SourceQualificationV2.VERIFIED)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: NOW + 20, sources=(bls, fed))
    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        for source, raw in ((bls, ICS), (fed, fomc)):
            response = FetchedDocumentV2(source.feed_url, source.feed_url, 200, raw, NOW + 10, {})
            _, coverage, _, _ = runtime._persist_response(repo, _Slot(source, NOW, NOW + 10, response, None), NOW + 20)
        assert coverage.complete and coverage.source_qualification == 'VERIFIED'
        assert coverage.covered_from_ns == coverage.covered_through_ns == 1_857_130_200_000_000_000
        for kind in CALENDAR_EXPORT_TYPES:
            for entry in repo.artifact_entries(kind):
                project_broad_evidence(repo, entry)
    runtime.close()


def test_profile_qualification_requires_scoped_external_review_of_real_archived_exact_bytes(tmp_path):
    from atlas.v2._serialization import sha256_json
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.runtime.official_calendar import DEFAULT_CALENDAR_SOURCES, OfficialFomcSourceProfileV1, _Slot
    from atlas.v2.science.broad_export import project_broad_evidence
    url = 'https://www.federalreserve.gov/explicit-test.ics'
    profile = OfficialFomcSourceProfileV1(url)
    sources = (DEFAULT_CALENDAR_SOURCES[0], profile.source())
    fomc = _fomc_ics(('earlier', 'FOMC Meeting', 'DTSTART:20260901T180000Z'),
                    ('later', 'FOMC Meeting', 'DTSTART:20281213T190000Z'))
    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        initial = OfficialCalendarMaintenanceV1(clock_ns=lambda: NOW + 20, sources=sources)
        refs = []
        for source, body, classes in ((sources[0], ICS, ['US_CPI', 'US_PAYROLL']),
                                      (sources[1], fomc, ['FOMC_RATE_DECISION'])):
            response = FetchedDocumentV2(source.feed_url, source.feed_url, 200, body, NOW + 10, {})
            _, coverage, raw_ref, _ = initial._persist_response(repo, _Slot(source, NOW, NOW + 10, response, None), NOW + 20)
            assert not coverage.complete
            review = {"schema_version": 1, "source_id": source.source_id, "feed_url": source.feed_url,
                "parser": source.parser, "received_at_ns": NOW + 10, "raw_ref": raw_ref,
                "raw_payload_hash": hashlib.sha256(body).hexdigest(), "qualification": 'VERIFIED',
                "qualification_method": 'EXTERNAL_OFFICIAL_SOURCE_CAPABILITY_REVIEW_V1',
                "exact_timezone_qualified_schedule": True, "required_classes": classes}
            ref = sha256_json(review)
            repo.register_artifact(ArtifactIndexEntryV2(ref, 'OfficialCalendarSourceQualificationV1',
                ref, NOW + 15, NOW + 15, review))
            project_broad_evidence(repo, repo.get_artifact(ref))
            refs.append(ref)
        initial.close()
        profile = OfficialFomcSourceProfileV1(url, qualification_evidence_ref=refs[1],
                                             bls_qualification_evidence_ref=refs[0])
        runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: NOW + 40, fomc_source_profile=profile)
        for source, body in ((runtime.sources[0], ICS), (runtime.sources[1], fomc)):
            response = FetchedDocumentV2(source.feed_url, source.feed_url, 200, body, NOW + 30, {})
            _, coverage, _, _ = runtime._persist_response(repo, _Slot(source, NOW + 25, NOW + 30, response, None), NOW + 40)
        assert coverage.complete and coverage.source_qualification == 'VERIFIED'
        for kind in ('OfficialCalendarRawV1', 'OfficialCalendarCoverageEvidenceV1',
                     'OfficialCalendarAggregateEvidenceV1', 'ScheduledEventV2', 'CalendarCoverageV2'):
            for entry in repo.artifact_entries(kind):
                project_broad_evidence(repo, entry)
        assert runtime._source_qualification(repo, runtime.sources[1], NOW + 14) == 'UNVERIFIED'
        runtime.close()


def test_timezone_profile_without_archived_calendar_statement_never_supplies_a_timezone(tmp_path):
    import json

    from atlas.v2._serialization import sha256_json
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.runtime.official_calendar import OfficialFomcSourceProfileV1, _Slot
    from atlas.v2.science.broad_export import project_broad_evidence
    fake = {'schema_version': 1, 'timezone': 'America/New_York', 'qualification': 'VERIFIED'}
    ref = sha256_json(fake)
    profile = OfficialFomcSourceProfileV1('https://www.federalreserve.gov/json/calendar.json',
        parser='OFFICIAL_FOMC_JSON_V1', timezone='America/New_York', timezone_evidence_ref=ref,
        qualification_evidence_ref=ref)
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: NOW + 20, fomc_source_profile=profile)
    source = runtime.sources[1]
    body = json.dumps({'events': [{"type": 'FOMC', "title": 'FOMC Meeting', "month": '2028-10',
                                         "days": '25', "time": '2:00 p.m.'}]}).encode()
    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        repo.register_artifact(ArtifactIndexEntryV2(ref, 'ArbitraryCalendarClaimV1', ref, NOW, NOW, fake))
        response = FetchedDocumentV2(source.feed_url, source.feed_url, 200, body, NOW + 10, {})
        events, coverage, raw_ref, _ = runtime._persist_response(repo, _Slot(source, NOW, NOW + 10, response, None), NOW + 20)
        assert events == () and not coverage.complete and coverage.source_qualification == 'UNVERIFIED'
        raw = repo.get_artifact(raw_ref)
        assert raw.metadata['qualified_timezone'] is None
        project_broad_evidence(repo, raw)
    runtime.close()
