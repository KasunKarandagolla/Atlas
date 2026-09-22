from __future__ import annotations

from dataclasses import replace

import pytest

from atlas.data.availability import AvailabilityRuleKind, ReplayAvailabilityRule, historical_import, reconstruct_public
from atlas.data.models import DataKind, ReplayMode, make_record
from atlas.data.replay import available_for_decision
from atlas.data.validation import CausalRecordValidator
from atlas.domain.enums import AvailabilityClass

T0 = 1_700_000_000_000_000_000


def actual(record_id="r1", event=T0, received=T0 + 10, payload=None):
    return make_record(
        record_id=record_id,
        source_id="BYBIT_PUBLIC",
        venue="BYBIT",
        instrument="BTCUSDT",
        data_kind=DataKind.BAR_1M_LAST,
        payload=payload or {"close": "50000"},
        received_at_ns=received,
        processed_at_ns=received + 1,
        available_at_ns=received + 1,
        data_ingested_at_ns=received,
        recorded_at_ns=received + 1,
        evidence_ref="raw:" + record_id,
        source_event_at_ns=event,
        bar_start_ns=event - 60_000_000_000,
        bar_end_ns=event,
        availability_class=AvailabilityClass.ACTUAL_OBSERVED,
    )


def test_actual_and_reconstructed_views_are_separate():
    r = actual(received=T0 + 1000)
    rule = ReplayAvailabilityRule("bar-close", "v1", AvailabilityRuleKind.BAR_END_PLUS_LAG, 5, "conservative bar lag")
    rr = reconstruct_public(r, rule)
    assert rr.record_id != r.record_id
    assert r.record_id in rr.record_id
    assert rule.rule_id in rr.record_id and rule.version in rr.record_id and rule.hash() in rr.record_id
    assert r.received_at_ns == rr.received_at_ns == T0 + 1000
    assert rr.replay_available_at_ns == T0 + 5
    assert available_for_decision([rr], T0 + 10, ReplayMode.ACTUAL_SYSTEM) == []
    assert available_for_decision([rr], T0 + 10, ReplayMode.RECONSTRUCTED_MARKET) == [rr]


def test_historical_import_cannot_backdate_receipt():
    with pytest.raises(ValueError, match="backdate"):
        historical_import(
            actual_ingested_at_ns=T0 + 1000,
            received_at_ns=T0 + 1,
            record_id="h",
            source_id="archive",
            venue="BYBIT",
            instrument="BTCUSDT",
            data_kind=DataKind.BAR_1H_LAST,
            payload={"close": "1"},
            evidence_ref="download",
            source_event_at_ns=T0,
            availability_class=AvailabilityClass.UNKNOWN,
        )
    r = historical_import(
        actual_ingested_at_ns=T0 + 1000,
        record_id="h",
        source_id="archive",
        venue="BYBIT",
        instrument="BTCUSDT",
        data_kind=DataKind.BAR_1H_LAST,
        payload={"close": "1"},
        evidence_ref="download",
        source_event_at_ns=T0,
        availability_class=AvailabilityClass.UNKNOWN,
    )
    assert r.received_at_ns == T0 + 1000 and r.data_ingested_at_ns == T0 + 1000


def test_conflicting_duplicate_and_clock_conflict_quarantine_conditions():
    v = CausalRecordValidator()
    a = actual()
    assert v.accept(a).accepted
    b = actual(payload={"close": "51000"})
    res = v.validate(b)
    assert not res.accepted and any("conflicting content" in x for x in res.reasons)
    future = actual("future", event=T0 + 100, received=T0 + 10)
    res = v.validate(future)
    assert not res.accepted and any("clock" in x for x in res.reasons)


def test_replay_rule_hash_is_deterministic():
    a = ReplayAvailabilityRule("r", "1", AvailabilityRuleKind.BAR_END_PLUS_LAG, 100, "x")
    b = ReplayAvailabilityRule("r", "1", AvailabilityRuleKind.BAR_END_PLUS_LAG, 100, "x")
    assert a.hash() == b.hash()


def test_validator_rejects_same_id_with_any_changed_causal_identity():
    original = actual()
    validator = CausalRecordValidator()
    assert validator.accept(original).accepted
    candidates = (
        replace(
            original,
            received_at_ns=T0 + 20,
            processed_at_ns=T0 + 21,
            available_at_ns=T0 + 21,
            data_ingested_at_ns=T0 + 20,
            recorded_at_ns=T0 + 21,
        ),
        replace(original, available_at_ns=T0 + 22, processed_at_ns=T0 + 21, recorded_at_ns=T0 + 22),
        replace(
            original,
            availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
            availability_lower_ns=T0 + 5,
            availability_upper_ns=T0 + 5,
            replay_available_at_ns=T0 + 5,
            availability_method="rule@v1:hash",
        ),
        replace(original, dependency_ids=("dependency-1",)),
        replace(original, evidence_ref="different-evidence"),
        replace(original, pipeline_version="phase3-v2"),
        replace(original, source_clock_precision_ns=2_000_000),
    )
    for candidate in candidates:
        result = validator.validate(candidate)
        assert not result.accepted
        assert any("immutable causal metadata" in reason for reason in result.reasons)
