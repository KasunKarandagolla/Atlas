from __future__ import annotations

from atlas.data.models import DataKind, make_record
from atlas.data.validation import CausalRecordValidator
from atlas.domain.enums import AvailabilityClass

T = 1_700_000_000_000_000_000


def rec(rid, *, deps=(), revision_of=None, available=T + 10):
    return make_record(
        record_id=rid,
        source_id="s",
        venue="BYBIT",
        instrument="ETHUSDT",
        data_kind=DataKind.OPEN_INTEREST,
        payload={"v": rid},
        received_at_ns=T + 1,
        processed_at_ns=T + 2,
        available_at_ns=available,
        data_ingested_at_ns=T + 1,
        recorded_at_ns=max(available, T + 2),
        evidence_ref="e:" + rid,
        source_event_at_ns=T,
        availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        dependency_ids=deps,
        revision_of=revision_of,
    )


def test_dependency_availability_and_revision_lineage():
    v = CausalRecordValidator()
    base = rec("base")
    assert v.accept(base).accepted
    child = rec("child", deps=("base",), available=T + 9)
    assert not v.validate(child).accepted
    child = rec("child", deps=("base",), available=T + 10)
    assert v.accept(child).accepted
    bad = rec("rev", revision_of="missing")
    assert not v.validate(bad).accepted
    good = rec("rev", revision_of="base")
    assert v.validate(good).accepted
