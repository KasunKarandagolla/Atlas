from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.runtime.s3_native_cadence import (
    S3_M1_DEFAULT_MAX_LATENESS_NS,
    S3_M1_EVENT_TYPE,
    S3M1BarDisposition,
    S3M1ReplayDisposition,
    classify_s3_m1_bar,
    classify_s3_m1_replay,
    find_s3_m1_origin_event,
    s3_m1_event_id,
    s3_m1_origin_metadata,
    s3_m1_origin_ref,
)

NS = 1_000_000_000
MINUTE_NS = 60 * NS
CLOSE_NS = 120 * NS


def _key(revision: str = "contract-r1") -> InstrumentKeyV2:
    return InstrumentKeyV2(
        VenueV2.BYBIT,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        revision,
    )


def _bar(
    *,
    interval: BarIntervalV2 = BarIntervalV2.M1,
    final: bool = True,
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
    available_delta_ns: int = 20,
) -> CausalBarV2:
    open_at_ns = 0 if interval == BarIntervalV2.M1 else 0
    close_at_ns = interval.duration_ns
    received_at_ns = close_at_ns + 10
    available_at_ns = close_at_ns + available_delta_ns
    replay_available = available_at_ns + 100 if availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET else None
    raw = RawObservationV2.build(
        instrument_revision=_key().content_hash,
        source_id="BYBIT_PUBLIC_WS",
        event_type=f"BAR_{interval.value}",
        event_at_ns=close_at_ns if final else None,
        received_at_ns=received_at_ns,
        ingested_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        translation_version="fixture-bars-v1",
        payload={"close": "100", "interval": interval.value},
        availability_class=availability_class,
        replay_available_at_ns=replay_available,
    )
    return CausalBarV2(
        raw,
        interval,
        open_at_ns,
        close_at_ns,
        Decimal("100"),
        Decimal("101"),
        Decimal("99"),
        Decimal("100"),
        Decimal("1"),
        final,
    )


def _event_and_entry(
    *,
    key: InstrumentKeyV2 | None = None,
    close_at_ns: int = CLOSE_NS,
    bar_ref: str = "a" * 64,
    event_at_ns: int | None = None,
) -> ArtifactIndexEntryV2:
    exact_key = key or _key()
    trigger_ref = "b" * 64
    cutoff_ns = close_at_ns + 20
    event = OpsDecisionEventV1(
        s3_m1_event_id(exact_key, close_at_ns),
        S3_M1_EVENT_TYPE,
        "BYBIT_PUBLIC_WS",
        trigger_ref,
        event_at_ns if event_at_ns is not None else close_at_ns,
        None,
        close_at_ns + 10,
        cutoff_ns,
        cutoff_ns,
        cutoff_ns + S3_M1_DEFAULT_MAX_LATENESS_NS,
        tuple(sorted({bar_ref, trigger_ref})),
    )
    metadata = {
        "event": event.to_dict(),
        **s3_m1_origin_metadata(exact_key, close_at_ns, bar_ref=bar_ref),
        "trigger_record_id": "first-raw-bar-record",
    }
    return ArtifactIndexEntryV2(
        event.content_hash,
        "OpsDecisionEventSourceV1",
        event.content_hash,
        event.information_cutoff_ns,
        event.information_cutoff_ns,
        metadata,
    )


def test_native_m1_identity_is_exact_key_and_close_not_payload():
    key = _key()
    event_id = s3_m1_event_id(key, CLOSE_NS)

    # No raw payload or recovery timestamp participates in the identity.
    assert event_id == s3_m1_event_id(key, CLOSE_NS)
    assert event_id != s3_m1_event_id(_key("contract-r2"), CLOSE_NS)
    assert event_id != s3_m1_event_id(key, CLOSE_NS + MINUTE_NS)
    assert s3_m1_origin_ref(key, CLOSE_NS) != s3_m1_origin_ref(_key("contract-r2"), CLOSE_NS)


def test_origin_lookup_reuses_first_exact_event_after_revised_payload():
    key = _key()
    first = _event_and_entry(key=key, bar_ref="a" * 64)
    # The lookup key contains no raw record ID or revised bar body, so it
    # returns the original immutable event bound to its first bar.
    reused = find_s3_m1_origin_event((first,), key, CLOSE_NS)

    assert reused is first
    assert reused.metadata["event"]["event_id"] == s3_m1_event_id(key, CLOSE_NS)
    assert reused.metadata["native_m1_origin"]["bar_ref"] == "a" * 64


def test_origin_lookup_ignores_other_event_types_and_old_m15_entries():
    key = _key()
    unrelated = ArtifactIndexEntryV2(
        "c" * 64,
        "OpsDecisionEventSourceV1",
        "c" * 64,
        1,
        1,
        {"event": {"event_id": "d" * 64, "event_type": "CONFIRMED_15M_CLOSE"}},
    )
    assert find_s3_m1_origin_event((unrelated,), key, CLOSE_NS) is None


def test_origin_lookup_rejects_unbound_or_corrupt_native_event():
    key = _key()
    good = _event_and_entry(key=key)
    body = dict(good.metadata["event"])
    body["event_id"] = "f" * 64
    corrupt_hash = sha256_json(body)
    corrupt = ArtifactIndexEntryV2(
        corrupt_hash,
        "OpsDecisionEventSourceV1",
        corrupt_hash,
        good.available_at_ns,
        good.available_at_ns,
        {**good.metadata, "event": body},
    )
    with pytest.raises(ValueError, match="conflicting event identity"):
        find_s3_m1_origin_event((corrupt,), key, CLOSE_NS)

    missing_origin = ArtifactIndexEntryV2(
        good.artifact_ref,
        "OpsDecisionEventSourceV1",
        good.content_hash,
        good.created_at_ns,
        good.available_at_ns,
        {"event": good.metadata["event"]},
    )
    with pytest.raises(ValueError, match="missing its durable origin metadata"):
        find_s3_m1_origin_event((missing_origin,), key, CLOSE_NS)


def test_one_origin_with_conflicting_durable_event_content_fails_closed():
    key = _key()
    first = _event_and_entry(key=key, event_at_ns=CLOSE_NS)
    conflicting = _event_and_entry(key=key, event_at_ns=CLOSE_NS + 1)
    assert first.metadata["event"]["event_id"] == conflicting.metadata["event"]["event_id"]
    assert first.artifact_ref != conflicting.artifact_ref

    with pytest.raises(ValueError, match="multiple immutable decision events"):
        find_s3_m1_origin_event((first, conflicting), key, CLOSE_NS)


@pytest.mark.parametrize(
    ("bar", "now_delta", "expected"),
    [
        (_bar(), 20, S3M1BarDisposition.TIMELY),
        (_bar(), 19, S3M1BarDisposition.FUTURE_AVAILABLE),
        (_bar(), S3_M1_DEFAULT_MAX_LATENESS_NS, S3M1BarDisposition.TIMELY),
        (_bar(), S3_M1_DEFAULT_MAX_LATENESS_NS + 1, S3M1BarDisposition.LATE),
        (_bar(available_delta_ns=S3_M1_DEFAULT_MAX_LATENESS_NS + 1), 5_000_000_001,
         S3M1BarDisposition.LATE),
        (_bar(interval=BarIntervalV2.M15), 20, S3M1BarDisposition.WRONG_INTERVAL),
        (_bar(final=False), 20, S3M1BarDisposition.NOT_FINAL),
        (_bar(availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET), 20,
         S3M1BarDisposition.NON_ACTUAL_AVAILABILITY),
    ],
)
def test_native_m1_first_event_classification_is_fail_closed(bar, now_delta, expected):
    assert classify_s3_m1_bar(bar, now_ns=bar.close_at_ns + now_delta) == expected


def test_persisted_event_replay_uses_original_deadline_without_rebasing():
    event = SimpleNamespace(
        event_id=s3_m1_event_id(_key(), CLOSE_NS),
        event_type=S3_M1_EVENT_TYPE,
        available_at_ns=CLOSE_NS + 20,
        deadline_ns=CLOSE_NS + 20 + 5_000,
    )

    assert classify_s3_m1_replay(event, now_ns=event.available_at_ns - 1) == (
        S3M1ReplayDisposition.NOT_YET_AVAILABLE
    )
    assert classify_s3_m1_replay(event, now_ns=event.deadline_ns) == S3M1ReplayDisposition.REUSE
    assert classify_s3_m1_replay(event, now_ns=event.deadline_ns + 1) == S3M1ReplayDisposition.EXPIRED
    assert classify_s3_m1_replay(
        event, now_ns=event.deadline_ns + 100_000, already_processed=True
    ) == S3M1ReplayDisposition.ALREADY_PROCESSED
    # Expiry remains tied to this same stored deadline on every later restart.
    assert classify_s3_m1_replay(event, now_ns=event.deadline_ns + 500_000) == S3M1ReplayDisposition.EXPIRED


def test_nonpositive_lateness_window_is_rejected():
    with pytest.raises(ValueError, match="positive integer"):
        classify_s3_m1_bar(_bar(), now_ns=CLOSE_NS + 20, max_lateness_ns=0)
