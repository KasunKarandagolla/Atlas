from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.s3_native_cadence import (
    MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
    S3_M1_ACCOUNTING_CHECKPOINT_TYPE,
    S3_M1_DEFAULT_MAX_LATENESS_NS,
    S3M1OriginAccountingAction,
    S3NativeM1OriginAccountingCheckpointV1,
    advance_s3_m1_origin_checkpoint,
    find_s3_m1_origin_accounting_checkpoint,
    find_s3_m1_origin_accounting_state,
    plan_s3_m1_origin_accounting,
    s3_m1_event_id,
    s3_m1_origin_metadata,
    s3_m1_origin_ref,
)

NS = 1_000_000_000
MINUTE_NS = 60 * NS
BASE_CLOSE_NS = 1_800 * MINUTE_NS


def _key(revision: str = "contract-r1") -> InstrumentKeyV2:
    return InstrumentKeyV2(
        VenueV2.BYBIT,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        sha256_json({"contract_revision": revision}),
    )


def _bar(
    close_at_ns: int,
    *,
    available_delta_ns: int = 20,
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
    final: bool = True,
    revision: str = "contract-r1",
) -> CausalBarV2:
    key = _key(revision)
    available_at_ns = close_at_ns + available_delta_ns
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id="BYBIT_PUBLIC_WS",
        event_type="BAR_1M",
        event_at_ns=close_at_ns if final else None,
        received_at_ns=close_at_ns + 10,
        ingested_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        translation_version="fixture-bars-v1",
        payload={"close": str(close_at_ns), "final": final},
        availability_class=availability_class,
        replay_available_at_ns=(
            available_at_ns + 100
            if availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET
            else None
        ),
    )
    return CausalBarV2(
        raw,
        BarIntervalV2.M1,
        close_at_ns - MINUTE_NS,
        close_at_ns,
        Decimal("100"),
        Decimal("101"),
        Decimal("99"),
        Decimal("100"),
        Decimal("1"),
        final,
    )


def _event_entry(key: InstrumentKeyV2, bar: CausalBarV2) -> ArtifactIndexEntryV2:
    trigger_ref = "b" * 64
    cutoff_ns = bar.raw.available_at_ns
    event = {
        "event_id": s3_m1_event_id(key, bar.close_at_ns),
        "event_type": "CONFIRMED_1M_CLOSE",
        "source_id": "BYBIT_PUBLIC_WS",
        "trigger_ref": trigger_ref,
        "source_event_at_ns": bar.close_at_ns,
        "source_published_at_ns": None,
        "received_at_ns": bar.raw.received_at_ns,
        "available_at_ns": cutoff_ns,
        "information_cutoff_ns": cutoff_ns,
        "deadline_ns": bar.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS,
        "causal_input_refs": sorted({bar.content_hash, trigger_ref}),
    }
    event_hash = sha256_json(event)
    return ArtifactIndexEntryV2(
        event_hash,
        "OpsDecisionEventSourceV1",
        event_hash,
        cutoff_ns,
        cutoff_ns,
        {
            "event": event,
            **s3_m1_origin_metadata(key, bar.close_at_ns, bar_ref=bar.content_hash),
        },
    )


def _late_gate_entry(
    key: InstrumentKeyV2,
    bar: CausalBarV2,
    *,
    observed_at_ns: int,
) -> ArtifactIndexEntryV2:
    origin = s3_m1_origin_metadata(key, bar.close_at_ns, bar_ref=bar.content_hash)[
        "native_m1_origin"
    ]
    assert isinstance(origin, dict)
    origin_ref = str(origin["origin_ref"])
    body = {
        "version": "OPS_PUBLIC_ACQUISITION_DEADLINE_GATE_V1",
        "observed_at_ns": observed_at_ns,
        "eligible_cutoff_ns": bar.close_at_ns,
        "event_ids": [s3_m1_event_id(key, bar.close_at_ns)],
        "deadlines_ns": [bar.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS],
        "status": "TEST GATE",
        "reason_code": "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE",
        "native_m1_origin_ref": origin_ref,
        "native_m1_bar_ref": bar.content_hash,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    return ArtifactIndexEntryV2(
        ref,
        "OpsPublicAcquisitionDeadlineGateV1",
        ref,
        observed_at_ns,
        observed_at_ns,
        {
            "deadline_gate": body,
            "native_m1_origin_ref": origin_ref,
            "native_m1_origin": origin,
        },
    )


def _observation_index(
    key: InstrumentKeyV2,
    close_at_ns: int,
    *,
    available_at_ns: int,
    record_id: str,
    bar_ref: str | None = None,
    availability_class: str = "ACTUAL_SYSTEM",
) -> ArtifactIndexEntryV2:
    metadata = {
        "record_id": record_id,
        "source_id": "BYBIT_PUBLIC_WS",
        "event_type": "BAR_1M",
        "instrument_revision": key.contract_revision,
        "instrument_key_json": key.to_canonical_json(),
        "event_at_ns": close_at_ns,
        "published_at_ns": None,
        "translation_version": "fixture-bars-v1",
        "revision_of": None,
        "quality_flags": [],
        "availability_class": availability_class,
        "replay_available_at_ns": None,
        "raw_payload_hash": "a" * 64,
        "bar_content_hash": bar_ref,
    }
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
    return ArtifactIndexEntryV2(
        ref,
        "PublicObservationIndexV2",
        sha256_json({"record_id": record_id}),
        available_at_ns,
        available_at_ns,
        metadata,
    )


def _checkpoint_entry(
    checkpoint: S3NativeM1OriginAccountingCheckpointV1,
) -> ArtifactIndexEntryV2:
    return ArtifactIndexEntryV2(
        checkpoint.content_hash,
        S3_M1_ACCOUNTING_CHECKPOINT_TYPE,
        checkpoint.content_hash,
        checkpoint.created_at_ns,
        checkpoint.available_at_ns,
        {"checkpoint": checkpoint.to_dict()},
    )


def test_delayed_three_minute_batch_is_oldest_first_and_keeps_deadlines_fixed():
    key = _key()
    bars = tuple(_bar(BASE_CLOSE_NS + offset * MINUTE_NS) for offset in range(3))
    now_ns = bars[-1].close_at_ns + 2 * NS

    plans = plan_s3_m1_origin_accounting(bars, key, now_ns=now_ns)

    assert [item.close_at_ns for item in plans] == [item.close_at_ns for item in bars]
    assert [item.action for item in plans] == [
        S3M1OriginAccountingAction.CREATE_LATE_TEST_GATE,
        S3M1OriginAccountingAction.CREATE_LATE_TEST_GATE,
        S3M1OriginAccountingAction.CREATE_TIMELY_EVENT,
    ]
    assert all(
        plan.deadline_ns == plan.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS
        for plan in plans
    )
    assert [item.origin_ref for item in plans] == [
        s3_m1_origin_ref(key, item.close_at_ns) for item in bars
    ]


def test_page_bound_drains_backlog_oldest_first_and_new_bars_do_not_starve_it():
    key = _key()
    bars = tuple(
        _bar(BASE_CLOSE_NS + offset * MINUTE_NS)
        for offset in range(MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE * 2 + 3)
    )
    now_ns = bars[-1].close_at_ns + 2 * NS
    page1 = plan_s3_m1_origin_accounting(bars, key, now_ns=now_ns)
    assert len(page1) == MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE
    assert [item.close_at_ns for item in page1] == [
        item.close_at_ns for item in bars[:MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE]
    ]

    new_bar = _bar(bars[-1].close_at_ns + MINUTE_NS)
    cursor = page1[-1].close_at_ns
    page2 = plan_s3_m1_origin_accounting(
        (*bars, new_bar), key, now_ns=now_ns, after_close_at_ns=cursor,
    )
    assert len(page2) == MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE
    assert [item.close_at_ns for item in page2] == [
        item.close_at_ns for item in bars[MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE:
                                         MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE * 2]
    ]
    page3 = plan_s3_m1_origin_accounting(
        (*bars, new_bar),
        key,
        now_ns=new_bar.close_at_ns + 2 * NS,
        after_close_at_ns=page2[-1].close_at_ns,
    )
    assert [item.close_at_ns for item in page3] == [
        *(item.close_at_ns for item in bars[MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE * 2:]),
        new_bar.close_at_ns,
    ]


def test_restart_after_gate_or_event_before_checkpoint_reuses_same_origin():
    key = _key()
    late_bar = _bar(BASE_CLOSE_NS)
    late_now = late_bar.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS + 1
    gate = _late_gate_entry(key, late_bar, observed_at_ns=late_now)
    assert find_s3_m1_origin_accounting_state((), (gate,), key, late_bar.close_at_ns) == (
        S3M1OriginAccountingAction.REUSE_LATE_TEST_GATE,
        gate,
    )
    replayed_late = plan_s3_m1_origin_accounting(
        (late_bar,), key, now_ns=late_now + MINUTE_NS, gate_entries=(gate,),
    )
    assert replayed_late[0].action == S3M1OriginAccountingAction.REUSE_LATE_TEST_GATE
    assert replayed_late[0].existing_accounting_ref == gate.artifact_ref

    timely_bar = _bar(BASE_CLOSE_NS + 3 * MINUTE_NS)
    event = _event_entry(key, timely_bar)
    replayed_event = plan_s3_m1_origin_accounting(
        (timely_bar,),
        key,
        now_ns=timely_bar.raw.available_at_ns + 1,
        event_entries=(event,),
    )
    assert replayed_event[0].action == S3M1OriginAccountingAction.REUSE_TIMELY_EVENT
    assert replayed_event[0].existing_accounting_ref == event.artifact_ref


def test_conflicting_event_and_late_gate_for_same_origin_fails_closed():
    key = _key()
    bar = _bar(BASE_CLOSE_NS)
    event = _event_entry(key, bar)
    gate = _late_gate_entry(
        key,
        bar,
        observed_at_ns=bar.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS + 1,
    )

    with pytest.raises(ValueError, match="both an immutable event and a contradictory late gate"):
        plan_s3_m1_origin_accounting(
            (bar,),
            key,
            now_ns=bar.raw.available_at_ns + 1,
            event_entries=(event,),
            gate_entries=(gate,),
        )


def test_nonfinal_reconstructed_and_missing_minutes_are_not_fabricated():
    key = _key()
    bars = (
        _bar(BASE_CLOSE_NS),
        _bar(BASE_CLOSE_NS + 2 * MINUTE_NS),
        _bar(BASE_CLOSE_NS + 3 * MINUTE_NS, final=False),
        _bar(
            BASE_CLOSE_NS + 4 * MINUTE_NS,
            availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
        ),
    )
    plans = plan_s3_m1_origin_accounting(
        bars, key, now_ns=BASE_CLOSE_NS + 5 * MINUTE_NS,
    )

    assert [item.close_at_ns for item in plans] == [
        BASE_CLOSE_NS,
        BASE_CLOSE_NS + 2 * MINUTE_NS,
    ]
    assert all(item.action == S3M1OriginAccountingAction.CREATE_LATE_TEST_GATE for item in plans)


def test_checkpoint_is_monotone_restart_safe_and_bound_to_full_revision():
    key = _key()
    first_close = BASE_CLOSE_NS
    later_close = first_close + MINUTE_NS
    first = advance_s3_m1_origin_checkpoint(
        None,
        key,
        now_ns=first_close + 10 * NS,
        source_available_through_ns=first_close + 10 * NS,
        next_close_cursor_ns=first_close,
        accounted_close_at_ns=first_close,
        has_more=True,
    )
    second = advance_s3_m1_origin_checkpoint(
        first,
        key,
        now_ns=first_close + 11 * NS,
        source_available_through_ns=first.source_available_through_ns,
        next_close_cursor_ns=None,
        accounted_close_at_ns=later_close,
        has_more=False,
    )
    found = find_s3_m1_origin_accounting_checkpoint(
        (_checkpoint_entry(first), _checkpoint_entry(second)),
        key,
    )
    assert found == second
    assert second.previous_checkpoint_ref == first.content_hash
    assert second.last_accounted_close_at_ns == later_close
    assert second.scan_complete is True

    with pytest.raises(ValueError, match="cannot cross instrument revisions"):
        advance_s3_m1_origin_checkpoint(
            second,
            _key("contract-r2"),
            now_ns=second.available_at_ns + 1,
            source_available_through_ns=second.source_available_through_ns + 1,
            next_close_cursor_ns=None,
            accounted_close_at_ns=None,
            has_more=False,
        )
    assert s3_m1_origin_ref(key, first_close) != s3_m1_origin_ref(
        _key("contract-r2"), first_close,
    )


def test_checkpoint_cursor_cannot_rebase_or_skip_prior_unaccounted_page():
    key = _key()
    close = BASE_CLOSE_NS
    checkpoint = advance_s3_m1_origin_checkpoint(
        None,
        key,
        now_ns=close + 10 * NS,
        source_available_through_ns=close + 10 * NS,
        next_close_cursor_ns=close,
        accounted_close_at_ns=close,
        has_more=True,
    )
    with pytest.raises(ValueError, match="cannot be rebased"):
        advance_s3_m1_origin_checkpoint(
            checkpoint,
            key,
            now_ns=checkpoint.available_at_ns + NS,
            source_available_through_ns=checkpoint.source_available_through_ns + NS,
            next_close_cursor_ns=close + MINUTE_NS,
            accounted_close_at_ns=close + MINUTE_NS,
            has_more=True,
        )
    with pytest.raises(ValueError, match="did not advance"):
        advance_s3_m1_origin_checkpoint(
            checkpoint,
            key,
            now_ns=checkpoint.available_at_ns + NS,
            source_available_through_ns=checkpoint.source_available_through_ns,
            next_close_cursor_ns=close,
            accounted_close_at_ns=close,
            has_more=True,
        )


def test_repository_source_page_is_bounded_exact_and_oldest_first(tmp_path):
    key = _key()
    first_close = BASE_CLOSE_NS
    second_close = first_close + MINUTE_NS
    third_close = second_close + MINUTE_NS
    rows = (
        _observation_index(key, second_close, available_at_ns=second_close + 20, record_id="second",
                           bar_ref="b" * 64),
        _observation_index(key, first_close, available_at_ns=first_close + 20, record_id="first-revision",
                           bar_ref="c" * 64),
        _observation_index(key, first_close, available_at_ns=first_close + 30, record_id="first-later-revision",
                           bar_ref="d" * 64),
        _observation_index(key, third_close, available_at_ns=third_close + 20, record_id="third",
                           bar_ref="e" * 64),
        _observation_index(_key("contract-r2"), first_close,
                           available_at_ns=first_close + 20, record_id="other-revision",
                           bar_ref="f" * 64),
        _observation_index(key, first_close + 5 * MINUTE_NS,
                           available_at_ns=first_close + 5 * MINUTE_NS + 20,
                           record_id="reconstructed", bar_ref="1" * 64,
                           availability_class="RECONSTRUCTED_MARKET"),
        _observation_index(key, first_close + 6 * MINUTE_NS,
                           available_at_ns=first_close + 6 * MINUTE_NS + 20,
                           record_id="forming", bar_ref=None),
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts(rows)
        assert repository.public_observation_source_ids() == ("BYBIT_PUBLIC_WS",)
        first_page = repository.native_m1_origin_observation_page(
            key,
            available_from_ns=0,
            available_through_ns=third_close + 100,
            limit=2,
        )
        assert [entry.metadata["record_id"] for entry in first_page.entries] == [
            "first-revision",
            "second",
        ]
        assert first_page.has_more is True
        assert first_page.last_close_at_ns == second_close

        second_page = repository.native_m1_origin_observation_page(
            key,
            available_from_ns=0,
            available_through_ns=third_close + 100,
            after_close_at_ns=first_page.last_close_at_ns,
            limit=2,
        )
        assert [entry.metadata["record_id"] for entry in second_page.entries] == ["third"]
        assert second_page.has_more is False


def test_repository_source_windows_discover_old_late_arrivals_without_replaying_history(tmp_path):
    key = _key()
    older_close = BASE_CLOSE_NS
    newer_close = older_close + MINUTE_NS
    older_late_available = newer_close + 20
    first_window = _observation_index(
        key, newer_close, available_at_ns=newer_close + 10, record_id="newer-first",
        bar_ref="2" * 64,
    )
    later_window_old_close = _observation_index(
        key, older_close, available_at_ns=older_late_available, record_id="late-older-close",
        bar_ref="3" * 64,
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifact(first_window)
        page1 = repository.native_m1_origin_observation_page(
            key,
            available_from_ns=0,
            available_through_ns=newer_close + 10,
            limit=4,
        )
        assert [entry.metadata["record_id"] for entry in page1.entries] == ["newer-first"]
        assert page1.has_more is False

        repository.register_artifact(later_window_old_close)
        page2 = repository.native_m1_origin_observation_page(
            key,
            available_from_ns=newer_close + 10,
            available_through_ns=older_late_available,
            limit=4,
        )
        assert [entry.metadata["record_id"] for entry in page2.entries] == [
            "late-older-close",
            "newer-first",
        ]
        assert page2.last_close_at_ns == newer_close


def test_pending_event_lookup_is_bounded_and_skips_durable_receipts(tmp_path):
    key = _key()
    first = _event_entry(key, _bar(BASE_CLOSE_NS))
    second = _event_entry(key, _bar(BASE_CLOSE_NS + MINUTE_NS))
    first_event_id = first.metadata["event"]["event_id"]
    identity_body = {"event_id": first_event_id, "receipt_ref": "c" * 64}
    identity_ref = sha256_json({
        "artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": first_event_id,
    })
    receipt_identity = ArtifactIndexEntryV2(
        identity_ref,
        "OpsSupervisorReceiptIdentityV1",
        sha256_json(identity_body),
        first.available_at_ns + 1,
        first.available_at_ns + 1,
        identity_body,
    )

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts((first, second, receipt_identity))
        page = repository.pending_decision_event_page(
            as_of_ns=second.available_at_ns + 1,
            limit=1,
        )
        assert tuple(entry.artifact_ref for entry in page.entries) == (second.artifact_ref,)
        assert page.has_more is False
        assert page.invalid_entry_count == 0
