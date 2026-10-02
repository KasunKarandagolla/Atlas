from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.m15_origin_accounting import (
    M15_ORIGIN_MAX_LATENESS_NS,
    M15OpportunityMissingnessV1,
    M15OriginAccountingAction,
    M15OriginAccountingCheckpointV1,
    M15OriginAccountingRecordV1,
    advance_m15_origin_checkpoint,
    m15_origin_ref,
    plan_m15_origin_accounting,
)

STEP = BarIntervalV2.M15.duration_ns
BASE = 100 * STEP


def key(revision: str = "first") -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                           "BTCUSDT", "BTC", "USDT", "USDT", sha256_json(revision))


def bar(close: int, *, delay: int = 10, final: bool = True,
        view: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM) -> CausalBarV2:
    raw = RawObservationV2.build(instrument_revision=key().contract_revision, source_id="FIXTURE_PUBLIC",
        event_type="BAR_15M", event_at_ns=close, received_at_ns=close + 5, ingested_at_ns=close + delay,
        available_at_ns=close + delay, translation_version="fixture", payload={"close": close},
        availability_class=view, replay_available_at_ns=close + delay if view != AvailabilityClassV2.ACTUAL_SYSTEM else None)
    return CausalBarV2(raw, BarIntervalV2.M15, close - STEP, close,
                      Decimal(100), Decimal(101), Decimal(99), Decimal(100), Decimal(1), final)


def index(value: CausalBarV2) -> ArtifactIndexEntryV2:
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": value.raw.record_id})
    return ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", value.raw.content_hash,
        value.raw.received_at_ns, value.raw.available_at_ns,
        {"instrument_key_json": key().to_canonical_json(), "bar_content_hash": value.content_hash,
         "source_id": value.raw.source_id})


def health(at: int, *, healthy: bool = True) -> PublicSourceHealthV2:
    return PublicSourceHealthV2("FIXTURE_PUBLIC", at, at,
        PublicSourceStateV2.HEALTHY_CURRENT if healthy else PublicSourceStateV2.DISCONNECTED,
        sha256_json({"at": at, "healthy": healthy}), "fixture")


def product() -> object:
    from atlas.v2.instruments import ProductContractV2, TradingStatusV2

    return ProductContractV2(key=key(), effective_at_ns=1, observed_at_ns=1, available_at_ns=1,
        base_units_per_contract=Decimal(1), tick_size=Decimal("0.1"), qty_step=Decimal("0.01"),
        min_qty=Decimal("0.01"), min_notional=None, max_qty=None,
        trading_status=TradingStatusV2.TRADING, metadata_ref=sha256_json("metadata"))


def test_delayed_batch_accounts_every_received_origin_oldest_first_without_timely_fabrication() -> None:
    bars = tuple(bar(BASE + offset * STEP) for offset in range(3))
    plans = plan_m15_origin_accounting(bars, key(), now_ns=bars[-1].close_at_ns + 20,
        observation_entries=tuple(index(row) for row in bars), health_entries=(health(BASE),), product=product())  # type: ignore[arg-type]
    assert [plan.bar.close_at_ns for plan in plans] == [row.close_at_ns for row in bars]
    assert [plan.action for plan in plans] == [M15OriginAccountingAction.RECORD_MISSINGNESS,
        M15OriginAccountingAction.RECORD_MISSINGNESS, M15OriginAccountingAction.ATTEMPT_TIMELY_EVENT]
    assert all(plan.missingness is None or plan.missingness.status == "NOT ESTIMABLE" for plan in plans)
    assert plans[0].missingness.origin_eligibility_deadline_ns == BASE + M15_ORIGIN_MAX_LATENESS_NS  # type: ignore[union-attr]


@pytest.mark.parametrize("failure", ["late_receipt", "unhealthy", "future_health", "missing_health", "future_metadata"])
def test_each_missing_prerequisite_retains_explicit_causal_missingness(failure: str) -> None:
    row = bar(BASE, delay=M15_ORIGIN_MAX_LATENESS_NS + 1 if failure == "late_receipt" else 10)
    source_health = () if failure == "missing_health" else (
        health(BASE + (20 if failure == "future_health" else 1), healthy=failure != "unhealthy"),)
    contract = product()
    if failure == "future_metadata":
        contract = replace(contract, observed_at_ns=BASE + 20, available_at_ns=BASE + 20)  # type: ignore[type-var]
    plans = plan_m15_origin_accounting((row,), key(), now_ns=row.raw.available_at_ns,
        observation_entries=(index(row),), health_entries=source_health, product=contract)  # type: ignore[arg-type]
    missing = plans[0].missingness
    assert missing is not None and missing.status == "NOT ESTIMABLE" and missing.authority == "ZERO"
    assert M15OpportunityMissingnessV1.from_dict(missing.to_dict()) == missing
    assert missing.observed_at_ns == row.raw.available_at_ns
    assert missing.observation_index_ref == index(row).artifact_ref
    if failure == "future_health":
        assert missing.source_health_ref is None


def test_boundary_is_inclusive_and_later_recovery_never_rescues_original_health() -> None:
    row = bar(BASE)
    unavailable = health(BASE, healthy=False)
    recovered = health(row.raw.available_at_ns + 1)
    plans = plan_m15_origin_accounting((row,), key(), now_ns=BASE + M15_ORIGIN_MAX_LATENESS_NS,
        observation_entries=(index(row),), health_entries=(unavailable, recovered), product=product())  # type: ignore[arg-type]
    assert plans[0].missingness.reason_code == "M15_SOURCE_UNHEALTHY_AT_ORIGIN"  # type: ignore[union-attr]
    assert plans[0].missingness.source_health_ref == unavailable.content_hash  # type: ignore[union-attr]
    timely = plan_m15_origin_accounting((row,), key(), now_ns=BASE + M15_ORIGIN_MAX_LATENESS_NS,
        observation_entries=(index(row),), health_entries=(health(BASE),), product=product())  # type: ignore[arg-type]
    assert timely[0].action == M15OriginAccountingAction.ATTEMPT_TIMELY_EVENT


def test_revision_isolation_nonfinal_reconstructed_future_and_absent_origins() -> None:
    rows = (bar(BASE), bar(BASE + 2 * STEP), bar(BASE + 3 * STEP, final=False),
            bar(BASE + 4 * STEP, view=AvailabilityClassV2.RECONSTRUCTED_MARKET), bar(BASE + 8 * STEP))
    plans = plan_m15_origin_accounting(rows, key(), now_ns=BASE + 5 * STEP,
        observation_entries=tuple(index(row) for row in rows))
    assert [plan.bar.close_at_ns for plan in plans] == [BASE, BASE + 2 * STEP]
    assert m15_origin_ref(key(), BASE) != m15_origin_ref(key("new_revision"), BASE)
    assert plan_m15_origin_accounting(rows, key("new_revision"), now_ns=BASE + 5 * STEP,
                                    observation_entries=tuple(index(row) for row in rows)) == ()


def test_bounded_pages_drain_oldest_first_and_validate_exact_index() -> None:
    rows = tuple(bar(BASE + n * STEP) for n in range(9))
    indexes = tuple(index(row) for row in rows)
    first = plan_m15_origin_accounting(rows, key(), now_ns=BASE + 10 * STEP, observation_entries=indexes)
    second = plan_m15_origin_accounting(rows, key(), now_ns=BASE + 10 * STEP, observation_entries=indexes,
                                       after_close_at_ns=first[-1].bar.close_at_ns)
    assert len(first) == len(second) == 4
    assert first[-1].bar.close_at_ns < second[0].bar.close_at_ns
    with pytest.raises(ValueError, match="exact indexed"):
        plan_m15_origin_accounting((rows[0],), key(), now_ns=BASE + 10 * STEP,
                                   observation_entries=(replace(indexes[0], content_hash="a" * 64),))


def test_restart_reuses_accounting_and_rejects_conflicting_or_backdated_markers() -> None:
    row = bar(BASE)
    plan = plan_m15_origin_accounting((row,), key(), now_ns=BASE + STEP, observation_entries=(index(row),))[0]
    assert plan.missingness is not None
    record = M15OriginAccountingRecordV1(key(), BASE, plan.origin_ref, row.content_hash,
        plan.observation_index_ref, plan.missingness.content_hash, plan.missingness.VERSION, BASE + STEP)
    entry = ArtifactIndexEntryV2(record.content_hash, record.VERSION, record.content_hash,
        record.observed_at_ns, record.observed_at_ns,
        {"accounting": record.to_dict(), "m15_origin_ref": record.origin_ref})
    replay = plan_m15_origin_accounting((row,), key(), now_ns=BASE + 2 * STEP,
        observation_entries=(index(row),), existing_entries=(entry,))[0]
    assert replay.action == M15OriginAccountingAction.REUSE_DURABLE_ORIGIN
    assert replay.existing_accounting_ref == entry.artifact_ref
    with pytest.raises(ValueError, match="contradictory"):
        plan_m15_origin_accounting((row,), key(), now_ns=BASE + 2 * STEP,
            observation_entries=(index(row),), existing_entries=(entry, entry))
    with pytest.raises(ValueError, match="identity or availability"):
        plan_m15_origin_accounting((row,), key(), now_ns=BASE + 20,
            observation_entries=(index(row),), existing_entries=(entry,))
    with pytest.raises(ValueError, match="cannot be backdated"):
        replace(plan.missingness, observed_at_ns=row.raw.available_at_ns - 1)
    with pytest.raises(ValueError, match="deadline cannot move"):
        replace(plan.missingness, origin_eligibility_deadline_ns=BASE + 10 * STEP)


def test_checkpoint_keeps_active_source_window_and_resets_cursor_for_late_old_arrivals() -> None:
    first = advance_m15_origin_checkpoint(None, key(), now_ns=BASE + 10 * STEP,
        source_available_through_ns=BASE + 9 * STEP, next_close_cursor_ns=BASE, has_more=True)
    assert M15OriginAccountingCheckpointV1.from_dict(first.to_dict()) == first
    finished = advance_m15_origin_checkpoint(first, key(), now_ns=BASE + 11 * STEP,
        source_available_through_ns=first.source_available_through_ns, next_close_cursor_ns=BASE + STEP,
        has_more=False)
    next_window = advance_m15_origin_checkpoint(finished, key(), now_ns=BASE + 12 * STEP,
        source_available_through_ns=BASE + 12 * STEP, next_close_cursor_ns=BASE - STEP, has_more=True)
    assert next_window.source_available_from_ns == finished.source_available_through_ns
    assert next_window.source_scan_after_close_at_ns == BASE - STEP
    assert next_window.previous_checkpoint_ref == finished.content_hash
    with pytest.raises(ValueError, match="cannot be rebased"):
        advance_m15_origin_checkpoint(first, key(), now_ns=BASE + 11 * STEP,
            source_available_through_ns=BASE + 11 * STEP, next_close_cursor_ns=BASE + STEP, has_more=True)
    with pytest.raises(ValueError, match="did not advance"):
        advance_m15_origin_checkpoint(first, key(), now_ns=BASE + 11 * STEP,
            source_available_through_ns=first.source_available_through_ns, next_close_cursor_ns=BASE, has_more=True)
    with pytest.raises(ValueError, match="cannot cross"):
        advance_m15_origin_checkpoint(first, key("new"), now_ns=BASE + 11 * STEP,
            source_available_through_ns=first.source_available_through_ns, next_close_cursor_ns=BASE + STEP, has_more=True)
