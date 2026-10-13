"""Adversarial regressions for the separately qualified V1 Phase-4 path."""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from decimal import Decimal

import pytest

from atlas.data.archive import ParquetArchive
from atlas.data.models import DataKind, MarketRecord, hash_payload, make_record
from atlas.domain.enums import AvailabilityClass
from atlas.scanner import Phase4HandoffRequest, WarmupState, evaluator_from_phase4
from atlas.science.phase4_engine import DecisionStatus, evaluate_phase4, evaluate_phase4_qualified
from atlas.science.phase4_persistence import (
    EVALUATION,
    FEATURE_SNAPSHOT_SOURCE_PROOF,
    TRADE_PLAN_EVIDENCE,
    Phase4ArtifactSet,
    persist_phase4_artifacts,
)
from atlas.science.research_archive import ResearchArtifactArchive
from atlas.strategy.crypto_trend_24h_v1 import SNAPSHOT_DEADLINE_NS, FeatureSnapshot, FeatureSnapshotEvidenceV1
from atlas.strategy.features import HOUR_NS, FeatureValues
from atlas.strategy.source_proof import (
    FeatureSnapshotSourceProofV1,
    ParquetFeatureSourceStore,
    SourceAvailabilityMode,
    SourceProofError,
    build_snapshot_with_source_proof,
    verify_snapshot_source_proof,
)
from tests.support.phase4_factory import decision_input, market_inputs, policy

SLOT = 4 * HOUR_NS


def _claimed_receipt(count: int, *, snapshot=None):
    case = decision_input()
    value = snapshot or case.snapshot
    assert value is not None
    ids = tuple(f"claimed-{index}" for index in range(count))
    needed_slot = max(SLOT, (count - 1) * HOUR_NS)
    slot = ((needed_slot + 4 * HOUR_NS - 1) // (4 * HOUR_NS)) * (4 * HOUR_NS)
    value = replace(
        value,
        slot_at_ns=slot,
        computed_at_ns=slot + 1,
        values=replace(value.values, end_at_ns=slot),
        source_record_ids=ids,
    )
    ends = tuple(slot - (count - 1 - index) * HOUR_NS for index in range(count))
    try:
        return value, FeatureSnapshotEvidenceV1(
            snapshot_hash=value.snapshot_hash(),
            instrument=value.instrument,
            slot_at_ns=value.slot_at_ns,
            computed_at_ns=value.computed_at_ns,
            source_record_ids=ids,
            source_end_at_ns=ends,
            source_available_at_ns=ends,
        )
    except ValueError:
        return value, None


@pytest.mark.parametrize("count", (0, 1, 2, 720, 722))
def test_non_721_caller_claims_never_match_a_complete_source_window(count: int) -> None:
    snapshot, receipt = _claimed_receipt(count)

    assert receipt is None or not receipt.validates(snapshot)


def test_two_id_phase4_factory_does_not_manufacture_a_complete_window_receipt() -> None:
    case = decision_input()

    assert case.snapshot is not None
    assert len(case.snapshot.source_record_ids) == 2
    assert case.snapshot_evidence is None or not case.snapshot_evidence.validates(case.snapshot)


def test_even_a_721_id_legacy_declaration_is_not_source_proof() -> None:
    snapshot, receipt = _claimed_receipt(721)

    assert receipt is not None
    assert receipt.matches_declarations(snapshot)
    assert not receipt.validates(snapshot)


def test_same_legacy_hash_with_changed_returns_or_variance_is_not_the_same_evidence() -> None:
    case = decision_input()
    assert case.snapshot is not None
    receipt_snapshot, receipt = _claimed_receipt(721, snapshot=case.snapshot)
    assert receipt is not None

    changed_values = replace(
        case.snapshot.values,
        end_at_ns=receipt_snapshot.slot_at_ns,
        returns=(0.25, *case.snapshot.values.returns[1:]),
        variance=case.snapshot.values.variance + 0.5,
    )
    changed_snapshot = replace(receipt_snapshot, values=changed_values)

    assert changed_snapshot.snapshot_hash() == receipt_snapshot.snapshot_hash()
    assert receipt.matches_declarations(changed_snapshot)
    assert not receipt.validates(changed_snapshot)


def test_historical_phase4_evaluation_reproduces_without_new_receipt() -> None:
    case = decision_input(snapshot_evidence=None)

    evaluation = evaluate_phase4(case)

    assert evaluation.status is DecisionStatus.TRADE_CANDIDATE
    assert evaluation.trade_plan is not None
    assert evaluation.trade_plan.plan_hash() == "ec8e89ac4a7750c9cc2f968e526bf4f5b70233def3bea91fc90708f600e86856"


def _make_hourly_records(
    *,
    slot: int,
    count: int = 721,
    instrument: str = "BTCUSDT",
    availability_class: AvailabilityClass = AvailabilityClass.ACTUAL_OBSERVED,
    late_last_by_ns: int = 0,
) -> tuple[MarketRecord, ...]:
    first_index = 721 - count
    records: list[MarketRecord] = []
    for index in range(first_index, 721):
        end_at_ns = slot - (720 - index) * HOUR_NS
        received = end_at_ns + 1_000_000
        replay_available = None
        replay_method = None
        lower = upper = None
        if availability_class is AvailabilityClass.RECONSTRUCTED_PUBLIC:
            replay_available = end_at_ns + 1_000_000
            replay_method = "bounded-test-rule@1"
            lower = upper = replay_available
            received = max(slot + 40_000_000_000, replay_available + 1)
        if index == 720:
            received += late_last_by_ns
        records.append(
            make_record(
                record_id=f"hour-{index}",
                source_id="BYBIT_PUBLIC",
                venue="BYBIT",
                instrument=instrument,
                data_kind=DataKind.BAR_1H_LAST,
                payload={"close": str(Decimal("100") + Decimal(index) / Decimal("10"))},
                source_event_at_ns=end_at_ns,
                bar_start_ns=end_at_ns - HOUR_NS,
                bar_end_ns=end_at_ns,
                source_revision="revision-1",
                received_at_ns=received,
                processed_at_ns=received,
                available_at_ns=received,
                data_ingested_at_ns=received,
                recorded_at_ns=received,
                evidence_ref=f"raw:{index}",
                availability_class=availability_class,
                availability_lower_ns=lower,
                availability_upper_ns=upper,
                replay_available_at_ns=replay_available,
                availability_method=replay_method,
            )
        )
    return tuple(records)


def _write_source_archive(tmp_path, records: tuple[MarketRecord, ...]) -> ParquetFeatureSourceStore:
    archive = ParquetArchive(tmp_path / "market-archive")
    groups: dict[str, list[MarketRecord]] = {}
    for record in records:
        day = archive._day(record.source_event_at_ns or record.received_at_ns)
        groups.setdefault(day, []).append(record)
    for group in groups.values():
        archive.write_batch(group)
    return ParquetFeatureSourceStore(archive.root)


def _qualified_input(snapshot, proof):
    return decision_input(
        snapshot=snapshot,
        snapshot_evidence=None,
        snapshot_source_proof=proof,
        policy=policy(slot_at_ns=snapshot.slot_at_ns, sigma=snapshot.values.sigma),
        now_ns=snapshot.computed_at_ns,
        availability_cutoff_ns=snapshot.slot_at_ns + SNAPSHOT_DEADLINE_NS,
        market_inputs=market_inputs(now_ns=snapshot.computed_at_ns),
    )


@pytest.fixture
def actual_source_window(tmp_path):
    slot = 724 * HOUR_NS
    records = _make_hourly_records(slot=slot)
    store = _write_source_archive(tmp_path, records)
    snapshot, proof = build_snapshot_with_source_proof(
        "BTCUSDT",
        slot,
        slot + 1_000_000_000,
        store,
    )
    return slot, store, snapshot, proof, records


@pytest.mark.parametrize("count", (0, 1, 2, 720, 722))
def test_incomplete_or_oversized_archives_cannot_build_qualified_snapshot(tmp_path, count: int) -> None:
    slot = 724 * HOUR_NS
    records = _make_hourly_records(slot=slot, count=min(count, 721))
    if count == 722:
        records = (*records, replace(records[-1], record_id="extra-hour"))
    store = _write_source_archive(tmp_path, records)

    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof("BTCUSDT", slot, slot + 1_000_000_000, store)


def test_exactly_721_archived_observations_qualify_through_evaluate_and_persist(actual_source_window, tmp_path) -> None:
    slot, store, snapshot, proof, records = actual_source_window

    assert len(records) == 721
    assert len(snapshot.values.returns) == 720
    assert proof.source_record_ids == snapshot.source_record_ids
    assert verify_snapshot_source_proof(snapshot, proof, ParquetFeatureSourceStore(store.archive.root))

    class UntrustedStore(ParquetFeatureSourceStore):
        def records(self, **kwargs):
            return records

    assert not verify_snapshot_source_proof(snapshot, proof, UntrustedStore(store.archive.root))
    evaluation = evaluate_phase4_qualified(_qualified_input(snapshot, proof), store)
    assert evaluation.status is DecisionStatus.TRADE_CANDIDATE
    assert evaluation.trade_plan is not None

    request = Phase4HandoffRequest(
        slot, "BTCUSDT", slot + SNAPSHOT_DEADLINE_NS, "universe", "cheap", WarmupState.WARM_AVAILABLE
    )
    scanner_result = evaluator_from_phase4(lambda _: _qualified_input(snapshot, proof), source_store=store)(request)
    assert scanner_result.status is DecisionStatus.TRADE_CANDIDATE
    assert scanner_result.trade_plan is not None
    assert scanner_result.trade_plan.plan_hash() == evaluation.trade_plan.plan_hash()

    research = ResearchArtifactArchive(tmp_path / "research")
    written = persist_phase4_artifacts(
        research,
        Phase4ArtifactSet(
            "qualified-test",
            evaluation=evaluation,
            trade_plan_evidence=evaluation.evidence,
            snapshot_source_proof=proof,
        ),
    )
    assert EVALUATION in written and TRADE_PLAN_EVIDENCE in written
    proof_path = written[FEATURE_SNAPSHOT_SOURCE_PROOF][0]
    import pyarrow.parquet as pq

    persisted = json.loads(pq.read_table(proof_path)["payload_json"][0].as_py())
    restored = FeatureSnapshotSourceProofV1.from_dict(persisted)
    assert restored.evidence_hash() == proof.evidence_hash()
    serialized_snapshot = json.loads(json.dumps(asdict(snapshot), sort_keys=True))
    serialized_values = serialized_snapshot["values"]
    restarted_snapshot = FeatureSnapshot(
        instrument=serialized_snapshot["instrument"],
        slot_at_ns=serialized_snapshot["slot_at_ns"],
        computed_at_ns=serialized_snapshot["computed_at_ns"],
        values=FeatureValues(
            end_at_ns=serialized_values["end_at_ns"],
            returns=tuple(serialized_values["returns"]),
            variance=serialized_values["variance"],
            sigma=serialized_values["sigma"],
            z=serialized_values["z"],
        ),
        source_record_ids=tuple(serialized_snapshot["source_record_ids"]),
    )
    restarted_store = ParquetFeatureSourceStore(store.archive.root)
    assert restarted_snapshot == snapshot
    assert verify_snapshot_source_proof(restarted_snapshot, restored, restarted_store)
    assert evaluate_phase4_qualified(_qualified_input(restarted_snapshot, restored), restarted_store).trade_plan


@pytest.mark.parametrize(
    "request_change",
    (
        {"instrument": "ETHUSDT"},
        {"slot_at_ns": 728 * HOUR_NS},
        {"availability_cutoff_ns": 724 * HOUR_NS + SNAPSHOT_DEADLINE_NS - 1},
    ),
)
def test_qualified_scanner_rejects_input_not_bound_to_request(actual_source_window, request_change) -> None:
    slot, store, snapshot, proof, _ = actual_source_window
    value = _qualified_input(snapshot, proof)
    request = Phase4HandoffRequest(
        slot, "BTCUSDT", slot + SNAPSHOT_DEADLINE_NS, "universe", "cheap", WarmupState.WARM_AVAILABLE
    )

    result = evaluator_from_phase4(
        lambda _: value,
        source_store=store,
    )(replace(request, **request_change))

    assert result.status is DecisionStatus.NOT_ESTIMABLE
    assert result.trade_plan is None
    assert "scanner request identity/cutoff" in result.reasons[0]


def test_missing_archive_record_and_plausible_fabricated_ids_fail_closed(actual_source_window, tmp_path) -> None:
    _, store, snapshot, proof, _ = actual_source_window
    empty_store = ParquetFeatureSourceStore(tmp_path / "empty-archive")
    result = evaluate_phase4_qualified(_qualified_input(snapshot, proof), empty_store)

    assert result.status is DecisionStatus.NOT_ESTIMABLE
    assert result.trade_plan is None
    assert "source proof" in result.reason


def test_self_consistent_fabricated_availability_receipt_cannot_qualify(actual_source_window, tmp_path) -> None:
    _, _, snapshot, proof, _ = actual_source_window
    fabricated = replace(
        proof,
        source_record_ids=tuple(f"fabricated-{index}" for index in range(721)),
        source_ids=tuple("BYBIT_PUBLIC" for _ in range(721)),
        actual_available_at_ns=tuple(proof.source_end_at_ns),
        effective_available_at_ns=tuple(proof.source_end_at_ns),
        source_record_fingerprints=tuple("a" * 64 for _ in range(721)),
        source_content_hashes=tuple("b" * 64 for _ in range(721)),
    )
    result = evaluate_phase4_qualified(
        replace(_qualified_input(snapshot, fabricated), snapshot_source_proof=fabricated),
        ParquetFeatureSourceStore(tmp_path / "unregistered"),
    )

    assert result.status is DecisionStatus.NOT_ESTIMABLE
    assert result.trade_plan is None


def test_missing_duplicate_late_revised_and_conflicting_hours_fail(tmp_path) -> None:
    slot = 724 * HOUR_NS
    full = _make_hourly_records(slot=slot)
    variants = []
    variants.append(full[:300] + full[301:])
    duplicate = replace(full[300], record_id="duplicate-hour-300")
    variants.append((*full, duplicate))
    variants.append(_make_hourly_records(slot=slot, late_last_by_ns=SNAPSHOT_DEADLINE_NS + 1))
    source = full[300]
    revision_at = slot + 10_000_000_000
    revised = make_record(
        record_id="revised-hour-300",
        source_id=source.source_id,
        venue=source.venue,
        instrument=source.instrument,
        data_kind=source.data_kind,
        payload=source.payload,
        source_event_at_ns=source.source_event_at_ns,
        bar_start_ns=source.bar_start_ns,
        bar_end_ns=source.bar_end_ns,
        source_revision="revision-2",
        revision_of=source.record_id,
        received_at_ns=revision_at,
        processed_at_ns=revision_at,
        available_at_ns=revision_at,
        data_ingested_at_ns=revision_at,
        recorded_at_ns=revision_at,
        evidence_ref="raw:revision-2",
    )
    variants.append((*full, revised))
    conflicting = make_record(
        record_id="conflicting-hour-300",
        source_id=source.source_id,
        venue=source.venue,
        instrument=source.instrument,
        data_kind=source.data_kind,
        payload={"close": "999"},
        source_event_at_ns=source.source_event_at_ns,
        bar_start_ns=source.bar_start_ns,
        bar_end_ns=source.bar_end_ns,
        source_revision="revision-other",
        received_at_ns=source.received_at_ns,
        processed_at_ns=source.processed_at_ns,
        available_at_ns=source.available_at_ns,
        data_ingested_at_ns=source.data_ingested_at_ns,
        recorded_at_ns=source.recorded_at_ns,
        evidence_ref="raw:conflict",
    )
    variants.append((*full, conflicting))

    for index, records in enumerate(variants):
        store = _write_source_archive(tmp_path / str(index), records)
        with pytest.raises(SourceProofError):
            build_snapshot_with_source_proof("BTCUSDT", slot, slot + SNAPSHOT_DEADLINE_NS, store)


def test_future_source_publication_and_valid_from_do_not_backdate_qualification(tmp_path) -> None:
    slot = 724 * HOUR_NS
    computed = slot + 1_000_000_000
    records = _make_hourly_records(slot=slot)
    source = records[300]
    variants = []
    for field in ("source_published_at_ns", "source_valid_from_ns"):
        content = source.content_dict()
        content[field] = computed + 1
        variants.append(replace(source, **{field: computed + 1, "content_hash": hash_payload(content)}))

    for index, invalid_source in enumerate(variants):
        updated = (*records[:300], invalid_source, *records[301:])
        store = _write_source_archive(tmp_path / str(index), updated)
        with pytest.raises(SourceProofError):
            build_snapshot_with_source_proof("BTCUSDT", slot, computed, store)


def test_noncanonical_record_and_source_identities_fail(tmp_path) -> None:
    slot = 724 * HOUR_NS
    computed = slot + 1_000_000_000
    records = _make_hourly_records(slot=slot)
    source = records[300]
    variants = [replace(source, record_id=f" {source.record_id} ")]
    content = source.content_dict()
    content["source_id"] = f" {source.source_id} "
    variants.append(replace(source, source_id=f" {source.source_id} ", content_hash=hash_payload(content)))
    content = source.content_dict()
    content["source_revision"] = " "
    variants.append(replace(source, source_revision=" ", content_hash=hash_payload(content)))

    for index, invalid_source in enumerate(variants):
        updated = (*records[:300], invalid_source, *records[301:])
        store = _write_source_archive(tmp_path / str(index), updated)
        with pytest.raises(SourceProofError):
            build_snapshot_with_source_proof("BTCUSDT", slot, computed, store)


def test_deadline_equality_passes_and_deadline_plus_one_fails(tmp_path) -> None:
    slot = 724 * HOUR_NS
    computed = slot + SNAPSHOT_DEADLINE_NS
    records = list(_make_hourly_records(slot=slot))
    final = records[-1]
    records[-1] = make_record(
        record_id=final.record_id,
        source_id=final.source_id,
        venue=final.venue,
        instrument=final.instrument,
        data_kind=final.data_kind,
        payload=final.payload,
        source_event_at_ns=final.source_event_at_ns,
        bar_start_ns=final.bar_start_ns,
        bar_end_ns=final.bar_end_ns,
        source_revision=final.source_revision,
        received_at_ns=computed,
        processed_at_ns=computed,
        available_at_ns=computed,
        data_ingested_at_ns=computed,
        recorded_at_ns=computed,
        evidence_ref=final.evidence_ref,
    )
    store = _write_source_archive(tmp_path / "at-deadline", tuple(records))
    snapshot, proof = build_snapshot_with_source_proof("BTCUSDT", slot, computed, store)
    assert verify_snapshot_source_proof(snapshot, proof, store)
    at_deadline = _qualified_input(snapshot, proof)
    at_deadline = replace(
        at_deadline,
        policy=policy(slot_at_ns=slot, sigma=snapshot.values.sigma),
        now_ns=computed,
        availability_cutoff_ns=computed,
        market_inputs=market_inputs(now_ns=computed),
    )
    assert evaluate_phase4_qualified(at_deadline, store).status is DecisionStatus.TRADE_CANDIDATE

    after = replace(
        records[-1],
        record_id="late-final",
        received_at_ns=computed + 1,
        processed_at_ns=computed + 1,
        available_at_ns=computed + 1,
        data_ingested_at_ns=computed + 1,
        recorded_at_ns=computed + 1,
        content_hash=records[-1].compute_content_hash(),
    )
    late_store = _write_source_archive(tmp_path / "after-deadline", (*records[:-1], after))
    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof("BTCUSDT", slot, computed, late_store)
    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof("BTCUSDT", slot, computed + 1, store)


def test_replay_availability_class_is_explicit_and_cannot_be_backdated(tmp_path) -> None:
    slot = 724 * HOUR_NS
    reconstructed = _make_hourly_records(
        slot=slot,
        availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
    )
    store = _write_source_archive(tmp_path / "reconstructed", reconstructed)
    computed = slot + 1_000_000_000

    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof(
            "BTCUSDT", slot, computed, store, availability_mode=SourceAvailabilityMode.ACTUAL_SYSTEM
        )
    snapshot, proof = build_snapshot_with_source_proof(
        "BTCUSDT",
        slot,
        computed,
        store,
        availability_mode=SourceAvailabilityMode.RECONSTRUCTED_MARKET,
    )
    assert proof.availability_mode is SourceAvailabilityMode.RECONSTRUCTED_MARKET
    assert verify_snapshot_source_proof(snapshot, proof, ParquetFeatureSourceStore(store.archive.root))
    evaluation = evaluate_phase4_qualified(_qualified_input(snapshot, proof), store)
    assert evaluation.status is DecisionStatus.NOT_ESTIMABLE
    assert evaluation.trade_plan is None
    assert "reconstructed replay availability" in evaluation.reason


def test_revision_received_after_cutoff_does_not_rewrite_prior_proof(actual_source_window, tmp_path) -> None:
    slot, store, snapshot, proof, records = actual_source_window
    original = records[300]
    late_at = snapshot.slot_at_ns + SNAPSHOT_DEADLINE_NS + 1
    revision = make_record(
        record_id="hour-300-revision-2",
        source_id=original.source_id,
        venue=original.venue,
        instrument=original.instrument,
        data_kind=original.data_kind,
        payload={"close": "999"},
        source_event_at_ns=original.source_event_at_ns,
        bar_start_ns=original.bar_start_ns,
        bar_end_ns=original.bar_end_ns,
        source_revision="revision-2",
        revision_of=original.record_id,
        received_at_ns=late_at,
        processed_at_ns=late_at,
        available_at_ns=late_at,
        data_ingested_at_ns=late_at,
        recorded_at_ns=late_at,
        evidence_ref="raw:revision-2",
    )
    revised_store = _write_source_archive(tmp_path / "revised", (*records, revision))

    assert verify_snapshot_source_proof(snapshot, proof, revised_store)
    assert evaluate_phase4_qualified(_qualified_input(snapshot, proof), revised_store).trade_plan is not None


def test_late_source_repair_does_not_qualify_the_old_information_cutoff(tmp_path) -> None:
    slot = 724 * HOUR_NS
    full = _make_hourly_records(slot=slot)
    missing = full[300]
    store = _write_source_archive(tmp_path, full[:300] + full[301:])
    computed = slot + 1_000_000_000
    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof("BTCUSDT", slot, computed, store)

    repaired_at = computed + 1
    repaired = make_record(
        record_id=missing.record_id,
        source_id=missing.source_id,
        venue=missing.venue,
        instrument=missing.instrument,
        data_kind=missing.data_kind,
        payload=missing.payload,
        source_event_at_ns=missing.source_event_at_ns,
        bar_start_ns=missing.bar_start_ns,
        bar_end_ns=missing.bar_end_ns,
        source_revision=missing.source_revision,
        received_at_ns=repaired_at,
        processed_at_ns=repaired_at,
        available_at_ns=repaired_at,
        data_ingested_at_ns=repaired_at,
        recorded_at_ns=repaired_at,
        evidence_ref="raw:late-repair",
    )
    store.archive.write_batch([repaired])
    with pytest.raises(SourceProofError):
        build_snapshot_with_source_proof("BTCUSDT", slot, computed, ParquetFeatureSourceStore(store.archive.root))


def test_tampered_archive_chunk_source_hash_fails_restart_verification(actual_source_window) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _, store, snapshot, proof, _ = actual_source_window
    chunk = next(store.archive.root.rglob("part-*.parquet"))
    rows = pq.read_table(chunk).to_pylist()
    rows[0]["content_hash"] = "0" * 64
    pq.write_table(pa.Table.from_pylist(rows), chunk)

    assert not verify_snapshot_source_proof(snapshot, proof, ParquetFeatureSourceStore(store.archive.root))


def test_bound_instrument_slot_computation_and_complete_payload_are_reverified(actual_source_window, tmp_path) -> None:
    _, store, snapshot, proof, _ = actual_source_window
    altered_returns = replace(snapshot.values, returns=(snapshot.values.returns[0] + 0.1, *snapshot.values.returns[1:]))
    altered_variance = replace(snapshot.values, variance=snapshot.values.variance + 0.1)
    for values in (altered_returns, altered_variance):
        altered_snapshot = replace(snapshot, values=values)
        assert altered_snapshot.snapshot_hash() == snapshot.snapshot_hash()
        assert not verify_snapshot_source_proof(altered_snapshot, proof, store)
        evaluation = evaluate_phase4_qualified(_qualified_input(altered_snapshot, proof), store)
        assert evaluation.status is DecisionStatus.NOT_ESTIMABLE and evaluation.trade_plan is None

    other_instrument_snapshot = replace(snapshot, instrument="ETHUSDT")
    delta = 4 * HOUR_NS
    other_slot_snapshot = replace(
        snapshot,
        slot_at_ns=snapshot.slot_at_ns + delta,
        computed_at_ns=snapshot.computed_at_ns + delta,
        values=replace(snapshot.values, end_at_ns=snapshot.slot_at_ns + delta),
    )
    other_computation_snapshot = replace(snapshot, computed_at_ns=snapshot.computed_at_ns + 1)
    for mismatched_snapshot in (other_instrument_snapshot, other_slot_snapshot, other_computation_snapshot):
        evaluation = evaluate_phase4_qualified(_qualified_input(mismatched_snapshot, proof), store)
        assert evaluation.status is DecisionStatus.NOT_ESTIMABLE and evaluation.trade_plan is None

    other_instrument = replace(proof, instrument="ETHUSDT")
    other_slot = replace(
        proof,
        slot_at_ns=proof.slot_at_ns + delta,
        computed_at_ns=proof.computed_at_ns + delta,
        information_cutoff_ns=proof.information_cutoff_ns + delta,
        source_end_at_ns=tuple(value + delta for value in proof.source_end_at_ns),
        actual_available_at_ns=tuple(value + delta for value in proof.actual_available_at_ns),
        replay_available_at_ns=tuple(
            value + delta if value is not None else None for value in proof.replay_available_at_ns
        ),
        effective_available_at_ns=tuple(value + delta for value in proof.effective_available_at_ns),
    )
    other_computation = replace(
        proof, computed_at_ns=proof.computed_at_ns + 1, information_cutoff_ns=proof.information_cutoff_ns + 1
    )
    for invalid in (other_instrument, other_slot, other_computation):
        assert not verify_snapshot_source_proof(snapshot, invalid, store)


def test_mismatched_archived_content_hash_or_fingerprint_fails(actual_source_window, tmp_path) -> None:
    _, store, snapshot, proof, _ = actual_source_window
    changed_hash = replace(proof, source_content_hashes=("0" * 64, *proof.source_content_hashes[1:]))
    changed_fingerprint = replace(proof, source_record_fingerprints=("1" * 64, *proof.source_record_fingerprints[1:]))

    assert not verify_snapshot_source_proof(snapshot, changed_hash, store)
    assert not verify_snapshot_source_proof(snapshot, changed_fingerprint, store)


def test_new_qualified_path_fails_closed_without_evidence_and_two_id_fixture_is_not_qualified() -> None:
    case = decision_input()
    assert case.snapshot is not None and case.snapshot_evidence is None
    assert len(case.snapshot.source_record_ids) == 2

    evaluation = evaluate_phase4_qualified(case, None)

    assert evaluation.status is DecisionStatus.NOT_ESTIMABLE
    assert evaluation.trade_plan is None
    assert "source proof" in evaluation.reason
