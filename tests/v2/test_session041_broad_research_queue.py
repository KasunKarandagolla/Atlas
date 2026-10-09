"""Durable bounded broad-research prepared-history queue coverage."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.chronology import record_computation
from atlas.v2.contracts import ArtifactEnvelope
from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.data.microstructure import SequenceValidBookV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, UniverseContractV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.active_history import ActiveHistoryPageV1
from atlas.v2.runtime.broad_research_queue import (
    LANE_BROAD_RESEARCH_V1,
    complete_prepared_history_snapshot,
    enqueue_prepared_history_snapshot,
    load_due_prepared_history_snapshot,
)
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1

CUTOFF = 1_800_000_000_000_000_000
KEY = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, ProductTypeV2.LINEAR_PERPETUAL,
    "BTCUSDT", "BTC", "USDT", "USDT", "e" * 64)


def _artifact(repository, body, *, at=CUTOFF - 10):
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(ref, "QueueTestInputV1", ref, at, at, body))
    return ref


def _event(trigger_ref, *, cutoff=CUTOFF):
    return OpsDecisionEventV1(event_id=sha256_json({"event": cutoff}), event_type="TEST",
        source_id="TEST_SOURCE", trigger_ref=trigger_ref, source_event_at_ns=cutoff - 20,
        source_published_at_ns=None, received_at_ns=cutoff - 10, available_at_ns=cutoff - 10,
        information_cutoff_ns=cutoff, deadline_ns=cutoff + 100, causal_input_refs=(trigger_ref,))


def _history(repository):
    raw = RawObservationV2.build(instrument_revision=KEY.contract_revision, source_id="TEST_SOURCE",
        event_type=f"BAR_{BarIntervalV2.H1.value}", event_at_ns=CUTOFF, received_at_ns=CUTOFF,
        ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
        translation_version="QUEUE_TEST_V1", payload=b"bar", sequence="one")
    bar = CausalBarV2(raw, BarIntervalV2.H1, CUTOFF - BarIntervalV2.H1.duration_ns, CUTOFF,
        Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
    source_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
    source_metadata = raw.to_dict()
    source_metadata["instrument_key_json"] = KEY.to_canonical_json()
    repository.register_artifact(ArtifactIndexEntryV2(source_ref, "PublicObservationIndexV2",
        raw.content_hash, raw.received_at_ns, raw.available_at_ns, source_metadata))
    state = advance(None, (IndexedCausalBarV2(bar, source_ref),), key=KEY, interval=BarIntervalV2.H1)
    return {KEY.to_canonical_json(): {BarIntervalV2.H1: ActiveHistoryPageV1(state, True, "READY")}}


def _inputs(repository, *, cutoff=CUTOFF):
    trigger = _artifact(repository, {"trigger": cutoff})
    universe = _universe(repository, available=cutoff - 10, decision_slot=cutoff - 10)
    composition = _artifact(repository, {"composition": cutoff})
    return _event(trigger, cutoff=cutoff), universe, (composition,)


def _universe(repository, *, available, decision_slot):
    universe = UniverseContractV2(ArtifactEnvelope(1, "queue-test-universe", available,
        available, "QUEUE_TEST_V1", ()), "QUEUE_TEST_UNIVERSE_V1", decision_slot,
        sha256_json("queue-test-policy"), ())
    repository.register_artifact(ArtifactIndexEntryV2(universe.content_hash,
        "UniverseContractV2", universe.content_hash, available, available,
        {"universe": universe.to_dict()}))
    return universe.content_hash


def test_snapshot_survives_restart_with_exact_cutoff_history_and_no_books(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        event, universe, refs = _inputs(repository)
        pages = _history(repository)
        queued = enqueue_prepared_history_snapshot(repository, event=event, universe_ref=universe,
            composition_refs=refs, prepared_histories=pages, observed_at_ns=CUTOFF + 1)
        assert queued.status == "QUEUED"
        snapshot_ref = queued.snapshot_ref
    with OpsRepository(path) as restarted:
        loaded, = load_due_prepared_history_snapshot(restarted, as_of_ns=CUTOFF + 50)
        state = loaded.histories[KEY.to_canonical_json()][BarIntervalV2.H1].state
        original = pages[KEY.to_canonical_json()][BarIntervalV2.H1].state
        assert loaded.snapshot_ref == snapshot_ref
        assert state is not None and state.to_dict() == original.to_dict()
        assert loaded.information_cutoff_ns == CUTOFF
        assert loaded.live_books_available is False
        assert loaded.live_book_reason == "LIVE_BOOKS_UNAVAILABLE_AFTER_RESTART"
        assert loaded.s4_features == {}
        assert loaded.s4_missing_reason == "S4_NOT_ESTIMABLE_NO_CUTOFF_FEATURE_SNAPSHOT"


def test_queue_is_one_slot_and_emits_explicit_deferred_record(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        event_a, universe_a, refs_a = _inputs(repository)
        first = enqueue_prepared_history_snapshot(repository, event=event_a, universe_ref=universe_a,
            composition_refs=refs_a, prepared_histories={}, observed_at_ns=CUTOFF + 1)
        event_b, universe_b, refs_b = _inputs(repository, cutoff=CUTOFF + 1_000)
        second = enqueue_prepared_history_snapshot(repository, event=event_b, universe_ref=universe_b,
            composition_refs=refs_b, prepared_histories={"not-a-canonical-key": None},
            observed_at_ns=CUTOFF + 1_001)
        assert first.status == "QUEUED"
        assert second.status == "DEFERRED"
        assert second.deferred_ref is not None
        deferred = repository.get_artifact(second.deferred_ref)
        assert deferred.metadata["deferred"]["reason"] == "RESEARCH_SLOT_CAPACITY"
        first_times = deferred.created_at_ns, deferred.available_at_ns
        replay = enqueue_prepared_history_snapshot(repository, event=event_b,
            universe_ref=universe_b, composition_refs=refs_b,
            prepared_histories={"still-not-canonical": None}, observed_at_ns=CUTOFF + 1_200)
        deferred_replay = repository.get_artifact(replay.deferred_ref)
        assert replay.deferred_ref == second.deferred_ref
        assert (deferred_replay.created_at_ns, deferred_replay.available_at_ns) == first_times
        assert len(repository.due_work_items(LANE_BROAD_RESEARCH_V1,
            as_of_ns=CUTOFF + 5_000, limit=2)) == 1


def test_completion_artifact_and_due_retirement_are_atomic_and_idempotent(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        event, universe, refs = _inputs(repository)
        queued = enqueue_prepared_history_snapshot(repository, event=event, universe_ref=universe,
            composition_refs=refs, prepared_histories={}, observed_at_ns=CUTOFF + 1)
        completion = complete_prepared_history_snapshot(repository, snapshot_ref=queued.snapshot_ref,
            completed_at_ns=CUTOFF + 20)
        completion_entry = repository.get_artifact(completion)
        assert completion_entry is not None
        assert completion_entry.content_hash == sha256_json(completion_entry.metadata["completion"])
        assert repository.due_work_items(LANE_BROAD_RESEARCH_V1,
            as_of_ns=CUTOFF + 50, limit=1) == ()
        assert complete_prepared_history_snapshot(repository, snapshot_ref=queued.snapshot_ref,
            completed_at_ns=CUTOFF + 21) == completion
        assert repository.get_artifact(completion).metadata["completion"]["completed_at_ns"] == CUTOFF + 20
        replay = enqueue_prepared_history_snapshot(repository, event=event, universe_ref=universe,
            composition_refs=refs, prepared_histories={}, observed_at_ns=CUTOFF + 21)
        assert replay.status == "ALREADY_TERMINAL"
        corrupted = json.loads(canonical_json(dict(repository.get_artifact(completion).metadata)))
        corrupted["completion"]["completed_at_ns"] = CUTOFF + 22
        repository._connection.execute("UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
            (canonical_json(corrupted), completion))
        with pytest.raises(ValueError, match="COMPLETION_IDENTITY_CONFLICT"):
            complete_prepared_history_snapshot(repository, snapshot_ref=queued.snapshot_ref,
                completed_at_ns=CUTOFF + 23)


def test_exact_prepared_s4_feature_and_receipt_round_trip(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        event, universe, refs = _inputs(repository)
        feature = SequenceValidBookV2(instrument=KEY, source_id="QUEUE_TEST_SOURCE",
            channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U", warmup_ns=0,
            stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF)
        repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash,
            "S4FeatureArtifactV2", feature.content_hash, CUTOFF + 1, CUTOFF + 1,
            {"feature": feature.to_dict()}))
        record_computation(repository, artifact_ref=feature.content_hash,
            information_cutoff_ns=CUTOFF, started_ns=CUTOFF + 1,
            finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=feature.input_refs, deadline_ns=CUTOFF + 100)
        pages = {KEY.to_canonical_json(): {}}
        queued = enqueue_prepared_history_snapshot(repository, event=event,
            universe_ref=universe, composition_refs=refs, prepared_histories=pages,
            s4_feature_refs={KEY.to_canonical_json(): feature.content_hash},
            observed_at_ns=CUTOFF + 2)
        loaded, = load_due_prepared_history_snapshot(repository, as_of_ns=CUTOFF + 3)
        assert queued.status == "QUEUED"
        assert loaded.s4_features[KEY.to_canonical_json()] == feature
        assert loaded.s4_feature_refs[KEY.to_canonical_json()] == feature.content_hash
        assert loaded.s4_missing_reason is None


def test_late_universe_is_accepted_only_with_a_causal_receipt(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        event, _universe_ref, refs = _inputs(repository)
        refs = (_artifact(repository, {"sealed-composition": CUTOFF}, at=CUTOFF + 1),)
        universe_ref = _universe(repository, available=CUTOFF + 1, decision_slot=CUTOFF + 20)
        record_computation(repository, artifact_ref=universe_ref,
            information_cutoff_ns=CUTOFF, started_ns=CUTOFF + 1,
            finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=(), deadline_ns=CUTOFF + 10)
        queued = enqueue_prepared_history_snapshot(repository, event=event,
            universe_ref=universe_ref, composition_refs=refs, prepared_histories={},
            observed_at_ns=CUTOFF + 2)
        assert queued.status == "QUEUED"
        loaded, = load_due_prepared_history_snapshot(repository, as_of_ns=CUTOFF + 3)
        assert loaded.universe_ref == universe_ref


def test_corrupt_snapshot_and_future_cutoff_evidence_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        event, universe, refs = _inputs(repository)
        queued = enqueue_prepared_history_snapshot(repository, event=event, universe_ref=universe,
            composition_refs=refs, prepared_histories=_history(repository), observed_at_ns=CUTOFF + 1)
        entry = repository.get_artifact(queued.snapshot_ref)
        metadata = json.loads(canonical_json(dict(entry.metadata)))
        metadata["snapshot"]["histories"][0]["reason_code"] = "CORRUPTED"
        repository._connection.execute("UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
            (canonical_json(metadata), queued.snapshot_ref))
        with pytest.raises(ValueError, match="CHECKSUM"):
            load_due_prepared_history_snapshot(repository, as_of_ns=CUTOFF + 2)

    with OpsRepository(tmp_path / "future.sqlite") as repository:
        event, universe, refs = _inputs(repository, cutoff=CUTOFF - 5)
        # Create a state whose bar/source is later than the immutable cutoff.
        future_pages = _history(repository)
        state = future_pages[KEY.to_canonical_json()][BarIntervalV2.H1].state
        assert state is not None
        future_state = advance(None, tuple(IndexedCausalBarV2(item.bar, item.observation_index_ref)
            for item in state.tail), key=KEY, interval=BarIntervalV2.H1)
        future_pages[KEY.to_canonical_json()][BarIntervalV2.H1] = ActiveHistoryPageV1(
            future_state, True, "READY")
        with pytest.raises(ValueError, match="cutoff chronology"):
            enqueue_prepared_history_snapshot(repository, event=event, universe_ref=universe,
                composition_refs=refs, prepared_histories=future_pages, observed_at_ns=CUTOFF + 1)
