"""Cache eviction cannot erase durable source identity or recovery history."""

from atlas.v2._serialization import canonical_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2, SourceHealthTrackerV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.raw import AppendStatusV2, RawObservationStoreV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import OpsRepository

from .test_data_runtime import product, raw


def test_health_cache_is_bounded_and_prior_gap_is_sticky():
    tracker = SourceHealthTrackerV2(max_history_per_source=2)
    for at, state in enumerate((PublicSourceStateV2.HEALTHY_CURRENT, PublicSourceStateV2.DISCONNECTED,
                               PublicSourceStateV2.HEALTHY_CURRENT, PublicSourceStateV2.HEALTHY_CURRENT), start=1):
        tracker.append(PublicSourceHealthV2("PUBLIC", at, at, state, str(at), "fixture"))
    assert len(tracker.history("PUBLIC")) == 2
    assert tracker.latest("PUBLIC").data_eligible
    assert tracker.had_unhealthy_after_healthy("PUBLIC")
    restored = SourceHealthTrackerV2()
    restored.seed_prior_gap("PUBLIC")
    assert restored.had_unhealthy_after_healthy("PUBLIC")


def test_evicted_raw_record_remains_deduplicated_from_accepted_persistence(tmp_path):
    registry = InstrumentRegistryV2()
    registry.register(product())
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=registry,
            clock_ns=lambda: 10000, archive=ParquetObservationArchiveV2(tmp_path / "archive"))
        collector.store = RawObservationStoreV2(max_records=2)
        records = [raw(sequence=str(index), event_at=index, received=10000, payload={"value": index})
                   for index in range(3)]
        for index, record in enumerate(records):
            collector.ingest(record, raw_payload=canonical_json({"value": index}), update_source_health=False)
            collector.flush_archive()
        assert collector.store.get(records[0].record_id) is None
        assert not collector._bar_hashes
        replay = collector.ingest(records[0], raw_payload=canonical_json({"value": 0}), update_source_health=False)
        assert replay.append.status == AppendStatusV2.DUPLICATE
        assert not collector._pending_archive


def test_intake_auto_flushes_bounded_batches_without_losing_raw_refs(tmp_path):
    registry = InstrumentRegistryV2()
    registry.register(product())
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=registry,
            clock_ns=lambda: 10000, archive=ParquetObservationArchiveV2(tmp_path / "archive"))
        for index in range(513):
            record = raw(sequence=str(index), event_at=index, received=10000, payload={"value": index})
            collector.ingest(record, raw_payload=canonical_json({"value": index}), update_source_health=False)
        assert len(collector._pending_archive) == 1
        assert len(repository.artifact_entries("PublicObservationIndexV2")) == 512
        collector.flush_archive()
        assert len(repository.artifact_entries("PublicObservationIndexV2")) == 513
