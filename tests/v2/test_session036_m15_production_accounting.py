"""Received M15 opportunity accounting uses the real production source seam."""


from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.production import IndexedPublicCycleSourceV1
from atlas.v2.science.m15_origin_accounting import M15OpportunityMissingnessV1, M15OriginAccountingRecordV1

from .test_session036_m15_origin_accounting import BASE, STEP, bar, product


def seed(repository, closes, now):
    registry = InstrumentRegistryV2()
    registry.register(product())
    collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: now,
        archive=ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations"))
    for close in closes:
        row = bar(close)
        collector.ingest(row.raw, raw_payload=canonical_json({"close": close}),
            instrument_key=product().key, bar=row, update_source_health=False)
    collector.flush_archive()
    return collector


def test_late_and_missing_origins_persist_without_healthy_runtime_or_candidate(tmp_path):
    now = BASE + STEP + 20
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = seed(repository, (BASE, BASE + STEP), now)
        source = IndexedPublicCycleSourceV1(clock_ns=lambda: now)
        assert source._account_m15_origins(repository, collector, now_ns=now) == ()
        missing = [M15OpportunityMissingnessV1.from_dict(entry.metadata["missingness"])
                   for entry in repository.artifact_entries(M15OpportunityMissingnessV1.VERSION)]
        assert {row.reason_code for row in missing} == {
            "M15_ORIGIN_FIRST_ACCOUNTED_AFTER_ELIGIBILITY_DEADLINE", "M15_SOURCE_HEALTH_UNAVAILABLE_AT_ORIGIN"}
        assert len(repository.artifact_entries(M15OriginAccountingRecordV1.VERSION)) == 2
        source.clock_ns = lambda: now + 1
        source._account_m15_origins(repository, collector, now_ns=now + 1)
        assert len(repository.artifact_entries(M15OpportunityMissingnessV1.VERSION)) == 2


def test_restart_after_terminal_before_cursor_does_not_duplicate_missingness(tmp_path, monkeypatch):
    now = BASE + STEP
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        collector = seed(repository, (BASE,), now)
        original = repository.register_artifact

        def fail_checkpoint(entry):
            if entry.artifact_type == "M15OriginAccountingCheckpointV1":
                raise RuntimeError("fixture crash")
            return original(entry)

        monkeypatch.setattr(repository, "register_artifact", fail_checkpoint)
        with pytest.raises(RuntimeError, match="fixture crash"):
            IndexedPublicCycleSourceV1(clock_ns=lambda: now)._account_m15_origins(repository, collector, now_ns=now)
    with OpsRepository(path) as repository:
        collector = seed(repository, (), now + 1)
        IndexedPublicCycleSourceV1(clock_ns=lambda: now + 1)._account_m15_origins(repository, collector, now_ns=now + 1)
        assert len(repository.artifact_entries(M15OpportunityMissingnessV1.VERSION)) == 1
        assert len(repository.artifact_entries(M15OriginAccountingRecordV1.VERSION)) == 1


def test_run_start_excludes_bootstrap_history_from_prospective_origins(tmp_path):
    now = BASE + STEP + 20
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = seed(repository, (BASE, BASE + STEP), now)
        source = IndexedPublicCycleSourceV1(clock_ns=lambda: now, minimum_m15_origin_close_at_ns=BASE + 1)
        source._account_m15_origins(repository, collector, now_ns=now)
        records = repository.artifact_entries(M15OriginAccountingRecordV1.VERSION)
        assert len(records) == 1
        assert records[0].metadata["accounting"]["close_at_ns"] == BASE + STEP
        assert len(repository.artifact_entries("PublicObservationIndexV2")) == 2
