"""Bounded causal source and M15 accounting reads preserve exact identities."""

from dataclasses import replace

import pytest

from atlas.v2._serialization import canonical_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import (
    ParquetObservationArchiveV2,
    reconstruct_causal_bars_from_archive,
    reconstruct_indexed_causal_bars_v1,
    reconstruct_native_bars_from_index_page,
    reconstruct_public_observations_from_archive,
)
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository, SourceHealthV2
from atlas.v2.science.m15_origin_accounting import advance_m15_origin_checkpoint

from .test_session036_m15_origin_accounting import BASE, STEP, bar, key, product


def test_sticky_source_gap_survives_reopen_without_future_health_rescue(tmp_path):
    path = tmp_path / "ops.sqlite"
    rows = (SourceHealthV2("PUBLIC", 1, 1, "DISCONNECTED"),
            SourceHealthV2("PUBLIC", 2, 2, "HEALTHY_CURRENT"),
            SourceHealthV2("PUBLIC", 3, 5, "DISCONNECTED"),
            SourceHealthV2("PUBLIC", 6, 6, "HEALTHY_CURRENT"))
    with OpsRepository(path) as repository:
        repository.record_source_health(rows[0])
        assert not repository.source_had_unhealthy_after_healthy("PUBLIC")
        repository.record_source_health(rows[1])
        assert not repository.source_had_unhealthy_after_healthy("PUBLIC")
        for row in rows[2:]:
            repository.record_source_health(row)
        assert repository.latest_source_health_at("PUBLIC", as_of_ns=4) == rows[1]
        assert repository.latest_source_health_at("PUBLIC", as_of_ns=5) == rows[2]
        assert repository.source_health_history("PUBLIC", limit=1) == (rows[-1],)
    with OpsRepository(path) as repository:
        assert repository.source_had_unhealthy_after_healthy("PUBLIC")
        assert not repository.source_had_unhealthy_after_healthy("OTHER")
        assert repository.latest_source_health_at("PUBLIC", as_of_ns=0) is None


def test_m15_page_reconstruction_exact_order_window_and_revision(tmp_path, monkeypatch):
    archive = tmp_path / "archive"
    registry = InstrumentRegistryV2()
    registry.register(product())
    rows = (bar(BASE + STEP), bar(BASE), bar(BASE + 2 * STEP))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=registry,
            clock_ns=lambda: BASE + 4 * STEP, archive=ParquetObservationArchiveV2(archive))
        for row in rows:
            collector.ingest(row.raw, raw_payload=canonical_json({"close": row.close_at_ns}),
                instrument_key=key(), bar=row, update_source_health=False)
        collector.flush_archive()
        from pathlib import Path

        def prohibit_archive_enumeration(*_args, **_kwargs):
            raise AssertionError("bounded history must use accepted indexed chunk locators")

        monkeypatch.setattr(Path, "glob", prohibit_archive_enumeration)
        latest = reconstruct_causal_bars_from_archive(repository, archive, key=key(),
            interval=BarIntervalV2.M15, information_cutoff_ns=BASE + 3 * STEP, limit=1)
        assert [item.bar.close_at_ns for item in latest] == [BASE + 2 * STEP]
        public = reconstruct_public_observations_from_archive(repository, archive, key=key(),
            instrument_revision=key().contract_revision, event_types=("BAR_15M",),
            information_cutoff_ns=BASE + 3 * STEP, limit=1)
        assert public[0].observation == rows[2].raw
        first = repository.m15_origin_observation_page(key(), available_from_ns=0,
            available_through_ns=BASE + 3 * STEP, limit=2)
        assert first.has_more
        assert [entry.metadata["event_at_ns"] for entry in first.entries] == [BASE, BASE + STEP]
        reconstructed = reconstruct_native_bars_from_index_page(repository, archive, key=key(),
            interval=BarIntervalV2.M15, index_entries=first.entries)
        assert [item.bar for item in reconstructed] == [rows[1], rows[0]]
        second = repository.m15_origin_observation_page(key(), available_from_ns=0,
            available_through_ns=BASE + 3 * STEP, after_close_at_ns=first.last_close_at_ns, limit=2)
        assert not second.has_more
        assert second.last_close_at_ns == BASE + 2 * STEP
        assert repository.m15_origin_observation_page(key("second"), available_from_ns=0,
            available_through_ns=BASE + 3 * STEP).entries == ()
        assert repository.m15_origin_observation_page(key(), available_from_ns=0,
            available_through_ns=BASE).entries == ()
        prospective = repository.m15_origin_observation_page(key(), available_from_ns=0,
            available_through_ns=BASE + 3 * STEP, min_close_at_ns=BASE + 1)
        assert [entry.metadata["event_at_ns"] for entry in prospective.entries] == [BASE + STEP, BASE + 2 * STEP]
        exact = repository.confirmed_bar_observation_entries(key(), event_type="BAR_15M",
            close_at_ns=BASE, as_of_ns=BASE + 3 * STEP)
        assert len(exact) == 1
        verified = reconstruct_indexed_causal_bars_v1(repository, archive, key=key(),
            interval=BarIntervalV2.M15, index_entries=exact, information_cutoff_ns=BASE + 3 * STEP)
        assert verified[0].bar == rows[1]
        with pytest.raises(ValueError, match="unavailable"):
            reconstruct_indexed_causal_bars_v1(repository, archive, key=key(),
                interval=BarIntervalV2.M15, index_entries=exact, information_cutoff_ns=BASE)
        with pytest.raises(ValueError, match="align"):
            repository.m15_origin_observation_page(key(), available_from_ns=0,
                available_through_ns=BASE + 3 * STEP, after_close_at_ns=BASE + 60_000_000_000)
        with pytest.raises(ValueError, match="identity"):
            reconstruct_native_bars_from_index_page(repository, archive, key=key(),
                interval=BarIntervalV2.M1, index_entries=first.entries)


def test_m15_checkpoint_validates_identity_and_rejects_generation_conflict(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        assert repository.latest_m15_origin_accounting_checkpoint(key()) is None
        first = advance_m15_origin_checkpoint(None, key(), now_ns=BASE,
            source_available_through_ns=BASE, next_close_cursor_ns=None, has_more=False)

        def entry(checkpoint):
            return ArtifactIndexEntryV2(checkpoint.content_hash, checkpoint.VERSION, checkpoint.content_hash,
                checkpoint.observed_at_ns, checkpoint.observed_at_ns,
                {"checkpoint": checkpoint.to_dict(), "instrument_key_json": key().to_canonical_json()})

        repository.register_artifact(entry(first))
        second = advance_m15_origin_checkpoint(first, key(), now_ns=BASE + 100,
            source_available_through_ns=BASE + 100, next_close_cursor_ns=None, has_more=False)
        repository.register_artifact(entry(second))
        assert repository.latest_m15_origin_accounting_checkpoint(key()) == entry(second)
        assert repository.latest_m15_origin_accounting_checkpoint(key("second")) is None
        repository.register_artifact(entry(replace(second, observed_at_ns=BASE + 101)))
        with pytest.raises(ValueError, match="generation"):
            repository.latest_m15_origin_accounting_checkpoint(key())


def test_exact_close_revision_overflow_and_registered_outcome_identity(tmp_path):
    from atlas.v2._serialization import sha256_json

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        for ordinal in range(3):
            payload = {"record_id": str(ordinal), "instrument_key_json": key().to_canonical_json(),
                "instrument_revision": key().contract_revision, "event_type": "BAR_15M",
                "availability_class": "ACTUAL_SYSTEM", "event_at_ns": BASE,
                "bar_content_hash": sha256_json(ordinal)}
            digest = sha256_json(payload)
            repository.register_artifact(ArtifactIndexEntryV2(digest, "PublicObservationIndexV2",
                digest, BASE + ordinal, BASE + ordinal, payload))
        with pytest.raises(ValueError, match="explicit bound"):
            repository.confirmed_bar_observation_entries(key(), event_type="BAR_15M",
                close_at_ns=BASE, as_of_ns=BASE + 10, limit=2)
        for at in (BASE, BASE + 1):
            payload = {"checkpoint": {"run_id": "run-exact", "at": at}}
            digest = sha256_json(payload)
            repository.register_artifact(ArtifactIndexEntryV2(digest,
                "ResearchPredictionOutcomeCheckpointV1", digest, at, at, payload))
        page = repository.artifact_entries_by_metadata_identity("ResearchPredictionOutcomeCheckpointV1",
            ("checkpoint", "run_id"), "run-exact", as_of_ns=BASE + 1, limit=1)
        assert page.has_more
        assert page.entries[0].available_at_ns == BASE + 1
        assert repository.artifact_entries_by_metadata_identity("ResearchPredictionOutcomeCheckpointV1",
            ("checkpoint", "run_id"), "run-other", as_of_ns=BASE + 1).entries == ()
