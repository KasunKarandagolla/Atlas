"""Verified block caching detects writes on either SQLite connection."""
from __future__ import annotations

import sqlite3

import pytest

from atlas.v2._serialization import canonical_json
from atlas.v2.memory.repository import OpsRepository

from .test_session041_block_migration import _entry


@pytest.mark.parametrize("read_mode", ["references", "namespace", "typed", "page", "latest"])
def test_bulk_lookup_decodes_each_block_once_across_sql_pages_and_cache_eviction(tmp_path, read_mode):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        expected = {}
        for block in range(12):
            entries = tuple(_entry(record=f"bulk-{block}-{ordinal}") for ordinal in range(64))
            repository.register_artifacts(entries)
            expected.update((entry.artifact_ref, entry) for entry in entries)
        assert repository._connection.execute(
            "SELECT count(*) FROM public_observation_metadata_block_v1").fetchone()[0] == 12
        repository._public_metadata_cache.clear()
        queries = []
        repository._connection.set_trace_callback(queries.append)
        if read_mode == "references":
            found = repository.get_artifact_metadata_by_refs((*reversed(tuple(expected)), "0" * 64))
            assert tuple(found) == tuple(sorted(expected))
        elif read_mode == "latest":
            page = repository.latest_artifact_entries("PublicObservationIndexV2",
                as_of_ns=max(entry.available_at_ns for entry in expected.values()), limit=len(expected))
            assert page.invalid_entry_count == 0
            assert not page.has_more
            expected_order = tuple(sorted(expected,
                key=lambda ref: (expected[ref].available_at_ns, ref), reverse=True))
            assert tuple(entry.artifact_ref for entry in page.entries) == expected_order
            found = {entry.artifact_ref: {"metadata": entry.metadata} for entry in page.entries}
        else:
            if read_mode == "namespace":
                entries = repository.artifact_entries("PublicObservationIndexV2")
            elif read_mode == "typed":
                entries = repository.artifact_entries_by_types(("PublicObservationIndexV2",), limit=1000)
            else:
                page = repository.artifact_entries_by_types_page(("PublicObservationIndexV2",),
                    as_of_ns=next(iter(expected.values())).available_at_ns, limit=1000)
                assert page.invalid_entry_count == 0
                assert len(page.raw_keys) == len(expected)
                entries = page.entries
            expected_order = tuple(sorted(expected,
                key=lambda ref: (expected[ref].created_at_ns, ref), reverse=read_mode == "page"))
            assert tuple(entry.artifact_ref for entry in entries) == expected_order
            found = {entry.artifact_ref: {"metadata": entry.metadata} for entry in entries}
        repository._connection.set_trace_callback(None)
        assert set(found) == set(expected)
        for ref, entry in expected.items():
            assert canonical_json(found[ref]["metadata"]) == canonical_json(entry.metadata)
        payload_reads = [query for query in queries
            if query.startswith("SELECT CASE WHEN length(compressed_metadata)")]
        assert len(payload_reads) == 12
        assert len(repository._public_metadata_cache) <= 8
        # Grouping is an ordering optimization. A subsequent changed block
        # still invalidates its positive cached verification and fails closed.
        connection = sqlite3.connect(repository.path, isolation_level=None)
        try:
            connection.execute("UPDATE public_observation_metadata_block_v1 SET compressed_metadata=x'00010203' "
                "WHERE block_ref=(SELECT max(block_ref) FROM public_observation_metadata_block_v1)")
        finally:
            connection.close()
        with pytest.raises(ValueError, match="invalid zlib"):
            repository.get_artifact_metadata_by_refs(tuple(expected))


def test_same_epoch_reuses_verified_block_without_reloading_blob(tmp_path):
    entry = _entry()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifact(entry)
        queries = []
        repository._connection.set_trace_callback(queries.append)
        for _ in range(4):
            assert repository.get_artifact(entry.artifact_ref) == entry
        repository._connection.set_trace_callback(None)
        payload_reads = [query for query in queries
                         if query.startswith("SELECT CASE WHEN length(compressed_metadata)")]
        assert len(payload_reads) == 1
        assert len(repository._public_metadata_cache) == 1


@pytest.mark.parametrize("other_connection", [False, True])
def test_cached_block_corruption_is_detected_after_either_writer(tmp_path, other_connection):
    entry = _entry()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifact(entry)
        assert repository.get_artifact(entry.artifact_ref) == entry
        connection = (sqlite3.connect(repository.path, isolation_level=None)
                      if other_connection else repository._connection)
        try:
            connection.execute("UPDATE public_observation_metadata_block_v1 "
                               "SET compressed_metadata=x'00010203'")
        finally:
            if other_connection:
                connection.close()
        with pytest.raises(ValueError, match="invalid zlib"):
            repository.get_artifact(entry.artifact_ref)
