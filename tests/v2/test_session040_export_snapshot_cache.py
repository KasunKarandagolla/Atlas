"""Strict bounded dependency reuse in one immutable read snapshot."""
from __future__ import annotations

import os
import time

import pytest

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.production import create_bybit_public_ws_port
from atlas.v2.science import tuning_export
from atlas.v2.science.tuning_export import _ValidationReader

from .test_s38_sustained_public_stream import FixturePublicSource, MixedWorkload, QueueStream
from .test_session036_tuning_export import IDENTITY, _rows, _sample


def entry(index):
    body = {"fixture": index}
    ref = sha256_json(body)
    return ArtifactIndexEntryV2(ref, "SnapshotCacheFixtureV1", ref, index + 1, index + 1, body)


def test_dependency_cache_is_lru_bounded_and_cannot_survive_snapshot(tmp_path, monkeypatch):
    database = tmp_path / "ops.sqlite"
    entries = tuple(entry(index) for index in range(12))
    with OpsRepository(database) as writer:
        writer.register_artifacts(entries)
        with _ValidationReader(database, read_only=True) as reader:
            monkeypatch.setattr(reader, "MAX_INDEX_CACHE_ENTRIES", 4)
            with reader.read_snapshot():
                for expected in entries:
                    assert reader.get_artifact(expected.artifact_ref) == expected
                    assert reader.get_artifact(expected.artifact_ref) == expected
                assert reader.index_cache_hits == len(entries)
                assert len(reader._index_cache) == 4
                assert reader._index_cache_bytes <= reader.MAX_INDEX_CACHE_BYTES
                assert entries[0].artifact_ref not in reader._index_cache
                assert reader._connection.total_changes == 0
            assert not reader._index_cache and reader._index_cache_bytes == 0
            later = entry(13)
            writer.register_artifact(later)
            with reader.read_snapshot():
                assert reader.get_artifact(later.artifact_ref) == later
            assert not reader._index_cache


def test_changed_raw_prefix_invalidates_cached_health_even_with_restored_mtime(tmp_path):
    clock = [time.time_ns()]
    source = QueueStream(lambda: clock[0])
    port = create_bybit_public_ws_port(public_source=FixturePublicSource(), public_stream_source=source,
                                      clock_ns=lambda: clock[0])
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repo = supervisor.repository
        assert repo is not None
        clock[0] += 100_000_000
        assert source.handoff.offer(MixedWorkload().frame(clock[0]))
        port._collect_public_stream_evidence(repo, now_ns=clock[0])
        locator = repo._connection.execute(
            "SELECT artifact_ref,chunk_id FROM public_stream_archive_locator_v1 WHERE kind=3 LIMIT 1").fetchone()
        assert locator is not None
        ref = bytes(locator[0]).hex()
        with _ValidationReader(repo.path, read_only=True) as reader, reader.read_snapshot():
            book = next(value for value in repo.artifact_entries("PublicBookLineageCheckpointV1")
                        if ref in value.metadata["checkpoint"]["frame_health_refs"])
            validated = reader.validate_public_checkpoint(book, book=True, as_of_ns=book.available_at_ns)
            assert reader.validate_public_checkpoint(book, book=True, as_of_ns=book.available_at_ns) == validated
            assert reader.validation_cache_hits == 1
            assert reader._validation_cache_bytes <= reader.MAX_INDEX_CACHE_BYTES
            with pytest.raises(ValueError, match="unavailable"):
                reader.validate_public_checkpoint(book, book=True, as_of_ns=book.available_at_ns - 1)
            body = json_value(book.metadata)
            body["checkpoint"]["book_state_hash"] = "f" * 64
            changed = ArtifactIndexEntryV2(book.artifact_ref, book.artifact_type, book.content_hash,
                                           book.created_at_ns, book.available_at_ns, body)
            with pytest.raises(ValueError, match="identity or chronology"):
                reader.validate_public_checkpoint(changed, book=True, as_of_ns=book.available_at_ns)
            original = reader.get_artifact(ref)
            assert reader.get_artifact(ref) == original
            proof = reader._index_cache[ref][2]
            path = proof[0]
            stat = path.stat()
            # Corrupt the exact metadata extent, preserve size and restore the
            # old mtime. ctime/file identity must still force strict hashing.
            from atlas.v2.data.public_archive_extents import extent_ref

            descriptor = reader.get_artifact(extent_ref("ops-stream-metadata", bytes(locator[1]).hex()))
            offset = descriptor.metadata["extent"]["offset"]
            with path.open("r+b") as handle:
                handle.seek(offset)
                byte = handle.read(1)
                handle.seek(offset)
                handle.write(bytes([byte[0] ^ 1]))
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            with pytest.raises(ValueError, match="bytes missing or changed"):
                reader.get_artifact(ref)
            with pytest.raises(ValueError, match="bytes missing or changed"):
                reader.validate_public_checkpoint(book, book=True, as_of_ns=book.available_at_ns)


def test_cache_refuses_an_entry_larger_than_its_byte_budget(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as writer:
        value = entry(0)
        writer.register_artifact(value)
        with _ValidationReader(writer.path, read_only=True) as reader, reader.read_snapshot():
            monkeypatch.setattr(reader, "MAX_INDEX_CACHE_BYTES", 1)
            assert reader.get_artifact(value.artifact_ref) == value
            assert not reader._index_cache and reader._index_cache_bytes == 0


def test_slow_validated_prefix_is_checkpointed_and_next_page_never_rescans_it(tmp_path, monkeypatch):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        expected = [_sample(writer, index + 1) for index in range(20)]
    original = tuning_export._validated_row
    visited = []

    def slow_validation(repository, value):
        visited.append(value.artifact_ref)
        time.sleep(.12)
        return original(repository, value)

    monkeypatch.setattr(tuning_export, "_validated_row", slow_validation)
    first = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY,
        cutoff_ns=100, max_snapshot_seconds=3)
    assert first["has_more"] and first["budget_yielded"]
    assert first["snapshot_budget_seconds"] == 3
    assert not first["validation_failures"]
    rows = _rows(tmp_path / "reports", first)
    second = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY,
        cutoff_ns=100, max_snapshot_seconds=3)
    assert second["after_rowid"] == first["through_rowid"]
    assert not second["has_more"] and not second["validation_failures"]
    rows += _rows(tmp_path / "reports", second)
    assert visited == expected == [row["artifact_ref"] for row in rows]
    assert second["counts"]["row_kind:EVIDENCE"] == 20


def test_interrupted_next_page_keeps_previous_checkpoint_and_restarts_exactly(tmp_path, monkeypatch):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        expected = [_sample(writer, index + 1) for index in range(3)]
    first = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY,
        cutoff_ns=100, max_rows=1)
    original = tuning_export._validated_row

    def interrupted(*args):
        raise RuntimeError("offline interrupted export")

    monkeypatch.setattr(tuning_export, "_validated_row", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY, cutoff_ns=100)
    import json

    from atlas.v2._serialization import sha256_json

    head = json.loads((tmp_path / "reports" / IDENTITY.run_id / "head.json").read_text())
    assert head["manifest_sha256"] == sha256_json(first)
    monkeypatch.setattr(tuning_export, "_validated_row", original)
    final = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY, cutoff_ns=100)
    assert final["after_rowid"] == first["through_rowid"] and not final["has_more"]
    assert [row["artifact_ref"] for row in _rows(tmp_path / "reports", first)
            + _rows(tmp_path / "reports", final)] == expected


def test_repeated_metrics_do_not_mutate_sealed_manifest_ancestry(tmp_path):
    from atlas.v2._serialization import sha256_json

    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        for index in range(4):
            _sample(writer, index + 1)
    previous = None
    for _ in range(4):
        result = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY,
            cutoff_ns=100, max_rows=1)
        if previous is not None:
            assert result["previous_manifest_sha256"] == sha256_json(previous)
            path = tmp_path / "reports" / IDENTITY.run_id / "manifests" / (sha256_json(previous) + ".json")
            assert path.exists()
        previous = result
    assert result["metric_summaries"]["ResearchResourceSampleV1:rss_bytes"]["count"] == 4
    assert not result["has_more"]


def test_source_traversal_is_bounded_even_when_projection_types_are_sparse(tmp_path, monkeypatch):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        for index in range(12):
            writer.register_artifact(entry(index))
        expected = _sample(writer, 20)
    monkeypatch.setattr(tuning_export, "MAX_SOURCE_ROWS_PER_SNAPSHOT", 4)
    through = 0
    pages = []
    while True:
        result = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY, cutoff_ns=100)
        assert result["rows_written"] <= 4
        assert result["after_rowid"] == through
        through = result["through_rowid"]
        pages.extend(_rows(tmp_path / "reports", result))
        if not result["has_more"]:
            break
    assert through == 13 and [row["artifact_ref"] for row in pages] == [expected]
    assert result["source_window_scope"] == "INDEXED_RELEVANT_TYPES_IN_INSERTION_ORDER"
    assert result["after_rowid"] == 0  # Dense raw rows did not consume separate report pages.


def test_legacy_store_without_insertion_index_retains_bounded_readonly_fallback(tmp_path, monkeypatch):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        for index in range(12):
            writer.register_artifact(entry(index))
        expected = _sample(writer, 20)
        writer._connection.execute("DROP INDEX artifact_type_insertion_lookup")
        writer._connection.commit()
    monkeypatch.setattr(tuning_export, "MAX_SOURCE_ROWS_PER_SNAPSHOT", 4)
    through, pages = 0, []
    while True:
        result = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY, cutoff_ns=100)
        assert result["through_rowid"] - through <= 4
        assert result["source_window_scope"] == "LEGACY_BOUNDED_SOURCE_ROW_WINDOW"
        through = result["through_rowid"]
        pages.extend(_rows(tmp_path / "reports", result))
        if not result["has_more"]:
            break
    assert through == 13 and [row["artifact_ref"] for row in pages] == [expected]
    with OpsRepository(database, read_only=True) as reader:
        assert reader._connection.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_type_insertion_lookup'").fetchone() is None


def test_indexed_projection_cursor_merges_exact_types_in_fifo_without_backlog_sort(tmp_path):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        expected = []
        for index in range(12):
            writer.register_artifact(entry(index))
            if index % 3 == 0:
                expected.append(_sample(writer, 20 + index))
        connection = writer._connection
        plan = connection.execute("EXPLAIN QUERY PLAN SELECT rowid FROM artifact_index "
            "INDEXED BY artifact_type_insertion_lookup WHERE artifact_type=? AND rowid>? AND rowid<=? "
            "ORDER BY rowid LIMIT 1", ("ResearchResourceSampleV1", 0, 100)).fetchall()
        assert not any("TEMP B-TREE" in str(tuple(row)) or "SCAN artifact_index" in str(tuple(row)) for row in plan)
        cursor = tuning_export._ProjectionCursor(connection, after=0, through=100, limit=2)
        first = cursor.fetchmany(128)
        assert len(first) == 2 and cursor.has_more
        second = tuning_export._ProjectionCursor(connection, after=first[-1]["source_rowid"], through=100, limit=2)
        assert [row["artifact_ref"] for row in first + second.fetchmany(128)] == expected
        assert not second.has_more


def test_dense_retained_history_does_not_consume_periodic_export_work(tmp_path):
    """Index geometry only: padding is not fabricated market/decision evidence."""
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as writer:
        first = _sample(writer, 20)
        connection = writer._connection
        with writer.atomic_composition():
            connection.execute("WITH RECURSIVE padding(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM padding WHERE n<200000) "
                "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) "
                "SELECT printf('%064x',n),'S40_INDEX_GEOMETRY_PADDING_V1',printf('%064x',n),1,1,'{}' FROM padding")
        second = _sample(writer, 30)
        steps = [0]

        def budget():
            steps[0] += 100
            return int(steps[0] > 15000)

        connection.set_progress_handler(budget, 100)
        try:
            cursor = tuning_export._ProjectionCursor(connection, after=0, through=200002, limit=4)
            rows = cursor.fetchmany(128)
            assert [row["artifact_ref"] for row in rows] == [first, second]
            assert not cursor.has_more and steps[0] <= 15000
        finally:
            connection.set_progress_handler(None, 0)
    result = tuning_export.export_tuning_snapshot(database, tmp_path / "reports", IDENTITY, cutoff_ns=100)
    assert result["rows_written"] == 2 and result["through_rowid"] == 200002
    assert result["validation_failures"] == {} and not result["has_more"]
    assert result["source_window_scope"] == "INDEXED_RELEVANT_TYPES_IN_INSERTION_ORDER"
