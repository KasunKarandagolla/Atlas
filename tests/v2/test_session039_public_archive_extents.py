"""Immutable Arrow extent durability, prefix integrity and corruption gates."""
import hashlib
import os
import stat

import pyarrow as pa
import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.public_archive_extents import (
    MAX_EXTENT_ROWS,
    MAX_SEGMENT_BYTES,
    read_extent,
    write_extent,
    write_extents,
)
from atlas.v2.memory.repository import OpsRepository


def _write(repository, rows, tick):
    return write_extent(repository, pa.Table.from_pylist(rows), namespace="ops-public-transport",
                        chunk_id=sha256_json([{key: value.hex() if isinstance(value, bytes) else value for key, value in row.items()} for row in rows]), clock_ns=lambda: tick, floor_ns=tick)


def test_appended_records_keep_exact_prefix_and_read_only_reconstruction(tmp_path):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as repository:
        first, path = _write(repository, [{"raw": b"first", "ordinal": 1}], 10)
        before = path.read_bytes()
        second, same_path = _write(repository, [{"raw": b"second", "ordinal": 2}], 20)
        assert same_path == path and path.read_bytes()[:len(before)] == before
        assert read_extent(repository, first).to_pylist() == [{"raw": b"first", "ordinal": 1}]
        assert read_extent(repository, second).to_pylist() == [{"raw": b"second", "ordinal": 2}]
        assert path.stat().st_size <= MAX_SEGMENT_BYTES
        digest = hashlib.sha256(before).hexdigest()
        changes = repository._connection.total_changes
        with OpsRepository(database, read_only=True) as reader:
            assert read_extent(reader, first).to_pylist()[0]["raw"] == b"first"
            assert reader._public_extent_writer is None
        assert repository._connection.total_changes == changes
    with OpsRepository(database) as repository:
        third, new_path = _write(repository, [{"raw": b"third", "ordinal": 3}], 30)
        assert new_path != path
        assert hashlib.sha256(path.read_bytes()[:len(before)]).hexdigest() == digest
        assert read_extent(repository, third).to_pylist()[0]["raw"] == b"third"
        assert read_extent(repository, first).to_pylist()[0]["raw"] == b"first"


def test_extent_batch_shares_one_durable_segment_sync(tmp_path, monkeypatch):
    import atlas.v2.data.public_archive_extents as archive_extents

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        rows = (
            pa.Table.from_pylist([{"raw": b"bybit", "ordinal": 1}]),
            pa.Table.from_pylist([{"raw": b"binance", "ordinal": 2}]),
        )
        chunks = tuple((table, "ops-public-transport", sha256_json({"ordinal": index}),
                        lambda: 100, 100) for index, table in enumerate(rows))
        fsync_kinds = []
        original_fsync = archive_extents.os.fsync

        def counted_fsync(fd):
            fsync_kinds.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
            return original_fsync(fd)

        monkeypatch.setattr(archive_extents.os, "fsync", counted_fsync)
        outputs = write_extents(repository, chunks)

        if os.name == "nt":
            assert fsync_kinds == ["file"]
        else:
            assert fsync_kinds == ["directory", "file", "directory"]
        assert len(outputs) == 2 and outputs[0][1] == outputs[1][1]
        refs = tuple(ref for ref, _path in outputs)
        descriptors = tuple(repository.get_artifact(ref) for ref in refs)
        assert all(descriptor is not None for descriptor in descriptors)
        extents = tuple(descriptor.metadata["extent"] for descriptor in descriptors if descriptor is not None)
        assert extents[0]["segment_name"] == extents[1]["segment_name"]
        assert extents[0]["offset"] < extents[1]["offset"]
        assert tuple(read_extent(repository, ref).to_pylist() for ref in refs) == tuple(
            table.to_pylist() for table in rows)


def test_single_extent_publishes_durable_segment_name_before_index(tmp_path, monkeypatch):
    import atlas.v2.data.public_archive_extents as archive_extents

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        fsync_kinds = []
        original_fsync = archive_extents.os.fsync

        def counted_fsync(fd):
            fsync_kinds.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
            return original_fsync(fd)

        monkeypatch.setattr(archive_extents.os, "fsync", counted_fsync)
        original_register = repository.register_artifact
        observed_at_register = []

        def register_after_durable_file(entry):
            observed_at_register.append(tuple(fsync_kinds))
            return original_register(entry)

        monkeypatch.setattr(repository, "register_artifact", register_after_durable_file)
        ref, path = _write(repository, [{"raw": b"durable", "ordinal": 1}], 10)

        assert path.is_file()
        assert repository.get_artifact(ref) is not None
        assert len(observed_at_register) == 1
        if os.name == "nt":
            assert observed_at_register == [("file",)]
        else:
            assert observed_at_register == [("directory", "file", "directory")]


def test_batched_extent_rollover_seals_each_segment_before_index(tmp_path, monkeypatch):
    import atlas.v2.data.public_archive_extents as archive_extents

    monkeypatch.setattr(archive_extents, "MAX_SEGMENT_BYTES", 400)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        tables = tuple(pa.Table.from_pylist([{"raw": bytes([index]) * 20, "ordinal": index}])
                       for index in range(3))
        chunks = tuple((table, "ops-public-transport", sha256_json({"rollover": index}),
                        lambda: 100, 100) for index, table in enumerate(tables))
        outputs = write_extents(repository, chunks)

        paths = tuple(path for _ref, path in outputs)
        assert len({path.name for path in paths}) >= 2
        assert all(path.stat().st_size <= archive_extents.MAX_SEGMENT_BYTES for path in set(paths))
        assert tuple(read_extent(repository, ref).to_pylist() for ref, _path in outputs) == tuple(
            table.to_pylist() for table in tables)


@pytest.mark.skipif(os.name == "nt", reason="directory fsync publication fault is POSIX-specific")
def test_segment_directory_sync_failure_does_not_publish_descriptor(tmp_path, monkeypatch):
    import atlas.v2.data.public_archive_extents as archive_extents

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        directory_syncs = 0
        original_fsync = archive_extents.os.fsync

        def fail_segment_directory_sync(fd):
            nonlocal directory_syncs
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                directory_syncs += 1
                if directory_syncs == 2:
                    raise OSError("injected segment directory sync failure")
            return original_fsync(fd)

        monkeypatch.setattr(archive_extents.os, "fsync", fail_segment_directory_sync)
        writer = archive_extents.PublicArchiveExtentWriterV1(repository)
        table = pa.Table.from_pylist([{"raw": b"retained but unindexed", "ordinal": 1}])
        ref = archive_extents.extent_ref("ops-public-transport", sha256_json({"sync-fault": 1}))
        with pytest.raises(OSError, match="directory sync failure"):
            writer.write(table, namespace="ops-public-transport", chunk_id=sha256_json({"sync-fault": 1}),
                         clock_ns=lambda: 10, floor_ns=10)

        assert directory_syncs == 2
        assert repository.get_artifact(ref) is None
        assert tuple((writer.root).glob("public-*.arrow"))


def test_batched_clock_failure_keeps_writer_retryable_after_durable_append(tmp_path):
    import atlas.v2.data.public_archive_extents as archive_extents

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        writer = archive_extents.PublicArchiveExtentWriterV1(repository)
        first_table = pa.Table.from_pylist([{"raw": b"first", "ordinal": 1}])
        first_ref = sha256_json({"clock-retry": "first"})
        first_ref, first_path = writer.write(
            first_table, namespace="ops-public-transport", chunk_id=first_ref,
            clock_ns=lambda: 10, floor_ns=10)
        before = writer.offset
        failed_table = pa.Table.from_pylist([{"raw": b"durable orphan", "ordinal": 2}])
        failed_chunk_id = sha256_json({"clock-retry": "failed"})

        def broken_clock():
            raise RuntimeError("injected clock sampling failure")

        with pytest.raises(RuntimeError, match="clock sampling failure"):
            writer.write_many(((failed_table, "ops-public-transport", failed_chunk_id,
                                broken_clock, 20),))

        assert writer.path == first_path
        assert writer.offset > before
        assert writer.path.stat().st_size == writer.offset
        failed_ref = archive_extents.extent_ref("ops-public-transport", failed_chunk_id)
        assert repository.get_artifact(failed_ref) is None

        recovered_table = pa.Table.from_pylist([{"raw": b"retry continues", "ordinal": 3}])
        recovered_chunk_id = sha256_json({"clock-retry": "recovered"})
        recovered_ref, recovered_path = writer.write(
            recovered_table, namespace="ops-public-transport", chunk_id=recovered_chunk_id,
            clock_ns=lambda: 30, floor_ns=30)

        assert recovered_path == first_path
        assert repository.get_artifact(recovered_ref) is not None
        assert read_extent(repository, first_ref).to_pylist() == first_table.to_pylist()
        assert read_extent(repository, recovered_ref).to_pylist() == recovered_table.to_pylist()


def test_single_clock_failure_keeps_writer_retryable_after_durable_append(tmp_path):
    import atlas.v2.data.public_archive_extents as archive_extents

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        writer = archive_extents.PublicArchiveExtentWriterV1(repository)
        first_table = pa.Table.from_pylist([{"raw": b"first", "ordinal": 1}])
        first_chunk_id = sha256_json({"single-clock-retry": "first"})
        first_ref, first_path = writer.write(
            first_table, namespace="ops-public-transport", chunk_id=first_chunk_id,
            clock_ns=lambda: 10, floor_ns=10)
        before = writer.offset
        failed_table = pa.Table.from_pylist([{"raw": b"durable append", "ordinal": 2}])
        failed_chunk_id = sha256_json({"single-clock-retry": "failed"})

        def broken_clock():
            raise RuntimeError("injected clock metrics failure")

        with pytest.raises(RuntimeError, match="clock metrics failure"):
            writer.write(failed_table, namespace="ops-public-transport", chunk_id=failed_chunk_id,
                         clock_ns=broken_clock, floor_ns=20)

        assert writer.path == first_path
        assert writer.offset > before
        assert writer.path.stat().st_size == writer.offset
        failed_ref = archive_extents.extent_ref("ops-public-transport", failed_chunk_id)
        assert repository.get_artifact(failed_ref) is None

        recovered_table = pa.Table.from_pylist([{"raw": b"retry continues", "ordinal": 3}])
        recovered_chunk_id = sha256_json({"single-clock-retry": "recovered"})
        recovered_ref, recovered_path = writer.write(
            recovered_table, namespace="ops-public-transport", chunk_id=recovered_chunk_id,
            clock_ns=lambda: 30, floor_ns=30)

        assert recovered_path == first_path
        assert repository.get_artifact(recovered_ref) is not None
        assert read_extent(repository, first_ref).to_pylist() == first_table.to_pylist()
        assert read_extent(repository, recovered_ref).to_pylist() == recovered_table.to_pylist()


@pytest.mark.parametrize("damage", ["changed", "truncated", "symlink"])
def test_extent_damage_is_not_treated_as_valid_evidence(tmp_path, damage):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        ref, path = _write(repository, [{"raw": b"exact"}], 10)
        raw = path.read_bytes()
        if damage == "changed":
            path.write_bytes(raw[:100] + bytes([raw[100] ^ 1]) + raw[101:])
        elif damage == "truncated":
            path.write_bytes(raw[:-1])
        else:
            other = path.with_suffix(".copy")
            path.rename(other)
            try:
                path.symlink_to(other)
            except OSError:
                pytest.skip("host does not grant native symlink creation")
        with pytest.raises(ValueError):
            read_extent(repository, ref)


def test_failed_sql_transaction_leaves_raw_orphan_without_fabricating_checkpoint(tmp_path):
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as repository:
        with pytest.raises(RuntimeError), repository.atomic_composition():
            ref, path = _write(repository, [{"raw": b"retained orphan"}], 10)
            raise RuntimeError("injected failure after durable raw write")
        assert path.exists() and repository.get_artifact(ref) is None
        with pytest.raises(ValueError):
            read_extent(repository, ref)
    with OpsRepository(database) as repository:
        new_ref, new_path = _write(repository, [{"raw": b"recovery"}], 20)
        assert new_path != path
        assert path.exists() and repository.get_artifact(ref) is None
        assert read_extent(repository, new_ref).to_pylist()[0]["raw"] == b"recovery"


def test_extent_row_bound_does_not_create_an_archive_or_index(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        with pytest.raises(ValueError, match="row bound"):
            _write(repository, [{"raw": b"x"}] * (MAX_EXTENT_ROWS + 1), 10)
        assert not repository.artifact_entries("PublicArchiveExtentV1")
        assert repository._public_extent_writer.path is None
