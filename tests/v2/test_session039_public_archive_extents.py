"""Immutable Arrow extent durability, prefix integrity and corruption gates."""
import hashlib

import pyarrow as pa
import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.public_archive_extents import MAX_EXTENT_ROWS, MAX_SEGMENT_BYTES, read_extent, write_extent
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
