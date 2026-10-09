"""Storage-only universe compression keeps every immutable domain byte."""

import base64
import hashlib
import json
import random
import string
import zlib
from dataclasses import replace

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.memory.compressed_metadata import (
    COMPRESSED_METADATA_TYPES,
    COMPRESSION_THRESHOLD_BYTES,
    MAX_UNCOMPRESSED_METADATA_BYTES,
    STORAGE_MARKER,
    STORAGE_VERSION,
    decode_metadata_json,
    encode_metadata_json,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository


def _entry(kind="UniverseContractV2", *, available=20, metadata=None):
    body = metadata if metadata is not None else {"original": "δ漢" * 4096, "values": [1, False, None]}
    ref = sha256_json({"kind": kind, "available": available, "metadata": body})
    return ArtifactIndexEntryV2(ref, kind, sha256_json(body), 10, available, body)


def _raw(repository, ref):
    return repository._connection.execute(
        "SELECT metadata_json FROM artifact_index WHERE artifact_ref=?", (ref,)
    ).fetchone()[0]


def _envelope(raw, *, size=None, digest=None):
    return canonical_json({STORAGE_MARKER: {
        "version": STORAGE_VERSION, "codec": "zlib",
        "uncompressed_bytes": len(raw) if size is None else size,
        "sha256": hashlib.sha256(raw).hexdigest() if digest is None else digest,
        "data": base64.b64encode(zlib.compress(raw)).decode("ascii"),
    }})


@pytest.mark.parametrize("kind", sorted(COMPRESSED_METADATA_TYPES))
def test_storage_roundtrip_restart_read_only_and_every_generic_read_path(tmp_path, kind):
    path = tmp_path / "ops.sqlite"
    entry, future = _entry(kind), _entry(kind, available=30)
    with OpsRepository(path) as repo:
        repo.register_artifacts((entry, entry, future))
        stored = _raw(repo, entry.artifact_ref)
        assert STORAGE_MARKER in json.loads(stored)
        assert len(stored.encode()) < len(canonical_json(entry.metadata).encode()) / 4
        repo.register_artifacts((entry, future))
        assert _raw(repo, entry.artifact_ref) == stored
        assert repo.get_artifact(entry.artifact_ref) == entry
        assert canonical_json(repo.get_artifact_metadata_by_refs((entry.artifact_ref,))[entry.artifact_ref]["metadata"]) == canonical_json(entry.metadata)
        assert set(repo.artifact_entries(kind)) == {entry, future}
        assert repo.artifact_entries_by_types((kind,), available_before_ns=20) == (entry,)
        latest = repo.latest_artifact_entries(kind, as_of_ns=20, limit=1)
        assert latest.entries == (entry,) and latest.invalid_entry_count == 0
        page = repo.artifact_entries_by_types_page((kind,), as_of_ns=20)
        assert page.entries == (entry,) and page.invalid_entry_count == 0
        assert repo.latest_artifact_entries(kind, as_of_ns=19, limit=1).entries == ()
    before = path.read_bytes()
    with OpsRepository(path, read_only=True) as reader, reader.read_snapshot():
        assert reader.get_artifact(entry.artifact_ref) == entry
        assert reader.latest_artifact_entries(kind, as_of_ns=20, limit=1).entries == (entry,)
        assert reader._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == 2
    assert path.read_bytes() == before


@pytest.mark.parametrize("kind", sorted(COMPRESSED_METADATA_TYPES))
def test_legacy_plain_duplicates_preserve_row_and_conflicts_remain_immutable(tmp_path, kind):
    entry = _entry(kind)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        plain = canonical_json(entry.metadata)
        repo._connection.execute("INSERT INTO artifact_index VALUES(?,?,?,?,?,?)", (
            entry.artifact_ref, kind, entry.content_hash, entry.created_at_ns, entry.available_at_ns, plain,
        ))
        rowid = repo._connection.execute("SELECT rowid FROM artifact_index").fetchone()[0]
        repo.register_artifacts((entry, entry))
        assert _raw(repo, entry.artifact_ref) == plain
        assert repo._connection.execute("SELECT rowid FROM artifact_index").fetchone()[0] == rowid
        assert repo.get_artifact(entry.artifact_ref) == entry
        for conflict in (replace(entry, metadata={"different": True}),
                         replace(entry, content_hash="f" * 64),
                         replace(entry, available_at_ns=21), replace(entry, created_at_ns=11)):
            with pytest.raises(ValueError, match="different immutable content"):
                repo.register_artifact(conflict)
        assert _raw(repo, entry.artifact_ref) == plain


def test_duplicate_uses_original_content_even_with_different_valid_zlib_encoding(tmp_path):
    entry = _entry()
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(entry)
        envelope = json.loads(_raw(repo, entry.artifact_ref))
        envelope[STORAGE_MARKER]["data"] = base64.b64encode(
            zlib.compress(canonical_json(entry.metadata).encode(), level=9)).decode("ascii")
        altered_encoding = canonical_json(envelope)
        repo._connection.execute("UPDATE artifact_index SET metadata_json=?", (altered_encoding,))
        repo.register_artifact(entry)
        assert _raw(repo, entry.artifact_ref) == altered_encoding


@pytest.mark.parametrize("size", [COMPRESSION_THRESHOLD_BYTES - 1, COMPRESSION_THRESHOLD_BYTES])
def test_compression_threshold_counts_utf8_bytes(size):
    body = {"x": "漢" * 1000}
    body["x"] += "a" * (size - len(canonical_json(body).encode()))
    plain = canonical_json(body)
    assert len(plain.encode()) == size
    stored = encode_metadata_json("UniverseObservationV2", plain)
    assert (STORAGE_MARKER in json.loads(stored)) == (size >= COMPRESSION_THRESHOLD_BYTES)
    assert decode_metadata_json("UniverseObservationV2", stored) == body


def test_incompressible_metadata_keeps_smaller_plain_encoding(tmp_path):
    generator = random.Random(41)
    entry = _entry(metadata={"original": "".join(generator.choices(string.ascii_letters + string.digits, k=8192))})
    plain = canonical_json(entry.metadata)
    assert len(plain.encode()) >= COMPRESSION_THRESHOLD_BYTES
    assert encode_metadata_json(entry.artifact_type, plain) == plain
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(entry)
        assert _raw(repo, entry.artifact_ref) == plain
        assert repo.get_artifact(entry.artifact_ref) == entry
        repo.register_artifact(entry)


@pytest.mark.parametrize("kind", ["PublicObservationIndexV2", "PublicStreamFrameIndexV1", "ExecutableQuoteV2", "CandidateSetV2", "ProductContractV2"])
def test_non_allowlisted_metadata_stays_plain(kind):
    plain = canonical_json(_entry(kind).metadata)
    assert encode_metadata_json(kind, plain) == plain
    assert canonical_json(decode_metadata_json(kind, plain)) == plain


@pytest.mark.parametrize("mutation", [
    "version", "codec", "size", "negative_size", "bool_size", "oversize", "hash", "bad_hash",
    "base64", "zlib", "truncated", "trailing", "concatenated", "extra_field", "extra_marker",
    "not_object", "noncanonical", "nested_marker", "duplicate_key", "duplicate_envelope_key",
], ids=lambda value: value)
def test_corrupt_envelopes_fail_closed_for_all_reads_and_duplicate_registration(tmp_path, mutation):
    entry = _entry()
    envelope = json.loads(encode_metadata_json(entry.artifact_type, canonical_json(entry.metadata)))
    body = envelope[STORAGE_MARKER]
    if mutation == "version":
        body["version"] = "ATLAS_COMPRESSED_METADATA_V2"
    elif mutation == "codec":
        body["codec"] = "pickle"
    elif mutation in {"size", "negative_size", "bool_size", "oversize"}:
        body["uncompressed_bytes"] = {"size": body["uncompressed_bytes"] + 1,
            "negative_size": -1, "bool_size": True, "oversize": MAX_UNCOMPRESSED_METADATA_BYTES + 1}[mutation]
    elif mutation in {"hash", "bad_hash"}:
        body["sha256"] = "0" * 64 if mutation == "hash" else "g" * 64
    elif mutation == "base64":
        body["data"] = "!invalid!"
    elif mutation == "zlib":
        body["data"] = base64.b64encode(b"not zlib").decode()
    elif mutation in {"truncated", "trailing", "concatenated"}:
        compressed = base64.b64decode(body["data"])
        compressed = compressed[:-1] if mutation == "truncated" else compressed + (
            b"trailing" if mutation == "trailing" else compressed)
        body["data"] = base64.b64encode(compressed).decode()
    elif mutation == "extra_field":
        body["unexpected"] = True
    elif mutation == "extra_marker":
        envelope["original"] = True
    elif mutation in {"not_object", "noncanonical", "nested_marker", "duplicate_key"}:
        raw = {"not_object": b'["' + b"a" * 5000 + b'"]',
            "noncanonical": b'{ "x": "' + b"a" * 5000 + b'" }',
            "nested_marker": canonical_json({STORAGE_MARKER: "a" * 5000}).encode(),
            "duplicate_key": b'{"x":1,"x":"' + b"a" * 5000 + b'"}'}[mutation]
        envelope = json.loads(_envelope(raw))
    corrupted = canonical_json(envelope)
    if mutation == "duplicate_envelope_key":
        corrupted = corrupted.replace('"codec":"zlib"', '"codec":"invalid","codec":"zlib"')
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(entry)
        repo._connection.execute("UPDATE artifact_index SET metadata_json=?", (corrupted,))
        with pytest.raises(ValueError):
            repo.get_artifact(entry.artifact_ref)
        with pytest.raises(ValueError):
            repo.get_artifact_metadata_by_refs((entry.artifact_ref,))
        with pytest.raises(ValueError):
            repo.artifact_entries(entry.artifact_type)
        with pytest.raises(ValueError):
            repo.register_artifact(entry)
        page = repo.latest_artifact_entries(entry.artifact_type, as_of_ns=20, limit=1)
        assert page.entries == () and page.invalid_entry_count == 1
        typed = repo.artifact_entries_by_types_page((entry.artifact_type,), as_of_ns=20)
        assert typed.entries == () and typed.invalid_entry_count == 1
        assert _raw(repo, entry.artifact_ref) == corrupted


def test_bomb_is_bounded_by_declared_bytes_and_global_cap():
    # This stream expands beyond 32 MiB; declaring 4096 permits only 4097
    # output bytes before rejection, with no unbounded flush/decompress call.
    bomb = _envelope(b"a" * (MAX_UNCOMPRESSED_METADATA_BYTES + 1), size=4096)
    with pytest.raises(ValueError, match="invalid size"):
        decode_metadata_json("UniverseContractV2", bomb)
    bomb = json.loads(bomb)
    bomb[STORAGE_MARKER]["uncompressed_bytes"] = MAX_UNCOMPRESSED_METADATA_BYTES
    with pytest.raises(ValueError, match="invalid size"):
        decode_metadata_json("UniverseContractV2", canonical_json(bomb))


def test_exact_32_mib_original_is_accepted_and_restored():
    plain = '{"x":"' + "a" * (MAX_UNCOMPRESSED_METADATA_BYTES - 8) + '"}'
    assert len(plain.encode()) == MAX_UNCOMPRESSED_METADATA_BYTES
    encoded = encode_metadata_json("BroadUniverseWorksetV2", plain)
    assert json.loads(encoded)[STORAGE_MARKER]["uncompressed_bytes"] == MAX_UNCOMPRESSED_METADATA_BYTES
    assert canonical_json(decode_metadata_json("BroadUniverseWorksetV2", encoded)) == plain


def test_oversized_new_write_reserved_marker_and_wrong_type_fail_closed():
    with pytest.raises(ValueError, match="uncompressed byte bound"):
        encode_metadata_json("UniverseContractV2", canonical_json({"x": "a" * MAX_UNCOMPRESSED_METADATA_BYTES}))
    with pytest.raises(ValueError, match="reserved storage marker"):
        encode_metadata_json("UniverseContractV2", canonical_json({STORAGE_MARKER: {}}))
    encoded = encode_metadata_json("UniverseContractV2", canonical_json(_entry().metadata))
    with pytest.raises(ValueError, match="invalid storage marker"):
        decode_metadata_json("PublicObservationIndexV2", encoded)


def test_actual_broad_workset_universe_hashes_membership_and_all_row_storage_reduction(tmp_path, record_property):
    from atlas.v2.runtime.broad_universe import STATE_TYPE, full_universe, latest_workset

    from .test_session041_production_breadth import NOW, product, publish

    assert STATE_TYPE == "BroadUniverseWorksetV2"
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        products = [product(index) for index in range(200)]
        body = publish(repo, products)
        universe = full_universe(repo, cutoff_ns=NOW)
        assert universe is not None and len(universe.entries) == 200
        assert set(body["product_refs"]) == {item.content_hash for item in products}
        assert universe.content_hash == body["universe_ref"]
        assert sha256_json(latest_workset(repo, cutoff_ns=NOW)) == sha256_json(body)
        rows = repo._connection.execute("SELECT * FROM artifact_index ORDER BY rowid").fetchall()
        entries = tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)
        plain_bytes = sum(len(canonical_json(entry.metadata).encode()) for entry in entries)
        stored_bytes = sum(len(row["metadata_json"].encode()) for row in rows)
        compressed_count = sum(STORAGE_MARKER in json.loads(row["metadata_json"]) for row in rows)
        assert compressed_count >= 2 and stored_bytes < plain_bytes * 0.7
        before = tuple(tuple(row) for row in rows)
        repo.register_artifacts(entries)
        assert tuple(tuple(row) for row in repo._connection.execute("SELECT * FROM artifact_index ORDER BY rowid")) == before
        baseline_path = tmp_path / "legacy-plain.sqlite"
        with OpsRepository(baseline_path) as baseline:
            with baseline._transaction() as connection:
                connection.executemany("INSERT INTO artifact_index VALUES(?,?,?,?,?,?)", (
                    (entry.artifact_ref, entry.artifact_type, entry.content_hash,
                     entry.created_at_ns, entry.available_at_ns, canonical_json(entry.metadata))
                    for entry in entries))
            assert baseline._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == len(rows)
            assert tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in baseline._connection.execute(
                "SELECT * FROM artifact_index ORDER BY rowid")) == entries
            baseline._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        repo._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        plain_sqlite_bytes, stored_sqlite_bytes = baseline_path.stat().st_size, path.stat().st_size
        assert stored_sqlite_bytes < plain_sqlite_bytes * 0.85
        record_property("all_artifact_rows", len(rows))
        record_property("compressed_rows", compressed_count)
        record_property("plain_metadata_bytes_all_rows", plain_bytes)
        record_property("stored_metadata_bytes_all_rows", stored_bytes)
        record_property("metadata_storage_reduction_fraction", 1 - stored_bytes / plain_bytes)
        record_property("plain_sqlite_bytes_all_rows", plain_sqlite_bytes)
        record_property("stored_sqlite_bytes_all_rows", stored_sqlite_bytes)
        record_property("sqlite_storage_reduction_fraction", 1 - stored_sqlite_bytes / plain_sqlite_bytes)
        record_property("universe_hash", universe.content_hash)
        record_property("workset_hash", sha256_json(body))
    with OpsRepository(path, read_only=True) as reader:
        assert canonical_json(latest_workset(reader, cutoff_ns=NOW)) == canonical_json(body)
        assert full_universe(reader, cutoff_ns=NOW).content_hash == universe.content_hash
        assert full_universe(reader, cutoff_ns=NOW).entries == universe.entries
