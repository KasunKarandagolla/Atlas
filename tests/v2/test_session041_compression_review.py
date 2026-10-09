"""Independent review of storage decoding, causal reads and exporter hydration."""

import base64
import hashlib
import json
import zlib

import pyarrow.parquet as pq
import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.memory import compressed_metadata as codec
from atlas.v2.memory.repository import (
    _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS,
    ArtifactIndexEntryV2,
    OpsRepository,
)
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot


def _entry(kind, index=0, *, created=10, available=20):
    metadata = {"index": index, "membership": ["δ漢" * 2048], "authority": "ZERO"}
    ref = sha256_json({"kind": kind, "metadata": metadata})
    return ArtifactIndexEntryV2(ref, kind, sha256_json(metadata), created, available, metadata)


def _envelope(raw, *, size=None):
    return canonical_json({codec.STORAGE_MARKER: {
        "version": codec.STORAGE_VERSION, "codec": "zlib",
        "uncompressed_bytes": len(raw) if size is None else size,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "data": base64.b64encode(zlib.compress(raw)).decode("ascii"),
    }})


def test_allowlist_is_exact_and_has_no_sql_metadata_identity_projection(tmp_path):
    assert {
        "UniverseContractV2", "BroadUniverseWorksetV2", "UniverseObservationV2",
    } == codec.COMPRESSED_METADATA_TYPES
    assert codec.MAX_UNCOMPRESSED_METADATA_BYTES == 32 * 1024 * 1024
    assert codec.COMPRESSION_THRESHOLD_BYTES == 4096
    assert not codec.COMPRESSED_METADATA_TYPES.intersection(
        kind for kind, _ in _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        indexes = repo._connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall()
        for row in indexes:
            if "json_extract" in row[0]:
                assert not any(kind in row[0] for kind in codec.COMPRESSED_METADATA_TYPES)
        for kind in codec.COMPRESSED_METADATA_TYPES:
            with pytest.raises(ValueError, match="no bounded artifact index"):
                repo.latest_artifact_entries(kind, as_of_ns=20, limit=1,
                    metadata_path=("authority",), identity_value="ZERO")
            with pytest.raises(ValueError, match="no bounded artifact index"):
                repo.artifact_entries_by_metadata_identity(kind, ("authority",), "ZERO",
                    as_of_ns=20, limit=1)


def test_batched_metadata_crosses_500_ref_boundary_with_mixed_storage(tmp_path):
    kinds = sorted(codec.COMPRESSED_METADATA_TYPES)
    entries = tuple(_entry(kinds[index % 3], index) for index in range(507))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        # Insert alternating legacy rows directly; normal registration must
        # retain those original bytes while compressing only newly added rows.
        repo._connection.executemany("INSERT INTO artifact_index VALUES(?,?,?,?,?,?)", (
            (entry.artifact_ref, entry.artifact_type, entry.content_hash,
             entry.created_at_ns, entry.available_at_ns, canonical_json(entry.metadata))
            for entry in entries[::2]))
        repo.register_artifacts(entries)
        rows_before = tuple(tuple(row) for row in repo._connection.execute(
            "SELECT rowid,* FROM artifact_index ORDER BY rowid"))
        missing = "f" * 64
        refs = [entry.artifact_ref for entry in reversed(entries)] + [entries[0].artifact_ref, missing]
        metadata = repo.get_artifact_metadata_by_refs(refs)
        assert set(metadata) == {entry.artifact_ref for entry in entries}
        for entry in entries:
            actual = metadata[entry.artifact_ref]
            assert actual["artifact_type"] == entry.artifact_type
            assert actual["content_hash"] == entry.content_hash
            assert actual["available_at_ns"] == entry.available_at_ns
            assert canonical_json(actual["metadata"]) == canonical_json(entry.metadata)
        repo.register_artifacts(tuple(reversed(entries)))
        assert tuple(tuple(row) for row in repo._connection.execute(
            "SELECT rowid,* FROM artifact_index ORDER BY rowid")) == rows_before


@pytest.mark.parametrize("kind", sorted(codec.COMPRESSED_METADATA_TYPES))
def test_invalid_compressed_page_preserves_cursor_and_future_availability(tmp_path, kind):
    good = _entry(kind, 0, created=10, available=19)
    corrupt = _entry(kind, 1, created=11)
    future = _entry(kind, 2, created=12, available=21)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifacts((good, corrupt, future))
        stored = repo._connection.execute(
            "SELECT metadata_json FROM artifact_index WHERE artifact_ref=?",
            (corrupt.artifact_ref,)).fetchone()[0]
        envelope = json.loads(stored)
        envelope[codec.STORAGE_MARKER]["sha256"] = "0" * 64
        repo._connection.execute("UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
            (canonical_json(envelope), corrupt.artifact_ref))
        page = repo.artifact_entries_by_types_page((kind,), as_of_ns=20, limit=1)
        assert page.entries == () and page.invalid_entry_count == 1
        assert page.raw_keys == ((corrupt.created_at_ns, corrupt.artifact_ref),)
        assert page.next_cursor == page.raw_keys[0]
        next_page = repo.artifact_entries_by_types_page((kind,), as_of_ns=20,
            after=page.next_cursor, limit=1)
        assert next_page.entries == (good,) and next_page.invalid_entry_count == 0
        latest = repo.latest_artifact_entries(kind, as_of_ns=20, limit=1)
        assert latest.entries == () and latest.invalid_entry_count == 1 and latest.has_more
        with pytest.raises(ValueError):
            repo.artifact_entries_by_types((kind,), available_before_ns=20)


@pytest.mark.parametrize("mutation", [
    "envelope_null", "envelope_list", "data_bool", "data_null", "digest_int",
    "size_float", "size_string", "envelope_whitespace", "bad_utf8", "deep_json",
])
def test_additional_malformed_envelopes_raise_value_error(mutation):
    plain = canonical_json(_entry("UniverseContractV2").metadata)
    envelope = json.loads(codec.encode_metadata_json("UniverseContractV2", plain))
    body = envelope[codec.STORAGE_MARKER]
    changes = {"data_bool": ("data", True), "data_null": ("data", None),
        "digest_int": ("sha256", 123), "size_float": ("uncompressed_bytes", float(len(plain.encode()))),
        "size_string": ("uncompressed_bytes", str(len(plain.encode())))}
    if mutation in changes:
        key, value = changes[mutation]
        body[key] = value
    elif mutation.startswith("envelope_") and mutation != "envelope_whitespace":
        envelope[codec.STORAGE_MARKER] = None if mutation == "envelope_null" else []
    elif mutation == "bad_utf8":
        envelope = json.loads(_envelope(b'{"x":"' + b"\xff" * 5000 + b'"}'))
    elif mutation == "deep_json":
        envelope = json.loads(_envelope(b'{"x":' + b"[" * 3000 + b"0" + b"]" * 3000 + b"}"))
    stored = canonical_json(envelope)
    if mutation == "envelope_whitespace":
        stored = " " + stored
    with pytest.raises(ValueError):
        codec.decode_metadata_json("UniverseContractV2", stored)


def test_bomb_decompression_requests_only_declared_size_plus_one(monkeypatch):
    stored = _envelope(b"a" * (8 * 1024 * 1024), size=4096)
    original = zlib.decompressobj
    calls = []

    class ObservedStream:
        def __init__(self):
            self.stream = original()

        def decompress(self, compressed, max_length):
            calls.append(max_length)
            return self.stream.decompress(compressed, max_length)

        def __getattr__(self, name):
            if name == "flush":
                pytest.fail("bounded decoder must never flush unlimited output")
            return getattr(self.stream, name)

    monkeypatch.setattr(codec.zlib, "decompressobj", ObservedStream)
    with pytest.raises(ValueError, match="invalid size"):
        codec.decode_metadata_json("UniverseObservationV2", stored)
    assert calls == [4097]


@pytest.mark.parametrize("legacy_scan", [False, True])
@pytest.mark.parametrize("corrupt_workset", [False, True])
def test_exporter_both_raw_sql_paths_restore_or_reject_compressed_metadata(tmp_path, legacy_scan, corrupt_workset):
    from .test_session041_production_breadth import NOW, product, publish

    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        workset = publish(repo, [product(index) for index in range(64)])
        expected = {entry.artifact_ref: entry for kind in
            ("UniverseContractV2", "BroadUniverseWorksetV2") for entry in repo.artifact_entries(kind)}
        assert {entry.artifact_type for entry in expected.values()} == {
            "UniverseContractV2", "BroadUniverseWorksetV2"}
        for ref in expected:
            stored = repo._connection.execute(
                "SELECT metadata_json FROM artifact_index WHERE artifact_ref=?", (ref,)).fetchone()[0]
            assert set(json.loads(stored)) == {codec.STORAGE_MARKER}
        if corrupt_workset:
            ref = sha256_json(workset)
            stored = repo._connection.execute(
                "SELECT metadata_json FROM artifact_index WHERE artifact_ref=?", (ref,)).fetchone()[0]
            envelope = json.loads(stored)
            envelope[codec.STORAGE_MARKER]["sha256"] = "0" * 64
            repo._connection.execute("UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
                (canonical_json(envelope), ref))
        if legacy_scan:
            repo._connection.execute("DROP INDEX artifact_type_insertion_lookup")
        repo._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    before = path.read_bytes()
    identity = TuningRunIdentityV1("compression-review", "a" * 64, "b" * 40, 0)
    manifest = export_tuning_snapshot(path, tmp_path / "reports", identity, cutoff_ns=NOW)
    assert manifest["validation_failures"] == ({"BroadUniverseWorksetV2": 1} if corrupt_workset else {})
    assert manifest["source_window_scope"] == (
        "LEGACY_BOUNDED_SOURCE_ROW_WINDOW" if legacy_scan else "INDEXED_RELEVANT_TYPES_IN_INSERTION_ORDER")
    rows = pq.read_table(tmp_path / "reports" / identity.run_id / manifest["partition"]).to_pylist()
    exported = {row["artifact_ref"]: row for row in rows if row["artifact_ref"] in expected}
    assert set(exported) == set(expected)
    for ref, entry in expected.items():
        if corrupt_workset and entry.artifact_type == "BroadUniverseWorksetV2":
            assert exported[ref]["row_kind"] == "INVALID"
            assert exported[ref]["evidence_payload_json"] is None
            assert exported[ref]["reason_codes"] == ["INDEXED_EVIDENCE_FAILED_VALIDATION"]
            continue
        payload = json.loads(exported[ref]["evidence_payload_json"])
        original = entry.metadata["universe" if entry.artifact_type == "UniverseContractV2" else "workset"]
        assert canonical_json(payload) == canonical_json(original)
        assert codec.STORAGE_MARKER not in payload
    if not corrupt_workset:
        assert json.loads(exported[sha256_json(workset)]["evidence_payload_json"]) == workset
    assert path.read_bytes() == before
