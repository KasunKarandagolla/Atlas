"""Rebuildable compact SQLite locators for immutable public Arrow evidence.

The domain ArtifactIndexEntryV2 returned to callers keeps its exact original
wire metadata and hashes. High-frequency records avoid duplicating that full
metadata and every generic operational access index in artifact_index.
"""
from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, json_value, sha256_json
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from .public_archive_extents import extent_ref, read_extent
from .raw import RawObservationV2

LOCATOR_TABLE = "public_stream_archive_locator_v1"
LOCATOR_DDL = (
    "CREATE TABLE IF NOT EXISTS public_stream_archive_locator_v1 ("
    "artifact_ref BLOB PRIMARY KEY CHECK(length(artifact_ref)=32),kind INTEGER NOT NULL CHECK(kind IN (1,2,3)),"
    "content_hash BLOB NOT NULL CHECK(length(content_hash)=32),created_at_ns INTEGER NOT NULL,"
    "available_at_ns INTEGER NOT NULL,feed_ref BLOB,chunk_id BLOB NOT NULL CHECK(length(chunk_id)=32),"
    "record_no INTEGER NOT NULL CHECK(record_no>=0 AND record_no<512),metadata_hash BLOB,payload_hash BLOB,"
    "event_at_ns INTEGER,CHECK(available_at_ns>=created_at_ns)) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS public_stream_archive_trade_window_v1 ON public_stream_archive_locator_v1 "
    "(feed_ref,event_at_ns DESC,available_at_ns DESC,artifact_ref DESC) WHERE kind=2",
)
KINDS = {"PublicStreamFrameIndexV1": 1, "PublicStreamTradeObservationIndexV1": 2,
         "PublicStreamSourceHealthV1": 3}
NAMESPACES = {1: "ops-l2-frames", 2: "ops-observations", 3: "ops-stream-metadata"}
MAX_CACHE_CHUNKS = 8
MAX_CACHE_BYTES = 32 * 1024 * 1024


def feed_ref(key_json: str, source_id: str) -> str:
    key = InstrumentKeyV2.from_dict(json.loads(key_json))
    return sha256_json({"version": "PublicStreamIndexFeedV1", "instrument_key_json": key.to_canonical_json(),
                        "source_id": source_id, "event_type": "TRADE"})


def _rows(repository: OpsRepository, kind: int, chunk_id: str) -> tuple[dict[str, Any], ...]:
    ref = extent_ref(NAMESPACES[kind], chunk_id)
    descriptor = repository.get_artifact(ref)
    if descriptor is None:
        raise ValueError("compact public locator extent is missing")
    body = descriptor.metadata["extent"]
    path = Path(repository.path).parent / "ops-public-extents" / str(body["segment_name"])
    stat = path.stat()
    cache_key = (ref, descriptor.content_hash, stat.st_size, stat.st_mtime_ns)
    cache: OrderedDict[Any, Any] = repository._public_index_cache
    cached = cache.get(cache_key)
    if cached is not None:
        cache.move_to_end(cache_key)
        return cached[0]
    table = read_extent(repository, ref)
    rows = tuple(table.to_pylist())
    cache[cache_key] = (rows, table.nbytes)
    cache.move_to_end(cache_key)
    while len(cache) > MAX_CACHE_CHUNKS or sum(item[1] for item in cache.values()) > MAX_CACHE_BYTES:
        cache.popitem(last=False)
    return rows


def _metadata(repository: OpsRepository, kind: int, chunk_id: str, record_no: int,
              feed: str | None) -> Mapping[str, Any]:
    rows = _rows(repository, kind, chunk_id)
    if not 0 <= record_no < len(rows):
        raise ValueError("compact public locator row is missing")
    row = rows[record_no]
    if kind == 3:
        return json.loads(row["metadata_json"])
    raw = row["raw_payload_bytes"]
    if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != row["raw_payload_hash"]:
        raise ValueError("compact public locator raw byte hash mismatch")
    if kind == 1:
        instrument = InstrumentKeyV2.from_dict(row["instrument"])
        return {"record_id": row["record_id"], "instrument": instrument.to_dict(),
                "instrument_hash": instrument.content_hash, "source_id": row["source_id"],
                "channel": row["channel"], "frame_type": row["frame_type"],
                "event_at_ns": row["event_at_ns"], "received_at_ns": row["received_at_ns"],
                "available_at_ns": row["available_at_ns"], "raw_payload_hash": row["raw_payload_hash"],
                "archive_chunk_id": chunk_id, "sequence_semantics": row["sequence_semantics"], "authority": "ZERO"}
    catalog = repository.get_artifact(feed) if feed is not None else None
    if catalog is None or catalog.artifact_type != "PublicStreamIndexFeedV1":
        raise ValueError("compact trade locator feed identity missing")
    identity = json_value(catalog.metadata["feed"])
    if sha256_json(identity) != catalog.content_hash or catalog.artifact_ref != catalog.content_hash:
        raise ValueError("compact trade feed identity mismatch")
    observation = RawObservationV2.from_dict(json.loads(row["observation_json"]))
    if observation.raw_payload_hash != row["raw_payload_hash"] or row["bar_json"] != "null":
        raise ValueError("compact trade locator observation mismatch")
    return {"record_id": observation.record_id, "source_id": observation.source_id,
            "event_type": observation.event_type, "instrument_revision": observation.instrument_revision,
            "instrument_key_json": identity["instrument_key_json"], "event_at_ns": observation.event_at_ns,
            "published_at_ns": observation.published_at_ns, "translation_version": observation.translation_version,
            "revision_of": observation.revision_of, "quality_flags": list(observation.quality_flags),
            "availability_class": observation.availability_class.value,
            "replay_available_at_ns": observation.replay_available_at_ns,
            "raw_payload_hash": observation.raw_payload_hash, "bar_content_hash": None, "archive_chunk_id": chunk_id}


def encode_entries(repository: OpsRepository, entries: Sequence[ArtifactIndexEntryV2],
                   *, archive_chunk_id: str | None = None) -> tuple[tuple[Any, ...], ...]:
    encoded = []
    for entry in entries:
        kind = KINDS[entry.artifact_type]
        chunk_id = archive_chunk_id if kind == 3 else str(entry.metadata["archive_chunk_id"])
        if chunk_id is None:
            raise ValueError("compact public metadata has no archive chunk")
        rows = _rows(repository, kind, chunk_id)
        if kind == 3:
            record_no = next((i for i, row in enumerate(rows) if row["artifact_ref"] == entry.artifact_ref), None)
        else:
            record_no = next((i for i, row in enumerate(rows) if row["record_id"] == entry.metadata["record_id"]), None)
        if record_no is None:
            raise ValueError("compact public locator has no exact archived row")
        feed = None
        if kind == 2:
            key_json = str(entry.metadata["instrument_key_json"])
            source_id = str(entry.metadata["source_id"])
            feed = feed_ref(key_json, source_id)
            if repository.get_artifact(feed) is None:
                body = {"version": "PublicStreamIndexFeedV1", "instrument_key_json": key_json,
                        "source_id": source_id, "event_type": "TRADE"}
                repository.register_artifact(ArtifactIndexEntryV2(feed, "PublicStreamIndexFeedV1", feed,
                    entry.available_at_ns, entry.available_at_ns, {"feed": body}))
        original = dict(entry.metadata)
        reconstructed = _metadata(repository, kind, chunk_id, record_no, feed)
        if canonical_json(original) != canonical_json(reconstructed):
            raise ValueError("compact public locator metadata differs from immutable archive")
        digest = sha256_json(reconstructed)
        if kind == 1 and entry.content_hash != digest:
            raise ValueError("compact frame index content hash mismatch")
        encoded.append((bytes.fromhex(entry.artifact_ref), kind, bytes.fromhex(entry.content_hash),
            entry.created_at_ns, entry.available_at_ns, bytes.fromhex(feed) if feed else None,
            bytes.fromhex(chunk_id), record_no, bytes.fromhex(digest) if kind != 1 else None,
            bytes.fromhex(str(entry.metadata["raw_payload_hash"])) if kind != 3 else None,
            entry.metadata.get("event_at_ns")))
    return tuple(encoded)


def decode_row(repository: OpsRepository, row: Any) -> ArtifactIndexEntryV2:
    kind = row["kind"]
    content_hash = bytes(row["content_hash"]).hex()
    feed = bytes(row["feed_ref"]).hex() if row["feed_ref"] is not None else None
    metadata = _metadata(repository, kind, bytes(row["chunk_id"]).hex(), row["record_no"], feed)
    expected = bytes(row["metadata_hash"]).hex() if row["metadata_hash"] is not None else content_hash
    if sha256_json(metadata) != expected:
        raise ValueError("compact public locator metadata hash mismatch")
    artifact_type = next(name for name, value in KINDS.items() if value == kind)
    entry = ArtifactIndexEntryV2(bytes(row["artifact_ref"]).hex(), artifact_type, content_hash,
        row["created_at_ns"], row["available_at_ns"], metadata)
    if kind in (1, 2):
        if entry.artifact_ref != sha256_json({"artifact_type": artifact_type, "record_id": metadata["record_id"]}):
            raise ValueError("compact public locator artifact identity mismatch")
        if bytes(row["payload_hash"]).hex() != metadata["raw_payload_hash"]:
            raise ValueError("compact public locator payload hash mismatch")
    archived = _rows(repository, kind, bytes(row["chunk_id"]).hex())[row["record_no"]]
    if kind == 1:
        if not entry.created_at_ns == entry.available_at_ns == metadata["available_at_ns"]:
            raise ValueError("compact frame locator chronology mismatch")
    elif kind == 2:
        observation = RawObservationV2.from_dict(json.loads(archived["observation_json"]))
        if (observation.content_hash != entry.content_hash
                or observation.available_at_ns != entry.available_at_ns
                or observation.received_at_ns != entry.created_at_ns):
            raise ValueError("compact trade locator content or chronology mismatch")
    elif kind == 3:
        from .health import PublicSourceHealthV2

        health = PublicSourceHealthV2.from_dict(json_value(metadata["health"]))
        if (health.content_hash != entry.content_hash or health.content_hash != entry.artifact_ref
                or archived["content_hash"] != entry.content_hash
                or archived["available_at_ns"] != entry.available_at_ns
                or archived["created_at_ns"] != entry.created_at_ns):
            raise ValueError("compact source health content or chronology mismatch")
    return entry
