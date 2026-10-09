"""Bounded shared metadata blocks for high-rate public observation indexes."""

from __future__ import annotations

import hashlib
import hmac
import json
import zlib
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .._serialization import FrozenMap, canonical_json, sha256_json

IDENTITY_TABLE = "public_instrument_identity_v1"
BLOCK_TABLE = "public_observation_metadata_block_v1"
LOCATOR_TABLE = "public_observation_metadata_locator_v1"
MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_BLOCK_ROWS = 512
MAX_BLOCK_BYTES = 4 * 1024 * 1024
MAX_COMPRESSED_BYTES = MAX_BLOCK_BYTES + 65536
MAX_CACHE_BLOCKS = 8
MAX_CACHE_BYTES = 8 * 1024 * 1024
IDENTITY_DDL = (
    "CREATE TABLE IF NOT EXISTS public_instrument_identity_v1 ("
    "instrument_key_ref BLOB PRIMARY KEY CHECK(length(instrument_key_ref)=32),"
    "canonical_key_json TEXT NOT NULL, key_sha256 BLOB NOT NULL CHECK(length(key_sha256)=32)) WITHOUT ROWID",
)
BLOCK_DDL = (
    "CREATE TABLE IF NOT EXISTS public_observation_metadata_block_v1 ("
    "block_ref BLOB PRIMARY KEY CHECK(length(block_ref)=32),"
    "entry_count INTEGER NOT NULL CHECK(entry_count BETWEEN 1 AND 512),"
    "revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),"
    "uncompressed_bytes INTEGER NOT NULL CHECK(uncompressed_bytes BETWEEN 1 AND 4194304),"
    "codec TEXT NOT NULL CHECK(codec='zlib-v1'), compressed_metadata BLOB NOT NULL "
    "CHECK(length(compressed_metadata) BETWEEN 1 AND 4259840)) WITHOUT ROWID",
)
LOCATOR_DDL = (
    "CREATE TABLE IF NOT EXISTS public_observation_metadata_locator_v1 ("
    "artifact_ref BLOB PRIMARY KEY CHECK(length(artifact_ref)=32),"
    "block_ref BLOB NOT NULL REFERENCES public_observation_metadata_block_v1(block_ref),"
    "ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 511), UNIQUE(block_ref,ordinal)) WITHOUT ROWID",
    # UNIQUE(block_ref,ordinal) already supplies the same access path.
    "DROP INDEX IF EXISTS public_observation_metadata_block_order_v1",
)
_DOMAIN_REQUIRED = frozenset({"instrument_key_json", "record_id", "source_id", "raw_payload_hash"})
_PROJECTION_FIELDS = (
    "record_id", "source_id", "event_type", "instrument_revision", "event_at_ns",
    "availability_class", "replay_available_at_ns", "raw_payload_hash", "bar_content_hash",
    "archive_chunk_id",
)
_BLOCK_VERSION = "PublicObservationMetadataBlockV1"


def instrument_identity_ref(canonical_key_json: str) -> str:
    if not isinstance(canonical_key_json, str) or not canonical_key_json:
        raise ValueError("public observation instrument identity is missing")
    try:
        key = json.loads(canonical_key_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("public observation instrument identity is invalid JSON") from error
    if not isinstance(key, Mapping) or canonical_json(key) != canonical_key_json:
        raise ValueError("public observation instrument identity is not canonical JSON")
    return sha256_json({"version": "PublicInstrumentIdentityRefV1",
                        "instrument_key_json": canonical_key_json})


def compact_projection(metadata: Mapping[str, Any]) -> tuple[str, str] | None:
    """Return only fields used by indexed public queries and the full-key digest."""
    if not _DOMAIN_REQUIRED.issubset(metadata):
        return None
    key_json = metadata.get("instrument_key_json")
    if not isinstance(key_json, str):
        return None
    key_ref = instrument_identity_ref(key_json)
    projection = {name: metadata[name] for name in _PROJECTION_FIELDS if name in metadata}
    projection["instrument_key_ref"] = key_ref
    if not isinstance(projection.get("record_id"), str):
        raise ValueError("public observation projection lacks a record identity")
    return canonical_json(projection), key_ref


def encode_blocks(entries: Sequence[tuple[str, Mapping[str, Any]]]) -> tuple[dict[str, Any], ...]:
    """Create deterministic zlib blocks under fixed row and byte ceilings."""
    ordered = tuple(sorted(entries, key=lambda item: item[0]))
    if len({ref for ref, _metadata in ordered}) != len(ordered):
        raise ValueError("public metadata block repeats an artifact identity")
    groups: list[list[tuple[str, Mapping[str, Any]]]] = []
    current: list[tuple[str, Mapping[str, Any]]] = []
    current_bytes = 0
    for ref, metadata in ordered:
        raw_metadata = canonical_json(metadata).encode("utf-8")
        if len(raw_metadata) > MAX_METADATA_BYTES:
            raise ValueError("public observation metadata exceeds its bounded storage size")
        projected = len(raw_metadata) + len(ref) + 32
        if current and (len(current) >= MAX_BLOCK_ROWS or current_bytes + projected > MAX_BLOCK_BYTES):
            groups.append(current)
            current, current_bytes = [], 0
        if projected > MAX_BLOCK_BYTES:
            raise ValueError("one public observation exceeds the metadata block byte budget")
        current.append((ref, metadata))
        current_bytes += projected
    if current:
        groups.append(current)

    result: list[dict[str, Any]] = []
    for group in groups:
        body = {"version": _BLOCK_VERSION, "entries": [
            {"artifact_ref": ref, "metadata": dict(metadata)} for ref, metadata in group
        ]}
        uncompressed = canonical_json(body).encode("utf-8")
        if len(uncompressed) > MAX_BLOCK_BYTES:
            raise ValueError("public metadata block exceeds its canonical byte bound")
        block_ref = hashlib.sha256(uncompressed).digest()
        result.append({
            "block_ref": block_ref,
            "entry_count": len(group),
            "uncompressed_bytes": len(uncompressed),
            "compressed_metadata": zlib.compress(uncompressed, 6),
            "locators": tuple(
                (bytes.fromhex(ref), block_ref, ordinal)
                for ordinal, (ref, _metadata) in enumerate(group)
            ),
        })
    return tuple(result)


def register_identity(connection: Any, *, key_ref: str, canonical_key_json: str) -> None:
    ref_bytes = bytes.fromhex(key_ref)
    digest = hashlib.sha256(canonical_key_json.encode("utf-8")).digest()
    previous = connection.execute(
        "SELECT canonical_key_json,key_sha256 FROM public_instrument_identity_v1 WHERE instrument_key_ref=?",
        (ref_bytes,),
    ).fetchone()
    if previous is not None:
        if (previous["canonical_key_json"] != canonical_key_json
                or not hmac.compare_digest(bytes(previous["key_sha256"]), digest)):
            raise ValueError("public instrument identity reference collision")
        return
    connection.execute("INSERT INTO public_instrument_identity_v1 VALUES(?,?,?)",
                       (ref_bytes, canonical_key_json, digest))


@dataclass(frozen=True)
class _DecodedEntry:
    artifact_ref: str
    metadata: FrozenMap
    projection_json: str
    key_ref: str
    key_json: str
    key_digest: bytes


def decode_metadata(connection: Any, *, artifact_ref: str, projection_json: str,
                     cache: OrderedDict[Any, Any] | None = None) -> Mapping[str, Any] | None:
    """Resolve and verify one public metadata row, retaining legacy plain rows."""
    projection = json.loads(projection_json)
    if not isinstance(projection, Mapping) or "instrument_key_ref" not in projection:
        return None
    if "instrument_key_json" in projection:
        raise ValueError("compact public observation projection contains full domain fields")
    locator = connection.execute(
        "SELECT l.block_ref,l.ordinal,b.entry_count,b.uncompressed_bytes,b.revision,b.codec,"
        "length(b.compressed_metadata) AS compressed_bytes "
        "FROM public_observation_metadata_locator_v1 l "
        "JOIN public_observation_metadata_block_v1 b USING(block_ref) WHERE l.artifact_ref=?",
        (bytes.fromhex(artifact_ref),),
    ).fetchone()
    if locator is None:
        raise ValueError("compact public observation metadata locator is missing")
    block_ref = bytes(locator["block_ref"])
    size, count, codec = locator["uncompressed_bytes"], locator["entry_count"], locator["codec"]
    if (type(size) is not int or not 1 <= size <= MAX_BLOCK_BYTES
            or type(count) is not int or not 1 <= count <= MAX_BLOCK_ROWS or codec != "zlib-v1"
            or type(locator["compressed_bytes"]) is not int
            or not 1 <= locator["compressed_bytes"] <= MAX_COMPRESSED_BYTES):
        raise ValueError("compact public metadata block bounds or codec are invalid")
    # A small revision changes on any block payload edit, including edits
    # by another process; unrelated artifact writes keep verified blocks hot.
    cache_key = (block_ref, size, count, locator["revision"])
    entries: tuple[_DecodedEntry, ...] | None = None
    if cache is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            entries = cached[0]
            cache.move_to_end(cache_key)
    if entries is None:
        stored = connection.execute(
            f"SELECT CASE WHEN length(compressed_metadata) BETWEEN 1 AND {MAX_COMPRESSED_BYTES} "
            "THEN compressed_metadata END AS compressed_metadata "
            "FROM public_observation_metadata_block_v1 WHERE block_ref=?", (block_ref,)).fetchone()
        if stored is None or stored["compressed_metadata"] is None:
            raise ValueError("compact public metadata block payload is missing or oversized")
        compressed = bytes(stored["compressed_metadata"])
        decoder = zlib.decompressobj()
        try:
            raw = decoder.decompress(compressed, size + 1)
        except zlib.error as error:
            raise ValueError("compact public metadata block has an invalid zlib stream") from error
        if len(raw) != size or not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
            raise ValueError("compact public metadata block has an invalid zlib stream")
        if not hmac.compare_digest(hashlib.sha256(raw).digest(), block_ref):
            raise ValueError("compact public metadata block hash mismatch")
        body = json.loads(raw.decode("utf-8"))
        if (not isinstance(body, Mapping) or set(body) != {"version", "entries"}
                or body["version"] != _BLOCK_VERSION or not isinstance(body["entries"], list)
                or len(body["entries"]) != count or canonical_json(body).encode("utf-8") != raw):
            raise ValueError("compact public metadata block is not canonical or complete")
        decoded: list[_DecodedEntry] = []
        identities: dict[str, tuple[str, bytes]] = {}
        seen_refs: set[str] = set()
        for item in body["entries"]:
            if (not isinstance(item, Mapping) or set(item) != {"artifact_ref", "metadata"}
                    or not isinstance(item["artifact_ref"], str) or not isinstance(item["metadata"], Mapping)):
                raise ValueError("compact public metadata block entry is malformed")
            ref, metadata = item["artifact_ref"], item["metadata"]
            if ref in seen_refs or ref != sha256_json({"artifact_type": "PublicObservationIndexV2",
                                                     "record_id": metadata.get("record_id")}):
                raise ValueError("compact public metadata record identity mismatch")
            seen_refs.add(ref)
            key_json = metadata.get("instrument_key_json")
            if not isinstance(key_json, str):
                raise ValueError("compact public metadata instrument identity is missing")
            identity = identities.get(key_json)
            if identity is None:
                identity = (instrument_identity_ref(key_json),
                            hashlib.sha256(key_json.encode("utf-8")).digest())
                identities[key_json] = identity
            key_ref, key_digest = identity
            projected = {name: metadata[name] for name in _PROJECTION_FIELDS if name in metadata}
            projected["instrument_key_ref"] = key_ref
            decoded.append(_DecodedEntry(ref, FrozenMap(metadata), canonical_json(projected),
                                         key_ref, key_json, key_digest))
        entries = tuple(decoded)
        if cache is not None and size <= MAX_CACHE_BYTES:
            cache[cache_key] = (entries, size)
            cache.move_to_end(cache_key)
            while (len(cache) > MAX_CACHE_BLOCKS
                   or sum(int(value[1]) for value in cache.values()) > MAX_CACHE_BYTES):
                cache.popitem(last=False)
    ordinal = locator["ordinal"]
    if type(ordinal) is not int or not 0 <= ordinal < len(entries):
        raise ValueError("compact public metadata locator ordinal is invalid")
    item = entries[ordinal]
    if item.artifact_ref != artifact_ref:
        raise ValueError("compact public metadata locator points to another artifact")
    if item.projection_json != projection_json:
        raise ValueError("compact public metadata projection differs from canonical domain metadata")
    identity = connection.execute(
        "SELECT canonical_key_json,key_sha256 FROM public_instrument_identity_v1 WHERE instrument_key_ref=?",
        (bytes.fromhex(item.key_ref),),
    ).fetchone()
    if identity is None or identity["canonical_key_json"] != item.key_json or not hmac.compare_digest(
            bytes(identity["key_sha256"]), item.key_digest):
        raise ValueError("compact public instrument identity catalog is missing or inconsistent")
    return item.metadata
