"""Versioned, lossless storage encoding for large universe index metadata.

This is a repository representation only: decoded domain metadata and artifact
identity are unchanged. Legacy plain JSON rows remain readable and append-only.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import zlib
from collections.abc import Mapping
from typing import Any

from .._serialization import canonical_json

COMPRESSED_METADATA_TYPES = frozenset({
    "UniverseContractV2", "BroadUniverseWorksetV2", "UniverseObservationV2",
})
COMPRESSION_THRESHOLD_BYTES = 4096
MAX_UNCOMPRESSED_METADATA_BYTES = 32 * 1024 * 1024
STORAGE_MARKER = "__atlas_metadata_storage__"
STORAGE_VERSION = "ATLAS_COMPRESSED_METADATA_V1"
_MAX_COMPRESSED_BYTES = MAX_UNCOMPRESSED_METADATA_BYTES + 65536
_MAX_BASE64_CHARACTERS = 4 * ((_MAX_COMPRESSED_BYTES + 2) // 3)
_ENVELOPE_FIELDS = frozenset({"version", "codec", "uncompressed_bytes", "sha256", "data"})


def encode_metadata_json(artifact_type: str, metadata_json: str) -> str:
    """Compress allowlisted canonical UTF-8 JSON at the fixed byte threshold."""
    if artifact_type == "PublicObservationIndexV2":
        return metadata_json
    if artifact_type not in COMPRESSED_METADATA_TYPES:
        return metadata_json
    metadata = json.loads(metadata_json)
    if STORAGE_MARKER in metadata:
        raise ValueError("artifact metadata contains the reserved storage marker")
    raw = metadata_json.encode("utf-8")
    if len(raw) < COMPRESSION_THRESHOLD_BYTES:
        return metadata_json
    if len(raw) > MAX_UNCOMPRESSED_METADATA_BYTES:
        raise ValueError("compressed artifact metadata exceeds the uncompressed byte bound")
    encoded = canonical_json({STORAGE_MARKER: {
        "version": STORAGE_VERSION,
        "codec": "zlib",
        "uncompressed_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "data": base64.b64encode(zlib.compress(raw, level=6)).decode("ascii"),
    }})
    return encoded if len(encoded.encode("utf-8")) < len(raw) else metadata_json


def decode_metadata_json(artifact_type: str, metadata_json: str) -> Mapping[str, Any]:
    """Decode plain or compressed storage JSON, rejecting corrupt envelopes."""
    try:
        metadata = json.loads(metadata_json)
        if not isinstance(metadata, Mapping):
            raise ValueError("persisted artifact metadata must be a JSON object")
        if artifact_type == "PublicObservationIndexV2" and "instrument_key_ref" in metadata:
            raise ValueError("compact public observation projection requires repository hydration")
        if STORAGE_MARKER not in metadata:
            return metadata
        if artifact_type not in COMPRESSED_METADATA_TYPES or set(metadata) != {STORAGE_MARKER}:
            raise ValueError("compressed artifact metadata has an invalid storage marker")
        envelope = metadata[STORAGE_MARKER]
        if not isinstance(envelope, dict) or set(envelope) != _ENVELOPE_FIELDS:
            raise ValueError("compressed artifact metadata has malformed envelope fields")
        if envelope["version"] != STORAGE_VERSION or envelope["codec"] != "zlib":
            raise ValueError("compressed artifact metadata has an unsupported version or codec")
        size, digest, data = envelope["uncompressed_bytes"], envelope["sha256"], envelope["data"]
        if type(size) is not int or not COMPRESSION_THRESHOLD_BYTES <= size <= MAX_UNCOMPRESSED_METADATA_BYTES:
            raise ValueError("compressed artifact metadata has an invalid uncompressed byte count")
        if (not isinstance(digest, str) or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)):
            raise ValueError("compressed artifact metadata has an invalid SHA-256")
        if not isinstance(data, str) or len(data) > _MAX_BASE64_CHARACTERS:
            raise ValueError("compressed artifact metadata exceeds the encoded byte bound")
        compressed = base64.b64decode(data, validate=True)
        if len(compressed) > _MAX_COMPRESSED_BYTES:
            raise ValueError("compressed artifact metadata exceeds the compressed byte bound")
        stream = zlib.decompressobj()
        raw = stream.decompress(compressed, size + 1)
        if (len(raw) != size or not stream.eof or stream.unconsumed_tail or stream.unused_data):
            raise ValueError("compressed artifact metadata has invalid size or trailing/incomplete stream")
        if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), digest):
            raise ValueError("compressed artifact metadata SHA-256 mismatch")
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, Mapping) or STORAGE_MARKER in decoded:
            raise ValueError("compressed artifact metadata must decode to original domain object")
        if canonical_json(decoded).encode("utf-8") != raw:
            raise ValueError("compressed artifact metadata is not canonical JSON")
        # Check the envelope itself too: duplicate keys must never be accepted.
        if canonical_json(metadata) != metadata_json:
            raise ValueError("compressed artifact metadata envelope is not canonical JSON")
        return decoded
    except (binascii.Error, zlib.error, UnicodeError, RecursionError, TypeError) as error:
        raise ValueError("compressed artifact metadata decoding failed") from error
