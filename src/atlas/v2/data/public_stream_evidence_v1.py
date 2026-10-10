"""Bounded lossless blocks for public stream evidence in ops schema 3."""
from __future__ import annotations

import hashlib
import json
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .._serialization import canonical_json, sha256_json, sha256_ref

PUBLIC_STREAM_SQL_BLOCK_V1 = "PUBLIC_STREAM_SQL_BLOCK_V1"
PUBLIC_STREAM_SQL_FORMAT_V1 = "SQL_STREAM_V1"
PUBLIC_STREAM_ARROW_FORMAT_V1 = "ARROW_EXTENT_V1"
MAX_PUBLIC_STREAM_BLOCK_BYTES_V1 = 4 * 1024 * 1024
MAX_PUBLIC_STREAM_BLOCK_RECORDS_V1 = 512


@dataclass(frozen=True)
class EncodedPublicStreamBlockV1:
    block_ref: str
    format: str
    codec: str
    decoded_sha256: str
    decoded_bytes: int
    record_count: int
    payload: bytes


def encode_public_stream_block_v1(
    records: Sequence[Mapping[str, Any]], *,
    contexts: Mapping[str, Mapping[str, Any]] | None = None,
) -> EncodedPublicStreamBlockV1:
    """Encode one self-contained, lossless block with stable content identity."""
    if not isinstance(records, (tuple, list)) or not 1 <= len(records) <= MAX_PUBLIC_STREAM_BLOCK_RECORDS_V1:
        raise ValueError("public stream block must contain 1..512 records")
    canonical_records = []
    for record in records:
        if not isinstance(record, Mapping) or any(not isinstance(key, str) for key in record):
            raise ValueError("public stream block records must be string-keyed mappings")
        canonical_records.append(dict(record))
    body: dict[str, Any] = {"version": PUBLIC_STREAM_SQL_BLOCK_V1, "records": canonical_records}
    if contexts:
        if len(contexts) > 64 or any(not isinstance(ref, str) or not isinstance(context, Mapping)
                                     for ref, context in contexts.items()):
            raise ValueError("public stream block contexts exceed their bound")
        body["contexts"] = {ref: dict(context) for ref, context in contexts.items()}
    decoded = canonical_json(body).encode("utf-8")
    if not 1 <= len(decoded) <= MAX_PUBLIC_STREAM_BLOCK_BYTES_V1:
        raise ValueError("public stream block exceeds the 4 MiB decoded bound")
    digest = hashlib.sha256(decoded).hexdigest()
    compressed = zlib.compress(decoded, level=1)
    if len(compressed) < len(decoded):
        codec, payload = "zlib-1", compressed
    else:
        codec, payload = "identity", decoded
    block_ref = sha256_json({"format": PUBLIC_STREAM_SQL_FORMAT_V1,
                             "version": PUBLIC_STREAM_SQL_BLOCK_V1,
                             "decoded_sha256": digest,
                             "decoded_bytes": len(decoded),
                             "record_count": len(canonical_records)})
    return EncodedPublicStreamBlockV1(block_ref, PUBLIC_STREAM_SQL_FORMAT_V1, codec,
                                      digest, len(decoded), len(canonical_records), payload)


def decode_public_stream_block_v1(*, block_ref: str, format: str, codec: str,
                                  decoded_sha256: str, decoded_bytes: int,
                                  record_count: int, payload: bytes) -> tuple[dict[str, Any], ...]:
    """Decode and validate the whole payload before exposing any record."""
    sha256_ref(block_ref, field="block_ref")
    sha256_ref(decoded_sha256, field="decoded_sha256")
    if (format != PUBLIC_STREAM_SQL_FORMAT_V1 or codec not in {"identity", "zlib-1"}
            or type(decoded_bytes) is not int or not 1 <= decoded_bytes <= MAX_PUBLIC_STREAM_BLOCK_BYTES_V1
            or type(record_count) is not int or not 1 <= record_count <= MAX_PUBLIC_STREAM_BLOCK_RECORDS_V1
            or not isinstance(payload, bytes) or len(payload) > MAX_PUBLIC_STREAM_BLOCK_BYTES_V1):
        raise ValueError("public stream SQL block header violates its format bounds")
    if codec == "identity":
        decoded = payload
    else:
        inflater = zlib.decompressobj()
        decoded = inflater.decompress(payload, decoded_bytes + 1)
        if (not inflater.eof or inflater.unconsumed_tail or inflater.unused_data
                or len(decoded) != decoded_bytes):
            raise ValueError("public stream SQL block compression is malformed")
    if (len(decoded) != decoded_bytes or hashlib.sha256(decoded).hexdigest() != decoded_sha256
            or sha256_json({"format": format, "version": PUBLIC_STREAM_SQL_BLOCK_V1,
                            "decoded_sha256": decoded_sha256, "decoded_bytes": decoded_bytes,
                            "record_count": record_count}) != block_ref):
        raise ValueError("public stream SQL block digest or identity mismatch")
    try:
        body = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("public stream SQL block is not valid UTF-8 JSON") from exc
    if (not isinstance(body, dict) or set(body) not in ({"version", "records"},
                                                          {"version", "records", "contexts"})
            or body["version"] != PUBLIC_STREAM_SQL_BLOCK_V1
            or not isinstance(body["records"], list) or len(body["records"]) != record_count
            or canonical_json(body).encode("utf-8") != decoded
            or any(not isinstance(row, dict) or any(not isinstance(key, str) for key in row)
                   for row in body["records"])):
        raise ValueError("public stream SQL block body is not canonical or complete")
    if "contexts" not in body:
        return tuple(body["records"])
    contexts = body["contexts"]
    if (not isinstance(contexts, dict) or len(contexts) > 64
            or any(not isinstance(ref, str) or not isinstance(context, dict)
                   or set(context) != {"initial_state_projection"}
                   or not isinstance(context["initial_state_projection"], dict)
                   for ref, context in contexts.items())):
        raise ValueError("public stream block context table is malformed")
    states = {ref: dict(context["initial_state_projection"]) for ref, context in contexts.items()}
    expanded = []
    for row in body["records"]:
        compact = row.get("compact_continuity")
        if compact is None:
            expanded.append(row)
            continue
        if (set(row) != {"artifact_ref", "artifact_type", "content_hash", "created_at_ns",
                         "available_at_ns", "compact_continuity"}
                or not isinstance(compact, dict)
                or set(compact) != {"feed_ref", "observation", "decision", "state_delta"}
                or compact["feed_ref"] not in states or not isinstance(compact["observation"], dict)
                or not isinstance(compact["decision"], dict) or not isinstance(compact["state_delta"], dict)):
            raise ValueError("public continuity compact record is malformed")
        state = dict(states[compact["feed_ref"]])
        if any(not isinstance(key, str) for key in compact["state_delta"]):
            raise ValueError("public continuity state delta has a non-string key")
        state.update(compact["state_delta"])
        states[compact["feed_ref"]] = state
        observation = compact["observation"]
        decision = compact["decision"]
        from .public_stream_continuity import (
            PublicStreamClassificationV1,
            PublicStreamContinuityStateV1,
            PublicStreamObservationV1,
        )

        if set(decision) != {"schema_version", "observation_ref", "classification", "reason_code", "state_ref"}:
            raise ValueError("public continuity compact decision fields are invalid")
        observation_value = PublicStreamObservationV1.from_dict(observation)
        state_value = PublicStreamContinuityStateV1.from_dict(state)
        if observation_value.to_dict() != observation or state_value.to_dict() != state:
            raise ValueError("public continuity compact domain decoder changed logical bytes")
        PublicStreamClassificationV1(decision["classification"])
        if type(decision["schema_version"]) is not int or decision["schema_version"] != 1:
            raise ValueError("public continuity compact decision version is invalid")
        metadata = {"version": "BROAD_PUBLIC_STREAM_CONTINUITY_V3",
                    "observation": observation, "decision": decision,
                    "state_projection": state, "authority": "ZERO"}
        domain_body = {"version": "BROAD_PUBLIC_STREAM_CONTINUITY_V3",
                       "observation": observation, "decision": decision,
                       "state_projection": state, "authority": "ZERO"}
        if (row["artifact_type"] != "BroadPublicStreamContinuityV3"
                or sha256_json({"artifact_type": "PublicStreamObservationV1",
                                "observation": observation}) != decision.get("observation_ref")
                or sha256_json({"artifact_type": "PublicStreamContinuityStateV1",
                                "state": state}) != decision.get("state_ref")
                or sha256_json({"artifact_type": "BroadPublicStreamContinuityV3",
                                "body": domain_body}) != row["content_hash"]
                or row["artifact_ref"] != row["content_hash"]):
            raise ValueError("public continuity compact record domain identity mismatch")
        expanded.append({"artifact_ref": row["artifact_ref"], "artifact_type": row["artifact_type"],
                         "content_hash": row["content_hash"], "created_at_ns": row["created_at_ns"],
                         "available_at_ns": row["available_at_ns"], "metadata": metadata})
    return tuple(expanded)
