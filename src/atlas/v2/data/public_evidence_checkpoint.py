"""Compact immutable public evidence lineage; operational caches are not evidence.

Book links bind bounded ordered archive chunks to a parent link. Raw Arrow/Parquet
remains authoritative; a full audit replays that chain, rather than copying
all prior frame hashes into every current BBO. No warm book is restored on
controller restart.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from .._serialization import json_value, sha256_json, sha256_ref, timestamp
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from .microstructure import SequenceValidBookV2
from .public_stream_continuity import PublicStreamContinuityStateV1

CONTINUITY_CHECKPOINT_TYPE = "PublicStreamContinuityCheckpointV2"
BOOK_CHECKPOINT_TYPE = "PublicBookLineageCheckpointV1"
MAX_CHUNKS_PER_CHECKPOINT = 32


def persist_continuity_checkpoint(repository: OpsRepository, state: PublicStreamContinuityStateV1,
                                 *, available_at_ns: int, prior_ref: str | None,
                                 transport_ref: str | None, clock_ns: Callable[[], int] | None = None) -> str:
    # These are lookup accelerators, not source facts. Persist scalar evidence
    # and mark the discarded trade lookup incomplete so restart must consult
    # the durable exact trade-ID index (or fail closed).
    started = max(available_at_ns, state.last_available_at_ns or 0, clock_ns() if clock_ns else available_at_ns)
    scalar = replace(state, trade_identity_cache=(), observation_replay_cache=(),
                     trade_identity_cache_complete=state.observed_trade_count == 0)
    scalar_body = scalar.to_dict()
    available = max(started, clock_ns() if clock_ns else started)
    body = {"version": "PUBLIC_STREAM_CONTINUITY_CHECKPOINT_V2", "state": scalar_body,
            "prior_checkpoint_ref": prior_ref, "transport_batch_ref": transport_ref,
            "available_at_ns": available, "computation_started_ns": started,
            "computation_finished_ns": available, "authority": "ZERO"}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(ref, CONTINUITY_CHECKPOINT_TYPE, ref,
                                started, available, {"checkpoint": body}))
    return ref


def decode_continuity_checkpoint(entry: ArtifactIndexEntryV2) -> PublicStreamContinuityStateV1:
    if entry.artifact_type == "PublicStreamContinuityStateV1":
        state = PublicStreamContinuityStateV1.from_dict(json_value(entry.metadata["state"]))
        if state.content_hash != entry.artifact_ref or state.content_hash != entry.content_hash:
            raise ValueError("legacy continuity state identity mismatch")
        return state
    body = json_value(entry.metadata["checkpoint"])
    if (entry.artifact_type != CONTINUITY_CHECKPOINT_TYPE
            or set(body) != {"version", "state", "prior_checkpoint_ref", "transport_batch_ref",
                            "available_at_ns", "authority", "computation_started_ns", "computation_finished_ns"}
            or body["version"] != "PUBLIC_STREAM_CONTINUITY_CHECKPOINT_V2"
            or body["authority"] != "ZERO" or sha256_json(body) != entry.artifact_ref
            or entry.content_hash != entry.artifact_ref
            or body["available_at_ns"] != entry.available_at_ns
            or entry.created_at_ns != body["computation_started_ns"]
            or not entry.created_at_ns <= body["computation_finished_ns"] == entry.available_at_ns):
        raise ValueError("compact continuity checkpoint identity mismatch")
    for name in ("computation_started_ns", "computation_finished_ns", "available_at_ns"):
        timestamp(body[name], field=name)
    for key in ("prior_checkpoint_ref", "transport_batch_ref"):
        if body[key] is not None:
            sha256_ref(body[key], field=key)
    state = PublicStreamContinuityStateV1.from_dict(body["state"])
    if (state.trade_identity_cache or state.observation_replay_cache
            or state.trade_identity_cache_complete != (state.observed_trade_count == 0)
            or (state.last_available_at_ns or 0) > entry.available_at_ns):
        raise ValueError("compact checkpoint contains caches or noncausal state")
    return state


def validate_continuity_checkpoint(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> PublicStreamContinuityStateV1:
    state = decode_continuity_checkpoint(entry)
    if entry.artifact_type == CONTINUITY_CHECKPOINT_TYPE:
        body = json_value(entry.metadata["checkpoint"])
        parent = body["prior_checkpoint_ref"]
        if parent is not None:
            prior = repository.get_artifact(parent)
            if prior is None or prior.available_at_ns > entry.available_at_ns:
                raise ValueError("continuity parent missing or noncausal")
            prior_state = decode_continuity_checkpoint(prior)
            if (prior_state.instrument != state.instrument or prior_state.source_id != state.source_id
                    or prior_state.channel != state.channel):
                raise ValueError("continuity parent feed mismatch")
        transport_ref = body["transport_batch_ref"]
        if transport_ref is not None:
            transport = repository.get_artifact(transport_ref)
            if (transport is None or transport.artifact_type not in ("PublicStreamTransportBatchV1", "PublicStreamTransportBatchV2")
                    or transport.content_hash != transport_ref
                    or sha256_json(json_value(transport.metadata["batch"])) != transport_ref
                    or transport.available_at_ns > entry.available_at_ns):
                raise ValueError("continuity transport missing or noncausal")
    return state


def persist_book_checkpoint(repository: OpsRepository, book: SequenceValidBookV2,
                            *, epoch_id: str, metadata_ref: str, as_of_ns: int,
                            prior_ref: str | None, archive_refs: tuple[str, ...],
                            transport_refs: tuple[str, ...], frame_health_refs: tuple[str, ...],
                            control_refs: tuple[str, ...] = (), clock_ns: Callable[[], int] | None = None) -> str:
    if len(control_refs) > 256:
        raise ValueError("book control publication backlog exceeded its bound")
    if len(archive_refs) > MAX_CHUNKS_PER_CHECKPOINT or len(transport_refs) > MAX_CHUNKS_PER_CHECKPOINT:
        raise ValueError("book lineage publication backlog exceeded its bound")
    started = max(as_of_ns, clock_ns() if clock_ns else as_of_ns)
    feature = book.feature(cutoff_ns=as_of_ns)
    state_hash = sha256_json(book.evidence_state())
    finished = max(started, clock_ns() if clock_ns else started)
    body = {"version": "PUBLIC_BOOK_LINEAGE_CHECKPOINT_V1", "authority": "ZERO",
            "instrument": book.instrument.to_dict(), "source_id": book.source_id,
            "channel": book.channel, "metadata_ref": metadata_ref, "epoch_id": epoch_id,
            "as_of_ns": as_of_ns, "prior_checkpoint_ref": prior_ref,
            "archive_checkpoint_refs": list(archive_refs),
            "transport_batch_refs": list(transport_refs),
            "frame_health_refs": list(frame_health_refs), "control_event_refs": list(control_refs),
            "book_state_hash": state_hash, "computation_started_ns": started,
            "computation_finished_ns": finished, "available_at_ns": finished,
            "sequence_state": feature.sequence_state.value, "book_epoch": book.epoch,
            "last_update_id": book.last_update_id, "valid_since_ns": book.valid_since_ns,
            "bbo": list(feature.bbo) if feature.bbo else None,
            "received_at_ns": as_of_ns - feature.data_age_ns if feature.data_age_ns is not None else None}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(ref, BOOK_CHECKPOINT_TYPE, ref,
                                started, finished, {"checkpoint": body}))
    return ref


def validate_book_checkpoint(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                             *, as_of_ns: int) -> Mapping[str, Any]:
    body = json_value(entry.metadata["checkpoint"])
    expected_fields = {"version", "authority", "instrument", "source_id", "channel", "metadata_ref", "epoch_id",
        "as_of_ns", "prior_checkpoint_ref", "archive_checkpoint_refs", "transport_batch_refs", "book_state_hash",
        "sequence_state", "book_epoch", "last_update_id", "valid_since_ns", "bbo", "received_at_ns", "frame_health_refs", "control_event_refs",
        "computation_started_ns", "computation_finished_ns", "available_at_ns"}
    for name in ("as_of_ns", "computation_started_ns", "computation_finished_ns", "available_at_ns"):
        timestamp(body[name], field=name)
    if (set(body) != expected_fields or entry.artifact_type != BOOK_CHECKPOINT_TYPE or entry.artifact_ref != entry.content_hash
            or sha256_json(body) != entry.content_hash or body.get("version") != "PUBLIC_BOOK_LINEAGE_CHECKPOINT_V1"
            or body.get("authority") != "ZERO" or body["available_at_ns"] != entry.available_at_ns
            or not body["as_of_ns"] <= body["computation_started_ns"] <= body["computation_finished_ns"] <= entry.available_at_ns
            or entry.available_at_ns > as_of_ns or entry.created_at_ns != body["computation_started_ns"]):
        raise ValueError("book checkpoint identity or chronology mismatch")
    timestamp(body["as_of_ns"], field="book as-of")
    for name in ("received_at_ns", "valid_since_ns"):
        if body[name] is not None:
            timestamp(body[name], field=name)
            if body[name] > body["as_of_ns"]:
                raise ValueError("book checkpoint contains future source state")
    if type(body["book_epoch"]) is not int or body["book_epoch"] < 0:
        raise ValueError("book epoch invalid")
    refs = body["archive_checkpoint_refs"]
    if not isinstance(refs, list) or len(refs) > MAX_CHUNKS_PER_CHECKPOINT or len(set(refs)) != len(refs):
        raise ValueError("book checkpoint archive population invalid")
    transports = body["transport_batch_refs"]
    if not isinstance(transports, list) or len(transports) > MAX_CHUNKS_PER_CHECKPOINT or len(set(transports)) != len(transports):
        raise ValueError("book checkpoint transport population invalid")
    last_transport_at = 0
    for ref in transports:
        transport = repository.get_artifact(sha256_ref(ref, field="transport_batch_ref"))
        if (transport is None or transport.artifact_type not in ("PublicStreamTransportBatchV1", "PublicStreamTransportBatchV2")
                or transport.content_hash != ref or sha256_json(json_value(transport.metadata["batch"])) != ref
                or transport.available_at_ns > entry.available_at_ns or transport.available_at_ns < last_transport_at):
            raise ValueError("book lineage transport missing, reordered or noncausal")
        last_transport_at = transport.available_at_ns
    health_refs = body["frame_health_refs"]
    if not isinstance(health_refs, list) or len(health_refs) > 128 or len(set(health_refs)) != len(health_refs):
        raise ValueError("book checkpoint frame health population invalid")
    for ref in health_refs:
        health_entry = repository.get_artifact(sha256_ref(ref, field="frame_health_ref"))
        if (health_entry is None or health_entry.artifact_type != "PublicStreamSourceHealthV1"
                or health_entry.content_hash != ref or health_entry.available_at_ns > entry.available_at_ns):
            raise ValueError("book checkpoint frame health missing or noncausal")
        from .health import PublicSourceHealthV2
        from ..instruments import InstrumentKeyV2

        health = PublicSourceHealthV2.from_dict(json_value(health_entry.metadata["health"]))
        transport = json_value(health_entry.metadata["transport"])
        if (health.content_hash != ref or sha256_json(transport) != health.transition_id
                or health.available_at_ns != health_entry.available_at_ns
                or transport.get("transport_batch_ref") not in transports
                or transport.get("instrument_hash") != InstrumentKeyV2.from_dict(body["instrument"]).content_hash
                or any(transport.get(k) != body[k] for k in ("channel", "source_id"))):
            raise ValueError("book checkpoint frame health identity mismatch")
    controls = body["control_event_refs"]
    if not isinstance(controls, list) or len(controls) > 256:
        raise ValueError("book control population invalid")
    for ref in controls:
        control = repository.get_artifact(sha256_ref(ref, field="control_event_ref"))
        if (control is None or control.artifact_type != "PublicStreamContinuityEventV1"
                or control.available_at_ns > entry.available_at_ns
                or sha256_json({"artifact_type": control.artifact_type,
                                "body": json_value(control.metadata)}) != control.content_hash
                or control.artifact_ref != control.content_hash):
            raise ValueError("book control evidence missing or invalid")
        observation = json_value(control.metadata["observation"])
        if any(observation[k] != body[k] for k in ("instrument", "source_id", "channel")):
            raise ValueError("book control feed identity mismatch")
    sha256_ref(body["book_state_hash"], field="book_state_hash")
    parent = body["prior_checkpoint_ref"]
    if parent is not None:
        old = repository.get_artifact(sha256_ref(parent, field="parent"))
        if old is None or old.artifact_type != BOOK_CHECKPOINT_TYPE or old.content_hash != parent or old.available_at_ns > entry.available_at_ns:
            raise ValueError("book lineage parent missing or noncausal")
        prior = json_value(old.metadata["checkpoint"])
        if sha256_json(prior) != parent or any(prior[k] != body[k] for k in ("instrument", "source_id", "channel")):
            raise ValueError("book lineage parent identity mismatch")
    for ref in refs:
        chunk = repository.get_artifact(sha256_ref(ref, field="archive_checkpoint_ref"))
        if chunk is None or chunk.artifact_type not in ("L2FrameArchiveCheckpointV2", "L2FrameArchiveCheckpointV3") or chunk.available_at_ns > entry.available_at_ns:
            raise ValueError("book lineage archive missing or noncausal")
        md = chunk.metadata
        if (any(json_value(md[k]) != body[k] for k in ("instrument", "source_id", "channel"))
                or chunk.content_hash != md["chunk_id"]
                or ref != sha256_json({"artifact_type": chunk.artifact_type, "chunk_id": md["chunk_id"]})):
            raise ValueError("book lineage archive identity mismatch")
    return body


def resolve_report_transport(repository: OpsRepository, metadata: Mapping[str, Any],
                             *, available_at_ns: int) -> Mapping[str, Any]:
    """Resolve the exact immutable health record instead of a repeated body."""
    from .health import PublicSourceHealthV2

    body = json_value(metadata)
    if body.get("storage_version") != "PUBLIC_CONTINUITY_REPORT_INDEX_V2" or set(body) != {
            "storage_version", "report", "state_ref", "source_health_ref", "transport_ref",
            "computation_started_ns", "computation_finished_ns"}:
        raise ValueError("compact report storage version or fields invalid")
    report = body["report"]
    for name in ("computation_started_ns", "computation_finished_ns"):
        timestamp(body[name], field=name)
    if not (report["as_of_ns"] <= body["computation_started_ns"] <= body["computation_finished_ns"] <= available_at_ns):
        raise ValueError("compact report derived chronology mismatch")
    ref = sha256_ref(body["transport_ref"], field="report transport reference")
    if ref != body["source_health_ref"] or ref != report["source_health_ref"]:
        raise ValueError("compact report transport binding mismatch")
    entry = repository.get_artifact(ref)
    if entry is None or entry.artifact_type != "PublicStreamSourceHealthV1":
        raise ValueError("compact report health evidence missing")
    health = PublicSourceHealthV2.from_dict(json_value(entry.metadata["health"]))
    transport = json_value(entry.metadata["transport"])
    if (health.content_hash != ref or entry.content_hash != ref
            or health.available_at_ns != report["as_of_ns"] or entry.available_at_ns != report["as_of_ns"]
            or sha256_json(transport) != health.transition_id
            or any(transport[k] != report[k] for k in ("instrument", "channel", "metadata_ref", "epoch_id", "source_id"))
            or (report["source_current"] and not health.data_eligible)):
        raise ValueError("compact report health identity or chronology mismatch")
    return transport
