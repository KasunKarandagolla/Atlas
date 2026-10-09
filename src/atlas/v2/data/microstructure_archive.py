"""Batched immutable Parquet archive for high-frequency raw S4 frames.

Only one compact checkpoint per chunk is sent to the existing atlas-ops writer;
individual frames never create capital-control SQLite rows.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, sha256_json, sha256_ref, timestamp
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository


@dataclass(frozen=True)
class L2RawFrameV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    frame_type: str
    raw_payload_bytes: bytes
    raw_payload_hash: str
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    first_update_id: int | None
    last_update_id: int | None
    previous_update_id: int | None
    sequence_semantics: str
    source_health: str
    availability_class: str
    source_health_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("raw L2 frame requires full InstrumentKeyV2")
        for name in ("source_id", "channel", "frame_type", "sequence_semantics", "source_health", "availability_class"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be non-empty")
        if not isinstance(self.raw_payload_bytes, bytes):
            raise ValueError("raw payload must be exact bytes")
        sha256_ref(self.raw_payload_hash, field="raw_payload_hash")
        if hashlib.sha256(self.raw_payload_bytes).hexdigest() != self.raw_payload_hash:
            raise ValueError("raw frame byte hash mismatch")
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            val = getattr(self, name)
            if val is not None:
                timestamp(val, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("processed availability cannot precede receipt")
        for name in ("first_update_id", "last_update_id", "previous_update_id"):
            val = getattr(self, name)
            if val is not None and (type(val) is not int or val < 0):
                raise ValueError(f"{name} must be nonnegative or null")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")

    @cached_property
    def record_id(self) -> str:
        ident = {"instrument": self.instrument.to_dict(), "source_id": self.source_id,
                 "channel": self.channel, "frame_type": self.frame_type,
                 "sequence": self.last_update_id,
                 "event_at_ns": self.event_at_ns if self.last_update_id is None else None}
        return sha256_json(ident)

    def metadata_dict(self) -> dict[str, Any]:
        return {"record_id": self.record_id, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "channel": self.channel, "frame_type": self.frame_type,
                "raw_payload_hash": self.raw_payload_hash, "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "first_update_id": self.first_update_id, "last_update_id": self.last_update_id,
                "previous_update_id": self.previous_update_id,
                "sequence_semantics": self.sequence_semantics, "source_health": self.source_health,
                "availability_class": self.availability_class,
                "source_health_ref": self.source_health_ref}


@dataclass(frozen=True)
class L2RestartCursorV2:
    instrument_hash: str
    source_id: str
    channel: str
    high_water_update_id: int | None
    last_frame_ref: str
    state_after_restart: str = "SNAPSHOT_RECOVERY"
    source_health_after_restart: str = "INCOMPLETE_SNAPSHOT"


class L2FrameArchiveV2:
    def __init__(self, root: str | Path, repository: OpsRepository, *, compact_live: bool = False,
                 clock_ns: Callable[[], int] | None = None) -> None:
        self.root = Path(root)
        self.repository = repository
        self.compact_live = compact_live
        self.clock_ns = clock_ns

    def checkpoint_ref(self, chunk_id: str) -> str:
        kind = "L2FrameArchiveCheckpointV3" if self.compact_live else "L2FrameArchiveCheckpointV2"
        return sha256_json({"artifact_type": kind, "chunk_id": chunk_id})

    def write_chunk(self, frames: tuple[L2RawFrameV2, ...]) -> tuple[str, Path]:
        return self.write_chunks((frames,))[0]

    def write_chunks(
        self,
        frame_groups: tuple[tuple[L2RawFrameV2, ...], ...],
    ) -> tuple[tuple[str, Path], ...]:
        import pyarrow as pa

        if not frame_groups:
            return ()
        if len(frame_groups) > 256:
            raise ValueError("archive chunk batch item bound exceeded")
        prepared: list[tuple[str, tuple[L2RawFrameV2, ...], Any, Path]] = []
        for frames in frame_groups:
            if not frames or any(not isinstance(frame, L2RawFrameV2) for frame in frames):
                raise ValueError("archive chunk requires nonempty typed raw frames")
            ordered = tuple(sorted(frames, key=lambda f: (f.available_at_ns, f.source_id, f.channel,
                                                           f.last_update_id if f.last_update_id is not None else -1,
                                                           f.raw_payload_hash)))
            keys = {(f.instrument.content_hash, f.source_id, f.channel) for f in ordered}
            if len(keys) != 1:
                raise ValueError("one Parquet chunk must contain one instrument/source/channel stream")
            identities: dict[str, str] = {}
            for frame in ordered:
                old = identities.get(frame.record_id)
                if old is not None and old != frame.raw_payload_hash:
                    raise ValueError("conflicting duplicate raw frame identity")
                identities[frame.record_id] = frame.raw_payload_hash
            chunk_id = sha256_json({"archive_type": "L2RawFrameChunkV2",
                                    "frames": [f.metadata_dict() for f in ordered]})
            rows = [{**frame.metadata_dict(), "instrument_json": canonical_json(frame.instrument.to_dict()),
                     "raw_payload_bytes": frame.raw_payload_bytes} for frame in ordered]
            candidate = pa.Table.from_pylist(rows)
            prepared.append((chunk_id, ordered, candidate, self.root / f"{chunk_id}.parquet"))
        prepared.sort(key=lambda item: item[0])
        if self.compact_live:
            from .public_archive_extents import write_extents

            specs = tuple((candidate, "ops-l2-frames", chunk_id,
                           self.clock_ns or (lambda available_at_ns=ordered[-1].available_at_ns: available_at_ns),
                           ordered[-1].available_at_ns)
                          for chunk_id, ordered, candidate, _path in prepared)
            extent_results = write_extents(self.repository, specs)
            extent_metadata = self.repository.get_artifact_metadata_by_refs(
                tuple(extent for extent, _path in extent_results))
            checkpoints = []
            outputs = []
            for (chunk_id, ordered, _candidate, _path), (extent, extent_path) in zip(
                    prepared, extent_results, strict=True):
                last = ordered[-1]
                descriptor = extent_metadata.get(extent)
                if descriptor is None:
                    raise RuntimeError("durable public archive extent descriptor missing after batch index")
                checkpoint_ref = self.checkpoint_ref(chunk_id)
                checkpoints.append(ArtifactIndexEntryV2(
                    checkpoint_ref, "L2FrameArchiveCheckpointV3", chunk_id,
                    int(descriptor["available_at_ns"]), int(descriptor["available_at_ns"]),
                    {"schema_version": 3, "chunk_id": chunk_id, "archive_extent_ref": extent,
                     "instrument": last.instrument.to_dict(), "instrument_hash": last.instrument.content_hash,
                     "source_id": last.source_id, "channel": last.channel,
                     "high_water_update_id": last.last_update_id, "last_record_id": last.record_id,
                     "last_payload_hash": last.raw_payload_hash, "sequence_semantics": last.sequence_semantics,
                     "state_after_restart": "SNAPSHOT_RECOVERY", "source_health_after_restart": "INCOMPLETE_SNAPSHOT",
                     "frame_count": len(ordered)}))
                outputs.append((chunk_id, extent_path))
            self.repository.register_artifacts(tuple(checkpoints))
            return tuple(outputs)
        self.root.mkdir(parents=True, exist_ok=True)
        import pyarrow.parquet as pq

        checkpoints = []
        outputs = []
        for chunk_id, ordered, candidate, path in prepared:
            temporary = path.with_suffix(".parquet.tmp")
            pq.write_table(candidate, temporary, compression="zstd")
            if path.exists():
                prior = pq.read_table(path)
                if prior.to_pylist() != pq.read_table(temporary).to_pylist():
                    temporary.unlink(missing_ok=True)
                    raise ValueError("immutable L2 chunk identity conflicts with archived bytes")
                temporary.unlink(missing_ok=True)
            else:
                temporary.replace(path)
            last = ordered[-1]
            checkpoint_ref = sha256_json({"artifact_type": "L2FrameArchiveCheckpointV2", "chunk_id": chunk_id})
            checkpoints.append(ArtifactIndexEntryV2(
                checkpoint_ref, "L2FrameArchiveCheckpointV2", chunk_id,
                last.available_at_ns, last.available_at_ns,
                {"schema_version": 1, "chunk_id": chunk_id, "archive_path_name": path.name,
                 "instrument": last.instrument.to_dict(), "instrument_hash": last.instrument.content_hash,
                 "source_id": last.source_id, "channel": last.channel,
                 "high_water_update_id": last.last_update_id, "last_record_id": last.record_id,
                 "last_payload_hash": last.raw_payload_hash, "sequence_semantics": last.sequence_semantics,
                 "state_after_restart": "SNAPSHOT_RECOVERY", "source_health_after_restart": "INCOMPLETE_SNAPSHOT",
                 "frame_count": len(ordered), "frame_refs": [f.record_id for f in ordered]},
            ))
            outputs.append((chunk_id, path))
        self.repository.register_artifacts(tuple(checkpoints))
        return tuple(outputs)

    def write_conflict_quarantine(self, existing: L2RawFrameV2, incoming: L2RawFrameV2) -> tuple[str, Path]:
        """Retain both exact payloads for one conflicting sequence identity."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        if existing.record_id != incoming.record_id or existing.raw_payload_hash == incoming.raw_payload_hash:
            raise ValueError("quarantine requires same raw-frame identity with different bytes")
        chunk_id = sha256_json({"artifact_type": "L2ConflictQuarantineV2", "record_id": existing.record_id,
                                "existing_hash": existing.raw_payload_hash, "incoming_hash": incoming.raw_payload_hash})
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"{chunk_id}.parquet"
        rows = []
        for side, frame in (("EXISTING", existing), ("INCOMING", incoming)):
            rows.append({**frame.metadata_dict(), "instrument_json": canonical_json(frame.instrument.to_dict()),
                         "raw_payload_bytes": frame.raw_payload_bytes, "quarantine_side": side,
                         "conflict_record_id": existing.record_id})
        temporary = path.with_suffix(".parquet.tmp")
        candidate = pa.Table.from_pylist(rows)
        pq.write_table(candidate, temporary, compression="zstd")
        if path.exists():
            old = pq.read_table(path).to_pylist()
            new = pq.read_table(temporary).to_pylist()
            if old != new:
                temporary.unlink(missing_ok=True)
                raise ValueError("conflict quarantine identity contains different payloads")
            temporary.unlink(missing_ok=True)
        else:
            temporary.replace(path)
        available = max(existing.available_at_ns, incoming.available_at_ns)
        ref = sha256_json({"artifact_type": "L2ConflictQuarantineV2", "chunk_id": chunk_id})
        self.repository.register_artifact(ArtifactIndexEntryV2(
            ref, "L2ConflictQuarantineV2", chunk_id, available, available,
            {"schema_version": 1, "chunk_id": chunk_id, "archive_path_name": path.name,
             "record_id": existing.record_id, "existing_hash": existing.raw_payload_hash,
             "incoming_hash": incoming.raw_payload_hash, "state": "GAP_DETECTED"},
        ))
        return chunk_id, path

    def restart_cursors(self) -> tuple[L2RestartCursorV2, ...]:
        latest: dict[tuple[str, str, str], Any] = {}
        for entry in self.repository.artifact_entries("L2FrameArchiveCheckpointV2"):
            md = entry.metadata
            key = (str(md["instrument_hash"]), str(md["source_id"]), str(md["channel"]))
            if key not in latest or (entry.available_at_ns, entry.artifact_ref) > (
                latest[key].available_at_ns, latest[key].artifact_ref
            ):
                latest[key] = entry
        return tuple(L2RestartCursorV2(
            key[0], key[1], key[2],
            md.get("high_water_update_id") if isinstance(md.get("high_water_update_id"), int) else None,
            str(md["last_payload_hash"]), str(md["state_after_restart"]),
            str(md["source_health_after_restart"]),
        ) for key, entry in sorted(latest.items()) for md in (entry.metadata,))

    def read_chunk(self, chunk_id: str) -> tuple[dict[str, Any], ...]:
        from .public_archive_extents import read_public_chunk

        sha256_ref(chunk_id, field="chunk_id")
        table = read_public_chunk(self.repository, self.root, chunk_id)
        rows = table.to_pylist()
        for row in rows:
            raw = row["raw_payload_bytes"]
            if hashlib.sha256(raw).hexdigest() != row["raw_payload_hash"]:
                raise RuntimeError("archived raw frame byte hash mismatch")
        return tuple(rows)
