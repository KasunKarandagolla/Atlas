"""Exact bounded transport batches, including frames without instrument metadata."""
from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

from .._serialization import sha256_json
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from .public_microstructure_ws import CapturedPublicFrameV2


def archive_transport_batch(repository: OpsRepository, frames: tuple[CapturedPublicFrameV2, ...],
                            *, clock_ns: Callable[[], int], floor_ns: int) -> str | None:
    if not frames:
        return None
    if len(frames) > 256:
        raise ValueError("transport archive batch exceeds the writer service bound")
    import pyarrow as pa
    import pyarrow.parquet as pq

    headers = [{"venue": f.venue.value, "source_id": f.source_id, "channel": f.channel,
                "raw_payload_hash": f.raw_payload_hash, "received_at_ns": f.received_at_ns,
                "available_at_ns": f.available_at_ns, "connection_epoch": f.connection_epoch,
                "fifo_index": index} for index, f in enumerate(frames)]
    chunk_id = sha256_json({"version": "PublicStreamTransportBatchV1", "frames": headers})
    root = Path(repository.path).parent / "ops-public-transport"
    root.mkdir(parents=True, exist_ok=True)
    path = root / (chunk_id + ".parquet")
    rows = [{**header, "raw_payload_bytes": frame.raw_payload_bytes}
            for header, frame in zip(headers, frames, strict=True)]
    if path.exists():
        if pq.read_table(path).to_pylist() != rows:
            raise ValueError("immutable public transport archive conflict")
    else:
        temporary = path.with_suffix(".parquet.tmp")
        try:
            pq.write_table(pa.Table.from_pylist(rows), temporary, compression="zstd")
            # Windows _commit requires a writable descriptor. r+b preserves
            # the completed Parquet bytes while qualifying the same fsync.
            with temporary.open("r+b") as handle:
                os.fsync(handle.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    available = max(floor_ns, clock_ns(), *(frame.available_at_ns for frame in frames))
    body = {"version": "PublicStreamTransportBatchV1", "chunk_id": chunk_id,
            "archive_path_name": path.name, "frames": headers,
            "available_at_ns": available, "authority": "ZERO"}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "PublicStreamTransportBatchV1", ref, available, available, {"batch": body}))
    return ref
