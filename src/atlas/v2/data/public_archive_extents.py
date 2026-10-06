"""Immutable, hash-verified Arrow IPC extents in bounded public archive segments.

Each indexed extent is a complete compressed Arrow stream. Appending another
extent never changes its bytes. The single ops writer fsyncs before indexing;
restart starts a new segment and never repairs an orphan tail into continuity.
Legacy closed Parquet chunks remain readable and are never rewritten.
"""
from __future__ import annotations

import hashlib
import os
import re
import time
import uuid
import zlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .._serialization import json_value, sha256_json, sha256_ref
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository

EXTENT_TYPE = "PublicArchiveExtentV1"
MAX_EXTENT_ROWS = 512
MAX_EXTENT_BYTES = 16 * 1024 * 1024
MAX_SEGMENT_BYTES = 32 * 1024 * 1024
MAX_DECODED_EXTENT_BYTES = 32 * 1024 * 1024
_NAMESPACES = {"ops-public-transport", "ops-l2-frames", "ops-observations", "ops-stream-metadata"}


def extent_ref(namespace: str, chunk_id: str) -> str:
    if namespace not in _NAMESPACES:
        raise ValueError("unknown public archive namespace")
    sha256_ref(chunk_id, field="logical chunk_id")
    return sha256_json({"version": EXTENT_TYPE, "namespace": namespace, "chunk_id": chunk_id})


class PublicArchiveSegmentWriterV1:
    """Seal independent extents without a database connection or index authority.

    Each instance exclusively owns its UUID segments. The ops controller alone
    publishes descriptors into SQLite after the immutable bytes are durable.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.path: Path | None = None
        self.offset = 0
        self.bytes_written = 0
        self.metrics: dict[str, Any] = {"active_phase": "IDLE"}

    def seal(self, table: Any, *, namespace: str, chunk_id: str,
             clock_ns: Callable[[], int], floor_ns: int) -> ArtifactIndexEntryV2:
        import pyarrow as pa

        ref = extent_ref(namespace, chunk_id)
        if not 1 <= table.num_rows <= MAX_EXTENT_ROWS or table.nbytes > MAX_DECODED_EXTENT_BYTES:
            raise ValueError("public archive extent row bound exceeded")
        started = time.monotonic_ns()
        self.metrics = {"active_phase": "ARROW_ENCODE", "phase_started_monotonic_ns": started}
        output = pa.BufferOutputStream()
        with pa.ipc.new_stream(output, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
            writer.write_table(table)
        ipc_payload = output.getvalue().to_pybytes()
        encoded_at = time.monotonic_ns()
        if len(ipc_payload) > MAX_EXTENT_BYTES:
            raise ValueError("public archive IPC byte bound exceeded")
        # Arrow's buffer compression leaves repeated schemas uncompressed.
        # Compress the complete independent IPC stream as well; no history or
        # dictionary from a previous extent is required to decode it.
        payload = zlib.compress(ipc_payload, level=1)
        compressed_at = time.monotonic_ns()
        if len(payload) > MAX_EXTENT_BYTES:
            raise ValueError("public archive extent byte bound exceeded")
        self.root.mkdir(parents=True, exist_ok=True)
        if self.path is None or self.offset + len(payload) > MAX_SEGMENT_BYTES:
            self.path = self.root / f"public-{uuid.uuid4().hex}.arrow"
            self.offset = 0
            mode = "xb"
        else:
            if self.path.is_symlink() or self.path.stat().st_size != self.offset:
                raise ValueError("public archive segment prefix changed")
            mode = "ab"
        offset = self.offset
        writing_at = time.monotonic_ns()
        self.metrics = {**self.metrics, "active_phase": "ARCHIVE_WRITE", "phase_started_monotonic_ns": writing_at}
        with self.path.open(mode) as handle:
            if handle.write(payload) != len(payload):
                raise OSError("public archive extent write incomplete")
            handle.flush()
            fsync_at = time.monotonic_ns()
            self.metrics = {**self.metrics, "active_phase": "ARCHIVE_FSYNC", "phase_started_monotonic_ns": fsync_at}
            os.fsync(handle.fileno())
        finished = time.monotonic_ns()
        self.bytes_written += len(payload)
        self.metrics = {"active_phase": "IDLE", "arrow_encode_ns": encoded_at - started,
            "compression_ns": compressed_at - encoded_at, "write_ns": fsync_at - writing_at,
            "fsync_ns": finished - fsync_at, "total_ns": finished - started,
            "observed_at_ns": clock_ns(), "bytes_written": self.bytes_written}
        self.offset += len(payload)
        if self.path.stat().st_size != self.offset:
            raise OSError("public archive extent size mismatch")
        from ..chronology import sample

        available = sample(clock_ns, floor_ns=floor_ns)
        body = {"version": EXTENT_TYPE, "namespace": namespace, "chunk_id": chunk_id,
                "segment_name": self.path.name, "offset": offset, "length": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(), "row_count": table.num_rows,
                "decoded_bytes": table.nbytes, "encoding": "ARROW_IPC_ZSTD_ZLIB_V1",
                "ipc_bytes": len(ipc_payload),
                "available_at_ns": available, "authority": "ZERO"}
        return ArtifactIndexEntryV2(ref, EXTENT_TYPE, sha256_json(body),
                                    available, available, {"extent": body})


class PublicArchiveExtentWriterV1(PublicArchiveSegmentWriterV1):
    def __init__(self, repository: OpsRepository) -> None:
        if repository.read_only:
            raise ValueError("read-only repository cannot own an archive writer")
        self.repository = repository
        super().__init__(Path(repository.path).parent / "ops-public-extents")

    def write(self, table: Any, *, namespace: str, chunk_id: str,
              clock_ns: Callable[[], int], floor_ns: int) -> tuple[str, Path]:
        ref = extent_ref(namespace, chunk_id)
        existing = self.repository.get_artifact(ref)
        if existing is not None:
            if not read_extent(self.repository, ref).equals(table):
                raise ValueError("immutable public archive extent conflict")
            return ref, self.root / str(existing.metadata["extent"]["segment_name"])
        entry = self.seal(table, namespace=namespace, chunk_id=chunk_id,
                          clock_ns=clock_ns, floor_ns=floor_ns)
        self.repository.register_artifact(entry)
        return ref, self.root / str(entry.metadata["extent"]["segment_name"])


def write_extent(repository: OpsRepository, table: Any, *, namespace: str, chunk_id: str,
                 clock_ns: Callable[[], int], floor_ns: int) -> tuple[str, Path]:
    writer = repository._public_extent_writer
    if writer is None:
        writer = PublicArchiveExtentWriterV1(repository)
        repository._public_extent_writer = writer
    return writer.write(table, namespace=namespace, chunk_id=chunk_id, clock_ns=clock_ns, floor_ns=floor_ns)


def read_extent(repository: OpsRepository, ref: str) -> Any:
    import pyarrow as pa

    entry = repository.get_artifact(ref)
    if entry is None or entry.artifact_type != EXTENT_TYPE:
        raise ValueError("public archive extent descriptor missing")
    body = json_value(entry.metadata["extent"])
    if (set(body) != {"version", "namespace", "chunk_id", "segment_name", "offset", "length", "sha256",
                      "row_count", "available_at_ns", "authority", "decoded_bytes", "encoding", "ipc_bytes"}
            or body["encoding"] != "ARROW_IPC_ZSTD_ZLIB_V1"
            or type(body["ipc_bytes"]) is not int or not 1 <= body["ipc_bytes"] <= MAX_EXTENT_BYTES
            or body["version"] != EXTENT_TYPE or body["authority"] != "ZERO"
            or extent_ref(body["namespace"], body["chunk_id"]) != ref
            or sha256_json(body) != entry.content_hash or body["available_at_ns"] != entry.available_at_ns
            or entry.created_at_ns != entry.available_at_ns
            or not re.fullmatch(r"public-[a-f0-9]{32}\.arrow", body["segment_name"])
            or type(body["offset"]) is not int or body["offset"] < 0
            or type(body["length"]) is not int or not 1 <= body["length"] <= MAX_EXTENT_BYTES
            or type(body["row_count"]) is not int or not 1 <= body["row_count"] <= MAX_EXTENT_ROWS
            or type(body["decoded_bytes"]) is not int or not 1 <= body["decoded_bytes"] <= MAX_DECODED_EXTENT_BYTES
            or body["offset"] + body["length"] > MAX_SEGMENT_BYTES):
        raise ValueError("public archive extent identity or bounds mismatch")
    sha256_ref(body["sha256"], field="extent sha256")
    path = Path(repository.path).parent / "ops-public-extents" / body["segment_name"]
    if path.is_symlink():
        raise ValueError("public archive segment cannot be a symlink")
    with path.open("rb") as handle:
        handle.seek(body["offset"])
        payload = handle.read(body["length"])
    if len(payload) != body["length"] or hashlib.sha256(payload).hexdigest() != body["sha256"]:
        raise ValueError("public archive extent bytes missing or changed")
    decoder = zlib.decompressobj()
    decoded = decoder.decompress(payload, body["ipc_bytes"] + 1)
    if (len(decoded) != body["ipc_bytes"] or not decoder.eof
            or decoder.unconsumed_tail or decoder.unused_data):
        raise ValueError("public archive IPC encoding or byte bound mismatch")
    table = pa.ipc.open_stream(pa.BufferReader(decoded)).read_all()
    if table.num_rows != body["row_count"] or table.nbytes != body["decoded_bytes"]:
        raise ValueError("public archive extent row count mismatch")
    return table


def read_public_chunk(repository: OpsRepository, root: Path, chunk_id: str) -> Any:
    import pyarrow.parquet as pq

    if root.name in _NAMESPACES:
        ref = extent_ref(root.name, chunk_id)
        if repository.get_artifact(ref) is not None:
            return read_extent(repository, ref)
    path = root / f"{chunk_id}.parquet"
    if path.is_symlink():
        raise ValueError("legacy public archive cannot be a symlink")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows > MAX_EXTENT_ROWS:
        raise ValueError("public archive chunk row bound exceeded")
    return parquet.read()
