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
MAX_EXTENT_BATCH_ITEMS = 256
MAX_EXTENT_BATCH_BYTES = 64 * 1024 * 1024
_NAMESPACES = {"ops-public-transport", "ops-l2-frames", "ops-observations", "ops-stream-metadata"}


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _windows_path(path: Path) -> str:
    value = str(path.absolute())
    if value.startswith("\\\\?\\"):
        return value
    if value.startswith("\\\\"):
        return "\\\\?\\UNC\\" + value[2:]
    return "\\\\?\\" + value


def _move_path_write_through(source: Path, target: Path) -> None:
    """Publish a newly created Windows path using a write-through rename."""
    import ctypes
    from ctypes import wintypes

    move_file = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW  # type: ignore[attr-defined]
    move_file.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    move_file.restype = wintypes.BOOL
    if not move_file(_windows_path(source), _windows_path(target), 0x00000008):
        error = ctypes.get_last_error()  # type: ignore[attr-defined]
        raise OSError(error, "MoveFileExW write-through publication failed", str(target))


def _ensure_directory_durable(path: Path) -> None:
    if path.exists():
        if path.is_symlink() or not path.is_dir():
            raise ValueError("public archive directory must be a real directory")
        return
    parent = path.parent
    if parent != path:
        _ensure_directory_durable(parent)
    if os.name == "nt":
        staged = parent / f".atlas-dir-{uuid.uuid4().hex}.tmp"
        staged.mkdir()
        _move_path_write_through(staged, path)
    else:
        path.mkdir()
        _fsync_directory(parent)


def _publish_new_segment(staged: Path, final: Path) -> int:
    """Make a file name durable before any SQLite row may refer to it."""
    if final.exists() or final.is_symlink():
        raise FileExistsError(final)
    if os.name == "nt":
        _move_path_write_through(staged, final)
        return 0
    os.rename(staged, final)
    started = time.monotonic_ns()
    _fsync_directory(final.parent)
    return time.monotonic_ns() - started


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
        _ensure_directory_durable(self.root)
        prior_path = self.path
        new_segment = prior_path is None or self.offset + len(payload) > MAX_SEGMENT_BYTES
        if new_segment:
            segment_path = self.root / f"public-{uuid.uuid4().hex}.arrow"
            write_path = self.root / f".{segment_path.name}.{uuid.uuid4().hex}.tmp"
            offset = 0
            mode = "xb"
        else:
            assert prior_path is not None
            if prior_path.is_symlink() or prior_path.stat().st_size != self.offset:
                raise ValueError("public archive segment prefix changed")
            segment_path = prior_path
            write_path = segment_path
            offset = self.offset
            mode = "ab"
        writing_at = time.monotonic_ns()
        self.metrics = {**self.metrics, "active_phase": "ARCHIVE_WRITE", "phase_started_monotonic_ns": writing_at}
        with write_path.open(mode) as handle:
            if handle.write(payload) != len(payload):
                raise OSError("public archive extent write incomplete")
            handle.flush()
            fsync_at = time.monotonic_ns()
            self.metrics = {**self.metrics, "active_phase": "ARCHIVE_FSYNC", "phase_started_monotonic_ns": fsync_at}
            os.fsync(handle.fileno())
        file_sync_finished = time.monotonic_ns()
        directory_sync_ns = 0
        if new_segment:
            self.metrics = {**self.metrics, "active_phase": "ARCHIVE_PUBLISH",
                            "phase_started_monotonic_ns": time.monotonic_ns()}
            directory_sync_ns = _publish_new_segment(write_path, segment_path)
        finished = time.monotonic_ns()
        new_offset = offset + len(payload)
        if segment_path.stat().st_size != new_offset:
            raise OSError("public archive extent size mismatch")
        self.bytes_written += len(payload)
        self.offset = new_offset
        self.path = segment_path
        self.metrics = {"active_phase": "IDLE", "arrow_encode_ns": encoded_at - started,
            "compression_ns": compressed_at - encoded_at, "write_ns": fsync_at - writing_at,
            "fsync_ns": file_sync_finished - fsync_at, "directory_sync_ns": directory_sync_ns,
            "total_ns": finished - started,
            "observed_at_ns": clock_ns(), "bytes_written": self.bytes_written}
        from ..chronology import sample

        available = sample(clock_ns, floor_ns=floor_ns)
        body = {"version": EXTENT_TYPE, "namespace": namespace, "chunk_id": chunk_id,
                "segment_name": segment_path.name, "offset": offset, "length": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(), "row_count": table.num_rows,
                "decoded_bytes": table.nbytes, "encoding": "ARROW_IPC_ZSTD_ZLIB_V1",
                "ipc_bytes": len(ipc_payload),
                "available_at_ns": available, "authority": "ZERO"}
        return ArtifactIndexEntryV2(ref, EXTENT_TYPE, sha256_json(body),
                                    available, available, {"extent": body})

    def seal_many(
        self,
        chunks: tuple[tuple[Any, str, str, Callable[[], int], int], ...],
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Seal independent Arrow extents with one fsync per bounded segment."""
        if not chunks:
            return ()
        if len(chunks) > MAX_EXTENT_BATCH_ITEMS:
            raise ValueError("public archive extent batch item bound exceeded")
        import pyarrow as pa

        started = time.monotonic_ns()
        self.metrics = {"active_phase": "ARROW_ENCODE", "phase_started_monotonic_ns": started,
                        "extent_count": len(chunks)}
        encoded: list[tuple[Any, str, str, Callable[[], int], int, str, bytes, int]] = []
        refs: set[str] = set()
        arrow_encode_ns = compression_ns = write_ns = fsync_ns = directory_sync_ns = 0
        encoded_bytes = 0
        for table, namespace, chunk_id, clock_ns, floor_ns in chunks:
            ref = extent_ref(namespace, chunk_id)
            if ref in refs:
                raise ValueError("archive extent batch repeats an immutable identity")
            refs.add(ref)
            if not 1 <= table.num_rows <= MAX_EXTENT_ROWS or table.nbytes > MAX_DECODED_EXTENT_BYTES:
                raise ValueError("public archive extent row bound exceeded")
            encode_started = time.monotonic_ns()
            output = pa.BufferOutputStream()
            with pa.ipc.new_stream(output, table.schema,
                                   options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
                writer.write_table(table)
            ipc_payload = output.getvalue().to_pybytes()
            encoded_at = time.monotonic_ns()
            arrow_encode_ns += encoded_at - encode_started
            if len(ipc_payload) > MAX_EXTENT_BYTES:
                raise ValueError("public archive IPC byte bound exceeded")
            # Each IPC stream is compressed independently and remains readable
            # without any neighboring extent or dictionary state.
            payload = zlib.compress(ipc_payload, level=1)
            compressed_at = time.monotonic_ns()
            compression_ns += compressed_at - encoded_at
            if len(payload) > MAX_EXTENT_BYTES:
                raise ValueError("public archive extent byte bound exceeded")
            encoded_bytes += len(payload)
            if encoded_bytes > MAX_EXTENT_BATCH_BYTES:
                raise ValueError("public archive extent batch byte bound exceeded")
            encoded.append((table, namespace, chunk_id, clock_ns, floor_ns, ref, payload, len(ipc_payload)))

        _ensure_directory_durable(self.root)
        if self.path is not None and (self.path.is_symlink() or self.path.stat().st_size != self.offset):
            raise ValueError("public archive segment prefix changed")
        from ..chronology import sample

        entries: list[ArtifactIndexEntryV2] = []
        segment_path = self.path
        segment_offset = self.offset
        pending: list[tuple[Any, str, str, Callable[[], int], int, str, bytes, int]] = []
        pending_bytes = 0

        def flush_segment() -> None:
            nonlocal segment_path, segment_offset, pending, pending_bytes, write_ns, fsync_ns, directory_sync_ns
            if not pending:
                return
            path = segment_path or self.root / f"public-{uuid.uuid4().hex}.arrow"
            write_path = path if segment_path is not None else self.root / f".{path.name}.{uuid.uuid4().hex}.tmp"
            base_offset = segment_offset if segment_path is not None else 0
            mode = "ab" if segment_path is not None else "xb"
            payload = b"".join(item[6] for item in pending)
            write_started = time.monotonic_ns()
            self.metrics = {**self.metrics, "active_phase": "ARCHIVE_WRITE",
                            "phase_started_monotonic_ns": write_started}
            with write_path.open(mode) as handle:
                if handle.write(payload) != len(payload):
                    raise OSError("public archive extent write incomplete")
                handle.flush()
                fsync_started = time.monotonic_ns()
                self.metrics = {**self.metrics, "active_phase": "ARCHIVE_FSYNC",
                                "phase_started_monotonic_ns": fsync_started}
                os.fsync(handle.fileno())
            file_sync_finished = time.monotonic_ns()
            if segment_path is None:
                self.metrics = {**self.metrics, "active_phase": "ARCHIVE_PUBLISH",
                                "phase_started_monotonic_ns": time.monotonic_ns()}
                directory_sync_ns += _publish_new_segment(write_path, path)
            write_ns += fsync_started - write_started
            fsync_ns += file_sync_finished - fsync_started
            segment_offset = base_offset + len(payload)
            if path.stat().st_size != segment_offset:
                raise OSError("public archive extent size mismatch")
            # The append is durable now. Advance the writer before clock
            # sampling or descriptor construction, which may fail afterward.
            self.path = path
            self.offset = segment_offset
            self.bytes_written += len(payload)
            segment_path = path
            offset = base_offset
            for table, namespace, chunk_id, clock_ns, floor_ns, ref, raw, ipc_bytes in pending:
                available = sample(clock_ns, floor_ns=floor_ns)
                body = {"version": EXTENT_TYPE, "namespace": namespace, "chunk_id": chunk_id,
                        "segment_name": path.name, "offset": offset, "length": len(raw),
                        "sha256": hashlib.sha256(raw).hexdigest(), "row_count": table.num_rows,
                        "decoded_bytes": table.nbytes, "encoding": "ARROW_IPC_ZSTD_ZLIB_V1",
                        "ipc_bytes": ipc_bytes, "available_at_ns": available, "authority": "ZERO"}
                entries.append(ArtifactIndexEntryV2(ref, EXTENT_TYPE, sha256_json(body),
                                                    available, available, {"extent": body}))
                offset += len(raw)
            pending = []
            pending_bytes = 0

        for item in encoded:
            size = len(item[6])
            if size > MAX_SEGMENT_BYTES:
                raise ValueError("public archive extent exceeds segment byte bound")
            if pending and segment_offset + pending_bytes + size > MAX_SEGMENT_BYTES:
                flush_segment()
                segment_path = None
                segment_offset = 0
            elif not pending and segment_path is not None and segment_offset + size > MAX_SEGMENT_BYTES:
                segment_path = None
                segment_offset = 0
            pending.append(item)
            pending_bytes += size
        flush_segment()
        self.metrics = {"active_phase": "IDLE", "extent_count": len(encoded),
            "arrow_encode_ns": arrow_encode_ns, "compression_ns": compression_ns,
            "write_ns": write_ns, "fsync_ns": fsync_ns, "directory_sync_ns": directory_sync_ns,
            "total_ns": time.monotonic_ns() - started,
            "observed_at_ns": max(item[3]() for item in encoded), "bytes_written": self.bytes_written}
        return tuple(entries)


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

    def write_many(
        self,
        chunks: tuple[tuple[Any, str, str, Callable[[], int], int], ...],
    ) -> tuple[tuple[str, Path], ...]:
        if not chunks:
            return ()
        if len(chunks) > MAX_EXTENT_BATCH_ITEMS:
            raise ValueError("public archive extent batch item bound exceeded")
        refs = tuple(extent_ref(namespace, chunk_id)
                     for _table, namespace, chunk_id, _clock, _floor in chunks)
        if len(set(refs)) != len(refs):
            raise ValueError("archive extent batch repeats an immutable identity")
        known = self.repository.get_artifact_metadata_by_refs(refs)
        existing: dict[str, ArtifactIndexEntryV2] = {}
        pending: list[tuple[Any, str, str, Callable[[], int], int]] = []
        for item, ref in zip(chunks, refs, strict=True):
            table, _namespace, _chunk_id, _clock, _floor = item
            if ref not in known:
                pending.append(item)
                continue
            prior = self.repository.get_artifact(ref)
            if (prior is None or prior.artifact_type != EXTENT_TYPE
                    or not read_extent(self.repository, ref).equals(table)):
                raise ValueError("immutable public archive extent conflict")
            existing[ref] = prior
        sealed = self.seal_many(tuple(pending))
        if sealed:
            self.repository.register_artifacts(sealed)
        indexed = {entry.artifact_ref: entry for entry in sealed}
        indexed.update(existing)
        if set(indexed) != set(refs):
            raise RuntimeError("public archive extent batch did not resolve every descriptor")
        return tuple((ref, self.root / str(indexed[ref].metadata["extent"]["segment_name"])) for ref in refs)


def write_extent(repository: OpsRepository, table: Any, *, namespace: str, chunk_id: str,
                 clock_ns: Callable[[], int], floor_ns: int) -> tuple[str, Path]:
    writer = repository._public_extent_writer
    if writer is None:
        writer = PublicArchiveExtentWriterV1(repository)
        repository._public_extent_writer = writer
    return writer.write(table, namespace=namespace, chunk_id=chunk_id, clock_ns=clock_ns, floor_ns=floor_ns)


def write_extents(
    repository: OpsRepository,
    chunks: tuple[tuple[Any, str, str, Callable[[], int], int], ...],
) -> tuple[tuple[str, Path], ...]:
    """Durably append a bounded extent set before atomically indexing it."""
    writer = repository._public_extent_writer
    if writer is None:
        writer = PublicArchiveExtentWriterV1(repository)
        repository._public_extent_writer = writer
    return writer.write_many(chunks)


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
