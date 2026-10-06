"""Bounded transport capture owned by atlas-ops, isolated from SQLite commits.

The capture thread has no repository, SQL connection, typed-book, or decision
authority. It seals exact FIFO Arrow transport extents and immutable descriptors.
Only the controller adopts those descriptors into the sole operational index.
Unadopted extents/manifests survive a crash as raw evidence, not continuity.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .._serialization import json_value, sha256_json
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from .capture_receipts import (
    CaptureReceiptBindingV1,
    CaptureReceiptJournalV1,
    _replace_durable_pointer,
    read_last_capture_receipt,
)
from .public_archive_extents import PublicArchiveSegmentWriterV1, read_extent
from .public_microstructure_ws import CapturedPublicFrameV2

MAX_PENDING_CAPTURE_BATCHES = 64
MAX_CAPTURE_BATCH_FRAMES = 256
CAPTURE_ACCUMULATION_SECONDS = 0.2


@dataclass(frozen=True)
class SealedPublicTransportV1:
    extent: ArtifactIndexEntryV2
    batch: ArtifactIndexEntryV2
    frame_count: int

    def adopt(self, repository: OpsRepository) -> tuple[CapturedPublicFrameV2, ...]:
        """Controller-only publication and strict reconstruction before interpretation."""
        repository.register_artifact(self.extent)
        rows = read_extent(repository, self.extent.artifact_ref).to_pylist()
        if not 1 <= len(rows) == self.frame_count <= MAX_CAPTURE_BATCH_FRAMES:
            raise ValueError("sealed transport frame population invalid")
        frames = []
        headers = []
        for index, row in enumerate(rows):
            if row["fifo_index"] != index:
                raise ValueError("sealed transport FIFO changed")
            frame = CapturedPublicFrameV2(row["venue"], row["source_id"], row["channel"],
                row["raw_payload_bytes"], row["raw_payload_hash"], row["received_at_ns"],
                row["available_at_ns"], row["connection_epoch"])
            frames.append(frame)
            headers.append({k: v for k, v in row.items() if k != "raw_payload_bytes"})
        body = json_value(self.batch.metadata["batch"])
        if (body["chunk_id"] != sha256_json({"version": "PublicStreamTransportBatchV1", "frames": headers})
                or body["archive_extent_ref"] != self.extent.artifact_ref
                or body["frame_count"] != self.frame_count
                or body["first_received_at_ns"] != frames[0].received_at_ns
                or body["last_received_at_ns"] != frames[-1].received_at_ns
                or sha256_json(body) != self.batch.content_hash
                or self.batch.artifact_ref != self.batch.content_hash
                or self.extent.available_at_ns > self.batch.available_at_ns):
            raise ValueError("sealed transport identity or chronology changed")
        repository.register_artifact(self.batch)
        return tuple(frames)


class DurablePublicCaptureV1:
    """One bounded raw-capture thread; no ops.sqlite connection is passed to it.

    The original 512/16MB handoff is unchanged. Writer backlog consists of at
    most 64 immutable descriptors; frame bodies live in retained raw archives.
    Exhaustion/failure closes the producer, preserves pending raw evidence and
    exposes terminal failure. Nothing is silently sampled or discarded.
    """

    def __init__(self, source: Any, *, clock_ns: Callable[[], int] = time.time_ns) -> None:
        self.source = source
        self.venue = source.venue
        self.topics = source.topics
        self.clock_ns = clock_ns
        self._root: Path | None = None
        self._pending: deque[SealedPublicTransportV1] = deque()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: str | None = None
        self._captured_frames = 0
        self._delivered_frames = 0
        self._high_water_batches = 0
        self._last_capture_ns: int | None = None
        self._max_capture_duration_ns = 0
        self._capture_started_monotonic_ns: int | None = None
        self._writer: PublicArchiveSegmentWriterV1 | None = None
        self._journal: CaptureReceiptJournalV1 | None = None
        self._binding: CaptureReceiptBindingV1 | None = None
        self._controller_capture_started = False
        self._controller_capture_clean = False

    def configure_capture(self, run_root: Path, *, capture_epoch: str = "0" * 64) -> None:
        if self._thread is not None:
            raise RuntimeError("cannot rebind active capture root")
        self._root = run_root
        identity_path = run_root / "run.json"
        if identity_path.exists():
            if identity_path.stat().st_size > 128 * 1024 or identity_path.is_symlink():
                raise ValueError("capture run identity unavailable or oversized")
            identity = json.loads(identity_path.read_text())
            if identity["content_hash"] != sha256_json({k: v for k, v in identity.items() if k != "content_hash"}):
                raise ValueError("capture run identity changed")
            self._binding = CaptureReceiptBindingV1(identity["run_id"], identity["config_hash"], capture_epoch)
        else:
            # Explicitly diagnostic standalone integration fixture, never an
            # installed run. Installed startup requires its validated run.json.
            self._binding = CaptureReceiptBindingV1("standalone-capture-fixture", "0" * 64, capture_epoch)

    def recover_controller_capture(self, repository: OpsRepository) -> None:
        """Bounded tail/head recovery before a producer starts; never infer continuity.

        A crashed active epoch may contain unindexed raw or incomplete transport
        receipts. Preserve it and require a new run even when its last known
        batch was indexed. A clean head additionally proves its final receipt's
        exact index binding using this existing sole writer.
        """
        assert self._root is not None and self._binding is not None
        head = self._root / "public-capture-lifecycle-v1.json"
        try:
            if head.exists():
                if head.is_symlink() or head.stat().st_size > 8192:
                    raise ValueError("capture lifecycle head invalid")
                body = json.loads(head.read_text())
                expected = {"version", "run_id", "config_hash", "capture_epoch", "state",
                            "observed_at_ns", "last_batch_ref", "authority", "content_hash"}
                if (set(body) != expected or body["version"] != "PUBLIC_CAPTURE_LIFECYCLE_V1"
                        or body["run_id"] != self._binding.run_id or body["config_hash"] != self._binding.configuration_hash
                        or body["authority"] != "ZERO" or body["state"] not in ("ACTIVE", "CLEAN")
                        or sha256_json({k: v for k, v in body.items() if k != "content_hash"}) != body["content_hash"]):
                    raise ValueError("capture lifecycle identity or hash changed")
                if body["state"] == "ACTIVE":
                    self._latch_recovery_failure(unclean=True)
                    raise RuntimeError("UNCLEAN_PUBLIC_CAPTURE_STOP_NEW_RUN_REQUIRED")
            tail_evidence = read_last_capture_receipt(self._root / "ops-public-capture",
                expected_run_id=self._binding.run_id, expected_configuration_hash=self._binding.configuration_hash)
            tail = tail_evidence.receipt if tail_evidence is not None else None
            if tail is not None:
                if tail.get("version") != "SEALED_PUBLIC_TRANSPORT_V1":
                    raise ValueError("previous capture ended with terminal failure")
                entry = repository.get_artifact(tail["batch_ref"])
                if entry is None or json_value(entry.metadata["batch"]) != tail["batch"]:
                    raise ValueError("previous captured tail is not exactly indexed")
                extent = repository.get_artifact(tail["extent"]["artifact_ref"])
                if extent is None:
                    raise ValueError("previous capture extent is not indexed")
                read_extent(repository, extent.artifact_ref)  # exact retained bytes still required
                if not head.exists() or body["last_batch_ref"] != entry.artifact_ref:
                    raise ValueError("clean capture head does not bind its final receipt")
            self._write_lifecycle("ACTIVE", None)
            self._controller_capture_started = True
        except ValueError:
            self._latch_recovery_failure(unclean=False)
            raise

    def _latch_recovery_failure(self, *, unclean: bool) -> None:
        # Startup is still on the controller, before the watchdog exists.
        # This is a filesystem qualification record, never a second DB writer.
        from ..runtime.live_health import LiveHealthControllerV1, LiveHealthFactsV1
        from .health import PublicSourceStateV2

        assert self._root is not None and self._binding is not None
        now = self.clock_ns()
        status = self.source.status()
        h = status.handoff
        facts = LiveHealthFactsV1(self._binding.run_id, self._binding.configuration_hash, now, now, now,
            status.state, bool(h.connected), PublicSourceStateV2.INCOMPLETE_SNAPSHOT, True,
            h.queue_items, h.max_queue_items, h.high_water_items, unclean_capture_stop=unclean,
            evidence_integrity_failure=not unclean)
        LiveHealthControllerV1(self._root, run_id=self._binding.run_id,
                              config_hash=self._binding.configuration_hash).observe(facts)

    def _write_lifecycle(self, state: str, last_ref: str | None) -> None:
        import os

        from .._serialization import canonical_json

        assert self._root is not None and self._binding is not None
        body = {"version": "PUBLIC_CAPTURE_LIFECYCLE_V1", "run_id": self._binding.run_id,
            "config_hash": self._binding.configuration_hash, "capture_epoch": self._binding.capture_epoch,
            "state": state, "observed_at_ns": self.clock_ns(), "last_batch_ref": last_ref, "authority": "ZERO"}
        body["content_hash"] = sha256_json(body)
        path = self._root / "public-capture-lifecycle-v1.json"
        temporary = path.with_suffix(".tmp")
        with temporary.open("wb") as handle:
            handle.write((canonical_json(body) + "\n").encode())
            handle.flush()
            os.fsync(handle.fileno())
        _replace_durable_pointer(temporary, path)

    def mark_controller_capture_clean(self, repository: OpsRepository) -> None:
        if not self._controller_capture_started or self._controller_capture_clean:
            return
        assert self._root is not None and self._binding is not None
        status = self.status()
        if (status.state == "FAILED" or status.capture["terminal_error"] or status.pending_frames or status.handoff.queue_items
                or status.handoff.overflowed or status.handoff.frames_rejected
                or self._thread is not None and self._thread.is_alive()):
            raise RuntimeError("PUBLIC_CAPTURE_CLEAN_STOP_NOT_PROVEN")
        tail_evidence = read_last_capture_receipt(self._root / "ops-public-capture",
            expected_run_id=self._binding.run_id, expected_configuration_hash=self._binding.configuration_hash)
        tail = tail_evidence.receipt if tail_evidence is not None else None
        last_ref = tail["batch_ref"] if tail is not None and tail.get("version") == "SEALED_PUBLIC_TRANSPORT_V1" else None
        if tail is not None and (last_ref is None or repository.get_artifact(last_ref) is None):
            raise ValueError("final capture receipt not adopted by sole writer")
        self._write_lifecycle("CLEAN", last_ref)
        self._controller_capture_clean = True

    def start(self) -> None:
        if self._root is None:
            raise RuntimeError("durable capture requires the controller-bound run root")
        if self._thread is not None:
            return
        # Import/initialize Arrow before network arrivals; cold imports must
        # never consume the finite forward-capture headroom.
        import pyarrow as pa

        pa.table({"capture_warmup": [1]})
        assert self._binding is not None
        self._journal = CaptureReceiptJournalV1(self._root / "ops-public-capture", binding=self._binding)
        self.source.start()
        self._thread = threading.Thread(target=self._run, name="atlas-raw-capture", daemon=True)
        self._thread.start()

    def status(self) -> Any:
        status = self.source.status()
        with self._lock:
            backlog = sum(batch.frame_count for batch in self._pending)
            capture = {"version": "DURABLE_PUBLIC_CAPTURE_V1", "pending_batches": len(self._pending),
                "max_pending_batches": MAX_PENDING_CAPTURE_BATCHES, "pending_frames": backlog,
                "high_water_batches": self._high_water_batches, "captured_frames": self._captured_frames,
                "delivered_frames": self._delivered_frames, "last_capture_at_ns": self._last_capture_ns,
                "max_capture_duration_ns": self._max_capture_duration_ns,
                "active_capture_duration_ns": (time.monotonic_ns() - self._capture_started_monotonic_ns
                    if self._capture_started_monotonic_ns is not None else 0),
                "archive": dict(self._writer.metrics) if self._writer is not None else {},
                "archive_bytes_written": self._writer.bytes_written if self._writer is not None else 0,
                "receipt_bytes_written": self._journal.bytes_written if self._journal is not None else 0,
                "terminal_error": self._error, "authority": "ZERO"}
            error = self._error
        return SimpleNamespace(state="FAILED" if error else status.state,
            attempt_count=status.attempt_count, reconnect_count=status.reconnect_count,
            last_error_code=error or status.last_error_code, handoff=status.handoff,
            pending_frames=backlog, capture=capture)

    def drain_sealed_transport(self) -> SealedPublicTransportV1 | None:
        with self._lock:
            if self._pending:
                batch = self._pending.popleft()
                self._delivered_frames += batch.frame_count
                return batch
            return None

    def drain(self, *, max_items: int | None = None) -> tuple[CapturedPublicFrameV2, ...]:
        raise RuntimeError("durable capture has one consumer; use sealed transport adoption")

    def request_pressure_stop(self) -> None:
        """Stop arrivals before predicted exhaustion; retain all queued raw.

        This is a terminal operational stop, not fabricated local frame loss.
        No SQLite access, index publication or decision authority is involved.
        """
        with self._lock:
            if self._error is not None:
                return
            self._error = "PREVENTIVE_CAPTURE_PRESSURE_STOP"
        request = getattr(self.source, "request_close", None)
        if callable(request):
            request()
        else:
            self.source.close()  # Explicit offline fixture seam.
        self._stop.set()

    def close(self) -> None:
        self.source.close()
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                raise TimeoutError("raw capture did not stop within bounded shutdown")

    def _seal(self, writer: PublicArchiveSegmentWriterV1,
              frames: tuple[CapturedPublicFrameV2, ...]) -> SealedPublicTransportV1:
        import pyarrow as pa

        headers = [{"venue": f.venue.value, "source_id": f.source_id, "channel": f.channel,
            "raw_payload_hash": f.raw_payload_hash, "received_at_ns": f.received_at_ns,
            "available_at_ns": f.available_at_ns, "connection_epoch": f.connection_epoch,
            "fifo_index": index} for index, f in enumerate(frames)]
        chunk_id = sha256_json({"version": "PublicStreamTransportBatchV1", "frames": headers})
        floor = max(f.available_at_ns for f in frames)
        extent = writer.seal(pa.Table.from_pylist([
            {**header, "raw_payload_bytes": frame.raw_payload_bytes}
            for header, frame in zip(headers, frames, strict=True)]), namespace="ops-public-transport",
            chunk_id=chunk_id, clock_ns=self.clock_ns, floor_ns=floor)
        from ..chronology import sample

        available = sample(self.clock_ns, floor_ns=extent.available_at_ns)
        body = {"version": "PublicStreamTransportBatchV2", "chunk_id": chunk_id,
            "archive_extent_ref": extent.artifact_ref, "frame_count": len(frames),
            "first_received_at_ns": frames[0].received_at_ns,
            "last_received_at_ns": frames[-1].received_at_ns,
            "available_at_ns": available, "authority": "ZERO"}
        ref = sha256_json(body)
        batch = ArtifactIndexEntryV2(ref, "PublicStreamTransportBatchV2", ref,
                                     available, available, {"batch": body})
        # This immutable receipt permits reconstructing captured-but-unindexed
        # raw after a controller crash. It confers no source qualification.
        assert self._root is not None
        manifests = self._root / "ops-public-capture"
        manifests.mkdir(exist_ok=True)
        receipt = {"version": "SEALED_PUBLIC_TRANSPORT_V1", "authority": "ZERO",
            "extent": {"artifact_ref": extent.artifact_ref, "artifact_type": extent.artifact_type,
                "content_hash": extent.content_hash, "created_at_ns": extent.created_at_ns,
                "available_at_ns": extent.available_at_ns, "metadata": json_value(extent.metadata)},
            "batch": body, "batch_ref": ref}
        assert self._journal is not None
        self._journal.append(receipt)
        return SealedPublicTransportV1(extent, batch, len(frames))

    def _run(self) -> None:
        assert self._root is not None
        writer = PublicArchiveSegmentWriterV1(self._root / "ops-public-extents")
        self._writer = writer
        last = time.monotonic()
        try:
            while True:
                status = self.source.status()
                queued = status.handoff.queue_items
                if not queued:
                    if self._stop.is_set():
                        if self._error == "PREVENTIVE_CAPTURE_PRESSURE_STOP" and self._journal is not None:
                            self._journal.append({"version": "PUBLIC_RAW_CAPTURE_FAILURE_V1", "authority": "ZERO",
                                "reason": self._error, "observed_at_ns": self.clock_ns(),
                                "captured_frames": self._captured_frames})
                        return
                    self._stop.wait(0.01)
                    continue
                if queued < 64 and not self._stop.is_set() and time.monotonic() - last < CAPTURE_ACCUMULATION_SECONDS:
                    self._stop.wait(0.01)
                    continue
                with self._lock:
                    if len(self._pending) >= MAX_PENDING_CAPTURE_BATCHES:
                        raise RuntimeError("CAPTURE_WRITER_BACKLOG_EXHAUSTED")
                started = time.monotonic_ns()
                self._capture_started_monotonic_ns = started
                frames = self.source.drain(max_items=min(64, queued))
                if not frames:
                    self._capture_started_monotonic_ns = None
                    continue
                batch = self._seal(writer, frames)
                with self._lock:
                    self._pending.append(batch)
                    self._captured_frames += len(frames)
                    self._high_water_batches = max(self._high_water_batches, len(self._pending))
                    self._last_capture_ns = self.clock_ns()
                    self._max_capture_duration_ns = max(self._max_capture_duration_ns,
                                                         time.monotonic_ns() - started)
                last = time.monotonic()
                self._capture_started_monotonic_ns = None
        except Exception as exc:
            with self._lock:
                self._error = (str(exc) if str(exc) == "CAPTURE_WRITER_BACKLOG_EXHAUSTED"
                               else "RAW_CAPTURE_FAILED_" + type(exc).__name__.upper())
            # A separate immutable capture receipt is retained even when the
            # controller is blocked in commit. A disk failure can prevent this
            # receipt too; terminal in-memory facts still cannot become healthy.
            try:
                failure = {"version": "PUBLIC_RAW_CAPTURE_FAILURE_V1", "authority": "ZERO",
                    "reason": self._error, "observed_at_ns": self.clock_ns(),
                    "captured_frames": self._captured_frames}
                if self._journal is not None:
                    self._journal.append(failure)
            except (OSError, RuntimeError, ValueError):
                pass
            self.source.close()
