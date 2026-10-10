"""Supervisor-owned lifecycle for dynamically planned broad public streams."""

from __future__ import annotations

import base64
import concurrent.futures
import copy
import hashlib
import json
import time
from collections import deque
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .._serialization import sha256_json
from ..data.broad_stream_source import (
    BroadDurablePublicCaptureV2,
    BroadPublicStreamPlanV2,
    BroadPublicStreamSourceV2,
)
from ..data.durable_public_capture import SealedPublicTransportV1
from ..data.health import PublicSourceHealthV2, PublicSourceStateV2
from ..data.microstructure import (
    BookStateV2,
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from ..data.microstructure_archive import (
    L2FrameArchiveV2,
    L2RawFrameV2,
    PreparedL2ArchiveBatchV2,
    PreparedL2ArchiveChunkV2,
)
from ..data.public_evidence_preparation import (
    MAX_PREPARATION_FRAMES_V2,
    MAX_PREPARATION_INPUT_BYTES_V2,
    MAX_PREPARATION_OUTPUT_RECORDS_V2,
    MAX_PREPARATION_SEAL_CHUNKS_V2,
    MAX_PREPARATION_TRADE_ROWS_V2,
    PREPARATION_VERSION_V2,
    PublicEvidencePreparationRequestV2,
    PublicEvidencePreparationResultV2,
    PublicEvidencePreparationWorkerV2,
    decode_prepared_events,
    read_preparation_input,
    read_preparation_result,
    write_preparation_input,
)
from ..data.public_microstructure_ws import (
    CapturedPublicFrameV2,
    parse_binance_rest_snapshot,
    raw_archive_record,
)
from ..data.public_stream_continuity import (
    PublicStreamContinuityDecisionV1,
    PublicStreamContinuityTrackerV1,
    PublicStreamObservationKindV1,
    PublicStreamObservationV1,
)
from ..instruments import InstrumentKeyV2, ProductContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository, PublicAdoptionCursorV2

_PHASE_TIMING_NAMES = (
    "input_write",
    "worker_wait",
    "worker_poll",
    "worker_pending",
    "worker_execution",
    "seal_worker_wait",
    "seal_worker_execution",
    "result_read_rehydrate",
    "seal_result_read",
    "stage_commit",
    "interpreter",
    "archive_seal",
    "archive_prepare_chunks",
    "archive_arrow_encode",
    "seal_input_write",
    "archive_compression",
    "archive_extent_write",
    "archive_extent_fsync",
    "archive_extent_rename",
    "archive_directory_sync",
    "public_commit",
)


@dataclass(frozen=True)
class _PreparedFrameParseV2:
    events: tuple[Any, ...]
    trade_payload_hashes: tuple[str, ...]


@dataclass(frozen=True)
class _PreparedFramePublicationV2:
    artifact_entries: tuple[ArtifactIndexEntryV2, ...]
    archive_groups: tuple[tuple[L2RawFrameV2, ...], ...]


@dataclass
class _PendingPublicAdoptionV2:
    sealed: SealedPublicTransportV1
    steps: Generator[PublicEvidencePreparationRequestV2,
                     PublicEvidencePreparationResultV2, tuple[CapturedPublicFrameV2, ...]]
    request: PublicEvidencePreparationRequestV2
    submitted_at_ns: int


class BroadPublicRuntimeV2:
    """Start only after dynamic metadata/tiering is available, then capture globally.

    Exact transport batches are adopted before ``interpret_frames`` is called.
    The supervisor remains the sole repository writer; this class has no
    background interpreter or network credentials.
    """

    def __init__(
        self,
        *,
        stream_factories: Mapping[str, Callable[[], Any]] | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
        interpret_frames: Callable[[OpsRepository, BroadPublicStreamPlanV2, tuple[Any, ...], int], None] | None = None,
        snapshot_reader: Callable[[InstrumentKeyV2, int], tuple[bytes, int]] | None = None,
    ) -> None:
        self.stream_factories = dict(stream_factories or {})
        self.clock_ns = clock_ns
        self.interpret_frames = interpret_frames
        self.snapshot_reader = snapshot_reader
        self.plan: BroadPublicStreamPlanV2 | None = None
        self.source: BroadPublicStreamSourceV2 | None = None
        self.capture: BroadDurablePublicCaptureV2 | None = None
        self._service_calls = 0
        self._service_frames = 0
        self._last_service_at_ns: int | None = None
        self._last_service_started_monotonic_ns: int | None = None
        self._last_service_duration_ns = 0
        self._max_service_gap_ns = 0
        self._max_service_duration_ns = 0
        self._terminal_error: str | None = None
        self._run_root: Path | None = None
        self._run_id: str | None = None
        self._capture_epoch = "0" * 64
        self._archive: L2FrameArchiveV2 | None = None
        self._preparation_worker: PublicEvidencePreparationWorkerV2 | None = None
        self._pending_adoption: _PendingPublicAdoptionV2 | None = None
        self._products: dict[InstrumentKeyV2, ProductContractV2] = {}
        self._sequence_books: dict[InstrumentKeyV2, SequenceValidBookV2] = {}
        self._trackers: dict[tuple[InstrumentKeyV2, str], PublicStreamContinuityTrackerV1] = {}
        self._lane_health: dict[str, str] = {}
        self._stream_epoch = ""
        self._snapshot_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._snapshot_future: concurrent.futures.Future[tuple[bytes, int]] | None = None
        self._snapshot_binding: tuple[str, InstrumentKeyV2, str, int | None] | None = None
        self._snapshot_started_monotonic_ns: int | None = None
        self._snapshot_buffer: list[tuple[ProductContractV2, CapturedPublicFrameV2, L2DeltaV2]] = []
        self._snapshot_next_retry_monotonic_ns = 0
        self._snapshot_failure: str | None = None
        self._connection_epochs: dict[tuple[InstrumentKeyV2, str], int | None] = {}
        self._snapshot_buffer_bytes = 0
        self._deferred_snapshot_requests: list[tuple[ProductContractV2, CapturedPublicFrameV2,
                                                     L2DeltaV2, int]] | None = None
        self._prepared_events_for_call: Mapping[int, _PreparedFrameParseV2] | None = None
        self._defer_publication_for_call = False
        self._indexed_frames_by_lane: dict[str, int] = {}
        self._occupancy_samples: deque[dict[str, Any]] = deque(maxlen=8192)
        self._phase_timing_stats_ns = {name: [0, 0, 0] for name in _PHASE_TIMING_NAMES}

    def _record_phase_duration_ns(self, phase: str, duration_ns: int) -> None:
        stats = self._phase_timing_stats_ns[phase]
        duration = max(0, int(duration_ns))
        stats[0] += 1
        stats[1] += duration
        stats[2] = max(stats[2], duration)

    def _phase_timing_summary_ns(self) -> dict[str, dict[str, int]]:
        return {
            phase: {"count": stats[0], "sum_ns": stats[1], "max_ns": stats[2]}
            for phase, stats in self._phase_timing_stats_ns.items()
        }

    @contextmanager
    def _measure_phase(self, phase: str):
        started = time.monotonic_ns()
        try:
            yield
        finally:
            self._record_phase_duration_ns(phase, time.monotonic_ns() - started)

    def _submit_preparation(
        self,
        steps: Generator[PublicEvidencePreparationRequestV2,
                         PublicEvidencePreparationResultV2, tuple[CapturedPublicFrameV2, ...]],
        sealed: SealedPublicTransportV1,
    ) -> _PendingPublicAdoptionV2:
        worker = self._preparation_worker
        if worker is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_WORKER_UNAVAILABLE")
        request = next(steps)
        worker.submit(request)
        return _PendingPublicAdoptionV2(sealed, steps, request, time.monotonic_ns())

    def _advance_preparation(self) -> tuple[bool, tuple[CapturedPublicFrameV2, ...] | None]:
        """Poll the single worker slot; continuation work stays on this writer."""
        pending = self._pending_adoption
        worker = self._preparation_worker
        if pending is None or worker is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_PENDING_STATE_INVALID")
        started = time.monotonic_ns()
        result = worker.poll()
        self._record_phase_duration_ns("worker_poll", time.monotonic_ns() - started)
        if result is None:
            return False, None
        result = worker.take_completion()
        self._record_phase_duration_ns("worker_pending", time.monotonic_ns() - pending.submitted_at_ns)
        self._record_phase_duration_ns("worker_execution", result.execution_duration_ns)
        if pending.request.kind == "SEAL":
            self._record_phase_duration_ns("seal_worker_execution", result.execution_duration_ns)
        try:
            next_request = pending.steps.send(result)
        except StopIteration as completed:
            self._pending_adoption = None
            return True, completed.value
        worker.submit(next_request)
        pending.request = next_request
        pending.submitted_at_ns = time.monotonic_ns()
        return True, None

    @property
    def sequence_books(self) -> Mapping[InstrumentKeyV2, SequenceValidBookV2]:
        return dict(self._sequence_books)

    @staticmethod
    def _run_identity(run_root: Path) -> str:
        root = Path(run_root).resolve()
        manifest_path = root / "run.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            run_id = manifest.get("run_id") if isinstance(manifest, dict) else None
            if not isinstance(run_id, str) or run_id != root.name:
                raise ValueError("public preparation run manifest does not bind its run directory")
            return run_id
        # Isolated runtime fixtures omit the product run manifest. Production
        # runs always carry it, while the directory name remains stable in the
        # focused repository tests.
        if not root.name:
            raise ValueError("public preparation run root has no stable identity")
        return root.name

    def recover(
        self,
        repository: OpsRepository,
        *,
        run_root: Path,
        products: tuple[ProductContractV2, ...],
        tiers: Mapping[InstrumentKeyV2, Any],
        now_ns: int,
        benchmark_keys: tuple[InstrumentKeyV2, ...] = (),
        open_positions: tuple[InstrumentKeyV2, ...] = (),
        active_watch_keys: tuple[InstrumentKeyV2, ...] = (),
    ) -> BroadPublicStreamPlanV2:
        """Bind and recover the stream after metadata has produced the plan population."""
        if repository.read_only:
            raise ValueError("broad public recovery requires the supervisor-owned writable repository")
        plan = BroadPublicStreamPlanV2.build(
            products, tiers, created_at_ns=now_ns, benchmark_keys=benchmark_keys,
            open_positions=open_positions, active_watch_keys=active_watch_keys,
        )
        if self.plan is not None and self.plan.plan_id != plan.plan_id:
            if (self.plan.keys, self.plan.lane_topics, self.plan.source_refs) != (
                plan.keys, plan.lane_topics, plan.source_refs,
            ):
                raise RuntimeError("BROAD_PUBLIC_STREAM_PLAN_CHANGED_USE_RECONFIGURE")
        if self.capture is None:
            if self._capture_epoch == "0" * 64:
                self._capture_epoch = sha256_json({"plan_id": plan.plan_id, "recovered_at_ns": now_ns})
            self.plan = plan
            self.source = BroadPublicStreamSourceV2(
                plan, stream_factories=self.stream_factories, clock_ns=self.clock_ns,
            )
            self.capture = BroadDurablePublicCaptureV2(self.source, clock_ns=self.clock_ns)
            self._run_root = run_root
            self._run_id = self._run_identity(run_root)
            self._archive = L2FrameArchiveV2(run_root.parent / "ops-l2-frames", repository,
                                             compact_live=True, clock_ns=self.clock_ns)
            if self._preparation_worker is None:
                self._preparation_worker = PublicEvidencePreparationWorkerV2()
            self._preparation_worker.start(run_root)
            self._products = {product.key: product for product in products}
            self._stream_epoch = sha256_json({"plan_id": plan.plan_id, "capture_epoch": self._capture_epoch})
            self.capture.configure_capture(run_root, capture_epoch=self._capture_epoch)
            self.capture.recover_controller_capture(repository)
            self.capture.start()
        return plan

    def reconfigure(
        self,
        repository: OpsRepository,
        *,
        products: tuple[ProductContractV2, ...],
        tiers: Mapping[InstrumentKeyV2, Any],
        now_ns: int,
        benchmark_keys: tuple[InstrumentKeyV2, ...] = (),
        open_positions: tuple[InstrumentKeyV2, ...] = (),
        active_watch_keys: tuple[InstrumentKeyV2, ...] = (),
    ) -> BroadPublicStreamPlanV2:
        """Rotate subscriptions after draining and sealing the previous lane."""
        if self._run_root is None:
            raise RuntimeError("BROAD_PUBLIC_STREAM_NOT_RECOVERED")
        candidate = BroadPublicStreamPlanV2.build(
            products, tiers, created_at_ns=now_ns, benchmark_keys=benchmark_keys,
            open_positions=open_positions, active_watch_keys=active_watch_keys,
        )
        if self.plan is not None and (self.plan.keys, self.plan.lane_topics, self.plan.source_refs) == (
            candidate.keys, candidate.lane_topics, candidate.source_refs,
        ):
            return self.plan
        self.finish(repository)
        if self._snapshot_future is not None:
            try:
                self._snapshot_future.result(timeout=5.0)
            except concurrent.futures.TimeoutError as exc:
                raise RuntimeError("BROAD_STREAM_RECONFIGURE_SNAPSHOT_WORKER_STILL_ACTIVE") from exc
            except Exception:
                pass
            self._snapshot_future = None
            self._snapshot_binding = None
            self._snapshot_buffer.clear()
            self._snapshot_buffer_bytes = 0
            self._snapshot_started_monotonic_ns = None
        self._sequence_books.clear()
        self._trackers.clear()
        self._connection_epochs.clear()
        self._lane_health.clear()
        self.plan = None
        self.capture = None
        self.source = None
        self._capture_epoch = sha256_json({"plan_id": candidate.plan_id, "rotated_at_ns": now_ns})
        return self.recover(
            repository, run_root=self._run_root, products=products, tiers=tiers, now_ns=now_ns,
            benchmark_keys=benchmark_keys, open_positions=open_positions,
            active_watch_keys=active_watch_keys,
        )

    def service(self, repository: OpsRepository, *, now_ns: int) -> None:
        """Adopt and interpret at most four sealed FIFO batches / 50 ms per call."""
        if self.capture is None or self.plan is None:
            return
        if self._terminal_error is not None:
            raise RuntimeError("BROAD_PUBLIC_RUNTIME_RESTART_REQUIRED") from RuntimeError(self._terminal_error)
        started = time.monotonic_ns()
        if self._last_service_started_monotonic_ns is not None:
            self._max_service_gap_ns = max(self._max_service_gap_ns,
                started - self._last_service_started_monotonic_ns)
        self._last_service_started_monotonic_ns = started
        try:
            # Snapshot progress and deadlines must advance even when the
            # transport is quiet and there is no new sealed batch to adopt.
            grouped: dict[tuple[str, str, str], list[L2RawFrameV2]] = {}
            self._poll_snapshot(repository, now_ns=now_ns, grouped=grouped)
            if self._archive is not None:
                if grouped:
                    self._archive.write_chunks(tuple(tuple(group) for group in grouped.values()))
            for _ in range(4):
                if self.interpret_frames is None and self._pending_adoption is not None:
                    _progressed, frames = self._advance_preparation()
                    if frames is None:
                        break
                    self._record_adopted_frames(frames)
                    if time.monotonic_ns() - started >= 50_000_000:
                        break
                    continue
                status = self.capture.status()
                if status.state == "FAILED" or status.capture.get("terminal_error"):
                    raise RuntimeError("BROAD_PUBLIC_CAPTURE_TERMINAL_FAILURE")
                if not status.pending_frames:
                    break
                sealed = self.capture.drain_sealed_transport()
                if sealed is None:
                    break
                if not isinstance(sealed, SealedPublicTransportV1):
                    raise TypeError("broad public capture returned an invalid sealed batch")
                if self.interpret_frames is None:
                    steps = self._adopt_sealed_descriptor(repository, sealed, now_ns=now_ns)
                    self._pending_adoption = self._submit_preparation(steps, sealed)
                    # One descriptor is retained until every worker result is
                    # validated and the sole writer commits its publication.
                    break
                # Compatibility callback remains a synchronous, isolated test seam.
                else:
                    with self._measure_phase("public_commit"), repository.atomic_composition():
                        frames = sealed.adopt(repository)
                        with self._measure_phase("interpreter"):
                            self.interpret_frames(repository, self.plan, frames, now_ns)
                self._record_adopted_frames(frames)
                if time.monotonic_ns() - started >= 50_000_000:
                    break
        except Exception as exc:
            self._terminal_error = f"{type(exc).__name__}:{exc}"
            try:
                self.capture.close()
            finally:
                raise
        finally:
            self._record_occupancy_sample()
            self._service_calls += 1
            self._last_service_at_ns = now_ns
            self._last_service_duration_ns = time.monotonic_ns() - started
            self._max_service_duration_ns = max(self._max_service_duration_ns, self._last_service_duration_ns)

    def _record_adopted_frames(self, frames: tuple[CapturedPublicFrameV2, ...]) -> None:
        self._service_frames += len(frames)
        for frame in frames:
            lane = frame.source_id.removesuffix("_PUBLIC_WS_BROAD_V2")
            self._indexed_frames_by_lane[lane] = self._indexed_frames_by_lane.get(lane, 0) + 1

    def finish(self, repository: OpsRepository) -> None:
        if self.capture is None:
            return
        self.capture.close()
        if self._terminal_error is not None:
            raise RuntimeError("BROAD_PUBLIC_CAPTURE_NOT_CLEAN")
        deadline = time.monotonic() + 5.0
        while True:
            capture_pending = self.capture.status().pending_frames
            if not capture_pending and self._pending_adoption is None:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("BROAD_PUBLIC_CAPTURE_FINAL_BACKLOG_DEADLINE_EXCEEDED")
            self.service(repository, now_ns=self.clock_ns())
            # Polling is nonblocking by design. Yield briefly while the single
            # bounded process job runs instead of spinning the sole writer.
            if self._pending_adoption is not None:
                time.sleep(0.001)
        if self.capture.status().pending_frames or self._pending_adoption is not None:
            raise RuntimeError("BROAD_PUBLIC_CAPTURE_FINAL_BACKLOG_EXCEEDED")
        self.capture.mark_controller_capture_clean(repository)

    def close(self) -> None:
        if self.capture is not None:
            self.capture.close()
        if self._snapshot_executor is not None:
            self._snapshot_executor.shutdown(wait=False, cancel_futures=True)
        if self._preparation_worker is not None:
            self._preparation_worker.close()

    def request_pressure_stop(self) -> None:
        if self.capture is not None:
            self.capture.request_pressure_stop()
        elif self.source is not None:
            self.source.request_close()

    def progress_snapshot(self) -> dict[str, Any]:
        # Read current lane status instead of depending on a previous caller
        # having refreshed the mutable diagnostic cache.
        self.status()
        lane_states = tuple(self._lane_health.values())
        healthy = self._terminal_error is None and bool(lane_states) and all(state == PublicSourceStateV2.HEALTHY_CURRENT.value
                                            for state in lane_states)
        return {
            "observed_at_ns": self.clock_ns(),
            "stream_recovery_required": self._terminal_error is not None or not healthy,
            "stream_source_state": (PublicSourceStateV2.HEALTHY_CURRENT.value if healthy else
                                    PublicSourceStateV2.INCOMPLETE_SNAPSHOT.value),
            "stream_source_states": dict(self._lane_health),
            "stream_unresolved_gap": (self._snapshot_failure is not None or any(
                book.sequence_state.state.value not in ("VALID", "WARMING")
                for book in self._sequence_books.values())),
            "stream_snapshot_failure": self._snapshot_failure,
            "stream_ingestion_failed": self._terminal_error is not None,
            "stream_service_gap_seconds": ((self.clock_ns() - self._last_service_at_ns) / 1_000_000_000
                                            if self._last_service_at_ns is not None else None),
            "stream_max_service_gap_seconds": self._max_service_gap_ns / 1_000_000_000,
            "stream_last_service_duration_seconds": self._last_service_duration_ns / 1_000_000_000,
            "stream_service_duration_seconds": self._max_service_duration_ns / 1_000_000_000,
            "stream_indexed_frames": sum(self._indexed_frames_by_lane.values()),
            "stream_indexed_frames_by_lane": dict(sorted(self._indexed_frames_by_lane.items())),
            "stream_occupancy_samples": tuple(dict(sample) for sample in self._occupancy_samples),
            "stream_phase_timings_ns": self._phase_timing_summary_ns(),
            "evidence_integrity_failure": self._terminal_error is not None,
        }

    def _record_occupancy_sample(self) -> None:
        try:
            pending = ((self.capture.status().pending_frames if self.capture is not None else 0)
                       + self._pending_adoption_frames())
        except Exception:
            pending = -1
        self._occupancy_samples.append({
            "observed_at_ns": self.clock_ns(),
            "pending_frames": pending,
            "indexed_frames": sum(self._indexed_frames_by_lane.values()),
            "indexed_by_lane": dict(sorted(self._indexed_frames_by_lane.items())),
        })

    def _pending_adoption_frames(self) -> int:
        pending = self._pending_adoption
        return pending.sealed.frame_count if pending is not None else 0

    def status(self) -> Any:
        if self.capture is None:
            handoff = SimpleNamespace(
                venue="BROAD", topics=(), queue_items=0, queue_bytes=0,
                max_queue_items=512, max_queue_bytes=16_000_000, max_drain_items=64,
                high_water_items=0, high_water_bytes=0, frames_received=0, frames_drained=0,
                controls_received=0, frames_rejected=0, closed_rejections=0, overflowed=False,
                backpressure=False, connected=False, closed=False, disconnect_count=0,
                last_disconnect_at_ns=None, heartbeat_count=0, last_heartbeat_at_ns=None,
                last_activity_at_ns=None, last_error_code=None, last_error_at_ns=None,
            )
            capture = {"pending_batches": 0, "max_pending_batches": 64, "pending_frames": 0,
                       "captured_frames": 0, "delivered_frames": 0, "terminal_error": self._terminal_error,
                       "active_capture_duration_ns": 0, "authority": "ZERO"}
            capture["adoption_pending_batches"] = int(self._pending_adoption is not None)
            capture["pending_frames"] = self._pending_adoption_frames()
            return SimpleNamespace(state="FAILED" if self._terminal_error else "CREATED", attempt_count=0,
                                   handoff=handoff, capture=capture, pending_frames=capture["pending_frames"],
                                   plan_id=None, lanes={},
                                   phase_timings_ns=self._phase_timing_summary_ns())
        source_status = self.capture.status()
        now_ns = self.clock_ns()
        for name, lane in source_status.lanes.items():
            h = lane.handoff
            self._lane_health[name] = (
                PublicSourceStateV2.HEALTHY_CURRENT.value
                if self._lane_is_current(lane, now_ns=now_ns)
                else PublicSourceStateV2.DISCONNECTED.value if lane.state == "FAILED" or h.closed
                else PublicSourceStateV2.INCOMPLETE_SNAPSHOT.value
            )
        capture = dict(source_status.capture)
        capture["adoption_pending_batches"] = int(self._pending_adoption is not None)
        capture["pending_frames"] = int(capture.get("pending_frames", 0)) + self._pending_adoption_frames()
        capture["terminal_error"] = self._terminal_error or capture.get("terminal_error")
        return SimpleNamespace(
            state=source_status.state, attempt_count=source_status.attempt_count,
            handoff=source_status.handoff, capture=capture, pending_frames=capture["pending_frames"],
            plan_id=self.plan.plan_id if self.plan is not None else None,
            lanes=source_status.lanes, service_calls=self._service_calls, service_frames=self._service_frames,
            last_service_at_ns=self._last_service_at_ns,
            last_service_duration_ns=self._last_service_duration_ns,
            max_service_gap_ns=self._max_service_gap_ns,
            max_service_duration_ns=self._max_service_duration_ns,
            indexed_frames=sum(self._indexed_frames_by_lane.values()),
            indexed_frames_by_lane=dict(sorted(self._indexed_frames_by_lane.items())),
            occupancy_samples=tuple(dict(sample) for sample in self._occupancy_samples),
            phase_timings_ns=self._phase_timing_summary_ns(),
            authority="ZERO",
        )

    def _adopt_sealed_descriptor(
        self,
        repository: OpsRepository,
        sealed: SealedPublicTransportV1,
        *,
        now_ns: int,
    ) -> Generator[PublicEvidencePreparationRequestV2,
                    PublicEvidencePreparationResultV2, tuple[CapturedPublicFrameV2, ...]]:
        """Writer-owned adoption continuation with nonblocking PARSE/SEAL yields."""
        frames = sealed.read_unpublished(self._run_root or Path(repository.path).parent)
        prepared = yield from self._prepare_descriptor_events(repository, sealed, frames, now_ns=now_ns)
        old_books, old_trackers, old_epochs = (
            self._sequence_books, self._trackers, self._connection_epochs,
        )
        staged_books, staged_trackers, staged_epochs = self._candidate_state_for_frames(frames)
        old_snapshot_failure = self._snapshot_failure
        deferred_snapshot_requests: list[tuple[ProductContractV2, CapturedPublicFrameV2,
                                              L2DeltaV2, int]] = []
        self._sequence_books, self._trackers, self._connection_epochs = (
            staged_books, staged_trackers, staged_epochs,
        )
        self._deferred_snapshot_requests = deferred_snapshot_requests
        self._prepared_events_for_call = prepared
        self._defer_publication_for_call = True
        try:
            with self._measure_phase("interpreter"):
                publication = self._interpret_frames(repository, frames, now_ns=now_ns)
            staged_snapshot_failure = self._snapshot_failure
        finally:
            self._sequence_books, self._trackers, self._connection_epochs = (
                old_books, old_trackers, old_epochs,
            )
            self._snapshot_failure = old_snapshot_failure
            self._deferred_snapshot_requests = None
            self._prepared_events_for_call = None
            self._defer_publication_for_call = False
        if publication is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_PUBLICATION_MISSING")
        if self._archive is None:
            raise RuntimeError("BROAD_PUBLIC_ARCHIVE_NOT_RECOVERED")
        with self._measure_phase("archive_seal"):
            archive_batch, archive_entries = yield from self._seal_prepared_archive(
                sealed, publication.archive_groups,
            )
        with self._measure_phase("public_commit"), repository.atomic_composition():
            adopted_frames = sealed.adopt_verified(repository, frames)
            if adopted_frames != frames:
                raise ValueError("BROAD_PUBLIC_DURABLE_DESCRIPTOR_CHANGED_DURING_PREPARATION")
            if publication.artifact_entries:
                repository.register_artifacts(publication.artifact_entries)
            self._archive.publish_prepared(archive_batch, archive_entries, repository=repository)
            if self._run_id is None:
                raise RuntimeError("BROAD_PUBLIC_RUN_ID_UNAVAILABLE")
            repository.clear_public_adoption_v2(self._run_id, sealed.batch.artifact_ref)
        # Candidate state becomes visible only after raw identity and all
        # derived rows commit together on the sole repository writer.
        self._sequence_books = staged_books
        self._trackers = staged_trackers
        self._connection_epochs = staged_epochs
        self._snapshot_failure = staged_snapshot_failure
        for product, frame, delta, processed_at in deferred_snapshot_requests:
            self._queue_snapshot_delta(repository, product, frame, delta, processed_at)
        return frames

    def _prepare_descriptor_events(
        self,
        repository: OpsRepository,
        sealed: SealedPublicTransportV1,
        frames: tuple[CapturedPublicFrameV2, ...],
        *,
        now_ns: int,
    ) -> Generator[PublicEvidencePreparationRequestV2,
                    PublicEvidencePreparationResultV2, dict[int, _PreparedFrameParseV2]]:
        if self.plan is None or self._run_root is None or self._run_id is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_NOT_RECOVERED")
        worker = self._preparation_worker
        if worker is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_WORKER_UNAVAILABLE")
        descriptor_hash = sealed.batch.artifact_ref
        plan_hash = self.plan.plan_id
        descriptor_products = {
            self.plan.key_for_frame(frame) for frame in frames
        }
        product_hashes = tuple(sorted(
            {self._products[key].content_hash for key in descriptor_products if key in self._products}
        ))
        if len(product_hashes) != len(descriptor_products):
            raise ValueError("BROAD_PUBLIC_DESCRIPTOR_PRODUCT_BINDING_INCOMPLETE")
        stored_cursor, stored_stages = repository.public_adoption_stages_v2(
            self._run_id, descriptor_hash,
        )
        if stored_cursor is not None and (
            stored_cursor.extent_hash != sealed.extent.content_hash
            or stored_cursor.batch_hash != sealed.batch.content_hash
            or stored_cursor.plan_hash != plan_hash
            or stored_cursor.product_hashes != product_hashes
            or stored_cursor.preparation_version != PREPARATION_VERSION_V2
        ):
            raise ValueError("BROAD_PUBLIC_PRIVATE_CURSOR_BINDING_MISMATCH")

        prepared_events: dict[int, list[Any]] = {index: [] for index in range(len(frames))}
        trade_hashes: dict[int, list[str]] = {index: [] for index in range(len(frames))}
        processed_at_by_frame: dict[int, int] = {}
        stored_next = (0, 0)
        for stage in stored_stages:
            output = stage.payload["output"]
            request = PublicEvidencePreparationRequestV2.from_dict(dict(output["request"]))
            completion = PublicEvidencePreparationResultV2.from_dict(dict(output["completion"]))
            if (request.frame_ordinal, request.trade_ordinal_start) != stored_next:
                raise ValueError("BROAD_PUBLIC_PRIVATE_STAGE_SEQUENCE_GAP")
            manifest = read_preparation_input(request)
            with self._measure_phase("result_read_rehydrate"):
                body = read_preparation_result(request, completion)
                frame_results = self._validate_prepared_parse_body(
                    request, body, sealed=sealed, plan_hash=plan_hash,
                    frames=frames, manifest=manifest,
                )
                decoded = self._decode_and_bind_manifest_events(frame_results, frames)
            first_result = frame_results[0]
            next_frame, next_trade = self._next_manifest_cursor(frame_results)
            cursor = stage.cursor
            if (cursor.frame_ordinal != request.frame_ordinal
                    or cursor.trade_ordinal_start != request.trade_ordinal_start
                    or cursor.trade_ordinal_end != first_result["trade_ordinal_end"]
                    or (cursor.next_frame_ordinal, cursor.next_trade_ordinal) != (next_frame, next_trade)):
                raise ValueError("BROAD_PUBLIC_PRIVATE_CURSOR_RESULT_MISMATCH")
            stored_next = (next_frame, next_trade)
            for frame_ordinal, events, hashes, processed_at in decoded:
                prepared_events[frame_ordinal].extend(events)
                trade_hashes[frame_ordinal].extend(hashes)
                processed_at_by_frame[frame_ordinal] = processed_at

        staged_new_count = 0
        if stored_cursor is None:
            frame_ordinal, trade_ordinal = 0, 0
            generation = 0
        else:
            frame_ordinal = stored_cursor.next_frame_ordinal
            trade_ordinal = stored_cursor.next_trade_ordinal
            generation = stored_cursor.cursor_generation
            if stored_next != (frame_ordinal, trade_ordinal):
                raise ValueError("BROAD_PUBLIC_PRIVATE_STAGE_SEQUENCE_GAP")
        if frame_ordinal > len(frames) or (frame_ordinal == len(frames) and trade_ordinal != 0):
            raise ValueError("BROAD_PUBLIC_PRIVATE_CURSOR_ORDINAL_OUT_OF_RANGE")

        private_root = self._run_root / ".public-evidence-preparation" / descriptor_hash
        private_root.mkdir(parents=True, exist_ok=True)
        while frame_ordinal < len(frames):
            manifest_frames: list[dict[str, Any]] = []
            estimated_input_bytes = 0
            for ordinal in range(frame_ordinal, min(len(frames), frame_ordinal + MAX_PREPARATION_FRAMES_V2)):
                frame = frames[ordinal]
                key = self.plan.key_for_frame(frame)
                if key not in self._products:
                    raise ValueError("BROAD_STREAM_FRAME_PRODUCT_REVISION_UNBOUND")
                next_estimate = estimated_input_bytes + 4 * ((len(frame.raw_payload_bytes) + 2) // 3) + 8192
                if manifest_frames and next_estimate > MAX_PREPARATION_INPUT_BYTES_V2:
                    break
                if next_estimate > MAX_PREPARATION_INPUT_BYTES_V2:
                    raise ValueError("BROAD_PREPARATION_FRAME_EXCEEDS_INPUT_BOUND")
                start_ordinal = trade_ordinal if ordinal == frame_ordinal else 0
                processed_at = processed_at_by_frame.get(
                    ordinal, max(now_ns, frame.available_at_ns),
                )
                manifest_frames.append({
                    "frame_ordinal": ordinal,
                    "frame": {
                        "venue": frame.venue.value,
                        "source_id": frame.source_id,
                        "channel": frame.channel,
                        "raw_payload_b64": base64.b64encode(frame.raw_payload_bytes).decode("ascii"),
                        "raw_payload_hash": frame.raw_payload_hash,
                        "received_at_ns": frame.received_at_ns,
                        "available_at_ns": frame.available_at_ns,
                        "connection_epoch": frame.connection_epoch,
                    },
                    "instrument": key.to_dict(),
                    "processed_at_ns": processed_at,
                    "source_health": "UNKNOWN",
                    "source_health_ref": None,
                    "trade_ordinal_start": start_ordinal,
                })
                estimated_input_bytes = next_estimate
            if not manifest_frames:
                raise RuntimeError("BROAD_PREPARATION_EMPTY_MANIFEST")
            first_manifest = manifest_frames[0]
            first_frame = frames[frame_ordinal]
            key = self.plan.key_for_frame(first_frame)
            processed_at = first_manifest["processed_at_ns"]
            manifest = {"frames": manifest_frames}
            input_path = private_root / f"slice-{frame_ordinal:04d}-{trade_ordinal:08d}.input.json"
            with self._measure_phase("input_write"):
                input_hash, _input_bytes = write_preparation_input(input_path, manifest)
            is_bybit_trades = first_frame.venue.value == "BYBIT" and first_frame.channel.startswith("publicTrade.")
            requested_end = trade_ordinal + MAX_PREPARATION_TRADE_ROWS_V2 if is_bybit_trades else 1
            job_identity = sha256_json({
                "version": PREPARATION_VERSION_V2,
                "run_id": self._run_id,
                "descriptor_hash": descriptor_hash,
                "frame_ordinal": frame_ordinal,
                "trade_ordinal_start": trade_ordinal,
                "trade_ordinal_end": requested_end,
                "input_hash": input_hash,
                "plan_hash": plan_hash,
                "product_hash": key.content_hash,
            })
            output_path = private_root / f"result-{job_identity}.json"
            request = PublicEvidencePreparationRequestV2(
                "PARSE", job_identity, self._run_id, str(self._run_root), descriptor_hash,
                input_hash, str(input_path), str(output_path), plan_hash, key.content_hash,
                frame_ordinal, trade_ordinal, requested_end, processed_at_ns=processed_at,
            )
            result = yield request
            with self._measure_phase("result_read_rehydrate"):
                body = read_preparation_result(request, result)
                frame_results = self._validate_prepared_parse_body(
                    request, body, sealed=sealed, plan_hash=plan_hash,
                    frames=frames, manifest=manifest,
                )
                decoded = self._decode_and_bind_manifest_events(frame_results, frames)
            first_result = frame_results[0]
            actual_end = first_result["trade_ordinal_end"]
            next_frame, next_trade = self._next_manifest_cursor(frame_results)
            for ordinal, events, hashes, logical_time in decoded:
                prepared_events[ordinal].extend(events)
                trade_hashes[ordinal].extend(hashes)
                processed_at_by_frame[ordinal] = logical_time
            generation += 1
            if (next_frame, next_trade) <= (frame_ordinal, trade_ordinal):
                raise RuntimeError("BROAD_PREPARATION_MANIFEST_MADE_NO_PROGRESS")
            stage_output = {"request": request.to_dict(), "completion": result.__dict__}
            cursor = PublicAdoptionCursorV2(
                run_id=self._run_id,
                descriptor_hash=descriptor_hash,
                extent_hash=sealed.extent.content_hash,
                batch_hash=sealed.batch.content_hash,
                plan_hash=plan_hash,
                product_hashes=product_hashes,
                preparation_version=PREPARATION_VERSION_V2,
                frame_ordinal=frame_ordinal,
                trade_ordinal_start=trade_ordinal,
                trade_ordinal_end=actual_end,
                next_frame_ordinal=next_frame,
                next_trade_ordinal=next_trade,
                logical_started_at_ns=processed_at,
                logical_finished_at_ns=max(processed_at, result.completed_at_ns),
                staged_output_hash=sha256_json(stage_output),
                cursor_generation=generation,
            )
            with self._measure_phase("stage_commit"):
                repository.stage_public_adoption_slice_v2(
                    cursor, {"cursor": cursor.to_dict(), "output": stage_output},
                )
            staged_new_count += 1
            frame_ordinal, trade_ordinal = next_frame, next_trade

        if len(stored_stages) + staged_new_count == 0:
            raise RuntimeError("BROAD_PREPARATION_DESCRIPTOR_HAS_NO_STAGED_SLICES")
        return {
            ordinal: _PreparedFrameParseV2(tuple(prepared_events[ordinal]), tuple(trade_hashes[ordinal]))
            for ordinal in range(len(frames))
        }

    def _validate_prepared_parse_body(
        self,
        request: PublicEvidencePreparationRequestV2,
        body: Mapping[str, Any],
        *,
        sealed: SealedPublicTransportV1,
        plan_hash: str,
        frames: tuple[CapturedPublicFrameV2, ...],
        manifest: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], ...]:
        """Validate every bounded worker result against its durable raw frame."""
        if (self.plan is None or request.kind != "PARSE"
                or request.descriptor_hash != sealed.batch.artifact_ref
                or request.plan_hash != plan_hash or request.frame_ordinal >= len(frames)
                or not isinstance(manifest, Mapping) or set(manifest) != {"frames"}):
            raise ValueError("BROAD_PREPARATION_RESULT_BINDING_INVALID")
        manifest_frames = manifest["frames"]
        results = body.get("frame_results")
        body_fields = {
            "request_hash", "version", "job_id", "descriptor_hash", "input_hash", "plan_hash",
            "frame_ordinal", "trade_ordinal_start", "trade_ordinal_end", "trade_total",
            "frame_results", "result_records",
        }
        if (not isinstance(manifest_frames, list)
                or not 1 <= len(manifest_frames) <= MAX_PREPARATION_FRAMES_V2
                or not isinstance(results, list) or not 1 <= len(results) <= len(manifest_frames)
                or set(body) != body_fields
                or body.get("request_hash") != request.request_hash
                or body.get("version") != PREPARATION_VERSION_V2
                or body.get("job_id") != request.job_id
                or body.get("descriptor_hash") != sealed.batch.artifact_ref
                or body.get("input_hash") != request.input_hash
                or body.get("plan_hash") != request.plan_hash):
            raise ValueError("BROAD_PREPARATION_RESULT_BINDING_INVALID")
        first_frame = frames[request.frame_ordinal]
        first_key = self.plan.key_for_frame(first_frame)
        first_is_bybit = first_frame.venue.value == "BYBIT" and first_frame.channel.startswith("publicTrade.")
        canonical_start = request.trade_ordinal_start if first_is_bybit else 0
        canonical_end = canonical_start + MAX_PREPARATION_TRADE_ROWS_V2 if first_is_bybit else 1
        if (request.product_hash != first_key.content_hash
                or request.trade_ordinal_start != canonical_start
                or request.trade_ordinal_end != canonical_end
                or not isinstance(manifest_frames[0], dict)):
            raise ValueError("BROAD_PREPARATION_REQUEST_CURSOR_INVALID")
        first_input = manifest_frames[0]
        if (first_input.get("frame_ordinal") != request.frame_ordinal
                or first_input.get("trade_ordinal_start") != request.trade_ordinal_start
                or first_input.get("instrument") != first_key.to_dict()
                or first_input.get("processed_at_ns") != request.processed_at_ns):
            raise ValueError("BROAD_PREPARATION_MANIFEST_START_INVALID")

        budget_left = MAX_PREPARATION_TRADE_ROWS_V2
        event_total = 0
        validated: list[Mapping[str, Any]] = []
        expected_result_fields = {
            "frame_ordinal", "venue", "source_id", "channel", "raw_payload_hash",
            "received_at_ns", "available_at_ns", "connection_epoch", "product_hash",
            "source_health", "source_health_ref", "processed_at_ns", "trade_ordinal_start",
            "trade_ordinal_requested_end", "trade_ordinal_end", "trade_total", "events",
            "trade_payload_hashes",
        }
        expected_manifest_fields = {
            "frame_ordinal", "frame", "instrument", "processed_at_ns", "source_health",
            "source_health_ref", "trade_ordinal_start",
        }
        for offset, result in enumerate(results):
            manifest_item = manifest_frames[offset]
            ordinal = request.frame_ordinal + offset
            if (ordinal >= len(frames) or not isinstance(manifest_item, dict)
                    or set(manifest_item) != expected_manifest_fields
                    or type(manifest_item.get("frame_ordinal")) is not int
                    or manifest_item["frame_ordinal"] != ordinal
                    or type(manifest_item.get("processed_at_ns")) is not int
                    or manifest_item["processed_at_ns"] < frames[ordinal].available_at_ns
                    or manifest_item.get("trade_ordinal_start") != (
                        request.trade_ordinal_start if offset == 0 else 0)):
                raise ValueError("BROAD_PREPARATION_MANIFEST_SEQUENCE_INVALID")
            frame = frames[ordinal]
            key = self.plan.key_for_frame(frame)
            if key not in self._products or manifest_item.get("instrument") != key.to_dict():
                raise ValueError("BROAD_PREPARATION_PRODUCT_REVISION_MISMATCH")
            raw_binding = {
                "venue": frame.venue.value,
                "source_id": frame.source_id,
                "channel": frame.channel,
                "raw_payload_b64": base64.b64encode(frame.raw_payload_bytes).decode("ascii"),
                "raw_payload_hash": frame.raw_payload_hash,
                "received_at_ns": frame.received_at_ns,
                "available_at_ns": frame.available_at_ns,
                "connection_epoch": frame.connection_epoch,
            }
            if manifest_item.get("frame") != raw_binding:
                raise ValueError("BROAD_PREPARATION_RAW_FRAME_BINDING_MISMATCH")
            source_health = manifest_item.get("source_health")
            source_health_ref = manifest_item.get("source_health_ref")
            if source_health != "UNKNOWN" or source_health_ref is not None:
                raise ValueError("BROAD_PREPARATION_HEALTH_INPUT_INVALID")
            is_bybit = frame.venue.value == "BYBIT" and frame.channel.startswith("publicTrade.")
            start = manifest_item["trade_ordinal_start"]
            events = result.get("events") if isinstance(result, dict) else None
            hashes = result.get("trade_payload_hashes") if isinstance(result, dict) else None
            end = result.get("trade_ordinal_end") if isinstance(result, dict) else None
            total = result.get("trade_total") if isinstance(result, dict) else None
            requested_end = result.get("trade_ordinal_requested_end") if isinstance(result, dict) else None
            # The PARSE worker is the sole JSON parser. Its result is digest-bound
            # to the exact raw input; keep this validation to typed counts/cursors
            # and event/source/product identity instead of scanning raw JSON again.
            expected_request_end = min(start + budget_left, total) if is_bybit and type(total) is int else 1
            if (not isinstance(result, dict) or set(result) != expected_result_fields
                    or any(type(result.get(name)) is not int for name in (
                        "frame_ordinal", "received_at_ns", "available_at_ns", "processed_at_ns",
                        "trade_ordinal_start", "trade_ordinal_requested_end", "trade_ordinal_end",
                        "trade_total",
                    ))
                    or any(result.get(name) != expected for name, expected in (
                        ("frame_ordinal", ordinal), ("venue", frame.venue.value),
                        ("source_id", frame.source_id), ("channel", frame.channel),
                        ("raw_payload_hash", frame.raw_payload_hash),
                        ("received_at_ns", frame.received_at_ns),
                        ("available_at_ns", frame.available_at_ns),
                        ("connection_epoch", frame.connection_epoch),
                        ("product_hash", key.content_hash),
                        ("source_health", source_health), ("source_health_ref", source_health_ref),
                        ("processed_at_ns", manifest_item["processed_at_ns"]),
                        ("trade_ordinal_start", start),
                    ))
                    or total < 0
                    or not isinstance(events, list) or not isinstance(hashes, list)
                    or len(events) > MAX_PREPARATION_TRADE_ROWS_V2
                    or len(hashes) > MAX_PREPARATION_TRADE_ROWS_V2
                    or requested_end != expected_request_end or end < start or end > requested_end
                    or total < end):
                raise ValueError("BROAD_PREPARATION_FRAME_RESULT_BINDING_INVALID")
            if is_bybit:
                count = end - start
                empty_completed = start == 0 and end == total == 0 and not events and not hashes
                if (len(events) != count or len(hashes) != count
                        or (count == 0 and not empty_completed)):
                    raise ValueError("BROAD_PREPARATION_TRADE_SLICE_INVALID")
                budget_left -= count
            else:
                expected_hash_count = 1 if frame.venue.value == "BINANCE" and frame.channel.endswith("@aggTrade") else 0
                if (start != 0 or end != 1 or total != 1 or len(events) != 1
                        or len(hashes) != expected_hash_count or expected_hash_count > budget_left):
                    raise ValueError("BROAD_PREPARATION_SINGLE_FRAME_RESULT_INVALID")
                budget_left -= expected_hash_count
            for digest in hashes:
                if (not isinstance(digest, str) or len(digest) != 64
                        or any(character not in "0123456789abcdef" for character in digest)):
                    raise ValueError("BROAD_PREPARATION_TRADE_HASH_INVALID")
            event_total += len(events)
            if event_total > 512:
                raise ValueError("BROAD_PREPARATION_RESULT_EXCEEDS_RECORD_BOUND")
            validated.append(result)
            if is_bybit and end < total:
                if offset != len(results) - 1:
                    raise ValueError("BROAD_PREPARATION_MANIFEST_CONTINUES_PARTIAL_TRADE_FRAME")
                break
            if is_bybit and budget_left == 0 and offset != len(results) - 1:
                raise ValueError("BROAD_PREPARATION_MANIFEST_EXCEEDS_TRADE_BUDGET")
        if (len(validated) != len(results) or type(body.get("result_records")) is not int
                or body["result_records"] != event_total):
            raise ValueError("BROAD_PREPARATION_RESULT_RECORD_COUNT_INVALID")
        first_result = validated[0]
        if any(body.get(name) != first_result[name] for name in (
            "frame_ordinal", "trade_ordinal_start", "trade_ordinal_end", "trade_total",
        )):
            raise ValueError("BROAD_PREPARATION_RESULT_START_SUMMARY_INVALID")
        return tuple(validated)

    @staticmethod
    def _next_manifest_cursor(frame_results: tuple[Mapping[str, Any], ...]) -> tuple[int, int]:
        if not frame_results:
            raise ValueError("BROAD_PREPARATION_EMPTY_RESULT_MANIFEST")
        last = frame_results[-1]
        is_bybit = last["venue"] == "BYBIT" and last["channel"].startswith("publicTrade.")
        if is_bybit and last["trade_ordinal_end"] < last["trade_total"]:
            return last["frame_ordinal"], last["trade_ordinal_end"]
        return last["frame_ordinal"] + 1, 0

    def _decode_and_bind_manifest_events(
        self,
        frame_results: tuple[Mapping[str, Any], ...],
        frames: tuple[CapturedPublicFrameV2, ...],
    ) -> tuple[tuple[int, tuple[Any, ...], tuple[str, ...], int], ...]:
        if self.plan is None:
            raise RuntimeError("BROAD_PUBLIC_PREPARATION_PLAN_UNAVAILABLE")
        decoded: list[tuple[int, tuple[Any, ...], tuple[str, ...], int]] = []
        for result in frame_results:
            ordinal = result["frame_ordinal"]
            frame = frames[ordinal]
            key = self.plan.key_for_frame(frame)
            events = decode_prepared_events({"events": result["events"]})
            for event in events:
                if (event.instrument != key or event.source_id != frame.source_id
                        or event.channel != frame.channel
                        or event.raw_content_ref != frame.raw_payload_hash
                        or event.received_at_ns != frame.received_at_ns
                        or event.available_at_ns != max(frame.available_at_ns, result["processed_at_ns"])
                        or event.source_health != result["source_health"]
                        or event.source_health_ref != result["source_health_ref"]):
                    raise ValueError("BROAD_PREPARATION_DECODED_EVENT_BINDING_INVALID")
            is_bybit_trade = frame.venue.value == "BYBIT" and frame.channel.startswith("publicTrade.")
            hashes = tuple(result["trade_payload_hashes"])
            if is_bybit_trade and len(hashes) != len(events):
                raise ValueError("BROAD_PREPARATION_DECODED_TRADE_HASH_COUNT_INVALID")
            decoded.append((ordinal, events, hashes, result["processed_at_ns"]))
        return tuple(decoded)

    def _seal_prepared_archive(
        self,
        sealed: SealedPublicTransportV1,
        archive_groups: tuple[tuple[L2RawFrameV2, ...], ...],
    ) -> Generator[PublicEvidencePreparationRequestV2,
                    PublicEvidencePreparationResultV2,
                    tuple[PreparedL2ArchiveBatchV2, tuple[ArtifactIndexEntryV2, ...]]]:
        if self._run_root is None or self.plan is None or self._preparation_worker is None:
            raise RuntimeError("BROAD_PUBLIC_ARCHIVE_PREPARATION_NOT_RECOVERED")
        if not archive_groups:
            return PreparedL2ArchiveBatchV2(()), ()
        archive_frames: list[L2RawFrameV2] = []
        group_ordinals: list[list[int]] = []
        for group in archive_groups:
            if not group:
                raise ValueError("BROAD_ARCHIVE_PREPARATION_EMPTY_GROUP")
            ordinals = []
            for frame in group:
                if frame.transport_ordinal is None:
                    raise ValueError("BROAD_ARCHIVE_TRANSPORT_ORDINAL_MISSING")
                ordinal = len(archive_frames)
                archive_frames.append(replace(frame, archive_ordinal=ordinal))
                ordinals.append(ordinal)
            group_ordinals.append(ordinals)
        if len(archive_frames) > MAX_PREPARATION_OUTPUT_RECORDS_V2:
            raise ValueError("BROAD_ARCHIVE_SEAL_BATCH_EXCEEDS_BOUND")
        private_root = self._run_root / ".public-evidence-preparation" / sealed.batch.artifact_ref
        private_root.mkdir(parents=True, exist_ok=True)
        input_path = private_root / f"seal-manifest-{sealed.batch.artifact_ref}.json"
        input_value = {
            "version": "PUBLIC_ARCHIVE_PREPARE_SEAL_V1",
            "transport_batch_ref": sealed.batch.artifact_ref,
            "transport_batch": dict(sealed.batch.metadata["batch"]),
            "transport_extent": {
                "artifact_ref": sealed.extent.artifact_ref,
                "artifact_type": sealed.extent.artifact_type,
                "content_hash": sealed.extent.content_hash,
                "created_at_ns": sealed.extent.created_at_ns,
                "available_at_ns": sealed.extent.available_at_ns,
                "metadata": dict(sealed.extent.metadata),
            },
            "records": [{"archive_ordinal": frame.archive_ordinal,
                         "transport_ordinal": frame.transport_ordinal,
                         "metadata": frame.metadata_dict()} for frame in archive_frames],
            "groups": group_ordinals,
        }
        with (self._measure_phase("input_write"), self._measure_phase("seal_input_write")):
            input_hash, _input_bytes = write_preparation_input(input_path, input_value)
        batch_id = sha256_json({"version": "PUBLIC_ARCHIVE_PREPARE_SEAL_V1",
                                "descriptor_hash": sealed.batch.artifact_ref,
                                "input_hash": input_hash})
        output_path = private_root / f"seal-result-{batch_id}.json"
        key = archive_frames[0].instrument
        floor_ns = max(frame.available_at_ns for frame in archive_frames)
        clock_at_ns = max(self.clock_ns(), floor_ns)
        job_id = sha256_json({"kind": "SEAL", "descriptor": sealed.batch.artifact_ref,
                             "chunk_id": batch_id, "input_hash": input_hash,
                             "plan_hash": self.plan.plan_id})
        request = PublicEvidencePreparationRequestV2(
            "SEAL", job_id, self._run_id or self._run_identity(self._run_root),
            str(self._run_root), sealed.batch.artifact_ref, input_hash,
            str(input_path), str(output_path), self.plan.plan_id, key.content_hash,
            0, 0, 0, namespace="ops-l2-frames", chunk_id=batch_id,
            floor_ns=floor_ns, clock_at_ns=clock_at_ns,
        )
        completion = yield request
        with self._measure_phase("result_read_rehydrate"):
            with self._measure_phase("seal_result_read"):
                body = read_preparation_result(request, completion)
            chunk_bindings = body.get("archive_chunks")
            extents = body.get("extents")
            if (body.get("version") != PREPARATION_VERSION_V2
                    or body.get("job_id") != request.job_id
                    or body.get("descriptor_hash") != sealed.batch.artifact_ref
                    or body.get("input_hash") != input_hash
                    or body.get("plan_hash") != self.plan.plan_id
                    or body.get("product_hash") != key.content_hash
                    or body.get("chunk_id") != batch_id
                    or body.get("frame_ordinal") != 0
                    or body.get("trade_ordinal_start") != 0 or body.get("trade_ordinal_end") != 0
                    or body.get("result_records") != len(archive_frames)
                    or not isinstance(chunk_bindings, list) or not chunk_bindings
                    or len(chunk_bindings) > MAX_PREPARATION_SEAL_CHUNKS_V2
                    or not isinstance(extents, list) or len(extents) != len(chunk_bindings)
                    or not isinstance(body.get("seal_metrics"), Mapping)):
                raise ValueError("BROAD_ARCHIVE_SEAL_RESULT_BINDING_INVALID")
            metrics = body["seal_metrics"]
            for source_name, phase_name in (
                ("prepare_ns", "archive_prepare_chunks"),
                ("arrow_encode_ns", "archive_arrow_encode"),
                ("compression_ns", "archive_compression"),
                ("write_ns", "archive_extent_write"),
                ("fsync_ns", "archive_extent_fsync"),
                ("rename_ns", "archive_extent_rename"),
                ("directory_sync_ns", "archive_directory_sync"),
            ):
                duration = metrics.get(source_name)
                if type(duration) is not int or duration < 0:
                    raise ValueError("BROAD_ARCHIVE_SEAL_METRICS_INVALID")
                self._record_phase_duration_ns(phase_name, duration)
            from ..data.public_archive_extents import EXTENT_TYPE, extent_ref

            prepared_chunks = []
            sealed_entries: list[ArtifactIndexEntryV2] = []
            seen_archive_ordinals: list[int] = []
            for binding, extent in zip(chunk_bindings, extents, strict=True):
                if (not isinstance(binding, Mapping)
                        or set(binding) != {"chunk_id", "archive_ordinals"}):
                    raise ValueError("BROAD_ARCHIVE_SEAL_CHUNK_BINDING_INVALID")
                chunk_id = str(binding["chunk_id"])
                ordinals = binding["archive_ordinals"]
                if (not isinstance(ordinals, list) or not 1 <= len(ordinals) <= 512
                        or any(type(ordinal) is not int or not 0 <= ordinal < len(archive_frames)
                               for ordinal in ordinals)):
                    raise ValueError("BROAD_ARCHIVE_SEAL_ORDINAL_BINDING_INVALID")
                frames = tuple(archive_frames[ordinal] for ordinal in ordinals)
                ordered = tuple(sorted(frames, key=lambda frame: (
                    frame.available_at_ns, frame.source_id, frame.channel,
                    frame.last_update_id if frame.last_update_id is not None else -1,
                    frame.raw_payload_hash,
                )))
                expected_id = sha256_json({"archive_type": "L2RawFrameChunkV2",
                                           "frames": [frame.metadata_dict() for frame in ordered]})
                if expected_id != chunk_id:
                    raise ValueError("BROAD_ARCHIVE_SEAL_CHUNK_IDENTITY_INVALID")
                seen_archive_ordinals.extend(ordinals)
                prepared_chunks.append(PreparedL2ArchiveChunkV2(chunk_id, ordered, None))
                if not isinstance(extent, Mapping):
                    raise ValueError("BROAD_ARCHIVE_SEAL_EXTENT_INVALID")
                metadata = extent.get("metadata")
                extent_metadata = metadata.get("extent") if isinstance(metadata, Mapping) else None
                if (extent.get("artifact_ref") != extent_ref("ops-l2-frames", chunk_id)
                        or extent.get("artifact_type") != EXTENT_TYPE
                        or not isinstance(extent_metadata, Mapping)
                        or extent_metadata.get("namespace") != "ops-l2-frames"
                        or extent_metadata.get("chunk_id") != chunk_id):
                    raise ValueError("BROAD_ARCHIVE_SEAL_EXTENT_BINDING_INVALID")
                sealed_entries.append(ArtifactIndexEntryV2(
                    str(extent["artifact_ref"]), str(extent["artifact_type"]),
                    str(extent["content_hash"]), int(extent["created_at_ns"]),
                    int(extent["available_at_ns"]), metadata,
                ))
            if sorted(seen_archive_ordinals) != list(range(len(archive_frames))):
                raise ValueError("BROAD_ARCHIVE_SEAL_ORDINAL_POPULATION_INVALID")
        return PreparedL2ArchiveBatchV2(tuple(prepared_chunks)), tuple(sealed_entries)

    def _candidate_state_for_frames(self, frames: tuple[CapturedPublicFrameV2, ...]) -> tuple[
        dict[InstrumentKeyV2, SequenceValidBookV2],
        dict[tuple[InstrumentKeyV2, str], PublicStreamContinuityTrackerV1],
        dict[tuple[InstrumentKeyV2, str], int | None],
    ]:
        if self.plan is None:
            raise RuntimeError("BROAD_PUBLIC_INTERPRETER_NOT_RECOVERED")
        books = self._sequence_books.copy()
        trackers = self._trackers.copy()
        epochs = self._connection_epochs.copy()
        for frame in frames:
            key = self.plan.key_for_frame(frame)
            if key in self._sequence_books:
                books[key] = copy.deepcopy(self._sequence_books[key])
            identity = key, frame.channel
            if identity in self._trackers:
                trackers[identity] = copy.deepcopy(self._trackers[identity])
        return books, trackers, epochs

    def _interpret_frames(
        self,
        repository: OpsRepository,
        frames: tuple[CapturedPublicFrameV2, ...],
        *,
        now_ns: int,
        prepared_events: Mapping[int, _PreparedFrameParseV2] | None = None,
        defer_publication: bool = False,
    ) -> _PreparedFramePublicationV2 | None:
        if prepared_events is None:
            prepared_events = self._prepared_events_for_call
        defer_publication = defer_publication or self._defer_publication_for_call
        if self.plan is None or self._archive is None:
            raise RuntimeError("BROAD_PUBLIC_INTERPRETER_NOT_RECOVERED")
        grouped: dict[tuple[str, str, str], list[L2RawFrameV2]] = {}
        artifact_entries: list[ArtifactIndexEntryV2] = []
        if not defer_publication:
            self._poll_snapshot(repository, now_ns=now_ns, grouped=grouped)
        # One immutable lane-health view is sufficient for every frame in this
        # sealed FIFO extent. Re-reading all venue lane locks for every frame
        # made broad batches pay O(frames × lanes) status work on the sole
        # repository writer.
        lane_statuses = self.source.status().lanes if self.source is not None else {}
        for frame_ordinal, frame in enumerate(frames):
            key = self.plan.key_for_frame(frame)
            product = self._products.get(key)
            if product is None:
                raise ValueError("BROAD_STREAM_FRAME_PRODUCT_REVISION_UNBOUND")
            processed_at = max(now_ns, frame.available_at_ns)
            health = self._frame_health(repository, frame, now_ns=processed_at,
                                        lane_statuses=lane_statuses, artifact_sink=artifact_entries)
            if prepared_events is None:
                events = self.plan.parse_frame(
                    frame, processed_at_ns=processed_at, source_health=health.state.value,
                    source_health_ref=health.content_hash,
                )
                prepared_frame = None
            else:
                prepared_frame = prepared_events.get(frame_ordinal)
                if prepared_frame is None:
                    raise ValueError("BROAD_PREPARATION_FRAME_ORDINAL_MISSING")
                events = tuple(replace(
                    event, source_health=health.state.value, source_health_ref=health.content_hash,
                ) for event in prepared_frame.events)
            identity = (key, frame.channel)
            tracker = self._trackers.get(identity)
            previous_epoch = self._connection_epochs.get(identity)
            if identity in self._connection_epochs and previous_epoch != frame.connection_epoch:
                if tracker is not None:
                    observation = PublicStreamObservationV1.transport(
                        instrument=key, source_id=frame.source_id, channel=frame.channel,
                        metadata_ref=product.metadata_ref,
                        epoch_id=f"{self._stream_epoch}:{key.venue.value}:{frame.channel}:{frame.connection_epoch}",
                        kind=PublicStreamObservationKindV1.RECONNECT,
                        observed_at_ns=frame.received_at_ns, available_at_ns=processed_at,
                    )
                    self._persist_tracker(repository, tracker, observation, artifact_sink=artifact_entries)
                book = self._sequence_books.get(key)
                if book is not None:
                    book.reconnect(processed_at)
                if self._snapshot_binding is not None and self._snapshot_binding[1] == key:
                    self._snapshot_failure = "BINANCE_SNAPSHOT_CONNECTION_EPOCH_CHANGED"
            self._connection_epochs[identity] = frame.connection_epoch
            if tracker is None:
                tracker = PublicStreamContinuityTrackerV1(
                    instrument=key, source_id=frame.source_id, channel=frame.channel,
                    metadata_ref=product.metadata_ref,
                    epoch_id=f"{self._stream_epoch}:{key.venue.value}:{frame.channel}:{frame.connection_epoch}",
                )
                self._trackers[identity] = tracker
            channel_book = frame.channel.startswith("orderbook.") or "@depth" in frame.channel
            if channel_book:
                book = self._sequence_books.get(key)
                if book is None:
                    semantics = "BYBIT_U" if key.venue.value == "BYBIT" else "BINANCE_U_PU"
                    book = SequenceValidBookV2(instrument=key, source_id=frame.source_id,
                                               channel=frame.channel, sequence_semantics=semantics)
                    self._sequence_books[key] = book
                if (key.venue.value == "BINANCE"
                        and book.sequence_state.state not in {BookStateV2.VALID, BookStateV2.WARMING}
                        and events and isinstance(events[0], L2DeltaV2)):
                    if self.snapshot_reader is None:
                        raise RuntimeError("BINANCE_REST_SNAPSHOT_PROVIDER_UNAVAILABLE")
                    for event in events:
                        if not isinstance(event, L2DeltaV2):
                            continue
                        if self._deferred_snapshot_requests is None:
                            self._queue_snapshot_delta(repository, product, frame, event, processed_at)
                        else:
                            self._deferred_snapshot_requests.append((product, frame, event, processed_at))
                        raw = raw_archive_record(
                            frame, instrument=key, frame_type="DELTA_AWAITING_REST_SNAPSHOT",
                            sequence_semantics="BINANCE_U_PU", first_update_id=event.first_update_id,
                            last_update_id=event.last_update_id, previous_update_id=event.previous_update_id,
                            event_at_ns=event.event_at_ns, transport_ordinal=frame_ordinal,
                        )
                        grouped.setdefault((key.content_hash, frame.source_id, frame.channel), []).append(raw)
                    continue
                for event in events:
                    if isinstance(event, L2SnapshotV2):
                        book.apply_snapshot(event)
                    elif isinstance(event, L2DeltaV2):
                        book.apply_delta(event)
                    elif isinstance(event, L2SequenceFaultV2):
                        book.apply_fault(event)
                    else:
                        raise ValueError("book channel parser returned a non-book event")
                    observation = PublicStreamObservationV1.from_book_event(
                        event, metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                        persisted_at_ns=processed_at,
                    )
                    self._persist_tracker(repository, tracker, observation, artifact_sink=artifact_entries)
                    raw = raw_archive_record(
                        frame, instrument=key,
                        frame_type=("SEQUENCE_FAULT" if isinstance(event, L2SequenceFaultV2) else
                                    "SNAPSHOT" if isinstance(event, L2SnapshotV2) else "DELTA"),
                        sequence_semantics=book.sequence_semantics,
                        first_update_id=getattr(event, "first_update_id", None),
                        last_update_id=getattr(event, "last_update_id", None),
                        previous_update_id=getattr(event, "previous_update_id", None),
                        event_at_ns=event.event_at_ns, transport_ordinal=frame_ordinal,
                    )
                    grouped.setdefault((key.content_hash, frame.source_id, frame.channel), []).append(raw)
            else:
                observation = PublicStreamObservationV1.from_frame(
                    frame, instrument=key, metadata_ref=product.metadata_ref,
                    epoch_id=tracker.state.epoch_id, persisted_at_ns=processed_at,
                    source_health_ref=health.content_hash, source_health_epoch_id=tracker.state.epoch_id,
                )
                self._persist_tracker(repository, tracker, observation, artifact_sink=artifact_entries)
                trade_events = tuple(trade for event in events for trade in (event if isinstance(event, tuple) else (event,)))
                if prepared_frame is None:
                    payload = json.loads(frame.raw_payload_bytes)
                    raw_rows = payload.get("data", payload)
                    if isinstance(raw_rows, Mapping):
                        raw_rows = (raw_rows,)
                    if not isinstance(raw_rows, (tuple, list)) or len(raw_rows) != len(trade_events):
                        raise ValueError("BROAD_TRADE_EXACT_ROW_BINDING_FAILED")
                    raw_hashes = tuple(sha256_json(row) for row in raw_rows if isinstance(row, Mapping))
                else:
                    raw_hashes = prepared_frame.trade_payload_hashes
                if len(raw_hashes) != len(trade_events):
                    raise ValueError("BROAD_TRADE_EXACT_ROW_BINDING_FAILED")
                for trade, trade_payload_hash in zip(trade_events, raw_hashes, strict=True):
                    trade_observation = PublicStreamObservationV1(
                        trade.instrument, trade.source_id, trade.channel, product.metadata_ref,
                        tracker.state.epoch_id, PublicStreamObservationKindV1.TRADE_OBSERVED,
                        trade.received_at_ns, max(trade.available_at_ns, processed_at),
                        trade.received_at_ns, trade.event_at_ns, trade.raw_content_ref,
                        trade.trade_id, trade_payload_hash, "CANONICALIZED_PER_TRADE_RAW_ROW_BYTES",
                        source_health_ref=trade.source_health_ref,
                        source_health_epoch_id=tracker.state.epoch_id,
                    )
                    self._persist_tracker(repository, tracker, trade_observation,
                                          artifact_sink=artifact_entries)
                event_at = max((getattr(event, "event_at_ns", 0) or 0 for event in trade_events), default=0) or None
                raw = raw_archive_record(
                    frame, instrument=key, frame_type=f"TRADE_FRAME_{frame.raw_payload_hash[:16]}",
                    sequence_semantics=("BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR"
                                        if key.venue.value == "BYBIT" else
                                        "BINANCE_AGG_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR"),
                    event_at_ns=event_at, transport_ordinal=frame_ordinal,
                )
                grouped.setdefault((key.content_hash, frame.source_id, frame.channel), []).append(raw)
        lane_statuses = self.source.status().lanes if self.source is not None else {}
        for lane_name, lane_status in lane_statuses.items():
            handoff = lane_status.handoff
            healthy = self._lane_is_current(lane_status, now_ns=now_ns)
            source_id = f"{lane_name}_PUBLIC_WS_BROAD_V2"
            state = PublicSourceStateV2.HEALTHY_CURRENT if healthy else PublicSourceStateV2.INCOMPLETE_SNAPSHOT
            observed = max((frame.received_at_ns for frame in frames if frame.source_id == source_id), default=now_ns)
            body = {"lane": lane_name, "state": state.value, "plan_id": self.plan.plan_id,
                    "observed_at_ns": observed, "queue_items": handoff.queue_items,
                    "overflowed": handoff.overflowed, "backpressure": handoff.backpressure,
                    "disconnect_count": handoff.disconnect_count}
            health = PublicSourceHealthV2(source_id, observed, max(observed, now_ns), state,
                                          sha256_json(body), "bounded broad stream lane health")
            artifact_entries.append(ArtifactIndexEntryV2(
                health.content_hash, "PublicSourceHealthV2", health.content_hash,
                health.available_at_ns, health.available_at_ns, {"health": health.to_dict(), "lane": body},
            ))
        if defer_publication:
            return _PreparedFramePublicationV2(
                tuple(artifact_entries), tuple(tuple(group) for group in grouped.values()),
            )
        if artifact_entries:
            repository.register_artifacts(tuple(artifact_entries))
        if grouped:
            self._archive.write_chunks(tuple(tuple(group) for group in grouped.values()))
        return None

    def _queue_snapshot_delta(self, repository: OpsRepository, product: ProductContractV2,
                               frame: CapturedPublicFrameV2, delta: L2DeltaV2, now_ns: int) -> None:
        binding = (self.plan.plan_id if self.plan else "", product.key, product.metadata_ref, frame.connection_epoch)
        if self._snapshot_binding is None:
            if time.monotonic_ns() < self._snapshot_next_retry_monotonic_ns:
                return
            if self._snapshot_executor is None:
                self._snapshot_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="atlas-binance-depth-snapshot",
                )
            self._snapshot_binding = binding
            self._snapshot_started_monotonic_ns = time.monotonic_ns()
            self._snapshot_failure = None
            snapshot_reader = self.snapshot_reader
            assert snapshot_reader is not None
            self._snapshot_future = self._snapshot_executor.submit(snapshot_reader, product.key, now_ns)
        if self._snapshot_binding != binding:
            # The worker is globally one-flight, so another instrument simply
            # waits its turn. A changed epoch for the bound key invalidates it.
            if self._snapshot_binding is not None and self._snapshot_binding[1] == product.key:
                self._snapshot_failure = "SNAPSHOT_BINDING_CHANGED_WHILE_REQUEST_PENDING"
            return
        if self._snapshot_failure is not None:
            return
        if (len(self._snapshot_buffer) >= 256
                or self._snapshot_buffer_bytes + len(frame.raw_payload_bytes) > 16_000_000):
            self._snapshot_failure = "BINANCE_SNAPSHOT_BRIDGE_BUFFER_OVERFLOW"
            return
        self._snapshot_buffer.append((product, frame, delta))
        self._snapshot_buffer_bytes += len(frame.raw_payload_bytes)

    @staticmethod
    def _lane_is_current(lane: Any, *, now_ns: int) -> bool:
        handoff = getattr(lane, "handoff", None)
        return bool(lane is not None and lane.state == "RUNNING" and handoff is not None
                    and handoff.connected and not handoff.overflowed and not handoff.backpressure
                    and handoff.last_activity_at_ns is not None
                    and 0 <= now_ns - handoff.last_activity_at_ns <= 5_000_000_000)

    def _frame_health(self, repository: OpsRepository, frame: CapturedPublicFrameV2,
                      *, now_ns: int, lane_statuses: Mapping[str, Any] | None = None,
                      artifact_sink: list[ArtifactIndexEntryV2] | None = None) -> PublicSourceHealthV2:
        if lane_statuses is None:
            lane_statuses = self.source.status().lanes if self.source is not None else {}
        lane_name = ("BYBIT" if frame.venue.value == "BYBIT" else
                     "BINANCE_DEPTH" if "@depth" in frame.channel else "BINANCE_MARKET")
        lane = lane_statuses.get(lane_name)
        handoff = getattr(lane, "handoff", None)
        connected = bool(
            lane is not None and self._lane_is_current(lane, now_ns=now_ns)
            and frame.connection_epoch is not None and frame.connection_epoch == lane.attempt_count
            and 0 <= now_ns - frame.received_at_ns <= 5_000_000_000
        )
        source_id = frame.source_id
        state = PublicSourceStateV2.HEALTHY_CURRENT if connected else PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        body = {"source_id": source_id, "venue": frame.venue.value, "channel": frame.channel,
                "connection_epoch": frame.connection_epoch,
                "active_attempt": lane.attempt_count if lane is not None else None,
                "observed_at_ns": frame.received_at_ns, "available_at_ns": now_ns,
                "state": state.value, "overflowed": bool(getattr(handoff, "overflowed", True)),
                "backpressure": bool(getattr(handoff, "backpressure", True))}
        health = PublicSourceHealthV2(source_id, frame.received_at_ns, max(now_ns, frame.received_at_ns),
            state, sha256_json(body), "connection epoch and bounded stream handoff health")
        entry = ArtifactIndexEntryV2(
            health.content_hash, "PublicSourceHealthV2", health.content_hash,
            health.available_at_ns, health.available_at_ns, {"health": health.to_dict(), "frame": body},
        )
        if artifact_sink is None:
            repository.register_artifact(entry)
        else:
            artifact_sink.append(entry)
        return health

    def _poll_snapshot(self, repository: OpsRepository, *, now_ns: int,
                       grouped: dict[tuple[str, str, str], list[L2RawFrameV2]]) -> None:
        future = self._snapshot_future
        if (future is not None and self._snapshot_started_monotonic_ns is not None
                and time.monotonic_ns() - self._snapshot_started_monotonic_ns >= 5_000_000_000):
            self._snapshot_failure = "BINANCE_REST_SNAPSHOT_WORKER_TIMEOUT"
        if future is None or not future.done():
            return
        binding = self._snapshot_binding
        buffered = tuple(self._snapshot_buffer)
        self._snapshot_future = None
        self._snapshot_binding = None
        self._snapshot_started_monotonic_ns = None
        self._snapshot_buffer.clear()
        self._snapshot_buffer_bytes = 0
        try:
            raw_body, received_at = future.result()
            if self._snapshot_failure is not None:
                raise ValueError(self._snapshot_failure)
            if type(received_at) is not int or not 0 <= now_ns - received_at <= 5_000_000_000:
                raise ValueError("BINANCE_SNAPSHOT_RECEIPT_STALE_OR_FUTURE")
            if not isinstance(raw_body, bytes) or not 0 < len(raw_body) <= 2_000_000:
                raise ValueError("BINANCE_SNAPSHOT_PAYLOAD_BOUND_INVALID")
            if binding is None or self.plan is None or binding[0] != self.plan.plan_id:
                raise ValueError("BINANCE_SNAPSHOT_PLAN_EPOCH_MISMATCH")
            if not buffered:
                raise ValueError("BINANCE_SNAPSHOT_BRIDGE_HAS_NO_BUFFERED_DELTAS")
            product, frame, _ = buffered[0]
            # Seal the exact REST response before it can mutate a sequence
            # book. The network receipt and controller availability differ.
            rest_record = L2RawFrameV2(
                product.key, "BINANCE_USDM_PUBLIC_V2", "USD-M depth snapshot REST", "REST_SNAPSHOT",
                raw_body, hashlib.sha256(raw_body).hexdigest(), None, received_at,
                now_ns, None, None, None, "BINANCE_U_PU", "UNKNOWN", "ACTUAL_SYSTEM",
            )
            if self._archive is not None:
                self._archive.write_chunk((rest_record,))
            else:
                grouped.setdefault((product.key.content_hash, rest_record.source_id, rest_record.channel), []).append(rest_record)
            if binding[1:] != (product.key, product.metadata_ref, frame.connection_epoch):
                raise ValueError("BINANCE_SNAPSHOT_REVISION_OR_CONNECTION_EPOCH_MISMATCH")
            if self._products.get(product.key) != product or product.trading_status.value != "TRADING":
                raise ValueError("BINANCE_SNAPSHOT_PRODUCT_NO_LONGER_CURRENT")
            if self.source is None:
                raise ValueError("BINANCE_SNAPSHOT_LANE_UNAVAILABLE")
            lane = self.source.status().lanes.get("BINANCE_DEPTH")
            if lane is None or not self._lane_is_current(lane, now_ns=now_ns) or lane.attempt_count != frame.connection_epoch:
                raise ValueError("BINANCE_SNAPSHOT_ACTIVE_CONNECTION_EPOCH_MISMATCH")
            snapshot_health = PublicSourceHealthV2(
                "BINANCE_USDM_PUBLIC_V2", received_at, max(now_ns, received_at),
                PublicSourceStateV2.HEALTHY_CURRENT,
                sha256_json({"snapshot_hash": hashlib.sha256(raw_body).hexdigest(),
                             "instrument": product.key.to_dict(), "received_at_ns": received_at}),
                "actual bounded Binance REST depth snapshot received for sequence bridge",
            )
            repository.register_artifact(ArtifactIndexEntryV2(
                snapshot_health.content_hash, "PublicSourceHealthV2", snapshot_health.content_hash,
                snapshot_health.available_at_ns, snapshot_health.available_at_ns,
                {"health": snapshot_health.to_dict(), "raw_payload_hash": hashlib.sha256(raw_body).hexdigest()},
            ))
            snapshot = parse_binance_rest_snapshot(
                raw_body, instrument=product.key,
                source_id="BINANCE_USDM_PUBLIC_V2", channel="USD-M depth snapshot REST",
                received_at_ns=received_at, available_at_ns=now_ns,
                declared_depth=1000, source_health="HEALTHY_CURRENT",
                source_health_ref=snapshot_health.content_hash,
                processed_at_ns=max(now_ns, received_at),
            )
            book = self._sequence_books[product.key]
            # Transport receipts stay exact. Interpretation of a buffered
            # bridge becomes available when this controller adopts the REST
            # response, never at its earlier network receipt.
            prior_deltas = tuple(row[2] for row in buffered if row[2].received_at_ns <= received_at)
            book.apply_snapshot(snapshot, buffered_deltas=prior_deltas)
            for _, _, delta in buffered:
                if delta.received_at_ns > received_at:
                    book.apply_delta(replace(delta, available_at_ns=max(now_ns, delta.available_at_ns)))
            if book.sequence_state.state not in {BookStateV2.WARMING, BookStateV2.VALID}:
                raise ValueError(f"BINANCE_SNAPSHOT_SEQUENCE_BRIDGE_FAILED:{book.sequence_state.reason}")
            stream_identity = (product.key, frame.channel)
            tracker = self._trackers[stream_identity]
            for product, frame, delta in buffered:
                observation = PublicStreamObservationV1.from_book_event(
                    delta, metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                    persisted_at_ns=max(now_ns, frame.available_at_ns),
                )
                self._persist_tracker(repository, tracker, observation)
            self._snapshot_failure = None
        except Exception as exc:
            self._snapshot_failure = f"{type(exc).__name__}:{exc}"
            self._snapshot_next_retry_monotonic_ns = time.monotonic_ns() + 1_000_000_000

    @staticmethod
    def _persist_tracker(repository: OpsRepository, tracker: PublicStreamContinuityTrackerV1,
                         observation: PublicStreamObservationV1,
                         *, artifact_sink: list[ArtifactIndexEntryV2] | None = None) -> None:
        transition = tracker.apply_deferred(observation)
        # Replay and trade-ID caches are bounded operational accelerators. They
        # are not source evidence, and serializing the growing caches into every
        # accepted trade row made broad capture O(batch × cache-size). Persist
        # an explicit cache-free checkpoint projection while retaining its
        # completeness bit; restart still requires exact durable lookup when
        # the bounded cache is incomplete.
        scalar_state = replace(
            transition.state,
            trade_identity_cache=(),
            observation_replay_cache=(),
            trade_identity_cache_complete=(transition.state.observed_trade_count == 0),
        )
        state_body = scalar_state.to_dict()
        state_ref = sha256_json({
            "artifact_type": "PublicStreamContinuityStateV1", "state": state_body,
        })
        observation_body = observation.to_dict()
        observation_ref = sha256_json({
            "artifact_type": "PublicStreamObservationV1", "observation": observation_body,
        })
        decision = PublicStreamContinuityDecisionV1(
            observation_ref, transition.classification, transition.reason_code, state_ref,
        )
        body = {"version": "BROAD_PUBLIC_STREAM_CONTINUITY_V3",
                "observation": observation_body, "decision": decision.to_dict(),
                "state_projection": state_body, "authority": "ZERO"}
        ref = sha256_json({"artifact_type": "BroadPublicStreamContinuityV3", "body": body})
        entry = ArtifactIndexEntryV2(
            ref, "BroadPublicStreamContinuityV3", ref, observation.available_at_ns,
            observation.available_at_ns, body,
        )
        if artifact_sink is None:
            repository.register_artifact(entry)
        else:
            artifact_sink.append(entry)
