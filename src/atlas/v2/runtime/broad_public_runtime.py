"""Supervisor-owned lifecycle for dynamically planned broad public streams."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .._serialization import sha256_json
from ..data.broad_stream_source import (
    BroadDurablePublicCaptureV2,
    BroadPublicStreamPlanV2,
    BroadPublicStreamSourceV2,
)
from ..data.durable_public_capture import MAX_PENDING_CAPTURE_BATCHES, SealedPublicTransportV1
from ..data.health import PublicSourceHealthV2, PublicSourceStateV2
from ..data.microstructure import (
    BookStateV2,
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from ..data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
from ..data.public_microstructure_ws import (
    CapturedPublicFrameV2,
    parse_binance_rest_snapshot,
    raw_archive_record,
)
from ..data.public_stream_continuity import (
    PublicStreamContinuityTrackerV1,
    PublicStreamObservationKindV1,
    PublicStreamObservationV1,
)
from ..instruments import InstrumentKeyV2, ProductContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository


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
        self._capture_epoch = "0" * 64
        self._archive: L2FrameArchiveV2 | None = None
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

    @property
    def sequence_books(self) -> Mapping[InstrumentKeyV2, SequenceValidBookV2]:
        return dict(self._sequence_books)

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
            self._archive = L2FrameArchiveV2(run_root.parent / "ops-l2-frames", repository,
                                             compact_live=True, clock_ns=self.clock_ns)
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
                # The immutable archive extent and descriptor become durable
                # before any frame is parsed or routed to stateful consumers.
                # One repository composition then publishes the exact raw
                # index and all derived rows together. This avoids a FULL
                # synchronous commit for every small per-frame health/trade
                # record while keeping raw bytes durable before SQL effects.
                with repository.atomic_composition():
                    frames = sealed.adopt(repository)
                    if self.interpret_frames is not None:
                        self.interpret_frames(repository, self.plan, frames, now_ns)
                    else:
                        self._interpret_frames(repository, frames, now_ns=now_ns)
                self._service_frames += len(frames)
                if time.monotonic_ns() - started >= 50_000_000:
                    break
        except Exception as exc:
            self._terminal_error = f"{type(exc).__name__}:{exc}"
            try:
                self.capture.close()
            finally:
                raise
        finally:
            self._service_calls += 1
            self._last_service_at_ns = now_ns
            self._last_service_duration_ns = time.monotonic_ns() - started
            self._max_service_duration_ns = max(self._max_service_duration_ns, self._last_service_duration_ns)

    def finish(self, repository: OpsRepository) -> None:
        if self.capture is None:
            return
        self.capture.close()
        if self._terminal_error is not None:
            raise RuntimeError("BROAD_PUBLIC_CAPTURE_NOT_CLEAN")
        deadline = time.monotonic() + 5.0
        for _ in range(MAX_PENDING_CAPTURE_BATCHES):
            if not self.capture.status().pending_frames:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("BROAD_PUBLIC_CAPTURE_FINAL_BACKLOG_DEADLINE_EXCEEDED")
            self.service(repository, now_ns=self.clock_ns())
        if self.capture.status().pending_frames:
            raise RuntimeError("BROAD_PUBLIC_CAPTURE_FINAL_BACKLOG_EXCEEDED")
        self.capture.mark_controller_capture_clean(repository)

    def close(self) -> None:
        if self.capture is not None:
            self.capture.close()
        if self._snapshot_executor is not None:
            self._snapshot_executor.shutdown(wait=False, cancel_futures=True)

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
            "evidence_integrity_failure": self._terminal_error is not None,
        }

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
            return SimpleNamespace(state="FAILED" if self._terminal_error else "CREATED", attempt_count=0,
                                   handoff=handoff, capture=capture, pending_frames=0,
                                   plan_id=None, lanes={})
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
        capture["terminal_error"] = self._terminal_error or capture.get("terminal_error")
        return SimpleNamespace(
            state=source_status.state, attempt_count=source_status.attempt_count,
            handoff=source_status.handoff, capture=capture, pending_frames=source_status.pending_frames,
            plan_id=self.plan.plan_id if self.plan is not None else None,
            lanes=source_status.lanes, service_calls=self._service_calls, service_frames=self._service_frames,
            last_service_at_ns=self._last_service_at_ns,
            last_service_duration_ns=self._last_service_duration_ns,
            max_service_gap_ns=self._max_service_gap_ns,
            max_service_duration_ns=self._max_service_duration_ns,
            authority="ZERO",
        )

    def _interpret_frames(self, repository: OpsRepository, frames: tuple[CapturedPublicFrameV2, ...], *, now_ns: int) -> None:
        if self.plan is None or self._archive is None:
            raise RuntimeError("BROAD_PUBLIC_INTERPRETER_NOT_RECOVERED")
        grouped: dict[tuple[str, str, str], list[L2RawFrameV2]] = {}
        artifact_entries: list[ArtifactIndexEntryV2] = []
        self._poll_snapshot(repository, now_ns=now_ns, grouped=grouped)
        # One immutable lane-health view is sufficient for every frame in this
        # sealed FIFO extent. Re-reading all venue lane locks for every frame
        # made broad batches pay O(frames × lanes) status work on the sole
        # repository writer.
        lane_statuses = self.source.status().lanes if self.source is not None else {}
        for frame in frames:
            key = self.plan.key_for_frame(frame)
            product = self._products.get(key)
            if product is None:
                raise ValueError("BROAD_STREAM_FRAME_PRODUCT_REVISION_UNBOUND")
            processed_at = max(now_ns, frame.available_at_ns)
            health = self._frame_health(repository, frame, now_ns=processed_at,
                                        lane_statuses=lane_statuses, artifact_sink=artifact_entries)
            events = self.plan.parse_frame(
                frame, processed_at_ns=processed_at, source_health=health.state.value,
                source_health_ref=health.content_hash,
            )
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
                        self._queue_snapshot_delta(repository, product, frame, event, processed_at)
                        raw = raw_archive_record(
                            frame, instrument=key, frame_type="DELTA_AWAITING_REST_SNAPSHOT",
                            sequence_semantics="BINANCE_U_PU", first_update_id=event.first_update_id,
                            last_update_id=event.last_update_id, previous_update_id=event.previous_update_id,
                            event_at_ns=event.event_at_ns,
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
                        event_at_ns=event.event_at_ns,
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
                payload = json.loads(frame.raw_payload_bytes)
                raw_rows = payload.get("data", payload)
                if isinstance(raw_rows, Mapping):
                    raw_rows = (raw_rows,)
                if not isinstance(raw_rows, (tuple, list)) or len(raw_rows) != len(trade_events):
                    raise ValueError("BROAD_TRADE_EXACT_ROW_BINDING_FAILED")
                for trade, raw_row in zip(trade_events, raw_rows, strict=True):
                    if not isinstance(raw_row, Mapping):
                        raise ValueError("BROAD_TRADE_EXACT_ROW_BINDING_FAILED")
                    trade_payload_hash = sha256_json(raw_row)
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
                    event_at_ns=event_at,
                )
                grouped.setdefault((key.content_hash, frame.source_id, frame.channel), []).append(raw)
        if artifact_entries:
            repository.register_artifacts(tuple(artifact_entries))
        if grouped:
            self._archive.write_chunks(tuple(tuple(group) for group in grouped.values()))
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
            repository.register_artifact(ArtifactIndexEntryV2(
                health.content_hash, "PublicSourceHealthV2", health.content_hash,
                health.available_at_ns, health.available_at_ns, {"health": health.to_dict(), "lane": body},
            ))

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
        state_ref = scalar_state.content_hash
        decision = transition.seal(state_ref=state_ref)
        body = {"version": "BROAD_PUBLIC_STREAM_CONTINUITY_V3",
                "observation": observation.to_dict(), "decision": decision.to_dict(),
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
