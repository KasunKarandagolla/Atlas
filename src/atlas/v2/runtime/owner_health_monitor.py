"""Controller-owned watchdog with no SQLite access or execution authority."""
from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..data.health import PublicSourceStateV2
from .live_health import LiveHealthControllerV1, LiveHealthFactsV1


class OwnerHealthMonitorV1:
    """One bounded observation slot, independent of controller commits.

    Inputs are scalar callbacks/status snapshots, never a SQL connection. The
    sole watchdog publishes the lifetime latch and a read-only UI projection.
    """

    def __init__(self, run: Path, *, run_id: str, config_hash: str, source: Any,
                 progress: Callable[[], dict[str, Any]], persistence: Callable[[], dict[str, Any]],
                 resources: Callable[[], dict[str, Any]], report: Callable[[], dict[str, Any]],
                 publish: Callable[[Path, dict[str, Any]], None]) -> None:
        self.run, self.run_id, self.config_hash, self.source = run, run_id, config_hash, source
        self.progress, self.persistence, self.resources, self.report, self.publish = (
            progress, persistence, resources, report, publish)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._publisher: threading.Thread | None = None
        self._publication = threading.Condition()
        self._pending_projection: dict[str, Any] | None = None
        self._publication_closed = False
        self._host_snapshot: dict[str, Any] = {}
        self.latest: dict[str, Any] | None = None
        self.error_type: str | None = None
        self.publication_error_type: str | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("owner watchdog already started")
        self._publisher = threading.Thread(target=self._publish_projection,
            name="atlas-owner-health-projection", daemon=True)
        self._publisher.start()
        self._thread = threading.Thread(target=self._run, name="atlas-owner-health", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                raise TimeoutError("owner watchdog shutdown exceeded its bound")
        with self._publication:
            self._publication_closed = True
            self._publication.notify_all()
        if self._publisher is not None:
            self._publisher.join(timeout=2)
            if self._publisher.is_alive():
                raise TimeoutError("owner health projection shutdown exceeded its bound")

    def _publish_projection(self) -> None:
        # Only the replaceable UI projection is coalesced. The immutable first
        # qualification failure is still published by the watchdog controller.
        # This thread has no repository, DB connection or source authority.
        while True:
            with self._publication:
                while self._pending_projection is None and not self._publication_closed:
                    self._publication.wait()
                if self._pending_projection is None:
                    return
                body = self._pending_projection
                self._pending_projection = None
            try:
                # Host/path/report observation can itself encounter slow file
                # metadata I/O. It must not stop the queue-pressure watchdog.
                resources, report = self.resources(), self.report()
                self._host_snapshot = {"resources": resources, "report": report,
                                       "observed_at_ns": time.time_ns()}
                self.publish(self.run / "live-health.json", body)
                self.publication_error_type = None
            except Exception as error:
                self.publication_error_type = type(error).__name__

    def _schedule_projection(self, body: dict[str, Any]) -> None:
        with self._publication:
            self._pending_projection = body
            self._publication.notify()

    def _run(self) -> None:
        controller = LiveHealthControllerV1(self.run, run_id=self.run_id, config_hash=self.config_hash)
        previous: dict[str, Any] | None = None
        last_publish = float("-inf")
        last_failed = False
        storage_samples: deque[tuple[int, int | None, int | None, int | None]] = deque(maxlen=61)
        last_storage_sample_ns = 0
        last_capture_progress = time.monotonic()
        last_captured_count = 0
        pressure_ticks = 0
        while True:
            try:
                source = self.source.status()
                handoff = source.handoff
                capture = getattr(source, "capture", {})
                progress, persistence = self.progress(), self.persistence()
                host = self._host_snapshot
                resources, report = host.get("resources", {}), host.get("report", {})
                host_at = host.get("observed_at_ns")
                # Timestamp after the atomic snapshots. A concurrently updated
                # controller/host observation is not a future clock conflict.
                now = time.time_ns()
                elapsed = (now - previous["at"]) / 1e9 if previous is not None else None
                received = handoff.frames_received
                queue = handoff.queue_items
                arrival = (max(0, received - previous["received"]) / elapsed
                           if previous is not None and elapsed and elapsed > 0 else None)
                growth = ((queue - previous["queue"]) / elapsed
                          if previous is not None and elapsed and elapsed > 0 else None)
                captured = capture.get("captured_frames", received - queue)
                if captured != last_captured_count:
                    last_capture_progress = time.monotonic()
                    last_captured_count = captured
                drain = (max(0, captured - previous["captured"]) / elapsed
                         if previous is not None and elapsed and elapsed > 0 else None)
                # The warning starts at half capacity. At three quarters, only
                # 0.4s of the declared 320fps burst envelope remains. Stop new
                # arrivals independently of a blocked controller/storage call.
                headroom = ((handoff.max_queue_items - queue) / growth
                            if growth is not None and growth > 0 else None)
                pressure_ticks = pressure_ticks + 1 if growth is not None and growth > 0 else 0
                capture_stalled = time.monotonic() - last_capture_progress > controller.policy.service_warning_seconds
                if capture_stalled and (queue >= handoff.max_queue_items * .75
                        or handoff.queue_bytes >= handoff.max_queue_bytes * .75
                        or pressure_ticks >= 3 and queue > 0 and headroom is not None
                        and headroom <= controller.policy.service_warning_seconds / 2):
                    stop_capture = getattr(self.source, "request_pressure_stop", None)
                    if callable(stop_capture):
                        stop_capture()
                        capture = getattr(self.source.status(), "capture", capture)
                footprint = resources.get("current_footprint_bytes")
                free, wal_bytes = resources.get("disk_free_bytes"), resources.get("wal_bytes")
                if (footprint is not None and free is not None and wal_bytes is not None
                        and now - last_storage_sample_ns >= 1_000_000_000):
                    storage_samples.append((now, footprint, free, wal_bytes))
                    last_storage_sample_ns = now
                # Volume-free-space growth conservatively includes reports,
                # receipt journals and unrelated host writes. A >=10s bounded
                # window avoids extrapolating a single checkpoint/write burst.
                disk_growth = wal_growth = None
                if storage_samples and now - storage_samples[0][0] >= 10_000_000_000:
                    first_at, first_bytes, first_free, first_wal = storage_samples[0]
                    span = (now - first_at) / 1e9
                    growths = [max(0, footprint - first_bytes) / span
                               if footprint is not None and first_bytes is not None else 0,
                               max(0, first_free - free) / span if first_free is not None and free is not None else 0]
                    disk_growth = max(growths)
                    wal_growth = (max(0, wal_bytes - first_wal) / span
                                  if wal_bytes is not None and first_wal is not None else None)
                phase = persistence.get("active_phase", "IDLE")
                pending_seconds = ((time.monotonic_ns() - persistence["phase_started_monotonic_ns"]) / 1e9
                                   if phase != "IDLE" and "phase_started_monotonic_ns" in persistence else None)
                checkpoint_at = persistence.get("checkpoint_progress_at_ns")
                uncheckpointed = max(0, persistence.get("wal_frames", 0) - persistence.get("checkpointed_frames", 0))
                # A finished historical slow commit is not current pressure.
                recent_commit = (persistence.get("commit_duration_ns", 0) / 1e9
                    if now - persistence.get("transaction_observed_at_ns", 0) < 1_000_000_000 else None)
                facts = LiveHealthFactsV1(self.run_id, self.config_hash, now, now,
                    progress.get("observed_at_ns"), source.state, bool(handoff.connected),
                    PublicSourceStateV2(progress.get("stream_source_state", "INCOMPLETE_SNAPSHOT")),
                    bool(progress.get("stream_recovery_required", True)), queue, handoff.max_queue_items,
                    handoff.high_water_items, queue_overflowed=handoff.overflowed,
                    frames_rejected=handoff.frames_rejected, unresolved_gap=bool(progress.get("stream_unresolved_gap")),
                    queue_bytes=handoff.queue_bytes, queue_capacity_bytes=handoff.max_queue_bytes,
                    queue_high_water_bytes=handoff.high_water_bytes,
                    capture_failed=bool(capture.get("terminal_error") or progress.get("stream_ingestion_failed")),
                    capture_pressure_stop=capture.get("terminal_error") == "PREVENTIVE_CAPTURE_PRESSURE_STOP",
                    evidence_integrity_failure=bool(progress.get("evidence_integrity_failure")),
                    clock_integrity_failure=bool(progress.get("clock_integrity_failure")),
                    arrival_frames_per_second=arrival, drain_frames_per_second=drain,
                    queue_growth_frames_per_second=growth, persistence_seconds=max(
                        pending_seconds or 0, recent_commit or 0, capture.get("active_capture_duration_ns", 0) / 1e9),
                    service_gap_seconds=progress.get("stream_service_gap_seconds"),
                    service_duration_seconds=progress.get("stream_service_duration_seconds"),
                    capture_pending_batches=capture.get("pending_batches"),
                    capture_max_pending_batches=capture.get("max_pending_batches"),
                    free_disk_bytes=resources.get("disk_free_bytes"), current_footprint_bytes=footprint,
                    growth_bytes_per_second=disk_growth, disk_reserve_bytes=resources.get("disk_reserve_bytes"),
                    wal_bytes=wal_bytes, wal_growth_bytes_per_second=wal_growth,
                    wal_uncheckpointed_frames=uncheckpointed,
                    wal_last_progress_at_ns=checkpoint_at, report_state=report.get("state", "UNKNOWN"),
                    report_started_at_ns=report.get("started_at_ns"),
                    last_export_success_at_ns=report.get("completed_at_ns") if report.get("state") == "SUCCEEDED" else None,
                    export_failure_count=int(report.get("state") == "FAILED"),
                    evidence_validation_failures=report.get("validation_failures", 0),
                    host_observed_at_ns=host_at,
                    resource_pressure=bool(resources.get("resource_pressure")) or host_at is None,
                    rss_bytes=resources.get("rss_bytes"), thread_count=resources.get("threads"),
                    handle_count=resources.get("handles"))
                result = controller.observe(facts)
                body = {"version": "OWNER_LIVE_HEALTH_PROJECTION_V1", "facts": facts.to_dict(),
                        "assessment": result.to_dict(), "policy": controller.policy.to_dict(), "authority": "ZERO"}
                self.latest = body
                previous = {"at": now, "received": received, "captured": captured, "queue": queue,
                            "footprint": footprint}
                if time.monotonic() - last_publish >= 1 or result.qualification_failed != last_failed:
                    self._schedule_projection(body)
                    last_publish = time.monotonic()
                    last_failed = result.qualification_failed
            except Exception as error:
                self.error_type = type(error).__name__
                # Existing latch is never cleared. The UI also checks snapshot
                # age, so a stuck/failed watchdog cannot remain GREEN.
            if self._stop.is_set():
                break  # Publish/latch the final capture facts before exit.
            self._stop.wait(.1)
