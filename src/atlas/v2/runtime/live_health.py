"""Bounded S40 operational guidance, separate from scientific qualification.

Only the atlas-ops controller publishes the immutable run-failure latch. The
desktop reads it and assesses current typed facts; it never resets failures or
writes SQLite. GREEN describes current operational observations, not a passed
prospective/economic/source-completeness gate.
"""

from __future__ import annotations

import math
import os
import re
import stat
import threading
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Literal

from atlas.v2._serialization import canonical_json, sha256_json, sha256_ref, strict_fields, timestamp
from atlas.v2.data.health import PublicSourceStateV2

LATCH_FILENAME = "run-qualification-failure-v1.json"
LATCH_TEMP_FILENAME = ".run-qualification-failure-v1.tmp"
MAX_LATCH_BYTES = 32 * 1024
MAX_EVIDENCE_REFS = 16
NS = 1_000_000_000
MAX_MEASUREMENT = (1 << 63) - 1
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
ProducerState = Literal["CREATED", "RUNNING", "EXHAUSTED", "FAILED", "CLOSED"]
ReportState = Literal["IDLE", "RUNNING", "SUCCEEDED", "FAILED", "UNKNOWN"]


def _integer(value: object, name: str, *, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= MAX_MEASUREMENT:
        raise ValueError(f"{name} must be a bounded integer >= {minimum}")


def _optional_number(value: object, name: str) -> None:
    if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool)
                              or not 0 <= value <= MAX_MEASUREMENT or not math.isfinite(value)):
        raise ValueError(f"{name} must be a bounded finite nonnegative measurement or None")


def _identity(run_id: str, config_hash: str) -> None:
    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id):
        raise ValueError("invalid run_id")
    sha256_ref(config_hash, field="config_hash")


@dataclass(frozen=True)
class LiveHealthPolicyV1:
    """Versioned diagnostic envelope; no trading/risk authority.

    Half a 512-frame queue retains 0.8 seconds at the declared 320 fps burst.
    The same 2x margin over an empty queue yields a 0.8-second leading-warning
    service/headroom budget. These are warnings, not permanent disqualification.
    The controller's separately captured path may tolerate longer SQL stalls.
    Disk projection is a measured estimate with 2x future-growth reserve plus
    the complete current footprint; it is not a 48-hour capacity guarantee.
    """

    queue_capacity: int = 512
    normal_frames_per_second: float = 160.0
    burst_frames_per_second: float = 320.0
    safety_margin: float = 2.0
    heartbeat_warning_ns: int = 10 * NS
    controller_warning_ns: int = 3 * NS
    growth_projection_seconds: int = 48 * 3600
    wal_progress_warning_ns: int = 30 * NS
    report_warning_ns: int = 10 * NS
    # Frozen V2 desktop performance target, used only as operational guidance.
    # This measures ops RSS, not aggregate host memory or an OOM guarantee.
    rss_warning_bytes: int = 1_500_000_000

    def __post_init__(self) -> None:
        for name in ("queue_capacity", "heartbeat_warning_ns", "controller_warning_ns",
                     "growth_projection_seconds", "wal_progress_warning_ns", "report_warning_ns", "rss_warning_bytes"):
            _integer(getattr(self, name), name, minimum=1)
        for name in ("normal_frames_per_second", "burst_frames_per_second", "safety_margin"):
            _optional_number(getattr(self, name), name)
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.burst_frames_per_second < self.normal_frames_per_second or self.safety_margin < 2:
            raise ValueError("burst must cover normal traffic and safety margin must be >= 2")

    @property
    def service_warning_seconds(self) -> float:
        return self.queue_capacity / self.burst_frames_per_second / self.safety_margin

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, **asdict(self)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class LiveHealthFactsV1:
    """One bounded observation of current runtime components.

    Rates/growth/latencies must come from actual bounded telemetry windows.
    None means not measured. Lifetime high-water alone never traps a recovered
    harmless transient in AMBER; rejected/lost data is a separate sticky fact.
    """

    run_id: str
    config_hash: str
    observed_at_ns: int
    runtime_heartbeat_at_ns: int | None
    controller_heartbeat_at_ns: int | None
    producer_state: ProducerState
    connected: bool
    source_state: PublicSourceStateV2
    recovery_required: bool
    queue_items: int
    queue_capacity: int
    queue_high_water: int
    queue_overflowed: bool = False
    frames_rejected: int = 0
    unresolved_gap: bool = False
    capture_failed: bool = False
    evidence_integrity_failure: bool = False
    clock_integrity_failure: bool = False
    unclean_capture_stop: bool = False
    capture_pressure_stop: bool = False
    queue_bytes: int | None = None
    queue_capacity_bytes: int | None = None
    queue_high_water_bytes: int | None = None
    arrival_frames_per_second: float | None = None
    drain_frames_per_second: float | None = None
    queue_growth_frames_per_second: float | None = None
    service_gap_seconds: float | None = None
    service_duration_seconds: float | None = None
    persistence_seconds: float | None = None
    capture_pending_batches: int | None = None
    capture_max_pending_batches: int | None = None
    free_disk_bytes: int | None = None
    current_footprint_bytes: int | None = None
    growth_bytes_per_second: float | None = None
    disk_reserve_bytes: int | None = None
    database_integrity_failed: bool = False
    wal_bytes: int | None = None
    wal_growth_bytes_per_second: float | None = None
    wal_uncheckpointed_frames: int | None = None
    wal_last_progress_at_ns: int | None = None
    report_state: ReportState = "UNKNOWN"
    report_started_at_ns: int | None = None
    last_export_success_at_ns: int | None = None
    last_export_failure_at_ns: int | None = None
    export_failure_count: int = 0
    evidence_validation_failures: int = 0
    resource_pressure: bool = False
    host_observed_at_ns: int | None = None
    rss_bytes: int | None = None
    thread_count: int | None = None
    handle_count: int | None = None
    active_workers: int | None = None
    max_active_workers: int | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identity(self.run_id, self.config_hash)
        timestamp(self.observed_at_ns, field="observed_at_ns")
        for name in ("runtime_heartbeat_at_ns", "controller_heartbeat_at_ns", "wal_last_progress_at_ns",
                     "report_started_at_ns", "last_export_success_at_ns", "last_export_failure_at_ns", "host_observed_at_ns"):
            if getattr(self, name) is not None:
                timestamp(getattr(self, name), field=name)
        if self.producer_state not in ("CREATED", "RUNNING", "EXHAUSTED", "FAILED", "CLOSED"):
            raise ValueError("unknown producer state")
        if self.report_state not in ("IDLE", "RUNNING", "SUCCEEDED", "FAILED", "UNKNOWN"):
            raise ValueError("unknown report state")
        object.__setattr__(self, "source_state", PublicSourceStateV2(self.source_state))
        for name in ("connected", "recovery_required", "queue_overflowed", "unresolved_gap", "capture_failed",
                     "evidence_integrity_failure", "clock_integrity_failure", "database_integrity_failed",
                     "resource_pressure", "unclean_capture_stop", "capture_pressure_stop"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be bool")
        for name in ("queue_items", "queue_high_water", "frames_rejected", "export_failure_count",
                     "evidence_validation_failures"):
            _integer(getattr(self, name), name)
        _integer(self.queue_capacity, "queue_capacity", minimum=1)
        if self.queue_items > self.queue_capacity or not self.queue_items <= self.queue_high_water <= self.queue_capacity:
            raise ValueError("queue measurements exceed the declared capacity/high-water")
        for name in ("capture_pending_batches", "capture_max_pending_batches", "free_disk_bytes",
                     "current_footprint_bytes", "disk_reserve_bytes", "wal_bytes", "wal_uncheckpointed_frames",
                     "rss_bytes", "thread_count", "handle_count", "active_workers", "max_active_workers",
                     "queue_bytes", "queue_capacity_bytes", "queue_high_water_bytes"):
            if getattr(self, name) is not None:
                _integer(getattr(self, name), name)
        if any(value is not None for value in (self.queue_bytes, self.queue_capacity_bytes, self.queue_high_water_bytes)):
            if (self.queue_bytes is None or self.queue_capacity_bytes is None or self.queue_high_water_bytes is None
                    or self.queue_capacity_bytes == 0
                    or not self.queue_bytes <= self.queue_high_water_bytes <= self.queue_capacity_bytes):
                raise ValueError("byte queue measurements require exact positive capacity/high-water")
        for name in ("arrival_frames_per_second", "drain_frames_per_second", "service_gap_seconds",
                     "service_duration_seconds", "persistence_seconds", "growth_bytes_per_second",
                     "wal_growth_bytes_per_second"):
            _optional_number(getattr(self, name), name)
        # Signed trends can show queue recovery; all other rates are magnitudes.
        if self.queue_growth_frames_per_second is not None and (
            type(self.queue_growth_frames_per_second) not in (int, float)
            or not -MAX_MEASUREMENT <= self.queue_growth_frames_per_second <= MAX_MEASUREMENT
            or not math.isfinite(self.queue_growth_frames_per_second)
        ):
            raise ValueError("queue growth must be a finite measured signed rate")
        for pending, capacity in ((self.capture_pending_batches, self.capture_max_pending_batches),):
            if ((pending is None) != (capacity is None)
                    or (pending is not None and (capacity is None or capacity == 0 or pending > capacity))):
                raise ValueError("measured occupancy requires a positive capacity and must fit it")
        if ((self.active_workers is None) != (self.max_active_workers is None)
                or (self.max_active_workers is not None and self.max_active_workers == 0)):
            raise ValueError("worker measurements require a positive declared capacity")
        if (not isinstance(self.evidence_refs, tuple) or len(self.evidence_refs) > MAX_EVIDENCE_REFS
                or len(set(self.evidence_refs)) != len(self.evidence_refs)):
            raise ValueError("evidence refs must be a bounded unique tuple")
        for ref in self.evidence_refs:
            sha256_ref(ref, field="evidence_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, **asdict(self), "source_state": self.source_state.value,
                "evidence_refs": list(self.evidence_refs)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> LiveHealthFactsV1:
        names = {field.name for field in fields(cls)}
        data = dict(strict_fields(value, expected=names | {"schema_version"},
                                 required=names | {"schema_version"}, name="LiveHealthFactsV1"))
        version = data.pop("schema_version")
        if type(version) is not int or version != 1:
            raise ValueError("unknown live health facts version")
        if not isinstance(data["evidence_refs"], (tuple, list)):
            raise ValueError("evidence refs must be an array")
        data["evidence_refs"] = tuple(data["evidence_refs"])
        return cls(**data)

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _irreversible_failure(facts: LiveHealthFactsV1) -> str | None:
    if facts.queue_overflowed:
        return "QUEUE_OVERFLOW_LOCAL_DATA_LOSS"
    if facts.frames_rejected:
        return "PUBLIC_STREAM_LOCAL_FRAME_REJECTION"
    if facts.capture_pressure_stop:
        return "PREVENTIVE_PUBLIC_CAPTURE_STOP"
    if facts.capture_failed:
        return "RAW_CAPTURE_TERMINAL_FAILURE"
    if facts.producer_state == "FAILED":
        return "PUBLIC_STREAM_TERMINAL_FAILURE"
    if facts.database_integrity_failed or facts.evidence_integrity_failure:
        return "EVIDENCE_INTEGRITY_FAILURE"
    if facts.clock_integrity_failure:
        return "EVIDENCE_CLOCK_INTEGRITY_FAILURE"
    if facts.unclean_capture_stop:
        return "UNCLEAN_PUBLIC_CAPTURE_STOP"
    return None


@dataclass(frozen=True)
class QualificationFailureV1:
    run_id: str
    config_hash: str
    failed_at_ns: int
    first_cause: str
    facts: LiveHealthFactsV1
    policy_hash: str

    def __post_init__(self) -> None:
        _identity(self.run_id, self.config_hash)
        timestamp(self.failed_at_ns, field="failed_at_ns")
        sha256_ref(self.policy_hash, field="policy_hash")
        if (self.facts.run_id != self.run_id or self.facts.config_hash != self.config_hash
                or self.failed_at_ns != self.facts.observed_at_ns
                or not isinstance(self.first_cause, str) or self.first_cause != _irreversible_failure(self.facts)):
            raise ValueError("qualification failure is not bound to its actual first observed failure")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "run_id": self.run_id, "config_hash": self.config_hash,
                "failed_at_ns": self.failed_at_ns, "first_cause": self.first_cause,
                "facts": self.facts.to_dict(), "facts_ref": self.facts.content_hash,
                "policy_hash": self.policy_hash, "authority": "ZERO"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


class QualificationLatchError(ValueError):
    """Closed reason code; never includes paths, credentials or exception text."""


class QualificationLatchV1:
    """Immutable single-controller publication; reads require no write authority.

    ``controller=True`` binds mutation to this process/thread. Atomically linking
    a complete fsynced temporary file prevents overwrite of a first failure.
    NTFS/local Unix hard-link semantics are checked by host preflight; failure
    to publish is fail-closed and must not be represented as a durable latch.
    """

    def __init__(self, run_directory: Path, *, run_id: str, config_hash: str, controller: bool = False) -> None:
        _identity(run_id, config_hash)
        self.path = Path(run_directory) / LATCH_FILENAME
        self._temporary = self.path.with_name(LATCH_TEMP_FILENAME)
        self.run_id, self.config_hash = run_id, config_hash
        self._owner = (os.getpid(), threading.get_ident()) if controller else None

    def read(self) -> QualificationFailureV1 | None:
        try:
            properties = self.path.lstat()
            if not stat.S_ISREG(properties.st_mode) or properties.st_size > MAX_LATCH_BYTES:
                raise QualificationLatchError("QUALIFICATION_LATCH_INVALID")
            with self.path.open("rb") as file:
                raw = file.read(MAX_LATCH_BYTES + 1)
        except FileNotFoundError:
            if self._temporary.exists() or self._temporary.is_symlink():
                raise QualificationLatchError("QUALIFICATION_LATCH_PUBLICATION_INCOMPLETE") from None
            return None
        except OSError as exc:
            raise QualificationLatchError("QUALIFICATION_LATCH_UNREADABLE") from exc
        if len(raw) > MAX_LATCH_BYTES:
            raise QualificationLatchError("QUALIFICATION_LATCH_INVALID")
        try:
            import json

            def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
                value: dict[str, Any] = {}
                for name, item in pairs:
                    if name in value:
                        raise ValueError("duplicate latch key")
                    value[name] = item
                return value

            envelope = json.loads(raw, object_pairs_hook=unique_object)
            strict_fields(envelope, expected={"failure", "content_hash"},
                          required={"failure", "content_hash"}, name="QualificationLatchV1")
            body = strict_fields(envelope["failure"], expected={"schema_version", "run_id", "config_hash",
                "failed_at_ns", "first_cause", "facts", "facts_ref", "policy_hash", "authority"},
                required={"schema_version", "run_id", "config_hash", "failed_at_ns", "first_cause", "facts",
                          "facts_ref", "policy_hash", "authority"}, name="QualificationFailureV1")
            if type(body["schema_version"]) is not int or body["schema_version"] != 1 or body["authority"] != "ZERO":
                raise ValueError("unexpected latch version/authority")
            facts = LiveHealthFactsV1.from_dict(body["facts"])
            failure = QualificationFailureV1(body["run_id"], body["config_hash"], body["failed_at_ns"],
                                             body["first_cause"], facts, body["policy_hash"])
            if facts.content_hash != body["facts_ref"] or failure.content_hash != envelope["content_hash"]:
                raise ValueError("invalid latch hash")
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as exc:
            raise QualificationLatchError("QUALIFICATION_LATCH_INVALID") from exc
        if failure.run_id != self.run_id or failure.config_hash != self.config_hash:
            raise QualificationLatchError("QUALIFICATION_LATCH_IDENTITY_MISMATCH")
        return failure

    def publish(self, failure: QualificationFailureV1) -> QualificationFailureV1:
        self._assert_controller()
        if failure.run_id != self.run_id or failure.config_hash != self.config_hash:
            raise QualificationLatchError("QUALIFICATION_LATCH_IDENTITY_MISMATCH")
        existing = self.read()
        if existing is not None:
            return existing
        payload = (canonical_json({"failure": failure.to_dict(), "content_hash": failure.content_hash}) + "\n").encode()
        if len(payload) > MAX_LATCH_BYTES:
            raise QualificationLatchError("QUALIFICATION_LATCH_INVALID")
        # One fixed exclusive temporary path bounds failed-publication state.
        # A crash before the atomic link leaves a visible, fail-closed marker;
        # reopening the run cannot silently forget its unpublished failure.
        temporary = self._temporary
        try:
            with temporary.open("xb") as file:
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())
            try:
                os.link(temporary, self.path)
            except FileExistsError:
                pass  # A complete first failure already exists; never replace it.
            if os.name != "nt":
                descriptor = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            result = self.read()
            if result is None:
                raise QualificationLatchError("QUALIFICATION_LATCH_PUBLICATION_FAILED")
            return result
        except OSError as exc:
            raise QualificationLatchError("QUALIFICATION_LATCH_PUBLICATION_FAILED") from exc
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as exc:
                raise QualificationLatchError("QUALIFICATION_LATCH_PUBLICATION_FAILED") from exc

    def _assert_controller(self) -> None:
        if self._owner != (os.getpid(), threading.get_ident()):
            raise PermissionError("qualification latch mutation belongs only to its controller")


@dataclass(frozen=True)
class LiveHealthAssessmentV1:
    run_id: str
    config_hash: str
    observed_at_ns: int
    assessed_at_ns: int
    action: Literal["CONTINUE", "ATTENTION", "STOP & EXPORT"]
    colour: Literal["GREEN", "AMBER", "RED"]
    status: Literal["TESTED", "TEST GATE"]
    reasons: tuple[str, ...]
    guidance: str
    source_state: PublicSourceStateV2
    qualification_failed: bool
    qualification_latch_ref: str | None
    facts_ref: str
    policy_hash: str
    queue_headroom_seconds: float | None
    estimated_disk_reserve_bytes: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, **asdict(self), "source_state": self.source_state.value,
                "reasons": list(self.reasons), "authority": "ZERO"}


_GUIDANCE = {
    "QUEUE_OVERFLOW_LOCAL_DATA_LOSS": "Public stream lost local frames — this run cannot qualify. Stop and export; create a new run.",
    "PUBLIC_STREAM_LOCAL_FRAME_REJECTION": "Public stream rejected local frames — this run cannot qualify. Stop and export; create a new run.",
    "RAW_CAPTURE_TERMINAL_FAILURE": "Raw public evidence capture failed — stop and export; create a new run.",
    "PUBLIC_STREAM_TERMINAL_FAILURE": "Public stream stopped after a terminal failure — stop and export; create a new run.",
    "EVIDENCE_INTEGRITY_FAILURE": "Evidence integrity failed — this run cannot qualify. Stop and preserve the evidence.",
    "EVIDENCE_CLOCK_INTEGRITY_FAILURE": "Evidence chronology failed — stop and preserve the evidence.",
    "UNCLEAN_PUBLIC_CAPTURE_STOP": "Previous public capture did not stop cleanly — preserve this run and create a new run.",
    "PREVENTIVE_PUBLIC_CAPTURE_STOP": "Public capture stopped to protect remaining queue headroom — export this run and recheck storage before a new run.",
    "QUEUE_PRESSURE_RISING": "Queue pressure rising — persistence is not keeping up.",
    "CAPTURE_BACKLOG_PRESSURE": "Durable capture backlog rising — the controller is not keeping up.",
    "STREAM_SERVICE_STALL": "Public stream service is delayed — inspect storage and queue pressure.",
    "PERSISTENCE_STALL": "Persistence is delayed — watch queue and capture headroom.",
    "RUNTIME_HEARTBEAT_STALE": "Runtime heartbeat is stale — check whether the runtime is still progressing.",
    "CONTROLLER_HEARTBEAT_STALE": "Controller progress is delayed — inspect persistence and queue pressure.",
    "HEARTBEAT_UNAVAILABLE": "Runtime progress evidence is unavailable — wait for startup or inspect the runtime.",
    "PUBLIC_STREAM_RECOVERING": "Public stream is recovering — wait for a sequence-valid snapshot.",
    "SOURCE_NOT_CURRENT": "Required public stream evidence is not current — inspect source recovery.",
    "DISK_HEADROOM_INSUFFICIENT": "Disk headroom is insufficient for the measured growth estimate — stop and export safely.",
    "WAL_CHECKPOINT_NOT_PROGRESSING": "WAL checkpoint is not progressing — close long readers and inspect disk headroom.",
    "REPORT_EXPORT_FAILED": "Report export failed — preserve the failure and retry once the runtime has headroom.",
    "REPORT_EXPORT_DELAYED": "Report export is delayed — inspect the report worker.",
    "EVIDENCE_VALIDATION_FAILED": "Evidence validation failed — inspect the report before treating the run as usable.",
    "RESOURCE_PRESSURE": "Runtime resources are under pressure — inspect the host and active workers.",
    "HOST_HEALTH_OBSERVATION_STALE": "Storage/resource observations are delayed — inspect host pressure and preserve evidence.",
    "HEALTH_OBSERVATION_CLOCK_CONFLICT": "Health timestamps conflict with the current clock — stop and inspect chronology.",
    "HEALTH_QUEUE_ENVELOPE_MISMATCH": "Queue configuration differs from the tested operational envelope.",
}


def assess_live_health(facts: LiveHealthFactsV1, *, policy: LiveHealthPolicyV1 | None = None,
                      latch: QualificationFailureV1 | None = None, latch_error: str | None = None,
                      at_ns: int | None = None) -> LiveHealthAssessmentV1:
    """Pure bounded assessment; safe for desktop/read-only consumers.

    ``at_ns`` is the current observation clock for the UI, so an old healthy
    snapshot cannot conceal a stale controller. Existing terminal failure wins
    over every later broad HTTP/source recovery. No filesystem/SQLite writes.
    """
    policy = policy or LiveHealthPolicyV1()
    now = facts.observed_at_ns if at_ns is None else timestamp(at_ns, field="at_ns")
    reasons: list[str] = []
    terminal = _irreversible_failure(facts)
    if latch is not None:
        if latch.run_id != facts.run_id or latch.config_hash != facts.config_hash:
            latch_error = "QUALIFICATION_LATCH_IDENTITY_MISMATCH"
        else:
            terminal = latch.first_cause
    allowed_latch_errors = {"QUALIFICATION_LATCH_INVALID", "QUALIFICATION_LATCH_UNREADABLE",
                            "QUALIFICATION_LATCH_IDENTITY_MISMATCH", "QUALIFICATION_LATCH_PUBLICATION_FAILED",
                            "QUALIFICATION_LATCH_PUBLICATION_INCOMPLETE"}
    if latch_error is not None and latch_error not in allowed_latch_errors:
        raise ValueError("unknown qualification latch failure code")
    red = terminal is not None or latch_error is not None
    if latch_error is not None:
        reasons.append(latch_error)
    if terminal is not None:
        reasons.append(terminal)
    stamps = (facts.observed_at_ns, facts.runtime_heartbeat_at_ns, facts.controller_heartbeat_at_ns,
              facts.wal_last_progress_at_ns, facts.report_started_at_ns, facts.last_export_success_at_ns,
              facts.last_export_failure_at_ns, facts.host_observed_at_ns)
    if any(stamp is not None and stamp > now for stamp in stamps):
        reasons.append("HEALTH_OBSERVATION_CLOCK_CONFLICT")
        red = True
    for stamp, threshold, code in (
        (facts.runtime_heartbeat_at_ns, policy.heartbeat_warning_ns, "RUNTIME_HEARTBEAT_STALE"),
        (facts.controller_heartbeat_at_ns, policy.controller_warning_ns, "CONTROLLER_HEARTBEAT_STALE"),
    ):
        if stamp is None:
            reasons.append("HEARTBEAT_UNAVAILABLE")
        elif now - stamp > threshold:
            reasons.append(code)
    if facts.queue_capacity != policy.queue_capacity:
        reasons.append("HEALTH_QUEUE_ENVELOPE_MISMATCH")
    arrival = facts.arrival_frames_per_second
    drain = facts.drain_frames_per_second
    growth = facts.queue_growth_frames_per_second
    if growth is None and arrival is not None and drain is not None:
        growth = arrival - drain
    headroom = (facts.queue_capacity - facts.queue_items) / growth if growth is not None and growth > 0 else None
    if (facts.queue_items >= facts.queue_capacity / policy.safety_margin
            or facts.queue_bytes is not None and facts.queue_capacity_bytes is not None
            and facts.queue_bytes >= facts.queue_capacity_bytes / policy.safety_margin
            or (facts.queue_items > 0 and headroom is not None and headroom <= policy.service_warning_seconds)):
        reasons.append("QUEUE_PRESSURE_RISING")
    if (facts.capture_pending_batches is not None and facts.capture_max_pending_batches is not None
            and facts.capture_pending_batches >= facts.capture_max_pending_batches / policy.safety_margin):
        reasons.append("CAPTURE_BACKLOG_PRESSURE")
    if any(value is not None and value > policy.service_warning_seconds for value in (
        facts.service_gap_seconds, facts.service_duration_seconds,
    )):
        reasons.append("STREAM_SERVICE_STALL")
    if facts.persistence_seconds is not None and facts.persistence_seconds > policy.service_warning_seconds:
        reasons.append("PERSISTENCE_STALL")
    if facts.recovery_required or facts.unresolved_gap or not facts.connected or facts.producer_state != "RUNNING":
        reasons.append("PUBLIC_STREAM_RECOVERING")
    if facts.source_state != PublicSourceStateV2.HEALTHY_CURRENT:
        reasons.append("SOURCE_NOT_CURRENT")
    reserve = facts.disk_reserve_bytes
    if facts.growth_bytes_per_second is not None and facts.current_footprint_bytes is not None:
        measured = facts.current_footprint_bytes + math.ceil(
            facts.growth_bytes_per_second * policy.growth_projection_seconds * policy.safety_margin)
        reserve = max(reserve or 0, measured)
    if reserve is not None and facts.free_disk_bytes is not None and facts.free_disk_bytes < reserve:
        reasons.append("DISK_HEADROOM_INSUFFICIENT")
    if (facts.wal_uncheckpointed_frames and facts.wal_growth_bytes_per_second is not None
            and facts.wal_growth_bytes_per_second > 0
            and (facts.wal_last_progress_at_ns is None or now - facts.wal_last_progress_at_ns > policy.wal_progress_warning_ns)):
        reasons.append("WAL_CHECKPOINT_NOT_PROGRESSING")
    unresolved_export_failure = (facts.export_failure_count > 0 and (
        facts.last_export_failure_at_ns is None or facts.last_export_success_at_ns is None
        or facts.last_export_success_at_ns <= facts.last_export_failure_at_ns))
    if facts.report_state == "FAILED" or unresolved_export_failure:
        reasons.append("REPORT_EXPORT_FAILED")
    if (facts.report_state == "RUNNING" and facts.report_started_at_ns is not None
            and now - facts.report_started_at_ns > policy.report_warning_ns):
        reasons.append("REPORT_EXPORT_DELAYED")
    if facts.evidence_validation_failures:
        reasons.append("EVIDENCE_VALIDATION_FAILED")
    if facts.host_observed_at_ns is not None and now - facts.host_observed_at_ns > policy.controller_warning_ns:
        reasons.append("HOST_HEALTH_OBSERVATION_STALE")
    if facts.resource_pressure or (facts.rss_bytes is not None and facts.rss_bytes >= policy.rss_warning_bytes) or (
                                  facts.active_workers is not None and facts.max_active_workers is not None
                                   and facts.active_workers > facts.max_active_workers):
        reasons.append("RESOURCE_PRESSURE")
    reasons = list(dict.fromkeys(reasons))
    colour: Literal["GREEN", "AMBER", "RED"]
    action: Literal["CONTINUE", "ATTENTION", "STOP & EXPORT"]
    if red:
        colour, action = "RED", "STOP & EXPORT"
    elif reasons:
        colour, action = "AMBER", "ATTENTION"
    else:
        colour, action = "GREEN", "CONTINUE"
    guidance = (_GUIDANCE.get(reasons[0], "Run qualification evidence is unavailable — stop and preserve the evidence.")
                if reasons else "Continue observing this run; live qualification remains a separate gate.")
    return LiveHealthAssessmentV1(facts.run_id, facts.config_hash, facts.observed_at_ns, now, action, colour,
        "TEST GATE" if reasons else "TESTED", tuple(reasons), guidance, facts.source_state,
        terminal is not None or latch_error is not None, None if latch is None else latch.content_hash,
        facts.content_hash, policy.content_hash, headroom, reserve)


class LiveHealthControllerV1:
    """The sole ops controller durably latches first integrity/terminal failure."""

    def __init__(self, run_directory: Path, *, run_id: str, config_hash: str,
                 policy: LiveHealthPolicyV1 | None = None) -> None:
        self.policy = policy or LiveHealthPolicyV1()
        self.latch = QualificationLatchV1(run_directory, run_id=run_id, config_hash=config_hash, controller=True)
        self._pending_failure: QualificationFailureV1 | None = None
        self._failure: QualificationFailureV1 | None = None
        self._latch_error: str | None = None
        # Validate durable startup state once. This file is immutable and this
        # controller is its sole publisher; healthy watchdog ticks must not
        # perform selected-path metadata I/O. UI/restart readers still validate
        # the file independently, including external corruption.
        try:
            self._failure = self.latch.read()
        except QualificationLatchError as exc:
            self._latch_error = str(exc)

    def observe(self, facts: LiveHealthFactsV1) -> LiveHealthAssessmentV1:
        self.latch._assert_controller()
        if facts.run_id != self.latch.run_id or facts.config_hash != self.latch.config_hash:
            raise ValueError("live health controller cannot observe another run/configuration")
        if self._latch_error is not None:
            return assess_live_health(facts, policy=self.policy, latch_error=self._latch_error)
        code = _irreversible_failure(facts)
        if self._pending_failure is None and code is not None:
            # A failed filesystem publication cannot erase a first failure in
            # this process. Retry the same bounded observation, including when
            # current source health later recovers. Only a published file is
            # represented as a durable qualification_latch_ref.
            self._pending_failure = QualificationFailureV1(facts.run_id, facts.config_hash,
                facts.observed_at_ns, code, facts, self.policy.content_hash)
        try:
            failure = self._failure
            if failure is None and self._pending_failure is not None:
                failure = self.latch.publish(self._pending_failure)
            if failure is not None:
                self._failure = failure
                self._pending_failure = None
            return assess_live_health(facts, policy=self.policy, latch=failure)
        except QualificationLatchError as exc:
            return assess_live_health(facts, policy=self.policy, latch_error=str(exc))
