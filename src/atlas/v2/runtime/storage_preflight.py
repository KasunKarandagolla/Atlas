"""Bounded qualification of a selected local storage path before public research.

All generated data lives in one temporary probe directory. No live run database
is opened. This short workload can reject an obviously unsuitable host/path; it
does not establish public-source or 48-hour endurance qualification.
"""

from __future__ import annotations

import hashlib
import importlib
import math
import os
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

from .._serialization import FrozenMap, json_value, sha256_json, sha256_ref
from ..data.capture_receipts import (
    CaptureReceiptBindingV1,
    CaptureReceiptJournalV1,
    read_last_capture_receipt,
)
from ..data.public_archive_extents import PublicArchiveSegmentWriterV1, read_extent
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..memory.writer_lock import OpsWriterAlreadyActive

PREFLIGHT_CONTRACT_V1 = "StoragePreflightResultV1"
S39_MEASURED_48H_PROJECTION_BYTES = 50_344_153_861

# The offline S41 max-breadth probe is still being measured. Until its
# batched-readback rerun completes, broad V2 runs must not inherit the S39
# projection as a capacity pass. The constant is deliberately None so a
# missing measurement is a visible fail-closed result rather than a guess.
S41_MAX_BREADTH_BYTES_PER_CYCLE_V2: int | None = None
S41_BROAD_REFRESH_CADENCE_SECONDS_V2 = 10
S41_BROAD_RETENTION_HOURS_V2 = 48
S41_MAX_PRODUCTS_PER_VENUE_V2 = 2048
S41_MAX_ACTIVE_PRODUCTS_V2 = 4096


@dataclass(frozen=True)
class StoragePreflightLimitsV1:
    """Registered diagnostic envelope, with an explicit engineering reserve.

    A 512-frame handoff at a declared 320-frame/s burst has 1.6 seconds of
    empty-buffer capacity. Half of that is reserved for scheduling/jitter, so
    any measured synchronous persistence operation exceeding 0.8 s rejects
    the path. A 64-frame batch must average no more than 0.2 s: twice the
    declared 160-frame/s sustained baseline. These are rejection thresholds,
    not promises that a host will never stall later.
    """

    queue_capacity_frames: int = 512
    sustained_frames_per_second: int = 160
    burst_frames_per_second: int = 320
    safety_margin: int = 2
    batch_frames: int = 64
    sustained_batches: int = 16
    burst_batches: int = 8
    payload_bytes_per_frame: int = 4096
    measured_48h_projection_bytes: int = S39_MEASURED_48H_PROJECTION_BYTES
    storage_reserve_numerator: int = 5
    storage_reserve_denominator: int = 4
    total_budget_seconds: int = 30

    def __post_init__(self) -> None:
        values = asdict(self)
        if any(type(value) is not int or value <= 0 for value in values.values()):
            raise ValueError("preflight limits must be positive integers")
        if (self.queue_capacity_frames > 512 or self.batch_frames > 64
                or self.batch_frames > self.queue_capacity_frames
                or self.sustained_batches > 64 or self.burst_batches > 64
                or self.payload_bytes_per_frame > 16_384 or self.total_budget_seconds > 60
                or self.burst_frames_per_second < self.sustained_frames_per_second
                or self.storage_reserve_numerator < self.storage_reserve_denominator
                or self.safety_margin < 2):
            raise ValueError("preflight limits exceed the bounded diagnostic envelope")

    @property
    def maximum_operation_ns(self) -> int:
        return self.queue_capacity_frames * 1_000_000_000 // (
            self.burst_frames_per_second * self.safety_margin)

    @property
    def maximum_mean_service_ns(self) -> int:
        return self.batch_frames * 1_000_000_000 // (
            self.sustained_frames_per_second * self.safety_margin)

    @property
    def minimum_free_bytes(self) -> int:
        numerator = self.measured_48h_projection_bytes * self.storage_reserve_numerator
        return (numerator + self.storage_reserve_denominator - 1) // self.storage_reserve_denominator

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "maximum_operation_ns": self.maximum_operation_ns,
                "maximum_mean_service_ns": self.maximum_mean_service_ns,
                "minimum_free_bytes": self.minimum_free_bytes,
                "projection_class": "MEASURED_S39_SHORT_RUN_ESTIMATE_NOT_GUARANTEED",
                "projection_source": "SESSION039_PUBLIC_EVIDENCE_RELIABILITY_LEDGER.json:capacity_projection_48h",
                "reserve_scope": "DB_WAL_ARCHIVES_REPORTS_FUTURE_EVIDENCE_AND_SAFETY_MARGIN"}


@dataclass(frozen=True)
class StorageProbeMeasurementV1:
    operation: str
    count: int
    minimum_ns: int
    maximum_ns: int
    mean_ns: int
    p95_ns: int
    bytes_processed: int = 0


@dataclass(frozen=True)
class StoragePreflightResultV1:
    selected_path: str
    identity_sha256: str
    started_at_ns: int
    completed_at_ns: int
    elapsed_ns: int
    status: str
    reasons: tuple[str, ...]
    limits: StoragePreflightLimitsV1
    measurements: tuple[StorageProbeMeasurementV1, ...]
    facts: FrozenMap
    probe_mode: str = "HOST_PATH"

    @property
    def allowed(self) -> bool:
        return self.status == "TESTED" and not self.reasons

    def as_dict(self) -> dict[str, Any]:
        body = {"schema_version": 1, "version": PREFLIGHT_CONTRACT_V1,
                "selected_path": self.selected_path, "identity_sha256": self.identity_sha256,
                "started_at_ns": self.started_at_ns, "completed_at_ns": self.completed_at_ns,
                "elapsed_ns": self.elapsed_ns, "status": self.status, "allowed": self.allowed,
                "reasons": list(self.reasons), "limits": self.limits.as_dict(),
                "measurements": [asdict(item) for item in self.measurements],
                "facts": json_value(self.facts), "probe_mode": self.probe_mode,
                "authority": "ZERO", "capital_enabled": False, "assisted_enabled": False,
                "live_source_qualification": "TEST GATE", "endurance_qualification": "TEST GATE"}
        return {**body, "content_hash": sha256_json(body)}


@dataclass(frozen=True)
class BroadStorageCapacityAssessmentV2:
    """Conservative S41 broad-workload disk estimate, separate from S40 V1.

    This projection only addresses predictable local disk headroom. It does
    not qualify source, queue, report, or endurance behaviour. `bytes_per_cycle`
    is populated only from the bounded maximum-breadth persistence probe.
    """

    identity_sha256: str
    selected_path: str
    observed_at_ns: int
    enabled_venues: tuple[str, ...]
    selected_max_products: int
    supported_max_products: int
    refresh_cadence_seconds: int
    retention_hours: int
    projected_cycles: int
    measured_max_cycle_bytes: int | None
    measurement_class: str
    dynamic_metadata_overhead_numerator: int
    dynamic_metadata_overhead_denominator: int
    safety_reserve_numerator: int
    safety_reserve_denominator: int
    owner_free_disk_reserve_bytes: int
    observed_free_bytes: int | None
    projected_workload_bytes: int | None
    required_free_bytes: int | None
    status: str
    allowed: bool
    capacity_qualified: bool
    reasons: tuple[str, ...]
    authority: str = "ZERO"
    capital_enabled: bool = False
    assisted_enabled: bool = False

    def as_dict(self) -> dict[str, Any]:
        body = {**asdict(self), "enabled_venues": list(self.enabled_venues),
                "reasons": list(self.reasons),
                "projection_class": "CONSERVATIVE_OFFLINE_ENVELOPE_NOT_ENDURANCE_QUALIFICATION",
                "projection_scope": "IMMUTABLE_BULK_METADATA_AND_OBSERVATION_INDEX_PLUS_ARCHIVE_GROWTH",
                "exclusions": ["public streams and capture queues", "history/enrichment scheduling",
                    "decision/report concurrency", "future venue metadata revisions beyond overhead reserve",
                    "filesystem-specific allocation and compression variance"],
                "live_source_qualification": "TEST GATE", "endurance_qualification": "TEST GATE"}
        return {**body, "content_hash": sha256_json(body)}


def assess_broad_storage_capacity_v2(
    *, identity_sha256: str, selected_path: str, observed_at_ns: int,
    enabled_venues: tuple[str, ...],
    observed_free_bytes: int | None, owner_free_disk_reserve_bytes: int,
    measured_max_cycle_bytes: int | None = None,
) -> BroadStorageCapacityAssessmentV2:
    """Estimate 48h headroom from the maximum-breadth persistence probe.

    A missing measurement, unsupported venue set, or invalid disk observation
    is TEST GATE. The model scales linearly up to the V2 per-venue cap, reserves
    50% for dynamic metadata/encoding variation, then adds a 25% capacity
    reserve and the owner's independent free-space floor.
    """
    sha256_ref(identity_sha256, field="broad capacity identity")
    if not isinstance(selected_path, str) or not selected_path:
        raise ValueError("broad capacity selected path is required")
    if type(observed_at_ns) is not int or observed_at_ns < 0:
        raise ValueError("broad capacity observation time must be a nonnegative integer")
    if (not isinstance(enabled_venues, tuple) or not enabled_venues
            or len(enabled_venues) > 2 or len(set(enabled_venues)) != len(enabled_venues)
            or any(venue not in {"BYBIT", "BINANCE"} for venue in enabled_venues)):
        raise ValueError("broad capacity venue selection is invalid")
    if type(owner_free_disk_reserve_bytes) is not int or owner_free_disk_reserve_bytes < 0:
        raise ValueError("owner free disk reserve must be a nonnegative integer")
    if observed_free_bytes is not None and (type(observed_free_bytes) is not int or observed_free_bytes < 0):
        raise ValueError("observed free disk bytes must be a nonnegative integer")
    if measured_max_cycle_bytes is not None and (
            type(measured_max_cycle_bytes) is not int or measured_max_cycle_bytes <= 0):
        raise ValueError("measured max-cycle bytes must be a positive integer")

    population = len(enabled_venues) * S41_MAX_PRODUCTS_PER_VENUE_V2
    cycles = S41_BROAD_RETENTION_HOURS_V2 * 60 * 60 // S41_BROAD_REFRESH_CADENCE_SECONDS_V2
    reasons: list[str] = []
    projected: int | None = None
    required: int | None = None
    if measured_max_cycle_bytes is None:
        reasons.append("BROAD_WORKLOAD_CAPACITY_MEASUREMENT_UNAVAILABLE")
    else:
        scaled_cycle = (measured_max_cycle_bytes * population + S41_MAX_ACTIVE_PRODUCTS_V2 - 1) \
            // S41_MAX_ACTIVE_PRODUCTS_V2
        metadata_adjusted = (scaled_cycle * 3 + 1) // 2
        projected = metadata_adjusted * cycles
        reserved = (projected * 5 + 3) // 4
        required = reserved + owner_free_disk_reserve_bytes
    if observed_free_bytes is None:
        reasons.append("BROAD_WORKLOAD_FREE_SPACE_UNAVAILABLE")
    elif required is not None and observed_free_bytes < required:
        reasons.append("BROAD_WORKLOAD_DISK_HEADROOM_INSUFFICIENT")
    allowed = measured_max_cycle_bytes is not None and observed_free_bytes is not None and not reasons
    return BroadStorageCapacityAssessmentV2(
        identity_sha256=identity_sha256, selected_path=selected_path,
        observed_at_ns=observed_at_ns, enabled_venues=enabled_venues,
        selected_max_products=population, supported_max_products=S41_MAX_ACTIVE_PRODUCTS_V2,
        refresh_cadence_seconds=S41_BROAD_REFRESH_CADENCE_SECONDS_V2,
        retention_hours=S41_BROAD_RETENTION_HOURS_V2, projected_cycles=cycles,
        measured_max_cycle_bytes=measured_max_cycle_bytes,
        measurement_class=("UNAVAILABLE_FAIL_CLOSED" if measured_max_cycle_bytes is None else
                           "THREE_CYCLE_SYNTHETIC_MAX_BREADTH_OFFLINE_UPPER_SAMPLE"),
        dynamic_metadata_overhead_numerator=3, dynamic_metadata_overhead_denominator=2,
        safety_reserve_numerator=5, safety_reserve_denominator=4,
        owner_free_disk_reserve_bytes=owner_free_disk_reserve_bytes,
        observed_free_bytes=observed_free_bytes, projected_workload_bytes=projected,
        required_free_bytes=required,
        status="TESTED" if allowed else "TEST GATE", allowed=allowed,
        capacity_qualified=False, reasons=tuple(reasons))


def _noop_phase(_operation: str) -> None:
    return None


def _noop_sqlite(_connection: sqlite3.Connection) -> None:
    return None


def _default_dependencies() -> Mapping[str, Any]:
    # Imports exercise the installed runtime without a network/provider call.
    versions: dict[str, Any] = {"sqlite": sqlite3.sqlite_version}
    for name in ("pyarrow", "duckdb", "websockets"):
        module = importlib.import_module(name)
        versions[name] = str(getattr(module, "__version__", "UNVERIFIED"))
    from ..product import runtime_dependency_smoke

    versions.update(runtime_dependency_smoke())
    return versions


def _default_export(database: Path, destination: Path, identity_sha256: str) -> Mapping[str, Any]:
    from ..science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

    # Identity is diagnostic-only, never borrowed from a real research run.
    identity = TuningRunIdentityV1("storage-preflight", identity_sha256, "0" * 40, 0)
    result = export_tuning_snapshot(database, destination, identity, cutoff_ns=time.time_ns(), max_rows=16_384)
    if result["validation_failures"]:
        raise ValueError("generated preflight evidence failed strict export validation")
    if result["has_more"]:
        raise ValueError("bounded generated preflight evidence did not finish exporting")
    if result["report"]["status"] != "TESTED":
        raise ValueError("generated preflight evidence report did not pass its bounded analysis smoke")
    return {"status": result["report"]["status"], "validation_failures": result["validation_failures"],
            "has_more": result["has_more"], "partition": result["partition"]}


@dataclass(frozen=True)
class StoragePreflightHooksV1:
    """Deterministic test seams; supplied hooks are recorded as fault injection.

    Production must omit this argument. Callback exception text is never
    copied into the result. Hooks cannot supply access to a live database.
    """

    phase: Callable[[str], None] = _noop_phase
    inspect_sqlite: Callable[[sqlite3.Connection], None] = _noop_sqlite
    free_bytes: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free
    dependencies: Callable[[], Mapping[str, Any]] = _default_dependencies
    export: Callable[[Path, Path, str], Mapping[str, Any]] = _default_export
    monotonic_ns: Callable[[], int] = time.monotonic_ns
    wall_ns: Callable[[], int] = time.time_ns
    sleep: Callable[[float], None] = time.sleep


class _ProbeRejected(RuntimeError):
    pass


@dataclass
class _ProbeMetrics:
    durations: dict[str, list[int]] = field(default_factory=dict)
    bytes_processed: dict[str, int] = field(default_factory=dict)

    def finish(self) -> tuple[StorageProbeMeasurementV1, ...]:
        measurements = []
        for name, values in sorted(self.durations.items()):
            ordered = sorted(values)
            measurements.append(StorageProbeMeasurementV1(name, len(values), ordered[0], ordered[-1],
                sum(values) // len(values), ordered[math.ceil(len(values) * 0.95) - 1],
                self.bytes_processed.get(name, 0)))
        return tuple(measurements)


def qualify_storage_path(
    selected_path: Path, *, identity_sha256: str,
    limits: StoragePreflightLimitsV1 | None = None,
    hooks: StoragePreflightHooksV1 | None = None,
) -> StoragePreflightResultV1:
    """Probe a local path using bounded temporary data, returning exact reasons.

    Run this in the product's isolated preflight process. A filesystem syscall
    can itself hang; a parent process must enforce the declared wall deadline.
    This function checks the same deadline between all bounded probe operations.
    """
    sha256_ref(identity_sha256, field="preflight identity")
    selected_limits = limits or StoragePreflightLimitsV1()
    selected_hooks = hooks or StoragePreflightHooksV1()
    started = selected_hooks.wall_ns()
    monotonic_start = selected_hooks.monotonic_ns()
    deadline = monotonic_start + selected_limits.total_budget_seconds * 1_000_000_000
    metrics = _ProbeMetrics()
    facts: dict[str, Any] = {"temporary_probe_only": True, "real_run_database_opened": False,
                            "probe_removed": False, "filesystem_type": "UNVERIFIED"}
    reasons: list[str] = []
    current_operation = "PATH_SEMANTICS"
    root = selected_path.expanduser()
    probe: Path | None = None

    def check_deadline() -> None:
        if selected_hooks.monotonic_ns() > deadline:
            raise _ProbeRejected("PREFLIGHT_TIME_BUDGET_EXCEEDED")

    def measured(name: str, action: Callable[[], Any], *, bytes_processed: int = 0) -> Any:
        nonlocal current_operation
        check_deadline()
        current_operation = name
        begin = selected_hooks.monotonic_ns()
        try:
            selected_hooks.phase(name)
            return action()
        finally:
            elapsed = selected_hooks.monotonic_ns() - begin
            if elapsed < 0:
                raise _ProbeRejected("MONOTONIC_CLOCK_REGRESSION")
            metrics.durations.setdefault(name, []).append(elapsed)
            metrics.bytes_processed[name] = metrics.bytes_processed.get(name, 0) + bytes_processed
            check_deadline()
            if (name in {"FILESYSTEM_WRITE_FLUSH_READ_RENAME_DELETE", "ARCHIVE_WRITE_FSYNC",
                         "CAPTURE_RECEIPT_FSYNC", "SQLITE_TRANSACTION_COMMIT", "SQLITE_PASSIVE_CHECKPOINT"}
                    and elapsed > selected_limits.maximum_operation_ns):
                raise _ProbeRejected(f"{name}_EXCEEDS_QUEUE_HEADROOM")

    try:
        if str(root).startswith(("\\\\", "//")):
            raise _ProbeRejected("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED")
        measured("PATH_CREATE", lambda: root.mkdir(parents=True, exist_ok=True))
        root = root.resolve(strict=True)
        if not root.is_dir():
            raise _ProbeRejected("SELECTED_PATH_NOT_DIRECTORY")
        facts["filesystem_type"] = measured("FILESYSTEM_LOCALITY", lambda: _local_filesystem_type(root))
        free = measured("DISK_HEADROOM", lambda: selected_hooks.free_bytes(root))
        if type(free) is not int or free < 0:
            raise _ProbeRejected("DISK_HEADROOM_UNAVAILABLE")
        facts["free_disk_bytes"] = free
        if free < selected_limits.minimum_free_bytes:
            raise _ProbeRejected("INSUFFICIENT_DISK_HEADROOM")
        probe = measured("PROBE_CREATE", lambda: Path(tempfile.mkdtemp(prefix=".atlas-preflight-", dir=root)))
        token = os.urandom(4096)
        original, renamed = probe / "semantics-original.bin", probe / "semantics-renamed.bin"

        def filesystem() -> None:
            with original.open("xb") as handle:
                if handle.write(token) != len(token):
                    raise OSError("short temporary write")
                handle.flush()
                os.fsync(handle.fileno())
            if original.read_bytes() != token:
                raise ValueError("temporary read mismatch")
            os.replace(original, renamed)
            if renamed.read_bytes() != token or original.exists():
                raise ValueError("temporary rename mismatch")
            linked = probe / "semantics-hard-link.bin"
            os.link(renamed, linked)
            if linked.read_bytes() != token:
                raise ValueError("temporary hard-link mismatch")
            linked.unlink()
            renamed.unlink()
            if renamed.exists():
                raise ValueError("temporary delete mismatch")

        measured("FILESYSTEM_WRITE_FLUSH_READ_RENAME_DELETE", filesystem, bytes_processed=len(token))
        facts["dependencies"] = measured("RUNTIME_DEPENDENCIES", selected_hooks.dependencies)
        import pyarrow as pa

        path = probe / "probe.sqlite"
        with measured("SQLITE_CREATE", lambda: OpsRepository(path)) as repository:
            connection = repository._connection
            measured("SQLITE_INSPECTION", lambda: selected_hooks.inspect_sqlite(connection))
            mode = measured("SQLITE_WAL_MODE", lambda: connection.execute("PRAGMA journal_mode").fetchone()[0])
            sync = measured("SQLITE_FULL_MODE", lambda: connection.execute("PRAGMA synchronous").fetchone()[0])
            facts.update(sqlite_journal_mode=str(mode), sqlite_synchronous=sync)
            if str(mode).lower() != "wal":
                raise _ProbeRejected("SQLITE_WAL_UNAVAILABLE")
            if sync != 2:
                raise _ProbeRejected("SQLITE_FULL_DURABILITY_UNAVAILABLE")
            archive = PublicArchiveSegmentWriterV1(probe / "ops-public-extents")
            receipt_root = probe / "ops-public-capture"
            receipt_binding = CaptureReceiptBindingV1("temporary-preflight", identity_sha256, "0" * 64)
            journal = CaptureReceiptJournalV1(receipt_root, binding=receipt_binding)
            last_receipt: dict[str, Any] | None = None
            sample_refs: list[str] = []
            extent_refs: list[str] = []
            replay_hashes: dict[str, str] = {}
            next_arrival = selected_hooks.monotonic_ns()
            interval_ns = selected_limits.batch_frames * 1_000_000_000 // selected_limits.sustained_frames_per_second
            total_batches = selected_limits.sustained_batches + selected_limits.burst_batches
            for batch in range(total_batches):
                sustained = batch < selected_limits.sustained_batches
                if sustained:
                    delay_ns = next_arrival - selected_hooks.monotonic_ns()
                    if delay_ns > 0:
                        selected_hooks.sleep(delay_ns / 1_000_000_000)
                    next_arrival += interval_ns
                begin_service = selected_hooks.monotonic_ns()
                rows: list[dict[str, Any]] = []
                at = selected_hooks.wall_ns()
                for index in range(selected_limits.batch_frames):
                    sequence = batch * selected_limits.batch_frames + index
                    # Vary every frame to avoid a zero-entropy compression benchmark.
                    payload = "".join(hashlib.sha512(
                        f"ATLAS_STORAGE_PREFLIGHT:{sequence}:{block}".encode()).hexdigest()
                        for block in range(math.ceil(selected_limits.payload_bytes_per_frame / 128)))[
                            :selected_limits.payload_bytes_per_frame]
                    rows.append({"sequence": sequence, "received_at_ns": at,
                                 "raw_payload": payload.encode("ascii")})
                table = pa.Table.from_pylist(rows)
                chunk = sha256_json({"probe": identity_sha256, "batch": batch,
                                     "payload_hashes": [hashlib.sha256(item["raw_payload"]).hexdigest() for item in rows]})
                entry = measured("ARCHIVE_WRITE_FSYNC", partial(archive.seal,
                    table, namespace="ops-public-transport", chunk_id=chunk,
                    clock_ns=selected_hooks.wall_ns, floor_ns=at), bytes_processed=table.nbytes)
                last_receipt = {"version": "STORAGE_PREFLIGHT_CAPTURE_RECEIPT_V1", "authority": "ZERO",
                    "temporary_only": True, "extent_ref": entry.artifact_ref,
                    "content_hash": entry.content_hash, "metadata": json_value(entry.metadata),
                    "frame_count": selected_limits.batch_frames}
                measured("CAPTURE_RECEIPT_FSYNC", partial(journal.append, last_receipt))
                entries = []
                for row in rows:
                    body = {"status": "TESTED", "rss_bytes": 0, "wal_bytes": 0, "cpu_percent": 0.0,
                            "preflight_sequence": row["sequence"], "temporary_only": True,
                            "probe_payload": row["raw_payload"].decode("ascii")}
                    ref = sha256_json(body)
                    entries.append(ArtifactIndexEntryV2(ref, "ResearchResourceSampleV1", ref, at, at, body))

                def commit(entries: list[ArtifactIndexEntryV2] = entries, entry: ArtifactIndexEntryV2 = entry) -> None:
                    with repository.atomic_composition():
                        repository.register_artifact(entry)
                        for sample in entries:
                            repository.register_artifact(sample)

                measured("SQLITE_TRANSACTION_COMMIT", commit,
                         bytes_processed=selected_limits.batch_frames * selected_limits.payload_bytes_per_frame)
                sample_refs.extend(sample.artifact_ref for sample in entries)
                extent_refs.append(entry.artifact_ref)
                replay_hashes[entry.artifact_ref] = _replay_hash(rows)
                duration = selected_hooks.monotonic_ns() - begin_service
                label = "SUSTAINED_BATCH_SERVICE" if sustained else "BURST_BATCH_SERVICE"
                metrics.durations.setdefault(label, []).append(duration)
                measured("ARCHIVE_REPLAY", partial(_assert_archive_rows,
                    repository, entry.artifact_ref, selected_limits.batch_frames, replay_hashes[entry.artifact_ref]))
                check_deadline()
            facts.update(generated_frame_count=total_batches * selected_limits.batch_frames,
                         generated_archive_extents=len(extent_refs), generated_index_rows=len(sample_refs) + len(extent_refs))
            tail = measured("CAPTURE_RECEIPT_REOPEN", lambda: read_last_capture_receipt(receipt_root,
                expected_run_id=receipt_binding.run_id, expected_configuration_hash=identity_sha256))
            if tail is None or json_value(tail.receipt) != last_receipt:
                raise _ProbeRejected("CAPTURE_RECEIPT_REOPEN_MISMATCH")
            facts.update(capture_receipt_bytes=journal.bytes_written,
                         capture_receipt_reopen_exact=True, capture_receipts_written=total_batches)
            measured("SQLITE_SECOND_WRITER_REJECTION", partial(_assert_second_writer_rejected, path))
            facts["sole_writer_lease_enforced"] = True
            with OpsRepository(path, read_only=True) as concurrent_reader, concurrent_reader.read_snapshot():
                if concurrent_reader.get_artifact(sample_refs[0]) is None:
                    raise _ProbeRejected("CONCURRENT_READER_EVIDENCE_MISSING")
                extra_body = {"status": "TESTED", "rss_bytes": 0, "probe": "PINNED_READ_SNAPSHOT"}
                extra_ref = sha256_json(extra_body)
                extra_at = selected_hooks.wall_ns()
                extra = ArtifactIndexEntryV2(extra_ref, "ResearchResourceSampleV1", extra_ref,
                                             extra_at, extra_at, extra_body)
                measured("SQLITE_TRANSACTION_COMMIT", partial(repository.register_artifact, extra))
                facts["pinned_reader_checkpoint"] = measured("SQLITE_PASSIVE_CHECKPOINT", lambda: tuple(
                    connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()))
                facts["generated_index_rows"] += 1
            checkpoint = measured("SQLITE_PASSIVE_CHECKPOINT", lambda: tuple(
                connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()))
            facts["wal_checkpoint"] = checkpoint
            if checkpoint[0] != 0 or checkpoint[1] != checkpoint[2]:
                raise _ProbeRejected("PROBE_WAL_CHECKPOINT_NOT_PROGRESSING")
            facts["database_bytes"] = path.stat().st_size
            wal = path.with_name(path.name + "-wal")
            facts["wal_bytes"] = wal.stat().st_size if wal.exists() else 0
            facts["raw_archive_bytes"] = sum(item.stat().st_size for item in archive.root.glob("*.arrow"))
        with measured("SQLITE_READONLY_REOPEN", lambda: OpsRepository(path, read_only=True)) as reader:
            integrity = measured("SQLITE_INTEGRITY", lambda: reader._connection.execute("PRAGMA quick_check(1)").fetchone()[0])
            if integrity != "ok":
                raise _ProbeRejected("SQLITE_INTEGRITY_FAILED")
            facts["sqlite_integrity"] = integrity
            if reader.get_artifact(sample_refs[-1]) is None:
                raise _ProbeRejected("SQLITE_REOPEN_EVIDENCE_MISSING")
            for ref in extent_refs:
                measured("REOPEN_ARCHIVE_REPLAY", partial(_assert_archive_rows,
                    reader, ref, selected_limits.batch_frames, replay_hashes[ref]))
        facts["export_smoke"] = measured("STRICT_READONLY_EXPORT", lambda: selected_hooks.export(
            path, probe / "reports", identity_sha256))
        for label in ("FILESYSTEM_WRITE_FLUSH_READ_RENAME_DELETE", "ARCHIVE_WRITE_FSYNC",
                      "CAPTURE_RECEIPT_FSYNC", "SQLITE_TRANSACTION_COMMIT", "SQLITE_PASSIVE_CHECKPOINT"):
            if max(metrics.durations[label]) > selected_limits.maximum_operation_ns:
                reasons.append(f"{label}_EXCEEDS_QUEUE_HEADROOM")
        for label in ("SUSTAINED_BATCH_SERVICE", "BURST_BATCH_SERVICE"):
            if sum(metrics.durations[label]) // len(metrics.durations[label]) > selected_limits.maximum_mean_service_ns:
                reasons.append(f"{label}_CANNOT_SUSTAIN_DECLARED_RATE")
        if max(metrics.durations["STRICT_READONLY_EXPORT"]) > 5_000_000_000:
            reasons.append("EXPORT_SMOKE_EXCEEDS_FIXED_BUDGET_SAFETY_MARGIN")
    except _ProbeRejected as error:
        reasons.append(str(error))
    except Exception as error:
        # Exception names identify the failure without exposing host logs or secrets.
        name = type(error).__name__
        safe_name = name if name.isascii() and name.isidentifier() and len(name) <= 64 else "Exception"
        reasons.append(f"{current_operation}_FAILED_{safe_name}")
    finally:
        if probe is not None:
            try:
                shutil.rmtree(probe)
                facts["probe_removed"] = not probe.exists()
            except Exception:
                reasons.append("TEMPORARY_PROBE_CLEANUP_FAILED")
    completed = selected_hooks.wall_ns()
    elapsed = selected_hooks.monotonic_ns() - monotonic_start
    if completed < started or elapsed < 0:
        reasons.append("PREFLIGHT_CLOCK_REGRESSION")
    if elapsed > selected_limits.total_budget_seconds * 1_000_000_000:
        reasons.append("PREFLIGHT_TIME_BUDGET_EXCEEDED")
    return StoragePreflightResultV1(str(root), identity_sha256, started, completed, max(0, elapsed),
        "TEST GATE" if reasons else "TESTED", tuple(dict.fromkeys(reasons)), selected_limits,
        metrics.finish(), FrozenMap(facts), "FAULT_INJECTION" if hooks is not None else "HOST_PATH")


def _replay_hash(rows: list[dict[str, Any]]) -> str:
    return sha256_json([{**row, "raw_payload": hashlib.sha256(row["raw_payload"]).hexdigest()} for row in rows])


def _assert_second_writer_rejected(path: Path) -> None:
    try:
        with OpsRepository(path):
            raise _ProbeRejected("SQLITE_SOLE_WRITER_LEASE_UNAVAILABLE")
    except OpsWriterAlreadyActive:
        return


def _assert_archive_rows(repository: OpsRepository, ref: str, expected_rows: int, expected_hash: str) -> None:
    table = read_extent(repository, ref)
    if table.num_rows != expected_rows or _replay_hash(table.to_pylist()) != expected_hash:
        raise ValueError("preflight archive replay order or byte identity mismatch")


def _local_filesystem_type(root: Path) -> str:
    """Reject known remote mounts, including Windows mapped network drives."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        volume = ctypes.create_unicode_buffer(261)
        filesystem = ctypes.create_unicode_buffer(261)
        kernel = ctypes.windll.kernel32  # type: ignore[attr-defined]
        kernel.GetVolumePathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        kernel.GetVolumePathNameW.restype = wintypes.BOOL
        kernel.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
        kernel.GetDriveTypeW.restype = wintypes.UINT
        kernel.GetVolumeInformationW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD),
            wintypes.LPWSTR, wintypes.DWORD]
        kernel.GetVolumeInformationW.restype = wintypes.BOOL
        if not kernel.GetVolumePathNameW(str(root), volume, len(volume)):
            raise _ProbeRejected("FILESYSTEM_LOCALITY_UNAVAILABLE")
        drive_type = kernel.GetDriveTypeW(volume.value)
        if drive_type == 4:
            raise _ProbeRejected("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED")
        if drive_type not in {2, 3, 6}:
            raise _ProbeRejected("FILESYSTEM_LOCALITY_UNAVAILABLE")
        if not kernel.GetVolumeInformationW(volume.value, None, 0, None, None, None, filesystem, len(filesystem)):
            raise _ProbeRejected("FILESYSTEM_TYPE_UNAVAILABLE")
        return str(filesystem.value)
    mounts = Path("/proc/self/mountinfo")
    if not mounts.exists():
        return "UNVERIFIED"
    matching = []
    for line in mounts.read_text(encoding="utf-8").splitlines():
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        fields, tail = before.split(), after.split()
        if len(fields) < 5 or not tail:
            continue
        # Linux mountinfo escapes spaces, tabs, newlines and literal backslashes.
        mount_name = fields[4]
        for encoded, decoded in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            mount_name = mount_name.replace(encoded, decoded)
        mount = Path(mount_name)
        if root.is_relative_to(mount):
            matching.append((len(mount_name), tail[0]))
    if not matching:
        return "UNVERIFIED"
    kind = max(matching)[1]
    if kind in {"nfs", "nfs4", "cifs", "smbfs", "fuse.sshfs", "9p"}:
        raise _ProbeRejected("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED")
    return kind
