"""Bounded storage guardian for the ATLAS RED full-cycle research session.

The session writes only inside one kernel-enforced, size-capped filesystem. This
module measures that filesystem, reconciles write reservations against actual
bytes, refuses writes that would breach the configured guard, and emits a
durable storage manifest. It has no trading, provider or repository authority.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

GUARDIAN_VERSION = "ATLAS_RED_STORAGE_GUARDIAN_V1"

# Owner-mandated ceilings (bytes). ABSOLUTE is never reached or exceeded.
ABSOLUTE_CEILING_BYTES = 5_000_000_000
OPERATING_BUDGET_BYTES = 4_000_000_000
PROTECTIVE_WRITE_STOP_BYTES = 4_800_000_000
DEFAULT_MINIMUM_RESERVE_BYTES = 200_000_000


class StorageBudgetExceeded(RuntimeError):
    """Raised when a write would breach the registered storage guard."""


@dataclass(frozen=True)
class StorageGuardLimitsV1:
    absolute_ceiling_bytes: int = ABSOLUTE_CEILING_BYTES
    operating_budget_bytes: int = OPERATING_BUDGET_BYTES
    protective_write_stop_bytes: int = PROTECTIVE_WRITE_STOP_BYTES
    minimum_reserve_bytes: int = DEFAULT_MINIMUM_RESERVE_BYTES

    def __post_init__(self) -> None:
        values = asdict(self)
        if any(type(value) is not int or value <= 0 for value in values.values()):
            raise ValueError("storage guard limits must be positive integers")
        if not (
            self.operating_budget_bytes
            < self.protective_write_stop_bytes
            < self.absolute_ceiling_bytes
        ):
            raise ValueError("storage guard limits must be strictly ordered")
        if self.minimum_reserve_bytes >= self.operating_budget_bytes:
            raise ValueError("storage guard reserve must be below the operating budget")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StorageReservationV1:
    reservation_id: str
    requested_bytes: int
    granted_bytes: int
    created_at_ns: int
    purpose: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StorageGuardianV1:
    """Measures one bounded workspace and enforces the global ATLAS-managed guard.

    ``external_baseline_bytes`` is the measured ATLAS-managed footprint outside
    the bounded workspace (pre-existing owner artifacts). It is measured once by
    the operator and refreshed explicitly; the guardian never deletes owner data.
    """

    workspace_root: Path
    limits: StorageGuardLimitsV1 = field(default_factory=StorageGuardLimitsV1)
    external_baseline_bytes: int = 0
    external_baseline_measured_at_ns: int = 0
    _reservations: dict[str, StorageReservationV1] = field(default_factory=dict, repr=False)
    _counter: int = field(default=0, repr=False)
    peak_occupied_bytes: int = 0
    peak_reserved_bytes: int = 0
    writes_paused: int = 0
    pause_reasons: list[str] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.workspace_root = Path(self.workspace_root)
        if not self.workspace_root.is_dir():
            raise ValueError("storage guardian workspace root must exist")
        if self.external_baseline_bytes < 0:
            raise ValueError("external baseline cannot be negative")
        self.peak_occupied_bytes = self.occupied_bytes()

    # -- measurement -------------------------------------------------
    def filesystem_bytes(self) -> tuple[int, int]:
        """Return (used_bytes, capacity_bytes) of the bounded workspace.

        A dedicated mount is measured with ``statvfs`` because the whole
        filesystem is the workspace. A plain directory is measured by walking
        it, so the guardian never charges unrelated host usage to the session.
        """

        if os.path.ismount(self.workspace_root):
            stats = os.statvfs(self.workspace_root)
            capacity = stats.f_blocks * stats.f_frsize
            used = (stats.f_blocks - stats.f_bfree) * stats.f_frsize
            return used, capacity
        used = measure_tree_bytes([self.workspace_root])
        capacity = used + 1_000_000_000
        return used, capacity

    def occupied_bytes(self) -> int:
        """Total ATLAS-managed bytes: external baseline + bounded workspace usage."""

        used, _ = self.filesystem_bytes()
        return self.external_baseline_bytes + used

    def reserved_bytes(self) -> int:
        return sum(item.granted_bytes for item in self._reservations.values())

    def _record(self, event: str, **fields: Any) -> None:
        self.events.append({"event": event, "at_ns": time.time_ns(), **fields})
        if len(self.events) > 4096:
            del self.events[:1024]

    # -- reservation --------------------------------------------------
    def reserve(self, requested_bytes: int, *, purpose: str) -> StorageReservationV1:
        """Reserve space before a write, or refuse it fail-closed."""

        if type(requested_bytes) is not int or requested_bytes < 0:
            raise ValueError("storage reservation must be a non-negative integer")
        projected = self.occupied_bytes() + self.reserved_bytes() + requested_bytes
        limit = self.limits.protective_write_stop_bytes
        if projected > limit:
            self.writes_paused += 1
            reason = (
                f"RESERVATION_DENIED purpose={purpose} requested={requested_bytes} "
                f"projected={projected} limit={limit}"
            )
            self.pause_reasons.append(reason)
            self._record("reservation_denied", purpose=purpose, requested_bytes=requested_bytes,
                         projected_bytes=projected)
            raise StorageBudgetExceeded(reason)
        self._counter += 1
        reservation = StorageReservationV1(
            reservation_id=f"res-{self._counter:08d}",
            requested_bytes=requested_bytes,
            granted_bytes=requested_bytes,
            created_at_ns=time.time_ns(),
            purpose=purpose,
        )
        self._reservations[reservation.reservation_id] = reservation
        self.peak_reserved_bytes = max(self.peak_reserved_bytes, self.reserved_bytes())
        self.peak_occupied_bytes = max(self.peak_occupied_bytes, self.occupied_bytes())
        self._record("reservation_granted", purpose=purpose, bytes=requested_bytes)
        return reservation

    def reconcile(self, reservation: StorageReservationV1, actual_bytes: int) -> None:
        """Reconcile a reservation against the bytes actually written."""

        if reservation.reservation_id not in self._reservations:
            raise ValueError("unknown storage reservation")
        if type(actual_bytes) is not int or actual_bytes < 0:
            raise ValueError("reconciled bytes must be a non-negative integer")
        if actual_bytes > reservation.granted_bytes:
            raise StorageBudgetExceeded(
                f"write exceeded its reservation: {actual_bytes} > {reservation.granted_bytes}")
        self._reservations[reservation.reservation_id] = StorageReservationV1(
            reservation_id=reservation.reservation_id,
            requested_bytes=reservation.requested_bytes,
            granted_bytes=actual_bytes,
            created_at_ns=reservation.created_at_ns,
            purpose=reservation.purpose,
        )
        self.peak_occupied_bytes = max(self.peak_occupied_bytes, self.occupied_bytes())
        self._record("reservation_reconciled", purpose=reservation.purpose, actual_bytes=actual_bytes)

    def release(self, reservation_id: str) -> None:
        self._reservations.pop(reservation_id, None)

    def guard_state(self) -> str:
        if self.occupied_bytes() + self.reserved_bytes() > self.limits.protective_write_stop_bytes:
            return "PROTECTIVE_WRITE_STOP"
        if self.occupied_bytes() + self.reserved_bytes() > self.limits.operating_budget_bytes:
            return "OVER_OPERATING_BUDGET"
        return "OK"

    # -- manifest -----------------------------------------------------
    def manifest(self, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        used, capacity = self.filesystem_bytes()
        payload: dict[str, Any] = {
            "version": GUARDIAN_VERSION,
            "generated_at_ns": time.time_ns(),
            "workspace_root": str(self.workspace_root),
            "limits": self.limits.as_dict(),
            "external_baseline_bytes": self.external_baseline_bytes,
            "external_baseline_measured_at_ns": self.external_baseline_measured_at_ns,
            "workspace_used_bytes": used,
            "workspace_capacity_bytes": capacity,
            "occupied_bytes": self.occupied_bytes(),
            "reserved_bytes": self.reserved_bytes(),
            "peak_occupied_bytes": self.peak_occupied_bytes,
            "peak_reserved_bytes": self.peak_reserved_bytes,
            "writes_paused": self.writes_paused,
            "pause_reasons": list(self.pause_reasons),
            "guard_state": self.guard_state(),
            "open_reservations": len(self._reservations),
        }
        if extra:
            payload.update(dict(extra))
        return payload

    def write_manifest(self, path: Path, *, extra: Mapping[str, Any] | None = None) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.manifest(extra=extra), indent=2, sort_keys=True).encode("utf-8")
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(payload)
        tmp.replace(path)
        return path


def measure_tree_bytes(paths: Iterable[Path]) -> int:
    """Measure allocated bytes of the given paths (no symlink following)."""

    total = 0
    for root in paths:
        root = Path(root)
        if not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            dirnames.sort()
            for name in sorted(filenames):
                try:
                    stat = os.lstat(os.path.join(dirpath, name))
                except OSError:
                    continue
                total += stat.st_blocks * 512
    return total


@dataclass(frozen=True)
class StorageLayoutV2:
    """Owner clarification: only temporary bytes consume the strict 5 GB cap.

    A dedicated kernel bounded volume is required for a strict-cap claim.
    Permanent evidence, SQLite WAL and final models use a disjoint durable root.
    """

    temporary_root: Path
    permanent_root: Path
    physical_free_floor_bytes: int = 2_000_000_000
    external_temporary_bytes: int = 0

    def __post_init__(self) -> None:
        temporary, permanent = self.temporary_root.resolve(), self.permanent_root.resolve()
        if temporary == permanent or temporary in permanent.parents or permanent in temporary.parents:
            raise ValueError("TEMPORARY_PERMANENT_PATHS_OVERLAP")
        if self.external_temporary_bytes < 0 or self.physical_free_floor_bytes < 0:
            raise ValueError("NEGATIVE_STORAGE_RESERVE")
        object.__setattr__(self, "temporary_root", temporary)
        object.__setattr__(self, "permanent_root", permanent)

    def verify(self, *, require_quota: bool = True) -> dict[str, Any]:
        if not self.temporary_root.is_dir() or not self.permanent_root.is_dir():
            raise ValueError("STORAGE_ROOT_MISSING")
        stats = os.statvfs(self.temporary_root)
        capacity = stats.f_blocks * stats.f_frsize
        fragments = sum(p.stat().st_size for p in self.permanent_root.rglob("*.atlas-part") if p.is_file())
        fragments += sum(measure_tree_bytes([p]) for p in self.permanent_root.rglob("*.atlas-publishing") if p.is_dir())
        # Atomic publication fragments on the permanent filesystem remain TEMPORARY.
        # A mount's capacity, including filesystem rounding, is the hard bound.
        bounded = (
            os.path.ismount(self.temporary_root)
            and capacity + self.external_temporary_bytes + fragments < ABSOLUTE_CEILING_BYTES
        )
        if require_quota and not bounded:
            raise StorageBudgetExceeded("TEMPORARY_KERNEL_QUOTA_UNVERIFIED")
        self.reserve_physical(0)
        return {
            "policy": "ATLAS_TEMPORARY_ONLY_BELOW_5GB_V2",
            "kernel_quota_verified": bounded,
            "temporary_capacity_bytes": capacity,
            "external_temporary_bytes": self.external_temporary_bytes,
            "publication_temporary_bytes": fragments,
            "permanent_root": str(self.permanent_root),
            "temporary_root": str(self.temporary_root),
            "physical_free_floor_bytes": self.physical_free_floor_bytes,
        }

    def reserve_physical(self, worst_case_additional_bytes: int) -> None:
        if worst_case_additional_bytes < 0:
            raise ValueError("NEGATIVE_WRITE_ESTIMATE")
        stats = os.statvfs(self.permanent_root)
        free = stats.f_bavail * stats.f_frsize
        if free - worst_case_additional_bytes < self.physical_free_floor_bytes:
            raise StorageBudgetExceeded("PHYSICAL_FREE_SPACE_FLOOR")

    def staged_copy(self, source: Path, destination: Path, *, expected_sha256: str, expected_bytes: int) -> Path:
        """Bounded resumable final placement; never remove source/owner data.

        A single .part is durable incomplete download state, never a checkpoint.
        Only verified complete bytes are atomically published. Interrupted .part
        files are retained for inspection/resume, counted against physical space.
        """
        import hashlib

        destination = destination.resolve()
        if self.permanent_root not in destination.parents:
            raise ValueError("FINAL_DESTINATION_OUTSIDE_PERMANENT_ROOT")
        self.reserve_physical(expected_bytes)
        stats = os.statvfs(self.temporary_root)
        temporary_bound = (
            stats.f_blocks * stats.f_frsize
            if os.path.ismount(self.temporary_root)
            else measure_tree_bytes([self.temporary_root])
        )
        if temporary_bound + self.external_temporary_bytes + expected_bytes >= ABSOLUTE_CEILING_BYTES:
            raise StorageBudgetExceeded("ATOMIC_PUBLICATION_TEMPORARY_RESERVATION_DENIED")
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(destination.name + ".atlas-part")
        if destination.exists():
            if destination.stat().st_size != expected_bytes or _file_digest(destination) != expected_sha256:
                raise ValueError("EXISTING_PERMANENT_FILE_CONFLICT")
            return destination
        offset = part.stat().st_size if part.exists() else 0
        if offset > expected_bytes:
            raise ValueError("INTERRUPTED_FRAGMENT_TOO_LARGE")
        # Resume only if every preserved prefix byte matches the genuine source.
        with source.open("rb") as src:
            if offset:
                with part.open("rb") as previous:
                    left = offset
                    while left:
                        count = min(left, 1 << 20)
                        if src.read(count) != previous.read(count):
                            raise ValueError("INTERRUPTED_FRAGMENT_PREFIX_CONFLICT")
                        left -= count
            with part.open("ab") as output:
                written = offset
                while block := src.read(1 << 20):
                    written += len(block)
                    if written > expected_bytes:
                        raise StorageBudgetExceeded("DOWNLOAD_EXCEEDS_RESERVED_BYTES")
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
        digest = hashlib.sha256()
        with part.open("rb") as handle:
            while block := handle.read(1 << 20):
                digest.update(block)
        if written != expected_bytes or digest.hexdigest() != expected_sha256:
            raise ValueError("CHECKPOINT_INTEGRITY_MISMATCH")
        part.replace(destination)
        return destination


def _file_digest(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()
