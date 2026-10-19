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
