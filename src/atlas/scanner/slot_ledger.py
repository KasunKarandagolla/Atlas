"""Durable registration and progress records for scheduled scanner slots.

The ledger is additive to the immutable per-instrument decision calendar. A
registration is written before any slot input provider is called, so a source
failure or process exit cannot make a scheduled slot disappear from the
denominator. Records use the existing content-addressed research archive and
never alter the slot's research or execution authority.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas.science.research_archive import ResearchArtifactArchive

from .models import SLOT_NS, ScannerPolicy, content_hash

SLOT_REGISTRATION = "scanner_slot_registration"
SLOT_PROGRESS = "scanner_slot_progress"
SCANNER_SCHEDULER_ID = "ATLAS_PHASE5_FOUR_HOURLY_UTC_V1"
ELIGIBLE_SCOPE_CONTRACT = "ELIGIBLE_ENTRIES_FROM_CAUSAL_UNIVERSE_SNAPSHOT_V1"


class ScannerSlotLedger:
    """Append-only slot registration/progress ledger backed by the research archive."""

    def __init__(self, archive: ResearchArtifactArchive):
        self.archive = archive

    @staticmethod
    def _read_family(root: Path, family: str) -> tuple[dict[str, Any], ...]:
        directory = root / family
        if not directory.exists():
            return ()
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - archive writer has same pinned requirement
            raise RuntimeError("pyarrow is required to recover scanner slot records") from exc
        records: list[dict[str, Any]] = []
        for path in sorted(directory.glob("part-*.parquet")):
            table = pq.read_table(path, columns=["payload_json", "sha256"])
            if table.num_rows != 1:
                raise ValueError("scanner slot archive record must contain exactly one row")
            payload = table.column("payload_json")[0].as_py()
            digest = table.column("sha256")[0].as_py()
            if not isinstance(payload, str) or not isinstance(digest, str):
                raise ValueError("scanner slot archive record has invalid payload types")
            if content_hash(json.loads(payload)) != digest or path.stem != f"part-{digest}":
                raise ValueError("scanner slot archive record hash mismatch")
            value = json.loads(payload)
            if not isinstance(value, dict):
                raise ValueError("scanner slot archive record must be an object")
            records.append(value)
        return tuple(records)

    def registrations(self) -> tuple[dict[str, Any], ...]:
        rows = self._read_family(self.archive.root, SLOT_REGISTRATION)
        return tuple(sorted(rows, key=lambda row: (int(row.get("due_at_ns", -1)),
                                                   int(row.get("registered_at_ns", -1)),
                                                   content_hash(row))))

    def progress_records(self) -> tuple[dict[str, Any], ...]:
        return self._read_family(self.archive.root, SLOT_PROGRESS)

    def _register(self, *, slot_at_ns: int, policy: ScannerPolicy, registered_at_ns: int,
                  registration_kind: str) -> str:
        if slot_at_ns < 0 or slot_at_ns % SLOT_NS:
            raise ValueError("scanner slot registration requires a four-hour UTC slot")
        if registered_at_ns < 0:
            raise ValueError("scanner slot registration time must be UTC nanoseconds")
        if registration_kind == "DUE_RUNNER" and registered_at_ns < slot_at_ns:
            raise ValueError("scanner slot cannot be registered before its due time")
        if registration_kind == "PROSPECTIVE_SCHEDULE" and registered_at_ns >= slot_at_ns:
            raise ValueError("prospective scanner slot must be registered before its due time")
        if registration_kind not in {"DUE_RUNNER", "PROSPECTIVE_SCHEDULE"}:
            raise ValueError("invalid scanner registration kind")
        slot_id = f"scan-{slot_at_ns}"
        existing = tuple(row for row in self.registrations() if row.get("scan_slot_id") == slot_id)
        for row in existing:
            if (row.get("scheduler_id") != SCANNER_SCHEDULER_ID
                    or row.get("due_at_ns") != slot_at_ns
                    or row.get("deadline_at_ns") != slot_at_ns + SLOT_NS
                    or row.get("scanner_policy_version") != policy.policy_version
                    or row.get("scanner_policy_hash") != policy.hash()
                    or row.get("eligible_universe_scope") != ELIGIBLE_SCOPE_CONTRACT):
                raise ValueError("scanner slot registration conflicts with its immutable policy or scope")
        if existing:
            # The first content-addressed registration is the slot's stable
            # identity. A retry after restart must not create a second record
            # just because its observed registration time has advanced.
            original = min(existing, key=lambda row: (int(row.get("registered_at_ns", 0)), content_hash(row)))
            return content_hash(original)
        body = {
            "schema_version": 1,
            "scan_slot_id": slot_id,
            "scheduler_id": SCANNER_SCHEDULER_ID,
            "due_at_ns": slot_at_ns,
            "deadline_at_ns": slot_at_ns + SLOT_NS,
            "scanner_policy_version": policy.policy_version,
            "scanner_policy_hash": policy.hash(),
            "eligible_universe_scope": ELIGIBLE_SCOPE_CONTRACT,
            "registered_at_ns": registered_at_ns,
            "registration_kind": registration_kind,
            "status": "REGISTERED",
        }
        self.archive.append(SLOT_REGISTRATION, body)
        return content_hash(body)

    def register(self, *, slot_at_ns: int, policy: ScannerPolicy, registered_at_ns: int) -> str:
        """Register a slot when its scheduled time has arrived."""
        return self._register(slot_at_ns=slot_at_ns, policy=policy, registered_at_ns=registered_at_ns,
                              registration_kind="DUE_RUNNER")

    def register_prospective(self, *, slot_at_ns: int, policy: ScannerPolicy,
                             registered_at_ns: int) -> str:
        """Register a future expected slot; never backfills an unobserved past slot."""
        return self._register(slot_at_ns=slot_at_ns, policy=policy, registered_at_ns=registered_at_ns,
                              registration_kind="PROSPECTIVE_SCHEDULE")

    def record_progress(
        self,
        *,
        registration_ref: str,
        slot_at_ns: int,
        attempt: int,
        status: str,
        observed_at_ns: int,
        universe_ref: str | None = None,
        eligible_instruments: tuple[str, ...] = (),
        result_ref: str | None = None,
        reason_code: str | None = None,
    ) -> str:
        allowed = {"STARTED", "UNIVERSE_AVAILABLE", "COMPLETED", "FAILED", "SOURCE_UNAVAILABLE",
                   "MISSED", "EXPIRED"}
        if status not in allowed:
            raise ValueError("invalid scanner slot progress status")
        if attempt < 1 or observed_at_ns < slot_at_ns:
            raise ValueError("invalid scanner slot progress chronology")
        if eligible_instruments != tuple(sorted(set(eligible_instruments))):
            raise ValueError("scanner slot universe scope must be sorted and unique")
        registrations = tuple(row for row in self.registrations()
                              if row.get("scan_slot_id") == f"scan-{slot_at_ns}")
        if not registrations or not any(content_hash(row) == registration_ref for row in registrations):
            raise ValueError("scanner progress must reference its durable slot registration")
        if observed_at_ns < min(int(row["registered_at_ns"]) for row in registrations):
            raise ValueError("scanner progress cannot precede slot registration")
        if status == "COMPLETED" and (universe_ref is None or result_ref is None):
            raise ValueError("completed scanner slots require universe and result references")
        if status in {"FAILED", "SOURCE_UNAVAILABLE", "MISSED", "EXPIRED"} and not reason_code:
            raise ValueError("terminal missing scanner slots require a reason code")
        body = {
            "schema_version": 1,
            "registration_ref": registration_ref,
            "scan_slot_id": f"scan-{slot_at_ns}",
            "attempt": attempt,
            "status": status,
            "observed_at_ns": observed_at_ns,
            "universe_ref": universe_ref,
            "eligible_instruments": list(eligible_instruments),
            "result_ref": result_ref,
            "reason_code": reason_code,
        }
        if any(content_hash(row) == content_hash(body) for row in self.progress_records()):
            return content_hash(body)
        self.archive.append(SLOT_PROGRESS, body)
        return content_hash(body)

    def latest_progress(self, slot_at_ns: int) -> dict[str, Any] | None:
        rows = [row for row in self.progress_records() if row.get("scan_slot_id") == f"scan-{slot_at_ns}"]
        stage_order = {"STARTED": 0, "UNIVERSE_AVAILABLE": 1, "FAILED": 2, "SOURCE_UNAVAILABLE": 2,
                       "MISSED": 2, "EXPIRED": 2, "COMPLETED": 3}
        return max(rows, key=lambda row: (int(row.get("attempt", 0)), int(row.get("observed_at_ns", -1)),
                                          stage_order.get(str(row.get("status")), -1), content_hash(row))) \
            if rows else None

    def has_completed(self, slot_at_ns: int) -> bool:
        latest = self.latest_progress(slot_at_ns)
        return latest is not None and latest.get("status") == "COMPLETED"

    def mark_overdue_unfinished(self, *, now_ns: int) -> tuple[int, ...]:
        """Durably mark registered, past-deadline work that has no observed terminal result."""
        if now_ns < 0:
            raise ValueError("reconciliation time must be UTC nanoseconds")
        marked: list[int] = []
        for registration in sorted(self.registrations(), key=lambda row: int(row["due_at_ns"])):
            slot_at_ns = int(registration["due_at_ns"])
            if int(registration["deadline_at_ns"]) > now_ns:
                continue
            latest = self.latest_progress(slot_at_ns)
            if latest is not None and latest.get("status") in {
                "COMPLETED", "FAILED", "SOURCE_UNAVAILABLE", "MISSED", "EXPIRED"
            }:
                continue
            attempt = int(latest["attempt"]) if latest is not None else 1
            self.record_progress(
                registration_ref=content_hash(registration), slot_at_ns=slot_at_ns,
                attempt=attempt, status="MISSED", observed_at_ns=now_ns,
                reason_code="REGISTERED_SLOT_DEADLINE_ELAPSED",
            )
            marked.append(slot_at_ns)
        return tuple(marked)

    def expected_slot_summary(self, *, as_of_ns: int) -> dict[str, int]:
        """Summarize only registered due slots; unregistered history is not inferred."""
        if as_of_ns < 0:
            raise ValueError("summary time must be UTC nanoseconds")
        registrations = {int(row["due_at_ns"]): row for row in self.registrations()}
        due_slots = {slot for slot in registrations if slot <= as_of_ns}
        future_slots = set(registrations) - due_slots
        statuses: dict[str, int] = {}
        for slot in due_slots:
            latest = self.latest_progress(slot)
            status = "REGISTERED" if latest is None else str(latest.get("status"))
            statuses[status] = statuses.get(status, 0) + 1
        return {
            "as_of_ns": as_of_ns,
            "expected_due_slots": len(due_slots),
            "completed_slots": statuses.get("COMPLETED", 0),
            "missed_slots": statuses.get("MISSED", 0),
            "failed_slots": statuses.get("FAILED", 0),
            "source_unavailable_slots": statuses.get("SOURCE_UNAVAILABLE", 0),
            "expired_slots": statuses.get("EXPIRED", 0),
            "incomplete_slots": sum(statuses.get(status, 0) for status in
                                     ("REGISTERED", "STARTED", "UNIVERSE_AVAILABLE")),
            "future_registered_slots": len(future_slots),
        }
