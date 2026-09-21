"""Sanitized ops status boundary (atlas-ops must never receive credentials).

Atomic local snapshot transport: write temp file + fsync + os.replace so a
partial/failed write can never masquerade as fresh. Status exposes only
sanitized fields; API keys, secrets, signed requests and private account
identifiers are never present.

Freshness: status includes schema version, generated_at timestamp, runtime
instance ID, and writer epoch for staleness detection.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FORBIDDEN_STATUS_FIELDS = frozenset(
    {
        "api_key",
        "api_secret",
        "secret",
        "token",
        "signature",
        "signed_request",
        "account_id",
        "private",
    }
)

STATUS_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class RuntimeStatus:
    runtime_state: str
    writer_epoch: int
    journal_healthy: bool
    reconciliation_health: str
    unresolved_intents: int
    unknown_commands: int
    unresolved_commands: int
    capability_hash: str
    all_qualified: bool
    assisted_enabled: bool
    # Freshness metadata
    schema_version: str = STATUS_SCHEMA_VERSION
    generated_at_ns: int = 0
    runtime_instance_id: str = ""
    writer_id: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.runtime_state, str) or not self.runtime_state.strip():
            raise ValueError("runtime_state must be non-blank")
        if not isinstance(self.writer_epoch, int) or isinstance(self.writer_epoch, bool):
            raise ValueError("writer_epoch must be int")
        for f in ("journal_healthy", "all_qualified", "assisted_enabled"):
            if not isinstance(getattr(self, f), bool):
                raise ValueError(f"{f} must be bool")
        for f in ("reconciliation_health", "capability_hash"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be non-blank")
        for f in ("unresolved_intents", "unknown_commands", "unresolved_commands"):
            v = getattr(self, f)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{f} must be int >= 0")
        if not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise ValueError("schema_version must be non-blank")
        if not isinstance(self.generated_at_ns, int) or isinstance(self.generated_at_ns, bool):
            raise ValueError("generated_at_ns must be int")
        if not isinstance(self.runtime_instance_id, str) or not self.runtime_instance_id.strip():
            raise ValueError("runtime_instance_id must be non-blank")
        if not isinstance(self.writer_id, str) or not self.writer_id.strip():
            raise ValueError("writer_id must be non-blank")

    def to_dict(self) -> dict[str, Any]:
        d = {
            "schema_version": self.schema_version,
            "generated_at_ns": self.generated_at_ns,
            "runtime_instance_id": self.runtime_instance_id,
            "writer_id": self.writer_id,
            "writer_epoch": self.writer_epoch,
            "runtime_state": self.runtime_state,
            "journal_healthy": self.journal_healthy,
            "reconciliation_health": self.reconciliation_health,
            "unresolved_intents": self.unresolved_intents,
            "unknown_commands": self.unknown_commands,
            "unresolved_commands": self.unresolved_commands,
            "capability_hash": self.capability_hash,
            "all_qualified": self.all_qualified,
            "assisted_enabled": self.assisted_enabled,
        }
        for key in d:
            if key.lower() in FORBIDDEN_STATUS_FIELDS:
                raise ValueError(f"forbidden status field: {key}")
        return d

    def is_fresh(self, now_ns: int, ttl_ns: int) -> bool:
        """Check if status is fresh within TTL."""
        if self.generated_at_ns <= 0:
            return False  # Not initialized
        if now_ns < self.generated_at_ns:
            return False  # Future timestamp - clock conflict
        return (now_ns - self.generated_at_ns) <= ttl_ns


def publish_status(
    path: str | Path,
    status: RuntimeStatus,
) -> None:
    """Atomically publish status JSON (temp + fsync + rename).

    Status must already have freshness metadata (generated_at_ns, runtime_instance_id, writer_id).
    """
    from atlas.domain.time import now_ns

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Ensure freshness metadata is set
    if status.generated_at_ns <= 0:
        status = RuntimeStatus(
            runtime_state=status.runtime_state,
            writer_epoch=status.writer_epoch,
            journal_healthy=status.journal_healthy,
            reconciliation_health=status.reconciliation_health,
            unresolved_intents=status.unresolved_intents,
            unknown_commands=status.unknown_commands,
            unresolved_commands=status.unresolved_commands,
            capability_hash=status.capability_hash,
            all_qualified=status.all_qualified,
            assisted_enabled=status.assisted_enabled,
            schema_version=status.schema_version,
            generated_at_ns=now_ns(),
            runtime_instance_id=status.runtime_instance_id or uuid.uuid4().hex,
            writer_id=status.writer_id or "unknown",
        )

    payload = json.dumps(status.to_dict(), sort_keys=True, separators=(",", ":"))
    fd, tmp = tempfile.mkstemp(dir=str(target.parent), prefix=".status-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    finally:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass


def load_status(path: str | Path) -> RuntimeStatus:
    raw = Path(path).read_text(encoding="utf-8")
    d = json.loads(raw)
    return RuntimeStatus(
        runtime_state=d["runtime_state"],
        writer_epoch=d["writer_epoch"],
        journal_healthy=d["journal_healthy"],
        reconciliation_health=d["reconciliation_health"],
        unresolved_intents=d["unresolved_intents"],
        unknown_commands=d.get("unknown_commands", 0),
        unresolved_commands=d.get("unresolved_commands", 0),
        capability_hash=d["capability_hash"],
        all_qualified=d["all_qualified"],
        assisted_enabled=d["assisted_enabled"],
        schema_version=d.get("schema_version", STATUS_SCHEMA_VERSION),
        generated_at_ns=d.get("generated_at_ns", 0),
        runtime_instance_id=d.get("runtime_instance_id", ""),
        writer_id=d.get("writer_id", ""),
    )
