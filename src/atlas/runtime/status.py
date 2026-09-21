"""Sanitized ops status boundary (atlas-ops must never receive credentials).

Atomic local snapshot transport: write temp file + fsync + os.replace so a
partial/failed write can never masquerade as fresh. Status exposes only
sanitized fields; API keys, secrets, signed requests and private account
identifiers are never present.
"""

from __future__ import annotations

import json
import os
import tempfile
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


@dataclass(frozen=True)
class RuntimeStatus:
    runtime_state: str
    writer_epoch: int
    journal_healthy: bool
    reconciliation_health: str
    unresolved_intents: int
    unknown_commands: int
    capability_hash: str
    all_qualified: bool
    assisted_enabled: bool

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
        for f in ("unresolved_intents", "unknown_commands"):
            v = getattr(self, f)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{f} must be int >= 0")

    def to_dict(self) -> dict[str, Any]:
        d = {
            "runtime_state": self.runtime_state,
            "writer_epoch": self.writer_epoch,
            "journal_healthy": self.journal_healthy,
            "reconciliation_health": self.reconciliation_health,
            "unresolved_intents": self.unresolved_intents,
            "unknown_commands": self.unknown_commands,
            "capability_hash": self.capability_hash,
            "all_qualified": self.all_qualified,
            "assisted_enabled": self.assisted_enabled,
        }
        for key in d:
            if key.lower() in FORBIDDEN_STATUS_FIELDS:
                raise ValueError(f"forbidden status field: {key}")
        return d


def publish_status(path: str | Path, status: RuntimeStatus) -> None:
    """Atomically publish status JSON (temp + fsync + rename)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
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
        unknown_commands=d["unknown_commands"],
        capability_hash=d["capability_hash"],
        all_qualified=d["all_qualified"],
        assisted_enabled=d["assisted_enabled"],
    )
