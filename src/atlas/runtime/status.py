"""Sanitized atomic runtime status snapshots."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path


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
    generated_at_ns: int
    runtime_instance_id: str
    writer_id: str
    schema_version: int = 1


def load_status(path: str | Path) -> RuntimeStatus:
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    return RuntimeStatus(
        d["runtime_state"],
        d["writer_epoch"],
        d["journal_healthy"],
        d["reconciliation_health"],
        d["unresolved_intents"],
        d.get("unknown_commands", 0),
        d.get("unresolved_commands", 0),
        d["capability_hash"],
        d["all_qualified"],
        d["assisted_enabled"],
        d.get("generated_at_ns", 0),
        d.get("runtime_instance_id", ""),
        d.get("writer_id", ""),
        d.get("schema_version", 1),
    )


def publish_status(path: str | Path, status: RuntimeStatus) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(asdict(status), sort_keys=True, separators=(",", ":")) + "\n"
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", dir=p.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
        d = os.open(p.parent, os.O_RDONLY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
