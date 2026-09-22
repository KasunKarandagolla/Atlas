"""Append-only Parquet research artifacts; never SQLite/live-control state."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def _canonical(value: Any) -> str:
    if hasattr(value, "__dataclass_fields__"):
        value = {name: getattr(value, name) for name in value.__dataclass_fields__}
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class ResearchArtifactArchive:
    def __init__(self, root: Path):
        self.root = root

    def append(self, artifact_type: str, artifact: Any) -> Path:
        """Write once under content hash; conflict is evidence corruption, not overwrite."""
        if not artifact_type or "/" in artifact_type:
            raise ValueError("safe artifact type required")
        payload = _canonical(artifact)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        path = self.root / artifact_type / f"part-{digest}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return path
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - validated in locked env
            raise RuntimeError("pyarrow is required for immutable research persistence") from exc
        temp = path.with_suffix(".tmp")
        pq.write_table(pa.table({"payload_json": [payload], "sha256": [digest]}), temp)
        temp.replace(path)
        return path
