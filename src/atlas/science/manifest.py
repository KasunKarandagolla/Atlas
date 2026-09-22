"""Canonical immutable Phase-4 model manifest helper."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ModelManifest:
    values: dict[str, Any]

    def canonical_json(self) -> str:
        return json.dumps(self.values, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()
