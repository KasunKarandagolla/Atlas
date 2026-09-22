"""Deterministic data validation/quarantine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .models import MarketRecord


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    reasons: tuple[str, ...]


class CausalRecordValidator:
    def __init__(self):
        self._hash_by_id = {}
        self._known_ids = set()
        self._available = {}

    def validate(self, r: MarketRecord) -> ValidationResult:
        reasons = []
        old = self._hash_by_id.get(r.record_id)
        if old is not None and old != r.content_hash:
            reasons.append("duplicate record identity with conflicting content")
        if r.source_event_at_ns is not None and r.source_event_at_ns > r.received_at_ns + r.source_clock_uncertainty_ns:
            reasons.append("source event clock conflicts with actual receipt")
        if (
            r.source_published_at_ns is not None
            and r.source_published_at_ns > r.received_at_ns + r.source_clock_uncertainty_ns
        ):
            reasons.append("publication clock conflicts with actual receipt")
        if r.revision_of and r.revision_of not in self._known_ids:
            reasons.append("revision lineage parent unknown")
        missing = [d for d in r.dependency_ids if d not in self._available]
        if missing:
            reasons.append(f"missing dependency availability: {missing}")
        elif r.dependency_ids and r.available_at_ns < max(self._available[d] for d in r.dependency_ids):
            reasons.append("availability precedes dependency availability")
        return ValidationResult(not reasons, tuple(reasons))

    def accept(self, r: MarketRecord) -> ValidationResult:
        result = self.validate(r)
        if result.accepted:
            self._hash_by_id[r.record_id] = r.content_hash
            self._known_ids.add(r.record_id)
            self._available[r.record_id] = r.available_at_ns
        return result


class QuarantineStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, r: MarketRecord, reasons: tuple[str, ...]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "record_id": r.record_id,
                        "content_hash": r.content_hash,
                        "reasons": list(reasons),
                        "recorded_at_ns": r.recorded_at_ns,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
