"""Versioned replay-availability policies; never overwrite actual receipt times."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum

from atlas.domain.enums import AvailabilityClass

from .models import MarketRecord, make_record


class AvailabilityRuleKind(StrEnum):
    BAR_END_PLUS_LAG = "BAR_END_PLUS_LAG"
    PUBLICATION_PLUS_LAG = "PUBLICATION_PLUS_LAG"
    SOURCE_EVENT_PLUS_LAG = "SOURCE_EVENT_PLUS_LAG"


@dataclass(frozen=True)
class ReplayAvailabilityRule:
    rule_id: str
    version: str
    kind: AvailabilityRuleKind
    lag_ns: int
    description: str

    def __post_init__(self):
        if not self.rule_id.strip() or not self.version.strip() or self.lag_ns < 0:
            raise ValueError("invalid replay rule")

    def hash(self) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "rule_id": self.rule_id,
                    "version": self.version,
                    "kind": self.kind.value,
                    "lag_ns": self.lag_ns,
                    "description": self.description,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def derive(self, r: MarketRecord) -> int:
        anchor = {
            "BAR_END_PLUS_LAG": r.bar_end_ns,
            "PUBLICATION_PLUS_LAG": r.source_published_at_ns,
            "SOURCE_EVENT_PLUS_LAG": r.source_event_at_ns,
        }[self.kind.value]
        if anchor is None:
            raise ValueError(f"{self.kind.value} anchor unavailable")
        return anchor + self.lag_ns


def reconstruct_public(record: MarketRecord, rule: ReplayAvailabilityRule) -> MarketRecord:
    replay = rule.derive(record)
    derived_id = f"{record.record_id}:replay:{rule.rule_id}@{rule.version}:{rule.hash()}"
    return make_record(
        record_id=derived_id,
        source_id=record.source_id,
        venue=record.venue,
        instrument=record.instrument,
        data_kind=record.data_kind,
        payload=record.payload,
        received_at_ns=record.received_at_ns,
        processed_at_ns=record.processed_at_ns,
        available_at_ns=record.available_at_ns,
        data_ingested_at_ns=record.data_ingested_at_ns,
        recorded_at_ns=record.recorded_at_ns,
        evidence_ref=f"{record.evidence_ref}:replay:{rule.hash()}",
        source_event_at_ns=record.source_event_at_ns,
        bar_start_ns=record.bar_start_ns,
        bar_end_ns=record.bar_end_ns,
        source_published_at_ns=record.source_published_at_ns,
        source_revision=record.source_revision,
        availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
        availability_lower_ns=replay,
        availability_upper_ns=replay,
        replay_available_at_ns=replay,
        availability_method=f"{rule.rule_id}@{rule.version}:{rule.hash()}",
        revision_of=record.revision_of,
        source_valid_from_ns=record.source_valid_from_ns,
        dependency_ids=record.dependency_ids,
        pipeline_version=record.pipeline_version,
        source_clock_precision_ns=record.source_clock_precision_ns,
        source_clock_uncertainty_ns=record.source_clock_uncertainty_ns,
    )


def historical_import(*, actual_ingested_at_ns: int, **kwargs) -> MarketRecord:
    """Backfill factory: actual receipt/ingest time is the import time, never backdated."""
    supplied = kwargs.pop("received_at_ns", actual_ingested_at_ns)
    if supplied != actual_ingested_at_ns:
        raise ValueError("historical import may not backdate received_at")
    processed = kwargs.pop("processed_at_ns", actual_ingested_at_ns)
    available = kwargs.pop("available_at_ns", max(actual_ingested_at_ns, processed))
    recorded = kwargs.pop("recorded_at_ns", max(available, actual_ingested_at_ns))
    return make_record(
        received_at_ns=actual_ingested_at_ns,
        data_ingested_at_ns=actual_ingested_at_ns,
        processed_at_ns=processed,
        available_at_ns=available,
        recorded_at_ns=recorded,
        **kwargs,
    )
