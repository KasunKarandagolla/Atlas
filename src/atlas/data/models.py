"""Canonical immutable market records for Phase 3 causal archives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.domain.enums import AvailabilityClass
from atlas.domain.time import ensure_utc_ns


class DataKind(StrEnum):
    INSTRUMENT_METADATA = "instrument_metadata"
    RISK_TIER = "risk_tier"
    BAR_1H_LAST = "bar_1h_last"
    BAR_1M_LAST = "bar_1m_last"
    BAR_1M_MARK = "bar_1m_mark"
    BAR_1M_INDEX = "bar_1m_index"
    QUOTE_TOP = "quote_top"
    DEPTH = "depth"
    TRADE = "trade"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"


class ReplayMode(StrEnum):
    ACTUAL_SYSTEM = "ACTUAL_SYSTEM"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"


V1_INSTRUMENTS = frozenset({"BTCUSDT", "ETHUSDT"})


def canonical_json(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def hash_payload(v: Any) -> str:
    return hashlib.sha256(canonical_json(v).encode()).hexdigest()


@dataclass(frozen=True)
class MarketRecord:
    record_id: str
    source_id: str
    venue: str
    instrument: str
    data_kind: DataKind
    source_event_at_ns: int | None
    bar_start_ns: int | None
    bar_end_ns: int | None
    source_published_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    processed_at_ns: int
    source_revision: str | None
    availability_class: AvailabilityClass
    availability_lower_ns: int | None
    availability_upper_ns: int | None
    replay_available_at_ns: int | None
    availability_method: str | None
    evidence_ref: str
    data_ingested_at_ns: int
    content_hash: str
    revision_of: str | None
    source_valid_from_ns: int | None
    recorded_at_ns: int
    dependency_ids: tuple[str, ...]
    pipeline_version: str
    source_clock_precision_ns: int
    source_clock_uncertainty_ns: int
    payload: dict[str, Any]

    def __post_init__(self):
        for n in ("record_id", "source_id", "venue", "instrument", "evidence_ref", "pipeline_version", "content_hash"):
            if not isinstance(getattr(self, n), str) or not getattr(self, n).strip():
                raise ValueError(f"{n} must be non-blank")
        if self.instrument not in V1_INSTRUMENTS:
            raise ValueError("Phase 3 V1 instrument scope is BTCUSDT/ETHUSDT")
        for n in (
            "received_at_ns",
            "available_at_ns",
            "processed_at_ns",
            "data_ingested_at_ns",
            "recorded_at_ns",
            "source_clock_precision_ns",
            "source_clock_uncertainty_ns",
        ):
            ensure_utc_ns(getattr(self, n), field=n)
        for n in (
            "source_event_at_ns",
            "bar_start_ns",
            "bar_end_ns",
            "source_published_at_ns",
            "availability_lower_ns",
            "availability_upper_ns",
            "replay_available_at_ns",
            "source_valid_from_ns",
        ):
            v = getattr(self, n)
            if v is not None:
                ensure_utc_ns(v, field=n)
        if self.bar_start_ns is not None and self.bar_end_ns is not None and self.bar_end_ns <= self.bar_start_ns:
            raise ValueError("bar_end must be after bar_start")
        if (
            self.source_event_at_ns is not None
            and self.source_published_at_ns is not None
            and self.source_published_at_ns < self.source_event_at_ns
        ):
            raise ValueError("publication precedes source event")
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("available_at precedes received_at")
        if self.processed_at_ns < self.received_at_ns:
            raise ValueError("processed_at precedes received_at")
        if self.available_at_ns < self.processed_at_ns:
            raise ValueError("available_at precedes processed_at")
        if self.data_ingested_at_ns < self.received_at_ns:
            raise ValueError("data_ingested_at cannot precede receipt")
        if self.recorded_at_ns < self.processed_at_ns:
            raise ValueError("recorded_at cannot precede processing")
        if (
            self.availability_lower_ns is not None
            and self.availability_upper_ns is not None
            and self.availability_upper_ns < self.availability_lower_ns
        ):
            raise ValueError("availability bounds invalid")
        if self.availability_class == AvailabilityClass.ACTUAL_OBSERVED and self.replay_available_at_ns is not None:
            raise ValueError("actual observation must not carry reconstructed replay time")
        if self.availability_class == AvailabilityClass.RECONSTRUCTED_PUBLIC:
            if self.replay_available_at_ns is None or not self.availability_method:
                raise ValueError("reconstructed record requires replay time/method")
            if self.received_at_ns <= self.replay_available_at_ns:
                raise ValueError(
                    "historical received_at fabrication: actual receipt must be after reconstructed replay time"
                )
        if self.content_hash != self.compute_content_hash():
            raise ValueError("content_hash mismatch")
        object.__setattr__(self, "dependency_ids", tuple(self.dependency_ids))
        object.__setattr__(self, "payload", dict(self.payload))

    def content_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "venue": self.venue,
            "instrument": self.instrument,
            "data_kind": self.data_kind.value,
            "source_event_at_ns": self.source_event_at_ns,
            "bar_start_ns": self.bar_start_ns,
            "bar_end_ns": self.bar_end_ns,
            "source_published_at_ns": self.source_published_at_ns,
            "source_revision": self.source_revision,
            "revision_of": self.revision_of,
            "source_valid_from_ns": self.source_valid_from_ns,
            "payload": self.payload,
        }

    def compute_content_hash(self) -> str:
        return hash_payload(self.content_dict())

    def immutable_dict(self) -> dict[str, Any]:
        """Canonical causal identity, excluding the derived fingerprint itself."""

        d = {field: getattr(self, field) for field in self.__dataclass_fields__}
        d["data_kind"] = self.data_kind.value
        d["availability_class"] = self.availability_class.value
        d["dependency_ids"] = list(self.dependency_ids)
        return d

    def compute_record_fingerprint(self) -> str:
        return hash_payload(self.immutable_dict())

    @property
    def record_fingerprint(self) -> str:
        return self.compute_record_fingerprint()

    def to_dict(self) -> dict[str, Any]:
        d = self.immutable_dict()
        d["record_fingerprint"] = self.record_fingerprint
        return d


def make_record(
    *,
    record_id: str,
    source_id: str,
    venue: str,
    instrument: str,
    data_kind: DataKind,
    payload: dict[str, Any],
    received_at_ns: int,
    processed_at_ns: int,
    available_at_ns: int,
    data_ingested_at_ns: int,
    recorded_at_ns: int,
    evidence_ref: str,
    source_event_at_ns: int | None = None,
    bar_start_ns: int | None = None,
    bar_end_ns: int | None = None,
    source_published_at_ns: int | None = None,
    source_revision: str | None = None,
    availability_class: AvailabilityClass = AvailabilityClass.ACTUAL_OBSERVED,
    availability_lower_ns: int | None = None,
    availability_upper_ns: int | None = None,
    replay_available_at_ns: int | None = None,
    availability_method: str | None = None,
    revision_of: str | None = None,
    source_valid_from_ns: int | None = None,
    dependency_ids: tuple[str, ...] = (),
    pipeline_version: str = "phase3-v1",
    source_clock_precision_ns: int = 1_000_000,
    source_clock_uncertainty_ns: int = 0,
) -> MarketRecord:
    base = {
        "source_id": source_id,
        "venue": venue,
        "instrument": instrument,
        "data_kind": data_kind.value,
        "source_event_at_ns": source_event_at_ns,
        "bar_start_ns": bar_start_ns,
        "bar_end_ns": bar_end_ns,
        "source_published_at_ns": source_published_at_ns,
        "source_revision": source_revision,
        "revision_of": revision_of,
        "source_valid_from_ns": source_valid_from_ns,
        "payload": payload,
    }
    return MarketRecord(
        record_id,
        source_id,
        venue,
        instrument,
        data_kind,
        source_event_at_ns,
        bar_start_ns,
        bar_end_ns,
        source_published_at_ns,
        received_at_ns,
        available_at_ns,
        processed_at_ns,
        source_revision,
        availability_class,
        availability_lower_ns,
        availability_upper_ns,
        replay_available_at_ns,
        availability_method,
        evidence_ref,
        data_ingested_at_ns,
        hash_payload(base),
        revision_of,
        source_valid_from_ns,
        recorded_at_ns,
        dependency_ids,
        pipeline_version,
        source_clock_precision_ns,
        source_clock_uncertainty_ns,
        payload,
    )
