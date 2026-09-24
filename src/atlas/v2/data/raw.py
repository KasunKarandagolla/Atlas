"""Strict immutable raw observation records for public V2 data."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .._serialization import canonical_json, nonblank, sha256_json, sha256_ref, strict_fields, string_tuple, timestamp


class AvailabilityClassV2(StrEnum):
    ACTUAL_SYSTEM = "ACTUAL_SYSTEM"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"
    DIAGNOSTIC_NONCAUSAL = "DIAGNOSTIC_NONCAUSAL"
    UNKNOWN = "UNKNOWN"


class AppendStatusV2(StrEnum):
    INSERTED = "INSERTED"
    DUPLICATE = "DUPLICATE"
    CONFLICT_QUARANTINED = "CONFLICT_QUARANTINED"


def _identity(record: RawObservationV2) -> str:
    event_identity: dict[str, Any]
    if record.sequence is not None:
        event_identity = {"sequence": record.sequence, "revision_of": record.revision_of}
    else:
        event_identity = {"event_at_ns": record.event_at_ns, "revision_of": record.revision_of}
    return sha256_json(
        {
            "schema_version": 1,
            "source_id": record.source_id,
            "instrument_revision": record.instrument_revision,
            "event_type": record.event_type,
            "event_identity": event_identity,
        }
    )


@dataclass(frozen=True)
class RawObservationV2:
    record_id: str
    instrument_revision: str
    source_id: str
    event_type: str
    event_at_ns: int | None
    published_at_ns: int | None
    received_at_ns: int
    ingested_at_ns: int
    available_at_ns: int
    raw_payload_hash: str
    translation_version: str
    revision_of: str | None
    quality_flags: tuple[str, ...]
    availability_class: AvailabilityClassV2
    sequence: str | int | None = None
    replay_available_at_ns: int | None = None

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        sha256_ref(self.instrument_revision, field="instrument_revision")
        nonblank(self.source_id, field="source_id")
        nonblank(self.event_type, field="event_type")
        nonblank(self.translation_version, field="translation_version")
        sha256_ref(self.raw_payload_hash, field="raw_payload_hash")
        for field_name in ("event_at_ns", "published_at_ns", "replay_available_at_ns"):
            value = getattr(self, field_name)
            if value is not None:
                timestamp(value, field=field_name)
        for field_name in ("received_at_ns", "ingested_at_ns", "available_at_ns"):
            timestamp(getattr(self, field_name), field=field_name)
        if self.ingested_at_ns < self.received_at_ns:
            raise ValueError("ingested_at_ns cannot precede actual receipt")
        if self.available_at_ns < max(self.received_at_ns, self.ingested_at_ns):
            raise ValueError("available_at_ns cannot precede receipt or validation/ingestion")
        if self.sequence is not None and (isinstance(self.sequence, bool) or not isinstance(self.sequence, (str, int))):
            raise ValueError("sequence must be an integer or string")
        if isinstance(self.sequence, str):
            nonblank(self.sequence, field="sequence")
        if isinstance(self.sequence, int) and self.sequence < 0:
            raise ValueError("sequence cannot be negative")
        if self.revision_of is not None:
            sha256_ref(self.revision_of, field="revision_of")
        flags = string_tuple(self.quality_flags, field="quality_flags", sorted_unique=True)
        object.__setattr__(self, "quality_flags", flags)
        try:
            availability = AvailabilityClassV2(self.availability_class)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"unknown availability_class: {self.availability_class!r}") from exc
        object.__setattr__(self, "availability_class", availability)
        if availability == AvailabilityClassV2.ACTUAL_SYSTEM and self.replay_available_at_ns is not None:
            raise ValueError("ACTUAL_SYSTEM records cannot carry reconstructed replay availability")
        if availability == AvailabilityClassV2.RECONSTRUCTED_MARKET and self.replay_available_at_ns is None:
            raise ValueError("RECONSTRUCTED_MARKET requires separate replay availability")
        expected = _identity(self)
        if self.record_id != expected:
            raise ValueError("record_id does not match deterministic event identity")

    @classmethod
    def build(
        cls,
        *,
        instrument_revision: str,
        source_id: str,
        event_type: str,
        received_at_ns: int,
        ingested_at_ns: int,
        available_at_ns: int,
        translation_version: str,
        payload: bytes | str | Mapping[str, Any] | list[Any],
        event_at_ns: int | None = None,
        published_at_ns: int | None = None,
        revision_of: str | None = None,
        quality_flags: tuple[str, ...] = (),
        availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
        sequence: str | int | None = None,
        replay_available_at_ns: int | None = None,
    ) -> RawObservationV2:
        if isinstance(payload, bytes):
            payload_bytes = payload
        elif isinstance(payload, str):
            payload_bytes = payload.encode("utf-8")
        else:
            payload_bytes = canonical_json(payload).encode("utf-8")
        provisional = cls.__new__(cls)
        values = {
            "instrument_revision": instrument_revision,
            "source_id": source_id,
            "event_type": event_type,
            "event_at_ns": event_at_ns,
            "published_at_ns": published_at_ns,
            "received_at_ns": received_at_ns,
            "ingested_at_ns": ingested_at_ns,
            "available_at_ns": available_at_ns,
            "raw_payload_hash": hashlib.sha256(payload_bytes).hexdigest(),
            "translation_version": translation_version,
            "revision_of": revision_of,
            "quality_flags": quality_flags,
            "availability_class": availability_class,
            "sequence": sequence,
            "replay_available_at_ns": replay_available_at_ns,
        }
        for name, value in values.items():
            object.__setattr__(provisional, name, value)
        object.__setattr__(provisional, "record_id", _identity(provisional))
        provisional.__post_init__()
        return provisional

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "record_id": self.record_id,
            "instrument_revision": self.instrument_revision,
            "source_id": self.source_id,
            "event_type": self.event_type,
            "sequence": self.sequence,
            "event_at_ns": self.event_at_ns,
            "published_at_ns": self.published_at_ns,
            "received_at_ns": self.received_at_ns,
            "ingested_at_ns": self.ingested_at_ns,
            "available_at_ns": self.available_at_ns,
            "raw_payload_hash": self.raw_payload_hash,
            "translation_version": self.translation_version,
            "revision_of": self.revision_of,
            "quality_flags": list(self.quality_flags),
            "availability_class": self.availability_class.value,
            "replay_available_at_ns": self.replay_available_at_ns,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "RawObservationV2", "observation": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RawObservationV2:
        fields = {
            "schema_version", "record_id", "instrument_revision", "source_id", "event_type", "sequence", "event_at_ns",
            "published_at_ns", "received_at_ns", "ingested_at_ns", "available_at_ns", "raw_payload_hash",
            "translation_version", "revision_of", "quality_flags", "availability_class", "replay_available_at_ns",
        }
        d = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported RawObservationV2 schema_version")
        if not isinstance(d["quality_flags"], list):
            raise ValueError("quality_flags must be an array")
        return cls(
            record_id=d["record_id"], instrument_revision=d["instrument_revision"], source_id=d["source_id"],
            event_type=d["event_type"], event_at_ns=d["event_at_ns"], published_at_ns=d["published_at_ns"],
            received_at_ns=d["received_at_ns"], ingested_at_ns=d["ingested_at_ns"], available_at_ns=d["available_at_ns"],
            raw_payload_hash=d["raw_payload_hash"], translation_version=d["translation_version"], revision_of=d["revision_of"],
            quality_flags=tuple(d["quality_flags"]), availability_class=AvailabilityClassV2(d["availability_class"]),
            sequence=d["sequence"], replay_available_at_ns=d["replay_available_at_ns"],
        )


@dataclass(frozen=True)
class AppendResultV2:
    status: AppendStatusV2
    stored: RawObservationV2
    incoming: RawObservationV2


class RawObservationStoreV2:
    """Idempotent append index; conflicting identities remain explicitly quarantined."""

    def __init__(self) -> None:
        self._records: dict[str, RawObservationV2] = {}
        self._quarantine: list[tuple[RawObservationV2, RawObservationV2]] = []

    def append(self, observation: RawObservationV2) -> AppendResultV2:
        current = self._records.get(observation.record_id)
        if current is None:
            self._records[observation.record_id] = observation
            return AppendResultV2(AppendStatusV2.INSERTED, observation, observation)
        if current.raw_payload_hash == observation.raw_payload_hash and (
            current.instrument_revision,
            current.source_id,
            current.event_type,
            current.sequence,
            current.event_at_ns,
            current.published_at_ns,
            current.translation_version,
            current.revision_of,
            current.quality_flags,
            current.availability_class,
            current.replay_available_at_ns,
        ) == (
            observation.instrument_revision,
            observation.source_id,
            observation.event_type,
            observation.sequence,
            observation.event_at_ns,
            observation.published_at_ns,
            observation.translation_version,
            observation.revision_of,
            observation.quality_flags,
            observation.availability_class,
            observation.replay_available_at_ns,
        ):
            return AppendResultV2(AppendStatusV2.DUPLICATE, current, observation)
        self._quarantine.append((current, observation))
        return AppendResultV2(AppendStatusV2.CONFLICT_QUARANTINED, current, observation)

    def get(self, record_id: str) -> RawObservationV2 | None:
        return self._records.get(record_id)

    def records(self) -> tuple[RawObservationV2, ...]:
        return tuple(self._records[key] for key in sorted(self._records))

    def quarantined_conflicts(self) -> tuple[tuple[RawObservationV2, RawObservationV2], ...]:
        return tuple(self._quarantine)
