"""InformationContract (freeze §4).

Immutable/versioned record carrying provenance + availability intervals.
Canonical time unit: UTC integer nanoseconds.

Validation enforces causality when values are present:
- available_at >= received_at (when both present)
- available_at >= processed_at (when both present; available only after stage completion)
- processed_at >= max dependency availability (checked via explicit map)
- source_published_at >= source_event_at (when both present)
- received_at >= source_published_at (when both present; receipt after publication)
- RECONSTRUCTED_PUBLIC must not fabricate historical received_at:
  requires replay_available_at + received_at + availability_method, and
  received_at must be strictly after replay_available_at (download today, replay historical).
- ACTUAL_OBSERVED requires received_at + available_at.
- Incompatible availability combinations fail explicitly.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .enums import AvailabilityClass
from .time import ensure_utc_ns


def _opt_ns(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    return ensure_utc_ns(value, field=field_name)


def _require_nonblank(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return value.strip()


@dataclass(frozen=True)
class InformationContract:
    contract_version: str
    source_id: str
    data_type: str
    units: str
    source_event_at_ns: int | None = None
    source_published_at_ns: int | None = None
    received_at_ns: int | None = None
    available_at_ns: int | None = None
    processed_at_ns: int | None = None
    source_revision: str | None = None
    availability_class: AvailabilityClass = AvailabilityClass.UNKNOWN
    availability_lower_ns: int | None = None
    availability_upper_ns: int | None = None
    replay_available_at_ns: int | None = None
    availability_method: str | None = None
    evidence_ref: str | None = None
    content_hash: str | None = None
    dependency_ids: tuple[str, ...] = ()
    pipeline_version: str = "1.0"
    quality: str = ""
    missingness: str = ""

    def __post_init__(self) -> None:
        _require_nonblank(self.contract_version, "contract_version")
        _require_nonblank(self.source_id, "source_id")
        _require_nonblank(self.data_type, "data_type")
        # units may be empty for unitless, but must be str
        if not isinstance(self.units, str):
            raise ValueError("units must be str")
        if not isinstance(self.availability_class, AvailabilityClass):
            raise ValueError(f"availability_class must be AvailabilityClass, got {self.availability_class!r}")

        for f in (
            "source_event_at_ns",
            "source_published_at_ns",
            "received_at_ns",
            "available_at_ns",
            "processed_at_ns",
            "availability_lower_ns",
            "availability_upper_ns",
            "replay_available_at_ns",
        ):
            v = getattr(self, f)
            if v is not None:
                ensure_utc_ns(v, field=f)

        object.__setattr__(self, "dependency_ids", tuple(self.dependency_ids))

        self._validate_causality()

    def _validate_causality(self) -> None:
        g = self
        # publication after event
        if g.source_event_at_ns is not None and g.source_published_at_ns is not None:
            if g.source_published_at_ns < g.source_event_at_ns:
                raise ValueError("source_published_at cannot precede source_event_at")
        # receipt after publication
        if g.source_published_at_ns is not None and g.received_at_ns is not None:
            if g.received_at_ns < g.source_published_at_ns:
                raise ValueError("received_at cannot precede source_published_at")
        # available at/after receipt
        if g.received_at_ns is not None and g.available_at_ns is not None:
            if g.available_at_ns < g.received_at_ns:
                raise ValueError("available_at cannot precede received_at")
        # available at/after stage completion
        if g.processed_at_ns is not None and g.available_at_ns is not None:
            if g.available_at_ns < g.processed_at_ns:
                raise ValueError("available_at cannot precede processed_at")
        # availability interval coherence
        if g.availability_lower_ns is not None and g.availability_upper_ns is not None:
            if g.availability_upper_ns < g.availability_lower_ns:
                raise ValueError("availability_upper cannot precede availability_lower")

        ac = g.availability_class
        if ac == AvailabilityClass.ACTUAL_OBSERVED:
            if g.received_at_ns is None or g.available_at_ns is None:
                raise ValueError("ACTUAL_OBSERVED requires received_at and available_at")
            # Actual observation must not carry a reconstructed replay schedule.
            if g.replay_available_at_ns is not None:
                raise ValueError("ACTUAL_OBSERVED must not carry replay_available_at")
        elif ac == AvailabilityClass.RECONSTRUCTED_PUBLIC:
            # Must not fabricate historical received_at: received is actual download (today),
            # replay is historical usability. Require both + method, and received > replay.
            if g.replay_available_at_ns is None:
                raise ValueError("RECONSTRUCTED_PUBLIC requires replay_available_at")
            if g.received_at_ns is None:
                raise ValueError("RECONSTRUCTED_PUBLIC requires actual received_at (download time)")
            if not (g.availability_method and g.availability_method.strip()):
                raise ValueError("RECONSTRUCTED_PUBLIC requires availability_method")
            if not (g.received_at_ns > g.replay_available_at_ns):
                raise ValueError(
                    "RECONSTRUCTED_PUBLIC must not fabricate historical received_at: "
                    "received_at must be strictly after replay_available_at"
                )
        elif ac == AvailabilityClass.REVISED_NO_VINTAGE:
            # Revised series without vintages cannot masquerade as original observation.
            if g.replay_available_at_ns is not None:
                raise ValueError("REVISED_NO_VINTAGE must not carry replay_available_at (no vintage)")
            if g.source_revision is not None and not str(g.source_revision).strip():
                raise ValueError("source_revision must be non-blank if supplied")
        elif ac == AvailabilityClass.UNKNOWN:
            pass
        else:  # pragma: no cover - enum exhaustive
            raise ValueError(f"unknown availability_class {ac!r}")

    def validate_dependencies(self, availability_by_id: Mapping[str, int]) -> None:
        """Derived data cannot be available before required dependencies.

        Caller supplies {dependency_id: available_at_ns}. Every declared
        dependency_id must be present, and self.available_at (or processed_at
        if available absent) must be >= max dependency availability.
        """
        if not self.dependency_ids:
            return
        missing = [d for d in self.dependency_ids if d not in availability_by_id]
        if missing:
            raise ValueError(f"missing dependency availabilities: {missing}")
        latest = max(ensure_utc_ns(availability_by_id[d], field=f"dependency[{d}]") for d in self.dependency_ids)
        anchor = self.available_at_ns if self.available_at_ns is not None else self.processed_at_ns
        if anchor is not None and anchor < latest:
            raise ValueError("derived record cannot be available before required dependencies")

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "source_id": self.source_id,
            "data_type": self.data_type,
            "units": self.units,
            "source_event_at_ns": self.source_event_at_ns,
            "source_published_at_ns": self.source_published_at_ns,
            "received_at_ns": self.received_at_ns,
            "available_at_ns": self.available_at_ns,
            "processed_at_ns": self.processed_at_ns,
            "source_revision": self.source_revision,
            "availability_class": self.availability_class.value,
            "availability_lower_ns": self.availability_lower_ns,
            "availability_upper_ns": self.availability_upper_ns,
            "replay_available_at_ns": self.replay_available_at_ns,
            "availability_method": self.availability_method,
            "evidence_ref": self.evidence_ref,
            "content_hash": self.content_hash,
            "dependency_ids": list(self.dependency_ids),
            "pipeline_version": self.pipeline_version,
            "quality": self.quality,
            "missingness": self.missingness,
        }

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def compute_content_hash(self) -> str:
        d = dict(self.to_dict())
        d.pop("content_hash", None)
        return hashlib.sha256(
            json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def actual_observed(
    *,
    source_id: str,
    data_type: str,
    units: str = "",
    source_event_at_ns: int | None = None,
    source_published_at_ns: int | None = None,
    received_at_ns: int,
    available_at_ns: int,
    processed_at_ns: int | None = None,
    pipeline_version: str = "1.0",
    evidence_ref: str | None = None,
    dependency_ids: tuple[str, ...] = (),
    quality: str = "",
    missingness: str = "",
) -> InformationContract:
    return InformationContract(
        contract_version="1.0",
        source_id=source_id,
        data_type=data_type,
        units=units,
        source_event_at_ns=source_event_at_ns,
        source_published_at_ns=source_published_at_ns,
        received_at_ns=received_at_ns,
        available_at_ns=available_at_ns,
        processed_at_ns=processed_at_ns,
        availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        pipeline_version=pipeline_version,
        evidence_ref=evidence_ref,
        dependency_ids=dependency_ids,
        quality=quality,
        missingness=missingness,
    )


def reconstructed_public(
    *,
    source_id: str,
    data_type: str,
    units: str = "",
    source_event_at_ns: int | None = None,
    received_at_ns: int,
    replay_available_at_ns: int,
    availability_method: str,
    availability_lower_ns: int | None = None,
    availability_upper_ns: int | None = None,
    pipeline_version: str = "1.0",
    evidence_ref: str | None = None,
    dependency_ids: tuple[str, ...] = (),
    quality: str = "",
    missingness: str = "",
) -> InformationContract:
    return InformationContract(
        contract_version="1.0",
        source_id=source_id,
        data_type=data_type,
        units=units,
        source_event_at_ns=source_event_at_ns,
        received_at_ns=received_at_ns,
        availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
        availability_lower_ns=availability_lower_ns,
        availability_upper_ns=availability_upper_ns,
        replay_available_at_ns=replay_available_at_ns,
        availability_method=availability_method,
        pipeline_version=pipeline_version,
        evidence_ref=evidence_ref,
        dependency_ids=dependency_ids,
        quality=quality,
        missingness=missingness,
    )
