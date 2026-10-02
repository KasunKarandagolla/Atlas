"""Pure bounded accounting for received final M15 origins, including missing cases.

The controller persists records before advancing a source-window checkpoint.
This module never invents bars, creates a decision event or repairs past health.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .._serialization import sha256_json, sha256_ref, strict_fields, timestamp
from ..data.bars import BarIntervalV2, CausalBarV2
from ..data.health import PublicSourceHealthV2
from ..data.raw import AvailabilityClassV2
from ..instruments import InstrumentKeyV2, ProductContractV2
from ..memory.repository import ArtifactIndexEntryV2

M15_ORIGIN_MAX_LATENESS_NS = 5_000_000_000
MAX_M15_ORIGINS_ACCOUNTED_PER_CYCLE = 4
M15_ORIGIN_CHECKPOINT_TYPE = "M15OriginAccountingCheckpointV1"


def _close(value: int) -> int:
    timestamp(value, field="M15 close_at_ns")
    if value % BarIntervalV2.M15.duration_ns:
        raise ValueError("M15 origin must align to its UTC close boundary")
    return value


def m15_origin_ref(key: InstrumentKeyV2, close_at_ns: int) -> str:
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("M15 origin requires its full instrument revision")
    return sha256_json({"version": "M15_ORIGIN_V1", "instrument_key": key.to_dict(),
                        "close_at_ns": _close(close_at_ns)})


def m15_origin_metadata(key: InstrumentKeyV2, close_at_ns: int, *, bar_ref: str) -> dict[str, Any]:
    sha256_ref(bar_ref, field="bar_ref")
    return {"m15_origin": {"version": "M15_ORIGIN_V1", "origin_ref": m15_origin_ref(key, close_at_ns),
            "instrument_key": key.to_dict(), "close_at_ns": close_at_ns, "bar_ref": bar_ref}}


_MISSING_REASONS = frozenset({"M15_ORIGIN_FIRST_ACCOUNTED_AFTER_ELIGIBILITY_DEADLINE",
    "M15_ORIGIN_FIRST_AVAILABLE_AFTER_ELIGIBILITY_DEADLINE", "M15_SOURCE_HEALTH_UNAVAILABLE_AT_ORIGIN",
    "M15_SOURCE_UNHEALTHY_AT_ORIGIN", "M15_PRODUCT_UNAVAILABLE_AT_ORIGIN",
    "M15_EVENT_PREREQUISITE_UNAVAILABLE"})


@dataclass(frozen=True)
class M15OpportunityMissingnessV1:
    instrument_key: InstrumentKeyV2
    close_at_ns: int
    origin_ref: str
    bar_ref: str
    observation_index_ref: str
    received_at_ns: int
    source_available_at_ns: int
    observed_at_ns: int
    origin_eligibility_deadline_ns: int
    reason_code: str
    source_health_ref: str | None = None
    status: str = "NOT ESTIMABLE"
    authority: str = "ZERO"

    VERSION = "M15OpportunityMissingnessV1"

    def __post_init__(self) -> None:
        if self.origin_ref != m15_origin_ref(self.instrument_key, self.close_at_ns):
            raise ValueError("M15 missingness origin identity conflicts")
        for key in ("bar_ref", "observation_index_ref"):
            sha256_ref(getattr(self, key), field=key)
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        for key in ("received_at_ns", "source_available_at_ns", "observed_at_ns", "origin_eligibility_deadline_ns"):
            timestamp(getattr(self, key), field=key)
        if self.source_available_at_ns < max(self.close_at_ns, self.received_at_ns):
            raise ValueError("M15 evidence availability precedes close or receipt")
        if self.observed_at_ns < self.source_available_at_ns:
            raise ValueError("M15 missingness observation cannot be backdated")
        if self.origin_eligibility_deadline_ns != self.close_at_ns + M15_ORIGIN_MAX_LATENESS_NS:
            raise ValueError("M15 origin eligibility deadline cannot move")
        if self.reason_code not in _MISSING_REASONS or self.status != "NOT ESTIMABLE" or self.authority != "ZERO":
            raise ValueError("M15 missingness requires an explicit closed reason and zero authority")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{key: getattr(self, key) for key in self.__dataclass_fields__},
                "instrument_key": self.instrument_key.to_dict()}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> M15OpportunityMissingnessV1:
        fields = {"version", *cls.__dataclass_fields__}
        row = dict(strict_fields(value, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported M15 missingness version")
        row["instrument_key"] = InstrumentKeyV2.from_dict(row["instrument_key"])
        return cls(**row)


class M15OriginAccountingAction(StrEnum):
    ATTEMPT_TIMELY_EVENT = "ATTEMPT_TIMELY_EVENT"
    RECORD_MISSINGNESS = "RECORD_MISSINGNESS"
    REUSE_DURABLE_ORIGIN = "REUSE_DURABLE_ORIGIN"


@dataclass(frozen=True)
class M15OriginAccountingPlan:
    bar: CausalBarV2
    observation_index_ref: str
    origin_ref: str
    action: M15OriginAccountingAction
    missingness: M15OpportunityMissingnessV1 | None = None
    existing_accounting_ref: str | None = None


@dataclass(frozen=True)
class M15OriginAccountingRecordV1:
    """One terminal opportunity binding, persisted before advancing its cursor."""

    instrument_key: InstrumentKeyV2
    close_at_ns: int
    origin_ref: str
    bar_ref: str
    observation_index_ref: str
    accounting_ref: str
    accounting_kind: str
    observed_at_ns: int
    authority: str = "ZERO"

    VERSION = "M15OriginAccountingRecordV1"

    def __post_init__(self) -> None:
        if self.origin_ref != m15_origin_ref(self.instrument_key, self.close_at_ns):
            raise ValueError("M15 terminal accounting origin identity conflicts")
        for key in ("bar_ref", "observation_index_ref", "accounting_ref"):
            sha256_ref(getattr(self, key), field=key)
        timestamp(self.observed_at_ns, field="observed_at_ns")
        if self.accounting_kind not in {"OpsDecisionEventSourceV1", M15OpportunityMissingnessV1.VERSION} or self.authority != "ZERO":
            raise ValueError("M15 accounting must bind an event or explicit missingness")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{key: getattr(self, key) for key in self.__dataclass_fields__},
                "instrument_key": self.instrument_key.to_dict()}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> M15OriginAccountingRecordV1:
        fields = {"version", *cls.__dataclass_fields__}
        row = dict(strict_fields(value, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported M15 terminal accounting version")
        row["instrument_key"] = InstrumentKeyV2.from_dict(row["instrument_key"])
        return cls(**row)


def plan_m15_origin_accounting(
    bars: Sequence[CausalBarV2], key: InstrumentKeyV2, *, now_ns: int,
    observation_entries: Sequence[ArtifactIndexEntryV2],
    health_entries: Sequence[PublicSourceHealthV2] = (), product: ProductContractV2 | None = None,
    existing_entries: Sequence[ArtifactIndexEntryV2] = (), after_close_at_ns: int | None = None,
    limit: int = MAX_M15_ORIGINS_ACCOUNTED_PER_CYCLE,
) -> tuple[M15OriginAccountingPlan, ...]:
    """Plan oldest first from one bounded source page; later health never rescues it."""
    timestamp(now_ns, field="now_ns")
    if type(limit) is not int or not 0 < limit <= MAX_M15_ORIGINS_ACCOUNTED_PER_CYCLE:
        raise ValueError("M15 accounting page exceeds its fixed bound")
    if after_close_at_ns is not None:
        _close(after_close_at_ns)
    if any(len(rows) > 4096 for rows in (bars, observation_entries, health_entries, existing_entries)):
        raise ValueError("M15 accounting requires bounded input pages")
    indexes = {entry.artifact_ref: entry for entry in observation_entries}
    selected: dict[int, tuple[CausalBarV2, str]] = {}
    for bar in bars:
        if (bar.interval != BarIntervalV2.M15 or not bar.final or bar.instrument_revision != key.contract_revision
                or bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or bar.raw.available_at_ns > now_ns
                or after_close_at_ns is not None and bar.close_at_ns <= after_close_at_ns):
            continue
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": bar.raw.record_id})
        entry = indexes.get(ref)
        if (entry is None or entry.artifact_type != "PublicObservationIndexV2"
                or entry.content_hash != bar.raw.content_hash or entry.available_at_ns != bar.raw.available_at_ns
                or entry.metadata.get("instrument_key_json") != key.to_canonical_json()
                or entry.metadata.get("bar_content_hash") != bar.content_hash
                or entry.metadata.get("source_id") != bar.raw.source_id):
            raise ValueError("M15 origin requires exact indexed raw/bar evidence")
        previous = selected.get(bar.close_at_ns)
        if previous is None or (bar.raw.available_at_ns, ref) < (previous[0].raw.available_at_ns, previous[1]):
            selected[bar.close_at_ns] = (bar, ref)
    plans: list[M15OriginAccountingPlan] = []
    for close_at_ns, (bar, index_ref) in sorted(selected.items())[:limit]:
        origin_ref = m15_origin_ref(key, close_at_ns)
        durable = [entry for entry in existing_entries if entry.metadata.get("m15_origin_ref") == origin_ref]
        if len(durable) > 1:
            raise ValueError("M15 origin has contradictory durable accounting records")
        if durable:
            entry = durable[0]
            raw_record = entry.metadata.get("accounting")
            if not isinstance(raw_record, Mapping):
                raise ValueError("M15 durable accounting requires its typed immutable record")
            record = M15OriginAccountingRecordV1.from_dict(raw_record)
            if (entry.artifact_type != record.VERSION or entry.content_hash != record.content_hash
                    or entry.artifact_ref != record.content_hash or record.origin_ref != origin_ref
                    or entry.available_at_ns != record.observed_at_ns or entry.available_at_ns > now_ns):
                raise ValueError("M15 origin accounting identity or availability conflicts")
            plans.append(M15OriginAccountingPlan(bar, index_ref, origin_ref,
                M15OriginAccountingAction.REUSE_DURABLE_ORIGIN, existing_accounting_ref=entry.artifact_ref))
            continue
        eligible_health = [health for health in health_entries if health.source_id == bar.raw.source_id
            and health.available_at_ns <= bar.raw.available_at_ns and health.observed_at_ns <= bar.raw.available_at_ns]
        health = max(eligible_health, key=lambda item: (item.available_at_ns, item.content_hash), default=None)
        deadline = close_at_ns + M15_ORIGIN_MAX_LATENESS_NS
        reason = None
        if bar.raw.available_at_ns > deadline:
            reason = "M15_ORIGIN_FIRST_AVAILABLE_AFTER_ELIGIBILITY_DEADLINE"
        elif now_ns > deadline:
            reason = "M15_ORIGIN_FIRST_ACCOUNTED_AFTER_ELIGIBILITY_DEADLINE"
        elif product is None or product.key != key or product.available_at_ns > bar.raw.available_at_ns or product.effective_at_ns > close_at_ns:
            reason = "M15_PRODUCT_UNAVAILABLE_AT_ORIGIN"
        elif health is None:
            reason = "M15_SOURCE_HEALTH_UNAVAILABLE_AT_ORIGIN"
        elif not health.data_eligible:
            reason = "M15_SOURCE_UNHEALTHY_AT_ORIGIN"
        missingness = M15OpportunityMissingnessV1(key, close_at_ns, origin_ref, bar.content_hash, index_ref,
            bar.raw.received_at_ns, bar.raw.available_at_ns, now_ns, deadline, reason,
            health.content_hash if health else None) if reason is not None else None
        plans.append(M15OriginAccountingPlan(bar, index_ref, origin_ref,
            M15OriginAccountingAction.RECORD_MISSINGNESS if missingness else M15OriginAccountingAction.ATTEMPT_TIMELY_EVENT,
            missingness))
    return tuple(plans)


@dataclass(frozen=True)
class M15OriginAccountingCheckpointV1:
    """Progress within a fixed availability window; old late closes remain discoverable."""

    instrument_key: InstrumentKeyV2
    generation: int
    previous_checkpoint_ref: str | None
    source_available_from_ns: int
    source_available_through_ns: int
    source_scan_after_close_at_ns: int | None
    scan_complete: bool
    observed_at_ns: int
    authority: str = "ZERO"

    VERSION = M15_ORIGIN_CHECKPOINT_TYPE

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_key, InstrumentKeyV2) or type(self.generation) is not int or self.generation < 1:
            raise ValueError("M15 checkpoint requires an exact revision and positive generation")
        if (self.generation == 1) != (self.previous_checkpoint_ref is None):
            raise ValueError("M15 checkpoint predecessor conflicts with generation")
        if self.previous_checkpoint_ref is not None:
            sha256_ref(self.previous_checkpoint_ref, field="previous_checkpoint_ref")
        for key in ("source_available_from_ns", "source_available_through_ns", "observed_at_ns"):
            timestamp(getattr(self, key), field=key)
        if not self.source_available_from_ns <= self.source_available_through_ns <= self.observed_at_ns:
            raise ValueError("M15 checkpoint source window is reversed or future")
        if type(self.scan_complete) is not bool or self.scan_complete != (self.source_scan_after_close_at_ns is None):
            raise ValueError("M15 checkpoint cursor conflicts with completed scan")
        if self.source_scan_after_close_at_ns is not None:
            _close(self.source_scan_after_close_at_ns)
        if self.authority != "ZERO":
            raise ValueError("M15 origin checkpoint has zero authority")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{key: getattr(self, key) for key in self.__dataclass_fields__},
                "instrument_key": self.instrument_key.to_dict()}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> M15OriginAccountingCheckpointV1:
        fields = {"version", *cls.__dataclass_fields__}
        row = dict(strict_fields(value, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported M15 checkpoint version")
        row["instrument_key"] = InstrumentKeyV2.from_dict(row["instrument_key"])
        return cls(**row)


def advance_m15_origin_checkpoint(
    previous: M15OriginAccountingCheckpointV1 | None, key: InstrumentKeyV2, *, now_ns: int,
    source_available_through_ns: int, next_close_cursor_ns: int | None, has_more: bool,
) -> M15OriginAccountingCheckpointV1:
    """Call only after every returned source origin has durable terminal accounting."""
    timestamp(now_ns, field="now_ns")
    timestamp(source_available_through_ns, field="source_available_through_ns")
    if type(has_more) is not bool or has_more and next_close_cursor_ns is None:
        raise ValueError("M15 unfinished source page requires its close cursor")
    if previous is not None:
        if previous.instrument_key != key:
            raise ValueError("M15 checkpoint cannot cross instrument revisions")
        if now_ns < previous.observed_at_ns:
            raise ValueError("M15 checkpoint clock moved backward")
        if previous.scan_complete:
            source_from = previous.source_available_through_ns
            if source_available_through_ns <= source_from:
                raise ValueError("M15 completed source watermark must advance")
        else:
            source_from = previous.source_available_from_ns
            if source_available_through_ns != previous.source_available_through_ns:
                raise ValueError("M15 active source window cannot be rebased")
            if (next_close_cursor_ns is not None and previous.source_scan_after_close_at_ns is not None
                    and next_close_cursor_ns <= previous.source_scan_after_close_at_ns):
                raise ValueError("M15 source close cursor did not advance")
    else:
        source_from = 0
    return M15OriginAccountingCheckpointV1(key, previous.generation + 1 if previous else 1,
        previous.content_hash if previous else None, source_from, source_available_through_ns,
        next_close_cursor_ns if has_more else None, not has_more, now_ns)
