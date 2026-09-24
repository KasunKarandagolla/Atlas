"""Causal final-bar normalization and append-only revision handling."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.money import ensure_positive_decimal

from .._serialization import canonical_json, decimal_value, timestamp
from .raw import AvailabilityClassV2, RawObservationV2


class BarIntervalV2(StrEnum):
    M15 = "15M"
    H1 = "1H"
    H4 = "4H"

    @property
    def duration_ns(self) -> int:
        return {BarIntervalV2.M15: 900, BarIntervalV2.H1: 3600, BarIntervalV2.H4: 14400}[self] * 1_000_000_000


def close_boundary_ns(open_at_ns: int, interval: BarIntervalV2) -> int:
    timestamp(open_at_ns, field="open_at_ns")
    return open_at_ns + BarIntervalV2(interval).duration_ns


@dataclass(frozen=True)
class CausalBarV2:
    raw: RawObservationV2
    interval: BarIntervalV2
    open_at_ns: int
    close_at_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    final: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "interval", BarIntervalV2(self.interval))
        if self.raw.event_type != f"BAR_{self.interval.value}":
            raise ValueError("bar event_type does not match its canonical interval")
        if "FORMING" in self.raw.quality_flags and self.final:
            raise ValueError("forming venue data cannot be represented as a final policy-ready bar")
        timestamp(self.open_at_ns, field="open_at_ns")
        timestamp(self.close_at_ns, field="close_at_ns")
        if self.open_at_ns % self.interval.duration_ns:
            raise ValueError("bar open must align to its UTC interval boundary")
        if self.close_at_ns != close_boundary_ns(self.open_at_ns, self.interval):
            raise ValueError("bar must use deterministic UTC half-open interval boundary")
        for name in ("open", "high", "low", "close"):
            object.__setattr__(self, name, ensure_positive_decimal(getattr(self, name), field=name))
        object.__setattr__(self, "volume", decimal_value(self.volume, field="volume"))
        if self.volume < 0:
            raise ValueError("volume cannot be negative")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("bar OHLC values are inconsistent")
        if not self.final:
            return
        if self.raw.event_at_ns is None or self.raw.event_at_ns < self.close_at_ns:
            raise ValueError("final bar event time must be at or after the exclusive close boundary")
        if self.raw.available_at_ns < max(self.close_at_ns, self.raw.received_at_ns):
            raise ValueError("final bar cannot be available before close confirmation and receipt")

    @property
    def instrument_revision(self) -> str:
        return self.raw.instrument_revision

    @property
    def replay_available_at_ns(self) -> int | None:
        return self.raw.replay_available_at_ns

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.to_dict()).encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "record_id": self.raw.record_id,
            "raw_payload_hash": self.raw.raw_payload_hash,
            "instrument_revision": self.instrument_revision,
            "interval": self.interval.value,
            "open_at_ns": self.open_at_ns,
            "close_at_ns": self.close_at_ns,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "final": self.final,
        }


class CausalBarStoreV2:
    """Append-only bars; replay queries choose only versions available by cutoff."""

    def __init__(self) -> None:
        self._bars: dict[tuple[str, BarIntervalV2, int], list[CausalBarV2]] = {}

    def append(self, bar: CausalBarV2) -> bool:
        if not bar.final:
            return False
        if bar.raw.availability_class not in (
            AvailabilityClassV2.ACTUAL_SYSTEM,
            AvailabilityClassV2.RECONSTRUCTED_MARKET,
        ):
            raise ValueError("policy-ready bars require actual or reconstructed availability")
        key = (bar.instrument_revision, bar.interval, bar.open_at_ns)
        versions = self._bars.setdefault(key, [])
        existing = next((item for item in versions if item.raw.record_id == bar.raw.record_id), None)
        if existing is not None:
            if existing.content_hash != bar.content_hash:
                raise ValueError("conflicting finalized bar identity")
            return False
        if versions:
            view = bar.raw.availability_class
            previous = max(versions, key=lambda item: (self._availability_key(item, view), item.raw.record_id))
            if bar.raw.revision_of != previous.raw.record_id:
                raise ValueError("bar correction must append with revision_of pointing to prior version")
        elif bar.raw.revision_of is not None:
            raise ValueError("first stored bar version cannot reference an unknown correction parent")
        versions.append(bar)
        versions.sort(key=lambda item: (self._availability_key(item, AvailabilityClassV2.ACTUAL_SYSTEM), item.raw.record_id))
        return True

    @staticmethod
    def _availability_key(bar: CausalBarV2, view: AvailabilityClassV2) -> int:
        if view == AvailabilityClassV2.ACTUAL_SYSTEM:
            return bar.raw.available_at_ns
        return bar.raw.replay_available_at_ns or 0

    def as_of(
        self,
        instrument_revision: str,
        interval: BarIntervalV2,
        *,
        information_cutoff_ns: int,
        availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
    ) -> tuple[CausalBarV2, ...]:
        timestamp(information_cutoff_ns, field="information_cutoff_ns")
        view = AvailabilityClassV2(availability_class)
        if view not in (AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET):
            raise ValueError("bar replay view must be ACTUAL_SYSTEM or RECONSTRUCTED_MARKET")
        result: list[CausalBarV2] = []
        for (revision, kind, _), versions in self._bars.items():
            if revision != instrument_revision or kind != BarIntervalV2(interval):
                continue
            available = [
                item for item in versions
                if item.raw.availability_class == view
                and (
                    item.raw.available_at_ns <= information_cutoff_ns
                    if view == AvailabilityClassV2.ACTUAL_SYSTEM
                    else item.raw.replay_available_at_ns is not None
                    and item.raw.replay_available_at_ns <= information_cutoff_ns
                )
            ]
            if available:
                result.append(
                    max(
                        available,
                        key=lambda item: (self._availability_key(item, view), item.raw.record_id),
                    )
                )
        return tuple(sorted(result, key=lambda item: (item.open_at_ns, item.raw.record_id)))

    def all_versions(self) -> tuple[CausalBarV2, ...]:
        return tuple(
            bar
            for key in sorted(self._bars, key=lambda item: (item[0], item[1].value, item[2]))
            for bar in self._bars[key]
        )


def translate_final_bar(
    *,
    raw: RawObservationV2,
    interval: BarIntervalV2,
    open_at_ns: int,
    values: Mapping[str, str | Decimal],
    final: bool,
) -> CausalBarV2 | None:
    """Build only from explicit final confirmation; forming bars are excluded."""
    if not final:
        return None
    close_at_ns = close_boundary_ns(open_at_ns, interval)
    return CausalBarV2(
        raw,
        interval,
        open_at_ns,
        close_at_ns,
        Decimal(str(values["open"])),
        Decimal(str(values["high"])),
        Decimal(str(values["low"])),
        Decimal(str(values["close"])),
        Decimal(str(values.get("volume", "0"))),
        True,
    )
