"""Frozen CRYPTO_TREND_24H_V1 signal and timing contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum

from .features import FeatureValues, HourlyClose, feature_values

CRYPTO_TREND_24H_V1 = "CRYPTO_TREND_24H_V1"
STRATEGY_VERSION = "1.0"
SNAPSHOT_DEADLINE_NS = 30_000_000_000
PLAN_TTL_NS = 60_000_000_000
HORIZON_NS = 24 * 3_600_000_000_000


class Signal(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


def is_decision_slot(at_ns: int) -> bool:
    hour = (at_ns // 3_600_000_000_000) % 24
    return at_ns % 3_600_000_000_000 == 0 and hour in (0, 4, 8, 12, 16, 20)


def signal_from_z(z: float) -> Signal:
    if z > 0.5:
        return Signal.LONG
    if z < -0.5:
        return Signal.SHORT
    return Signal.FLAT


@dataclass(frozen=True)
class FeatureSnapshot:
    instrument: str
    slot_at_ns: int
    computed_at_ns: int
    values: FeatureValues
    source_record_ids: tuple[str, ...]
    source_available_at_ns: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.instrument not in {"BTCUSDT", "ETHUSDT"}:
            raise ValueError("frozen V1 universe is BTCUSDT/ETHUSDT")
        if not is_decision_slot(self.slot_at_ns):
            raise ValueError("four-hour decision slot required")
        if not self.slot_at_ns <= self.computed_at_ns <= self.snapshot_deadline_ns:
            raise ValueError("slot_at_ns <= computed_at_ns <= slot_at_ns + 30 seconds required")
        if self.values.end_at_ns != self.slot_at_ns:
            raise ValueError("feature must end at decision slot")
        if (not isinstance(self.source_record_ids, tuple) or not self.source_record_ids
                or any(not isinstance(ref, str) or not ref.strip() or ref != ref.strip()
                       for ref in self.source_record_ids)
                or len(set(self.source_record_ids)) != len(self.source_record_ids)):
            raise ValueError("snapshot source record identity must be nonblank and unique")
        if (not isinstance(self.source_available_at_ns, tuple)
                or len(self.source_available_at_ns) != len(self.source_record_ids)
                or any(type(at) is not int or at < 0 or at > self.computed_at_ns
                       for at in self.source_available_at_ns)):
            raise ValueError("all required source records must be available by snapshot computation")

    @property
    def snapshot_deadline_ns(self) -> int:
        return self.slot_at_ns + SNAPSHOT_DEADLINE_NS

    @property
    def expires_at_ns(self) -> int:
        return self.slot_at_ns + PLAN_TTL_NS

    @property
    def horizon_end_ns(self) -> int:
        return self.slot_at_ns + HORIZON_NS

    @property
    def signal(self) -> Signal:
        return signal_from_z(self.values.z)

    def snapshot_hash(self) -> str:
        data = {"strategy": CRYPTO_TREND_24H_V1, "version": STRATEGY_VERSION, "instrument": self.instrument,
                "slot": self.slot_at_ns, "sigma": self.values.sigma, "z": self.values.z, "records": self.source_record_ids}
        return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def build_snapshot(instrument: str, slot_at_ns: int, computed_at_ns: int, closes: tuple[HourlyClose, ...]) -> FeatureSnapshot:
    if not slot_at_ns <= computed_at_ns <= slot_at_ns + SNAPSHOT_DEADLINE_NS:
        raise ValueError("slot_at_ns <= computed_at_ns <= slot_at_ns + 30 seconds required")
    return FeatureSnapshot(instrument, slot_at_ns, computed_at_ns,
                           feature_values(closes, decision_at_ns=computed_at_ns),
                           tuple(x.record_id for x in closes),
                           tuple(x.available_at_ns for x in closes))
