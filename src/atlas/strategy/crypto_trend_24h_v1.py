"""Frozen CRYPTO_TREND_24H_V1 signal and timing contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum

from .features import FeatureValues, HourlyClose, feature_values, validate_qualified_closes

CRYPTO_TREND_24H_V1 = "CRYPTO_TREND_24H_V1"
STRATEGY_VERSION = "1.0"
SNAPSHOT_DEADLINE_NS = 30_000_000_000
PLAN_TTL_NS = 60_000_000_000
HORIZON_NS = 24 * 3_600_000_000_000
SNAPSHOT_EVIDENCE_VERSION = "FEATURE_SNAPSHOT_AVAILABILITY_V1"


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

    def __post_init__(self) -> None:
        if self.instrument not in {"BTCUSDT", "ETHUSDT"}:
            raise ValueError("frozen V1 universe is BTCUSDT/ETHUSDT")
        if not is_decision_slot(self.slot_at_ns):
            raise ValueError("four-hour decision slot required")
        if self.computed_at_ns > self.snapshot_deadline_ns:
            raise ValueError("snapshot missed T+30 deadline")
        if self.values.end_at_ns != self.slot_at_ns:
            raise ValueError("feature must end at decision slot")

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


@dataclass(frozen=True)
class FeatureSnapshotEvidenceV1:
    """Untrusted availability declaration kept outside the frozen snapshot.

    This legacy receipt is not source proof. New qualification uses
    FeatureSnapshotSourceProofV1 and reopens the indexed archive.
    """

    snapshot_hash: str
    instrument: str
    slot_at_ns: int
    computed_at_ns: int
    source_record_ids: tuple[str, ...]
    source_end_at_ns: tuple[int, ...]
    source_available_at_ns: tuple[int, ...]
    schema_version: str = SNAPSHOT_EVIDENCE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SNAPSHOT_EVIDENCE_VERSION:
            raise ValueError("unsupported FeatureSnapshot availability evidence version")
        if self.instrument not in {"BTCUSDT", "ETHUSDT"} or not is_decision_slot(self.slot_at_ns):
            raise ValueError("availability evidence must bind the frozen V1 instrument and decision slot")
        if (
            type(self.slot_at_ns) is not int
            or type(self.computed_at_ns) is not int
            or not self.slot_at_ns <= self.computed_at_ns <= self.slot_at_ns + SNAPSHOT_DEADLINE_NS
        ):
            raise ValueError("availability evidence computation must occur from the slot through T+30")
        if len(self.snapshot_hash) != 64 or any(char not in "0123456789abcdef" for char in self.snapshot_hash):
            raise ValueError("availability evidence requires a canonical snapshot hash")
        if (
            not isinstance(self.source_record_ids, tuple)
            or not self.source_record_ids
            or any(
                not isinstance(record_id, str) or not record_id.strip() or record_id != record_id.strip()
                for record_id in self.source_record_ids
            )
            or len(set(self.source_record_ids)) != len(self.source_record_ids)
        ):
            raise ValueError("availability evidence source identity must be nonblank, canonical, and unique")
        if (
            not isinstance(self.source_end_at_ns, tuple)
            or not isinstance(self.source_available_at_ns, tuple)
            or len(self.source_end_at_ns) != len(self.source_record_ids)
            or len(self.source_available_at_ns) != len(self.source_record_ids)
        ):
            raise ValueError("availability declaration requires one timestamp per declared source")
        previous_end: int | None = None
        for end_at_ns, available_at_ns in zip(self.source_end_at_ns, self.source_available_at_ns, strict=True):
            if (
                type(end_at_ns) is not int
                or type(available_at_ns) is not int
                or end_at_ns < 0
                or end_at_ns % 3_600_000_000_000
                or available_at_ns < end_at_ns
                or available_at_ns > self.computed_at_ns
            ):
                raise ValueError("hourly source must be finalized and available by snapshot computation")
            if previous_end is not None and end_at_ns != previous_end + 3_600_000_000_000:
                raise ValueError("availability evidence source closes must be contiguous")
            previous_end = end_at_ns
        if previous_end != self.slot_at_ns:
            raise ValueError("availability evidence source window must end at the decision slot")

    def validates(self, snapshot: FeatureSnapshot) -> bool:
        """Legacy API: declarations alone are not evidence and cannot validate."""
        return False

    def matches_declarations(self, snapshot: FeatureSnapshot) -> bool:
        """Compare the serialized declaration fields without granting proof."""
        return (
            self.instrument == snapshot.instrument
            and self.slot_at_ns == snapshot.slot_at_ns
            and self.computed_at_ns == snapshot.computed_at_ns
            and self.source_record_ids == snapshot.source_record_ids
            and snapshot.values.end_at_ns == snapshot.slot_at_ns
            and self.snapshot_hash == snapshot.snapshot_hash()
        )

    def evidence_hash(self) -> str:
        body = {
            "schema_version": self.schema_version,
            "snapshot_hash": self.snapshot_hash,
            "instrument": self.instrument,
            "slot_at_ns": self.slot_at_ns,
            "computed_at_ns": self.computed_at_ns,
            "source_record_ids": self.source_record_ids,
            "source_end_at_ns": self.source_end_at_ns,
            "source_available_at_ns": self.source_available_at_ns,
        }
        return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _build_snapshot_with_evidence(
    instrument: str, slot_at_ns: int, computed_at_ns: int, closes: tuple[HourlyClose, ...]
) -> tuple[FeatureSnapshot, FeatureSnapshotEvidenceV1]:
    if (
        type(slot_at_ns) is not int
        or type(computed_at_ns) is not int
        or not slot_at_ns <= computed_at_ns <= slot_at_ns + SNAPSHOT_DEADLINE_NS
    ):
        raise ValueError("slot_at_ns <= computed_at_ns <= slot_at_ns + 30 seconds (T+30 deadline) required")
    window = validate_qualified_closes(closes, decision_at_ns=computed_at_ns)
    values = feature_values(window, decision_at_ns=computed_at_ns)
    snapshot = FeatureSnapshot(
        instrument,
        slot_at_ns,
        computed_at_ns,
        values,
        tuple(item.record_id for item in window),
    )
    evidence = FeatureSnapshotEvidenceV1(
        snapshot_hash=snapshot.snapshot_hash(),
        instrument=instrument,
        slot_at_ns=slot_at_ns,
        computed_at_ns=computed_at_ns,
        source_record_ids=tuple(item.record_id for item in window),
        source_end_at_ns=tuple(item.end_at_ns for item in window),
        source_available_at_ns=tuple(item.available_at_ns for item in window),
    )
    return snapshot, evidence


def build_snapshot(
    instrument: str, slot_at_ns: int, computed_at_ns: int, closes: tuple[HourlyClose, ...]
) -> FeatureSnapshot:
    """Build the frozen five-field snapshot after validating actual input timing."""
    snapshot, _ = _build_snapshot_with_evidence(instrument, slot_at_ns, computed_at_ns, closes)
    return snapshot


def build_snapshot_with_evidence(
    instrument: str, slot_at_ns: int, computed_at_ns: int, closes: tuple[HourlyClose, ...]
) -> tuple[FeatureSnapshot, FeatureSnapshotEvidenceV1]:
    """Build a legacy snapshot plus an untrusted, self-consistency declaration.

    The returned declaration is useful for diagnostics only. It cannot qualify
    Phase-4 decisions; use build_snapshot_with_source_proof with the immutable
    source archive for that purpose.
    """
    return _build_snapshot_with_evidence(instrument, slot_at_ns, computed_at_ns, closes)
