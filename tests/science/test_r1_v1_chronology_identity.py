"""V1 chronology guard regressions that preserve the frozen snapshot hash."""

from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.strategy.crypto_trend_24h_v1 import FeatureSnapshot, build_snapshot
from atlas.strategy.features import HOUR_NS, FeatureWindowError, HourlyClose

SLOT = 720 * HOUR_NS


def _closes(*, late_last_by_ns: int = 0, id_transform=None) -> tuple[HourlyClose, ...]:
    return tuple(
        HourlyClose(
            index * HOUR_NS,
            Decimal("100") + Decimal(index) / 10,
            index * HOUR_NS + (late_last_by_ns if index == 720 else 0),
            id_transform(index) if id_transform else f"close-{index}",
        )
        for index in range(721)
    )


def test_snapshot_computation_cannot_precede_decision_slot() -> None:
    with pytest.raises(ValueError, match="slot_at_ns <= computed_at_ns"):
        build_snapshot("BTCUSDT", SLOT, SLOT - 1, _closes())


def test_snapshot_computation_cannot_precede_last_required_close_availability() -> None:
    closes = _closes(late_last_by_ns=2)
    with pytest.raises(FeatureWindowError, match="future/unavailable close"):
        build_snapshot("BTCUSDT", SLOT, SLOT + 1, closes)


def test_snapshot_rejects_future_source_receipt_and_accepts_deadline_boundaries() -> None:
    closes = _closes()
    at_slot = build_snapshot("BTCUSDT", SLOT, SLOT, closes)
    at_deadline = build_snapshot("BTCUSDT", SLOT, SLOT + 30_000_000_000, closes)
    assert at_slot.computed_at_ns == SLOT
    assert at_deadline.computed_at_ns == SLOT + 30_000_000_000
    with pytest.raises(FeatureWindowError, match="future/unavailable close"):
        build_snapshot("BTCUSDT", SLOT, SLOT, _closes(late_last_by_ns=1))


def test_hourly_close_availability_must_follow_final_close_boundary() -> None:
    closes = list(_closes())
    closes[-1] = HourlyClose(SLOT, closes[-1].close, SLOT - 1, closes[-1].record_id)
    with pytest.raises(FeatureWindowError, match="availability precedes final close"):
        build_snapshot("BTCUSDT", SLOT, SLOT + 1, tuple(closes))


@pytest.mark.parametrize("bad_id", ("", "  "))
def test_v1_close_source_ids_must_be_nonblank_and_unique(bad_id: str) -> None:
    with pytest.raises(FeatureWindowError, match="source record identity"):
        build_snapshot(
            "BTCUSDT", SLOT, SLOT + 1, _closes(id_transform=lambda index: bad_id if index == 20 else f"close-{index}")
        )


def test_v1_close_source_ids_must_be_unique() -> None:
    with pytest.raises(FeatureWindowError, match="source record identity"):
        build_snapshot(
            "BTCUSDT",
            SLOT,
            SLOT + 1,
            _closes(id_transform=lambda index: "duplicate" if index in (20, 21) else f"close-{index}"),
        )


def test_legacy_v1_snapshot_hash_remains_the_existing_legacy_digest() -> None:
    closes = _closes()
    snapshot = build_snapshot("BTCUSDT", SLOT, SLOT + 1, closes)
    # This pins the existing V1 identity input shape. AUD-06's complete identity
    # migration remains a separate versioned seam.
    assert snapshot.snapshot_hash() == "c86086000f333ba7d372f81a1a14945a53eaef7693647483ddc74f745df5bc25"
    assert (
        FeatureSnapshot(
            snapshot.instrument,
            snapshot.slot_at_ns,
            snapshot.computed_at_ns,
            snapshot.values,
            snapshot.source_record_ids,
        ).snapshot_hash()
        == snapshot.snapshot_hash()
    )
