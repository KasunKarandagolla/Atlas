"""Compatibility checks for pre-R1 V1 snapshot artifacts and qualified builds."""

from __future__ import annotations

import json
from dataclasses import asdict, fields
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.domain.enums import Side
from atlas.science.phase4_engine import DecisionStatus, evaluate_phase4, phase4_action_hash
from atlas.science.trade_plan_mapping import candidate_trade_plan
from atlas.strategy import crypto_trend_24h_v1 as v1
from atlas.strategy.crypto_trend_24h_v1 import FeatureSnapshot
from atlas.strategy.features import HOUR_NS, FeatureValues, FeatureWindowError, HourlyClose, feature_values
from tests.support.phase4_factory import decision_input, policy

LEGACY_FIXTURE = Path(__file__).parents[1] / "fixtures" / "legacy_feature_snapshot_v1_pre_r1.json"
LEGACY_SNAPSHOT_HASH = "c86086000f333ba7d372f81a1a14945a53eaef7693647483ddc74f745df5bc25"
LEGACY_ACTION_HASH = "6ff8ec0c3bf5cc545e5c72509a173af711331e9da8f3fd1321bbdf6528686ecb"
HISTORICAL_SLOT = 720 * HOUR_NS


def _legacy_fixture() -> tuple[dict[str, object], FeatureSnapshot]:
    payload = json.loads(LEGACY_FIXTURE.read_text(encoding="utf-8"))
    value = payload["values"]
    assert isinstance(value, dict)
    snapshot = FeatureSnapshot(
        payload["instrument"],
        payload["slot_at_ns"],
        payload["computed_at_ns"],
        FeatureValues(
            value["end_at_ns"],
            tuple(value["returns"]),
            value["variance"],
            value["sigma"],
            value["z"],
        ),
        tuple(payload["source_record_ids"]),
    )
    return payload, snapshot


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


def test_historical_five_argument_snapshot_loads_and_keeps_exact_legacy_serialization() -> None:
    expected, snapshot = _legacy_fixture()

    assert tuple(field.name for field in fields(snapshot)) == (
        "instrument",
        "slot_at_ns",
        "computed_at_ns",
        "values",
        "source_record_ids",
    )
    assert json.loads(json.dumps(asdict(snapshot), sort_keys=True)) == expected


def test_historical_snapshot_hash_tradeplan_reference_and_action_identity_are_unchanged() -> None:
    _, snapshot = _legacy_fixture()
    fixed_policy = policy(
        side=Side.LONG,
        quantity=Decimal("1"),
        mark=Decimal("100"),
        sigma=snapshot.values.sigma,
        slot_at_ns=snapshot.slot_at_ns,
    )

    plan = candidate_trade_plan(
        plan_id="legacy-fixture-plan",
        snapshot=snapshot,
        policy=fixed_policy,
        policy_hash="a" * 64,
        normal_risk=Decimal("10"),
        stress_risk=Decimal("20"),
        margin=Decimal("25"),
        leverage=Decimal("2"),
        cost_evidence_ref="b" * 64,
        account_scope="historical-fixture",
    )

    assert snapshot.snapshot_hash() == LEGACY_SNAPSHOT_HASH
    assert plan.snapshot_hash == LEGACY_SNAPSHOT_HASH
    assert phase4_action_hash(fixed_policy, snapshot, Decimal("1")) == LEGACY_ACTION_HASH


def test_legacy_hourly_close_records_remain_readable_but_do_not_qualify_new_builds() -> None:
    legacy_slot = 724 * HOUR_NS
    legacy_closes = tuple(
        HourlyClose(
            4 * HOUR_NS + index * HOUR_NS,
            Decimal("100") + Decimal(index) / 10,
            4 * HOUR_NS + index * HOUR_NS - 1,
        )
        for index in range(721)
    )

    values = feature_values(legacy_closes)
    assert values.end_at_ns == legacy_slot
    assert len(values.returns) == 720
    with pytest.raises(FeatureWindowError, match="final close"):
        v1.build_snapshot("BTCUSDT", legacy_slot, legacy_slot + 1, legacy_closes)

    identityless = tuple(HourlyClose(item.end_at_ns, item.close, item.end_at_ns) for item in legacy_closes)
    with pytest.raises(FeatureWindowError, match="identity"):
        v1.build_snapshot("BTCUSDT", legacy_slot, legacy_slot + 1, identityless)


def test_qualified_snapshot_builder_returns_separate_availability_receipt() -> None:
    builder = getattr(v1, "build_snapshot_with_evidence", None)
    assert callable(builder), "qualified construction needs a versioned evidence wrapper"

    closes = _closes()
    snapshot, receipt = builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT, closes)

    assert snapshot.snapshot_hash() == LEGACY_SNAPSHOT_HASH
    assert receipt.validates(snapshot)
    assert receipt.source_record_ids == snapshot.source_record_ids
    assert len(receipt.source_available_at_ns) == 721


def test_qualified_builder_uses_actual_computation_time_and_deadline_boundaries() -> None:
    builder = getattr(v1, "build_snapshot_with_evidence", None)
    assert callable(builder)
    closes = _closes()

    at_slot, at_slot_evidence = builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT, closes)
    at_deadline, at_deadline_evidence = builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT + 30_000_000_000, closes)
    assert at_slot.computed_at_ns == HISTORICAL_SLOT and at_slot_evidence.validates(at_slot)
    assert at_deadline.computed_at_ns == HISTORICAL_SLOT + 30_000_000_000
    assert at_deadline_evidence.validates(at_deadline)

    with pytest.raises(ValueError, match=r"T\+30|deadline"):
        builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT + 30_000_000_001, closes)


def test_qualified_builder_rejects_sources_unavailable_at_actual_computation() -> None:
    builder = getattr(v1, "build_snapshot_with_evidence", None)
    assert callable(builder)
    closes = _closes(late_last_by_ns=2)

    with pytest.raises(ValueError, match="future/unavailable close"):
        builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT + 1, closes)


def test_qualified_builder_preserves_finality_and_rejects_conflicting_source_identity() -> None:
    builder = getattr(v1, "build_snapshot_with_evidence", None)
    assert callable(builder)

    early = list(_closes())
    early[-1] = HourlyClose(HISTORICAL_SLOT, early[-1].close, HISTORICAL_SLOT - 1, early[-1].record_id)
    with pytest.raises(ValueError, match="final close"):
        builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT + 1, tuple(early))

    duplicate_ids = _closes(id_transform=lambda i: "same" if i in (20, 21) else f"close-{i}")
    with pytest.raises(ValueError, match="identity"):
        builder("BTCUSDT", HISTORICAL_SLOT, HISTORICAL_SLOT + 1, duplicate_ids)


def test_unproven_legacy_snapshot_does_not_qualify_a_new_phase4_decision() -> None:
    case = decision_input(snapshot_evidence=None)

    result = evaluate_phase4(case)

    assert result.status is DecisionStatus.NOT_ESTIMABLE
    assert result.trade_plan is None
    assert "availability evidence" in result.reason
