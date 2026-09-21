"""TradePlan + execution eligibility tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.capability import initial_unverified_fixture
from atlas.domain.enums import CapabilityStatus, Side
from atlas.domain.trade_plan import TradePlan, validate_plan_for_execution

T0 = 1_700_000_000_000_000_000
T_EXP = T0 + 60_000_000_000
T_HORIZON = T0 + 24 * 3600_000_000_000


def make_valid_plan(**overrides) -> TradePlan:
    base = dict(  # noqa: C408
        plan_id="plan-0001",
        version="v1",
        policy_hash="policyhash0001",
        snapshot_hash="snapshothash0001",
        expires_at_ns=T_EXP,
        market="BYBIT",
        account_scope="testnet-acct",
        instrument="BTCUSDT",
        side=Side.LONG,
        qty_limit=Decimal("0.01"),
        entry_policy="IOC_LIMIT_FULL_STOP",
        collar=Decimal("50000"),
        stop=Decimal("48000"),
        stop_trigger_basis="MarkPrice",
        management_policy="FIXED_STOP_TIME_EXIT_24H",
        horizon_end_ns=T_HORIZON,
        cost_distribution_ref="costdist-v1",
        normal_risk=Decimal("10"),
        stress_risk=Decimal("25"),
        margin=Decimal("100"),
        leverage_bound=Decimal("2"),
        risk_config_hash="riskhash-v1",
        created_at_ns=T0,
        available_at_ns=T0,
        reference_price=Decimal("49000"),
    )
    base.update(overrides)
    return TradePlan(**base)  # type: ignore[arg-type]


def test_valid_plan_and_hash_deterministic():
    p = make_valid_plan()
    assert not p.is_expired(T0)
    assert p.plan_hash() == make_valid_plan().plan_hash()
    assert p.to_canonical_json() == make_valid_plan().to_canonical_json()


def test_expired_plan_detectably_invalid():
    p = make_valid_plan()
    assert p.is_expired(T_EXP)
    assert p.is_expired(T_EXP + 1)
    caps = initial_unverified_fixture()
    result = validate_plan_for_execution(p, caps, T_EXP)
    assert not result.ok
    assert any("expired" in r for r in result.reasons)


def test_unverified_protection_rejects_execution():
    p = make_valid_plan()
    caps = initial_unverified_fixture()
    result = validate_plan_for_execution(p, caps, T0)
    assert not result.ok
    assert any("protection capability" in r for r in result.reasons)
    # Structured reasons, not only True/False
    assert isinstance(result.reasons, list) and len(result.reasons) >= 6


def test_supported_capabilities_allow_valid_plan():
    import dataclasses

    from atlas.domain.capability import REQUIRED_CAPABILITY_FIELDS, Capabilities

    caps_all = Capabilities(
        **dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.SUPPORTED)  # type: ignore[arg-type]
    )
    base = initial_unverified_fixture()
    caps = dataclasses.replace(base, capabilities=caps_all)
    p = make_valid_plan()
    result = validate_plan_for_execution(p, caps, T0)
    assert result.ok, result.reasons


def test_malformed_ids_hashes_rejected():
    with pytest.raises(ValueError):
        make_valid_plan(plan_id="  ")
    with pytest.raises(ValueError):
        make_valid_plan(policy_hash="")
    with pytest.raises(ValueError):
        make_valid_plan(qty_limit=Decimal("0"))
    with pytest.raises(ValueError):
        make_valid_plan(qty_limit=Decimal("-1"))


def test_stop_side_compatibility():
    # LONG stop above reference must fail
    with pytest.raises(ValueError, match="LONG stop must be below"):
        make_valid_plan(side=Side.LONG, stop=Decimal("50000"), reference_price=Decimal("49000"))
    with pytest.raises(ValueError, match="SHORT stop must be above"):
        make_valid_plan(
            side=Side.SHORT, stop=Decimal("48000"), reference_price=Decimal("49000")
        )
    # Compatible SHORT passes construction
    make_valid_plan(side=Side.SHORT, stop=Decimal("50000"), reference_price=Decimal("49000"))


def test_expiry_horizon_ordering():
    with pytest.raises(ValueError, match="expires_at must be after creation"):
        make_valid_plan(expires_at_ns=T0)
    with pytest.raises(ValueError, match="horizon_end must be after creation"):
        make_valid_plan(horizon_end_ns=T0)
