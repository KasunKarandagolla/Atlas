"""Session 001 repair tests — domain/pure validators (FIX3, FIX4, FIX7, FIX8, FIX9, FIX12)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.capability import (
    REQUIRED_CAPABILITY_FIELDS,
    Capabilities,
    capability_contract_from_manifest,
    initial_unverified_fixture,
)
from atlas.domain.enums import CapabilityStatus, CommandOutcome, LifecycleState
from atlas.domain.transitions import (
    is_allowed_lifecycle_transition,
    is_allowed_outcome_transition,
    validate_lifecycle_transition,
    validate_outcome_transition,
)


def _manifest_base(**overrides):
    base = {
        "contract_version": "1.0",
        "runtime": {
            "distribution": "nautilus_trader",
            "version": "2.0.0rc5",
            "source_commit": "x",
            "installed_artifact_sha256": "a" * 64,
            "dependency_lock_sha256": "b" * 64,
            "python_platform_abi": "cpython-312-x86_64-linux-gnu",
        },
        "venue": {
            "environment": "testnet",
            "account_identity_hash": "acct-1",
            "account_generation_and_margin_mode": "gen1-isolated",
            "product": "linear",
            "position_mode": "one_way",
            "symbols": ["BTCUSDT", "ETHUSDT"],
        },
        "capabilities": dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, "SUPPORTED"),
        "assisted_enabled": False,
    }
    base.update(overrides)
    return base


# FIX3: frozen lifecycle transitions
def test_legal_transitions_sample():
    validate_lifecycle_transition(LifecycleState.INTENT_PERSISTED, LifecycleState.SUBMITTING)
    validate_lifecycle_transition(LifecycleState.SUBMITTING, LifecycleState.SUBMIT_UNKNOWN)
    validate_lifecycle_transition(LifecycleState.OPEN_UNPROTECTED, LifecycleState.OPEN_PROTECTED)
    validate_lifecycle_transition(LifecycleState.OPEN_PROTECTED, LifecycleState.EXIT_PENDING)
    validate_lifecycle_transition(LifecycleState.FLAT_PENDING_RECONCILIATION, LifecycleState.CLOSED)
    validate_lifecycle_transition(LifecycleState.CLOSED, LifecycleState.RECOVERY_REQUIRED)


def test_illegal_transitions_rejected():
    illegal = [
        (LifecycleState.CLOSED, LifecycleState.SUBMITTING),
        (LifecycleState.OPEN_PROTECTED, LifecycleState.INTENT_PERSISTED),
        (LifecycleState.SUBMIT_UNKNOWN, LifecycleState.PLAN_APPROVED),
        (LifecycleState.EXIT_PENDING, LifecycleState.INTENT_PERSISTED),
        (LifecycleState.PLAN_APPROVED, LifecycleState.OPEN_PROTECTED),
        (LifecycleState.INTENT_PERSISTED, LifecycleState.OPEN_PROTECTED),
    ]
    for frm, to in illegal:
        assert not is_allowed_lifecycle_transition(frm, to)
        with pytest.raises(ValueError, match="illegal lifecycle"):
            validate_lifecycle_transition(frm, to)


def test_lifecycle_exhaustive_no_self_transition():
    for state in LifecycleState:
        assert not is_allowed_lifecycle_transition(state, state)


# FIX4: command outcome transitions
def test_outcome_unsent_to_unknown_allowed():
    assert is_allowed_outcome_transition(CommandOutcome.UNSENT, CommandOutcome.UNKNOWN)
    validate_outcome_transition(CommandOutcome.UNSENT, CommandOutcome.UNKNOWN)


def test_outcome_terminal_never_regresses():
    for term in (
        CommandOutcome.DEFINITE_ACCEPT,
        CommandOutcome.DEFINITE_REJECT,
        CommandOutcome.RECONCILED,
    ):
        assert not is_allowed_outcome_transition(term, CommandOutcome.UNSENT)
        assert not is_allowed_outcome_transition(term, CommandOutcome.UNKNOWN)
        with pytest.raises(ValueError, match="illegal command outcome"):
            validate_outcome_transition(term, CommandOutcome.UNSENT)


def test_outcome_unknown_resolves_only_with_evidence_states():
    for target in (
        CommandOutcome.DEFINITE_ACCEPT,
        CommandOutcome.DEFINITE_REJECT,
        CommandOutcome.RECONCILED,
    ):
        assert is_allowed_outcome_transition(CommandOutcome.UNKNOWN, target)
    # UNKNOWN must not regress to UNSENT
    assert not is_allowed_outcome_transition(CommandOutcome.UNKNOWN, CommandOutcome.UNSENT)
    # Contradictory terminal overwrite rejected
    assert not is_allowed_outcome_transition(CommandOutcome.DEFINITE_ACCEPT, CommandOutcome.DEFINITE_REJECT)
    assert not is_allowed_outcome_transition(CommandOutcome.DEFINITE_REJECT, CommandOutcome.DEFINITE_ACCEPT)


# FIX7: placeholder evidence blocks assisted
def test_supported_caps_plus_placeholders_still_blocked():
    c = initial_unverified_fixture()
    import dataclasses

    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.SUPPORTED))  # type: ignore[arg-type]
    blocked = dataclasses.replace(c, capabilities=caps)
    reasons = blocked.validate_for_assisted()
    assert any("placeholder" in r for r in reasons)
    with pytest.raises(ValueError, match="placeholder"):
        dataclasses.replace(blocked, assisted_enabled=True)


def test_bad_sha256_shape_blocked():
    c = initial_unverified_fixture(
        installed_artifact_sha256="not-a-hash",
        dependency_lock_sha256="b" * 64,
        python_platform_abi="abi",
        account_identity_hash="acct",
        account_generation_and_margin_mode="mode",
    )
    import dataclasses

    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.SUPPORTED))  # type: ignore[arg-type]
    c2 = dataclasses.replace(c, capabilities=caps)
    assert any("SHA256" in r for r in c2.validate_for_assisted())


# FIX8: strict boolean parsing
def test_manifest_rejects_string_booleans():
    for bad in ("false", "true", "0", "1", 0, 1, None):
        data = _manifest_base(assisted_enabled=bad)
        with pytest.raises(ValueError, match="real YAML boolean"):
            capability_contract_from_manifest(data)


def test_manifest_accepts_real_booleans():
    data = _manifest_base(assisted_enabled=False)
    c = capability_contract_from_manifest(data)
    assert c.assisted_enabled is False


# FIX9: decimal normalization
def test_tradeplan_normalizes_str_inputs():
    from atlas.domain.enums import Side
    from atlas.domain.trade_plan import TradePlan

    t0 = 1_700_000_000_000_000_000
    p = TradePlan(
        plan_id="p",
        version="v1",
        policy_hash="h1",
        snapshot_hash="h2",
        expires_at_ns=t0 + 60_000_000_000,
        market="BYBIT",
        account_scope="acct",
        instrument="BTCUSDT",
        side=Side.LONG,
        qty_limit="0.01",  # type: ignore[arg-type]
        entry_policy="IOC",
        collar="50000",  # type: ignore[arg-type]
        stop="48000",  # type: ignore[arg-type]
        stop_trigger_basis="MarkPrice",
        management_policy="M",
        horizon_end_ns=t0 + 1_000_000_000,
        cost_distribution_ref="c",
        normal_risk="10",  # type: ignore[arg-type]
        stress_risk=25,  # type: ignore[arg-type]
        margin="100",  # type: ignore[arg-type]
        leverage_bound="2",  # type: ignore[arg-type]
        risk_config_hash="r",
        created_at_ns=t0,
        available_at_ns=t0,
        reference_price="49000",  # type: ignore[arg-type]
    )
    for name in (
        "qty_limit",
        "collar",
        "stop",
        "normal_risk",
        "stress_risk",
        "margin",
        "leverage_bound",
        "reference_price",
    ):
        assert isinstance(getattr(p, name), Decimal), name
    # Canonical serialization works after str inputs
    assert '"qty_limit":"0.01"' in p.to_canonical_json()


def test_risk_reservation_eco_normalize_and_nan_rejected():
    from atlas.domain.execution import EconomicEvent, ProtectionObservation, Reservation
    from atlas.domain.risk import RiskPolicy

    r = Reservation(
        reservation_id="r",
        intent_id="i",
        remaining_open_qty="0.01",  # type: ignore[arg-type]
        normal_loss="10",  # type: ignore[arg-type]
        stress_loss=25,  # type: ignore[arg-type]
        notional="500",  # type: ignore[arg-type]
        beta_adjusted_notional="400",  # type: ignore[arg-type]
        margin="100",  # type: ignore[arg-type]
        es_contribution="5",  # type: ignore[arg-type]
    )
    assert isinstance(r.normal_loss, Decimal)
    po = ProtectionObservation(
        position_epoch=0,
        desired_stop_version=1,
        qty="0.01",  # type: ignore[arg-type]
        trigger_basis="MarkPrice",
        stop_price="48000",  # type: ignore[arg-type]
        semantics="FULL",
        evidence_ids=(),
        observed_at_ns=1_000,
    )
    assert isinstance(po.qty, Decimal) and isinstance(po.stop_price, Decimal)
    ev = EconomicEvent(
        account="a",
        venue_transaction_id="t",
        currency="USDT",
        amount="-1.5",  # type: ignore[arg-type]
        effective_time_ns=1_000,
        received_at_ns=1_001,
        event_type="FEE",
        revision="r1",
    )
    assert isinstance(ev.amount, Decimal)
    pol = RiskPolicy(
        policy_version="v",
        policy_effective_at_ns=1,
        eligible_equity_definition="E",
        normal_loss_per_trade_frac="0.001",  # type: ignore[arg-type]
        aggregate_open_normal_loss_frac="0.005",  # type: ignore[arg-type]
        stress_loss_per_trade_frac="0.0025",  # type: ignore[arg-type]
        portfolio_es_alpha="0.975",  # type: ignore[arg-type]
        portfolio_es_limit_frac="0.01",  # type: ignore[arg-type]
        account_gross_notional_limit="1.0",  # type: ignore[arg-type]
        instrument_notional_limit="0.5",  # type: ignore[arg-type]
        correlated_crypto_beta_limit="0.75",  # type: ignore[arg-type]
        venue_collateral_limit="1.0",  # type: ignore[arg-type]
        min_free_margin_reserve_frac="0.5",  # type: ignore[arg-type]
        drawdown_reduce_threshold="0.05",  # type: ignore[arg-type]
        drawdown_stop_threshold="0.10",  # type: ignore[arg-type]
        drawdown_reduce_recovery="0.04",  # type: ignore[arg-type]
        drawdown_stop_recovery="0.08",  # type: ignore[arg-type]
        max_contract_leverage="2.0",  # type: ignore[arg-type]
    )
    assert isinstance(pol.normal_loss_per_trade_frac, Decimal)
    with pytest.raises(ValueError):
        Reservation(
            reservation_id="r2",
            intent_id="i",
            remaining_open_qty=Decimal("NaN"),
            normal_loss=Decimal("1"),
            stress_loss=Decimal("1"),
            notional=Decimal("1"),
            beta_adjusted_notional=Decimal("1"),
            margin=Decimal("1"),
            es_contribution=Decimal("1"),
        )


# FIX12: bool type safety
def test_health_snapshot_rejects_truthy_non_bools():
    from atlas.domain.enums import HealthState, ProtectionStatus, ReconciliationHealth
    from atlas.runtime.health import HealthSnapshot

    base = {
        "state": HealthState.READY,
        "writer_owned": True,
        "account_matched": True,
        "data_current": True,
        "reconciliation": ReconciliationHealth.CURRENT,
        "protection": ProtectionStatus.NONE,
        "has_open_exposure": False,
        "drawdown_stop_active": False,
    }
    HealthSnapshot(**base)  # type: ignore[arg-type]
    for field in (
        "writer_owned",
        "account_matched",
        "data_current",
        "has_open_exposure",
        "drawdown_stop_active",
    ):
        bad = dict(base)
        bad[field] = "false"
        with pytest.raises(ValueError, match="must be bool"):
            HealthSnapshot(**bad)  # type: ignore[arg-type]
        bad2 = dict(base)
        bad2[field] = 1
        with pytest.raises(ValueError, match="must be bool"):
            HealthSnapshot(**bad2)  # type: ignore[arg-type]
