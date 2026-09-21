"""Phase 1 safe-runtime tests: startup/recovery, identity, health, ops, dependency."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from atlas.domain.capability import initial_unverified_fixture
from atlas.domain.enums import (
    CommandType,
    HealthState,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
    Side,
)
from atlas.domain.execution import Intent, Reservation, generate_client_order_id, make_command
from atlas.domain.trade_plan import TradePlan
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime import coordinator as coord
from atlas.runtime.connectivity import (
    PrivateVerification,
    PublicState,
    PublicVenueHealth,
)
from atlas.runtime.nautilus_boundary import PINNED_VERSION, verify_installation
from atlas.runtime.prerequisites import (
    IdentityExpectation,
    ObservedAccountState,
    check_identity,
)
from atlas.runtime.recovery import RecoveryDecision, run_recovery
from atlas.runtime.status import RuntimeStatus, load_status, publish_status
from atlas.runtime.writer_lock import WriterLock

T0 = 1_700_000_000_000_000_000
T_EXP = T0 + 60_000_000_000
T_HOR = T0 + 24 * 3600_000_000_000


def _plan() -> TradePlan:
    return TradePlan(
        plan_id="plan-1",
        version="v1",
        policy_hash="pol",
        snapshot_hash="snap",
        expires_at_ns=T_EXP,
        market="BYBIT",
        account_scope="test-acct",
        instrument="BTCUSDT",
        side=Side.LONG,
        qty_limit=Decimal("0.01"),
        entry_policy="IOC_LIMIT_FULL_STOP",
        collar=Decimal("50000"),
        stop=Decimal("48000"),
        stop_trigger_basis="MarkPrice",
        management_policy="FIXED_STOP_TIME_EXIT_24H",
        horizon_end_ns=T_HOR,
        cost_distribution_ref="cost-v1",
        normal_risk=Decimal("10"),
        stress_risk=Decimal("25"),
        margin=Decimal("100"),
        leverage_bound=Decimal("2"),
        risk_config_hash="risk-v1",
        created_at_ns=T0,
        available_at_ns=T0,
        reference_price=Decimal("49000"),
    )


def _intent(iid: str) -> tuple[Intent, Reservation]:
    intent = Intent(
        intent_id=iid,
        position_epoch=0,
        plan_id="plan-1",
        plan_version="v1",
        client_order_id=generate_client_order_id(),
        writer_epoch=1,
        lifecycle=LifecycleState.INTENT_PERSISTED,
        protection_status=ProtectionStatus.NONE,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=T0,
        state_version=0,
    )
    res = Reservation(
        reservation_id=f"res-{iid}",
        intent_id=iid,
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("10"),
        stress_loss=Decimal("25"),
        notional=Decimal("500"),
        beta_adjusted_notional=Decimal("400"),
        margin=Decimal("100"),
        es_contribution=Decimal("5"),
    )
    return intent, res


def _fresh_public(now: int) -> PublicVenueHealth:
    return PublicVenueHealth(
        state=PublicState.SYNCHRONIZED,
        received_at_ns=now - 1_000_000_000,
        source_time_ns=now - 1_000_000_000,
        last_heartbeat_ns=now - 500_000_000,
    )


def _observed_ok(**over) -> ObservedAccountState:
    base = {"environment": "testnet", "venue": "BYBIT", "product": "linear", "position_mode": "one_way", "margin_profile": "isolated", "account_identity_hash": "test-acct-hash-123", "instruments_with_metadata": ("BTCUSDT", "ETHUSDT"), "private_verified": False}
    base.update(over)
    return ObservedAccountState(**base)  # type: ignore[arg-type]  # noqa: C408


# Startup / recovery
def test_boot_empty_journal_recovering_not_ready(tmp_path):
    res = coord.boot(
        journal_path=str(tmp_path / "j.db"),
        lock_path=str(tmp_path / "w.lock"),
        capability_hash="x",
        all_qualified=False,
        assisted_enabled=False,
        identity_expected=IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"),
        identity_observed=_observed_ok(),
        public_health=PublicVenueHealth(state=PublicState.DISCONNECTED),
        private_verification=PrivateVerification(),
        now_ns=T0,
        max_public_staleness_ns=5_000_000_000,
    )
    assert res.state == HealthState.RECOVERING
    assert res.new_risk_allowed is False


def test_unsent_command_stays_unsent_only_without_marker(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(_plan())
    ii, rr = _intent("i-u")
    j.create_intent_with_reservation(ii, rr)
    j.persist_command(
        make_command(
            command_id="c-u",
            intent_id="i-u",
            command_type=CommandType.SUBMIT_ENTRY,
            payload_dict={"a": 1},
            expected_state_version=0,
            created_at_ns=T0,
        )
    )
    j.close()
    res = coord.boot(
        journal_path=str(tmp_path / "j.db"),
        lock_path=str(tmp_path / "w2.lock"),
        capability_hash="x",
        all_qualified=False,
        assisted_enabled=False,
        identity_expected=IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"),
        identity_observed=_observed_ok(),
        public_health=_fresh_public(T0 + 10_000_000_000),
        private_verification=PrivateVerification(),
        now_ns=T0 + 10_000_000_000,
        max_public_staleness_ns=5_000_000_000,
    )
    assert res.unresolved_intents == 1
    assert res.unknown_commands == 0  # no marker => UNSENT, not UNKNOWN
    assert res.new_risk_allowed is False


def test_dispatch_started_reloads_unknown_and_blocks_ready(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(_plan())
    ii, rr = _intent("i-k")
    j.create_intent_with_reservation(ii, rr)
    j.persist_command(
        make_command(
            command_id="c-k",
            intent_id="i-k",
            command_type=CommandType.SUBMIT_ENTRY,
            payload_dict={"a": 1},
            expected_state_version=0,
            created_at_ns=T0,
        )
    )
    j.mark_send_started("c-k", T0 + 1)
    j.close()
    res = coord.boot(
        journal_path=str(tmp_path / "j.db"),
        lock_path=str(tmp_path / "w3.lock"),
        capability_hash="x",
        all_qualified=False,
        assisted_enabled=False,
        identity_expected=IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"),
        identity_observed=_observed_ok(),
        public_health=_fresh_public(T0 + 10_000_000_000),
        private_verification=PrivateVerification(),
        now_ns=T0 + 10_000_000_000,
        max_public_staleness_ns=5_000_000_000,
    )
    assert res.unknown_commands == 1
    assert res.new_risk_allowed is False
    assert res.certificate.decision in (
        RecoveryDecision.REMAIN_RECOVERING,
        RecoveryDecision.RECOVERY_REQUIRED,
    )


def test_duplicate_local_writer_rejected(tmp_path):
    w1 = WriterLock(tmp_path / "dup.lock")
    w1.acquire()
    with pytest.raises(Exception, match="(?i)writer|already|acquir|exists"):
        coord.boot(
            journal_path=str(tmp_path / "j.db"),
            lock_path=str(tmp_path / "dup.lock"),
            capability_hash="x",
            all_qualified=False,
            assisted_enabled=False,
            identity_expected=IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"),
            identity_observed=_observed_ok(),
            public_health=PublicVenueHealth(state=PublicState.DISCONNECTED),
            private_verification=PrivateVerification(),
            now_ns=T0,
            max_public_staleness_ns=5_000_000_000,
        )
    w1.release()


def test_restart_reuses_client_order_identity(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(_plan())
    ii, rr = _intent("i-r")
    cid = ii.client_order_id
    j.create_intent_with_reservation(ii, rr)
    j.close()
    j2 = SQLiteJournal(tmp_path / "j.db")
    assert j2.load_intent("i-r").client_order_id == cid
    j2.close()


def test_recovery_certificate_never_claims_venue_without_observations():
    from atlas.runtime.recovery import RecoveryCertificate

    with pytest.raises(ValueError, match="venue_observations_obtained"):
        RecoveryCertificate(
            recovery_run_id="r",
            writer_id="w",
            writer_epoch=1,
            journal_schema_version=2,
            unresolved_intents=("i1",),
            unresolved_commands=("c1",),
            unknown_commands=("c1",),
            reconciliation_health=ReconciliationHealth.CURRENT,
            protection_uncertainty_summary="x",
            started_at_ns=T0,
            ended_at_ns=T0,
            evidence_refs=("journal-restore",),
            venue_observations_obtained=False,
            venue_evidence_refs=(),
            decision=RecoveryDecision.READY,
        )


# Identity
@pytest.mark.parametrize(
    "field,value",
    [
        ("environment", "live"),
        ("venue", "COINBASE"),
        ("product", "inverse"),
        ("position_mode", "hedge"),
        ("margin_profile", "cross"),
        ("account_identity_hash", "WRONG"),
    ],
)
def test_identity_mismatches_fail(field, value):
    kw = {field: value}
    res = check_identity(IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"), _observed_ok(**kw))
    assert not res.ok


def test_missing_instrument_metadata_fails():
    obs = _observed_ok(instruments_with_metadata=("BTCUSDT",))
    res = check_identity(IdentityExpectation(expected_account_identity_hash="test-acct-hash-123"), obs)
    assert not res.ok
    assert any("ETHUSDT" in r or "metadata" in r for r in res.reasons)


# Health
def test_stale_public_not_fresh():
    h = PublicVenueHealth(
        state=PublicState.SYNCHRONIZED,
        received_at_ns=T0,
        source_time_ns=T0,
        last_heartbeat_ns=T0,
    )
    assert not h.is_fresh(now_ns=T0 + 60_000_000_000, max_staleness_ns=5_000_000_000)


def test_stale_exchange_timestamp_not_fresh_even_if_received_now():
    now = T0 + 100_000_000_000
    h = PublicVenueHealth(
        state=PublicState.SYNCHRONIZED,
        received_at_ns=now,  # received just now...
        source_time_ns=T0,  # ...but data is old
        last_heartbeat_ns=now,
    )
    assert not h.is_fresh(now_ns=now, max_staleness_ns=5_000_000_000)


def test_conflicted_blocks_ready_in_recovery():
    cert = run_recovery(
        recovery_run_id="r",
        writer_id="w",
        writer_epoch=1,
        journal_schema_version=2,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        reconciliation_health=ReconciliationHealth.CONFLICTED,
        protection_uncertainty_summary="x",
        started_at_ns=T0,
        ended_at_ns=T0,
        venue_observations_obtained=True,
        venue_evidence_refs=("venue-obs-1",),
        prerequisites_ok=True,
    )
    assert cert.decision == RecoveryDecision.RECOVERY_REQUIRED


# Ops boundary
def test_status_sanitized_and_atomic(tmp_path):
    path = tmp_path / "status.json"
    from atlas.domain.time import now_ns
    st = RuntimeStatus(
        runtime_state="RECOVERING",
        writer_epoch=3,
        journal_healthy=True,
        reconciliation_health="STALE",
        unresolved_intents=1,
        unknown_commands=1,
        unresolved_commands=1,
        capability_hash="abc",
        all_qualified=False,
        assisted_enabled=False,
        generated_at_ns=now_ns(),
        runtime_instance_id="test-instance",
        writer_id="test-writer",
    )
    publish_status(path, st)
    loaded = load_status(path)
    assert loaded == st
    raw = json.loads(path.read_text())
    for forbidden in ("api_key", "secret", "token", "account_id", "signed_request"):
        assert forbidden not in raw


def test_ops_has_no_order_interface():
    import atlas.runtime.coordinator as coord_mod
    import atlas.runtime.status as status_mod

    for mod in (status_mod, coord_mod):
        with open(mod.__file__, encoding="utf-8") as _fh:
            src = _fh.read().lower()
        assert "place_order" not in src
        assert "submit_order" not in src
        assert "create_order" not in src


# Dependency
def test_nautilus_pinned_version_or_absent():
    ev = verify_installation()
    if ev.installed:
        assert ev.version == PINNED_VERSION
    caps = initial_unverified_fixture()
    for f in (
        "entry_ioc_with_attached_full_mark_market_stop",
        "native_stop_visible_and_resizes_on_partial_fill",
        "reduce_only_wire_and_matching_enforcement",
        "ambiguous_submit_not_treated_as_definite_rejection",
        "external_native_stop_fill_reconciliation",
        "native_position_stop_read_and_repair_port",
    ):
        assert getattr(caps.capabilities, f).value == "UNVERIFIED"
    assert caps.assisted_enabled is False


@pytest.mark.integration
def test_public_testnet_connectivity_opt_in():
    pytest.skip("opt-in public testnet check; no credentials, network not required")
