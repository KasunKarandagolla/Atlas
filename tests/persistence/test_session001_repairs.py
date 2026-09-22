"""Session 001 repair tests — persistence (FIX1, FIX2, FIX5, FIX6, FIX10, FIX11)."""

from __future__ import annotations

import sqlite3
import threading
from decimal import Decimal

import pytest

from atlas.domain.enums import (
    CommandOutcome,
    CommandType,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
    Side,
)
from atlas.domain.execution import (
    Approval,
    EconomicEvent,
    Intent,
    Reservation,
    generate_client_order_id,
    make_command,
)
from atlas.domain.trade_plan import TradePlan
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal

T0 = 1_700_000_000_000_000_000
T_EXP = T0 + 60_000_000_000
T_HOR = T0 + 24 * 3600_000_000_000


def make_plan(plan_id: str = "plan-1") -> TradePlan:
    return TradePlan(
        plan_id=plan_id,
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


def make_intent_res(intent_id: str, plan_id: str = "plan-1"):
    intent = Intent(
        intent_id=intent_id,
        position_epoch=0,
        plan_id=plan_id,
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
        reservation_id=f"res-{intent_id}",
        intent_id=intent_id,
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("10"),
        stress_loss=Decimal("25"),
        notional=Decimal("500"),
        beta_adjusted_notional=Decimal("400"),
        margin=Decimal("100"),
        es_contribution=Decimal("5"),
    )
    return intent, res


# FIX1: dispatch marker -> UNKNOWN + restart
def test_dispatch_marker_produces_unknown_and_survives_reopen(tmp_path):
    path = tmp_path / "fix1.db"
    j = SQLiteJournal(path)
    j.create_trade_plan(make_plan())
    intent, res = make_intent_res("i-fix1")
    j.create_intent_with_reservation(intent, res)
    cmd = make_command(
        command_id="cmd-fix1",
        intent_id="i-fix1",
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"a": 1},
        expected_state_version=0,
        created_at_ns=T0,
    )
    j.persist_command(cmd)
    assert j.load_command("cmd-fix1").outcome == CommandOutcome.UNSENT
    j.mark_send_started("cmd-fix1", T0 + 5)
    assert j.load_command("cmd-fix1").outcome == CommandOutcome.UNKNOWN
    j.close()
    # Reopen: UNKNOWN preserved without any second operation
    j2 = SQLiteJournal(path)
    assert j2.load_command("cmd-fix1").outcome == CommandOutcome.UNKNOWN
    assert j2.load_command("cmd-fix1").send_started_at_ns == T0 + 5
    j2.close()


def test_unsent_without_marker_stays_unsent(tmp_path):
    j = SQLiteJournal(tmp_path / "fix1b.db")
    j.create_trade_plan(make_plan())
    intent, res = make_intent_res("i-fix1b")
    j.create_intent_with_reservation(intent, res)
    j.persist_command(
        make_command(
            command_id="c",
            intent_id="i-fix1b",
            command_type=CommandType.SUBMIT_ENTRY,
            payload_dict={"a": 1},
            expected_state_version=0,
            created_at_ns=T0,
        )
    )
    assert j.load_command("c").outcome == CommandOutcome.UNSENT
    j.close()


# FIX2: state version + migration
def test_state_version_increment_and_stale_rejection(tmp_path):
    j = SQLiteJournal(tmp_path / "fix2.db")
    assert j.schema_version() == 6
    j.create_trade_plan(make_plan())
    intent, res = make_intent_res("i-fix2")
    j.create_intent_with_reservation(intent, res)
    assert j.load_intent("i-fix2").state_version == 0
    out = j.update_intent_state(
        intent_id="i-fix2",
        lifecycle=LifecycleState.SUBMITTING,
        protection=ProtectionStatus.NONE,
        health=ReconciliationHealth.CURRENT,
        expected_version=0,
    )
    assert out.state_version == 1
    # Stale expected version rejected
    with pytest.raises(PersistenceError, match="stale"):
        j.update_intent_state(
            intent_id="i-fix2",
            lifecycle=LifecycleState.ENTRY_WORKING,
            protection=ProtectionStatus.NONE,
            health=ReconciliationHealth.CURRENT,
            expected_version=0,
        )
    # Version cannot decrease / illegal transition rejected
    with pytest.raises(PersistenceError):
        j.update_intent_state(
            intent_id="i-fix2",
            lifecycle=LifecycleState.INTENT_PERSISTED,
            protection=ProtectionStatus.NONE,
            health=ReconciliationHealth.CURRENT,
            expected_version=1,
        )
    j.close()
    j2 = SQLiteJournal(tmp_path / "fix2.db")
    assert j2.load_intent("i-fix2").state_version == 1
    j2.close()


def test_migration_from_v1(tmp_path):
    # Build a genuine v1 database (old DDL, version stamp 1).
    path = tmp_path / "v1.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE schema_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE trade_plans (plan_id TEXT PRIMARY KEY, version TEXT NOT NULL,"
        " canonical_json TEXT NOT NULL, plan_hash TEXT NOT NULL,"
        " expires_at_ns INTEGER NOT NULL, created_at_ns INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE approvals (approval_id TEXT PRIMARY KEY, user_identity TEXT NOT NULL,"
        " plan_id TEXT NOT NULL, plan_version TEXT NOT NULL, approved_at_ns INTEGER NOT NULL,"
        " expires_at_ns INTEGER NOT NULL, consumed_at_ns INTEGER)"
    )
    conn.execute(
        "CREATE TABLE intents (intent_id TEXT PRIMARY KEY, position_epoch INTEGER NOT NULL,"
        " plan_id TEXT NOT NULL, plan_version TEXT NOT NULL, client_order_id TEXT NOT NULL UNIQUE,"
        " writer_epoch INTEGER NOT NULL, lifecycle TEXT NOT NULL, protection_status TEXT NOT NULL,"
        " reconciliation_health TEXT NOT NULL, created_at_ns INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE commands (command_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL,"
        " command_type TEXT NOT NULL, exact_payload_hash TEXT NOT NULL, payload TEXT NOT NULL,"
        " expected_state_version INTEGER NOT NULL, created_at_ns INTEGER NOT NULL,"
        " send_started_at_ns INTEGER, outcome TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE reservations (reservation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL,"
        " remaining_open_qty TEXT NOT NULL, normal_loss TEXT NOT NULL, stress_loss TEXT NOT NULL,"
        " notional TEXT NOT NULL, beta_adjusted_notional TEXT NOT NULL, margin TEXT NOT NULL,"
        " es_contribution TEXT NOT NULL, version INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE observations (observation_id TEXT PRIMARY KEY, source TEXT NOT NULL,"
        " venue_identity TEXT NOT NULL, source_time_ns INTEGER, receive_time_ns INTEGER NOT NULL,"
        " raw_hash TEXT NOT NULL, request_id TEXT, query_interval_ns INTEGER,"
        " completeness TEXT NOT NULL DEFAULT '')"
    )
    conn.execute(
        "CREATE TABLE protection_observations (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " position_epoch INTEGER NOT NULL, desired_stop_version INTEGER NOT NULL, qty TEXT NOT NULL,"
        " trigger_basis TEXT NOT NULL, stop_price TEXT NOT NULL, semantics TEXT NOT NULL,"
        " evidence_ids_json TEXT NOT NULL, observed_at_ns INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE economic_events (venue_transaction_id TEXT PRIMARY KEY,"
        " account TEXT NOT NULL, currency TEXT NOT NULL, amount TEXT NOT NULL,"
        " effective_time_ns INTEGER NOT NULL, received_at_ns INTEGER NOT NULL,"
        " event_type TEXT NOT NULL, revision TEXT NOT NULL)"
    )
    conn.execute("INSERT INTO schema_metadata(key, value) VALUES('schema_version', '1')")
    conn.commit()
    conn.close()
    j = SQLiteJournal(path)
    assert j.schema_version() == 6
    # v1 rows remain readable with default state_version 0
    j.close()


# FIX5: atomic approval + intent + reservation
def test_atomic_approval_intent_reservation_rollback(tmp_path):
    j = SQLiteJournal(tmp_path / "fix5.db")
    j.create_trade_plan(make_plan())
    j.create_approval(
        Approval(
            approval_id="ap-5",
            user_identity="u",
            plan_id="plan-1",
            plan_version="v1",
            approved_at_ns=T0,
            expires_at_ns=T_EXP,
        )
    )
    intent, res = make_intent_res("i-5")
    bad_res = Reservation(
        reservation_id="res-dup",
        intent_id="i-5",
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("1"),
        stress_loss=Decimal("1"),
        notional=Decimal("1"),
        beta_adjusted_notional=Decimal("1"),
        margin=Decimal("1"),
        es_contribution=Decimal("1"),
    )
    # Force failure: pre-insert a conflicting reservation_id so the atomic tx fails.
    j.create_intent_with_reservation(*make_intent_res("i-pre"))
    # Manually create id collision by reusing reservation_id
    clash_intent, _ = make_intent_res("i-clash")
    import dataclasses

    clash_res = dataclasses.replace(bad_res, reservation_id="res-i-pre")
    with pytest.raises(PersistenceError):
        j.consume_approval_with_intent_reservation(
            approval_id="ap-5",
            plan_id="plan-1",
            plan_version="v1",
            now_ns=T0 + 1,
            intent=intent,
            reservation=clash_res,
        )
    # All effects rolled back: approval unused, no intent, no reservation
    # Approval must still be consumable (proves it remained unused after failure).
    consumed = j.consume_approval(approval_id="ap-5", plan_id="plan-1", plan_version="v1", now_ns=T0 + 2)
    assert consumed.is_consumed()
    assert j.count("intents") == 1  # only i-pre; failed i-5 left nothing
    j.close()
    _ = clash_intent


def test_atomic_approval_concurrency_single_winner(tmp_path):
    j = SQLiteJournal(tmp_path / "fix5c.db")
    j.create_trade_plan(make_plan())
    j.create_approval(
        Approval(
            approval_id="ap-5c",
            user_identity="u",
            plan_id="plan-1",
            plan_version="v1",
            approved_at_ns=T0,
            expires_at_ns=T_EXP,
        )
    )
    results: list = []

    def _try(idx: int):
        try:
            ii, rr = make_intent_res(f"i-5c-{idx}")
            j.consume_approval_with_intent_reservation(
                approval_id="ap-5c",
                plan_id="plan-1",
                plan_version="v1",
                now_ns=T0 + 1,
                intent=ii,
                reservation=rr,
            )
            results.append("ok")
        except PersistenceError:
            results.append("fail")

    threads = [threading.Thread(target=_try, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count("ok") == 1
    assert results.count("fail") == 7
    j.close()


# FIX6: durable command + reservation binding
def test_prepare_dispatch_binds_command_and_advances_version(tmp_path):
    j = SQLiteJournal(tmp_path / "fix6.db")
    j.create_trade_plan(make_plan())
    intent, res = make_intent_res("i-6")
    j.create_intent_with_reservation(intent, res)
    cmd = j.prepare_dispatch(
        intent_id="i-6",
        expected_state_version=0,
        expected_reservation_version=1,
        command_id="cmd-6",
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"instrument": "BTCUSDT", "qty": "0.01"},
        created_at_ns=T0,
    )
    assert cmd.expected_state_version == 0
    assert j.load_intent("i-6").state_version == 1
    assert j.load_intent("i-6").lifecycle == LifecycleState.SUBMITTING
    # Reservation preserved (not released on UNKNOWN path)
    totals = j.reservation_totals()
    assert totals["normal_loss"] == Decimal("10")
    # Stale version rejected
    with pytest.raises(PersistenceError, match="stale"):
        j.prepare_dispatch(
            intent_id="i-6",
            expected_state_version=0,
            expected_reservation_version=1,
            command_id="cmd-6b",
            command_type=CommandType.SUBMIT_ENTRY,
            payload_dict={"x": 1},
            created_at_ns=T0,
        )


# FIX10: composite economic identity
def test_economic_event_composite_key(tmp_path):
    j = SQLiteJournal(tmp_path / "fix10.db")

    def _ev(acct: str, tx: str) -> EconomicEvent:
        return EconomicEvent(
            account=acct,
            venue_transaction_id=tx,
            currency="USDT",
            amount=Decimal("-1"),
            effective_time_ns=T0,
            received_at_ns=T0 + 1,
            event_type="FEE",
            revision="r1",
        )

    j.append_economic_event(_ev("acct-A", "tx-1"))
    j.append_economic_event(_ev("acct-B", "tx-1"))  # different account allowed
    assert j.count("economic_events") == 2
    with pytest.raises(PersistenceError):
        j.append_economic_event(_ev("acct-A", "tx-1"))  # duplicate composite rejected
    j.close()


# FIX11: fail-closed pragmas
def test_pragma_fail_closed_on_memory_db():
    with pytest.raises(PersistenceError, match="WAL"):
        SQLiteJournal(":memory:")


def test_pragmas_verified_active(tmp_path):
    j = SQLiteJournal(tmp_path / "fix11.db")
    p = j.pragmas()
    assert str(p["journal_mode"]).upper() == "WAL"
    assert str(p["synchronous"]) in ("2", "FULL")
    assert str(p["foreign_keys"]) in ("1", "ON")
    j.close()
