"""SQLite durable journal tests (freeze §1.4 + approval single-use)."""

from __future__ import annotations

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
    Observation,
    ProtectionObservation,
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


def make_intent_res(intent_id: str, client_id: str | None = None):
    cid = client_id or generate_client_order_id()
    intent = Intent(
        intent_id=intent_id,
        position_epoch=0,
        plan_id="plan-1",
        plan_version="v1",
        client_order_id=cid,
        writer_epoch=1,
        lifecycle=LifecycleState.INTENT_PERSISTED,
        protection_status=ProtectionStatus.NONE,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=T0,
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


def test_pragmas_wal_full_fk(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    p = j.pragmas()
    assert str(p["journal_mode"]).upper() == "WAL"
    # synchronous=FULL maps to 2
    assert str(p["synchronous"]) == "2", p
    assert str(p["foreign_keys"]) == "1", p
    assert j.schema_version() == 4
    j.close()


def test_schema_creation_idempotent(tmp_path):
    path = tmp_path / "j.db"
    j1 = SQLiteJournal(path)
    j1.close()
    j2 = SQLiteJournal(path)
    assert j2.schema_version() == 4
    j2.close()


def test_create_load_trade_plan(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    plan = make_plan()
    j.create_trade_plan(plan)
    loaded = j.load_trade_plan("plan-1")
    assert loaded.plan_hash() == plan.plan_hash()
    j.close()


def test_unique_client_order_id(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    cid = generate_client_order_id()
    i1, r1 = make_intent_res("i-1", cid)
    j.create_intent_with_reservation(i1, r1)
    i2, r2 = make_intent_res("i-2", cid)  # duplicate client id
    with pytest.raises(PersistenceError, match="integrity|UNIQUE|unique"):
        j.create_intent_with_reservation(i2, r2)
    j.close()


def test_approval_consumed_once(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    ap = Approval(
        approval_id="ap-1",
        user_identity="user",
        plan_id="plan-1",
        plan_version="v1",
        approved_at_ns=T0,
        expires_at_ns=T_EXP,
    )
    j.create_approval(ap)
    out = j.consume_approval(
        approval_id="ap-1", plan_id="plan-1", plan_version="v1", now_ns=T0 + 1
    )
    assert out.is_consumed()
    with pytest.raises(PersistenceError, match="already consumed"):
        j.consume_approval(
            approval_id="ap-1", plan_id="plan-1", plan_version="v1", now_ns=T0 + 2
        )
    j.close()


def test_approval_expired_and_wrong_plan_fail(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    ap = Approval(
        approval_id="ap-e",
        user_identity="u",
        plan_id="plan-1",
        plan_version="v1",
        approved_at_ns=T0,
        expires_at_ns=T_EXP,
    )
    j.create_approval(ap)
    with pytest.raises(PersistenceError, match="expired"):
        j.consume_approval(
            approval_id="ap-e", plan_id="plan-1", plan_version="v1", now_ns=T_EXP
        )
    with pytest.raises(PersistenceError, match="bound to"):
        j.consume_approval(
            approval_id="ap-e", plan_id="other", plan_version="v1", now_ns=T0 + 1
        )
    j.close()


def test_concurrent_double_approval_consumption(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    j.create_approval(
        Approval(
            approval_id="ap-c",
            user_identity="u",
            plan_id="plan-1",
            plan_version="v1",
            approved_at_ns=T0,
            expires_at_ns=T_EXP,
        )
    )
    results: list = []

    def _try():
        try:
            j.consume_approval(
                approval_id="ap-c", plan_id="plan-1", plan_version="v1", now_ns=T0 + 1
            )
            results.append("ok")
        except PersistenceError:
            results.append("fail")

    threads = [threading.Thread(target=_try) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count("ok") == 1
    assert results.count("fail") == 7
    j.close()


def test_atomic_intent_reservation_and_rollback(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    i1, r1 = make_intent_res("i-a")
    j.create_intent_with_reservation(i1, r1)
    assert j.count("intents") == 1
    # Forced failure: mismatched intent_id must leave neither record
    i2, r2 = make_intent_res("i-b")
    bad_res = Reservation(
        reservation_id="res-bad",
        intent_id="different-id",
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("1"),
        stress_loss=Decimal("1"),
        notional=Decimal("1"),
        beta_adjusted_notional=Decimal("1"),
        margin=Decimal("1"),
        es_contribution=Decimal("1"),
    )
    with pytest.raises(PersistenceError):
        j.create_intent_with_reservation(i2, bad_res)
    assert j.count("intents") == 1
    assert j.count("reservations") == 1
    # Direct FK failure also atomic: reservation referencing missing intent via raw SQL
    # is prevented; our API always writes both together.
    j.close()


def test_persist_command_before_dispatch_flow(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    i1, r1 = make_intent_res("i-cmd")
    j.create_intent_with_reservation(i1, r1)
    cmd = make_command(
        command_id="cmd-1",
        intent_id="i-cmd",
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"instrument": "BTCUSDT", "qty": "0.01"},
        expected_state_version=0,
        created_at_ns=T0,
    )
    j.persist_command(cmd)
    loaded = j.load_command("cmd-1")
    assert loaded.outcome == CommandOutcome.UNSENT
    assert loaded.send_started_at_ns is None
    j.mark_send_started("cmd-1", T0 + 5)
    # FIX1: dispatch marker atomically produces UNKNOWN (no second op needed)
    reloaded = j.load_command("cmd-1")
    assert reloaded.send_started_at_ns == T0 + 5
    assert reloaded.outcome == CommandOutcome.UNKNOWN
    # Second marker must fail
    with pytest.raises(PersistenceError):
        j.mark_send_started("cmd-1", T0 + 6)
    # UNKNOWN preserved: explicit transition required
    j.update_command_outcome("cmd-1", CommandOutcome.DEFINITE_ACCEPT)
    assert j.load_command("cmd-1").outcome == CommandOutcome.DEFINITE_ACCEPT
    j.close()


def test_unresolved_intent_retrieval(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    i1, r1 = make_intent_res("i-u1")
    j.create_intent_with_reservation(i1, r1)
    i2, r2 = make_intent_res("i-u2")
    j.create_intent_with_reservation(i2, r2)
    unresolved = j.load_unresolved_intents()
    assert {i.intent_id for i in unresolved} == {"i-u1", "i-u2"}
    j.update_intent_lifecycle(
        "i-u1", LifecycleState.CLOSED, ProtectionStatus.NONE, ReconciliationHealth.CURRENT
    )
    unresolved2 = j.load_unresolved_intents()
    assert {i.intent_id for i in unresolved2} == {"i-u2"}
    j.close()


def test_reservation_totals(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.create_trade_plan(make_plan())
    for idx in ("t1", "t2"):
        ii, rr = make_intent_res(f"i-{idx}")
        j.create_intent_with_reservation(ii, rr)
    totals = j.reservation_totals()
    assert totals["normal_loss"] == Decimal("20")
    assert totals["notional"] == Decimal("1000")
    j.close()


def test_observations_and_economic_events_append(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    j.append_observation(
        Observation(
            observation_id="o1",
            source="test",
            venue_identity="BYBIT-testnet",
            source_time_ns=T0,
            receive_time_ns=T0 + 1,
            raw_hash="abc",
        )
    )
    j.append_protection_observation(
        ProtectionObservation(
            position_epoch=0,
            desired_stop_version=1,
            qty=Decimal("0.01"),
            trigger_basis="MarkPrice",
            stop_price=Decimal("48000"),
            semantics="FULL_POSITION_MARKET",
            evidence_ids=("o1",),
            observed_at_ns=T0,
        )
    )
    j.append_economic_event(
        EconomicEvent(
            account="test-acct",
            venue_transaction_id="tx-1",
            currency="USDT",
            amount=Decimal("-1.5"),
            effective_time_ns=T0,
            received_at_ns=T0 + 1,
            event_type="FEE",
            revision="r1",
        )
    )
    assert j.count("observations") == 1
    assert j.count("protection_observations") == 1
    assert j.count("economic_events") == 1
    j.close()


def test_db_reopen_retains_records(tmp_path):
    path = tmp_path / "persist.db"
    j = SQLiteJournal(path)
    j.create_trade_plan(make_plan())
    i1, r1 = make_intent_res("i-persist")
    j.create_intent_with_reservation(i1, r1)
    j.close()
    j2 = SQLiteJournal(path)
    assert j2.load_trade_plan("plan-1").plan_id == "plan-1"
    assert len(j2.load_unresolved_intents()) == 1
    j2.close()


def test_persistence_failure_not_success(tmp_path):
    j = SQLiteJournal(tmp_path / "j.db")
    with pytest.raises(PersistenceError):
        j.load_trade_plan("missing")
    with pytest.raises(PersistenceError):
        j.load_command("missing")
