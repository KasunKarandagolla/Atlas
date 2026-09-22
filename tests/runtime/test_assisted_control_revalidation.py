from __future__ import annotations

from decimal import Decimal

from conftest import T0, completed_run
from support.assisted_control_fixture import (
    NOW_NS,
    evidence,
    make_approval,
    make_plan,
    persist_ready_recovery,
)

from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, generate_client_order_id, make_command
from atlas.domain.risk import engineering_default_policy
from atlas.persistence.sqlite import SQLiteJournal
from atlas.risk.engine import AccountState
from atlas.runtime.assisted_control import AssistedControlShell
from atlas.runtime.recovery import recover_from_persisted_run


def _shell(journal: SQLiteJournal) -> AssistedControlShell:
    return AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                writer_epoch=1)


def test_stale_quote_and_mark_block_before_consumption(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = _shell(journal)
    stale_quote = evidence(plan, quote_at_ns=NOW_NS - 2_000_000_000)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=stale_quote)
    assert result.status == "REVALIDATION_BLOCKED" and "stale quote" in result.reasons
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    assert journal.count("intents") == 1  # only the closed recovery intent
    assert journal.count("commands") == 1  # only the terminal recovery command

    stale_mark = evidence(plan, mark_at_ns=NOW_NS - 2_000_000_000)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=stale_mark)
    assert result.status == "REVALIDATION_BLOCKED" and "stale mark" in result.reasons
    journal.close()


def test_account_identity_and_changed_risk_policy_block(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = _shell(journal)
    mismatch = evidence(plan, account_scope="other-account")
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=mismatch)
    assert result.status == "REVALIDATION_BLOCKED" and "account identity mismatch" in result.reasons

    other_policy = engineering_default_policy(policy_version="other-policy")
    changed = evidence(plan, policy=other_policy)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=changed)
    assert result.status == "REVALIDATION_BLOCKED" and "RiskPolicy hash" in " ".join(result.reasons)
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()


def test_non_ready_recovery_certificate_blocks(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    completed_run(journal)
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-not-ready",
        reconciliation_run_id="run-1",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        started_at_ns=T0,
        ended_at_ns=T0 + 20,
        prerequisites_ok=True,
    )
    assert certificate.decision.value != "READY"
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="user-1",
                                           evidence=evidence(plan, recovery_run_id="recovery-not-ready"))
    assert result.status == "REVALIDATION_BLOCKED"
    assert "recovery certificate is not READY" in result.reasons
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()


def test_risk_headroom_failure_blocks(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    thin = AccountState(Decimal("100000"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"))
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="user-1", evidence=evidence(plan, account=thin))
    assert result.status == "REVALIDATION_BLOCKED"
    assert "free-margin-reserve" in result.reasons
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()


def test_existing_unknown_opening_intent_blocks_without_new_persistence(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    persist_ready_recovery(journal)
    make_plan(journal, plan_id="plan-unknown")
    intent = Intent(
        intent_id="unknown-intent",
        position_epoch=0,
        plan_id="plan-unknown",
        plan_version="v1",
        client_order_id=generate_client_order_id(),
        writer_epoch=1,
        lifecycle=LifecycleState.INTENT_PERSISTED,
        protection_status=ProtectionStatus.NONE,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=T0,
    )
    reservation = Reservation("res-unknown", "unknown-intent", Decimal("0.01"), Decimal("10"), Decimal("25"),
                              Decimal("490"), Decimal("490"), Decimal("100"), Decimal("0"))
    journal.create_intent_with_reservation(intent, reservation)
    command = make_command(command_id="cmd-unknown", intent_id="unknown-intent",
                           command_type=CommandType.SUBMIT_ENTRY, payload_dict={"x": 1},
                           expected_state_version=0, created_at_ns=T0)
    journal.persist_command(command)
    journal.mark_send_started("cmd-unknown", T0 + 1)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="user-1", evidence=evidence(plan))
    assert result.status == "REVALIDATION_BLOCKED"
    assert "existing unresolved intent" in result.reasons
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    assert journal.count("intents") == 2
    journal.close()


def test_conflicting_pending_reservation_risk_blocks(tmp_path):
    journal = SQLiteJournal(tmp_path / "revalidation.db")
    persist_ready_recovery(journal)
    make_plan(journal, plan_id="pending-plan")
    pending = Intent(
        intent_id="pending-intent",
        position_epoch=0,
        plan_id="pending-plan",
        plan_version="v1",
        client_order_id=generate_client_order_id(),
        writer_epoch=1,
        lifecycle=LifecycleState.CLOSED,
        protection_status=ProtectionStatus.NONE,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=T0,
    )
    reservation = Reservation("res-pending", "pending-intent", Decimal("1"), Decimal("100000"),
                              Decimal("200000"), Decimal("1000000"), Decimal("1000000"),
                              Decimal("100000"), Decimal("0"))
    journal.create_intent_with_reservation(pending, reservation)
    plan = make_plan(journal, plan_id="new-plan")
    approval = make_approval(journal, plan)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="user-1", evidence=evidence(plan))
    assert result.status == "REVALIDATION_BLOCKED"
    assert any(reason in result.reasons for reason in ("gross-notional", "instrument-notional", "aggregate-normal-loss"))
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()
