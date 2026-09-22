from __future__ import annotations

from decimal import Decimal

from conftest import T0
from support.assisted_control_fixture import (
    NOW_NS,
    evidence,
    make_approval,
    make_plan,
    persist_ready_recovery,
)

from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.assisted_control import AssistedControlShell


def _shell(journal: SQLiteJournal) -> AssistedControlShell:
    return AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                writer_epoch=1)


def test_correct_plan_version_is_accepted_and_consumed_once(tmp_path):
    journal = SQLiteJournal(tmp_path / "approval.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="user-1", evidence=evidence(plan))
    assert result.status == "DISPATCH_BLOCKED"
    assert journal.load_approval(approval.approval_id).consumed_at_ns == NOW_NS
    assert result.intent is not None and result.intent.lifecycle.value == "SUBMITTING"
    assert result.command is not None and result.command.send_started_at_ns is None
    assert result.command.outcome.value == "UNSENT"
    assert journal.load_reservation(result.intent.intent_id).remaining_open_qty == Decimal("0.01")
    journal.close()


def test_wrong_version_expired_approval_and_plan_extension_are_rejected_without_side_effects(tmp_path):
    journal = SQLiteJournal(tmp_path / "approval.db")
    plan = make_plan(journal)
    wrong = make_approval(journal, plan, approval_id="wrong-version", plan_version="v2")
    result = _shell(journal).prepare_entry(plan=plan, approval_id=wrong.approval_id,
                                           user_identity="user-1", evidence=evidence(plan))
    assert result.status == "APPROVAL_BLOCKED"
    assert journal.load_approval(wrong.approval_id).consumed_at_ns is None
    assert journal.count("intents") == 0 and journal.count("commands") == 0

    extension = make_approval(journal, plan, approval_id="extension", expires_at_ns=plan.expires_at_ns + 1)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=extension.approval_id,
                                           user_identity="user-1", evidence=evidence(plan))
    assert result.status == "APPROVAL_BLOCKED" and "extends beyond" in result.reasons[0]
    assert journal.load_approval(extension.approval_id).consumed_at_ns is None

    expired_plan = make_plan(journal, plan_id="expired-plan", expires_at_ns=T0 + 5_000_000_000)
    expired = make_approval(journal, expired_plan, approval_id="expired", expires_at_ns=T0 + 5_000_000_000)
    result = _shell(journal).prepare_entry(plan=expired_plan, approval_id=expired.approval_id,
                                           user_identity="user-1", evidence=evidence(expired_plan))
    assert result.status == "APPROVAL_BLOCKED"
    assert journal.load_approval(expired.approval_id).consumed_at_ns is None
    assert journal.count("intents") == 0
    journal.close()


def test_approval_replay_is_rejected(tmp_path):
    journal = SQLiteJournal(tmp_path / "approval.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = _shell(journal)
    first = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                evidence=evidence(plan))
    assert first.status == "DISPATCH_BLOCKED"
    replay = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=evidence(plan))
    assert replay.status == "APPROVAL_BLOCKED" and "already consumed" in replay.reasons[0]
    assert journal.count("intents") == 2  # closed recovery intent + one new intent
    journal.close()


def test_approval_user_identity_mismatch_is_rejected(tmp_path):
    journal = SQLiteJournal(tmp_path / "approval.db")
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    result = _shell(journal).prepare_entry(plan=plan, approval_id=approval.approval_id,
                                           user_identity="other-user", evidence=evidence(plan))
    assert result.status == "APPROVAL_BLOCKED" and "user identity" in result.reasons[0]
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()
