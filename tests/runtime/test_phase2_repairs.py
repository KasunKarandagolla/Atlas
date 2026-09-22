from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest
from conftest import T0, add_intent, completed_run, terminal_entry

from atlas.domain.enums import LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.persistence.migrations import bootstrap
from atlas.persistence.schema import SCHEMA_VERSION
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.fill_dedup import FillRecord
from atlas.runtime.flat_certificate import (
    FlatCertificationDecision,
    certify_flat_from_journal,
    persist_and_release_if_flat,
)
from atlas.runtime.protection_deadline import ProtectionDeadlineMachine, ProtectionDeadlineState
from atlas.runtime.recovery import RecoveryCertificate, RecoveryDecision, recover_from_persisted_run


def fill(t=T0, qty="0.01"):
    return FillRecord(
        "exec",
        "order",
        "a" * 32,
        "intent-1",
        "BTCUSDT",
        "Buy",
        Decimal(qty),
        Decimal("50000"),
        Decimal("0"),
        "USDT",
        t,
        t + 1,
        "fixture",
        "a" * 64,
    )


def test_protection_deadline_never_downgrades_after_breach():
    m = ProtectionDeadlineMachine()
    m.on_fill("intent-1", 0, fill(), 1, Decimal("48000"))
    s = m.tick(T0 + 2_000_000_000)["intent-1"]
    assert s.state == ProtectionDeadlineState.RECOVERY_REQUIRED
    for t in (T0 + 4_000_000_000, T0 + 6_000_000_000, T0 + 20_000_000_000):
        tr = m.tick(t)
        s = tr.get("intent-1", m.get_snapshot("intent-1"))
        assert s.state == ProtectionDeadlineState.RECOVERY_REQUIRED


def test_fake_certificate_cannot_release_reservation(journal):
    intent = add_intent(journal)
    with pytest.raises(PersistenceError, match="certified-flat"):
        journal.release_reservation(intent_id=intent.intent_id, certificate_id="fake", released_at_ns=T0 + 1)
    assert journal.reservation_totals()["normal_loss"] == Decimal("10")


def test_full_risk_vector_released_only_after_certified_flat(journal):
    intent = add_intent(journal)
    terminal_entry(journal, intent)
    journal.update_intent_state(
        intent_id=intent.intent_id,
        lifecycle=LifecycleState.CLOSED,
        protection=ProtectionStatus.NONE,
        health=ReconciliationHealth.CURRENT,
        expected_version=0,
    )
    completed_run(journal)
    cert = certify_flat_from_journal(
        journal=journal,
        certification_id="flat-1",
        reconciliation_run_id="run-1",
        intent_id=intent.intent_id,
        current_writer_id="writer",
        current_writer_epoch=1,
        account_identity_hash="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        now_ns=T0 + 20,
    )
    assert cert.decision == FlatCertificationDecision.CERTIFIED_FLAT
    released = persist_and_release_if_flat(journal, cert)
    assert released is not None
    assert all(v == 0 for v in journal.reservation_totals().values())
    assert journal.count("reservation_release_events") == 1


def test_single_positions_query_cannot_make_recovery_ready(journal):
    intent = add_intent(journal)
    terminal_entry(journal, intent)
    journal.update_intent_state(
        intent_id=intent.intent_id,
        lifecycle=LifecycleState.CLOSED,
        protection=ProtectionStatus.NONE,
        health=ReconciliationHealth.CURRENT,
        expected_version=0,
    )
    from conftest import q

    from atlas.runtime.reconciliation_evidence import (
        DEFAULT_EXECUTION_RISK_QUERIES,
        QueryType,
        ReconciliationRun,
        ReconciliationRunState,
    )

    run = ReconciliationRun(
        "short",
        "acct",
        "BTCUSDT",
        "writer",
        1,
        "runtime",
        T0,
        None,
        DEFAULT_EXECUTION_RISK_QUERIES,
        ReconciliationRunState.OPEN,
    )
    journal.create_reconciliation_run(run)
    e = q(QueryType.POSITIONS, "p", facts={"signed_qty": "0"})
    journal.append_reconciliation_query_evidence(e)
    journal.bind_query_to_run("short", "p")
    cert = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-short",
        reconciliation_run_id="short",
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
        ended_at_ns=T0 + 3,
        prerequisites_ok=True,
        flat_certificate_id="fake",
    )
    assert cert.decision == RecoveryDecision.RECOVERY_REQUIRED


def test_recovery_requires_persisted_flat_artifact(journal):
    intent = add_intent(journal)
    terminal_entry(journal, intent)
    journal.update_intent_state(
        intent_id=intent.intent_id,
        lifecycle=LifecycleState.CLOSED,
        protection=ProtectionStatus.NONE,
        health=ReconciliationHealth.CURRENT,
        expected_version=0,
    )
    completed_run(journal, run_id="run-missing")
    cert = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-1",
        reconciliation_run_id="run-missing",
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
        flat_certificate_id="does-not-exist",
    )
    assert cert.decision == RecoveryDecision.REMAIN_RECOVERING
    completed_run(journal, run_id="run-1")
    flat = certify_flat_from_journal(
        journal=journal,
        certification_id="flat",
        reconciliation_run_id="run-1",
        intent_id=intent.intent_id,
        current_writer_id="writer",
        current_writer_epoch=1,
        account_identity_hash="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        now_ns=T0 + 21,
    )
    journal.append_flat_certificate(flat)
    # new recovery run required for a new certificate; old evidence cannot be relabelled as a different run.
    cert2 = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-2",
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
        ended_at_ns=T0 + 22,
        prerequisites_ok=True,
        flat_certificate_id="flat",
    )
    assert cert2.decision == RecoveryDecision.READY
    path = journal.path
    journal.close()
    journal = SQLiteJournal(path)
    assert journal.count("recovery_reconciliation_bindings") == 2

    retry = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-2",
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
        ended_at_ns=T0 + 22,
        prerequisites_ok=True,
        flat_certificate_id="flat",
    )
    assert retry == cert2

    with pytest.raises(PersistenceError, match="already bound"):
        recover_from_persisted_run(
            journal=journal,
            recovery_run_id="recovery-3",
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
            ended_at_ns=T0 + 22,
            prerequisites_ok=True,
            flat_certificate_id="flat",
        )

    with pytest.raises(PersistenceError, match="identity mismatch"):
        recover_from_persisted_run(
            journal=journal,
            recovery_run_id="recovery-4",
            reconciliation_run_id="run-1",
            runtime_instance_id="different-runtime",
            writer_id="writer",
            writer_epoch=1,
            unresolved_intent_ids=(),
            unresolved_command_ids=(),
            unknown_command_ids=(),
            account="acct",
            instrument="BTCUSDT",
            position_epoch=0,
            started_at_ns=T0,
            ended_at_ns=T0 + 22,
            prerequisites_ok=True,
            flat_certificate_id="flat",
        )

    with pytest.raises(PersistenceError, match="identity mismatch"):
        recover_from_persisted_run(
            journal=journal,
            recovery_run_id="recovery-5",
            reconciliation_run_id="run-1",
            runtime_instance_id="runtime",
            writer_id="different-writer",
            writer_epoch=1,
            unresolved_intent_ids=(),
            unresolved_command_ids=(),
            unknown_command_ids=(),
            account="acct",
            instrument="BTCUSDT",
            position_epoch=0,
            started_at_ns=T0,
            ended_at_ns=T0 + 22,
            prerequisites_ok=True,
            flat_certificate_id="flat",
        )


def test_reconciliation_binding_is_transactional_and_one_to_one(journal):
    completed_run(journal)
    first = RecoveryCertificate(
        recovery_run_id="binding-recovery-1",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        journal_schema_version=5,
        unresolved_intents=(),
        unresolved_commands=(),
        unknown_commands=(),
        reconciliation_health=ReconciliationHealth.CONFLICTED,
        started_at_ns=T0,
        ended_at_ns=T0 + 20,
        evidence_refs=("evidence-1",),
        venue_evidence_refs=(),
        decision=RecoveryDecision.RECOVERY_REQUIRED,
    )
    assert journal.append_recovery_certificate(first, reconciliation_run_id="run-1") is True
    assert journal.append_recovery_certificate(first, reconciliation_run_id="run-1") is False

    second = RecoveryCertificate(
        recovery_run_id="binding-recovery-2",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        journal_schema_version=5,
        unresolved_intents=(),
        unresolved_commands=(),
        unknown_commands=(),
        reconciliation_health=ReconciliationHealth.CONFLICTED,
        started_at_ns=T0,
        ended_at_ns=T0 + 21,
        evidence_refs=("evidence-2",),
        venue_evidence_refs=(),
        decision=RecoveryDecision.RECOVERY_REQUIRED,
    )
    with pytest.raises(PersistenceError, match="already bound"):
        journal.append_recovery_certificate(second, reconciliation_run_id="run-1")
    assert journal.count("recovery_certificates") == 1
    assert journal.count("recovery_reconciliation_bindings") == 1


def test_future_sqlite_schema_fails_closed(tmp_path):
    p = tmp_path / "future.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE schema_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
    c.execute("INSERT INTO schema_metadata VALUES('schema_version',?)", (str(SCHEMA_VERSION + 1),))
    c.commit()
    with pytest.raises(RuntimeError, match="future SQLite schema"):
        bootstrap(c)
    assert c.execute("SELECT value FROM schema_metadata WHERE key='schema_version'").fetchone()[0] == str(
        SCHEMA_VERSION + 1
    )
    c.close()
