from __future__ import annotations

from test_journal import T0, make_plan

from atlas.domain.enums import ReconciliationHealth
from atlas.domain.execution import Approval
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.recovery import RecoveryCertificate, RecoveryDecision


def test_v6_to_v2_extension_migration_preserves_v1_rows(tmp_path):
    path = tmp_path / "existing-v1-live.sqlite"
    journal = SQLiteJournal(path)
    plan = make_plan("v6-preserved-plan")
    approval = Approval(
        "v6-preserved-approval",
        "operator",
        plan.plan_id,
        plan.version,
        T0 + 1,
        plan.expires_at_ns - 1,
    )
    journal.create_trade_plan(plan)
    journal.create_approval(approval)
    recovery = RecoveryCertificate(
        recovery_run_id="v6-preserved-recovery",
        runtime_instance_id="v6-runtime",
        writer_id="v6-writer",
        writer_epoch=4,
        journal_schema_version=6,
        unresolved_intents=(),
        unresolved_commands=(),
        unknown_commands=(),
        reconciliation_health=ReconciliationHealth.STALE,
        started_at_ns=T0 + 2,
        ended_at_ns=T0 + 3,
        evidence_refs=("local-restore-only",),
        venue_evidence_refs=(),
        decision=RecoveryDecision.RECOVERY_REQUIRED,
    )
    journal.append_recovery_certificate(recovery)
    before_plan_row = journal._conn.execute(
        "SELECT * FROM trade_plans WHERE plan_id=?", (plan.plan_id,)
    ).fetchone()
    before_approval_row = journal._conn.execute(
        "SELECT * FROM approvals WHERE approval_id=?", (approval.approval_id,)
    ).fetchone()
    before_plan = dict(before_plan_row)
    before_approval = dict(before_approval_row)
    before_recovery = dict(
        journal._conn.execute(
            "SELECT * FROM recovery_certificates WHERE recovery_run_id=?", (recovery.recovery_run_id,)
        ).fetchone()
    )

    # Recreate the pre-remediation v6 schema state. The migration must only add
    # namespaced V2 tables and preserve all V1 row bytes/semantics.
    for trigger in (
        "v2_live_authority_evidence_no_update",
        "v2_live_authority_evidence_no_delete",
        "v2_capital_authority_no_update",
        "v2_capital_authority_no_delete",
    ):
        journal._conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    journal._conn.execute("DROP TABLE IF EXISTS v2_live_authority_evidence")
    journal._conn.execute("DROP TABLE IF EXISTS v2_capital_authority_attestations")
    journal._conn.execute("DROP TABLE IF EXISTS v2_schema_metadata")
    journal.close()

    reopened = SQLiteJournal(path)
    assert reopened.schema_version() == 6
    assert reopened._conn.execute(
        "SELECT value FROM v2_schema_metadata WHERE key='capital_authority_schema_version'"
    ).fetchone()[0] == "2"
    assert dict(reopened._conn.execute("SELECT * FROM trade_plans WHERE plan_id=?", (plan.plan_id,)).fetchone()) == before_plan
    assert dict(
        reopened._conn.execute("SELECT * FROM approvals WHERE approval_id=?", (approval.approval_id,)).fetchone()
    ) == before_approval
    assert dict(
        reopened._conn.execute(
            "SELECT * FROM recovery_certificates WHERE recovery_run_id=?", (recovery.recovery_run_id,)
        ).fetchone()
    ) == before_recovery
    assert reopened.load_recovery_certificate(recovery.recovery_run_id) == recovery
    assert reopened.load_trade_plan(plan.plan_id).plan_hash() == plan.plan_hash()
    assert reopened.load_approval(approval.approval_id) == approval
    assert reopened._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='v2_capital_authority_attestations'"
    ).fetchone()
    assert reopened._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='v2_capital_authority_no_update'"
    ).fetchone()
    reopened.close()


def test_v2_extension_v1_migrates_both_source_and_receipt_identity(tmp_path):
    from runtime.test_phase4_v2_capital_authority import (
        _live_journal,
        _lookup_source_receipt,
        _synthetic_attestation,
    )

    lock, journal, ownership, runtime_id = _live_journal(tmp_path, runtime_id="migration-runtime")
    attestation, _ = _synthetic_attestation(journal, ownership, runtime_id)
    receipt_hash = attestation.live_evidence_refs[0]
    receipt_before = journal.load_v2_live_authority_evidence(receipt_hash)
    assert receipt_before is not None
    assert receipt_before.evidence_ref != receipt_before.content_hash
    attestation_before = journal.load_v2_capital_authority_attestation(attestation.content_hash)
    assert attestation_before == attestation

    # Recreate an extension-v1 database with persisted canonical receipts but
    # no source projection. Its receipt hashes and attestation are immutable.
    journal._conn.execute("DROP INDEX v2_live_authority_source_context_idx")
    journal._conn.execute("ALTER TABLE v2_live_authority_evidence DROP COLUMN source_evidence_ref")
    journal._conn.execute(
        "UPDATE v2_schema_metadata SET value='1' WHERE key='capital_authority_schema_version'"
    )
    journal._conn.commit()
    journal.close()

    reopened = SQLiteJournal(tmp_path / "live.sqlite")
    reopened._bind_v2_live_writer(lock, runtime_id)
    assert reopened.schema_version() == 6
    assert reopened._conn.execute(
        "SELECT value FROM v2_schema_metadata WHERE key='capital_authority_schema_version'"
    ).fetchone()[0] == "2"
    assert reopened.load_v2_live_authority_evidence(receipt_hash) == receipt_before
    assert reopened.load_v2_capital_authority_attestation(attestation.content_hash) == attestation_before
    projected = reopened._conn.execute(
        "SELECT source_evidence_ref,evidence_hash FROM v2_live_authority_evidence WHERE evidence_hash=?",
        (receipt_hash,),
    ).fetchone()
    assert tuple(projected) == (receipt_before.evidence_ref, receipt_before.content_hash)
    assert _lookup_source_receipt(reopened, receipt_before, require_live=False) == receipt_before
    assert reopened._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='v2_live_authority_evidence_no_update'"
    ).fetchone()
    assert reopened._conn.execute(
        "SELECT COUNT(*) FROM v2_capital_authority_attestations WHERE attestation_hash=?",
        (attestation.content_hash,),
    ).fetchone()[0] == 1
    reopened.close()
    lock.release()
