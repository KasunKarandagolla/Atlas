from __future__ import annotations

import pytest

from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryStatus,
    QueryType,
    ReconciliationQueryEvidence,
    compute_evidence_hash,
)
from atlas.runtime.recovery import (
    RecoveryDecision,
    RecoveryProtectionEvidence,
    recover_from_persisted_evidence,
)

T0 = 1_700_000_000_000_000_000


def _query(query_id: str, *, complete: bool = True) -> ReconciliationQueryEvidence:
    completeness = Completeness.COMPLETE if complete else Completeness.INCOMPLETE_PAGINATED
    return ReconciliationQueryEvidence(
        query_id=query_id,
        query_type=QueryType.POSITIONS,
        account="acct-hash",
        instrument="BTCUSDT",
        requested_interval_start_ns=T0 - 10,
        requested_interval_end_ns=T0 + 10,
        pagination_cursors=(query_id,),
        pages_observed=1,
        total_records_returned=0,
        completeness=completeness,
        status=QueryStatus.SUCCESS,
        source_time_ns=T0,
        receipt_time_ns=T0 + 1,
        request_ids=(query_id,),
        retention_coverage_start_ns=T0 - 10,
        retention_coverage_end_ns=T0 + 10,
        evidence_hash=compute_evidence_hash({"query_id": query_id, "complete": complete}),
        error_message=None,
    )


def test_restart_reconstructs_query_evidence_and_persists_ready_certificate(tmp_path):
    path = tmp_path / "recovery.db"
    journal = SQLiteJournal(path)
    assert journal.append_reconciliation_query_evidence(_query("query-ready"))
    journal.close()

    restarted = SQLiteJournal(path)
    certificate = recover_from_persisted_evidence(
        journal=restarted,
        recovery_run_id="recovery-ready",
        writer_id="writer-1",
        writer_epoch=7,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct-hash",
        instrument="BTCUSDT",
        query_ids=("query-ready",),
        reconciliation_started_at_ns=T0,
        reconciliation_completed_at_ns=T0 + 2,
        protection_uncertainty_summary="typed flat certificate supplied",
        started_at_ns=T0,
        ended_at_ns=T0 + 3,
        prerequisites_ok=True,
        protection_evidence=RecoveryProtectionEvidence(True, False, ("flat-cert-1",)),
    )
    assert certificate.decision == RecoveryDecision.READY
    restored = restarted.load_recovery_certificate("recovery-ready")
    assert restored == certificate
    assert restarted.load_recovery_incidents("recovery-ready") == []
    assert restarted.schema_version() == 4
    restarted.close()


def test_incomplete_reconstructed_evidence_creates_recovery_incident(tmp_path):
    journal = SQLiteJournal(tmp_path / "recovery.db")
    assert journal.append_reconciliation_query_evidence(_query("query-incomplete", complete=False))
    certificate = recover_from_persisted_evidence(
        journal=journal,
        recovery_run_id="recovery-incomplete",
        writer_id="writer-1",
        writer_epoch=7,
        unresolved_intent_ids=(),
        unresolved_command_ids=("cmd-unknown",),
        unknown_command_ids=("cmd-unknown",),
        account="acct-hash",
        instrument="BTCUSDT",
        query_ids=("query-incomplete",),
        reconciliation_started_at_ns=T0,
        reconciliation_completed_at_ns=T0 + 2,
        protection_uncertainty_summary="query pagination incomplete",
        started_at_ns=T0,
        ended_at_ns=T0 + 3,
        prerequisites_ok=True,
        protection_evidence=RecoveryProtectionEvidence(False, True, ("protection-query-1",)),
    )
    assert certificate.decision == RecoveryDecision.RECOVERY_REQUIRED
    incidents = journal.load_recovery_incidents("recovery-incomplete")
    assert len(incidents) == 1
    assert incidents[0].evidence_refs == certificate.evidence_refs
    journal.close()


def test_reconstruction_requires_exact_persisted_identity_and_ids(tmp_path):
    journal = SQLiteJournal(tmp_path / "recovery.db")
    journal.append_reconciliation_query_evidence(_query("query-identity"))
    with pytest.raises(PersistenceError, match="missing"):
        journal.load_reconciliation_evidence_bundle(
            reconciliation_run_id="run",
            account="acct-hash",
            instrument="BTCUSDT",
            query_ids=("not-persisted",),
            started_at_ns=T0,
            completed_at_ns=T0 + 1,
        )
    with pytest.raises(PersistenceError, match="identity"):
        journal.load_reconciliation_evidence_bundle(
            reconciliation_run_id="run",
            account="wrong-account",
            instrument="BTCUSDT",
            query_ids=("query-identity",),
            started_at_ns=T0,
            completed_at_ns=T0 + 1,
        )
    journal.close()
