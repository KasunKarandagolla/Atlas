from __future__ import annotations

from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryStatus,
    QueryType,
    ReconciliationQueryEvidence,
    make_query_evidence,
    merge_query_evidence,
)
from atlas.runtime.recovery import RecoveryIncident

T0 = 1_700_000_000_000_000_000


def _query(
    *,
    complete: Completeness,
    records: int = 0,
    retention_start: int | None = T0 - 10,
    retention_end: int | None = T0 + 10,
    query_id: str = "q",
) -> ReconciliationQueryEvidence:
    return make_query_evidence(
        query_id=query_id,
        query_type=QueryType.ORDER_HISTORY,
        account="acct",
        instrument="BTCUSDT",
        requested_interval_start_ns=T0,
        requested_interval_end_ns=T0 + 10,
        pagination_cursors=(query_id,),
        pages_observed=1,
        total_records_returned=records,
        completeness=complete,
        status=QueryStatus.SUCCESS,
        source_time_ns=T0,
        receipt_time_ns=T0 + 1,
        request_ids=(query_id,),
        retention_coverage_start_ns=retention_start,
        retention_coverage_end_ns=retention_end,
        error_message=None,
    )


def test_complete_plus_incomplete_never_certifies_absence():
    merged = merge_query_evidence(
        [_query(complete=Completeness.COMPLETE), _query(complete=Completeness.INCOMPLETE_PAGINATED, query_id="q2")]
    )
    assert merged is not None
    assert merged.completeness == Completeness.INCOMPLETE_PAGINATED
    assert not merged.can_certify_absence


def test_retention_must_span_requested_interval():
    evidence = _query(complete=Completeness.COMPLETE, retention_start=T0 + 1, retention_end=T0 + 10)
    assert not evidence.can_certify_absence


def test_query_and_recovery_incident_evidence_survive_restart(tmp_path):
    evidence = _query(complete=Completeness.INCOMPLETE_PAGINATED, query_id="persisted")
    path = tmp_path / "recovery.db"
    journal = SQLiteJournal(path)
    assert journal.append_reconciliation_query_evidence(evidence)
    journal.append_recovery_incident(
        RecoveryIncident("incident-1", "recovery-1", "UNKNOWN_SUBMIT", "OPEN", (evidence.evidence_hash,), T0)
    )
    journal.close()
    restarted = SQLiteJournal(path)
    assert restarted.load_reconciliation_query_evidence("persisted")[0].evidence_hash == evidence.evidence_hash
    assert restarted.load_recovery_incidents("recovery-1")[0].incident_id == "incident-1"
    restarted.close()
