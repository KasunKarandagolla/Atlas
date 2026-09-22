from __future__ import annotations

import pytest
from conftest import T0, q

from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    QueryType,
    ReconciliationRun,
    ReconciliationRunState,
)


def _open_run(journal, run_id: str, *, started_at_ns: int = T0) -> None:
    journal.create_reconciliation_run(
        ReconciliationRun(
            run_id,
            "acct",
            "BTCUSDT",
            "writer",
            1,
            "runtime",
            started_at_ns,
            None,
            DEFAULT_EXECUTION_RISK_QUERIES,
            ReconciliationRunState.OPEN,
        )
    )


def _bind_complete_set(journal, run_id: str, *, retention_segments=((T0 - 200, T0 + 200),)) -> None:
    for query_type in DEFAULT_EXECUTION_RISK_QUERIES:
        facts = {"signed_qty": "0", "position_epoch": 0} if query_type == QueryType.POSITIONS else {}
        evidence = q(
            query_type,
            f"{run_id}-{query_type.value}",
            facts=facts,
            retention_segments=retention_segments,
        )
        journal.append_reconciliation_query_evidence(evidence)
        journal.bind_query_to_run(run_id, evidence.query_id)


def test_query_received_before_run_start_cannot_be_bound(journal):
    evidence = q(QueryType.POSITIONS, "before", receipt_time_ns=T0 + 1, facts={"signed_qty": "0"})
    journal.append_reconciliation_query_evidence(evidence)
    _open_run(journal, "late-start", started_at_ns=T0 + 10)

    with pytest.raises(PersistenceError, match="before reconciliation run started"):
        journal.bind_query_to_run("late-start", "before")


def test_query_received_after_requested_completion_cannot_complete(journal):
    _open_run(journal, "future-query")
    evidence = q(QueryType.POSITIONS, "future", receipt_time_ns=T0 + 20, facts={"signed_qty": "0"})
    journal.append_reconciliation_query_evidence(evidence)
    journal.bind_query_to_run("future-query", "future")

    with pytest.raises(PersistenceError, match="outside reconciliation run"):
        journal.complete_reconciliation_run("future-query", T0 + 10)


def test_missing_history_retention_coverage_cannot_complete(journal):
    _open_run(journal, "missing-retention")
    _bind_complete_set(journal, "missing-retention", retention_segments=())

    with pytest.raises(PersistenceError, match="retention"):
        journal.complete_reconciliation_run("missing-retention", T0 + 10)


def test_partial_history_retention_coverage_cannot_complete(journal):
    _open_run(journal, "partial-retention")
    _bind_complete_set(journal, "partial-retention", retention_segments=((T0 - 100, T0),))

    with pytest.raises(PersistenceError, match="retention"):
        journal.complete_reconciliation_run("partial-retention", T0 + 10)


def test_disjoint_history_retention_coverage_cannot_complete(journal):
    _open_run(journal, "gap-retention")
    _bind_complete_set(
        journal,
        "gap-retention",
        retention_segments=((T0 - 200, T0 - 1), (T0 + 1, T0 + 200)),
    )

    with pytest.raises(PersistenceError, match="retention"):
        journal.complete_reconciliation_run("gap-retention", T0 + 10)


def test_continuous_history_retention_coverage_can_complete(journal):
    _open_run(journal, "full-retention")
    _bind_complete_set(journal, "full-retention")
    journal.complete_reconciliation_run("full-retention", T0 + 10)

    assert journal.load_reconciliation_run("full-retention").state == ReconciliationRunState.COMPLETE
