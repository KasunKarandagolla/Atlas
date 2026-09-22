from __future__ import annotations

import pytest
from conftest import T0, q

from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    QueryType,
    ReconciliationRun,
    ReconciliationRunState,
    covers_interval,
    merge_query_evidence,
)


def test_disjoint_retention_segments_do_not_manufacture_continuous_coverage():
    a = q(QueryType.ORDER_HISTORY, "a")
    b = q(QueryType.ORDER_HISTORY, "b")
    a = type(a)(
        *(
            [getattr(a, f) for f in a.__dataclass_fields__][:15]
            + [((T0 - 200, T0 - 50),), a.facts, a.evidence_hash, a.error_message]
        )
    )
    b = type(b)(
        *(
            [getattr(b, f) for f in b.__dataclass_fields__][:15]
            + [((T0 + 50, T0 + 200),), b.facts, b.evidence_hash, b.error_message]
        )
    )
    merged = merge_query_evidence([a, b])
    assert merged is not None
    assert not covers_interval(merged.retention_segments, T0 - 100, T0 + 100)
    assert not merged.can_certify_absence


def test_account_and_instrument_scopes_may_coexist_in_one_run(journal):
    run = ReconciliationRun(
        "r", "acct", "BTCUSDT", "w", 1, "rt", T0, None, DEFAULT_EXECUTION_RISK_QUERIES, ReconciliationRunState.OPEN
    )
    journal.create_reconciliation_run(run)
    wallet = q(QueryType.WALLET_BALANCE, "wallet")
    position = q(QueryType.POSITIONS, "pos", facts={"signed_qty": "0"})
    journal.append_reconciliation_query_evidence(wallet)
    journal.append_reconciliation_query_evidence(position)
    journal.bind_query_to_run("r", "wallet")
    journal.bind_query_to_run("r", "pos")
    qs = journal.load_run_queries("r")
    assert {x.query_type for x in qs} == {QueryType.WALLET_BALANCE, QueryType.POSITIONS}


def test_cross_run_binding_does_not_implicitly_happen(journal):
    e = q(QueryType.POSITIONS, "q", facts={"signed_qty": "0"})
    journal.append_reconciliation_query_evidence(e)
    r1 = ReconciliationRun(
        "r1", "acct", "BTCUSDT", "w", 1, "rt", T0, None, DEFAULT_EXECUTION_RISK_QUERIES, ReconciliationRunState.OPEN
    )
    r2 = ReconciliationRun(
        "r2", "acct", "BTCUSDT", "w", 1, "rt", T0, None, DEFAULT_EXECUTION_RISK_QUERIES, ReconciliationRunState.OPEN
    )
    journal.create_reconciliation_run(r1)
    journal.create_reconciliation_run(r2)
    journal.bind_query_to_run("r1", "q")
    assert len(journal.load_run_queries("r1")) == 1 and len(journal.load_run_queries("r2")) == 0
    with pytest.raises(PersistenceError, match="already bound"):
        journal.bind_query_to_run("r2", "q")
