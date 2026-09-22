from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, generate_client_order_id, make_command
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    Completeness,
    QueryScope,
    QueryStatus,
    QueryType,
    ReconciliationRun,
    ReconciliationRunState,
    make_query_evidence,
)

T0 = 1_800_000_000_000_000_000


@pytest.fixture
def journal(tmp_path):
    j = SQLiteJournal(tmp_path / "atlas.db")
    yield j
    j.close()


def add_intent(j: SQLiteJournal, intent_id="intent-1"):
    j._conn.execute(
        "INSERT OR IGNORE INTO trade_plans VALUES(?,?,?,?,?,?)", ("plan", "v1", "{}", "fixture", T0 + 1, T0)
    )
    j._conn.commit()
    i = Intent(
        intent_id,
        0,
        "plan",
        "v1",
        generate_client_order_id(),
        1,
        LifecycleState.INTENT_PERSISTED,
        ProtectionStatus.UNCONFIRMED,
        ReconciliationHealth.STALE,
        T0,
        0,
    )
    r = Reservation(
        "res-" + intent_id,
        intent_id,
        Decimal("0.01"),
        Decimal("10"),
        Decimal("20"),
        Decimal("500"),
        Decimal("500"),
        Decimal("100"),
        Decimal("5"),
    )
    j.create_intent_with_reservation(i, r)
    return i


def terminal_entry(j: SQLiteJournal, intent):
    c = make_command(
        command_id="entry-" + intent.intent_id,
        intent_id=intent.intent_id,
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"client_order_id": intent.client_order_id},
        expected_state_version=intent.state_version,
        created_at_ns=T0,
    )
    j.persist_command(c)
    j.mark_send_started(c.command_id, T0 + 1)
    j.update_command_outcome(c.command_id, CommandOutcome.RECONCILED)


def q(
    kind: QueryType,
    query_id: str,
    *,
    account="acct",
    instrument="BTCUSDT",
    records=0,
    facts=None,
    complete=True,
    status=QueryStatus.SUCCESS,
    receipt_time_ns=T0 + 1,
    requested_interval_start_ns=T0 - 100,
    requested_interval_end_ns=T0 + 100,
    retention_segments=((T0 - 200, T0 + 200),),
):
    scope = (
        QueryScope.ACCOUNT if kind in (QueryType.WALLET_BALANCE, QueryType.TRANSACTION_LOG) else QueryScope.INSTRUMENT
    )
    inst = None if scope == QueryScope.ACCOUNT else instrument
    return make_query_evidence(
        query_id=query_id,
        query_type=kind,
        scope=scope,
        account=account,
        instrument=inst,
        requested_interval_start_ns=requested_interval_start_ns,
        requested_interval_end_ns=requested_interval_end_ns,
        pagination_cursors=(query_id,),
        pages_observed=1,
        total_records_returned=records,
        completeness=Completeness.COMPLETE if complete else Completeness.INCOMPLETE_PAGINATED,
        status=status,
        source_time_ns=T0,
        receipt_time_ns=receipt_time_ns,
        request_ids=(query_id,),
        retention_segments=retention_segments,
        facts=facts or {},
        error_message=None,
    )


def completed_run(
    j: SQLiteJournal,
    run_id="run-1",
    account="acct",
    instrument="BTCUSDT",
    writer_id="writer",
    writer_epoch=1,
    runtime="runtime",
    extra_queries=(),
):
    run = ReconciliationRun(
        run_id,
        account,
        instrument,
        writer_id,
        writer_epoch,
        runtime,
        T0,
        None,
        DEFAULT_EXECUTION_RISK_QUERIES,
        ReconciliationRunState.OPEN,
    )
    j.create_reconciliation_run(run)
    for kind in DEFAULT_EXECUTION_RISK_QUERIES:
        facts = {"signed_qty": "0", "position_epoch": 0} if kind == QueryType.POSITIONS else {}
        e = q(kind, f"{run_id}-{kind.value}", account=account, instrument=instrument, facts=facts)
        j.append_reconciliation_query_evidence(e)
        j.bind_query_to_run(run_id, e.query_id)
    for e in extra_queries:
        j.append_reconciliation_query_evidence(e)
        j.bind_query_to_run(run_id, e.query_id)
    j.complete_reconciliation_run(run_id, T0 + 10)
    return j.load_reconciliation_run(run_id)
