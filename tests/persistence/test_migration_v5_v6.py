from __future__ import annotations

import json
import sqlite3

import pytest

from atlas.persistence.migrations import bootstrap, current_version
from atlas.persistence.schema import DDL_STATEMENTS, recovery_binding_ddl
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    Completeness,
    QueryScope,
    QueryStatus,
    QueryType,
    ReconciliationRunState,
    make_query_evidence,
)
from atlas.runtime.recovery import recover_from_persisted_run

T0 = 1_900_000_000_000_000_000


def _insert_query(conn: sqlite3.Connection, query) -> None:
    conn.execute(
        """INSERT INTO reconciliation_query_evidence VALUES(
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""",
        (
            query.query_id,
            query.query_type.value,
            query.scope.value,
            query.account,
            query.instrument,
            query.requested_interval_start_ns,
            query.requested_interval_end_ns,
            json.dumps(list(query.pagination_cursors)),
            query.pages_observed,
            query.total_records_returned,
            query.completeness.value,
            query.status.value,
            query.source_time_ns,
            query.receipt_time_ns,
            json.dumps(list(query.request_ids)),
            json.dumps([list(segment) for segment in query.retention_segments]),
            json.dumps(query.facts, sort_keys=True),
            query.evidence_hash,
            query.error_message,
        ),
    )


def _make_query(run_id: str, query_type: QueryType):
    account_scope = query_type in {QueryType.WALLET_BALANCE, QueryType.TRANSACTION_LOG}
    instrument = None if account_scope else "BTCUSDT"
    scope = QueryScope.ACCOUNT if account_scope else QueryScope.INSTRUMENT
    facts = {"signed_qty": "0", "position_epoch": 0} if query_type == QueryType.POSITIONS else {}
    return make_query_evidence(
        query_id=f"{run_id}-{query_type.value}",
        query_type=query_type,
        scope=scope,
        account="acct",
        instrument=instrument,
        requested_interval_start_ns=T0 - 100,
        requested_interval_end_ns=T0 + 100,
        pagination_cursors=(f"{run_id}-page",),
        pages_observed=1,
        total_records_returned=0,
        completeness=Completeness.COMPLETE,
        status=QueryStatus.SUCCESS,
        source_time_ns=T0,
        receipt_time_ns=T0 + 1,
        request_ids=(f"{run_id}-request",),
        retention_segments=((T0 - 200, T0 + 200),),
        facts=facts,
        error_message=None,
    )


def _create_prebinding_v5_database(
    path, *, certificate_ids: tuple[str, ...] = ("recovery-old",), include_run: bool = True
) -> None:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    for statement in DDL_STATEMENTS:
        if statement != recovery_binding_ddl():
            conn.execute(statement)
    conn.execute("INSERT INTO schema_metadata(key,value) VALUES('schema_version','5')")
    if include_run:
        conn.execute(
            """INSERT INTO reconciliation_runs VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                "run-old",
                "acct",
                "BTCUSDT",
                "writer",
                1,
                "runtime",
                T0,
                T0 + 10,
                json.dumps([query_type.value for query_type in DEFAULT_EXECUTION_RISK_QUERIES]),
                ReconciliationRunState.COMPLETE.value,
            ),
        )
        for query_type in DEFAULT_EXECUTION_RISK_QUERIES:
            query = _make_query("run-old", query_type)
            _insert_query(conn, query)
            conn.execute(
                "INSERT INTO reconciliation_run_queries(run_id,query_id) VALUES(?,?)",
                ("run-old", query.query_id),
            )
    for recovery_run_id in certificate_ids:
        conn.execute(
            """INSERT INTO recovery_certificates(
                recovery_run_id,runtime_instance_id,writer_id,writer_epoch,
                journal_schema_version,unresolved_intents_json,
                unresolved_commands_json,unknown_commands_json,
                reconciliation_health,started_at_ns,ended_at_ns,
                evidence_refs_json,venue_evidence_refs_json,decision,
                flat_certificate_id,protection_evidence_id,compatibility_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                recovery_run_id,
                "runtime",
                "writer",
                1,
                5,
                "[]",
                "[]",
                "[]",
                "CURRENT",
                T0,
                T0 + 20,
                json.dumps(["reconciliation-run:run-old", f"evidence:{recovery_run_id}"]),
                json.dumps([f"venue:{recovery_run_id}"]),
                "READY",
                "flat-old",
                None,
                "{}",
            ),
        )
    conn.commit()
    conn.close()


def _attempt_new_recovery(journal: SQLiteJournal, recovery_run_id: str) -> None:
    recover_from_persisted_run(
        journal=journal,
        recovery_run_id=recovery_run_id,
        reconciliation_run_id="run-old",
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
        flat_certificate_id="missing-flat",
    )


def test_prebinding_v5_consumption_is_migrated_and_cannot_be_reused(tmp_path):
    path = tmp_path / "prebinding-v5.db"
    _create_prebinding_v5_database(path)

    journal = SQLiteJournal(path)
    assert journal.schema_version() == 6
    assert journal.count("recovery_reconciliation_bindings") == 1
    historical = journal.load_recovery_certificate("recovery-old")
    assert historical is not None
    assert historical.journal_schema_version == 5
    assert historical.evidence_refs[0] == "reconciliation-run:run-old"
    binding = journal._conn.execute(
        "SELECT * FROM recovery_reconciliation_bindings WHERE reconciliation_run_id='run-old'"
    ).fetchone()
    assert binding["recovery_run_id"] == "recovery-old"
    assert binding["legacy_multi_use"] == 0
    with pytest.raises(PersistenceError, match="already bound"):
        _attempt_new_recovery(journal, "recovery-new")
    with pytest.raises(PersistenceError):
        _attempt_new_recovery(journal, "recovery-old")
    assert journal.count("recovery_reconciliation_bindings") == 1
    journal.close()

    reopened = SQLiteJournal(path)
    assert reopened.load_recovery_certificate("recovery-old") == historical
    assert reopened.schema_version() == 6
    reopened.close()


def test_prebinding_v5_multi_use_history_is_preserved_and_blocked(tmp_path):
    path = tmp_path / "prebinding-v5-multi.db"
    _create_prebinding_v5_database(path, certificate_ids=("recovery-old-b", "recovery-old-a"))

    journal = SQLiteJournal(path)
    assert journal.schema_version() == 6
    assert journal.load_recovery_certificate("recovery-old-a") is not None
    assert journal.load_recovery_certificate("recovery-old-b") is not None
    binding = journal._conn.execute(
        "SELECT * FROM recovery_reconciliation_bindings WHERE reconciliation_run_id='run-old'"
    ).fetchone()
    assert binding["recovery_run_id"] == "recovery-old-a"
    assert binding["legacy_multi_use"] == 1
    assert json.loads(binding["legacy_recovery_ids_json"]) == ["recovery-old-a", "recovery-old-b"]
    with pytest.raises(PersistenceError, match="already bound"):
        _attempt_new_recovery(journal, "recovery-new")
    assert journal.count("recovery_certificates") == 2
    journal.close()


def test_v5_to_v6_migration_rolls_back_on_unresolvable_consumption(tmp_path):
    path = tmp_path / "prebinding-v5-broken.db"
    _create_prebinding_v5_database(path, include_run=False)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    with pytest.raises(RuntimeError, match="missing reconciliation run"):
        bootstrap(conn)
    assert current_version(conn) == 5
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='recovery_reconciliation_bindings'"
    ).fetchone() is None
    assert conn.execute("SELECT COUNT(*) FROM recovery_certificates").fetchone()[0] == 1
    conn.close()


def test_fresh_journal_is_schema_v6_with_durable_binding_table(tmp_path):
    journal = SQLiteJournal(tmp_path / "fresh-v6.db")
    assert journal.schema_version() == 6
    assert journal.count("recovery_reconciliation_bindings") == 0
    journal.close()
