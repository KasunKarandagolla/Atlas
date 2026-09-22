"""Transactional SQLite bootstrap and the v4-to-v6 migrations.

Migration is intentionally explicit.  A v4 recovery certificate is not a v5
runtime artifact: its missing runtime identity and missing typed artifacts are
preserved as legacy compatibility metadata and cannot silently authorize a new
recovery decision.  Schema 6 adds durable one-use recovery-cycle claims and
backfills historical schema-5 consumption before exposing the new version.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .schema import (
    DDL_STATEMENTS,
    SCHEMA_VERSION,
    recovery_binding_ddl,
)

LEGACY_RUNTIME_INSTANCE_ID = "legacy-v4-runtime-unavailable"


def _metadata_exists(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_metadata'").fetchone() is not None
    )


def current_version(conn: sqlite3.Connection) -> int | None:
    if not _metadata_exists(conn):
        return None
    row = conn.execute("SELECT value FROM schema_metadata WHERE key='schema_version'").fetchone()
    return int(row[0]) if row else None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _decode_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _legacy_certificate_metadata(row: sqlite3.Row, columns: list[str]) -> dict[str, Any]:
    legacy: dict[str, Any] = {}
    for column in columns:
        legacy[column] = _decode_json(row[column])
    return {
        "legacy_schema": 4,
        "runtime_instance_identity": "UNAVAILABLE_LEGACY_V4",
        "historical_evidence_authoritative": False,
        "legacy_v4": legacy,
        "protection_uncertainty_summary": row["protection_uncertainty_summary"],
        "venue_observations_obtained": bool(row["venue_observations_obtained"]),
        "protection_evidence": {
            "certified_flat": bool(row["protection_certified_flat"])
            if row["protection_certified_flat"] is not None
            else False,
            "current_protection": bool(row["protection_current"]) if row["protection_current"] is not None else False,
            "evidence_refs": _decode_json(row["protection_evidence_refs_json"])
            if row["protection_evidence_refs_json"]
            else [],
        },
    }


def _rename_legacy_tables(conn: sqlite3.Connection) -> tuple[bool, bool]:
    """Rename v4-shaped tables before v5 DDL is applied.

    Returning flags keeps the work in one caller-owned transaction.  SQLite DDL
    participates in that transaction, so any exception restores the old names.
    """

    migrate_recovery = False
    if _table_exists(conn, "recovery_certificates"):
        columns = _columns(conn, "recovery_certificates")
        migrate_recovery = "runtime_instance_id" not in columns
        if migrate_recovery:
            conn.execute("ALTER TABLE recovery_certificates RENAME TO recovery_certificates_v4")

    migrate_queries = False
    if _table_exists(conn, "reconciliation_query_evidence"):
        columns = _columns(conn, "reconciliation_query_evidence")
        migrate_queries = "scope" not in columns or "retention_segments_json" not in columns
        if migrate_queries:
            conn.execute("ALTER TABLE reconciliation_query_evidence RENAME TO reconciliation_query_evidence_v4")

    return migrate_recovery, migrate_queries


def _copy_v4_recovery_certificates(conn: sqlite3.Connection) -> None:
    old_columns = _columns(conn, "recovery_certificates_v4")
    required = {
        "recovery_run_id",
        "writer_id",
        "writer_epoch",
        "journal_schema_version",
        "unresolved_intents_json",
        "unresolved_commands_json",
        "unknown_commands_json",
        "reconciliation_health",
        "protection_uncertainty_summary",
        "started_at_ns",
        "ended_at_ns",
        "evidence_refs_json",
        "venue_observations_obtained",
        "venue_evidence_refs_json",
        "decision",
        "protection_certified_flat",
        "protection_current",
        "protection_evidence_refs_json",
    }
    missing = sorted(required - old_columns)
    if missing:
        raise RuntimeError(f"v4 recovery_certificates missing columns: {missing}")

    rows = conn.execute("SELECT * FROM recovery_certificates_v4").fetchall()
    columns = [
        description[0] for description in conn.execute("SELECT * FROM recovery_certificates_v4 LIMIT 0").description
    ]
    insert_sql = """
        INSERT INTO recovery_certificates (
            recovery_run_id, runtime_instance_id, writer_id, writer_epoch,
            journal_schema_version, unresolved_intents_json,
            unresolved_commands_json, unknown_commands_json,
            reconciliation_health, started_at_ns, ended_at_ns,
            evidence_refs_json, venue_evidence_refs_json, decision,
            flat_certificate_id, protection_evidence_id, compatibility_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
    """
    for row in rows:
        metadata = _legacy_certificate_metadata(row, columns)
        conn.execute(
            insert_sql,
            (
                row["recovery_run_id"],
                LEGACY_RUNTIME_INSTANCE_ID,
                row["writer_id"],
                row["writer_epoch"],
                row["journal_schema_version"],
                row["unresolved_intents_json"],
                row["unresolved_commands_json"],
                row["unknown_commands_json"],
                row["reconciliation_health"],
                row["started_at_ns"],
                row["ended_at_ns"],
                row["evidence_refs_json"],
                row["venue_evidence_refs_json"],
                row["decision"],
                json.dumps(metadata, sort_keys=True),
            ),
        )
    conn.execute("DROP TABLE recovery_certificates_v4")


def _copy_v4_query_evidence(conn: sqlite3.Connection) -> None:
    old_columns = _columns(conn, "reconciliation_query_evidence_v4")
    required = {
        "query_id",
        "query_type",
        "account",
        "instrument",
        "requested_interval_start_ns",
        "requested_interval_end_ns",
        "pagination_cursors_json",
        "pages_observed",
        "total_records_returned",
        "completeness",
        "status",
        "source_time_ns",
        "receipt_time_ns",
        "request_ids_json",
        "retention_coverage_start_ns",
        "retention_coverage_end_ns",
        "evidence_hash",
        "error_message",
    }
    missing = sorted(required - old_columns)
    if missing:
        raise RuntimeError(f"v4 reconciliation evidence missing columns: {missing}")

    rows = conn.execute("SELECT * FROM reconciliation_query_evidence_v4").fetchall()
    insert_sql = """
        INSERT INTO reconciliation_query_evidence (
            query_id, query_type, scope, account, instrument,
            requested_interval_start_ns, requested_interval_end_ns,
            pagination_cursors_json, pages_observed, total_records_returned,
            completeness, status, source_time_ns, receipt_time_ns,
            request_ids_json, retention_segments_json, facts_json,
            evidence_hash, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for row in rows:
        scope = "account" if row["instrument"] is None else "instrument"
        start = row["retention_coverage_start_ns"]
        end = row["retention_coverage_end_ns"]
        segments = [] if start is None or end is None else [[start, end]]
        legacy_facts = {
            "legacy_schema": 4,
            "historical_hash_binding": "UNVERIFIED_V4_HASH",
        }
        conn.execute(
            insert_sql,
            (
                row["query_id"],
                row["query_type"],
                scope,
                row["account"],
                row["instrument"],
                row["requested_interval_start_ns"],
                row["requested_interval_end_ns"],
                row["pagination_cursors_json"],
                row["pages_observed"],
                row["total_records_returned"],
                row["completeness"],
                row["status"],
                row["source_time_ns"],
                row["receipt_time_ns"],
                row["request_ids_json"],
                json.dumps(segments),
                json.dumps(legacy_facts, sort_keys=True),
                row["evidence_hash"],
                row["error_message"],
            ),
        )
    conn.execute("DROP TABLE reconciliation_query_evidence_v4")


def _add_v5_columns(conn: sqlite3.Connection) -> None:
    if _table_exists(conn, "intents") and "state_version" not in _columns(conn, "intents"):
        conn.execute("ALTER TABLE intents ADD COLUMN state_version INTEGER NOT NULL DEFAULT 0")
    if _table_exists(conn, "recovery_certificates"):
        columns = _columns(conn, "recovery_certificates")
        if "compatibility_json" not in columns:
            conn.execute("ALTER TABLE recovery_certificates ADD COLUMN compatibility_json TEXT NOT NULL DEFAULT '{}'")
    if _table_exists(conn, "protection_evidence"):
        columns = _columns(conn, "protection_evidence")
        if "market_stop_semantics" not in columns:
            conn.execute("ALTER TABLE protection_evidence ADD COLUMN market_stop_semantics INTEGER NOT NULL DEFAULT 0")


def _add_v6_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "recovery_reconciliation_bindings"):
        return
    columns = _columns(conn, "recovery_reconciliation_bindings")
    if "legacy_multi_use" not in columns:
        conn.execute(
            "ALTER TABLE recovery_reconciliation_bindings "
            "ADD COLUMN legacy_multi_use INTEGER NOT NULL DEFAULT 0"
        )
    if "legacy_recovery_ids_json" not in columns:
        conn.execute(
            "ALTER TABLE recovery_reconciliation_bindings "
            "ADD COLUMN legacy_recovery_ids_json TEXT NOT NULL DEFAULT '[]'"
        )


def _decode_evidence_refs(value: Any, recovery_run_id: str) -> tuple[str, ...]:
    decoded = _decode_json(value)
    if not isinstance(decoded, list) or any(not isinstance(item, str) for item in decoded):
        raise RuntimeError(f"v5 recovery certificate {recovery_run_id} has invalid evidence_refs_json")
    refs = []
    for item in decoded:
        if item.startswith("reconciliation-run:"):
            referenced_run_id = item.removeprefix("reconciliation-run:")
            if not referenced_run_id:
                raise RuntimeError(f"v5 recovery certificate {recovery_run_id} has an empty reconciliation run ref")
            refs.append(referenced_run_id)
    return tuple(dict.fromkeys(refs))


def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """Create durable run claims and account for all historical v5 claims.

    Historical certificates remain byte-for-byte unchanged.  If schema 5
    allowed multiple certificates to reference one run, the first
    deterministic certificate owns the blocking binding and the complete
    historical set is recorded on that binding as a legacy conflict.
    """

    conn.execute(recovery_binding_ddl())
    _add_v6_columns(conn)
    rows = conn.execute(
        "SELECT recovery_run_id,runtime_instance_id,writer_id,writer_epoch,ended_at_ns,evidence_refs_json "
        "FROM recovery_certificates ORDER BY recovery_run_id"
    ).fetchall()
    consumers: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        for reconciliation_run_id in _decode_evidence_refs(row["evidence_refs_json"], row["recovery_run_id"]):
            consumers.setdefault(reconciliation_run_id, []).append(row)

    for reconciliation_run_id, historical_rows in sorted(consumers.items()):
        run = conn.execute(
            "SELECT run_id FROM reconciliation_runs WHERE run_id=?", (reconciliation_run_id,)
        ).fetchone()
        if run is None:
            raise RuntimeError(
                f"v5 recovery certificate references missing reconciliation run {reconciliation_run_id}"
            )
        historical_ids = tuple(sorted({row["recovery_run_id"] for row in historical_rows}))
        existing = conn.execute(
            "SELECT * FROM recovery_reconciliation_bindings WHERE reconciliation_run_id=?",
            (reconciliation_run_id,),
        ).fetchone()
        if existing is not None:
            if existing["recovery_run_id"] not in historical_ids:
                raise RuntimeError(
                    f"v5 binding for {reconciliation_run_id} does not match historical recovery evidence"
                )
            legacy_multi_use = int(existing["legacy_multi_use"]) or len(historical_ids) > 1
            conn.execute(
                "UPDATE recovery_reconciliation_bindings "
                "SET legacy_multi_use=?,legacy_recovery_ids_json=? WHERE reconciliation_run_id=?",
                (legacy_multi_use, json.dumps(historical_ids), reconciliation_run_id),
            )
            continue

        selected = next(row for row in historical_rows if row["recovery_run_id"] == historical_ids[0])
        conn.execute(
            """INSERT INTO recovery_reconciliation_bindings(
                reconciliation_run_id,recovery_run_id,runtime_instance_id,
                writer_id,writer_epoch,claimed_at_ns,legacy_multi_use,
                legacy_recovery_ids_json
            ) VALUES(?,?,?,?,?,?,?,?)""",
            (
                reconciliation_run_id,
                selected["recovery_run_id"],
                selected["runtime_instance_id"],
                selected["writer_id"],
                selected["writer_epoch"],
                selected["ended_at_ns"],
                int(len(historical_ids) > 1),
                json.dumps(historical_ids),
            ),
        )


def bootstrap(conn: sqlite3.Connection) -> int:
    """Create or migrate the journal, committing schema metadata last."""

    existing = current_version(conn)
    if existing is not None and existing > SCHEMA_VERSION:
        raise RuntimeError(f"future SQLite schema {existing} > supported {SCHEMA_VERSION}; fail closed")

    # A journal opens a fresh sqlite connection, but refusing to nest a second
    # transaction avoids accidentally committing a caller's unrelated work.
    if conn.in_transaction:
        raise RuntimeError("schema bootstrap requires a clean SQLite transaction")

    conn.execute("BEGIN IMMEDIATE")
    try:
        binding_statement = recovery_binding_ddl()
        if existing == 5:
            # A pre-binding v5 journal must not acquire an empty v6 table
            # through ordinary CREATE IF NOT EXISTS processing.
            for statement in DDL_STATEMENTS:
                if statement != binding_statement:
                    conn.execute(statement)
            _add_v5_columns(conn)
            _migrate_v5_to_v6(conn)
        else:
            migrate_recovery, migrate_queries = _rename_legacy_tables(conn)
            for statement in DDL_STATEMENTS:
                conn.execute(statement)
            if migrate_recovery:
                _copy_v4_recovery_certificates(conn)
            if migrate_queries:
                _copy_v4_query_evidence(conn)
            _add_v5_columns(conn)
        conn.execute(
            "INSERT INTO schema_metadata(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return SCHEMA_VERSION
