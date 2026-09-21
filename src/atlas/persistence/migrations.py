"""Deterministic schema migration/bootstrap (v1 -> v3)."""

from __future__ import annotations

import sqlite3

from .schema import DDL_STATEMENTS, SCHEMA_VERSION


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # 1. intents.state_version
    cols = _table_columns(conn, "intents")
    if "state_version" not in cols:
        cur.execute(
            "ALTER TABLE intents ADD COLUMN state_version INTEGER NOT NULL DEFAULT 0"
        )
    # 2. economic_events composite key: rebuild if old single-col PK shape present.
    # Detect old shape: PRIMARY KEY on venue_transaction_id alone (sql contains it).
    cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='economic_events'"
    )
    row = cur.fetchone()
    sql = row[0] if row else ""
    needs_rebuild = (
        "PRIMARY KEY (account" not in sql
        and "economic_events" in sql
    )
    if needs_rebuild:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS economic_events_new (
                account TEXT NOT NULL,
                venue_transaction_id TEXT NOT NULL,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                effective_time_ns INTEGER NOT NULL,
                received_at_ns INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                revision TEXT NOT NULL,
                PRIMARY KEY (account, venue_transaction_id)
            )
            """
        )
        # Copy rows that fit the new shape; old table had same columns.
        cur.execute(
            """
            INSERT OR IGNORE INTO economic_events_new
                (account, venue_transaction_id, currency, amount,
                 effective_time_ns, received_at_ns, event_type, revision)
            SELECT account, venue_transaction_id, currency, amount,
                   effective_time_ns, received_at_ns, event_type, revision
            FROM economic_events
            """
        )
        cur.execute("DROP TABLE economic_events")
        cur.execute("ALTER TABLE economic_events_new RENAME TO economic_events")


def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    """Create the append-only execution/status evidence tables."""
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        if (
            "CREATE TABLE IF NOT EXISTS execution_evidence" in stmt
            or "CREATE TABLE IF NOT EXISTS order_status_observations" in stmt
            or "CREATE TABLE IF NOT EXISTS reconciliation_query_evidence" in stmt
            or "CREATE TABLE IF NOT EXISTS recovery_incidents" in stmt
            or "CREATE TABLE IF NOT EXISTS capability_evidence_log" in stmt
            or "CREATE TABLE IF NOT EXISTS capability_qualification_log" in stmt
        ):
            cur.execute(stmt)


def bootstrap(conn: sqlite3.Connection) -> int:
    """Create schema idempotently; migrate v1->v2 deterministically. Returns version."""
    cur = conn.cursor()
    has_metadata = _metadata_exists(conn)
    existing: int | None = None
    if has_metadata:
        cur.execute("SELECT value FROM schema_metadata WHERE key='schema_version'")
        row = cur.fetchone()
        existing = int(row[0]) if row else None
    if existing == 1:
        for stmt in DDL_STATEMENTS:
            # Create missing tables without altering existing data layout first.
            if "CREATE TABLE IF NOT EXISTS intents" in stmt:
                cur.execute(stmt)
            elif "CREATE TABLE IF NOT EXISTS economic_events" in stmt:
                pass  # handled by rebuild below
            elif "CREATE TABLE IF NOT EXISTS schema_metadata" in stmt:
                cur.execute(stmt)
            else:
                cur.execute(stmt)
        _migrate_v1_to_v2(conn)
        _migrate_v2_to_v3(conn)
    elif existing == 2:
        _migrate_v2_to_v3(conn)
    else:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
    cur.execute(
        "INSERT INTO schema_metadata(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()
    return SCHEMA_VERSION


def _metadata_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_metadata'"
    )
    return cur.fetchone() is not None


def current_version(conn: sqlite3.Connection) -> int | None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT value FROM schema_metadata WHERE key='schema_version'")
    except sqlite3.OperationalError:
        return None
    row = cur.fetchone()
    return int(row[0]) if row else None
