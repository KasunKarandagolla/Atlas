"""Deterministic schema migration/bootstrap."""

from __future__ import annotations

import sqlite3

from .schema import DDL_STATEMENTS, SCHEMA_VERSION


def bootstrap(conn: sqlite3.Connection) -> int:
    """Create schema idempotently; record version in schema_metadata. Returns version."""
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)
    cur.execute(
        "INSERT INTO schema_metadata(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()
    return SCHEMA_VERSION


def current_version(conn: sqlite3.Connection) -> int | None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT value FROM schema_metadata WHERE key='schema_version'")
    except sqlite3.OperationalError:
        return None
    row = cur.fetchone()
    return int(row[0]) if row else None
