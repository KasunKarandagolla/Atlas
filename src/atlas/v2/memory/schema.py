"""Versioned schema for the separate, single-writer atlas-ops SQLite store."""

from __future__ import annotations

import sqlite3

OPS_SCHEMA_VERSION = 1
OPS_SCHEMA_NAMESPACE = "atlas-ops"
_REQUIRED_TABLES = {
    "schema_meta", "watch", "watch_transition", "ops_outbox", "source_health",
    "model_registry", "artifact_index",
}

_DDL = (
    """CREATE TABLE schema_meta (
        namespace TEXT PRIMARY KEY,
        schema_version INTEGER NOT NULL CHECK (schema_version >= 1)
    )""",
    """CREATE TABLE watch (
        watch_id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        state_version INTEGER NOT NULL CHECK (state_version >= 0),
        expires_at_ns INTEGER NOT NULL,
        required_next_event TEXT NOT NULL,
        last_event_id TEXT,
        last_evaluated_at_ns INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL
    )""",
    """CREATE TABLE watch_transition (
        watch_id TEXT NOT NULL REFERENCES watch(watch_id),
        event_id TEXT NOT NULL,
        state_version INTEGER NOT NULL CHECK (state_version >= 0),
        from_state TEXT NOT NULL,
        to_state TEXT NOT NULL,
        event_at_ns INTEGER NOT NULL,
        transition_at_ns INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        result_watch_json TEXT NOT NULL,
        PRIMARY KEY (watch_id, event_id, state_version)
    )""",
    """CREATE TABLE ops_outbox (
        outbox_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL REFERENCES watch(watch_id),
        event_id TEXT NOT NULL,
        state_version INTEGER NOT NULL,
        dedupe_key TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL,
        handled_at_ns INTEGER,
        handling_ref TEXT,
        FOREIGN KEY (watch_id, event_id, state_version)
            REFERENCES watch_transition(watch_id, event_id, state_version)
    )""",
    """CREATE TABLE source_health (
        source_id TEXT NOT NULL,
        observed_at_ns INTEGER NOT NULL,
        available_at_ns INTEGER NOT NULL,
        status TEXT NOT NULL,
        details_ref TEXT,
        payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        PRIMARY KEY (source_id, observed_at_ns)
    )""",
    """CREATE TABLE model_registry (
        manifest_hash TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        checkpoint_id TEXT NOT NULL,
        promotion_status TEXT NOT NULL,
        manifest_json TEXT NOT NULL
    )""",
    """CREATE TABLE artifact_index (
        artifact_ref TEXT PRIMARY KEY,
        artifact_type TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL,
        available_at_ns INTEGER NOT NULL,
        metadata_json TEXT NOT NULL
    )""",
    "CREATE INDEX watch_active_order ON watch(state, expires_at_ns, watch_id)",
    "CREATE INDEX outbox_pending_order ON ops_outbox(handled_at_ns, created_at_ns, outbox_id)",
)


def initialize(connection: sqlite3.Connection) -> None:
    """Initialize only an empty ops DB, or validate the known schema version."""
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if "schema_meta" in tables:
        rows = connection.execute(
            "SELECT schema_version FROM schema_meta WHERE namespace=?", (OPS_SCHEMA_NAMESPACE,)
        ).fetchall()
        if len(rows) != 1:
            raise RuntimeError("atlas-ops schema metadata is missing or ambiguous")
        version = rows[0][0]
        if type(version) is not int or version > OPS_SCHEMA_VERSION:
            raise RuntimeError(f"unsupported future atlas-ops schema version: {version!r}")
        if version != OPS_SCHEMA_VERSION:
            raise RuntimeError(f"unsupported atlas-ops schema version: {version!r}")
        if not _REQUIRED_TABLES.issubset(tables):
            raise RuntimeError("atlas-ops schema is incomplete")
        return
    if tables:
        raise RuntimeError("database has tables but is not an initialized atlas-ops store")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in _DDL:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_meta(namespace, schema_version) VALUES (?, ?)",
            (OPS_SCHEMA_NAMESPACE, OPS_SCHEMA_VERSION),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
