"""Deterministic SQLite DDL (freeze §1.4 minimum durable records).

Append/audit-friendly: rows are inserted, never updated except for explicit
state-transition columns (approval consumption, command dispatch/outcome,
intent lifecycle). History is preserved via observations/economic events.

Schema v2 (Session 002 repairs):
- intents.state_version: monotonically increasing local state version (freeze §1.5).
- economic_events: composite identity (account, venue_transaction_id) per freeze
  (venue IDs are not globally unique across accounts).
"""

from __future__ import annotations

SCHEMA_VERSION = 2

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schema_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trade_plans (
        plan_id TEXT PRIMARY KEY,
        version TEXT NOT NULL,
        canonical_json TEXT NOT NULL,
        plan_hash TEXT NOT NULL,
        expires_at_ns INTEGER NOT NULL,
        created_at_ns INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS approvals (
        approval_id TEXT PRIMARY KEY,
        user_identity TEXT NOT NULL,
        plan_id TEXT NOT NULL REFERENCES trade_plans(plan_id),
        plan_version TEXT NOT NULL,
        approved_at_ns INTEGER NOT NULL,
        expires_at_ns INTEGER NOT NULL,
        consumed_at_ns INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS intents (
        intent_id TEXT PRIMARY KEY,
        position_epoch INTEGER NOT NULL,
        plan_id TEXT NOT NULL REFERENCES trade_plans(plan_id),
        plan_version TEXT NOT NULL,
        client_order_id TEXT NOT NULL UNIQUE,
        writer_epoch INTEGER NOT NULL,
        lifecycle TEXT NOT NULL,
        protection_status TEXT NOT NULL,
        reconciliation_health TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL,
        state_version INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS commands (
        command_id TEXT PRIMARY KEY,
        intent_id TEXT NOT NULL REFERENCES intents(intent_id),
        command_type TEXT NOT NULL,
        exact_payload_hash TEXT NOT NULL,
        payload TEXT NOT NULL,
        expected_state_version INTEGER NOT NULL,
        created_at_ns INTEGER NOT NULL,
        send_started_at_ns INTEGER,
        outcome TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id TEXT PRIMARY KEY,
        intent_id TEXT NOT NULL REFERENCES intents(intent_id),
        remaining_open_qty TEXT NOT NULL,
        normal_loss TEXT NOT NULL,
        stress_loss TEXT NOT NULL,
        notional TEXT NOT NULL,
        beta_adjusted_notional TEXT NOT NULL,
        margin TEXT NOT NULL,
        es_contribution TEXT NOT NULL,
        version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS observations (
        observation_id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        venue_identity TEXT NOT NULL,
        source_time_ns INTEGER,
        receive_time_ns INTEGER NOT NULL,
        raw_hash TEXT NOT NULL,
        request_id TEXT,
        query_interval_ns INTEGER,
        completeness TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS protection_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_epoch INTEGER NOT NULL,
        desired_stop_version INTEGER NOT NULL,
        qty TEXT NOT NULL,
        trigger_basis TEXT NOT NULL,
        stop_price TEXT NOT NULL,
        semantics TEXT NOT NULL,
        evidence_ids_json TEXT NOT NULL,
        observed_at_ns INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS economic_events (
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
    """,
]
