"""SQLite control-plane schema for the frozen ATLAS v1 journal.

The schema is deliberately boring: durable control facts are relational, while
raw/evidence payloads remain immutable JSON or content hashes. Foreign keys that
were part of the v4 journal are retained in fresh v5 databases.
"""

from __future__ import annotations

SCHEMA_VERSION = 5

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
    CREATE TABLE IF NOT EXISTS reservation_release_events (
        release_id TEXT PRIMARY KEY,
        intent_id TEXT NOT NULL,
        certificate_id TEXT NOT NULL,
        previous_vector_json TEXT NOT NULL,
        released_at_ns INTEGER NOT NULL
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
    """
    CREATE TABLE IF NOT EXISTS execution_evidence (
        execution_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        client_order_id TEXT NOT NULL,
        intent_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        side TEXT NOT NULL,
        qty TEXT NOT NULL,
        price TEXT NOT NULL,
        fee TEXT NOT NULL,
        fee_currency TEXT NOT NULL,
        trade_time_ns INTEGER NOT NULL,
        receive_time_ns INTEGER NOT NULL,
        source TEXT NOT NULL,
        raw_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_status_observations (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        client_order_id TEXT NOT NULL,
        intent_id TEXT NOT NULL,
        status TEXT NOT NULL,
        cum_exec_qty TEXT NOT NULL,
        cum_exec_fee TEXT NOT NULL,
        cum_exec_value TEXT NOT NULL,
        avg_exec_price TEXT,
        receive_time_ns INTEGER NOT NULL,
        source TEXT NOT NULL,
        raw_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_query_evidence (
        query_id TEXT PRIMARY KEY,
        query_type TEXT NOT NULL,
        scope TEXT NOT NULL,
        account TEXT NOT NULL,
        instrument TEXT,
        requested_interval_start_ns INTEGER,
        requested_interval_end_ns INTEGER,
        pagination_cursors_json TEXT NOT NULL,
        pages_observed INTEGER NOT NULL,
        total_records_returned INTEGER NOT NULL,
        completeness TEXT NOT NULL,
        status TEXT NOT NULL,
        source_time_ns INTEGER,
        receipt_time_ns INTEGER NOT NULL,
        request_ids_json TEXT NOT NULL,
        retention_segments_json TEXT NOT NULL,
        facts_json TEXT NOT NULL,
        evidence_hash TEXT NOT NULL,
        error_message TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_runs (
        run_id TEXT PRIMARY KEY,
        account TEXT NOT NULL,
        instrument TEXT,
        writer_id TEXT NOT NULL,
        writer_epoch INTEGER NOT NULL,
        runtime_instance_id TEXT NOT NULL,
        started_at_ns INTEGER NOT NULL,
        completed_at_ns INTEGER,
        required_query_types_json TEXT NOT NULL,
        state TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_run_queries (
        run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
        query_id TEXT NOT NULL UNIQUE REFERENCES reconciliation_query_evidence(query_id),
        PRIMARY KEY (run_id, query_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS protection_evidence (
        evidence_id TEXT PRIMARY KEY,
        account_ref TEXT NOT NULL,
        instrument TEXT NOT NULL,
        position_epoch INTEGER NOT NULL,
        desired_stop_version INTEGER NOT NULL,
        observed_signed_qty TEXT NOT NULL,
        full_position_semantics INTEGER NOT NULL,
        stop_price TEXT NOT NULL,
        trigger_basis TEXT NOT NULL,
        market_stop_semantics INTEGER NOT NULL,
        closing_only_behavior INTEGER NOT NULL,
        position_view_evidence_ids_json TEXT NOT NULL,
        conditional_order_view_evidence_ids_json TEXT NOT NULL,
        observation_time_ns INTEGER NOT NULL,
        receive_time_ns INTEGER NOT NULL,
        status TEXT NOT NULL,
        raw_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flat_certificates (
        certification_id TEXT PRIMARY KEY,
        reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
        intent_id TEXT REFERENCES intents(intent_id),
        writer_id TEXT NOT NULL,
        writer_epoch INTEGER NOT NULL,
        account_identity_hash TEXT NOT NULL,
        instrument TEXT NOT NULL,
        position_epoch INTEGER NOT NULL,
        decision TEXT NOT NULL,
        certified_at_ns INTEGER NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        payload_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recovery_incidents (
        incident_id TEXT PRIMARY KEY,
        recovery_run_id TEXT NOT NULL,
        category TEXT NOT NULL,
        status TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        opened_at_ns INTEGER NOT NULL,
        resolved_at_ns INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recovery_certificates (
        recovery_run_id TEXT PRIMARY KEY,
        runtime_instance_id TEXT NOT NULL,
        writer_id TEXT NOT NULL,
        writer_epoch INTEGER NOT NULL,
        journal_schema_version INTEGER NOT NULL,
        unresolved_intents_json TEXT NOT NULL,
        unresolved_commands_json TEXT NOT NULL,
        unknown_commands_json TEXT NOT NULL,
        reconciliation_health TEXT NOT NULL,
        started_at_ns INTEGER NOT NULL,
        ended_at_ns INTEGER NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        venue_evidence_refs_json TEXT NOT NULL,
        decision TEXT NOT NULL,
        flat_certificate_id TEXT,
        protection_evidence_id TEXT,
        compatibility_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS capability_evidence_log (
        evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
        capability_name TEXT NOT NULL,
        state TEXT NOT NULL,
        test_run_id TEXT,
        evidence_refs_json TEXT NOT NULL,
        test_timestamp_ns INTEGER,
        environment TEXT NOT NULL,
        notes TEXT NOT NULL,
        target_profile_hash TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS capability_qualification_log (
        qualification_id TEXT PRIMARY KEY,
        capability_name TEXT NOT NULL,
        previous_state TEXT NOT NULL,
        new_state TEXT NOT NULL,
        test_run_id TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        qualified_by TEXT NOT NULL,
        qualified_at_ns INTEGER NOT NULL,
        target_profile_hash TEXT NOT NULL
    )
    """,
]


def recovery_certificate_ddl() -> str:
    """Return the v5 recovery-certificate DDL without relying on list indices."""

    return next(
        statement for statement in DDL_STATEMENTS if "CREATE TABLE IF NOT EXISTS recovery_certificates" in statement
    )


def protection_evidence_ddl() -> str:
    return next(
        statement for statement in DDL_STATEMENTS if "CREATE TABLE IF NOT EXISTS protection_evidence" in statement
    )
