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
    """CREATE INDEX artifact_outcome_decision_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.outcome.decision_ref') END,
        created_at_ns DESC, artifact_ref DESC, available_at_ns
    ) WHERE artifact_type='MaturedOutcomeV2'""",
    """CREATE INDEX artifact_actual_binding_action_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.binding.action_hash') END,
        created_at_ns DESC, artifact_ref DESC, available_at_ns
    ) WHERE artifact_type='ActualActionPositionBindingV2'""",
    """CREATE INDEX artifact_policy_payoff_action_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.payoff.action_hash') END,
        created_at_ns DESC, artifact_ref DESC, available_at_ns
    ) WHERE artifact_type='PolicyPayoffV2'""",
    """CREATE INDEX artifact_diagnostic_decision_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.diagnostic.decision_ref') END,
        created_at_ns DESC, artifact_ref DESC, available_at_ns
    ) WHERE artifact_type='DiagnosticTargetEvidenceV2'""",
    """CREATE INDEX artifact_outcome_status_decision_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.status.decision_ref') END,
        created_at_ns DESC, artifact_ref DESC, available_at_ns
    ) WHERE artifact_type='OutcomeMaturityStatusV1'""",
    """CREATE INDEX public_observation_source_id_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.source_id') END
    ) WHERE artifact_type='PublicObservationIndexV2'""",
    """CREATE INDEX public_reconciliation_source_id_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.reconciliation.source_id') END,
        created_at_ns DESC, artifact_ref DESC
    ) WHERE artifact_type='OpsPublicSourceReconciliationV1'""",
    """CREATE INDEX native_m1_event_origin_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.native_m1_origin.origin_ref') END,
        created_at_ns DESC, artifact_ref DESC
    ) WHERE artifact_type='OpsDecisionEventSourceV1'""",
    """CREATE INDEX native_m1_event_id_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event.event_id') END,
        created_at_ns DESC, artifact_ref DESC
    ) WHERE artifact_type='OpsDecisionEventSourceV1'""",
    """CREATE INDEX ops_decision_event_available_order ON artifact_index (
        available_at_ns, created_at_ns, artifact_ref
    ) WHERE artifact_type='OpsDecisionEventSourceV1'""",
    """CREATE INDEX ops_receipt_event_id_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event_id') END
    ) WHERE artifact_type='OpsSupervisorReceiptIdentityV1'""",
    """CREATE INDEX native_m1_gate_origin_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.native_m1_origin_ref') END,
        created_at_ns DESC, artifact_ref DESC
    ) WHERE artifact_type='OpsPublicAcquisitionDeadlineGateV1'""",
    """CREATE INDEX native_m1_checkpoint_key_generation_lookup ON artifact_index (
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END,
        CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.checkpoint.generation') END DESC,
        artifact_ref DESC
    ) WHERE artifact_type='S3NativeM1OriginAccountingCheckpointV1'""",
)

_ARTIFACT_IDENTITY_INDEX_DDL = _DDL[9:]

# Controller-owned, rebuildable scheduling projections. They carry no outcome
# or authority; immutable artifact_index evidence remains authoritative. Keeping
# this additive extension separate preserves the accepted schema-v1 wire.
_DUE_WORK_DDL = (
    """CREATE TABLE IF NOT EXISTS public_bar_repair_head (
        instrument_key_json TEXT NOT NULL, interval TEXT NOT NULL,
        recovery_started_at_ns INTEGER NOT NULL, verified_close_at_ns INTEGER NOT NULL,
        certificate_ref TEXT, available_at_ns INTEGER NOT NULL,
        PRIMARY KEY(instrument_key_json,interval)
    )""",
    """CREATE TABLE IF NOT EXISTS native_origin_window_head (
        instrument_key_json TEXT NOT NULL, event_type TEXT NOT NULL,
        window_ref TEXT NOT NULL, available_from_ns INTEGER NOT NULL,
        available_through_ns INTEGER NOT NULL, cursor_available_ns INTEGER NOT NULL,
        cursor_ref TEXT NOT NULL, complete INTEGER NOT NULL,
        PRIMARY KEY(instrument_key_json,event_type)
    )""",
    """CREATE TABLE IF NOT EXISTS native_origin_window (
        window_ref TEXT NOT NULL, close_at_ns INTEGER NOT NULL,
        available_at_ns INTEGER NOT NULL, artifact_ref TEXT NOT NULL,
        PRIMARY KEY(window_ref,close_at_ns)
    )""",
    """CREATE TABLE IF NOT EXISTS active_history_head (
        instrument_key_json TEXT NOT NULL, interval TEXT NOT NULL,
        state_json TEXT, state_ref TEXT, scan_close_at_ns INTEGER NOT NULL,
        cutoff_ns INTEGER NOT NULL, available_at_ns INTEGER NOT NULL,
        dirty_available_at_ns INTEGER,
        PRIMARY KEY(instrument_key_json,interval)
    )""",
    """CREATE TABLE IF NOT EXISTS collector_cursor_head (
        source_id TEXT NOT NULL, channel TEXT NOT NULL, artifact_ref TEXT NOT NULL,
        high_water_sequence INTEGER NOT NULL, created_at_ns INTEGER NOT NULL,
        PRIMARY KEY(source_id,channel)
    )""",
    """CREATE TABLE IF NOT EXISTS due_work (
        lane TEXT NOT NULL, work_id TEXT NOT NULL, source_ref TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL, due_at_ns INTEGER NOT NULL,
        payload_json TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'PENDING', reason_code TEXT,
        PRIMARY KEY(lane, work_id)
    )""",
    """CREATE INDEX IF NOT EXISTS due_work_ready ON due_work
        (lane, due_at_ns, work_id) WHERE state='PENDING'""",
    """CREATE INDEX IF NOT EXISTS due_work_oldest ON due_work
        (lane, created_at_ns, work_id) WHERE state='PENDING'""",
    """CREATE TABLE IF NOT EXISTS due_work_pressure (
        lane TEXT PRIMARY KEY, pending_count INTEGER NOT NULL DEFAULT 0,
        retired_count INTEGER NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS due_work_discovery (
        projection_id TEXT PRIMARY KEY, last_rowid INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS due_work_quarantine_pressure (
        lane TEXT PRIMARY KEY, quarantined_count INTEGER NOT NULL DEFAULT 0
    )""",
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
        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in _ARTIFACT_IDENTITY_INDEX_DDL:
                connection.execute(statement.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1))
            for statement in _DUE_WORK_DDL:
                connection.execute(statement)
            if "collector_cursor_head" not in tables:
                connection.execute("INSERT OR IGNORE INTO due_work_discovery(projection_id,last_rowid) "
                                   "VALUES('COLLECTOR_HEADS_V1',0)")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return
    if tables:
        raise RuntimeError("database has tables but is not an initialized atlas-ops store")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in _DDL:
            connection.execute(statement)
        for statement in _DUE_WORK_DDL:
            connection.execute(statement)
        connection.execute("INSERT INTO due_work_discovery(projection_id,last_rowid) "
                           "VALUES('COLLECTOR_HEADS_V1',-1)")
        connection.execute(
            "INSERT INTO schema_meta(namespace, schema_version) VALUES (?, ?)",
            (OPS_SCHEMA_NAMESPACE, OPS_SCHEMA_VERSION),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def validate_read_only(connection: sqlite3.Connection) -> None:
    """Validate an existing ops store without creating or changing schema state."""
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if not _REQUIRED_TABLES.issubset(tables):
        raise RuntimeError("atlas-ops schema is incomplete")
    rows = connection.execute(
        "SELECT schema_version FROM schema_meta WHERE namespace=?", (OPS_SCHEMA_NAMESPACE,)
    ).fetchall()
    if len(rows) != 1 or rows[0][0] != OPS_SCHEMA_VERSION:
        raise RuntimeError("atlas-ops schema version is missing or unsupported")
