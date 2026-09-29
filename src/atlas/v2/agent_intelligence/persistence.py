"""Additive, append-only agent job persistence inside the existing ops.sqlite file."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import uuid
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, sha256_json, sha256_ref
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1, ProviderPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import (
    AgentAttemptV1,
    AgentEvidenceRefV1,
    AgentJobStateV1,
    AgentJobV1,
    AgentModelProfile,
    AgentModelProfileV2,
    AgentValidationReceiptV1,
    BrokerDispatchAuthorizationV1,
    BrokerDispatchAuthorizationV2,
    ResearchProposalRequestV1,
    ResearchProposalV1,
    model_profile_from_dict,
)
from atlas.v2.memory.schema import validate_read_only

AGENT_SCHEMA_NAMESPACE = "atlas-agent-intelligence"
AGENT_SCHEMA_VERSION = 2
AGENT_NAMESPACE_WRITER_OWNER = "atlas-ops/controller"
_NS_PER_DAY = 86_400_000_000_000
_SECRET_PATTERN = re.compile(
    r"(?:sk-[A-Za-z0-9_-]{20,}|sk-ant-[A-Za-z0-9_-]{20,}|sk-or-v1-[A-Za-z0-9_-]{20,}|"
    r"gsk_[A-Za-z0-9_-]{20,}|AIza[A-Za-z0-9_-]{24,}|hf_[A-Za-z0-9]{20,}|"
    r"\bBearer\s+[A-Za-z0-9._~-]{16,}|-----BEGIN [A-Z ]+PRIVATE KEY-----)", re.I)
_SENSITIVE_KEY = re.compile(r"(?:api[_-]?(?:key|secret)|exchange[_-]?credential|access[_-]?token|private[_-]?key)", re.I)

_AGENT_DDL = (
    "CREATE TABLE agent_intelligence_meta(namespace TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version>=1))",
    "CREATE TABLE agent_model_profiles(profile_hash TEXT PRIMARY KEY, profile_json TEXT NOT NULL, created_at_ns INTEGER NOT NULL)",
    """CREATE TABLE agent_family_budgets(
        family_id TEXT PRIMARY KEY, experiment_ref TEXT NOT NULL, initial_attempts INTEGER NOT NULL,
        proposal_jobs_started INTEGER NOT NULL, initial_parameter_units INTEGER NOT NULL,
        parameter_units_used INTEGER NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_requests(
        request_key TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, request_json TEXT NOT NULL,
        request_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_family_usage(
        request_key TEXT PRIMARY KEY REFERENCES agent_requests(request_key), family_id TEXT NOT NULL,
        parameter_units INTEGER NOT NULL, status TEXT NOT NULL, recorded_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_jobs(
        job_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE REFERENCES agent_requests(request_key),
        lifecycle_state TEXT NOT NULL, lease_epoch INTEGER NOT NULL DEFAULT 0,
        lease_owner TEXT, lease_expires_at_ns INTEGER, deadline_ns INTEGER NOT NULL,
        request_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL, state_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_attempts(
        attempt_id TEXT PRIMARY KEY, request_key TEXT NOT NULL REFERENCES agent_requests(request_key),
        attempt_index INTEGER NOT NULL, lease_epoch INTEGER NOT NULL, started_at_ns INTEGER NOT NULL,
        reserved_cost_usd TEXT NOT NULL, attempt_json TEXT NOT NULL, attempt_hash TEXT NOT NULL,
        UNIQUE(request_key,attempt_index))""",
    """CREATE TABLE agent_attempt_outcomes(
        outcome_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES agent_attempts(attempt_id),
        outcome_json TEXT NOT NULL, outcome_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_broker_dispatches(
        attempt_id TEXT PRIMARY KEY REFERENCES agent_attempts(attempt_id), job_id TEXT NOT NULL,
        request_key TEXT NOT NULL, request_hash TEXT NOT NULL, attempt_index INTEGER NOT NULL,
        authorization_id TEXT NOT NULL UNIQUE, capability_nonce TEXT NOT NULL UNIQUE,
        lease_epoch INTEGER NOT NULL, evidence_hash TEXT NOT NULL, model_profile_hash TEXT NOT NULL,
        deadline_ns INTEGER NOT NULL, authorized_at_ns INTEGER NOT NULL, expires_at_ns INTEGER NOT NULL,
        budget_reservation_id TEXT NOT NULL, reserved_cost_usd TEXT NOT NULL,
        max_input_tokens INTEGER NOT NULL, max_output_tokens INTEGER NOT NULL,
        dispatched_at_ns INTEGER NOT NULL, dispatch_hash TEXT NOT NULL)""",
    """CREATE TABLE agent_results(
        result_id TEXT PRIMARY KEY, request_key TEXT NOT NULL REFERENCES agent_requests(request_key),
        attempt_id TEXT NOT NULL REFERENCES agent_attempts(attempt_id), lease_epoch INTEGER NOT NULL,
        result_hash TEXT NOT NULL, result_json TEXT NOT NULL, received_at_ns INTEGER NOT NULL,
        eligible INTEGER NOT NULL CHECK(eligible IN (0,1)),
        UNIQUE(attempt_id,result_hash))""",
    """CREATE TABLE agent_authorities(
        request_key TEXT PRIMARY KEY REFERENCES agent_requests(request_key),
        result_id TEXT NOT NULL UNIQUE REFERENCES agent_results(result_id),
        receipt_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    "CREATE UNIQUE INDEX agent_one_authority_per_request ON agent_authorities(request_key)",
    """CREATE TABLE agent_validation_receipts(
        receipt_hash TEXT PRIMARY KEY, request_key TEXT NOT NULL REFERENCES agent_requests(request_key),
        result_id TEXT NOT NULL REFERENCES agent_results(result_id), receipt_json TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL, authoritative INTEGER NOT NULL CHECK(authoritative IN (0,1)))""",
    """CREATE TABLE agent_budget_reservations(
        reservation_id TEXT PRIMARY KEY, request_key TEXT NOT NULL REFERENCES agent_requests(request_key),
        attempt_index INTEGER NOT NULL, utc_day INTEGER NOT NULL, reserved_usd TEXT NOT NULL,
        daily_ceiling_usd TEXT NOT NULL, price_schedule_hash TEXT NOT NULL,
        UNIQUE(request_key,attempt_index))""",
    """CREATE TABLE agent_daily_budgets(
        utc_day INTEGER PRIMARY KEY, daily_ceiling_usd TEXT NOT NULL, price_schedule_hash TEXT NOT NULL)""",
    """CREATE TABLE agent_tool_calls(
        tool_call_id TEXT PRIMARY KEY, request_key TEXT NOT NULL REFERENCES agent_requests(request_key),
        tool_index INTEGER NOT NULL, tool_name TEXT NOT NULL, artifact_ref TEXT NOT NULL,
        status TEXT NOT NULL, response_hash TEXT NOT NULL, called_at_ns INTEGER NOT NULL,
        UNIQUE(request_key,tool_index))""",
    """CREATE TABLE agent_job_events(
        event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES agent_jobs(job_id),
        from_state TEXT, to_state TEXT NOT NULL, lease_epoch INTEGER NOT NULL,
        event_at_ns INTEGER NOT NULL, event_json TEXT NOT NULL, event_hash TEXT NOT NULL)""",
    "CREATE INDEX agent_jobs_state_order ON agent_jobs(lifecycle_state,created_at_ns,job_id)",
    "CREATE UNIQUE INDEX agent_one_active_job ON agent_jobs((1)) WHERE lifecycle_state IN ('QUEUED','LEASED','RUNNING','RESULT_RECEIVED')",
    "CREATE INDEX agent_attempts_request_order ON agent_attempts(request_key,attempt_index)",
    "CREATE INDEX agent_results_request_order ON agent_results(request_key,received_at_ns,result_id)",
    """CREATE TRIGGER agent_profiles_no_update BEFORE UPDATE ON agent_model_profiles BEGIN SELECT RAISE(ABORT,'immutable model profile'); END""",
    """CREATE TRIGGER agent_profiles_no_delete BEFORE DELETE ON agent_model_profiles BEGIN SELECT RAISE(ABORT,'immutable model profile'); END""",
    """CREATE TRIGGER agent_family_budget_immutable_config BEFORE UPDATE ON agent_family_budgets
        WHEN NEW.experiment_ref!=OLD.experiment_ref OR NEW.initial_attempts!=OLD.initial_attempts
          OR NEW.initial_parameter_units!=OLD.initial_parameter_units OR NEW.created_at_ns!=OLD.created_at_ns
        BEGIN SELECT RAISE(ABORT,'immutable family budget configuration'); END""",
    """CREATE TRIGGER agent_family_budget_no_delete BEFORE DELETE ON agent_family_budgets BEGIN SELECT RAISE(ABORT,'immutable family budget'); END""",
    """CREATE TRIGGER agent_family_usage_no_update BEFORE UPDATE ON agent_family_usage BEGIN SELECT RAISE(ABORT,'immutable family usage'); END""",
    """CREATE TRIGGER agent_family_usage_no_delete BEFORE DELETE ON agent_family_usage BEGIN SELECT RAISE(ABORT,'immutable family usage'); END""",
    """CREATE TRIGGER agent_requests_no_update BEFORE UPDATE ON agent_requests BEGIN SELECT RAISE(ABORT,'immutable agent request'); END""",
    """CREATE TRIGGER agent_requests_no_delete BEFORE DELETE ON agent_requests BEGIN SELECT RAISE(ABORT,'immutable agent request'); END""",
    """CREATE TRIGGER agent_attempts_no_update BEFORE UPDATE ON agent_attempts BEGIN SELECT RAISE(ABORT,'immutable agent attempt'); END""",
    """CREATE TRIGGER agent_attempts_no_delete BEFORE DELETE ON agent_attempts BEGIN SELECT RAISE(ABORT,'immutable agent attempt'); END""",
    """CREATE TRIGGER agent_attempt_outcomes_no_update BEFORE UPDATE ON agent_attempt_outcomes BEGIN SELECT RAISE(ABORT,'immutable agent outcome'); END""",
    """CREATE TRIGGER agent_attempt_outcomes_no_delete BEFORE DELETE ON agent_attempt_outcomes BEGIN SELECT RAISE(ABORT,'immutable agent outcome'); END""",
    """CREATE TRIGGER agent_broker_dispatches_no_update BEFORE UPDATE ON agent_broker_dispatches BEGIN SELECT RAISE(ABORT,'immutable broker dispatch'); END""",
    """CREATE TRIGGER agent_broker_dispatches_no_delete BEFORE DELETE ON agent_broker_dispatches BEGIN SELECT RAISE(ABORT,'immutable broker dispatch'); END""",
    """CREATE TRIGGER agent_results_no_update BEFORE UPDATE ON agent_results BEGIN SELECT RAISE(ABORT,'immutable agent result'); END""",
    """CREATE TRIGGER agent_results_no_delete BEFORE DELETE ON agent_results BEGIN SELECT RAISE(ABORT,'immutable agent result'); END""",
    """CREATE TRIGGER agent_validation_receipts_no_update BEFORE UPDATE ON agent_validation_receipts BEGIN SELECT RAISE(ABORT,'immutable validation receipt'); END""",
    """CREATE TRIGGER agent_validation_receipts_no_delete BEFORE DELETE ON agent_validation_receipts BEGIN SELECT RAISE(ABORT,'immutable validation receipt'); END""",
    """CREATE TRIGGER agent_authorities_no_update BEFORE UPDATE ON agent_authorities BEGIN SELECT RAISE(ABORT,'immutable agent authority'); END""",
    """CREATE TRIGGER agent_authorities_no_delete BEFORE DELETE ON agent_authorities BEGIN SELECT RAISE(ABORT,'immutable agent authority'); END""",
    """CREATE TRIGGER agent_budget_no_update BEFORE UPDATE ON agent_budget_reservations BEGIN SELECT RAISE(ABORT,'immutable spend reservation'); END""",
    """CREATE TRIGGER agent_budget_no_delete BEFORE DELETE ON agent_budget_reservations BEGIN SELECT RAISE(ABORT,'immutable spend reservation'); END""",
    """CREATE TRIGGER agent_daily_budget_no_update BEFORE UPDATE ON agent_daily_budgets BEGIN SELECT RAISE(ABORT,'immutable daily budget'); END""",
    """CREATE TRIGGER agent_daily_budget_no_delete BEFORE DELETE ON agent_daily_budgets BEGIN SELECT RAISE(ABORT,'immutable daily budget'); END""",
    """CREATE TRIGGER agent_tool_calls_no_update BEFORE UPDATE ON agent_tool_calls BEGIN SELECT RAISE(ABORT,'immutable tool call'); END""",
    """CREATE TRIGGER agent_tool_calls_no_delete BEFORE DELETE ON agent_tool_calls BEGIN SELECT RAISE(ABORT,'immutable tool call'); END""",
    """CREATE TRIGGER agent_events_no_update BEFORE UPDATE ON agent_job_events BEGIN SELECT RAISE(ABORT,'immutable job event'); END""",
    """CREATE TRIGGER agent_events_no_delete BEFORE DELETE ON agent_job_events BEGIN SELECT RAISE(ABORT,'immutable job event'); END""",
)

ACTION_ASSESSMENT_SCHEMA_NAMESPACE = "atlas-agent-action-assessment"
ACTION_ASSESSMENT_SCHEMA_VERSION = 2
_ACTION_ASSESSMENT_DDL = (
    "CREATE TABLE agent_action_assessment_meta(namespace TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version>=1))",
    """CREATE TABLE agent_action_assessment_packets(
        packet_ref TEXT PRIMARY KEY, packet_hash TEXT NOT NULL UNIQUE, packet_json TEXT NOT NULL,
        originating_receipt_ref TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_requests(
        request_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL UNIQUE,
        packet_ref TEXT NOT NULL REFERENCES agent_action_assessment_packets(packet_ref),
        request_json TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_attempts(
        attempt_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_requests(request_id), attempt_index INTEGER NOT NULL CHECK(attempt_index=1),
        state TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_reservations(
        reservation_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_requests(request_id), attempt_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_attempts(attempt_id), utc_day INTEGER NOT NULL,
        reserved_usd TEXT NOT NULL, price_schedule_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_dispatches(
        authorization_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_attempts(attempt_id), request_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_requests(request_id), authorization_json TEXT NOT NULL,
        authorization_hash TEXT NOT NULL UNIQUE, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_outcomes(
        outcome_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE
        REFERENCES agent_action_assessment_requests(request_id), attempt_id TEXT UNIQUE
        REFERENCES agent_action_assessment_attempts(attempt_id), status TEXT NOT NULL,
        result_json TEXT, provider_output_hash TEXT, failure_code TEXT, received_at_ns INTEGER NOT NULL,
        eligible INTEGER NOT NULL CHECK(eligible IN (0,1)))""",
    """CREATE TABLE agent_action_assessment_validations(
        receipt_hash TEXT PRIMARY KEY, request_id TEXT NOT NULL
        REFERENCES agent_action_assessment_requests(request_id), attempt_id TEXT NOT NULL
        REFERENCES agent_action_assessment_attempts(attempt_id), receipt_json TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_skips(
        receipt_ref TEXT PRIMARY KEY, reason_code TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_acceptances(
        packet_ref TEXT PRIMARY KEY REFERENCES agent_action_assessment_packets(packet_ref),
        request_id TEXT NOT NULL UNIQUE REFERENCES agent_action_assessment_requests(request_id),
        result_hash TEXT NOT NULL, created_at_ns INTEGER NOT NULL)""",
    """CREATE TABLE agent_action_assessment_observation_projections(
        request_id TEXT PRIMARY KEY REFERENCES agent_action_assessment_requests(request_id),
        observation_ref TEXT NOT NULL UNIQUE, projected_at_ns INTEGER NOT NULL)""",
    *tuple(f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT,'immutable action assessment record'); END"
        for table in ("agent_action_assessment_packets", "agent_action_assessment_requests",
            "agent_action_assessment_attempts", "agent_action_assessment_reservations",
            "agent_action_assessment_dispatches", "agent_action_assessment_outcomes",
            "agent_action_assessment_validations", "agent_action_assessment_skips",
            "agent_action_assessment_acceptances", "agent_action_assessment_observation_projections")),
    *tuple(f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'immutable action assessment record'); END"
        for table in ("agent_action_assessment_packets", "agent_action_assessment_requests",
            "agent_action_assessment_attempts", "agent_action_assessment_reservations",
            "agent_action_assessment_dispatches", "agent_action_assessment_outcomes",
            "agent_action_assessment_validations", "agent_action_assessment_skips",
            "agent_action_assessment_acceptances", "agent_action_assessment_observation_projections")),
)


def initialize_agent_extension(connection: sqlite3.Connection) -> None:
    """Create the namespaced extension without touching existing ops schema/version rows."""
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = {row[0] for row in rows}
    if not {"schema_meta", "artifact_index", "ops_outbox"}.issubset(tables):
        raise RuntimeError("agent persistence requires an initialized atlas-ops database")
    exists = "agent_intelligence_meta" in tables
    if exists:
        row = connection.execute("SELECT schema_version FROM agent_intelligence_meta WHERE namespace=?",
                                 (AGENT_SCHEMA_NAMESPACE,)).fetchone()
        if row is None:
            raise RuntimeError("agent persistence schema version is unsupported")
        if row[0] == 1:
            _migrate_agent_extension_v1_to_v2(connection)
        elif row[0] != AGENT_SCHEMA_VERSION:
            raise RuntimeError("agent persistence schema version is unsupported")
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {row[0] for row in rows}
        required = {"agent_model_profiles", "agent_family_budgets", "agent_family_usage", "agent_requests", "agent_jobs", "agent_attempts", "agent_results", "agent_authorities",
                    "agent_broker_dispatches", "agent_validation_receipts", "agent_budget_reservations", "agent_daily_budgets",
        "agent_tool_calls", "agent_job_events"}
        if not required.issubset(tables):
            raise RuntimeError("agent persistence schema is incomplete")
        return
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in _AGENT_DDL:
            connection.execute(statement)
        connection.execute("INSERT INTO agent_intelligence_meta VALUES(?,?)",
                           (AGENT_SCHEMA_NAMESPACE, AGENT_SCHEMA_VERSION))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _migrate_agent_extension_v1_to_v2(connection: sqlite3.Connection) -> None:
    """Add exact durable dispatch-authorization bindings without changing ops schema metadata."""
    columns = {row[1] for row in connection.execute("PRAGMA table_info(agent_broker_dispatches)")}
    additions = (
        ("job_id", "TEXT"), ("request_hash", "TEXT"), ("attempt_index", "INTEGER"),
        ("authorization_id", "TEXT"), ("evidence_hash", "TEXT"), ("model_profile_hash", "TEXT"),
        ("deadline_ns", "INTEGER"), ("authorized_at_ns", "INTEGER"), ("expires_at_ns", "INTEGER"),
        ("budget_reservation_id", "TEXT"), ("reserved_cost_usd", "TEXT"),
        ("max_input_tokens", "INTEGER"), ("max_output_tokens", "INTEGER"),
    )
    connection.execute("BEGIN IMMEDIATE")
    try:
        for column, sql_type in additions:
            if column not in columns:
                connection.execute(f"ALTER TABLE agent_broker_dispatches ADD COLUMN {column} {sql_type}")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS agent_dispatch_authorization_id_unique "
                           "ON agent_broker_dispatches(authorization_id) WHERE authorization_id IS NOT NULL")
        connection.execute("UPDATE agent_intelligence_meta SET schema_version=? WHERE namespace=?",
                           (AGENT_SCHEMA_VERSION, AGENT_SCHEMA_NAMESPACE))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def initialize_action_assessment_extension(connection: sqlite3.Connection) -> None:
    """Create the versioned critic ledger beside, without changing, the S26/28 agent schema."""
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = {row[0] for row in rows}
    if "agent_intelligence_meta" not in tables:
        raise RuntimeError("action-assessment ledger requires the accepted agent extension")
    if "agent_action_assessment_meta" in tables:
        row = connection.execute("SELECT schema_version FROM agent_action_assessment_meta WHERE namespace=?",
                                 (ACTION_ASSESSMENT_SCHEMA_NAMESPACE,)).fetchone()
        required = {"agent_action_assessment_packets", "agent_action_assessment_requests",
            "agent_action_assessment_attempts", "agent_action_assessment_reservations",
            "agent_action_assessment_dispatches", "agent_action_assessment_outcomes",
            "agent_action_assessment_validations", "agent_action_assessment_skips",
            "agent_action_assessment_acceptances"}
        if row is None:
            raise RuntimeError("action-assessment ledger schema is incomplete or unsupported")
        if row[0] == 1:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("""CREATE TABLE agent_action_assessment_observation_projections(
                    request_id TEXT PRIMARY KEY REFERENCES agent_action_assessment_requests(request_id),
                    observation_ref TEXT NOT NULL UNIQUE, projected_at_ns INTEGER NOT NULL)""")
                connection.execute("""CREATE TRIGGER agent_action_assessment_observation_projections_no_update
                    BEFORE UPDATE ON agent_action_assessment_observation_projections
                    BEGIN SELECT RAISE(ABORT,'immutable action assessment record'); END""")
                connection.execute("""CREATE TRIGGER agent_action_assessment_observation_projections_no_delete
                    BEFORE DELETE ON agent_action_assessment_observation_projections
                    BEGIN SELECT RAISE(ABORT,'immutable action assessment record'); END""")
                # The original v1 metadata table constrained schema_version=1. Recreate only this
                # metadata row; all critic evidence tables remain untouched.
                connection.execute("DROP TABLE agent_action_assessment_meta")
                connection.execute("CREATE TABLE agent_action_assessment_meta(namespace TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version>=1))")
                connection.execute("INSERT INTO agent_action_assessment_meta VALUES(?,?)",
                    (ACTION_ASSESSMENT_SCHEMA_NAMESPACE, ACTION_ASSESSMENT_SCHEMA_VERSION))
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            tables.add("agent_action_assessment_observation_projections")
        required.add("agent_action_assessment_observation_projections")
        row = connection.execute("SELECT schema_version FROM agent_action_assessment_meta WHERE namespace=?",
                                 (ACTION_ASSESSMENT_SCHEMA_NAMESPACE,)).fetchone()
        if row is None or row[0] != ACTION_ASSESSMENT_SCHEMA_VERSION or not required.issubset(tables):
            raise RuntimeError("action-assessment ledger schema is incomplete or unsupported")
        return
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in _ACTION_ASSESSMENT_DDL:
            connection.execute(statement)
        connection.execute("INSERT INTO agent_action_assessment_meta VALUES(?,?)",
                           (ACTION_ASSESSMENT_SCHEMA_NAMESPACE, ACTION_ASSESSMENT_SCHEMA_VERSION))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _safe_json(value: Any) -> str:
    encoded = canonical_json(value)
    def sensitive(item: Any) -> bool:
        if isinstance(item, dict):
            return any(_SENSITIVE_KEY.search(str(key)) or sensitive(child) for key, child in item.items())
        if isinstance(item, list):
            return any(sensitive(child) for child in item)
        return False
    if _SECRET_PATTERN.search(encoded) or sensitive(value):
        # Never persist credentials or provider secrets. Preserve an immutable redacted failure record instead.
        safe: dict[str, str] = {"status": "REDACTED_SENSITIVE_OUTPUT", "content_hash": sha256_json(encoded)}
        if isinstance(value, dict) and isinstance(value.get("proposal_hash"), str):
            safe["proposal_hash"] = value["proposal_hash"]
        return canonical_json(safe)
    return encoded


def _proposal_parameter_units(rule: dict[str, Any]) -> int:
    count = int(rule.get("threshold") is not None)
    children = rule.get("children", [])
    if isinstance(children, list):
        count += sum(_proposal_parameter_units(child) for child in children if isinstance(child, dict))
    return count


class AgentJobRepository:
    """Separate job repository over ops.sqlite; a worker never receives this connection."""

    def __init__(self, path: str | Path,
                 price_schedule: ProviderPriceScheduleV1 | DeepSeekPriceScheduleV1) -> None:
        raw = str(path)
        if not raw or raw.startswith("file:") or "://" in raw:
            raise ValueError("agent persistence requires a local ops.sqlite path")
        self.path = raw
        self.price_schedule = price_schedule
        self._lock = threading.RLock()
        self._writer_thread_id = threading.get_ident()
        self._connection = sqlite3.connect(raw, isolation_level=None, check_same_thread=False, timeout=30)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=30000")
        if self._connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
            self._connection.close()
            raise RuntimeError("agent persistence requires existing ops WAL mode")
        if self._connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            self._connection.close()
            raise RuntimeError("agent persistence requires SQLite foreign keys")
        validate_read_only(self._connection)
        initialize_agent_extension(self._connection)

    def close(self) -> None:
        self.assert_writer_thread()
        with self._lock:
            self._connection.close()

    def assert_writer_thread(self) -> None:
        if threading.get_ident() != self._writer_thread_id:
            raise RuntimeError("agent/critic persistence is owned by the atlas-ops controller thread")

    def __enter__(self) -> AgentJobRepository:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _transaction(self):
        repository = self

        class Transaction:
            def __enter__(self) -> sqlite3.Connection:
                repository.assert_writer_thread()
                repository._lock.acquire()
                repository._connection.execute("BEGIN IMMEDIATE")
                return repository._connection

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
                try:
                    repository._connection.rollback() if exc_type else repository._connection.commit()
                finally:
                    repository._lock.release()
        return Transaction()

    @staticmethod
    def _job(row: sqlite3.Row) -> AgentJobV1:
        return AgentJobV1(row["job_id"], row["request_key"], AgentJobStateV1(row["lifecycle_state"]),
            row["lease_epoch"], row["lease_owner"], row["lease_expires_at_ns"], row["deadline_ns"],
            row["request_hash"], row["created_at_ns"])

    def _event(self, connection: sqlite3.Connection, job_id: str, before: str | None, after: str,
               epoch: int, at_ns: int, event: dict[str, Any]) -> None:
        event_id = str(uuid.uuid4())
        body = {"version": "AgentJobEventV1", "job_id": job_id, "from_state": before,
                "to_state": after, "lease_epoch": epoch, "event_at_ns": at_ns, "detail": event}
        encoded = _safe_json(body)
        connection.execute("INSERT INTO agent_job_events VALUES(?,?,?,?,?,?,?,?)",
            (event_id, job_id, before, after, epoch, at_ns, encoded, sha256_json(encoded)))

    def create_request(self, request: ResearchProposalRequestV1, *, job_id: str, now_ns: int) -> AgentJobV1:
        request_json = canonical_json(request.to_dict())
        request_hash = sha256_json(request.to_dict())
        request_key = request.request_key
        if type(now_ns) is not int or now_ns < 0 or now_ns >= request.absolute_deadline_ns:
            raise ValueError("request deadline has expired")
        if request.remaining_attempt_budget < 1 or request.remaining_parameter_search_budget < 1:
            raise ValueError("research attempt or parameter-search budget is exhausted")
        if len(request.evidence_manifest) > request.max_read_tool_calls:
            raise ValueError("request evidence exceeds its read-tool budget")
        job_cost = Decimal(request.max_job_cost_usd)
        daily_cost = Decimal(request.daily_cost_budget_usd)
        self.price_schedule.validate_request_budgets(calls=request.max_model_calls,
            input_tokens=request.max_input_tokens, output_tokens=request.max_output_tokens,
            job_usd=job_cost, daily_usd=daily_cost)
        with self._transaction() as connection:
            profile_row = connection.execute("SELECT profile_json FROM agent_model_profiles WHERE profile_hash=?",
                                             (request.model_profile_hash,)).fetchone()
            if profile_row is None:
                raise ValueError("immutable AgentModelProfileV1 must be registered before the request")
            profile = model_profile_from_dict(json.loads(profile_row["profile_json"]))
            if (profile.content_hash != request.model_profile_hash or profile.pricing_schedule_id != self.price_schedule.version
                    or profile.provider != self.price_schedule.provider
                    or profile.requested_model_id != self.price_schedule.requested_model_id
                    or profile.prompt_contract_hash != request.prompt_contract_hash
                    or profile.schema_hash != request.schema_hash or profile.tool_contract_hash != request.tool_contract_hash
                    or request.max_input_tokens > profile.max_input_tokens
                    or request.max_output_tokens > profile.max_output_tokens):
                raise ValueError("request/profile/provider-price binding mismatch")
            if isinstance(profile, AgentModelProfileV2):
                if not isinstance(self.price_schedule, DeepSeekPriceScheduleV1) \
                        or profile.base_url != self.price_schedule.base_url \
                        or profile.endpoint_path != self.price_schedule.endpoint_path:
                    raise ValueError("versioned provider binding and price schedule differ")
            elif not isinstance(self.price_schedule, ProviderPriceScheduleV1):
                raise ValueError("OpenAI V1 profile requires the unchanged V1 price schedule")
            prior = connection.execute("SELECT * FROM agent_jobs WHERE request_key=?", (request_key,)).fetchone()
            if prior is not None:
                return self._job(prior)
            family = connection.execute("SELECT * FROM agent_family_budgets WHERE family_id=?",
                                        (request.research_family_id,)).fetchone()
            if family is None:
                connection.execute("INSERT INTO agent_family_budgets VALUES(?,?,?,?,?,?,?)",
                    (request.research_family_id, request.experiment_ref, request.remaining_attempt_budget, 0,
                     request.remaining_parameter_search_budget, 0, now_ns))
                family = connection.execute("SELECT * FROM agent_family_budgets WHERE family_id=?",
                                            (request.research_family_id,)).fetchone()
            expected_attempts = family["initial_attempts"] - family["proposal_jobs_started"]
            expected_parameters = family["initial_parameter_units"] - family["parameter_units_used"]
            if (family["experiment_ref"] != request.experiment_ref or expected_attempts <= 0
                    or expected_parameters <= 0 or request.remaining_attempt_budget != expected_attempts
                    or request.remaining_parameter_search_budget != expected_parameters):
                raise ValueError("research-family attempt or parameter-search budget is exhausted or misbound")
            collision = connection.execute("SELECT request_key FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
            if collision is not None:
                raise ValueError("job_id already belongs to a different immutable request")
            connection.execute("INSERT INTO agent_requests VALUES(?,?,?,?,?)",
                (request_key, request.request_id, request_json, request_hash, now_ns))
            try:
                connection.execute("INSERT INTO agent_jobs(job_id,request_key,lifecycle_state,deadline_ns,request_hash,created_at_ns,state_at_ns) "
                                   "VALUES(?,?,?,?,?,?,?)", (job_id, request_key, AgentJobStateV1.QUEUED.value,
                                                           request.absolute_deadline_ns, request_hash, now_ns, now_ns))
            except sqlite3.IntegrityError as exc:
                raise ValueError("another research job is already active") from exc
            self._event(connection, job_id, None, AgentJobStateV1.QUEUED.value, 0, now_ns,
                        {"request_hash": request_hash})
            connection.execute("UPDATE agent_family_budgets SET proposal_jobs_started=proposal_jobs_started+1 "
                "WHERE family_id=?", (request.research_family_id,))
            row = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._job(row)

    def register_model_profile(self, profile: AgentModelProfile, *, created_at_ns: int) -> str:
        if profile.pricing_schedule_id != self.price_schedule.version:
            raise ValueError("model profile references an unknown price schedule")
        encoded = canonical_json(profile.to_dict())
        with self._transaction() as connection:
            prior = connection.execute("SELECT profile_json FROM agent_model_profiles WHERE profile_hash=?",
                                       (profile.content_hash,)).fetchone()
            if prior is not None and prior["profile_json"] != encoded:
                raise ValueError("model profile hash collides with different immutable content")
            if prior is None:
                connection.execute("INSERT INTO agent_model_profiles VALUES(?,?,?)",
                                   (profile.content_hash, encoded, created_at_ns))
        return profile.content_hash

    def get_request(self, request_key: str) -> ResearchProposalRequestV1:
        with self._lock:
            row = self._connection.execute("SELECT request_json FROM agent_requests WHERE request_key=?",
                                           (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        return ResearchProposalRequestV1.from_dict(json.loads(row["request_json"]))

    def get_job(self, job_id: str) -> AgentJobV1:
        with self._lock:
            row = self._connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return self._job(row)

    def lease(self, job_id: str, *, owner: str, now_ns: int, lease_ns: int) -> AgentJobV1:
        if not owner or len(owner) > 128 or type(now_ns) is not int or type(lease_ns) is not int or lease_ns <= 0:
            raise ValueError("lease owner or duration is invalid")
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            current = AgentJobStateV1(row["lifecycle_state"])
            if now_ns >= row["deadline_ns"]:
                self._set_terminal(connection, row, AgentJobStateV1.EXPIRED, now_ns, "deadline_before_lease")
            elif current == AgentJobStateV1.QUEUED or (
                current in {AgentJobStateV1.LEASED, AgentJobStateV1.RUNNING}
                and (row["lease_expires_at_ns"] is None or row["lease_expires_at_ns"] <= now_ns)
            ):
                epoch = row["lease_epoch"] + 1
                expiry = min(now_ns + lease_ns, row["deadline_ns"])
                connection.execute("UPDATE agent_jobs SET lifecycle_state=?,lease_epoch=?,lease_owner=?,"
                    "lease_expires_at_ns=?,state_at_ns=? WHERE job_id=?",
                    (AgentJobStateV1.LEASED.value, epoch, owner, expiry, now_ns, job_id))
                self._event(connection, job_id, current.value, AgentJobStateV1.LEASED.value, epoch, now_ns,
                            {"lease_owner": owner, "lease_expires_at_ns": expiry})
            else:
                raise ValueError("job is already leased or terminal")
            updated = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._job(updated)

    def start(self, job_id: str, *, owner: str, epoch: int, now_ns: int) -> AgentJobV1:
        with self._transaction() as connection:
            self._fenced_job(connection, job_id, owner, epoch, now_ns, require_state=AgentJobStateV1.LEASED)
            connection.execute("UPDATE agent_jobs SET lifecycle_state=?,state_at_ns=? WHERE job_id=?",
                (AgentJobStateV1.RUNNING.value, now_ns, job_id))
            self._event(connection, job_id, AgentJobStateV1.LEASED.value, AgentJobStateV1.RUNNING.value,
                        epoch, now_ns, {"worker_started": True})
            updated = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._job(updated)

    def _fenced_job(self, connection: sqlite3.Connection, job_id: str, owner: str, epoch: int, now_ns: int,
                    *, require_state: AgentJobStateV1 | None = None) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        if row["lease_owner"] != owner or row["lease_epoch"] != epoch or row["lease_expires_at_ns"] is None:
            raise ValueError("stale worker lease fence")
        if now_ns >= row["lease_expires_at_ns"] or now_ns >= row["deadline_ns"]:
            raise ValueError("worker lease or job deadline expired")
        if require_state is not None and row["lifecycle_state"] != require_state.value:
            raise ValueError("job lifecycle state does not permit this operation")
        return row

    def reserve_attempt(self, job_id: str, *, owner: str, epoch: int, now_ns: int) -> AgentAttemptV1:
        with self._transaction() as connection:
            row = self._fenced_job(connection, job_id, owner, epoch, now_ns, require_state=AgentJobStateV1.RUNNING)
            req_row = connection.execute("SELECT request_json FROM agent_requests WHERE request_key=?",
                                          (row["request_key"],)).fetchone()
            request = ResearchProposalRequestV1.from_dict(json.loads(req_row["request_json"]))
            attempts = connection.execute("SELECT COUNT(*) FROM agent_attempts WHERE request_key=?",
                                          (row["request_key"],)).fetchone()[0]
            if attempts >= request.max_model_calls or attempts >= self.price_schedule.maximum_model_calls_per_job:
                raise ValueError("model-call retry budget exhausted")
            reserve = self.price_schedule.worst_case_call_usd(input_tokens=request.max_input_tokens,
                                                               output_tokens=request.max_output_tokens)
            job_reserved = sum((Decimal(r[0]) for r in connection.execute(
                "SELECT reserved_usd FROM agent_budget_reservations WHERE request_key=?", (row["request_key"],))), Decimal(0))
            if job_reserved + reserve > Decimal(request.max_job_cost_usd):
                raise ValueError("per-job cost budget exhausted")
            day = now_ns // _NS_PER_DAY
            daily_row = connection.execute("SELECT * FROM agent_daily_budgets WHERE utc_day=?", (day,)).fetchone()
            requested_daily = Decimal(request.daily_cost_budget_usd)
            if daily_row is None:
                connection.execute("INSERT INTO agent_daily_budgets VALUES(?,?,?)",
                    (day, str(requested_daily), self.price_schedule.content_hash))
                ceiling = requested_daily
            else:
                if daily_row["price_schedule_hash"] != self.price_schedule.content_hash:
                    raise ValueError("daily price schedule changed; a versioned budget rollover is required")
                ceiling = min(requested_daily, Decimal(daily_row["daily_ceiling_usd"]))
                if ceiling != Decimal(daily_row["daily_ceiling_usd"]):
                    raise ValueError("daily budget cannot be changed after the first UTC-day reservation")
            daily_reserved = sum((Decimal(r[0]) for r in connection.execute(
                "SELECT reserved_usd FROM agent_budget_reservations WHERE utc_day=?", (day,))), Decimal(0))
            if daily_reserved + reserve > ceiling:
                raise ValueError("daily inference cost budget exhausted")
            attempt_id = str(uuid.uuid4())
            budget_reservation_id = str(uuid.uuid4())
            record = AgentAttemptV1(attempt_id, row["request_key"], attempts + 1, epoch, "DISPATCH_RESERVED",
                now_ns, str(reserve), request.model_profile_hash,
                sha256_json({"request_key": row["request_key"], "attempt_index": attempts + 1,
                             "model_profile_hash": request.model_profile_hash,
                             "max_input_tokens": request.max_input_tokens,
                             "max_output_tokens": request.max_output_tokens}))
            attempt_json = canonical_json(record.to_dict())
            attempt_hash = sha256_json(attempt_json)
            connection.execute("INSERT INTO agent_attempts VALUES(?,?,?,?,?,?,?,?)",
                (attempt_id, row["request_key"], attempts + 1, epoch, now_ns, str(reserve), attempt_json, attempt_hash))
            connection.execute("INSERT INTO agent_budget_reservations VALUES(?,?,?,?,?,?,?)",
                (budget_reservation_id, row["request_key"], attempts + 1, day, str(reserve), str(ceiling),
                 self.price_schedule.content_hash))
        return record

    def record_tool_call(self, request_key: str, authorization: AgentEvidenceRefV1, *, status: str,
                         response: dict[str, Any], called_at_ns: int) -> None:
        request = self.get_request(request_key)
        if authorization not in request.evidence_manifest:
            raise ValueError("tool call is outside the immutable job manifest")
        with self._transaction() as connection:
            count = connection.execute("SELECT COUNT(*) FROM agent_tool_calls WHERE request_key=?",
                                       (request_key,)).fetchone()[0]
            if count >= request.max_read_tool_calls or count >= self.price_schedule.maximum_read_tool_calls_per_job:
                raise ValueError("read-tool budget exhausted")
            connection.execute("INSERT INTO agent_tool_calls VALUES(?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), request_key, count + 1, authorization.tool_name, authorization.artifact_ref,
                 status, sha256_json(_safe_json(response)), called_at_ns))

    def append_attempt_outcome(self, attempt_id: str, outcome: dict[str, Any], *, at_ns: int) -> str:
        encoded = _safe_json(outcome)
        digest = sha256_json(encoded)
        outcome_id = str(uuid.uuid4())
        with self._transaction() as connection:
            connection.execute("INSERT INTO agent_attempt_outcomes VALUES(?,?,?,?,?)",
                               (outcome_id, attempt_id, encoded, digest, at_ns))
        return outcome_id

    def authorize_broker_dispatch(self, job_id: str, request: ResearchProposalRequestV1,
                                  attempt: AgentAttemptV1, *, owner: str, lease_epoch: int,
                                  authorization_id: str, capability_nonce: str, evidence_hash: str,
                                  model_profile_hash: str, expires_at_ns: int,
                                  authorized_at_ns: int) -> BrokerDispatchAuthorizationV1 | BrokerDispatchAuthorizationV2:
        """Durably authorize the exact attempt before the controller issues its signed capability."""
        sha256_ref(evidence_hash, field="evidence_hash")
        with self._transaction() as connection:
            row = connection.execute("SELECT j.*,a.request_key AS attempt_request_key, "
                "a.lease_epoch AS attempt_epoch,a.attempt_index,a.attempt_json,a.attempt_hash, "
                "a.reserved_cost_usd,a.attempt_id AS persisted_attempt_id "
                "FROM agent_jobs j JOIN agent_attempts a ON a.request_key=j.request_key "
                "WHERE j.job_id=? AND a.attempt_id=?", (job_id, attempt.attempt_id)).fetchone()
            if row is None:
                raise ValueError("dispatch attempt is not persisted for this job")
            if (row["request_key"] != request.request_key
                    or row["attempt_request_key"] != request.request_key
                    or row["persisted_attempt_id"] != attempt.attempt_id
                    or row["attempt_index"] != attempt.attempt_index
                    or row["attempt_epoch"] != lease_epoch or attempt.lease_epoch != lease_epoch
                    or attempt.request_key != request.request_key
                    or attempt.state != "DISPATCH_RESERVED"
                    or sha256_json(row["attempt_json"]) != row["attempt_hash"]):
                raise ValueError("dispatch attempt/request binding is invalid")
            if (row["lifecycle_state"] != AgentJobStateV1.RUNNING.value
                    or row["lease_owner"] != owner or row["lease_epoch"] != lease_epoch
                    or row["lease_expires_at_ns"] is None
                    or authorized_at_ns >= row["lease_expires_at_ns"]
                    or authorized_at_ns >= row["deadline_ns"]):
                raise ValueError("dispatch authorization has a stale job lease or deadline")
            if (request.model_profile_hash != model_profile_hash
                    or row["request_hash"] != sha256_json(request.to_dict())
                    or request.absolute_deadline_ns != row["deadline_ns"]
                    or attempt.model_profile_hash != model_profile_hash
                    or attempt.reserved_cost_usd != row["reserved_cost_usd"]
                    or attempt.attempt_index > request.max_model_calls):
                raise ValueError("dispatch authorization request/profile/budget binding is invalid")
            if (not authorized_at_ns < expires_at_ns <= min(row["lease_expires_at_ns"], row["deadline_ns"],
                    authorized_at_ns + 120_000_000_000)):
                raise ValueError("dispatch capability expiry exceeds its durable lease/deadline scope")
            reserved = connection.execute("SELECT * FROM agent_budget_reservations "
                "WHERE request_key=? AND attempt_index=?", (request.request_key, attempt.attempt_index)).fetchone()
            if (reserved is None or reserved["reserved_usd"] != row["reserved_cost_usd"]
                    or reserved["price_schedule_hash"] != self.price_schedule.content_hash):
                raise ValueError("dispatch authorization has no matching immutable budget reservation")
            existing = connection.execute("SELECT 1 FROM agent_broker_dispatches WHERE attempt_id=?",
                                          (attempt.attempt_id,)).fetchone()
            if existing is not None:
                raise ValueError("persisted attempt already has a dispatch authorization")
            request_row = connection.execute("SELECT request_json,request_hash FROM agent_requests WHERE request_key=?",
                                             (request.request_key,)).fetchone()
            if (request_row is None or request_row["request_hash"] != row["request_hash"]
                    or canonical_json(request.to_dict()) != request_row["request_json"]):
                raise ValueError("immutable request persistence does not match dispatch authorization")
            profile_row = connection.execute("SELECT profile_json FROM agent_model_profiles WHERE profile_hash=?",
                                             (model_profile_hash,)).fetchone()
            if profile_row is None:
                raise ValueError("dispatch model profile is not registered")
            profile = model_profile_from_dict(json.loads(profile_row["profile_json"]))
            authorization: BrokerDispatchAuthorizationV1 | BrokerDispatchAuthorizationV2
            if isinstance(profile, AgentModelProfileV2):
                if not isinstance(self.price_schedule, DeepSeekPriceScheduleV1):
                    raise ValueError("DeepSeek profile has no matching versioned price schedule")
                authorization = BrokerDispatchAuthorizationV2.create(
                    job_id=job_id, request_key=request.request_key, request_hash=row["request_hash"],
                    attempt_id=attempt.attempt_id, call_index=attempt.attempt_index, lease_epoch=lease_epoch,
                    authorization_id=authorization_id, capability_nonce=capability_nonce,
                    evidence_hash=evidence_hash, model_profile_hash=model_profile_hash,
                    provider_binding_hash=profile.provider_binding_hash,
                    price_schedule_hash=self.price_schedule.content_hash,
                    provider=profile.provider, requested_model_id=profile.requested_model_id,
                    endpoint=self.price_schedule.endpoint, deadline_ns=row["deadline_ns"],
                    authorized_at_ns=authorized_at_ns, expires_at_ns=expires_at_ns,
                    budget_reservation_id=reserved["reservation_id"],
                    reserved_cost_usd=reserved["reserved_usd"], max_input_tokens=request.max_input_tokens,
                    max_output_tokens=request.max_output_tokens)
            else:
                if not isinstance(self.price_schedule, ProviderPriceScheduleV1):
                    raise ValueError("OpenAI V1 profile has no matching unchanged price schedule")
                authorization = BrokerDispatchAuthorizationV1.create(
                    job_id=job_id, request_key=request.request_key, request_hash=row["request_hash"],
                    attempt_id=attempt.attempt_id, call_index=attempt.attempt_index, lease_epoch=lease_epoch,
                    authorization_id=authorization_id, capability_nonce=capability_nonce,
                    evidence_hash=evidence_hash, model_profile_hash=model_profile_hash,
                    provider=profile.provider, requested_model_id=profile.requested_model_id,
                    deadline_ns=row["deadline_ns"], authorized_at_ns=authorized_at_ns,
                    expires_at_ns=expires_at_ns, budget_reservation_id=reserved["reservation_id"],
                    reserved_cost_usd=reserved["reserved_usd"], max_input_tokens=request.max_input_tokens,
                    max_output_tokens=request.max_output_tokens)
            connection.execute("INSERT INTO agent_broker_dispatches("
                "attempt_id,job_id,request_key,request_hash,attempt_index,authorization_id,capability_nonce,"
                "lease_epoch,evidence_hash,model_profile_hash,deadline_ns,authorized_at_ns,expires_at_ns,"
                "budget_reservation_id,reserved_cost_usd,max_input_tokens,max_output_tokens,"
                "dispatched_at_ns,dispatch_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    attempt.attempt_id, job_id, request.request_key, row["request_hash"], attempt.attempt_index,
                    authorization.authorization_id, authorization.capability_nonce, lease_epoch, evidence_hash,
                    model_profile_hash, row["deadline_ns"], authorized_at_ns, expires_at_ns,
                    reserved["reservation_id"], reserved["reserved_usd"], request.max_input_tokens,
                    request.max_output_tokens, authorized_at_ns, authorization.authorization_hash))
        return authorization

    def submit_result(self, job_id: str, attempt_id: str, *, owner: str, epoch: int,
                      result: dict[str, Any], received_at_ns: int) -> tuple[str, bool, str]:
        encoded = _safe_json(result)
        digest = sha256_json(encoded)
        with self._transaction() as connection:
            job = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            attempt = connection.execute("SELECT * FROM agent_attempts WHERE attempt_id=? AND request_key=?",
                (attempt_id, job["request_key"])).fetchone()
            if attempt is None:
                raise ValueError("result references no persisted dispatch attempt")
            eligible = int(job["lease_owner"] == owner and job["lease_epoch"] == epoch
                and attempt["lease_epoch"] == epoch and job["lifecycle_state"] == AgentJobStateV1.RUNNING.value
                and job["lease_expires_at_ns"] is not None and received_at_ns < job["lease_expires_at_ns"]
                and received_at_ns < job["deadline_ns"])
            prior = connection.execute("SELECT * FROM agent_results WHERE attempt_id=? AND result_hash=?",
                                       (attempt_id, digest)).fetchone()
            if prior is not None:
                return prior["result_id"], bool(prior["eligible"]), digest
            result_id = str(uuid.uuid4())
            connection.execute("INSERT INTO agent_results VALUES(?,?,?,?,?,?,?,?)",
                (result_id, job["request_key"], attempt_id, epoch, digest, encoded, received_at_ns, eligible))
            before = job["lifecycle_state"]
            if eligible:
                connection.execute("UPDATE agent_jobs SET lifecycle_state=?,state_at_ns=? WHERE job_id=?",
                    (AgentJobStateV1.RESULT_RECEIVED.value, received_at_ns, job_id))
                self._event(connection, job_id, before, AgentJobStateV1.RESULT_RECEIVED.value, epoch,
                    received_at_ns, {"result_id": result_id, "result_hash": digest})
            else:
                self._event(connection, job_id, before, before, epoch, received_at_ns,
                    {"late_result_retained": result_id, "result_hash": digest, "eligible": False})
        return result_id, bool(eligible), digest

    def finalize_validation(self, job_id: str, result_id: str, receipt: AgentValidationReceiptV1,
                            *, now_ns: int) -> bool:
        with self._transaction() as connection:
            job = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            result = connection.execute("SELECT * FROM agent_results WHERE result_id=? AND request_key=?",
                (result_id, job["request_key"])).fetchone()
            if result is None:
                raise ValueError("validation result is outside the immutable request")
            req = connection.execute("SELECT request_json FROM agent_requests WHERE request_key=?",
                                     (job["request_key"],)).fetchone()
            if receipt.request_key != job["request_key"] or receipt.provider_result_hash != result["result_hash"]:
                raise ValueError("validation receipt does not bind the persisted result/request")
            authoritative = False
            if receipt.validation_status == "VALID" and result["eligible"] and now_ns < job["deadline_ns"]:
                existing = connection.execute("SELECT * FROM agent_authorities WHERE request_key=?",
                                              (job["request_key"],)).fetchone()
                if existing is None:
                    connection.execute("INSERT INTO agent_authorities VALUES(?,?,?,?)",
                        (job["request_key"], result_id, receipt.receipt_hash, now_ns))
                    authoritative = True
                elif existing["result_id"] == result_id:
                    authoritative = True
            connection.execute("INSERT OR IGNORE INTO agent_validation_receipts VALUES(?,?,?,?,?,?)",
                (receipt.receipt_hash, job["request_key"], result_id, _safe_json(receipt.to_dict()), now_ns,
                 int(authoritative)))
            usage_row = connection.execute("SELECT 1 FROM agent_family_usage WHERE request_key=?",
                                           (job["request_key"],)).fetchone()
            if usage_row is None:
                try:
                    result_payload = json.loads(result["result_json"])
                    proposal_raw = result_payload.get("raw_output", "") if isinstance(result_payload, dict) else ""
                    proposal_wire = json.loads(proposal_raw) if isinstance(proposal_raw, str) else {}
                    proposal = ResearchProposalV1.from_dict(proposal_wire)
                    units = max(1, _proposal_parameter_units(proposal.proposed_rule.to_dict()))
                except (ValueError, TypeError, KeyError, json.JSONDecodeError, RecursionError):
                    units = 1
                family_id = json.loads(req["request_json"])["research_family_id"]
                budget = connection.execute("SELECT * FROM agent_family_budgets WHERE family_id=?",
                    (family_id,)).fetchone()
                remaining = budget["initial_parameter_units"] - budget["parameter_units_used"]
                charged = min(units, remaining)
                connection.execute("INSERT INTO agent_family_usage VALUES(?,?,?,?,?)",
                    (job["request_key"], budget["family_id"], charged, receipt.validation_status, now_ns))
                connection.execute("UPDATE agent_family_budgets SET parameter_units_used=parameter_units_used+? "
                    "WHERE family_id=?", (charged, budget["family_id"]))
            current = AgentJobStateV1(job["lifecycle_state"])
            if authoritative:
                after = AgentJobStateV1.VALIDATED
            elif receipt.validation_status != "VALID" and current not in _TERMINAL:
                after = AgentJobStateV1.INVALID
            elif now_ns >= job["deadline_ns"] and current not in _TERMINAL:
                after = AgentJobStateV1.EXPIRED
            else:
                after = current
            if after != current:
                connection.execute("UPDATE agent_jobs SET lifecycle_state=?,state_at_ns=? WHERE job_id=?",
                    (after.value, now_ns, job_id))
                self._event(connection, job_id, current.value, after.value, job["lease_epoch"], now_ns,
                    {"receipt_hash": receipt.receipt_hash, "authoritative": authoritative})
        return authoritative

    def transition_terminal(self, job_id: str, state: AgentJobStateV1, *, now_ns: int, reason: str) -> AgentJobV1:
        if state not in _TERMINAL:
            raise ValueError("terminal transition requires an allowed terminal state")
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            current = AgentJobStateV1(row["lifecycle_state"])
            if current not in _TERMINAL:
                if state == AgentJobStateV1.EXPIRED and now_ns < row["deadline_ns"]:
                    raise ValueError("job deadline has not elapsed")
                self._set_terminal(connection, row, state, now_ns, reason)
            updated = connection.execute("SELECT * FROM agent_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._job(updated)

    def _set_terminal(self, connection: sqlite3.Connection, row: sqlite3.Row,
                      state: AgentJobStateV1, now_ns: int, reason: str) -> None:
        current = AgentJobStateV1(row["lifecycle_state"])
        if current in _TERMINAL:
            return
        connection.execute("UPDATE agent_jobs SET lifecycle_state=?,state_at_ns=?,lease_expires_at_ns=? WHERE job_id=?",
            (state.value, now_ns, now_ns, row["job_id"]))
        self._event(connection, row["job_id"], current.value, state.value, row["lease_epoch"], now_ns,
                    {"reason": reason})

    def results(self, request_key: str) -> tuple[dict[str, Any], ...]:
        with self._lock:
            rows = self._connection.execute("SELECT * FROM agent_results WHERE request_key=? "
                "ORDER BY received_at_ns,result_id", (request_key,)).fetchall()
        return tuple(dict(row) for row in rows)

    def authoritative_result(self, request_key: str) -> tuple[ResearchProposalV1, AgentValidationReceiptV1] | None:
        """Load the single accepted proposal bound to its immutable result and receipt."""
        with self._lock:
            row = self._connection.execute(
                "SELECT a.result_hash,a.result_json,v.receipt_hash,v.receipt_json "
                "FROM agent_authorities x JOIN agent_results a ON a.result_id=x.result_id "
                "JOIN agent_validation_receipts v ON v.receipt_hash=x.receipt_hash "
                "WHERE x.request_key=? AND v.authoritative=1", (request_key,)).fetchone()
        if row is None:
            return None
        result = json.loads(row["result_json"])
        raw = result.get("raw_output") if isinstance(result, dict) else None
        if not isinstance(raw, str):
            raise RuntimeError("authoritative agent result has no structured output")
        proposal = ResearchProposalV1.from_dict(json.loads(raw))
        receipt = AgentValidationReceiptV1.from_dict(json.loads(row["receipt_json"]))
        if (receipt.validation_status != "VALID" or receipt.provider_result_hash != row["result_hash"]
                or receipt.receipt_hash != row["receipt_hash"]):
            raise RuntimeError("authoritative proposal receipt/result binding is corrupt")
        return proposal, receipt

    def known_proposal_hashes(self, research_family_id: str, *, excluding_request_key: str | None = None) -> tuple[str, ...]:
        """Return deterministic content/novelty identities already retained for this family."""
        with self._lock:
            rows = self._connection.execute("SELECT r.request_key,r.request_json,a.result_json FROM agent_requests r "
                "JOIN agent_results a ON a.request_key=r.request_key ORDER BY a.received_at_ns,a.result_id").fetchall()
        hashes: set[str] = set()
        for row in rows:
            request = json.loads(row["request_json"])
            if (request.get("research_family_id") != research_family_id
                    or row["request_key"] == excluding_request_key):
                continue
            result = json.loads(row["result_json"])
            raw = result.get("raw_output") if isinstance(result, dict) else None
            if not isinstance(raw, str):
                continue
            try:
                proposal = ResearchProposalV1.from_dict(json.loads(raw))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError, RecursionError):
                continue
            hashes.add(proposal.content_hash)
            hashes.add(sha256_json({"family": proposal.research_family_id,
                "rule": proposal.proposed_rule.to_dict(), "features": sorted(proposal.feature_dependencies),
                "population": proposal.target_population, "horizon": proposal.horizon,
                "cost": proposal.cost_semantics, "ablation": proposal.intended_ablation}))
        return tuple(sorted(hashes))


_TERMINAL = frozenset({AgentJobStateV1.VALIDATED, AgentJobStateV1.UNAVAILABLE, AgentJobStateV1.INVALID,
                       AgentJobStateV1.EXPIRED, AgentJobStateV1.CANCELLED})


class ActionAssessmentRepository:
    """Append-only hidden critic ledger, written only by atlas-ops/controller."""

    def __init__(self, path: str | Path, *, price_schedule: DeepSeekPriceScheduleV1) -> None:
        raw = str(path)
        if not raw or raw.startswith("file:") or "://" in raw:
            raise ValueError("action-assessment persistence requires a local ops.sqlite path")
        self.path = raw
        self.price_schedule = price_schedule
        self._lock = threading.RLock()
        self._writer_thread_id = threading.get_ident()
        self._connection = sqlite3.connect(raw, isolation_level=None, check_same_thread=False, timeout=30)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=30000")
        if self._connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
            self._connection.close()
            raise RuntimeError("action-assessment persistence requires existing ops WAL mode")
        validate_read_only(self._connection)
        initialize_agent_extension(self._connection)
        initialize_action_assessment_extension(self._connection)

    def close(self) -> None:
        self.assert_writer_thread()
        with self._lock:
            self._connection.close()

    def assert_writer_thread(self) -> None:
        if threading.get_ident() != self._writer_thread_id:
            raise RuntimeError("action-assessment ledger is owned by the atlas-ops controller thread")

    def __enter__(self) -> ActionAssessmentRepository:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _transaction(self):
        repository = self

        class Transaction:
            def __enter__(self) -> sqlite3.Connection:
                repository.assert_writer_thread()
                repository._lock.acquire()
                repository._connection.execute("BEGIN IMMEDIATE")
                return repository._connection

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
                try:
                    repository._connection.rollback() if exc_type else repository._connection.commit()
                finally:
                    repository._lock.release()
        return Transaction()

    def record_skip(self, receipt_ref: str, reason_code: str, *, now_ns: int) -> None:
        sha256_ref(receipt_ref, field="receipt_ref")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", reason_code):
            raise ValueError("action-assessment skip reason is invalid")
        with self._transaction() as connection:
            row = connection.execute("SELECT reason_code FROM agent_action_assessment_skips WHERE receipt_ref=?",
                                     (receipt_ref,)).fetchone()
            if row is not None:
                if row["reason_code"] != reason_code:
                    raise ValueError("receipt was already sealed with a different action-critic skip reason")
                return
            connection.execute("INSERT INTO agent_action_assessment_skips VALUES(?,?,?)",
                               (receipt_ref, reason_code, now_ns))

    def persist_packet_request(self, packet: Any, request: Any, *, now_ns: int) -> None:
        packet_body, request_body = packet.to_dict(), request.to_dict()
        packet_json, request_json = canonical_json(packet_body), canonical_json(request_body)
        if _safe_json(packet_body) != packet_json or _safe_json(request_body) != request_json:
            raise ValueError("sensitive material cannot be persisted in an action-assessment contract")
        if (request.packet_ref != packet.packet_ref or request.packet_hash != packet.content_hash
                or request.action_hash != packet.action_hash):
            raise ValueError("action-assessment request does not bind the exact sealed packet")
        with self._transaction() as connection:
            existing_packet = connection.execute("SELECT packet_hash,packet_json FROM agent_action_assessment_packets "
                "WHERE packet_ref=?", (packet.packet_ref,)).fetchone()
            if existing_packet is not None:
                if existing_packet["packet_hash"] != packet.content_hash or existing_packet["packet_json"] != packet_json:
                    raise ValueError("content-addressed sealed packet identity collision")
            else:
                connection.execute("INSERT INTO agent_action_assessment_packets VALUES(?,?,?,?,?)",
                    (packet.packet_ref, packet.content_hash, packet_json, packet.originating_receipt_ref, now_ns))
            existing_request = connection.execute("SELECT request_hash,request_json FROM agent_action_assessment_requests "
                "WHERE request_id=?", (request.request_id,)).fetchone()
            if existing_request is not None:
                if existing_request["request_hash"] != request.content_hash or existing_request["request_json"] != request_json:
                    raise ValueError("deterministic action-assessment request identity collision")
            else:
                connection.execute("INSERT INTO agent_action_assessment_requests VALUES(?,?,?,?,?)",
                    (request.request_id, request.content_hash, packet.packet_ref, request_json, now_ns))

    def request_state(self, request_id: str) -> dict[str, Any] | None:
        self.assert_writer_thread()
        with self._lock:
            row = self._connection.execute("SELECT r.request_json,r.request_hash,p.packet_ref,p.packet_hash,"
                "a.attempt_id,d.authorization_id,d.authorization_json,d.authorization_hash,o.status,o.result_json,"
                "o.provider_output_hash,o.failure_code,o.received_at_ns,o.eligible FROM agent_action_assessment_requests r "
                "JOIN agent_action_assessment_packets p ON p.packet_ref=r.packet_ref "
                "LEFT JOIN agent_action_assessment_attempts a ON a.request_id=r.request_id "
                "LEFT JOIN agent_action_assessment_dispatches d ON d.request_id=r.request_id "
                "LEFT JOIN agent_action_assessment_outcomes o ON o.request_id=r.request_id WHERE r.request_id=?",
                (request_id,)).fetchone()
        return dict(row) if row is not None else None

    def create_attempt(self, request_id: str, *, attempt_id: str, now_ns: int) -> None:
        with self._transaction() as connection:
            prior = connection.execute("SELECT attempt_id FROM agent_action_assessment_attempts WHERE request_id=?",
                                       (request_id,)).fetchone()
            if prior is not None:
                if prior["attempt_id"] != attempt_id:
                    raise ValueError("action-assessment request already has its one attempt")
                return
            connection.execute("INSERT INTO agent_action_assessment_attempts VALUES(?,?,?,?,?)",
                               (attempt_id, request_id, 1, "CREATED", now_ns))

    def reserve_cost(self, request_id: str, *, attempt_id: str, reservation_id: str, now_ns: int,
                     reserved_usd: Decimal) -> None:
        if reserved_usd != self.price_schedule.worst_case_call_usd(input_tokens=12_000, output_tokens=2_048):
            raise ValueError("critic reservation does not use the conservative existing DeepSeek price schedule")
        utc_day = now_ns // _NS_PER_DAY
        with self._transaction() as connection:
            prior = connection.execute("SELECT reservation_id,reserved_usd,price_schedule_hash "
                "FROM agent_action_assessment_reservations WHERE request_id=?", (request_id,)).fetchone()
            if prior is not None:
                if (prior["reservation_id"] != reservation_id or prior["reserved_usd"] != str(reserved_usd)
                        or prior["price_schedule_hash"] != self.price_schedule.content_hash):
                    raise ValueError("action-assessment cost reservation is immutable")
                return
            rows = connection.execute("SELECT reserved_usd FROM agent_action_assessment_reservations WHERE utc_day=?",
                                      (utc_day,)).fetchall()
            used = sum((Decimal(row["reserved_usd"]) for row in rows), Decimal(0))
            if used + reserved_usd > self.price_schedule.maximum_daily_cost_usd:
                raise ValueError("action-assessment daily cost ceiling is exhausted")
            connection.execute("INSERT INTO agent_action_assessment_reservations VALUES(?,?,?,?,?,?,?)",
                (reservation_id, request_id, attempt_id, utc_day, str(reserved_usd),
                 self.price_schedule.content_hash, now_ns))

    def persist_dispatch_authorization(self, authorization: Any, *, now_ns: int) -> None:
        body = authorization.to_dict()
        encoded = canonical_json(body)
        if _safe_json(body) != encoded:
            raise ValueError("sensitive material cannot be persisted in a dispatch authorization")
        with self._transaction() as connection:
            attempt = connection.execute("SELECT request_id FROM agent_action_assessment_attempts WHERE attempt_id=?",
                                         (authorization.attempt_id,)).fetchone()
            if attempt is None:
                raise ValueError("dispatch authorization lacks its persisted attempt")
            request_id = attempt["request_id"]
            existing = connection.execute("SELECT authorization_hash,authorization_json FROM "
                "agent_action_assessment_dispatches WHERE request_id=?", (request_id,)).fetchone()
            if existing is not None:
                if existing["authorization_hash"] != authorization.authorization_hash or existing["authorization_json"] != encoded:
                    raise ValueError("action-assessment dispatch authorization is already sealed")
                return
            request = connection.execute("SELECT request_hash FROM agent_action_assessment_requests WHERE request_id=?",
                                         (request_id,)).fetchone()
            reservation = connection.execute("SELECT reservation_id FROM agent_action_assessment_reservations "
                                             "WHERE request_id=?", (request_id,)).fetchone()
            if (request is None or request["request_hash"] != authorization.request_hash or reservation is None
                    or reservation["reservation_id"] != authorization.reservation_id):
                raise ValueError("dispatch authorization must follow its immutable request and cost reservation")
            connection.execute("INSERT INTO agent_action_assessment_dispatches VALUES(?,?,?,?,?,?)",
                (authorization.authorization_id, authorization.attempt_id, request_id, encoded,
                 authorization.authorization_hash, now_ns))

    def has_dispatch(self, request_id: str) -> bool:
        self.assert_writer_thread()
        with self._lock:
            return self._connection.execute("SELECT 1 FROM agent_action_assessment_dispatches WHERE request_id=?",
                                            (request_id,)).fetchone() is not None

    def record_validation(self, request_id: str, attempt_id: str, receipt: Any, *, now_ns: int) -> None:
        body = receipt.to_dict()
        with self._transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO agent_action_assessment_validations VALUES(?,?,?,?,?)",
                (receipt.receipt_hash, request_id, attempt_id, canonical_json(body), now_ns))

    def record_outcome(self, request_id: str, *, attempt_id: str | None, status: str,
                       result: Mapping[str, Any] | None, provider_output_hash: str | None,
                       failure_code: str | None, received_at_ns: int, eligible: bool) -> None:
        if status not in {"COMPLETE", "REFUSED", "UNAVAILABLE", "INVALID", "EXPIRED", "SKIPPED"}:
            raise ValueError("action-assessment terminal status is invalid")
        if eligible and (status != "COMPLETE" or result is None or attempt_id is None):
            raise ValueError("only a validated complete critic result can be accepted shadow evidence")
        if provider_output_hash is not None:
            sha256_ref(provider_output_hash, field="provider_output_hash")
        if result is None:
            encoded = None
        else:
            encoded = canonical_json(dict(result))
            if _safe_json(dict(result)) != encoded:
                raise ValueError("sensitive material cannot be persisted in critic findings")
        with self._transaction() as connection:
            packet = connection.execute("SELECT packet_ref FROM agent_action_assessment_requests WHERE request_id=?",
                                        (request_id,)).fetchone()
            if packet is None:
                raise ValueError("action-assessment outcome lacks its immutable request")
            prior = connection.execute("SELECT status,result_json,eligible FROM agent_action_assessment_outcomes "
                                       "WHERE request_id=?", (request_id,)).fetchone()
            if prior is not None:
                if prior["status"] != status or prior["result_json"] != encoded or bool(prior["eligible"]) != eligible:
                    raise ValueError("one terminal action-assessment outcome is already sealed")
                return
            outcome_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"atlas-action-assessment-outcome:{request_id}"))
            if eligible:
                assert result is not None and attempt_id is not None
                result_hash = sha256_json(dict(result))
                accepted = connection.execute("SELECT request_id,result_hash FROM "
                    "agent_action_assessment_acceptances WHERE packet_ref=?", (packet["packet_ref"],)).fetchone()
                if accepted is not None and (accepted["request_id"] != request_id or accepted["result_hash"] != result_hash):
                    raise ValueError("sealed packet already has its one accepted shadow assessment")
                if accepted is None:
                    connection.execute("INSERT INTO agent_action_assessment_acceptances VALUES(?,?,?,?)",
                        (packet["packet_ref"], request_id, result_hash, received_at_ns))
            connection.execute("INSERT INTO agent_action_assessment_outcomes VALUES(?,?,?,?,?,?,?,?,?)",
                (outcome_id, request_id, attempt_id, status, encoded, provider_output_hash, failure_code,
                 received_at_ns, int(eligible)))

    def recover_dispatch_without_result(self, request_id: str, *, now_ns: int) -> None:
        state = self.request_state(request_id)
        if state is None or state["authorization_id"] is None or state["status"] is not None:
            return
        self.record_outcome(request_id, attempt_id=state["attempt_id"], status="UNAVAILABLE", result=None,
            provider_output_hash=None, failure_code="DISPATCH_OUTCOME_LOST_ON_RESTART", received_at_ns=now_ns,
            eligible=False)

    def authorized_requests_without_terminal_result(self) -> tuple[str, ...]:
        self.assert_writer_thread()
        with self._lock:
            rows = self._connection.execute("SELECT d.request_id FROM agent_action_assessment_dispatches d "
                "LEFT JOIN agent_action_assessment_outcomes o ON o.request_id=d.request_id "
                "WHERE o.request_id IS NULL ORDER BY d.created_at_ns,d.request_id").fetchall()
        return tuple(str(row["request_id"]) for row in rows)

    def unprojected_terminal_records(self, *, limit: int = 2) -> tuple[Mapping[str, Any], ...]:
        self.assert_writer_thread()
        if type(limit) is not int or not 0 <= limit <= 64:
            raise ValueError("observation projection bound must be between zero and 64")
        with self._lock:
            rows = self._connection.execute("SELECT r.request_id,r.request_hash,r.request_json,p.packet_ref,"
                "p.packet_hash,p.packet_json,o.status,o.result_json,o.failure_code,o.received_at_ns,o.eligible,"
                "d.authorization_json,d.authorization_hash FROM agent_action_assessment_requests r "
                "JOIN agent_action_assessment_packets p ON p.packet_ref=r.packet_ref "
                "JOIN agent_action_assessment_outcomes o ON o.request_id=r.request_id "
                "LEFT JOIN agent_action_assessment_dispatches d ON d.request_id=r.request_id "
                "LEFT JOIN agent_action_assessment_observation_projections x ON x.request_id=r.request_id "
                "WHERE x.request_id IS NULL ORDER BY o.received_at_ns,r.request_id LIMIT ?", (limit,)).fetchall()
        return tuple(dict(row) for row in rows)

    def mark_observation_projected(self, request_id: str, observation_ref: str, *, now_ns: int) -> None:
        self.assert_writer_thread()
        sha256_ref(observation_ref, field="observation_ref")
        with self._transaction() as connection:
            prior = connection.execute("SELECT observation_ref FROM agent_action_assessment_observation_projections "
                                       "WHERE request_id=?", (request_id,)).fetchone()
            if prior is not None:
                if prior["observation_ref"] != observation_ref:
                    raise ValueError("critic observation projection identity is already sealed")
                return
            terminal = connection.execute("SELECT 1 FROM agent_action_assessment_outcomes WHERE request_id=?",
                                          (request_id,)).fetchone()
            if terminal is None:
                raise ValueError("critic observation cannot precede a terminal controller result")
            connection.execute("INSERT INTO agent_action_assessment_observation_projections VALUES(?,?,?)",
                               (request_id, observation_ref, now_ns))
