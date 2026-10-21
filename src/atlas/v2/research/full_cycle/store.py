"""Durable research evidence store for the ATLAS RED full-cycle programme.

Every table is append-only by construction of the primary key: the same
original observation cannot be silently overwritten, and a later correction is
stored as a new revision row. This store is a *research* projection; it never
replaces the Ops artifact store or the production archive.
"""

from __future__ import annotations

import json
import sqlite3
import time
import zlib
from collections.abc import Iterable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STORE_VERSION = "ATLAS_RED_FULL_CYCLE_STORE_V2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS store_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cycle (
    cycle_id TEXT PRIMARY KEY,
    cycle_kind TEXT NOT NULL,
    started_at_ns INTEGER NOT NULL,
    completed_at_ns INTEGER,
    cutoff_ns INTEGER NOT NULL,
    server_time_ns INTEGER,
    status TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS source_observation (
    observation_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    source_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    instrument_symbol TEXT,
    event_type TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL,
    payload_zlib BLOB,
    event_at_ns INTEGER,
    published_at_ns INTEGER,
    received_at_ns INTEGER NOT NULL,
    available_at_ns INTEGER NOT NULL,
    http_status INTEGER,
    availability_class TEXT NOT NULL,
    sequence TEXT,
    quality_flags TEXT NOT NULL DEFAULT '[]',
    error_kind TEXT
);
CREATE INDEX IF NOT EXISTS source_observation_cycle ON source_observation(cycle_id);
CREATE INDEX IF NOT EXISTS source_observation_symbol ON source_observation(instrument_symbol, event_type);

CREATE TABLE IF NOT EXISTS product_metadata (
    product_ref TEXT NOT NULL,
    observed_at_ns INTEGER NOT NULL,
    cycle_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    contract_type TEXT NOT NULL,
    trading_status TEXT NOT NULL,
    tick_size TEXT NOT NULL,
    qty_step TEXT NOT NULL,
    min_qty TEXT NOT NULL,
    max_qty TEXT,
    min_notional TEXT,
    listing_at_ns INTEGER,
    delisting_at_ns INTEGER,
    metadata_ref TEXT NOT NULL,
    PRIMARY KEY (product_ref, observed_at_ns)
);

CREATE TABLE IF NOT EXISTS bar (
    bar_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_at_ns INTEGER NOT NULL,
    close_at_ns INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    quote_volume TEXT,
    trades INTEGER,
    available_at_ns INTEGER NOT NULL,
    received_at_ns INTEGER NOT NULL,
    raw_row_json TEXT NOT NULL,
    source_observation_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bar_lookup ON bar(symbol, interval, close_at_ns);
CREATE INDEX IF NOT EXISTS bar_cycle ON bar(cycle_id);

CREATE TABLE IF NOT EXISTS discovery_population (
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    observed INTEGER NOT NULL,
    eligible INTEGER NOT NULL,
    exclusion_reason TEXT,
    quote_volume_24h_usd TEXT,
    turnover_24h_usd TEXT,
    price_change_pct_24h TEXT,
    last_price TEXT,
    spread_bps TEXT,
    mark_price TEXT,
    funding_rate TEXT,
    next_funding_at_ns INTEGER,
    discovery_score TEXT,
    discovery_rank INTEGER,
    compute_tier TEXT,
    deep_cohort INTEGER NOT NULL DEFAULT 0,
    selection_probability TEXT,
    PRIMARY KEY (cycle_id, symbol)
);
CREATE INDEX IF NOT EXISTS discovery_population_symbol ON discovery_population(symbol, cycle_id);

CREATE TABLE IF NOT EXISTS market_context (
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    horizon TEXT NOT NULL,
    bars_used INTEGER NOT NULL,
    realized_vol_bps TEXT,
    atr14_bps TEXT,
    atr14_abs TEXT,
    ema20_h1 TEXT,
    ema50_h1 TEXT,
    trend_score TEXT,
    adx14 TEXT,
    compression_ratio TEXT,
    classification TEXT,
    range_position TEXT,
    return_bps TEXT,
    corr_btc REAL,
    beta_btc REAL,
    corr_eth REAL,
    spread_bps TEXT,
    turnover_24h_usd TEXT,
    funding_rate TEXT,
    oi_change_15m TEXT,
    computed_at_ns INTEGER NOT NULL,
    PRIMARY KEY (cycle_id, symbol, horizon)
);

CREATE TABLE IF NOT EXISTS strategy_applicability (
    applicability_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    native_clock TEXT NOT NULL,
    native_due_at_ns INTEGER NOT NULL,
    information_cutoff_ns INTEGER NOT NULL,
    universe_eligible INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    indicator_values TEXT NOT NULL DEFAULT '{}',
    setup_predicates TEXT NOT NULL DEFAULT '{}',
    trigger_predicates TEXT NOT NULL DEFAULT '{}',
    unavailable_evidence TEXT NOT NULL DEFAULT '[]',
    candidate_action_ref TEXT,
    evidence_refs TEXT NOT NULL DEFAULT '[]',
    source_grade TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS applicability_cycle ON strategy_applicability(cycle_id);
CREATE INDEX IF NOT EXISTS applicability_symbol ON strategy_applicability(symbol, strategy);

CREATE TABLE IF NOT EXISTS research_action (
    action_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_hash TEXT NOT NULL,
    side TEXT NOT NULL,
    direction_origin TEXT NOT NULL,
    entry_reference TEXT NOT NULL,
    entry_collar_bps TEXT,
    stop_price TEXT NOT NULL,
    stop_basis TEXT NOT NULL,
    quantity TEXT NOT NULL,
    notional_usd TEXT NOT NULL,
    risk_profile TEXT NOT NULL,
    horizon_end_ns INTEGER NOT NULL,
    deadline_ns INTEGER NOT NULL,
    take_profit_price TEXT,
    take_profit_levels TEXT NOT NULL DEFAULT '[]',
    fee_bps TEXT NOT NULL,
    expected_spread_bps TEXT NOT NULL,
    funding_assumption TEXT NOT NULL,
    slippage_bps TEXT NOT NULL,
    hard_refusal_reason TEXT,
    evidence_refs TEXT NOT NULL DEFAULT '[]',
    code_hash TEXT NOT NULL,
    sealed_at_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS research_action_cycle ON research_action(cycle_id);

CREATE TABLE IF NOT EXISTS lifecycle_evidence (
    lifecycle_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    management_kind TEXT NOT NULL,
    path_id TEXT NOT NULL,
    entry_state TEXT NOT NULL,
    exit_state TEXT NOT NULL,
    exit_reason TEXT NOT NULL,
    gross_cash TEXT NOT NULL,
    cost_cash TEXT NOT NULL,
    net_cash TEXT NOT NULL,
    horizon_end_ns INTEGER NOT NULL,
    uncertainty TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS lifecycle_action ON lifecycle_evidence(action_id);

CREATE TABLE IF NOT EXISTS mature_outcome (
    action_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    maturity_status TEXT NOT NULL,
    horizon_ns INTEGER NOT NULL,
    net_cash TEXT,
    return_bps TEXT,
    max_favorable_bps TEXT,
    max_adverse_bps TEXT,
    censoring_reason TEXT,
    evaluated_at_ns INTEGER NOT NULL,
    evaluation_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS mature_outcome_strategy ON mature_outcome(strategy, maturity_status);

CREATE TABLE IF NOT EXISTS model_inventory (
    model_id TEXT PRIMARY KEY,
    checkpoint TEXT NOT NULL,
    revision TEXT,
    role TEXT NOT NULL,
    weight_sha256 TEXT,
    tokenizer TEXT,
    tokenizer_revision TEXT,
    tokenizer_sha256 TEXT,
    disk_bytes INTEGER,
    runtime TEXT,
    load_status TEXT NOT NULL,
    inference_status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    updated_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_provenance (
    provenance_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    information_cutoff_ns INTEGER NOT NULL,
    split_name TEXT NOT NULL,
    feature_names TEXT NOT NULL,
    target_name TEXT NOT NULL,
    n_rows INTEGER NOT NULL,
    n_matured INTEGER NOT NULL,
    metric_name TEXT,
    metric_value TEXT,
    comparator_name TEXT,
    comparator_value TEXT,
    detail TEXT NOT NULL DEFAULT '{}',
    created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS missingness (
    missing_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    symbol TEXT,
    scope TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    recorded_at_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS missingness_cycle ON missingness(cycle_id);
"""


@dataclass(frozen=True)
class ResearchStoreV1:
    path: Path

    @classmethod
    def open(cls, path: Path) -> ResearchStoreV1:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        store = cls(path=path)
        store._connect().executescript(SCHEMA)
        store._connect().commit()
        store.set_meta("store_version", STORE_VERSION)
        store._migrate()
        if store.get_meta("created_at_ns") is None:
            store.set_meta("created_at_ns", str(time.time_ns()))
        return store

    def _connect(self) -> sqlite3.Connection:
        """One long-lived WAL connection for the single-writer research store.

        Autocommit per statement is deliberately avoided: the bounded workspace
        filesystem makes one fsync per transaction expensive, so every write
        batch is a single explicit transaction.
        """

        connection = getattr(self, "_connection", None)
        if connection is None:
            connection = sqlite3.connect(self.path, timeout=60.0)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            object.__setattr__(self, "_connection", connection)
        return connection

    # -- meta ---------------------------------------------------------
    def set_meta(self, key: str, value: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO store_meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute("SELECT value FROM store_meta WHERE key=?", (key,)).fetchone()
        return None if row is None else str(row[0])

    # -- writers ------------------------------------------------------
    def begin_cycle(
        self, cycle_id: str, cycle_kind: str, *, started_at_ns: int, cutoff_ns: int, server_time_ns: int | None = None
    ) -> None:
        prior = self.query("SELECT * FROM cycle WHERE cycle_id=?", (cycle_id,))
        if prior:
            original = prior[0]
            if any(
                original[n] != v
                for n, v in (
                    ("cycle_kind", cycle_kind),
                    ("started_at_ns", started_at_ns),
                    ("cutoff_ns", cutoff_ns),
                    ("server_time_ns", server_time_ns),
                )
            ):
                raise ValueError("IMMUTABLE_CYCLE_IDENTITY_CONFLICT")
            return
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO cycle(cycle_id, cycle_kind, started_at_ns, completed_at_ns, "
                "cutoff_ns, server_time_ns, status, request_count, failure_reason) "
                "VALUES(?,?,?,?,?,?,'RUNNING',0,NULL)",
                (cycle_id, cycle_kind, started_at_ns, None, cutoff_ns, server_time_ns),
            )

    def complete_cycle(
        self, cycle_id: str, *, status: str, request_count: int, failure_reason: str | None = None
    ) -> None:
        prior = self.query("SELECT * FROM cycle WHERE cycle_id=?", (cycle_id,))
        if not prior:
            raise ValueError("CYCLE_NOT_FOUND")
        if prior[0]["completed_at_ns"] is not None:
            if any(
                prior[0][n] != v
                for n, v in (("status", status), ("request_count", request_count), ("failure_reason", failure_reason))
            ):
                raise ValueError("IMMUTABLE_CYCLE_COMPLETION_CONFLICT")
            return
        with self._connect() as connection:
            connection.execute(
                "UPDATE cycle SET completed_at_ns=?, status=?, request_count=?, failure_reason=? WHERE cycle_id=?",
                (time.time_ns(), status, request_count, failure_reason, cycle_id),
            )

    def attach_write_guard(self, guard) -> None:
        """Bind the operator's physical-space guard; schema and legacy readers stay unchanged."""
        object.__setattr__(self, "_write_guard", guard)

    def _record(self, table: str, row: Mapping[str, Any], *, commit: bool = True) -> None:
        """Canonical exact duplicate is idempotent; a conflicting identity aborts."""
        columns = {r[1] for r in self._connect().execute(f"PRAGMA table_info({table})")}
        payload = {}
        json_fields = {
            "quality_flags",
            "detail",
            "indicator_values",
            "setup_predicates",
            "trigger_predicates",
            "unavailable_evidence",
            "evidence_refs",
            "take_profit_levels",
            "feature_names",
            "provenance",
            "execution_plan",
        }
        for name, value in row.items():
            if name in json_fields and isinstance(value, str):
                value = json.loads(value)
            if name not in columns:
                raise ValueError(f"UNKNOWN_EVIDENCE_FIELD:{table}.{name}")
            payload[name] = (
                json.dumps(value, sort_keys=True, default=str, separators=(",", ":")) if name in json_fields else value
            )
        guard = getattr(self, "_write_guard", None)
        if guard is not None:
            size = sum(len(v) if isinstance(v,(str,bytes)) else 16 for v in payload.values())
            guard(size * 16 + 65536)  # DB, indexes, WAL and transaction overhead
        names = list(payload)
        identity = [r[1] for r in self._connect().execute(f"PRAGMA table_info({table})") if r[5]]
        if not identity and table == "outcome_revision":
            identity = ["outcome_id"]
        try:
            with self._connect() if commit else nullcontext(self._connect()) as connection:
                connection.execute(
                    f"INSERT INTO {table} ({','.join(names)}) VALUES ({','.join('?' for _ in names)}) ON CONFLICT ({','.join(identity)}) DO NOTHING",
                    tuple(payload[n] for n in names),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"IMMUTABLE_EVIDENCE_CONFLICT:{table}") from exc

    def record_observation(self, row: Mapping[str, Any]) -> None:
        self._record("source_observation", {"availability_class": "ACTUAL_SYSTEM", **row})

    def record_products(self, rows: Iterable[Mapping[str, Any]]) -> None:
        with self._connect():
            for row in rows:
                self._record("product_metadata", row, commit=False)

    def record_bars(self, rows: Iterable[Mapping[str, Any]]) -> None:
        with self._connect():
            for row in rows:
                self._record("bar", row, commit=False)

    def record_discovery(self, rows: Iterable[Mapping[str, Any]]) -> None:
        with self._connect():
            for row in rows:
                self._record("discovery_population", row, commit=False)

    def record_context(self, rows: Iterable[Mapping[str, Any]]) -> None:
        with self._connect():
            for row in rows:
                self._record("market_context", row, commit=False)

    def record_applicability(self, row: Mapping[str, Any]) -> None:
        self._record("strategy_applicability", row)

    def record_action(self, row: Mapping[str, Any]) -> None:
        if not row.get("policy_id") or not row.get("policy_hash"):
            raise ValueError("POLICY_IDENTITY_REQUIRED")
        self._record("research_action", row)

    def record_lifecycle(self, row: Mapping[str, Any]) -> None:
        self._record("lifecycle_evidence", row)

    def record_outcome(self, row: Mapping[str, Any]) -> None:
        import hashlib

        payload = dict(row)
        identity = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
        self._record("outcome_revision", {**payload, "outcome_id": identity})

    def record_model(self, row: Mapping[str, Any]) -> None:
        self._record("model_inventory", row)

    def record_provenance(self, row: Mapping[str, Any]) -> None:
        self._record("ml_provenance", row)

    def record_missingness(self, row: Mapping[str, Any]) -> None:
        self._record("missingness", row)

    def close(self) -> None:
        self._connect().close()
        object.__setattr__(self, "_connection", None)

    def _migrate(self) -> None:
        """Add fields without repairing/rewriting old evidence or original grades."""
        additions = {
            "strategy_applicability": {
                "policy_id": "TEXT NOT NULL DEFAULT ''",
                "policy_hash": "TEXT NOT NULL DEFAULT ''",
            },
            "research_action": {
                "provenance": "TEXT NOT NULL DEFAULT '{}'",
                "execution_plan": "TEXT NOT NULL DEFAULT '{}'",
                "decision_at_ns": "INTEGER",
                "setup_at_ns": "INTEGER",
                "trigger_at_ns": "INTEGER",
                "evidence_grade": "TEXT NOT NULL DEFAULT 'LEGACY_UNQUALIFIED'",
                "opportunity_id": "TEXT NOT NULL DEFAULT ''",
                "instrument_revision": "TEXT NOT NULL DEFAULT ''",
            },
            "market_context": {
                "feature_ref": "TEXT", "evidence_refs": "TEXT NOT NULL DEFAULT '[]'",
                "information_cutoff_ns": "INTEGER",
                "available_at_ns": "INTEGER",
                "instrument_revision": "TEXT NOT NULL DEFAULT ''",
                "source_grade": "TEXT NOT NULL DEFAULT 'LEGACY_UNQUALIFIED'",
            },
            "discovery_population": {
                "canonical_discovery_score": "TEXT",
                "canonical_discovery_ref": "TEXT",
                "canonical_unavailable_reason": "TEXT NOT NULL DEFAULT 'CANONICAL_INPUTS_UNAVAILABLE'",
                "discovery_policy_id": "TEXT NOT NULL DEFAULT 'RESEARCH_LIQUIDITY_ATTENTION_PROXY_V1'",
                "information_cutoff_ns": "INTEGER",
                "available_at_ns": "INTEGER",
            },
        }
        with self._connect() as c:
            for table, fields in additions.items():
                existing = {r[1] for r in c.execute(f"PRAGMA table_info({table})")}
                for name, definition in fields.items():
                    if name not in existing:
                        c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            # Preserve the original one-row outcomes; corrected evaluations append revisions.
            c.execute(
                "CREATE TABLE IF NOT EXISTS outcome_revision AS SELECT *, '' AS outcome_id, "
                "'LEGACY_UNQUALIFIED' AS evidence_grade, NULL AS label_available_at_ns, "
                "'' AS lifecycle_id FROM mature_outcome WHERE 0"
            )
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS original_action_opportunity ON research_action(opportunity_id) "
                "WHERE evidence_grade='ACTUAL_PROSPECTIVE_OBSERVATION' AND opportunity_id<>''"
            )
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS outcome_revision_identity ON outcome_revision(outcome_id)")
            c.execute(
                "CREATE VIEW IF NOT EXISTS effective_outcome AS "
                "SELECT * FROM outcome_revision r WHERE NOT EXISTS "
                "(SELECT 1 FROM outcome_revision n WHERE n.action_id=r.action_id "
                "AND (n.evaluated_at_ns>r.evaluated_at_ns OR "
                "(n.evaluated_at_ns=r.evaluated_at_ns AND n.outcome_id>r.outcome_id)))"
            )
            for (table,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                if table in ("store_meta", "cycle"):
                    continue
                info = c.execute(f"PRAGMA table_info({table})").fetchall()
                cols = [r[1] for r in info]
                pk = [r[1] for r in info if r[5]] or (["outcome_id"] if table == "outcome_revision" else [])
                if not pk:
                    raise ValueError(f"NO_IMMUTABLE_IDENTITY:{table}")
                key = " AND ".join(f'"{n}" IS NEW."{n}"' for n in pk)
                changed = " OR ".join(f'"{n}" IS NOT NEW."{n}"' for n in cols)
                c.execute(
                    f"CREATE TRIGGER IF NOT EXISTS immutable_insert_{table} BEFORE INSERT ON {table} "
                    f"WHEN EXISTS(SELECT 1 FROM {table} WHERE {key} AND ({changed})) "
                    "BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_EVIDENCE_CONFLICT'); END"
                )
                for operation in ("UPDATE", "DELETE"):
                    c.execute(
                        f"CREATE TRIGGER IF NOT EXISTS immutable_{operation}_{table} BEFORE {operation} ON {table} "
                        "BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_EVIDENCE_CONFLICT'); END"
                    )

    # -- readers ------------------------------------------------------
    def query(self, sql: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(sql, tuple(parameters)).fetchall()
        return [dict(row) for row in rows]

    def scalar(self, sql: str, parameters: Sequence[Any] = ()) -> Any:
        rows = self.query(sql, parameters)
        if not rows:
            return None
        return list(rows[0].values())[0]

    def table_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._connect() as connection:
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall():
                counts[name] = int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
        return counts

    def file_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            if candidate.exists():
                total += candidate.stat().st_size
        return total


def compress_payload(payload: bytes) -> bytes:
    return zlib.compress(payload, 6)
