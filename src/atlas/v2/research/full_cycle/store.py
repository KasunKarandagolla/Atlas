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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STORE_VERSION = "ATLAS_RED_FULL_CYCLE_STORE_V1"

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
    def begin_cycle(self, cycle_id: str, cycle_kind: str, *, started_at_ns: int, cutoff_ns: int,
                    server_time_ns: int | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO cycle(cycle_id, cycle_kind, started_at_ns, completed_at_ns, "
                "cutoff_ns, server_time_ns, status, request_count, failure_reason) "
                "VALUES(?,?,?,?,?,?,'RUNNING',0,NULL)",
                (cycle_id, cycle_kind, started_at_ns, None, cutoff_ns, server_time_ns),
            )

    def complete_cycle(self, cycle_id: str, *, status: str, request_count: int,
                       failure_reason: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE cycle SET completed_at_ns=?, status=?, request_count=?, failure_reason=? "
                "WHERE cycle_id=?",
                (time.time_ns(), status, request_count, failure_reason, cycle_id),
            )

    def record_observation(self, row: Mapping[str, Any]) -> None:
        payload = row.get("payload_zlib")
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO source_observation(observation_id, cycle_id, venue, source_id, "
                "endpoint, instrument_symbol, event_type, request_sha256, payload_sha256, payload_bytes, "
                "payload_zlib, event_at_ns, published_at_ns, received_at_ns, available_at_ns, http_status, "
                "availability_class, sequence, quality_flags, error_kind) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["observation_id"], row["cycle_id"], row["venue"], row["source_id"],
                    row["endpoint"], row.get("instrument_symbol"), row["event_type"],
                    row["request_sha256"], row["payload_sha256"], int(row["payload_bytes"]),
                    payload, row.get("event_at_ns"), row.get("published_at_ns"),
                    int(row["received_at_ns"]), int(row["available_at_ns"]), row.get("http_status"),
                    row.get("availability_class", "ACTUAL_SYSTEM"), row.get("sequence"),
                    json.dumps(row.get("quality_flags", []), sort_keys=True), row.get("error_kind"),
                ),
            )

    def record_products(self, rows: Iterable[Mapping[str, Any]]) -> None:
        payload = [
            (
                row["product_ref"], int(row["observed_at_ns"]), row["cycle_id"], row["venue"],
                row["symbol"], row["base_asset"], row["quote_asset"], row["contract_type"],
                row["trading_status"], row["tick_size"], row["qty_step"], row["min_qty"],
                row.get("max_qty"), row.get("min_notional"), row.get("listing_at_ns"),
                row.get("delisting_at_ns"), row["metadata_ref"],
            )
            for row in rows
        ]
        if not payload:
            return
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO product_metadata(product_ref, observed_at_ns, cycle_id, venue, symbol, "
                "base_asset, quote_asset, contract_type, trading_status, tick_size, qty_step, min_qty, "
                "max_qty, min_notional, listing_at_ns, delisting_at_ns, metadata_ref) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                payload,
            )

    def record_bars(self, rows: Iterable[Mapping[str, Any]]) -> None:
        payload = [
            (
                row["bar_id"], row["cycle_id"], row["symbol"], row["interval"],
                int(row["open_at_ns"]), int(row["close_at_ns"]), row["open"], row["high"], row["low"],
                row["close"], row["volume"], row.get("quote_volume"), row.get("trades"),
                int(row["available_at_ns"]), int(row["received_at_ns"]), row["raw_row_json"],
                row["source_observation_id"],
            )
            for row in rows
        ]
        if not payload:
            return
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO bar(bar_id, cycle_id, symbol, interval, open_at_ns, close_at_ns, "
                "open, high, low, close, volume, quote_volume, trades, available_at_ns, received_at_ns, "
                "raw_row_json, source_observation_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                payload,
            )

    def record_discovery(self, rows: Iterable[Mapping[str, Any]]) -> None:
        payload = [
            (
                row["cycle_id"], row["symbol"], int(row.get("observed", 0)), int(row.get("eligible", 0)),
                row.get("exclusion_reason"), row.get("quote_volume_24h_usd"), row.get("turnover_24h_usd"),
                row.get("price_change_pct_24h"), row.get("last_price"), row.get("spread_bps"),
                row.get("mark_price"), row.get("funding_rate"), row.get("next_funding_at_ns"),
                row.get("discovery_score"), row.get("discovery_rank"), row.get("compute_tier"),
                int(row.get("deep_cohort", 0)), row.get("selection_probability"),
            )
            for row in rows
        ]
        if not payload:
            return
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO discovery_population(cycle_id, symbol, observed, eligible, "
                "exclusion_reason, quote_volume_24h_usd, turnover_24h_usd, price_change_pct_24h, last_price, "
                "spread_bps, mark_price, funding_rate, next_funding_at_ns, discovery_score, discovery_rank, "
                "compute_tier, deep_cohort, selection_probability) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                payload,
            )

    def record_context(self, rows: Iterable[Mapping[str, Any]]) -> None:
        payload = [
            (
                row["cycle_id"], row["symbol"], row["horizon"], int(row["bars_used"]),
                row.get("realized_vol_bps"), row.get("atr14_bps"), row.get("atr14_abs"),
                row.get("ema20_h1"), row.get("ema50_h1"), row.get("trend_score"), row.get("adx14"),
                row.get("compression_ratio"), row.get("classification"), row.get("range_position"),
                row.get("return_bps"), row.get("corr_btc"), row.get("beta_btc"), row.get("corr_eth"),
                row.get("spread_bps"), row.get("turnover_24h_usd"), row.get("funding_rate"),
                row.get("oi_change_15m"), int(row["computed_at_ns"]),
            )
            for row in rows
        ]
        if not payload:
            return
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO market_context(cycle_id, symbol, horizon, bars_used, realized_vol_bps, "
                "atr14_bps, atr14_abs, ema20_h1, ema50_h1, trend_score, adx14, compression_ratio, "
                "classification, range_position, return_bps, corr_btc, beta_btc, corr_eth, spread_bps, "
                "turnover_24h_usd, funding_rate, oi_change_15m, computed_at_ns) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                payload,
            )

    def record_applicability(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO strategy_applicability(applicability_id, cycle_id, symbol, strategy, "
                "native_clock, native_due_at_ns, information_cutoff_ns, universe_eligible, status, reason_code, "
                "detail, indicator_values, setup_predicates, trigger_predicates, unavailable_evidence, "
                "candidate_action_ref, evidence_refs, source_grade) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["applicability_id"], row["cycle_id"], row["symbol"], row["strategy"],
                    row["native_clock"], int(row["native_due_at_ns"]), int(row["information_cutoff_ns"]),
                    int(row.get("universe_eligible", 0)), row["status"], row["reason_code"],
                    json.dumps(row.get("detail", {}), sort_keys=True, default=str),
                    json.dumps(row.get("indicator_values", {}), sort_keys=True, default=str),
                    json.dumps(row.get("setup_predicates", {}), sort_keys=True, default=str),
                    json.dumps(row.get("trigger_predicates", {}), sort_keys=True, default=str),
                    json.dumps(row.get("unavailable_evidence", []), sort_keys=True, default=str),
                    row.get("candidate_action_ref"),
                    json.dumps(row.get("evidence_refs", []), sort_keys=True, default=str),
                    row.get("source_grade", "UNQUALIFIED"),
                ),
            )

    def record_action(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO research_action(action_id, cycle_id, symbol, strategy, policy_id, "
                "policy_hash, side, direction_origin, entry_reference, entry_collar_bps, stop_price, "
                "stop_basis, quantity, notional_usd, risk_profile, horizon_end_ns, deadline_ns, "
                "take_profit_price, take_profit_levels, fee_bps, expected_spread_bps, funding_assumption, "
                "slippage_bps, hard_refusal_reason, evidence_refs, code_hash, sealed_at_ns) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["action_id"], row["cycle_id"], row["symbol"], row["strategy"], row["policy_id"],
                    row["policy_hash"], row["side"], row["direction_origin"], row["entry_reference"],
                    row.get("entry_collar_bps"), row["stop_price"], row["stop_basis"], row["quantity"],
                    row["notional_usd"], row["risk_profile"], int(row["horizon_end_ns"]),
                    int(row["deadline_ns"]), row.get("take_profit_price"),
                    json.dumps(row.get("take_profit_levels", []), sort_keys=True, default=str),
                    row["fee_bps"], row["expected_spread_bps"], row["funding_assumption"],
                    row["slippage_bps"], row.get("hard_refusal_reason"),
                    json.dumps(row.get("evidence_refs", []), sort_keys=True, default=str),
                    row["code_hash"], int(row["sealed_at_ns"]),
                ),
            )

    def record_lifecycle(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO lifecycle_evidence(lifecycle_id, action_id, management_kind, path_id, "
                "entry_state, exit_state, exit_reason, gross_cash, cost_cash, net_cash, horizon_end_ns, "
                "uncertainty, detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["lifecycle_id"], row["action_id"], row["management_kind"], row["path_id"],
                    row["entry_state"], row["exit_state"], row["exit_reason"], row["gross_cash"],
                    row["cost_cash"], row["net_cash"], int(row["horizon_end_ns"]), row["uncertainty"],
                    json.dumps(row.get("detail", {}), sort_keys=True, default=str),
                ),
            )

    def record_outcome(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO mature_outcome(action_id, cycle_id, symbol, strategy, maturity_status, "
                "horizon_ns, net_cash, return_bps, max_favorable_bps, max_adverse_bps, censoring_reason, "
                "evaluated_at_ns, evaluation_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["action_id"], row["cycle_id"], row["symbol"], row["strategy"],
                    row["maturity_status"], int(row["horizon_ns"]), row.get("net_cash"),
                    row.get("return_bps"), row.get("max_favorable_bps"), row.get("max_adverse_bps"),
                    row.get("censoring_reason"), int(row["evaluated_at_ns"]), row["evaluation_version"],
                ),
            )

    def record_model(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO model_inventory(model_id, checkpoint, revision, role, weight_sha256, "
                "tokenizer, tokenizer_revision, tokenizer_sha256, disk_bytes, runtime, load_status, "
                "inference_status, detail, updated_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["model_id"], row["checkpoint"], row.get("revision"), row["role"],
                    row.get("weight_sha256"), row.get("tokenizer"), row.get("tokenizer_revision"),
                    row.get("tokenizer_sha256"), row.get("disk_bytes"), row.get("runtime"),
                    row["load_status"], row["inference_status"],
                    json.dumps(row.get("detail", {}), sort_keys=True, default=str),
                    int(row["updated_at_ns"]),
                ),
            )

    def record_provenance(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO ml_provenance(provenance_id, experiment_id, model_id, "
                "information_cutoff_ns, split_name, feature_names, target_name, n_rows, n_matured, "
                "metric_name, metric_value, comparator_name, comparator_value, detail, created_at_ns) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["provenance_id"], row["experiment_id"], row["model_id"],
                    int(row["information_cutoff_ns"]), row["split_name"],
                    json.dumps(row.get("feature_names", []), sort_keys=True, default=str),
                    row["target_name"], int(row["n_rows"]), int(row["n_matured"]), row.get("metric_name"),
                    row.get("metric_value"), row.get("comparator_name"), row.get("comparator_value"),
                    json.dumps(row.get("detail", {}), sort_keys=True, default=str),
                    int(row["created_at_ns"]),
                ),
            )

    def record_missingness(self, row: Mapping[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO missingness(missing_id, cycle_id, symbol, scope, reason_code, detail, "
                "recorded_at_ns) VALUES(?,?,?,?,?,?,?)",
                (
                    row["missing_id"], row["cycle_id"], row.get("symbol"), row["scope"], row["reason_code"],
                    json.dumps(row.get("detail", {}), sort_keys=True, default=str),
                    int(row["recorded_at_ns"]),
                ),
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
