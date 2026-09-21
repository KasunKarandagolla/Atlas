"""SQLite WAL durable journal with single-writer abstraction (freeze §1.4).

- WAL mode, synchronous=FULL, foreign_keys=ON.
- stdlib sqlite3 with explicit SQL (no ORM).
- Transactional writes; persistence failure raises PersistenceError (never success).
- Unique client-order-ID constraint enforced by schema.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

from atlas.domain.enums import (
    CommandOutcome,
    CommandType,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
)
from atlas.domain.execution import (
    Approval,
    Command,
    EconomicEvent,
    Intent,
    Observation,
    ProtectionObservation,
    Reservation,
)
from atlas.domain.money import canonical_decimal_str
from atlas.domain.trade_plan import TradePlan

from .migrations import bootstrap, current_version

TERMINAL_LIFECYCLES = frozenset({LifecycleState.CLOSED.value})


class PersistenceError(RuntimeError):
    """Explicit persistence failure. MUST NOT be treated as success."""


class SQLiteJournal:
    """Single-writer journal. One instance owns the connection for its path."""

    def __init__(self, path: str | Path, *, timeout: float = 10.0) -> None:
        self._path = str(path)
        self._lock = threading.Lock()
        try:
            self._conn = sqlite3.connect(
                self._path, timeout=timeout, isolation_level=None, check_same_thread=False
            )
            self._conn.row_factory = sqlite3.Row
            self._apply_pragmas()
            bootstrap(self._conn)
        except sqlite3.Error as exc:
            raise PersistenceError(f"failed to open journal at {self._path}: {exc}") from exc

    def _apply_pragmas(self) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            # Verify WAL requested (row holds mode). Best-effort: keep value for inspection.
            cur.execute("PRAGMA synchronous=FULL;")
            cur.execute("PRAGMA foreign_keys=ON;")
        except sqlite3.Error as exc:
            raise PersistenceError(f"pragma setup failed: {exc}") from exc

    def pragmas(self) -> dict[str, Any]:
        cur = self._conn.cursor()
        out: dict[str, Any] = {}
        for name in ("journal_mode", "synchronous", "foreign_keys"):
            try:
                cur.execute(f"PRAGMA {name};")
                row = cur.fetchone()
                out[name] = row[0] if row else None
            except sqlite3.Error as exc:
                raise PersistenceError(f"pragma read failed: {exc}") from exc
        return out

    def schema_version(self) -> int | None:
        try:
            return current_version(self._conn)
        except sqlite3.Error as exc:
            raise PersistenceError(f"schema version read failed: {exc}") from exc

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE;")
                yield cur
                self._conn.commit()
            except Exception:
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    pass
                raise

    def _wrap(self, op: str, fn):  # helper to convert sqlite errors
        try:
            return fn()
        except PersistenceError:
            raise
        except sqlite3.IntegrityError as exc:
            raise PersistenceError(f"{op} integrity failure: {exc}") from exc
        except sqlite3.Error as exc:
            raise PersistenceError(f"{op} failed: {exc}") from exc

    # ---- TradePlan ----
    def create_trade_plan(self, plan: TradePlan) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO trade_plans(plan_id, version, canonical_json, plan_hash,"
                    " expires_at_ns, created_at_ns) VALUES(?,?,?,?,?,?)",
                    (
                        plan.plan_id,
                        plan.version,
                        plan.to_canonical_json(),
                        plan.plan_hash(),
                        plan.expires_at_ns,
                        plan.created_at_ns,
                    ),
                )

        self._wrap("create_trade_plan", _op)

    def load_trade_plan(self, plan_id: str) -> TradePlan:
        def _op() -> TradePlan:
            cur = self._conn.cursor()
            cur.execute("SELECT canonical_json FROM trade_plans WHERE plan_id=?", (plan_id,))
            row = cur.fetchone()
            if row is None:
                raise PersistenceError(f"trade plan not found: {plan_id}")
            import json as _json

            from atlas.domain.enums import Side
            from atlas.domain.money import ensure_decimal

            d = _json.loads(row["canonical_json"])
            return TradePlan(
                plan_id=d["plan_id"],
                version=d["version"],
                policy_hash=d["policy_hash"],
                snapshot_hash=d["snapshot_hash"],
                expires_at_ns=d["expires_at_ns"],
                market=d["market"],
                account_scope=d["account_scope"],
                instrument=d["instrument"],
                side=Side(d["side"]),
                qty_limit=ensure_decimal(d["qty_limit"]),
                entry_policy=d["entry_policy"],
                collar=ensure_decimal(d["collar"]),
                stop=ensure_decimal(d["stop"]),
                stop_trigger_basis=d["stop_trigger_basis"],
                management_policy=d["management_policy"],
                horizon_end_ns=d["horizon_end_ns"],
                cost_distribution_ref=d["cost_distribution_ref"],
                normal_risk=ensure_decimal(d["normal_risk"]),
                stress_risk=ensure_decimal(d["stress_risk"]),
                margin=ensure_decimal(d["margin"]),
                leverage_bound=ensure_decimal(d["leverage_bound"]),
                risk_config_hash=d["risk_config_hash"],
                created_at_ns=d["created_at_ns"],
                available_at_ns=d["available_at_ns"],
                reference_price=ensure_decimal(d["reference_price"])
                if d.get("reference_price") is not None
                else None,
            )

        return self._wrap("load_trade_plan", _op)

    # ---- Approvals (single-use) ----
    def create_approval(self, approval: Approval) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO approvals(approval_id, user_identity, plan_id, plan_version,"
                    " approved_at_ns, expires_at_ns, consumed_at_ns) VALUES(?,?,?,?,?,?,?)",
                    (
                        approval.approval_id,
                        approval.user_identity,
                        approval.plan_id,
                        approval.plan_version,
                        approval.approved_at_ns,
                        approval.expires_at_ns,
                        approval.consumed_at_ns,
                    ),
                )

        self._wrap("create_approval", _op)

    def consume_approval(
        self, *, approval_id: str, plan_id: str, plan_version: str, now_ns: int
    ) -> Approval:
        """Atomically consume a valid unused approval exactly once.

        Fails if: missing, already consumed, expired, or bound to another plan/version.
        Uses a single UPDATE ... WHERE consumed_at_ns IS NULL transaction so two
        concurrent consumers cannot both succeed.
        """

        def _op() -> Approval:
            with self._tx() as cur:
                cur.execute(
                    "SELECT approval_id, user_identity, plan_id, plan_version,"
                    " approved_at_ns, expires_at_ns, consumed_at_ns"
                    " FROM approvals WHERE approval_id=?",
                    (approval_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise PersistenceError(f"approval not found: {approval_id}")
                if row["consumed_at_ns"] is not None:
                    raise PersistenceError(f"approval already consumed: {approval_id}")
                if row["plan_id"] != plan_id or row["plan_version"] != plan_version:
                    raise PersistenceError(
                        f"approval {approval_id} bound to {row['plan_id']}/{row['plan_version']},"
                        f" not {plan_id}/{plan_version}"
                    )
                if now_ns >= int(row["expires_at_ns"]):
                    raise PersistenceError(f"approval expired: {approval_id}")
                cur.execute(
                    "UPDATE approvals SET consumed_at_ns=? WHERE approval_id=? "
                    "AND consumed_at_ns IS NULL",
                    (now_ns, approval_id),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(
                        f"approval concurrent consumption lost: {approval_id}"
                    )
                return Approval(
                    approval_id=row["approval_id"],
                    user_identity=row["user_identity"],
                    plan_id=row["plan_id"],
                    plan_version=row["plan_version"],
                    approved_at_ns=int(row["approved_at_ns"]),
                    expires_at_ns=int(row["expires_at_ns"]),
                    consumed_at_ns=now_ns,
                )

        return self._wrap("consume_approval", _op)

    # ---- Intent + reservation (atomic) ----
    def create_intent_with_reservation(self, intent: Intent, reservation: Reservation) -> None:
        if intent.intent_id != reservation.intent_id:
            raise PersistenceError("intent/reservation intent_id mismatch")

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO intents(intent_id, position_epoch, plan_id, plan_version,"
                    " client_order_id, writer_epoch, lifecycle, protection_status,"
                    " reconciliation_health, created_at_ns)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        intent.intent_id,
                        intent.position_epoch,
                        intent.plan_id,
                        intent.plan_version,
                        intent.client_order_id,
                        intent.writer_epoch,
                        intent.lifecycle.value,
                        intent.protection_status.value,
                        intent.reconciliation_health.value,
                        intent.created_at_ns,
                    ),
                )
                cur.execute(
                    "INSERT INTO reservations(reservation_id, intent_id, remaining_open_qty,"
                    " normal_loss, stress_loss, notional, beta_adjusted_notional,"
                    " margin, es_contribution, version) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        reservation.reservation_id,
                        reservation.intent_id,
                        canonical_decimal_str(reservation.remaining_open_qty),
                        canonical_decimal_str(reservation.normal_loss),
                        canonical_decimal_str(reservation.stress_loss),
                        canonical_decimal_str(reservation.notional),
                        canonical_decimal_str(reservation.beta_adjusted_notional),
                        canonical_decimal_str(reservation.margin),
                        canonical_decimal_str(reservation.es_contribution),
                        reservation.version,
                    ),
                )

        self._wrap("create_intent_with_reservation", _op)

    def update_intent_lifecycle(
        self,
        intent_id: str,
        lifecycle: LifecycleState,
        protection: ProtectionStatus,
        health: ReconciliationHealth,
    ) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "UPDATE intents SET lifecycle=?, protection_status=?,"
                    " reconciliation_health=? WHERE intent_id=?",
                    (lifecycle.value, protection.value, health.value, intent_id),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(f"intent not found: {intent_id}")

        self._wrap("update_intent_lifecycle", _op)

    def load_unresolved_intents(self) -> list[Intent]:
        def _op() -> list[Intent]:
            cur = self._conn.cursor()
            placeholders = ",".join("?" for _ in TERMINAL_LIFECYCLES) or "?"
            if TERMINAL_LIFECYCLES:
                cur.execute(
                    f"SELECT * FROM intents WHERE lifecycle NOT IN ({placeholders}) ORDER BY created_at_ns",
                    tuple(TERMINAL_LIFECYCLES),
                )
            else:
                cur.execute("SELECT * FROM intents ORDER BY created_at_ns")
            rows = cur.fetchall()
            out: list[Intent] = []
            for r in rows:
                out.append(
                    Intent(
                        intent_id=r["intent_id"],
                        position_epoch=int(r["position_epoch"]),
                        plan_id=r["plan_id"],
                        plan_version=r["plan_version"],
                        client_order_id=r["client_order_id"],
                        writer_epoch=int(r["writer_epoch"]),
                        lifecycle=LifecycleState(r["lifecycle"]),
                        protection_status=ProtectionStatus(r["protection_status"]),
                        reconciliation_health=ReconciliationHealth(r["reconciliation_health"]),
                        created_at_ns=int(r["created_at_ns"]),
                    )
                )
            return out

        return self._wrap("load_unresolved_intents", _op)

    def reservation_totals(self) -> dict[str, Decimal]:
        def _op() -> dict[str, Decimal]:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT remaining_open_qty, normal_loss, stress_loss, notional,"
                " beta_adjusted_notional, margin, es_contribution FROM reservations"
            )
            totals = {
                "remaining_open_qty": Decimal("0"),
                "normal_loss": Decimal("0"),
                "stress_loss": Decimal("0"),
                "notional": Decimal("0"),
                "beta_adjusted_notional": Decimal("0"),
                "margin": Decimal("0"),
                "es_contribution": Decimal("0"),
            }
            for r in cur.fetchall():
                for k in totals:
                    totals[k] += Decimal(str(r[k]))
            return totals

        return self._wrap("reservation_totals", _op)

    # ---- Commands (persist before dispatch) ----
    def persist_command(self, command: Command) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO commands(command_id, intent_id, command_type,"
                    " exact_payload_hash, payload, expected_state_version,"
                    " created_at_ns, send_started_at_ns, outcome)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        command.command_id,
                        command.intent_id,
                        command.command_type.value,
                        command.exact_payload_hash,
                        command.payload,
                        command.expected_state_version,
                        command.created_at_ns,
                        command.send_started_at_ns,
                        command.outcome.value,
                    ),
                )

        self._wrap("persist_command", _op)

    def mark_send_started(self, command_id: str, send_started_at_ns: int) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "UPDATE commands SET send_started_at_ns=? WHERE command_id=?"
                    " AND send_started_at_ns IS NULL",
                    (send_started_at_ns, command_id),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(
                        f"command not found or already sending: {command_id}"
                    )

        self._wrap("mark_send_started", _op)

    def update_command_outcome(self, command_id: str, outcome: CommandOutcome) -> None:
        if not isinstance(outcome, CommandOutcome):
            raise PersistenceError("outcome must be CommandOutcome")

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "UPDATE commands SET outcome=? WHERE command_id=?",
                    (outcome.value, command_id),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(f"command not found: {command_id}")

        self._wrap("update_command_outcome", _op)

    def load_command(self, command_id: str) -> Command:
        def _op() -> Command:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM commands WHERE command_id=?", (command_id,))
            r = cur.fetchone()
            if r is None:
                raise PersistenceError(f"command not found: {command_id}")
            return Command(
                command_id=r["command_id"],
                intent_id=r["intent_id"],
                command_type=CommandType(r["command_type"]),
                exact_payload_hash=r["exact_payload_hash"],
                payload=r["payload"],
                expected_state_version=int(r["expected_state_version"]),
                created_at_ns=int(r["created_at_ns"]),
                send_started_at_ns=int(r["send_started_at_ns"])
                if r["send_started_at_ns"] is not None
                else None,
                outcome=CommandOutcome(r["outcome"]),
            )

        return self._wrap("load_command", _op)

    # ---- Evidence (append-only) ----
    def append_observation(self, obs: Observation) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO observations(observation_id, source, venue_identity,"
                    " source_time_ns, receive_time_ns, raw_hash, request_id,"
                    " query_interval_ns, completeness) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        obs.observation_id,
                        obs.source,
                        obs.venue_identity,
                        obs.source_time_ns,
                        obs.receive_time_ns,
                        obs.raw_hash,
                        obs.request_id,
                        obs.query_interval_ns,
                        obs.completeness,
                    ),
                )

        self._wrap("append_observation", _op)

    def append_protection_observation(self, obs: ProtectionObservation) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO protection_observations(position_epoch, desired_stop_version,"
                    " qty, trigger_basis, stop_price, semantics, evidence_ids_json,"
                    " observed_at_ns) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        obs.position_epoch,
                        obs.desired_stop_version,
                        canonical_decimal_str(obs.qty),
                        obs.trigger_basis,
                        canonical_decimal_str(obs.stop_price),
                        obs.semantics,
                        json.dumps(list(obs.evidence_ids), sort_keys=True),
                        obs.observed_at_ns,
                    ),
                )

        self._wrap("append_protection_observation", _op)

    def append_economic_event(self, ev: EconomicEvent) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO economic_events(venue_transaction_id, account, currency,"
                    " amount, effective_time_ns, received_at_ns, event_type, revision)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (
                        ev.venue_transaction_id,
                        ev.account,
                        ev.currency,
                        canonical_decimal_str(ev.amount),
                        ev.effective_time_ns,
                        ev.received_at_ns,
                        ev.event_type,
                        ev.revision,
                    ),
                )

        self._wrap("append_economic_event", _op)

    def count(self, table: str) -> int:
        allowed = {
            "trade_plans",
            "approvals",
            "intents",
            "commands",
            "reservations",
            "observations",
            "protection_observations",
            "economic_events",
        }
        if table not in allowed:
            raise PersistenceError(f"unknown table: {table}")

        def _op() -> int:
            cur = self._conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row = cur.fetchone()
            return int(row[0])

        return self._wrap(f"count({table})", _op)

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error as exc:
            raise PersistenceError(f"close failed: {exc}") from exc
