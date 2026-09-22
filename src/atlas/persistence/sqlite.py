"""Durable SQLite control journal for ATLAS V1.

Capital-control writes commit before external side effects. Evidence is append-only
except explicit lifecycle/command/reservation state transitions. SQLite is not a
competing OMS; it stores ATLAS intent, risk, approvals and recovery evidence.
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

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Approval, Command, EconomicEvent, Intent, Reservation, make_command
from atlas.domain.money import canonical_decimal_str
from atlas.domain.transitions import validate_lifecycle_transition, validate_outcome_transition

from .migrations import bootstrap, current_version


class PersistenceError(RuntimeError):
    pass


def _query_evidence_from_row(row: sqlite3.Row) -> Any:
    """Reconstruct immutable query evidence for transactional validation."""

    from atlas.runtime.reconciliation_evidence import (
        Completeness,
        QueryScope,
        QueryStatus,
        QueryType,
        ReconciliationQueryEvidence,
    )

    return ReconciliationQueryEvidence(
        row["query_id"],
        QueryType(row["query_type"]),
        QueryScope(row["scope"]),
        row["account"],
        row["instrument"],
        row["requested_interval_start_ns"],
        row["requested_interval_end_ns"],
        tuple(json.loads(row["pagination_cursors_json"])),
        row["pages_observed"],
        row["total_records_returned"],
        Completeness(row["completeness"]),
        QueryStatus(row["status"]),
        row["source_time_ns"],
        row["receipt_time_ns"],
        tuple(json.loads(row["request_ids_json"])),
        tuple(tuple(segment) for segment in json.loads(row["retention_segments_json"])),
        dict(json.loads(row["facts_json"])),
        row["evidence_hash"],
        row["error_message"],
    )


class SQLiteJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._transaction_lock = threading.RLock()
        try:
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            if self._conn.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                raise PersistenceError("WAL unavailable")
            if int(self._conn.execute("PRAGMA synchronous").fetchone()[0]) < 2:
                raise PersistenceError("synchronous=FULL unavailable")
            if int(self._conn.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise PersistenceError("foreign_keys unavailable")
            bootstrap(self._conn)
        except Exception as exc:
            try:
                self._conn.close()
            except Exception:
                pass
            if isinstance(exc, PersistenceError):
                raise
            raise PersistenceError(f"journal open/bootstrap failed: {exc}") from exc

    @property
    def is_open(self) -> bool:
        return not self._closed

    def pragmas(self) -> dict[str, Any]:
        return self._wrap(
            "pragmas",
            lambda: {
                "journal_mode": self._conn.execute("PRAGMA journal_mode").fetchone()[0],
                "synchronous": self._conn.execute("PRAGMA synchronous").fetchone()[0],
                "foreign_keys": self._conn.execute("PRAGMA foreign_keys").fetchone()[0],
            },
        )

    def schema_version(self) -> int | None:
        return self._wrap("schema_version", lambda: current_version(self._conn))

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        with self._transaction_lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _wrap(self, name: str, fn):
        if self._closed:
            raise PersistenceError(f"{name}: journal closed")
        try:
            return fn()
        except PersistenceError:
            raise
        except sqlite3.Error as exc:
            raise PersistenceError(f"{name} failed: {exc}") from exc

    # ---- Plans / approvals / intents / reservations ----
    def create_trade_plan(self, plan: Any) -> None:
        self._wrap("create_trade_plan", lambda: self._insert_plan(plan))

    def _insert_plan(self, plan: Any) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO trade_plans(plan_id,version,canonical_json,plan_hash,expires_at_ns,created_at_ns) VALUES(?,?,?,?,?,?)",
                (
                    plan.plan_id,
                    plan.version,
                    plan.to_canonical_json(),
                    plan.plan_hash(),
                    plan.expires_at_ns,
                    plan.created_at_ns,
                ),
            )

    def load_trade_plan(self, plan_id: str) -> Any:
        from atlas.domain.enums import Side
        from atlas.domain.money import ensure_decimal
        from atlas.domain.trade_plan import TradePlan

        def op():
            r = self._conn.execute("SELECT canonical_json FROM trade_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if not r:
                raise PersistenceError(f"trade plan not found: {plan_id}")
            d = json.loads(r["canonical_json"])
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
                reference_price=ensure_decimal(d["reference_price"]) if d.get("reference_price") is not None else None,
            )

        return self._wrap("load_trade_plan", op)

    def create_approval(self, a: Approval) -> None:
        self._wrap("create_approval", lambda: self._insert_approval(a))

    def _insert_approval(self, a: Approval) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO approvals VALUES(?,?,?,?,?,?,?)",
                (
                    a.approval_id,
                    a.user_identity,
                    a.plan_id,
                    a.plan_version,
                    a.approved_at_ns,
                    a.expires_at_ns,
                    a.consumed_at_ns,
                ),
            )

    def consume_approval(self, *, approval_id: str, plan_id: str, plan_version: str, now_ns: int) -> Approval:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM approvals WHERE approval_id=?", (approval_id,)).fetchone()
                if not r:
                    raise PersistenceError("approval not found")
                if r["consumed_at_ns"] is not None:
                    raise PersistenceError("approval already consumed")
                if r["plan_id"] != plan_id or r["plan_version"] != plan_version:
                    raise PersistenceError(
                        f"approval bound to {r['plan_id']}/{r['plan_version']}, not {plan_id}/{plan_version}"
                    )
                if now_ns >= r["expires_at_ns"]:
                    raise PersistenceError("approval expired")
                c.execute(
                    "UPDATE approvals SET consumed_at_ns=? WHERE approval_id=? AND consumed_at_ns IS NULL",
                    (now_ns, approval_id),
                )
                if c.rowcount != 1:
                    raise PersistenceError("approval concurrent replay")
                return Approval(
                    r["approval_id"],
                    r["user_identity"],
                    r["plan_id"],
                    r["plan_version"],
                    r["approved_at_ns"],
                    r["expires_at_ns"],
                    now_ns,
                )

        return self._wrap("consume_approval", op)

    def consume_approval_with_intent_reservation(
        self,
        *,
        approval_id: str,
        plan_id: str,
        plan_version: str,
        now_ns: int,
        intent: Intent,
        reservation: Reservation,
    ) -> Approval:
        if (
            intent.intent_id != reservation.intent_id
            or intent.plan_id != plan_id
            or intent.plan_version != plan_version
        ):
            raise PersistenceError("intent/approval binding mismatch")

        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM approvals WHERE approval_id=?", (approval_id,)).fetchone()
                if not r:
                    raise PersistenceError("approval not found")
                if r["consumed_at_ns"] is not None:
                    raise PersistenceError("approval already consumed")
                if r["plan_id"] != plan_id or r["plan_version"] != plan_version:
                    raise PersistenceError(
                        f"approval bound to {r['plan_id']}/{r['plan_version']}, not {plan_id}/{plan_version}"
                    )
                if now_ns >= r["expires_at_ns"]:
                    raise PersistenceError("approval expired")
                c.execute(
                    "UPDATE approvals SET consumed_at_ns=? WHERE approval_id=? AND consumed_at_ns IS NULL",
                    (now_ns, approval_id),
                )
                if c.rowcount != 1:
                    raise PersistenceError("approval concurrent consumption lost")
                c.execute(
                    "INSERT INTO intents VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
                        intent.state_version,
                    ),
                )
                c.execute(
                    "INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?,?,?)",
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
                return Approval(
                    r["approval_id"],
                    r["user_identity"],
                    r["plan_id"],
                    r["plan_version"],
                    r["approved_at_ns"],
                    r["expires_at_ns"],
                    now_ns,
                )

        return self._wrap("consume_approval_with_intent_reservation", op)

    def create_intent_with_reservation(self, intent: Intent, res: Reservation) -> None:
        if intent.intent_id != res.intent_id:
            raise PersistenceError("intent/reservation mismatch")

        def op():
            with self._tx() as c:
                c.execute(
                    "INSERT INTO intents VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
                        intent.state_version,
                    ),
                )
                c.execute(
                    "INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        res.reservation_id,
                        res.intent_id,
                        canonical_decimal_str(res.remaining_open_qty),
                        canonical_decimal_str(res.normal_loss),
                        canonical_decimal_str(res.stress_loss),
                        canonical_decimal_str(res.notional),
                        canonical_decimal_str(res.beta_adjusted_notional),
                        canonical_decimal_str(res.margin),
                        canonical_decimal_str(res.es_contribution),
                        res.version,
                    ),
                )

        self._wrap("create_intent_with_reservation", op)

    def _intent_from_row(self, r: sqlite3.Row) -> Intent:
        return Intent(
            r["intent_id"],
            r["position_epoch"],
            r["plan_id"],
            r["plan_version"],
            r["client_order_id"],
            r["writer_epoch"],
            LifecycleState(r["lifecycle"]),
            ProtectionStatus(r["protection_status"]),
            ReconciliationHealth(r["reconciliation_health"]),
            r["created_at_ns"],
            r["state_version"],
        )

    def load_intent(self, intent_id: str) -> Intent:
        def op():
            r = self._conn.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
            if not r:
                raise PersistenceError("intent not found")
            return self._intent_from_row(r)

        return self._wrap("load_intent", op)

    def load_unresolved_intents(self) -> list[Intent]:
        terminal = LifecycleState.CLOSED.value
        return self._wrap(
            "load_unresolved_intents",
            lambda: [
                self._intent_from_row(r)
                for r in self._conn.execute(
                    "SELECT * FROM intents WHERE lifecycle<>? ORDER BY created_at_ns", (terminal,)
                ).fetchall()
            ],
        )

    def update_intent_state(
        self,
        *,
        intent_id: str,
        lifecycle: LifecycleState,
        protection: ProtectionStatus,
        health: ReconciliationHealth,
        expected_version: int,
    ) -> Intent:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
                if not r:
                    raise PersistenceError("intent not found")
                if r["state_version"] != expected_version:
                    raise PersistenceError("stale intent state_version")
                old = LifecycleState(r["lifecycle"])
                if lifecycle != old:
                    try:
                        validate_lifecycle_transition(old, lifecycle)
                    except ValueError as exc:
                        raise PersistenceError(str(exc)) from exc
                c.execute(
                    "UPDATE intents SET lifecycle=?,protection_status=?,reconciliation_health=?,state_version=? WHERE intent_id=? AND state_version=?",
                    (
                        lifecycle.value,
                        protection.value,
                        health.value,
                        expected_version + 1,
                        intent_id,
                        expected_version,
                    ),
                )
                if c.rowcount != 1:
                    raise PersistenceError("intent version conflict")
            return self.load_intent(intent_id)

        return self._wrap("update_intent_state", op)

    def update_intent_lifecycle(
        self, intent_id: str, lifecycle: LifecycleState, protection: ProtectionStatus, health: ReconciliationHealth
    ) -> Intent:
        return self.update_intent_state(
            intent_id=intent_id,
            lifecycle=lifecycle,
            protection=protection,
            health=health,
            expected_version=self.load_intent(intent_id).state_version,
        )

    def load_reservation(self, intent_id: str) -> Reservation:
        def op():
            r = self._conn.execute("SELECT * FROM reservations WHERE intent_id=?", (intent_id,)).fetchone()
            if not r:
                raise PersistenceError("reservation not found")
            return Reservation(
                r["reservation_id"],
                r["intent_id"],
                Decimal(r["remaining_open_qty"]),
                Decimal(r["normal_loss"]),
                Decimal(r["stress_loss"]),
                Decimal(r["notional"]),
                Decimal(r["beta_adjusted_notional"]),
                Decimal(r["margin"]),
                Decimal(r["es_contribution"]),
                r["version"],
            )

        return self._wrap("load_reservation", op)

    def reservation_totals(self) -> dict[str, Decimal]:
        names = (
            "remaining_open_qty",
            "normal_loss",
            "stress_loss",
            "notional",
            "beta_adjusted_notional",
            "margin",
            "es_contribution",
        )
        out = {n: Decimal("0") for n in names}
        for r in self._wrap("reservation_totals", lambda: self._conn.execute("SELECT * FROM reservations").fetchall()):
            for n in names:
                out[n] += Decimal(r[n])
        return out

    def release_reservation(self, *, intent_id: str, certificate_id: str, released_at_ns: int) -> Reservation:
        """Release the full risk vector, preserving an append-only audit event."""
        cert_row = self._conn.execute(
            "SELECT decision,payload_json FROM flat_certificates WHERE certification_id=?", (certificate_id,)
        ).fetchone()
        if not cert_row or cert_row["decision"] != "CERTIFIED_FLAT":
            raise PersistenceError("reservation release requires persisted certified-flat artifact bound to intent")
        reasons = self._flat_authority_reasons(json.loads(cert_row["payload_json"]), expected_intent_id=intent_id)
        if reasons:
            raise PersistenceError(
                "reservation release requires independently verified flat evidence: " + "; ".join(reasons)
            )

        def op():
            with self._tx() as c:
                cert = c.execute(
                    "SELECT decision,intent_id FROM flat_certificates WHERE certification_id=?", (certificate_id,)
                ).fetchone()
                if not cert or cert["decision"] != "CERTIFIED_FLAT" or cert["intent_id"] != intent_id:
                    raise PersistenceError(
                        "reservation release requires persisted certified-flat artifact bound to intent"
                    )
                r = c.execute("SELECT * FROM reservations WHERE intent_id=?", (intent_id,)).fetchone()
                if not r:
                    raise PersistenceError("reservation not found")
                before = {
                    k: r[k]
                    for k in (
                        "remaining_open_qty",
                        "normal_loss",
                        "stress_loss",
                        "notional",
                        "beta_adjusted_notional",
                        "margin",
                        "es_contribution",
                    )
                }
                release_id = f"{intent_id}:{certificate_id}"
                c.execute(
                    "INSERT INTO reservation_release_events VALUES(?,?,?,?,?)",
                    (release_id, intent_id, certificate_id, json.dumps(before, sort_keys=True), released_at_ns),
                )
                c.execute(
                    "UPDATE reservations SET remaining_open_qty='0',normal_loss='0',stress_loss='0',notional='0',beta_adjusted_notional='0',margin='0',es_contribution='0',version=version+1 WHERE intent_id=?",
                    (intent_id,),
                )
            return self.load_reservation(intent_id)

        return self._wrap("release_reservation", op)

    # ---- commands ----
    def persist_command(self, cmd: Command) -> None:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT state_version FROM intents WHERE intent_id=?", (cmd.intent_id,)).fetchone()
                if not r:
                    raise PersistenceError("intent missing")
                if r[0] != cmd.expected_state_version:
                    raise PersistenceError("stale command expected_state_version")
                c.execute(
                    "INSERT INTO commands VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        cmd.command_id,
                        cmd.intent_id,
                        cmd.command_type.value,
                        cmd.exact_payload_hash,
                        cmd.payload,
                        cmd.expected_state_version,
                        cmd.created_at_ns,
                        cmd.send_started_at_ns,
                        cmd.outcome.value,
                    ),
                )

        self._wrap("persist_command", op)

    def prepare_dispatch(
        self,
        *,
        intent_id: str,
        expected_state_version: int,
        expected_reservation_version: int,
        command_id: str,
        command_type: CommandType,
        payload_dict: dict[str, Any],
        created_at_ns: int,
        next_lifecycle: LifecycleState = LifecycleState.SUBMITTING,
    ) -> Command:
        cmd = make_command(
            command_id=command_id,
            intent_id=intent_id,
            command_type=command_type,
            payload_dict=payload_dict,
            expected_state_version=expected_state_version,
            created_at_ns=created_at_ns,
        )

        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
                if not r:
                    raise PersistenceError("intent not found")
                if r["state_version"] != expected_state_version:
                    raise PersistenceError("stale intent version")
                rr = c.execute("SELECT version FROM reservations WHERE intent_id=?", (intent_id,)).fetchone()
                if not rr:
                    raise PersistenceError("reservation missing")
                if rr["version"] != expected_reservation_version:
                    raise PersistenceError("stale reservation version")
                old = LifecycleState(r["lifecycle"])
                if next_lifecycle != old:
                    validate_lifecycle_transition(old, next_lifecycle)
                c.execute(
                    "INSERT INTO commands VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        cmd.command_id,
                        cmd.intent_id,
                        cmd.command_type.value,
                        cmd.exact_payload_hash,
                        cmd.payload,
                        cmd.expected_state_version,
                        cmd.created_at_ns,
                        None,
                        cmd.outcome.value,
                    ),
                )
                c.execute(
                    "UPDATE intents SET lifecycle=?,state_version=? WHERE intent_id=? AND state_version=?",
                    (next_lifecycle.value, expected_state_version + 1, intent_id, expected_state_version),
                )
                if c.rowcount != 1:
                    raise PersistenceError("intent version conflict")
            return cmd

        return self._wrap("prepare_dispatch", op)

    def _command_from_row(self, r: sqlite3.Row) -> Command:
        return Command(
            r["command_id"],
            r["intent_id"],
            CommandType(r["command_type"]),
            r["exact_payload_hash"],
            r["payload"],
            r["expected_state_version"],
            r["created_at_ns"],
            r["send_started_at_ns"],
            CommandOutcome(r["outcome"]),
        )

    def load_command(self, command_id: str) -> Command:
        def op():
            r = self._conn.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
            if not r:
                raise PersistenceError("command not found")
            return self._command_from_row(r)

        return self._wrap("load_command", op)

    def load_commands_for_intent(self, intent_id: str) -> list[Command]:
        return self._wrap(
            "load_commands_for_intent",
            lambda: [
                self._command_from_row(r)
                for r in self._conn.execute(
                    "SELECT * FROM commands WHERE intent_id=? ORDER BY created_at_ns,command_id", (intent_id,)
                ).fetchall()
            ],
        )

    def list_commands_for_intent(self, intent_id: str) -> list[Command]:
        return self.load_commands_for_intent(intent_id)

    def load_unresolved_commands(self) -> list[Command]:
        terminal = (CommandOutcome.DEFINITE_REJECT.value, CommandOutcome.RECONCILED.value)
        return self._wrap(
            "load_unresolved_commands",
            lambda: [
                self._command_from_row(r)
                for r in self._conn.execute(
                    "SELECT * FROM commands WHERE outcome NOT IN (?,?) ORDER BY created_at_ns", terminal
                ).fetchall()
            ],
        )

    def mark_send_started(self, command_id: str, at_ns: int) -> Command:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
                if not r:
                    raise PersistenceError("command not found")
                current = CommandOutcome(r["outcome"])
                validate_outcome_transition(current, CommandOutcome.UNKNOWN)
                if r["send_started_at_ns"] is not None:
                    raise PersistenceError("dispatch already started")
                c.execute(
                    "UPDATE commands SET send_started_at_ns=?,outcome=? WHERE command_id=?",
                    (at_ns, CommandOutcome.UNKNOWN.value, command_id),
                )
            return self.load_command(command_id)

        return self._wrap("mark_send_started", op)

    def update_command_outcome(self, command_id: str, outcome: CommandOutcome) -> Command:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT outcome FROM commands WHERE command_id=?", (command_id,)).fetchone()
                if not r:
                    raise PersistenceError("command missing")
                validate_outcome_transition(CommandOutcome(r[0]), outcome)
                c.execute("UPDATE commands SET outcome=? WHERE command_id=?", (outcome.value, command_id))
            return self.load_command(command_id)

        return self._wrap("update_command_outcome", op)

    # ---- economic/execution/status evidence ----
    def append_economic_event(self, e: EconomicEvent) -> None:
        self._wrap("append_economic_event", lambda: self._append_economic(e))

    def _append_economic(self, e: EconomicEvent):
        with self._tx() as c:
            c.execute(
                "INSERT INTO economic_events VALUES(?,?,?,?,?,?,?,?)",
                (
                    e.account,
                    e.venue_transaction_id,
                    e.currency,
                    canonical_decimal_str(e.amount),
                    e.effective_time_ns,
                    e.received_at_ns,
                    e.event_type,
                    e.revision,
                ),
            )

    def append_execution_evidence(self, fill: Any) -> bool:
        def op():
            with self._tx() as c:
                r = c.execute("SELECT * FROM execution_evidence WHERE execution_id=?", (fill.execution_id,)).fetchone()
                vals = (
                    fill.execution_id,
                    fill.order_id,
                    fill.client_order_id,
                    fill.intent_id,
                    fill.instrument,
                    fill.side,
                    canonical_decimal_str(fill.qty),
                    canonical_decimal_str(fill.price),
                    canonical_decimal_str(fill.fee),
                    fill.fee_currency,
                    fill.trade_time_ns,
                    fill.receive_time_ns,
                    fill.source,
                    fill.raw_hash,
                )
                if r:
                    if (
                        tuple(
                            r[k]
                            for k in (
                                "execution_id",
                                "order_id",
                                "client_order_id",
                                "intent_id",
                                "instrument",
                                "side",
                                "qty",
                                "price",
                                "fee",
                                "fee_currency",
                                "trade_time_ns",
                                "receive_time_ns",
                                "source",
                                "raw_hash",
                            )
                        )
                        != vals
                    ):
                        raise PersistenceError("conflicting execution evidence")
                    return False
                c.execute("INSERT INTO execution_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", vals)
                return True

        return self._wrap("append_execution_evidence", op)

    def load_execution_evidence(self, *, intent_id: str | None = None) -> list[Any]:
        from atlas.runtime.fill_dedup import FillRecord

        def op():
            q = "SELECT * FROM execution_evidence"
            params = ()
            if intent_id is not None:
                q += " WHERE intent_id=?"
                params = (intent_id,)
            q += " ORDER BY trade_time_ns,execution_id"
            return [
                FillRecord(
                    r["execution_id"],
                    r["order_id"],
                    r["client_order_id"],
                    r["intent_id"],
                    r["instrument"],
                    r["side"],
                    Decimal(r["qty"]),
                    Decimal(r["price"]),
                    Decimal(r["fee"]),
                    r["fee_currency"],
                    r["trade_time_ns"],
                    r["receive_time_ns"],
                    r["source"],
                    r["raw_hash"],
                )
                for r in self._conn.execute(q, params).fetchall()
            ]

        return self._wrap("load_execution_evidence", op)

    def append_order_status_observation(self, s: Any) -> None:
        def op():
            with self._tx() as c:
                c.execute(
                    """INSERT INTO order_status_observations(order_id,client_order_id,intent_id,status,cum_exec_qty,cum_exec_fee,cum_exec_value,avg_exec_price,receive_time_ns,source,raw_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        s.order_id,
                        s.client_order_id,
                        s.intent_id,
                        s.status,
                        canonical_decimal_str(s.cum_exec_qty),
                        canonical_decimal_str(s.cum_exec_fee),
                        canonical_decimal_str(s.cum_exec_value),
                        canonical_decimal_str(s.avg_exec_price) if s.avg_exec_price is not None else None,
                        s.receive_time_ns,
                        s.source,
                        s.raw_hash,
                    ),
                )

        self._wrap("append_order_status_observation", op)

    def load_order_status_observations(self, *, intent_id: str | None = None) -> list[Any]:
        from atlas.runtime.fill_dedup import OrderStatusRecord

        def op():
            q = "SELECT * FROM order_status_observations"
            params = ()
            if intent_id is not None:
                q += " WHERE intent_id=?"
                params = (intent_id,)
            q += " ORDER BY observation_id"
            return [
                OrderStatusRecord(
                    r["order_id"],
                    r["client_order_id"],
                    r["intent_id"],
                    r["status"],
                    Decimal(r["cum_exec_qty"]),
                    Decimal(r["cum_exec_fee"]),
                    Decimal(r["cum_exec_value"]),
                    Decimal(r["avg_exec_price"]) if r["avg_exec_price"] is not None else None,
                    r["receive_time_ns"],
                    r["source"],
                    r["raw_hash"],
                )
                for r in self._conn.execute(q, params).fetchall()
            ]

        return self._wrap("load_order_status_observations", op)

    def append_observation(self, obs: Any) -> None:
        self._wrap("append_observation", lambda: self._append_observation(obs))

    def _append_observation(self, obs: Any) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO observations(observation_id,source,venue_identity,source_time_ns,receive_time_ns,raw_hash,request_id,query_interval_ns,completeness) VALUES(?,?,?,?,?,?,?,?,?)",
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

    def append_protection_observation(self, obs: Any) -> None:
        self._wrap("append_protection_observation", lambda: self._append_protection_observation(obs))

    def _append_protection_observation(self, obs: Any) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO protection_observations(position_epoch,desired_stop_version,qty,trigger_basis,stop_price,semantics,evidence_ids_json,observed_at_ns) VALUES(?,?,?,?,?,?,?,?)",
                (
                    obs.position_epoch,
                    obs.desired_stop_version,
                    canonical_decimal_str(obs.qty),
                    obs.trigger_basis,
                    canonical_decimal_str(obs.stop_price),
                    obs.semantics,
                    json.dumps(list(obs.evidence_ids)),
                    obs.observed_at_ns,
                ),
            )

    # ---- reconciliation evidence/run membership ----
    def append_reconciliation_query_evidence(self, e: Any) -> bool:
        def op():
            if not getattr(e, "hash_binds_payload", False):
                raise PersistenceError("reconciliation evidence hash does not bind its immutable payload")
            with self._tx() as c:
                r = c.execute(
                    "SELECT evidence_hash FROM reconciliation_query_evidence WHERE query_id=?", (e.query_id,)
                ).fetchone()
                if r:
                    if r[0] != e.evidence_hash:
                        raise PersistenceError("conflicting reconciliation evidence")
                    return False
                c.execute(
                    """INSERT INTO reconciliation_query_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        e.query_id,
                        e.query_type.value,
                        e.scope.value,
                        e.account,
                        e.instrument,
                        e.requested_interval_start_ns,
                        e.requested_interval_end_ns,
                        json.dumps(list(e.pagination_cursors)),
                        e.pages_observed,
                        e.total_records_returned,
                        e.completeness.value,
                        e.status.value,
                        e.source_time_ns,
                        e.receipt_time_ns,
                        json.dumps(list(e.request_ids)),
                        json.dumps([list(s) for s in e.retention_segments]),
                        json.dumps(e.facts, sort_keys=True),
                        e.evidence_hash,
                        e.error_message,
                    ),
                )
                return True

        return self._wrap("append_reconciliation_query_evidence", op)

    def load_reconciliation_query_evidence(self, query_id: str | None = None) -> list[Any]:
        def op():
            rows = self._conn.execute(
                "SELECT * FROM reconciliation_query_evidence"
                + (" WHERE query_id=?" if query_id is not None else "")
                + " ORDER BY receipt_time_ns,query_id",
                ((query_id,) if query_id is not None else ()),
            ).fetchall()
            if query_id is not None and not rows:
                raise PersistenceError("query evidence missing")
            return [_query_evidence_from_row(r) for r in rows]

        return self._wrap("load_reconciliation_query_evidence", op)

    def create_reconciliation_run(self, run: Any) -> None:
        def op():
            from atlas.runtime.reconciliation_evidence import (
                DEFAULT_EXECUTION_RISK_QUERIES,
                ReconciliationRunState,
            )

            if run.state != ReconciliationRunState.OPEN or run.completed_at_ns is not None:
                raise PersistenceError("reconciliation runs must be created OPEN without completion time")
            missing = set(DEFAULT_EXECUTION_RISK_QUERIES) - set(run.required_query_types)
            if missing:
                names = ", ".join(sorted(item.value for item in missing))
                raise PersistenceError(f"required query set omits mandatory evidence: {names}")
            with self._tx() as c:
                c.execute(
                    "INSERT INTO reconciliation_runs VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        run.run_id,
                        run.account,
                        run.instrument,
                        run.writer_id,
                        run.writer_epoch,
                        run.runtime_instance_id,
                        run.started_at_ns,
                        run.completed_at_ns,
                        json.dumps([q.value for q in run.required_query_types]),
                        run.state.value,
                    ),
                )

        self._wrap("create_reconciliation_run", op)

    def bind_query_to_run(self, run_id: str, query_id: str) -> None:
        # query must exist; binding immutable
        def op():
            with self._tx() as c:
                q = c.execute("SELECT * FROM reconciliation_query_evidence WHERE query_id=?", (query_id,)).fetchone()
                run = c.execute(
                    "SELECT account,instrument,started_at_ns,state FROM reconciliation_runs WHERE run_id=?", (run_id,)
                ).fetchone()
                if not q:
                    raise PersistenceError("query missing")
                if not run:
                    raise PersistenceError("reconciliation run missing")
                evidence = _query_evidence_from_row(q)
                if not evidence.hash_binds_payload:
                    raise PersistenceError("reconciliation evidence hash does not bind its immutable payload")
                if run["state"] != "OPEN":
                    raise PersistenceError("cannot bind evidence to a completed reconciliation run")
                if evidence.receipt_time_ns < run["started_at_ns"]:
                    raise PersistenceError("query evidence was received before reconciliation run started")
                if evidence.account != run["account"]:
                    raise PersistenceError("query account does not match run")
                if evidence.scope.value == "instrument" and evidence.instrument != run["instrument"]:
                    raise PersistenceError("query instrument does not match run")
                try:
                    c.execute("INSERT INTO reconciliation_run_queries VALUES(?,?)", (run_id, query_id))
                except sqlite3.IntegrityError as exc:
                    raise PersistenceError("query evidence already bound to another run") from exc

        self._wrap("bind_query_to_run", op)

    def complete_reconciliation_run(self, run_id: str, completed_at_ns: int) -> None:
        self._wrap("complete_reconciliation_run", lambda: self._complete_run(run_id, completed_at_ns))

    def _complete_run(self, run_id: str, at: int):
        with self._tx() as c:
            from atlas.runtime.reconciliation_evidence import (
                DEFAULT_EXECUTION_RISK_QUERIES,
                HISTORY_COVERAGE_QUERY_TYPES,
                Completeness,
                QueryStatus,
                QueryType,
            )

            row = c.execute("SELECT * FROM reconciliation_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["state"] != "OPEN":
                raise PersistenceError("run missing/not OPEN")
            if at < row["started_at_ns"]:
                raise PersistenceError("reconciliation completion precedes run start")
            query_rows = c.execute(
                """SELECT q.* FROM reconciliation_run_queries rq
                   JOIN reconciliation_query_evidence q ON q.query_id=rq.query_id
                   WHERE rq.run_id=?""",
                (run_id,),
            ).fetchall()
            try:
                evidence_rows = [_query_evidence_from_row(item) for item in query_rows]
            except (TypeError, ValueError, KeyError) as exc:
                raise PersistenceError(f"invalid reconciliation evidence: {exc}") from exc
            for evidence in evidence_rows:
                if not evidence.hash_binds_payload:
                    raise PersistenceError(f"evidence hash is not bound: {evidence.query_id}")
                if not row["started_at_ns"] <= evidence.receipt_time_ns <= at:
                    raise PersistenceError(f"query receipt is outside reconciliation run: {evidence.query_id}")
            by_type = {evidence.query_type: evidence for evidence in evidence_rows}
            required = {QueryType(item) for item in json.loads(row["required_query_types_json"])}
            missing = set(DEFAULT_EXECUTION_RISK_QUERIES) - required
            missing.update(query for query in DEFAULT_EXECUTION_RISK_QUERIES if query not in by_type)
            if missing:
                names = ", ".join(sorted(item.value if hasattr(item, "value") else item for item in missing))
                raise PersistenceError(f"cannot complete reconciliation run; missing evidence: {names}")
            for query_type in DEFAULT_EXECUTION_RISK_QUERIES:
                evidence = by_type[query_type]
                if evidence.status != QueryStatus.SUCCESS or evidence.completeness != Completeness.COMPLETE:
                    raise PersistenceError(f"cannot complete reconciliation run; {query_type.value} is incomplete")
                if query_type in HISTORY_COVERAGE_QUERY_TYPES and not evidence.has_retention_coverage:
                    raise PersistenceError(
                        f"cannot complete reconciliation run; {query_type.value} retention is incomplete"
                    )
            c.execute(
                "UPDATE reconciliation_runs SET completed_at_ns=?,state='COMPLETE' WHERE run_id=? AND state='OPEN'",
                (at, run_id),
            )
            if c.rowcount != 1:
                raise PersistenceError("run missing/not OPEN")

    def load_reconciliation_run(self, run_id: str) -> Any:
        from atlas.runtime.reconciliation_evidence import QueryType, ReconciliationRun, ReconciliationRunState

        def op():
            r = self._conn.execute("SELECT * FROM reconciliation_runs WHERE run_id=?", (run_id,)).fetchone()
            if not r:
                raise PersistenceError("run missing")
            return ReconciliationRun.from_persisted(
                run_id=r["run_id"],
                account=r["account"],
                instrument=r["instrument"],
                writer_id=r["writer_id"],
                writer_epoch=r["writer_epoch"],
                runtime_instance_id=r["runtime_instance_id"],
                started_at_ns=r["started_at_ns"],
                completed_at_ns=r["completed_at_ns"],
                required_query_types=tuple(QueryType(x) for x in json.loads(r["required_query_types_json"])),
                state=ReconciliationRunState(r["state"]),
            )

        return self._wrap("load_reconciliation_run", op)

    def load_run_queries(self, run_id: str) -> tuple[Any, ...]:
        ids = [
            r[0]
            for r in self._wrap(
                "load_run_queries",
                lambda: self._conn.execute(
                    "SELECT query_id FROM reconciliation_run_queries WHERE run_id=? ORDER BY query_id", (run_id,)
                ).fetchall(),
            )
        ]
        return tuple(self.load_reconciliation_query_evidence(i)[0] for i in ids)

    def load_reconciliation_evidence_bundle(
        self,
        *,
        reconciliation_run_id: str,
        account: str,
        instrument: str | None,
        query_ids: tuple[str, ...],
        started_at_ns: int,
        completed_at_ns: int,
    ) -> Any:
        from atlas.runtime.reconciliation_evidence import (
            ReconciliationRun,
            ReconciliationRunState,
            build_reconciliation_bundle,
        )

        if not query_ids:
            raise PersistenceError("reconciliation bundle requires persisted query IDs")
        try:
            queries = tuple(self.load_reconciliation_query_evidence(i)[0] for i in query_ids)
        except PersistenceError as exc:
            raise PersistenceError(f"reconciliation query evidence missing: {exc}") from exc
        if any(e.account != account or e.instrument != instrument for e in queries):
            raise PersistenceError("reconciliation evidence identity does not match requested bundle")
        run = ReconciliationRun.from_persisted(
            run_id=reconciliation_run_id,
            account=account,
            instrument=instrument,
            writer_id="legacy",
            writer_epoch=0,
            runtime_instance_id="legacy-runtime",
            started_at_ns=started_at_ns,
            completed_at_ns=completed_at_ns,
            required_query_types=tuple(e.query_type for e in queries),
            state=ReconciliationRunState.COMPLETE,
        )
        return build_reconciliation_bundle(run, queries)

    # ---- typed protection / flat / recovery artifacts ----
    def append_protection_evidence(self, evidence_id: str, e: Any, raw_hash: str) -> None:
        def op():
            expected_hash = e.expected_evidence_hash()
            if raw_hash != expected_hash:
                raise PersistenceError("protection evidence hash does not bind its immutable payload")
            if e.status.value == "CONFIRMED":
                if e.observed_signed_qty == 0:
                    raise PersistenceError("confirmed protection cannot describe a flat position")
                if not e.full_position_semantics or not e.market_stop_semantics:
                    raise PersistenceError("confirmed protection requires full-position market-stop semantics")
                if e.trigger_basis != "MarkPrice" or not e.closing_only_behavior:
                    raise PersistenceError("confirmed protection semantics are incomplete")
                if not e.position_view_evidence_ids or not e.conditional_order_view_evidence_ids:
                    raise PersistenceError("confirmed protection requires position and conditional evidence references")
                if e.receive_time_ns < e.observation_time_ns:
                    raise PersistenceError("protection receipt precedes source observation")
            with self._tx() as c:
                c.execute(
                    """INSERT INTO protection_evidence(evidence_id,account_ref,instrument,position_epoch,desired_stop_version,observed_signed_qty,full_position_semantics,stop_price,trigger_basis,market_stop_semantics,closing_only_behavior,position_view_evidence_ids_json,conditional_order_view_evidence_ids_json,observation_time_ns,receive_time_ns,status,raw_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        evidence_id,
                        e.account_ref,
                        e.instrument,
                        e.position_epoch,
                        e.desired_stop_version,
                        canonical_decimal_str(e.observed_signed_qty),
                        int(e.full_position_semantics),
                        canonical_decimal_str(e.stop_price),
                        e.trigger_basis,
                        int(e.market_stop_semantics),
                        int(e.closing_only_behavior),
                        json.dumps(list(e.position_view_evidence_ids)),
                        json.dumps(list(e.conditional_order_view_evidence_ids)),
                        e.observation_time_ns,
                        e.receive_time_ns,
                        e.status.value,
                        raw_hash,
                    ),
                )

        self._wrap("append_protection_evidence", op)

    def load_protection_evidence(self, evidence_id: str) -> Any:
        from atlas.runtime.protection_evidence import ProtectionEvidence

        def op():
            r = self._conn.execute("SELECT * FROM protection_evidence WHERE evidence_id=?", (evidence_id,)).fetchone()
            if not r:
                raise PersistenceError("protection evidence missing")
            return ProtectionEvidence(
                r["account_ref"],
                r["instrument"],
                r["position_epoch"],
                r["desired_stop_version"],
                Decimal(r["observed_signed_qty"]),
                bool(r["full_position_semantics"]),
                Decimal(r["stop_price"]),
                r["trigger_basis"],
                bool(r["closing_only_behavior"]),
                tuple(json.loads(r["position_view_evidence_ids_json"])),
                tuple(json.loads(r["conditional_order_view_evidence_ids_json"])),
                r["observation_time_ns"],
                r["receive_time_ns"],
                ProtectionStatus(r["status"]),
                market_stop_semantics=bool(r["market_stop_semantics"]),
            )

        return self._wrap("load_protection_evidence", op)

    def validate_protection_evidence_for_recovery(
        self,
        evidence_id: str,
        *,
        reconciliation_run_id: str,
        account: str,
        instrument: str,
        position_epoch: int,
        now_ns: int,
        max_staleness_ns: int,
    ) -> tuple[bool, tuple[str, ...]]:
        from atlas.runtime.reconciliation_evidence import (
            Completeness,
            QueryStatus,
            QueryType,
            build_reconciliation_bundle,
        )

        try:
            evidence = self.load_protection_evidence(evidence_id)
            run = self.load_reconciliation_run(reconciliation_run_id)
            queries = self.load_run_queries(reconciliation_run_id)
            bundle = build_reconciliation_bundle(run, queries)
        except (PersistenceError, ValueError) as exc:
            return False, (f"cannot load protection authority: {exc}",)
        reasons = []
        if not bundle.complete_for_recovery:
            reasons.append("reconciliation run is incomplete or contains unbound evidence")
        if evidence.status.value != "CONFIRMED":
            reasons.append("protection evidence is not confirmed")
        if (
            not evidence.full_position_semantics
            or not evidence.market_stop_semantics
            or evidence.trigger_basis != "MarkPrice"
            or not evidence.closing_only_behavior
        ):
            reasons.append("protection semantics are incomplete")
        if (
            evidence.account_ref != account
            or evidence.instrument != instrument
            or evidence.position_epoch != position_epoch
        ):
            reasons.append("protection evidence identity mismatch")
        if evidence.observed_signed_qty == 0:
            reasons.append("protection evidence describes a flat position")
        if evidence.receive_time_ns > now_ns or now_ns - evidence.receive_time_ns > max_staleness_ns:
            reasons.append("protection evidence stale or clock-conflicted")
        stored_hash = self._conn.execute(
            "SELECT raw_hash FROM protection_evidence WHERE evidence_id=?", (evidence_id,)
        ).fetchone()
        if stored_hash is None or stored_hash[0] != evidence.expected_evidence_hash():
            reasons.append("protection evidence hash does not bind persisted fields")
        by_id = {query.query_id: query for query in queries}
        positions = by_id.get(next(iter(evidence.position_view_evidence_ids), ""))
        if (
            positions is None
            or positions.query_type != QueryType.POSITIONS
            or positions.status != QueryStatus.SUCCESS
            or positions.completeness != Completeness.COMPLETE
        ):
            reasons.append("position-view evidence is not bound to the current run")
        else:
            try:
                if Decimal(str(positions.facts.get("signed_qty"))) != evidence.observed_signed_qty:
                    reasons.append("protection quantity does not match current position")
                if "position_epoch" not in positions.facts:
                    reasons.append("position-view evidence lacks position epoch")
                elif int(positions.facts["position_epoch"]) != evidence.position_epoch:
                    reasons.append("position-view evidence epoch does not match protection")
            except (ArithmeticError, TypeError, ValueError):
                reasons.append("position-view evidence lacks valid signed quantity or epoch")
        if not evidence.conditional_order_view_evidence_ids:
            reasons.append("conditional-order evidence is missing")
        for ref in evidence.conditional_order_view_evidence_ids:
            query = by_id.get(ref)
            if (
                query is None
                or query.query_type != QueryType.CONDITIONAL_ORDERS
                or query.status != QueryStatus.SUCCESS
                or query.completeness != Completeness.COMPLETE
            ):
                reasons.append("conditional-order evidence is not bound to the current run")
            else:
                facts = query.facts
                if facts.get("native_stop_visible") is not True:
                    reasons.append("conditional/native-stop evidence lacks positive visibility")
                representation = str(facts.get("protection_representation", "")).lower()
                if representation == "conditional_order":
                    order_ids = facts.get("protection_order_ids", ())
                    if not order_ids or query.total_records_returned <= 0:
                        reasons.append("conditional-order evidence lacks a visible protection order")
                elif representation == "position_level_native_stop":
                    if not str(facts.get("native_stop_reference", "")).strip():
                        reasons.append("position-level native-stop evidence lacks a reference")
                else:
                    reasons.append("conditional/native-stop representation is not positively identified")
        trading_stop = next((query for query in queries if query.query_type == QueryType.TRADING_STOP), None)
        if (
            trading_stop is None
            or trading_stop.status != QueryStatus.SUCCESS
            or trading_stop.completeness != Completeness.COMPLETE
        ):
            reasons.append("native protection query is incomplete")
        else:
            expected = {
                "position_epoch": evidence.position_epoch,
                "signed_qty": str(evidence.observed_signed_qty),
                "desired_stop_version": evidence.desired_stop_version,
                "stop_price": str(evidence.stop_price),
                "trigger_basis": evidence.trigger_basis,
                "market_stop_semantics": True,
                "full_position_semantics": True,
                "closing_only_behavior": True,
                "native_stop_visible": True,
            }
            for key, value in expected.items():
                if key not in trading_stop.facts:
                    reasons.append(f"native protection fact missing: {key}")
                else:
                    actual = trading_stop.facts[key]
                    try:
                        if key in {"signed_qty", "stop_price"}:
                            mismatch = Decimal(str(actual)) != Decimal(str(value))
                        elif key in {"position_epoch", "desired_stop_version"}:
                            mismatch = int(actual) != int(value)
                        else:
                            mismatch = str(actual).lower() != str(value).lower()
                    except (ArithmeticError, TypeError, ValueError):
                        mismatch = True
                    if mismatch:
                        reasons.append(f"native protection fact mismatch: {key}")
        return not reasons, tuple(reasons)

    def _flat_authority_reasons(self, data: dict[str, Any], *, expected_intent_id: str | None = None) -> list[str]:
        """Derive release authority from current durable evidence."""
        from atlas.domain.enums import CommandOutcome
        from atlas.runtime.reconciliation_evidence import (
            Completeness,
            QueryStatus,
            QueryType,
            build_reconciliation_bundle,
        )

        reasons = []
        if data.get("decision") != "CERTIFIED_FLAT":
            reasons.append("flat certificate is not certified")
        if not data.get("derived_from_journal", False):
            reasons.append("flat certificate was not derived by the journal verifier")
        intent_id = data.get("intent_id")
        if expected_intent_id is not None and intent_id != expected_intent_id:
            reasons.append("flat certificate intent mismatch")
        if not intent_id:
            reasons.append("flat certificate has no intent")
        try:
            run = self.load_reconciliation_run(data["reconciliation_run_id"])
            queries = self.load_run_queries(data["reconciliation_run_id"])
            bundle = build_reconciliation_bundle(run, queries)
        except (KeyError, PersistenceError, ValueError) as exc:
            return [f"cannot load reconciliation evidence: {exc}"]
        if not bundle.complete_for_recovery:
            reasons.append("reconciliation run is not complete and hash-bound")
        if run.account != data.get("account_identity_hash"):
            reasons.append("flat certificate account mismatch")
        if run.instrument != data.get("instrument"):
            reasons.append("flat certificate instrument mismatch")
        if run.writer_id != data.get("writer_id") or run.writer_epoch != int(data.get("writer_epoch", -1)):
            reasons.append("flat certificate writer mismatch")
        by_type = {query.query_type: query for query in queries}
        positions = by_type.get(QueryType.POSITIONS)
        if positions is None:
            reasons.append("position evidence is missing")
        else:
            try:
                if Decimal(str(positions.facts.get("signed_qty"))) != 0:
                    reasons.append("current signed position is non-zero")
            except (ArithmeticError, TypeError, ValueError):
                reasons.append("position evidence lacks a valid signed quantity")
            if positions.completeness != Completeness.COMPLETE or positions.status != QueryStatus.SUCCESS:
                reasons.append("position evidence is incomplete")
            if "position_epoch" not in positions.facts:
                reasons.append("position evidence lacks position epoch")
            else:
                try:
                    if int(data.get("position_epoch", -1)) != int(positions.facts["position_epoch"]):
                        reasons.append("position epoch evidence mismatch")
                except (TypeError, ValueError):
                    reasons.append("position evidence has invalid position epoch")
        residual_keys = (
            "remaining_open_qty",
            "remaining_open_orders",
            "residual_open_orders",
            "residual_conditional_orders",
        )
        for query_type, label in (
            (QueryType.OPEN_ORDERS, "opening orders"),
            (QueryType.CONDITIONAL_ORDERS, "conditional/protection orders"),
        ):
            query = by_type.get(query_type)
            if query is None or not query.can_certify_absence:
                reasons.append(f"cannot certify absence of {label}")
            elif any(
                str(query.facts[key]) not in {"0", "0.0", "0.00", "false", "False"}
                for key in residual_keys
                if key in query.facts
            ):
                reasons.append(f"{label} evidence reports residual risk")
        for query_type in (
            QueryType.ORDER_HISTORY,
            QueryType.EXECUTION_HISTORY,
            QueryType.WALLET_BALANCE,
            QueryType.TRADING_STOP,
        ):
            query = by_type.get(query_type)
            if query is None or query.status != QueryStatus.SUCCESS or query.completeness != Completeness.COMPLETE:
                reasons.append(f"{query_type.value} evidence is incomplete")
            elif not query.has_retention_coverage:
                reasons.append(f"{query_type.value} retention window is not covered")
        if intent_id:
            try:
                commands = self.load_commands_for_intent(intent_id)
            except PersistenceError as exc:
                reasons.append(str(exc))
                commands = []
            opening = [command for command in commands if command.command_type.value == "SUBMIT_ENTRY"]
            if not opening:
                reasons.append("no opening command evidence for intent")
            if any(
                command.outcome not in (CommandOutcome.DEFINITE_REJECT, CommandOutcome.RECONCILED)
                for command in opening
            ):
                reasons.append("opening command remains unresolved")
            required_refs = {query.evidence_hash for query in queries} | {command.command_id for command in opening}
            if not required_refs.issubset(set(data.get("evidence_refs", ()))):
                reasons.append("flat certificate does not bind all persisted evidence")
            execution_ids = [fill.execution_id for fill in self.load_execution_evidence(intent_id=intent_id)]
            if len(execution_ids) != len(set(execution_ids)):
                reasons.append("execution evidence is not deduplicated")
        if any(
            incident.category == "late_contradictory_evidence" and incident.status == "RECOVERY_REQUIRED"
            for incident in self.load_recovery_incidents(run.run_id)
        ):
            reasons.append("late contradictory evidence reopened recovery")
        return reasons

    def validate_flat_certificate_for_recovery(
        self,
        certification_id: str,
        *,
        reconciliation_run_id: str,
        account: str,
        instrument: str,
        position_epoch: int,
        writer_id: str,
        writer_epoch: int,
    ) -> tuple[bool, tuple[str, ...]]:
        payload = self.load_flat_certificate_payload(certification_id)
        if payload is None:
            return False, ("flat certificate reference does not exist",)
        reasons = self._flat_authority_reasons(payload)
        if (
            payload.get("reconciliation_run_id") != reconciliation_run_id
            or payload.get("account_identity_hash") != account
            or payload.get("instrument") != instrument
            or int(payload.get("position_epoch", -1)) != position_epoch
            or payload.get("writer_id") != writer_id
            or int(payload.get("writer_epoch", -1)) != writer_epoch
        ):
            reasons.append("flat certificate identity mismatch")
        return not reasons, tuple(reasons)

    def append_flat_certificate(self, cert: Any) -> None:
        payload = cert.to_dict()
        refs = cert.evidence_refs

        def op():
            if cert.decision.value == "CERTIFIED_FLAT":
                reasons = self._flat_authority_reasons(payload)
                if reasons:
                    raise PersistenceError("flat certificate cannot authorize release: " + "; ".join(reasons))
            with self._tx() as c:
                c.execute(
                    "INSERT INTO flat_certificates VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        cert.certification_id,
                        cert.reconciliation_run_id,
                        cert.intent_id,
                        cert.writer_id,
                        cert.writer_epoch,
                        cert.account_identity_hash,
                        cert.instrument,
                        cert.position_epoch,
                        cert.decision.value,
                        cert.certified_at_ns,
                        json.dumps(list(refs)),
                        json.dumps(payload, sort_keys=True),
                    ),
                )

        self._wrap("append_flat_certificate", op)

    def load_flat_certificate_payload(self, certification_id: str) -> dict[str, Any] | None:
        def op():
            r = self._conn.execute(
                "SELECT payload_json FROM flat_certificates WHERE certification_id=?", (certification_id,)
            ).fetchone()
            return json.loads(r[0]) if r else None

        return self._wrap("load_flat_certificate_payload", op)

    def append_recovery_incident(self, incident: Any) -> None:
        def op():
            with self._tx() as c:
                c.execute(
                    "INSERT INTO recovery_incidents VALUES(?,?,?,?,?,?,?)",
                    (
                        incident.incident_id,
                        incident.recovery_run_id,
                        incident.category,
                        incident.status,
                        json.dumps(list(incident.evidence_refs)),
                        incident.opened_at_ns,
                        incident.resolved_at_ns,
                    ),
                )

        self._wrap("append_recovery_incident", op)

    def append_recovery_certificate(self, cert: Any) -> None:
        compatibility = {
            "compatibility_metadata": dict(getattr(cert, "compatibility_metadata", {}) or {}),
            "protection_uncertainty_summary": getattr(cert, "protection_uncertainty_summary", ""),
            "venue_observations_obtained": bool(getattr(cert, "venue_observations_obtained", True)),
            "protection_evidence": None
            if getattr(cert, "protection_evidence", None) is None
            else {
                "certified_flat": cert.protection_evidence.certified_flat,
                "current_protection": cert.protection_evidence.current_protection,
                "evidence_refs": list(cert.protection_evidence.evidence_refs),
            },
        }

        def op():
            with self._tx() as c:
                c.execute(
                    """INSERT INTO recovery_certificates(recovery_run_id,runtime_instance_id,writer_id,writer_epoch,journal_schema_version,unresolved_intents_json,unresolved_commands_json,unknown_commands_json,reconciliation_health,started_at_ns,ended_at_ns,evidence_refs_json,venue_evidence_refs_json,decision,flat_certificate_id,protection_evidence_id,compatibility_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        cert.recovery_run_id,
                        cert.runtime_instance_id,
                        cert.writer_id,
                        cert.writer_epoch,
                        cert.journal_schema_version,
                        json.dumps(list(cert.unresolved_intents)),
                        json.dumps(list(cert.unresolved_commands)),
                        json.dumps(list(cert.unknown_commands)),
                        cert.reconciliation_health.value,
                        cert.started_at_ns,
                        cert.ended_at_ns,
                        json.dumps(list(cert.evidence_refs)),
                        json.dumps(list(cert.venue_evidence_refs)),
                        cert.decision.value,
                        cert.flat_certificate_id,
                        cert.protection_evidence_id,
                        json.dumps(compatibility, sort_keys=True),
                    ),
                )

        self._wrap("append_recovery_certificate", op)

    def load_recovery_certificate(self, recovery_run_id: str) -> Any | None:
        from atlas.runtime.recovery import RecoveryCertificate, RecoveryDecision, RecoveryProtectionEvidence

        def op():
            r = self._conn.execute(
                "SELECT * FROM recovery_certificates WHERE recovery_run_id=?", (recovery_run_id,)
            ).fetchone()
            if not r:
                return None
            meta = json.loads(r["compatibility_json"]) if "compatibility_json" in r.keys() else {}  # noqa: SIM118 - sqlite3.Row exposes column names via keys()
            pe = meta.get("protection_evidence")
            compatibility_metadata = meta.get("compatibility_metadata")
            if compatibility_metadata is None and meta.get("legacy_schema"):
                compatibility_metadata = meta
            return RecoveryCertificate(
                recovery_run_id=r["recovery_run_id"],
                runtime_instance_id=r["runtime_instance_id"],
                writer_id=r["writer_id"],
                writer_epoch=int(r["writer_epoch"]),
                journal_schema_version=int(r["journal_schema_version"]),
                unresolved_intents=tuple(json.loads(r["unresolved_intents_json"])),
                unresolved_commands=tuple(json.loads(r["unresolved_commands_json"])),
                unknown_commands=tuple(json.loads(r["unknown_commands_json"])),
                reconciliation_health=ReconciliationHealth(r["reconciliation_health"]),
                started_at_ns=int(r["started_at_ns"]),
                ended_at_ns=int(r["ended_at_ns"]),
                evidence_refs=tuple(json.loads(r["evidence_refs_json"])),
                venue_evidence_refs=tuple(json.loads(r["venue_evidence_refs_json"])),
                decision=RecoveryDecision(r["decision"]),
                flat_certificate_id=r["flat_certificate_id"],
                protection_evidence_id=r["protection_evidence_id"],
                protection_uncertainty_summary=meta.get("protection_uncertainty_summary", ""),
                venue_observations_obtained=bool(meta.get("venue_observations_obtained", True)),
                protection_evidence=RecoveryProtectionEvidence(
                    bool(pe["certified_flat"]), bool(pe["current_protection"]), tuple(pe["evidence_refs"])
                )
                if pe
                else None,
                compatibility_metadata=compatibility_metadata or {},
            )

        return self._wrap("load_recovery_certificate", op)

    def load_recovery_incidents(self, recovery_run_id: str | None = None) -> list[Any]:
        from atlas.runtime.recovery import RecoveryIncident

        def op():
            rows = self._conn.execute(
                "SELECT * FROM recovery_incidents"
                + (" WHERE recovery_run_id=?" if recovery_run_id is not None else "")
                + " ORDER BY opened_at_ns,incident_id",
                ((recovery_run_id,) if recovery_run_id is not None else ()),
            ).fetchall()
            return [
                RecoveryIncident(
                    r["incident_id"],
                    r["recovery_run_id"],
                    r["category"],
                    r["status"],
                    tuple(json.loads(r["evidence_refs_json"])),
                    r["opened_at_ns"],
                    r["resolved_at_ns"],
                )
                for r in rows
            ]

        return self._wrap("load_recovery_incidents", op)

    # ---- capability evidence ----
    def append_capability_evidence(self, e: Any) -> None:
        self._wrap("append_capability_evidence", lambda: self._append_cap(e))

    def _append_cap(self, e: Any):
        with self._tx() as c:
            c.execute(
                "INSERT INTO capability_evidence_log(capability_name,state,test_run_id,evidence_refs_json,test_timestamp_ns,environment,notes,target_profile_hash) VALUES(?,?,?,?,?,?,?,?)",
                (
                    e.capability_name,
                    e.state.value,
                    e.test_run_id,
                    json.dumps(list(e.evidence_refs)),
                    e.test_timestamp_ns,
                    e.environment,
                    e.notes,
                    e.target_profile_hash,
                ),
            )

    def append_capability_qualification(self, r: Any) -> None:
        def op():
            with self._tx() as c:
                c.execute(
                    "INSERT INTO capability_qualification_log VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        r.qualification_id,
                        r.capability_name,
                        r.previous_state.value,
                        r.new_state.value,
                        r.test_run_id,
                        json.dumps(list(r.evidence_refs)),
                        r.qualified_by,
                        r.qualified_at_ns,
                        r.target_profile_hash,
                    ),
                )

        self._wrap("append_capability_qualification", op)

    def load_latest_capability_evidence(self) -> dict[str, Any]:
        from atlas.runtime.capability_ledger import CapabilityEvidence, EvidenceState

        def op():
            rows = self._conn.execute(
                """SELECT e.* FROM capability_evidence_log e JOIN (SELECT capability_name,MAX(evidence_id) AS latest_id FROM capability_evidence_log GROUP BY capability_name) latest ON latest.latest_id=e.evidence_id"""
            ).fetchall()
            return {
                r["capability_name"]: CapabilityEvidence(
                    r["capability_name"],
                    EvidenceState(r["state"]),
                    r["test_run_id"],
                    tuple(json.loads(r["evidence_refs_json"])),
                    r["test_timestamp_ns"],
                    r["environment"],
                    r["notes"],
                    r["target_profile_hash"],
                )
                for r in rows
            }

        return self._wrap("load_latest_capability_evidence", op)

    def release_reservation_from_flat_certificate(self, certificate: Any) -> Reservation:
        self.append_flat_certificate(certificate)
        if not getattr(certificate, "can_release_reservation", False):
            raise PersistenceError("flat certificate is not eligible for reservation release")
        if certificate.intent_id is None:
            raise PersistenceError("flat certificate has no intent")
        return self.release_reservation(
            intent_id=certificate.intent_id,
            certificate_id=certificate.certification_id,
            released_at_ns=certificate.certified_at_ns,
        )

    def count(self, table: str) -> int:
        allowed = {r[0] for r in self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if table not in allowed:
            raise PersistenceError("unknown table")
        return int(self._wrap("count", lambda: self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]))

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._conn.close()
            self._closed = True
        except sqlite3.Error as exc:
            raise PersistenceError(f"close failed: {exc}") from exc
