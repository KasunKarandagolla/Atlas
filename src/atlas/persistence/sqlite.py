"""SQLite WAL durable journal with single-writer abstraction (freeze §1.4).

- WAL mode, synchronous=FULL, foreign_keys=ON with fail-closed verification.
- stdlib sqlite3 with explicit SQL (no ORM).
- Transactional writes; persistence failure raises PersistenceError (never success).
- Unique client-order-ID constraint enforced by schema.
- Schema v4: durable execution/status/query evidence and typed recovery certificates.
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
from atlas.domain.transitions import (
    validate_lifecycle_transition,
    validate_outcome_transition,
)
from atlas.runtime.capability_ledger import CapabilityEvidence, EvidenceState, QualificationRecord
from atlas.runtime.fill_dedup import FillRecord, OrderStatusRecord
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryStatus,
    QueryType,
    ReconciliationEvidenceBundle,
    ReconciliationQueryEvidence,
)
from atlas.runtime.recovery import (
    RecoveryCertificate,
    RecoveryDecision,
    RecoveryIncident,
    RecoveryProtectionEvidence,
)

from .migrations import bootstrap, current_version

TERMINAL_LIFECYCLES = frozenset({LifecycleState.CLOSED.value})


class PersistenceError(RuntimeError):
    """Explicit persistence failure. MUST NOT be treated as success."""


class SQLiteJournal:
    """Single-writer journal. One instance owns the connection for its path."""

    def __init__(self, path: str | Path, *, timeout: float = 10.0) -> None:
        self._path = str(path)
        self._lock = threading.Lock()
        self._closed = False
        self._conn: sqlite3.Connection
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                self._path, timeout=timeout, isolation_level=None, check_same_thread=False
            )
            self._conn = conn
            self._conn.row_factory = sqlite3.Row
            self._apply_pragmas()
            self._verify_pragmas_fail_closed()
            bootstrap(self._conn)
        except PersistenceError:
            if conn is not None:
                conn.close()
            raise
        except sqlite3.Error as exc:
            if conn is not None:
                conn.close()
            raise PersistenceError(f"failed to open journal at {self._path}: {exc}") from exc

    def _apply_pragmas(self) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=FULL;")
            cur.execute("PRAGMA foreign_keys=ON;")
        except sqlite3.Error as exc:
            raise PersistenceError(f"pragma setup failed: {exc}") from exc

    def _verify_pragmas_fail_closed(self) -> None:
        """Read pragmas back; fail with PersistenceError if required mode inactive."""
        cur = self._conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode;")
            journal_mode = str(cur.fetchone()[0]).upper()
            cur.execute("PRAGMA synchronous;")
            synchronous = str(cur.fetchone()[0]).upper()
            cur.execute("PRAGMA foreign_keys;")
            foreign_keys = str(cur.fetchone()[0]).upper()
        except sqlite3.Error as exc:
            raise PersistenceError(f"pragma verification failed: {exc}") from exc
        # synchronous=FULL reads back as 2 (or 'FULL' on some builds).
        if journal_mode != "WAL":
            raise PersistenceError(f"journal_mode must be WAL, got {journal_mode!r}")
        if synchronous not in ("2", "FULL"):
            raise PersistenceError(f"synchronous must be FULL, got {synchronous!r}")
        if foreign_keys not in ("1", "ON"):
            raise PersistenceError(f"foreign_keys must be ON, got {foreign_keys!r}")

    def pragmas(self) -> dict[str, Any]:
        if self._closed:
            raise PersistenceError("journal is closed")
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
        if self._closed:
            raise PersistenceError("journal is closed")
        try:
            return current_version(self._conn)
        except sqlite3.Error as exc:
            raise PersistenceError(f"schema version read failed: {exc}") from exc

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        if self._closed:
            raise PersistenceError("journal is closed")
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
        """Atomically consume a valid unused approval exactly once."""

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
                    " reconciliation_health, created_at_ns, state_version)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
        """FIX5: atomic approval consumption + intent + client-ID + reservation.

        Single transaction: verify approval (exists/unused/unexpired/matching),
        consume it, insert intent, insert reservation. Any failure rolls back ALL
        effects (approval stays unused, no intent, no reservation).
        """
        if intent.intent_id != reservation.intent_id:
            raise PersistenceError("intent/reservation intent_id mismatch")
        if intent.plan_id != plan_id or intent.plan_version != plan_version:
            raise PersistenceError("intent plan binding does not match approval plan")

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
                cur.execute(
                    "INSERT INTO intents(intent_id, position_epoch, plan_id, plan_version,"
                    " client_order_id, writer_epoch, lifecycle, protection_status,"
                    " reconciliation_health, created_at_ns, state_version)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
                return Approval(
                    approval_id=row["approval_id"],
                    user_identity=row["user_identity"],
                    plan_id=row["plan_id"],
                    plan_version=row["plan_version"],
                    approved_at_ns=int(row["approved_at_ns"]),
                    expires_at_ns=int(row["expires_at_ns"]),
                    consumed_at_ns=now_ns,
                )

        return self._wrap("consume_approval_with_intent_reservation", _op)

    def load_intent(self, intent_id: str) -> Intent:
        def _op() -> Intent:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,))
            r = cur.fetchone()
            if r is None:
                raise PersistenceError(f"intent not found: {intent_id}")
            cols = set(r.keys())
            sv = int(r["state_version"]) if "state_version" in cols else 0
            return Intent(
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
                state_version=sv,
            )

        return self._wrap("load_intent", _op)

    def update_intent_state(
        self,
        *,
        intent_id: str,
        lifecycle: LifecycleState,
        protection: ProtectionStatus,
        health: ReconciliationHealth,
        expected_version: int,
    ) -> Intent:
        """Advance intent state exactly once: validates transition + version.

        Rejects stale expected_version, version decrease, and illegal lifecycle
        transitions (frozen §1.5). Protection/reconciliation stay independent.
        """

        def _op() -> Intent:
            with self._tx() as cur:
                cur.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,))
                r = cur.fetchone()
                if r is None:
                    raise PersistenceError(f"intent not found: {intent_id}")
                current_version = int(r["state_version"])
                if expected_version != current_version:
                    raise PersistenceError(
                        f"stale expected version: expected {expected_version},"
                        f" current {current_version}"
                    )
                current_lifecycle = LifecycleState(r["lifecycle"])
                if lifecycle != current_lifecycle:
                    try:
                        validate_lifecycle_transition(current_lifecycle, lifecycle)
                    except ValueError as exc:
                        raise PersistenceError(str(exc)) from exc
                new_version = current_version + 1
                cur.execute(
                    "UPDATE intents SET lifecycle=?, protection_status=?,"
                    " reconciliation_health=?, state_version=? WHERE intent_id=?"
                    " AND state_version=?",
                    (
                        lifecycle.value,
                        protection.value,
                        health.value,
                        new_version,
                        intent_id,
                        current_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(
                        f"intent concurrent version conflict: {intent_id}"
                    )
                return Intent(
                    intent_id=r["intent_id"],
                    position_epoch=int(r["position_epoch"]),
                    plan_id=r["plan_id"],
                    plan_version=r["plan_version"],
                    client_order_id=r["client_order_id"],
                    writer_epoch=int(r["writer_epoch"]),
                    lifecycle=lifecycle,
                    protection_status=protection,
                    reconciliation_health=health,
                    created_at_ns=int(r["created_at_ns"]),
                    state_version=new_version,
                )

        return self._wrap("update_intent_state", _op)

    def update_intent_lifecycle(
        self,
        intent_id: str,
        lifecycle: LifecycleState,
        protection: ProtectionStatus,
        health: ReconciliationHealth,
    ) -> None:
        """Legacy helper: loads current version then advances once (non-atomic read).

        Prefer update_intent_state with explicit expected_version for runtime paths.
        """
        current = self.load_intent(intent_id)
        self.update_intent_state(
            intent_id=intent_id,
            lifecycle=lifecycle,
            protection=protection,
            health=health,
            expected_version=current.state_version,
        )

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
                cols = set(r.keys())
                sv = int(r["state_version"]) if "state_version" in cols else 0
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
                        state_version=sv,
                    )
                )
            return out

        return self._wrap("load_unresolved_intents", _op)

    def load_reservation(self, intent_id: str) -> Reservation:
        def _op() -> Reservation:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM reservations WHERE intent_id=?", (intent_id,))
            r = cur.fetchone()
            if r is None:
                raise PersistenceError(f"reservation not found for intent: {intent_id}")
            return Reservation(
                reservation_id=r["reservation_id"],
                intent_id=r["intent_id"],
                remaining_open_qty=Decimal(str(r["remaining_open_qty"])),
                normal_loss=Decimal(str(r["normal_loss"])),
                stress_loss=Decimal(str(r["stress_loss"])),
                notional=Decimal(str(r["notional"])),
                beta_adjusted_notional=Decimal(str(r["beta_adjusted_notional"])),
                margin=Decimal(str(r["margin"])),
                es_contribution=Decimal(str(r["es_contribution"])),
                version=int(r["version"]),
            )

        return self._wrap("load_reservation", _op)

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
        """Persist command; rejects stale expected_state_version vs intent version."""

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "SELECT state_version FROM intents WHERE intent_id=?",
                    (command.intent_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise PersistenceError(f"intent not found: {command.intent_id}")
                current = int(row["state_version"])
                if command.expected_state_version != current:
                    raise PersistenceError(
                        f"stale command expected version: got {command.expected_state_version},"
                        f" intent at {current}"
                    )
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
        """FIX6: atomic runtime preparation before transport.

        Single transaction: loads intent + version, verifies reservation version,
        verifies command version binding, persists exact payload+SHA256, advances
        intent lifecycle/version. Commits before transport is called. Never
        releases reservation (UNKNOWN/CANCEL_PENDING/timeout keep full exposure).
        """
        import hashlib as _hashlib
        import json as _json

        payload = _json.dumps(payload_dict, sort_keys=True, separators=(",", ":"))
        payload_hash = _hashlib.sha256(payload.encode("utf-8")).hexdigest()

        def _op() -> Command:
            with self._tx() as cur:
                cur.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,))
                r = cur.fetchone()
                if r is None:
                    raise PersistenceError(f"intent not found: {intent_id}")
                current = int(r["state_version"])
                if expected_state_version != current:
                    raise PersistenceError(
                        f"stale intent version: expected {expected_state_version},"
                        f" current {current}"
                    )
                cur.execute(
                    "SELECT version FROM reservations WHERE intent_id=?", (intent_id,)
                )
                res = cur.fetchone()
                if res is None:
                    raise PersistenceError(f"reservation missing for intent {intent_id}")
                if int(res["version"]) != expected_reservation_version:
                    raise PersistenceError(
                        f"stale reservation version: expected {expected_reservation_version},"
                        f" got {int(res['version'])}"
                    )
                current_lifecycle = LifecycleState(r["lifecycle"])
                if next_lifecycle != current_lifecycle:
                    try:
                        validate_lifecycle_transition(current_lifecycle, next_lifecycle)
                    except ValueError as exc:
                        raise PersistenceError(str(exc)) from exc
                new_version = current + 1
                cmd = Command(
                    command_id=command_id,
                    intent_id=intent_id,
                    command_type=command_type,
                    exact_payload_hash=payload_hash,
                    payload=payload,
                    expected_state_version=expected_state_version,
                    created_at_ns=created_at_ns,
                    send_started_at_ns=None,
                    outcome=CommandOutcome.UNSENT,
                )
                cur.execute(
                    "INSERT INTO commands(command_id, intent_id, command_type,"
                    " exact_payload_hash, payload, expected_state_version,"
                    " created_at_ns, send_started_at_ns, outcome)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
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
                cur.execute(
                    "UPDATE intents SET lifecycle=?, state_version=? WHERE intent_id=?"
                    " AND state_version=?",
                    (next_lifecycle.value, new_version, intent_id, current),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(f"intent version conflict: {intent_id}")
                return cmd

        return self._wrap("prepare_dispatch", _op)

    def mark_send_started(self, command_id: str, send_started_at_ns: int) -> None:
        """FIX1: dispatch marker atomically moves UNSENT -> UNKNOWN.

        Single transaction verifies eligibility (UNSENT + no marker), records the
        timestamp, and flips outcome to UNKNOWN. A crash after commit recovers as
        UNKNOWN without any second manual operation.
        """

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "SELECT outcome, send_started_at_ns, created_at_ns FROM commands"
                    " WHERE command_id=?",
                    (command_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise PersistenceError(f"command not found: {command_id}")
                if row["send_started_at_ns"] is not None:
                    raise PersistenceError(f"dispatch already started: {command_id}")
                if row["outcome"] != CommandOutcome.UNSENT.value:
                    raise PersistenceError(
                        f"command {command_id} not eligible for first dispatch:"
                        f" outcome={row['outcome']}"
                    )
                if send_started_at_ns < int(row["created_at_ns"]):
                    raise PersistenceError("send_started_at cannot precede created_at")
                cur.execute(
                    "UPDATE commands SET send_started_at_ns=?, outcome=? WHERE command_id=?"
                    " AND send_started_at_ns IS NULL AND outcome=?",
                    (
                        send_started_at_ns,
                        CommandOutcome.UNKNOWN.value,
                        command_id,
                        CommandOutcome.UNSENT.value,
                    ),
                )
                if cur.rowcount != 1:
                    raise PersistenceError(
                        f"concurrent dispatch marker lost: {command_id}"
                    )

        self._wrap("mark_send_started", _op)

    def update_command_outcome(self, command_id: str, outcome: CommandOutcome) -> None:
        """FIX4: enforce allowed outcome transitions; terminal states never regress."""
        if not isinstance(outcome, CommandOutcome):
            raise PersistenceError("outcome must be CommandOutcome")

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "SELECT outcome FROM commands WHERE command_id=?", (command_id,)
                )
                row = cur.fetchone()
                if row is None:
                    raise PersistenceError(f"command not found: {command_id}")
                current = CommandOutcome(row["outcome"])
                try:
                    validate_outcome_transition(current, outcome)
                except ValueError as exc:
                    raise PersistenceError(str(exc)) from exc
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

    def list_commands_for_intent(self, intent_id: str) -> list[Command]:
        def _op() -> list[Command]:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM commands WHERE intent_id=? ORDER BY created_at_ns",
                (intent_id,),
            )
            out: list[Command] = []
            for r in cur.fetchall():
                out.append(
                    Command(
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
                )
            return out

        return self._wrap("list_commands_for_intent", _op)

    def load_unresolved_commands(self) -> list[Command]:
        """Commands without terminal evidence: UNSENT without marker stays UNSENT;
        anything dispatch-marked without DEFINITE/RECONCILED outcome is UNKNOWN-class."""

        def _op() -> list[Command]:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM commands WHERE outcome IN (?,?,?) ORDER BY created_at_ns",
                (
                    CommandOutcome.UNSENT.value,
                    CommandOutcome.UNKNOWN.value,
                    CommandOutcome.DEFINITE_ACCEPT.value,
                ),
            )
            out: list[Command] = []
            for r in cur.fetchall():
                out.append(
                    Command(
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
                )
            return out

        return self._wrap("load_unresolved_commands", _op)

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
        """Append an economic event using account/transaction identity."""

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO economic_events(account, venue_transaction_id, currency,"
                    " amount, effective_time_ns, received_at_ns, event_type, revision)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (
                        ev.account,
                        ev.venue_transaction_id,
                        ev.currency,
                        canonical_decimal_str(ev.amount),
                        ev.effective_time_ns,
                        ev.received_at_ns,
                        ev.event_type,
                        ev.revision,
                    ),
                )

        return self._wrap("append_economic_event", _op)

    # ---- Durable execution/status evidence ----
    def append_execution_evidence(self, fill: FillRecord) -> bool:
        """Append one execution, returning False for an exact duplicate.

        Exchange execution IDs are the durable deduplication key. Reusing an
        ID with a different payload is a contradiction, never a replacement.
        """
        def _op() -> bool:
            with self._tx() as cur:
                cur.execute("SELECT * FROM execution_evidence WHERE execution_id=?", (fill.execution_id,))
                row = cur.fetchone()
                if row is not None:
                    fields = {
                        "order_id": fill.order_id,
                        "client_order_id": fill.client_order_id,
                        "intent_id": fill.intent_id,
                        "instrument": fill.instrument,
                        "side": fill.side,
                        "qty": canonical_decimal_str(fill.qty),
                        "price": canonical_decimal_str(fill.price),
                        "fee": canonical_decimal_str(fill.fee),
                        "fee_currency": fill.fee_currency,
                        "trade_time_ns": fill.trade_time_ns,
                        "receive_time_ns": fill.receive_time_ns,
                        "source": fill.source,
                        "raw_hash": fill.raw_hash,
                    }
                    if any(row[k] != v for k, v in fields.items()):
                        raise PersistenceError(
                            f"conflicting execution evidence for execution_id={fill.execution_id}"
                        )
                    return False
                cur.execute(
                    "INSERT INTO execution_evidence(execution_id, order_id, client_order_id, intent_id,"
                    " instrument, side, qty, price, fee, fee_currency, trade_time_ns, receive_time_ns, source, raw_hash)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        fill.execution_id, fill.order_id, fill.client_order_id, fill.intent_id,
                        fill.instrument, fill.side, canonical_decimal_str(fill.qty),
                        canonical_decimal_str(fill.price), canonical_decimal_str(fill.fee),
                        fill.fee_currency, fill.trade_time_ns, fill.receive_time_ns,
                        fill.source, fill.raw_hash,
                    ),
                )
                return True

        return self._wrap("append_execution_evidence", _op)

    @staticmethod
    def _fill_from_row(row: sqlite3.Row) -> FillRecord:
        return FillRecord(
            execution_id=row["execution_id"], order_id=row["order_id"],
            client_order_id=row["client_order_id"], intent_id=row["intent_id"],
            instrument=row["instrument"], side=row["side"], qty=Decimal(row["qty"]),
            price=Decimal(row["price"]), fee=Decimal(row["fee"]),
            fee_currency=row["fee_currency"], trade_time_ns=int(row["trade_time_ns"]),
            receive_time_ns=int(row["receive_time_ns"]), source=row["source"],
            raw_hash=row["raw_hash"],
        )

    def get_execution_evidence(self, execution_id: str) -> FillRecord | None:
        def _op() -> FillRecord | None:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM execution_evidence WHERE execution_id=?", (execution_id,))
            row = cur.fetchone()
            return self._fill_from_row(row) if row is not None else None

        return self._wrap("get_execution_evidence", _op)

    def load_execution_evidence(
        self, *, intent_id: str | None = None, client_order_id: str | None = None
    ) -> list[FillRecord]:
        def _op() -> list[FillRecord]:
            cur = self._conn.cursor()
            clauses: list[str] = []
            params: list[str] = []
            if intent_id is not None:
                clauses.append("intent_id=?")
                params.append(intent_id)
            if client_order_id is not None:
                clauses.append("client_order_id=?")
                params.append(client_order_id)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            cur.execute(f"SELECT * FROM execution_evidence{where} ORDER BY trade_time_ns, execution_id", params)
            return [self._fill_from_row(row) for row in cur.fetchall()]

        return self._wrap("load_execution_evidence", _op)

    def append_order_status_observation(self, status: OrderStatusRecord) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO order_status_observations(order_id, client_order_id, intent_id, status,"
                    " cum_exec_qty, cum_exec_fee, cum_exec_value, avg_exec_price, receive_time_ns, source, raw_hash)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        status.order_id, status.client_order_id, status.intent_id, status.status,
                        canonical_decimal_str(status.cum_exec_qty), canonical_decimal_str(status.cum_exec_fee),
                        canonical_decimal_str(status.cum_exec_value),
                        canonical_decimal_str(status.avg_exec_price) if status.avg_exec_price is not None else None,
                        status.receive_time_ns, status.source, status.raw_hash,
                    ),
                )

        self._wrap("append_order_status_observation", _op)

    def load_order_status_observations(self, *, intent_id: str | None = None) -> list[OrderStatusRecord]:
        def _op() -> list[OrderStatusRecord]:
            cur = self._conn.cursor()
            if intent_id is None:
                cur.execute("SELECT * FROM order_status_observations ORDER BY receive_time_ns, observation_id")
            else:
                cur.execute(
                    "SELECT * FROM order_status_observations WHERE intent_id=? ORDER BY receive_time_ns, observation_id",
                    (intent_id,),
                )
            return [
                OrderStatusRecord(
                    order_id=row["order_id"], client_order_id=row["client_order_id"], intent_id=row["intent_id"],
                    status=row["status"], cum_exec_qty=Decimal(row["cum_exec_qty"]),
                    cum_exec_fee=Decimal(row["cum_exec_fee"]), cum_exec_value=Decimal(row["cum_exec_value"]),
                    avg_exec_price=Decimal(row["avg_exec_price"]) if row["avg_exec_price"] is not None else None,
                    receive_time_ns=int(row["receive_time_ns"]), source=row["source"], raw_hash=row["raw_hash"],
                ) for row in cur.fetchall()
            ]

        return self._wrap("load_order_status_observations", _op)

    def append_reconciliation_query_evidence(self, evidence: ReconciliationQueryEvidence) -> bool:
        """Persist one immutable typed query result; exact replays are ignored."""
        def _op() -> bool:
            with self._tx() as cur:
                cur.execute("SELECT evidence_hash FROM reconciliation_query_evidence WHERE query_id=?", (evidence.query_id,))
                row = cur.fetchone()
                if row is not None:
                    if row["evidence_hash"] != evidence.evidence_hash:
                        raise PersistenceError(f"conflicting reconciliation evidence for {evidence.query_id}")
                    return False
                cur.execute(
                    "INSERT INTO reconciliation_query_evidence(query_id, query_type, account, instrument,"
                    " requested_interval_start_ns, requested_interval_end_ns, pagination_cursors_json, pages_observed,"
                    " total_records_returned, completeness, status, source_time_ns, receipt_time_ns, request_ids_json,"
                    " retention_coverage_start_ns, retention_coverage_end_ns, evidence_hash, error_message)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        evidence.query_id, evidence.query_type.value, evidence.account, evidence.instrument,
                        evidence.requested_interval_start_ns, evidence.requested_interval_end_ns,
                        json.dumps(list(evidence.pagination_cursors), sort_keys=True), evidence.pages_observed,
                        evidence.total_records_returned, evidence.completeness.value, evidence.status.value,
                        evidence.source_time_ns, evidence.receipt_time_ns,
                        json.dumps(list(evidence.request_ids), sort_keys=True), evidence.retention_coverage_start_ns,
                        evidence.retention_coverage_end_ns, evidence.evidence_hash, evidence.error_message,
                    ),
                )
                return True

        return self._wrap("append_reconciliation_query_evidence", _op)

    def load_reconciliation_query_evidence(self, query_id: str | None = None) -> list[ReconciliationQueryEvidence]:
        def _op() -> list[ReconciliationQueryEvidence]:
            cur = self._conn.cursor()
            if query_id is None:
                cur.execute("SELECT * FROM reconciliation_query_evidence ORDER BY receipt_time_ns, query_id")
            else:
                cur.execute("SELECT * FROM reconciliation_query_evidence WHERE query_id=?", (query_id,))
            rows = cur.fetchall()
            return [
                ReconciliationQueryEvidence(
                    query_id=row["query_id"], query_type=QueryType(row["query_type"]), account=row["account"],
                    instrument=row["instrument"], requested_interval_start_ns=row["requested_interval_start_ns"],
                    requested_interval_end_ns=row["requested_interval_end_ns"],
                    pagination_cursors=tuple(json.loads(row["pagination_cursors_json"])),
                    pages_observed=int(row["pages_observed"]), total_records_returned=int(row["total_records_returned"]),
                    completeness=Completeness(row["completeness"]), status=QueryStatus(row["status"]),
                    source_time_ns=row["source_time_ns"], receipt_time_ns=int(row["receipt_time_ns"]),
                    request_ids=tuple(json.loads(row["request_ids_json"])),
                    retention_coverage_start_ns=row["retention_coverage_start_ns"],
                    retention_coverage_end_ns=row["retention_coverage_end_ns"], evidence_hash=row["evidence_hash"],
                    error_message=row["error_message"],
                ) for row in rows
            ]

        return self._wrap("load_reconciliation_query_evidence", _op)

    def load_reconciliation_evidence_bundle(
        self,
        *,
        reconciliation_run_id: str,
        account: str,
        instrument: str | None,
        query_ids: tuple[str, ...],
        started_at_ns: int,
        completed_at_ns: int,
    ) -> ReconciliationEvidenceBundle:
        """Reconstruct one typed reconciliation bundle after restart.

        Query rows are immutable and intentionally do not carry mutable run
        state. The caller supplies the persisted query IDs belonging to the
        run, then this method derives the aggregate status/completeness from
        those rows without inventing venue evidence.
        """
        if not query_ids:
            raise PersistenceError("reconciliation bundle requires persisted query IDs")
        persisted = {e.query_id: e for e in self.load_reconciliation_query_evidence()}
        missing = [query_id for query_id in query_ids if query_id not in persisted]
        if missing:
            raise PersistenceError(f"reconciliation query evidence missing: {missing}")
        queries = tuple(persisted[query_id] for query_id in query_ids)
        if any(e.account != account or e.instrument != instrument for e in queries):
            raise PersistenceError("reconciliation evidence identity does not match requested bundle")
        severity = {
            Completeness.COMPLETE: 0,
            Completeness.INCOMPLETE_PAGINATED: 1,
            Completeness.INCOMPLETE_TRUNCATED: 2,
            Completeness.INCOMPLETE_RETENTION_LIMIT: 3,
            Completeness.UNKNOWN: 4,
        }
        if any(e.status in (QueryStatus.FAILED, QueryStatus.TIMEOUT) for e in queries):
            overall_status = QueryStatus.FAILED
        elif any(e.status in (QueryStatus.PARTIAL, QueryStatus.RATE_LIMITED) for e in queries):
            overall_status = QueryStatus.PARTIAL
        else:
            overall_status = QueryStatus.SUCCESS
        return ReconciliationEvidenceBundle(
            reconciliation_run_id=reconciliation_run_id,
            account=account,
            instrument=instrument,
            started_at_ns=started_at_ns,
            completed_at_ns=completed_at_ns,
            queries=queries,
            overall_status=overall_status,
            overall_completeness=max(
                (e.completeness for e in queries), key=lambda item: severity[item]
            ),
        )

    def append_recovery_certificate(self, certificate: RecoveryCertificate) -> None:
        """Persist the immutable typed recovery decision and evidence chain."""
        protection = certificate.protection_evidence

        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO recovery_certificates(recovery_run_id, writer_id, writer_epoch,"
                    " journal_schema_version, unresolved_intents_json, unresolved_commands_json,"
                    " unknown_commands_json, reconciliation_health, protection_uncertainty_summary,"
                    " started_at_ns, ended_at_ns, evidence_refs_json, venue_observations_obtained,"
                    " venue_evidence_refs_json, decision, protection_certified_flat, protection_current,"
                    " protection_evidence_refs_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        certificate.recovery_run_id,
                        certificate.writer_id,
                        certificate.writer_epoch,
                        certificate.journal_schema_version,
                        json.dumps(list(certificate.unresolved_intents), sort_keys=True),
                        json.dumps(list(certificate.unresolved_commands), sort_keys=True),
                        json.dumps(list(certificate.unknown_commands), sort_keys=True),
                        certificate.reconciliation_health.value,
                        certificate.protection_uncertainty_summary,
                        certificate.started_at_ns,
                        certificate.ended_at_ns,
                        json.dumps(list(certificate.evidence_refs), sort_keys=True),
                        int(certificate.venue_observations_obtained),
                        json.dumps(list(certificate.venue_evidence_refs), sort_keys=True),
                        certificate.decision.value,
                        int(protection.certified_flat) if protection is not None else None,
                        int(protection.current_protection) if protection is not None else None,
                        json.dumps(list(protection.evidence_refs), sort_keys=True)
                        if protection is not None else None,
                    ),
                )

        self._wrap("append_recovery_certificate", _op)

    def load_recovery_certificate(self, recovery_run_id: str) -> RecoveryCertificate | None:
        """Load the exact recovery certificate persisted for a run."""
        def _op() -> RecoveryCertificate | None:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM recovery_certificates WHERE recovery_run_id=?", (recovery_run_id,))
            row = cur.fetchone()
            if row is None:
                return None
            protection: RecoveryProtectionEvidence | None = None
            if row["protection_certified_flat"] is not None:
                protection = RecoveryProtectionEvidence(
                    certified_flat=bool(row["protection_certified_flat"]),
                    current_protection=bool(row["protection_current"]),
                    evidence_refs=tuple(json.loads(row["protection_evidence_refs_json"])),
                )
            return RecoveryCertificate(
                recovery_run_id=row["recovery_run_id"], writer_id=row["writer_id"],
                writer_epoch=int(row["writer_epoch"]), journal_schema_version=int(row["journal_schema_version"]),
                unresolved_intents=tuple(json.loads(row["unresolved_intents_json"])),
                unresolved_commands=tuple(json.loads(row["unresolved_commands_json"])),
                unknown_commands=tuple(json.loads(row["unknown_commands_json"])),
                reconciliation_health=ReconciliationHealth(row["reconciliation_health"]),
                protection_uncertainty_summary=row["protection_uncertainty_summary"],
                started_at_ns=int(row["started_at_ns"]), ended_at_ns=int(row["ended_at_ns"]),
                evidence_refs=tuple(json.loads(row["evidence_refs_json"])),
                venue_observations_obtained=bool(row["venue_observations_obtained"]),
                venue_evidence_refs=tuple(json.loads(row["venue_evidence_refs_json"])),
                decision=RecoveryDecision(row["decision"]), protection_evidence=protection,
            )

        return self._wrap("load_recovery_certificate", _op)

    def append_recovery_incident(self, incident: RecoveryIncident) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO recovery_incidents(incident_id, recovery_run_id, category, status,"
                    " evidence_refs_json, opened_at_ns, resolved_at_ns) VALUES(?,?,?,?,?,?,?)",
                    (
                        incident.incident_id, incident.recovery_run_id, incident.category, incident.status,
                        json.dumps(list(incident.evidence_refs), sort_keys=True), incident.opened_at_ns,
                        incident.resolved_at_ns,
                    ),
                )

        self._wrap("append_recovery_incident", _op)

    def load_recovery_incidents(self, recovery_run_id: str | None = None) -> list[RecoveryIncident]:
        def _op() -> list[RecoveryIncident]:
            cur = self._conn.cursor()
            if recovery_run_id is None:
                cur.execute("SELECT * FROM recovery_incidents ORDER BY opened_at_ns, incident_id")
            else:
                cur.execute("SELECT * FROM recovery_incidents WHERE recovery_run_id=? ORDER BY opened_at_ns, incident_id", (recovery_run_id,))
            return [
                RecoveryIncident(
                    incident_id=row["incident_id"], recovery_run_id=row["recovery_run_id"], category=row["category"],
                    status=row["status"], evidence_refs=tuple(json.loads(row["evidence_refs_json"])),
                    opened_at_ns=int(row["opened_at_ns"]), resolved_at_ns=row["resolved_at_ns"],
                ) for row in cur.fetchall()
            ]

        return self._wrap("load_recovery_incidents", _op)

    def append_capability_evidence(self, evidence: CapabilityEvidence) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO capability_evidence_log(capability_name, state, test_run_id, evidence_refs_json,"
                    " test_timestamp_ns, environment, notes, target_profile_hash) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        evidence.capability_name, evidence.state.value, evidence.test_run_id,
                        json.dumps(list(evidence.evidence_refs), sort_keys=True), evidence.test_timestamp_ns,
                        evidence.environment, evidence.notes, evidence.target_profile_hash,
                    ),
                )

        self._wrap("append_capability_evidence", _op)

    def load_latest_capability_evidence(self) -> dict[str, CapabilityEvidence]:
        def _op() -> dict[str, CapabilityEvidence]:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM capability_evidence_log WHERE evidence_id IN "
                "(SELECT MAX(evidence_id) FROM capability_evidence_log GROUP BY capability_name)"
            )
            return {
                row["capability_name"]: CapabilityEvidence(
                    capability_name=row["capability_name"], state=EvidenceState(row["state"]),
                    test_run_id=row["test_run_id"], evidence_refs=tuple(json.loads(row["evidence_refs_json"])),
                    test_timestamp_ns=row["test_timestamp_ns"], environment=row["environment"], notes=row["notes"],
                    target_profile_hash=row["target_profile_hash"],
                ) for row in cur.fetchall()
            }

        return self._wrap("load_latest_capability_evidence", _op)

    def append_capability_qualification(self, record: QualificationRecord) -> None:
        def _op() -> None:
            with self._tx() as cur:
                cur.execute(
                    "INSERT INTO capability_qualification_log(qualification_id, capability_name, previous_state,"
                    " new_state, test_run_id, evidence_refs_json, qualified_by, qualified_at_ns, target_profile_hash)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        record.qualification_id, record.capability_name, record.previous_state.value,
                        record.new_state.value, record.test_run_id, json.dumps(list(record.evidence_refs), sort_keys=True),
                        record.qualified_by, record.qualified_at_ns, record.target_profile_hash,
                    ),
                )

        self._wrap("append_capability_qualification", _op)

    def release_reservation_from_flat_certificate(self, certificate: Any) -> Reservation:
        """Release only after a valid positive flat certificate."""
        if not getattr(certificate, "can_release_reservation", False):
            raise PersistenceError("reservation release requires a certified flat certificate")
        intent_id = getattr(certificate, "intent_id", None)
        if not intent_id:
            raise PersistenceError("flat certificate is not bound to an intent")

        def _op() -> Reservation:
            with self._tx() as cur:
                cur.execute("SELECT * FROM reservations WHERE intent_id=?", (intent_id,))
                row = cur.fetchone()
                if row is None:
                    raise PersistenceError(f"reservation not found for {intent_id}")
                version = int(row["version"])
                cur.execute(
                    "UPDATE reservations SET remaining_open_qty=?, version=? WHERE intent_id=? AND version=?",
                    ("0", version + 1, intent_id, version),
                )
                if cur.rowcount != 1:
                    raise PersistenceError("reservation release version conflict")
                return Reservation(
                    reservation_id=row["reservation_id"], intent_id=row["intent_id"], remaining_open_qty=Decimal("0"),
                    normal_loss=Decimal(row["normal_loss"]), stress_loss=Decimal(row["stress_loss"]),
                    notional=Decimal(row["notional"]), beta_adjusted_notional=Decimal(row["beta_adjusted_notional"]),
                    margin=Decimal(row["margin"]), es_contribution=Decimal(row["es_contribution"]), version=version + 1,
                )

        return self._wrap("release_reservation_from_flat_certificate", _op)

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
            "execution_evidence",
            "order_status_observations",
            "reconciliation_query_evidence",
            "recovery_incidents",
            "recovery_certificates",
            "capability_evidence_log",
            "capability_qualification_log",
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
        if self._closed:
            return
        try:
            self._conn.close()
            self._closed = True
        except sqlite3.Error as exc:
            raise PersistenceError(f"close failed: {exc}") from exc

    @property
    def is_open(self) -> bool:
        return not self._closed
