"""Two-second protection deadline state machine; no transport."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.enums import LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import CommandType, ProtectionObservation, make_command
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal

from .fill_dedup import FillRecord


class ProtectionDeadlineState(StrEnum):
    UNCONFIRMED_POST_FILL = "UNCONFIRMED_POST_FILL"
    CONFIRMED = "CONFIRMED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


@dataclass(frozen=True)
class ProtectionDeadlineConfig:
    unconfirmed_threshold_ns: int = 2_000_000_000
    repair_interval_ns: int = 2_000_000_000
    max_repair_attempts: int = 3


@dataclass(frozen=True)
class ProtectionDeadlineSnapshot:
    intent_id: str
    position_epoch: int
    state: ProtectionDeadlineState
    desired_stop_version: int
    observed_signed_qty: Decimal
    last_observation_ns: int
    fill_time_ns: int
    repair_attempts: int
    next_repair_at_ns: int | None
    cancel_scheduled: bool
    flatten_eligible: bool


class ProtectionDeadlineMachine:
    def __init__(self, config: ProtectionDeadlineConfig | None = None):
        self.config = config or ProtectionDeadlineConfig()
        self._s: dict[str, ProtectionDeadlineSnapshot] = {}
        self._stop: dict[str, Decimal] = {}

    def on_fill(
        self,
        intent_id: str,
        position_epoch: int,
        fill: FillRecord,
        desired_stop_version: int,
        desired_stop_price: Decimal,
    ) -> ProtectionDeadlineSnapshot:
        q = fill.qty if fill.side == "Buy" else -fill.qty
        snap = ProtectionDeadlineSnapshot(
            intent_id,
            position_epoch,
            ProtectionDeadlineState.UNCONFIRMED_POST_FILL,
            desired_stop_version,
            q,
            fill.trade_time_ns,
            fill.trade_time_ns,
            0,
            fill.trade_time_ns + self.config.unconfirmed_threshold_ns,
            False,
            False,
        )
        self._s[intent_id] = snap
        self._stop[intent_id] = desired_stop_price
        return snap

    def on_additional_fill(self, intent_id: str, fill: FillRecord) -> ProtectionDeadlineSnapshot:
        c = self._s[intent_id]
        q = c.observed_signed_qty + (fill.qty if fill.side == "Buy" else -fill.qty)
        breached = c.state == ProtectionDeadlineState.RECOVERY_REQUIRED
        snap = ProtectionDeadlineSnapshot(
            intent_id,
            c.position_epoch,
            ProtectionDeadlineState.RECOVERY_REQUIRED if breached else ProtectionDeadlineState.UNCONFIRMED_POST_FILL,
            c.desired_stop_version + 1,
            q,
            fill.trade_time_ns,
            fill.trade_time_ns,
            c.repair_attempts,
            c.next_repair_at_ns if breached else fill.trade_time_ns + self.config.unconfirmed_threshold_ns,
            c.cancel_scheduled or breached,
            c.flatten_eligible or breached,
        )
        self._s[intent_id] = snap
        return snap

    def on_protection_observation(
        self, intent_id: str, o: ProtectionObservation, now_ns: int
    ) -> ProtectionDeadlineSnapshot:
        c = self._s[intent_id]
        sem = o.semantics.lower()
        valid = (
            o.position_epoch == c.position_epoch
            and o.desired_stop_version == c.desired_stop_version
            and o.qty == c.observed_signed_qty
            and o.stop_price == self._stop[intent_id]
            and o.trigger_basis == "MarkPrice"
            and "full" in sem
            and "market" in sem
            and ("reduce" in sem or "close" in sem)
            and bool(o.evidence_ids)
            and 0 <= now_ns - o.observed_at_ns <= self.config.unconfirmed_threshold_ns
        )
        if valid:
            snap = ProtectionDeadlineSnapshot(
                c.intent_id,
                c.position_epoch,
                ProtectionDeadlineState.CONFIRMED,
                c.desired_stop_version,
                c.observed_signed_qty,
                o.observed_at_ns,
                c.fill_time_ns,
                c.repair_attempts,
                None,
                c.cancel_scheduled,
                c.flatten_eligible,
            )
            self._s[intent_id] = snap
            return snap
        return c

    def tick(self, now_ns: int) -> dict[str, ProtectionDeadlineSnapshot]:
        out = {}
        for k, c in list(self._s.items()):
            if (
                c.state == ProtectionDeadlineState.CONFIRMED
                or c.next_repair_at_ns is None
                or now_ns < c.next_repair_at_ns
            ):
                continue
            # Monotonic: after breach, recovery remains required until positive protection/flat evidence resolves externally.
            attempts = min(c.repair_attempts + 1, self.config.max_repair_attempts)
            snap = ProtectionDeadlineSnapshot(
                c.intent_id,
                c.position_epoch,
                ProtectionDeadlineState.RECOVERY_REQUIRED,
                c.desired_stop_version,
                c.observed_signed_qty,
                c.last_observation_ns,
                c.fill_time_ns,
                attempts,
                now_ns + self.config.repair_interval_ns if attempts < self.config.max_repair_attempts else None,
                True,
                True,
            )
            self._s[k] = snap
            out[k] = snap
        return out

    def get_snapshot(self, intent_id: str) -> ProtectionDeadlineSnapshot | None:
        return self._s.get(intent_id)

    def get_scheduled_actions(self, intent_id: str) -> dict[str, object]:
        """Return durable-action intent for compatibility and observability.

        This reports what the state machine authorizes; it does not claim that
        any venue command was sent or executed.
        """
        c = self._s.get(intent_id)
        if c is None:
            return {}
        breached = c.state == ProtectionDeadlineState.RECOVERY_REQUIRED
        return {
            "cancel_entry_leaves": c.cancel_scheduled,
            "reduce_only_flatten_eligible": c.flatten_eligible,
            "stop_repair_intent": {
                "position_epoch": c.position_epoch,
                "desired_stop_version": c.desired_stop_version,
                "attempt": c.repair_attempts,
            }
            if breached
            else None,
            "recovery_required": breached,
        }


def persist_deadline_actions(journal: SQLiteJournal, s: ProtectionDeadlineSnapshot, now_ns: int) -> tuple[str, ...]:
    if s.state != ProtectionDeadlineState.RECOVERY_REQUIRED:
        return ()
    intent = journal.load_intent(s.intent_id)
    if intent.lifecycle != LifecycleState.RECOVERY_REQUIRED:
        intent = journal.update_intent_state(
            intent_id=intent.intent_id,
            lifecycle=LifecycleState.RECOVERY_REQUIRED,
            protection=ProtectionStatus.UNCONFIRMED,
            health=ReconciliationHealth.CONFLICTED,
            expected_version=intent.state_version,
        )
    actions = (
        (f"{s.intent_id}-cancel-entry", CommandType.CANCEL_ENTRY, {"position_epoch": s.position_epoch}),
        (
            f"{s.intent_id}-repair-stop",
            CommandType.REPAIR_STOP,
            {"position_epoch": s.position_epoch, "desired_stop_version": s.desired_stop_version},
        ),
        (
            f"{s.intent_id}-flatten",
            CommandType.FLATTEN,
            {"position_epoch": s.position_epoch, "signed_qty": str(s.observed_signed_qty), "reduce_only": True},
        ),
    )
    ids = []
    for cid, ct, payload in actions:
        try:
            journal.load_command(cid)
        except PersistenceError:
            journal.persist_command(
                make_command(
                    command_id=cid,
                    intent_id=intent.intent_id,
                    command_type=ct,
                    payload_dict=payload,
                    expected_state_version=intent.state_version,
                    created_at_ns=now_ns,
                )
            )
        ids.append(cid)
    return tuple(ids)
