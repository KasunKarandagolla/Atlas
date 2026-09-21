"""Protection deadline state machine (freeze §1.3).

Deterministic orchestration logic for frozen protection deadline WITHOUT network calls:

After fill:
1. protection becomes stale/unconfirmed
2. inspection begins immediately
3. aggregate current net position must be covered
4. if unconfirmed at 2 seconds:
   - opening leaves scheduled for cancellation
   - stop repair intent recorded
   - reduce-only flatten intent becomes eligible per emergency path
5. remain RECOVERY_REQUIRED until flat or current protection proven

Do NOT claim flatten executed.
Do NOT remove existing native protection before repair.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import ProtectionObservation, make_command
from atlas.domain.time import ensure_utc_ns
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.fill_dedup import FillRecord


class ProtectionDeadlineState(StrEnum):
    """Protection deadline states."""
    CONFIRMED = "CONFIRMED"           # Protection verified current
    UNCONFIRMED_POST_FILL = "UNCONFIRMED_POST_FILL"  # After fill, inspection started
    REPAIR_SCHEDULED = "REPAIR_SCHEDULED"  # 2s elapsed, repair scheduled
    FLATTEN_ELIGIBLE = "FLATTEN_ELIGIBLE"  # 2s elapsed, reduce-only flatten eligible
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"  # Cannot confirm, recovery needed


@dataclass(frozen=True)
class ProtectionDeadlineConfig:
    """Configuration for protection deadline behavior."""
    unconfirmed_threshold_ns: int = 2_000_000_000  # 2 seconds
    max_repair_attempts: int = 3
    repair_interval_ns: int = 1_000_000_000  # 1 second


@dataclass(frozen=True)
class ProtectionDeadlineSnapshot:
    """Snapshot of protection deadline state."""

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

    def __post_init__(self) -> None:
        if not self.intent_id or not self.intent_id.strip():
            raise ValueError("intent_id must be non-blank")
        if not isinstance(self.position_epoch, int) or isinstance(self.position_epoch, bool) or self.position_epoch < 0:
            raise ValueError("position_epoch must be int >= 0")
        if not isinstance(self.state, ProtectionDeadlineState):
            raise ValueError("state must be ProtectionDeadlineState")
        if not isinstance(self.desired_stop_version, int) or isinstance(self.desired_stop_version, bool) or self.desired_stop_version < 0:
            raise ValueError("desired_stop_version must be int >= 0")
        if not isinstance(self.observed_signed_qty, Decimal):
            raise ValueError("observed_signed_qty must be Decimal")
        ensure_utc_ns(self.last_observation_ns, field="last_observation_ns")
        ensure_utc_ns(self.fill_time_ns, field="fill_time_ns")
        if not isinstance(self.repair_attempts, int) or isinstance(self.repair_attempts, bool) or self.repair_attempts < 0:
            raise ValueError("repair_attempts must be int >= 0")
        if self.next_repair_at_ns is not None:
            ensure_utc_ns(self.next_repair_at_ns, field="next_repair_at_ns")
        if not isinstance(self.cancel_scheduled, bool):
            raise ValueError("cancel_scheduled must be bool")
        if not isinstance(self.flatten_eligible, bool):
            raise ValueError("flatten_eligible must be bool")


class ProtectionDeadlineMachine:
    """Deterministic protection deadline state machine.

    Tracks protection state after fills and enforces 2-second deadline.
    Does NOT execute any network calls - only produces intents/commands.
    """

    def __init__(self, config: ProtectionDeadlineConfig | None = None) -> None:
        self._config = config or ProtectionDeadlineConfig()
        self._snapshots: dict[str, ProtectionDeadlineSnapshot] = {}
        self._desired_stop_prices: dict[str, Decimal | None] = {}

    def on_fill(
        self,
        intent_id: str,
        position_epoch: int,
        fill: FillRecord,
        desired_stop_version: int,
        desired_stop_price: Decimal | None = None,
    ) -> ProtectionDeadlineSnapshot:
        """Handle a fill - protection becomes unconfirmed, inspection begins."""
        # Aggregate current net position must be covered
        snapshot = ProtectionDeadlineSnapshot(
            intent_id=intent_id,
            position_epoch=position_epoch,
            state=ProtectionDeadlineState.UNCONFIRMED_POST_FILL,
            desired_stop_version=desired_stop_version,
            observed_signed_qty=fill.qty if fill.side == "Buy" else -fill.qty,
            last_observation_ns=fill.trade_time_ns,
            fill_time_ns=fill.trade_time_ns,
            repair_attempts=0,
            next_repair_at_ns=fill.trade_time_ns + self._config.unconfirmed_threshold_ns,
            cancel_scheduled=False,
            flatten_eligible=False,
        )
        self._snapshots[intent_id] = snapshot
        self._desired_stop_prices[intent_id] = desired_stop_price
        return snapshot

    def on_protection_observation(
        self,
        intent_id: str,
        observation: ProtectionObservation,
        now_ns: int,
    ) -> ProtectionDeadlineSnapshot | None:
        """Handle a protection observation - may confirm or remain unconfirmed."""
        if intent_id not in self._snapshots:
            return None

        current = self._snapshots[intent_id]

        # Check the actual V1 protection semantics, not just epoch/version/qty.
        desired_stop_price = self._desired_stop_prices.get(intent_id)
        semantics = observation.semantics.lower()
        age_ns = now_ns - observation.observed_at_ns
        valid_semantics = (
            observation.trigger_basis == "MarkPrice"
            and "full" in semantics
            and "market" in semantics
            and ("reduce" in semantics or "close" in semantics)
            and bool(observation.evidence_ids)
        )
        if desired_stop_price is not None:
            valid_semantics = valid_semantics and observation.stop_price == desired_stop_price

        if (observation.position_epoch == current.position_epoch and
            observation.desired_stop_version == current.desired_stop_version and
            observation.qty == current.observed_signed_qty and
            age_ns >= 0 and age_ns <= self._config.unconfirmed_threshold_ns and
            valid_semantics):
            # Protection confirmed
            snapshot = ProtectionDeadlineSnapshot(
                intent_id=current.intent_id,
                position_epoch=current.position_epoch,
                state=ProtectionDeadlineState.CONFIRMED,
                desired_stop_version=current.desired_stop_version,
                observed_signed_qty=current.observed_signed_qty,
                last_observation_ns=observation.observed_at_ns,
                fill_time_ns=current.fill_time_ns,
                repair_attempts=current.repair_attempts,
                next_repair_at_ns=None,
                cancel_scheduled=current.cancel_scheduled,
                flatten_eligible=current.flatten_eligible,
            )
            self._snapshots[intent_id] = snapshot
            return snapshot

        # Observation doesn't match - remains unconfirmed
        snapshot = ProtectionDeadlineSnapshot(
            intent_id=current.intent_id,
            position_epoch=current.position_epoch,
            state=current.state,
            desired_stop_version=current.desired_stop_version,
            observed_signed_qty=current.observed_signed_qty,
            last_observation_ns=observation.observed_at_ns,
            fill_time_ns=current.fill_time_ns,
            repair_attempts=current.repair_attempts,
            next_repair_at_ns=current.next_repair_at_ns,
            cancel_scheduled=current.cancel_scheduled,
            flatten_eligible=current.flatten_eligible,
        )
        self._snapshots[intent_id] = snapshot
        return snapshot


    def tick(self, now_ns: int) -> dict[str, ProtectionDeadlineSnapshot]:
        """Advance time - check deadlines and transition states."""
        transitions: dict[str, ProtectionDeadlineSnapshot] = {}

        for intent_id, current in self._snapshots.items():
            if current.state == ProtectionDeadlineState.CONFIRMED:
                continue

            # Check if unconfirmed threshold elapsed
            if current.next_repair_at_ns is not None and now_ns >= current.next_repair_at_ns:
                if current.state == ProtectionDeadlineState.UNCONFIRMED_POST_FILL:
                    # At the two-second breach recovery is required.  The
                    # returned flags are durable actions for the caller; no
                    # transport or execution is claimed here.
                    snapshot = ProtectionDeadlineSnapshot(
                        intent_id=current.intent_id,
                        position_epoch=current.position_epoch,
                        state=ProtectionDeadlineState.RECOVERY_REQUIRED,
                        desired_stop_version=current.desired_stop_version,
                        observed_signed_qty=current.observed_signed_qty,
                        last_observation_ns=current.last_observation_ns,
                        fill_time_ns=current.fill_time_ns,
                        repair_attempts=current.repair_attempts,
                        next_repair_at_ns=now_ns + self._config.repair_interval_ns,
                        cancel_scheduled=True,
                        flatten_eligible=True,
                    )
                    self._snapshots[intent_id] = snapshot
                    transitions[intent_id] = snapshot
                elif current.state in (
                    ProtectionDeadlineState.REPAIR_SCHEDULED,
                    ProtectionDeadlineState.RECOVERY_REQUIRED,
                ):
                    if current.repair_attempts < self._config.max_repair_attempts:
                        # Schedule next repair attempt
                        snapshot = ProtectionDeadlineSnapshot(
                            intent_id=current.intent_id,
                            position_epoch=current.position_epoch,
                            state=ProtectionDeadlineState.REPAIR_SCHEDULED,
                            desired_stop_version=current.desired_stop_version,
                            observed_signed_qty=current.observed_signed_qty,
                            last_observation_ns=current.last_observation_ns,
                            fill_time_ns=current.fill_time_ns,
                            repair_attempts=current.repair_attempts + 1,
                            next_repair_at_ns=now_ns + self._config.repair_interval_ns,
                            cancel_scheduled=current.cancel_scheduled,
                            flatten_eligible=current.flatten_eligible,
                        )
                        self._snapshots[intent_id] = snapshot
                        transitions[intent_id] = snapshot
                    else:
                        # Max repair attempts reached - require recovery
                        snapshot = ProtectionDeadlineSnapshot(
                            intent_id=current.intent_id,
                            position_epoch=current.position_epoch,
                            state=ProtectionDeadlineState.RECOVERY_REQUIRED,
                            desired_stop_version=current.desired_stop_version,
                            observed_signed_qty=current.observed_signed_qty,
                            last_observation_ns=current.last_observation_ns,
                            fill_time_ns=current.fill_time_ns,
                            repair_attempts=current.repair_attempts,
                            next_repair_at_ns=None,
                            cancel_scheduled=current.cancel_scheduled,
                            flatten_eligible=current.flatten_eligible,
                        )
                        self._snapshots[intent_id] = snapshot
                        transitions[intent_id] = snapshot

        return transitions

    def get_snapshot(self, intent_id: str) -> ProtectionDeadlineSnapshot | None:
        return self._snapshots.get(intent_id)

    def is_recovery_required(self, intent_id: str) -> bool:
        snapshot = self._snapshots.get(intent_id)
        return snapshot is not None and snapshot.state == ProtectionDeadlineState.RECOVERY_REQUIRED

    def get_scheduled_actions(self, intent_id: str) -> dict[str, Any]:
        """Get scheduled actions for an intent (for test verification)."""
        snapshot = self._snapshots.get(intent_id)
        if snapshot is None:
            return {}

        actions: dict[str, Any] = {}
        if snapshot.cancel_scheduled:
            actions["cancel_entry_leaves"] = True
        if snapshot.flatten_eligible:
            actions["reduce_only_flatten_eligible"] = True
        if snapshot.state in (
            ProtectionDeadlineState.REPAIR_SCHEDULED,
            ProtectionDeadlineState.RECOVERY_REQUIRED,
        ):
            actions["stop_repair_intent"] = {
                "position_epoch": snapshot.position_epoch,
                "desired_stop_version": snapshot.desired_stop_version,
                "attempt": snapshot.repair_attempts + 1,
            }
        if snapshot.state == ProtectionDeadlineState.RECOVERY_REQUIRED:
            actions["recovery_required"] = True
        return actions

    def on_additional_fill(
        self,
        intent_id: str,
        fill: FillRecord,
    ) -> ProtectionDeadlineSnapshot | None:
        """Handle additional fill - last protection observation becomes stale."""
        if intent_id not in self._snapshots:
            return None

        current = self._snapshots[intent_id]
        # Update aggregate signed quantity.  Once the deadline has breached,
        # an additional fill must not clear required actions or recovery.
        new_qty = current.observed_signed_qty + (fill.qty if fill.side == "Buy" else -fill.qty)
        actions_already_required = (
            current.cancel_scheduled
            or current.flatten_eligible
            or current.state == ProtectionDeadlineState.RECOVERY_REQUIRED
        )

        snapshot = ProtectionDeadlineSnapshot(
            intent_id=current.intent_id,
            position_epoch=current.position_epoch,
            state=ProtectionDeadlineState.RECOVERY_REQUIRED
            if actions_already_required
            else ProtectionDeadlineState.UNCONFIRMED_POST_FILL,
            desired_stop_version=current.desired_stop_version + 1,  # New desired version
            observed_signed_qty=new_qty,
            last_observation_ns=fill.trade_time_ns,
            fill_time_ns=fill.trade_time_ns,
            repair_attempts=0,
            next_repair_at_ns=(
                current.next_repair_at_ns
                if actions_already_required and current.next_repair_at_ns is not None
                else fill.trade_time_ns + self._config.unconfirmed_threshold_ns
            ),
            cancel_scheduled=current.cancel_scheduled if actions_already_required else False,
            flatten_eligible=current.flatten_eligible if actions_already_required else False,
        )
        self._snapshots[intent_id] = snapshot
        return snapshot


def persist_deadline_actions(
    journal: SQLiteJournal,
    snapshot: ProtectionDeadlineSnapshot,
    now_ns: int,
) -> tuple[str, ...]:
    """Persist deadline actions as UNSENT commands; perform no transport."""
    if snapshot.state not in (
        ProtectionDeadlineState.REPAIR_SCHEDULED,
        ProtectionDeadlineState.FLATTEN_ELIGIBLE,
        ProtectionDeadlineState.RECOVERY_REQUIRED,
    ):
        return ()
    intent = journal.load_intent(snapshot.intent_id)
    if intent.lifecycle != LifecycleState.RECOVERY_REQUIRED:
        intent = journal.update_intent_state(
            intent_id=intent.intent_id,
            lifecycle=LifecycleState.RECOVERY_REQUIRED,
            protection=ProtectionStatus.UNCONFIRMED,
            health=ReconciliationHealth.CONFLICTED,
            expected_version=intent.state_version,
        )
    actions: dict[str, tuple[CommandType, dict[str, Any]]] = {
        f"{snapshot.intent_id}-cancel-entry": (
            CommandType.CANCEL_ENTRY,
            {"position_epoch": snapshot.position_epoch, "reason": "protection_deadline"},
        ),
        f"{snapshot.intent_id}-repair-stop": (
            CommandType.REPAIR_STOP,
            {"position_epoch": snapshot.position_epoch, "desired_stop_version": snapshot.desired_stop_version},
        ),
        f"{snapshot.intent_id}-flatten": (
            CommandType.FLATTEN,
            {"position_epoch": snapshot.position_epoch, "signed_qty": str(snapshot.observed_signed_qty), "reduce_only": True},
        ),
    }
    persisted: list[str] = []
    for command_id, (command_type, payload) in actions.items():
        try:
            journal.load_command(command_id)
        except PersistenceError:
            journal.persist_command(
                make_command(
                    command_id=command_id, intent_id=intent.intent_id, command_type=command_type,
                    payload_dict=payload, expected_state_version=intent.state_version, created_at_ns=now_ns,
                )
            )
        persisted.append(command_id)
    return tuple(persisted)
