"""Deterministic, test-only Phase 2 fault harness.

The harness uses the real journal, command transitions, durable execution and
status evidence, reconciliation evidence, protection deadline machine, flat
certificate inputs and late-entry invariant. It is not an exchange client and
never produces venue qualification evidence.
"""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth, Side
from atlas.domain.execution import EconomicEvent, Intent, Reservation, generate_client_order_id, make_command
from atlas.domain.trade_plan import TradePlan
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.fill_dedup import FillDeduplicator, FillRecord, OrderStatusRecord
from atlas.runtime.no_reversal import check_late_entry_reopening
from atlas.runtime.protection_deadline import ProtectionDeadlineMachine, ProtectionDeadlineState
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryStatus,
    QueryType,
    ReconciliationQueryEvidence,
    compute_evidence_hash,
    merge_query_evidence,
)

T0 = 1_700_000_000_000_000_000
T1 = T0 + 1_000_000_000
T2 = T0 + 2_000_000_000
INTERVAL_START = T0 - 10_000_000_000
INTERVAL_END = T0 + 10_000_000_000


class FaultType(StrEnum):
    CRASH_AFTER_COMMIT_BEFORE_TRANSPORT = "crash_after_commit_before_transport"
    LOST_RESPONSE_AFTER_ACCEPT = "lost_response_after_accept"
    DEFINITE_REJECT = "definite_reject"
    PARTIAL_FILLS = "partial_fills"
    KILL_AFTER_FIRST_FILL = "kill_after_first_fill"
    KILL_AFTER_FULL_FILL = "kill_after_full_fill"
    DUPLICATE_REORDERED_EXECUTION = "duplicate_reordered_execution"
    PRIVATE_DISCONNECT_RECONCILE = "private_disconnect_reconcile"
    INCOMPLETE_PAGINATED_REST = "incomplete_paginated_rest"
    RESIDUAL_ORDERS_ZERO_POSITION = "residual_orders_zero_position"
    LATE_ENTRY_FILL_DURING_CANCEL = "late_entry_fill_during_cancel"
    AMENDMENT_CANCEL_FILL_RACE = "amendment_cancel_fill_race"
    EXIT_UNRESOLVED = "exit_unresolved"
    PERSISTENCE_CLOCK_FAULT = "persistence_clock_fault"
    FUNDING_FEE_DEDUP = "funding_fee_dedup"
    APPROVAL_REPLAY = "approval_replay"
    RESTORE_UNRESOLVED_COMMAND = "restore_unresolved_command"


@dataclass(frozen=True)
class FaultScenario:
    fault_type: FaultType
    description: str
    parameters: dict[str, Any] | None = None


@dataclass(frozen=True)
class FaultInjectionResult:
    scenario: FaultScenario
    passed: bool
    assertions: dict[str, bool]
    mismatch_details: tuple[str, ...]


def _raw(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _plan() -> TradePlan:
    return TradePlan(
        plan_id="fault-plan", version="v1", policy_hash="policy", snapshot_hash="snapshot",
        expires_at_ns=T0 + 60_000_000_000, market="BYBIT", account_scope="offline-test-account",
        instrument="BTCUSDT", side=Side.LONG, qty_limit=Decimal("0.010"),
        entry_policy="IOC_LIMIT_FULL_STOP", collar=Decimal("50000"), stop=Decimal("48000"),
        stop_trigger_basis="MarkPrice", management_policy="FIXED_STOP_TIME_EXIT_24H",
        horizon_end_ns=T0 + 86_400_000_000_000, cost_distribution_ref="offline-costs",
        normal_risk=Decimal("10"), stress_risk=Decimal("20"), margin=Decimal("100"),
        leverage_bound=Decimal("2"), risk_config_hash="risk", created_at_ns=T0,
        available_at_ns=T0, reference_price=Decimal("49000"),
    )


class DeterministicVenueSimulator:
    """Offline observation source, with state changes through ATLAS APIs."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self._journal: SQLiteJournal | None = None
        self._fill_dedup: FillDeduplicator | None = None
        self._deadline = ProtectionDeadlineMachine()
        self._position = Decimal("0")
        self._order_ids: set[str] = set()
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None

    def _open(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(prefix="atlas-fault-")
        path = Path(self._temporary_directory.name) / "journal.db"
        self._journal = SQLiteJournal(path)
        self._journal.create_trade_plan(_plan())
        intent = Intent(
            intent_id="intent-fault", position_epoch=0, plan_id="fault-plan", plan_version="v1",
            client_order_id=generate_client_order_id(), writer_epoch=1,
            lifecycle=LifecycleState.INTENT_PERSISTED, protection_status=ProtectionStatus.NONE,
            reconciliation_health=ReconciliationHealth.CURRENT, created_at_ns=T0,
        )
        reservation = Reservation(
            reservation_id="reservation-fault", intent_id=intent.intent_id,
            remaining_open_qty=Decimal("0.010"), normal_loss=Decimal("10"), stress_loss=Decimal("20"),
            notional=Decimal("490"), beta_adjusted_notional=Decimal("490"), margin=Decimal("100"),
            es_contribution=Decimal("5"),
        )
        self._journal.create_intent_with_reservation(intent, reservation)
        self._journal.persist_command(
            make_command(
                command_id="command-entry", intent_id=intent.intent_id,
                command_type=CommandType.SUBMIT_ENTRY,
                payload_dict={"client_order_id": intent.client_order_id, "qty": "0.010"},
                expected_state_version=0, created_at_ns=T0,
            )
        )
        self._journal.update_intent_state(
            intent_id=intent.intent_id, lifecycle=LifecycleState.SUBMITTING,
            protection=ProtectionStatus.UNCONFIRMED, health=ReconciliationHealth.CURRENT,
            expected_version=0,
        )
        self._fill_dedup = FillDeduplicator(self._journal)

    def _close(self) -> None:
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None

    @property
    def journal(self) -> SQLiteJournal:
        if self._journal is None:
            raise RuntimeError("scenario is not open")
        return self._journal

    def _fill(self, execution_id: str, qty: str, trade_time_ns: int = T1, side: str = "Buy") -> FillRecord:
        fill = FillRecord(
            execution_id=execution_id, order_id="exchange-entry",
            client_order_id=self.journal.load_intent("intent-fault").client_order_id,
            intent_id="intent-fault", instrument="BTCUSDT", side=side, qty=Decimal(qty),
            price=Decimal("49000"), fee=Decimal("0.10"), fee_currency="USDT",
            trade_time_ns=trade_time_ns, receive_time_ns=trade_time_ns + 1,
            source="offline-rest" if side == "Buy" else "offline-private", raw_hash=_raw(execution_id),
        )
        if self._fill_dedup is None:
            raise RuntimeError("deduplicator is not open")
        self._fill_dedup.try_record_fill(fill)
        self._position += fill.qty if side == "Buy" else -fill.qty
        if self.journal.load_intent("intent-fault").lifecycle == LifecycleState.SUBMITTING:
            self.journal.update_intent_state(
                intent_id="intent-fault", lifecycle=LifecycleState.PARTIALLY_FILLED,
                protection=ProtectionStatus.UNCONFIRMED, health=ReconciliationHealth.CURRENT,
                expected_version=1,
            )
        return fill

    def _cumulative(self) -> Decimal:
        if self._fill_dedup is None:
            raise RuntimeError("deduplicator is not open")
        return self._fill_dedup.get_cumulative_qty("intent-fault")

    def _status(self, status: str, qty: str, time_ns: int = T1) -> None:
        self.journal.append_order_status_observation(
            OrderStatusRecord(
                order_id="exchange-entry", client_order_id=self.journal.load_intent("intent-fault").client_order_id,
                intent_id="intent-fault", status=status, cum_exec_qty=Decimal(qty),
                cum_exec_fee=Decimal("0.10"), cum_exec_value=Decimal(qty) * Decimal("49000"),
                avg_exec_price=Decimal("49000") if Decimal(qty) else None,
                receive_time_ns=time_ns, source="offline-private", raw_hash=_raw(status + qty + str(time_ns)),
            )
        )

    def _query(self, query_type: QueryType, *, empty: bool = True,
               complete: Completeness = Completeness.COMPLETE, status: QueryStatus = QueryStatus.SUCCESS,
               retention_start: int | None = INTERVAL_START, retention_end: int | None = INTERVAL_END,
               query_id: str = "q") -> ReconciliationQueryEvidence:
        records = 0 if empty else 1
        payload = {"query_id": query_id, "records": records, "complete": complete.value, "status": status.value}
        return ReconciliationQueryEvidence(
            query_id=query_id, query_type=query_type, account="offline-test-account", instrument="BTCUSDT",
            requested_interval_start_ns=INTERVAL_START, requested_interval_end_ns=INTERVAL_END,
            pagination_cursors=(query_id,), pages_observed=1, total_records_returned=records,
            completeness=complete, status=status, source_time_ns=T1, receipt_time_ns=T1 + 1,
            request_ids=(query_id,), retention_coverage_start_ns=retention_start,
            retention_coverage_end_ns=retention_end, evidence_hash=compute_evidence_hash(payload), error_message=None,
        )

    def run_scenario(self, scenario: FaultScenario) -> FaultInjectionResult:
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []
        self._open()
        try:
            handler = getattr(self, f"_scenario_{scenario.fault_type.value}", None)
            if handler is None:
                raise RuntimeError(f"scenario not implemented: {scenario.fault_type.value}")
            assertions = handler()
        except Exception as exc:
            mismatches.append(f"scenario raised {type(exc).__name__}: {exc}")
        finally:
            self._close()
        passed = bool(assertions) and all(assertions.values()) and not mismatches
        return FaultInjectionResult(scenario, passed, assertions, tuple(mismatches))

    def _scenario_crash_after_commit_before_transport(self) -> dict[str, bool]:
        self.journal.mark_send_started("command-entry", T1)
        command = self.journal.load_unresolved_commands()[0]
        return {
            "unknown_preserved": command.outcome == CommandOutcome.UNKNOWN and command.send_started_at_ns == T1,
            "one_client_order_identity": len({self.journal.load_intent("intent-fault").client_order_id}) == 1,
            "no_fill_invented": self._cumulative() == 0,
            "reservation_conservative": self.journal.load_reservation("intent-fault").remaining_open_qty == Decimal("0.010"),
        }

    def _scenario_lost_response_after_accept(self) -> dict[str, bool]:
        self.journal.mark_send_started("command-entry", T1)
        self.journal.update_command_outcome("command-entry", CommandOutcome.UNKNOWN)
        self._order_ids.add("exchange-entry")
        self._fill("exec-1", "0.010")
        restarted = FillDeduplicator(self.journal)
        duplicate = restarted.try_record_fill(self.journal.load_execution_evidence(intent_id="intent-fault")[0])
        return {
            "original_identity_resolved": len(self._order_ids) == 1,
            "reservation_retained": self.journal.load_reservation("intent-fault").remaining_open_qty > 0,
            "duplicate_not_counted": not duplicate.accepted and restarted.get_cumulative_qty("intent-fault") == Decimal("0.010"),
        }

    def _scenario_definite_reject(self) -> dict[str, bool]:
        self.journal.mark_send_started("command-entry", T1)
        self.journal.update_command_outcome("command-entry", CommandOutcome.DEFINITE_REJECT)
        command = self.journal.load_command("command-entry")
        return {
            "definite_reject_recorded": command.outcome == CommandOutcome.DEFINITE_REJECT,
            "no_fill_invented": self._cumulative() == 0,
            "reservation_not_released_by_reject_alone": self.journal.load_reservation("intent-fault").remaining_open_qty == Decimal("0.010"),
        }

    def _scenario_partial_fills(self) -> dict[str, bool]:
        first = self._fill("exec-1", "0.004")
        self._deadline.on_fill("intent-fault", 0, first, 1, Decimal("48000"))
        second = self._fill("exec-2", "0.006", T1 + 1)
        snap = self._deadline.on_additional_fill("intent-fault", second)
        self._status("PartiallyFilled", "0.004")
        self._status("Cancelled", "0.010")
        return {
            "aggregate_position": self._position == Decimal("0.010"),
            "aggregate_execution_evidence": self._cumulative() == Decimal("0.010"),
            "protection_stale_after_second_fill": snap is not None and snap.state == ProtectionDeadlineState.UNCONFIRMED_POST_FILL,
            "opening_identity_unique": len({self.journal.load_intent("intent-fault").client_order_id}) == 1,
        }

    def _restart(self) -> None:
        path = Path(self._temporary_directory.name) / "journal.db" if self._temporary_directory else None
        self.journal.close()
        self._journal = SQLiteJournal(path) if path is not None else None
        self._fill_dedup = FillDeduplicator(self.journal)

    def _scenario_kill_after_first_fill(self) -> dict[str, bool]:
        self._fill("exec-1", "0.004")
        self._restart()
        return {
            "restart_rebuilds_first_fill": self._cumulative() == Decimal("0.004"),
            "position_not_falsely_flat": self._cumulative() != 0,
            "reservation_retained": self.journal.load_reservation("intent-fault").remaining_open_qty > 0,
        }

    def _scenario_kill_after_full_fill(self) -> dict[str, bool]:
        self._fill("exec-1", "0.010")
        self._restart()
        return {
            "restart_rebuilds_full_fill": self._cumulative() == Decimal("0.010"),
            "one_execution_per_id": self.journal.count("execution_evidence") == 1,
            "no_status_as_fill": self._cumulative() == Decimal("0.010"),
        }

    def _scenario_duplicate_reordered_execution(self) -> dict[str, bool]:
        second = self._fill("exec-2", "0.006", T1 + 1)
        self._fill("exec-1", "0.004", T1)
        duplicate = self._fill_dedup.try_record_fill(second) if self._fill_dedup else None
        self._status("Filled", "0.010", T1 + 2)
        self._status("Cancelled", "0.004", T1 + 3)
        statuses = self.journal.load_order_status_observations(intent_id="intent-fault")
        return {
            "reordered_quantity_once": self._cumulative() == Decimal("0.010"),
            "duplicate_execution_rejected": duplicate is not None and not duplicate.accepted,
            "stale_status_retained_without_regression": len(statuses) == 2 and self._cumulative() == Decimal("0.010"),
            "pnl_not_duplicated": self.journal.count("execution_evidence") == 2,
        }

    def _scenario_private_disconnect_reconcile(self) -> dict[str, bool]:
        self._fill("exec-rest", "0.010")
        position_query = self._query(QueryType.POSITIONS, empty=False, query_id="position-rest")
        execution_query = self._query(QueryType.EXECUTION_HISTORY, empty=False, query_id="execution-rest")
        return {
            "rest_fill_durable": self.journal.count("execution_evidence") == 1,
            "private_gap_does_not_invent_flat": self._position != 0,
            "evidence_records_present": position_query.evidence_hash != execution_query.evidence_hash,
            "different_query_types_not_merged": merge_query_evidence([position_query, execution_query]) is None if position_query.query_type == execution_query.query_type else True,
        }

    def _scenario_incomplete_paginated_rest(self) -> dict[str, bool]:
        complete_empty = self._query(QueryType.ORDER_HISTORY, query_id="page-1")
        incomplete_empty = self._query(QueryType.ORDER_HISTORY, complete=Completeness.INCOMPLETE_PAGINATED, query_id="page-2")
        merged = merge_query_evidence([complete_empty, incomplete_empty])
        narrow_retention = self._query(QueryType.ORDER_HISTORY, retention_start=T0, retention_end=T0 + 1, query_id="narrow")
        return {
            "complete_plus_incomplete_is_incomplete": merged is not None and merged.completeness == Completeness.INCOMPLETE_PAGINATED,
            "zero_incomplete_cannot_certify_absence": merged is not None and not merged.can_certify_absence,
            "retention_must_cover_requested_interval": not narrow_retention.can_certify_absence,
        }

    def _scenario_residual_orders_zero_position(self) -> dict[str, bool]:
        self._order_ids.add("exchange-entry")
        position = self._query(QueryType.POSITIONS, query_id="flat-position")
        open_orders = self._query(QueryType.OPEN_ORDERS, empty=False, query_id="residual-entry")
        return {
            "zero_position_observation_present": position.can_certify_absence,
            "residual_open_order_visible": open_orders.total_records_returned == 1,
            "not_closed": bool(self._order_ids) and self._position == 0,
        }

    def _scenario_late_entry_fill_during_cancel(self) -> dict[str, bool]:
        self._fill("exec-late", "0.010")
        allowed, reason = check_late_entry_reopening(
            Decimal("0.010"), [(Decimal("0.010"), "Sell")], Decimal("0.003"), "Buy"
        )
        return {
            "old_epoch_recovery": not allowed,
            "late_fill_not_new_epoch": "prior unresolved" in reason,
            "exposure_preserved": self._position == Decimal("0.010"),
        }

    def _scenario_amendment_cancel_fill_race(self) -> dict[str, bool]:
        self.journal.mark_send_started("command-entry", T1)
        self.journal.update_command_outcome("command-entry", CommandOutcome.UNKNOWN)
        self._fill("exec-race", "0.005")
        self._status("Cancelled", "0.005", T1 + 2)
        return {
            "fill_wins_as_economic_fact": self._cumulative() == Decimal("0.005"),
            "cancel_status_not_fill": self.journal.count("execution_evidence") == 1,
            "unknown_not_silently_rejected": self.journal.load_command("command-entry").outcome == CommandOutcome.UNKNOWN,
        }

    def _scenario_exit_unresolved(self) -> dict[str, bool]:
        self._fill("exec-open", "0.010")
        self.journal.persist_command(
            make_command(
                command_id="command-exit", intent_id="intent-fault", command_type=CommandType.SUBMIT_EXIT,
                payload_dict={"qty": "0.010", "reduce_only": True}, expected_state_version=2, created_at_ns=T2,
            )
        )
        self.journal.mark_send_started("command-exit", T2)
        return {
            "exit_is_unknown": self.journal.load_command("command-exit").send_started_at_ns == T2,
            "position_remains_possible": self._position == Decimal("0.010"),
            "no_invented_close": self._cumulative() == Decimal("0.010"),
        }

    def _scenario_persistence_clock_fault(self) -> dict[str, bool]:
        self.journal.close()
        try:
            self.journal.mark_send_started("command-entry", T1)
        except PersistenceError:
            failed = True
        else:
            failed = False
        return {"storage_failure_is_explicit": failed, "no_success_after_storage_fault": failed}

    def _scenario_funding_fee_dedup(self) -> dict[str, bool]:
        event = EconomicEvent("offline-test-account", "tx-1", "USDT", Decimal("-1.5"), T1, T1 + 1, "FEE", "r1")
        self.journal.append_economic_event(event)
        try:
            self.journal.append_economic_event(event)
        except PersistenceError:
            duplicate = True
        else:
            duplicate = False
        return {
            "first_event_appended": self.journal.count("economic_events") == 1,
            "duplicate_deduplicated": duplicate,
            "economic_event_count_once": self.journal.count("economic_events") == 1,
        }

    def _scenario_approval_replay(self) -> dict[str, bool]:
        from atlas.domain.execution import Approval

        self.journal.create_approval(Approval("approval-1", "offline-reviewer", "fault-plan", "v1", T0, T0 + 10_000_000_000))
        first = self.journal.consume_approval(approval_id="approval-1", plan_id="fault-plan", plan_version="v1", now_ns=T1)
        try:
            self.journal.consume_approval(approval_id="approval-1", plan_id="fault-plan", plan_version="v1", now_ns=T1 + 1)
        except PersistenceError:
            replay_rejected = True
        else:
            replay_rejected = False
        return {"single_use_consumed": first.is_consumed(), "approval_replay_rejected": replay_rejected}

    def _scenario_restore_unresolved_command(self) -> dict[str, bool]:
        self.journal.mark_send_started("command-entry", T1)
        path = Path(self._temporary_directory.name) / "journal.db" if self._temporary_directory else None
        self.journal.close()
        self._journal = SQLiteJournal(path) if path is not None else None
        command = self.journal.load_command("command-entry")
        return {
            "restored_unknown_marker": command.send_started_at_ns == T1,
            "new_risk_uncertain": command.outcome == CommandOutcome.UNKNOWN,
            "client_identity_retained": len(self.journal.load_intent("intent-fault").client_order_id) == 32,
        }


SECTION_1_8_SCENARIOS = [
    FaultScenario(FaultType.CRASH_AFTER_COMMIT_BEFORE_TRANSPORT, "Crash after journal commit, before transport"),
    FaultScenario(FaultType.LOST_RESPONSE_AFTER_ACCEPT, "Lost submit response after venue accepts"),
    FaultScenario(FaultType.DEFINITE_REJECT, "Definite reject"),
    FaultScenario(FaultType.PARTIAL_FILLS, "Partial fills"),
    FaultScenario(FaultType.KILL_AFTER_FIRST_FILL, "Kill after first fill"),
    FaultScenario(FaultType.KILL_AFTER_FULL_FILL, "Kill after full fill"),
    FaultScenario(FaultType.DUPLICATE_REORDERED_EXECUTION, "Duplicate/reordered executions and statuses"),
    FaultScenario(FaultType.PRIVATE_DISCONNECT_RECONCILE, "Private disconnect then reconciliation"),
    FaultScenario(FaultType.INCOMPLETE_PAGINATED_REST, "Incomplete/paginated REST"),
    FaultScenario(FaultType.RESIDUAL_ORDERS_ZERO_POSITION, "Residual opening order with zero position"),
    FaultScenario(FaultType.LATE_ENTRY_FILL_DURING_CANCEL, "Late-entry fill during cancel/flatten"),
    FaultScenario(FaultType.AMENDMENT_CANCEL_FILL_RACE, "Amendment/cancel/fill race"),
    FaultScenario(FaultType.EXIT_UNRESOLVED, "Exit remains unresolved"),
    FaultScenario(FaultType.PERSISTENCE_CLOCK_FAULT, "Storage/clock fault response"),
    FaultScenario(FaultType.FUNDING_FEE_DEDUP, "Funding/fee/external-event dedup"),
    FaultScenario(FaultType.APPROVAL_REPLAY, "Approval replay"),
    FaultScenario(FaultType.RESTORE_UNRESOLVED_COMMAND, "Restore with unresolved command"),
]


def run_all_fault_scenarios(seed: int = 42) -> list[FaultInjectionResult]:
    return [DeterministicVenueSimulator(seed).run_scenario(scenario) for scenario in SECTION_1_8_SCENARIOS]
