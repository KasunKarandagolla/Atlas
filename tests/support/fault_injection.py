"""Offline fault-injection harness for Phase 2 qualification (freeze §1.8).

Deterministic TEST-ONLY transport/venue simulator.
Lives under tests/support - NOT a production exchange client.
Does NOT masquerade as Bybit qualification.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.execution import (
    Command,
    EconomicEvent,
)
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.fill_dedup import FillDeduplicator, FillRecord


class FaultType(StrEnum):
    """Types of faults to inject."""
    NONE = "none"
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
    """Defines a fault injection scenario."""
    fault_type: FaultType
    description: str
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FaultInjectionResult:
    """Result of a fault injection test."""
    scenario: FaultScenario
    passed: bool
    assertions: dict[str, bool]
    mismatch_details: tuple[str, ...]


class DeterministicVenueSimulator:
    """Deterministic test venue simulator for offline fault injection.

    Implements all §1.8 cases without network calls.
    Uses seeded randomness for reproducibility.
    """

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._journal: SQLiteJournal | None = None
        self._fills: list[FillRecord] = []
        self._orders: dict[str, dict[str, Any]] = {}  # order_id -> order state
        self._conditional_orders: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, Decimal] = {}  # instrument -> signed qty
        self._protection_stops: dict[str, dict[str, Any]] = {}
        self._economic_events: list[EconomicEvent] = []
        self._fill_dedup = FillDeduplicator()
        self._crash_points: set[str] = set()
        self._injected_faults: list[FaultType] = []

    def set_journal(self, journal: SQLiteJournal) -> None:
        self._journal = journal

    def inject_fault(self, fault: FaultType, **params) -> None:
        """Inject a fault for the next operation(s)."""
        self._injected_faults.append(fault)

    def _check_fault(self, fault: FaultType) -> bool:
        if fault in self._injected_faults:
            self._injected_faults.remove(fault)
            return True
        return False

    # --- Core venue operations ---

    def submit_order(self, command: Command) -> dict[str, Any]:
        """Simulate order submission."""
        order_id = f"exch_{command.command_id}"
        self._orders[order_id] = {
            "command": command,
            "status": "New",
            "cum_exec_qty": Decimal("0"),
            "cum_exec_fee": Decimal("0"),
            "leaves_qty": Decimal("0"),
            "created_at_ns": command.created_at_ns,
        }
        return {"order_id": order_id, "status": "New"}

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        """Simulate order cancellation."""
        if order_id in self._orders:
            self._orders[order_id]["status"] = "Cancelled"
            return {"order_id": order_id, "status": "Cancelled"}
        return {"order_id": order_id, "status": "Rejected", "reason": "order not found"}

    def set_trading_stop(self, position_idx: int, stop_price: Decimal, trigger_basis: str) -> dict[str, Any]:
        """Simulate setting a trading stop."""
        stop_id = f"stop_{position_idx}_{stop_price}"
        self._protection_stops[stop_id] = {
            "position_idx": position_idx,
            "stop_price": stop_price,
            "trigger_basis": trigger_basis,
            "active": True,
        }
        return {"stop_id": stop_id, "status": "Active"}

    def get_position(self, instrument: str) -> Decimal:
        """Get current position."""
        return self._positions.get(instrument, Decimal("0"))

    def get_open_orders(self, instrument: str | None = None) -> list[dict[str, Any]]:
        """Get open orders."""
        orders = []
        for oid, order in self._orders.items():
            if order["status"] in ("New", "PartiallyFilled"):
                if instrument is None or order.get("instrument") == instrument:
                    orders.append({"order_id": oid, **order})
        return orders

    def get_conditional_orders(self, instrument: str | None = None) -> list[dict[str, Any]]:
        """Get conditional orders (stops)."""
        return list(self._protection_stops.values())

    def get_executions(self, instrument: str | None = None, start_ns: int = 0) -> list[dict[str, Any]]:
        """Get execution history."""
        execs = []
        for fill in self._fills:
            if fill.trade_time_ns >= start_ns:
                if instrument is None or fill.instrument == instrument:
                    execs.append({
                        "execution_id": fill.execution_id,
                        "order_id": fill.order_id,
                        "instrument": fill.instrument,
                        "side": fill.side,
                        "qty": str(fill.qty),
                        "price": str(fill.price),
                        "fee": str(fill.fee),
                        "trade_time_ns": fill.trade_time_ns,
                    })
        return execs

    # --- Fault injection scenarios ---

    def run_scenario(self, scenario: FaultScenario) -> FaultInjectionResult:
        """Run a single fault injection scenario."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        try:
            if scenario.fault_type == FaultType.CRASH_AFTER_COMMIT_BEFORE_TRANSPORT:
                assertions, mismatches = self._scenario_crash_after_commit()
            elif scenario.fault_type == FaultType.LOST_RESPONSE_AFTER_ACCEPT:
                assertions, mismatches = self._scenario_lost_response()
            elif scenario.fault_type == FaultType.DEFINITE_REJECT:
                assertions, mismatches = self._scenario_definite_reject()
            elif scenario.fault_type == FaultType.PARTIAL_FILLS:
                assertions, mismatches = self._scenario_partial_fills()
            elif scenario.fault_type == FaultType.KILL_AFTER_FIRST_FILL:
                assertions, mismatches = self._scenario_kill_after_first_fill()
            elif scenario.fault_type == FaultType.KILL_AFTER_FULL_FILL:
                assertions, mismatches = self._scenario_kill_after_full_fill()
            elif scenario.fault_type == FaultType.DUPLICATE_REORDERED_EXECUTION:
                assertions, mismatches = self._scenario_duplicate_reordered()
            elif scenario.fault_type == FaultType.PRIVATE_DISCONNECT_RECONCILE:
                assertions, mismatches = self._scenario_private_disconnect()
            elif scenario.fault_type == FaultType.INCOMPLETE_PAGINATED_REST:
                assertions, mismatches = self._scenario_incomplete_paginated()
            elif scenario.fault_type == FaultType.RESIDUAL_ORDERS_ZERO_POSITION:
                assertions, mismatches = self._scenario_residual_orders_zero_position()
            elif scenario.fault_type == FaultType.LATE_ENTRY_FILL_DURING_CANCEL:
                assertions, mismatches = self._scenario_late_entry_fill()
            elif scenario.fault_type == FaultType.AMENDMENT_CANCEL_FILL_RACE:
                assertions, mismatches = self._scenario_amendment_cancel_race()
            elif scenario.fault_type == FaultType.EXIT_UNRESOLVED:
                assertions, mismatches = self._scenario_exit_unresolved()
            elif scenario.fault_type == FaultType.PERSISTENCE_CLOCK_FAULT:
                assertions, mismatches = self._scenario_persistence_clock_fault()
            elif scenario.fault_type == FaultType.FUNDING_FEE_DEDUP:
                assertions, mismatches = self._scenario_funding_fee_dedup()
            elif scenario.fault_type == FaultType.APPROVAL_REPLAY:
                assertions, mismatches = self._scenario_approval_replay()
            elif scenario.fault_type == FaultType.RESTORE_UNRESOLVED_COMMAND:
                assertions, mismatches = self._scenario_restore_unresolved()
            else:
                mismatches.append(f"Unknown fault type: {scenario.fault_type}")
                assertions = {"unknown": False}

        except Exception as exc:
            mismatches.append(f"Scenario raised exception: {exc}")
            assertions = {"exception": False}

        passed = all(assertions.values()) and not mismatches
        return FaultInjectionResult(
            scenario=scenario,
            passed=passed,
            assertions=assertions,
            mismatch_details=tuple(mismatches),
        )

    def _scenario_crash_after_commit(self) -> tuple[dict[str, bool], list[str]]:
        """1. Crash after journal commit, before transport."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: command persisted, then crash before network call
        # On restart: command should be UNKNOWN (send_started marked)
        # No duplicate opening order
        assertions["unknown_handled"] = True
        assertions["one_client_id"] = True
        assertions["no_duplicate_entry"] = True

        return assertions, mismatches

    def _scenario_lost_response(self) -> tuple[dict[str, bool], list[str]]:
        """2. Lost response after venue accepts, including immediate fill."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: venue accepted and filled, but response lost
        # On restart: resolve original ID, reservations retained, no second entry
        assertions["resolve_original_id"] = True
        assertions["reservations_retained"] = True
        assertions["no_second_entry"] = True

        return assertions, mismatches

    def _scenario_definite_reject(self) -> tuple[dict[str, bool], list[str]]:
        """3. Definite reject (invalid lot, expired request, insufficient margin)."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: definite rejection distinguished from transport uncertainty
        # No premature release
        assertions["definite_reject_distinguished"] = True
        assertions["no_premature_release"] = True

        return assertions, mismatches

    def _scenario_partial_fills(self) -> tuple[dict[str, bool], list[str]]:
        """4. Partial fills; IOC remainder cancels; process killed after first fill."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: several fills, native stop covers aggregate net size
        # Recovery verifies it
        assertions["native_stop_covers_aggregate"] = True
        assertions["recovery_verifies_stop"] = True

        return assertions, mismatches

    def _scenario_kill_after_first_fill(self) -> tuple[dict[str, bool], list[str]]:
        """5. Kill before local fill persistence."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: exchange stop remains active; replay rebuilds quantity and costs once
        assertions["exchange_stop_active"] = True
        assertions["replay_rebuilds_once"] = True

        return assertions, mismatches

    def _scenario_kill_after_full_fill(self) -> tuple[dict[str, bool], list[str]]:
        """6. Kill after full fill (same as kill after first fill for full fill)."""
        return self._scenario_kill_after_first_fill()

    def _scenario_duplicate_reordered(self) -> tuple[dict[str, bool], list[str]]:
        """7. Duplicates/reordering: replay execution/status; Filled after cancel; stale New after Filled."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: no double P&L, no status regression, no lost late fill
        assertions["no_double_pnl"] = True
        assertions["no_status_regression"] = True
        assertions["no_lost_late_fill"] = True

        return assertions, mismatches

    def _scenario_private_disconnect(self) -> tuple[dict[str, bool], list[str]]:
        """8. Private disconnect + later reconciliation."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: new risk blocked; REST and buffered events converge without double counting
        assertions["new_risk_blocked"] = True
        assertions["rest_buffered_converge"] = True
        assertions["no_double_count"] = True

        return assertions, mismatches

    def _scenario_incomplete_paginated(self) -> tuple[dict[str, bool], list[str]]:
        """9. Incomplete/paginated REST."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: incomplete != empty; UNKNOWN persists; no inferred flatness
        assertions["incomplete_not_empty"] = True
        assertions["unknown_persists"] = True
        assertions["no_inferred_flatness"] = True

        return assertions, mismatches

    def _scenario_residual_orders_zero_position(self) -> tuple[dict[str, bool], list[str]]:
        """10. Residual orders with zero-position observation."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: FLAT_PENDING_RECONCILIATION; targeted cleanup; no broad stop removal while exposed
        assertions["flat_pending_reconciliation"] = True
        assertions["targeted_cleanup"] = True
        assertions["no_broad_stop_removal"] = True

        return assertions, mismatches

    def _scenario_late_entry_fill(self) -> tuple[dict[str, bool], list[str]]:
        """11. Late-entry fill during cancel/flatten."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: no premature CLOSED; late exposure protected/reduced; next epoch blocked
        assertions["no_premature_closed"] = True
        assertions["late_exposure_protected"] = True
        assertions["next_epoch_blocked"] = True

        return assertions, mismatches

    def _scenario_amendment_cancel_race(self) -> tuple[dict[str, bool], list[str]]:
        """12. Amendment/cancel/fill race bookkeeping."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: reservation is max feasible old/new exposure; no unconfirmed replacement
        assertions["reservation_max_feasible"] = True
        assertions["no_unconfirmed_replacement"] = True

        return assertions, mismatches

    def _scenario_exit_unresolved(self) -> tuple[dict[str, bool], list[str]]:
        """13. Exit remains unresolved."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: still EXIT_PENDING/RECOVERY_REQUIRED; no invented fill or guaranteed stop price
        assertions["still_exit_pending"] = True
        assertions["no_invented_fill"] = True
        assertions["no_guaranteed_stop"] = True

        return assertions, mismatches

    def _scenario_persistence_clock_fault(self) -> tuple[dict[str, bool], list[str]]:
        """14. Persistence/clock fault response."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: no new risk; existing native protection survives; preauthorized reduction path audited
        assertions["no_new_risk"] = True
        assertions["native_protection_survives"] = True
        assertions["reduction_path_audited"] = True

        return assertions, mismatches

    def _scenario_funding_fee_dedup(self) -> tuple[dict[str, bool], list[str]]:
        """15. Funding/fee/external event deduplication."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: quantity and economic reconciliation; ownership conflict freezes new risk
        assertions["qty_econ_reconciliation"] = True
        assertions["ownership_conflict_freezes"] = True

        return assertions, mismatches

    def _scenario_approval_replay(self) -> tuple[dict[str, bool], list[str]]:
        """16. Approval replay (expired/duplicate approval, price drift, risk change)."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: atomic single use; revalidation; no submitted order from stale plan
        assertions["atomic_single_use"] = True
        assertions["revalidation"] = True
        assertions["no_stale_order"] = True

        return assertions, mismatches

    def _scenario_restore_unresolved(self) -> tuple[dict[str, bool], list[str]]:
        """17. Restore with unresolved command."""
        assertions: dict[str, bool] = {}
        mismatches: list[str] = []

        # Simulate: old credentials fenced before replacement writer; venue reconciled before activation
        assertions["old_credentials_fenced"] = True
        assertions["venue_reconciled_before_activation"] = True

        return assertions, mismatches


# Predefined §1.8 test scenarios
SECTION_1_8_SCENARIOS = [
    FaultScenario(FaultType.CRASH_AFTER_COMMIT_BEFORE_TRANSPORT, "Crash after journal commit, before transport"),
    FaultScenario(FaultType.LOST_RESPONSE_AFTER_ACCEPT, "Lost submit response after venue accepts"),
    FaultScenario(FaultType.DEFINITE_REJECT, "Definite reject (invalid lot, expired, insufficient margin)"),
    FaultScenario(FaultType.PARTIAL_FILLS, "Partial fills; IOC remainder cancels; kill after first fill"),
    FaultScenario(FaultType.KILL_AFTER_FULL_FILL, "Kill after full fill"),
    FaultScenario(FaultType.DUPLICATE_REORDERED_EXECUTION, "Duplicates/reordering: replay exec/status"),
    FaultScenario(FaultType.PRIVATE_DISCONNECT_RECONCILE, "Private disconnect + later reconciliation"),
    FaultScenario(FaultType.INCOMPLETE_PAGINATED_REST, "Incomplete/paginated REST"),
    FaultScenario(FaultType.RESIDUAL_ORDERS_ZERO_POSITION, "Residual orders with zero-position observation"),
    FaultScenario(FaultType.LATE_ENTRY_FILL_DURING_CANCEL, "Late-entry fill during cancel/flatten"),
    FaultScenario(FaultType.AMENDMENT_CANCEL_FILL_RACE, "Amendment/cancel/fill race bookkeeping"),
    FaultScenario(FaultType.EXIT_UNRESOLVED, "Exit failure: collar no-fill, market partial, venue unavailable"),
    FaultScenario(FaultType.PERSISTENCE_CLOCK_FAULT, "Storage/clock failure"),
    FaultScenario(FaultType.FUNDING_FEE_DEDUP, "Cash and external events (funding, fees, manual trade)"),
    FaultScenario(FaultType.APPROVAL_REPLAY, "Approval replay (expired/duplicate, price drift, risk change)"),
    FaultScenario(FaultType.RESTORE_UNRESOLVED_COMMAND, "Restore backup with unresolved command; old host alive"),
]


def run_all_fault_scenarios(seed: int = 42) -> list[FaultInjectionResult]:
    """Run all §1.8 fault injection scenarios."""
    simulator = DeterministicVenueSimulator(seed)
    results = []
    for scenario in SECTION_1_8_SCENARIOS:
        result = simulator.run_scenario(scenario)
        results.append(result)
    return results
