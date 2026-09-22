"""Deterministic offline Phase-6 fixtures; no venue connection or order submission."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from conftest import T0, add_intent, completed_run, terminal_entry

from atlas.domain.enums import (
    HealthState,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
    Side,
)
from atlas.domain.execution import Approval, Command, Intent, Reservation, generate_client_order_id
from atlas.domain.risk import engineering_default_policy
from atlas.domain.trade_plan import TradePlan
from atlas.persistence.sqlite import SQLiteJournal
from atlas.risk.engine import AccountState
from atlas.runtime.assisted_control import (
    AssistedRevalidationEvidence,
    DispatchAck,
    account_state_hash,
)
from atlas.runtime.flat_certificate import certify_flat_from_journal
from atlas.runtime.health import HealthSnapshot
from atlas.runtime.protection_evidence import ProtectionEvidence
from atlas.runtime.recovery import RecoveryCertificate, recover_from_persisted_run

PLAN_EXPIRY_NS = T0 + 60_000_000_000
APPROVAL_EXPIRY_NS = T0 + 50_000_000_000
NOW_NS = T0 + 10_000_000_000


def make_plan(journal: SQLiteJournal, *, plan_id: str = "plan-p6", version: str = "v1",
              account_scope: str = "acct", expires_at_ns: int = PLAN_EXPIRY_NS) -> TradePlan:
    risk_policy = engineering_default_policy()
    plan = TradePlan(
        plan_id=plan_id,
        version=version,
        policy_hash=risk_policy.policy_hash(),
        snapshot_hash="snapshot-p6",
        expires_at_ns=expires_at_ns,
        market="BYBIT",
        account_scope=account_scope,
        instrument="BTCUSDT",
        side=Side.LONG,
        qty_limit=Decimal("0.01"),
        entry_policy="IOC_LIMIT_FULL_STOP",
        collar=Decimal("50000"),
        stop=Decimal("48000"),
        stop_trigger_basis="MarkPrice",
        management_policy="FIXED_STOP_TIME_EXIT_24H",
        horizon_end_ns=T0 + 24 * 3_600_000_000_000,
        cost_distribution_ref="cost-p6",
        normal_risk=Decimal("10"),
        stress_risk=Decimal("25"),
        margin=Decimal("100"),
        leverage_bound=Decimal("2"),
        risk_config_hash=risk_policy.policy_hash(),
        created_at_ns=T0,
        available_at_ns=T0,
        reference_price=Decimal("49000"),
    )
    journal.create_trade_plan(plan)
    return plan


def make_approval(journal: SQLiteJournal, plan: TradePlan, *, approval_id: str = "approval-p6",
                  user_identity: str = "user-1", plan_version: str | None = None,
                  expires_at_ns: int = APPROVAL_EXPIRY_NS) -> Approval:
    approval = Approval(
        approval_id=approval_id,
        user_identity=user_identity,
        plan_id=plan.plan_id,
        plan_version=plan_version or plan.version,
        approved_at_ns=T0,
        expires_at_ns=expires_at_ns,
    )
    journal.create_approval(approval)
    return approval


def persist_ready_recovery(journal: SQLiteJournal, *, recovery_run_id: str = "recovery-p6",
                           runtime_instance_id: str = "runtime", writer_id: str = "writer",
                           writer_epoch: int = 1) -> RecoveryCertificate:
    """Build a genuine READY certificate from persisted flat/reconciliation evidence."""
    intent = add_intent(journal)
    terminal_entry(journal, intent)
    journal.update_intent_state(
        intent_id=intent.intent_id,
        lifecycle=LifecycleState.CLOSED,
        protection=ProtectionStatus.NONE,
        health=ReconciliationHealth.CURRENT,
        expected_version=0,
    )
    completed_run(journal)
    flat = certify_flat_from_journal(
        journal=journal,
        certification_id="flat-p6",
        reconciliation_run_id="run-1",
        intent_id=intent.intent_id,
        current_writer_id=writer_id,
        current_writer_epoch=writer_epoch,
        account_identity_hash="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        now_ns=T0 + 21,
    )
    journal.release_reservation_from_flat_certificate(flat)
    return recover_from_persisted_run(
        journal=journal,
        recovery_run_id=recovery_run_id,
        reconciliation_run_id="run-1",
        runtime_instance_id=runtime_instance_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        started_at_ns=T0,
        ended_at_ns=T0 + 22,
        prerequisites_ok=True,
        flat_certificate_id=flat.certification_id,
    )


def evidence(plan: TradePlan, *, now_ns: int = NOW_NS, recovery_run_id: str = "recovery-p6",
             runtime_instance_id: str = "runtime", writer_id: str = "writer", writer_epoch: int = 1,
             account_scope: str = "acct", instrument: str = "BTCUSDT",
             bid: Decimal = Decimal("48999"), ask: Decimal = Decimal("49001"), mark: Decimal = Decimal("49000"),
             quote_at_ns: int | None = None, mark_at_ns: int | None = None,
             account: AccountState | None = None, signed_position_qty: Decimal = Decimal("0"),
             health: HealthSnapshot | None = None, requested_quantity: Decimal | None = None,
             policy=None):
    resolved_account = account or AccountState(
        eligible_equity=Decimal("100000"),
        margin_available=Decimal("100000"),
        current_margin=Decimal("0"),
        drawdown=Decimal("0"),
        existing_es=Decimal("0"),
    )
    resolved_health = health or HealthSnapshot(
        state=HealthState.READY,
        writer_owned=True,
        account_matched=True,
        data_current=True,
        reconciliation=ReconciliationHealth.CURRENT,
        protection=ProtectionStatus.NONE,
        has_open_exposure=False,
        drawdown_stop_active=False,
    )
    return AssistedRevalidationEvidence(
        now_ns=now_ns,
        runtime_instance_id=runtime_instance_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        recovery_run_id=recovery_run_id,
        instrument=instrument,
        account_scope=account_scope,
        account_snapshot_hash=account_state_hash(resolved_account),
        account=resolved_account,
        bid=bid,
        ask=ask,
        mark=mark,
        quote_at_ns=quote_at_ns if quote_at_ns is not None else now_ns - 500_000_000,
        mark_at_ns=mark_at_ns if mark_at_ns is not None else now_ns - 500_000_000,
        tick=Decimal("1"),
        risk_policy=policy or engineering_default_policy(),
        signed_position_qty=signed_position_qty,
        health=resolved_health,
        requested_quantity=requested_quantity,
    )


def open_intent(journal: SQLiteJournal, *, intent_id: str = "open-intent",
                lifecycle: LifecycleState = LifecycleState.OPEN_PROTECTED) -> Intent:
    plan = make_plan(journal, plan_id=f"plan-{intent_id}")
    intent = Intent(
        intent_id=intent_id,
        position_epoch=0,
        plan_id=plan.plan_id,
        plan_version=plan.version,
        client_order_id=generate_client_order_id(),
        writer_epoch=1,
        lifecycle=lifecycle,
        protection_status=ProtectionStatus.CONFIRMED,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=T0,
    )
    reservation = Reservation(
        reservation_id=f"res-{intent_id}",
        intent_id=intent_id,
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("10"),
        stress_loss=Decimal("25"),
        notional=Decimal("490"),
        beta_adjusted_notional=Decimal("490"),
        margin=Decimal("100"),
        es_contribution=Decimal("0"),
    )
    journal.create_intent_with_reservation(intent, reservation)
    return intent


@dataclass
class FakeNautilusPort:
    ack: DispatchAck
    journal: SQLiteJournal | None = None
    calls: list[str] = field(default_factory=list, init=False)
    observed: list[tuple[str, str, int]] = field(default_factory=list, init=False)

    def dispatch(self, command: Command) -> DispatchAck:
        if self.journal is not None:
            stored = self.journal.load_command(command.command_id)
            intent = self.journal.load_intent(stored.intent_id)
            reservation = self.journal.load_reservation(intent.intent_id)
            if stored.send_started_at_ns is None:
                raise AssertionError("command send marker missing")
            if intent.lifecycle not in (LifecycleState.SUBMITTING, LifecycleState.EXIT_PENDING):
                raise AssertionError("intent not in a dispatch-prepared lifecycle at port call")
            if reservation.remaining_open_qty <= 0:
                raise AssertionError("reservation missing at port call")
            self.observed.append((stored.command_id, intent.lifecycle.value, reservation.version))
        self.calls.append(command.command_id)
        return self.ack


@dataclass
class FakeProtectionPort:
    calls: list[tuple[int, Decimal, Decimal, str]] = field(default_factory=list, init=False)

    def ensure_full_stop(self, position_epoch: int, expected_signed_qty, stop_price, trigger_basis: str):
        self.calls.append((position_epoch, Decimal(expected_signed_qty), Decimal(stop_price), trigger_basis))
        return ProtectionEvidence(
            account_ref="acct",
            instrument="BTCUSDT",
            position_epoch=position_epoch,
            desired_stop_version=1,
            observed_signed_qty=Decimal(expected_signed_qty),
            full_position_semantics=True,
            stop_price=Decimal(stop_price),
            trigger_basis=trigger_basis,
            closing_only_behavior=True,
            position_view_evidence_ids=("fake-position",),
            conditional_order_view_evidence_ids=("fake-conditional",),
            observation_time_ns=T0 + 1,
            receive_time_ns=T0 + 2,
            status=ProtectionStatus.CONFIRMED,
            market_stop_semantics=True,
        )

    def read_protection(self, account: str, instrument: str, position_idx: int = 0):
        raise AssertionError("read_protection not used in Phase-6 offline shell tests")

    def read_economic_events(self, cursor: str | None, overlap_start: int):
        return ()
