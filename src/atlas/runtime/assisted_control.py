"""Thin Phase-6 assisted control shell over existing durable control primitives.

This module does not execute live or testnet orders.  It prepares plan-bound,
durable commands and exposes the dispatch gate for offline state-machine tests.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from atlas.domain.capability import CapabilityContract
from atlas.domain.enums import (
    CommandOutcome,
    CommandType,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
    Side,
)
from atlas.domain.execution import Approval, Command, Intent, Reservation, generate_client_order_id
from atlas.domain.money import canonical_decimal_str, ensure_decimal
from atlas.domain.risk import RiskPolicy
from atlas.domain.time import ensure_utc_ns
from atlas.domain.trade_plan import TradePlan
from atlas.domain.transitions import is_allowed_lifecycle_transition
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.risk.engine import AccountState, RiskVector, evaluate_reservation
from atlas.runtime.health import HealthSnapshot, new_risk_allowed
from atlas.runtime.no_reversal import check_no_reversal
from atlas.runtime.protection_evidence import verify_protection
from atlas.runtime.protection_port import BybitProtectionPort
from atlas.runtime.recovery import RecoveryCertificate, RecoveryDecision
from atlas.runtime.wire_contract import (
    ExitWireContract,
    build_entry_wire_contract,
    build_exit_wire_contract,
    build_market_exit_wire_contract,
)
from atlas.strategy.policy import entry_collar

MAX_QUOTE_AGE_NS = 1_000_000_000
MAX_PROTECTION_STALENESS_NS = 2_000_000_000


class DispatchAck(StrEnum):
    DEFINITE_ACCEPT = "DEFINITE_ACCEPT"
    DEFINITE_REJECT = "DEFINITE_REJECT"
    UNKNOWN = "UNKNOWN"


class NautilusCommandPort(Protocol):
    """Narrow injected boundary; Nautilus remains the operational order authority."""

    def dispatch(self, command: Command) -> DispatchAck: ...


def account_state_hash(account: AccountState) -> str:
    def encoded(value: object) -> str:
        return canonical_decimal_str(value) if isinstance(value, Decimal) else str(value)

    payload = {name: encoded(getattr(account, name)) for name in account.__dataclass_fields__}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class AssistedRevalidationEvidence:
    now_ns: int
    runtime_instance_id: str
    writer_id: str
    writer_epoch: int
    recovery_run_id: str
    instrument: str
    account_scope: str
    account_snapshot_hash: str
    account: AccountState
    bid: Decimal
    ask: Decimal
    mark: Decimal
    quote_at_ns: int
    mark_at_ns: int
    tick: Decimal
    risk_policy: RiskPolicy
    signed_position_qty: Decimal
    health: HealthSnapshot
    account_at_ns: int
    requested_quantity: Decimal | None = None
    existing_reservations: tuple[RiskVector, ...] = ()

    def __post_init__(self) -> None:
        ensure_utc_ns(self.now_ns, field="now_ns")
        ensure_utc_ns(self.quote_at_ns, field="quote_at_ns")
        ensure_utc_ns(self.mark_at_ns, field="mark_at_ns")
        ensure_utc_ns(self.account_at_ns, field="account_at_ns")
        for name in ("runtime_instance_id", "writer_id", "recovery_run_id", "instrument", "account_scope",
                     "account_snapshot_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-blank")
        for name in ("bid", "ask", "mark", "tick", "signed_position_qty"):
            object.__setattr__(self, name, ensure_decimal(getattr(self, name), field=name))
        if self.requested_quantity is not None:
            object.__setattr__(self, "requested_quantity",
                               ensure_decimal(self.requested_quantity, field="requested_quantity"))
        if self.bid <= 0 or self.ask <= 0 or self.mark <= 0 or self.tick <= 0 or self.bid > self.ask:
            raise ValueError("quote/mark/tick evidence invalid")
        if self.account_snapshot_hash != account_state_hash(self.account):
            raise ValueError("account snapshot hash does not bind the supplied account state")
        if not isinstance(self.risk_policy, RiskPolicy):
            raise ValueError("typed RiskPolicy required")
        if not isinstance(self.health, HealthSnapshot):
            raise ValueError("typed HealthSnapshot required")
        object.__setattr__(self, "existing_reservations", tuple(self.existing_reservations))


@dataclass(frozen=True)
class RevalidationResult:
    ok: bool
    reasons: tuple[str, ...]
    quantity: Decimal
    entry_price: Decimal
    candidate_risk: RiskVector


@dataclass(frozen=True)
class AssistedControlResult:
    status: str
    reasons: tuple[str, ...] = ()
    approval: Approval | None = None
    intent: Intent | None = None
    command: Command | None = None
    command_outcome: CommandOutcome | None = None
    quantity: Decimal | None = None
    entry_price: Decimal | None = None


@dataclass(frozen=True)
class AssistedControlStatus:
    paused: bool
    assisted_enabled: bool
    capability_qualified: bool
    latest_recovery_decision: str | None
    new_risk_allowed: bool
    unresolved_intents: int
    unknown_commands: int
    unresolved_commands: int


def validate_approval(*, journal: SQLiteJournal, plan: TradePlan, approval_id: str,
                      user_identity: str, now_ns: int) -> Approval:
    """Validate plan/version/user/expiry binding before any consumption."""
    stored_plan = journal.load_trade_plan(plan.plan_id)
    if stored_plan.version != plan.version or stored_plan.plan_hash() != plan.plan_hash():
        raise PersistenceError("stored TradePlan does not match the supplied plan/version")
    approval = journal.load_approval(approval_id)
    if approval.plan_id != plan.plan_id or approval.plan_version != plan.version:
        raise PersistenceError("approval is not bound to this TradePlan version")
    if approval.user_identity != user_identity:
        raise PersistenceError("approval user identity mismatch")
    if approval.consumed_at_ns is not None:
        raise PersistenceError("approval already consumed")
    if now_ns >= approval.expires_at_ns:
        raise PersistenceError("approval expired")
    if now_ns >= plan.expires_at_ns:
        raise PersistenceError("TradePlan expired")
    if approval.expires_at_ns > plan.expires_at_ns:
        raise PersistenceError("approval expiry extends beyond TradePlan authority")
    if now_ns < plan.available_at_ns:
        raise PersistenceError("TradePlan not yet available")
    return approval


def revalidate_plan(*, journal: SQLiteJournal, plan: TradePlan,
                    evidence: AssistedRevalidationEvidence) -> RevalidationResult:
    """Re-check current execution/capital conditions without rerunning strategy science."""
    reasons: list[str] = []
    certificate = journal.load_recovery_certificate(evidence.recovery_run_id)
    if certificate is None:
        reasons.append("persisted recovery certificate missing")
    else:
        reasons.extend(_recovery_reasons(certificate=certificate, journal=journal, evidence=evidence))
    unresolved_intents = journal.load_unresolved_intents()
    if unresolved_intents:
        reasons.append("existing unresolved intent")
    for command in journal.load_unresolved_commands():
        if command.outcome in (CommandOutcome.UNSENT, CommandOutcome.UNKNOWN, CommandOutcome.DEFINITE_ACCEPT):
            reasons.append("existing unresolved opening command")
            break
    if plan.is_expired(evidence.now_ns):
        reasons.append("TradePlan expired")
    if evidence.instrument != plan.instrument:
        reasons.append("instrument identity mismatch")
    if evidence.account_scope != plan.account_scope:
        reasons.append("account identity mismatch")
    if plan.risk_config_hash != evidence.risk_policy.policy_hash():
        reasons.append("current RiskPolicy hash differs from approved plan")
    if evidence.risk_policy.policy_effective_at_ns > evidence.now_ns:
        reasons.append("current RiskPolicy is not yet effective")
    if evidence.quote_at_ns > evidence.now_ns or evidence.now_ns - evidence.quote_at_ns > MAX_QUOTE_AGE_NS:
        reasons.append("stale quote")
    if evidence.mark_at_ns > evidence.now_ns or evidence.now_ns - evidence.mark_at_ns > MAX_QUOTE_AGE_NS:
        reasons.append("stale mark")
    if evidence.account_at_ns > evidence.now_ns:
        reasons.append("future account snapshot")
    elif evidence.now_ns - evidence.account_at_ns > MAX_QUOTE_AGE_NS:
        reasons.append("stale account snapshot")
    if evidence.signed_position_qty != 0:
        reasons.append("instrument is not flat/reconciled for a new opening intent")
    quantity = plan.qty_limit if evidence.requested_quantity is None else min(plan.qty_limit,
                                                                            evidence.requested_quantity)
    if quantity <= 0 or quantity > plan.qty_limit:
        reasons.append("requested quantity outside approved plan quantity")
    collar_price = entry_collar(plan.side, evidence.bid, evidence.ask, evidence.tick)
    if plan.side is Side.LONG and collar_price > plan.collar:
        reasons.append("current collar exceeds approved LONG collar")
    if plan.side is Side.SHORT and collar_price < plan.collar:
        reasons.append("current collar exceeds approved SHORT collar")
    if plan.side is Side.LONG and plan.stop >= evidence.mark:
        reasons.append("LONG stop is not protective of current mark")
    if plan.side is Side.SHORT and plan.stop <= evidence.mark:
        reasons.append("SHORT stop is not protective of current mark")
    if quantity > 0:
        candidate = RiskVector(
            normal_loss=plan.normal_risk,
            stress_loss=plan.stress_risk,
            notional=quantity * evidence.mark,
            beta_notional=quantity * evidence.mark,
            margin=plan.margin,
            es_contribution=Decimal("0"),
            remaining_open_qty=quantity,
        )
        totals = journal.reservation_totals()
        journal_pending = RiskVector(
            normal_loss=totals["normal_loss"],
            stress_loss=totals["stress_loss"],
            notional=totals["notional"],
            beta_notional=totals["beta_adjusted_notional"],
            margin=totals["margin"],
            es_contribution=totals["es_contribution"],
            remaining_open_qty=totals["remaining_open_qty"],
        )
        existing_risk = tuple(evidence.existing_reservations) + (journal_pending,)
        decision = evaluate_reservation(evidence.risk_policy, evidence.account, existing_risk,
                                        candidate, leverage=plan.leverage_bound)
        if not decision.accepted:
            reasons.extend(decision.reasons or ("current risk gate failed",))
    else:
        candidate = RiskVector(Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"),
                               Decimal("0"), Decimal("0"))
    risk_gate = new_risk_allowed(evidence.health)
    if not risk_gate.allowed:
        reasons.extend(risk_gate.reasons)
    return RevalidationResult(not reasons, tuple(dict.fromkeys(reasons)), quantity, collar_price, candidate)


def _recovery_reasons(*, certificate: RecoveryCertificate, journal: SQLiteJournal,
                      evidence: AssistedRevalidationEvidence) -> list[str]:
    reasons: list[str] = []
    if certificate.decision is not RecoveryDecision.READY:
        reasons.append("recovery certificate is not READY")
    if certificate.reconciliation_health is not ReconciliationHealth.CURRENT:
        reasons.append("recovery reconciliation is not CURRENT")
    if certificate.runtime_instance_id != evidence.runtime_instance_id:
        reasons.append("recovery certificate runtime instance mismatch")
    if certificate.writer_id != evidence.writer_id or certificate.writer_epoch != evidence.writer_epoch:
        reasons.append("recovery certificate writer identity/epoch mismatch")
    if certificate.journal_schema_version != (journal.schema_version() or 0):
        reasons.append("recovery certificate journal schema mismatch")
    if certificate.unresolved_intents or certificate.unresolved_commands or certificate.unknown_commands:
        reasons.append("recovery certificate contains unresolved work")
    return reasons


def _apply_dispatch_ack(*, journal: SQLiteJournal, command_id: str, ack: DispatchAck,
                        now_ns: int) -> Command:
    command = journal.load_command(command_id)
    intent = journal.load_intent(command.intent_id)
    if ack is DispatchAck.DEFINITE_ACCEPT:
        return journal.update_command_outcome(command_id, CommandOutcome.DEFINITE_ACCEPT)
    if ack is DispatchAck.DEFINITE_REJECT:
        updated = journal.update_command_outcome(command_id, CommandOutcome.DEFINITE_REJECT)
        if intent.lifecycle in (LifecycleState.SUBMITTING, LifecycleState.SUBMIT_UNKNOWN):
            journal.update_intent_state(intent_id=intent.intent_id, lifecycle=LifecycleState.FLAT_PENDING_RECONCILIATION,
                                        protection=ProtectionStatus.NONE,
                                        health=ReconciliationHealth.CURRENT,
                                        expected_version=intent.state_version)
        return updated
    if ack is DispatchAck.UNKNOWN:
        if intent.lifecycle is LifecycleState.SUBMITTING:
            journal.update_intent_state(intent_id=intent.intent_id, lifecycle=LifecycleState.SUBMIT_UNKNOWN,
                                        protection=ProtectionStatus.NONE,
                                        health=ReconciliationHealth.CURRENT,
                                        expected_version=intent.state_version)
        return command
    raise ValueError("unknown dispatch acknowledgement")


def _dispatch_with_port(*, journal: SQLiteJournal, command_id: str, port: NautilusCommandPort,
                        now_ns: int) -> Command:
    """Internal offline state-machine path; public callers must pass the assisted gate first."""
    command = journal.load_command(command_id)
    if command.send_started_at_ns is None:
        command = journal.mark_send_started(command_id, now_ns)
    ack = port.dispatch(command)
    return _apply_dispatch_ack(journal=journal, command_id=command_id, ack=ack, now_ns=now_ns)


class AssistedControlShell:
    """Thin approval/revalidation/persistence shell; never a second OMS."""

    def __init__(self, *, journal: SQLiteJournal, runtime_instance_id: str, writer_id: str, writer_epoch: int,
                 capability_contract: CapabilityContract | None = None, capability_hash: str = "",
                 all_qualified: bool = False, assisted_enabled: bool = False, paused: bool = False,
                 nautilus_port: NautilusCommandPort | None = None):
        self.journal = journal
        self.runtime_instance_id = runtime_instance_id
        self.writer_id = writer_id
        self.writer_epoch = writer_epoch
        self.capability_contract = capability_contract
        self.capability_hash = capability_hash
        self.all_qualified = all_qualified
        self.assisted_enabled = assisted_enabled
        self.paused = paused
        self.nautilus_port = nautilus_port

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

    def dispatch_gate_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if not self.assisted_enabled:
            reasons.append("assisted_enabled false")
        if not self.all_qualified:
            reasons.append("all_qualified false")
        if self.capability_contract is None:
            reasons.append("typed capability contract not supplied")
        else:
            if self.capability_hash != self.capability_contract.contract_hash():
                reasons.append("capability hash does not bind contract")
            reasons.extend(self.capability_contract.assisted_blockers())
            if not self.capability_contract.assisted_enabled:
                reasons.append("contract assisted_enabled false")
        return tuple(dict.fromkeys(reasons))

    def status(self, *, recovery_run_id: str | None = None) -> AssistedControlStatus:
        certificate = self.journal.load_recovery_certificate(recovery_run_id) if recovery_run_id else None
        commands = self.journal.load_unresolved_commands()
        return AssistedControlStatus(
            paused=self.paused,
            assisted_enabled=self.assisted_enabled,
            capability_qualified=self.dispatch_gate_reasons() == (),
            latest_recovery_decision=certificate.decision.value if certificate is not None else None,
            new_risk_allowed=not self.paused and self.dispatch_gate_reasons() == (),
            unresolved_intents=len(self.journal.load_unresolved_intents()),
            unknown_commands=sum(1 for command in commands if command.outcome is CommandOutcome.UNKNOWN),
            unresolved_commands=sum(1 for command in commands
                                    if command.outcome in (CommandOutcome.UNSENT, CommandOutcome.UNKNOWN,
                                                           CommandOutcome.DEFINITE_ACCEPT)),
        )

    def prepare_entry(self, *, plan: TradePlan, approval_id: str, user_identity: str,
                      evidence: AssistedRevalidationEvidence, intent_id: str | None = None,
                      ) -> AssistedControlResult:
        if self.paused:
            return AssistedControlResult("PAUSED", ("pause latch blocks new opening risk",))
        identity_reasons = []
        if evidence.runtime_instance_id != self.runtime_instance_id:
            identity_reasons.append("evidence runtime instance does not match shell runtime instance")
        if evidence.writer_id != self.writer_id or evidence.writer_epoch != self.writer_epoch:
            identity_reasons.append("evidence writer identity/epoch does not match shell writer")
        if identity_reasons:
            return AssistedControlResult("REVALIDATION_BLOCKED", tuple(identity_reasons))
        try:
            approval = validate_approval(journal=self.journal, plan=plan, approval_id=approval_id,
                                         user_identity=user_identity, now_ns=evidence.now_ns)
        except PersistenceError as exc:
            return AssistedControlResult("APPROVAL_BLOCKED", (str(exc),))
        revalidation = revalidate_plan(journal=self.journal, plan=plan, evidence=evidence)
        if not revalidation.ok:
            return AssistedControlResult("REVALIDATION_BLOCKED", revalidation.reasons, approval=approval)
        next_epoch = self.journal.next_position_epoch()
        intent = Intent(
            intent_id=intent_id or uuid.uuid4().hex,
            position_epoch=next_epoch,
            plan_id=plan.plan_id,
            plan_version=plan.version,
            client_order_id=generate_client_order_id(),
            writer_epoch=self.writer_epoch,
            lifecycle=LifecycleState.INTENT_PERSISTED,
            protection_status=ProtectionStatus.NONE,
            reconciliation_health=ReconciliationHealth.CURRENT,
            created_at_ns=evidence.now_ns,
            state_version=0,
        )
        reservation = Reservation(
            reservation_id=f"res-{intent.intent_id}",
            intent_id=intent.intent_id,
            remaining_open_qty=revalidation.quantity,
            normal_loss=revalidation.candidate_risk.normal_loss,
            stress_loss=revalidation.candidate_risk.stress_loss,
            notional=revalidation.candidate_risk.notional,
            beta_adjusted_notional=revalidation.candidate_risk.beta_notional,
            margin=revalidation.candidate_risk.margin,
            es_contribution=revalidation.candidate_risk.es_contribution,
            version=1,
        )
        try:
            self.journal.consume_approval_with_intent_reservation(
                approval_id=approval.approval_id,
                plan_id=plan.plan_id,
                plan_version=plan.version,
                now_ns=evidence.now_ns,
                intent=intent,
                reservation=reservation,
                expected_position_epoch=next_epoch,
            )
        except PersistenceError as exc:
            return AssistedControlResult("PERSISTENCE_BLOCKED", (str(exc),), approval=approval)
        wire = build_entry_wire_contract(plan, revalidation.entry_price, plan.stop, revalidation.quantity,
                                         intent.client_order_id)
        command = self.journal.prepare_dispatch(
            intent_id=intent.intent_id,
            expected_state_version=intent.state_version,
            expected_reservation_version=reservation.version,
            command_id=uuid.uuid4().hex,
            command_type=CommandType.SUBMIT_ENTRY,
            payload_dict=wire.to_bybit_params(),
            created_at_ns=evidence.now_ns,
            next_lifecycle=LifecycleState.SUBMITTING,
        )
        dispatched_intent = self.journal.load_intent(intent.intent_id)
        gate = self.dispatch_gate_reasons()
        if gate:
            return AssistedControlResult("DISPATCH_BLOCKED", gate, approval=approval, intent=dispatched_intent,
                                         command=command, command_outcome=command.outcome,
                                         quantity=revalidation.quantity, entry_price=revalidation.entry_price)
        if self.nautilus_port is None:
            return AssistedControlResult("DISPATCH_BLOCKED", ("Nautilus command port not supplied",),
                                         approval=approval, intent=dispatched_intent, command=command,
                                         command_outcome=command.outcome, quantity=revalidation.quantity,
                                         entry_price=revalidation.entry_price)
        dispatched = _dispatch_with_port(journal=self.journal, command_id=command.command_id,
                                         port=self.nautilus_port, now_ns=evidence.now_ns)
        return AssistedControlResult("DISPATCHED", (), approval, dispatched_intent, dispatched, dispatched.outcome,
                                     revalidation.quantity, revalidation.entry_price)

    def _risk_reduction_gate(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.capability_contract is None:
            reasons.append("typed capability contract not supplied")
        else:
            if self.capability_hash != self.capability_contract.contract_hash():
                reasons.append("capability hash does not bind contract")
            if self.capability_contract.capabilities.reduce_only_wire_and_matching_enforcement.value != "SUPPORTED":
                reasons.append("reduce-only capability UNVERIFIED")
        return tuple(dict.fromkeys(reasons))

    def _protection_gate(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.capability_contract is None:
            reasons.append("typed capability contract not supplied")
        else:
            if self.capability_hash != self.capability_contract.contract_hash():
                reasons.append("capability hash does not bind contract")
            if (self.capability_contract.capabilities.native_position_stop_read_and_repair_port.value
                    != "SUPPORTED"):
                reasons.append("protection repair capability UNVERIFIED")
        return tuple(dict.fromkeys(reasons))

    def _prepare_exit(self, *, intent_id: str, reconciled_signed_qty: Decimal, quantity: Decimal,
                      exit_price: Decimal | None, now_ns: int, market: bool) -> AssistedControlResult:
        intent = self.journal.load_intent(intent_id)
        plan = self.journal.load_trade_plan(intent.plan_id)
        signed = ensure_decimal(reconciled_signed_qty, field="reconciled_signed_qty")
        qty = ensure_decimal(quantity, field="quantity")
        wire: ExitWireContract = (build_market_exit_wire_contract(plan, qty) if market
                                  else build_exit_wire_contract(plan, exit_price or Decimal("0"), qty))
        reversal = check_no_reversal(signed, qty, wire.side)
        if not reversal.allowed:
            return AssistedControlResult("RISK_REDUCTION_BLOCKED", reversal.violations)
        reservation = self.journal.load_reservation(intent.intent_id)
        try:
            command = self.journal.prepare_dispatch(
                intent_id=intent.intent_id,
                expected_state_version=intent.state_version,
                expected_reservation_version=reservation.version,
                command_id=uuid.uuid4().hex,
                command_type=CommandType.FLATTEN if market else CommandType.SUBMIT_EXIT,
                payload_dict=wire.to_bybit_params(),
                created_at_ns=now_ns,
                next_lifecycle=LifecycleState.EXIT_PENDING,
            )
        except PersistenceError as exc:
            return AssistedControlResult("RISK_REDUCTION_BLOCKED", (str(exc),))
        gate = self._risk_reduction_gate()
        if gate:
            return AssistedControlResult("RISK_REDUCTION_DISPATCH_BLOCKED", gate, intent=intent,
                                         command=command, command_outcome=command.outcome, quantity=qty)
        if self.nautilus_port is None:
            return AssistedControlResult("RISK_REDUCTION_DISPATCH_BLOCKED", ("Nautilus command port not supplied",),
                                         intent=intent, command=command, command_outcome=command.outcome,
                                         quantity=qty)
        dispatched = _dispatch_with_port(journal=self.journal, command_id=command.command_id,
                                         port=self.nautilus_port, now_ns=now_ns)
        return AssistedControlResult("RISK_REDUCTION_DISPATCHED", (), intent=intent, command=dispatched,
                                     command_outcome=dispatched.outcome, quantity=qty)

    def close(self, *, intent_id: str, reconciled_signed_qty: Decimal, quantity: Decimal,
              exit_price: Decimal, now_ns: int) -> AssistedControlResult:
        return self._prepare_exit(intent_id=intent_id, reconciled_signed_qty=reconciled_signed_qty,
                                  quantity=quantity, exit_price=exit_price, now_ns=now_ns, market=False)

    def flatten(self, *, intent_id: str, reconciled_signed_qty: Decimal, now_ns: int) -> AssistedControlResult:
        return self._prepare_exit(intent_id=intent_id, reconciled_signed_qty=reconciled_signed_qty,
                                  quantity=abs(reconciled_signed_qty), exit_price=None, now_ns=now_ns,
                                  market=True)

    def protect(self, *, intent_id: str, reconciled_signed_qty: Decimal, expected_signed_qty: Decimal,
                stop_price: Decimal, protection_port: BybitProtectionPort | None, now_ns: int,
                ) -> AssistedControlResult:
        intent = self.journal.load_intent(intent_id)
        plan = self.journal.load_trade_plan(intent.plan_id)
        reconciled = ensure_decimal(reconciled_signed_qty, field="reconciled_signed_qty")
        qty = ensure_decimal(expected_signed_qty, field="expected_signed_qty")
        stop = ensure_decimal(stop_price, field="stop_price")
        if qty == 0:
            return AssistedControlResult("PROTECTION_BLOCKED", ("owned/reconciled exposure required",))
        if reconciled != qty:
            return AssistedControlResult("PROTECTION_BLOCKED", ("expected quantity does not match reconciled exposure",))
        if stop != plan.stop:
            return AssistedControlResult("PROTECTION_BLOCKED", ("stop price does not match approved plan",))
        if plan.side is Side.LONG and stop >= (plan.reference_price or stop + 1):
            return AssistedControlResult("PROTECTION_BLOCKED", ("LONG stop is not below reference",))
        if plan.side is Side.SHORT and stop <= (plan.reference_price or stop - 1):
            return AssistedControlResult("PROTECTION_BLOCKED", ("SHORT stop is not above reference",))
        reservation = self.journal.load_reservation(intent.intent_id)
        payload = {"position_epoch": intent.position_epoch, "expected_signed_qty": canonical_decimal_str(qty),
                   "stop_price": canonical_decimal_str(stop), "trigger_basis": "MarkPrice"}
        try:
            command = self.journal.prepare_dispatch(
                intent_id=intent.intent_id,
                expected_state_version=intent.state_version,
                expected_reservation_version=reservation.version,
                command_id=uuid.uuid4().hex,
                command_type=CommandType.REPAIR_STOP,
                payload_dict=payload,
                created_at_ns=now_ns,
                next_lifecycle=intent.lifecycle,
            )
        except PersistenceError as exc:
            return AssistedControlResult("PROTECTION_BLOCKED", (str(exc),), intent=intent)
        gate = self._protection_gate()
        if gate:
            return AssistedControlResult("PROTECTION_BLOCKED", gate, intent=intent, command=command,
                                         command_outcome=command.outcome, quantity=qty, entry_price=stop)
        if protection_port is None:
            return AssistedControlResult("PROTECTION_BLOCKED", ("protection port not supplied",), intent=intent,
                                         command=command, command_outcome=command.outcome, quantity=qty,
                                         entry_price=stop)
        return _execute_protection_repair(journal=self.journal, plan=plan, intent=intent,
                                          command_id=command.command_id, expected_signed_qty=qty,
                                          stop_price=stop, protection_port=protection_port, now_ns=now_ns)


def _execute_protection_repair(*, journal: SQLiteJournal, plan: TradePlan, intent: Intent,
                               command_id: str, expected_signed_qty: Decimal, stop_price: Decimal,
                               protection_port: BybitProtectionPort, now_ns: int) -> AssistedControlResult:
    """Durable repair then positive read-back verification; acknowledgement is not proof."""
    command = journal.load_command(command_id)
    if command.send_started_at_ns is None:
        command = journal.mark_send_started(command_id, now_ns)
    try:
        protection_port.ensure_full_stop(intent.position_epoch, expected_signed_qty, stop_price, "MarkPrice")
        observation = protection_port.read_protection(plan.account_scope, plan.instrument, 0)
    except Exception as exc:  # noqa: BLE001 - any port uncertainty stays unconfirmed
        updated = _update_intent_protection_state(journal, intent.intent_id, verified=False)
        return AssistedControlResult("PROTECTION_UNCONFIRMED", (f"protection port uncertainty: {exc}",),
                                     intent=updated, command=journal.load_command(command_id),
                                     command_outcome=CommandOutcome.UNKNOWN, quantity=expected_signed_qty,
                                     entry_price=stop_price)
    if observation is None:
        updated = _update_intent_protection_state(journal, intent.intent_id, verified=False)
        return AssistedControlResult("PROTECTION_UNCONFIRMED", ("protection read-back unavailable",),
                                     intent=updated, command=journal.load_command(command_id),
                                     command_outcome=CommandOutcome.UNKNOWN, quantity=expected_signed_qty,
                                     entry_price=stop_price)
    verification = verify_protection(
        observation,
        expected_position_epoch=intent.position_epoch,
        expected_signed_qty=expected_signed_qty,
        expected_stop_price=stop_price,
        expected_trigger_basis="MarkPrice",
        now_ns=now_ns,
        max_staleness_ns=MAX_PROTECTION_STALENESS_NS,
        account_ref=plan.account_scope,
        instrument=plan.instrument,
        conditional_order_evidence_ids=tuple(observation.evidence_ids),
        conditional_order_view_available=True,
    )
    evidence_id = f"protection-{command.command_id}"
    journal.append_protection_evidence(evidence_id, verification.evidence,
                                       verification.evidence.expected_evidence_hash())
    if not verification.verified:
        updated = _update_intent_protection_state(journal, intent.intent_id, verified=False)
        return AssistedControlResult("PROTECTION_UNCONFIRMED", verification.mismatch_details, intent=updated,
                                     command=journal.load_command(command_id),
                                     command_outcome=CommandOutcome.UNKNOWN, quantity=expected_signed_qty,
                                     entry_price=stop_price)
    updated = _update_intent_protection_state(journal, intent.intent_id, verified=True)
    if updated.protection_status is not ProtectionStatus.CONFIRMED:
        return AssistedControlResult("PROTECTION_UNCONFIRMED", ("no legal durable protected lifecycle",),
                                     intent=updated, command=journal.load_command(command_id),
                                     command_outcome=CommandOutcome.UNKNOWN, quantity=expected_signed_qty,
                                     entry_price=stop_price)
    reconciled = journal.update_command_outcome(command_id, CommandOutcome.RECONCILED)
    return AssistedControlResult("PROTECTED", (), intent=updated, command=reconciled,
                                 command_outcome=reconciled.outcome, quantity=expected_signed_qty,
                                 entry_price=stop_price)


def _update_intent_protection_state(journal: SQLiteJournal, intent_id: str, *, verified: bool) -> Intent:
    current = journal.load_intent(intent_id)
    if verified:
        target = (LifecycleState.OPEN_PROTECTED
                  if current.lifecycle is LifecycleState.OPEN_PROTECTED
                  or is_allowed_lifecycle_transition(current.lifecycle, LifecycleState.OPEN_PROTECTED)
                  else LifecycleState.RECOVERY_REQUIRED)
        protection = (ProtectionStatus.CONFIRMED if target is LifecycleState.OPEN_PROTECTED
                      else ProtectionStatus.UNCONFIRMED)
    else:
        target = (LifecycleState.OPEN_UNPROTECTED
                  if current.lifecycle is LifecycleState.OPEN_UNPROTECTED
                  or is_allowed_lifecycle_transition(current.lifecycle, LifecycleState.OPEN_UNPROTECTED)
                  else LifecycleState.RECOVERY_REQUIRED)
        protection = ProtectionStatus.UNCONFIRMED
    return journal.update_intent_state(intent_id=intent_id, lifecycle=target, protection=protection,
                                      health=ReconciliationHealth.CURRENT,
                                      expected_version=current.state_version)
