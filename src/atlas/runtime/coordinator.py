"""Deterministic atlas-crypto-live coordinator (Phase 1, incapable of submission).

Startup: BOOT -> acquire writer -> open/verify journal -> RECOVERING ->
restore unresolved intents/commands/reservations -> classify
(no marker => UNSENT; marker w/o terminal evidence => UNKNOWN) ->
block new risk while uncertainty exists -> evaluate prerequisites ->
READY only when every locally provable requirement passes.

Absence of venue verification keeps new risk disabled. No READY faking.
No order-submission interface exists in this module by design.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from atlas.domain.capability import CapabilityContract
from atlas.domain.enums import (
    CommandOutcome,
    HealthState,
    ProtectionStatus,
    ReconciliationHealth,
)
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal

from .connectivity import PrivateVerification, PublicVenueHealth
from .health import HealthSnapshot, new_risk_allowed
from .prerequisites import IdentityExpectation, ObservedAccountState, check_identity
from .recovery import RecoveryCertificate, RecoveryDecision, run_recovery
from .writer_lock import WriterLock, WriterOwnership

TERMINAL_COMMAND_OUTCOMES = frozenset(
    {
        CommandOutcome.DEFINITE_ACCEPT.value,
        CommandOutcome.DEFINITE_REJECT.value,
        CommandOutcome.RECONCILED.value,
    }
)

UNRESOLVED_COMMAND_OUTCOMES = frozenset(
    {
        CommandOutcome.UNSENT.value,
        CommandOutcome.UNKNOWN.value,
        CommandOutcome.DEFINITE_ACCEPT.value,
    }
)


@dataclass(frozen=True)
class BootResult:
    state: HealthState
    certificate: RecoveryCertificate
    new_risk_allowed: bool
    reasons: tuple[str, ...]
    unresolved_intents: int
    unknown_commands: int
    unresolved_commands: int


@dataclass(frozen=True)
class RiskPolicyDecision:
    """Typed, injected permission for current RiskPolicy evidence."""

    approved: bool
    policy_hash: str | None = None
    evidence_ref: str | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.approved, bool):
            raise ValueError("risk policy approval must be bool")
        if self.approved and (not self.policy_hash or not self.evidence_ref):
            raise ValueError("approved risk policy requires policy hash and evidence reference")


def _valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def _check_capability_gate(
    capability_contract: CapabilityContract | None,
    capability_hash: str,
    all_qualified: bool,
    assisted_enabled: bool,
    now_ns: int,
) -> tuple[bool, list[str]]:
    """Validate the supplied contract and its binding hash fail-closed."""
    reasons: list[str] = []
    if capability_contract is None:
        reasons.append("typed capability contract not supplied")
        return False, reasons
    if capability_hash != capability_contract.contract_hash():
        reasons.append("supplied capability hash does not bind to supplied contract")
    runtime = capability_contract.runtime
    venue = capability_contract.venue
    if runtime.distribution != "nautilus_trader":
        reasons.append("runtime distribution mismatch (expected nautilus_trader)")
    if runtime.version != "2.0.0rc5":
        reasons.append("runtime version mismatch (expected 2.0.0rc5)")
    if runtime.source_commit != "1b0a49d2792a9432a3aca3fcb617ce7a630d905e":
        reasons.append("runtime source commit mismatch")
    if not _valid_sha256(runtime.installed_artifact_sha256):
        reasons.append("installed artifact SHA256 is not valid")
    if not _valid_sha256(runtime.dependency_lock_sha256):
        reasons.append("dependency lock SHA256 is not valid")
    if runtime.python_platform_abi != "cpython-312-x86_64-linux-gnu":
        reasons.append("Python 3.12 platform ABI mismatch")
    if venue.environment != "testnet":
        reasons.append("venue environment must be testnet")
    if venue.product != "linear":
        reasons.append("venue product must be linear")
    if venue.position_mode != "one_way":
        reasons.append("venue position mode must be one_way")
    if tuple(venue.supported_symbols) != ("BTCUSDT", "ETHUSDT"):
        reasons.append("venue symbols must be exactly BTCUSDT + ETHUSDT")
    if "isolated" not in venue.account_generation_and_margin_mode.lower():
        reasons.append("account profile is not isolated-compatible")
    if not venue.account_identity_hash.strip() or venue.account_identity_hash.startswith("REQUIRED"):
        reasons.append("venue account identity is a placeholder")
    if not capability_contract.capabilities.all_supported():
        reasons.extend(capability_contract.capabilities.blocking_reasons())
    if not capability_contract.assisted_enabled:
        reasons.append("capability contract assisted_enabled must be true for positive gate")
    if not all_qualified:
        reasons.append("not all capabilities qualified (all_qualified=False)")
    if not assisted_enabled:
        reasons.append("assisted execution disabled (assisted_enabled=False)")
    return len(reasons) == 0, reasons


def boot(
    *,
    journal_path: str,
    lock_path: str,
    capability_hash: str,
    all_qualified: bool,
    assisted_enabled: bool,
    identity_expected: IdentityExpectation,
    identity_observed: ObservedAccountState,
    public_health: PublicVenueHealth,
    private_verification: PrivateVerification,
    now_ns: int,
    max_public_staleness_ns: int,
    clock_uncertainty_ns: int = 0,
    writer: WriterLock | None = None,
    journal: SQLiteJournal | None = None,
    capability_contract: CapabilityContract | None = None,
    risk_policy: RiskPolicyDecision | None = None,
    runtime_instance_id: str | None = None,
) -> BootResult:
    """Run the deterministic Phase 1 boot sequence. Raises on fatal errors."""
    owned = writer if writer is not None else WriterLock(lock_path)
    close_writer = writer is None
    # If writer was passed in, assume it's already acquired (ownership exists)
    # Otherwise, acquire it now
    ownership: WriterOwnership
    if writer is None:
        try:
            ownership = owned.acquire()
        except Exception as exc:
            raise PersistenceError(f"writer acquisition failed: {exc}") from exc
    else:
        ownership_opt = owned.ownership
        if ownership_opt is None:
            raise PersistenceError("passed writer has no ownership; must acquire before calling boot")
        ownership = ownership_opt
    close_journal = journal is None
    try:
        journal = journal or SQLiteJournal(journal_path)
    except Exception:
        if close_writer:
            try:
                owned.release()
            except Exception:
                pass
        raise
    try:
        schema_version = journal.schema_version() or 0
        intents = journal.load_unresolved_intents()
        commands = journal.load_unresolved_commands()
        unknown = [
            c
            for c in commands
            if c.send_started_at_ns is not None and c.outcome.value not in TERMINAL_COMMAND_OUTCOMES
        ]
        unresolved_commands_list = [
            c for c in commands if c.outcome.value in UNRESOLVED_COMMAND_OUTCOMES
        ]
        # Reconciliation health: conflicted if configured so; stale if public stale;
        # CURRENT only when public fresh. Private unverified does not fake CURRENT.
        if not public_health.is_fresh(now_ns=now_ns, max_staleness_ns=max_public_staleness_ns, clock_uncertainty_ns=clock_uncertainty_ns):
            recon = ReconciliationHealth.STALE
        else:
            recon = ReconciliationHealth.CURRENT
        ident = check_identity(identity_expected, identity_observed)
        prereq_ok = bool(ident.ok) and private_verification.state.value == "VERIFIED"
        # Phase 1: private never VERIFIED without credentials => prereq_ok False.
        cert = run_recovery(
            recovery_run_id=runtime_instance_id or uuid.uuid4().hex,
            writer_id=ownership.writer_id,
            writer_epoch=ownership.writer_epoch,
            journal_schema_version=schema_version,
            unresolved_intent_ids=tuple(i.intent_id for i in intents),
            unresolved_command_ids=tuple(c.command_id for c in unresolved_commands_list),
            unknown_command_ids=tuple(c.command_id for c in unknown),
            reconciliation_health=recon,
            protection_uncertainty_summary="protection unverified in Phase 1",
            started_at_ns=now_ns,
            ended_at_ns=now_ns,
            venue_observations_obtained=False,
            venue_evidence_refs=(),
            prerequisites_ok=prereq_ok and recon == ReconciliationHealth.CURRENT,
            protection_evidence=None,
        )
        # Capability/assisted gate: must pass for any new risk allowance
        cap_gate_ok, cap_reasons = _check_capability_gate(
            capability_contract, capability_hash, all_qualified, assisted_enabled, now_ns
        )
        if capability_contract is not None:
            if capability_contract.venue.account_identity_hash != identity_observed.account_identity_hash:
                cap_gate_ok = False
                cap_reasons.append("observed account identity does not match capability contract")
        policy = risk_policy or RiskPolicyDecision(approved=False, reason="no current RiskPolicy evidence supplied")
        if not policy.approved:
            cap_reasons.append(policy.reason or "current RiskPolicy permission denied")
        if cert.decision == RecoveryDecision.READY:
            state = HealthState.READY
        elif cert.decision == RecoveryDecision.RECOVERY_REQUIRED:
            state = HealthState.ENTRY_HALTED  # Not EMERGENCY_EXIT (no qualified exit yet)
        else:
            state = HealthState.RECOVERING
        snap = HealthSnapshot(
            state=state,
            writer_owned=True,
            account_matched=bool(ident.ok),
            data_current=public_health.is_fresh(
                now_ns=now_ns, max_staleness_ns=max_public_staleness_ns, clock_uncertainty_ns=clock_uncertainty_ns
            ),
            reconciliation=recon,
            protection=ProtectionStatus.UNCONFIRMED,
            has_open_exposure=bool(intents),
            drawdown_stop_active=False,
        )
        gate = new_risk_allowed(snap)
        # Combine all gates: recovery gate AND capability/assisted gate
        all_reasons = list(gate.reasons) + cap_reasons
        new_risk = gate.allowed and cap_gate_ok and policy.approved
        return BootResult(
            state=state,
            certificate=cert,
            new_risk_allowed=new_risk,
            reasons=tuple(all_reasons),
            unresolved_intents=len(intents),
            unknown_commands=len(unknown),
            unresolved_commands=len(unresolved_commands_list),
        )
    finally:
        if close_journal:
            try:
                journal.close()
            except Exception:
                pass
        if close_writer:
            try:
                owned.release()
            except Exception:
                pass
