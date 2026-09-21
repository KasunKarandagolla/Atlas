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

from atlas.domain.capability import initial_unverified_fixture
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


def _check_capability_gate(
    capability_hash: str,
    all_qualified: bool,
    assisted_enabled: bool,
    now_ns: int,
) -> tuple[bool, list[str]]:
    """Check capability/assisted gate. Phase 1: always blocks new risk."""
    reasons: list[str] = []
    if not all_qualified:
        reasons.append("not all capabilities qualified (all_qualified=False)")
    if not assisted_enabled:
        reasons.append("assisted execution disabled (assisted_enabled=False)")
    # Phase 1: capabilities are UNVERIFIED, assisted is false => always blocks
    # Even if both were true, Phase 1 has no venue evidence => still blocks
    fixture = initial_unverified_fixture()
    if not fixture.capabilities.all_supported():
        reasons.append("required capabilities not SUPPORTED (Phase 1: all UNVERIFIED)")
    if fixture.assisted_enabled:
        reasons.append("assisted_enabled must be false in Phase 1")
    # Capability contract identity must match frozen V1 (distribution, version, commit)
    if fixture.runtime.distribution != "nautilus_trader":
        reasons.append("runtime distribution mismatch (expected nautilus_trader)")
    if fixture.runtime.version != "2.0.0rc5":
        reasons.append("runtime version mismatch (expected 2.0.0rc5)")
    if fixture.runtime.source_commit != "1b0a49d2792a9432a3aca3fcb617ce7a630d905e":
        reasons.append("runtime source commit mismatch (expected frozen V1 commit)")
    # Placeholder evidence fields must not be placeholders for production READY
    for field_name, val in (
        ("runtime.installed_artifact_sha256", fixture.runtime.installed_artifact_sha256),
        ("runtime.dependency_lock_sha256", fixture.runtime.dependency_lock_sha256),
        ("runtime.python_platform_abi", fixture.runtime.python_platform_abi),
        ("venue.account_identity_hash", fixture.venue.account_identity_hash),
        ("venue.account_generation_and_margin_mode", fixture.venue.account_generation_and_margin_mode),
    ):
        if val in ("REQUIRED", "REQUIRED_AT_INSTALL", "CONFIGURED") or not val.strip():
            reasons.append(f"capability contract {field_name} holds placeholder {val!r}")
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
    try:
        journal = SQLiteJournal(journal_path)
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
            recovery_run_id=uuid.uuid4().hex,
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
        )
        # Capability/assisted gate: must pass for any new risk allowance
        cap_gate_ok, cap_reasons = _check_capability_gate(
            capability_hash, all_qualified, assisted_enabled, now_ns
        )
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
        new_risk = gate.allowed and cap_gate_ok
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
        try:
            journal.close()
        except Exception:
            pass
        if close_writer:
            try:
                owned.release()
            except Exception:
                pass
