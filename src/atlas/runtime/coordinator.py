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
from .writer_lock import WriterLock

TERMINAL_COMMAND_OUTCOMES = frozenset(
    {
        CommandOutcome.DEFINITE_ACCEPT.value,
        CommandOutcome.DEFINITE_REJECT.value,
        CommandOutcome.RECONCILED.value,
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
    writer: WriterLock | None = None,
) -> BootResult:
    """Run the deterministic Phase 1 boot sequence. Raises on fatal errors."""
    _ = (capability_hash, all_qualified, assisted_enabled)
    owned = writer if writer is not None else WriterLock(lock_path)
    close_writer = writer is None
    try:
        ownership = owned.acquire()
    except Exception as exc:
        raise PersistenceError(f"writer acquisition failed: {exc}") from exc
    try:
        journal = SQLiteJournal(journal_path)
    except Exception:
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
        # Reconciliation health: conflicted if configured so; stale if public stale;
        # CURRENT only when public fresh. Private unverified does not fake CURRENT.
        if not public_health.is_fresh(now_ns=now_ns, max_staleness_ns=max_public_staleness_ns):
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
            unresolved_command_ids=tuple(c.command_id for c in commands),
            unknown_command_ids=tuple(c.command_id for c in unknown),
            reconciliation_health=recon,
            protection_uncertainty_summary="protection unverified in Phase 1",
            started_at_ns=now_ns,
            ended_at_ns=now_ns,
            venue_observations_obtained=False,
            prerequisites_ok=prereq_ok and recon == ReconciliationHealth.CURRENT,
        )
        if cert.decision == RecoveryDecision.READY:
            state = HealthState.READY
        elif cert.decision == RecoveryDecision.RECOVERY_REQUIRED:
            state = HealthState.EMERGENCY_EXIT
        else:
            state = HealthState.RECOVERING
        snap = HealthSnapshot(
            state=state,
            writer_owned=True,
            account_matched=bool(ident.ok),
            data_current=public_health.is_fresh(
                now_ns=now_ns, max_staleness_ns=max_public_staleness_ns
            ),
            reconciliation=recon,
            protection=ProtectionStatus.UNCONFIRMED,
            has_open_exposure=bool(intents),
            drawdown_stop_active=False,
        )
        gate = new_risk_allowed(snap)
        return BootResult(
            state=state,
            certificate=cert,
            new_risk_allowed=bool(gate.allowed),
            reasons=gate.reasons,
            unresolved_intents=len(intents),
            unknown_commands=len(unknown),
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
