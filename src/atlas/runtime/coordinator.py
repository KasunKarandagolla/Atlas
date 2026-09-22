"""Fail-closed crypto-live coordinator. No order-submission interface."""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

from atlas.domain.capability import CapabilityContract
from atlas.domain.enums import CommandOutcome, HealthState, ProtectionStatus, ReconciliationHealth
from atlas.domain.risk import RiskPolicy
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal

from .connectivity import PrivateVerification, PublicVenueHealth
from .health import HealthSnapshot, new_risk_allowed
from .prerequisites import IdentityExpectation, ObservedAccountState, check_identity
from .recovery import RecoveryCertificate, RecoveryDecision
from .writer_lock import WriterLock

_SHA=re.compile(r'^[0-9a-f]{64}$')
@dataclass(frozen=True)
class RiskPolicyDecision:
    approved:bool; policy_hash:str|None=None; evidence_ref:str|None=None; account_snapshot_hash:str|None=None; reservation_version:int|None=None
    def validates(self,policy:RiskPolicy|None,snapshot_hash:str|None,reservation_version:int|None)->bool:
        return bool(self.approved and policy and self.policy_hash==policy.policy_hash() and self.evidence_ref and self.account_snapshot_hash==snapshot_hash and self.reservation_version==reservation_version and reservation_version is not None)
@dataclass(frozen=True)
class BootResult:
    state:HealthState; certificate:RecoveryCertificate; new_risk_allowed:bool; reasons:tuple[str,...]; unresolved_intents:int; unknown_commands:int; unresolved_commands:int

def _check_capability_gate(contract:CapabilityContract|None,capability_hash:str,all_qualified:bool,assisted_enabled:bool,now_ns:int|None=None)->tuple[bool,list[str]]:
    if contract is None:return False,['typed capability contract not supplied']
    r=contract.assisted_blockers()
    if capability_hash!=contract.contract_hash():r.append('capability hash does not bind contract')
    if not all_qualified:r.append('all_qualified false')
    if not assisted_enabled:r.append('assisted_enabled false')
    if not contract.assisted_enabled:r.append('contract assisted_enabled false')
    return not r,r

def boot(*,journal_path:str,lock_path:str,capability_hash:str,all_qualified:bool,assisted_enabled:bool,identity_expected:IdentityExpectation,identity_observed:ObservedAccountState,public_health:PublicVenueHealth,private_verification:PrivateVerification,now_ns:int,max_public_staleness_ns:int,clock_uncertainty_ns:int=0,writer:WriterLock|None=None,journal:SQLiteJournal|None=None,capability_contract:CapabilityContract|None=None,risk_policy:RiskPolicy|None=None,risk_decision:RiskPolicyDecision|None=None,risk_snapshot_hash:str|None=None,reservation_version:int|None=None,runtime_instance_id:str|None=None)->BootResult:
    owned=writer or WriterLock(lock_path);close_writer=writer is None
    if writer is None:ownership=owned.acquire()
    elif writer.ownership is None:raise PersistenceError('writer not acquired')
    else:ownership=writer.ownership
    j=journal or SQLiteJournal(journal_path);close_journal=journal is None
    try:
        intents=j.load_unresolved_intents();commands=j.load_unresolved_commands();unknown=[c for c in commands if c.outcome==CommandOutcome.UNKNOWN or (c.send_started_at_ns is not None and c.outcome==CommandOutcome.UNSENT)]
        unresolved=[c for c in commands if c.outcome in (CommandOutcome.UNSENT,CommandOutcome.UNKNOWN,CommandOutcome.DEFINITE_ACCEPT)]
        ident=check_identity(identity_expected,identity_observed);public_fresh=public_health.is_fresh(now_ns=now_ns,max_staleness_ns=max_public_staleness_ns,clock_uncertainty_ns=clock_uncertainty_ns)
        private_ok=private_verification.verified and private_verification.account_identity_hash==identity_observed.account_identity_hash
        # Phase 1/2 safe runtime has no complete persisted venue run by default; never infer CURRENT from public freshness alone.
        decision=RecoveryDecision.RECOVERY_REQUIRED if unresolved or unknown else RecoveryDecision.REMAIN_RECOVERING
        cert=RecoveryCertificate(uuid.uuid4().hex,runtime_instance_id or uuid.uuid4().hex,ownership.writer_id,ownership.writer_epoch,j.schema_version() or 0,tuple(i.intent_id for i in intents),tuple(c.command_id for c in unresolved),tuple(c.command_id for c in unknown),ReconciliationHealth.STALE,now_ns,now_ns,('local-restore-only',),(),decision,None,None)
        cap_ok,cap_reasons=_check_capability_gate(capability_contract,capability_hash,all_qualified,assisted_enabled)
        policy_ok=(risk_decision or RiskPolicyDecision(False)).validates(risk_policy,risk_snapshot_hash,reservation_version)
        if not policy_ok:cap_reasons.append('typed RiskPolicy/account snapshot/reservation decision not positively bound')
        reasons=list(ident.reasons)+cap_reasons
        if not public_fresh:reasons.append('public data not fresh')
        if not private_ok:reasons.append('private account evidence not verified')
        state=HealthState.ENTRY_HALTED if decision==RecoveryDecision.RECOVERY_REQUIRED else HealthState.RECOVERING
        snap=HealthSnapshot(state,True,ident.ok,public_fresh,cert.reconciliation_health,ProtectionStatus.UNCONFIRMED,bool(intents),False)
        local=new_risk_allowed(snap); reasons.extend(local.reasons)
        return BootResult(state,cert,local.allowed and cap_ok and policy_ok and private_ok,tuple(dict.fromkeys(reasons)),len(intents),len(unknown),len(unresolved))
    finally:
        if close_journal:j.close()
        if close_writer:owned.release()
