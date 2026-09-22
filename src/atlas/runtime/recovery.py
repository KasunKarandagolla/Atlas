"""Recovery orchestration requiring a complete durable reconciliation run."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.domain.enums import ProtectionStatus, ReconciliationHealth
from atlas.domain.time import ensure_utc_ns
from atlas.persistence.sqlite import PersistenceError

from .reconciliation_evidence import build_reconciliation_bundle


class RecoveryDecision(StrEnum):READY='READY';REMAIN_RECOVERING='REMAIN_RECOVERING';RECOVERY_REQUIRED='RECOVERY_REQUIRED'
@dataclass(frozen=True)
class RecoveryProtectionEvidence:
    """Legacy typed view retained for callers migrating to persisted artifacts."""
    certified_flat:bool; current_protection:bool; evidence_refs:tuple[str,...]
    def __post_init__(self)->None:
        if not isinstance(self.certified_flat,bool) or not isinstance(self.current_protection,bool): raise ValueError('recovery protection flags must be bool')
        object.__setattr__(self,'evidence_refs',tuple(self.evidence_refs))
        if not (self.certified_flat or self.current_protection): raise ValueError('recovery requires certified flatness or current protection')
        if not self.evidence_refs or any(not isinstance(x,str) or not x.strip() for x in self.evidence_refs): raise ValueError('recovery protection evidence_refs must be non-empty')
@dataclass(frozen=True)
class RecoveryIncident:
    incident_id:str; recovery_run_id:str; category:str; status:str; evidence_refs:tuple[str,...]; opened_at_ns:int; resolved_at_ns:int|None=None
@dataclass(frozen=True,init=False)
class RecoveryCertificate:
    recovery_run_id:str; runtime_instance_id:str; writer_id:str; writer_epoch:int; journal_schema_version:int; unresolved_intents:tuple[str,...]; unresolved_commands:tuple[str,...]; unknown_commands:tuple[str,...]; reconciliation_health:ReconciliationHealth; started_at_ns:int; ended_at_ns:int; evidence_refs:tuple[str,...]; venue_evidence_refs:tuple[str,...]; decision:RecoveryDecision; flat_certificate_id:str|None=None; protection_evidence_id:str|None=None; protection_uncertainty_summary:str=''; venue_observations_obtained:bool=True; protection_evidence:RecoveryProtectionEvidence|None=None
    def __init__(self,*args:Any,**kwargs:Any)->None:
        names=('recovery_run_id','runtime_instance_id','writer_id','writer_epoch','journal_schema_version','unresolved_intents','unresolved_commands','unknown_commands','reconciliation_health','started_at_ns','ended_at_ns','evidence_refs','venue_evidence_refs','decision','flat_certificate_id','protection_evidence_id')
        if len(args)==len(names): values=dict(zip(names,args,strict=True))
        elif args: raise TypeError(f'expected {len(names)} v5 positional fields')
        else: values={}
        values.update(kwargs)
        values.setdefault('runtime_instance_id','legacy-runtime')
        values.setdefault('flat_certificate_id',None); values.setdefault('protection_evidence_id',None)
        values.setdefault('protection_uncertainty_summary',''); values.setdefault('venue_observations_obtained',True); values.setdefault('protection_evidence',None)
        for name in ('recovery_run_id','runtime_instance_id','writer_id','writer_epoch','journal_schema_version','unresolved_intents','unresolved_commands','unknown_commands','reconciliation_health','started_at_ns','ended_at_ns','evidence_refs','venue_evidence_refs','decision','flat_certificate_id','protection_evidence_id','protection_uncertainty_summary','venue_observations_obtained','protection_evidence'):
            if name not in values: raise TypeError(f'missing required field: {name}')
            object.__setattr__(self,name,values[name])
        self.__post_init__()
    def __post_init__(self):
        ensure_utc_ns(self.started_at_ns,field='started_at_ns');ensure_utc_ns(self.ended_at_ns,field='ended_at_ns')
        if self.ended_at_ns<self.started_at_ns: raise ValueError('ended_at cannot precede started_at')
        if self.decision==RecoveryDecision.READY:
            if not self.venue_observations_obtained:raise ValueError('READY requires venue_observations_obtained=True')
            if self.unresolved_intents or self.unresolved_commands or self.unknown_commands:raise ValueError('READY cannot contain unresolved work')
            if self.reconciliation_health!=ReconciliationHealth.CURRENT:raise ValueError('READY requires CURRENT reconciliation')
            if not self.venue_evidence_refs:raise ValueError('READY requires venue evidence')
            if not (self.flat_certificate_id or self.protection_evidence_id or (self.protection_evidence and self.protection_evidence.evidence_refs)):raise ValueError('READY requires persisted flat/protection artifact')

def _validate_resolution_artifact(*,journal:Any,flat_certificate_id:str|None,protection_evidence_id:str|None,reconciliation_run_id:str,account:str,instrument:str,position_epoch:int,current_writer_id:str,current_writer_epoch:int,now_ns:int,max_protection_staleness_ns:int)->tuple[bool,list[str]]:
    reasons=[]
    if flat_certificate_id:
        p=journal.load_flat_certificate_payload(flat_certificate_id)
        if not p:reasons.append('flat certificate reference does not exist')
        else:
            if p.get('decision')!='CERTIFIED_FLAT':reasons.append('flat certificate not certified')
            if p.get('reconciliation_run_id')!=reconciliation_run_id:reasons.append('flat certificate belongs to another run')
            if p.get('account_identity_hash')!=account or p.get('instrument')!=instrument or int(p.get('position_epoch',-1))!=position_epoch:reasons.append('flat certificate identity mismatch')
            if p.get('writer_id')!=current_writer_id or int(p.get('writer_epoch',-1))!=current_writer_epoch:reasons.append('flat certificate writer mismatch')
    elif protection_evidence_id:
        try:e=journal.load_protection_evidence(protection_evidence_id)
        except PersistenceError:reasons.append('protection evidence reference does not exist')
        else:
            if e.status!=ProtectionStatus.CONFIRMED:reasons.append('protection evidence not confirmed')
            if e.receive_time_ns>now_ns or now_ns-e.receive_time_ns>max_protection_staleness_ns:reasons.append('protection evidence stale or clock-conflicted')
            if e.account_ref!=account or e.instrument!=instrument or e.position_epoch!=position_epoch:reasons.append('protection evidence identity mismatch')
    else:reasons.append('no flat/protection artifact')
    return not reasons,reasons

def recover_from_persisted_run(*,journal:Any,recovery_run_id:str,reconciliation_run_id:str,runtime_instance_id:str,writer_id:str,writer_epoch:int,unresolved_intent_ids:tuple[str,...],unresolved_command_ids:tuple[str,...],unknown_command_ids:tuple[str,...],account:str,instrument:str,position_epoch:int,started_at_ns:int,ended_at_ns:int,prerequisites_ok:bool,flat_certificate_id:str|None=None,protection_evidence_id:str|None=None,max_protection_staleness_ns:int=2_000_000_000)->RecoveryCertificate:
    run=journal.load_reconciliation_run(reconciliation_run_id); queries=journal.load_run_queries(reconciliation_run_id); bundle=build_reconciliation_bundle(run,queries)
    if run.account!=account or run.instrument!=instrument or run.writer_id!=writer_id or run.writer_epoch!=writer_epoch or run.runtime_instance_id!=runtime_instance_id: raise PersistenceError('reconciliation run identity mismatch')
    artifact_ok,artifact_reasons=_validate_resolution_artifact(journal=journal,flat_certificate_id=flat_certificate_id,protection_evidence_id=protection_evidence_id,reconciliation_run_id=reconciliation_run_id,account=account,instrument=instrument,position_epoch=position_epoch,current_writer_id=writer_id,current_writer_epoch=writer_epoch,now_ns=ended_at_ns,max_protection_staleness_ns=max_protection_staleness_ns)
    actual_intents=tuple(i.intent_id for i in journal.load_unresolved_intents())
    actual_commands=tuple(c.command_id for c in journal.load_unresolved_commands() if c.outcome.value in {'UNSENT','UNKNOWN','DEFINITE_ACCEPT'})
    actual_unknown=tuple(c.command_id for c in journal.load_unresolved_commands() if c.outcome.value=='UNKNOWN')
    for supplied,actual,label in ((unresolved_intent_ids,actual_intents,'intents'),(unresolved_command_ids,actual_commands,'commands'),(unknown_command_ids,actual_unknown,'unknown commands')):
        if supplied is not None and tuple(supplied)!=actual: raise PersistenceError(f'caller {label} do not match durable journal')
    unresolved_intent_ids,unresolved_command_ids,unknown_command_ids=actual_intents,actual_commands,actual_unknown
    unresolved=bool(actual_intents or actual_commands or actual_unknown)
    health=ReconciliationHealth.CURRENT if bundle.complete_for_recovery else ReconciliationHealth.STALE
    if unresolved or not bundle.complete_for_recovery:decision=RecoveryDecision.RECOVERY_REQUIRED
    elif not prerequisites_ok or not artifact_ok:decision=RecoveryDecision.REMAIN_RECOVERING
    else:decision=RecoveryDecision.READY
    refs=tuple(q.evidence_hash for q in queries)
    cert=RecoveryCertificate(recovery_run_id,runtime_instance_id,writer_id,writer_epoch,journal.schema_version() or 0,tuple(unresolved_intent_ids),tuple(unresolved_command_ids),tuple(unknown_command_ids),health,started_at_ns,ended_at_ns,('reconciliation-run:'+reconciliation_run_id,)+tuple(artifact_reasons),refs,decision,flat_certificate_id,protection_evidence_id)
    journal.append_recovery_certificate(cert)
    if decision!=RecoveryDecision.READY:
        journal.append_recovery_incident(RecoveryIncident(f'{recovery_run_id}:incident',recovery_run_id,'recovery_gate',decision.value,cert.evidence_refs,ended_at_ns))
    return cert


def run_recovery(*,recovery_run_id:str,writer_id:str,writer_epoch:int,journal_schema_version:int,unresolved_intent_ids:tuple[str,...],unresolved_command_ids:tuple[str,...],unknown_command_ids:tuple[str,...],reconciliation_health:ReconciliationHealth,protection_uncertainty_summary:str,started_at_ns:int,ended_at_ns:int,venue_observations_obtained:bool,venue_evidence_refs:tuple[str,...],prerequisites_ok:bool,protection_evidence:RecoveryProtectionEvidence|None=None,reconciliation_evidence:Any=None)->RecoveryCertificate:
    """Compatibility classifier for pre-v5 callers.

    It remains fail-closed: a caller-provided boolean/string pair is not a
    persisted artifact.  New code must use ``recover_from_persisted_run``.
    """
    refs=tuple(getattr(q,'evidence_hash',q) for q in getattr(reconciliation_evidence,'queries',())) if reconciliation_evidence is not None else ()
    if reconciliation_health==ReconciliationHealth.CONFLICTED:
        decision=RecoveryDecision.RECOVERY_REQUIRED
    elif unresolved_intent_ids or unresolved_command_ids or unknown_command_ids or reconciliation_health!=ReconciliationHealth.CURRENT or not prerequisites_ok:
        decision=RecoveryDecision.RECOVERY_REQUIRED if reconciliation_health!=ReconciliationHealth.CURRENT and (unresolved_command_ids or unknown_command_ids) else RecoveryDecision.REMAIN_RECOVERING
    else:
        # The legacy API has no journal/artifact handle, so it cannot certify READY.
        decision=RecoveryDecision.REMAIN_RECOVERING
    return RecoveryCertificate(recovery_run_id=recovery_run_id,runtime_instance_id='legacy-runtime',writer_id=writer_id,writer_epoch=writer_epoch,journal_schema_version=journal_schema_version,unresolved_intents=tuple(unresolved_intent_ids),unresolved_commands=tuple(unresolved_command_ids),unknown_commands=tuple(unknown_command_ids),reconciliation_health=reconciliation_health,started_at_ns=started_at_ns,ended_at_ns=ended_at_ns,evidence_refs=('journal-restore',)+refs,venue_evidence_refs=tuple(venue_evidence_refs) if venue_observations_obtained else (),decision=decision,protection_uncertainty_summary=protection_uncertainty_summary,venue_observations_obtained=venue_observations_obtained,protection_evidence=protection_evidence)


def recover_from_persisted_evidence(*,journal:Any,recovery_run_id:str,writer_id:str,writer_epoch:int,journal_schema_version:int|None=None,unresolved_intent_ids:tuple[str,...],unresolved_command_ids:tuple[str,...],unknown_command_ids:tuple[str,...],account:str,instrument:str|None,query_ids:tuple[str,...],reconciliation_started_at_ns:int,reconciliation_completed_at_ns:int,protection_uncertainty_summary:str,started_at_ns:int,ended_at_ns:int,prerequisites_ok:bool,protection_evidence:RecoveryProtectionEvidence|None=None)->RecoveryCertificate:
    """Legacy restart entry point mapped to the v5 complete-query gate."""
    from .reconciliation_evidence import (
        DEFAULT_EXECUTION_RISK_QUERIES,
    )
    queries=tuple(journal.load_reconciliation_query_evidence(qid)[0] for qid in query_ids)
    required=tuple(DEFAULT_EXECUTION_RISK_QUERIES)
    missing=tuple(q for q in required if q not in {e.query_type for e in queries})
    complete=not missing and all(e.status.value=='success' and e.completeness.value=='complete' for e in queries)
    health=ReconciliationHealth.CURRENT if complete else ReconciliationHealth.STALE
    cert=run_recovery(recovery_run_id=recovery_run_id,writer_id=writer_id,writer_epoch=writer_epoch,journal_schema_version=journal_schema_version or journal.schema_version() or 0,unresolved_intent_ids=unresolved_intent_ids,unresolved_command_ids=unresolved_command_ids,unknown_command_ids=unknown_command_ids,reconciliation_health=health,protection_uncertainty_summary=protection_uncertainty_summary,started_at_ns=started_at_ns,ended_at_ns=ended_at_ns,venue_observations_obtained=bool(queries),venue_evidence_refs=tuple(e.evidence_hash for e in queries),prerequisites_ok=prerequisites_ok and complete,protection_evidence=protection_evidence)
    journal.append_recovery_certificate(cert)
    if cert.decision!=RecoveryDecision.READY:
        journal.append_recovery_incident(RecoveryIncident(f'{recovery_run_id}:incident',recovery_run_id,'recovery_gate',cert.decision.value,cert.evidence_refs,ended_at_ns))
    return cert
