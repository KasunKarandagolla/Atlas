"""Capability evidence ledger; offline evidence never becomes venue support."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from atlas.persistence.sqlite import SQLiteJournal
class EvidenceState(StrEnum):UNVERIFIED='UNVERIFIED';TESTED_OFFLINE='TESTED_OFFLINE';TEST_GATE_TESTNET='TEST_GATE_TESTNET';PASSED_TESTNET='PASSED_TESTNET';FAILED='FAILED';BLOCKED_BY_ENVIRONMENT='BLOCKED_BY_ENVIRONMENT'
_SHA=re.compile(r'^[0-9a-f]{64}$')
def _refs(x:tuple[str,...])->bool:return bool(x) and all(isinstance(r,str) and r.strip() and r.upper() not in {'PASSED','FAILED','REQUIRED','PLACEHOLDER'} for r in x)
@dataclass(frozen=True)
class CapabilityEvidence:
    capability_name:str; state:EvidenceState; test_run_id:str|None; evidence_refs:tuple[str,...]; test_timestamp_ns:int|None; environment:str; notes:str; target_profile_hash:str|None=None
@dataclass(frozen=True)
class QualificationRecord:
    qualification_id:str; capability_name:str; previous_state:EvidenceState; new_state:EvidenceState; test_run_id:str; evidence_refs:tuple[str,...]; qualified_by:str; qualified_at_ns:int; target_profile_hash:str
class CapabilityEvidenceLedger:
    REQUIRED_CAPABILITIES=('entry_ioc_with_attached_full_mark_market_stop','native_stop_visible_and_resizes_on_partial_fill','reduce_only_wire_and_matching_enforcement','ambiguous_submit_not_treated_as_definite_rejection','external_native_stop_fill_reconciliation','native_position_stop_read_and_repair_port')
    def __init__(self,journal:SQLiteJournal|None=None):
        self.journal=journal
        self._e={n:CapabilityEvidence(n,EvidenceState.UNVERIFIED,None,(),None,'testnet','initial unverified') for n in self.REQUIRED_CAPABILITIES}
        self._q:list[QualificationRecord]=[]
        if journal is not None and hasattr(journal,'load_latest_capability_evidence'):
            for name,evidence in journal.load_latest_capability_evidence().items():
                if name in self._e:self._e[name]=evidence
    def get_evidence(self,n:str):return self._e.get(n)
    def record_offline_test(self,n:str,test_run_id:str,evidence_refs:tuple[str,...],timestamp_ns:int,passed:bool,notes:str='')->CapabilityEvidence:
        if n not in self._e or not test_run_id.strip() or not _refs(evidence_refs):raise ValueError('real offline test identity/evidence required')
        e=CapabilityEvidence(n,EvidenceState.TESTED_OFFLINE if passed else EvidenceState.FAILED,test_run_id,evidence_refs,timestamp_ns,'offline',notes)
        self._e[n]=e
        if self.journal:self.journal.append_capability_evidence(e)
        return e
    def record_testnet_gate(self,n:str,test_run_id:str,evidence_refs:tuple[str,...],timestamp_ns:int,passed:bool,*,target_profile_hash:str,notes:str='')->CapabilityEvidence:
        if n not in self._e or not test_run_id.strip() or not _refs(evidence_refs) or not _SHA.fullmatch(target_profile_hash):raise ValueError('testnet gate requires immutable refs + exact profile hash')
        e=CapabilityEvidence(n,EvidenceState.TEST_GATE_TESTNET if passed else EvidenceState.FAILED,test_run_id,evidence_refs,timestamp_ns,'testnet',notes,target_profile_hash);self._e[n]=e
        if self.journal:self.journal.append_capability_evidence(e)
        return e
    def qualify_capability(self,n:str,qualification_id:str,test_run_id:str,evidence_refs:tuple[str,...],qualified_by:str,timestamp_ns:int,target_profile_hash:str)->QualificationRecord:
        c=self._e[n]
        if c.state!=EvidenceState.TEST_GATE_TESTNET: raise ValueError('qualification gate mismatch: state')
        if c.test_run_id!=test_run_id: raise ValueError('qualification gate mismatch: test_run_id')
        if c.evidence_refs!=evidence_refs: raise ValueError('qualification gate mismatch: evidence_refs')
        if c.target_profile_hash!=target_profile_hash or not _SHA.fullmatch(target_profile_hash): raise ValueError('qualification gate mismatch: target_profile_hash')
        e=CapabilityEvidence(n,EvidenceState.PASSED_TESTNET,test_run_id,evidence_refs,timestamp_ns,'testnet',f'qualified by {qualified_by}',target_profile_hash);self._e[n]=e
        r=QualificationRecord(qualification_id,n,c.state,e.state,test_run_id,evidence_refs,qualified_by,timestamp_ns,target_profile_hash);self._q.append(r)
        if self.journal:self.journal.append_capability_evidence(e);self.journal.append_capability_qualification(r)
        return r
    def all_unverified(self)->bool:return all(e.state==EvidenceState.UNVERIFIED for e in self._e.values())
