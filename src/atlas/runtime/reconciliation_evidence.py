"""Typed reconciliation evidence and durable run semantics (freeze §1.6)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.domain.time import ensure_utc_ns


class QueryType(StrEnum):
    OPEN_ORDERS='open_orders'; ORDER_HISTORY='order_history'; EXECUTION_HISTORY='execution_history'; POSITIONS='positions'; WALLET_BALANCE='wallet_balance'; TRADING_STOP='trading_stop'; CONDITIONAL_ORDERS='conditional_orders'; TRANSACTION_LOG='transaction_log'
class QueryScope(StrEnum): ACCOUNT='account'; INSTRUMENT='instrument'
class QueryStatus(StrEnum): SUCCESS='success'; PARTIAL='partial'; FAILED='failed'; RATE_LIMITED='rate_limited'; TIMEOUT='timeout'
class Completeness(StrEnum): COMPLETE='complete'; INCOMPLETE_PAGINATED='incomplete_paginated'; INCOMPLETE_TRUNCATED='incomplete_truncated'; INCOMPLETE_RETENTION_LIMIT='incomplete_retention_limit'; UNKNOWN='unknown'
class ReconciliationRunState(StrEnum): OPEN='OPEN'; COMPLETE='COMPLETE'
ACCOUNT_SCOPED=frozenset({QueryType.WALLET_BALANCE,QueryType.TRANSACTION_LOG})
INSTRUMENT_SCOPED=frozenset({QueryType.OPEN_ORDERS,QueryType.ORDER_HISTORY,QueryType.EXECUTION_HISTORY,QueryType.POSITIONS,QueryType.TRADING_STOP,QueryType.CONDITIONAL_ORDERS})
DEFAULT_EXECUTION_RISK_QUERIES=(QueryType.OPEN_ORDERS,QueryType.CONDITIONAL_ORDERS,QueryType.ORDER_HISTORY,QueryType.EXECUTION_HISTORY,QueryType.POSITIONS,QueryType.WALLET_BALANCE,QueryType.TRADING_STOP)

def compute_evidence_hash(payload:dict[str,Any])->str:return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def _coalesce(segments:tuple[tuple[int,int],...])->tuple[tuple[int,int],...]:
    if not segments:return ()
    xs=sorted(segments); out:list[tuple[int,int]]=[]
    for start,end in xs:
        ensure_utc_ns(start,field='retention_start'); ensure_utc_ns(end,field='retention_end')
        if end<start: raise ValueError('retention segment invalid')
        if out and start<=out[-1][1]+1: out[-1]=(out[-1][0],max(out[-1][1],end))
        else: out.append((start,end))
    return tuple(out)
def covers_interval(segments:tuple[tuple[int,int],...],start:int,end:int)->bool:
    return any(a<=start and b>=end for a,b in _coalesce(segments))
@dataclass(frozen=True,init=False)
class ReconciliationQueryEvidence:
    query_id:str; query_type:QueryType; scope:QueryScope; account:str; instrument:str|None; requested_interval_start_ns:int|None; requested_interval_end_ns:int|None; pagination_cursors:tuple[str,...]; pages_observed:int; total_records_returned:int; completeness:Completeness; status:QueryStatus; source_time_ns:int|None; receipt_time_ns:int; request_ids:tuple[str,...]; retention_segments:tuple[tuple[int,int],...]; facts:dict[str,str]; evidence_hash:str; error_message:str|None
    def __init__(self,*args:Any,**kwargs:Any)->None:
        """Construct v5 evidence while accepting the v4 public shape.

        Session-005 adds explicit scope, retention segments and facts.  The
        older repository API exposed account/instrument directly and used one
        retention interval.  Keeping that input shape here preserves the
        historical journal/test API without weakening the v5 invariants.
        """
        names=tuple(self.__dataclass_fields__)
        old_names=(
            'query_id','query_type','account','instrument','requested_interval_start_ns',
            'requested_interval_end_ns','pagination_cursors','pages_observed',
            'total_records_returned','completeness','status','source_time_ns',
            'receipt_time_ns','request_ids','retention_coverage_start_ns',
            'retention_coverage_end_ns','evidence_hash','error_message',
        )
        if len(args)==len(names):
            values=dict(zip(names,args,strict=True))
        elif len(args)==len(old_names):
            values=dict(zip(old_names,args,strict=True))
        elif args:
            raise TypeError(f'expected {len(names)} v5 or {len(old_names)} v4 positional fields')
        else:
            values={}
        values.update(kwargs)
        if 'scope' not in values:
            # The pre-v5 API did not expose scope and sometimes passed the
            # instrument through for transaction-log queries.  v5 makes the
            # account/instrument boundary explicit: account-scoped queries
            # never carry an instrument.
            if values.get('query_type') in ACCOUNT_SCOPED:
                values['scope']=QueryScope.ACCOUNT
                values['instrument']=None
            else:
                values['scope']=QueryScope.ACCOUNT if values.get('instrument') is None else QueryScope.INSTRUMENT
        if 'retention_segments' not in values:
            start=values.pop('retention_coverage_start_ns',None)
            end=values.pop('retention_coverage_end_ns',None)
            values['retention_segments']=() if start is None or end is None else ((start,end),)
        else:
            values.pop('retention_coverage_start_ns',None)
            values.pop('retention_coverage_end_ns',None)
        values.setdefault('facts',{})
        for name in names:
            if name not in values:
                raise TypeError(f'missing required field: {name}')
            object.__setattr__(self,name,values[name])
        self.__post_init__()
    def __post_init__(self):
        if not self.query_id.strip() or not self.account.strip(): raise ValueError('query/account required')
        if self.query_type in ACCOUNT_SCOPED and (self.scope!=QueryScope.ACCOUNT or self.instrument is not None): raise ValueError('account-scoped query must use instrument=None')
        if self.query_type in INSTRUMENT_SCOPED and (self.scope!=QueryScope.INSTRUMENT or not self.instrument): raise ValueError('instrument-scoped query requires instrument')
        if self.requested_interval_start_ns is not None:ensure_utc_ns(self.requested_interval_start_ns,field='requested_start')
        if self.requested_interval_end_ns is not None:ensure_utc_ns(self.requested_interval_end_ns,field='requested_end')
        if self.requested_interval_start_ns is not None and self.requested_interval_end_ns is not None and self.requested_interval_end_ns<self.requested_interval_start_ns: raise ValueError('requested interval invalid')
        ensure_utc_ns(self.receipt_time_ns,field='receipt_time_ns')
        if self.source_time_ns is not None: ensure_utc_ns(self.source_time_ns,field='source_time_ns')
        if self.pages_observed<0 or self.total_records_returned<0: raise ValueError('negative page/record count')
        object.__setattr__(self,'pagination_cursors',tuple(self.pagination_cursors));object.__setattr__(self,'request_ids',tuple(self.request_ids));object.__setattr__(self,'retention_segments',_coalesce(tuple(self.retention_segments))); object.__setattr__(self,'facts',dict(self.facts))
        if len(self.evidence_hash)!=64 or any(c not in '0123456789abcdef' for c in self.evidence_hash): raise ValueError('evidence_hash must be sha256')
    @property
    def is_empty_result(self)->bool:return self.total_records_returned==0
    @property
    def is_incomplete(self)->bool:return self.completeness!=Completeness.COMPLETE
    @property
    def retention_coverage_start_ns(self)->int|None:return self.retention_segments[0][0] if self.retention_segments else None
    @property
    def retention_coverage_end_ns(self)->int|None:return self.retention_segments[-1][1] if self.retention_segments else None
    @property
    def can_certify_absence(self)->bool:
        return self.status==QueryStatus.SUCCESS and self.completeness==Completeness.COMPLETE and self.is_empty_result and self.requested_interval_start_ns is not None and self.requested_interval_end_ns is not None and covers_interval(self.retention_segments,self.requested_interval_start_ns,self.requested_interval_end_ns)

def merge_query_evidence(items:list[ReconciliationQueryEvidence])->ReconciliationQueryEvidence|None:
    if not items:return None
    f=items[0]
    for e in items[1:]:
        if (e.query_type,e.scope,e.account,e.instrument,e.requested_interval_start_ns,e.requested_interval_end_ns)!=(f.query_type,f.scope,f.account,f.instrument,f.requested_interval_start_ns,f.requested_interval_end_ns): raise ValueError('cannot merge inconsistent query evidence')
    sev={Completeness.COMPLETE:0,Completeness.INCOMPLETE_PAGINATED:1,Completeness.INCOMPLETE_TRUNCATED:2,Completeness.INCOMPLETE_RETENTION_LIMIT:3,Completeness.UNKNOWN:4}
    comp=max((e.completeness for e in items),key=lambda x:sev[x])
    if any(e.status in (QueryStatus.FAILED,QueryStatus.TIMEOUT) for e in items): status=QueryStatus.FAILED
    elif any(e.status in (QueryStatus.PARTIAL,QueryStatus.RATE_LIMITED) for e in items):status=QueryStatus.PARTIAL
    else:status=QueryStatus.SUCCESS
    # Only actual contiguous union is retained; gaps remain gaps.
    segments=_coalesce(tuple(s for e in items for s in e.retention_segments))
    return ReconciliationQueryEvidence(f.query_id+'-merged',f.query_type,f.scope,f.account,f.instrument,f.requested_interval_start_ns,f.requested_interval_end_ns,tuple(x for e in items for x in e.pagination_cursors),sum(e.pages_observed for e in items),sum(e.total_records_returned for e in items),comp,status,max(e.source_time_ns or 0 for e in items) or None,max(e.receipt_time_ns for e in items),tuple(x for e in items for x in e.request_ids),segments,dict(f.facts),compute_evidence_hash({'queries':[e.evidence_hash for e in items]}),None)
@dataclass(frozen=True)
class ReconciliationRun:
    run_id:str; account:str; instrument:str|None; writer_id:str; writer_epoch:int; runtime_instance_id:str; started_at_ns:int; completed_at_ns:int|None; required_query_types:tuple[QueryType,...]=DEFAULT_EXECUTION_RISK_QUERIES; state:ReconciliationRunState=ReconciliationRunState.OPEN
    def __post_init__(self):
        for n in ('run_id','account','writer_id','runtime_instance_id'):
            if not getattr(self,n).strip(): raise ValueError(f'{n} required')
        ensure_utc_ns(self.started_at_ns,field='started_at_ns')
        if self.completed_at_ns is not None:
            ensure_utc_ns(self.completed_at_ns,field='completed_at_ns')
            if self.completed_at_ns<self.started_at_ns: raise ValueError('completion before start')
        object.__setattr__(self,'required_query_types',tuple(self.required_query_types))
@dataclass(frozen=True)
class ReconciliationEvidenceBundle:
    run:ReconciliationRun; queries:tuple[ReconciliationQueryEvidence,...]; overall_status:QueryStatus; overall_completeness:Completeness; missing_required_types:tuple[QueryType,...]
    @property
    def complete_for_recovery(self)->bool:return self.run.state==ReconciliationRunState.COMPLETE and self.overall_status==QueryStatus.SUCCESS and self.overall_completeness==Completeness.COMPLETE and not self.missing_required_types

def build_reconciliation_bundle(run:ReconciliationRun,queries:tuple[ReconciliationQueryEvidence,...])->ReconciliationEvidenceBundle:
    if run.state!=ReconciliationRunState.COMPLETE: missing=tuple(run.required_query_types)
    else: missing=tuple(q for q in run.required_query_types if q not in {e.query_type for e in queries})
    for e in queries:
        if e.account!=run.account: raise ValueError('query account does not match run')
        if e.scope==QueryScope.INSTRUMENT and e.instrument!=run.instrument: raise ValueError('query instrument does not match run')
    statuses={e.status for e in queries}; comps={e.completeness for e in queries}
    status=QueryStatus.FAILED if statuses & {QueryStatus.FAILED,QueryStatus.TIMEOUT} else QueryStatus.PARTIAL if statuses & {QueryStatus.PARTIAL,QueryStatus.RATE_LIMITED} else QueryStatus.SUCCESS
    sev={Completeness.COMPLETE:0,Completeness.INCOMPLETE_PAGINATED:1,Completeness.INCOMPLETE_TRUNCATED:2,Completeness.INCOMPLETE_RETENTION_LIMIT:3,Completeness.UNKNOWN:4}
    comp=max(comps,key=lambda x:sev[x]) if comps else Completeness.UNKNOWN
    return ReconciliationEvidenceBundle(run,queries,status,comp,missing)
