from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, generate_client_order_id, make_command
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    Completeness,
    QueryScope,
    QueryStatus,
    QueryType,
    ReconciliationQueryEvidence,
    ReconciliationRun,
    ReconciliationRunState,
    compute_evidence_hash,
)

T0=1_800_000_000_000_000_000
@pytest.fixture
def journal(tmp_path):
    j=SQLiteJournal(tmp_path/'atlas.db');yield j;j.close()
def add_intent(j:SQLiteJournal,intent_id='intent-1'):
    i=Intent(intent_id,0,'plan','v1',generate_client_order_id(),1,LifecycleState.INTENT_PERSISTED,ProtectionStatus.UNCONFIRMED,ReconciliationHealth.STALE,T0,0)
    r=Reservation('res-'+intent_id,intent_id,Decimal('0.01'),Decimal('10'),Decimal('20'),Decimal('500'),Decimal('500'),Decimal('100'),Decimal('5'))
    j.create_intent_with_reservation(i,r);return i
def terminal_entry(j:SQLiteJournal,intent):
    c=make_command(command_id='entry-'+intent.intent_id,intent_id=intent.intent_id,command_type=CommandType.SUBMIT_ENTRY,payload_dict={'client_order_id':intent.client_order_id},expected_state_version=intent.state_version,created_at_ns=T0)
    j.persist_command(c);j.mark_send_started(c.command_id,T0+1);j.update_command_outcome(c.command_id,CommandOutcome.RECONCILED)
def q(kind:QueryType,query_id:str,*,account='acct',instrument='BTCUSDT',records=0,facts=None,complete=True,status=QueryStatus.SUCCESS):
    scope=QueryScope.ACCOUNT if kind in (QueryType.WALLET_BALANCE,QueryType.TRANSACTION_LOG) else QueryScope.INSTRUMENT
    inst=None if scope==QueryScope.ACCOUNT else instrument
    payload={'id':query_id,'kind':kind.value,'records':records,'facts':facts or {}}
    return ReconciliationQueryEvidence(query_id,kind,scope,account,inst,T0-100,T0+100,(query_id,),1,records,Completeness.COMPLETE if complete else Completeness.INCOMPLETE_PAGINATED,status,T0,T0+1,(query_id,),((T0-200,T0+200),),facts or {},compute_evidence_hash(payload),None)
def completed_run(j:SQLiteJournal,run_id='run-1',account='acct',instrument='BTCUSDT',writer_id='writer',writer_epoch=1,runtime='runtime'):
    run=ReconciliationRun(run_id,account,instrument,writer_id,writer_epoch,runtime,T0,T0+10,DEFAULT_EXECUTION_RISK_QUERIES,ReconciliationRunState.COMPLETE);j.create_reconciliation_run(run)
    for kind in DEFAULT_EXECUTION_RISK_QUERIES:
        facts={'signed_qty':'0'} if kind==QueryType.POSITIONS else {}
        e=q(kind,f'{run_id}-{kind.value}',account=account,instrument=instrument,facts=facts)
        j.append_reconciliation_query_evidence(e);j.bind_query_to_run(run_id,e.query_id)
    return run
