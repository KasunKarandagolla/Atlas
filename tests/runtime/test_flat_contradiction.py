from __future__ import annotations

from conftest import T0, add_intent, completed_run, terminal_entry

from atlas.domain.enums import LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.runtime.flat_certificate import certify_flat_from_journal, record_flat_contradiction


def test_late_contradiction_appends_incident_and_reopens_projection(journal):
    i=add_intent(journal);terminal_entry(journal,i);journal.update_intent_state(intent_id=i.intent_id,lifecycle=LifecycleState.CLOSED,protection=ProtectionStatus.NONE,health=ReconciliationHealth.CURRENT,expected_version=0);completed_run(journal)
    c=certify_flat_from_journal(journal=journal,certification_id='flat-c',reconciliation_run_id='run-1',intent_id=i.intent_id,current_writer_id='writer',current_writer_epoch=1,account_identity_hash='acct',instrument='BTCUSDT',position_epoch=0,now_ns=T0+20);journal.append_flat_certificate(c)
    original=journal.load_flat_certificate_payload('flat-c')
    record_flat_contradiction(journal,certificate_id='flat-c',evidence_ref='late-execution',observed_at_ns=T0+30)
    assert journal.load_flat_certificate_payload('flat-c')==original
    assert journal.load_intent(i.intent_id).lifecycle==LifecycleState.RECOVERY_REQUIRED
    assert journal.count('recovery_incidents')==1
