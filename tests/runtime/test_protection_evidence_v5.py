from __future__ import annotations

from decimal import Decimal

from conftest import T0

from atlas.domain.execution import ProtectionObservation
from atlas.runtime.protection_evidence import verify_protection


def obs(at):return ProtectionObservation(0,1,Decimal('0.01'),'MarkPrice',Decimal('48000'),'Full Market ReduceOnly',('position-view',),at)
def test_future_source_time_fails_closed_without_fabricating_receipt():
    result=verify_protection(obs(T0+100),0,Decimal('0.01'),Decimal('48000'),'MarkPrice',T0,2_000_000_000,account_ref='acct',instrument='BTCUSDT',receive_time_ns=T0,conditional_order_evidence_ids=('conditional-view',),conditional_order_view_available=True)
    assert not result.verified
    assert result.evidence.receive_time_ns==T0
    assert result.evidence.observation_time_ns==T0+100

def test_typed_protection_roundtrip_is_restart_safe(journal):
    result=verify_protection(obs(T0),0,Decimal('0.01'),Decimal('48000'),'MarkPrice',T0+1,2_000_000_000,account_ref='acct',instrument='BTCUSDT',receive_time_ns=T0+1,conditional_order_evidence_ids=('conditional-view',),conditional_order_view_available=True)
    assert result.verified
    journal.append_protection_evidence('pe-1',result.evidence,'a'*64)
    restored=journal.load_protection_evidence('pe-1');assert restored==result.evidence
