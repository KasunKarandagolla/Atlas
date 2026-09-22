"""Typed protection evidence; acknowledgements alone are never proof."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.enums import ProtectionStatus
from atlas.domain.execution import ProtectionObservation
from atlas.domain.time import ensure_utc_ns

_PLACEHOLDERS={'','REQUIRED','PLACEHOLDER','TEST_GATE','UNVERIFIED'}
def _valid_refs(refs:tuple[str,...])->bool:return bool(refs) and all(isinstance(x,str) and x.strip() and x.strip().upper() not in _PLACEHOLDERS for x in refs)
@dataclass(frozen=True)
class ProtectionEvidence:
    account_ref:str; instrument:str; position_epoch:int; desired_stop_version:int; observed_signed_qty:Decimal; full_position_semantics:bool; stop_price:Decimal; trigger_basis:str; closing_only_behavior:bool; position_view_evidence_ids:tuple[str,...]; conditional_order_view_evidence_ids:tuple[str,...]; observation_time_ns:int; receive_time_ns:int; status:ProtectionStatus
    def __post_init__(self):
        if not self.account_ref.strip() or not self.instrument.strip(): raise ValueError('protection identity required')
        ensure_utc_ns(self.observation_time_ns,field='observation_time_ns');ensure_utc_ns(self.receive_time_ns,field='receive_time_ns')
        if self.receive_time_ns<0: raise ValueError('invalid receive time')
        if self.stop_price<=0: raise ValueError('positive stop required')
    @property
    def is_confirmed(self)->bool:return self.status==ProtectionStatus.CONFIRMED
@dataclass(frozen=True)
class ProtectionVerificationResult:
    evidence:ProtectionEvidence; verified:bool; mismatch_details:tuple[str,...]
def verify_protection(observation:ProtectionObservation,expected_position_epoch:int,expected_signed_qty:Decimal,expected_stop_price:Decimal,expected_trigger_basis:str,now_ns:int,max_staleness_ns:int,*,account_ref:str,instrument:str,receive_time_ns:int|None=None,conditional_order_evidence_ids:tuple[str,...]=(),conditional_order_view_available:bool=False)->ProtectionVerificationResult:
    m=[]; sem=observation.semantics.lower()
    if observation.position_epoch!=expected_position_epoch:m.append('position epoch mismatch')
    if observation.qty!=expected_signed_qty or expected_signed_qty==0:m.append('signed quantity not exact current exposure')
    if observation.stop_price!=expected_stop_price:m.append('stop price mismatch')
    if observation.trigger_basis!=expected_trigger_basis or observation.trigger_basis!='MarkPrice':m.append('trigger basis mismatch')
    if 'full' not in sem or 'market' not in sem:m.append('not full-position market semantics')
    if not ('reduce' in sem or 'close' in sem):m.append('not closing-only semantics')
    if not _valid_refs(observation.evidence_ids):m.append('position evidence refs invalid')
    if conditional_order_view_available and not _valid_refs(conditional_order_evidence_ids):m.append('conditional evidence refs invalid')
    # Older callers supplied the local evaluation time as ``now_ns`` and had
    # no separate receipt field.  Preserve that API while keeping the v5
    # path explicit: new ingestion code must pass the genuine local receipt
    # timestamp and is never allowed to derive it from source time.
    if receive_time_ns is None:
        receive_time_ns=now_ns
    ensure_utc_ns(receive_time_ns,field='receive_time_ns');ensure_utc_ns(now_ns,field='now_ns')
    if receive_time_ns>now_ns:m.append('receive time is in the future')
    age=now_ns-observation.observed_at_ns
    if age<0:m.append('source clock conflict/future observation')
    elif age>max_staleness_ns:m.append('protection evidence stale')
    e=ProtectionEvidence(account_ref,instrument,observation.position_epoch,observation.desired_stop_version,observation.qty,'full' in sem,observation.stop_price,observation.trigger_basis,('reduce' in sem or 'close' in sem),tuple(observation.evidence_ids),tuple(conditional_order_evidence_ids),observation.observed_at_ns,receive_time_ns,ProtectionStatus.CONFIRMED if not m else ProtectionStatus.UNCONFIRMED)
    return ProtectionVerificationResult(e,not m,tuple(m))
