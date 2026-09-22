"""Long-lived safe runtime skeleton. Holds writer and SQLite journal for process lifetime."""
from __future__ import annotations

import signal
import threading
import uuid
from dataclasses import dataclass

from atlas.domain.capability import CapabilityContract
from atlas.domain.risk import RiskPolicy
from atlas.domain.time import now_ns
from atlas.persistence.sqlite import SQLiteJournal

from .connectivity import PrivateVerification, PublicVenueHealth
from .coordinator import BootResult, RiskPolicyDecision, boot
from .prerequisites import IdentityExpectation, ObservedAccountState
from .status import RuntimeStatus, publish_status
from .writer_lock import WriterLock


@dataclass(frozen=True)
class SafeRuntimeConfig:
    journal_path:str;lock_path:str;status_path:str;capability_hash:str;all_qualified:bool;assisted_enabled:bool;identity_expected:IdentityExpectation;identity_observed:ObservedAccountState;public_health:PublicVenueHealth;private_verification:PrivateVerification;max_public_staleness_ns:int;clock_uncertainty_ns:int=0;tick_interval_ns:int=10_000_000_000;capability_contract:CapabilityContract|None=None;risk_policy:RiskPolicy|None=None;risk_decision:RiskPolicyDecision|None=None;risk_snapshot_hash:str|None=None;reservation_version:int|None=None
class SafeRuntime:
    def __init__(self,c:SafeRuntimeConfig):
        self.c=c;self._writer:WriterLock|None=None;self._journal:SQLiteJournal|None=None;self._runtime_instance_id=uuid.uuid4().hex;self._last:BootResult|None=None;self._ticks=0;self._shutdown=threading.Event();signal.signal(signal.SIGINT,self._sig);signal.signal(signal.SIGTERM,self._sig)
    def _sig(self,*_):self._shutdown.set()
    @property
    def runtime_instance_id(self):return self._runtime_instance_id
    @property
    def journal(self):return self._journal
    @property
    def writer_held(self):return self._writer is not None and self._writer.ownership is not None
    @property
    def tick_count(self):return self._ticks
    @property
    def new_risk_allowed(self):return bool(self._last and self._last.new_risk_allowed)
    def start(self)->BootResult:
        if self._writer is not None:raise RuntimeError('already started')
        self._writer=WriterLock(self.c.lock_path)
        try:
            self._writer.acquire();self._journal=SQLiteJournal(self.c.journal_path);last=self._run();self._last=last;self._publish(last);return last
        except Exception:self.shutdown();raise
    def _run(self)->BootResult:
        return boot(journal_path=self.c.journal_path,lock_path=self.c.lock_path,capability_hash=self.c.capability_hash,all_qualified=self.c.all_qualified,assisted_enabled=self.c.assisted_enabled,identity_expected=self.c.identity_expected,identity_observed=self.c.identity_observed,public_health=self.c.public_health,private_verification=self.c.private_verification,now_ns=now_ns(),max_public_staleness_ns=self.c.max_public_staleness_ns,clock_uncertainty_ns=self.c.clock_uncertainty_ns,writer=self._writer,journal=self._journal,capability_contract=self.c.capability_contract,risk_policy=self.c.risk_policy,risk_decision=self.c.risk_decision,risk_snapshot_hash=self.c.risk_snapshot_hash,reservation_version=self.c.reservation_version,runtime_instance_id=self._runtime_instance_id)
    def _publish(self,r:BootResult):
        publish_status(self.c.status_path,RuntimeStatus(r.state.value,r.certificate.writer_epoch,True,r.certificate.reconciliation_health.value,r.unresolved_intents,r.unknown_commands,r.unresolved_commands,self.c.capability_hash,self.c.all_qualified,self.c.assisted_enabled,now_ns(),self._runtime_instance_id,r.certificate.writer_id))
    def tick(self)->BootResult:
        if self._writer is None or self._journal is None:raise RuntimeError('not started')
        self._last=self._run();self._ticks+=1;self._publish(self._last);return self._last
    def shutdown(self):
        self._shutdown.set()
        if self._journal:self._journal.close();self._journal=None
        if self._writer:self._writer.release();self._writer=None
    def run_forever(self):
        if not self.writer_held:self.start()
        while not self._shutdown.is_set():self.tick();self._shutdown.wait(self.c.tick_interval_ns/1e9)
        self.shutdown()
