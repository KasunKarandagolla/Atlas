from __future__ import annotations

from dataclasses import dataclass

from atlas.domain.enums import HealthState, ProtectionStatus, ReconciliationHealth


@dataclass(frozen=True)
class HealthSnapshot:
    state:HealthState; writer_owned:bool; account_matched:bool; data_current:bool; reconciliation:ReconciliationHealth; protection:ProtectionStatus; has_open_exposure:bool; drawdown_stop_active:bool
    def __post_init__(self):
        for name in ('writer_owned','account_matched','data_current','has_open_exposure','drawdown_stop_active'):
            if not isinstance(getattr(self,name),bool): raise ValueError(f'{name} must be bool')
@dataclass(frozen=True)
class RiskGateResult:allowed:bool;reasons:tuple[str,...]
RiskGateDecision=RiskGateResult
def new_risk_allowed(s:HealthSnapshot)->RiskGateResult:
    r=[]
    if s.state!=HealthState.READY:r.append('runtime not READY')
    if not s.writer_owned:r.append('writer not owned')
    if not s.account_matched:r.append('account not matched')
    if not s.data_current:r.append('data stale')
    if s.reconciliation!=ReconciliationHealth.CURRENT:r.append('reconciliation not current')
    if s.has_open_exposure and s.protection!=ProtectionStatus.CONFIRMED:r.append('open exposure not protected')
    if s.drawdown_stop_active:r.append('drawdown stop active')
    return RiskGateResult(not r,tuple(r))
