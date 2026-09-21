"""Runtime health + deterministic new-risk gate (freeze §1.5, §1.6, §1.8).

Health states: BOOT, RECOVERING, READY, DEGRADED, ENTRY_HALTED,
PROTECTION_UNCERTAIN, EMERGENCY_EXIT.

new_risk_allowed(...) returns False unless ALL prerequisites hold:
- health == READY
- writer ownership proven
- account/identity matched
- data fresh (feed current)
- reconciliation CURRENT
- protection confirmed-or-flat (no unconfirmed exposure)
- no drawdown stop (risk gate)
"""

from __future__ import annotations

from dataclasses import dataclass

from atlas.domain.enums import HealthState, ProtectionStatus, ReconciliationHealth


@dataclass(frozen=True)
class HealthSnapshot:
    state: HealthState
    writer_owned: bool
    account_matched: bool
    data_current: bool
    reconciliation: ReconciliationHealth
    protection: ProtectionStatus
    has_open_exposure: bool
    drawdown_stop_active: bool

    def __post_init__(self) -> None:
        if not isinstance(self.state, HealthState):
            raise ValueError("state must be HealthState")
        if not isinstance(self.reconciliation, ReconciliationHealth):
            raise ValueError("reconciliation must be ReconciliationHealth")
        if not isinstance(self.protection, ProtectionStatus):
            raise ValueError("protection must be ProtectionStatus")
        for name in (
            "writer_owned",
            "account_matched",
            "data_current",
            "has_open_exposure",
            "drawdown_stop_active",
        ):
            val = getattr(self, name)
            if not isinstance(val, bool):
                raise ValueError(f"{name} must be bool, got {val!r}")


@dataclass(frozen=True)
class RiskGateDecision:
    allowed: bool
    reasons: tuple

    def __bool__(self) -> bool:
        return self.allowed


def new_risk_allowed(snap: HealthSnapshot) -> RiskGateDecision:
    reasons: list = []
    if snap.state != HealthState.READY:
        reasons.append(f"health state {snap.state.value} != READY")
    if not snap.writer_owned:
        reasons.append("writer ownership not proven (new-risk-disabled)")
    if not snap.account_matched:
        reasons.append("account identity/mode not matched")
    if not snap.data_current:
        reasons.append("market/account data not current")
    if snap.reconciliation != ReconciliationHealth.CURRENT:
        reasons.append(f"reconciliation {snap.reconciliation.value} != CURRENT")
    if snap.drawdown_stop_active:
        reasons.append("drawdown stop active")
    # Unconfirmed protection blocks new risk whenever exposure exists.
    if snap.has_open_exposure and snap.protection != ProtectionStatus.CONFIRMED:
        reasons.append(
            f"open exposure with protection {snap.protection.value} (requires CONFIRMED)"
        )
    # PROTECTION_UNCERTAIN-style: even without known exposure flag, a BREACHED or
    # UNCONFIRMED protection marker with stale/conflicted reconciliation already blocked above.
    return RiskGateDecision(allowed=not reasons, reasons=tuple(reasons))
