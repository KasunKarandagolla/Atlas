"""Offline drawdown hysteresis state; it can only constrain new risk."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from atlas.domain.risk import RiskPolicy


class DrawdownState(StrEnum):
    NORMAL = "NORMAL"
    REDUCED = "REDUCED"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class DrawdownEvidence:
    human_review: bool = False
    clean_reconciliation: bool = False


def next_drawdown_state(policy: RiskPolicy, current: DrawdownState, drawdown, evidence: DrawdownEvidence | None = None) -> DrawdownState:
    evidence = evidence or DrawdownEvidence()
    if drawdown >= policy.drawdown_stop_threshold:
        return DrawdownState.STOPPED
    if current is DrawdownState.STOPPED:
        if drawdown < policy.drawdown_stop_recovery and evidence.human_review and evidence.clean_reconciliation:
            return DrawdownState.NORMAL if drawdown <= policy.drawdown_reduce_recovery else DrawdownState.REDUCED
        return DrawdownState.STOPPED
    if drawdown > policy.drawdown_reduce_threshold:
        return DrawdownState.REDUCED
    if current is DrawdownState.REDUCED and drawdown >= policy.drawdown_reduce_recovery:
        return DrawdownState.REDUCED
    return DrawdownState.NORMAL
