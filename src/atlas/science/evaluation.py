"""Typed complete-policy result contract and A0/B0 selection skeleton."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.strategy.crypto_trend_24h_v1 import Signal


class DecisionStatus(StrEnum):
    TRADE_CANDIDATE = "TRADE_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    NO_TRADE_NUMERICAL = "NO_TRADE_NUMERICAL"
    UNSUPPORTED_POLICY = "UNSUPPORTED_POLICY"
    GATE_DISABLED_DIAGNOSTIC = "GATE_DISABLED_DIAGNOSTIC"
    SKIP_DATA = "SKIP_DATA"
    NO_SIGNAL = "NO_SIGNAL"
    NO_TRADE_GATE = "NO_TRADE_GATE"
    NO_TRADE_EVENT = "NO_TRADE_EVENT"
    NO_TRADE_RISK = "NO_TRADE_RISK"
    NO_TRADE_NO_EDGE = "NO_TRADE_NO_EDGE"


@dataclass(frozen=True)
class EvaluationResult:
    baseline: str
    status: DecisionStatus
    reason: str
    action: Signal
    lcb: float | None = None
    decision_value: float | None = None
    evidence_hashes: tuple[str, ...] = ()

    def artifact_hash(self) -> str:
        data: dict[str, Any] = {"baseline": self.baseline, "status": self.status.value, "reason": self.reason,
                                 "action": self.action.value, "lcb": self.lcb, "j": self.decision_value,
                                 "evidence": self.evidence_hashes}
        return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def evaluate_baseline(*, baseline: str, signal: Signal, gates_ok: bool, support_ok: bool, risk_ok: bool, lcb: float | None = None, j: float | None = None) -> EvaluationResult:
    if baseline not in {"B0", "A0"}:
        raise ValueError("only frozen B0/A0 baselines")
    if signal is Signal.FLAT:
        return EvaluationResult(baseline, DecisionStatus.NO_SIGNAL, "frozen signal is flat", signal)
    if not gates_ok:
        return EvaluationResult(baseline, DecisionStatus.NO_TRADE_GATE, "operational/account/data gate", signal)
    if not support_ok:
        return EvaluationResult(baseline, DecisionStatus.NOT_ESTIMABLE, "insufficient causal scenario support", signal)
    if not risk_ok:
        return EvaluationResult(baseline, DecisionStatus.NO_TRADE_RISK, "hard capital or stress gate", signal)
    if baseline == "B0":
        return EvaluationResult(baseline, DecisionStatus.TRADE_CANDIDATE, "raw frozen trend policy", signal)
    if lcb is None or j is None:
        return EvaluationResult(baseline, DecisionStatus.NOT_ESTIMABLE, "missing A0 uncertainty evidence", signal)
    if lcb <= 0 or j <= 1e-5:
        return EvaluationResult(baseline, DecisionStatus.NO_TRADE_NO_EDGE, "LCB/J gate", signal, lcb, j)
    return EvaluationResult(baseline, DecisionStatus.TRADE_CANDIDATE, "A0 LCB/ES qualified", signal, lcb, j)
