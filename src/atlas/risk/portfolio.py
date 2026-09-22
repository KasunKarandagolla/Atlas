"""Deterministic empirical portfolio ES and frozen A0 decision value."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from statistics import fmean

MATERIALITY_FLOOR = 1e-5


def empirical_es(losses: Sequence[float], alpha: float = 0.975) -> float:
    """Empirical minimization of the Rockafellar-Uryasev ES expression."""
    if not losses or not 0 < alpha < 1:
        raise ValueError("losses and alpha required")
    # A convex piecewise linear objective has an optimum at an observed loss.
    def objective(v: float) -> float:
        return v + fmean(max(0.0, loss - v) for loss in losses) / (1.0 - alpha)
    return min(objective(v) for v in sorted(set(losses)))


def decision_value(lcb_expected_pnl: float, equity: float, existing_pnl: Sequence[float], candidate_pnl: Sequence[float], lambda_r: float = 1.0) -> float:
    if equity <= 0 or len(existing_pnl) != len(candidate_pnl):
        raise ValueError("positive equity and paired paths required")
    before = empirical_es([-p / equity for p in existing_pnl])
    after = empirical_es([-(p + y) / equity for p, y in zip(existing_pnl, candidate_pnl, strict=True)])
    return lcb_expected_pnl / equity - lambda_r * (after - before)


@dataclass(frozen=True)
class PortfolioDecision:
    es_before: float
    es_after: float
    incremental_es: float
    j: float
    qualified: bool
    reason: str


def common_path_portfolio_decision(lcb_expected_pnl: float, equity: float, pi0: Sequence[float], candidate: Sequence[float],
                                   alpha: float = 0.975) -> PortfolioDecision:
    """Compute ES before/after on paired paths; Pi0 includes pending/UNKNOWN exposure."""
    if equity <= 0 or not pi0 or len(pi0) != len(candidate):
        raise ValueError("positive equity and paired common paths required")
    before = empirical_es([-value / equity for value in pi0], alpha)
    after = empirical_es([-(value + pnl) / equity for value, pnl in zip(pi0, candidate, strict=True)], alpha)
    incremental = after - before
    j = lcb_expected_pnl / equity - incremental
    if lcb_expected_pnl <= 0:
        return PortfolioDecision(before, after, incremental, j, False, "LCB_NONPOSITIVE")
    if j <= MATERIALITY_FLOOR:
        return PortfolioDecision(before, after, incremental, j, False, "J_BELOW_MATERIALITY")
    return PortfolioDecision(before, after, incremental, j, True, "")
