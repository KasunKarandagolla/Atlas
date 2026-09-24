"""Independent, evidence-bound research regime axes, version REGIME_V1."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegimeAxis:
    state: str
    evidence_refs: tuple[str, ...]
    reason: str | None = None


@dataclass(frozen=True)
class RegimeAxes:
    trend_state: RegimeAxis
    volatility_state: RegimeAxis
    liquidity_state: RegimeAxis
    crowding_state: RegimeAxis
    event_state: RegimeAxis
    unknown_or_ood_state: RegimeAxis


def classify_regime(*, ema20: float | None, ema50: float | None, close: float | None,
                    realized_variance: float | None, spread_bps: float | None,
                    funding: float | None, event_gate: str | None,
                    feature_ref: str, bbo_ref: str | None = None,
                    funding_ref: str | None = None, event_ref: str | None = None) -> RegimeAxes:
    unknown = RegimeAxis("UNKNOWN", (), "MISSING_SOURCE")
    trend = (RegimeAxis("UP" if close > ema50 and ema20 > ema50 else "DOWN" if close < ema50 and ema20 < ema50 else "MIXED", (feature_ref,))
             if close is not None and ema20 is not None and ema50 is not None else unknown)
    # These are engineering states, not fitted economic thresholds.
    volatility = RegimeAxis("OBSERVED", (feature_ref,)) if realized_variance is not None else unknown
    liquidity = RegimeAxis("WIDE" if spread_bps > 10 else "TIGHT", (bbo_ref,)) if spread_bps is not None and bbo_ref else unknown
    crowding = RegimeAxis("POSITIVE_FUNDING" if funding > 0 else "NEGATIVE_FUNDING" if funding < 0 else "NEUTRAL", (funding_ref,)) if funding is not None and funding_ref else unknown
    event = RegimeAxis(event_gate, (event_ref,)) if event_gate in ("CLEAR", "BLOCKED") and event_ref else unknown
    ood = RegimeAxis("UNKNOWN", (), "MISSING_AXIS") if any(x.state == "UNKNOWN" for x in (trend, volatility, liquidity, crowding, event)) else RegimeAxis("NO_MISSING_AXIS", (feature_ref, bbo_ref or "", funding_ref or "", event_ref or ""))
    return RegimeAxes(trend, volatility, liquidity, crowding, event, ood)
