"""Strategy applicability matrix for the ATLAS RED full-cycle programme.

Every registered strategy family is evaluated against every coin in a cycle at
that strategy's own native clock, using the production owners' own registered
predicates and policy identities. The evaluation is a *research* evaluation: it
reuses the production predicate functions and policy hashes, and it records,
as explicit unavailable evidence, the production gates that a bounded session
probe cannot honestly satisfy (source-health identity, event gate, executable
BBO at the original instant, exact native trade population).

It never converts a contextual strategy into a standalone directional signal and
never uses a future outcome to decide an original-time applicability.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from statistics import median, pstdev
from typing import Any

from ...contracts import V2Side
from ...data.bars import CausalBarV2
from ...features.joins import JoinedBars
from ...features.technical import atr, ema
from ...instruments import InstrumentKeyV2
from ...strategies import s1_trend, s2_breakout, s3_mean_reversion
from ..full_cycle.analysis import STRATEGY_NATIVE_CLOCK, ApplicabilityStatusV1

APPLICABILITY_VERSION = "ATLAS_RED_FULL_CYCLE_APPLICABILITY_V1"


@dataclass(frozen=True)
class ApplicabilityRecordV1:
    cycle_id: str
    symbol: str
    strategy: str
    native_clock: str
    native_due_at_ns: int
    information_cutoff_ns: int
    universe_eligible: bool
    status: str
    reason_code: str
    detail: Mapping[str, Any] = field(default_factory=dict)
    indicator_values: Mapping[str, Any] = field(default_factory=dict)
    setup_predicates: Mapping[str, Any] = field(default_factory=dict)
    trigger_predicates: Mapping[str, Any] = field(default_factory=dict)
    unavailable_evidence: tuple[str, ...] = ()
    source_grade: str = "UNQUALIFIED"
    policy_id: str = ""
    policy_hash: str = ""

    @property
    def applicability_id(self) -> str:
        return hashlib.sha256(
            "|".join([self.cycle_id, self.symbol, self.strategy, self.native_due_at_ns.__str__(),
                      self.status, self.reason_code]).encode("utf-8")).hexdigest()

    def as_row(self) -> dict[str, Any]:
        return {
            "applicability_id": self.applicability_id,
            "cycle_id": self.cycle_id,
            "symbol": self.symbol,
            "strategy": self.strategy,
            "native_clock": self.native_clock,
            "native_due_at_ns": self.native_due_at_ns,
            "information_cutoff_ns": self.information_cutoff_ns,
            "universe_eligible": 1 if self.universe_eligible else 0,
            "status": self.status,
            "reason_code": self.reason_code,
            "detail": dict(self.detail),
            "indicator_values": dict(self.indicator_values),
            "setup_predicates": dict(self.setup_predicates),
            "trigger_predicates": dict(self.trigger_predicates),
            "unavailable_evidence": list(self.unavailable_evidence),
            "source_grade": self.source_grade,
            "policy_id": self.policy_id,
            "policy_hash": self.policy_hash,
        }


def evaluate_s1_watch_lifecycle(*, cycle_id: str, symbol: str, key: InstrumentKeyV2,
                                m15: Sequence[CausalBarV2], h1: Sequence[CausalBarV2],
                                h4: Sequence[CausalBarV2], universe_eligible: bool,
                                setup_cache: dict[int, Any],
                                max_trigger_bars: int = 32) -> list[ApplicabilityRecordV1]:
    """Evaluate the registered S1 watch lifecycle over the observed 15M sequence.

    The production coordinator creates a watch on a confirmed 15M close and
    triggers it on a later confirmed 15M close that breaks the *immediately
    prior* confirmed 15M high/low, with a four-bar expiry. This reproduces
    that lifecycle on genuine bars, and records the probe gaps (no original
    executable BBO, no event-gate identity) explicitly.
    """

    records: list[ApplicabilityRecordV1] = []
    if len(m15) < 2:
        return records
    if max_trigger_bars and len(m15) > max_trigger_bars + 8:
        m15 = m15[-(max_trigger_bars + 8):]
    m15_times = [int(bar.close_at_ns) for bar in m15]
    h1_times = [int(bar.close_at_ns) for bar in h1]
    h4_times = [int(bar.close_at_ns) for bar in h4]

    def _setup_at(index: int) -> Any:
        if index in setup_cache:
            return setup_cache[index]
        cutoff = m15_times[index]
        join = _joined(key, cutoff,
                       _slice(h4, h4_times, cutoff), _slice(h1, h1_times, cutoff),
                       _slice(m15, m15_times, cutoff))
        value: Any = None
        if len(join.h4) >= 50 and len(join.h1) >= 50:
            value = s1_trend._setup(join)
        setup_cache[index] = value
        return value

    for trigger_index in range(1, len(m15)):
        trigger_bar = m15[trigger_index]
        previous = m15[trigger_index - 1]
        cutoff = m15_times[trigger_index]
        join = _joined(key, cutoff, _slice(h4, h4_times, cutoff), _slice(h1, h1_times, cutoff),
                       _slice(m15, m15_times, cutoff))
        regime = s1_trend._regime(join) if len(join.h4) >= 50 else None
        atr_values = atr(list(m15[: trigger_index + 1]), 14)
        atr15 = atr_values[-1] if atr_values else None
        for offset in range(1, 5):
            setup_index = trigger_index - offset
            if setup_index < 0:
                continue
            setup = _setup_at(setup_index)
            setup_at_ns = m15_times[setup_index]
            record_cycle = f"{cycle_id}-T{cutoff}-S{setup_at_ns}"
            if setup is None:
                records.append(ApplicabilityRecordV1(
                    record_cycle, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], setup_at_ns, setup_at_ns,
                    universe_eligible, ApplicabilityStatusV1.NO_SETUP, "SETUP_RULE_FAILED",
                    detail={"watch_offset_bars": offset, "trigger_close_at_ns": cutoff},
                    source_grade="QUALIFIED_PRICE_PREDICATES",
                    policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash))
                continue
            side, extreme, setup_refs = setup
            indicators = {
                "h4_close": str(join.h4[-1].close),
                "h4_ema20": str(_ema_value(join.h4, 20)),
                "h4_ema50": str(_ema_value(join.h4, 50)),
                "atr15m": atr15,
                "trigger_close": str(trigger_bar.close),
                "prior_15m_high": str(previous.high),
                "prior_15m_low": str(previous.low),
            }
            setup_predicates = {
                "regime_side_at_setup": side.value,
                "regime_side_at_trigger": regime.value if regime is not None else None,
                "setup_side": side.value,
                "setup_extreme": str(extreme),
                "pullback_touched_and_reclaimed": True,
                "watch_offset_bars": offset,
            }
            crossed = (trigger_bar.close > previous.high) if side == V2Side.LONG else (
                trigger_bar.close < previous.low)
            trigger_predicates = {
                "prior_15m_high": str(previous.high),
                "prior_15m_low": str(previous.low),
                "close": float(trigger_bar.close),
                "breakout_confirmed": crossed,
            }
            if regime is not None and regime != side:
                records.append(ApplicabilityRecordV1(
                    record_cycle, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], setup_at_ns, setup_at_ns,
                    universe_eligible, ApplicabilityStatusV1.NO_SETUP, "CONTRARY_CONFIRMED_4H_REGIME",
                    indicator_values=indicators, setup_predicates=setup_predicates,
                    trigger_predicates=trigger_predicates,
                    source_grade="QUALIFIED_PRICE_PREDICATES",
                    policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash))
                continue
            if not crossed:
                records.append(ApplicabilityRecordV1(
                    record_cycle, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], setup_at_ns, setup_at_ns,
                    universe_eligible, ApplicabilityStatusV1.NO_SETUP, "TRIGGER_RULE_FAILED",
                    indicator_values=indicators, setup_predicates=setup_predicates,
                    trigger_predicates=trigger_predicates,
                    source_grade="QUALIFIED_PRICE_PREDICATES",
                    policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash))
                continue
            status = (ApplicabilityStatusV1.RESEARCH_ACTION if universe_eligible
                      else ApplicabilityStatusV1.INELIGIBLE)
            reason = "TRIGGER_CONFIRMED" if universe_eligible else "UNIVERSE_INELIGIBLE"
            records.append(ApplicabilityRecordV1(
                record_cycle, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], setup_at_ns, setup_at_ns,
                universe_eligible, status, reason,
                detail={"setup_refs": list(setup_refs), "side": side.value,
                        "trigger_close_at_ns": cutoff, "watch_offset_bars": offset},
                indicator_values=indicators, setup_predicates=setup_predicates,
                trigger_predicates=trigger_predicates,
                unavailable_evidence=("EXECUTABLE_BBO_AT_ORIGINAL_INSTANT", "EVENT_GATE_IDENTITY",
                                      "MARK_INDEX_IDENTITY"),
                source_grade="QUALIFIED_PRICE_PREDICATES_PROBE_GAPS",
                policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash))
    return records


def _slice(bars: Sequence[CausalBarV2], times: Sequence[int], cutoff_ns: int) -> list[CausalBarV2]:
    import bisect

    return list(bars[: bisect.bisect_right(times, cutoff_ns)])


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _joined(key: InstrumentKeyV2, cutoff_ns: int, h4: Sequence[CausalBarV2],
            h1: Sequence[CausalBarV2], m15: Sequence[CausalBarV2]) -> JoinedBars:
    return JoinedBars(key=key, cutoff_ns=cutoff_ns, h4=tuple(h4), h1=tuple(h1), m15=tuple(m15),
                      status="AVAILABLE", reason=None, source_health_ref=None)


def evaluate_s1(*, cycle_id: str, symbol: str, key: InstrumentKeyV2, cutoff_ns: int,
                h4: Sequence[CausalBarV2], h1: Sequence[CausalBarV2],
                m15: Sequence[CausalBarV2], universe_eligible: bool) -> ApplicabilityRecordV1:
    """S1 multi-timeframe trend pullback, native clock CONFIRMED_15M_CLOSE."""

    due = m15[-1].close_at_ns if m15 else cutoff_ns
    unavailable: list[str] = []
    if not m15:
        return ApplicabilityRecordV1(cycle_id, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "M15_HISTORY_UNAVAILABLE", unavailable_evidence=("M15_BARS",),
                                     policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash)
    join = _joined(key, cutoff_ns, h4, h1, m15)
    if len(join.h4) < 50 or len(join.h1) < 50:
        return ApplicabilityRecordV1(cycle_id, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "EMA_WARMUP_MISSING",
                                     detail={"h4_bars": len(join.h4), "h1_bars": len(join.h1)},
                                     unavailable_evidence=("H4_EMA50_WARMUP", "H1_EMA50_WARMUP"),
                                     policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash)
    regime = s1_trend._regime(join)
    setup = s1_trend._setup(join)
    indicators = {
        "h4_close": str(join.h4[-1].close),
        "h4_ema20": str(_ema_value(join.h4, 20)),
        "h4_ema50": str(_ema_value(join.h4, 50)),
        "h1_bars": len(join.h1),
        "m15_close": str(join.m15[-1].close),
    }
    setup_predicates = {
        "regime_side": regime.value if regime is not None else None,
        "pullback_touched_and_reclaimed": setup is not None,
        "setup_side": setup[0].value if setup is not None else None,
        "setup_extreme": str(setup[1]) if setup is not None else None,
    }
    trigger_predicates: dict[str, Any] = {}
    if setup is None:
        return ApplicabilityRecordV1(cycle_id, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP, "SETUP_RULE_FAILED",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash)
    side, extreme, setup_refs = setup
    # Registered trigger: the confirmed 15M close breaks the prior confirmed
    # 15M high/low; the stop band is 0.5-3 ATR(15M) around the setup extreme.
    m15_closes = [float(bar.close) for bar in join.m15]
    m15_closes[-1] = float(join.m15[-1].close)
    atr_values = atr(list(join.m15), 14)
    atr15 = atr_values[-1] if atr_values else None
    trigger_high = max(float(bar.high) for bar in join.m15[-2:]) if len(join.m15) >= 2 else None
    trigger_low = min(float(bar.low) for bar in join.m15[-2:]) if len(join.m15) >= 2 else None
    close = float(join.m15[-1].close)
    triggered = False
    if side == V2Side.LONG and trigger_high is not None:
        triggered = close > trigger_high
    elif side == V2Side.SHORT and trigger_low is not None:
        triggered = close < trigger_low
    stop_distance_atr = None
    if atr15:
        stop_distance_atr = abs(float(extreme) - close) / atr15
    trigger_predicates = {
        "prior_15m_high": trigger_high,
        "prior_15m_low": trigger_low,
        "close": close,
        "atr15m": atr15,
        "breakout_confirmed": triggered,
        "stop_distance_atr": stop_distance_atr,
        "stop_band_valid": None if stop_distance_atr is None else bool(0.5 <= stop_distance_atr <= 3.0),
    }
    indicators["atr15m"] = atr15
    if not triggered:
        return ApplicabilityRecordV1(cycle_id, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP, "TRIGGER_RULE_FAILED",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     trigger_predicates=trigger_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash)
    status = (ApplicabilityStatusV1.RESEARCH_ACTION if universe_eligible
              else ApplicabilityStatusV1.INELIGIBLE)
    reason = "TRIGGER_CONFIRMED" if universe_eligible else "UNIVERSE_INELIGIBLE"
    unavailable = ["EXECUTABLE_BBO_AT_ORIGINAL_INSTANT", "EVENT_GATE_IDENTITY", "SOURCE_HEALTH_IDENTITY"]
    return ApplicabilityRecordV1(cycle_id, symbol, "S1", STRATEGY_NATIVE_CLOCK["S1"], due, cutoff_ns,
                                 universe_eligible, status, reason,
                                 detail={"setup_refs": list(setup_refs), "side": side.value},
                                 indicator_values=indicators, setup_predicates=setup_predicates,
                                 trigger_predicates=trigger_predicates, unavailable_evidence=tuple(unavailable),
                                 source_grade="QUALIFIED_PRICE_PREDICATES_PROBE_GAPS",
                                 policy_id=s1_trend.POLICY_ID, policy_hash=s1_trend.S1_POLICY.policy_hash)


def _ema_value(bars: Sequence[CausalBarV2], period: int) -> Any:
    values = ema([float(bar.close) for bar in bars], period)
    return values[-1] if values else None


def evaluate_s2(*, cycle_id: str, symbol: str, key: InstrumentKeyV2, cutoff_ns: int,
                h4: Sequence[CausalBarV2], h1: Sequence[CausalBarV2],
                m15: Sequence[CausalBarV2], universe_eligible: bool,
                precomputed_atr: Sequence[float | None] | None = None,
                precomputed_widths: Sequence[float] | None = None,
                precomputed_closes: Sequence[float] | None = None) -> ApplicabilityRecordV1:
    """S2 compression breakout, native clock CONFIRMED_15M_CLOSE."""

    policy = s2_breakout.S2_POLICY
    due = m15[-1].close_at_ns if m15 else cutoff_ns
    if not m15:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "M15_HISTORY_UNAVAILABLE", unavailable_evidence=("M15_BARS",),
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if len(m15) < s2_breakout.SAMPLE_SIZE + 21:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "INSUFFICIENT_30_DAY_HISTORY_AFTER_WARMUP",
                                     detail={"m15_bars": len(m15), "required": s2_breakout.SAMPLE_SIZE + 21},
                                     unavailable_evidence=("M15_30_DAY_COMPARISON_SAMPLE",),
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if not h1 or not h4:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "MISSING_1H_OR_4H_CONTEXT",
                                     unavailable_evidence=("H1_CONTEXT", "H4_CONTEXT"),
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    pre = list(m15[:-1])
    if precomputed_atr is not None and precomputed_closes is not None:
        atr_values = list(precomputed_atr[: len(pre)])
        closes = list(precomputed_closes[: len(pre)])
    else:
        atr_values = atr(pre, 14)
        closes = [float(bar.close) for bar in pre]
    latest_atr = atr_values[-1]
    start = len(pre) - s2_breakout.SAMPLE_SIZE - 1
    sample_atr = atr_values[start:-1]
    if latest_atr is None or any(value is None or not math.isfinite(value) for value in sample_atr):
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "ATR_HISTORY_UNAVAILABLE",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if precomputed_widths is not None:
        widths = list(precomputed_widths[start:len(pre)])
    else:
        widths = []
        for index in range(start, len(pre)):
            window = closes[index - 19:index + 1]
            widths.append(4 * pstdev(window) / (math.fsum(window) / 20))
    if len(widths) != s2_breakout.SAMPLE_SIZE + 1:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "BOLLINGER_HISTORY_UNAVAILABLE",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    width_threshold = s2_breakout.empirical_quantile(tuple(widths[:-1]), 0.20)
    atr_threshold = float(median([float(value) for value in sample_atr if value is not None]))
    compression = s2_breakout.compression_passes(widths[-1], width_threshold, latest_atr, atr_threshold)
    atr_decimal = Decimal(str(latest_atr))
    range_bars = pre[-20:]
    range_high = max(bar.high for bar in range_bars)
    range_low = min(bar.low for bar in range_bars)
    width = range_high - range_low
    width_valid = s2_breakout.range_width_valid(width, atr_decimal)
    trigger = m15[-1]
    side = s2_breakout.breakout_side(trigger.close, range_high, range_low, atr_decimal)
    volume_median = median([bar.volume for bar in range_bars])
    volume_ok = trigger.volume > volume_median
    indicators = {
        "latest_bollinger_width20": widths[-1],
        "width_percentile_20": width_threshold,
        "latest_atr14": str(atr_decimal),
        "atr_median": atr_threshold,
        "range_high": str(range_high),
        "range_low": str(range_low),
        "range_width": str(width),
        "volume_median": str(volume_median),
        "trigger_close": str(trigger.close),
    }
    setup_predicates = {
        "compression_passes": compression,
        "range_width_valid": width_valid,
        "m15_bars": len(m15),
    }
    trigger_predicates = {
        "breakout_side": side.value if side is not None else None,
        "volume_above_median": volume_ok,
    }
    if not compression:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP, "COMPRESSION_RULE_FAILED",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     trigger_predicates=trigger_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if not width_valid:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP,
                                     "RANGE_WIDTH_OUTSIDE_0.5_TO_3_ATR",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     trigger_predicates=trigger_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if side is None:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP, "TRIGGER_RULE_FAILED",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     trigger_predicates=trigger_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    if not volume_ok:
        return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NO_SETUP, "VOLUME_RULE_FAILED",
                                     indicator_values=indicators, setup_predicates=setup_predicates,
                                     trigger_predicates=trigger_predicates,
                                     source_grade="QUALIFIED_PRICE_PREDICATES",
                                     policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)
    status = (ApplicabilityStatusV1.RESEARCH_ACTION if universe_eligible
              else ApplicabilityStatusV1.INELIGIBLE)
    reason = "TRIGGER_CONFIRMED" if universe_eligible else "UNIVERSE_INELIGIBLE"
    return ApplicabilityRecordV1(cycle_id, symbol, "S2", STRATEGY_NATIVE_CLOCK["S2"], due, cutoff_ns,
                                 universe_eligible, status, reason,
                                 indicator_values=indicators, setup_predicates=setup_predicates,
                                 trigger_predicates=trigger_predicates,
                                 unavailable_evidence=("EXECUTABLE_BBO_AT_ORIGINAL_INSTANT",),
                                 source_grade="QUALIFIED_PRICE_PREDICATES_PROBE_GAPS",
                                 policy_id=s2_breakout.POLICY_ID, policy_hash=policy.policy_hash)


def evaluate_s3(*, cycle_id: str, symbol: str, key: InstrumentKeyV2, cutoff_ns: int,
                m1: Sequence[CausalBarV2], universe_eligible: bool,
                exact_trade_population: bool) -> ApplicabilityRecordV1:
    """S3 VWAP/AR(1) reversion, native clock CONFIRMED_1M_CLOSE."""

    policy = s3_mean_reversion.policy_spec()
    due = m1[-1].close_at_ns if m1 else cutoff_ns
    if not m1:
        return ApplicabilityRecordV1(cycle_id, symbol, "S3", STRATEGY_NATIVE_CLOCK["S3"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "M1_HISTORY_UNAVAILABLE", unavailable_evidence=("M1_BARS",),
                                     policy_id=policy.policy_id, policy_hash=policy.policy_hash)
    if not exact_trade_population:
        return ApplicabilityRecordV1(cycle_id, symbol, "S3", STRATEGY_NATIVE_CLOCK["S3"], due, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "EXACT_NATIVE_TRADE_POPULATION_UNAVAILABLE",
                                     detail={"m1_bars": len(m1),
                                             "note": "aggregated trades are not the every-trade population "
                                                     "required by the registered S3 VWAP contract"},
                                     unavailable_evidence=("EVERY_TRADE_VWAP_WINDOW", "TRADE_VWAP_SNAPSHOT"),
                                     source_grade="SOURCE_UNAVAILABLE",
                                     policy_id=policy.policy_id, policy_hash=policy.policy_hash)
    return ApplicabilityRecordV1(cycle_id, symbol, "S3", STRATEGY_NATIVE_CLOCK["S3"], due, cutoff_ns,
                                 universe_eligible, ApplicabilityStatusV1.RESEARCH_ACTION,
                                 "SETUP_EVALUATED_WITH_EXACT_TRADE_POPULATION",
                                 unavailable_evidence=(),
                                 source_grade="QUALIFIED",
                                 policy_id=policy.policy_id, policy_hash=policy.policy_hash)


def evaluate_s4(*, cycle_id: str, symbol: str, cutoff_ns: int, depth_available: bool,
                trade_evidence_available: bool, universe_eligible: bool) -> ApplicabilityRecordV1:
    """S4 order-flow context. Contextual by registration: never a standalone signal."""

    policy_id = "S4_ORDER_FLOW_CONTEXT_V1"
    if not depth_available:
        return ApplicabilityRecordV1(cycle_id, symbol, "S4", STRATEGY_NATIVE_CLOCK["S4"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "ORDER_BOOK_EVIDENCE_UNAVAILABLE",
                                     unavailable_evidence=("DEPTH_SNAPSHOT",),
                                     source_grade="SOURCE_UNAVAILABLE", policy_id=policy_id)
    return ApplicabilityRecordV1(cycle_id, symbol, "S4", STRATEGY_NATIVE_CLOCK["S4"], cutoff_ns, cutoff_ns,
                                 universe_eligible, ApplicabilityStatusV1.CONTEXT_ONLY,
                                 "ORDER_FLOW_CONTEXT_AVAILABLE_NO_STANDALONE_DIRECTIONAL_SIGNAL",
                                 detail={"depth_available": True, "trade_evidence_available": trade_evidence_available},
                                 unavailable_evidence=() if trade_evidence_available else ("EXACT_TRADE_SEQUENCE",),
                                 source_grade="QUALIFIED_CONTEXT", policy_id=policy_id)


def evaluate_s5(*, cycle_id: str, symbol: str, cutoff_ns: int, oi_available: bool,
                funding_available: bool, liquidation_population_complete: bool,
                universe_eligible: bool) -> ApplicabilityRecordV1:
    """S5 crowding/deleveraging context. Contextual by registration."""

    policy_id = "S5_CROWDING_DELEVERAGING_CONTEXT_V1"
    missing: list[str] = []
    if not oi_available:
        missing.append("OPEN_INTEREST")
    if not funding_available:
        missing.append("FUNDING")
    if not liquidation_population_complete:
        missing.append("COMPLETE_LIQUIDATION_POPULATION")
    if missing:
        return ApplicabilityRecordV1(cycle_id, symbol, "S5", STRATEGY_NATIVE_CLOCK["S5"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "DERIVATIVES_EVIDENCE_INCOMPLETE", unavailable_evidence=tuple(missing),
                                     source_grade="SOURCE_UNAVAILABLE", policy_id=policy_id)
    return ApplicabilityRecordV1(cycle_id, symbol, "S5", STRATEGY_NATIVE_CLOCK["S5"], cutoff_ns, cutoff_ns,
                                 universe_eligible, ApplicabilityStatusV1.CONTEXT_ONLY,
                                 "CROWDING_CONTEXT_AVAILABLE_NO_STANDALONE_DIRECTIONAL_SIGNAL",
                                 source_grade="QUALIFIED_CONTEXT", policy_id=policy_id)


def evaluate_s6(*, cycle_id: str, symbol: str, cutoff_ns: int, eligible_breadth: int,
                hours_used: int, score: float | None, beta_btc: float | None,
                universe_eligible: bool) -> ApplicabilityRecordV1:
    """S6 cross-sectional relative strength, native clock CONFIRMED_1H_CLOSE."""

    from ...strategies.s6_cross_section import MIN_BREADTH
    from ...strategies.s6_cross_section import POLICY_ID as S6_POLICY_ID

    policy = s6_cross_section_policy()
    if eligible_breadth < MIN_BREADTH:
        return ApplicabilityRecordV1(cycle_id, symbol, "S6", STRATEGY_NATIVE_CLOCK["S6"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "ELIGIBLE_BREADTH_BELOW_REGISTERED_MINIMUM",
                                     detail={"eligible_breadth": eligible_breadth, "minimum": MIN_BREADTH},
                                     unavailable_evidence=("CROSS_SECTION_BREADTH",),
                                     source_grade="SOURCE_UNAVAILABLE",
                                     policy_id=S6_POLICY_ID, policy_hash=policy.policy_hash)
    if hours_used < 720:
        return ApplicabilityRecordV1(cycle_id, symbol, "S6", STRATEGY_NATIVE_CLOCK["S6"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "INSUFFICIENT_30_DAY_HOURLY_RETURNS",
                                     detail={"hours_used": hours_used, "required": 720},
                                     unavailable_evidence=("TRAILING_720_HOURLY_RETURNS",),
                                     source_grade="SOURCE_UNAVAILABLE",
                                     policy_id=S6_POLICY_ID, policy_hash=policy.policy_hash)
    status = (ApplicabilityStatusV1.RESEARCH_ACTION if score is not None and universe_eligible
              else ApplicabilityStatusV1.CONTEXT_ONLY)
    reason = ("RELATIVE_STRENGTH_STATE_AVAILABLE" if score is not None
              else "RELATIVE_STRENGTH_NOT_COMPUTED")
    return ApplicabilityRecordV1(cycle_id, symbol, "S6", STRATEGY_NATIVE_CLOCK["S6"], cutoff_ns, cutoff_ns,
                                 universe_eligible, status, reason,
                                 detail={"score": score, "beta_btc": beta_btc,
                                         "eligible_breadth": eligible_breadth, "hours_used": hours_used},
                                 unavailable_evidence=("S6_EXACT_ACTION_CONTRACT",),
                                 source_grade="QUALIFIED_PRICE_PREDICATES_PROBE_GAPS",
                                 policy_id=S6_POLICY_ID, policy_hash=policy.policy_hash)


def s6_cross_section_policy() -> Any:
    from ...strategies.s6_cross_section import policy_spec

    return policy_spec()


def evaluate_s7(*, cycle_id: str, symbol: str, cutoff_ns: int,
                event_calendar_available: bool, universe_eligible: bool) -> ApplicabilityRecordV1:
    """S7 verified event response. An unavailable calendar never implies CLEAR."""

    policy_id = "S7_VERIFIED_EVENT_CONTEXT_V1"
    if not event_calendar_available:
        return ApplicabilityRecordV1(cycle_id, symbol, "S7", STRATEGY_NATIVE_CLOCK["S7"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "EVENT_CALENDAR_COVERAGE_UNVERIFIED",
                                     detail={"note": "absence of an event calendar is not evidence of no event"},
                                     unavailable_evidence=("EVENT_CALENDAR", "VERIFIED_EVENT_PUBLICATION"),
                                     source_grade="SOURCE_UNAVAILABLE", policy_id=policy_id)
    return ApplicabilityRecordV1(cycle_id, symbol, "S7", STRATEGY_NATIVE_CLOCK["S7"], cutoff_ns, cutoff_ns,
                                 universe_eligible, ApplicabilityStatusV1.CONTEXT_ONLY,
                                 "EVENT_CONTEXT_AVAILABLE", source_grade="QUALIFIED_CONTEXT", policy_id=policy_id)


def evaluate_s8(*, cycle_id: str, symbol: str, cutoff_ns: int, pair_defined: bool,
                hours_used: int, universe_eligible: bool) -> ApplicabilityRecordV1:
    """S8 registered pairs research basket. Two-leg, never a single-instrument signal."""

    from ...strategies.s8_pairs import S8_PROFILE_ID as S8_POLICY_ID

    policy_id = S8_POLICY_ID
    if not pair_defined:
        return ApplicabilityRecordV1(cycle_id, symbol, "S8", STRATEGY_NATIVE_CLOCK["S8"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.NOT_DUE,
                                     "NO_REGISTERED_PAIR_FOR_SYMBOL",
                                     unavailable_evidence=("REGISTERED_PAIR_CATALOG",),
                                     source_grade="NOT_APPLICABLE", policy_id=policy_id)
    if hours_used < 720:
        return ApplicabilityRecordV1(cycle_id, symbol, "S8", STRATEGY_NATIVE_CLOCK["S8"], cutoff_ns, cutoff_ns,
                                     universe_eligible, ApplicabilityStatusV1.SOURCE_UNAVAILABLE,
                                     "INSUFFICIENT_PAIR_FIT_HISTORY",
                                     detail={"hours_used": hours_used, "required": 720},
                                     unavailable_evidence=("SYNCHRONIZED_PAIR_HISTORY_720H",),
                                     source_grade="SOURCE_UNAVAILABLE", policy_id=policy_id)
    return ApplicabilityRecordV1(cycle_id, symbol, "S8", STRATEGY_NATIVE_CLOCK["S8"], cutoff_ns, cutoff_ns,
                                 universe_eligible, ApplicabilityStatusV1.RESEARCH_ACTION,
                                 "TWO_LEG_RESEARCH_BASKET_AVAILABLE",
                                 unavailable_evidence=(),
                                 source_grade="QUALIFIED", policy_id=policy_id)


def evaluate_path_b(*, cycle_id: str, symbol: str, cutoff_ns: int, eligible: bool,
                    novelty_score: float | None, universe_eligible: bool) -> ApplicabilityRecordV1:
    """Path B bounded anomaly/novelty exploration (registered discovery surrogate)."""

    policy_id = "PATH_B_BOUNDED_ANOMALY_EXPLORATION_V1"
    if not eligible:
        return ApplicabilityRecordV1(cycle_id, symbol, "PATH_B", STRATEGY_NATIVE_CLOCK["PATH_B"], cutoff_ns,
                                     cutoff_ns, universe_eligible, ApplicabilityStatusV1.INELIGIBLE,
                                     "NOT_IN_ELIGIBLE_DISCOVERY_POPULATION",
                                     source_grade="NOT_APPLICABLE", policy_id=policy_id)
    if novelty_score is None:
        return ApplicabilityRecordV1(cycle_id, symbol, "PATH_B", STRATEGY_NATIVE_CLOCK["PATH_B"], cutoff_ns,
                                     cutoff_ns, universe_eligible, ApplicabilityStatusV1.RESOURCE_NOT_EVALUATED,
                                     "NOVELTY_SCREEN_NOT_EVALUATED",
                                     unavailable_evidence=("NOVELTY_SCREEN",),
                                     source_grade="UNQUALIFIED", policy_id=policy_id)
    return ApplicabilityRecordV1(cycle_id, symbol, "PATH_B", STRATEGY_NATIVE_CLOCK["PATH_B"], cutoff_ns,
                                 cutoff_ns, universe_eligible, ApplicabilityStatusV1.RESEARCH_ACTION,
                                 "BOUNDED_NOVELTY_SCREEN_ELIGIBLE",
                                 detail={"novelty_score": novelty_score},
                                 source_grade="RESEARCH_EXPLORATION", policy_id=policy_id)
