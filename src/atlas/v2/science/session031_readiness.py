"""Deterministic, profitability-independent S1/S2/S3 evidence readiness checks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import sha256_json, timestamp
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.strategies.s1_trend import S1_POLICY, EventGate, ExecutableQuote, MarkIndexEvidence
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import AR_OBSERVATION_COUNT, S3_POLICY, fit_ar1

M15_NS = BarIntervalV2.M15.duration_ns
M1_NS = BarIntervalV2.M1.duration_ns
S2_REQUIRED_M15_BARS = 2_901
S1_REQUIRED_H4_BARS = 50
S1_REQUIRED_H1_BARS = 50
S1_REQUIRED_M15_FEATURE_BARS = 20
S3_REQUIRED_M1_BARS = AR_OBSERVATION_COUNT
S3_STANDARDIZATION_PRECEDING = 120


@dataclass(frozen=True)
class SleeveReadinessV1:
    sleeve: str
    status: str
    reason_codes: tuple[str, ...]
    observed_counts: tuple[tuple[str, int | None], ...]
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.status not in {"READY", "NOT_ESTIMABLE", "NO_CANDIDATE"}:
            raise ValueError("unsupported sleeve readiness status")
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise ValueError("readiness reasons must be sorted and unique")
        if tuple(sorted(set(self.evidence_refs))) != self.evidence_refs:
            raise ValueError("readiness refs must be sorted and unique")
        if tuple(sorted(self.observed_counts)) != self.observed_counts:
            raise ValueError("readiness counts must have stable order")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "SLEEVE_READINESS_V1", "sleeve": self.sleeve, "status": self.status,
                "reason_codes": list(self.reason_codes),
                "observed_counts": dict(self.observed_counts),
                "evidence_refs": list(self.evidence_refs)}


@dataclass(frozen=True)
class StrategyEvidenceSnapshotV1:
    key: InstrumentKeyV2
    cutoff_ns: int
    bars: Mapping[BarIntervalV2, tuple[CausalBarV2, ...]]
    source_health_current: bool
    trade_source_health_current: bool
    fresh_quote: ExecutableQuote | None
    fresh_mark_index: MarkIndexEvidence | None
    event_gate: EventGate | None
    point_in_time_universe_eligible: bool
    s1_feature_ready: bool
    s1_active_watch: bool
    s1_subsequent_close_evidence: bool
    s2_feature_ready: bool
    public_trade_count: int
    trade_vwap_count: int
    residual_values: tuple[float, ...]
    residual_refs: tuple[str, ...]
    trade_refs: tuple[str, ...]
    residual_close_times_ns: tuple[int, ...] = ()
    residual_vwap_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="readiness.cutoff_ns")
        if self.public_trade_count < 0 or self.trade_vwap_count < 0:
            raise ValueError("readiness evidence counts cannot be negative")
        normalized: dict[BarIntervalV2, tuple[CausalBarV2, ...]] = {}
        for interval, items in self.bars.items():
            frame = BarIntervalV2(interval)
            ordered = tuple(items)
            if any(not isinstance(item, CausalBarV2) or item.interval != frame for item in ordered):
                raise ValueError("readiness bars must be typed and grouped by their exact interval")
            normalized[frame] = ordered
        object.__setattr__(self, "bars", normalized)
        for name in ("residual_refs", "trade_refs", "residual_vwap_refs"):
            values = tuple(sorted(set(getattr(self, name))))
            object.__setattr__(self, name, values)
        if any(type(value) is not int or value < 0 for value in self.residual_close_times_ns):
            raise ValueError("residual close times must be UTC nanosecond timestamps")
        if self.residual_close_times_ns and len(self.residual_close_times_ns) != len(self.residual_values):
            raise ValueError("residual timestamps and values must align one-to-one")


def _continuous_tail(
    rows: Sequence[CausalBarV2], *, key: InstrumentKeyV2, cutoff_ns: int, interval: BarIntervalV2,
) -> tuple[tuple[CausalBarV2, ...], int]:
    available = tuple(sorted((
        item for item in rows
        if item.final and item.close_at_ns <= cutoff_ns
        and item.instrument_revision == key.contract_revision and item.raw.available_at_ns <= cutoff_ns
    ), key=lambda item: (item.open_at_ns, item.content_hash)))
    deduped: dict[int, CausalBarV2] = {}
    for item in available:
        prior = deduped.get(item.open_at_ns)
        if prior is None or (item.raw.available_at_ns, item.content_hash) > (prior.raw.available_at_ns, prior.content_hash):
            deduped[item.open_at_ns] = item
    ordered = tuple(deduped[opened] for opened in sorted(deduped))
    if not ordered:
        return (), 0
    start = len(ordered) - 1
    while start > 0 and ordered[start].open_at_ns - ordered[start - 1].open_at_ns == interval.duration_ns:
        start -= 1
    tail = ordered[start:]
    gaps = max(0, len(ordered) - len(tail))
    return tail, gaps


def _result(
    sleeve: str, reasons: set[str], counts: Mapping[str, int | None], refs: set[str],
) -> SleeveReadinessV1:
    sorted_reasons = tuple(sorted(reasons))
    return SleeveReadinessV1(
        sleeve, "READY" if not reasons else "NOT_ESTIMABLE", sorted_reasons,
        tuple(sorted(counts.items())), tuple(sorted(refs)),
    )


def evaluate_strategy_readiness_v1(snapshot: StrategyEvidenceSnapshotV1) -> dict[str, Any]:
    """Evaluate persisted evidence inputs without invoking strategy candidate generation."""
    key, cutoff = snapshot.key, snapshot.cutoff_ns
    tails: dict[BarIntervalV2, tuple[CausalBarV2, ...]] = {}
    gaps: dict[BarIntervalV2, int] = {}
    for interval in BarIntervalV2:
        tails[interval], gaps[interval] = _continuous_tail(
            snapshot.bars.get(interval, ()), key=key, cutoff_ns=cutoff, interval=interval,
        )
    refs: set[str] = {
        bar.content_hash
        for interval, count in ((BarIntervalV2.M1, S3_REQUIRED_M1_BARS),
                                (BarIntervalV2.M15, S2_REQUIRED_M15_BARS),
                                (BarIntervalV2.H1, S1_REQUIRED_H1_BARS),
                                (BarIntervalV2.H4, S1_REQUIRED_H4_BARS))
        for bar in tails[interval][-count:]
    }
    if snapshot.fresh_quote is not None:
        refs.add(snapshot.fresh_quote.evidence_ref)
    if snapshot.fresh_mark_index is not None:
        refs.add(snapshot.fresh_mark_index.evidence_ref)
    if snapshot.event_gate is not None:
        refs.add(snapshot.event_gate.evidence_ref)
    refs.update(snapshot.trade_refs)
    refs.update(snapshot.residual_refs)

    s1_reasons: set[str] = set()
    h4, h1, m15 = (tails[BarIntervalV2.H4], tails[BarIntervalV2.H1], tails[BarIntervalV2.M15])
    s1_counts = {"confirmed_4h": len(h4), "confirmed_1h": len(h1), "confirmed_15m": len(m15),
                 "active_watch": int(snapshot.s1_active_watch), "contiguity_gaps_4h": gaps[BarIntervalV2.H4],
                 "contiguity_gaps_1h": gaps[BarIntervalV2.H1], "contiguity_gaps_15m": gaps[BarIntervalV2.M15]}
    if len(h4) < S1_REQUIRED_H4_BARS:
        s1_reasons.add("S1_CONFIRMED_4H_EMA_HISTORY_MISSING")
    if len(h1) < S1_REQUIRED_H1_BARS:
        s1_reasons.add("S1_CONFIRMED_1H_SETUP_HISTORY_MISSING")
    if len(m15) < S1_REQUIRED_M15_FEATURE_BARS:
        s1_reasons.add("S1_CONFIRMED_15M_TRIGGER_OR_FEATURE_HISTORY_MISSING")
    if ((len(h4) < S1_REQUIRED_H4_BARS and gaps[BarIntervalV2.H4])
            or (len(h1) < S1_REQUIRED_H1_BARS and gaps[BarIntervalV2.H1])
            or (len(m15) < S1_REQUIRED_M15_FEATURE_BARS and gaps[BarIntervalV2.M15])):
        s1_reasons.add("S1_REQUIRED_BAR_CONTINUITY_GAP")
    if not snapshot.s1_feature_ready:
        s1_reasons.add("S1_EMA_ATR_OR_REALIZED_VARIANCE_FEATURE_NOT_READY")
    if not snapshot.s1_subsequent_close_evidence:
        s1_reasons.add("S1_SUBSEQUENT_CONFIRMED_CLOSE_BEHAVIOR_UNOBSERVED")
    if not snapshot.source_health_current:
        s1_reasons.add("S1_REQUIRED_SOURCE_HEALTH_NOT_CURRENT")
    if snapshot.fresh_quote is None or not snapshot.fresh_quote.valid_at(cutoff):
        s1_reasons.add("S1_FRESH_BBO_MISSING_OR_STALE")
    if snapshot.fresh_mark_index is None or not snapshot.fresh_mark_index.valid_at(cutoff):
        s1_reasons.add("S1_MARK_INDEX_MISSING_OR_STALE")
    if snapshot.event_gate is None or not snapshot.event_gate.valid_at(cutoff):
        s1_reasons.add("S1_EVENT_GATE_MISSING_OR_STALE")
    elif snapshot.event_gate.state.value == "BLOCKED":
        s1_reasons.add("S1_EVENT_GATE_BLOCKED")
    if not snapshot.point_in_time_universe_eligible:
        s1_reasons.add("S1_POINT_IN_TIME_INSTRUMENT_ELIGIBILITY_UNAVAILABLE")
    s1 = _result("S1", s1_reasons, s1_counts, refs)

    s2_reasons: set[str] = set()
    s2_m15 = tails[BarIntervalV2.M15]
    s2_counts = {"confirmed_15m_contiguous": len(s2_m15), "required_15m_contiguous": S2_REQUIRED_M15_BARS,
                 "comparison_measurements_required": 2_880, "comparison_measurements_available": max(0, len(s2_m15) - 21),
                 "required_preceding_indicator_range_bars": 21, "contiguity_gaps_15m": gaps[BarIntervalV2.M15],
                 "confirmed_1h": len(h1), "confirmed_4h": len(h4)}
    if len(s2_m15) < S2_REQUIRED_M15_BARS:
        s2_reasons.add("S2_INSUFFICIENT_2901_CONTIGUOUS_15M_BARS")
    if gaps[BarIntervalV2.M15] and len(s2_m15) < S2_REQUIRED_M15_BARS:
        s2_reasons.add("S2_FRAGMENTED_15M_COMPARISON_HISTORY")
    if not s2_m15 or cutoff - s2_m15[-1].close_at_ns > 5_000_000_000:
        s2_reasons.add("S2_LATEST_CONFIRMED_15M_TRIGGER_STALE_OR_MISSING")
    if len(s2_m15) >= S2_REQUIRED_M15_BARS and len(s2_m15) - 21 < 2_880:
        s2_reasons.add("S2_COMPARISON_WINDOW_OR_PRECEDING_INDICATOR_HISTORY_MISSING")
    if not snapshot.s2_feature_ready:
        s2_reasons.add("S2_ATR_BOLLINGER_OR_VOLUME_INDICATOR_NOT_READY")
    if not h1:
        s2_reasons.add("S2_CONFIRMED_H1_CONTEXT_MISSING")
    if not h4:
        s2_reasons.add("S2_CONFIRMED_H4_CONTEXT_MISSING")
    if snapshot.fresh_quote is None or not snapshot.fresh_quote.valid_at(cutoff, 5_000_000_000):
        s2_reasons.add("S2_FRESH_BBO_MISSING_OR_STALE")
    if not snapshot.point_in_time_universe_eligible:
        s2_reasons.add("S2_POINT_IN_TIME_UNIVERSE_ELIGIBILITY_UNAVAILABLE")
    if not snapshot.source_health_current:
        s2_reasons.add("S2_REQUIRED_SOURCE_HEALTH_NOT_CURRENT")
    s2 = _result("S2", s2_reasons, s2_counts, refs)

    s3_reasons: set[str] = set()
    m1 = tails[BarIntervalV2.M1]
    residual_count = len(snapshot.residual_values)
    s3_counts = {"confirmed_1m_contiguous": len(m1), "required_1m_contiguous": S3_REQUIRED_M1_BARS,
                 "causal_trade_observations": snapshot.public_trade_count,
                 "historical_trade_vwap_refs": snapshot.trade_vwap_count,
                 "causal_residual_observations": residual_count,
                 "strictly_preceding_standardization_residuals": min(
                     S3_STANDARDIZATION_PRECEDING, max(0, residual_count - 1),
                 ),
                 "residual_vwap_refs": len(snapshot.residual_vwap_refs),
                 "required_standardization_residuals": S3_STANDARDIZATION_PRECEDING}
    if len(m1) < S3_REQUIRED_M1_BARS:
        s3_reasons.add("S3_INSUFFICIENT_10081_CONTIGUOUS_M1_BARS")
    if gaps[BarIntervalV2.M1] and len(m1) < S3_REQUIRED_M1_BARS:
        s3_reasons.add("S3_ONE_MINUTE_SOURCE_GAP")
    if snapshot.public_trade_count <= 0 or not snapshot.trade_refs:
        s3_reasons.add("S3_PUBLIC_TRADE_EVIDENCE_MISSING")
    if snapshot.trade_vwap_count < S3_REQUIRED_M1_BARS:
        s3_reasons.add("S3_HISTORICAL_TRADE_DERIVED_VWAP_HISTORY_MISSING")
    if residual_count < S3_REQUIRED_M1_BARS or len(snapshot.residual_refs) < S3_REQUIRED_M1_BARS:
        s3_reasons.add("S3_CAUSAL_RESIDUAL_AR_WINDOW_INCOMPLETE")
    if residual_count < S3_STANDARDIZATION_PRECEDING + 1:
        s3_reasons.add("S3_STRICTLY_PRECEDING_120_RESIDUALS_MISSING")
    if len(snapshot.residual_vwap_refs) < S3_REQUIRED_M1_BARS:
        s3_reasons.add("S3_RESIDUALS_LACK_EXACT_HISTORICAL_TRADE_VWAP_REFS")
    required_residual_times = snapshot.residual_close_times_ns[-S3_REQUIRED_M1_BARS:]
    if (len(required_residual_times) < S3_REQUIRED_M1_BARS
            or any(right - left != M1_NS for left, right in zip(required_residual_times,
                                                                 required_residual_times[1:], strict=False))):
        s3_reasons.add("S3_CAUSAL_RESIDUAL_TIMESTAMPS_NOT_CONTIGUOUS")
    if not snapshot.trade_source_health_current:
        s3_reasons.add("S3_TRADE_SOURCE_HEALTH_NOT_CURRENT")
    if not snapshot.source_health_current:
        s3_reasons.add("S3_BAR_SOURCE_HEALTH_NOT_CURRENT")
    if not tails[BarIntervalV2.M15] or not h4:
        s3_reasons.add("S3_CONFIRMED_M15_OR_H4_CONTEXT_MISSING")
    if snapshot.fresh_quote is None or not snapshot.fresh_quote.valid_at(cutoff, 1_000_000_000):
        s3_reasons.add("S3_BBO_EXCEEDS_ONE_SECOND_AGE_OR_MISSING")
    if snapshot.event_gate is None or not snapshot.event_gate.valid_at(cutoff):
        s3_reasons.add("S3_EVENT_EVIDENCE_MISSING_OR_STALE")
    if not snapshot.point_in_time_universe_eligible:
        s3_reasons.add("S3_POINT_IN_TIME_UNIVERSE_ELIGIBILITY_UNAVAILABLE")
    ar: dict[str, float | None] = {"alpha": None, "phi": None, "half_life_minutes": None}
    if (len(snapshot.residual_values) >= S3_REQUIRED_M1_BARS and len(m1) >= S3_REQUIRED_M1_BARS
            and len(required_residual_times) >= S3_REQUIRED_M1_BARS
            and tuple(bar.close_at_ns for bar in m1[-S3_REQUIRED_M1_BARS:]) == required_residual_times
            and all(
        right.open_at_ns - left.open_at_ns == M1_NS
        for left, right in zip(m1[-S3_REQUIRED_M1_BARS:], m1[-S3_REQUIRED_M1_BARS + 1:], strict=False)
    )):
        try:
            alpha, phi, half_life = fit_ar1(snapshot.residual_values[-S3_REQUIRED_M1_BARS:])
            ar = {"alpha": alpha, "phi": phi, "half_life_minutes": half_life}
            if not 5 <= half_life <= 30:
                s3_reasons.add("S3_AR_HALF_LIFE_OUTSIDE_SUPPORTED_RANGE")
        except (ValueError, ArithmeticError):
            s3_reasons.add("S3_AR_PARAMETERS_NOT_ESTIMABLE")
    else:
        s3_reasons.add("S3_AR_INPUTS_NOT_CONTIGUOUS_OR_INCOMPLETE")
    s3 = _result("S3", s3_reasons, s3_counts, refs)

    return {
        "version": "SESSION031_STRATEGY_READINESS_V1",
        "instrument": key.to_dict(), "cutoff_ns": cutoff,
        "policy_hashes": {"S1": S1_POLICY.policy_hash, "S2": S2_POLICY.policy_hash, "S3": S3_POLICY.policy_hash},
        "sleeves": {"S1": s1.to_dict(), "S2": s2.to_dict(), "S3": s3.to_dict()},
        "s3_ar_diagnostic": ar,
        "candidate_activity_created": False,
        "available_at_cutoff_enforced": True,
        "economic_value_claim": "NOT ESTIMABLE",
    }


def s3_cadence_report_v1(
    *, expected_native_m1_origins: Sequence[int], actual_production_handoff_origins: Sequence[int],
    source_gap_origins: Sequence[int] = (), policy_eligibility_gap_origins: Sequence[int] = (),
    warmup_incomplete_origins: Sequence[int] = (), replay_view: str = "ACTUAL_SYSTEM",
) -> dict[str, Any]:
    expected = tuple(sorted(set(expected_native_m1_origins)))
    actual = tuple(sorted(set(actual_production_handoff_origins)))
    expected_set, actual_set = set(expected), set(actual)
    missing = tuple(sorted(expected_set - actual_set))
    categories = {
        "genuine_source_gaps": tuple(sorted(set(source_gap_origins) & set(missing))),
        "policy_eligibility_gaps": tuple(sorted(set(policy_eligibility_gap_origins) & set(missing))),
        "warmup_incomplete_gaps": tuple(sorted(set(warmup_incomplete_origins) & set(missing))),
    }
    categorized = set().union(*(set(items) for items in categories.values()))
    return {
        "version": "S3_NATIVE_M1_CADENCE_REPORT_V1",
        "expected_native_s3_one_minute_origins": list(expected),
        "actual_delivered_production_handoff_origins": list(actual),
        "missing_or_uncovered_origins": list(missing),
        "unclassified_missing_origins": sorted(set(missing) - categorized),
        "gaps_by_cause": {name: list(values) for name, values in categories.items()},
        "actual_production_cadence_ns": M15_NS,
        "required_native_s3_cadence_ns": M1_NS,
        "native_one_minute_coverage": "UNVERIFIED" if not expected else "TEST GATE" if missing else "TESTED",
        "replay_view": replay_view,
        "replay_view_limitation": "ACTUAL_SYSTEM_AND_RECONSTRUCTED_MARKET_ARE_SEPARATE_CAUSAL_VIEWS",
        "production_cadence_changed": False,
    }


def readiness_identity(report: Mapping[str, Any]) -> str:
    return sha256_json(dict(report))
