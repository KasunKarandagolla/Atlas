"""Causal, shadow-only S2 compression breakout and pure failed-break rule."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from statistics import median, pstdev

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    EligibilityStatusV2,
    FeatureArtifactV2,
    PolicySpecV2,
    V2Side,
)
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.technical import atr
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import ExecutableQuote

POLICY_ID = "S2_COMPRESSION_BREAKOUT"
POLICY_VERSION = "1.0.0-shadow"
QUANTILE_VERSION = "S2_EMPIRICAL_QUANTILE_V1"
SAMPLE_SIZE = 30 * 24 * 4
M15_NS = BarIntervalV2.M15.duration_ns
HOUR_NS = BarIntervalV2.H1.duration_ns
FRESHNESS_NS = 5_000_000_000


def empirical_quantile(values: tuple[float, ...], p: float) -> float:
    """Finite nearest-rank percentile: sorted[ceil(p*n)-1]."""
    if not values or not 0 < p <= 1 or not all(math.isfinite(x) for x in values):
        raise ValueError("finite nonempty sample and 0 < p <= 1 required")
    return sorted(values)[math.ceil(p * len(values)) - 1]


def policy_spec(*, variant: str = "BASELINE") -> PolicySpecV2:
    variants = {
        "RAW_RANGE": (False, False, "immediate"),
        "COMPRESSION_ONLY": (True, False, "immediate"),
        "VOLUME_ONLY": (False, True, "immediate"),
        "BASELINE": (True, True, "immediate"),
        "DELAYED_RETEST": (True, True, "separate_delayed_retest_research_hypothesis"),
    }
    if variant not in variants:
        raise ValueError("unsupported S2 experiment")
    compression, volume, entry_mode = variants[variant]
    return PolicySpecV2.build(
        policy_id=POLICY_ID + ("_" + variant if variant != "BASELINE" else ""),
        version=POLICY_VERSION, strategy_family="COMPRESSION_BREAKOUT", capital_status="SHADOW_ONLY",
        decision_event="CONFIRMED_15M_CLOSE",
        required_features=tuple(sorted(("confirmed_15m_ohlcv", "latest_confirmed_1h_context",
            "latest_confirmed_4h_context", "atr14", "bollinger_width20", "volume", "executable_bbo_spread"))),
        optional_features=(),
        timeframe_rules=FrozenMap({"trigger": "confirmed_15M", "context": "latest_confirmed_1H_4H_refs_no_direction_gate",
            "bar_revision": "contract_revision", "identity": "exact_full_InstrumentKeyV2"}),
        setup_parameters=FrozenMap({"variant": variant, "compression_enabled": compression,
            "volume_enabled": volume, "comparison_sample_15m_bars": SAMPLE_SIZE,
            "comparison_excludes_latest_t_minus_1_and_trigger": True,
            "latest_measurement": "completed_t_minus_1", "bollinger_period": 20,
            "bollinger_width_formula": "4_population_stddev_close20_div_arithmetic_mean_close20",
            "bollinger_width_percentile": "0.20", "quantile_convention": QUANTILE_VERSION,
            "quantile_rank": "ceil(p*n)", "atr_period": 14, "atr_convention": "TECHNICAL_V1_WILDER_SEEDED_FIRST_14_TR_MEAN",
            "atr_median": "ordinary_empirical_median",
            "volume_median": "ordinary_empirical_median",
            "strict_compression_below": True, "range_bars": 20, "range_excludes_trigger": True,
            "volume_median_bars": 20, "strict_volume_above": True,
            "experiment_only": variant != "BASELINE"}),
        direction_rule=FrozenMap({"long": "trigger_close_gt_range_high_plus_0.1_pre_trigger_ATR",
            "short": "trigger_close_lt_range_low_minus_0.1_pre_trigger_ATR",
            "exact_0.1_ATR_boundary": "NO_CANDIDATE"}),
        entry_rule=FrozenMap({"time_in_force": "IOC", "order_type": "LIMIT", "shadow_only": True,
            "entry_mode": entry_mode, "no_same_epoch_reprice": True,
            "bbo_max_age_ns": FRESHNESS_NS, "confirmed_bar_max_lag_ns": FRESHNESS_NS,
            "candidate_deadline_offset_ns": FRESHNESS_NS}),
        collar_rule=FrozenMap({"adverse_bps": 5, "long_reference": "ask", "short_reference": "bid"}),
        stop_rule=FrozenMap({"long": "frozen_range_low", "short": "frozen_range_high",
            "minimum_range_width_atr": "0.5", "maximum_range_width_atr": "3",
            "boundaries_inclusive": True, "dynamic_widening": False}),
        trigger_basis="CONFIRMED_CLOSE", management_rule=FrozenMap({"fixed_stop": True,
            "failed_break_first_subsequent_closed_15m_bars": 2,
            "inside_range": "range_low <= close <= range_high", "pyramiding": False,
            "discretionary_stop_widening": False, "same_epoch_repricing": False}),
        time_exit_rule=FrozenMap({"after_ns": 2 * HOUR_NS, "type": "TIME_EXIT"}),
        max_hold_ns=2 * HOUR_NS, expiry_rule=FrozenMap({"watch": "none",
            "candidate_deadline_offset_ns": FRESHNESS_NS}), model_requirements=(),
    )


S2_POLICY = policy_spec()


def experiment_specs() -> tuple[PolicySpecV2, ...]:
    return tuple(policy_spec(variant=name) for name in (
        "RAW_RANGE", "COMPRESSION_ONLY", "VOLUME_ONLY", "BASELINE", "DELAYED_RETEST"))


@dataclass(frozen=True)
class S2Decision:
    status: str
    reason: str
    candidate: CandidateActionV2 | None = None
    setup_ref: str | None = None
    trigger_ref: str | None = None


def _index(repository: OpsRepository, ref: str, kind: str, at_ns: int, body: Mapping[str, object]) -> None:
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at_ns, at_ns, body))


def compression_passes(width: float, width_threshold: float, atr_value: float, atr_median: float) -> bool:
    return width < width_threshold and atr_value < atr_median


def range_width_valid(width: Decimal, pre_trigger_atr: Decimal) -> bool:
    return Decimal("0.5") * pre_trigger_atr <= width <= Decimal(3) * pre_trigger_atr


def breakout_side(close: Decimal, range_high: Decimal, range_low: Decimal,
                  pre_trigger_atr: Decimal) -> V2Side | None:
    if close > range_high + Decimal("0.1") * pre_trigger_atr:
        return V2Side.LONG
    if close < range_low - Decimal("0.1") * pre_trigger_atr:
        return V2Side.SHORT
    return None


class S2ShadowCoordinator:
    def __init__(self, repository: OpsRepository, *, policy: PolicySpecV2 = S2_POLICY,
                 cost_model_ref: str = "S2_SHADOW_COST_UNESTIMATED_V1") -> None:
        if policy.policy_hash != S2_POLICY.policy_hash or policy.policy_id != POLICY_ID:
            raise ValueError("baseline coordinator requires exact S2 baseline policy")
        self.repository = repository
        self.policy = policy
        self.cost_model_ref = cost_model_ref

    def on_trigger_close(self, join: JoinedBars, feature: FeatureArtifactV2, *,
                         universe: UniverseContractV2, bbo: ExecutableQuote | None) -> S2Decision:
        if join.status != "AVAILABLE" or join.key != feature.key or feature.information_cutoff_ns != join.cutoff_ns:
            return S2Decision("NOT_ESTIMABLE", join.reason or "FEATURE_OR_JOIN_MISMATCH")
        cutoff = join.cutoff_ns
        if not join.m15 or join.m15[-1].close_at_ns != cutoff or cutoff - join.m15[-1].close_at_ns > FRESHNESS_NS:
            return S2Decision("NOT_ESTIMABLE", "TRIGGER_NOT_LATEST_CONFIRMED_CLOSE")
        if not join.h1:
            return S2Decision("NOT_ESTIMABLE", "MISSING_1H_CONTEXT")
        if not join.h4:
            return S2Decision("NOT_ESTIMABLE", "MISSING_4H_CONTEXT")
        bars = join.m15
        if len(bars) < SAMPLE_SIZE + 21:
            return S2Decision("NOT_ESTIMABLE", "INSUFFICIENT_30_DAY_HISTORY_AFTER_WARMUP")
        used = bars[-(SAMPLE_SIZE + 21):]
        if any(not bar.final or bar.instrument_revision != join.key.contract_revision
               or bar.raw.available_at_ns > cutoff or bar.close_at_ns > cutoff for bar in bars + join.h1[-1:] + join.h4[-1:]):
            return S2Decision("NOT_ESTIMABLE", "UNCONFIRMED_UNAVAILABLE_OR_WRONG_REVISION")
        if any(b.close_at_ns - a.close_at_ns != M15_NS for a, b in zip(used, used[1:], strict=False)):
            return S2Decision("NOT_ESTIMABLE", "INCOMPLETE_30_DAY_HISTORY")
        if bbo is None or bbo.key != join.key or not bbo.valid_at(cutoff, FRESHNESS_NS):
            return S2Decision("NOT_ESTIMABLE", "BBO_STALE_OR_UNAVAILABLE")
        if (feature.envelope.available_at_ns > cutoff or
                not {join.m15[-1].content_hash, join.h1[-1].content_hash,
                     join.h4[-1].content_hash}.issubset(feature.envelope.input_refs)):
            return S2Decision("NOT_ESTIMABLE", "FEATURE_NOT_CAUSAL_FOR_TRIGGER")
        if universe.envelope.available_at_ns > cutoff or cutoff > universe.decision_slot_ns:
            return S2Decision("NOT_ESTIMABLE", "UNIVERSE_NOT_AVAILABLE_AT_CUTOFF")
        entries = [entry for entry in universe.entries if entry.key == join.key]
        if len(entries) != 1 or not entries[0].data_eligible or not entries[0].scanner_eligible or (
            POLICY_ID not in entries[0].strategy_eligibility or
            entries[0].strategy_eligibility[POLICY_ID].status != EligibilityStatusV2.ELIGIBLE
        ):
            return S2Decision("NO_CANDIDATE", "UNIVERSE_INELIGIBLE")
        # The comparison window has exactly 2880 measurements, ending at t-2.
        # The candidate measurement is t-1; t enters none of these indicators.
        pre = bars[:-1]
        atr_values = atr(pre)
        latest_atr = atr_values[-1]
        start = len(pre) - SAMPLE_SIZE - 1
        sample_atr = atr_values[start:-1]
        if latest_atr is None or any(x is None or not math.isfinite(x) for x in sample_atr):
            return S2Decision("NOT_ESTIMABLE", "ATR_HISTORY_UNAVAILABLE")
        closes = [float(bar.close) for bar in pre]
        widths: list[float] = []
        for i in range(start, len(pre)):
            window = closes[i - 19:i + 1]
            widths.append(4 * pstdev(window) / (math.fsum(window) / 20))
        if len(widths) != SAMPLE_SIZE + 1 or not all(math.isfinite(x) for x in widths):
            return S2Decision("NOT_ESTIMABLE", "BOLLINGER_HISTORY_UNAVAILABLE")
        width_threshold = empirical_quantile(tuple(widths[:-1]), 0.20)
        atr_threshold = float(median([float(x) for x in sample_atr if x is not None]))
        if not compression_passes(widths[-1], width_threshold, latest_atr, atr_threshold):
            return S2Decision("NO_CANDIDATE", "COMPRESSION_RULE_FAILED")
        atr_decimal = Decimal(str(latest_atr))
        if atr_decimal <= 0:
            return S2Decision("NOT_ESTIMABLE", "ATR_INVALID")
        range_bars = pre[-20:]
        range_high = max(bar.high for bar in range_bars)
        range_low = min(bar.low for bar in range_bars)
        width = range_high - range_low
        if not range_width_valid(width, atr_decimal):
            return S2Decision("NO_CANDIDATE", "RANGE_WIDTH_OUTSIDE_0.5_TO_3_ATR")
        trigger = bars[-1]
        long_threshold = range_high + Decimal("0.1") * atr_decimal
        short_threshold = range_low - Decimal("0.1") * atr_decimal
        side = breakout_side(trigger.close, range_high, range_low, atr_decimal)
        if side is None:
            return S2Decision("NO_CANDIDATE", "TRIGGER_RULE_FAILED")
        volume_median = median([bar.volume for bar in range_bars])
        if trigger.volume <= volume_median:
            return S2Decision("NO_CANDIDATE", "VOLUME_RULE_FAILED")
        setup = {"version": "S2_SETUP_V1", "policy_hash": self.policy.policy_hash,
            "key": join.key.to_dict(), "feature_hash": feature.content_hash,
            "universe_ref": universe.content_hash, "cutoff_ns": cutoff,
            "quantile_convention": QUANTILE_VERSION, "sample_count": SAMPLE_SIZE,
            "comparison_first_ref": pre[start].content_hash, "comparison_last_ref": pre[-2].content_hash,
            "comparison_refs": [bar.content_hash for bar in pre[start:-1]],
            "latest_measurement_ref": pre[-1].content_hash, "latest_bollinger_width20": widths[-1],
            "width_percentile_20": width_threshold, "latest_atr14": str(atr_decimal),
            "atr_median": atr_threshold, "atr_ref": pre[-1].content_hash,
            "range_refs": [bar.content_hash for bar in range_bars],
            "range_high": str(range_high), "range_low": str(range_low), "range_width": str(width),
            "volume_median": str(volume_median), "h1_ref": join.h1[-1].content_hash,
            "h4_ref": join.h4[-1].content_hash}
        setup_ref = sha256_json(setup)
        trigger_body = {"version": "S2_TRIGGER_V1", "setup_ref": setup_ref,
            "trigger_ref": trigger.content_hash, "trigger_close": str(trigger.close),
            "trigger_volume": str(trigger.volume), "threshold": str(long_threshold if side == V2Side.LONG else short_threshold),
            "direction": side.value, "bid": str(bbo.bid), "ask": str(bbo.ask),
            "spread": str(bbo.ask - bbo.bid),
            "spread_bps": str((bbo.ask - bbo.bid) / ((bbo.ask + bbo.bid) / 2) * 10000),
            "bbo_observed_at_ns": bbo.observed_at_ns, "bbo_available_at_ns": bbo.available_at_ns,
            "bbo_ref": bbo.evidence_ref}
        trigger_ref = sha256_json(trigger_body)
        _index(self.repository, self.policy.policy_hash, "PolicySpecV2", 0, {"policy": self.policy.to_dict()})
        self.repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2",
            feature.content_hash, feature.envelope.created_at_ns, feature.envelope.available_at_ns,
            {"feature": feature.to_dict()}))
        _index(self.repository, setup_ref, "S2SetupEvidenceV1", cutoff, setup)
        _index(self.repository, trigger_ref, "S2TriggerEvidenceV1", cutoff, trigger_body)
        reference = bbo.ask if side == V2Side.LONG else bbo.bid
        collar = reference * (Decimal("1.0005") if side == V2Side.LONG else Decimal("0.9995"))
        stop = range_low if side == V2Side.LONG else range_high
        candidate_id = sha256_json({"policy_hash": self.policy.policy_hash, "setup_ref": setup_ref,
            "trigger_ref": trigger_ref, "key": join.key.to_dict()})
        refs = tuple(sorted({setup_ref, trigger_ref, feature.content_hash, self.policy.policy_hash,
            universe.content_hash, trigger.content_hash, bbo.evidence_ref, join.h1[-1].content_hash,
            join.h4[-1].content_hash, *(bar.content_hash for bar in range_bars)}))
        envelope = ArtifactEnvelope(1, candidate_id, cutoff, cutoff, "S2_SHADOW_V1", refs)
        candidate = CandidateActionV2(envelope, candidate_id, join.key, self.policy.policy_hash,
            feature.content_hash, side, cutoff, cutoff + FRESHNESS_NS, cutoff + 2 * HOUR_NS,
            reference, collar, stop, 0, self.cost_model_ref, quantity=None)
        self.repository.register_artifact(ArtifactIndexEntryV2(candidate.content_hash, "CandidateActionV2",
            candidate.content_hash, cutoff, cutoff, {"candidate": candidate.to_dict(),
                "feature_hash": feature.content_hash, "setup_evidence_ref": setup_ref,
                "trigger_evidence_ref": trigger_ref, "universe_ref": universe.content_hash,
                "entry_policy": "IOC_NO_SAME_EPOCH_REPRICE"}))
        return S2Decision("CANDIDATE", "SHADOW_TRIGGER", candidate, setup_ref, trigger_ref)


def failed_break_exit(candidate: CandidateActionV2, setup: dict[str, object],
                      subsequent_closed_bars: tuple[CausalBarV2, ...]) -> str | None:
    """Return first/second bar ref on a failed break; never calls an order API."""
    if (candidate.policy_hash != S2_POLICY.policy_hash or setup.get("policy_hash") != candidate.policy_hash
            or setup.get("key") != candidate.key.to_dict()
            or sha256_json(setup) not in candidate.envelope.input_refs):
        raise ValueError("frozen setup does not belong to candidate")
    low, high = Decimal(str(setup["range_low"])), Decimal(str(setup["range_high"]))
    relevant = sorted((bar for bar in subsequent_closed_bars
                       if bar.close_at_ns in (candidate.decision_at_ns + M15_NS,
                                              candidate.decision_at_ns + 2 * M15_NS)),
                      key=lambda bar: bar.close_at_ns)
    for bar in relevant:
        if not bar.final or bar.instrument_revision != candidate.key.contract_revision:
            raise ValueError("failed-break evaluation requires exact final revision")
        if low <= bar.close <= high:
            return bar.content_hash
    return None
