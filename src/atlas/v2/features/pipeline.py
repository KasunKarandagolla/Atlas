"""Causal feature snapshot over already selected point-in-time bars."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2

from .candles import CausalTrade, candle_geometry, prior_utc_day_range, utc_day_vwap
from .joins import JoinedBars
from .structure import confirmed_legs, confirmed_swings, fibonacci, morphology, structure_events, support_resistance
from .technical import technical_series

FEATURE_SET_VERSION = "INTRADAY_CORE_V1"


def feature_snapshot(join: JoinedBars, *, source_health_ref: str | None = None,
                     trades: Sequence[CausalTrade] = (),
                     replay_view: ReplayViewV2 = ReplayViewV2.ACTUAL_SYSTEM) -> FeatureArtifactV2:
    if join.source_health_ref is None:
        raise ValueError("feature snapshot requires causal source-health evidence")
    if source_health_ref is not None and source_health_ref != join.source_health_ref:
        raise ValueError("feature source-health ref does not match as-of join")
    source_health_ref = join.source_health_ref
    all_bars = join.h4 + join.h1 + join.m15
    for bar in all_bars:
        availability = bar.raw.available_at_ns if replay_view == ReplayViewV2.ACTUAL_SYSTEM else bar.replay_available_at_ns
        if availability is None or availability > join.cutoff_ns or bar.close_at_ns > join.cutoff_ns:
            raise ValueError("feature input unavailable at cutoff")
        if bar.instrument_revision != join.key.contract_revision:
            raise ValueError("feature input instrument revision mismatch")
    values: dict[str, FeatureValueV2] = {}
    units = {"ema20": "price", "ema50": "price", "atr14": "price", "robust_slope20": "price/bar",
             "adx14": "index", "rsi14": "index", "macd": "price", "macd_signal": "price",
             "roc10": "fraction", "realized_variance20": "log_return_squared", "ewma_variance": "log_return_squared",
             "bollinger_width20": "fraction", "range": "price", "range20_mean": "price",
             "donchian_high20": "price", "donchian_low20": "price", "donchian_breakout": "state"}
    technical_by_frame = {}
    for label, bars in (("h4", join.h4), ("h1", join.h1), ("m15", join.m15)):
        series = technical_series(bars) if bars else ()
        technical_by_frame[label] = series
        technical = series[-1] if series else dict.fromkeys(units)
        for name, value in technical.items():
            values[f"{label}.{name}"] = FeatureValueV2(value, units[name], None if value is not None else "INSUFFICIENT_HISTORY_OR_MISSING_TIMEFRAME")
    atr = values["m15.atr14"].value
    if join.m15:
        geometry = candle_geometry(join.m15[-1], atr=float(atr) if atr is not None else None,
                                   previous=join.m15[-2] if len(join.m15) >= 2 else None,
                                   volume_history=join.m15[-21:-1])
    else:
        geometry = dict.fromkeys(("signed_body_atr", "upper_wick_atr", "lower_wick_atr", "range_atr", "close_position", "gap", "volume_z"))
    for name, value in geometry.items():
        values[f"candle.{name}"] = FeatureValueV2(value, "ratio" if name != "gap" else "price", None if value is not None else "MISSING_DENOMINATOR_OR_HISTORY")
    prior_high, prior_low, _ = prior_utc_day_range(join.m15, cutoff_ns=join.cutoff_ns)
    values["location.prior_day_high"] = FeatureValueV2(prior_high, "price", None if prior_high is not None else "PRIOR_DAY_UNAVAILABLE")
    values["location.prior_day_low"] = FeatureValueV2(prior_low, "price", None if prior_low is not None else "PRIOR_DAY_UNAVAILABLE")
    vwap, trade_refs = utc_day_vwap(trades, key=join.key, cutoff_ns=join.cutoff_ns)
    values["location.utc_day_trade_vwap"] = FeatureValueV2(vwap, "price", None if vwap is not None else "CAUSAL_TRADES_UNAVAILABLE")
    for name in ("anchored_vwap", "volume_profile"):
        values[f"location.{name}"] = FeatureValueV2(None, "price", "CAUSAL_TRADE_OR_ANCHOR_INPUT_UNAVAILABLE")
    state_ref = None
    if join.m15:
        atr_series = tuple(row["atr14"] for row in technical_by_frame["m15"])
        swings = confirmed_swings(join.m15)
        events = structure_events(join.m15, atr_series)
        zones = support_resistance(join.m15, atr_series)
        legs = confirmed_legs(swings)
        state_ref = sha256_json({"version": "STRUCTURE_V1", "swings": [asdict(x) for x in swings],
                                 "events": [asdict(x) for x in events], "zones": [asdict(x) for x in zones]})
        for kind in ("HIGH", "LOW"):
            known = [s for s in swings if s.kind == kind]
            item = known[-1] if known else None
            values[f"structure.last_confirmed_{kind.lower()}"] = FeatureValueV2(item.price if item else None, "price",
                                                None if item else "NO_CONFIRMED_SWING")
            values[f"structure.last_confirmed_{kind.lower()}_at_ns"] = FeatureValueV2(item.confirmed_at_ns if item else None, "ns",
                                                None if item else "NO_CONFIRMED_SWING")
        for kind in ("BOS_UP", "BOS_DOWN", "CHOCH_UP", "CHOCH_DOWN", "FVG_BULL", "FVG_BEAR", "SWEEP_UP", "SWEEP_DOWN", "ORDER_BLOCK_CANDIDATE_UP", "ORDER_BLOCK_CANDIDATE_DOWN"):
            values[f"structure.{kind.lower()}_at_close"] = FeatureValueV2(int(any(e.kind == kind and e.confirmed_at_ns == join.m15[-1].close_at_ns for e in events)), "boolean")
        values["structure.zone_version_count"] = FeatureValueV2(len(zones), "count")
        levels = fibonacci(legs[-1]) if legs else {}
        for ratio in ("0.382", "0.5", "0.618"):
            values[f"fibonacci.{ratio}"] = FeatureValueV2(levels.get(ratio), "price", None if ratio in levels else "NO_CONFIRMED_LEG")
        shape = morphology(legs, bars=join.m15)
        for name, value in shape.items():
            values[f"morphology.{name}"] = FeatureValueV2(value, "research_measure", None if value is not None else "INSUFFICIENT_CONFIRMED_LEGS_OR_VOLUME")
    h4_latest = technical_by_frame["h4"][-1] if technical_by_frame["h4"] else None
    if h4_latest and h4_latest["ema20"] is not None and h4_latest["ema50"] is not None and join.h4:
        close = float(join.h4[-1].close)
        e20, e50 = h4_latest["ema20"], h4_latest["ema50"]
        assert e20 is not None and e50 is not None
        trend_code = 1 if close > e50 and e20 > e50 else -1 if close < e50 and e20 < e50 else 0
        values["regime.trend_state"] = FeatureValueV2(trend_code, "-1_down_0_mixed_1_up")
    else:
        values["regime.trend_state"] = FeatureValueV2(None, "-1_down_0_mixed_1_up", "EMA_WARMUP_MISSING")
    values["regime.volatility_state"] = FeatureValueV2(1 if values["m15.realized_variance20"].value is not None else None,
        "1_observed", None if values["m15.realized_variance20"].value is not None else "VOLATILITY_HISTORY_MISSING")
    for axis in ("liquidity_state", "crowding_state", "event_state", "unknown_or_ood_state"):
        values[f"regime.{axis}"] = FeatureValueV2(None, "state", "EXTERNAL_EVIDENCE_NOT_BOUND_TO_FEATURE_SNAPSHOT")
    refs = tuple(sorted({bar.content_hash for bar in all_bars} | {source_health_ref} | set(trade_refs)))
    artifact_id = sha256_json({"feature_set": FEATURE_SET_VERSION, "key": join.key.to_dict(), "cutoff": join.cutoff_ns,
                              "inputs": refs, "view": replay_view.value})
    envelope = ArtifactEnvelope(1, artifact_id, join.cutoff_ns, join.cutoff_ns, FEATURE_SET_VERSION, refs)
    return FeatureArtifactV2(envelope, join.key, FEATURE_SET_VERSION, join.cutoff_ns, join.cutoff_ns,
                             FrozenMap(values), source_health_ref, replay_view, state_ref)
