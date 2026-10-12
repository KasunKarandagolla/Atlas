"""Reproducible offline S46 technical-indicator parity and edge probe."""

from __future__ import annotations

import json
import math
import runpy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import numpy as np
import talib

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.features.technical import adx, atr, ema, rsi, technical_series
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.strategies.s6_cross_section import S6_POLICY


def _delta(actual, expected, *, start: int = 0) -> dict[str, object]:
    a = np.asarray([np.nan if item is None else item for item in actual], dtype=np.float64)
    b = np.asarray(expected, dtype=np.float64)
    valid = np.isfinite(a) & np.isfinite(b)
    valid[:start] = False
    difference = np.abs(a[valid] - b[valid])
    return {
        "samples": int(difference.size),
        "max_abs": float(difference.max()) if difference.size else None,
        "mean_abs": float(difference.mean()) if difference.size else None,
    }


def _flat_bar(index: int, *, key, interval: BarIntervalV2 = BarIntervalV2.M15) -> CausalBarV2:
    close_at = (index + 1) * interval.duration_ns
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id="s46-parity-fixture",
        event_type=f"BAR_{interval.value}",
        received_at_ns=close_at,
        ingested_at_ns=close_at,
        available_at_ns=close_at,
        event_at_ns=close_at,
        payload={"index": index, "close": 100, "high": 100, "low": 100, "volume": 0},
        translation_version="s46-parity-v1",
    )
    price = Decimal("100")
    return CausalBarV2(raw, interval, index * interval.duration_ns, close_at,
                       price, price, price, price, Decimal(0), True)


def _make_bar(index: int, *, key, interval: BarIntervalV2 = BarIntervalV2.M15) -> CausalBarV2:
    close_at = (index + 1) * interval.duration_ns
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id="s46-negative-fixture",
        event_type=f"BAR_{interval.value}",
        received_at_ns=close_at,
        ingested_at_ns=close_at,
        available_at_ns=close_at,
        event_at_ns=close_at,
        payload={"index": index, "close": 100},
        translation_version="s46-negative-v1",
    )
    price = Decimal("100")
    return CausalBarV2(raw, interval, index * interval.duration_ns, close_at,
                       price, price + Decimal("0.3"), price - Decimal("0.3"), price, Decimal(0), True)


def main() -> None:
    probe_path = Path("docs/v2/transition-tq01-evidence/talib_parity_probe.py")
    probe = runpy.run_path(str(probe_path), run_name="s46_parity_helper")
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, ProductTypeV2.LINEAR_PERPETUAL,
                          "BTCUSDT", "bitcoin", "USDT", "USDT", "e" * 64)
    rng = np.random.default_rng(441)
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.004, 320)))
    bars = tuple(probe["bars_from_close"](closes))
    values = technical_series(bars)
    close = np.asarray([float(bar.close) for bar in bars], dtype=np.float64)
    high = np.asarray([float(bar.high) for bar in bars], dtype=np.float64)
    low = np.asarray([float(bar.low) for bar in bars], dtype=np.float64)
    macd, signal, _histogram = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    atlas_macd = [
        fast - slow if fast is not None and slow is not None else None
        for fast, slow in zip(ema(close, 12), ema(close, 26), strict=True)
    ]
    atlas_macd_signal = [row["macd_signal"] for row in values]

    flat_close = np.full(80, 100.0, dtype=np.float64)
    flat_bars = tuple(_flat_bar(i, key=key) for i in range(80))
    flat_atr = atr(flat_bars)
    flat_adx = adx(flat_bars)
    flat_rsi = rsi(flat_close, 14)
    talib_flat_atr = talib.ATR(flat_close, flat_close, flat_close, timeperiod=14)
    talib_flat_adx = talib.ADX(flat_close, flat_close, flat_close, timeperiod=14)
    talib_flat_rsi = talib.RSI(flat_close, timeperiod=14)

    rejects: dict[str, str] = {}
    bad_inputs = {
        "incomplete": [replace(bars[0], final=False)],
        "reversed": tuple(reversed(bars[:3])),
        "duplicate": (bars[0], bars[0]),
        "mixed_interval": (_make_bar(0, key=key),
                           _make_bar(1, key=key, interval=BarIntervalV2.H1)),
        "mixed_revision": (bars[0], _make_bar(1, key=replace(key, contract_revision="f" * 64))),
    }
    for name, candidate in bad_inputs.items():
        try:
            technical_series(candidate)
        except ValueError as exc:
            rejects[name] = str(exc)
        else:
            rejects[name] = "ACCEPTED"

    sparse = tuple(probe["bars_from_close"](closes, skip={55, 56, 170}))
    output = {
        "fixture": "320 finalized deterministic synthetic H1-independent M15 OHLC bars; no venue data",
        "candidate": {"talib": talib.__version__, "numpy": np.__version__},
        "comparison_source": "existing TQ-01 parity fixture, rerun against current S46 worktree src",
        "warmup_first_valid_index": {
            "ema20_atlas": next(i for i, item in enumerate(ema(close, 20)) if item is not None),
            "ema20_talib": int(np.flatnonzero(np.isfinite(talib.EMA(close, timeperiod=20)))[0]),
            "rsi14_atlas": next(i for i, item in enumerate(rsi(close, 14)) if item is not None),
            "rsi14_talib": int(np.flatnonzero(np.isfinite(talib.RSI(close, timeperiod=14)))[0]),
            "atr14_atlas": next(i for i, item in enumerate(atr(bars)) if item is not None),
            "atr14_talib": int(np.flatnonzero(np.isfinite(talib.ATR(high, low, close, timeperiod=14)))[0]),
            "adx14_atlas": next(i for i, item in enumerate(adx(bars)) if item is not None),
            "adx14_talib": int(np.flatnonzero(np.isfinite(talib.ADX(high, low, close, timeperiod=14)))[0]),
            "macd_signal_atlas": next(i for i, row in enumerate(values) if row["macd_signal"] is not None),
            "macd_signal_talib": int(np.flatnonzero(np.isfinite(signal))[0]),
        },
        "delta": {
            "ema20": _delta(ema(close, 20), talib.EMA(close, timeperiod=20)),
            "rsi14_nonflat": _delta(rsi(close, 14), talib.RSI(close, timeperiod=14)),
            "atr14_after_200": _delta(atr(bars), talib.ATR(high, low, close, timeperiod=14), start=200),
            "adx14_after_200": _delta(adx(bars), talib.ADX(high, low, close, timeperiod=14), start=200),
            "macd_line": _delta(atlas_macd, macd),
            "macd_line_after_200": _delta(atlas_macd, macd, start=200),
            "macd_signal": _delta(atlas_macd_signal, signal),
            "macd_signal_after_200": _delta(atlas_macd_signal, signal, start=200),
            "bollinger_width20_population": _delta(
                [row["bollinger_width20"] for row in values],
                (talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0]
                 - talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[2])
                / talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1],
            ),
        },
        "constant_zero_range_zero_volume": {
            "atlas_atr14": flat_atr[-1],
            "talib_atr14": float(talib_flat_atr[-1]),
            "atlas_adx14": flat_adx[-1],
            "talib_adx14": float(talib_flat_adx[-1]),
            "atlas_rsi14": flat_rsi[14],
            "talib_rsi14": float(talib_flat_rsi[14]),
            "accepted": len(technical_series(flat_bars)) == len(flat_bars),
        },
        "sparse_observations": {
            "count": len(sparse),
            "gaps_detected": sum(
                right.close_at_ns - left.close_at_ns > BarIntervalV2.M15.duration_ns
                for left, right in zip(sparse, sparse[1:], strict=False)
            ),
            "accepted_without_gap_label": len(technical_series(sparse)) == len(sparse),
        },
        "negative_inputs_rejected": rejects,
        "policy_identity": {"s6_policy_hash": S6_POLICY.policy_hash},
        "dependency_decision": "preserve TECHNICAL_V1; no dependency or adapter added",
        "resource_measurements": None,
    }
    if any(value == "ACCEPTED" for value in rejects.values()):
        raise AssertionError("an invalid technical input was accepted")
    if any(not math.isfinite(float(value)) for value in (flat_atr[-1], flat_adx[-1], flat_rsi[14])):
        raise AssertionError("flat-input feature produced a nonfinite value")
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()
