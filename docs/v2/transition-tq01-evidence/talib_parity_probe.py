"""Bounded parity probe: ATLAS causal formulas vs isolated TA-Lib wheel."""
from __future__ import annotations

import json
from decimal import Decimal

import numpy as np
import pandas as pd
import talib
import pandas_ta_classic as pta

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.features.technical import adx, atr, ema, rsi, technical_series
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2


def bars_from_close(close: np.ndarray, *, skip: set[int] = frozenset()) -> list[CausalBarV2]:
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, ProductTypeV2.LINEAR_PERPETUAL,
                          "BTCUSDT", "bitcoin", "USDT", "USDT", "e" * 64)
    result = []
    interval = BarIntervalV2.M15
    for index, value in enumerate(close):
        if index in skip:
            continue
        prior = close[index - 1] if index else value
        opening = float(prior)
        high, low = max(opening, float(value)) * 1.001, min(opening, float(value)) * 0.999
        close_ns = (index + 1) * interval.duration_ns
        raw = RawObservationV2.build(
            instrument_revision=key.contract_revision, source_id="offline-parity-fixture",
            event_type=f"BAR_{interval.value}", received_at_ns=close_ns, ingested_at_ns=close_ns,
            available_at_ns=close_ns, event_at_ns=close_ns, translation_version="tq01-parity-v1",
            payload={"index": index, "close": float(value)},
        )
        result.append(CausalBarV2(raw, interval, index * interval.duration_ns, close_ns,
            Decimal(str(opening)), Decimal(str(high)), Decimal(str(low)), Decimal(str(float(value))),
            Decimal("10"), True))
    return result


def compare(name: str, actual, expected, *, start: int = 0) -> dict[str, object]:
    a, b = np.asarray(actual, dtype=float), np.asarray(expected, dtype=float)
    valid = np.isfinite(a) & np.isfinite(b)
    valid[:start] = False
    delta = np.abs(a[valid] - b[valid])
    return {"name": name, "n": int(delta.size), "max_abs_delta": float(delta.max()) if delta.size else None,
            "mean_abs_delta": float(delta.mean()) if delta.size else None}


def main() -> None:
    rng = np.random.default_rng(441)
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.004, 320)))
    bars = bars_from_close(closes)
    values = technical_series(bars)
    c = closes.astype(np.float64)
    close_series = pd.Series(c)
    high_series = pd.Series([float(x.high) for x in bars])
    low_series = pd.Series([float(x.low) for x in bars])
    pta_bbands = pta.bbands(close_series, length=20, std=2.0, ddof=0, talib=False)
    pta_adx = pta.adx(high_series, low_series, close_series, length=14, lensig=14,
                      mamode="rma", drift=1, talib=False)
    results = [
        compare("EMA20_SMA_seed", ema(c, 20), talib.EMA(c, timeperiod=20)),
        compare("RSI14_Wilder", rsi(c, 14), talib.RSI(c, timeperiod=14)),
        compare("pandas_ta_classic_EMA20", ema(c, 20), pta.ema(close_series, length=20, talib=False)),
        compare("pandas_ta_classic_RSI14", rsi(c, 14), pta.rsi(close_series, length=14, talib=False)),
        compare("pandas_ta_classic_ATR14", atr(bars, 14), pta.atr(
            high_series, low_series, close_series, length=14, mamode="rma", talib=False), start=60),
        compare("pandas_ta_classic_ADX14", adx(bars, 14), pta_adx["ADX_14"], start=60),
        compare("ATR14_first_bar_seed_alignment", atr(bars, 14), talib.ATR(
            np.asarray([float(x.high) for x in bars]), np.asarray([float(x.low) for x in bars]), c,
            timeperiod=14), start=60),
        compare("ADX14_Wilder_seed_alignment", adx(bars, 14), talib.ADX(
            np.asarray([float(x.high) for x in bars]), np.asarray([float(x.low) for x in bars]), c,
            timeperiod=14), start=60),
        compare("ATR14_after_long_warmup", atr(bars, 14), talib.ATR(
            np.asarray([float(x.high) for x in bars]), np.asarray([float(x.low) for x in bars]), c,
            timeperiod=14), start=200),
        compare("ADX14_after_long_warmup", adx(bars, 14), talib.ADX(
            np.asarray([float(x.high) for x in bars]), np.asarray([float(x.low) for x in bars]), c,
            timeperiod=14), start=200),
        compare("BBWidth20_population_vs_TALib_BBANDS", [x["bollinger_width20"] for x in values],
            (talib.BBANDS(c, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0]
             - talib.BBANDS(c, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[2])
            / talib.BBANDS(c, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1]),
        compare("pandas_ta_classic_BBWidth20", [x["bollinger_width20"] for x in values],
            pta_bbands["BBB_20_2.0"] / 100.0),
    ]
    sparse = bars_from_close(closes, skip={55, 56, 170})
    sparse_results = compare("sparse_EMA20_same_observed_values", ema([float(x.close) for x in sparse], 20),
                             talib.EMA(np.asarray([float(x.close) for x in sparse]), timeperiod=20))
    reordered_rejected = False
    try:
        technical_series(list(reversed(bars)))
    except ValueError:
        reordered_rejected = True
    print(json.dumps({"talib_version": talib.__version__, "pandas_ta_classic_version": pta.__version__,
        "fixture": "deterministic synthetic noisy OHLC; no venue data",
        "comparisons": results, "sparse_comparison": sparse_results, "reordered_input_rejected": reordered_rejected,
        "license": "TA-Lib Python wrapper BSD-2-Clause (wheel embeds C library; see upstream license files)"}, sort_keys=True))


if __name__ == "__main__":
    main()
