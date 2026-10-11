"""Sparse observed-value parity only; skipped bars are omitted from each library input."""
from __future__ import annotations
import json
import numpy as np
import pandas as pd
import talib
import pandas_ta_classic as pta
from atlas.v2.features.technical import adx, atr, ema, rsi, technical_series
from talib_parity_probe import bars_from_close, compare

rng = np.random.default_rng(20261012)
closes = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.003, 360)))
bars = bars_from_close(closes, skip={33, 34, 35, 117, 221, 222, 300})
c = np.asarray([float(bar.close) for bar in bars], dtype=np.float64)
h = np.asarray([float(bar.high) for bar in bars], dtype=np.float64)
l = np.asarray([float(bar.low) for bar in bars], dtype=np.float64)
hs, ls, cs = pd.Series(h), pd.Series(l), pd.Series(c)
pta_bb = pta.bbands(cs, length=20, std=2.0, ddof=0, talib=False)
pta_adx = pta.adx(hs, ls, cs, length=14, lensig=14, mamode="rma", drift=1, talib=False)
features = technical_series(bars)
results = [
    compare("sparse_EMA20_TALib", ema(c, 20), talib.EMA(c, timeperiod=20)),
    compare("sparse_RSI14_TALib", rsi(c, 14), talib.RSI(c, timeperiod=14)),
    compare("sparse_ATR14_TALib_after_warmup", atr(bars, 14), talib.ATR(h, l, c, timeperiod=14), start=100),
    compare("sparse_ADX14_TALib_after_warmup", adx(bars, 14), talib.ADX(h, l, c, timeperiod=14), start=200),
    compare("sparse_EMA20_pandas_ta", ema(c, 20), pta.ema(cs, length=20, talib=False)),
    compare("sparse_RSI14_pandas_ta", rsi(c, 14), pta.rsi(cs, length=14, talib=False)),
    compare("sparse_ATR14_pandas_ta_after_warmup", atr(bars, 14), pta.atr(hs, ls, cs, length=14, mamode="rma", talib=False), start=100),
    compare("sparse_ADX14_pandas_ta_after_warmup", adx(bars, 14), pta_adx["ADX_14"], start=200),
    compare("sparse_BBWidth20_pandas_ta", [x["bollinger_width20"] for x in features], pta_bb["BBB_20_2.0"] / 100.0),
]
out = {"talib_version": talib.__version__, "pandas_ta_classic_version": pta.__version__,
       "observed_bar_count": len(bars), "omitted_positions": [33,34,35,117,221,222,300],
       "fixture": "deterministic sparse synthetic observed OHLC; no venue data; compare observed sequence only",
       "comparisons": results}
print(json.dumps(out, sort_keys=True))
