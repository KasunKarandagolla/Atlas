"""Pure causal technical series, version TECHNICAL_V1.

Index n is computed from bars[:n+1] only. Input bars must be final, ordered,
and already selected as of the caller's information cutoff.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from statistics import pstdev

from atlas.v2.data.bars import CausalBarV2
from atlas.v2.math.core import ewma_variance, realized_variance, robust_slope


def _bars(bars: Sequence[CausalBarV2]) -> None:
    if any(not bar.final for bar in bars):
        raise ValueError("technical features require final bars")
    if any(bars[i].close_at_ns >= bars[i + 1].close_at_ns for i in range(len(bars) - 1)):
        raise ValueError("bars must be strictly ordered")
    if bars and any(bar.interval != bars[0].interval or bar.instrument_revision != bars[0].instrument_revision for bar in bars):
        raise ValueError("mixed bar intervals or instrument revisions")


def ema(values: Sequence[float], period: int) -> tuple[float | None, ...]:
    if period <= 0:
        raise ValueError("EMA period must be positive")
    if any(not math.isfinite(x) for x in values):
        raise ValueError("EMA values must be finite")
    result: list[float | None] = []
    state: float | None = None
    for index, value in enumerate(values):
        if index + 1 < period:
            result.append(None)
        elif index + 1 == period:
            state = math.fsum(values[:period]) / period
            result.append(state)
        else:
            assert state is not None
            state += (2 / (period + 1)) * (value - state)
            result.append(state)
    return tuple(result)


def atr(bars: Sequence[CausalBarV2], period: int = 14) -> tuple[float | None, ...]:
    _bars(bars)
    if period <= 0:
        raise ValueError("ATR period must be positive")
    ranges: list[float] = []
    result: list[float | None] = []
    state: float | None = None
    for i, bar in enumerate(bars):
        previous = float(bars[i - 1].close) if i else float(bar.close)
        ranges.append(max(float(bar.high - bar.low), abs(float(bar.high) - previous), abs(float(bar.low) - previous)))
        if i + 1 < period:
            result.append(None)
        elif i + 1 == period:
            state = math.fsum(ranges) / period
            result.append(state)
        else:
            assert state is not None
            state = ((period - 1) * state + ranges[-1]) / period
            result.append(state)
    return tuple(result)


def rsi(closes: Sequence[float], period: int = 14) -> tuple[float | None, ...]:
    if period <= 0 or any(not math.isfinite(x) for x in closes):
        raise ValueError("invalid RSI input")
    result: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return tuple(result)
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gain = math.fsum(max(0, x) for x in changes[:period]) / period
    loss = math.fsum(max(0, -x) for x in changes[:period]) / period
    for i in range(period, len(closes)):
        if i > period:
            gain = ((period - 1) * gain + max(0, changes[i - 1])) / period
            loss = ((period - 1) * loss + max(0, -changes[i - 1])) / period
        result[i] = 50.0 if loss == 0 and gain == 0 else 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
    return tuple(result)


def adx(bars: Sequence[CausalBarV2], period: int = 14) -> tuple[float | None, ...]:
    _bars(bars)
    if period <= 0:
        raise ValueError("ADX period must be positive")
    result: list[float | None] = [None] * len(bars)
    tr: list[float] = []
    plus: list[float] = []
    minus: list[float] = []
    for i in range(1, len(bars)):
        current, previous = bars[i], bars[i - 1]
        up = float(current.high - previous.high)
        down = float(previous.low - current.low)
        plus.append(up if up > down and up > 0 else 0.0)
        minus.append(down if down > up and down > 0 else 0.0)
        tr.append(max(float(current.high - current.low), abs(float(current.high - previous.close)), abs(float(current.low - previous.close))))
    dx: list[float] = []
    smoothed: tuple[float, float, float] | None = None
    for j in range(len(tr)):
        if j + 1 < period:
            continue
        if j + 1 == period:
            smoothed = (math.fsum(tr[:period]), math.fsum(plus[:period]), math.fsum(minus[:period]))
        else:
            assert smoothed is not None
            smoothed = tuple((period - 1) * old / period + new for old, new in zip(smoothed, (tr[j], plus[j], minus[j]), strict=True))  # type: ignore[assignment]
        assert smoothed is not None
        plus_di = 100 * smoothed[1] / smoothed[0] if smoothed[0] else 0.0
        minus_di = 100 * smoothed[2] / smoothed[0] if smoothed[0] else 0.0
        denominator = plus_di + minus_di
        dx.append(100 * abs(plus_di - minus_di) / denominator if denominator else 0.0)
        if len(dx) == period:
            result[j + 1] = math.fsum(dx) / period
        elif len(dx) > period:
            prior = result[j]
            assert prior is not None
            result[j + 1] = ((period - 1) * prior + dx[-1]) / period
    return tuple(result)


def technical_series(bars: Sequence[CausalBarV2]) -> tuple[dict[str, float | None], ...]:
    _bars(bars)
    closes = [float(bar.close) for bar in bars]
    ema20, ema50, ema12, ema26 = (ema(closes, n) for n in (20, 50, 12, 26))
    macd_line = [a - b if a is not None and b is not None else None for a, b in zip(ema12, ema26, strict=True)]
    valid_macd = [x for x in macd_line if x is not None]
    macd_signal = ema(valid_macd, 9)
    atr14, rsi14, adx14 = atr(bars), rsi(closes), adx(bars)
    result: list[dict[str, float | None]] = []
    for i, bar in enumerate(bars):
        returns = tuple(math.log(closes[j] / closes[j - 1]) for j in range(max(1, i - 19), i + 1))
        width = 4 * pstdev(closes[i - 19:i + 1]) / (math.fsum(closes[i - 19:i + 1]) / 20) if i >= 19 else None
        previous_high = max(float(item.high) for item in bars[i - 20:i]) if i >= 20 else None
        previous_low = min(float(item.low) for item in bars[i - 20:i]) if i >= 20 else None
        mpos = i - 25
        signal = macd_signal[mpos] if mpos >= 0 and mpos < len(macd_signal) else None
        result.append({
            "ema20": ema20[i], "ema50": ema50[i], "robust_slope20": robust_slope(tuple(closes[:i + 1])),
            "adx14": adx14[i], "atr14": atr14[i], "rsi14": rsi14[i],
            "macd": macd_line[i], "macd_signal": signal,
            "roc10": closes[i] / closes[i - 10] - 1 if i >= 10 else None,
            "realized_variance20": realized_variance(returns, minimum=20),
            "ewma_variance": ewma_variance(returns), "bollinger_width20": width,
            "range": float(bar.high - bar.low),
            "range20_mean": math.fsum(float(x.high - x.low) for x in bars[i - 19:i + 1]) / 20 if i >= 19 else None,
            "donchian_high20": previous_high, "donchian_low20": previous_low,
            "donchian_breakout": (1.0 if closes[i] > previous_high else -1.0 if closes[i] < previous_low else 0.0) if previous_high is not None and previous_low is not None else None,
        })
    return tuple(result)
