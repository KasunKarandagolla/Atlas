"""Discovery population, market context and strategy applicability for ATLAS RED.

Discovery records the *entire* observed product population of a cycle, with the
reasons for inclusion and exclusion, so the research denominator is never
narrowed to the coins the algorithm happened to choose. Strategy applicability
records every registered strategy family against every coin in the cycle at
that strategy's own native clock, including the cases that were inapplicable,
rejected, unavailable or not evaluated.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from ...data.bars import BarIntervalV2, CausalBarV2
from ...features.technical import adx, atr, ema

ANALYSIS_VERSION = "ATLAS_RED_FULL_CYCLE_ANALYSIS_V1"

MIN_TURNOVER_USD = Decimal("10_000_000")
MAX_SPREAD_BPS = Decimal("10")


class ApplicabilityStatusV1(StrEnum):
    """Typed research applicability states.

    These extend, and do not replace, the production status owners
    (`EligibilityStatusV2`, `DecisionStatusV2`, `CandidateSelectionStatus`,
    `WatchStateV2`). A research state never grants selection authority.
    """

    NOT_DUE = "NOT_DUE"
    NO_SETUP = "NO_SETUP"
    INELIGIBLE = "INELIGIBLE"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    RESOURCE_NOT_EVALUATED = "RESOURCE_NOT_EVALUATED"
    MISSED_DEADLINE = "MISSED_DEADLINE"
    CANDIDATE = "CANDIDATE"
    RESEARCH_ACTION = "RESEARCH_ACTION"
    CONTEXT_ONLY = "CONTEXT_ONLY"


STRATEGY_NATIVE_CLOCK: Mapping[str, str] = {
    "S1": "CONFIRMED_15M_CLOSE",
    "S2": "CONFIRMED_15M_CLOSE",
    "S3": "CONFIRMED_1M_CLOSE",
    "S4": "CONTEXT_10S_OBSERVATION",
    "S5": "CONTEXT_15M_CLOSE",
    "S6": "CONFIRMED_1H_CLOSE",
    "S7": "VERIFIED_EVENT_PUBLICATION",
    "S8": "CONFIRMED_1H_CLOSE",
    "PATH_B": "DISCOVERY_ATTEMPT_CYCLE",
}


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _dec(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _f(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def build_discovery_population(
    *,
    cycle_id: str,
    cutoff_ns: int,
    products: Sequence[Mapping[str, Any]],
    tickers: Mapping[str, Mapping[str, Any]],
    premium: Mapping[str, Mapping[str, Any]],
    books: Mapping[str, Mapping[str, Any]],
    deep_symbols: Sequence[str],
) -> list[dict[str, Any]]:
    """Record the entire observed product population with explicit inclusion reasons."""

    deep = set(deep_symbols)
    rows: list[dict[str, Any]] = []
    for product in products:
        symbol = str(product["symbol"])
        ticker = tickers.get(symbol)
        premium_row = premium.get(symbol, {})
        book = books.get(symbol, {})
        observed = ticker is not None
        quote_volume = _dec(ticker.get("quote_volume_24h_usd")) if ticker else None
        change_pct = _dec(ticker.get("price_change_pct_24h")) if ticker else None
        spread_bps = _dec(book.get("spread_bps"))
        reasons: list[str] = []
        if not observed:
            reasons.append("NO_TICKER_OBSERVATION")
        if str(product["trading_status"]) != "TRADING":
            reasons.append(f"TRADING_STATUS_{product['trading_status']}")
        if quote_volume is None:
            reasons.append("QUOTE_VOLUME_UNAVAILABLE")
        elif quote_volume < MIN_TURNOVER_USD:
            reasons.append("TURNOVER_BELOW_REGISTERED_MINIMUM")
        if spread_bps is None:
            reasons.append("SPREAD_UNAVAILABLE")
        elif spread_bps > MAX_SPREAD_BPS:
            reasons.append("SPREAD_ABOVE_REGISTERED_MAXIMUM")
        eligible = not reasons
        attention = None
        if quote_volume is not None and change_pct is not None:
            attention = math.log10(1.0 + float(quote_volume)) * (1.0 + abs(float(change_pct)) / 100.0)
        rows.append({
            "cycle_id": cycle_id,
            "symbol": symbol,
            "observed": 1 if observed else 0,
            "eligible": 1 if eligible else 0,
            "exclusion_reason": ";".join(reasons) if reasons else None,
            "quote_volume_24h_usd": None if quote_volume is None else str(quote_volume),
            "turnover_24h_usd": None if quote_volume is None else str(quote_volume),
            "price_change_pct_24h": None if change_pct is None else str(change_pct),
            "last_price": None if ticker is None else ticker.get("last_price"),
            "spread_bps": None if spread_bps is None else str(spread_bps),
            "mark_price": premium_row.get("mark_price"),
            "funding_rate": premium_row.get("funding_rate"),
            "next_funding_at_ns": premium_row.get("next_funding_at_ns"),
            "discovery_score": None if attention is None else f"{attention:.10f}",
            "compute_tier": "DEEP" if symbol in deep else "CHEAP",
            "deep_cohort": 1 if symbol in deep else 0,
            "selection_probability": None,
        })
    ranked = sorted(
        (row for row in rows if row["discovery_score"] is not None),
        key=lambda row: (-float(row["discovery_score"]), row["symbol"]),
    )
    for index, row in enumerate(ranked, start=1):
        row["discovery_rank"] = index
    eligible_count = len(ranked)
    if eligible_count:
        probability = len(deep) / eligible_count
        for row in rows:
            row["selection_probability"] = f"{probability:.10f}"
    # Observed market instruments that the admitted product universe does not contain
    # stay in the denominator with an explicit admission reason.
    admitted = {row["symbol"] for row in rows}
    for symbol in sorted(set(tickers) - admitted):
        ticker = tickers[symbol]
        quote_volume = _dec(ticker.get("quote_volume_24h_usd"))
        change_pct = _dec(ticker.get("price_change_pct_24h"))
        attention = None
        if quote_volume is not None and change_pct is not None:
            attention = math.log10(1.0 + float(quote_volume)) * (1.0 + abs(float(change_pct)) / 100.0)
        rows.append({
            "cycle_id": cycle_id,
            "symbol": symbol,
            "observed": 1,
            "eligible": 0,
            "exclusion_reason": "NOT_IN_ADMITTED_PRODUCT_UNIVERSE",
            "quote_volume_24h_usd": None if quote_volume is None else str(quote_volume),
            "turnover_24h_usd": None if quote_volume is None else str(quote_volume),
            "price_change_pct_24h": None if change_pct is None else str(change_pct),
            "last_price": ticker.get("last_price"),
            "spread_bps": (books.get(symbol) or {}).get("spread_bps"),
            "mark_price": (premium.get(symbol) or {}).get("mark_price"),
            "funding_rate": (premium.get(symbol) or {}).get("funding_rate"),
            "next_funding_at_ns": (premium.get(symbol) or {}).get("next_funding_at_ns"),
            "discovery_score": None if attention is None else f"{attention:.10f}",
            "compute_tier": "OUT_OF_UNIVERSE",
            "deep_cohort": 0,
            "selection_probability": None,
        })
    return rows


def select_deep_cohort(
    population: Sequence[Mapping[str, Any]], *, size: int, rotation_offset: int
) -> tuple[tuple[str, ...], str]:
    """Deterministic fair rotation over the ranked eligible population.

    The deep cohort is a *sample* of the eligible population. It is never
    relabelled as full-universe coverage.
    """

    eligible = [row for row in population if row.get("eligible")]
    if not eligible:
        return (), "EMPTY_ELIGIBLE_POPULATION"
    ranked = sorted(eligible, key=lambda row: (row.get("discovery_rank") or 10**9, row["symbol"]))
    count = len(ranked)
    take = min(size, count)
    offset = rotation_offset % count
    selected: list[str] = []
    for index in range(take):
        selected.append(str(ranked[(offset + index) % count]["symbol"]))
    return tuple(sorted(set(selected))), f"FAIR_ROTATION_RANKED_ELIGIBLE_OFFSET_{offset}_SIZE_{take}"


@dataclass(frozen=True)
class BarSeriesV1:
    symbol: str
    interval: BarIntervalV2
    bars: tuple[Any, ...]

    @property
    def count(self) -> int:
        return len(self.bars)

    @property
    def closes(self) -> tuple[Decimal, ...]:
        return tuple(bar.close for bar in self.bars)

    @property
    def closes_ns(self) -> tuple[int, ...]:
        return tuple(int(bar.close_at_ns) for bar in self.bars)


def causal_bars_from_rows(
    rows: Sequence[Mapping[str, Any]], *, key: Any, interval: BarIntervalV2, server_time_ns: int
) -> tuple[Any, ...]:
    """Rebuild canonical causal bars with the production Binance translator.

    The stored raw kline row is re-translated so the bar identity, availability
    and finality semantics are exactly the production owner's, never a local
    re-implementation.
    """

    from ...data.binance import translate_kline

    selected = [row for row in rows if row["interval"] == interval.value]
    selected.sort(key=lambda row: int(row["open_at_ns"]))
    bars: list[Any] = []
    for row in selected:
        raw_row = json.loads(row["raw_row_json"])
        raw, values, open_at_ns, final = translate_kline(
            raw_row, key=key, interval=interval,
            received_at_ns=int(row["received_at_ns"]), server_time_ns=server_time_ns)
        if not final:
            continue
        bar = CausalBarV2(
            raw=raw,
            interval=interval,
            open_at_ns=open_at_ns,
            close_at_ns=close_at_ns(raw_row, interval),
            open=Decimal(values["open"]),
            high=Decimal(values["high"]),
            low=Decimal(values["low"]),
            close=Decimal(values["close"]),
            volume=Decimal(values["volume"]),
            final=True,
        )
        bars.append(bar)
    return tuple(bars)


def close_at_ns(raw_row: Sequence[Any], interval: BarIntervalV2) -> int:
    from ...data.bars import close_boundary_ns

    return close_boundary_ns(int(raw_row[0]) * 1_000_000, interval)


def series_from_rows(rows: Sequence[Mapping[str, Any]], *, symbol: str,
                     interval: BarIntervalV2, key: Any = None,
                     server_time_ns: int = 0) -> BarSeriesV1 | None:
    selected = [row for row in rows if row["symbol"] == symbol and row["interval"] == interval.value]
    if not selected or key is None:
        return None
    bars = causal_bars_from_rows(selected, key=key, interval=interval, server_time_ns=server_time_ns)
    if not bars:
        return None
    return BarSeriesV1(symbol=symbol, interval=interval, bars=bars)


def compute_market_context(
    *,
    cycle_id: str,
    symbol: str,
    series_by_interval: Mapping[str, BarSeriesV1 | None],
    btc_series: BarSeriesV1 | None,
    eth_series: BarSeriesV1 | None,
    spread_bps: Decimal | None,
    turnover_24h_usd: Decimal | None,
    funding_rate: Decimal | None,
    oi_change_15m: Decimal | None,
    computed_at_ns: int,
) -> dict[str, Any]:
    """Market context features; every feature records its own sample support."""

    m15 = series_by_interval.get("M15")
    h1 = series_by_interval.get("H1")
    row: dict[str, Any] = {
        "cycle_id": cycle_id,
        "symbol": symbol,
        "horizon": "H4",
        "bars_used": 0,
        "computed_at_ns": computed_at_ns,
        "spread_bps": None if spread_bps is None else str(spread_bps),
        "turnover_24h_usd": None if turnover_24h_usd is None else str(turnover_24h_usd),
        "funding_rate": None if funding_rate is None else str(funding_rate),
        "oi_change_15m": None if oi_change_15m is None else str(oi_change_15m),
    }
    if h1 is not None and h1.count >= 30:
        bars = list(h1.bars)
        closes = [float(bar.close) for bar in bars]
        rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        realized_vol_bps = None
        if len(rets) >= 10:
            mean = sum(rets) / len(rets)
            variance = sum((value - mean) ** 2 for value in rets) / (len(rets) - 1)
            realized_vol_bps = math.sqrt(variance) * math.sqrt(24 * 60) * 10_000
        ema_values = ema(closes, 20)
        ema20 = ema_values[-1] if ema_values else None
        ema50_values = ema(closes, 50)
        ema50 = ema50_values[-1] if ema50_values else None
        atr_values = atr(bars, 14)
        atr14_abs = atr_values[-1] if atr_values else None
        atr14_bps = None
        if atr14_abs is not None and closes[-1] > 0:
            atr14_bps = atr14_abs / closes[-1] * 10_000
        adx_values = adx(bars, 14)
        adx14 = adx_values[-1] if adx_values else None
        trend_score = None
        classification = "UNCLASSIFIED"
        if ema20 is not None and ema50 is not None and closes[-1] > 0:
            separation = abs(ema20 - ema50) / closes[-1]
            trend_score = separation
            direction = 1.0 if ema20 > ema50 else -1.0
            if adx14 is not None and adx14 >= 25 and separation >= 0.002:
                classification = "TRENDING"
            elif adx14 is not None and adx14 < 20:
                classification = "RANGING"
            else:
                classification = "TRANSITIONAL"
            row["_trend_direction"] = direction
        compression_ratio = None
        if m15 is not None and m15.count >= 30:
            m15_closes = [float(bar.close) for bar in m15.bars]
            recent = m15_closes[-8:]
            older = m15_closes[-32:-8] if len(m15_closes) >= 32 else None
            if older:
                recent_range = max(recent) - min(recent)
                older_range = max(older) - min(older)
                if older_range > 0:
                    compression_ratio = recent_range / older_range
        row.update({
            "bars_used": h1.count,
            "realized_vol_bps": None if realized_vol_bps is None else f"{realized_vol_bps:.6f}",
            "atr14_abs": None if atr14_abs is None else f"{atr14_abs:.12f}",
            "atr14_bps": None if atr14_bps is None else f"{atr14_bps:.6f}",
            "ema20_h1": None if ema20 is None else f"{ema20:.12f}",
            "ema50_h1": None if ema50 is None else f"{ema50:.12f}",
            "trend_score": None if trend_score is None else f"{trend_score:.10f}",
            "adx14": None if adx14 is None else f"{adx14:.6f}",
            "compression_ratio": None if compression_ratio is None else f"{compression_ratio:.8f}",
            "classification": classification,
        })
        window = min(60, len(rets))
        if window >= 10:
            row["return_bps"] = f"{(math.log(closes[-1] / closes[-1 - window]) * 10_000):.6f}"
    btc = _correlation(series_by_interval.get("H1"), btc_series)
    eth = _correlation(series_by_interval.get("H1"), eth_series)
    if btc:
        row["corr_btc"], row["beta_btc"] = btc
    if eth:
        row["corr_eth"] = eth[0]
    return row


def _correlation(left: BarSeriesV1 | None, right: BarSeriesV1 | None) -> tuple[float, float] | None:
    if left is None or right is None:
        return None
    map_right = {ts: float(value) for ts, value in zip(right.closes_ns, right.closes, strict=True)}
    pairs: list[tuple[float, float]] = []
    for index in range(1, len(left.closes)):
        ts = left.closes_ns[index]
        if ts not in map_right:
            continue
        previous = float(left.closes[index - 1])
        if previous <= 0 or map_right[ts] <= 0:
            continue
        pairs.append((math.log(float(left.closes[index]) / previous), math.log(map_right[ts])))
    if len(pairs) < 10:
        return None
    mean_x = sum(value[0] for value in pairs) / len(pairs)
    mean_y = sum(value[1] for value in pairs) / len(pairs)
    cov = sum((value[0] - mean_x) * (value[1] - mean_y) for value in pairs)
    var_x = sum((value[0] - mean_x) ** 2 for value in pairs)
    var_y = sum((value[1] - mean_y) ** 2 for value in pairs)
    if var_x <= 0 or var_y <= 0:
        return None
    correlation = cov / math.sqrt(var_x * var_y)
    beta = cov / var_x
    return correlation, beta
