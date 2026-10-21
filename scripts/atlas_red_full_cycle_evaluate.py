"""Full-cycle research evaluation over the collected genuine public evidence.

Produces, from the preserved raw receipts only:
  * the multi-coin discovery population with market context,
  * the coin x market context x strategy applicability matrix (S1-S8 and Path B),
  * sealed research actions with exact entry/stop/quantity/horizon,
  * modelled lifecycle paths for every supported management alternative,
  * mature-outcome rows with explicit censoring.

No future outcome is used to decide an original-time applicability. No order is
created. This is a research projection, not an execution record.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import sys
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.data.bars import BarIntervalV2  # noqa: E402
from atlas.v2.features.technical import atr  # noqa: E402
from atlas.v2.instruments import (  # noqa: E402
    EnvironmentV2,
    InstrumentKeyV2,
    ProductTypeV2,
    VenueV2,
)
from atlas.v2.research.full_cycle.analysis import (  # noqa: E402
    causal_bars_from_rows,
    compute_market_context,
)
from atlas.v2.research.full_cycle.applicability import (  # noqa: E402
    evaluate_path_b,
    evaluate_s1_watch_lifecycle,
    evaluate_s2,
    evaluate_s3,
    evaluate_s4,
    evaluate_s5,
    evaluate_s6,
    evaluate_s7,
    evaluate_s8,
)
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402

EVALUATION_VERSION = "ATLAS_RED_FULL_CYCLE_EVALUATION_V2_RECONSTRUCTED"
M15_NS = BarIntervalV2.M15.duration_ns
HOUR_NS = BarIntervalV2.H1.duration_ns


def _cache_key(symbol: str, interval: str, rows: Sequence[Mapping[str, Any]]) -> str:
    import hashlib

    payload = json.dumps([symbol, interval, list(rows)], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    import pickle

    try:
        with path.open("rb") as handle:
            return pickle.load(handle)
    except Exception:
        return {}


def _key_for(row: Mapping[str, Any]) -> InstrumentKeyV2:
    return InstrumentKeyV2(
        VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        row["symbol"], row["base_asset"], "USDT", "USDT", row["metadata_ref"])


def _slice_until(bars: Sequence[Any], times: Sequence[int], cutoff_ns: int) -> list[Any]:
    index = bisect.bisect_right(times, cutoff_ns)
    return list(bars[:index])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--max-closes-per-symbol", type=int, default=48)
    parser.add_argument("--output-summary", default=None)
    args = parser.parse_args(argv)

    workspace = Path(args.workspace).resolve()
    store = ResearchStoreV1.open(workspace / "data" / "research.sqlite")
    started = time.time()

    products = store.query(
        "SELECT symbol, base_asset, metadata_ref, tick_size, qty_step, min_qty, min_notional, "
        "trading_status FROM product_metadata WHERE observed_at_ns = "
        "(SELECT MAX(observed_at_ns) FROM product_metadata)"
    )
    product_by_symbol = {row["symbol"]: row for row in products}
    bars = store.query(
        "SELECT bar_id, symbol, interval, open_at_ns, close_at_ns, open, high, low, close, volume, "
        "quote_volume, trades, available_at_ns, received_at_ns, raw_row_json, source_observation_id "
        "FROM bar ORDER BY symbol, interval, close_at_ns"
    )
    by_symbol: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in bars:
        by_symbol.setdefault(row["symbol"], {}).setdefault(row["interval"], []).append(row)

    latest_population = store.query(
        "SELECT d.* FROM discovery_population d JOIN "
        "(SELECT symbol, MAX(cycle_id) AS mc FROM discovery_population GROUP BY symbol) m "
        "ON d.symbol = m.symbol AND d.cycle_id = m.mc"
    )
    eligible = {row["symbol"]: row for row in latest_population if row["eligible"]}
    observed = {row["symbol"]: row for row in latest_population}

    cache_path = workspace / "data" / "bar_cache.pkl"
    bar_cache = {}  # V1 cache omitted availability/revision identities; never trust it.
    deep_symbols = sorted(
        symbol for symbol, intervals in by_symbol.items() if "15M" in intervals and len(intervals["15M"]) > 200
    )
    server_time_ns = int(store.scalar("SELECT MAX(server_time_ns) FROM cycle") or time.time_ns())
    context_rows: list[dict[str, Any]] = []
    applicability_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    lifecycle_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    # ---- S6 cross-section over the eligible universe (registered formula) ----
    s6_scores: dict[str, tuple[float, float, int]] = {}
    eligible_with_h1 = [symbol for symbol in sorted(eligible) if "1H" in by_symbol.get(symbol, {})]
    btc_symbol = "BTCUSDT" if "BTCUSDT" in by_symbol else None
    btc_returns: dict[int, float] = {}
    if btc_symbol:
        btc_rows = by_symbol[btc_symbol]["1H"]
        btc_closes = [Decimal(row["close"]) for row in btc_rows]
        btc_times = [int(row["close_at_ns"]) for row in btc_rows]
        for index in range(1, len(btc_closes)):
            if btc_closes[index - 1] > 0:
                btc_returns[btc_times[index]] = math.log(float(btc_closes[index] / btc_closes[index - 1]))
    for symbol in eligible_with_h1:
        rows = by_symbol[symbol]["1H"]
        closes = [Decimal(row["close"]) for row in rows]
        times = [int(row["close_at_ns"]) for row in rows]
        matched: list[tuple[float, float]] = []
        for index in range(1, len(closes)):
            if closes[index - 1] <= 0:
                continue
            btc = btc_returns.get(times[index])
            if btc is None:
                continue
            matched.append((math.log(float(closes[index] / closes[index - 1])), btc))
        if len(matched) < 30:
            continue
        mean_x = sum(value[0] for value in matched) / len(matched)
        mean_y = sum(value[1] for value in matched) / len(matched)
        cov = sum((a - mean_x) * (b - mean_y) for a, b in matched)
        var_y = sum((b - mean_y) ** 2 for _, b in matched)
        if var_y <= 0:
            continue
        beta = cov / var_y
        residuals = [a - beta * b for a, b in matched]
        recent = residuals[-4:]
        if len(recent) < 4:
            continue
        variance = sum((value - sum(residuals) / len(residuals)) ** 2 for value in residuals) / (len(residuals) - 1)
        residual_vol = math.sqrt(variance)
        if residual_vol <= 0:
            continue
        score = sum(recent) / residual_vol
        s6_scores[symbol] = (score, beta, len(matched))

    # ---- per symbol evaluation (reconstructed grade throughout) ----
    for symbol in sorted(by_symbol):
        product = product_by_symbol.get(symbol)
        if product is None:
            continue
        key = _key_for(product)
        intervals = by_symbol[symbol]
        m15_rows = intervals.get("15M", [])
        h1_rows = intervals.get("1H", [])
        h4_rows = intervals.get("4H", [])
        m1_rows = intervals.get("1M", [])
        cutoff = server_time_ns
        is_eligible = symbol in eligible
        population = observed.get(symbol, {})

        def series(
            interval: BarIntervalV2, rows: Sequence[Mapping[str, Any]], symbol: str = symbol, key: Any = key
        ) -> tuple[list[Any], list[int]]:
            cache_key = _cache_key(symbol, interval.value, rows)
            cached = bar_cache.get(cache_key)
            if cached is None:
                cached = list(causal_bars_from_rows(rows, key=key, interval=interval, server_time_ns=server_time_ns))
                bar_cache[cache_key] = cached
            return list(cached), [int(bar.close_at_ns) for bar in cached]

        h1_bars, h1_times = series(BarIntervalV2.H1, h1_rows)
        h4_bars, h4_times = series(BarIntervalV2.H4, h4_rows)
        m15_bars, m15_times = series(BarIntervalV2.M15, m15_rows)
        m1_bars, m1_times = series(BarIntervalV2.M1, m1_rows)

        # market context
        context = compute_market_context(
            cycle_id=f"CTX-V2-{symbol}-{cutoff}-{time.time_ns()}",
            symbol=symbol,
            series_by_interval={
                "M15": BarSeriesShim(m15_bars),
                "H1": BarSeriesShim(h1_bars),
                "H4": BarSeriesShim(h4_bars),
            },
            btc_series=BarSeriesShim(series(BarIntervalV2.H1, by_symbol.get(btc_symbol, {}).get("1H", []))[0])
            if btc_symbol
            else None,
            eth_series=None,
            spread_bps=Decimal(population["spread_bps"]) if population.get("spread_bps") else None,
            turnover_24h_usd=Decimal(population["quote_volume_24h_usd"])
            if population.get("quote_volume_24h_usd")
            else None,
            funding_rate=Decimal(population["funding_rate"]) if population.get("funding_rate") else None,
            oi_change_15m=None,
            computed_at_ns=time.time_ns(),
        )
        if context.get("bars_used"):
            context_rows.append(context)

        # S1: registered watch lifecycle over the observed 15M sequence
        if m15_bars:
            applicability_rows.extend(
                row.as_row()
                for row in evaluate_s1_watch_lifecycle(
                    cycle_id=f"S1-{symbol}",
                    symbol=symbol,
                    key=key,
                    m15=m15_bars,
                    h1=h1_bars,
                    h4=h4_bars,
                    universe_eligible=is_eligible,
                    setup_cache={},
                    max_trigger_bars=int(args.max_closes_per_symbol) * 2,
                )
            )

        # S2: compression breakout evaluated at each confirmed close
        closes = m15_times[-args.max_closes_per_symbol :] if m15_times else []
        s2_atr: list[float | None] = []
        s2_widths: list[float] = []
        s2_closes: list[float] = []
        if len(m15_bars) > 40:
            import math as _math
            from statistics import pstdev as _pstdev

            s2_closes = [float(bar.close) for bar in m15_bars]
            s2_atr = list(atr(m15_bars, 14))
            s2_widths = [0.0] * len(s2_closes)
            for index in range(19, len(s2_closes)):
                window = s2_closes[index - 19 : index + 1]
                s2_widths[index] = 4 * _pstdev(window) / (_math.fsum(window) / 20)
        for close_ns in closes:
            setup_h1 = _slice_until(h1_bars, h1_times, close_ns)
            setup_h4 = _slice_until(h4_bars, h4_times, close_ns)
            setup_m15 = _slice_until(m15_bars, m15_times, close_ns)
            record = evaluate_s2(
                cycle_id=f"S2-V2-{symbol}-{close_ns}",
                symbol=symbol,
                key=key,
                cutoff_ns=close_ns,
                h4=setup_h4,
                h1=setup_h1,
                m15=setup_m15,
                universe_eligible=is_eligible,
                precomputed_atr=s2_atr or None,
                precomputed_widths=s2_widths or None,
                precomputed_closes=s2_closes or None,
            )
            applicability_rows.append(record.as_row())

        # S3: native 1-minute clock, exact trade population unavailable
        s3 = evaluate_s3(
            cycle_id=f"S3-V2-{symbol}-{cutoff}",
            symbol=symbol,
            key=key,
            cutoff_ns=m1_times[-1] if m1_times else cutoff,
            m1=m1_bars,
            universe_eligible=is_eligible,
            exact_trade_population=False,
        )
        applicability_rows.append(s3.as_row())

        depth_available = bool(
            store.scalar(
                "SELECT COUNT(*) FROM source_observation WHERE instrument_symbol=? AND event_type='DEPTH'", (symbol,)
            )
        )
        trade_available = bool(
            store.scalar(
                "SELECT COUNT(*) FROM source_observation WHERE instrument_symbol=? AND event_type='AGG_TRADES'",
                (symbol,),
            )
        )
        applicability_rows.append(
            evaluate_s4(
                cycle_id=f"S4-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                depth_available=depth_available,
                trade_evidence_available=trade_available,
                universe_eligible=is_eligible,
            ).as_row()
        )

        oi_available = bool(
            store.scalar(
                "SELECT COUNT(*) FROM source_observation WHERE instrument_symbol=? AND event_type='OPEN_INTEREST'",
                (symbol,),
            )
        )
        funding_available = bool(
            store.scalar(
                "SELECT COUNT(*) FROM source_observation WHERE instrument_symbol=? AND event_type='FUNDING_HISTORY'",
                (symbol,),
            )
        )
        applicability_rows.append(
            evaluate_s5(
                cycle_id=f"S5-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                oi_available=oi_available,
                funding_available=funding_available,
                liquidation_population_complete=False,
                universe_eligible=is_eligible,
            ).as_row()
        )

        score_tuple = s6_scores.get(symbol)
        applicability_rows.append(
            evaluate_s6(
                cycle_id=f"S6-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                eligible_breadth=len(eligible_with_h1),
                hours_used=score_tuple[2] if score_tuple else 0,
                score=score_tuple[0] if score_tuple else None,
                beta_btc=score_tuple[1] if score_tuple else None,
                universe_eligible=is_eligible,
            ).as_row()
        )

        applicability_rows.append(
            evaluate_s7(
                cycle_id=f"S7-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                event_calendar_available=False,
                universe_eligible=is_eligible,
            ).as_row()
        )

        applicability_rows.append(
            evaluate_s8(
                cycle_id=f"S8-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                pair_defined=False,
                hours_used=0,
                universe_eligible=is_eligible,
            ).as_row()
        )

        applicability_rows.append(
            evaluate_path_b(
                cycle_id=f"PATHB-V2-{symbol}-{cutoff}",
                symbol=symbol,
                cutoff_ns=cutoff,
                eligible=is_eligible,
                novelty_score=None,
                universe_eligible=is_eligible,
            ).as_row()
        )

    # This offline CLI produces reconstructed diagnostics only. It cannot mint
    # original-time plans from later-acquired historical price prefixes.
    for row in context_rows:
        store.record_context([row])
    for row in applicability_rows:
        row["source_grade"] = "RECONSTRUCTED_HISTORICAL_RESEARCH"
        store.record_applicability(row)
    for row in action_rows:
        store.record_action(row)
    for row in lifecycle_rows:
        store.record_lifecycle(row)
    for row in outcome_rows:
        store.record_outcome(row)
    for row in missing_rows:
        store.record_missingness(row)

    import pickle

    try:
        with cache_path.open("wb") as handle:
            pickle.dump(bar_cache, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        pass

    summary = {
        "version": EVALUATION_VERSION,
        "generated_at_ns": time.time_ns(),
        "elapsed_seconds": round(time.time() - started, 3),
        "symbols_with_bars": len(by_symbol),
        "deep_symbols": len(deep_symbols),
        "eligible_symbols": len(eligible),
        "s6_scored_symbols": len(s6_scores),
        "applicability_rows": len(applicability_rows),
        "action_rows": len(action_rows),
        "lifecycle_rows": len(lifecycle_rows),
        "outcome_rows": len(outcome_rows),
        "context_rows": len(context_rows),
        "table_counts": store.table_counts(),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.output_summary:
        Path(args.output_summary).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_summary).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return 0


class BarSeriesShim:
    """Adapter exposing the BarSeriesV1 surface used by the context owner."""

    def __init__(self, bars: Sequence[Any]) -> None:
        self._bars = list(bars)

    @property
    def count(self) -> int:
        return len(self._bars)

    @property
    def closes(self) -> tuple[Decimal, ...]:
        return tuple(bar.close for bar in self._bars)

    @property
    def closes_ns(self) -> tuple[int, ...]:
        return tuple(int(bar.close_at_ns) for bar in self._bars)

    @property
    def bars(self) -> tuple[Any, ...]:
        return tuple(self._bars)


if __name__ == "__main__":
    raise SystemExit(main())
