"""Fast research-action and lifecycle generation from the stored applicability matrix.

Reads the already-evaluated applicability records and seals the research
actions and modelled lifecycle paths for every confirmed trigger. This is the
bounded second stage of the full-cycle evaluation; it changes no applicability
result and applies no future information to an original-time decision.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.data.bars import BarIntervalV2  # noqa: E402
from atlas.v2.research.full_cycle.actions import (  # noqa: E402
    evaluate_lifecycle,
    maturity_status,
    seal_research_action,
)
from atlas.v2.research.full_cycle.analysis import causal_bars_from_rows  # noqa: E402
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402

HOUR_NS = 3_600_000_000_000
ACTIONS_VERSION = "ATLAS_RED_FULL_CYCLE_RESEARCH_ACTION_V1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output-summary", default=None)
    args = parser.parse_args(argv)
    workspace = Path(args.workspace).resolve()
    store = ResearchStoreV1.open(workspace / "data" / "research.sqlite")
    server_time_ns = int(store.scalar("SELECT MAX(server_time_ns) FROM cycle") or time.time_ns())

    products = {row["symbol"]: row for row in store.query(
        "SELECT symbol, base_asset, metadata_ref, tick_size, qty_step, min_notional, trading_status "
        "FROM product_metadata WHERE observed_at_ns = "
        "(SELECT MAX(observed_at_ns) FROM product_metadata)")}
    population = {row["symbol"]: row for row in store.query(
        "SELECT symbol, spread_bps, funding_rate, mark_price, quote_volume_24h_usd "
        "FROM discovery_population WHERE cycle_id = "
        "(SELECT MAX(cycle_id) FROM discovery_population)")}
    candidates = store.query(
        "SELECT * FROM strategy_applicability WHERE status IN ('RESEARCH_ACTION','CANDIDATE') "
        "AND strategy IN ('S1','S2')")
    bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in store.query(
            "SELECT bar_id, symbol, interval, open_at_ns, close_at_ns, open, high, low, close, volume, "
            "quote_volume, trades, available_at_ns, received_at_ns, raw_row_json, source_observation_id "
            "FROM bar WHERE interval IN ('15M','1H','4H') ORDER BY symbol, interval, close_at_ns"):
        bars_by_symbol.setdefault(row["symbol"], []).append(row)

    from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

    action_rows: list[dict[str, Any]] = []
    lifecycle_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    bar_cache: dict[str, list[Any]] = {}

    for record in candidates:
        symbol = record["symbol"]
        product = products.get(symbol)
        if product is None:
            continue
        indicators = json.loads(record["indicator_values"] or "{}")
        setup = json.loads(record["setup_predicates"] or "{}")
        triggers = json.loads(record["trigger_predicates"] or "{}")

        def num(value: Any) -> Decimal | None:
            try:
                if value in (None, ""):
                    return None
                return Decimal(str(value))
            except Exception:
                return None

        close = num(triggers.get("close")) or num(triggers.get("trigger_close")) \
            or num(indicators.get("trigger_close"))
        atr_value = num(triggers.get("atr15m")) or num(indicators.get("atr15m")) \
            or num(indicators.get("latest_atr14"))
        extreme = num(setup.get("setup_extreme")) or num(indicators.get("range_low")) \
            or num(indicators.get("range_high"))
        side = setup.get("setup_side") or triggers.get("breakout_side")
        if side not in ("LONG", "SHORT") or close is None or atr_value is None or extreme is None:
            continue
        if atr_value <= 0:
            continue
        buffer = Decimal("0.25")
        stop = extreme - buffer * atr_value if side == "LONG" else extreme + buffer * atr_value
        distance_atr = abs(close - stop) / atr_value
        if not (Decimal("0.5") <= distance_atr <= Decimal("3")):
            continue
        row_population = population.get(symbol, {})
        spread_bps = num(row_population.get("spread_bps")) or Decimal("0")
        max_hold = 4 * HOUR_NS if record["strategy"] == "S1" else 2 * HOUR_NS
        decision_at_ns = int(record["information_cutoff_ns"])
        action = seal_research_action(
            cycle_id=record["cycle_id"], symbol=symbol, strategy=record["strategy"],
            policy_id=record.get("policy_id") or "", policy_hash=record.get("policy_hash") or "",
            side=side, entry_reference=close, stop_price=stop,
            stop_basis=("S1_SETUP_EXTREME_MINUS_0.25_ATR15M" if record["strategy"] == "S1"
                        else "S2_FROZEN_RANGE_BOUNDARY"),
            qty_step=Decimal(product["qty_step"]), tick_size=Decimal(product["tick_size"]),
            min_notional=Decimal(product["min_notional"]) if product.get("min_notional") else None,
            spread_bps=spread_bps, atr_value=atr_value, max_hold_ns=max_hold,
            decision_at_ns=decision_at_ns, deadline_offset_ns=5_000_000_000,
            evidence_refs=(record["applicability_id"],), sealed_at_ns=time.time_ns())
        action_rows.append(action.as_row())

        rows = bars_by_symbol.get(symbol)
        if not rows:
            continue
        if symbol not in bar_cache:
            key = InstrumentKeyV2(
                VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                symbol, product["base_asset"], "USDT", "USDT", product["metadata_ref"])
            series: dict[str, list[Any]] = {}
            for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4):
                selected = [row for row in rows if row["interval"] == interval.value]
                series[interval.value] = list(causal_bars_from_rows(
                    selected, key=key, interval=interval, server_time_ns=server_time_ns))
            bar_cache[symbol] = series
        series = bar_cache[symbol]
        m15_series = series["15M"]
        future = [bar for bar in m15_series if bar.close_at_ns > decision_at_ns]
        if not future:
            continue
        lifecycle = evaluate_lifecycle(
            action, future_bars=future, spread_bps=spread_bps,
            funding_rate=num(row_population.get("funding_rate")),
            mark_price=num(row_population.get("mark_price")), atr_value=atr_value)
        for outcome in lifecycle:
            lifecycle_rows.append({
                "lifecycle_id": f"{outcome.path_id}|{outcome.management_kind}",
                "action_id": outcome.path_id, "management_kind": outcome.management_kind,
                "path_id": outcome.path_id, "entry_state": outcome.entry_state,
                "exit_state": outcome.exit_state, "exit_reason": outcome.exit_reason,
                "gross_cash": str(outcome.gross_cash), "cost_cash": str(outcome.cost_cash),
                "net_cash": str(outcome.net_cash), "horizon_end_ns": outcome.horizon_end_ns,
                "uncertainty": outcome.uncertainty, "detail": dict(outcome.detail)})
        status, censoring, horizon = maturity_status(action=action, future_bars=future,
                                                     evaluated_at_ns=server_time_ns)
        # Causal market context computed strictly from information available at
        # the original decision time.
        from atlas.v2.research.full_cycle.analysis import compute_market_context

        def _shim(bars: list[Any]) -> Any:
            class Shim:
                def __init__(self, values: list[Any]) -> None:
                    self._values = values

                @property
                def count(self) -> int:
                    return len(self._values)

                @property
                def closes(self) -> tuple[Decimal, ...]:
                    return tuple(bar.close for bar in self._values)

                @property
                def closes_ns(self) -> tuple[int, ...]:
                    return tuple(int(bar.close_at_ns) for bar in self._values)

                @property
                def bars(self) -> tuple[Any, ...]:
                    return tuple(self._values)

            return Shim(bars)

        def _upto(bars: list[Any], cutoff_ns: int) -> list[Any]:
            return [bar for bar in bars if bar.close_at_ns <= cutoff_ns]

        context = compute_market_context(
            cycle_id=f"CTX-{symbol}-{decision_at_ns}", symbol=symbol,
            series_by_interval={"M15": _shim(_upto(series["15M"], decision_at_ns)),
                                "H1": _shim(_upto(series["1H"], decision_at_ns)),
                                "H4": _shim(_upto(series["4H"], decision_at_ns))},
            btc_series=None, eth_series=None,
            spread_bps=spread_bps,
            turnover_24h_usd=num(row_population.get("quote_volume_24h_usd")),
            funding_rate=num(row_population.get("funding_rate")),
            oi_change_15m=None, computed_at_ns=decision_at_ns)
        if context.get("bars_used"):
            store.record_context([context])
        hold = next((item for item in lifecycle if item.management_kind == "HOLD"), None)
        outcome_rows.append({
            "action_id": action.action_id, "cycle_id": action.cycle_id, "symbol": symbol,
            "strategy": action.strategy, "maturity_status": status, "horizon_ns": horizon,
            "net_cash": None if hold is None else str(hold.net_cash),
            "return_bps": None if hold is None or action.notional_usd == 0 else
            str((hold.net_cash / action.notional_usd) * Decimal(10_000)),
            "max_favorable_bps": None, "max_adverse_bps": None,
            "censoring_reason": censoring, "evaluated_at_ns": server_time_ns,
            "evaluation_version": "ATLAS_RED_FULL_CYCLE_EVALUATION_V1"})

    for row in action_rows:
        store.record_action(row)
    for row in lifecycle_rows:
        store.record_lifecycle(row)
    for row in outcome_rows:
        store.record_outcome(row)
    summary = {"actions": len(action_rows), "lifecycle": len(lifecycle_rows),
               "outcomes": len(outcome_rows),
               "matured": sum(1 for row in outcome_rows if row["maturity_status"] == "MATURED"),
               "candidates": len(candidates)}
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.output_summary:
        Path(args.output_summary).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
