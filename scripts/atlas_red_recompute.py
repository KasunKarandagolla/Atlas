"""Copy an existing research database safely; migrate, audit and recompute supported paths.

The source is opened read-only and copied using SQLite backup, including committed
WAL pages. The destination must be new. No market acquisition, label fabrication,
legacy rewrite, or strategy optimization takes place.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from dataclasses import fields
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.data.bars import BarIntervalV2  # noqa:E402
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2  # noqa:E402
from atlas.v2.research.full_cycle.actions import ResearchActionV1  # noqa:E402
from atlas.v2.research.full_cycle.analysis import causal_bars_from_rows  # noqa:E402
from atlas.v2.research.full_cycle.causality import action_qualification  # noqa:E402
from atlas.v2.research.full_cycle.native import persist_modeled_outcomes  # noqa:E402
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa:E402


def decode_action(row: dict) -> ResearchActionV1:
    values = {f.name: row[f.name] for f in fields(ResearchActionV1) if f.name in row}
    for key in (
        "entry_reference",
        "entry_collar_bps",
        "stop_price",
        "quantity",
        "notional_usd",
        "fee_bps",
        "expected_spread_bps",
        "slippage_bps",
        "take_profit_price",
    ):
        if values.get(key) is not None:
            values[key] = Decimal(values[key])
    for name in ("execution_plan", "provenance"):
        values[name] = json.loads(values.get(name) or "{}")
    plan = values["execution_plan"]
    values.update(
        qty_step=Decimal(plan.get("qty_step", ".001")),
        tick_size=Decimal(plan.get("tick_size", ".01")),
        min_notional=Decimal(plan["min_notional"]) if plan.get("min_notional") else None,
        take_profit_policy=plan.get("take_profit_policy", "NOT_IN_REGISTERED_POLICY"),
        evidence_refs=tuple(json.loads(values.get("evidence_refs") or "[]")),
    )
    return ResearchActionV1(**values)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--evaluated-at-ns", type=int, required=True)
    args = parser.parse_args(argv)
    if args.destination.exists():
        parser.error("Destination must be new; original data will never be replaced")
    if not args.source.is_file():
        parser.error("Genuine source database is missing")
    args.destination.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(args.source.resolve().as_uri() + "?mode=ro", uri=True)
    dest = sqlite3.connect(args.destination)
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()
    store = ResearchStoreV1.open(args.destination)
    results = {
        "schema_integrity": store.scalar("PRAGMA integrity_check"),
        "legacy_actions_excluded": 0,
        "original_actions": 0,
        "modeled_paths": 0,
        "programme_economics": "NOT ESTIMABLE",
        "evaluation_version": "ATLAS_RED_FULL_CYCLE_RECOMPUTE_V2",
    }
    for row in store.query("select * from research_action"):
        if not action_qualification(row):
            results["legacy_actions_excluded"] += 1
            continue
        results["original_actions"] += 1
        action = decode_action(row)
        key = InstrumentKeyV2(
            VenueV2.BINANCE,
            EnvironmentV2.MAINNET,
            ProductTypeV2.LINEAR_PERPETUAL,
            action.symbol,
            action.symbol.removesuffix("USDT"),
            "USDT",
            "USDT",
            action.instrument_revision,
        )
        bars = causal_bars_from_rows(
            store.query("select * from bar where symbol=? and interval=?", (action.symbol, "15M")),
            key=key,
            interval=BarIntervalV2.M15,
            server_time_ns=args.evaluated_at_ns,
        )
        future = [b for b in bars if b.open_at_ns >= action.sealed_at_ns]
        paths = persist_modeled_outcomes(
            store,
            action,
            future_bars=future,
            spread_bps=action.expected_spread_bps,
            evaluated_at_ns=args.evaluated_at_ns,
        )
        results["modeled_paths"] += len(paths)
    store.close()
    results["destination_sha256"] = hashlib.sha256(args.destination.read_bytes()).hexdigest()
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
