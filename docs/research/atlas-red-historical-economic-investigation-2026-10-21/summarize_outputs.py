"""Reporting only: no signal, sizing, exit or cost changes after holdout.

Run from repository root with --permanent-root. All additional slices are
explicitly descriptive. The full evaluation ledgers remain permanent outside git.
"""

import argparse
import bisect
import csv
import gzip
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.research.full_cycle.applicability import _joined  # noqa: E402
from atlas.v2.research.full_cycle.historical_archive import bars_for, h4_from_h1, key_for  # noqa: E402
from atlas.v2.research.full_cycle.historical_economics import (  # noqa: E402
    END,
    csv_write,
    gzip_writer,
    summarize,
)
from atlas.v2.strategies.s1_trend import _regime  # noqa: E402

p = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument("--permanent-root", type=Path, required=True)
parser.add_argument("--output", type=Path, default=p)
a = parser.parse_args()
p = a.output
with gzip.open(p / "ACTION_PATHS_all.jsonl.gz", "rt") as f:
    paths = [json.loads(x) for x in f]
rows = list(csv.DictReader((p / "ECONOMIC_RESULTS.csv").open()))
# Frozen NO_OPEN comparator has no hypothetical fills or trading cash.
rows = [r for r in rows if r["strategy"] != "NO_OPEN"]
for split in ("JULY_DEVELOPMENT", "AUGUST_HOLDOUT", "ALL"):
    ref = dict.fromkeys(rows[0], "")
    ref.update(
        strategy="NO_OPEN",
        split=split,
        scenario="NO_COST_NO_FILL",
        management="NO_OPEN",
        capital_model="CASH",
        modeled_actions=0,
        modeled_filled_actions=0,
        no_fill_actions=0,
        total_conditional_cash=0,
        observed_closed_subset_cash=0,
        closed_exit_drawdown_cash=0,
        mean_cash_per_price_evaluated_slot=0,
        notional_hours=0,
        censored_actions=0,
        price_evaluated_slots=59520 if split == "ALL" else 29760,
        evidence_grade="ANALYTICAL_NO_OPEN_REFERENCE",
        missing_reason="No opening; full-programme expenses not estimated",
    )
    rows.append(ref)
csv_write(p / "ECONOMIC_RESULTS.csv", rows)

mechanisms = []
exposures = []
calendar = []
for strategy in ("S1", "S2", "S2_RAW_RANGE"):
    for split in ("JULY_DEVELOPMENT", "AUGUST_HOLDOUT", "ALL"):
        selected = [
            r
            for r in paths
            if r["strategy"] == strategy
            and (r["split"] == split or split == "ALL")
            and r["scenario"] == "BASE"
            and r["management"] == "HOLD"
        ]
        groups = defaultdict(list)
        for r in selected:
            groups[r["exit_reason"]].append(r)
        for reason, rs in sorted(groups.items()):
            mechanisms.append(
                {
                    "strategy": strategy,
                    "split": split,
                    "exit_reason": reason,
                    "count": len(rs),
                    "gross_cash": sum(float(r["gross_cash"]) for r in rs),
                    "fees_cash": sum(float(r["entry_fee"]) + float(r["exit_fee"]) for r in rs),
                    "net_before_funding_cash": sum(float(r["conditional_before_funding_cash"]) for r in rs),
                    "avg_hold_minutes": sum((r["exit_at_ns"] - r["modeled_decision_at_ns"]) / 60e9 for r in rs)
                    / len(rs),
                }
            )
        events = []
        for r in selected:
            if r["entry_state"] == "NO_FILL":
                continue
            notional = float(r["notional_usd"])
            events.extend([(r["modeled_decision_at_ns"], 1, notional), (r["exit_at_ns"], -1, -notional)])
        n = peak_n = 0
        gross = peak_gross = 0.0
        for _t, d, v in sorted(events, key=lambda x: (x[0], -x[1])):
            n += d
            gross += v
            peak_n = max(peak_n, n)
            peak_gross = max(peak_gross, gross)
        filled = [r for r in selected if r["entry_state"] != "NO_FILL"]
        cash = sum(float(r["conditional_before_funding_cash"]) for r in selected)
        exposures.append(
            {
                "strategy": strategy,
                "split": split,
                "peak_simultaneous_modeled_positions": peak_n,
                "peak_gross_notional_usdt": peak_gross,
                "filled_trade_win_rate": sum(float(r["conditional_before_funding_cash"]) > 0 for r in filled)
                / len(filled),
                "arithmetic_mean_per_price_trigger_including_stop_rejects": cash
                / ({"S1": 1655, "S2": 320, "S2_RAW_RANGE": 633}[strategy])
                if split == "ALL"
                else None,
                "classification": "INDEPENDENT_OPPORTUNITY_BOOK_NOT_AN_EXECUTABLE_ACCOUNT",
            }
        )
    groups = defaultdict(list)
    for r in paths:
        if r["strategy"] == strategy and r["scenario"] == "BASE" and r["management"] == "HOLD":
            day = datetime.fromtimestamp(r["modeled_decision_at_ns"] / 1e9, UTC).strftime("%Y-%m-%d")
            groups[day].append(r)
    for day, rs in sorted(groups.items()):
        calendar.append({"strategy": strategy, "decision_utc_day": day, **summarize(rs, None)})
csv_write(p / "MECHANISMS.csv", mechanisms)
csv_write(p / "EXPOSURE.csv", exposures)
csv_write(p / "CALENDAR_RESULTS.csv", calendar)

sources = json.loads((p / "archive_sources.json").read_text())
states = {}
for symbol in ("BTCUSDT", "ETHUSDT"):
    h4 = h4_from_h1([b for b in bars_for(a.permanent_root / "raw", sources, symbol, "1h") if b.close_at_ns < END])
    times = [b.close_at_ns for b in h4]
    labels = []
    for i, b in enumerate(h4):
        side = _regime(_joined(key_for(symbol), b.close_at_ns, h4[: i + 1], (), ()))
        labels.append(side.value if side else "NEUTRAL_OR_WARMUP")
    states[symbol] = (times, labels)
groups = defaultdict(list)
for r in paths:
    if r["scenario"] != "BASE" or r["management"] != "HOLD":
        continue
    context = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        times, labels = states[symbol]
        i = bisect.bisect_right(times, r["modeled_decision_at_ns"]) - 1
        context.append(labels[i] if i >= 0 else "UNAVAILABLE")
    groups[(r["strategy"], r["split"], *context)].append(r)
regimes = []
for key, rs in sorted(groups.items()):
    regimes.append(
        dict(zip(("strategy", "split", "btc_h4_native_regime", "eth_h4_native_regime"), key, strict=True))
        | summarize(rs, None)
    )
csv_write(p / "REGIME_RESULTS.csv", regimes)

# Compact complete due-slot refusal ledger; no raw market rows in repository.
for suffix in ("july", "all"):
    full = p / f"EVALUATIONS_{suffix}.jsonl.gz"
    if not full.exists():
        full = a.permanent_root / "outputs" / full.name
    if suffix == "all":
        with gzip.open(full, "rt") as f, gzip_writer(p / "EVALUATION_DECISIONS.csv.gz") as output:
            fields = ["symbol", "strategy", "native_due_at_ns", "status", "reason_code"]
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            for line in f:
                record = json.loads(line)
                writer.writerow({k: record[k] for k in fields})
print(json.dumps({"conditional_paths": len(paths), "regime_slices": len(regimes), "reporting_only": True}))
