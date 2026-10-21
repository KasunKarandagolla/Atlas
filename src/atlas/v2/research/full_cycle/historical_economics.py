"""Historical pilot orchestration; native strategy and lifecycle owners do math.

All amounts are conditional research estimates, never prospective qualification.
"""

from __future__ import annotations

import bisect
import csv
import gzip
import io
import json
import math
import random
import shutil
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import UTC, datetime
from decimal import Decimal as D
from pathlib import Path
from statistics import mean, pstdev

from atlas.v2.features.technical import atr
from atlas.v2.strategies import s2_breakout

from .actions import evaluate_lifecycle, seal_research_action
from .applicability import evaluate_s1_watch_lifecycle, evaluate_s2
from .historical_archive import bars_for, digest, h4_from_h1, key_for
from .store import ResearchStoreV1

DAY = 86400_000_000_000
M15 = 900_000_000_000
HOUR = 3600_000_000_000
BEGIN = int(datetime(2024, 7, 1, tzinfo=UTC).timestamp() * 1e9)
SPLIT = int(datetime(2024, 8, 1, tzinfo=UTC).timestamp() * 1e9)
END = int(datetime(2024, 9, 1, tzinfo=UTC).timestamp() * 1e9)
SCENARIOS = {"LOW": ("2", "0"), "BASE": ("5", "2"), "HIGH": ("10", "4")}


def json_line(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n"


def gzip_writer(path):
    return io.TextIOWrapper(gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0), encoding="utf-8")


def csv_write(path, rows):
    if not rows:
        raise ValueError("No fictional empty result tables")
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def simulate(record, bar, future, atr_value, *, scenario):
    """Counterfactual boundary-clock action; seal_research_action is NOT original-time proof.

    Actual receipts stay in CausalBarV2. The hypothetical seal is a simulation
    variable only and is always exported as modeled_decision_at_ns.
    """
    fee, slip = SCENARIOS[scenario]
    if record.strategy == "S1":
        side = record.setup_predicates["setup_side"]
        extreme = D(record.setup_predicates["setup_extreme"])
        stop = extreme - D("0.25") * D(str(atr_value)) if side == "LONG" else extreme + D("0.25") * D(str(atr_value))
        distance = (bar.close - stop) if side == "LONG" else (stop - bar.close)
        if not D("0.5") * D(str(atr_value)) <= distance <= D(3) * D(str(atr_value)):
            return None, (), "STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR"
        hold = 4 * HOUR
        plan = {}
    else:
        side = record.trigger_predicates["breakout_side"]
        iv = record.indicator_values
        stop = D(iv["range_low"] if side == "LONG" else iv["range_high"])
        hold = 2 * HOUR
        plan = {"range_low": iv["range_low"], "range_high": iv["range_high"]}
    # Analytical decimal grid, never presented as actual historical venue filters.
    tick = bar.close * D("0.0000000001")
    action = seal_research_action(
        cycle_id="HISTORICAL_BOUNDARY_PROXY_V1",
        symbol=record.symbol,
        strategy=record.strategy,
        policy_id=record.policy_id,
        policy_hash=record.policy_hash,
        side=side,
        entry_reference=bar.close,
        stop_price=stop,
        stop_basis="REGISTERED_CLOSE_TRIGGER_OHLC_BOUND",
        qty_step=D("0.00000001"),
        tick_size=tick,
        min_notional=None,
        spread_bps=D(2),
        atr_value=D(str(atr_value)),
        max_hold_ns=hold,
        decision_at_ns=record.information_cutoff_ns,
        deadline_offset_ns=5_000_000_000,
        evidence_refs=(record.applicability_id,),
        sealed_at_ns=record.information_cutoff_ns,
        setup_at_ns=record.detail.get("setup_at_ns", record.information_cutoff_ns),
        trigger_at_ns=record.information_cutoff_ns,
        instrument_revision=bar.instrument_revision,
        opportunity_id=digest(
            (
                record.symbol
                + record.strategy
                + str(record.information_cutoff_ns)
                + str(record.detail.get("watch_id", ""))
            ).encode()
        ),
        max_qty=D(10000) / bar.close,
        execution_plan={
            **plan,
            "simulation_clock": "HYPOTHETICAL_ZERO_DELAY_NOT_AN_ACTUAL_SEAL",
            "historical_venue_filters": "UNKNOWN",
            "fee_scenario": scenario,
        },
    )
    action = replace(
        action,
        fee_bps=D(fee),
        slippage_bps=D(slip),
        action_id=digest((action.action_id + scenario).encode()),
        risk_profile="VIRTUAL_10000_USDT_0.5PCT_STOP_RISK_10000_NOTIONAL_CAP",
        provenance={
            "actual_historical_seal": "UNAVAILABLE",
            "counterfactual_boundary_clock": True,
            "original_source_health": "UNAVAILABLE",
            "historical_filters": "UNAVAILABLE",
        },
    )
    outcomes = evaluate_lifecycle(action, future_bars=future, spread_bps=D(2))
    return action, outcomes, action.hard_refusal_reason or ("PATH_UNAVAILABLE" if not outcomes else "")


def summarize(rows, slots):
    closed = [r for r in rows if r["exit_state"] in ("CLOSED", "NO_POSITION")]
    cash = [float(r["conditional_before_funding_cash"]) for r in closed]
    filled = [r for r in closed if r["entry_state"] != "NO_FILL"]
    wins = [x for x in cash if x > 0]
    losses = [x for x in cash if x < 0]
    equity = peak = dd = 0.0
    for r in sorted(closed, key=lambda r: (r["exit_at_ns"], r["action_id"])):
        equity += float(r["conditional_before_funding_cash"])
        peak = max(peak, equity)
        dd = max(dd, peak - equity)
    notionals = [float(r["notional_usd"]) for r in filled]
    held_hours = sum(float(r["notional_usd"]) * (r["exit_at_ns"] - r["modeled_decision_at_ns"]) / HOUR for r in filled)
    result = {
        "modeled_actions": len(rows),
        "observable_conditional_paths": len(closed),
        "modeled_filled_actions": len(filled),
        "no_fill_actions": len(closed) - len(filled),
        "censored_actions": len(rows) - len(closed),
        "mean_cash_per_modeled_action": sum(cash) / len(rows) if rows and len(closed) == len(rows) else None,
        "total_conditional_cash": sum(cash) if len(closed) == len(rows) else None,
        "observed_closed_subset_cash": sum(cash),
        "win_rate_closed": len(wins) / len(cash) if cash else None,
        "avg_win": mean(wins) if wins else None,
        "avg_loss": mean(losses) if losses else None,
        "profit_factor": sum(wins) / -sum(losses) if losses else None,
        "closed_exit_drawdown_cash": dd if cash else None,
        "mean_cash_per_price_evaluated_slot": sum(cash) / slots if slots and len(closed) == len(rows) else None,
        "price_evaluated_slots": slots,
        "mean_filled_notional": mean(notionals) if notionals else None,
        "notional_hours": held_hours,
        "cash_after_adverse_5bps_per_filled_action": sum(cash) - sum(notionals) * 0.0005
        if len(closed) == len(rows)
        else None,
        "cash_after_adverse_20bps_per_filled_action": sum(cash) - sum(notionals) * 0.002
        if len(closed) == len(rows)
        else None,
        "cash_at_50pct_fill_quantity": sum(cash) / 2 if len(closed) == len(rows) else None,
        "all_no_fill_cash": 0,
        "arrival_60seconds_cash": 0,
        "exact_realized_cash": None,
        "native_full_policy_cash": None,
        "evidence_grade": "CONDITIONAL_HISTORICAL_OHLC_MODEL_FUNDING_MISSING",
        "missing_reason": "BBO/book/fill/fee/product/source-health/event/PIT-universe unavailable; funding scenario, not exact cash",
    }
    return result


def calendar_bootstrap(rows, begin, end):
    """All symbols in the same calendar block; exploratory, never admission."""
    results = {}
    for weeks in (1, 2):
        width = weeks * 7 * DAY
        n = math.ceil((end - begin) / width)
        blocks = [0.0] * n
        for r in rows:
            if r["exit_state"] not in ("CLOSED", "NO_POSITION"):
                continue
            blocks[min(n - 1, (r["modeled_decision_at_ns"] - begin) // width)] += float(
                r["conditional_before_funding_cash"]
            )
        rng = random.Random(20261021)
        samples = sorted(sum(rng.choice(blocks) for _ in blocks) for _ in range(2000))
        results[f"{weeks}_week"] = {
            "calendar_blocks": n,
            "exploratory_95pct_total_cash_interval": [samples[49], samples[1949]],
            "significance": "NOT ESTIMABLE",
            "reason": "fewer than 20 independent weeks; fixed incomplete cohort, execution uncertainty, multiple descriptive comparisons",
        }
    return results


def overlay(rows):
    """One active analytical action, no reinvestment; matched comparator envelopes."""
    busy = 0
    chosen = []
    blocked = 0
    for r in sorted(rows, key=lambda r: (r["modeled_decision_at_ns"], r["strategy"], r["symbol"], r["action_id"])):
        if r["modeled_decision_at_ns"] < busy:
            blocked += 1
            continue
        chosen.append(r)
        if r["entry_state"] != "NO_FILL":
            busy = r["exit_at_ns"]
    return chosen, blocked


def evaluate(root: Path, out: Path, *, holdout: bool):
    end = END if holdout else SPLIT
    suffix = "all" if holdout else "july"
    sources = json.loads((out / "archive_sources.json").read_text())
    symbols = json.loads((out / "COHORT_SELECTION.json").read_text())["selected"]
    store = ResearchStoreV1.open(root.parent / f"research-{suffix}.sqlite")
    counts = Counter()
    ledger = []
    counterparts = Counter()
    full_evaluations = 0
    eval_path = out / f"EVALUATIONS_{suffix}.jsonl.gz"
    with gzip_writer(eval_path) as evidence:
        for symbol in symbols:
            if shutil.disk_usage(root).free < 2_000_000_000 + 200_000_000:
                raise OSError("PHYSICAL_RESERVE_2GB")
            bars = [b for b in bars_for(root, sources, symbol, "15m") if b.close_at_ns <= end + 4 * HOUR]
            h1 = [b for b in bars_for(root, sources, symbol, "1h") if b.close_at_ns <= end]
            h4 = h4_from_h1(h1)
            key = key_for(symbol)
            av = atr(bars)
            times = [b.close_at_ns for b in bars]
            h1times = [b.close_at_ns for b in h1]
            h4times = [b.close_at_ns for b in h4]
            # Replay warmup before emitting July; no August input enters July predicates.
            signal_bars = [b for b in bars if b.close_at_ns < end]
            s1 = evaluate_s1_watch_lifecycle(
                cycle_id="HISTORICAL_PRICE_PILOT",
                symbol=symbol,
                key=key,
                m15=signal_bars,
                h1=h1,
                h4=h4,
                universe_eligible=False,
                setup_cache={},
                max_trigger_bars=0,
                precomputed_atr=av[: len(signal_bars)],
            )
            # universe_eligible=False is intentional: these are price diagnostics, not eligible actions.
            closes = [float(b.close) for b in bars]
            widths = [float("nan")] * len(bars)
            for i in range(19, len(bars)):
                w = closes[i - 19 : i + 1]
                widths[i] = 4 * pstdev(w) / (math.fsum(w) / 20)
            for i, b in enumerate(bars):
                t = b.close_at_ns
                if not BEGIN <= t < end:
                    continue
                split = "JULY_DEVELOPMENT" if t < SPLIT else "AUGUST_HOLDOUT"
                r1 = s1[i]
                # Source identity and all prefix arrays stop at this exact confirmed close.
                left = max(0, i + 1 - (s2_breakout.SAMPLE_SIZE + 21))
                r2 = evaluate_s2(
                    cycle_id="HISTORICAL_PRICE_PILOT",
                    symbol=symbol,
                    key=key,
                    cutoff_ns=t,
                    h4=h4[max(0, bisect.bisect_right(h4times, t) - 1) : bisect.bisect_right(h4times, t)],
                    h1=h1[max(0, bisect.bisect_right(h1times, t) - 1) : bisect.bisect_right(h1times, t)],
                    m15=bars[left : i + 1],
                    universe_eligible=False,
                    precomputed_atr=av[left : i + 1],
                    precomputed_widths=widths[left : i + 1],
                    precomputed_closes=closes[left : i + 1],
                )
                records = [r1, r2]
                # Existing RAW_RANGE challenger retains native range width, stop, entry and failure exits.
                iv = r2.indicator_values
                if iv:
                    raw_side = s2_breakout.breakout_side(
                        b.close, D(iv["range_high"]), D(iv["range_low"]), D(iv["latest_atr14"])
                    )
                    raw_ok = (
                        s2_breakout.range_width_valid(D(iv["range_width"]), D(iv["latest_atr14"]))
                        and raw_side is not None
                    )
                    raw = replace(
                        r2,
                        strategy="S2_RAW_RANGE",
                        policy_id=s2_breakout.policy_spec(variant="RAW_RANGE").policy_id,
                        policy_hash=s2_breakout.policy_spec(variant="RAW_RANGE").policy_hash,
                        reason_code="PRICE_TRIGGER" if raw_ok else "NO_SETUP",
                        trigger_predicates={
                            **r2.trigger_predicates,
                            "breakout_side": raw_side.value if raw_side else None,
                        },
                    )
                    records.append(raw)
                for rec in records:
                    counts[(rec.strategy, split, "evaluated")] += 1
                    counts[(rec.strategy, split, rec.reason_code)] += 1
                    row = rec.as_row()
                    evidence.write(json_line(row))
                    store._record("strategy_applicability", row, commit=False)
                    full_evaluations += 1
                    trigger = (
                        (rec.strategy == "S1" and rec.trigger_predicates.get("breakout_confirmed"))
                        or (rec.strategy == "S2" and rec.reason_code == "UNIVERSE_INELIGIBLE")
                        or (rec.strategy == "S2_RAW_RANGE" and rec.reason_code == "PRICE_TRIGGER")
                    )
                    if not trigger:
                        continue
                    counts[(rec.strategy, split, "price_trigger")] += 1
                    horizon = 4 * HOUR if rec.strategy == "S1" else 2 * HOUR
                    future = bars[bisect.bisect_right(times, t) : bisect.bisect_right(times, t + horizon)]
                    # bisect_right includes first next bar (whose open is t); last closes at horizon.
                    for scenario in SCENARIOS:
                        simulation_rec = replace(rec, strategy="S2") if rec.strategy == "S2_RAW_RANGE" else rec
                        action, outcomes, refusal = simulate(
                            simulation_rec,
                            b,
                            future,
                            av[i] if rec.strategy == "S1" else float(iv["latest_atr14"]),
                            scenario=scenario,
                        )
                        if refusal:
                            counterparts[(rec.strategy, split, scenario, refusal)] += 1
                        if action is None:
                            continue
                        counts[(rec.strategy, split, "sized_action")] += int(scenario == "BASE")
                        store._record("research_action", action.as_row(), commit=False)
                        for outcome in outcomes:
                            detail = outcome.detail
                            lr = {
                                **asdict(outcome),
                                "lifecycle_id": digest((action.action_id + outcome.management_kind).encode()),
                                "action_id": action.action_id,
                            }
                            store._record("lifecycle_evidence", json.loads(json.dumps(lr, default=str)), commit=False)
                            ledger.append(
                                {
                                    "action_id": action.action_id,
                                    "opportunity_id": action.opportunity_id,
                                    "symbol": symbol,
                                    "strategy": rec.strategy,
                                    "policy_id": rec.policy_id,
                                    "policy_hash": rec.policy_hash,
                                    "split": split,
                                    "scenario": scenario,
                                    "management": outcome.management_kind,
                                    "side": action.side,
                                    "modeled_decision_at_ns": t,
                                    "setup_at_ns": action.setup_at_ns,
                                    "exit_at_ns": detail.get("exit_at_ns", action.horizon_end_ns),
                                    "entry_state": outcome.entry_state,
                                    "exit_state": outcome.exit_state,
                                    "exit_reason": outcome.exit_reason,
                                    "entry_price": detail.get("entry_price"),
                                    "stop_price": str(action.stop_price),
                                    "notional_usd": str(action.notional_usd),
                                    "quantity": str(action.quantity),
                                    "gross_cash": str(outcome.gross_cash),
                                    "entry_fee": detail.get("entry_fee", "0"),
                                    "exit_fee": detail.get("exit_fee", "0"),
                                    "conditional_before_funding_cash": str(outcome.net_cash),
                                    "remaining_quantity": detail.get("remaining_quantity", "0"),
                                    "uncertainty": outcome.uncertainty,
                                    "actual_historical_seal_at_ns": None,
                                    "actual_fill": False,
                                }
                            )
            store._connect().commit()
            print(f"{symbol}: native price predicates evaluated; cumulative model paths={len(ledger)}", flush=True)
    with gzip_writer(out / f"ACTION_PATHS_{suffix}.jsonl.gz") as f:
        for row in ledger:
            f.write(json_line(row))
    result = []
    strategy_results = []
    inference = {}
    splits = ["JULY_DEVELOPMENT"] + (["AUGUST_HOLDOUT", "ALL"] if holdout else [])
    for strategy in ("S1", "S2", "S2_RAW_RANGE"):
        for split in splits:
            parts = ["JULY_DEVELOPMENT", "AUGUST_HOLDOUT"] if split == "ALL" else [split]
            c = Counter()
            for (st, sp, k), v in counts.items():
                if st == strategy and sp in parts:
                    c[k] += v
            strategy_results.append(
                {
                    "strategy": strategy,
                    "split": split,
                    "native_clock": "CONFIRMED_15M_CLOSE",
                    "historically_due_price_slots": int((end - BEGIN) // M15) * len(symbols)
                    if split == "ALL"
                    else 31 * 96 * len(symbols),
                    "successfully_price_evaluated": c["evaluated"],
                    "price_trigger_count": c["price_trigger"],
                    "sized_conditional_actions": c["sized_action"],
                    "native_executable_candidates_produced": 0,
                    "native_eligible_denominator": None,
                    "native_setup_population": None,
                    "status": "PRICE_DIAGNOSTIC_AND_CONDITIONAL_EXECUTION_ONLY",
                    "reason_counts": json.dumps(
                        {k: v for k, v in c.items() if k not in ("evaluated", "price_trigger", "sized_action")},
                        sort_keys=True,
                    ),
                    "missing_required_inputs": "Historical BBO/source-health/contract/universe/event identity; true fills and fees",
                    "denominator_definition": "one symbol/M15 confirmed close within declared evaluation interval; warmup excluded",
                }
            )
            for scenario in SCENARIOS:
                for mgmt in ("HOLD", "CLOSE", "TIGHTEN"):
                    rs = [
                        r
                        for r in ledger
                        if r["strategy"] == strategy
                        and r["split"] in parts
                        and r["scenario"] == scenario
                        and r["management"] == mgmt
                    ]
                    for mode in ("INDEPENDENT_ACTIONS", "ONE_AT_A_TIME"):
                        selected, blocked = (rs, 0) if mode == "INDEPENDENT_ACTIONS" else overlay(rs)
                        row = {
                            "strategy": strategy,
                            "split": split,
                            "scenario": scenario,
                            "management": mgmt,
                            "capital_model": mode,
                            "overlap_blocked": blocked,
                            **summarize(selected, c["evaluated"]),
                        }
                        result.append(row)
                    if scenario == "BASE" and mgmt == "HOLD":
                        begin = BEGIN if split != "AUGUST_HOLDOUT" else SPLIT
                        final = SPLIT if split == "JULY_DEVELOPMENT" else end
                        inference[strategy + "_" + split] = calendar_bootstrap(rs, begin, final)
    # Unknown denominators are missing, never invented zeros or synthetic evaluations.
    base_row = strategy_results[0]
    for st, clock, reason, due in [
        (
            "S3",
            "CONFIRMED_1M_CLOSE",
            "No M1/every-trade VWAP seven-day population; aggTrades forbidden substitute",
            62 * 1440 * len(symbols) if holdout else 31 * 1440 * len(symbols),
        ),
        (
            "S4",
            "CONTEXT_10S_OBSERVATION",
            "No historical sequence-valid book/BBO/trade receipt/source health; contextual",
            None,
        ),
        (
            "S5",
            "CONTEXT_15M_CLOSE",
            "Funding probe only; no complete OI/liquidation population; contextual",
            62 * 96 * len(symbols) if holdout else 31 * 96 * len(symbols),
        ),
        (
            "S6",
            "CONFIRMED_1H_CLOSE",
            "Only ten research symbols, below twenty eligible breadth; historical eligibility and funding/liquidity missing",
            62 * 24 * len(symbols) if holdout else 31 * 24 * len(symbols),
        ),
        (
            "S7",
            "VERIFIED_EVENT_PUBLICATION",
            "No versioned verified calendar/publication/asset mapping; cannot call no event",
            None,
        ),
        ("S8", "CONFIRMED_1H_CLOSE", "No registered economic pair catalogue or both-leg execution contract", None),
        (
            "PATH_B",
            "DISCOVERY_ATTEMPT_CYCLE",
            "No registered bounded novelty experiment/policy for this cohort; no generated directions",
            None,
        ),
    ]:
        strategy_results.append(
            {
                **dict.fromkeys(base_row),
                "strategy": st,
                "split": "ALL" if holdout else "JULY_DEVELOPMENT",
                "native_clock": clock,
                "historically_due_price_slots": due,
                "successfully_price_evaluated": 0,
                "native_executable_candidates_produced": 0,
                "status": "REQUIRED_DATA_UNAVAILABLE" if st != "PATH_B" else "NOT_EVALUATED",
                "missing_required_inputs": reason,
                "denominator_definition": "hypothetical registered calendar where defined; no original prospective scheduling",
            }
        )
    csv_write(out / f"STRATEGY_RESULTS{'' if holdout else '_july'}.csv", strategy_results)
    csv_write(out / f"ECONOMIC_RESULTS{'' if holdout else '_july'}.csv", result)
    breakdown = []
    for strategy in ("S1", "S2", "S2_RAW_RANGE"):
        for split in splits:
            for symbol in symbols:
                for side in ("LONG", "SHORT"):
                    rs = [
                        r
                        for r in ledger
                        if r["strategy"] == strategy
                        and (r["split"] == split or split == "ALL")
                        and r["symbol"] == symbol
                        and r["side"] == side
                        and r["scenario"] == "BASE"
                        and r["management"] == "HOLD"
                    ]
                    if rs:
                        breakdown.append(
                            {
                                "strategy": strategy,
                                "split": split,
                                "symbol": symbol,
                                "side": side,
                                **summarize(rs, None),
                            }
                        )
    csv_write(out / f"SYMBOL_DIRECTION_{suffix}.csv", breakdown)
    (out / f"UNCERTAINTY_{suffix}.json").write_text(json.dumps(inference, indent=2, sort_keys=True) + "\n")
    (out / f"REFUSALS_{suffix}.json").write_text(
        json.dumps(
            [
                {"strategy": k[0], "split": k[1], "scenario": k[2], "reason": k[3], "count": v}
                for k, v in sorted(counterparts.items())
            ],
            indent=2,
        )
        + "\n"
    )
    integrity = store.query("PRAGMA integrity_check")
    qualified = store.query("SELECT count(*) AS n FROM effective_outcome")[0]["n"]
    store.close()
    print(
        json.dumps(
            {
                "evaluation_records": full_evaluations,
                "conditional_paths": len(ledger),
                "sqlite_integrity": integrity,
                "qualified_outcomes": qualified,
            }
        ),
        flush=True,
    )


def funding_cash_interval(side, quantity, rate, low, high, *, exposure_ambiguous=False):
    """Native signed cash primitive on genuine mark bounds; no fake point mark."""
    from atlas.v2.science.costs import funding_cash

    from .actions import FundingSettlement

    values = [
        funding_cash(side, D(quantity), D(1), FundingSettlement(1, D(rate), D(p), 1, "archived-mark-bound"))
        for p in (low, high)
    ]
    if exposure_ambiguous:
        values.append(D(0))
    return min(values), max(values)


def funding_bounds_report(permanent, out):
    import zipfile

    from .historical_archive import read_zip

    sources = json.loads((out / "AUXILIARY_SOURCES.json").read_text())
    events = {}
    marks = {}
    months = defaultdict(set)
    for record in sources:
        if record["status"] != "VERIFIED":
            continue
        symbol = record["symbol"]
        kind = record["type"]
        name = record["url"].rsplit("/", 1)[1]
        blob = (permanent / "aux" / kind / name).read_bytes()
        if digest(blob) != record["archive_sha256"]:
            raise ValueError("AUXILIARY_CONTENT_CHANGED")
        if kind == "fundingRate":
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                rows = list(csv.DictReader(io.StringIO(z.read(z.namelist()[0]).decode())))
            for row in rows:
                t = int(row["calc_time"]) * 1_000_000
                events.setdefault(symbol, []).append((t, D(row["last_funding_rate"])))
            months[symbol].add(record["month"])
        else:
            _, rows, _ = read_zip(blob, name, "15m", record["month"])
            for row in rows:
                marks[(symbol, int(row[0]) * 1_000_000)] = (D(row[3]), D(row[2]))
    with gzip.open(out / "ACTION_PATHS_all.jsonl.gz", "rt") as f:
        paths = [json.loads(x) for x in f]
    grouped = defaultdict(list)
    action_rows = []
    for row in paths:
        lower = upper = D(0)
        available = True
        payments = 0
        if row["entry_state"] != "NO_FILL":
            start = row["modeled_decision_at_ns"]
            end = row["exit_at_ns"]
            # Required months are checked, and every retained event is used at its original calc_time.
            for at in (start, end):
                month = datetime.fromtimestamp(at / 1e9, UTC).strftime("%Y-%m")
                if month not in months[row["symbol"]]:
                    available = False
            for t, rate in events.get(row["symbol"], []):
                if not start < t <= end:
                    continue
                mark = marks.get((row["symbol"], t // M15 * M15))
                if mark is None:
                    available = False
                    continue
                uncertain = row["exit_reason"] in ("STOP", "STOP_GAP") and end - M15 < t <= end
                lo, hi = funding_cash_interval(row["side"], row["quantity"], rate, *mark, exposure_ambiguous=uncertain)
                lower += lo
                upper += hi
                payments += 1
        item = {k: row[k] for k in ("action_id", "symbol", "strategy", "split", "scenario", "management")}
        item.update(
            funding_lower=str(lower) if available else None,
            funding_upper=str(upper) if available else None,
            archived_rate_events_during_modeled_exposure=payments,
            conditional_cash_lower=str(D(row["conditional_before_funding_cash"]) + lower) if available else None,
            conditional_cash_upper=str(D(row["conditional_before_funding_cash"]) + upper) if available else None,
            funding_status="CONDITIONAL_CALC_TIME_SETTLEMENT_AND_15M_MARK_INTERVAL" if available else "MISSING_SOURCE",
            actual_settled_account_cash=None,
        )
        action_rows.append(item)
        for split in (row["split"], "ALL"):
            grouped[(row["strategy"], split, row["scenario"], row["management"])].append(item)
    with gzip_writer(out / "FUNDING_ACTION_INTERVALS.jsonl.gz") as f:
        for row in action_rows:
            f.write(json_line(row))
    results = []
    for key, rs in sorted(grouped.items()):
        complete = all(r["funding_lower"] is not None for r in rs)
        results.append(
            dict(zip(("strategy", "split", "scenario", "management"), key, strict=True))
            | {
                "modeled_actions": len(rs),
                "funding_lower_cash": sum(float(r["funding_lower"]) for r in rs) if complete else None,
                "funding_upper_cash": sum(float(r["funding_upper"]) for r in rs) if complete else None,
                "after_fees_and_funding_lower_cash": sum(float(r["conditional_cash_lower"]) for r in rs)
                if complete
                else None,
                "after_fees_and_funding_upper_cash": sum(float(r["conditional_cash_upper"]) for r in rs)
                if complete
                else None,
                "missing_source_actions": sum(r["funding_lower"] is None for r in rs),
                "scope": "INDEPENDENT_ACTIONS_NOT_NATIVE_PORTFOLIO",
                "assumption": "Archived calc_time treated as settlement; original 0/1ms retained; actual mark within 15m envelope; exit-bar exposure interval",
                "exact_funding_cash": None,
            }
        )
    csv_write(out / "FUNDING_RESULTS.csv", results)
