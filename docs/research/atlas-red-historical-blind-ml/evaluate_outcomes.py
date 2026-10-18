"""Separate outcome stage: immutable decisions in, research outcomes out.

Cannot fit models, choose thresholds, invoke a provider or create a legal action.
No decision callback is accepted. Git and byte checks run before source reads.
"""

from __future__ import annotations

import argparse
import collections
import csv
import functools
import hashlib
import json
import math
import pathlib
import resource
import statistics
import subprocess
import time
from decimal import Decimal, localcontext

import common
import numpy as np
from causal_replay import load_native, source_rows
from economics import unit_cash

ARMS = ("S1_ALL", "S1_S6", "EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT", "NO_OPEN")
MODELS = ("EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT")
HORIZONS = (1, 4, 24)
FINAL = common.ns("2025-10-01T00:00:00Z")


def verify_seal(commit):
    relative = str(common.HERE.relative_to(common.ROOT) / "DECISION_SEAL.json")
    sealed = subprocess.check_output(["git", "show", commit + ":" + relative], cwd=common.ROOT)
    if sealed != (common.HERE / "DECISION_SEAL.json").read_bytes():
        raise ValueError("DECISION_SEAL_NOT_EXACT_COMMITTED_BYTES")
    seal = json.loads(sealed)
    for path, expected in seal["pinned_files"].items():
        local = common.ROOT / path
        body = subprocess.check_output(["git", "show", commit + ":" + path], cwd=common.ROOT)
        if hashlib.sha256(body).hexdigest() != expected or common.sha(local) != expected:
            raise ValueError("SEALED_DECISION_MODEL_CODE_SOURCE_CHANGED:" + path)
    if seal["test_outcomes_computed"] or seal["protocol_sha256"] != common.PROTOCOL_SHA:
        raise ValueError("INVALID_PREOUTCOME_SEAL")
    assemble_calendar(seal)
    return seal


def assemble_calendar(seal):
    """Lossless transport packaging only; never repair a mismatched artifact."""
    for path, spec in seal.get("assembled_files", {}).items():
        target = common.ROOT / path
        if target.exists():
            if common.sha(target) != spec["sha256"]:
                raise ValueError("ASSEMBLED_CALENDAR_IDENTITY_MISMATCH")
            continue
        temporary = target.with_suffix(target.suffix + ".assembly-tmp")
        with temporary.open("wb") as writer:
            for part in spec["parts"]:
                piece = common.ROOT / part
                if common.sha(piece) != seal["pinned_files"][part]:
                    raise ValueError("CALENDAR_PART_IDENTITY_MISMATCH")
                with piece.open("rb") as reader:
                    while chunk := reader.read(1024 * 1024):
                        writer.write(chunk)
        if common.sha(temporary) != spec["sha256"]:
            raise ValueError("ASSEMBLED_CALENDAR_IDENTITY_MISMATCH")
        temporary.replace(target)


def outcome(row, bars, horizon, *, delayed=False, final=FINAL):
    at = int(row["cutoff_ns"])
    reference = int(row["delayed_reference_ns"] if delayed else row["reference_ns"])
    terminal = int(row["reference_ns"]) + horizon * common.HOUR
    if reference <= int(row["ready_ns"]):
        raise ValueError("PRICE_REFERENCE_NOT_AFTER_READY")
    if reference < at + common.M15 or reference >= terminal:
        raise ValueError("INVALID_RESEARCH_REFERENCE")
    if terminal + 6 * common.SECOND >= final:
        return {"status": "CENSORED_WINDOW_END"}
    required = list(range(at, terminal + common.M15, common.M15))
    path = [bars.get(t) for t in required]
    if any(b is None for b in path):
        return {"status": "CENSORED_NATIVE_SOURCE_GAP"}
    ref, end = bars[reference], bars[terminal]
    a, b = ref.close, end.close
    sign = 1 if row["side"] == "LONG" else -1
    gross = sign * float(b / a - 1) * 10000
    extremes = [0.0]
    for bar in path:
        if bar.end > reference:
            extremes += [sign * float(price / a - 1) * 10000 for price in (bar.high, bar.low)]
    refs = [bar.source for bar in path]
    return {
        "status": "OBSERVED",
        "reference_ns": reference,
        "terminal_ns": terminal,
        "label_available_ns": terminal + 6 * common.SECOND,
        "reference_close": str(a),
        "endpoint_close": str(b),
        "reference_source": ref.source,
        "endpoint_source": end.source,
        "path_sources_sha256": common.digest(refs),
        "gross_bps": gross,
        "mfe_bps": max(extremes),
        "mae_bps": min(extremes),
    }


def distribution(values):
    if not values:
        return {"n": 0, "mean": None, "median": None, "q01": None, "q05": None, "q95": None}
    x = np.asarray(values, dtype=float)
    return {
        "n": len(values),
        "mean": math.fsum(values) / len(values),
        "median": float(np.median(x)),
        "q01": float(np.quantile(x, 0.01)),
        "q05": float(np.quantile(x, 0.05)),
        "q95": float(np.quantile(x, 0.95)),
    }


def monday(at):
    import datetime as dt

    t = dt.datetime.fromtimestamp(int(at) / common.SECOND, dt.UTC)
    day = t.date() - dt.timedelta(days=t.weekday())
    return day.isoformat()


def calendar_weeks():
    import datetime as dt

    first = dt.date.fromisoformat(monday(common.ns("2025-07-01T00:00:00Z")))
    end = dt.date.fromisoformat(monday(FINAL - 1))
    return [(first + dt.timedelta(days=7 * i)).isoformat() for i in range((end - first).days // 7 + 1)]


def paired_intervals(rows, columns, *, seed=18102026):
    weeks = calendar_weeks()
    sums = {w: [0.0] * (len(columns) + 1) for w in weeks}
    for row in rows:
        block = sums[monday(row["cutoff_ns"])]
        block[0] += 1
        for i, column in enumerate(columns, 1):
            block[i] += float(row[column])
    result = {}
    for size in (1, 2):
        values = np.array([np.sum([sums[w] for w in weeks[i : i + size]], axis=0) for i in range(0, len(weeks), size)])
        rng = np.random.default_rng(seed + size)
        draws = rng.integers(0, len(values), size=(10000, len(values)))
        sampled = values[draws].sum(axis=1)
        sampled = sampled[sampled[:, 0] > 0]
        intervals = {
            c: [float(x) for x in np.quantile(sampled[:, i + 1] / sampled[:, 0], [0.025, 0.975])]
            for i, c in enumerate(columns)
        }
        result[str(size) + "_week"] = {
            "blocks": len(values),
            "resamples": len(sampled),
            "mean_intervals": intervals,
            "inference_status": "TEST GATE: <20 blocks; exploratory dependence/selection-sensitive intervals only",
        }
    return result


def accepted(row, arm):
    return row[arm + "_accept"] == "1"


def economic_table(rows, horizon):
    complete = [r for r in rows if r[f"outcome_{horizon}h_status"] == "OBSERVED"]
    result = {}
    for arm in ARMS:
        selected = [r for r in rows if accepted(r, arm)]
        mature = [r for r in complete if accepted(r, arm)]
        values = [float(r[f"gross_{horizon}h_bps"]) if accepted(r, arm) else 0.0 for r in complete]
        result[arm] = {
            "original_N": len(rows),
            "common_complete_N": len(complete),
            "accepted_original": len(selected),
            "rejected_original": len(rows) - len(selected),
            "accepted_censored": len(selected) - len(mature),
            "common_original_gross_bps": distribution(values),
            "accepted_gross_bps": distribution([float(r[f"gross_{horizon}h_bps"]) for r in mature]),
            "full_N_gross_mean_bps": None
            if len(selected) != len(mature)
            else math.fsum(values) / len(rows)
            if rows
            else None,
        }
    return result


def prediction_metrics(rows, bundle):
    complete = [r for r in rows if r["outcome_4h_status"] == "OBSERVED"]
    matched = [r for r in complete if r["common_model_support"] == "1"]
    result = {
        "original_N": len(rows),
        "outcome_complete_N": len(complete),
        "same_row_common_model_N": len(matched),
        "models": {},
    }
    for name in MODELS:
        own = [r for r in complete if r[name + "_prediction_bps"] != ""]
        reports = {}
        for label, population in (("own_support", own), ("same_row_common_support", matched)):
            errors = [float(r[name + "_prediction_bps"]) - float(r["gross_4h_bps"]) for r in population]
            lo = bundle["models"][name]["residual_q05_bps"]
            hi = bundle["models"][name]["residual_q95_bps"]
            reports[label] = {
                "n": len(errors),
                "bias_prediction_minus_target_bps": statistics.mean(errors) if errors else None,
                "MAE_bps": statistics.mean(abs(e) for e in errors) if errors else None,
                "RMSE_bps": math.sqrt(statistics.mean(e * e for e in errors)) if errors else None,
                "direction_sign_agreement": statistics.mean(
                    (float(r[name + "_prediction_bps"]) > 0) == (float(r["gross_4h_bps"]) > 0) for r in population
                )
                if population
                else None,
                "empirical_residual_interval_coverage": statistics.mean(lo <= -e <= hi for e in errors)
                if errors
                else None,
                "interpretation": "Regression/residual diagnostic, not probability/calibration qualification",
            }
        result["models"][name] = {
            "metrics": reports,
            "original_prediction_status": dict(collections.Counter(r[name + "_status"] for r in rows)),
        }
    paired = []
    for r in matched:
        y = float(r["gross_4h_bps"])
        e = {n: float(r[n + "_prediction_bps"]) - y for n in MODELS}
        paired.append(
            {
                "cutoff_ns": r["cutoff_ns"],
                "CTX_minus_empirical_MSE": e["RIDGE_CONTEXT"] ** 2 - e["EMPIRICAL"] ** 2,
                "CTX_minus_S1_MSE": e["RIDGE_CONTEXT"] ** 2 - e["RIDGE_S1"] ** 2,
            }
        )
    result["paired_error_uncertainty"] = (
        paired_intervals(paired, ("CTX_minus_empirical_MSE", "CTX_minus_S1_MSE")) if paired else {}
    )
    return result


def cash_sensitivities(rows):
    complete = [r for r in rows if r["outcome_4h_status"] == "OBSERVED" and r["delayed_4h_status"] == "OBSERVED"]
    result, paired = {}, []
    paired_cash = [{"cutoff_ns": r["cutoff_ns"]} for r in complete]
    for arm in ARMS:
        by_scenario = {}
        for scenario in (
            "GROSS",
            "FIXED_BEFORE_UNKNOWN_FUNDING",
            "FIXED_ASSUMED_FUNDING_CREDIT5",
            "FIXED_ASSUMED_FUNDING_DEBIT5",
            "ADVERSE_DELAY_NOFILL",
        ):
            values = []
            components = collections.defaultdict(float)
            filled_count = 0
            nofill_count = 0
            for row_index, r in enumerate(complete):
                take = accepted(r, arm)
                nofill = scenario == "ADVERSE_DELAY_NOFILL" and int(r["event_id"][:16], 16) % 5 == 0
                filled = take and not nofill
                if take and nofill:
                    nofill_count += 1
                filled_count += int(filled)
                delayed = scenario == "ADVERSE_DELAY_NOFILL"
                fees, spread, slip, funding = (
                    (6, 4, 4, 5)
                    if delayed
                    else (0, 0, 0, 0)
                    if scenario == "GROSS"
                    else (4, 2, 2, -5 if scenario.endswith("CREDIT5") else 5 if scenario.endswith("DEBIT5") else None)
                )
                cash = unit_cash(
                    side=r["side"],
                    reference=r["delayed_reference_close"] if delayed else r["reference_4h_close"],
                    endpoint=r["endpoint_4h_close"],
                    fees_bps=fees,
                    spread_bps=spread,
                    slippage_bps=slip,
                    funding_charge_bps=funding,
                    filled=filled,
                )
                value = (
                    cash["conditional_net_cash"]
                    if cash["conditional_net_cash"] is not None
                    else cash["before_unknown_funding_cash"]
                )
                if scenario == "GROSS":
                    value = cash["gross_cash"]
                values.append(float(value))
                if scenario in ("FIXED_BEFORE_UNKNOWN_FUNDING", "FIXED_ASSUMED_FUNDING_DEBIT5", "ADVERSE_DELAY_NOFILL"):
                    paired_cash[row_index][arm + ":" + scenario] = float(value) * 10000
                for k in (
                    "gross_cash",
                    "fees_cash",
                    "spread_cash",
                    "slippage_cash",
                    "funding_charge_cash",
                    "programme_expense_cash",
                ):
                    if cash[k] is not None:
                        components[k] += float(cash[k])
            by_scenario[scenario] = {
                "original_N": len(rows),
                "common_original_N": len(complete),
                "accepted": sum(accepted(r, arm) for r in complete),
                "assumed_fills": filled_count,
                "assumed_no_fills": nofill_count,
                "unit_notional_cash": distribution(values),
                "sum_unit_notional_cash": math.fsum(values),
                "mean_bps_of_one_unit_per_original_opportunity": math.fsum(values) / len(values) * 10000
                if values
                else None,
                "component_sum_cash": dict(components),
                "actual_programme_net_cash": None,
                "actual_return_on_capital": None,
                "actual_portfolio_drawdown": None,
                "full_original_mean_status": "NOT ESTIMABLE if any accepted filled horizon is censored; common complete results shown",
                "status": "CONDITIONAL_RESEARCH_ASSUMPTIONS; no actual fills/qualified risk-sized actions",
            }
        result[arm] = by_scenario
    for r in complete:
        y = float(r["gross_4h_bps"])
        item = {"cutoff_ns": r["cutoff_ns"]}
        for arm in ARMS:
            item[arm] = y if accepted(r, arm) else 0.0
        item["CTX_minus_empirical"] = item["RIDGE_CONTEXT"] - item["EMPIRICAL"]
        item["CTX_minus_S1"] = item["RIDGE_CONTEXT"] - item["RIDGE_S1"]
        paired.append(item)
    result["paired_gross_price_uncertainty"] = (
        paired_intervals(paired, (*ARMS, "CTX_minus_empirical", "CTX_minus_S1")) if paired else {}
    )
    if paired_cash:
        cash_columns = tuple(k for k in paired_cash[0] if k != "cutoff_ns")
        result["paired_conditional_cash_uncertainty_bps"] = paired_intervals(paired_cash, cash_columns)
    return result


def mechanism_and_overlap(rows):
    """Registered count/composition and opportunity-time controls; no new policy."""
    complete = [r for r in rows if r["outcome_4h_status"] == "OBSERVED"]
    if not complete:
        return {"common_complete_N": 0}
    baseline = statistics.mean(float(r["gross_4h_bps"]) for r in complete)
    result = {"common_complete_N": len(complete), "S1_ALL_mean_bps": baseline, "arms": {}}
    for arm in ARMS:
        selected = [r for r in complete if accepted(r, arm)]
        fraction = len(selected) / len(complete)
        mean = math.fsum(float(r["gross_4h_bps"]) for r in selected) / len(complete)
        strata = {}
        for names in (("side",), ("side", "btc_h4_trend"), ("side", "eth_h4_trend")):
            grouped = collections.defaultdict(list)
            for r in complete:
                grouped[tuple(r[k] for k in names)].append(r)
            expected = 0.0
            for members in grouped.values():
                expected += (
                    sum(accepted(r, arm) for r in members)
                    * statistics.mean(float(r["gross_4h_bps"]) for r in members)
                    / len(complete)
                )
            strata["+".join(names)] = {"count_matched_expectation_bps": expected, "extra_bps": mean - expected}
        changes = []
        for r in selected:
            changes += [(int(r["reference_ns"]), 1), (int(r["reference_ns"]) + 4 * common.HOUR, -1)]
        live = peak = 0
        for _, delta in sorted(changes):
            live += delta
            peak = max(peak, live)
        concentration = {}
        for key in ("symbol", "week"):
            shares = collections.Counter(monday(r["cutoff_ns"]) if key == "week" else r[key] for r in selected)
            concentration[key] = {"max_accepted_share": max(shares.values()) / len(selected) if selected else None}
        result["arms"][arm] = {
            "accepted": len(selected),
            "retained_fraction": fraction,
            "mean_per_original_bps": mean,
            "count_only_thinning_expectation_bps": fraction * baseline,
            "extra_vs_count_only_bps": mean - fraction * baseline,
            "raw_delta_vs_S1_ALL_bps": mean - baseline,
            "composition_controls": strata,
            "avoided_negative_bps_per_original": -math.fsum(
                float(r["gross_4h_bps"]) for r in complete if not accepted(r, arm) and float(r["gross_4h_bps"]) < 0
            )
            / len(complete),
            "foregone_positive_bps_per_original": math.fsum(
                float(r["gross_4h_bps"]) for r in complete if not accepted(r, arm) and float(r["gross_4h_bps"]) > 0
            )
            / len(complete),
            "opportunity_hours": len(selected) * 4,
            "peak_overlapping_hypothetical_horizons": peak,
            "concentration": concentration,
            "MFE_bps": distribution([float(r["mfe_4h_bps"]) for r in selected if r.get("mfe_4h_bps", "") != ""]),
            "MAE_bps": distribution([float(r["mae_4h_bps"]) for r in selected if r.get("mae_4h_bps", "") != ""]),
            "interpretation": "Observational count/stratum controls and overlapping research horizons; no causal attribution, actual positions or portfolio risk",
        }
    return result


def slices(rows, bundle):
    labels = (
        "side",
        "symbol",
        "week",
        "btc_h4_trend",
        "eth_h4_trend",
        "s1_regime",
        "volatility_slice",
        "s6_decile",
        "common_model_support",
        "calendar_half",
    )
    result = {}
    for label in labels:
        groups = collections.defaultdict(list)
        for r in rows:
            value = (
                monday(r["cutoff_ns"])
                if label == "week"
                else ("FIRST" if int(r["cutoff_ns"]) < common.ns("2025-08-16T00:00:00Z") else "SECOND")
                if label == "calendar_half"
                else r[label]
            )
            groups[value].append(r)
        result[label] = {
            str(k): {
                "original_N": len(v),
                "arms": economic_table(v, 4),
                "ML": {name: prediction_metrics_simple(v, name) for name in MODELS},
            }
            for k, v in sorted(groups.items())
        }
    return result


def prediction_metrics_simple(rows, name):
    values = [
        float(r[name + "_prediction_bps"]) - float(r["gross_4h_bps"])
        for r in rows
        if r["outcome_4h_status"] == "OBSERVED" and r[name + "_prediction_bps"] != ""
    ]
    return {
        "original_N": len(rows),
        "supported_complete_N": len(values),
        "bias_bps": statistics.mean(values) if values else None,
        "MAE_bps": statistics.mean(abs(x) for x in values) if values else None,
        "RMSE_bps": math.sqrt(statistics.mean(x * x for x in values)) if values else None,
    }


def calendar_check(decisions_path, calendar_path):
    p = common.protocol()
    at = common.ns("2025-07-01T00:00:00Z")
    index, counts, event_ids = 0, collections.Counter(), set()
    for row in common.read_csv(calendar_path):
        expected_at = at + (index // len(p["cohort"])) * common.M15
        if row["symbol"] != p["cohort"][index % len(p["cohort"])] or int(row["cutoff_ns"]) != expected_at:
            raise ValueError("WHOLE_CALENDAR_KEY_MISMATCH")
        counts["current_m15:" + row["source_current_m15"]] += 1
        counts["S1:" + row["s1_precursor"]] += 1
        counts["S6:" + row["s6_state"]] += 1
        if row["first_event_id"]:
            if row["first_event_id"] in event_ids:
                raise ValueError("DUPLICATE_EPISODE_WEIGHT")
            event_ids.add(row["first_event_id"])
        index += 1
    if index != (FINAL - at) // common.M15 * len(p["cohort"]):
        raise ValueError("INCOMPLETE_NATIVE_CALENDAR")
    rows = list(common.read_csv(decisions_path))
    if event_ids != {r["event_id"] for r in rows} or len(rows) != len(event_ids):
        raise ValueError("CALENDAR_EVENT_POPULATION_MISMATCH")
    return {
        "native_symbol_slots": index,
        "counts": dict(counts),
        "unique_first_events": len(event_ids),
        "join_mismatches": 0,
    }


def independent_price_checks(rows, old_cache, new_cache):
    """Fixed first/middle/last per symbol/horizon, exact raw CSV Decimal oracle."""
    by_sha = {
        r["downloaded_sha256"]: (r, old_cache if location == "old" else new_cache)
        for r, location in source_rows()
        if r.get("status") == "VERIFIED"
    }
    checked = set()

    @functools.lru_cache(maxsize=8)
    def raw(archive):
        metadata, cache = by_sha[archive]
        path = cache / metadata["csv_scratch"]
        if archive not in checked:
            if common.sha(path) != metadata["csv_sha256"]:
                raise ValueError("INDEPENDENT_CSV_PROOF_MISMATCH")
            checked.add(archive)
        with path.open(newline="") as f:
            return list(csv.reader(f))

    groups = collections.defaultdict(list)
    for r in rows:
        for h in HORIZONS:
            if r[f"outcome_{h}h_status"] == "OBSERVED":
                groups[r["symbol"], h].append(r)
    errors, cases = [], []
    for (symbol, h), items in sorted(groups.items()):
        for index in sorted({0, len(items) // 2, len(items) - 1}):
            r = items[index]
            first_archive, first_index = r["trigger_source"].split(":")
            last_archive, last_index = r[f"endpoint_{h}h_source"].split(":")
            first_index, last_index = int(first_index), int(last_index)
            if first_archive == last_archive:
                refs = [(first_archive, i) for i in range(first_index, last_index + 1)]
            else:
                refs = [(first_archive, i) for i in range(first_index, len(raw(first_archive)))]
                refs += [(last_archive, i) for i in range(last_index + 1) if raw(last_archive)[i][0].isdigit()]
            if len(refs) != 2 + h * 4 or common.digest([f"{a}:{i}" for a, i in refs]) != r[f"path_{h}h_sources_sha256"]:
                raise ValueError("INDEPENDENT_NATIVE_PATH_IDENTITY")
            a_ref, i_ref = r[f"reference_{h}h_source"].split(":")
            with localcontext() as context:
                context.prec = 50
                reference = Decimal(raw(a_ref)[int(i_ref)][4])
                endpoint = Decimal(raw(last_archive)[last_index][4])
                sign = 1 if r["side"] == "LONG" else -1
                expected = 10000 * sign * (endpoint / reference - 1)
                excursions = [Decimal(0)]
                previous = int(r["cutoff_ns"]) - common.M15
                for a, i in refs:
                    data = raw(a)[i]
                    end = (int(data[6]) + 1) * 1_000_000
                    if end - previous != common.M15:
                        raise ValueError("INDEPENDENT_NATIVE_PATH_CLOCK")
                    previous = end
                    if end > int(r["reference_ns"]):
                        excursions.extend(10000 * sign * (Decimal(data[j]) / reference - 1) for j in (2, 3))
                diffs = [
                    abs(float(expected) - float(r[f"gross_{h}h_bps"])),
                    abs(float(max(excursions)) - float(r[f"mfe_{h}h_bps"])),
                    abs(float(min(excursions)) - float(r[f"mae_{h}h_bps"])),
                ]
            if max(diffs) > 1e-8:
                raise ValueError("INDEPENDENT_PRICE_NUMERICAL_MISMATCH")
            errors.extend(diffs)
            cases.append({"event_id": r["event_id"], "symbol": symbol, "horizon": h, "maximum_error_bps": max(diffs)})
    return {
        "method": "Fixed first/middle/last source-backed raw CSV cases per symbol/horizon; 50-digit Decimal return/MFE/MAE; full native path hashes/clocks",
        "cases": len(cases),
        "fields_per_case": 3,
        "checked_csv_objects": len(checked),
        "maximum_absolute_error_bps": max(errors, default=0),
        "tolerance_bps": 1e-8,
        "mismatches": 0,
        "case_identities": cases,
    }


def evaluate(commit, old_cache, new_cache, outdir):
    started = time.monotonic()
    seal = verify_seal(commit)
    before = common.sha(common.HERE / "DECISIONS.csv.gz")
    calendar = calendar_check(common.HERE / "DECISIONS.csv.gz", common.HERE / "CALENDAR.csv.gz")
    rows = list(common.read_csv(common.HERE / "DECISIONS.csv.gz"))
    by_symbol = collections.defaultdict(list)
    for row in rows:
        by_symbol[row["symbol"]].append(row)
    out = []
    for symbol, events in sorted(by_symbol.items()):
        bars = {b.end: b for b in load_native(symbol, "15m", 2025, range(6, 10), old_cache, new_cache)}
        for row in events:
            for h in HORIZONS:
                result = (
                    outcome(row, bars, h)
                    if row["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"
                    else {"status": row["study_status"]}
                )
                row[f"outcome_{h}h_status"] = result["status"]
                for key in (
                    "gross_bps",
                    "mfe_bps",
                    "mae_bps",
                    "reference_close",
                    "endpoint_close",
                    "reference_source",
                    "endpoint_source",
                    "path_sources_sha256",
                ):
                    new_key = (
                        f"{key.removesuffix('_bps')}_{h}h_bps"
                        if key.endswith("_bps")
                        else key.replace("_", f"_{h}h_", 1)
                    )
                    row[new_key] = result.get(key, "")
            delayed = (
                outcome(row, bars, 4, delayed=True)
                if row["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"
                else {"status": row["study_status"]}
            )
            row["delayed_4h_status"] = delayed["status"]
            row["delayed_reference_close"] = delayed.get("reference_close", "")
            out.append(row)
    out.sort(key=lambda r: (int(r["cutoff_ns"]), common.protocol()["cohort"].index(r["symbol"])))
    outdir.mkdir(parents=True, exist_ok=True)
    common.write_csv(outdir / "OUTCOMES.csv.gz", out)
    primary = [r for r in out if r["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"]
    models = common.load(common.HERE / "PRICE_MODELS.json")
    result = {
        "grade": common.GRADE,
        "decision_commit": commit,
        "protocol_sha256": common.PROTOCOL_SHA,
        "decision_sha256": before,
        "calendar": calendar,
        "first_event_status_counts": dict(collections.Counter(r["study_status"] for r in out)),
        "original_qualified_altcoin_N": len(primary),
        "horizon_censor_counts": {
            str(h): dict(collections.Counter(r[f"outcome_{h}h_status"] for r in primary)) for h in HORIZONS
        },
        "arms": {str(h): economic_table(primary, h) for h in HORIZONS},
        "ML": prediction_metrics(primary, models),
        "cash_sensitivities": cash_sensitivities(primary),
        "mechanism_and_overlap": mechanism_and_overlap(primary),
        "independent_raw_price_checks": independent_price_checks(primary, old_cache, new_cache),
        "cross_market_slices": slices(primary, models),
        "full_programme_after_cost_cash": "NOT ESTIMABLE",
        "Track_A_action_cash_ML": "NOT ESTIMABLE",
        "qualified_agent_decisions": "UNVERIFIED / absent; deterministic controls are not Agent B",
        "outcome_ledger_sha256": common.sha(outdir / "OUTCOMES.csv.gz"),
        "seal_sha256": common.digest(seal),
        "decision_mutated": False,
    }
    if common.sha(common.HERE / "DECISIONS.csv.gz") != before:
        raise ValueError("OUTCOME_STAGE_MUTATED_SEALED_DECISIONS")
    common.save(outdir / "RESULTS.json", result)
    common.save(
        outdir / "OUTCOME_RESOURCES.json",
        {"wall_seconds": time.monotonic() - started, "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss},
    )
    return {
        "original_qualified_altcoin_N": len(primary),
        "censor_counts": result["horizon_censor_counts"],
        "outcomes_sha256": result["outcome_ledger_sha256"],
        "results_sha256": common.sha(outdir / "RESULTS.json"),
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--decision-commit", required=True)
    p.add_argument("--old-cache", type=pathlib.Path, required=True)
    p.add_argument("--new-cache", type=pathlib.Path, required=True)
    p.add_argument("--outdir", type=pathlib.Path, required=True)
    args = p.parse_args()
    print(common.canonical(evaluate(args.decision_commit, args.old_cache, args.new_cache, args.outdir)))
