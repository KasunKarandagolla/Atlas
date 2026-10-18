#!/usr/bin/env python3
"""One preregistered S1×S6 historical price study, with zero execution authority."""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import io
import json
import math
import pathlib
import subprocess
from datetime import UTC, datetime, timedelta

import independent_sol_reproduction as oracle
import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
VERSION = HERE / "v3-corrected"
GRADE = "HISTORICAL_PRICE_PROXY_NONEXECUTABLE"
POLICIES = ("P0", "P1", "P2", "P3")
HORIZONS = (1, 4, 24)


def sha(path):
    return oracle.sha_file(pathlib.Path(path))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")


def write_csv(path, rows):
    if not rows:
        raise ValueError("EMPTY_RESEARCH_LEDGER")
    with (
        path.open("wb") as raw,
        gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0, compresslevel=9) as compressed,
    ):
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        writer = csv.DictWriter(text, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        text.flush()
        text.detach()


def read_csv(path):
    with gzip.open(path, "rt", newline="") as stream:
        return list(csv.DictReader(stream))


def protocol():
    value = json.loads((VERSION / "STUDY_PROTOCOL.json").read_text())
    if value["horizons_hours"] != [1, 4, 24] or list(value["policies"]) != list(POLICIES):
        raise ValueError("UNREGISTERED_STUDY_VARIANT")
    return value


def window_end(window):
    if window not in ("A", "B"):
        raise ValueError("UNKNOWN_WINDOW")
    return oracle.timestamp(2024 if window == "A" else 2025, 7, 1)


def reference_time(cutoff, lag):
    if lag not in (5, 30, 60) or cutoff % oracle.M15:
        raise ValueError("INVALID_REPLAY_OR_NATIVE_CLOCK")
    ready = oracle.replay_readiness([cutoff], lag)
    return (ready // oracle.M15 + 1) * oracle.M15


def policy_status(event, policy):
    if policy not in POLICIES:
        raise ValueError("UNREGISTERED_POLICY")
    if event["study_status"] != "SOURCE_QUALIFIED_ALT_EVENT":
        return "UNAVAILABLE"
    if policy == "P0":
        return "ACCEPTED"
    if policy == "P3":
        return "REJECTED_NO_OPENING_REFERENCE"
    if (
        event["s6_state"] != "S6_FIXED_COHORT_RANK_DIAGNOSTIC"
        or event["s6_decile"] not in ("TOP", "MIDDLE", "BOTTOM")
        or not event["s6_rank"]
    ):
        return "UNAVAILABLE"
    aligned = (event["side"] == "LONG" and event["s6_decile"] == "TOP") or (
        event["side"] == "SHORT" and event["s6_decile"] == "BOTTOM"
    )
    opposed = (event["side"] == "LONG" and event["s6_decile"] == "BOTTOM") or (
        event["side"] == "SHORT" and event["s6_decile"] == "TOP"
    )
    accept = aligned if policy == "P1" else not opposed
    return "ACCEPTED" if accept else "REJECTED"


def extract(panel, validation, destination):
    protocol()
    verification = json.loads(validation.read_text())
    for label in ("A", "B"):
        for name in ("s1_primary", "s6", "anchor_context", "physical_h1_source", "physical_m15_source"):
            if verification["comparisons"].get("checked_rows", {}).get(name + ":" + label) != 409920:
                raise ValueError("CHECKPOINT_A_INCOMPLETE_COMPARISON")
    for group, count in verification["comparisons"]["rows_with_mismatch"].items():
        if count and not group.startswith("s1_observed:"):
            raise ValueError("CHECKPOINT_A_NOT_QUALIFIED:" + group)
    if verification["pinned_artifacts"][panel.name] != sha(panel):
        raise ValueError("VALIDATED_PANEL_IDENTITY")
    rows, seen = [], set()
    counts = collections.Counter()
    with gzip.open(panel, "rt", newline="") as stream:
        for row in csv.DictReader(stream):
            if row["s1_first_trigger_event"] != "1":
                continue
            event_id = row["s1_first_trigger_event_id"]
            if event_id in seen:
                raise ValueError("DUPLICATE_FIRST_EVENT_WEIGHT")
            seen.add(event_id)
            cutoff = int(row["decision_cutoff_utc_ns"])
            if row["symbol"] in ("BTCUSDT", "ETHUSDT"):
                status = "ANCHOR_CONTEXT_EXCLUDED"
            elif (
                row["source_status"] != "OBSERVED"
                or row["h1_source_status"] != "OBSERVED"
                or row["s1_precursor_state"] not in ("SETUP", "NO_SETUP")
                or row["s1_trigger_episode_status"] != "VERIFIED_FIRST_TRIGGER"
                or row["s1_trigger_window_active"] != "1"
            ):
                status = "UNVERIFIED_OR_INCOMPLETE_S1_SOURCE"
            else:
                status = "SOURCE_QUALIFIED_ALT_EVENT"
            event = {
                "window": row["window"],
                "symbol": row["symbol"],
                "event_id": event_id,
                "setup_id": row["s1_trigger_setup_id"],
                "cutoff_ns": cutoff,
                "setup_cutoff_ns": row["s1_trigger_setup_cutoff_utc_ns"],
                "side": row["s1_trigger_setup_side"],
                "setup_ref": row["s1_trigger_setup_ref"],
                "setup_extreme": row["s1_trigger_setup_extreme"],
                "study_status": status,
                "s1_precursor_state": row["s1_precursor_state"],
                "s1_regime": row["h4_trend"],
                "s6_state": row["s6_state"],
                "s6_score": row["s6_score"],
                "s6_rank": row["s6_rank"],
                "s6_decile": row["s6_decile"],
                "s6_exclusion_reason": row["s6_exclusion_reason"],
                "btc_h1_return": row["btc_h1_return"],
                "eth_h1_return": row["eth_h1_return"],
                "btc_h4_trend": row["btc_h4_trend"],
                "eth_h4_trend": row["eth_h4_trend"],
                "source_m15_archive_sha256": row["m15_archive_sha256"],
                "source_m15_row": row["m15_row_index"],
                "source_h1_archive_sha256": row["h1_archive_sha256"],
                "source_h1_row": row["h1_row_index"],
                "actual_receipt_status": row["actual_receipt_status"],
                "replay_ready_5_ns": cutoff + 6 * oracle.SECOND,
                "replay_ready_30_ns": cutoff + 31 * oracle.SECOND,
                "replay_ready_60_ns": cutoff + 61 * oracle.SECOND,
                "same_deadline_executable": "NO",
                "reference_ns": reference_time(cutoff, 5),
                "evidence_grade": GRADE,
            }
            if len({reference_time(cutoff, lag) for lag in (5, 30, 60)}) != 1:
                raise ValueError("TIMING_SENSITIVITY_REFERENCE_CHANGED")
            counts[row["window"] + ":" + status] += 1
            rows.append(event)
    write_csv(destination, rows)
    result = {
        "evidence_grade": GRADE,
        "feature_population_sha256": sha(destination),
        "panel_sha256": sha(panel),
        "validation_sha256": sha(validation),
        "protocol_sha256": sha(VERSION / "STUDY_PROTOCOL.json"),
        "counts": dict(sorted(counts.items())),
        "unique_first_events": len(rows),
        "duplicate_event_weights": 0,
        "outcomes_computed": False,
        "historical_continuous_live_ema_state": "UNVERIFIED; explicit finite research origins only",
    }
    save(destination.parent / "POPULATION_FREEZE.json", result)
    return result


def price_outcome(event, bars, horizon, lag=5):
    if horizon not in HORIZONS:
        raise ValueError("UNREGISTERED_HORIZON")
    cutoff = int(event["cutoff_ns"])
    ref = reference_time(cutoff, lag)
    end = ref + horizon * oracle.HOUR
    final = window_end(event["window"])
    if ref >= final or end + (lag + 1) * oracle.SECOND >= final:
        return {"status": "CENSORED_WINDOW_END"}
    start = bars.get(ref)
    if cutoff not in bars or start is None:
        return {"status": "CENSORED_REFERENCE_MISSING"}
    path = [bars.get(t) for t in range(ref, end + oracle.M15, oracle.M15)]
    if any(b is None for b in path):
        return {"status": "CENSORED_NATIVE_OUTCOME_GAP"}
    side = 1 if event["side"] == "LONG" else -1 if event["side"] == "SHORT" else 0
    if not side:
        raise ValueError("INVALID_S1_SIDE")
    price = start.close
    gross = 10000 * side * (path[-1].close / price - 1)
    signed_extremes = [10000 * side * (float(b.prices[j]) / price - 1) for b in path[1:] for j in (1, 2)]
    return {
        "status": "OBSERVED",
        "gross_bps": gross,
        "mfe_bps": max(0.0, *signed_extremes),
        "mae_bps": min(0.0, *signed_extremes),
        "positive_friction_budget_bps": gross if gross > 0 else "",
        "reference_price": start.prices[3],
        "reference_source": start.source,
        "endpoint_ns": end,
        "endpoint_source": path[-1].source,
        "future_source_refs_sha256": hashlib.sha256("|".join(b.source for b in path).encode()).hexdigest(),
        "outcome_zero_volume_bars": sum(float(b.prices[4]) == 0 for b in path[1:]),
    }


def monday(ns):
    day = datetime.fromtimestamp(int(ns) // oracle.SECOND, UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return (day - timedelta(days=day.weekday())).date().isoformat()


def all_weeks(window):
    first = oracle.timestamp(2024 if window == "A" else 2025, 3, 1)
    end = window_end(window)
    return sorted({monday(t) for t in range(first, end, 24 * oracle.HOUR)})


def accepted(row, policy):
    return row[policy + "_status"] == "ACCEPTED"


def mean(values):
    return math.fsum(values) / len(values) if values else None


def quantiles(values):
    return (
        dict(
            zip(
                ("q01", "q05", "q50", "q95", "q99"),
                map(float, np.quantile(values, [0.01, 0.05, 0.5, 0.95, 0.99])),
                strict=True,
            )
        )
        if values
        else {}
    )


def paired_blocks(rows, window, horizon, protocol_value):
    weeks = all_weeks(window)
    groups = {w: [] for w in weeks}
    for row in rows:
        groups[monday(row["cutoff_ns"])].append(row)
    out = {}
    for size in (1, 2):
        sums = []
        counts = []
        for i in range(0, len(weeks), size):
            block = [r for w in weeks[i : i + size] for r in groups[w]]
            counts.append(len(block))
            sums.append(
                [math.fsum(float(r[f"proxy_{horizon}h_bps"]) * accepted(r, p) for r in block) for p in POLICIES]
            )
        sums = np.array(sums, dtype=float)
        counts = np.array(counts, dtype=float)
        rng = np.random.default_rng(
            protocol_value["uncertainty"]["rng_seed"] + horizon + (0 if window == "A" else 1000) + size
        )
        draws = rng.integers(0, len(counts), size=(10000, len(counts)))
        denominators = counts[draws].sum(axis=1)
        valid = denominators > 0
        sampled = sums[draws].sum(axis=1)[valid] / denominators[valid, None]

        def interval(values):
            return [float(v) for v in np.quantile(values, [0.025, 0.975])] if len(values) else None

        out[str(size) + "_calendar_week_blocks"] = {
            "calendar_blocks": len(counts),
            "nonempty_blocks": int(np.count_nonzero(counts)),
            "resamples": 10000,
            "nonempty_resamples": int(valid.sum()),
            "below_20_block_diagnostic_floor": bool(np.count_nonzero(counts) < 20),
            "exploratory_intervals_per_original_common_event_bps": {
                p: interval(sampled[:, i]) for i, p in enumerate(POLICIES)
            },
            "paired_delta_vs_P0_intervals_bps": {
                p: interval(sampled[:, i] - sampled[:, 0]) for i, p in enumerate(POLICIES)
            },
            "claim": "exploratory development uncertainty; no independent-event CI, significance or profitability inference",
        }
    return out


def mechanism(rows):
    """Fixed-arm decomposition only; no new selection policy or search."""
    n = len(rows)
    if not n:
        return {}
    y = np.array([float(r["proxy_4h_bps"]) for r in rows])
    baseline = float(y.mean())
    out = {}
    for p in ("P1", "P2"):
        mask = np.array([accepted(r, p) for r in rows])
        arm = float(np.sum(y * mask) / n)
        retained = float(mask.mean())
        item = {
            "retained_fraction": retained,
            "mean_per_original_common_event_bps": arm,
            "delta_vs_P0_bps": arm - baseline,
            "count_only_random_thinning_expectation_bps": retained * baseline,
            "extra_vs_count_only_expectation_bps": arm - retained * baseline,
            "retained_mean_bps": mean(list(y[mask])),
            "removed_mean_bps": mean(list(y[~mask])),
            "foregone_positive_movement_per_original_bps": float(np.maximum(y[~mask], 0).sum() / n),
            "avoided_negative_movement_per_original_bps": float(-np.minimum(y[~mask], 0).sum() / n),
        }
        for name, dimensions in (
            ("side", ("side",)),
            ("side_and_BTC_regime", ("side", "btc_h4_trend")),
            ("side_and_ETH_regime", ("side", "eth_h4_trend")),
        ):
            strata = collections.defaultdict(list)
            for i, r in enumerate(rows):
                strata[tuple(r[d] for d in dimensions)].append(i)
            expectation = 0.0
            for indices in strata.values():
                expectation += int(mask[indices].sum()) / n * float(y[indices].mean())
            item[name + "_count_matched_expectation_bps"] = expectation
            item[name + "_extra_vs_expectation_bps"] = arm - expectation
        for dimension in ("symbol", "week"):
            labels = np.array([r["symbol"] if dimension == "symbol" else monday(r["cutoff_ns"]) for r in rows])
            shares = collections.Counter(labels[mask])
            total = sum(shares.values())
            deltas = []
            contributions = {}
            for label in sorted(set(labels)):
                keep = labels != label
                if keep.any():
                    deltas.append(float(np.mean((y * mask - y)[keep])))
                contributions[str(label)] = float(np.sum((y * mask - y)[labels == label]) / n)
            item[dimension + "_concentration"] = {
                "max_accepted_share": max(shares.values()) / total if total else None,
                "accepted_HHI": sum((v / total) ** 2 for v in shares.values()) if total else None,
                "leave_one_out_delta_vs_P0_min_max_bps": [min(deltas), max(deltas)] if deltas else None,
                "delta_contributions_bps": contributions,
            }
        drop = max(1, math.ceil(n * 0.01))
        order = np.argsort(np.abs(y), kind="stable")
        keep = order[:-drop]
        item["top_1pct_absolute_proxy_sensitivity"] = {
            "removed_events": drop,
            "P0_mean_after_removal_bps": float(y[keep].mean()) if len(keep) else None,
            "delta_vs_P0_after_removal_bps": float(np.mean((y * mask - y)[keep])) if len(keep) else None,
            "not_used_for_selection": True,
        }
        out[p] = item
    return out


def overlap(rows, policy):
    clocks = []
    for row in rows:
        if accepted(row, policy):
            ref = int(row["reference_ns"])
            clocks.extend(((ref, 1), (ref + 4 * oracle.HOUR, -1)))
    current = peak = 0
    for _, delta in sorted(clocks):
        current += delta
        peak = max(peak, current)
    return {
        "accepted_opportunity_hours": len(clocks) // 2 * 4,
        "maximum_overlapping_reference_horizons": peak,
        "not_portfolio_exposure_or_capital": True,
    }


def summarize(ledger, window, config):
    original = [r for r in ledger if r["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"]
    report = {
        "window": window,
        "evidence_grade": GRADE,
        "original_source_qualified_events": len(original),
        "all_first_event_status_counts": dict(collections.Counter(r["study_status"] for r in ledger)),
        "horizons": {},
        "protocol_sha256": sha(VERSION / "STUDY_PROTOCOL.json"),
        "timing_sensitivity": {
            "lags_seconds": [5, 30, 60],
            "derived_offsets_seconds": [6, 31, 61],
            "same_reference_for_every_original_event": True,
            "nonexecuted": True,
            "same_deadline_qualified_events": 0,
        },
    }
    for h in HORIZONS:
        common = [r for r in original if r[f"outcome_{h}h_status"] == "OBSERVED"]
        stats = {
            "original_N": len(original),
            "common_complete_original_C": len(common),
            "censored_original_N_minus_C": len(original) - len(common),
            "outcome_status_counts": dict(collections.Counter(r[f"outcome_{h}h_status"] for r in original)),
            "arms": {},
        }
        for p in POLICIES:
            decisions = collections.Counter(r[p + "_status"] for r in original)
            chosen = [r for r in common if accepted(r, p)]
            accepted_censored = sum(accepted(r, p) and r[f"outcome_{h}h_status"] != "OBSERVED" for r in original)
            vals = [float(r[f"proxy_{h}h_bps"]) for r in chosen]
            per_common = math.fsum(vals) / len(common) if common else None
            per_N = math.fsum(vals) / len(original) if original and not accepted_censored else None
            stats["arms"][p] = {
                "decision_counts_on_N": dict(decisions),
                "accepted_matured": len(chosen),
                "accepted_censored": accepted_censored,
                "mean_bps_per_original_N": per_N,
                "N_mean_status": "NOT_ESTIMABLE_CENSORED_ACCEPTED"
                if accepted_censored
                else "OBSERVED_PRICE_PROXY_ONLY",
                "mean_bps_per_common_complete_original_C": per_common,
                "mean_bps_per_observed_accepted_secondary": mean(vals),
                "accepted_proxy_quantiles_bps": quantiles(vals),
                "mean_adverse_excursion_bps": mean([float(r[f"mae_{h}h_bps"]) for r in chosen]),
                "mean_favourable_excursion_bps": mean([float(r[f"mfe_{h}h_bps"]) for r in chosen]),
                "fraction_observed_accepted_with_positive_friction_budget": sum(v > 0 for v in vals) / len(vals)
                if vals
                else None,
                "mean_accepted_all_in_friction_break_even_bps_if_positive": mean(vals)
                if vals and mean(vals) > 0
                else None,
            }
        for p in POLICIES:
            value = stats["arms"][p]["mean_bps_per_common_complete_original_C"]
            base = stats["arms"]["P0"]["mean_bps_per_common_complete_original_C"]
            stats["arms"][p]["delta_vs_P0_common_bps"] = (
                value - base if value is not None and base is not None else None
            )
            stats["arms"][p]["delta_vs_P3_common_bps"] = value
        stats["paired_calendar_block_uncertainty"] = paired_blocks(common, window, h, config)
        report["horizons"][str(h)] = stats
        if h == 4:
            report["mechanism_checks_4h"] = mechanism(common)
            report["overlap_4h"] = {p: overlap(common, p) for p in POLICIES}
            distributions = {}
            for dimension in (
                "side",
                "symbol",
                "week",
                "s1_regime",
                "btc_h4_trend",
                "eth_h4_trend",
                "input_zero_volume",
            ):
                groups = collections.defaultdict(list)
                for row in common:
                    groups[monday(row["cutoff_ns"]) if dimension == "week" else row[dimension]].append(row)
                distributions[dimension] = {}
                for key, group in sorted(groups.items()):
                    distributions[dimension][str(key)] = {
                        "original_common_events": len(group),
                        "arms": {
                            p: {
                                "accepted": sum(accepted(r, p) for r in group),
                                "mean_bps_per_group_original": math.fsum(
                                    float(r["proxy_4h_bps"]) * accepted(r, p) for r in group
                                )
                                / len(group),
                                "mean_bps_per_accepted": mean(
                                    [float(r["proxy_4h_bps"]) for r in group if accepted(r, p)]
                                ),
                            }
                            for p in POLICIES
                        },
                    }
            report["primary_4h_distributions"] = distributions
    return report


def choose_A(result, config):
    if result["window"] != "A":
        raise ValueError("RULE_SELECTION_IS_A_ONLY")
    values = {
        p: result["horizons"]["4"]["arms"][p]["mean_bps_per_common_complete_original_C"] for p in ("P0", "P1", "P2")
    }
    if any(v is None or not math.isfinite(v) for v in values.values()):
        raise ValueError("NO_COMPLETE_A_SELECTION_POPULATION")
    best = max(values.values())
    chosen = next(p for p in config["A_selection"]["tie_priority"] if best - values[p] <= 1e-12)
    return {
        "selected_rule": chosen,
        "A_objective_values_bps": values,
        "selection_window": "A",
        "B_used": False,
        "objective": "mean 4h proxy per common-matured original A event",
        "evidence_grade": GRADE,
    }


def run_outcomes(features, cache, window, outdir, selection_file=None, selection_sha=None):
    config = protocol()
    if window == "B":
        if not selection_file or not selection_sha or sha(selection_file) != selection_sha:
            raise ValueError("B_REQUIRES_EXACT_FROZEN_A_SELECTION")
        selection = json.loads(selection_file.read_text())
        if (
            selection["B_used"]
            or selection["selection_window"] != "A"
            or selection["protocol_sha256"] != sha(VERSION / "STUDY_PROTOCOL.json")
        ):
            raise ValueError("INVALID_A_SELECTION_PROVENANCE")
        if selection["feature_population_sha256"] != sha(features):
            raise ValueError("FEATURE_POPULATION_CHANGED_AFTER_A_SELECTION")
        relative = selection_file.resolve().relative_to(HERE.parents[2])
        committed = subprocess.check_output(["git", "show", "HEAD:" + str(relative)])
        if hashlib.sha256(committed).hexdigest() != selection_sha:
            raise ValueError("A_SELECTION_NOT_IMMUTABLY_COMMITTED_BEFORE_B")
    elif window != "A":
        raise ValueError("UNKNOWN_WINDOW")
    population = json.loads((features.parent / "POPULATION_FREEZE.json").read_text())
    if population["feature_population_sha256"] != sha(features) or population["protocol_sha256"] != sha(
        VERSION / "STUDY_PROTOCOL.json"
    ):
        raise ValueError("FROZEN_FEATURE_POPULATION_OR_PROTOCOL_CHANGED")
    source = [json.loads(line) for line in (HERE / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    proofs = {
        r["url"]: r
        for r in map(json.loads, (HERE / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if "csv_sha256" in r
    }
    events = [r for r in read_csv(features) if r["window"] == window]
    if len({r["event_id"] for r in events}) != len(events):
        raise ValueError("DUPLICATE_EVENT_WEIGHT")
    by_symbol = collections.defaultdict(list)
    for row in events:
        by_symbol[row["symbol"]].append(row)
    ledger = []
    for symbol, rows in sorted(by_symbol.items()):
        bars = (
            {b.end: b for b in oracle.load_bars(symbol, "15m", source, cache, proofs)}
            if symbol not in ("BTCUSDT", "ETHUSDT")
            else {}
        )
        for event in rows:
            row = dict(event)
            for p in POLICIES:
                row[p + "_status"] = policy_status(event, p)
            current = bars.get(int(event["cutoff_ns"]))
            row["input_zero_volume"] = (
                "YES" if current and float(current.prices[4]) == 0 else "NO" if current else "UNAVAILABLE"
            )
            for h in HORIZONS:
                result = (
                    price_outcome(event, bars, h)
                    if event["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"
                    else {"status": event["study_status"]}
                )
                row[f"outcome_{h}h_status"] = result["status"]
                for name, key in (
                    ("proxy", "gross_bps"),
                    ("mfe", "mfe_bps"),
                    ("mae", "mae_bps"),
                    ("positive_friction_budget", "positive_friction_budget_bps"),
                ):
                    row[f"{name}_{h}h_bps"] = result.get(key, "")
                for key in (
                    "reference_price",
                    "reference_source",
                    "endpoint_ns",
                    "endpoint_source",
                    "future_source_refs_sha256",
                    "outcome_zero_volume_bars",
                ):
                    row[f"{h}h_{key}"] = result.get(key, "")
            ledger.append(row)
    ledger.sort(key=lambda r: (int(r["cutoff_ns"]), r["symbol"]))
    outdir.mkdir(parents=True, exist_ok=True)
    ledger_path = outdir / (window + "_PRICE_PROXIES.csv.gz")
    write_csv(ledger_path, ledger)
    report = summarize(ledger, window, config)
    report.update(
        {
            "feature_population_sha256": sha(features),
            "price_proxy_ledger_sha256": sha(ledger_path),
            "source_manifest_sha256": sha(HERE / "SOURCE_MANIFEST.jsonl"),
            "source_checksum_proof_sha256": sha(HERE / "SOL_SOURCE_CHECKS.jsonl"),
            "study_code_sha256": sha(pathlib.Path(__file__)),
            "economic_status": "NOT ESTIMABLE; price proxies are not cash/PnL/actions/fills",
        }
    )
    if window == "A":
        selection = choose_A(report, config)
        selection.update(
            {
                "protocol_sha256": sha(VERSION / "STUDY_PROTOCOL.json"),
                "feature_population_sha256": sha(features),
                "A_price_proxy_ledger_sha256": sha(ledger_path),
                "study_code_sha256": sha(pathlib.Path(__file__)),
            }
        )
        save(outdir / "A_RULE_SELECTION.json", selection)
    else:
        report["frozen_A_selection"] = {
            "selected_rule": selection["selected_rule"],
            "selection_sha256": selection_sha,
            "B_reselection": False,
        }
    save(outdir / (window + "_STUDY_RESULTS.json"), report)
    return {
        "window": window,
        "original_source_qualified_events": report["original_source_qualified_events"],
        "primary_4h": report["horizons"]["4"],
        "ledger_sha256": sha(ledger_path),
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    a = sub.add_parser("extract")
    for flag in ("panel", "validation", "destination"):
        a.add_argument("--" + flag, type=pathlib.Path, required=True)
    a = sub.add_parser("outcomes")
    for flag in ("features", "csv-cache", "outdir"):
        a.add_argument("--" + flag, type=pathlib.Path, required=True)
    a.add_argument("--window", choices=("A", "B"), required=True)
    a.add_argument("--selection-file", type=pathlib.Path)
    a.add_argument("--selection-sha")
    args = p.parse_args()
    if args.command == "extract":
        result = extract(args.panel, args.validation, args.destination)
    else:
        result = run_outcomes(
            args.features, args.csv_cache, args.window, args.outdir, args.selection_file, args.selection_sha
        )
    print(json.dumps(result, sort_keys=True, allow_nan=False))
