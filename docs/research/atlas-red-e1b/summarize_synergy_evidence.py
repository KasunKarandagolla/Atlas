#!/usr/bin/env python3
"""Registered count decomposition and independent exact-CSV proxy spot checks.

These are diagnostics of the fixed P0/P1/P2/P3 results, not new policies,
thresholds or a second rule selection. The A selection file is read-only.
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
from decimal import Decimal, localcontext

import numpy as np
import study_s1_s6 as study


def decomposition_intervals(rows, window):
    weeks = study.all_weeks(window)
    groups = {w: [] for w in weeks}
    for r in rows:
        groups[study.monday(r["cutoff_ns"])].append(r)
    results = {}
    for size in (1, 2):
        values = []
        for i in range(0, len(weeks), size):
            block = [r for w in weeks[i : i + size] for r in groups[w]]
            y = [float(r["proxy_4h_bps"]) for r in block]
            values.append(
                [
                    len(block),
                    math.fsum(y),
                    *[math.fsum(float(r["proxy_4h_bps"]) for r in block if study.accepted(r, p)) for p in ("P1", "P2")],
                    *[sum(study.accepted(r, p) for r in block) for p in ("P1", "P2")],
                ]
            )
        values = np.array(values, dtype=float)
        seed = study.protocol()["uncertainty"]["rng_seed"] + 4 + (0 if window == "A" else 1000) + size
        rng = np.random.default_rng(seed)
        draws = rng.integers(0, len(values), size=(10000, len(values)))
        sampled = values[draws].sum(axis=1)
        sampled = sampled[sampled[:, 0] > 0]
        base = sampled[:, 1] / sampled[:, 0]
        item = {}
        for j, p in enumerate(("P1", "P2")):
            arm = sampled[:, 2 + j] / sampled[:, 0]
            retained = sampled[:, 4 + j] / sampled[:, 0]
            raw = arm - base
            count_effect = (retained - 1) * base
            extra = arm - retained * base
            if len(raw) and float(np.max(np.abs(raw - count_effect - extra))) > 1e-10:
                raise ValueError("DECOMPOSITION_IDENTITY")
            item[p] = {
                "raw_delta_interval_bps": [float(x) for x in np.quantile(raw, [0.025, 0.975])],
                "count_reduction_component_interval_bps": [float(x) for x in np.quantile(count_effect, [0.025, 0.975])],
                "extra_vs_count_only_interval_bps": [float(x) for x in np.quantile(extra, [0.025, 0.975])],
            }
        results[str(size) + "_calendar_week_blocks"] = {
            "blocks": len(values),
            "nonempty_resamples": len(sampled),
            "arms": item,
            "interpretation": "exploratory registered fixed-arm decomposition; observational, not a causal information effect",
        }
    return results


def verify_price_sample(directory, cache):
    """First/middle/last observed outcome per symbol/window/horizon; no value selection."""
    source = [
        r
        for r in map(json.loads, (study.HERE / "SOURCE_MANIFEST.jsonl").read_text().splitlines())
        if r.get("status") == "VERIFIED"
    ]
    by_sha = {r["downloaded_sha256"]: r for r in source}
    proof = {
        r["archive_sha256"]: r
        for r in map(json.loads, (study.HERE / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if "csv_sha256" in r
    }
    checked_csv = set()

    @functools.lru_cache(maxsize=8)
    def raw_rows(archive):
        path = cache / by_sha[archive]["csv_scratch"]
        if archive not in checked_csv:
            if study.sha(path) != proof[archive]["csv_sha256"]:
                raise ValueError("RAW_CSV_SOURCE_IDENTITY")
            checked_csv.add(archive)
        with path.open(newline="") as f:
            return list(csv.reader(f))

    def close(ref):
        archive, index = ref.split(":")
        return Decimal(raw_rows(archive)[int(index)][4])

    records = []
    maximum = 0.0
    for window in ("A", "B"):
        rows = study.read_csv(directory / (window + "_PRICE_PROXIES.csv.gz"))
        groups = collections.defaultdict(list)
        for r in rows:
            for h in study.HORIZONS:
                if r[f"outcome_{h}h_status"] == "OBSERVED":
                    groups[r["symbol"], h].append(r)
        for (symbol, h), items in sorted(groups.items()):
            for index in sorted({0, len(items) // 2, len(items) - 1}):
                r = items[index]
                with localcontext() as context:
                    context.prec = 50
                    a, b = close(r[f"{h}h_reference_source"]), close(r[f"{h}h_endpoint_source"])
                    sign = 1 if r["side"] == "LONG" else -1
                    expected = Decimal(10000) * sign * (b / a - 1)
                    first_archive, first_index = r[f"{h}h_reference_source"].split(":")
                    last_archive, last_index = r[f"{h}h_endpoint_source"].split(":")
                    first_index, last_index = int(first_index), int(last_index)
                    refs = [(first_archive, first_index)]
                    if first_archive == last_archive:
                        refs += [(first_archive, i) for i in range(first_index + 1, last_index + 1)]
                    else:
                        refs += [(first_archive, i) for i in range(first_index + 1, len(raw_rows(first_archive)))]
                        refs += [
                            (last_archive, i) for i in range(last_index + 1) if raw_rows(last_archive)[i][0].isdigit()
                        ]
                    if len(refs) != 1 + h * 4:
                        raise ValueError("RAW_NATIVE_OUTCOME_PREFIX_INCOMPLETE")
                    previous = int(r["reference_ns"])
                    extremes = [Decimal(0)]
                    for archive, native_index in refs[1:]:
                        raw = raw_rows(archive)[native_index]
                        end_ns = (int(raw[6]) + 1) * 1_000_000
                        if end_ns - previous != study.oracle.M15:
                            raise ValueError("RAW_NATIVE_OUTCOME_PREFIX_CLOCK")
                        previous = end_ns
                        extremes.extend(Decimal(10000) * sign * (Decimal(raw[j]) / a - 1) for j in (2, 3))
                    expected_refs = hashlib.sha256(
                        "|".join(f"{archive}:{i}" for archive, i in refs).encode()
                    ).hexdigest()
                    if expected_refs != r[f"{h}h_future_source_refs_sha256"]:
                        raise ValueError("RAW_OUTCOME_PATH_SOURCE_PROVENANCE")
                    errors = [
                        abs(float(expected) - float(r[f"proxy_{h}h_bps"])),
                        abs(float(max(extremes)) - float(r[f"mfe_{h}h_bps"])),
                        abs(float(min(extremes)) - float(r[f"mae_{h}h_bps"])),
                    ]
                error = max(errors)
                if error > 1e-8:
                    raise ValueError("INDEPENDENT_RAW_PRICE_PROXY_MISMATCH")
                maximum = max(maximum, error)
                records.append(
                    {
                        "window": window,
                        "symbol": symbol,
                        "horizon_hours": h,
                        "event_id": r["event_id"],
                        "reference_source": r[f"{h}h_reference_source"],
                        "endpoint_source": r[f"{h}h_endpoint_source"],
                        "reference_close": str(a),
                        "endpoint_close": str(b),
                        "absolute_error_bps": error,
                    }
                )
    return {
        "method": "exact verified CSV rows; independent 50-digit Decimal return/MFE/MAE; complete native-path identities; no outcome-value sampling",
        "numeric_fields_per_comparison": 3,
        "tolerance_absolute_bps": 1e-8,
        "comparisons": len(records),
        "checked_csv_objects": len(checked_csv),
        "max_absolute_error_bps": maximum,
        "mismatches": 0,
        "examples": records,
    }


def run(directory, cache):
    original_selection = study.sha(directory / "A_RULE_SELECTION.json")
    result = {
        "summarizer_code_sha256": study.sha(pathlib.Path(__file__)),
        "evidence_grade": study.GRADE,
        "fixed_arms_only": True,
        "A_selection_unchanged": True,
        "windows": {},
    }
    for window in ("A", "B"):
        rows = study.read_csv(directory / (window + "_PRICE_PROXIES.csv.gz"))
        qualified = [r for r in rows if r["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"]
        common = [r for r in qualified if r["outcome_4h_status"] == "OBSERVED"]
        censored = {
            str(h): dict(
                collections.Counter(
                    r["symbol"] + ":" + r[f"outcome_{h}h_status"]
                    for r in qualified
                    if r[f"outcome_{h}h_status"] != "OBSERVED"
                )
            )
            for h in study.HORIZONS
        }
        result["windows"][window] = {
            "count_decomposition_uncertainty": decomposition_intervals(common, window),
            "input_zero_volume_counts": dict(collections.Counter(r["input_zero_volume"] for r in qualified)),
            "censor_by_symbol_and_reason": censored,
            "observed_4h_paths_with_zero_volume_bars": sum(int(r["4h_outcome_zero_volume_bars"]) > 0 for r in common),
        }
    result["independent_source_price_sample"] = verify_price_sample(directory, cache)
    if study.sha(directory / "A_RULE_SELECTION.json") != original_selection:
        raise ValueError("A_SELECTION_CHANGED")
    study.save(directory / "MECHANISM_AND_PRICE_CHECKS.json", result)
    return {
        "independent_source_price_sample": {
            k: v for k, v in result["independent_source_price_sample"].items() if k != "examples"
        },
        "windows": result["windows"],
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", type=pathlib.Path, default=study.VERSION)
    p.add_argument("--csv-cache", type=pathlib.Path, required=True)
    args = p.parse_args()
    print(json.dumps(run(args.directory, args.csv_cache), sort_keys=True))
