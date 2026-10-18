#!/usr/bin/env python3
"""Bounded local-source reuse, version packaging and old/new streaming audit."""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import itertools
import json
import pathlib
import shutil
import subprocess

import summarize_research_outputs as summary
from verify_sol_sources import verify_member

HERE = pathlib.Path(__file__).resolve().parent
VERSION = HERE / "v3-corrected"


def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def save(path, obj):
    path.write_text(json.dumps(obj, sort_keys=True, indent=2, allow_nan=False) + "\n")


def prepare(zip_cache, csv_cache, scratch):
    """No network: prove retained bytes and emit existing producer inputs."""
    scratch.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(line) for line in (HERE / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    expected = {
        r["url"]: r
        for r in map(json.loads, (HERE / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if "csv_sha256" in r
    }
    count = total = 0
    for r in rows:
        if r.get("status") != "VERIFIED":
            continue
        name = r["url"].rsplit("/", 1)[1]
        checksum = (zip_cache / (name + ".CHECKSUM")).read_text().split()
        if checksum != [r["published_sha256"], name]:
            raise ValueError("CACHED_CHECKSUM_IDENTITY")
        proof = verify_member((zip_cache / name).read_bytes(), r, (csv_cache / r["csv_scratch"]).read_bytes())
        if proof["csv_sha256"] != expected[r["url"]]["csv_sha256"]:
            raise ValueError("CACHED_CSV_PROOF_IDENTITY")
        count += 1
        total += proof["archive_bytes"]
    if (count, total) != (757, 55549936):
        raise ValueError("PINNED_SOURCE_SET_CHANGED")
    for label, predicate in (
        ("H1.jsonl", lambda r: r.get("status") in ("VERIFIED", "MISSING_OBJECT_404") and r["interval"] == "1h"),
        ("M15.jsonl", lambda r: r.get("status") in ("VERIFIED", "MISSING_OBJECT_404") and r["interval"] == "15m"),
        ("REJECTED.jsonl", lambda r: r.get("status") == "PARSER_REJECTED_PRECEDING_ATTEMPT"),
    ):
        (scratch / label).write_text(
            "".join(json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n" for r in rows if predicate(r))
        )
    result = {
        "verified_cached_zip_checksum_csv_objects": count,
        "archive_payload_bytes": total,
        "network_requests": 0,
        "archive_downloads": 0,
        "retained_historical_404_objects": 13,
        "retained_parser_rejected_attempts": 420,
        "historical_rejected_bytes": "UNKNOWN",
        "source_manifest_sha256": sha(HERE / "SOURCE_MANIFEST.jsonl"),
        "checksum_proof_sha256": sha(HERE / "SOL_SOURCE_CHECKS.jsonl"),
    }
    save(VERSION / "LOCAL_SOURCE_REUSE.json", result)
    return result


def package(builddir):
    dataset = summary.run(outdir=builddir)
    for name in ("SOURCE_MANIFEST.jsonl", "ASOF_COVERAGE.csv.gz"):
        if sha(builddir / name) != sha(HERE / name):
            raise ValueError("UNCHANGED_NATIVE_SOURCE_OR_COVERAGE_CHANGED:" + name)
    shutil.copyfile(
        builddir / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz", VERSION / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz"
    )
    dataset["producer_commit_sha"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dataset["previous_reviewed_scientific_sha"] = "c29dfc2c2ae6cb903b53f72fdf2d457c9a5bb32d"
    dataset["previous_dataset_manifest_sha256"] = sha(HERE / "DATASET_MANIFEST.json")
    dataset["new_acquisition"] = False
    dataset["source_manifest_artifact"]["path"] = "../SOURCE_MANIFEST.jsonl"
    dataset["coverage"]["artifact"]["path"] = "../ASOF_COVERAGE.csv.gz"
    save(VERSION / "DATASET_MANIFEST.json", dataset)
    names = (
        "DIAGNOSTIC_FEATURE_PANEL.csv.gz",
        "ASOF_COVERAGE.csv.gz",
        "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz",
        "SOURCE_MANIFEST.jsonl",
        "DATASET_MANIFEST.json",
    )
    paths = {
        name: (
            VERSION / name
            if name in ("DATASET_MANIFEST.json", "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz")
            else builddir / name
        )
        for name in names
    }
    config = {
        "version": "ATLAS_RED_E1B_CORRECTED_V3",
        "artifact_sha256": {n: sha(p) for n, p in paths.items()},
        "artifact_paths": {
            "SOURCE_MANIFEST.jsonl": "../SOURCE_MANIFEST.jsonl",
            "ASOF_COVERAGE.csv.gz": "../ASOF_COVERAGE.csv.gz",
        },
    }
    save(VERSION / "VALIDATION_INPUTS.json", config)
    return config


def compare(old, new):
    changes, rows, old_counts, new_counts = (collections.Counter() for _ in range(4))
    examples = []
    example_count = collections.Counter()
    event_hashes = {name: hashlib.sha256() for name in ("old", "new")}
    with gzip.open(old, "rt", newline="") as a, gzip.open(new, "rt", newline="") as b:
        for left, right in itertools.zip_longest(csv.DictReader(a), csv.DictReader(b)):
            if left is None or right is None:
                raise ValueError("ROW_DENOMINATOR_CHANGED")
            keys = ("window", "symbol", "decision_cutoff_utc_ns")
            if any(left[k] != right[k] for k in keys):
                raise ValueError("NATIVE_KEY_CHANGED")
            changed = False
            for key in left:
                if left[key] != right[key]:
                    changed = True
                    group = left["window"] + ":" + key
                    changes[group] += 1
                    if example_count[group] < 2:
                        examples.append(
                            {
                                **{k: left[k] for k in keys},
                                "field": key,
                                "old": left[key],
                                "new": right[key],
                                "source_m15_archive_sha256": right["m15_archive_sha256"],
                            }
                        )
                        example_count[group] += 1
            rows[left["window"] + ":rows"] += 1
            rows[left["window"] + ":changed_rows"] += changed
            for tag, row, counter in (("old", left, old_counts), ("new", right, new_counts)):
                for field in ("s1_first_trigger_event", "s1_setup_event", "s1_raw_trigger_predicate"):
                    counter[row["window"] + ":" + field] += int(row[field])
                if row["s1_first_trigger_event"] == "1":
                    event_hashes[tag].update((row["s1_first_trigger_event_id"] + "\n").encode())
    result = {
        "rows": dict(rows),
        "field_changes": dict(sorted(changes.items())),
        "old_counts": dict(old_counts),
        "new_counts": dict(new_counts),
        "first_event_ordered_hashes": {k: v.hexdigest() for k, v in event_hashes.items()},
        "bounded_examples": examples,
        "old_panel_sha256": sha(old),
        "new_panel_sha256": sha(new),
    }
    save(VERSION / "OLD_NEW_DIFF.json", result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    a = sub.add_parser("prepare")
    for flag in ("zip-cache", "csv-cache", "scratch"):
        a.add_argument("--" + flag, type=pathlib.Path, required=True)
    a = sub.add_parser("package")
    a.add_argument("--builddir", type=pathlib.Path, required=True)
    a = sub.add_parser("compare")
    a.add_argument("--old", type=pathlib.Path, required=True)
    a.add_argument("--new", type=pathlib.Path, required=True)
    args = p.parse_args()
    if args.command == "prepare":
        result = prepare(args.zip_cache, args.csv_cache, args.scratch)
    elif args.command == "package":
        result = package(args.builddir)
    else:
        result = compare(args.old, args.new)
    print(json.dumps(result, sort_keys=True))
