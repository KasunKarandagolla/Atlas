#!/usr/bin/env python3
"""Extract first UTC decision day from each window as a compact real-data sample."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import pathlib

from acquire_archives import COHORT
from build_research_panel import WINDOWS, ns


def make_sample(source: pathlib.Path, output: pathlib.Path) -> dict[str, object]:
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {label: 0 for label, *_ in WINDOWS}
    starts = {label: ns(sy, sm, sd) for label, sy, sm, sd, _, _, _ in WINDOWS}
    ends = {label: start + 86_400_000_000_000 for label, start in starts.items()}
    with source.open("rb") as raw, output.open("wb") as destination, gzip.GzipFile(filename="", mode="rb", fileobj=raw) as src_gz:
        reader = csv.DictReader(io.TextIOWrapper(src_gz, encoding="utf-8", newline=""))
        with gzip.GzipFile(filename="", mode="wb", compresslevel=9, fileobj=destination, mtime=0) as out_gz:
            writer = None
            text = io.TextIOWrapper(out_gz, encoding="utf-8", newline="")
            for row in reader:
                cutoff = int(row["decision_cutoff_utc_ns"])
                label = row["window"]
                if starts.get(label, -1) <= cutoff < ends.get(label, -1):
                    if writer is None:
                        writer = csv.DictWriter(text, fieldnames=list(row))
                        writer.writeheader()
                    writer.writerow(row)
                    counts[label] += 1
            text.flush()
            text.detach()
    if any(counts[label] != len(COHORT) * 96 for label in counts):
        raise ValueError(f"COMPACT_SAMPLE_ROW_COUNT_MISMATCH:{counts}")
    return {"path": output.name, "counts": counts, "rows": sum(counts.values()),
            "bytes": output.stat().st_size, "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    print(json.dumps(make_sample(args.input, args.output), sort_keys=True))


if __name__ == "__main__":
    main()
