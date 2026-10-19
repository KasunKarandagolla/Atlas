"""Low-overhead attribution for exact artifact-index reference reads."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path


def deny_network_and_children(event: str, _args: object) -> None:
    if event in {
        "socket.connect", "socket.getaddrinfo", "socket.gethostbyname",
        "socket.sendto", "subprocess.Popen", "os.system", "os.exec",
        "os.posix_spawn",
    }:
        raise PermissionError("G1B_OFFLINE_ONLY")


for key in list(os.environ):
    if any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")):
        os.environ.pop(key)
sys.addaudithook(deny_network_and_children)

parser = argparse.ArgumentParser()
parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[4])
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--per-venue", type=int, default=2048)
parser.add_argument("--source-checkpoint", required=True)
parser.add_argument("--source-diff-sha256")
args = parser.parse_args()
repo = args.repo_root.resolve()
sys.path[:0] = [str(repo), str(repo / "src"), str(repo / "tests")]

from v2 import test_session041_scale_review as fixture  # noqa: E402

from atlas.v2.memory.repository import OpsRepository  # noqa: E402

calls = []
original = OpsRepository.get_artifact_metadata_by_refs


def wrapped(self, refs):
    caller = sys._getframe(1).f_code
    started = time.perf_counter_ns()
    result = original(self, refs)
    elapsed = time.perf_counter_ns() - started
    types = Counter(value.get("artifact_type") for value in result.values())
    calls.append({
        "caller": caller.co_name, "file": caller.co_filename, "line": caller.co_firstlineno,
        "requested": len(refs), "found": len(result), "found_types": dict(types),
        "elapsed_ns": elapsed,
    })
    return result


OpsRepository.get_artifact_metadata_by_refs = wrapped
root = args.output.with_suffix("")
if root.exists():
    raise SystemExit(f"refusing to reuse output root {root}")
started = time.perf_counter_ns()
metrics = fixture.measure_bounded_cycles(root, per_venue=args.per_venue, cycles=1)
whole_ns = time.perf_counter_ns() - started
elapsed = sorted(item["elapsed_ns"] for item in calls)
body = {
    "mode": "B_LOOKUP_CALL_COUNTS_WITH_PER_CALL_TIMER",
    "source_checkpoint": args.source_checkpoint,
    "source_diff_sha256": args.source_diff_sha256,
    "command": "measure_bounded_cycles for one cycle with a wrapper only on get_artifact_metadata_by_refs",
    "fixture_sha256": hashlib.sha256((repo / "tests/v2/test_session041_scale_review.py").read_bytes()).hexdigest(),
    "whole_helper_elapsed_ns": whole_ns,
    "metrics": metrics,
    "lookup_call_count": len(calls),
    "lookup_calls": calls,
    "lookup_latency_ns": {
        "count": len(elapsed), "p50": int(statistics.median(elapsed)),
        "p95": elapsed[int(.95 * (len(elapsed) - 1))],
        "p99": elapsed[int(.99 * (len(elapsed) - 1))], "max": max(elapsed),
    },
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n")
shapes = Counter((x["caller"], x["line"], x["requested"], x["found"]) for x in calls)
print(json.dumps({
    "output": str(args.output), "whole_helper_seconds": round(whole_ns / 1e9, 3),
    "service_seconds": round(sum(x["adoption_seconds"] + x["workset_publication_seconds"] for x in metrics["samples"]), 3),
    "lookup_call_count": len(calls), "lookup_latency_ns": body["lookup_latency_ns"],
    "lookup_shapes": [
        {"caller": key[0], "line": key[1], "requested": key[2], "found": key[3], "calls": count}
        for key, count in shapes.most_common(20)
    ],
}, separators=(",", ":")))
