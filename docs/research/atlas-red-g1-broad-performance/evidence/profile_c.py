"""Diagnostic G1-B C profile; timings are not cadence certification."""
from __future__ import annotations

import argparse
import cProfile
import json
import os
import platform
import pstats
import resource
import sys
import threading
import time
from functools import wraps
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
parser.add_argument("--cycles", type=int, default=1)
parser.add_argument("--source-checkpoint", required=True)
parser.add_argument("--source-diff-sha256")
args = parser.parse_args()
repo = args.repo_root.resolve()
sys.path[:0] = [str(repo), str(repo / "src"), str(repo / "tests")]

from v2 import test_session041_scale_review as fixture  # noqa: E402

from atlas.v2.runtime.production import ProductionOpsCyclePortV1  # noqa: E402

root = args.output.with_suffix("")
if root.exists():
    raise SystemExit(f"refusing to reuse output root {root}")
profile = cProfile.Profile()
sections: dict[str, list[int]] = {}


def instrument(name: str, function):
    @wraps(function)
    def wrapped(*call_args, **kwargs):
        started = time.perf_counter_ns()
        profile.enable()
        try:
            return function(*call_args, **kwargs)
        finally:
            profile.disable()
            sections.setdefault(name, []).append(time.perf_counter_ns() - started)
    return wrapped


fixture._records = instrument("synthetic_input_construction", fixture._records)
ProductionOpsCyclePortV1._persist_broad_public_snapshot = instrument(
    "broad_controller_adoption", ProductionOpsCyclePortV1._persist_broad_public_snapshot
)
fixture.publish_broad_workset = instrument("broad_workset_publication", fixture.publish_broad_workset)

samples: list[dict[str, int]] = []
stop = threading.Event()
pid = os.getpid()
root.mkdir(parents=True)


def read_kb(path: Path) -> dict[str, int]:
    values = {}
    try:
        for line in path.read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"VmRSS", "VmHWM", "RssAnon", "RssFile", "RssShmem"}:
                values[key] = int(value.strip().split()[0]) * 1024
    except (OSError, ValueError):
        pass
    return values


def proc_values() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"read_bytes", "write_bytes", "rchar", "wchar"}:
                values[key] = int(value.strip())
    except (OSError, ValueError):
        pass
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split()
        values["cpu_ticks"] = int(fields[13]) + int(fields[14])
    except (OSError, ValueError, IndexError):
        pass
    return values


def sampler() -> None:
    due = time.perf_counter_ns()
    while not stop.is_set():
        now = time.perf_counter_ns()
        sample = {"sample_at_ns": now, "wake_lag_ns": max(0, now - due)}
        sample.update(read_kb(Path(f"/proc/{pid}/status")))
        sample.update(proc_values())
        samples.append(sample)
        due = now + 100_000_000
        stop.wait(max(0, due - time.perf_counter_ns()) / 1e9)


thread = threading.Thread(target=sampler, name="g1b-resource-sampler", daemon=True)
rusage_start = resource.getrusage(resource.RUSAGE_SELF)
started = time.perf_counter_ns()
thread.start()
metrics = fixture.measure_bounded_cycles(root, per_venue=args.per_venue, cycles=args.cycles)
whole_ns = time.perf_counter_ns() - started
stop.set()
thread.join()
rusage_end = resource.getrusage(resource.RUSAGE_SELF)

stats = pstats.Stats(profile)
functions = []
for (filename, line, name), (primitive, total, self_s, cumulative_s, _callers) in stats.stats.items():
    if "/atlas/" not in filename and "/tests/" not in filename:
        continue
    functions.append({
        "file": filename, "line": line, "function": name,
        "primitive_calls": primitive, "calls": total,
        "self_seconds": self_s, "cumulative_seconds": cumulative_s,
    })
functions.sort(key=lambda item: item["cumulative_seconds"], reverse=True)
body = {
    "mode": "C_CPROFILE_AND_100MS_PROC_SAMPLER",
    "source_checkpoint": args.source_checkpoint,
    "source_diff_sha256": args.source_diff_sha256,
    "command": "measure_bounded_cycles with cProfile enabled only in input construction, broad adoption, and workset publication; /proc sampler runs concurrently",
    "fixture_sha256": __import__("hashlib").sha256(
        (repo / "tests/v2/test_session041_scale_review.py").read_bytes()
    ).hexdigest(),
    "whole_helper_elapsed_ns": whole_ns,
    "service_phase_instrumented_wall_samples_ns": sections,
    "original_helper_metrics": metrics,
    "profiled_function_count": len(functions),
    "functions_by_cumulative": functions,
    "resource_sampler": {
        "interval_ns": 100_000_000, "sample_count": len(samples), "samples": samples,
        "process_user_cpu_seconds": rusage_end.ru_utime - rusage_start.ru_utime,
        "process_system_cpu_seconds": rusage_end.ru_stime - rusage_start.ru_stime,
    },
    "environment": {"python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count()},
    "interpretation": "Instrumented diagnostic only; compare observer overhead with the matching low-overhead B run.",
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n")
print(json.dumps({
    "output": str(args.output), "whole_helper_seconds": round(whole_ns / 1e9, 3),
    "service_seconds": [round(s["adoption_seconds"] + s["workset_publication_seconds"], 3) for s in metrics["samples"]],
    "profiled_function_count": len(functions), "resource_samples": len(samples),
    "cpu_seconds": round(rusage_end.ru_utime - rusage_start.ru_utime + rusage_end.ru_stime - rusage_start.ru_stime, 3),
}, separators=(",", ":")))
