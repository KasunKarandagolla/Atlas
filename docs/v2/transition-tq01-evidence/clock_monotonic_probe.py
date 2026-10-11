from __future__ import annotations

import json
import time

samples = []
for index in range(121):
    wall = time.time_ns()
    mono = time.monotonic_ns()
    samples.append({"index": index, "wall_ns": wall, "monotonic_ns": mono, "offset_ns": wall - mono})
    if index < 120:
        time.sleep(1.0)
offsets = [sample["offset_ns"] for sample in samples]
result = {
    "sample_count": len(samples),
    "nominal_interval_seconds": 1,
    "observed_wall_elapsed_ns": samples[-1]["wall_ns"] - samples[0]["wall_ns"],
    "observed_monotonic_elapsed_ns": samples[-1]["monotonic_ns"] - samples[0]["monotonic_ns"],
    "wall_minus_monotonic_offset_first_ns": offsets[0],
    "wall_minus_monotonic_offset_min_ns": min(offsets),
    "wall_minus_monotonic_offset_max_ns": max(offsets),
    "offset_range_ns": max(offsets) - min(offsets),
    "clock_source": "system CLOCK_REALTIME and CLOCK_MONOTONIC via Python time module",
    "interpretation_limit": "Local wall-to-monotonic relative offset over 120s; not an external UTC/NTP offset or clock accuracy measurement.",
    "samples": samples,
}
with open("docs/v2/transition-tq01-evidence/clock_monotonic_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, sort_keys=True, separators=(",", ":"))
    f.write("\n")
print(json.dumps({k: v for k, v in result.items() if k != "samples"}, sort_keys=True))
