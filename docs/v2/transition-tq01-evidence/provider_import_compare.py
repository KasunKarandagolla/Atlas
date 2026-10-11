from __future__ import annotations
import json
import re
import subprocess
import sys
import time

PYTHON = "/tmp/atlas-tq01-pydantic-venv/bin/python"
CASES = {
    "direct_transport_json": "import json, httpx2; print(httpx2.__version__)",
    "pydantic_ai_openai_framework": "import pydantic_ai; import pydantic_ai.models.openai; print(pydantic_ai.__version__)",
}
rows = {}
for name, code in CASES.items():
    samples = []
    for _ in range(3):
        start = time.perf_counter_ns()
        run = subprocess.run(["/usr/bin/time", "-f", "TQ01_TIME_RSS=%e,%M", PYTHON, "-c", code],
                             capture_output=True, text=True, check=True)
        elapsed = time.perf_counter_ns() - start
        match = re.search(r"TQ01_TIME_RSS=([0-9.]+),([0-9]+)", run.stderr)
        if not match:
            raise RuntimeError("GNU time measurement missing")
        samples.append({"wall_ns_parent_includes_spawn": elapsed,
                        "child_time_seconds": float(match.group(1)),
                        "child_max_rss_kib": int(match.group(2)),
                        "stdout": run.stdout.strip()})
    rows[name] = samples
result = {
    "evidence_class": "OFFLINE_COLD_PROCESS_IMPORTS_ONLY_NO_PROVIDER_CALL",
    "python": sys.version,
    "pydantic_ai_version": "2.42.0",
    "direct_transport": "httpx2==2.13.1 plus stdlib json; no request",
    "samples_per_case": 3,
    "results": rows,
}
print(json.dumps(result, sort_keys=True))
