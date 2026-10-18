"""Small offline evidence inventory; no market, provider, execution or outcome API.

Only named research databases' artifact-type counts are read. No account tables,
raw account logs, secrets or private exchange evidence are accessed.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = (
    "docs/research/atlas-red-e1b/SOURCE_MANIFEST.jsonl",
    "docs/research/atlas-red-e1b/SOL_SOURCE_CHECKS.jsonl",
    "docs/research/atlas-red-historical-blind-ml/RESULTS.json",
    "docs/research/atlas-red-historical-blind-ml/PROTOCOL.json",
    "docs/research/atlas-red-historical-blind-ml/DECISION_SEAL.json",
    "docs/research/atlas-red-historical-blind-ml/SOURCE_OBJECTS.jsonl",
    "docs/research/atlas-red-causal-replay/SOURCE_READINESS.json",
    "docs/research/atlas-red-causal-replay/evidence/TRACE_SEAL.json",
    "docs/research/atlas-red-causal-replay/evidence/TRACE_AUDIT.json",
    "docs/research/atlas-red-causal-replay/evidence/PARENT_COMPARISON.json",
    "docs/v2/transition-r9-local-evidence/api-qualification.json",
    "docs/v2/transition-r9-local-evidence/storage-preflight.json",
    "docs/v2/transition-r9-local-evidence/connected-fixture.json",
)
OPS_NAMES = (
    "causal-qualification-pinned.sqlite", "causal-qualification-repeat.sqlite",
    "native-trace-pinned.sqlite", "native-trace-pinned-repeat.sqlite",
    "blind-ml-origin-ops.sqlite", "blind-ml-repeat-origin-ops.sqlite",
)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def inspect(ops_root=None):
    hashes = {name: digest(ROOT / name) for name in EVIDENCE}
    history = json.loads((ROOT / EVIDENCE[2]).read_text())
    audit = json.loads((ROOT / "docs/research/atlas-red-causal-replay/evidence/TRACE_AUDIT.json").read_text())
    public = json.loads((ROOT / "docs/v2/transition-r9-local-evidence/api-qualification.json").read_text())
    databases = []
    if ops_root is not None:
        for name in OPS_NAMES:
            path = ops_root / name
            if not path.exists():
                databases.append({"name": name, "status": "LOCAL_EVIDENCE_NOT_PRESENT"})
                continue
            with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
                counts = dict(con.execute("SELECT artifact_type,count(*) FROM artifact_index GROUP BY artifact_type"))
            databases.append({"name": name, "status": "READ_ONLY_TYPE_COUNT_QUERY", "database_sha256": digest(path),
                              "artifact_counts": counts})
    return {"version": "ECONOMIC_FEASIBILITY_OFFLINE_INVENTORY_V1", "authority": "ZERO",
            "inspected_evidence_sha256": hashes, "local_research_databases": databases,
            "existing_historical_grade": history["grade"],
            "existing_historical_programme_cash": history["full_programme_after_cost_cash"],
            "existing_qualified_actions": audit["qualified_actions"],
            "existing_orders": audit["orders"], "existing_agent_calls": audit["agent_calls"],
            "existing_source_states": audit["source_objects"],
            "r9_public_rest_raw_retained": public["binance_production_public_rest"]["raw_payload_saved"],
            "r9_public_ws_raw_retained": public["binance_production_public_websocket"]["raw_frames_saved"],
            "packages": {name: importlib.util.find_spec(name) is not None for name in
                         ("pyarrow", "nautilus_trader", "lightgbm", "torch", "transformers")},
            "no_execution_or_new_outcomes": True, "market_requests": 0,
            "absence_scope": "Named verified caches, selected local research Ops stores and public repository evidence; not exchange-wide or private-source absence"}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ops-root", type=Path)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.write_text(json.dumps(inspect(args.ops_root), sort_keys=True, indent=2) + "\n")
