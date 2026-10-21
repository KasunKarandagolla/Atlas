"""Mature existing sealed actions; refuse retrospective trigger-to-action promotion.

No action is minted from a historical price-only applicability row. Real actions
must come from the original native coordinator with full source/plan lineage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.research.full_cycle.causality import action_qualification  # noqa: E402
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402


def check_candidates(store: ResearchStoreV1) -> dict:
    counts = {"candidates": 0, "refused_reconstructed": 0, "qualified_original_actions": 0}
    for row in store.query("SELECT * FROM strategy_applicability WHERE status IN ('RESEARCH_ACTION','CANDIDATE')"):
        counts["candidates"] += 1
        identity = hashlib.sha256(f"V2-ORIGINAL-TIME-REFUSAL|{row['applicability_id']}".encode()).hexdigest()
        if not store.scalar("SELECT COUNT(*) FROM missingness WHERE missing_id=?", (identity,)):
            store.record_missingness(
                {
                    "missing_id": identity,
                    "cycle_id": row["cycle_id"],
                    "symbol": row["symbol"],
                    "scope": "RESEARCH_ACTION",
                    "reason_code": "ORIGINAL_NATIVE_ACTION_PROVENANCE_REQUIRED",
                    "detail": {
                        "applicability_id": row["applicability_id"],
                        "source_grade": row["source_grade"],
                        "note": "A later seal cannot restore an original opportunity.",
                    },
                    "recorded_at_ns": time.time_ns(),
                }
            )
        counts["refused_reconstructed"] += 1
    counts["qualified_original_actions"] = sum(
        action_qualification(a) for a in store.query("SELECT * FROM research_action")
    )
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output-summary")
    args = parser.parse_args(argv)
    store = ResearchStoreV1.open(Path(args.workspace) / "data" / "research.sqlite")
    report = {
        **check_candidates(store),
        "status": "NOT ESTIMABLE",
        "next": "Use native original-time action producer and explicit versioned lifecycle evidence.",
    }
    payload = json.dumps(report, indent=2, sort_keys=True)
    print(payload)
    if args.output_summary:
        Path(args.output_summary).write_text(payload + "\n")
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
