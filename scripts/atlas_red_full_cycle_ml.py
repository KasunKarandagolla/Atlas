"""ML inventory, ML-ready tables and chronological model comparison (CLI)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.research.full_cycle.ml import (  # noqa: E402
    FEATURE_NAMES,
    TARGET_NAME,
    build_ml_rows,
    chronological_split,
    fit_and_compare,
    foundation_inventory,
)
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--model-cache", default=None)
    parser.add_argument(
        "--download", action="store_true", help="attempt the bounded checkpoint download for the selected models"
    )
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    workspace = Path(args.workspace).resolve()
    store = ResearchStoreV1.open(workspace / "data" / "research.sqlite")
    if args.download:
        parser.error("Use atlas_red_provision_models.py with separate permanent/temp roots and pinned identities")
    cache_root = Path(args.model_cache) if args.model_cache else workspace / "models" / "hf"

    inventory = foundation_inventory(cache_root, download=bool(args.download))
    now = time.time_ns()
    for entry in inventory:
        store.record_model(
            {
                "model_id": entry["model_id"] + "@" + str(now),
                "checkpoint": entry["checkpoint"] or f"in-repository:{entry['model_id']}",
                "revision": entry.get("revision"),
                "role": entry["role"],
                "weight_sha256": None,
                "tokenizer": None,
                "tokenizer_revision": None,
                "tokenizer_sha256": None,
                "disk_bytes": entry.get("detail", {}).get("identity", {}).get("total_bytes"),
                "runtime": "ATLAS_REPOSITORY" if entry["checkpoint"] is None else "HF_CHECKPOINT",
                "load_status": entry["load_status"],
                "inference_status": entry["inference_status"],
                "detail": entry.get("detail", {}),
                "updated_at_ns": now,
            }
        )

    rows = build_ml_rows(store)
    comparison = fit_and_compare(rows)
    split = chronological_split([row for row in rows if row["label_state"] == "MATURED_NET_CASH"])
    comparison["split_sizes"] = {name: len(value) for name, value in split.items() if isinstance(value, list)}
    comparison["feature_names"] = list(FEATURE_NAMES)
    comparison["target_name"] = TARGET_NAME
    comparison["label_states"] = {
        state: sum(1 for row in rows if row["label_state"] == state)
        for state in sorted({row["label_state"] for row in rows})
    }
    comparison["row_count"] = len(rows)
    comparison["ml_ready_rows_persisted"] = True
    store.set_meta("ml_comparison", json.dumps(comparison, sort_keys=True, default=str))
    store.set_meta("ml_row_count", str(len(rows)))

    if comparison.get("status") == "HISTORICAL_DIAGNOSTIC":
        store.record_provenance(
            {
                "provenance_id": f"MLP-{int(time.time())}",
                "experiment_id": "ATLAS_RED_FULL_CYCLE_CHRONOLOGICAL_ML_V1",
                "model_id": "M1_LIGHTGBM_L2_CASH_MEAN",
                "information_cutoff_ns": now,
                "split_name": "CHRONOLOGICAL_60_20_20_WITH_24H_EMBARGO",
                "feature_names": list(FEATURE_NAMES),
                "target_name": TARGET_NAME,
                "n_rows": comparison["n_rows"],
                "n_matured": comparison["n_matured"],
                "metric_name": "TEST_RMSE_BPS",
                "metric_value": str(comparison["models"].get("LIGHTGBM_L2_CASH_MEAN", {}).get("test_rmse_bps")),
                "comparator_name": "EMPIRICAL_MEAN",
                "comparator_value": str(comparison["models"].get("EMPIRICAL_MEAN", {}).get("test_rmse_bps")),
                "detail": comparison,
                "created_at_ns": now,
            }
        )

    payload = {"inventory": inventory, "comparison": comparison}
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    if args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
