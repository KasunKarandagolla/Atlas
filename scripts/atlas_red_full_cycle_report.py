"""Economic dashboard, evidence inventory and hypothesis register (CLI).

Every number in the report is computed from the preserved research store. A
quantity that the evidence cannot support is reported as NOT ESTIMABLE rather
than as a zero, and no profitability claim is made from selected trades.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.research.full_cycle.analysis import paired_management_comparisons  # noqa: E402
from atlas.v2.research.full_cycle.causality import action_qualification  # noqa: E402
from atlas.v2.research.full_cycle.storage import StorageGuardianV1  # noqa: E402
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402

REPORT_VERSION = "ATLAS_RED_FULL_CYCLE_ECONOMIC_REPORT_V2"


def _scalar(store: ResearchStoreV1, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    value = store.scalar(sql, parameters)
    return value


def _mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values) / Decimal(len(values))


def build_report(store: ResearchStoreV1, *, workspace: Path) -> dict[str, Any]:
    counts = store.table_counts()
    cycles = store.query("SELECT * FROM cycle ORDER BY started_at_ns")
    first_cycle = cycles[0]["started_at_ns"] if cycles else None
    last_cycle = cycles[-1]["completed_at_ns"] or cycles[-1]["started_at_ns"] if cycles else None

    population = store.query(
        "SELECT COUNT(DISTINCT symbol) AS symbols, "
        "SUM(CASE WHEN eligible=1 THEN 1 ELSE 0 END) AS eligible_rows, "
        "COUNT(DISTINCT CASE WHEN eligible=1 THEN symbol END) AS eligible_symbols "
        "FROM discovery_population"
    )
    symbols_observed = population[0]["symbols"] if population else 0
    eligible_symbols = population[0]["eligible_symbols"] if population else 0

    applicability = store.query(
        "SELECT strategy, status, reason_code, COUNT(*) AS n FROM strategy_applicability "
        "GROUP BY strategy, status, reason_code ORDER BY strategy, n DESC"
    )
    by_strategy: dict[str, dict[str, int]] = {}
    for row in applicability:
        bucket = by_strategy.setdefault(row["strategy"], {})
        bucket[f"{row['status']}:{row['reason_code']}"] = int(row["n"])
    strategy_totals = {strategy: sum(values.values()) for strategy, values in by_strategy.items()}

    actions = store.query("SELECT * FROM research_action")
    outcomes = store.query("SELECT * FROM effective_outcome")
    lifecycle = store.query("SELECT * FROM lifecycle_evidence")

    qualified_ids = {a["action_id"] for a in actions if action_qualification(a)}
    matured = [
        row
        for row in outcomes
        if row["maturity_status"] == "MATURED"
        and row.get("net_cash") is not None
        and row["action_id"] in qualified_ids
        and row.get("evidence_grade") == "MODELED_SHADOW_EXECUTION"
    ]
    censored = [row for row in outcomes if row["maturity_status"] != "MATURED"]

    management: dict[str, list[Decimal]] = {}
    for row in lifecycle:
        if row["action_id"] not in qualified_ids:
            continue
        management.setdefault(row["management_kind"], []).append(Decimal(row["net_cash"]))

    by_strategy_cash: dict[str, list[Decimal]] = {}
    hold_by_action = {row["action_id"]: row for row in lifecycle if row["management_kind"] == "HOLD"}
    for row in matured:
        hold = hold_by_action.get(row["action_id"])
        if hold is not None:
            by_strategy_cash.setdefault(row["strategy"], []).append(Decimal(hold["net_cash"]))

    observation_rows = store.query(
        "SELECT venue, source_id, endpoint, event_type, COUNT(*) AS n, "
        "SUM(payload_bytes) AS bytes, MIN(received_at_ns) AS first_ns, MAX(received_at_ns) AS last_ns "
        "FROM source_observation GROUP BY venue, source_id, endpoint, event_type "
        "ORDER BY bytes DESC"
    )
    endpoint_coverage = [
        {
            "venue": row["venue"],
            "source_id": row["source_id"],
            "endpoint": row["endpoint"],
            "event_type": row["event_type"],
            "records": int(row["n"]),
            "bytes": int(row["bytes"] or 0),
            "first_received_at_ns": row["first_ns"],
            "last_received_at_ns": row["last_ns"],
        }
        for row in observation_rows
    ]

    missing = store.query(
        "SELECT scope, reason_code, COUNT(*) AS n FROM missingness GROUP BY scope, reason_code ORDER BY n DESC"
    )

    symbols_with_actions = sorted({row["symbol"] for row in actions})
    symbols_with_matured = sorted({row["symbol"] for row in matured})

    report: dict[str, Any] = {
        "version": REPORT_VERSION,
        "whole_programme_economics": "NOT ESTIMABLE",
        "legacy_outcomes_excluded": store.scalar("SELECT COUNT(*) FROM mature_outcome"),
        "evidence_erratum": "ATLAS_RED_FULL_CYCLE_ERRATUM_V2_2026_10_21",
        "generated_at_ns": time.time_ns(),
        "collection": {
            "cycles": len(cycles),
            "first_cycle_started_at_ns": first_cycle,
            "last_cycle_completed_at_ns": last_cycle,
            "collection_window_seconds": ((last_cycle - first_cycle) / 1e9) if first_cycle and last_cycle else None,
            "distinct_symbols_observed": symbols_observed,
            "eligible_symbols": eligible_symbols,
            "discovery_population_rows": counts.get("discovery_population", 0),
            "source_observation_rows": counts.get("source_observation", 0),
            "raw_payload_bytes": int(_scalar(store, "SELECT SUM(payload_bytes) FROM source_observation") or 0),
            "bar_rows": counts.get("bar", 0),
            "endpoint_coverage": endpoint_coverage,
        },
        "applicability": {
            "by_strategy": by_strategy,
            "totals": strategy_totals,
            "total_records": counts.get("strategy_applicability", 0),
        },
        "research_actions": {
            "count": len(actions),
            "distinct_symbols": len(symbols_with_actions),
            "symbols": symbols_with_actions[:64],
            "by_strategy": dict(Counter(row["strategy"] for row in actions)),
            "hard_refusals": sum(1 for row in actions if row.get("hard_refusal_reason")),
        },
        "paired_management_diagnostics": paired_management_comparisons(
            [r for r in lifecycle if r["action_id"] in qualified_ids]
        ),
        "lifecycle": {
            "rows": len(lifecycle),
            "by_management_kind": {kind: len(values) for kind, values in management.items()},
            "mean_net_cash_by_management_kind": {
                kind: str(value) for kind, value in ((kind, _mean(values)) for kind, values in management.items())
            },
        },
        "outcomes": {
            "rows": len(outcomes),
            "matured": len(matured),
            "censored_or_pending": len(censored),
            "censoring_reasons": dict(Counter(row.get("censoring_reason") or "NONE" for row in censored)),
            "mean_net_cash_per_notional_bps": None,
            "matured_symbols": len(symbols_with_matured),
        },
        "ml": {
            "row_count": int(store.get_meta("ml_row_count") or 0),
            "comparison_status": (
                json.loads(store.get_meta("ml_comparison") or "{}").get("status")
                if store.get_meta("ml_comparison")
                else None
            ),
        },
        "missingness": [
            {"scope": row["scope"], "reason_code": row["reason_code"], "count": int(row["n"])} for row in missing
        ],
        "table_counts": counts,
    }
    if matured:
        action_by_id = {row["action_id"]: row for row in actions}
        bps_values: list[Decimal] = []
        for row in matured:
            action = action_by_id.get(row["action_id"])
            if action is None:
                continue
            notional = Decimal(action["notional_usd"])
            if notional > 0:
                bps_values.append(Decimal(row["net_cash"]) / notional * Decimal(10_000))
        report["outcomes"]["mean_net_cash_per_notional_bps"] = str(_mean(bps_values)) if bps_values else None
        report["outcomes"]["by_strategy"] = {
            strategy: {"n": len(values), "mean_net_cash": str(_mean(values))}
            for strategy, values in by_strategy_cash.items()
        }
    else:
        report["outcomes"]["whole_programme_economics"] = (
            "NOT_ESTIMABLE: no matured after-cost action outcome exists in this session"
        )
    return report


def write_storage_manifest(workspace: Path, report: dict[str, Any]) -> dict[str, Any]:
    guardian = StorageGuardianV1(workspace_root=workspace, external_baseline_bytes=0)
    manifest = guardian.manifest(extra={
        "note": "measured at report time; the external baseline is recorded separately in "
                "docs/research/atlas-red-full-cycle/STORAGE_REPORT.md",
        "research": {"table_counts": report["table_counts"]},
    })
    target = workspace / "data" / "storage_manifest.json"
    guardian.write_manifest(target, extra=manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)
    workspace = Path(args.workspace).resolve()
    store = ResearchStoreV1.open(workspace / "data" / "research.sqlite")
    report = build_report(store, workspace=workspace)
    # Permanent evidence is excluded from the temporary-only quota; use V2 layout checks.
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    print(payload)
    if args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
