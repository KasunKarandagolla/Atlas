"""Build a source-section requirement crosswalk without granting blanket passes.

This is an audit-accounting helper, not a qualification runner. Candidate paths
and tests are navigation aids only. S40 checkpoint artifacts and S41 JUnit XML
are attached as evidence references; they do not change a requirement's status.

The script is intentionally not executed as part of the S41 root integration
review. After review, invoke it from the S41 checkout to materialize the JSON.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "SESSION041_FULL_V2_REQUIREMENT_MATRIX.json"
OUTPUT = ROOT / "docs/v2/SESSION041_REQUIREMENT_ACCOUNTING_FINDINGS.json"

S40_EVIDENCE = [
    "docs/v2/SESSION040_OFFLINE_VALIDATION_V1.json",
    "docs/v2/SESSION040_MEASURED_REPRODUCTIONS_V1.json",
    "docs/v2/SESSION040_WINDOWS_NATIVE_RESILIENCE_V1.json",
    "docs/v2/SESSION040_OWNER_RUNTIME_RESILIENCE_LEDGER.json",
    "docs/v2/SESSION040_OWNER_RUNTIME_FMEA_V1.md",
    "docs/v2/SESSION040_PREFLIGHT_AND_LIVE_HEALTH_CONTRACT_V1.md",
    "docs/v2/PHASE3_ENGINEERING_GATE.json",
    "docs/v2/PHASE4_VENUE_CAPITAL_GATE.json",
    "docs/v2/PHASE5_RELEASE_GATE.json",
]

S41_XML = {
    "/tmp/atlas-s41-v1-all.xml": "original all-suite invocation; error, not qualification evidence",
    "/tmp/atlas-s41-focused-integrated.xml": "136 cases, 4 failures",
    "/tmp/atlas-s41-broad.xml": "57 cases, 1 failure",
    "/tmp/atlas-s41-production-breadth.xml": "8 cases, 1 failure",
    "/tmp/atlas-s41-publication.xml": "2 cases, 1 failure",
    "/tmp/atlas-s41-selected-persistence.xml": "72 cases, 1 failure",
    "/tmp/atlas-s41-bybit-native.xml": "23 cases, 1 failure; earlier attempt",
    "/tmp/atlas-s41-bybit-native2.xml": "23 cases, 0 failures; later targeted attempt",
    "/tmp/atlas-s41-binance-recovery.xml": "26 cases, 0 failures",
    "/tmp/atlas-s41-native-selection.xml": "35 cases, 0 failures",
    "/tmp/atlas-s41-selected-demo.xml": "34 cases, 0 failures",
    "/tmp/atlas-s41-export-scope.xml": "26 cases, 0 failures",
}

# These are intentionally broad candidate lists. They are attached only when
# section/requirement text has a matching topic; presence is not a test result.
AREAS: list[tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]] = [
    (("venue", "bybit", "binance", "exchange", "order", "execution", "capital", "protection", "recovery"),
     ("src/atlas/v2/runtime/production.py", "src/atlas/runtime/bybit_native.py", "src/atlas/runtime/binance_native.py", "src/atlas/runtime/bybit_demo.py", "src/atlas/runtime/binance_demo.py", "src/atlas/runtime/recovery.py", "src/atlas/runtime/protection_evidence.py"),
     ("tests/runtime/test_session041_bybit_native.py", "tests/runtime/test_session041_binance_native.py", "tests/runtime/test_session041_selected_demo.py", "tests/runtime/test_session041_bybit_demo.py", "tests/runtime/test_session041_binance_demo.py", "tests/runtime/test_session041_binance_reconciliation.py", "tests/runtime/test_recovery_gate.py", "tests/runtime/test_protection_evidence.py")),
    (("calendar", "event", "news", "decision-calendar"),
     ("src/atlas/science/decision_calendar.py", "src/atlas/scanner/calendar.py", "src/atlas/v2/news/events.py", "src/atlas/v2/science/s3_calendar.py", "src/atlas/v2/runtime/production.py"),
     ("tests/science/test_decision_calendar.py", "tests/scanner/test_scanner_calendar.py", "tests/v2/test_session034_scientific_calendar_closure.py", "tests/v2/test_session037_event_scope.py")),
    (("strategy", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "pair", "relative value", "feature", "model arena", "forecast"),
     ("src/atlas/v2/runtime/full_strategy_surface.py", "src/atlas/v2/runtime/production.py", "src/atlas/v2/science/analogue.py", "src/atlas/v2/chronology.py", "src/atlas/strategy/"),
     ("tests/v2/test_session041_strategy_surface.py", "tests/v2/test_session037_strategy_prefix.py", "tests/v2/test_session041_source_scope.py", "tests/v2/test_session036_analogue_diagnostic.py")),
    (("universe", "market data", "source", "stream", "public", "product", "instrument", "acquisition", "bars"),
     ("src/atlas/v2/runtime/broad_universe.py", "src/atlas/v2/runtime/broad_public_runtime.py", "src/atlas/v2/runtime/broad_serviced_acquisition.py", "src/atlas/v2/runtime/serviced_acquisition.py", "src/atlas/v2/runtime/production.py", "src/atlas/v2/product.py"),
     ("tests/v2/test_session041_dynamic_universe.py", "tests/v2/test_session041_broad_public_source.py", "tests/v2/test_session041_broad_stream_source.py", "tests/v2/test_session041_production_breadth.py", "tests/v2/test_session041_product_configuration.py")),
    (("report", "export", "publication", "evidence", "memory", "storage", "sqlite", "parquet", "health", "preflight", "resilience", "wal"),
     ("src/atlas/v2/runtime/read_only_report_worker.py", "src/atlas/v2/runtime/storage_preflight.py", "src/atlas/v2/runtime/live_health.py", "src/atlas/v2/science/broad_export.py", "src/atlas/v2/science/tuning_export.py", "src/atlas/v2/runtime/production.py"),
     ("tests/v2/test_session041_broad_export.py", "tests/v2/test_session038_report_worker.py", "tests/v2/test_session040_owner_runtime.py", "tests/v2/test_session040_export_snapshot_cache.py", "tests/v2/test_session039_public_evidence_storage.py")),
    (("desktop", "ipc", "product", "operator", "configuration", "config", "owner"),
     ("src/atlas/v2/product.py", "src/atlas/desktop/", "configs/", "atlas-product.spec"),
     ("tests/v2/test_session041_product_configuration.py", "tests/v2/test_session036_product.py", "tests/v2/test_session040_owner_runtime.py")),
    (("agent", "critic", "provider", "deepseek", "intelligence", "discovery", "research proposer"),
     ("src/atlas/v2/runtime/research_model_shadow.py", "src/atlas/v2/runtime/action_critic_shadow.py", "src/atlas/v2/runtime/action_critic_dispatcher.py"),
     ("tests/v2/test_session030_action_critic_runtime.py", "tests/v2/test_session041_strategy_surface.py")),
    (("chronology", "causal", "prefix", "revision", "availability", "replay"),
     ("src/atlas/v2/chronology.py", "src/atlas/data/availability.py", "src/atlas/data/replay.py", "src/atlas/v2/runtime/production.py"),
     ("tests/v2/test_session037_chronology.py", "tests/v2/test_session036_economic_chronology.py", "tests/v2/test_session037_strategy_prefix.py")),
]


def existing(paths: tuple[str, ...] | list[str]) -> list[str]:
    return [path for path in paths if (ROOT / path).exists()]


def topic_paths(text: str) -> tuple[list[str], list[str]]:
    value = text.lower()
    implementation: list[str] = []
    tests: list[str] = []
    for terms, code, test_paths in AREAS:
        if any(
            re.search(rf"\b{re.escape(term)}\b", value) is not None
            if re.fullmatch(r"[a-z]\d", term)
            else term in value
            for term in terms
        ):
            implementation.extend(existing(code))
            tests.extend(existing(test_paths))
    return sorted(set(implementation)), sorted(set(tests))


def s40_refs(text: str) -> list[str]:
    value = text.lower()
    selected: list[str] = []
    if any(word in value for word in ("storage", "report", "export", "health", "preflight", "resilience", "owner", "wal", "disk")):
        selected.extend(S40_EVIDENCE[:6])
    if any(word in value for word in ("venue", "execution", "capital", "protection", "recovery", "order", "binance", "bybit")):
        selected.extend((S40_EVIDENCE[0], S40_EVIDENCE[2], S40_EVIDENCE[6], S40_EVIDENCE[7]))
    if any(word in value for word in ("release", "closure", "phase", "qualification", "gate")):
        selected.extend(S40_EVIDENCE[6:])
    return [path for path in dict.fromkeys(selected) if (ROOT / path).exists()]


def xml_facts() -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for path, note in S41_XML.items():
        item: dict[str, Any] = {"path": path, "note": note, "evidence_scope": "CURRENT_SESSION_TEST_REPORT; git/checkpoint identity must be checked before attribution"}
        try:
            root = ET.parse(path).getroot()
            suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
            item["suite_totals"] = [
                {key: int(suite.get(key, "0")) for key in ("tests", "failures", "errors", "skipped")}
                for suite in suites
            ]
        except (OSError, ET.ParseError):
            item["availability"] = "MISSING_OR_UNREADABLE"
        facts.append(item)
    return facts


def section_kind(rows: list[dict[str, Any]]) -> str:
    text = " ".join(str(row.get("requirement", "")) for row in rows).lower()
    if any(word in text for word in ("must", "required", "shall", "never", "only", "prohibited", "cannot")):
        return "NORMATIVE_CANDIDATE_REQUIRES_COORDINATOR_REVIEW"
    if any(word in text for word in ("status", "at the handoff", "history", "prior", "checkpoint", "was " , "session-")):
        return "CONTEXT_OR_HISTORICAL_CANDIDATE_REQUIRES_COORDINATOR_REVIEW"
    return "BOOKKEEPING_OR_MIXED_CANDIDATE_REQUIRES_COORDINATOR_REVIEW"


def resolution_index(paths: list[Path]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("requirements", []) if isinstance(payload, dict) else []
        for row in rows:
            if not isinstance(row, dict) or not row.get("requirement_id"):
                continue
            merged.setdefault(row["requirement_id"], {}).update(row)
    return merged


def build(matrix: dict[str, Any], audit_paths: list[Path]) -> dict[str, Any]:
    requirements = matrix["requirements"]
    by_section: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    for row in requirements:
        by_section[(row["authority_source"], row["source_section"])].append(row)
    resolutions = resolution_index(audit_paths)
    groups = []
    for (source, section), rows in sorted(by_section.items()):
        candidates = "\n".join(str(row.get("requirement", "")) for row in rows)
        implementation, tests = topic_paths(section + "\n" + candidates)
        explicit = [resolutions[row["requirement_id"]] for row in rows if row["requirement_id"] in resolutions]
        group = {
            "authority_source": source,
            "source_section": section,
            "requirement_count": len(rows),
            "source_clause_refs": [
                {"requirement_id": row["requirement_id"], "source_start_line": row["source_start_line"], "source_end_line": row["source_end_line"]}
                for row in rows
            ],
            "classification_candidate": section_kind(rows),
            "candidate_implementation_files": implementation,
            "candidate_tests_not_run_claims": tests,
            "s40_checkpoint_evidence_refs_not_s41_passes": s40_refs(section + "\n" + candidates),
            "explicit_audit_resolution_rows": explicit,
            "missing_work": "Coordinator must inspect the exact clauses against implementation and attributable evidence; resolve normative/context/bookkeeping classification. Candidate paths do not imply coverage.",
        }
        if "event" in section.lower() or "calendar" in section.lower():
            group["known_gap_prompt"] = "Verify official economic-event/calendar ingestion and production wiring; calendar-related names/tests do not establish an official source or operational feed."
        if "s8" in section.lower() or "pair" in section.lower() or "relative value" in section.lower():
            group["known_gap_prompt"] = "Verify owner-frozen S8 pairs/configuration, data, execution policy, and production activation as separate evidence."
        if any(term in section.lower() for term in ("authorization", "owner", "run host", "selected")):
            group["known_gap_prompt"] = "Verify selected-account authorization and supported run-host qualification from direct attributable records."
        if any(term in section.lower() for term in ("strategy", "report", "closure", "release", "40.", "43.", "44.")):
            group["known_gap_prompt"] = "Full strategy-surface/report coverage and strict closure remain separate gates; targeted tests or historical statuses do not close them."
        groups.append(group)

    all_ids = {row["requirement_id"] for row in requirements}
    unknown_ids = sorted(set(resolutions) - all_ids)
    touched = len(set(resolutions) & all_ids)
    return {
        "artifact_type": "AtlasSession041RequirementAccountingFindingsV1",
        "session": 41,
        "status": "UNVERIFIED",
        "closure_gate_passed": False,
        "draft": True,
        "start_sha": matrix.get("start_sha"),
        "final_sha": None,
        "source_relationship": {
            "governing_roots": [row for row in matrix["source_catalog"] if row["role"] == "GOVERNING_FREEZE"],
            "additive_contracts_require_authority_review": [row for row in matrix["source_catalog"] if row["role"] != "GOVERNING_FREEZE"],
            "accepted_amended_root_relationship": "The root treats V1 freeze, amended V2 freeze, and agent freeze as accepted authority in that order; this accounting preserves source hashes and does not reinterpret them.",
            "consultation_floor": "UNACCEPTED; not included as a governing source or alternative evidence policy.",
        },
        "accounting": {
            "requirement_count": len(requirements),
            "section_group_count": len(groups),
            "all_rows_initially_unverified": all(row.get("current_state") == "UNVERIFIED" and row.get("scope_classification") == "AUDIT_PENDING" for row in requirements),
            "current_matrix_status_mutated": False,
            "classification_note": "Section classification and path lists are audit navigation hints. All source clauses remain UNVERIFIED until individually reviewed with attributable evidence.",
            "resolution_rows_merged": touched,
            "unknown_resolution_ids": unknown_ids,
        },
        "ordinary_gaps_to_keep_visible": [
            {"topic": "official_event_calendar", "gap": "Verify official source ingestion and production use; no name-only credit.", "candidate_files": ["src/atlas/scanner/calendar.py", "src/atlas/science/decision_calendar.py", "src/atlas/v2/news/events.py", "src/atlas/v2/runtime/production.py"], "candidate_tests": ["tests/scanner/test_scanner_calendar.py", "tests/science/test_decision_calendar.py", "tests/v2/test_session034_scientific_calendar_closure.py"]},
            {"topic": "owner_s8_pairs_configuration", "gap": "Verify explicitly selected owner-approved S8 pair/configuration and its complete production path.", "candidate_files": ["src/atlas/v2/runtime/full_strategy_surface.py", "src/atlas/v2/runtime/production.py", "configs/"], "candidate_tests": ["tests/v2/test_session041_strategy_surface.py"]},
            {"topic": "selected_authorization_and_run_host", "gap": "A local/host preflight or selected demo test is not selected-account authorization or supported-run-host qualification.", "candidate_files": ["src/atlas/v2/runtime/storage_preflight.py", "src/atlas/v2/runtime/production.py", "src/atlas/runtime/identity.py"], "candidate_tests": ["tests/runtime/test_session041_selected_demo.py", "tests/v2/test_session040_owner_runtime.py"]},
            {"topic": "full_strategy_and_report_surface", "gap": "Targeted strategy and export runs do not establish complete V2 strategy/report scope or capacity qualification.", "candidate_files": ["src/atlas/v2/runtime/full_strategy_surface.py", "src/atlas/v2/runtime/read_only_report_worker.py", "src/atlas/v2/science/broad_export.py"], "candidate_tests": ["tests/v2/test_session041_strategy_surface.py", "tests/v2/test_session041_broad_export.py"]},
            {"topic": "strict_closure", "gap": "Keep closure false until every normative clause is resolved and required offline/live gates have direct attributable evidence.", "candidate_files": ["docs/v2/SESSION041_REQUIREMENT_ACCOUNTING_FINDINGS.json", "SESSION041_FULL_V2_REQUIREMENT_MATRIX.json"], "candidate_tests": []},
        ],
        "session041_junit_xml_inventory": xml_facts(),
        "s40_checkpoint_evidence_catalog": [path for path in S40_EVIDENCE if (ROOT / path).exists()],
        "source_section_mapping": groups,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=MATRIX)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--audit", action="append", type=Path, default=[], help="Coordinator-reviewed per-clause audit resolution JSON; never a blanket pass.")
    args = parser.parse_args()
    matrix = json.loads(args.matrix.read_text(encoding="utf-8"))
    result = build(matrix, args.audit)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"requirement_count": result["accounting"]["requirement_count"], "section_groups": result["accounting"]["section_group_count"], "merged_resolution_rows": result["accounting"]["resolution_rows_merged"], "status": result["status"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
