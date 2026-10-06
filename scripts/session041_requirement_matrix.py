"""Produce a conservative, line-addressed S41 authority accounting draft.

This inventories source clauses; it never infers implementation or test success.
Human workstream findings are merged separately by the coordinator.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
START = "55ccd6f371caa660ba4d3c7a6ad4759d2d3b6393"


def workstream(section: str, source: str) -> str:
    text = (section + " " + source).lower()
    if "agent" in text or "discovery" in text:
        return "E_INTELLIGENCE_AGENT"
    if any(word in text for word in ("desktop", "ipc", "product", "deployment")):
        return "F_PRODUCT"
    if any(word in text for word in ("storage", "resilience", "persistence", "health", "report", "memory")):
        return "G_RESILIENCE"
    if any(word in text for word in ("execution", "capital", "protection", "risk", "recovery", "binance")):
        return "C_EXECUTION_ROOT"
    if any(word in text for word in ("universe", "market-data", "bars", "scanner", "source", "collection")):
        return "B_PUBLIC_UNIVERSE"
    if any(word in text for word in ("strategy", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "feature", "model", "mathemat", "scenario", "news", "event", "intelligence", "research")):
        return "E_INTELLIGENCE_AGENT"
    return "A_AUTHORITY_ROOT"


def clauses(path: Path):
    lines = path.read_text().splitlines()
    section = path.name
    pending: list[tuple[int, str]] = []
    fenced = False
    for number, line in enumerate(lines, 1):
        if line.startswith("#") and not fenced:
            if pending:
                yield section, pending
                pending = []
            section = line.lstrip("# ")
        elif line.startswith(("```", "~~~")):
            fenced = not fenced
            pending.append((number, line))
            if not fenced:
                yield section, pending
                pending = []
        elif not line.strip() and not fenced:
            if pending:
                yield section, pending
                pending = []
        elif line.strip() and line.strip() != "---":
            if not fenced and re.match(r"(?:[-*] |\d+\. |\|)", line):
                if pending:
                    yield section, pending
                yield section, [(number, line)]
                pending = []
            else:
                pending.append((number, line))
    if pending:
        yield section, pending


def main() -> None:
    primary = [
        ROOT / "ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md",
        ROOT / "ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md",
        ROOT / "docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md",
    ]
    additive = sorted(
        path for path in (ROOT / "docs/v2").glob("*.md")
        if any(word in path.name for word in ("CONTRACT", "ADR", "DESIGN", "AMENDMENT", "FMEA", "SCHEMA", "BUILD_GATE"))
        and "PROPOSAL" not in path.name
        and path not in primary
    )
    source_catalog = []
    requirements = []
    for index, path in enumerate(primary + additive):
        source = str(path.relative_to(ROOT))
        source_id = f"AUTH{index + 1:02}"
        source_catalog.append({"id": source_id, "path": source, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "role": "GOVERNING_FREEZE" if path in primary else "ACCEPTED_ADDITIVE_CONTRACT_REQUIRES_COORDINATOR_VERIFICATION"})
        for section, block in clauses(path):
            text = "\n".join(line for _, line in block)
            requirements.append({
                "requirement_id": f"{source_id}-L{block[0][0]:04}",
                "requirement": text,
                "authority_source": source,
                "source_sha256": source_catalog[-1]["sha256"],
                "source_section": section,
                "source_start_line": block[0][0],
                "source_end_line": block[-1][0],
                "affected_subsystem": workstream(section, source),
                "implementation_files": [],
                "existing_tests": [],
                "current_state": "UNVERIFIED",
                "scope_classification": "AUDIT_PENDING",
                "gap_class": "UNCLASSIFIED_PENDING_IMPLEMENTATION_INSPECTION",
                "missing_engineering_work": "Inspect exact implementation and production integration; do not infer a pass from filenames or prior status.",
                "external_live_only_proof_required": [],
                "assigned_workstream": workstream(section, source),
                "assigned_subagent": None,
                "final_resolution": "UNVERIFIED",
                "final_sha": None,
                "validation_evidence": [],
            })
    # Checkpoint records are catalogued as evidence, not promoted to authority.
    checkpoints = [
        {"path": str(path.relative_to(ROOT)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in sorted((ROOT / "docs/v2").glob("*.json"))
        if not path.name.startswith("SESSION041")
    ]
    matrix = {
        "schema_version": 1,
        "session": 41,
        "status": "UNVERIFIED",
        "closure_gate_passed": False,
        "draft": True,
        "start_sha": START,
        "authority_order": ["V1 safety and frozen V1", "amended V2 additive scope", "agent extension and accepted amendments", "current-state/checkpoints for status only", "consultation non-authoritative"],
        "accounting_method": "Conservative clause superset retaining every non-heading source block, list item, table row and complete schema block. Context and historical phase instructions are retained for coordinator classification. Source inclusion does not assert that every row is normative or satisfied.",
        "source_catalog": source_catalog,
        "checkpoint_evidence_catalog": checkpoints,
        "requirements": requirements,
        "ordinary_missing_implementation": [],
        "unclassified_requirements_prevent_closure": True,
        "capital_enabled": False,
        "assisted_enabled": False,
        "economic_status": "NOT ESTIMABLE",
        "consultation_alternative_evidence_policy": "UNACCEPTED",
    }
    target = ROOT / "SESSION041_FULL_V2_REQUIREMENT_MATRIX.json"
    target.write_text(json.dumps(matrix, indent=2) + "\n")
    print(json.dumps({"sources": len(source_catalog), "clauses": len(requirements), "checkpoint_evidence_sources": len(checkpoints), "draft": str(target)}))


if __name__ == "__main__":
    main()
