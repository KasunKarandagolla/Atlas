"""Inventory completed S41 validation evidence without promoting closure."""

from __future__ import annotations

import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    target = ROOT / "SESSION041_FULL_V2_VALIDATION_LEDGER.json"
    previous = json.loads(target.read_text()) if target.exists() else {}
    archive = ROOT / "docs/v2/session041-evidence"
    archive.mkdir(parents=True, exist_ok=True)
    paths = sorted({*Path("/tmp").glob("atlas-s41-*.xml"), *Path("/tmp").glob("s41-*.xml"),
        *Path("/tmp").glob("session041-*.xml"), *Path("/tmp/s41-central").glob("*.xml")})
    evidence = []
    for path in paths:
        raw = path.read_bytes()
        root = ET.fromstring(raw)
        suites = list(root.iter("testsuite"))
        totals = {key: sum(int(suite.get(key, "0")) for suite in suites)
            for key in ("tests", "failures", "errors", "skipped")}
        failures = [{"name": case.get("name"), "class": case.get("classname"),
            "kind": result.tag, "message": result.get("message")}
            for case in root.iter("testcase") for result in case
            if result.tag in {"failure", "error", "skipped"}]
        item = {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "totals": totals, "passed": totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"],
            "exceptions": failures, "checkpoint_attribution": "DEVELOPMENT_TREE; FINAL_SHA_NOT_QUALIFIED"}
        retained = archive / (item["sha256"] + ".xml")
        if not retained.exists():
            retained.write_bytes(raw)
        item["retained_path"] = str(retained.relative_to(ROOT))
        receipt = path.with_suffix(".json")
        if receipt.exists() and path.parent.name == "s41-central":
            metadata = json.loads(receipt.read_text())
            item["source_receipt"] = {"path": str(receipt), "sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
                "completed": metadata.get("completed"), "exit_code": metadata.get("exit_code"),
                "source_before": metadata.get("source_before", {}).get("tree_sha256"),
                "source_after": metadata.get("source_after", {}).get("tree_sha256"),
                "source_stable": metadata.get("source_stable")}
            copied_receipt = archive / (item["source_receipt"]["sha256"] + ".json")
            if not copied_receipt.exists():
                copied_receipt.write_bytes(receipt.read_bytes())
            item["source_receipt"]["retained_path"] = str(copied_receipt.relative_to(ROOT))
        evidence.append(item)
    indexed = {item["sha256"]: item for item in previous.get("evidence", [])}
    indexed.update({item["sha256"]: item for item in evidence})
    ledger = {"schema_version": 1, "session": 41, "status": "UNVERIFIED",
        "engineering_closure_passed": False,
        "head_at_inventory": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "final_code_sha": None, "final_tested_sha": None, "native_windows_qualified": False,
        "package_created": False, "capital_enabled": False, "assisted_enabled": False,
        "economic_status": "NOT ESTIMABLE", "evidence": list(indexed.values()),
        "notes": ["Skipped tests are never counted as passed.",
            "Failed attempts are retained; later focused passes do not qualify a full suite.",
            "Interrupted invocations without completed JUnit XML are not listed as passes.",
            "Development source receipts and review hashes do not qualify an untested final SHA.",
            "No Linux result substitutes for native Windows qualification."]}
    target.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"evidence_reports": len(evidence), "status": ledger["status"], "output": str(target)}))


if __name__ == "__main__":
    main()
