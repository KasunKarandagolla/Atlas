"""Index retained evidence without claiming qualification from fixture success."""

import gzip
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = Path(__file__).resolve().parent
BASE = "b0db7d0527e68c737eefd564a87f740f5929c5f6"


def digest(data):
    return hashlib.sha256(data).hexdigest()


files = (
    subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "src", "tests"], cwd=ROOT)
    .decode()
    .splitlines()
)
identities = {p: digest((ROOT / p).read_bytes()) for p in sorted(set(files))}
changed = (
    subprocess.check_output(["git", "diff", "--name-only", BASE, "--", "src", "tests"], cwd=ROOT).decode().splitlines()
)
changed += (
    subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "src", "tests"], cwd=ROOT)
    .decode()
    .splitlines()
)
reports = []
for path in sorted(EVIDENCE.glob("*.xml")):
    suite = next(ET.parse(path).iter("testsuite"))
    counts = {k: int(suite.attrib[k]) for k in ("tests", "errors", "failures", "skipped")}
    counts["passed"] = counts["tests"] - counts["errors"] - counts["failures"] - counts["skipped"]
    cases = []
    for case in suite.iter("testcase"):
        for kind in ("failure", "error", "skipped"):
            item = case.find(kind)
            if item is not None:
                cases.append(
                    {
                        "test": case.attrib["classname"] + "::" + case.attrib["name"],
                        "kind": kind,
                        "message": item.attrib.get("message"),
                    }
                )
    raw_path = path.with_suffix(".xml.gz")
    reports.append(
        {
            "path": str(path.relative_to(ROOT)),
            "counts": counts,
            "status": "TESTED" if not counts["errors"] and not counts["failures"] else "TEST GATE",
            "wall_seconds_junit": float(suite.attrib["time"]),
            "normalized_sha256": digest(path.read_bytes()),
            "raw_sha256": digest(gzip.decompress(raw_path.read_bytes())) if raw_path.exists() else None,
            "command": json.loads(path.with_suffix(".command.json").read_text()),
            "failures_errors_skips": cases,
        }
    )
entries = []
for path in sorted(EVIDENCE.rglob("*")):
    if path.is_file() and path.name != "evidence_manifest.json" and "__pycache__" not in path.parts:
        entries.append(
            {"path": str(path.relative_to(ROOT)), "sha256": digest(path.read_bytes()), "bytes": path.stat().st_size}
        )
preflight = json.loads((EVIDENCE / "preflight.json").read_text())
manifest = {
    "version": "ATLAS_S48_EVIDENCE_MANIFEST_V1",
    "status": "IMPLEMENTED",
    "base_sha": BASE,
    "s47_source_sha": preflight["source"],
    "branch": "impl/session-048-position-lifecycle-protection",
    "implementation_push": json.loads((EVIDENCE / "implementation_push.json").read_text())
    if (EVIDENCE / "implementation_push.json").exists()
    else None,
    "final_head_identity": "Git commit containing final handoff; exact final remote SHA returned after final ls-remote verification (cannot self-embed)",
    "model": "gpt-6.1-sol",
    "subagents": [],
    "source_test_tree_sha256": digest(json.dumps(identities, sort_keys=True).encode()),
    "changed_source_tests": {p: identities[p] for p in sorted(set(changed))},
    "frozen_hashes": preflight["freezes"],
    "reports": reports,
    "artifacts": entries,
    "capital_enabled": False,
    "assisted_enabled": False,
    "management_amendment": "PROPOSED",
    "dependency_changes": [],
    "full_repository_suite_ran": False,
    "resource_qualification": "NOT ESTIMABLE: no Atlas CPU/RSS/latency/bandwidth/token probe; test wall times only",
    "limitations": [
        "Overlapping reports do not add opportunities; red/intermediate and baseline failures remain retained",
        "Four inherited assertion failures reproduced on accepted base; no production readiness/selector weakening",
        "Native Binance wire/protection, final economic completeness, liquidation/ADL and sustained capacity remain TEST GATE",
        "Projection/context checks and half-TP shadow cannot admit execution, release reservations or mature outcomes",
        "Full-source mypy retains 52 baseline diagnostics; zero added normalized messages",
        "Context7 plugin installed, callable MCP unavailable; no installation",
        "Eight genuine weeks/200 matured opportunities with regime/dependence coverage is a floor, not a pass",
    ],
}
(EVIDENCE / "evidence_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(
    "Retained reports:",
    len(reports),
    "artifact identities:",
    len(entries),
    "source/test changes:",
    len(manifest["changed_source_tests"]),
)
