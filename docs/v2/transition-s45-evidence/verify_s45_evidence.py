from __future__ import annotations

import hashlib
import json
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = Path(__file__).resolve().parent
MANIFEST = EVIDENCE / "evidence_manifest.json"
REPORTS = (
    ROOT / "docs/v2/ATLAS_TRANSITION_S45_DESIGN_DECISION_REGISTER.md",
    ROOT / "docs/v2/ATLAS_TRANSITION_S45_DOC_FINALIZATION_READINESS.md",
    ROOT / "docs/v2/ATLAS_TRANSITION_S45_FACTUAL_HANDOFF.md",
)
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"AKIA[0-9A-Z]{16}"),
    re.compile(rb"(?i)(?:api[_-]?key|secret|token)\s*[:=]\s*[\"'][A-Za-z0-9_./+=-]{24,}"),
)


def verify() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    errors: list[str] = []
    for item in manifest["files"]:
        path = EVIDENCE / item["path"]
        if not path.is_file():
            errors.append(f"missing evidence: {path}")
            continue
        content = path.read_bytes()
        if len(content) != item["bytes"]:
            errors.append(f"byte count mismatch: {path}")
        if hashlib.sha256(content).hexdigest() != item["sha256"]:
            errors.append(f"SHA-256 mismatch: {path}")
        if "junit" in item:
            document = ET.parse(path).getroot()
            suites = [document] if document.tag == "testsuite" else list(document.iter("testsuite"))
            counts = tuple(
                sum(int(suite.attrib.get(key, 0)) for suite in suites)
                for key in ("tests", "failures", "errors", "skipped")
            )
            expected = (
                item["junit"]["cases"],
                item["junit"]["failures"],
                item["junit"]["errors"],
                item["junit"]["skips"],
            )
            if counts != expected:
                errors.append(f"JUnit count mismatch: {path}: {counts} != {expected}")

    link_count = 0
    for report in REPORTS:
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", report.read_text(encoding="utf-8")):
            if target.startswith(("https://", "http://", "#", "mailto:")):
                continue
            local_path = target.split("#", 1)[0]
            if not local_path:
                continue
            link_count += 1
            if not (report.parent / local_path).resolve().exists():
                errors.append(f"broken report link: {report.name}: {target}")

    tracked = subprocess.run(
        ["git", "diff", "--name-only", "HEAD", "--"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    changed_paths = set(tracked + untracked)
    for name in changed_paths:
        path = ROOT / name
        if not path.is_file():
            continue
        content = path.read_bytes()
        if any(pattern.search(content) for pattern in SECRET_PATTERNS):
            errors.append(f"possible credential pattern: {name}")

    if errors:
        raise SystemExit("\n".join(errors))
    print(
        f"PASS: {len(manifest['files'])} evidence artifacts/hashes/byte counts and JUnit totals; "
        f"{link_count} local report links; {len(changed_paths)} changed/untracked paths secret-scanned."
    )
    print(f"Manifest SHA-256 (manifest excludes its own hash): {hashlib.sha256(MANIFEST.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    verify()
