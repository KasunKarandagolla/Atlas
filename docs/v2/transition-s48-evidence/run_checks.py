"""Retain exact offline check commands, output and JUnit original bytes."""

import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(os.environ.get("ATLAS_CHECK_ROOT", Path(__file__).resolve().parents[3]))
EVIDENCE = Path(__file__).resolve().parent
label, *arguments = sys.argv[1:]
python = "/home/kasun/Music/atlas/.venv/bin/python"
env = dict(os.environ)
env["PYTHONPATH"] = str(ROOT / "src") + ":" + str(Path(__file__).resolve().parents[1] / "transition-s47-evidence")
command = [
    python,
    "-m",
    "pytest",
    "-p",
    "s47_offline_guard",
    "-q",
    "-o",
    "junit_family=legacy",
    *arguments,
    "--junitxml=" + str(EVIDENCE / (label + ".xml")),
]
started = time.monotonic()
files = (
    subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "src", "tests"], cwd=ROOT)
    .decode()
    .splitlines()
    if (ROOT / ".git").exists()
    else [str(p.relative_to(ROOT)) for folder in ("src", "tests") for p in (ROOT / folder).rglob("*.py")]
)
identities = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sorted(set(files))}
tree_hash = hashlib.sha256(json.dumps(identities, sort_keys=True).encode()).hexdigest()
result = subprocess.run(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
output = result.stdout
with gzip.open(EVIDENCE / (label + ".output.txt.gz"), "wb") as stream:
    stream.write(output)
report = EVIDENCE / (label + ".xml")
if report.exists():
    raw = report.read_bytes()
    with gzip.open(EVIDENCE / (label + ".xml.gz"), "wb") as stream:
        stream.write(raw)
    report.write_bytes(b"\n".join(line.rstrip() for line in raw.splitlines()) + b"\n")
(EVIDENCE / (label + ".command.json")).write_text(
    json.dumps(
        {
            "command": command,
            "cwd": str(ROOT),
            "PYTHONPATH": env["PYTHONPATH"],
            "exit_code": result.returncode,
            "wall_seconds": time.monotonic() - started,
            "source_test_tree_sha256_at_start": tree_hash,
            "scope": "offline fixtures; parent IP effects blocked; subprocess fixtures use fake transports",
        },
        indent=2,
    )
    + "\n"
)
print(output.decode(errors="replace")[-9000:])
sys.exit(result.returncode)
