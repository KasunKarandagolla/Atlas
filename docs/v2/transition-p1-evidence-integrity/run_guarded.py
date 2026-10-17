"""Run the specified local evidence script with credentials removed and sockets fenced."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
if not args.command:
    parser.error("provide a local Python script and its arguments")
root = args.repo.resolve()
blocked = ("BINANCE", "BYBIT", "GEMINI", "GEMMA", "OPENAI_API", "ANTHROPIC", "TELEGRAM", "AWS_SECRET", "AWS_ACCESS")
env = {k: v for k, v in os.environ.items() if not any(x in k.upper() for x in blocked)
       and not k.upper().endswith(("_API_KEY", "_API_SECRET", "_ACCESS_TOKEN"))}
env["PYTHONPATH"] = os.pathsep.join(str(root / p) for p in ("scripts", "src", "tests", "."))
env["PYTHONDONTWRITEBYTECODE"] = "1"
raise SystemExit(subprocess.call([sys.executable, *args.command], cwd=root, env=env))
