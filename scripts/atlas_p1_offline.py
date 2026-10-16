#!/usr/bin/env python3
"""Reproduce P1 pytest checks with provider/venue credentials scrubbed.

The existing S49 socket/DNS guard is installed at interpreter startup and in
Python child processes. Loopback/Unix sockets remain usable for IPC fixtures.
This is a test runner, not an operating-mode or account configuration change.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    blocked = ("BINANCE", "BYBIT", "GEMINI", "GEMMA", "OPENAI_API", "ANTHROPIC", "TELEGRAM", "AWS_SECRET", "AWS_ACCESS")
    env = {
        key: value
        for key, value in os.environ.items()
        if not any(name in key.upper() for name in blocked)
        and not key.upper().endswith(("_API_KEY", "_API_SECRET", "_ACCESS_TOKEN"))
    }
    env["PYTHONPATH"] = os.pathsep.join(str(path) for path in (root / "scripts", root / "src", root / "tests", root))
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return subprocess.call(
        [sys.executable, "-m", "pytest", "-p", "atlas_s49_offline_guard", *sys.argv[1:]], cwd=root, env=env
    )


if __name__ == "__main__":
    raise SystemExit(main())
