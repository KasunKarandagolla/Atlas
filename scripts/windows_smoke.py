"""Offline packaged-runtime checks with development tools removed from child PATH."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import tempfile
from pathlib import Path

from windows_manifest import inventory, verify, write_json

FIXTURE = Path(__file__).resolve().parents[1] / "packaging/windows/first-run-fixture.json"


def fresh_machine_environment() -> dict[str, str]:
    allowed = {"SYSTEMROOT", "WINDIR", "LOCALAPPDATA", "APPDATA", "TEMP", "TMP", "USERPROFILE",
               "PROGRAMDATA", "ALLUSERSPROFILE", "USERNAME", "USERDOMAIN", "COMSPEC", "SYSTEMDRIVE"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in allowed}
    windows = os.environ["SYSTEMROOT"]
    environment["PATH"] = str(Path(windows) / "System32") + ";" + windows
    environment["QT_QPA_PLATFORM"] = "offscreen"
    return environment


def json_command(executable: Path, arguments: list[str], *, environment: dict[str, str]) -> dict:
    try:
        result = subprocess.run([str(executable), *arguments], env=environment, cwd=executable.parent,
                                text=True, capture_output=True, timeout=120, check=True)
    except subprocess.CalledProcessError as error:
        stdout = (error.stdout or "")[-4000:]
        stderr = (error.stderr or "")[-4000:]
        raise ValueError(f"Packaged runtime command failed ({error.returncode}); stdout={stdout!r}; stderr={stderr!r}") from error
    lines = result.stdout.strip().splitlines()
    if not lines:
        raise ValueError("Packaged runtime emitted no diagnostic result")
    response = json.loads(lines[-1])
    if not isinstance(response, dict):
        raise ValueError("Packaged runtime diagnostic is not an object")
    return response


def smoke(payload: Path, *, expected_sha: str | None = None, data_root: Path | None = None) -> dict:
    if platform.system() != "Windows":
        raise ValueError("BLOCKED BY ENVIRONMENT: native Windows smoke requires Windows")
    manifest = verify(payload, expected_sha=expected_sha)
    before = inventory(payload)
    environment = fresh_machine_environment()
    diagnostics = json_command(payload / "atlas-product.exe", ["--diagnostics", "--json"], environment=environment)
    for field in ("capital_enabled", "assisted_enabled"):
        if diagnostics.get(field) is not False:
            raise ValueError("Packaged runtime authority boundary missing")
    if diagnostics.get("source_sha") != manifest["source_sha"]:
        raise ValueError("Packaged diagnostics source identity mismatch")
    if diagnostics.get("version") != manifest["version"]:
        raise ValueError("Packaged diagnostics version identity mismatch")
    broker = json_command(payload / "atlas-product.exe", ["--broker-smoke"], environment=environment)
    if (broker.get("status") != "TESTED" or broker.get("transport") != "AF_PIPE"
            or broker.get("no_provider_calls") is not True or broker.get("active_handlers_after_close") != 0
            or broker.get("max_handlers") != 4 or broker.get("current_user_dacl_verified") is not True
            or broker.get("protected_secret_roundtrip") != "TESTED"
            or broker.get("damaged_secret_rejected") is not True
            or broker.get("owner_secret_accessed") is not False):
        raise ValueError("Native broker transport/completion/shutdown fixture failed")
    if broker.get("owner_process_death_observed") is not True:
        raise ValueError("Native broker owner lifetime fixture failed")
    with tempfile.TemporaryDirectory(prefix="atlas-windows-smoke-") as temporary:
        evidence_root = data_root or Path(temporary) / "research"
        result = json_command(payload / "atlas-product.exe", ["--smoke", "--data-root", str(evidence_root)],
                              environment=environment)
        if result.get("status") != "TESTED":
            raise ValueError("Offline packaged production fixture failed")
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        run_id = result.get("run_id")
        if not isinstance(run_id, str) or len(run_id) != 32 or any(c not in "0123456789abcdef" for c in run_id):
            raise ValueError("Offline fixture returned an invalid run identity")
        run_root = evidence_root / "runs" / run_id
        run_manifest = json.loads((run_root / "run.json").read_text(encoding="utf-8"))
        state = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        if (run_manifest.get("configuration") != fixture["configuration"]
                or run_manifest.get("source_sha") != manifest["source_sha"]
                or run_manifest.get("run_id") != run_id or state.get("run_id") != run_id
                or run_manifest.get("capital_enabled") is not False
                or run_manifest.get("assisted_enabled") is not False
                or run_manifest.get("holdout_access") is not False):
            raise ValueError("First-run configuration/source/authority identity mismatch")
        if (state.get("status") != "TESTED" or state.get("reason") != "OFFLINE_COMPOSITION_FIXTURE_ONLY"
                or type(state.get("stopped_at_ns")) is not int or state["stopped_at_ns"] <= 0):
            raise ValueError("Offline production fixture did not publish a clean stopped state")
    if inventory(payload) != before:
        raise ValueError("Runtime modified its installation payload")
    return {"schema_version": 1, "status": "TESTED", "check": "NATIVE_WINDOWS_OFFLINE_PRODUCT",
            "source_sha": manifest["source_sha"], "diagnostics": diagnostics, "offline_fixture": result,
            "native_broker_transport": broker,
            "first_run_configuration": "TESTED",
            "child_path_contains_python_git": False, "provider_credentials_passed": False,
            "capital_enabled": False, "assisted_enabled": False,
            "actual_windows11_hardware": "UNVERIFIED", "live_public_runtime": "TEST GATE"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--source-sha")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = smoke(args.payload.resolve(), expected_sha=args.source_sha)
    write_json(args.output, result)
    print(json.dumps({"status": result["status"], "check": result["check"], "source_sha": result["source_sha"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
