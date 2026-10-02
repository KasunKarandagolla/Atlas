"""Portable deterministic checks of package integrity/exclusions and build inputs."""

from __future__ import annotations

import ast
import hashlib
import json
import tempfile
from pathlib import Path

import yaml
from windows_manifest import (
    LOCK_COMPOSITION_OVERRIDES,
    PYTHON_VERSION,
    RUNTIME_PLATFORM,
    inventory,
    locked_versions,
    verify,
    write_json,
)


def require_rejection(callback, message: str) -> None:
    try:
        callback()
    except ValueError:
        return
    raise AssertionError(message)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    for path in [*sorted((root / "scripts").glob("windows_*.py")), root / "atlas-product.spec"]:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    resource_config = json.loads((root / "packaging/windows/resources.json").read_text(encoding="utf-8"))
    for name in resource_config["files"]:
        path = root / name
        if not path.is_file() or path.is_symlink() or path.resolve().parent != path.parent.resolve():
            raise AssertionError("Required bundle resource unavailable")
    lock = root / "requirements-windows-lock.txt"
    versions = locked_versions(lock)
    if "nautilus-trader" in versions or versions.get("pyinstaller") != "6.22.3":
        raise AssertionError("Windows runtime boundary/version invalid")
    core = locked_versions(root / "requirements-lock.txt")
    agents = locked_versions(root / "requirements-agent-lock.txt")
    for name, version in versions.items():
        for accepted in (core, agents):
            if name in accepted and accepted[name] != version:
                override = LOCK_COMPOSITION_OVERRIDES.get(name)
                if (not override or core.get(name) != override["requirements-lock.txt"]
                        or agents.get(name) != override["requirements-agent-lock.txt"]
                        or version != override["requirements-windows-lock.txt"]):
                    raise AssertionError("Windows dependency differs from accepted lock: " + name)
    parts = lock.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(parts):
        if "==" in line and not line.startswith("#"):
            if index + 1 == len(parts) or "--hash=sha256:" not in parts[index + 1]:
                raise AssertionError("Dependency lacks required hash")
    workflow = yaml.safe_load((root / ".github/workflows/windows-product.yml").read_text(encoding="utf-8"))
    if workflow["jobs"]["build"]["runs-on"] != "windows-2025":
        raise AssertionError("Build is not native Windows")
    setup = [step for step in workflow["jobs"]["build"]["steps"]
             if str(step.get("uses", "")).startswith("actions/setup-python@")]
    if len(setup) != 1 or setup[0]["with"] != {"python-version": PYTHON_VERSION, "architecture": "x64"}:
        raise AssertionError("Native interpreter identity differs from the manifest")
    triggers = workflow.get("on", workflow.get(True))  # YAML 1.1 recognizes "on" as boolean.
    if triggers["push"]["branches"] != ["impl/session-036-final-development-closure-windows-tune-ready"]:
        raise AssertionError("Automatic native build escaped the dedicated implementation branch")
    if triggers["workflow_dispatch"]["inputs"]["signed_release"]["default"] is not False:
        raise AssertionError("Diagnostic push must not imply a signed owner release")
    with tempfile.TemporaryDirectory(prefix="atlas-windows-pipeline-check-") as temporary:
        payload = Path(temporary)
        file = payload / "fixture.txt"
        file.write_text("source-bound fixture\n", encoding="utf-8")
        entries = inventory(payload)
        manifest: dict = {"schema_version": 1, "capital_enabled": False, "assisted_enabled": False,
                    "artifact_type": "AtlasWindowsBuildManifestV1", "version": "2.0.36.0",
                    "python_version": PYTHON_VERSION, "runtime_platform": RUNTIME_PLATFORM,
                    "dependency_locks": dict.fromkeys((
                        "requirements-lock.txt", "requirements-agent-lock.txt", "requirements-windows-lock.txt"), "a" * 64),
                    "runtime_lock_sha256": "a" * 64,
                    "source_sha": "e13994752f165ad8b42f7f7c441076078d4388cc", "payload": entries,
                    "payload_tree_sha256": hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
        write_json(payload / "build-manifest.json", manifest)
        verify(payload, expected_sha=manifest["source_sha"])
        require_rejection(lambda: verify(payload, expected_sha="b" * 40), "Wrong source identity accepted")
        for name, invalid in (("capital_enabled", True), ("assisted_enabled", True),
                              ("source_sha", "UNVERIFIED"), ("dependency_locks", {}),
                              ("python_version", "3.12.13"), ("runtime_platform", {"system": "Linux"}),
                              ("artifact_type", "unrelated")):
            write_json(payload / "build-manifest.json", {**manifest, name: invalid})
            require_rejection(lambda: verify(payload), "Invalid manifest field accepted: " + name)
        write_json(payload / "build-manifest.json", manifest)
        file.write_text("tampered\n", encoding="utf-8")
        require_rejection(lambda: verify(payload), "Payload tampering accepted")
        secret = payload / ".env"
        secret.write_text("not-a-real-secret\n", encoding="utf-8")
        require_rejection(lambda: inventory(payload), "Forbidden configuration accepted")
        secret.unlink()
        large = payload / "large.json"
        # Scan across a 64KiB boundary and beyond the former 1MB scanning limit.
        large.write_bytes(b" " * (64 * 1024 * 17 - 5) + b"sk-" + b"A" * 40)
        require_rejection(lambda: inventory(payload), "Credential in large text resource accepted")
        large.unlink()
        environment = payload / ".venv312"
        environment.mkdir()
        (environment / "python.exe").write_bytes(b"fixture")
        require_rejection(lambda: inventory(payload), "Development environment accepted")
    print(json.dumps({"status": "TESTED", "check": "PORTABLE_WINDOWS_PIPELINE_INVARIANTS",
                      "locked_dependencies": len(versions), "native_windows_execution": "BLOCKED BY ENVIRONMENT"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
