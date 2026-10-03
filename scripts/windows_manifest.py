"""Deterministic native package manifest and exclusion validation (stdlib only)."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
from pathlib import Path

MANIFEST = "build-manifest.json"
FORBIDDEN_PARTS = {".git", ".aws", ".venv", ".codex", ".agents", "tests", "__pycache__",
                   ".pytest_cache", ".mypy_cache", ".ruff_cache", ".hypothesis"}
FORBIDDEN_SUFFIXES = {".sqlite", ".sqlite-wal", ".sqlite-shm", ".pfx", ".p12", ".key"}
LOCKS = ("requirements-lock.txt", "requirements-agent-lock.txt", "requirements-windows-lock.txt")
RESOURCES = Path("packaging/windows/resources.json")
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
PYTHON_VERSION = "3.12.10"
RUNTIME_PLATFORM = {"system": "Windows", "architecture": "x64", "implementation": "CPython"}
LOCK_COMPOSITION_OVERRIDES = {
    "typing-extensions": {
        "requirements-lock.txt": "4.15.0",
        "requirements-agent-lock.txt": "4.16.0",
        "requirements-windows-lock.txt": "4.16.0",
        "reason": "Accepted provider SDK graph requires its existing 4.16.0 pin; Windows composition is separately identified.",
    }
}
SECRET_PATTERN = re.compile(
    rb"(?:sk-[A-Za-z0-9_-]{32,}|AKIA[A-Z0-9]{16}|-----BEGIN (?:(?:RSA|EC|DSA|OPENSSH|ENCRYPTED) )?PRIVATE KEY-----)")


def contains_suspected_credential(path: Path) -> bool:
    """Inspect complete text resources without loading an unbounded file."""
    previous = b""
    with path.open("rb") as stream:
        while chunk := stream.read(64 * 1024):
            joined = previous + chunk
            if SECRET_PATTERN.search(joined):
                return True
            previous = joined[-256:]
    return False


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        result = hashlib.file_digest(stream, "sha256").hexdigest()
    return result


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def inventory(payload: Path) -> list[dict]:
    entries = []
    for path in sorted(payload.rglob("*")):
        if path.is_symlink():
            raise ValueError("Payload contains symlink")
        if not path.is_file() or path == payload / MANIFEST:
            continue
        relative = path.relative_to(payload)
        if len(relative.parts) == 1 and re.fullmatch(r"unins\d+\.(exe|dat|msg)", path.name, re.IGNORECASE):
            # Installer-owned files are outside the frozen application payload.
            continue
        lower_parts = {part.lower() for part in relative.parts}
        if (lower_parts & FORBIDDEN_PARTS or any(part.startswith(".venv") for part in lower_parts)
                or path.name.lower().startswith(".env")
                or path.suffix.lower() in FORBIDDEN_SUFFIXES):
            raise ValueError("Payload contains forbidden development or secret file: " + relative.as_posix())
        if path.suffix.lower() in {".json", ".txt", ".yaml", ".yml", ".md", ".ini", ".cfg", ".conf", ".pem", ".crt"}:
            if contains_suspected_credential(path):
                raise ValueError("Payload contains suspected credential; values suppressed")
        entries.append({"path": relative.as_posix(), "sha256": digest(path), "bytes": path.stat().st_size})
    return entries


def locked_versions(path: Path) -> dict[str, str]:
    return dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^\s\\]+)", path.read_text(encoding="utf-8"), re.MULTILINE))


def source_bound_locks(root: Path, source_sha: str) -> dict[str, str]:
    """Require checkout bytes to match Git blobs, including native line endings."""
    if re.fullmatch(r"[a-f0-9]{40}", source_sha) is None:
        raise ValueError("Source SHA must be exact Git commit identity")
    result = {}
    for name in LOCKS:
        try:
            blob = subprocess.check_output(["git", "cat-file", "blob", f"{source_sha}:{name}"],
                cwd=root, stderr=subprocess.PIPE, timeout=15)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            raise ValueError("Source dependency lock Git blob unavailable: " + name) from exc
        expected = hashlib.sha256(blob).hexdigest()
        if digest(root / name) != expected:
            raise ValueError("Checkout dependency lock differs from exact source bytes: " + name)
        result[name] = expected
    return result


def validate_native_payload(entries: list[dict], root: Path) -> None:
    paths = {entry["path"] for entry in entries}
    required = {"atlas-product.exe", "_internal/python312.dll"}
    resources = json.loads((root / RESOURCES).read_text(encoding="utf-8"))
    required.update("_internal/" + name for name in resources["files"])
    if not required.issubset(paths):
        raise ValueError("Payload missing required files: " + ", ".join(sorted(required - paths)))
    for name in ("qwindows.dll",):
        if not any(path.endswith("/" + name) for path in paths):
            raise ValueError("Qt Windows platform plugin missing")
    for package in ("pyarrow/", "duckdb", "lightgbm/"):
        if not any(package in path and path.endswith((".pyd", ".dll")) for path in paths):
            raise ValueError("Native runtime dependency missing: " + package)
    if any("nautilus" in path.lower() for path in paths):
        raise ValueError("Capital runtime accidentally bundled")


def create(payload: Path, root: Path, *, source_sha: str, version: str) -> dict:
    if not re.fullmatch(r"[a-f0-9]{40}", source_sha):
        raise ValueError("Source SHA must be exact Git commit identity")
    if (platform.system() != "Windows" or platform.machine().upper() not in {"AMD64", "X86_64"}
            or platform.python_implementation() != "CPython" or platform.python_version() != PYTHON_VERSION):
        raise ValueError("Package manifest requires pinned native Windows x64 CPython " + PYTHON_VERSION)
    source_locks = source_bound_locks(root, source_sha)
    entries = inventory(payload)
    validate_native_payload(entries, root)
    versions = locked_versions(root / "requirements-windows-lock.txt")
    installed = {}
    for name, expected in versions.items():
        actual = importlib.metadata.version(name)
        if actual != expected:
            raise ValueError("Build dependency version differs from lock: " + name)
        installed[name] = actual
    result = {
        "schema_version": 1, "artifact_type": "AtlasWindowsBuildManifestV1",
        "source_sha": source_sha, "version": version, "python_version": sys.version.split()[0],
        "runtime_platform": RUNTIME_PLATFORM,
        "runtime_lock_sha256": digest(root / "requirements-windows-lock.txt"),
        "dependency_locks": source_locks,
        "dependency_composition_overrides": LOCK_COMPOSITION_OVERRIDES,
        "build_dependencies": installed, "payload": entries,
        "payload_tree_sha256": hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "capital_enabled": False, "assisted_enabled": False,
        "runtime_boundary": "PUBLIC_RESEARCH_ZERO_AUTHORITY_INTELLIGENCE",
        "native_artifact_validation": "UNVERIFIED",
    }
    write_json(payload / MANIFEST, result)
    return result


def verify(payload: Path, *, expected_sha: str | None = None, root: Path | None = None) -> dict:
    if (payload / MANIFEST).stat().st_size > MAX_MANIFEST_BYTES:
        raise ValueError("Package manifest exceeds bounded input size")
    manifest = json.loads((payload / MANIFEST).read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != 1
            or manifest.get("artifact_type") != "AtlasWindowsBuildManifestV1"
            or manifest.get("capital_enabled") is not False or manifest.get("assisted_enabled") is not False):
        raise ValueError("Invalid package authority manifest")
    if (not isinstance(manifest.get("source_sha"), str)
            or not re.fullmatch(r"[a-f0-9]{40}", manifest["source_sha"])
            or not isinstance(manifest.get("version"), str) or not manifest["version"]
            or len(manifest["version"]) > 128):
        raise ValueError("Invalid package source/version identity")
    if manifest.get("python_version") != PYTHON_VERSION or manifest.get("runtime_platform") != RUNTIME_PLATFORM:
        raise ValueError("Invalid package interpreter/platform identity")
    locks = manifest.get("dependency_locks")
    if (not isinstance(locks, dict) or set(locks) != set(LOCKS)
            or any(not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value)
                   for value in locks.values())
            or manifest.get("runtime_lock_sha256") != locks["requirements-windows-lock.txt"]):
        raise ValueError("Invalid package dependency lock identities")
    if expected_sha is not None and manifest["source_sha"] != expected_sha:
        raise ValueError("Package source identity mismatch")
    actual = inventory(payload)
    if actual != manifest["payload"]:
        raise ValueError("Payload inventory/hash mismatch")
    expected_tree = hashlib.sha256(json.dumps(actual, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if expected_tree != manifest["payload_tree_sha256"]:
        raise ValueError("Payload tree identity mismatch")
    if root is not None:
        validate_native_payload(actual, root)
        if source_bound_locks(root, manifest["source_sha"]) != manifest["dependency_locks"]:
            raise ValueError("Source dependency lock differs from package")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("create", "verify"))
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--source-sha")
    parser.add_argument("--version")
    args = parser.parse_args()
    if args.mode == "create":
        if args.root is None or args.source_sha is None or args.version is None:
            parser.error("create requires --root --source-sha --version")
        manifest = create(args.payload, args.root, source_sha=args.source_sha, version=args.version)
    else:
        manifest = verify(args.payload, expected_sha=args.source_sha, root=args.root)
    print(json.dumps({"status": "TESTED", "check": "PACKAGE_MANIFEST", "source_sha": manifest["source_sha"],
                      "payload_files": len(manifest["payload"]), "payload_tree_sha256": manifest["payload_tree_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
