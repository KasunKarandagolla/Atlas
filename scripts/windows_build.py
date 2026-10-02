"""Clean native Windows build; dependency installation belongs to the build host."""

from __future__ import annotations

import argparse
import ast
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

from windows_manifest import create, digest, verify, write_json

PYTHON_VERSION = "3.12.10"
INNO_VERSION = "6.5.4"


def run(command: list[str], *, root: Path, capture: bool = False) -> str:
    result = subprocess.run(command, cwd=root, check=True, text=True, capture_output=capture, timeout=1800)
    return result.stdout.strip() if capture else ""


def product_version(root: Path) -> str:
    module = ast.parse((root / "src/atlas/__init__.py").read_text(encoding="utf-8"))
    for item in module.body:
        if isinstance(item, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__version__" for t in item.targets):
            return str(ast.literal_eval(item.value))
    raise ValueError("Package version missing")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iscc", type=Path, required=True)
    parser.add_argument("--installer-version", default="2.0.36.0")
    parser.add_argument("--signed-release", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if platform.system() != "Windows" or platform.machine().upper() not in {"AMD64", "X86_64"}:
        parser.error("Native Windows x64 required; Linux cross-build is not qualified")
    if platform.python_version() != PYTHON_VERSION:
        parser.error("Build requires pinned Python " + PYTHON_VERSION)
    if not re.fullmatch(r"\d+\.\d+\.\d+\.\d+", args.installer_version):
        parser.error("Installer version must contain four numeric components")
    if run(["git", "status", "--porcelain"], root=root, capture=True):
        parser.error("Build requires a clean committed checkout, including untracked files")
    source_sha = run(["git", "rev-parse", "HEAD"], root=root, capture=True)
    os.environ["SOURCE_DATE_EPOCH"] = run(["git", "show", "-s", "--format=%ct", "HEAD"], root=root, capture=True)
    os.environ["PYTHONHASHSEED"] = "0"
    inno_help = subprocess.run([str(args.iscc), "/?"], text=True, capture_output=True, timeout=15)
    if f"{INNO_VERSION}" not in inno_help.stdout + inno_help.stderr:
        parser.error("Inno Setup compiler version differs from pinned " + INNO_VERSION)
    run([sys.executable, "-m", "pip", "check"], root=root)
    run([sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "atlas-product.spec"], root=root)
    payload = root / "dist/atlas-product"
    native_dependencies = json.loads(run(
        [sys.executable, str(root / "scripts/windows_pe_dependencies.py"), "--payload", str(payload)],
        root=root, capture=True))
    write_json(root / "dist/windows-native-dependencies.json", native_dependencies)
    if args.signed_release:
        if not os.environ.get("ATLAS_SIGNING_PFX_BASE64") or not os.environ.get("ATLAS_SIGNING_PASSWORD"):
            parser.error("Signed release requires protected CI signing credentials")
        run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
             str(root / "packaging/windows/sign.ps1"), "-Path", str(payload / "atlas-product.exe")], root=root)
    manifest = create(payload, root, source_sha=source_sha, version=product_version(root))
    verify(payload, expected_sha=source_sha, root=root)
    run([sys.executable, str(root / "scripts/windows_smoke.py"), "--payload", str(payload),
         "--source-sha", source_sha, "--output", str(root / "dist/windows-native-smoke.json")], root=root)
    command = [str(args.iscc), f"/DProductVersion={args.installer_version}", f"/DSourceSHA={source_sha[:12]}",
               f"/DPayloadDir={payload}", f"/DOutputDir={root / 'dist'}"]
    if args.signed_release:
        command += ["/DSignTool=atlas", "/Satlas=powershell -NoProfile -ExecutionPolicy Bypass -File "
                    + '"' + str(root / "packaging/windows/sign.ps1") + '" -Path $f']
    command.append(str(root / "packaging/windows/atlas.iss"))
    run(command, root=root)
    installers = list((root / "dist").glob(f"ATLAS-{args.installer_version}-{source_sha[:12]}-win11-x64-setup.exe"))
    if len(installers) != 1:
        raise ValueError("Expected exactly one versioned installer")
    installer = installers[0]
    if args.signed_release:
        signature = run(["powershell", "-NoProfile", "-Command",
                         "(Get-AuthenticodeSignature -LiteralPath '" + str(installer).replace("'", "''") + "').Status"],
                        root=root, capture=True)
        if signature != "Valid":
            raise ValueError("Installer Authenticode signature is not valid")
    write_json(root / "dist/windows-release-manifest.json", {
        "schema_version": 1, "source_sha": source_sha, "version": manifest["version"],
        "installer_version": args.installer_version, "installer": installer.name,
        "installer_sha256": digest(installer), "runtime_lock_sha256": manifest["runtime_lock_sha256"],
        "payload_tree_sha256": manifest["payload_tree_sha256"], "signed_release": args.signed_release,
        "authenticode_signature": "TESTED" if args.signed_release else "UNVERIFIED",
        "native_offline_smoke": "TESTED", "actual_windows11_hardware": "UNVERIFIED",
        "live_public_runtime": "TEST GATE", "capital_enabled": False,
    })
    print(json.dumps({"status": "TESTED", "check": "NATIVE_WINDOWS_BUILD", "source_sha": source_sha,
                      "installer": installer.name, "signed_release": args.signed_release}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
