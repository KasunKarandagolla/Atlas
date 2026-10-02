"""Native disposable-user installer/reinstall/uninstall evidence-preservation gate."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import tempfile
from pathlib import Path

from windows_manifest import digest, write_json
from windows_smoke import smoke

APP_ID = "{7D7E9B64-241F-487A-8536-9D5AD35554B1}_is1"


def require_disposable_user() -> None:
    """Never replace a real owner's existing installation during a smoke test."""
    import winreg

    key = "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\" + APP_ID
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        for view in (winreg.KEY_WOW64_64KEY, winreg.KEY_WOW64_32KEY):
            try:
                with winreg.OpenKey(hive, key, access=winreg.KEY_READ | view):
                    raise ValueError("Existing ATLAS installation: use a disposable Windows user for this gate")
            except FileNotFoundError:
                continue


def install(installer: Path, destination: Path) -> None:
    subprocess.run([str(installer), "/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART", "/CURRENTUSER",
                    "/DIR=" + str(destination)], check=True, timeout=180)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installer", type=Path, required=True)
    parser.add_argument("--upgrade-installer", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Windows":
        parser.error("BLOCKED BY ENVIRONMENT: installer validation requires native Windows")
    require_disposable_user()
    with tempfile.TemporaryDirectory(prefix="atlas-installer-gate-") as temporary:
        root = Path(temporary)
        destination = root / "Programs/ATLAS"
        evidence = root / "ATLAS/research"
        evidence.mkdir(parents=True)
        sentinel = evidence / "preserved-owner-evidence.txt"
        sentinel.write_text("Immutable owner evidence preservation fixture\n", encoding="utf-8")
        expected = digest(sentinel)
        install(args.installer.resolve(), destination)
        initial = smoke(destination, data_root=evidence)
        stale_dependency = destination / "_internal/atlas-obsolete-upgrade-fixture.txt"
        stale_dependency.write_text("Must be removed by the upgrade\n", encoding="utf-8")
        install((args.upgrade_installer or args.installer).resolve(), destination)
        if stale_dependency.exists():
            raise ValueError("Upgrade retained an obsolete dependency outside the new payload")
        upgraded = smoke(destination, data_root=evidence)
        if args.upgrade_installer and initial["diagnostics"]["version"] == upgraded["diagnostics"]["version"]:
            raise ValueError("Cross-version upgrade fixture must use a different product version")
        if digest(sentinel) != expected:
            raise ValueError("Installer upgrade altered owner evidence")
        subprocess.run([str(destination / "unins000.exe"), "/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART"],
                       check=True, timeout=180)
        if digest(sentinel) != expected or (destination / "atlas-product.exe").exists():
            raise ValueError("Uninstall did not preserve external evidence/remove application")
        result = {"schema_version": 1, "status": "TESTED", "check": "WINDOWS_INSTALLER_LIFECYCLE",
                  "source_sha": upgraded["source_sha"], "install": initial, "reinstall": upgraded,
                  "external_evidence_preserved": True, "uninstall": "TESTED",
                  "obsolete_dependency_removed": True,
                  "cross_version_upgrade": "TESTED" if args.upgrade_installer else "UNVERIFIED",
                  "actual_fresh_windows11_laptop": "UNVERIFIED"}
    write_json(args.output, result)
    print(json.dumps({"status": "TESTED", "check": result["check"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
