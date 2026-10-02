"""Reject a frozen Windows payload that depends on unbundled compiler runtimes."""

from __future__ import annotations

import argparse
import ctypes
import json
import platform
import re
from pathlib import Path

import pefile


def verify_native_dependencies(payload: Path) -> dict:
    if platform.system() != "Windows":
        raise ValueError("BLOCKED BY ENVIRONMENT: PE dependency validation requires Windows")
    native = sorted(path for path in payload.rglob("*") if path.suffix.lower() in {".exe", ".dll", ".pyd"})
    bundled = {path.name.lower() for path in native}
    compiler_runtime = re.compile(
        r"(?:msvc[pr]\d+.*|vcruntime\d+.*|vccorlib\d+.*|concrt\d+.*|vcomp\d+.*|vcamp\d+.*|libiomp\d+.*|libomp.*)\.dll$",
        re.I)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetSystemDirectoryW.argtypes = (ctypes.c_wchar_p, ctypes.c_uint)
    kernel.GetSystemDirectoryW.restype = ctypes.c_uint
    buffer = ctypes.create_unicode_buffer(32768)
    if not kernel.GetSystemDirectoryW(buffer, len(buffer)):
        raise OSError("Windows system directory lookup failed")
    system = Path(buffer.value)
    missing = set()
    for path in native:
        image = pefile.PE(str(path), fast_load=True)
        try:
            image.parse_data_directories(directories=[
                pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_IMPORT"],
                pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_DELAY_IMPORT"],
            ])
            imports = [*getattr(image, "DIRECTORY_ENTRY_IMPORT", []), *getattr(image, "DIRECTORY_ENTRY_DELAY_IMPORT", [])]
            for entry in imports:
                name = entry.dll.decode("ascii").lower()
                if name in bundled or name.startswith(("api-ms-", "ext-ms-")):
                    continue
                if compiler_runtime.fullmatch(name) or not (system / name).is_file():
                    missing.add(name)
        finally:
            image.close()
    if missing:
        raise ValueError("Unbundled native dependencies: " + ", ".join(sorted(missing)))
    return {"status": "TESTED", "check": "NATIVE_DLL_DEPENDENCY_CLOSURE", "pe_files": len(native),
            "unbundled_compiler_runtimes": [], "windows_system_apis_permitted": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify_native_dependencies(args.payload), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
