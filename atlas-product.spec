# Native Windows one-directory product build. Never cross-compile from Linux.
# ruff: noqa: F821 -- PyInstaller supplies the spec evaluation globals.
import json
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

root = Path(SPECPATH)
# Hidden-import discovery runs before Analysis applies pathex. The clean build
# environment deliberately does not install an editable copy of this checkout.
sys.path.insert(0, str(root / "src"))
resources = json.loads((root / "packaging/windows/resources.json").read_text(encoding="utf-8"))
datas = []
for relative in resources["files"]:
    if Path(relative).is_absolute() or ".." in Path(relative).parts:
        raise ValueError("Package resource must be inside the source tree")
    source = root / relative
    if not source.is_file() or source.is_symlink() or not source.resolve().is_relative_to(root.resolve()):
        raise ValueError("Required package resource unavailable: " + relative)
    datas.append((str(source), str(Path(relative).parent)))
for relative in resources["configuration_directories"]:
    if Path(relative).is_absolute() or ".." in Path(relative).parts:
        raise ValueError("Configuration resource must be inside the source tree")
    directory = root / relative
    if directory.exists():
        for source in sorted(directory.rglob("*.json")):
            if source.is_symlink() or not source.resolve().is_relative_to(root.resolve()):
                raise ValueError("Symlink configuration is not a package resource")
            datas.append((str(source), str(source.relative_to(root).parent)))
def runtime_data(package):
    result = []
    for source, destination in collect_data_files(package):
        path = Path(source)
        parts = {part.lower() for part in path.parts}
        if (parts & {"tests", "test", "__pycache__", "docs", "examples"}
                or path.suffix.lower() in {".pyx", ".pxd", ".pyi"}):
            continue
        result.append((source, destination))
    return result


datas += runtime_data("certifi")
datas += runtime_data("pydantic_ai")
datas += runtime_data("pydantic_graph")
def runtime_modules(package):
    return tuple(name for name in collect_submodules(package)
                 if not any(part.lower() in {"tests", "test", "examples"} for part in name.split(".")))


hidden = list(runtime_modules("atlas.v2")) + ["atlas.desktop.app"]
hidden += runtime_modules("pydantic_ai") + runtime_modules("pydantic_graph")
hidden += ["PySide6.QtCore", "PySide6.QtGui", "PySide6.QtWidgets", "pyarrow.parquet", "duckdb", "lightgbm"]
a = Analysis(
    [str(root / "src/atlas_product_entry.py")], pathex=[str(root / "src")],
    binaries=[], datas=datas, hiddenimports=hidden, hookspath=[], hooksconfig={},
    runtime_hooks=[], excludes=["pytest", "hypothesis", "mypy", "ruff", "nautilus_trader", "torch", "tensorflow"],
    noarchive=False, optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, [], exclude_binaries=True, name="atlas-product", debug=False,
    bootloader_ignore_signals=False, strip=False, upx=False, console=True,
    disable_windowed_traceback=False,
)
coll = COLLECT(exe, a.binaries, a.datas, strip=False, upx=False, name="atlas-product")
