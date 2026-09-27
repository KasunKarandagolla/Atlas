# Minimal one-folder PyInstaller build for the read-only Qt observer.
a = Analysis(
    ["src/atlas_desktop_entry.py"],
    pathex=["src"],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="atlas-desktop",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
)

# Keep the projection service as its own console process in the same release bundle.
projection = Analysis(
    ["src/atlas_v2_projection_entry.py"],
    pathex=["src"],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
projection_pyz = PYZ(projection.pure)
projection_exe = EXE(
    projection_pyz,
    projection.scripts,
    [],
    exclude_binaries=True,
    name="atlas-v2-projection",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
)
coll = COLLECT(
    exe,
    projection_exe,
    a.binaries,
    a.datas,
    projection.binaries,
    projection.datas,
    strip=False,
    upx=False,
    name="atlas-desktop",
)
