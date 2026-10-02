"""Fixed application resources in a source checkout or a frozen directory bundle."""

from __future__ import annotations

import sys
from pathlib import Path


def resource_file(relative: str) -> Path:
    name = Path(relative)
    if name.is_absolute() or ".." in name.parts:
        raise ValueError("application resource must be relative to the bundle")
    bundle = getattr(sys, "_MEIPASS", None)
    root = Path(bundle) if bundle is not None else Path(__file__).resolve().parents[3]
    resolved = (root / name).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError("application resource escapes its bundle")
    return resolved
