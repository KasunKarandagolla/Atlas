"""Linux filesystem and namespace boundary for untrusted local model code."""

from __future__ import annotations

import os
import shutil
import stat
import sys
from pathlib import Path
from typing import Protocol

from .worker_protocol import ModelProviderError


class WorkerSandboxV2(Protocol):
    def command(self, program: str) -> list[str]: ...


_SENSITIVE_NAMES = {".env", ".ssh", ".aws", ".config", "credentials", "secrets", "tokens"}
_SENSITIVE_SUFFIXES = {".db", ".sqlite", ".sqlite3", ".pem", ".key"}


def _parents(path: Path) -> list[str]:
    return [str(parent) for parent in reversed(path.parents) if parent != Path("/")]


def _validate_artifact(path: Path, runtime: Path) -> Path:
    if not path.is_absolute() or not path.exists():
        raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact must be an existing absolute path")
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact path contains a symlink")
    real = path.resolve(strict=True)
    home = Path.home().resolve()
    project = Path(__file__).resolve().parents[4]
    for broad in (Path("/"), Path("/home"), home, project, Path("/mnt/data"), Path("/tmp"), Path("/var"), runtime):
        if real == broad or (real.is_dir() and broad.is_relative_to(real)):
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact mount is too broad")

    def check_name(item: Path) -> None:
        name = item.name.lower()
        if name in _SENSITIVE_NAMES or any(word in name for word in ("credential", "secret", "private-token", "live-control")) or item.suffix.lower() in _SENSITIVE_SUFFIXES:
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact contains a protected path")

    def check_file(item: Path) -> None:
        try:
            with item.open("rb") as source:
                header = source.read(16)
        except OSError as exc:
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact cannot be inspected") from exc
        if header == b"SQLite format 3\x00":
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact contains a SQLite database")

    for component in real.parts:
        check_name(Path(component))
    if real.is_dir():
        device = real.stat().st_dev

        def walk_error(exc: OSError) -> None:
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact directory cannot be inspected") from exc

        for root, dirs, files in os.walk(real, followlinks=False, onerror=walk_error):
            for name in dirs + files:
                entry = Path(root) / name
                check_name(entry)
                if entry.is_symlink() or not (entry.is_dir() or entry.is_file()):
                    raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact contains an unsafe entry")
                if entry.stat().st_dev != device:
                    raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact crosses a filesystem mount")
                if entry.is_file():
                    check_file(entry)
    elif not real.is_file():
        raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact must be a regular file or directory")
    else:
        check_file(real)
    return real


class BubblewrapSandboxV2:
    """Deny-by-default mount view; only trusted configuration supplies artifact paths."""

    def __init__(self, approved_read_only_paths: tuple[str | Path, ...] = ()) -> None:
        if sys.platform != "linux":
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "Bubblewrap worker isolation requires Linux")
        binary = shutil.which("bwrap") or shutil.which("bubblewrap")
        if binary is None:
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "Bubblewrap executable is unavailable")
        try:
            resolved_binary = Path(binary).resolve(strict=True)
            info = resolved_binary.stat()
            runtime = Path(sys.base_prefix).resolve(strict=True)
            executable = Path(sys.executable).resolve(strict=True)
        except OSError as exc:
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "Bubblewrap or Python runtime is unavailable") from exc
        if info.st_uid != 0 or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "Bubblewrap executable is not a trusted system binary")
        self.binary = str(resolved_binary)
        self.runtime = runtime
        if not executable.is_relative_to(runtime):
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "Python executable is outside its runtime prefix")
        self.executable = str(executable)
        artifacts = tuple(_validate_artifact(Path(item), self.runtime) for item in approved_read_only_paths)
        if len(set(artifacts)) != len(artifacts):
            raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact mounts overlap")
        for index, first in enumerate(artifacts):
            for second in artifacts[index + 1:]:
                if first.is_relative_to(second) or second.is_relative_to(first):
                    raise ModelProviderError("WORKER_MOUNT_UNSAFE", "approved artifact mounts overlap")
        self.artifacts = artifacts

    def command(self, program: str) -> list[str]:
        args = [
            self.binary, "--unshare-all", "--die-with-parent", "--new-session", "--clearenv",
            "--setenv", "PATH", "/usr/bin:/bin", "--setenv", "TMPDIR", "/tmp",
            "--ro-bind", "/usr", "/usr",
        ]
        for system_dir in ("/lib", "/lib64"):
            if Path(system_dir).exists():
                args.extend(("--ro-bind", system_dir, system_dir))
        for parent in _parents(self.runtime):
            args.extend(("--dir", parent))
        args.extend(("--ro-bind", str(self.runtime), str(self.runtime)))
        args.extend(("--dir", "/artifacts"))
        for index, path in enumerate(self.artifacts):
            args.extend(("--ro-bind", str(path), f"/artifacts/{index}"))
        args.extend((
            "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
            "--remount-ro", "/", "--chdir", "/", "--",
            self.executable, "-I", "-S", "-c", program,
        ))
        return args
