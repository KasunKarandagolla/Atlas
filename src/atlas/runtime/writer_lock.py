"""Local single-writer/fencing skeleton (Phase 1).

Scope (explicit):
- Provides a process-local/file ownership guard: one active writer per lock path.
- Duplicate local startup is rejected with WriterAlreadyActive.
- Writer epoch is monotonically represented (monotonic integer per acquisition;
  persisted next-epoch counter alongside the lock file).
- If ownership cannot be proven, runtime must enter new-risk-disabled state
  (see health.new_risk_allowed requiring writer_owned=True).

LIMITATION (must remain documented): a local file lock does NOT fence another
physical host. Cross-host replacement requires the old host stopped AND its
credential revoked or otherwise externally fenced (freeze §1.4, §1.6, §1.8 test 20).
A writer-epoch value in ATLAS does not cause Bybit to reject an old writer.
No active-active failover is implemented.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


class WriterAlreadyActive(RuntimeError):
    pass


@dataclass(frozen=True)
class WriterOwnership:
    writer_id: str
    writer_epoch: int
    lock_path: str


class WriterLock:
    """Exclusive file guard. Create-once semantics: O_CREAT|O_EXCL."""

    def __init__(self, lock_path: str | Path) -> None:
        self._lock_path = Path(lock_path)
        self._fd: int | None = None
        self._ownership: WriterOwnership | None = None

    @property
    def ownership(self) -> WriterOwnership | None:
        return self._ownership

    def _read_next_epoch(self) -> int:
        epoch_path = self._lock_path.with_suffix(self._lock_path.suffix + ".epoch")
        try:
            raw = epoch_path.read_text(encoding="utf-8").strip()
            return int(raw) + 1
        except (FileNotFoundError, ValueError):
            return 1

    def _write_next_epoch(self, epoch: int) -> None:
        epoch_path = self._lock_path.with_suffix(self._lock_path.suffix + ".epoch")
        epoch_path.write_text(str(epoch), encoding="utf-8")

    def acquire(self) -> WriterOwnership:
        if self._fd is not None:
            raise WriterAlreadyActive("writer already acquired in this process")
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        writer_id = uuid.uuid4().hex
        try:
            fd = os.open(str(self._lock_path), flags, 0o600)
        except FileExistsError as exc:
            raise WriterAlreadyActive(
                f"duplicate local writer denied: lock exists at {self._lock_path}. "
                "NOTE: this local lock does NOT fence another host; "
                "cross-host fencing requires credential revocation."
            ) from exc
        try:
            epoch = self._read_next_epoch()
            payload = f"{writer_id}:{epoch}:{time.time_ns()}:{os.getpid()}\n"
            os.write(fd, payload.encode("utf-8"))
            self._write_next_epoch(epoch)
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                self._lock_path.unlink()
            except OSError:
                pass
            raise
        self._fd = fd
        self._ownership = WriterOwnership(
            writer_id=writer_id, writer_epoch=epoch, lock_path=str(self._lock_path)
        )
        return self._ownership

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            os.close(self._fd)
        finally:
            self._fd = None
            self._ownership = None
            try:
                self._lock_path.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self) -> WriterOwnership:
        return self.acquire()

    def __exit__(self, *args: object) -> None:
        self.release()
