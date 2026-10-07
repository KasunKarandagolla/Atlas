"""Local single-writer file lock. This does NOT fence another host; cross-host fencing remains a venue/credential operation."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, TextIO


class WriterAlreadyActive(RuntimeError):
    pass


@dataclass(frozen=True)
class WriterOwnership:
    writer_id: str
    writer_epoch: int
    lock_path: str = ""


class WriterLock:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._fh: TextIO | None = None
        self._ownership: WriterOwnership | None = None
        self._lease_file: BinaryIO | None = None

    @property
    def ownership(self):
        return self._ownership

    def acquire(self) -> WriterOwnership:
        if self._fh is not None:
            raise WriterAlreadyActive("writer already acquired")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lease: BinaryIO | None = None
        fh: TextIO | None = None
        try:
            if os.name == "nt":
                import msvcrt

                # The immutable sidecar holds the byte-range lease. Epoch-file
                # truncation never removes the locked byte or races a startup
                # seed write from a competing process. Retain this inode.
                lease = self.path.with_name(self.path.name + ".lease").open("a+b")
                lease.seek(0, os.SEEK_END)
                if lease.tell() == 0:
                    lease.write(b"\0")
                    lease.flush()
                lease.seek(0)
                msvcrt.locking(lease.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            fh = open(self.path, "a+", encoding="utf-8")  # noqa: SIM115 - lifetime ownership handle
            if os.name != "nt":
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if fh is not None:
                fh.close()
            if lease is not None:
                lease.close()
            raise WriterAlreadyActive("writer lock unavailable") from exc
        assert fh is not None
        try:
            fh.seek(0)
            raw = fh.read().strip()
            epoch = int(raw.split(":", 1)[0]) + 1 if raw and raw.split(":", 1)[0].isdigit() else 1
            wid = uuid.uuid4().hex
            fh.seek(0)
            fh.truncate()
            fh.write(f"{epoch}:{wid}")
            fh.flush()
            os.fsync(fh.fileno())
        except Exception:
            fh.close()
            if lease is not None:
                lease.close()
            raise
        self._fh = fh
        self._lease_file = lease
        self._ownership = WriterOwnership(wid, epoch, str(self.path))
        return self._ownership

    def release(self):
        if self._fh is None:
            return
        try:
            if os.name == "nt":
                # Closing the sidecar releases the Windows byte-range lock.
                if self._lease_file is not None:
                    self._lease_file.close()
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
            self._lease_file = None
            self._ownership = None
