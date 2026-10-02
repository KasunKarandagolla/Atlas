"""OS-owned lifetime lease for the single local V2 operational writer.

The sidecar is deliberately retained after release: deleting a lock file can
let competing processes lock different inodes. A crash releases the OS lock.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


class OpsWriterAlreadyActive(RuntimeError):
    """Another repository already owns this operational database writer."""


class OpsWriterLock:
    def __init__(self, database_path: str | Path) -> None:
        if str(database_path).startswith(("\\\\", "//")):
            raise ValueError("ops writer requires a local database, not a network share")
        database = Path(database_path).resolve()
        # Symlinks resolve to one lease name. Hard links would produce different
        # lease and WAL names for the same SQLite inode, so reject that layout.
        if database.exists() and database.stat().st_nlink > 1:
            raise ValueError("ops writer database cannot have hard-link aliases")
        self.path = database.with_name(database.name + ".writer.lock")
        self._file: BinaryIO | None = None

    def acquire(self) -> None:
        if self._file is not None:
            raise RuntimeError("ops writer lock is already acquired")
        file = self.path.open("a+b")
        try:
            # Windows byte-range locks require a byte to exist at offset zero.
            if os.name == "nt":
                import msvcrt

                file.seek(0, os.SEEK_END)
                if file.tell() == 0:
                    file.write(b"\0")
                    file.flush()
                file.seek(0)
                # Linux type stubs omit these Windows-only standard-library APIs.
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            else:
                import fcntl

                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            file.close()
            raise OpsWriterAlreadyActive("ops database writer is already active or its lease is unavailable") from exc
        except BaseException:
            file.close()
            raise
        self._file = file

    def close(self) -> None:
        file, self._file = self._file, None
        if file is not None:
            # Closing the descriptor releases both flock and Windows byte locks.
            file.close()
