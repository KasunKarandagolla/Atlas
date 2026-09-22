"""Local single-writer file lock. This does NOT fence another host; cross-host fencing remains a venue/credential operation."""
from __future__ import annotations

import fcntl
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


class WriterAlreadyActive(RuntimeError): pass
@dataclass(frozen=True)
class WriterOwnership:writer_id:str;writer_epoch:int;lock_path:str=''
class WriterLock:
    def __init__(self,path:str|Path):self.path=Path(path);self._fh:TextIO|None=None;self._ownership:WriterOwnership|None=None
    @property
    def ownership(self):return self._ownership
    def acquire(self)->WriterOwnership:
        if self._fh is not None:raise WriterAlreadyActive('writer already acquired')
        self.path.parent.mkdir(parents=True,exist_ok=True);fh=open(self.path,'a+',encoding='utf-8')  # noqa: SIM115 - lock handle must outlive this method
        try:fcntl.flock(fh.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as exc:fh.close();raise WriterAlreadyActive('writer lock unavailable') from exc
        fh.seek(0);raw=fh.read().strip();epoch=int(raw.split(':',1)[0])+1 if raw and raw.split(':',1)[0].isdigit() else 1
        wid=uuid.uuid4().hex;fh.seek(0);fh.truncate();fh.write(f'{epoch}:{wid}');fh.flush();os.fsync(fh.fileno());self._fh=fh;self._ownership=WriterOwnership(wid,epoch,str(self.path));return self._ownership
    def release(self):
        if self._fh is None:return
        try:fcntl.flock(self._fh.fileno(),fcntl.LOCK_UN)
        finally:self._fh.close();self._fh=None;self._ownership=None
