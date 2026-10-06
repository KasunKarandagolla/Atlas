"""Bounded append-only descriptors for already-sealed public Arrow evidence.

Each canonical receipt has its own length, checksum and closing marker. Files
rotate at 32 MiB; the durable latest pointer changes only on rotation. A bounded
tail read detects interrupted writes instead of repairing them into continuity.
These descriptors confer no source qualification and own no database handle.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import struct
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, json_value, sha256_json, sha256_ref

MAX_JOURNAL_BYTES = 32 * 1024 * 1024
MAX_RECEIPT_BYTES = 128 * 1024
MAX_POINTER_BYTES = 4096
MAX_HEADER_BYTES = 4096
LATEST_POINTER_NAME = "latest.json"
_HEADER = struct.Struct(">8sI")
_RECORD = struct.Struct(">8sI")
_FOOTER = struct.Struct(">32sQ8s")
_HEADER_MAGIC = b"ATCAPJ1\n"
_RECORD_MAGIC = b"ATCAPR1\n"
_FOOTER_MAGIC = b"ATCAPE1\n"
_JOURNAL_NAME = re.compile(r"capture-[a-f0-9]{32}\.bin")


@dataclass(frozen=True)
class CaptureReceiptBindingV1:
    run_id: str
    configuration_hash: str
    capture_epoch: str

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.run_id):
            raise ValueError("capture receipt run identity invalid")
        sha256_ref(self.configuration_hash, field="capture configuration hash")
        sha256_ref(self.capture_epoch, field="capture epoch")

    def to_dict(self) -> dict[str, str]:
        return {"run_id": self.run_id, "configuration_hash": self.configuration_hash,
                "capture_epoch": self.capture_epoch}

    @classmethod
    def from_dict(cls, value: Any) -> CaptureReceiptBindingV1:
        if not isinstance(value, dict) or set(value) != {"run_id", "configuration_hash", "capture_epoch"}:
            raise ValueError("capture receipt binding fields invalid")
        return cls(value["run_id"], value["configuration_hash"], value["capture_epoch"])


@dataclass(frozen=True)
class CaptureReceiptLocationV1:
    journal_name: str
    offset: int
    record_length: int
    receipt_sha256: str

    def __post_init__(self) -> None:
        if (not isinstance(self.journal_name, str) or not _JOURNAL_NAME.fullmatch(self.journal_name)
                or type(self.offset) is not int or self.offset < 0
                or type(self.record_length) is not int
                or not _RECORD.size + _FOOTER.size < self.record_length <= _RECORD.size + MAX_RECEIPT_BYTES + _FOOTER.size
                or self.offset + self.record_length > MAX_JOURNAL_BYTES):
            raise ValueError("capture receipt location bounds invalid")
        sha256_ref(self.receipt_sha256, field="capture receipt checksum")

    def to_dict(self) -> dict[str, Any]:
        return {"journal_name": self.journal_name, "offset": self.offset,
                "record_length": self.record_length, "receipt_sha256": self.receipt_sha256}


@dataclass(frozen=True)
class CapturedReceiptV1:
    location: CaptureReceiptLocationV1
    receipt: Mapping[str, Any]
    binding: CaptureReceiptBindingV1


def _bounded_json(payload: bytes, *, limit: int) -> dict[str, Any]:
    if not 1 <= len(payload) <= limit:
        raise ValueError("capture journal JSON size invalid")
    value = json.loads(payload)
    if not isinstance(value, dict) or canonical_json(value).encode("utf-8") != payload:
        raise ValueError("capture journal JSON is not an exact canonical object")
    return value


def _read_header(handle: Any) -> tuple[CaptureReceiptBindingV1, int, str]:
    prefix = handle.read(_HEADER.size)
    if len(prefix) != _HEADER.size:
        raise ValueError("capture journal header incomplete")
    magic, length = _HEADER.unpack(prefix)
    if magic != _HEADER_MAGIC or not 1 <= length <= MAX_HEADER_BYTES:
        raise ValueError("capture journal header prefix invalid")
    payload = handle.read(length)
    digest = handle.read(32)
    if len(payload) != length or hashlib.sha256(payload).digest() != digest:
        raise ValueError("capture journal header checksum changed")
    body = _bounded_json(payload, limit=MAX_HEADER_BYTES)
    if set(body) != {"version", "authority", "binding"} or body["version"] != "CAPTURE_RECEIPT_JOURNAL_V1" or body["authority"] != "ZERO":
        raise ValueError("capture journal header identity invalid")
    return CaptureReceiptBindingV1.from_dict(body["binding"]), _HEADER.size + length + 32, digest.hex()


def _check_identity(binding: CaptureReceiptBindingV1, *, run_id: str, configuration_hash: str) -> None:
    if binding.run_id != run_id or binding.configuration_hash != configuration_hash:
        raise ValueError("capture journal belongs to a different run or configuration")


def read_capture_receipt(root: Path, location: CaptureReceiptLocationV1, *,
                         expected_binding: CaptureReceiptBindingV1) -> CapturedReceiptV1:
    """Verify one exact retained descriptor using bounded reads and no DB access."""
    path = root / location.journal_name
    if root.is_symlink() or path.is_symlink():
        raise ValueError("capture receipt path cannot be a symlink")
    if path.stat().st_size > MAX_JOURNAL_BYTES:
        raise ValueError("capture journal file exceeds its bound")
    with path.open("rb") as handle:
        binding, header_length, _ = _read_header(handle)
        if binding != expected_binding or location.offset < header_length:
            raise ValueError("capture receipt binding or location invalid")
        handle.seek(location.offset)
        record = handle.read(location.record_length)
    if len(record) != location.record_length:
        raise ValueError("capture receipt record incomplete")
    magic, length = _RECORD.unpack(record[:_RECORD.size])
    digest, closing_length, closing_magic = _FOOTER.unpack(record[-_FOOTER.size:])
    payload = record[_RECORD.size:-_FOOTER.size]
    if (magic != _RECORD_MAGIC or closing_magic != _FOOTER_MAGIC
            or length != len(payload) or closing_length != location.record_length
            or digest.hex() != location.receipt_sha256 or hashlib.sha256(payload).digest() != digest):
        raise ValueError("capture receipt checksum or framing changed")
    receipt = _bounded_json(payload, limit=MAX_RECEIPT_BYTES)
    if receipt.get("authority") != "ZERO":
        raise ValueError("capture receipt authority invalid")
    return CapturedReceiptV1(location, receipt, binding)


def read_last_capture_receipt(root: Path, *, expected_run_id: str,
                              expected_configuration_hash: str) -> CapturedReceiptV1 | None:
    """Read only the latest pointer/header/footer/record; an incomplete tail fails closed.

    Different capture epochs are allowed for a reopened *same* immutable run.
    The caller must reconcile the returned batch/failure with its sole SQL index.
    Missing pointer in a nonempty dedicated journal directory is not a clean run.
    """
    pointer = root / LATEST_POINTER_NAME
    if not pointer.exists():
        if root.exists() and next(root.iterdir(), None) is not None:
            raise ValueError("capture journal latest pointer missing")
        return None
    if root.is_symlink() or pointer.is_symlink():
        raise ValueError("capture journal pointer cannot be a symlink")
    with pointer.open("rb") as handle:
        wire = _bounded_json(handle.read(MAX_POINTER_BYTES + 1), limit=MAX_POINTER_BYTES)
    if set(wire) != {"pointer", "sha256"} or not isinstance(wire["pointer"], dict) or sha256_json(wire["pointer"]) != wire["sha256"]:
        raise ValueError("capture journal pointer checksum changed")
    body = wire["pointer"]
    if (set(body) != {"version", "authority", "binding", "journal_name", "header_length", "header_sha256"}
            or body["version"] != "CAPTURE_RECEIPT_JOURNAL_POINTER_V1" or body["authority"] != "ZERO"
            or not isinstance(body["journal_name"], str) or not _JOURNAL_NAME.fullmatch(body["journal_name"])):
        raise ValueError("capture journal pointer fields invalid")
    binding = CaptureReceiptBindingV1.from_dict(body["binding"])
    _check_identity(binding, run_id=expected_run_id, configuration_hash=expected_configuration_hash)
    path = root / body["journal_name"]
    if path.is_symlink():
        raise ValueError("capture journal cannot be a symlink")
    size = path.stat().st_size
    if size > MAX_JOURNAL_BYTES:
        raise ValueError("capture journal file exceeds its bound")
    with path.open("rb") as handle:
        actual_binding, header_length, header_hash = _read_header(handle)
        if (actual_binding != binding or header_length != body["header_length"]
                or header_hash != body["header_sha256"]):
            raise ValueError("capture journal pointer header binding changed")
        if size < header_length + _RECORD.size + _FOOTER.size + 1:
            raise ValueError("capture journal latest record incomplete")
        handle.seek(size - _FOOTER.size)
        digest, record_length, magic = _FOOTER.unpack(handle.read(_FOOTER.size))
    if magic != _FOOTER_MAGIC:
        raise ValueError("capture journal interrupted tail")
    location = CaptureReceiptLocationV1(path.name, size - record_length, record_length, digest.hex())
    return read_capture_receipt(root, location, expected_binding=binding)


def _replace_durable_pointer(temporary: Path, pointer: Path) -> None:
    if os.name == "nt":
        import ctypes

        windows_ctypes: Any = ctypes
        kernel = windows_ctypes.WinDLL("kernel32", use_last_error=True)
        replace = kernel.MoveFileExW
        replace.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint]
        replace.restype = ctypes.c_int
        if not replace(str(temporary), str(pointer), 0x1 | 0x8):  # REPLACE_EXISTING | WRITE_THROUGH
            raise windows_ctypes.WinError(windows_ctypes.get_last_error())
    else:
        os.replace(temporary, pointer)
        descriptor = os.open(pointer.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class CaptureReceiptJournalV1:
    """One thread owns append/rotation; prior journal bodies are never rewritten.

    A new instance validates the previous tail and starts its own new UUID file.
    Per-record fsync remains mandatory. Pointer updates are amortized by bounded
    file rotation. Any append/rotation failure terminates this writer instance.
    """

    def __init__(self, root: Path, *, binding: CaptureReceiptBindingV1) -> None:
        read_last_capture_receipt(root, expected_run_id=binding.run_id,
                                 expected_configuration_hash=binding.configuration_hash)
        self.root = root
        self.binding = binding
        self.path: Path | None = None
        self.offset = 0
        self._owner_thread: int | None = None
        self._failed = False
        self.bytes_written = 0

    def _rotate(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise ValueError("capture journal root cannot be a symlink")
        body = {"version": "CAPTURE_RECEIPT_JOURNAL_V1", "authority": "ZERO", "binding": self.binding.to_dict()}
        payload = canonical_json(body).encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        header = _HEADER.pack(_HEADER_MAGIC, len(payload)) + payload + digest
        path = self.root / f"capture-{uuid.uuid4().hex}.bin"
        with path.open("xb") as handle:
            if handle.write(header) != len(header):
                raise OSError("capture journal header write incomplete")
            handle.flush()
            os.fsync(handle.fileno())
        pointer = {"version": "CAPTURE_RECEIPT_JOURNAL_POINTER_V1", "authority": "ZERO",
                   "binding": self.binding.to_dict(), "journal_name": path.name,
                   "header_length": len(header), "header_sha256": digest.hex()}
        wire = canonical_json({"pointer": pointer, "sha256": sha256_json(pointer)}).encode("utf-8")
        if len(wire) > MAX_POINTER_BYTES:
            raise ValueError("capture journal pointer exceeds its bound")
        temporary = self.root / f"pointer-{uuid.uuid4().hex}.tmp"
        with temporary.open("xb") as handle:
            if handle.write(wire) != len(wire):
                raise OSError("capture journal pointer write incomplete")
            handle.flush()
            os.fsync(handle.fileno())
        _replace_durable_pointer(temporary, self.root / LATEST_POINTER_NAME)
        self.bytes_written += len(header) + len(wire)
        # The pointer now binds even a first-record interruption to this run;
        # restart fails closed on its incomplete tail instead of using an older one.
        self.path, self.offset = path, len(header)

    def append(self, receipt: Mapping[str, Any]) -> CaptureReceiptLocationV1:
        if self._failed:
            raise RuntimeError("capture receipt journal writer is terminal")
        if self._owner_thread is None:
            self._owner_thread = threading.get_ident()
        elif self._owner_thread != threading.get_ident():
            raise RuntimeError("capture receipt journal has one owning thread")
        payload = canonical_json(json_value(dict(receipt))).encode("utf-8")
        if not 1 <= len(payload) <= MAX_RECEIPT_BYTES or receipt.get("authority") != "ZERO":
            raise ValueError("capture receipt body exceeds its bound or authority")
        digest = hashlib.sha256(payload).digest()
        length = _RECORD.size + len(payload) + _FOOTER.size
        record = _RECORD.pack(_RECORD_MAGIC, len(payload)) + payload + _FOOTER.pack(digest, length, _FOOTER_MAGIC)
        try:
            if self.path is None or self.offset + length > MAX_JOURNAL_BYTES:
                self._rotate()
            assert self.path is not None
            if self.path.is_symlink() or self.path.stat().st_size != self.offset:
                raise ValueError("capture receipt journal prefix changed")
            location = CaptureReceiptLocationV1(self.path.name, self.offset, length, digest.hex())
            with self.path.open("ab") as handle:
                if handle.write(record) != len(record):
                    raise OSError("capture receipt journal write incomplete")
                handle.flush()
                os.fsync(handle.fileno())
            self.offset += length
            self.bytes_written += length
            if self.path.stat().st_size != self.offset:
                raise OSError("capture receipt journal size mismatch")
            return location
        except Exception:
            self._failed = True
            raise
