"""Fixed-purpose, path-based preparation process for retained public evidence.

The worker has only PARSE and SEAL operations. Requests carry small immutable
bindings and file paths; frame arrays and Arrow tables never cross a process
pipe. It has no repository handle, provider client, or mutable decision state.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import multiprocessing
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .._serialization import canonical_json, sha256_json, sha256_ref

PREPARATION_VERSION_V2 = "PUBLIC_EVIDENCE_PREPARATION_V2"
MAX_PREPARATION_INPUT_BYTES_V2 = 4 * 1024 * 1024
MAX_PREPARATION_RESULT_BYTES_V2 = 4 * 1024 * 1024
MAX_PREPARATION_TRADE_ROWS_V2 = 64
MAX_PREPARATION_FRAMES_V2 = 16
MAX_PREPARATION_OUTPUT_RECORDS_V2 = 512
MAX_PREPARATION_SEAL_CHUNKS_V2 = 16
MAX_PREPARATION_WATCHDOG_SECONDS_V2 = 1.5
MAX_PREPARATION_STARTUP_SECONDS_V2 = 15.0


@dataclass(frozen=True)
class PublicEvidencePreparationRequestV2:
    kind: Literal["PARSE", "SEAL"]
    job_id: str
    run_id: str
    run_root: str
    descriptor_hash: str
    input_hash: str
    input_path: str
    output_path: str
    plan_hash: str
    product_hash: str
    frame_ordinal: int
    trade_ordinal_start: int
    trade_ordinal_end: int
    preparation_version: str = PREPARATION_VERSION_V2
    processed_at_ns: int = 0
    namespace: str = ""
    chunk_id: str = ""
    floor_ns: int = 0
    clock_at_ns: int = 0

    def __post_init__(self) -> None:
        if self.kind not in ("PARSE", "SEAL"):
            raise ValueError("preparation kind must be PARSE or SEAL")
        for name in ("job_id", "run_id", "preparation_version"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be nonempty")
        for name in ("descriptor_hash", "input_hash", "plan_hash", "product_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("frame_ordinal", "trade_ordinal_start", "trade_ordinal_end", "processed_at_ns",
                     "floor_ns", "clock_at_ns"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.trade_ordinal_end < self.trade_ordinal_start:
            raise ValueError("preparation trade ordinal range is invalid")
        if self.kind == "PARSE":
            if self.trade_ordinal_end - self.trade_ordinal_start > MAX_PREPARATION_TRADE_ROWS_V2:
                raise ValueError("parse preparation slice exceeds 64 trade rows")
        else:
            if self.namespace != "ops-l2-frames" or not self.chunk_id:
                raise ValueError("seal preparation requires a frozen L2 archive namespace and chunk")
            sha256_ref(self.chunk_id, field="chunk_id")
        _require_path(self.run_root, field="run_root")
        _require_path(self.input_path, field="input_path")
        _require_path(self.output_path, field="output_path")
        root = Path(self.run_root).resolve()
        inp, out = Path(self.input_path).resolve(), Path(self.output_path).resolve()
        if not inp.is_relative_to(root) or not out.is_relative_to(root):
            raise ValueError("preparation input/output paths must remain under the bound run root")

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Any) -> PublicEvidencePreparationRequestV2:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("preparation request has unexpected fields")
        return cls(**value)

    @property
    def request_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class PublicEvidencePreparationResultV2:
    job_id: str
    request_hash: str
    output_path: str
    output_hash: str
    output_bytes: int
    result_records: int
    execution_duration_ns: int
    completed_at_ns: int

    def __post_init__(self) -> None:
        if not self.job_id.strip():
            raise ValueError("preparation result job ID is empty")
        sha256_ref(self.request_hash, field="request_hash")
        sha256_ref(self.output_hash, field="output_hash")
        for name in ("output_bytes", "result_records", "execution_duration_ns", "completed_at_ns"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.output_bytes > MAX_PREPARATION_RESULT_BYTES_V2:
            raise ValueError("preparation result exceeds the 4 MiB result bound")
        _require_path(self.output_path, field="output_path")

    @classmethod
    def from_dict(cls, value: Any) -> PublicEvidencePreparationResultV2:
        fields = set(cls.__dataclass_fields__)
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("preparation result has unexpected fields")
        return cls(**value)


class PublicEvidencePreparationError(RuntimeError):
    pass


class PublicEvidencePreparationTimeout(PublicEvidencePreparationError):
    pass


def _require_path(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field} must be a nonempty filesystem path")


def _durable_replace(path: Path, payload: bytes) -> None:
    if len(payload) > MAX_PREPARATION_RESULT_BYTES_V2:
        raise ValueError("preparation output exceeds the 4 MiB result bound")
    if path.is_symlink():
        raise ValueError("preparation output cannot replace a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        if handle.write(payload) != len(payload):
            raise OSError("preparation output write incomplete")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    if os.name != "nt":
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _replace_rebuildable_input(path: Path, payload: bytes) -> None:
    """Atomically publish a worker input that can be rebuilt from sealed raw.

    These files contain only derived requests/Arrow batches. Their source
    transport extent is already durable, and neither a stage cursor nor a
    public artifact is advanced until the worker result has been validated.
    A crash may therefore lose this directory entry without losing evidence:
    recovery rebuilds the request from the immutable transport extent. Keep
    the atomic rename and digest binding, but avoid a second data and directory
    fsync for this disposable work file.
    """
    if len(payload) > MAX_PREPARATION_INPUT_BYTES_V2:
        raise ValueError("preparation input exceeds the 4 MiB request-file bound")
    if path.is_symlink():
        raise ValueError("preparation input cannot replace a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        if handle.write(payload) != len(payload):
            raise OSError("preparation input write incomplete")
        handle.flush()
    os.replace(temporary, path)


def write_preparation_input(path: str | Path, value: dict[str, Any]) -> tuple[str, int]:
    """Write a canonical bounded private input file and return its digest/size."""
    target = Path(path)
    payload = canonical_json(value).encode("utf-8")
    if len(payload) > MAX_PREPARATION_INPUT_BYTES_V2:
        raise ValueError("preparation input exceeds the 4 MiB request-file bound")
    _replace_rebuildable_input(target, payload)
    return hashlib.sha256(payload).hexdigest(), len(payload)


def write_preparation_blob(path: str | Path, payload: bytes) -> tuple[str, int]:
    """Durably stage one bounded Arrow IPC input for a SEAL request."""
    if not isinstance(payload, bytes) or len(payload) > MAX_PREPARATION_INPUT_BYTES_V2:
        raise ValueError("preparation blob exceeds the 4 MiB input-file bound")
    _replace_rebuildable_input(Path(path), payload)
    return hashlib.sha256(payload).hexdigest(), len(payload)


def read_preparation_input(request: PublicEvidencePreparationRequestV2) -> dict[str, Any]:
    path = Path(request.input_path)
    if path.is_symlink() or path.stat().st_size > MAX_PREPARATION_INPUT_BYTES_V2:
        raise ValueError("preparation input file violates its size/path bound")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != request.input_hash:
        raise ValueError("preparation input bytes or hash mismatch")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("preparation input body is not an object")
    return value


def read_preparation_result(
    request: PublicEvidencePreparationRequestV2,
    result: PublicEvidencePreparationResultV2,
) -> dict[str, Any]:
    if result.job_id != request.job_id or result.request_hash != request.request_hash:
        raise ValueError("preparation result request binding mismatch")
    if Path(result.output_path).resolve() != Path(request.output_path).resolve():
        raise ValueError("preparation result path binding mismatch")
    path = Path(result.output_path)
    if path.is_symlink():
        raise ValueError("preparation result cannot be a symlink")
    payload = path.read_bytes()
    if (len(payload) != result.output_bytes or len(payload) > MAX_PREPARATION_RESULT_BYTES_V2
            or hashlib.sha256(payload).hexdigest() != result.output_hash):
        raise ValueError("preparation result bytes or hash mismatch")
    value = json.loads(payload)
    if not isinstance(value, dict) or value.get("request_hash") != request.request_hash:
        raise ValueError("preparation result body binding mismatch")
    if value.get("job_id") != request.job_id:
        raise ValueError("preparation result job binding mismatch")
    if (type(value.get("result_records")) is not int
            or value["result_records"] != result.result_records):
        raise ValueError("preparation result record count mismatch")
    return value


def decode_prepared_events(value: dict[str, Any]) -> tuple[Any, ...]:
    """Rebuild the fixed set of parse event types from a verified worker file."""
    from decimal import Decimal

    from ..instruments import InstrumentKeyV2
    from .microstructure import (
        AggressiveTradeV2,
        BookLevelV2,
        L2DeltaV2,
        L2SequenceFaultV2,
        L2SnapshotV2,
    )

    rows = value.get("events")
    if not isinstance(rows, list) or len(rows) > MAX_PREPARATION_TRADE_ROWS_V2:
        raise ValueError("prepared event block exceeds its 64 record slice")
    events = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"kind", "body"} or not isinstance(row["body"], dict):
            raise ValueError("prepared event row shape is invalid")
        body = row["body"]
        body["instrument"] = InstrumentKeyV2.from_dict(body["instrument"])
        if row["kind"] in ("L2SnapshotV2", "L2DeltaV2"):
            body["bids"] = tuple(BookLevelV2(Decimal(item["price"]), Decimal(item["quantity"]))
                                  for item in body["bids"])
            body["asks"] = tuple(BookLevelV2(Decimal(item["price"]), Decimal(item["quantity"]))
                                  for item in body["asks"])
        if row["kind"] == "L2SnapshotV2":
            event = L2SnapshotV2(**{key: value for key, value in body.items() if key != "schema_version"})
        elif row["kind"] == "L2DeltaV2":
            event = L2DeltaV2(**{key: value for key, value in body.items() if key != "schema_version"})
        elif row["kind"] == "L2SequenceFaultV2":
            event = L2SequenceFaultV2(**body)
        elif row["kind"] == "AggressiveTradeV2":
            event = AggressiveTradeV2(**{key: value for key, value in body.items() if key != "schema_version"})
        else:
            raise ValueError("prepared worker returned an unknown event type")
        events.append(event)
    return tuple(events)


class PublicEvidencePreparationWorkerV2:
    """One spawned worker with one in-flight request and one completion slot."""

    def __init__(self, *, start_method: str = "spawn") -> None:
        self._context = multiprocessing.get_context(start_method)
        self._process: multiprocessing.Process | None = None
        self._connection: Any | None = None
        self._request: PublicEvidencePreparationRequestV2 | None = None
        self._completion: PublicEvidencePreparationResultV2 | None = None
        self._submitted_at = 0.0
        self._job_started_at: float | None = None
        self._wait_timeout = MAX_PREPARATION_WATCHDOG_SECONDS_V2
        self._run_root: str | None = None

    @property
    def busy(self) -> bool:
        return self._request is not None or self._completion is not None

    def submit(self, request: PublicEvidencePreparationRequestV2) -> None:
        if self.busy:
            raise RuntimeError("preparation worker already has an in-flight job or completion")
        self.start(request.run_root)
        assert self._connection is not None
        message = canonical_json({"request": request.to_dict(), "request_hash": request.request_hash}).encode()
        if len(message) > 16 * 1024:
            raise ValueError("preparation IPC request header exceeds its strict bound")
        self._connection.send_bytes(message)
        self._request = request
        self._submitted_at = time.monotonic()
        self._job_started_at = None

    def start(self, run_root: str | Path) -> None:
        """Spawn and warm the fixed worker before any measured service call."""
        root = str(Path(run_root).resolve())
        if self._process is not None and self._process.is_alive() and self._run_root == root:
            return
        self._terminate()
        self._start(root)
        assert self._connection is not None and self._process is not None
        deadline = time.monotonic() + MAX_PREPARATION_STARTUP_SECONDS_V2
        while time.monotonic() < deadline:
            if self._connection.poll(0.01):
                try:
                    ready = json.loads(self._connection.recv_bytes(16 * 1024))
                except (EOFError, OSError, json.JSONDecodeError) as exc:
                    self._terminate()
                    raise PublicEvidencePreparationError("preparation worker warmup pipe failed") from exc
                if ready != {"phase": "READY", "run_root": root}:
                    self._terminate()
                    raise PublicEvidencePreparationError("preparation worker warmup binding mismatch")
                return
            if not self._process.is_alive():
                code = self._process.exitcode
                self._terminate()
                raise PublicEvidencePreparationError(f"preparation worker exited during warmup (exit={code})")
        self._terminate()
        raise PublicEvidencePreparationTimeout(
            f"preparation worker warmup exceeded {MAX_PREPARATION_STARTUP_SECONDS_V2:g} s")

    def poll(self) -> PublicEvidencePreparationResultV2 | None:
        if self._request is None:
            return self._completion
        if self._process is None or self._connection is None:
            raise PublicEvidencePreparationError("preparation worker process is unavailable")
        if not self._process.is_alive() and not self._connection.poll():
            request = self._request
            self._request = None
            raise PublicEvidencePreparationError(
                f"preparation worker exited before completion (exit={self._process.exitcode}, job={request.job_id})")
        if (self._job_started_at is None
                and time.monotonic() - self._submitted_at > MAX_PREPARATION_STARTUP_SECONDS_V2):
            self._terminate()
            request = self._request
            self._request = None
            raise PublicEvidencePreparationTimeout(
                f"preparation worker startup exceeded {MAX_PREPARATION_STARTUP_SECONDS_V2:g} s: {request.job_id}")
        if (self._job_started_at is not None
                and time.monotonic() - self._job_started_at > MAX_PREPARATION_WATCHDOG_SECONDS_V2):
            self._terminate()
            request = self._request
            self._request = None
            raise PublicEvidencePreparationTimeout(f"preparation worker exceeded 1.5 s watchdog: {request.job_id}")
        if not self._connection.poll(0):
            return None
        try:
            message = json.loads(self._connection.recv_bytes(16 * 1024))
        except (EOFError, OSError) as exc:
            request = self._request
            self._request = None
            raise PublicEvidencePreparationError(
                f"preparation worker pipe closed before completion: {request.job_id}") from exc
        request = self._request
        assert request is not None
        if message.get("phase") == "STARTED":
            if message.get("request_hash") != request.request_hash or message.get("job_id") != request.job_id:
                raise PublicEvidencePreparationError("preparation start acknowledgement binding mismatch")
            self._job_started_at = time.monotonic()
            return None
        self._request = None
        if message.get("request_hash") != request.request_hash or message.get("job_id") != request.job_id:
            raise PublicEvidencePreparationError("preparation completion binding mismatch")
        if message.get("error"):
            raise PublicEvidencePreparationError(str(message["error"]))
        result = PublicEvidencePreparationResultV2.from_dict(message.get("result"))
        if result.output_path != request.output_path:
            raise PublicEvidencePreparationError("preparation output path differs from request")
        self._completion = result
        return result

    def take_completion(self) -> PublicEvidencePreparationResultV2:
        if self._completion is None:
            raise RuntimeError("preparation completion is not ready")
        result = self._completion
        self._completion = None
        self._job_started_at = None
        return result

    def wait(self, request: PublicEvidencePreparationRequestV2,
             *, timeout_s: float = MAX_PREPARATION_WATCHDOG_SECONDS_V2) -> PublicEvidencePreparationResultV2:
        if self._request != request and self._completion is None:
            self.submit(request)
        self._wait_timeout = min(timeout_s, MAX_PREPARATION_WATCHDOG_SECONDS_V2)
        while True:
            result = self.poll()
            if (self._job_started_at is not None
                    and time.monotonic() - self._job_started_at >= self._wait_timeout):
                self._terminate()
                self._request = None
                raise PublicEvidencePreparationTimeout(f"preparation worker exceeded watchdog: {request.job_id}")
            if result is not None:
                return self.take_completion()
            time.sleep(0.001)

    def terminate_for_test(self) -> None:
        """Deterministic worker-crash seam; production callers use close()."""
        if self._process is not None:
            if self._process.is_alive():
                self._process.terminate()
            self._process.join(timeout=1.0)

    def close(self) -> None:
        if self._connection is not None and self._process is not None and self._process.is_alive():
            try:
                self._connection.send_bytes(b'{"command":"STOP"}')
            except (BrokenPipeError, EOFError, OSError):
                pass
            self._process.join(timeout=0.1)
        self._terminate()

    def _start(self, run_root: str) -> None:
        parent, child = self._context.Pipe(duplex=True)
        process = self._context.Process(target=_worker_main, args=(child, run_root),
                                        name="atlas-public-evidence-preparation", daemon=True)
        process.start()
        child.close()
        self._process, self._connection = process, parent
        self._run_root = str(Path(run_root).resolve())

    def _terminate(self) -> None:
        if self._process is not None:
            if self._process.is_alive():
                self._process.terminate()
            self._process.join(timeout=1.0)
            self._process.close()
        if self._connection is not None:
            self._connection.close()
        self._process = self._connection = None
        self._run_root = None
        self._completion = None


def _worker_main(connection: Any, initial_run_root: str) -> None:
    seal_writer = None
    seal_root = Path(initial_run_root).resolve() / "ops-public-extents"
    parse_cache: dict[tuple[str, int, str], Any] = {}
    try:
        # Import the fixed parser/sealer dependencies before READY so module
        # import time is startup cost, outside the 1.5 s bounded job watchdog.
        importlib.import_module("..instruments", package=__package__)
        importlib.import_module(".microstructure", package=__package__)
        importlib.import_module(".public_microstructure_ws", package=__package__)

        try:
            importlib.import_module("pyarrow")
            importlib.import_module("pyarrow.ipc")
        except ModuleNotFoundError:
            # PARSE remains usable in narrow development environments. A SEAL
            # job reports the missing fixed dependency as a bounded failure.
            pass
        else:
            importlib.import_module(".public_archive_extents", package=__package__)
        connection.send_bytes(canonical_json({"phase": "READY",
                                               "run_root": str(Path(initial_run_root).resolve())}).encode())
        while True:
            try:
                message = json.loads(connection.recv_bytes(16 * 1024))
            except EOFError:
                return
            if message.get("command") == "STOP":
                return
            request = PublicEvidencePreparationRequestV2.from_dict(message.get("request"))
            request_hash = request.request_hash
            if (message.get("request_hash") != request_hash
                    or Path(request.run_root).resolve() != Path(initial_run_root).resolve()):
                connection.send_bytes(canonical_json({"job_id": request.job_id,
                    "request_hash": request_hash, "error": "PREPARATION_REQUEST_BINDING_INVALID"}).encode())
                continue
            connection.send_bytes(canonical_json({"phase": "STARTED", "job_id": request.job_id,
                                                   "request_hash": request_hash}).encode())
            try:
                job_started_ns = time.monotonic_ns()
                if request.kind != "PARSE":
                    parse_cache.clear()
                result_value = _execute_request(request, seal_writer, seal_root, parse_cache)
                if request.kind == "SEAL":
                    seal_writer = result_value.pop("_writer")
                output = {"request_hash": request_hash, **result_value}
                payload = canonical_json(output).encode("utf-8")
                if len(payload) > MAX_PREPARATION_RESULT_BYTES_V2:
                    raise ValueError("preparation result exceeds 4 MiB")
                _durable_replace(Path(request.output_path), payload)
                result = PublicEvidencePreparationResultV2(
                    request.job_id, request_hash, request.output_path, hashlib.sha256(payload).hexdigest(),
                    len(payload), int(output.get("result_records", 0)),
                    time.monotonic_ns() - job_started_ns, time.time_ns(),
                )
                connection.send_bytes(canonical_json({"job_id": request.job_id,
                    "request_hash": request_hash, "result": result.__dict__}).encode())
            except BaseException as exc:
                connection.send_bytes(canonical_json({"job_id": request.job_id,
                    "request_hash": request_hash,
                    "error": f"{type(exc).__name__}:{exc}"[:400]}).encode())
    finally:
        connection.close()


def _execute_request(request: PublicEvidencePreparationRequestV2, seal_writer: Any,
                     seal_root: Path, parse_cache: dict[tuple[str, int, str], Any]) -> dict[str, Any]:
    input_path = Path(request.input_path)
    if input_path.is_symlink() or input_path.stat().st_size > MAX_PREPARATION_INPUT_BYTES_V2:
        raise ValueError("preparation input file violates its size/path bound")
    raw_input = input_path.read_bytes()
    if hashlib.sha256(raw_input).hexdigest() != request.input_hash:
        raise ValueError("preparation input hash mismatch")
    if request.kind == "PARSE":
        value = json.loads(raw_input)
        if (not isinstance(value, dict)
                or set(value) not in ({"frame", "instrument", "processed_at_ns", "source_health",
                                      "source_health_ref"}, {"frames"})):
            raise ValueError("parse input shape is invalid")
        output = _parse_input(request, value, parse_cache)
        return output
    return _seal_input(request, raw_input, seal_writer, seal_root)


def _parse_input(request: PublicEvidencePreparationRequestV2, value: dict[str, Any],
                 parse_cache: dict[tuple[str, int, str], Any]) -> dict[str, Any]:
    from ..instruments import VenueV2

    if "frames" in value:
        rows = value["frames"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_PREPARATION_FRAMES_V2:
            raise ValueError("parse manifest must contain 1..16 consecutive frames")
        manifest = rows
    else:
        manifest = [{
            "frame_ordinal": request.frame_ordinal,
            "frame": value["frame"],
            "instrument": value["instrument"],
            "processed_at_ns": value["processed_at_ns"],
            "source_health": value["source_health"],
            "source_health_ref": value["source_health_ref"],
            "trade_ordinal_start": request.trade_ordinal_start,
        }]
    if not manifest or not isinstance(manifest[0], dict):
        raise ValueError("parse manifest first frame is invalid")
    first = manifest[0]
    if (first.get("frame_ordinal") != request.frame_ordinal
            or first.get("trade_ordinal_start") != request.trade_ordinal_start):
        raise ValueError("parse manifest start cursor differs from request")
    first_frame = first.get("frame")
    if not isinstance(first_frame, dict):
        raise ValueError("parse manifest first raw frame is invalid")
    first_is_bybit_trade = (
        first_frame.get("venue") == VenueV2.BYBIT.value
        and isinstance(first_frame.get("channel"), str)
        and first_frame["channel"].startswith("publicTrade.")
    )
    canonical_end = (request.trade_ordinal_start + MAX_PREPARATION_TRADE_ROWS_V2
                     if first_is_bybit_trade else 1)
    if request.trade_ordinal_end != canonical_end:
        raise ValueError("parse request cursor end is not canonical")

    started = time.monotonic_ns()
    trade_rows_remaining = MAX_PREPARATION_TRADE_ROWS_V2
    frame_results: list[dict[str, Any]] = []
    event_total = 0
    for index, frame_input in enumerate(manifest):
        if not isinstance(frame_input, dict) or set(frame_input) != {
            "frame_ordinal", "frame", "instrument", "processed_at_ns", "source_health",
            "source_health_ref", "trade_ordinal_start",
        }:
            raise ValueError("parse manifest frame fields are invalid")
        frame_ordinal = frame_input["frame_ordinal"]
        trade_start = frame_input["trade_ordinal_start"]
        if (type(frame_ordinal) is not int or frame_ordinal != request.frame_ordinal + index
                or type(trade_start) is not int
                or trade_start != (request.trade_ordinal_start if index == 0 else 0)):
            raise ValueError("parse manifest frame ordinals are not consecutive")
        if index and time.monotonic_ns() - started >= 50_000_000:
            break
        frame_value = frame_input["frame"]
        is_bybit_trade = (
            isinstance(frame_value, dict) and frame_value.get("venue") == VenueV2.BYBIT.value
            and isinstance(frame_value.get("channel"), str)
            and frame_value["channel"].startswith("publicTrade.")
        )
        is_binance_trade = (
            isinstance(frame_value, dict) and frame_value.get("venue") == VenueV2.BINANCE.value
            and isinstance(frame_value.get("channel"), str)
            and frame_value["channel"].endswith("@aggTrade")
        )
        if trade_rows_remaining == 0 and (is_bybit_trade or is_binance_trade):
            break
        frame_result = _parse_one_frame(
            request, frame_input, frame_ordinal=frame_ordinal,
            trade_ordinal_start=trade_start,
            bybit_row_limit=trade_rows_remaining,
            started_at_ns=started,
            parse_cache=parse_cache,
        )
        frame_results.append(frame_result)
        event_total += len(frame_result["events"])
        if event_total > MAX_PREPARATION_OUTPUT_RECORDS_V2:
            raise ValueError("parse manifest exceeds 512 output records")
        if frame_result["is_bybit_trade"]:
            trade_rows_remaining -= len(frame_result["trade_payload_hashes"])
            if frame_result["trade_ordinal_end"] < frame_result["trade_total"]:
                break
        elif is_binance_trade:
            trade_rows_remaining -= len(frame_result["trade_payload_hashes"])
        if trade_rows_remaining < 0:
            raise ValueError("parse manifest exceeds 64 total trade rows")
        if time.monotonic_ns() - started >= 50_000_000:
            break

    if not frame_results:
        raise ValueError("parse manifest made no frame progress")
    first_result = frame_results[0]
    output_frames = [{key: value for key, value in result.items() if key != "is_bybit_trade"}
                     for result in frame_results]
    return {
        "version": PREPARATION_VERSION_V2,
        "job_id": request.job_id,
        "descriptor_hash": request.descriptor_hash,
        "input_hash": request.input_hash,
        "plan_hash": request.plan_hash,
        "frame_ordinal": request.frame_ordinal,
        "trade_ordinal_start": request.trade_ordinal_start,
        "trade_ordinal_end": first_result["trade_ordinal_end"],
        "trade_total": first_result["trade_total"],
        "frame_results": output_frames,
        "result_records": event_total,
    }


def _parse_one_frame(
    request: PublicEvidencePreparationRequestV2,
    value: dict[str, Any],
    *,
    frame_ordinal: int,
    trade_ordinal_start: int,
    bybit_row_limit: int,
    started_at_ns: int,
    parse_cache: dict[tuple[str, int, str], Any],
) -> dict[str, Any]:
    from ..instruments import InstrumentKeyV2, VenueV2
    from .microstructure import L2DeltaV2, L2SequenceFaultV2, L2SnapshotV2
    from .public_microstructure_ws import (
        CapturedPublicFrameV2,
        parse_binance_aggtrade,
        parse_binance_depth_frame,
        parse_bybit_orderbook_frame,
        parse_bybit_trades,
    )

    frame_value = value["frame"]
    if not isinstance(frame_value, dict) or set(frame_value) != {
        "venue", "source_id", "channel", "raw_payload_b64", "raw_payload_hash", "received_at_ns",
        "available_at_ns", "connection_epoch",
    }:
        raise ValueError("parse frame binding fields are invalid")
    raw = base64.b64decode(frame_value["raw_payload_b64"], validate=True)
    if hashlib.sha256(raw).hexdigest() != frame_value["raw_payload_hash"]:
        raise ValueError("parse frame raw payload hash mismatch")
    frame = CapturedPublicFrameV2(
        VenueV2(frame_value["venue"]), frame_value["source_id"], frame_value["channel"], raw,
        frame_value["raw_payload_hash"], frame_value["received_at_ns"], frame_value["available_at_ns"],
        frame_value["connection_epoch"],
    )
    key = InstrumentKeyV2.from_dict(value["instrument"])
    processed_at_ns = value["processed_at_ns"]
    if (key.content_hash == "" or type(processed_at_ns) is not int or processed_at_ns < 0
            or (frame_ordinal == request.frame_ordinal
                and (key.content_hash != request.product_hash
                     or processed_at_ns != request.processed_at_ns))):
        raise ValueError("parse product or logical-time binding mismatch")
    kwargs = {"instrument": key, "processed_at_ns": processed_at_ns,
              "source_health": value["source_health"], "source_health_ref": value["source_health_ref"]}
    cache_key = (request.descriptor_hash, frame_ordinal, frame.raw_payload_hash)
    decoded = parse_cache.get(cache_key)
    if decoded is None:
        parse_cache.clear()
        decoded = json.loads(raw)
        parse_cache[cache_key] = decoded
    exact_row_hashes: list[str] = []
    requested_trade_end = 1
    actual_trade_end = 1
    is_bybit_trade = frame.venue == VenueV2.BYBIT and frame.channel.startswith("publicTrade.")
    if frame.venue == VenueV2.BYBIT and frame.channel.startswith("publicTrade."):
        rows = decoded.get("data")
        if (not isinstance(rows, list) or trade_ordinal_start > len(rows)
                or type(bybit_row_limit) is not int or bybit_row_limit < 0):
            raise ValueError("Bybit exact trade ordinal slice is outside the raw frame")
        requested_trade_end = min(trade_ordinal_start + bybit_row_limit, len(rows))
        selected = rows[trade_ordinal_start:requested_trade_end]
        # Do not copy the full decoded data array once per trade. A large raw
        # frame is parsed once and each bounded slice reuses only its small
        # outer envelope plus the selected row.
        frame_envelope = {name: item for name, item in decoded.items() if name != "data"}
        parsed_events = []
        for row in selected:
            parsed_events.extend(parse_bybit_trades(
                frame, **kwargs, _decoded_payload={**frame_envelope, "data": [row]},
            ))
            exact_row_hashes.append(sha256_json(row))
            if time.monotonic_ns() - started_at_ns >= 50_000_000:
                break
        actual_trade_end = trade_ordinal_start + len(exact_row_hashes)
        events = tuple(parsed_events)
        total = len(rows)
    elif frame.venue == VenueV2.BYBIT and frame.channel.startswith("orderbook.50."):
        events = (parse_bybit_orderbook_frame(frame, **kwargs, _decoded_payload=decoded),)
        total = 1
    elif frame.venue == VenueV2.BINANCE and "@depth" in frame.channel:
        events = (parse_binance_depth_frame(frame, **kwargs, _decoded_payload=decoded),)
        total = 1
    elif frame.venue == VenueV2.BINANCE and frame.channel.endswith("@aggTrade"):
        event = parse_binance_aggtrade(frame, **kwargs, _decoded_payload=decoded)
        row = decoded.get("data", decoded)
        exact_row_hashes = [sha256_json(row)]
        events, total = (event,), 1
    else:
        raise ValueError("parse worker received an unplanned frame channel")
    encoded_events = []
    for event in events:
        if isinstance(event, L2SnapshotV2):
            kind = "L2SnapshotV2"
        elif isinstance(event, L2DeltaV2):
            kind = "L2DeltaV2"
        elif isinstance(event, L2SequenceFaultV2):
            kind = "L2SequenceFaultV2"
        else:
            kind = "AggressiveTradeV2"
        if hasattr(event, "to_dict"):
            body = event.to_dict()
        else:
            body = {name: getattr(event, name) for name in (
                "instrument", "source_id", "channel", "fault", "event_at_ns", "received_at_ns",
                "available_at_ns", "raw_content_ref", "source_health", "source_health_ref")}
            body["instrument"] = event.instrument.to_dict()
        encoded_events.append({"kind": kind, "body": body})
    return {
        "frame_ordinal": frame_ordinal,
        "venue": frame.venue.value,
        "source_id": frame.source_id,
        "channel": frame.channel,
        "raw_payload_hash": frame.raw_payload_hash,
        "received_at_ns": frame.received_at_ns,
        "available_at_ns": frame.available_at_ns,
        "connection_epoch": frame.connection_epoch,
        "product_hash": key.content_hash,
        "source_health": value["source_health"],
        "source_health_ref": value["source_health_ref"],
        "processed_at_ns": processed_at_ns,
        "trade_ordinal_start": trade_ordinal_start,
        "trade_ordinal_requested_end": requested_trade_end,
        "trade_ordinal_end": actual_trade_end,
        "trade_total": total,
        "is_bybit_trade": is_bybit_trade,
        "events": encoded_events,
        "trade_payload_hashes": exact_row_hashes,
    }


def _seal_input(request: PublicEvidencePreparationRequestV2, raw_input: bytes,
                seal_writer: Any, seal_root: Path) -> dict[str, Any]:
    import pyarrow as pa

    if seal_writer is None:
        from .public_archive_extents import PublicArchiveSegmentWriterV1

        seal_writer = PublicArchiveSegmentWriterV1(seal_root)
    table = pa.ipc.open_stream(pa.BufferReader(raw_input)).read_all()
    if (not 1 <= table.num_rows <= MAX_PREPARATION_OUTPUT_RECORDS_V2
            or table.nbytes > MAX_PREPARATION_RESULT_BYTES_V2):
        raise ValueError("seal table exceeds 512-row/4 MiB chunk bound")

    batch_columns = {"_atlas_chunk_id", "_atlas_floor_ns"}
    if batch_columns.issubset(table.column_names):
        if (len(table.column_names) < 3
                or table.num_rows > MAX_PREPARATION_OUTPUT_RECORDS_V2):
            raise ValueError("seal batch columns or row bound are invalid")
        grouped: dict[str, tuple[int, list[dict[str, Any]]]] = {}
        for row in table.to_pylist():
            chunk_id = row.pop("_atlas_chunk_id")
            floor_ns = row.pop("_atlas_floor_ns")
            if (not isinstance(chunk_id, str) or len(chunk_id) != 64
                    or any(character not in "0123456789abcdef" for character in chunk_id)
                    or type(floor_ns) is not int or floor_ns < 0):
                raise ValueError("seal batch chunk identity/floor is invalid")
            prior = grouped.get(chunk_id)
            if prior is None:
                grouped[chunk_id] = (floor_ns, [row])
            elif prior[0] != floor_ns:
                raise ValueError("seal batch repeats one chunk with conflicting availability")
            else:
                prior[1].append(row)
        chunk_ids = tuple(grouped)
        batch_id = sha256_json({"version": "PUBLIC_ARCHIVE_SEAL_BATCH_V1",
                                "chunk_ids": list(chunk_ids)})
        if (not 1 <= len(chunk_ids) <= MAX_PREPARATION_SEAL_CHUNKS_V2
                or request.chunk_id != batch_id
                or request.floor_ns != max(floor for floor, _rows in grouped.values())):
            raise ValueError("seal batch request does not bind its chunk set")
        base_schema = table.drop(["_atlas_chunk_id", "_atlas_floor_ns"]).schema

        def clock() -> int:
            return request.clock_at_ns

        specs = tuple((pa.Table.from_pylist(rows, schema=base_schema), request.namespace,
                       chunk_id, clock, floor_ns)
                      for chunk_id, (floor_ns, rows) in grouped.items())
        if any(not 1 <= chunk_table.num_rows <= MAX_PREPARATION_OUTPUT_RECORDS_V2
               or chunk_table.nbytes > MAX_PREPARATION_RESULT_BYTES_V2
               for chunk_table, *_ in specs):
            raise ValueError("seal batch contains an oversized chunk")
        entries = seal_writer.seal_many(specs)
        if len(entries) != len(chunk_ids):
            raise RuntimeError("seal worker did not produce every requested extent")
        extents = [{"artifact_ref": entry.artifact_ref, "artifact_type": entry.artifact_type,
                    "content_hash": entry.content_hash, "created_at_ns": entry.created_at_ns,
                    "available_at_ns": entry.available_at_ns, "metadata": dict(entry.metadata)}
                   for entry in entries]
        return {"version": PREPARATION_VERSION_V2, "job_id": request.job_id,
                "descriptor_hash": request.descriptor_hash, "input_hash": request.input_hash,
                "plan_hash": request.plan_hash, "product_hash": request.product_hash,
                "frame_ordinal": request.frame_ordinal,
                "trade_ordinal_start": request.trade_ordinal_start,
                "trade_ordinal_end": request.trade_ordinal_end,
                "chunk_id": request.chunk_id, "chunk_ids": list(chunk_ids),
                "result_records": table.num_rows, "extents": extents,
                "seal_metrics": dict(seal_writer.metrics), "_writer": seal_writer}

    # Retain the single-chunk request format for older private work records and
    # focused archive compatibility tests.
    def clock() -> int:
        return request.clock_at_ns

    entries = seal_writer.seal_many(((table, request.namespace, request.chunk_id, clock, request.floor_ns),))
    if len(entries) != 1:
        raise RuntimeError("seal worker did not produce exactly one extent descriptor")
    entry = entries[0]
    return {"version": PREPARATION_VERSION_V2, "job_id": request.job_id,
            "descriptor_hash": request.descriptor_hash, "input_hash": request.input_hash,
            "plan_hash": request.plan_hash, "product_hash": request.product_hash,
            "frame_ordinal": request.frame_ordinal, "trade_ordinal_start": request.trade_ordinal_start,
            "trade_ordinal_end": request.trade_ordinal_end, "result_records": table.num_rows,
            "extent": {"artifact_ref": entry.artifact_ref, "artifact_type": entry.artifact_type,
                       "content_hash": entry.content_hash, "created_at_ns": entry.created_at_ns,
                       "available_at_ns": entry.available_at_ns, "metadata": dict(entry.metadata)},
            "seal_metrics": dict(seal_writer.metrics),
            "_writer": seal_writer}
