"""Authenticated, bounded, read-only loopback IPC for desktop projections."""

from __future__ import annotations

import hmac
import ipaddress
import json
import os
import secrets
import socket
import socketserver
import stat
import struct
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .._serialization import canonical_json
from ..memory.repository import OpsRepository
from .projection import project_chart_series, project_snapshot

IPC_PROTOCOL_VERSION = 2
MAX_IPC_MESSAGE_BYTES = 1_000_000
IPC_TIMEOUT_SECONDS = 3.0
MAX_IPC_CLIENTS = 16
READ_ONLY_COMMANDS = frozenset({"ping", "health", "snapshot", "overview", "scanner", "watches", "evidence", "chart"})


class IPCProtocolError(ValueError):
    """A safe, stable client-visible IPC error."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _read_exact(sock: socket.socket, count: int) -> bytes:
    chunks: list[bytes] = []
    remaining = count
    while remaining:
        try:
            chunk = sock.recv(remaining)
        except TimeoutError as exc:
            raise IPCProtocolError("READ_TIMEOUT") from exc
        if not chunk:
            raise IPCProtocolError("TRUNCATED_FRAME")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame(sock: socket.socket, *, maximum: int = MAX_IPC_MESSAGE_BYTES) -> bytes:
    header = _read_exact(sock, 4)
    (size,) = struct.unpack("!I", header)
    if size == 0:
        raise IPCProtocolError("EMPTY_FRAME")
    if size > maximum:
        raise IPCProtocolError("MESSAGE_TOO_LARGE")
    return _read_exact(sock, size)


def _write_frame(sock: socket.socket, data: bytes, *, maximum: int = MAX_IPC_MESSAGE_BYTES) -> None:
    if not data or len(data) > maximum:
        raise IPCProtocolError("RESPONSE_TOO_LARGE")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _response(request_id: str | None, *, result: Any = None, error: str | None = None) -> bytes:
    payload: dict[str, Any] = {"protocol_version": IPC_PROTOCOL_VERSION, "request_id": request_id, "ok": error is None}
    if error is None:
        payload["result"] = result
    else:
        payload["error"] = {"code": error}
    return canonical_json(payload).encode("utf-8")


def _json_object(raw: bytes) -> Mapping[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IPCProtocolError("MALFORMED_JSON") from exc
    if not isinstance(value, Mapping):
        raise IPCProtocolError("INVALID_REQUEST")
    return value


def _valid_request_id(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 36:
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


class ProjectionService:
    """Owns a read-only SQLite handle and a local TCP server, never runtime lifecycle."""

    def __init__(
        self,
        database: str | Path,
        token: str,
        *,
        archive_root: str | Path | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        try:
            address = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError("IPC host must be a loopback IP address") from exc
        if not address.is_loopback:
            raise ValueError("IPC host must be loopback-only")
        if not isinstance(token, str) or len(token) < 32 or len(token) > 256:
            raise ValueError("IPC authentication token must be 32 to 256 characters")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("IPC port must be between 0 and 65535")
        self._token = token.encode("utf-8")
        self._repository = OpsRepository(database, read_only=True)
        self._archive_root = Path(archive_root) if archive_root is not None else None
        self._request_count = 0
        self._request_lock = threading.Lock()
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                sock: socket.socket = self.request
                sock.settimeout(IPC_TIMEOUT_SECONDS)
                peer = self.client_address[0]
                try:
                    peer_address = ipaddress.ip_address(peer)
                    if not peer_address.is_loopback:
                        raise IPCProtocolError("LOCAL_CLIENTS_ONLY")
                    try:
                        raw = _read_frame(sock)
                        request = _json_object(raw)
                    except IPCProtocolError as exc:
                        _write_frame(sock, _response(None, error=exc.code))
                        return
                    with owner._request_lock:
                        owner._request_count += 1
                    request_id = request.get("request_id")
                    correlated_id = request_id if _valid_request_id(request_id) else None
                    try:
                        result = owner._dispatch(request)
                        payload = _response(correlated_id, result=result)
                    except IPCProtocolError as exc:
                        payload = _response(correlated_id, error=exc.code)
                    except Exception:
                        payload = _response(correlated_id, error="INTERNAL_ERROR")
                    if len(payload) > MAX_IPC_MESSAGE_BYTES:
                        payload = _response(correlated_id, error="RESPONSE_TOO_LARGE")
                    _write_frame(sock, payload)
                except (OSError, IPCProtocolError):
                    return

            def finish(self) -> None:
                return None

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
            block_on_close = False
            request_queue_size = 32
            address_family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
            client_slots = threading.BoundedSemaphore(MAX_IPC_CLIENTS)

            def process_request(self, request: Any, client_address: Any) -> None:
                if not self.client_slots.acquire(blocking=False):
                    request.close()
                    return
                try:
                    super().process_request(request, client_address)
                except BaseException:
                    self.client_slots.release()
                    raise

            def process_request_thread(self, request: Any, client_address: Any) -> None:
                try:
                    super().process_request_thread(request, client_address)
                finally:
                    self.client_slots.release()

        self._server = Server((str(address), port), Handler)
        self._server.timeout = IPC_TIMEOUT_SECONDS
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    @property
    def is_alive(self) -> bool:
        return self._server.fileno() >= 0

    @property
    def request_count(self) -> int:
        with self._request_lock:
            return self._request_count

    def _dispatch(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        fields = {"protocol_version", "request_id", "request_type", "session_token", "params"}
        if set(request) != fields:
            raise IPCProtocolError("INVALID_REQUEST")
        if type(request.get("protocol_version")) is not int or request["protocol_version"] != IPC_PROTOCOL_VERSION:
            raise IPCProtocolError("UNSUPPORTED_PROTOCOL_VERSION")
        if not _valid_request_id(request.get("request_id")):
            raise IPCProtocolError("INVALID_REQUEST_ID")
        supplied = request.get("session_token")
        if not isinstance(supplied, str) or not hmac.compare_digest(supplied.encode("utf-8"), self._token):
            raise IPCProtocolError("UNAUTHORIZED")
        kind = request.get("request_type")
        if not isinstance(kind, str) or kind not in READ_ONLY_COMMANDS:
            raise IPCProtocolError("UNKNOWN_REQUEST")
        params = request.get("params")
        if not isinstance(params, Mapping):
            raise IPCProtocolError("INVALID_PARAMS")
        if kind in {"ping", "health", "snapshot", "overview", "scanner", "watches", "evidence"} and params:
            raise IPCProtocolError("INVALID_PARAMS")
        if kind == "ping":
            return {"protocol_version": IPC_PROTOCOL_VERSION, "service": "ATLAS_DESKTOP_PROJECTION_V2", "state": "READY"}
        if kind == "chart":
            return self._dispatch_chart(params)
        snapshot = project_snapshot(self._repository)
        if kind == "health":
            return {"schema_version": snapshot.schema_version, "projection_version": snapshot.projection_version,
                    "freshness_state": snapshot.freshness_state,
                    "statuses": [item.to_dict() for item in snapshot.overview.statuses]}
        if kind == "snapshot":
            return snapshot.to_dict()
        if kind == "overview":
            return snapshot.overview.to_dict()
        if kind == "scanner":
            return {"rows": [item.to_dict() for item in snapshot.scanner_rows]}
        if kind == "watches":
            return {"rows": [item.to_dict() for item in snapshot.watch_rows]}
        if kind == "evidence":
            return {"rows": [item.to_dict() for item in snapshot.evidence], "truncated": snapshot.evidence_truncated}
        raise IPCProtocolError("UNKNOWN_REQUEST")

    def _dispatch_chart(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        chart_fields = {"key_json", "interval", "information_cutoff_ns", "availability_view", "limit"}
        if set(params) - chart_fields or not {"key_json", "interval", "information_cutoff_ns"}.issubset(params):
            raise IPCProtocolError("INVALID_PARAMS")
        archive_root = self._archive_root
        try:
            chart = project_chart_series(self._repository, key_json=params["key_json"],
                interval=params["interval"], information_cutoff_ns=params["information_cutoff_ns"],
                availability_view=params.get("availability_view", "ACTUAL_SYSTEM"), archive_root=archive_root,
                limit=params.get("limit", 5000))
        except (TypeError, ValueError):
            raise IPCProtocolError("INVALID_PARAMS") from None
        result = chart.to_dict()
        if len(canonical_json(result).encode("utf-8")) > MAX_IPC_MESSAGE_BYTES - 256:
            raise IPCProtocolError("RESPONSE_TOO_LARGE")
        return result

    def serve_forever(self) -> None:
        self._server.serve_forever(poll_interval=0.2)

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("projection service already started")
        self._thread = threading.Thread(target=self.serve_forever, name="atlas-v2-projection", daemon=True)
        self._thread.start()

    def close(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(timeout=IPC_TIMEOUT_SECONDS + 1)
            self._thread = None
        self._server.server_close()
        self._repository.close()

    def __enter__(self) -> ProjectionService:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


class ProjectionClient:
    def __init__(self, host: str, port: int, token: str, *, timeout: float = IPC_TIMEOUT_SECONDS) -> None:
        address = ipaddress.ip_address(host)
        if not address.is_loopback:
            raise ValueError("desktop IPC client only connects to loopback")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("IPC client port is invalid")
        if type(timeout) not in (int, float) or not 0 < timeout <= IPC_TIMEOUT_SECONDS:
            raise ValueError("IPC timeout exceeds the configured bound")
        self.host, self.port, self._token, self.timeout = host, port, token, float(timeout)

    def request(self, request_type: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        if request_type not in READ_ONLY_COMMANDS:
            raise ValueError("request type is not in the read-only command set")
        request_id = str(uuid.uuid4())
        request = {"protocol_version": IPC_PROTOCOL_VERSION, "request_id": request_id,
                   "request_type": request_type, "session_token": self._token, "params": dict(params or {})}
        data = canonical_json(request).encode("utf-8")
        if len(data) > MAX_IPC_MESSAGE_BYTES:
            raise ValueError("IPC request exceeds the message bound")
        family = socket.AF_INET6 if ipaddress.ip_address(self.host).version == 6 else socket.AF_INET
        with socket.socket(family, socket.SOCK_STREAM) as sock:
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            _write_frame(sock, data)
            payload = _json_object(_read_frame(sock))
        if payload.get("protocol_version") != IPC_PROTOCOL_VERSION or payload.get("request_id") != request_id:
            raise IPCProtocolError("RESPONSE_CORRELATION_FAILED")
        if payload.get("ok") is not True:
            error = payload.get("error")
            code = error.get("code") if isinstance(error, Mapping) else "REMOTE_ERROR"
            raise IPCProtocolError(str(code))
        result = payload.get("result")
        if not isinstance(result, Mapping):
            raise IPCProtocolError("INVALID_RESPONSE")
        return result


def read_token(path: str | Path) -> str:
    token_path = Path(path)
    if token_path.is_symlink() or not token_path.is_file():
        raise ValueError("IPC token path must be a regular file")
    if os.name != "nt" and stat.S_IMODE(token_path.stat().st_mode) & 0o077:
        raise ValueError("IPC token file permissions must be private")
    token = token_path.read_text(encoding="utf-8").strip()
    if len(token) < 32 or len(token) > 256:
        raise ValueError("IPC token file has an invalid value")
    return token


def load_or_create_token(path: str | Path) -> str:
    token_path = Path(path)
    if token_path.exists() or token_path.is_symlink():
        return read_token(token_path)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(48)
    try:
        descriptor = os.open(token_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return read_token(token_path)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(token + "\n")
    try:
        token_path.chmod(0o600)
    except OSError:
        pass
    return token


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Read-only ATLAS V2 desktop projection service")
    parser.add_argument("--db", required=True, help="existing atlas-ops SQLite database")
    parser.add_argument("--archive-root", help="existing public observation Parquet archive")
    parser.add_argument("--token-file", required=True, help="local IPC token file (created mode 0600)")
    parser.add_argument("--host", default="127.0.0.1", help="loopback address only")
    parser.add_argument("--port", type=int, default=0, help="TCP port; 0 selects an ephemeral port")
    args = parser.parse_args()
    token = load_or_create_token(args.token_file)
    service = ProjectionService(args.db, token, archive_root=args.archive_root, host=args.host, port=args.port)
    try:
        print(f"ATLAS V2 projection service listening on {service.address[0]}:{service.address[1]}")
        service.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        service.close()


if __name__ == "__main__":
    main()
