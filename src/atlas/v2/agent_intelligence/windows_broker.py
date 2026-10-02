"""Current-user Windows named-pipe transport for the tool-free shadow critic.

Only bounded JSON bytes cross authenticated AF_PIPE connections. No proposer,
tool dispatch, arbitrary endpoint or provider selection is exposed here.
Native pipe creation uses a protected current-user DACL and rejects remote
clients. Actual Windows behavior is a separate native validation gate.
"""

from __future__ import annotations

import ctypes
import json
import os
import re
import secrets
import struct
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from multiprocessing import AuthenticationError, connection
from typing import Any

from .._serialization import canonical_json, strict_fields
from .broker import (
    BROKER_PROTOCOL_VERSION,
    MAX_BROKER_CONNECTIONS,
    MAX_BROKER_FRAME_BYTES,
    BrokerProtocolError,
    InferenceBroker,
    _result_from_wire,
    _result_wire,
)
from .contracts import ActionAssessmentRequestV2, ProviderResultV1, SealedActionAssessmentPacketV1
from .controller import DirectActionAssessmentBrokerPort

PIPE_NAME_RE = re.compile(r"^\\\\\.\\pipe\\AtlasCritic-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
PIPE_TIMEOUT_SECONDS_V1 = 35.0


class _ProcessFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


def _owner_process_kernel() -> Any:
    if os.name != "nt":
        raise RuntimeError("Windows owner process binding requires native Windows")
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(_ProcessFileTime)] * 4)]
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def _process_creation_ticks(kernel: Any, handle: int) -> int:
    creation, exited, kernel_time, user_time = (_ProcessFileTime() for _ in range(4))
    if not kernel.GetProcessTimes(handle, ctypes.byref(creation), ctypes.byref(exited),
                                  ctypes.byref(kernel_time), ctypes.byref(user_time)):
        raise OSError("Windows owner process creation identity unavailable")
    ticks = (creation.high << 32) | creation.low
    if ticks <= 0:
        raise ValueError("Windows owner process creation identity invalid")
    return ticks


def _owner_identity_for_pid(pid: int, *, _kernel: Any = None) -> dict[str, int]:
    if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF:
        raise ValueError("Windows owner PID invalid")
    kernel = _owner_process_kernel() if _kernel is None else _kernel
    handle = kernel.OpenProcess(0x00100000 | 0x1000, False, pid)
    if not handle:
        raise OSError("Windows owner process unavailable")
    try:
        return {"pid": pid, "creation_filetime_ticks": _process_creation_ticks(kernel, handle)}
    finally:
        kernel.CloseHandle(handle)


def current_owner_identity() -> dict[str, int]:
    """Bind the controller's current process instance, including PID reuse protection."""
    return _owner_identity_for_pid(os.getpid())


class WindowsOwnerProcessV1:
    """Retain an OS handle to the exact controller instance; never infer PID liveness."""

    def __init__(self, identity: Mapping[str, Any], *, _kernel: Any = None) -> None:
        fields = {"pid", "creation_filetime_ticks"}
        row = strict_fields(identity, expected=fields, required=fields, name="WindowsOwnerProcessV1")
        if (type(row["pid"]) is not int or not 0 < row["pid"] <= 0xFFFFFFFF
                or type(row["creation_filetime_ticks"]) is not int
                or not 0 < row["creation_filetime_ticks"] <= 0xFFFFFFFFFFFFFFFF):
            raise ValueError("Windows owner process identity invalid")
        self._kernel = _owner_process_kernel() if _kernel is None else _kernel
        self._handle = self._kernel.OpenProcess(0x00100000 | 0x1000, False, row["pid"])
        if not self._handle:
            raise OSError("Windows owner process unavailable")
        try:
            if _process_creation_ticks(self._kernel, self._handle) != row["creation_filetime_ticks"]:
                raise ValueError("Windows owner process creation identity differs")
            if not self.alive():
                raise OSError("Windows owner process already stopped")
        except BaseException:
            self.close()
            raise

    def alive(self) -> bool:
        if not self._handle:
            return False
        result = self._kernel.WaitForSingleObject(self._handle, 0)
        if result == 258:  # WAIT_TIMEOUT: the exact process is still running.
            return True
        if result == 0:  # WAIT_OBJECT_0: the retained process instance has exited.
            return False
        raise OSError("Windows owner process wait failed")

    def close(self) -> None:
        handle, self._handle = self._handle, None
        if handle:
            self._kernel.CloseHandle(handle)

    def __enter__(self) -> WindowsOwnerProcessV1:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()


def _pipe_name(value: str) -> str:
    if not isinstance(value, str) or PIPE_NAME_RE.fullmatch(value) is None:
        raise ValueError("critic endpoint must be an ATLAS local named pipe")
    return value


def _json_object(raw: bytes) -> Mapping[str, Any]:
    if not raw or len(raw) > MAX_BROKER_FRAME_BYTES:
        raise BrokerProtocolError("FRAME_SIZE_INVALID")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise BrokerProtocolError("INVALID_BROKER_REQUEST")
            value[key] = item
        return value

    def invalid_constant(_value: str) -> Any:
        raise BrokerProtocolError("INVALID_BROKER_REQUEST")

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid_constant)
    if not isinstance(value, Mapping):
        raise BrokerProtocolError("INVALID_BROKER_REQUEST")
    return value


def _response(broker: Any, raw: bytes) -> bytes:
    """Reuse signed critic validation; the research inference operation is absent."""
    try:
        data = _json_object(raw)
        fields = {"protocol_version", "request_id", "command", "capability", "authorization_id",
                  "attempt_id", "request", "packet"}
        row = strict_fields(data, expected=fields, required=fields, name="ActionAssessmentBrokerRequestV1")
        if (type(row["protocol_version"]) is not int or row["protocol_version"] != BROKER_PROTOCOL_VERSION
                or row["command"] != "assess_action_v1" or not isinstance(row["request"], Mapping)
                or not isinstance(row["packet"], Mapping)
                or not isinstance(row["request_id"], str)
                or str(uuid.UUID(row["request_id"])) != row["request_id"]):
            raise BrokerProtocolError("BROKER_OPERATION_UNSUPPORTED")
        if any(not isinstance(row[key], str) or not row[key]
               for key in ("capability", "authorization_id", "attempt_id")):
            raise BrokerProtocolError("INVALID_BROKER_REQUEST")
        result = broker.assess_action_v1(capability=row["capability"], authorization_id=row["authorization_id"],
            attempt_id=row["attempt_id"], request_data=row["request"], packet_data=row["packet"])
        if not isinstance(result, ProviderResultV1):
            raise BrokerProtocolError("INVALID_PROVIDER_RESULT")
        body = {"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": row["request_id"],
                "ok": True, "result": _result_wire(result)}
        encoded = canonical_json(body).encode("utf-8")
        if len(encoded) > MAX_BROKER_FRAME_BYTES:
            raise BrokerProtocolError("FRAME_SIZE_INVALID")
        return encoded
    except Exception as exc:
        code = exc.code if isinstance(exc, BrokerProtocolError) else "BROKER_PROTOCOL_ERROR"
        return canonical_json({"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": None,
                               "ok": False, "error": code}).encode("utf-8")


class _DeadlineBytesConnection:
    """Authentication also uses bounded bytes and a fixed connection deadline."""

    def __init__(self, channel: Any, deadline: float) -> None:
        self.channel = channel
        self.deadline = deadline

    def send_bytes(self, raw: bytes) -> None:
        if not raw or len(raw) > MAX_BROKER_FRAME_BYTES:
            raise BrokerProtocolError("FRAME_SIZE_INVALID")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("critic pipe deadline elapsed")
        self.channel.send_bytes(raw)

    def recv_bytes(self, maxlength: int | None = None) -> bytes:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0 or not self.channel.poll(remaining):
            raise TimeoutError("critic pipe deadline elapsed")
        raw = self.channel.recv_bytes(min(maxlength or MAX_BROKER_FRAME_BYTES, MAX_BROKER_FRAME_BYTES))
        if not raw or len(raw) > MAX_BROKER_FRAME_BYTES:
            raise BrokerProtocolError("FRAME_SIZE_INVALID")
        return raw


def _authenticate(channel: _DeadlineBytesConnection, key: bytes, *, server: bool) -> None:
    # These stdlib helpers use send_bytes/recv_bytes, never object unpickling.
    if server:
        connection.deliver_challenge(channel, key)  # type: ignore[arg-type]
        connection.answer_challenge(channel, key)  # type: ignore[arg-type]
    else:
        connection.answer_challenge(channel, key)  # type: ignore[arg-type]
        connection.deliver_challenge(channel, key)  # type: ignore[arg-type]


class _WindowsPipeSecurity:
    """Explicit native security attributes; no dependency on token default DACL."""

    def __init__(self) -> None:
        if os.name != "nt":
            raise RuntimeError("Windows named-pipe transport requires native Windows")
        from ctypes import wintypes

        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        self.advapi = ctypes.WinDLL("advapi32", use_last_error=True)  # type: ignore[attr-defined]
        self.kernel.GetCurrentProcess.restype = wintypes.HANDLE
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        self.kernel.CloseHandle.restype = wintypes.BOOL
        self.kernel.LocalFree.argtypes = [ctypes.c_void_p]
        self.kernel.LocalFree.restype = ctypes.c_void_p
        self.kernel.CancelIoEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
        self.kernel.CancelIoEx.restype = wintypes.BOOL
        self.advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
        self.advapi.OpenProcessToken.restype = wintypes.BOOL
        self.advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                                  wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
        self.advapi.GetTokenInformation.restype = wintypes.BOOL
        self.advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        self.advapi.ConvertSidToStringSidW.restype = wintypes.BOOL
        self.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
            wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.DWORD)]
        self.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = wintypes.BOOL
        self.advapi.GetSecurityInfo.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.DWORD,
            ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p)]
        self.advapi.GetSecurityInfo.restype = wintypes.DWORD
        self.advapi.GetAce.argtypes = [ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p)]
        self.advapi.GetAce.restype = wintypes.BOOL
        self.advapi.GetSecurityDescriptorControl.argtypes = [ctypes.c_void_p,
            ctypes.POINTER(wintypes.WORD), ctypes.POINTER(wintypes.DWORD)]
        self.advapi.GetSecurityDescriptorControl.restype = wintypes.BOOL
        token = wintypes.HANDLE()
        if not self.advapi.OpenProcessToken(self.kernel.GetCurrentProcess(), 0x0008, ctypes.byref(token)):
            raise OSError("Windows process token unavailable")
        try:
            size = wintypes.DWORD()
            self.advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if not 0 < size.value <= 65_536:
                raise OSError("Windows user token size invalid")
            info = ctypes.create_string_buffer(size.value)
            if not self.advapi.GetTokenInformation(token, 1, info, size, ctypes.byref(size)):
                raise OSError("Windows user token unavailable")
            sid = ctypes.cast(info, ctypes.POINTER(ctypes.c_void_p))[0]
            self.user_sid = self._sid_string(sid)
        finally:
            self.kernel.CloseHandle(token)

    def _sid_string(self, sid: Any) -> str:
        value = ctypes.c_void_p()
        if not self.advapi.ConvertSidToStringSidW(sid, ctypes.byref(value)):
            raise OSError("Windows SID conversion failed")
        try:
            return ctypes.wstring_at(value)
        finally:
            self.kernel.LocalFree(value)

    def create_pipe(self, name: str, *, first: bool) -> int:
        from ctypes import wintypes

        class SecurityAttributes(ctypes.Structure):
            _fields_ = [("nLength", wintypes.DWORD), ("lpSecurityDescriptor", ctypes.c_void_p),
                        ("bInheritHandle", wintypes.BOOL)]

        descriptor = ctypes.c_void_p()
        sddl = f"D:P(A;;GA;;;{self.user_sid})"
        if not self.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1,
                ctypes.byref(descriptor), None):
            raise OSError("Windows current-user pipe DACL creation failed")
        attributes = SecurityAttributes(ctypes.sizeof(SecurityAttributes), descriptor, False)
        self.kernel.CreateNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
            wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(SecurityAttributes)]
        self.kernel.CreateNamedPipeW.restype = wintypes.HANDLE
        try:
            # Duplex, overlapped, first-instance collision refusal; message mode,
            # message reads, blocking IO and rejection of every remote client.
            access = 0x00000003 | 0x40000000 | (0x00080000 if first else 0)
            handle = self.kernel.CreateNamedPipeW(name, access, 0x00000004 | 0x00000002 | 0x00000008,
                # Four handlers plus the newly accepted saturation peer and its
                # replacement pending accept. The peer is rejected immediately.
                MAX_BROKER_CONNECTIONS + 2, 65_536, 65_536, 500, ctypes.byref(attributes))
        finally:
            self.kernel.LocalFree(descriptor)
        if handle is None or handle == ctypes.c_void_p(-1).value:
            raise OSError("Windows local critic pipe creation failed")
        try:
            self.verify_user_only(handle)
        except BaseException:
            self.kernel.CloseHandle(handle)
            raise
        return int(handle)

    def verify_user_only(self, handle: int) -> None:
        from ctypes import wintypes

        dacl = ctypes.c_void_p()
        descriptor = ctypes.c_void_p()
        # SE_KERNEL_OBJECT=6, DACL_SECURITY_INFORMATION=4.
        error = self.advapi.GetSecurityInfo(handle, 6, 4, None, None, ctypes.byref(dacl), None,
                                            ctypes.byref(descriptor))
        if error != 0:
            raise OSError("Windows critic pipe DACL inspection failed")
        try:
            control, revision = wintypes.WORD(), wintypes.DWORD()
            if (not self.advapi.GetSecurityDescriptorControl(descriptor, ctypes.byref(control), ctypes.byref(revision))
                    or not control.value & 0x1000):  # SE_DACL_PROTECTED
                raise OSError("Windows critic pipe DACL must reject inherited permissions")
            if not dacl.value or struct.unpack_from("<H", ctypes.string_at(dacl, 8), 4)[0] != 1:
                raise OSError("Windows critic pipe must allow only its current user")
            ace = ctypes.c_void_p()
            if not self.advapi.GetAce(dacl, 0, ctypes.byref(ace)):
                raise OSError("Windows critic pipe ACE inspection failed")
            header = ctypes.string_at(ace, 8)
            if header[0] != 0 or header[1] != 0 or not ace.value:
                raise OSError("Windows critic pipe contains unexpected permissions")
            if self._sid_string(ace.value + 8) != self.user_sid:
                raise OSError("Windows critic pipe permits another user")
        finally:
            self.kernel.LocalFree(descriptor)


class _NativePipeListener:
    """Cancelable overlapped native accept producing stdlib PipeConnection."""

    def __init__(self, name: str) -> None:
        self.name = _pipe_name(name)
        self.security = _WindowsPipeSecurity()
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._pending = self.security.create_pipe(name, first=True)

    def accept(self) -> Any:
        import _winapi  # type: ignore[import-not-found]

        native_api: Any = _winapi  # Win32-only stdlib API is absent from Linux type stubs.

        with self._lock:
            if self._closed.is_set():
                raise OSError("critic pipe is closed")
            handle = self._pending
        try:
            operation = native_api.ConnectNamedPipe(handle, overlapped=True)
            while native_api.WaitForSingleObject(operation.event, 200) == 258:
                if self._closed.is_set():
                    operation.cancel()
                    raise OSError("critic pipe is closed")
            _, error = operation.GetOverlappedResult(True)
            if error != 0:
                raise OSError("critic pipe accept failed")
            with self._lock:
                if self._closed.is_set():
                    raise OSError("critic pipe is closed")
                self._pending = self.security.create_pipe(self.name, first=False)
            return connection.PipeConnection(handle)  # type: ignore[attr-defined]
        except BaseException:
            # The listener retains ownership of this pending handle on failure.
            raise

    def close(self) -> None:
        self._closed.set()
        with self._lock:
            handle, self._pending = self._pending, 0
            if handle:
                self.security.kernel.CancelIoEx(handle, None)
                self.security.kernel.CloseHandle(handle)


class WindowsActionCriticBrokerServer:
    """Bounded local broker; only signed direct action assessments are accepted."""

    def __init__(self, pipe_name: str, broker: InferenceBroker, *, authentication_key: bytes,
                 _listener_factory: Callable[[str], Any] = _NativePipeListener) -> None:
        self.pipe_name = _pipe_name(pipe_name)
        if not isinstance(authentication_key, bytes) or len(authentication_key) < 32:
            raise ValueError("critic transport authentication requires 256 bits")
        self._broker = broker
        self._authkey = authentication_key
        self._listener_factory = _listener_factory
        self._listener: Any = None
        self._slots = threading.BoundedSemaphore(MAX_BROKER_CONNECTIONS)
        self._connections: set[Any] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: str | None = None
        self._rejected_connections = 0

    @property
    def active_handlers(self) -> int:
        with self._lock:
            return len(self._connections)

    def start(self) -> None:
        if self._thread is not None or self._stop.is_set():
            raise RuntimeError("critic pipe server lifecycle cannot be reused")
        self._listener = self._listener_factory(self.pipe_name)
        self._thread = threading.Thread(target=self._serve, name="atlas-critic-pipe", daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                channel = self._listener.accept()
            except (OSError, EOFError):
                if not self._stop.is_set():
                    self._failure = "BROKER_UNAVAILABLE"
                return
            if self._stop.is_set():
                channel.close()
                return
            if not self._slots.acquire(blocking=False):
                # Reject before authentication: a saturated peer gets an explicit
                # transport failure without any provider call or handler thread.
                self._rejected_connections += 1
                channel.close()
                continue
            with self._lock:
                self._connections.add(channel)
            try:
                threading.Thread(target=self._handle, args=(channel,), daemon=True).start()
            except BaseException:
                channel.close()
                with self._lock:
                    self._connections.discard(channel)
                self._slots.release()
                self._failure = "BROKER_UNAVAILABLE"
                return

    def _handle(self, channel: Any) -> None:
        watchdog = threading.Timer(PIPE_TIMEOUT_SECONDS_V1, channel.close)
        watchdog.daemon = True
        watchdog.start()
        bounded = _DeadlineBytesConnection(channel, time.monotonic() + PIPE_TIMEOUT_SECONDS_V1)
        try:
            _authenticate(bounded, self._authkey, server=True)
            if self._stop.is_set():
                return
            bounded.send_bytes(_response(self._broker, bounded.recv_bytes()))
        except (OSError, EOFError, TimeoutError, AuthenticationError, BrokerProtocolError):
            pass
        finally:
            watchdog.cancel()
            channel.close()
            with self._lock:
                self._connections.discard(channel)
            self._slots.release()

    def health(self) -> dict[str, Any]:
        return {"status": "TEST GATE" if self._failure is not None else "IMPLEMENTED",
                "failure_code": self._failure, "transport": "AF_PIPE", "active_handlers": self.active_handlers,
                "max_handlers": MAX_BROKER_CONNECTIONS, "closed": self._stop.is_set(),
                "saturated_connections": self._rejected_connections}

    def close(self) -> None:
        self._stop.set()
        if self._listener is not None:
            self._listener.close()
        with self._lock:
            channels = tuple(self._connections)
        for channel in channels:
            channel.close()
        if self._thread is not None:
            self._thread.join(timeout=2)


class WindowsActionCriticClientPort(DirectActionAssessmentBrokerPort):
    def __init__(self, pipe_name: str, *, authentication_key: bytes,
                 _connect: Callable[..., Any] = connection.Client) -> None:
        self.pipe_name = _pipe_name(pipe_name)
        if not isinstance(authentication_key, bytes) or len(authentication_key) < 32:
            raise ValueError("critic transport authentication requires 256 bits")
        self._authkey = authentication_key
        self._connect = _connect

    def _exchange(self, body: Mapping[str, Any]) -> ProviderResultV1:
        deadline = time.monotonic() + PIPE_TIMEOUT_SECONDS_V1
        # Explicit AF_PIPE prevents a path being interpreted as a socket/URL.
        try:
            channel = self._connect(self.pipe_name, family="AF_PIPE", authkey=None)
        except (OSError, EOFError, AuthenticationError) as exc:
            raise BrokerProtocolError("BROKER_UNAVAILABLE") from exc
        watchdog = threading.Timer(max(0.0, deadline - time.monotonic()), channel.close)
        watchdog.daemon = True
        watchdog.start()
        try:
            bounded = _DeadlineBytesConnection(channel, deadline)
            _authenticate(bounded, self._authkey, server=False)
            bounded.send_bytes(canonical_json(body).encode("utf-8"))
            response = _json_object(bounded.recv_bytes())
            expected = {"protocol_version", "request_id", "ok", "result"} if response.get("ok") is True else {
                "protocol_version", "request_id", "ok", "error"}
            strict_fields(response, expected=expected, required=expected, name="CriticPipeResponseV1")
            if (type(response["protocol_version"]) is not int
                    or response["protocol_version"] != BROKER_PROTOCOL_VERSION
                    or response["request_id"] not in {body["request_id"], None}):
                raise BrokerProtocolError("BROKER_RESPONSE_MISMATCH")
            if response["ok"] is not True:
                if response["ok"] is not False or not isinstance(response["error"], str):
                    raise BrokerProtocolError("BROKER_RESPONSE_INVALID")
                raise BrokerProtocolError(str(response["error"]))
            if response["request_id"] != body["request_id"]:
                raise BrokerProtocolError("BROKER_RESPONSE_MISMATCH")
            if not isinstance(response["result"], Mapping):
                raise BrokerProtocolError("BROKER_RESPONSE_INVALID")
            result = response["result"]
            if (not isinstance(result.get("raw_output"), str)
                    or any(type(result.get(key)) is not bool for key in ("refusal", "truncated", "retryable"))
                    or any(result.get(key) is not None and not isinstance(result.get(key), str)
                           for key in ("returned_model_id", "model_revision", "provider_request_id", "failure_code"))):
                raise BrokerProtocolError("INVALID_PROVIDER_RESULT")
            try:
                return _result_from_wire(result)
            except (ValueError, TypeError, AttributeError) as exc:
                raise BrokerProtocolError("INVALID_PROVIDER_RESULT") from exc
        except (OSError, EOFError, AuthenticationError) as exc:
            raise BrokerProtocolError("BROKER_UNAVAILABLE") from exc
        finally:
            watchdog.cancel()
            channel.close()

    def assess(self, *, capability: str, authorization_id: str, attempt_id: str,
               request: ActionAssessmentRequestV2, packet: SealedActionAssessmentPacketV1) -> ProviderResultV1:
        return self._exchange({"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": str(uuid.uuid4()),
            "command": "assess_action_v1", "capability": capability, "authorization_id": authorization_id,
            "attempt_id": attempt_id, "request": request.to_dict(), "packet": packet.to_dict()})

    def execute(self, work: Any) -> ProviderResultV1:
        return self.assess(capability=work.capability, authorization_id=work.identity.authorization_id,
            attempt_id=work.identity.attempt_id, request=work.request, packet=work.packet)


@dataclass(frozen=True)
class WindowsCriticBrokerTransportV1:
    server: WindowsActionCriticBrokerServer
    client_port: WindowsActionCriticClientPort
    authentication_key: bytes = field(repr=False)

    def close(self) -> None:
        self.server.close()


def create_windows_critic_broker_transport(broker: InferenceBroker) -> WindowsCriticBrokerTransportV1:
    endpoint = rf"\\.\pipe\AtlasCritic-{uuid.uuid4()}"
    authkey = secrets.token_bytes(32)
    server = WindowsActionCriticBrokerServer(endpoint, broker, authentication_key=authkey)
    server.start()
    return WindowsCriticBrokerTransportV1(server,
        WindowsActionCriticClientPort(endpoint, authentication_key=authkey), authkey)


def native_windows_critic_broker_smoke_v1() -> dict[str, Any]:
    """Native authenticated pipe lifecycle only; no model/provider/API credential."""
    if os.name != "nt":
        return {"status": "BLOCKED BY ENVIRONMENT", "transport": "AF_PIPE", "no_provider_calls": True}

    # Exercise the installed DPAPI backend with disposable synthetic material.
    # Never read or replace the owner's configured provider credential.
    import subprocess
    import tempfile
    from pathlib import Path

    from ..product import WindowsSecretStore

    # A disposable Windows system process provides a real process-instance
    # handle fixture even when sys.executable is the frozen ATLAS application.
    executable = Path(os.environ["SYSTEMROOT"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    owner = subprocess.Popen([str(executable), "-NoLogo", "-NoProfile", "-NonInteractive",
                              "-Command", "Start-Sleep -Seconds 30"],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        identity = _owner_identity_for_pid(owner.pid)
        with WindowsOwnerProcessV1(identity) as bound_owner:
            if not bound_owner.alive():
                raise RuntimeError("native owner process fixture was not alive")
            owner.terminate()
            owner.wait(timeout=5)
            if bound_owner.alive():
                raise RuntimeError("native owner process death was not observed")
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)

    with tempfile.TemporaryDirectory(prefix="atlas-protected-secret-smoke-") as temporary:
        store = WindowsSecretStore(Path(temporary))
        profile = "deepseek-v41-action-critic-v1"
        synthetic = "ATLAS_OFFLINE_SECRET_FIXTURE_" + secrets.token_hex(32)
        store.put_provider_key(profile, synthetic)
        protected = (Path(temporary) / "deepseek-action-critic.dpapi").read_bytes()
        if synthetic.encode() in protected or store.get_provider_key(profile) != synthetic:
            raise RuntimeError("native protected secret roundtrip failed")
        corrupted = bytearray(protected)
        corrupted[len(corrupted) // 2] ^= 1
        try:
            WindowsSecretStore._crypt(bytes(corrupted), decrypt=True)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("native protected secret admitted a damaged ciphertext")

    class FakeBroker:
        calls = 0

        def assess_action_v1(self, **_kwargs: Any) -> ProviderResultV1:
            self.calls += 1
            return ProviderResultV1("{}", "native-offline-fixture", None, False, False, 0, 0,
                                    "native-pipe-smoke", None, False)

    fixture = FakeBroker()
    runtime = create_windows_critic_broker_transport(fixture)  # type: ignore[arg-type]
    try:
        request = {"protocol_version": 1, "request_id": str(uuid.uuid4()),
            "command": "assess_action_v1", "capability": "native-offline-fixture", "authorization_id": "fixture",
            "attempt_id": "fixture", "request": {}, "packet": {}}
        result = runtime.client_port._exchange(request)
        if result.provider_request_id != "native-pipe-smoke":
            raise RuntimeError("native critic pipe smoke did not return its fixture identity")
        denied = WindowsActionCriticClientPort(runtime.server.pipe_name, authentication_key=secrets.token_bytes(32))
        try:
            denied._exchange(request)
        except BrokerProtocolError as exc:
            if exc.code != "BROKER_UNAVAILABLE":
                raise RuntimeError("native critic pipe authentication did not fail closed") from exc
        else:
            raise RuntimeError("native critic pipe admitted an incorrect authentication key")
        if fixture.calls != 1:
            raise RuntimeError("native critic pipe dispatched an unauthenticated fixture")
    finally:
        runtime.close()
    deadline = time.monotonic() + 2
    while runtime.server.active_handlers and time.monotonic() < deadline:
        time.sleep(0.01)
    if runtime.server.active_handlers:
        raise RuntimeError("native critic pipe handlers did not shut down")
    return {"status": "TESTED", "transport": "AF_PIPE", "active_handlers_after_close": 0,
            "max_handlers": MAX_BROKER_CONNECTIONS, "no_provider_calls": True,
            "wrong_key_rejected": True, "current_user_dacl_verified": True,
            "protected_secret_roundtrip": "TESTED", "damaged_secret_rejected": True,
            "owner_secret_accessed": False, "owner_process_death_observed": True}
