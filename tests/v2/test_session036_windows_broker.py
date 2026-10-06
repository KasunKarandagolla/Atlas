"""Authenticated byte-only critic transport invariants with deterministic peers."""

from __future__ import annotations

import ctypes
import json
import queue
import threading
import time
import uuid
from ctypes import wintypes
from types import SimpleNamespace
from typing import Any

import pytest

from atlas.v2.agent_intelligence import windows_broker as pipe
from atlas.v2.agent_intelligence.broker import BrokerProtocolError, _result_wire
from atlas.v2.agent_intelligence.contracts import ProviderResultV1

KEY = b"x" * 32
ENDPOINT = rf"\\.\pipe\AtlasCritic-{uuid.uuid4()}"
EOF = object()


class Channel:
    """No object send/recv API exists, including during authentication."""

    def __init__(self) -> None:
        self.incoming: queue.Queue[Any] = queue.Queue()
        self.peer: Channel
        self.closed = False
        self.pending: Any = None

    def send_bytes(self, raw: bytes) -> None:
        if self.closed or self.peer.closed:
            raise OSError("closed fake channel")
        assert isinstance(raw, bytes)
        self.peer.incoming.put(raw)

    def poll(self, timeout: float) -> bool:
        if self.pending is not None:
            return True
        try:
            self.pending = self.incoming.get(timeout=timeout)
        except queue.Empty:
            return False
        return True

    def recv_bytes(self, maxlength: int) -> bytes:
        if not self.poll(2):
            raise TimeoutError("fake receive timed out")
        value, self.pending = self.pending, None
        if value is EOF:
            raise EOFError
        if len(value) > maxlength:
            raise OSError("fake frame exceeds receive bound")
        return bytes(value)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.incoming.put(EOF)
            self.peer.incoming.put(EOF)


def pair() -> tuple[Channel, Channel]:
    a, b = Channel(), Channel()
    a.peer, b.peer = b, a
    return a, b


class Listener:
    def __init__(self, _name: str) -> None:
        self.pending: queue.Queue[Any] = queue.Queue()
        self.closed = False

    def accept(self) -> Channel:
        value = self.pending.get(timeout=3)
        if value is EOF:
            raise OSError("listener closed")
        return value

    def close(self) -> None:
        self.closed = True
        self.pending.put(EOF)

    def connect(self, name: str, *, family: str, authkey: Any) -> Channel:
        assert name == ENDPOINT and family == "AF_PIPE" and authkey is None
        a, b = pair()
        self.pending.put(b)
        return a


class Broker:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def assess_action_v1(self, **kwargs: Any) -> ProviderResultV1:
        self.calls.append(kwargs)
        return ProviderResultV1("{}", "fixture-model", None, False, False, 3, 2, "fixture-request")


def body() -> dict[str, Any]:
    return {"protocol_version": 1, "request_id": str(uuid.uuid4()), "command": "assess_action_v1",
            "capability": "signed-fixture", "authorization_id": "authorization", "attempt_id": "attempt",
            "request": {}, "packet": {}}


def wait_for(predicate: Any) -> None:
    deadline = time.monotonic() + 2
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert predicate()


@pytest.mark.parametrize("endpoint", ["https://example.com", r"\\remote\pipe\AtlasCritic-foo", "relative",
                                     r"\\.\pipe\Other-00000000-0000-0000-0000-000000000000"])
def test_only_closed_local_pipe_identity_is_accepted(endpoint: str) -> None:
    with pytest.raises(ValueError):
        pipe.WindowsActionCriticClientPort(endpoint, authentication_key=KEY)


def test_authentication_requires_a_nonpublic_256_bit_key() -> None:
    with pytest.raises(ValueError):
        pipe.WindowsActionCriticClientPort(ENDPOINT, authentication_key=b"short")
    with pytest.raises(ValueError):
        pipe.WindowsActionCriticBrokerServer(ENDPOINT, Broker(), authentication_key="x" * 32)  # type: ignore[arg-type]


def test_authenticated_roundtrip_and_one_shot_lifecycle() -> None:
    listener, broker = Listener(ENDPOINT), Broker()
    server = pipe.WindowsActionCriticBrokerServer(ENDPOINT, broker, authentication_key=KEY,
                                                 _listener_factory=lambda _name: listener)  # type: ignore[arg-type]
    client = pipe.WindowsActionCriticClientPort(ENDPOINT, authentication_key=KEY, _connect=listener.connect)
    server.start()
    try:
        result = client._exchange(body())
        assert result.provider_request_id == "fixture-request"
        assert len(broker.calls) == 1
        assert broker.calls[0]["capability"] == "signed-fixture"
        with pytest.raises(RuntimeError):
            server.start()
    finally:
        server.close()
    wait_for(lambda: server.active_handlers == 0)
    assert server.health()["closed"] is True
    with pytest.raises(RuntimeError):
        server.start()


def test_handler_releases_slot_if_native_close_reports_already_closed(monkeypatch) -> None:
    server = pipe.WindowsActionCriticBrokerServer(ENDPOINT, Broker(), authentication_key=KEY)
    a, channel = pair()
    server._connections.add(channel)
    assert server._slots.acquire(blocking=False)

    def unavailable(*_args, **_kwargs):
        raise EOFError

    def already_closed():
        raise OSError(6, "fake invalid handle")

    monkeypatch.setattr(pipe, "_authenticate", unavailable)
    monkeypatch.setattr(channel, "close", already_closed)
    server._handle(channel)
    assert server.active_handlers == 0
    assert server._slots.acquire(blocking=False)
    server._slots.release()
    a.closed = True


def test_wrong_authentication_key_never_reaches_provider() -> None:
    listener, broker = Listener(ENDPOINT), Broker()
    server = pipe.WindowsActionCriticBrokerServer(ENDPOINT, broker, authentication_key=KEY,
                                                 _listener_factory=lambda _name: listener)  # type: ignore[arg-type]
    server.start()
    try:
        client = pipe.WindowsActionCriticClientPort(ENDPOINT, authentication_key=b"y" * 32,
                                                   _connect=listener.connect)
        with pytest.raises(BrokerProtocolError, match="BROKER_UNAVAILABLE"):
            client._exchange(body())
        wait_for(lambda: server.active_handlers == 0)
        assert broker.calls == []
    finally:
        server.close()


def test_saturation_and_shutdown_release_unauthenticated_peers() -> None:
    listener, broker = Listener(ENDPOINT), Broker()
    server = pipe.WindowsActionCriticBrokerServer(ENDPOINT, broker, authentication_key=KEY,
                                                 _listener_factory=lambda _name: listener)  # type: ignore[arg-type]
    server.start()
    clients = [listener.connect(ENDPOINT, family="AF_PIPE", authkey=None)
               for _ in range(pipe.MAX_BROKER_CONNECTIONS)]
    wait_for(lambda: server.active_handlers == pipe.MAX_BROKER_CONNECTIONS)
    rejected = listener.connect(ENDPOINT, family="AF_PIPE", authkey=None)
    assert rejected.poll(2)
    with pytest.raises(EOFError):
        rejected.recv_bytes(100)
    assert server.health()["saturated_connections"] == 1
    server.close()
    wait_for(lambda: server.active_handlers == 0)
    assert broker.calls == [] and listener.closed
    for client in clients:
        client.close()


@pytest.mark.parametrize("mutation", ["infer", "future_operation", "extra_field", "boolean_protocol", "nonstring_capability"])
def test_invalid_or_unrelated_operations_have_no_dispatch(mutation: str) -> None:
    request, broker = body(), Broker()
    if mutation in {"infer", "future_operation"}:
        request["command"] = mutation
    elif mutation == "extra_field":
        request["secret"] = "not accepted"
    elif mutation == "boolean_protocol":
        request["protocol_version"] = True
    else:
        request["capability"] = {"unexpected": True}
    response = json.loads(pipe._response(broker, json.dumps(request).encode()))
    assert response["ok"] is False and broker.calls == []


@pytest.mark.parametrize("raw", [
    pytest.param(b"", id="empty"),
    pytest.param(b"[]", id="non_object"),
    pytest.param(b'{"x":1,"x":2}', id="duplicate_key"),
    pytest.param(b'{"x":NaN}', id="non_finite_number"),
    pytest.param(b"not-json", id="malformed_json"),
    pytest.param(b"x" * (pipe.MAX_BROKER_FRAME_BYTES + 1), id="oversized_frame"),
])
def test_bad_json_and_frame_bounds_fail_without_provider(raw: bytes) -> None:
    broker = Broker()
    assert json.loads(pipe._response(broker, raw))["ok"] is False
    assert broker.calls == []


def test_deadline_and_authentication_byte_bounds() -> None:
    a, b = pair()
    expired = pipe._DeadlineBytesConnection(a, time.monotonic() - 1)
    with pytest.raises(TimeoutError):
        expired.send_bytes(b"request")
    with pytest.raises(TimeoutError):
        expired.recv_bytes()
    current = pipe._DeadlineBytesConnection(a, time.monotonic() + 1)
    with pytest.raises(BrokerProtocolError, match="FRAME_SIZE_INVALID"):
        current.send_bytes(b"x" * (pipe.MAX_BROKER_FRAME_BYTES + 1))
    b.send_bytes(b"abcd")
    with pytest.raises(OSError):
        current.recv_bytes(3)
    a.close()


def test_connect_failure_is_explicit_and_does_not_fallback() -> None:
    calls: list[str] = []

    def unavailable(endpoint: str, **_kwargs: Any) -> Any:
        calls.append(endpoint)
        raise OSError("fixture unavailable")

    client = pipe.WindowsActionCriticClientPort(ENDPOINT, authentication_key=KEY, _connect=unavailable)
    with pytest.raises(BrokerProtocolError, match="BROKER_UNAVAILABLE"):
        client._exchange(body())
    assert calls == [ENDPOINT]


@pytest.mark.parametrize("mutation", ["missing_request_id", "wrong_request_id", "integer_refusal", "model_object",
                                     "negative_tokens", "boolean_tokens", "extra_result_field"])
def test_result_binding_and_host_metadata_types_are_validated(mutation: str) -> None:
    request = body()
    a, b = pair()
    response: dict[str, Any] = {"protocol_version": 1, "request_id": request["request_id"], "ok": True,
                              "result": _result_wire(Broker().assess_action_v1())}
    if mutation == "missing_request_id":
        response["request_id"] = None
    elif mutation == "wrong_request_id":
        response["request_id"] = str(uuid.uuid4())
    elif mutation == "integer_refusal":
        response["result"]["refusal"] = 0
    elif mutation == "model_object":
        response["result"]["returned_model_id"] = {"alias": "model"}
    elif mutation == "negative_tokens":
        response["result"]["output_tokens"] = -1
    elif mutation == "boolean_tokens":
        response["result"]["input_tokens"] = True
    else:
        response["result"]["extra"] = 1

    def peer() -> None:
        bounded = pipe._DeadlineBytesConnection(b, time.monotonic() + 2)
        pipe._authenticate(bounded, KEY, server=True)
        bounded.recv_bytes()
        bounded.send_bytes(json.dumps(response).encode())

    thread = threading.Thread(target=peer)
    thread.start()
    client = pipe.WindowsActionCriticClientPort(ENDPOINT, authentication_key=KEY, _connect=lambda *_args, **_kwargs: a)
    with pytest.raises(BrokerProtocolError):
        client._exchange(request)
    thread.join(timeout=2)
    assert not thread.is_alive()
    b.close()


def test_native_smoke_cannot_claim_windows_test_on_linux() -> None:
    if pipe.os.name == "nt":
        pytest.skip("native transport is exercised by the Windows validation harness")
    assert pipe.native_windows_critic_broker_smoke_v1()["status"] == "BLOCKED BY ENVIRONMENT"
    with pytest.raises(RuntimeError, match="native Windows"):
        pipe._WindowsPipeSecurity()


class WinFunction:
    def __init__(self, call: Any) -> None:
        self.call = call

    def __call__(self, *args: Any) -> Any:
        return self.call(*args)


def owner_kernel(*, ticks: int = (123 << 32) | 456, wait: int = 258,
                 opens: bool = True, times: bool = True) -> tuple[Any, list[Any], list[Any]]:
    opened, closed = [], []

    def open_process(access: int, inherit: bool, pid: int) -> int:
        opened.append((access, inherit, pid))
        return (1 << 40) + 42 if opens else 0

    def process_times(_handle: int, created: Any, *_unused: Any) -> bool:
        creation = ctypes.cast(created, ctypes.POINTER(pipe._ProcessFileTime)).contents
        creation.low, creation.high = ticks & 0xFFFFFFFF, ticks >> 32
        return times

    kernel = SimpleNamespace(OpenProcess=WinFunction(open_process), GetProcessTimes=WinFunction(process_times),
                             WaitForSingleObject=WinFunction(lambda _handle, timeout: wait), CloseHandle=closed.append)
    return kernel, opened, closed


def test_owner_process_binds_creation_identity_observes_death_and_closes_once() -> None:
    kernel, opened, closed = owner_kernel()
    identity = pipe._owner_identity_for_pid(123, _kernel=kernel)
    assert identity == {"pid": 123, "creation_filetime_ticks": (123 << 32) | 456}
    assert closed == [(1 << 40) + 42]
    with pipe.WindowsOwnerProcessV1(identity, _kernel=kernel) as owner:
        assert owner.alive()
        kernel.WaitForSingleObject = WinFunction(lambda handle, timeout: 0)
        assert not owner.alive()
    owner.close()
    assert not owner.alive()
    assert opened == [(0x00100000 | 0x1000, False, 123)] * 2
    assert closed == [(1 << 40) + 42] * 2


@pytest.mark.parametrize("fault", ["pid_reused", "times_unavailable", "already_stopped", "wait_failed", "open_failed"])
def test_owner_process_rejects_unverifiable_identity_and_closes_failed_handle(fault: str) -> None:
    kernel, opened, closed = owner_kernel(ticks=999 if fault == "pid_reused" else (123 << 32) | 456,
        times=fault != "times_unavailable", opens=fault != "open_failed",
        wait=0 if fault == "already_stopped" else 0xFFFFFFFF if fault == "wait_failed" else 258)
    with pytest.raises((ValueError, OSError)):
        pipe.WindowsOwnerProcessV1({"pid": 123, "creation_filetime_ticks": (123 << 32) | 456}, _kernel=kernel)
    assert len(opened) == 1
    assert len(closed) == (0 if fault == "open_failed" else 1)


@pytest.mark.parametrize("identity", [
    {"pid": True, "creation_filetime_ticks": 1}, {"pid": 0, "creation_filetime_ticks": 1},
    {"pid": 123, "creation_filetime_ticks": 0}, {"pid": 123, "creation_filetime_ticks": True},
    {"pid": 123, "creation_filetime_ticks": 1, "alias": "another-owner"},
])
def test_owner_process_rejects_malformed_identity_without_opening_handle(identity: dict[str, Any]) -> None:
    kernel, opened, closed = owner_kernel()
    with pytest.raises((ValueError, TypeError)):
        pipe.WindowsOwnerProcessV1(identity, _kernel=kernel)
    assert not opened and not closed


def test_owner_creation_lookup_closes_handle_on_failure() -> None:
    kernel, _opened, closed = owner_kernel(times=False)
    with pytest.raises(OSError):
        pipe._owner_identity_for_pid(123, _kernel=kernel)
    assert closed == [(1 << 40) + 42]


def test_native_pipe_creation_specifies_current_user_acl_and_remote_rejection() -> None:
    calls: dict[str, Any] = {}
    freed: list[Any] = []
    verified: list[int] = []

    def descriptor(sddl: str, revision: int, pointer: Any, _length: Any) -> bool:
        calls["descriptor"] = (sddl, revision)
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_void_p))[0] = 123
        return True

    def create(*args: Any) -> int:
        calls["create"] = args[:-1]
        # SECURITY_ATTRIBUTES has an aligned pointer after its DWORD length.
        class Attributes(ctypes.Structure):
            _fields_ = [("length", wintypes.DWORD), ("descriptor", ctypes.c_void_p), ("inherit", wintypes.BOOL)]
        calls["attributes"] = ctypes.cast(args[-1], ctypes.POINTER(Attributes)).contents.inherit
        return 42

    security = pipe._WindowsPipeSecurity.__new__(pipe._WindowsPipeSecurity)
    security.user_sid = "S-1-5-21-1-2-3-1001"
    security.kernel = SimpleNamespace(CreateNamedPipeW=WinFunction(create), LocalFree=freed.append)
    security.advapi = SimpleNamespace(ConvertStringSecurityDescriptorToSecurityDescriptorW=descriptor)
    security.verify_user_only = verified.append  # type: ignore[method-assign]
    assert security.create_pipe(ENDPOINT, first=True) == 42
    assert calls["descriptor"] == ("D:P(A;;GA;;;S-1-5-21-1-2-3-1001)", 1)
    assert calls["attributes"] == 0 and verified == [42] and len(freed) == 1
    name, access, mode, instances, output_bytes, input_bytes, _timeout = calls["create"]
    assert name == ENDPOINT and access == 0x00000003 | 0x40000000 | 0x00080000
    assert mode == 0x00000004 | 0x00000002 | 0x00000008
    assert instances == pipe.MAX_BROKER_CONNECTIONS + 2 and output_bytes == input_bytes == 65_536
    security.create_pipe(ENDPOINT, first=False)
    assert calls["create"][1] & 0x00080000 == 0


@pytest.mark.parametrize("fault", [None, "null_acl", "extra_ace", "inherited_acl", "different_user", "deny_ace"])
def test_native_acl_verifier_rejects_permission_broadening(fault: str | None) -> None:
    acl = ctypes.create_string_buffer(bytes([2, 0, 24, 0, 2 if fault == "extra_ace" else 1, 0, 0, 0]))
    ace = ctypes.create_string_buffer(bytes([1 if fault == "deny_ace" else 0, 0, 20, 0, 0, 0, 0, 0]) + b"fake-sid")
    freed: list[Any] = []

    def info(_handle: int, kind: int, requested: int, _owner: Any, _group: Any,
             acl_pointer: Any, _sacl: Any, descriptor_pointer: Any) -> int:
        assert kind == 6 and requested == 4
        ctypes.cast(acl_pointer, ctypes.POINTER(ctypes.c_void_p))[0] = 0 if fault == "null_acl" else ctypes.addressof(acl)
        ctypes.cast(descriptor_pointer, ctypes.POINTER(ctypes.c_void_p))[0] = 123
        return 0

    def control(_descriptor: Any, pointer: Any, _revision: Any) -> bool:
        ctypes.cast(pointer, ctypes.POINTER(wintypes.WORD))[0] = 0 if fault == "inherited_acl" else 0x1000
        return True

    def get_ace(_acl: Any, index: int, pointer: Any) -> bool:
        assert index == 0
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_void_p))[0] = ctypes.addressof(ace)
        return True

    security = pipe._WindowsPipeSecurity.__new__(pipe._WindowsPipeSecurity)
    security.user_sid = "owner"
    security.kernel = SimpleNamespace(LocalFree=freed.append)
    security.advapi = SimpleNamespace(GetSecurityInfo=info, GetSecurityDescriptorControl=control, GetAce=get_ace)
    security._sid_string = lambda _pointer: "other" if fault == "different_user" else "owner"  # type: ignore[method-assign]
    if fault is None:
        security.verify_user_only(42)
    else:
        with pytest.raises(OSError):
            security.verify_user_only(42)
    assert len(freed) == 1
