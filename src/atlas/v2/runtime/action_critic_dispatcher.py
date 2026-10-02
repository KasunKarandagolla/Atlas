"""Bounded non-DB worker for the opt-in hidden action critic."""

from __future__ import annotations

import queue
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import sha256_ref
from atlas.v2.agent_intelligence.contracts import (
    ActionAssessmentDispatchAuthorizationV1,
    ActionAssessmentRequestV2,
    ProviderResultV1,
    SealedActionAssessmentPacketV1,
)


@dataclass(frozen=True)
class ActionAssessmentDispatchIdentityV1:
    request_id: str
    request_hash: str
    packet_ref: str
    packet_hash: str
    action_hash: str
    attempt_id: str
    authorization_id: str
    authorization_hash: str
    capability_nonce: str
    profile_hash: str
    authorized_at_ns: int
    deadline_ns: int

    def __post_init__(self) -> None:
        uuid.UUID(self.request_id)
        uuid.UUID(self.attempt_id)
        uuid.UUID(self.authorization_id)
        uuid.UUID(self.capability_nonce)
        for name in ("request_hash", "packet_ref", "packet_hash", "action_hash", "authorization_hash", "profile_hash"):
            sha256_ref(getattr(self, name), field=name)
        if (type(self.authorized_at_ns) is not int or type(self.deadline_ns) is not int
                or not 0 <= self.authorized_at_ns < self.deadline_ns):
            raise ValueError("dispatch identity chronology is invalid")


@dataclass(frozen=True)
class ActionAssessmentDispatchWorkV1:
    identity: ActionAssessmentDispatchIdentityV1
    request: ActionAssessmentRequestV2
    packet: SealedActionAssessmentPacketV1
    authorization: ActionAssessmentDispatchAuthorizationV1
    capability: str

    def __post_init__(self) -> None:
        if not isinstance(self.capability, str) or not self.capability:
            raise ValueError("authorized critic work requires a signed task capability")
        identity = self.identity
        auth = self.authorization
        request = self.request
        packet = self.packet
        if (identity.request_id != request.request_id or identity.request_hash != request.content_hash
                or identity.packet_ref != packet.packet_ref or identity.packet_hash != packet.content_hash
                or identity.action_hash != packet.action_hash or identity.authorization_id != auth.authorization_id
                or identity.authorization_hash != auth.authorization_hash
                or identity.capability_nonce != auth.capability_nonce or identity.profile_hash != request.profile_hash
                or identity.authorized_at_ns != auth.authorized_at_ns or identity.deadline_ns != request.deadline_ns
                or auth.request_hash != request.content_hash or auth.packet_ref != packet.packet_ref
                or auth.packet_hash != packet.content_hash or auth.action_hash != packet.action_hash):
            raise ValueError("critic work identity does not match its immutable request/packet/authorization")


@dataclass(frozen=True)
class ActionAssessmentDispatchCompletionV1:
    identity: ActionAssessmentDispatchIdentityV1
    result: ProviderResultV1
    started_at_ns: int
    received_at_ns: int

    def __post_init__(self) -> None:
        if (type(self.started_at_ns) is not int or type(self.received_at_ns) is not int
                or self.started_at_ns < self.identity.authorized_at_ns
                or self.received_at_ns < self.started_at_ns):
            raise ValueError("critic completion chronology is invalid")


class ActionAssessmentShadowDispatcher:
    """One daemon I/O thread, one pending item, and no object with ledger authority."""

    MAX_CONCURRENT = 1
    MAX_PENDING = 1
    MAX_ACTIVE = MAX_CONCURRENT + MAX_PENDING
    MAX_COMPLETIONS_PER_DRAIN = 2

    def __init__(self, io_port: Any, *, now_ns: Callable[[], int] = time.time_ns) -> None:
        if not callable(getattr(io_port, "execute", None)):
            raise TypeError("critic I/O port must expose execute(work)")
        self._io_port = io_port
        self._now_ns = now_ns
        self._work_queue: queue.Queue[ActionAssessmentDispatchWorkV1] = queue.Queue(maxsize=self.MAX_PENDING)
        self._completion_queue: queue.Queue[ActionAssessmentDispatchCompletionV1] = queue.Queue(
            maxsize=self.MAX_ACTIVE)
        self._lock = threading.Lock()
        self._writer_thread_id = threading.get_ident()
        self._active_count = 0
        self._reservation: str | None = None
        self._closed = False
        self._worker_thread_id: int | None = None
        self._thread = threading.Thread(target=self._run, name="atlas-action-critic-io", daemon=True)
        self._thread.start()

    @property
    def worker_thread_id(self) -> int | None:
        return self._worker_thread_id

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active_count

    @property
    def pending_count(self) -> int:
        with self._lock:
            return self._work_queue.qsize() + int(self._reservation is not None)

    def reserve_capacity(self) -> str | None:
        """Reserve queue handoff capacity without waiting or issuing authorization."""
        with self._lock:
            if self._closed or self._reservation is not None or self._active_count >= self.MAX_ACTIVE:
                return None
            if self._work_queue.full():
                return None
            token = str(uuid.uuid4())
            self._reservation = token
            return token

    def release_capacity(self, token: str) -> None:
        with self._lock:
            if self._reservation == token:
                self._reservation = None

    def submit_reserved(self, token: str, work: ActionAssessmentDispatchWorkV1) -> bool:
        """Hand off only after controller persistence has committed authorization."""
        with self._lock:
            if self._closed or self._reservation != token or self._work_queue.full():
                if self._reservation == token:
                    self._reservation = None
                return False
            self._work_queue.put_nowait(work)
            self._active_count += 1
            self._reservation = None
            return True

    def drain_completed(self, *, max_items: int = 1) -> tuple[ActionAssessmentDispatchCompletionV1, ...]:
        self._assert_writer_thread()
        if type(max_items) is not int or not 0 <= max_items <= self.MAX_COMPLETIONS_PER_DRAIN:
            raise ValueError("critic completion drain bound must be between zero and two")
        rows: list[ActionAssessmentDispatchCompletionV1] = []
        for _ in range(max_items):
            try:
                rows.append(self._completion_queue.get_nowait())
            except queue.Empty:
                break
            with self._lock:
                self._active_count = max(0, self._active_count - 1)
        return tuple(rows)

    def close(self) -> None:
        """Mark closed and return immediately; the daemon may finish or be abandoned."""
        with self._lock:
            self._closed = True
            self._reservation = None

    def _assert_writer_thread(self) -> None:
        if threading.get_ident() != self._writer_thread_id:
            raise RuntimeError("critic completions may be drained only by the controller writer thread")

    def _run(self) -> None:
        self._worker_thread_id = threading.get_ident()
        while True:
            try:
                work = self._work_queue.get(timeout=0.1)
            except queue.Empty:
                with self._lock:
                    if self._closed:
                        return
                continue
            with self._lock:
                closed = self._closed
            started_at_ns = max(work.identity.authorized_at_ns, self._now_ns())
            if closed:
                result = _failure_result("BROKER_UNAVAILABLE")
            else:
                try:
                    result = _sanitize_result(self._io_port.execute(work))
                except TimeoutError:
                    result = _failure_result("PROVIDER_TIMEOUT")
                except Exception as exc:
                    result = _failure_result(getattr(exc, "code", "BROKER_UNAVAILABLE"))
            received_at_ns = max(started_at_ns, self._now_ns())
            completion = ActionAssessmentDispatchCompletionV1(
                work.identity, result, started_at_ns, received_at_ns)
            # The active-work cap guarantees room in this bounded queue. Never block the I/O thread.
            try:
                self._completion_queue.put_nowait(completion)
            except queue.Full:
                # Defensive fail-closed path: controller restart recovery will seal the durable dispatch lost.
                return


_SAFE_FAILURES = frozenset({
    "BROKER_UNAVAILABLE", "BROKER_SATURATED", "PROVIDER_UNAVAILABLE", "PROVIDER_TIMEOUT", "RATE_LIMITED",
    "PROVIDER_REFUSAL", "PROVIDER_TRUNCATED", "PROVIDER_ERROR", "PROVIDER_AUTHENTICATION_FAILED",
    "PROVIDER_REQUEST_REJECTED", "RETURNED_MODEL_ID_DRIFT", "RETURNED_MODEL_ID_MISSING",
    "AGENT_DEPENDENCY_UNAVAILABLE", "INPUT_TOKEN_BUDGET_EXCEEDED", "TOKEN_LIMIT_EXCEEDED",
    "OUTPUT_SIZE_LIMIT", "MALFORMED_STRUCTURED_OUTPUT", "UNEXPECTED_TOOL_OUTPUT", "BROKER_PROTOCOL_ERROR",
})


def _failure_result(reason: str) -> ProviderResultV1:
    code = reason if reason in _SAFE_FAILURES else "BROKER_UNAVAILABLE"
    return ProviderResultV1("", None, None, False, False, 0, 0, None, code, False)


def _sanitize_result(value: Any) -> ProviderResultV1:
    if not isinstance(value, ProviderResultV1):
        return _failure_result("BROKER_PROTOCOL_ERROR")
    if (not isinstance(value.raw_output, str) or len(value.raw_output.encode("utf-8")) > 100_000
            or type(value.input_tokens) is not int or type(value.output_tokens) is not int
            or value.input_tokens < 0 or value.output_tokens < 0):
        return _failure_result("OUTPUT_SIZE_LIMIT")
    model_id = value.returned_model_id if isinstance(value.returned_model_id, str) else None
    if model_id is not None:
        model_id = "".join(char for char in model_id if 32 <= ord(char) < 127)[:128] or None
    request_id = value.provider_request_id if isinstance(value.provider_request_id, str) else None
    if request_id is not None:
        request_id = "".join(char for char in request_id if 32 <= ord(char) < 127)[:160] or None
    failure = value.failure_code if isinstance(value.failure_code, str) and value.failure_code in _SAFE_FAILURES else None
    return ProviderResultV1(value.raw_output, model_id, None, bool(value.refusal), bool(value.truncated),
        value.input_tokens, value.output_tokens, request_id, failure, False)
