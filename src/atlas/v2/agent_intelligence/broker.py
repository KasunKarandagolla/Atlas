"""Fixed-function inference broker and authenticated local socket protocol.

Only this process constructs the OpenAI SDK client and receives OPENAI_API_KEY.
Its protocol has one command (structured discovery inference), no arbitrary HTTP,
provider URL, hosted tool, file, browser, shell, MCP, venue, or database operation.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import socket
import stat
import struct
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, sha256_json, strict_fields
from atlas.v2.agent_intelligence.contracts import (
    BrokerDispatchAuthorizationV1,
    ProviderResultV1,
    ResearchProposalRequestV1,
)
from atlas.v2.agent_intelligence.provider import ResearchProviderUnavailable
from atlas.v2.models.worker_protocol import _reject_sensitive_fields

BROKER_PROTOCOL_VERSION = 1
MAX_BROKER_FRAME_BYTES = 256_000
BROKER_SOCKET_TIMEOUT_SECONDS = 35.0
MAX_CAPABILITY_LIFETIME_NS = 120_000_000_000
MAX_REPLAY_CACHE_ENTRIES = 256
ALLOWED_PROVIDER = "openai"
ALLOWED_MODEL_ID = "gpt-6-astra"


class BrokerProtocolError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class BrokerCapabilityV1:
    job_id: str
    request_key: str
    model_profile_hash: str
    evidence_hash: str
    issued_at_ns: int
    expires_at_ns: int
    max_model_calls: int
    max_input_tokens: int
    max_output_tokens: int
    nonce: str
    authorization_id: str
    attempt_id: str
    lease_epoch: int
    call_index: int
    deadline_ns: int
    budget_reservation_id: str
    capability: str

    @staticmethod
    def issue_authorized(*, authorization: BrokerDispatchAuthorizationV1,
                         signing_key: bytes) -> BrokerCapabilityV1:
        if len(signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        auth = authorization.to_dict()
        auth_body = dict(auth)
        auth_hash = auth_body.pop("authorization_hash", None)
        if not isinstance(auth_hash, str) or auth_hash != sha256_json(auth_body):
            raise ValueError("durable broker dispatch authorization hash is invalid")
        if authorization.provider != ALLOWED_PROVIDER or authorization.requested_model_id != ALLOWED_MODEL_ID:
            raise ValueError("broker authorization provider/model is outside the fixed allowlist")
        if (authorization.expires_at_ns > min(authorization.deadline_ns,
                authorization.authorized_at_ns + MAX_CAPABILITY_LIFETIME_NS)):
            raise ValueError("broker capability expiry exceeds the durable authorization")
        body = {"version": "BrokerCapabilityV1", "authorization_hash": authorization.authorization_hash,
                "authorization_id": authorization.authorization_id, "job_id": authorization.job_id,
                "request_key": authorization.request_key, "request_hash": authorization.request_hash,
                "attempt_id": authorization.attempt_id, "lease_epoch": authorization.lease_epoch,
                "call_index": authorization.call_index, "capability_nonce": authorization.capability_nonce,
                "provider": authorization.provider, "requested_model_id": authorization.requested_model_id,
                "model_profile_hash": authorization.model_profile_hash,
                "evidence_hash": authorization.evidence_hash, "issued_at_ns": authorization.authorized_at_ns,
                "authorized_at_ns": authorization.authorized_at_ns,
                "deadline_ns": authorization.deadline_ns, "expires_at_ns": authorization.expires_at_ns,
                "budget_reservation_id": authorization.budget_reservation_id,
                "reserved_cost_usd": authorization.reserved_cost_usd,
                "max_model_calls": 1, "max_input_tokens": authorization.max_input_tokens,
                "max_output_tokens": authorization.max_output_tokens}
        encoded = canonical_json(body).encode("utf-8")
        signature = hmac.new(signing_key, encoded, hashlib.sha256).digest()
        token = (base64.urlsafe_b64encode(encoded).decode("ascii") + "."
                 + base64.urlsafe_b64encode(signature).decode("ascii"))
        return BrokerCapabilityV1(authorization.job_id, authorization.request_key,
            authorization.model_profile_hash, authorization.evidence_hash, authorization.authorized_at_ns,
            authorization.expires_at_ns, 1, authorization.max_input_tokens, authorization.max_output_tokens,
            authorization.capability_nonce, authorization.authorization_id, authorization.attempt_id,
            authorization.lease_epoch, authorization.call_index, authorization.deadline_ns,
            authorization.budget_reservation_id, token)

    @staticmethod
    def verify(token: str, *, signing_key: bytes, now_ns: int) -> Mapping[str, Any]:
        try:
            payload_part, signature_part = token.split(".", 1)
            payload = base64.urlsafe_b64decode(payload_part.encode("ascii"))
            signature = base64.urlsafe_b64decode(signature_part.encode("ascii"))
            expected = hmac.new(signing_key, payload, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            body = json.loads(payload.decode("utf-8"))
            fields = {"version", "authorization_hash", "authorization_id", "job_id", "request_key",
                      "request_hash", "attempt_id", "lease_epoch", "call_index", "capability_nonce",
                      "provider", "requested_model_id", "model_profile_hash", "evidence_hash", "issued_at_ns",
                      "authorized_at_ns", "deadline_ns", "expires_at_ns", "budget_reservation_id",
                      "reserved_cost_usd", "max_model_calls", "max_input_tokens", "max_output_tokens"}
            strict_fields(body, expected=fields, required=fields, name="BrokerCapabilityV1")
            if body["version"] != "BrokerCapabilityV1" or now_ns < body["issued_at_ns"] or now_ns >= body["expires_at_ns"]:
                raise ValueError
            if (body["provider"] != ALLOWED_PROVIDER or body["requested_model_id"] != ALLOWED_MODEL_ID
                    or body["max_model_calls"] != 1 or body["authorized_at_ns"] != body["issued_at_ns"]
                    or body["expires_at_ns"] > body["deadline_ns"]
                    or body["deadline_ns"] < body["expires_at_ns"]
                    or body["expires_at_ns"] - body["authorized_at_ns"] > MAX_CAPABILITY_LIFETIME_NS):
                raise ValueError
            for name in ("job_id", "attempt_id", "authorization_id", "capability_nonce", "budget_reservation_id"):
                if str(uuid.UUID(body[name])) != body[name]:
                    raise ValueError
            for name in ("authorization_hash", "request_key", "request_hash", "model_profile_hash", "evidence_hash"):
                if not isinstance(body[name], str) or len(body[name]) != 64:
                    raise ValueError
            if (type(body["lease_epoch"]) is not int or body["lease_epoch"] < 1
                    or type(body["call_index"]) is not int or not 1 <= body["call_index"] <= 3
                    or type(body["max_input_tokens"]) is not int or not 1 <= body["max_input_tokens"] <= 32_000
                    or type(body["max_output_tokens"]) is not int or not 1 <= body["max_output_tokens"] <= 8_000):
                raise ValueError
            auth_fields = {name: body[name] for name in (
                "job_id", "request_key", "request_hash", "attempt_id", "call_index", "lease_epoch",
                "authorization_id", "capability_nonce", "evidence_hash", "model_profile_hash", "provider",
                "requested_model_id", "deadline_ns", "authorized_at_ns", "expires_at_ns",
                "budget_reservation_id", "reserved_cost_usd", "max_input_tokens", "max_output_tokens")}
            if sha256_json({"version": "BrokerDispatchAuthorizationV1", **auth_fields}) != body["authorization_hash"]:
                raise ValueError
            return body
        except Exception as exc:
            raise BrokerProtocolError("INVALID_OR_EXPIRED_CAPABILITY") from exc


def _result_wire(result: ProviderResultV1) -> dict[str, Any]:
    return {"version": "ProviderResultV1", "raw_output": result.raw_output,
            "returned_model_id": result.returned_model_id, "model_revision": result.model_revision,
            "refusal": result.refusal, "truncated": result.truncated, "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens, "provider_request_id": result.provider_request_id,
            "failure_code": result.failure_code, "retryable": result.retryable}


def _result_from_wire(value: Mapping[str, Any]) -> ProviderResultV1:
    fields = {"version", "raw_output", "returned_model_id", "model_revision", "refusal", "truncated",
              "input_tokens", "output_tokens", "provider_request_id", "failure_code", "retryable"}
    row = strict_fields(value, expected=fields, required=fields, name="ProviderResultV1")
    if row["version"] != "ProviderResultV1":
        raise BrokerProtocolError("INVALID_PROVIDER_RESULT")
    return ProviderResultV1(row["raw_output"], row["returned_model_id"], row["model_revision"], row["refusal"],
        row["truncated"], row["input_tokens"], row["output_tokens"], row["provider_request_id"],
        row["failure_code"], row["retryable"])


class InferenceBroker:
    """One bounded structured inference operation; it has no persistence dependency."""

    def __init__(self, provider: Any, *, signing_key: bytes) -> None:
        if len(signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        self._provider = provider
        self.__signing_key = bytes(signing_key)
        self._lock = threading.Lock()
        self._seen: OrderedDict[tuple[str, str], tuple[str, ProviderResultV1]] = OrderedDict()
        self._busy: set[str] = set()

    def infer(self, *, capability: str, job_id: str, attempt_id: str, lease_epoch: int,
              call_index: int, request_data: Mapping[str, Any], evidence: Sequence[Mapping[str, Any]],
              now_ns: int | None = None) -> ProviderResultV1:
        now = time.time_ns() if now_ns is None else now_ns
        body = BrokerCapabilityV1.verify(capability, signing_key=self.__signing_key, now_ns=now)
        if (body["job_id"] != job_id or body["attempt_id"] != attempt_id
                or body["lease_epoch"] != lease_epoch or body["call_index"] != call_index
                or type(lease_epoch) is not int or type(call_index) is not int):
            raise BrokerProtocolError("CAPABILITY_SCOPE_MISMATCH")
        request = ResearchProposalRequestV1.from_dict(request_data)
        if (request.request_key != body["request_key"]
                or sha256_json(request.to_dict()) != body["request_hash"]
                or request.model_profile_hash != body["model_profile_hash"]
                or request.absolute_deadline_ns != body["deadline_ns"]
                or request.max_input_tokens != body["max_input_tokens"]
                or request.max_output_tokens != body["max_output_tokens"]
                or body["provider"] != ALLOWED_PROVIDER
                or body["requested_model_id"] != ALLOWED_MODEL_ID
                or not 1 <= call_index <= request.max_model_calls):
            raise BrokerProtocolError("REQUEST_BINDING_MISMATCH")
        if len(evidence) != len(request.evidence_manifest) or len(evidence) > request.max_read_tool_calls:
            raise BrokerProtocolError("EVIDENCE_BUDGET_EXCEEDED")
        _reject_sensitive_fields(request.to_dict(), field_name="agent_request")
        _reject_sensitive_fields(evidence, field_name="bounded_evidence")
        for auth, item in zip(request.evidence_manifest, evidence, strict=True):
            if (item.get("tool_name") != auth.tool_name or item.get("artifact_ref") != auth.artifact_ref
                    or item.get("cursor") != auth.cursor or item.get("status") not in
                    {"PRESENT", "MISSING", "UNAVAILABLE", "FORBIDDEN", "EXPIRED"}
                    or not isinstance(item.get("rows"), list) or len(item["rows"]) > 50):
                raise BrokerProtocolError("EVIDENCE_BINDING_MISMATCH")
        evidence_json = canonical_json(list(evidence)).encode("utf-8")
        if len(evidence_json) > 192_000:
            raise BrokerProtocolError("EVIDENCE_SIZE_LIMIT")
        if sha256_json(list(evidence)) != body["evidence_hash"]:
            raise BrokerProtocolError("EVIDENCE_HASH_MISMATCH")
        idempotency_key = (body["authorization_id"], body["capability_nonce"])
        input_hash = sha256_json({"request_key": request.request_key, "attempt_id": attempt_id,
                                  "lease_epoch": lease_epoch, "call_index": call_index,
                                  "model_profile_hash": request.model_profile_hash,
                                  "evidence_hash": body["evidence_hash"], "evidence": list(evidence)})
        with self._lock:
            prior = self._seen.get(idempotency_key)
            if prior is not None:
                if prior[0] != input_hash:
                    raise BrokerProtocolError("CALL_INDEX_CONTRADICTION")
                return prior[1]
            if len(self._seen) >= MAX_REPLAY_CACHE_ENTRIES:
                raise BrokerProtocolError("REPLAY_CACHE_FULL")
            if body["authorization_id"] in self._busy:
                raise BrokerProtocolError("DISPATCH_ALREADY_IN_PROGRESS")
            self._busy.add(body["authorization_id"])
        try:
            result = self._provider.propose(request, evidence)
            if not isinstance(result, ProviderResultV1):
                result = ProviderResultV1("", None, None, False, False, 0, 0, None,
                                          "PROVIDER_RETURN_TYPE_INVALID", False)
            elif result.input_tokens > body["max_input_tokens"] or result.output_tokens > body["max_output_tokens"]:
                result = ProviderResultV1("", result.returned_model_id, result.model_revision, result.refusal, True,
                    result.input_tokens, result.output_tokens, result.provider_request_id,
                    "TOKEN_LIMIT_EXCEEDED", False)
            with self._lock:
                self._seen[idempotency_key] = (input_hash, result)
            return result
        except ResearchProviderUnavailable as exc:
            result = ProviderResultV1("", None, None, exc.code == "PROVIDER_REFUSAL", False, 0, 0,
                                      None, exc.code, exc.retryable)
            with self._lock:
                self._seen[idempotency_key] = (input_hash, result)
            return result
        except BrokerProtocolError:
            raise
        except Exception:
            result = ProviderResultV1("", None, None, False, False, 0, 0,
                                      None, "PROVIDER_ERROR", False)
            with self._lock:
                self._seen[idempotency_key] = (input_hash, result)
            return result
        finally:
            with self._lock:
                self._busy.discard(body["authorization_id"])


def _read_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise BrokerProtocolError("TRUNCATED_FRAME")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame(sock: socket.socket) -> bytes:
    (length,) = struct.unpack("!I", _read_exact(sock, 4))
    if length == 0 or length > MAX_BROKER_FRAME_BYTES:
        raise BrokerProtocolError("FRAME_SIZE_INVALID")
    return _read_exact(sock, length)


def _write_frame(sock: socket.socket, value: Mapping[str, Any]) -> None:
    body = canonical_json(value).encode("utf-8")
    if not body or len(body) > MAX_BROKER_FRAME_BYTES:
        raise BrokerProtocolError("FRAME_SIZE_INVALID")
    sock.sendall(struct.pack("!I", len(body)) + body)


class InferenceBrokerServer:
    """Local AF_UNIX server exposes just one authenticated inference operation."""

    def __init__(self, socket_path: str | Path, broker: InferenceBroker) -> None:
        self.socket_path = Path(socket_path)
        if not self.socket_path.is_absolute() or len(str(self.socket_path)) > 100:
            raise ValueError("broker socket must be an absolute short local path")
        self.broker = broker
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._socket: socket.socket | None = None

    def start(self) -> None:
        self.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise RuntimeError("broker socket path already exists")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.socket_path))
        os.chmod(self.socket_path, stat.S_IRUSR | stat.S_IWUSR)
        server.listen(4)
        server.settimeout(0.5)
        self._socket = server
        self._thread = threading.Thread(target=self._serve, name="atlas-agent-inference-broker", daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        assert self._socket is not None
        while not self._stop.is_set():
            try:
                connection, _ = self._socket.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            threading.Thread(target=self._handle, args=(connection,), daemon=True).start()

    def _handle(self, connection: socket.socket) -> None:
        connection.settimeout(BROKER_SOCKET_TIMEOUT_SECONDS)
        try:
            raw = _read_frame(connection)
            data = json.loads(raw.decode("utf-8"))
            fields = {"protocol_version", "request_id", "command", "capability", "job_id", "attempt_id",
                      "lease_epoch", "call_index", "request", "evidence"}
            row = strict_fields(data, expected=fields, required=fields, name="BrokerRequestV1")
            if (row["protocol_version"] != BROKER_PROTOCOL_VERSION or row["command"] != "infer"
                    or not isinstance(row["request_id"], str) or len(row["request_id"]) != 36
                    or not isinstance(row["request"], Mapping) or not isinstance(row["evidence"], list)):
                raise BrokerProtocolError("INVALID_BROKER_REQUEST")
            result = self.broker.infer(capability=row["capability"], job_id=row["job_id"],
                attempt_id=row["attempt_id"], lease_epoch=row["lease_epoch"], call_index=row["call_index"],
                request_data=row["request"], evidence=row["evidence"])
            _write_frame(connection, {"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": row["request_id"],
                                      "ok": True, "result": _result_wire(result)})
        except Exception as exc:
            code = exc.code if isinstance(exc, BrokerProtocolError) else "BROKER_UNAVAILABLE"
            try:
                _write_frame(connection, {"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": None,
                                          "ok": False, "error": code})
            except Exception:
                pass
        finally:
            connection.close()

    def close(self) -> None:
        self._stop.set()
        if self._socket is not None:
            self._socket.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass


class InferenceBrokerClient:
    """Worker-side client; it knows only one already-connected broker socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock

    def propose(self, *, capability: str, job_id: str, attempt_id: str, lease_epoch: int,
                call_index: int, request: ResearchProposalRequestV1,
                evidence: Sequence[Mapping[str, Any]]) -> ProviderResultV1:
        request_id = str(uuid.uuid4())
        payload = {"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": request_id, "command": "infer",
            "capability": capability, "job_id": job_id, "attempt_id": attempt_id, "lease_epoch": lease_epoch,
            "call_index": call_index, "request": request.to_dict(), "evidence": list(evidence)}
        self._socket.settimeout(BROKER_SOCKET_TIMEOUT_SECONDS)
        _write_frame(self._socket, payload)
        response = json.loads(_read_frame(self._socket).decode("utf-8"))
        if response.get("protocol_version") != BROKER_PROTOCOL_VERSION or response.get("request_id") not in {request_id, None}:
            raise BrokerProtocolError("BROKER_RESPONSE_MISMATCH")
        if response.get("ok") is not True:
            raise BrokerProtocolError(str(response.get("error", "BROKER_UNAVAILABLE")))
        if not isinstance(response.get("result"), Mapping):
            raise BrokerProtocolError("BROKER_RESPONSE_INVALID")
        return _result_from_wire(response["result"])


def broker_main() -> int:
    """Opt-in credential-owning service entry; no key is accepted on argv or logged."""
    import argparse

    from atlas.v2.agent_intelligence.provider import PydanticAIResearchProposalProvider

    parser = argparse.ArgumentParser(prog="atlas-agent-broker")
    parser.add_argument("--socket", default=os.environ.get("ATLAS_AGENT_BROKER_SOCKET"))
    args = parser.parse_args()
    if not isinstance(args.socket, str) or not args.socket:
        raise SystemExit("ATLAS_AGENT_BROKER_SOCKET is required")
    signing_text = os.environ.get("ATLAS_AGENT_CAPABILITY_KEY", "")
    try:
        signing_key = bytes.fromhex(signing_text)
    except ValueError as exc:
        raise SystemExit("ATLAS_AGENT_CAPABILITY_KEY must be 64 or more hex characters") from exc
    if len(signing_key) < 32:
        raise SystemExit("ATLAS_AGENT_CAPABILITY_KEY must be at least 256 bits")
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise SystemExit("PROVIDER_CREDENTIAL_UNAVAILABLE")
    provider = PydanticAIResearchProposalProvider(api_key, model_id=ALLOWED_MODEL_ID)
    broker = InferenceBroker(provider, signing_key=signing_key)
    server = InferenceBrokerServer(args.socket, broker)
    try:
        server.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    finally:
        server.close()
