"""Fixed-function inference broker and authenticated local socket protocol.

Only this process constructs the configured OpenAI-compatible SDK client and receives
the selected provider credential (OPENAI_API_KEY or DEEPSEEK_API_KEY). Its protocol
has two disjoint fixed commands (research proposal and sealed action assessment), no
arbitrary HTTP, provider URL, hosted tool, file, browser, shell, MCP, venue, or database operation.
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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, sha256_json, strict_fields
from atlas.v2.agent_intelligence.contracts import (
    ActionAssessmentDispatchAuthorizationV1,
    ActionAssessmentProviderProfileV1,
    ActionAssessmentRequestV2,
    AgentModelProfileV1,
    AgentModelProfileV2,
    BrokerDispatchAuthorizationV1,
    BrokerDispatchAuthorizationV2,
    ProviderResultV1,
    ResearchProposalRequestV1,
    SealedActionAssessmentPacketV1,
)
from atlas.v2.agent_intelligence.provider import ResearchProviderUnavailable
from atlas.v2.models.worker_protocol import _reject_sensitive_fields

BROKER_PROTOCOL_VERSION = 1
MAX_BROKER_FRAME_BYTES = 256_000
BROKER_SOCKET_TIMEOUT_SECONDS = 35.0
MAX_CAPABILITY_LIFETIME_NS = 120_000_000_000
MAX_REPLAY_CACHE_ENTRIES = 256
MAX_BROKER_CONNECTIONS = 4
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


@dataclass(frozen=True)
class BrokerCapabilityV2:
    """Versioned capability for the exact DeepSeek provider/model/price binding."""

    job_id: str
    request_key: str
    model_profile_hash: str
    provider_binding_hash: str
    price_schedule_hash: str
    evidence_hash: str
    issued_at_ns: int
    expires_at_ns: int
    max_model_calls: int
    max_input_tokens: int
    max_output_tokens: int
    authorization_id: str
    attempt_id: str
    lease_epoch: int
    call_index: int
    deadline_ns: int
    budget_reservation_id: str
    capability: str

    @staticmethod
    def issue_authorized(*, authorization: BrokerDispatchAuthorizationV2,
                         signing_key: bytes) -> BrokerCapabilityV2:
        if len(signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        auth = authorization.to_dict()
        auth_body = dict(auth)
        auth_hash = auth_body.pop("authorization_hash", None)
        if not isinstance(auth_hash, str) or auth_hash != sha256_json(auth_body):
            raise ValueError("durable DeepSeek dispatch authorization hash is invalid")
        if (authorization.provider != "deepseek" or authorization.requested_model_id != "deepseek-flash"
                or authorization.endpoint != "https://api.deepseek.com/responses"):
            raise ValueError("broker authorization provider/model/endpoint is outside the V2 binding")
        if authorization.expires_at_ns > min(authorization.deadline_ns,
                authorization.authorized_at_ns + MAX_CAPABILITY_LIFETIME_NS):
            raise ValueError("broker capability expiry exceeds the durable authorization")
        body = {"version": "BrokerCapabilityV2", **{key: value for key, value in auth.items()
            if key not in {"version", "authorization_hash"}},
            "authorization_hash": authorization.authorization_hash,
            "model_profile_version": "AgentModelProfileV2", "issued_at_ns": authorization.authorized_at_ns,
            "max_model_calls": 1}
        encoded = canonical_json(body).encode("utf-8")
        signature = hmac.new(signing_key, encoded, hashlib.sha256).digest()
        token = (base64.urlsafe_b64encode(encoded).decode("ascii") + "."
                 + base64.urlsafe_b64encode(signature).decode("ascii"))
        return BrokerCapabilityV2(authorization.job_id, authorization.request_key,
            authorization.model_profile_hash, authorization.provider_binding_hash,
            authorization.price_schedule_hash, authorization.evidence_hash,
            authorization.authorized_at_ns, authorization.expires_at_ns, 1,
            authorization.max_input_tokens, authorization.max_output_tokens,
            authorization.authorization_id, authorization.attempt_id, authorization.lease_epoch,
            authorization.call_index, authorization.deadline_ns, authorization.budget_reservation_id, token)

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
            fields = {"version", "authorization_hash", "job_id", "request_key", "request_hash", "attempt_id",
                "call_index", "lease_epoch", "authorization_id", "capability_nonce", "evidence_hash",
                "model_profile_hash", "provider_binding_hash", "price_schedule_hash", "provider",
                "requested_model_id", "endpoint", "deadline_ns", "authorized_at_ns", "expires_at_ns",
                "budget_reservation_id", "reserved_cost_usd", "max_input_tokens", "max_output_tokens",
                "model_profile_version", "issued_at_ns", "max_model_calls"}
            strict_fields(body, expected=fields, required=fields, name="BrokerCapabilityV2")
            if (body["version"] != "BrokerCapabilityV2" or body["model_profile_version"] != "AgentModelProfileV2"
                    or now_ns < body["issued_at_ns"] or now_ns >= body["expires_at_ns"]):
                raise ValueError
            if (body["provider"] != "deepseek" or body["requested_model_id"] != "deepseek-flash"
                    or body["endpoint"] != "https://api.deepseek.com/responses"
                    or body["max_model_calls"] != 1 or body["issued_at_ns"] != body["authorized_at_ns"]
                    or body["expires_at_ns"] > body["deadline_ns"]
                    or body["expires_at_ns"] - body["authorized_at_ns"] > MAX_CAPABILITY_LIFETIME_NS
                    or body["max_input_tokens"] != 12_000 or body["max_output_tokens"] != 4_000):
                raise ValueError
            for name in ("job_id", "attempt_id", "authorization_id", "capability_nonce", "budget_reservation_id"):
                if str(uuid.UUID(body[name])) != body[name]:
                    raise ValueError
            for name in ("authorization_hash", "request_key", "request_hash", "model_profile_hash",
                         "provider_binding_hash", "price_schedule_hash", "evidence_hash"):
                if not isinstance(body[name], str) or len(body[name]) != 64:
                    raise ValueError
            if (type(body["lease_epoch"]) is not int or body["lease_epoch"] < 1
                    or type(body["call_index"]) is not int or not 1 <= body["call_index"] <= 3):
                raise ValueError
            auth_fields = {name: body[name] for name in (
                "job_id", "request_key", "request_hash", "attempt_id", "call_index", "lease_epoch",
                "authorization_id", "capability_nonce", "evidence_hash", "model_profile_hash",
                "provider_binding_hash", "price_schedule_hash", "provider", "requested_model_id", "endpoint",
                "deadline_ns", "authorized_at_ns", "expires_at_ns", "budget_reservation_id",
                "reserved_cost_usd", "max_input_tokens", "max_output_tokens")}
            if sha256_json({"version": "BrokerDispatchAuthorizationV2", **auth_fields}) != body["authorization_hash"]:
                raise ValueError
            return body
        except Exception as exc:
            raise BrokerProtocolError("INVALID_OR_EXPIRED_CAPABILITY") from exc


@dataclass(frozen=True)
class ActionAssessmentBrokerCapabilityV1:
    """Disjoint one-call capability for the sealed hidden action-critic task."""

    authorization_id: str
    attempt_id: str
    packet_ref: str
    packet_hash: str
    request_hash: str
    action_hash: str
    profile_hash: str
    provider_binding_hash: str
    price_schedule_hash: str
    deadline_ns: int
    issued_at_ns: int
    expires_at_ns: int
    max_model_calls: int
    max_input_tokens: int
    max_output_tokens: int
    capability: str

    @staticmethod
    def issue_authorized(*, authorization: ActionAssessmentDispatchAuthorizationV1,
                         signing_key: bytes) -> ActionAssessmentBrokerCapabilityV1:
        if len(signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        auth_body = authorization.to_dict()
        auth_hash = auth_body.pop("authorization_hash", None)
        if not isinstance(auth_hash, str) or auth_hash != sha256_json(auth_body):
            raise ValueError("durable action-assessment authorization hash is invalid")
        if (authorization.task_identity != "ActionAssessmentProvider" or authorization.provider != "deepseek"
                or authorization.requested_model_id != "deepseek-flash"
                or authorization.endpoint != "https://api.deepseek.com/responses"
                or authorization.max_input_tokens != 12_000 or authorization.max_output_tokens != 2_048):
            raise ValueError("action-assessment authorization is outside the fixed critic binding")
        body = {"version": "ActionAssessmentBrokerCapabilityV1",
                **{key: value for key, value in auth_body.items() if key != "version"},
                "authorization_hash": authorization.authorization_hash, "issued_at_ns": authorization.authorized_at_ns,
                "max_model_calls": 1, "max_dynamic_tools": 0}
        encoded = canonical_json(body).encode("utf-8")
        signature = hmac.new(signing_key, encoded, hashlib.sha256).digest()
        token = base64.urlsafe_b64encode(encoded).decode("ascii") + "." + base64.urlsafe_b64encode(signature).decode("ascii")
        return ActionAssessmentBrokerCapabilityV1(
            authorization.authorization_id, authorization.attempt_id, authorization.packet_ref,
            authorization.packet_hash, authorization.request_hash, authorization.action_hash,
            authorization.profile_hash, authorization.provider_binding_hash, authorization.price_schedule_hash,
            authorization.deadline_ns, authorization.authorized_at_ns, authorization.expires_at_ns,
            1, authorization.max_input_tokens, authorization.max_output_tokens, token)

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
            fields = {"version", "task_identity", "packet_ref", "packet_hash", "request_hash", "action_hash",
                "profile_hash", "provider_binding_hash", "price_schedule_hash", "provider", "requested_model_id",
                "endpoint", "attempt_id", "deadline_ns", "authorized_at_ns", "expires_at_ns", "reservation_id",
                "reserved_cost_usd", "max_input_tokens", "max_output_tokens", "authorization_id", "capability_nonce",
                "authorization_hash", "issued_at_ns", "max_model_calls", "max_dynamic_tools"}
            strict_fields(body, expected=fields, required=fields, name="ActionAssessmentBrokerCapabilityV1")
            if (body["version"] != "ActionAssessmentBrokerCapabilityV1"
                    or body["task_identity"] != "ActionAssessmentProvider"
                    or body["provider"] != "deepseek" or body["requested_model_id"] != "deepseek-flash"
                    or body["endpoint"] != "https://api.deepseek.com/responses"
                    or body["max_model_calls"] != 1 or body["max_dynamic_tools"] != 0
                    or body["max_input_tokens"] != 12_000 or body["max_output_tokens"] != 2_048
                    or body["issued_at_ns"] != body["authorized_at_ns"]
                    or not body["issued_at_ns"] <= now_ns < body["expires_at_ns"]
                    or body["expires_at_ns"] > body["deadline_ns"]):
                raise ValueError
            for name in ("attempt_id", "reservation_id", "authorization_id", "capability_nonce"):
                if str(uuid.UUID(body[name])) != body[name]:
                    raise ValueError
            for name in ("packet_ref", "packet_hash", "request_hash", "action_hash", "profile_hash",
                         "provider_binding_hash", "price_schedule_hash"):
                if not isinstance(body[name], str) or len(body[name]) != 64:
                    raise ValueError
            auth_fields = {key: body[key] for key in fields - {"version", "authorization_hash", "issued_at_ns",
                "max_model_calls", "max_dynamic_tools"}}
            if sha256_json({"version": ActionAssessmentDispatchAuthorizationV1.VERSION, **auth_fields}) != body["authorization_hash"]:
                raise ValueError
            return body
        except Exception as exc:
            raise BrokerProtocolError("INVALID_OR_EXPIRED_ACTION_ASSESSMENT_CAPABILITY") from exc


def _capability_version(token: str) -> str:
    try:
        payload_part = token.split(".", 1)[0]
        body = json.loads(base64.urlsafe_b64decode(payload_part.encode("ascii")).decode("utf-8"))
        version = body.get("version") if isinstance(body, Mapping) else None
        if version in {"BrokerCapabilityV1", "BrokerCapabilityV2"}:
            return version
    except Exception:
        pass
    raise BrokerProtocolError("INVALID_OR_EXPIRED_CAPABILITY")


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

    def __init__(self, provider: Any, *, signing_key: bytes,
                 model_profile: AgentModelProfileV1 | AgentModelProfileV2 | ActionAssessmentProviderProfileV1 | None = None,
                 price_schedule_hash: str | None = None,
                 now_ns: Callable[[], int] = time.time_ns) -> None:
        if len(signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        if isinstance(model_profile, ActionAssessmentProviderProfileV1):
            if (price_schedule_hash != model_profile.price_schedule_hash
                    or getattr(provider, "provider_id", None) != model_profile.provider
                    or getattr(provider, "requested_model_id", None) != model_profile.requested_model_id
                    or getattr(provider, "endpoint", None) != model_profile.endpoint
                    or getattr(provider, "task_identity", None) != model_profile.task_identity):
                raise ValueError("action-assessment broker, provider and immutable task profile do not match")
        elif isinstance(model_profile, AgentModelProfileV2):
            if (price_schedule_hash is None or len(price_schedule_hash) != 64
                    or getattr(provider, "provider_id", None) != model_profile.provider
                    or getattr(provider, "requested_model_id", None) != model_profile.requested_model_id
                    or getattr(provider, "endpoint", None) != model_profile.base_url + model_profile.endpoint_path):
                raise ValueError("DeepSeek broker, provider and immutable profile binding do not match")
        elif price_schedule_hash is not None:
            raise ValueError("a V2 price-schedule binding is only valid with AgentModelProfileV2")
        self._provider = provider
        self._profile = model_profile
        self._price_schedule_hash = price_schedule_hash
        self.__signing_key = bytes(signing_key)
        self._now_ns = now_ns
        self._lock = threading.Lock()
        self._seen: OrderedDict[tuple[str, str], tuple[str, ProviderResultV1, int]] = OrderedDict()
        self._busy: set[str] = set()
        self._last_admitted_at_ns = 0

    def _admit_dispatch(self, key: tuple[str, str], input_hash: str,
                        authorization_id: str, now_ns: int) -> ProviderResultV1 | None:
        """Retain replay protection for exactly the signed capability lifetime.

        Expired capabilities cannot pass verification again. A clock rollback is
        rejected so it cannot revive a completion removed from this bounded cache.
        The durable controller remains the owner of accepted result recovery.
        """
        with self._lock:
            if now_ns < self._last_admitted_at_ns:
                raise BrokerProtocolError("BROKER_CLOCK_REGRESSION")
            self._last_admitted_at_ns = now_ns
            for prior_key, (_, _, expires_at_ns) in tuple(self._seen.items()):
                if expires_at_ns <= now_ns:
                    del self._seen[prior_key]
            prior = self._seen.get(key)
            if prior is not None:
                if prior[0] != input_hash:
                    raise BrokerProtocolError("CALL_INDEX_CONTRADICTION")
                return prior[1]
            if authorization_id in self._busy:
                raise BrokerProtocolError("DISPATCH_ALREADY_IN_PROGRESS")
            if self._busy:
                raise BrokerProtocolError("BROKER_SATURATED")
            # Reserve a completion slot before the provider effect. Concurrent
            # requests may never overrun the replay-protection memory budget.
            if len(self._seen) + len(self._busy) >= MAX_REPLAY_CACHE_ENTRIES:
                raise BrokerProtocolError("REPLAY_CACHE_FULL")
            self._busy.add(authorization_id)
        return None

    def assess_action_v1(self, *, capability: str, authorization_id: str, attempt_id: str,
                         request_data: Mapping[str, Any], packet_data: Mapping[str, Any],
                         now_ns: int | None = None) -> ProviderResultV1:
        """Execute one signed, direct, tool-free action-critic call."""
        now = self._now_ns() if now_ns is None else now_ns
        if not isinstance(self._profile, ActionAssessmentProviderProfileV1):
            raise BrokerProtocolError("CAPABILITY_PROFILE_VERSION_MISMATCH")
        body = ActionAssessmentBrokerCapabilityV1.verify(capability, signing_key=self.__signing_key, now_ns=now)
        try:
            request = ActionAssessmentRequestV2.from_dict(request_data)
            packet = SealedActionAssessmentPacketV1.from_dict(packet_data)
        except Exception as exc:
            raise BrokerProtocolError("INVALID_ACTION_ASSESSMENT_REQUEST") from exc
        if (body["authorization_id"] != authorization_id or body["attempt_id"] != attempt_id
                or body["packet_ref"] != packet.packet_ref or body["packet_hash"] != packet.content_hash
                or body["request_hash"] != request.content_hash or body["action_hash"] != packet.action_hash
                or body["profile_hash"] != self._profile.content_hash
                or body["provider_binding_hash"] != self._profile.provider_binding_hash
                or body["price_schedule_hash"] != self._price_schedule_hash
                or body["provider"] != self._profile.provider
                or body["requested_model_id"] != self._profile.requested_model_id
                or body["endpoint"] != self._profile.endpoint
                or request.packet_ref != packet.packet_ref or request.packet_hash != packet.content_hash
                or request.action_hash != packet.action_hash or request.profile_hash != self._profile.content_hash
                or request.provider_binding_hash != self._profile.provider_binding_hash
                or request.price_schedule_hash != self._price_schedule_hash
                or request.deadline_ns != body["deadline_ns"] or now >= packet.original_deadline_d_ns
                or len(canonical_json(packet.to_dict()).encode("utf-8")) > self._profile.packet_max_bytes):
            raise BrokerProtocolError("ACTION_ASSESSMENT_BINDING_MISMATCH")
        key = (authorization_id, body["capability_nonce"])
        input_hash = sha256_json({"request": request.to_dict(), "packet": packet.to_dict(),
                                  "attempt_id": attempt_id, "profile_hash": self._profile.content_hash})
        prior_result = self._admit_dispatch(key, input_hash, authorization_id, now)
        if prior_result is not None:
            return prior_result
        try:
            result = self._provider.assess(request, packet)
            if not isinstance(result, ProviderResultV1):
                result = ProviderResultV1("", None, None, False, False, 0, 0, None,
                                          "PROVIDER_RETURN_TYPE_INVALID", False)
            elif result.input_tokens > body["max_input_tokens"] or result.output_tokens > body["max_output_tokens"]:
                result = ProviderResultV1("", result.returned_model_id, None, result.refusal, True,
                    result.input_tokens, result.output_tokens, result.provider_request_id,
                    "TOKEN_LIMIT_EXCEEDED", False)
            with self._lock:
                self._seen[key] = (input_hash, result, body["expires_at_ns"])
            return result
        except ResearchProviderUnavailable as exc:
            result = ProviderResultV1("", None, None, exc.code == "PROVIDER_REFUSAL", False, 0, 0,
                                      None, exc.code, False)
            with self._lock:
                self._seen[key] = (input_hash, result, body["expires_at_ns"])
            return result
        except Exception:
            result = ProviderResultV1("", None, None, False, False, 0, 0,
                                      None, "PROVIDER_ERROR", False)
            with self._lock:
                self._seen[key] = (input_hash, result, body["expires_at_ns"])
            return result
        finally:
            with self._lock:
                self._busy.discard(authorization_id)

    def infer(self, *, capability: str, job_id: str, attempt_id: str, lease_epoch: int,
              call_index: int, request_data: Mapping[str, Any], evidence: Sequence[Mapping[str, Any]],
              now_ns: int | None = None) -> ProviderResultV1:
        now = self._now_ns() if now_ns is None else now_ns
        if isinstance(self._profile, ActionAssessmentProviderProfileV1):
            raise BrokerProtocolError("ACTION_ASSESSMENT_CAPABILITY_CANNOT_INVOKE_RESEARCH")
        capability_version = _capability_version(capability)
        if capability_version == "BrokerCapabilityV1":
            if isinstance(self._profile, AgentModelProfileV2):
                raise BrokerProtocolError("CAPABILITY_PROFILE_VERSION_MISMATCH")
            body = BrokerCapabilityV1.verify(capability, signing_key=self.__signing_key, now_ns=now)
            if isinstance(self._profile, AgentModelProfileV1) \
                    and body["model_profile_hash"] != self._profile.content_hash:
                raise BrokerProtocolError("CAPABILITY_PROFILE_VERSION_MISMATCH")
        else:
            if not isinstance(self._profile, AgentModelProfileV2) or self._price_schedule_hash is None:
                raise BrokerProtocolError("CAPABILITY_PROFILE_VERSION_MISMATCH")
            body = BrokerCapabilityV2.verify(capability, signing_key=self.__signing_key, now_ns=now)
            if (body["model_profile_hash"] != self._profile.content_hash
                    or body["provider_binding_hash"] != self._profile.provider_binding_hash
                    or body["price_schedule_hash"] != self._price_schedule_hash
                    or body["endpoint"] != self._profile.base_url + self._profile.endpoint_path
                    or body["provider"] != self._profile.provider
                    or body["requested_model_id"] != self._profile.requested_model_id):
                raise BrokerProtocolError("CAPABILITY_PROFILE_BINDING_MISMATCH")
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
                or (capability_version == "BrokerCapabilityV1" and
                    (body["provider"] != ALLOWED_PROVIDER or body["requested_model_id"] != ALLOWED_MODEL_ID))
                or (capability_version == "BrokerCapabilityV2" and
                    (body["provider"] != "deepseek" or body["requested_model_id"] != "deepseek-flash"))
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
        prior_result = self._admit_dispatch(idempotency_key, input_hash, body["authorization_id"], now)
        if prior_result is not None:
            return prior_result
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
                self._seen[idempotency_key] = (input_hash, result, body["expires_at_ns"])
            return result
        except ResearchProviderUnavailable as exc:
            result = ProviderResultV1("", None, None, exc.code == "PROVIDER_REFUSAL", False, 0, 0,
                                      None, exc.code, exc.retryable)
            with self._lock:
                self._seen[idempotency_key] = (input_hash, result, body["expires_at_ns"])
            return result
        except BrokerProtocolError:
            raise
        except Exception:
            result = ProviderResultV1("", None, None, False, False, 0, 0,
                                      None, "PROVIDER_ERROR", False)
            with self._lock:
                self._seen[idempotency_key] = (input_hash, result, body["expires_at_ns"])
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
        self._endpoint_identity: tuple[int, int] | None = None
        self._handler_slots = threading.BoundedSemaphore(MAX_BROKER_CONNECTIONS)
        self._connections: set[socket.socket] = set()
        self._connections_lock = threading.Lock()

    def start(self) -> None:
        self.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise RuntimeError("broker socket path already exists")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.socket_path))
            endpoint_stat = self.socket_path.stat()
            self._endpoint_identity = (endpoint_stat.st_dev, endpoint_stat.st_ino)
            os.chmod(self.socket_path, stat.S_IRUSR | stat.S_IWUSR)
            server.listen(MAX_BROKER_CONNECTIONS)
            server.settimeout(0.5)
        except BaseException:
            server.close()
            self._remove_owned_endpoint()
            raise
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
            if not self._handler_slots.acquire(blocking=False):
                try:
                    connection.settimeout(0.1)
                    _write_frame(connection, {"protocol_version": BROKER_PROTOCOL_VERSION,
                        "request_id": None, "ok": False, "error": "BROKER_SATURATED"})
                except OSError:
                    pass
                finally:
                    connection.close()
                continue
            with self._connections_lock:
                self._connections.add(connection)
            try:
                threading.Thread(target=self._handle, args=(connection,), daemon=True).start()
            except BaseException:
                with self._connections_lock:
                    self._connections.discard(connection)
                connection.close()
                self._handler_slots.release()
                raise

    def _handle(self, connection: socket.socket) -> None:
        try:
            connection.settimeout(BROKER_SOCKET_TIMEOUT_SECONDS)
            raw = _read_frame(connection)
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, Mapping):
                raise BrokerProtocolError("INVALID_BROKER_REQUEST")
            command = data.get("command")
            if command == "infer":
                fields = {"protocol_version", "request_id", "command", "capability", "job_id", "attempt_id",
                          "lease_epoch", "call_index", "request", "evidence"}
                row = strict_fields(data, expected=fields, required=fields, name="BrokerRequestV1")
                if (row["protocol_version"] != BROKER_PROTOCOL_VERSION
                        or not isinstance(row["request_id"], str) or len(row["request_id"]) != 36
                        or not isinstance(row["request"], Mapping) or not isinstance(row["evidence"], list)):
                    raise BrokerProtocolError("INVALID_BROKER_REQUEST")
                result = self.broker.infer(capability=row["capability"], job_id=row["job_id"],
                    attempt_id=row["attempt_id"], lease_epoch=row["lease_epoch"], call_index=row["call_index"],
                    request_data=row["request"], evidence=row["evidence"])
            elif command == "assess_action_v1":
                fields = {"protocol_version", "request_id", "command", "capability", "authorization_id",
                          "attempt_id", "request", "packet"}
                row = strict_fields(data, expected=fields, required=fields, name="ActionAssessmentBrokerRequestV1")
                if (row["protocol_version"] != BROKER_PROTOCOL_VERSION
                        or not isinstance(row["request_id"], str) or len(row["request_id"]) != 36
                        or not isinstance(row["request"], Mapping) or not isinstance(row["packet"], Mapping)):
                    raise BrokerProtocolError("INVALID_ACTION_ASSESSMENT_BROKER_REQUEST")
                result = self.broker.assess_action_v1(capability=row["capability"],
                    authorization_id=row["authorization_id"], attempt_id=row["attempt_id"],
                    request_data=row["request"], packet_data=row["packet"])
            else:
                raise BrokerProtocolError("BROKER_OPERATION_UNSUPPORTED")
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
            with self._connections_lock:
                self._connections.discard(connection)
            self._handler_slots.release()

    def close(self) -> None:
        self._stop.set()
        if self._socket is not None:
            self._socket.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        with self._connections_lock:
            connections = tuple(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        self._remove_owned_endpoint()

    def _remove_owned_endpoint(self) -> None:
        identity, self._endpoint_identity = self._endpoint_identity, None
        if identity is not None:
            try:
                current = self.socket_path.lstat()
                if (current.st_dev, current.st_ino) == identity:
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

    def assess_action_v1(self, *, capability: str, authorization_id: str, attempt_id: str,
                         request: ActionAssessmentRequestV2,
                         packet: SealedActionAssessmentPacketV1) -> ProviderResultV1:
        request_id = str(uuid.uuid4())
        payload = {"protocol_version": BROKER_PROTOCOL_VERSION, "request_id": request_id,
            "command": "assess_action_v1", "capability": capability,
            "authorization_id": authorization_id, "attempt_id": attempt_id,
            "request": request.to_dict(), "packet": packet.to_dict()}
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
    from pathlib import Path

    from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1
    from atlas.v2.agent_intelligence.profile import (
        deepseek_v41_flash_action_critic_profile,
        deepseek_v41_flash_model_profile,
    )
    from atlas.v2.agent_intelligence.provider import (
        DeepSeekResponsesActionAssessmentProvider,
        DeepSeekResponsesResearchProposalProvider,
        PydanticAIResearchProposalProvider,
    )

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
    provider_profile = os.environ.get("ATLAS_AGENT_PROVIDER_PROFILE", "openai-astra-v1")
    provider: PydanticAIResearchProposalProvider | DeepSeekResponsesResearchProposalProvider | DeepSeekResponsesActionAssessmentProvider
    if provider_profile == "openai-astra-v1":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise SystemExit("PROVIDER_CREDENTIAL_UNAVAILABLE")
        provider = PydanticAIResearchProposalProvider(api_key, model_id=ALLOWED_MODEL_ID)
        broker = InferenceBroker(provider, signing_key=signing_key)
    elif provider_profile == "deepseek-v41-flash-v1":
        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not api_key:
            raise SystemExit("PROVIDER_CREDENTIAL_UNAVAILABLE")
        repo_root = Path(__file__).resolve().parents[4]
        price_schedule = DeepSeekPriceScheduleV1.load(
            repo_root / "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json")
        profile = deepseek_v41_flash_model_profile(price_schedule=price_schedule,
            agent_lock_path=repo_root / "requirements-agent-lock.txt")
        provider = DeepSeekResponsesResearchProposalProvider(api_key)
        broker = InferenceBroker(provider, signing_key=signing_key, model_profile=profile,
                                 price_schedule_hash=price_schedule.content_hash)
    elif provider_profile == "deepseek-v41-action-critic-v1":
        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not api_key:
            raise SystemExit("PROVIDER_CREDENTIAL_UNAVAILABLE")
        repo_root = Path(__file__).resolve().parents[4]
        price_schedule = DeepSeekPriceScheduleV1.load(
            repo_root / "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json")
        critic_profile = deepseek_v41_flash_action_critic_profile(price_schedule=price_schedule,
            agent_lock_path=repo_root / "requirements-agent-lock.txt")
        provider = DeepSeekResponsesActionAssessmentProvider(api_key)
        broker = InferenceBroker(provider, signing_key=signing_key, model_profile=critic_profile,
                                 price_schedule_hash=price_schedule.content_hash)
    else:
        raise SystemExit("PROVIDER_PROFILE_UNSUPPORTED")
    server = InferenceBrokerServer(args.socket, broker)
    try:
        server.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    finally:
        server.close()
