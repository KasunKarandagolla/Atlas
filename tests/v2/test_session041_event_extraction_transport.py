from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Mapping
from typing import Any

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.agent_intelligence.contracts import (
    AgentEvidenceRefV1,
    EventExtractionRequestV1,
    EventExtractionV1,
)
from atlas.v2.agent_intelligence.event_extraction import EVENT_EXTRACTION_SCHEMA_HASH_V1
from atlas.v2.agent_intelligence.event_extraction_transport import (
    EVENT_EXTRACTION_BROKER_PROTOCOL_VERSION,
    EVENT_EXTRACTION_MODEL,
    EVENT_EXTRACTION_OPERATION,
    EVENT_EXTRACTION_PROFILE_HASH_V1,
    EVENT_EXTRACTION_REASONING_EFFORT,
    EventExtractionBroker,
    EventExtractionBrokerClientProvider,
    EventExtractionCapabilityV1,
    EventExtractionTransportError,
    EventExtractionUnixClientPort,
    EventExtractionUnixServer,
    OpenAIResponsesEventExtractionProvider,
    _handle_request,
    _pipe_name,
    _wire_request,
    issue_event_extraction_capability_v1,
)

SIGNING_KEY = b"s" * 32
RAW_REF = "a" * 64
RECEIPT_REF = "b" * 64


def _evidence(title: str = "CPI release next week") -> dict[str, Any]:
    return {
        "source_id": "official-test-feed",
        "source_class": "OFFICIAL_MACRO",
        "source_url": "https://example.test/feed.xml",
        "raw_ref": RAW_REF,
        "receipt_ref": RECEIPT_REF,
        "received_at_ns": 10,
        "items": [{
            "item_index": 0,
            "title": title,
            "text": "The CPI release is scheduled for Wednesday.",
            "url": "https://example.test/cpi",
            "claimed_published_at": "2026-10-01T00:00:00Z",
        }],
    }


def _request(*, deadline_ns: int = 10_000_000_000) -> EventExtractionRequestV1:
    if deadline_ns == 10_000_000_000:
        deadline_ns = time.time_ns() + 60_000_000_000
    return EventExtractionRequestV1(
        str(uuid.uuid4()), RAW_REF,
        (AgentEvidenceRefV1("get_registered_artifact", RAW_REF, 5),
         AgentEvidenceRefV1("get_registered_artifact", RECEIPT_REF, 10)),
        deadline_ns, EVENT_EXTRACTION_SCHEMA_HASH_V1,
    )


def _packed(evidence: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return (
        {"tool_name": "get_registered_artifact", "artifact_ref": RAW_REF,
         "available_through_ns": 5, "status": "PRESENT", "rows": [evidence]},
        {"tool_name": "get_registered_artifact", "artifact_ref": RECEIPT_REF,
         "available_through_ns": 10, "status": "PRESENT", "rows": [{
             "receipt_ref": RECEIPT_REF, "raw_ref": RAW_REF, "received_at_ns": 10,
         }]},
    )


def _result(request: EventExtractionRequestV1) -> EventExtractionV1:
    return EventExtractionV1(request.request_id, RAW_REF, (FrozenMap({
        "item_index": 0,
        "event_type": "US_CPI",
        "severity": "HIGH",
        "event_time_text": "Wednesday",
        "asset_mentions": [],
        "supporting_spans": ["CPI release is scheduled for Wednesday"],
        "unknown_fields": [],
    }),))


def _cap(request: EventExtractionRequestV1, evidence: tuple[Mapping[str, Any], ...],
         *, now_ns: int = 100) -> EventExtractionCapabilityV1:
    return issue_event_extraction_capability_v1(request, evidence, signing_key=SIGNING_KEY, issued_at_ns=now_ns)


class FakeProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.failure: Exception | None = None
        self.delay_until_release = False

    def extract(self, request: EventExtractionRequestV1,
                evidence: tuple[Mapping[str, Any], ...]) -> EventExtractionV1:
        self.calls += 1
        self.entered.set()
        if self.delay_until_release:
            self.release.wait()
        if self.failure is not None:
            raise self.failure
        return _result(request)


def test_capability_binds_request_evidence_schema_deadline_and_fixed_profile() -> None:
    evidence = _evidence()
    evidence_scope = _packed(evidence)
    request = _request()
    token = _cap(request, evidence_scope)
    assert token.token
    assert token.token not in repr(token)
    assert sha256_json({
        "version": "AtlasEventExtractionProviderProfileV1",
        "provider": "openai", "model": "gpt-6-astra", "api": "responses",
        "reasoning_effort": "medium", "structured_output": "json_schema_strict",
        "max_model_calls_per_request": 1, "tools": [], "fallback": False,
    }) == EVENT_EXTRACTION_PROFILE_HASH_V1
    with pytest.raises(EventExtractionTransportError, match="CAPABILITY_INVALID_OR_EXPIRED"):
        EventExtractionBroker(FakeProvider(), signing_key=SIGNING_KEY,
                              clock_ns=lambda: 101).execute(token, request, _packed(_evidence("changed")))


def test_wire_schema_rejects_unknown_fields_and_secret_fields() -> None:
    request = _request()
    evidence = _evidence()
    provider = FakeProvider()
    broker = EventExtractionBroker(provider, signing_key=SIGNING_KEY, clock_ns=lambda: 100)
    evidence_scope = _packed(evidence)
    capability = _cap(request, evidence_scope)
    wire = _wire_request(request, evidence_scope, capability)
    wire["unapproved"] = True
    response = _handle_request(broker, json.dumps(wire).encode())
    assert response["error"] == "WIRE_SCHEMA_INVALID"
    assert provider.calls == 0
    with pytest.raises(EventExtractionTransportError, match="SECRET_FIELD_FORBIDDEN"):
        _cap(request, _packed({**evidence, "api_key": "must-never-cross-wire"}))


def test_unix_transport_uses_separate_operation_and_returns_bound_result(tmp_path: Any) -> None:
    request, evidence = _request(), _evidence()
    evidence_scope = _packed(evidence)
    provider = FakeProvider()
    broker = EventExtractionBroker(provider, signing_key=SIGNING_KEY, clock_ns=lambda: 100)
    socket_path = tmp_path / "s7.sock"
    with EventExtractionUnixServer(socket_path, broker):
        provider_port = EventExtractionBrokerClientProvider(
            EventExtractionUnixClientPort(socket_path), signing_key=SIGNING_KEY, clock_ns=lambda: 100)
        result = provider_port.extract(request, evidence_scope)
        assert result == _result(request)
        assert provider.calls == 1
        assert socket_path.stat().st_mode & 0o777 == 0o600


def test_saturation_and_deadline_fail_closed_without_retry(tmp_path: Any) -> None:
    provider = FakeProvider()
    provider.delay_until_release = True
    broker = EventExtractionBroker(provider, signing_key=SIGNING_KEY, max_active=1,
                                   clock_ns=lambda: 100)
    socket_path = tmp_path / "s7-saturation.sock"
    first_request, first_evidence = _request(), _packed(_evidence())
    second_request, second_evidence = _request(), _packed(_evidence("second item"))
    first_cap, second_cap = _cap(first_request, first_evidence), _cap(second_request, second_evidence)
    errors: list[BaseException] = []
    with EventExtractionUnixServer(socket_path, broker):
        first = threading.Thread(target=lambda: _capture_call(errors, EventExtractionUnixClientPort(socket_path),
            first_request, first_evidence, first_cap))
        first.start()
        assert provider.entered.wait(1)
        try:
            with pytest.raises(EventExtractionTransportError, match="BROKER_SATURATED"):
                EventExtractionUnixClientPort(socket_path).extract(second_request, second_evidence, second_cap)
        finally:
            provider.release.set()
            first.join(2)
    assert not errors
    assert provider.calls == 1

    late_provider = FakeProvider()
    now = [100]
    late_broker = EventExtractionBroker(late_provider, signing_key=SIGNING_KEY, clock_ns=lambda: now[0])
    late_request, late_evidence = _request(deadline_ns=200), _packed(_evidence())
    late_cap = _cap(late_request, late_evidence)

    class LateProvider(FakeProvider):
        def extract(self, request: EventExtractionRequestV1,
                    evidence: tuple[Mapping[str, Any], ...]) -> EventExtractionV1:
            self.calls += 1
            now[0] = 201
            return _result(request)

    late_provider = LateProvider()
    late_broker = EventExtractionBroker(late_provider, signing_key=SIGNING_KEY, clock_ns=lambda: now[0])
    with pytest.raises(EventExtractionTransportError, match="PROVIDER_RESPONSE_LATE"):
        late_broker.execute(late_cap, late_request, late_evidence)
    assert late_provider.calls == 1


def _capture_call(errors: list[BaseException], client: EventExtractionUnixClientPort,
                  request: EventExtractionRequestV1, evidence: tuple[Mapping[str, Any], ...],
                  capability: EventExtractionCapabilityV1) -> None:
    try:
        client.extract(request, evidence, capability)
    except BaseException as exc:
        errors.append(exc)


def test_provider_failure_consumes_capability_and_is_not_retried() -> None:
    request, evidence = _request(), _packed(_evidence())
    provider = FakeProvider()
    provider.failure = RuntimeError("provider secret detail must not escape")
    broker = EventExtractionBroker(provider, signing_key=SIGNING_KEY, clock_ns=lambda: 100)
    capability = _cap(request, evidence)
    with pytest.raises(EventExtractionTransportError, match="PROVIDER_UNAVAILABLE") as caught:
        broker.execute(capability, request, evidence)
    assert "provider secret detail" not in str(caught.value)
    with pytest.raises(EventExtractionTransportError, match="CAPABILITY_ALREADY_CONSUMED"):
        broker.execute(capability, request, evidence)
    assert provider.calls == 1


def test_openai_provider_uses_one_fixed_tool_free_responses_call() -> None:
    request, evidence = _request(), _packed(_evidence())
    output = json.dumps({"items": [_result(request).extracted_events[0].to_dict()]})

    class Responses:
        calls: list[dict[str, Any]] = []

        def create(self, **kwargs: Any) -> Any:
            self.calls.append(kwargs)
            return {"status": "completed", "output_text": output, "refusal": None}

    class Client:
        max_retries = 0

        def __init__(self) -> None:
            self.responses = Responses()

    client = Client()
    provider = OpenAIResponsesEventExtractionProvider(client)
    provider.extract(request, evidence)
    call = client.responses.calls[0]
    assert len(client.responses.calls) == 1
    assert call["model"] == EVENT_EXTRACTION_MODEL
    assert call["reasoning"] == {"effort": EVENT_EXTRACTION_REASONING_EFFORT}
    assert call["tools"] == [] and call["tool_choice"] == "none"
    assert call["text"]["format"]["type"] == "json_schema"
    assert call["text"]["format"]["strict"] is True
    assert call["store"] is False and call["max_output_tokens"] == 8_000
    assert "api_key" not in json.dumps(call).lower()


def test_existing_critic_endpoint_rejects_s7_operation() -> None:
    from atlas.v2.agent_intelligence.broker import BROKER_PROTOCOL_VERSION
    from atlas.v2.agent_intelligence.windows_broker import _response

    class Critic:
        calls = 0

        def assess_action_v1(self, **_kwargs: Any) -> Any:
            self.calls += 1
            raise AssertionError("critic must not receive S7 extraction")

    critic = Critic()
    raw = json.dumps({
        "protocol_version": BROKER_PROTOCOL_VERSION,
        "request_id": str(uuid.uuid4()),
        "command": EVENT_EXTRACTION_OPERATION,
        "capability": "not-a-critic-capability",
        "request": {},
        "evidence": {},
    }).encode()
    response = json.loads(_response(critic, raw))
    assert response["ok"] is False
    assert response["protocol_version"] == BROKER_PROTOCOL_VERSION
    assert critic.calls == 0


def test_profile_is_distinct_from_critic_protocol_operation() -> None:
    assert EVENT_EXTRACTION_OPERATION == "ATLAS_S7_EVENT_EXTRACTION_V1"
    assert EVENT_EXTRACTION_BROKER_PROTOCOL_VERSION == 1
    event_pipe = rf"\\.\pipe\AtlasEventExtract-{uuid.uuid4()}"
    assert _pipe_name(event_pipe) == event_pipe
    from atlas.v2.agent_intelligence.windows_broker import _pipe_name as critic_pipe_name

    with pytest.raises(ValueError):
        critic_pipe_name(event_pipe)
