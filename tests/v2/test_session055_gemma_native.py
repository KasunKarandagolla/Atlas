"""Deterministic Gemma adapter contract tests; all provider replies are fixtures."""

from __future__ import annotations

import io
import json
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.research.agent import (
    ProviderCapabilitiesV1,
    ResearchBudgetV1,
    ResearchJobV1,
    validate_response,
)
from atlas.v2.research.decision_brief import DecisionBriefV1
from atlas.v2.research.gemma_native import (
    GEMINI_API_HOST,
    GEMMA_MODEL_ID,
    GeminiJobDispatchPermitV1,
    GeminiModelMetadataPermitV1,
    GeminiNativeClient,
    GeminiProviderError,
    GeminiSmokePermitV1,
    GemmaResearchProviderV1,
)
from atlas.v2.research.memory import ResearchMemoryV1

JOB_ID = "fixture-gemma-job"
SNAPSHOT = sha256_json("snapshot")
CANDIDATE = sha256_json("candidate")
ACTION = sha256_json("action")
EVIDENCE = sha256_json("evaluation")


def research_job(*, status: str = "COMPLETE", capabilities=None) -> ResearchJobV1:
    brief = DecisionBriefV1(status, (), FrozenMap({
        "slot_id": "slot-1", "snapshot_ref": SNAPSHOT, "cutoff_ns": 10, "deadline_ns": 100,
        "shortlist": [CANDIDATE], "economics": [{
            "candidate_id": CANDIDATE, "action_hash": ACTION, "supported_choice": True,
            "evidence_refs": {"evaluation": EVIDENCE},
        }], "facts": [], "tool_allowlist": ["inspect_snapshot_v1", "inspect_economics_v1", "inspect_memory_v1"],
        "authority": "ZERO", "shadow_only": True,
    }))
    if capabilities is None:
        caps = __import__("atlas.v2.research.agent", fromlist=["unverified_provider_capabilities"])
        capabilities = caps.unverified_provider_capabilities("gemini", GEMMA_MODEL_ID)
    return ResearchJobV1(JOB_ID, brief, capabilities, ResearchBudgetV1("FIXTURE", 1, 0, 0, 0, 4000), 11)


class Response(io.BytesIO):
    def __init__(self, payload: object, url: str | None = None):
        super().__init__(json.dumps(payload).encode())
        self.url = url

    def geturl(self) -> str:
        return self.url or f"https://{GEMINI_API_HOST}/v1beta/models/{GEMMA_MODEL_ID}:generateContent"


def permit() -> GeminiSmokePermitV1:
    return GeminiSmokePermitV1(
        "fixture-only", sha256_json("owner-verified-free-tier-fixture"), max_requests=2,
        free_tier_verified=True, no_billing_authorized=True, max_input_bytes=4096, max_output_tokens=256,
    )


def verified_capabilities() -> ProviderCapabilitiesV1:
    return ProviderCapabilitiesV1(
        "GEMINI_FAKE_CAPABILITY_FIXTURE_V1", "gemini", GEMMA_MODEL_ID, True, False, True, True,
        True, True, 4000, False, "FIXTURE_ONLY", "NOT_PROBED", sha256_json("fixture capability evidence"),
    )


def job_permit(job: ResearchJobV1) -> GeminiJobDispatchPermitV1:
    return GeminiJobDispatchPermitV1(
        job.request_hash, sha256_json({"fixture_intent": job.request_hash}),
        sha256_json("free-tier-fixture"), True, True,
        max_input_bytes=job.budget.max_projected_tokens,
        max_output_tokens=min(job.budget.max_projected_tokens, job.capabilities.context_limit or 0),
    )


def model_payload() -> dict[str, object]:
    return {"name": f"models/{GEMMA_MODEL_ID}", "supportedGenerationMethods": ["generateContent"]}


def generated_payload(output: dict[str, object], *, usage: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "modelVersion": GEMMA_MODEL_ID,
        "candidates": [{"content": {"parts": [{"text": json.dumps(output)}], "role": "model"},
                       "finishReason": "STOP"}],
    }
    if usage:
        payload["usageMetadata"] = {"promptTokenCount": 12, "candidatesTokenCount": 20,
                                    "totalTokenCount": 32}
    return payload


def output(*, job_id: str = JOB_ID) -> dict[str, object]:
    return {"job_id": job_id, "snapshot_ref": SNAPSHOT, "cutoff_ns": 10, "kind": "SELECT_ONE",
            "candidate_id": CANDIDATE, "action_hash": ACTION, "evidence_refs": [EVIDENCE]}


class MemoryRepository:
    def __init__(self, initial: dict[str, dict[str, object]]):
        self.read_only = False
        self.rows = {ref: SimpleNamespace(content_hash=sha256_json(body), metadata=body,
                                          available_at_ns=0)
                     for ref, body in initial.items()}

    def get_artifact(self, ref):
        return self.rows.get(ref)

    def register_artifact(self, entry):
        self.rows[entry.artifact_ref] = SimpleNamespace(artifact_type=entry.artifact_type,
            content_hash=entry.content_hash, metadata=dict(entry.metadata),
            available_at_ns=entry.available_at_ns)

    def atomic_composition(self):
        return nullcontext()


def production_memory(job: ResearchJobV1):
    receipt_ref = sha256_json("production-receipt")
    source_population_ref = sha256_json("whole-candidate-population")
    brief_ref = ResearchMemoryV1.production_brief_identity(receipt_ref)
    memory_ref = ResearchMemoryV1.production_memory_identity(receipt_ref)
    identity_ref = ResearchMemoryV1.production_projection_identity(receipt_ref)
    projection = {"version": "PRODUCTION_DECISION_BRIEF_PROJECTION_V1", "authority": "ZERO",
                  "receipt_ref": receipt_ref, "source_population_ref": source_population_ref,
                  "brief": job.brief.to_dict(), "status": job.brief.status, "source_refs": [source_population_ref]}
    memory = {"version": "PRODUCTION_INTELLIGENCE_MEMORY_PROJECTION_V1", "authority": "ZERO",
              "receipt_ref": receipt_ref, "brief_ref": brief_ref, "brief_status": job.brief.status,
              "source_population_ref": source_population_ref, "source_refs": [source_population_ref],
              "created_at_ns": 11, "due_work_status": "AVAILABLE", "due_work_reason": "FIXTURE",
              "due_work_refs": [], "provider_job_status": "NOT_CREATED_NO_AUTHORITY"}
    pointer = {"version": "PRODUCTION_INTELLIGENCE_MEMORY_IDENTITY_V1", "authority": "ZERO",
               "receipt_ref": receipt_ref, "brief_ref": brief_ref, "memory_ref": memory_ref}
    repo = MemoryRepository({brief_ref: projection, memory_ref: memory, identity_ref: pointer})
    repo.rows[receipt_ref] = SimpleNamespace(artifact_type="OpsSupervisorReceiptV1",
        content_hash=receipt_ref, metadata={}, available_at_ns=1)
    return repo, receipt_ref, source_population_ref


def test_native_client_is_pinned_and_redacts_credentials():
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=Mock())
    assert client.host == GEMINI_API_HOST
    assert client.model_id == GEMMA_MODEL_ID
    assert "fixture-secret" not in repr(client)
    with pytest.raises(ValueError, match="host"):
        GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=Mock(),
                           host="generativelanguage.google.internal")


@pytest.mark.parametrize(
    ("request_timeout_seconds", "remaining_seconds", "expected_timeout"),
    [(60.0, 60.0, 60.0), (35.0, 60.0, 35.0), (35.0, 8.0, 8.0)],
)
def test_request_timeout_is_configurable_and_capped_by_absolute_deadline(
    request_timeout_seconds, remaining_seconds, expected_timeout
):
    opener = Mock()
    opener.open.return_value = Response(generated_payload(output()))
    now_ns = 1_000_000_000
    client = GeminiNativeClient(
        api_key="fixture-secret", permit=permit(), opener=opener,
        clock_ns=lambda: now_ns, monotonic_ns=lambda: 0,
        request_timeout_seconds=request_timeout_seconds,
    )

    client.generate_json("synthetic prompt", deadline_ns=now_ns + int(remaining_seconds * 1e9))

    assert opener.open.call_args.kwargs["timeout"] == expected_timeout
    assert opener.open.call_count == 1


def test_request_timeout_configuration_is_bounded_and_environment_driven(monkeypatch):
    with pytest.raises(ValueError, match="between 0.1 and 120"):
        GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=Mock(),
                           request_timeout_seconds=120.1)
    monkeypatch.setenv("GEMINI_API_KEY", "fixture-secret")
    monkeypatch.setenv("GEMMA_MODEL", GEMMA_MODEL_ID)
    monkeypatch.setenv("ATLAS_GEMINI_FREE_TIER_VERIFIED", "true")
    monkeypatch.setenv("ATLAS_GEMINI_REQUEST_TIMEOUT_SECONDS", "9.5")
    client = GeminiNativeClient.from_environment(permit=permit(), opener=Mock())
    assert client._request_timeout_seconds == 9.5
    monkeypatch.setenv("ATLAS_GEMINI_REQUEST_TIMEOUT_SECONDS", "invalid")
    with pytest.raises(GeminiProviderError, match="CONFIGURED_TIMEOUT_INVALID"):
        GeminiNativeClient.from_environment(permit=permit(), opener=Mock())


def test_model_and_structured_output_probes_use_native_api_and_hide_payloads():
    opener = Mock()
    opener.open.side_effect = [Response(model_payload()), Response(generated_payload(output()))]
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                clock_ns=lambda: 20, monotonic_ns=lambda: 0)
    result = client.probe_capabilities(deadline_ns=100, synthetic_prompt="Return the word READY.")
    assert result.requested_model_id == GEMMA_MODEL_ID
    assert result.returned_model_id == f"models/{GEMMA_MODEL_ID}"
    assert result.model_available is True and result.text_response is True
    assert result.structured_json is True
    assert result.usage_metadata is True
    assert result.read_only_tools is False
    assert result.cancellation is False
    assert result.pricing_verified is False
    assert result.requests_made == 2
    assert result.request_body_bytes > 0 and result.response_body_bytes > 0
    model_request, generation_request = [call.args[0] for call in opener.open.call_args_list]
    assert model_request.full_url == f"https://{GEMINI_API_HOST}/v1beta/models/{GEMMA_MODEL_ID}"
    assert generation_request.full_url == f"https://{GEMINI_API_HOST}/v1beta/models/{GEMMA_MODEL_ID}:generateContent"
    assert model_request.get_header("X-goog-api-key") == "fixture-secret"
    assert "fixture-secret" not in model_request.full_url + generation_request.full_url
    request_body = json.loads(generation_request.data)
    assert request_body["generationConfig"]["responseMimeType"] == "application/json"
    assert request_body["generationConfig"]["maxOutputTokens"] == 256
    assert "tools" not in request_body
    assert opener.open.call_args_list[0].kwargs["timeout"] > 0


def test_metadata_probe_checks_one_model_get_without_free_tier_marker_or_generation(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fixture-secret")
    monkeypatch.setenv("GEMMA_MODEL", GEMMA_MODEL_ID)
    monkeypatch.delenv("ATLAS_GEMINI_FREE_TIER_VERIFIED", raising=False)
    opener = Mock()
    opener.open.return_value = Response(model_payload())
    client = GeminiNativeClient.from_environment(
        permit=GeminiModelMetadataPermitV1("metadata-only-fixture"), opener=opener,
        clock_ns=lambda: 20, monotonic_ns=lambda: 0,
    )

    result = client.probe_model_metadata(deadline_ns=100)

    assert result.requested_model_id == GEMMA_MODEL_ID
    assert result.returned_model_id == f"models/{GEMMA_MODEL_ID}"
    assert result.supported_generation_methods == ("generateContent",)
    assert result.generate_content_available is True
    assert result.requests_made == 1
    assert result.response_body_bytes > 0
    request = opener.open.call_args.args[0]
    assert request.full_url == f"https://{GEMINI_API_HOST}/v1beta/models/{GEMMA_MODEL_ID}"
    assert request.get_method() == "GET" and request.data is None
    assert request.get_header("X-goog-api-key") == "fixture-secret"
    assert "fixture-secret" not in request.full_url

    with pytest.raises(GeminiProviderError, match="GENERATION_NOT_AUTHORIZED"):
        client.generate_json("synthetic prompt", deadline_ns=100)
    with pytest.raises(GeminiProviderError, match="GENERATION_NOT_AUTHORIZED"):
        client._request(method="POST", body={"contents": []}, deadline_ns=100)
    assert opener.open.call_count == 1
    assert client.requests_made == 1


def test_metadata_probe_fails_closed_when_generation_method_is_unavailable():
    opener = Mock()
    opener.open.return_value = Response({
        "name": f"models/{GEMMA_MODEL_ID}", "supportedGenerationMethods": ["countTokens"],
    })
    client = GeminiNativeClient(
        api_key="fixture-secret", permit=GeminiModelMetadataPermitV1("metadata-only-fixture"),
        opener=opener, clock_ns=lambda: 20, monotonic_ns=lambda: 0,
    )

    result = client.probe_model_metadata(deadline_ns=100)

    assert result.generate_content_available is False
    assert result.failure_code == "MODEL_METHOD_UNAVAILABLE"
    assert result.requests_made == 1
    assert opener.open.call_count == 1


def test_capability_probe_does_not_claim_schema_support_for_invalid_json_shape():
    opener = Mock()
    opener.open.side_effect = [Response(model_payload()), Response(generated_payload({"not": "the schema"}))]
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                clock_ns=lambda: 20, monotonic_ns=lambda: 0)
    result = client.probe_capabilities(deadline_ns=100, synthetic_prompt="Return a synthetic schema object.")
    assert result.model_available and result.text_response
    assert result.structured_json is False


def test_research_provider_returns_only_validated_contract_shape_and_never_tools():
    opener = Mock()
    opener.open.return_value = Response(generated_payload(output()))
    job = research_job(capabilities=verified_capabilities())
    client = GeminiNativeClient(api_key="fixture-secret", permit=job_permit(job), opener=opener,
                                clock_ns=lambda: 20, monotonic_ns=lambda: 0)
    provider = GemmaResearchProviderV1(client)
    response = provider.respond(job, ())
    assert validate_response(job, response) is None
    assert provider.returned_model == f"models/{GEMMA_MODEL_ID}"
    sent = json.loads(opener.open.call_args.args[0].data)
    assert sent["contents"][0]["parts"][0]["text"].find("UNTRUSTED_DATA") >= 0
    assert "tools" not in sent
    assert sent["generationConfig"]["maxOutputTokens"] == 4000


def test_invalid_identity_missing_text_prompt_injection_and_tool_parts_fail_closed():
    for payload, reason in [
        ({"modelVersion": "models/other", "candidates": [{"content": {"parts": [{"text": json.dumps(output())}]}}]},
         "IDENTITY"),
        ({"modelVersion": GEMMA_MODEL_ID, "candidates": []}, "RESPONSE"),
        ({"modelVersion": GEMMA_MODEL_ID, "candidates": [{"content": {"parts": [{"functionCall": {"name": "inspect_snapshot_v1"}}]}}]},
         "CAPABILITY"),
    ]:
        opener = Mock()
        opener.open.return_value = Response(payload)
        client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                    clock_ns=lambda: 20, monotonic_ns=lambda: 0)
        with pytest.raises(GeminiProviderError, match=reason):
            client.generate_json("synthetic prompt", deadline_ns=100)

    injection = output() | {"candidate_id": "injected-candidate"}
    opener = Mock()
    opener.open.return_value = Response(generated_payload(injection))
    job = research_job(capabilities=verified_capabilities())
    provider = GemmaResearchProviderV1(GeminiNativeClient(
        api_key="fixture-secret", permit=job_permit(job), opener=opener,
        clock_ns=lambda: 20, monotonic_ns=lambda: 0))
    with pytest.raises(GeminiProviderError, match="INVALID_RESPONSE_CANDIDATE_ACTION_SCOPE"):
        provider.respond(job, ())


def test_request_size_deadline_and_rate_limit_errors_are_bounded_and_sanitized():
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=Mock(),
                                clock_ns=lambda: 200, monotonic_ns=lambda: 0)
    with pytest.raises(GeminiProviderError, match="DEADLINE"):
        client.generate_json("small", deadline_ns=199)
    with pytest.raises(GeminiProviderError, match="INPUT_BUDGET"):
        client.generate_json("x" * 4097, deadline_ns=300)
    opener = Mock()
    opener.open.side_effect = OSError("429 secret fixture-secret response")
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                clock_ns=lambda: 200, monotonic_ns=lambda: 0)
    with pytest.raises(GeminiProviderError) as raised:
        client.generate_json("small", deadline_ns=300)
    assert "fixture-secret" not in str(raised.value)
    assert raised.value.code == "PROVIDER_ERROR"
    assert opener.open.call_count == 1
    assert client.request_body_bytes == len(opener.open.call_args.args[0].data)

    opener = Mock()
    opener.open.side_effect = HTTPError("https://redacted", 429, "quota fixture-secret",
                                        {}, io.BytesIO(b"sensitive fixture-secret body"))
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                clock_ns=lambda: 200, monotonic_ns=lambda: 0)
    with pytest.raises(GeminiProviderError) as limited:
        client.generate_json("small", deadline_ns=300)
    assert limited.value.code == "RATE_LIMITED"
    assert "fixture-secret" not in str(limited.value)
    assert opener.open.call_count == 1

    opener = Mock()
    opener.open.side_effect = TimeoutError("sensitive fixture-secret timeout detail")
    client = GeminiNativeClient(api_key="fixture-secret", permit=permit(), opener=opener,
                                clock_ns=lambda: 200, monotonic_ns=lambda: 0)
    with pytest.raises(GeminiProviderError) as timed_out:
        client.generate_json("small", deadline_ns=300)
    assert timed_out.value.code == "PROVIDER_TIMEOUT"
    assert "fixture-secret" not in str(timed_out.value)
    assert opener.open.call_count == 1


def test_free_tier_and_billing_gate_are_mandatory_for_any_real_transport_object():
    evidence = sha256_json("owner-evidence")
    with pytest.raises(ValueError, match="free-tier"):
        GeminiSmokePermitV1("fixture", evidence, False)
    with pytest.raises(ValueError, match="billing"):
        GeminiSmokePermitV1("fixture", evidence, True)
    with pytest.raises(ValueError, match="billing"):
        GeminiSmokePermitV1("fixture", evidence, True, no_billing_authorized=False)
    with pytest.raises(ValueError, match="request ceiling"):
        GeminiSmokePermitV1("fixture", evidence, True, no_billing_authorized=True, max_requests=3)


def test_environment_loader_requires_explicit_free_tier_flag_and_exact_model(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fixture-secret")
    monkeypatch.setenv("GEMMA_MODEL", GEMMA_MODEL_ID)
    monkeypatch.delenv("ATLAS_GEMINI_FREE_TIER_VERIFIED", raising=False)
    with pytest.raises(GeminiProviderError, match="FREE_TIER_CONFIGURATION_UNVERIFIED"):
        GeminiNativeClient.from_environment(permit=permit(), opener=Mock())
    monkeypatch.setenv("ATLAS_GEMINI_FREE_TIER_VERIFIED", "true")
    client = GeminiNativeClient.from_environment(permit=permit(), opener=Mock())
    assert "fixture-secret" not in repr(client)
    monkeypatch.setenv("GEMMA_MODEL", "another-model")
    with pytest.raises(GeminiProviderError, match="CONFIGURED_MODEL_MISMATCH"):
        GeminiNativeClient.from_environment(permit=permit(), opener=Mock())


def test_production_memory_gemmas_shadow_is_tagged_and_cannot_touch_original_brief():
    capabilities = ProviderCapabilitiesV1(
        "GEMINI_FAKE_CAPABILITY_FIXTURE_V1", "gemini", GEMMA_MODEL_ID, True, False, True, True,
        True, True, 4000, False, "FIXTURE_ONLY", "NOT_PROBED", sha256_json("fixture capability evidence"),
    )
    job = research_job(capabilities=capabilities)
    repo, receipt_ref, a0_population_ref = production_memory(job)
    original_brief_hash = job.brief.content_hash
    memory = ResearchMemoryV1(repo)
    intent = memory.prepare_gemma_shadow_dispatch(job, receipt_ref=receipt_ref, now_ns=20)
    assert intent["status"] == "READY_TO_DISPATCH"
    result = memory.record_gemma_shadow_result(
        job, receipt_ref=receipt_ref, response=output(), returned_model=GEMMA_MODEL_ID,
        usage_metadata=(12, 20, 32), transport_bytes=(300, 450), now_ns=30,
    )
    assert result["status"] == "VALID_SHADOW"
    assert result["authority_tag"] == "ZERO_AUTHORITY_SHADOW"
    assert result["decision"].to_dict() == output()
    assert result["a0_population_ref"] == a0_population_ref
    assert result["brief_ref"] == ResearchMemoryV1.production_brief_identity(receipt_ref)
    assert memory._read(result["brief_ref"])["brief"]["body"] == job.brief.body.to_dict()
    assert job.brief.content_hash == original_brief_hash
    assert result["usage_metadata"].to_dict() == {
        "prompt_tokens": 12, "candidate_tokens": 20, "total_tokens": 32,
    }
    assert result["transport_bytes"].to_dict() == {"request_body_bytes": 300, "response_body_bytes": 450}


def test_blocked_production_brief_and_unverified_gemma_capabilities_never_dispatch():
    blocked = research_job(status="BLOCKED")
    repo, receipt_ref, _ = production_memory(blocked)
    result = ResearchMemoryV1(repo).prepare_gemma_shadow_dispatch(blocked, receipt_ref=receipt_ref, now_ns=20)
    assert result["status"] == "LLM_NOT_CALLED"
    assert result["reason"] == "PRODUCTION_BRIEF_NOT_COMPLETE"

    eligible = research_job()
    repo, receipt_ref, _ = production_memory(eligible)
    result = ResearchMemoryV1(repo).prepare_gemma_shadow_dispatch(eligible, receipt_ref=receipt_ref, now_ns=20)
    assert result["status"] == "BUDGET_DENIED"
    assert result["reason"] == "UNKNOWN_PRICE"
