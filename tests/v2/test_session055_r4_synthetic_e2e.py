"""Synthetic R4 composition: S47 Brief -> fake Gemini -> durable memory -> DEMO_ONLY request."""

from __future__ import annotations

import io
import json
from unittest.mock import Mock

from atlas.runtime.binance_futures_demo_r4 import (
    DemoOnlyAdapter,
    DemoOnlyCredentials,
    DemoOnlyIdentity,
    build_demo_order_request,
    normalize_demo_order_state,
)
from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.desktop.projection import project_snapshot
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.agent import ProviderCapabilitiesV1
from atlas.v2.research.gemma_native import (
    GEMINI_API_HOST,
    GEMMA_MODEL_ID,
    GeminiJobDispatchPermitV1,
    GeminiNativeClient,
    GemmaResearchProviderV1,
)

from .test_session055_gemma_native import generated_payload, output, research_job


class GeminiFixtureResponse(io.BytesIO):
    def geturl(self) -> str:
        return f"https://{GEMINI_API_HOST}/v1beta/models/{GEMMA_MODEL_ID}:generateContent"


def test_fake_gemma_memory_and_demo_request_keep_original_economics_unchanged(tmp_path):
    capabilities = ProviderCapabilitiesV1(
        "GEMINI_FAKE_E2E_CAPABILITIES_V1", "gemini", GEMMA_MODEL_ID, True, False, True, True,
        True, True, 4000, False, "FIXTURE_ONLY", "NOT_PROBED", sha256_json("fixture evidence"),
    )
    source_job = research_job(capabilities=capabilities)
    from atlas.v2.research.memory import ResearchMemoryV1
    receipt_ref = sha256_json("synthetic-r4-receipt")
    population_ref = sha256_json("synthetic-a0-population")
    brief_ref = ResearchMemoryV1.production_brief_identity(receipt_ref)
    memory_ref = ResearchMemoryV1.production_memory_identity(receipt_ref)
    identity_ref = ResearchMemoryV1.production_projection_identity(receipt_ref)
    projection = {"version": "PRODUCTION_DECISION_BRIEF_PROJECTION_V1", "authority": "ZERO",
                  "receipt_ref": receipt_ref, "source_population_ref": population_ref,
                  "brief": source_job.brief.to_dict(), "status": "COMPLETE", "source_refs": [population_ref]}
    memory_body = {"version": "PRODUCTION_INTELLIGENCE_MEMORY_PROJECTION_V1", "authority": "ZERO",
                   "receipt_ref": receipt_ref, "brief_ref": brief_ref, "brief_status": "COMPLETE",
                   "source_population_ref": population_ref, "source_refs": [population_ref],
                   "created_at_ns": 11, "due_work_status": "AVAILABLE", "due_work_reason": "FIXTURE",
                   "due_work_refs": [], "provider_job_status": "NOT_CREATED_NO_AUTHORITY"}
    pointer = {"version": "PRODUCTION_INTELLIGENCE_MEMORY_IDENTITY_V1", "authority": "ZERO",
               "receipt_ref": receipt_ref, "brief_ref": brief_ref, "memory_ref": memory_ref}
    original_brief_hash = source_job.brief.content_hash
    original_economics = tuple(source_job.brief.body.get("economics", ()))

    database = tmp_path / "r4-synthetic.sqlite"
    with OpsRepository(database) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            receipt_ref, "OpsSupervisorReceiptV1", sha256_json({"receipt": receipt_ref}), 11, 11,
            {"receipt": {"receipt_ref": receipt_ref}},
        ))
        for ref, kind, body in (
            (brief_ref, "ProductionDecisionBriefProjectionV1", projection),
            (memory_ref, "ProductionIntelligenceMemoryProjectionV1", memory_body),
            (identity_ref, "ProductionIntelligenceMemoryIdentityV1", pointer),
        ):
            repository.register_artifact(ArtifactIndexEntryV2(ref, kind, sha256_json(body), 11, 11, body))
        memory = ResearchMemoryV1(repository)
        job = memory.build_production_gemma_job(
            receipt_ref, capabilities=capabilities, budget=source_job.budget, now_ns=20,
        )
        assert job is not None
        intent = memory.prepare_gemma_shadow_dispatch(job, receipt_ref=receipt_ref, now_ns=20)
        assert intent["status"] == "READY_TO_DISPATCH"

        opener = Mock()
        opener.open.return_value = GeminiFixtureResponse(
            json.dumps(generated_payload(output(job_id=job.job_id))).encode())
        client = GeminiNativeClient(
            api_key="fixture-gemini-key",
            permit=GeminiJobDispatchPermitV1(
                job.request_hash, intent["intent_ref"], sha256_json("free-tier-fixture"), True, True,
                max_input_bytes=job.budget.max_projected_tokens,
                max_output_tokens=min(job.budget.max_projected_tokens, job.capabilities.context_limit or 0),
            ),
            opener=opener, clock_ns=lambda: 20, monotonic_ns=lambda: 0,
        )
        provider = GemmaResearchProviderV1(client)
        proposal = provider.respond(job, ())
        assert proposal == output(job_id=job.job_id)
        result = memory.record_gemma_shadow_result(
            job, receipt_ref=receipt_ref, response=proposal, returned_model=provider.returned_model,
            usage_metadata=provider.usage_metadata, transport_bytes=provider.transport_bytes, now_ns=30,
        )
        assert result["authority_tag"] == "ZERO_AUTHORITY_SHADOW"
        assert result["a0_population_ref"] == population_ref
        assert canonical_json(result["decision"]) == canonical_json(output(job_id=job.job_id))
        assert job.brief.content_hash == original_brief_hash
        assert tuple(job.brief.body.get("economics", ())) == original_economics
        assert opener.open.call_count == 1
        desktop = project_snapshot(repository, now_ns=30)
        desktop_status = {item.name: item for item in desktop.overview.statuses}
        assert desktop_status["gemma_shadow"].state == "VALID_SHADOW"
        assert "proposal=SELECT_ONE" in desktop_status["gemma_shadow"].value
        assert desktop_status["demo_only"].state == "UNAVAILABLE"

        # Deterministic test order data comes from the A0 fixture; the Gemma
        # response is deliberately not an input to the request builder.
        a0_request = build_demo_order_request(symbol="BTCUSDT", side="BUY", quantity="0.001",
                                              client_order_id="r4-e2e-a0", reduce_only=False)
        demo = DemoOnlyAdapter(identity=DemoOnlyIdentity("demo-scope", "demo-key-ref"),
                               credentials=DemoOnlyCredentials("fixture-demo-key", "fixture-demo-secret"),
                               clock_ms=lambda: 1_800_000_000_123, allow_fixture_credentials=True)
        signed_read = demo.build_signed_read("/fapi/v3/account")
        assert "demo-fapi.binance.com" in signed_read.full_url
        assert a0_request.venue_effect == "REQUEST_GENERATION_ONLY"
        assert normalize_demo_order_state({"status": "PARTIALLY_FILLED"}).value == "PARTIALLY_FILLED"
        assert not hasattr(demo, "submit_order")

    # The fixture was durably written, re-opened, and returned as audit-only.
    result_ref = ResearchMemoryV1.production_gemma_result_identity(receipt_ref, job.request_hash)
    with OpsRepository(database) as restarted:
        saved = restarted.get_artifact(result_ref)
        assert saved is not None
        assert saved.metadata["authority_tag"] == "ZERO_AUTHORITY_SHADOW"
        assert canonical_json(saved.metadata["decision"]) == canonical_json(output(job_id=job.job_id))
