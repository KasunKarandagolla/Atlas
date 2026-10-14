#!/usr/bin/env python3
"""Bounded Gemini metadata plus one synthetic no-tool Gemma generation probe."""

from __future__ import annotations

import hashlib
import json
import time

from atlas.v2.research.gemma_native import (
    GEMMA_MODEL_ID,
    GeminiModelMetadataPermitV1,
    GeminiNativeClient,
    GeminiProviderError,
    GeminiSmokePermitV1,
    _matches_research_schema,
)


def main() -> int:
    metadata_started_ns = time.perf_counter_ns()
    metadata_client = GeminiNativeClient.from_environment(
        permit=GeminiModelMetadataPermitV1("atlas-red-gemma-model-metadata-v1"),
    )
    observed_at_ns = time.time_ns()
    metadata = metadata_client.probe_model_metadata(deadline_ns=observed_at_ns + 15_000_000_000)
    metadata_latency_ms = round((time.perf_counter_ns() - metadata_started_ns) / 1_000_000, 3)
    result = {
        "requested_model_id": GEMMA_MODEL_ID,
        "metadata_returned_model_id": metadata.returned_model_id,
        "metadata_http_status": metadata_client.last_http_status,
        "metadata_generation_method_supported": metadata.generate_content_available,
        "metadata_failure_code": metadata.failure_code,
        "metadata_request_count": metadata.requests_made,
        "metadata_response_body_bytes": metadata.response_body_bytes,
        "metadata_latency_ms": metadata_latency_ms,
        "credential_exposed": False,
        "raw_provider_payload_saved": False,
        "generation_requests": 0,
    }
    if not metadata.generate_content_available or metadata_client.last_http_status != 200:
        print(json.dumps(result, sort_keys=True))
        return 2

    permit = GeminiSmokePermitV1(
        test_id="atlas-red-gemma-smoke-v1",
        free_tier_evidence_ref=hashlib.sha256(
            b"owner-confirmed-free-tier-no-charge-smoke-authorization-v1"
        ).hexdigest(),
        free_tier_verified=True,
        no_billing_authorized=True,
        max_requests=1,
        max_input_bytes=4096,
        max_output_tokens=256,
    )
    client = GeminiNativeClient.from_environment(permit=permit)
    prompt = (
        "Return only this synthetic response in the requested JSON schema: "
        '{"job_id":"smoke","snapshot_ref":"' + "1" * 64 + '","cutoff_ns":1,'
        '"kind":"ABSTAIN","candidate_id":null,"action_hash":null,"evidence_refs":[]}'
    )
    started_ns = time.perf_counter_ns()
    deadline_ns = time.time_ns() + 45_000_000_000
    try:
        generated = client.generate_json(prompt, deadline_ns=deadline_ns)
        decoded = json.loads(generated.text)
        valid_abstain = (
            _matches_research_schema(decoded)
            and decoded.get("kind") == "ABSTAIN"
            and decoded.get("candidate_id") is None
            and decoded.get("action_hash") is None
            and decoded.get("evidence_refs") == []
            and decoded.get("job_id") == "smoke"
            and decoded.get("snapshot_ref") == "1" * 64
            and decoded.get("cutoff_ns") == 1
        )
        result.update({
            "generation_http_status": client.last_http_status,
            "generation_returned_model_id": generated.returned_model_id,
            "generated_text_available": bool(generated.text),
            "json_parse_valid": True,
            "schema_valid": _matches_research_schema(decoded),
            "synthetic_abstain_valid": valid_abstain,
            "finish_reason": generated.finish_reason,
            "prompt_tokens": generated.prompt_tokens,
            "candidate_tokens": generated.candidate_tokens,
            "total_tokens": generated.total_tokens,
            "generation_request_body_bytes": generated.request_body_bytes,
            "generation_response_body_bytes": generated.response_bytes,
            "generation_latency_ms": round((time.perf_counter_ns() - started_ns) / 1_000_000, 3),
            "generation_requests": client.requests_made,
            "generation_failure_code": None,
            "observed_at_ns": time.time_ns(),
            "credential_exposed": False,
            "raw_provider_payload_saved": False,
            "authority": "ZERO",
        })
    except (GeminiProviderError, json.JSONDecodeError) as error:
        code = error.code if isinstance(error, GeminiProviderError) else "INVALID_PROVIDER_JSON"
        result.update({
            "generation_http_status": client.last_http_status,
            "generation_latency_ms": round((time.perf_counter_ns() - started_ns) / 1_000_000, 3),
            "generation_requests": client.requests_made,
            "generation_failure_code": code,
            "observed_at_ns": time.time_ns(),
            "credential_exposed": False,
            "raw_provider_payload_saved": False,
            "authority": "ZERO",
        })
    print(json.dumps(result, sort_keys=True))
    return 0 if (
        result.get("generation_http_status") == 200
        and result.get("generation_requests") == 1
        and result.get("generated_text_available") is True
        and result.get("schema_valid") is True
        and result.get("synthetic_abstain_valid") is True
    ) else 2


if __name__ == "__main__":
    raise SystemExit(main())
