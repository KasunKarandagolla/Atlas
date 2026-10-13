#!/usr/bin/env python3
"""Two-request, synthetic, no-tool Gemma capability probe; emits sanitized metadata only."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import time

from atlas.v2.research.gemma_native import GeminiNativeClient, GeminiSmokePermitV1


def main() -> int:
    permit = GeminiSmokePermitV1(
        test_id="atlas-red-gemma-smoke-v1",
        free_tier_evidence_ref=hashlib.sha256(
            b"owner-confirmed-free-tier-no-charge-smoke-authorization-v1"
        ).hexdigest(),
        free_tier_verified=True,
        no_billing_authorized=True,
        max_requests=2,
        max_input_bytes=4096,
        max_output_tokens=256,
    )
    client = GeminiNativeClient.from_environment(permit=permit)
    prompt = (
        "Return only this synthetic response in the requested JSON schema: "
        '{"job_id":"smoke","snapshot_ref":"' + "1" * 64 + '","cutoff_ns":1,'
        '"kind":"ABSTAIN","candidate_id":null,"action_hash":null,"evidence_refs":[]}'
    )
    started_ns = time.time_ns()
    probe = client.probe_capabilities(
        deadline_ns=started_ns + 60_000_000_000,
        synthetic_prompt=prompt,
    )
    result = dataclasses.asdict(probe)
    result.update(
        observed_at_ns=time.time_ns(),
        duration_ms=round((time.time_ns() - started_ns) / 1_000_000, 3),
        credential_exposed=False,
        raw_provider_payload_saved=False,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if probe.model_available and probe.text_response else 2


if __name__ == "__main__":
    raise SystemExit(main())
