#!/usr/bin/env python3
"""One-request Gemma model metadata probe; never calls generateContent."""

from __future__ import annotations

import dataclasses
import json
import time

from atlas.v2.research.gemma_native import GeminiModelMetadataPermitV1, GeminiNativeClient


def main() -> int:
    client = GeminiNativeClient.from_environment(
        permit=GeminiModelMetadataPermitV1("atlas-red-gemma-model-metadata-v1"),
    )
    started_ns = time.time_ns()
    probe = client.probe_model_metadata(deadline_ns=started_ns + 15_000_000_000)
    result = dataclasses.asdict(probe)
    result.update(
        observed_at_ns=time.time_ns(),
        duration_ms=round((time.time_ns() - started_ns) / 1_000_000, 3),
        credential_exposed=False,
        generation_requests=0,
        raw_provider_payload_saved=False,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if probe.generate_content_available else 2


if __name__ == "__main__":
    raise SystemExit(main())
