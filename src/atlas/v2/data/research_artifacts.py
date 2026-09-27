"""Small artifact-index bridge for immutable S4/S5/context summaries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._serialization import json_value, sha256_json, sha256_ref, timestamp
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository


def persist_research_artifact(repository: OpsRepository, artifact_type: str, artifact: Any, *,
                              available_at_ns: int, created_at_ns: int | None = None) -> ArtifactIndexEntryV2:
    """Persist one compact research summary through atlas-ops's existing sole writer."""
    timestamp(available_at_ns, field="available_at_ns")
    created = available_at_ns if created_at_ns is None else timestamp(created_at_ns, field="created_at_ns")
    if created > available_at_ns:
        raise ValueError("artifact creation cannot follow its availability")
    to_dict = getattr(artifact, "to_dict", None)
    body = to_dict() if callable(to_dict) else artifact
    if not isinstance(body, Mapping):
        raise ValueError("research artifacts must be immutable typed objects or mappings")
    body_json = json_value(body)
    supplied = getattr(artifact, "content_hash", None)
    digest = supplied if isinstance(supplied, str) else sha256_json({"artifact_type": artifact_type, "artifact": body_json})
    sha256_ref(digest, field="artifact.content_hash")
    artifact_ref = sha256_json({"artifact_type": artifact_type, "content_hash": digest})
    metadata = {"schema_version": 1, "research_artifact": body_json,
                "selector_influence": "ZERO", "capital_authority": "NONE"}
    return repository.register_artifact(ArtifactIndexEntryV2(
        artifact_ref, artifact_type, digest, created, available_at_ns, metadata,
    ))
