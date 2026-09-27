"""Deterministic release classification and canonical release-manifest hashing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

RELEASE_MANIFEST_TYPE = "AtlasV2ReleaseManifest"
RELEASE_MANIFEST_SCHEMA_VERSION = 1
REQUIRED_RELEASE_FIELDS = frozenset(
    {
        "release_version",
        "git_sha",
        "v1_baseline_sha",
        "accepted_session024_sha",
        "python",
        "dependency_lock",
        "nautilus_identity",
        "v1_golden_hash",
        "v2_schema_versions",
        "live_authority",
        "frozen_policy_hashes",
        "selector_hash",
        "evidence_matrix_hash",
        "m0_identity",
        "m1_identity",
        "analogue_hash",
        "discovery_hash",
        "holdout",
        "desktop_projection_version",
        "ipc_protocol_version",
        "public_data_archive_schemas",
        "phase2_gate_ref",
        "phase3_gate_ref",
        "phase4_gate_ref",
        "risk_policy_status",
        "venue_qualification_status",
        "economic_evidence_status",
        "capital_enabled",
        "packaging_targets_tested",
        "release_classification",
    }
)


class ReleaseClassificationV2(StrEnum):
    SHADOW_RELEASED = "SHADOW_RELEASED"
    ASSISTED_CANARY_ELIGIBLE = "ASSISTED_CANARY_ELIGIBLE"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ReleaseReadinessV2:
    """Evidence gates consumed by the deterministic Phase-5 classifier."""

    deterministic_regression_passed: bool
    research_scan_desktop_usable: bool
    release_manifest_complete: bool
    critical_engineering_defect: bool
    capital_enabled: bool
    missing_evidence_surfaced: bool
    exact_venue_profile_supported: bool = False
    protection_recovery_qualified: bool = False
    external_writer_fencing_demonstrated: bool = False
    economic_gate_passed: bool = False
    exact_policy_promoted: bool = False
    prospective_evidence_satisfied: bool = False
    live_risk_policy_explicitly_approved: bool = False

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be bool")


def classify_release(readiness: ReleaseReadinessV2) -> ReleaseClassificationV2:
    """Classify a release without inferring evidence from mocks or missing data."""
    shadow_ready = (
        readiness.deterministic_regression_passed
        and readiness.research_scan_desktop_usable
        and readiness.release_manifest_complete
        and not readiness.critical_engineering_defect
        and readiness.missing_evidence_surfaced
    )
    if not shadow_ready:
        return ReleaseClassificationV2.BLOCKED

    canary_ready = all(
        (
            readiness.exact_venue_profile_supported,
            readiness.protection_recovery_qualified,
            readiness.external_writer_fencing_demonstrated,
            readiness.economic_gate_passed,
            readiness.exact_policy_promoted,
            readiness.prospective_evidence_satisfied,
            readiness.live_risk_policy_explicitly_approved,
        )
    )
    if readiness.capital_enabled and not canary_ready:
        return ReleaseClassificationV2.BLOCKED
    return (
        ReleaseClassificationV2.ASSISTED_CANARY_ELIGIBLE
        if canary_ready
        else ReleaseClassificationV2.SHADOW_RELEASED
    )


def canonical_json(value: Any) -> str:
    """Serialize JSON deterministically for content-addressed release evidence."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def release_manifest_hash(payload: Mapping[str, Any]) -> str:
    """Hash a manifest payload, excluding the outer artifact envelope."""
    return hashlib.sha256(canonical_json(dict(payload)).encode("utf-8")).hexdigest()


def seal_release_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the immutable versioned manifest envelope and its canonical hash."""
    frozen_payload = dict(payload)
    return {
        "artifact_type": RELEASE_MANIFEST_TYPE,
        "schema_version": RELEASE_MANIFEST_SCHEMA_VERSION,
        "payload": frozen_payload,
        "content_hash": release_manifest_hash(frozen_payload),
    }


def validate_release_manifest(manifest: Mapping[str, Any]) -> str:
    """Validate the complete envelope and recompute its content hash."""
    if set(manifest) != {"artifact_type", "schema_version", "payload", "content_hash"}:
        raise ValueError("release manifest fields are incomplete or unknown")
    if manifest["artifact_type"] != RELEASE_MANIFEST_TYPE:
        raise ValueError("unsupported release manifest type")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != RELEASE_MANIFEST_SCHEMA_VERSION:
        raise ValueError("unsupported release manifest schema")
    payload = manifest["payload"]
    if not isinstance(payload, Mapping):
        raise ValueError("release manifest payload must be an object")
    missing = REQUIRED_RELEASE_FIELDS - set(payload)
    if missing:
        raise ValueError("release manifest is incomplete")
    expected = release_manifest_hash(payload)
    if manifest["content_hash"] != expected:
        raise ValueError("release manifest content hash mismatch")
    return expected
