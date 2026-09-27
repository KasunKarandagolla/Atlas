"""Versioned, immutable V2 live-control authority records.

These records are written only to the credential-bearing live journal.  Ops
artifact labels are provenance, not authority; every live evidence reference is
separately stored in that journal and bound to the active writer context.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.v2._serialization import sha256_json, sha256_ref, strict_fields
from atlas.v2.instruments import EnvironmentV2, VenueV2

V2_CAPITAL_AUTHORITY_VERSION = "V2_CAPITAL_AUTHORITY_ATTESTATION_V1"
V2_LIVE_AUTHORITY_EVIDENCE_VERSION = "V2_LIVE_AUTHORITY_EVIDENCE_V1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
V2_CAPITAL_AUTHORITY_CONTRACT_HASH = sha256_json(
    {
        "version": V2_CAPITAL_AUTHORITY_VERSION,
        "evidence_version": V2_LIVE_AUTHORITY_EVIDENCE_VERSION,
        "extension_schema_version": 2,
        "source_evidence_lookup": {
            "source_identity_field": "evidence_ref",
            "receipt_identity_field": "content_hash",
            "context_fields": [
                "writer_id",
                "writer_epoch",
                "runtime_instance_id",
                "recovery_run_id",
                "account_identity_hash",
                "venue",
                "environment",
                "capability_profile_hash",
                "kind",
                "capability",
                "account_risk_snapshot_hash",
                "evidence_cutoff_ns",
                "max_age_ns",
                "require_live",
            ],
        },
        "evidence_kinds": ["CAPABILITY", "ACCOUNT_RISK", "EXPOSURE_RECONCILIATION", "PROTECTION"],
        "statuses": ["LIVE_ATTESTED", "SYNTHETIC_TEST_ONLY"],
        "tables": ["v2_live_authority_evidence", "v2_capital_authority_attestations"],
        "attestation_fields": [
            "bridge_hash",
            "trade_plan_hash",
            "capability_profile_hash",
            "capability_snapshot_hash",
            "capability_evidence_refs",
            "account_identity_hash",
            "venue",
            "environment",
            "instrument_key_hash",
            "product_hash",
            "product_revision",
            "v1_risk_policy_hash",
            "risk_policy_v2_hash",
            "account_risk_snapshot_hash",
            "account_risk_observation_hash",
            "risk_evidence_hash",
            "exposure_reconciliation_hash",
            "protection_evidence_hash",
            "ops_artifact_refs",
            "ops_artifact_fingerprints",
            "live_evidence_refs",
            "writer_id",
            "writer_epoch",
            "runtime_instance_id",
            "recovery_run_id",
            "evidence_cutoff_ns",
            "attested_at_ns",
            "expires_at_ns",
            "status",
            "source_class",
        ],
    }
)


class V2LiveAuthorityEvidenceKind(StrEnum):
    CAPABILITY = "CAPABILITY"
    ACCOUNT_RISK = "ACCOUNT_RISK"
    EXPOSURE_RECONCILIATION = "EXPOSURE_RECONCILIATION"
    PROTECTION = "PROTECTION"


class V2CapitalAuthorityStatus(StrEnum):
    LIVE_ATTESTED = "LIVE_ATTESTED"
    SYNTHETIC_TEST_ONLY = "SYNTHETIC_TEST_ONLY"


@dataclass(frozen=True)
class V2LiveAuthorityEvidence:
    """Hash-only evidence receipt captured by the active live writer.

    ``source_class`` is deliberately constrained. Synthetic receipts persist
    for offline journal tests, but can never be represented as live evidence.
    """

    evidence_ref: str
    kind: V2LiveAuthorityEvidenceKind
    account_identity_hash: str
    venue: VenueV2
    environment: EnvironmentV2
    product_revision: str
    capability_profile_hash: str
    capability: str | None
    account_risk_snapshot_hash: str | None
    observed_at_ns: int
    writer_id: str
    writer_epoch: int
    runtime_instance_id: str
    recovery_run_id: str
    source_class: str
    synthetic_fixture: bool = False
    version: str = V2_LIVE_AUTHORITY_EVIDENCE_VERSION

    def __post_init__(self) -> None:
        if self.version != V2_LIVE_AUTHORITY_EVIDENCE_VERSION:
            raise ValueError("unsupported V2 live authority evidence version")
        sha256_ref(self.evidence_ref, field="evidence_ref")
        sha256_ref(self.account_identity_hash, field="account_identity_hash")
        sha256_ref(self.capability_profile_hash, field="capability_profile_hash")
        object.__setattr__(self, "kind", V2LiveAuthorityEvidenceKind(self.kind))
        object.__setattr__(self, "venue", VenueV2(self.venue))
        object.__setattr__(self, "environment", EnvironmentV2(self.environment))
        if not all(
            value.strip()
            for value in (self.product_revision, self.writer_id, self.runtime_instance_id, self.recovery_run_id)
        ):
            raise ValueError("live authority evidence identity fields are required")
        if self.kind is V2LiveAuthorityEvidenceKind.CAPABILITY:
            if not isinstance(self.capability, str) or not self.capability.strip():
                raise ValueError("capability evidence must name its exact capability row")
        elif self.capability is not None:
            raise ValueError("non-capability evidence cannot name a capability row")
        if self.kind is V2LiveAuthorityEvidenceKind.ACCOUNT_RISK:
            if self.account_risk_snapshot_hash is None:
                raise ValueError("account-risk evidence must bind its exact snapshot hash")
            sha256_ref(self.account_risk_snapshot_hash, field="account_risk_snapshot_hash")
        elif self.account_risk_snapshot_hash is not None:
            raise ValueError("only account-risk evidence may bind an account snapshot")
        if type(self.observed_at_ns) is not int or self.observed_at_ns < 0:
            raise ValueError("live authority evidence timestamp must be nonnegative")
        if type(self.writer_epoch) is not int or self.writer_epoch < 1:
            raise ValueError("live authority evidence writer epoch must be positive")
        if type(self.synthetic_fixture) is not bool:
            raise ValueError("synthetic_fixture must be boolean")
        expected_source = "SYNTHETIC_TEST_FIXTURE" if self.synthetic_fixture else "AUTHENTICATED_LIVE_CONTROL"
        if self.source_class != expected_source:
            raise ValueError("live authority evidence source class does not match fixture class")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "evidence_ref": self.evidence_ref,
            "kind": self.kind.value,
            "account_identity_hash": self.account_identity_hash,
            "venue": self.venue.value,
            "environment": self.environment.value,
            "product_revision": self.product_revision,
            "capability_profile_hash": self.capability_profile_hash,
            "capability": self.capability,
            "account_risk_snapshot_hash": self.account_risk_snapshot_hash,
            "observed_at_ns": self.observed_at_ns,
            "writer_id": self.writer_id,
            "writer_epoch": self.writer_epoch,
            "runtime_instance_id": self.runtime_instance_id,
            "recovery_run_id": self.recovery_run_id,
            "source_class": self.source_class,
            "synthetic_fixture": self.synthetic_fixture,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> V2LiveAuthorityEvidence:
        fields = set(cls.__dataclass_fields__)
        d = strict_fields(data, expected=fields, required=fields, name="V2LiveAuthorityEvidence")
        return cls(**d)


@dataclass(frozen=True)
class V2CapitalAuthorityAttestation:
    bridge_hash: str
    trade_plan_hash: str
    capability_profile_hash: str
    capability_snapshot_hash: str
    capability_evidence_refs: tuple[str, ...]
    account_identity_hash: str
    venue: VenueV2
    environment: EnvironmentV2
    instrument_key_hash: str
    product_hash: str
    product_revision: str
    v1_risk_policy_hash: str
    risk_policy_v2_hash: str
    account_risk_snapshot_hash: str
    account_risk_observation_hash: str
    risk_evidence_hash: str
    exposure_reconciliation_hash: str
    protection_evidence_hash: str
    ops_artifact_refs: tuple[str, ...]
    ops_artifact_fingerprints: tuple[tuple[str, str], ...]
    live_evidence_refs: tuple[str, ...]
    writer_id: str
    writer_epoch: int
    runtime_instance_id: str
    recovery_run_id: str
    evidence_cutoff_ns: int
    attested_at_ns: int
    expires_at_ns: int
    status: V2CapitalAuthorityStatus
    source_class: str
    version: str = V2_CAPITAL_AUTHORITY_VERSION

    def __post_init__(self) -> None:
        if self.version != V2_CAPITAL_AUTHORITY_VERSION:
            raise ValueError("unsupported V2 capital authority attestation version")
        for name in (
            "bridge_hash",
            "trade_plan_hash",
            "capability_profile_hash",
            "capability_snapshot_hash",
            "account_identity_hash",
            "instrument_key_hash",
            "product_hash",
            "v1_risk_policy_hash",
            "risk_policy_v2_hash",
            "account_risk_snapshot_hash",
            "account_risk_observation_hash",
            "risk_evidence_hash",
            "exposure_reconciliation_hash",
            "protection_evidence_hash",
        ):
            sha256_ref(getattr(self, name), field=name)
        for name in ("capability_evidence_refs", "ops_artifact_refs", "live_evidence_refs"):
            refs = getattr(self, name)
            if refs != tuple(sorted(set(refs))):
                raise ValueError(f"{name} must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        if self.ops_artifact_fingerprints != tuple(sorted(self.ops_artifact_fingerprints)):
            raise ValueError("ops_artifact_fingerprints must be sorted")
        if tuple(ref for ref, _ in self.ops_artifact_fingerprints) != self.ops_artifact_refs:
            raise ValueError("ops artifact fingerprints must cover the exact attested ref set")
        for ref, fingerprint in self.ops_artifact_fingerprints:
            sha256_ref(ref, field="ops artifact fingerprint ref")
            sha256_ref(fingerprint, field="ops artifact fingerprint")
        if not self.capability_evidence_refs or not self.ops_artifact_refs or not self.live_evidence_refs:
            raise ValueError("attestation must bind ops artifacts and live evidence")
        object.__setattr__(self, "venue", VenueV2(self.venue))
        object.__setattr__(self, "environment", EnvironmentV2(self.environment))
        object.__setattr__(self, "status", V2CapitalAuthorityStatus(self.status))
        for name in ("writer_id", "runtime_instance_id", "recovery_run_id", "product_revision"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        if type(self.writer_epoch) is not int or self.writer_epoch < 1:
            raise ValueError("writer_epoch must be positive")
        for name in ("evidence_cutoff_ns", "attested_at_ns", "expires_at_ns"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if not self.evidence_cutoff_ns <= self.attested_at_ns < self.expires_at_ns:
            raise ValueError("attestation cutoff/expiry chronology invalid")
        if self.status is V2CapitalAuthorityStatus.SYNTHETIC_TEST_ONLY:
            if self.source_class != "SYNTHETIC_TEST_FIXTURE":
                raise ValueError("synthetic attestation source class mismatch")
        elif self.source_class != "AUTHENTICATED_LIVE_CONTROL":
            raise ValueError("live attestation source class mismatch")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "bridge_hash": self.bridge_hash,
            "trade_plan_hash": self.trade_plan_hash,
            "capability_profile_hash": self.capability_profile_hash,
            "capability_snapshot_hash": self.capability_snapshot_hash,
            "capability_evidence_refs": list(self.capability_evidence_refs),
            "account_identity_hash": self.account_identity_hash,
            "venue": self.venue.value,
            "environment": self.environment.value,
            "instrument_key_hash": self.instrument_key_hash,
            "product_hash": self.product_hash,
            "product_revision": self.product_revision,
            "v1_risk_policy_hash": self.v1_risk_policy_hash,
            "risk_policy_v2_hash": self.risk_policy_v2_hash,
            "account_risk_snapshot_hash": self.account_risk_snapshot_hash,
            "account_risk_observation_hash": self.account_risk_observation_hash,
            "risk_evidence_hash": self.risk_evidence_hash,
            "exposure_reconciliation_hash": self.exposure_reconciliation_hash,
            "protection_evidence_hash": self.protection_evidence_hash,
            "ops_artifact_refs": list(self.ops_artifact_refs),
            "ops_artifact_fingerprints": [list(item) for item in self.ops_artifact_fingerprints],
            "live_evidence_refs": list(self.live_evidence_refs),
            "writer_id": self.writer_id,
            "writer_epoch": self.writer_epoch,
            "runtime_instance_id": self.runtime_instance_id,
            "recovery_run_id": self.recovery_run_id,
            "evidence_cutoff_ns": self.evidence_cutoff_ns,
            "attested_at_ns": self.attested_at_ns,
            "expires_at_ns": self.expires_at_ns,
            "status": self.status.value,
            "source_class": self.source_class,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def binding_reasons(
        self,
        *,
        bridge: Any,
        plan: Any,
        capability_profile: Any,
        account_identity_hash: str,
        v1_risk_policy_hash: str,
        risk_policy_v2_hash: str,
        account_risk_snapshot_hash: str,
        account_risk_observation_hash: str,
        risk_evidence_hash: str,
        exposure_reconciliation_hash: str,
        protection_evidence_hash: str,
        writer_id: str,
        writer_epoch: int,
        runtime_instance_id: str,
        recovery_run_id: str,
        cutoff_ns: int,
        now_ns: int,
    ) -> tuple[str, ...]:
        expected = {
            "bridge_hash": bridge.content_hash,
            "trade_plan_hash": plan.content_hash,
            "capability_profile_hash": capability_profile.content_hash,
            "capability_snapshot_hash": bridge.venue_capability_snapshot_ref,
            "account_identity_hash": account_identity_hash,
            "venue": VenueV2(bridge.venue),
            "environment": EnvironmentV2(bridge.environment),
            "instrument_key_hash": bridge.instrument_key_ref,
            "product_hash": bridge.product_ref,
            "product_revision": bridge.product_revision,
            "v1_risk_policy_hash": v1_risk_policy_hash,
            "risk_policy_v2_hash": risk_policy_v2_hash,
            "account_risk_snapshot_hash": account_risk_snapshot_hash,
            "account_risk_observation_hash": account_risk_observation_hash,
            "risk_evidence_hash": risk_evidence_hash,
            "exposure_reconciliation_hash": exposure_reconciliation_hash,
            "protection_evidence_hash": protection_evidence_hash,
            "writer_id": writer_id,
            "writer_epoch": writer_epoch,
            "runtime_instance_id": runtime_instance_id,
            "recovery_run_id": recovery_run_id,
            "evidence_cutoff_ns": cutoff_ns,
        }
        reasons = [f"attestation {name} mismatch" for name, value in expected.items() if getattr(self, name) != value]
        if not self.evidence_cutoff_ns <= self.attested_at_ns < self.expires_at_ns:
            reasons.append("attestation chronology invalid")
        if now_ns < self.attested_at_ns or now_ns >= self.expires_at_ns:
            reasons.append("attestation is stale or expired")
        if self.status is not V2CapitalAuthorityStatus.LIVE_ATTESTED:
            reasons.append("synthetic or non-live attestation cannot authorize opening risk")
        return tuple(reasons)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> V2CapitalAuthorityAttestation:
        fields = set(cls.__dataclass_fields__)
        d = dict(strict_fields(data, expected=fields, required=fields, name="V2CapitalAuthorityAttestation"))
        for name in ("capability_evidence_refs", "ops_artifact_refs", "live_evidence_refs"):
            if not isinstance(d[name], list):
                raise ValueError(f"{name} must be an array")
            d[name] = tuple(d[name])
        if not isinstance(d["ops_artifact_fingerprints"], list):
            raise ValueError("ops_artifact_fingerprints must be an array")
        d["ops_artifact_fingerprints"] = tuple(tuple(item) for item in d["ops_artifact_fingerprints"])
        d["venue"] = VenueV2(d["venue"])
        d["environment"] = EnvironmentV2(d["environment"])
        d["status"] = V2CapitalAuthorityStatus(d["status"])
        return cls(**d)
