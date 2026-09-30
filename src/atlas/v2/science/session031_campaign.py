"""Immutable metadata identities for the bounded Session-031 shadow campaign checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.v2._serialization import sha256_json, sha256_ref

S30_START_SHA = "8ed124c4127c94c2c6e394a2bf6f941c3a5f6b57"
V1_FREEZE_SHA256 = "c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c"
V2_FREEZE_SHA256 = "e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78"
AGENT_FREEZE_SHA256 = "d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d"
S23_EXPERIMENT_ID = "SESSION023_OFFLINE_FAMILY_V1"
S23_EXPERIMENT_REF = "b59f287c7c9a7fea5aa4c76f9c940395cf7a724761bfca326cf4e0eb7aedbc42"
S23_MULTIPLICITY_FAMILY_ID = "SESSION023_PHASE3_FAMILY_V1"
S23_MULTIPLICITY_REF = "6cd00ba25d598271b46d58af50ec7a9bb52da14307bf3bf810dc915ee16ff2c9"
S23_MAXIMUM_ATTEMPTS = 17
S23_PARAMETER_SEARCH_BUDGET = 17
S23_PRIOR_ATTEMPTS = (
    ("ABLATION_CANDLES", "FAILED"),
    ("ABLATION_DERIVATIVES_CROWDING", "FAILED"),
    ("ABLATION_ELLIOTT_MEASURABLE_MORPHOLOGY", "FAILED"),
    ("ABLATION_FIBONACCI", "FAILED"),
    ("ABLATION_KILLZONE_TIME_WINDOW_CHALLENGERS", "FAILED"),
    ("ABLATION_ORDINARY_TIME_CALENDAR", "FAILED"),
    ("ABLATION_S4_FLOW_CONTEXT", "FAILED"),
    ("ABLATION_SMC_STRUCTURE", "FAILED"),
    ("ABLATION_SUPPORT_RESISTANCE", "FAILED"),
    ("ABLATION_WYCKOFF_MEASURABLE_CHALLENGERS", "FAILED"),
    ("ANALOGUE", "FAILED"),
    ("M1_CONFIG_1", "FAILED"),
    ("M1_CONFIG_2", "FAILED"),
    ("M1_CONFIG_3", "FAILED"),
    ("M1_CONFIG_4", "FAILED"),
    ("MULTI_SLEEVE", "FAILED"),
    ("S8_BASKET", "FAILED"),
)


class CampaignLaneV1(StrEnum):
    FROZEN_BASELINE = "A_FROZEN_DETERMINISTIC_BASELINE"
    ADAPTIVE_LAB = "B_ADAPTIVE_CHALLENGER_LABORATORY"
    HISTORICAL_DIAGNOSTIC = "C_HISTORICAL_DIAGNOSTIC"
    LOCKED_CHALLENGER = "D_FUTURE_LOCKED_CHALLENGER"
    FAULT_REPLICA = "E_FAULT_RECOVERY_REPLICA"


@dataclass(frozen=True)
class CampaignManifestV1:
    campaign_id: str
    schema_version: int
    exact_s30_start_sha: str
    v1_freeze_sha256: str
    v2_freeze_sha256: str
    agent_freeze_sha256: str
    dependency_lock_sha256: tuple[tuple[str, str], ...]
    baseline_identity: tuple[tuple[str, str], ...]
    discovery_experiment_id: str
    discovery_experiment_ref: str
    discovery_maximum_attempts: int
    discovery_parameter_search_budget: int
    discovery_prior_attempts: tuple[tuple[str, str], ...]
    multiplicity_family_id: str
    multiplicity_family_ref: str
    host_environment: str
    utc_clock_declaration: str
    public_source_scope: tuple[str, ...]
    instrument_scope: tuple[str, ...]
    observation_calendar: str
    evidence_requirements: tuple[str, ...]
    resource_budgets: tuple[tuple[str, str], ...]
    permitted_operations: tuple[str, ...]
    status: str
    reason_codes: tuple[str, ...]
    intervention_receipts: tuple[str, ...]
    final_holdout_prohibition: str
    capital_enabled: bool
    assisted_execution_enabled: bool
    authority: str = "METADATA_ONLY_ZERO_STRATEGY_MODEL_EXECUTION_AUTHORITY"

    VERSION = "PUBLIC_SHADOW_CAMPAIGN_MANIFEST_V1"

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not self.campaign_id.strip():
            raise ValueError("campaign manifest identity is invalid")
        for name in ("exact_s30_start_sha", "v1_freeze_sha256", "v2_freeze_sha256", "agent_freeze_sha256"):
            if name != "exact_s30_start_sha":
                sha256_ref(getattr(self, name), field=name)
        sha256_ref(self.discovery_experiment_ref, field="discovery_experiment_ref")
        sha256_ref(self.multiplicity_family_ref, field="multiplicity_family_ref")
        if (self.discovery_experiment_id != S23_EXPERIMENT_ID
                or self.discovery_experiment_ref != S23_EXPERIMENT_REF
                or self.multiplicity_family_id != S23_MULTIPLICITY_FAMILY_ID
                or self.multiplicity_family_ref != S23_MULTIPLICITY_REF
                or self.discovery_maximum_attempts != S23_MAXIMUM_ATTEMPTS
                or self.discovery_parameter_search_budget != S23_PARAMETER_SEARCH_BUDGET):
            raise ValueError("campaign must preserve the existing S23 experiment and multiplicity identity")
        prior = tuple(sorted(tuple(item) for item in self.discovery_prior_attempts))
        if (prior != S23_PRIOR_ATTEMPTS or len(prior) != self.discovery_maximum_attempts
                or any(state not in {"FAILED", "ABANDONED", "COMPLETED"} for _, state in prior)):
            raise ValueError("campaign cannot rename, reset, omit, or add to the existing S23 attempt ledger")
        object.__setattr__(self, "discovery_prior_attempts", prior)
        for name in ("dependency_lock_sha256", "baseline_identity", "resource_budgets"):
            rows = tuple(sorted(tuple(row) for row in getattr(self, name)))
            if any(len(row) != 2 or not all(isinstance(value, str) and value for value in row) for row in rows):
                raise ValueError(f"campaign {name} must contain non-empty key/value pairs")
            if len({row[0] for row in rows}) != len(rows):
                raise ValueError(f"campaign {name} keys must be unique")
            if name == "dependency_lock_sha256":
                for _, digest in rows:
                    sha256_ref(digest, field="dependency_lock_sha256")
            object.__setattr__(self, name, rows)
        for name in (
            "public_source_scope", "instrument_scope", "evidence_requirements",
            "permitted_operations", "reason_codes", "intervention_receipts",
        ):
            values = tuple(sorted(set(getattr(self, name))))
            if any(not isinstance(value, str) or not value for value in values):
                raise ValueError(f"campaign {name} must contain unique non-empty values")
            object.__setattr__(self, name, values)
        for name in (
            "host_environment", "utc_clock_declaration", "observation_calendar", "status",
            "final_holdout_prohibition", "authority",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"campaign {name} is required")
        if self.capital_enabled or self.assisted_execution_enabled:
            raise ValueError("Session-031 campaign metadata cannot enable capital or assisted execution")
        if self.exact_s30_start_sha != S30_START_SHA:
            raise ValueError("campaign must bind the accepted Session-030 checkpoint")
        if self.authority != "METADATA_ONLY_ZERO_STRATEGY_MODEL_EXECUTION_AUTHORITY":
            raise ValueError("campaign manifest cannot grant runtime authority")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            **{
                name: [list(row) for row in value]
                if name in {"dependency_lock_sha256", "baseline_identity", "resource_budgets", "discovery_prior_attempts"}
                else list(value) if isinstance(value, tuple) else value
                for name, value in self.__dict__.items()
            },
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class LaneIdentityV1:
    lane: CampaignLaneV1
    writable_store: str
    evidence_class: str
    adaptive_tuning: bool
    execution_authority: str
    promotion_eligible: bool
    final_holdout_access: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "lane", CampaignLaneV1(self.lane))
        if not all((self.writable_store, self.evidence_class, self.execution_authority)):
            raise ValueError("lane identity fields are required")
        if self.execution_authority != "ZERO" or self.final_holdout_access or self.promotion_eligible:
            raise ValueError("Session-031 lane identities cannot authorize execution or holdout access")
        expected = {
            CampaignLaneV1.FROZEN_BASELINE: ("PRIMARY_OPS_STORE", "GENUINE_PROSPECTIVE", False),
            CampaignLaneV1.ADAPTIVE_LAB: ("SEPARATE_RESEARCH_STORE", "DEVELOPMENT_ONLY", True),
            CampaignLaneV1.HISTORICAL_DIAGNOSTIC: ("RESEARCH_REPLICA", "RETROSPECTIVE_RECONSTRUCTED", False),
            CampaignLaneV1.LOCKED_CHALLENGER: ("SEPARATE_VERSIONED_SHADOW_STORE", "FUTURE_PROSPECTIVE", False),
            CampaignLaneV1.FAULT_REPLICA: ("TEMPORARY_COPIED_TEST_STORES", "SYNTHETIC_ENGINEERING_ONLY", False),
        }[self.lane]
        if (self.writable_store, self.evidence_class, self.adaptive_tuning) != expected:
            raise ValueError("campaign lane provenance/store separation differs from its frozen identity")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "CAMPAIGN_LANE_IDENTITY_V1", **{
            name: value.value if isinstance(value, StrEnum) else value
            for name, value in self.__dict__.items()
        }}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def default_lane_identities() -> tuple[LaneIdentityV1, ...]:
    return (
        LaneIdentityV1(CampaignLaneV1.FROZEN_BASELINE, "PRIMARY_OPS_STORE", "GENUINE_PROSPECTIVE", False, "ZERO", False, False),
        LaneIdentityV1(CampaignLaneV1.ADAPTIVE_LAB, "SEPARATE_RESEARCH_STORE", "DEVELOPMENT_ONLY", True, "ZERO", False, False),
        LaneIdentityV1(CampaignLaneV1.HISTORICAL_DIAGNOSTIC, "RESEARCH_REPLICA", "RETROSPECTIVE_RECONSTRUCTED", False, "ZERO", False, False),
        LaneIdentityV1(CampaignLaneV1.LOCKED_CHALLENGER, "SEPARATE_VERSIONED_SHADOW_STORE", "FUTURE_PROSPECTIVE", False, "ZERO", False, False),
        LaneIdentityV1(CampaignLaneV1.FAULT_REPLICA, "TEMPORARY_COPIED_TEST_STORES", "SYNTHETIC_ENGINEERING_ONLY", False, "ZERO", False, False),
    )
