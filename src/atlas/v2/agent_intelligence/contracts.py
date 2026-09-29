"""ATLAS-owned, immutable contracts for offline zero-authority research jobs.

These records deliberately do not import the optional model framework.  The
framework is contained behind ``ResearchProposalProvider`` in ``provider.py``.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json, sha256_ref, strict_fields

CONTRACT_NAMESPACE = "ATLAS_AGENT_INTELLIGENCE_V1"
PROMPT_CONTRACT_VERSION = "DISCOVERY_PROPOSER_PROMPT_V1"
TOOL_CONTRACT_VERSION = "DISCOVERY_READ_TOOLS_V1"
PROPOSAL_SCHEMA_VERSION = "RESEARCH_PROPOSAL_V1"
RULE_GRAMMAR_VERSION = "RESEARCH_RULE_DSL_V1"
MAX_PROPOSAL_BYTES = 48_000
MAX_TEXT_CHARS = 4_000
MAX_EVIDENCE_REFS = 32
MAX_FEATURE_FAMILIES = 24
MAX_RULE_NODES = 96
MAX_DEPTH = 8
SUPPORTED_RULE_OPERATIONS_V1 = frozenset({"AND", "OR", "GT", "GTE", "LT", "LTE", "EQ", "RISING", "FALLING",
                                          "CROSS_ABOVE", "CROSS_BELOW"})


class AgentJobStateV1(StrEnum):
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    RESULT_RECEIVED = "RESULT_RECEIVED"
    VALIDATED = "VALIDATED"
    UNAVAILABLE = "UNAVAILABLE"
    INVALID = "INVALID"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class ReadStatusV1(StrEnum):
    PRESENT = "PRESENT"
    MISSING = "MISSING"
    UNAVAILABLE = "UNAVAILABLE"
    FORBIDDEN = "FORBIDDEN"
    EXPIRED = "EXPIRED"


class RevisionStatusV1(StrEnum):
    FIXED_REVISION = "FIXED_REVISION"
    ALIAS_ONLY = "ALIAS_ONLY"
    UNKNOWN = "UNKNOWN"


def _identifier(value: str, name: str, limit: int = 160) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"{name} must be a non-empty string no longer than {limit} characters")
    if any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} contains control characters")
    return value


def _uuid(value: str, name: str) -> str:
    if not isinstance(value, str) or len(value) != 36:
        raise ValueError(f"{name} must be a canonical UUID")
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical UUID") from exc
    return value


def _refs(values: Sequence[str], name: str, *, maximum: int = MAX_EVIDENCE_REFS) -> tuple[str, ...]:
    items = tuple(values)
    if len(items) > maximum or len(items) != len(set(items)):
        raise ValueError(f"{name} exceeds its bound or contains duplicate refs")
    for index, value in enumerate(items):
        sha256_ref(value, field=f"{name}[{index}]")
    return items


def _frozen_map(value: Mapping[str, Any], name: str, max_bytes: int = 24_000) -> FrozenMap:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    frozen = FrozenMap(value)
    if len(canonical_json(frozen).encode("utf-8")) > max_bytes:
        raise ValueError(f"{name} exceeds its size limit")
    return frozen


@dataclass(frozen=True)
class AgentModelProfileV1:
    provider: str
    requested_model_id: str
    returned_model_id: str | None
    model_revision: str | None
    revision_status: RevisionStatusV1
    reasoning_settings: FrozenMap
    max_input_tokens: int
    max_output_tokens: int
    prompt_contract_hash: str
    schema_hash: str
    tool_contract_hash: str
    runtime_dependency_hash: str
    pricing_schedule_id: str
    profile_version: str = "AgentModelProfileV1"

    def __post_init__(self) -> None:
        for field_name in ("provider", "requested_model_id", "pricing_schedule_id"):
            _identifier(getattr(self, field_name), field_name)
        if self.returned_model_id is not None:
            _identifier(self.returned_model_id, "returned_model_id")
        if self.model_revision is not None:
            _identifier(self.model_revision, "model_revision")
        if type(self.max_input_tokens) is not int or not 1 <= self.max_input_tokens <= 32_000:
            raise ValueError("max_input_tokens is outside the offline profile bound")
        if type(self.max_output_tokens) is not int or not 1 <= self.max_output_tokens <= 8_000:
            raise ValueError("max_output_tokens is outside the offline profile bound")
        for name in ("prompt_contract_hash", "schema_hash", "tool_contract_hash", "runtime_dependency_hash"):
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "reasoning_settings", _frozen_map(self.reasoning_settings, "reasoning_settings", 2_000))
        if self.provider != "openai" or self.requested_model_id != "gpt-6-astra":
            raise ValueError("initial offline profile is pinned to OpenAI GPT-6 Astra")
        if self.reasoning_settings.to_dict() != {"effort": "medium"}:
            raise ValueError("initial offline profile reasoning settings must be medium")
        if self.revision_status == RevisionStatusV1.ALIAS_ONLY and self.model_revision is not None:
            raise ValueError("alias-only profile must not invent a model revision")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.profile_version, "provider": self.provider,
                "requested_model_id": self.requested_model_id, "returned_model_id": self.returned_model_id,
                "model_revision": self.model_revision, "revision_status": self.revision_status.value,
                "reasoning_settings": self.reasoning_settings.to_dict(), "max_input_tokens": self.max_input_tokens,
                "max_output_tokens": self.max_output_tokens, "prompt_contract_hash": self.prompt_contract_hash,
                "schema_hash": self.schema_hash, "tool_contract_hash": self.tool_contract_hash,
                "runtime_dependency_hash": self.runtime_dependency_hash,
                "pricing_schedule_id": self.pricing_schedule_id}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AgentModelProfileV1:
        fields = {"version", "provider", "requested_model_id", "returned_model_id", "model_revision",
                  "revision_status", "reasoning_settings", "max_input_tokens", "max_output_tokens",
                  "prompt_contract_hash", "schema_hash", "tool_contract_hash", "runtime_dependency_hash",
                  "pricing_schedule_id"}
        row = strict_fields(data, expected=fields, required=fields, name="AgentModelProfileV1")
        if row["version"] != "AgentModelProfileV1" or not isinstance(row["reasoning_settings"], Mapping):
            raise ValueError("AgentModelProfileV1 wire record is invalid")
        profile = cls(row["provider"], row["requested_model_id"], row["returned_model_id"], row["model_revision"],
            RevisionStatusV1(row["revision_status"]), FrozenMap(row["reasoning_settings"]),
            row["max_input_tokens"], row["max_output_tokens"], row["prompt_contract_hash"], row["schema_hash"],
            row["tool_contract_hash"], row["runtime_dependency_hash"], row["pricing_schedule_id"])
        if canonical_json(profile.to_dict()) != canonical_json(data):
            raise ValueError("AgentModelProfileV1 wire record is not canonical")
        return profile


@dataclass(frozen=True)
class AgentModelProfileV2:
    """Additive DeepSeek binding; V1 remains pinned to OpenAI/Astra."""

    provider: str
    requested_model_id: str
    returned_model_id: str | None
    model_revision: str | None
    revision_status: RevisionStatusV1
    base_url: str
    endpoint_path: str
    model_family: str
    reasoning_setting_id: str
    reasoning_settings: FrozenMap
    structured_output_format: str
    max_input_tokens: int
    max_output_tokens: int
    max_model_calls_per_job: int
    max_read_tool_calls_per_job: int
    max_concurrent_jobs: int
    prompt_contract_hash: str
    schema_hash: str
    tool_contract_hash: str
    runtime_dependency_hash: str
    pricing_schedule_id: str
    revision_evidence_hash: str | None = None
    profile_version: str = "AgentModelProfileV2"

    def __post_init__(self) -> None:
        for field_name in ("provider", "requested_model_id", "base_url", "endpoint_path", "model_family",
                           "reasoning_setting_id", "structured_output_format", "pricing_schedule_id"):
            _identifier(getattr(self, field_name), field_name)
        if self.returned_model_id is not None:
            _identifier(self.returned_model_id, "returned_model_id")
        if self.model_revision is not None:
            _identifier(self.model_revision, "model_revision")
        if self.profile_version != "AgentModelProfileV2":
            raise ValueError("DeepSeek provider profile version is fixed")
        if (self.provider != "deepseek" or self.requested_model_id != "deepseek-flash"
                or self.base_url != "https://api.deepseek.com" or self.endpoint_path != "/responses"
                or self.model_family != "DeepSeek-V4.1-Flash"):
            raise ValueError("DeepSeek provider/model/endpoint binding is outside this amendment")
        if self.reasoning_setting_id != "DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1":
            raise ValueError("DeepSeek reasoning-setting identity is outside this amendment")
        object.__setattr__(self, "reasoning_settings", _frozen_map(self.reasoning_settings, "reasoning_settings", 2_000))
        if self.reasoning_settings.to_dict() != {"effort": "high"}:
            raise ValueError("DeepSeek reasoning settings must use the separately versioned high setting")
        if self.structured_output_format != "json_schema":
            raise ValueError("DeepSeek structured output must use JSON Schema")
        if (type(self.max_input_tokens) is not int or self.max_input_tokens != 12_000
                or type(self.max_output_tokens) is not int or self.max_output_tokens != 4_000
                or type(self.max_model_calls_per_job) is not int or self.max_model_calls_per_job != 3
                or type(self.max_read_tool_calls_per_job) is not int or self.max_read_tool_calls_per_job != 8
                or type(self.max_concurrent_jobs) is not int or self.max_concurrent_jobs != 1):
            raise ValueError("DeepSeek offline research limits differ from the approved amendment")
        for name in ("prompt_contract_hash", "schema_hash", "tool_contract_hash", "runtime_dependency_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.revision_status == RevisionStatusV1.ALIAS_ONLY:
            if self.model_revision is not None or self.revision_evidence_hash is not None:
                raise ValueError("alias-only DeepSeek profile cannot invent revision evidence")
            if self.returned_model_id not in {None, self.requested_model_id}:
                raise ValueError("alias-only DeepSeek profile must return the requested alias")
        elif self.revision_status == RevisionStatusV1.UNKNOWN:
            if self.model_revision is not None or self.revision_evidence_hash is not None:
                raise ValueError("unknown DeepSeek model identity cannot claim revision evidence")
        elif self.revision_status == RevisionStatusV1.FIXED_REVISION:
            if (self.model_revision is None or self.model_revision != self.returned_model_id
                    or self.revision_evidence_hash is None):
                raise ValueError("fixed DeepSeek revision requires matching identity and evidence hash")
            sha256_ref(self.revision_evidence_hash, field="revision_evidence_hash")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.profile_version, "provider": self.provider,
                "requested_model_id": self.requested_model_id, "returned_model_id": self.returned_model_id,
                "model_revision": self.model_revision, "revision_status": self.revision_status.value,
                "base_url": self.base_url, "endpoint_path": self.endpoint_path, "model_family": self.model_family,
                "reasoning_setting_id": self.reasoning_setting_id,
                "reasoning_settings": self.reasoning_settings.to_dict(),
                "structured_output_format": self.structured_output_format,
                "max_input_tokens": self.max_input_tokens, "max_output_tokens": self.max_output_tokens,
                "max_model_calls_per_job": self.max_model_calls_per_job,
                "max_read_tool_calls_per_job": self.max_read_tool_calls_per_job,
                "max_concurrent_jobs": self.max_concurrent_jobs,
                "prompt_contract_hash": self.prompt_contract_hash, "schema_hash": self.schema_hash,
                "tool_contract_hash": self.tool_contract_hash,
                "runtime_dependency_hash": self.runtime_dependency_hash,
                "pricing_schedule_id": self.pricing_schedule_id,
                "revision_evidence_hash": self.revision_evidence_hash}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @property
    def provider_binding_hash(self) -> str:
        return sha256_json({"version": "AgentProviderBindingV2", "provider": self.provider,
            "requested_model_id": self.requested_model_id, "base_url": self.base_url,
            "endpoint_path": self.endpoint_path, "model_family": self.model_family,
            "reasoning_setting_id": self.reasoning_setting_id,
            "reasoning_settings": self.reasoning_settings.to_dict(),
            "structured_output_format": self.structured_output_format,
            "max_input_tokens": self.max_input_tokens, "max_output_tokens": self.max_output_tokens,
            "prompt_contract_hash": self.prompt_contract_hash, "schema_hash": self.schema_hash,
            "tool_contract_hash": self.tool_contract_hash,
            "runtime_dependency_hash": self.runtime_dependency_hash,
            "pricing_schedule_id": self.pricing_schedule_id})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AgentModelProfileV2:
        fields = {"version", "provider", "requested_model_id", "returned_model_id", "model_revision",
            "revision_status", "base_url", "endpoint_path", "model_family", "reasoning_setting_id",
            "reasoning_settings", "structured_output_format", "max_input_tokens", "max_output_tokens",
            "max_model_calls_per_job", "max_read_tool_calls_per_job", "max_concurrent_jobs",
            "prompt_contract_hash", "schema_hash", "tool_contract_hash", "runtime_dependency_hash",
            "pricing_schedule_id", "revision_evidence_hash"}
        row = strict_fields(data, expected=fields, required=fields, name="AgentModelProfileV2")
        if row["version"] != "AgentModelProfileV2" or not isinstance(row["reasoning_settings"], Mapping):
            raise ValueError("AgentModelProfileV2 wire record is invalid")
        profile = cls(row["provider"], row["requested_model_id"], row["returned_model_id"], row["model_revision"],
            RevisionStatusV1(row["revision_status"]), row["base_url"], row["endpoint_path"], row["model_family"],
            row["reasoning_setting_id"], FrozenMap(row["reasoning_settings"]), row["structured_output_format"],
            row["max_input_tokens"], row["max_output_tokens"], row["max_model_calls_per_job"],
            row["max_read_tool_calls_per_job"], row["max_concurrent_jobs"], row["prompt_contract_hash"],
            row["schema_hash"], row["tool_contract_hash"], row["runtime_dependency_hash"],
            row["pricing_schedule_id"], row["revision_evidence_hash"])
        if canonical_json(profile.to_dict()) != canonical_json(data):
            raise ValueError("AgentModelProfileV2 wire record is not canonical")
        return profile


AgentModelProfile = AgentModelProfileV1 | AgentModelProfileV2


def model_profile_from_dict(data: Mapping[str, Any]) -> AgentModelProfile:
    version = data.get("version")
    if version == "AgentModelProfileV1":
        return AgentModelProfileV1.from_dict(data)
    if version == "AgentModelProfileV2":
        return AgentModelProfileV2.from_dict(data)
    raise ValueError("unsupported agent model profile version")


@dataclass(frozen=True)
class AgentEvidenceRefV1:
    tool_name: str
    artifact_ref: str
    available_through_ns: int
    cursor: str | None = None

    def __post_init__(self) -> None:
        if self.tool_name not in {"get_registered_artifact", "inspect_failed_discovery_variants",
                                  "inspect_authorized_ablation_results", "get_registered_policy_or_model_manifest"}:
            raise ValueError("evidence tool is not in the fixed read-only tool set")
        sha256_ref(self.artifact_ref, field="artifact_ref")
        if type(self.available_through_ns) is not int or self.available_through_ns < 0:
            raise ValueError("available_through_ns must be a non-negative integer")
        if self.cursor is not None:
            sha256_ref(self.cursor, field="cursor")

    def to_dict(self) -> dict[str, Any]:
        return {"tool_name": self.tool_name, "artifact_ref": self.artifact_ref,
                "available_through_ns": self.available_through_ns, "cursor": self.cursor}


@dataclass(frozen=True)
class ResearchProposalRequestV1:
    request_id: str
    research_family_id: str
    experiment_ref: str
    preregistration_ref: str
    development_cutoff_ns: int
    outcome_maturity_cutoff_ns: int
    allowed_feature_families: tuple[str, ...]
    allowed_operation_grammar: tuple[str, ...]
    attempt_history_refs: tuple[str, ...]
    remaining_attempt_budget: int
    remaining_parameter_search_budget: int
    multiplicity_family: str
    baseline_policy_ref: str
    cost_evaluation_target: FrozenMap
    inaccessible_holdout_identities: tuple[str, ...]
    evidence_manifest: tuple[AgentEvidenceRefV1, ...]
    prompt_contract_version: str
    prompt_contract_hash: str
    model_profile_hash: str
    tool_contract_version: str
    tool_contract_hash: str
    proposal_schema_version: str
    schema_hash: str
    absolute_deadline_ns: int
    max_model_calls: int
    max_read_tool_calls: int
    max_input_tokens: int
    max_output_tokens: int
    max_job_cost_usd: str
    daily_cost_budget_usd: str

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        for name in ("research_family_id", "multiplicity_family", "prompt_contract_version",
                     "tool_contract_version", "proposal_schema_version"):
            _identifier(getattr(self, name), name)
        for name in ("experiment_ref", "preregistration_ref", "baseline_policy_ref", "prompt_contract_hash",
                     "model_profile_hash", "tool_contract_hash", "schema_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("development_cutoff_ns", "outcome_maturity_cutoff_ns", "absolute_deadline_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.outcome_maturity_cutoff_ns > self.development_cutoff_ns:
            raise ValueError("outcome maturity cutoff cannot exceed development cutoff")
        if type(self.max_model_calls) is not int or not 1 <= self.max_model_calls <= 3:
            raise ValueError("maximum model calls must be between one and three")
        if type(self.max_read_tool_calls) is not int or not 0 <= self.max_read_tool_calls <= 8:
            raise ValueError("maximum read-tool calls must be between zero and eight")
        if type(self.max_input_tokens) is not int or not 1 <= self.max_input_tokens <= 32_000:
            raise ValueError("input token cap is invalid")
        if type(self.max_output_tokens) is not int or not 1 <= self.max_output_tokens <= 8_000:
            raise ValueError("output token cap is invalid")
        if type(self.remaining_attempt_budget) is not int or self.remaining_attempt_budget < 0:
            raise ValueError("remaining_attempt_budget must be non-negative")
        if type(self.remaining_parameter_search_budget) is not int or self.remaining_parameter_search_budget < 0:
            raise ValueError("remaining_parameter_search_budget must be non-negative")
        features = tuple(_identifier(item, "allowed feature family") for item in self.allowed_feature_families)
        if not features or len(features) > MAX_FEATURE_FAMILIES or len(set(features)) != len(features):
            raise ValueError("allowed feature families must be non-empty, bounded and unique")
        object.__setattr__(self, "allowed_feature_families", features)
        grammar = tuple(_identifier(item, "operation grammar token") for item in self.allowed_operation_grammar)
        if (not grammar or len(grammar) > len(SUPPORTED_RULE_OPERATIONS_V1) or len(set(grammar)) != len(grammar)
                or not set(grammar).issubset(SUPPORTED_RULE_OPERATIONS_V1)):
            raise ValueError("allowed operation grammar must be non-empty, bounded and unique")
        object.__setattr__(self, "allowed_operation_grammar", grammar)
        object.__setattr__(self, "attempt_history_refs", _refs(self.attempt_history_refs, "attempt_history_refs"))
        holdouts = tuple(_identifier(item, "inaccessible holdout identity") for item in self.inaccessible_holdout_identities)
        if len(set(holdouts)) != len(holdouts) or len(holdouts) > 32:
            raise ValueError("holdout identities must be unique and bounded")
        object.__setattr__(self, "inaccessible_holdout_identities", holdouts)
        evidence = tuple(self.evidence_manifest)
        if len(evidence) > MAX_EVIDENCE_REFS or any(not isinstance(item, AgentEvidenceRefV1) for item in evidence):
            raise ValueError("evidence manifest is invalid or oversized")
        if len({(item.tool_name, item.artifact_ref) for item in evidence}) != len(evidence):
            raise ValueError("evidence manifest contains duplicate authorization entries")
        object.__setattr__(self, "evidence_manifest", evidence)
        object.__setattr__(self, "cost_evaluation_target", _frozen_map(self.cost_evaluation_target, "cost_evaluation_target"))
        for name in ("max_job_cost_usd", "daily_cost_budget_usd"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d{1,6})?", value):
                raise ValueError(f"{name} must be a bounded canonical decimal string")
            if float(value) <= 0 or float(value) > 1_000:
                raise ValueError(f"{name} is outside the configured research cost range")

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"version": "ResearchProposalRequestV1", "schema_version": self.SCHEMA_VERSION}
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if name == "cost_evaluation_target":
                value = value.to_dict()
            elif name == "evidence_manifest":
                value = [item.to_dict() for item in value]
            elif isinstance(value, tuple):
                value = list(value)
            body[name] = value
        return body

    @property
    def request_key(self) -> str:
        return sha256_json({"type": "ResearchProposalRequestV1", "request": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ResearchProposalRequestV1:
        fields = {"version", "schema_version", *cls.__dataclass_fields__}
        row = strict_fields(data, expected=fields, required=fields, name="ResearchProposalRequestV1")
        if row["version"] != "ResearchProposalRequestV1" or row["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported ResearchProposalRequestV1 version")
        values = {name: row[name] for name in cls.__dataclass_fields__}
        if not isinstance(values["cost_evaluation_target"], Mapping):
            raise ValueError("cost_evaluation_target must be an object")
        values["cost_evaluation_target"] = FrozenMap(values["cost_evaluation_target"])
        values["allowed_feature_families"] = tuple(values["allowed_feature_families"])
        values["allowed_operation_grammar"] = tuple(values["allowed_operation_grammar"])
        values["attempt_history_refs"] = tuple(values["attempt_history_refs"])
        values["inaccessible_holdout_identities"] = tuple(values["inaccessible_holdout_identities"])
        values["evidence_manifest"] = tuple(AgentEvidenceRefV1(**item) for item in values["evidence_manifest"])
        request = cls(**values)
        if canonical_json(request.to_dict()) != canonical_json(data):
            raise ValueError("ResearchProposalRequestV1 is not canonical")
        return request


@dataclass(frozen=True)
class ResearchRuleV1:
    """Finite, declarative DSL: no expressions, code, or model-authored feature names."""

    operator: str
    feature_family: str | None = None
    feature_name: str | None = None
    threshold: str | None = None
    children: tuple[ResearchRuleV1, ...] = ()

    def validate(self, *, permitted_families: frozenset[str], grammar: frozenset[str], depth: int = 0) -> int:
        if depth > MAX_DEPTH:
            raise ValueError("research rule is too deeply nested")
        if self.operator not in grammar:
            raise ValueError("research rule contains a forbidden operation")
        if self.operator in {"AND", "OR"}:
            if self.feature_family is not None or self.feature_name is not None or self.threshold is not None:
                raise ValueError("logical research rule cannot contain a leaf payload")
            if not 2 <= len(self.children) <= 8:
                raise ValueError("logical research rule arity is outside the grammar")
        else:
            if self.children or self.feature_family not in permitted_families:
                raise ValueError("research rule feature family is not permitted")
            _identifier(self.feature_name or "", "feature_name", 96)
            if self.operator in {"GT", "GTE", "LT", "LTE", "EQ"}:
                if self.threshold is None or not re.fullmatch(r"-?(?:0|[1-9]\d*)(?:\.\d{1,8})?", self.threshold):
                    raise ValueError("numeric comparison requires a canonical decimal threshold")
            elif self.threshold is not None:
                raise ValueError("this research operator does not accept a threshold")
        count = 1
        for child in self.children:
            count += child.validate(permitted_families=permitted_families, grammar=grammar, depth=depth + 1)
        if count > MAX_RULE_NODES:
            raise ValueError("research rule exceeds its node bound")
        return count

    def to_dict(self) -> dict[str, Any]:
        return {"operator": self.operator, "feature_family": self.feature_family,
                "feature_name": self.feature_name, "threshold": self.threshold,
                "children": [child.to_dict() for child in self.children]}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ResearchRuleV1:
        fields = {"operator", "feature_family", "feature_name", "threshold", "children"}
        row = strict_fields(value, expected=fields, required=fields, name="ResearchRuleV1")
        if not isinstance(row["children"], list):
            raise ValueError("research rule children must be an array")
        return cls(row["operator"], row["feature_family"], row["feature_name"], row["threshold"],
                   tuple(cls.from_dict(child) for child in row["children"]))


@dataclass(frozen=True)
class ResearchProposalV1:
    research_family_id: str
    proposal_id: str
    proposal_version: int
    causal_hypothesis: str
    proposed_rule: ResearchRuleV1
    feature_dependencies: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    availability_requirements: tuple[str, ...]
    falsifier: str
    target_population: str
    horizon: str
    cost_semantics: str
    intended_ablation: str
    development_slices: tuple[str, ...]
    known_failed_predecessors: tuple[str, ...]
    proposal_lineage: tuple[str, ...]
    requested_followup_evaluation_type: str
    proposal_version_label: str = PROPOSAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _identifier(self.research_family_id, "research_family_id")
        _identifier(self.proposal_id, "proposal_id")
        if type(self.proposal_version) is not int or self.proposal_version < 1:
            raise ValueError("proposal_version must be positive")
        for name in ("causal_hypothesis", "falsifier", "target_population", "horizon", "cost_semantics",
                     "intended_ablation", "requested_followup_evaluation_type"):
            text = getattr(self, name)
            if not isinstance(text, str) or not text.strip() or len(text) > MAX_TEXT_CHARS:
                raise ValueError(f"{name} must be non-empty and bounded")
        for name in ("feature_dependencies", "availability_requirements", "development_slices",
                     "known_failed_predecessors", "proposal_lineage"):
            values = tuple(getattr(self, name))
            if any(not isinstance(item, str) or not item.strip() or len(item) > 240 for item in values):
                raise ValueError(f"{name} contains an invalid item")
            if len(values) > 64 or len(set(values)) != len(values):
                raise ValueError(f"{name} is oversized or duplicated")
            object.__setattr__(self, name, values)
        object.__setattr__(self, "evidence_refs", _refs(self.evidence_refs, "evidence_refs", maximum=16))
        if self.proposal_version_label != PROPOSAL_SCHEMA_VERSION:
            raise ValueError("unsupported research proposal schema")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.proposal_version_label, "research_family_id": self.research_family_id,
                "proposal_id": self.proposal_id, "proposal_version": self.proposal_version,
                "causal_hypothesis": self.causal_hypothesis, "proposed_rule": self.proposed_rule.to_dict(),
                "feature_dependencies": list(self.feature_dependencies),
                "evidence_refs": list(self.evidence_refs),
                "availability_requirements": list(self.availability_requirements), "falsifier": self.falsifier,
                "target_population": self.target_population, "horizon": self.horizon,
                "cost_semantics": self.cost_semantics, "intended_ablation": self.intended_ablation,
                "development_slices": list(self.development_slices),
                "known_failed_predecessors": list(self.known_failed_predecessors),
                "proposal_lineage": list(self.proposal_lineage),
                "requested_deterministic_followup_evaluation_type": self.requested_followup_evaluation_type}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ResearchProposalV1:
        fields = {"version", "research_family_id", "proposal_id", "proposal_version", "causal_hypothesis",
                  "proposed_rule", "feature_dependencies", "availability_requirements", "falsifier",
                  "evidence_refs",
                  "target_population", "horizon", "cost_semantics", "intended_ablation", "development_slices",
                  "known_failed_predecessors", "proposal_lineage", "requested_deterministic_followup_evaluation_type"}
        row = strict_fields(value, expected=fields, required=fields, name="ResearchProposalV1")
        if row["version"] != PROPOSAL_SCHEMA_VERSION:
            raise ValueError("unsupported ResearchProposalV1 version")
        return cls(row["research_family_id"], row["proposal_id"], row["proposal_version"], row["causal_hypothesis"],
                   ResearchRuleV1.from_dict(row["proposed_rule"]), tuple(row["feature_dependencies"]),
                   tuple(row["evidence_refs"]), tuple(row["availability_requirements"]), row["falsifier"], row["target_population"],
                   row["horizon"], row["cost_semantics"], row["intended_ablation"],
                   tuple(row["development_slices"]), tuple(row["known_failed_predecessors"]),
                   tuple(row["proposal_lineage"]), row["requested_deterministic_followup_evaluation_type"])

    @property
    def content_hash(self) -> str:
        return sha256_json({"type": "ResearchProposalV1", "proposal": self.to_dict()})


@dataclass(frozen=True)
class ActionAssessmentRequestV1:
    request_id: str
    action_ref: str
    evidence_manifest: tuple[AgentEvidenceRefV1, ...]
    deadline_ns: int
    schema_hash: str

    VERSION = "ActionAssessmentRequestV1"

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        sha256_ref(self.action_ref, field="action_ref")
        sha256_ref(self.schema_hash, field="schema_hash")
        if type(self.deadline_ns) is not int or self.deadline_ns < 0:
            raise ValueError("deadline_ns must be a non-negative integer")
        manifest = tuple(self.evidence_manifest)
        if len(manifest) > 8 or any(not isinstance(item, AgentEvidenceRefV1) for item in manifest):
            raise ValueError("action assessment evidence manifest is invalid or oversized")
        if len({(item.tool_name, item.artifact_ref) for item in manifest}) != len(manifest):
            raise ValueError("action assessment evidence manifest contains duplicates")
        object.__setattr__(self, "evidence_manifest", manifest)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, "request_id": self.request_id, "action_ref": self.action_ref,
                "evidence_manifest": [item.to_dict() for item in self.evidence_manifest],
                "deadline_ns": self.deadline_ns, "schema_hash": self.schema_hash}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AgentAssessmentV1:
    request_id: str
    status: str
    findings: tuple[FrozenMap, ...]

    VERSION = "AgentAssessmentV1"

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        if self.status not in {"COMPLETE", "REFUSED", "UNAVAILABLE", "INVALID"}:
            raise ValueError("assessment status is invalid")
        findings = tuple(item if isinstance(item, FrozenMap) else _frozen_map(item, "assessment finding", 4_000)
                         for item in self.findings)
        if len(findings) > 32 or len(canonical_json([item.to_dict() for item in findings]).encode("utf-8")) > 24_000:
            raise ValueError("assessment findings exceed their bound")
        object.__setattr__(self, "findings", findings)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, "request_id": self.request_id, "status": self.status,
                "findings": [item.to_dict() for item in self.findings]}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class EventExtractionRequestV1:
    request_id: str
    source_artifact_ref: str
    evidence_manifest: tuple[AgentEvidenceRefV1, ...]
    deadline_ns: int
    schema_hash: str

    VERSION = "EventExtractionRequestV1"

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        sha256_ref(self.source_artifact_ref, field="source_artifact_ref")
        sha256_ref(self.schema_hash, field="schema_hash")
        if type(self.deadline_ns) is not int or self.deadline_ns < 0:
            raise ValueError("deadline_ns must be a non-negative integer")
        manifest = tuple(self.evidence_manifest)
        if len(manifest) > 8 or any(not isinstance(item, AgentEvidenceRefV1) for item in manifest):
            raise ValueError("event extraction evidence manifest is invalid or oversized")
        if len({(item.tool_name, item.artifact_ref) for item in manifest}) != len(manifest):
            raise ValueError("event extraction evidence manifest contains duplicates")
        object.__setattr__(self, "evidence_manifest", manifest)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, "request_id": self.request_id,
                "source_artifact_ref": self.source_artifact_ref,
                "evidence_manifest": [item.to_dict() for item in self.evidence_manifest],
                "deadline_ns": self.deadline_ns, "schema_hash": self.schema_hash}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class EventExtractionV1:
    request_id: str
    source_artifact_ref: str
    extracted_events: tuple[FrozenMap, ...]

    VERSION = "EventExtractionV1"

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        sha256_ref(self.source_artifact_ref, field="source_artifact_ref")
        events = tuple(item if isinstance(item, FrozenMap) else _frozen_map(item, "extracted event", 2_000)
                       for item in self.extracted_events)
        if len(events) > 100 or len(canonical_json([item.to_dict() for item in events]).encode("utf-8")) > 24_000:
            raise ValueError("extracted events exceed their bound")
        object.__setattr__(self, "extracted_events", events)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, "request_id": self.request_id,
                "source_artifact_ref": self.source_artifact_ref,
                "extracted_events": [item.to_dict() for item in self.extracted_events]}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AgentJobV1:
    job_id: str
    request_key: str
    lifecycle_state: AgentJobStateV1
    lease_epoch: int
    lease_owner: str | None
    lease_expires_at_ns: int | None
    deadline_ns: int
    request_hash: str
    created_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {"version": "AgentJobV1", **{name: getattr(self, name).value if isinstance(getattr(self, name), StrEnum)
                else getattr(self, name) for name in self.__dataclass_fields__}}


@dataclass(frozen=True)
class AgentAttemptV1:
    attempt_id: str
    request_key: str
    attempt_index: int
    lease_epoch: int
    state: str
    started_at_ns: int
    reserved_cost_usd: str
    model_profile_hash: str
    provider_request_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": "AgentAttemptV1", **self.__dict__}


@dataclass(frozen=True)
class BrokerDispatchAuthorizationV1:
    """Durable sole-writer authorization created before a broker capability."""

    job_id: str
    request_key: str
    request_hash: str
    attempt_id: str
    call_index: int
    lease_epoch: int
    authorization_id: str
    capability_nonce: str
    evidence_hash: str
    model_profile_hash: str
    provider: str
    requested_model_id: str
    deadline_ns: int
    authorized_at_ns: int
    expires_at_ns: int
    budget_reservation_id: str
    reserved_cost_usd: str
    max_input_tokens: int
    max_output_tokens: int
    authorization_hash: str

    @classmethod
    def create(cls, *, job_id: str, request_key: str, request_hash: str, attempt_id: str,
               call_index: int, lease_epoch: int, authorization_id: str, capability_nonce: str,
               evidence_hash: str, model_profile_hash: str, provider: str, requested_model_id: str,
               deadline_ns: int, authorized_at_ns: int, expires_at_ns: int,
               budget_reservation_id: str, reserved_cost_usd: str, max_input_tokens: int,
               max_output_tokens: int) -> BrokerDispatchAuthorizationV1:
        for name, value in (("job_id", job_id), ("attempt_id", attempt_id),
                            ("authorization_id", authorization_id),
                            ("capability_nonce", capability_nonce),
                            ("budget_reservation_id", budget_reservation_id)):
            _uuid(value, name)
        for name, value in (("request_key", request_key), ("request_hash", request_hash),
                            ("evidence_hash", evidence_hash), ("model_profile_hash", model_profile_hash)):
            sha256_ref(value, field=name)
        if (type(call_index) is not int or not 1 <= call_index <= 3
                or type(lease_epoch) is not int or lease_epoch < 1):
            raise ValueError("dispatch authorization attempt fence is invalid")
        if (type(deadline_ns) is not int or type(authorized_at_ns) is not int or type(expires_at_ns) is not int
                or not authorized_at_ns < expires_at_ns <= deadline_ns):
            raise ValueError("dispatch authorization time bounds are invalid")
        if provider != "openai" or requested_model_id != "gpt-6-astra":
            raise ValueError("dispatch authorization provider/model is outside the fixed allowlist")
        if (not isinstance(reserved_cost_usd, str)
                or not re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d{1,6})?", reserved_cost_usd)):
            raise ValueError("dispatch authorization budget reservation is invalid")
        if (type(max_input_tokens) is not int or not 1 <= max_input_tokens <= 32_000
                or type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 8_000):
            raise ValueError("dispatch authorization token caps are invalid")
        body = {"version": "BrokerDispatchAuthorizationV1", "job_id": job_id,
            "request_key": request_key, "request_hash": request_hash, "attempt_id": attempt_id,
            "call_index": call_index, "lease_epoch": lease_epoch, "authorization_id": authorization_id,
            "capability_nonce": capability_nonce, "evidence_hash": evidence_hash,
            "model_profile_hash": model_profile_hash, "provider": provider,
            "requested_model_id": requested_model_id, "deadline_ns": deadline_ns,
            "authorized_at_ns": authorized_at_ns, "expires_at_ns": expires_at_ns,
            "budget_reservation_id": budget_reservation_id, "reserved_cost_usd": reserved_cost_usd,
            "max_input_tokens": max_input_tokens, "max_output_tokens": max_output_tokens}
        return cls(job_id, request_key, request_hash, attempt_id, call_index, lease_epoch, authorization_id,
            capability_nonce, evidence_hash, model_profile_hash, provider, requested_model_id, deadline_ns,
            authorized_at_ns, expires_at_ns, budget_reservation_id, reserved_cost_usd, max_input_tokens,
            max_output_tokens, sha256_json(body))

    def to_dict(self) -> dict[str, Any]:
        return {"version": "BrokerDispatchAuthorizationV1", **self.__dict__}


@dataclass(frozen=True)
class BrokerDispatchAuthorizationV2:
    """Durable DeepSeek dispatch binding; it is distinct from historical V1 auth."""

    job_id: str
    request_key: str
    request_hash: str
    attempt_id: str
    call_index: int
    lease_epoch: int
    authorization_id: str
    capability_nonce: str
    evidence_hash: str
    model_profile_hash: str
    provider_binding_hash: str
    price_schedule_hash: str
    provider: str
    requested_model_id: str
    endpoint: str
    deadline_ns: int
    authorized_at_ns: int
    expires_at_ns: int
    budget_reservation_id: str
    reserved_cost_usd: str
    max_input_tokens: int
    max_output_tokens: int
    authorization_hash: str

    @classmethod
    def create(cls, *, job_id: str, request_key: str, request_hash: str, attempt_id: str,
               call_index: int, lease_epoch: int, authorization_id: str, capability_nonce: str,
               evidence_hash: str, model_profile_hash: str, provider_binding_hash: str,
               price_schedule_hash: str, provider: str, requested_model_id: str, endpoint: str,
               deadline_ns: int, authorized_at_ns: int, expires_at_ns: int,
               budget_reservation_id: str, reserved_cost_usd: str, max_input_tokens: int,
               max_output_tokens: int) -> BrokerDispatchAuthorizationV2:
        for name, value in (("job_id", job_id), ("attempt_id", attempt_id),
                            ("authorization_id", authorization_id), ("capability_nonce", capability_nonce),
                            ("budget_reservation_id", budget_reservation_id)):
            _uuid(value, name)
        for name, value in (("request_key", request_key), ("request_hash", request_hash),
                            ("evidence_hash", evidence_hash), ("model_profile_hash", model_profile_hash),
                            ("provider_binding_hash", provider_binding_hash),
                            ("price_schedule_hash", price_schedule_hash)):
            sha256_ref(value, field=name)
        if (type(call_index) is not int or not 1 <= call_index <= 3
                or type(lease_epoch) is not int or lease_epoch < 1):
            raise ValueError("dispatch authorization attempt fence is invalid")
        if (type(deadline_ns) is not int or type(authorized_at_ns) is not int or type(expires_at_ns) is not int
                or not authorized_at_ns < expires_at_ns <= deadline_ns):
            raise ValueError("dispatch authorization time bounds are invalid")
        if (provider != "deepseek" or requested_model_id != "deepseek-flash"
                or endpoint != "https://api.deepseek.com/responses"):
            raise ValueError("dispatch authorization provider/model/endpoint is outside the fixed V2 binding")
        if (not isinstance(reserved_cost_usd, str)
                or not re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d{1,6})?", reserved_cost_usd)):
            raise ValueError("dispatch authorization budget reservation is invalid")
        if (type(max_input_tokens) is not int or max_input_tokens != 12_000
                or type(max_output_tokens) is not int or max_output_tokens != 4_000):
            raise ValueError("dispatch authorization token caps differ from the DeepSeek amendment")
        body = {"version": "BrokerDispatchAuthorizationV2", "job_id": job_id,
            "request_key": request_key, "request_hash": request_hash, "attempt_id": attempt_id,
            "call_index": call_index, "lease_epoch": lease_epoch, "authorization_id": authorization_id,
            "capability_nonce": capability_nonce, "evidence_hash": evidence_hash,
            "model_profile_hash": model_profile_hash, "provider_binding_hash": provider_binding_hash,
            "price_schedule_hash": price_schedule_hash, "provider": provider,
            "requested_model_id": requested_model_id, "endpoint": endpoint, "deadline_ns": deadline_ns,
            "authorized_at_ns": authorized_at_ns, "expires_at_ns": expires_at_ns,
            "budget_reservation_id": budget_reservation_id, "reserved_cost_usd": reserved_cost_usd,
            "max_input_tokens": max_input_tokens, "max_output_tokens": max_output_tokens}
        return cls(job_id, request_key, request_hash, attempt_id, call_index, lease_epoch, authorization_id,
            capability_nonce, evidence_hash, model_profile_hash, provider_binding_hash, price_schedule_hash,
            provider, requested_model_id, endpoint, deadline_ns, authorized_at_ns, expires_at_ns,
            budget_reservation_id, reserved_cost_usd, max_input_tokens, max_output_tokens, sha256_json(body))

    def to_dict(self) -> dict[str, Any]:
        return {"version": "BrokerDispatchAuthorizationV2", **self.__dict__}


@dataclass(frozen=True)
class AgentValidationReceiptV1:
    request_key: str
    proposal_hash: str
    provider_result_hash: str
    validator_version: str
    validation_status: str
    reasons: tuple[str, ...]
    validated_at_ns: int
    receipt_hash: str

    @classmethod
    def create(cls, request_key: str, proposal_hash: str, provider_result_hash: str, status: str,
               reasons: Sequence[str], at_ns: int) -> AgentValidationReceiptV1:
        sha256_ref(provider_result_hash, field="provider_result_hash")
        validator_version = "DETERMINISTIC_PROPOSAL_VALIDATOR_V1"
        body = {"version": "AgentValidationReceiptV1", "request_key": request_key, "proposal_hash": proposal_hash,
                "provider_result_hash": provider_result_hash,
                "validator_version": validator_version, "validation_status": status,
                "reasons": list(reasons), "validated_at_ns": at_ns}
        return cls(request_key, proposal_hash, provider_result_hash, validator_version, status,
                   tuple(reasons), at_ns, sha256_json(body))

    def to_dict(self) -> dict[str, Any]:
        return {"version": "AgentValidationReceiptV1", "request_key": self.request_key,
                "proposal_hash": self.proposal_hash, "provider_result_hash": self.provider_result_hash,
                "validator_version": self.validator_version,
                "validation_status": self.validation_status, "reasons": list(self.reasons),
                "validated_at_ns": self.validated_at_ns, "receipt_hash": self.receipt_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AgentValidationReceiptV1:
        fields = {"version", "request_key", "proposal_hash", "provider_result_hash", "validator_version",
                  "validation_status", "reasons", "validated_at_ns", "receipt_hash"}
        row = strict_fields(data, expected=fields, required=fields, name="AgentValidationReceiptV1")
        if (row["version"] != "AgentValidationReceiptV1"
                or row["validator_version"] != "DETERMINISTIC_PROPOSAL_VALIDATOR_V1"
                or not isinstance(row["reasons"], list)
                or row["validation_status"] not in {"VALID", "INVALID"}
                or type(row["validated_at_ns"]) is not int or row["validated_at_ns"] < 0):
            raise ValueError("AgentValidationReceiptV1 wire record is invalid")
        sha256_ref(row["request_key"], field="request_key")
        sha256_ref(row["proposal_hash"], field="proposal_hash")
        sha256_ref(row["provider_result_hash"], field="provider_result_hash")
        body = {key: row[key] for key in fields if key != "receipt_hash"}
        if sha256_json(body) != row["receipt_hash"]:
            raise ValueError("AgentValidationReceiptV1 hash mismatch")
        receipt = cls(row["request_key"], row["proposal_hash"], row["provider_result_hash"],
            row["validator_version"], row["validation_status"], tuple(row["reasons"]),
            row["validated_at_ns"], row["receipt_hash"])
        return receipt


@dataclass(frozen=True)
class ProviderResultV1:
    raw_output: str
    returned_model_id: str | None
    model_revision: str | None
    refusal: bool
    truncated: bool
    input_tokens: int
    output_tokens: int
    provider_request_id: str | None = None
    failure_code: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if len(self.raw_output.encode("utf-8")) > MAX_PROPOSAL_BYTES:
            raise ValueError("provider structured output exceeds the byte bound")
        for name in ("input_tokens", "output_tokens"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a non-negative integer")


@runtime_checkable
class ResearchProposalProvider(Protocol):
    def propose(self, request: ResearchProposalRequestV1, evidence: Sequence[Mapping[str, Any]]) -> ProviderResultV1: ...


@runtime_checkable
class ActionAssessmentProvider(Protocol):
    def assess(self, request: ActionAssessmentRequestV1, evidence: Sequence[Mapping[str, Any]]) -> AgentAssessmentV1: ...


@runtime_checkable
class EventExtractionProvider(Protocol):
    def extract(self, request: EventExtractionRequestV1, evidence: Sequence[Mapping[str, Any]]) -> EventExtractionV1: ...


def schema_hash(schema: Mapping[str, Any]) -> str:
    return sha256_json({"namespace": CONTRACT_NAMESPACE, "schema": schema})
