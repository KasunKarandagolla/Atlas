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

from atlas.v2._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, strict_fields

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
ACTION_ASSESSMENT_PACKET_VERSION = "SealedActionAssessmentPacketV1"
ACTION_ASSESSMENT_REQUEST_VERSION = "ActionAssessmentRequestV2"
ACTION_ASSESSMENT_SCHEMA_VERSION = "ATLAS_ACTION_CRITIC_OUTPUT_V1"
ACTION_ASSESSMENT_PROFILE_VERSION = "ActionAssessmentProviderProfileV1"
ACTION_ASSESSMENT_TASK_IDENTITY = "ActionAssessmentProvider"
ACTION_ASSESSMENT_PACKET_MAX_BYTES = 36_000
ACTION_ASSESSMENT_MAX_FINDINGS = 16
ACTION_ASSESSMENT_FINDING_TYPES = frozenset({
    "EVENT_ENTITY_AMBIGUOUS",
    "EVENT_TIME_AMBIGUOUS",
    "SOURCE_CLAIMS_CONFLICT",
    "SOURCE_ASSERTION_UNSUPPORTED",
    "ARTIFACT_SEMANTIC_MISMATCH",
    "REQUIRED_CONTEXT_UNAVAILABLE",
})
_CRITIC_PACKET_FORBIDDEN_KEY = re.compile(
    r"(?:credential|api[_-]?key|secret|private[_-]?key|password|account[_-]?id|exchange[_-]?account|"
    r"raw[_-]?log|file[_-]?path|filesystem|database[_-]?row|url)", re.I)
_CRITIC_PACKET_FORBIDDEN_VALUE = re.compile(
    r"https?://|file://|(?:^|\s)/(?:home|root|tmp|etc)/|\bBearer\s+[A-Za-z0-9._~-]{16,}|"
    r"sk-[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{20,}|"
    r"-----BEGIN [A-Z ]+PRIVATE KEY-----", re.I)


def _validate_critic_packet_summary(value: Any, *, depth: int = 0, counter: list[int] | None = None) -> None:
    nodes = counter if counter is not None else [0]
    nodes[0] += 1
    if nodes[0] > 4_000 or depth > 10:
        raise ValueError("action-assessment summary exceeds its structural bound")
    if isinstance(value, Mapping):
        if len(value) > 128 or any(not isinstance(key, str) or _CRITIC_PACKET_FORBIDDEN_KEY.search(key) for key in value):
            raise ValueError("action-assessment summary contains an unsupported field")
        for child in value.values():
            _validate_critic_packet_summary(child, depth=depth + 1, counter=nodes)
    elif isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError("action-assessment summary array exceeds its bound")
        for child in value:
            _validate_critic_packet_summary(child, depth=depth + 1, counter=nodes)
    elif isinstance(value, str):
        if len(value) > 2_000 or _CRITIC_PACKET_FORBIDDEN_VALUE.search(value):
            raise ValueError("action-assessment summary contains an unsafe or oversized value")
    elif value is None or type(value) in {bool, int, float}:
        return
    else:
        raise ValueError("action-assessment summary contains an unsupported value type")
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
class SealedActionAssessmentPacketV1:
    """Content-addressed, host-produced evidence packet for a hidden zero-authority critic."""

    originating_receipt_ref: str
    decision_event_id: str
    candidate_set_ref: str
    selected_candidate_ref: str
    selector_policy_hash: str
    action_artifact_ref: str
    action_hash: str
    economic_evaluation_ref: str
    m0_model_ref: str
    m0_prediction_ref: str
    m1_diagnostic_ref: str
    analogue_diagnostic_ref: str
    pretrade_scenario_ref: str
    estimation_uncertainty_ref: str
    execution_uncertainty_ref: str
    numerical_error_ref: str
    support_ref: str
    calibration_ref: str
    ood_ref: str
    deterministic_stress_ref: str
    portfolio_ref: str
    portfolio_es_ref: str
    source_health_ref: str
    source_health_evidence_refs: tuple[str, ...]
    market_evidence_refs: tuple[str, ...]
    artifact_refs: tuple[str, ...]
    artifact_types: FrozenMap
    availability_by_ref: FrozenMap
    source_cutoff_t0_ns: int
    sealed_cutoff_t_ns: int
    original_deadline_d_ns: int
    missingness: FrozenMap
    summaries: FrozenMap

    VERSION = ACTION_ASSESSMENT_PACKET_VERSION

    def __post_init__(self) -> None:
        required_refs = (
            "originating_receipt_ref", "candidate_set_ref", "selected_candidate_ref", "selector_policy_hash",
            "action_artifact_ref", "action_hash", "economic_evaluation_ref", "m0_model_ref",
            "m0_prediction_ref", "m1_diagnostic_ref", "analogue_diagnostic_ref", "pretrade_scenario_ref",
            "estimation_uncertainty_ref", "execution_uncertainty_ref", "numerical_error_ref", "support_ref",
            "calibration_ref", "ood_ref", "deterministic_stress_ref", "portfolio_ref", "portfolio_es_ref",
            "source_health_ref",
        )
        for name in required_refs:
            sha256_ref(getattr(self, name), field=name)
        nonblank(self.decision_event_id, field="decision_event_id")
        source_refs = _refs(self.source_health_evidence_refs, "source_health_evidence_refs", maximum=64)
        market_refs = _refs(self.market_evidence_refs, "market_evidence_refs", maximum=256)
        refs = _refs(self.artifact_refs, "artifact_refs", maximum=384)
        artifact_reference_fields = set(required_refs) - {"selector_policy_hash", "action_hash"}
        expected_refs = set(source_refs) | set(market_refs) | {
            getattr(self, name) for name in artifact_reference_fields
        }
        if not expected_refs.issubset(refs):
            raise ValueError("sealed packet omits a required exact artifact reference")
        if self.source_health_ref != self.originating_receipt_ref:
            raise ValueError("source health must bind the exact originating receipt")
        if not (type(self.source_cutoff_t0_ns) is int and type(self.sealed_cutoff_t_ns) is int
                and type(self.original_deadline_d_ns) is int
                and 0 <= self.source_cutoff_t0_ns <= self.sealed_cutoff_t_ns < self.original_deadline_d_ns):
            raise ValueError("sealed packet chronology must satisfy T0 <= T < D")
        artifact_types = self.artifact_types if isinstance(self.artifact_types, FrozenMap) else FrozenMap(self.artifact_types)
        availability = self.availability_by_ref if isinstance(self.availability_by_ref, FrozenMap) else FrozenMap(self.availability_by_ref)
        missingness = self.missingness if isinstance(self.missingness, FrozenMap) else FrozenMap(self.missingness)
        summaries = self.summaries if isinstance(self.summaries, FrozenMap) else FrozenMap(self.summaries)
        if set(artifact_types) != set(refs) or set(availability) != set(refs):
            raise ValueError("sealed packet artifact inventory is incomplete or ambiguous")
        if any(not isinstance(artifact_types[ref], str) or type(availability[ref]) is not int
               or availability[ref] > self.sealed_cutoff_t_ns for ref in refs):
            raise ValueError("sealed packet exposes missing, untyped or future artifacts")
        if any(availability[ref] > self.source_cutoff_t0_ns for ref in market_refs):
            raise ValueError("market/news evidence cannot follow the information cutoff T0")
        if set(summaries) != set(refs):
            raise ValueError("sealed packet must contain one bounded typed summary for every exact ref")
        for ref in refs:
            summary_entry = summaries[ref]
            if (not isinstance(summary_entry, Mapping) or summary_entry.get("artifact_ref") != ref
                    or summary_entry.get("artifact_type") != artifact_types[ref]
                    or not isinstance(summary_entry.get("summary"), Mapping)):
                raise ValueError("sealed packet summary does not match its exact artifact inventory")
            _validate_critic_packet_summary(summary_entry)
        summary_by_ref = {ref: summaries[ref]["summary"] for ref in refs}
        action_summary = summary_by_ref[self.action_artifact_ref]
        candidate_summary = summary_by_ref[self.selected_candidate_ref]
        candidate_set_summary = summary_by_ref[self.candidate_set_ref]
        evaluation_summary = summary_by_ref[self.economic_evaluation_ref]
        deterministic_bindings = {
            "action_identity_hash": isinstance(action_summary.get("action_identity"), Mapping)
                and sha256_json(action_summary["action_identity"]) == self.action_hash,
            "action_hash": action_summary.get("action_hash") == self.action_hash,
            "action_candidate_ref": action_summary.get("candidate_ref") == self.selected_candidate_ref,
            "action_candidate_set_ref": action_summary.get("candidate_set_ref") == self.candidate_set_ref,
            "action_sizing_ref": action_summary.get("sizing_ref") in refs,
            "selected_candidate_id": candidate_set_summary.get("selected_candidate_id")
                == candidate_summary.get("candidate_id"),
            "selected_candidate_ref": candidate_set_summary.get("selected_candidate_ref")
                == self.selected_candidate_ref,
            "selector_policy_hash": candidate_set_summary.get("selection_policy_hash")
                == self.selector_policy_hash,
            "evaluation_action_hash": evaluation_summary.get("action_hash") == self.action_hash,
            "evaluation_action_ref": evaluation_summary.get("action_artifact_ref") == self.action_artifact_ref,
            "evaluation_candidate_ref": evaluation_summary.get("candidate_ref") == self.selected_candidate_ref,
            "evaluation_candidate_set_ref": evaluation_summary.get("candidate_set_ref") == self.candidate_set_ref,
            "evaluation_selector_policy_hash": evaluation_summary.get("selection_policy_hash")
                == self.selector_policy_hash,
            "m0_model_ref": evaluation_summary.get("m0_model_ref") == self.m0_model_ref,
            "m0_prediction_ref": evaluation_summary.get("m0_prediction_ref") == self.m0_prediction_ref,
            "pretrade_scenario_ref": evaluation_summary.get("pretrade_scenario_ref") == self.pretrade_scenario_ref,
            "estimation_uncertainty_ref": evaluation_summary.get("estimation_uncertainty_ref")
                == self.estimation_uncertainty_ref,
            "execution_uncertainty_ref": evaluation_summary.get("execution_uncertainty_ref")
                == self.execution_uncertainty_ref,
            "numerical_error_ref": evaluation_summary.get("numerical_error_ref") == self.numerical_error_ref,
            "support_ref": evaluation_summary.get("support_ref") == self.support_ref,
            "calibration_ref": evaluation_summary.get("calibration_ref") == self.calibration_ref,
            "ood_ref": evaluation_summary.get("ood_ref") == self.ood_ref,
            "deterministic_stress_ref": evaluation_summary.get("deterministic_stress_ref")
                == self.deterministic_stress_ref,
            "portfolio_ref": evaluation_summary.get("existing_portfolio_ref") == self.portfolio_ref,
        }
        if not all(deterministic_bindings.values()):
            failed = "_".join(name for name, matches in deterministic_bindings.items() if not matches)
            raise ValueError(f"sealed action-assessment deterministic binding mismatch: {failed}")
        for ref in (self.m0_model_ref, self.m0_prediction_ref, self.m1_diagnostic_ref,
                    self.analogue_diagnostic_ref, self.pretrade_scenario_ref, self.estimation_uncertainty_ref,
                    self.execution_uncertainty_ref, self.numerical_error_ref, self.support_ref,
                    self.calibration_ref, self.ood_ref, self.deterministic_stress_ref, self.portfolio_ref,
                    self.portfolio_es_ref):
            artifact_summary = summary_by_ref[ref]
            if (artifact_summary.get("action_hash") != self.action_hash
                    and artifact_summary.get("query_action_hash") != self.action_hash):
                raise ValueError("sealed derived assessment artifact belongs to a different action")
        object.__setattr__(self, "market_evidence_refs", market_refs)
        object.__setattr__(self, "source_health_evidence_refs", source_refs)
        object.__setattr__(self, "artifact_refs", refs)
        object.__setattr__(self, "artifact_types", artifact_types)
        object.__setattr__(self, "availability_by_ref", availability)
        object.__setattr__(self, "missingness", missingness)
        object.__setattr__(self, "summaries", summaries)
        if len(canonical_json(self.to_dict()).encode("utf-8")) > ACTION_ASSESSMENT_PACKET_MAX_BYTES:
            raise ValueError("sealed action-assessment packet exceeds its byte limit")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "originating_receipt_ref": self.originating_receipt_ref,
            "decision_event_id": self.decision_event_id,
            "candidate_set_ref": self.candidate_set_ref,
            "selected_candidate_ref": self.selected_candidate_ref,
            "selector_policy_hash": self.selector_policy_hash,
            "action_artifact_ref": self.action_artifact_ref,
            "action_hash": self.action_hash,
            "economic_evaluation_ref": self.economic_evaluation_ref,
            "m0_model_ref": self.m0_model_ref,
            "m0_prediction_ref": self.m0_prediction_ref,
            "m1_diagnostic_ref": self.m1_diagnostic_ref,
            "analogue_diagnostic_ref": self.analogue_diagnostic_ref,
            "pretrade_scenario_ref": self.pretrade_scenario_ref,
            "estimation_uncertainty_ref": self.estimation_uncertainty_ref,
            "execution_uncertainty_ref": self.execution_uncertainty_ref,
            "numerical_error_ref": self.numerical_error_ref,
            "support_ref": self.support_ref,
            "calibration_ref": self.calibration_ref,
            "ood_ref": self.ood_ref,
            "deterministic_stress_ref": self.deterministic_stress_ref,
            "portfolio_ref": self.portfolio_ref,
            "portfolio_es_ref": self.portfolio_es_ref,
            "source_health_ref": self.source_health_ref,
            "source_health_evidence_refs": list(self.source_health_evidence_refs),
            "market_evidence_refs": list(self.market_evidence_refs),
            "artifact_refs": list(self.artifact_refs),
            "artifact_types": self.artifact_types.to_dict(),
            "availability_by_ref": self.availability_by_ref.to_dict(),
            "source_cutoff_t0_ns": self.source_cutoff_t0_ns,
            "sealed_cutoff_t_ns": self.sealed_cutoff_t_ns,
            "original_deadline_d_ns": self.original_deadline_d_ns,
            "missingness": self.missingness.to_dict(),
            "summaries": self.summaries.to_dict(),
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @property
    def packet_ref(self) -> str:
        return sha256_json({"artifact_type": self.VERSION, "content_hash": self.content_hash})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SealedActionAssessmentPacketV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported sealed action-assessment packet")
        for name in ("market_evidence_refs", "source_health_evidence_refs", "artifact_refs"):
            if not isinstance(row[name], list):
                raise ValueError(f"{name} must be an array")
        for name in ("artifact_types", "availability_by_ref", "missingness", "summaries"):
            if not isinstance(row[name], Mapping):
                raise ValueError(f"{name} must be an object")
            row[name] = FrozenMap(row[name])
        row["market_evidence_refs"] = tuple(row["market_evidence_refs"])
        row["source_health_evidence_refs"] = tuple(row["source_health_evidence_refs"])
        row["artifact_refs"] = tuple(row["artifact_refs"])
        return cls(**row)


@dataclass(frozen=True)
class ActionAssessmentRequestV2:
    request_id: str
    packet_ref: str
    packet_hash: str
    action_hash: str
    task_identity: str
    profile_hash: str
    provider_binding_hash: str
    price_schedule_hash: str
    provider: str
    requested_model_id: str
    model_family: str
    revision_status: RevisionStatusV1
    endpoint: str
    prompt_hash: str
    schema_hash: str
    deadline_ns: int
    max_model_calls: int = 1
    max_dynamic_tools: int = 0
    max_input_tokens: int = 12_000
    max_output_tokens: int = 2_048

    VERSION = ACTION_ASSESSMENT_REQUEST_VERSION

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        for name in ("packet_ref", "packet_hash", "action_hash", "profile_hash", "provider_binding_hash",
                     "price_schedule_hash", "prompt_hash", "schema_hash"):
            sha256_ref(getattr(self, name), field=name)
        if (self.task_identity != ACTION_ASSESSMENT_TASK_IDENTITY or self.provider != "deepseek"
                or self.requested_model_id != "deepseek-flash" or self.model_family != "DeepSeek-V4.1-Flash"
                or self.endpoint != "https://api.deepseek.com/responses"
                or RevisionStatusV1(self.revision_status) != RevisionStatusV1.ALIAS_ONLY):
            raise ValueError("action assessment request provider/task binding is outside the frozen critic profile")
        if (type(self.deadline_ns) is not int or self.deadline_ns < 0 or self.max_model_calls != 1
                or self.max_dynamic_tools != 0 or self.max_input_tokens != 12_000 or self.max_output_tokens != 2_048):
            raise ValueError("action assessment request limits differ from the frozen one-call profile")
        object.__setattr__(self, "revision_status", RevisionStatusV1(self.revision_status))

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: value.value if isinstance(value, StrEnum) else value
                for name, value in self.__dict__.items()}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionAssessmentRequestV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported ActionAssessmentRequestV2")
        return cls(**row)


@dataclass(frozen=True)
class ActionAssessmentProviderProfileV1:
    provider: str
    requested_model_id: str
    model_family: str
    revision_status: RevisionStatusV1
    base_url: str
    endpoint_path: str
    reasoning_setting_id: str
    reasoning_settings: FrozenMap
    structured_output_format: str
    task_identity: str
    price_schedule_id: str
    price_schedule_hash: str
    prompt_hash: str
    schema_hash: str
    runtime_dependency_hash: str
    maximum_input_tokens: int = 12_000
    maximum_output_tokens: int = 2_048
    maximum_model_calls: int = 1
    maximum_dynamic_tools: int = 0
    maximum_concurrent_jobs: int = 1
    packet_version: str = ACTION_ASSESSMENT_PACKET_VERSION
    packet_max_bytes: int = ACTION_ASSESSMENT_PACKET_MAX_BYTES

    VERSION = ACTION_ASSESSMENT_PROFILE_VERSION

    def __post_init__(self) -> None:
        fixed = (self.provider == "deepseek" and self.requested_model_id == "deepseek-flash"
                 and self.model_family == "DeepSeek-V4.1-Flash"
                 and RevisionStatusV1(self.revision_status) == RevisionStatusV1.ALIAS_ONLY
                 and self.base_url == "https://api.deepseek.com" and self.endpoint_path == "/responses"
                 and self.reasoning_setting_id == "DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1"
                 and self.structured_output_format == "json_schema"
                 and self.task_identity == ACTION_ASSESSMENT_TASK_IDENTITY
                 and self.maximum_input_tokens == 12_000 and self.maximum_output_tokens == 2_048
                 and self.maximum_model_calls == 1 and self.maximum_dynamic_tools == 0
                 and self.maximum_concurrent_jobs == 1 and self.packet_version == ACTION_ASSESSMENT_PACKET_VERSION
                 and self.packet_max_bytes == ACTION_ASSESSMENT_PACKET_MAX_BYTES)
        if not fixed or self.reasoning_settings.to_dict() != {"effort": "high"}:
            raise ValueError("action-critic model profile differs from the fixed hidden shadow binding")
        for name in ("price_schedule_hash", "prompt_hash", "schema_hash", "runtime_dependency_hash"):
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "revision_status", RevisionStatusV1(self.revision_status))

    @property
    def endpoint(self) -> str:
        return self.base_url + self.endpoint_path

    @property
    def provider_binding_hash(self) -> str:
        return sha256_json({"provider": self.provider, "requested_model_id": self.requested_model_id,
            "model_family": self.model_family, "endpoint": self.endpoint, "task_identity": self.task_identity,
            "revision_status": self.revision_status.value, "reasoning_setting_id": self.reasoning_setting_id,
            "structured_output_format": self.structured_output_format})

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: value.to_dict() if isinstance(value, FrozenMap)
                else value.value if isinstance(value, StrEnum) else value
                for name, value in self.__dict__.items()}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ActionAssessmentFindingV1:
    finding_type: str
    subject_artifact_ref: str
    supporting_evidence_refs: tuple[str, ...]
    explanation: str
    field_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.finding_type not in ACTION_ASSESSMENT_FINDING_TYPES:
            raise ValueError("unknown action-assessment finding type")
        sha256_ref(self.subject_artifact_ref, field="subject_artifact_ref")
        refs = _refs(self.supporting_evidence_refs, "supporting_evidence_refs", maximum=16)
        if not isinstance(self.explanation, str) or not self.explanation.strip() or len(self.explanation) > 1_000:
            raise ValueError("action-assessment explanation is empty or exceeds its bound")
        paths = tuple(self.field_paths)
        if len(paths) > 8 or len(set(paths)) != len(paths) or any(
                not isinstance(path, str) or len(path) > 160 or not path.startswith("/") for path in paths):
            raise ValueError("action-assessment field paths are invalid or oversized")
        object.__setattr__(self, "supporting_evidence_refs", refs)
        object.__setattr__(self, "field_paths", paths)

    def to_dict(self) -> dict[str, Any]:
        return {"finding_type": self.finding_type, "subject_artifact_ref": self.subject_artifact_ref,
                "supporting_evidence_refs": list(self.supporting_evidence_refs),
                "explanation": self.explanation, "field_paths": list(self.field_paths)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionAssessmentFindingV1:
        fields = set(cls.__dataclass_fields__)
        row = strict_fields(data, expected=fields, required=fields - {"field_paths"}, name="ActionAssessmentFindingV1")
        if not isinstance(row["supporting_evidence_refs"], list) or not isinstance(row.get("field_paths", []), list):
            raise ValueError("action-assessment finding refs/paths must be arrays")
        return cls(row["finding_type"], row["subject_artifact_ref"], tuple(row["supporting_evidence_refs"]),
                   row["explanation"], tuple(row.get("field_paths", [])))


@dataclass(frozen=True)
class ActionAssessmentResultV2:
    request_id: str
    packet_ref: str
    packet_hash: str
    action_hash: str
    status: str
    findings: tuple[ActionAssessmentFindingV1, ...]

    VERSION = "AgentAssessmentV2"

    def __post_init__(self) -> None:
        _uuid(self.request_id, "request_id")
        for name in ("packet_ref", "packet_hash", "action_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.status not in {"COMPLETE", "REFUSED", "UNAVAILABLE", "INVALID", "EXPIRED"}:
            raise ValueError("action-assessment result status is invalid")
        findings = tuple(self.findings)
        if len(findings) > ACTION_ASSESSMENT_MAX_FINDINGS or any(
                not isinstance(item, ActionAssessmentFindingV1) for item in findings):
            raise ValueError("action-assessment findings exceed their closed bound")
        if self.status != "COMPLETE" and findings:
            raise ValueError("non-complete action assessment cannot contain findings")
        object.__setattr__(self, "findings", findings)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, "request_id": self.request_id, "packet_ref": self.packet_ref,
                "packet_hash": self.packet_hash, "action_hash": self.action_hash, "status": self.status,
                "findings": [finding.to_dict() for finding in self.findings]}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionAssessmentResultV2:
        fields = {"version", "request_id", "packet_ref", "packet_hash", "action_hash", "status", "findings"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row["version"] != cls.VERSION or not isinstance(row["findings"], list):
            raise ValueError("invalid action-assessment result wire")
        return cls(row["request_id"], row["packet_ref"], row["packet_hash"], row["action_hash"],
                   row["status"], tuple(ActionAssessmentFindingV1.from_dict(item) for item in row["findings"]))


@dataclass(frozen=True)
class ActionAssessmentValidationReceiptV1:
    request_hash: str
    packet_ref: str
    packet_hash: str
    action_hash: str
    provider_output_hash: str
    validator_version: str
    validation_status: str
    reasons: tuple[str, ...]
    validated_at_ns: int
    receipt_hash: str

    VERSION = "ActionAssessmentValidationReceiptV1"
    VALIDATOR_VERSION = "DETERMINISTIC_ACTION_ASSESSMENT_VALIDATOR_V1"

    def __post_init__(self) -> None:
        for name in ("request_hash", "packet_ref", "packet_hash", "action_hash", "provider_output_hash", "receipt_hash"):
            sha256_ref(getattr(self, name), field=name)
        if (self.validator_version != self.VALIDATOR_VERSION
                or self.validation_status not in {"VALID", "INVALID", "LATE", "UNAVAILABLE", "REFUSED"}
                or type(self.validated_at_ns) is not int or self.validated_at_ns < 0):
            raise ValueError("action-assessment validation receipt binding is invalid")
        reasons = tuple(self.reasons)
        if len(reasons) > 16 or any(not isinstance(item, str) or not item or len(item) > 160 for item in reasons):
            raise ValueError("action-assessment validation reasons exceed their bound")
        object.__setattr__(self, "reasons", reasons)
        body = {"version": self.VERSION, "request_hash": self.request_hash, "packet_ref": self.packet_ref,
            "packet_hash": self.packet_hash, "action_hash": self.action_hash,
            "provider_output_hash": self.provider_output_hash, "validator_version": self.validator_version,
            "validation_status": self.validation_status, "reasons": list(reasons),
            "validated_at_ns": self.validated_at_ns}
        if sha256_json(body) != self.receipt_hash:
            raise ValueError("action-assessment validation receipt hash mismatch")

    @classmethod
    def create(cls, *, request_hash: str, packet_ref: str, packet_hash: str, action_hash: str,
               provider_output_hash: str, status: str, reasons: Sequence[str], at_ns: int
               ) -> ActionAssessmentValidationReceiptV1:
        body = {"version": cls.VERSION, "request_hash": request_hash, "packet_ref": packet_ref,
                "packet_hash": packet_hash, "action_hash": action_hash,
                "provider_output_hash": provider_output_hash, "validator_version": cls.VALIDATOR_VERSION,
                "validation_status": status, "reasons": list(reasons), "validated_at_ns": at_ns}
        return cls(request_hash, packet_ref, packet_hash, action_hash, provider_output_hash,
                   cls.VALIDATOR_VERSION, status, tuple(reasons), at_ns, sha256_json(body))

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: value for name, value in self.__dict__.items()
                if name != "receipt_hash"}, "receipt_hash": self.receipt_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionAssessmentValidationReceiptV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION or not isinstance(row["reasons"], list):
            raise ValueError("invalid action-assessment validation receipt wire")
        row["reasons"] = tuple(row["reasons"])
        return cls(**row)


@dataclass(frozen=True)
class ActionAssessmentDispatchAuthorizationV1:
    task_identity: str
    packet_ref: str
    packet_hash: str
    request_hash: str
    action_hash: str
    profile_hash: str
    provider_binding_hash: str
    price_schedule_hash: str
    provider: str
    requested_model_id: str
    endpoint: str
    attempt_id: str
    deadline_ns: int
    authorized_at_ns: int
    expires_at_ns: int
    reservation_id: str
    reserved_cost_usd: str
    max_input_tokens: int
    max_output_tokens: int
    authorization_id: str
    capability_nonce: str
    authorization_hash: str

    VERSION = "ActionAssessmentDispatchAuthorizationV1"

    @classmethod
    def create(cls, *, task_identity: str, packet_ref: str, packet_hash: str, request_hash: str,
               action_hash: str, profile_hash: str, provider_binding_hash: str, price_schedule_hash: str,
               provider: str, requested_model_id: str, endpoint: str, attempt_id: str, deadline_ns: int,
               authorized_at_ns: int, expires_at_ns: int, reservation_id: str, reserved_cost_usd: str,
               max_input_tokens: int, max_output_tokens: int, authorization_id: str,
               capability_nonce: str) -> ActionAssessmentDispatchAuthorizationV1:
        for name, value in (("attempt_id", attempt_id), ("reservation_id", reservation_id),
                            ("authorization_id", authorization_id), ("capability_nonce", capability_nonce)):
            _uuid(value, name)
        for name, value in (("packet_ref", packet_ref), ("packet_hash", packet_hash),
                            ("request_hash", request_hash), ("action_hash", action_hash),
                            ("profile_hash", profile_hash), ("provider_binding_hash", provider_binding_hash),
                            ("price_schedule_hash", price_schedule_hash)):
            sha256_ref(value, field=name)
        if (task_identity != ACTION_ASSESSMENT_TASK_IDENTITY or provider != "deepseek"
                or requested_model_id != "deepseek-flash" or endpoint != "https://api.deepseek.com/responses"):
            raise ValueError("action-assessment dispatch task/provider binding is invalid")
        if (not (type(deadline_ns) is int and type(authorized_at_ns) is int and type(expires_at_ns) is int
                 and authorized_at_ns < expires_at_ns <= deadline_ns)
                or max_input_tokens != 12_000 or max_output_tokens != 2_048):
            raise ValueError("action-assessment dispatch time/token ceilings are invalid")
        if (not isinstance(reserved_cost_usd, str)
                or not re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d{1,6})?", reserved_cost_usd)):
            raise ValueError("action-assessment cost reservation is invalid")
        body = {"version": cls.VERSION, "task_identity": task_identity, "packet_ref": packet_ref,
                "packet_hash": packet_hash, "request_hash": request_hash, "action_hash": action_hash,
                "profile_hash": profile_hash, "provider_binding_hash": provider_binding_hash,
                "price_schedule_hash": price_schedule_hash, "provider": provider,
                "requested_model_id": requested_model_id, "endpoint": endpoint, "attempt_id": attempt_id,
                "deadline_ns": deadline_ns, "authorized_at_ns": authorized_at_ns,
                "expires_at_ns": expires_at_ns, "reservation_id": reservation_id,
                "reserved_cost_usd": reserved_cost_usd, "max_input_tokens": max_input_tokens,
                "max_output_tokens": max_output_tokens, "authorization_id": authorization_id,
                "capability_nonce": capability_nonce}
        return cls(task_identity, packet_ref, packet_hash, request_hash, action_hash, profile_hash,
                   provider_binding_hash, price_schedule_hash, provider, requested_model_id, endpoint,
                   attempt_id, deadline_ns, authorized_at_ns, expires_at_ns, reservation_id,
                   reserved_cost_usd, max_input_tokens, max_output_tokens, authorization_id,
                   capability_nonce, sha256_json(body))

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **self.__dict__}


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
class ActionAssessmentProviderV2(Protocol):
    """Direct tool-free provider contract over a sealed immutable packet."""

    def assess(self, request: ActionAssessmentRequestV2,
               packet: SealedActionAssessmentPacketV1) -> ProviderResultV1: ...


@runtime_checkable
class EventExtractionProvider(Protocol):
    def extract(self, request: EventExtractionRequestV1, evidence: Sequence[Mapping[str, Any]]) -> EventExtractionV1: ...


def schema_hash(schema: Mapping[str, Any]) -> str:
    return sha256_json({"namespace": CONTRACT_NAMESPACE, "schema": schema})
