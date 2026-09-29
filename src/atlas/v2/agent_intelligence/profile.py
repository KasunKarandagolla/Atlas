"""Build the exact initial, alias-only GPT-6 Astra offline model profile."""

from __future__ import annotations

import hashlib
from pathlib import Path

from atlas.v2._serialization import FrozenMap
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1, ProviderPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import (
    ACTION_ASSESSMENT_PACKET_MAX_BYTES,
    ACTION_ASSESSMENT_PACKET_VERSION,
    ACTION_ASSESSMENT_TASK_IDENTITY,
    ActionAssessmentProviderProfileV1,
    AgentModelProfileV1,
    AgentModelProfileV2,
    RevisionStatusV1,
)
from atlas.v2.agent_intelligence.provider import (
    ACTION_CRITIC_PROMPT_HASH,
    ACTION_CRITIC_SCHEMA_HASH,
    PROMPT_CONTRACT_HASH,
    TOOL_CONTRACT_HASH,
)
from atlas.v2.agent_intelligence.validation import proposal_schema_hash


def initial_model_profile(*, price_schedule: ProviderPriceScheduleV1,
                          agent_lock_path: str | Path) -> AgentModelProfileV1:
    lock_bytes = Path(agent_lock_path).read_bytes()
    return AgentModelProfileV1(
        provider="openai",
        requested_model_id="gpt-6-astra",
        returned_model_id=None,
        model_revision=None,
        revision_status=RevisionStatusV1.ALIAS_ONLY,
        reasoning_settings=FrozenMap({"effort": "medium"}),
        max_input_tokens=price_schedule.maximum_input_tokens,
        max_output_tokens=price_schedule.maximum_output_tokens,
        prompt_contract_hash=PROMPT_CONTRACT_HASH,
        schema_hash=proposal_schema_hash(),
        tool_contract_hash=TOOL_CONTRACT_HASH,
        runtime_dependency_hash=hashlib.sha256(lock_bytes).hexdigest(),
        pricing_schedule_id=price_schedule.version,
    )


def deepseek_v41_flash_model_profile(*, price_schedule: DeepSeekPriceScheduleV1,
                                     agent_lock_path: str | Path) -> AgentModelProfileV2:
    """Build the additive alias-only DeepSeek profile for the fixed Responses binding."""
    lock_bytes = Path(agent_lock_path).read_bytes()
    if price_schedule.provider != "deepseek" or price_schedule.requested_model_id != "deepseek-flash":
        raise ValueError("DeepSeek model profile requires the reviewed DeepSeek price schedule")
    return AgentModelProfileV2(
        provider="deepseek",
        requested_model_id="deepseek-flash",
        returned_model_id=None,
        model_revision=None,
        revision_status=RevisionStatusV1.ALIAS_ONLY,
        base_url="https://api.deepseek.com",
        endpoint_path="/responses",
        model_family="DeepSeek-V4.1-Flash",
        reasoning_setting_id="DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1",
        reasoning_settings=FrozenMap({"effort": "high"}),
        structured_output_format="json_schema",
        max_input_tokens=price_schedule.maximum_input_tokens,
        max_output_tokens=price_schedule.maximum_output_tokens,
        max_model_calls_per_job=price_schedule.maximum_model_calls_per_job,
        max_read_tool_calls_per_job=price_schedule.maximum_read_tool_calls_per_job,
        max_concurrent_jobs=price_schedule.maximum_concurrent_research_jobs,
        prompt_contract_hash=PROMPT_CONTRACT_HASH,
        schema_hash=proposal_schema_hash(),
        tool_contract_hash=TOOL_CONTRACT_HASH,
        runtime_dependency_hash=hashlib.sha256(lock_bytes).hexdigest(),
        pricing_schedule_id=price_schedule.version,
    )


def deepseek_v41_flash_action_critic_profile(*, price_schedule: DeepSeekPriceScheduleV1,
        agent_lock_path: str | Path) -> ActionAssessmentProviderProfileV1:
    """Build the separate one-call ActionAssessmentProvider profile over the accepted price schedule."""
    lock_bytes = Path(agent_lock_path).read_bytes()
    if (price_schedule.provider != "deepseek" or price_schedule.requested_model_id != "deepseek-flash"
            or price_schedule.endpoint != "https://api.deepseek.com/responses"):
        raise ValueError("action critic requires the accepted DeepSeek price schedule")
    return ActionAssessmentProviderProfileV1(
        provider="deepseek",
        requested_model_id="deepseek-flash",
        model_family="DeepSeek-V4.1-Flash",
        revision_status=RevisionStatusV1.ALIAS_ONLY,
        base_url="https://api.deepseek.com",
        endpoint_path="/responses",
        reasoning_setting_id="DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1",
        reasoning_settings=FrozenMap({"effort": "high"}),
        structured_output_format="json_schema",
        task_identity=ACTION_ASSESSMENT_TASK_IDENTITY,
        price_schedule_id=price_schedule.version,
        price_schedule_hash=price_schedule.content_hash,
        prompt_hash=ACTION_CRITIC_PROMPT_HASH,
        schema_hash=ACTION_CRITIC_SCHEMA_HASH,
        runtime_dependency_hash=hashlib.sha256(lock_bytes).hexdigest(),
        maximum_input_tokens=12_000,
        maximum_output_tokens=2_048,
        maximum_model_calls=1,
        maximum_dynamic_tools=0,
        maximum_concurrent_jobs=1,
        packet_version=ACTION_ASSESSMENT_PACKET_VERSION,
        packet_max_bytes=ACTION_ASSESSMENT_PACKET_MAX_BYTES,
    )
