"""Build the exact initial, alias-only GPT-6 Astra offline model profile."""

from __future__ import annotations

import hashlib
from pathlib import Path

from atlas.v2._serialization import FrozenMap
from atlas.v2.agent_intelligence.budget import ProviderPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import AgentModelProfileV1, RevisionStatusV1
from atlas.v2.agent_intelligence.provider import PROMPT_CONTRACT_HASH, TOOL_CONTRACT_HASH
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
