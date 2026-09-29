# ATLAS Session 028 Handoff — DeepSeek V4.1 Flash Amendment

**Amendment:** `ATLAS_AGENT_PROVIDER_MODEL_AMENDMENT_DEEPSEEK_V41_V1`

**Branch:** `impl/session-028-agent-provider-deepseek-v41-amendment`

**Starting checkpoint:** `81ee4c1edbe0156691863b90daf3174168fa387b`

**Tested implementation commit:** `32b244d8e0b7066ee43e2f184b6a8d87914f28ee`

**Implementation remote tip:** independently verified as `32b244d8e0b7066ee43e2f184b6a8d87914f28ee` before this docs-only closeout.

**Docs-only commit / final remote tip:** reported in the final closeout because a commit cannot contain its own resulting SHA.

**Merge:** none.

## Scope and preserved history

This is an additive `ResearchProposalProvider` provider/model amendment. `AgentModelProfileV1` remains pinned to `openai / gpt-6-astra`, its Responses API wire record and `OPENAI_API_KEY` broker ownership remain unchanged, and the Session-026 validation, gate, and agent freeze were not edited. The OpenAI pricing artifact and both dependency locks remain unchanged. The OpenAI profile content hash remains `75e4e39340c534cfc32503b36baa5c7c28bd6cc5bb2e8db02310152d0e6bd34b`.

The additive DeepSeek profile is `AgentModelProfileV2`, content hash `819b22be3b3122df8b3574492a1aac1de4280011b4f9f28b8caca945bac16adf`, with provider binding hash `3fd47a85e3c935f2d1ee01438b9b7fd697e6aa0005c41ed4934d6a83efb9cf48`. It binds provider `deepseek`, requested alias `deepseek-flash`, family `DeepSeek-V4.1-Flash`, base URL `https://api.deepseek.com`, and `/responses`. The alias remains `ALIAS_ONLY`; no immutable revision or decision eligibility is inferred.

DeepSeek uses the separately identified `DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1` setting (`high`) and JSON Schema output. Ceilings are 12,000 input tokens, 4,000 output tokens, three persisted model attempts, eight read-tool calls, and one concurrent job. The separate price schedule `DEEPSEEK_V41_FLASH_PEAK_2026_09_V1` uses official peak cache-hit input `$0.006/M`, cache-miss input `$0.30/M`, and output `$1.20/M`; reservations use cache-miss input pricing. Unknown pricing is not dispatchable and peak/off-peak scheduling is not implemented.

The adapter fixes the endpoint/model, uses zero SDK retries, at most 30 seconds, `trust_env=False`, no provider tools, and no fallback. A V2 durable authorization is written before the V2 capability is issued. The capability binds profile, provider, model, endpoint, price schedule, request/evidence/attempt and token ceilings. Worker isolation and broker ownership remain intact: `atlas-ops/controller` is the only agent-ledger writer, the broker has no `ops.sqlite`/`OpsRepository`, and the worker has no database, provider credential, or network authority. DeepSeek reasoning content is discarded at the provider boundary; only final structured output and allowed metadata can enter ATLAS, followed by the unchanged deterministic proposal validator.

The amendment adds no `SHADOW`, critic, extraction, decision/admission/TradePlan influence, execution, capital, assisted execution, or seventh V2 phase. Deterministic ops remains `DISABLED`; only zero-authority `OFFLINE_RESEARCH` is eligible. Economics is `NOT ESTIMABLE`; capital and assisted execution are disabled; the final holdout is `UNASSIGNED / UNTOUCHED`.

## Changed files

- `src/atlas/v2/agent_intelligence/contracts.py`, `profile.py`, `budget.py`, `persistence.py`, `broker.py`, `controller.py`, `provider.py`, and `__init__.py` — additive V2 profile, price schedule, dispatch authorization/capability, broker selection and fixed Responses adapter.
- `configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json` — fixed conservative DeepSeek peak schedule.
- `docs/v2/ATLAS_AGENT_PROVIDER_MODEL_AMENDMENT_DEEPSEEK_V41_V1.md` — versioned amendment.
- `tests/v2/test_session028_provider_amendment.py` — focused fake-provider, capability, privacy, ownership, limits, and SDK MockTransport tests.
- `docs/v2/SESSION028_AGENT_PROVIDER_VALIDATION.json`, `docs/v2/AGENT_PROVIDER_MODEL_AMENDMENT_GATE_V1.json`, and this handoff — docs-only validation closeout.

Session-027 deterministic runtime files were not modified. `requirements.in`, `requirements-lock.txt`, `requirements-agent.in`, and `requirements-agent-lock.txt` were not modified.

## Validation

| Check | Result |
| --- | --- |
| Session-028 focused tests | 20 passed; fake provider and pinned OpenAI-compatible SDK through `httpx2.MockTransport` |
| Session-026 agent and broker ownership | 43 passed |
| Worker isolation | 5 passed |
| Session-027 supervisor and production | 32 passed; agent disabled and action/evaluation remains provider-independent |
| Full V2 | 498 passed, 0 skipped, 0 failed; 1,708.22 seconds |
| Full non-V2/V1 | 485 passed, 3 skipped, 0 failed; 636.65 seconds |
| Serialization/wire contracts and V1 golden recomputation | 10 passed; 26.92 seconds |
| Ruff | passed (`.venv/bin/ruff check .`) |
| Mypy | passed (195 source files) |
| Compileall | passed (`src tests`) |
| Core and isolated agent `pip check` | passed; no broken requirements |
| Accepted freeze, release, validation/gate, golden, and dependency hashes | recomputed and unchanged; values are in `SESSION028_AGENT_PROVIDER_VALIDATION.json` |

Normal tests made zero real provider calls. The actual SDK conformance test used a local mock transport and produced no network request.

### One optional live provider smoke

One direct DeepSeek request was made using the protected runtime credential; no key value was printed or persisted. It used a synthetic local research request, no market/account/private/holdout data, no tools, zero SDK retries, and a 512-token output ceiling. The requested and returned model IDs were both `deepseek-flash`; usage was 1,929 input and 512 output tokens. The response was truncated at the ceiling, so it did not produce a valid ATLAS proposal and did not pass the smoke criteria. No second request or fallback was made. The direct endpoint gate remains `BLOCKED BY ENVIRONMENT` for this checkpoint. This is engineering evidence only and carries no economic weight.

NVIDIA NIM was not called and remains `UNVERIFIED / TEST GATE`; it is not a fallback. No public-market or venue request, authenticated venue call, or GitHub Actions run was made or claimed. The ordinary test runs did not use network transport.

## Status and remaining gates

- DeepSeek offline adapter and amendment: `IMPLEMENTED / TESTED`.
- OpenAI/Astra V1 profile, pricing, wire history and Session-026 gate: preserved.
- DeepSeek direct endpoint smoke: `BLOCKED BY ENVIRONMENT` after the single permitted smoke truncated; no second call in this session.
- NVIDIA NIM compatibility: `UNVERIFIED / TEST GATE`.
- Live agent shadow and decision influence: `TEST GATE`.
- Economics: `NOT ESTIMABLE`.
- Capital and assisted execution: disabled.
- Final holdout: `UNASSIGNED / UNTOUCHED`.
- Independent review of the pushed branch remains outstanding.

Pre-existing untracked files `ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md`, `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md`, and `atlas-session005.zip` were left untouched and unstaged.
