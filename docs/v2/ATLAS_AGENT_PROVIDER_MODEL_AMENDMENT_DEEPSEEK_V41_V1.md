# ATLAS Agent Provider/Model Amendment — DeepSeek V4.1 Flash V1

**Amendment identity:** `ATLAS_AGENT_PROVIDER_MODEL_AMENDMENT_DEEPSEEK_V41_V1`

**Applies to:** the existing zero-authority `ResearchProposalProvider` path only

**Provider profile:** `AgentModelProfileV2` / `DEEPSEEK_V41_FLASH_PEAK_2026_09_V1`

## Scope and authority

This is additive provider/model support only. The accepted OpenAI `gpt-6-astra` Responses API agent freeze and Session-026 history remain unchanged, including `AgentModelProfileV1`, its wire records, `OPENAI_API_KEY` ownership, the Session-026 validation/gate and the agent freeze document. Existing provider-neutral ATLAS ports remain authoritative: `ResearchProposalProvider`, `ActionAssessmentProvider` and `EventExtractionProvider`.

DeepSeek receives zero decision or capital authority. Only `OFFLINE_RESEARCH` is eligible. The agent remains `DISABLED` unless that bounded mode is explicitly configured. This amendment does not implement `SHADOW`, an action critic, event extraction, decision influence, admission influence, TradePlan influence, execution, capital or assisted execution. It does not create a seventh V2 phase.

Alias-only model identity is insufficient for decision influence. The requested API model is exactly `deepseek-flash`; its represented family is `DeepSeek-V4.1-Flash`. The alias is not an immutable revision, so the initial profile status is `ALIAS_ONLY`. No immutable revision is inferred from the family name.

There is no provider or model fallback. A DeepSeek timeout, refusal, rate limit, API failure or model drift returns the corresponding explicit unavailable/invalid state. It never selects OpenAI, NVIDIA NIM or another model. Any later fallback policy requires another reviewed amendment.

## Fixed provider binding

- Provider: `deepseek`.
- Requested model: `deepseek-flash` only.
- Base URL: `https://api.deepseek.com`.
- Responses endpoint: `/responses`.
- API: OpenAI-compatible Responses API with JSON Schema structured output.
- Reasoning identity: `DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1`, setting `high`.
- No caller-selected model or endpoint; no hosted web/code/file tools, shell, MCP, browser, venue or capital tools.
- SDK automatic retries: zero. Timeout: at most 30 seconds. `trust_env=False`.

The DeepSeek price schedule is provider-neutral and versioned separately from the OpenAI cache-write schedule. It uses conservative peak prices and cache-miss input pricing for reservation:

| Billing class | USD per 1M tokens |
| --- | ---: |
| Cache-hit input | 0.006 |
| Cache-miss input (reservation rate) | 0.30 |
| Output | 1.20 |

The maximum configured request costs `$0.008400` for 12,000 input and 4,000 output tokens; three such calls reserve at most `$0.025200` per job. Unknown pricing is not dispatchable. Automatic peak/off-peak scheduling is not implemented.

Initial ceilings are maximum input tokens `12,000`, maximum output tokens `4,000`, three persisted model attempts per job, eight bounded read-tool calls and one concurrent job. They are ceilings, not targets. Provider credentials remain broker-only. The DeepSeek credential is read only from `DEEPSEEK_API_KEY` by the configured inference broker. The worker receives no provider secret; secrets are not accepted in a command line, job payload, prompt, capability, SQLite record or log.

Dispatch authorization is durably written before its signed capability is created. The capability cryptographically binds the immutable profile hash, provider-binding hash, endpoint/model identity, price-schedule hash, request/evidence/attempt identity and token limits. OpenAI V1 capabilities and DeepSeek V2 capabilities are disjoint; cross-provider and cross-profile replay fail closed.

## Output and privacy boundary

Provider reasoning/chain-of-thought is never persisted. If returned, reasoning items are discarded inside the provider adapter. Only final structured output, requested and returned model IDs, any separately supported revision evidence, usage counts, provider request identity, refusal/truncation/failure status and already-authorized sanitized transport metadata may cross into ATLAS. Reasoning text cannot enter `ProviderResultV1`, attempt persistence, proposal persistence, validation receipts or logs. ATLAS runs the existing deterministic proposal validator after receipt; provider schema compliance does not replace validation.

## Evidence limits and gates

A real provider smoke is optional engineering evidence, not economic evidence. Normal development and tests make zero real provider calls. The initial alias remains ineligible for decision influence even if a real smoke succeeds. DeepSeek direct endpoint smoke status is `BLOCKED BY ENVIRONMENT` unless the separately allowed single call is actually run and passes. No NIM adapter is implemented here; if a separately reviewed NIM path is added later, its provider failure must also map to explicit unavailability and must never switch providers.

NVIDIA NIM remains `UNVERIFIED / TEST GATE`; it is not a fallback and is not included in this adapter. Any future NIM path must first satisfy a separately reviewed current API/structured-output contract. Economics remains `NOT ESTIMABLE`; live agent shadow and decision influence remain `TEST GATE`; capital and assisted execution remain disabled; the final holdout remains `UNASSIGNED / UNTOUCHED`.

## Official provider references

- [DeepSeek Models & Pricing](https://api-docs.deepseek.com/quick_start/pricing/) — model alias/family, endpoint base URL, peak rates and cache rates.
- [DeepSeek Responses API reference](https://api-docs.deepseek.com/api/create-response/) — `/responses`, model IDs and JSON Schema output format.
- [DeepSeek Responses API guide](https://api-docs.deepseek.com/guides/responses_api/) — Responses compatibility and reasoning/output fields.
- [DeepSeek Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode/) — native reasoning effort settings.
