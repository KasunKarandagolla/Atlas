# ATLAS — Agent Intelligence Extension Freeze V1

**Freeze ID:** `ATLAS_AGENT_INTELLIGENCE_EXTENSION_V1`
**Date:** 28 September 2026
**Status:** APPROVED FOR BOUNDED IMPLEMENTATION
**Authority:** additive post-Phase-5 extension; does not reopen or renumber the six frozen V2 phases.
**Accepted implementation base:** `impl/session-025-v2-release-engineering-shadow-closure @ 21055dcb5496665aaf165bde1e4f7e73db773192`
**Existing release:** Session-025 `SHADOW_RELEASED`; capital and assisted mode remain disabled.
**Economic status:** `NOT ESTIMABLE`.

## 1. Design decision

The Astra consultation is accepted with amendments.

The LLM is a bounded semantic intelligence component, not a trading-authority component.

Initial deployment order:
1. offline Discovery-Lab research proposer;
2. hidden zero-authority action critic after the exact action and all numerical evaluation artifacts are complete;
3. separate tool-free S7 event extraction task;
4. only after prospective qualification, a narrow deterministic restrictive mapper may consume selected typed findings before admission.

The LLM never owns candidate selection, hard-risk sizing, quantity, stop, leverage, approval, order placement, cancellation, position closing, protection, reconciliation, UNKNOWN resolution, reservation release or RiskPolicy.

## 2. Frozen role

### Offline research proposer — first implementation
The agent may propose falsifiable hypotheses, failure slices, deterministic rule variants and bounded experiments to the existing Discovery Lab.

It is a proposal producer only. It cannot preregister its own experiment, execute generated code, consume protected holdouts outside existing permissions, start unrestricted backtests, promote policy, write a live CandidateSet or affect capital.

### Action critic — later hidden shadow
Insertion point is frozen at Position 6:

`frozen exact action -> M0/M1/analogue/scenario/uncertainty/stress/ES -> sealed assessment packet -> hidden agent critic`

Baseline deterministic admission continues unchanged while the critic is shadow-only.

### Event extraction — separate task
S7 event extraction is a separate tool-free structured inference profile. Existing deterministic event blackouts and source-health rules remain authoritative.

## 3. Pipeline authority

Frozen live pipeline:

`evidence -> causal features -> S1-S8 -> CandidateSet -> deterministic selector -> hard-risk sizing -> frozen action -> M0/M1/analogue/scenarios -> deterministic admission -> immutable TradePlan -> exact human approval -> crypto-live -> NautilusTrader -> qualified venue`

Agent paths are side branches. Initial agent output has zero decision authority.

A later decision-affecting version may only enter through a separately promoted deterministic restrictive mapper. For the same frozen action it may remove eligibility but can never restore an action rejected by an existing gate, resize it, change stop/leverage/direction or trigger same-slot alternate-candidate shopping.

## 4. Framework decision

ATLAS does not adopt a generic agent framework as a core dependency or architectural owner.

Freeze ATLAS task-specific ports:
- `ResearchProposalProvider`
- `ActionAssessmentProvider`
- `EventExtractionProvider`

Framework/provider objects must not enter ATLAS strategy, risk, TradePlan, approval, execution or persistence contracts.

For the offline tool-using `ResearchProposalProvider`, the preferred first adapter is a pinned minimal `pydantic-ai-slim[openai]` installation, subject to dependency/security/conformance tests.

Do not enable Pydantic AI Harness, shell/code execution, browser/web search, MCP, dynamic tool registration, subagents, multi-agent orchestration, long-term conversational memory, provider fallback or durable graph/workflow platforms.

`ActionAssessmentProvider` and `EventExtractionProvider` should use a direct structured model request through the ATLAS inference broker unless measured requirements later prove an agent loop is necessary.

## 5. Model/provider decision

Initial quality-first model: OpenAI `gpt-6-astra`, Responses API, fixed medium reasoning configuration.

For zero-authority offline research, an alias is permitted if requested/returned model identifiers, provider metadata, prompt/schema/settings hashes and usage are persisted.

Decision influence is prohibited until ATLAS can bind an enforceable model revision/snapshot or equivalent fixed-version commitment and qualify that version.

No automatic provider/model fallback.

## 6. Process isolation

Add a separate `atlas-agent-worker`.

It has no exchange credentials, live-control DB access, approval/reservation capability, writable RiskPolicy, order/protection/recovery API, arbitrary SQL, arbitrary filesystem access or arbitrary internet access.

Provider authentication belongs to a small ATLAS inference broker, not to the worker. The worker receives a short-lived job-scoped capability and sealed packet/read scope.

Execution/protection/recovery must start and operate with all agent infrastructure absent.

## 7. Repository mapping

Existing accepted seams are reused but not overloaded:
- `src/atlas/v2/models/worker_protocol.py` supplies isolation and typed-worker patterns, but its forecast contracts remain unchanged.
- `src/atlas/v2/science/discovery.py` already recognizes `OFFLINE_LLM_ADAPTER`; integrate through additive proposal contracts.
- `OpsRepository` remains the non-capital persistence owner.
- Existing `ops_outbox` is watch-transition-specific and must not be repurposed as a generic agent queue.

Create a namespaced additive agent ledger in `ops.sqlite`.

Do not modify V1 live-control schema or Session-024 capital-authority contracts.

## 8. Agent ledger

Add immutable/versioned records for at least:
- `AgentJobV1`
- `AgentAttemptV1`
- `ResearchProposalRequestV1`
- `ResearchProposalV1`
- `ActionAssessmentRequestV1`
- `AgentAssessmentV1`
- `AgentValidationReceiptV1`
- `EventExtractionRequestV1`
- `EventExtractionV1`

Initial lifecycle:
`QUEUED -> LEASED -> RUNNING -> RESULT_RECEIVED -> VALIDATED`
or terminal:
`UNAVAILABLE | INVALID | EXPIRED | CANCELLED`

Late results are retained but never retroactively decision-eligible. Duplicate delivery is expected; at most one accepted result exists for one immutable request identity. Persist request before any billable provider effect.

## 9. Causality and request binding

Every action assessment binds CandidateSet, selected candidate, selector policy, frozen action, M0, eligible M1, analogue support, pretrade scenarios, uncertainty/OOD/support, stress/ES, source-health/missingness, market cutoff `T0`, sealed assessment cutoff `T`, and unextendable deadline `D`.

Underlying market/news evidence must be available by `T0`. Derived evaluation artifacts exposed to the critic must be available by `T`. Inference cannot reset `D`.

Historical inference generated later is marked retrospective and never backdated as contemporaneous evidence.

## 10. Output semantics

The model produces untrusted structured payloads only. Trusted host metadata supplies identities, model/runtime versions, prompt/schema/tool hashes, exact evidence refs, timestamps, deadlines, usage and content hashes.

Initial closed finding taxonomy is shadow-only:
- `EVENT_ENTITY_AMBIGUOUS`
- `EVENT_TIME_AMBIGUOUS`
- `SOURCE_CLAIMS_CONFLICT`
- `SOURCE_ASSERTION_UNSUPPORTED`
- `ARTIFACT_SEMANTIC_MISMATCH`
- `REQUIRED_CONTEXT_UNAVAILABLE`

No generic confidence, conviction, trade-quality score or probability of profit.

A deterministic validator independently checks schema, binding, lineage, time, references, spans, units, missingness and eligibility.

## 11. Tools

First live action critic: zero dynamic tools.
First event extractor: zero dynamic tools.

Offline research proposer may receive only manifest-scoped, read-only bounded tools, initially:
- read exact registered artifact by allowed ref;
- inspect prior failed variants for the authorized discovery family;
- inspect authorized ablation/evaluation summaries;
- inspect registered policy/model manifest.

No URL, SQL, path, shell, Python/code executor, web browser, MCP, venue API or capital operation.

## 12. Budget and failure behavior

Initial offline ceiling:
- max 3 model calls including retries;
- max 8 read-tool calls;
- absolute wall-clock deadline;
- explicit token/output limits;
- explicit per-job and daily USD caps;
- one concurrent job initially.

Exact numerical caps are versioned configuration and must be checked against current provider pricing before use.

Unknown price or unbounded payload => no provider dispatch.

Initial authority modes implemented in first checkpoint:
- `DISABLED`
- `OFFLINE_RESEARCH`

`SHADOW` is implemented only in a later reviewed integration. `DISPLAY_ONLY` and `REQUIRED_RESTRICTIVE` are not enabled by the first implementation.

## 13. Security

Treat retrieved documents and model output as untrusted data. No content can modify system policy, tools, model routing, scope or authorization.

No hosted provider tools. No unsolicited external tracing. Audit storage is ATLAS-owned and sanitized. Provider key and job capability never enter prompts/tool results.

## 14. Evaluation and promotion

Baseline `A0` remains deterministic ATLAS without agent influence.

Historical LLM evaluation is diagnostic because pretrained models may contain later knowledge. Prospective frozen-policy evidence is primary.

No decision influence before engineering, research-usefulness, hidden-shadow, leakage-control, signed qualification, prospective after-cost value, operational/latency/error/coverage, fixed-model-version, and governance gates all pass.

The existing ATLAS economic promotion ladder remains authoritative.

## 15. Relationship to Session 025

Session 025 remains accepted and immutable as the baseline `SHADOW_RELEASED` release.

This work is a post-freeze additive extension, not V2 Phase 6 and not a rewrite of Phase 0–5.

First implementation checkpoint is offline-only. A later hidden live-shadow checkpoint is authorized only after independent review.

Because the accepted release does not ship a general continuously running `atlas-ops` daemon, live-shadow deployment must first prove or add a bounded non-capital ops runtime/supervisor. Do not hide that missing operational seam inside the LLM framework.

## 16. First implementation exit gate

Pass only when:
- dependency absent or mode `DISABLED` leaves accepted baseline artifacts/outputs unchanged;
- agent dependency is isolated from crypto-live;
- agent tables are namespaced and migration/reopen safe;
- request identities are immutable/replayable;
- no future/holdout leakage is possible through the read surface;
- provider/tool/retry/deadline/budget failures are explicit;
- fake-provider adversarial suite passes;
- optional real-provider contract smoke uses no trading credentials and makes no economic claim;
- Discovery proposals are quarantined append-only artifacts with no self-promotion path;
- full V1/V2 regression and V1 golden remain unchanged;
- secret/dependency checks pass;
- economics remain `NOT ESTIMABLE`;
- capital and assisted modes remain disabled.

No live action critic, approval-screen content, admission hook or capital effect is part of the first implementation.
