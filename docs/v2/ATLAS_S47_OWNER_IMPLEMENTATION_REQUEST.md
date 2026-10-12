# ATLAS RED — S47 GPT-6.1 Sol Implementation Session

**Mission:** Continue the single owner-finalized Atlas target from the independently accepted S46 implementation. Deliver surgical, tested progress on quantitative economics, a compact Decision Brief, bounded agent analysis tools, provider portability and durable event-driven analysis memory. Reuse the existing repository. Do not build another MVP, rewrite frozen systems or declare unqualified economic value.

**Assigned model: GPT-6.1 Sol ONLY**, including every subagent. Do not substitute another model.

**Accepted S46 implementation base:** `95efa393e50f5e71646c409a3b4ff83cf5acb6a2` on `impl/session-046-causal-features-peer-context`. Rediscover the actual newest GitHub remote SHA before starting.

Repository: `https://github.com/KasunKarandagolla/Atlas`

Owner reference sources: `/home/kasun/Music/atlas/Red` — READ ONLY.

Original laptop checkout: `/home/kasun/Music/atlas` — preserve all dirty/untracked files.

Suggested isolated S47 worktree: `/home/kasun/Music/atlas-s47`.

## 0. MANDATORY FIRST: MODEL AND CODEX TOOL CHECK

Before implementation, confirm that this Codex session is running **GPT-6.1 Sol**. Verify model selection explicitly. Do not silently use Instant, Luna, Astra or another model.

If using subagents, demonstrate that every subagent inherits **the same GPT-6.1 Sol model**. If inheritance cannot be verified, use no subagents. Do not let this consume substantial setup time.

Next, inspect the actual installed Codex MCP/plugin inventory:

```bash
codex --version
codex mcp list
```

Also inspect Codex's `/mcp` interface and available plugin listing if the installed CLI supports it.

Check these tools separately:

1. **Serena MCP** — preferred for symbols, call references, local code navigation and bounded edits.
2. **Context7 plugin/MCP** — preferred for exact pinned third-party library documentation.
3. **Repomix** — OPTIONAL, only if a small targeted inventory cannot be efficiently obtained with Serena, `rg` and `git`.

**If Serena or Context7 is missing or not operational:**

- First tell the owner precisely what is missing.
- Ask the owner to authorize installation/configuration.
- Do not silently install global tooling, change authentication settings or expose credentials.
- After authorization, perform/guide installation, confirm functionality and **continue this SAME S47 implementation session and worktree**.
- Context7 may require a fresh Codex thread to load newly installed tools. If so, preserve the S47 branch, model requirement, checked authorities and handoff; this is still S47, not S48.
- If installation fails or the owner declines, explain the obstacle and request permission to use ordinary repository tools instead. Do not substitute a large unreviewed tool stack.

Official installation references are also in `04_ATLAS_ENGINEERING_EXECUTION_RUNBOOK.md`, Section 6.

**Serena, if absent**, subject to owner permission and current official instructions:

```bash
uv tool install -p 3.13 serena-agent
serena init
serena setup codex
```

Confirm Serena is visible and connected in Codex. Activate `/home/kasun/Music/atlas-s47`, not the original checkout or `Red`.

**Context7, if absent**, subject to owner permission and installed Codex plugin support:

```bash
codex plugin marketplace add upstash/context7
codex plugin add context7@context7-marketplace
```

Follow the browser OAuth flow without putting secrets in commands or source files. Restart the Codex thread if required and verify the tools work.

If the Codex plugin surface is unavailable, use the official Context7 Codex setup documentation to choose a supported MCP registration method. Inspect any resulting configuration changes before approval.

**Do not install Repomix by default.** If genuinely necessary, obtain permission and sandbox it to the **S47 worktree only**, excluding `.env`, keys, runtime/private databases and generated sensitive artifacts.

### How to use the tools efficiently

Use Serena for symbol discovery, reference analysis and small edits, rather than repeatedly reading entire 2,000–4,000-line modules.

Use Context7 only when exact installed API/version behavior is relevant, such as the project's pinned inference, statistics, persistence or modeling libraries. Never use library documentation as a substitute for inspecting actual repository code.

Prefer small excerpts, targeted file ranges and focused diffs. Avoid enormous context dumps, repeated repository packaging and broad MCP searches.

Record which tools were verified and whether they materially reduced repeated reads or errors. **Do not claim measured token savings unless token usage is actually available.**

These tools belong to **Codex development**, not ATLAS's trading-agent permissions.

## 1. AUTHORITATIVE PREFLIGHT

Read, in full, the three immutable freeze documents and verify exact SHA-256:

1. `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`
   `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`

2. `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md`
   `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`

3. `ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md`
   `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`

Stop on any hash mismatch. Never edit or replace these historical authorities.

Then read:

- Repository `AGENTS.md`, current `README.md`, `docs/INDEX.md`, root `log.md`, append-only `translog.md`.
- `Red/00_START_HERE.md`.
- `Red/ATLAS_FINAL_TARGET_DESIGN_LOCK_2026-10-12.md`.
- `Red/ATLAS_TRANSITION_ARCHITECTURE_DESIGN_V1.md`.
- `Red/ATLAS_OWNER_DECISIONS_AGENT_AUTHORITY_DISCOVERY_ALTCOINS_2026-10-12.md`.
- Relevant ADRs: **ADR-002, ADR-005, ADR-006, ADR-007 and ADR-008**; consult ADR-009/010 when relevant.
- `Red/04_ATLAS_ENGINEERING_EXECUTION_RUNBOOK.md` Sections **2, 3, 4, 5, 6 and 7**.
- Latest S46 handoff and `docs/v2/transition-s46-evidence/evidence_manifest.json`.
- Actual GitHub code, tests and policies at the latest accepted implementation SHA.

The owner has finalized the target design. **Do not reopen general architecture debates** or substitute a different trading product.

The finalized target does **not** amend the three freezes.

### Git and environment

Discover all current remote candidate branches, ancestor relationships, newest pushed commits and actual uncommitted changes.

Start from the newest independently accepted descendant of the S46 checkpoint, unless an explicit owner/reviewer-approved newer base exists.

Create `impl/session-047-economics-agent-decision-brief` in a separate worktree. Preserve the original dirty checkout untouched.

Confirm actual Python environment, project dependencies, source layout and existing test commands. Never assume documentation proves that a module works.

No real trading order, private venue endpoint, authenticated market operation, paid inference, paid chart service or capital action is authorized.

## 2. FIRST CODE TASK: SMALL S46 CARRYOVER REPAIRS

Before building S47 consumers, examine:

**A. Peer context dependency chronology.** Review `PeerRelationshipRequestV1` in `src/atlas/v2/features/peer_context.py`. Independently check whether the S6 state can be unavailable at `computed_at_ns` while a peer result is nevertheless constructed as though available. Write an adversarial red test and, if confirmed, make the smallest fail-closed correction. Preserve cache/policy identities or add a correctly versioned change if output identity must change.

**B. Event-saturation test gate.** The combined 35-test calendar/event suite failed twice: expected `BROKER_SATURATED`, received `CALL_OUTCOME_UNKNOWN`. Isolated event tests passed. Investigate with bounded repeatable tests to distinguish test scheduling, transport ambiguity and genuine incorrect classification. Preserve the fail-closed behavior. Do not weaken the assertion or treat a timeout as a broker rejection without proof. If no reliable fix is established, retain `TEST GATE` and its evidence.

**C. Sparse technical features.** `TECHNICAL_V1` accepts sparse finalized bars without a gap label. Do not silently change frozen indicator calculations; exclude or explicitly label unqualified gap-sensitive outputs in new Decision Brief evidence.

After each modification, run focused tests and direct consumers before touching the next module.

## 3. VERSIONED AMENDMENT GATE — DO NOT BYPASS

The frozen current pipeline is:

`CandidateSet → deterministic selector → hard-risk sizing → exact frozen action → M0/M1/scenarios/ES → deterministic admission → immutable plan`.

The current agent critic remains zero-authority, post-evaluation and tool-free.

The future target is a DeepSeek-preferred **final discretionary** decision among economically supported eligible choices, with ATLAS retaining independent risk and execution control.

These are **not the same contract**.

Before implementing decision-affecting behavior:

1. Discover whether independently accepted **versioned amendments** already exist for the specific changed authority.
2. Identify precise changed contracts and old policy identities to preserve.
3. If amendments are absent, produce concise proposed amendment artifacts specifying:
   - separate multi-candidate comparative research/selection arm;
   - shortlist formation and complete A0 comparison calendar;
   - position of risk sizing, exact-action freezing and per-action economics;
   - discretionary agent `SELECT_ONE / REJECT_ALL / ABSTAIN` authority;
   - model/provider, tool, budget and deadline permissions;
   - immutable evidence and rollout/rollback identity;
   - explicit retention of the old deterministic selector and hidden critic.
4. Mark these `PROPOSED`, not `ACCEPTED`.
5. Request owner approval and independent review before crossing those authority boundaries.

**Do not self-approve an amendment.**

Until an amendment is accepted, implement only non-decision-affecting read-only data contracts, sidecar research, bounded fake-provider evaluation and isolated shadow experiments. This work remains valuable and is not a separate MVP.

If approval arrives during S47, continue the permitted implementation in the **same S47 session**. If not, finish/push the tested unprivileged scope and identify exactly what remains blocked.

Do not implement agent preselection through a hidden change to the frozen selector.

## 4. ECONOMICS AND SMALL ADAPTIVE SHORTLIST

Reuse existing causal CandidateSet, selection audits, strategy geometry, M0/M1, analogue, scenario, uncertainty, stress/ES and whole-calendar evidence.

Do not write a second generic valuation engine.

Build a bounded **read-only comparative research view**:

- Discover cheap Python-screened candidates before any model request.
- Distinguish candidate screening from full exact-action after-cost evaluation.
- Represent zero, one or several supported candidates with **no arbitrary fixed shortlist size**.
- Preserve all screened, excluded, rejected, expired, unsupported and no-candidate slots.
- Keep the existing deterministic A0 baseline intact.
- Preserve original cutoffs, source availability, candidate identities, model versions, prior eligible mature labels and execution assumptions.
- Never compare a preliminary score to an actual risk-sized after-cost evaluation as though they were the same quantity.
- For a separately amended multi-action shadow arm, independently risk-size and freeze each eligible action before its own economics; do not turn alternative evaluation into same-slot rescue shopping.
- Respect portfolio/multiplicity dependence, liquidity, fees, funding, fill/no-fill, scenario, OOD and ES.
- Unknown cost, unqualified source, insufficient support or a missing exact action produces explicit `NOT_ESTIMABLE`, never an invented expected return.

Reuse M0 and M1 if eligible. Optional pretrained ML is allowed only with actual installed model/checkpoint evidence, compatible forecast target/horizon, causal input cutoff and validated calibration/support.

No unsupported model scores or invented probabilities of profit.

## 5. MANDATORY COMPACT DECISION BRIEF

Implement a versioned, typed, bounded **Decision Brief** builder as a research/shadow read model, independent of today's frozen decision path.

It should assemble **before** any future billable model request:

- immutable decision-slot/request ID, cutoff, deadline, policy/schema hashes, universe and product revisions;
- adaptive shortlist with eligibility, actual action/evaluation status and explicit missingness;
- compact timely market facts: completed candles, trend/momentum/structure, spread/liquidity, relevant derivatives and source health;
- BTC/ETH context plus optional causal S46 peer relationship, with exact provenance and no implied veto;
- M0, eligible M1/analogue, after-cost estimates, cost/fill assumptions, uncertainty, scenarios and ES for truly assessed exact actions;
- qualified optional pretrained ML with model identity, target, horizon and limitations;
- relevant prior analysis, watches, invalidators, due-work, news/event and outcome references;
- optional provenance-backed snapshot reference, not an invented chart or continuously generated image;
- tool allowlist, budget status, original deadline and permissible response options.

Everything must be causal at the bound cutoff.

Set hard row, byte, instrument, image-reference and token-projection limits **through explicit versioned configuration**. Do not invent owner deployment budgets or quietly truncate mandatory risk/evidence fields.

When a brief cannot fit safely, emit an explicit incomplete/blocked result rather than silently dropping contradictions.

Make brief identities and content hashes reproducible. Add negative tests for future-tail injections, missing costs, stale peer/context, incorrect ML horizon, revised product identity, missing mature labels, deadline expiry and full-calendar preservation.

## 6. PROVIDER-NEUTRAL AGENT RESEARCH CONTRACT

Preferred future provider: **DeepSeek**. Preserve the current frozen OpenAI agent provider and critic behavior unchanged.

Implement only a separate, inert-until-authorized adapter boundary for the new decision role.

Create a per-provider/model capability record for:

- structured output support;
- bounded tool-call support;
- returned model/version identity;
- deadline/cancellation behavior;
- token accounting and pricing availability;
- context limits;
- image support, if any;
- rate/retention constraints;
- explicit no-fallback behavior.

Do not assume OpenAI-compatible APIs implement identical features.

Use **fake local providers only** in S47. No real API key, paid request, external model login or provider pricing assumption.

Test:

- `SELECT_ONE` with an authorized candidate;
- `REJECT_ALL`;
- `ABSTAIN`;
- invented candidate/action ID;
- unsupported tool request;
- malformed result;
- late response;
- stale snapshot;
- provider failure;
- unknown price;
- unverified capability;
- budget exhausted;
- restart or duplicate response;
- prompt injection within retrieved source text.

An agent may not change quantity, leverage, direction, stop, RiskPolicy, execution instructions or venue protection.

Current frozen critic and event extractor retain **zero dynamic tools**.

Until a separately reviewed policy becomes effective, all new discretionary decisions remain **shadow-only, disconnected from admission and capital**.

## 7. BOUNDED AGENT-INITIATED ANALYSIS TOOLS

Build a minimal typed read-only gateway only to support the new proposed/research role.

Prefer **one compact model job and at most one bounded supplementary tool-result round** in the initial tested configuration. Further rounds require independently reviewed need, budget and permission.

Prioritize reuse of existing facts and modules:

1. Inspect approved completed-bar / feature / BTC-ETH-peer snapshot.
2. Inspect already computed exact-action economics and model evidence within the authorized role.
3. Inspect registered events, memory and previously completed analysis if materially useful.

Expand tool breadth only when an actual missing-information test justifies it. Do not implement the entire ADR-006 aspirational catalog immediately.

Every request must bind:

`job_id, snapshot_ref, candidate_scope, cutoff_ns, deadline_ns, allowed_tool_name, tool_version, validated_parameters, resource_budget`.

Every receipt must carry status, content hash, input refs, actual availability, latency/bytes and explicit missingness.

Never expose arbitrary Python, shell, SQL, path access, provider/browser tools, exchange endpoints or private account objects to the model.

Tool calls never authorize model dispatch; the deterministic scheduler decides whether another call is worth the budget.

An agent news idea can only propose a **bounded new cheap screening request** with future slot identity; it cannot insert a new trade into an already sealed slot.

## 8. DURABLE ANALYSIS MEMORY AND SCHEDULING

Before adding a database table, map the existing:

- `watch` and watch transitions;
- watch-specific `ops_outbox`;
- agent job/attempt ledger;
- S45 repaired `due_work`;
- outcome maturity and durable public evidence index;
- existing calendar, analysis/history and restart consumers.

Reuse established contracts wherever possible. Do not repurpose the watch outbox as a generic LLM queue or duplicate the S45 due-work implementation.

Add only the minimum versioned memory entities or services required for:

- bounded prior-analysis reuse by exact source/policy/model/cutoff identity;
- source/position/watch invalidation;
- event-driven research due work;
- deduplicated model-eligible jobs;
- durable `LLM_NOT_CALLED`, `BUDGET_DENIED`, `MEMO_REUSED`, `EXPIRED`, `UNKNOWN` and failure status;
- crash/restart and late-result handling;
- preserving original calendar denominators and evidence references.

All mutations belong to the existing authorized ops writer. No model may mutate the ledger directly.

Compaction/expiry can target **proved disposable caches only**. Preserve unresolved watches, protection/UNKNOWN records, earlier mature labels, original source receipts, current audits, V1 golden data and immutable decision evidence.

Avoid a generic vector database, unrestricted transcript archive, new scheduling framework or raw market-data rewrite.

Test duplicate triggers, retry-after-unknown, crash before/after persistence, future available-at, expired job, changed snapshot, source gap, stale memo, double-paid-dispatch prevention and replay stability.

## 9. TEST AND INTEGRATION DISCIPLINE

For each independently changed module:

1. Write or select a failing test that demonstrates the missing behavior.
2. Implement the smallest compatible patch.
3. Run focused red→green tests.
4. Run immediate callers/consumers.
5. Record actual results and only then proceed.

At checkpoints and session end, run impacted suites covering:

- S46 peer context and S6;
- causal features, S1/S2 and selection;
- M0/M1 and economic/admission/scenario consumers;
- agent broker/worker, tool and event isolation;
- watch/outbox, S45 due-work, maturity, restart and idempotence;
- official calendar and event transport, including the retained saturation failure;
- durable capture and chronology;
- V1 golden and recovery;
- S44 close-driven M15 regression.

Test future-tail/prefix equivalence, source revision, unavailable data, bounded results, missing/late provider, budget denial and failure/restart.

Run Ruff, `compileall`, baseline-aware mypy, `pip check`, lock/license review, freeze hashes, `git diff --check`, secret scan and staged-diff review.

Do not use paid/live endpoints, loosen assertions, hide skips, inflate queue capacity, or describe synthetic tests as actual venue/economic qualification.

If full-suite execution is disproportionately expensive, run all impacted tests and record precisely what remained unrun.

Measure CPU/RSS/latency/disk/network/tokens only when instruments actually capture them. Do not fabricate measurements.

## 10. EXECUTION COORDINATION

Keep coordinator and all verified subagents on GPT-6.1 Sol only.

Subagents, if used, get disjoint code paths and independent temporary worktrees. No simultaneous conflicting SQLite/migration ownership. Limit parallel heavy work on the modest laptop.

The coordinator is sole writer of root `log.md`, `translog.md` and final handoff.

Prefer surgical modifications to existing modules or small additive components. Do not mass rename packages, rewrite the runtime, introduce a new orchestration framework or create a separate competing product.

Preserve original frozen contracts, V1 Bybit recovery/protection comparator, S45 outcome content hashing and S46 peer behavior.

## 11. REQUIRED PUSHED DELIVERABLES

Produce:

1. A meaningful S47 implementation commit containing only validated changes.
2. Any separately proposed versioned amendment documents, clearly marked `PROPOSED` unless independently accepted.
3. `docs/v2/ATLAS_S47_IMPLEMENTATION_HANDOFF.md`.
4. `docs/v2/transition-s47-evidence/evidence_manifest.json`, listing exact retained passing **and failing** reports with SHA-256.
5. Updated current routing in existing `README.md` and `docs/INDEX.md` where necessary.
6. New concise top checkpoint in root `log.md` without deleting earlier history.
7. Append-only S47 record in `translog.md`.

The handoff must identify:

- actual base and remote final SHAs, ancestry, changed files and frozen hashes;
- model identity and whether same-model subagents were used;
- installed/verified Serena and Context7 status and which were actually useful;
- implementation versus proposed-only contracts;
- accepted amendment IDs, if any, or explicit missing-authority gate;
- evidence of maintained A0/hidden-critic isolation;
- exact tests and failures, skips, lint/type/package checks;
- chronology, missingness, budget, restart and failure cases;
- no-new-dependency or exact dependency/license changes;
- CPU/RSS/latency/provider-token values **only if measured**;
- retained economic/capital/venue blockers;
- exact rollback and S48 dependency boundaries.

Commit and **PUSH** to the S47 branch. Verify the exact remote head using `git ls-remote`; do not rely on a successful local commit alone.

Do **not** merge. Do **not** automatically launch S48. Do not self-accept.

Return the final SHA and the concise evidence summary for independent ChatGPT review.

## 12. NON-NEGOTIABLE STOP CONDITIONS

Stop dependent changes on:

- freeze hash or identity mismatch;
- unapproved agent decision-authority change;
- incorrect causal chronology or evidence loss;
- incorrect or missing whole-calendar denominator;
- UNKNOWN promoted into success;
- duplicate paid-effect risk after uncertain dispatch;
- persistence/restart corruption;
- risk, protection or capital-control regression;
- unexpected live/private/paid endpoint activity.

Retain the known unqualified gates rather than silently repairing them with assumptions:

- fake Bybit chronology/supervisor fixture;
- TQ-01 sustained drain;
- Linux F1/A2 genuine capacity;
- real Binance final-M15 close+5s;
- native authenticated Binance protection and capital;
- sparse-bar feature eligibility;
- incremental prospective economic value: **NOT ESTIMABLE**.

Minimum prospective evidence remains at least **8 genuine weeks and 200 matured opportunities**, with appropriate regime coverage and dependence-aware inference. This is a floor, not an automatic promotion.

**Final instruction:** Perform real scoped S47 code and tests, not another architecture-only report. Make the missing plugin setup the first owner-interaction gate if necessary; then continue within the same S47 implementation. Build every nonconflicting useful seam, but do not cross an unapproved frozen-authority boundary. Preserve the single finished-product target and return an independently reviewable pushed implementation.
