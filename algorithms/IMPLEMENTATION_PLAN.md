# ATLAS - Complete Implementation Plan

**16 October 2026 | One complete system | Six engineering sessions plus a later evidence decision**

Read [the algorithm](ATLAS_SYSTEM_DESIGN.md), [acceptance register](REQUIREMENTS_AND_ACCEPTANCE.md) and [copy-ready session prompts](CODEX_SESSION_PROMPTS.md) together. The PDF is the algorithm's reading edition. The editable algorithm and the PDF must agree.

## 1. Scope, baseline and completion rule

Implement the complete finalized system by extending R9. Do not replace its OMS, execution journal, persistence ownership, numerical stack, research surface or UI. Do not create a weaker MVP or move a core requirement into a future product version. All selected model runners, strategy research routes, early-exit research/permission path, compulsory agent, shared account, concurrency, resource controls and recovery are part of these sessions.

Engineering completion, operational qualification and capital promotion are separate evidence states of the same system. Unqualified money authority must remain blocked. A missing required runner/reader or fake-only integration is an open implementation row, not a completed but disabled feature. No session may hide an omission by renaming it optional.

Baseline: `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd` on `fix/red-r9-local-complete-cycle-2026-10-14`. The documentation branch is `docs/atlas-final-system-2026-10-16`, descended from that baseline. Begin implementation from the verified documentation tip so the new authority and plan are present. The older repository default branch is not the starting point.

The only authorized work in the document-preparation session is documentation and its publication. Future session prompts do not grant paid-provider, private-account, Demo-order or live-capital permission by their existence. Use the owner's actual scope for each subsequent action.

## 2. Session allocation and minimum-session policy

| Session | Sole model | Work owner | Dependency |
| --- | --- | --- | --- |
| S1 | GPT-6.1 Sol | Decision contracts, quantitative targets, model/research interfaces and acceptance oracles | Verified documentation tip |
| S2 | GPT-6.1 Sol | Account ownership, allocation, execution, permissions, protection and recovery | S1 contracts |
| S3 | GPT-6.1 Sol | Typed Brief, compulsory coordinator, tools, memory and directive lifecycle | S1/S2 interfaces |
| S4 | GPT-6.1 Sol | Real data/model composition, periodic runtime, resource/storage controls and research campaign | S1-S3 |
| S5 | GPT-6 Luna | Bulk UI, display/config wiring, reports, packaging and operator guidance | Frozen S1-S4 interfaces/recipes |
| S6 | GPT-6.1 Sol | Fresh-context review, integration repairs, exact-build qualification and evidence collection launch | All engineering scopes |
| E1 | GPT-6.1 Sol | Later empirical promotion/rejection decision after sufficient observation | Completed implementation and frozen forward evidence |

There are two model switches in engineering: Sol to Luna, then back to Sol. Never mix models inside a Codex session or assign financial/state-machine work to Luna to save session count. One session can contain several coherent commits, tests and context continuations. The six-session target is not permission to lower quality or guarantee that external blockers will disappear. If a scope cannot finish, continue it with its assigned model and keep the blocker visible; do not silently move core work to an unspecified later release.

Do not spend a separate session on every module, daily campaign observation or routine review. Review changes at their owning seam, then perform one fresh-context integrated review in S6. Long-running collection proceeds without a new Codex session per day. Empirical time cannot be compressed into a coding session.

## 3. Rules for every session

1. Verify the selected model using trusted session metadata when available. Do not invent a model identity from an assistant statement. Do not change models within the session.
2. Read repository `AGENTS.md`, current `log.md`, all three frozen authorities, this algorithm, this plan and relevant acceptance rows. Historical freezes remain required for compatibility; the current owner-approved design controls the new target where it explicitly differs.
3. Fetch/verify remote branch, exact starting SHA and ancestry. Work from the immediately preceding verified tip in an isolated branch/worktree; preserve unrelated dirty files. Keep one dependency chain, not competing edits to shared contracts.
4. Record a bounded implementation map and baseline checks before editing. Read actual production callers and saved readers; passing a helper test alone does not prove composition.
5. Preserve existing policy hashes/serialized artifacts. Add only necessary schema compatibility and migrations, with old-reader/replay tests. No product-version ladder, duplicate engine or broad rewrite.
6. Run tests that verify meaningful financial, causal, concurrency or failure behavior. Do not make implementation-mirroring tests the acceptance oracle. Baseline failures stay recorded and are reproduced when relevant.
7. Update the acceptance register with source/test/evidence references and exact build. Never label fixture, replay, public read, Demo order or live evidence interchangeably.
8. Root is the sole log writer. End with a short checkpoint and handoff: scope completed, changed contracts, tests, blockers, exact next task and pushed SHA. Never record credentials or raw private payloads.
9. Commit, push and independently verify the remote SHA; do not merge or activate capital merely because a session completed. A rejected lease requires inspecting the new head, not force-pushing over it.

## 4. S1 - Decision and quantitative foundation

**Sole model:** GPT-6.1 Sol. This is complex quantitative and compatibility work.

### Deliverables

- Introduce immutable candidate-evaluation population, independently frozen actions and a separate final selection result. Remove prior-selection requirements from the new evaluation seam without altering historical behavior. Adapt production callers, feature/action projection, sizing interfaces, Brief/outcome bindings and compatibility hashes together.
- Implement the empirical net-action comparator and squared-loss M0 mean path, including compact fit artifacts, mature-label readers, causal preprocessing, support/OOD, calibration and inference. Preserve Huber/L1 diagnostics with honest labels. Implement the L2 LightGBM challenger and old-model comparison pipeline.
- Define exact action cash targets, common terminal convention, no/partial-fill labels, source grades, signed funding, all fee legs, execution-price slippage accounting and programme-cost attribution. Separate realized venue truth from counterfactual assumptions.
- Implement offline selected-policy mean/selection allowances and the registered whole-policy calendar-block/multiple-comparison procedure. Freeze trial budgets, support rules and holdout access before final evaluation. No synchronous refits/bootstrap on the dispatch path.
- Preserve and integrate the intended research roles of S1-S8, analogues, volatility/state models and the existing return-forecast control. Implement the hold-versus-close future-value calculation and early-exit labels/research output. Do not expand context-only sleeves into invented directional policies.
- Define real numeric-runner interfaces for TiRex-2, Chronos-2-small and Kronos-mini/tokenizer: causal data shape, horizons/units, manifest/hashes, cancellation, resource envelope and immutable outputs. Real runtime bridges are completed in S4; their contract and test oracles are owned here.
- Freeze typed downstream interfaces for risk/account, Brief/agent, storage, status/configuration and UI. Later Luna work must not make semantic decisions.

### Primary integration points

`src/atlas/v2/contracts.py`, `selection.py`, `science/research_selection.py`, `risk.py`, `science/action.py`, `m0.py`, `m1.py`, `outcomes.py`, `evaluation_service.py`, `pretrade.py`, `scenario_engine.py`, `costs.py`, `portfolio.py`, `runtime/production.py`, `runtime/full_strategy_surface.py`, `features/`, `models/` and the existing strategy modules.

These are seams to extend, not an instruction to edit every file. Split existing large modules only when a bounded extraction demonstrably reduces coupling without changing authority.

### Acceptance before handoff

- A rank-one negative candidate cannot suppress a supported positive rank-two alternative; a third missing-data candidate remains in the denominator. One immutable set binds them all.
- The +1/-200 skew fixture cannot pass positive-mean admission. No location artifact can masquerade as a promoted mean artifact.
- Future/immature labels, revised information, unsupported size/liquidity and leaked final-winner features are rejected. Cross-symbol/overlap splits and holdout reservation remain causal.
- No-fill, partial-fill, funding-boundary, two-fee-leg and slippage-double-count tests pass. All active policies have deterministic exact management definitions or honest context/research roles.
- Early exit is not justified by sunk loss; hold/close uses comparable future paths.
- Legacy golden actions, indicator definitions, hashes and saved records replay unchanged. Required rows Q01-Q08, ST01-ST03 and their dependent contract portions close with meaningful evidence.

No new model receives money authority from S1.

## 5. S2 - Shared account, execution and recovery

**Sole model:** GPT-6.1 Sol. Luna must not implement or review these semantics as final authority.

### Deliverables

- Map actual Binance account semantics into explicit equity/free balance/bot allocation/manual reserve buckets. Implement per-command reflection/uncertainty/settlement with event-watermark evidence and no double subtraction/release.
- Implement exclusive symbol/side ownership, explicit aggregate adoption and the manual-add/reduce/flat/reversal/order/stop conflict transition table. Preserve known/possible exposure through every conflict and restart.
- Commit reservation, episode latch, lease and intent atomically. Serialize openings and support several protected positions; prove at least two. Keep a declared resource-qualified maximum rather than a hardcoded single total position.
- Implement exact feasible-lot checks, nonlinear impact/tier margin, common-factor joint shocks, liquidation/stop buffers, realized-loss-only consumption and user-bound risk/allocation settings. Unsupported mode never triggers an automatic account mutation.
- Implement scope-specific approval/autonomy/revocation, pause/cancel/flatten/adopt commands and immutable stop/time duties. Risk tightening cannot erase already-armed effects.
- Extend the existing one-writer journal, outbox, native algo identity/readback, UNKNOWN classification, no-reversal, dust, flat certificate and reconciliation boundaries. No second SDK retry owner or OMS.
- Finalize the reactive-protection capability manifest and residual-risk disclosure. Start monitoring at possible send, not a delayed fill alone. Keep safety/exit goals progressing across symbols while unrelated effects are unknown.
- Implement startup, physical-owner replacement, stale-task invalidation and trusted-storage terminal behavior. Establish protected safety queues/timers/connections/rate budgets and interfaces used by S4.

### Primary integration points

`src/atlas/runtime/v2_capital_authority.py`, `phase4_v2.py`, `assisted_control.py`, `selected_demo.py`, `binance_native.py`, `binance_reconciliation.py`, `position_lifecycle.py`, `protection_deadline.py`, `protection_evidence.py`, `flat_certificate.py`, `no_reversal.py`, `writer_lock.py`, `recovery.py`, `src/atlas/persistence/` and `src/atlas/v2/risk.py`.

### Acceptance before handoff

- Reordered snapshots, duplicate fills, lost ACK, manual margin use, deposits, funding and two competing symbols cannot overspend or double-release.
- Approximately $10 with no feasible lot returns NO_FEASIBLE_LOT without lot round-up or leverage bypass. Two funded distinct symbols can reserve sequentially and coexist.
- Manual additions/reductions/reversal at every send/protection boundary follow named authority; no fictional lots, wrong-side/manual close or unauthorized cancellation.
- Crash before intent, after arm, after wire send, after partial fill and around stop/readback reconciles without duplication/reversal. Unknown opening plus zero quantity is not flat proof.
- Permission revocation/expiry and risk changes cannot resurrect old plans or remove stop/time obligations.
- Compound tests include partial exposure, unknown remainder, stop rejection, mandatory timeout and unrelated reconciliation. Healthy safety deadlines remain measured; unavailable transport is reported unresolved.

This session uses offline/fixture evidence by default. An exact Demo/native lifecycle is qualified in S6 only within actual authorization. No new exchange action is implied by this plan.

## 6. S3 - Production Brief and compulsory coordinator

**Sole model:** GPT-6.1 Sol.

### Deliverables

- Implement required typed production readers with provenance, available-at bounds and job-specific manifests. Distinguish analysis-complete from capital-complete. Keep legacy blocked fixtures; never delete refusal guards to manufacture success.
- Integrate one durable coordinator through the existing gateway/memory/broker/budget boundaries. Use hosted `gemma-4-26b-a4b-it` on the existing native transport with strict host validation. Complete the bounded tool/continuation protocol rather than reusing a tool-free smoke as proof.
- Implement workflow/watch decisions, allowlisted analysis requests, verified event mapping, selection/rejection/abstention and the qualified early-exit request path. The host computes numeric rank/risk and dispatches orders.
- Implement conditional pre-trigger and exact direct directives, policy/evidence bindings, one-use consumption, setup invalidators, expiry and approval integration. Observed outage invalidates entry directives; no resurrection after conformance recovery.
- Enforce one active route/ledger, worst-case usage reservation, possible spend on lost response, bounded queue/calls/tools/tokens and no automatic paid fallback. Preserve existing critic/research records without running competing authority loops.
- Add persistent in-app health/incident and evidence/refusal projections for Luna. No external notifications are sent without a separately authorized channel.

### Primary integration points

`src/atlas/v2/runtime/production_intelligence.py`, `research/production_facts.py`, `decision_brief.py`, `agent.py`, `gemma_native.py`, `research/memory.py`, `agent_intelligence/`, `runtime/production.py`, `product.py` and the existing watch/due-work/memory repositories.

### Acceptance before handoff

- Complete typed fixtures produce an honest analysis packet; removing required evidence blocks the correct job, while an unavailable optional forecast does not block unrelated analysis.
- Fake candidates/refs, prompt injection, unknown fields, nonfinite values, stale/late output and out-of-scope tools produce no authority.
- Conditional directives cannot waive economics, exact approval, account freshness or setup consumption. Direct responses cannot extend the original deadline.
- Provider timeout/429, quota exhaustion, model identity change, lost response and approval expiry retain spend/effect uncertainty and pause entries; existing stops/time exits remain independent.
- The coordinator's selected candidate must satisfy the host's registered ranking rule. New semantic concerns return to deterministic evidence verification.
- Run bounded real API conformance only with the available authorized route. Record task competence separately from JSON validity. A missing route is an explicit blocked gate.

## 7. S4 - Complete continuous runtime and bounded research

**Sole model:** GPT-6.1 Sol. This contains resource arbitration, source chronology and model integration, not simple bulk work.

### Deliverables

- Wire the existing periodic ScannerRunner and durable slot ledger into the real production supervisor. Integrate native strategy/watch clocks, account-driven effort, fair exploration, missed slots and cold-start data collection without a promoted mean model.
- Complete real public-data paths and the S1-S8 research/context composition: bar warmup, trade/book continuity, derivatives/funding, verified events, peer breadth, outcomes and future-value exit research. Unavailable source truth remains explicit.
- Implement real bounded inference bridges for all three selected foundation models plus their exact tokenizer/manifest contracts. Run real checkpoint inference in the supported environment, save sanitized smoke/performance evidence and compare outputs with the empirical forecast baseline. A fixture-only adapter does not close this deliverable.
- Implement one-worker serial admission, cancellation/termination, memory/CPU quotas, explicit model downloads, cache eviction and an optional separately qualified numerical-endpoint mode. No local LLM or on-entry framework installation.
- Implement the complete resource budget: tiered subscriptions, process-tree memory, priority safety capacity, bounded queues/history, physical disk reserve, emergency durability allowance, logical partitions, WAL/read limits, retention pins, replay grades and export atomicity.
- Keep one complete application with headless/reduced desktop choices. Audit exact dependency locks, model/framework compatibility, SQLite runtime and Windows/Linux deployment recipes. Reproduce baseline rc5 separately from selecting a qualified operational adapter build; do not blindly upgrade/downgrade.
- Implement the frozen experiment/campaign runner and reports for no-opening, deterministic R9, empirical mean, compulsory coordinator and registered challengers. Collect all slots, refusals, provider incidents, actual/simulated outcomes and programme costs.

### Primary integration points

`src/atlas/v2/runtime/production.py`, `ops_supervisor.py`, `storage_preflight.py`, `live_health.py`, `broad_public_runtime.py`, `research_prerequisites.py`, `outcome_maturity.py`, `action_outcome_producer.py`, `src/atlas/scanner/`, `v2/data/`, `v2/models/`, `v2/memory/`, `v2/resources.py`, `product.py`, dependency locks and platform scripts.

### Acceptance before handoff

- One genuine durable source-to-scanner-to-action/evaluation-to-Brief-to-compulsory-agent shadow cycle completes and replays after restart. Unavailable economics stay unavailable; no capital bypass.
- Cold start can gather honest research/labels without already owning a trained profitable model. Historical backfill does not become a missed live entry.
- Each selected model executes real, causal, manifest-bound inference or remains explicitly BLOCKED; all required integrations must close before engineering completion.
- Pressure sheds optional research/deep discovery before safety. Memory/disk partitions, raw ring, downloads, staging and WAL are actually bounded.
- A pinned reader, physical disk fill, quota rollover, export failure, clock jump and process suspension cannot cause false settlement/rearm or active-evidence deletion.
- Produce exact target deployment recipes and benchmark scripts. Start the admitted-workload soak early enough that S6 can inspect results. A failed target remains failed; no threshold-only repair.

## 8. S5 - Bulk product delivery

**Sole model:** GPT-6 Luna. Start only after S1-S4 interfaces and expected status/configuration behavior are frozen by Sol.

### Allowed work

- Wire existing desktop/IPC projections to approved typed read models; add status panels, bounded charts, watch/refusal tables, comparison reports and exports.
- Present actual bot allocation, manual ownership/conflict, available/possible quantity, protection coverage, model integration/qualification, provider cost/quota, resource/evidence grade and feasible-lot frontier.
- Wire configuration controls to existing validators/commands without duplicating their logic. Separate pause entries, cancel own remainder, flatten owned exposure and adopt aggregate exposure.
- Implement operator help and explicit disabled/unavailable reasons. Fix labels/links/configuration examples against Sol-approved definitions.
- Apply frozen Windows/Linux packaging/install recipes and build metadata. Add projection/format/packaging checks and literal fixture updates where the semantic oracle is already supplied.

### Forbidden work

No target/uncertainty or model-promotion design; risk/balance/quantity arithmetic; fee/funding logic; account/order state transitions; SQL authority/migration design; permission/authentication rules; native adapter semantics; retry/UNKNOWN/protection/recovery behavior; safety timer changes; dependency/build policy choices; invented financial test oracles.

If a required interface is absent or ambiguous, record the exact blocker for S6 or return it to the Sol-owned scope. Do not implement a new semantic layer in the UI. Do not silently turn a disabled filter into a passed safety check.

### Acceptance before handoff

- UI accurately displays zero feasible capital, manual conflict, unknown orders, partially protected quantity and provider/resource incidents without implying permission or profitability.
- Headless operation still works; the desktop does not write control tables or pin unlimited read transactions.
- Packaging follows the exact approved locks/recipes; clean smoke and resource behavior are recorded separately for available platforms.
- The staged diff contains only approved presentation/wiring/packaging work. S6 reviews it before integrated acceptance.

## 9. S6 - Full review and exact-build qualification

**Sole model:** GPT-6.1 Sol, in a fresh session context.

### Deliverables

- Review the complete baseline-to-current diff, especially Luna changes, financial predicates, migrations, new authority and production callers. Close every core register row or report engineering BLOCKED. No missing core feature is reclassified as a future version.
- Run meaningful targeted suites and the required broad regression/static/lock/licence/secret checks. Reproduce the three R9 timing/resource failures; identify cause and qualification envelope. Preserve old failures honestly. No F2 pass may be obtained by relaxing the old 200 ms/80 ms thresholds without a justified new measured service contract and impact analysis.
- Qualify the exact Binance lifecycle with authorization: ordinary/algo/child IDs, send/ACK loss, partial/no fill, stop coverage, mandatory/discretionary exit boundaries, dust, cancel/fill race, permissions, manual conflict and restart with exposure. Include at least two simultaneous Demo positions.
- Execute all compound failures in the acceptance register, including restoration in different orders. Verify physical single-writer/replacement, no duplicate/reversal, positive flat proof, durable reservations and no premature rearm.
- Complete at least 72 hours on actual declared Windows/Linux 4 GB Celeron-class targets with warmup, admitted peak load, both UI/headless modes, model work, provider incidents and retention rollover. Distinguish absent hardware from a pass.
- Freeze the exact engineering build, input profiles, experiment family, primary estimand, confidence procedure, risk tolerances and future collection schedule. Launch/continue genuine prospective evidence under the fixed policy.

### Required outputs

An exact-build handoff, machine-readable acceptance/evidence manifest, platform/resource measurements, venue lifecycle evidence, no-degradation comparison, unresolved blocker list and one honest status: ENGINEERING_COMPLETE / OPERATIONALLY_QUALIFIED / BLOCKED. These labels are evidence states, not product editions. No status grants capital by itself.

The full suite may retain a documented legacy load failure outside the active admitted profile, but it cannot be described as entirely green. Every new-system required path and safety gate must pass on its claimed profile. Baseline failures affecting active paths are release blockers. Missing venue/host/provider evidence also stays blocked.

## 10. E1 - Later evidence and capital decision

**Sole model:** GPT-6.1 Sol. This is one later decision session, not another feature-building version.

Inspect the frozen forward campaign after the algorithm's observation/support/analysis conditions are met. Verify data/model/provider vintages, full denominator, costs, causal labels, independent block support, actual execution calibration, risk/tails and any policy changes. Invalidate contaminated comparisons rather than resetting history opportunistically.

Compare programme PnL with no-opening, deterministic R9 and empirical controls. Evaluate the agent's incremental trading/operating value; model error and JSON validity alone are insufficient. Apply the preregistered selection/search/confidence procedure and stress actual fees, funding, spread, latency and free-route assumptions.

Decide per defined capability whether it is evidence-qualified; do not invent a new implementation roadmap to disguise a failure. Capital GO additionally requires current actual lot feasibility, approved permissions/allocation and accepted residual protection risk. Inconclusive or negative value means no capital promotion, while the delivered system and evidence remain intact. An independently authorized live execution-calibration experiment is separately capped and never presented as proven profitable production.

## 11. Avoidable rework

- Correct candidate/evaluation/selection contracts before wiring the agent; otherwise the new agent will inherit selection-first bias.
- Define allocation/reflection/ownership before enabling concurrency; locks alone cannot resolve external manual trading.
- Bind model targets, source grades, support and uncertainty together; changing only a loss string leaves misleading saved outputs.
- Implement job-specific typed Brief readers; bypassing BLOCKED makes later evidence unusable.
- Freeze account/agent/status interfaces before Luna work; keep validators out of UI.
- Complete resource arbitration before sustained capture; a 2 GiB constant cannot bound a roughly 50 GB projected capture workload.
- Preserve working boundaries and exact old evidence. Do not rebuild indicator libraries, duplicate retry owners or erase inconvenient failures.

## 12. Final completion standard

The project is fully implemented only when all core requirements have real code and required evidence, existing capabilities have not been silently lost, and operators can install, run, explain, stop and recover the same complete system. Profitability remains a separate empirical result. The plan optimizes for maximum validated trading value with minimum necessary complexity, not maximum model count or the appearance of a finished release.
