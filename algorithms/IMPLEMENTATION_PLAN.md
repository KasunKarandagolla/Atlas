# ATLAS RED - minimum defensible implementation sequence

**Finalized target work forecast; independent publication acceptance and subsequent code-scope approval required. Operative amendment remains proposed.** Runtime anchor R9 `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`; documentation anchor `3436dde4ca5169d0cdec6befe8a485c0c0e992b2`. Discover the actual tip before implementation. This correction authorizes documentation publication only. No trading-code changes, paid/provider generation, private venue requests or orders occur. Future offline implementation requires separate accepted scope.

## 1. Recommendation and 4/5/6 comparison

**Choose five engineering sessions: P1 Sol → P2 Sol → P3 Luna → P4 Sol → P5 Sol.** Four complex Sol passes have distinct responsibilities: quantitative contracts, account/execution lifecycle, agent/production integration, independent final review. One Luna pass consolidates all semantics-frozen mechanical work. No separate Sol session is needed just for agent plumbing, UI, renderer glue or documentation.

| Plan | Sol / Luna | Model switches | Real ownership/dependency arrangement | Main rework/test risk | Verdict |
| --- | --- | --- | --- | --- | --- |
| 4 | 3 / 1 | 2 | Sol quantitative+all account/execution contracts and implementation; Luna bulk; Sol all agent/production/performance integration; Sol independent review | First pass combines mean/selection artifact migration with 2,378-line control-journal and lifecycle changes; alternatively the third pass absorbs both execution and integration. Both merge two independent high-risk clusters before their consumer oracles settle. | **Reject as the high-confidence plan.** Possible chat compression is not evidence that the merged critical work is safe or cheaper. |
| **5** | **4 / 1** | **2** | P1 numerical/new-policy contracts; P2 capital/lifecycle; P3 mechanical adapters/UI/packaging; P4 one agent+production integration owner; P5 fresh review | P4 is substantial, but it consumes completed quantitative/account APIs and Luna glue. Current agent projector is already a postreceipt consumer of the same production pipeline; combining their integration avoids a second competing composition. | **Smallest defensible plan on this audit.** Conditional on P1/P2 actually closing contracts before Luna. |
| 6 | 4 / 2 | 4 | Sol numerical; Sol capital; Luna adapters; Sol agent/production; Luna final UI/packaging; Sol review | Lower UI interface rework if runtime status contracts remain unsettled, but an additional Luna pass and two extra switches. Does not reduce the difficult quantitative/execution/integration work. | **Fallback if frozen view/adapter contracts cannot be delivered before P3.** Do not create it by default. |

The previous six-pass 5-Sol/1-Luna arrangement remains possible but spends a separate Sol pass on work that shares production ownership. The revised six-pass alternative uses 4-Sol/2-Luna instead. No token prices, elapsed coding hours or percentage cost savings are asserted.

This is a dependency/risk judgment, not a mathematical minimum or a promise that each scope fits one short context. A session means one model-only Codex work package with a reviewable pushed checkpoint. Do not disguise a failed five-pass plan by counting repeated new Codex threads as “the same session”. Record extra correction sessions honestly. Continue within an existing context when feasible; use normal checkpoints/compaction. No daily sessions are needed while an unchanged soak/campaign runs.

## 2. Common entry/exit contract

Before P1, independent review accepts the actual pushed document correction and the owner authorizes P1 offline-code scope/model boundaries. Conflicting operative changes remain disabled unless their exact amendment scope is independently accepted. Original owner-source traceability is now reconciled in AUTHORITY_RECONCILIATION.md with exact identities in SOURCE_AUTHORITY_MANIFEST.json; no source is invented. The precise manual/adoption policy and operative amendment retain separate pending acceptance gates. Proposal acceptance authorizes only the specified implementation scope, not new permissions, paid calls or capital.

Each session verifies model from trusted session metadata; no self-reported label is sufficient. Exactly one of GPT-6.1 Sol or GPT-6 Luna, including all subagents. If unavailable/unverifiable, ask the owner rather than substitute. Use no subagents if inheritance is unverifiable. No subagents are required by this plan.

Read AGENTS/current root checkpoint/translog and accepted authorities, verify remote tip/ancestry and preserve dirty/untracked work in an isolated worktree. Run meaningful changed-seam tests and required checks; retain baseline failures. The root coordinator alone updates log.md; preserve append-only translog history. Commit/push only authorized implementation work and independently verify the exact tip. No auto-merge or self-acceptance. Before a dependent session, independent ChatGPT review examines the **actual pushed diff and evidence**; a handoff assertion is not acceptance. These reviews are not additional Codex implementation sessions.

No paid API, provider generation, authenticated exchange/private read, Demo/live order, new permission or account-mode change without separate explicit authorization. Offline fixtures and network-fenced tests are the default. Missing hardware/checkpoint/venue evidence remains visible, not a feature waiver.

## 3. P1 - GPT-6.1 Sol: quantitative foundation and closed contracts

**Own:** new evaluation/selection artifacts and exact-action targets; M0 mean fit/inference, M1 mean challenger, empirical comparator; causal training/support/uncertainty and statistical oracles; new eligibility/action/Brief/tool/image/management interfaces; future public status/config schemas. All opaque IDs/units/time/availability/permission meanings and red→green oracles are settled here.

**Ordered edits:**

1. Add new contract identities in `v2/contracts.py` and the existing serialization owners: immutable CandidateEvaluationSet, per-candidate EvaluationRecord, EligibleSet and final SelectionResult. Retain CandidateSetV2 and frozen selector readers.
2. Extend `v2/risk.py`, `science/action.py`, `m0.py`, `m1.py`, `evaluation_service.py`, `pretrade.py`, `outcomes.py` and admission callers with a new per-action route independent of previous selection. Do not remove historical selected-ID checks globally.
3. Split new prefit mean training/calibration from bounded inference. Current `run_phase2_economic_evaluation` calls `fit_m0`; new dispatch must not refit each alternative. Reuse solver/preprocessing, mature provenance, holdout reservation, costs and portfolio machinery with new estimand IDs.
4. Define exact management alternatives/labels, standalone conservative cash and portfolio-risk gates; B selection authority and comparator policies. Clarify NO_OPEN, tool-result influence, expired/not-evaluated states and no fallback.
5. Freeze source→reader→consumer requirements, numerical model time-series/units/horizon manifests, ChartEvidenceBundle, route-selection benchmark with metric definitions/pass limits/allowed dependencies/privacy-spend terms and an objective tie rule, observation verifiers and tool/effort/rescreen schemas. Publish executable acceptance fixtures/oracles, not vague method names. Do not implement native financial semantics in UI.

**Required tests:** rank-one negative/rank-two positive, fourth-plus winner and missing candidate; 99×+1/1×−200 mean; future/immature/overlapping labels; no/partial fill, both fee legs/funding, slippage once; new-versus-old artifact isolation and historical goldens; no successful new action depends on selected_candidate_id. Predeclare whole-policy A/B/(optional C), numerical/image comparison and complete-denominator evidence. No numerical confidence claim without support.

**Handoff to P2/P3:** typed interfaces, exact units/predicates, consumer list, migration identity, golden fixtures, allowed file owners and explicit blocked external rows. If these are incomplete, P1 is not accepted; Luna must not fill the gaps.

## 4. P2 - GPT-6.1 Sol: capital, execution and position lifecycle

**Own:** `runtime/assisted_control.py`, `phase4_v2.py`, `v2_capital_authority.py`, selected Binance host/readiness/reconciliation, `position_lifecycle.py`, protection/flat/recovery/writer owners, control persistence/schema/migrations and shared account/risk interfaces.

1. Implement allocation/reflection/uncertainty accounting and exclusive/aggregate ownership/conflict policy on the existing journal. Approval+reservation+lease/setup latch+intent must commit atomically where authority is consumed. Ops and provider stores communicate through hashes/outboxes, not assumed cross-store transactions.
2. Replace the legacy global unresolved-intent opening veto only in the new accepted path. Scope actual/possible exposure and reservations per account/instrument/epoch; allow two independently protected instruments under one dispatch owner. No global flag bypass or duplicated OMS.
3. Implement the finite management registry and exact reducing quantity, dust, stop replacement/trailing/TP races; mandatory TARGET_FLAT remains independent of model/optional work.
4. Generalize the **currently Demo/Testnet-only** Binance identity/config through a separate strict accepted environment contract for eventual production use, preserving Demo-only readers/tests. This is code capability, not production authorization. Verify the actual pinned Nautilus transport semantics; native parameter names do not prove exchange wire behavior.
5. Add compatible migration/rollback, UNKNOWN/ambiguous-send and safe restart with existing/open/manual exposure. Keep monotone stops and never downgrade to a binary unable to read active records.

**Required tests:** competing reservations/manual withdrawals, delayed reflection and duplicate fills; $10 infeasible lot; same-symbol manual add/reduce/reversal; crash at approval/intent/arm/wire/ACK/fill; partially protected quantity and simultaneous symbols; cancel/fill/stop/TP races; unknown on A cannot starve B; positive flat proof; native wire parameter exclusion rules and environment fencing. Real venue evidence remains a separate authorized gate. Implement the exact manual transitions and MI01-MI12 in SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md; pending owner concurrence does not become consent through code. No new ambiguous aggregate close is a mandatory-exit shortcut.

**Handoff:** stable backend/status/command contracts and fixtures for all Luna consumers. Native ownership, gateway-to-intent and managed aggregate scope have no unresolved semantic TODOs.

## 5. P3 - GPT-6 Luna: all well-specified mechanical work

**May own:**

* Chart evidence projection serialization, the mechanical local/remote comparison harness and the **one selected** renderer adapter/templates. P1 must freeze metric definitions, pass limits, allowed dependencies, privacy/spend terms and an objective selection rule. P3 may run/record that harness and apply that rule; it cannot invent resource/safety thresholds. The harness uses thin supplied-data prototypes, not two full production vendors. A missing host or ambiguous result remains BLOCKED; Sol/independent review resolves any new judgment before production route work. P4 qualifies the integrated route, rather than choosing the first route only after Luna has finished.
* Numeric-worker manifest/config/download/offline-file checks and exact serialization/load glue for the selected research forecasters, only where Sol supplied time-series/units/output oracles. Do not invent target transformations, quantiles or sampling semantics.
* Predefined read-only projection/IPC field wiring, requested/effective effort display and commands to backend validators, existing chart/history/export controls and source/verification/cost/missingness status.
* Package recipes/hooks, dependency inventories, CLI launchers, reports, docs and smoke/regression scaffolding using Sol's accepted oracles. No new package/runtime choice without the recipe's allowed alternatives.

**Must not own:** trading/uncertainty formulas, eligibility/ranking meaning, fee/account/quantity logic, journal or schema migrations, source chronology/completeness, permission/approval/UNKNOWN transitions, native order/retry/protection semantics, provider trust/budget policy or economic promotion. Existing tests cannot be weakened to match the implementation. Missing semantics go back to Sol; do not write hidden placeholders that appear complete.

**Files:** `desktop/app.py`, `v2/desktop/{projection,ipc}.py` for predeclared views only; presentation portions of `product.py`; existing packaging scripts/entry wrappers; a small render adapter and registered template data; mechanical portions of model worker adapters. Separate Sol-frozen backend portions of shared files by exact symbols, not a blanket Luna licence to edit `product.py` or `projection.py`.

**Exit:** real mechanical capability against deterministic contract fixtures, historical GUI/IPC paths preserved, no forbidden semantic diff, packaging evidence distinguished by platform. Provider/target-host absence is reported; no invented real conformance.

## 6. P4 - GPT-6.1 Sol: one difficult agent/production integration pass

This combines the previous dedicated agent and runtime sessions because their concrete boundary is `OpsSupervisorV2` → `project_production_receipt` → Brief/memory → coordinator → accepted selection → control consumer. Separate implementations here would both rewrite the same source/caller identity chain.

1. Replace the new policy's selection-first production composition using P1 APIs; retain the legacy path. Attach a bounded real native-clock scheduler and prospective ledger. V1 ScannerRunner's frozen four-hour clock is **not** a drop-in S3/M15 scheduler. Reuse its registration/missed/dedupe semantics without corrupting clock identity.
2. Complete job-specific production readers for all declared facts, BTC/ETH/peers, source/flow/event/outcome/model fields and every evaluated action. Preserve the projector completeness guard until real readers and a successful fixture justify the new compiler. Do not mark partial data COMPLETE.
3. Extend existing gateway/broker/usage/job memory with separately accepted tools, one qualified real image transport, eligibility-bound SELECT_ONE, event rescreens, effort changes and independent verifiers. Legacy critic/extractor roles remain replayable, not competing live decision authorities. Preserve DeepSeek future preference with per-route capability gates; reuse native Gemma rather than silently promote it. Outcome due_work/watch outbox remain typed; use the existing namespaced agent ledger and smallest distinct research-task projection needed.
4. Complete nontrivial actual TiRex/Chronos/Kronos inference contracts and source readers, job isolation, cancellation and conformance using P3 glue. Fixture callbacks or flattened heterogeneous features cannot qualify actual model input.
5. Integrate bounded archive/cache/pins and profile actual R9 storage/projection bottlenecks. Optimize the measured hot owner path first; no database/framework replacement, widened queues, reduced ten-second breadth or unapproved service threshold relaxation.
6. Produce a genuine complete source→candidate alternatives→sizing/economics→Brief→compulsory agent→durable **shadow** result→outcome/restart chain, plus exact plan-to-control handoff tests. A fake complete Brief disconnected from genuine production sources is insufficient.

**Exit:** all selected paths implemented with meaningful offline integration; frozen production/status/build contracts, candidate/decision calendar and complete evidence records. Start an instrumented target-host soak/campaign only within actual authorization. The code must keep BLOCKED states for unavailable venue/host/provider/checkpoint evidence.

**Scope risk:** if P1/P2 contracts were not actually frozen, or P4 discovers a new fundamental numeric/account rule, stop and obtain independent review. The four-pass plan was rejected to avoid hiding such work here. A separate correction pass may be necessary; do not falsify the count or move it into Luna.

## 7. P5 - GPT-6.1 Sol, fresh context: independent exact-build review

Inspect the actual complete baseline→current pushed diff, especially Luna shared-file edits, new artifacts/migrations, absence of old implicit SELECTED prerequisites and duplicate provider/control owners. Close all 79 core rows and 12 preservation/16 compound cases with exact evidence or explicit blocker. Review licence/locks/secret checks, linked SQLite, platform packaging and baseline failures.

Run relevant focused tests, required broad checks and exact-build compound incidents. Independently assess genuine complete production success, two positions/manual interference/stop replacement/UNKNOWN/restart, actual permitted provider/image/model conformance, ten-second observation and 72h Windows/Linux minimum-host/retention evidence. Never count fake receipts or this audit's lightweight environment as qualification.

Issue ENGINEERING_COMPLETE / OPERATIONALLY_QUALIFIED / BLOCKED with the unresolved register and exact build. No automatic capital promotion. Any fixes require their own focused checks and updated exact evidence, not silent acceptance.

## 8. Evidence time and later decision

72h operation and eight-week-plus prospective evidence consume elapsed time, not a Codex session per day. Freeze the campaign, schedule monitoring outside chat and inspect only meaningful incidents. A later after-cost/capital decision is required; if performed in Codex, budget **one further Sol review**, making six total planned Codex interactions after engineering. It can instead be an independently authorized analysis of the fixed artifacts without a new coding session. Do not claim five sessions includes eight weeks of evidence or guarantees a capital GO.

## 9. File ownership and regression radius

| Source owner / exact seam | Work class | Owning pass | Regression surface |
| --- | --- | --- | --- |
| `v2/contracts.py`, selection artifacts, `science/research_selection.py` | New evaluation/eligibility identities; preserve old selector | P1 Sol; P4 caller wiring | Risk/action/M0/outcome/Brief/plan hashes and saved readers |
| `science/{m0,m1,evaluation_service,pretrade,admission,outcomes}.py` | Mean target, prefit inference, support/calibration | P1 Sol | Existing diagnostics and whole-policy experiments |
| `science/{costs,portfolio,scenario_engine,analogue}.py` | Reuse arithmetic; narrowly extend lifecycle/compatibility | P1 Sol | Fee/funding/execution paths, no/partial fill, portfolio and analogue labels |
| `v2/risk.py`, `runtime/assisted_control.py`, authority/phase4 | Allocation and scoped concurrency | P2 Sol | All intent/reservation/approval and old V1 behavior |
| Control journal/schema/migrations | Atomicity, leases, effect/reflection and compatibility | P2 Sol | Crash/reopen, old DBs, rollback and active records |
| Binance demo/native/readiness/reconciliation and lifecycle | Exact environment/protection/reduce/management | P2 Sol | Venue identity, algo/child IDs, possible exposure/flat certificates |
| `research/{decision_brief,agent,gemma_native,production_facts,memory}.py` | Accepted reader/authority/binary semantics | P1 contracts; P4 Sol implementation | Completeness, budgets, source time, agent jobs and action binding |
| Broker/controller/job persistence | Reuse; add typed accepted coordinator role | P4 Sol | Spend/lease/identity isolation and old critic/extractor replay |
| `runtime/{production,ops_supervisor,production_intelligence}.py` | Single new-policy orchestration | P4 Sol | Most new pipeline consumers; must be one owner |
| Data/feature/history/peer readers and native cadence | Preserve actual observation; fill causal bindings | P4 Sol | Source publication, broad breadth, S1-S8 and missingness |
| Numeric model adapters | Pure glue versus time-series/quantile meaning | P3 Luna glue; P1/P4 Sol semantics | Real weights/shape/units/runtime and old fixtures |
| Renderer/templates | Frozen pure projection/output | P3 Luna; P4 qualification | No effect on financial values or safety; CH rows |
| Desktop/IPC/product presentation | Frozen projections/config commands | P3 Luna | Old bounded chart, statuses/controls; P5 review |
| Package scripts/entry wrappers/docs | Frozen recipe | P3 Luna | Clean start/import/recover and no hidden downloads |

No broad split of `production.py`, `admission.py`, `product.py` or `memory/repository.py` is required. Extract a small compatible evaluation reader, renderer or adapter only when the actual consumer boundary justifies it. No source module is retired merely because it looks old.


## P1 offline implementation checkpoint — 2026-10-16 (review pending)

Owner-authorized P1 candidate `impl/atlas-red-p1-quant-foundation-2026-10-16` is based on verified `3cb4f052472623381145573523c7077950de78fd`, descendant of accepted f9f8ef0. Functioning new offline route is owned by existing `science/evaluation_service.py` with focused `mean_evaluation`, `mean_models`, `mean_scenarios`, `mean_research` and research semantic modules. Reuses contracts/risk/action/costs/portfolio/OpsRepository/LightGBM owners; no second operational controller or store. Historical routes/readers remain. No SQL migration, live allocation/OMS/native lifecycle, renderer/weight loader or campaign runtime added.

Source build `81a268d4bbade5999b3057d9301411eacf4fa14752a2f09013bd918bc5e363a7`; full source symbols, 52 versioned contracts, exact fields/units/clock/refusal rules and P2/P3/P4 fixture handoff: [P1 report](../docs/v2/ATLAS_RED_P1_HANDOFF.md) and [interface inventory](../docs/v2/transition-p1-quant-evidence/interface-inventory.json). Tests: combined 1,137 pass/1 skip, final P1+IPC 132 pass, preservation 272 pass/two long S3 exclusions; coverage union 1,420 pass/1 skip, not full-suite qualification. Mypy has exact baseline 53 errors/17 files. Acceptance CSV distinguishes tested P1 subsets from overall open rows. Actual pretrained/provider/renderer, operational native lifecycle, integrated prospective campaign and host/economic qualifications remain later gates. Independent exact-remote-SHA P1 review must precede P2. This append records progress only; canonical design/PDF/manual/proposed policy and accepted meanings are unchanged.


## Phase A P1 financial-evidence correction — 2026-10-16 (independent review pending)

Owner-authorized isolated correction from unaccepted `8fc2109c…`, branch `fix/atlas-red-p1-financial-evidence-2026-10-16`. `science/mean_calibration.py` verifies immutable origin/OOF/fold/serving-training/dependence chains; existing `mean_research.chronological_mean_oof` supplies actual fits through an additive recording callback. `science/mean_scenario_evidence.py` resolves causal templates, execution, empirical probability/tail support and existing portfolio projections. Existing evaluator and B consumer verify both chains. Old codecs/P1 evidence preserved; unsupported V1 grants refused. Reuses OpsRepository, model/cash/ES/portfolio owners; no new store/OMS/schema/account lifecycle.

Source `581c44bfd2ee7e9322d890c828d21cb2d7420ea4a381ffb6a64d665af388443c`; Broad affected group 1,200 pass/1 skip; final complete P1 group 179 pass, including all A/B and seven common-background/serving-training cases. Latest deduplicated coverage 1207 pass/1 skip, not a full repository suite. Final P1 follows the last serving-membership/background hardening; broad legacy coverage was observed before those isolated new-path guards. Exact tests/interfaces/source/evidence/reopen/limits: [correction handoff](../docs/v2/ATLAS_RED_P1_FINANCIAL_EVIDENCE_CORRECTION.md). Mypy baseline remains 53 errors/17 files. Canonical design/PDF/manual/proposed policy and freezes unchanged. The parent is not accepted and this correction cannot self-accept. Stop for independent exact-SHA review; P2 still requires separate explicit owner authorization after acceptance. Capital NO-GO.
