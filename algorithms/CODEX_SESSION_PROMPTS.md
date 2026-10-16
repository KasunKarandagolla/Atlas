# ATLAS RED - model-separated implementation handoff

**P1 sole model: GPT-6.1 Sol.** Target direction and the five-session forecast have conditional design approval. This is a complete proposed offline implementation prompt, **not an instruction to start during the documentation pass**. Independent acceptance of the actual published SHA and subsequent owner approval of P1's code scope are required. New operative/manual authority remains unaccepted.

The final handoff supplied after remote verification replaces `EXPECTED_DOCUMENTATION_SHA` below with the actual immutable pushed SHA. The repository prompt cannot contain its own commit hash without a circular dependency; before use, obtain the separately verified SHA, set it once and reject a changed branch rather than silently following it. MANIFEST.sha256 independently binds this exact content. No unverified placeholder is an execution anchor.

## Copy-ready P1 instruction body

```text
ATLAS RED P1 - GPT-6.1 Sol ONLY, including any explicitly authorized subagents.

EXPECTED_DOCUMENTATION_SHA: use the exact 40-hex SHA from the independently
verified final documentation handoff, not the older 3436dde4 publication.
EXPECTED_RUNTIME_ANCESTOR: 96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd
REPOSITORY: KasunKarandagolla/Atlas
DOCUMENTATION_BRANCH: docs/atlas-final-system-2026-10-16

GATE BEFORE CODING
Verify the owner has subsequently approved this P1 offline-code scope and an
independent reviewer accepted the actual final documentation SHA/diff/checksums.
If not, stop before editing. Verify actual model with trusted session metadata;
if GPT-6.1 Sol is unavailable/unverifiable ask owner, never substitute. Use no
subagents unless expressly authorized and same-model identity is verifiable.
One model for the entire session; do not perform Luna's mechanical bulk scope.

Verify exact remote SHA, ancestry and actual working-tree state. Fetch current
branch heads; a newer/unexpected tip requires diff review, not blind carry-forward.
Create an isolated implementation branch/worktree preserving unrelated changes.
Never force-push or start from the older default foundation branch.

Read AGENTS.md, root log.md and relevant append-only translog.md. Read all three
historical freezes IN FULL and verify:
V1 c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c
V2 e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78
Agent d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d
Read algorithms/ATLAS_SYSTEM_DESIGN.md, DESIGN_DECISIONS.md,
AUTHORITY_RECONCILIATION.md, SOURCE_AUTHORITY_MANIFEST.json,
SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md, operative amendment proposal,
IMPLEMENTATION_PLAN.md, REQUIREMENTS_AND_ACCEPTANCE.md, both CSV registers,
REPOSITORY_AUDIT.md and SURGICAL_CHANGE_MAP.md. Verify MANIFEST.sha256.
Read original owner sources in the supplied read-only Red/reference set as
identified by exact manifest hashes, especially target lock/OD section 1 and
ADR-002 A.0, ADR-003/005/006/007/008/009/010/011, runbook sections 2-7.
Original contents have been reconciled; do not invent missing-source claims.
If this implementation environment lacks an exact file, identify it precisely.
Keep Red read-only and do not upload original source ZIPs/private context.

AUTHORITY
ATLAS_RED_ELIGIBLE_ACTION_COORDINATOR_V1 remains PROPOSED - NOT OPERATIONALLY
EFFECTIVE. The precise shared aggregate/manual policy is PENDING OWNER APPROVAL.
Offline contract/quantitative code is permitted only within the subsequent P1
authorization; no code change may activate a conflicting policy. Existing
zero-authority roles and capital disables stay. No paid/provider generation,
authenticated account reads, public-live venue experiments, Demo/live orders,
account-mode changes, deployment, permission widening or capital activation.
Use network-fenced fixtures and scrub credentials in child test environments.

INSPECT REAL FUNCTIONS AND CALLERS BEFORE CHANGING THEM
R9 ProductionOpsCyclePortV1.process_event selects CandidateSet before risk/action.
size_selected_candidate, freeze_action and m0.action_features bind selected ID.
run_phase2_economic_evaluation calls fit_m0 synchronously. M0._fit is Huber IRLS;
M1 uses regression_l1. production_intelligence's missing-reader BLOCKED guard is
intentional. Read their actual producers/readers/tests/serialization before edits.
Preserve old path hashes, V1/Bybit comparators and historical readers; no generic
rewrite or global deletion of selected-ID/authority checks.

IMPLEMENT FULL P1 SCOPE
1. Add immutable independently keyed CandidateEvaluationSet/work trace,
   exact-action EvaluationRecord/EligibleSet and separate SelectionResult in
   existing contracts/serializer owners. Preserve all population/refusal/miss
   records; no singleton selected-set workaround or fixed-three search cutoff.
2. Extend risk/action/M0/evaluation/outcome consumers with new per-action path
   independent of prior final winner. Risk alone sizes exact quantity; all
   material action fields bind economics/approval. Do not implement P2 native
   control, ownership migration or dispatch in this pass.
3. Implement arithmetic-mean net-action cash target, simple empirical comparator,
   L2 ridge candidate and L2 LightGBM challenger with offline fit/calibration and
   compact bounded prefit inference. Preserve Huber/L1 as historical location
   diagnostics, mature provenance/holdout reservation and compatible arithmetic.
   Never trim genuine losses to pass; unsupported/OOD economics NOT ESTIMABLE.
4. Implement exact no/partial-fill, fee/funding/slippage-once/common-terminal
   targets and finite HOLD-versus-management future-value contracts. Keep cash
   uncertainty, joint risk/stress, support and programme external costs distinct.
   No static bootstrap/path count is proof of selected conditional coverage.
5. Implement accepted TARGET B interface: any independently eligible sealed E
   member may be selected; Python owns all hard numerical/permission gates.
   No argmax veto, arbitrary 10% band or failure fallback. Current exact E needs
   a timely bound response; pre-trigger analysis is not future unseen authority.
   A/C remain fair research controls with separate feasible evolving ledgers.
6. Freeze tested complete downstream schemas/oracles for Brief dependencies,
   bounded analysis/work receipts, raw versus derived chronology, future news
   rescreens, prospective effort commands, A/P/O/W ownership/status projections,
   exact finite management and UI/IPC. Manual-policy acceptance is not assumed;
   P2 owns lifecycle implementation and its explicit pending activation gate.
7. Freeze actual time-major target/covariate/horizon/quantile/units/manifests for
   TiRex-2, Kronos-mini/tokenizer and Chronos-2-small. No fixture callback or flat
   heterogeneous tuple qualifies real inference. One promoted mean, no voting.
8. Freeze ChartEvidenceBundle, strategy-specific neutral questions/numerical
   predicates, actual image-byte transport contract, cache/bytes/deadline bounds
   and target local-versus-supplied-data-remote benchmark. Define metrics, pass
   limits, privacy/spend/allowed dependencies and objective selection rule so
   Luna's P3 harness/one selected renderer cannot invent safety semantics.
   DeepSeek preference does not waive route capability tests; existing Gemma
   transport remains reusable. No two compulsory production vendors.
9. Preserve quarantined Path B, S1-S8 roles, causal analogues/features/memory and
   all existing useful source/safety/desktop capabilities. Outcome due_work and
   watch ops_outbox are not generic agent queues. Define smallest namespaced
   research-task projection only if existing ledger cannot represent scope.
10. Preregister whole-policy mean/support/selection uncertainty, matched A/B and
    optional C, no-image/image and model ablations, search/holdout/analysis looks,
    costs/tails/materiality/power. Keep actual, shadow and counterfactual grades
    separate; no positive economic/capital claim without real future evidence.

RED-TO-GREEN / NON-DEGRADATION
Rank-one negative/rank-two positive; fourth-plus useful action; missing row.
99 times +1/once -200 cannot become positive-mean admission from robust location.
No new successful evaluation depends on selected_candidate_id; no fit/search on
dispatch deadline. Future/immature/overlapping/revised/unsupported inputs refuse.
Fees both legs, signed funding boundaries, no/partial fill and slippage once.
Exact action/set/epoch change invalidates prior response/approval. Old golden
hashes/readers remain unchanged; never relabel a Huber artifact as a mean.
Test lower-ranked eligible B choice succeeds, failed/outside-E choice refuses,
late/duplicate/changed-set/ref/tool/holdout requests cannot gain authority.
Supply exact numerical fixtures for P2/P3/P4; Luna cannot fill semantic gaps.

VERIFICATION AND HANDOFF
Run focused changed-seam tests and actual consumers, V1 invariant/golden subset,
prefix/causal/saved-reader tests, required Ruff/compile/baseline-aware mypy,
dependency/lock/secret/diff and freeze checks. Use repository installed lock;
do not install all heavy frameworks or call providers to manufacture a pass.
The prior selective audit's 69 pass/11 failures is neither a green full suite
nor proof every failure is harmless; R9 timing/storage/mypy gates stay visible.
Broaden tests only for actual changed surface or required session gate.

Root coordinator alone updates existing log.md and appends translog.md without
rewriting history. Record exact start/commit/source/test/artifact hashes, actual
counts and evidence grades, unresolved owner/provider/venue/hardware/economic
gates, migration/rollback/consumer inventory and P2/P3 tested interfaces.
Commit only authorized P1 scope, review staged diff/secrets, push and independently
verify exact remote SHA. Do not merge/deploy/self-accept. Stop for independent
ChatGPT review of actual pushed diff/evidence before dependent session.
```

## Remaining model-only sessions

P2 Sol owns capital/ownership/native lifecycle and manual-policy tests. P3 Luna receives accepted executable P1/P2 meanings and does mechanical adapters/UI/packaging only. P4 Sol owns real causal source/scanner/Brief/provider/tool/forecaster/memory integration. P5 fresh Sol independently reviews actual code, corrective integration and exact-build qualification. Full scopes and exit gates are IMPLEMENTATION_PLAN.md. Later prompts must use actual accepted predecessor SHAs/interfaces, not invented future outputs. A justified sixth correction is preferable to false completion.
