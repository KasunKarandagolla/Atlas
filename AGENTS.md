# ATLAS RED — repository instructions for Codex and GitHub-backed reviewers

These instructions apply to ATLAS work in this repository, including Codex Cloud. This file governs **workflow and source lookup**, not trading permission. A newer GitHub document or commit is not automatically accepted operational authority.

## 1. Start from verifiable GitHub state

1. Discover the current remote branches, HEAD SHAs, ancestry, actual diff, root `log.md` latest checkpoint and relevant append-only `translog.md` history. Preserve dirty checkouts; use an isolated branch/worktree. **The default branch `impl/session-001-foundation` is historical**, not automatically the live implementation.
2. Open [docs/project/CURRENT_STATE.md](docs/project/CURRENT_STATE.md) for the latest **recorded** accepted-target SHA, implementation candidate SHA, pending approvals and next gate. Confirm its claims against actual GitHub refs; this pointer is not a substitute for evidence.
3. Read [algorithms/README.md](algorithms/README.md) from the independently accepted design SHA and the domain-relevant sections of `ATLAS_SYSTEM_DESIGN.md`, `DESIGN_DECISIONS.md`, `REQUIREMENTS_AND_ACCEPTANCE.md`, `IMPLEMENTATION_PLAN.md`, `SURGICAL_CHANGE_MAP.md`, `AUTHORITY_RECONCILIATION.md`, exact manual policy and proposed operative amendment. Check `MANIFEST.sha256` and the acceptance record before trusting a new target.
4. **Historical frozen authority stays intact:** before substantive design/review/implementation read these original GitHub files **in full**, verify SHA-256 and protect their original meanings:
   - `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` — `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`.
   - `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md` — `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`.
   - `docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md` — `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.
   They govern historical contracts, compatibility, replay/rollback and unchanged current authority; **they are not a mandate to implement inferior old algorithms instead of the independently approved newer target**. Target design alone does not activate a conflicting policy.
5. Original 12 October owner decisions and ADR-001–011 are indexed/reconciled in `algorithms/AUTHORITY_RECONCILIATION.md` and `SOURCE_AUTHORITY_MANIFEST.json` but are **not all present verbatim in this public repo**. Never claim to have read unavailable originals. Where exact originals are necessary obtain owner-approved private read-only access; never publish private source packets here.

## 2. Engineering target, evidence and authority

- The last **independently accepted design publication** at this workflow migration is `f9f8ef083ba5bafcad9bab4d07227676200a948f`; the unchanged R9 runtime ancestor is `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`. Re-evaluate these after every later pushed candidate, review and explicit acceptance. Use the actual accepted target for proposed behavior and **actual code/tests** for implementation truth.
- One complete Binance-first research/execution system: broad mandatory ten-second observation; causal S1–S8 and exploratory research; adaptive independently risk-sized exact-action economics; qualified bounded AI choice only from eligible actions; BTC/ETH/peer, registered tools, news and chart intelligence; durable memory, advanced management; account ownership, one durable dispatcher and independent protection/recovery. Preserve historic V1/Bybit comparators and readers.
- Keep separate: **DESIGN ACCEPTED**, **CODE IMPLEMENTED/TESTED**, **PROVIDER/VENUE/HARDWARE QUALIFIED**, **OPERATIVE POLICY ACCEPTED/ACTIVATED**, and **CAPITAL/ECONOMIC APPROVED**. Do not let a publication, mock, synthetic smoke, green unit test or promising forecast cross these gates. Report IMPLEMENTED / TESTED / UNVERIFIED / TEST GATE / NOT ESTIMABLE / BLOCKED BY ENVIRONMENT.
- Current manual-account aggregate/adoption policy needs explicit **owner concurrence**; proposed `ATLAS_RED_ELIGIBLE_ACTION_COORDINATOR_V1` and companion operating amendments are **NOT OPERATIONALLY EFFECTIVE**. Capital remains **NO-GO**. Never self-approve an amendment, silently set permissions or equate owner approval of coding with live authorization.
- No paid API/generation, authenticated/private venue access, Demo/live orders, account-mode changes, deployment or capital without separate explicit authorization. Offline network-fenced fixtures are default. Do not store secrets, account IDs, private raw logs or private owner source bytes in this **public** repo.

## 3. Model-separated Codex Cloud delivery

- Follow the newest independently accepted [implementation plan](algorithms/IMPLEMENTATION_PLAN.md), currently a **forecast**: P1 GPT-6.1 Sol → P2 GPT-6.1 Sol → P3 GPT-6 Luna → P4 GPT-6.1 Sol → P5 fresh GPT-6.1 Sol. Exactly one verified model per session **including subagents**; if unverifiable, use none. No substitution without owner consent.
- Reserve Sol for mathematical/economic/AI-authority, risk/capital/account, lifecycle/protection and difficult real integration. Give Luna only tested, semantics-frozen bulk integration, render/UI/IPC, packaging and routine testing. Do not force incomplete high-risk work into Luna to save cost.
- No code edits without owner-approved scoped implementation work. Surgical reuse, actual tests, source chronology, prior saved readers and replay matter more than session count. Commit, push, independently verify tip, then independent ChatGPT review of **actual diff, code and tests** before dependent work. Codex cannot self-accept or auto-merge.

## 4. Keep the *live documentation* synchronized with each meaningful push

For **every significant implementation checkpoint**, in the same reviewable pushed change:
1. Update root `log.md` (preserve earlier entries) and **append** significant transitions to `translog.md` without rewriting history.
2. Update `docs/project/CURRENT_STATE.md` with exact pushed candidate reference, scope, measured checks and failures, outstanding gates, accepted target pointer and next review. A commit cannot embed its own SHA; record its actual SHA in the subsequent independent acceptance record/pointer update.
3. Update the affected rows of `algorithms/ACCEPTANCE_REGISTER.csv` and corresponding requirements/progress/evidence links. Mark only tests actually run and evidence actually observed; the handoff is never its own acceptance.
4. Update the implementation plan or code/source-reuse map **only when materially affected**. If a design/policy requirement changes, update the canonical algorithm, acceptance mapping, proposed versioned amendment (as needed), generated PDF and `MANIFEST.sha256` together; verify their exact content. Do not regenerate all design documents for routine fixes.
5. Attach reviewable sanitized test/provenance/migration/rollback evidence, record actual commands/counts, baseline failures, and inspect the staged diff for secrets and unrelated deletions. Push, verify the **remote** commit. A subsequent independent reviewer records acceptance or rejection against that exact SHA, never by editing history or approving an unexamined later tip.

Only the root coordinator updates `log.md`. Subagents submit evidence to it. Maintain old failures as historical evidence, not today's certification. If GitHub or primary source/permissions are unavailable, say so and stop at the relevant gate.
