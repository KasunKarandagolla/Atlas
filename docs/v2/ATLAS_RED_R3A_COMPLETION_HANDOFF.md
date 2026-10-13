# ATLAS RED — R3A Completion Handoff

Date: 2026-10-13 UTC

## Status

This session delivered bounded production changes and verified the focused consumers. It does not qualify the complete production intelligence subsystem. The R2 service-cadence regression remains a blocking TEST GATE because the reported historical red was not reproduced and its original raw failure is unavailable. Whole-schedule coverage and invalidation evidence also remain gates.

| Area | Status | Result |
|---|---|---|
| Typed production facts | IMPLEMENTED / TESTED | Verified M15 technical facts can enter the production Decision Brief through an indexed, causal reader. Other production fact families remain unavailable. |
| Decision Brief | TESTED / BLOCKED | Genuine facts and selected A0 references persist. The Brief remains BLOCKED while required typed sections and exact supported economics are absent. |
| Prior-analysis reading | IMPLEMENTED / TESTED | Exact-scope COMPLETE projections can be surfaced as historical context. No current analysis qualifies, so the current result is NO_QUALIFIED_PRIOR_ANALYSIS. |
| Memory invalidation and all replay cases | TEST GATE | Explicit append-only invalidation evidence and complete watch, news, policy, and due-trigger replay coverage are not implemented. |
| Whole-schedule calendar denominator | TEST GATE / NOT ESTIMABLE | The registered production scanner schedule population is unavailable. Receipt-only observations do not prove a complete schedule. |
| Optional projection boundary | IMPLEMENTED / TESTED | Projection runs post-cycle on the supervisor’s existing writer, has explicit effort and output bounds, records bounded sanitized failures, and leaves sealed A0 artifacts unchanged. Sustained overlap service qualification remains TEST GATE. |
| R2 public service cadence | TEST GATE | Five controlled trials each on R2 and R3A passed. The previously reported approximately 463 ms failure was not reproduced; its original raw process output was unavailable. |
| Full repository qualification | TEST GATE / BLOCKED BY ENVIRONMENT | The required full suite was not run because the rolling 24-hour filesystem-write total cannot be established safely. |
| Runtime model identity | UNVERIFIED | The owner selected GPT-6 Luna. This tool surface does not expose an independently verifiable exact runtime model identity. No subagents were used. |

## Starting checkpoint and source authority

The isolated worktree was created on fix/red-r3a-completion-2026-10-13. Its verified starting remote was impl/red-r3a-production-brief-memory-2026-10-13 at 3f675572797e88850997d32bdcb96226360ff174. The implementation ancestry is 7a425ba410ab246ab1ce2b5136768b88594b74b8, with accepted R2 ancestor 67442118d5ffef818541e1b6f83d7fa6bae45293. The implementation branch tip tested and pushed is 8fbf1ec31588a1c315acce048176066b4c6bbecc. Both accepted R3A and R2 ancestry checks pass. The owner’s local Red reference and the shared main checkout were left untouched.

The read-only owner ZIP contains 23 files and has SHA-256 bd3e258007ab6cc8ceee7d8b564320e4acc0ed86066fc6a6c65e0947fad8b825. Its full file inventory is in transition-r3a-completion-evidence/source-preflight.json. The two absent administrative files are 01_CHATGPT_PROJECT_INSTRUCTIONS_COPY_PASTE.md and 02_NEW_PROJECT_FIRST_CHAT_KICKOFF.md. No ZIP content was added to production source.

The three required freeze hashes match:

| Authority | SHA-256 |
|---|---|
| ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md | c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c |
| ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md | e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78 |
| ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md | d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d |

The target design lock, transition architecture, owner decision record, ADR-001/002/005/006/007/008, runbook sections 2/3/4/5/7, repository R3A handoff and evidence manifest, R3B amendment acceptance delta, and S47 comparative proposal were read. Their repository paths and hashes are recorded in source-preflight.json. The S47 amendment remains PROPOSED — NOT ACCEPTED; effective policy identity is NONE. The target design does not supersede frozen authority.

## Implementation and call paths

The production implementation is in commit 0a47e0d971c3e785825c29bc9c9a29ad43b36121. Follow-up implementation-branch commits 52720b4522fc3aa2a04d233f23edd7fec7532461 and 8fbf1ec31588a1c315acce048176066b4c6bbecc add fixture typing/format and persisted-source restart verification. Tests and final equivalence were run against the branch tip 8fbf1ec31588a1c315acce048176066b4c6bbecc.

The first documentation/evidence receipt commit, 60e9ce947aabdca35b788c2602415804c55ad5be, contains only documentation and evidence. GitHub returned that exact branch tip through git ls-remote. A final documentation-only receipt updates this verification record and the inventory; it does not change the tested implementation SHA.

Changed files:

- src/atlas/v2/research/production_facts.py — registered indexed M15 technical fact reader.
- src/atlas/v2/research/decision_brief.py — typed fact admission into the existing compiler.
- src/atlas/v2/runtime/production_intelligence.py — production evidence collection, causal reader call, bounded prior and due-work reads, projection body, and availability references.
- src/atlas/v2/runtime/ops_supervisor.py — post-cycle optional projection budget and bounded failure receipt handling.
- src/atlas/v2/research/memory.py and src/atlas/v2/memory/repository.py — existing projection persistence and bounded prior-artifact lookup support.
- tests/v2/test_session053_production_completion.py — red-to-green production-fact, restart, A0 equivalence, failure, deadline-budget, and transaction rollback regressions.

The supervisor seals its deterministic cycle and runs mandatory source service and ordinary cycle work before invoking the optional projection hook. Projection executes synchronously on the existing supervisor/repository writer. It creates no writer thread. The existing disable-production-intelligence rollback remains available.

### Supported production evidence

read_production_feature_facts selects only the existing A0 candidate and the exact universe product revision. It reads indexed FeatureArtifactV2 and source-health evidence, verifies typed identity and hashes, checks availability and effective availability against the event cutoff, and reconstructs source ancestry from the repository after restart. The admitted feature policy is explicitly registered. RSI14, ROC10, and ADX14 values are recomputed from completed M15 bars and checked against persisted feature values. The bounded reader allows at most 256 bars and 512 feature input references; gap, warmup, stale, future, unfinalized, late-revised, wrong-hash, missing, and wrong-product evidence does not produce a qualified number. Missingness remains explicit.

The Decision Brief therefore contains genuine technical facts when those checks pass, and typed source-health state and references from the existing causal receipt. The production test verifies one available technical fact and its replay from persisted candidate, universe, bar, feature, and source-health records after reopening SQLite.

Other requested fact families do not have verified production readers in this change: market structure, cached BTC/ETH and peer context, liquidity, flow, derivatives/funding, persisted news/events, watch transitions and invalidators, matured outcomes, and qualified M0/M1/scenario/stress/ES or ML forecasts. Those sections remain missing or NOT ESTIMABLE. ML has no qualified installed checkpoint/manifest/calibration/input support in this path, so no neutral or copied prediction is emitted.

The adapter retains the existing independently verified selected A0 action/evaluation references and attempts to recover economics for that exact action from indexed production evidence. It can qualify economics only when the original evaluation and all required support resolve. The tested fixture has no qualified exact-action economics, so expected net value and expected PnL lower confidence bound stay NOT ESTIMABLE. Candidate ranking is identified as PRELIMINARY_ONLY_NO_AFTER_COST_INFERENCE. M1 and analogue stage references are preserved, with qualification set to NOT_ESTIMABLE_TYPED_DIAGNOSTIC_NOT_RECONSTRUCTED. The compiler reports BLOCKED while required sections remain unavailable.

### Durable memory and due work

The prior-analysis reader examines at most 32 indexed production Brief projections. It requires immutable projection and memory identity, valid content hashes, COMPLETE status, exact slot/snapshot/cutoff/candidate set/universe/selected candidate/action/research policy/schema scope, and every registered source’s availability by the current cutoff. A match is labeled QUALIFIED_HISTORICAL_CONTEXT and explicitly carries historical_only=true and current_advice=false. No persisted prior is eligible while current production Briefs are BLOCKED; the normal result is NO_QUALIFIED_PRIOR_ANALYSIS.

Due work is read from the existing RESEARCH lane by exact applicable slot, snapshot, cutoff, deadline, and candidate scope. The projection preserves indexed source identity and original created/due times. It does not create paid-provider work, a new model dispatch, generic outbox jobs, or a second queue. No retention pruning was performed.

Projection identity and persistence are transactionally idempotent. Tests cover repeated failed projection attempts capped at two failure receipts, sanitized errors, unchanged A0 state, pre-commit rollback, process reopen, and successful retry without duplicate Brief/memory identities. They do not establish all requested event/watch/news/model invalidation transitions, immutable append-only invalidation evidence, duplicate due-trigger replay, or outcome-maturity cutoffs. Those remain TEST GATE. A completed immutable result is not rewritten to a later timestamp.

### Decision-calendar accounting

The current Brief truthfully records a one-receipt population only. Its whole_schedule_denominator_status is UNAVAILABLE, its source is CURRENT_OPS_SUPERVISOR_RECEIPT_ONLY, and missing_expected_slots is NOT ESTIMABLE, with reason NO_REGISTERED_SCANNER_SCHEDULE_POPULATION.

The available decision-calendar artifacts describe surviving event receipts and their A0 outputs. They do not independently prove the expected complete scheduled-slot population for the production scanner. No complete-looking schedule was inferred from those receipts. A missed slot without recoverable source evidence remains UNKNOWN/unqualified. Whole-policy denominator conservation, explicit missing-slot indexing, universe/scanner-version partitioning, and restart-stable finite schedule tests are not implemented and remain a blocking TEST GATE.

### Projection budget and failure behavior

The default projection effort limit is 80 ms; configuration is restricted to 1–100 ms. The Brief limits are 256 rows, 256 instruments, one million UTF-8 bytes, zero image references, and at most 4096 source references. Technical fact reconstruction also has explicit bar/reference limits. Budget checks occur during collection and before persistence. A projection over budget or output bounds defers or records BLOCKED; it never changes a sealed A0 artifact. Failure receipts store bounded sanitized exception type/reason and at most two attempts per receipt identity. Restart can replay an incomplete projection on the same immutable receipt.

Synthetic production-path measurements covered 457 indexed feature inputs (192 M15, 100 H1, 100 H4, and 64 auxiliary inputs), three candidates, 486 source references, one emitted fact, and a 186,429-byte persisted Brief body. At the default 80 ms, measured trials were 83.439, 92.584, and 85.026 ms and each was deferred. At 100 ms, three trials took 87.914, 100.201, and 83.117 ms; two persisted a BLOCKED Brief and one safely deferred. A profiled trial measured 84.406 ms wall time, 84.294 ms process CPU, and 56,564 KiB peak RSS. SQLite after close measured 4,186,112 bytes in a successful trial and 3,932,160 bytes in a deferred trial.

These measurements demonstrate bounded failure behavior at this synthetic size. They do not qualify all 256 instruments, 256 Brief rows, maximum output bytes, sustained active-source overlap, or service cadence under continuous capture. The 80 ms limit was exceeded in all three default-budget trials, and one 100 ms trial exceeded its limit. Maximum permitted effort and mandatory service under overlap remain TEST GATE.

## A0 compatibility

Cross-commit fixtures compared R2 67442118d5ffef818541e1b6f83d7fa6bae45293, accepted R3A 3f675572797e88850997d32bdcb96226360ff174 with optional projection disabled, and completion tip 8fbf1ec31588a1c315acce048176066b4c6bbecc with projection enabled. Artifact references, content hashes, timestamps, availability, and canonical metadata matched for OpsSupervisorReceiptV1, CandidateSetV2, SizingDecisionV2, ActionArtifactV2, EvaluationArtifactV2, and DecisionCalendarEntryV2. Their fixture counts were respectively 1, 3, 1, 1, 1, and 1. DecisionCalendarV2 had zero indexed rows in all three fixtures. The enabled/disabled integration regression independently compares the full indexed artifact sets. Evidence: transition-r3a-completion-evidence/final-a0-cross-commit-equivalence.json.

No deterministic A0 selection, frozen exact-action identity, sizing, risk, reservation, source-capture, historical hash, queue, recovery, or outcome contract was intentionally changed.

## R2 cadence diagnosis

The exact regression was tests/v2/test_session041_broad_stream_source.py::test_snapshot_stall_preserves_actual_service_cadence_and_causal_adoption[5.0]. It directly exercises BroadPublicRuntimeV2 and does not instantiate OpsSupervisorV2; the production-intelligence enabled/disabled option is therefore not applicable to this test path.

Five controlled accepted-R2 trials and five controlled R3A trials passed on this host. Instrumented per-trial results and test receipts are in transition-r3a-completion-evidence/cadence/. Each group contains 2,482 measured public runtime service calls. R2 service duration maximum was 1.879 ms and p99 79.499 μs; R3A maximum was 1.518 ms and p99 88.232 μs. Snapshot polling wall maximum was 51.357 μs on R2 and 78.628 μs on R3A. Future completion had five observations per five trials. Archive chunk writes occur before the service loop and measured at most 30.611 ms on R2 and 22.303 ms on R3A. SQLite artifact registration measured at most 1.008 ms on R2 and 0.790 ms on R3A.

The owner-reported historical approximately 463 ms service call against the existing 100 ms limit was not reproduced in these controlled trials. Its original raw failure log was unavailable. No causal production bottleneck was demonstrated, and host scheduling noise is not established. The regression is unresolved and remains a blocking TEST GATE; no timeout or assertion was relaxed.

## Regression and static verification

Final implementation tip 8fbf1ec31588a1c315acce048176066b4c6bbecc passed 563 test executions across these sequential groups:

| Group | Tests | Result |
|---|---:|---|
| R3A production completion, R3A integration, S47 Brief/memory/agent/peer, S48 | 109 | Passed |
| Production lifecycle, supervisor/restart, S33/S37 due work and outcomes, S44 M15, S46 peer, S48 protection | 177 | Passed |
| R1 source qualification, chronology, historical V1/Phase-4/golden and archive consumers | 112 | Passed |
| R2 capture receipt/durability/delivery/pressure/public stream/recovery matrix | 63 | Passed |
| R2 mixed broad capacity, 1,024 contracts | 1 | Passed |
| R2 public runtime and Binance qualification | 40 | Passed |
| S45 content-hash/outcome maturity/resolution consumers | 61 | Passed |
| Total | 563 | Passed |

Each exact pytest command, source SHA, return code, duration, RSS, and measured filesystem-write delta is in final3-regression-resource-summary.json. JUnit XML and compressed raw console output are retained beside it.

Red-to-green and rerun records:

- On the accepted R3A baseline, test_genuine_indexed_feature_enters_production_brief_without_changing_a0 failed because no production technical fact was present. The raw red output is preserved losslessly as red-feature-baseline.junit.xml.gz and red-feature-baseline.log.gz. The test passes on the final tip.
- An earlier 177-test calendar/lifecycle run had two missing-NumPy failures. The locked offline-research dependency was enabled; the exact two tests passed, and the final 177-test group passed.
- An earlier 177-test calendar/lifecycle run with a long temporary socket path had one environment path-length failure. The exact failing socket test passed with a short temporary root; the final 177-test group passed with bounded short roots. Both initial failure receipts and rerun JUnit are retained.
- All ten controlled exact R2 cadence trials passed; the historical reported red is unavailable and unresolved as described above.

Static checks at the final source state: Ruff lint passed; compileall passed; the two new files pass Ruff format; uv lock --check --python 3.12 passed; locked package consistency passed; git whitespace checks and the high-signal secret scan passed. Whole changed-file Ruff formatting reports two already-unformatted legacy files, repository.py and ops_supervisor.py; the accepted baseline produces the same two paths. Mypy exits nonzero on seven existing diagnostics versus eight at baseline, with no new normalized diagnostic and one prior optional-universe diagnostic resolved. Exact outputs and hashes are retained under transition-r3a-completion-evidence.

Dependencies were not changed, so dependency and license review was not triggered. The three frozen authority hashes matched before and after implementation.

The full repository suite was not run. Historical repository evidence reports 2,783 collected tests and an earlier run stopped after 1,196; those historical counts are not current passes. Current affected groups are listed above. Complete full-suite regression remains TEST GATE.

## Resource scope and limits

Before expensive suites, the workspace had approximately 31.1 GB free and /tmp approximately 5.08 GB free. Tests used isolated short temporary roots, which reached approximately 1.15 GB at peak and were removed. The final3 runner measured 1,806,336 bytes of filesystem writes across its seven sequential groups. Maximum child RSS across these groups was 475,420 KiB. The container quota was two CPUs and 8 GiB memory. After tests, the workspace had 31,112,272,512 bytes free and /tmp had 4,537,548,800 bytes free. The retained evidence directory is measured in the JSON evidence manifest.

At the post-test resource sample, cgroup io.stat reported 958,930,944 cumulative bytes written for the observed device. That counter is not a rolling 24-hour accounting ledger. Rolling 24-hour filesystem writes are UNVERIFIED, so the 5,000,000,000-byte hard rolling limit cannot be certified. The full suite was not started under this uncertainty. These local synthetic results do not establish sustained Binance delivery or low-resource Linux parity.

## Authority, rollback, and remaining gates

Agent/provider dispatch remains disabled. No DeepSeek, OpenAI, paid provider, credential, broker, account, private venue, order, capital, assisted execution, discretionary selection, rescreening effect, model-dependent action, strategy, or training authority was added. The hidden zero-authority critic and native protection/recovery state remain unchanged. The S47 comparative amendment remains unaccepted.

The existing --disable-production-intelligence switch continues to bypass the optional projection. Deterministic recovery, source observation, protection, A0 processing, and outcome maturity do not require projection completion.

Promotion remains blocked on:

1. Reproducing or causally resolving the historical R2 cadence failure with original evidence or equivalent controlled red evidence.
2. A separately proven registered schedule population, complete missing-slot accounting, and denominator conservation across restart.
3. Explicit append-only memory invalidation and the outstanding memory/watch/news/outcome/due-trigger replay tests.
4. Worst-permitted Brief/source overlap performance, byte/row overflow, deadline-expiry, and compulsory service under active-capture regression evidence.
5. Full repository regression under a verified rolling-write budget, plus sustained-source and target Linux qualification.
6. Independent owner review of the pushed implementation and evidence. This handoff is not self-acceptance.
