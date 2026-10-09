# S41 paused review snapshot — blockers and issues

**Purpose:** independent review of the paused Session 041 working tree. This is a blocker inventory, not a release/readiness claim. The full-V2 engineering gate is open. No installer was built and no owner live campaign was run.

**Snapshot:** branch `impl/session-041-final-full-v2-engineering-closure`, base HEAD `a01f343f7c6be005e6d92450997bd1bbd0344800`, with the uncommitted S41 tree that will be captured by the temporary review branch. No final tested SHA exists. All S41 test reports cited here are development-tree evidence unless explicitly stated otherwise.

## 1. Immediate stop: broad stream and report capacity

The synthetic 1,024-contract workload used a single active Bybit frame producer, an idle Binance stream lane, and concurrent read-only report export. No tested capture batch size satisfies both sustained ingestion and the frozen 1.5-second maximum service-gap limit:

| Candidate | Observed outcome | Evidence |
| --- | --- | --- |
| 16 frames per capture descriptor | Preventive capture-pressure stop after 1,419 frames; 209 delivered, 1,210 pending, 62/64 pending batches. Maximum measured service call was 0.881 s; backlog still grew to the safety stop. | [`7bde944…xml`](docs/v2/session041-evidence/7bde94433bf3f5bea6482cd82075d32ddbc786f6fb04b1a02c53822491f719d8.xml), SHA-256 `7bde94433bf3f5bea6482cd82075d32ddbc786f6fb04b1a02c53822491f719d8` |
| 16 frames, shared lane reservation enabled | A frame was rejected at 480/512 handoff items because 32 items remained reserved for the idle Binance lane. This was explicit and fail-closed. | [`7a9970…xml`](docs/v2/session041-evidence/7a9970f8b745a06112abcb5cac08f2ea66a2ffbd152260867ee6ede364686b3e.xml), SHA-256 `7a9970f8b745a06112abcb5cac08f2ea66a2ffbd152260867ee6ede364686b3e` |
| 32 frames per descriptor | All 1,791 synthetic frames were offered/retained and report export completed, but maximum service gap was 2.399 s and one service call lasted 2.310 s, exceeding the frozen 1.5 s limit. | [`c4c9ac…xml`](docs/v2/session041-evidence/c4c9ac0bc3287d073f9f118619457102f2faf3a07366389b1f6146f6a2319cff.xml), SHA-256 `c4c9ac0bc3287d073f9f118619457102f2faf3a07366389b1f6146f6a2319cff` |

The source retains the 16-frame setting. The 32-frame experiment was reverted. The global 512-item/16 MB queue, capture descriptor ceiling, durability behavior and service SLO were not raised or relaxed. The reported synthetic run does not establish live venue behavior, and it did not have Binance frames flowing. A capacity/storage architecture review is needed before this gate can be resumed; buffer growth or a looser timing threshold would not resolve it.

## 2. Ordinary engineering work still open

### 2.1 Requirement and authority accounting

- The requirement matrix contains 2,592 source clauses. Current matrix counters show **all 2,592 `UNVERIFIED`**, all 2,592 with `UNCLASSIFIED_PENDING_IMPLEMENTATION_INSPECTION` gap labels, and no final per-row resolution SHA. Candidate file/test paths are navigation hints, not proof.
- The authority audit says 846 rows require root review; the matrix has 840 `AUDIT_PENDING` scope labels; the readiness draft says 843. Reconcile these counts before review acceptance. The S41 authority pass resolved some scope labels only; it did not establish implementation/test completion.
- The owner-root amended V2 freeze is recorded with SHA-256 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, but no pre-S41 accepted comparison copy was found. No material conflict was found; byte identity to the previously accepted copy remains unverified.
- The status-only current-state document was not found/read after the recorded searches. Its absence does not change authority, but its checkpoint facts remain unavailable.
- The authority and requirement files are drafts; the source-hash/line-span audit and coordinator classifications do not close their implementation requirements.

Relevant records: [`SESSION041_FULL_V2_REQUIREMENT_MATRIX.json`](SESSION041_FULL_V2_REQUIREMENT_MATRIX.json), [`SESSION041_AUTHORITY_CLAUSE_AUDIT_V2.json`](docs/v2/SESSION041_AUTHORITY_CLAUSE_AUDIT_V2.json), [`SESSION041_REQUIREMENT_ACCOUNTING_FINDINGS.json`](docs/v2/SESSION041_REQUIREMENT_ACCOUNTING_FINDINGS.json).

### 2.2 Full-universe capacity, lifetime storage and preflight

- A 4,096-instrument synthetic bulk-only run completed three cycles in 781 seconds and persisted 24,576 exact raw/index records. It did **not** include continuous streams, full enrichment, concurrent report/export, WAL/read contention, restart endurance, or the combined resource envelope.
- Maximum-breadth repeated acquisition with streams and enrichment remains unqualified. Full-workload storage admission/preflight, free-disk behavior and a hard reserve stop remain open.
- The 1,024-contract concurrent-export gate fails as described above. No broad-scale sustained stream fairness test with persistence stalls has passed.
- A public-resilience review on an earlier checkpoint reported a possible unbounded run-lifetime growth path: repeated 4,096-product polling appends immutable product observations, universe/workset artifacts and bulk market rows. It also reported that a legal maximum-size workset could exceed the broad-export 8 MiB payload ceiling. These findings need explicit revalidation against the latest S41 tree and disposition; the focused bulk test does not resolve them.
- The focused broad-export test report records 22 tests passing and a separate wrapper report records 3 passing, but these are not proof of maximum legal workset export during continuous full-breadth ingestion.
- The S40 storage preflight and pressure UI exist, but are not proven to admit/reject the complete final V2 workload before a long run. The current broad test failure also means operator pressure behavior has not passed at the target workload.

Relevant records: [`SESSION041_FULL_V2_RESILIENCE_VALIDATION.json`](SESSION041_FULL_V2_RESILIENCE_VALIDATION.json), [`SESSION041_DYNAMIC_UNIVERSE_VALIDATION.json`](SESSION041_DYNAMIC_UNIVERSE_VALIDATION.json), [`SESSION041_PUBLIC_EVIDENCE_CAPACITY_AUDIT.json`](docs/v2/SESSION041_PUBLIC_EVIDENCE_CAPACITY_AUDIT.json), [`SESSION041_REVIEW_PUBLIC_RESILIENCE.json`](docs/v2/SESSION041_REVIEW_PUBLIC_RESILIENCE.json), [`SESSION041_EXPORT_REVIEW.json`](docs/v2/SESSION041_EXPORT_REVIEW.json).

### 2.3 Public Bybit/Binance behavior and dynamic universe

- Dynamic dual-venue universe code and 4,096-product fixture paths exist, but the dual-venue capability matrix still marks all public evidence classes `UNVERIFIED` and `source_contract_review_complete=false`.
- Offline synthetic tests do not establish current venue product populations, listing/filter/status transitions, source health, history coverage, WebSocket continuity, or actual recovery behavior. Actual venue breadth and source qualification remain future live gates after offline engineering closes.
- The source-complete integration must still account for venue-specific semantics for bars/bootstrap/gap repair, aggregate trades, tickers, mark/index, funding/history, OI/history, depth/book, snapshot recovery, receipts/availability, metadata chronology and source capability.
- The S41 synthetic capacity run had no Binance frames; simultaneous real public collection across both venues under mixed load has not been demonstrated.

Relevant records: [`SESSION041_DUAL_VENUE_CAPABILITY_MATRIX.json`](SESSION041_DUAL_VENUE_CAPABILITY_MATRIX.json), [`SESSION041_DYNAMIC_UNIVERSE_VALIDATION.json`](SESSION041_DYNAMIC_UNIVERSE_VALIDATION.json).

### 2.4 Strategy, event and intelligence integration

- S1–S8 and the deterministic/intelligence modules have code paths and focused tests, but the exact authority, causal inputs, missingness behavior, evidence lineage and integrated production role are not closed clause-by-clause. Candidate code existing is not proof of the complete authorized surface.
- The continuing closure record still lists durable S4–S8 queue integration/atomic surface publication and recovery, production exact-action M0/M1/analogue integration, and real persisted-model-path coverage as open/revalidation work.
- Official event-source ingestion and production event gating must be distinguished from calendar/scanner code. Event abnormality production and the authority decision for the unspecified volatility criterion remain unresolved in the continuing findings.
- S7 event extraction is authorized as a zero-authority path, but its separate Windows broker operation needs a versioned protocol-v2 operation or dedicated endpoint. That additive transport contract and Windows-host behavior are not qualified here.
- Final proofs that CandidateSet is complete, selection deterministic, risk alone sizes, M0 chronological, M1 OOF/chronological, support/OOD/scenario/stress/ES/admission integrated, and frozen action immutable remain absent for the final source SHA.
- DeepSeek critic zero authority and Discovery Lab quarantine remain the intended contracts, but final provider/process isolation tests and independent review on the integrated source SHA are pending.

Relevant records: [`SESSION041_CONTINUING_CLOSURE_FINDINGS.json`](docs/v2/SESSION041_CONTINUING_CLOSURE_FINDINGS.json), [`SESSION041_STRATEGY_INTELLIGENCE_MATRIX.json`](SESSION041_STRATEGY_INTELLIGENCE_MATRIX.json), [`SESSION041_SECURITY_AND_AUTHORITY_REVIEW.md`](SESSION041_SECURITY_AND_AUTHORITY_REVIEW.md).

### 2.5 Binance execution review and immutable run configuration

- The recorded offline Binance execution/recovery suite passed 115 cases with zero skips; selected-demo binding integrity was separately exercised in 13 cases. No authenticated exchange calls were made. The report explicitly leaves actual authentication/account identity, IOC/fills, cancel races, protection, reconciliation, restart/history, writer fencing and emergency flatten for demo/test qualification.
- Binance opening remains a fixed TEST GATE; passing deterministic mocks is not account/protection qualification.
- The Binance qualification artifact says a fresh independent review against a stable integrated SHA is still `UNVERIFIED`.
- A coordinator review/watch-list in `SESSION041_REQUIREMENT_RESOLUTIONS_FG.json` also calls for explicit disposition/revalidation of Binance client-ID adapter encoding, authenticated account/margin-mode binding, sent-command reconciliation into durable terminal outcomes, order-type-specific market filters, and adapter-derived serialized close payloads. Because these notes may predate later S41 edits, reviewers should confirm each against the current source rather than assume either resolved or currently defective.
- Selected execution-venue/account/environment identity, single-writer fencing and credential-reference UI exist in the worktree, but the full production/native owner configuration path has not passed final stable-SHA and Windows validation.

Relevant records: [`SESSION041_BINANCE_EXECUTION_QUALIFICATION.json`](SESSION041_BINANCE_EXECUTION_QUALIFICATION.json), [`SESSION041_REQUIREMENT_RESOLUTIONS_FG.json`](docs/v2/SESSION041_REQUIREMENT_RESOLUTIONS_FG.json), [`SESSION041_FINAL_LIVE_TEST_READINESS.md`](SESSION041_FINAL_LIVE_TEST_READINESS.md).

### 2.6 Cross-cutting validation and review

- No final code SHA or final tested SHA is assigned. The validation ledger and closure artifacts explicitly say the gate is open.
- Full current-tree V1 golden/contracts, complete V2 suite and complete non-V2/V1 regressions have not been run as one source-stable qualification. S41 changes shared SQLite journal/migration/writer-lock behavior; the recorded independent review specifically requires V1 duplicate-receipt/idempotence, POSIX/Windows writer exclusion, restart and Phase 0–6 regressions.
- All ten requested fresh independent review domains are not yet completed against one stable final SHA. Existing focused/older checkpoint reviews do not substitute for the integrated review.
- Final Ruff/mypy/compileall/dependency and portable/native lock/freeze-hash checks, repository/build/log/report secret scans, and generated-artifact secret scan remain pending as a complete final gate. An earlier F/G resolution record documents a mypy invocation with nine errors in `production.py` around lines 5712–5818; it predates later edits and must be rerun, not silently treated as either current or fixed.
- The final native Windows package build and install/launch/preflight/run/export/stop/restart/reinstall/uninstall/preservation checks cannot be qualified from this Linux environment. No package has been made.

## 3. External, environment and scientific gates (not substitutes for missing engineering)

| Classification | Remaining proof |
| --- | --- |
| **TEST GATE** | Authenticated Binance DEMO/TEST behavior and Bybit DEMO/TEST behavior; actual account mode/product filters/order outcomes/cancel-fill races/protection readback and repair/restart/reconciliation/flatten; actual venue metadata populations and source health; approved provider/Windows broker behavior; owner network, restart and endurance drills. No credential was accessed or exposed in this engineering continuation. |
| **BLOCKED BY ENVIRONMENT** | Native Windows packaging/install/DPAPI and end-user lifecycle qualification. This Linux run does not establish Windows readiness. |
| **NOT ESTIMABLE** | Profitability and qualifying prospective economic evidence. The frozen floor still requires genuine prospective duration/opportunity/regime and dependence-aware evidence; code-time or 48/72-hour engineering runs do not replace it. |
| **DEFERRED_BY_FREEZE** | Automatic cross-venue fallback/routing, cross-venue arbitrage, simultaneous multi-venue capital, S8 joint-basket capital authority, model-controlled risk/sizing/stops/lifecycle, chart vision and decision-affecting agent promotion. |

The product invariants remain `capital_enabled=false`, `assisted_enabled=false`, critic authority ZERO, Discovery Lab quarantined, and economics `NOT ESTIMABLE`. No profitability or live qualification claim is made.

## 4. Review sequence requested

1. Resolve the 16-vs-32-frame capacity tradeoff without changing frozen limits; then rerun concurrent export, sustained stream/backpressure, WAL/storage, restart and resource tests.
2. Re-review the earlier lifetime-storage/export-size and Binance command-contract findings against the exact current tree; record which remain open and close each with code/test evidence where required.
3. Resolve all 2,592 clauses, including the 840/846/843 audit-count discrepancy, on the governing authority; attach implementation, exact assertions, gap class and evidence per requirement.
4. Run full source-stable V1/V2 and static/security/dependency/lock gates; complete independent reviews on that same SHA.
5. Only then assess engineering closure. Native Windows validation remains separately required before a final installer can be called ready.

## 5. Snapshot evidence index

- [`SESSION041_HANDOFF.md`](SESSION041_HANDOFF.md) — branch/checkpoint, prior focused evidence and current pause point.
- [`SESSION041_FULL_V2_ENGINEERING_CLOSURE.md`](SESSION041_FULL_V2_ENGINEERING_CLOSURE.md) — concise closure status and mixed-load capacity failure.
- [`SESSION041_FULL_V2_VALIDATION_LEDGER.json`](SESSION041_FULL_V2_VALIDATION_LEDGER.json) — current attributed focused evidence inventory.
- [`SESSION041_FULL_V2_RESILIENCE_VALIDATION.json`](SESSION041_FULL_V2_RESILIENCE_VALIDATION.json) — failed capacity candidates and remaining workload gates.
- [`SESSION041_REQUIREMENT_RESOLUTIONS_FG.json`](docs/v2/SESSION041_REQUIREMENT_RESOLUTIONS_FG.json) — detailed F/G clause resolution draft and review prompts.
