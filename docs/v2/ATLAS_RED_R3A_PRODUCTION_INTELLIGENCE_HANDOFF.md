# ATLAS RED R3A production intelligence handoff

**Review checkpoint, not self-acceptance.** This handoff records an additive,
read-only production integration and its evidence.

| Area | Status | Evidence |
|---|---|---|
| Production Brief and memory projection | **IMPLEMENTED** | Real `ProductionOpsCyclePortV1` supervisor lifecycle; details below |
| Focused integration and affected regression groups | **TESTED** | 11/11 R3A tests; S47 83/83; R1 compatibility 112/112; regression ledger below |
| Full repository promotion | **TEST GATE** | Full suite not run; prior 2,783 collection stopped at 1,196 |
| Complete production Brief facts and whole-schedule denominator | **UNVERIFIED** | Production readers for those typed facts and a whole-schedule index are unavailable |
| Economic value, live Binance delivery, provider qualification | **NOT ESTIMABLE** | No R3A provider dispatch or capital/venue qualification occurred |
| Required owner source package and exact model label | **BLOCKED BY ENVIRONMENT** | Owner package and runtime model identity were not available for independent verification |

## Starting point and implementation

- Isolated branch: `impl/red-r3a-production-brief-memory-2026-10-13`.
- Starting accepted R2 remote tip: `67442118d5ffef818541e1b6f83d7fa6bae45293` on `fix/red-r2-mixed-source-delivery-2026-10-13`.
- R2 and R1 implementation ancestors: `910d4001323f4a6136af886df618b99a6c8fb8aa` and `9930fc834022ce3e8f5bc2239bd60867a251f46e`.
- Implementation commit: `7a425ba410ab246ab1ce2b5136768b88594b74b8`.
- Final documentation-only receipt SHA is returned in the delivery response after push verification.

The three frozen authority documents were read and their SHA-256 values match:

- V1 freeze: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`
- Amended V2 freeze: `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`
- Agent intelligence freeze: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`

The current environment did not contain the read-only owner package at the
previously documented `/workspace/attachments/.../Red.zip` or
`/home/kasun/Music/atlas/Red` paths. Therefore `00_START_HERE.md`, target lock,
owner decisions, transition architecture, ADR-002/005/006/007/008, and runbook
sections 2/3/4/5/7 could not be reread here. No missing owner authority was
inferred. Serena and Context7 were not callable. The required runtime model
label could not be independently verified as GPT-6 Luna, so no subagents were
used.

The S47 comparative amendment remains **PROPOSED — NOT ACCEPTED** with
effective policy identity **NONE**. R3A did not modify it or the frozen hidden
critic, event extractor, offline proposer, selector, sizing policy, or frozen
action contract.

## Production call path

The normal built-in path is:

`OpsSupervisorV2.main()` → `_import_port("atlas.v2.runtime.production:create_production_port")` →
`ProductionOpsCyclePortV1.recover/collect/process_event()` → persisted stage
receipts for public evidence, causal features, CandidateSet, deterministic
selection, hard-risk sizing, frozen action, evaluation, diagnostics, and the
decision calendar → sealed `OpsSupervisorReceiptV1` → public stream service
and outcome maintenance → post-receipt projection → durable read-only
`project_snapshot()`.

`BroadProductionOpsCyclePortV2` inherits `ProductionOpsCyclePortV1`, so it uses
the same projection hook. The supervisor only runs the hook for that production
port family. The hook runs after the event receipt, cycle receipt, stream
service, and outcome-maintenance work have been persisted. Exceptions in this
optional path do not change the sealed A0 result or add a cycle failure. The
projection is synchronous after those operations; its added post-cycle latency
was not separately qualified.

The adapter in
[`production_intelligence.py`](../../src/atlas/v2/runtime/production_intelligence.py)
reuses `build_decision_brief()` and the existing production economic recovery
reader. It verifies the sealed receipt and event identity, CandidateSet and
universe hashes, exact event/cutoff, effective source availability, source and
product revisions, and exact-action/sizing identities. It does not recalculate
ranking, sizing, or economics.

R3A adds one post-receipt adapter and uses the existing S47 memory service and
ops writer. `ResearchMemoryV1.persist_production_projection()` atomically
persists a Brief, a memory projection, and an identity artifact through the
authorized ops repository writer. No provider request, due-work item, retry,
fake intent, or dispatch job is created. Existing due work is only read for an
exact slot, trigger, cutoff, deadline, and CandidateSet match.

## Artifact and evidence contract

For each immutable receipt reference, identity refs are deterministic:

- Brief: `sha256_json({"artifact_type":"ProductionDecisionBriefProjectionV1","receipt_ref":receipt_ref})`.
- Memory: `sha256_json({"artifact_type":"ProductionIntelligenceMemoryProjectionV1","receipt_ref":receipt_ref})`.
- Identity: `sha256_json({"version":"PRODUCTION_INTELLIGENCE_MEMORY_IDENTITY_V1","receipt_ref":receipt_ref})`.

Persisted types are `ProductionDecisionBriefProjectionV1`,
`ProductionIntelligenceMemoryProjectionV1`, and
`ProductionIntelligenceMemoryIdentityV1`. A replay returns the original refs.
The memory record identifies provider-job status as
`NOT_CREATED_NO_AUTHORITY`; prior analysis is not reused without a matching
typed source/cutoff/candidate/policy identity. Due-work evidence remains
`NONE`, `MATCHED`, `UNKNOWN`, or page-bounded based on the existing index.

The Brief binds slot/event ID, CandidateSet, universe and product refs, source
cutoff, computation time, hard deadline, receipt, stage refs, calendar refs,
and archive availability. Original source availability remains in the source
index; the projection does not backdate it. The original per-event candidate
population and A0 calendar refs are preserved. This receipt is slot-scoped:
the whole-schedule denominator is explicitly **UNAVAILABLE** with reason
`PRODUCTION_RECEIPT_IS_SLOT_SCOPED`; R3A does not claim paired whole-calendar
coverage.

Bounds are 256 rows, 1 MB, 256 instruments, zero image refs, 4,096 references,
and a 1 MB projected-token byte upper bound. The token field is explicitly a
UTF-8 byte upper bound, not tokenizer output. Row, byte, token, and reference
overflow tests return typed `BLOCKED` results without changing A0 artifacts.

No production `COMPLETE` Brief was emitted. When the existing compiler returned
`COMPLETE` for candidate-only facts, the adapter changed it to `BLOCKED` with
`PRODUCTION_TYPED_SECTIONS_NOT_RECONSTRUCTED`. Typed production readers for
technical, structure, liquidity, derivatives/funding, BTC/ETH, event/news,
prior analysis, watches, invalidators, and matured outcomes are not registered;
those sections carry explicit missing reasons. M1 and analogue stage receipts
are shown as diagnostic availability only and carry
`NOT_ESTIMABLE_TYPED_DIAGNOSTIC_NOT_RECONSTRUCTED`. ML forecast qualification
is `UNQUALIFIED`.

Examples exercised in the production lifecycle:

- **Blocked candidate/action/economics:** event
  `19dd9debb0c793b597e1db7312b219a46ea6b768451aea203b606641033c5917` retained
  its selected A0 candidate and persisted exact action evidence, while the
  Brief was `BLOCKED` and economic value `NOT_ESTIMABLE` because typed Brief
  sections were unavailable.
- **Expired:** advancing the projection clock to the fixed deadline during
  compilation yielded `EXPIRED`; economics became `NOT_ESTIMABLE` and the A0
  snapshot stayed identical.
- **Missing/negative:** a no-candidate slot preserved its calendar; future risk
  availability produced a `NOT_ESTIMABLE` no-action result; reference overflow
  returned `BLOCKED` with `PROJECTION_REFERENCE_LIMIT`.
- **Unknown economics:** the persisted exact action remained visible while
  absent action-conditioned evaluation remained `NOT_ESTIMABLE`.

The read-only desktop projection exposes latest Brief state/ref, selected
candidate/action/economic status, missing-section count, due-work status,
`research_agent=DISABLED`, and blocked capital state. No future agent response
is displayed. Broad GUI rendering was not part of this backend seam.

## A0 compatibility and restart evidence

The deterministic fixture was run on accepted R2 and on R3A with only the new
projection disabled. Full receipt bodies and these A0 artifact ref/hash sets
matched exactly: `OpsSupervisorReceiptV1`, `CandidateSetV2`,
`SizingDecisionV2`, `ActionArtifactV2`, `EvaluationArtifactV2`, and
`DecisionCalendarEntryV2`. The machine record says `equal=true`:
[`a0-baseline-compatibility.json`](transition-r3a-evidence/a0-baseline-compatibility.json).

Repeated events and a repository restart retained one Brief and one memory
artifact with the same refs. An injected persistence failure after the Brief
write but before transaction completion left no partial artifacts; replay then
persisted one complete pair. An injected projection exception preserved the
sealed receipt and A0 refs and did not create a `TradePlanEnvelopeV2` or
`OrderIntentV1`. No capital or assisted-execution flag was enabled, and no
provider or order API was called.

Rollback is the additive switch `--disable-production-intelligence` or
constructor parameter `production_intelligence_enabled=False`. It disables
only Brief/memory projection; A0, source observation, protection, and outcome
maintenance continue.

## Test ledger

JUnit reports are stored as deterministic gzip files under
[`transition-r3a-evidence/`](transition-r3a-evidence/). Each compressed report
was decompressed and hash-checked against its original XML. Exact counts and
both hashes are in `junit-report-index.json`.

| Command/group | Result |
|---|---|
| `.venv/bin/python -m pytest tests/v2/test_session052_production_intelligence.py -q --junitxml=docs/v2/transition-r3a-evidence/production-brief-green-measured.junit.xml` | **11/11 pass**: production lifecycle, no candidate, unknown economics, risk unavailable, exact action, calendar refs, bounds, deadline, restart/duplicate, failure isolation, desktop projection, and atomic persistence |
| R2-baseline red run of `tests/v2/test_session052_production_intelligence.py` | **1/1 expected failure**: normal R2 ops produced zero Brief records |
| `.venv/bin/python -m pytest tests/v2/test_session047_decision_brief.py tests/v2/test_session047_research_memory.py tests/v2/test_session047_research_agent.py tests/v2/test_session047_peer_chronology.py tests/v2/test_session048_s47_repairs.py tests/v2/test_session048_management_shadow.py -q` | **83/83 pass** |
| S27 production/supervisor + due-work/outcome + S44 M15 + S46 peer + S48 management/protection/lifecycle group (exact paths recorded in the command ledger below) | **175/177 pass initially**; two S44 tests lacked optional `numpy`; after installing the declared locked `offline-research` extra, unchanged S44 cases passed **2/2**, so all 177 unique cases passed across the group and supplemental run |
| R1 source qualification, chronology, golden, calendar, archive and recovery group | **112/112 pass** |
| R2 public runtime and Binance qualification group | **39/40 initially**; the one preparation-worker timing case passed unchanged on rerun. All 40 unique cases passed across 41 invocations |
| R2 capture matrix | **61/63 initially**. `[3.0]` passed unchanged on rerun; `[5.0]` failed again at 0.463 s against a 0.1 s limit. One timing case remains a regression **TEST GATE** |
| R2 mixed 1,024-contract public workload | First run missed the 1.5 s service-gap limit at 1.589 s; unchanged rerun passed **1/1**, conserving all 428 offered/accepted/drained/sealed/delivered/interpreted frames with zero rejects |

The larger commands were:

```text
.venv/bin/python -m pytest tests/v2/test_session027_production.py tests/v2/test_session027_ops_supervisor.py tests/v2/test_session037_due_work.py tests/v2/test_session033_outcome_maturity.py tests/v2/test_session033_outcome_resolution.py tests/v2/test_session033_outcome_runtime.py tests/v2/test_session044_close_driven_m15.py tests/v2/test_session046_peer_context.py tests/v2/test_session048_management_shadow.py tests/v2/test_session048_s47_repairs.py tests/runtime/test_session048_position_protection.py tests/runtime/test_session048_lifecycle_projection.py -q --junitxml=docs/v2/transition-r3a-evidence/production-calendar-regression.junit.xml

.venv/bin/python -m pytest tests/contract/test_v1_golden_baseline.py tests/science/test_r1_source_qualification.py tests/science/test_r1_v1_chronology_identity.py tests/science/test_r1_v1_snapshot_compatibility.py tests/science/test_phase4_baseline.py tests/science/test_phase4_engine_decisions.py tests/science/test_phase4_end_to_end.py tests/science/test_phase4_persistence.py tests/science/test_phase4_artifacts_and_controls.py tests/science/test_prefix_invariance_phase4.py tests/science/test_decision_calendar.py tests/scanner/test_phase5_end_to_end.py tests/data/test_archive_runtime.py tests/data/test_prefix_and_archive.py tests/persistence/test_recovery_persistence.py tests/integration/test_v1_phase0_6_acceptance.py -q --junitxml=docs/v2/transition-r3a-evidence/r1-source-golden-regression.junit.xml

.venv/bin/python -m pytest tests/v2/test_session040_capture_receipts.py tests/v2/test_session040_durable_capture.py tests/v2/test_session041_capture_delivery_batching.py tests/v2/test_session041_capture_pressure_reserve.py tests/v2/test_session041_broad_stream_source.py tests/v2/test_session041_stream_service_paging.py tests/v2/test_session041_recovery_queue.py -q --junitxml=docs/v2/transition-r3a-evidence/r2-capture-matrix.junit.xml

.venv/bin/python -m pytest tests/v2/test_session041_mixed_broad_capacity.py -q --junitxml=docs/v2/transition-r3a-evidence/r2-mixed-1024-rerun.junit.xml

.venv/bin/python -m pytest tests/v2/test_session041_public_runtime_integration.py tests/v2/test_session041_binance_qualification.py -q --junitxml=docs/v2/transition-r3a-evidence/r2-public-runtime-qualification-final.junit.xml
```

Affected checks passed: Ruff lint; new-file format; compileall; current changed
file mypy (5 files, no issues); baseline-shadow mypy (3 existing changed
modules, no issues); `uv lock --check`; `uv pip check`; and locked dev plus
offline-research dry run. `ops_supervisor.py` still fails Ruff format in both
R2 baseline and R3A; no format churn was introduced. No dependency or license
file changed. Local added-content credential-pattern scan had no matches; no
external secret scanner was installed.

The full repository suite remains **TEST GATE**: this R3A session did not run
it. The prior 2,783-test collection was interrupted at 1,196. Historical R2
classifications remain visible: consumer compatibility was 145/146 with the
basket-outcome failure reproduced at baseline; health/recovery was 149/150
with PySide6 unavailable. Those are not R3A passes.

## Resources and open gates

The measured 11-test R3A run took 18.333 s wall, 15.440 s child user CPU,
2.310 s child system CPU, and 55,712 KiB peak child RSS. `/tmp` usage grew by
4,096 bytes for that run. The observed cgroup block-write counter increased by
139,264 bytes during it; the final observed counter was 791,220,224 bytes. This
container counter is not an owner rolling 24-hour total. The owner's 5 GB
rolling budget is **NOT ESTIMABLE** from this environment. The 177-case
production/calendar regression group used 662.664 s user CPU, 19.873 s system
CPU, 397,800 KiB peak RSS, and a 253,372,954-byte largest test temp root; the
83-case S47 group used 11.937/1.762 s user/system CPU and 85,548 KiB peak RSS;
the 112-case R1 group used 160.165/9.147 s and 221,104 KiB peak RSS. These are
per-run measurements, not rolling totals. See
[`production-brief-resource-observations.json`](transition-r3a-evidence/production-brief-resource-observations.json).

Unfinished acceptance gates are: whole-schedule and paired A0 denominator;
registered causal readers for the missing typed sections; production-path
future-taint and missing-index adversarial cases; persisted due-work and
invalidation restart cases; instrument/image-limit overflow integration tests;
the one R2 capture timing failure; full repository regression; owner package
review; exact Luna identity verification; and independent acceptance of the
versioned R3B amendment delta. No provider dispatch, selection authority,
capital, assisted execution, exchange order, merge, or deployment occurred.
