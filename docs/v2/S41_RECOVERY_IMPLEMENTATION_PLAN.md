# S41 recovery implementation plan

Base: `9bfcfa5ea45388cecf2081e753930e3287828871`, after coordinating review of the companion decisions. One subsequent engineering session; stop with attributable open gates if capacity or required external evidence fails. These tasks implement settled interfaces, not permission to invent policy. Production changes and tests below are for that subsequent session only.

## Ownership and dependency order

Use at most two workstreams if parallel workers are available. **A owns capture/storage/production**, including the shared repository/schema. **B owns safety/policy tests and event transport**. A single integrator owns `production.py`, `product.py`, exports and admission after A's interface lands. B sends interface needs to A; it does not independently edit repository/schema or shared production code. Serial execution of the same tasks is valid.

| Stage | Owner and exact files | Required change / evidence before next stage |
| --- | --- | --- |
| 0 | Integrator: authority files, existing S41 ledgers, configuration/lock inventory | Rediscover remote tips; verify base ancestry and authority hashes. Save pre-change source/profile identity. Carry the critical assertion crosswalk and preserved clause records forward; retain the reconciled 840 scope / 846 review-proof counters without bulk closure. Confirm capital disabled. |
| 1 | A: `data/public_microstructure_ws.py`, `data/broad_stream_source.py`, `data/public_stream_source.py` under `src/atlas/v2/` | Q2 shared snapshot, shared sticky loss and diagnostic lane sampling. Preserve V1 snapshot caller compatibility, reservation arithmetic and lock order. Barrier tests pass before throughput edits. |
| 2 | A: `memory/schema.py`, `memory/repository.py`, `data/compact_observation_index.py`, `data/compact_public_index.py`, `data/public_archive_extents.py`; new `data/public_evidence_blocks.py` | Add schema 2/E2 marker, block/locator/trade-ID/publication tables and decoder. Additive migration, old-reader refusal, mixed old/new read parity and corruption/transaction tests. Retain V1 block formats. No backfill by default. |
| 3 | A: `data/broad_public_source.py`, `data/public_http.py`, `data/collector.py`, `runtime/broad_universe.py` | Preserve each full raw bulk response/page once, per-row exact revision/locator, all receipt identities, unmapped evidence and 10-s cadence. Route high-rate observation/universe/workset metadata through E2 without changing logical hashes or missingness. |
| 4 | A: `data/durable_public_capture.py`, `runtime/broad_public_runtime.py`, `data/public_stream_continuity.py`, `data/microstructure_archive.py`; new `data/public_evidence_preparation.py`; integrator `runtime/production.py` | One bounded spawn preparation process, parse once, ordinal maps and batched exact trade lookups. Move file seal/encoding outside SQL transactions. Durable frame/trade cursor and staged state; <=64 trades/slice, 50-ms cooperative checks, published-frame barrier. Preserve FIFO/feed semantics and old archive readers. Extend the same bounded adoption to REST pages and snapshot delta replay. |
| 5 | Integrator: `science/tuning_export.py`, `science/broad_export.py`, `memory/repository.py` | Format-aware header and cutoff parity, stable inventory/cursor namespaces, complete legal maximum workset export, bounded read snapshots with compression after read release. Publication effective-availability checks must precede downstream/cutoff use. |
| 6 | B: `src/atlas/persistence/sqlite.py`, `migrations.py`, `src/atlas/runtime/writer_lock.py`, `src/atlas/v2/memory/writer_lock.py`, `binance_demo.py`, `binance_native.py`, `binance_reconciliation.py`, supporting `binance_readiness.py` only if proven necessary | Preserve duplicate append rejection and existing caller idempotence, safe indexes/migration and native fences. Add exact lost-ACK/account/revision/partial/late-event/reconciliation tests. Do not auto-resolve UNKNOWN or remove opening TEST GATE. Fix only reproduced engineering defects, not speculative ledger semantic changes. Can run alongside stages 1–5. |
| 7 | B: `runtime/full_strategy_surface.py`, `runtime/broad_research_queue.py`, `runtime/official_calendar.py`, `news/events.py`, `agent_intelligence/event_extraction_transport.py`; `strategies/s6_cross_section.py`, `science/research_selection.py`, `docs/v2/ADR_SESSION041_S6_SHADOW_ACTION_POLICY_V1.md` | S6 approval status pending; retain both immutable selector version/hash specs and use authorized 1.0.0 for new runs; unapproved new action disabled for new runs, old recorded replay preserved. S7 persisted tri-state abnormality and required UNKNOWN fail closed; dedicated V1 extraction endpoint. Durable queue crash/publication/recovery tests for authorized S4–S8 roles. Integrator alone connects production entry points. |
| 8 | Integrator: `runtime/production.py`, `runtime/economic_binding.py`, `runtime/economic_sources.py`, `runtime/analogue_diagnostic.py`, `science/evaluation_service.py`, `science/m0.py`, `science/scenario_engine.py`, `science/m1.py`, `science/active_training.py`; new `runtime/m1_diagnostic_worker.py` | Actual persisted production M0/M1/analogue path; bounded cooperative M0 computation outside write transactions; prepare/compute/finalize split and isolated actual M1 fit/load, sealed exact action, chronological OOF/maturity/support/OOD, comparison/scenario/stress/ES/missing outputs using existing contracts. No new numerical policy. Late completion cannot affect past decisions. |
| 9 | Integrator: `runtime/storage_preflight.py`, `product.py`; extend `tests/v2/test_session041_mixed_broad_capacity.py`; new `scripts/session041_recovery_capacity.py` | Measure actual combined E2 footprint, enforce A2 profile-bound admission and independent runtime pressure stop. Remove broad `None` only by consuming valid matching evidence. Run integrated gate after all write paths/export roles are final. |
| 10 | Integrator: existing requirement/validation evidence and reviewed source freeze | Build concise assertion crosswalk with exact tested-tree/report hashes and open external gates. Run final full gate once. No readiness promotion for missing/failed/native-skipped gates. Commit/push implementation separately; no merge, installer or capital change without separate instruction. |

All abbreviated A/B module paths in the table are relative to `src/atlas/v2/` unless a full prefix is specified. Supporting dependencies listed were inspected because these paths call them; do not audit all 294 changed files again.

## Exact interface details to implement

- Q2: immutable snapshot with aggregate/per-lane allocation, limits/floors, generation and shared sticky rejection/loss. Snapshot validity describes reservations including in-flight mutation, not instantaneous physical deque sizes. No lane/budget equality assertion from independently sampled status.
- E2 schema tables are `public_evidence_block_v2`, `public_evidence_locator_v2`, `public_trade_identity_v2`, `public_evidence_publication_v2`, and `public_adoption_cursor_v2`; reuse/intern the existing exact feed/product catalog where compatible. Locator records bind deterministic publication IDs; immutable receipts assign publication sequence only on completion. E2 export cursor is `(publication_seq, record_ordinal)`; legacy rowid scan excludes E2 markers. Staged locators require a valid durable adoption cursor; public readers require a completed publication. Constraints reject invalid refs/versions/ordinals and ambiguous source/record identity; deliberate forward publication IDs in staging are permitted. Durable cursor payload includes staged state, raw hash, exact product/plan revision, last committed frame/trade ordinal and preparation version.
- Keep generic `ArtifactIndexEntryV2` logical timestamps/hash/metadata. Add optional publication ref/time and an `effective_available_at_ns` property, defaulting to logical availability for legacy records. Public reads exclude staged rows; public cutoff queries and downstream prerequisite checks use effective availability. Export validates canonical domain timestamps separately and emits both chronologies. Writer-only staging access is a private API, never a report flag. Identity/alias comparisons use logical fields; physical publication sidecars are separately validated and never change a canonical identity.
- Preparation request/result is a fixed versioned dataclass, not arbitrary Python work/callback serialization: job kind PARSE or SEAL, run/descriptor/input hashes, exact ordinal range and product/plan bindings, bounded input/output paths and headers. One in-flight/one completion, associated with a retained descriptor; validate no orphan queue bypass. Block chunks <=4 MiB decoded / <=512 records; split a large logical artifact across chunks. Use existing extent limits/fsync semantics and own immutable files. No worker SQLite connection or network/provider access.
- Batch repository acceptance fetches existing headers/descriptors/IDs once per slice and distinguishes complete absent from unavailable. One stage commit, shared block encoding and one grouped publication commit per service slice amortize across its completed frames; no new per-frame file seal or commit. Internal batch insertion avoids per-entry savepoint churn, while the external `atomic_composition` rollback contract is retained. Worker output cannot supply authoritative acceptance, source health or publication time.
- Published receipt follows committed durable seals/locator staging; publication itself commits before decisions/effects. Cursor progress, trade-ID insertion and staged tracker state commit together. Corrupt/incomplete staging cannot advance public cursors. After crash, unpublished completion receives recovery-time availability. Broken capture continuity stays broken.
- M1 uses the existing due-work repository API with distinct `M1_DIAGNOSTIC_V1` lane and one pending exact input job; not the occupied broad-maintenance slot. Commit holdout reservation/input/job before compute; worker is SQLite/network-free with fixed single-thread algorithm and <=60-s watchdog, <=4,096 raw entries/512 fit rows and no population subset. Validate and atomically publish results/completion/retirement on the writer; late results cannot affect past admission. Keep offline `fit_m1` wrapper compatible. M0 pure numerical/replay/scenario loops service streams between bounded chunks, outside SQL write transactions, with sealed inputs and original deadline checks.
- A2 measurement stores both commit and a deterministic source-tree hash excluding docs/evidence; lock/config/profile and representation versions are included. `BroadCapacityMeasurementV2` identifies measured components/window rates/startup/transients/input geometry and results. `BroadResourceEnvelopeV2` binds requested duration/population/rates/memory budgets/free floor to that measurement. Only a matching successful measurement admits a run.

## Three validation levels (V2 §32)

### 1. Fast changed-seam gate

Run import/compile, serialization/hash smoke, changed-file lint/secret scan and the V1 golden invariant subset on code branches, then only the affected seam targets after each small edit. Add the following focused test modules with the exact assertion families in the gate matrix; do not generate per-clause test cases:

- `tests/v2/test_session041_recovery_queue.py`: deterministic barrier interleavings, floors, byte/item bounds, shared loss, rollback and bounded joins.
- `tests/v2/test_session041_recovery_evidence.py`: schema rollback/reopen, raw bulk byte reconstruction, revision/locator binding, duplicate/conflict, actual/replay/effective chronology, mixed formats and old cursors, corruption, complete max-workset export.
- `tests/v2/test_session041_recovery_adoption.py`: worker death/timeout, no SQL/network worker authority, sliced acceptance/restart, >2,048 trade identities, evicted duplicate and conflict, raw-before-index, late publication, rollback, snapshot replay and REST service fairness.
- `tests/v2/test_session041_recovery_production.py`: persisted real production queue/model/event paths; frozen roles; no unapproved S6 action; no S7 unknown clearance; real model fit/load and late-completion/restart assertions.
- `tests/v2/test_session041_recovery_admission.py`: missing/mismatched measurements reject, component accounting/no double counting, disk/WAL/resource stops and stable source/profile binding.

Example commands (run with the repo's pinned Python environment):

```sh
python -m pytest tests/v2/test_session041_recovery_queue.py tests/v2/test_session041_broad_stream_source.py -q
python -m pytest tests/v2/test_session041_recovery_evidence.py tests/v2/test_session041_compact_public_observations.py tests/v2/test_session039_public_archive_extents.py tests/v2/test_session041_broad_export.py -q
python -m pytest tests/v2/test_session041_recovery_adoption.py tests/v2/test_session041_stream_service_paging.py tests/v2/test_session041_broad_adoption_windows.py -q
python -m pytest tests/persistence/test_journal.py tests/runtime/test_session041_binance_native.py tests/runtime/test_session041_binance_reconciliation.py tests/runtime/test_session041_binance_unsent_recovery.py -q
python -m pytest tests/v2/test_session041_recovery_production.py tests/v2/test_session041_strategy_surface.py tests/v2/test_session041_broad_research_queue.py tests/v2/test_session041_event_extraction_transport.py -q
python -m pytest tests/v2/test_session041_recovery_admission.py tests/v2/test_session041_broad_storage_preflight.py -q
```

Only select the lines relevant to the change. Keep existing S1–S3/V1 invariants in affected selection/risk/journal seam tests. Record command/tree/report hash; passing helpers alone never close production or native gates.

### 2. Integrated capacity gate

Extend the existing mixed test; its target stays:

```sh
python -m pytest tests/v2/test_session041_mixed_broad_capacity.py::test_mixed_1024_contract_public_workload_under_bounded_stalls -q --junitxml=validation/s41-recovery-mixed.xml
```

Add the settled harness CLI (new, not claimed to exist at this base):

```sh
python scripts/session041_recovery_capacity.py --profile <exact-run-profile.json> --root <disposable-path-on-selected-device> --seconds 1800 --rate 160 --burst-rate 320 --output validation/s41-recovery-capacity.json
```

The profile declares nonzero two-lane split, depth/trade mix, payload/trades-per-frame maxima, chosen breadth up to 2,048 products/venue, 10-s cadence, active histories <=24, enrichment <=2, reports every 60 s, memory/CPU/free-floor reserves and burst schedule. Exercise full chosen breadth, repeated due REST cycles, asynchronous snapshot replay, sealed strategy maintenance/model evidence, concurrent read-only reports, cold/older geometry, bounded WAL stalls and crash/restart. No extrapolation from 256 products; no Bybit-only substitute. A reduced breadth is explicitly different qualification.

Required: accepted = durably retained = indexed/published at clean finish; zero rejected frames or hidden losses; no unbounded backlog trend; <=1.5-s max service gap including REST/report work; all due broad cycles meet the admitted 10-s cadence or record truthful missingness and **fail capacity acceptance**; reservation fairness; raw/index order and cutoff correctness; clean terminal receipt and unclean continuity rejection. Measure entire host/process set, separate non-ML <1.5-GB target from declared total budget. Record per-component maximum 60-s growth rates, allocation/WAL/transient peaks, CPU/RSS/I/O and commit/fsync/stall distributions. Assert disk reserve before start and stop/drain under pressure. A deliberate oversized stall may correctly stop: report that safety subcase separately, not as sustained-capacity success.

Retain a bounded diagnostic storage-path probe <=30 s. The integrated measurement qualifies the selected code/profile/host envelope; it does not prove 48-hour endurance or venue truth. If the 30-minute combined gate fails, profile remains inadmissible. Profile-required final older-report geometry may take longer; execute it once at this level, never infer it from a tiny clean DB.

### 3. Final full gate

Once, after production source/config/locks freeze (documentation-only evidence commits are separately identified):

```sh
python scripts/session041_validation.py --suite v1 --output validation/final
python scripts/session041_validation.py --suite v2 --output validation/final
ruff check src scripts tests
mypy src/atlas
mypy scripts/session040_native_resilience.py
mypy --platform win32 scripts/windows_*.py
python -m compileall -q src scripts tests
python -m pip check
```

Use existing `.github/workflows/windows-product.yml` command inventory with `engineering_only=true`, bound to the final SHA, extending its native source tests for Q2/E2, Windows competing-process fencing, preparation spawn/fsync/rename, event-extraction authenticated named pipe and pressure/restart. Run the new mixed capacity harness on Windows; the old S40-only harness is insufficient. Include agent sandbox/key/DB/network denial tests, forbidden-authority/static scans, pinned hash-locked dependency installation/audit, source/authority hashes, manifest/spec/package-contract checks and native desktop source smoke. Skips, interrupted reports or absent Windows host are open gates.

Actual distributable build/install/signing/hash validation is a separately authorized release action within this final level, not part of the Luna source implementation authorization. Keep that package proof open until authorized and actually executed; neither this design session nor Luna may claim it from source checks. No extra validation level or repeated whole-suite loop. If source changes after freeze, invalidate affected evidence and rerun affected seams/capacity; freeze anew before one final complete gate.

## Stop and handoff rules

- Any new authoritative tip/freeze mismatch: stop before changes and reconcile the checkpoint.
- Queue accounting/fencing/raw durability/cutoff/hash parity regression: stop dependent work; fix the seam, do not relax the contract.
- E2 schema or old-run parity failure: leave writer version/admission blocked; retain all legacy data. Do not switch to an incompatible new-only reader.
- Sustained gate/resource/cadence failure: reject selected profile; retain evidence and identify measured bottleneck. Do not raise buffers/deadlines or silently reduce breadth/cadence.
- Missing S6/S7 approvals: complete gated engineering and report policy coverage incomplete. Only exact accepted amendment may unlock it; no inference from general “complete V2”.
- Missing Windows/package/actual-account/prospective evidence: retain separate external gates. Software fixtures cannot satisfy them. No capital enablement, merge or installer in this authorization.
- Session handoff states exact implementation SHA, commands and evidence hashes, which assertions passed, measured profile envelope and remaining external approvals/proofs. Do not declare full S41/V2 complete merely because tasks or test counts are complete.
