# S40 preflight and owner live-health contract — V1

Status: IMPLEMENTED; full offline/static and native engineering evidence is TESTED at `fdec6aa711b95dd5efad207cbf6aa8c94176cfb2`. Independent coordinating review and owner Windows 11/live-source qualification remain TEST GATE. Packaging is intentionally deferred by owner instruction, not failed. This document does not promote a live source or scientific policy.

Verified starting checkpoint: `c0a290ec89f66507d155f2975976ba49132cab18`. Branch: `fix/session-040-owner-runtime-resilience-preflight-health`. Exact implementation and engineering checkpoint identities are supplied by the S40 ledger; no S40 package identity is created.

## Authority and scope

This is a zero-authority operational contract inside `atlas-ops`. The three governing freezes and accepted provider/critic amendments remain unchanged. Capital and assisted execution remain false, model/critic authority ZERO, economics NOT ESTIMABLE, and the final holdout is not accessed. Operator colours/actions do not replace formal ATLAS statuses or the scientific promotion ladder.

The preserved S39 owner run `249acb36304c40e7bec88618cda6d9f3` cannot be rewritten into a passing run. The owner's exact DB has not been found in this environment. Owner-summary facts, repository traces, and synthetic reproductions are distinguished in the handoff.

## Pre-run storage qualification

`preflight_run` starts a credential-free child under the selected immutable run/path binding. The parent holds the existing exclusive writer lease while probing, prevents a second probe against an active writer, and enforces a 45-second process deadline. The child uses a newly created temporary directory only. It never opens the real run's `ops.sqlite` and never fabricates market evidence.

The real installed runtime calls this gate before starting public capture. A failed result blocks startup. The desktop exposes the same gate through **Storage preflight**, retaining the immutable attempt and replaceable latest result. Passing means only that the measured host/path is not obviously incapable of the declared short workload; live source and endurance qualification remain TEST GATE.

The probe checks:

- local-path classification and create/read/write/flush/fsync/rename/hard-link/delete semantics;
- available capacity, pinned runtime dependencies, SQLite WAL and FULL synchronous mode;
- 1,536 synthetic 4 KiB records in 64-record persistence batches, with sustained and burst phases;
- exact Arrow archive replay before and after store reopen; capture-receipt append/fsync (including first journal/pointer creation) and exact bounded tail reopen;
- representative sole-writer transactions, duplicate writer rejection, a pinned read snapshot, passive checkpoint recovery, and read-only integrity/reopen;
- strict read-only tuning export, without increasing its ten-second snapshot budget;
- complete removal of the temporary probe and monotone probe chronology.

Every synthetic payload block varies independently; a compressible repeated string is not used as a substitute for representative file work. Test-hook overrides are marked `FAULT_INJECTION`; installed operation uses `HOST_PATH` without capacity/latency overrides. Results bind selected path, run-manifest hash, limits, measurements, exact reasons and their canonical content hash.

### Threshold derivation

The declared workload is 160 frames/s with a 320 frames/s burst, using the unchanged 512-frame handoff. These rates are synthetic engineering envelopes, not a universal Bybit maximum. An empty buffer represents 3.2 seconds at normal rate or 1.6 seconds at burst rate. A 2× margin gives an individual-operation/leading-warning allowance of **0.8 seconds**. A 64-record batch at 320 records/s represents **0.2 seconds**, which bounds mean sustained and burst service in preflight, including both archive and receipt-journal durability plus SQLite work. Strict export smoke has a five-second margin inside the existing ten-second budget. The probe has a separate 30-second total workload allowance.

Initial free-space reserve is at least **62,930,192,327 bytes**: the recorded S39 short-run 48-hour SQLite-plus-raw estimate of 50,344,153,861 bytes multiplied by 1.25 and rounded up. This is an estimate with reserve, not a guarantee. The existing configured minimum can only increase this requirement. Live measured growth supplements the floor and includes current DB/WAL, archives/receipts, reports through volume consumption, future evidence and margin. Evidence is not deleted to make capacity pass.

Exact rejection codes identify path/dependency/WAL/FULL/permission/capacity/latency/export/cleanup/clock failures. A native CI volume may correctly be rejected for capacity; that refusal is retained and never called a qualified campaign host.

## Sustainable capture and the sole writer

The public source's bounded FIFO handoff is drained by an independent raw-capture thread within the same non-capital `atlas-ops` process. It seals the existing immutable Arrow transport representation and fsynced receipt journal. It has no repository or SQLite connection. The controller remains the only `ops.sqlite` writer, adopts exact sealed descriptors atomically before interpretation, and performs derived/book/continuity work later.

Limits remain explicit: 512 handoff items, 16 MiB handoff bytes, at most 64 items per drain, at most 64 pending sealed descriptors, fixed extent/segment/receipt limits, bounded active books and S39 compact lineage. Raw bytes, FIFO, original receipt/availability, hashes and recovery identities remain exact. SQL backlog is visible. No buffer limit, validation threshold or durability setting is increased.

SQL commit stalls and raw-storage stalls are different envelopes. Independent capture can continue during a blocked controller commit. Raw capture still depends on archive/journal durability and CPU scheduling. A watchdog requests a preventive source stop when capture has not progressed for more than 0.8 seconds and occupancy reaches three quarters of the item/byte capacity, or sustained growth leaves at most 0.4 seconds of measured headroom. This stop is explicit and terminal for the run; it is not fabricated local frame loss or a continuity pass. Queued accepted bytes remain retained and sealed where storage permits.

A simultaneous host/kernel stall can prevent all threads from running. No finite buffer proves tolerance of arbitrary stalls or traffic. Outside demonstrated envelopes, preflight rejection, pressure warnings, explicit preventive stop or fail-closed loss evidence apply; a universal no-loss guarantee is not made.

## Current component health versus run qualification

Typed `LiveHealthFactsV1` bind run/configuration, observation time, source/recovery state, producer/connection state, item/byte occupancy and high-water, received/rejected counts, measured arrival/drain/growth, service/persistence timing, capture backlog, disk/WAL/checkpoint progress, report state, evidence failures and available process resource measurements. Missing measurements remain null. A historical high-water alone does not trap a recovered harmless transient in warning.

`LiveHealthPolicyV1` is versioned and hashed. It supplies operational thresholds only. `LiveHealthAssessmentV1` binds the exact facts and policy; scientific/source completeness and capital permission are not inferred from its colour.

| Guidance | Meaning and examples | Qualification effect |
|---|---|---|
| GREEN — CONTINUE | Current typed components are fresh and no warning or prior failure exists | Does not pass source/endurance/economic gates |
| AMBER — ATTENTION | Half-full queue, shrinking headroom, capture backlog, service/commit stall, stale heartbeat, recovery, disk reserve, non-progressing growing WAL, failed/delayed export, validation failure, or resource pressure | A harmless resolved warning may clear; no invented permanent loss |
| RED — STOP & EXPORT | Terminal/local-loss/integrity failure, corrupt/incomplete failure latch, or observed clock conflict | Stop, retain/export evidence, and follow the exact reason |

The RSS warning uses the frozen V2 1.5 GB performance target as operational guidance for measured ops RSS. It is not aggregate host memory or an OOM guarantee. Unknown handles/threads/resources remain unknown.

### Lifetime latch

`QualificationFailureV1` stores the first exact terminal facts, run/config hash, policy hash, cause, timestamp and content identity in an immutable bounded file. Publication is fsynced and atomically refuses overwrite. An incomplete/corrupt/symlinked/mismatched latch is fail-closed. Publication failures preserve the first cause in bounded memory and retry without claiming a durable file exists.

Lifetime causes include local queue overflow, frame rejection, preventive capture stop, terminal capture/producer failure, evidence/database integrity failure, evidence clock failure and an unclean previous capture stop. A later HTTP or WebSocket recovery never clears this latch. Launching the failed run is refused; a new immutable run identity is required. A clean stop/reopen verifies bounded final raw adoption and retains a fresh connection epoch; it does not prove uninterrupted continuity.

Capture additionally writes a bounded ACTIVE/CLEAN lifecycle head and append-only exact receipt journal. Restart verifies run/config/epoch identity, the bounded final receipt and its exact raw/index binding. An ACTIVE old epoch, unindexed final receipt or corrupt/partial tail is retained and invalidates reopen; no historical sweep or invented continuity is used.

### Watchdog and desktop projection

The watchdog samples at 10 Hz from bounded scalar callbacks without acquiring a database/writer lock. Replaceable desktop projections and filesystem-based resource/report observations use a separate one-slot publisher so file metadata reads and projection flush/fsync cannot starve pressure detection. Host observations carry their actual measurement time and warn when stale. Only this convenience projection is coalesced; immutable first-failure evidence is separate. The publisher has one in-flight and one pending body, no SQLite/source authority and bounded shutdown waits. A stuck projection becomes visibly stale.

The desktop reads the durable latch before examining the replaceable projection. A missing, stale or corrupt projection cannot hide an existing RED latch. It shows the broad HTTP/context state separately from owner-run qualification and action guidance. An old GREEN projection is reassessed against current time.

Minute telemetry and a final stopped observation index the exact typed health/persistence binding on the sole writer so a terminal failure between periodic samples remains available to tuning export.

## Reporting and WAL

Exports use read-only snapshots, strict exact artifact/raw/chronology validation and the unchanged ten-second snapshot budget. Snapshot-local caches are bounded and cleared on scope exit; compressed raw bytes are still hashed when reused. A validated prefix may yield at half budget, with an exact immutable cursor and explicit `has_more`; no row is silently omitted.

Current stores use bounded exact-type insertion-index seeks merged in global rowid order. Dense non-projection raw history cannot consume the periodic report's work allowance. At most 8,192 relevant rows are considered per snapshot. Legacy stores lacking the index retain an explicit bounded source-row compatibility scan; the read-only exporter never creates an index. A fixed cutoff and immutable manifest/partition hashes preserve restart/interruption semantics.

Periodic reporting performs one incremental snapshot per minute for new S40 configurations. Manual/final export can perform at most eight snapshots. Remaining backlog and future-blocked evidence remain explicit. The compact analytical summary uses the existing bounded 128-partition window and identifies omitted history; it is not silently called a whole-campaign analysis.

SQLite WAL/FULL, writer lease, atomic raw adoption and existing passive checkpoint policy remain. Diagnostics distinguish transaction begin, row/archive work, commit, rollback, checkpoint attempt and actual logical progress. A growing WAL without progress warns independently of its physical high-water allocation. Bounded normal readers are tested; uncancellable OS I/O is outside any claimed hard wall-clock guarantee and requires visible warning/stop rather than evidence deletion or reduced durability.

## Owner action and intentionally deferred release

The required Windows-native engineering workflow passed on the Windows Server 2025 hosted runner. Its selected NTFS-path preflight took 6.656 seconds, measured 231,339,700,224 free bytes against the 62,930,192,327-byte reserve, confirmed WAL and FULL synchronous durability, wrote/fsynced/reopened exact receipts, rejected a second writer, exercised a pinned reader/checkpoint, and passed strict report smoke. It generated and removed temporary probe data and did not open a real run DB. The 30-minute companion matrix processed 307,201 frames with zero rejection or continuity gaps. It does not qualify the owner's Windows 11 path or live source.

Submit the engineering checkpoint for independent coordinating review before using it as the base for subsequent full-V2 completion. Preserve failed S39 evidence. The owner explicitly deferred all S40 installer/distributable/installation ZIP/Desktop work; none is created or copied in this session. Native source desktop, DPAPI, broker and runtime tests are TESTED in the source-level native workflow; installer lifecycle and bundled-payload execution remain UNVERIFIED until a later release is separately authorized.

For a later reviewed release, select a high-capacity local path, create a NEW run identity, pass preflight, then perform intelligence-disabled commissioning with an early export. Do not proceed if preflight refuses the path. A RED run cannot be repaired into qualification by reconnecting or restarting. The separate intelligence-enabled endurance test follows review of clean commissioning evidence. S40 performs no owner live test and does not claim a 48-hour run.
