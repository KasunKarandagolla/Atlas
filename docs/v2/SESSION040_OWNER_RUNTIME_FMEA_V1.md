# S40 current owner-runtime failure-mode review — version 1

Status: IMPLEMENTED review; full offline/static and native engineering gates are TESTED at `fdec6aa711b95dd5efad207cbf6aa8c94176cfb2`. Independent coordinating review and actual owner Windows 11/live-source qualification remain TEST GATE. Packaging is intentionally deferred by owner instruction, not failed. Frozen contracts remain unchanged.

Verified start: `c0a290ec89f66507d155f2975976ba49132cab18`. Owner run `249acb36304c40e7bec88618cda6d9f3` is retained as failed qualification evidence; exact local database access is still UNVERIFIED.

Each grouped row covers the named seams. The detailed failure responses below distinguish safe rejection, transient recovery and lifetime invalidation. Test filenames identify required cases; they are not assertions that a pending gate was executed. Exact executed counts/checkpoint hashes belong to the offline/native ledger. No S40 distributable is created; this review covers the engineering checkpoint.

## Current failure responses

In this table, **latch** means immutable first-failure evidence for the lifetime of that run. **Reject** means refuse startup or the affected operation without inventing source/economic evidence. Temporary feed recovery never fills old decisions retroactively. Every file/DB operation remains under the existing non-capital authority; only atlas-ops writes ops.sqlite.

| Failure modes / seams | Detection and prevention | Recovery / run invalidation | Durable evidence / owner indication | Proof source |
|---|---|---|---|---|
| Desktop startup, dependency import or bundled identity mismatch | Pinned manifest/source/locks, dependency smoke, actual source Widgets startup; bundled execution deferred | Reject affected startup; no provider/source effects | Diagnostics/preflight reason; ATTENTION | S36 product/package; S40 owner/native |
| Run creation, configuration tamper, cross-package reopen | Immutable source/config/run hash, exact current-build checks, strict config bounds | Reject launch; export old evidence through read-only compatible path | Run manifest/launch result; ATTENTION | S36 product; S40 owner |
| Path creation/permission, missing directory, rename/delete/hard-link semantics | Actual temporary preflight operations under writer lease | Reject before source start; preserve real evidence | Immutable preflight attempt and measured reason; ATTENTION | S40 storage/owner |
| Network/shared filesystem or unsupported WAL/FULL | Path classification, actual WAL/FULL probe, strict store reopen | Reject unsupported path; no durability downgrade | Preflight facts/reasons; ATTENTION | S40 storage |
| Insufficient initial capacity | S39 measured projection with reserve, configured stricter minimum | Reject start; choose adequate local path | Preflight required/free bytes; ATTENTION | S40 storage/owner |
| Live disk exhaustion or unexpectedly fast growth | Bounded measured footprint/free-space windows and future-growth reserve | Warn before reserve; controlled stop at configured exhaustion; write failure fails closed | Typed health/telemetry and final status; ATTENTION or STOP & EXPORT if integrity lost | S40 health/native; S36 product |
| Slow file write, compression, archive/receipt-journal fsync, antivirus-like latency | Raw archive phase timing, capture progress/queue headroom, preflight representative archive and receipt durability/reopen work | Within tested envelope retain FIFO; approaching unsupported capture pressure stops arrivals; terminal capture failure latches | Raw extent/receipt, lifecycle, health first cause; AMBER then RED | S40 capture/owner/native |
| SQLite begin, row/index work, FULL commit stall | Separate measured begin/body/commit/rollback phases, independent raw capture | Bounded sealed backlog tolerates supported controller stalls; overflow/capture failure latches | Persistence timing and exact pending raw receipts; ATTENTION then STOP & EXPORT | S40 capture/native |
| SQLite busy, second writer, start race | Existing exclusive writer lease and bounded busy handling; probe holds lease | Reject second owner; preserve first process/state | Launch/preflight result; ATTENTION | S36 product; S40 storage/owner |
| Long-lived readers, WAL growth, checkpoint attempts with no progress | Fixed-budget read scopes, passive maintenance, physical bytes plus logical frames/progress timestamp | Release bounded readers; checkpoint catches up; warn on growing non-progressing WAL, stop on reserve exhaustion | Typed persistence/health/telemetry; ATTENTION | S40 storage/owner/native; S38 report worker |
| Interrupted checkpoint or process crash during commit | SQLite WAL/FULL atomicity, integrity/reopen checks, capture ACTIVE/CLEAN head | Never infer committed evidence from an interrupted transaction; unclean capture reopen latches | SQLite/raw retained, exact lifecycle and first failure; STOP & EXPORT | S40 capture/receipts/native; S39 recovery |
| WebSocket normal/burst arrivals exceed controller cadence | Independent bounded FIFO raw capture, measured service and sealed backlog | Supported rate drains and returns headroom; sustained unsupported backlog stops/fails explicitly | Queue/item/byte high-water, captured/adopted counts and health evidence | S40 capture/native; S38 sustained |
| Raw capture itself stops progressing | Independent watchdog sees no progress, rising item/byte pressure and remaining time | Preventive stop at declared threshold; accepted queued raw retained; run requires new identity | PREVENTIVE_PUBLIC_CAPTURE_STOP latch; STOP & EXPORT | S40 owner/native |
| Deliberate handoff overload or rejected local frames | Unchanged hard capacity, overflow/reject flags, fail-closed producer | Lifetime run invalidation; never silently drain/drop or reset into a pass | QUEUE_OVERFLOW_LOCAL_DATA_LOSS / rejection latch, raw/transport health; RED | S32/S35 source; S40 capture/health/native |
| Producer terminal failure, unexpected thread death | Typed producer state, capture error and heartbeat/observation ages | Lifetime terminal latch; no broad-HTTP rescue | Producer/capture first cause and final telemetry; STOP & EXPORT | S40 health/owner; S32 source |
| WebSocket disconnect, temporary network loss, reconnect epoch | Existing typed connection controls and exact epochs, source age and recovery state | Preserve controls; require sequence-valid snapshot/warmup; recover current component only | Continuity/health/control refs; ATTENTION, existing latch remains RED | S32 continuity/source/integration; S39 replay |
| Malformed, unexpected, truncated or conflicting public frame | Exact raw bytes retained, strict topic/schema/sequence validators | Quarantine/fail dependent source; no invented translation or completeness | Typed frame health and raw extent binding; ATTENTION/terminal RED | S32/S35 integration; S39 extents |
| Stale frame, source clock conflict, local availability regression | Receipt/source age, monotone stage chronology and explicit epoch checks | Reject stale/future artifacts, recover snapshot when allowed; proven integrity failure latches | Exact times/reason refs, never backdated; ATTENTION/RED | S32 continuity; S37 chronology; S40 capture clock |
| Book sequence gap, invalid delta or reset | Frozen sequence validation and snapshot recovery rules | Fail dependent inference until genuine snapshot/warmup; old gap retained | Book/continuity checkpoint and recovery refs; ATTENTION | S32 continuity; S39 book replay |
| Growing active book/lineage/cache bodies | S39 explicit frame/level/cache bounds and compact immutable checkpoints | Capacity failure invalidates dependent book, fresh snapshot required; history retained | Pressure/continuity/checkpoint evidence | S39 long-run/replay; S40 native resources |
| Raw archive corruption, changed extent bytes, partial journal tail | Exact extent/receipt hashes, bounds, FIFO, run/config/epoch binding and bounded tail validation | Fail closed; retain corrupt/orphan evidence; never silently repair or truncate | Integrity latch and retained raw/journal; STOP & EXPORT | S39 extents; S40 receipts/capture |
| Crash after raw seal before SQL adoption | Immutable raw extent plus fsynced receipt, sole-writer adoption before interpretation | Preserve unindexed raw; unclean or unindexed-tail reopen invalidates qualification | ACTIVE lifecycle/receipt/index comparison; RED/new run | S40 capture/receipts/native |
| Slow REST, error, pending/stuck public acquisition | Existing independent bounded worker and serviced waits; raw capture independent | Explicit unavailable/timeout result; no deadline extension or fake receipt; source may recover | Acquisition/recovery/source-health evidence; ATTENTION | S38 serviced acquisition/HTTP/history; S40 native |
| History/bootstrap/outcome/context maintenance competes with ingestion | Existing bounded active pages/time allowances, source service boundaries and separate raw capture | Backlog remains visible; no elapsed-history sweep or future labels | Active-work/history/context/maturity reports | S37 active work/history; S38 history; S40 native |
| Public context fetch/parse/collection failure | Bounded one-slot collector, source/schema validation | Preserve transient failure; retry only existing bounded policy, missing context remains explicit | Public context reports/missingness; ATTENTION where essential | Existing context tests; S40 combined/native |
| CPU scheduling stall or host-wide uncancellable I/O | Stage/heartbeat/queue headroom, stale projections and measured preflight | Supported injected faults tested; arbitrary all-thread/kernel stall is not guaranteed. Fail-closed loss and preservation apply | Health/persistence/queue evidence; ATTENTION/RED | S40 native fault/soak; outside-envelope UNVERIFIED |
| Health projection fsync or resource/report file-read stall | Dedicated bounded projection/host sampler; scalar queue watchdog independent | Queue decision continues; stale host/projection warns; no second DB writer | Typed observation age; immutable latch wins over stale projection | S40 owner/native |
| Runtime/controller heartbeat stale or watchdog failure | Typed actual times and desktop reassessment at current time | Warning cannot stay GREEN on old facts; clock conflict visible; terminal source loss still latches | Stale heartbeat/host reason; ATTENTION/RED | S40 health/owner |
| Broad HTTP HEALTHY after terminal WS loss | Desktop reads lifetime latch first and separates current HTTP/context from run qualification | Irreversible failure never auto-resets or becomes valid on reconnect/restart | Immutable first cause; STOP & EXPORT even with missing projection | S40 health/owner |
| Missing/corrupt/symlinked/incomplete qualification latch | Bounded strict identity/hash/duplicate-key/temp-file validation, atomic no-overwrite | Fail closed; preserve original and first pending cause; no repair-to-pass | Latch error; RED, start refused | S40 health/owner |
| First export, deep checkpoint lookups, repeated hashes | Bounded snapshot-local exact-dependency caches; raw compressed bytes still hashed | Strict validation remains; yield exact validated prefix before fixed budget expires | Immutable manifest, failures/cursor/has_more and partition hashes | S40 export/cache/native |
| Dense raw index or large retained history overwhelms periodic paging | Existing covering insertion index, bounded relevant-type head merge, FIFO global rowid cursor | Skip only declared non-projection types; retained dependencies still validated; legacy bounded fallback explicit | Source-window scope/limit/cursor and backlog | S40 export indexed/scaled work; native |
| Slow/failed/invalid report, report request while writer under pressure | Read-only bounded snapshot worker, export lock, strict validator, immutable failure status | Writer/capture remain independent; failure visible, successful later export may clear warning | Report-operation status/failure evidence; ATTENTION | S38 report worker; S40 export/owner/native |
| Repeated report request, interrupted export or manifest checkpoint corruption | Single worker/export lease; hash-sealed partitions and atomic head; exact predecessor hash | Refuse conflict/corruption; orphan temporaries not dataset members; resume exact last head | Manifest/partition chain and report status | S36 tuning; S40 export/native |
| Final export after stop or restart | Stop capture, adopt bounded final raw backlog, release writer, join bounded report worker, read-only final export | Preserve failure if worker remains stuck; no fake complete report | Stop epoch, final typed telemetry, final report/failure | S36 product; S40 owner/native |
| Growing RSS, handles, threads, workers or retained collections | Bounded active structures/slots; measured resource series and RSS target warning | Operator warning; supported native trends asserted; arbitrary host leak guarantee not claimed | Telemetry/resource series and RESOURCE_PRESSURE | S40 health/provider/native; 30-minute Windows soak TESTED |
| DB, locator, WAL, raw archive or report growth | Separate raw/SQL/report counters and volume consumption; compact S39 lineage; bounded summary window | Legitimate immutable history grows; projected headroom warns, no deletion/wiping | Capacity/growth samples and explicit omitted-summary history | S39 storage; S40 native/scaled |
| Clean shutdown, stop race or new run after failed run | Exact epoch stop request, sole writer, final raw adoption, CLEAN proof, immutable latch | Clean reopen uses new connection epoch; failed/unclean run cannot become qualification-valid; new identity starts fresh | Lifecycle, stop epoch, latch and final report | S36 product; S40 capture/owner/native |
| Slow, unavailable, malformed, rate-limited or budget-exhausted DeepSeek/critic | Frozen bounded broker/dispatcher/request/deadline/budget isolation, strict model/result metadata | Terminal zero-authority evidence; no fallback/model change, no action/risk mutation | Agent ledger/shadow observations/provider status | S40 provider matrix; S29/S30/S36 broker |
| Broker restart/death, abandoned job, late completion, worker crash | Exact immutable request/action identities; bounded explicit abandonment before detach/stop | Public core continues; abandoned result cannot resurrect; no hidden retry/rescue | Terminal critic evidence, final run/provider reason | S40 provider/owner; S29/S30 recovery |
| Intelligence disabled, actual credentials/connectivity unavailable | Disabled branch requires no key; fake fixtures cover errors without provider calls | Baseline remains usable; genuine provider conformance BLOCKED BY ENVIRONMENT if unavailable | Run config/provider status and explicit smoke gate | S36 product; S40 provider/native |
| Same-version install/reinstall/uninstall or GUI death | Source/payload/lock identity, per-user installer, external data sentinel, separate ops process | Installer preserves external evidence; GUI has no control DB authority | Native lifecycle/smoke manifests; owner Windows11 remains TEST GATE | Installer lifecycle UNVERIFIED / intentionally deferred by owner; source GUI/process tests remain required |

## Limits that remain explicit

The exact S39 owner DB/export hotspot has not been inspected locally. Synthetic commit-stall and exporter profiles establish software failure mechanisms; they do not prove which Windows/SQLite syscall caused the owner's particular stall. Actual Windows11 host capacity, uncancellable kernel I/O, real provider conformance and genuine 48-hour public endurance remain UNVERIFIED / TEST GATE until their own evidence exists. The failed S39 run stays failed.

## Execution scope

The exact-checkpoint offline suite passed 1,913 tests with eight explicit skips:
1,428 V2 passed/five skipped and 485 V1/non-V2 passed/three skipped. Ruff,
mypy, compileall and dependency checks passed. Native suites passed: S38/S39
stream 70/70; S40 fault matrix 295 passed/one explicit POSIX-only skip; desktop
product 86 passed/two explicit Unix-transport skips. Native source desktop,
preflight/health UI, protected-secret and authenticated AF_PIPE tests passed.
The native Windows matrix ran 30 actual wall-clock minutes, processed 307,201
frames, and recorded zero rejects or continuity gaps; queue high-water was
197/512, sealed backlog high-water 25/64, max capture operation 0.672 seconds,
and raw replay exactly matched the captured FIFO hash. Controller commit stalls
through five seconds and the 35-second blocked-provider case passed. The strict
owner-size export passed all 4,777 relevant rows cold in 2.891 seconds and
fresh in 3.031 seconds, with no invalid rows or backlog. The 48-hour-shaped
projection test passed 2,764,800 compact rows plus 200,000 irrelevant rows in
eight bounded pages; it is synthetic query geometry with explicit remaining
backlog, not a completed whole-campaign export or genuine 48-hour evidence.
Installer lifecycle is intentionally deferred with packaging, not called a
failed or passed engineering test.

The geometry is sixteen compact rows/s: four owner stream channels with health
and continuity publication at ordinary one-second cadence, plus 2x margin.
Full book/raw validation is separately covered by the owner-size fixture. This
is not an assertion that real 48-hour endurance passed.

The local extended provider attempt exhausted the explicit 64-batch sealed
backlog with 4,470 captured/adopted frames and no raw handoff rejection; its
preflight-rejected host is not a supported throughput pass. The exact failed
assertion, terminal capture reason and measured capacities remain retained.

The passing native results ran on GitHub's Windows Server 2025 image, not the
owner's Windows 11 laptop. Exact owner DB/syscall attribution, genuine provider
connectivity, real source behavior/rate extremes, arbitrarily long kernel or
filesystem stalls, installer lifecycle and 48-hour owner endurance remain
UNVERIFIED or TEST GATE. The preserved S39 owner run remains failed evidence.
