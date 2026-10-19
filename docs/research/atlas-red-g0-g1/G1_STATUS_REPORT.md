# G1 result: OFFLINE_CAPTURE_TEST_GATE

19 October 2026 (Asia/Colombo). Isolated branch `research/atlas-red-g0-g1-capture-qualification-2026-10-18`, base `19a2d91ebc1b136722e7baf18ea32b4b4d4cbe81`. G0 contract was committed first at `402648d`; the final engineering commit's exact SHA is returned after remote verification. Neither commit self-accepts. [External feasibility acceptance](../../project/ACCEPTANCE_FEASIBILITY_19A2D91_2026-10-18.md) applies only to the exact supplied feasibility SHA.

Durable capture works under the tested synthetic offline conditions. The full admitted maximum-breadth workload still fails its service cadence, and this cloud path fails the unchanged disk reserve. Actual4GB host,real source timing,nativeT+5 and programme economics remain unqualified. No collection,provider,private venue,model generation,order,deployment or policy/capital activation occurred.

## Actual corrections

1. `SealedPublicTransportV1.adopt_verified` registered the extent and batch in two independent calls. A SQLite ABORT on the batch left the extent indexed when called without an outer composition. A new red test reproduces this. The existing sole-writer atomic batch API now publishes both together, rolls back both on failure, preserves raw files and supports duplicate/repeated restart readback. Existing composed controller callers retain their transaction boundary. This was a partial index publication defect, not evidence that a bad frame had reached a trading decision.
2. `DurablePublicCaptureV1._record_captured_batch` replaced the lifetime maximum payload size with the latest batch's maximum. A4096-byte batch followed by a1-byte batch reported1. The new red test now passes with a monotone lifetime maximum.
3. The storage preflight registered65rows with65nested owner calls per batch. It now reuses `OpsRepository.register_artifacts` for the identical65rows and FULL transaction. Original archive,receipt,fsync,readback,workload,queue,deadlines,reserve and thresholds remain. This corrects measured avoidable overhead; it does not establish that it caused the historical owner's throughput rejection.
4. Preflight facts expose bounded raw latency samples for independent percentile verification. The existing measurement schema/readers remain unchanged. A standalone Linux source-checkout harness reuses native capture,receipt,Arrow,Ops,existing max-breadth fixture and path/host helpers; it introduces no new persistence engine,source-health owner,scanner or dispatcher.

## Storage investigation and measurements

The exact historical R9 evidence remains unchanged:16sustained+8burst batches,1536frames,64×4096bytes/batch. Sustained means336.992/326.775ms and burst308.589/327.812ms exceeded200ms. Integrity/receipt/reopen/sole-writer checks passed then; maximum breadth was absent. The original physical host is unavailable, so its filesystem/contention cause is **UNVERIFIED**.

The unmodified preflight on this cloud selected path rejects `INSUFFICIENT_DISK_HEADROOM` before workload execution: its requirement is62,930,192,327bytes. The report retains actual free-space samples. Explicit separate hooks permit a storage-only diagnostic with injected free space and skipped full desktop dependency smoke. It is always `FAULT_INJECTION`,not a host pass. Exact packages were installed outside the repo to execute the storage/native/Qt tests; no repository dependency/lock changed and no model fit or weights were used.

Controlled cProfile runs, same default workload and durability:

| Measured item | Before | After |
| --- | ---: | ---: |
| Repository registration calls | 1561 | 25 |
| SQLite execute calls | 12833 | 5105 |
| Mean SQLite transaction operation | 11.791ms | 6.391ms |
| Sustained mean batch service | 52.217ms | 36.876ms |
| Burst mean batch service | 27.577ms | 18.030ms |

Both diagnostic runs passed the unchanged service rejection gates. These profiler-instrumented cloud observations include cold-start differences and are not statistical performance claims or a host speedup certificate. Reduced owner/SQL calls and equal populations establish the specific overhead improvement. Final unprofiled diagnostic raw samples/quantiles are in `evidence/qualification-final.json`; its sustained/burst means31.414/16.600ms and SQLite mean6.386ms still do not qualify a target disk or full source workload. Sustained max240.613ms is retained; the existing batch threshold is on its mean,while separate persistence-operation maximum is800ms. No failed individual sample was removed.

The final maximum-breadth probe actually executes the existing native collector/archive/workset path:4096products,8192rows/cycle,3cycles,24,576exact raw+index rows and40,977domain artifacts checked after restart. All3cutoff worksets match. Maximum observed logical DB+archive growth is **42,504,371bytes/cycle**; second/third40,694,806/40,698,870. Startup product metadata,physical process writes,WAL and archive bytes are reported separately. Only this measured number feeds the existing report-local capacity assessor; its unchanged factors produce1,377,141,631,200required bytes for the declared two-venue48h envelope with zero additional owner floor. This is a projection from fixed synthetic samples,not measured endurance or an eight-week retention certificate. The global unknown remains `None`; ordinary product admission remains closed.

**Cadence failure:** adoption19.033/20.691/20.341seconds, plus workset2.463/2.972/3.009seconds, each exceeds the10second broad cycle. It is not a deadline pass just because all records persisted eventually. This measurement excludes live streams,enrichment,decisions,reports and dynamic metadata,so it cannot certify a larger real workload. The observed service owner is serial controller/collector/workset work; CPU use and timings are recorded. Full-host filesystem or serialization root cause is not inferred from aggregate timing alone. The smallest correction is profiling those existing owners on the exact admitted profile,then correcting demonstrated costs or recording insufficient capacity. No sampling/queue/deadline/retention workaround is supplied.

Final harness peak process-tree RSS/HWM945,475,584bytes,PSS942,756,864,private940,544,000;WAL7,992,832;temporary140,560,860bytes,2272resource samples at50ms,114.340CPU seconds. This includes the measurement helper's full verification maps and telemetry,not measured steady production core memory. Available memory is sampled against host/cgroup limits. It cannot qualify4GB hardware. Sampling can miss transient WAL/temp/queue peaks. No temporary probe survives normal completion.

## Durability, chronology and source integrity

The new capture runs each offer/seal/adopt/read back512frames; normal160fps and adverse320fps with250ms controller stall. Exact FIFO bytes,receipt/availability,source/channel/connection epoch survive two complete read-only reopens; zero lost/rejected/unavailable frames in those runs. Their throughput includes startup,drain and readback and is labelled accordingly. Native strategy/economic/Agent B work is absent,not zero latency. Exact operation sample counts,p50,p95,p99 and maxima are retained rather than inferred from a projection.

Focused fault tests and the existing suites cover raw hash/extent corruption and truncation,receipt framing/header/pointer corruption and interrupted batches,terminal fsync/ENOSPC injection,SQLite ABORT/WAL commit failure,crash before/after adoption,unclean lifecycle refusal,clean restart,duplicates,exact per-connection identity,FIFO,queue overflow,backpressure and physical sole-writer lease. New SQL failures roll back both descriptors; raw remains recoverable and never establishes continuous source qualification. Disk-full tests inject ENOSPC; they are not a destructive physical target-disk fill or a real power-cut certificate. Failure counts/populations are asserted in the named tests and JUnit artifacts; rejected frames remain latched in native handoff/health state.

Existing source/health/book/universe and chronology tests retain duplicate/reorder/disconnect/reconnect/gap/bridge/warmup,missing/stale quote/mark/funding,metadata revision/future receipt,clock regression,late input and expired decision refusals. No absent value is coerced to zero,CLEAR or healthy. No native enum is replaced. Actual availability and `RECONSTRUCTED_MARKET` are distinct.

Canonical time fields remain the existing owners: `RawObservationV2.event_at_ns/published_at_ns/received_at_ns/ingested_at_ns/available_at_ns/sequence/revision_of/instrument_revision/translation_version`; publication unknown stays null. Transport adds source/channel/connection epoch; continuity binds health epoch; `PUBLIC_INDEX_AFTER_ARCHIVE_V1` separates raw availability and `index_published_at_ns`. Derived receipts retain actual start/finish/availability,raw cutoff,code/policy inputs and original consumer deadline. No backfilled receipt,bar-close substitution or deadline extension was introduced. Existing T+6/T+7 historical research failures remain unchanged. Full maintained-state→features→strategy→P1/Brief→B/provider timing is explicitly UNVERIFIED and not measured by receipt→controller latency.

## Independent gates and next action

| Gate | Exact status / scope | Remaining gate |
| --- | --- | --- |
| A Storage correctness | IMPLEMENTED / TESTED offline mechanical fixtures | Real-host power-loss/filesystem qualification and source continuity separate |
| B Throughput/capacity | TESTED diagnostic batching improvement; **TEST GATE** maximum breadth/cadence and disk headroom | Correct measured owner-path service or establish an honestly supported actual host/profile; do not guess capacity |
| C Chronology | TESTED existing typed chronology/refusal regressions | Genuine original-time source and full native decision timing UNVERIFIED |
| D Source integrity | TESTED deterministic gaps,reconnect,duplicates,health/book failures | Genuine source population and coverage UNVERIFIED; no public collection executed |
| E Native T+5 | Instrumented capture timing; **UNVERIFIED / TEST GATE** full decision path | Execute real maintained-state and all required downstream segments under original deadline after their source/contract gates |
| F Target hardware | Harness IMPLEMENTED / TESTED on cloud; actual target **UNVERIFIED** | Real selected4GB host/platform,original preflight,admitted load and72h/retention evidence; Windows harness unsupported |
| G Whole programme | **NOT ESTIMABLE** | Separate G2–G5; no economics/operating authority follows from capture |

[Target procedure](TARGET_HOST_PROCEDURE.md) supplies a runnable offline command and the existing bounded public-only Linux diagnostic command,which was not invoked. A future public request must pin reviewed build,selected host/path,public domains/channels,population,duration,byte/disk limits and stop conditions,after original target preflight and service gates pass. Current maximum-breadth and cloud headroom failures block a full prospective observation campaign. No prospective OBSERVE activation is approved.

Smallest next action: independent exact-SHA review of this candidate,then profile/correct the existing maximum-breadth service and run the same offline harness on the actual selected host. G2 genuine P1 original-population/calibration/provenance and complete typed readers remain a separate Sol-owned gate; synthetic-only grades cannot be promoted by renaming. No dependent G2,provider,ML,trading or collection session starts here.

See `REPRODUCIBILITY.md`,`EXECUTION_EVIDENCE.json`,`ARTIFACTS.sha256` and retained test artifacts for exact commands/counts/hashes and baseline-runner correction. Frozen algorithms,three freezes,strategies,policies,comparators,locks and prior evidence bytes are preserved. Acceptance evidence links change without requirement closure or self-acceptance. **OBSERVE / ZERO ALLOCATION / ZERO PAID ALLOWANCE / CAPITAL NO-GO. STOP after verified candidate push for independent review.**
