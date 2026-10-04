# Live-run tuning evidence schema V1

The Session 037 exporter extends the accepted Session 036 store and read-only publication path. It adds component, chronology, prerequisite, pressure and lifecycle projections. Final validation is recorded in the Session 037 closure ledger; projection results do not qualify live endurance or economics.

## Evidence ownership

The existing operational SQLite artifact index remains authoritative for its accepted roles. `export_tuning_snapshot()` opens it read-only and holds one consistent WAL snapshot. It neither registers new operational evidence nor calls a provider. Analysis files are derived products and can be rebuilt from retained operational evidence.

Existing accepted raw archives retain source and instrument identities, source/event/receipt/ingestion/availability chronology, translation provenance and payload hashes. L2 raw-frame chunks retain exact raw payload bytes in immutable Zstandard Parquet and their indexed checkpoint references. The tuning exporter does not delete or rotate raw archives and does not copy raw ticks, prompts, provider responses or secret fields into analysis rows. Retention required for reconstruction must be managed by the existing archive owner.

## Run identity and publication

`TuningRunIdentityV1` contains a bounded run ID, canonical configuration SHA256, exact 40-character source Git SHA and start time. The product launcher supplies this identity from its sealed run manifest and assigns each run its own operational database. A changed configuration or source identity cannot reuse an existing export directory.

Each call accepts a fixed `cutoff_ns`, at most 100,000 rows, batches of at most 256 rows and a snapshot budget of at most 30 seconds. Defaults are 100,000 rows, 128 rows per batch and 10 seconds. SQLite insertion `rowid`, rather than decision time or availability time, is the incremental cursor. A later insertion with an earlier timestamp is therefore preserved by the next partition. Rows are eligible only when their recorded availability is no later than the fixed cutoff. A future row stops cursor advancement and sets `blocked_future_evidence`; it cannot silently disappear behind the cursor. `has_more` requires another bounded call before export is complete.

The directory `<output_root>/<run_id>/` contains an immutable `run-identity.json`, compressed content-addressed Parquet partitions, immutable content-addressed manifests, immutable compact JSON reports and an atomically replaced `head.json`. An OS file lock permits one exporter publisher. A manifest binds the prior manifest, cutoff, insertion cursor, partition checksum, cumulative source-record digest and report counters. Head advances only after the partition, manifest and report are sealed. Interrupted temporary or orphan files are not dataset members. A snapshot timeout leaves the prior head intact. Repeating an unchanged completed or blocked snapshot reuses its manifest; newly inserted evidence can change the status even at an unchanged cutoff.

## Compact Parquet rows

One explicitly typed long-form schema is used; filter `row_kind` and `artifact_type` to form logical tables. Every row binds `run_id`, `config_hash`, `source_sha`, operational `source_rowid`, artifact type/hash and created/available times. Null means absent or unsupported; it never means zero.

| Logical table / row kind | Stored compact evidence |
| --- | --- |
| Decisions / `DECISION` | Validated calendar reference, origin event, decision time, CandidateSet/candidate/action hashes, policy identity, source stage, selection/admission states, reason codes, evidence references and exact instrument key when a candidate supplies one |
| Missing origins / `MISSINGNESS` | Native M1 late-origin and M15 opportunity missingness, exact instrument revision/key, origin reference, slot time, supporting gate/source reference and reason; separate from calendar rows |
| Received M15 origins / `ORIGIN` | Validated origin, exact instrument and raw-bar index, terminal event/missingness binding and actual observation time |
| Outcomes / `OUTCOME` | Exact calendar/candidate/action/policy binding, target, label state, ACTUAL/SIMULATED/COUNTERFACTUAL provenance, decimal payoff wire value, instrument key when an exact action exists and evidence references |
| Intelligence / `INTELLIGENCE` | Validated critic receipt/calendar/packet/request/action chain, provider/model profile hashes, terminal status, failure reasons, measured dispatch latency and evidence references; zero authority |
| Model requests / `MODEL_REQUEST` | Immutable route/provider/manifest, exact typed request/input hash, source references, original deadline, run/configuration, exact action when present or negative decision-calendar/event binding; transported inputs remain referenced rather than duplicated |
| Model forecasts / `MODEL_FORECAST` | Exact sealed request/calendar/action references, manifest/input identity, actual inference/receipt/expiry timestamps, validated numerical-values reference, output status and recorded horizon sample counts in `model_sample_counts_json`; counts are not independent economic samples |
| Model values / `MODEL_VALUES` | Checksummed bounded numerical baseline values in `model_values_json`, using the declared `log_return:<horizon_ns>:mean` and quantile grammar; provider text is excluded |
| Model terminals / `MODEL_TERMINAL` | Exact immutable request/configuration/route/action/calendar bindings, measured queue/run time, usable/failure state and forecast/value references |
| Diagnostic prediction labels / `MODEL_OUTCOME` | Exact terminal/request/route/model/config/calendar/action and forecast-values references, declared return target/horizon, label state, validated endpoint evidence and observed log return; prediction error and interval coverage where supported. These are research diagnostics, not executable policy payoff. |
| Model registry and missingness / `MODEL_REGISTRY`, `MODEL_MISSINGNESS` | Explicit fixed routes and per-receipt unavailable/expired/invalid-source cases, including negative opportunities with no frozen action |
| Source/runtime/model evidence / `EVIDENCE` | Allowlisted source-health, continuity, recovery, supervisor, outcome-maturity, M0/M1 support/prediction/calibration, analogue/evaluation, readiness and resource artifacts projected into closed metric/identity/reason fields |
| Rejected evidence / `INVALID` | Original indexed type/hash/times plus `INDEXED_EVIDENCE_FAILED_VALIDATION`; no inferred payoff or reconstructed success |
| Features / `FEATURE` | Exact feature/schema/key/cutoff, bounded typed values and missing reasons, separate regime axes, reconstructable cutoff-causal dependencies |
| Action estimates / `ACTION_PREDICTION` | Exact M0/M1 action, fitted method/configuration, support/calibration/OOF/OOD/vector identities, nullable net value and status |
| Pipeline / `PIPELINE_STAGE`, `PIPELINE_RECEIPT` | Exact event identity, stage order and completion, source/receipt/cutoff/terminal times; authority remains ZERO |
| Computations / `COMPUTATION` | Exact target and immediate-input cutoff, computation start/finish, publication and market-to-publication latency, original-deadline eligibility |
| Prerequisites / `PREREQUISITES` | Exact event/product inventory, supported evidence and unsupported roles/reasons, bound event-gate chronology |
| Replay / `REPLAY_SOURCE`, `ACTION_LIFECYCLE` | Exact sealed action/calendar/path/cost assumptions, entry/exits, resolved fees/funding/net payoff, duration and descriptive ROI on frozen sizing margin; unresolved values null |
| Active work / `PRESSURE` | Validated bounds, backlog, cursors, pending history/recovery, overflow and reasons; retained terminals do not become new active work |

The projector reuses existing full calendar/outcome/critic index validators through a read-only adapter that requires the exact previously persisted artifact. Wrong action joins are rejected. Critic dependencies must have been available by observation recording. Native M1 missingness must bind its valid late-origin gate. Product resource telemetry must match the exact run/configuration, content hash, availability and ZERO authority. Model request/forecast/terminal projections validate the immutable declared registry, fixed source cutoff, exact action/calendar/event/deadline bindings and actual inference chronology. Derived baseline input snapshots are published at actual computation time, separately from the market cutoff.

`metrics_json` contains only closed, recorded scalar metrics; `reason_codes` and `evidence_refs` are bounded projections. Product telemetry preserves `cpu_seconds`, `threads`, `disk_free_bytes`, `rss_bytes`, `handles`, `peak_rss_bytes`, `db_bytes` and `wal_bytes` when recorded. CPU seconds are cumulative process time; Linux peak RSS is a peak and cannot be relabeled current RSS. No CPU utilization percentage is inferred. Summaries of cumulative measurements describe recorded samples and do not substitute for rates or utilization.

## Periodic and final reports

The launcher invokes the same exporter periodically and on shutdown. JSON reports contain cumulative calendar-entry, missing-origin, outcome, intelligence and invalid-row counts; counts by policy, source stage, selection/admission state, status and closed reason; recorded metric count/sum/min/max; validation failures; explicit truncation/future flags; and disabled capital/assisted-execution status. A TESTED report qualifies only its read-only offline projection. Invalid, incomplete or future-blocked exports report TEST GATE.

The disposable read-only DuckDB report reconciles manifest-listed validated partitions. It counts distinct recorded origins, maps registered M15 events to exact origins, and keeps calendar rows, candidates, actions and terminal stages separate. A candidate has one immutable terminal calendar state; different candidates/variants sharing an origin remain dependent. Outcome coverage joins exact calendar references and separates target, provenance and maturity. Scalar prediction errors and observed 90% interval coverage are grouped by exact target, horizon, route/model and instrument. Unsupported pairs stay null. Queries use one thread, 128 MB, no spill, a time limit, at most 128 recent partitions and 256 groups per projection. The recent window exposes omitted history, row/time ranges and TEST GATE; older partitions remain retained and queryable. Budget/query failure produces TEST GATE and a null denominator.

Recorded received origins and missingness define the observed denominator. Zero recorded origins does not prove source coverage: expected but unregistered origins remain null. No absent opportunities, independent economic samples or significance are inferred.

Read-only DuckDB or Arrow can query only manifest-listed partitions. Useful supported questions include which recorded stages/states reject most calendar entries, which missingness reasons recur, which policy states have outcomes, which model/provider profile failed, where measured latency accumulates, and how memory/handles/DB/WAL/disk samples change. Join outcomes to `decision_ref` and actions to `action_hash`; compare configurations as dependent observations sharing market history.

## Interpretation and unsupported evidence

Reports supply regime/calendar slices, six-hour descriptive feature stability and missingness, same-action M0/M1 disagreement, immutable registered method/route comparisons, recorded stage intervals and measured derived computation latency, prerequisite missingness, lifecycle costs/duration/margin ROI and pressure. Comparisons are descriptive and dependent. Stage completion intervals include queue and other work; they are not isolated compute time. Unknown expected origins stay null because no instrument-slot expectation contract may be invented.

The installed price-return diagnostic remains separate from action payoff. Exact action replay uses sealed predecision assumptions and supported later source evidence. Missing depth, fees, funding, fill or execution ordering remains unresolved or NOT ESTIMABLE; a candle/BBO does not prove an executable fill. Lifecycle ROI uses the exact frozen sizing margin and introduces no leverage policy. No adaptive management controller is added.

Endpoint prices and source identities remain validated against the exact indexed canonical bar checksum, raw record and payload hash. Completed identities are checked before raw reconstruction. Prediction and action maturation use durable bounded due-work projections with visible backlog and original identities. Atomic composition and incremental manifests preserve restart/recovery without another operational writer.

A 48-hour report supports diagnosis and hypotheses only. Positive economic claims retain the frozen prospective-duration, matured-opportunity, regime, dependence and holdout requirements.


`EconomicSourceManifestV1` projects as `ECONOMIC_CONFIGURATION`, with its immutable
manifest hash as `method_config_hash`. `OpsEconomicEvidenceResolutionV1` projects
as `ECONOMIC_BINDING`, linking that configuration to the exact event/candidate/
action and sealed market cutoff. Both retain raw references; account scope text
and complete model/configuration bodies are not copied into compact exports.
Declared prerequisite source availability is separate from later exact-action
binding and from scientific execution qualification.
