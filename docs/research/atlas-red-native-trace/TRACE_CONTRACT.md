# Native decision-trace contract V1

Namespace: `NATIVE_ALGORITHM_RESEARCH_TRACE_V1`. All records are research observations with **ZERO authority**. No serialized trace is a CandidateAction, qualification receipt, live decision, fill or economic label.

## Existing owners and instrumented boundaries

| Boundary | Actual owner / instrumentation |
| --- | --- |
| Official cache verification / causal row reader | Existing `verify_sol_sources.verify_member`, `causal_replay.load_native`, E1B H4 aggregation; read-only `replay.py` adapts typed bars with separate actual-local and reconstructed clocks |
| Technical/structural values | Unchanged `features.pipeline.feature_snapshot`; its native technical, candle and structure calls consume the complete finite prefix |
| S1 precursor | Unchanged `strategies.s1_trend._setup` → `_regime`; native side/extreme/source references retained verbatim |
| S6 context | Unchanged `strategies.s6_cross_section._four_hour_context`, `_hourly_returns`; no duplicate score/rank calculation |
| Qualified S1/S2 output | `observe_coordinators` calls actual coordinator methods only with legitimate typed seam; no fabricated universe, quote, event or historical health |
| Native clock identities | Existing `m15_origin_ref`, `s3_m1_origin_ref`, policy `decision_event`, interval constants; event-driven populations remain unknown |
| P1 / Brief output recording | `persist_native_output` retains actual typed result; P1 result must independently resolve in its own native index; synthetic fixtures remain explicitly synthetic |
| Persistence / query | Existing `OpsRepository` generic immutable artifact index and atomic batches; no new schema or DB framework |
| Seal / separate audit | Exact artifact hashes, `TRACE_SEAL.json`, independent `audit.py`; no future market/cash outcome interface |

## Slot identity and stages

A unique decision-observation ID is SHA-256 of version, full research instrument key, native-minute observation time, source-scope ref and code ref. This is an instrumentation identity, not an invented native watch or trade. The original minute cohort/calendar is immutable; source revisions or code changes produce a new explicit run identity.

Required stages:

1. `source`: source/symbol/revision grade, market cutoff, declared replay visibility, original receipt UNKNOWN and historical source health UNVERIFIED.
2. `features`: exact native feature observation ref if evaluated, pure-component ref, reconstructed processing readiness, unchanged original M15 deadline and status. Non-admitted rich work remains NOT_EVALUATED.
3. `strategies`: all S1–S8 and bounded Path B, policy/code identities, native/conditional/event clock, due `true|false|null`, actual output or precise source/resource refusal. `null` never means known NOT_DUE. Qualified coordinator returns are separate from pure precursor/context diagnostics.
4. `economics`: actual P1 required-field names and missingness, candidate/exact-action refs if legitimately produced; here all absent and cash null. No synthetic record enters the genuine population.
5. `selection`: actual Brief/EligibleSet/Agent B result or block; no argmax, invented abstention or provider fallback.
6. `management`: actual action/entry/stop/management/outcome refs or a named block, never a hypothetical filled unit-notional position.

Canonical source/prefix/feature/component records bind exact SHA-256 identities, frame counts/first/terminal intervals and ordered-prefix hashes. H1/M15 bar bindings preserve exact native values and official source-row identities. Derived H4 bindings include all four native H1 parents. Feature native input references are resolvable to bindings or the explicit archive-health limitation. Native units, missing reasons and policy hashes are retained without interpretation.

## Time, immutability and readers

Actual 2026 local source rehydration and Ops publication never become original 2025 receipt. Stable market identities exclude variable current transport/publication metadata. Reconstructed source visibility and processing completion are separately labeled assumptions; all timing failures remain visible. A native owner requiring ACTUAL_SYSTEM inputs is blocked instead of receiving backdated raw records.

Ops uses its own immutable row encoding, single writer and transactions. Stable slot aliases refuse changed outputs; identical duplicate/restart publication reuses the original row and timestamp. Native typed outputs use a distinct `NativeTraceReturnV1` namespace and explicit `SYNTHETIC_OFFLINE` or `RECONSTRUCTED_MARKET` grade. Existing legacy readers/contracts and production qualification namespaces remain unchanged.

Readers: existing Ops `get_artifact` / metadata queries; `audit.py --decision-id` for a full slot; streaming compressed export for complete audit and reference resolution. Export records contain artifact ref/type/content hash and canonical body. Actual index publication times remain in Ops and measured evidence. Three lossless binary parts bind the export; restoration refuses corrupted parts or an existing mismatched assembled file.

Bounds: ≤1,000,000 bytes per trace artifact, ≤64 per atomic batch; protocol scope/CPU/storage limits in PROTOCOL.json. Errors propagate and prevent a successful seal; evidence failure cannot mint a candidate, selection or order. No guarantee about physical disk faults, live execution, full native integration or target-host service timing follows from these research tests.
