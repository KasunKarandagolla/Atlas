# Causal replay qualification and native strategy integration

18 October 2026. **IMPLEMENTED / TESTED: scoped causal replay repair and native research integration. Full qualified historical trading pipeline: TEST GATE.** OBSERVE; bot allocation and paid allowance zero; capital NO-GO. Independent review of the exact pushed candidate is required.

## Authority

Source: exact `f8d5d95f2492a9af959a4d48b0664233cf9aa123`, freshly verified on GitHub. All 87 remote heads and commit headers were inspected; no newer related correction or acceptance was found. Accepted design f9f8ef0 and separately reviewed P1 f77f3aa remain ancestors. Isolated branch: `research/atlas-red-causal-replay-qualification-2026-10-18`. Final remote SHA is supplied at delivery; no commit can embed its own identity.

[External acceptance](../../project/ACCEPTANCE_NATIVE_TRACE_F8D5D95_2026-10-18.md) records only the owner's supplied **PASS restricted to the native trace foundation**, exact f8d5d95. No transcript, reviewer procedures or reviewer rerun are inferred. Codex is the recorder, not the accepting reviewer. This continuation is not covered by that verdict.

The complete original freezes were read in the preceding native-trace work in this same conversation. This continuation reuses that reading and freshly verifies all three immutable byte identities; [authority evidence](AUTHORITY.json) states this distinction. Original private owner/ADR packets absent from the public repository are not claimed read. Public canonical market chronology, native source contracts, requirements/acceptance rows and proposed-policy restrictions remain authoritative. No design, PDF, strategy methodology, policy activation or capital permission changes.

Owner-selected GPT-6.1 Sol / High. Independent runtime identity/effort attestation **UNVERIFIED**. No subagents used.

## Defect, counterexample and surgical correction

The real path is `CausalBarStoreV2.as_of` → `asof_join` → `JoinedBars` → `feature_snapshot` → `prior_utc_day_range`. The store explicitly selects ACTUAL_SYSTEM or RECONSTRUCTED_MARKET; `asof_join` does not manufacture source health. The feature pipeline already checks the requested availability view. Its prior-day helper nevertheless filtered `raw.available_at_ns`, excluding correctly reconstructed historical bars rehydrated in 2026.

[Parent reproduction](evidence/PARENT_REPRODUCTION.json) and [corrected reproduction](evidence/CORRECTED_REPRODUCTION.json) run the same deterministic synthetic probe against clean parent and current native owners. Parent: 96 complete prior-day M15 intervals, high/low null; current: native high101/low99. Both retain the actual 2026 local receipt and reject a future dependency. These are explicitly synthetic counterexamples, not genuine market qualification.

`prior_utc_day_range(..., availability_class=ACTUAL_SYSTEM)` now accepts an explicit view. Its default uses the identical actual-availability predicate. Reconstructed mode requires reconstructed bars and their separate replay availability, with no receipt fallback. Complete 96-slot, finality, interval, duplicate and revision checks remain. `feature_snapshot` passes its declared view and emits a distinct reconstructed V2 feature identity. Original actual feature versions, artifact identities, accepted version allowlist and trade-location methods remain unchanged. The new research versions are deliberately outside the actual S1 qualification allowlist.

The investigation also reproduced an adjacent availability defect: the partial trade-VWAP branch could consume an actual-only `CausalTrade` during reconstructed replay. That type has no reconstructed visibility field. The branch now preserves a null `REPLAY_TRADE_AVAILABILITY_UNAVAILABLE`, excludes those trade refs and leaves ACTUAL_SYSTEM trade behavior intact. Anchored VWAP and Volume Profile retain their existing blocked replay behavior. No candle VWAP/profile or synthetic receipt is invented.

Canonical basis: V1 section4 actual versus reconstructed availability; V2 sections3.3,7.4–7.5 and31 causal source/research rules; consolidated design section5 and D03 original-cutoff/deadline rules. These corrections apply existing availability semantics to descriptive features. They do not change entry/setup/trigger, sizing, ranking or management rules. [Interface contract](INTERFACE_CONTRACT.md) defines compatibility.

## Genuine native replay and same-source comparison

Fixed already revealed development interval: **[2025-07-01 00:00,01:00) UTC**, unchanged ordered 35-symbol convenience cohort. It is not a new holdout or a historical Binance-universe reconstruction. No October2025 onward data or new market acquisition was consumed. H1/H4 finite prefixes begin Jan1; M15 begins June1. Declared starts, all interior intervals and native terminal intervals are mandatory. Unknown original continuous-live seeds remain UNVERIFIED; no truncated fallback or retrospective gap repair.

Reuse: **302 cached official ZIP/CHECKSUM/CSV bindings freshly verified**, 13 retained historical404 records and 210 retained rejected attempts in this selected manifest. No new requests. Historical rejected download-byte totals remain unknown. The stable source-manifest ref is unchanged: `411ac6565ce1940773167f3a583145e90dc9f899a614f98130a835a61eb9a2b7`. Archive presence and404s establish no instrument eligibility.

| Evidence | Result |
| --- | --- |
| Original instrument/minute research observations | 2,100; all35×60 keys preserved |
| Native M15 inspection slots | 140;132 complete prefixes/eight EOSUSDT–MATICUSDT source blocks |
| Actual S1 pure precursor results | 12 SETUP_OBSERVED/120 NO_SETUP; repeated states, no invented watches/first triggers |
| Actual S6 pure context results | 116UP/eight DOWN/eight MIXED; no qualified rank/breadth assertion |
| Full native feature snapshots | 12: BTC/ETH/BNB at all four M15 slots, fixed instrumentation budget |
| Feature values | 97 each;1,056 available/108 explicitly missing;24 prior-day high/low values |
| Full-feature denominator | 12 observed/120 resource-not-evaluated/eight source-unavailable/1,960 not due |
| Immutable indexed records | 27,362;24,939 exact bar bindings, including derived-H4 native-H1 parents |
| Genuine downstream actions/Agent B/orders/fills | Zero; cash and programme returns NOT ESTIMABLE |

[Fixed post-seal comparison](evidence/PARENT_COMPARISON.json) matches **all132 native source prefixes and S1/S6 payloads exactly**, with zero mismatches, the same eight source blocks and all2,100 original keys. Across three parent full-feature slots, only prior-day high, prior-day low and partial trade-VWAP missing reason differ, exactly as declared. Nine additional full snapshots extend coverage. Native mathematics is invoked, not reimplemented. Primary replay invokes pure S1/S6 components144 times:132 observations plus12 internal full-feature parity checks.

Existing native S2 is exercised on complete synthetic fixtures, separately graded. Genuine S2 cannot pass actual-source/BBO prerequisites; compression is downstream of BBO and was not bypassed. Genuine S1 watch ownership/four-opportunity/trigger lifecycle is not inferred from repeated pure setups. [Source qualification matrix](SOURCE_READINESS.json) covers all S1–S8, Path B, features, P1, Brief, Agent B and management, distinguishing indicator, setup, action, economics and live requirements. Matrix unavailable states are adapter/investigation findings, not fictitious returned native decisions.

## Clocks and source grade

Historical market event/close is distinct from unknown original observation/receipt, declared replay visibility, calculation completion, actual 2026 Ops publication and original decision deadline. H1/M15 replay visibility is close+5s; H4 from H1 adds1s aggregation; feature processing adds at least1s after the latest dependency. **33 observations are readyT+7 and99 readyT+6; all132 miss frozenT+5.** No earlier cutoff, backdated publication, late-entry rescue or relaxed deadline occurs. The Ops audit checks current publication times independently of replay clocks. Current local rehydration is not original exchange receipt; per-bar original receipt/health cannot be recovered from this package.

The separate post-seal audit verifies hashes, dependency resolution, full calendar, unavailable states and timing. It reads sealed records only, cannot change decisions and produces no future-price, filled-action or cash label. The prior seal remains immutable and correctly rejects the changed native code on this checkout; reproduce that old calculation at exact f8d5d95. Historical code inventories are not rewritten to pretend old hashes describe new source.

## Tests, resources and readiness

Final focused scope: **74 passed**, no failures/skips. Existing regression scope: **221 passed/17 failed**, no skips. All17 failures reproduce at clean f8d5d95:16 missingPyArrow, one missingNautilus. Disjoint current coverage:295passed/17failed across312 cases; **not a full repository suite**. Tests include exact parent ACTUAL_SYSTEM artifacts with/without trades, native S1/S2/S6/P1/Brief parity, replay boundaries/gaps/lookahead/receipt separation, full sealed coverage, hashes, duplicate/restart/atomic write failure and denied authority/handoff expansion. Exact commands, development failures and environment are in [reproducibility](REPRODUCIBILITY.md) and execution evidence.

Primary measured replay:308.97s wrapper,629,272KiB peak process RSS,65,523,712-byte Ops database and23,570,097-byte output/transport footprint before later reports. Maximum record566,960bytes; atomic batches≤64; maximum primary Ops commit7.265ms. The audit used40,964KiB and3.754s. Caps:1MB/record,100MB output and250MB per-run scratch. These cloud process measurements are not 4GB Celeron/Windows/Linux host qualification, physical-disk-full testing, sustained service or72h qualification. Repeat evidence records actual independent run measurements; no summed process-tree/minimum-host claim.

Remaining: **TEST GATE** exact-SHA review and full qualified native integration; **UNVERIFIED** original receipt/health/PIT eligibility/live seeds/model identity/host; **BLOCKED BY ENVIRONMENT** PyArrow/Nautilus and unqualified optional model runtimes; **NOT ESTIMABLE** genuine exact actions, eligible sets, qualified agent decisions, fills/costs/portfolio/whole-programme economics. No profitability or indicator-value conclusion follows.

## Smallest next evidence and handoff

The next evidence campaign must first acquire, under separate owner authorization, the actual missing point-in-time metadata/filter/fee/funding, quote/mark/index, verified calendar and receipt/source-health records for native S1/S2/S6 contracts. Original receipt cannot be recreated by choosing faster assumed archive latency; prospective capture may be necessary. NativeM1/trade completeness, sequence-valid L2/flow and authenticated events are additionally required for their respective sleeves. Qualified forecasts/support, real original action populations, exact plans, risk/portfolio state, Agent B histories and independently evidenced fills/costs remain separate dependencies. Adding historical candles alone cannot identify programme economics. No acquisition or outcome campaign is run here.

[Typed mechanical handoff](LUNA_HANDOFF.json) is frozen to hashed inputs and three operations: render sealed traces, verify existing cached inventory and port frozen commands. It grants no execution authorization. A later Luna session requires independent acceptance of this exact candidate and a separate owner request. Availability/strategy/risk/economics/Agent B/management questions stay with Sol; no new holdout, outcomes or policy decisions are delegated. Stop at review.
