# Replay availability interface — scoped correction

## Existing owners and change boundary

Only `src/atlas/v2/features/candles.py` and `features/pipeline.py` change under production source. Existing strategy, source store/join, active-history, native-trace, risk, numerical evaluator, Brief, provider, management and persistence owners are reused unchanged. Nothing is connected to a live dispatcher. No dependency, database schema, canonical methodology or policy change.

`prior_utc_day_range(bars, *, cutoff_ns, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)` retains its tuple result `(high|None, low|None, ordered_source_refs)`.

| View | Visibility | Compatibility |
| --- | --- | --- |
| ACTUAL_SYSTEM/default | Existing `bar.raw.available_at_ns <= cutoff_ns` | Identical default predicate and complete-day checks; exact full parent feature artifacts remain equal |
| RECONSTRUCTED_MARKET | Explicit reconstructed source class, non-null `replay_available_at_ns <= cutoff_ns` | No use of actual receipt as replay visibility; no missing-clock fallback |
| Unsupported view | ValueError | No event-only or guessed latency mode |

The helper selects only final M15 bars within the prior UTC day, completing by day end. It requires exactly96 unique interval opens, consistent revision and no conflicting duplicate. A hidden late correction cannot replace an available version; `CausalBarStoreV2.as_of` owns version selection. This helper does not resolve unknown historical version history or establish source health. `feature_snapshot` owns full-key checks and rejects any future/unavailable input. Complete finite source origins/gaps are additionally enforced by the research replay before native invocation.

## Feature identities and actual-system compatibility

Actual/default feature versions and the existing `GAP_QUALIFIED_FEATURE_SET_VERSIONS` are unchanged. Full ACTUAL_SYSTEM payloads, units, source refs, state/envelope hashes and trade values match parent goldens exactly. Existing source-health checks remain unchanged.

Reconstructed output uses `INTRADAY_CORE_GAP_QUALIFIED_REPLAY_DAY_RANGE_V2` or its separately named exact-prefix variant. These are deliberately outside the actual S1 allowlist. Repaired descriptive values therefore cannot silently qualify a reconstructed watch/action. The exact-prefix branch does not relax the existing ActiveCausalHistoryStateV1 actual-source contracts.

`CausalTrade` represents actual availability only; it has no replay clock. Reconstructed partial UTC-day trade VWAP is null with `REPLAY_TRADE_AVAILABILITY_UNAVAILABLE`, with no actual trade refs admitted by that branch. Anchored VWAP/profile retain their existing replay unavailability. ACTUAL_SYSTEM trade calculations remain native and unchanged. No new reconstructed-trade contract is invented.

## Six distinct times

1. Historical market event and exclusive bar close.
2. Original actual exchange-source observation/receipt: UNKNOWN_NOT_CAPTURED.
3. Declared reconstructed availability: close+5s, or derivedH4+6s.
4. Research processing completion: latest dependency+1s or later.
5. Actual current rehydration/Ops publication:2026; kept distinct from replay clocks.
6. Original native M15 deadline:close+5s, unchanged.

There is no successful original-deadline decision atT+6/T+7. Reconstructed data never becomes ACTUAL_SYSTEM. An archive-health limitation reference is not a HEALTHY_CURRENT receipt, tradable universe or event CLEAR certificate. `asof_join` without health still returns NOT_ESTIMABLE/SOURCE_HEALTH_UNKNOWN.

## Native consumers and evidence storage

The unchanged adapter calls `feature_snapshot`, S1 `_setup`/`_regime` and S6 `_four_hour_context`/`_hourly_returns`. It preserves actual native output values and hashes. S1 precursor state is not a qualified watch, unique trigger or trade. S6 context is not a full eligible-cohort ranking. S2 `on_trigger_close` requires actual raw availability and BBO before its compression branch; neither is bypassed.

P1 requires its actual typed CandidateMeanInputsV1, original population, independently sized exact action, source-qualified model/cost/risk/scenario contracts and owning-index validation. Brief and Agent B require their own genuine bound evidence; management requires an actual eligible action/position. Missing inputs stop the path; no deterministic agent substitute or qualification artifact is issued.

Existing `NativeDecisionTraceV1`, `TraceWriter`, `OpsRepository` and lossless gzip transport/audit are reused. New runs have distinct source/code/decision refs, protocol and seal. Original evidence bytes are intact; old code-bound seals require the old checkout. Code changes are not hidden by rewriting historical hashes. Actual current publication remains in the owning index, not backdated into a historical identity.

Research observation denominator:35×60 minutes;140 nativeM15 checks; event-driven S4/S5/S7/PathB populations remain unknown, never forced NOT_DUE. S8 hourly symbol inspections are not pair opportunities. S6 conditionalM15 checks are not independent hourly rankings. Full-feature resource omissions retain the declared fixed budget and explicit post-seal counts. No zero-filled missing feature, fabricated candidate, fill, cash label or capital route exists in this instrumentation.
