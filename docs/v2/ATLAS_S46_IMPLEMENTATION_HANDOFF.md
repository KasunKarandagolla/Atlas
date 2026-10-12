# ATLAS S46 implementation handoff

**Status:** `IMPLEMENTED`; branch push and independent review are the remaining session steps. **Date:** 2026-10-12.

## Source and authority

- Starting commit and verified remote S45 tip: `ce5f67031e7c5184da91aff416ec4aca18b14bbb`.
- Worktree: `/home/kasun/Music/atlas-s46`.
- Branch: `impl/session-046-causal-features-peer-context`.
- The original checkout and its pre-existing dirty/untracked owner files were left untouched.
- Remote heads were rediscovered immediately before worktree creation; no newer suitable session tip was present.
- V1 freeze SHA-256: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`.
- V2 freeze SHA-256: `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`.
- Agent freeze SHA-256: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.

The owner target was treated as product intent; no V1, V2 or Agent contract was changed. No seventh V2 phase, agent authority, capital, source cadence or risk-policy change was introduced. Promotion remains `INTEGRATED → ENGINEERING_PASS → HISTORICAL_DIAGNOSTIC → PROSPECTIVE_SHADOW → INCREMENTAL_VALUE_PASS → DECISION_ELIGIBLE`.

## Changed files

- `README.md`, `docs/INDEX.md`, `log.md`, `translog.md`.
- `src/atlas/v2/features/peer_context.py`.
- `tests/v2/test_session046_peer_context.py`.
- `tests/v2/test_memory.py`, `tests/v2/test_session014_s1.py`.
- `docs/v2/transition-s46-evidence/cleanup_consumers_initial_failed.xml`, `cleanup_consumers_corrected.xml`, `production_outcomes.xml`, `durable_capture.xml`, `calendar_events_initial.xml`, `calendar_events_retry_failed.xml`, `official_calendar_isolated.xml`, `event_transport_isolated.xml`, `event_saturation_isolated_retry.xml`, `v1_s44.xml`, `selection_migration.xml`, `peer_context_red_before_implementation.xml`, `peer_context_initial_failed.xml`, `peer_context_initial_failed_raw.xml.gz`, `peer_context_corrected.xml`, `peer_context_final.xml`, `peer_s6_feature_consumers.xml`, `peer_s6_feature_consumers_final.xml`, `technical_parity_s46.py`, `technical_parity_s46.json`, and `technical_parity_talib_reference_s46.json`.
- `docs/v2/transition-s46-evidence/evidence_manifest.json` indexes every retained evidence artifact and JUnit report hash/count.
- No other production module, strategy policy, selector, lockfile, dependency manifest, migration, artifact reader or capture pipeline changed.

## Cleanup disposition

The runbook Section 5 file-action register was used as a review lead. Narrow reverse-reference review covered production entrypoints and imports, feature consumers, serialization and persisted readers, migrations, command targets, tests, fixtures, docs links and historical evidence. Technical functions remain in use by S1/S2/S6 and the feature pipeline. Root logs, V1 comparator/golden records, S41/TQ/S44 failure reports and migration readers have functional or audit consumers. No deletion had complete reverse-reference and replay proof. **DELETE NONE.**

`docs/INDEX.md` now routes current authority and active entrypoints separately from historical evidence. Root `README.md` identifies S46 as the current implementation and reports its tests and open gates. The S46 checkpoint is at the top of the existing `log.md`; prior checkpoint history is preserved. `translog.md` was appended only.

Two existing tests asserted Ops schema version 1 even though the S45 baseline repository schema is version 3. Only those two test expectations were updated. The initial run failed 2/42 on those stale assertions; the corrected cleanup/consumer run passed 48/48. No production persistence or migration code changed.

## Technical feature and library comparison

`src/atlas/v2/features/technical.py`, `structure.py` and `pipeline.py` were not changed. `TECHNICAL_V1` calculation semantics, policy hashes, archived outputs and replay identity remain intact. The reproducible deterministic comparison script and JSON are in [`transition-s46-evidence/`](transition-s46-evidence/).

On the same finalized synthetic OHLC inputs, EMA20, non-flat RSI14 and population Bollinger width matched TA-Lib 0.8.1 to floating-point noise (maximum absolute differences `1.42e-14`, `3.55e-14` and `1.29e-14`). Initialization did not fully match:

- ATR14 first output: Atlas index 13, TA-Lib index 14; full-window max difference in the retained probe `6.19e-4`, falling to `1.93e-8` after index 200.
- ADX14 first output: both index 27; full-window max difference `0.34462`, falling to `3.62e-5` after index 200.
- MACD line/signal maximum initial differences `0.04925` / `0.10526`; both fall to approximately machine precision after index 200 because their seed paths differ.
- On constant zero-range/zero-volume input, Atlas RSI returns 50 while TA-Lib returns 0. Both return zero ATR/ADX.
- Sparse finalized observations were accepted by existing technical calculations without a gap flag. Existing causal prefix and negative tests remain the guard for chronology/future-tail behavior; feature code was not changed.

This sparse-input behavior is a confirmed limitation of unchanged `TECHNICAL_V1`. Using those features on gapped inputs remains a `TEST GATE` until gap eligibility is explicitly handled. The new peer seam independently returns `NOT_ESTIMABLE` for a gap in its common H1 returns; it does not consume technical indicator rows.

The installed optional comparison environment was Python 3.12.13, TA-Lib 0.8.1, NumPy 2.5.3, Linux x86_64 `cp312` / manylinux wheel. The inspected installed distribution occupied 28,415,168 bytes uncompressed; its extension was 6,058,433 bytes and bundled C library 3,386,481 bytes. This is distribution size, not a runtime/RSS measurement. No Windows host was available; upstream publishes Linux, Windows and macOS wheels for Python 3.9–3.14 ([release](https://github.com/TA-Lib/ta-lib-python/releases/tag/v0.8.1), [wheel/platform notes](https://github.com/TA-Lib/ta-lib-python#installation)). The Python wrapper license is BSD-2-Clause ([license](https://github.com/TA-Lib/ta-lib-python/blob/master/LICENSE)); the bundled C library license is BSD-3-Clause ([license](https://github.com/TA-Lib/ta-lib/blob/main/LICENSE)).

**Decision:** full seed, warmup and constant-input parity was not established, no causal feature benefit or CPU/RSS improvement was measured, and the wrapper adds a compiled dependency. Preserve current calculations; add no TA-Lib, pandas-ta or other indicator dependency/adapter. No installed project pin or lock was changed. No cross-platform runtime or performance claim is made.

## Peer relationship seam

`src/atlas/v2/features/peer_context.py` adds one explicitly requested pair at a time through `PeerRelationshipRequestV1` and emits `PeerRelationshipV1`. The seam:

- Binds full anchor, peer and BTC `InstrumentKeyV2` identities, their exact revisions, the current `UniverseContractV2`, matching S6 state, cutoff and replay availability view. Inputs require completed H1 bars; S6’s existing `_hourly_returns` selection filters actual/reconstructed availability and excludes future or late revisions.
- Uses a declared lookback of 24–720 completed H1 returns (default 720) and minimum 24 actual common UTC timestamps. It records the common timestamps and count. A timestamp gap, insufficient or absent history, or near-zero variance returns `NOT_ESTIMABLE` with null pair covariance/correlation/beta and a named reason. Estimates use sample covariance/variance (`n-1`).
- Includes exact universe, S6-state, product, bar and optional S6 evidence hashes. Cache keys bind method version, full instrument keys/revisions, universe and S6 references, cutoff, lookback, replay view and source hash. Computation, availability and expiry times are explicit.
- Emits raw pair covariance/correlation and beta only when estimable. BTC-adjusted correlation is optional and uses the existing S6 BTC betas only when their input refs match; otherwise it is null with a reason. The output states `RAW_PAIR_METRICS_INCLUDE_SHARED_BTC_FACTOR; BTC_ADJUSTED_CORRELATION_USES_EXISTING_S6_BETAS`.
- Uses an in-memory FIFO cache bounded to 32 records, one H1 maximum TTL, and expiry no later than the next completed H1 boundary. Lookup reports cache miss, not-yet-available, stale or available. Explicit invalidators cover a new completed H1 bar, source change, gap and product revision. Restart begins empty and rebuilds from the caller’s current bar evidence; no durable cache or migration exists.
- Has no venue/network connector and is not wired to frozen selector, risk, plan, decision, or trading paths. The caller chooses relevant shortlisted/registered pairs; no all-pairs matrix is created.

The v2 source tree had no persisted scanner research-cluster artifact with point-in-time provenance to bind, so this seam does not consume a cluster label. `CausalBarV2` carries an instrument revision rather than the whole instrument key; the seam checks every bar revision against the exact request key, and checks full identities through the current universe and exact S6 rows.

Negative tests cover missing anchor and peer histories, misaligned/gapped common samples, future-tail injection, late revisions, reconstructed-versus-actual views, zero variance and BTC collinearity, exact product revision, source-hash invalidation, availability cutoff/TTL, H1/source/gap invalidation, restart/rebuild, bounded FIFO eviction and a socket side-effect trap. S6 tests preserve numeric behavior; the S6 policy hash remains `a8ec8cf56a06de32ebc7f92beb9f895dce6229dac87860b23528cca49069c029`.

## Fresh test results

All reports, including initial failures and corrected runs, are indexed by [`transition-s46-evidence/evidence_manifest.json`](transition-s46-evidence/evidence_manifest.json). No skips occurred in the final reported groups.

| Scope | Result | Report |
| --- | --- | --- |
| Cleanup, V1 golden, feature, S1/S2/S6, S44 and memory consumers | 48/48 passed, 94.485 s | [`cleanup_consumers_corrected.xml`](transition-s46-evidence/cleanup_consumers_corrected.xml) |
| Peer-context focused tests | 10/10 passed, 32.292 s | [`peer_context_final.xml`](transition-s46-evidence/peer_context_final.xml) |
| Peer plus S6, feature prefix and action/cohort consumers | 42/42 passed, 144.188 s | [`peer_s6_feature_consumers_final.xml`](transition-s46-evidence/peer_s6_feature_consumers_final.xml) |
| Outcome resolution, runtime, due-work, action producer and S33 maturity/restart | 76/76 passed, 123.440 s; includes 20 due-work and 15 maturity tests | [`production_outcomes.xml`](transition-s46-evidence/production_outcomes.xml) |
| Journal and durable public capture | 27/27 passed, 97.100 s | [`durable_capture.xml`](transition-s46-evidence/durable_capture.xml) |
| V1 golden, journal/recovery and S44 close-driven M15 | 26/26 passed, 30.434 s | [`v1_s44.xml`](transition-s46-evidence/v1_s44.xml) |
| S1/S2 selection/audits and v1-to-v3 adoption migration | 53/53 passed, 62.537 s | [`selection_migration.xml`](transition-s46-evidence/selection_migration.xml) |
| Official calendar isolated | 27/27 passed, 5.952 s | [`official_calendar_isolated.xml`](transition-s46-evidence/official_calendar_isolated.xml) |
| Event transport isolated | 8/8 passed, 3.170 s | [`event_transport_isolated.xml`](transition-s46-evidence/event_transport_isolated.xml) |

The combined calendar/event run failed the same existing saturation test twice (34/35 each, reports `calendar_events_initial.xml` and `calendar_events_retry_failed.xml`): expected `BROKER_SATURATED`, received fail-closed `CALL_OUTCOME_UNKNOWN`. The event file passed as a whole in isolation and the individual saturation test passed in isolation. No assertion or production transport code was changed. **HYPOTHESIS:** the mismatch is timing-sensitive under the combined runner; its root cause is unverified. Carry the combined-run scheduling/error-classification issue as a minor `TEST GATE`; do not count the combined run as passing.

The initial cleanup test failures were stale schema-v1 expectations, corrected in tests only. The first peer test draft also had fixture/assertion failures; the final module run is 10/10. Red and failed JUnit reports are preserved. The early peer failure XML had trailing line spaces; the readable XML had only line-ending whitespace removed for `git diff --check`, and the exact raw bytes are retained as [`peer_context_initial_failed_raw.xml.gz`](transition-s46-evidence/peer_context_initial_failed_raw.xml.gz). The manifest records both hashes.

## Dependencies, static checks and resource evidence

- No dependency, core requirement, pyproject dependency, package lock or source package change. `requirements-lock.txt` remains authoritative for the recorded S45/S46 venv (DuckDB 1.5.5); `uv.lock` resolves 1.5.6. This discrepancy is recorded, not regenerated.
- `pip check`: no broken requirements. `uv lock --check`: resolved 39 packages. Installed project venv versions included DuckDB 1.5.5 and NumPy 2.5.3.
- Ruff passed on changed Python files and the parity script. Targeted mypy passed for `peer_context.py` with `--follow-imports=skip --ignore-missing-imports`; mypy noted the existing unused `tests.*` override. `compileall` and `git diff --check` passed.
- All three freeze hashes were rechecked. Documentation links and all eight `pyproject.toml` entrypoint targets passed static checks.
- A sanitized changed-file secret scan and final staged diff review are recorded before push in the root log/translog.
- No peer or indicator CPU/RSS benchmark, capacity test, sustained drain, venue diagnostic or soak was run. S45 performance attempts remain invalid for sustained-drain claims.

## Gates and disposition

**Minor carryover / TEST GATE:** combined calendar/event saturation assertion described above.

**Feature-data TEST GATE:** existing `TECHNICAL_V1` accepts sparse finalized bars without a gap label. No new S46 technical feature consumer was wired to that output; future gap-sensitive use must resolve this eligibility behavior.

**Critical promotion/capital blockers:** fake Bybit source/supervisor fixture retains `INCOMPLETE_SNAPSHOT` plus a noncausal dependency `ValueError`; TQ-01 breadth-stall evidence remains 480/512 queue occupancy with one rejected frame, and S45 80/120 frames/s attempts have no valid sustained drain metrics; real Binance final-M15 delivery within close+5s is **UNVERIFIED**; Linux F1/A2 sustained capacity is **TEST GATE**; authenticated/native Binance protection and capital remain unqualified; economic value is **NOT ESTIMABLE**. Capital and assisted execution remain disabled.

No production deletion, capture/indexing optimization, queue/deadline change, source cadence change, execution/risk-policy change or library change was justified. The performance gate remains open because no valid matched workload measurement was available. Peer disagreement is research context only and provides neither a recommendation nor a veto.

## Rollback and next action

Rollback consists of reverting the S46 branch commits. The peer module is additive and unused by frozen decision paths; removing it requires removing its focused tests and documentation/evidence routing. Restore the two test schema expectations only if the underlying repository schema is independently changed back, which S46 does not do.

Next action: independent review of the pushed S46 SHA, ancestry, full diff, reports, freeze identities, deleted callers (`DELETE NONE`), and remaining gates. Do not self-merge or start S47.

## Push evidence

Implementation commit and verified remote branch SHA will be recorded here after push. Final remote SHA: pending.
