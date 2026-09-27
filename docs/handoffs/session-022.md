# ATLAS V2 Session 022 handoff

## Checkpoint and scope

- Required starting checkpoint: `impl/session-021-v2-s3-s6-s7-context-alerts` at `266ef1fe88da2bc4e37cda44c9a9c089c01883b7`.
- Work branch: `impl/session-022-v2-s4-s5-microstructure-crowding-context`.
- Final implementation SHA: `0c27183986b78211157b61e646ad617e0bc1e2da` (the code commit; the following handoff-only commit is the pushed branch tip and is reported in the closeout).
- Both authoritative freeze documents and the Session-020/021 handoffs were reviewed before editing. Existing collector/archive/source-health contracts, Bybit/Binance public adapters, S1/S2/S3/S6/S7 policies and selector, `INTRADAY_CORE_V1`, causal structure/morphology and ops persistence were inspected.
- Session 022 adds research/context evidence only. S4/S5/context have zero influence on the accepted CandidateSet and selector. Capital remains disabled; no order, approval, sizing, leverage, protection or reservation authority was added. No V1 capital/recovery semantics were changed.

## Changed files

- `docs/v2/EVIDENCE_CAPABILITY_MATRIX_V2.json`
- `docs/v2/V1_GOLDEN_BASELINE.json`, `pyproject.toml`, `requirements-lock.txt`
- `src/atlas/v2/data/{capabilities,derivatives,microstructure,microstructure_archive,public_microstructure_ws,research_artifacts}.py`
- `src/atlas/v2/features/context.py`
- `src/atlas/v2/strategies/s5_crowding.py`
- `tests/v2/test_session022_s4_microstructure.py`
- `tests/v2/test_session022_s5_context.py`
- This handoff.

## Evidence capability matrix and collection

- Matrix: `EVIDENCE_CAPABILITY_MATRIX_V2_1`, nine exact venue/environment/product/channel rows, content hash `498e3222c6568ff49e6b3f4e610c9adf6febb1bc5a285d96b9f2d5663fa25820`. The checked-in JSON rows match the typed matrix. JSON file SHA-256: `3650c9f434cee4e537c1d6d2e9bcacd1d68820aee54b221e22363bd43d1a98b1`.
- Every row binds venue, environment, product, source/channel, snapshot/delta and sequence semantics, event/receipt/availability time, depth/cadence/units and any side/funding/OI/basis/liquidation convention, coverage/censoring, reset/repair behavior, source-health and warmup needs, permitted/unsupported uses, and evidence refs/version. Missing or unqualified capabilities return named `NOT ESTIMABLE` states.
- The narrow collector uses credential-free public WebSocket endpoints only. The locked environment/Nautilus surface did not satisfy the exact raw-frame, receipt-time and sequence-evidence contract; no trading/auth endpoint was added.
- Bybit book rows use snapshot plus deltas, `u` update ID, and fail closed on nonconsecutive updates; `u=1` is treated as a reset. `cts` is retained when present, otherwise `ts`. Bybit trade `S` is taker side. The public all-liquidation `S` is liquidated-position side, and the feed is treated as censored, not complete.
- Binance depth uses `U/u/pu` update semantics and requires a REST `lastUpdateId` snapshot plus a buffered-update bridge before validity. Buffered updates only become decision-available after reconciliation; they are not backdated. Binance `aggTrade.m=true` means the buyer was maker, so the seller was aggressor. Binance `forceOrder` liquidation evidence is `NOT ESTIMABLE` under this matrix.
- These semantics are parser/contract support, not live venue qualification. Matrix rows remain `UNVERIFIED`, `TEST GATE`, or `NOT ESTIMABLE` as recorded. No feed was live-qualified in this environment.
- Raw high-frequency frames are archived immutably in Parquet with exact bytes/hash, receipt time, sequence metadata, cursor/restart state and recovery evidence. Compact indices/artifacts use the existing single-writer `OpsRepository`; no second SQLite writer was added. Live WS integration and external source-health qualification remain `UNVERIFIED` / `TEST GATE`.

## S4 sequence-valid microstructure and absorption

- Feature artifact `S4_MICROSTRUCTURE_FEATURE_V1`, policy hash `2708c4a7091cb9c8bab75b388371acbaac905db9f229e9e375f656edd7dffcdf`.
- Absorption shadow `S4_ABSORPTION_SHADOW_V1`, policy hash `fcd2e5b0f3643de92df8fb73e16ca924b3570b92285cc72eebb76ac03c964614`.
- Execution/entry-quality context `S4_EXECUTION_QUALITY_CONTEXT_V1`, policy hash `dae5c5519eb2d5c204607ca8e81f06fdc9b32ed07f69b6460b3fc6d46ccf995a`.
- Deterministic book states are `COLD`, `WARMING`, `VALID`, `GAP_DETECTED`, `SNAPSHOT_RECOVERY`, and `INVALID`. A sequence gap, conflicting duplicate, invalid out-of-order update, disconnect/reconnect without reconciled snapshot, stale feed, or unhealthy source invalidates flow inference immediately. Recovery needs an explicit reconciled snapshot and 30 seconds of continuous valid post-recovery book/trade coverage (engineering default); default staleness is 1 second. Recovery starts a new epoch. Reconstructed-market availability cannot silently qualify S4.
- Causal, cutoff-bound features include BBO, spread, mid, microprice, declared depth bands, imbalance, OFI, signed aggressive-trade imbalance, supported 1/5/30-second windows, displayed additions/removals, persistence/replenishment and price response. Every artifact binds full instrument identity, exact refs, cutoff/view, producer/policy, source health, book epoch and missingness. No backward fill; future markout is an outcome only.
- Absorption is a prior-only expected-response residual hypothesis requiring unusually large signed flow and persistent/replenishing opposing displayed depth. Defaults are labeled unqualified engineering research thresholds. It is not evidence of hidden intent or economic value. Exact action status is `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; no action is emitted.

## S5 derivatives/crowding, continuation and reversal

- Context `S5_CROWDING_CONTEXT_V1`, policy hash `8b29caee27d91d4ecc13b51cd605d13d83bbfca22de0789925adbef1a07e23d7`.
- Deleveraging continuation `S5_DELEVERAGING_CONTINUATION_SHADOW_V1`, policy hash `47c48f9d347957a3175523f7753531dbba5098c1c0cde7301200d4bb813f2fd2`.
- Post-cascade reversal `S5_POST_CASCADE_REVERSAL_SHADOW_V1`, policy hash `6e11389f7c06406acb8a8207d057f8b17edc56e4a9e619322fb64f5b083cf8c8`.
- Typed evidence preserves funding `CURRENT`/`PREDICTED`/`SETTLED`, next-funding time, raw OI units/convention, mark/index/last, basis inputs, 15-minute OI change, price/OI relation, liquidation side/product and coverage, source health, actual versus reconstructed availability, receipt/event times, revisions and exact input refs. Historical import time is not treated as original receipt, and later revisions do not rewrite earlier cutoffs.
- Crowding describes observable context only. It does not infer ownership, trader leverage, exact liquidation maps, liquidation completeness or institutional intent. High funding alone cannot produce a short hypothesis. Context is not connected to hard-risk quantity, stop or leverage.
- Continuation preserves asynchronous `VULNERABILITY`, `BREAK`, `DELEVERAGING_EVIDENCE`, and `CONTINUATION_CONFIRMED` stages with event/availability timestamps and refs. Later OI remains available only at its actual availability time; confirmation must follow the evidence and its latency. Unknown/degraded liquidation coverage is carried forward; censored prints are weak evidence only. No actionable action is emitted; exact action remains `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`.
- Reversal is separate from continuation. It requires exhaustion/absorption plus reclaim or flow reversal; a liquidation print alone is insufficient. It fails closed if the cutoff-known S4 book/flow evidence is invalid or unavailable. No candle substitution or exact action is used.

## Structural and time-context challengers

- Measurable structure context `STRUCTURE_WYCKOFF_MEASURABLE_CONTEXT_V1`, hash `bb3d669abfc92309410bf375c01aea8ca030a381bbc0ef2bba1566aa6ac280f6`.
- Effort/result `EFFORT_VS_RESULT_CAUSAL_V1`, hash `0fe745a8e899215f91689ff28ea1d86d573458fc4490dc7ab5c9c704e7e40414`.
- UTC time context `UTC_TIME_CONTEXT_V1`, hash `8cf70a4b79a2a1006b640043c7a809c19db93c23ac3170803f7bcdae3b4387e8`.
- Fixed UTC windows `ICT_KILLZONE_RESEARCH_CHALLENGER_V1`, hash `88d7103d1dfc3b37a94220bcc3f7ca8a79f52461cdc36466081224c58c433e1f`.
- Spring/upthrust are measurable range-boundary false-break/return events. Effort/result fits only chronologically prior evidence. UTC time, weekday/weekend, explicitly declared session overlap, time-to-funding, and macro-event proximity require cutoff-known sources; macro context uses the accepted S7 event gate/calendar. Killzones are a distinct, unqualified research challenger.
- Accepted causal swing/BOS/CHoCH/FVG/sweep/support-resistance/Fibonacci/morphology evidence is bound by exact cutoff-filtered references, not recomputed from future-known structure. These correlated descriptors are context, not votes. `INTRADAY_CORE_V1` was not changed.

## Selector boundary and preserved identities

- Active selector remains `S1_S2_SCANNER_RANK_V1`. S4/S5/context artifacts are excluded from CandidateSet assembly.
- A reproducibility regression builds the accepted S1/S2 CandidateSet, persists S4/S5/context artifacts, rebuilds it, and checks canonical bytes/hash are unchanged.
- S1 `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0` — preserved.
- S2 `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd` — preserved.
- S1/S2 selector `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac` — preserved.
- S3 `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6` — preserved.
- S6 `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3` — preserved.

## Dependencies and verification

- Added only `websockets==17.0.1`, exactly pinned with hashes. No other runtime dependency changed.
- `requirements-lock.txt` SHA-256: `64c072e71415da5fdba66fdf747e5603e6014e01380a46dba9e95ee4e138a044`. `docs/v2/V1_GOLDEN_BASELINE.json` SHA-256: `2ecd9ea20d2ac26c86ba2679093c435f1533d21b73891c4b6a2048db8d5d459d`. Only the required dependency-lock digest metadata changed in the V1 golden file; V1 contract/golden behavior remained unchanged.
- Focused Session-022 tests: **36 passed** (19 S4; 17 S5/context).
- Complete `tests/v2`: **291 collected, 0 failed**. This includes Session-021, public collector/archive/history/data, Session-020 causal E2E/desktop seam, CandidateSet/selection and Session-019 evaluator regressions.
- Complete V1 suite (`tests --ignore=tests/v2`): **415 collected; suite passed**, including V1 golden and UNKNOWN/recovery/risk/protection coverage.
- Ruff: passed. Mypy: passed, 169 source files. `compileall -q src`, `git diff --check`, pip check, and hash-locked dependency dry-run passed. Lock validation checked 30 packages and proposed no changes. Preserved frozen policy/selector hashes were recomputed and matched.
- The fallback secret-pattern scan covered all 14 staged implementation/test/config files: zero high-confidence pattern hits; matched values were not printed or retained. Dedicated `gitleaks`, `trufflehog`, and `detect-secrets` binaries were unavailable.

## Status and remaining work

- S4/S5/context engineering contracts and tests: `IMPLEMENTED`, `TESTED`.
- Live Bybit/Binance feed semantics, cadence/coverage and external source-health qualification: `UNVERIFIED` / `TEST GATE`; endpoint/parser support is not live qualification. Binance forceOrder liquidation path: `NOT ESTIMABLE`.
- Economic value, profitability, exact S4/S5 action contracts and Phase-3 pass: `NOT ESTIMABLE`. No economic-value or Phase-3 claim is made. Capital remains disabled.
- No external-environment blocker prevented implementation or local tests. Real live-feed qualification was not performed.
- Session 023 owns multi-sleeve selection and all-sleeve CandidateSet assembly, multiplicity correction, M1, analogues, discovery lab and the final Phase-3 gate. This branch does not widen or replace the accepted selector and does not begin Session 023.
- The coordinating ChatGPT must independently inspect the pushed SHA, diff, handoff and tests before Session 023 is authorized.
