# ATLAS V2 — Session 021 Handoff

## Checkpoint

- Branch: `impl/session-021-v2-s3-s6-s7-context-alerts`
- Starting SHA: `9dc828f061584372702fd94236b1d3a733709846`
- Final pushed SHA: recorded in the final Codex closeout after push. A commit cannot embed its own commit SHA without changing that SHA.
- Starting ancestry: Session-021 branch was created directly from the accepted Session-020 SHA above.
- No merge or Session-022 work is included.

## Session result

Session-021 adds research-only S3, S6, and S7 evidence, additive M1/M5 bars, and point-in-time strategy history eligibility. Capital remains disabled. The new sleeves persist hypotheses, watches, alerts, triggers, or candidates independently and have zero influence on `S1_S2_SCANNER_RANK_V1`.

The frozen S1/S2 code, policy identities, selector, Session-020 causal path, Session-019 evaluator, hard-risk sizing, action identity, replay/outcome contracts, and V1 capital/recovery code were not changed.

## Changed files

- `src/atlas/v2/data/bars.py`
- `src/atlas/v2/data/binance.py`
- `src/atlas/v2/data/bybit.py`
- `src/atlas/v2/data/subscriptions.py`
- `src/atlas/v2/data/universe.py`
- `src/atlas/v2/news/__init__.py`
- `src/atlas/v2/news/events.py`
- `src/atlas/v2/strategies/s3_mean_reversion.py`
- `src/atlas/v2/strategies/s6_cross_section.py`
- `tests/v2/test_session021_data_s3.py`
- `tests/v2/test_session021_s6.py`
- `tests/v2/test_session021_s7.py`
- `tests/v2/test_session021_selection_boundary.py`
- `tests/v2/test_session021_universe.py`
- `docs/handoffs/session-021.md`

## S3 — VWAP/statistical mean reversion

- Status: `IMPLEMENTED`, `TESTED`.
- Policy: `S3_VWAP_STAT_MEAN_REVERSION`, version `1.0.0-shadow-research`, capital status `SHADOW_ONLY`.
- Policy hash: `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`.
- Policy artifact: existing `PolicySpecV2` contract. Its identity includes the 5 bps adverse IOC collar as an engineering default that is not economically validated.
- Inputs are full-key cutoff-known causal public trades and their raw refs/timestamps, UTC-day trade VWAP, completed 1M bars, completed 15M and latest confirmed 4H context, volatility, fresh executable BBO, exact-cutoff S7 safety-gate projection, point-in-time S3 eligibility, and fresh bar/trade source health.
- VWAP is derived only from available trades. Candle OHLCV is not used to infer VWAP. Actual and reconstructed availability are separate and each decision binds one explicit view.
- The current residual is `log(cutoff price) - log(frozen UTC-day trade VWAP)`. Its z-score uses exactly the 120 completed residuals strictly before the latest completed 1M bar, using sample standard deviation.
- AR(1) uses the latest 10,081 contiguous completed 1M residual observations: OLS `r_t = alpha + phi*r_(t-1) + epsilon`, which supplies the 10,080 one-minute transitions in the seven-day window. No future samples are admitted. `0 < phi < 1` is required; half-life is `-ln(2)/ln(phi)` and must be inclusively 5–30 minutes.
- ADX14 above 25 rejects. A setup watch requires a strict deviation beyond either 2 sigma boundary. A candidate trigger requires a subsequent completed 1M bar to measurably move toward the frozen entry VWAP.
- Positive residual creates a short hypothesis; negative residual creates a long hypothesis. The target VWAP does not move after entry.
- Stop is the frozen VWAP multiplied by `exp(± one residual sigma)`, with long stops rounded down and short stops rounded up to tick. Nonpositive, invalid, or less-than-one-tick effective distances fail closed. Maximum holding horizon is 60 minutes.
- Setup, watch, candidate, and input refs are persisted through the existing `atlas-ops` writer. Candidate quantity is unset; no sizing or capital API is called. Candidate handoff to the frozen selector is rejected by policy identity.
- Named unavailable states cover missing/insufficient M1 history, incomplete seven-day AR window, missing causal trades/VWAP, stale BBO or source health, invalid volatility/AR/phi/half-life, strong trend, unknown/blocked event gate, point-in-time ineligibility, and invalid/tiny rounded stop.

## S6 — cross-sectional relative strength

- Status: `IMPLEMENTED`, `TESTED`.
- Policy: `S6_CROSS_SECTIONAL_RELATIVE_STRENGTH`, version `1.0.0-shadow-research`, capital status `SHADOW_ONLY`.
- Policy hash: `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`.
- `S6ResearchPolicyV2` explicitly records the mathematical convention and missing action contract; it has no invented stop, sizing, or holding-horizon fields.
- Breadth requires at least 20 point-in-time strategy-eligible instruments, including the BTC proxy. The BTC proxy is the regression reference and is excluded from competitor ranking. Below minimum breadth, missing proxy/history, zero BTC variance, or nonfinite/zero residual volatility return `NOT_ESTIMABLE`.
- Convention: completed 1H log returns; exact timestamp intersection with BTC; trailing 720 completed hourly returns over 30 days; sample beta is covariance(asset, BTC) divided by sample BTC variance; residual return is asset minus beta times BTC; 4H residual return is the latest four aligned residual hourly returns summed; residual volatility is sample standard deviation over the matched trailing residual sample; score is 4H residual return divided by that volatility.
- Full `InstrumentKeyV2` joins are used. Stable rank ties sort by canonical full instrument identity. Decile size is `ceil(0.10 * rankable peer breadth)`. The score is an engineering research definition, not an optimized economic claim.
- A top-decile long or bottom-decile short hypothesis also requires own latest confirmed 4H trend alignment. A later 15M close-confirmed directional trigger is persisted with its refs. Liquidity/spread, funding, source health, and estimated BTC beta remain research context.
- Since the frozen policy does not define S6 stop semantics, exact action status is `NOT ESTIMABLE` / `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; no S6 `CandidateActionV2` is emitted. Session 023 owns that action contract and multi-sleeve selection work.

## S7 — event/news collection, safety, alerts, and directional shadow

- Status: `IMPLEMENTED`, `TESTED`.
- `NewsEventV2` schema version 2 records source/class, canonical URL, content hash, claimed publication time, actual receipt and extraction-completion times, affected canonical assets, event type/severity/confidence, spans, duplicate group, raw and receipt refs, source health, authentication evidence, supersession, and expiry. Availability is the later of actual receipt and extraction completion; claimed publication time never grants earlier availability.
- `EventSafetyGateV2` version `EVENT_SAFETY_GATE_V2_1`; S7 projects mechanically to the existing S1 `EventGate`. S3 requires the persisted S7 artifact behind that projection at the exact decision cutoff and checks its content ref, state, blocked bit, and gate version.
- Scheduled US CPI, payroll, and FOMC rate decisions block on the half-open interval `[scheduled_time - 30 minutes, scheduled_time + 15 minutes)`. Missing, stale, incomplete, or unverified calendar coverage is `UNKNOWN/BLOCKED`; an empty event list is not evidence of clear coverage.
- Abnormality is consumed as explicit `NORMAL`, `ABNORMAL`, or `UNKNOWN`. `ABNORMAL` and `UNKNOWN` extend blocking. No spread/volatility abnormality thresholds were invented.
- Open venue/asset incidents remain blocked. They clear only with an available verified-resolution revision or explicit human-review evidence; expiry alone never clears them.
- Default source hierarchy/configuration: Federal Reserve and BLS are official macro; SEC, Bybit announcement/status, Binance announcement/status, and allowlisted project/security sources are official venue/project/security; Coin Metrics Community is reputable, slower context; GDELT is discovery. All default source qualifications remain `UNVERIFIED`.
- Collection uses injectable no-credential public transports and deterministic stdlib RSS/Atom/JSON parsing. Raw response bytes are archived immutably before parsing. Canonical URL/content exact dedupe precedes event classification; semantic fingerprints, revisions/supersession, source conflicts, and explicit asset mapping are deterministic. An authentication-state change appends one immutable superseding revision; a repeat of the same content/authentication state is idempotent.
- `EventAlertV2` schema version 2 / `EVENT_ALERT_V2_1` is a durable ops evidence artifact bound to the triggering event and its exact availability. It does not use the watch-transition outbox. No desktop projection or external push transport was added; delivery is `UNVERIFIED`.
- Directional shadow is separately versioned `S7_DIRECTIONAL_REACTION_V1`. It requires authenticated or explicitly resolved source evidence, receipt before the reaction window, unambiguous canonical asset mapping, acceptable source health, causal pre-event beta, and actual final 5M bars. It persists abnormal 5M return versus pre-event beta, abnormal volume, spread, and subsequent confirmation. A potential trigger requires a second closed 5M bar continuing the first direction, volume above the historical 90th percentile, and acceptable spread. It never changes the safety gate and emits no candidate action; incomplete exact action semantics are `NOT ESTIMABLE`.

## Additive causal data and point-in-time universe

- `BarIntervalV2.M1` and `M5` add deterministic one- and five-minute UTC durations. Bybit and Binance USD-M public kline mappings, tier-3 subscription channels, and watch event channels are additive. Existing 15M/1H/4H meaning is unchanged.
- Final-bar, forming-bar exclusion, correction append, full instrument revision identity, and actual/reconstructed availability use the existing bar/archive contracts. M1 is not reconstructed from 15M. S7 reaction bars are not synthesized from later bars.
- `session021_strategy_history_days_v2()` opts into S3 seven-day and S6 30-day history eligibility without widening default S1/S2 eligibility. S3 history-qualified instruments receive M1/M5 tier-3 subscriptions. S6 breadth and S7 asset mapping use point-in-time canonical identities. Observation does not grant capital eligibility; `capital_eligible` remains false.
- New research/context artifacts are separate and versioned; `INTRADAY_CORE_V1` and its S1/S2 feature hashes were not changed.

## Preserved identities and authority

- S1: `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0` — preserved.
- S2: `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd` — preserved.
- Selection ID: `S1_S2_SCANNER_RANK_V1`.
- Selection hash: `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac` — preserved.
- A byte/hash reproducibility test persists S3/S6/S7 artifacts and confirms the accepted S1/S2 `CandidateSetV2` output remains identical.
- No V1 production file, `ops.sqlite` table/schema, second SQLite writer, capital/order API, credentials, sizing behavior, or hard-risk authority was added or changed.

## Dependencies and verification

- Runtime dependencies: unchanged; news collection uses the Python standard library. `pyproject.toml`, console entry points, and desktop packaging were not changed.
- `requirements-lock.txt` before and after: `466b64fab963e2d1884be733bd9cc6836969d15215979bf73f055fff2a4de0cc`.
- Status: `TESTED` — hash-locked uv dry-run checked 29 packages and proposed no changes; pip check reported no broken requirements.
- Secret scanner executables were unavailable. A credential-pattern scan of all changed implementation/test files found zero matches. No `.env` was added; the pre-existing `.env.example` was not changed. No matched secret value is retained here.

## Test results

- Session-021 focused suite: 31 `TESTED` (S3: 15; S6: 6; S7: 7; selector boundary: 1; universe: 2).
- Complete `tests/v2`: 251 `TESTED`.
- Public data/collector/archive regression group: 45 `TESTED`, including `test_data_runtime`, Session-020 archive remediation, archive runtime/prefix tests, and Session-021 M1/M5/S3 data tests.
- Direct S1/S2, CandidateSet, frozen contracts, and selector regression group: 34 `TESTED`.
- Session-020 phase-2 causal E2E and desktop/IPC tests: 6 `TESTED`.
- V1 golden baseline and UNKNOWN/recovery/risk/protection smoke group: 52 `TESTED`.
- Ruff: `TESTED` — all checks passed.
- Mypy: `TESTED` — no issues in 283 source files.
- Compileall: `TESTED` — `src` and `tests` compiled successfully.
- `git diff --check`: `TESTED` — clean.
- pip check and dependency-lock dry-run: `TESTED` — clean as above.
- Secret scan: `TESTED` — zero credential-pattern matches; dedicated scanner binaries were unavailable.
- No Session-019 common evaluation contract, desktop/IPC implementation, shared dependency, or serialization contract was modified.

## Assumptions and external qualification

- S3/S6/S7 artifacts are research hypotheses, context, alerts, watches, and candidates only. They are not decision-eligible and do not grant capital authority. No profitability claim is made. Phase 3 has not passed.
- Public source parser/endpoint configuration is not live qualification. Actual source health, calendar coverage, authentication, delivery, and network qualification remain `UNVERIFIED / TEST GATE` unless backed by cutoff-known evidence.
- External live feed qualification and push delivery were not exercised. No dependency change or network-sharing behavior was tested or claimed.
- Windows-specific verification: `BLOCKED BY ENVIRONMENT` (no Windows environment was available).
- Linux packaging/import smoke was not required because desktop packaging, runtime dependencies, and entry points were unchanged.

## Remaining work

- Session 022: sequence-valid L2/OFI, absorption, depth replenishment and impact-residual/S4 inference; any structural/time-context challengers reserved for that session; versioned evidence for abnormal spread/volatility/source-health thresholds.
- Session 023: multi-sleeve selection policy, CandidateSet expansion, multiplicity/selection audit, and the missing S6 stop/action/horizon contract.
- Later scope remains unchanged: S5 crowding/cascade/reversal, LightGBM M1, causal analogue retrieval, discovery lab, S8, venue capital qualification, Binance trading, V2 capital bridge, approval/risk/reservation changes, live orders, and simultaneous multi-venue capital.
- Coordinating ChatGPT must independently inspect the pushed Session-021 SHA, diff, files, and tests through GitHub before Session 022 is authorized.
