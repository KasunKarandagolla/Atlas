# ATLAS V2 Session 014 — Phase-2A core intelligence and S1 shadow handoff

## Checkpoint and authority

- Starting branch: `impl/session-013-v2-phase1-worker-isolation-gate`.
- Starting and verified remote SHA: `3490eed6ab3911059548eb7fceb21162577bc56e`.
- Final branch: `impl/session-014-v2-core-intelligence-s1`.
- Final SHA: recorded in the Session-014 closeout after push; a commit cannot contain its own hash.
- The existing Session-009 checkout had untracked supplied inputs and was left untouched. Session 014 used `/home/kasun/Music/atlas-session-014`, a clean dedicated worktree from the required SHA. No later approved remote branch existed when fetched.
- Authority read: `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`; supplied `/home/kasun/Music/atlas/ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md` (SHA-256 `bae3e1a9e48aec64d1292e5bc791c2e87949ed33f4124b1f9807b589cc07a484`); `docs/v2/V1_EXIT_AUDIT.md`; `docs/v2/V1_GOLDEN_BASELINE.json`; Session-011, Session-012 and Session-013 handoffs.

## Added modules

- `src/atlas/v2/features/`: `__init__.py`, `joins.py`, `technical.py`, `candles.py`, `structure.py`, `regime.py`, `pipeline.py`.
- `src/atlas/v2/math/`: `__init__.py`, `core.py`.
- `src/atlas/v2/strategies/`: `__init__.py`, `s1_trend.py`.
- `src/atlas/v2/models/adapters/chronos2.py`.
- `tests/v2/test_session014_core.py`, `test_session014_s1.py`, `test_session014_chronos.py`.
- This handoff. No V1 production file, Phase-1 persistence schema, Phase-1 public adapter, data collector or existing model adapter was changed.

## Chronos-2

**IMPLEMENTED / TESTED:** Exact `ModelManifestV2` identity and weight/tokenizer hash expectations, versioned preprocessing/postprocessing hashes, causal-prefix and context limits, target/horizon/quantile checks, explicit missing/unsupported outputs, and deterministic-calendar-only known covariates. Real inference is exposed only through `LocalProcessProvider` isolation. Fixture postprocessing has zero decision influence. No heavy package, checkpoint or weights entered the main runtime or dependency lock.

**UNVERIFIED:** Exact Chronos-2 checkpoint/package loading and forecast behavior. The fixture manifest and fixture outputs are schema tests, not a real checkpoint integration or promotion.

## Feature and mathematical versions

**IMPLEMENTED / TESTED:** `INTRADAY_CORE_V1` uses full `InstrumentKeyV2` revision-bound, source-health-bound as-of joins. A missing, unavailable, degraded or source-mismatched health observation makes the join `NOT_ESTIMABLE`. The confirmed 15M bar and latest confirmed 1H/4H bars available at the cutoff are used; forming bars and future revisions are excluded. `FeatureArtifactV2` binds sorted exact bar, source-health and used trade refs, cutoff, confirmation, replay class, feature version and deterministic hash. Missing values carry reasons; no NaN is emitted.

Technical methods are SMA-seeded EMA20/EMA50 and Wilder ATR14/RSI14/ADX14, MACD 12/26/9, ROC10, prior-20-bar Donchian, 20-bar realized log-return variance, EWMA variance with decay 0.94, 20-bar Bollinger width, causal range statistics, prior UTC-day high/low and UTC-day trade VWAP when same-revision causal trades are supplied. Anchored VWAP and volume profile remain explicitly missing without defensible inputs. Candle geometry is continuous with explicit denominator/history missingness.

**IMPLEMENTED / TESTED:** `MATH_CORE_V1` uses Theil-Sen pairwise median slope (20-bar lookback in feature artifacts, minimum 3, price per bar), scalar forward-only Kalman state/variance update, realized and EWMA variance, origin-only ordinary-least-squares HAR using matured earlier targets, immutable ordered empirical residual refs with insufficient-history status, and action-independent deterministic common-path IDs/draws. Singular or insufficient HAR support returns missing. No full-sample smoother or economic evaluator is included.

Independent `REGIME_V1` axes hold evidence refs and use `UNKNOWN` for missing sources. The core feature snapshot records trend/volatility evidence and explicit missingness for liquidity/crowding/event axes until their separate evidence is bound. No regime axis has capital authority.

## Structure confirmation

**IMPLEMENTED / TESTED:** 15M pivots use two bars left and two right with strict unique highs/lows. Pivot time is bar `i` close; confirmation and first availability are bar `i+2` close. BOS uses a close beyond an already confirmed swing by 0.1 ATR; CHoCH is the first qualifying break against established direction. FVG appears at the third completed bar. Sweep requires a 0.1 ATR pierce and close back across an already known swing and is only morphology. The order-block candidate is the last opposite candle among the prior five, first recorded at BOS confirmation. Subsequent immutable update events record age, exact-level touches, a price-touch mitigation proxy and normalized distance. These are research definitions, not semantic proof.

Support/resistance versions are incremental and immutable, merge at 0.25 ATR measured at pivot confirmation, and break/touch without historical reclustering. Tie ordering is distance then zone ID. Fibonacci 0.382/0.5/0.618 uses confirmed legs only. Elliott-like morphology records amplitudes, durations, retracement/extension, overlap, acceleration, volume ratio when two confirmed legs have bar volumes, and multi-scale ratio; no definitive wave labels are emitted.

## S1 policy and watch

**IMPLEMENTED / TESTED:** Baseline policy ID `S1_MTF_TREND_PULLBACK`, version `1.0.0-shadow`, policy hash `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`. The long rule requires confirmed 4H close above EMA50, EMA20 above EMA50, a low touch of contemporaneous EMA20 in the last three closed 1H bars, and latest 1H close above EMA20/EMA50. Short is symmetric. A watch then waits for a subsequent confirmed 15M close beyond the immediately prior confirmed 15M high/low. A pre-watch 15M bar cannot trigger. The exact 4H, 1H, three setup 1H, feature, gate and trigger refs are retained.

The deterministic setup-window interpretation is **the same three closed 1H bars used for the pullback test**. The long extreme is their minimum low; short is maximum high. The stop is that extreme minus/plus 0.25 causal ATR15M. Executable ask/bid is the Decimal reference; adverse collar is 5 bps. Distance below 0.5 ATR or above 3 ATR rejects a candidate. BBO and mark/index observations have a policy-recorded 5-second maximum age. The typed event gate has a policy-recorded one-hour maximum age; `BLOCKED` prevents setup/candidate and `UNKNOWN`/stale gives `NOT_ESTIMABLE`.

Watches use existing optimistic `ops.sqlite` transitions and outbox. Four subsequent 15M closes are inclusive; an untriggered fourth close expires the watch at the fourth boundary plus one nanosecond. Restart restores `CANDLE_CLOSED_15M`; deterministic IDs prevent duplicate setup/trigger handoff, and old replay bars cannot reopen a watch. A contrary confirmed 4H regime invalidates it. Candidate indexing occurs before `READY_FOR_RECHECK` and `CONFIRMED`. `HANDED_OFF` requires a separately indexed `ResearchCandidateAcceptanceV1` with `ACCEPTED` status and exact candidate/feature/watch refs; the handoff receipt then indexes immutable candidate, feature, setup and pipeline acceptance refs. It means research-pipeline acceptance only.

The immutable `CandidateActionV2` contains exact revision, policy/feature hashes, direction, decision/deadline/four-hour horizon, executable reference, collar, stop, cost-model ref, watch state version and evidence refs. `quantity=None`. The policy records IOC, fixed stop, four-hour time exit, no partial TP, no trailing, no pyramiding, no averaging and no same-epoch repricing. No CandidateSet, TradePlan, risk quantity, approval, reservation, order or venue mutation was created. Baseline S1 rejects a trade-VWAP enriched feature snapshot with an explicit variant-required reason; optional inputs cannot silently change its candidate identity.

**IMPLEMENTED:** Immutable shadow experiment specs have distinct hashes for simple 4H trend, no pullback, no SMC/candle/Fibonacci context, funding/OI, flow, combined funding/OI/flow and two stop-buffer neighbors. These are experiment identities for later comparison; no variant economic result or final CandidateSet ranking is claimed.

## Test evidence

- Targeted Session-014 tests: **TESTED**, 17 passed (pure math/technical/SMC/prefix, S1/restart/integration, Chronos-2 boundary).
- Prefix/future-tail: **TESTED**, fixed and Hypothesis arbitrary future tails cover technical series including EMA/RSI/ADX/ATR/realized/EWMA/Bollinger, candle geometry, VWAP, slope, forward Kalman, swing, BOS/CHoCH/FVG/sweep, zones, confirmed legs/Fibonacci/morphology, feature artifact hash, and missing regime axes.
- SMC timing: **TESTED**, pivot `i` unavailable before `i+2`; FVG at third close; BOS uses confirmed swing; CHoCH needs prior direction; order block appears with BOS; old events and zones are prefix invariant.
- All `tests/v2`: **TESTED**, 74 passed.
- Required V1 invariant smoke: **TESTED**, 11 passed.
- Ruff `src/atlas/v2 tests/v2`: **TESTED**; mypy same scope: **TESTED**, 57 source files; compileall same scope: **TESTED**; staged diff check: **TESTED** after final staging.
- Complete V1 regression was not run in this intermediate session. The full Phase-2 acceptance gate remains `TEST GATE`.

## Preservation, environment and remaining status

- V1 golden file: **TESTED**, byte-for-byte unchanged, SHA-256 `b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c`.
- Dependencies and `requirements-lock.txt`: **TESTED**, unchanged; lock SHA-256 `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.
- Environment: Python 3.12.13; Linux 5.15.0-177-generic x86_64, glibc 2.35; reused the locked Session-010 virtual environment. No credentials were needed.
- Secret scan: **TESTED**, repository scanner binaries unavailable; tracked-file plus final-diff fallback found zero high-confidence credentials, private keys or non-template `.env` files. No market data, caches, DB, logs, sandbox outputs or weights are staged.
- **NOT ESTIMABLE:** S1 economic value, alpha/profitability, variant/ablation value, foundation-model value and any after-cost policy comparison. This session has no outcome/evaluator pipeline.
- **TEST GATE:** V2 Phase-2 acceptance and S1 capital authority. Tier 4 is false, model influence is zero, `assisted_enabled=false`; the six V1 Bybit capabilities remain `UNVERIFIED / TEST GATE`. No capital path is enabled.
- **UNVERIFIED:** Exact Chronos-2 checkpoint/package; Windows worker isolation; authenticated venue behavior; any future enriched variant outcome.
- **BLOCKED BY ENVIRONMENT:** 72-hour public-data soak is not running; Session-012 status is carried forward without new evidence.

This handoff does not declare V2 Phase 2 complete. S2, selection/evaluator/risk quantity, desktop and capital work remain outside this branch.
