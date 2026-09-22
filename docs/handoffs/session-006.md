# Session 006 — Phase 4 baseline science and risk engine

## Identity

- BASE SHA: `055b2f7e790af0f01e80260e5ed5ec2dac685809`
- BRANCH: `impl/session-006-phase4-baseline-science-risk`
- REVIEWED PARTIAL CHECKPOINTS (not completion):
  - `09908939d877c4e85d8943573e36cb177c280fb7`
  - `0e56db64f1bedc7fd1338e68d31ab03044f0b511`
  - `6d974c49db1127aff2b02f5db0092471bf1a46b7` — REJECTED for Phase-4 completion.
- FINAL IMPLEMENTATION SHA: `32de28695870afd4adb0c05e43bf011130b9a8b7`
  (`fix: close phase 4 integration review gaps`).  This document is recorded in
  the documentation commit that immediately follows it.

## Validation of `32de28695870afd4adb0c05e43bf011130b9a8b7`

- Python 3.12.13; locked environment with PyArrow 25.0.1.
- Full suite (clean hashed install): `315 passed, 1 skipped`.
- Ruff: PASS (0 errors).  mypy: PASS (136 source files, no issues).
- compileall: PASS.  `git diff --check`: clean.
- Tracked-only clean clone of the branch + fresh hashed install: `315 passed,
  1 skipped`, Ruff PASS, mypy PASS, working tree clean.
- Secret scan: no credentials, API keys, tokens, `.env`, CCXT or order transport
  introduced by this patch.

## Final closed-scope Phase-4 repair (starting SHA `82d22a3d721a1476de02071fcfb99d9192b5b4bb`)

Independent review of the `82d22a3` branch tip closed the remaining five
implementation defects:

1. **Genuine chronological OOF in the outer bootstrap.** `replicate_oof_hours()`
   now refits the scaler/model at the frozen Monday cadence *inside the replicate
   sample*, so every historical forecast uses only labels that matured before its
   origin; the final replicate fit never rewrites an earlier forecast.  The
   replicate rebuilds `residual = realized_standardized_target − stored_forecast`
   from those causal forecasts.  The already selected penalty is passed in as
   `locked_ridge` (via the shared `select_ridge_chronological` contract), so no
   replicate performs another hyper-parameter search.  `rebuild_replicate_hours()`
   was removed.
2. **Simulated features evolve through the 24h path.** `joint_minute_paths()` now
   takes the actual causal 721-close window per instrument plus the frozen model
   instead of a loose `(z, sigma)` pair.  After each simulated completed hour the
   window is advanced (`append_simulated_close`) and `sigma*`, `z*`, `mu*` are
   recomputed with the existing frozen feature engine (`feature_values` +
   `finite_window_variance`); the model coefficients stay frozen for the whole
   path.
3. **Causal funding forecast in the outer replay.** Settlements come from the
   existing `forecast_funding()`: the current decision-time anchor (latest
   observable predicted rate, or the latest settled rate with the explicit
   downgrade label) plus the *settlement-to-settlement changes* sampled from the
   replay block.  Historical absolute rates are never replayed as future rates.
   Entry latency is read from the same sampled path (`path_latency_ns`) instead
   of a replicate-wide median; missing path latency/spread/depth/funding evidence
   is `NOT_ESTIMABLE`.
4. **Stress/risk evidence bound to the frozen action.** `Phase4ScenarioEvaluation`
   gained a `risk_inputs_hash` (canonical hash of the deterministic
   `CandidateRiskInputs`), and `evaluate_phase4()` now also rejects a stress
   template whose side, `current_mark` or quantity does not match the frozen
   policy/selected quantity.  All mismatches fail closed with `NOT_ESTIMABLE`
   before any TradePlan can be created.
5. **Exact frozen block selection.** `select_block_length_frozen()` and
   `chronological_energy_scores()` default to `candidate_stride=None` and
   `max_scenarios=None`, so production evaluates every eligible start of the
   frozen history.  The exact pairwise cloud term of the energy score
   (`pairwise_cloud_distance`) is computed once per candidate/horizon/window —
   identical definition, eligible starts, BTC/ETH and 4h/24h weighting,
   training-vol standardization and tie-breaking — only the summation is
   symmetric and the redundant per-observation recomputation removed.

Every repair reuses an existing authoritative Phase-4 function; no parallel
feature, OOF, bootstrap, funding, risk or execution implementation was added.

### Final repair record

```text
starting SHA:       82d22a3d721a1476de02071fcfb99d9192b5b4bb
implementation SHA: 1f458ee47ec8072783403b2146d1dd281742cbfe  (fix: finalize phase 4 causal evaluation)
final branch SHA:   recorded in the documentation commit that follows this document
```

Closed acceptance matrix for this repair (all evaluated offline):

```text
1. CHRONOLOGICAL BOOTSTRAP OOF          PASS
2. LOCKED BOOTSTRAP RIDGE               PASS
3. DYNAMIC SIMULATED FEATURE STATE      PASS
4. CAUSAL FUNDING FORECAST REPLAY       PASS
5. SAME-BLOCK EXECUTION EVIDENCE        PASS
6. STRESS/RISK ACTION BINDING           PASS
7. EXACT FROZEN BLOCK SELECTION         PASS
```

Single clean tracked-only validation of `1f458ee` (fresh Python 3.12.13 venv,
`pip install --require-hashes -r requirements-lock.txt`, PyArrow
25.0.1): `328 passed, 1 skipped`; Ruff PASS; mypy PASS (136 source files);
`compileall` PASS; `git diff --check` clean; working tree clean; dependency lock
SHA256 unchanged at
`0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.

## Independent review of `6d974c49db1127aff2b02f5db0092471bf1a46b7`

Independent review rejected that checkpoint for Phase-4 completion because of:

1. current-state bridge mismatch — the bridge replayed the archived realized
   hour return instead of applying the frozen current-state bridge
   (`epsilon = archive_return / archive_sigma - archive_mu / 60`, then
   `r*_minute = sigma_current * (mu_current / 60 + epsilon)`);
2. bootstrap refit not propagated into scenario P&L — refitted coefficients and
   rebuilt OOF residuals were computed but the replay still used the archived
   forecast/residual unchanged;
3. fabricated bootstrap microstructure and omitted funding — outer replay
   synthesized bid/ask/depth from candle prices and called the replay with empty
   settlements;
4. hard-risk evidence not bound to the actual stress/ES results — caller-supplied
   per-unit stress loss and ES contribution could size optimistically;
5. unbound scenario evidence artifacts — bootstrap, paths, manifests, policy and
   snapshot could be mixed without proof they belong together;
6. execution timing gaps — stop latency was exposed but not applied, entry could
   use an in-progress minute, and the T+24h escalation skipped the refreshed
   bounded retry;
7. non-frozen block-validation orchestration — block selection used an arbitrary
   8-day/24-hour window instead of the frozen three 7-day windows;
8. incomplete decision-calendar coverage — an arbitrary slot list could claim
   complete coverage while omitting 20:00 UTC.

## Targeted repair (this patch)

1. **Current-state bridge** (`scenarios.py`): `bridge_hour`/`joint_minute_paths`
   take an explicit `CurrentModelState(sigma, mu)` per instrument. Historical
   blocks supply archive sigma, archive OOF mu and the archived minute
   innovation; the simulated minute return is
   `current_sigma * (current_mu/60 + innovation/archive_sigma + residual/60)`,
   which is algebraically the frozen epsilon formula. Archived-hour coherence
   (`realized/sigma == mu + residual`) is validated; mark/index stay paired with
   the same sampled block and keep the sampled relative basis bounded.
2. **Bootstrap refit propagation** (`outer_loop.py`, `huber_mean.py`): the frozen
   three-window ridge-selection contract was extracted once and reused by both
   the weekly refit and each replicate (`select_ridge_chronological`). Replicates
   rebuild ``mu``/residual from the refitted coefficients via
   `rebuild_replicate_hours`, so refit uncertainty now moves the scenario inputs
   and the replicate P&L. Dead OOF-residual calculations were removed.
3. **Evidence-only execution replay** (`outer_loop.py`): quotes, displayed depth,
   entry latency, taker fee and funding settlements are read from the SAME
   sampled block; missing spread/depth/latency/funding evidence is
   `NOT_ESTIMABLE`. Nothing is derived from OHLC, and the replay is never called
   with empty settlements when funding evidence is required.
4. **Hard-risk binding** (`phase4_engine.py`, `risk/sizing.py`): the caller no
   longer supplies a `PerUnitRisk`. The engine derives deterministic scalable
   per-unit values, sizes once, re-evaluates the actual stress suite at the
   SELECTED quantity, builds the final risk vector from that actual stress loss,
   checks the venue-collateral limit against account evidence, and enforces the
   portfolio ES hard limit from `ES_after` of the common-path calculation.
   Failures return `NO_TRADE_RISK` without any smaller-size search.
5. **Action/evidence binding** (`phase4_engine.py`): new immutable
   `Phase4ScenarioEvaluation` carrying snapshot hash, action hash, quantity,
   RiskPolicy hash, model/block/scenario manifests, seed identity, candidate and
   portfolio path hashes, the bootstrap result and the stress template. The
   engine verifies every binding (including `bootstrap.action_hash`) and fails
   closed with `NOT_ESTIMABLE` on any conflict.
6. **Execution timing** (`execution_replay.py`, `policy_replay.py`): stops now
   execute at `trigger + resolution + supplied latency` (never on the trigger
   bar); entry uses the first complete minute after arrival (a bar already in
   progress at send/arrival is not fillable); the T+24h exit is a bounded IOC,
   then a refreshed bounded retry, then the two-second market escalation, then a
   declared bound or `NOT_ESTIMABLE`. A stop whose execution falls at/after the
   horizon defers to the frozen time exit instead of a fabricated fill.
7. **Frozen block-validation windows** (`residual_blocks.py`): new
   `select_block_length_frozen` uses the same three non-overlapping 7-day
   windows that end at the Monday refit instant with 60 days of preceding
   support, plus non-overlapping candidate scenarios bounding the quadratic
   energy objective. The end-to-end fixture now exercises that production
   orchestration.
8. **Complete decision calendar** (`decision_calendar.py`): `frozen_slot_grid`
   plus `evaluate_calendar_interval`/`assert_complete` require every 00/04/08/12/
   16/20 UTC slot for BTCUSDT and ETHUSDT; the end-to-end test now covers all six
   daily slots, and a missing 20:00 UTC slot fails completeness validation.

## Independent review of `0e56db64f1bedc7fd1338e68d31ab03044f0b511`

Independent review of the `0e56...` checkpoint found three outstanding defects
(recorded here exactly as found, not rewritten):

1. block-selection scoring used residual-only returns and omitted the archived
   OOF conditional mean contribution;
2. deterministic stress evaluation invented unsupported venue/path mechanics
   (for example arbitrary exit-delay notional formulas and margin multipliers);
3. full production Phase-4 orchestration (outer bootstrap, complete policy
   replay, A0/B0 wiring, decision calendar, end-to-end fixture) remained
   incomplete.

## Continuation corrections (this patch)

1. **Block-selection return reconstruction**: `chronological_energy_scores()`
   now scores the archived standardized process `y = mu_OOF + epsilon` as the
   joint return `r = sigma * (mu_OOF + epsilon)` for BTC and ETH, standardized
   by training volatility, with equal 4h/24h weighting, no cost or barrier
   terms, and the frozen longer-block tie-break.
2. **Stress engine**: replaced invented venue mechanics with typed,
   evidence-only inputs (`StressExecutionAssumptions`, `StressMarginAssumptions`,
   `StressFundingAssumptions`, `StressCollateralAssumptions`).  Missing
   stressed prices/depth/spread/divergence/correlation, margin tier schedules,
   liquidation thresholds/costs or mechanics now return `NOT_ESTIMABLE` instead
   of a fabricated number.  Venue collateral loss and collateral haircut are
   account-level losses, never trade stop-out losses.
3. **Synchronized joint residual archive**: per-hour last/mark/index minute
   evidence, feature references, gaps/excursions, spread/depth and latency/fill
   observations, funding publication/settlement times, availability and replay
   class, calendar/universe identity and evidence hashes; exactly 60 bars per
   series whenever minute replay is claimed complete, otherwise explicit absence.
4. **last/mark/index minute bridge**: anchored to the current causal snapshot,
   paired with the same sampled BTC/ETH residual block, preserving normalized
   innovations, opening gaps and high/low excursions without clipping, and
   failing closed on unsupported volatility, excess basis drift, broken
   reconstruction, insufficient archive support or poor barrier calibration.
5. **Complete 24h policy replay**: proposal/approval/send delay, arrival, IOC
   entry with NO_FILL/PARTIAL_FILL/FULL_FILL, no chase, fixed absolute
   MarkPrice stop with gap/latency/depth/spread-impact bounds, partial stop and
   time-exit lots, fees, funding settlements, the 25bp T+24h opposite-quote
   collar and the frozen unresolved-exit escalation (`TIME_EXIT`,
   `EXTENDED_EXIT`, `NOT_ESTIMABLE` for unbounded extension).  Lot conservation
   is enforced and reversal is impossible.
6. **Outer bootstrap orchestration**: block-resampled replicates that refit the
   scaler and Huber model, run frozen ridge selection, rebuild chronological OOF
   calibration support, re-estimate execution/cost assumptions from supplied
   observations, and evaluate the *same* immutable current action.  LCB is the
   1-indexed `ceil(B * delta)` replicate-mean order statistic, with two
   independent inner seed sets and `NO_TRADE_NUMERICAL` on instability.
   Production defaults remain `B=200`, `inner=512`, `delta=0.025`, family error
   `0.05`; tests inject small counts.
7. **Portfolio ES/J on common paths**: `Pi0 + Y` losses, empirical ES at
   `alpha=0.975`, incremental ES and `J = LCB(E[Y])/E - (ES_after - ES_before)`
   with `lambda_R = 1`, requiring `LCB > 0` and `J > 1e-5`.  Pending/UNKNOWN
   exposure is included in `Pi0`; a negative-alpha action cannot qualify only by
   lowering ES.
8. **Deterministic risk sizing**: the largest venue-rounded quantity satisfying
   per-trade/aggregate normal loss, stress loss, ES headroom, gross/instrument/
   beta notional, margin/free-margin reserve, venue collateral, leverage and
   serial new-risk intents, solved once before statistical acceptance; A0 is
   then run exactly once at that quantity (no LCB quantity search).
9. **A0/B0 orchestration**: one pure offline evaluator with the frozen
   fail-closed ordering `SKIP_DATA -> gate -> NO_SIGNAL -> NOT_ESTIMABLE ->
   NO_TRADE_RISK -> NO_TRADE_NUMERICAL -> NO_TRADE_NO_EDGE -> TRADE_CANDIDATE`;
   B0 shares direction, execution mechanics and hard constraints but has no
   statistical expected-alpha veto.
10. **TradePlan + evidence artifacts**: qualified candidates map to the existing
    immutable `TradePlan` (strategy/version, snapshot hash, RiskPolicy hash,
    sized quantity, IOC collar, fixed MarkPrice stop, management policy, T+24h
    horizon, T+60s expiry, cost evidence, normal/stress risk, margin, leverage,
    reference mark, created/available times) plus a separate evaluation evidence
    artifact (gates, model/block/scenario hashes, LCB, ES before/after, J, stress
    results, rejection and NOT_ESTIMABLE reasons).  No plan is executed.
11. **Decision calendar**: chronological BTC/ETH four-hour evaluator persisting
    one immutable outcome per slot/instrument, including rejects and no-fills
    with zero P&L, plus the matured fill/exit statuses, and a generic Phase-5
    handoff record that deliberately contains no universe/scanner/ranking
    fields.
12. **Research persistence**: append-only content-addressed Parquet artifacts
    for weekly fit manifests, OOF forecasts, matured residuals, joint residual
    blocks, block selection, scenario configuration, bootstrap results, stress
    results, B0/A0 evaluation, TradePlan evidence, decision-calendar records and
    failed/inconclusive experiment variants.  DuckDB remains research-only; the
    authoritative SQLite schema (6) is unchanged.
13. **End-to-end fixture**: one deterministic chronological synthetic fixture
    (721-close warmup, 90+ day model support, Monday refits, OOF maturation,
    61-day synchronized residual support, block selection, 4-hour decisions,
    every rejection path, NO_FILL/PARTIAL_FILL/FULL_FILL/STOP_EXIT/TIME_EXIT/
    EXTENDED_EXIT, fees, multiple funding settlements, A0 vs B0, TradePlan
    creation and decision-calendar persistence) with small injected scenario
    counts.  It proves plumbing only; it is not a profitability claim.
14. **Prefix/future-tail invariance**: tests mutating future closes, labels, OOF
    outcomes, minute bars, depth/cost observations and event records prove that
    earlier features, weekly fits, OOF forecasts, bridges, block selection,
    decisions, TradePlans and calendar records do not change.

`huber_mean.fit_huber_ridge` received a numerics-preserving inner-loop
optimization (identical accumulation order, hoisted weight products) so the
frozen IRLS fits are affordable in the integration fixture; the Huber tests
confirm unchanged coefficients and objective values.

## Independent-review correction record

The partial checkpoint was not a Phase-4 completion.  Review found: mean-loss
Huber/ridge penalty scaling mismatch; OOF forecasts did not reject future fits;
the horizon was incorrectly anchored to plan creation; time-exit rounding was
reversed; missing execution evidence was represented as no-fill; venue
collateral loss was represented as trade loss; focused test coverage and full
orchestration were incomplete.  The current continuation corrects these code
contracts and adds dedicated regression tests.  It remains PARTIAL until the
complete-policy, persistence, bootstrap, bridge and calendar requirements are
fully exercised.

## Offline implementation status

- `CRYPTO_TREND_24H_V1`, B0, A0: IMPLEMENTED, TESTED_OFFLINE (complete policy engine).
- Signal/EWMA, Decimal collar/stop/time policy: IMPLEMENTED, TESTED_OFFLINE.
- Huber/ridge mean-loss objective, Monday refit, causal OOF archive: IMPLEMENTED, TESTED_OFFLINE.
- Forecast+residual energy-score block selection (4h/24h, BTC/ETH): IMPLEMENTED, TESTED_OFFLINE.
- Synchronized joint residual archive, last/mark/index minute bridge: IMPLEMENTED, TESTED_OFFLINE.
- Complete IOC/stop/T+24h time-exit/escalation replay and lot conservation: IMPLEMENTED, TESTED_OFFLINE.
- Funding forecast, settlement accounting and reserve limits: IMPLEMENTED, TESTED_OFFLINE.
- Outer expected-mean LCB bootstrap, common-path portfolio ES/J: IMPLEMENTED, TESTED_OFFLINE.
- Deterministic hard-constraint risk sizing and drawdown state: IMPLEMENTED, TESTED_OFFLINE.
- Evidence-valued deterministic stress suite (12 frozen stresses): IMPLEMENTED, TESTED_OFFLINE.
- TradePlan mapping, immutable evaluation artifacts, decision calendar: IMPLEMENTED, TESTED_OFFLINE.
- Append-only Parquet research persistence: IMPLEMENTED, TESTED_OFFLINE.

No order transport, credential handling, account mutation, scanner, approval UI, or assisted control was added.

## Explicit numerical choices

The three-parameter Huber/ridge solver is deterministic IRLS with pivoted
three-by-three elimination and tolerance `1e-10`; this is a standard small
numeric implementation where the freeze did not mandate a package/solver.
It uses no new dependency.  Ties use the frozen larger ridge/block choice.

## First-class not-estimable conditions

The implementation rejects unavailable/future close windows, insufficient
matured labels/validation folds, no contiguous synchronized blocks, inadequate
block support, unavailable execution data, missing A0 uncertainty evidence,
and unsupported policy/execution inputs.  `NOT_ESTIMABLE` is represented as a
typed decision status, never a substituted numeric alpha estimate.

## Validation

- Python: `3.12.13`.
- Dependency lock SHA256: `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.
- New dependencies: none (the implementation uses the standard library only).
- Previous reviewed partial checkpoints (kept for history):
  - `09908939d877c4e85d8943573e36cb177c280fb7`: `199 passed, 1 skipped` (clean locked suite).
  - `0e56db64f1bedc7fd1338e68d31ab03044f0b511`: `212 passed, 1 skipped` (clean locked suite).
  - `6d974c49db1127aff2b02f5db0092471bf1a46b7`: `296 passed, 1 skipped` (clean locked suite), rejected on review.
- Targeted repair (locked environment with PyArrow, clean hashed install):
  see the final report for exact pytest/Ruff/mypy/compileall counts.

The existing engineering defaults remain `ENGINEERING_DEFAULTS_ONLY_NOT_SAFE_FOR_LIVE`.

## Safety / economics

- All six Bybit capability states: UNVERIFIED (unchanged).
- `assisted_enabled=false` (unchanged).
- No authenticated Bybit, no orders/test orders, no exchange mutation, no
  approval workflow, no capital reservation side effect, no scanner, no top-K
  ranking, no foundation model, no CCXT and no second OMS were added.
- Economic validation: UNVERIFIED.  No profitability, safety, alpha, or
  live-readiness claim is made from this offline implementation.
- No live or test order was submitted.

## Phase-5 interface boundary (not implemented)

Phase 4 finishes a generic immutable decision artifact (slot identity,
instrument, strategy version, availability cutoff, feature-snapshot hash,
signal, gate/risk/scenario status and reasons, B0/A0 results, TradePlan
id/hash, rejection and NOT_ESTIMABLE reasons, matured outcome reference).
Phase 5 items — unattended universe refresh, cheap scan, top-K/deep warmup,
deterministic exploration, alert-only mode, health/status and scanner
decision-calendar persistence — remain unimplemented.

## NOT_ESTIMABLE conditions

Missing microstructure, missing calendar, missing margin tier or liquidation
mechanics, missing funding anchor, unsupported volatility, insufficient OOF or
block support, insufficient archive support for the mark/index bridge, poor
barrier calibration, unbounded exit extension, missing cost observations and
numerically unstable acceptance all propagate a typed `NOT_ESTIMABLE` (or an
explicitly declared conservative bound) rather than a convenient number.

## Remaining work

Phase 5 alert/scanner orchestration and Phase 6 execution/approval integration
remain explicitly out of scope.  Forward causal archive accumulation and real
execution/funding/calendar calibration are required before a deployed-policy
study can be estimable.
