# Session 006 — Phase 4 baseline science and risk engine

## Identity

- BASE SHA: `055b2f7e790af0f01e80260e5ed5ec2dac685809`
- BRANCH: `impl/session-006-phase4-baseline-science-risk`
- PARTIAL CHECKPOINT REJECTED FOR COMPLETION: `09908939d877c4e85d8943573e36cb177c280fb7`.
- FINAL SHA: recorded after the final commit/push.

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

- `CRYPTO_TREND_24H_V1`: IMPLEMENTED as a pure frozen signal/policy contract.
- B0: IMPLEMENTED as the raw trend decision baseline without the LCB veto.
- A0: IMPLEMENTED as the separately typed LCB/ES decision baseline.
- Signal/EWMA, Decimal collar/stop/time policy: IMPLEMENTED, TESTED_OFFLINE.
- Huber/ridge mean-loss objective and causal OOF fit binding: IMPLEMENTED, TESTED_OFFLINE.
- Synchronized residual block sampling / bridge: IMPLEMENTED, TESTED_OFFLINE.
- IOC/no-fill/partial-fill/mark-stop and linear fee/funding accounting: IMPLEMENTED, TESTED_OFFLINE.
- Expected-mean LCB, empirical ES, deterministic risk reservation/sizing: IMPLEMENTED, TESTED_OFFLINE.
- Deterministic stress definitions: IMPLEMENTED, TESTED_OFFLINE.

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
- Corrected focused Phase-4 tests: `18 passed` in the locked environment.
- Initial base pre-flight in the host interpreter: `4 failed, 189 passed, 2 skipped`.
  The four existing archive tests failed only because `pyarrow` was absent from
  that interpreter (`EnvironmentDependencyError`); no baseline test was removed.
- Post-change host full suite: `4 failed, 194 passed, 2 skipped`, with the same
  four pre-existing PyArrow-environment failures only.
- Clean pinned hashed installation full suite: `199 passed, 1 skipped`.
- Ruff: PASS.
- mypy: PASS (`109 source files`; existing informational notes only).
- compileall: PASS.

- Tracked-only clean clone, hashed install and full suite: `199 passed, 1 skipped`.

- Corrected continuation clean locked full suite: `212 passed, 1 skipped`.

The existing engineering defaults remain `ENGINEERING_DEFAULTS_ONLY_NOT_SAFE_FOR_LIVE`.

## Safety / economics

- All six Bybit capability states: UNVERIFIED (unchanged).
- `assisted_enabled=false` (unchanged).
- Economic validation: UNVERIFIED.  No profitability, safety, alpha, or
  live-readiness claim is made from this offline implementation.
- No live or test order was submitted.

## Remaining work

Phase 5 alert/scanner orchestration and Phase 6 execution/approval integration
remain explicitly out of scope.  Forward causal archive accumulation and real
execution/funding/calendar calibration are required before a deployed-policy
study can be estimable.
