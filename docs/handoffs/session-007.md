# Session 007 — Phase-4 final micro-fix and Phase-5 continuous scanner

## Identity

- PHASE-4 BRANCH: `impl/session-006-phase4-baseline-science-risk`
- PHASE-4 FINAL MICRO-FIX SHA: `f4f082fa5be50db4f770027a33aec1be1ab34087`
  (`fix: make bootstrap oof lookup timestamp causal`)
- PHASE-5 BRANCH: `impl/session-007-phase5-continuous-scanner-alert`
- PHASE-5 IMPLEMENTATION SHA: `bcfc9b4e96dd0bbf5013da51b419581be2755126`
  (`feat: implement phase 5 continuous scanner and alerts`)
- PHASE-5 FINAL SHA: recorded by the documentation commit that immediately
  follows this implementation SHA and reported in the session final report.

## Stage A — Phase-4 bootstrap chronology repair

`replicate_oof_hours()` no longer carries a one-way refit cursor across the
sampled bootstrap sequence.  Each sampled hour now performs a deterministic
`bisect_right` lookup over the sorted refit instants and uses the latest
`refit.fit_at_ns <= hour.at_ns`.  Nonchronological block concatenation can no
longer drop or mis-bind earlier sampled hours.

Focused regression:

```text
tests/science/test_outer_bootstrap.py
tests/science/test_prefix_invariance_phase4.py
PASS
```

Phase 4 is frozen at `f4f082fa5be50db4f770027a33aec1be1ab34087`.

## Stage B — implemented

- immutable point-in-time universe snapshots with retained delisted/excluded
  history, explicit availability/effective fields and content hash;
- deterministic, versioned `SCANNER_PRIORITY_ONLY_NOT_ALPHA` cheap score with
  persisted causal inputs and input hash;
- deterministic rank, canonical instrument-ID tie-break, rank bands and
  past-only correlation clusters;
- frozen top-3 queue, top-5 deep budget and always-warm BTCUSDT/ETHUSDT;
- deterministic B1/B2/B3 shadow exploration with UTC-slot rotation, stable hash,
  known inclusion probability and cyclic fallback;
- typed warmup/deadline outcomes, including `NOT_ESTIMABLE_WARMUP` and
  `MISSED` without stale/partial substitution;
- complete per-instrument scanner decision calendar (including excluded
  instruments and `NULL`/`NOT_APPLICABLE` later-phase fields);
- injected Phase-4 handoff port plus `evaluator_from_phase4()` binding to the
  existing frozen `evaluate_phase4()` pipeline; qualified BTC/ETH TradePlans
  are reused, never recomputed;
- exploration candidates receive no Phase-4 capital evaluation and no plan;
- deterministic alert-only projections with an injected credential-free
  transport stub;
- scanner health/status with `assisted_enabled=false` and all six Bybit
  capabilities `UNVERIFIED`;
- minimal blind-spot estimators with decision-time block bootstrap and IPW only
  for known exploration probabilities;
- paired full-calendar scanner revision comparison, rejecting executed-trades-
  only evidence;
- existing `ResearchArtifactArchive` (Parquet/Arrow content-addressed) used for
  all scanner artifact families.

## Validation

Development loop used only the Phase-4 bootstrap regression plus `tests/scanner`.

Single clean tracked-only validation at implementation SHA
`bcfc9b4e96dd0bbf5013da51b419581be2755126` (fresh Python 3.12.13 venv,
`pip install --require-hashes -r requirements-lock.txt`):

```text
PYTHONPATH=src:. python -m pytest -ra     359 passed, 1 skipped
PYTHONPATH=src:. python -m ruff check     PASS
PYTHONPATH=src:. python -m mypy src tests PASS
PYTHONPATH=src:. python -m compileall     PASS
git diff --check                           clean
```

The skipped test is the existing opt-in public testnet check; no credentials or
network were used.  Secret/execution scan found no credentials, `.env`, CCXT,
order submission or second OMS.  No live or test order was submitted.

## Assumptions

- The freeze does not prescribe a cheap-score formula, so the smallest
  transparent heuristic was used: `abs(return_24h) / max(volatility_24h, eps)`.
  It is explicitly labeled `SCANNER_PRIORITY_ONLY_NOT_ALPHA` and never
  authorizes capital.
- Rich scanner inputs (causal returns, deep warmup availability, counterfactual
  values) are supplied by the existing data/science layers; the scanner does not
  recreate features, models, scenarios, risk, replay or TradePlan construction.
- Alert transport is an injected port; no Telegram or other credential-bearing
  transport is implemented.

## UNVERIFIED / INCONCLUSIVE conditions

- Economic validation remains UNVERIFIED; no profitability claim is made.
- All six Bybit capability states remain UNVERIFIED.
- `assisted_enabled=false`; Phase 6 approval/execution controls are not present.
- Missing, partial or late deep evidence remains `NOT_ESTIMABLE_WARMUP` with a
  visible deadline outcome.
- Blind-spot metrics return `INCONCLUSIVE` when slot support, known exploration
  probability or matured counterfactual support is inadequate.
- Revision comparison returns `INVALID_UNPAIRED` when the paired complete
  calendars differ.

## Remaining Phase-6 work

Plan-version-bound one-use approval, fresh quote/account/risk revalidation,
atomic reservation and durable command before submit, pause/protect/close/
flatten controls, recovery certificate and the assisted control shell.  Phase 5
does not implement any of these and does not submit orders.

No profitability claim is made. Economic validation remains UNVERIFIED. No
live/test order was submitted. All six Bybit capabilities remain UNVERIFIED.
`assisted_enabled=false`.
