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
- Rich decision-time inputs (causal returns, deep warmup availability) are
  supplied by the existing data/science layers; counterfactual outcomes are
  appended later through `ScannerCalendar.mature()` and are never available to
  `run_scan_slot()`.  The scanner does not recreate features, models, scenarios,
  risk, replay or TradePlan construction.
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

---

# Final Phase-5 completion patch

## Identity

- STARTING SHA: `bc420c462592936b566dfb1e5f5096f26b3ba29c`
- IMPLEMENTATION SHA: `5df212c7a4cd90430633fdf4cacf7ceb4d67ebea`
  (`fix: complete phase 5 unattended scanner evidence flow`)
- FINAL SHA: the documentation commit immediately following the implementation
  SHA; reported in the session final report.

## Repaired review blockers

1. **Unattended scanner/universe runner.** Added `src/atlas/scanner/runner.py`
   with `ScannerRunner`, `due_slot()` and minimal injected
   `UniverseProvider` / `CheapInputProvider` / `WarmupEvidenceProvider` ports.
   The runner resolves the latest due four-hour UTC slot, validates that every
   supplied artifact was available by that slot, delegates to the existing
   `run_scan_slot()`, persists the complete result and returns
   `ALREADY_COMPLETED` for an already-recorded slot.  `missed_slots()` and
   `run_catch_up()` are deterministic diagnostics only; later evidence is
   rejected rather than backdated.
2. **Universe contract-spec binding.** `UniverseEntry` now carries
   `contract_spec_ref`, `contract_spec_hash` and
   `contract_spec_available_at_ns`.  The reference must be available by the
   universe cutoff, and the snapshot content hash includes the contract-spec
   identity, so revised/future metadata cannot rewrite an older snapshot.
3. **Causal scanner-outcome maturation.** `run_scan_slot()` no longer accepts
   future counterfactual values.  Decision-time rows always carry
   `counterfactual_value=NULL` and no matured IDs.  Outcomes are appended with
   `ScannerCalendar.mature()` only at/after the horizon, bound to the original
   row hash, label/outcome IDs and evidence hash.  Repeated identical
   maturation is idempotent; conflicting maturation is rejected; original
   ranking/selection/TradePlan rows are unchanged.  Blind-spot inputs come from
   `observations_from_matured()`, and decision-time blind-spot output is
   `INCONCLUSIVE` until matured support exists.
4. **Warmup/deadline denominators.** `BlindSpotObservation` now carries
   `warmup_required` and `deadline_applicable`.  `blindspot_metrics()` computes
   warmup exclusion only over required deep/warmup observations and deadline
   loss only over applicable deadlines; `NOT_SELECTED` / `NOT_APPLICABLE`
   instruments no longer count as failures.

## Focused tests

```text
tests/scanner/test_phase5_end_to_end.py
tests/scanner/test_scanner_calendar.py
tests/scanner/test_scanner_blindspots.py
tests/scanner/test_scanner_warmup.py
tests/scanner/test_universe_snapshot.py
tests/scanner/test_scanner_runner.py
PASS

tests/science/test_outer_bootstrap.py
tests/science/test_prefix_invariance_phase4.py
PASS
```

## Clean validation at `5df212c7a4cd90430633fdf4cacf7ceb4d67ebea`

One clean tracked-only worktree, one fresh Python 3.12.13 venv, one hashed lock
install:

```text
PYTHONPATH=src:. python -m pytest -ra     369 passed, 1 skipped
PYTHONPATH=src:. python -m ruff check     PASS
PYTHONPATH=src:. python -m mypy src tests PASS
PYTHONPATH=src:. python -m compileall     PASS
git diff --check                           clean
```

The skipped test is the existing opt-in public testnet check.  No credentials,
authenticated exchange mutation, order submission, second OMS or Phase-6
control was added.

## Environment status

- Unattended public Bybit universe/contract collection: **BLOCKED BY
  ENVIRONMENT** (injected provider boundary and deterministic fixtures only; no
  authenticated exchange calls).
- Economic validation: **UNVERIFIED**.
- All six Bybit capability states: **UNVERIFIED**.
- `assisted_enabled=false`; Phase 5 remains research/alert only.

No profitability claim is made. Economic validation remains UNVERIFIED. No
live/test order was submitted. All six Bybit capabilities remain UNVERIFIED.
`assisted_enabled=false`.
