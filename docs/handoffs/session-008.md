# Session 008 — Phase-5 runner finalization and Phase-6 assisted control shell

## Identity

- PHASE-5 BRANCH: `impl/session-007-phase5-continuous-scanner-alert`
- PHASE-5 FINAL SHA: `248879b60d81877aa5dba295e1d8243637609a7c`
  (`fix: finalize phase 5 scanner runner semantics`)
- PHASE-6 BRANCH: `impl/session-008-phase6-assisted-control-shell`
- PHASE-6 IMPLEMENTATION SHA: `29bdd5020efd3d2dc7163cb8a38813d8cbacae2b`
  (`feat: implement phase 6 assisted control shell`)
- PHASE-6 FINAL SHA: the documentation commit immediately following the
  implementation SHA; reported in the session final report.

## Stage A — final Phase-5 runner repair

Two runner defects were closed without changing `run_scan_slot()` or Phase-4/5
architecture:

1. **Post-slot warmup timestamps are valid.** The runner still requires
   universe and cheap-input evidence to be available by slot `T`, but warmup
   lifecycle timestamps are now validated against the actual runner `now` and
   ordered (`enqueued <= started <= finished`). A `T+20s` completion before the
   `T+30s` deadline becomes `WARM_AVAILABLE/MET`; `T+40s` reaches
   `evaluate_warmup()` and becomes `NOT_ESTIMABLE_WARMUP/MISSED`.
2. **CATCH_UP is diagnostic-only.** `LIVE` is the only mode that writes the
   authoritative `ScannerCalendar` and normal research artifacts. `CATCH_UP`
   uses a temporary calendar, `persist=False`, and a credential-free recording
   transport, returns `mode=CATCH_UP status=DIAGNOSTIC_ONLY`, and cannot create
   an authoritative historical live result.

Stage-A focused tests passed:

```text
tests/scanner/test_scanner_runner.py
tests/scanner/test_scanner_warmup.py
tests/scanner/test_phase5_end_to_end.py
PASS
```

Phase 5 is frozen at `248879b60d81877aa5dba295e1d8243637609a7c`.

## Stage B — Phase-6 implemented

One focused production module, `src/atlas/runtime/assisted_control.py`, wraps
existing durable control primitives:

- plan/version/user/expiry-bound one-use `Approval` validation;
- fresh quote/mark/account/RiskPolicy/recovery/runtime revalidation without
  rerunning strategy science;
- persisted `RecoveryCertificate` gate requiring `READY`, `CURRENT`
  reconciliation and no unresolved intents/commands/UNKNOWN work;
- atomic `consume_approval_with_intent_reservation()` reuse;
- existing entry/exit/market-exit wire contracts;
- durable `Command` persistence before any dispatch;
- UNKNOWN-safe acknowledgement handling: `SUBMIT_UNKNOWN`, same intent, same
  `client_order_id`, reservation retained, and no automatic retry;
- narrow injected `NautilusCommandPort`; Nautilus remains the operational
  order/fill/position authority;
- pause/new-risk latch that never blocks risk-reducing controls;
- protect only through `BybitProtectionPort.ensure_full_stop()`;
- explicit-quantity reduce-only close and market flatten paths using the
  existing no-reversal invariant;
- small `AssistedControlStatus` projection.

The only persistence change is a focused `SQLiteJournal.load_approval()` read
helper. No schema change, table or competing OMS was added.

### Offline dispatch semantics

The public production gate is hard-disabled with the current repository state:
`assisted_enabled=false` and all six Bybit capabilities `UNVERIFIED`.
Production entry dispatch therefore persists durable state but refuses before
any external transport call.

Offline fake-port tests exercise the private durable dispatch-state function
only after a command exists, verifying at the port boundary that:

```text
command row exists
send_started_at is durable
intent is in a dispatch-prepared lifecycle
reservation already exists
```

Definite accept does not infer a fill. Definite reject does not release the
reservation. Timeout/UNKNOWN retains the same intent, client order ID and
reservation, and a second opening preparation is rejected locally.

## Validation

Focused Phase-6 tests:

```text
tests/runtime/test_assisted_control_approval.py
tests/runtime/test_assisted_control_revalidation.py
tests/runtime/test_assisted_control_dispatch.py
tests/runtime/test_assisted_control_risk_reduction.py
tests/runtime/test_phase6_end_to_end.py
PASS
```

Affected runtime/persistence suites and the final Phase-5 runner regression
also pass.

One clean tracked-only worktree at implementation SHA
`29bdd5020efd3d2dc7163cb8a38813d8cbacae2b`, one fresh Python 3.12.13 venv, one
hashed lock install:

```text
PYTHONPATH=src:. python -m pytest -ra     394 passed, 1 skipped
PYTHONPATH=src:. python -m ruff check     PASS
PYTHONPATH=src:. python -m mypy src tests PASS
PYTHONPATH=src:. python -m compileall     PASS
git diff --check                           clean
```

The skipped test is the existing opt-in public testnet check; no credentials or
network were used.

## Safety / offline-only status

- No live or testnet order was submitted.
- No authenticated Bybit or other exchange call was added.
- No credentials, API keys, secrets, `.env` content, CCXT or direct ordinary
  Bybit order REST was added.
- Research/scanner code cannot mutate capital.
- `assisted_enabled=false` and all six Bybit capability states remain
  `UNVERIFIED`.
- Actual Bybit protected-control and ordinary execution remain **BLOCKED BY
  ENVIRONMENT / TEST GATE**.

## Assumptions

- The shell receives an already-authenticated `user_identity`; no UI or
  authentication system is implemented.
- Fresh quote/mark evidence follows the frozen V1 one-second freshness limit.
- Recovery READY evidence is constructed through the existing persisted
  reconciliation/flat-certificate machinery, not caller booleans.
- Nautilus remains the normal operational order/fill/position authority; the
  injected port is only a command carrier.

## Exact remaining §8.2 qualification gates

1. Pinned Nautilus/Bybit wire behavior on the actual installed artifact and
   target account profile (attached stop, partial-fill resizing, reduce-only
   enforcement, external stop reconciliation, protection read/repair).
2. Account and margin contract confirmation (one-way/isolated, risk tier, fees,
   quantity/tick, maintenance margin/liquidation accounting).
3. Unknown-submit and crash-recovery P0 fixtures with no duplicated exposure,
   premature reservation release or false `CLOSED`.
4. Demonstrated single-writer fencing when replacing a failed writer.
5. User-approved live `RiskPolicy`; engineering defaults do not authorize
   capital.
6. Protection and emergency path qualification (client-death native stop,
   two-second unconfirmed-protection behavior, direct venue plus local
   emergency control).

No profitability claim is made. Economic validation remains UNVERIFIED. No
live/test order was submitted. All six Bybit capabilities remain UNVERIFIED.
`assisted_enabled=false`. Actual assisted execution remains blocked by the
frozen §8.2 qualification gates and a user-approved live RiskPolicy.

---

# Final Phase-6 completion patch

## Identity

- STARTING SHA: `a7935d944c242642d9c189cf47e7b048267d2db9`
- IMPLEMENTATION SHA: `62f03676ecfdf4b5e02348e6756cf185e29d2378`
  (`fix: finalize phase 6 control invariants`)
- FINAL BRANCH SHA: the documentation commit immediately following the
  implementation SHA; reported in the session final report.

## Repaired control invariants

1. **Durable, positively verified protection repair.** `protect` now persists an
   exact `REPAIR_STOP` command before any side effect, durably marks dispatch
   start, calls `ensure_full_stop()`, immediately reads protection back, and
   uses the existing `verify_protection()` for epoch, quantity, approved stop,
   MarkPrice, full-position market semantics, closing-only semantics,
   freshness and evidence-reference checks. Only positive verified read-back
   reconciles `REPAIR_STOP` and returns `PROTECTED`. Timeouts, missing/stale
   read-back and semantic/quantity/stop mismatches remain
   `PROTECTION_UNCONFIRMED` with the command unresolved/`UNKNOWN`.
2. **Risk-reduction gates separated from new-risk enablement.** Close/flatten
   now require only owned/reconciled exposure, explicit bounded quantity,
   no-reversal and a supported reduce-only capability. Protect requires only
   the narrow protection repair/read capability plus positive verification.
   `assisted_enabled=false` still blocks all opening entry dispatch.
3. **Client order ID bound into the durable opening payload.** The existing
   entry wire contract now carries the persisted `Intent.client_order_id` as
   Bybit `orderLinkId`; the command payload hash, intent and transport observe
   the same 32-lowercase-hex identity. UNKNOWN recovery cannot rebuild the
   opening payload with another ID.
4. **Shell identity and fresh account evidence bound.** Revalidation evidence
   must match the shell runtime instance, writer ID and writer epoch before
   approval consumption, and now carries `account_at_ns`; future or stale
   account snapshots block while the account snapshot hash remains bound to
   the typed `AccountState`.
5. **Monotonic durable position epoch.** New Phase-6 openings allocate
   `SQLiteJournal.next_position_epoch()`. The atomic approval+intent+
   reservation transaction validates that the proposed epoch is still the next
   durable epoch, so one concurrent claim may win and the other rolls back.
   Closed epochs are never reused, and protection verification remains bound
   to the intent's epoch.

## Tests and validation

Focused Phase-6/wire tests passed, including durable repair ordering, positive
and negative protection read-back, timeout uncertainty, risk-reduction gate
separation, client-order-ID payload binding, shell identity/account freshness,
monotonic epoch advancement and atomic competing epoch claims.

One clean tracked-only worktree at implementation SHA
`62f03676ecfdf4b5e02348e6756cf185e29d2378`, one fresh Python 3.12.13 venv, one
hashed lock install:

```text
PYTHONPATH=src:. python -m pytest -ra     408 passed, 1 skipped
PYTHONPATH=src:. python -m ruff check     PASS
PYTHONPATH=src:. python -m mypy src tests PASS
PYTHONPATH=src:. python -m compileall     PASS
git diff --check                           clean
```

Protection, close and flatten positive behavior was exercised only through
explicit offline fake ports and test-only typed capability contracts. No
authenticated Bybit protection, close, flatten or order action was performed.
No live or testnet order was submitted.

No profitability claim is made. Economic validation remains UNVERIFIED. All six
Bybit capabilities remain UNVERIFIED. `assisted_enabled=false`. Actual assisted
execution remains blocked by the frozen §8.2 qualification gates and a
user-approved live RiskPolicy.
