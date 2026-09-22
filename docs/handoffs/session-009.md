# Session 009 — Final V1 Phase 0–6 stabilization and acceptance

## Identity

- STARTING SHA: `9e36f9870bebe82f4f59e5c16efac5f327f03602`
- BRANCH: `impl/session-009-v1-phase0-6-stabilization`
- IMPLEMENTATION SHA: `a0a2ea38e467a9d474b48b534c62e1bf1e14bf8f`
  (`fix: stabilize atlas v1 phase 0-6 integration`)
- FINAL SHA: the documentation commit immediately following the implementation
  SHA; reported in the session final report.

Phase 5 remains frozen at
`248879b60d81877aa5dba295e1d8243637609a7c`.

## Concrete defect repaired

**Durable protection state.** The existing Phase-6 protection path now updates
the persisted `Intent` after positive or failed read-back proof:

- positive verified evidence transitions to `OPEN_PROTECTED` +
  `ProtectionStatus.CONFIRMED` using only legal frozen lifecycle transitions,
  then reconciles `REPAIR_STOP`;
- absent/conflicting/stale proof records `ProtectionStatus.UNCONFIRMED` and
  transitions to `OPEN_UNPROTECTED` where legal, otherwise
  `RECOVERY_REQUIRED`;
- the command stays `UNKNOWN` until positive proof reconciles it;
- command payload, persisted protection evidence and intent all bind the same
  `position_epoch`.

No new protection subsystem or state model was added.

## V1 cross-phase acceptance

Added one small integration module:
`tests/integration/test_v1_phase0_6_acceptance.py`.

It proves:

- Phase 4 → Phase 5: the scanner calls the existing `evaluate_phase4()`
  pipeline, preserves the immutable TradePlan hash and Phase-4 evidence
  reference, keeps `NO_SIGNAL` outcomes valid, writes complete scanner rows,
  emits alert-only projections, and creates no approval/intent/reservation/
  command;
- TradePlan → Phase 6: the real Phase-4 TradePlan persists, binds a one-use
  approval, revalidates against genuine persisted READY recovery evidence,
  atomically creates intent + reservation, allocates a monotonic epoch, binds
  the opening payload to the persisted `client_order_id`, and stops at
  `DISPATCH_BLOCKED` with production capabilities unverified;
- failure/recovery handoff: UNKNOWN keeps the same intent, client order ID and
  epoch, retains the reservation, blocks a second opening, and refuses
  reservation release without a certified-flat artifact.

Existing Phase-2/recovery tests were reused for recovery-certificate binding,
late contradictory evidence reopening recovery, negative-lookup semantics,
execution deduplication and flat-certificate release authority.

## Validation

Focused protection/Phase-6 tests and the V1 integration module passed during
development. Affected unit/contract/data/scanner/runtime/persistence groups also
passed.

One clean tracked-only worktree at implementation SHA
`a0a2ea38e467a9d474b48b534c62e1bf1e14bf8f`, one fresh Python 3.12.13 venv, one
hashed lock install:

```text
PYTHONPATH=src:. python -m pytest -ra     413 passed, 1 skipped
PYTHONPATH=src:. python -m ruff check     PASS
PYTHONPATH=src:. python -m mypy src tests PASS
PYTHONPATH=src:. python -m compileall     PASS
git diff --check                           clean
```

The skipped test is the existing opt-in public testnet check; no credentials or
network were used.

## Status ledger

- Phase 0–6 V1 implementation: **IMPLEMENTED / TESTED_OFFLINE**
- Phase-4 baseline science and immutable TradePlan: **IMPLEMENTED / TESTED_OFFLINE**
- Phase-5 scanner, calendar and alert-only output: **IMPLEMENTED / TESTED_OFFLINE / FROZEN**
- Phase-6 assisted control contracts: **IMPLEMENTED / TESTED_OFFLINE**
- Durable protection-state agreement: **IMPLEMENTED / TESTED_OFFLINE**
- Actual unattended public Bybit collection: **BLOCKED BY ENVIRONMENT / UNVERIFIED**
- Authenticated execution, close/flatten and protection: **TEST GATE / UNVERIFIED**
- All six Bybit capability states: **UNVERIFIED**
- Economic/profitability validation: **UNVERIFIED**
- `assisted_enabled=false`: **IMPLEMENTED**

Authority boundaries remain: Bybit = external reality; Nautilus = normal
order/fill/position projection; ATLAS SQLite = durable intent/approval/
reservation/command/recovery audit; Parquet = immutable research archive;
DuckDB = research analytics only. No second writer or OMS exists.

## Required external/live gates (do not block Phase 0–6 implementation acceptance)

All §8.2 empirical gates remain unpassed: pinned Nautilus/Bybit wire behavior,
account/margin contract, unknown-submit crash recovery, single-writer fencing,
user-approved live RiskPolicy, and protection/emergency path qualification.

No profitability claim is made. Economic validation remains UNVERIFIED. No live
or test order was submitted. All six Bybit capabilities remain UNVERIFIED.
`assisted_enabled=false`. Actual assisted execution remains blocked by the
frozen §8.2 qualification gates and a user-approved live RiskPolicy.
