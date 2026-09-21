# Session 003 Handoff — Review Repair and Phase 2 Offline Qualification

## Branch and inherited commits

- Branch: `impl/session-003-phase2-offline-qualification`
- Inherited reviewed Session 003 base: `adaa3036f80a3de3d954495b4e4fdc339f6292d1`
- Inherited corrected Session 002: `64ff179`
- Session 003 repair commit: `55a4173b0d4bd9f5630c6c5aaad09f9e728d0333`

The repair commit corrects the prior handoff's unsupported `TESTED_OFFLINE` claims. No live orders, test orders, authenticated mutations, or Bybit capability promotions were performed.

## Review corrections implemented

- `SafeRuntime` now acquires one `WriterLock` and opens one `SQLiteJournal` for its full process lifetime. Ticks reuse both; shutdown closes the journal before releasing the writer; partial startup failures clean up.
- Integrated new-risk eligibility now consumes the supplied typed `CapabilityContract`, verifies its exact hash and frozen Nautilus/profile/artifact/lock/account prerequisites, and includes an injected fail-closed `RiskPolicyDecision`.
- Placeholder identity is constructible for safe startup but is rejected by readiness checks. Production entrypoint configuration remains explicitly placeholder/unverified.
- Recovery READY requires current reconciliation, positive venue evidence, no unresolved intents/commands/UNKNOWN state, all prerequisites, and typed certified-flat or current-protection evidence.
- Protection evidence preserves account/instrument identity and validates current exact signed quantity coverage, stop semantics, MarkPrice, full-position behavior, closing-only behavior, evidence references, and clock freshness.
- Reconciliation merge severity, interval coverage, retention coverage, and absence certification are fail-closed.
- Flat certificates model current flatness and terminal order certainty; historical fills may be nonzero. Late contradictory evidence reopens recovery, and delayed funding is not made an execution-risk prerequisite.
- Entry/exit wire builders implement the frozen logical contract. Emergency MARKET exits omit price rather than emitting a fake zero price.
- The two-second protection deadline schedules durable cancel/repair/explicit-quantity flatten actions and projects `RECOVERY_REQUIRED`; it never claims flatten execution or removes native protection.
- Execution evidence and order-status observations are durable and append-only. Duplicate execution IDs are deduplicated; conflicting payloads quarantine/fail; statuses do not create fills.
- Capability promotion requires a prior gate state, exact capability, non-placeholder target profile hash, matching test-run identity, immutable references, explicit qualification, and exact environment/profile binding. No capability was promoted.
- Late fills remain in the prior unresolved epoch until a valid CLOSED certificate exists.
- The fault harness no longer contains hard-coded success assertions; collected tests drive real journal, transition, reservation, fill, reconciliation, protection, certificate, recovery, and lifecycle mechanics.

## Schema and dependency artifacts

- SQLite schema migration: schema v3, including durable execution evidence, order-status observations, reconciliation query evidence, recovery incidents, and capability evidence/qualification records.
- Full Python 3.12 lock SHA256: `f96c2a44760f1dd95f05b93857afab4af9a8cb5554a69f56b82e8e7ac09f0a54`
- Nautilus artifact: `nautilus_trader-2.0.0rc5-cp312-cp312-manylinux_2_34_x86_64.whl`
- Nautilus wheel SHA256: `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`
- Nautilus source commit: `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`
- Python/platform ABI: `cpython-312-x86_64-linux-gnu`
- The lock was installed into a clean Python 3.12 virtual environment and the exact rc5 offline Bybit config construction/import test passed. The base environment's rc5 test is `BLOCKED BY ENVIRONMENT` because its externally managed interpreter does not have the wheel installed; this does not change the clean-lock result.

## Freeze §1.8 qualification matrix

Only the labels below are used. `TESTED_OFFLINE` means a collected test exercised deterministic local mechanics; it does not qualify Bybit behavior.

| # | Scenario | Status |
|---:|---|---|
| 1 | Identity and mode | IMPLEMENTED |
| 2 | Wire contract | TESTED |
| 3 | Crash after journal commit before transport | TESTED_OFFLINE |
| 4 | Lost response after simulated acceptance | TESTED_OFFLINE |
| 5 | Definite reject | TESTED_OFFLINE |
| 6 | Partial fills | TESTED_OFFLINE |
| 7 | Kill after full fill | TESTED_OFFLINE |
| 8 | Duplicate/reordered execution and statuses | TESTED_OFFLINE |
| 9 | Native-stop repair | TEST GATE |
| 10 | Stop/close race | TEST GATE |
| 11 | Late-entry race | TESTED_OFFLINE |
| 12 | Private disconnect then reconciliation | TESTED_OFFLINE |
| 13 | Incomplete/paginated REST | TESTED_OFFLINE |
| 14 | Residual opening order with zero position | TESTED_OFFLINE |
| 15 | Amendment/cancel/fill race | TESTED_OFFLINE |
| 16 | Exit remains unresolved | TESTED_OFFLINE |
| 17 | Storage/clock fault response | TESTED_OFFLINE |
| 18 | Funding/fee/external-event dedup | TESTED_OFFLINE |
| 19 | Approval replay | TESTED_OFFLINE |
| 20 | Restore with unresolved command/fencing | TEST GATE |

The collected fault-injection module exercised all 17 deterministic offline scenarios requested by the freeze. Actual Bybit matching, authenticated reads, stop visibility/resizing, and fencing against a live venue remain TEST GATE.

## Capability status

All six capabilities remain `UNVERIFIED`:

- `entry_ioc_with_attached_full_mark_market_stop`
- `native_stop_visible_and_resizes_on_partial_fill`
- `reduce_only_wire_and_matching_enforcement`
- `ambiguous_submit_not_treated_as_definite_rejection`
- `external_native_stop_fill_reconciliation`
- `native_position_stop_read_and_repair_port`

`assisted_enabled`: `false`.

## Verification results

Collected verification on the repaired branch:

```text
PYTHONPATH=src:. python3.12 -m pytest -ra
142 passed, 2 skipped

PYTHONPATH=src:. python3.12 -m ruff check src tests
All checks passed!

PYTHONPATH=src:. python3.12 -m mypy src tests
Success: no issues found in 64 source files
```

The two skips are the base-environment Nautilus import test described above and the opt-in public testnet test, which has no credentials and was not enabled. The clean-lock Python 3.12 environment passed the exact rc5 offline configuration test.

The bounded SafeRuntime test proves that the journal, writer, and runtime instance ID remain stable across multiple ticks; status timestamps advance; new risk remains false; and shutdown releases both resources. Secret scanning found no `.env`, API key, token, secret, account identifier, signed payload, CCXT dependency, mainnet setting, or order transport.

## Remaining Phase 2 work

- TEST GATE: authenticated Bybit reads for private order/position/wallet streams, native protection observations, transaction-log pagination, and external stop reconciliation.
- TEST GATE: actual Bybit stop visibility/resizing, stop/close races, reduce-only matching, ambiguous-submit behavior, and live fencing.
- Finish any further restart/reconciliation integration only with persisted venue observations; no transport or qualification promotion is authorized.
- Keep the target profile, account identity, writer identity, and RiskPolicy evidence fail-closed until independently supplied and verified.

Phase 3 was not started. No strategy, scanner, model, market archive, or foundation-model work was added.
