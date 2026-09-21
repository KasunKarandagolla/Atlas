# Session 004 Handoff — Phase 2 Persistence Integration

## Branch and inherited repair

- Branch: `impl/session-004-phase2-completion`
- Inherited Session 003 repair branch tip: `94fb52a393d908a6eede6f4da9ac0fbc428003c5`
- Inherited implementation repair: `55a4173b0d4bd9f5630c6c5aaad09f9e728d0333`
- Session 004 implementation commit: `4507505d9c3a3fa700f6ce6bc3010da15fcd4c24`

Session 004 completed the remaining offline persistence integration. It did not add a production exchange client, transport, strategy, scanner, model, or authenticated operation.

## Integration completed

- Added schema v4 migration for durable typed `recovery_certificates`.
- Added SQLite persistence and restart loading for `RecoveryCertificate`, including decision, writer epoch, reconciliation health, unresolved command/UNKNOWN lists, venue evidence references, and typed flat/current-protection evidence.
- Added `load_reconciliation_evidence_bundle()`, which reconstructs a typed bundle only from explicitly named persisted query IDs and re-derives aggregate status/completeness fail-closed.
- Added `recover_from_persisted_evidence()`, the offline recovery orchestrator: it consumes the reconstructed typed bundle, runs the existing recovery gate, persists the certificate, and appends a durable `RecoveryIncident` for every non-READY result.
- Added tests for clean restart reconstruction, certificate round-trip, incomplete query plus UNKNOWN recovery incident, and exact account/query identity binding.
- Existing integration tests continue to cover durable execution/status evidence, conservative reservation release from a valid flat certificate, durable protection-deadline commands, target-profile-bound capability evidence, deterministic writer/runtime identity, and exact Nautilus rc5 offline configuration construction.

## Schema and dependency artifacts

- SQLite schema: v4. The v3-to-v4 migration creates `recovery_certificates`; prior v1, v2, and v3 databases remain upgradeable through the existing migration chain.
- Full Python 3.12 lock SHA256: `f96c2a44760f1dd95f05b93857afab4af9a8cb5554a69f56b82e8e7ac09f0a54`
- Nautilus artifact: `nautilus_trader-2.0.0rc5-cp312-cp312-manylinux_2_34_x86_64.whl`
- Nautilus wheel SHA256: `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`
- Nautilus source commit: `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`
- Python/platform ABI: `cpython-312-x86_64-linux-gnu`
- A fresh Python 3.12 virtual environment installed `requirements-lock.txt` with `--require-hashes`; the exact rc5 offline config test passed there.

## Verification

Final commands on this branch:

```text
PYTHONPATH=src python3.12 -m pytest -ra
145 passed, 2 skipped in 56.41s

python3.12 -m ruff check src tests
All checks passed!

python3.12 -m mypy src tests
Success: no issues found in 65 source files
```

Mypy emitted only the two existing notes for untyped test-function bodies. The two pytest skips are the base interpreter's unavailable rc5 installation and the opt-in public testnet check with no credentials. The clean locked environment passed the rc5 test, so the base-interpreter skip is `BLOCKED BY ENVIRONMENT`, not an assertion that rc5 is unavailable.

`git diff --check` passed. Secret scanning found no credentials, API keys, tokens, account identifiers, signed payloads, `.env`, CCXT, mainnet, or order transport. `.env.example` is the only environment-named file.

## Offline scenarios actually exercised

The collected deterministic harness executes all 17 offline §1.8 scenarios:

1. crash after journal commit before transport;
2. lost response after simulated acceptance;
3. definite reject;
4. partial fills;
5. kill after first fill;
6. kill after full fill;
7. duplicate/reordered execution and statuses;
8. private disconnect then reconciliation;
9. incomplete/paginated REST;
10. residual opening order with zero position;
11. late-entry fill during cancel/flatten;
12. amendment/cancel/fill race;
13. unresolved exit;
14. storage/clock fault response;
15. funding/fee/external-event dedup;
16. approval replay;
17. restore with unresolved command.

These are local mechanics tests only. No simulator result promotes a live capability.

## Qualification and capability status

The truthful Session 003 §1.8 matrix is retained in `docs/handoffs/session-003.md`:

- offline-testable local mechanics are `TESTED_OFFLINE` only;
- native-stop repair, stop/close race, and live restore/fencing remain `TEST GATE`;
- all six Bybit capabilities remain `UNVERIFIED`;
- `assisted_enabled` remains `false`.

No testnet mutation, authenticated read, live matching, or capability qualification was performed.

## Remaining Phase 2 work

- TEST GATE: authenticated Bybit private order/position/wallet and transaction-log reads.
- TEST GATE: native protection visibility/resizing, stop/close races, reduce-only matching, ambiguous-submit behavior, external stop-fill reconciliation, and live fencing.
- Keep RiskPolicy, account identity, target profile, writer identity, and venue evidence fail-closed until independently verified.

Phase 3 was not started. The causal market-record schema, information-time fields, `ACTUAL_SYSTEM`, `RECONSTRUCTED_MARKET`, and prefix-invariance framework were intentionally left untouched.
