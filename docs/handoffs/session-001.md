# Session 001 Handoff — Foundation (Phase 0 + safe Phase 1)

Branch: `impl/session-001-foundation`
Date (UTC): 2026-09-21
Freeze: `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` (authoritative; wins on conflict)

## Scope completed

Phase 0 (all implementable) + safe Phase 1 subset without authenticated exchange connectivity:

1. Project structure (`src/` layout), `pyproject.toml` (pytest/ruff/mypy/Hypothesis), `.gitignore`, `.env.example`.
2. Domain enums: 13 lifecycle states, protection (NONE/UNCONFIRMED/CONFIRMED/BREACHED), reconciliation (CURRENT/STALE/CONFLICTED), command outcome (UNSENT/UNKNOWN/DEFINITE_ACCEPT/DEFINITE_REJECT/RECONCILED), availability class (ACTUAL_OBSERVED/RECONSTRUCTED_PUBLIC/UNKNOWN/REVISED_NO_VINTAGE), capability status (UNVERIFIED/SUPPORTED/UNSUPPORTED/FAILED), plus environment/health/side/command-type.
3. Money/time primitives: `Decimal` only (reject float/NaN/Infinity/negative where prohibited), canonical deterministic serialization, UTC integer nanoseconds canonical (reject naive datetimes, no silent conversion).
4. `CapabilityContract` (immutable/versioned, explicit status — never bare bool, unknown never supported, assisted_enabled blocked unless all SUPPORTED, initial fixture all UNVERIFIED).
5. `InformationContract` (immutable/versioned, §4 provenance/availability fields, causality validation: available>=received, available>=processed, published>=event, received>=published, RECONSTRUCTED_PUBLIC anti-fabrication rule, dependency-availability check, deterministic serialization + factories).
6. `TradePlan` (immutable/versioned, frozen fields, positive qty / expiry>creation / horizon>creation / side-compatible stop / non-blank hashes, `is_expired`, deterministic `validate_plan_for_execution` with structured reasons; fixtures: valid, expired, unverified-protection).
7. `RiskPolicy` (typed/versioned, fractions not percentages, default `max_simultaneous_new_risk_intents=1`, threshold-ordering + non-negative validation, frozen `drawdown_scaling` pure function, engineering defaults explicitly labeled NOT SAFE).
8. Execution records: `Approval`/`Intent` (with lifecycle/protection/health)/`Command` (payload-hash binding, UNKNOWN preserved)/`Reservation` (budget vector)/`Observation`/`ProtectionObservation`/`EconomicEvent`; 32-lowercase-hex client-order-ID helper (UUID128) with format/uniqueness property tests. No exchange submission.
9. SQLite durable journal: WAL + `synchronous=FULL` + FK ON, single-writer abstraction, transactional writes, deterministic migration/bootstrap, UNIQUE client-order-ID, atomic intent+reservation, persist-command-before-dispatch + mark-send-started + outcome update, approval single-use transaction (concurrent-safe), unresolved intents, reservation totals, append-only evidence, explicit `PersistenceError` (never success), reopen persistence.
10. Writer ownership skeleton: instance ID + monotonic epoch + file guard, duplicate local startup rejected, documented NOT cross-host fencing, new-risk-disabled when unowned. No active-active failover.
11. Runtime identity/health: typed identity (environment/venue/product/position/margin/account-hash), health states (BOOT/RECOVERING/READY/DEGRADED/ENTRY_HALTED/PROTECTION_UNCERTAIN/EMERGENCY_EXIT), deterministic `new_risk_allowed(...)` (READY + owned + account + data + CURRENT + protection-if-exposed + no-drawdown-stop). No Bybit connectivity.
12. `docs/capability/bybit-v1.yaml` (Nautilus 2.0.0rc5, commit 1b0a49d…, BTCUSDT/ETHUSDT linear one-way isolated-margin-assumption, all six capabilities UNVERIFIED, assisted false) + parser/validator test.
13. Nautilus pin attempt (no substitute, no freeze change).
14. Serious tests (71) + README + this handoff.

Explicitly NOT done (per session objective): strategies, scanner, scenario engine, foundation models, Telegram, live orders, Phase 2 protection behavior, authenticated/testnet connectivity, Redis/Kafka/Celery/Docker/K8s, ML libs, web framework, ORM.

## Files/modules created

- `pyproject.toml`, `.gitignore`, `.env.example`, `README.md`
- `src/atlas/__init__.py`
- `src/atlas/domain/__init__.py`, `enums.py`, `money.py`, `time.py`, `capability.py`, `information.py`, `trade_plan.py`, `risk.py`, `execution.py`
- `src/atlas/persistence/__init__.py`, `schema.py`, `migrations.py`, `sqlite.py`
- `src/atlas/runtime/__init__.py`, `identity.py`, `writer_lock.py`, `health.py`
- `src/atlas/config/__init__.py`, `settings.py`
- `docs/capability/bybit-v1.yaml`, `docs/capability/nautilus-pin-status.json`
- `docs/handoffs/session-001.md` (this file)
- `tests/__init__.py`
- `tests/unit/test_enums_money_time.py`, `test_capability.py`, `test_information.py`, `test_trade_plan.py`, `test_risk.py`, `test_execution.py`, `test_manifest.py`, `test_writer_health.py`
- `tests/contract/test_contracts.py`
- `tests/persistence/test_journal.py`

## Tests

Commands (from repo root):

- `PYTHONPATH=src pytest` → **71 passed** (~22s)
- `PYTHONPATH=src pytest tests/unit tests/contract tests/persistence -q` → **71 passed**
- `python3 -m ruff check src tests` → **All checks passed** (line-length 120; E501/SIM102/SIM105 ignored by config; C408/F841/UP007 fixed)
- `python3 -m mypy src tests` → **Success: no issues found in 31 source files**

Coverage areas (failure paths exercised, not only happy paths):

- Domain: capability assisted-gate, bare-bool rejection, valid/expired/unverified-protection plans, malformed IDs/hashes, stop/side incompatibility, Decimal/UTC validation, RiskPolicy ordering/negatives, drawdown scaling (frozen formula + bounds + property).
- Information causality: available<received rejected, processed>available rejected, RECONSTRUCTED receipt fabrication rejected (equal + earlier), missing method/replay rejected, ACTUAL missing receipt/replay-carry rejected, REVISED replay-carry rejected, dependency latency + missing-dep rejected, deterministic serialization + content-hash stability, Hypothesis available>=received property.
- Execution: lifecycle integrity, 32-char hex format + uniqueness (50–500 sample), UNKNOWN preserved, payload-hash tamper rejected, protection/reconciliation independence (4×3 matrix), reservation negativity rejected.
- Persistence: WAL/ FULL /FK pragmas, idempotent bootstrap, unique client-order-ID, approval once + expired + wrong-plan + 8-thread concurrent single-winner, atomic intent+reservation + mismatch rollback, command persist→send-started→UNKNOWN→ACCEPT flow, unresolved filtering, totals, evidence append, reopen retention, missing-record PersistenceError.
- Writer/runtime: duplicate denied (cross-handle + same-process), epoch monotonic, RECOVERING/STALE/CONFLICTED/UNCONFIRMED/BREACHED/unowned block new risk, READY+owned+CURRENT+CONFIRMED allows.

## Dependency status

- Nautilus `2.0.0rc5` install: **failed / not available** on this host index (`pip download --pre --no-deps nautilus_trader==2.0.0rc5` → "No matching distribution"; only `1.202.0` visible). Recorded as `BLOCKED_BY_ENVIRONMENT` in `docs/capability/nautilus-pin-status.json`.
- No substitute version installed; freeze unchanged.
- Python 3.10.12, Linux x86_64 (glibc2.35). Artifact/lock SHAs remain `REQUIRED_AT_INSTALL`.
- Dev deps used for verification only: pytest 9.1.1, hypothesis, pyyaml, ruff, mypy (not pinned as runtime deps).

## Evidence status

- Domain enums / money-time primitives: TESTED
- CapabilityContract + UNVERIFIED fixture + assisted gate: TESTED
- InformationContract causality + serialization: TESTED
- TradePlan + `validate_plan_for_execution` (valid/expired/unverified): TESTED
- RiskPolicy validation + drawdown scaling: TESTED
- Execution records + client-order-ID: TESTED
- SQLite journal + approval single-use + atomicity + reopen: TESTED
- Writer lock skeleton + identity/health gate: TESTED
- Bybit manifest mapping (UNVERIFIED preserved): TESTED
- Nautilus 2.0.0rc5 installed artifact/behavior: UNVERIFIED
- Nautilus pin install on this host: BLOCKED BY ENVIRONMENT
- Exchange wire behavior (IOC+stop, resize, reduce-only, ambiguous-submit, stop-fill recon, read/repair): UNVERIFIED
- Account/margin profile, crash/UNKNOWN recovery, fencing, live RiskPolicy, protection/emergency path (§8.2 gates): TEST GATE
- Strategies / scanner / scenario / models / Telegram / live orders / Phase 2: UNVERIFIED (not attempted)

## Known limitations

- SQLite journal is single-host, single-writer; file lock does not fence remote hosts.
- UTC-ns is canonical; datetime inputs must be tz-aware (naive raises).
- `Decimal` canonical form strips trailing zeros (value-preserving); very large Hypothesis decimals covered.
- Reservation totals are sums over open reservations; portfolio-ES/stress math lives in future phases.
- `new_risk_allowed` is a local predicate; it does not connect to Bybit or prove venue state.
- Manifest `account_identity_hash` / artifact hashes are placeholders until install + account binding.
- Ruff config ignores E501/SIM102/SIM105 for practicality; line-length 120.

## Architecture deviations

NONE. Freeze §§1, 1.4, 1.5, 1.8, 4, 7, 9.11 followed without reinterpretation. Minor packaging addition (`domain/time.py` split from `money.py`) is structural only, required by the time-primitive contract; no frozen formula changed.

## Next task

Authenticated/testnet runtime + protection/recovery qualification ONLY after review of this branch. Do not enable assisted execution until every §8.2 gate (wire behavior, account/margin contract, UNKNOWN/crash fixtures, fencing, signed live RiskPolicy, protection/emergency path) passes on the pinned artifact + target account.
