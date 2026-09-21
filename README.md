# ATLAS — V1 Foundation (Session 001 repairs + Session 002 safe runtime)

ATLAS is a continuously operating research, scanning, alerting and optionally
trading platform (separate crypto/FX capital and strategy logic). This repo
currently implements the **frozen V1 foundation only**: typed execution/
protection contracts, a SQLite durable journal, and a safe Phase 1 runtime
skeleton that cannot submit orders. It is a safety/evidence foundation, not a
trading system.

## V1 scope (freeze)

- First crypto implementation: one Bybit account/subaccount, one-way linear
  `BTCUSDT` / `ETHUSDT`, isolated margin, one writer, no pyramiding.
- IOC limit entry carrying a full-position native MarkPrice market stop;
  explicit-quantity reduce-only exits; narrow protection port (future).
- `CRYPTO_TREND_24H_V1` strategy, scenario engine, scanner, foundation models,
  Telegram, live orders and Phase 2 protection behavior are **not** in this
  session.

Architecture freeze (authoritative):
`ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`.
If this README conflicts with the freeze, the freeze wins.

## Current implementation status

Session 001 repairs (on `impl/session-001-foundation`): dispatch marker
atomically yields UNKNOWN; monotonic intent `state_version` (schema v2) with
stale-version rejection; frozen §1.5 lifecycle validator; command-outcome
transition safety; atomic approval+intent+reservation; atomic
command+reservation preparation before send; capability placeholder/SHA256
gating for assisted; strict manifest booleans; Decimal normalization;
economic-event composite identity; fail-closed pragma verification; bool-safe
health snapshot.

Phase 1 safe runtime (on `impl/session-002-safe-runtime`, no orders):
deterministic `atlas-crypto-live` coordinator (BOOT → writer lock → journal →
RECOVERING → restore/UNKNOWN-classify → prerequisites → READY only on positive
evidence), typed recovery certificate (never claims venue success without
venue observations), Nautilus boundary (import/version/config only, mocked in
tests), testnet identity validator, public/private connectivity health
abstractions (offline by default), atomic `atlas-ops` status snapshots
(sanitized), safe entrypoint (`python -m atlas.runtime`) that refuses new risk.

- `src/atlas/domain/`: enums, `Decimal`/UTC-ns primitives, `CapabilityContract`,
  `InformationContract`, `TradePlan` + `validate_plan_for_execution`,
  `RiskPolicy` + frozen drawdown scaling, execution records
  (`Approval`/`Intent`/`Command`/`Reservation`/`Observation`/
  `ProtectionObservation`/`EconomicEvent`) + 32-char client-order-ID helper,
  frozen `transitions` validators.
- `src/atlas/persistence/`: SQLite WAL journal (`synchronous=FULL`,
  `foreign_keys=ON`, fail-closed verified), single-writer abstraction,
  deterministic v1→v2 migration, atomic intent+reservation and
  approval+intent+reservation, persist-command-before-dispatch with version
  binding, UNKNOWN-on-dispatch, single-use approval consumption,
  unresolved-intent/command loading, reservation totals, append-only evidence.
- `src/atlas/runtime/`: typed identity, local single-writer/file lock skeleton
  (explicitly NOT cross-host fencing), health states + deterministic
  `new_risk_allowed(...)` gate, coordinator/recovery/status/prerequisites,
  Nautilus boundary, connectivity models, ops status, safe entrypoint.
- `src/atlas/config/`: typed settings (env names only, no secrets).
- `docs/capability/bybit-v1.yaml`: Bybit manifest, all capabilities
  `UNVERIFIED`, Nautilus `2.0.0rc5` pin identity only.

No live orders, no strategies, no scanner, no models, no Telegram, no CCXT,
no custom OMS, no Docker/K8s. Assisted execution is impossible by construction
while capabilities remain `UNVERIFIED` and placeholders persist.

## Safety / evidence philosophy

- Unknown never implies supported. `UNVERIFIED` blocks assisted execution.
- `UNKNOWN` submit outcomes are preserved, never auto-resolved to success.
- Persistence failure raises `PersistenceError` and is never treated as success.
- Local file lock does NOT fence another host; cross-host replacement requires
  credential revocation plus venue reconciliation.
- Every decision artifact is versioned, hashed (SHA256 canonical JSON), and
  carries provenance/availability metadata. No profitability claims exist.

## Local setup

Requires Python >= 3.12 (frozen Nautilus `2.0.0rc5` provides cp312 wheels;
Python 3.10 cannot install the pin).

```bash
python3.12 -m pip install --break-system-packages --user pytest hypothesis pyyaml
```

For Nautilus boundary import checks only (optional, no qualification):

```bash
python3.12 -m pip download --pre --no-deps --dest /tmp/nautilus312 "nautilus_trader==2.0.0rc5"
python3.12 -m pip install --pre --target /tmp/nt312 "nautilus_trader==2.0.0rc5"
```

Copy env names (never commit values):

```bash
cp .env.example .env
```

## Test command

```bash
PYTHONPATH=src pytest
PYTHONPATH=src python3.12 -m pytest
python3 -m ruff check src tests
python3 -m mypy src tests
```

Expected: **94+ passed** (Session 001 repairs), growing with Phase 1 runtime tests.

## No real-money capability yet

- No exchange credentials in repo; `.env` is gitignored.
- No order submission code paths exist.
- Nautilus `2.0.0rc5` wheel verified importable under Python 3.12
  (see `docs/capability/nautilus-pin-status.json`); installation alone qualifies
  nothing — all six Bybit behaviors remain `UNVERIFIED`, assisted stays false.
- Do not fund, connect, or trade any account from these branches.

## Branch / review workflow

- Session 001 repairs: `impl/session-001-foundation`. Session 002 runtime:
  `impl/session-002-safe-runtime`. Do not modify `main`.
- Logical commits; push branches for review; DO NOT merge without review.
- Handoffs: `docs/handoffs/session-001.md`, `docs/handoffs/session-002.md`.
