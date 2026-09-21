# ATLAS — V1 Foundation (Session 001)

ATLAS is a continuously operating research, scanning, alerting and optionally
trading platform (separate crypto/FX capital and strategy logic). This repo
currently implements the **frozen V1 foundation only**: typed execution/
protection contracts and a SQLite durable journal. It is a safety/evidence
foundation, not a trading system.

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

## Current implementation status (Session 001)

Implemented + tested (Phase 0 + safe Phase 1 subset, no exchange connectivity):

- `src/atlas/domain/`: enums, `Decimal`/UTC-ns primitives, `CapabilityContract`,
  `InformationContract`, `TradePlan` + `validate_plan_for_execution`,
  `RiskPolicy` + frozen drawdown scaling, execution records
  (`Approval`/`Intent`/`Command`/`Reservation`/`Observation`/
  `ProtectionObservation`/`EconomicEvent`) + 32-char client-order-ID helper.
- `src/atlas/persistence/`: SQLite WAL journal (`synchronous=FULL`,
  `foreign_keys=ON`), single-writer abstraction, deterministic
  migration/bootstrap, atomic intent+reservation, persist-command-before-dispatch,
  single-use approval consumption, unresolved-intent loading, reservation totals,
  append-only evidence tables.
- `src/atlas/runtime/`: typed identity, local single-writer/file lock skeleton
  (explicitly NOT cross-host fencing), health states + deterministic
  `new_risk_allowed(...)` gate.
- `src/atlas/config/`: typed settings (env names only, no secrets).
- `docs/capability/bybit-v1.yaml`: Bybit manifest, all capabilities
  `UNVERIFIED`, Nautilus `2.0.0rc5` pin identity only.

No live orders, no strategies, no scanner, no models, no Telegram, no CCXT,
no custom OMS, no Docker/K8s. Assisted execution is impossible by construction
while capabilities remain `UNVERIFIED`.

## Safety / evidence philosophy

- Unknown never implies supported. `UNVERIFIED` blocks assisted execution.
- `UNKNOWN` submit outcomes are preserved, never auto-resolved to success.
- Persistence failure raises `PersistenceError` and is never treated as success.
- Local file lock does NOT fence another host; cross-host replacement requires
  credential revocation plus venue reconciliation.
- Every decision artifact is versioned, hashed (SHA256 canonical JSON), and
  carries provenance/availability metadata. No profitability claims exist.

## Local setup

Requires Python >= 3.10.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Copy env names (never commit values):

```bash
cp .env.example .env
```

## Test command

```bash
PYTHONPATH=src pytest
PYTHONPATH=src pytest tests/unit tests/contract tests/persistence -q
python3 -m ruff check src tests
python3 -m mypy src tests
```

Expected (Session 001): **71 passed**.

## No real-money capability yet

- No exchange credentials in repo; `.env` is gitignored.
- No order submission code paths exist.
- Nautilus `2.0.0rc5` could not be installed in this environment
  (`BLOCKED_BY_ENVIRONMENT`); no substitute installed.
- Do not fund, connect, or trade any account from this branch.

## Branch / review workflow

- Work only on `impl/session-001-foundation`. Do not modify `main`.
- Logical commits; push branch for review; DO NOT merge without review.
- Handoff: `docs/handoffs/session-001.md`.
