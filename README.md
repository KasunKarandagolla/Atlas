# ATLAS — Session 005 direct implementation

This package is a direct implementation workspace based on GitHub branch
`impl/session-004-phase2-completion` at verified tip
`3ca43f7ff270d617cb1a4dd98863e0a6e7cd3a57`.

The authoritative contract is `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`.

## Implemented in this package

Phase-2 repair work:
- schema v5 recovery/reconciliation evidence membership;
- complete risk-vector reservation release with immutable release audit;
- reconciliation query scope (`account` vs `instrument`) and one-run membership;
- full recovery query-set gating rather than single-endpoint READY;
- artifact-backed flat/protection recovery authority;
- typed durable protection evidence;
- monotonic `RECOVERY_REQUIRED` after the two-second protection deadline;
- separate runtime-instance, reconciliation-run and recovery-certificate identities;
- fail-closed future SQLite schema handling;
- stricter capability evidence gates;
- no ordinary exchange transport or second OMS.

Phase-3 causal-data foundation:
- immutable causal market-record contract for BTCUSDT/ETHUSDT;
- actual receipt/ingestion vs reconstructed replay availability separation;
- `ACTUAL_SYSTEM` and `RECONSTRUCTED_MARKET` replay views;
- versioned/hashable replay-availability rules;
- historical import that refuses backdated `received_at`;
- deterministic duplicate/clock/dependency/revision quarantine validation;
- generic prefix-invariance and future-tail-independence framework;
- append-only Parquet/Arrow archive boundary;
- DuckDB research-only query boundary;
- credential-free public Bybit event ingestion boundary.

## Safety status

All six Bybit behavioral capabilities remain `UNVERIFIED`.
`assisted_enabled` remains `false`.
No live or test order was sent. No authenticated mutation was performed.

## Validation in this execution environment

This qualification run uses Python 3.12.13 with the resolved hashed lock.

Executed here:

```text
PYTHONPATH=src:. python3.12 -m pytest -ra
full preserved suite passes; one credential-required public test is skipped

PYTHONPATH=src:. python3.12 -m compileall -q src tests
passed
```

The Python 3.12 lock includes hashed `pyarrow==25.0.1` and `duckdb==1.5.5`.
Parquet and DuckDB runtime tests exercise real archive writes and research-only
queries. The six Bybit behavior items remain future TEST GATE work.
