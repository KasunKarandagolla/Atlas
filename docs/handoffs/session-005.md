# Session 005 Handoff — Phase-2 Evidence Repair and Phase-3 Causal Data Foundation

## Provenance

- Repository: `KasunKarandagolla/Atlas`
- GitHub-derived base branch: `impl/session-004-phase2-completion`
- Base SHA: `3ca43f7ff270d617cb1a4dd98863e0a6e7cd3a57`
- Package: `atlas-session005.zip`
- ZIP SHA256: `def76651bedf21aee1a040f011ea10499ec58f27c3bdae9b6e4fbe4476e9f4b4` — **VERIFIED**
- Package `MANIFEST.sha256`: every listed file — **VERIFIED**
- Working branch: `impl/session-005-phase3-causal-data-foundation`

The package was overlaid onto the real checkout. Git metadata, historical
documents and all pre-import tracked tests were preserved. No delete-style
sync was used, and the transport ZIP is not a repository source artifact.

## IMPLEMENTED — Phase 2 repairs

- SQLite schema v5, durable reconciliation runs and immutable query-to-run membership.
- Explicit account versus instrument query scope.
- Complete execution-risk recovery query set; a lone positions query cannot produce READY.
- Recovery READY requires a persisted, identity/run/epoch/currentness-bound flat or protection artifact.
- Full reservation-vector release (`remaining_open_qty`, `normal_loss`, `stress_loss`, `notional`, `beta_adjusted_notional`, `margin`, `es_contribution`) with immutable release audit.
- Late contradiction incidents preserve the flat certificate and reopen the lifecycle projection.
- Durable typed protection evidence with actual receive timestamps and clock-conflict quarantine.
- Monotonic `RECOVERY_REQUIRED` after the two-second protection deadline.
- Separate stable runtime-instance and per-evaluation recovery-run identities.
- Future SQLite schema versions fail closed.
- Capability evidence gates require real references and exact profile hashes.

## TESTED — Phase 3 causal/data foundation

- Python 3.12.13 environment and full hashed lock install.
- `pyarrow==25.0.1` and `duckdb==1.5.5` runtime dependencies.
- Immutable BTCUSDT/ETHUSDT Parquet partitions with deterministic content hashes, idempotent duplicate writes, conflicting-identity rejection, PyArrow schema/JSON/timestamp round-trip and append-only checks.
- DuckDB reads and filters Parquet archive inputs only; it is not an authoritative live-state store.
- ACTUAL_SYSTEM versus RECONSTRUCTED_MARKET separation, genuine receipt timestamps, replay-rule hashing, dependency ordering, revisions, duplicate/clock quarantine, prefix invariance and future-tail independence.
- Credential-free public Bybit market-data smoke: one BTCUSDT linear 1-minute public bar was fetched and translated with a genuine local receipt timestamp.
- Public Nautilus-style event translation tests cover supported bar shapes and reject unsupported shapes.

## Dependency/runtime evidence

- Python: `3.12.13`
- `requirements-lock.txt` SHA256: `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`
- Nautilus: `nautilus-trader==2.0.0rc5`
- Nautilus source commit: `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`
- Nautilus wheel SHA256: `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`
- ABI: `cpython-312-x86_64-linux-gnu`

## Quality gates

- Pytest: **TESTED** — `172 passed, 1 skipped` in the repository environment.
- Ruff: **TESTED** — `All checks passed!`
- Mypy: **TESTED** — `Success: no issues found in 85 source files`
- Compileall: **TESTED**
- `git diff --check`: **TESTED**
- Clean lock install in `/tmp/atlas-session005-clean`: **TESTED** with `pip install --require-hashes`; full clean suite also passed `172 passed, 1 skipped`.
- Skip reason: `tests/runtime/test_safe_runtime.py:388` is an explicit opt-in public testnet check requiring credentials; no network was required for the suite.
- Public-data result: **TESTED** — credential-free Bybit mainnet public market-data smoke collected one BTCUSDT linear 1-minute bar and preserved local receipt time; no credentials or authenticated calls.

## Safety and capability status

All six frozen Bybit capabilities remain **UNVERIFIED** and are not promoted by
offline code, simulators, public market data or documentation:

1. `entry_ioc_with_attached_full_mark_market_stop`
2. `native_stop_visible_and_resizes_on_partial_fill`
3. `reduce_only_wire_and_matching_enforcement`
4. `ambiguous_submit_not_treated_as_definite_rejection`
5. `external_native_stop_fill_reconciliation`
6. `native_position_stop_read_and_repair_port`

- `assisted_enabled=false` — **TESTED**
- Authenticated account stream: not used.
- Order submission or exchange mutation: none.
- Secret scan: **TESTED**; no credentials, `.env`, database, private key or sensitive log is included in the staged package.

## TEST GATE / remaining work

- **TEST GATE:** all six private Bybit behavioral capabilities and any assisted execution remain unqualified.
- **BLOCKED BY ENVIRONMENT:** none for the requested local Python 3.12, Arrow, DuckDB, Nautilus, static-quality or public read-only smoke gates.
- Phase 4 strategy/scenario/model work remains not started.
