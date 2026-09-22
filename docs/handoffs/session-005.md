# Session 005 Repair Handoff

## Provenance

- Repository: `KasunKarandagolla/Atlas`
- Branch: `impl/session-005-phase3-causal-data-foundation`
- Authoritative base: `3ca43f7ff270d617cb1a4dd98863e0a6e7cd3a57`
- Supplied package SHA256: `def76651bedf21aee1a040f011ea10499ec58f27c3bdae9b6e4fbe4476e9f4b4`
- Reviewed checkpoint repaired: `93c1fd092c1f822c604abeeb978f367221b10220`

The reviewed checkpoint failed reproducibility review because `.gitignore` contained
`data/`, which ignored the nested Python packages `src/atlas/data/` and
`tests/data/`.  The original Session-005 ZIP contained those files, but commit
`93c1fd092c1f822c604abeeb978f367221b10220` did not.  That failed-checkpoint
history is retained; this handoff records the repair rather than rewriting it.

The archive manifest passed with `sha256sum -c MANIFEST.sha256`.  The ZIP was
used as an overlay source only; the Git checkout, older tests, documents and
Git metadata were preserved.

## IMPLEMENTED — Phase-2 repair

- Real transactional v4-to-v5 migration for `recovery_certificates`, including
  historical v4 field preservation in `compatibility_json`.
- Historical v4 runtime identity is explicitly `legacy-v4-runtime-unavailable`
  and is not treated as current runtime evidence.
- Migration failures roll back table changes and leave `schema_metadata` at v4.
- Fresh v5 foreign-key relationships from the v4 journal are retained.
- Recovery runs are created OPEN with no completion timestamp.  The mandatory
  V1 execution-risk query set cannot be reduced, and completion verifies every
  required query is bound to that exact run and complete.
- Reconciliation evidence hashes are recomputed from the complete immutable
  payload before persistence.  Legacy migrated hashes are marked unverified and
  fail closed for new recovery authority.
- Release-authorizing flat certificates must be journal-derived and independently
  revalidated against the current run, account, instrument, position epoch,
  writer, query set, terminal opening commands, zero position and residual-risk
  evidence.  The complete reservation vector and immutable release audit remain.
- Durable protection evidence now persists explicit full-position market-stop
  semantics and a canonical evidence hash.  Restart recovery validates the
  persisted artifact against current run-bound position, conditional-order and
  native-protection evidence; `status=CONFIRMED` alone is insufficient.
- Positive residual opening/conditional risk prevents recovery `READY`.
- Deadline monotonicity, late contradiction incidents, runtime/recovery identity
  separation and future-schema fail-closed behavior remain covered.

## IMPLEMENTED — Phase-3 causal data foundation

- The eight `src/atlas/data/*.py` implementation files and all Session-005 data
  tests are now unignored and committed candidates; the archive and public
  adapter retain the prior runtime-test improvements.
- Processing cannot precede receipt.  Historical imports retain actual local
  receipt/ingestion time separately from reconstructed replay availability.
- ACTUAL_SYSTEM and RECONSTRUCTED_MARKET views, dependency ordering, revision
  lineage, source-clock conflict quarantine and prefix/future-tail invariance
  are tested.
- Parquet/Arrow archive writes are deterministic and append-only.  Exact writes
  are idempotent; conflicting logical `record_id` content is rejected across
  partitions before commitment.  Payload and dependency JSON round-trip through
  real Parquet files.
- DuckDB executes real archive-only research queries.  It does not receive a
  writable authoritative SQLite connection or become live state.
- Public Bybit/Nautilus translation is credential-free, read-only and limited to
  BTCUSDT/ETHUSDT linear public data shapes.  No private stream, order transport,
  account mutation, CCXT or second OMS was added.

## TESTED

- Python: `3.12.13`
- Requirements lock SHA256: `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`
- PyArrow: `25.0.1`
- DuckDB: `1.5.5`
- Nautilus: `2.0.0rc5`
- Nautilus source commit: `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`
- Nautilus wheel SHA256: `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`
- Full working-tree pytest: **178 passed, 1 skipped**
- Ruff: **passed** under the previous policy; E701/E702/E703 are not suppressed
- Mypy: **passed**, `Success: no issues found in 87 source files`
- Compileall: **passed**
- `git diff --check`: **passed**
- Fresh Python 3.12 hashed-lock install: **passed**
- Fresh-install full pytest: **178 passed, 1 skipped**
- Real Parquet/Arrow round-trip, deterministic partitioning, idempotency,
  conflict rejection and append-only checks: **TESTED**
- Real DuckDB BTCUSDT/ETHUSDT archive filtering and research-only boundary:
  **TESTED**
- Bounded credential-free public Bybit mainnet BTCUSDT 1-minute kline read and
  translation with genuine local receipt time: **TESTED**

The one skip is `tests/runtime/test_safe_runtime.py:398`, an explicit opt-in
public testnet check requiring credentials.  It did not hide any Arrow, DuckDB,
Nautilus or causal test.

## TRACKED-ONLY REPRODUCIBILITY

- Repair commit: to be filled with the final repair commit SHA after staging.
- A clone containing only committed Git files must include:
  `src/atlas/data/*.py`, `tests/data/*.py`, the migration integration test and
  the authority adversarial tests.
- The tracked-only clone validation result will be recorded here before push.

## TEST GATE / UNVERIFIED

All six frozen Bybit capabilities remain **UNVERIFIED**:

1. `entry_ioc_with_attached_full_mark_market_stop`
2. `native_stop_visible_and_resizes_on_partial_fill`
3. `reduce_only_wire_and_matching_enforcement`
4. `ambiguous_submit_not_treated_as_definite_rejection`
5. `external_native_stop_fill_reconciliation`
6. `native_position_stop_read_and_repair_port`

`assisted_enabled=false` remains in force.  No authenticated test order, live
order, credential mutation or exchange mutation occurred.  Phase 4 and Session
006 were not started.

Remaining work is the explicitly authorized TEST GATE qualification matrix for
private Bybit behavior and live fencing, plus any independent review of this
repair commit.  No `BLOCKED BY ENVIRONMENT` remains for the local Python 3.12,
hashed installation, quality, Parquet, DuckDB or bounded public-read gates.
