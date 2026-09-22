# Session 005 Repair Handoff

## Provenance

- Repository: `KasunKarandagolla/Atlas`
- Branch: `impl/session-005-phase3-causal-data-foundation`
- Authoritative base: `3ca43f7ff270d617cb1a4dd98863e0a6e7cd3a57`
- Supplied package SHA256: `def76651bedf21aee1a040f011ea10499ec58f27c3bdae9b6e4fbe4476e9f4b4`
- Previously reviewed checkpoints: `93c1fd092c1f822c604abeeb978f367221b10220`
  and `b0a1ec1f2ed9e2db40d84b05573340049a8b58d1`.  The current repair
  implementation commit is `4f5d136`.

Checkpoint `93c1fd092c1f822c604abeeb978f367221b10220` failed independent
review because `.gitignore` contained `data/`, which ignored the nested Python
packages `src/atlas/data/` and `tests/data/`.  The original Session-005 ZIP
contained those files, but that commit did not; it also retained Phase-2
evidence weaknesses.  During the subsequent repair/tracked-only validation,
an intermittent SQLite shared-connection concurrency issue was discovered and
fixed in `99d5156201a58fa5c19774e619b31207584636ec`.

Checkpoint `b0a1ec1f2ed9e2db40d84b05573340049a8b58d1` was independently
rejected for caller-controlled temporal recovery binding, insufficient
positive native-protection proof, insufficient history-retention enforcement,
and causal record identity using content hash rather than a full immutable
causal fingerprint.  Checkpoint
`1bd3e5bc180972a0f3827801cc55f1b94ed657c5` repaired those issues except for
durable one-use reconciliation-run/recovery-cycle authority.  Both failed
checkpoint histories are retained; this handoff records the repairs rather
than rewriting them.

Checkpoint `d9004283bc354bbffc701c70b5b29d52c9ae6749` was independently
rejected only because its new durable binding table was not a real schema
migration: pre-binding schema-5 journals could open with an empty table and
reuse a historically consumed reconciliation run.  Its new-journal binding,
temporal checks and idempotent retry behavior remain preserved here.

The archive manifest passed with `sha256sum -c MANIFEST.sha256`.  The ZIP was
used as an overlay source only; the Git checkout, older tests, documents and
Git metadata were preserved.

## IMPLEMENTED — Phase-2 repair

- Real transactional v4-to-v5 migration for `recovery_certificates`, including
  historical v4 field preservation in `compatibility_json`.
- Schema version is now **6**.  The explicit transactional v5-to-v6 migration
  creates `recovery_reconciliation_bindings` only after scanning historical
  v5 certificate evidence refs for `reconciliation-run:<run_id>` consumption.
  Single-use history is backfilled deterministically; historical certificates
  remain unchanged.
- Legacy v5 multi-use of one reconciliation run is preserved as an explicit
  `legacy_multi_use` binding with the complete historical recovery-ID list.
  The selected blocking binding and this compatibility evidence prevent any
  new recovery cycle from using that run.  Missing/invalid historical run
  references roll back the migration and leave schema metadata at 5.
- Historical v4 runtime identity is explicitly `legacy-v4-runtime-unavailable`
  and is not treated as current runtime evidence.
- Migration failures roll back table changes and leave `schema_metadata` at v4.
- Fresh v6 foreign-key relationships from the v4 journal are retained.
- Recovery runs are created OPEN with no completion timestamp.  The mandatory
  V1 execution-risk query set cannot be reduced, and completion verifies every
  required query is bound to that exact run, temporally within the run and
  complete, including continuous history retention coverage.
- A completed reconciliation run must belong to the current recovery cycle;
  earlier completed runs cannot authorize a later recovery evaluation.
- Reconciliation evidence hashes are recomputed from the complete immutable
  payload before persistence.  Legacy migrated hashes are marked unverified and
  fail closed for new recovery authority.
- Release-authorizing flat certificates must be journal-derived and independently
  revalidated against the current run, account, instrument, position epoch,
  writer, query set, terminal opening commands, zero position and residual-risk
  evidence.  The complete reservation vector and immutable release audit remain.
- Durable protection evidence now persists explicit full-position market-stop
  semantics and a canonical evidence hash.  Restart recovery validates the
  persisted artifact against current run-bound position, positive conditional/
  native-stop visibility facts and native-protection semantics; `status=CONFIRMED`
  alone, zero quantity or empty facts are insufficient.
- Positive residual opening/conditional risk prevents recovery `READY`.
- Recovery certificate persistence now uses the transactional
  `recovery_reconciliation_bindings` table.  A reconciliation run can be
  claimed by at most one recovery cycle and a recovery cycle can claim exactly
  one run; exact same-ID retries are idempotent, while distinct recovery IDs,
  replayed timestamps and runtime/writer identity mismatches fail closed.
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
  are idempotent; a full causal `record_fingerprint` rejects conflicting
  logical `record_id` metadata across partitions before commitment.  Payload
  and dependency JSON round-trip through real Parquet files.
- Reconstructed replay views use deterministic derived IDs bound to the source
  record and replay rule identity; they do not overwrite an actual record ID.
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
- Full working-tree pytest: **194 passed, 1 skipped**
- Ruff: **passed** under the previous policy; E701/E702/E703 are not suppressed
- Mypy: **passed**, `Success: no issues found in 89 source files`
- Compileall: **passed**
- `git diff --check`: **passed**
- Fresh Python 3.12 hashed-lock install: **passed**
- Fresh-install full pytest: **194 passed, 1 skipped**
- New durable one-use reconciliation binding, idempotent retry, replayed-
  timestamp and runtime/writer identity tests, plus existing temporal
  run-binding, recovery-cycle, protection-positive-evidence,
  retention-coverage and causal-fingerprint tests: **TESTED**
- v5-to-v6 single-consumption backfill, legacy multi-use quarantine,
  migration rollback and fresh-v6 schema tests: **TESTED**
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

- Repair implementation commits: `48b52c4fbb3b3a691ab687135ceccb439a3a72cd`,
  `99d5156201a58fa5c19774e619b31207584636ec` and
  `90b85127b56482668b21533740d3f5b0f052607b`; the durable authority repair
  is `f0a308e`; the v5-to-v6 migration repair is `4f5d136`.
- A tracked-only clone from
  `4f5d136`
  contained all eight
  `src/atlas/data/*.py` files, all five `tests/data/*.py` files, the migration
  integration test and the authority adversarial tests.
- The tracked-only clone installed `requirements-lock.txt` with
  `pip install --require-hashes` under Python 3.12.13 and passed the complete
  suite: **194 passed, 1 skipped**.  The skip was the same explicit,
  credentialed public-testnet check documented above.

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
