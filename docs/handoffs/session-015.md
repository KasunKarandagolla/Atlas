# ATLAS V2 Session 015 — Phase-2A public-bar instrument revision correction

## Checkpoint, scope and authority

- Starting branch: `impl/session-014-v2-core-intelligence-s1`; required and verified remote tip `eed9f0e2581a06ec09eab3c0fec7b4d232cba8f9`.
- Final branch: `impl/session-015-v2-phase2a-instrument-identity-fix`. Final pushed SHA is reported in the Session-015 closeout because a commit cannot contain its own SHA.
- The older Session-009 checkout and its untracked supplied files were left untouched. Work used a clean dedicated Session-015 worktree.
- Authorities read: `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`; supplied `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md` (SHA-256 `bae3e1a9e48aec64d1292e5bc791c2e87949ed33f4124b1f9807b589cc07a484`); `docs/v2/V1_EXIT_AUDIT.md`; `docs/v2/V1_GOLDEN_BASELINE.json`; and Session-012, Session-013 and Session-014 handoffs.
- Scope: only the Session-014 public-bar revision consumer seam and its tests. No V1 production, Session-012 adapter/raw/bar wire schema, Session-013 worker, policy parameter, persistence schema, dependency or capital path changed.

## Review defect and identity semantics

**TESTED:** Before changing production code or the old fixture, a focused test translated Bybit instrument metadata and final 4H/1H/15M klines using the actual Session-012 adapter functions. It established `key.contract_revision != key.content_hash`, appended bars carrying `key.contract_revision`, and failed: `asof_join(...).status` was `NOT_ESTIMABLE` instead of `AVAILABLE`.

Cause: Session-014 `asof_join` queried `CausalBarStoreV2.as_of` with the full `InstrumentKeyV2.content_hash`, while the store indexes `CausalBarV2.instrument_revision`, delegated from `RawObservationV2.instrument_revision`. Both public venue translators set that field to `key.contract_revision`. Session-014 `feature_snapshot` independently compared each bar against the wrong full-key hash. Its synthetic fixture used the wrong hash and masked the mismatch.

**IMPLEMENTED / TESTED:** `features/joins.py` now queries with `key.contract_revision`; `features/pipeline.py` now validates `bar.instrument_revision == join.key.contract_revision`. No raw/bar schema or stored historical artifact needs migration. The Session-014 fixture now uses a valid 64-character revision digest in its key and sets raw `instrument_revision=key.contract_revision`. The S1 fixture binds its product reference to that revision and asserts all test bars carry it.

The audit of `src/atlas/v2/data/**`, `features/**`, `strategies/**` and `tests/v2/**` found no other revision/full-key confusion in the Session-014 path. Remaining full-key `content_hash` uses are intentional where present: the new tests assert it differs from the raw revision; feature and S1 artifacts hash `key.to_dict()` or retain the complete key for identity; `bar.content_hash`, `feature.content_hash`, product/universe content hashes and source-health hashes identify immutable evidence, not the raw bar revision. Candle, technical and structure code compares bar revisions with other bar revisions. Session-012 translators, collector and bar store consistently use `contract_revision` for raw observation/bar indexing. S1 compares full `InstrumentKeyV2` values across join, feature, universe, quote and watch; it does not use ticker-only identity.

## Adapter and revision regressions

- **IMPLEMENTED / TESTED:** A deterministic, network-free Bybit metadata → kline → final bar → bar store → as-of join → `FeatureArtifactV2` test uses `BYBIT_PUBLIC_HTTP` health and checks the selected revision, exact bar input refs and stable feature hash.
- **IMPLEMENTED / TESTED:** The symmetric Binance USD-M test uses its actual exchange-info/kline translators and `BINANCE_USDM_PUBLIC_HTTP` health.
- **IMPLEMENTED / TESTED:** Both venues use native ticker `BTCUSDT` with different metadata revisions and cannot cross-select. Replacing the otherwise identical key's `contract_revision` returns `NOT_ESTIMABLE` with `MISSING_4H_1H_15M`; a forged join with the wrong key is rejected by the feature snapshot.
- **IMPLEMENTED / TESTED:** A point-in-time Bybit tick-size metadata change creates revision B after revision A. B has no bars before its final triplet arrives. After B arrives, A and B select disjoint histories, and A's earlier immutable feature hash remains unchanged.
- **IMPLEMENTED / TESTED:** The existing long/short S1 path now uses production-equivalent bar revision semantics through join, feature snapshot, durable watch, restart, subsequent trigger and shadow candidate. Candidate quantity remains `None`; no CandidateSet, TradePlan, approval, reservation, order or venue mutation is introduced. S1 policy hash before and after: `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`.

## Gates and environment

- New adapter/revision tests: **TESTED**, 4 passed. Corrected Session-014 core/S1/Chronos suites: **TESTED**, 17 passed. All `tests/v2`: **TESTED**, 78 passed.
- Required V1 invariant smoke: **TESTED**, 11 passed. No V1 production file changed.
- Ruff `src/atlas/v2 tests/v2`: **TESTED**, passed. Mypy same scope: **TESTED**, no issues in 58 files. Compileall same scope and `git diff --check`: **TESTED**, passed.
- `docs/v2/V1_GOLDEN_BASELINE.json`: **TESTED**, unchanged, SHA-256 `b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c`.
- Dependencies and `requirements-lock.txt`: **TESTED**, unchanged; lock SHA-256 `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.
- Environment: Python 3.12.13 on Linux 5.15.0-177-generic x86_64; reused the locked Session-010 virtual environment. No credentials, network request or authenticated endpoint used.
- Secret scan: **TESTED**; no repository scanner binary was available, so a tracked-file plus final-diff fallback inspected for private keys, service tokens, credentials and non-template `.env` files. No high-confidence secret was found. No market download, SQLite database, model weights or sandbox artifact is staged.

## Remaining status

- V2 Phase-2 acceptance, S1 capital authority and all six frozen V1 Bybit capabilities: **TEST GATE**. The venue capabilities remain **UNVERIFIED**; `assisted_enabled=false`, Tier 4 remains false and model influence remains zero.
- S1 economic value, profitability, optional-variant value and foundation-model value: **NOT ESTIMABLE** without the later outcome/evaluator pipeline.
- Exact Chronos-2 checkpoint integration and Windows worker isolation: **UNVERIFIED**.
- 72-hour public-data soak: **BLOCKED BY ENVIRONMENT**; no new soak evidence was produced or process claimed to be running.
- Later Phase-2 work remains outside this corrective branch: S2, CandidateSet selection, economic evaluation, deterministic risk quantity, desktop and capital integration.
