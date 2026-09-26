# ATLAS V2 Session 020 handoff

## Checkpoint and scope

- Required starting branch: `impl/session-019-v2-m0-scenario-evaluator-shadow-plan`.
- Required starting SHA: `3d491d5fa4ed2fee1bd575c893d16b33994d60cf`; the remote tip matched exactly before edits. No reviewed Session-020 branch/checkpoint existed at startup.
- Work branch: `impl/session-020-v2-desktop-ipc-phase2-gate`.
- Final pushed tip SHA and remote-tip verification are recorded in the final Codex closeout. A Git commit cannot contain its own hash.
- Read both authoritative freeze documents and the Sessions 016–019 handoffs in full. Phase-2 desktop/evidence integration only; no Session-021 work.
- No capital authority, order path, RiskPolicy mutation, strategy promotion, venue qualification, or V1 runtime change was added.

## Changed files

- `atlas-desktop.spec`, `pyproject.toml`, `requirements-lock.txt`, and `docs/v2/V1_GOLDEN_BASELINE.json`.
- `src/atlas/desktop/{__init__.py,app.py,models.py}` and `src/atlas_desktop_entry.py`.
- `src/atlas/v2/desktop/{__init__.py,ipc.py,projection.py}`.
- `src/atlas/v2/memory/{repository.py,schema.py}`.
- `tests/v2/test_session020_{desktop_ipc,phase2_e2e}.py`.
- Test-fixture integration changes in `tests/v2/test_session017_risk.py` and `tests/v2/test_session019_evaluation_integration.py`.
- This handoff.

## Desktop, projection and IPC

- The desktop is a separate PySide6 process. It only owns a `ProjectionClient`; it does not start, stop, or control the engine. PyQtGraph renders persisted causal candle bars when present. The UI never recomputes strategy signals.
- Overview shows the projection process, source-health and input freshness, universe/scanner status, decision counts, capability and capital state. Unsupported engine/process version, uptime, queue, disk, model-worker and recovery values are explicitly `UNAVAILABLE`; research health, degraded data, capital block and protection/recovery status remain separate. `NOT ESTIMABLE`, `UNVERIFIED`, `TEST GATE` and `BLOCKED BY ENVIRONMENT` remain distinct.
- Scanner preserves the latest CandidateSet members, including selected, rejected and unselected rows, with per-instrument eligibility and observed state, strategy/policy identity, rank, sizing, frozen-action ref, evaluation/reasons, expiry and synthetic marker. Eligibility is displayed as evidence, never capital authority.
- Chart reads final persisted candle evidence and its indexed `PublicObservationIndexV2` refs at the selected information cutoff. Missing archives/bars produce an explicit unavailable state. No candles are interpolated; no feature overlays are invented.
- Watches come from persisted `OpportunityWatchV2` lifecycle rows, including terminal states and causal refs. Evidence is a bounded sanitized artifact summary/detail view with schema/version, hashes, provenance, decision/event/availability times, dependency refs, source-health and status/reasons. Raw credentials, private account IDs and arbitrary metadata are not transported.
- Immutable projection contract: schema version `2`, projection version `ATLAS_DESKTOP_PROJECTION_V2`. It uses UTC nanoseconds internally, stable bounded sorting, exact SHA-256 refs, explicit freshness/unavailable states and deterministic canonical JSON. It is a dedicated DTO, not an `EvaluationArtifactV2` transport.
- IPC: authenticated loopback TCP, protocol version `2`, 4-byte length-prefixed canonical JSON, UUID request correlation, one-megabyte frame bound, bounded client count and three-second socket timeout. Only loopback bind/connect is accepted. The token file is private on POSIX and never appears in a snapshot or error.
- Read-only command set: `ping`, `health`, `snapshot`, `overview`, `scanner`, `watches`, `evidence`, `chart`. Unknown/mutating commands fail closed. No pickle, order/risk/capital command, arbitrary path request, or credential API exists.
- `OpsRepository(read_only=True)` opens SQLite with `mode=ro`, `query_only`, validates the existing schema without initialization, and rejects transactions. Projection reads use existing indexed artifacts and bounded watch/source-health queries; V1 capital-control storage is not opened.

## Process separation and integration results

- Process-safety test starts the projection service, launches an offscreen Qt desktop, verifies the five view names and snapshot response, then exits the desktop unexpectedly. The service remains alive, the SQLite database hash is unchanged, and a fresh client reconnects.
- S1 engineering E2E uses the production collector/archive, point-in-time universe, causal feature artifacts, S1 persisted watch/candidate, shared CandidateSet/selection, deterministic hard-risk sizing, frozen action, Session-019 evaluation, terminal DecisionCalendar, evidence projection, IPC and Qt chart rendering.
- The S1 fixture correctly ends `NOT_ESTIMABLE`; its persisted reason codes are displayed exactly. No real M0 support is fabricated.
- S2 creates a candidate through its own coordinator and policy, retains S2 setup/trigger and failed-break management evidence, and reaches the same shared CandidateSet/scanner projection as an unselected row. The UI reports its actual `NOT_APPLICABLE` economic status; S1 management rules are not substituted.
- The synthetic fixture replays the exact frozen action to a later full-fill retrospective payoff and indexes a matured `EXECUTABLE_ACTION_VALUE` outcome. The original `NOT_ESTIMABLE` DecisionCalendar entry and frozen action link are asserted unchanged. Synthetic status remains visible in scanner, chart and evidence; diagnostic-only labels remain distinct.

## Dependencies and packaging

- Added optional `desktop` extra: PySide6, PyQtGraph and PyInstaller. Console entry points: `atlas-v2-projection` and `atlas-desktop`. Headless imports do not require Qt.
- Lock regenerated reproducibly with uv 0.11.8 for Python 3.12 / x86_64 manylinux. `requirements-lock.txt` SHA-256 changed from `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39` to `466b64fab963e2d1884be733bd9cc6836969d15215979bf73f055fff2a4de0cc`. NautilusTrader remains pinned at 2.0.0rc5; no unrelated runtime dependency was changed. Lock validation checked 29 packages and reported no changes needed.
- The V1 golden metadata now records the new complete lock digest. Recomputed V1 contract and decision/replay golden values are unchanged. Golden JSON SHA-256 changed from `b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c` to `40af3f5438a41954ce6ea6465273d888f7db72d92b40f3592eadba009d1ae26e`.
- Linux PyInstaller bundle: **PASS**, built with Python 3.12.13, PyInstaller 6.22.3, PySide6 6.11.2 and PyQtGraph 0.14.0. The final windowed bundle was started with Qt offscreen against a fixture projection service, received a V2 snapshot, terminated, and reconnected while service/database state remained unchanged. PyInstaller warned that optional `pyqtgraph.opengl` 3D submodules were unavailable because PyOpenGL is not installed; the desktop uses the bundled 2D chart only.
- Windows packaging: **BLOCKED BY ENVIRONMENT**; this session ran on Linux and had no Windows host/runner. No cross-compilation claim is made.

## Regression and quality gates

- Focused Session-020 desktop/IPC/E2E tests: **6 passed, 0 failed, 0 skipped**. The offscreen Qt tests ran directly; none were skipped.
- All `tests/v2`: **207 passed, 0 failed, 0 skipped**.
- Session-017 impacted action/integration/replay/risk, Session-018 contracts/forward/remediation, and Session-019 admission/evaluation/M0/remediation/scenarios: **98 passed, 0 failed, 0 skipped**.
- Complete V1 suite (`tests --ignore=tests/v2`): **414 passed, 0 failed, 1 skipped**. The sole skip is the existing opt-in public testnet check because credentials/network are not required. The V1 golden check is included and passed.
- Standalone V1 golden comparison: **1 passed**. The initially run V1 suite found only the intentional lock-digest mismatch after adding the optional desktop extra; updating that metadata made the complete rerun pass without changing recomputed V1 values.
- Ruff `ruff check .`: passed. Mypy `mypy src tests`: no issues in **271 source files**. `compileall -q src tests`, `git diff --check`, and dependency-lock validation: passed.
- Required V2 tests exercised prefix/future-tail invariance, SMC swing confirmation, S1/S2 selection and strategy safety, OOF-only M0, no-fill/partial-fill/cost replay, rolling-risk arithmetic, frozen policy hashes, source/evidence chronology, and the desktop read-only/crash/reconnect gates.
- Preserved hashes: S1 `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`; S2 `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`; selection policy `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`.

## Security review and status

- `gitleaks`, `trufflehog` and `detect-secrets` executables are unavailable. The fallback scan covered **305 tracked paths**: 0 disallowed paths; 2 generic credential-assignment pattern hits in unchanged offline test fixtures (`test_data_runtime.py` and `test_model_runtime.py`); and 0 private-key, cloud/source token or high-confidence key-pattern hits. The complete staged diff had **0 pattern hits**. The two fixture hits are mocked API-key query/environment values, not credentials; matched values were not printed.
- Real S1/S2 economic value and V2 profitability remain `NOT ESTIMABLE`. Real eligible historical M0 labels and qualified joint execution templates/calibration are still absent. Continuous live high-resolution data, exact real action-to-position linkage and authenticated exact venue qualification remain `UNVERIFIED` / `TEST GATE`. The 72-hour soak remains `BLOCKED BY ENVIRONMENT`. Capital remains disabled.
- No `HISTORICAL_DIAGNOSTIC`, `PROSPECTIVE_SHADOW`, `INCREMENTAL_VALUE_PASS` or `DECISION_ELIGIBLE` claim is made. Fixture success is engineering validation only.
- Phase-2 engineering integration gate: **PASSED** on the tests and local environment above, with Windows packaging explicitly environment-blocked. Do not interpret this as economic, venue or prospective-shadow qualification.
- Exact next scope from the frozen plan for Session 021: **Phase 3 explicit breadth — S3, S6, S7 collection/alerts/safety and mechanical feature/context wiring.** Session 021 was not started. Coordinating ChatGPT must independently review the pushed Session-020 SHA, diff, tests and handoff through GitHub before authorizing Phase 3.
