# G1-B duplicate-index correction — 19 October 2026

**Classification: `G1B_CORRECTION_TEST_GATE`.** The independent review verdict supplied for `d7c9738749c8b24b027b937fc963af070dc9331d` was “CORRECTION REQUIRED — performance evidence verified, new implementation acceptance withheld.” This correction is a review candidate, not acceptance. It is based directly on that exact candidate, which descends from reviewed `e75a83226cea2cf8dfee10823541848e8ffb0de4` and has direct Git parent `1feef019e4919a468c2690d53c6b9647cb9ef743`, on isolated branch `fix/atlas-red-g1b-duplicate-cache-2026-10-19`.

## Reproduction and correction

Before production edits, an executable regression ran through `BroadProductionOpsCyclePortV2._persist_broad_public_snapshot`, its recovered `PublicCollectorV2`, and the actual `OpsRepository`. It persisted a valid completed Bybit bar and reconciled it as `HEALTHY_CURRENT`; ten seconds later, the same record ID, payload hash, instrument revision and durable reference raised `BROAD_PUBLIC_RECONCILIATION_INDEX_SET_MISMATCH`. The captured source identity was `BYBIT_PUBLIC_HTTP`, record `855b2896193ac61561fbce949c27e9c9766546392f547b85ad631ab006e2061e`, index `7a3d8403afe806a8932ff18a65bfdf0b4fbfd6d7f8f97e23c91f7be730193f00`, raw payload hash `4705b577827a0f2d29c09782f4afacc2f67f833cb495b8c38abdf59d33442920`. The complete red traceback and receipt timestamps are in [red-duplicate-bar-d7.log](evidence/red-duplicate-bar-d7.log).

The production change validates the existing repository index before treating a completed bar as a duplicate: type, cutoff availability, source, record and instrument identity, event chronology fields, translation, revision, quality and availability fields, raw hash shape/value, and the causal bar hash. A valid duplicate adds that exact repository-returned index mapping to both `refs_by_source` and `verified_indexes_by_source`, satisfying the reconciliation set invariant without a second read or fabricated evidence. A mismatched or future index is rejected and leaves the source incomplete. Corrected bars still append a distinct `revision_of` identity. The standard collector path and callers without a verified-index cache are unchanged.

Positive and negative cases pass: same-cycle duplicate and immutable receipt/index checks; restart readback without clearing the existing reconnect health gate; corrected-bar revision; conflicting non-bar duplicate quarantine; wrong-source, future-availability, wrong bar-hash and wrong raw-hash rejection; and rejection of an incomplete verified-index set. Existing 257-reference reconciliation tests retain three bounded page callbacks both with and without the cache, and the real stream-service stall test remains green. The expanded relevant set passed **41/41**. The registered focused G1-B set passed **17/17** (the original 16 plus the incomplete-cache case); its command/output is in [correction-session-focused-g1b.log](evidence/correction-session-focused-g1b.log).

## Regression and environment

The previously published [broad-regression-selected.log](evidence/broad-regression-selected.log) was recoverable in the prior local worktree despite being ignored by `*.log`. Its SHA-256 exactly matches the historical manifest entry, `5d1406c7f50b43026f8ac8b6325e497e061964d66b30a44222176764ec1002fd`; the exact original was restored. It was not reconstructed or relabeled.

The exact previously reported broader selection was rerun as a new correction-session run: **193 passed, 1 failed** in 335.81 seconds. The lone failure is `tests/v2/test_session020_phase2_e2e.py::test_phase2_s1_s2_persistence_ipc_and_matured_outcome`, which imports missing `pyqtgraph` at `src/atlas/desktop/app.py:12`. The same node fails on the clean reviewed `d7c9738` worktree for the same missing package; both outputs are preserved separately. This is **BLOCKED BY ENVIRONMENT** for that desktop test, not a correction regression. The selected run is not a full-suite claim.

Static checks passed: Ruff, compilation, `uv lock --check`, `git diff --check`, algorithm manifest verification, the three immutable freeze hashes, and every entry in the updated `evidence/SHA256SUMS` manifest. Test runtime: CPython 3.12.14, pytest 9.1.1, Ruff 0.17.0, PyArrow 25.0.1, DuckDB 1.5.5 and Nautilus Trader 2.0.0rc5. Exact output is preserved in [correction-session-static.log](evidence/correction-session-static.log).

## Maximum-breadth measurement

The registered offline B runner used the unchanged 2,048 Binance + 2,048 Bybit fixture, three cycles, SQLite WAL/FULL, archive writes, receipt/workset publication, raw/index checks and complete restart readback. Corrected service samples were **9.015640, 10.074105 and 9.742046 seconds**. The second exceeds the unchanged ten-second requirement by 74 ms; under the supplied instruction this is a **TEST GATE**. The exact-parent same-host control measured **9.316096, 10.045548 and 10.699903 seconds**, so the overage also occurs on unmodified `d7c9738`. This fixture contains no completed bars, so it does not execute the new duplicate-verification branch; the control and identical persisted outputs show no evidence that the correction introduced a material service-time increase. The measured overage is retained as a gate, not waived as noise.

The corrected and exact-parent control runs match the published post-reuse result on all three workset hashes, all 384 Parquet SHA-256 values, logical-store SHA-256, raw and source-reference ID hashes, and per-cycle DB-plus-archive growth. Each verified 24,576 raw rows and 24,576 exact index metadata rows after restart, all 40,977 domain artifact metadata rows and three cutoff worksets; zero loss or rejected records. Callback counts were 1,546/1,547/1,547; logical growth was 42,504,371 / 40,694,806 / 40,698,870 bytes. Full B outputs are [corrected](evidence/benchmark-b-correction-max-4096x3.json) and [exact-parent control](evidence/benchmark-b-d7-control-max-4096x3.json). Both preserve the runner’s single audit-denied network/child event, equal to the previously published run; the audit hook rejects it before access.

The registered previous result remains 9.364496/9.774849/9.734267 seconds, with 225 ms worst-sample headroom. This correction run does not establish a new below-ten-second pass. No durability check, population, cadence, source eligibility, capacity setting or policy was changed; `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` remains `None`.

## Commands and remaining gates

All commands ran offline with the existing test environment and the benchmark’s network/child-process audit hook. The red command was:

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python -m pytest -s -vv -o addopts='' -p no:cacheprovider \
  tests/v2/test_session041_production_breadth.py::test_repeated_durable_completed_bar_reconciles_on_actual_production_path
```

Focused G1-B command:

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python -m pytest -q -o addopts='' -p no:cacheprovider \
  tests/v2/test_session041_stream_service_paging.py \
  tests/v2/test_session041_broad_adoption_windows.py \
  tests/v2/test_session041_scale_review.py \
  tests/v2/test_session041_public_runtime_integration.py
```

Expanded affected command (41/41):

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python -m pytest -q -o addopts='' -p no:cacheprovider \
  tests/v2/test_session041_stream_service_paging.py \
  tests/v2/test_session041_broad_adoption_windows.py \
  tests/v2/test_session041_scale_review.py \
  tests/v2/test_session041_public_runtime_integration.py \
  tests/v2/test_session041_production_breadth.py
```

The broader command (193 passed, one environment failure) was:

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python -m pytest -q -o addopts='' -p no:cacheprovider \
  tests/v2/test_session041_stream_service_paging.py \
  tests/v2/test_session041_broad_adoption_windows.py \
  tests/v2/test_session041_scale_review.py \
  tests/v2/test_session041_public_runtime_integration.py \
  tests/v2/test_session041_production_breadth.py \
  tests/v2/test_session041_mixed_broad_capacity.py \
  tests/v2/test_session041_recovery_adoption.py \
  tests/v2/test_session041_binance_qualification.py \
  tests/v2/test_session041_capture_delivery_batching.py \
  tests/v2/test_g1_capture_integrity.py \
  tests/v2/test_g1_qualification_harness.py \
  tests/v2/test_session037_bar_repair.py \
  tests/v2/test_data_runtime.py \
  tests/v2/test_phase1_data_model_integration.py \
  tests/v2/test_session020_phase2_e2e.py \
  tests/v2/test_session027_ops_supervisor.py \
  tests/v2/test_session027_production.py \
  tests/v2/test_session031_public_shadow_readiness.py
```

Its sanitized output is [correction-session-broad-regression.log](evidence/correction-session-broad-regression.log); the expanded result is [correction-session-expanded-tests.log](evidence/correction-session-expanded-tests.log). The parent-side package probe used:

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python -m pytest -q -o addopts='' \
  tests/v2/test_session020_phase2_e2e.py::test_phase2_s1_s2_persistence_ipc_and_matured_outcome
```

Maximum-breadth corrected command:

```sh
PYTHONPATH=src /workspace/work/atlas-g1-env/bin/python docs/research/atlas-red-g1-broad-performance/evidence/benchmark_b.py \
  --per-venue 2048 --cycles 3 --venue both --sampler none \
  --source-checkpoint d7c9738749c8b24b027b937fc963af070dc9331d \
  --dirty-source-diff-sha256 a863f55b377d44b36e25f6286a95db70d6a5256dcbbf551ae1d2035c4a0f31d6 \
  --output docs/research/atlas-red-g1-broad-performance/evidence/benchmark-b-correction-max-4096x3.json
```

The same command on clean `d7c9738`, omitting `--dirty-source-diff-sha256` and writing `benchmark-b-d7-control-max-4096x3.json`, is the scoped timing control. A full command/evidence checksum list is in `evidence/SHA256SUMS`.

Overall G1 remains a test gate. Physical target-host/storage reserve, actual source cadence and continuity, native T+5, 48-hour endurance/retention, and economic qualification remain open. This branch does not accept itself; no merge, public collection, OBSERVE activation, capital or deployment action is authorized. Stop after push for independent exact-SHA review.
