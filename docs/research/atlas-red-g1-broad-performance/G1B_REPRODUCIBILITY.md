# G1-B reproducibility and verification record

Source parent: reviewed `e75a83226cea2cf8dfee10823541848e8ffb0de4`. Candidate branch: `research/atlas-red-g1-broad-performance-2026-10-19`. The exact candidate commit is returned after push for separate review. The implementation source/test diff SHA-256 is `57b94d7a9fa21794218c4f94aa84aeac7514ba47f34f18898d4a717b600bf9ca` (the binary diff over the three listed source/test paths); changed source blobs are `src/atlas/v2/data/collector.py` `b7749567de2a7182d1022ae4801c687113f7af90` and `src/atlas/v2/runtime/production.py` `af13a2a6f3a1c33e6dc69b945bc420f67d193058`.

The controlled comparison used Python 3.12.14 on Linux x86_64 (`Linux-6.18.55-x86_64-with-glibc2.41`), three CPUs, DuckDB 1.5.5, Nautilus Trader 2.0.0rc5 and PyArrow 25.0.1. The source fixture is `tests/v2/test_session041_scale_review.py`; synthetic rows are constructed locally and network/child-process audit events are denied by the benchmark drivers. No market or provider access is part of these runs.

## Benchmark reproduction

A used the unmodified `measure_bounded_cycles(root, per_venue=2048, cycles=3)` helper from the reviewed parent, before production or benchmark edits. Its complete output is `evidence/benchmark-a-unmodified.json`. The helper's timer is adoption plus workset publication; exact receipt/index checks occur between those intervals, while setup and restart/domain audits run outside them. Its 99.06-second total wall time is not the service timer.

B's portable, committed runner is `evidence/benchmark_b.py`. From the candidate repository root, run the baseline against a clean e75 checkout and then the candidate against its current source tree, using fresh output paths:

```sh
python docs/research/atlas-red-g1-broad-performance/evidence/benchmark_b.py \
  --per-venue 2048 --cycles 3 --venue both --sampler none \
  --repo-root /absolute/clean/e75/checkout \
  --source-checkpoint e75a83226cea2cf8dfee10823541848e8ffb0de4 \
  --output /absolute/new-output/g1b-before.json

python docs/research/atlas-red-g1-broad-performance/evidence/benchmark_b.py \
  --per-venue 2048 --cycles 3 --venue both --sampler none \
  --source-checkpoint e75a83226cea2cf8dfee10823541848e8ffb0de4 \
  --dirty-source-diff-sha256 57b94d7a9fa21794218c4f94aa84aeac7514ba47f34f18898d4a717b600bf9ca \
  --output /absolute/new-output/g1b-after.json
```

The runner's `--repo-root` defaults to the repository containing the script. For a baseline run, pass `--repo-root /absolute/clean/e75/checkout`; for the candidate run, use this candidate checkout. The 3-cycle exact outputs are `evidence/benchmark-b-unmodified-max-4096x3.json` and `evidence/benchmark-b-max-after-reuse.json`. The packaged runner was also smoke-tested from its committed location on 128 products per venue, one cycle: 0.725 seconds service time; 512/512 raw rows and exact index metadata verified after restart; zero loss/rejection; no resource sampler enabled.

The committed `docs/research/atlas-red-g1-broad-performance/evidence/profile_c.py` reproduces instrumented C diagnostics. Example baseline run (use a clean e75 checkout as `--repo-root`):

```sh
python docs/research/atlas-red-g1-broad-performance/evidence/profile_c.py \
  --repo-root /absolute/clean/e75/checkout \
  --per-venue 2048 --cycles 3 \
  --source-checkpoint e75a83226cea2cf8dfee10823541848e8ffb0de4 \
  --output /absolute/new-output/g1b-c-before.json
```

For the corrected implementation, use the candidate repository as `--repo-root`, set `--cycles 1`, and add `--source-diff-sha256` to bind the source diff. Compute it with `git diff --binary e75a83226cea2cf8dfee10823541848e8ffb0de4 -- src/atlas/v2/data/collector.py src/atlas/v2/runtime/production.py tests/v2/test_session041_stream_service_paging.py | sha256sum` (discard the filename printed by `sha256sum`). C enables cProfile only within synthetic input construction, adoption and workset publication while a 100 ms `/proc` sampler runs concurrently. It is diagnostic only: pre-change C service samples were 53.137/53.380/52.988 seconds, and corrected one-cycle C measured 15.822 seconds, materially slower than low-overhead B. Do not use C timing as cadence certification.

`docs/research/atlas-red-g1-broad-performance/evidence/profile_lookup.py` records per-call metadata lookup attribution for one cycle. It uses the same `--repo-root`, `--source-checkpoint`, `--output` options as `profile_c.py`; for the corrected checkout also pass `--source-diff-sha256`. The pre-change saved run found 64 reconciliation page refetches for 8,192 indexes, totaling 12.471 seconds; saved low-overhead attribution is `evidence/lookup-unmodified-max-4096x1.json`; the independent cProfile call graph is `evidence/callgraph-c-unmodified-max-4096x1.json`.

## Regression and static checks

The final focused G1-B selection is:

```sh
python -m pytest -o addopts='' -q \
  tests/v2/test_session041_stream_service_paging.py \
  tests/v2/test_session041_broad_adoption_windows.py \
  tests/v2/test_session041_scale_review.py \
  tests/v2/test_session041_public_runtime_integration.py
```

Result: **16 passed**. The selection includes tests for exact cached-index reuse across bounded pages and source mismatch rejection, as well as existing default-read behavior and affected broad adoption/runtime cases.

A broader affected regression selection covered the G1 qualification, recovery/adoption, public runtime, Phase 1/2 persistence and desktop, operations supervisor/production, and Session 031 readiness modules. Result: **185 passed, 1 failed** in 317.45 seconds. The one legacy desktop test fails at `import pyqtgraph`; the exact test fails identically on clean parent e75 because the package is absent from this environment. The failure is not caused by the G1-B diff. Sanitized full console output is preserved as `evidence/broad-regression-selected.log`. This selected regression run is not a full repository-suite claim.

Exact selected command:

```sh
python -m pytest -q -o addopts='' \
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

Static checks passed: Ruff on both implementation files, changed test and two packaged diagnostic drivers; `compileall` on those files; `uv lock --check`; `git diff --check`; `sha256sum -c algorithms/MANIFEST.sha256`; and the three frozen contract hashes. The source-map and acceptance-register updates required regenerating only the algorithm manifest. Freeze digests remain V1 `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`, V2 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, and Agent `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`. `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` remains `None`.


The baseline failure check was:

```sh
python -m pytest -o addopts='' -q \
  tests/v2/test_session020_phase2_e2e.py::test_phase2_s1_s2_persistence_ipc_and_matured_outcome
```

It fails on both the candidate and e75 at `src/atlas/desktop/app.py:12`, `ModuleNotFoundError: No module named 'pyqtgraph'`.
