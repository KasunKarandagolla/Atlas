# ATLAS RED R1 verification log

**Date:** 2026-10-13 UTC
**Branch:** `impl/red-r1-surgical-correctness-2026-10-13`
**Test environment:** isolated `work/r1-test-venv`, CPython 3.12.14, synced with `uv sync --locked --extra dev`; locked PyArrow 25.0.1, DuckDB 1.5.6, Nautilus Trader 2.0.0rc5. The project lock and dependency declarations were not edited.

## Test-first evidence

Before implementing the repairs, the new focused regressions reproduced the target failures: 7 V2 gap/coverage/VWAP/S1 assertions and 6 V1 chronology/source-identity assertions failed. After implementation, the V1 golden digest was explicitly pinned to the existing `snapshot_hash()` value.

One added exact-prefix regression initially failed in its fixture because it tried to advance more than the history API’s 128-bar work limit in one call. The fixture was changed to use bounded pages; its assertion then passed. A production run also caught a too-broad S1 warmup guard. That guard was removed; the strategy retains its existing warmup behavior, while cadence checks and the independent hard-sizing evidence requirement remain.

Final diff review found that the legacy UTC-day VWAP key still carried a partial value. It now stays unavailable until full-day coverage is proven; partial-to-cutoff VWAP has its own feature key, and S1 checks that explicit partial key. The M0/S1/production consumers were rerun after that correction.

## Passing regression groups

| Group | Result | Scope |
| --- | ---: | --- |
| R1 and affected V2 consumers | **199 passed** in 32.27s | Includes source-reference rebinding rejection; V2 risk, S1/S2, exact-prefix history, chronology, M0, S3, installed economics, trade location, supervisor |
| V1 chronology and phase-4 compatibility | **28 passed** in 10.68s | New chronology/identity cases, frozen V1 golden, prefix invariance, persistence, full phase-4 end-to-end funding lifecycle |
| S27 default production composition | **16 passed** in 308.45s | Final run after the VWAP feature-key correction; includes PyArrow-backed restart and native-M1 trade-completeness gates |

Commands used (with the same test paths shown in the evidence manifest):

```bash
work/r1-test-venv/bin/python -m pytest -o addopts='' -q -rA --basetemp=work/pytest-v2-vwap-identity-final \
  tests/v2/test_r1_surgical_correctness.py tests/v2/test_session017_risk.py \
  tests/v2/test_session014_core.py tests/v2/test_session014_s1.py tests/v2/test_session016_s2.py \
  tests/v2/test_session037_active_history.py tests/v2/test_session037_feature_prefix.py \
  tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_action_contract.py \
  tests/v2/test_session041_trade_location.py tests/v2/test_session041_trade_location_runtime.py \
  tests/v2/test_session037_chronology.py tests/v2/test_session037_installed_economics.py \
  tests/v2/test_session027_ops_supervisor.py tests/v2/test_session019_m0.py \
  tests/v2/test_session019_evaluation_integration.py tests/v2/test_session021_data_s3.py
```

```bash
work/r1-test-venv/bin/python -m pytest -o addopts='' -q -rA --basetemp=work/pytest-v1-all \
  tests/science/test_r1_v1_chronology_identity.py tests/science/test_phase4_baseline.py \
  tests/contract/test_v1_golden_baseline.py tests/science/test_prefix_invariance_phase4.py \
  tests/science/test_phase4_persistence.py tests/science/test_phase4_end_to_end.py
```

```bash
work/r1-test-venv/bin/python -m pytest -o addopts='' -q -rA --basetemp=work/pytest-production-final-vwap \
  tests/v2/test_session027_production.py
```

After the final source-reference identity guard, the directly affected production restart path was rerun:

```bash
work/r1-test-venv/bin/python -m pytest -o addopts='' -q -rA --basetemp=work/pytest-r1-restart-final \
  tests/v2/test_session027_production.py::test_default_production_composes_public_to_risk_action_and_economics_after_restart
```

Result: **1 passed** in 36.99s.

No full repository suite was run for R1. Historical S49 full-suite evidence is incomplete and is not represented as a full-suite pass.

## Static checks

- **PASS:** scoped Ruff check on all changed Python source and test files.
- **PASS:** `python -m compileall -q src tests`.
- **PASS:** `uv lock --check --python 3.12`.
- **PASS:** `uv pip check --python work/r1-test-venv/bin/python` (all 20 locked packages compatible).
- **PASS:** `git diff --check`.
- **BASELINE MATCH:** full-source mypy over `src/atlas` reports 52 existing errors both at the accepted S49 base and current source; normalized output has no delta. Mypy exits nonzero on both.
- **BASELINE FAILURE:** full-repository Ruff reports 16 diagnostics in six unchanged historical evidence scripts under `docs/v2/transition-s45-evidence/` and `docs/v2/transition-tq01-evidence/`. Scoped Ruff passes.
- **FORMAT BASELINE:** ten modified legacy Python files already failed `ruff format --check` at `HEAD`; they still fail, and were not broadly reformatted. The two newly added regression modules and the two modified files that passed at baseline pass format check.

The machine did not need user credentials, exchange access, market requests, or external providers. Package installation used the environment’s configured package-manager egress and the existing lockfile only.
