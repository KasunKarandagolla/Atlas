# ATLAS RED — Final R1 Correctness Closeout Test Log

Date: 2026-10-13 UTC
Branch: `fix/red-r1-final-correctness-2026-10-13`
Baseline: `8b77c76b2ef0581cc453bc4f806693884e763cb4`

All green commands below ran from `/workspace/Atlas-r1-final`, used Python 3.12.14, disabled bytecode, and set `PYTHONPATH=src`. `--basetemp` points to separate bounded temporary directories. No test was skipped, xfailed, or weakened for this closeout.

## Test-first red evidence

The pre-edit regression run produced **7 failures and 1 pass**. It failed for short/long caller-declared source lists (1, 2, 720, 722), the two-ID fixture, altered full feature payload with the same legacy snapshot hash, and historical evaluation without a receipt. The empty source-list case already failed closed. The raw output is retained losslessly as [`ATLAS_R1_FINAL_RED_OUTPUT.txt.gz`](ATLAS_R1_FINAL_RED_OUTPUT.txt.gz). Its uncompressed SHA-256 is `36b738b1b37c1f37831bdcf76a99ce925ca3e11e18c1a735b5bb3f32a797d380`; compressed file SHA-256 is `987abf80ece043192e182db85648c5ec9963af26517f05cc484a38384e9e2aab`.

The shell argument string for that original red invocation was not retained with its stdout; the raw failing output is retained, and the exact test module/cases are identified above. This does not affect the final green commands below.

An initial test attempt also stopped because PyArrow was missing from the inherited environment. The locked Python 3.12 development environment was synchronized, including locked PyArrow 25.0.1; final archive and persistence tests then ran through the concrete Parquet implementation. An early V1 golden attempt also lacked Nautilus Trader; the locked development environment supplied the existing `2.0.0rc5` dependency and the final golden test passed.

## Final green commands

### Source proof and scanner request binding

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-source-identity tests/science/test_r1_source_qualification.py tests/scanner/test_phase5_end_to_end.py
```

Result: **36 passed in 29.32s**. This includes 721 archived closes qualifying through evaluation and persistence; 0/1/2/720/722 rejection; absent/fabricated and noncanonical source IDs/revisions; missing, duplicate, late, revised and conflicting observations; future publication/valid-from; actual versus reconstructed availability; deadline equality/after-deadline; full feature payload verification; cross-instrument/slot/computation and scanner-request binding; archive tampering; restart re-verification from serialized snapshot and proof values; and the old two-ID fixture.

### V1, historical Phase 4, archive, recovery and scanner consumers

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-v1-identity tests/contract/test_v1_golden_baseline.py tests/science/test_r1_source_qualification.py tests/science/test_r1_v1_chronology_identity.py tests/science/test_r1_v1_snapshot_compatibility.py tests/science/test_phase4_baseline.py tests/science/test_phase4_engine_decisions.py tests/science/test_phase4_end_to_end.py tests/science/test_phase4_persistence.py tests/science/test_phase4_artifacts_and_controls.py tests/science/test_prefix_invariance_phase4.py tests/science/test_decision_calendar.py tests/scanner/test_phase5_end_to_end.py tests/data/test_archive_runtime.py tests/data/test_prefix_and_archive.py tests/persistence/test_recovery_persistence.py tests/integration/test_v1_phase0_6_acceptance.py
```

Result: **112 passed in 40.74s**. The frozen V1 golden and prefix-invariance checks, old fixture recovery, Phase-4 calculations, trade-plan persistence, decision calendar, archive and recovery comparators ran on the final implementation. Historical diagnostic replay without a new receipt retains its pinned snapshot/action/TradePlan/calendar hashes; the qualified path returns `NOT_ESTIMABLE` without verified source proof.

### Affected R1 V2 consumers

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-v2-final tests/v2/test_r1_surgical_correctness.py tests/v2/test_session017_risk.py tests/v2/test_session014_core.py tests/v2/test_session014_s1.py tests/v2/test_session016_s2.py tests/v2/test_session037_active_history.py tests/v2/test_session037_feature_prefix.py tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_action_contract.py tests/v2/test_session041_trade_location.py tests/v2/test_session041_trade_location_runtime.py tests/v2/test_session037_chronology.py tests/v2/test_session037_installed_economics.py tests/v2/test_session027_ops_supervisor.py tests/v2/test_session019_m0.py tests/v2/test_session019_evaluation_integration.py tests/v2/test_session021_data_s3.py
```

Result: **199 passed in 27.62s**.

### S45 outcome/content-hash and maturity consumers

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-s45-outcomes tests/v2/test_session033_outcome_resolution.py tests/v2/test_session033_outcome_runtime.py tests/v2/test_session037_action_outcome_producer.py tests/v2/test_session037_due_work.py tests/v2/test_session033_outcome_maturity.py
```

Result: **76 passed in 5.08s**.

### S27 production and restart composition

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-s27-production tests/v2/test_session027_production.py
```

Result: **16 passed in 226.58s**.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/pytest -o addopts='' -q -p no:cacheprovider --basetemp=/workspace/atlas-r1-final-s27-restart tests/v2/test_session027_production.py::test_default_production_composes_public_to_risk_action_and_economics_after_restart
```

Result: **1 passed in 34.35s**.

## Static and contract checks

- `ruff check` on all changed Python files: pass.
- `ruff format --check` on new `source_proof.py` and `test_r1_source_qualification.py`: pass. Baseline-aware comparison on the touched legacy files reports the same nine formatter failures at base and current; no broad reformat was applied.
- Full-source `mypy --no-incremental src/atlas`: 52 diagnostics on current (279 files) and 52 on accepted baseline (278 files), normalized delta **+0/-0**. Four initial diagnostics in new `source_proof.py` were fixed before this final result.
- `PYTHONPYCACHEPREFIX=/workspace/atlas-r1-compile-final PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /workspace/.venvs/atlas/bin/python -m compileall -q src tests`: pass.
- `uv lock --check --python 3.12`: pass.
- `uv pip check --python /workspace/.venvs/atlas/bin/python`: pass, 20 packages compatible.
- `git diff --check`: pass. No dependency, lock, license, frozen authority, capital, assisted-order, or live-execution changes.
- The three supplied frozen-authority SHA-256 values match exactly; see the handoff and evidence manifest.

## Resource evidence and remaining test gate

Instrumented process-write measurements retained from the bounded setup/focused runs total **1,534,853,120 bytes**: locked environment sync 244,002,816; V1 focused group 20,111,360; affected V2 group 545,185,792; S27 production 523,386,880; S27 restart 202,166,272. Current selected retained test temporary artifacts total **572,362,202 bytes**, including the final S27 directories at 162,096,478 and 28,980,956 bytes. These are retained-output sizes, not substitutes for write counts.

At the final measurement the workspace filesystem showed 1,735,708,672 bytes used and 30,299,721,728 available. Workspace occupancy and instrumented process writes are recorded separately. Dependency download/install writes and the complete rolling 24-hour host total were not independently instrumented, so this report does not claim a precise global 24-hour write total.

**TEST GATE:** the full repository suite was not run because its rolling write budget could not be bounded safely. The S49 retained qualification gate also remains: the 1,024-contract mixed delivery case reached queue high-water 480/512 and rejected a frame; the prior repository run stopped after 1,196/2,783 tests in 1:01:24 with six failures and five skips. No S49 assertions were weakened.
