# Reproducibility and review boundaries

Primary source base `19a2d91ebc1b136722e7baf18ea32b4b4d4cbe81`; G0 specification/authority-only commit `402648d` precedes all benchmark-driven code changes. Source and workload hashes are in `evidence/qualification-final.json`; exact final remote candidate SHA is returned at delivery. Review the entire base→candidate diff,not only the final commit. The final commit's immediate parent is the G0 commit.

The available execution environment initially lacked PyArrow,DuckDB,Nautilus. A separate tools venv was created at `/workspace/work/atlas-g1-env`, without changing the original checkout or repository locks. Actual commands:

```sh
python -m venv --system-site-packages /workspace/work/atlas-g1-env
/workspace/work/atlas-g1-env/bin/python -m pip install pyarrow==25.0.1 duckdb==1.5.5 nautilus-trader==2.0.0rc5 websockets==17.0.1 pytest==9.1.1 hypothesis==6.169.0 ruff==0.17.0
/workspace/work/atlas-g1-env/bin/python -m pip install lightgbm==4.7.0
/workspace/work/atlas-g1-env/bin/python -m pip install pyside6==6.11.2
/workspace/work/atlas-g1-env/bin/python -m pip check
```

No fit,generation,weights or provider call was performed. Exact installed versions are separately recorded. This is an isolated verification environment,not a complete locked production build. Missing initial dependencies are identified,not counted as tests passed; the repaired environment executes the original PyArrow/Nautilus golden cases in the final regression. Existing historical failures remain unchanged.

## Measurement commands actually run

The retained `evidence/preflight_profile_driver.py` is the actual baseline driver: it strips credentials and denies socket/DNS,records the untouched host-path preflight,then cProfiles the original default workload using explicitly injected free space and storage-only dependency hooks. It was run from the isolated checkout before the source fixes. The candidate run used the same driver with output names changed from `baseline` to `candidate`. The driver writes to the scratch `/workspace/work/atlas-g1-run`; create it before reproducing. To compare,run from separate base/candidate source checkouts with the same interpreter/device and no concurrent workload. Do not change its workload,thresholds or FULL/fsync rules.

```sh
/workspace/work/atlas-g1-env/bin/python /workspace/work/atlas-g1-run/baseline.py
/workspace/work/atlas-g1-env/bin/python /workspace/work/atlas-g1-run/candidate.py
/workspace/work/atlas-g1-env/bin/python scripts/qualify_public_observation.py --storage-path /workspace/work/atlas-g1-run/host-probe --output /workspace/work/atlas-g1-run/qualification-final.json --maximum-breadth
```

`baseline.py` bytes are retained as `preflight_profile_driver.py`; `candidate.py` differs only in output names. Preflight JSON ephemeral selected-path strings are normalized to `TEMPORARY_G1_PROBE` and rehashed in the public copies; timing,populations and facts remain unchanged. Public text/XML copies normalize trailing whitespace; primary JSON compacts repeated telemetry rows without changing its domain/content hash. The final guarded harness emits its sanitized record directly. `qualification-final.json` is primary exact-build evidence. A first exploratory harness run in scratch produced the same42,504,371-byte maximum and the same cadence failure; it is not used as final code-identity evidence because harness reporting/validation was being edited. No failed capacity sample is suppressed from the primary report.

The maximum-breadth helper is reused directly from `tests/v2/test_session041_scale_review.py`,with `per_venue=2048,cycles=3`. It invokes the native collector,Parquet/SQLite persistence,broad workset and all exact post-restart readers. It is not a substitute market simulator or economic evaluator. All source inputs are synthetic,no historical-data download occurs. No genuine retained raw market archive was found in the explicitly scoped workspace locations; repository-audit ZIPs were not treated as market evidence.

## Tests and static commands

The small retained `evidence/offline_pytest.py` is the final corrected launcher: credential values are removed; a Python audit hook denies all connect/DNS/sendto; plugin autoload is disabled. Its `if __name__ == '__main__'` guard is required for the native preparation-worker fixture. Use the tools interpreter as `$ATLAS_G1_PYTHON` below; the actual interpreter was `/workspace/work/atlas-g1-env/bin/python`.

```sh
$ATLAS_G1_PYTHON docs/research/atlas-red-g0-g1/evidence/offline_pytest.py \
 tests/v2/test_g1_capture_integrity.py tests/v2/test_g1_qualification_harness.py \
 tests/v2/test_session040_storage_preflight.py tests/v2/test_session041_broad_storage_preflight.py \
 tests/v2/test_session040_capture_receipts.py tests/v2/test_session040_durable_capture.py \
 tests/v2/test_session039_public_archive_extents.py tests/v2/test_session041_capture_pressure_reserve.py \
 tests/v2/test_session041_capture_delivery_batching.py tests/v2/test_session032_public_stream_continuity.py \
 tests/v2/test_session032_public_stream_source.py tests/v2/test_session032_public_stream_integration.py \
 tests/v2/test_session041_broad_public_source.py tests/v2/test_session041_broad_stream_source.py \
 tests/v2/test_session041_binance_qualification.py tests/v2/test_session040_live_health.py \
 tests/v2/test_session041_dynamic_universe.py tests/v2/test_data_runtime.py \
 tests/v2/test_session022_s4_microstructure.py tests/v2/test_session037_chronology.py \
 tests/contract/test_v1_golden_baseline.py --junitxml=/workspace/work/atlas-g1-run/regression-verified.xml
```

Final corrected run: **339passed,0failed,0skipped in41.39s**. Additional targeted missing-input tests and their counts are in EXECUTION_EVIDENCE.json. No full-repository suite is claimed.

Retained earlier runs:

- Baseline native capture/source regressions:83passed,zero failures/skips,18.34s.
- New capture counterexamples against unchanged production owners:2failed/3passed; exact partial adoption and decreasing maximum reproduced. Same5pass after fixes.
- First expanded run:337passed/2failed in58.16s. The temporary launcher lacked a multiprocessing main guard; the native spawned fixture reran pytest instead of warming its worker,then hit its unchanged15s startup gate. The same run also triggered a Hypothesis slow-generation health check. This was a test-launcher defect,not a reason to lengthen production deadlines or disable Hypothesis checks. The corrected launcher reran those2tests and all18new G1 cases:20passed in1.67s. The final complete339-test selection above then passed in a single corrected run. Both failed and corrected artifacts are retained. There is no claim of339passes in the first failed run.

```sh
/workspace/work/atlas-g1-env/bin/ruff check scripts/qualify_public_observation.py tests/v2/test_g1_capture_integrity.py tests/v2/test_g1_qualification_harness.py src/atlas/v2/runtime/storage_preflight.py src/atlas/v2/data/durable_public_capture.py
/workspace/work/atlas-g1-env/bin/python -m compileall -q src tests/v2/test_g1_capture_integrity.py tests/v2/test_g1_qualification_harness.py scripts/qualify_public_observation.py
uv lock --check --offline
(cd algorithms && sha256sum -c MANIFEST.sha256)
git diff --check
```

Also check all three canonical freeze hashes from AGENTS.md,unchanged canonical algorithm/PDF,all strategy/feature/risk/policy/clock/lock identities and historical evidence against the parent. Only two production owners change. New harness code has no order/private/provider transport path; explicit negative tests deny both network and child execution and test secret-variable removal without reading credential values. The existing public-only diagnostic factory/profile restrictions and Linux path refusal are regression-tested; its network command was never invoked.

## Publication integrity

`ARTIFACTS.sha256` hashes every new package file except itself,plus new source/tests and affected production owners. `EXECUTION_EVIDENCE.json` records commands,counts,measurement limitations and parent protection checks. Canonical MANIFEST changes only the affected acceptance/implementation progress pointers; canonical algorithm/PDF are not regenerated. Acceptance state columns are not promoted. Original translog/log/state history is preserved.

Commit and push the isolated branch,then compare `git rev-parse HEAD` to `git ls-remote --heads origin research/atlas-red-g0-g1-capture-qualification-2026-10-18`; inspect the exact immediate parent and ancestry to19a2d91 and accepted designf9f8ef0. A commit cannot embed its own SHA. The exact verified SHA is returned for independent review; only a later external review may record its acceptance. No merge or next session follows.
