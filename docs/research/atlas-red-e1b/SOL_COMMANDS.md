# Independent Sol reproduction commands

Run from the candidate repository root with Python 3.12 and existing NumPy/pytest available. No dependency file changes are required. The recorded environment used NumPy 2.3.5 from the managed runtime and pytest 9.1.1 from the existing research venv; exact observed versions are authoritative in `SOL_EXECUTION_EVIDENCE.json`. This was not a complete realization of the production lock (NumPy 2.5.3/Nautilus).

Set paths to external artifacts/caches; keep them outside Git:

```bash
ATLAS_PANEL=/path/to/DIAGNOSTIC_FEATURE_PANEL.csv.gz
ATLAS_CSV_CACHE=/path/to/verified-extracted-csvs
ATLAS_ZIP_CACHE=/path/to/retained-official-zips
ATLAS_SCRATCH=/path/to/empty-independent-scratch
ATLAS_REPEAT_SCRATCH=/path/to/another-empty-independent-scratch
ATLAS_REPEAT_OUTPUT=/path/to/repeat-output
ATLAS_PYTHON=python3
ATLAS_TEST_PYTHON=python3
```

If the external full panel/CSV cache is unavailable, use the unchanged pinned producer to rebuild **outside** the checkout, preserving the reviewed source/dataset manifests in Git. Follow `REPRODUCIBILITY.md`'s parser-reject extraction and producer recipe with an external `--outdir`. That costs the same bounded 757-object acquisition only when no verified cache is available. Verify the generated full-panel hash against `68ee994b983f47de86cec2781c9469f6e992d03a03728517978790b571c9d682`; STOP on mismatch. Acquisition timestamps/timings in a new producer manifest can differ: they are not stable market identities. The independent source check below uses the exact reviewed manifest's official URLs/hashes and binds cached CSV bytes to ZIP members; do not overwrite the pinned manifests with new transport metadata.

## Source and full-calendar runs

```bash
"$ATLAS_PYTHON" docs/research/atlas-red-e1b/verify_sol_sources.py \
  --manifest docs/research/atlas-red-e1b/SOURCE_MANIFEST.jsonl \
  --cache "$ATLAS_ZIP_CACHE" --csv-cache "$ATLAS_CSV_CACHE" \
  --output docs/research/atlas-red-e1b/SOL_SOURCE_CHECKS.jsonl \
  --execution /path/to/source-execution.json

"$ATLAS_PYTHON" docs/research/atlas-red-e1b/independent_sol_reproduction.py \
  --panel "$ATLAS_PANEL" --csv-cache "$ATLAS_CSV_CACHE" \
  --source-proof docs/research/atlas-red-e1b/SOL_SOURCE_CHECKS.jsonl \
  --scratch "$ATLAS_SCRATCH" --outdir docs/research/atlas-red-e1b

"$ATLAS_PYTHON" docs/research/atlas-red-e1b/independent_sol_reproduction.py \
  --panel "$ATLAS_PANEL" --csv-cache "$ATLAS_CSV_CACHE" \
  --source-proof docs/research/atlas-red-e1b/SOL_SOURCE_CHECKS.jsonl \
  --scratch "$ATLAS_REPEAT_SCRATCH" --outdir "$ATLAS_REPEAT_OUTPUT"

cmp docs/research/atlas-red-e1b/SOL_REPRODUCTION_RESULTS.json "$ATLAS_REPEAT_OUTPUT/SOL_REPRODUCTION_RESULTS.json"
cmp docs/research/atlas-red-e1b/SOL_MISMATCH_EXAMPLES.jsonl "$ATLAS_REPEAT_OUTPUT/SOL_MISMATCH_EXAMPLES.jsonl"
```

The source check fetches only pinned missing objects and reuses present verified ZIP/checksum files. Its first recorded run fetched all 757 because only CSVs were retained previously; its repeat fetched no archive payload. The stable source proof must retain the published proof hash. Actual 2026 acquisition timestamps remain in separate execution metadata and prove no historical receipt.

## Executed test groups and checks

The real-source tests skip if `ATLAS_SOL_CSV_CACHE` is not supplied. Both real-source tests executed in this session: final focused run has **zero skips**. `ATLAS_SOL_INPUT_RESULTS` writes the stable all-window input-comparison summary.

```bash
ATLAS_SOL_CSV_CACHE="$ATLAS_CSV_CACHE" \
ATLAS_SOL_INPUT_RESULTS=docs/research/atlas-red-e1b/SOL_S6_INPUT_COMPARISON.json \
  "$ATLAS_TEST_PYTHON" -m pytest tests/research/test_atlas_red_e1b_sol.py -ra

"$ATLAS_TEST_PYTHON" -m pytest \
  tests/research/test_atlas_red_e1b_panel.py \
  tests/v2/test_session014_s1.py tests/v2/test_session021_s6.py \
  tests/v2/test_session037_strategy_prefix.py \
  tests/v2/test_session041_s6_cohort.py tests/v2/test_session041_s6_action_contract.py \
  tests/data/test_prefix_and_archive.py -ra

"$ATLAS_TEST_PYTHON" -m pytest \
  tests/v2/test_session037_feature_prefix.py tests/v2/test_session034_causal_timing.py \
  tests/science/test_prefix_invariance_phase4.py tests/contract/test_v1_golden_baseline.py -ra

ruff check src tests/research docs/research/atlas-red-e1b
"$ATLAS_TEST_PYTHON" -m compileall -q src tests/research docs/research/atlas-red-e1b
uv lock --check --offline
uv pip check --python "$ATLAS_TEST_PYTHON"
git diff --check
git diff --exit-code d339d845a417e36b9a83c83b4f366e74fd207474 -- \
  src tests/v2 tests/data tests/contract uv.lock pyproject.toml requirements-lock.txt
sha256sum ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md \
  ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md \
  docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md
```

Run `sha256sum -c MANIFEST.sha256` **from `algorithms/`**. Run `sha256sum -c SOL_ARTIFACTS.sha256` from the repository root. The former's paths are relative to its directory; a root-directory invocation failed for that reason before the corrected check passed. No manifest file is regenerated wholesale.

The extra group returns **12 passed / 1 failed** because the V1 golden test cannot import Nautilus. The exact reviewed-parent checkout was separately run with the same existing venv:

```bash
"$ATLAS_TEST_PYTHON" -m pytest tests/contract/test_v1_golden_baseline.py -ra
```

It has the same missing-Nautilus failure. This is retained as BLOCKED BY ENVIRONMENT, not fixed, skipped or presented as a full-suite pass.

## Measured execution wrapper

The environment has no `/usr/bin/time`. Successful full runs used Python's standard resource measurement around the exact CLI arguments above:

```python
import json, resource, runpy, sys, time
sys.argv = ["independent_sol_reproduction.py", "--panel", panel_path,
            "--csv-cache", csv_cache, "--source-proof", source_proof,
            "--scratch", empty_scratch, "--outdir", output_directory]
started = time.monotonic()
try:
    runpy.run_path("docs/research/atlas-red-e1b/independent_sol_reproduction.py", run_name="__main__")
finally:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(json.dumps({"wall_seconds": time.monotonic() - started,
                      "user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime,
                      "max_rss_kib": usage.ru_maxrss}), file=sys.stderr)
```

Wall/RSS metadata are kept outside deterministic mathematical outputs. Development runs and failures are listed in execution evidence; no failed or superseded result is certified as the final reproduction. Run the two final commands with separate empty scratch directories; source caches can be shared read-only. No full panel or source ZIP/CSV is committed.
