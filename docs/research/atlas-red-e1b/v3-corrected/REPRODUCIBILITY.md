# Reproduce the corrected dataset and registered price study

All market data used here came from the existing approved ZIP/CSV caches. New source downloads and market HTTP requests: **zero**. Source identities and old/unknown acquisition facts are preserved. No paid/private exchange or model-generation access is used. Full panels and temporary output belong outside Git.

## Environment and immutable source checkout

Recorded: CPython 3.12.14, existing NumPy 2.3.5, pytest 9.1.1, Ruff 0.17.0, Linux. The existing research venv lacks NumPy; executed tests used its pytest with the managed runtime NumPy directory on PYTHONPATH. No dependency/lock file changed. `uv.lock` pins NumPy 2.5.3 and Nautilus is absent: complete locked-runtime qualification remains UNVERIFIED / BLOCKED. Installed-package compatibility is not complete dependency realization.

Use a producer worktree at **22913d753ef9e98536f56381e641365ed4021a71** for the build and package commands. This preserves the producer commit recorded by `package` and reproduces the dataset-manifest bytes; running that metadata writer from an arbitrary different HEAD produces a different commit identity. The final candidate contains the unchanged producer bytes, the study code, committed A selection and evidence. No P1 branch merge/cherry-pick is needed.

Path aliases below denote the actual external caches/build directories; substitute your own paths. Exact executed paths, wrappers, counts, failures and resource observations are recorded in `EXECUTION_EVIDENCE.json`. `RESEARCH_VERSION.json` is the authoritative v3 entry manifest. The dataset summarizer preserves legacy `full_rebuild_command` / `compact_sample_command` strings for the old root layout; **use the versioned commands here**, not those old output destinations. Do not overwrite the historical root data/evidence files.

```bash
ATLAS_SOURCE_REPO=/path/to/producer-worktree-at-22913d7
ATLAS_FINAL_REPO=/path/to/final-research-candidate
ATLAS_CSV_CACHE=/path/to/existing-verified-csvs
ATLAS_ZIP_CACHE=/path/to/existing-official-zip-checksum-cache
ATLAS_INPUTS=/path/to/local-manifest-inputs
ATLAS_BUILD=/path/to/empty-corrected-output
ATLAS_REPEAT=/path/to/another-empty-corrected-output
ATLAS_VALIDATION_SCRATCH=/path/to/empty-independent-scratch
ATLAS_VALIDATION_OUTPUT=/path/to/independent-output
ATLAS_STUDY_OUTPUT=/path/to/price-study-replay
```

The executed source worktree was `/workspace/work/atlas-e1b-defects-synergy`, with the two existing caches `/workspace/work/atlas-e1b-trigger-censor-qualified-scratch` and `/workspace/work/e1b-sol-official-zips`. Source manifest/proof are the unchanged reviewed files. If the caches are absent, the approved exact-source archive recipe can restore that set under separate source-access accounting; no extra months/universe are needed. Cache restoration/fresh online rebuilding was **not rerun here**. A substituted panel or mismatched bytes must stop at the pin gate.

## Checkpoint A: cached sources, producer and repeat

Run in `$ATLAS_SOURCE_REPO` at source commit 22913d7:

```bash
python3 docs/research/atlas-red-e1b/corrected_research.py prepare \
  --zip-cache "$ATLAS_ZIP_CACHE" --csv-cache "$ATLAS_CSV_CACHE" --scratch "$ATLAS_INPUTS"

python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch "$ATLAS_CSV_CACHE" --outdir "$ATLAS_BUILD" \
  --offline-manifests "$ATLAS_INPUTS/H1.jsonl" "$ATLAS_INPUTS/M15.jsonl" \
  --rejected-attempt "$ATLAS_INPUTS/REJECTED.jsonl"

python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch "$ATLAS_CSV_CACHE" --outdir "$ATLAS_REPEAT" \
  --offline-manifests "$ATLAS_INPUTS/H1.jsonl" "$ATLAS_INPUTS/M15.jsonl" \
  --rejected-attempt "$ATLAS_INPUTS/REJECTED.jsonl"

cmp "$ATLAS_BUILD/DIAGNOSTIC_FEATURE_PANEL.csv.gz" "$ATLAS_REPEAT/DIAGNOSTIC_FEATURE_PANEL.csv.gz"
cmp "$ATLAS_BUILD/ASOF_COVERAGE.csv.gz" "$ATLAS_REPEAT/ASOF_COVERAGE.csv.gz"
cmp "$ATLAS_BUILD/SOURCE_MANIFEST.jsonl" "$ATLAS_REPEAT/SOURCE_MANIFEST.jsonl"

python3 docs/research/atlas-red-e1b/corrected_research.py package --builddir "$ATLAS_BUILD"
```

Full panel hash must be `811ef7c972c69d1c6d26f9a8518e388fc64a5774ee801c141d45863959e0bce4`; sample `558a3c650383cd18f038dc4cfc63dc6ac7e9cb9373d4d256a8d023ff0a2287aa`; source/coverage unchanged. The existing builder peaked near 2.85 GiB RSS; avoid parallel full builds on modest machines. Repeats use cached inputs. The independent validator streams and peaked near 260 MiB.

Run the following from `$ATLAS_FINAL_REPO`; it reads the checked-in v3 pins/sample/manifest and the external new full panel:

```bash
python3 docs/research/atlas-red-e1b/independent_sol_reproduction.py \
  --panel "$ATLAS_BUILD/DIAGNOSTIC_FEATURE_PANEL.csv.gz" --csv-cache "$ATLAS_CSV_CACHE" \
  --source-proof docs/research/atlas-red-e1b/SOL_SOURCE_CHECKS.jsonl \
  --artifact-config docs/research/atlas-red-e1b/v3-corrected/VALIDATION_INPUTS.json \
  --scratch "$ATLAS_VALIDATION_SCRATCH" --outdir "$ATLAS_VALIDATION_OUTPUT"

python3 docs/research/atlas-red-e1b/corrected_research.py compare \
  --old /path/to/reviewed-68ee994b-panel/DIAGNOSTIC_FEATURE_PANEL.csv.gz \
  --new "$ATLAS_BUILD/DIAGNOSTIC_FEATURE_PANEL.csv.gz"
```

The comparison utility writes the versioned diff record; use a separate disposable checkout for a review replay if the review must leave the candidate clean. Original panel is optional for reproducing the corrected mathematics, but required to rerun the complete old/new audit. Its exact SHA is preserved in the old manifest and new diff. Primary, anchor, physical-source, coverage and S6 mismatches must be zero before the study. The old secondary observed-history diagnostic intentionally remains different and is explained in the method/report.

## Frozen feature population, A choice, then B

```bash
mkdir -p "$ATLAS_STUDY_OUTPUT"

python3 docs/research/atlas-red-e1b/study_s1_s6.py extract \
  --panel "$ATLAS_BUILD/DIAGNOSTIC_FEATURE_PANEL.csv.gz" \
  --validation "$ATLAS_VALIDATION_OUTPUT/SOL_REPRODUCTION_RESULTS.json" \
  --destination "$ATLAS_STUDY_OUTPUT/FIRST_EVENT_FEATURES.csv.gz"

python3 docs/research/atlas-red-e1b/study_s1_s6.py outcomes \
  --features "$ATLAS_STUDY_OUTPUT/FIRST_EVENT_FEATURES.csv.gz" \
  --csv-cache "$ATLAS_CSV_CACHE" --window A --outdir "$ATLAS_STUDY_OUTPUT"

cmp "$ATLAS_STUDY_OUTPUT/A_RULE_SELECTION.json" docs/research/atlas-red-e1b/v3-corrected/A_RULE_SELECTION.json

python3 docs/research/atlas-red-e1b/study_s1_s6.py outcomes \
  --features "$ATLAS_STUDY_OUTPUT/FIRST_EVENT_FEATURES.csv.gz" \
  --csv-cache "$ATLAS_CSV_CACHE" --window B --outdir "$ATLAS_STUDY_OUTPUT" \
  --selection-file docs/research/atlas-red-e1b/v3-corrected/A_RULE_SELECTION.json \
  --selection-sha 852bb522951bce03a54f80e120f2c20d218fb8831c6414e694559e188caf444f

python3 docs/research/atlas-red-e1b/summarize_synergy_evidence.py \
  --csv-cache "$ATLAS_CSV_CACHE" --directory "$ATLAS_STUDY_OUTPUT"
```

For the last command, copy the byte-identical A selection from the replay output if already present (the A command creates it); do not modify it. The B utility reads the original committed selection for its ancestry/hash check. Actual first execution froze A at d1bd9f38b2dd2a8784f1f2bf5e007ba7d7490799 before any B outcomes. Both initial and repeat B runs also verified unchanged study-code bytes. A/B repeat ledger/result/selection hashes are in execution evidence; no new selection is made on B. Whole-path independent price checks cover 576 fixed first/middle/last symbol/window/horizon cases; no outcome-value sampling is used.

## Tests and other checks

Actual final scoped run: **161 passed, 1 failed, zero skips**, 44.55s pytest time. Failure: missing `nautilus_trader` in V1 golden, also reproduced at exact reviewed c29dfc2c. This is not a full repository suite pass. Earlier red/green and individual scope invocations are retained in execution evidence.

```bash
ATLAS_SOL_CSV_CACHE="$ATLAS_CSV_CACHE" \
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/research/test_atlas_red_e1b_panel.py tests/research/test_atlas_red_e1b_corrections.py \
  tests/research/test_atlas_red_e1b_sol.py tests/research/test_atlas_red_e1b_study.py \
  tests/v2/test_session014_s1.py tests/v2/test_session021_s6.py \
  tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_cohort.py \
  tests/v2/test_session041_s6_action_contract.py tests/data/test_prefix_and_archive.py \
  tests/v2/test_session037_feature_prefix.py tests/v2/test_session034_causal_timing.py \
  tests/science/test_prefix_invariance_phase4.py tests/contract/test_v1_golden_baseline.py -ra

/workspace/.venvs/atlas/bin/ruff check src tests/research docs/research/atlas-red-e1b
python3 -m compileall -q src tests/research docs/research/atlas-red-e1b
uv lock --check --offline
uv pip check --python /workspace/.venvs/atlas/bin/python
git diff --check
git diff --exit-code c29dfc2c2ae6cb903b53f72fdf2d457c9a5bb32d -- src uv.lock pyproject.toml requirements-lock.txt
sha256sum -c docs/research/atlas-red-e1b/v3-corrected/ARTIFACTS.sha256
```

Run `sha256sum -c MANIFEST.sha256` from `algorithms/`. The old `SOL_ARTIFACTS.sha256` belongs to exact c29dfc2c; verify it in that unchanged checkout, not against newly corrected producer/validator code. All three freeze hashes/Git objects and unchanged accepted algorithm/PDF are checked separately. Wall/RSS were measured via Python `time.monotonic` / `resource.getrusage`, not unavailable `/usr/bin/time`. All scripted stages are read-only with respect to production authority; generated research files only.
