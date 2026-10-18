# Reproduction commands and exact scope

Run from the repository root at the final candidate. The scientific `.py` files and tests are unchanged from pre-outcome seal **94dfe762cc11c58b394fcfa057f85a943606a049**. The protocol was published earlier at **9fa877b5e137d1e32be447336ed54a5f1d20d301**. Keep those commits available for `git show`; use a full clone/fetch rather than a depth-one final snapshot without its seal ancestor. No auto-merge or qualification is implied.

Environment actually used: CPython 3.12.14, provided NumPy 2.3.5, existing pytest9.1.1/Ruff0.17.0 venv. No packages installed or locks edited. Locked NumPy2.5.3 differs; LightGBM4.7.0/Nautilus/Torch/Transformers are absent. Installed 17-package compatibility is not complete locked-runtime qualification. Actual paths/wrappers/logs/resources and development failures are in EXECUTION_EVIDENCE.json.

## Sources and caches

```bash
ATLAS_RESEARCH=docs/research/atlas-red-historical-blind-ml
ATLAS_OLD_CSV=/workspace/work/atlas-e1b-trigger-censor-qualified-scratch
ATLAS_OLD_ZIP=/workspace/work/e1b-sol-official-zips
ATLAS_NEW_CACHE=/workspace/work/blind-ml-new-source-cache
ATLAS_REPEAT=/path/to/empty-replay-output
ATLAS_OPS=/path/to/empty-replay-ops.sqlite
```

Old caches contain the exact previously verified 757 official objects/CSVs. Their prior source/proof manifests and compact development outcome ledgers are committed. If absent, restoring the same official objects requires the earlier source-proof recipe in `docs/research/atlas-red-e1b/SOL_COMMANDS.md`; do not substitute a different cache/panel, expand the cohort or claim a fresh old-source rebuild was executed here. This session rechecked existing bindings and did not repeat those downloads or regenerate the full E1B panel.

The old-source cache recheck actually executed:

```bash
python3 docs/research/atlas-red-e1b/corrected_research.py prepare \
  --zip-cache "$ATLAS_OLD_ZIP" --csv-cache "$ATLAS_OLD_CSV" \
  --scratch /workspace/work/blind-ml-inputs
```

Use a disposable checkout for replay commands that write committed evidence paths. The old helper regenerates identical LOCAL_SOURCE_REUSE bytes; no historical artifact is reinterpreted. The new acquisition's exact 210-object URL selection is fixed by the protocol. Cached ZIPs/checksums/404 receipts live outside Git; only manifests/proofs are published.

```bash
# With existing cache: no market network requests, exact source-manifest equality.
python3 "$ATLAS_RESEARCH/acquire_sources.py" --cache "$ATLAS_NEW_CACHE" --offline

# Cold recovery of ONLY the registered new quarter, if needed in a fresh clone:
# python3 "$ATLAS_RESEARCH/acquire_sources.py" --cache "$ATLAS_NEW_CACHE"
```

Cold recovery has variable acquisition timestamps/timing/order and may encounter changed remote availability. It must reproduce SOURCE_OBJECTS.jsonl's stable market identities or STOP at the source gate. Preserve original ACQUISITION.jsonl operator evidence; use a disposable checkout and compare stable identities before retaining a replay. Historical 404 does not establish point-in-time trading status.

## Model and decision replay

These commands reproduce inspected development features, model parameters and sealed decisions, not a new study or a test-data fit. They write model/development artifacts into the research directory; use a disposable checkout and compare exact bytes. TRAINING_RESOURCES/REPLAY_RESOURCES contain actual variable processing times and are not expected byte-identical.

```bash
python3 "$ATLAS_RESEARCH/diagnostic_models.py" \
  --old-cache "$ATLAS_OLD_CSV" --new-cache "$ATLAS_NEW_CACHE"

python3 "$ATLAS_RESEARCH/decisions.py" \
  --old-cache "$ATLAS_OLD_CSV" --new-cache "$ATLAS_NEW_CACHE" \
  --outdir "$ATLAS_REPEAT" --ops-path "$ATLAS_OPS"
```

Do not refit using revealed test rows. The trainer accepts only prior A/B development ledgers under hard temporal/provenance/feature-order checks. It never constructs MeanTrainingRowV1 cash records. The original model/development byte hashes must remain:

```text
DEVELOPMENT_ROWS.csv.gz  4f91a724e0d4d65f7acd5c421d3c8ee7c7ef1ef269e492b0d8ceaa8c2a27224f
PRICE_MODELS.json       129dcb44ce9ff84d01ba23c723e224f1a5d5e78967894de23baf1e529daedc8d
DECISIONS.csv.gz        29638920e0b61c82ba136b52bae5668e7c327cf25b09b2f4ffad3c3492c30d7b
CALENDAR.csv.gz         3db6184fa97ed7fc53b6e3ff87e6918096384367a32930ccb0221c3021df4648
```

The 14,788,948-byte compressed calendar is losslessly committed in five `calendar-parts/` pieces of at most 3,000,000 bytes because a single-blob connector publication failed. DECISION_SEAL binds each piece and the exact combined file. `verify_seal` assembles an absent calendar atomically and refuses an existing mismatched calendar. The generated full name is ignored by Git; this is transport packaging, not missing calendar evidence or scientific regeneration. The pure decision ledger and full calendar were repeated byte-identically before outcome access.

## Separate outcome stage

This verifies exact Git-sealed source/code/model/decision bytes before reading future paths. No decision callback, fit, model choice, threshold argument, provider or action authority exists in this interface. Complete source/path/maturity checks and an independent direct-CSV Decimal oracle run as part of scoring.

```bash
python3 "$ATLAS_RESEARCH/evaluate_outcomes.py" \
  --decision-commit 94dfe762cc11c58b394fcfa057f85a943606a049 \
  --old-cache "$ATLAS_OLD_CSV" --new-cache "$ATLAS_NEW_CACHE" \
  --outdir "$ATLAS_REPEAT"
```

Expected output identities:

```text
OUTCOMES.csv.gz  3c7a54cd70c399e38a86013b60392293d68a88ce3023e6764ad2b16ce49086fc
RESULTS.json     29a0e4d61b27e319540ec933ed178a6a66d006136d46281003f01c8281ecaab8
```

The complete initial and repeat results/ledgers match. Outcome resource timestamps/elapsed time vary. A Git seal proves publication order, not pristine human blindness. Test sources are now revealed and spent; reproduction is not a new holdout.

## Actual tests

Final new group: **51 passed / zero skips**, including genuine cached source comparisons. Real-source tests explicitly skip if the required cache is missing in another environment; none skipped here.

```bash
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/research/test_atlas_historical_blind_ml.py -ra
```

Impacted pre-seal group, executed once as a broad scope: **264 passed / three failed / zero skips**, 201.48s. It overlaps the new tests; do not sum counts as independent coverage. Later focused transport/import/source-oracle tests are in the final 51-case group. No production code changed between scopes.

```bash
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
ATLAS_SOL_CSV_CACHE="$ATLAS_OLD_CSV" \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/research/test_atlas_historical_blind_ml.py \
  tests/research/test_atlas_red_e1b_panel.py tests/research/test_atlas_red_e1b_corrections.py \
  tests/research/test_atlas_red_e1b_study.py \
  tests/p1/test_mean_models.py tests/p1/test_mean_risk_cash.py \
  tests/p1/test_mean_evaluation.py tests/p1/test_mean_research_management.py \
  tests/p1/test_mean_selection.py tests/p1/test_evidence_integrity.py \
  tests/v2/test_session014_s1.py tests/v2/test_session021_s6.py \
  tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_cohort.py \
  tests/v2/test_session041_s6_action_contract.py tests/data/test_prefix_and_archive.py \
  tests/contract/test_v1_golden_baseline.py -ra
```

Exact same three environment failures were reproduced at reviewed source **918d233b** using:

```bash
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/p1/test_mean_models.py::test_actual_lightgbm_l2_fit_compact_inference_and_serialization \
  tests/p1/test_mean_models.py::test_m1_nonlinear_prediction_matches_native_booster \
  tests/contract/test_v1_golden_baseline.py -ra
```

No assertion edits, installed substitute or silent skip conceal those failures. The new test's 50-digit real-source price oracle is checked on already inspected April development data before the seal; scoring then checks 270 fixed genuine test cases ×3 numeric fields. No selective group is presented as a full repository pass.

## Preservation and resource checks

```bash
/workspace/.venvs/atlas/bin/ruff check src tests/research docs/research/atlas-red-historical-blind-ml
python3 -m compileall -q src tests/research docs/research/atlas-red-historical-blind-ml
uv lock --check --offline
uv pip check --python /workspace/.venvs/atlas/bin/python
sha256sum -c docs/research/atlas-red-historical-blind-ml/ARTIFACTS.sha256
git diff --check
git diff --exit-code 918d233b2501319bc031d679b3d9f5647ae13156 -- \
  src pyproject.toml uv.lock requirements-lock.txt \
  algorithms/ATLAS_SYSTEM_DESIGN.md algorithms/ATLAS_ALGORITHM.pdf
git diff --exit-code 94dfe762cc11c58b394fcfa057f85a943606a049 -- \
  'docs/research/atlas-red-historical-blind-ml/*.py' tests/research/test_atlas_historical_blind_ml.py
```

From `algorithms/`, run `sha256sum -c MANIFEST.sha256`; all 18 entries must pass. Freeze SHA/Git objects and old evidence pins are independently checked; new record/register pointers do not promote requirement states. Inventory excludes its own hash and the ignored assembled calendar, whose exact hash and all committed pieces are in the seal/entry manifest.

Measured using Python monotonic/getrusage wrappers (no `/usr/bin/time`): final numerical development/fit about 38.5s; default decision replay 34.1s / 695,524 KiB process peak RSS; outcome scoring 11.35s / 144,656 KiB. Repeat measures are in execution evidence. These are this environment's observations, not 4GB Celeron/Windows production qualification or historical latency. No full-panel regeneration, model-weight downloads, hyperparameter search or paid inference occurred.
