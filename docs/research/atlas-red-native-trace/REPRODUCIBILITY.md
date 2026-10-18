# Reproduction, query and verification commands

Run from the exact final candidate reported at delivery. All market history in this package is already revealed development data. Reproduction is not a new holdout. No October 2025 onward acquisition/evaluation is authorized here.

Environment used: CPython 3.12.14; existing venv pytest9.1.1/Ruff0.17.0. No packages installed or locks changed. The provided NumPy2.3.5 differs from locked2.5.3; PyArrow and Nautilus are absent, as are qualified LightGBM/optional forecaster runtimes. Installed-package compatibility is not full lock realization or venue/host qualification.

## Replay with retained official caches

The cache preparation/recovery recipes are in the unchanged prior `atlas-red-e1b/SOL_COMMANDS.md` and `atlas-red-historical-blind-ml/REPRODUCIBILITY.md`. This session reused locally retained ZIPs/checksums/CSVs; it did not execute fresh downloads. A fresh environment without those exact verified inputs is **BLOCKED BY ENVIRONMENT**, not permission to change sources or manufacture evidence. The committed trace can still be audited without market caches.

Use a disposable output directory and new scratch DB; original source, protocol and code hashes must match. The loader rechecks only the selected source scope and passes cutoff-bounded values to the native adapter. It never fits models or reads outcome files.

```bash
ATLAS_TRACE=docs/research/atlas-red-native-trace
ATLAS_OLD_CSV=/workspace/work/atlas-e1b-trigger-censor-qualified-scratch
ATLAS_OLD_ZIP=/workspace/work/e1b-sol-official-zips
ATLAS_NEW_CACHE=/workspace/work/blind-ml-new-source-cache

python3 "$ATLAS_TRACE/replay.py" \
  --old-csv "$ATLAS_OLD_CSV" --old-zip "$ATLAS_OLD_ZIP" \
  --new-cache "$ATLAS_NEW_CACHE" \
  --outdir /workspace/work/native-trace-pinned-repeat \
  --db /workspace/work/native-trace-pinned-repeat.sqlite
```

The first published evidence output was generated with the same command, `--outdir docs/research/atlas-red-native-trace/evidence --db /workspace/work/native-trace-pinned.sqlite`. The original and fresh-repeat trace and seal are byte-identical. Variable actual publication/resource metadata are deliberately separate. Source hashes, complete source-row identity bindings, finite prefix first/terminal intervals and dependent native output refs are sealed. No future market/cash outcomes were generated before or after the seal.

## Separate post-seal audit and query

```bash
python3 "$ATLAS_TRACE/audit.py" \
  --evidence "$ATLAS_TRACE/evidence" \
  --db /workspace/work/native-trace-pinned.sqlite \
  --output /workspace/work/native-trace-audit-repeat.json

# A fresh clone can omit --db and audit all committed content identities,
# dependencies and calendar denominators without SQLite or market regeneration.
python3 "$ATLAS_TRACE/audit.py" --evidence "$ATLAS_TRACE/evidence"
```

The audit first verifies the exact sealed code and artifact hashes. When the ignored assembled TRACE.jsonl.gz is absent it atomically restores its exact bytes from three pinned transport pieces. Existing mismatched files or missing/corrupted pieces fail; they are not repaired from unverified sources. No decision records, market inputs or policy values are mutated. `--db` additionally checks the original indexed artifacts and genuine 2026 publication times; without it that publication check is explicitly NOT_CHECKED_NO_DB.

For a query, obtain any `decision_id` from `evidence/EXAMPLE_DECISION.json` or the streamed export:

```bash
python3 "$ATLAS_TRACE/audit.py" --evidence "$ATLAS_TRACE/evidence" \
  --db /workspace/work/native-trace-pinned.sqlite \
  --decision-id EXACT_DECISION_ID
```

The resulting slot references its source, code, feature/component observations and every strategy/economic/selection/management refusal. Use the existing read-only Ops `get_artifact(ref)` for referenced payloads, or resolve `artifact_ref` in the streaming compressed export. The export is not a new database owner.

## Actual focused tests

Final new scope **39 passed, zero failed/skipped**, including genuine retained-source comparison and full sealed-calendar audit. Another environment lacking source caches explicitly skips genuine-cache tests; do not report those as reproduced. The new synthetic P1 fixture executes the existing producer/evaluator, not forged eligible records.

```bash
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/research/test_atlas_native_trace.py \
  tests/p1/test_native_trace_bridge.py -ra
```

Impacted existing scope **164 passed / 15 failed / zero skips**, 180.46s:

```bash
PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages \
  /workspace/.venvs/atlas/bin/python -m pytest \
  tests/v2/test_session014_core.py tests/v2/test_session014_s1.py \
  tests/v2/test_session016_s2.py tests/v2/test_session021_s6.py \
  tests/v2/test_session037_strategy_prefix.py tests/v2/test_session037_feature_prefix.py \
  tests/v2/test_session041_strategy_surface.py tests/v2/test_session041_s6_cohort.py \
  tests/v2/test_session041_s6_action_contract.py tests/v2/test_session047_decision_brief.py \
  tests/p1/test_mean_evaluation.py tests/p1/test_mean_risk_cash.py \
  tests/p1/test_mean_selection.py tests/p1/test_evidence_integrity.py \
  tests/data/test_prefix_and_archive.py tests/contract/test_v1_golden_baseline.py -ra
```

All exact 15 failing node IDs in EXECUTION_EVIDENCE.json were independently rerun at clean reviewed parent f9de368 and failed identically in 2.09s: 14 missing-PyArrow archive/surface fixtures, one missing-Nautilus V1 golden case. No assertion weakening, skip, package substitute or false full-suite pass. Commands for parent reproduction are the same pytest prefix followed by `parent_failure_node_ids` from that record, run in a clean checkout of exact f9de368.

## Preservation and tooling

```bash
/workspace/.venvs/atlas/bin/ruff check src tests/research \
  tests/p1/test_native_trace_bridge.py docs/research/atlas-red-native-trace
python3 -m compileall -q src tests/research tests/p1/test_native_trace_bridge.py \
  docs/research/atlas-red-native-trace
uv lock --check --offline
uv pip check --python /workspace/.venvs/atlas/bin/python
sha256sum -c docs/research/atlas-red-native-trace/ARTIFACTS.sha256
git diff --check
git diff --exit-code f9de3686709123f2f9770a7d66ac52ca5450200a -- \
  src/atlas/v2/features src/atlas/v2/strategies src/atlas/v2/science \
  src/atlas/v2/risk.py src/atlas/v2/research src/atlas/v2/memory \
  algorithms/ATLAS_SYSTEM_DESIGN.md algorithms/ATLAS_ALGORITHM.pdf \
  pyproject.toml uv.lock requirements-lock.txt
```

From algorithms/, `sha256sum -c MANIFEST.sha256` checks all18 entries. Freeze hashes/Git identities, append-only log prefixes, unchanged acceptance state columns, research-only diff and no secrets are checked separately in execution evidence. The sole source addition is native_research_trace.py; the native owners and their policy bytes are unchanged.

Resource wrappers use monotonic/getrusage, not an unavailable /usr/bin/time. Measurements, command arguments, counts/failures and development repairs are recorded. A new 1MB per-artifact /64-row batch bound, 100MB output cap and 250MB per-replay-DB scratch check are integration limits, not accepted production retention/service contracts. Development copies are retired after their logs/hashes are retained; aggregate scratch/target-host qualification is not claimed. The SQLite ABORT probe is an injected transactional write failure, not physical disk-full testing. No minimum-host or sustained-runtime gate passes from these observations.
