# Commands and evidence boundaries

Source checkout: `f8d5d95f2492a9af959a4d48b0664233cf9aa123`; candidate branch `research/atlas-red-causal-replay-qualification-2026-10-18`. Exact final remote SHA is returned at delivery and must be independently reviewed. No auto-merge or dependent session is authorized.

## Environment and inputs

Python3.12.14, pytest9.1.1, Ruff0.17.0, Linux cloud worker. Interpreter `/workspace/.venvs/atlas/bin/python`. Tests/replay use the supplied NumPy2.3.5 via the following PYTHONPATH; the lock specifies2.5.3. No dependency or lock was changed and nothing was installed. PyArrow, Nautilus and qualified optional model runtimes are unavailable. Local installed-package compatibility is not locked-environment qualification.

```bash
export PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages
```

Required existing source caches, read only:

- `--old-csv /workspace/work/atlas-e1b-trigger-censor-qualified-scratch`
- `--old-zip /workspace/work/e1b-sol-official-zips`
- `--new-cache /workspace/work/blind-ml-new-source-cache`

Missing caches block regeneration; no network fallback or acquisition is authorized. The selected manifest verifies302 official ZIP/CHECKSUM/CSV objects and retains13 old404/210 old rejected records. It is a selected continuation of the unchanged historical manifest, not a new attempt log; rejected byte totals remain unknown. It pins H1Jan–July and M15June/July. The existing loader may parse the already revealed monthly CSV, then filters every native prefix before strategy access. October onward and future outcomes are not opened.

## Executed native replay and sealed audit

Primary command (fresh evidence directory/DB at execution; runner refuses an existing trace/DB):

```bash
/workspace/.venvs/atlas/bin/python /workspace/work/run_blind_research_stage.py docs/research/atlas-red-causal-replay/replay.py /workspace/work/causal-qualification-replay-wrapper.json --old-csv /workspace/work/atlas-e1b-trigger-censor-qualified-scratch --old-zip /workspace/work/e1b-sol-official-zips --new-cache /workspace/work/blind-ml-new-source-cache --outdir docs/research/atlas-red-causal-replay/evidence --db /workspace/work/causal-qualification-pinned.sqlite
```

The external wrapper only records monotonic wall time, child user CPU and process maxRSS using `runpy`/`resource`. The package's own `REPLAY_RESOURCES.json` also records these limits, actual publication spans and Ops metrics. For another run, call `replay.py` directly with fresh scratch `--outdir`/`--db`; the wrapper is not needed for mathematical reproducibility.

Fresh repeat: identical command/arguments except wrapper output `/workspace/work/causal-qualification-repeat-wrapper.json`, `--outdir /workspace/work/causal-qualification-repeat` and `--db /workspace/work/causal-qualification-repeat.sqlite`. Both successful runs' actual resources and byte comparison are in [repeat verification](evidence/REPEAT_VERIFICATION.json).

```bash
/workspace/.venvs/atlas/bin/python docs/research/atlas-red-causal-replay/audit.py --evidence docs/research/atlas-red-causal-replay/evidence --db /workspace/work/causal-qualification-pinned.sqlite --output docs/research/atlas-red-causal-replay/evidence/TRACE_AUDIT.json
/workspace/.venvs/atlas/bin/python docs/research/atlas-red-causal-replay/audit.py --evidence /workspace/work/causal-qualification-repeat --db /workspace/work/causal-qualification-repeat.sqlite --output /workspace/work/causal-qualification-repeat-audit.json
/workspace/.venvs/atlas/bin/python docs/research/atlas-red-causal-replay/compare.py --parent docs/research/atlas-red-native-trace/evidence --current docs/research/atlas-red-causal-replay/evidence --output docs/research/atlas-red-causal-replay/evidence/PARENT_COMPARISON.json
```

Primary audit was also measured through the same wrapper with `/workspace/work/causal-qualification-audit-wrapper.json`. Without a DB, the audit still verifies the full sealed content, code, native dependencies and calendar, but labels actual Ops publication `NOT_CHECKED_NO_DB`. Four pinned binary parts restore the ignored assembled gzip atomically and losslessly. Corrupt existing bytes, parts or code are refused. No full panel or new source download is necessary to read these artifacts.

The parent comparison intentionally reads old sealed bytes without claiming their old code hashes describe this new checkout. The prior foundation's `audit.py` correctly refuses changed feature source; test this negative explicitly. To rerun old mathematics, use exact f8d5d95. Historical package files, seals and source inventories are not rewritten. The active native-trace tests now restore transport parts on a fresh checkout and use the separately sealed continuation for current-code parity; an explicit test preserves the historical code-hash refusal. There is no hidden skip or relaxed numerical tolerance.

## Counterexamples and actual-system compatibility

`probe.py` is a pure synthetic availability counterexample, not a market replay. Executed with `PYTHONPATH=src` from clean parent `/workspace/work/atlas-native-trace` and current `/workspace/work/atlas-causal-qualification`:

```bash
/workspace/.venvs/atlas/bin/python /workspace/work/atlas-causal-qualification/docs/research/atlas-red-causal-replay/probe.py
```

Outputs: [parent](evidence/PARENT_REPRODUCTION.json), [corrected](evidence/CORRECTED_REPRODUCTION.json). The synthetic full ACTUAL_SYSTEM artifacts with/without trades were generated at clean f8d5d95 using the original `v2.test_session014_core.bar` fixture. Both entire payloads are pinned in `tests/fixtures/causal-replay/actual_system_feature_v1.json`; current tests compare exact canonical bytes, including envelope IDs, units, missingness and refs. The probe uses production feature calculations, not copied strategy formulas.

## Tests

Final focused command, **74passed/0failed/0skipped**,16.67s:

```bash
/workspace/.venvs/atlas/bin/python -m pytest tests/research/test_atlas_causal_replay.py tests/research/test_atlas_native_trace.py tests/p1/test_native_trace_bridge.py -ra --junitxml=/workspace/work/causal-qualification-focused.xml
```

Existing regression command, **221passed/17failed/0skipped**,187.32s:

```bash
/workspace/.venvs/atlas/bin/python -m pytest tests/v2/test_session014_core.py tests/v2/test_session014_s1.py tests/v2/test_session016_s2.py tests/v2/test_session021_s6.py tests/v2/test_session037_strategy_prefix.py tests/v2/test_session037_feature_prefix.py tests/v2/test_session041_strategy_surface.py tests/v2/test_session041_s6_cohort.py tests/v2/test_session041_s6_action_contract.py tests/v2/test_session047_decision_brief.py tests/p1/test_mean_evaluation.py tests/p1/test_mean_risk_cash.py tests/p1/test_mean_selection.py tests/p1/test_evidence_integrity.py tests/data/test_prefix_and_archive.py tests/contract/test_v1_golden_baseline.py tests/v2/test_r1_surgical_correctness.py tests/v2/test_session041_trade_location.py tests/v2/test_data_runtime.py tests/v2/test_session021_data_s3.py -ra --junitxml=/workspace/work/causal-qualification-regressions.xml
```

All17 exact failing node IDs were rerun in the clean parent worktree. Same17 failures,1.46s pytest:16 `ModuleNotFoundError: pyarrow`, one missing `nautilus_trader` golden prerequisite. Exact node IDs, complete argv, failure summaries and XML/log hashes are in `EXECUTION_EVIDENCE.json`; no assertion was weakened or environment failure recast as a pass.

The final disjoint current test scope is312 cases:295passed/17failed, zero skips. It is **not the whole repository suite**, an economic validation, a hardware certification or a production integration pass. Fresh environments without retained genuine caches may explicitly skip the pre-existing cache-dependent test; no such skip occurred here. The sealed trace tests remain reproducible from Git alone.

Development runs retained in execution evidence: initial new scope20passed/one invalid synthetic wrong-revision fixture failed before the intended guard (fixed by constructing a valid alternate-key raw record); first combined scope81passed/two missing ignored assembled-gzip failures (portable lossless restoration/current sealed fixture added); intermediate72passed, then final74passed. Lint initially found one dict style and one import-order issue, corrected. These were engineering iterations, not outcome-conditioned algorithm changes or holdout attempts.

Other executed checks:

```bash
/workspace/.venvs/atlas/bin/ruff check src tests/research/test_atlas_native_trace.py tests/research/test_atlas_causal_replay.py tests/p1/test_native_trace_bridge.py docs/research/atlas-red-causal-replay
/workspace/.venvs/atlas/bin/python -m compileall -q src tests docs/research/atlas-red-causal-replay
uv lock --check --offline
uv pip check --python /workspace/.venvs/atlas/bin/python
sha256sum -c docs/research/atlas-red-causal-replay/ARTIFACTS.sha256
git diff --check f8d5d95f2492a9af959a4d48b0664233cf9aa123
```

Also `(cd algorithms && sha256sum -c MANIFEST.sha256)`, all freeze hashes, pre-existing production/design/lock preservation, append-only log prefixes, unchanged acceptance state columns, scoped secret-pattern review and exact remote tree/parent/ancestry/diff checks. A real SQLite ABORT trigger additionally measured atomic rollback with zero inserted rows,0.897ms, explicitly **not physical disk-full**. Production safety-path/80–200ms service, process-tree,72h and minimum-host gates are not certified.

## Identities and stopping gate

Trace SHA-256: `eea3aca6eb2029af641214cbd6516bdd6333b77d19edc4f873397760b243b227`.

Protocol SHA-256: `42a795d46da7f7459a785430a4f6a556c5fa97c33e3c6afee4aeeb01021c8169`.

Native/source code ref: `b3d3c30a6b151507085646314113231a2a92da468799007b426383574b0da59a`.

The package manifest/inventory supplies all exact file/seal/part identities. Example decision: `546bb5b930d02bd3cb997d723bc512268ad44067894db4e41a86c9cf1b4a6616`. Its complete record links source/code, native feature/component references, all strategies, unavailable economics/selection/management and missed original deadline. Review the actual final commit and its complete f8d5d95→candidate diff. No automatic acceptance, next session, acquisition, outcomes, trading or capital authority follows.
