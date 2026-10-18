# Evidence and bounded proof commands

Source checkout: exact `4f00fa8afca4ac15b1cbf64519adf1f684b0589f`; isolated branch `research/atlas-red-economic-feasibility-gate-2026-10-18`. New candidate requires independent review of its exact final remote SHA, returned at delivery. No follow-up campaign/session is launched.

Python3.12.14, pytest9.1.1, Ruff0.17.0, Linux cloud environment. Managed network policy was restricted/enforced/current; **only GitHub network work was used**. No market endpoint, stream, private/account source, paid provider, model generation/download, new outcome or holdout was accessed. No dependencies were installed. The supplied test PYTHONPATH is inherited from the prior environment; its NumPy2.3.5 differs from locked2.5.3. A local package compatibility check is not a full locked-environment qualification.

```bash
export PYTHONPATH=/opt/codex/runtimes/codex-primary-runtime/dependencies/python/lib/python3.12/site-packages
/workspace/.venvs/atlas/bin/python docs/research/atlas-red-economic-feasibility/inventory.py --ops-root /workspace/work --output docs/research/atlas-red-economic-feasibility/evidence/OFFLINE_INVENTORY.json
/workspace/.venvs/atlas/bin/python -m pytest tests/research/test_atlas_economic_feasibility.py -ra --junitxml=/workspace/work/economic-feasibility-new.xml
```

New native probes: **26passed/0failed/0skipped**,1.22s. No market model was fitted. The constructed scopes, bars, training-row rejection inputs and synthetic native watch/candidate controls are explicitly synthetic. They do not supply the genuine missing population, qualified action, cash or fills.

Selected existing guards, **30passed/0failed/0skipped**,7.36s:

```bash
/workspace/.venvs/atlas/bin/python -m pytest tests/research/test_atlas_native_trace.py::test_unavailable_inputs_preserve_native_or_adapter_reason tests/research/test_atlas_native_trace.py::test_native_coordinator_exact_watch_and_refusal_parity tests/research/test_atlas_native_trace.py::test_complete_synthetic_s2_native_candidate_is_identical tests/research/test_atlas_native_trace.py::test_frozen_deadline_and_actual_replay_separation tests/research/test_atlas_native_trace.py::test_native_clocks_deadline_and_complete_surface_accounting tests/research/test_atlas_native_trace.py::test_adapter_has_no_outcome_loader_or_substitute_numerical_strategy tests/v2/test_session037_active_history.py::test_exact_full_prefix_equivalence_beyond_two_tail_windows tests/v2/test_session037_active_history.py::test_serialized_restart_preserves_seeds_exactly_and_hash_is_immutable tests/v2/test_session037_active_history.py::test_reconstructed_market_and_forming_bars_never_enter_actual_state tests/v2/test_session037_active_history.py::test_work_and_wire_budgets_identity_and_authority_are_strict tests/v2/test_session037_active_history.py::test_gaps_are_retained_and_counted_without_manufacturing_bars_or_days tests/v2/test_session036_m15_origin_accounting.py::test_each_missing_prerequisite_retains_explicit_causal_missingness tests/v2/test_session036_m15_origin_accounting.py::test_boundary_is_inclusive_and_later_recovery_never_rescues_original_health tests/v2/test_session036_m15_origin_accounting.py::test_revision_isolation_nonfinal_reconstructed_future_and_absent_origins tests/p1/test_mean_models.py::test_future_immature_censored_diagnostic_and_incompatible_rows tests/p1/test_evidence_integrity.py::test_original_ledger_retains_unavailable_empty_and_missed_slots_without_backdating -ra --junitxml=/workspace/work/economic-feasibility-targeted.xml
```

Explicit dependency probes, **two actual failures**,0.54s:

```bash
/workspace/.venvs/atlas/bin/python -m pytest tests/v2/test_data_runtime.py::test_import_keeps_actual_receipt_and_separate_reconstructed_availability tests/contract/test_v1_golden_baseline.py::test_v1_golden_baseline_recomputes_from_frozen_code_and_fixtures -ra --junitxml=/workspace/work/economic-feasibility-environment.xml
```

Same two exact nodes rerun at clean parent `/workspace/work/atlas-causal-qualification`, HEAD4f00fa8: **two failures**,0.45s. First needsPyArrow for an existing archive write; second needsNautilus for a golden prerequisite. Reasons match exactly. No requirement was skipped, assertion weakened or dependency failure counted as pass. These failures are distinct from absent original market sources and unimplemented genuine financial provenance. Prior session's larger regression counts are historical records, not freshly rerun claims.

Disjoint current coverage: **58cases,56passed/2failed, zero skips**. This is not a full-suite, genuine complete-path, platform, deadline or economic certification.

## Inventory method and scope

`inventory.py` hashes13 selected existing source/decision/operator evidence files and reads only `artifact_type,count(*)` from six explicitly named research SQLite stores, using URI `mode=ro`. Full database hashes bind the observed files. It does not read account tables, private data or arbitrary live databases. The two retained blind-ML stores contain only6,173 `HistoricalPriceOriginDiagnosticV1` entries each; native stores contain the recorded zero-authority trace types. Duplicate runs do not increase independent observations.

The workspace discovery command below returned no capture journals/Arrow/Parquet paths. It inventories filenames only, not contents or private credentials. This is a bounded accessible-workspace check, not a global/provider absence assertion:

```bash
rg --files --hidden --no-ignore -g '!**/.git/**' -g '!**/.venvs/**' -g '!**/__pycache__/**' -g 'capture-*.bin' -g '*.arrow' -g '*.parquet' /workspace
```

`LOCAL_OPS_INVENTORY.json` records the snapshot, precise type counts and database hashes. `OFFLINE_INVENTORY.json` is portable for repository-file verification; missing local stores are explicitly `LOCAL_EVIDENCE_NOT_PRESENT` in a fresh clone. It makes no new checksum-archive acquisition or replay claim. Cached kline verification results are reused from exact prior sealed artifacts/manifests; no new bulk archive parse or one-hour replay was performed.

The inventory was repeated through the existing external `run_blind_research_stage.py` monotonic/resource wrapper. Outputs were byte-identical. Measured wrapper wall0.304s, peak processRSS23,032KiB. This measures an inventory, **not target-host ingestion/strategy/Agent B latency**. No meaningful new storage or4GB-host qualification was attempted.

`CAPABILITY_AND_TIMING.json` preserves exact native Binance capability statuses and selected existing public operator/target reports. The R9 record's public REST/WS subrecords were read; private/demo account records and credentials were not queried. Public report timestamps/latency counts are operator evidence, not a new independent network run. Raw-payload-retention false flags and rejected storage preflight are preserved. The complete native T+5 path remains UNVERIFIED.

## Static, integrity and authority checks

```bash
/workspace/.venvs/atlas/bin/ruff check src tests/research/test_atlas_economic_feasibility.py docs/research/atlas-red-economic-feasibility
/workspace/.venvs/atlas/bin/python -m compileall -q src tests/research/test_atlas_economic_feasibility.py docs/research/atlas-red-economic-feasibility
uv lock --check --offline
uv pip check --python /workspace/.venvs/atlas/bin/python
sha256sum -c docs/research/atlas-red-economic-feasibility/ARTIFACTS.sha256
git diff --check 4f00fa8afca4ac15b1cbf64519adf1f684b0589f
```

Also run `(cd algorithms && sha256sum -c MANIFEST.sha256)`. Verify all three original freeze SHA-256 identities, exact unchanged production/dependency/design/PDF and previous research bytes, append-only log prefixes and unchanged acceptance state columns. Scope is evidence references only. New external acceptance applies only to exact4f00fa8 causal correction; neither this writer nor the handoff self-accepts the economic verdict.

Development corrections are retained honestly in execution evidence: the owner-symbol resolver initially rejected a nonexistent trade-completeness class name; it was corrected to actual `collect_exact_trade_window_v1` before publication. An initial negative fixture changed a sealed universe without resealing its synthetic envelope; corrected before reaching the intended eligibility refusal. A capability test initially expected an underscore status instead of native `NOT ESTIMABLE`; it now preserves the actual canonical enum/value. No native source or semantic test oracle was changed.

`ECONOMIC_FEASIBILITY_VERDICT.json` binds inspected file/code hashes and ends with exactly one primary recommendation. Package inventory binds new code, tests, documents and evidence without self-hashing. Final publication verifies actual GitHub SHA, sole parent4f00fa8, accepted-design/P1 ancestry, complete diff/tree and clean worktree. No auto-merge, next session, source acquisition or economic experiment follows.
