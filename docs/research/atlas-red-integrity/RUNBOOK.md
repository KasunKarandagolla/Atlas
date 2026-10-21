# Next-test runbook — gated, no campaign started here

Status: CORRECTION_TEST_GATE. This procedure is executable preparation, not authority to weaken missing original-source, execution-model, hardware or model gates. The current collector is a limited public research probe; it does not automatically supply the complete native original-time chain.

## 1. Exact source and independent review

Implementation source SHA: `56bcbcb54aab6043832f35ab97c4098e721d174f` (filled before final publication). Starting research SHA: `7ee561210180f9e0ffa3297a520d35e4857bfe98`. Use the delivered final review tip, verify its ancestry and verify the source hashes in `evidence/source_build.json`. Independent exact-SHA acceptance is required; this candidate does not accept itself. OBSERVE, zero allocation/paid budget, CAPITAL NO-GO.

```bash
git fetch origin fix/atlas-red-economic-evidence-integrity-ml-2026-10-21
git worktree add /new/empty/atlas-test-checkout \
  origin/fix/atlas-red-economic-evidence-integrity-ml-2026-10-21
cd /new/empty/atlas-test-checkout
git rev-parse HEAD
```

## 2. Linux numerical environment and storage

Use separate new owner-approved directories. Never repurpose an existing data directory as a disposable mountpoint. Replace `/data` with actual persistent host paths and inventory all other temporary bytes. The default 3 GB temporary volume plus external temporary reservation and publication fragments must remain below 5,000,000,000 bytes including block rounding.

```bash
export ATLAS_PERMANENT_ROOT=/data/atlas-permanent
export ATLAS_TEMP_ROOT=/data/atlas-empty-temp-mount
export ATLAS_TEMP_CAPACITY_BYTES=3000000000
export ATLAS_EXTERNAL_TEMPORARY_BYTES=1000000000  # replace with measured conservative inventory
mkdir -p "$ATLAS_PERMANENT_ROOT" "$ATLAS_TEMP_ROOT"
python3.12 -m venv "$ATLAS_PERMANENT_ROOT/env/numerical"
export ATLAS_NUMERIC_PY="$ATLAS_PERMANENT_ROOT/env/numerical/bin/python"
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" -m pip install \
  --require-hashes --no-cache-dir -r configs/research/numerical-linux-py312.lock
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_storage_check.py
```

A Linux `unshare`/tmpfs pass does not qualify the 4 GB host's RAM. Windows requires its own bounded-volume and supported package/runtime qualification; the Linux recipe must not be presented as a Windows pass.

## 3. Automated model installation and validation

```bash
scripts/atlas_red_bounded_run.sh scripts/atlas_red_setup_models.sh --imports-only
scripts/atlas_red_bounded_run.sh scripts/atlas_red_setup_models.sh
```

The second command first resolves a durable revision lock for exactly TiRex-2, Chronos-2-small, Kronos-mini and the 2k tokenizer, because the cloud session could not access Hugging Face metadata and no prior pins existed. It then uses those exact revisions, verifies source/final hashes, keeps weights permanently, and invokes the actual model APIs serially on causal synthetic ABI inputs. Runtime locks are already pinned with package hashes; Kronos source is pinned to `67b630e67f6a18c9e9be918d9b4337c960db1e9a`. Future runs reuse the local revision lock and reject mismatches.

Checkpoint metadata/download/inference remains unverified here. A denied network destination, unsupported loader, incomplete license/weight manifest, output-shape/units failure, or host resource failure is a named gate. Do not substitute a larger Chronos model, a different Kronos tokenizer, a chat model or paid endpoint. Inspect `<permanent>/checkpoint-revisions.json` and each `*-smoke.json`; imports alone do not pass inference. Provisioning must not execute on ten-second observation or protection paths.

## 4. Preserve and migrate the existing database

Stop the original collector cleanly first. Supply its genuine path; it is absent from this cloud checkout. Do not regenerate the past by fetching new data. The destination must be new.

```bash
export ATLAS_ORIGINAL_DB=/actual/original/research.sqlite
export ATLAS_REPLAY_ROOT="$ATLAS_PERMANENT_ROOT/recomputed-2026-10-21"
mkdir -p "$ATLAS_REPLAY_ROOT/data"
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_recompute.py \
  --source "$ATLAS_ORIGINAL_DB" \
  --destination "$ATLAS_REPLAY_ROOT/data/research.sqlite" \
  --evaluated-at-ns ACTUAL_RETAINED_EVALUATION_CUTOFF_NS
```

Replace the last value with the true permitted evaluation time; do not invent it. SQLite's read-only source backup includes committed WAL data. It retains old outcomes and records additive corrected revisions only where supported. Unqualified legacy actions are excluded. Compare raw receipt IDs/payload hashes/times and counts with the source, `integrity_check`, and the preserved original reports. Keep the original database/backing image; no deletion is needed.

## 5. Coverage and reconstructed diagnostics

```bash
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_full_cycle_evaluate.py \
  --workspace "$ATLAS_REPLAY_ROOT" --max-closes-per-symbol 48
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_full_cycle_actions.py \
  --workspace "$ATLAS_REPLAY_ROOT"
```

These commands retain all S1–S8/Path B roles and explicit refusals. The action checker refuses retrospective applicability-to-prospective promotion. Missing source health, original universe/product revision, exact trades, derivatives/calendar/pair/action contracts remain visible. Broad proxy ranking is never canonical discovery. No completed retrospective diagnostic increases the prospective opportunity denominator.

## 6. Future public collector entrypoint — separate campaign authorization

Do not run this command in the correction session. After independent acceptance and campaign authorization, the existing bounded credential-free collector can retain public receipts:

```bash
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_full_cycle_collect.py \
  --workspace "$ATLAS_PERMANENT_ROOT/research" --temporary-root "$ATLAS_TEMP_ROOT" \
  --duration-minutes 60 --cheap-interval-seconds 10 \
  --deep-cohort-size 24 --rotation-step 7 --universe-symbols 140 \
  --external-baseline-bytes "$ATLAS_EXTERNAL_TEMPORARY_BYTES"
```

Deep-cohort/universe counts are sampling/resource parameters, not a fixed canonical trading universe. This probe does not pass the existing G1 ten-second/target-host qualification. Missing native original-time source contracts must be supplied and verified before calling the result a complete prospective evidence campaign. No private/authenticated exchange access, orders or paid inference.

## 7. Native actions, lifecycle and label maturation

The authoritative S1/S2 producers remain `S1ShadowCoordinator` and `S2ShadowCoordinator`; retain their original watches/latches, native cutoffs, feature/universe/source-health references and deadlines. `seal_native_candidate` checks exact policy/product and binds a virtual research plan. `persist_native_feature_context` retains the exact original feature artifact. `persist_modeled_outcomes` binds future paths and costs to the original action and appends versioned outcomes. These are library seams, not evidence that the public probe already invokes the full native workflow.

Use the supplied connected native fixture to verify this seam on the exact build:

```bash
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" -m pytest -q -o addopts='' \
  -p no:cacheprovider tests/v2/test_full_cycle_integrity.py tests/v2/test_atlas_red_full_cycle.py
```

Original observation qualification and modeled execution qualification are separate. Supply actual funding settlement records and supported coverage; a latest funding rate is insufficient. Gap/stop ordering, queue/no-fill/partial-fill uncertainty, missing future data or unvalued residual exposure censors point labels. A successful modeled stop proves no native protection. The recomputation CLI never upgrades a retrospective action or manufactures missing original receipts.

## 8. ML, reports and export

```bash
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_full_cycle_ml.py \
  --workspace "$ATLAS_REPLAY_ROOT" --model-cache "$ATLAS_PERMANENT_ROOT/models" \
  --output "$ATLAS_PERMANENT_ROOT/ml-report.json"
scripts/atlas_red_bounded_run.sh "$ATLAS_NUMERIC_PY" scripts/atlas_red_full_cycle_report.py \
  --workspace "$ATLAS_REPLAY_ROOT" --output "$ATLAS_PERMANENT_ROOT/economic-report.json"
```

The qualified view excludes legacy/reconstructed/immature labels; future features cannot join by a fabricated cycle-name timestamp. Train/validation/test uses purge/embargo and opportunity/time grouping; transformations fit on training only. Predictive RMSE, repeated management variants, synthetic fixtures or additional Monte Carlo do not establish economic edge. Do not tune canonical parameters using the old 29 labels. Report programme economics as NOT ESTIMABLE until qualified support and the registered dependence-aware economic tests pass.

## 9. Safe stop/restart and acceptance criteria

SIGINT/SIGTERM requests a collector stop; wait for its current bounded operation and durable completion, close readers, and use SQLite backup/checkpoint tools before moving data. A process killed before commit cannot be assumed complete. Restart preserves raw receipts, evidence grades, watches/consumed opportunities and immutable published identities. A conflicting identity, unavailable reserve or over-quota write must stop affected work. Never clean owner directories or required immature dependencies.

Acceptance requires: exact independently reviewed source; all focused/affected regressions and static/hash/dependency checks; original native source/health/product/universe/feature/decision timestamps; no restored missed deadlines; immutable policy/action/observation lineage; supported future execution/funding evidence and explicit censoring; independent-opportunity accounting; causal purged ML; actual pinned model smoke results for included models; verified temporary accounting and physical reserve; and actual declared host capability. Missing any indispensable input is a refusal/test gate. This candidate's current status is **CORRECTION_TEST_GATE**; no automatic merge, activation or capital approval.
