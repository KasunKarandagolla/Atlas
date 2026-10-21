# Reproduction and review

Base: `924a26f2a6a85f6468d4d4f6a44069c332c4096e`. Protocol commit: `97233b6cc66d9b17a0f3c5ca08d4b2d4e9051d43`. Analysis/assumption freeze before August: `9487123316f2411fd9725f99c0d0f6b70a463b1f`. Final publication adds reports, sources and descriptive reporting only; obtain its exact immutable SHA from the branch handoff / `git rev-parse HEAD`, because a commit cannot embed its own SHA.

Branch: `research/atlas-red-historical-economic-investigation-2026-10-21`. Accepted design stays `f9f8ef083ba5bafcad9bab4d07227676200a948f`; corrected implementation `75fca65e8352bd10c4b17033d90dd96cfc18f8e9` is an ancestor, **not independently accepted**. All remote heads were fetched explicitly because origin initially tracked only the historical default branch. The original `/workspace/Atlas` checkout at `bac8b16e767e7a253cc41c4e73f16ac1c6658319` remains unchanged. Independent reviews in the repository accept narrower prior historical/source work; none promotes this correction or operating policy.

Three original freezes were read in full and their exact hashes verified:

```
c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c  V1
e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78  V2
d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d  Agent
```

Current canonical design/PDF/active policy and production strategies are unchanged. Private original owner/ADR packets were unavailable and are not claimed read. Their public source-authority reconciliation and accepted design/acceptance/implementation/research contracts were consulted. The task uses the owner's explicit new scoped historical authorization; it does not treat prior runbook campaign gates as authority to start a service.

Runtime: existing `/workspace/.venvs/atlas/bin/python`, Python 3.12.14, standard library analysis and existing pure ATLAS owners. Pytest 9.1.1 and the existing Ruff ran. No packages/models were installed, no foundation inference or ML fitting ran. Requested GPT-6.1 Sol / High identity cannot be independently attested by this runtime; no subagents. Exact source hashes and machine verification are in [evidence/verification.json](evidence/verification.json); output hashes are in [ARTIFACTS.sha256](ARTIFACTS.sha256).

## Actual principal commands

Read-only discovery/read/hash commands accompanied these commands; no full suite ran:

```bash
git ls-remote --heads origin
git fetch origin '+refs/heads/*:refs/remotes/origin/*'
git merge-base f9f8ef08 924a26f2
git merge-base 75fca65e 924a26f2
git worktree add -b research/atlas-red-historical-economic-investigation-2026-10-21 \
  /workspace/Atlas-historical-2026-10-21 924a26f2a6a85f6468d4d4f6a44069c332c4096e
```

Phase A used bounded Python urllib GETs of the exact ZIP and sibling CHECKSUM URLs in `MARKET_DATA_EVIDENCE.json`, a 25-second timeout, 2,000,000-byte ZIP limit, 4,096-byte checksum limit, ZIP CRC/member/schema/timestamp/price checks and SHA-256 equality. A separate GET retained the official public-data README. These were actual bytes, not HEAD/DNS checks. The auxiliary BTC funding and mark probes were also actual GET/checksum validations; their records are retained. No alternate proxy, TLS bypass or private exchange request was used.

The following environment was used on each bounded invocation:

```bash
export ATLAS_TEMP_ROOT=/workspace/atlas-historical-empty-temp
export ATLAS_PERMANENT_ROOT=/workspace/shared/atlas-historical-2026-10-21
export ATLAS_TEMP_CAPACITY_BYTES=256000000
export ATLAS_EXTERNAL_TEMPORARY_BYTES=200000000
export PYTHONPATH=src
PY=/workspace/.venvs/atlas/bin/python
P=docs/research/atlas-red-historical-economic-investigation-2026-10-21
B=scripts/atlas_red_bounded_run.sh
$B "$PY" scripts/atlas_red_historical_economics.py --phase acquire --raw-root "$ATLAS_PERMANENT_ROOT/raw"
$B "$PY" scripts/atlas_red_historical_economics.py --phase july --raw-root "$ATLAS_PERMANENT_ROOT/raw"
# Auxiliary acquisition initially ran the retained identical acquisition procedure
# from a permanent script; its published CLI has configurable paths:
$B "$PY" scripts/atlas_red_historical_funding.py --permanent-root "$ATLAS_PERMANENT_ROOT" --phase acquire
# After commit 9487123, without changing strategy/cost code:
$B "$PY" scripts/atlas_red_historical_economics.py --phase all --raw-root "$ATLAS_PERMANENT_ROOT/raw"
$B "$PY" scripts/atlas_red_historical_funding.py --permanent-root "$ATLAS_PERMANENT_ROOT" --phase bounds
$B "$PY" "$P/summarize_outputs.py" --permanent-root "$ATLAS_PERMANENT_ROOT"
$B "$PY" -m pytest -q -o addopts='' -p no:cacheprovider \
 tests/v2/test_historical_archive_economics.py \
 tests/v2/test_full_cycle_integrity.py::test_s1_reconstruction_first_event_and_trigger_time
$B "$PY" -m pytest -q -o addopts='' -p no:cacheprovider \
 tests/contract/test_v1_golden_baseline.py::test_v1_golden_baseline_recomputes_from_frozen_code_and_fixtures
```

The final focused selection passed **13/13**. An earlier new test imported a nonexistent fixture constant (11 passed/1 failed), corrected before 12/12 and final 13/13. The first July integration attempt exposed Decimal-to-SQLite binding, corrected in the research adapter before the completed July analysis and before August. Logs preserve these failures.

The additional V1 golden node **failed** because `nautilus_trader` is absent. The same node failed identically on an isolated clean `924a26f2` worktree. No dependency installation or golden weakening followed. Final current-scope verification is therefore **13 passed / one separate same-base environment failure**, not a fully green suite. Existing strategy/financial tests were not broadly rerun. Ruff on changed analysis code, original freeze hashes and the pre-publication algorithms manifest passed. The reporting script receives separate lint/AST checks. An initial publication manifest command ran from the repository root and reported a missing MANIFEST.sha256; it was repeated successfully from algorithms/. SQLite integrity is `ok`; effective qualified outcomes stay zero. Four real BTC native-prefix comparisons passed exactly, including a positive trigger; 6,759 July paths and 54 summary rows were unchanged after the August extension (not counted as unit tests).

## Fresh reproduction without changing published evidence

Use a fresh clone at the final pushed SHA, a new permanent root and an empty dedicated temporary mountpoint. Use a compatible Python 3.12 runtime; no optional trading/model stack is needed for the research computations. Supply `--output "$ATLAS_PERMANENT_ROOT/rebuilt"` to **each** acquisition/evaluation/funding CLI and to `summarize_outputs.py`. Run acquisition, auxiliary acquisition, July, all, bounds and reporting in that order. Keep the published source manifests untouched.

Before running labels, compare `(URL, archive_sha256)` from the newly acquired `archive_sources.json` and `AUXILIARY_SOURCES.json` with their published counterparts. Stop on any changed/missing official object; do not silently accept a later archive revision. All original actual acquisition timestamps will differ on a fresh download. Price/action/result identities do not manufacture historical receipt times; deterministic conditional result tables and compact action/evaluation outputs should match published hashes. Published original source receipts remain the audit record. The first Phase A sample is independently refetchable at its pinned URLs/hash.

Detailed full evaluation ledgers (about 19 MB compressed), raw ZIPs and SQLite files remain in separate permanent storage, never committed as raw market archives. [EXTERNAL_ARTIFACTS.json](EXTERNAL_ARTIFACTS.json) records full-ledger paths/size/hash; the public complete compact due-slot ledger and detailed conditional action/funding paths permit review. Regenerate full ledgers with the declared CLIs. After reproduction, compare artifact hashes; changed source vintages are a new dataset, not an equivalent rerun. A fresh remote clone end-to-end rebuild was **not** executed in this session; same-data native-prefix checks were executed.

## Resources and limits

125 kline acquisition: 89.91 seconds / 43,884 KiB peak process RSS. Completed July: 74.34 seconds / 89,744 KiB. Full 62-day analysis: **143.93 seconds / 111,924 KiB**. These are cloud process measurements, not 4 GB target-host or ten-second native service qualification.

The existing bounded wrapper creates a separate private 256,000,000-byte tmpfs. Capacity + declared 200,000,000 external temporary allowance + 4,096 margin = **456,004,096 bytes**, strictly below 5,000,000,000. Inventory of existing external caches was approximately 8.1 MB; no owner evidence/cache was deleted. A 0.1-second sampler over 1,407 observations measured **94,208 bytes temporary peak** and **31,342,837,760 bytes minimum physical free space**, above the required 2 GB reserve. A sampled peak is not an exact mathematical peak; the kernel cap is separately enforced. Raw evidence, SQLite/WAL and final reports/code have separate permanent accounting. `evidence/storage-final.json` records final measured directory/database bytes. No oversized write/ENOSPC probe or full regression was repeated.

Evidence grades: genuine public market archives; retrospective price-predicate reconstruction; conditional modeled execution/funding cash; **no original-time native qualification, prospective sealed actions, actual executions, Agent B quality, complete-programme profitability, promotion or capital approval**. August is now spent as a holdout. Independent review must examine the actual pushed diff, native math reuse, first-event semantics, hypothetical clock, cost/censoring assumptions, denominators and descriptive inference limitations before any dependent action.
