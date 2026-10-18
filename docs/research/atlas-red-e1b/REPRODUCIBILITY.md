# R-E1B reproduction and artifact handling

## Rebuild

The full research feature panel is generated from checksum-verified anonymous monthly Binance USD-M public archives. Provide a scratch directory with at least 4 GiB free space; the builder caps each ZIP at 20 MiB, aggregate accepted archive bytes at 1 GiB, and network concurrency at four. It uses the environment's normal proxy configuration and TLS verification.

```sh
git show ab645e792fe6593d9fb206954585b2996673f96c:docs/research/atlas-red-e1b/SOURCE_MANIFEST.jsonl \
  | python3 -c 'import json,sys; [print(json.dumps(r,sort_keys=True,separators=(",",":"))) for r in map(json.loads,sys.stdin) if r.get("status")=="PARSER_REJECTED_PRECEDING_ATTEMPT"]' \
  > /path/to/prior-parser-rejected-attempts.jsonl
python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch /path/to/scratch/e1b \
  --outdir docs/research/atlas-red-e1b \
  --rejected-attempt /path/to/prior-parser-rejected-attempts.jsonl
python3 docs/research/atlas-red-e1b/summarize_research_outputs.py
```

The first shell pipeline extracts the pinned 420 historical parser-rejected attempts without assigning them ZIP byte totals. The builder writes `SOURCE_MANIFEST.jsonl`, `ASOF_COVERAGE.csv.gz`, and `DIAGNOSTIC_FEATURE_PANEL.csv.gz`; the summarizer validates coverage/panel counts, creates `COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz`, and writes `DATASET_MANIFEST.json`. To rebuild offline from already extracted source CSVs, pass `--offline-manifests H1.jsonl M15.jsonl` to the panel builder; those manifests must point to source CSVs present in the scratch directory. The exact extraction and builder command strings are also recorded in the dataset manifest.

The corrected current full panel contains 819,840 rows and is 206,171,887 compressed bytes, above GitHub's 100 MiB per-file limit. Its SHA-256 is `68ee994b983f47de86cec2781c9469f6e992d03a03728517978790b571c9d682`; exact hashes and row/state counts are preserved in `DATASET_MANIFEST.json`. The 6,720-row sample contains all 35 symbols for the first UTC decision day in each window. The complete 1,024,800-row H1/M15 coverage calendar and all source verification records are committed.

## Evidence limits

The accepted, unique checksum-verified ZIP payload totals 55,549,936 bytes across 757 objects: H1 13,662,312 bytes, M15 evaluation 33,891,893 bytes, and M15 prewindow seed 7,995,731 bytes. The earlier combined figure 49,584,205 was a subtotal typo; H1 plus M15 evaluation is 47,554,205 bytes before adding the M15 seed. Thirteen requested objects returned official 404. A first H1 parser pass made 420 attempts and rejected the CSV header before its result captured byte metadata. These attempts remain labeled `PARSER_REJECTED_PRECEDING_ATTEMPT`, include the exact header observed on the successful follow-up, and have `downloaded_bytes=NOT_CAPTURED_BY_REJECTED_PARSER`; their bytes are not added to accepted ZIP totals. The actual complete network byte total is unknown because of those uncaptured first-pass bytes and transport overhead.

The 2024 and 2025 rows are reconstructed from files fetched after their historical dates. They do not prove original receipt times, historical eligibility, true fills, costs, profitability, or complete-market coverage. S1 outputs are bar-based precursor diagnostics; S6 outputs are price-only fixed-cohort ranks. See `COHORT_AND_SPLITS.md` for the frozen cohort, window boundaries, missing source detail, and inference limits.

The owner confirmed GPT-6 Luna, Medium selection; runtime model/effort metadata is not exposed for independent verification. The current producer's clean-clone rebuild is documented below. The exact repository base and final pushed commit are recorded in the enclosing task report.

## Clean-clone rebuild proof

A fresh depth-1 clone of commit `ff0d8882ed1359bc88aa35daf1c8de175c179532` was built with an empty scratch directory and the online one-command recipe above. It re-fetched and checksum-verified 757 archives, retained 13 official 404s, and reproduced the full panel exactly: 819,840 rows, 187,947,551 bytes, SHA-256 `d3955dcf2fdb42bed5b7ba3b8430e20d662405ec6cb1dfa6c20e1f718bef0cae`. The coverage output also matched exactly: 1,024,800 rows, 9,378,530 bytes, SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`. The compact sample SHA-256 matched `b5ada9250a1f7cd09355e785e38fdee0211ce3ffcf1d6d19172496802e69ba4b`. The six research tests also passed in that clean clone. The final metadata commit records these results; it changes only this manifest and report, not builder or analysis code.

## R1/R2 correction rebuild — 2026-10-17 (historical state before the 2026-10-18 censoring follow-up)

The final correction rebuild used the existing checksum-verified local CSV cache and manifests offline; it did not fetch archives. The command shape was:

```sh
python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch /path/to/e1b-cache \
  --outdir docs/research/atlas-red-e1b \
  --offline-manifests /path/to/e1b-h1-manifest.jsonl /path/to/e1b-m15-with-seed-manifests.jsonl \
  --rejected-attempt /path/to/e1b-h1-parser-rejected-attempt.jsonl
python3 docs/research/atlas-red-e1b/summarize_research_outputs.py
```

These are sanitized placeholders for the pre-existing local cache; no absolute workspace or private path is committed. The final corrected build took **2m04.111s** wall time. RSS and network bytes were not measured; no network requests were made. The first run was aborted at 350,000 rows in window A after its per-row history scan was found. Completed outputs were superseded after checks found missing current M15 rows outside an active setup window labeled `NOT_IN_TRIGGER_WINDOW`, then setup-event identities incrementing on unchanged repeated SETUP state and failing to reset on changed side/extreme. The final builder classifies every missing current M15 input as `SOURCE_MISSING`; unchanged setup attributes retain their originating identity, changed attributes begin a new setup, and incomplete prewindow trigger history is marked unknown rather than calling an observed match the first. The final panel contains 819,840 rows (205,348,124 compressed bytes), SHA-256 `0cb67844a9a13110fba6628707d3be7a2be03ffa1fc156543b49ff8284e2f6cc`. The compact sample is 6,720 rows / 1,701,039 bytes, SHA-256 `79e4fbd8551623c957501bd099fcc5075e70398e32fe1e2c101bc810a1fbac6f`. The source manifest is 1,139,495 bytes, SHA-256 `8d510aee5fde7843fe44f74979680d10baef5daae199aed53311fcedfe4f9709`; unchanged coverage is 1,024,800 rows / 9,378,530 bytes, SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`. Final setup events are A/B 15,062/13,124, raw predicates 13,690/12,003, and first events 9,261/7,899. The delivered run uses the same 757 checksum-verified source archives.

At the time of this 2026-10-17 record, corrected code had not yet been rebuilt in a fresh clone. That status is superseded by the qualified 2026-10-18 clean-clone rebuild below. The 2026-10-17 artifacts are historical; current artifact hashes are recorded in the current `DATASET_MANIFEST.json`.

Historical actual receipt remains unknown. The builder emits diagnostic-only `ResearchBar` values with stable archive-SHA/CSV-row identity, no `RawObservationV2`, and separate counterfactual close+5s replay availability. Derived feature readiness is the latest required H1/H4/M15 replay time plus at least one second. Exact-close S1 values are descriptive only. Raw predicate, unique first-trigger event and unique hourly S6 counts are reconciled in the current dataset manifest.

## R1 active-window first-trigger censoring correction — 2026-10-18

The active-opportunity defect was reproduced against reviewed parent `ab645e792fe6593d9fb206954585b2996673f96c` using the actual `build()` producer path. Red command: `python -m pytest -o addopts= -q tests/research/test_atlas_red_e1b_panel.py -k r1_producer_censors_later_match_after_missing_active_m15_opportunities`. It failed **2 tests / 18 deselected** (LONG and SHORT); both failed at `assert not any(row["s1_first_trigger_event"] == "1" for row in rows)` because the old producer emitted a first event after an earlier missing current M15 and a missing/noncontiguous previous M15. The final fixtures retain both source-missing reasons and raw positive rows while suppressing a verified first event. Known first events remain established after later gaps.

Corrected producer commit `04f05ab641457ab6deee8456da2c2a77d84f5149` was freshly cloned at depth 1 from `fix/atlas-red-e1b-first-trigger-censoring-2026-10-18`. The clone started with empty scratch/output directories. Its online build fetched 757 official monthly ZIPs and sibling `.CHECKSUM` files; every accepted ZIP passed the published SHA-256 check. Thirteen requested objects returned official 404. The frozen cohort SHA remains `b51f4a360e60bbb62fae982e4120ff3681b12a13ef8cd4a0aff01d761f6f03b5` (35 symbols). Archive concurrency remained four; per-object and total limits remained 20 MiB and 1 GiB.

The source manifest also preserves the 420 original `PARSER_REJECTED_PRECEDING_ATTEMPT` records. They are the exact prior-checkpoint records, not new downloads or fabricated byte totals. Their source is the `ab645e792fe6593d9fb206954585b2996673f96c` `DATASET_MANIFEST.json` SHA-256 `f22f7e16ab8fd9d92b84b8df51cf9d6e662d1ffc03465acdcc5d9e7c8aa89e0e`; their ZIP byte totals remain `NOT_CAPTURED_BY_REJECTED_PARSER`. The qualified clean clone extracted these records from the pinned prior source manifest and passed them to the online builder in the same run.

Final artifacts:

- Full panel: 819,840 rows, 206,171,887 bytes, SHA-256 `68ee994b983f47de86cec2781c9469f6e992d03a03728517978790b571c9d682`. It remains uncommitted because it exceeds GitHub's 100 MiB file limit.
- Coverage: 1,024,800 rows, 9,378,530 bytes, SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`.
- Compact sample: 6,720 rows, 1,706,325 bytes, SHA-256 `f678dce748d7f7ac4402067088ce9b970e6fe7b2ba02b4c607f31537b73e2608`.
- Source manifest: 1,139,495 bytes, SHA-256 `c64a12c3aa5e4c500f0b431fe65c14fc07e45f18bc30100b6298f539f4c80d7b`.
- Dataset manifest: SHA-256 `41e4a5c8975610bae7a9e9c288ab4114e2981da19814a0df1871a8ab3b18221a`.

The separate cold-cache build from previously checksum-verified local CSVs produced byte-identical panel and coverage artifacts. All 1,191 canonical source-manifest rows matched with zero differences after excluding acquisition timing, scratch paths, and retrieval order. All 757 freshly retrieved archives have downloaded SHA-256 equal to the sibling `.CHECKSUM` value. The raw source-manifest digest records this run's transport metadata; these differences do not affect panel bytes or source-row lineage. The prior 420 parser-rejected records remain present in the final source manifest.

The exclusive setup-level outcome partition reconciles for every window/symbol pair. Window A: 15,062 unique setup identities = 9,261 verified in-window first triggers + 5,800 complete observed no-matches + 1 incomplete/unresolved; zero censored setups. Window B: 13,126 unique setup identities = 7,899 verified first triggers + 5,225 complete observed no-matches + 2 censored setups; zero incomplete/unresolved. Both censored identities are EOSUSDT in B; both were censored by missing active-window observations, with no pre-window-history censure. Their seven active opportunity rows were all `SOURCE_MISSING` for `CURRENT_M15_MISSING`, and there were zero observed positive predicates after censorship. Raw-positive M15 rows remain 13,690 in A and 12,003 in B. Verified first-trigger totals therefore remain equal to the reviewed reference values (9,261/7,899); neither censored setup had a later positive predicate that would change those totals.

All 15,610 missing current-M15 rows in B remain explicit `SOURCE_MISSING`: EOSUSDT has 3,898 and MATICUSDT has 11,712, each with reason `CURRENT_M15_MISSING`. The coverage calendar separately reconciles 14,591 official-404 and 1,019 native-gap M15 slots in B. M15-row counts, setup-level counts, and overlapping diagnostics are kept separate in `DATASET_MANIFEST.json`; only the exclusive outcome partition is mutually exclusive. The full summary includes every per-symbol zero and nonzero count and asserts that each partition total equals its unique setup identities. It rejects duplicate first events, censored first events, missing setup lineage, and event IDs that do not hash the setup ID plus M15 cutoff.

The final manifest marks `clean_clone_rebuild_tested=true` for depth-one producer commit `04f05ab641457ab6deee8456da2c2a77d84f5149`, empty initial scratch and output directories, and official anonymous retrieval/checksum verification. Producer code SHA-256 is `8f6418eeeeee841f11784172eddd7d573d11a61b4f96b7b7c830e71e401dfd96`; summarizer code SHA-256 is `4a0c54f6689b4af56457703e42e9882b448805329f0235d04ca4d7977710c55e`. The exact prior-attempt extraction and rebuild commands are recorded in `DATASET_MANIFEST.json`; the summary command was run against the clean-clone outputs.

Verification commands and results:

```sh
python -m pytest -o addopts= -q tests/research/test_atlas_red_e1b_panel.py
# 21 passed
python -m pytest -o addopts= -q tests/v2/test_session014_s1.py tests/v2/test_session021_s6.py tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_cohort.py tests/v2/test_session041_s6_action_contract.py tests/data/test_prefix_and_archive.py
# 40 passed
ruff check docs/research/atlas-red-e1b/build_research_panel.py docs/research/atlas-red-e1b/summarize_research_outputs.py tests/research/test_atlas_red_e1b_panel.py
# passed
python -m compileall -q docs/research/atlas-red-e1b tests/research/test_atlas_red_e1b_panel.py
uv lock --check
uv pip check
git diff --check
# all passed
```

`mypy src` reports the existing 53 errors in 17 files on both `ab645e7` and the corrected worktree; normalized diagnostics match exactly. The repository-wide test suite was not run. The 2024/2025 archive values remain reconstructed market data with unknown historical receipt; this rebuild creates no fills, costs, outcomes, or profitability evidence.
