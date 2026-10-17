# R-E1B reproduction and artifact handling

## Rebuild

The full research feature panel is generated from checksum-verified anonymous monthly Binance USD-M public archives. Provide a scratch directory with at least 4 GiB free space; the builder caps each ZIP at 20 MiB, aggregate accepted archive bytes at 1 GiB, and network concurrency at four. It uses the environment's normal proxy configuration and TLS verification.

```sh
python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch /path/to/scratch/e1b \
  --outdir docs/research/atlas-red-e1b
python3 docs/research/atlas-red-e1b/summarize_research_outputs.py
```

The first command writes `SOURCE_MANIFEST.jsonl`, `ASOF_COVERAGE.csv.gz`, and `DIAGNOSTIC_FEATURE_PANEL.csv.gz`. The second validates coverage/panel counts, creates `COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz`, and writes `DATASET_MANIFEST.json`. To rebuild offline from already extracted source CSVs, pass `--offline-manifests H1.jsonl M15.jsonl` to the panel builder; those manifests must point to source CSVs present in the scratch directory.

The corrected current full panel contains 819,840 rows and is 205,348,124 compressed bytes, above GitHub's 100 MiB per-file limit. Its SHA-256 is `0cb67844a9a13110fba6628707d3be7a2be03ffa1fc156543b49ff8284e2f6cc`; exact hashes and row/state counts are preserved in `DATASET_MANIFEST.json`. The 187,947,551-byte / `d3955dcf…` panel later in this document is the historical pre-correction artifact from the earlier clean-clone proof, not the corrected panel. The committed 6,720-row sample contains all 35 symbols for the first UTC decision day in each window. The complete 1,024,800-row H1/M15 coverage calendar and all source verification records are committed.

## Evidence limits

The accepted, unique checksum-verified ZIP payload totals 55,549,936 bytes across 757 objects: H1 13,662,312 bytes, M15 evaluation 33,891,893 bytes, and M15 prewindow seed 7,995,731 bytes. The earlier combined figure 49,584,205 was a subtotal typo; H1 plus M15 evaluation is 47,554,205 bytes before adding the M15 seed. Thirteen requested objects returned official 404. A first H1 parser pass made 420 attempts and rejected the CSV header before its result captured byte metadata. These attempts remain labeled `PARSER_REJECTED_PRECEDING_ATTEMPT`, include the exact header observed on the successful follow-up, and have `downloaded_bytes=NOT_CAPTURED_BY_REJECTED_PARSER`; their bytes are not added to accepted ZIP totals. The actual complete network byte total is unknown because of those uncaptured first-pass bytes and transport overhead.

The 2024 and 2025 rows are reconstructed from files fetched after their historical dates. They do not prove original receipt times, historical eligibility, true fills, costs, profitability, or complete-market coverage. S1 outputs are bar-based precursor diagnostics; S6 outputs are price-only fixed-cohort ranks. See `COHORT_AND_SPLITS.md` for the frozen cohort, window boundaries, missing source detail, and inference limits.

The independent model/effort attestation is recorded as `UNVERIFIED`. A clean-clone rebuild is documented below. That historical proof concerns the earlier producer only and does not establish clean-clone parity for the current correction. The exact repository base and final pushed commit are recorded in the enclosing task report.

## Clean-clone rebuild proof

A fresh depth-1 clone of commit `ff0d8882ed1359bc88aa35daf1c8de175c179532` was built with an empty scratch directory and the online one-command recipe above. It re-fetched and checksum-verified 757 archives, retained 13 official 404s, and reproduced the full panel exactly: 819,840 rows, 187,947,551 bytes, SHA-256 `d3955dcf2fdb42bed5b7ba3b8430e20d662405ec6cb1dfa6c20e1f718bef0cae`. The coverage output also matched exactly: 1,024,800 rows, 9,378,530 bytes, SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`. The compact sample SHA-256 matched `b5ada9250a1f7cd09355e785e38fdee0211ce3ffcf1d6d19172496802e69ba4b`. The six research tests also passed in that clean clone. The final metadata commit records these results; it changes only this manifest and report, not builder or analysis code.

## R1/R2 correction rebuild — 2026-10-17

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

The original clean-clone proof above describes an earlier producer SHA and does not establish clean-clone parity for R1/R2. Corrected code was not rebuilt in a fresh clone or from an empty archive cache. Its full panel has 819,840 rows; panel, compact sample, source manifest and unchanged coverage hashes are in the current `DATASET_MANIFEST.json`.

Historical actual receipt remains unknown. The builder emits diagnostic-only `ResearchBar` values with stable archive-SHA/CSV-row identity, no `RawObservationV2`, and separate counterfactual close+5s replay availability. Derived feature readiness is the latest required H1/H4/M15 replay time plus at least one second. Exact-close S1 values are descriptive only. Raw predicate, unique first-trigger event and unique hourly S6 counts are reconciled in the current dataset manifest.
