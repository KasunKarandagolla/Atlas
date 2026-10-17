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

The full panel contains 819,840 rows and is 187,947,551 compressed bytes, above GitHub's 100 MiB per-file limit. It is therefore ignored by this package's `.gitignore`; its exact SHA-256 and row/state counts are preserved in `DATASET_MANIFEST.json`. The committed 6,720-row sample contains all 35 symbols for the first UTC decision day in each window. The complete 1,024,800-row H1/M15 coverage calendar and all source verification records are committed.

## Evidence limits

The accepted, unique checksum-verified ZIP payload totals 55,549,936 bytes across 757 objects: H1 13,662,312 bytes, M15 evaluation 33,891,893 bytes, and M15 prewindow seed 7,995,731 bytes. The earlier combined figure 49,584,205 was a subtotal typo; H1 plus M15 evaluation is 47,554,205 bytes before adding the M15 seed. Thirteen requested objects returned official 404. A first H1 parser pass made 420 attempts and rejected the CSV header before its result captured byte metadata. These attempts remain labeled `PARSER_REJECTED_PRECEDING_ATTEMPT`, include the exact header observed on the successful follow-up, and have `downloaded_bytes=NOT_CAPTURED_BY_REJECTED_PARSER`; their bytes are not added to accepted ZIP totals. The actual complete network byte total is unknown because of those uncaptured first-pass bytes and transport overhead.

The 2024 and 2025 rows are reconstructed from files fetched after their historical dates. They do not prove original receipt times, historical eligibility, true fills, costs, profitability, or complete-market coverage. S1 outputs are bar-based precursor diagnostics; S6 outputs are price-only fixed-cohort ranks. See `COHORT_AND_SPLITS.md` for the frozen cohort, window boundaries, missing source detail, and inference limits.

The independent model/effort attestation is recorded as `UNVERIFIED`. A clean-clone rebuild is documented below. The exact repository base and final pushed commit are recorded in the enclosing task report.

## Clean-clone rebuild proof

A fresh depth-1 clone of commit `ff0d8882ed1359bc88aa35daf1c8de175c179532` was built with an empty scratch directory and the online one-command recipe above. It re-fetched and checksum-verified 757 archives, retained 13 official 404s, and reproduced the full panel exactly: 819,840 rows, 187,947,551 bytes, SHA-256 `d3955dcf2fdb42bed5b7ba3b8430e20d662405ec6cb1dfa6c20e1f718bef0cae`. The coverage output also matched exactly: 1,024,800 rows, 9,378,530 bytes, SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`. The compact sample SHA-256 matched `b5ada9250a1f7cd09355e785e38fdee0211ce3ffcf1d6d19172496802e69ba4b`. The six research tests also passed in that clean clone. The final metadata commit records these results; it changes only this manifest and report, not builder or analysis code.
