# R-E1B Session 1 evidence handoff

## R1/R2 correction checkpoint — 2026-10-17

The exact correction is described in [`E1B_R1_R2_CORRECTION_HANDOFF.md`](E1B_R1_R2_CORRECTION_HANDOFF.md). The current producer uses deterministic setup and originating-trigger identities, separately counts raw bar predicates and unique first-trigger events, preserves all missing M15 input rows, and does not emit fabricated `RawObservationV2` actual receipt fields. Historical receipt is unknown; a distinct +5s replay schedule and ≥1s feature processing delay are documented. Exact-close S1 values remain zero-lag descriptive diagnostics.

The original counts below are the accepted E1B baseline and are retained as historical evidence. In the corrected panel, precursor states remain A 87,540 SETUP / 322,380 NO_SETUP and B 80,228 SETUP / 317,980 NO_SETUP / 11,712 SOURCE_MISSING. Transition-based setup-event counts are A 15,062 and B 13,124; raw trigger predicates are A 13,690 and B 12,003; unique first-trigger events are A 9,261 and B 7,899. All missing current M15 inputs remain SOURCE_MISSING (A 0, B 15,610); within active setup windows, 3,898 B EOS rows and 11,712 B MATIC rows lack a valid current/prior trigger pair. Current per-symbol and S6 unique-hour counts are in the dataset manifest. Full panel count remains 819,840; coverage remains 1,024,800 with unchanged hash. The corrected panel SHA-256 is `0cb67844a9a13110fba6628707d3be7a2be03ffa1fc156543b49ff8284e2f6cc` (205,348,124 bytes).

Original exact-SHA clean-clone evidence applies to the prior E1B producer only; clean-clone parity for this correction was not rerun. Independent acceptance of the correction is pending.

## Delivered evidence

Research candidate: branch `research/atlas-red-binance-cohort-e1b-2026-10-18`, code/data package SHA `9c872fb8e66a1396412453e132cc00bb576e9bcb` before this documentation-only follow-up. This is a research diagnostic, not an activated trading system. The ordered 35-symbol convenience cohort is frozen at SHA-256 `b51f4a360e60bbb62fae982e4120ff3681b12a13ef8cd4a0aff01d761f6f03b5` and covers decision cutoffs `[2024-03-01 00:00, 2024-07-01 00:00)` and `[2025-03-01 00:00, 2025-07-01 00:00)` UTC.

Official anonymous Binance USD-M monthly ZIP objects and sibling checksums were validated before rows were used. Accepted inputs: 757 archives, 55,549,936 unique ZIP bytes, 1,292,663 CSV rows. This comprises H1 413 / 13,662,312 bytes, evaluation M15 275 / 33,891,893 bytes, and M15 February prewindow seeds 69 / 7,995,731 bytes. Thirteen objects returned official 404. First-pass parser rejects remain separately labeled and are not counted as missing or included in accepted bytes. See `SOURCE_MANIFEST.jsonl` for per-object provenance and `DATASET_MANIFEST.json` for full reconciliation.

The complete feature panel has 819,840 rows (409,920 per window), SHA-256 `d3955dcf2fdb42bed5b7ba3b8430e20d662405ec6cb1dfa6c20e1f718bef0cae`. The committed same-slot real sample has 6,720 rows and includes all 35 symbols on the first decision day of both windows. Complete as-of input coverage has 1,024,800 rows. The 187,947,551-byte full panel is reproducible but omitted from Git because it exceeds GitHub's 100 MiB per-file limit.

Coverage gaps are explicit, not silently imputed: B M15 has 14,591 `OFFICIAL_ARCHIVE_404` and 1,019 `NATIVE_SLOT_GAP` slots (15,610 total); B H1 has 3,647 archive-404 and 254 native-gap slots (3,901 total). A has no missing native slots. In the fixed feature panel, S1 precursor counts are A 87,540 SETUP / 322,380 NO_SETUP; B 80,228 SETUP / 317,980 NO_SETUP / 11,712 SOURCE_MISSING. S1 bar-trigger counts are A 19,970 BAR_TRIGGER / 322,383 NOT_IN_TRIGGER_WINDOW / 67,567 NO_TRIGGER; B 17,524 / 317,980 / 58,806 / 15,610 SOURCE_MISSING. S6 states are A 398,208 price-only fixed-cohort diagnostics plus 11,712 BTC reference anchors; B 370,892 price-only diagnostics, 27,316 NOT_RANKABLE and 11,712 BTC anchors. S6 requires 720 continuous matched H1 returns. These breadth counts do not establish S6 strategy eligibility.

## Evidence grade and limits

Grade: `RECONSTRUCTED_MARKET` for fetched historical archive bars. Model/effort attestation: `UNVERIFIED`. The records do not establish historical receive time, historical listing/contract revisions, point-in-time eligibility, complete-market coverage, source health, BBO/mark/index/events/funding/liquidity, fills, costs, or executable opportunity status. Missing archives are not delisting evidence. A/B are descriptive historical slices; B is not an untouched holdout. Future outcomes and labels are not included. Cost/eligibility qualification and true after-cost profitability are `NOT ESTIMABLE`. No profitability claim is supported.

S1 rows are causal bar-based precursor and trigger diagnostics checked against the repository's pure S1 math; the complete execution coordinator is not run. S6 is a transparent fixed-cohort price-only adapter cross-checked against the repository calculation. The full policy eligibility and breadth contract is not established. No strategy algorithm was changed, and no order or Sol evaluation was run.

## Rebuild and checks

From a clean checkout with Python dependencies available, use an empty scratch directory with at least 4 GiB free:

```sh
python3 docs/research/atlas-red-e1b/build_research_panel.py \
  --scratch /path/to/scratch/e1b \
  --outdir docs/research/atlas-red-e1b
python3 docs/research/atlas-red-e1b/summarize_research_outputs.py
```

The first command fetches and checksum-validates the fixed H1/M15 cohort, builds the complete panel and coverage calendar, and writes the source manifest. The second validates counts, regenerates the compact sample and dataset manifest. Network concurrency is capped at four; per-archive and aggregate limits are enforced by the builder. See `REPRODUCIBILITY.md` for the exact clean-clone proof and artifact hashes.

The six E1B-specific tests pass. Existing impacted regression command also passed 40 tests:

```sh
/workspace/.venvs/atlas/bin/python -m pytest -q \
  tests/v2/test_session014_s1.py \
  tests/v2/test_session021_s6.py \
  tests/v2/test_session037_strategy_prefix.py \
  tests/v2/test_session041_s6_cohort.py \
  tests/v2/test_session041_s6_action_contract.py \
  tests/data/test_prefix_and_archive.py
```

## Next review gate

Independent ChatGPT review must inspect the exact pushed code/data diff, S1/S6 parity evidence, source/missingness accounting, and this handoff. Any later evaluation must preserve the fixed cohort, time windows, source calendar, and same-slot keys; it must remain diagnostic until separately authorized and qualified.
