# Sol Session R-E2 reproduction and evidence gate

## Current status

E1 retrieved one real Binance public archive: BTCUSDT 1H for 2024-01-01 (1,561-byte ZIP; official 92-byte .CHECKSUM; matching SHA-256 `09a24635f090c9b2ab17f4a61aa3c4d262a7ccc88e96ff0199ce66a54a8ab66b`). It contains 24 hourly records. The raw ZIP is not committed. No S1/S6 feature or policy output was run, no opportunity panel or label exists, and no outcome was inspected.

Although the earlier HEAD probes were proxy-denied, later anonymous GET of the exact archive and checksum succeeded. This proves limited file access. It does not solve cohort construction: the root is an HTML landing page, the symbol directory returned 404, and a tested object-list route returned proxy 403. No exhaustive historic USD-M USDT-perpetual symbol/object-key inventory or listing/delisting/filter-revision chronology was identified. Treat the full research panel as blocked at source sufficiency; do not substitute a present-day universe.

## Single smallest factual remedy

Obtain one authoritative historical USD-M USDT-perpetual inventory manifest that includes every official archive object key for the candidate period plus point-in-time listing, delisting, rename and contract/filter revision dates. This would make unbiased archive coverage and S6 breadth measurable. Do not widen download scope until this source exists and a measured cost estimate supports the 1 GiB/4 GiB/90-minute limits.

## Preserve policy and evidence gates

- Keep baseline policy bytes and variants at F77 unchanged.
- S1 requires its 4H/1H/confirmed-15M sequence, fresh BBO/mark and event gate; candle-only replay is diagnostic.
- S2 is not runnable unless its exact 2,880 prior 15M observations plus required warmup and 1H/4H context exist.
- S3 needs cutoff-available trade VWAP and source completeness; 1M candle volume is not a substitute.
- S4 needs genuine book/event continuity, receipt and latency evidence.
- S5 continuation and reversal remain distinct; absent funding/OI/liquidation coverage or required S4 inputs stay unavailable.
- S6 needs 720 completed hourly returns, aligned BTC/ETH/peer context and at least 20 as-of eligible symbols before its later 15M action.
- S7 historical provider receipts cannot be reconstructed by prompting a current model.
- S8 needs 721 synchronized hourly pair prices and both-leg execution/economic evidence.
- M0/M1 require a complete, mature point-in-time exact-action population. Agent B history requires original stored requests/responses.

No E1 outcome was calculated, but prior policy/data exposure is unverified. Treat Jan–Sep 2026 as historical diagnostic unless an independent exposure audit establishes eligibility. Keep later outcomes in a separate maturation partition.

## Bounded sample reproduction

The one sample is pinned in `DATASET_MANIFEST.json`. From a clean checkout, run this single command:

```sh
python docs/research/atlas-red-e1/refetch_sample.py
```

The standard-library script downloads only the official ZIP and sibling CHECKSUM to memory, rejects unexpected redirect hosts, verifies published and pinned SHA-256 values, checks ZIP CRC/member/schema, and validates all 24 consecutive hourly timestamps. It retains no raw file. The clean-clone execution proof and its exact repository SHA are recorded in the manifest. This recipe re-fetches and validates the sample only; it does not regenerate a policy panel. The fixture test counts are code-contract checks, not market-source verification.

Independent ChatGPT review of the exact pushed handoff is required before Sol's evaluation. This package does not authorize P2 or any trading, provider or capital action.
