# Sol Session R-E2 reproduction and evidence gate

## Current status

The current package is a blocker handoff and schema definition. It is not a complete replayable market dataset or a tested one-command refetch recipe: the official Binance archive hostname returned HTTP 403 before a ZIP or .CHECKSUM could be read. The exact probes, README hash and zero-byte coverage are in DATASET_MANIFEST.json.

## Required first action

Have the managed environment's supported configuration workflow review an anonymous HTTPS egress allowance for data.binance.vision. Do not use Binance credentials, authenticated/current exchange REST, alternate mirrors or direct-network bypasses.

After that review has taken effect, first repeat only these HEAD requests:

https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip

https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip.CHECKSUM

If the official host redirects, record and review the exact redirect host before fetching. Download the ZIP and sibling checksum to work/, verify Binance's SHA-256 before parsing, record actual download time separately from archive event time, then parse timestamps and rows without claiming actual historical receive times. Do not use or fabricate a checksum from a changed source blob.

The exact first-sample URL pattern above is a probe candidate, not a verified available file path. If it does not resolve, use the official public-data README/helper convention to identify the actual official path; preserve the 403 or 404 distinction and stop if source identity cannot be proved.

## Existing policy gates to retain

- Keep the policy bytes and baseline variants at the F77 base unchanged.
- S1 requires its 4H/1H/confirmed-15M sequence, fresh BBO/mark and event gate; a candle-only replay is diagnostic.
- S2 is not runnable unless its exact 2,880 prior 15M observations plus required warmup and 1H/4H context exist.
- S3 needs cutoff-available trade VWAP and source completeness; 1M candle volume is not a substitute.
- S4 book/execution context needs a genuine sequence, continuity, receipt and latency evidence.
- S5 continuation and reversal remain distinct; absent funding/OI/liquidation coverage or required S4 inputs stay unavailable.
- S6 needs 720 consecutive completed hourly returns, aligned BTC/ETH/peer context and at least 20 as-of eligible symbols before its later 15M action.
- S7 historical provider receipts cannot be reconstructed by prompting a current model.
- S8 needs 721 synchronized hourly pair prices and both-leg execution/economic evidence.
- M0/M1 require a complete, mature point-in-time exact-action population. Agent B history requires original stored requests/responses; a current call is not a historical substitute.

## Holdout and handoff

No E1 outcome was calculated, but earlier policy/data exposure is unverified. Treat 2026-01 through 2026-09 as historical diagnostic unless an independent exposure audit establishes eligibility. Keep outcome labels in a separate maturation partition and do not fit or select on that partition before a registered comparison plan.

The minimal next milestone is not an S1/S6 return table. It is one official checksum-verified archive file, parsed row/schema/timestamp validation, then a measured wider H1/H4 cohort and as-of universe feasibility estimate. Only after that should the existing strategy consumers run. Every no-signal, unavailable, missing, refused or missed slot must remain in the denominator.

Clean-clone regeneration remains **BLOCKED / NOT TESTED** until a source blob and immutable official checksum are actually obtained and a deterministic regeneration command is implemented and replayed without the original cache.
