> **Recheck update (2026-10-17 18:45 UTC):** The earlier 18:11–18:12 archive HEAD probes recorded below were proxy-denied at that time. A later anonymous GET succeeded for exactly one official BTCUSDT 1H daily archive and its sibling CHECKSUM. The ZIP is 1,561 bytes (SHA-256 `09a24635f090c9b2ab17f4a61aa3c4d262a7ccc88e96ff0199ce66a54a8ab66b`); the 92-byte CHECKSUM agrees. It parses as one 12-column header plus 24 consecutive UTC hourly rows for 2024-01-01. No raw ZIP is committed. This changes the claim from zero sample access to **one genuine checksum-verified sample only**. It does not establish the historical universe, S1 inputs, S6 breadth, any signal, or outcome. The root directory is an HTML landing page, the symbol directory returns 404, and a tested S3 listing route is proxy-blocked; no broad symbol inventory/as-of universe source was established. The existing zero-row panel and all zero strategy-output counts remain correct. Full historical evidence remains blocked at the cohort/data-sufficiency gate.

# ATLAS RED Session R-E1 — source-feasibility stop and handoff

## Result

**PARTIAL SOURCE ACCESS; full panel blocked at source sufficiency.** One official BTCUSDT 1H daily ZIP and sibling checksum were later retrieved and verified. The repository strategy code and offline fixture tests were inspected, but no historical signal, opportunity, return, fill or label was generated.

This is a factual source-access stop, not a conclusion about whether Binance has the requested historical files or whether ATLAS has economic value. The official README and one BTCUSDT 1H market archive sample were retrieved. The README describes USD-M futures klines, trades and aggTrades, and says each ZIP has a sibling .CHECKSUM. Neither the README nor this sample proves point-in-time contract history, historical book completeness, fills or provider receipt chronology.

## Repository and authority preflight

- git ls-remote --heads found the requested P1 evidence-integrity candidate at f77f3aacbfdc2546e6f3ea2d78196b0905ceb613, parent 81870a18a1f773e84ad1b0c82690dc8e4d677bb8. The accepted design f9f8ef083ba5bafcad9bab4d07227676200a948f is an ancestor. Current state marks P1 independent acceptance pending; no separate F77 acceptance artifact was present in this checkout.
- The mounted /workspace/Atlas branch was the historical impl/session-001-foundation at bac8b16e767e7a253cc41c4e73f16ac1c6658319. It was clean and left untouched. This isolated worktree starts from the verified F77 SHA on research/atlas-red-binance-evidence-e1-2026-10-17.
- The three protected authority hashes matched: V1 c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c; amended V2 e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78; Agent freeze d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d. No authority or finalized algorithm file was changed.
- Requested runtime identity “GPT-6 Luna, High thinking effort” could not be independently attested. It is recorded as **UNVERIFIED**. No model call, paid API, private venue access, credentials, account access or order was used.

## Binance source probes

Retrieved official documentation: https://raw.githubusercontent.com/binance/binance-public-data/master/README.md, 5,282 bytes, SHA-256 2e133d9945a9263a02781369078e72805eff074680f305cd5460016471c6caad. This is documentation, not market data.

The initial candidate public archive HEAD requests failed at the proxy with HTTP 403 / CONNECT tunnel failed at the times recorded here:

- HEAD https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip at 2026-10-17 18:11:02 UTC.
- HEAD https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip.CHECKSUM at 2026-10-17 18:12:59 UTC.

These were time-specific failures: a later anonymous GET retrieved the exact ZIP and sibling checksum (see correction at the top). Current evidence is one official BTCUSDT 1H day, 24 rows, checksum verified; no bypass was attempted. This does not establish symbols beyond BTCUSDT, historical listing/filters or role-level sufficiency.

The full role matrix is SOURCE_FEASIBILITY_AND_ROLE_MATRIX.csv. For roles whose source may exist but was not retrieved, UNAVAILABLE means unavailable to this session; NOT_TESTED means the underlying archive capability remains unverified. No inference is made that Binance lacks those files.

## Existing policy and code findings

- The unchanged S1 consumer is src/atlas/v2/strategies/s1_trend.py, policy S1_MTF_TREND_PULLBACK 1.0.0-shadow. It requires final 4H/1H/15M bars, ATR/volatility, fresh BBO and mark/index, and a valid event gate. Bar-only setup state cannot qualify an exact action.
- S2 uses src/atlas/v2/strategies/s2_breakout.py, policy S2_COMPRESSION_BREAKOUT 1.0.0-shadow. The exact 30-day 15M comparison prefix is 2,880 bars before additional warmup. Its existing two-hour time exit and first-two-bar failure behavior were not changed.
- S3 uses src/atlas/v2/strategies/s3_mean_reversion.py; trade VWAP is computed from causal cutoff-available trades, not 1M candle volume. No trade completeness, BBO or VWAP input was obtained.
- S4 is a microstructure/execution context in src/atlas/v2/data/microstructure.py, not an independent directional strategy. A real event sequence and depth evidence are required for book-derived context; a candle or one snapshot is insufficient.
- S5 continuation and post-cascade reversal are separate research paths in src/atlas/v2/strategies/s5_crowding.py. Required funding/OI/liquidation coverage, chronology and (for reversal) S4 evidence were absent.
- S6 uses src/atlas/v2/strategies/s6_cross_section.py: 720 completed H1 returns, aligned BTC/ETH/peer context and at least 20 eligible symbols; the later 15M action has separate evidence gates. No broad cohort or action clock was observed.
- S7 uses src/atlas/v2/news/events.py. Historical backfill alone cannot establish original publication/receipt, revisions or provider output at a past decision.
- S8 in src/atlas/v2/strategies/s8_pairs.py requires 721 synchronized hourly price pairs and two-leg economics/execution evidence. Neither leg can stand in for the pair.
- Existing feature, cost, risk, mean-model, management and selection consumers are identified in the matrix. No policy threshold, strategy, feature role or existing algorithm was modified.

## Outputs and test evidence

The manifest, as-of coverage row and panel schemas are deliberately explicit about absence. OPPORTUNITY_PANEL.csv and HELDOUT_OUTCOMES.csv contain headers only. The decision panel has 0 rows; the separate outcome table has 0 rows. No holdout outcomes were accessed or compared. Prior policy/data exposure of Jan–Sep 2026 remains **UNVERIFIED**, so it must not be called untouched or prospective.

Existing S1/S2/S3/S5/S6/S8, prefix and archive tests were run only on their repository fixtures. The scoped strategy/prefix command passed **111 tests** in 21.05 seconds. The broader attempted command including tests/data/test_archive_runtime.py had **111 passed and 4 failed** because pyarrow is missing in the existing environment. No dependency was installed or changed. These fixture tests are code-contract evidence only, not genuine Binance observations or historical strategy output.

No adapter or downloader was added. Consequently, there is no clean-clone archive refetch/regeneration proof, no pinned archive hash, no raw-to-feature-to-signal trace and no complete Sol replay command yet. The OPPORTUNITY_PANEL_SCHEMA.json and HELDOUT_OUTCOMES_SCHEMA.json are schema-only specifications, not populated datasets. Peak disk was not instrumented; no archive staging or extraction occurred. Bulk acquisition time was 0 minutes; USD spend was 0.

## Smallest factual remedy

Obtain an authoritative historical USD-M USDT-perpetual inventory manifest with official archive object keys and point-in-time listing, delisting, rename and contract/filter revision dates. This is required to measure unbiased coverage and S6 eligible breadth. Only after that source exists should a wider cohort be estimated against the 1 GiB / 4 GiB / 90-minute ceilings.

## Stop gate

No profitability, win-rate, Agent B uplift, actual-fill, provider-behavior or prospective-confirmation claim is supported. Session R-E1 stopped before bulk acquisition and before any outcome comparison. Independent ChatGPT review of the exact pushed research-only commit is required before Sol's evaluation session. No P2, authority change, merge, deployment, provider choice, capital activation or venue action is included.
