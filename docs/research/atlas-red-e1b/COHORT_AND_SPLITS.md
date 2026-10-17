# R-E1B frozen diagnostic cohort and windows

## Fixed convenience cohort

The primary research cohort is frozen as the following 35 current-name USD-M USDT symbols, in this order:

`BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, AVAXUSDT, LINKUSDT, DOTUSDT, LTCUSDT, BCHUSDT, ETCUSDT, TRXUSDT, XLMUSDT, ATOMUSDT, NEARUSDT, APTUSDT, ARBUSDT, OPUSDT, FILUSDT, UNIUSDT, INJUSDT, SUIUSDT, SEIUSDT, RUNEUSDT, ICPUSDT, CRVUSDT, AAVEUSDT, LDOUSDT, FTMUSDT, MATICUSDT, EOSUSDT, ALGOUSDT, HBARUSDT`.

SHA-256 of the newline-delimited ordered symbol list (with a final newline): `b51f4a360e60bbb62fae982e4120ff3681b12a13ef8cd4a0aff01d761f6f03b5`.

BTC and ETH are context/reference anchors; the other 33 are the convenience altcoin set. The list was selected for this research run, not from point-in-time listing or eligibility evidence, a historical all-exchange inventory, or a random market-wide sample. It is vulnerable to survivor, current-name, and archive-availability bias. No missing archive is treated as proof of delisting. No substitute symbol was added after seeing availability or price behavior.

## Windows and acquisition periods

| Slice | Diagnostic decisions (UTC) | 1h archive period | 15m archive period |
|---|---|---|---|
| A | 2024-03-01 00:00 through 2024-06-30 23:59 | 2024-01 through 2024-06 | 2024-02 seed plus 2024-03 through 2024-06 |
| B | 2025-03-01 00:00 through 2025-06-30 23:59 | 2025-01 through 2025-06 | 2025-02 seed plus 2025-03 through 2025-06 |

Decision cutoffs are closed UTC boundaries in `[begin, final)`, including the first March 1 00:00 cutoff. Each expected native input slot opens one interval before its decision cutoff; the first M15 decision therefore uses the February 28 23:45 bar from the February seed, while bars closing at or after July 1 are excluded. `ASOF_COVERAGE.csv.gz` includes both `slot_open_utc_ns` and `decision_cutoff_utc_ns`, keyed on the same close-time calendar as the panel, so per-window M15 row and missingness counts reconcile exactly. The February files supply only the preceding bar needed for the first March 1 decision; February decision rows do not enter the evaluation panel. One February archive object is unavailable (recorded in `SOURCE_MANIFEST.jsonl`).

All expected native H1 and M15 slots in both declared windows are represented by `ASOF_COVERAGE.csv.gz`, including rows with no archive row. The feature panel contains the full native 15m decision-slot calendar for each cohort symbol. H1 archives include the preceding 2024/2025 January–February warmup. Four-hour bars are reconstructed only from four exact, contiguous, UTC-aligned closed H1 bars; reconstructed bars do not acquire genuine historical receive timestamps.

## Source and eligibility limits

Sources are official anonymous monthly USD-M public-data ZIP objects and sibling `.CHECKSUM` files from `data.binance.vision`. Their object bytes and published SHA-256 values were verified before CSV use. Rows are marked `RECONSTRUCTED_MARKET`: the archive was fetched in 2026 and does not reveal when a 2024/2025 consumer would actually have received the bar. Contract filter revisions, historical listing and delisting dates, symbol migration, point-in-time strategy eligibility, BBO, mark/index, events, funding, liquidity evidence, trades, fills, fees, and source-health receipts are not established by this panel.

The observed 404s are preserved as missing objects: H1 has seven missing objects (MATICUSDT 2025-01 through 2025-06 and EOSUSDT 2025-06); evaluation M15 has five (MATICUSDT 2025-03 through 2025-06 and EOSUSDT 2025-06); one M15 February seed object is missing. These are not interpreted as delisting evidence. A verified archive does not prove a tradable instrument was eligible then.

The S1 output uses a one-pass EMA cache and pointwise implementation of the repository pure `_regime` and `_setup` math on causal prefixes; deterministic synthetic and verified real-prefix tests compare it against those repository functions, and outputs are labeled `S1_BAR_PRECURSOR_DIAGNOSTIC`; the complete shadow coordinator is not called because the required original decision-time BBO, mark/index, event, source-health, contract/universe and receipt evidence is unavailable. S1 precursor and bar-trigger rows are not executable candidates, trades, fills, or profit observations.

The S6 output implements a transparent, price-only fixed-cohort adapter using the existing repository formula: aligned completed H1 log returns, 720-return BTC beta/residual volatility, latest four residual returns, and the existing 50-closed-H4 EMA20/EMA50 context, then descending score rank. It is labeled `S6_FIXED_COHORT_RANK_DIAGNOSTIC`; the repository's full rank-state coordinator is not called because funding/liquidity evidence and point-in-time strategy eligibility are absent. Rankable breadth is observed price-series breadth only, never the S6 strategy-eligible breadth of 20.

## Evaluation questions and no-peeking rules

This package contains features and opportunity/missingness states only. It does not contain a subsequent return, future label, event result, profitability estimate, optimized threshold, or selected winning combination. Keep A and B as historical diagnostic slices; B is not an untouched holdout. Do not use later bars to repair prior inputs, change cohort membership, or assign archive availability to an earlier consumer.

A later analysis may join frozen decisions to a separately defined, later-maturing outcome partition and compare forward price movement by frozen horizon, S1 precursor with and without S6 strata, BTC/ETH context, simple no-opening/price baselines, dependence and regimes, and conditional cost-stress ranges. Such joins are not exact-trade after-cost returns or evidence of genuine fills. Any profitability, complete-population mean, all-Binance inference, or trading eligibility claim remains unsupported without the missing source and venue evidence.
