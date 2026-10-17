# Sol Session 2 reproduction protocol for R-E1B

This is a protocol for a later independent review, not permission to evaluate the package. Do not run a Sol evaluation until the exact E1B candidate and this handoff receive independent ChatGPT review and the owner authorizes that evaluation. No later result may alter the frozen cohort, periods, thresholds, features, or source calendar after looking at outcomes.

## Rebuild inputs

Use the clean-clone recipe and checksums in `REPRODUCIBILITY.md`. Confirm the 35-symbol ordered cohort hash in `COHORT_AND_SPLITS.md`, UTC decision windows, `SOURCE_MANIFEST.jsonl`, full panel SHA-256 `d3955dcf2fdb42bed5b7ba3b8430e20d662405ec6cb1dfa6c20e1f718bef0cae`, coverage SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`, and the exact boundary convention. Do not infer source availability from later archive presence.

## Same-slot comparisons

Join independent calculations to the diagnostic panel by the exact `(window, symbol, decision_cutoff_utc_ns)` key. Preserve every expected calendar slot, including missing and excluded states. Compare the independently reconstructed S1 precursor state, regime, side/setup extreme, and M15 trigger state to the panel values at the same cutoff. S1 comparison must use only completed bars available before that cutoff; the March 1 00:00 UTC slot is a required boundary check using its completed February 28 23:45 M15 seed and no future bar.

For S6, compare the same fixed cohort and cutoff using exactly 720 continuous completed H1 log returns, the BTC-aligned beta/residual volatility, the latest four residual returns, the fixed H4 EMA context and descending score order. Record every `NOT_RANKABLE`, source-missing, BTC-anchor, and cohort exclusion. Separately document where the repository's complete policy eligibility/breadth contract requires unavailable funding/liquidity or point-in-time eligibility evidence; do not convert price-series rankability into eligibility or force a 20-symbol breadth assertion.

Report row-level mismatches, omitted slots and reason counts before any aggregate. No refit, threshold optimization, alternate cohort, lookahead repair, or outcome-conditioned filtering is allowed. A/B are historical diagnostic slices, not a prospective holdout.

## Gates and permitted claims

Evidence is reconstructed archive market data; original receipt times and contemporaneous source health are unavailable. Point-in-time eligibility, BBO/mark/index/events/funding/liquidity, trades, true fills, fees, operational execution, and costs are not demonstrated. Cost-adjusted performance and profitability remain `NOT ESTIMABLE`; no profitability claim is supported. Strategy-level opportunity and complete policy eligibility remain unverified. This protocol cannot authorize orders, deployment, capital, paid services, or operational-policy changes.
