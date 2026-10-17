# Sol Session 2 reproduction protocol for R-E1B

This is a protocol for a later independent review, not permission to evaluate the package. Do not run a Sol evaluation until the exact E1B candidate and this handoff receive independent ChatGPT review and the owner authorizes that evaluation. No later result may alter the frozen cohort, periods, thresholds, features, or source calendar after looking at outcomes.

## Rebuild inputs

Use the recipe and checksums in `REPRODUCIBILITY.md`. Confirm the 35-symbol ordered cohort hash in `COHORT_AND_SPLITS.md`, UTC decision windows, `SOURCE_MANIFEST.jsonl`, corrected full panel SHA-256 `0cb67844a9a13110fba6628707d3be7a2be03ffa1fc156543b49ff8284e2f6cc`, coverage SHA-256 `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`, and the exact boundary convention. Repeated SETUP states with unchanged side/extreme share an originating setup ID/cutoff; a changed side/extreme starts a new origin. Four subsequent M15 slots belong to that origin, and incomplete prior opportunity history is marked unknown rather than asserting first-trigger status. Do not infer source availability from later archive presence. The sample remains diagnostic; corrected-code clean-clone parity is unverified, and independent acceptance is pending.

## Same-slot comparisons

### R1/R2 output contract at this candidate

For R1, compare the current `s1_precursor_state` and `s1_setup_event_*` columns separately from `s1_trigger_setup_*`. At an exact hourly boundary, the prior setup owns the final 15-minute opportunity; a new current H1 setup must not overwrite its direction. Retain all `s1_raw_trigger_predicate` matches but count only `s1_first_trigger_event` once per `s1_trigger_setup_id`. `SOURCE_MISSING` must remain visible for every absent current M15 row and for a missing/noncontiguous previous M15 bar during an active setup. Reconcile setup states, setup events, raw predicates, first events, and missing reasons per window and symbol. Repeated setup states and quarter-hour rows are not independent trades or hourly ranks.

For R2, archived actual receipt is `UNKNOWN_NOT_CAPTURED_AT_ACQUISITION`; do not construct actual receipt from a close, CSV mtime, or rebuild clock. `ResearchBar` is a research-only adapter and is not a `RawObservationV2` or production observation. The declared counterfactual default is bar close +5 seconds; +30/+60 are labeled sensitivities. Derived readiness adds at least one second after all dependencies. A bar closing at cutoff is not replay-visible then. Existing exact-close panel predicates are labeled zero-lag descriptive and must not be treated as delayed replay decisions.

Join independent calculations to the diagnostic panel by the exact `(window, symbol, decision_cutoff_utc_ns)` key. Preserve every expected calendar slot, including missing and excluded states. Compare the independently reconstructed S1 precursor state, regime, side/setup extreme, and M15 trigger state to the panel values at the same cutoff. S1 comparison must use only completed bars available before that cutoff; the March 1 00:00 UTC slot is a required boundary check using its completed February 28 23:45 M15 seed and no future bar.

For S6, compare the same fixed cohort and cutoff using exactly 720 continuous completed H1 log returns, the BTC-aligned beta/residual volatility, the latest four residual returns, the fixed H4 EMA context and descending score order. Record every `NOT_RANKABLE`, source-missing, BTC-anchor, and cohort exclusion. Separately document where the repository's complete policy eligibility/breadth contract requires unavailable funding/liquidity or point-in-time eligibility evidence; do not convert price-series rankability into eligibility or force a 20-symbol breadth assertion.

Report row-level mismatches, omitted slots and reason counts before any aggregate. No refit, threshold optimization, alternate cohort, lookahead repair, or outcome-conditioned filtering is allowed. A/B are historical diagnostic slices, not a prospective holdout.

## Gates and permitted claims

Evidence is reconstructed archive market data; original receipt times and contemporaneous source health are unavailable. Point-in-time eligibility, BBO/mark/index/events/funding/liquidity, trades, true fills, fees, operational execution, and costs are not demonstrated. Cost-adjusted performance and profitability remain `NOT ESTIMABLE`; no profitability claim is supported. Strategy-level opportunity and complete policy eligibility remain unverified. This protocol cannot authorize orders, deployment, capital, paid services, or operational-policy changes.
