# R-E1B independent scientific reproduction — v1

2026-10-18. Research candidate; independent exact-SHA review pending. OBSERVE, zero bot allocation, capital NO-GO.

## Identity and authority

Repository: `KasunKarandagolla/Atlas`. Input branch `fix/atlas-red-e1b-first-trigger-censoring-2026-10-18`, exact reviewed input `d339d845a417e36b9a83c83b4f366e74fd207474`, parent/producer `04f05ab641457ab6deee8456da2c2a77d84f5149`. Fresh remote discovery found no later E1B correction or contradictory acceptance. The default branch is historical and was not used as current truth. Work is isolated on `research/atlas-red-e1b-sol-reproduction-2026-10-18`.

Before new scientific calculations, administrative commit `eef20fd4379b15e06c7436f7f7cfdd2d72f884e9` was published and independently checked against the remote branch. Its [versioned external acceptance record](../../project/ACCEPTANCE_E1B_D339D845_2026-10-18.md) records the owner-supplied **PASS — scoped E1B engineering review**, exactly for `d339d845`, with all supplied limitations. Codex is the recorder, not that accepting reviewer. This scientific candidate is not self-accepted, and new discrepancies are not a retrospective expansion or retraction of the external engineering verdict.

The accepted design remains `f9f8ef083ba5bafcad9bab4d07227676200a948f`; separately reviewed P1 foundation `f77f3aacbfdc2546e6f3ea2d78196b0905ceb613` is not P2 authorization. Canonical design, applicable decisions/requirements/plan/change map, acceptance register and authority reconciliation were inspected. The three freezes were read in full and verified:

| Freeze | SHA-256 |
|---|---|
| V1 | `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c` |
| V2 | `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78` |
| Agent | `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d` |

Their Git object identities and bytes, production code, finalized trading design, policies and dependency files remain unchanged. Original private source packets indexed by the authority manifest are unavailable verbatim; no claim is made to have read them. Owner-selected **GPT-6.1 Sol**; independent runtime model/effort metadata is unavailable. No subagents, model generation, paid/private venue calls, orders or outcomes were used.

## Inputs and source integrity

The [current protocol](SOL_SESSION2_REPRODUCTION.md) pins all five reviewed artifacts and the ordered 35-symbol cohort. All five SHA-256 checks passed, including the existing external full panel, without substituting or regenerating it. A/B cutoff intervals are `[2024-03-01, 2024-07-01)` and `[2025-03-01, 2025-07-01)` UTC, with January–February H1/H4 warmup and February M15 seed. The first A M15 open is **February 29 23:45**, not February 28; B uses February 28. Historical generic leap-year wording is superseded by the current protocol and native timestamps.

The existing cache retained extracted CSVs but no ZIP payloads. `verify_sol_sources.py` therefore performed one bounded anonymous HTTPS refetch of the exact pinned set: **757 ZIPs / 55,549,936 payload bytes**, sibling official checksums, pinned hashes, ZIP member identity/CRC and exact cached-CSV byte binding. All passed. The 13 pinned missing objects' sibling checksum URLs again returned 404; payloads were not retried for those missing entries. The original manifest's 13 official payload 404s and 420 historical parser-rejected attempts remain intact. Rejected-attempt bytes and total transport overhead are unknown.

The source proof contains 770 stable records, SHA-256 `65b973415cd3fc8131312b3f80e3217aeb538230b9a16c2efbcbedb0e7a8d013`. A second run reused all 757 retained ZIP/checksum pairs, checked the CSVs again, rechecked 13 small checksum endpoints, and reproduced that proof byte-for-byte. Current 2026 acquisition timing is recorded separately; it is never assigned to 2024/2025 receipt. No expanded historical universe or extra periods were fetched.

## Independent method and comparison contract

`independent_sol_reproduction.py` imports no builder, acquisition, summarizer or production mathematics. It shares only the frozen S1 policy identity. It independently parses verified CSVs, constructs exact four-H1 H4 bars, reconstructs stable source fingerprints, computes seeded EMA recurrences, regime/pullback/reclaim, setup continuity/IDs, native four-opportunity ownership, strict Decimal triggers and causal per-episode first-event censoring. Pure production S1 functions are a separately identified test comparator, not the oracle. No coordinator or action is executed.

The **primary** S1 oracle uses each declared contiguous UTC-year prefix, seeded from its first 20/50 completed bars, and requires the terminal H1/H4 close expected at the cutoff. No missing prefix can fall back to an older terminal bar. A separately labeled **secondary observed-history calculation** reproduces the producer's concatenated observed-bar EMA seeds and permissive stale terminal. It explains differences; it does not replace the primary result or claim that disconnected observations are complete market history. Neither convention establishes the unobserved July–December history or an original production EMA checkpoint.

S6 uses a different numerical path: independently aligned adjacent H1 log returns, centered covariance/variance, explicit residuals and sample standard deviation (`ddof=1`), the last four residual sum divided by residual volatility, independent last-50-H4 EMA context and deterministic descending rank/base-asset tie order. BTC is the excluded proxy; ETH remains context and a diagnostic rank participant, never an inferred eligible altcoin action. A canonical-instrument-key tie test checks the simplified key for this exact cohort. Missing/zero-variance/zero-residual inputs remain excluded. The production neutral-trend label `MIXED` and the adapter's `NEUTRAL` are distinct declared vocabularies.

Predeclared absolute/relative tolerances are recorded in the result JSON: score `1e-9/1e-9`, beta `1e-11/1e-10`, residual volatility `1e-12/1e-10`, H1 EMAs `1e-10/2e-12`, returns `1e-14/1e-12`. The centered 720-input calculation is checked against a 50-digit Decimal oracle within `2e-14 * max(1, |value|)`. The producer uses differences of accumulated sums/products, whose float64 reduction and cancellation errors grow with prefix length; the predeclared bounds allow that numerical path and serialized floats. Exact discrete state/rank/tie comparisons have no tolerance. Seed changes, stale history and exclusions are not numerical errors to be tolerated away. No bound was increased after examining real results.

The full panel is streamed, split into temporary per-symbol files, and joined on `(window, symbol, decision_cutoff_utc_ns)` with exact native ordering, duplicate/future-key rejection and all expected rows retained. Only the compact S6 hourly dictionary and one symbol's source history/episodes are held in memory. Coverage is independently joined to native source rows, including missing reasons, archive hashes, row indices and timestamp grade; the complete 6,720-row compact sample also matches its full-panel rows exactly.

## Q1 — S1: partial reproduction; scientific TEST GATE

Both windows have **409,920 same-slot comparisons**, 819,840 total. The secondary observed-history oracle matches every compared S1 field. Primary complete-prefix results:

| Window | Primary rows with any mismatch | Verified first events | Unique setups | Complete no-match | Unresolved | Censored |
|---|---:|---:|---:|---:|---:|---:|
| A | 0 | 9,261 | 15,062 | 5,800 | 1 | 0 |
| B | 15,604 | 7,899 | 13,126 | 5,225 | 0 | 2 |

The setup partitions independently reconcile to the panel and the reference totals for all 70 symbol/window pairs. Three pre-window nonmatches plus one observed boundary opportunity correctly make a complete four-opportunity no-match; an initial audit version omitted those certified pre-window observations from its partition, was corrected in the independent validator, and is protected by a focused regression. The producer and dataset were not changed.

**D1 — EOSUSDT stale terminal prefix: 3,892 B rows.** Starting **2025-05-21 11:00 UTC** (`1747825200000000000`), the last observed H1 ends at 10:00 and the last H4 at 08:00. The producer's continuity test accepts the old prefix even though its terminal close is no longer current, retaining SETUP/SHORT and old H1 source identity. The primary oracle returns SOURCE_MISSING. H1 physical status/hash/index/reason disagree on these 3,892 repeated M15 rows; old trigger ownership/status metadata also differs on 3,891 rows. Source archive SHA is `3adc7f5a4b8ecdb87541f86da858d4c41190cbf5d215b50122875b7f5b8f39b4`, last H1 physical member row 490. This is missing-data/terminal-prefix/identity handling, not a tolerance issue. No verified first event differs; the two EOS setups remain censored. No missing bar is repaired.

**D2 — FTMUSDT disconnected H4 EMA seed: 11,712 B rows.** At the first B cutoff the H4 close is `0.770200`; the independent year-prefix EMA50 is `0.7702001291867097`, while the secondary concatenated-history value is `0.7701999452750369`. EMA20 is `0.7702000000000006` in both. Thus the primary H4 label is DOWN and the producer label UP. All 2,928 observed B H1 rows are flat OHLC with zero volume. The tiny seed-dependent distance later persists at float precision; directional labels are fragile on this stagnant series. No SETUP/event/first-trigger count changes. The first-boundary H1 archive is `cdbfade7e23820189f26ae837c9cebfdb12ceb0aeed1d2c5e238ded86a8f407f`, member row 672. This is an unresolved seed/history-convention scientific discrepancy; disconnected 2024→2025 observations cannot prove a complete causal EMA history. No epsilon or new strategy rule is introduced.

**D3 — shared BTC/ETH zero returns serialized as missing: 140 A rows and 280 B rows.** ETH at 2024-05-15 23:00 UTC, ETH at 2025-05-01 19:00 UTC and BTC at 2025-05-26 02:00 UTC have genuine zero H1 close returns. The producer uses `value or ""` for shared anchor fields and erases each zero across all 35 symbols and four M15 cutoffs. The independent anchor reconstruction preserves numeric zero. Exact examples include both relevant anchor archive hashes, member rows and cutoff, in addition to each panel row's own source hashes. This is output missingness/serialization, not unavailable source or numerical tolerance. Shared H4 anchor trends agree; no S6 score/rank or first-event count changes. No producer correction is made here.

The detailed result records every field's comparison and mismatch count, per-symbol affected rows, numerical maxima, exact keys and bounded examples with source hashes. The 15,604 H4-label discrepancies comprise disjoint FTM 11,712 and EOS 3,892 rows. All valid compared H1 EMA/return numeric values match exactly. All raw predicates, setup events, unique first events and first-event IDs match. All **15,610 missing current-M15 B rows** are retained; physical M15 status/reasons/identities and coverage/source joins have zero mismatches.

Do not interpret the zero first-event discrepancy as complete S1 field agreement. D1 and D3 require separately reviewed producer dispositions; D2 needs an explicit scientific seed/history disposition. Full S1 causal-prefix fidelity is not certified here.

## Q2 — S6: reproduced price-only adapter

The comparison covers **204,960 unique hourly symbol slots**, repeated across all 819,840 native M15 panel rows, including anchors/exclusions. There are **192,275 scored hourly symbol slots** (769,100 repeated panel rows), 5,856 BTC anchor slots and 6,829 unscored non-BTC slots. There are zero mismatches in score, beta, residual volatility, H4 trend label, rank, decile, rankable breadth, anchor/exclusion state or reason.

Across all 199,104 non-BTC hourly input windows, a separate subject-versus-independent input test compares every complete 720-return array and latest four inputs: 195,203 complete and 3,901 incomplete windows, **281,092,320 return values**, 1,561,624 latest-four values, maximum log-return difference `1.3877787807814457e-17`. Complete inputs do not alone imply rankability: the flat FTM series contributes 2,928 zero-residual exclusions. Arrays are not serialized in the panel; this separately identified source-prefix test compares the frozen producer's input construction to the independent grid. The principal oracle remains independent of producer functions.

Maximum score/beta/volatility absolute differences are respectively **`1.581401676276073e-12` / `1.5631940186722204e-13` / `8.482797797526587e-16`**. Maximum score relative error is `1.2101987223862396e-09` near zero; the predeclared absolute-plus-relative rule handles that without changing ranks or tolerances. BTC/ETH shared H1-return/H4-context fields are also checked at every panel row; their separate D3 zero-versus-missing discrepancies are not included in the matching S6 calculation fields. All source H1 logs use only adjacent completed bars; H4 groups use exactly four native closes and as-of selection. No future observation fills a gap.

This result grants no full S6 strategy or signal-production authority. Point-in-time funding/liquidity/contract/universe and other predicates are absent. The fixed cohort's price-series breadth cannot establish the strategy's 20-eligible-symbol breadth gate.

## Q3 — chronology and data grade

All compared historical receipt fields are empty/`UNKNOWN_NOT_CAPTURED_AT_ACQUISITION`; grade is **RECONSTRUCTED_MARKET**. Default raw visibility is T+5 seconds and derived readiness at least T+6. Sensitivities are T+30/T+31 and T+60/T+61. All 804,230 observed current-M15 rows are unavailable at exact T under each assumption; all **17,160 verified first events** are therefore exact-close descriptive predicates, not executable decisions using information already received at T.

Native closes, January–February warmup, H4 alignment and the 70 first-boundary source proofs are explicit. At a fresh H1/H4 boundary the completed context also waits for its required replay-visible dependencies. At intermediate M15 boundaries the current M15 is the latest dependency. Missing required dependencies cannot become available merely because a timestamp exists. The audit found zero observed rows with a populated feature-ready timestamp but an invalid primary required S1 prefix; missing EOS/MATIC rows have no current-M15 readiness.

The frozen S1 policy's candidate deadline and confirmed-bar allowance are T+5 seconds. Even the assumed +5-second raw lag plus one second of processing yields T+6, too late for that contract; +30/+60 are later still. This is a replay feasibility conflict for these exact-close observations, not measured historical latency or proof that genuine live collection can/cannot meet the contract. Deadlines and algorithms remain unchanged. No opportunity is promoted into an executable trade.

## Q4/Q5 — information roles and economic feasibility

The reproduced first-event/price-decile cross-tab is descriptive. A has 1,101 TOP / 6,839 MIDDLE / 1,010 BOTTOM first events plus 311 BTC-anchor events; B has 1,000 / 5,584 / 1,025 plus 290 BTC-anchor events. ETH observations are included in diagnostic ranks, not eligible altcoin actions. These counts show that the two information roles are distinct contemporaneous descriptions. They measure neither predictive improvement nor after-cost value, and have no complete eligible-action denominator.

[The feasibility report](SOL_ECONOMIC_EVIDENCE_FEASIBILITY.md) provides the explicit evidence/identifiability table and a predeclared smallest prospective integrated campaign. Missing point-in-time eligibility, original receipts/source health, BBO/mark/index/depth, funding/events/filters, exact actions/independent sizing, all S1–S8/Agent B decisions, qualified execution/costs and matched prospective programme outcomes prevent economic identification. More OHLC data alone does not change that. No forward returns, outcome labels, thresholds, combinations, profitability comparisons or simulated missing evidence were generated.

## Execution evidence and unresolved gates

[SOL_EXECUTION_EVIDENCE.json](SOL_EXECUTION_EVIDENCE.json) records exact commands, environment, actual counts/failures/skips and resources. [SOL_ARTIFACTS.sha256](SOL_ARTIFACTS.sha256) pins the code/tests/source proof/results/reports. Deterministic whole-calendar results and mismatch examples are reproduced in two clean scratch runs of the final utility; source proof reuse also reproduces identical bytes. The full repository suite is not claimed.

- **TEST GATE:** D1/D2 S1 context/prefix fidelity and D3 shared zero-return missingness; independently review and decide disposition before dependent research. This session leaves the producer unchanged.
- **BLOCKED BY ENVIRONMENT:** V1 golden baseline requires absent `nautilus_trader`; the same test fails at reviewed parent `d339d845`. Other executed impacted tests are recorded separately.
- **UNVERIFIED:** independent runtime model/effort; full locked production environment qualification (the research oracle uses existing NumPy 2.3.5, while `uv.lock` pins 2.5.3); unavailable private originals and genuine historical receipt/eligibility/execution/Agent B evidence. Installed-package compatibility is not a complete lock realization.
- **NOT ESTIMABLE:** executable trade counts and complete programme after-cost returns, predictive/economic contributions and profitability from E1B.
- **REVIEW PENDING:** this entire new research candidate, including administrative synchronization, tests, source checks, mathematical findings and study proposal. No auto-merge, P2, provider evaluation, activation, capital, deployment or trading follows this push.

Next reviewer: discover the exact final remote branch SHA reported in delivery; verify its parent `eef20fd`, ancestry through `d339d845`/`04f05ab`, complete reviewed-input-to-candidate diff, freeze identities, independent oracle and focused tests, source proofs/panel pins, all D1–D3 mismatch classifications and honest gate boundaries. Rerun feasible checks and state separately what was executed versus examined as operator evidence. Record a new exact-SHA verdict; do not self-accept or infer economic readiness from this package.
