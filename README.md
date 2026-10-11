# ATLAS — current project status

**Updated 2026-10-11.** This is the current status entry point. The latest checkpoint and detailed test evidence are in [`log.md`](log.md); historical S41 ledgers retain their original scope, results and hashes.

## Where the project stands

ATLAS is a capital-disabled public-market research and shadow system. The first owner operating profile is **one active public venue: Binance USD-M USDT perpetuals**. Bybit must remain inactive in that profile. Trading and assisted execution are disabled; no exchange credentials are needed for public-data research.

The V2 freeze also permits simultaneous **public research** on Bybit and Binance as a product capability. That capability does not require both venues to run in the first-test profile. Simultaneous multi-venue capital, automatic cross-venue routing and arbitrage remain deferred unless separately versioned and accepted.

| Area | Current status |
| --- | --- |
| Binance-only close-driven M15 acquisition profile | **IMPLEMENTED** as a versioned opt-in; disabled by default pending independent acceptance |
| Deterministic timely S1 opportunity path | **TESTED** with local production components: final M15, supporting evidence, durable causal publication, CandidateSet and deterministic selection |
| Existing S2 candidate/selector contract | **TESTED** in its focused deterministic regression suite; the new close-driven production fixture exercises S1, not a live S2 opportunity |
| Late or unavailable M15 evidence | **TESTED** to remain explicitly missing/late and produce no actionable event, including after restart |
| Real Binance delivery speed and opportunity coverage | **UNVERIFIED**; local fixtures do not measure exchange or network latency |
| Ordinary long-run storage admission / A2 capacity | **TEST GATE**; no accepted capacity certificate exists and `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` remains `None` |
| Capital and assisted execution | Disabled |
| Economic value | **NOT ESTIMABLE** |

The close-driven addition preserves continuous stream evidence, the ten-second broad-market scan, deterministic tiers, exploration, mandatory watches, final 1H/4H evidence, venue-final bar confirmation and the frozen five-second decision window. It does not establish that a real exchange response will meet that window or qualify a long-running shadow deployment.

## What remains before a useful shadow run

The immediate step is independent review of the bounded S44 source and offline evidence, followed by explicit acceptance of the opt-in profile. After that, a separately authorized, public-only Binance diagnostic must measure actual source latency, evidence completeness and resource use. Ordinary research startup remains fail-closed until its matching A2 storage/capacity admission is available. Native Windows, longer endurance, authenticated venue protection and prospective economic evidence are separate qualifications.

Do not start an ordinary run or treat the profile's presence in the UI/configuration as approval. S44 did not run an authorized Binance diagnostic, F1/F4/A2 capacity campaign, installer, merge or capital action. One unintended Binance public WebSocket connection during an early test-fixture attempt is disclosed in [`log.md`](log.md); it was terminated and no credential, account or trading path was used.

## Historical records

The [S41 handoff](SESSION041_HANDOFF.md), [live-test readiness record](SESSION041_FINAL_LIVE_TEST_READINESS.md), [engineering closure](SESSION041_FULL_V2_ENGINEERING_CLOSURE.md), [blocker report](SESSION041_REVIEW_BLOCKERS_AND_ISSUES.md), capability matrix and validation/requirement ledgers are historical evidence. Their S41 findings and failed dual-venue capacity reports remain unchanged. The machine-readable ledgers retain their original schemas and checkpoint claims; they are not current S44 acceptance records.

The file `ATLAS_CHATGPT_PROJECT_CURRENT_STATE_2026-10-04.md` was not present in this checkout. This README and the latest `log.md` entry provide replacement current-state text; no external ChatGPT Project attachment was edited.
