# ATLAS - Requirements, Non-Degradation and Acceptance Register

**16 October 2026 | All rows below are planned acceptance obligations, not claimed passes**

This register closes scope for the single finalized system. Every core row needs working implementation and a meaningful verification result. A stub, fixture-only integration, disabled adapter or renamed future backlog item is not completion. Financial promotion is separately evidence-gated; research authority can remain research authority without removing the delivered capability.

Read the algorithm for definitions and the implementation plan for exact session scope. Owners are Codex session responsibilities, not additional processes or subagents. All owning sessions except presentation-only S5 use GPT-6.1 Sol; S5 uses GPT-6 Luna and receives final Sol review in S6.

## 1. Evidence record required for every row

At implementation, retain: requirement ID; status; exact commit/build; source symbols; test/procedure names; data/provider/venue/instrument/profile identities; time range; command; result; sanitized artifact path; artifact SHA-256; evidence grade; limitations; blocker; and next bounded action. Reference a shared artifact rather than duplicating it. Machine records must agree with summaries.

Allowed status progression: PLANNED, IMPLEMENTED, FIXTURE_VERIFIED, INTEGRATION_VERIFIED, OPERATIONALLY_QUALIFIED, VALUE_QUALIFIED or BLOCKED. These are evidence states, not product versions. Do not skip a required real integration by reporting a fixture state as full completion. N/A needs an explicit scope explanation consistent with the finalized algorithm; it cannot erase a core requirement.

Evidence classes distinguish abstract contract, synthetic fixture, causal replay, genuine public data, real provider conformance, authenticated read-only, Demo order lifecycle, actual live execution and prospective economic evidence. None automatically substitutes for another. The original PDF's reported 61 abstract tests are not independently supplied/rerun evidence.

## 2. Quantitative decision and research core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| Q01 | Immutable evaluation set separate from final selection | S1 | Rank-one negative/rank-two positive/third unavailable fixture selects honestly, preserves population/refusals and legacy replay |
| Q02 | Exact action includes size, entry, partials, stop, exits, horizon and provenance | S1 | Change any bound field and old evaluation/approval fails; no fictional one-member selected-set workaround |
| Q03 | Mean-target M0, empirical payoff comparator and honest old location models | S1 | 99 times +1 and once -200 cannot pass positive-mean admission; old artifact cannot claim new target |
| Q04 | L2 LightGBM challenger and compact fit/inference separation | S1 | Real offline fit/inference pipeline and chronological comparison work; no LightGBM import/refit required by the safety owner |
| Q05 | Complete fees/funding/fill and programme accounting | S1/S2 | No/partial fills, both fee legs, funding interval boundaries, signed funding and slippage-double-count cases reconcile exactly |
| Q06 | Causal mature training, source grades, support/OOD and uncertainty | S1/S4 | Future/immature/revised/leaked/unsupported rows refused; selected-policy calibration cannot be replaced by payoff quantiles |
| Q07 | Locked whole-policy and agent/model comparison procedure | S1/S4/E1 | Same calendar blocks across symbols; registered search/holdout, bootstrap/support/multiplicity/cost-sensitivity rules reproduce |
| Q08 | Complete hold-versus-close early-exit research path | S1/S3/S4 | Future incremental paths/labels/tool request work; sunk loss cannot justify exit; unsupported capital exit stays blocked |

## 3. Strategy and model capability preservation/integration

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| ST01 | Preserve S1/S2/S3 exact causal policy behavior and management definitions | S1/S4 | Golden seeds/equality/expiry/latches/time exits reproduce; new evaluator accepts supported non-winner actions |
| ST02 | Integrate S4/S5/S6/S7 research/context with real required source readers | S1/S3/S4 | Correct chronology, breadth and source coverage; separate cascade continuation/reversal; OI alone never supplies direction |
| ST03 | Preserve S8, failed-break/range-fade and existing other-venue research | S1/S4 | Pair replay includes both-leg costs, frozen hedge, partial/orphan assumptions; no false single-action/native atomic authority |
| ML01 | Real TiRex-2 bridge | S1 interface/S4 runtime | Exact checkpoint/revision/hashes execute causal real inference in a supported environment; resource/cancel/output evidence retained |
| ML02 | Real Chronos-2-small bridge | S1 interface/S4 runtime | Exact `autogluon/chronos-2-small` artifact executes; horizon/quantile/covariate semantics and memory limits verified |
| ML03 | Real Kronos-mini plus correct tokenizer bridge | S1 interface/S4 runtime | Exact mini/2k-tokenizer pair executes; context, sampling, units and OHLC invariants tested |
| ML04 | Existing forecasts, analogues and state/volatility/hedge models remain usable | S1/S4 | Compatible empirical return forecast, causal neighbour/OOD, EWMA/ATR/HAR, forward state and S3/S8 diagnostics retain their defined roles |

ML01-ML03 do not pass from injected fixture callables. If a model/runtime/artifact is unavailable, record BLOCKED and do not claim complete model integration. Capital promotion needs separate incremental evidence. Serial or resource-rejected scheduling is not model deletion.

## 4. Shared account, risk and permission core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| AC01 | Actual USDT account truth and explicit mode/capability mapping | S2 | Unsupported/mismatched environment/mode refuses openings; no occupied-account mutation; stale truth cannot become zero |
| AC02 | Bot allocation distinct from whole-account equity and manual reserve | S2 | Deposit/manual profit does not increase bound ceiling; zero allocation/default OBSERVE produces no intent |
| AC03 | Reflection/uncertainty-aware reservations | S2 | Duplicates/reordering/lost ACK/delayed balance cannot double-count margin or release possible exposure |
| AC04 | Exclusive ownership and aggregate adoption/conflict table | S2 | Manual add/reduce/flat/reversal/order/stop changes at each boundary cause defined safe state with no fictional lots |
| AC05 | Concurrent protected positions through serial opening commits | S2/S6 | Two funded symbols coexist; competing headroom cannot overspend; dispatch ordering and declared maximum enforced |
| AC06 | Exact lot, margin, liquidation and joint-altcoin stress | S1/S2 | Minimum lot/rounding/tier/fee/funding/mark shock checked at final quantity; leverage cannot repair failed loss cash |
| AC07 | Loss-only rolling consumption and coherent PnL | S2 | Profits do not replenish loss allowance; fees/funding attributed once; duplicate closed outcome rejected |
| AC08 | Scoped approvals, revoke/pause/cancel/flatten/adopt | S2/S3 | Expired/changed/used nonce rejected; armed/UNKNOWN effects and sealed protection survive permission reduction |

## 5. Execution, protection and recovery core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| EX01 | One physical dispatch owner and durable-before-send journal | S2/S6 | Concurrent process/replacement and kill points cannot create two capable senders or an unrecorded opening |
| EX02 | Exact IOC/no-fill/partial behavior and deterministic identities | S2 | Duplicate/out-of-order events converge; rejected versus UNKNOWN send classification is distinct |
| EX03 | Native conditional algo/child/query/cancel and verified coverage | S2/S6 | Exact adapter/account stop manifest passes readback/side/quantity/trigger checks; ACK alone is insufficient |
| EX04 | Reactive protection gap explicitly measured/accepted | S2/S6 | Kill before/after possible fill and stop ACK/readback; healthy two-second goal measured, unavailable-host residual reported honestly |
| EX05 | TARGET_FLAT, mandatory exits and non-starved safety | S2/S4/S6 | Unknown remainder plus uncovered partial plus timeout still advances protection/close without reversal |
| EX06 | Dust and positive flat proof | S2/S6 | One flat snapshot/negative lookup cannot settle while future increasing fill remains possible; cancel only own obsolete protection |
| EX07 | Full restart reconciliation and no stale authority | S2/S6 | Restore ordinary/algo/child effects, reservations and latches; late approvals/directives never resurrect an expired opportunity |
| EX08 | Trusted-storage failure terminal policy | S2/S4/S6 | Exhaust reserve/corrupt copied DB/pin WAL: no unrecoverable new command, false settlement or automatic rearm |

## 6. Compulsory coordinator and typed evidence core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| AG01 | Complete typed production Brief with per-job dependencies | S3/S4 | Genuine complete analysis path; required evidence removal blocks; optional model absence does not falsify completeness |
| AG02 | One compulsory real API coordinator and bounded tools | S3/S6 | Exact Gemma route passes real bounded conformance; current tool-free synthetic ABSTAIN is not counted as a production job |
| AG03 | Useful workflow/watch/analysis/event decisions | S3/S4/E1 | Allowlisted requests execute/expire; claims map to trusted spans/assets/times; compare measured yield/errors/cost with deterministic control |
| AG04 | Conditional/direct directives and host numerical rule | S3 | One-use scope/epoch/TTL enforced; no self-sized or lower-rule-value action without verified eligible-set rebuild |
| AG05 | Budget/queue/identity/failure latches | S3/S4 | Worst-case usage reserved; lost response retains possible spend; outage/429/alias change invalidates entries; no paid fallback |
| AG06 | Durable memory, injection resistance and independent protection | S3/S6 | Restart-safe watches/jobs; untrusted instructions/fake refs rejected; hung provider cannot block stop/time tasks |

## 7. Runtime, data, storage and platform core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| RT01 | Actual periodic scanner and prospective denominator composition | S4 | Genuine scheduled slots/misses/restart dedupe; no historical empty interval added to live denominator |
| RT02 | Tiered broad observation and capital/due-work effort | S4 | All cheap eligible coverage plus fair shortlist; CAPITAL_WAIT/WATCH_ONLY preserve safety and useful watches |
| RT03 | Required source/clock/continuity/warmup correctness | S4 | Feed silence/gaps/reorder/revisions/sleep/clock jumps invalidate affected decisions; no backdated tradable opportunity |
| RT04 | Process memory, CPU, queue and worker isolation/admission | S4/S6 | Actual peak admitted workload meets targets; research/deep discovery shed before safety; no compulsory local LLM |
| RT05 | Byte partitions, physical reserve and bounded downloads/staging | S4/S6 | Quota and whole disk pressure handled; partitions sum to 2 GiB; runtime/install/model/cache/temp limits measured |
| RT06 | Pinning, replay grades, WAL/reader/export lifecycle | S4/S6 | Active evidence never evicted; failed export not archived; source deletion changes grade and invalidates dependent claims |
| RT07 | Reproducible dependencies, migration and patched runtime | S4/S6 | Exact lock/build hashes, linked SQLite fix identity, clean install/import/recover and old serialization tested |
| RT08 | Actual Windows/Linux 4 GB/Celeron qualification | S4/S5/S6 | At least 72 hours with startup, capture, UI/headless, model work, failures and retention rollover; absent host is BLOCKED |

## 8. Product and integrated release core

| ID | Required delivered behavior | Owner | Objective oracle |
| --- | --- | --- | --- |
| PD01 | Accurate account/ownership/protection/feasibility UI | S5/S6 | Zero lot, partial coverage, possible fill and conflict display cannot imply flat/protected/approved |
| PD02 | Typed commands and validated configuration | S2 semantics/S5 wiring/S6 review | UI calls existing validators; no control-table writes or disabled-limit-as-pass labels |
| PD03 | Bounded charts, reports, exports and operator recovery guidance | S5/S6 | Limits observed; programme versus venue/manual PnL distinguishable; no indefinite read transaction |
| PD04 | Exact-build platform packaging and headless mode | S4 recipe/S5 wiring/S6 review | Clean install/start/stop/recover and build identity on target platforms; no hidden model download |
| RL01 | Baseline capability/non-degradation closure | S1-S6 | All NG rows below preserved or deliberately corrected with a named test and evidence |
| RL02 | Genuine complete shadow workflow and cold start | S4/S6 | Collection works before mean qualification; source-to-agent artifacts persist and restart; unavailable economics never authorizes money |
| RL03 | Exact Demo lifecycle and compound incidents | S6 | Two concurrent positions and incidents below have authenticated venue evidence within authorization, not fake receipts |
| RL04 | Reproducible fixed prospective campaign | S1/S4/S6 | Full denominator, holdout/search ledger, costs and configuration/model/provider revisions retained |
| RL05 | Evidence-based capital decision | E1 | Operational gates, feasible account, residual-risk acceptance, permissions and preregistered programme-value/tail tests all pass or capital remains blocked |

## 9. No-degradation baseline checklist

| ID | Preserve or explicitly correct | Relevant existing seam | Required proof |
| --- | --- | --- | --- |
| NG01 | Frozen policy/action/indicator identities | Root freezes; strategies; features | Old golden artifacts/hashes reproduce; deliberate new bindings do not rewrite old records |
| NG02 | One writer, journal, approvals, UNKNOWN and no-reversal | runtime/; persistence/ | Saved-command crash/replay/cancel/fill cases remain valid |
| NG03 | Multiple research sleeves, pair research and other-venue comparators | full_strategy_surface.py; strategies/ | Existing role/authority retained and accessible; no silent deletion for session count |
| NG04 | M0/M1 historical diagnostics, baselines and analogue machinery | science/; models/ | Old model outputs remain correctly labelled/readable; new mean artifacts cannot mix targets |
| NG05 | Causal data, watermarks, mature outcomes and source grades | data/; features/; outcome services | Availability/gap/maturity/grade tests and old saved readers pass |
| NG06 | Scanner exploration, latches and durable slot denominator | scanner/; watch memory | Fair coverage, dedupe, no repeated setup and missed-slot restart behavior persist |
| NG07 | Typed Brief, sealed gateway, memory and budget controls | research/; agent_intelligence/ | Legacy refusal/zero-authority fixtures stay; real new success path added separately |
| NG08 | Native stop/recovery/flat-certification boundaries | Binance adapters and lifecycle | No relaxation to ACK-only protection, naive retries or zero-snapshot flat proof |
| NG09 | Desktop/IPC and bounded offline/research tools | desktop/; product.py | Existing useful paths remain; headless is an additional operating mode, not lost functionality |
| NG10 | Historical failure/evidence records and actual calendar rule | log.md; R9 evidence; news/events.py | No erased failures or inflated totals; minus-30 inclusive/plus-15 exclusive blackout fixture passes |

Trading superiority is not an NG assertion. It requires RL05/Q07 forward evidence. Preserving code behavior cannot prove future profit, and refusing an unsupported action is not a regression simply because a less strict implementation might trade it.

## 10. Mandatory compound incident matrix

| ID | Injection | Objective invariant |
| --- | --- | --- |
| C01 | Partial IOC, process kill, provider unavailable, delayed account stream | No duplicate/reversal; all possible quantity reserved; uncovered interval visible; no automatic rearm |
| C02 | Manual add/reduce/reversal during pending timeout/stop change | Named ownership transition; no fictional lots/manual cancellation or wrong-side close |
| C03 | Unknown opener, rejected stop, mandatory exit, peak quotes | TARGET_FLAT/protection advances for known owned exposure; no waiting forever on unrelated unknown |
| C04 | Physical disk fill, pinned reader, WAL growth, export failure | Active journal/evidence preserved; reserve policy applied; failed durability cannot authorize an unrecoverable opening |
| C05 | Correlated altcoin gap, spread/funding shock, manual margin consumption | Account-wide actual/possible risk retained; stop trigger never represented as guaranteed loss bound |
| C06 | Provider 429/timeout, model identity change, late approval | Entry directives invalidated; possible spend retained; no paid fallback or expired authority |
| C07 | Feed sequence gap, laptop suspend, clock jump and process restart | Resync/recover, expire missed triggers, preserve prospective denominator and setup latch |
| C08 | Old host paused, replacement starts, old host resumes | At most one physically capable dispatcher; absent fencing proof means no takeover |
| C09 | Two funded positions plus a third competing candidate and external withdrawal | Serial generation/revalidation prevents overspend; existing protection remains prioritized |
| C10 | Optional model memory spike plus slow database and source burst | Terminate/shed optional work within envelope; safety queue/timers retain declared healthy-path service |

Run component recovery in different orders and repeat key crash points. A provider recovery cannot clear storage/account/protection latches. Record exact build, injection and observed limits. Host/venue unavailability is not a healthy-deadline pass.

## 11. Regression and release interpretation

Use focused tests at each changed seam, then required broad checks in S6. Do not rerun expensive full suites without a reason after a validated unchanged build. Include relevant historical fixtures and saved schemas; preserve failures with exact reproducibility notes.

R9's three timing/resource failures and 53 baseline mypy diagnostics are not waived. Reproduce relevant failures, fix active-path causes and report remaining legacy-profile limits separately. An unused high-load profile that still fails cannot be called green; it also does not justify lying about the admitted complete-system workload. Every claimed active path must pass its own exact service and safety envelope.

Release claims must state: implemented scope; fixture/integration/venue/provider/host evidence; actual risk permissions; economic status; unqualified capabilities; and exact remaining blockers. No 'all done' report is allowed while a core row is a stub or a mandatory real integration is absent. No 'most profitable' claim is allowed without appropriate comparative evidence, and no test can prove universal future superiority.
