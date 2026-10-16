# ATLAS - Final System Design

**Owner-approved design direction | 16 October 2026 | Documentation, not a capital release**

One complete trading and research system for Binance USDT perpetuals, a shared manual account, broad altcoin observation, concurrent positions, a compulsory hosted API coordinator, and a 4 GB Celeron-class Windows/Linux laptop.

**Objective:** maximize validated programme profit within explicit capital, tail-risk, permission, reliability and resource constraints. Intelligence is useful only when it improves measured decisions, execution, research or operating efficiency. No architecture or model can guarantee maximum profitability or superiority in every market.

**Current status:** design finalized for implementation; runtime implementation and empirical qualification remain outstanding. Live capital is blocked on the evidence reviewed. This document records decisions and acceptance obligations, not tests passed by the new system.

**Code baseline:** `KasunKarandagolla/Atlas`, branch `fix/red-r9-local-complete-cycle-2026-10-14`, commit `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`. Its commit time is 15 October 2026, 01:12:22 Asia/Colombo. Do not start from the older repository default branch.

## 1. One complete system and a non-degradation contract

There is one target system. There is no weaker MVP, lite product, second OMS or sequence of product versions that moves core requirements into a later project. The six engineering sessions are work partitions of this same system. All core requirements in `REQUIREMENTS_AND_ACCEPTANCE.md` must be implemented and verified before engineering completion is claimed.

Implementation, evidence and capital authority are different facts. An implemented model may be unsuitable for a particular machine or unqualified for money. A strategy may have a complete research workflow but lack evidence for capital admission. These outcomes are visible capability states, not a promise to implement a missing feature later. A registry entry, injected fake callable or permanent unavailable stub does not complete a required integration.

Preserve every working baseline capability at its established authority: existing strategy research, causal indicators, model isolation, empirical forecasts, analogue retrieval, economic/scenario primitives, memory, scanner exploration, outcome maturity, UI/IPC, single-writer execution, approvals, journal/outbox, UNKNOWN handling, no-reversal logic, protection, reconciliation and recovery. Preserve existing non-Binance adapters and historical readers without expanding the active venue scope.

Do not overwrite frozen historical policies or reinterpret their saved artifacts. Necessary schema identifiers, hashes, model revisions and migration records remain because replay and safe recovery require them. They do not create competing product versions. Historical zero-authority agent records stay zero-authority; the finalized authority applies only through explicit new policy bindings and qualified dispatch checks.

A change must preserve relevant golden behavior or identify a deliberate correction, its reason and its test. Removing a capability requires proof that no active caller, saved record, required workflow or acceptance row needs it. Reducing discovery load during pressure is allowed; reducing protection or silently dropping a core feature is not.

## 2. What the baseline actually establishes

The source audit covered the complete repository tree, the three governing freezes, 98 selected source files and relevant tests/evidence. It was not a fresh full-suite or exchange certification run. R9 provides substantial implementation to reuse, including multiple research sleeves and durable scanner accounting.

R9's connected production fixture terminates safely with a blocked Brief, unavailable economics and no agent/order intent. The production Brief has incomplete typed readers and explicit guards against false completeness. The real Gemma request was a synthetic ABSTAIN smoke. Public REST/WebSocket probes did not establish a durable complete production chain. No order was submitted in the recorded R9 qualification. [R1]

The recorded partitions observed 2,925 unique cases after a later focused test addition: 2,914 passed, eight skipped, three resource/timing failures. The complete suite was not rerun after that addition. R9 also records 53 mypy diagnostics matching its R8 baseline. Neither count is a current clean certification. Its Celeron storage probes exceeded the 200 ms batch service gate, and full breadth remained unqualified. [R2]

No positive after-cost edge, real native protection lifecycle or minimum-host sustained operation is established by this design. The original attachment's reported 61 abstract tests were not supplied as an executable package and are not counted as new-system evidence.

## 3. Architecture and ownership

Keep a local application with clear owners rather than introducing distributed services.

| Owner | Responsibilities | Forbidden authority |
| --- | --- | --- |
| Data/ops owner | Causal data, universe, features, source health, scanner, watches, evidence, research jobs and retention | Exchange commands and financial permission changes |
| Numerical evaluator | Exact-action costs, mean/support/OOD, uncertainty, scenarios, incremental portfolio comparisons | Provider selection by favourable prediction or direct order submission |
| Execution owner | Account truth, ownership, permissions, allocations, reservations, durable commands, native protection and recovery | LLM-directed bypasses or unjournaled opening commands |
| Hosted coordinator | Bounded workflow/watch selection, registered analysis requests, verified evidence interpretation, eligible-action decision and qualified exit request | Credentials, invented risk numbers, arbitrary tools, policy changes or direct exchange access |
| Research worker | Mature labels, model fitting, calibration, forecaster inference and experiment reports | Live control-database writes, automatic promotion or exchange credentials |
| Desktop/IPC | Explain state; submit typed user commands and approvals | Mutating execution tables or treating display values as account truth |

Use one credential-bearing exchange writer, the existing control journal, a separate ops/evidence ownership boundary, one bounded numerical worker and one hosted coordinator route. The numerical and provider work must be cancellable and unable to monopolize safety timers, connections or rate budget. Do not introduce Redis, Kafka, a vector database, another OMS or an agent-debate framework.

## 4. State, identities and contract boundaries

Use full instrument identity: venue, environment, contract/settlement identity, symbol and instrument revision. Bind fee/filter/funding revisions, policy/configuration hashes, feature/model manifests, evidence availability, account generation, ownership epoch and plan identity where relevant.

State dimensions are orthogonal: account health, ownership, permission, setup episode, command/order state, actual quantity, possible quantity, protection coverage and exit goal. A filled position can coexist with an unknown opening remainder and insufficient protection. A flat snapshot can coexist with possible future fills. One generic RUNNING/STOPPED enum cannot express these states.

The decision contracts are:

1. `CandidateEvaluationSet`: one immutable due-slot population, deterministic candidate cap and inclusion/refusal records. No final winner is required to value a member.
2. `ExactAction`: complete entry/partial-fill/stop/exit policy, quantity, risk envelope, information cutoff and provenance. Any material change produces a new bound action.
3. `ActionEvaluation`: exact action, compatible mean model, costs, support/OOD, selected-policy uncertainty allowances, scenarios and status.
4. `SelectionDecision`: a separate result bound to the whole comparable set, the registered rule and a valid coordinator directive.
5. `CapitalPlan`: selected exact action plus current account/ownership/permission/capability bindings, expiry and one-use nonce.

Update the real production callers, feature projection, sizing, M0/M1, evaluator, Brief and outcomes together. Do not create a different one-candidate 'selected' set for every alternative. Scanner rank remains an origin-known feature if used; final economic winner rank must not leak back into its own value features.

## 5. Market data, time and broad coverage

Observe eligible current Binance USDT perpetuals cheaply through metadata, ticker/mark and source-health summaries. Reconstruct point-in-time listing/delisting, contract revisions and filter changes for research. Capital eligibility additionally requires qualified liquidity, price, fee, funding and account evidence.

Use three workload tiers: cheap universe observation; warm bar/feature histories; deep trade/book evidence. The starting resource contract allows approximately 20 warm symbols, at most three fully evaluated alternatives per decision packet and two deep discovery books, plus required owned-position evidence. These are configurable admitted-workload limits, not a claim that every symbol receives every model. Owned-position evidence has priority; the maximum position count must itself be resource-qualified.

Retain causal completed bars, exact indicator seed/index/equality behavior and golden fixtures. Track source event time, receive time, available-at time, persisted-at time, sequence continuity, final-bar status and per-channel watermarks separately. Backfill cannot recreate the original opportunity deadline or missing order-book evidence. Reject future-available data, silent stale channels and invalid/nonfinite records without replacing last valid exposure with zero.

Synchronize books from the required snapshot and stream sequence. Drop/reorder/gap detection must invalidate dependent features until resynchronization. Use UTC for venue candles, event/funding calendars and research splits; use monotonic time for elapsed deadlines. Suspend/resume, large clock changes and offline intervals enter recovery and expire stale opportunities.

Preserve rotating exploration and known inclusion probabilities. Persist expected slots, no-candidate, denied, missed, resource-not-evaluated and unavailable states. No historical interval is retroactively counted as prospectively scheduled. Backfill and model work are bounded by range, rows, bytes, time and temporary storage.

## 6. Strategy surface and management capabilities

All existing strategy families remain part of the complete system. Their role is explicit, and no research-only capability is relabelled capital-ready merely to claim completion.

| Family | Complete-system duty | Capital boundary |
| --- | --- | --- |
| S1 trend pullback | Preserve causal setup/action definitions and four-hour baseline time exit; generate and evaluate exact actions | Full evidence, execution and economic gates |
| S2 compression breakout | Preserve setup latch, first-two-bar failure behavior and two-hour baseline timeout | Same gates; no repeated breakout attempts from a model restart |
| S3 VWAP/AR(1) reversion | Integrate native minute/trade evidence, reversion/half-life diagnostics, costs and exact-action evaluation | No claim of microsecond scalping; required source/latency evidence must qualify |
| S4 order flow | Complete book/trade-derived entry-quality, absorption and execution-cost context | Context is not an independent directional order without its own complete action contract |
| S5 crowding/cascade | Keep continuation and post-cascade reversal separate; integrate OI, funding, liquidation coverage and confirmation chronology | OI alone supplies no direction; censored coverage cannot become a passed gate |
| S6 relative strength | Preserve broad cross-sectional and peer context; bind breadth/history and any exact action to the contemporaneous universe | No ranking from a biased or unavailable breadth population |
| S7 events | Verified source extraction, asset/time mapping, calendar coverage and scheduled blackouts; retain directional research | Direction requires its own executable policy and economic qualification |
| S8 pairs | Preserve synchronized two-leg research, frozen-beta basket simulation, both-leg fees/funding, partial/orphan risk and reports | Existing research authority remains. Live atomic/two-leg execution is not implied by ordinary concurrent single-symbol positions |

Complete the integration and research routes for these roles in this project. Do not call a missing core data reader or permanent exact-action stub a deferred feature. New standalone directional policies for context-only sleeves require an explicit policy definition; this document does not invent them by combining indicator votes.

Preserve failed-break and range-fade research as separate hypotheses. Keep macro blackout semantics distinct from calendar-coverage windows: the existing exclusion is event minus 30 minutes inclusive to plus 15 minutes exclusive. Correct inconsistent prose without silently changing the policy.

Implement the full position-review and early-exit comparison path, including labels, tooling, permissions and tests. Compare expected future wealth from holding versus closing now; realized sunk loss is not an exit justification. Fixed stops, mandatory structural exits and time exits remain deterministic. An agent-requested discretionary exit becomes executable only with qualified hold/exit economics and current authority. Otherwise its complete shadow outcome is recorded; the implementation is not postponed.

Pyramiding, financed replacement/rotation, live multi-leg execution and additional active venues are outside the agreed core trading policy. Existing research for them is preserved. They are not required to pretend that the core single-action policy is complete.

## 7. Model assignments and actual integrations

Models have distinct duties; they do not vote. One explicitly promoted estimator supplies the capital mean. The host never selects the most favourable output among M0, M1, analogues and foundations.

| Component | Assigned task | Implementation and authority |
| --- | --- | --- |
| Shrunk empirical net-action mean | Small support-cell payoff benchmark and calibration sanity check | Required working comparator; eligible for mean authority only after the same gates |
| M0_MEAN, squared-loss ridge | Lightweight action-conditioned conditional arithmetic mean of net cash payoff | Primary promotion candidate; fit/calibrate offline and use a compact immutable inference artifact |
| Existing M0 Huber | Preserve robust-location diagnostics, support machinery and historical replay | Never automatically labelled arithmetic expected profit |
| M1_MEAN, LightGBM L2 | Nonlinear challenger for the same net-action target and failure-slice analysis | Complete offline fit/inference/evaluation pipeline; old L1/MAE model remains a location diagnostic |
| Causal analogues | Compatible neighbours, support/OOD, explanations and diagnostic comparison | Bounded numeric retrieval; no independent vote or automatic override |
| EWMA/realized variance/ATR; HAR | Cheap volatility/risk context; HAR as a measured forecasting challenger | Causal features with explicit dependency and ablation evidence |
| Forward Kalman/robust slope; AR(1)/half-life | Trend/state features and S3 reversion eligibility | Forward-only estimates; no full-sample smoothing or universal alpha layer |
| Cointegration/Kalman hedge | S8 pair-state and hedge research | Preserve frozen-decision hedge semantics and both-leg economics |
| Hosted Gemma 4 26B A4B IT | Compulsory bounded coordinator | Exact route `gemma-4-26b-a4b-it`; no numeric risk authority |

The existing empirical horizon-log-return `StatisticalBaselineV2` remains a forecasting control. It is not the new empirical net-action payoff comparator.

All three selected foundation integrations are in engineering scope, including real inference bridges, causal input/output contracts, manifests, cancellation, memory limits, reproducible evidence and comparison reports. Injected fixture callables alone do not complete them.

| Checkpoint | Duty | Scheduling and qualification |
| --- | --- | --- |
| `NX-AI/TiRex-2` | General multivariate/covariate forecast and scenario challenger | Serial isolated inference; preserve as first general research comparator |
| `autogluon/chronos-2-small` | 28M probabilistic forecasting alternative | Serial comparison with TiRex; standard 120M `amazon/chronos-2` is not silently substituted |
| `NeoQuasar/Kronos-mini` with `NeoQuasar/Kronos-Tokenizer-2k` | 4.1M financial OHLCV forecast/scenario challenger | Pin tokenizer, sampling, context and OHLC units; validate financial path consistency |

Maintainer facts establish model identity and advertised capability, not crypto edge or 4 GB capacity. [E1-E3] Pin checkpoint revision, hashes, runtime, preprocessing and postprocessing. Each runner must execute real bounded inference in its supported research environment before its integration row passes. Measure Windows/Linux support separately; TiRex's package documentation currently states Linux/macOS testing. Do not add Docker as a compulsory 4 GB dependency.

The scheduler loads at most one optional numeric model at a time and never installs/downloads a framework on an entry or protection path. Local inference requires admitted measured capacity; a separately qualified numerical endpoint is an alternative, not a generic chat endpoint. If neither route qualifies, report the exact block and keep the mandatory completion row open. No claim of complete model integration may be based solely on disabled adapters.

Not running every model on every trade is a resource and statistical decision, not a weaker product. Promote a foundation output only as a fixed, tested feature or scenario input after it improves complete-policy value. A price quantile is neither a payoff-mean confidence interval nor a probability of profit.

## 8. Payoff targets, costs and comparison

For exact action `a`, define `Y(a)` as account wealth at a common 24-hour terminal convention with the sealed action minus wealth with no new opening, under coherent market/execution paths. Cash remains cash after a shorter strategy exits; do not assume automatic reinvestment. Portfolio comparisons use shared path IDs and include manual, actual, pending and possible exposure.

Cash payoff includes actual filled quantity, every entry/exit fee, signed funding, spread/impact/delay through actual or qualified modelled execution prices, and the complete stop/exit policy. Do not subtract slippage twice if prices already include it. No fill has zero trading payoff, with any genuinely incurred programme expense still recorded. Partial fill and unfilled remainder behavior are explicit. Cancel/replace, funding-boundary and dust outcomes cannot vanish from training.

Maintain three reconciled views: venue trading PnL; programme PnL after all incurred API/data/operating costs; whole-account equity including manual trading and deposits/withdrawals. Provider spend outside Binance is not a second deduction from exchange available USDT. A configured programme reserve can independently restrict bot allocation. Charge a shared analysis bill once, not to every hypothetical candidate. Sunk analysis expense is included in programme reporting; only additional future costs affect the current incremental decision.

Read actual account commission and symbol filters; preserve vintages and use conservative taker treatment for marketable IOC assumptions. Funding payment times/intervals, not a universal eight-hour assumption, determine exposure. Unknown required costs or price outside a qualified envelope cause refusal/revaluation.

The economic gate is `score = mean_net_cash - mean_selection_allowance - execution_bias_allowance - numerical_allowance - incremental_future_programme_cost`. Require a strictly positive score plus all non-economic gates. Components must have cash units, documented calibration and no double counting. The execution-bias allowance covers residual model bias, not costs already in the target. Numerical allowance covers approximation only. Do not call the score a confidence bound unless held-out coverage for that exact selected policy supports the claim.

One deterministic host rule selects maximum conservative score among comparable current eligible actions. Ties prefer lower stressed tail exposure, then lower reserved margin, then stable action ID. The agent may reject/abstain or identify a verifiable evidence problem; it cannot invent a preference for a lower-scored action under this rule. Verified problems rebuild the eligible set.

## 9. Training, uncertainty and profitability evidence

M0's robust center and M1's L1 output are not generally conditional means. The fixture with 99 payoffs of +1 and one of -200 must not produce positive-mean admission: arithmetic mean is -1.01. Squared loss addresses the estimand, not nonstationarity, label bias or tail scarcity. Genuine tail losses stay in the data; corrupt records are handled by documented data-quality rules.

Fit only mature labels whose outcomes were available before the fit cutoff. Separate `ACTUAL_RECONCILED`, `PROSPECTIVE_SHADOW`, `CAUSAL_REPLAY`, `DIAGNOSTIC` and `CENSORED` evidence. Counterfactual fills are estimates. A successful or no-fill unselected action is not an actual execution label. Preserve all attempted variants, missing states and the prospective denominator.

Use chronological train/calibrate/test periods, training-only preprocessing and feature selection, purging for actual label overlap and an explicit embargo. Preserve the historical 24-hour embargo for baseline reproduction. Different embargoes are preregistered experiments, not a silent way to add observations. Split all symbols on the same calendar boundaries and challenge new listings, liquidity and stress regimes. Pool only compatible action/size/liquidity states; do not train a separate model per coin without support.

Calibration is offline. Use chronological cross-fitted residuals and coarse compatible support cells to estimate mean bias/support/OOD for the entire deployed selection policy. Freeze feature/support thresholds, mean-selection allowance and residual execution-bias procedure before final holdout. Inadequate calibration makes economics NOT_ESTIMABLE. No synchronous bootstrap, scenario refit or model search is allowed in the dispatch deadline.

For the final complete-policy claim, preregister one primary calendar-time programme-PnL estimand and paired comparisons with no-opening, the preserved deterministic R9 coordinator and the simple empirical comparator. Use 10,000 offline calendar-block bootstrap resamples and one-sided 95% lower estimates. Select block length from 1, 3 or 7 calendar days using training/calibration dependence diagnostics only; default to seven when inconclusive. Require at least 20 block equivalents and stable conclusions across the registered block sensitivities; sparse dependent evidence remains inconclusive. This is a validation procedure, not a guarantee of nominal error control in a changing market.

Freeze the comparison family and use Holm-adjusted one-sided tests for model/strategy claims within that family; do not repeatedly inspect and reset a holdout. Make one scheduled final assessment after the preregistered collection window and support requirements. The inherited eight-week/200-opportunity floor is only a floor and may be insufficient for these conditions. Define economically meaningful uplift, cost stress and tail/drawdown tolerances before observing final results.

Require positive programme value versus no-opening, acceptable tail/risk behavior, execution-calibrated costs and no operational degradation. Claim superiority over R9 or a simpler comparator only if its incremental test passes. A mandatory agent can have operational value without alpha; measure watch yield, evidence errors, avoidable calls, abstention and missed value. Keep the deterministic comparator shadow-only, not an unauthorized live fallback.

Foundation pretraining overlap and opaque hosted updates make forward evidence primary. Record observable provider/model identity, configuration, prompt/schema and usage. A material identity/conformance change quarantines decisions until requalified. No agent self-promotion, automatic live retuning or selection of the best-looking backtest.

## 10. Shared-account capital and ownership

Account balances come from reconciled exchange evidence, never a fictional configured starting balance. The capital-enabled core profile is single-asset USDT with one-way positions and isolated margin on bot-owned symbols where the exact adapter/account permits it; automatic margin additions are disabled. Observe existing manual cross-margin/other exposure account-wide. Reject an unsupported opening profile instead of changing occupied account settings. Existing research/reader capabilities remain available.

The user explicitly sets a bot allocation ceiling, risk limits and reserves. Deposits or manual profits do not silently expand the bot ceiling. Default permission is OBSERVE, allocation is zero and paid-provider allowance is zero until the user configures them. Missing allocation/risk/permission is a well-defined refusal, not an implementation omission.

Usable opening capacity is the minimum of fresh exchange available USDT, remaining bot allocation and applicable collateral/risk headroom, minus only locally committed increasing exposure not proven reflected, uncertainty reserves and user manual/safety reserves. Exchange available balance already reflects some obligations; do not subtract existing initial margin twice. Define source semantics explicitly at the account adapter. Separate account equity, available balance, eligible bot equity and reserved margin.

For each opening command maintain `UNREFLECTED`, `REFLECTED`, `UNCERTAIN` and `SETTLED` accounting states, evidence IDs and watermarks. A timeout never releases a possible send. Move reservations only on positive reconciliation evidence, idempotently. External increasing ordinary and conditional orders enter possible exposure. Commit reservation, one-use setup consumption, ownership lease and intent together in the existing control database.

Serialize opening reservations and final dispatch checks. Several protected positions on different owned symbols may coexist; serial writes do not mean one total position. The default declared concurrency envelope is three positions and one increasing dispatch at a time, configurable only within measured capacity and risk. Prove at least two concurrent positions. At approximately $10 the feasible count may be zero; do not round up or increase leverage to manufacture feasibility.

Use disjoint symbols for manual and bot activity. There are no fictional independent manual/bot lots in one aggregate leg. Explicit adoption binds the complete reconciled position, existing orders, protection and management authority. A local lease cannot fence another API client or Binance UI.

| External change | Required response |
| --- | --- |
| Position addition/reduction or unexpected increasing order | Enter CONFLICT; block new exposure and optional changes; reconcile. Retain valid closing-only protection while disclosing aggregate effects. New aggregate management needs explicit adoption |
| External flat | Reconcile all possible own increasing orders/fills before issuing a flat certificate; cancel only proven own obsolete protection |
| External reversal | Reconcile/cancel proven own increasing remainder and obsolete own protection under the qualified transition; never close the new manual leg by assuming the old side still exists |
| Manual stop deletion/change | Mark coverage invalid, enter the declared protection incident and use only current proven management authority; do not silently adopt manual orders |
| Explicit whole-position adoption | Rebind aggregate quantity, orders, protection, policy, permission and ownership epoch before new discretionary commands |

Already-sent closing protection may still affect the aggregate position. The UI must explain this limitation. On ambiguous ownership, conservative conflict and preserved protection are preferable to inventing authority.

## 11. Feasible sizing, risk and permissions

Run a cheap feasibility screen before deep scanning or API work. Use Decimal and exact tick/lot/notional filters, adverse-price rounding, fee/funding/exit reserves, leverage/maintenance tiers, liquidation buffer, concentration and joint shocks. Quantity-dependent impact and tiered margin require lot-aligned `loss(q)` and `margin(q)` checks, not a globally constant risk-per-unit assumption.

Risk limits are explicit user-owned settings; optional filters can be disabled visibly. Technical validity, honest evidence, one writer, no duplicate send, ownership, mandatory protection, economic qualification and trustworthy command durability cannot be disabled by FULL_ACCESS. Risk settings are preferences, not proof of safety. Increasing leverage does not reduce loss for unchanged notional price movement.

Include actual, manual, pending and possible exposure in common crypto shocks, idiosyncratic gaps, spread/basis expansion, funding and mark-price/maintenance checks. Deterministic stress is a bounded engineering scenario, not a claimed probability. Probabilistic ES remains diagnostic until calibrated. The exact stress magnitudes, cash/drawdown/concentration limits and liquidation buffers are mandatory bound profile inputs; absent values block capital. Demonstrate simultaneous adverse moves across multiple altcoins rather than assuming distinct symbols diversify them.

The daily loss-consumption policy sums negative net realized bot contributions over the rolling 24-hour window; profits do not replenish that allowance. Keep the historical netted calculation only for historical replay. Funding/fees and position-attribution rules prevent double counting. Separate open-risk, realized-loss and allocation limits.

Permissions distinguish OBSERVE, exact-entry approval and bounded autonomous entry; independent scopes govern discretionary exit, whole-position adoption and paid-provider spend. Approvals bind exact action hash, quantity, collar, stop, management/horizon, account/environment, ownership, policy/capability/configuration, expiry and nonce. Any material change invalidates them. No response means no approval.

Pause entries, cancel own increasing remainder, flatten owned exposure, revoke discretionary authority and adopt aggregate exposure are separate typed commands. Revocation cancels unsent authority immediately; it does not erase armed/UNKNOWN effects or immutable stop/time duties. Tightening risk blocks new exposure and applies the configured already-authorized reduction rule. Loosening requires fresh evidence and cannot resurrect old plans.

## 12. Compulsory API coordinator and decision framework

Use one hosted coordinator with task profiles, sealed Briefs, durable memory and a closed decision vocabulary. Reuse existing gateway, validation, broker and budget components; do not activate parallel critic/coordinator loops with separate spend ledgers.

Choose `gemma-4-26b-a4b-it` over the existing native Google transport as the configured coordinator candidate. The route exists in official documentation and R9 has a genuine synthetic smoke; neither establishes trading competence. [E4] A provider-neutral domain protocol remains, but compatibility transport rewrites are not required merely for branding. Existing alternative adapters are preserved. One explicitly configured, separately qualified route may replace another; there is no silent or paid automatic fallback.

The coordinator can select registered workflows, create bounded watches, request registered analysis, map verified event evidence, reject/abstain, select the host-rule winner and request a qualified exit comparison. It cannot author executable predicates, invent numbers, alter risk, choose an unqualified strategy/model, access exchange keys or submit an order.

Prefer setup-specific conditional directives prepared before a short trigger. Bind symbol/revision, side, workflow/template, evidence prefix, allowed host predicates, setup epoch, one-use allowance, invalidators and absolute expiry. The host later instantiates a fresh feasible exact action. This is bounded delegation, not a claim that the LLM saw future facts. A direct exact-action response is allowed only when measured latency fits the unchanged deadline. No response extends a policy TTL.

State transitions are `EVIDENCE_READY`, `ANALYSIS_REQUIRED`/`WATCHING`/`ELIGIBLE_SET`, `SELECTED`/`REJECTED`/`ABSTAINED`, optional approval, reservation and dispatch/expiry. State lives in durable application records, not chat history. The host validates the maximum-conservative-score rule and current gates after any directive or approval.

Allowed tools are bounded typed reads: existing snapshot/economics/memory operations plus watch, owned-position, event-source and compatible-history reads; registered analysis requests are queued through the host. No arbitrary SQL, shell, filesystem, browser or exchange tool. Source claims need verified spans, asset identity and available-at time. News/memory instructions remain untrusted data.

Default engineering limits are one concurrent job, three calls and four bounded tool reads per job, three candidate summaries, an input target of 6,000 tokens, output cap of 1,024 tokens, queue capacity eight and an absolute deadline no later than the bound policy deadline. Maximum calls/day is 60, limited further by actual provider quota and user cap. These are ceilings, not a promise to service every warm-symbol slot. Use pre-trigger directives, batching and material-change refreshes. Reject or expire stale work before dispatch.

Strict structured fields are decision, candidate/template reference, rule ID, evidence references, requested registered analyses and missing/uncertainty reason. The host attaches identities, hashes, usage, timestamps and permissions. Reject unknown fields, fake refs, invalid types/nonfinite numbers, out-of-scope requests and late results. Tool-capable native transport and any continuation protocol require conformance; the present tool-free adapter alone is not sufficient.

Reserve worst-case call/token/cost budget including retries, continuations and billable reasoning before a job. Unknown paid price blocks dispatch; paid cap defaults to zero. Lost responses retain possible spend. Official Gemma pricing currently lists free input/output, but quotas and data terms still apply. [E5] Minimize prompts to necessary anonymized evidence; never send secrets, raw account IDs or ledger exports.

Observed outage, quota failure, identity mismatch or circuit opening invalidates unconsumed entry directives and pauses openings. Existing stops, scheduled exits and reconciliation continue. Rearm requires a successful fresh bounded conformance job, fresh evidence and resolution of every other health latch. No old opportunity is resurrected. Without continuous polling, remote availability is known only from the last successful bounded job; directive freshness is explicit.

## 13. Brief, memory, watches and effort scheduling

Reconstruct typed production readers for completed candles, technical/state/structure, liquidity, derivatives, source health, BTC/ETH context, events, prior analysis, watches, invalidators, due work, outcomes, exact-action economics and available forecasts. Required/optional dependencies belong to a per-job policy manifest. Do not make every optional forecasting model a universal prerequisite.

An analysis-complete Brief can contain explicit unavailable economics and still support collection/research. A capital-complete packet additionally requires current eligible economics and every dispatch gate. Remove neither the current completeness guards nor provenance checks until real required readers replace them. Preserve legacy fail-closed fixtures and add genuine successful-path evidence separately.

Watches persist instrument/policy, setup epoch, side, creation evidence, trigger/invalidation IDs, next due event, absolute expiry, last evaluation and attempt latch. Evaluate native events, not repeated chat polling. Deduplicate by watch/event/state. Restart replays causally, expires old triggers and preserves consumed attempts. A new model/configuration does not reset an economic setup.

The deterministic effort scheduler uses SAFETY_ONLY for critical unknown/conflict/resource incidents; CAPITAL_WAIT for no feasible opening/no useful watch due; WATCH_ONLY for useful constrained due work; ACTIVE for feasible capacity and healthy qualified inputs. Every mode maintains owned-position safety. Use hysteresis to avoid resource churn; manual margin consumption invalidates openings immediately. Persist fair strategy clocks, prospective slots, missed reasons and the full refusal funnel.

## 14. Binance execution and protection

Reuse Nautilus and the existing narrow native gap adapter through one execution owner. Bind the exact build, account/environment, order modes and native capability contract. Binance conditional stops use a distinct algo service with query/cancel/child identities; ordinary order ACKs do not prove native stop coverage. [E6]

Entry uses the existing frozen marketable LIMIT IOC policy where qualified, with explicit price collar, deadline, partial/no-fill behavior and taker costs. An existing five-second decision deadline or five-basis-point collar is an inherited hypothesis to validate, not a guaranteed fill or cost bound. Alternative timing/entry policies need their own evidence and may not silently replace the baseline.

Before transport, durably record the exact intent, reserve capacity and consume the setup; then durably arm dispatch. Deterministic command identities and idempotent reducers prevent duplicate effects. Classify definite rejection separately from ambiguous send. Never blindly retry an UNKNOWN opening; reconcile ordinary/algo IDs, executions and positions with positive evidence.

The capital capability manifest must record native stop type, trigger basis, price-protection flag, quantity/close semantics, instrument/side binding, readback and partial-fill adjustment. Accept only the tested combination; do not assume linked brackets/OCO or pre-armed atomic protection. Retain a valid closing stop while a mandatory exit is underway unless its proven side/quantity semantics are hazardous.

Monitor possible exposure from SEND_ARMED, not only a delayed fill callback. The inherited healthy-path goal is verified native coverage within two seconds of observable possible exposure requiring protection, measured with explicit transport/observability delays. Host/venue/network outages can prevent this. A trigger and an ACK do not guarantee execution or maximum loss. Capital activation requires explicit acceptance of the reactive fill-to-protection residual risk; otherwise it remains blocked.

Coalesce mandatory exit goals to TARGET_FLAT, cancel increasing remainder and reduce proven owned exposure without waiting indefinitely for another unknown order. Preserve possible quantity and reconcile continuously. Never let an UNKNOWN on one symbol starve uncovered known quantity on another. Read actual quantity before every closing command, preserve no-reversal and handle dust/partial/duplicate/out-of-order fills.

Flat certification requires proven terminal increasing orders, reconciled executions and no possible future increasing fill, not one zero snapshot. Cancel only proven own obsolete protection. Do not touch unrelated manual orders.

## 15. Recovery and simultaneous failures

Startup is RECOVERING. Verify physical single-writer ownership; restore journals, reservations, setup latches, directives and pending approvals; reconcile ordinary/algo/child identities, executions, funding, positions and protection using pagination and buffered user events. A negative lookup or missing WebSocket message does not prove terminality.

No automatic hot takeover across machines. Local epochs do not fence another host. A replacement must prove the previous owner cannot dispatch before rearming. Never change occupied account mode or cancel manual orders during startup.

Safety uses independent timer events, bounded network work, protected connection/rate capacity and separate queues from research. Latch account/order unknown, ownership conflict, missing protection, provider failure, source discontinuity and storage/resource faults separately. Recovery of one does not clear the others. Replay is idempotent and cannot convert an expired approval into a new intent.

| Combined incident | Required terminal behavior |
| --- | --- |
| Partial IOC + host loss + provider outage | Preserve possible exposure; use already-installed venue protection; recover before trading; report any uncovered interval honestly |
| Manual same-symbol change + queued stop/time exit | Apply explicit conflict/adoption authority; no fictional lots, manual cancellation or wrong-side close |
| UNKNOWN opening + silent account stream + market burst | Independent safety progresses; reservations persist; no blind resend, false flat or early rearm |
| Disk full + pinned WAL + mandatory exit | Use declared emergency durable capacity; if durability is untrustworthy, no new commands under the standard path; retain venue protection and alert |
| Correlated altcoin gap + funding shock + manual collateral use | Reconcile account-wide stress/liquidation state; no claim that stop prices cap realized loss |
| Quota loss + model identity change + late approval | Quarantine route/directives, preserve stops, reject late authority and require fresh conformance |

When trustworthy durable control commits cannot be made, the standard system relies on existing venue protection and alerts instead of secretly sending unrecoverable commands. A non-durable emergency-close exception is not included. This limitation is explicit and tested. Finite storage and an unavailable venue cannot support an unconditional recovery promise.

## 16. Resource, retention and dependency contract

The same complete system supports headless and reduced/normal desktop operation. Lazy-load query, chart and research frameworks; use incremental indicators, bounded batches and one optional numeric worker. Do not require a local LLM, full-universe raw L2 or multiple resident foundation models.

| Resource | Starting measured acceptance target |
| --- | --- |
| Total application process tree | Below 900 MiB steady; shed optional work near 1,100 MiB; stop new research/openings at 1,250 MiB while retaining safety |
| Persistent application data | 1.5 GiB soft; 2 GiB hard, including DB/WAL, caches, models, logs and temporary/export staging |
| Control allocation | 256 MiB including a physically reserved 128 MiB emergency-continuation allowance |
| Ops/capsules/outcomes allocation | 640 MiB; active/control/claim evidence pinned |
| Bar/feature and raw-ring allocations | 256 MiB plus 64 MiB respectively |
| Model artifact cache | 512 MiB; one admitted model, verified eviction/download boundaries |
| Logs/incidents/export staging | 192 MiB; bounded file count/size |
| Unallocated quota headroom | 128 MiB; partition totals equal 2 GiB |
| Physical free space | At least 512 MiB outside reserved application extents before new opening/research; monitor whole filesystem |
| Installed runtime/build footprint | Separate measured maximum 8 GiB for the deployment bundle/environments; bounded build caches; no hidden unlimited downloads |
| Agent/model concurrency | One hosted job and one admitted numerical job; safety remains independent |

These are proposed test targets, not passed benchmarks or universal runtime guarantees. If a required core workload cannot meet them, fix its cause, use the same headless capability or report a blocked requirement. Do not conceal failure by omitting core functionality or silently raising a ceiling. Research may pause under live safety pressure without disappearing from the delivered system.

Keep control SQLite WAL with synchronous FULL and existing single-writer ownership. Batch ops/capture persistence; avoid fsync per raw frame. Bound reader lifetime and WAL bytes; checkpoint safely and measure reader-induced growth. Use a SQLite runtime containing applicable WAL correctness fixes/backports, verify the linked runtime, and test crash/recovery rather than assuming a Python version proves safety. [E7]

Retention is byte-based with source dependencies and pinning. Preserve active positions/orders, unresolved incidents, approvals, actual execution ledgers, usage ledgers and evidence supporting retained claims. Evict expired unreferenced raw/diagnostic/cache/model data first. Never delete active control state to permit a new trade. Failed export is not successful archival. If pinned data exceeds capacity, stop new work and request explicit export or quota change.

Label SOURCE_REPLAYABLE, DECISION_CAPSULE_REPLAYABLE, SUMMARY_ONLY and EVIDENCE_MISSING. Capsules retain exact inputs/features/model/output/decisions but cannot claim raw reconstruction when sources are gone. Train/qualify only to the evidence grade actually retained. Compact no-candidate denominators rather than repeating large snapshots.

Audit existing lockfiles rather than inventing an unpinned replacement. Reconcile platform dependency divergence, reproduce rc5 for baseline evidence and qualify the chosen supported adapter build before capital. Maintain frozen historical serialization. Clean install/import/start/recover, numeric worker isolation, IPC, process termination and shutdown must pass on target Windows/Linux. Preserve licence and provenance records.

## 17. Master event algorithm

The safety path advances before discovery and does not wait on the hosted coordinator.

```text
ON event or independent safety timer:
  accept only valid causal observations; update monotonic deadlines
  reconcile dirty account/order/protection state within bounded work
  update ownership, reservations, risk and all health latches
  coalesce mandatory protect/cancel/exit/reconcile goals
  advance the single execution owner's safety goals; persist receipts
  if critical truth/durability/protection conflict: record SAFETY_ONLY
  otherwise choose effort from feasible capital and useful due work
  maintain owned positions, watches and fair cheap-universe coverage

ON a registered strategy/watch decision event:
  persist prospective slot and all inclusion/missing/refusal states
  construct one immutable bounded CandidateEvaluationSet
  for each candidate: check expiry/episode/ownership and feasible lot
    freeze its exact action and account/risk assumptions
    perform cheap inference from a compatible prefit economic artifact
    record supported evaluation or honest NOT_ESTIMABLE/refusal
  build the typed analysis/capital packet required by this job
  use a valid pre-trigger directive or bounded direct coordinator job
  host-validate evidence, rule, budget, deadline and current eligibility
  if no valid selection or observed provider failure: no new opening
  bind the exact plan and obtain scoped one-use approval if required
  atomically reserve allocation, ownership, setup episode and intent
  revalidate account/permission/price/cost/capability at dispatch
  if materially changed: re-evaluate/reapprove or expire; never patch silently
  durably arm then send once; ambiguous send enters reconciliation

ON fill, protection change, timeout, permission change or restart:
  update orthogonal actual/possible quantity and authority
  advance immutable stop/time and qualified exit obligations
  retain reservations/leases until positive terminal proof
  never rearm from one flat snapshot or resurrect an expired setup
```

## 18. Completion, qualification and capital decision

Engineering completion requires every core row to have working implementation, meaningful test, exact-build evidence and no placeholder. All selected foundation runners and the early-exit research path are included. Relevant baseline capabilities and historical replay remain intact. Missing environment, checkpoint or venue access is reported as a blocked row, not a passed feature or a later product version.

Operational qualification requires a genuine complete shadow cycle and at least 72 hours on actual declared Windows/Linux minimum hosts, including startup/backfill, peak admitted load, retention rollover, UI/headless operation, provider faults and compound incidents. Record process-tree memory, lag percentiles, missed slots, CPU pressure, WAL/storage high-water, API usage and protection scheduling. Test at least two concurrent Demo positions, manual interference, partial/unknown/native-stop/restart lifecycles on the exact build. Demo is not evidence of live fill economics.

Capital additionally requires a feasible actual account/lot profile; accepted reactive-protection residual risk; current permissions/allocation; exact venue capability; positive prospective programme value and required comparator/risk evidence. Any separately authorized tiny live execution-calibration experiment has its own loss/spend cap and is not labelled profitable production. This documentation does not authorize such an experiment.

**Decision:** GO to implement this complete specification and test it. NO-GO for live capital on current evidence. No guarantee of maximum profit or universal superiority is made. Evidence may justify a simpler active trading policy while the full research/control capabilities remain delivered.

## 19. Sources and authority

This finalized design incorporates the owner's 16 October instruction: preserve strengths, deliver all core features in one system, separate Codex models by session, and document/push the design. It supersedes conflicting proposed F2 wording and earlier scope-defer recommendations for this target. Existing frozen documents remain unchanged as historical authorities for old records and baseline reproduction; their live-agent prohibition is not silently removed from legacy paths. A new bound operational policy still needs implementation, tests and capital qualification.

- R1: [R9 end-to-end evidence](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/docs/v2/ATLAS_RED_R9_LOCAL_END_TO_END.md).
- R2: [R9 tests and performance](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/docs/v2/ATLAS_RED_R9_LOCAL_TESTS_PERFORMANCE.md).
- R3: [R9 execution and protection](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/docs/v2/ATLAS_RED_R9_LOCAL_EXECUTION_PROTECTION.md).
- R4: [Production intelligence projector](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/src/atlas/v2/runtime/production_intelligence.py), [research selection](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/src/atlas/v2/science/research_selection.py), [M0](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/src/atlas/v2/science/m0.py), [M1](https://github.com/KasunKarandagolla/Atlas/blob/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd/src/atlas/v2/science/m1.py).
- E1: [TiRex-2 maintainer repository](https://github.com/NX-AI/tirex-2).
- E2: [Chronos maintainer repository](https://github.com/amazon-science/chronos-forecasting).
- E3: [Kronos maintainer repository](https://github.com/shiyu-coder/Kronos).
- E4: [Google hosted Gemma API](https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api).
- E5: [Google API pricing](https://ai.google.dev/gemini-api/docs/pricing).
- E6: [Binance USD-M trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade) and [account API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account).
- E7: [SQLite WAL documentation](https://www.sqlite.org/wal.html).

Primary external sources were checked on 16 October 2026. Model availability, quotas, prices, venue rules and installed runtime facts must be checked again at qualification. This document's new design procedures and numerical budgets are engineering decisions and unpassed acceptance targets, not claims made by those sources.
