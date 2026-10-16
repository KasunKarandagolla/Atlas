# ATLAS RED - consolidated algorithm and system design

**FINALIZED TARGET FOR IMPLEMENTATION PLANNING | 16 October 2026 | Operative amendment PROPOSED - NOT OPERATIONALLY EFFECTIVE**

One complete trading and research system for Binance USDT perpetuals, a shared manual account, broad altcoin observation, concurrent positions, a compulsory hosted API coordinator, and a 4 GB Celeron-class Windows/Linux laptop.

**Objective:** maximize validated programme profit within explicit capital, tail-risk, permission, reliability and resource constraints. Intelligence is useful only when it improves measured decisions, execution, research or operating efficiency. No architecture or model can guarantee maximum profitability or superiority in every market.

**Current status:** the target direction and five-session forecast have conditional design approval; this synchronized publication is for independent documentation acceptance. No implementation starts in this documentation session. The precise manual/adoption policy is PENDING OWNER APPROVAL and the operative amendment is not effective. Runtime implementation and empirical qualification remain outstanding. Live capital is blocked on the evidence reviewed. This document records decisions and acceptance obligations, not tests passed by the new system.

**Code baseline:** `KasunKarandagolla/Atlas`, branch `fix/red-r9-local-complete-cycle-2026-10-14`, commit `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`. Its commit time is 15 October 2026, 01:12:22 Asia/Colombo. Do not start from the older repository default branch.

## 1. One complete system and a non-degradation contract

There is one target system. There is no weaker MVP, lite product, second OMS or sequence of product versions that moves core requirements into a later project. The five engineering sessions are work partitions of this same system. All core requirements in `REQUIREMENTS_AND_ACCEPTANCE.md` must be implemented and verified before engineering completion is claimed.

Implementation, evidence and capital authority are different facts. An implemented model may be unsuitable for a particular machine or unqualified for money. A strategy may have a complete research workflow but lack evidence for capital admission. These outcomes are visible capability states, not a promise to implement a missing feature later. A registry entry, injected fake callable or permanent unavailable stub does not complete a required integration.

Preserve every working baseline capability at its established authority: existing strategy research, causal indicators, model isolation, empirical forecasts, analogue retrieval, economic/scenario primitives, memory, scanner exploration, outcome maturity, UI/IPC, single-writer execution, approvals, journal/outbox, UNKNOWN handling, no-reversal logic, protection, reconciliation and recovery. Preserve existing non-Binance adapters and historical readers without expanding the active venue scope.

Do not overwrite frozen historical policies or reinterpret their saved artifacts. Necessary schema identifiers, hashes, model revisions and migration records remain because replay and safe recovery require them. They do not create competing product versions. Historical zero-authority agent records stay zero-authority; the proposed authority can apply only through explicit new policy bindings and qualified dispatch checks.

A change must preserve relevant golden behavior or identify a deliberate correction, its reason and its test. Removing a capability requires proof that no active caller, saved record, required workflow or acceptance row needs it. Reducing discovery load during pressure is allowed; reducing protection or silently dropping a core feature is not.

## 2. What the baseline actually establishes

The source audit verified all 287 production Python source files against Git blob identities, indexed their definitions/call candidates and hand-traced critical actual production, science, provider, journal/native and GUI paths. It inspected 192 selected test files plus fixture dependencies and the complete 301-test-file inventory. All three historical freezes were read in full and their supplied/repository hashes verified; the actual supplied original owner/ADR sources are now reconciled in AUTHORITY_RECONCILIATION.md, with hashes/sections in SOURCE_AUTHORITY_MANIFEST.json. See REPOSITORY_AUDIT.md for exact scope and limitations. It was not a fresh full-suite or exchange certification run. R9 provides substantial implementation to reuse, including multiple research sleeves and durable scanner accounting.

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

1. `CandidateEvaluationSet`: one immutable due-slot population, capacity-derived evaluation envelope and inclusion/refusal records. No final winner is required to value a member.
2. `ExactAction`: complete entry/partial-fill/stop/exit policy, quantity, risk envelope, information cutoff and provenance. Any material change produces a new bound action.
3. `ActionEvaluation`: exact action, compatible mean model, costs, support/OOD, selected-policy uncertainty allowances, scenarios and status.
4. `SelectionDecision`: a separate result bound to the whole comparable set, the registered rule and a valid coordinator directive.
5. `CapitalPlan`: selected exact action plus current account/ownership/permission/capability bindings, expiry and one-use nonce.

Update the real production callers, feature projection, sizing, M0/M1, evaluator, Brief and outcomes together. Do not create a different one-candidate 'selected' set for every alternative. Scanner rank remains an origin-known feature if used; final economic winner rank must not leak back into its own value features.

## 5. Market data, time and broad coverage

Retain the actual mandatory ten-second cheap refresh (`CHEAP_REFRESH_INTERVAL_NS_V2 = 10_000_000_000`) of current Binance USDT perpetual metadata, ticker/mark and source-health summaries. Manual effort and optional work do not reduce this source cadence or breadth. Reconstruct point-in-time listing/delisting, contract revisions and filter changes for research. Capital eligibility additionally requires qualified liquidity, price, fee, funding and account evidence.

Use three workload tiers: cheap universe observation; warm causal histories/features; deep trade/book evidence. Approximately 20 warm symbols and two deep discovery books are initial benchmark seeds, not fixed trading-universe limits. Pin benchmarks, owned positions and useful due watches, then admit/rotate optional histories by measured memory/source cost, age, fair exploration and setup usefulness. Every required position input has priority. Derive the operational maximum from actual capacity; if mandatory pinned work cannot fit, stop new exposure. Separate the cheap population, adaptive exact-action evaluation budget and token-bounded API packet; there is no permanent three-candidate trading cutoff.

Retain causal completed bars, exact indicator seed/index/equality behavior and golden fixtures. Track source event time, receive time, available-at time, persisted-at time, sequence continuity, final-bar status and per-channel watermarks separately. Backfill cannot recreate the original opportunity deadline or missing order-book evidence. Reject future-available data, silent stale channels and invalid/nonfinite records without replacing last valid exposure with zero.

Synchronize books from the required snapshot and stream sequence. Drop/reorder/gap detection must invalidate dependent features until resynchronization. Use UTC for venue candles, event/funding calendars and research splits; use monotonic time for elapsed deadlines. Suspend/resume, large clock changes and offline intervals enter recovery and expire stale opportunities.

Preserve rotating exploration and known inclusion probabilities. Persist expected slots, no-candidate, denied, missed, resource-not-evaluated and unavailable states. No historical interval is retroactively counted as prospectively scheduled. Backfill and model work are bounded by range, rows, bytes, time and temporary storage.

## 6. Strategy surface and management capabilities

All existing strategy families remain part of the complete system. Path A retains registered strategy detectors; Path B retains the bounded objective novelty/anomaly screen outside existing templates and the quarantined Discovery Lab. Its hypotheses cannot generate orders, execute model-written code, read protected holdouts or self-promote. Both populations and failed/ignored leads retain causal denominator evidence (TA 4.3, ADR-002 B, V2 27.4). Their role is explicit, and no research-only capability is relabelled capital-ready merely to claim completion.

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

Implement a finite complete management registry: HOLD, CLOSE, registered PARTIAL_TP, monotone TIGHTEN, COST_ADJUSTED_BE, REGISTERED_TRAIL and TP_ADJUST, plus event-driven review and independently authorized risk reduction. Preserve the existing half-TP-at-2R shadow comparator. All alternative generators, future-value labels, permissions, native command paths and fault tests are in these same engineering scopes. Capital use requires their own support/value/venue authority; it is not granted by an agent request.

Pyramiding, exposure-increasing layering, live atomic/multi-leg execution and additional active venues are outside the selected core execution policy. Deliver a joint financed-rotation research comparator (HOLD A versus CLOSE A then conditional OPEN B), including released-capital timing, B expiry/no-fill and complete costs. Live financed rotation is deliberately excluded from core execution because it is an unqualified contingent portfolio strategy, not assumed economically inferior and not silently promised as later work. Preserve all existing related research.

### 6.1 Management algorithms and venue obligations

One management contract/evaluator/serializer extends the existing lifecycle. Alternatives are HOLD, CLOSE, registered PARTIAL_TP, TIGHTEN, COST_ADJUSTED_BE, REGISTERED_TRAIL and TP_ADJUST. Verified events request review; hard risk can require REDUCE/FLAT independently. Limit optional alternatives to eight per review and retain safety goals outside this quota. No stop widening, add, reversal, horizon extension or cancel-required-protection instruction is permitted.

For optional action `m`, compare `Delta_m = W_T(policy with m now) - W_T(hold current sealed policy)` on the same future joint paths, exact position and execution assumptions. Sunk realized loss cancels from the comparison. Include closing fees/slippage, future funding/duration, post-partial quantity, remaining protection and further registered management. Required risk reduction does not wait for positive estimated Delta or the agent.

Closing quantity is `floor_step(fraction * abs(current_signed_quantity))`, capped by fresh proven current exposure and pending closing effects. Apply the venue's actual closing filters; do not assume minimum-notional exemptions. If quantity is invalid or leaves unmanageable dust, return NO_ACTION or a separately preauthorized full-close alternative. Never round a partial upward or open an opposite leg to clean dust.

For a long, any new stop must be >= the existing effective stop after tick rounding; for a short <=. Validate trigger basis, current trigger side, product revision and maximum time horizon. Cash-cost-adjusted BE solves for remaining-position net cash including known entry/exit fees, actual/future funding assumptions and slippage allowance; label it a threshold, not guaranteed zero loss. Trailing uses only observed post-entry favourable extrema and registered distance/step/callback rules.

Native Binance conditional TP/SL/trailing routes and ordinary reducing closes are distinct. In supported one-way mode, explicit-quantity closing orders use the qualified reduce-only path. `closePosition=true` is limited to supported close-all stop/TP types and omits both quantity and reduceOnly. Native trailing has its own activation/callback contract; activation is not equivalent to protective coverage. These are documented venue constraints, not proof of adapter behavior. [E6]

Use an atomically qualified stop amendment if available. Otherwise create a compatible closing-only replacement, verify live coverage, then cancel only the proven obsolete own stop, **only if** the exact venue/build supports overlapping protection safely. If not, retain the old stop and report NO_CHANGE; never cancel the old stop first merely to enable an optional tighten. Mandatory protection incident follows the independent repair/reduce controller.

One unresolved optional management command per position; all management commands are journaled and bound to epoch/action/permission. Cancel/TP/stop/IOC races reconcile actual fills before further action. TARGET_FLAT supersedes optional changes; queued obsolete tasks cannot revive after flat/reversal/conflict. Provider failure cannot cancel committed deterministic trailing/stop/time obligations. If the host trails a fixed native stop in steps, host outage freezes its last installed stop; do not claim continuous native trailing.

Deliver a joint rotation research comparator that values HOLD A versus CLOSE A then conditional OPEN B, including release timing, no-fill/expiry/approval failure and both sets of costs. Live financed rotation is explicitly excluded from the selected core execution policy because it introduces a contingent portfolio strategy and extra irreversible transitions without supporting value evidence. This is not a claim that rotation is inferior: include a toy case where it wins and one where entry expires after closing A. Do not route it through unrelated normal-opening commands or silently promise later implementation.

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
| One qualified hosted API coordinator; current Gemma route is the initial candidate | Compulsory bounded workflow/analysis and eligible-set B decision | Reuse native transport; verify actual route/revision/tools/images. No numeric risk authority or automatic fallback |

The existing empirical horizon-log-return `StatisticalBaselineV2` remains a forecasting control. It is not the new empirical net-action payoff comparator.

All three selected foundation integrations are in engineering scope, including real inference bridges, causal input/output contracts, manifests, cancellation, memory limits, reproducible evidence and comparison reports. Injected fixture callables alone do not complete them.

| Checkpoint | Duty | Scheduling and qualification |
| --- | --- | --- |
| `NX-AI/TiRex-2` | General multivariate/covariate forecast and scenario challenger | Serial isolated inference; retain as a general research comparator |
| `autogluon/chronos-2-small` | Small probabilistic forecasting alternative | Serial comparison with TiRex; standard 120M `amazon/chronos-2` is not silently substituted |
| `NeoQuasar/Kronos-mini` with `NeoQuasar/Kronos-Tokenizer-2k` | 4.1M financial OHLCV forecast/scenario challenger | Pin tokenizer, sampling, context and OHLC units; validate financial path consistency |

Maintainer facts establish model identity and advertised capability, not crypto edge or 4 GB capacity. [E1-E3] Pin checkpoint revision, hashes, runtime, preprocessing and postprocessing. Each runner must execute real bounded inference in its supported research environment before its integration row passes. Measure Windows/Linux support separately; TiRex's package documentation currently states Linux/macOS testing. Do not add Docker as a compulsory 4 GB dependency.

The scheduler loads at most one optional numeric model at a time and never installs/downloads a framework on an entry or protection path. Local inference requires admitted measured capacity; a separately qualified numerical endpoint is an alternative, not a generic chat endpoint. If neither route qualifies, report the exact block and keep the mandatory completion row open. No claim of complete model integration may be based solely on disabled adapters.

Not running every model on every trade is a resource and statistical decision, not a weaker product. Promote a foundation output only as a fixed, tested feature or scenario input after it improves complete-policy value. A price quantile is neither a payoff-mean confidence interval nor a probability of profit.

### 7.1 Runner scheduling and comparison order

Preserve all three selected real inference integrations through the common isolated ABI. Reuse the existing Kronos-mini synthetic smoke for interface/fixture work without claiming it qualifies other runners. Retain the historical statistical-baseline, TiRex-2, Kronos-mini, Chronos-2 engineering-evidence order; a blocked attempt remains open while other permitted zero-authority work proceeds. This order is not a ranking of trading skill or permission to delete a blocked runner. Use exact loader/file manifests and actual peak measurements, not parameter counts or full-repository downloads. Run optional forecasts before useful native decisions or as due research; no fitting/loading on protection/dispatch deadlines. Choose any decision-affecting feature set only by incremental complete-policy evidence, keeping all other implemented research runners accessible.

## 8. Payoff targets, costs and comparison

For exact action `a`, define `Y(a)` as account wealth at a common 24-hour terminal convention with the sealed action minus wealth with no new opening, under coherent market/execution paths. Cash remains cash after a shorter strategy exits; do not assume automatic reinvestment. Portfolio comparisons use shared path IDs and include manual, actual, pending and possible exposure.

Cash payoff includes actual filled quantity, every entry/exit fee, signed funding, spread/impact/delay through actual or qualified modelled execution prices, and the complete stop/exit policy. Do not subtract slippage twice if prices already include it. No fill has zero trading payoff, with any genuinely incurred programme expense still recorded. Partial fill and unfilled remainder behavior are explicit. Cancel/replace, funding-boundary and dust outcomes cannot vanish from training.

Maintain three reconciled views: venue trading PnL; programme PnL after all incurred API/data/operating costs; whole-account equity including manual trading and deposits/withdrawals. Provider spend outside Binance is not a second deduction from exchange available USDT. A configured programme reserve can independently restrict bot allocation. Charge a shared analysis bill once, not to every hypothetical candidate. Sunk analysis expense is included in programme reporting; only additional future costs affect the current incremental decision.

Read actual account commission and symbol filters; preserve vintages and use conservative taker treatment for marketable IOC assumptions. Funding payment times/intervals, not a universal eight-hour assumption, determine exposure. Unknown required costs or price outside a qualified envelope cause refusal/revaluation.

The economic gate is `score = mean_net_cash - mean_selection_allowance - execution_bias_allowance - numerical_allowance - incremental_future_programme_cost`. Require a strictly positive score plus all non-economic gates. Components must have cash units, documented calibration and no double counting. The execution-bias allowance covers residual model bias, not costs already in the target. Numerical allowance covers approximation only. Do not call the score a confidence bound unless held-out coverage for that exact selected policy supports the claim.

### 8.1 Portfolio eligibility and meaningful final discretion (B)

For origin t, E_t contains only immutable exact actions with complete required facts, independently supported strictly positive conservative net cash L_i, accepted hard account/ownership/permission/joint-stress gates and qualified execution/management. Each action binds quantity, trigger/entry, no/partial-fill policy, stop/exit, horizon, model/cost/source identities and the same origin/account generation. An unsupported alternative remains UNAVAILABLE, not zero-valued.

Expose J_i = L_i - lambda_R * delta_risk_i, with the joint risk increment in cash units on common background paths. The risk price is an explicitly accepted preference, not a test-fitted alpha. Reuse calibrated ES only where its support/calibration qualifies. A separately accepted stress-only policy may set the soft term to zero; a policy requiring ES cannot trade without it. Hard joint stress always applies. Any accepted nonnegative materiality floor is an explicit gate on every member. Include a research NO_OPEN reference; provider failure persists its actual refusal reason.

The compulsory coordinator may SELECT_ONE of **any member of E_t**, REJECT_ALL or ABSTAIN. There is no numerical argmax veto and no uncalibrated near-tie/sacrifice band. Python computes all exact actions and gates. Reasons are source-bound and recorded; invented facts cannot become trusted information or bonuses. Ordinary preference among eligible actions is allowed and is not claimed to improve expected profit. A named materially unverified issue may require abstention under the accepted policy.

After selection/approval, revalidate current account, ownership, risk, permission, exposure, source health, freshness and sealed action. Any material action/evidence change needs a linked new immutable evaluation/eligible-set identity and a response bound to it within the original deadline/budget. No silent reselection. Invalid/late/unavailable coordinator output never invokes a deterministic or paid-provider opening fallback.

Pre-trigger research/watches reduce work but do not authorize selection among future unseen exact actions. A timely response bound to the current exact E is required. Fast setups that cannot fit measured provider latency record a miss; do not extend frozen TTLs. Retain historical conditional-directive readers without granting them new B authority.

Deterministic A and optionally preregistered bounded C remain shadow controls. B may sacrifice accurately estimated value or exploit useful verified context omitted by rank; neither advantage nor universal superiority is proved. Full policy comparisons, including selection bias, failed jobs and costs, decide capital influence.

## 9. Training, uncertainty and profitability evidence

M0's robust center and M1's L1 output are not generally conditional means. The fixture with 99 payoffs of +1 and one of -200 must not produce positive-mean admission: arithmetic mean is -1.01. Squared loss addresses the estimand, not nonstationarity, label bias or tail scarcity. Genuine tail losses stay in the data; corrupt records are handled by documented data-quality rules.

Fit only mature labels whose outcomes were available before the fit cutoff. Separate `ACTUAL_RECONCILED`, `PROSPECTIVE_SHADOW`, `CAUSAL_REPLAY`, `DIAGNOSTIC` and `CENSORED` evidence. Counterfactual fills are estimates. A successful or no-fill unselected action is not an actual execution label. Preserve all attempted variants, missing states and the prospective denominator.

Use chronological train/calibrate/test periods, training-only preprocessing and feature selection, purging for actual label overlap and an explicit embargo. Preserve the historical 24-hour embargo for baseline reproduction. Different embargoes are preregistered experiments, not a silent way to add observations. Split all symbols on the same calendar boundaries and challenge new listings, liquidity and stress regimes. Pool only compatible action/size/liquidity states; do not train a separate model per coin without support.

Calibration is offline. Use chronological cross-fitted residuals and coarse compatible support cells to estimate mean bias/support/OOD for the entire deployed selection policy. Freeze feature/support thresholds, mean-selection allowance and residual execution-bias procedure before final holdout. Inadequate calibration makes economics NOT_ESTIMABLE. No synchronous bootstrap, scenario refit or model search is allowed in the dispatch deadline.

For final complete-policy claims, preregister matched-allocation calendar programme cash, material effect/tail tolerances, trial family and analysis schedule. Evolve separate allocation/position/reservation ledgers for each policy under the same public opportunity and external manual/cashflow stream. Per-action cash admission and the 24-hour static comparison do not price all future capital opportunities. Whole-policy prospective ledgers measure reuse, lock time and actual refusal/expiry/incident costs. A shadow fill model does not create actual counterfactual executions.

Use 10,000 offline calendar-block bootstrap resamples as a numerical-precision starting point, not market sample size. Select dependence treatment on training/calibration data only, retain registered block sensitivities and regard 20 block equivalents as a diagnostic floor, not a power guarantee. Validate null false-promotion and signal power through training-only stress simulations; freeze one-sided tests and Holm family handling before the scheduled final holdout/forward assessment. Holm cannot repair invalid base p-values or regime nonstationarity. Calibrate the entire adaptive selection/tool/management policy in outer chronology; aggregate programme intervals do not establish per-action conditional mean coverage. The eight-week/200-opportunity floor alone is insufficient. Insufficient support remains INCONCLUSIVE/NOT_ESTIMABLE.

Require positive programme value versus no-opening, acceptable tail/risk behavior, execution-calibrated costs and no operational degradation. Claim superiority over R9 or a simpler comparator only if its incremental test passes. A mandatory agent can have operational value without alpha; measure watch yield, evidence errors, avoidable calls, abstention and missed value. Keep the deterministic comparator shadow-only, not an unauthorized live fallback.

Foundation pretraining overlap and opaque hosted updates make forward evidence primary. Record observable provider/model identity, configuration, prompt/schema and usage. A material identity/conformance change quarantines decisions until requalified. No agent self-promotion, automatic live retuning or selection of the best-looking backtest.

### 9.1 Fair authority, information and image comparisons

Use the same corrected mean/economic gates, origin population and available information when comparing A/B/C authority; label historical R9 separately. Maintain independent evolving capital/exposure/fill ledgers with the same starting capital, opportunity definition, execution assumptions and maximum analysis budget. Charge each arm its actual calls and shared costs once; never bill imaginary LLM calls to deterministic control to manufacture fairness. Matched-case diagnostics are useful but simulated alternate fills are conditional counterfactual estimates.

Predeclare numerical/tools-only versus same-information-plus-image, costs, false vetoes, unsupported claims and deadline misses. If an external image adds a new feed/study, separate that information ablation. Use common calendar splits and purging, locked search/holdout, dependence-aware uncertainty, material effect/tail criteria and planned analysis looks. Policies may have different exposure histories; naive per-trade paired tests are insufficient. Prospective whole-programme net cash and risk are primary.

## 10. Shared-account capital and ownership

Account balances come from reconciled exchange evidence, never a fictional configured starting balance. The capital-enabled core profile is single-asset USDT with one-way positions and isolated margin on bot-owned symbols where the exact adapter/account permits it; automatic margin additions are disabled. Observe existing manual cross-margin/other exposure account-wide. Reject an unsupported opening profile instead of changing occupied account settings. Existing research/reader capabilities remain available.

The user explicitly sets a bot allocation ceiling, risk limits and reserves. Deposits or manual profits do not silently expand the bot ceiling. Default permission is OBSERVE, allocation is zero and paid-provider allowance is zero until the user configures them. Missing allocation/risk/permission is a well-defined refusal, not an implementation omission.

Usable opening capacity is the minimum of fresh exchange available USDT, remaining bot allocation and applicable collateral/risk headroom, minus only locally committed increasing exposure not proven reflected, uncertainty reserves and user manual/safety reserves. Exchange available balance already reflects some obligations; do not subtract existing initial margin twice. Define source semantics explicitly at the account adapter. Separate account equity, available balance, eligible bot equity and reserved margin.

For each opening command maintain `UNREFLECTED`, `REFLECTED`, `UNCERTAIN` and `SETTLED` accounting states, evidence IDs and watermarks. A timeout never releases a possible send. Move reservations only on positive reconciliation evidence, idempotently. External increasing ordinary and conditional orders enter possible exposure. Commit reservation, one-use setup consumption, ownership lease and intent together in the existing control database.

Serialize opening reservations and final dispatch checks. Several protected distinct-symbol positions may coexist. The position cap is the minimum of the configured user cap, measured safety/source capacity and current risk feasibility; three is a benchmark configuration, not a universal algorithmic limit. One in-flight increasing command is the initial admitted envelope; a larger envelope requires explicit exact-build simultaneous-uncertainty/protection tests. Network waits never block the safety scheduler. Prove at least two concurrent positions. At approximately $10 the feasible count may be zero; no lot rounding or leverage bypass creates capacity.

Use disjoint manual and bot symbols and the exact proposed `ATLAS_RED_SHARED_ACCOUNT_EXCLUSIVE_AGGREGATE_V1` transition contract in [SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md](SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md). **PENDING OWNER APPROVAL.** A lease manages the complete aggregate, not fictional segregated lots. Account A, position P, ordinary/algo/child order O and ownership W generations, actual/possible quantity, native coverage and current command authority are independent bindings. A local lease does not fence Binance UI/manual clients.

Manual additions, reversals, possible increasing orders and ambiguous fills invalidate unsent entries/optional reviews, retain UNKNOWN/reservations, cancel only proven own increasing remainders and enter conflict. No new aggregate close/repair is authorized solely by matching symbols. Proven monotone external reductions may retain protection-only/time-risk duties for the whole residual **only under the separately accepted reduction-continuity clause** with complete intervening execution/order proof; offsetting trades or gaps fail that proof. Flat observations never settle possible late fills. Stop edits trigger an independent protection incident; repair requires proven whole-position authority and a qualified non-widening mandate, never cancellation of a manual replacement.

Already-installed close-all protection **can close added manual quantity before detection**; explicit-quantity reduce-only orders also affect a fungible aggregate. No software test can prove separate manual lots are protected from those effects. Disclose this residual risk and require owner concurrence before capital. Mandatory reductions remain model-independent but still need proven current ownership/quantity/durability/native semantics; absent scope yields OWNER_INTERVENTION_REQUIRED, not a fabricated successful exit. Valid safety work on unrelated owned instruments continues.

Explicit adoption binds the entire reconciled aggregate, all orders/protection/fees/funding, A/P/O/W, exact mandate, expiry and one-use approval; this simple profile requires all increasing UNKNOWN effects resolved. It is not entry EV, new-risk permission or automatic consent to wider risk. Restart restores conflicts and possible effects; no stale approval or agent result resurrects authority. AC01-08, EX03-07, RB01 and MI01-MI12 supply the exact acceptance oracles.

## 11. Feasible sizing, risk and permissions

Run a cheap feasibility screen before deep scanning or API work. Use Decimal and exact tick/lot/notional filters, adverse-price rounding, fee/funding/exit reserves, leverage/maintenance tiers, liquidation buffer, concentration and joint shocks. Quantity-dependent impact and tiered margin require lot-aligned `loss(q)` and `margin(q)` checks, not a globally constant risk-per-unit assumption.

Risk limits are explicit user-owned settings; optional filters can be disabled visibly. Technical validity, honest evidence, one writer, no duplicate send, ownership, mandatory protection, economic qualification and trustworthy command durability cannot be disabled by FULL_ACCESS. Risk settings are preferences, not proof of safety. Increasing leverage does not reduce loss for unchanged notional price movement.

Include actual, manual, pending and possible exposure in common crypto shocks, idiosyncratic gaps, spread/basis expansion, funding and mark-price/maintenance checks. Deterministic stress is a bounded engineering scenario, not a claimed probability. Probabilistic ES remains diagnostic until calibrated. The exact stress magnitudes, cash/drawdown/concentration limits and liquidation buffers are mandatory bound profile inputs; absent values block capital. Demonstrate simultaneous adverse moves across multiple altcoins rather than assuming distinct symbols diversify them.

Bind separate named risk metrics: net realized bot loss over rolling 24 hours, cashflow-adjusted allocated-equity drawdown, open/possible stress risk and an optional gross-negative-contribution circuit. The recommended standard preference is explicit net-loss plus drawdown limits; a user can additionally select a gross-loss cap where profits do not replenish it. A stricter gross cap is not proved profit-optimal. Retain historical calculations and identities for replay; never retroactively reinterpret old admission. Fees/funding are attributed once, deposits/manual gains do not silently expand allocation, and missing risk configuration blocks capital.

Permissions distinguish OBSERVE, exact-entry approval and bounded autonomous entry; independent scopes govern discretionary exit, whole-position adoption and paid-provider spend. Approvals bind exact action hash, quantity, collar, stop, management/horizon, account/environment, ownership, policy/capability/configuration, expiry and nonce. Any material change invalidates them. No response means no approval.

Pause entries, cancel own increasing remainder, flatten owned exposure, revoke discretionary authority and adopt aggregate exposure are separate typed commands. Revocation cancels unsent authority immediately; it does not erase armed/UNKNOWN effects or immutable stop/time duties. Tightening risk blocks new exposure and applies the configured already-authorized reduction rule. Loosening requires fresh evidence and cannot resurrect old plans.

## 12. Compulsory API coordinator and decision framework

Reuse existing gateway, broker, job/usage records, durable research memory and native Gemma research transport. OD section 5 and ADR-004 retain **DeepSeek as the preferred future candidate**, subject to explicit per-route conformance, price/privacy, model identity and amendment acceptance. Existing Gemma remains a reusable research transport/candidate alternative, not an accepted replacement or trading/VLM winner. Implement one configured qualified route through the provider-neutral domain boundary, with a narrow OpenAI-compatible adapter only if needed for that selected provider. Text/schema/tools/images/usage/deadlines are separately tested capabilities. No compulsory second vendor, framework rewrite, parallel critic or automatic fallback.

The coordinator requests registered workflows, numerical/analogue/chart analysis, watches, verified event rescreens and finite management reviews; it selects any independently eligible exact action under B, rejects or abstains. It cannot change quantities/risk/fees/model promotion, access secrets, author executable predicates or send venue commands. It sees bounded summaries of the full evaluated eligible set and continuation refs. The evaluation budget, token packet and safety capacity are distinct.

States are EVIDENCE_READY, ANALYSIS_REQUIRED/WATCHING/ELIGIBLE_SET, SELECTED/REJECTED/ABSTAINED, optional approval, reserved, dispatched or expired. Current exact E, snapshot, source prefix, account/ownership/permission generations, plan, one-use nonce and absolute deadline are bound through durable records. Pre-trigger analysis is reusable evidence, not authority for future unseen E. Revisions do not reset deadline or call budget. The host refuses stale, changed, duplicate, invented or outside-E selections without fallback.

Starting test ceilings: one provider job, three model calls including continuations/retries, four tool operations, at most one supplementary result round, about 6,000 input tokens, 1,024 output tokens, queue eight and 60 model calls/day. These are bounded ceilings to qualify, not guaranteed throughput or calibrated best settings. Deterministic watch/event invalidation reduces calls. Actual route quota/price and worst-case spend are reserved before dispatch; paid allowance defaults to zero. Lost responses retain possible spend. Charge actual external costs once in programme accounting without deducting them twice from Binance free USDT.

Requests/results have strict fields, bounded refs/assets/uncertainty, signed host identities, actual usage/times and hashes. Native image bytes and bounded tool round trips require real conformance; text-only JSON and a fake SELECT_ONE are insufficient. No arbitrary code/SQL/URL/browser/file/exchange tools. Untrusted source/memory instructions cannot alter policy. On observed outage/429/identity mismatch, invalidate unconsumed opening authority; stops/exits/recovery continue independently. Fresh conformance and all independent healthy latches are required to rearm; no stale setup resurrection.

### 12.1 Capable typed research tools and derived chronology

Preserve existing names/read receipts as compatibility aliases where appropriate. Use one registry and common gateway, not eight workers or agents.

| Tool family | Exact allowed request | Bounded result and genuinely new information |
| --- | --- | --- |
| `inspect_market` | Instrument refs, completed intervals, registered structure/feature IDs, <=256 bars | Existing facts or a registered causal calculation (e.g. confirmed structure); no arbitrary indicator code |
| `assess_execution` | Exact action/quantity/side, registered quote/depth profile | Executable collar/depth/partial-fill and cost diagnostics; current price acquisition requires a new snapshot if beyond origin |
| `inspect_relationships` | Instrument, BTC/ETH and <=3 registered peers, <=720 completed common H1 observations | Aligned beta/correlation/residual/overlap estimates with uncertainty/missingness; no all-pairs computation |
| `retrieve_analogues` | Compatibility key, origin prefix, fixed distance version, <=20 neighbours | Previously unshown compatible states/outcomes available by origin; never protected future labels |
| `inspect_economics` | Exact action/evaluation/model refs | Paginate/check already computed costs, support and scenarios; no new model chosen for a favourable score |
| `verify_event` | Registered source/event/span refs and proposed asset IDs | Verified publication/availability/mapping/conflicts; no arbitrary URL or model-invented source |
| `compare_management` | Owned position/epoch, registered alternative IDs, <=8 optional alternatives plus safety actions | New hold-versus-action future-value calculations with precise quantities/stops and current ownership; no execution effect |
| `request_registered_work` | Allowlisted work ID and bounded immutable parameters | Queue registered numerical challenger, chart or cheap rescreen work; no recursive child request or automatic model call |

Common request binds job/parent ID, policy/tool version, snapshot/population, action/position scope, original raw-market prefix, derived-input cutoff, absolute deadline, rows/bytes/CPU/model/call/cost budget and idempotency key. Common receipt binds canonical validated parameters, exact input refs, calculation version, actual start/finish/availability, resource usage, output hash, units and missingness. Status vocabulary: COMPLETE, PARTIAL, UNAVAILABLE, STALE, RESOURCE_DENIED, BUDGET_DENIED, DEADLINE_EXPIRED, NEEDS_NEW_SNAPSHOT, INVALID and ERROR.

Default tool response cap is 64 KiB, maximum 512 fact rows, four tool operations and at most one supplementary result round per decision job, within the existing three-call absolute model ceiling. CPU per synchronous cached/read operation is admitted against a measured sub-budget; expensive calculations become cancellable due work. A five-second job does not gain five extra seconds per tool. Row truncation is explicit with a continuation ref and cannot hide mandatory contradictions.

The registry initially enumerates CONFIRMED_STRUCTURE, COLLAR_DEPTH_COST, BTC_ETH_PEER_CONTEXT, COMPATIBLE_ANALOGUES, EXACT_ECONOMICS_READ, M0_MEAN_PREDICT, M1_MEAN_PREDICT, HAR_VOL_FORECAST, FORWARD_KALMAN_STATE, AR1_HALF_LIFE, FOUNDATION_FORECAST with one of the three exact manifests, MANAGEMENT_COMPARE, EVENT_VERIFY, CHART_SNAPSHOT and CHEAP_EVENT_RESCREEN. Predict tasks use prefit artifacts; none authorizes training, protected-holdout access or model promotion. Cached reads start with a 20 ms CPU/50 ms wall admission target; slow reads/calculations move to bounded async work with explicit per-operation limits and the unchanged outer deadline. These are measured admission targets, not guarantees that a blocked disk call is interruptible; execution safety has its independent owner/process and protected service. Each optional request names the missing fact or calculation and intended decision use so the scheduler can refuse expensive duplication.

Derived computation can finish after the market prefix using only earlier raw inputs. Reuse `DerivedComputationChronologyV1`: immediate-input availability <= input cutoff <= start <= finish <= output availability <= consumer deadline; raw dependencies remain <= original market prefix. A newly acquired raw fact learned later requires a new snapshot/slot, not a backdated receipt. Existing three tools mainly re-read sealed Brief contents; their informational contribution is pagination/explanation. Newly registered calculations/acquisitions are the actual incremental-information capability. [E9]

### 12.2 On-demand chart and image evidence

Deliver one actual chart path and actual pixels into a qualified VLM, sharing ChartEvidenceBundle with numeric tools. The bundle binds venue/environment/perpetual revision, price basis, completed UTC bars, actual/reconstructed availability, source prefix/health, indicator seeds and confirmation times, approved levels, disclosure rights, template/renderer/model identities, hashes, bounds and deadline. A renderer only draws supplied values; it cannot compute trading truth.

**Technology remains conditional until target-host qualification.** The serious candidates are isolated static mplfinance/Agg and a supplied-data external renderer such as QuickChart. The earlier synthetic local 97-103 MiB / 1.45-4.05 s cold result is diagnostic. It is not a Windows/Linux Celeron measurement. Build a small comparison harness/prototype, not two compulsory production providers. Compare the same bundle under actual admitted workload: full process-tree memory/commit, cold/warm service tails, safety interference, install/font/cache/temp footprint, network/privacy/licence/cost, source parity, end-to-end image-model latency and outages. Select the simplest passing route and implement/qualify that one. If neither qualifies, CH01 is BLOCKED, not deleted.

Numerical evidence is the normal representation and declared per-decision fallback. No image is required on a safety path or every trade. No browser/GUI dependency to feed the agent. Images from independently fetched TradingView data, including CHART-IMG, are display-only until exact mapping/cutoff/study provenance qualifies. No automatic provider switch, cookie scraping, arbitrary URL/config/code or paid fallback.

Initial request ceilings remain test inputs: one legible image/job, at most 256 total displayed bars, 1280x720, 1 MiB encoded / 4 MiB decoded; binary bytes separate from 64 KiB text-tool results; an initially bounded 2 MiB provider wire envelope accounts for base64. These caps may be revised only through measured profile acceptance. Original outer deadlines never reset. Existing desktop bounded history is preserved separately.

One schema and coordinator, with strategy-specific neutral questions and numeric companions: S1 pullback, S2 breakout/failed break, S3 reversion/range, S4 source-based flow, S5 verified crowding, S6 relative strength/peers, S7 event context, S8 pair research and owned-position reviews. Charts cannot reconstruct book/trades, OI/funding, liquidation coverage or news availability.

Responses give AGREES / CONTRADICTS / AMBIGUOUS / INSUFFICIENT, bounded claims, bar/swing/level/source refs and uncertainty. Host verification checks identity, timestamps, actual image/bundle binding and registered predicates. Unsupported claims remain research hypotheses; model confidence is not probability or economic support. No image-derived executable numbers, mean premium, gate override or independent management authority.

Images of X add no raw market observation beyond X; they may help a constrained model interpret shape omitted by summaries. Test numerical/tools-only, same-information-plus-image and deterministic selection on blind matched cases and feasible prospective programmes, including false vetoes, missed deadlines and all costs. The complete diagnostic image capability remains delivered if capital influence does not earn promotion. Keep one provider-neutral domain interface and the reusable native Gemma transport. DeepSeek is the owner-preferred future candidate; exact selected text/tool/image route remains a conformance and acceptance gate, not a mandated qualified winner. No always-on second critic/model committee or local VLM.

ChartRequest binds job/set/action, instrument/venue/environment, registered template, source/indicator refs, original raw cutoff and completed bars, dimensions, budgets, privacy licence and deadline. ChartResult binds source/bundle/template/renderer/image hashes, real bytes and MIME, times, actual usage, numerical companions and explicit missingness. Cache identity uses immutable dependencies, not TTL alone. A hash or filename without bytes reaching the model is not image delivery.

Use shared neutral prompts per strategy, with deterministic numerical predicates and fixtures for every factual claim. Structure decisions are bounded observations, not model-generated stop/entry numbers. Reject active/future bars, noncausal pivots/studies, incorrect price basis, mismapped contracts, unsafe decode/redirects and unregistered renderer configuration. One implementation route is selected after evidence; the unselected route is an evaluated alternative, not an unfinished core provider.

## 13. Brief, memory, watches and effort scheduling

Reconstruct typed production readers for completed candles, technical/state/structure, liquidity, derivatives, source health, BTC/ETH context, events, prior analysis, watches, invalidators, due work, outcomes, exact-action economics and available forecasts. Required/optional dependencies belong to a per-job policy manifest. Do not make every optional forecasting model a universal prerequisite.

An analysis-complete Brief can contain explicit unavailable economics and still support collection/research. A capital-complete packet additionally requires current eligible economics and every dispatch gate. Remove neither the current completeness guards nor provenance checks until real required readers replace them. Preserve legacy fail-closed fixtures and add genuine successful-path evidence separately.

Outcome due_work and watch-specific ops_outbox retain their typed contracts and S8/maturity priority. They are not repurposed as a generic agent queue. Reuse the namespaced AgentJobRepository/research-memory index; add a distinct bounded typed research-task projection only if existing contracts cannot represent the due event. No new database/scheduler platform is justified (Agent section 7, ADR-007 sections 1/4/7).

Watches persist instrument/policy, setup epoch, side, creation evidence, trigger/invalidation IDs, next due event, absolute expiry, last evaluation and attempt latch. Evaluate native events, not repeated chat polling. Deduplicate by watch/event/state. Restart replays causally, expires old triggers and preserves consumed attempts. A new model/configuration does not reset an economic setup.

The deterministic effort scheduler uses SAFETY_ONLY for critical unknown/conflict/resource incidents; CAPITAL_WAIT for no feasible opening/no useful watch due; WATCH_ONLY for useful constrained due work; ACTIVE for feasible capacity and healthy qualified inputs. Every mode maintains owned-position safety. Use hysteresis to avoid resource churn; manual margin consumption invalidates openings immediately. Persist fair strategy clocks, prospective slots, missed reasons and the full refusal funnel.

### 13.1 Adaptive work, verified news rescreens and one manual effort control

Separate immutable origin population, evaluation work trace and the final eligible set. Cheap screen all observed eligible products at their registered clocks. Keep infeasible live actions visible; allow separately labelled reference-size research. Use cheap available feature/risk/cost proxies for scheduling only, never final admission.

For expensive work, initialize with the top two available priorities, then sequentially choose the next candidate by a frozen deterministic priority function: potential improvement over current best using a training-only calibrated proxy plus uncertainty/age bonus, less measured compute/acquisition cost. A fixed round-robin exploration lane receives at least one admitted optional unit per complete configured coverage cycle when resources permit. Unknown proxy support receives explicit exploration/unknown status, not an invented upper confidence bound. No new learned value-of-information model is necessary.

An implementable ordinary-lane priority is `max(0, proxy_i + kappa*s_i - best_J - incremental_future_cost_i) / (estimated_ms_i + 1)`, with stable older-work/ID ties. `proxy_i` and `s_i` are compatible origin-known scheduling summaries, not eligible economics or a claimed confidence bound. `kappa`, proxy/missingness buckets and the cash improvement floor are fixed on training/calibration data in the registered scheduling policy. A separate due-exploration unit overrides ordinary priority by rotating strategy/liquidity/origin strata. Set the work-time budget from original time remaining minus reserved safety, measured direct-agent and dispatch allowances (reserve measured time for the required response bound to current E even when pre-trigger analysis exists). Stop on exhausted resources/deadline, or on the registered marginal-usefulness floor while still recording every untested member. This heuristic is cheaper than a learned Bayesian VOI layer and can miss good opportunities; the fourth-plus-winner and matched-compute coverage tests are its qualification.

Continue only while original deadline, safety reserve, evidence/model/API budgets and marginal-usefulness threshold permit. A hard emergency count/byte bound still exists in configuration, derived from measured throughput; it is not a permanent 'top three' trading rule. The final API packet has its own token/page limit and a summary index of the full eligible set, with bounded continuation. Omitted from evaluation means NOT_EVALUATED_RESOURCE/DEADLINE, not no opportunity. Selection calibration covers this entire adaptive policy.

Warm histories are capacity-admitted: pin BTC/ETH, owned positions and still-useful due watches, then allocate remaining slots with age/exploration, setup quality, warmup cost and hysteresis. Approximately 20 is a benchmark seed, not a fixed coin list or immutable maximum. Deep books similarly prioritize positions and active hypotheses. If pinned mandatory work does not fit, stop new exposure; no silent loss of required position evidence.

News rescreen uses a verified canonical event revision and registered source/asset mapping. Deterministic mappers nominate obvious assets; the agent can nominate missed linked symbols with source spans. Reject ambiguous tickers, stale/retracted sources and unsupported contracts. Default maximum three proposed symbols/event, one same-event/symbol/policy rescreen, five-minute per-symbol cooldown, one event-follow-up depth, six cheap rescreens/minute global ceiling plus the admitted optional budget. Expiry is `min(event_policy.valid_until, accepted_at + 60 seconds)`; the news-reaction profile admits sources first available within its registered ten-minute freshness window. Scheduled future events use their separate calendar policy. These are testable initial anti-storm limits, not claims of optimal cadence. Mandatory risk/protection invalidation is not throttled by the research cooldown. Revisions can invalidate existing watches but do not erase prior events.

Persist a **new future** event/rescreen slot after acceptance with origin type, availability, parent event/job, mapping, due time, expiry and relationship to the setup episode. Agent work cannot insert candidates into a sealed slot or reset a consumed breakout. A rescreen may create a watch for the next native strategy clock; it does not invent a news-entry policy. Preserve both scheduled and supplemental calendar counts, deduplicated economic episodes and refusal/missingness.

Manual effort is one LOW/NORMAL/HIGH preference with optional-work token-bucket refill multipliers 0.25/1.0/2.0, capped by the measured worker/network quotas and explicit user/provider spend ceilings. It changes no risk number or paid cap. Record runtime changes prospectively as configuration events; existing job scopes stay immutable. Retain backward CONSERVE/STANDARD config readers.

| Situation | Required behavior |
| --- | --- |
| LOW/NORMAL/HIGH | Alter only optional job rate/priority within current caps; same mandatory cheap observation, clocks, safety and outcome maturation |
| All bot capital reserved | Keep safety, observation, denominator and useful watches. Rich opening analysis only if a reference-size research task is useful or capacity may free before its expiry |
| Active positions | Their source/protection/exit work is pinned and independent of slider/provider quota |
| New altcoin while no free capital | Permit cheap verified rescreen and a useful expiring watch; record NO_CAPACITY for actual entry, diagnostic size separately; no financed close by implication |
| Provider quota/resource pressure | Effective effort=min(user preference, automatic admission); show requested and effective settings/reason; stop optional dispatch, never drop evidence invisibly |

The actual production Brief must contain independent BTC **and ETH**/peer readers with aligned common-time observations, support/uncertainty and missingness; the BTC-only research cache is not sufficient. Preserve its historical semantics. Native intraday clocks are separate from ScannerRunner's four-hour V1 APIs. Connect one prospective ledger to each actual due clock; reuse registration/miss/dedupe semantics without changing the clock identity.

A live optional-effort change is a prospective validated persisted command/config event. Preserve historical CONSERVE/STANDARD immutable run-config hashes/readers; do not mutate them. Requested and effective effort are distinct. Mandatory ten-second sources, strategy clocks, mature outcomes, ownership/protection and valid reference research persist even when capital is full.

## 14. Binance execution and protection

Reuse Nautilus and the existing narrow native gap adapter through one execution owner. Bind the exact build, account/environment, order modes and native capability contract. Binance conditional stops use a distinct algo service with query/cancel/child identities; ordinary order ACKs do not prove native stop coverage. [E6]

Entry uses the existing frozen marketable LIMIT IOC policy where qualified, with explicit price collar, deadline, partial/no-fill behavior and taker costs. An existing five-second decision deadline or five-basis-point collar is an inherited hypothesis to validate, not a guaranteed fill or cost bound. Alternative timing/entry policies need their own evidence and may not silently replace the baseline.

Before transport, durably record the exact intent, reserve capacity and consume the setup; then durably arm dispatch. Deterministic command identities and idempotent reducers prevent duplicate effects. Classify definite rejection separately from ambiguous send. Never blindly retry an UNKNOWN opening; reconcile ordinary/algo IDs, executions and positions with positive evidence.

The capital capability manifest must record native stop type, trigger basis, price-protection flag, quantity/close semantics, instrument/side binding, readback and partial-fill adjustment. Accept only the tested combination; do not assume linked brackets/OCO or pre-armed atomic protection. Retain a valid closing stop while a mandatory exit is underway unless its proven side/quantity semantics are hazardous.

Monitor possible exposure from SEND_ARMED, including unknown entry sends, rather than only a delayed fill callback. Preserve the two-second alarm/action threshold: start the declared repair/cancel/reduction response when coverage remains unconfirmed. Separately measure exchange fill time with clock uncertainty, local observable effect, first safety action, native ACK and verified coverage. Qualify a healthy coverage-latency distribution; do not reinterpret an alarm as a guaranteed two-second fill-to-verified bound. Host/venue/network outages can prevent action; neither stop trigger nor ACK guarantees execution or maximum loss. Explicit acceptance of reactive fill-to-protection residual risk remains required.

Coalesce mandatory exit goals to TARGET_FLAT, cancel increasing remainder and reduce proven owned exposure without waiting indefinitely for another unknown order. Preserve possible quantity and reconcile continuously. Never let an UNKNOWN on one symbol starve uncovered known quantity on another. Read actual quantity before every closing command, preserve no-reversal and handle dust/partial/duplicate/out-of-order fills.

Flat certification requires proven terminal increasing orders, reconciled executions and no possible future increasing fill, not one zero snapshot. Cancel only proven own obsolete protection. Do not touch unrelated manual orders.

Current `BinanceDemoIdentity` supports DEMO/TESTNET only. Add a strict distinct accepted production environment/capability contract and native configuration path with exact endpoint/account/mode isolation; keep Demo-only tests/readers intact. Do not treat a UI flag as production support. SDK arguments are not wire proof: inspect actual pinned-adapter serialization, especially closePosition quantity/reduceOnly exclusions, and readback coverage. New code capability does not authorize private reads/orders.

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

Keep control SQLite WAL/FULL, physical writer lock, durable-before-send, no-reversal, typed reconciliation/flat certificates and uncertainty-preserving recovery. OpsRepository owns research/due work; AgentJobRepository owns provider jobs; immutable compressed Arrow extents and bounded DuckDB research are reused. Connect stores through immutable hashes and idempotent outbox/inbox consumption, not imagined cross-database ACID. Only the control transaction reserves capital and one-use dispatch authority.

All storage is bounded, including DB/WAL, raw/bar/feature data, model artifacts, downloaded/runtime packages, images, fonts, logs and staging. Active control/unresolved effects and claim/training dependencies are pinned. Export is verified and atomic; a missing original changes replay grade and invalidates dependent claims. If required pins cannot fit, stop affected work before deletion. Two GiB hot and 33 GiB total are **unpassed benchmark seeds**, not the selected capacity promise. Measure mandatory ten-second breadth bytes/day and eight-week evidence needs, compression, rollover and installation peaks; set explicit owner-approved caps/reserves from that evidence. No hidden unlimited folder.

Measure total process-tree PSS/private memory/commit and service tails on actual 4 GB Celeron Windows and Linux, including startup, peak admitted load, optional worker contention, source continuity, native safety, sleep/clock jumps and at least 72h retention rollover. Do not relax the old 200ms storage or 80ms projection limits merely to obtain a pass. If a new measured service contract is genuinely necessary, document its safety/coverage consequences and obtain explicit acceptance. Absent hardware/venue/checkpoint/provider evidence remains BLOCKED BY ENVIRONMENT.

Working measurement seeds remain: core steady memory below 900 MiB; stop admitting research/openings near a measured 1,250 MiB application tree; 128 MiB durable emergency allowance; 64 MiB raw ring; one optional numeric worker and one provider job. These values must be checked against actual total private/PSS/commit, OS reserve and safety tails; they do not override mandatory service gates or qualify a 4 GB host. Chart decode/base64/runtime peaks and UI history are counted independently.

Measure a full managed-byte inventory: control/ops/provider SQLite and WAL, recent raw bars/features, evidence archive/dependency pins, model/tokenizer minimal loader files, installed runtimes/packages, fonts/cache/logs and staging/copies. The historical 2/20/1/8/2 GiB hot/archive/model/runtime/staging split and 33 GiB aggregate with 2 GiB physical reserve are **unpassed comparison seeds**, not an accepted retention envelope. Forecast actual ten-second bytes/day, eight-week pinned campaign, compression/rollover and worst installation/export peak; accept caps only after those fit. Stop affected collection/claims before required pins exceed the cap. No unlimited external folder or waiver.

Retain SOURCE_REPLAYABLE, DECISION_CAPSULE_REPLAYABLE, SUMMARY_ONLY and EVIDENCE_MISSING grades. Exact inputs/models/output in a capsule do not recreate deleted raw sources. Verified export is atomic and checked before eviction; active obligations/control and claim/training dependencies cannot be discarded for a new opening. Bound read transactions, DuckDB work and WAL growth; never fsync every quote.

Retain Python 3.12+, Nautilus 2.0.0rc5 baseline, PyArrow, DuckDB, Qt/PyQtGraph and existing lock/build identities. Qualify chosen supported adapters and patched linked SQLite rather than replace frameworks. Optional model/image frameworks never import/install on safety startup or entry deadlines. Pin minimal supported files/hashes/licences, reject hidden downloads, count cold peak and support/cancel evidence. No compulsory Docker or local LLM. Clean install/start/stop/recover, migration/old serialization and target platform recipes are required.

## 17. Master event algorithm

The following is the exact proposed event order; named operations are required contracts, not claims that similarly named new functions already exist.

```text
START:
  acquire the one physical credential-bearing dispatcher fence
  validate build/config/policy identities, accepted authority, stores and capacity
  recover durable commands, possible effects, reservations, ownership and native stops
  reconcile ordinary/algo/child orders, positions, permissions and flat evidence
  preserve UNKNOWN; do not rearm until every independent required health latch clears
  start independent protection/reconciliation/mandatory-exit scheduling
  default OBSERVE, zero bot allocation and zero paid allowance

EVERY SAFETY / ACCOUNT / ORDER EVENT (priority independent of provider/data research):
  normalize exact venue/environment/instrument/epoch and deduplicate provenance
  persist authenticated observation and reduce possible-effect state conservatively
  update A/P/O/W, actual/possible quantity, balance reflection and manual transitions
  invalidate unsent optional/open authority; preserve UNKNOWN and existing native scope
  a new close/repair needs current proven aggregate authority; otherwise record intervention
  progress required protection / TARGET_FLAT / time exits within proven authority
  cancel only proven own stale/increasing orders; never silently cancel manual orders
  preserve no-reversal, native coverage/readback and positive flat proof
  release only proven resolved effects; optional work cannot block these operations

EVERY MANDATORY TEN-SECOND BROAD REFRESH:
  collect admitted full cheap contract population, metadata/mark/ticker/source health
  record actual expected/received/missed clock slots and causal available-at
  keep BTC, ETH, owned-position requirements, native clocks and mature outcomes
  apply current optional-effort preference intersected with automatic health/capacity
  rotate warm/deep exploration fairly; record denied/missing work rather than false NO_OP

ON NATIVE STRATEGY / VERIFIED EVENT / WATCH DUE SLOT:
  freeze origin population, raw prefix, original deadline and setup epochs
  derive only causal confirmed inputs; gaps/new raw facts require new identities
  generate finite exact-action hypotheses at their established S1-S8 roles
  adaptively evaluate within measured CPU/byte/source/cost and deadline envelope
  for EACH evaluated action, independently:
    size on current account/allocation/ownership/lot/margin and joint-stress limits
    freeze exact entry/no-fill/partial/stop/management/horizon and action hash
    read compatible immutable prefit mean/support/uncertainty/cost/scenario artifacts
    compute conservative net-cash and accepted risk objective; never fit on deadline
    record ELIGIBLE / failed gate / UNAVAILABLE; do not depend on prior final winner
  append adaptive work/refusal trace, unexamined population and all outcomes
  seal exact E with current required evidence and expiry; if empty record reason
  compile complete per-job typed Brief from real readers, source provenance and memory
  if mandatory reader or compulsory API admission fails: no opening, explicit reason

COMPULSORY COORDINATOR JOB (within unchanged original budget/deadline):
  expose bounded pages/index of E plus trusted facts and missingness
  accept allowlisted bounded tools / watches / registered work requests
  new raw fact => linked new snapshot/set/job within same remaining outer budget
  derived result => prove raw prefix, calculation/input cutoff and real availability
  demand-only chart => shared bundle -> one qualified renderer -> actual model pixels
  independently verify structured claims; unsupported claims remain hypotheses
  SELECT_ONE any exact E member, REJECT_ALL or ABSTAIN under proposed B
  strict schema/ref/expiry/identity/budget validation; no fallback trading
  persist exact response/set/usage/result hashes and bind one-use selection

ON SELECTED EXACT ACTION AND ANY REQUIRED USER APPROVAL:
  execution owner reads fresh account/possible exposure/ownership/permission/source
  revalidate exact action, mean/support, lot/fee/funding/filter revisions and deadline
  if materially changed: no dispatch; new immutable evaluation and coordinator required
  in ONE control transaction consume approval/selection + reserve capital/lease/setup
    + persist exact intent and outbox; duplicate or stale generation fails
  one dispatcher arms durable command, sends at most under qualified IOC semantics
  uncertain send => UNKNOWN, retain all possible effects; never naive opening retry
  fills/remainders/coverage proceed through independent safety lifecycle above

ON OPTIONAL OWNED-POSITION REVIEW:
  apply the exact manual-conflict/reduction/adoption state machine first
  freeze current A/P/O/W, proven ownership, remaining quantity and sealed policy
  enumerate HOLD and accepted finite CLOSE/PARTIAL/TIGHTEN/BE/TRAIL/TP alternatives
  compare incremental future wealth versus HOLD on common causal paths and costs
  agent may request only registered eligible optional management; all host gates apply
  bind exact reducing quantity, monotone stop, horizon, one-use trigger and permission
  serialize one unresolved optional action/epoch; TARGET_FLAT always supersedes it
  use qualified atomic replacement or create->verify->cancel safe overlap
  unavailable replacement capability => retain old protection and record NO_CHANGE
  reconcile stop/TP/partial/cancel/manual races without reversal or fictional lots

ON OUTCOME / MAINTENANCE:
  mature/censor labels honestly; retain full opportunity/miss/selection denominator
  update research memory, watches and deduplicated future event rescreens
  fit/calibrate/search only in bounded isolated research with locked chronology/holdout
  retain all selected real forecast runners as research; never vote/shop/promote online
  checkpoint/archive/export with verified pins, replay grades and physical reserve
  publish bounded truthful UI/IPC/evidence; do not give display process control writes

ON COMPOUND FAILURE / RESTART / CAPACITY LOSS:
  expire unconsumed opening authority, shed optional work and retain possible spend
  protection/recovery continues by its own healthy capability; do not claim SLA in outage
  trusted-storage failure invokes accepted terminal no-new-command policy
  host loss relies on last verified native protection; stops do not guarantee loss bounds
  reopen from durable truth and fresh queries; old approvals/setups never resurrect
  manual intervention/host replacement requires ownership/fencing and explicit policy
```

Every outcome retains source/build/action/model/config/provider identities, actual clocks, costs, evidence grade, refusal/miss reason and exact artifact hash. Control, ops and provider databases use idempotent hash-linked messaging; only the control transaction grants dispatch authority. No cross-database ACID or free-capital assumption is implied.

## 18. Completion, qualification and capital decision

Engineering completion requires every core row to have working code, meaningful tests and exact-build evidence, without placeholders. Include all selected numeric runners, typed calculations and one qualified chart/image route, verified rescreens, the manual effort backend/UI, eligible-set B selection and finite advanced management/rotation research. Qualification of authority is separate from implementation completion. Missing checkpoint/provider/host/venue evidence stays explicitly blocked. No existing working baseline function or old reader is silently lost.

Operational qualification requires a genuine complete shadow cycle and at least 72 hours on actual declared Windows/Linux minimum hosts, including startup/backfill, peak admitted load, retention rollover, UI/headless operation, provider faults and compound incidents. Record process-tree memory, lag percentiles, missed slots, CPU pressure, WAL/storage high-water, API usage and protection scheduling. Test at least two concurrent Demo positions, manual interference, partial/unknown/native-stop/restart lifecycles on the exact build. Demo is not evidence of live fill economics.

Capital additionally requires a feasible actual account/lot profile; accepted reactive-protection residual risk; current permissions/allocation; exact venue capability; positive prospective programme value and required comparator/risk evidence. Any separately authorized tiny live execution-calibration experiment has its own loss/spend cap and is not labelled profitable production. This documentation does not authorize such an experiment.

**Target verdict:** documentation is finalized for independent acceptance with explicit owner-approval gates. CONDITIONAL GO for the five-session engineering forecast **after acceptance and separate offline-code scope authorization**. This documentation session starts no implementation. NO-GO for live capital on current evidence. No guarantee of maximum profit or universal superiority is made. Evidence may justify a simpler active trading policy while the full research/control capabilities remain delivered.

## 19. Sources and authority

The actual original owner-source reconciliation and exact file/section hashes are [AUTHORITY_RECONCILIATION.md](AUTHORITY_RECONCILIATION.md) and [SOURCE_AUTHORITY_MANIFEST.json](SOURCE_AUTHORITY_MANIFEST.json). OD denotes the 12 October owner-decisions document; TL the target lock; TA the transition architecture; RB the engineering runbook. Full filenames/sections are in that source table. The precise proposed manual/adoption contract is [SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md](SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md).

This target preserves historical freeze bytes and saved-artifact meanings but does not assume old trading rules are optimal. An operative change needs the exact independently accepted technical amendment, migrations/replay/rollback and qualification record in ATLAS_OPERATIONAL_RECONCILIATION_AMENDMENT_PROPOSED.md. Its initial status is PROPOSED, not accepted by this review. Owner agreement to design direction does not alone certify a new selector, tool, management, account or storage authority. The exact proposed policy identity is `ATLAS_RED_ELIGIBLE_ACTION_COORDINATOR_V1`; its effective date and accepted authority are NONE. The actual supplied original Red reference sources are reconciled in AUTHORITY_RECONCILIATION.md and identified in SOURCE_AUTHORITY_MANIFEST.json. The precise new manual/adoption policy and operative amendment still require explicit owner/independent acceptance; source reconciliation is not that consent. Technical contract revisions are not competing product versions.

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

Primary external sources were checked during the source consultations, with Binance, SQLite, Gemma and forecaster interfaces rechecked for this consolidation on 16 October 2026. Model availability, quotas, prices, venue rules and installed runtime facts must be checked again at qualification. This document's new design procedures and numerical budgets are engineering decisions and unpassed acceptance targets, not claims made by those sources.

- E8: [CHART-IMG official API](https://doc.chart-img.com/); no causal/latency qualification implied.
- E9: [Existing derived chronology contract](https://github.com/KasunKarandagolla/Atlas/blob/3436dde4ca5169d0cdec6befe8a485c0c0e992b2/docs/v2/SESSION037_DERIVED_RESEARCH_CHRONOLOGY_CONTRACT_V1.md).
- E10: [White, A Reality Check for Data Snooping](https://users.ssc.wisc.edu/~behansen/718/White2000.pdf).
- E11: [Wang et al., off-policy evaluation](https://proceedings.mlr.press/v70/wang17a.html); trading is more stateful than its contextual-bandit assumptions.
- E12: [TiRex file manifests](https://huggingface.co/NX-AI/TiRex-2/tree/main), [Chronos-small files](https://huggingface.co/autogluon/chronos-2-small/tree/main) and [Kronos-mini files](https://huggingface.co/NeoQuasar/Kronos-mini/tree/main); listed sizes are not RAM measurements.
