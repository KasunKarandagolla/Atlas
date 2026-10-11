# Draft only: ATLAS research-agent and model boundary amendment

**State: PROPOSAL FOR INDEPENDENT REVIEW. NOT ACCEPTED, NOT IMPLEMENTED, AND NOT AUTHORITY TO TRADE.**<br>
This document describes the contract and migration work required before any transition beyond the existing frozen V1/V2 identities. It does not alter the three accepted freezes or current runtime defaults.

## 1. Proposed identity and bounded purpose

If independently approved, introduce a separately versioned, capital-disabled research identity, provisionally `AtlasResearchDecisionV1`, keyed to:

- immutable base opportunity/candidate-set hash, strategy and source-profile identities;
- exact model/provider/framework identity, model/checkpoint revision and artifact hashes;
- exact structured schema, prompt/tool profile, tool registry, parameter set and policy version;
- evidence refs, availability cutoffs, run/job id, event and monotonic deadlines, result/receipt hash;
- forecast horizon or management decision interval, venue/product scope and action-calendar slot.

The boundary may choose one already registered candidate, reject, abstain, or request one of explicitly registered bounded read-only tools. It may not invent a candidate, search arbitrary shell/network/files, create eligibility, alter size/leverage/risk, submit or cancel an order, edit a protective stop, waive source health, or backdate a decision. An expired, malformed, contradictory, incomplete, out-of-distribution or unavailable result maps to `ABSTAIN` with a durable reason; the deterministic baseline remains independently available and unchanged.

## 2. Proposed change register (each row needs separate review)

| Contract area | Frozen/default behavior | Proposed review item | Required new identity and tests |
|---|---|---|---|
| AI candidate selection | Existing deterministic selector and CandidateSet remain authoritative | Select one registered S1/S2 or future registered candidate, reject, abstain, or request a bounded tool | Selector policy/schema hash; same-slot exclusivity, all-disagree, contradictory candidates, no-candidate/excluded denominator, late/duplicate/invalid output, human review and baseline parity |
| External discovery | No arbitrary scanning or unbounded discovery | Separate exploratory feed/universe/model proposal with no direct candidate authority | Discovery source, universe denominator, filter and budget identities; blind-spot/exploration audit before and after cheap filters; no scanner top-K selection bias |
| Model and vision routing | No AI numeric authority; numeric evidence remains sourced | Optional local model or image/text analysis as an advisory sidecar; paid provider only after exact explicit spend authorization | Provider/model alias pin and allowed drift policy, image preprocessing/render hash, tokens/cost ceiling, prompt and schema identities, `NOT_AVAILABLE` fallback, adversarial chart transformations and numeric source cross-checks |
| Agent framework/provider | Existing broker boundary and fixed operation contracts | Optional Pydantic AI or direct structured call behind broker; no candidate-owned tools | Framework/dependency lock and license/SBOM, tool-capability identity, timeout/deadline, concurrency/reservation and retry policy, late charge and unknown outcome semantics |
| Position management | Frozen deterministic protection/lifecycle and V1 no-pyramiding rules | Advisory management suggestions; dynamic TP/SL, move-to-BE, trailing, partial TP, hold/reduce/close or limits are independent variants | Each management policy has own identity and isolated shadow cohort; cannot change native protection authority; fixed baseline paired against each variant |
| Forecast/ML | No frozen model forecast authority | Add Kronos or another forecast only as a swappable research feature | Dataset/vintage/timeframe, feature availability and sampling identity; model+tokenizer hashes, seed/sampling, horizon, calibration, baseline identity, runtime/resources, train-overlap evidence, no trade/fill claims |
| Time horizon/window | Current exact M15 close+5s and frozen action calendar | Research alternate window or holding duration as a new policy | Signal horizon, close definition, maximum age, decision expiry, outcome maturity, event calendar and data-vintage identities; preserve matched complete calendar and missed opportunities |
| Multi-venue / product discovery | One active public Binance USD-M USDT-perpetual owner profile; both-venue public research optional only | Candidate research on other venue/products | Venue/product metadata revision, denomination/settlement semantics, independent source health/sequence, fee/funding evidence, universe denominator and exclusion archive; no routing/capital authority |

## 3. Scope, tool and authority contract

Every proposed request is a content-addressed snapshot. Tools, if any, are named and typed in a reviewed allowlist. They read only registered, cutoff-bounded evidence references and have explicit row/byte/time limits. Caller-provided prompt, source/news text, chart content and tool output are data, not instructions. No recursive or model-chosen arbitrary tool name, URL, file path, SQL, Python or shell execution is permitted. Credentials and account/position state are unavailable to this interface.

The output is schema-validated and then cross-checked against the exact request id, candidate identities, evidence refs and available-through cutoffs. Unknown fields, invented references, invalid enum values, evidence not yet available, unsupported numeric claims, duplicate result or model/provider identity mismatch produce a retained invalid/abstain event. The agent has zero independent execution or protection authority. All actions remain capital-disabled shadow proposals unless an entirely separate future authorization changes that state.

## 4. Causality, replay, and ledger behavior

The immutable event must distinguish source event time, claimed publication time, receive time, persist time, availability time, processing time and monotonic deadline. Unknown timestamps stay unknown. Replays use the exact source snapshot and original feature/strategy/selector/prompt/model/tool versions. Late evidence and late model output cannot revise the past decision; revised documents create new evidence revisions while old snapshots remain auditable. Candidate sets retain selected, rejected, unselected, missing, contradictory, no-candidate, not-estimable and invalid-result rows.

Provider calls are idempotency-keyed. Before dispatch, reserve the worst-case approved budget for input, system/developer text, tool output, images, output and retries; unknown price means no dispatch. Persist reservation, dispatch intent, provider response/request ids when supplied, usage/charges, timeout/unknown state and release/reconciliation. A late charge cannot alter baseline or silently overrun budget. No retry can multiply the cap.

## 5. Research and simulation requirements

Before any shadow cohort, independently test:

1. typed candidate select/reject/abstain and tool capability; malformed JSON, wrong scope, prompt injection in data, hallucinated price/fill, invented reference, timeout, cancellation, duplicate, process restart and cost-reservation denial;
2. feature and event prefixes, data revisions and receipt-time cutoffs against replay; stale/reordered/incomplete feed, socket reconnect and provider outage;
3. same-symbol and same-slot conflicts, all-method disagreement, missing/no-candidate observations, excluded universe denominator and stable decision calendar;
4. deterministic baseline unaffected by the sidecar, independent decision identity and fail-closed promotion; no future quote, candle, news publication or outcome reaches a request;
5. fixed management versus each separately versioned experimental TP/SL/BE/trailing/partial policy under partial fill, stop trigger during cancel/replace, reduce-only rejection, gap, fees, funding, latency, liquidation, disconnect and restart;
6. resource and operational bounds on the target host: import, cold/warm latency, CPU/RSS, queue, bytes/WAL/archive, request weights and complete backlog drain, with capture retained when AI is off.

No backtest metric alone accepts the change. Forecast results use an untouched chronological holdout and matching horizon versus last-value, drift and volatility baselines with MAE and calibration. Trade-policy effects are evaluated only if real decision, fill, fee, funding, slippage and cost provenance supports them. No economic claim before at least eight weeks of forward shadow and 200 matured opportunities, with regime/dependence-aware paired full calendars and explicit missed-opportunity analysis.

## 6. Migration, rollback, and release gates

No in-place rewrite of frozen records or selector identity. Add versioned, additive event/artifact schemas and a read-only migration. Old records must remain replayable and verifiable; migration output must record parent refs, tool/code version, content hash and any dropped/unknown fields. Test old/new dual-read equivalence in scratch, crash/restart at every journal boundary, idempotent re-run and rollback to the existing frozen baseline. Default feature flag is off. A kill switch disables sidecar dispatch without interrupting capture, deterministic candidates, durable journaling or protection.

Approval sequence: independent contract review; dependency/license/supply-chain review; deterministic fixture and fault-suite acceptance; shadow-only resource and source qualification; untouched prospective shadow; explicit owner decision. No report or tool in TQ-01 self-accepts this draft. Until these gates complete, retain the current freezes and all capital-disabled/source-health/A2 protections.

## 7. Questions for independent reviewers

- Is a model-assisted selector useful after the independent deterministic candidate set is complete, and what exact select/reject/abstain task should be measured?
- Does a direct structured provider call provide enough capability with fewer dependencies than a tool framework?
- Is any vision input needed after timestamped numeric evidence and source receipts are available?
- Which one management-policy variant, if any, deserves a separate prospective cohort?
- Which forecast horizon and data vintage match the actual frozen M15 opportunity without lookahead or spot/perpetual mismatch?
- Can the target laptop sustain capture, journaling and read-only UI while also running optional research inference, with measured headroom?
- Which dependency and model artifacts may be distributed with a desktop product after full license/SBOM review?
