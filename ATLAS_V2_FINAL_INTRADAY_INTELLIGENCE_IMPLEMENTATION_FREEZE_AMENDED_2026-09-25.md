# ATLAS V2 — FINAL INTRADAY INTELLIGENCE, MULTI-VENUE FUTURES & IMPLEMENTATION FREEZE

**Original freeze date:** 24 September 2026  
**Amended:** 25 September 2026  
**Status:** **V2 DESIGN FREEZE / IMPLEMENTATION AUTHORITY — INTELLIGENCE AMENDMENT INCORPORATED**  
**Repository:** `KasunKarandagolla/Atlas`  
**V1 baseline checkpoint:** `146bae0a2f10e2f794cbed3441123071c55a2baf` (`impl/session-009-v1-phase0-6-stabilization`)  
**Capital status at freeze:** V1/V2 live capital remains **UNVERIFIED / TEST GATE**. This document authorizes no live trading by itself.

---

# 0. Purpose, authority and interpretation

This document freezes the ground-level design for ATLAS V2.

It exists to convert the product vision and Astra consultation into an implementation specification that can be followed branch by branch without guessing architecture, data ownership, strategy meaning, model authority, risk authority, storage semantics or release gates.

This document is intentionally detailed. It is not a guarantee that any strategy is profitable, any model is accurate enough for live use, or any exchange capability is safe. Those remain empirical questions.

## 0.1 Source basis

This V2 freeze was prepared from:

1. the authoritative **ATLAS Final Implementation Clarification and V1 Freeze**;
2. the actual public GitHub repository and Session-009 checkpoint;
3. `ATLAS_V2_ARCHITECTURE_REVIEW.md`;
4. `ATLAS_V2_IMPLEMENTATION_SPECIFICATION.md`;
5. the earlier leveraged-retail and adaptive-alpha proposals;
6. owner decisions made in the V2 design discussion;
7. the final review amendments recorded after the Astra consultation;
8. `ATLAS_V2_INTELLIGENCE_ADVERSARIAL_REVIEW_AND_ADDENDUM.md` (25 September 2026) and the owner-approved reconciliation against the actual Session-017 repository state.

## 0.2 Authority relationship with V1

The V1 freeze remains authoritative for all existing V1 behavior and all safety invariants unless this V2 document explicitly introduces a **versioned V2 contract**.

V2 must not silently mutate V1 serialization, V1 hashes, `CRYPTO_TREND_24H_V1`, V1 risk evidence, V1 journal semantics, V1 UNKNOWN handling or the existing V1 TradePlan.

When both documents apply:

- V1 governs the existing Phase 0–6 implementation and its capital-control invariants.
- This document governs additive V2 contracts, V2 intelligence, V2 desktop, broader-universe research, Model Arena extensions, Binance compatibility and future V2 policy evaluation.
- If a proposed implementation changes a V1 frozen contract, it must be introduced as a new version with explicit migration, replay comparison and approval.

## 0.3 Meaning of “freeze”

“Freeze” means implementation decisions are no longer casually changed during coding.

A change to a frozen V2 contract requires:

- a written ADR/change proposal;
- the reason;
- affected artifacts/contracts;
- backward-compatibility impact;
- causal/replay impact;
- tests;
- migration/rollback plan;
- new version identifier.

Research parameters explicitly marked **engineering default**, **research hypothesis**, **training-selected** or **TEST GATE** are not frozen economic truths.

---

# 1. Current V1 state at the V2 handoff

As of this freeze, the latest repository branch remains:

`impl/session-009-v1-phase0-6-stabilization`

with SHA:

`146bae0a2f10e2f794cbed3441123071c55a2baf`

The GitHub default branch is still the older:

`impl/session-001-foundation`

and must not be assumed authoritative.

The Session-009 handoff records:

| Area | Status at V2 start |
|---|---|
| Phase 0 contracts/capability foundation | IMPLEMENTED / TESTED_OFFLINE |
| Phase 1 safe runtime skeleton | IMPLEMENTED / TESTED_OFFLINE |
| Phase 2 recovery/protection mechanics | IMPLEMENTED / TESTED_OFFLINE; external capabilities remain gates |
| Phase 3 causal data foundation | IMPLEMENTED / TESTED_OFFLINE |
| Phase 4 baseline science/risk | IMPLEMENTED / TESTED_OFFLINE |
| Phase 5 scanner/alert | IMPLEMENTED / TESTED_OFFLINE / FROZEN |
| Phase 6 assisted-control shell | IMPLEMENTED / TESTED_OFFLINE |
| full recorded suite | 413 passed, 1 skipped |
| authenticated Bybit protection/execution | UNVERIFIED / TEST GATE |
| all frozen Bybit capability states | UNVERIFIED |
| live RiskPolicy approval | NOT GRANTED by this document |
| profitability | NOT ESTIMABLE |
| assisted live execution | disabled |

This V2 design assumes V1 remains the safety foundation and does not recreate it.

## 1.1 Current V2 implementation ground reality — 25 September 2026

The V1 stabilized baseline remains Session-009, but the **current V2 implementation checkpoint** is:

```text
branch: impl/session-017-v2-risk-action-replay
SHA: f0833199d444f23b90336a3fdeff389e41ff1a42
```

This SHA is the concrete starting point for all further V2 implementation unless later repository evidence records a reviewed successor.

At this checkpoint:

- **IMPLEMENTED / TESTED:** S1 trend/pullback, S2 compression breakout, causal feature core including candle geometry and causal swing/SR/SMC/Fibonacci/Elliott-like morphology, CandidateSet persistence, deterministic S1/S2 selection, V2 hard-risk sizing, frozen action identity, fees/funding-aware retrospective execution replay, and actual-versus-simulated rolling-risk segregation.
- Session-017 remediation preserves S1, S2 and selection policy hashes; fixes IOC/deadline resolution ambiguity, short notional sizing, and actual/simulated rolling-outcome provenance.
- `ReplayPathV2` / `PolicyPayoffV2` are **retrospective research evidence only**. They are not decision-time pretrade scenarios.
- **TEST GATE:** chronological M0 action-value fitting, decision-time `PretradeScenarioArtifactV2`, final economic/ES evaluator, `EvaluationArtifactV2`, `TradePlanEnvelopeV2`, mature outcome learning, desktop completion and capital authority.
- **NOT ESTIMABLE:** S1/S2 economic value and V2 profitability.
- **BLOCKED BY ENVIRONMENT:** the declared 72-hour public soak.
- **UNVERIFIED / TEST GATE:** exact remaining model/environment and venue-capability qualifications.
- Capital remains disabled. No intelligence amendment changes that status.

Reported Session-017 remediation verification at this checkpoint: 35 focused tests, 138 V2 tests and 11 V1 invariant-smoke tests passed. These are repository handoff facts; later sessions must report only tests they actually rerun.

---

# 2. Owner-frozen product vision

ATLAS V2 is a **retail-accessible, professional-style, cross-platform intraday crypto trading intelligence and research system**.

## 2.1 Initial trading product

Initial V2 market:

- crypto;
- leveraged futures/perpetuals;
- linear USDT-margined contracts initially;
- intraday/day-trading;
- typical holding time from minutes to hours;
- normally within approximately one day;
- one-way exposure initially;
- isolated-margin compatible profiles initially.

Spot is a later versioned expansion.

The reason for deferring spot is product positioning and capital efficiency, not a claim that spot cannot be profitable.

## 2.2 Universe

V2 must think beyond BTC and ETH.

It must observe and rank the broader **liquid crypto perpetual market**, but it must never equate broad observation with trading permission.

Instrument states are separate:

```text
OBSERVED
  -> DATA_ELIGIBLE
  -> SCANNER_ELIGIBLE
  -> STRATEGY_ELIGIBLE(policy)
  -> DEEP_ANALYSIS_ELIGIBLE
  -> CAPITAL_ELIGIBLE(venue, account, policy)
```

BTC and ETH remain permanently monitored benchmark/regime instruments.

## 2.3 Venues

Initial venue targets:

- Bybit;
- Binance.

V2 is venue-flexible but not automatically cross-venue.

Initial capital rule:

> One credential-bearing trading account/venue writer at a time.

Both venues may be researched simultaneously. Both may eventually become independently capital-qualified. Simultaneous capital use across venues requires a later global capital/reservation specification.

Cross-venue smart routing and arbitrage are **not** part of first-release V2.

## 2.4 Desktop

ATLAS V2 is a desktop-first application.

Targets:

- Windows;
- Linux;
- Linux Lite-class low-resource systems.

The desktop UI is not the trading engine.

A GUI crash, UI restart or chart failure must not remove venue-native protection, mutate live risk or become execution truth.

## 2.5 Intelligence

V2 includes:

- advanced quantitative/statistical models;
- standard technical indicators;
- candlestick geometry;
- support/resistance;
- SMC/ICT features;
- Fibonacci relationships;
- Elliott-like swing morphology;
- multi-timeframe reasoning;
- market microstructure;
- funding/OI/liquidations;
- market-wide relative strength;
- scheduled/unscheduled event context;
- modular pretrained ML;
- durable opportunity memory;
- permanent evidence/outcome memory.

No methodology receives authority merely because it is famous.

The amended intelligence objective is:

> **Do not search for the rule with the best historical return. Evaluate the exact currently proposed action and require supported positive after-cost incremental value after selection effects, estimation uncertainty, execution uncertainty, market-state support, tail risk and portfolio constraints. When that cannot be established, abstention is correct intelligence.**

V2 therefore separates:

1. **market-state evidence** — what was actually known at the cutoff;
2. **strategy hypotheses** — S1–S8 propose exact candidate actions;
3. **hard-risk/action identity** — deterministic sizing freezes the action before economic admission;
4. **action-value intelligence** — M0, challengers, analogues and pretrade scenarios estimate the value/support of that exact action;
5. **skeptic/admission logic** — uncertainty, OOD/support, stress and portfolio gates produce `TRADE`, `NO_TRADE` or `NOT_ESTIMABLE`;
6. **scientist/research logic** — whole-calendar outcomes drive ablation and bounded new-hypothesis discovery with zero execution authority.

---

# 3. Non-negotiable safety and scientific invariants

These are inherited from V1 and remain mandatory.

## 3.1 Capital-control ownership

- Venue state is external reality.
- NautilusTrader is the normal operational order/fill/position engine.
- ATLAS owns durable intent, approval, reservation, risk, recovery and audit.
- Do not create a second OMS.
- Persist required intent, command and reservation before external side effects.
- Lost acknowledgement or network timeout means `UNKNOWN`.
- Never create a new opening-order identity because a submit timed out.
- A negative lookup does not prove an order never existed.
- Operational uncertainty blocks new risk.
- Existing protection must not depend on models, desktop, news or research workers.
- Research agents and LLMs never control credentials, orders, leverage or risk limits.

## 3.2 Risk

- Risk determines quantity.
- Risk determines feasible margin/leverage.
- Leverage never creates edge.
- Model confidence cannot relax hard risk.
- Missing risk evidence means no new risk.
- Existing protective orders remain even if new-entry gates fail.

## 3.3 Causality

Where applicable preserve:

- source event time;
- source publication time;
- actual receipt time;
- available time;
- processing start/end;
- recorded time;
- revision/provenance;
- replay availability class.

Historical downloads never fabricate historical receipt timestamps.

Future/revised information cannot rewrite old decisions.

## 3.4 Research evidence

Evaluate complete policy outcomes:

- TRADE;
- NO_TRADE;
- NOT_ESTIMABLE;
- unselected candidate;
- rejected candidate;
- no fill;
- partial fill;
- stale data;
- model timeout;
- expired watch;
- approval delay;
- order delay;
- fees;
- funding;
- slippage;
- execution failures;
- selection effects.

Failed and inconclusive experiments are retained.

---

# 4. V2 process architecture

V2 adds intelligence around the existing capital core.

```text
                    BYBIT / BINANCE / OFFICIAL EVENTS
                               |
                               v
                 causal ingestion + universe
                               |
                               v
                causal market-state evidence
                               |
                               v
                    scanner / watches
                               |
                               v
                 S1-S8 policy candidates
                               |
                               v
                        CandidateSet
                               |
                               v
               deterministic selection
                               |
                               v
                 deterministic hard risk
                               |
                               v
                   frozen exact action
                               |
                               v
        M0 / challengers / analogue support
                   + pretrade scenarios
                               |
                               v
          cost + uncertainty + OOD/support
                               |
                               v
              stress + portfolio ES gate
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
        NO_TRADE / NOT_ESTIMABLE      TradePlan V2 (shadow/
                                      authority still separate)
                                             |
                                             v
                                      live authority gate
                                             |
                                             v
                                       NautilusTrader
                                             |
                                             v
                                      qualified venue
```

## 4.1 `atlas-crypto-live`

Credential-bearing process.

Responsibilities:

- existing Nautilus live boundary;
- approvals;
- revalidation;
- risk reservations;
- durable command;
- protection;
- reconciliation;
- recovery;
- closing/emergency controls.

Authority:

- sole live control SQLite writer;
- sole holder of exchange trading credentials for its account.

It must not become the home for heavy research, news classification or foundation-model inference.

## 4.2 `atlas-ops`

No exchange mutation authority.

Responsibilities:

- public collection;
- dynamic universe;
- scanner;
- event scheduler;
- opportunity watches;
- news/events;
- model requests;
- desktop projections;
- evidence persistence.

It owns a separate `ops.sqlite`.

## 4.3 `atlas-worker`

Disposable process/environment.

Responsibilities:

- pretrained inference;
- expensive feature batches;
- fitting;
- research experiments;
- shadow evaluation.

Restrictions:

- no trading credentials;
- no live control DB writes;
- no writable RiskPolicy;
- no order APIs.

## 4.4 `atlas-desktop`

Responsibilities:

- visualization;
- scanner;
- charts;
- watches;
- evidence;
- risk/health display;
- later immutable-plan approval requests.

Restrictions:

- never edits live tables;
- never creates orders directly;
- never changes risk limits directly;
- stale UI data must be visibly stale.

---

# 5. Repository layout

Preserve existing V1 modules.

Do not mass-rename or “clean up” Phase 0–6 merely to fit V2.

Add:

```text
src/atlas/v2/
    contracts.py
    instruments.py

    data/
        bybit.py
        binance.py
        bars.py
        universe.py
        quality.py
        historical.py

    features/
        technical.py
        candles.py
        structure.py
        flow.py
        derivatives.py
        regime.py
        cross_section.py

    strategies/
        common.py
        s1_trend.py
        s2_breakout.py
        s3_mean_reversion.py
        s4_orderflow.py
        s5_crowding.py
        s6_cross_section.py
        s7_events.py
        s8_pairs.py

    science/
        labels.py
        fit.py
        meta.py
        scenario.py
        evaluate.py
        selection.py
        uncertainty.py

    memory/
        schema.py
        repository.py
        scheduler.py
        outcomes.py

    models/
        protocol.py
        registry.py
        local.py
        remote.py
        tirex.py
        kronos.py
        chronos.py

    news/
        sources.py
        normalize.py
        extract.py
        policy.py

    runtime/
        bridge.py
        capability.py
        ipc.py

src/atlas/desktop/
    app.py
    models/
    views/
    widgets/
    ipc_client.py

configs/v2/
docs/v2/
tests/v2/
```

Shared pure functions may be moved only after golden V1 replay proves unchanged results.

---

# 6. Core V2 contracts

All V2 boundary artifacts are immutable, versioned and content-addressed where appropriate.

## 6.1 Common conventions

- timestamps: integer UTC nanoseconds;
- money/price/quantity/rates used for authority: canonical decimal strings / `Decimal`;
- statistical arrays: finite float64 with explicit units;
- NaN is not authoritative; use nullable value plus missing reason;
- hashes: SHA-256 canonical JSON or exact bytes;
- unknown fields at a strict versioned wire boundary fail validation unless schema explicitly permits extensions;
- every artifact records producer/version;
- every decision artifact records input references.

Common envelope:

```text
schema_version
artifact_id
created_at_ns
available_at_ns
producer_version
input_refs[]
content_hash
```

Causal invariant:

```text
max(input.available_at_ns)
    <= information_cutoff_ns
    <= computation_started_ns
    <= computation_finished_ns
    <= result.available_at_ns
    <= consumer_decision_deadline_ns
```

Any exception must be an explicitly different replay/reconstruction class.

## 6.2 Instrument identity

```text
InstrumentKeyV2:
  venue: BYBIT | BINANCE
  environment: MAINNET | TESTNET
  product: LINEAR_PERPETUAL
  native_symbol: string
  base_asset_id: string
  quote_asset: USDT
  settlement_asset: USDT
  contract_revision: string
```

Never join assets by ticker text alone.

Redenomination, multipliers, tick/lot changes or contract replacement create a new revision.

## 6.3 Product contract

```text
ProductContractV2:
  key
  effective_at_ns
  observed_at_ns
  available_at_ns
  base_units_per_contract
  tick_size
  qty_step
  min_qty
  min_notional?
  max_qty?
  trading_status
  listing_at_ns?
  delisting_at_ns?
  fee_schedule_ref?
  margin_tiers_ref?
  funding_schedule_ref?
  metadata_ref
```

## 6.4 Universe contract

```text
UniverseContractV2:
  envelope
  universe_version
  decision_slot_ns
  selection_policy_hash
  entries[]
```

Each entry includes:

```text
key
product_ref
observed
data_eligible
scanner_eligible
deep_analysis_eligible
capital_eligible
strategy_eligibility: map<policy_id, status/reason>
reasons[]
cheap_feature_ref?
warmup_ref?
```

This explicit strategy-level eligibility resolves the difference between “30 days of observed history” and a policy that may require much longer fitting history.

## 6.5 Feature artifact

```text
FeatureArtifactV2:
  envelope
  key
  feature_set_version
  information_cutoff_ns
  confirmed_at_ns
  values:
      feature_id:
          value?
          unit
          missing_reason?
  state_ref?
  source_health_ref
  replay_view: ACTUAL_SYSTEM | RECONSTRUCTED_MARKET
```

## 6.6 Policy specification

```text
PolicySpecV2:
  policy_id
  version
  policy_hash
  strategy_family
  capital_status
  decision_event
  required_features[]
  optional_features[]
  timeframe_rules
  setup_parameters
  direction_rule
  entry_rule
  collar_rule
  stop_rule
  trigger_basis
  management_rule
  time_exit_rule
  max_hold_ns
  expiry_rule
  model_requirements[]
  fallback_policy_id?
```

## 6.7 Candidate action

```text
CandidateActionV2:
  envelope
  candidate_id
  key
  account_scope?
  policy_hash
  snapshot_hash
  side: LONG | SHORT
  decision_at_ns
  deadline_ns
  horizon_end_ns
  entry_reference
  entry_collar
  stop_price
  quantity?
  state_version
  cost_model_ref
```

Quantity is absent until deterministic risk sizing.

## 6.8 Strategy forecast

```text
StrategyForecastV2:
  envelope
  forecast_id
  candidate_id
  key
  strategy_id
  strategy_version
  policy_hash
  horizon_ns
  expires_at_ns
  target_definition
  distribution_ref?
  mean?
  q05?
  q50?
  q95?
  p_net_positive?
  cost_ref?
  mae_ref?
  mfe_ref?
  calibration_ref?
  capacity_ref?
  regime_support_ref
  contradictions[]
  status
```

A forecast is evidence, not an order.

## 6.9 Candidate-set / selection artifact — REQUIRED V2 AMENDMENT

Every decision event must persist **all generated policy candidates before one is selected**.

```text
CandidateSetV2:
  envelope
  decision_event_id
  universe_ref
  selection_policy_hash
  candidates[]:
      candidate_id
      policy_id
      key
      side
      selection_feature_refs[]
      eligibility_status
      rank?
      rank_reason?
      rejection_reason?
  selected_candidate_id?
  tie_break_rule
  selection_status:
      SELECTED | NO_CANDIDATE | NOT_ESTIMABLE
```

This artifact is mandatory for:

- scanner-bias measurement;
- multiplicity control;
- strategy competition;
- exact reproduction;
- counterfactual outcome maturation.

No candidate may disappear merely because another one was selected.

## 6.10 Evaluation artifact

```text
EvaluationArtifactV2:
  envelope
  action_hash
  quantity
  risk_policy_hash
  account_snapshot_ref
  universe_ref
  candidate_set_ref
  selection_policy_hash
  state_ref
  meta_version
  pretrade_scenario_ref
  stress_ref
  existing_portfolio_ref
  expected_net_value?
  expected_pnl_lcb?
  lcb_method_ref
  estimation_uncertainty_ref?
  execution_uncertainty_ref?
  numerical_error_ref?
  support_ref
  calibration_ref?
  ood_ref
  outcome_distribution_ref?
  es_before?
  es_after?
  decision: CANDIDATE | NO_TRADE | NOT_ESTIMABLE
  reasons[]
  expires_at_ns
```

`pretrade_scenario_ref` must refer to a decision-time scenario artifact, never a retrospective `ReplayPathV2`. Support/OOD and uncertainty are first-class admission evidence; more scenario draws cannot substitute for insufficient historical support.

## 6.11 TradePlan V2

V1 TradePlan serialization remains unchanged.

V2 uses:

```text
TradePlanEnvelopeV2:
  envelope
  plan_id
  plan_version
  execution_contract_version
  key
  product_ref
  account_scope
  policy_hash
  action_hash
  evaluation_ref
  risk_policy_hash
  capability_manifest_hash
  reservation_snapshot_version
  side
  qty_limit
  entry_policy
  collar
  stop
  stop_trigger_basis
  management_policy
  horizon_end_ns
  normal_risk
  stress_risk
  margin
  leverage_bound
  reference_price
  expires_at_ns
```

Unknown plan versions fail closed.

Changing venue, quantity, stop, policy, risk version or account invalidates approval.

---

# 7. Market-data design

## 7.1 Primary source rule

Prefer verified Nautilus adapter surfaces.

A direct credential-free venue HTTP/WebSocket adapter is permitted only for public fields absent or unsuitable in the pinned Nautilus adapter.

A narrow credential-bearing protection port explicitly authorized by the V1 freeze remains allowed in `atlas-crypto-live`. It must not expand into a general second execution SDK.

## 7.2 Bybit targets

Research/public ingestion may include:

- V5 instrument info;
- public trades;
- final klines;
- BBO;
- order-book depth;
- ticker;
- mark/index;
- funding history/current;
- OI;
- liquidations;
- announcements/status.

## 7.3 Binance USD-M targets

Research/public ingestion may include:

- `/fapi/v1/exchangeInfo`;
- klines;
- aggTrade/trade semantics as qualified;
- book ticker;
- depth snapshot + diff stream;
- mark/premium index;
- funding history/current;
- OI current/history;
- force-order/liquidation stream;
- official status/announcements where available.

## 7.4 Raw observation record

Every record stores:

```text
record_id
instrument_revision
source_id
event_type
sequence?
event_at_ns
published_at_ns?
received_at_ns
ingested_at_ns
available_at_ns
raw_payload_hash
translation_version
revision_of?
quality_flags[]
availability_class
```

## 7.5 Historical data

Preferred:

- official Binance public data archive;
- official Bybit historical download sources where verified;
- bounded venue REST gap repair;
- ATLAS forward collection.

Historical import rule:

> The actual import/receipt time is the time ATLAS downloaded the file. A reconstructed historical availability is a separate field and never becomes ACTUAL_SYSTEM evidence.

## 7.6 Bars

Use UTC half-open intervals `[open, close)`.

Initial S1/S2 policies use venue-final bars.

A bar is usable only after final confirmation/receipt.

At a 15M trigger:

- 15M uses the just-confirmed 15M bar;
- 1H uses latest confirmed 1H bar;
- 4H uses latest confirmed 4H bar;
- forming 1H/4H data cannot masquerade as closed.

Corrections append evidence revisions.

---

# 8. Universe, scanner and resource tiers

## 8.1 Initial shadow screening profile

Engineering starting values, not proven economic thresholds:

- USDT linear perpetual;
- trading status active;
- at least 30 observed days for broad scanner eligibility;
- required bars present;
- trailing 24h quote turnover initially >= USD 10m;
- spread initially <= 10 bps;
- adequate source health.

Strategy-specific requirements may require much more history.

## 8.2 Compute tiers

```text
Tier 0: all observed contracts
  metadata/listing state

Tier 1: scanner eligible
  price, returns, volume, spread, funding, OI, cheap volatility

Tier 2: top ~20
  richer bars/trades/OI/derivatives

Tier 3: top ~5 + open positions + active watches
  L2/order-flow + expensive models

Tier 4: capital eligible
  explicit venue/instrument/policy/account qualification
```

Exact K values are engineering defaults measured through scanner blind-spot evidence.

## 8.3 Exploration

Use deterministic known-probability shadow exploration outside the primary shortlist.

Record inclusion probability.

Exploration never consumes capital.

## 8.4 Warmup

A newly selected L2 instrument starts cold.

Do not reconstruct historical order-book state from bars.

Flow-dependent policies remain `NOT_ESTIMABLE_WARMUP` until valid warmup is complete.

## 8.5 Irrecoverable forward evidence — collect now

Before granting new strategy influence, audit whether current forward collection preserves:

- L2/BBO snapshots/deltas, declared depth/cadence, venue sequence/update semantics, resets/gaps, exchange and actual receipt timestamps;
- trades/aggressor IDs/side semantics and receipt time;
- OI/funding/basis raw units, current/predicted/settled distinction, mark/index/last, next-funding time and revisions;
- raw liquidation messages with venue/product/side convention, schema version and known coverage limitations;
- official news/event raw content/hash, publication claim, actual receipt, extraction completion and revisions;
- point-in-time instrument/universe/filter/fee revisions;
- collector/source health, clock drift, disconnect/offline periods and inclusion policy.

A versioned **evidence capability matrix** records feed semantics, resolution, coverage, units, gaps and permitted uses. Missing high-resolution evidence blocks dependent inference; it must not be fabricated from candles.

Broad low-cost coverage plus deterministic/sampled control coverage is preferred over retaining L2 only for eventual winners.

---

# 9. Feature system

All features are versioned, causal and individually testable.

## 9.1 Standard technical features

Initial implementation may use TA-Lib batch functions after exact version pinning and parity tests.

Core set:

### Trend
- EMA20/EMA50;
- regression slope;
- ADX14;
- Donchian/range breakout.

### Momentum
- RSI14;
- MACD;
- ROC;
- Stochastic only as a challenger if incremental.

### Volatility
- ATR14;
- realized variance;
- EWMA variance;
- Bollinger width;
- range statistics.

### Location
- UTC-day trade VWAP;
- anchored VWAP variants;
- prior day/session high/low;
- volume profile when defensible.

Avoid feature explosion.

## 9.2 Candlestick features

Keep continuous geometry:

```text
signed_body / ATR
upper_wick / ATR
lower_wick / ATR
range / ATR
close_position
gap
volume_z
```

Named TA-Lib patterns may also be recorded but do not directly produce orders.

## 9.3 SMC / ICT

SMC/ICT is **included**.

Initial causal features:

- confirmed swing high/low;
- BOS;
- CHoCH;
- FVG;
- liquidity pools;
- liquidity sweep;
- order-block candidate;
- premium/discount;
- age;
- touches/mitigation;
- normalized distance.

### Swing confirmation

Initial research profile:

- 15M pivots;
- two bars on each side;
- pivot at `i` becomes available only after `i+2` closes;
- record pivot time and confirmation time;
- no retroactive decision availability.

### BOS

Closed price breaks an already confirmed swing by initial engineering threshold `0.1 ATR`.

### CHoCH

First qualifying break against confirmed directional structure.

### FVG

At third completed bar:

- bullish: `low[i] > high[i-2]`;
- bearish: symmetric.

### Liquidity sweep

Price pierces an already known swing by initial `0.1 ATR` and closes back across it.

This is a price morphology pattern, not proof of stop hunting.

### Order block

Initial research definition:

last opposite candle within the prior five completed candles before qualifying BOS, recorded only at BOS confirmation.

## 9.4 Support/resistance

Build an incremental causal state machine.

Inputs:

- confirmed pivots;
- ATR/volatility;
- touch/break events;
- optional volume-at-price.

Initial merge distance:

`0.25 ATR` at level creation.

Zone versions are immutable.

Do not full-sample recluster and relabel old history.

## 9.5 Fibonacci

Compute relationships from confirmed causal legs:

- 0.382;
- 0.5;
- 0.618;
- optional 0.786/extension ratios as research variants.

They are features, not automatic trade levels.

## 9.6 Elliott-like morphology

Elliott is **not removed**.

Do not implement subjective manual “wave count = buy.”

Initial morphology stores:

- last confirmed leg amplitudes;
- durations;
- retracement ratios;
- extension ratios;
- overlap;
- acceleration;
- volume/flow behavior;
- multi-scale leg relationships.

These features are evaluated through ablation.

## 9.7 Wyckoff/time-of-day and feature-family discipline — amended

Do not create subjective Wyckoff or ICT mythology classifiers.

Represent measurable challengers only:

- spring/upthrust as range-boundary false break + return;
- effort-versus-result as volume/aggressive-flow versus realized price response;
- time-of-day, weekday/weekend, market-session overlap, time-to-funding and macro-event proximity as causal context;
- fixed ICT/killzone windows only as explicit challengers against ordinary calendar features.

All feature families are correlated evidence, not independent votes. Compare the same whole policy with/without each family, including its data, latency and compute burden.

---

# 10. Mathematical modelling layer

Advanced mathematics is a first-class V2 requirement.

Complexity must beat simpler baselines.

## 10.1 Trend/state

Primary:

- multi-scale returns;
- robust regression slope;
- forward Kalman/state-space filter.

Never use a full-sample smoother as if it were online.

## 10.2 Volatility

Primary:

- realized variance;
- EWMA;
- intraday HAR-style regression.

Challengers:

- GARCH;
- EGARCH;
- GJR-GARCH via `arch`.

## 10.3 Regime

Do not trust one magical regime label.

Represent separate axes:

```text
trend_state
volatility_state
liquidity_state
crowding_state
event_state
unknown/OOD state
```

Training quantiles are fitted on training data only.

Offline `ruptures` results cannot backdate future-known change points.

Online drift methods such as River are challengers.

## 10.4 Mean reversion

Use:

- VWAP residual;
- z-score;
- AR(1);
- half-life;
- stationarity diagnostics.

OU interpretations remain hypotheses unless supported.

## 10.5 Relative value

Use:

- Engle-Granger;
- Johansen/VECM where appropriate;
- Kalman/time-varying hedge ratio;
- residual half-life;
- both-leg execution costs.

## 10.6 Tail/uncertainty

Primary:

- empirical joint blocks;
- synchronized bootstrap;
- deterministic stress suite.

Challengers:

- Student-t;
- EVT/GPD;
- conformal methods where assumptions are supportable.

Separate:

- outcome dispersion / market randomness;
- uncertainty in expected value / estimation error;
- execution-model uncertainty;
- numerical error.

More simulated paths reduce numerical error only; they do not create more independent market evidence. Fill/no-fill/partial-fill behavior and subsequent returns must remain jointly modeled where dependence is material.

---

# 11. Strategy architecture

Strategies are policies, not votes.

All sleeves must produce candidate hypotheses/evidence.

No sleeve may:

- reserve capital;
- submit an order;
- modify a stop;
- increase leverage;
- cancel protection.

Capital status at initial V2 build:

| Sleeve | Initial status |
|---|---|
| S1 Trend continuation/pullback | first complete shadow policy |
| S2 Compression breakout | second complete shadow policy |
| S3 VWAP/statistical mean reversion | research/shadow |
| S4 Order-flow/microstructure | execution-context first; standalone shadow |
| S5 Funding/OI/liquidation crowding | context first; directional shadow |
| S6 Cross-sectional relative strength | research/shadow then candidate |
| S7 Event/news reaction | veto/context first; directional shadow |
| S8 Relative value/pairs | research basket only |


# 12. S1 — Multi-timeframe trend continuation/pullback

## Data

Required:

- final 4H, 1H, 15M OHLCV;
- ATR/realized volatility;
- executable BBO;
- mark/index;
- event gate.

Optional enriched variant:

- trade VWAP;
- funding;
- OI;
- 5M/1M/L2 order flow.

Optional inputs define a separately named variant and cannot disappear silently.

## Initial shadow policy

Long:

1. 4H close > EMA50.
2. 4H EMA20 > EMA50.
3. 1H, within last three closed bars, low touched/breached EMA20.
4. latest 1H closes back above EMA20 and EMA50.
5. create watch.
6. trigger only when a subsequent confirmed 15M close breaks prior 15M high.
7. short is symmetric.

Watch:

- expires after four 15M bars;
- invalidates on contrary 4H regime.

Stop:

- setup-window low minus `0.25 ATR15M` for long;
- symmetric short;
- reject distance < `0.5 ATR` or > `3 ATR`.

Entry:

- IOC;
- initial adverse collar 5 bps from executable BBO;
- no same-epoch repricing after no-fill.

Management:

- fixed stop;
- initial four-hour time exit;
- no discretionary trailing;
- no partial TP;
- no pyramiding.

## Kill/ablation tests

Compare:

- simple 4H trend baseline;
- S1 without pullback;
- S1 without SMC/candle/Fibonacci context;
- S1 with/without funding/OI;
- S1 with/without flow;
- parameter neighborhood.

Retire additions that do not improve complete-policy value.

---

# 13. S2 — Compression breakout

## Data

- confirmed 15M bars;
- 1H/4H context;
- ATR;
- Bollinger width;
- volume;
- spread;
- optional L2/OFI.

## Initial shadow policy

Compression:

- Bollinger width 20 below trailing 30-day 20th percentile;
- ATR14 below trailing 30-day median.

Range:

- high/low of preceding 20 completed 15M bars;
- trigger bar excluded.

Trigger:

- close beyond range by `0.1 ATR`;
- trigger volume above preceding 20-bar median.

Stop:

- opposite range edge;
- reject width < `0.5 ATR` or > `3 ATR`.

Management:

- if either next two 15M bars closes back inside frozen range, execute predeclared failed-break exit;
- otherwise fixed stop;
- two-hour time exit.

Retest entry is a different policy version.

## Kill tests

Compare:

- raw range breakout;
- compression filter only;
- volume filter only;
- compression + volume;
- delayed/retest versions separately;
- realistic fill/arrival assumptions.

---

# 14. S3 — VWAP/statistical mean reversion

## Data

- trades/BBO;
- UTC-day VWAP;
- 1M/15M bars;
- volatility;
- 4H trend;
- event flags.

## Initial research policy

1. residual = log price - log VWAP;
2. standardize over preceding 120 completed 1M observations;
3. fit AR(1) on trailing seven days;
4. require `0 < phi < 1`;
5. half-life = `-ln(2)/ln(phi)`;
6. require half-life 5–30 minutes;
7. reject strong trend (initial ADX14 > 25);
8. watch deviation > ±2 sigma;
9. enter only after completed 1M bar moves back toward VWAP.

Target:

- entry-time VWAP frozen.

Stop:

- one residual sigma adverse in log space, rounded conservatively;
- reject invalid/too-small stop.

Time exit:

- <= 60 minutes.

No moving target to rescue a losing trade.

---

# 15. S4 — Order-flow/microstructure

## Data

Requires sequence-valid:

- L2;
- BBO;
- trades/aggressor;
- spread;
- depth;
- liquidation context;
- source latency/sequence health.

Use Nautilus order-book machinery where possible.

Features:

- microprice / weighted-mid proxy;
- OFI;
- depth imbalance over declared bands;
- signed aggressive trade imbalance;
- 1s/5s/30s windows where feed/latency evidence supports them;
- spread state;
- depth change and displayed replenishment/persistence;
- signed-flow versus realized-price-response residual;
- short-horizon impact/adverse markout;
- explicit feed age, sequence health and alignment uncertainty.

`Absorption` is a probabilistic observable hypothesis: unusually large signed aggressive flow with unexpectedly small same-direction price response, supported by persistent/replenishing displayed liquidity and optionally later flow/microprice reversal. It is not proof of a hidden participant or institutional intent. Prefer a training-fitted expected-response residual over unstable volume/divided-by-near-zero price-change ratios.

Initial role:

> execution-cost and entry-quality context for S1/S2.

Standalone strategy remains shadow-only initially because human delay and uncertain queue priority can invalidate seconds-scale edge.

Book sequence gap => flow-dependent policies `NOT_ESTIMABLE` until snapshot recovery and warmup.

---

# 16. S5 — Funding/OI/liquidation crowding

## Data

- funding rate/schedule;
- predicted funding only when evidence is reliable;
- OI quantity/value;
- mark/index;
- basis;
- liquidation stream health;
- price/volume;
- volatility.

Features:

- 15M OI change;
- price/OI relationship;
- funding percentile;
- basis;
- liquidation intensity relative to venue-specific baseline.

Do not infer position ownership from OI.

High funding alone never triggers a short.

Initial main use:

- context/veto;
- correct funding costs.

Directional research is split into separately versioned policies.

### Cascade/deleveraging continuation challenger

Model asynchronous evidence states rather than requiring one universal event order:

1. **vulnerability** — crowding/funding/basis, structural location and available liquidity;
2. **break** — defined structure failure with adverse price/flow response;
3. **deleveraging evidence** — observed liquidation intensity and/or newly available OI contraction strengthens or weakens the hypothesis;
4. **continuation admission** — only the residual opportunity after the confirmation latency may become a candidate.

Do not infer exact future liquidation maps, trader ownership or leverage from public OI. Venue liquidation feeds may be incomplete/censored and must carry coverage/schema semantics.

### Post-cascade reversal challenger

Keep reversal separate. Require a declared exhaustion/absorption plus reclaim/flow-reversal condition. A large liquidation print alone is insufficient.

High funding alone never triggers a short.

---

# 17. S6 — Cross-sectional relative strength

This sleeve uses the broad market.

## Data

- point-in-time universe;
- hourly/4H returns;
- residual volatility;
- liquidity/spread;
- funding;
- BTC/market proxy;
- optional sector tag.

## Initial research policy

1. estimate trailing 30-day hourly beta to BTC market proxy;
2. residualize return;
3. rank 4H residual return / residual volatility;
4. require at least 20 strategy-eligible instruments or `NOT_ESTIMABLE`;
5. top decile long / bottom decile short candidates only if own 4H trend aligns;
6. use S1-style close-confirmed 15M trigger;
7. account risk rejects redundant crypto-beta exposure.

First release still permits one new opening intent per account.

A selected single long is not a market-neutral portfolio.

---

# 18. S7 — Event/news reaction

## Source hierarchy

1. official macro source;
2. official venue/project/security source;
3. reputable news source;
4. discovery aggregator.

Initial sources:

- Federal Reserve;
- BLS;
- SEC;
- Bybit/Binance official announcements/status;
- allowlisted official project/security channels;
- GDELT discovery;
- Coin Metrics Community for slower context where available.

## Initial capital behavior

Scheduled high-impact events are initially **entry gates**, not directional signals.

### Frozen capital blackout inherited from V1

For CPI, US payroll and FOMC rate decision:

> block new entries from **30 minutes before until 15 minutes after** the scheduled event.

The Astra 15m-before variant is retained only as a shadow research challenger until evidence supports versioning the capital gate.

Extend block while spread/volatility/source health remains abnormal.

Missing required calendar coverage => fail closed.

Unresolved exchange/asset incidents remain blocked until verified resolution or explicit human review; a timer cannot automatically clear them.

## Directional event shadow policy

Only research if:

- event/source is authenticated/resolved;
- event received before trigger window;
- affected asset mapping is unambiguous.

Initial reaction features:

- abnormal 5M return vs pre-event beta;
- abnormal volume;
- spread;
- subsequent confirmation.

Potential shadow trigger:

- second closed 5M bar continues first reaction direction;
- volume > historical 90th percentile;
- spread acceptable.

LLM sentiment is optional context only.

S7 has three distinct products:

1. **live user alerts** for authenticated/relevant events;
2. **deterministic safety/event gates** under frozen source/validation rules;
3. **directional shadow reaction policies** evaluated separately for economic value.

Publication time is not ATLAS availability. Preserve first receipt, extraction completion, revisions and source conflicts. LLM extraction is schema-constrained evidence enrichment only; ambiguous output remains unknown.

---

# 19. S8 — Relative value/pairs

S8 is research-only until multi-leg capital authority exists.

## Data

- synchronized prices;
- both books;
- fees;
- funding;
- economic pair definition.

## Initial research profile

- 30-day hourly fit;
- hourly decision;
- entry residual |z| > 2;
- convergence exit |z| < 0.5;
- stop |z| > 3.5;
- four-hour time exit;
- freeze beta for each simulated trade.

Include:

- both-leg fees;
- funding;
- partial fills;
- sequential delay;
- orphan-leg risk.

Output:

`ResearchBasketForecast`

Never emit a normal single-instrument executable plan.

---

# 20. Multi-timeframe coordinator

Default hierarchy:

```text
4H    regime / high-level structure
1H    setup
15M   confirmed trigger
5M    optional refinement
1M/L2 execution quality
```

This is a starting policy architecture, not a law for every strategy.

Event types include:

```text
CANDLE_CLOSED
BOOK_HEALTH_CHANGED
FUNDING_UPDATED
OI_UPDATED
LIQUIDATION_BURST
SCHEDULED_EVENT_DUE
NEWS_EVENT_RECEIVED
MODEL_RESULT_AVAILABLE
WATCH_EXPIRED
POSITION_REVIEW_DUE
SOURCE_HEALTH_CHANGED
```

Only closed bars are used when the feature requires closure.

---

# 21. Opportunity memory

Use deterministic durable application state, not chat/LLM memory.

State machine:

```text
DETECTED
  -> WAITING_FOR_EVENT
  -> READY_FOR_RECHECK
  -> CONFIRMED
  -> HANDED_OFF

terminal alternatives:
  -> INVALIDATED
  -> EXPIRED
```

`HANDED_OFF` means candidate accepted by decision pipeline, not order existence.

Schema:

```text
OpportunityWatchV2:
  watch_id
  key
  strategy_id
  strategy_version
  policy_hash
  state
  state_version
  created_at_ns
  updated_at_ns
  thesis_hash
  evidence_refs[]
  required_next_event
  wake_at_ns?
  invalidators[]
  expires_at_ns
  last_event_id?
  last_evaluated_at_ns
  parent_watch_id?
  handoff_receipt?
```

Restart:

1. load active watches;
2. restore required subscriptions;
3. consume missed events chronologically where evidence exists;
4. expire obsolete watches;
5. recheck current validity;
6. never re-submit an old historical trigger as a new live opportunity.

Use optimistic version compare and a transaction that writes:

- state transition;
- ops outbox item.

Deduplicate:

`(watch_id, event_id, state_version)`.

---

# 22. Permanent decision/evidence memory

Store evidence for:

- selected;
- rejected;
- unselected;
- no-trade;
- not-estimable;
- expired;
- no-fill;
- partial-fill.

Required categories:

- universe;
- eligibility;
- scanner rank;
- CandidateSet;
- timeframes;
- technical features;
- SMC/ICT;
- S/R;
- candle morphology;
- Fibonacci;
- Elliott morphology;
- regime;
- derivatives;
- flow;
- event/news;
- model artifacts;
- contradictions;
- expected costs;
- risk;
- scenarios;
- plan;
- approval;
- fill;
- PnL;
- fees;
- funding;
- slippage;
- MFE/MAE;
- mature counterfactual where defensible;
- attribution.

Storage:

- `ops.sqlite` for active operational intelligence state;
- Parquet/Arrow for immutable research artifacts;
- DuckDB for analysis;
- SQLite FTS5 only if text search is useful.

No vector DB initially.

Raw unreferenced L2 may have shorter retention, but evidence required to reproduce retained decisions cannot be silently deleted.

---

# 23. Final decision mechanism — no indicator voting

This section is frozen.

ATLAS must not do:

```text
RSI bullish +1
SMC bullish +1
Elliott bullish +1
ML bullish +1
=> BUY
```

## 23.1 Step A — event/universe eligibility

At a decision event:

- persist as-of universe;
- verify source health;
- verify policy-specific history;
- verify event gate;
- verify operational health;
- reject stale/unsupported data;
- generate policy candidates.

## 23.2 Step B — CandidateSet

Persist all candidate actions.

Examples:

```text
S1 LONG SOL
S2 LONG BTC
S3 SHORT ETH
S6 LONG LINK
```

No action disappears.

## 23.3 Step C — deterministic selection

Use only selection-stage information.

Selection policy is separately versioned and evaluated as part of the whole policy.

Ties use deterministic policy ID + instrument key rules.

Initial release with only S1:

- scanner rank is selection.

When S1/S2 coexist:

- use frozen selection policy;
- do not improvise manual priority after observing outcomes.

## 23.4 Step D — deterministic sizing

Risk computes the largest feasible rounded quantity satisfying:

- normal loss;
- stress loss;
- gross notional;
- instrument cap;
- correlation;
- margin;
- venue limits;
- rolling intraday risk;
- free margin.

Do not search quantity to maximize a noisy LCB.

## 23.5 Step E — freeze action identity

Action hash includes:

- instrument revision;
- venue;
- side;
- quantity;
- entry rule/collar;
- stop;
- exit policy;
- horizon;
- policy version;
- risk policy version.

## 23.6 Step F — action-conditioned economic evaluation

Define policy payoff:

```text
Y(a) =
  executed entry/exit cash flows
  - fees
  + signed funding cash flows
```

No-fill remains zero trade payoff but a real policy outcome.

Partial fill uses actual/simulated quantity.

Spread/impact must not be double counted.

## 23.7 M0 baseline

M0 is the required transparent meta baseline.

Use:

- regularized Huber/linear model for conditional mean;
- chronological OOF features;
- OOF residual distribution;
- separate calibrated logistic model only if `P(Y>0)` is required.

Indicators, SMC, model outputs and regime are features.

Weights are learned in training-only chronology, not hand-voted.

## 23.8 M1 challenger

LightGBM is the first nonlinear challenger.

It receives the same action-aligned OOF evidence.

Foundation-model outputs may become features.

M1 must beat M0 after:

- costs;
- calibration;
- stability;
- compute;
- operational degradation.

## 23.9 Amended action-value intelligence

The initial intelligence stack is intentionally small.

### M0 — transparent action-value baseline

Use the existing chronological regularized linear/Huber design to estimate **net value of the exact frozen action**, not raw direction. Fit only on eligible matured earlier outcomes. Include no-fill and partial-fill policy outcomes where defensibly labeled.

### M1 — first nonlinear challenger

LightGBM remains the first nonlinear challenger. It must use the same decision calendar and causal evidence as M0 and earn influence through chronological incremental-value evidence.

### Causal analogue challenger

Add a small numerical historical-state retrieval baseline for local support/OOD diagnostics, explanation and nonparametric action-value comparison. Compatibility must include policy/action semantics, horizon, venue/product, execution mode, liquidity/participation and feature availability. Report distance, weights, payoff dispersion, missingness and effective/independent support. Start with ordinary numerical retrieval; no vector database.

Analogue evidence is not an independent vote if the same neighbours also generate scenarios.

### Pretrade scenario artifact

`PretradeScenarioArtifactV2` is distinct from retrospective `ReplayPathV2` / `PolicyPayoffV2`.

It must:

- bind the exact frozen action hash and cutoff;
- use only sources/model vintages available by cutoff;
- finish before action expiry;
- preserve joint price/spread/depth/fill/latency/funding behavior where material;
- distinguish probability-bearing scenarios from deterministic stress;
- conserve weights and count fees/funding once;
- record support, model/calibration vintage and uncertainty.

Retrospective replay may mature labels for past decisions but can never become future evidence for its own decision.

### Explicit uncertainty budget

Economic admission separates:

```text
estimated action value
- estimation uncertainty
- execution-model uncertainty
- numerical uncertainty
```

Outcome dispersion and ES remain separate risk concepts. A lower PnL quantile is not a confidence bound on expected PnL.

Unsupported or materially OOD inference returns `NOT_ESTIMABLE`.

## 23.10 Step G — risk/economic gate

Require:

- positive expected-PnL lower bound;
- materiality threshold;
- stress pass;
- ES/portfolio pass;
- data support;
- numerical stability;
- venue capability;
- evidence binding.

Failure => `NO_TRADE` or `NOT_ESTIMABLE`.

## 23.11 Step H — immutable plan

Only admitted action creates `TradePlanEnvelopeV2`.

No strategy/model submits an order.


# 24. Risk and leverage V2

V1 RiskPolicy remains the baseline authority.

V2 adds versioned fields only where necessary.

## 24.1 Common 24h portfolio risk horizon

Retain a common 24-hour portfolio risk horizon initially.

If a strategy exits after 30 minutes or four hours:

- realized exit cash is held through common horizon;
- do not add another simulated trade unless sequential trading is explicitly part of that policy.

This permits portfolio ES comparisons across mixed holding periods.

## 24.2 Rolling intraday loss/risk budget — REQUIRED V2 AMENDMENT

A day-trading system may take sequential trades. One-candidate ES alone does not cap repeated losses.

Add to `RiskPolicyV2`:

```text
rolling_24h_realized_loss_limit_frac
rolling_24h_new_risk_limit_frac
max_opening_intents_per_account
```

Define:

```text
realized_loss_consumed_24h =
    max(0, -sum(realized_net_pnl of closed ATLAS V2 positions
                whose close time is within trailing 24h))
```

Profits do not increase the risk allowance.

Before a new trade:

```text
realized_loss_consumed_24h
+ existing_open_normal_loss
+ pending_reserved_normal_loss
+ proposed_normal_loss
<= rolling_24h_new_risk_limit
```

Also require:

```text
realized_loss_consumed_24h
<= rolling_24h_realized_loss_limit
```

If the realized-loss stop is exceeded:

- block new risk;
- preserve protection;
- require configured hysteresis/human recovery as defined by policy.

No live numerical values are authorized by this document.

Engineering values must be explicitly user-approved in a RiskPolicy version before capital.

## 24.3 Initial concurrency

Initial V2:

`max_simultaneous_new_risk_intents = 1` per account.

No pyramiding.

No new epoch until existing epoch is certified closed.

---

# 25. Scenario/economic evaluation

Reuse V1 scenario/risk primitives only when units/horizon semantics remain valid.

V2 gets a separate evaluator.

Do not modify `phase4_engine.py` into a generic V2 engine.

## 25.1 Two evidence classes — amended

Keep two explicit classes:

1. **Retrospective execution replay** — current Session-017 `ReplayPathV2` / `PolicyPayoffV2`; labels and diagnostics only.
2. **Decision-time pretrade scenarios** — new `PretradeScenarioArtifactV2`; generated exclusively from cutoff-known evidence and model/calibration vintages.

Never relabel a retrospective path as contemporaneous pretrade evidence.

Both classes must preserve relevant dependence between fills, liquidity deterioration and future returns. Do not independently shuffle fill states away from the market paths that generated them.

Execution-aware evidence may include:

- decision-to-arrival latency;
- IOC fill/no-fill;
- partial fills;
- actual/simulated depth;
- collar;
- stop trigger basis;
- stop gap-through;
- exit delay;
- fees;
- funding;
- event delays;
- human delay when assisted.

OHLC ambiguity:

- adverse ordering or unresolved range;
- never favorable ordering by default.

## 25.2 Common paths

Existing portfolio and candidate use synchronized common market paths.

Do not independently draw correlated instruments.

## 25.3 Multiplicity

V1 two-symbol correction does not mechanically extend to a broad universe.

V2 promotion evaluates **whole-policy OOS P&L** including:

- scanner;
- selection;
- strategy;
- refit schedule;
- cost;
- execution;
- gating.

Freeze tested policy family and use dependence-aware family-level correction.

More Monte Carlo paths do not create more independent market observations.

---

# 26. Model Arena — global Phase 7

Phase 7 begins before full V2 strategy expansion.

## 26.1 Architecture

```text
ModelRequestV2
  -> ModelProvider
       -> LocalProcessProvider
       -> RemoteHTTPProvider
  -> ModelAdapter
  -> ForecastArtifactV2
```

Heavy models run in isolated environments.

The live writer never imports heavy ML stacks.

## 26.2 Frozen initial engineering order

To preserve continuity with the V1 freeze while incorporating the Astra review:

1. statistical baseline;
2. **TiRex-2** — first general foundation adapter;
3. **Kronos-mini** — first finance-specific adapter;
4. **Chronos-2** — immediate general challenger;
5. Kronos-small;
6. later TimesFM, Moirai, Toto, TabPFN, FinCast and others only if justified.

This is an **integration order**, not a claim of predictive superiority.

At most:

- one general active pilot worker;
- one distinct finance active pilot worker.

Test models serially on modest hardware.

## 26.3 V2 forecast horizons

Versioned V2 outputs may support:

- 15M;
- 1H;
- 4H;
- optional 24H context.

This is a V2 extension; it does not alter the frozen V1 4h/24h feature contract.

## 26.4 Model manifest

Must include:

```text
provider
source_repository
source_commit
checkpoint_id
checkpoint_revision
weight_sha256[]
tokenizer_id?
tokenizer_revision?
tokenizer_sha256[]
code_license_ref
weight_license_ref
allowed_use_status
preprocessing_hash
postprocessing_hash
environment_lock_hash
device
precision
supported_inputs
supported_outputs
context_limit
training_cutoff_ns?
contamination_class
promotion_status
```

## 26.5 Request/response

Request:

```text
request_id
input_artifact_refs[]
input_hash
instrument key
policy_context_ref
model_manifest_hash
information_cutoff_ns
requested_targets[]
requested_horizons[]
requested_quantiles[]
deadline_ns
seed
resource_budget
```

Response:

```text
request_id
model_manifest_hash
input_hash
inference_started_ns
completed_ns
received_ns
expires_ns
targets[]
horizons[]
native_quantiles[]
values_ref?
samples_ref?
missing_outputs[]
units
resource_metrics
status
```

Unsupported quantiles remain unsupported.

No future realized funding or economic release is passed as a known covariate.

## 26.6 Deadline behavior

- idempotent request key;
- bounded queue;
- discard stale requests;
- late output archived but cannot alter past decision;
- retries do not extend decision deadline;
- model unavailable => separately qualified fallback or `NO_TRADE`.

## 26.7 Promotion

```text
INTEGRATED
 -> ENGINEERING_PASS
 -> HISTORICAL_DIAGNOSTIC
 -> PROSPECTIVE_SHADOW
 -> INCREMENTAL_VALUE_PASS
 -> DECISION_ELIGIBLE
```

No automatic live promotion.

---

# 27. News/events/fundamentals

## 27.1 Collection

Use simple typed Python collection.

Initial libraries:

- `httpx`;
- `feedparser`.

No agent framework required.

## 27.2 Event record

```text
NewsEventV2:
  event_id
  source_id
  source_url
  content_hash
  claimed_published_at_ns?
  received_at_ns
  extraction_completed_ns
  affected_asset_ids[]
  event_type
  severity
  confidence
  supporting_spans[]
  duplicate_group
  supersedes_id?
  expires_at_ns?
```

## 27.3 Processing

```text
fetch
 -> raw hash/archive
 -> canonical URL
 -> exact dedupe
 -> semantic fingerprint
 -> revision append
 -> structured extraction
 -> source conflict check
 -> asset mapping
 -> deterministic event policy
 -> market reaction features
```

An LLM may enrich descriptions using schema-constrained output.

Restrictions:

- no exchange tools;
- no credentials;
- no order authority;
- no unsupported numeric assertions;
- ambiguous => unknown.

---

## 27.4 Bounded strategy-discovery laboratory — amended

After the Phase-2 evaluator works end-to-end, ATLAS may run a zero-authority offline discovery laboratory.

Purpose:

- propose falsifiable feature interactions, motifs, failure slices and exact policy challengers;
- search large historical/forward evidence without granting an agent execution authority.

Rules:

1. preregister hypothesis family, features, availability assumptions, search/parameter budget, baseline, metrics and stop rule before outer evaluation;
2. every proposal becomes a deterministic versioned executable policy/experiment;
3. retain all attempted variants, failures, manual interventions and abandoned experiments;
4. fit preprocessing, feature selection, similarity metrics and calibration inside chronological training folds only;
5. use matured earlier labels only; purge overlapping label intervals and apply dependency-aware embargo where required;
6. control multiplicity across strategy/feature/selector variants with one declared dependence-aware family procedure;
7. a viewed final holdout is spent; redesign requires fresh future evidence;
8. require prospective shadow before promotion;
9. no self-promotion, live self-tuning, risk modification, credentials or order authority.

Successful-trader reverse-engineering is **DEFERRED** unless unusually complete, permitted, timestamp-precise entry/exit/position histories exist. If piloted, use it only to generate hypotheses and validate those hypotheses independently on the full market decision calendar.

---

# 28. Persistence

## 28.1 `ops.sqlite`

One writer: `atlas-ops`.

Tables:

```text
watch
watch_transition
ops_outbox
source_health
model_registry
artifact_index
```

## 28.2 Live control SQLite

One writer: `atlas-crypto-live`.

V1 semantics remain.

No network-shared writable SQLite.

## 28.3 Parquet

Decision calendar logical key:

```text
(policy_version, decision_slot, instrument_key, decision_revision)
```

Outcomes include:

```text
decision_id
action_hash
label_definition
label_view
horizon_end
matured_at
actual_or_simulated
fills
fees
funding
pnl
MFE
MAE
ambiguity_flags
```

Evidence lifecycle:

```text
PENDING
 -> MATURABLE
 -> MATURED

or QUARANTINED

revision -> SUPERSEDED
```

A rejected candidate's future market path does not prove hypothetical fill.

`MaturedOutcomeV2` (or an equivalent existing schema) must explicitly bind candidate/action/policy identity, decision time, outcome horizon, maturity/availability, payoff components, evidence resolution, unresolved/censored state and `ACTUAL | SIMULATED | COUNTERFACTUAL` provenance. Only separately qualified actual/reconciled outcomes may influence realized-risk authority.

The permanent decision calendar must retain selected, unselected, rejected, no-candidate, `NO_TRADE`, `NOT_ESTIMABLE`, expired, no-fill and partial-fill states. Selection itself is part of the evaluated policy.

---

# 29. Desktop implementation

## 29.1 Primary stack

Initial benchmark:

- PySide6 Widgets;
- PyQtGraph;
- Python packaging via PyInstaller after smoke tests.

Fallback if measured targets fail:

- Tauri + Lightweight Charts.

Do not switch merely for fashion.

## 29.2 Views

First release:

1. Overview
2. Scanner
3. Chart
4. Watches
5. Evidence

Add positions/risk/recovery controls when live bridge is qualified.

UI must display:

- instrument/venue/product;
- current selection rank;
- watch state;
- “waiting for 15M close”;
- rejection reason;
- data age;
- model expiry;
- cost estimate;
- capability state;
- new-risk enabled/disabled state.

## 29.3 IPC

Initial protocol:

- local domain socket Linux;
- authenticated loopback TCP Windows;
- 4-byte network-order frame length;
- UTF-8 JSON;
- maximum 1 MiB per frame;
- schema version;
- request UUID;
- deadline;
- payload hash;
- session token;
- method allowlist.

No pickle.

Large evidence is referenced by immutable artifact path/hash.

Snapshot + monotonic sequence streaming.

Sequence gap => request new snapshot.

Do not coalesce:

- approvals;
- risk;
- recovery;
- command outcomes.

## 29.4 Performance targets

On 4-core / 8GB x86-64 target machine:

- non-ML engine + ops + UI RSS target < 1.5GB;
- user action p95 < 200ms;
- chart updates <= 10Hz;
- default visible bars <= 10k;
- 100-instrument cheap scan target < 1s after inputs are ready;
- bounded queues;
- deep ML asynchronous.

These are targets, not claims.

---

# 30. Bybit and Binance execution qualification

## 30.1 Capability manifest identity

Bind each capability profile to:

```text
venue
environment
account_scope
account_mode
margin_mode
product_revision
Nautilus artifact hash
protection profile version
evidence hashes
expiry
```

Status:

```text
UNVERIFIED
SUPPORTED
UNSUPPORTED
EXPIRED
```

Documentation alone cannot produce `SUPPORTED`.

## 30.2 Bybit

Preserve V1 protection semantics.

Test:

- entry/stop wire fields;
- partial fill;
- stop coverage/resize;
- reduce-only;
- cancel/fill race;
- UNKNOWN submit;
- disconnect/reconnect;
- stop trigger while client dead;
- cash/funding reconciliation;
- emergency flatten.

## 30.3 Binance

Initial state:

> public data + shadow research allowed; new opening capital blocked.

Qualification must establish:

- position/margin mode;
- standard vs conditional/algo order identities;
- close-position/reduce-only semantics;
- stop coverage;
- partial fills;
- rejected protection;
- ambiguous submission;
- disconnect/recovery;
- triggered child/algo history;
- emergency flatten.

Do not assume Bybit attached-stop semantics exist on Binance.

If Binance cannot meet the V1-equivalent fill-time protection requirement, capital remains blocked until an explicit `BinanceProtectionProfileV2` is approved as a versioned risk contract.

---

# 31. Research protocol

## 31.1 Chronology

Initial evaluation schedule where history permits:

- 180d training;
- 30d inner validation;
- 30d outer test;
- advance monthly;
- at least three outer windows;
- final untouched holdout.

If history is insufficient:

`HISTORICAL_DIAGNOSTIC`, not a fabricated pass.

## 31.2 Leakage controls

- purge overlapping labels;
- embargo >= maximum policy holding horizon where needed;
- preprocessing fit only on training;
- meta model sees OOF base predictions;
- no full-sample swing/structure relabeling;
- no revised data leakage.

## 31.3 Costs/execution

Include:

- account fee schedule;
- spread;
- slippage;
- funding;
- IOC no-fill;
- partial fill;
- stop gaps;
- approval delay;
- model latency;
- order latency;
- cancellation races.

## 31.4 Metrics

Report at minimum:

- net PnL per opportunity;
- net PnL traded-only;
- expected value;
- confidence/uncertainty;
- fill rate;
- turnover;
- drawdown;
- ES;
- calibration;
- coverage;
- NOT_ESTIMABLE rate;
- compute cost;
- selection lift/missed-value share where estimable.

Win rate alone is insufficient.

## 31.5 Prospective shadow

Begin forward collection immediately.

Initial minimum observation target:

- 8 weeks;
- 200 matured candidate opportunities;
- meaningful volatility/liquidity diversity.

This is a minimum evidence-collection floor, not statistical proof.

Dependence may require much longer.

---

# 32. Efficient testing philosophy

V2 must be rigorous without wasting time through repeated irrelevant tests.

## 32.1 Test tiers

### Tier A — invariant smoke gate
Run on every implementation branch that touches code:

- import/compile;
- targeted unit tests;
- V1 golden invariant subset;
- serialization/hash smoke;
- secret scan;
- lint on changed code.

### Tier B — changed-seam integration
Run only when the branch changes the seam:

Examples:

- data branch => causal ingestion/replay tests;
- model branch => model ABI/deadline/isolation tests;
- desktop branch => IPC/UI crash tests;
- risk branch => risk/scenario tests;
- venue branch => recovery/capability fixtures.

Do not rerun UI packaging because a statistical feature changed unless the dependency boundary changed.

### Tier C — phase gate
At completion of each V2 implementation phase:

- full relevant V2 suite;
- full V1 regression suite;
- golden hash comparison;
- integration path for the phase;
- final diff/secret scan.

### Tier D — expensive evidence gate
Run only at defined milestones:

- 72-hour data soak after data foundation;
- full walk-forward/outer evaluation after a policy is frozen;
- prospective shadow continuously;
- exchange authenticated qualification only when explicitly authorized;
- Windows/Linux packaging benchmark at desktop phase and final release.

Do not rerun a 72-hour soak for documentation-only changes.

## 32.2 No redundant economic retesting

A strategy result is invalidated only when a change affects:

- data;
- features;
- labels;
- selection;
- policy;
- costs;
- risk;
- execution;
- model;
- decision timing.

Pure UI text/layout changes do not require a full economic backtest.

## 32.3 Full-suite cadence

- V2 Phase 0 baseline: full V1 suite once.
- Intermediate sessions: impacted tests + invariant subset.
- Each phase acceptance: full V1 + full relevant V2.
- Final release: all suites + packaging + recovery + qualification evidence.

---

# 33. V2 implementation phases

These are **V2 implementation phases**, not a renumbering of global V1 Phase 0–6.

They map mainly to global Phase 7 (Model Arena), Phase 8 (V2 research/intelligence) and Phase 10 (release qualification). Global Phase 9 FX remains separate.

The design intentionally uses **six phases** to minimize repetitive handoffs while keeping review boundaries meaningful.


## V2 PHASE 0 — V1 EXIT AUDIT & TRANSITION BASELINE
### First V2 session — mandatory

**Suggested session:** 010  
**Suggested branch:** `impl/session-010-v2-transition-audit`

### Purpose

Before writing V2 intelligence, establish exactly where V1 stopped and what can safely be reused.

### Scope

This phase is primarily analysis/audit/hygiene.

Do:

1. fetch repository branch inventory;
2. verify Session-009 SHA;
3. inspect Session-009 handoff;
4. inspect actual current tests/CI availability;
5. run the complete V1 offline suite in the available environment;
6. capture:
   - Python version;
   - dependency lock hash;
   - Nautilus artifact hash;
   - OS/platform;
   - suite results;
7. produce `docs/v2/V1_EXIT_AUDIT.md`;
8. identify:
   - frozen modules;
   - extension seams;
   - hard-coded BTC/ETH contracts;
   - stale README/package metadata;
   - unverified live capabilities;
9. correct stale README/project descriptions without altering V1 behavior;
10. establish golden V1 hashes/replay fixtures used by all later V2 phases;
11. verify no secrets;
12. do **not** implement strategies, desktop, Binance execution or model influence.

### Required audit output

Explicit matrix:

```text
IMPLEMENTED
TESTED
UNVERIFIED
TEST GATE
NOT ESTIMABLE
BLOCKED BY ENVIRONMENT
```

for every Phase 0–6 subsystem.

### Quality gate

Phase 0 passes only when:

- Session-009 source is verified;
- V1 tests are reproduced or environment-blocked evidence is explicit;
- golden outputs/hashes are captured;
- V1 extension seams are documented;
- stale docs are corrected;
- no V1 contract was silently changed;
- handoff exists.

If V1 baseline cannot be reproduced, V2 coding stops until the discrepancy is understood.

---

## V2 PHASE 1 — ADDITIVE FOUNDATION, DYNAMIC UNIVERSE, MEMORY & MODEL ARENA CORE
### Maps to global Phase 7 + V2 foundation

**Suggested sessions:** 011–012 if needed, one phase gate  
**Primary outcome:** safe research foundation with no new capital authority.

### Scope

Implement:

#### Contracts
- InstrumentKeyV2;
- ProductContractV2;
- UniverseContractV2;
- FeatureArtifactV2;
- PolicySpecV2;
- CandidateActionV2;
- CandidateSetV2;
- StrategyForecastV2;
- EvaluationArtifactV2;
- TradePlanEnvelopeV2;
- ModelRequest/Manifest/Forecast;
- OpportunityWatchV2.

#### Universe/data
- canonical instrument registry;
- Bybit V2 public adapter using existing primitives;
- Binance public market-data adapter;
- strategy-specific eligibility;
- tiered subscriptions;
- causal bars;
- source health;
- historical importer.

#### Memory
- `ops.sqlite`;
- watch;
- transition;
- outbox;
- source health;
- artifact index;
- restart recovery.

#### Model Arena core
- provider-neutral ABI;
- local isolated worker;
- remote HTTP worker;
- manifest registry;
- deadline/idempotency;
- statistical baseline;
- TiRex-2 adapter;
- Kronos-mini adapter;
- Chronos-2 adapter may be integrated as immediate challenger if phase capacity permits, otherwise first task of Phase 2;
- all model influence remains zero.

#### Forward evidence
Start live/public credential-free collection immediately.

### Not in scope

- live Binance orders;
- strategy capital;
- meta trading;
- S3–S8 full implementation;
- automatic model promotion.

### Quality gate

Pass only when:

- V1 golden suite unchanged;
- V2 schema/hash tests pass;
- unknown plan/schema versions fail closed;
- point-in-time universe history is reproducible;
- Binance public data is causal and restart-safe;
- watch state survives restart;
- model worker cannot access trading secrets/live DB;
- late model output cannot affect expired decision;
- one short public collection run validates reconnect/duplication;
- then begin/continue the longer 72-hour collection soak without blocking unrelated coding.

Do not wait for model alpha evidence to pass this engineering phase.

---

## V2 PHASE 2 — CORE INTRADAY INTELLIGENCE, S1/S2, EVALUATOR & DESKTOP
### Global Phase 8 — first complete measurable policies

**Current status at SHA `f083319...`: TEST GATE.** S1/S2, CandidateSet/selection, hard-risk sizing, frozen action identity and retrospective execution replay are implemented/tested. M0, honest matured labels, decision-time pretrade scenarios, final evaluator/ES admission, `EvaluationArtifactV2`, `TradePlanEnvelopeV2` and desktop/end-to-end closure remain.

**Primary outcome:** complete read-only/shadow V2 system for S1/S2 before broader strategy influence.

### Scope

Implement:

#### Feature core
- 4H/1H/15M joins;
- EMA/RSI/ADX/ATR;
- realized volatility;
- VWAP;
- candle geometry;
- causal swing/SR/SMC;
- Fibonacci;
- Elliott morphology;
- regime axes.

#### Mathematics
- robust slope;
- Kalman forward filter;
- EWMA/HAR-style volatility;
- empirical residual distribution;
- common-path utilities.

#### Strategies
- S1 exact policy;
- S2 exact policy;
- watch creation/wakeup;
- policy-specific invalidation/expiry.

#### Selection/evaluation
- CandidateSet;
- deterministic candidate rank;
- action hash;
- deterministic quantity;
- V2 scenario/economic evaluator;
- M0 baseline;
- costs/funding;
- rolling intraday risk budget contract;
- 24h portfolio risk horizon.

#### Amended remaining decision seam
- audit irrecoverable forward collection and add collection-only paths if essential evidence is missing;
- mature-outcome/provenance contract;
- `PretradeScenarioArtifactV2` distinct from retrospective replay;
- honest S1/S2 chronological labels;
- M0 net action-value baseline;
- joint execution-aware pretrade scenario generator;
- explicit estimation/execution/numerical uncertainty;
- support/OOD diagnostics;
- final economic/ES admission;
- `EvaluationArtifactV2`;
- `TradePlanEnvelopeV2` shadow path.

#### Desktop
- Overview;
- Scanner;
- Chart;
- Watches;
- Evidence;
- local IPC;
- UI does not own runtime.

### Not in scope

- capital dispatch;
- Binance trading;
- standalone orderflow scalping;
- S8 capital;
- automatic multi-strategy allocation.

### Quality gate

Pass only when one complete path works:

```text
public candle
 -> universe
 -> features
 -> S1/S2 watch/candidate
 -> CandidateSet
 -> quantity
 -> evaluation
 -> NO_TRADE or shadow plan
 -> evidence
 -> desktop
 -> matured outcome
```

Required:

- prefix/future-tail invariance;
- SMC swing confirmation tests;
- strategy kill/ablation tests;
- OOF-only economic model;
- no-fill/partial-fill/cost replay;
- rolling-risk arithmetic tests;
- GUI crash does not affect engine;
- Windows/Linux packaging smoke if environments exist;
- complete V1 regression at phase gate.

---

## V2 PHASE 3 — RESEARCH EXPANSION, EVENTS, MARKET-WIDE INTELLIGENCE & META CHALLENGERS
### Global Phase 8 — broaden only after S1/S2 work end-to-end

**Suggested session:** 015, possibly split internally without creating new architecture phase  
**Primary outcome:** complete research library without granting automatic capital.

### Scope

Implement:

- S3 mean reversion;
- S4 order-flow/absorption execution context + separately versioned standalone shadow;
- S5 asynchronous crowding/deleveraging continuation + separately versioned exhaustion/reversal challengers;
- S6 cross-sectional relative strength;
- S7 live event/news alerts + deterministic safety/context + separately evaluated directional shadow;
- S8 research basket only unless joint-action authority is explicitly versioned later;
- official event/news collection;
- event dedupe/revision;
- 30m-before/15m-after capital blackout inherited from V1;
- optional LLM structured event enrichment;
- LightGBM M1 challenger;
- Chronos-2 if not completed;
- further model challengers only serially when justified;
- group ablation for SMC/candles/Fibonacci/Elliott/Wyckoff/time-of-day;
- causal analogue retrieval baseline for support/OOD/explanation;
- bounded zero-authority AI strategy-discovery laboratory;
- successful-trader reverse-engineering deferred unless unusually complete permitted data already exist;
- strategy-selection policy for multiple sleeves.

### Quality gate

Pass only when:

- each sleeve has exact input/availability contract;
- unavailable input produces named fallback or NOT_ESTIMABLE;
- all sleeves produce artifacts, not orders;
- S8 cannot reach normal TradePlan;
- order-book gaps invalidate S4;
- news duplicates do not duplicate watches/candidates;
- event receipt latency is replayed;
- M1 uses OOF evidence only;
- CandidateSet preserves all competitors;
- multiplicity/selection audit is produced;
- whole-policy S1/S2 baseline remains reproducible.

No sleeve receives live authority merely because implementation exists.

---

## V2 PHASE 4 — VENUE QUALIFICATION, CAPITAL BRIDGE & FAILURE HARDENING
### Separate engineering authority gate

**Suggested session:** 016  
**Primary outcome:** prove whether V2 can safely route an already-qualified policy through a venue.

### Scope

#### Bybit
- rerun V1 capability evidence against exact current artifact/account profile;
- V2 plan bridge;
- approval binding;
- risk/reservation integration;
- protection/recovery regression.

#### Binance
- exact Nautilus adapter/account profile;
- one-way/isolated profile;
- order/algo identity mapping;
- stop/protection spike;
- UNKNOWN;
- partial fills;
- disconnect;
- history/reconciliation;
- emergency flatten.

#### Failure hardening
Inject:

- GUI death;
- worker death;
- news death;
- disk full;
- writer crash before/after send;
- lost ACK;
- late fill;
- cancel/fill race;
- model timeout;
- book gap;
- stale metadata;
- duplicate events;
- stale writer/fencing.

### Capital rule

No capital is enabled merely because Phase 4 code passes offline.

Venue capability status must be:

`SUPPORTED`

for the exact:

- venue;
- environment;
- account;
- product;
- margin/position mode;
- Nautilus artifact;
- protection profile.

If Binance protection cannot satisfy the required contract, Binance remains research/shadow.

### Quality gate

- no duplicate opening under UNKNOWN;
- no premature reservation release;
- no false CLOSED;
- no unprotected owned exposure accepted by the required profile;
- stale writer fenced;
- approval invalidated by plan/risk/venue change;
- V1 regression unchanged;
- full fault matrix documented.

---

## V2 PHASE 5 — RELEASE QUALIFICATION & CONTROLLED V2 RELEASE
### Final V2 release phase

**Suggested session:** 017+ when prospective evidence is mature  
**Primary outcome:** release one narrowly frozen policy/venue/profile or explicitly release shadow-only.

### Scope

1. freeze exact release candidate:
   - venue;
   - account profile;
   - policy;
   - model requirements;
   - feature versions;
   - selection policy;
   - cost model;
   - risk policy;
   - capability manifest;
2. complete walk-forward/holdout evidence;
3. complete prospective shadow evidence;
4. complete 72-hour or longer operations soak as required;
5. desktop packaging;
6. recovery drill;
7. current fee/filter/margin verification;
8. dependency/secret scan;
9. handoff;
10. user approval of live RiskPolicy if capital is considered.

### Minimum economic evidence target

Before any positive economic claim:

- >= 8 weeks prospective shadow;
- >= 200 matured candidate opportunities;
- adequate regime coverage;
- dependence-aware effective sample interpretation.

This is not an automatic pass threshold.

### Release possibilities

#### A. SHADOW_RELEASED
All research/desktop features available; capital remains disabled.

#### B. ASSISTED_CANARY_ELIGIBLE
Only if:

- economic gate passes;
- venue capability passes;
- recovery passes;
- explicit RiskPolicy approved;
- exact policy promoted;
- no unresolved critical defects.

#### C. BLOCKED
Keep scanner/desktop/research useful while capital remains off.

There is no schedule-based requirement to force category B.

---

# 34. Remaining implementation/session plan from Session-017

The architecture still has exactly six V2 phases. The following is the **minimum sensible remaining session plan**, not new phases.

Model allocation is deliberate:

- **GPT-6 Sol:** genuinely difficult causal/statistical/safety design, ambiguous evidence semantics, evaluator/ML/microstructure/capital reviews.
- **GPT-6 Luna:** high-volume implementation where contracts and gates are already explicit: adapters, CRUD/UI, deterministic wiring, repetitive tests/docs/packaging.
- Do not mix a difficult open-ended design problem into a Luna session merely to save cost. Do not spend a Sol session on bulk mechanical implementation when the contract is already settled.
- A session may be split only when reviewability or a failed gate requires it. Do not create extra sessions for convenience.

### Target remaining sequence — 8 sessions

```text
018 SOL   Phase 2: collection/evidence audit + matured-outcome and pretrade-scenario contracts
019 SOL   Phase 2: chronological M0 + joint scenario evaluator + uncertainty/ES admission + shadow plan
020 LUNA  Phase 2: desktop/IPC/evidence wiring + end-to-end integration + Phase-2 gate

021 LUNA  Phase 3: explicit breadth — S3, S6, S7 collection/alerts/safety and mechanical feature/context wiring
022 SOL   Phase 3: S4 absorption + S5 deleveraging/cascade/reversal + structural/time-context challengers
023 SOL   Phase 3: M1 + analogue intelligence + bounded discovery lab + multiplicity/selection audit + Phase-3 gate

024 SOL   Phase 4: venue qualification, capital bridge and failure/recovery hardening
025 LUNA  Phase 5: release engineering, packaging, full regression, documentation and shadow-release closure
```

This is a planning target, not permission to skip a failing gate. If prospective/soak/authenticated evidence is unavailable, Session 025 may correctly end as `SHADOW_RELEASED` or `BLOCKED`; calendar time cannot be replaced by more Codex sessions.

The immediate next session from `f083319...` is Session 018. Preserve the current S1/S2/selection/risk/replay contracts while auditing whether forward evidence needed by S4/S5/news can still be honestly collected.

---

# 35. Development workflow

Every implementation session:

1. inspect repo;
2. read V1 freeze;
3. read this V2 freeze;
4. verify starting SHA/branch;
5. create/use dedicated branch;
6. implement bounded scope;
7. preserve unrelated work;
8. add tests;
9. run relevant test tier;
10. inspect final diff;
11. secret scan;
12. write/update handoff/closeout with exact tests, assumptions, gates and final intended checkpoint;
13. push the checkpoint branch;
14. verify the remote tip matches the intended final SHA;
15. **independently review the pushed SHA/diff/tests through GitHub before authorizing the next session**.

A Codex completion report is evidence to inspect, not automatic acceptance. The coordinating ChatGPT must verify the pushed branch/SHA and review relevant actual files/diff/handoff before generating the next Codex session prompt.

Handoff records:

- starting SHA;
- final SHA;
- implementation completed;
- tests and environment;
- assumptions;
- unverified behavior;
- external gates;
- exact remaining work.

Do not merge until review passes.

---

# 36. Preferred libraries/tools

These are frozen **initial selections**, subject to exact version/install qualification.

## Core
- Python 3.12 line preserved initially for V1 reproducibility;
- NautilusTrader current pinned V1 artifact until versioned upgrade;
- SQLite WAL;
- PyArrow/Parquet;
- DuckDB;
- pytest;
- Hypothesis where useful.

## Desktop
- PySide6;
- PyQtGraph;
- PyInstaller.

## Technical/statistical
- TA-Lib Python batch API;
- NumPy;
- SciPy;
- statsmodels;
- `arch`;
- scikit-learn;
- LightGBM.

## Online/drift challengers
- River;
- `ruptures` for offline research only where causality is respected.

## HTTP/news
- `httpx`;
- `feedparser`.

## IPC
- standard local sockets first;
- pyzmq only if measured fan-out/backpressure needs justify it.

## Research references, not default dependencies
- Cryptofeed;
- Tauri;
- Lightweight Charts;
- smartmoneyconcepts;
- Qlib;
- MLflow.

No dependency is adopted just because it exists.

---

# 37. Agent/MCP policy

Agents are allowed for:

- literature/repository search;
- experiment proposal;
- code generation;
- tests;
- documentation;
- post-trade attribution;
- model scouting;
- data-quality explanation.

Agents are not allowed to:

- hold trading credentials;
- submit orders;
- alter risk limits;
- approve their own strategy;
- promote research to live.

Do not introduce a general agent framework unless typed Python jobs are demonstrably insufficient.

The bounded strategy-discovery laboratory is an offline research capability, not a live autonomous trader. It receives no credentials, cannot modify RiskPolicy, cannot promote its own output and cannot silently rewrite a live policy.

---

# 38. Security

Never commit:

- API keys;
- exchange credentials;
- Telegram tokens;
- passwords;
- `.env`;
- private account identifiers;
- sensitive logs.

Use:

- `.env.example`;
- minimum-permission trading keys;
- withdrawal permission disabled where possible;
- local OS-protected credential storage if the desktop later stores credentials;
- sanitized logging;
- secret scan before push.

No model worker receives account secrets.

---

# 39. Observability and operator behavior

Every process exposes health:

```text
process
version
uptime
last successful input
data lag
queue depth
disk health
model worker status
source health
new_risk_allowed
capability status
recovery state
```

Alerts must distinguish:

- research failure;
- degraded data;
- capital blocked;
- protection/recovery critical.

Do not flood the operator with every feature transition.

---

# 40. Stop conditions

Stop adding features when:

- current policy cannot beat its simple baseline after realistic costs;
- feature ablation shows no incremental value;
- model improvement does not justify runtime/maintenance;
- support is insufficient;
- execution assumptions dominate the result;
- venue capability is unresolved;
- reproducibility cannot be maintained.

Do not respond to weak evidence by adding more indicators.

Do not respond to a weak evaluator by increasing model complexity before label quality, selection bias, fill/return dependence, support/OOD and uncertainty accounting are understood.

---

# 41. Explicit immediate non-goals

Not first V2:

- spot capital;
- market making;
- passive queue strategies;
- CEX/DEX arbitrage;
- cross-venue smart routing;
- multi-venue simultaneous capital;
- options;
- cross/portfolio margin;
- raw-data RL trader;
- large custom foundation-model training;
- large autonomous web platform;
- Kafka/Kubernetes;
- vector DB;
- automatic research-to-live promotion.

---

# 42. Status vocabulary

Use exactly:

- **IMPLEMENTED**
- **TESTED**
- **UNVERIFIED**
- **TEST GATE**
- **NOT ESTIMABLE**
- **BLOCKED BY ENVIRONMENT**

Do not label code “working” solely because it exists.

---

# 43. V2 release definition

V2 is not defined by live trading.

A valid V2 release may be:

> a complete causal desktop research/shadow system scanning the broader liquid futures universe across Bybit/Binance data, with durable memory, exact strategy/evidence artifacts, S1/S2 complete policies, Model Arena, mathematical/technical intelligence, reproducible outcomes and capital blocked.

Capital eligibility is a separate qualification state.

---

# 44. Final frozen statement

ATLAS V2 is:

> **a cross-platform, retail-accessible, venue-flexible intraday crypto futures intelligence and trading-research system that reconstructs a causal market state from technical/structural, microstructure, derivatives and event evidence; lets S1–S8 propose exact candidate actions; freezes deterministic risk-sized actions before economic admission; evaluates those actions using transparent chronological action-value modelling, execution-aware pretrade scenarios, uncertainty/support/OOD checks and measured challengers; learns from the complete decision calendar through bounded zero-authority research; and routes only explicitly qualified plans through the existing deterministic ATLAS risk/recovery/execution authority.**

The implementation priority is:

> **correct evidence and one complete measurable policy before breadth.**

The capital priority is:

> **recovery and risk before opportunity.**

The scientific priority is:

> **after-cost complete-policy value, not indicator agreement or model accuracy in isolation.**

The engineering priority is:

> **reuse proven libraries and existing V1 foundations; do not build complexity without a demonstrated need.**
