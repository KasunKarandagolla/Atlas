# ATLAS RED — Independent Professional Trader Logic Review

**Review date:** 2026-10-13  
**Repository:** KasunKarandagolla/Atlas  
**Reviewed implementation lineage:** fix/red-r3a-completion-2026-10-13  
**Verified branch head at review start:** 4faf9a065eb00df705694bc107401c41c0b322b2  
**Tested implementation SHA recorded by the project:** 8fbf1ec31588a1c315acce048176066b4c6bbecc  
**Review type:** Independent trading logic, quantitative strategy, risk, execution and system-design review  
**Capital authority:** None. This report does not enable orders, activate capital, change trading logic, or authorize API/provider spending.

---

## 1. Executive trader verdict

Atlas is much stronger at **causal evidence, fail-closed controls, immutable decision provenance, risk accounting, after-cost evaluation design and protection semantics** than it is at **finding and choosing genuinely superior trades**.

If my own capital depended on Atlas today, I would **not permit capital deployment**. That is not because the codebase lacks sophistication. It is because the part that should create edge remains unproven, while the system currently spends substantial complexity proving the integrity of a candidate whose initial selection is still primarily driven by **instrument turnover rank rather than comparative expected after-cost trading value**.

The strongest parts are worth preserving:

- point-in-time data and chronology discipline;
- exact instrument identity and immutable artifacts;
- explicit missingness and fail-closed behavior;
- hard risk sizing with fees, stop loss, stress, margin, drawdown, gross notional and portfolio ES constraints;
- exact frozen actions;
- matured, executable, after-cost outcomes;
- chronological M0/M1 research discipline;
- coherent joint execution scenario paths rather than independently shuffled components;
- strong separation of research/AI from capital authority;
- strict protection evidence and reconciliation semantics.

The weakest parts are the ones a professional trader would care about most:

1. **Candidate selection is not an alpha competition.** The current production ranking evidence is PUBLIC_UNIVERSE_TURNOVER_V1, and the multi-sleeve selector chooses the lowest scanner rank. This means the most liquid eligible signal wins before its economics are evaluated.
2. **The economic evaluator is a veto after selection, not a cross-candidate optimizer.** If the turnover-ranked winner is economically rejected, Atlas does not evaluate the second-best simultaneous candidate and choose it instead.
3. **S1, S2 and S3 have plausible hypotheses but no repository evidence establishing durable after-cost edge.** The project itself correctly reports economic value/profitability as NOT ESTIMABLE.
4. **S3 is especially problematic in current production.** Its setup/stop geometry tends to require substantial reversion before a valid stop is on the correct side of the entry, and current production explicitly treats complete Bybit trade coverage as unproven, preventing ordinary creation of S3 VWAP/residual/watch/candidate state from those observations.
5. **S6 is potentially useful research but is not currently an approved new-run selector sleeve.** Its BTC-only residual factor model is also too crude for serious altcoin cross-sectional capital allocation.
6. **M0 is statistically ambitious relative to its support floor.** It has 121 fixed input columns while allowing support from 30 compatible rows and 20 independent rows. Ridge/Huber regularization and chronological calibration are good defenses, but this remains a weak data-to-dimensionality ratio for a conditional trading value model.
7. **Current risk is safer than most retail crypto systems, but it sizes to hard-risk capacity, not conviction-adjusted edge.** In addition, correlated crypto beta is effectively treated as plus or minus one unit of notional for every instrument.
8. **Real operational proof remains incomplete at exactly the points that matter for a close-driven intraday system:** final close delivery inside the five-second decision window, complete trade-stream support for S3, full schedule denominators, authenticated protection transport, and capital-qualified venue lifecycle behavior.
9. **AI/memory currently adds observability, not measurable trading intelligence.** That is a good safety state. It should stay that way until incremental value is demonstrated prospectively.

My preferred direction is not “add more indicators” or “add a smarter AI.” It is the opposite: **reduce the alpha surface to one or two simple strategies, collect honest complete-policy outcomes, make candidate comparison explicitly after-cost and portfolio-aware under a separately approved authority amendment, and delete every feature/model/filter that cannot demonstrate incremental chronological value.**

---

## 2. Repository and authority verification

### 2.1 Verified Git state

The supplied implementation branch exists and resolved to:

- **fix/red-r3a-completion-2026-10-13**
- head **4faf9a065eb00df705694bc107401c41c0b322b2**
- message: **docs: verify R3A completion evidence receipt**

The branch history immediately below that head is:

- **60e9ce…** — documentation/evidence update;
- **8fbf1ec31588a1c315acce048176066b4c6bbecc** — **test: verify production fact replay after restart**;
- earlier R3A implementation commits beneath that.

A direct comparison of **8fbf1ec… → 4faf9a…** shows that the two later commits are documentation/evidence changes, not a newer trading implementation. Therefore this review treats:

- **4faf9a…** as the verified branch head and evidence state;
- **8fbf1ec…** as the project-recorded tested implementation state.

The repository default branch is not the R3A branch, so this review did not infer implementation state from the default branch.

The RED lineage also contains R1/R2 and audit branches. A direct comparison showed the R3A completion head is ahead of the R1 final-correctness head in the same lineage. The separately named audit branch is not treated as the current implementation authority.

### 2.2 Read authority and project control documents

Reviewed:

- **AGENTS.md**
- **README.md**
- **docs/INDEX.md**
- **log.md**
- **ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md**
- **ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md**
- **docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md**
- relevant current R3A handoff/evidence documents
- proposed S47/S48 amendment documents to distinguish future intent from current authority.

The immutable authority hashes declared in **docs/INDEX.md** are:

- V1 freeze: **c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c**
- V2 freeze: **e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78**
- Agent freeze: **d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d**

Important authority distinction:

- The V1, V2 and Agent freeze documents are operative frozen authority.
- S47 comparative decision and S48 management documents in the repository are **proposals**, not accepted amendments.
- The repository index references a newer owner RED target/design source outside this repository. I do not treat that out-of-repository target as a replacement for the frozen authority.
- Any recommendation below that changes candidate-selection authority, management rules, AI influence, sizing authority or capital behavior is explicitly marked as requiring a **separate versioned approval/amendment**.

### 2.3 Test evidence treatment

The repository records broad R3A evidence, including the tested implementation SHA and affected-suite passes. I treat that as evidence of implemented behavior and invariants.

I do **not** treat unit/integration test success as evidence of profitability.

I did not independently execute the repository test suite in this review. No claim below depends on pretending that I did.

---

## 3. Finding classification

I use three evidence classes:

- **PROVEN DEFECT / PROVEN CURRENT LIMITATION** — directly established by the current call path, policy contract, current production gate, or repository status evidence. “Defect” here can mean a trader-logic defect relative to the objective of intelligent opportunity selection even when the implementation faithfully follows its frozen specification.
- **PLAUSIBLE TRADING WEAKNESS** — the code implements the stated rule, but market-microstructure or statistical reasoning makes the rule questionable. This requires empirical testing rather than immediate code correction.
- **UNVERIFIED HYPOTHESIS** — a potentially valuable or harmful effect that cannot be established without chronological market evidence.

Severity reflects expected trading consequence, not software severity.

---

## 4. Highest-priority findings

| ID | Severity | Class | Finding | Trading consequence |
|---|---|---|---|---|
| F1 | Critical | Proven current trader-logic defect | Production scanner rank is descending public-universe turnover, and multi-sleeve selection picks rank 1 before economics | Atlas can select the most liquid signal rather than the best after-cost opportunity |
| F2 | High | Proven current limitation | Economics evaluates the already selected/frozen action; it does not compare all simultaneous candidates | A rejected winner can cause a no-trade even if another candidate would have positive after-cost value |
| F3 | High | Proven evidence gap | Profitability/economic value remains NOT ESTIMABLE; no complete current evidence proves durable edge | Capital decision cannot be justified from repository evidence |
| F4 | High | Proven current limitation + plausible design weakness | S3 ordinary production origin is blocked by unproven complete Bybit trade coverage; stop geometry tends to force late entry | S3 is not currently a dependable live research opportunity source and may surrender much of the mean-reversion move before entry |
| F5 | High | Plausible statistical weakness | M0 uses 121 columns with minimum 30 compatible / 20 independent samples | Conditional expected-value estimates can be unstable despite ridge/Huber and OOF controls |
| F6 | High | Proven operational evidence gap | S1/S2 use five-second BBO/bar/candidate windows while real final close delivery within that window is not fully qualified in project status | Systematic expiry or stale evidence can erase nominal strategy opportunity |
| F7 | Medium-High | Plausible strategy weakness | S1 and S2 permit stops up to 3 ATR but use short 4h/2h time exits and no strategy-level profit target | Poor payoff geometry can require unusually high win rate or strong immediate continuation after costs |
| F8 | Medium-High | Proven simplification | Hard risk uses signed notional as correlated-crypto beta unit, effectively beta ±1 per instrument | Altcoin portfolio correlation risk can be materially under- or over-estimated |
| F9 | Medium | Proven current design | S2 loads 1H/4H context but declares “no direction gate” | Dead context adds data dependency without demonstrated edge |
| F10 | Medium | Proven current design | New S6 actions are excluded from the multi-sleeve selector pending approval | Cross-sectional alt opportunity research cannot currently become the selected production research action |
| F11 | Medium | Proven current design | Any uncertain candidate/scanner evidence can make the multi-sleeve selection NOT_ESTIMABLE | One incomplete candidate can suppress otherwise valid opportunities |
| F12 | Medium | Proven current state | M1, analogue research, production intelligence and memory have zero authority and do not improve current selection | Complexity exists without measured decision lift; safe but currently non-alpha |
| F13 | High for capital | Proven current blocker | Authenticated Bybit protection port remains a test gate; lifecycle projection is bounded/demo-oriented and cannot authorize opening/maturity | Atlas is not capital-ready even if signal economics later become positive |

---

## 5. Actual end-to-end decision path

The current important call path is not simply the older selector module.

The production path in **src/atlas/v2/runtime/production.py**:

1. builds point-in-time market/universe evidence;
2. runs strategy coordinators and persists unsized candidates;
3. creates scanner evidence;
4. calls **src/atlas/v2/science/research_selection.py::assemble_multisleeve_research_candidate_set**;
5. hard-sizes the selected candidate through **src/atlas/v2/risk.py::size_selected_candidate**;
6. freezes the exact action;
7. runs **run_phase2_economic_evaluation** for that exact action;
8. separately runs M1/analogue diagnostics;
9. persists decision-calendar/outcome state;
10. projects zero-authority production intelligence/memory;
11. capital/protection qualification remains separately gated.

This is generally a good architectural separation. The problem is **where the optimization happens**: selection occurs before full economics.

### 5.1 The production ranking objective is liquidity, not edge

Evidence:

**src/atlas/v2/runtime/production.py**, candidate generation/ranking around lines 5787–5814:

- eligible instruments are ordered by **descending trailing turnover**;
- rank is assigned from that order;
- scanner source identity is **PUBLIC_UNIVERSE_TURNOVER_V1**;
- candidates inherit the rank of their instrument.

Then:

**src/atlas/v2/science/research_selection.py::assemble_multisleeve_research_candidate_set**

- supports S1/S2/S3 new-run sleeves;
- deliberately rejects outcome/model/economic artifacts as selection inputs;
- sorts candidates by scanner rank, then policy ID, canonical instrument identity and candidate ID;
- selects the first candidate.

This creates a clean, leakage-resistant selector, but it is not a trader’s best-opportunity selector.

A high-turnover BTC signal with mediocre payoff geometry can beat a materially better ETH or alt candidate solely because BTC turnover is higher. If two strategies trigger on the same instrument, the later tie-break is policy identity rather than measured opportunity quality.

That is safe and deterministic. It is not an economically intelligent ranking objective.

### 5.2 Economics is a veto, not comparative selection

**src/atlas/v2/runtime/production.py** runs hard risk and freezes the selected action before economic evaluation.

**src/atlas/v2/science/evaluation_service.py::run_phase2_economic_evaluation** then computes M0, scenarios, uncertainty, stress, portfolio ES and admission for that one exact action.

This is scientifically defensible for avoiding “shopping” among noisy models, but it has an important trader consequence:

> If candidate A wins because its instrument has higher turnover and A later fails after-cost admission, Atlas does not automatically evaluate candidate B from the same decision opportunity set and choose B.

This is the single most important architectural weakness I would change if the goal becomes actual capital efficiency.

However, changing this behavior would alter frozen selection authority. A comparative after-cost selector belongs behind a **separate versioned amendment**, not a silent change.

---

## 6. Strategy-by-strategy review

## 6.1 S1 — multi-timeframe trend pullback

**File:** src/atlas/v2/strategies/s1_trend.py  
**Policy:** S1_MTF_TREND_PULLBACK  
**Policy builder:** policy_spec  
**Core implementation:** _regime, setup/watch handling, subsequent M15 trigger/candidate handoff

Current baseline parameters from policy_spec:

- decision event: confirmed 15M close;
- 4H regime;
- latest three closed 1H bars for pullback/setup;
- trigger: subsequent confirmed 15M close breaks prior confirmed 15M high/low;
- IOC limit entry;
- BBO age <= 5s;
- mark/index age <= 5s;
- confirmed-bar max lag <= 5s;
- candidate deadline = decision + 5s;
- adverse entry collar = 5 bps;
- stop based on same three closed 1H setup bars plus 0.25 ATR15m buffer;
- accepted stop distance = 0.5–3 ATR15m;
- fixed stop, no trailing, no partial TP, no pyramiding, no averaging;
- time exit = 4h;
- setup/watch expires after four subsequent 15M bars or contrary confirmed 4H regime.

### Edge hypothesis

The underlying hypothesis is credible: trade pullback/reclaim in a confirmed higher-timeframe trend, then require lower-timeframe continuation.

This is a valid family to test.

### What is good

- Rule is explainable and falsifiable.
- Symmetric long/short treatment is better than discretionary asymmetry.
- Exact completed-bar chronology is strong.
- Stop is tied to the setup, not an arbitrary account percentage.
- No averaging down or discretionary widening.
- Variants are explicit research variants rather than silently adaptive production behavior.
- Entry data freshness is strict.

### Concerns

**S1-A — Severity High — Plausible weakness: payoff horizon versus stop width.**

A 4H regime + three-hour pullback + confirmed M15 breakout can be a relatively late continuation entry, while stop width can be as large as 3 ATR15m and the action times out after four hours.

There is no fixed strategy-level take-profit and no minimum strategy-level reward-to-risk ratio. Economic admission can reject poor actions later, which is good, but the raw policy itself may produce many candidates whose maximum realistic remaining impulse is too small for their stop width.

Acceptance test: bucket S1 outcomes by stop distance in ATR and measure after-cost expectancy and LCB for 0.5–1, 1–2 and 2–3 ATR buckets. If the widest bucket does not contribute positive incremental value, remove it rather than asking M0 to learn around it.

**S1-B — Severity Medium — Plausible weakness: confirmation redundancy and lag.**

4H trend confirmation + 1H pullback/reclaim + M15 breakout are three related trend/momentum conditions. Multiple correlated confirmations often improve narrative confidence while degrading entry price.

Ablate:
- simple 4H trend + M15 trigger;
- 4H trend + 1H pullback without reclaim;
- current baseline.

Keep the extra confirmation only if it improves after-cost value, not hit rate alone.

**S1-C — Severity Medium — Unverified hypothesis: funding/OI/flow might help only conditionally.**

The baseline intentionally does not use funding, OI or flow; they exist as variants. That is preferable to blindly adding them.

Do not promote those variants based on intuition. Require chronological incremental net-value improvement after costs and turnover.

### Trader verdict on S1

**Retain as research. Do not expand it.** It is simple enough to test, but its current complete-policy edge is unproven and its entry/stop/horizon geometry needs direct after-cost evidence.

---

## 6.2 S2 — compression breakout

**File:** src/atlas/v2/strategies/s2_breakout.py  
**Policy:** S2_COMPRESSION_BREAKOUT  
**Policy builder:** policy_spec

Key parameters:

- confirmed 15M decision;
- 2880-bar comparison sample, approximately 30 days;
- latest measurement is completed t-1;
- Bollinger width20 below empirical 20th percentile;
- ATR14 below its historical median;
- frozen range = prior 20 M15 bars, approximately 5h;
- trigger requires close beyond range by > 0.1 pre-trigger ATR;
- volume must be strictly above 20-bar median;
- 1H/4H context refs are required but explicitly **no direction gate**;
- IOC entry, 5 bps collar, five-second freshness/deadline;
- stop is opposite frozen range boundary;
- range/stop width accepted from 0.5–3 ATR;
- failed-break management: first two subsequent closed M15 bars, exit if close returns inside range;
- time exit = 2h.

### Edge hypothesis

Of the current strategies, S2 is the one I would test first with my own research budget.

Compression → expansion is a legitimate crypto futures hypothesis, and S2 has explicit failed-break management rather than relying only on a distant stop.

### What is good

- Exact compression sample and quantile convention are frozen.
- Trigger excludes the setup range and uses pre-trigger ATR.
- Failed-break logic addresses the most important breakout failure mode.
- Variants already exist for raw range, compression-only, volume-only and delayed retest, enabling useful ablation.
- It is straightforward to explain where expected P&L should come from: post-compression directional expansion.

### Concerns

**S2-A — Severity Medium — Proven design inefficiency: 1H/4H context is required but unused directionally.**

policy_spec literally declares:

**latest_confirmed_1H_4H_refs_no_direction_gate**

That creates data dependency and missingness without a decision rule.

Either:
- remove those required refs from the baseline; or
- turn higher-timeframe context into a separately preregistered variant and prove it adds after-cost value.

Do not keep “context” because it sounds sophisticated.

**S2-B — Severity High — Plausible weakness: 5h stop range versus 2h edge horizon.**

The stop can span up to 3 ATR while maximum holding time is two hours.

That can be rational only if the trade has very strong immediate convexity: small number of very large winners, or a high frequency of quick failed-break exits before full stop.

The repository does not establish that distribution.

Required analysis:
- MAE/MFE by stop-width bucket;
- failed-break exit contribution;
- fraction of full stops before first/second failed-break close;
- after-cost expectancy by breakout distance and volatility regime.

**S2-C — Severity Medium — Plausible weakness: 30-day percentile is a regime-sensitive reference.**

Crypto volatility can shift much faster than 30 days. A 20th-percentile Bollinger-width threshold plus ATR below median may mean very different absolute conditions after a volatility transition.

Do not replace this with more knobs. Test alternative fixed lookbacks as discrete policies and require stability across high/low volatility regimes.

**S2-D — Severity Medium — Plausible weakness: volume > median is weak confirmation.**

A 20-bar median is a low bar for “breakout participation.” It may not distinguish institutional expansion from random range leakage.

The right question is not whether a higher volume threshold looks better in-sample. It is whether the existing volume filter adds incremental chronological after-cost value over the COMPRESSION_ONLY variant.

**S2-E — Severity Medium — Plausible microstructure weakness: confirmed-close chase.**

The system waits for a close beyond the range +0.1 ATR, then uses IOC within a five-bps collar. On fast altcoin breakouts, this can enter after the best liquidity is gone.

This must be tested with real depth/latency/no-fill evidence. Candle backtests are insufficient.

### Trader verdict on S2

**Make S2 the primary strategy-development candidate.** It has the cleanest falsifiable source of edge and the best existing ablation structure. Do not add complexity until the basic compression-breakout policy proves after-cost value.

---

## 6.3 S3 — VWAP statistical mean reversion

**File:** src/atlas/v2/strategies/s3_mean_reversion.py  
**Policy:** S3_VWAP_STAT_MEAN_REVERSION  
**Policy builder:** policy_spec  
**Entry handoff:** S3ShadowCoordinator.on_subsequent_bar

Key parameters:

- confirmed 1M bars;
- causal public-trade UTC-day VWAP;
- residual = ln(price) - ln(VWAP);
- exact seven-day completed 1M AR window;
- 10081 AR observations;
- OLS AR(1), phi strictly inside (0,1);
- half-life accepted from 5–30 minutes;
- z standardization over 120 preceding completed residuals;
- watch threshold |z| > 2;
- reject ADX14 > 25;
- trigger only when a subsequent completed 1M residual moves measurably toward frozen VWAP;
- BBO age <= 1s;
- 5 bps collar;
- long stop = frozen VWAP × exp(-1 residual sigma);
- short stop = frozen VWAP × exp(+1 residual sigma);
- max hold = 60 minutes.

### What is good

- Uses actual trade VWAP rather than candle proxy.
- Requires causal trade completeness.
- Explicitly freezes the VWAP used by the setup.
- AR/half-life qualification is preferable to blindly fading every z-score.
- Rejecting strong trend is directionally sensible.
- One-minute decision and one-second BBO requirements acknowledge the shorter edge half-life.
- No discretionary target movement or resizing.

### Major current limitation

**S3-A — Severity High — Proven current production limitation: complete Bybit trade coverage is not proven.**

In **src/atlas/v2/runtime/production.py**, the production S3 path contains an explicit comment that complete Bybit trade coverage remains unproven. The code therefore keeps those observations diagnostic and calls S3 setup evaluation with empty residuals/current VWAP so they cannot produce ordinary new VWAP/residual/watch/candidate state.

This is exactly the correct fail-closed choice.

But from a trader perspective it means S3 is not currently a dependable production opportunity source even though the multi-sleeve selection policy lists S3 as an eligible action sleeve.

Do not interpret “S3 selectable” as “S3 operationally producing valid opportunities.”

### Structural trading concern

**S3-B — Severity High — Plausible design weakness: stop geometry forces late reversion entry.**

on_subsequent_bar computes entry from current BBO, then places the stop at ±1 residual sigma around the frozen VWAP and rejects the candidate if the stop is not on the valid side of entry.

For a positive-residual short setup, a valid short requires current entry **below** the +1σ stop. For a negative-residual long setup, a valid long requires current entry **above** the -1σ stop.

Because the watch originates at an extreme deviation and entry is only allowed after movement toward VWAP, this mechanism can require a substantial part of the expected reversion to occur **before Atlas is allowed to enter**.

The Session 021 test fixture makes this visible: it creates a very positive setup residual around VWAP 100 and does not produce a short candidate until the BBO is near 100/100.1, with stop about 101.1.

This is not a coding error—the wrong-side stop guard is correct. The question is whether the frozen stop definition leaves enough expected residual edge after entry to pay spread, taker fees, slippage and adverse selection.

Acceptance criterion:
- measure expected distance-to-VWAP at actual valid entry;
- compare it with entry-to-stop distance and total cost;
- reject the policy if median/LCB payoff geometry after valid trigger is not materially positive.

**S3-C — Severity Medium — Plausible weakness: UTC-day VWAP reset.**

A UTC-day VWAP has a structural reset. The meaning of residual, sigma and reversion can change near the day boundary. This requires explicit time-of-day analysis.

**S3-D — Severity Medium — Plausible weakness: AR(1) stationarity assumptions.**

A seven-day AR(1) on residuals around a changing intraday VWAP can be useful as a filter, but it should not be mistaken for stable statistical arbitrage. Crypto intraday residual dynamics are regime-dependent and strongly affected by trend/liquidation episodes.

### Trader verdict on S3

**Pause strategy promotion.** First prove complete trade-stream coverage. Then re-evaluate stop/entry geometry. I would not spend time tuning AR parameters until those two issues are solved.

---

## 6.4 S6 — cross-sectional relative strength

**File:** src/atlas/v2/strategies/s6_cross_section.py  
**Policy:** S6_CROSS_SECTIONAL_RELATIVE_STRENGTH

Research score parameters:

- minimum breadth = 20;
- 30 days × 24 completed hourly returns = 720;
- exact timestamp intersection of asset and BTC;
- beta = sample covariance(asset, BTC) / sample variance(BTC);
- residual return = asset hourly log return - beta × BTC hourly return;
- residual volatility = sample standard deviation of residual hourly returns;
- score = sum of latest four residual hourly returns / residual volatility;
- own 4H trend using EMA20/EMA50;
- top decile long only if own trend aligns;
- bottom decile short only if own trend aligns.

The later S6 exact action contract adds:

- subsequent confirmed M15 breakout;
- five-second BBO/mark/deadline;
- 5 bps collar;
- three-H1 setup stop with 0.25 ATR buffer;
- 0.5–3 ATR stop distance;
- 4h horizon.

### Current authority status

**S6-A — Severity Medium — Proven current limitation.**

The current multi-sleeve research selector explicitly excludes new S6 actions pending approval. S6 can produce sidecar/research evidence but does not compete for the selected new-run action.

This is correct under current authority.

### Trading concerns

**S6-B — Severity Medium-High — Plausible weakness: BTC-only one-factor residual is too crude for alts.**

Many alts have material ETH, sector, ecosystem and liquidity-factor exposure. A single 30-day static BTC beta can label factor rotation as “idiosyncratic” relative strength.

Before capital use I would compare:
- BTC-only residual;
- BTC+ETH two-factor residual;
- simple sector/cluster neutral residual where enough breadth exists.

Only keep extra factors if they improve out-of-sample after-cost rank spread.

**S6-C — Severity Medium — Plausible weakness: score scaling is not a calibrated z-statistic.**

The numerator is a four-hour residual sum and denominator is one-hour residual standard deviation. Cross-sectional ranking can still work, but the numeric score should not be interpreted as a four-hour z-score without horizon scaling and autocorrelation assumptions.

**S6-D — Severity Medium — Plausible weakness: stacked momentum confirmation can be late.**

Four-hour residual momentum + own 4H trend + later M15 breakout are all related continuation signals. This can select the most extended names precisely when funding, spread and liquidation risk are highest.

### Trader verdict on S6

**Keep research-only.** It is potentially the most interesting altcoin diversification sleeve, but the factor model and entry timing need cleaner evidence before action approval.

---

## 7. Indicators, feature engineering and market structure

**Files:**
- src/atlas/v2/features/technical.py
- src/atlas/v2/features/structure.py
- src/atlas/v2/features/pipeline.py
- related candle/regime/location modules

Atlas computes a broad set of technical, candle, structure, location, Fibonacci and regime features.

This is not automatically a problem because many are diagnostic/research features rather than direct strategy gates.

The problem arises when broad feature availability encourages the system to infer that “more context” equals more edge.

### 7.1 Redundancy and narrative risk

EMA trend, robust slope, ADX, ROC, candle direction, BOS/CHOCH and related structure signals often describe overlapping price persistence.

If multiple correlated features are used together without ablation, they can:
- make historical examples look convincing;
- reduce sample size;
- increase lag;
- increase model dimensionality;
- hide the fact that the original price trend did the work.

The V2 freeze itself contains the right philosophy: stop adding complexity if the complete policy does not beat a simple baseline after realistic costs.

I would enforce that principle more aggressively.

### 7.2 M0 dimensionality

**src/atlas/v2/science/m0.py** defines:

- 30 frame-specific technical features: 10 features × H4/H1/M15;
- 25 other market/structure/regime fields;
- every market field contributes both a value and a missingness column;
- 11 action/context columns.

That produces **121 fixed input columns**.

Yet **fit_m0** allows:

- minimum compatible training rows = 30;
- minimum independent support = 20;
- minimum OOF training rows = 5;
- minimum OOF calibration rows = 30.

Ridge regression can fit p > n and Huber loss improves robustness, but that does not create information. With only 20–30 independent complete-policy observations, 121 inputs create a substantial risk that estimated conditional value is unstable and dominated by regularization choices.

This is a **plausible statistical weakness**, not proof that M0 is wrong.

Important positive detail: **_compatibility** includes policy hash, venue, product, trigger basis, horizon duration, management rule, entry rule, collar rule and base units, so S3 is not silently pooled with S1/S2 merely because the explicit one-hot action columns only name S1 and S2. The compatibility partition is strong.

### Recommendation

Use small strategy-specific M0 feature sets.

For example:

- S1: stop distance, normalized trend slope, pullback depth, breakout distance, realized volatility, spread/depth, funding if demonstrably relevant.
- S2: compression percentile, ATR percentile, range width/ATR, breakout distance/ATR, volume surprise, spread/depth.
- S3: z-score, half-life, residual sigma, distance to VWAP, ADX, spread/depth, time-of-day.
- S6: cross-sectional rank, factor residual, residual vol, liquidity rank, funding, trend state.

Start with roughly 6–12 economically interpretable variables per strategy, then add only features with repeated chronological incremental value.

This feature-policy change would require proper versioning of M0 schema/model policy, but it does not require granting AI or capital authority.

---

## 8. Universe and opportunity discovery

**File:** src/atlas/v2/data/universe.py  
**Class:** DynamicUniverseRuntimeV2

Default eligibility includes:

- minimum 30 observed days;
- minimum 24h quote turnover = USD 10,000,000;
- maximum spread = 10 bps;
- healthy current source;
- required bars;
- strategy-specific history gates;
- tiered compute budget;
- turnover-ranked tiers.

### What is good

- Point-in-time eligibility is explicit.
- Full instrument identity is preserved.
- Data/source health is not treated as a cosmetic warning.
- Low-liquidity symbols are excluded before expensive strategy work.
- Compute tiers are bounded.
- Active watches/positions receive priority.

### Trader concerns

**Turnover is being asked to do two jobs.**

It is appropriate as a tradability/compute-priority variable.

It is not appropriate as the final opportunity-quality rank.

A sensible professional workflow is:

1. use turnover/spread/depth to decide **whether an instrument is tradable**;
2. calculate strategy-specific opportunity quality among tradable instruments;
3. evaluate expected after-cost complete-policy value;
4. allocate risk across qualified opportunities.

Atlas currently lets step 1 dominate step 2.

### BTC/ETH versus alts

The current design is likely to prioritize BTC/ETH or the largest alts whenever several candidates exist because turnover rank is the tie-break that matters first.

That is safe for execution quality but can defeat the stated objective of identifying profitable altcoin opportunities.

I would not solve this by artificially boosting alts. I would solve it by separating **liquidity eligibility** from **expected-value ranking**.

---

## 9. Entry, exits, stops, take-profits and payoff geometry

### 9.1 Entries

IOC limit entries with fixed adverse collars are operationally disciplined.

However, a fixed 5 bps collar across BTC, ETH and alts is not an economic model. It is an engineering rule.

The economic layer’s depth/slippage scenarios are therefore essential. Any backtest or replay that assumes every qualified IOC fills at the reference price would be invalid.

### 9.2 Stops

The code is strong on:
- wrong-side rejection;
- exact MarkPrice semantics;
- no discretionary widening;
- stress loss;
- native protection evidence.

The weakness is not “stops are bad.” It is whether each strategy’s stop location is statistically aligned with the actual edge horizon.

S1/S6 allow up to 3 ATR with four-hour horizons.  
S2 allows a 3 ATR range with two-hour horizon.  
S3 uses a one-sigma residual stop around VWAP, which may force late entry.

These must be validated as complete policies, not justified with generic R multiples.

### 9.3 No take-profit

The frozen baseline has no ordinary partial take-profit/trailing behavior. I do **not** recommend immediately adding them.

That would multiply degrees of freedom before basic edge is proven.

S48’s half-at-2R management comparator is explicitly proposed, not current authority. If management alternatives are tested, keep them as preregistered complete policies and require genuine chronological improvement.

### 9.4 Time exits

Time exits are useful because intraday edge decays.

But 2h/4h/60m are currently policy choices, not demonstrated optimal horizons.

The correct validation is:
- conditional net expectancy by holding time;
- hazard of stop/failed-break after edge decay;
- marginal P&L from each extra holding interval;
- funding crossings;
- opportunity cost.

If edge is usually realized or invalidated much earlier, shorten the horizon rather than adding a model to predict it.

---

## 10. Hard risk and position sizing

**Files:**
- src/atlas/v2/risk.py
- src/atlas/domain/risk.py

The engineering defaults are explicitly labelled **ENGINEERING_DEFAULTS_ONLY_NOT_SAFE_FOR_LIVE**.

Notable defaults:

- normal loss per trade = 0.10% of eligible equity;
- aggregate open normal loss = 0.50%;
- stress loss per trade = 0.25%;
- portfolio ES alpha = 97.5%;
- portfolio ES limit = 1.0%;
- gross notional <= 1.0× eligible equity;
- instrument notional <= 0.5×;
- correlated crypto beta limit <= 0.75×;
- free margin reserve = 50%;
- max leverage = 2×;
- drawdown reduction begins at 5%;
- new risk stops at 10% drawdown;
- maximum simultaneous new risk intent = 1.

### What is genuinely good

**src/atlas/v2/risk.py::size_selected_candidate** is one of the best parts of Atlas.

It verifies:
- policy and product identity;
- current account state;
- exposure completeness;
- no pyramiding;
- pending/open exposure consistency;
- actual closed outcome provenance;
- rolling loss;
- drawdown scaling;
- stop side;
- stress exit support;
- fees;
- gross/instrument notional;
- margin and leverage;
- stress and portfolio ES;
- venue limits.

Normal unit loss includes stop distance plus entry and exit taker fees.

This is considerably more realistic than sizing from “1% account risk / stop distance” alone.

### 10.1 Correlation weakness

The correlated beta unit is effectively:

- +notional for long;
- -notional for short.

That means every instrument is treated as beta magnitude 1 for this hard bound.

For BTC that may be a reasonable conservative shorthand. For high-beta alts it can understate market-equivalent exposure. For low-beta or hedged instruments it can overstate it.

**Recommendation:** preserve the simple hard gross/notional caps, but add a separately versioned conservative instrument beta/covariance exposure bound once enough causal estimates exist. Never let a noisy factor model relax the existing gross/stress limits.

### 10.2 Sizing to capacity rather than quality

The function finds the maximum quantity allowed by the hard constraints.

That means a barely admitted action and a very high-confidence action can both be sized to the same binding hard-risk ceiling.

This is intentional separation of hard risk from model authority, and it is safer than naive Kelly sizing.

Still, with real capital I would eventually want a **bounded edge tier below the hard cap**, but only after robust prospective evidence. That change affects sizing authority and therefore requires explicit versioned approval.

I would not use unconstrained Kelly sizing in this system.

---

## 11. Fees, slippage, liquidity, funding and execution realism

### 11.1 Fees and funding

**src/atlas/v2/science/costs.py** has explicit:

- taker entry/exit fees;
- signed funding cashflow;
- funding schedules;
- action cost contracts.

**src/atlas/v2/science/outcome_resolution.py** requires typed cutoff-known fee and funding evidence before executable monetary maturity.

This is strong.

### 11.2 Execution scenarios

**src/atlas/v2/science/scenario_engine.py** is also strong conceptually:

- requires exact action and execution model evidence;
- uses intact joint execution paths;
- includes entry, exit, fees and funding;
- includes no-fill/partial-fill mechanics;
- validates S2 failed-break and stop precedence;
- refuses unsupported scenario support rather than inventing probabilities;
- uses deterministic seed identity.

This is much better than adding an arbitrary “0.05% slippage” to candle backtests.

### 11.3 Remaining realism risk

The quality of the result still depends on the quality and representativeness of the joint path templates, depth evidence, latency model and execution residual calibration.

A sophisticated evaluator can produce false confidence if its execution support is narrow.

Acceptance requirements should therefore include:
- market-impact sensitivity by quantity/depth participation;
- latency perturbation;
- spread widening;
- gap-through-stop behavior;
- no-fill and partial-fill frequency;
- venue outages/stale BBO;
- funding crossings;
- action size scaling.

Do not certify a strategy solely because M0 predicts positive value. Certify the complete action only when execution support is representative.

---

## 12. M0, M1, scenarios, outcomes and learning

## 12.1 M0

**File:** src/atlas/v2/science/m0.py  
**Model:** Huber ridge action-value baseline

Strong properties:

- exact frozen action binding;
- compatible complete-policy history;
- causal feature snapshot;
- matured executable outcomes;
- chronological OOF predictions;
- future labels cannot rewrite prior OOF state;
- independent support counting avoids overlapping holding windows;
- calibration and OOD gates;
- explicit missingness;
- estimate uncertainty.

Weakness:

- too many features for early sample support;
- initial support floors are adequate as “do not even try below this” gates, not adequate proof of stable conditional alpha.

I would keep M0 but drastically reduce its feature dimension and treat it as a **calibration/veto model**, not the source of strategy edge.

## 12.2 M1

**File:** src/atlas/v2/science/m1.py  
**Model:** pinned deterministic LightGBM challenger

Current design is disciplined:

- no live import requirement;
- no selection/sizing/action mutation authority;
- 180d training + 30d validation + chronological outer windows;
- embargo;
- untouched 30d holdout reservation;
- four preregistered small parameter configurations;
- deterministic seed/threading;
- genuine historical chronology requirement;
- OOF calibration and OOD.

This is a good research challenger.

But production **src/atlas/v2/runtime/production.py::_run_m1** executes it after the action is already selected/frozen/economically evaluated, as a diagnostic.

Therefore M1 currently contributes **zero measurable trading decision value** unless later research uses its results to approve a versioned model policy.

My rule:

> If M1 cannot repeatedly beat a low-dimensional M0 on untouched chronological complete-policy net value and calibration, remove it from the critical path and keep it offline.

Do not reward model sophistication.

## 12.3 Outcomes and counterfactuals

The outcome architecture is one of Atlas’s strongest research assets.

It distinguishes:
- actual/simulated/counterfactual provenance;
- selected/unselected/rejected states;
- executable action value versus non-executable diagnostics;
- fee/funding/replay evidence;
- matured versus censored/unresolved labels.

Tests explicitly cover counterfactual matured economically rejected exact actions as M0 targets.

That is valuable because it reduces one form of selection bias.

The remaining requirement is a complete decision-calendar denominator so “no opportunity,” “not estimable,” rejected and selected states can be analyzed without survivorship bias.

---

## 13. AI decisions, research memory and production intelligence

**Files:**
- src/atlas/v2/runtime/production_intelligence.py
- src/atlas/v2/research/decision_brief.py
- src/atlas/v2/research/memory.py
- Agent freeze authority

Current production projection is correctly zero-authority.

Notable facts:

- projection is bounded;
- it does not alter A0 decision artifacts;
- exact action economics is shown only when qualified persisted evidence exists;
- candidate ranking is explicitly labelled **PRELIMINARY_ONLY_NO_AFTER_COST_INFERENCE**;
- full schedule denominator can be unavailable;
- M1/analogue typed diagnostics are not reconstructed into current production brief;
- memory projection records **provider_job_status = NOT_CREATED_NO_AUTHORITY**;
- paid dispatch remains false in this production projection;
- the Agent freeze prohibits trading authority.

### Trader assessment

This layer is not currently the problem.

It also is not currently a source of edge.

Keep it as:
- evidence summarization;
- post-trade review;
- anomaly explanation;
- research hypothesis generation;
- memory retrieval.

Do not allow it to:
- override candidate selection;
- resize;
- move stops;
- choose leverage;
- authorize orders.

Any future S47-style comparative decision role is a material authority change and must be separately approved and prospectively tested.

---

## 14. Position lifecycle and protection

**Files:**
- src/atlas/runtime/protection_deadline.py
- src/atlas/runtime/protection_evidence.py
- src/atlas/runtime/protection_port.py
- src/atlas/runtime/position_lifecycle.py
- src/atlas/runtime/phase4_v2.py

### Strengths

Protection evidence is rigorous:

- exact position epoch;
- exact signed quantity;
- exact stop price;
- MarkPrice trigger basis;
- full-position market semantics;
- closing-only behavior;
- evidence IDs;
- freshness.

The protection deadline state machine uses a two-second unconfirmed threshold, two-second repair interval and bounded repair attempts, and transitions to recovery-required rather than assuming an acknowledgement equals protection.

### Capital blocker

**src/atlas/runtime/protection_port.py::create_protection_port** returns an unavailable test-gate implementation for the supported Bybit development/testnet profile.

The Binance lifecycle projection is explicitly bounded/demo-oriented and has no authority to admit opening, release reservations or mature economics.

**src/atlas/runtime/phase4_v2.py** correctly requires a complete authenticated testnet capability matrix before capital capability.

This is a proven blocker to capital readiness and should remain one.

No trading-edge improvement justifies bypassing protection qualification.

---

## 15. Where Atlas can currently make poor trading decisions

### Failure mode 1 — chooses liquidity instead of edge

BTC S1 triggers and ETH S2 triggers simultaneously.

BTC gets scanner rank 1 due higher turnover.

S1 is selected, sized and frozen.

Economic evaluator rejects S1.

ETH S2 is never chosen for that slot even if its complete-policy expected value would have been positive.

This is the most important current decision-quality failure mode.

### Failure mode 2 — one uncertain candidate suppresses the set

The multi-sleeve selector is deliberately conservative: uncertain eligibility/scanner evidence can make the whole selection NOT_ESTIMABLE.

This avoids cherry-picking around missing data, which is scientifically clean.

But operationally it means one bad candidate can suppress valid independent opportunities.

A future amended selector should distinguish:
- uncertainty that contaminates the comparison population;
- uncertainty local to one candidate.

That change requires careful denominator semantics and separate authority approval.

### Failure mode 3 — late confirmation leaves poor residual payoff

S1/S6 stack multiple momentum confirmations.

S3 waits for reversion and then needs a valid one-sigma stop on the correct side.

S2 waits for a completed breakout close.

All can enter after a large portion of the move has already happened.

This is not necessarily wrong—confirmation can improve hit rate—but it makes **after-cost remaining excursion** the critical metric.

### Failure mode 4 — risk cap is correct but economic quality is marginal

A candidate can be within all hard-risk constraints and still be a mediocre trade.

Risk management prevents catastrophe; it does not create edge.

The architecture understands this via economic admission, but because admission happens after selection, capital opportunity can still be wasted.

### Failure mode 5 — model complexity outruns real observations

M0/M1 can be scientifically correct and still add no value when sample size is low.

Crypto strategy data is especially deceptive because many observations share the same market regime.

Twenty “independent” non-overlapping holds are not twenty independent market regimes.

### Failure mode 6 — operational timing invalidates paper edge

S1/S2 candidate lifetime is five seconds.

If confirmed close, feature production, BBO/mark, candidate selection, risk, economics or publication cannot complete causally within the window, the theoretical signal is irrelevant.

The repository already treats these timing/throughput questions as qualification work. That should be considered part of strategy edge, not merely infrastructure.

---

## 16. What I would build if my own capital depended on Atlas

This section describes my preferred design, not current authority.

### 16.1 Keep

I would retain almost unchanged:

- immutable causal data/evidence model;
- exact full instrument identity;
- fail-closed missingness;
- dynamic tradability universe;
- explicit exact-action policies;
- no pyramiding by default;
- hard loss/notional/margin/drawdown limits;
- frozen actions;
- fee/funding-aware outcomes;
- coherent execution scenarios;
- decision calendar;
- chronological OOF/holdout discipline;
- protection/reconciliation evidence;
- zero-authority AI.

These are durable advantages.

### 16.2 Remove or simplify

I would remove from the **critical decision path** until proven useful:

- broad decorative market-structure context that does not change a preregistered rule;
- redundant 1H/4H S2 context with no directional gate;
- large M0 feature surface;
- M1 from routine decision-time processing if it cannot demonstrate incremental value;
- any AI-produced “confidence” that is not a measured forecast with calibration;
- strategy expansion beyond S1/S2 until those policies are economically understood.

### 16.3 Start with one strategy

I would make **S2 compression breakout** the first serious complete-policy research target.

Reasons:
- explicit source of edge;
- simple counterfactual;
- clean ablation variants;
- failed-break management;
- straightforward execution assumptions;
- no dependence on incomplete public trade reconstruction.

Then test S1 as a second independent sleeve.

I would pause S3 promotion and keep S6 research-only.

### 16.4 Separate tradability ranking from opportunity ranking

Current turnover ranking should remain as a **liquidity/compute priority**.

It should not be the final opportunity selector.

My target flow would be:

1. build point-in-time tradable universe;
2. generate all exact strategy candidates;
3. hard-filter candidates that cannot be safely sized;
4. calculate candidate-specific complete-policy after-cost distribution;
5. calculate marginal portfolio stress/ES contribution;
6. rank only candidates whose conservative expected value exceeds a materiality floor;
7. choose the candidate with highest conservative value per unit of scarce risk/capital;
8. reject all if none qualify.

A simple possible comparison statistic is:

**candidate score = conservative expected net value / max(normal stop risk, stress contribution, marginal portfolio ES contribution)**

The exact formula should be preregistered and tested. It should not be tuned by looking at final holdout results.

This is a material departure from the current selector-before-economics architecture and therefore requires a **versioned selection-authority amendment**. S47 is directionally related but remains proposed.

### 16.5 Keep sizing conservative

Do not jump to Kelly.

Even after edge proof:
- retain hard 0.1%-class trade-risk caps or separately approved conservative equivalents;
- preserve gross/instrument/stress/ES/margin limits;
- add an edge tier only below the hard cap;
- never let ML increase size because of a single high prediction.

### 16.6 Build strategy-specific small models

Replace the general 121-column M0 surface with small per-strategy feature policies.

A model should answer:

> “Given this exact S2 setup, how has this complete policy performed after costs in comparable causal states?”

not:

> “Can 121 technical/context variables discover a hidden trading rule from 30 rows?”

### 16.7 Treat execution as part of alpha

For my capital, a signal with positive candle-return expectancy but unreliable IOC fill or expensive stop execution has no edge.

Every policy report should therefore show:
- no-fill rate;
- partial-fill rate;
- realized entry slippage;
- exit slippage;
- stop gap loss;
- fee/funding share of gross edge;
- latency sensitivity;
- capacity versus depth.

### 16.8 AI remains a reviewer

I would use an LLM to:
- summarize why a signal was rejected;
- compare current state with prior failed/successful episodes;
- identify untested hypotheses;
- draft postmortems.

I would not let it choose the trade until a separately governed prospective experiment proves an incremental benefit over the deterministic system.

---

## 17. Prioritized improvement plan

## P0 — establish an honest full opportunity denominator

**Value:** highest  
**Authority impact:** no trading-authority expansion required

Relevant files:
- src/atlas/v2/runtime/production.py
- src/atlas/v2/runtime/ops_supervisor.py
- src/atlas/v2/science/outcomes.py
- src/atlas/v2/runtime/production_intelligence.py

Problem:
current production intelligence can report that the whole-schedule denominator is unavailable.

Acceptance criteria:
- every expected decision slot has exactly one terminal state;
- candidate, no-candidate, not-estimable, expired and rejected states are all represented;
- restart cannot duplicate or erase slots;
- missing source evidence remains explicit;
- prospective reports can calculate opportunity frequency and rejection rates without survivorship bias.

Why first:
without a denominator, almost every performance statistic can lie.

---

## P1 — prove or reject S2 after costs before adding strategy complexity

**Value:** highest  
**Authority impact:** research only if baseline unchanged

Relevant files:
- src/atlas/v2/strategies/s2_breakout.py
- tests/v2/test_session016_s2.py
- outcome/scenario/evaluation modules

Required comparisons:
- RAW_RANGE;
- COMPRESSION_ONLY;
- VOLUME_ONLY;
- BASELINE;
- delayed-retest hypothesis only as separate preregistered policy.

Acceptance criteria:
- chronological after-cost expectancy and conservative LCB reported for each variant;
- execution/no-fill/partial-fill included;
- no final-window parameter tuning;
- results segmented by BTC, ETH, liquid alts, long/short and volatility regime;
- no single instrument/month is allowed to dominate the conclusion;
- each added filter must demonstrate incremental value over the simpler policy.

Decision:
if BASELINE does not beat RAW_RANGE/COMPRESSION_ONLY after costs, remove filters instead of tuning them.

---

## P2 — replace turnover as final candidate rank with comparative after-cost ranking

**Value:** extremely high  
**Authority impact:** **requires separate versioned approval/amendment**

Relevant files:
- src/atlas/v2/runtime/production.py
- src/atlas/v2/science/research_selection.py
- src/atlas/v2/science/evaluation_service.py
- src/atlas/v2/science/admission.py
- decision-calendar/outcome contracts

Acceptance criteria:
- all candidates are evaluated using the same cutoff and declared evidence budget;
- candidate economics cannot use future labels;
- comparison statistic is preregistered;
- comparison includes marginal portfolio risk;
- rejected/unsupported candidates remain in the denominator;
- selection is stable under replay/restart;
- no “try next candidate until one passes” hidden shopping loop;
- prospective shadow comparison demonstrates better portfolio-level after-cost outcomes than turnover-first selection.

Design guard:
this must be a single deterministic comparison over a declared candidate set, not sequential rescue attempts.

---

## P3 — simplify M0 before trusting it

**Value:** high  
**Authority impact:** versioned model/feature policy change

Relevant files:
- src/atlas/v2/science/m0.py
- src/atlas/v2/features/
- tests/v2/test_session019_m0.py

Acceptance criteria:
- strategy-specific feature sets;
- materially fewer dimensions;
- same chronology/missingness/OOD safeguards retained;
- feature addition requires incremental chronological performance;
- support floors raised to a level appropriate to feature count and regime diversity;
- calibration error and prediction stability reported by time window;
- model must beat an intercept-only / simple bucketed baseline, not merely produce finite predictions.

---

## P4 — fix S3 data qualification and test stop geometry

**Value:** high  
**Authority impact:** data qualification can proceed without strategy change; stop change requires versioned policy approval

Relevant files:
- src/atlas/v2/runtime/production.py
- src/atlas/v2/strategies/s3_mean_reversion.py
- src/atlas/v2/data/
- tests/v2/test_session021_data_s3.py
- S3 forward/native cadence tests

Acceptance criteria:
- complete trade coverage proven end to end for the exact venue/product;
- no gap in public-trade sequence can be silently interpreted as zero volume;
- UTC-day VWAP reproducible from persisted trades;
- entry-to-VWAP reward and entry-to-stop risk distribution measured at actual valid candidate time;
- after-cost expectancy positive across multiple chronological windows before policy promotion.

If the current stop geometry leaves inadequate reward after valid entry, test a new S3 policy version. Do not silently move the stop.

---

## P5 — qualify five-second decision timing as part of strategy viability

**Value:** high  
**Authority impact:** none

Relevant files:
- S1/S2 policy code;
- production runtime;
- source/capture timing evidence;
- close-driven tests.

Acceptance criteria:
- real final M15 close delivery latency distribution measured;
- BBO/mark freshness measured at decision completion;
- percentage of signals expiring before complete evaluation reported;
- p95/p99 latency remains inside declared decision window with margin;
- missed opportunities are counted as part of policy performance.

If this cannot be achieved consistently, change the policy/deadline in a new version. Do not pretend backtest signals were tradable.

---

## P6 — improve correlated exposure without weakening hard caps

**Value:** medium-high  
**Authority impact:** risk-policy version change

Relevant files:
- src/atlas/v2/risk.py
- src/atlas/domain/risk.py
- portfolio scenario modules

Acceptance criteria:
- instrument-specific beta/covariance estimates are causal and conservative;
- missing/unstable beta cannot increase allowed risk;
- gross notional, stress loss and ES caps remain binding safeguards;
- high-beta alt baskets are not treated as 1× BTC exposure by default;
- stress tests include correlation convergence during liquidation regimes.

---

## P7 — decide whether M1 deserves to exist in the operating workflow

**Value:** medium  
**Authority impact:** none while diagnostic

Relevant file:
- src/atlas/v2/science/m1.py

Acceptance criteria:
- M1 must beat low-dimensional M0 on untouched chronological outer/holdout data;
- compare MAE/calibration **and** economic decision utility;
- improvement must persist after fees/execution and across regimes;
- if not, keep LightGBM offline only or remove it from routine production diagnostics.

---

## P8 — keep AI/memory observational until incremental value is proven

**Value:** medium  
**Authority impact:** any decision influence requires amendment

Relevant files:
- src/atlas/v2/runtime/production_intelligence.py
- src/atlas/v2/research/
- Agent freeze
- proposed S47 amendment

Acceptance criteria for any future influence:
- prospective shadow-only comparison;
- exact deterministic baseline frozen beforehand;
- no access to future outcomes;
- abstention measured;
- incremental after-cost decision value and error modes reported;
- no sizing/stop/order authority unless separately approved.

---

## P9 — capital/protection qualification remains a hard gate

**Value:** existential for live capital  
**Authority impact:** governed qualification, not strategy change

Relevant files:
- src/atlas/runtime/protection_port.py
- src/atlas/runtime/protection_deadline.py
- src/atlas/runtime/protection_evidence.py
- src/atlas/runtime/position_lifecycle.py
- src/atlas/runtime/phase4_v2.py

Acceptance criteria:
- authenticated testnet port proves all required capability rows;
- partial fills update exact stop quantity;
- stop readback proves full-position MarkPrice market semantics;
- unknown submit/reconnect/reconciliation proven;
- emergency flatten proven;
- restart/recovery does not lose protection state;
- no capital upgrade from synthetic fixtures.

---

## 18. Validation design to prove or disprove edge

A credible validation program must reproduce the decisions Atlas could actually make, not idealized trades.

### 18.1 Dataset construction

For every expected decision slot preserve:

- exact instruments eligible at that time;
- source health;
- all strategy candidates;
- scanner rank;
- selection state;
- hard-risk outcome;
- action;
- economic evaluation;
- no-fill/partial/full fill;
- fees;
- funding;
- exit reason;
- final net outcome.

No retroactive universe membership.

No dropping bad-data days after seeing results.

### 18.2 Chronology

Use:

- strictly forward walk;
- purging/embargo at least equal to policy horizon where labels overlap;
- parameters frozen before each evaluation window;
- model/features fit only on information available then;
- final untouched holdout;
- prospective shadow period after offline design is complete.

### 18.3 Costs

Use the exact venue/product fee schedule at the decision time.

Include:
- taker fees;
- spread;
- measured IOC slippage;
- depth/impact;
- stop slippage;
- funding;
- partial/no-fill;
- latency.

Report both gross and net results so cost dependence is visible.

### 18.4 Primary metrics

For each complete policy and portfolio:

- number of expected slots;
- number of valid candidates;
- number selected;
- number economically admitted/rejected/not estimable;
- fill states;
- after-cost net expectancy;
- conservative lower confidence bound;
- profit/loss distribution;
- drawdown;
- stop loss frequency;
- failed-break/time-exit frequency;
- portfolio ES/stress contribution;
- turnover;
- capacity.

Do not choose a strategy by Sharpe alone.

### 18.5 Stability slices

At minimum:

- BTC;
- ETH;
- liquid alts;
- long/short;
- high/low volatility;
- trend/range;
- high/low funding;
- wide/tight spread;
- time of day;
- pre/post major event;
- venue.

A strategy that only works in one narrow retrospective slice is a hypothesis, not an edge.

### 18.6 Ablation

For every filter or model, ask:

> What is the incremental after-cost value versus the immediately simpler complete policy?

Examples:

S1:
- 4H trend only;
- + pullback;
- + reclaim;
- + M15 breakout.

S2:
- raw range;
- + compression;
- + volume;
- + failed-break management.

S3:
- raw VWAP z fade;
- + AR half-life;
- + ADX filter;
- + move-toward trigger.

S6:
- raw cross-sectional momentum;
- BTC residual;
- BTC+ETH residual;
- + 4H trend;
- + M15 trigger.

Delete filters that do not add stable value.

### 18.7 Selection ablation

This is essential.

Compare prospectively:

A. current turnover-first selector;  
B. strategy-specific deterministic opportunity score;  
C. approved comparative after-cost LCB/risk selector.

Evaluate at portfolio level using the **same full opportunity denominator**.

This test will answer whether the biggest current architectural concern is economically material.

### 18.8 Minimum evidence philosophy

I would not define “profitable” from a tiny number of trades.

For management-policy changes, the proposed S48 document already points toward a genuine prospective floor involving weeks and matured opportunities. That proposal is not authority, but its direction is sensible.

For my own capital I would want:
- enough independent matured actions for each active strategy to estimate uncertainty credibly;
- multiple chronological regimes;
- a prospective shadow period;
- no unresolved execution qualification.

The exact numeric promotion threshold should be preregistered before the final evaluation, not chosen after seeing P&L.

---

## 19. Frozen-authority conflict map

| Recommendation | Current authority compatibility |
|---|---|
| improve evidence denominator | compatible if zero-authority/evidence only |
| S1/S2/S3/S6 offline ablation | compatible as research if policy identities stay explicit |
| simplify future M0 feature schema | requires versioned model/feature policy but no capital-authority change |
| prove S3 trade completeness | compatible qualification work |
| measure real five-second latency | compatible qualification work |
| replace turnover-first selection with after-cost comparative selection | **requires separate versioned selection-authority approval/amendment** |
| allow M0/M1/AI to choose among candidates | **requires separate versioned authority approval** |
| edge-weight position size | **requires risk/sizing authority amendment** |
| add partial TP/trailing/new management | **requires new management policy/version; S48 remains proposed** |
| activate live/mainnet capital | **not authorized by this review and blocked by existing qualification requirements** |
| give LLM order/stop/leverage authority | conflicts with Agent freeze unless explicitly superseded by approved authority |

---

## 20. Final strategy ranking from a trader perspective

This is a research priority ranking, **not a profitability ranking**.

### 1. S2 compression breakout — best candidate to prove first

Reason:
clear hypothesis, strong ablation structure, explicit failed-break management, least dependence on questionable model complexity.

Main risk:
confirmed-close chase and stop/horizon asymmetry.

### 2. S1 trend pullback — reasonable second strategy

Reason:
simple, explainable continuation policy with strong causal implementation.

Main risk:
stacked lagging confirmations and wide stop versus four-hour horizon.

### 3. S6 cross-sectional relative strength — interesting research diversifier

Reason:
potential genuine cross-sectional alt alpha distinct from single-instrument technical rules.

Main risk:
crude one-factor residual model, momentum stacking, current selector exclusion.

### 4. S3 VWAP mean reversion — pause until foundational issues are solved

Reason:
good causal/statistical intent.

Main risk:
current trade-completeness blocker and questionable stop/entry geometry.

---

## 21. Bottom line

Atlas currently looks like a **high-integrity research and risk-control platform wrapped around an unproven alpha engine**.

That is a better starting point than the opposite.

The current system should not be criticized for refusing to claim profitability: that restraint is correct.

But the next development priority should not be more features, more AI, more strategies or more model sophistication.

If my own trading capital depended on it, I would do four things:

1. **freeze strategy proliferation;**
2. **prove S2 and then S1 as complete after-cost policies on a complete chronological denominator;**
3. **simplify M0 and execution assumptions until every source of value is interpretable;**
4. **seek a separate versioned approval for one-shot comparative after-cost candidate selection, because turnover-first ranking is currently the weakest link between good market evidence and good capital decisions.**

Until that evidence exists, Atlas can legitimately say it is disciplined, causal and risk-aware.

It cannot legitimately say it has demonstrated a consistent profitable trading edge across BTC, ETH and altcoins.

That conclusion is not a failure of the project. It is the correct evidentiary state.

---

## 22. Evidence index — principal files/functions reviewed

### Strategy and selection

- src/atlas/v2/strategies/s1_trend.py — policy_spec, _regime, setup/trigger coordinator path
- src/atlas/v2/strategies/s2_breakout.py — policy_spec, empirical_quantile, setup/trigger path
- src/atlas/v2/strategies/s3_mean_reversion.py — policy_spec, S3ShadowCoordinator.on_subsequent_bar
- src/atlas/v2/strategies/s6_cross_section.py — policy_spec, cross-sectional score/cohort/action path
- src/atlas/v2/science/research_selection.py — assemble_multisleeve_research_candidate_set
- src/atlas/v2/selection.py — legacy deterministic selection/evidence contracts
- src/atlas/v2/runtime/production.py — actual production composition, scanner rank, selection, hard risk, economics, diagnostics

### Features/data/universe

- src/atlas/v2/features/technical.py
- src/atlas/v2/features/structure.py
- src/atlas/v2/features/pipeline.py
- src/atlas/v2/data/bars.py
- src/atlas/v2/data/universe.py
- src/atlas/v2/runtime/broad_universe.py

### Risk/economics/science

- src/atlas/domain/risk.py — RiskPolicy, ENGINEERING_DEFAULTS
- src/atlas/v2/risk.py — size_selected_candidate
- src/atlas/v2/science/m0.py — action_features, _compatibility, fit_m0, independent support
- src/atlas/v2/science/m1.py — chronology, search, fit_m1
- src/atlas/v2/science/scenario_engine.py — joint path scenario generation/payoff
- src/atlas/v2/science/evaluation_service.py — run_phase2_economic_evaluation
- src/atlas/v2/science/admission.py — conservative admission/LCB
- src/atlas/v2/science/costs.py — fees/funding
- src/atlas/v2/science/outcomes.py
- src/atlas/v2/science/outcome_resolution.py
- src/atlas/v2/science/economic_sources.py / runtime economic source binding where applicable

### Intelligence/lifecycle/protection

- src/atlas/v2/runtime/production_intelligence.py — project_production_receipt
- src/atlas/v2/research/decision_brief.py
- src/atlas/v2/research/memory.py
- src/atlas/runtime/protection_deadline.py
- src/atlas/runtime/protection_evidence.py
- src/atlas/runtime/protection_port.py
- src/atlas/runtime/position_lifecycle.py
- src/atlas/runtime/phase4_v2.py

### Representative tests/evidence reviewed

- tests/v2/test_session014_s1.py
- tests/v2/test_session016_s2.py
- tests/v2/test_session021_data_s3.py
- tests/v2/test_session041_s6_action_contract.py
- tests/v2/test_session041_production_breadth.py
- tests/v2/test_session019_m0.py
- tests/v2/test_session023_m1.py
- tests/v2/test_session036_economic_chronology.py
- tests/v2/test_session052_production_intelligence.py
- tests/v2/test_session053_production_completion.py
- runtime protection/lifecycle tests
- docs/v2/ATLAS_RED_R3A_COMPLETION_HANDOFF.md
- docs/v2/ATLAS_RED_R3A_COMPLETION_EVIDENCE_MANIFEST.json
- log.md

---

**Independent review conclusion:** **NOT CAPITAL READY; ALPHA NOT PROVEN; ARCHITECTURAL CONTROL QUALITY HIGH; OPPORTUNITY-SELECTION QUALITY REQUIRES MATERIAL IMPROVEMENT AND REALISTIC CHRONOLOGICAL AFTER-COST PROOF.**
