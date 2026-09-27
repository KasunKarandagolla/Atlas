# Session 023 — Phase-3 engineering closeout

## Checkpoint and review

- Required starting SHA: `09efb89ff99e156fb39c7c0a89c5a86f639ccf47` on accepted `impl/session-022-v2-s4-s5-microstructure-crowding-context`.
- Branch: `impl/session-023-v2-m1-analogue-discovery-selection-gate`.
- Final implementation SHA: `445a6c2e33e0808533d3b38e8f7613eb6e59c17f`. This contains the tested implementation, immutable research evidence, validation manifest and engineering gate.
- Final pushed closeout SHA: the branch-tip commit containing this handoff; its exact SHA is supplied in the Session-023 closeout response and can be resolved with `git rev-parse origin/impl/session-023-v2-m1-analogue-discovery-selection-gate`. The handoff commit adds documentation to the implementation checkpoint above.
- Starting ancestry and clean tracked state were verified before edits. Both authoritative freeze documents were read in full, as were Session-020/021/022 handoffs. Current M0/evaluator/outcome/scenario, CandidateSet/selection/action/risk, S1–S7, Model Arena/adapters, dependency lock and golden contracts were inspected. S8 was absent at the starting checkpoint.
- The pre-existing untracked amended V2 freeze and `atlas-session005.zip` remain untouched and untracked. No unrelated implementation was changed.
- The coordinating ChatGPT must independently inspect the actual pushed SHA, diff, files, handoff and tests before authorizing Session 024. Session 024 has not started.

## Implemented research contracts

All new components have zero capital authority. Decision ordering remains candidate generation → persist all candidates → deterministic selection → hard-risk sizing → freeze exact action → economic evaluation. M0 admission, action freezing, sizing, risk limits, approval/reservation authority, live transports and existing model adapters are unchanged. There is no averaging, voting, action rescue or automatic promotion.

### M1

- `M1_LIGHTGBM_ACTION_VALUE_V1`, version `1.0.0-offline-research`; policy hash `7ec4703f20ec9e3e2bf58c5a785f1ef179a22c3f8382b1c059679ee6306bc57f`.
- Feature policy hash `b608c93873df82168c4d922b5d18b976efdc94a07c84159d75d1258aaa428f28`. The declared 46-column vector contains paired values/missingness for a small subset of causal M0 technical, candle, regime and frozen-action evidence. Session-021/022 context is not automatically added.
- Native LightGBM CPU training uses the actual package, not a substitute. Objective `regression_l1`, validation MAE, seed **23017**, **one thread**, deterministic mode and forced column processing. Four fixed configurations: leaves 7/15 × 48/96 boosting rounds, learning rate 0.05, minimum child samples 5. Search results retain failed configurations; selection uses only inner validation.
- Immutable versioned feature/training/OOF/model-fit/prediction/calibration/support/OOD/incremental-comparison contracts use standard content hashes and repository indexing. Predictions bind exact action/candidate/set/cutoff, source features, model fit, training, OOF, calibration, support, OOD, availability and compatibility.
- Honest training resolves existing executable-action-value eligibility and source validation. It retains actual/simulated/counterfactual provenance, no-fill/partial/full fill, requested/filled quantity, gross value, fees, funding and the execution evidence that binds latency/execution assumptions. Current actions and future/unmatured labels are excluded.
- Chronology: 180-day training, 30-day inner validation, 30-day outer test, three outer windows with 30-day advances, then a final 30-day holdout. Overlapping labels are purged; embargo is at least 24 hours and never below the maximum relevant holding horizon. Scaling, bounded search and calibration use training chronology only. No random K-fold API exists.
- The first final-holdout reservation is immutable for the model family/compatible population. Later queries cannot roll it into training. A viewed reservation is globally SPENT; reuse is rejected. Native synthetic outer-fold tests prove engineering behavior, not genuine historical chronology.
- No qualified genuine action-value history was available. Current research predictions, calibration and incremental economics are `NOT ESTIMABLE`.

### Causal analogues

- `CAUSAL_ANALOGUE_ACTION_VALUE_V1`, policy hash `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`. Numerical retrieval only; no vector database.
- Compatibility precedes distance: policy/action semantics, side, exact holding horizon, venue/product, execution mode, quantity/participation representation, liquidity, feature schema/availability and cost semantics. Repository source loading revalidates the honest matured label, frozen action, original features and execution provenance.
- Decision and label availability must precede the query cutoff; overlapping targets are embargoed. Median/MAD scaling uses compatible prior rows only. Deterministic distances/weights, population/scaler refs, neighbours/payoffs, missingness, dispersion, effective support, independent components, temporal concentration, regime coverage, OOD and explanations are retained.
- Overlapping labels and shared episodes form dependence components. Both independent components and effective episode-weight support require at least 20; concentrated or unsupported neighbourhoods return `NOT ESTIMABLE`.
- Analogue estimates are research support/OOD/explanation evidence. Shared scenario neighbours are explicitly not another independent vote.

### Additive selection and sleeve boundary

- `MULTI_SLEEVE_RESEARCH_SELECTION_V1`, version `1.0.0-research`, hash `37355c64d45a67bdce3a271a63db377b953c05847561bcda33847c27cecb0dac`.
- The accepted `S1_S2_SCANNER_RANK_V1` is untouched. A separately hashed research universe/set/index preserves original S1/S2 records and has no capital activation.
- Ordering is accepted scanner rank ascending → policy ID → canonical InstrumentKeyV2 → candidate ID. Source references are cutoff checked; nested M0/M1/analogue/outcome inputs are rejected. All valid exact-action competitors remain recorded, including ineligible/unsupported members. Unknown competitors prevent an unjustified selection.
- S1/S2-only canonical bytes/hash and selected candidate reproduce the accepted baseline. S3 enters only with the accepted policy hash and complete valid single-action stop/horizon/management contract.

| Sleeve | Normal research CandidateSet contract | Reason/boundary |
| --- | --- | --- |
| S1 | Complete exact action | Accepted four-hour policy |
| S2 | Complete exact action | Accepted two-hour policy |
| S3 | Complete shadow exact action | Accepted one-hour policy; input/rank eligibility still applies |
| S4 | Excluded | `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; context/absorption hypothesis only |
| S5 | Excluded | `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; crowding/directional hypotheses only |
| S6 | Excluded | `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; stop and action horizon not frozen |
| S7 | Excluded | `NOT_ESTIMABLE_EXACT_ACTION_CONTRACT`; reaction/watch hypothesis lacks complete action |
| S8 | Excluded | `RESEARCH_BASKET_ONLY_NO_SINGLE_ACTION_CONTRACT` |

The explicit `ResearchSleeveSelectionAuditV2` is persisted by the integration/report evidence. No absent action was fabricated.

### S8 basket closure

- `S8_HOURLY_PAIRS_RESEARCH_V1`; `ResearchBasketForecastV2` profile hash `c118b1e3f6732cf8897efc4c08a2d21eef50cec63b78d5bb40787f9fa5b0f7fe`.
- Explicit economic pair; same venue/environment/product/settlement semantics; synchronized 721 hourly closes spanning 30 days. Fit/query availability is cutoff causal. Hourly decision; entry `|z| > 2`, convergence `|z| < 0.5`, stop `|z| > 3.5`, four-hour time exit. Alpha/beta/standardization remain frozen through the replay.
- Both-leg price, available book/execution, fee/funding references, partial-fill, sequential-delay and orphan-leg states/amounts are represented. Historical L2 is not fabricated. Missing both-leg execution or matured basket outcomes yields `NOT ESTIMABLE`.
- No normal CandidateAction, TradePlan, sizing or live order API is emitted. Tests prove rejection at the single-action boundary.

### Discovery, multiplicity, calendar and ablation

- `BOUNDED_DISCOVERY_LAB_V2_V2`, contract hash `9270c622e372dc54bb90ce15d5cc7ba94da6c1d18fa462c40e0f46fa54cae7b1`. Preregistered schema-constrained offline operations, feature/availability assumptions, finite attempt/parameter budget, baseline/metrics, chronology/purge/embargo, multiplicity family, stop rule, holdout identity and prospective requirement.
- Append-only attempts retain specifications, source/proposer, parameters, times, evidence refs, results, failures, manual intervention and holdout views. Budget/future-label/spent-holdout rejections are retained. A spent holdout cannot reset through a renamed experiment; redesign requires fresh future evidence.
- The durable design registers **17 proposals**: four M1 configurations, analogue, research selector, S8, and ten feature ablations. All missing-evidence results and the required M0 baseline remain in an **18-member** family. No economic search or final-holdout evaluation occurred. No AI/LLM provider was used; optional proposal flags prohibit credentials, order tools, risk mutation, capital and self-promotion.
- `DEPENDENCE_BLOCK_HOLM_AUDIT_V2_V1`: synchronized contiguous decision-time blocks covering the maximum policy horizon, paired whole-policy net values, centered-null block bootstrap, then Holm step-down FWER at alpha 0.05. Holm accommodates dependence between family tests; block resampling preserves within-block time dependence under the declared block qualification. At least three genuine outer windows and 20 independent blocks/support are required. Monte Carlo repetitions never add independent market support. This family is `NOT ESTIMABLE`; no economic pass is inferred.
- `WHOLE_CALENDAR_SELECTION_AUDIT_V2_V1` retains selected/unselected/rejected/no-candidate/expired/NO_TRADE/NOT_ESTIMABLE states and qualified actual/simulated/counterfactual no/partial/full-fill outcomes. It resolves members through each immutable CandidateSet's exact references; future same-ID revisions cannot replace earlier members. It never invents an unselected fill. Calendar coverage is separate from value coverage/missed-value share/lift; unsupported values/uncertainty remain `NOT ESTIMABLE`. IPW requires a cutoff-known preregistered exploration design with actual inclusion probabilities.
- `WHOLE_POLICY_FEATURE_ABLATION_V2_V1` declares candles, SMC/structure, support/resistance, Fibonacci, measurable Elliott morphology, measurable Wyckoff challengers, ordinary time/calendar, killzone/time-window challengers, derivatives/crowding and S4 flow. Every design preserves scanner, generation, selection, sizing, execution/costs, no/partial-fill, latency and gates. No adequate matured whole-policy history exists; all economic ablations remain `NOT ESTIMABLE`.

## Durable artifacts and gate

- `docs/v2/SESSION023_RESEARCH_DESIGN.json`: preregistration, complete attempted family/failure ledger, multiplicity result, ablation design and untouched future holdout identity.
- `docs/v2/SESSION023_ENGINEERING_EVIDENCE.json`: explicitly synthetic immutable same-action comparison, whole-calendar audit and sleeve exclusions from the deterministic integration fixture.
- `docs/v2/SESSION023_VALIDATION.json`: actual test cases/counts, source hashes, commands, environment, frozen identities and credential scan. Validation content hash `65cbb0e8dbf9ce5c81e94bee6e22cb52630e530542c53b9b17007942cf7f53ac`.
- `docs/v2/PHASE3_ENGINEERING_GATE.json`: `PHASE3_ENGINEERING_GATE_V2_V1`, artifact hash `a1b95b5e2cd3a6e9329b1eaa5ad9c70bc28fbc69d8eebb6e031a8973b3d7afb2`; **Phase-3 engineering `TESTED`**. Every required engineering check is true and linked to passing test evidence in the validation manifest.
- Separate states: economic value **`NOT ESTIMABLE`**; live feed qualification **`UNVERIFIED / TEST GATE`**; capital **disabled**.
- Formal promotion states remain `INTEGRATED`. Engineering evidence is `TESTED`; no automatic ladder advancement occurred. The exact six-stage ladder is validated one stage at a time with review evidence. No `PROSPECTIVE_SHADOW`, `INCREMENTAL_VALUE_PASS` or `DECISION_ELIGIBLE` claim is made.

## Dependencies and Tier C

- Added only optional `offline-research = ["lightgbm==4.7.0"]`. The live writer does not require LightGBM; a guarded fresh process imports writer, coordinator, SafeRuntime and CLI plus research contracts with LightGBM/NumPy/SciPy imports unavailable.
- Actual environment: Python **3.12.13**, `Linux-5.15.0-177-generic-x86_64-with-glibc2.35`; LightGBM **4.7.0**, NumPy **2.5.3**, SciPy **1.18.1**, narwhals **2.26.0**.
- Complete lock diff inspected: LightGBM plus required narwhals/SciPy; existing dependency versions unchanged. Hash-locked install and dry-run validation passed. Lock SHA-256 `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`. Only `dependency_lock.sha256` changed in the V1 golden metadata; all V1 behavior hashes are preserved.
- Complete V2: **389 passed**, including **87 Session-023**, **47 Session-022**, **35 Session-021**, and **19 Session-020** cases, M0/evaluator/scenario/outcome, model ABI/deadline/isolation, selection/action/risk and desktop/IPC seam coverage.
- Complete V1: **414 passed, 1 existing skipped**. V1 golden recomputation and accepted-checkpoint metadata comparison passed.
- Ruff, mypy (**183 checked files**, all source plus focused new tests), compileall, working/staged `git diff --check`, pip check and hash-locked dependency validation passed. The lock dry-run checked **33 packages**, proposed **zero changes**; installed-package compatibility checked **34 packages**.
- Tracked credential scan: **349 implementation/report paths**, zero high-confidence hits; matched values were not printed. It checked private-key/AWS/GitHub/Slack patterns plus high-entropy credential assignments. Dedicated gitleaks/trufflehog/detect-secrets binaries were unavailable. The final documentation-inclusive scan is recorded in closeout; no GitHub CI result is claimed.
- Integration ends honestly in `NOT ESTIMABLE`, with a simulated no-fill outcome; no artificial TRADE is forced. S4/S5 attachments leave sizing/action unchanged; S6/S7 evidence remains visible; S8 stays outside single-action execution.

## Preserved identities

- MATRIX: `294b47506e8a7b2a73275a70e13494acd37c4f1216df53e422f0ad863f848b81`.
- S1: `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`.
- S1_S2_SELECTOR: `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`.
- S2: `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`.
- S3: `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`.
- S4: `327c816792290ceaf64ef6dd6b6e90e0382c04782e1dc7d9ca5895ce7f2ae146`.
- S6: `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`.

## Changed files

- `docs/v2/PHASE3_ENGINEERING_GATE.json`
- `docs/v2/SESSION023_ENGINEERING_EVIDENCE.json`
- `docs/v2/SESSION023_RESEARCH_DESIGN.json`
- `docs/v2/SESSION023_VALIDATION.json`
- `docs/v2/V1_GOLDEN_BASELINE.json`
- `pyproject.toml`
- `requirements-lock.txt`
- `src/atlas/v2/science/analogue.py`
- `src/atlas/v2/science/audits.py`
- `src/atlas/v2/science/discovery.py`
- `src/atlas/v2/science/m1.py`
- `src/atlas/v2/science/phase3.py`
- `src/atlas/v2/science/research_selection.py`
- `src/atlas/v2/science/session023_report.py`
- `src/atlas/v2/strategies/s8_pairs.py`
- `tests/v2/session023_support.py`
- `tests/v2/test_session023_analogue.py`
- `tests/v2/test_session023_discovery_s8.py`
- `tests/v2/test_session023_integration.py`
- `tests/v2/test_session023_m1.py`
- `tests/v2/test_session023_selection_audits.py`
- `docs/handoffs/session-023.md`

## External limitations and remaining Session-024 work

No environment blocker prevented native LightGBM implementation or local validation. Genuine matured chronological whole-policy evidence, prospective shadow evidence, incremental after-cost value, operational/compute qualification and real both-leg S8 execution evidence remain unavailable or unverified. Synthetic fixtures supply engineering evidence only.

After independent review and explicit authorization, Session 024 owns Phase-4 venue qualification, the capital bridge and failure/recovery hardening: authenticated venue/account/product/runtime qualification; actual fees/funding/fill/latency/protection and source-health evidence; controlled single-venue bridge to unchanged approval/reservation/hard-risk authority; and UNKNOWN, recovery, reconciliation and failure handling. Unqualified models/sleeves retain zero capital influence. This session introduces no live S8 multi-leg authority, new risk limits, live tuning/self-learning, automatic promotion or simultaneous multi-venue capital.

Capital remains disabled. Session 024 has not started.

## Final Session-023 Holdout-Population Remediation

This final narrow remediation started from required SHA `9a65ec9ebfe423a7bb26d5e508cbdfc949f09644` on branch `impl/session-023-v2-m1-analogue-discovery-selection-gate`. It closes holdout membership inference for raw matured outcomes. No Session-024 or Phase-4 venue/capital work was started.

### Immutable holdout population and evidence-derived membership

- Discovery now requires a versioned `DiscoveryHoldoutPopulationV2` reference bound to the experiment and family, chronology, preregistration time, decision-calendar population, venue/product/policy universe, decision/event identity rules, assignment rule, and immutable source-evidence revision when assigned. Population versions are append-only; assigned or spent intervals cannot be moved or replaced under the same identity.
- The Session-023 final population retains rule `FINAL_30D_AFTER_THREE_REQUIRED_OUTER_WINDOWS_V1` as `NOT_ESTIMABLE`, with no fabricated start/end dates or source revision. Its exact contract reference is `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`.
- Once a genuine assignment is available, membership is derived from the validated matured outcome’s immutable decision chronology and bound identity/scope. The interval is half-open: `final_holdout_start_ns <= decision_at_ns < final_holdout_end_ns`. Wrapper omission or later outcome-availability revisions cannot alter classification. While the population is unassigned, evaluation returns `NOT_ESTIMABLE_FINAL_HOLDOUT_POPULATION_UNASSIGNED` instead of guessing.
- Raw or wrapped final-holdout evidence is rejected from training and validation. Outer use requires an explicit view bound to the exact population version and evidence refs; the attempt and view are retained and the population becomes `SPENT`. Experiment/family renames and omitted holdout metadata do not reset spent state; a second view fails, and redesign requires genuinely later evidence. Rejected attempts remain in the durable ledger.
- The final discovery-lab policy hash is `846fd322e5d0a8d91a4ae659771f7e37f26bcff384508a5895f154a93b0cfac3`. The accepted analogue policy identity remains `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`.

### Adversarial tests and validation

- New tests cover raw final-holdout outcomes in training, validation, and outer refs; explicit versus omitted holdout views; wrapper behavior; the immediately preceding decision and exact start boundary; immutable classification across availability revisions; immutable population versions; unassigned named `NOT ESTIMABLE`; durable rejection; experiment/family renaming and omitted metadata; and redesign requiring fresh future evidence.
- Complete `tests/v2`: **392 passed**. Session-023: **90 passed**; Session-022: **47 passed**; Session-021: **35 passed**; Session-020: **19 passed**. The standalone Session-020 E2E/IPC seam passed **6/6** with the required socket permission. Complete V1: **415 collected, 414 passed, 1 existing skip**. The separate V1 golden recomputation/comparison passed.
- Ruff, mypy (**183 source files**), compileall, `git diff --check`, pip check (**34 compatible packages**), hash-locked dependency validation (**33 packages, zero changes**), frozen identity checks, and tracked-file credential scan (**349 paths, zero high-confidence matches**) passed. No GitHub workflow run is claimed.
- The regenerated validation manifest hash is `159d0190c67994d40fdfe603d86c9d28d33256cb85b7b45ea563886bd4d4b166`; the new Phase-3 gate hash is `f05ce110367b4f6d6b22e6c8231e4b22fb71d9906f4899c83d4bd035ae8ef59f`, referencing that manifest. Phase-3 engineering remains `TESTED`; economics remain `NOT ESTIMABLE`; live venue/source qualification remains `UNVERIFIED / TEST GATE`; capital remains disabled. Session 024 remains unstarted pending the coordinating ChatGPT’s independent inspection of the pushed SHA.

### Preserved Session-022 identities

- S1: `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`.
- S2: `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`.
- S3: `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`.
- S4: `327c816792290ceaf64ef6dd6b6e90e0382c04782e1dc7d9ca5895ce7f2ae146`.
- S6: `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`.
- S1/S2 selector: `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`.
- Evidence matrix: `294b47506e8a7b2a73275a70e13494acd37c4f1216df53e422f0ad863f848b81`.

### Files changed by this final remediation

- `docs/handoffs/session-023.md`
- `docs/v2/PHASE3_ENGINEERING_GATE.json`
- `docs/v2/SESSION023_RESEARCH_DESIGN.json`
- `docs/v2/SESSION023_VALIDATION.json`
- `src/atlas/v2/science/discovery.py`
- `src/atlas/v2/science/phase3.py`
- `src/atlas/v2/science/session023_report.py`
- `tests/v2/test_session023_discovery_s8.py`
- `tests/v2/test_session023_m1.py`

## Final Session-023 Analogue & Discovery Enforcement Remediation

This closeout records the narrow remediation on branch `impl/session-023-v2-m1-analogue-discovery-selection-gate`, starting from required SHA `6eccab03f7b975019c145839bd63c058fa7b0e95`. The final analogue/discovery identities, gate hashes, and validation totals above supersede the earlier Session-023 values in this handoff.

### Analogue compatibility evidence

- The repository builder derives compatibility from the indexed frozen `ActionArtifactV2`, full `InstrumentKeyV2`, exact causal `FeatureArtifactV2`, versioned cutoff-known S4 sequence-valid book evidence and archived raw-frame lineage, product and quantity contract, and the action's typed cost contract, fee/funding schedules, and replay assumptions.
- Action execution semantics bind the frozen order/stop/management/horizon semantics and replay assumptions. Outcome provenance remains a separate `ACTUAL`, `SIMULATED`, or `COUNTERFACTUAL` field. Compatibility is re-derived before query construction; caller-authored matching labels cannot establish compatibility.
- Liquidity, participation, feature availability, and cost inputs missing at the decision cutoff return named `NOT ESTIMABLE` errors. Later revisions leave the earlier key/query unchanged. The 20-independent-episode and effective-support floors remain in force.
- The analogue policy hash is `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`.
- Adversarial coverage exercises caller tampering of execution, quantity/participation, liquidity, cost, feature availability, action semantics, horizon, venue, and product; it also checks deterministic independent fixture reproduction, missing evidence, later revisions, future tails, dependence, and effective support.

### Discovery chronology and holdout evidence

- Action-value evaluation resolves typed, indexed `MaturedOutcomeV2` artifacts through the existing maturity and executable-action eligibility contract. Feature artifacts, unmatured labels, and synthetic/fixture evidence cannot act as historical labels. S8 economic discovery remains `NOT ESTIMABLE` until an honest matured two-leg contract exists.
- Any evaluation refs require a deterministic typed chronological split specification. The builder validates split membership, label maturity/availability, boundary crossing, overlap purge, maximum policy horizon, preregistered chronology/purge agreement, and minimum embargo.
- Holdout use is derived recursively from immutable evaluation lineage. Training and validation reject holdout-linked evidence. An outer holdout view requires an explicit declaration, stores the exact attempt and evidence refs, spends the reservation, and prevents reuse or experiment-renaming resets. Redesign after spending requires genuinely later evidence. Rejected attempts remain in the durable ledger.
- The discovery lab contract hash is `9270c622e372dc54bb90ce15d5cc7ba94da6c1d18fa462c40e0f46fa54cae7b1`.

### Final validation and gate

- The complete V2 suite passed **389/389**; the complete V1 suite passed **414/415** with **one existing skip**. The V1 golden recomputation/comparison passed separately. Session totals are Session-023 **87**, Session-022 **47**, Session-021 **35**, and Session-020 **19**.
- Ruff, mypy (**183 checked files**), compileall, `git diff --check`, pip check, hash-locked dependency validation (**33 locked packages, zero proposed changes**), frozen identity checks, and the tracked-file credential scan (**349 paths, zero high-confidence matches**) passed.
- Validation manifest hash: `65cbb0e8dbf9ce5c81e94bee6e22cb52630e530542c53b9b17007942cf7f53ac`. The regenerated Phase-3 gate hash is `a1b95b5e2cd3a6e9329b1eaa5ad9c70bc28fbc69d8eebb6e031a8973b3d7afb2`; its validation reference includes the new analogue and discovery adversarial node IDs.
- Preserved Session-022 identities remain unchanged: S1 `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`; S2 `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`; S3 `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`; S4 `327c816792290ceaf64ef6dd6b6e90e0382c04782e1dc7d9ca5895ce7f2ae146`; S6 `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`; S1/S2 selector `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`; evidence matrix `294b47506e8a7b2a73275a70e13494acd37c4f1216df53e422f0ad863f848b81`.
- Economics remain `NOT ESTIMABLE`; live venue/source qualification remains `UNVERIFIED / TEST GATE`; capital remains disabled. No GitHub workflow run is claimed. Session 024 has not started and still requires the coordinating ChatGPT's independent inspection of the pushed SHA.

### Files changed by this remediation

- `docs/handoffs/session-023.md`
- `docs/v2/PHASE3_ENGINEERING_GATE.json`
- `docs/v2/SESSION023_RESEARCH_DESIGN.json`
- `docs/v2/SESSION023_VALIDATION.json`
- `src/atlas/v2/data/microstructure.py`
- `src/atlas/v2/science/analogue.py`
- `src/atlas/v2/science/costs.py`
- `src/atlas/v2/science/discovery.py`
- `tests/v2/session023_support.py`
- `tests/v2/test_session018_remediation.py`
- `tests/v2/test_session023_analogue.py`
- `tests/v2/test_session023_discovery_s8.py`
- `tests/v2/test_session023_integration.py`
- `tests/v2/test_session023_m1.py`
