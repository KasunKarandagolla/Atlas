# P1 financial-evidence correction — Phase A review candidate

**IMPLEMENTED / TESTED OFFLINE; INDEPENDENT EXACT-SHA ACCEPTANCE PENDING. Capital NO-GO.** This corrects the two owner-supplied independent-review findings. It does not self-accept P1 or authorize P2.

## Parent, authority and authorization

Parent: reviewed but **unaccepted** P1 candidate [`8fc2109cda6fea6a8bea0f07d8e43999e9cee1c9`](https://github.com/KasunKarandagolla/Atlas/commit/8fc2109cda6fea6a8bea0f07d8e43999e9cee1c9). Correction branch: `fix/atlas-red-p1-financial-evidence-2026-10-16`. Refreshed all remote heads; no newer P1 correction, independent P1 acceptance or P2 branch was found. Verified ancestry remains R9 `96fc24a…` → accepted design `f9f8ef083ba5bafcad9bab4d07227676200a948f` → governance `3cb4f052…` → P1 parent → correction. The default historical branch was not used. Working tree was clean before the isolated branch was created.

The owner's separate instruction authorizes “P1 GPT-6.1 Sol offline correction of findings A and B, including tests, evidence/documentation updates, commit and push.” Model selection remains **owner-confirmed GPT-6.1 Sol**; independent authoritative runtime attestation is unavailable. No subagents were used. P2 requires independent acceptance of the exact corrective remote SHA and a separate owner authorization; neither is supplied by this report.

Accepted design, original policies, private-source availability limits and historical freezes retain their prior authority. No private owner/ADR originals were published or represented as newly available. All three historical freeze byte identities and canonical design/PDF/manual/proposed policy, lock and dependency declarations match the parent. Root instructions and the earlier full source reading remain applicable; source and governance pointers were refreshed for this correction.

## A — Verified selected-policy OOF calibration

The old `SelectedPolicyResidualV1` and `MeanSelectionCalibrationV1` codecs are unchanged. Previously saved `SUPPORTED_OFFLINE` records remain readable as historical records. The old declaration-only `calibrate_selected_policy` now returns NOT ESTIMABLE with no allowance; a boolean cannot construct supported new-policy calibration. The evaluator and B consumer require V2 evidence.

`science/mean_calibration.py` adds the following strict immutable contracts on the existing `OpsRepository`:

| Contract / version suffix | Evidence meaning |
| --- | --- |
| `MeanEvidenceScopeV1` / `MEAN_EVIDENCE_SCOPE_V1` | Exact product, policy, action/size/liquidity compatibility cell, fixed feature revision, 24h target horizon, numerical support envelope, registration/expiry, provenance and explicit evidence grade |
| `MeanOriginEvidenceV1` / `MEAN_HISTORICAL_ORIGIN_EVIDENCE_V1` | Original synthetic action identifier and immutable origin/features available by that origin; contains no future target. Its content hash is the historical action reference in this replay evidence chain |
| `MeanOutcomeEvidenceV1` / `MEAN_OUTCOME_EVIDENCE_V1` | Binds the original origin/action to its mature common-24h cash target, features and fill state; original label availability is independent of prediction availability |
| `MeanCalibrationProtocolV1` / `OOF_CALIBRATION_PROTOCOL_V1` | Predeclared calendar window, block/embargo, confidence/resampling, fold estimator/ridge and protected first holdout |
| `MeanOOFPredictionV1` / `VERIFIABLE_OOF_PREDICTION_V1` | Actual original prediction, origin/action/features, exact historical fold model and immutable ordered training-row membership; no future target in the prediction artifact |
| `SelectedPolicyResidualV2` / `SELECTED_POLICY_MEAN_RESIDUAL_V2` | Explicit prediction receipt → separately mature target-row link |
| `MeanDependenceQualificationV1` / `OOF_DEPENDENCE_QUALIFICATION_V1` | Exact protocol, final serving model, residual membership, original availability/expiry, derived effective blocks/lag diagnostics and offline computed allowance |
| `MeanSelectionCalibrationV2` / `SELECTED_POLICY_MEAN_CALIBRATION_V2` | Unchanged V1 cash/scope fields plus mandatory qualification reference; distinct wire version |

All version strings have the `ATLAS_RED_` prefix. Exact ordered fields/defaults/base classes are in the [new interface inventory](transition-p1-financial-evidence/interface-inventory.json).

`produce_verified_mean_oof` connects a recording callback to the existing `chronological_mean_oof` loop. It persists the actual M0 fold fits, eligible input rows, immutable prediction receipts and mature residual links. It does not implement a second training pipeline. Each fold's original model is distinct from the final serving model, which may legitimately be fitted after all historical calibration origins.

`verify_oof_residuals` resolves and hashes the full chain, including the pre-origin feature record, target, all training outcomes/rows and model. It checks mature cutoffs, original availability, no own target, 24h embargo/overlap, scope/feature/provenance, original compact prediction equality, unique membership and protected holdout exclusion. The first protected reservation marker is checked, so replacing a holdout object does not reset it. `verify_serving_training` independently checks final serving-model training membership and maturity; it does not confuse it with fold membership.

`calibrate_verified_selected_policy` performs the offline complete-calendar-group bootstrap. Every sampled block contributes its error sum **and observation count**; division uses sampled total action count. A mean of unweighted block means is not substituted. The allowance is nonnegative and no lower than verified per-action mean error. `verify_mean_calibration` checks all evidence, final model/cell/estimator identity, derived effective support, expiry, exact calibration/proof allowance agreement and that necessary mean-error floor. Offline recomputation in the capsule reproducer independently reproduces the full registered bootstrap output and hash. No fitting, OOF generation or bootstrap executes on admission or B consumption.

The current proof producer uses the existing M0 ridge OOF loop. A serving M1 cannot borrow M0 calibration: its independent matching-family OOF evidence is required before new-policy eligibility. Existing real L2 M1 fitting, compact inference and research comparisons are unchanged; absence of that matching calibration is a refusal, not a favorable allowance.

Support is an explicitly **SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT** over CAUSAL_REPLAY inputs. Independent synthetic calendar-block assumptions are declared in the protocol, not asserted by an admission boolean. At least 20 effective blocks and the implemented lag-1–3 diagnostics are checked; positive serial dependence reduces effective count, and absolute lag correlation above 0.2 refuses. These diagnostic rules do **not** prove actual market independence. Genuine reconciled/shadow source qualification needs separately reviewed source adapters/evidence; these synthetic origin/outcome records cannot be relabelled as actual fills or genuine prospective history. Existing whole-programme diagnostic functions are unchanged and cannot supply this action-calibration proof.

## B — Verified scenario/execution/probability/ES evidence

`MeanScenarioSetV1` and the existing replay/cash/ES arithmetic retain their saved meanings. V1 flags alone cannot grant new eligibility. `science/mean_scenario_evidence.py` adds eight contracts:

| Contract | Required evidence |
| --- | --- |
| `ScenarioMarketEvidenceV1` | Full indexed historical quotes/quantities/marks and optional signed funding; actual observation/publication clocks |
| `ScenarioObservationV1` | Mature empirical category, execution delays and cash-loss observation in the same qualified scope |
| `ScenarioSourceV1` | Original inputs and exact historical templates, source method/expiry, current account, stress paths, common background references |
| `ScenarioProtocolV1` | Predeclared train/validation periods, 24h embargo, confidence, block/tail/error requirements, holdout and synthetic dependence assumption |
| `ScenarioExecutionModelV1` | Indexed mature execution observations supporting the exact delay envelope and scope |
| `ScenarioProbabilityCalibrationV1` | Immutable disjoint mature training/validation membership and exact path population/empirical weights |
| `ScenarioRiskSupportV1` | Whole scenario binding, current account/risk policy, confidence, effective blocks/tail count, verified validation ES and supported loss envelope |
| `MeanScenarioSetV2` | Unchanged V1 fields plus mandatory qualification reference and new exact version |

`verify_scenario_evidence` independently resolves source, execution, probability and risk records before economic use. It checks hash/body/type identity, original publication and expiry, product/policy/features/cell/provenance, exact 24h horizon, path IDs, weights, delay support, current account/risk/stress and background bindings. Empirical probabilities are recomputed from actual indexed category counts; the last Decimal mass accounts for division rounding. Validation is separate, mature, embargoed and outside the protected holdout. Effective calendar blocks are derived, and tail support uses actual validation observations rather than simulated path or resample count.

The supported source method shifts immutable historical market/funding templates by a declared origin offset. Future simulation timestamps are permitted; each underlying generation input must already have been observed and published. Simulated values must match the bound historical template. A future realised quote cannot pass as a pre-origin input.

Common background paths reuse `ExistingPortfolioPathV2` and its existing index owner. Flat snapshots require zero background PnL. Nonempty snapshots require complete matching component identities, common path/horizon/account/scope, indexed supporting observations and matching signed cash payoff. Changing both a claimed background profit and its portfolio projection cannot override the bound source observation. This is offline projection mathematics, not account reconciliation or a new OMS.

`run_mean_action_evaluation` now passes the existing repository into `_evaluate_one`, which verifies both chains before conservative scoring and scenario ES. Exact computed common-path loss is also checked against the qualified loss envelope. Mandatory source/execution checks remain active when `require_es=False`. `validate_offline_b_selection` repeats evidence verification at consumption, including expiry and current material-evaluation bindings. Missing/revised/stale/unsupported evidence is UNAVAILABLE or a refusal, never economic zero. No fit/search/bootstrap/provider request is inserted into the deadline route.

## Positive/negative proof and compatibility

[verification.json](transition-p1-financial-evidence/verification.json) gives actual commands, observed counts, source timing and retained gates. **Measured:** Broad affected group 1,200 pass/1 skip; final complete P1 group 179 pass, including all A/B and seven common-background/serving-training cases. Latest deduplicated coverage 1207 pass/1 skip, not a full repository suite. Final P1 follows the last serving-membership/background hardening; broad legacy coverage was observed before those isolated new-path guards. Ruff/compile/pip/lock checks pass; mypy retains exactly 53 errors in 17 files with no normalized additions. Final source build is `581c44bfd2ee7e9322d890c828d21cb2d7420ea4a381ffb6a64d665af388443c` (37 code/test/script files). [Evidence manifest](transition-p1-financial-evidence/evidence-manifest.json) gives every artifact SHA256. [Source build](transition-p1-financial-evidence/source-build.json) hashes the corrected source; the old `81a268d4…` identity is not reused.

- **Red:** both new tests fail on `8fc2109c`: 30 declaration-only zero residuals obtain support; missing-source boolean scenarios enter E. Original red text/XML are retained.
- **A-NEG-01:** 30 actual in-sample fold fits containing their own target labels, with zero residuals, cannot produce supported calibration or enter E. A-NEG-02–06 cover future model/prediction, own/overlapping membership, missing/invented inputs, altered original cash/features/cell, protected holdout and unsupported/stale/future/mismatched dependence. Final serving-model invented/future membership is tested separately.
- **A-POS-01:** actual earlier mature M0 fits generate 30 unique fold predictions with correct embargo; the final later serving model binds the verified calibration. A real nonzero residual case produces an allowance of 1; a mutually rehashed zero allowance is refused. Unequal block counts retain per-selected-action weighting.
- **B-NEG-01–08:** missing source/execution/calibration/risk, stale/future support, changed scope, false booleans, wrong path IDs/weights/background/account, future realised input, overlap, unsupported delay and altered body identity all refuse. Common-background omission, wrong path/payoff and invented profit also refuse.
- **B-POS-01:** indexed causal source/execution/probability/risk support passes real evaluation with ES required; correct common-path cash/ES/J is retained. The fixture has 40 training and 200 validation observations, five tail observations at 97.5% and at least 20 effective calendar blocks. These are synthetic contract results, not market probability qualification.
- Original rank-two/fourth-plus, full population/refusals/resource/deadline, sizing/cash/funding/partial fills, S2/S3 replay, tail-mean −1.01, B outside-E/expiry/revision/duplicate and V1/Bybit regressions remain covered by current runs.

[Qualified capsule](transition-p1-financial-evidence/qualified-capsule.json) contains the actual 2,019 captured immutable entries and sealed E/request/response/result. [Replay receipt](transition-p1-financial-evidence/capsule-replay.json) records load into a fresh database, actual strict B validation after reopen, identical result and duplicate refusal after a second reopen. The [reproducer](transition-p1-financial-evidence/reproduce.py) recomputes all four offline bootstraps as well. Work timing fields may change artifact hashes between complete new runs; economic results and causal rules remain reproducible. All identities/data/clocks are explicitly synthetic, with zero authority.

No SQL schema/migration, source deletion or reinterpretation of old saved fields is introduced. New versions use the existing strict `OfflineArtifact` Decimal/string/tuple/canonical hash codec. Old V1 readers still round-trip the published P1 capsule; they reject V2 wire versions. New consumers refuse unsupported V1 qualification. Historical operational readers/routes are unchanged. Rollback retains opaque new research evidence and the existing operational schema; it does not convert new evidence into old approvals or erase consumed receipts.

## Downstream rules and remaining gates

P2 receives the same financial sizing/cash/action contracts, with mandatory verified calibration/scenario references. P3 must preserve the exact new versions/units/provenance/status projections; it cannot supply missing qualification or reinterpret a historical SUPPORTED flag. P4 must use these producers/readers and current material-revision projections when integrating the single runtime. Cash/allowances/ES are USDT; probabilities/rates are dimensionless; quantity and price retain product-contract units; all clocks/cutoffs/durations are UTC nanoseconds and the common terminal is 24h. Original candidate deadline, immutable eligibility identities and zero-authority B semantics are unchanged.

The P1 parent was not independently accepted; this correction also remains a candidate. Exact final remote SHA and full parent→correction diff are returned in delivery after `git ls-remote` verification. Independent review must inspect both new evidence chains, counterexamples, corrected fixtures, source/capsule hashes and regressions before recording acceptance.

The two previously excluded seven-day S3 tests remain **TEST GATE / NOT RUN** in this correction:

1. `test_s3_production_setup_consumes_causal_seven_day_prefix_and_fails_residual_tampering`.
2. `test_s3_subsequent_reversion_emits_unsized_candidate_with_frozen_vwap_and_selector_zero`.

Their expensive full-history replay was not included in the bounded affected groups. Full repository certification is not claimed. Mypy retains its pre-correction baseline errors; provider/actual market calibration, selected pretrained weights/image/renderer/native venue/Windows/minimum-host/72h qualification and prospective economics remain separate unpassed gates. Historical storage/timing failures are retained. No genuine economic claim is estimable from the synthetic capsule.

OBSERVE, bot allocation zero, paid budget zero, capital **NO-GO**. Manual aggregate/adoption policy remains PENDING OWNER APPROVAL; `ATLAS_RED_ELIGIBLE_ACTION_COORDINATOR_V1` remains PROPOSED / NOT OPERATIONALLY EFFECTIVE. No provider generation, exchange read/probe/order, deployment, merge or P2 implementation occurred. **Stop after this P1 correction push for independent exact-SHA review.**
