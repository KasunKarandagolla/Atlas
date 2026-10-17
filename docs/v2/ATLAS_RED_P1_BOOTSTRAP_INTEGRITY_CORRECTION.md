# ATLAS RED — second P1 financial-evidence correction

2026-10-17. **OFFLINE CORRECTION CANDIDATE — INDEPENDENT ACCEPTANCE PENDING.**

This addendum supplements, and preserves, the [original P1 handoff](ATLAS_RED_P1_HANDOFF.md) and [first financial-evidence correction](ATLAS_RED_P1_FINANCIAL_EVIDENCE_CORRECTION.md). The owner supplied the independent review of `215a0045d8203f23dbd4935639ef448556cc2353` and explicitly authorized bootstrap-integrity correction, tests, evidence/documentation, commit and push. No P1 acceptance or P2 authorization is inferred.

## A. Source, model and scope

The clean starting local/remote corrective branch was `fix/atlas-red-p1-financial-evidence-2026-10-16` at exact `215a0045d8203f23dbd4935639ef448556cc2353`. Work is isolated on `fix/atlas-red-p1-bootstrap-integrity-2026-10-17`. Its parent contains the unaccepted `8fc2109c…` P1, governance `3cb4f052…`, and accepted design `f9f8ef083ba5bafcad9bab4d07227676200a948f`. Ref discovery found no later corrective descendant or P1 acceptance record. The final delivery supplies the actual pushed SHA; a commit cannot embed its own hash.

Model selection is **owner-confirmed GPT-6.1 Sol** in this continuing conversation. Available runtime metadata does not attest the model; no independent attestation is claimed. No subagents or substitute models were used. The separately authorized scope is this offline P1 correction only.

Source build: `fbe5dba51388c48ec0cc81303425eb7add4b11bc84f38d04d1bc55baffdf1c19` ([336-file manifest](transition-p1-bootstrap-integrity/source-build.json)). The [new interface inventory](transition-p1-bootstrap-integrity/interface-inventory.json) identifies its producer, writer and consumers. No dependency, SQL schema, accepted algorithm, original freeze, scenario contract or operative-policy change.

## B. Complete-bound verification and durable trust

The first correction checked the verified empirical mean-error floor and agreement between two artifacts. That did not establish the registered bootstrap upper bound. The four [red tests](transition-p1-bootstrap-integrity/red.txt), run before production source edits on `215a0045`, demonstrate both understated allowances, connected economic admission and saved B consumption accepting the forgery.

The existing `selected_policy_upper_cash` algorithm is **byte-for-byte unchanged**: sorted weekly calendar groups, sample complete groups with `random.Random(6101)`, accumulate each sampled group's residual sum and selected-action count, divide by sampled total count, take index `ceil((1-alpha)*resamples)-1`, and apply the nonnegative and empirical-mean floors. Alpha and sample count come from the immutable registered protocol. No payoff quantile, unweighted block mean, different confidence level or conservative approximation replaces it. Excess conservatism introduced by this fix is **zero**.

The additive `MeanCalibrationValidationV1` is a **reserved Ops writer receipt**, not a portable self-authenticating certificate. It binds the exact V2 calibration hash, actual verifier implementation identity, independently recomputed required allowance, availability and expiry. Calibration → qualification → protocol/model/scope/residual → prediction/training/origin/outcome hashes transitively bind all inputs, membership, chronology, embargo, support and policy/product/size/liquidity compatibility. The implementation identity hashes the actual producer/verifier/model/codec/repository/consumer sources, Python random source/runtime and Decimal context. No hash supplied by a caller selects the trusted implementation.

`OpsRepository.register_artifacts` is the enforced publishing boundary. Every **new** reserved receipt, including generic single/batch imports, calls `validate_calibration_validation_registration`. That function independently resolves and verifies the entire causal chain through `_verify_mean_calibration_chain`, derives actual dependence groups, recomputes the **complete** registered bootstrap and compares it with both the allowance and exact requested receipt. An understated allowance or false certificate fails before commit. Receipts are checked after their batch dependencies; a failure rolls back the entire existing transaction. The alternative `publish_stream_slice_v1` insertion path explicitly refuses this reserved type. Compact archive locators already accept only their three public-data types. An exact previously validated immutable retry does no statistical work.

The invariant is: a newly committed receipt can contain required bound R only after the trusted writer recomputes R from the complete bound inputs and checks calibration allowance ≥ R. Consumption requires that exact immutable calibration/implementation receipt and checks the allowance against R again. Changing and rehashing either financial record changes the receipt lookup identity; obtaining a replacement invokes the complete independent calculation.

This is a real application write boundary, with the same trusted local Ops owner/database assumption as the existing one-use and holdout receipts. Arbitrary SQL, replacement of the writer binary or installation of an attacker-written SQLite database is outside that trust boundary. **Do not import an untrusted database as trusted owner state.** Import artifact exports through the checked API, which recomputes their receipts. Matching hashes, constructing a receipt object or declaring `verified=True` confers no admission status.

`calibrate_verified_selected_policy` still produces the original V2 calibration and V1 dependence qualification, then requests checked publication. `verify_mean_calibration` still reconstructs the complete original causal evidence on consumption, then requires the current writer-validated receipt. The evaluator's existing call now enforces the complete bound. `validate_offline_b_selection` also calls it and additionally requires the saved evaluation's allowance to equal the authentic calibration allowance. A rehashed saved allowance cannot borrow a genuine calibration.

Admission and B perform no bootstrap, fit, training or OOF generation. The added operation is one bounded indexed receipt lookup and compact validation. Full source-file fingerprinting is cached once at offline publication/startup; runtime/context identity is checked on consumption. Changed allowance, qualification, protocol, source, membership, model, scope, expiry or relevant implementation gives a different binding and requires complete offline revalidation. A missing or malformed receipt yields UNAVAILABLE / NOT ESTIMABLE, never zero.

V1/V2 calibration and scenario codecs retain their exact saved meanings. Old V2 records remain readable but have **no new admission authority** without explicit complete offline verification and a current receipt. Reopen of trusted owner state preserves validation; importing serialized artifacts into a fresh owner rechecks every receipt. A new implementation requires new receipts. Retain old records for historical replay; rolling back the binary is not approval to resume the vulnerable P1 admission path.

## C. Numerical and connected oracles

The [parent numerical capture](transition-p1-bootstrap-integrity/parent-numerical.json) ran against a separate exact `215a0045` checkout. The [current numerical/replay evidence](transition-p1-bootstrap-integrity/numerical-and-replay.json) and [reproducer](transition-p1-bootstrap-integrity/reproduce.py) use genuine fixture origin/outcome records, actual M0 fits, per-fold predictions, mature targets and compatible scope/embargo. These are explicitly synthetic offline data, not market qualification.

| Oracle | Observed result (USDT) |
| --- | --- |
| 15 residuals +10 and 15 residuals −10 in 30 weekly blocks | Mean error **0**; effective blocks **30**; lag correlations −0.166667, −0.2, −0.033333 |
| Registered alpha .05, 10,000 draws, seed 6101 | Required upper bound **3.333333333333333333333333333**; independent reference implementation agrees |
| Rehashed qualification + calibration, allowance 0 or 1.666666666666666666666666666 | Verification and receipt publishing **REFUSED** |
| Same exact action, mean 5, other numerical allowance 2, forged zero bound on parent | **ELIGIBLE**, score **+3** |
| Same exact action with the correct bound | **INELIGIBLE**, score **−0.333333333333333333333333333**; cannot be selected via another eligible member |
| Supported genuine bound, mean 5, other allowances zero | **ELIGIBLE**, score **+1.666666666666666666666666667**; sealed lower-ranked B selection succeeds |
| Unequal action counts, 50 selected actions in 30 blocks | Correct weighted bound **4.210526315789473684210526316**; incorrect unweighted bound **3.333333333333333333333333333** |

CAL-NEG-01–06 and CAL-POS-01–03 are exercised through `tests/p1/test_bootstrap_integrity.py` plus the preserved complete P1 group. They cover zero/midpoint forgeries, generic receipt falsification, atomic rollback, protocol/model/source/cell/membership/expiry/build mutations, saved E/B substitution and reopen, no generation on either hot path, weighted positive admission and legacy readers without promotion. Monkeypatched bootstrap/OOF/fit functions throw if invoked on admission/B. Fresh database import invokes all five bootstrap validations; subsequent reopen, admission and B invoke none. A second reopen refuses duplicate consumption.

## D. Verification and performance

Final complete P1: 200 passed, 0 failed/skipped/deselected (200 collected). Expanded broad legacy/Ops partition: 1,154 passed, 1 skipped, 0 failed/deselected (1,155 collected). Disjoint combined affected coverage: 1,354 passed, 1 skipped, 0 failed (1,355 collected). Both partitions follow the final source change.

Exact final suite commands/counts and source timing are recorded in [verification.json](transition-p1-bootstrap-integrity/verification.json). Both the complete P1 directory and expanded broad affected partition run **after the final production source change**. They have disjoint case selections; the combined broad total is their union. Earlier reported 179/1,200/1,207 counts remain historical evidence only.

The broad selection retains economic/model/causal/OOF/selection/scenario/portfolio tests; V1/Bybit/unit/scanner/data/runtime/persistence/golden/replay tests; and adds the directly affected Ops repository, compressed metadata, stream publication/archive, writer, collector restart and recovery suites. Two expensive seven-day S3 tests remain explicitly **TEST GATE / NOT RUN**, with exact IDs in verification.json. No full-repository or target-host certification is claimed.

Ruff `src scripts tests`, compileall, pip consistency, offline lock check and diff checks pass. Existing mypy remains **53 errors across 17 files**, exactly matching the normalized fresh parent baseline, zero added diagnostics (301 source files checked). Protected freeze/design/PDF/policy/lock hashes, old evidence/report bytes, saved artifact replay and staged secret inspection are recorded separately.

[Performance measurements](transition-p1-bootstrap-integrity/performance.json) were collected before the regression partitions started. The added receipt check over 200 warm samples measured **0.316325 ms median, 0.491548 ms p95, 2.325092 ms maximum**. First source fingerprint measured **0.932651 ms**. The full four-candidate evaluator measured **996.58 ms median / 1,031.38 ms maximum** over five samples, against unchanged 10 s CPU / 20 s wall work budgets. A recorded complete evaluator used 1.058 s CPU/wall. Complete B validation over ten fresh requests measured **684.59 ms median / 759.43 ms maximum**, including all existing evidence and durable one-use writes. Two eligible members incur two compact receipt checks. The pre-existing causal chain dominates total cost; separate whole-verifier samples have ordinary measurement noise and are not used to invent a negative overhead. These cloud samples do not qualify minimum hardware or deadline tails. No thresholds or resource limits were weakened.

The compressed [2,514-entry capsule](transition-p1-bootstrap-integrity/qualified-capsule.json.gz) preserves exact saved source/evaluation/request/response identities. Its five receipts are independently recalculated on fresh import (1.519 s measured), then the real B validator returns the identical saved result after reopen. A second reopen refuses consumption. [Evidence manifest](transition-p1-bootstrap-integrity/evidence-manifest.json) lists SHA-256 hashes for the sanitized artifacts; [artifact identities](transition-p1-bootstrap-integrity/artifact-identities.json) distinguish named refs from body hashes, and [validator implementation](transition-p1-bootstrap-integrity/validator-implementation.json) records the exact code/runtime binding. Source-build and validator implementation hashes are separate identities.

## E. Remaining gates and mandatory stop

The owner must obtain independent acceptance of the actual pushed SHA and diff. This report is not that acceptance. Genuine market/calibration/source, provider/checkpoint/image, venue/native lifecycle, target hardware/72h/storage and prospective economics remain unqualified. Economic profitability is **NOT ESTIMABLE**. The shared-account manual/adoption policy remains **PENDING OWNER APPROVAL**. The operative amendment remains **PROPOSED / NOT OPERATIONALLY EFFECTIVE**. No paid generation, private venue access, order, account change, deployment or capital activity occurred.

**P1 CORRECTION PUSHED — INDEPENDENT ACCEPTANCE PENDING** (exact remote SHA in delivery).

**P2 NOT STARTED. OPERATIVE POLICY INACTIVE. CAPITAL NO-GO.**

Stop after verified push. P2 requires independent acceptance of that exact correction SHA and a separate explicit P2 GPT-6.1 Sol offline implementation authorization.
