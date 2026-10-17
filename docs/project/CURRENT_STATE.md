# ATLAS RED — live GitHub engineering status

**Updated:** 2026-10-17
**Purpose:** Mutable pointer to independently accepted **design**, actual **implementation**, separately authorized **operation** and the **next engineering gate**. Check GitHub refs and evidence on every new task; do not treat this file alone as proof of implementation or acceptance.

| Dimension | Verified recorded state |
| --- | --- |
| Repository | [KasunKarandagolla/Atlas](https://github.com/KasunKarandagolla/Atlas), **PUBLIC** |
| Canonical documentation branch | `docs/atlas-final-system-2026-10-16` (discover latest remote HEAD before use) |
| **Independently accepted algorithm/design commit** | [`f9f8ef083ba5bafcad9bab4d07227676200a948f`](https://github.com/KasunKarandagolla/Atlas/commit/f9f8ef083ba5bafcad9bab4d07227676200a948f) |
| Independent acceptance record | [ACCEPTANCE_F9F8EF_2026-10-16.md](ACCEPTANCE_F9F8EF_2026-10-16.md) |
| Last verified implemented R9 branch/commit | `fix/red-r9-local-complete-cycle-2026-10-14` @ [`96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`](https://github.com/KasunKarandagolla/Atlas/commit/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd) |
| Original P1 candidate | `impl/atlas-red-p1-quant-foundation-2026-10-16` @ `8fc2109cda6fea6a8bea0f07d8e43999e9cee1c9`; owner-supplied independent review withheld acceptance for financial-evidence A/B |
| Prior P1 correction | `fix/atlas-red-p1-financial-evidence-2026-10-16` @ `215a0045d8203f23dbd4935639ef448556cc2353`; owner-supplied independent review still withholds acceptance for bootstrap-allowance integrity |
| Current P1 correction candidate | `fix/atlas-red-p1-bootstrap-integrity-2026-10-17`, exact parent `215a0045…`; verified pushed SHA returned in delivery. **INDEPENDENT ACCEPTANCE PENDING** |
| Repo default branch | `impl/session-001-foundation` (historical; **do not use as latest implementation**) |
| Operating manual/account policy | `ATLAS_RED_SHARED_ACCOUNT_EXCLUSIVE_AGGREGATE_V1`: **PENDING OWNER APPROVAL** |
| Operative policy amendment | `ATLAS_RED_ELIGIBLE_ACTION_COORDINATOR_V1`: **PROPOSED — NOT OPERATIONALLY EFFECTIVE** |
| P1 Sol offline code/tests | **OWNER AUTHORIZED; IMPLEMENTED / TESTED OFFLINE**. Model owner-confirmed GPT-6.1 Sol; independent runtime attestation unavailable; no subagents |
| Paid model, private venue, Demo/live, deployment, capital | **NOT AUTHORIZED**; capital **NO-GO** |
| Economic qualification | **NOT ESTIMABLE**; no genuine proven after-cost edge |

## Required first reads

1. [Root AGENTS.md](../../AGENTS.md), latest [log.md](../../log.md) and relevant append-only [translog.md](../../translog.md).
2. The accepted [algorithms/README.md](../../algorithms/README.md), [canonical algorithm](../../algorithms/ATLAS_SYSTEM_DESIGN.md), [acceptance register](../../algorithms/ACCEPTANCE_REGISTER.csv), [implementation plan](../../algorithms/IMPLEMENTATION_PLAN.md), source [authority reconciliation](../../algorithms/AUTHORITY_RECONCILIATION.md), [manual policy](../../algorithms/SHARED_ACCOUNT_MANUAL_INTERFERENCE_POLICY.md) and [proposed amendment](../../algorithms/ATLAS_OPERATIONAL_RECONCILIATION_AMENDMENT_PROPOSED.md).
3. Historical V1/V2/Agent freezes with verified hashes for all substantive review/implementation; these preserve historical behavior and do not overrule the independently accepted future target. Verbatim original owner/ADR source packet is not public here; the reconciled, hashed source map is [SOURCE_AUTHORITY_MANIFEST.json](../../algorithms/SOURCE_AUTHORITY_MANIFEST.json).

## Engineering plan and truthfulness

**Accepted planned sequence:** P1 GPT-6.1 Sol → P2 GPT-6.1 Sol → P3 GPT-6 Luna → P4 GPT-6.1 Sol → P5 fresh GPT-6.1 Sol. Owner authorized P1 offline work in this session; P1 is now a review candidate, not independently accepted. No P2 start is authorized by this status.

The second P1 financial-evidence correction is explicitly owner-authorized: bootstrap-allowance integrity, offline tests/evidence/docs, commit and push. Registered bootstrap unchanged. The existing Ops writer independently checks the entire causal chain and complete registered bootstrap before any new reserved `MeanCalibrationValidationV1` receipt can be inserted, including generic imports. Admission/B require that durable exact source/model/protocol/qualification/build/expiry binding, and B matches the saved cash allowance. No fit/bootstrap on the hot path. Old V2 calibration bytes remain readable but cannot grant new admission without complete revalidation. No SQL migration, P2 work or scenario redesign.

Final complete P1: 200 passed, 0 failed/skipped/deselected (200 collected). Expanded broad legacy/Ops partition: 1,154 passed, 1 skipped, 0 failed/deselected (1,155 collected). Disjoint combined affected coverage: 1,354 passed, 1 skipped, 0 failed (1,355 collected). Both partitions follow the final source change. Ruff/compile/pip/offline lock/diff/freeze checks pass; mypy remains exact 53-error/17-file baseline, zero new diagnostics. Added receipt validation measured 0.316 ms median /0.492 ms p95, 2.325 ms maximum over 200 warm cloud samples; no target-host qualification. Source build `fbe5dba51388c48ec0cc81303425eb7add4b11bc84f38d04d1bc55baffdf1c19`. A 2,514-entry synthetic capsule recomputes all five receipts on fresh import, reproduces saved B after reopen and refuses duplicate consumption on a second reopen. All original freeze/design/PDF/policy/lock bytes, prior reports and 57 earlier evidence files preserved.

See [second correction addendum](../v2/ATLAS_RED_P1_BOOTSTRAP_INTEGRITY_CORRECTION.md), [final commands/counts](../v2/transition-p1-bootstrap-integrity/verification.json), [numerical/replay evidence](../v2/transition-p1-bootstrap-integrity/numerical-and-replay.json), [performance](../v2/transition-p1-bootstrap-integrity/performance.json) and [SHA-256 evidence manifest](../v2/transition-p1-bootstrap-integrity/evidence-manifest.json). The [original P1 report](../v2/ATLAS_RED_P1_HANDOFF.md) and [first correction](../v2/ATLAS_RED_P1_FINANCIAL_EVIDENCE_CORRECTION.md) remain historical, unaccepted candidates. Two long seven-day S3 cases remain TEST GATE / NOT RUN. No paid/provider/private venue/order/deployment/capital activity. Genuine economic profitability remains NOT ESTIMABLE; all provider/venue/native/model/image/hardware/storage/72h gates remain separate.

## Mandatory update discipline

At **every significant implementation push**, update this status **in the same pushed change** with the actual starting/ending branch context (the new SHA can be added in the subsequent independent acceptance record), changed source/consumers, exact test commands/results, unresolved blockers and next acceptance gate. Also update root `log.md`, append major transitions to `translog.md`, revise affected `algorithms/ACCEPTANCE_REGISTER.csv` rows and link relevant sanitized test artifacts. Only change algorithm PDF/manifest when its underlying algorithm changes. A handoff's own claim never constitutes independent acceptance.

After independent review, add an exact SHA acceptance/rejection record and advance the accepted pointer only for what that review truly accepted. No self-approved operational policy, capital permission or fabricated passing test.

## Next gate

**Independent review of the exact pushed bootstrap-integrity correction SHA and `215a0045`→correction diff.** Review actual calibration/scenario evidence, counterexamples, immutable source/readers, complete final P1 and broad observed regressions, saved artifact replay and retained limits. Record acceptance/rejection separately. P2 requires that exact-SHA acceptance AND separate explicit owner authorization; do not start it automatically. Manual/adoption consent and operative policy remain pending, capital NO-GO.
