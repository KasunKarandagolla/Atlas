# ATLAS RED — live GitHub engineering status

**Updated:** 2026-10-16  
**Purpose:** Mutable pointer to independently accepted **design**, actual **implementation**, separately authorized **operation** and the **next engineering gate**. Check GitHub refs and evidence on every new task; do not treat this file alone as proof of implementation or acceptance.

| Dimension | Verified recorded state |
| --- | --- |
| Repository | [KasunKarandagolla/Atlas](https://github.com/KasunKarandagolla/Atlas), **PUBLIC** |
| Canonical documentation branch | `docs/atlas-final-system-2026-10-16` (discover latest remote HEAD before use) |
| **Independently accepted algorithm/design commit** | [`f9f8ef083ba5bafcad9bab4d07227676200a948f`](https://github.com/KasunKarandagolla/Atlas/commit/f9f8ef083ba5bafcad9bab4d07227676200a948f) |
| Independent acceptance record | [ACCEPTANCE_F9F8EF_2026-10-16.md](ACCEPTANCE_F9F8EF_2026-10-16.md) |
| Last verified implemented R9 branch/commit | `fix/red-r9-local-complete-cycle-2026-10-14` @ [`96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`](https://github.com/KasunKarandagolla/Atlas/commit/96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd) |
| P1 implementation candidate | `impl/atlas-red-p1-quant-foundation-2026-10-16`, from verified base `3cb4f052472623381145573523c7077950de78fd`; actual final remote SHA is in the delivery receipt, independent acceptance pending |
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

New evaluation-before-selection and mean-cash research route is implemented/tested offline; existing production remains historical pending P4 integration. Final source build `81a268d4bbade5999b3057d9301411eacf4fa14752a2f09013bd918bc5e363a7`. Combined checks 1,137 pass/1 skip, final P1+IPC 132 pass, preservation 272 pass/two long S3 exclusions; latest deduplicated coverage 1,420 pass/1 skip, not a full-suite run. Ruff/compile/pip/uv-lock/diff pass; mypy 53 baseline errors/17 files, zero normalized delta. Initial missing-Qt failures were resolved by declared pinned dependencies and retained in evidence. Three freezes, canonical design/PDF and policy bytes are unchanged.

See [P1 handoff](../v2/ATLAS_RED_P1_HANDOFF.md), [verification](../v2/transition-p1-quant-evidence/verification.json), [interface inventory](../v2/transition-p1-quant-evidence/interface-inventory.json) and [evidence hashes](../v2/transition-p1-quant-evidence/evidence-manifest.json). Account/cash integration evidence is synthetic; real local model fits do not establish market edge. Actual account/native lifecycle, integrated production Brief/campaign, selected pretrained/provider/image conformance, Windows/minimum-host/storage/timing and prospective economics remain unqualified. No provider/venue/order/deployment/capital activity.

## Mandatory update discipline

At **every significant implementation push**, update this status **in the same pushed change** with the actual starting/ending branch context (the new SHA can be added in the subsequent independent acceptance record), changed source/consumers, exact test commands/results, unresolved blockers and next acceptance gate. Also update root `log.md`, append major transitions to `translog.md`, revise affected `algorithms/ACCEPTANCE_REGISTER.csv` rows and link relevant sanitized test artifacts. Only change algorithm PDF/manifest when its underlying algorithm changes. A handoff's own claim never constitutes independent acceptance.

After independent review, add an exact SHA acceptance/rejection record and advance the accepted pointer only for what that review truly accepted. No self-approved operational policy, capital permission or fabricated passing test.

## Next gate

**Independent P1 review against the exact verified remote candidate SHA.** Inspect base→candidate source/tests, mathematical counterexamples, actual fit/inference, serialized contracts/replay, preserved readers, retained failures and limits. Record acceptance/rejection separately; only acceptance permits separately scoped dependent work. Do not self-accept or launch P2. Manual-account/adoption consent and operative policy approval remain separate pending owner decisions; capital NO-GO.
