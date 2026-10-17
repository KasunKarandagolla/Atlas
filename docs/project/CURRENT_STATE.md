# ATLAS RED — live GitHub engineering status

## R-E1B R1/R2 research-output correction candidate — 2026-10-17

Isolated from the newly verified E1B research tip `fa6085bdbe456ffa07ac59ce8c51afacb65ecdc5` (parent `6e423d0196fe1f744456d169a136074f3f2453c4`) on `fix/atlas-red-e1b-signal-time-integrity-2026-10-18`. The correction is research-output-only; S1/S6 policy, coordinator, frozen authorities, and production readers are untouched. The correction branch's exact final pushed SHA and remote verification are recorded in the companion `E1B_R1_R2_CORRECTION_HANDOFF.md` after delivery.

R1 separates current precursor state, setup-event ID/cutoff/side/ref, originating trigger setup, every raw predicate, and one first-trigger event per setup ID. Previous setup ownership persists through its final fourth M15 opportunity across an H1 boundary. Every missing current M15 row and every missing/noncontiguous prior M15 input during an active window is SOURCE_MISSING. R2 uses a research-only `ResearchBar`, stable archive checksum/CSV row identities, UNKNOWN actual retrieval/receipt time, a distinct close+5s replay schedule, and ≥1s derived processing readiness. Exact-close feature predicates remain zero-lag descriptive and not replay-visible at the cutoff.

Correction tests: 11 E1B tests passed, including red reproductions, production binding at opposing/empty boundary states, unchanged and changed setup identity, four-opportunity expiry/dedup/missingness, prior-history uncertainty, stable source identities, replay-clock checks and real-prefix parity. The affected S1/S6/prefix/cohort/archive regression group passed 40 tests. Final corrected offline rebuild retains 819,840 feature rows and 1,024,800 coverage rows; panel SHA-256 `0cb67844a9a13110fba6628707d3be7a2be03ffa1fc156543b49ff8284e2f6cc`, compact sample SHA-256 `79e4fbd8551623c957501bd099fcc5075e70398e32fe1e2c101bc810a1fbac6f`, source manifest SHA-256 `8d510aee5fde7843fe44f74979680d10baef5daae199aed53311fcedfe4f9709`. Corrected setup events are A/B 15,062/13,124; raw predicates 13,690/12,003; unique first events 9,261/7,899. All 15,610 missing current M15 inputs in B are SOURCE_MISSING. Coverage hash remains `eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61`. The 757 verified archives, 13 official 404s and 420 historical parser rejects are retained; no network fetch occurred. Corrected-code clean-clone parity is not established. Model/effort attestation is UNVERIFIED. Independent ChatGPT acceptance is pending; Sol evaluation and outcomes remain stopped.

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
| Prior bootstrap-integrity candidate | `81870a18a1f773e84ad1b0c82690dc8e4d677bb8`; owner-supplied independent assessment found N1–N3; acceptance withheld |
| Current P1 correction candidate | `fix/atlas-red-p1-evidence-integrity-2026-10-17`, exact parent `81870a18a1f773e84ad1b0c82690dc8e4d677bb8`; exact pushed SHA returned in delivery. **INDEPENDENT ACCEPTANCE PENDING** |
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

The owner authorized the N1–N3 evidence-integrity correction, including an explicit scoped N3 original pre-outcome population/selection producer contract after all three findings were reproduced. Actual M0 fit reconstruction, original complete selected-policy membership, and complete checked saved evaluation receipts protect publication/import/B on the existing Ops writer. Expensive verification stays offline; B discretion, financial objective, bootstrap, risk restrictions and historical frozen authority are unchanged. No SQL migration or P2 work. Legacy records without original proof remain readable and diagnostic-only.

Final P1: **220 passed**. Disjoint affected broad: **1,154 passed / 1 skipped**. Combined **1,374 passed / 1 skipped**, zero failures; both partitions after final production/test changes. Final observed checks, exact commands, original RED→GREEN evidence, baseline failures, source/artifact hashes, performance and trusted-source import/restart replay: [correction report](../v2/ATLAS_RED_P1_EVIDENCE_INTEGRITY_CORRECTION.md), [verification](../v2/transition-p1-evidence-integrity/verification.json) and [hash manifest](../v2/transition-p1-evidence-integrity/evidence-manifest.json). Mypy retains the existing 53 errors/17 files with zero additions. Protected freezes/design/PDF/policies/dependencies/prior evidence remain unchanged. This is affected offline coverage, not a full repository or host qualification. Two historical seven-day S3 cases remain TEST GATE / NOT RUN.

Grade remains SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT. Explicit original Ops producer/clock trust and complete indexed population are the supported contract boundary; real source completeness, historical provider choices and genuine prospective production integration remain UNVERIFIED. No original historical selections were invented. No paid/provider/private venue/order/deployment/capital activity. Economic profitability remains NOT ESTIMABLE; all separate operational/provider/native/image/hardware/storage/72h gates remain open.

## Mandatory update discipline

At **every significant implementation push**, update this status **in the same pushed change** with the actual starting/ending branch context (the new SHA can be added in the subsequent independent acceptance record), changed source/consumers, exact test commands/results, unresolved blockers and next acceptance gate. Also update root `log.md`, append major transitions to `translog.md`, revise affected `algorithms/ACCEPTANCE_REGISTER.csv` rows and link relevant sanitized test artifacts. Only change algorithm PDF/manifest when its underlying algorithm changes. A handoff's own claim never constitutes independent acceptance.

After independent review, add an exact SHA acceptance/rejection record and advance the accepted pointer only for what that review truly accepted. No self-approved operational policy, capital permission or fabricated passing test.

## Session R-E1 historical-evidence source checkpoint — 2026-10-17

Research-only worktree starts at f77f3aacbfdc2546e6f3ea2d78196b0905ceb613; its independent P1 acceptance remains pending. GitHub and official archive documentation were reachable, but both the candidate Binance USD-M BTCUSDT 1H ZIP and .CHECKSUM requests were rejected by the session proxy with HTTP 403. No market archive, symbol/date coverage, signal, label or holdout outcome was observed. See the [Session R-E1 handoff](../research/atlas-red-e1/SESSION1_EVIDENCE_HANDOFF.md), [source matrix](../research/atlas-red-e1/SOURCE_FEASIBILITY_AND_ROLE_MATRIX.csv) and [manifest](../research/atlas-red-e1/DATASET_MANIFEST.json).

Existing strategy/prefix fixture coverage is 111 passed; an attempted archive-runtime group has four failures because pyarrow is unavailable in the existing environment. No dependency, production source, policy, authority or acceptance row changed. Source grade is BLOCKED BY ENVIRONMENT. The smallest remedy is managed review of anonymous HTTPS to the official data.binance.vision host, followed by one checksum-verified sample. Do not start Sol's evaluation until the exact research handoff receives independent ChatGPT review.

## Next gate

**Independent ChatGPT review of the exact pushed N1–N3 evidence-integrity SHA and complete `81870a18`→correction diff.** Review code/tests, original producer/import trust limits, actual RED→GREEN proofs, genuine positives, complete final P1 and affected regressions, immutable readers/replay and retained gates. Record acceptance/rejection separately. P2 requires exact-SHA acceptance AND separate explicit owner authorization; do not start automatically. Manual/adoption consent and operative policy remain pending, capital NO-GO.

## Session R-E1 archive probe correction — 2026-10-17

A later retry retrieved one official BTCUSDT 1H daily ZIP and its .CHECKSUM (1,561 and 92 bytes); SHA-256 matched `09a24635f090c9b2ab17f4a61aa3c4d262a7ccc88e96ff0199ce66a54a8ab66b`. Parsed 24 consecutive rows on 2024-01-01. The earlier 403 was time-specific. No symbol prefix inventory or historical listing/contract revisions were obtained (symbol folder 404; tested S3 listing request proxy 403). No S1/S6 outputs, policy features, opportunity rows or outcomes were generated. Session remains stopped before bulk acquisition because an unbiased all-symbol/as-of cohort cannot be established from the evidence currently accessible. See the E1 handoff and manifest for the verified sample and exact source probes.

Fixture retest at `2135d161ebb6d8e8d7741bd8257174d280720ae2`: scoped S1/S2/S3/S5/S6/S8 and prefix group 111/111 passed in 20.06s. Adding archive runtime yielded 111 passed / 4 pyarrow-missing failures (18.77s). These are existing fixture/code tests, not archive strategy outputs.

Added a stdlib-only bounded sample refetcher that verifies official and pinned SHA-256, archive/member/schema and all 24 hourly timestamps, retaining no data. Local run passed; fresh-clone proof pending. It does not construct the research panel.

Clean-clone fallback proof: from a fresh depth-1 clone at `2cabae9ea89d644c133ee2fcbd2a7974c7e9ca14`, `python docs/research/atlas-red-e1/refetch_sample.py` fetched the official ZIP/CHECKSUM, validated both hashes and all 24 rows, retained no archive, and left the clone clean. This is a one-sample proof only; full panel regeneration is not implemented.

Added `docs/research/atlas-red-e1/SAMPLE_TRACE.json`, emitted by the pinned refetch utility: 24 raw bar records map to deterministic H1 range features and 23 within-sample close returns. The feature trace preserves unknown historical availability. S1 and S6 are explicitly BLOCKED_INPUT / NOT_RUN; neither policy consumer was called and no signal state is inferred. Script lint/compile and trace consistency checks pass; final clean-clone trace equivalence pending.

Clean-clone replay at SHA `9e7744a9787bf340fd75ffabf6ac517b4ed3e3b7` refetched the official sample and emitted bytes identical to `SAMPLE_TRACE.json`. The trace contains 24 source rows and deterministic H1 features; S1/S6 are BLOCKED_INPUT / NOT_RUN, with no policy calls or outcomes. Clone stayed clean and no raw archive was retained.

## Session R-E1B real multi-symbol archive research — 2026-10-17

Research package candidate `research/atlas-red-binance-cohort-e1b-2026-10-18` at code/data SHA `9c872fb8e66a1396412453e132cc00bb576e9bcb` before this documentation-only follow-up. Follows the exact frozen 35-symbol cohort and March–June UTC windows in `docs/research/atlas-red-e1b/COHORT_AND_SPLITS.md`. 757 official anonymous archive objects passed sibling-checksum verification (55,549,936 accepted ZIP bytes); 13 official 404s and all native gaps are explicit in coverage. Full panel: 819,840 rows; coverage: 1,024,800 slots; both were reproduced from a fresh depth-one clone with empty scratch and exact hashes recorded in the dataset manifest. Full panel exceeds GitHub's file limit and remains regenerable; compact sample, complete coverage, source manifest, builder, tests, and session/Sol handoffs are committed.

The E1B-specific six tests passed. Impacted existing regression set passed 40/40 with `/workspace/.venvs/atlas/bin/python -m pytest -q tests/v2/test_session014_s1.py tests/v2/test_session021_s6.py tests/v2/test_session037_strategy_prefix.py tests/v2/test_session041_s6_cohort.py tests/v2/test_session041_s6_action_contract.py tests/data/test_prefix_and_archive.py`. The complete shadow coordinator and full S6 policy eligibility are not demonstrated. Grade is reconstructed historical archive data; model attestation `UNVERIFIED`; costs, eligibility and profitability `NOT ESTIMABLE`; no profitability claim, order, or Sol evaluation. Independent ChatGPT review is the next gate. See [E1B handoff](../research/atlas-red-e1b/SESSION1_EVIDENCE_HANDOFF.md), [Sol reproduction protocol](../research/atlas-red-e1b/SOL_SESSION2_REPRODUCTION.md), and [rebuild evidence](../research/atlas-red-e1b/REPRODUCIBILITY.md).
