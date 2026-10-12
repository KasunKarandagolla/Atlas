# ATLAS S45 — transition document finalization readiness

**Date:** 2026-10-12
**Status:** design-document closure only; no freeze amendment or implementation authority granted.
**Related register:** [S45 design decision register](ATLAS_TRANSITION_S45_DESIGN_DECISION_REGISTER.md)

## Readiness conclusion

The proposed architecture design specification **can be frozen while hardware, source, venue, model and economic gates remain open**, if the document labels current frozen behavior separately from target behavior and carries explicit `TEST GATE`, `NOT ESTIMABLE` and authorization conditions. That would freeze interfaces, ownership, causal rules, failure behavior, staged qualification and rollback expectations. It would **not** certify the system as source-qualified, profitable, suitable for live trading or capable on a particular Linux/Windows host.

Do not amend or replace the three existing freezes as part of that design-document freeze. First approve a distinct, versioned transition specification and the bounded ADRs below. Any conflict with current runtime remains inactive until its own amendment and review.

## Source documents and availability

The owner’s V1 and amended V2 freeze copies are **not tracked at those paths in the S45 base checkout and were absent from `~/atlas-tq01-inputs/`**. They were available as pre-existing, untracked owner files in the preserved original checkout, read there in full and not copied into this worktree:

| Exact file | S45 access and SHA-256 | Handling |
|---|---|---|
| `/home/kasun/Music/atlas/ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` | Read from preserved original checkout; `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c` | Not present in S45 tracked tree; not copied. Its accepted content hash agrees with TQ-01. |
| `/home/kasun/Music/atlas/ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md` | Read from preserved original checkout; `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78` | Not present in S45 tracked tree; not copied. Its accepted content hash agrees with TQ-01. |
| `docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md` | Tracked and read in full; `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d` | Present in S45 worktree. |
| `docs/v2/ATLAS_TRANSITION_TQ01_LINUX_QUALIFICATION.md` and `docs/v2/ATLAS_TRANSITION_TQ01_VERSIONED_AMENDMENT_DRAFT.md` | Tracked in the TQ-01 base and reviewed as status/proposal | Qualification is evidence/status; amendment remains unaccepted draft. |
| Standalone S44 report | No separate S44 qualification report was found in the S45 tracked tree or root log | S44 status was read from the root coordinator log, README and TQ-01 report. The log’s S44 checkpoint supplies its exact source SHA and selected test evidence. |
| `/home/kasun/atlas-tq01-inputs/ATLAS_INTELLIGENCE_ARCHITECTURE_TRANSITION_PROPOSAL_2026-10-11.md` | Available and read as proposal/reference, not authority | Left in input directory; not copied. |
| `/home/kasun/atlas-tq01-inputs/ATLAS_TRANSITION_DECISION_AND_REUSE_RESEARCH_2026-10-11.md`, `ATLAS_PRETRAINED_TRADING_ML_RESEARCH_2026-10-11.md`, `ATLAS_PRE_TRANSITION_TRADER_ADVERSARIAL_REVIEW_2026-10-11.md` | Available in input directory and used as dated research references | Not implementation contracts; not copied. |

There is no S45 finding that any missing freeze file has new or altered authority. Reviewers should use the SHA-256 values above to locate the accepted owner copies. The owner inputs and their supplied manifest/README remain outside the project tree.

**Exact availability note:** the two named freeze files were unavailable inside the supplied input directory and S45 worktree, but not unavailable for review because the same exact files existed in the preserved original checkout at the paths above. The only requested status artifact not found anywhere in the checked-out repository/input bundle was a standalone S44 report; no content was inferred for it.

## Documents and sections to consolidate

| Document | Preserve and consolidate | Contradiction or detail to close |
|---|---|---|
| `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` — especially §§2–9, 9.1–9.11 | BTC/ETH v1 action; causal timestamps; versioned RiskPolicy; IOC entry; native MarkPrice stop; no TP/pyramiding/trailing in baseline; serial reservations; UNKNOWN/recovery; model/evidence promotion and 8-week/200-opportunity floor. | Keep this as current baseline. Any changed selector, order lifecycle, risk rule, schema or authority gets a new version plus migration/replay/rollback. Do not repurpose V1 fields or hashes. |
| `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md` — §§2–10, 11–27, 28–32, 35–44 | Product scope, causal universe/source records, tiered data, S1–S8 roles, S6 policy, CandidateSet, deterministic selection, M0/M1/scenario/evaluator, model arena, complete memory, promotion and dependencies. | V2 supports both venues as targets, while owner proposal selects a Binance-only first profile. State this as staged capability selection, not a contradiction requiring dual-venue load. V2 deterministic selection remains current until an amendment. |
| `docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md` — §§1–16 | Existing research proposer, hidden critic, separate event extractor, task ports, isolation, optional wrapper, fixed tools, budgets, immutable job ledger and promotion. | The owner’s future AI candidate-selection and research-candidate proposal goal conflicts with zero decision authority and deterministic V2 selection. Preserve as future amendment only; do not redefine critic position or allow agent-managed exits. |
| `docs/v2/ATLAS_TRANSITION_TQ01_LINUX_QUALIFICATION.md` | Current S44/TQ evidence, defects, host facts, no live source/venue/economic qualification, exact failed gates. | TQ’s synthetic suite is not a production due-work scheduler qualification. S45 repairs due-work and changes capture-test synchronization only; capacity and source remain gates. |
| `docs/v2/ATLAS_TRANSITION_TQ01_VERSIONED_AMENDMENT_DRAFT.md` | Historical unaccepted draft and proposed questions only. | Keep its amendment status visible. Do not merge it into a new specification or treat an older proposal as approved. |
| Root `log.md` and `translog.md` | Current checkpoint and append-only session evidence. | `log.md` is coordinator-owned short status; translog is the chronology. Neither is policy authority. |
| Owner research inputs under `~/atlas-tq01-inputs/` | Product proposal, reuse cards, pretrained model evidence and adversarial lifecycle review. | Label all as consultation dated 2026-10-11. Validate exact upstream versions/licenses and model/provider data terms at any later adoption review. |

## Draft ADR/amendment boundaries

These are proposed document boundaries, not accepted ADRs or new V2 phases.

1. **Transition target specification:** consolidate the target dataflow, ownership map, Path A/Path B, current-versus-proposed authority and qualification ladder. It must reference all frozen documents by ID/hash and leave them intact.
2. **Universe and capture profile ADR:** define Binance REST inventory, stream topic roles, instrument revisions, BTC/ETH anchors, evidence tiers, source health, data-age rules, 10-second broad refresh, M15 close+5s deadline, archives, disk/queue limits and crash recovery. Numerical capacities stay conditional on measurements. No new source package is adopted by this ADR alone.
3. **Agent decision-authority amendment:** decide P1 candidate selection versus P2 quarantined agent-origin hypotheses, deadline, exact admissible templates, selection denominator, no same-slot rescue, evidence/tool allowlist, budget/model identity, abstention and deterministic validation. Preserve risk sizing, hard gates, human authority and execution in ATLAS. This conflicts with Agent V1 §§1–5, 10–16 and V2 §§23, 26, 37.
4. **Position-management amendment:** retain existing baseline and separately identify any stop movement, TP/partial exit, dynamic hold or scaling variant. Specify venue order identities, position coverage, reduce-only, stop/TP/cancel races, restart/UNKNOWN, margins/funding and who may request a change. The deterministic protective supervisor must work with agent/provider/UI absent. This conflicts with V1 §§2–9, V2 §§24, 30 and Agent V1 §§1–3, 6.
5. **Provider/model and dependency ADR:** compare direct structured request with optional `pydantic-ai-slim`, exact provider/model version, tool/vision capability, data retention, current pricing, worst-case reservation and local resource use. Keep Kronos-mini an optional, fixed-weight, isolated forecast challenger. Adoption is blocked until the exact artifact/license/lock is reviewed.
6. **Market-context artifact decision:** no new covariance or regime engine is needed for current behavior. Use refs to S6’s BTC beta/residual rank, V1 scanner cluster labels and V1 joint BTC/ETH risk/scenario artifacts. A new wrapper should be proposed only if an agent/consumer cannot bind those refs and availability consistently; if created, it must pass through immutable refs/status/missingness and not recompute factors. Do not claim an ETH independent beta when the design is singular or evidence stale.
7. **Evidence and promotion appendix:** specify full-calendar immutable decision/outcome record, prospective timestamps, no-fill/partial/censored outcomes, economic estimand, costs, dependence/multiplicity, rollback and capital promotion. The 8-week/200-opportunity floor is necessary collection guidance, not proof.

No new strategy, V2 phase or production integration is required to finalize these documents. Each ADR should name responsible writer, reviewer and acceptance evidence when scheduled; this S45 session does not assign owners or dates.

## Proposed architecture paragraph

ATLAS observes a point-in-time, versioned universe through public venue metadata and bounded market-data streams; it preserves source bytes, sequence, actual receipt, validation, processing, availability, health and missingness before deriving features. Deterministic skills and S1–S8 produce a complete CandidateSet and a separate, quarantined evidence-first discovery queue, with contrary and absent evidence retained. A task-scoped research agent and optional fixed pretrained model may inspect only sealed, read-only evidence and may propose, select or abstain only under a separately reviewed authority version; today they have no trade-selection authority. Deterministic policy code freezes an exact, expiry-bound action and risk alone sets quantity. The after-cost evaluator requires support, uncertainty, execution bounds, stress and portfolio-risk gates before a hypothetical plan is written. Live execution, if separately enabled after venue and account qualification plus human authorization, remains within the existing single-writer ATLAS/Nautilus boundary. A deterministic position supervisor, venue-native protection and recovery path run independently from any model, UI or news worker. Every excluded, no-candidate, no-trade, not-estimable, expired, nonfilled, partially filled and reconciled terminal result remains in the causal decision calendar.

```text
Venue REST metadata + public WS streams + official event feeds
        │ source/receipt/availability/sequence/health, immutable raw archive
        ▼
Versioned universe + completed causal bars/features + full observation calendar
        ├── Path A: S1–S8 registered strategy candidates
        └── Path B: bounded research discovery (quarantined)
                    ▼
       CandidateSet / contradictions / exclusions / warmup
                    ▼
 Optional isolated agent or pretrained forecast (future reviewed version only)
                    ▼
 Deterministic exact action → risk-sized quantity → frozen action identity
                    ▼
 Costs/funding/fill bounds + support/uncertainty + stress/portfolio gate
                    ▼
 Prospective shadow outcome and complete mature evidence calendar
                    │ separate venue/account/human gate
                    ▼
 ATLAS single writer → Nautilus → qualified venue
                    └── deterministic protective supervisor/reconciler is continuous
```

The sketch describes target ownership, not measured runtime behavior. The first step has no authenticated endpoint, paid provider, capital or order side effect.

## Ordered minimal document-finalization plan

1. **Accept this status register as the review base** and verify the three freeze hashes. Do not alter freeze content during review.
2. **Draft one versioned `ATLAS_TRANSITION_ARCHITECTURE_DESIGN_V1.md`** from the paragraph/flow above, the S45 register and owner proposal. Put `CURRENT_FROZEN`, `PROPOSED_FOR_AMENDMENT`, `TEST GATE` and `NOT ESTIMABLE` labels beside every behavior; carry exact applicable policy/hash IDs.
3. **Draft only the necessary ADRs:** (a) Binance-first universe/capture contract; (b) AI candidate authority and Path B; (c) position-management policy variants and deterministic supervision; (d) provider/model/dependency choices. The market-context ADR is unnecessary unless the read-only reference path proves inadequate.
4. **Reconcile interfaces and contradictions:** enumerate frozen versus proposed event/candidate/action/assessment/plan/outcome artifacts; make it explicit which data the agent can read, the exact cutoff and deadline, and which deterministic process owns risk, protection and execution.
5. **Attach conditional qualification gates and rollback:** source, throughput/storage, Windows/Linux feature parity, dependencies, model resource/use rights, venue wire, operational faults, prospective economics and final UI. Do not invent pass values where S45 measured none.
6. **Run an independent owner/reviewer acceptance of the design package.** Only after that may a separately scoped implementation proposal identify which amendments become active. Keep source, economic and capital qualification separate from design sign-off.

### Supported now

- Preserve the V1 risk/execution/UNKNOWN/recovery core and V2 causal evidence, CandidateSet, S1–S8, M0/M1/scenario roles and complete decision calendar.
- Keep the first owner profile Binance-only and public-data, with BTC/ETH as permanent anchors; preserve Bybit support as a separate capability.
- Avoid indicator voting, duplicate covariance code, unbounded agent frameworks, strategy deletion and a second order writer.
- Keep economic promotion whole-calendar, after-cost, uncertainty-aware, multiplicity-controlled and prospective where model contamination is unknown.
- Require deterministic protective supervision independent of LLM response, desktop, news or research workers.

### Must remain conditional

- Current Binance REST/WebSocket receipt timing, eligible universe, rate budget, source health and close+5s hit rate.
- Queue/service throughput, SQLite/WAL/archive/index/storage rates, recovery after disk/I/O pressure, and platform parity.
- Agent provider choice, exact pinned model/version, current price, image support, privacy/retention and whether its latency fits a real deadline. No paid call was made in S45.
- Kronos/perps domain match, CPU/RAM and incremental after-cost value.
- Binance account/margin/stop/partial-fill/close/reduce-only/UNKNOWN/restart wire behavior.
- Any management variant beyond frozen IOC/fixed stop/time exit; prospective economic value; 8-week/200-opportunity support and all capital authority.

The design may be frozen with these items explicitly open. It cannot be labeled “live-trading-capable,” “economically qualified,” “safe on Linux” or “profitable” until the corresponding gates have independent evidence and authorization.
