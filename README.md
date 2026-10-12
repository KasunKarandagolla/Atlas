# ATLAS — current project status

**Updated 2026-10-12.** S48 continues the independently reviewed S47 checkpoint `b0db7d0527e68c737eefd564a87f740f5929c5f6`: source-bound Decision Brief repairs, bounded saturation rejection, expiring protection supervision and a read-only position lifecycle projection. See the [S48 handoff](docs/v2/ATLAS_S48_IMPLEMENTATION_HANDOFF.md), [evidence](docs/v2/transition-s48-evidence/evidence_manifest.json), [Binance contract audit](docs/v2/ATLAS_S48_BINANCE_SKILLS_HUB_CONTRACT_COMPARISON.md), [`log.md`](log.md) and [`docs/INDEX.md`](docs/INDEX.md). Independent acceptance remains pending.

## Current operating boundary

ATLAS is a capital-disabled public-market research and shadow system. The initial owner profile is Binance USD-M USDT perpetuals; Bybit remains inactive in that profile. Public research capability and capital execution are separate. Trading and assisted execution remain disabled. S48 does not amend the frozen V1, V2 or Agent contracts. The [comparative decision amendment](docs/v2/ATLAS_S47_COMPARATIVE_DECISION_AMENDMENT_PROPOSED_V1.md) remains **PROPOSED**, awaiting independent acceptance.

## S48 implementation status

Facts entering COMPLETE briefs require registered typed source readback, exact product/cutoff/availability and reproducible values. External-venue context is explicit. The two stale Ops schema assertions now match v3, retaining migration checks. The socket listener drains one bounded request frame before definite saturation rejection; uncertain delivery remains UNKNOWN. Protection confirmation expires or becomes recovery-required on contradictory evidence. Conditional algo readback requires exact identity and closing semantics.

The bounded lifecycle projection reuses the current journal/recovery and has no execution, reservation-release or maturity authority. It preserves venue/product/epoch/exposure scope and exposes flatness pending final economics. Native Binance opening qualification remains TEST GATE; Bybit frozen attached protection is unchanged. The [management amendment](docs/v2/ATLAS_S48_MANAGEMENT_RESEARCH_AMENDMENT_PROPOSED_V1.md) is **PROPOSED**, and its sole half-TP comparator is shadow-only. No second OMS, venue router, dependency, real/demo order or model call was added. Exact checkpoint results and remaining limitations belong to the handoff and manifest.

## Previous implementation — S47

New research read models preserve every CandidateSet row and the original A0 choice, mark unassessed/unsupported economics explicitly, exclude unqualified sparse-bar outputs and enforce configured bounds without silent truncation. Existing M0/M1/analogue/scenario evidence is reused; no alternative valuation engine or changed selector. Only fake local providers are runnable. Three sealed read tools allow at most one supplementary round. Durable request, trigger, intent, terminal, memo, invalidation and late-result refs reuse existing ops artifacts and a separate due-work lane, with no new table or dependency. Uncertain attempts never redispatch; late evidence cannot clear UNKNOWN.

**TESTED checkpoints:** final S47/due-work 85/85; peer/features/S6 113/113; economics/selection 145/145; memory/capture/V1/S44 132/132. Agent/event 128 pass, one retained saturation failure and two explicit existing skips. Additional broker/worker/outbox: 63 pass and two unchanged schema assertions reproduced on S46. The handoff/manifest retain exact passing/failing reports and static results. No paid provider, chart, private exchange or capital action. Incremental value remains **NOT ESTIMABLE**.

## Previous completed implementation — S46

The peer seam uses exact instrument and universe revisions, existing S6 H1 return selection and BTC betas, explicit common timestamps and evidence refs, null states for unavailable estimates, and a process-local cache bounded to 32 entries and one H1 TTL. It does not feed selector, risk, plan or trading decisions. `TECHNICAL_V1`, S6 calculations and policy hashes remain unchanged. Cleanup disposition is **DELETE NONE**.

**TESTED:** peer tests 10/10; peer/S6/feature consumers 42/42; production outcome and maturity group 76/76; durable capture/recovery 27/27; V1 golden, persistence/recovery and S44 M15 group 26/26; S1/S2/selection/migration group 53/53. Calendar and event suites passed 35/35 when run independently. Two combined runs each had the existing saturation assertion observe `CALL_OUTCOME_UNKNOWN` instead of `BROKER_SATURATED`; the fail-closed result and isolated passing reports are retained in the S46 evidence directory. Library comparison did not establish full warmup/seed parity, so no dependency was added.

## Open qualifications

- Fake Bybit source/supervisor fixture: **TEST GATE**; `INCOMPLETE_SNAPSHOT` and noncausal dependency `ValueError` remain unresolved.
- TQ-01 injected stream stall: **TEST GATE**; 480/512 queue occupancy and one rejected frame; S45 80/120 frames/s attempts produced no valid sustained drain metrics.
- Real Binance final-M15 delivery within close+5s: **UNVERIFIED**.
- Linux F1/A2 sustained capacity: **TEST GATE**.
- Authenticated/native Binance protection and capital remain unqualified; capital and assisted execution stay disabled.
- Historical combined calendar/event saturation failures are retained; S48 repairs/consumer run passes the original combined tests. Ambiguous delivery remains UNKNOWN; this is offline test evidence.
- Existing `TECHNICAL_V1` on sparse bars: **TEST GATE** for future gap-sensitive use; current calculations accept sparse input without a gap label.
- Economic value: **NOT ESTIMABLE**.

Do not infer source latency, capacity or economics from deterministic fixtures. S48 makes no live diagnostic, credential, capital or promotion-ladder change.

## Previous completed implementation — S45

S45 fixed immutable `content_hash` selection in due-work discovery. Its production outcome batch passed 61/61, S33 maturity passed 15/15, durable capture passed 25/25, combined event/calendar passed 35/35, and V1 golden and S44 compatibility each passed 1/1. These are historical results. See the [S45 factual handoff](docs/v2/ATLAS_TRANSITION_S45_FACTUAL_HANDOFF.md) and its [evidence manifest](docs/v2/transition-s45-evidence/evidence_manifest.json).

## Active entrypoints

Package scripts are declared in [`pyproject.toml`](pyproject.toml): `atlas-product`, `atlas-v2-projection`, `atlas-desktop`, `atlas-v2-public-qualification`, `atlas-v2-public-soak`, `atlas-ops`, `atlas-agent-worker`, and `atlas-agent-broker`. Runtime modules and their boundaries are listed in [`docs/INDEX.md`](docs/INDEX.md). Their presence does not authorize capital or assisted execution.

## Historical evidence

S41 recovery, TQ-01 qualification, S44 close-driven M15 and S45 outcomes remain discoverable in [`docs/INDEX.md`](docs/INDEX.md), `docs/v2/`, root logs, and the retained evidence directories. Historical failures and frozen V1 comparator artifacts are preserved with their original scope and identities.

`requirements-lock.txt` is the installation lock used by the recorded S45/S46 environment; it pins DuckDB 1.5.5; the same lock remains unchanged in S47. `uv.lock` resolves DuckDB 1.5.6. S46 records this discrepancy without regenerating either lock.
