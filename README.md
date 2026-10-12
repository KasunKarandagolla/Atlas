# ATLAS — current project status

**Updated 2026-10-12.** S46 implements bounded, read-only H1 peer relationship context on the S45 foundation (implementation commit `e2828d41f0942184f0b75a001ebf19d77561a6b6`). The [S46 handoff](docs/v2/ATLAS_S46_IMPLEMENTATION_HANDOFF.md) records tests and open qualifications. See [`log.md`](log.md) for the coordinator checkpoint and [`docs/INDEX.md`](docs/INDEX.md) for the document map.

## Current operating boundary

ATLAS is a capital-disabled public-market research and shadow system. The initial owner profile is Binance USD-M USDT perpetuals; Bybit remains inactive in that profile. Public research capability and capital execution are separate. Trading and assisted execution remain disabled. S46 does not amend the frozen V1, V2 or Agent contracts.

## S46 implementation status

The peer seam uses exact instrument and universe revisions, existing S6 H1 return selection and BTC betas, explicit common timestamps and evidence refs, null states for unavailable estimates, and a process-local cache bounded to 32 entries and one H1 TTL. It does not feed selector, risk, plan or trading decisions. `TECHNICAL_V1`, S6 calculations and policy hashes remain unchanged. Cleanup disposition is **DELETE NONE**.

**TESTED:** peer tests 10/10; peer/S6/feature consumers 42/42; production outcome and maturity group 76/76; durable capture/recovery 27/27; V1 golden, persistence/recovery and S44 M15 group 26/26; S1/S2/selection/migration group 53/53. Calendar and event suites passed 35/35 when run independently. Two combined runs each had the existing saturation assertion observe `CALL_OUTCOME_UNKNOWN` instead of `BROKER_SATURATED`; the fail-closed result and isolated passing reports are retained in the S46 evidence directory. Library comparison did not establish full warmup/seed parity, so no dependency was added.

## Open qualifications

- Fake Bybit source/supervisor fixture: **TEST GATE**; `INCOMPLETE_SNAPSHOT` and noncausal dependency `ValueError` remain unresolved.
- TQ-01 injected stream stall: **TEST GATE**; 480/512 queue occupancy and one rejected frame; S45 80/120 frames/s attempts produced no valid sustained drain metrics.
- Real Binance final-M15 delivery within close+5s: **UNVERIFIED**.
- Linux F1/A2 sustained capacity: **TEST GATE**.
- Authenticated/native Binance protection and capital remain unqualified; capital and assisted execution stay disabled.
- Combined calendar/event saturation assertion: **TEST GATE**; isolated suites pass, while both combined runs fail closed with an unexpected outcome classification.
- Existing `TECHNICAL_V1` on sparse bars: **TEST GATE** for future gap-sensitive use; current calculations accept sparse input without a gap label.
- Economic value: **NOT ESTIMABLE**.

Do not infer source latency, capacity or economics from deterministic fixtures. S46 makes no live diagnostic, credential, capital, or promotion-ladder change.

## Previous completed implementation — S45

S45 fixed immutable `content_hash` selection in due-work discovery. Its production outcome batch passed 61/61, S33 maturity passed 15/15, durable capture passed 25/25, combined event/calendar passed 35/35, and V1 golden and S44 compatibility each passed 1/1. These are historical results. See the [S45 factual handoff](docs/v2/ATLAS_TRANSITION_S45_FACTUAL_HANDOFF.md) and its [evidence manifest](docs/v2/transition-s45-evidence/evidence_manifest.json).

## Active entrypoints

Package scripts are declared in [`pyproject.toml`](pyproject.toml): `atlas-product`, `atlas-v2-projection`, `atlas-desktop`, `atlas-v2-public-qualification`, `atlas-v2-public-soak`, `atlas-ops`, `atlas-agent-worker`, and `atlas-agent-broker`. Runtime modules and their boundaries are listed in [`docs/INDEX.md`](docs/INDEX.md). Their presence does not authorize capital or assisted execution.

## Historical evidence

S41 recovery, TQ-01 qualification, S44 close-driven M15 and S45 outcomes remain discoverable in [`docs/INDEX.md`](docs/INDEX.md), `docs/v2/`, root logs, and the retained evidence directories. Historical failures and frozen V1 comparator artifacts are preserved with their original scope and identities.

`requirements-lock.txt` is the installation lock used by the recorded S45/S46 environment; it pins DuckDB 1.5.5. `uv.lock` resolves DuckDB 1.5.6. S46 records this discrepancy without regenerating either lock.
