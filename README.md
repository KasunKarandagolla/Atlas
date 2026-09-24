# ATLAS — stabilized V1 and V2 transition audit

ATLAS is a private research and execution-control project. The current V1
checkpoint is the stabilized Phase 0–6 baseline from Session 009:

`impl/session-009-v1-phase0-6-stabilization` at
`146bae0a2f10e2f794cbed3441123071c55a2baf`.

Session 010 is the mandatory V2 Phase 0 exit audit. Its dedicated checkpoint
branch is `impl/session-010-v2-transition-audit`. It records what the frozen V1
code and offline tests establish, preserves machine-readable V1 golden values,
and documents additive V2 seams. It does not implement V2 Phase 1 or Phase 2.

## Authority and evidence

The governing documents are:

- `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` —
  authoritative for existing V1 behavior and safety contracts.
- `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md` — the
  supplied additive V2 implementation authority used for the transition audit
  (provided in the invoking workspace; SHA-256
  `bae3e1a9e48aec64d1292e5bc791c2e87949ed33f4124b1f9807b589cc07a484`).
- `docs/v2/V1_EXIT_AUDIT.md` — current Phase 0 findings and status matrix.
- `docs/v2/V1_GOLDEN_BASELINE.json` — deterministic V1 transition values.
- `docs/handoffs/session-009.md` and `docs/handoffs/session-010.md` — checkpoint
  evidence and handoff records.

V1 freezes BTCUSDT and ETHUSDT as its strategy and capital universe, uses the
Bybit linear one-way isolated profile, and keeps NautilusTrader as the normal
order/fill/position engine. ATLAS retains durable intent, approvals,
reservations, risk, recovery and audit authority. V2 additions must be
versioned and must not silently change these V1 contracts.

## Current operating status

- All six authenticated Bybit capability states remain `UNVERIFIED` and are
  `TEST GATE` items.
- `assisted_enabled=false`; no live or test order was submitted for Session
  010.
- No profitability or economic-validation claim is established; that status is
  `NOT ESTIMABLE`.
- The Session-009 offline suite is rerun and recorded in the Session-010 audit.
  Offline tests do not qualify exchange capabilities.

The GitHub default branch may still point to the older
`impl/session-001-foundation`. It is stale and non-authoritative. Implementation
checkpoints are identified by their approved branch and exact SHA; do not infer
the current baseline from the default branch.
