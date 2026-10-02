# Session 036 analogue compatibility amendment proposal

Status: **UNVERIFIED**. Classification: **REQUIRES_VERSIONED_AMENDMENT**.
This proposal does not authorize a contract change. Production invokes the
existing bounded retrieval API and preserves its current fail-closed policy.

The frozen compatibility contract hashes absolute `horizon_end_ns`, entry/stop
price levels, quantity and exact liquidity evidence identity. Liquidity includes
its source artifact and decision cutoff. Genuinely historical actions therefore
generally cannot share a query key even where relative policy mechanics and
observed states are comparable. Empty historical support does not itself
establish failed retrieval or an economically useless strategy.

Evidence: `science/analogue.py`, `action_semantics_hash()`, `_liquidity_evidence()`,
`build_analogue_compatibility()` and `_validate_action_compatibility()`. Existing
`ANALOGUE_POLICY_HASH`, `ANALOGUE_FROZEN_ACTION_SEMANTICS_V2` and associated query,
compatibility and result identities must not be silently redefined.

Proposed reviewed version: `ANALOGUE_RELATIVE_COMPATIBILITY_V3`. Retain exact
action/candidate/instrument revision, risk policy, cost vintage and raw liquidity
refs. Define compatibility using reviewed mechanics and relative horizon duration.
Retain price, stop distance, size, participation, spread and depth as typed
features with units/missingness. Any normalization needs reviewed units and target
transformation. Do not treat missingness as zero, pool incompatible cost/execution
semantics, alter exact actions or grant admission authority. No defaults are supplied.

Migration is additive: retain V2 artifacts/reports, publish V3 under new identities
and register variants before new runs. Compare the same cutoff-known stream and
cutoff-matured labels, preserving embargo/dependence. Do not reclassify historical
failures. Rollback selects V2 for a new run; it cannot silently switch an active run.

Review/tests must cover action binding, cross-date relative horizon compatibility,
unit/target equivalence, incompatible instruments/costs/mechanics, availability
cutoffs, matured labels/embargo, missingness, duplicate episodes, bounded overflow
and unchanged risk/capital authority. Until review and implementation, historical
usefulness remains **NOT ESTIMABLE** under the frozen contract.
