# ATLAS R1 versioned amendment proposal

**Status:** proposal only. No owner acceptance, frozen-contract change, capital use, or trading authority is implied.

This proposal records the two audit findings whose complete repair changes persisted artifact meaning or the risk-policy consumer chain. The R1 implementation keeps those boundaries explicit and fails closed where its current evidence seam permits.

## AUD-01 — funding inside hard sizing

### Finding

The current hard sizing bound consumes entry-to-stop/stress loss and taker fees. It has no candidate-level adverse funding bound. Funding settled later in economic scenarios cannot retroactively constrain the persisted `SizingDecisionV2.quantity`. The audit found no bounded funding-rate evidence contract that can support this calculation today.

### Proposed versioned contracts

Add a typed `FundingRiskBoundV1` with, at minimum:

- exact instrument key and position side;
- information cutoff, creation, availability, and candidate deadline chronology;
- approved holding horizon and all settlement instants within that horizon;
- schedule reference and source reference;
- a conservative adverse per-unit cash bound at each settlement;
- an explicit completeness state distinguishing a known zero bound from missing or unbounded evidence.

Missing or unbounded evidence must yield `NOT_ESTIMABLE`. Zero funding may be emitted only when the schedule and rate evidence establish no adverse settlement inside the approved hold, or establish a bounded zero rate. A forecast mean or realized funding history is not a conservative bound.

Introduce a new risk-policy version and `SizingDecisionV3`. The V3 decision should persist the funding reserve as a distinct component and include its exact evidence references. The reserve must reduce allowable quantity before a decision is sealed and carried into the durable reservation. Later economics must reconcile the same funding amount exactly once; it must not add the funding charge again on top of a sizing reserve that already represents it.

### Required consumer migration

Before use, inventory and version the consumers of `SizingDecisionV2` across candidate admission, action/replay, persistence and restart, outcomes, chronology receipts, risk projections, and desktop read models. Keep V2 artifacts readable and retain their original hashes. Do not silently reinterpret old V2 quantities as funding-aware. The new policy must reject an unsupported legacy decision on any path that requires the new funding guarantee.

### Acceptance tests

- deterministic adverse funding increases the per-unit bound and lowers allowable quantity;
- absent, late, or unbounded funding evidence produces `NOT_ESTIMABLE`;
- funding is reconciled exactly once through sizing, reservation, restart, and later economics;
- a reservation retains its funding evidence and bound after restart;
- V2 historical reads and hashes remain unchanged;
- all capital, assisted, provider, and order gates remain closed.

## AUD-06 — complete V1 feature identity

### Finding

V1 `FeatureSnapshot.snapshot_hash()` omits returns, variance, and the complete source payload. Distinct return histories can therefore share a digest when the existing summary values and source IDs match. R1 now rejects blank/reused source IDs and impossible source chronology, but intentionally leaves the frozen V1 digest shape unchanged.

### Proposed identity migration

Add a separately versioned `FeatureSnapshotIdentityV2` that hashes the canonical complete feature payload, decision slot, computation time, per-source availability, immutable source-content references, strategy ID, and strategy version. Keep `snapshot_hash()` as the legacy V1 digest for historical plans, replay, and phase-4 records. New V2 identities must not be substituted into old persisted references.

Inventory every plan/calendar/replay/persistence consumer before adopting the new digest. Provide an explicit dual-read path for historical V1 snapshots and a versioned comparison rule; do not re-key or rewrite historical records in place.

### Acceptance tests

- equal V1 summary values with distinct return paths produce distinct V2 identities;
- identical canonical payloads serialize to the same V2 digest across restart;
- blank and duplicate source IDs remain rejected;
- V1 golden plan and replay hashes remain unchanged;
- persisted V1 plans remain readable through the explicit compatibility path.

## AUD-02 — long-lived active-history cadence proof

R1 qualifies direct feature frames, makes visible interval gaps unavailable, fails S1 watch/trigger paths closed on joined-frame gaps, versions the feature set, and requires a gap-qualified indexed feature artifact before S1 hard sizing. The persisted `ActiveCausalHistoryStateV1` still retains only a bounded tail and does not persist a proof that the entire recursive prefix was gap-free. A gap older than that retained tail cannot be established from the current state contract.

Before relying on long-lived exact-prefix recursive indicators, add a new versioned history coverage receipt/state field that carries cadence completeness across bounded advances and restart. Legacy history state without that proof must remain unqualified until it is rebuilt from sufficient source evidence or re-warmed. Preserve the old history artifact meaning and hashes. Test a gap that falls outside the retained tail, a restart across the gap, re-warm boundaries, revisions, and a contiguous prefix against the frozen golden outputs.

## Approval boundary

The AUD-01 and full AUD-06 contracts, their consumer migrations, and the AUD-02 persisted-history coverage version require owner review before implementation. This document is a concrete proposal only; it does not authorize those migrations.
