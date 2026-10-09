# ADR S41-01: S6 shadow action contract

**Status:** Approved for additive engineering by the Session 041 owner directive
to complete the authorized S1–S8 research candidate surface. Independent review
of the integrated implementation remains required.

**Authority:** Amended V2 freeze §§17, 23.2 and 23.3; Session 041 owner request.
This ADR does not amend the V1 safety freeze, capital policy, or agent authority.

## Old contract

`S6ResearchPolicyV2` computes the frozen 30-day cross-sectional relative-strength
rank, persists top/bottom decile trend-aligned hypotheses, and waits for a
subsequent confirmed 15-minute close through the prior 15-minute high/low. The
accepted Session 023 checkpoint explicitly left the stop, action, and holding
horizon undefined. A confirmed trigger therefore remained
`NOT_ESTIMABLE_EXACT_ACTION_CONTRACT` and could not enter `CandidateSetV2`.

## New contract and version

Keep `S6ResearchPolicyV2` and every existing state, hypothesis, watch, trigger,
and candidate identity unchanged. Add a separate, content-addressed
`S6_CROSS_SECTIONAL_RELATIVE_STRENGTH` `PolicySpecV2`, version
`2.0.0-shadow-single-action`. The existing multi-sleeve selection contract is
versioned from `1.0.0-research` to `1.1.0-research` because S6 joins the exact
candidate competitor set. It may produce only an unsized `CandidateActionV2`:

- Signal, breadth, ranking, trend alignment, and subsequent 15-minute trigger
  remain exactly as defined by the existing S6 rank policy.
- Require the exact S6 hypothesis/state and point-in-time row; three latest
  completed hourly bars through the latest completed H1 at the S6 hypothesis
  cutoff; the latest completed M15 at or immediately before that cutoff and its
  immediately subsequent confirmed trigger bar; exact ATR14 feature; exact
  full-key executable BBO, mark/index, current clear deterministic event gate,
  and a cost-contract reference. Cost evidence may be explicitly
  `NOT_ESTIMABLE`; the reference does not assert estimated economics.
- Entry reference is the fresh ask for a long and bid for a short. The action is
  LIMIT/IOC with a 5 basis point adverse collar and no same-epoch repricing.
- Long stop is the minimum low of those three hourly bars minus 0.25 ATR14.
  Short stop is the maximum high plus 0.25 ATR14. Reject a stop distance outside
  0.5–3 ATR14. Round only through the existing product tick/price rules.
- The action decision cutoff is the exact Ops event cutoff at which the trigger
  and required current evidence are available, bounded to the trigger close
  plus five seconds. The candidate publication deadline is five seconds after
  that decision cutoff;
  maximum holding period and fixed-stop time exit are four hours. No trailing,
  widening, partial take profit, pyramiding, averaging, or repricing is allowed.
- The existing one-next-15-minute-bar hypothesis watch expiry is unchanged and
  remains separate from candidate publication deadline and holding horizon.
- Missing, stale, future, contradictory, or wrong-revision inputs produce a
  named `NOT_ESTIMABLE`/`NO_CANDIDATE` result. They never produce a fallback
  price or stop.

All values are versioned in the new action policy. Changing any action value
requires a new action-policy version and hash.

## Reason and safety impact

The amended V2 freeze requires S6 top/bottom-decile trend-aligned candidates and
includes S6 in the complete `CandidateSetV2` examples, but it does not specify
an exact stop or holding horizon. This smallest additive contract reuses the
S1-style confirmed-close trigger, collar, stop buffer, stop-distance guard, and
time horizon while using an S6-specific three-hour-bar structural anchor.

The output remains `SHADOW_ONLY`, quantity is always `None`, and selector and
capital authority remain zero. Risk remains the only sizing owner. The change
does not enable capital, assisted execution, or a new live sleeve.

## Migration and replay compatibility

Existing S6 hashes and artifacts are immutable. No old trigger is retroactively
promoted. The legacy `S6ConfirmedTriggerV2` remains unchanged for replay. New
candidate identities bind an `S6ActionTriggerEligibilityV1` reference and the
action-policy hash, exact decision cutoff, S6 hypothesis, cross-sectional
state, trigger bars, feature/ATR evidence, quote, mark/index, event gate, cost
reference, and full instrument revision. Old runs continue to
replay under their original research-only contract. The new policy is eligible
only for decision cutoffs after its implementation is configured and all exact
inputs are available.

## Validation

Focused tests cover long and short action construction, exact chronology and
identity, stale/future/missing evidence, stop-distance rejection, deterministic
identity, and unsized/zero-authority behavior. Integrated CandidateSet,
selector, production, S1–S3 regression, and independent authority review are
required before this policy is considered engineering-tested.

## Approval state

`APPROVED_FOR_SHADOW_ENGINEERING_BY_SESSION041_OWNER_DIRECTIVE` — the owner
explicitly required the complete authorized S1–S8 research candidate surface
and authorized additive versioned amendments when needed. This is not approval
for economic promotion, decision influence outside the frozen selector, or
capital use. Integrated independent review and all normal live qualification
gates remain pending.
