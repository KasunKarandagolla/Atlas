# Derived research chronology contract V1

Contract: `DerivedComputationChronologyV1`. Status: IMPLEMENTED; final validation
and independent coordinating review are recorded in the S37 closure ledger.
Authority: ZERO. This is additive research evidence, with no change to capital,
RiskPolicy, frozen strategy definitions, existing serialization or approval.

V2 §6.1 requires each computation's immediate input availability to precede its
information cutoff and computation start. The agent freeze §9 also distinguishes
the original market prefix from later derived assessment publication. A market
cutoff must therefore remain separate from a downstream stage's input cutoff.

The closed receipt schema contains:

- `version`, exact `artifact_ref`, `artifact_content_hash`, `artifact_type`;
- `market_information_cutoff_ns`: the original immutable raw-information prefix;
- `information_cutoff_ns`: the maximum of that market cutoff and the actual
  publication timestamps of every immediate declared input;
- `computation_started_ns`, `computation_finished_ns`, `available_at_ns`;
- original `consumer_deadline_ns`, derived boolean `consumer_eligible`;
- exact `input_refs`, `authority` = `ZERO`.

Required local ordering is:

```text
max(immediate input.available_at_ns)
    <= information_cutoff_ns
    <= computation_started_ns
    <= computation_finished_ns
    <= available_at_ns
```

The stored input cutoff must equal its recomputed value, not merely bound it.
Raw dependencies must be available by `market_information_cutoff_ns`. A later
derived dependency must have a valid receipt over that same market prefix and
must precede this stage's start and original deadline. Recursive validation uses
a shared fixed lookup budget and rejects cycles, incomplete references, unknown
fields, altered cutoffs, conflicting hashes and chronology regression. A raw
observation cannot receive a derived receipt. Formal target dependencies are
unioned into validation so a receipt cannot hide an input.

For example, raw evidence available by 10 can produce a feature at 13. A candidate
using that feature has market cutoff 10, immediate-input cutoff 13, start 14,
finish 15 and publication 16. Its market evidence remains the prefix at 10. It
cannot consume a raw observation learned at 12. Neither feature nor candidate is
published at 10. The candidate's policy origin and original expiry stay sealed.

Late outputs may remain audit evidence with `consumer_eligible=false`; consumers
cannot use them to reopen an expired decision. Restart reuses the exact original
published result and receipt rather than assigning a new earlier timestamp.

The report retains local computation time and market-to-publication latency.
Raw references remain reconstructable. A feature whose raw input follows its
market cutoff is invalid even when that input precedes feature publication.

Tests: `test_session037_chronology.py`, `test_session037_tuning_analysis.py`,
`test_session037_training_bounds.py`, and production composition/restart tests.
This contract is not a replay availability exception and does not turn later raw
receipt into historical knowledge.


Economic composition uses the same additive receipt. `EconomicSourceManifestV1`
is a strict, zero-authority, cutoff-known declaration scoped to the full
instrument/product revision, account scope and policy hash. It names immutable
model/calibration/execution sources and the admission configuration. Its fixed
seed method binds the exact action hash and market cutoff. Missing, ambiguous,
foreign, future or overflowing declarations cannot select a global latest model.
No account, fee, source or qualification evidence is supplied implicitly.

`OpsEconomicEvidenceResolutionV1` is computed only after the candidate, sizing
and exact action exist. Its later publication binds the declared manifest and
exact decision/action identities with the original market prefix and deadline.
Restart reuses that publication; later declarations cannot revise the decision.
Bare post-cutoff legacy role wrappers remain unusable without this causal path.

Explicit cutoff-known `JointExecutionDataV2` may be rebound only when its semantic
action hash, requested quantity, cutoff, execution model and fee identity already
match exactly. Binding changes only `action_artifact_ref` and actual computation /
publication timestamps. Prices, depth, fill assumptions, latency, funding,
management, qualification and fixture markers remain identical. The validator
reproduces this equality against the original template; a receipt alone cannot
license altered scientific facts. Support binding changes only the template ref
and publication time, preserving source episode/window/bundle and compatibility.
This is exact identity binding, not a template generator or compatibility rescue.

The evaluator consumes declared joint sources/support, execution residuals,
stress and synchronized existing-portfolio valuations rather than substituting
empty populations. Missing support stays `NOT_ESTIMABLE`. An evidenced flat
account yields zero existing-portfolio value, not fabricated candidate payoff.
Scenario and portfolio-completeness publications receive actual later receipts.
Two fixed independent scenario seeds can measure numerical convergence when the
model and real template evidence support it; they create no new market samples.

Work is capped at 128 source declarations, 128 requested references, 16,384
points per execution template and the declared fixed provenance limits. Whole
population overflow refuses inference and source-manifest overflow publishes
pressure evidence. All contracts here are additive research evidence; existing
V1/V2 serialized wires, capital gates and RiskPolicy remain unchanged.
