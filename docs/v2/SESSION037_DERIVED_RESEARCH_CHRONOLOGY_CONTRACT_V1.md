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
