# S37 engineering and intelligence design V1

Status: `TESTED` at code/offline-validation level after principal integration
review and complete final-37 validation. This document describes implemented seams
and their intended research value. It is not an independent acceptance verdict, an amendment
to frozen authority, a validation result, or authorization for a live campaign.
The principal closure ledger and handoff own those decisions and their evidence.

## Product composition

The design extends the accepted S36 product rather than adding another runtime,
writer, execution authority, or analytical database. Public observations enter
the existing archive and immutable artifact index. The production composition
constructs features and frozen strategy candidates, retains selection and
rejection evidence, and records exact decision calendars. Supported risk and
economic inputs can lead to a frozen research action and action-level evaluation;
missing prerequisites terminate that path with explicit evidence.

The installed `run_component` connects public-context maintenance, exact-prefix
history maintenance, statistical postreceipt research, prediction maturation and
action-outcome maturation to the existing supervisor. Maintenance and completed
worker publication use the supervisor's one repository writer. A fetch worker
returns a bounded completion slot and does not write research evidence itself.
Smoke operation selects the offline port and does not start the public fetcher.

Implementation anchors:

- `src/atlas/v2/product.py`: `run_component`, `post_cycle`, `_ResearchShadows`.
- `src/atlas/v2/runtime/production.py`: `_compose_public_event_inputs`, exact
  prerequisite resolution, strategy/watch integration and decision calendars.
- `src/atlas/v2/memory/repository.py` and `schema.py`: immutable artifacts plus
  additive, rebuildable active-work projections and atomic composition.

## First-test intelligence method

The method preserves the accepted separation between deterministic opportunity
selection, numerical action economics and optional semantic criticism. Each
component answers a different question; none acquires capital authority.

| Component | Information contributed | Failure or abstention boundary | How usefulness can be examined |
| --- | --- | --- | --- |
| Frozen S1/S2 logic and causal features | Reproducible opportunity identity, proposed policy and market context | Missing history, unavailable source, invalid chronology or incomplete setup | Origin accounting, stage funnel, feature values and missing reasons, exact candidate/calendar identities |
| M0 Huber/ridge action-value estimator | Transparent action-aligned expected net value, uncertainty, support and chronological calibration | Insufficient eligible matured outcomes, calibration/support failure, OOD state or work-budget refusal | Exact-action predictions, support/calibration artifacts, later governed action outcomes |
| M1 LightGBM challenger | Registered nonlinear alternative on the declared action-aligned feature subset | Missing dependency/lock, insufficient support, failed fitting or work-budget refusal; no replacement provider | Same-action M0/M1 disagreement, immutable method/configuration hashes and outcome coverage |
| Joint economic/scenario evaluation | Cost, execution, uncertainty, stress and existing-portfolio consequences where supported | Missing exact execution, fee, stress, portfolio or venue evidence | Evaluation artifacts, prerequisite inventory, action payoff linkage and unresolved reasons |
| Installed fixed empirical return diagnostic | A causal price-forecast control with registered manifest, route, input hash and quantiles | Original deadline expired, missing causal context or unresolved future endpoint | Prediction error and interval coverage by target, horizon, route and instrument |
| Configured accepted DeepSeek action critic | Closed-taxonomy semantic criticism after an exact frozen packet | Disabled profile, unavailable provider, failed schema/conformance or original deadline failure | Critic observations, terminal failures, maturity links and provider/model identities |

M0/M1 target exact eligible action-level after-cost evidence. The installed
`empirical-horizon-return-v1` diagnostic targets price log return. These targets
remain separate: a correct return forecast does not establish a fill, executable
policy payoff or economic qualification. M1 retains the accepted small declared
configuration search and deterministic selection policy; S37 does not create a
new arena, automatic model promotion or adaptive provider router.

The design is defensible for a first prospective test because deterministic
selection, explainable numerical estimation, a registered nonlinear challenger
and independent price diagnostics expose different possible errors. Its strength
must be tested with actual supported evidence. On a fresh public-shadow run,
private account and execution facts are ordinarily absent, so the action-economic
path can legitimately abstain while source, feature, opportunity and price-label
diagnostics accumulate. No cold-start coefficient rescue, fabricated fee or
synthetic fill makes that path appear ready.

Likely weaknesses include sparse eligible action labels, shared and overlapping
market history, changing regimes, incomplete semantic mapping, source gaps,
conservative execution ambiguity, delayed computation and bounded-work pressure.
Reports expose these limits; they do not convert them into independent samples.

Chart vision, ensembles, specialist routing, fusion and adaptive management were
not added merely because they were discussed. Structured bars, source chronology,
technical context and raw-linked news already provide testable information.
Unique incremental information from a visual or additional model is not
established here. The existing analogue proposal remains a separate amendment
question; this design does not silently promote it into accepted economics.

## Chronology and exact-prefix strategy compatibility

`DerivedComputationChronologyV1` binds an exact artifact and content hash to its
sealed `market_information_cutoff_ns`, immediate-input `information_cutoff_ns`, actual computation start/finish, publication time,
declared dependencies and original consumer deadline. Dependencies are checked
recursively with a shared fixed lookup budget. Raw inputs published after the
market cutoff cannot be laundered through a derived receipt. A late publication
can be retained for audit while remaining ineligible for the decision consumer.
Candidate-set and hard-risk calendars use actual source publication rather than
backdating to the market cutoff.

The immediate-input cutoff is exactly the maximum of the market cutoff and all
declared input publication times. It precedes computation start, so every local
computation preserves the V2 §6.1 ordering. Later derived inputs must prove that
their raw evidence belonged to the original market prefix. A downstream stage
does not obtain fresh market information merely because a prior transformation
finished later. Both cutoffs are hash-bound; consumers reject unknown receipt
fields, rehashed cutoff substitutions and missing dependencies. This additive
receipt contract leaves existing frozen artifact serialization and policy hashes
intact. See `SESSION037_DERIVED_RESEARCH_CHRONOLOGY_CONTRACT_V1.md`.

The active history state preserves EMA20/EMA50 and Wilder ATR14 recurrence from
the original causal prefix. It stores exact recursive seeds, a bounded tail,
source locators, total-prefix count and observed UTC-day count. Every middle-tail
item is covered by the tail hash; the mutable head must reproduce its immutable
certificate. Advancing is append-only. A visible earlier source revision triggers
a bounded rebuild from the original source, with explicit pending pressure.
Future revisions and newer caches cannot answer an earlier historical cutoff.

The strategy compatibility objective is mathematical equivalence to the original
full-prefix S1/S2 calculations, while retaining frozen policy identities and
capital semantics. A mismatched or future supplied history is refused rather
than replaced with a shorter seed. The tests compare full-prefix watches,
candidates, setup statistics and economics against exact checkpoint tails.

This guarantee does **not** describe every diagnostic feature. The additive
`INTRADAY_CORE_EXACT_PREFIX_EMA_ATR_V1` feature version substitutes exact-prefix
EMA/ATR values. RSI, MACD, ADX, variance, structure and other existing diagnostics
still use their declared finite context. The legacy feature version remains
distinct. Missing external regime axes and unsupported trade/anchor features
retain null values and reasons rather than plausible inferred states.

Anchors: `chronology.py`; `data/active_history.py`; `runtime/active_history.py`;
`features/pipeline.py`; `strategies/s1_trend.py`; `strategies/s2_breakout.py`.
Focused specifications are in `test_session037_chronology.py`,
`test_session037_active_history.py`, `test_session037_history_runtime.py`,
`test_session037_strategy_prefix.py` and `test_session037_feature_prefix.py`.

## Prerequisites, action outcomes and lifecycle evidence

`ResearchPrerequisiteInventoryV1` publishes exact supported product, policy,
account, fee, stress, venue and execution-model evidence, and a bound event gate.
Every unavailable role has a typed status and reason. Supplied facts must already
exist with matching content, product/account scope and cutoff-visible chronology.
The same event/product cannot acquire revised prerequisites on restart.
Public news alone does not manufacture calendar coverage or a clear event gate.

`RetrospectiveActionOutcomeProducerV1` is connected to the maturation coordinator.
It requires sealed `ActionReplaySourceEvidenceV1` linking the decision, exact
frozen action, product, existing portfolio, replay assumptions, fees, funding and
per-minute/raw evidence. It invokes the existing replay contract and fill math.
Assumptions and cost declarations must precede the decision; later outcome
computation receives actual later publication time. Conflicting sources/payoffs,
future setup facts and insufficient execution evidence fail closed.

Supported replay produces `PolicyPayoffV2` and an immutable
`ActionReplayLifecycleSummaryV1`. The summary preserves entry and exit times,
exit reason, filled and remaining quantities, execution status, payoff reference
and exact source evidence. Replay evidence retains the underlying path and
policy state changes. Tuning joins this chain back to candidate, sizing, feature,
calendar and prediction evidence. Closed supported paths expose net payoff,
fees, funding, time in trade and descriptive net ROI on the exact frozen sizing
margin. Open, unresolved and unsupported cases carry null economics where
needed; missing economics are not zeros.

This supports research into recognition, judgment, entry/fill assumptions,
fixed-policy lifecycle, latency and cost drag. It does not implement a new
discretionary hold/reduce/close controller, dynamic TP/SL or adaptive capital
management. A BBO sample alone does not prove depth, complete last/mark ranges or
an executable fill. Public-source incapability remains visible. ROI is a
descriptive supported replay quantity, not profitability evidence or a leverage
policy. No 10× capital floor is inserted.

Anchors: `runtime/research_prerequisites.py`, `runtime/action_outcome_producer.py`,
`runtime/outcome_maturity.py`, `science/replay.py`, `science/outcomes.py` and the
prerequisite, action-producer and lifecycle-projection S37 tests.

## Bounded active work and restart behavior

The following are coded ceilings and scheduling allowances, not measured Windows
latency or endurance qualification:

| Work | Bound / response |
| --- | --- |
| Exact history advance | At most 128 source rows per key/frame turn; 500 ms installed round-robin cadence; at most 128 products |
| Retained history tails | M15 2,902 bars; H1/H4 256 each; M1 10,081; full-prefix EMA/ATR seeds retained separately |
| Native origin discovery and bar-gap repair | Persisted progress in pages of at most 128 source rows; incomplete work remains pending |
| Public S3 context | Exact key/type and fixed seven-day context plus warmup; 16,384-row whole-population cap; overflow pressure |
| Active typed evidence inventory | 4,096-entry ceiling with explicit whole-population refusal on overflow/invalid rows |
| M0/M1 fitting | At most 512 fit rows and 4,096 indexed input entries; refuse the whole fit rather than silently train on a newest-page subset |
| Action replay source | At most 256 minutes and 1,024 raw references; supplementary evidence separately bounded |
| Action maturation | At most eight inspected/attempted decisions per turn; per-decision read/page bounds; installed 200 ms allowance within supervisor's 250 ms maintenance setting |
| Prediction maturation | At most eight terminals and eight horizon work items per turn; one-minute retry cadence |
| Public context | One fetch worker and one completion slot; fixed round-robin 60-second cadence; 2-second transport timeout; bounded parsing |
| Public quote receipt | One exact receipt per requested kind; full-key/revision/class equality seeks before raw reconstruction |
| Creation-ordered identity query | Exact indexed creation order; inspect at most 1,024 candidates; excessive unavailable prefix explicitly refused |
| Generic typed pages | At most 128 namespaces and 16,384 raw candidates per namespace; exact creation-order seeks; excessive unavailable prefix explicitly refused |
| Origin checkpoint generation | Exact instrument/generation indexes; inspect the latest two rows for conflict |
| L2 restart inventory | Three hierarchical expression-index successor seeks per stream, then one latest receipt; distinct-stream cap, no retained frame partition sweep |
| Native S3 rejection identity | Exact indexed decision-event lookup of at most two CandidateSet/index rows; conflicts or additional rows refuse reuse |
| Installed watch recovery | Ordered seeks for each active state, globally merged; inspect at most 513 watches and refuse an active population above 512 |
| Watch status and transitions | Indexed creation/transition order, bounded requested page; preflight marks the original capped inventory incomplete before filtering by product |

Due-work tables separate pending horizons from terminal retained history. Terminal
work is retired, malformed work quarantined, and unresolved work rescheduled with
its original scientific identity. Discovery cursors, history heads, native-origin
projections, collector cursor heads and bar-repair heads persist progress across
restart. Atomic composition rolls back watch transitions, artifacts and due-work
updates together. A partially discovered origin page does not advance the
immutable accounting close cursor merely because source discovery progressed.

Pressure evidence includes backlog, overflow, quarantine/retirement counts,
oldest due age, cursor, exact key/frame, history count and readiness. The design
retains historical evidence without requiring active routines to revisit every
terminal record. A reconnect repair must cover the declared gap from a previous
healthy boundary; a contiguous later tail does not prove a missing middle bar.
Completed immutable repair certificates are bound into recovery evidence.
None of this proves Bybit trade completeness for S3.

Hourly unchanged product metadata receipts are retained, but composition selects
one latest effective cutoff-visible contract per full instrument key. Equal
effective-time conflicts abstain. Event gates are reevaluated for that exact
event/product from bounded typed calendar, abnormality and incident facts; a
different product's CLEAR gate cannot replace missing or blocking evidence.

An interruption after payoff persistence and before lifecycle publication can
resume: the lifecycle receipt binds the sealed replay source availability, while
its publication uses the actual later restart clock. It does not invent the
earlier payoff's unavailable computation-start timestamp.

Relevant tests cover bounded migration, new-origin due scheduling, retirement,
restart, source-duplication query plans, same-clock origin revisions, missing
repair bars, checkpoint tampering and read-only observation during writing.

## Tuning during an active run

The authoritative compact tuning dataset remains validated Parquet partitions
with immutable checksum manifests, raw artifact references and source-row cursor.
The export opens SQLite read-only under a snapshot; it performs no provider
request and opens no second operational writer. Incremental insertion-order
export includes later outcomes without reconstructing the whole raw feed.
Interrupted temporary/orphan files are not dataset members.

Each export processes at most 100,000 rows with bounded batches and a default
10-second snapshot allowance. `has_more` and future-row blocking remain explicit.
The disposable DuckDB report uses one thread, 128 MB memory, no persistent
analytical store, a time interrupt and at most 128 recent manifested partitions.
Older retained partitions remain reconstructable; a recent-window report marks
omitted history and reports its row/time range. Group truncation is also visible.

The compact evidence can answer:

- Which registered origins reached selection, sizing and maturation, and which
  failed with recorded reasons? Unregistered expected origins remain unknown.
- Which feature values, schemas, regime axes or missing inputs accompanied a
  decision? Stability summaries are descriptive, not drift-significance claims.
- What did M0 and M1 say about the **same** exact action and registered method
  configuration? Unpaired/missing estimates remain visible.
- Where did recorded stage completion and publication latency accumulate?
  Stage intervals include queue/other work and are not isolated compute time.
  Derived receipts additionally group measured computation/publication latency
  by their exact target type, separating feature, candidate, sizing and action.
- Which routes produced requests, terminals and labels? Which predictions had
  errors or quantile-coverage failures by horizon and instrument?
- Was lifecycle net payoff/fee/funding/margin ROI supported, unresolved or
  missing, and how long was the exact action exposed?
- Which history, prerequisite, recovery or resource lane is under pressure?

Identity includes run/configuration/source, feature and policy versions, exact
action hash, method configuration, route/manifest and dependency lock where
applicable. Shared origins and overlapping labels remain dependence-sensitive.
No automatic switching, hidden tuning, future-label adaptation or final-holdout
access is introduced. The first short run is diagnostic; the report keeps
economic significance `NOT ESTIMABLE`.

Anchor: `science/tuning_export.py` and
`tests/v2/test_session037_tuning_analysis.py`, together with accepted S36 export
tests. Human-readable reports are convenience products of the structured store.

## Windows and acceptance boundaries

The design preserves S36 bundled runtime, pinned dependencies, installer,
first-run/run selection, safe evidence location, source/package identity,
DPAPI provider-secret storage and public operation without a provider secret.
It connects maintenance in the installed lifecycle rather than requiring the
owner to invoke Python, Git or direct SQLite queries. Capital and assisted
execution remain disabled; model and critic authority remains zero.

This draft makes no claim that native Windows packaging, fresh-owner hardware,
48-hour endurance, live-source completeness, provider conformance or prospective
economics have passed. Final test execution, authority/hash preservation,
repository ancestry, installer evidence, secret scan and final blocker verdict
must be recorded by the principal in the closure ledger and handoff. Longer
prospective evidence and separately authorized capital qualification remain
governed gates, not automatic consequences of an engineering pass.


The final composition audit reproduced an ordinary economic wiring defect:
exact-candidate prerequisite wrappers were required before the market cutoff,
although the candidate identity is honestly published later. The scoped
`EconomicSourceManifestV1` declares cutoff-known sources/configuration; production
creates a later `OpsEconomicEvidenceResolutionV1` after freezing the action.
Explicit exactly compatible joint execution templates are rebound without
changing any scientific fact. Real M0 fitting and declared scenario/support,
execution uncertainty, stress and portfolio evaluation are connected. Missing
sources and support still abstain. This removes a software impossibility; it
provides no qualification, profitability or arbitrary new action-template model.
The chronology contract records the exact additive binding and refusal rules.

The source-declaration reader refuses more than 128 raw members/provenance refs or 16,384 path points before typed parsing. Later-feature admission uses the original-cutoff receipt. Economic cashflows are computed before sealing, and evaluation validation finishes before its actual publication clock; the terminal calendar shares its atomic publication. These changes preserve scientific amounts, frozen serialization and capital authority.
