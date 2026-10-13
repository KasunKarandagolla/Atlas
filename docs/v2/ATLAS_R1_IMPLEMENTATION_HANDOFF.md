# ATLAS RED R1 implementation handoff

**Disposition:** targeted correctness repairs are implemented in an isolated branch. This is not a release, capital, or trading qualification. AUD-01 funding-aware hard sizing and the versioned full-identity migrations remain open gates.

## Checkpoint

- Branch: `impl/red-r1-surgical-correctness-2026-10-13`
- Accepted S49 remote tip: `a73c92d83ca8b2fe5163e636b2c9732361b35e23`
- S49 implementation ancestor verified on this base: `00ed85f9dae78b29989e442dfb0d70576671ff48`
- The prior S49 coordinator note recorded a different `00ed85fb...` identity. That historical note is preserved; this handoff records the verified commit identity and correct remote tip.
- Frozen source authorities retained from the supplied package:
  - V1: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`
  - V2: `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`
  - Agent V1: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`
- The owner’s canonical `00_START_HERE.md` and required execution-runbook sections were not present in this environment. Implementation used the supplied surgical prompt, the frozen authority hashes above, the S49 source baseline, and the extracted audit package. No absent owner decision is inferred.

## Repairs

- V1 hourly-close records now require canonical unique IDs and availability no earlier than close finality. Snapshot computation cannot precede its decision slot or any required input receipt. The legacy V1 `snapshot_hash()` payload stays unchanged and is pinned by a golden digest.
- V2 feature sets have new gap-qualified version IDs. Time-series values become unavailable across visible non-contiguous H4/H1/M15 intervals; exact-prefix checkpoint tails are also checked for visible gaps. Prior-day range requires all 96 unique M15 slots. Generic trade VWAP deduplicates identical native trade replays, rejects conflicting identities, and exposes only a partial-to-cutoff value while the complete-day descriptor remains unavailable.
- S1 refuses gapped joined or retained-history windows, unqualified feature versions, and conflicting trade identities while preserving its existing warmup result. S1 hard sizing independently requires an indexed, cutoff-causal, gap-qualified feature artifact with required values. The sizing computation receipt advances to V2 so an older cache identity cannot skip this evidence check. `SizingDecisionV2` remains unchanged.
- Regression fixtures now create the S1 feature evidence required by that gate; the session-023 synthetic feature fixture uses the gap-qualified version and required values.

## Open findings and approval boundaries

- **AUD-01 — OPEN / P1.** Hard sizing still has no candidate-level conservative funding bound. The audit-anchored, versioned migration proposal is in [`ATLAS_R1_VERSIONED_AMENDMENT_PROPOSAL.md`](ATLAS_R1_VERSIONED_AMENDMENT_PROPOSAL.md). Until an accepted versioned funding contract reaches sizing and reservation, capital remains closed.
- **AUD-02 — PARTIAL.** Direct feature frames, S1 joined bars, and visible exact-history tails fail closed on cadence gaps. `ActiveCausalHistoryStateV1` does not persist a full-prefix gap-free proof after its bounded tail rolls forward. A gap older than that tail remains unverified and requires a versioned history coverage receipt/rebuild before relying on long-lived exact-prefix indicators.
- **AUD-05 — IMPLEMENTED.** Impossible V1 source/compute chronology and reused or blank source IDs are rejected. Existing V1 snapshot hash meaning stays unchanged.
- **AUD-06 — PARTIAL / VERSIONED MIGRATION OPEN.** Blank/reused V1 source IDs are rejected and source availability is retained. Full feature-payload hashing is not substituted into the frozen V1 digest. The V2 identity and compatibility proposal is documented separately.
- **AUD-07 — IMPLEMENTED FOR GENERIC DESCRIPTORS.** Partial prior-day ranges no longer publish as complete; exact duplicate native trades collapse, conflicts fail closed, and the complete-day VWAP remains explicitly unverified. S1’s existing optional-input guard and S3’s separate completeness rules remain in place.

AUD-01 and the complete AUD-06 identity migration require owner review because they change persisted artifact and consumer semantics. The AUD-02 bounded-history proof also needs a new state/receipt version before a historical prefix can be called fully qualified. The proposal document defines those seams; no migration is silently applied.

## Verification

Commands, counts, baseline comparisons, and the environment record are in [`ATLAS_R1_EVIDENCE_MANIFEST.json`](ATLAS_R1_EVIDENCE_MANIFEST.json) and [`ATLAS_R1_TEST_LOG.md`](ATLAS_R1_TEST_LOG.md). The test group covers the new R1 regressions and affected V1/V2 risk, feature, strategy, chronology, production, and persistence consumers. No repository-wide suite is claimed.

Static checks use the lock-pinned Python 3.12 environment. `uv.lock` and `pyproject.toml` are unchanged. Scoped Ruff, compileall, dependency consistency, and `uv lock --check --python 3.12` are recorded in the manifest. Full-repository Ruff and existing-format baselines are documented without reformatting unrelated audit evidence.

No exchange account, order, capital, paid provider, or live market request was used. No merge, deployment, provider enablement, or trading authority change was made. All existing capital and assisted/agent execution gates remain closed.
