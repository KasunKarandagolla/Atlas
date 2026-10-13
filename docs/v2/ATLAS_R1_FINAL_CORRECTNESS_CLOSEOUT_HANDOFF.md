# ATLAS RED — Final R1 Correctness Closeout

**Disposition:** `IMPLEMENTED` and `TESTED` for the bounded corrective change. Full Atlas engineering promotion remains `TEST GATE`. This handoff is evidence for independent review; it is not self-acceptance.

## Repository and authority

- Repository: `KasunKarandagolla/Atlas` (`origin` is the GitHub repository).
- Fix branch: `fix/red-r1-final-correctness-2026-10-13`.
- Exact implementation baseline and verified compatibility remote HEAD at preflight: `8b77c76b2ef0581cc453bc4f806693884e763cb4`.
- Compatibility implementation: `96b34ed48fca976e5da293248b5e682827cb4d32`.
- Original R1 implementation: `b7f3f17ef885781d9e8efa8dbd2254ee92b0433d`.
- Original S49 baseline: `a73c92d83ca8b2fe5163e636b2c9732361b35e23`.
- Fresh pre-work remote checks also confirmed `impl/red-r1-surgical-correctness-2026-10-13` at `85a4174cd5d373f0217c46c9944f6ed35f4c0a2b`, `audit/red-six-reviewer-2026-10-13` at `e2d3451a071c124375fa2a8869a4217273686652`, and `impl/session-049-final-integration-release` at `a73c92d83ca8b2fe5163e636b2c9732361b35e23`. The unrelated `/workspace/Atlas` worktree (`work` at `bac8b16e767e7a253cc41c4e73f16ac1c6658319`) was preserved.
- The supplied owner source package was found at `/workspace/attachments/85db8a4a-4697-4a9a-8ba2-08e987046400/Red.zip`, SHA-256 `8be29d4486a061e7d70a6e3d36fdd49caacef371faa617d12887db12e17f008b`. `00_START_HERE.md`, final target design, transition architecture, owner decisions, relevant ADRs, runbook sections 2/3/4/5/7, root `AGENTS.md`, `log.md`, `translog.md`, all R1 handoffs/manifests/test logs/amendment proposals, and all three frozen authorities were read. This supersedes the compatibility handoff's earlier environment-absence statement.
- Frozen SHA-256 values match: V1 `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`; V2 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`; Agent V1 `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.
- The owner confirmed the selected model is GPT-6 Luna. The coordinator and the two read-only audits used GPT-6 Luna; the audits made no changes.

## Correctness repair

The compatibility commit restored the frozen five-field `FeatureSnapshot` and its old hash, but `FeatureSnapshotEvidenceV1` still allowed caller-created short lists and metadata-only “validation.” It also made the new receipt guard part of the old `evaluate_phase4()` contract, which changed historical A0/B0 outcomes and TradePlan reproduction.

This correction adds a separate V1 source-proof contract and a separately qualified evaluation path:

1. `ParquetFeatureSourceStore` reads the existing content-addressed `ParquetArchive` with fixed file/row/byte bounds. It re-hashes each Parquet chunk, checks its archive partition, reconstructs every `MarketRecord`, and thereby rechecks `content_hash` and the complete immutable record fingerprint.
2. The builder selects exactly 721 finalized `BAR_1H_LAST` observations at the exact hourly timestamps from `slot − 720h` through the decision slot. It rejects missing, duplicate, late, contradictory or visible revised observations, mixed source/venue/pipeline identity, invalid chronology, and source publication/valid-from times after computation.
3. Only actual-system availability can qualify a new decision. Every selected record must be available no later than snapshot computation. Reconstructed replay availability remains explicitly represented and verifiable for historical diagnostics, but `evaluate_phase4_qualified()` returns `NOT_ESTIMABLE` for that mode.
4. The builder calls the unchanged frozen `feature_values()` implementation. The new `FeatureSnapshotSourceProofV1` binds all 720 returns, variance, sigma and z to the legacy snapshot hash, ordered record IDs, content hashes, fingerprints, source/revision identities, actual/replay/effective availability, and a chronology hash. The versioned receipt is serialized separately and re-opened against the archive on qualification and restart.
5. The scanner's new-decision adapter uses `evaluate_phase4_qualified()` and binds the snapshot instrument, slot, and cutoff to the scanner request. No configured source archive, missing proof, mismatched request, malformed archive, or unverifiable record fails closed with `NOT_ESTIMABLE` and no TradePlan.

### Trust boundary

The authoritative seam is the **owner-configured existing `ParquetArchive` root** populated by the source-ingestion path. The verifier establishes that the exact record content and availability fields being relied on are present under their content-addressed immutable identities and reconstruct the feature payload. A caller-created receipt, timestamps, IDs, or hashes alone do not qualify; the record must resolve in the configured archive and reproduce the complete feature values. Tests use synthetic `MarketRecord` observations written with the real `ParquetArchive.write_batch()` and run the same verifier; they do not inject `verified=True`, subclass the production store, or use a mock proof shortcut. The scanner's default missing-store case is fail-closed.

This seam does not add a venue-signature system. Source authenticity continues to rely on the existing trusted ingestion/archive boundary and the owner-configured root. No production market archive was available in this session to attest a live observation set; without the trusted configured root, new qualification is `NOT ESTIMABLE`.

## Historical V1 boundary

- `FeatureSnapshot` remains the accepted five-field object. Its `snapshot_hash()` algorithm and the frozen feature calculation, EWMA reseeding, volatility floor, thresholds, action identity, TradePlan serialization/hash, and calendar record identity were not changed.
- `evaluate_phase4()` and the named `evaluate_phase4_historical()` preserve the pre-receipt historical diagnostic calculation. `historical_evaluator_from_phase4()` is the diagnostic scanner adapter. A historical result may display its old diagnostic candidate while the new scanner adapter independently refuses qualification without source proof.
- `evaluate_phase4_qualified()` is the new decision path. It verifies source proof before returning the existing evaluation result. It does not change historical evaluation meaning to satisfy new qualification.
- Legacy `FeatureSnapshotEvidenceV1` serialization remains readable, but `validates()` no longer grants source qualification; field consistency is available as `matches_declarations()` for diagnostics only.
- Golden historical identities are pinned in tests: snapshot `32f96ead8b7a9cc6226032bb4dd1381f6ffd1da4f83be1828ae4d057b3b8bf30`; action `ed3d7eca6b60f758a333a3183bf0d0e1a637819cf5d5b0666039244e1b766e2a`; TradePlan `ec8e89ac4a7750c9cc2f968e526bf4f5b70233def3bea91fc90708f600e86856`; decision-calendar record `d0b8e7798e653d6a348cb56f2c81c7847201fbdb4f13969d09b8b04065bb3dee`. The pre-R1 fixture snapshot/action/TradePlan hashes are also retained by the compatibility tests.

## Changed files

| File | Reason |
| --- | --- |
| `src/atlas/strategy/source_proof.py` | New bounded archive reader, exact 721-observation reconstruction, full feature binding, versioned durable source proof and restart verifier. |
| `src/atlas/strategy/crypto_trend_24h_v1.py` | Keep legacy availability declaration serialization but stop treating self-declared metadata as proof. |
| `src/atlas/science/phase4_engine.py` | Restore historical calculation semantics and add fail-closed source-qualified evaluation with scanner identity binding. |
| `src/atlas/science/phase4_persistence.py` | Persist the new source proof under a separate versioned artifact kind. |
| `src/atlas/scanner/handoff.py`, `src/atlas/scanner/__init__.py` | Route new scanner decisions through proof verification and expose a separate historical diagnostic adapter. |
| `src/atlas/strategy/__init__.py` | Export the additive source-proof API. |
| `tests/science/test_r1_source_qualification.py` | Red-to-green source, payload, chronology, availability, deadline, revision, restart, consumer and scanner adversarial coverage. |
| `tests/support/phase4_factory.py` | Remove the two-ID synthetic receipt bypass; fixtures requiring no source proof remain unqualified. |
| `tests/science/test_decision_calendar.py` | Pin historical snapshot/action/TradePlan/calendar hashes and prove missing evidence is not new qualification. |
| `tests/science/test_phase4_persistence.py`, `tests/science/test_r1_v1_snapshot_compatibility.py` | Preserve separate diagnostic versus qualified persistence/compatibility expectations. |
| `tests/scanner/test_phase5_end_to_end.py`, `tests/integration/test_v1_phase0_6_acceptance.py` | Use the explicit historical adapter for legacy replay and test new-path fail-closed behavior. |
| `docs/v2/ATLAS_R1_FINAL_*`, root `log.md`, root `translog.md` | Retain raw red evidence, exact green commands, final handoff, machine-readable evidence, and coordinator checkpoint. |

No frozen authority, dependency/lock, strategy, risk, selector, broker, capital, assisted-execution or live-execution contract changed. No R2 work began.

## Test disposition

- Source-proof and scanner-request binding: `TESTED`, 35/35.
- Final V1/Phase-4/archive/recovery/scanner group: `TESTED`, 111/111.
- Affected R1 V2 consumers: `TESTED`, 199/199.
- S45 outcome/content-hash and maturity consumers: `TESTED`, 76/76.
- S27 production: `TESTED`, 16/16; restart composition: `TESTED`, 1/1.
- Baseline-aware static, lock, dependency and frozen-hash checks: `TESTED` as recorded in [`ATLAS_R1_FINAL_TEST_LOG.md`](ATLAS_R1_FINAL_TEST_LOG.md).
- Full-repository suite: `TEST GATE`; it was not run because the rolling write budget could not be bounded safely. The known S49 1,024-contract mixed-delivery rejection and partial 1,196/2,783 full-suite attempt remain visible and unchanged.
- Production source data: `UNVERIFIED`; without a trusted owner-configured archive, the runtime result is `NOT ESTIMABLE`.

`AUD-01` adverse funding bounds in hard sizing and durable reservation, `AUD-02` historical cadence proof beyond the retained active-history tail, and `AUD-06` complete V1/V2 snapshot identity migration remain amendment-gated. No capital, assisted orders, authenticated Binance/Bybit endpoint, paid-model call, exchange order, merge, or deployment was enabled or used.

The implementation is committed as `9930fc834022ce3e8f5bc2239bd60867a251f46e`, pushed to `fix/red-r1-final-correctness-2026-10-13`, and `git ls-remote` returned that exact SHA. The raw red output is retained losslessly in `ATLAS_R1_FINAL_RED_OUTPUT.txt.gz`. A documentation-only receipt records the verified implementation push; the exact final branch head after that receipt is stated in the coordinator delivery response. Independent review is pending.
