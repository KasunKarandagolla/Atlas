# ATLAS RED R1 Compatibility Correction Handoff

**Status:** implementation and documentation receipt commits were pushed and each SHA matched the remote branch ref. The current final branch tip is reported in the delivery response.

## Baseline and authority

- Repository: KasunKarandagolla/Atlas.
- Fix branch: fix/red-r1-v1-snapshot-compat-2026-10-13.
- Pushed implementation commit: 96b34ed48fca976e5da293248b5e682827cb4d32; remote ref verification matched exactly.
- Pushed documentation receipt commit: 575ec9c448a260ccf49c88b837d98d341b0ac77b; remote ref verification matched exactly.
- Verified starting remote branch: impl/red-r1-surgical-correctness-2026-10-13 at 85a4174cd5d373f0217c46c9944f6ed35f4c0a2b.
- R1 implementation ancestor: b7f3f17ef885781d9e8efa8dbd2254ee92b0433d.
- The correction worktree was created from the verified remote R1 tip. The pre-existing R1 worktree and owner inputs were preserved.
- The supplied owner package was found at /workspace/attachments/85db8a4a-4697-4a9a-8ba2-08e987046400/Red.zip. Its 00_START_HERE.md, required execution-runbook sections 2, 3, 4, 5 and 7, owner source index, and three frozen authorities were read. This resolves the prior R1 report that those files were absent; it is not an authority waiver.
- Frozen authority SHA-256 values match exactly: V1 c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c; V2 e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78; Agent V1 d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d.
- Runtime model identity was not exposed in a way that verifies GPT-6 Luna. No subagents were used. This model-verification limitation remains explicit.

## Correction

R1 had added source_available_at_ns to the frozen five-field FeatureSnapshot and required it to be populated, so valid historical five-argument construction failed and dataclasses.asdict() gained a field. The correction restores the original frozen five-field object and leaves its snapshot_hash() algorithm unchanged.

New construction uses build_snapshot() to validate actual input availability at the supplied computed_at_ns. build_snapshot_with_evidence() additionally returns a separate FeatureSnapshotEvidenceV1 receipt with an explicit version. Phase 4 requires a matching receipt before it can produce a new decision/TradePlan; the receipt is persisted as its own artifact kind. A legacy snapshot remains readable, hashable and replayable, but does not gain qualification by being read.

| Contract | Schema and behavior |
| --- | --- |
| Pre-R1 V1 | Five fields: instrument, slot_at_ns, computed_at_ns, values, source_record_ids; original serialization and hash. |
| R1 | Six serialized fields, with source_available_at_ns defaulting to () while validation requires a populated tuple. |
| Corrected | Original five-field serialized object and original hash; optional qualification is represented outside it by versioned FeatureSnapshotEvidenceV1. |

HourlyClose construction and legacy window reads retain the prior permissive record-ID/availability behavior. The new qualified builder separately requires canonical unique IDs, hourly finality, no future/unavailable input at actual computation time, a 721-close contiguous window ending at the decision slot, computation no earlier than the slot, and computation no later than T+30. Deadline equality passes; T+30+1 ns fails. Historical data is not silently rewritten or requalified.

No V1 strategy math, signal, policy, timing rule, TradePlan schema/hash, replay identity, frozen authority, or R2 behavior was amended.

## Changed files

- src/atlas/strategy/crypto_trend_24h_v1.py: restore legacy snapshot and add separate versioned availability receipt/builders.
- src/atlas/strategy/features.py: preserve legacy reads and put strict identity/finality checks on qualified construction.
- src/atlas/strategy/__init__.py: export the receipt type.
- src/atlas/science/phase4_engine.py: require matching proof for new phase-4 qualification, without changing TradePlan identity.
- src/atlas/science/phase4_persistence.py: store proof under its separate versioned artifact kind.
- tests/fixtures/legacy_feature_snapshot_v1_pre_r1.json: static pre-R1 five-field historical fixture.
- tests/science/test_r1_v1_snapshot_compatibility.py: red-to-green compatibility, replay identity, proof and chronology cases.
- tests/science/test_r1_v1_chronology_identity.py, tests/science/test_phase4_persistence.py, tests/support/phase4_factory.py: preserve R1 chronology coverage while adapting phase-4 fixtures to explicit external evidence.
- This handoff, its JSON evidence manifest, and append-only root log entries record the correction.

## Test evidence and promotion gates

The compatibility regression file was run before production edits and failed 7 cases, including the five-argument construction, legacy serialization/read path, and evidence qualification API. After the change, the compatibility cases are included in the passing V1/phase-4 group.

- V1 chronology, compatibility, frozen golden, prefix invariance, phase-4 persistence and end-to-end group: 36 passed. The historical fixture retains snapshot hash c86086000f333ba7d372f81a1a14945a53eaef7693647483ddc74f745df5bc25 and phase-4 action hash 6ff8ec0c3bf5cc545e5c72509a173af711331e9da8f3fd1321bbdf6528686ecb; TradePlan snapshot reference matches the same legacy digest.
- Affected R1 V2 consumer group: 199 passed.
- S27 production group: 16 passed.
- S27 restart composition path: 1 passed.
- Exact commands and durations are in ATLAS_R1_COMPATIBILITY_CORRECTION_EVIDENCE_MANIFEST.json.
- ruff check on changed Python files, compileall, uv lock --check --python 3.12, package consistency, and git diff --check pass. Full-source mypy reports the same 52 historical diagnostics as the R1 baseline, normalized delta +0/-0.
- Ruff format check passes for newly formatted/touched files that were clean at baseline. Five touched legacy files fail format check both at the R1 base and current content; no broad reformat was applied.
- The complete repository suite was not run and remains a required engineering promotion gate. It is recorded as TEST GATE; measured retained work footprints are below the owner's 5,000,000,000-byte rolling limit, but the full-suite write budget was not safely bounded.

Open proposals remain open: AUD-01 funding-aware sizing, AUD-02 historical-prefix coverage migration, and AUD-06 full-payload V2 identity migration. None was implemented here. No authenticated venue requests, exchange orders, paid model calls, capital enablement, merge, deployment, self-acceptance, or R2 work occurred.
