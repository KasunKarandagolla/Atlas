# ATLAS documentation and evidence index

Current S49: [implementation handoff](v2/ATLAS_S49_IMPLEMENTATION_HANDOFF.md), [integration report](v2/ATLAS_S49_END_TO_END_INTEGRATION_REPORT.md), [target implementation audit](v2/ATLAS_S49_TARGET_IMPLEMENTATION_AUDIT.md), and [evidence manifest](v2/transition-s49-evidence/evidence_manifest.json). Run the [offline integration check](../scripts/atlas_s49_offline_integration.py) for the configured end-to-end groups and the [full repository test runner](../scripts/atlas_s49_full_repository.py) for all collected pytest cases. Both use offline network isolation. The full run collected 2,783 tests but stopped at 1,196 reached (43%) after 1:01:24 in a long S3 history fixture; the partial six-failure log is not a full-suite verdict. The S49 evidence retains the production `agent_mode=DISABLED` integration gate and reproducible mixed-load queue rejection. Linux package/Qt IPC smoke passed; Windows is `BLOCKED BY ENVIRONMENT`. No capital, paid provider, authenticated API, demo order, or discretionary management authority is activated. The local Red update is summarized in the [S49 handoff](v2/ATLAS_S49_IMPLEMENTATION_HANDOFF.md).

Previous S48: [implementation handoff](v2/ATLAS_S48_IMPLEMENTATION_HANDOFF.md), [evidence manifest](v2/transition-s48-evidence/evidence_manifest.json), [Binance reference comparison](v2/ATLAS_S48_BINANCE_SKILLS_HUB_CONTRACT_COMPARISON.md), [PROPOSED management amendment](v2/ATLAS_S48_MANAGEMENT_RESEARCH_AMENDMENT_PROPOSED_V1.md). Continue the accepted S47 base; no capital or discretionary management authority. ADR-003 in the owner source set is the primary lifecycle target. Research source/execution venue identity is explicit; future exchanges require their own qualified local evidence and accepted authority. The existing Bybit comparator remains.

Historical S47 checkpoint (2026-10-12). S47 continues accepted S46 `95efa393e50f5e71646c409a3b4ff83cf5acb6a2` on `impl/session-047-economics-agent-decision-brief`. See the [S47 handoff](v2/ATLAS_S47_IMPLEMENTATION_HANDOFF.md), [S47 evidence](v2/transition-s47-evidence/evidence_manifest.json), root [README](../README.md) and [coordinator log](../log.md).

## Previous S47 research scope and authority gate

- [Proposed comparative decision amendment](v2/ATLAS_S47_COMPARATIVE_DECISION_AMENDMENT_PROPOSED_V1.md): owner authorized review; independently accepted amendment **NONE**.
- [Preserved owner implementation request](v2/ATLAS_S47_OWNER_IMPLEMENTATION_REQUEST.md).
- [Setup continuation history](v2/ATLAS_S47_SETUP_CONTINUATION.md): tooling setup completed; implementation status is now in the handoff.
- Additive source: `src/atlas/v2/research/{decision_brief,agent,memory}.py`; fake-only, zero authority, disconnected from admission/capital.
- [S46 handoff](v2/ATLAS_S46_IMPLEMENTATION_HANDOFF.md) and its original [evidence](v2/transition-s46-evidence/evidence_manifest.json) remain historical proof.


## Current authority

The owner source set is `/home/kasun/Music/atlas/Red` and remains a reference package except for the narrow S49 owner-authorized addendum to the owner decisions file and its matching `MANIFEST.json` hash/byte record. Those local files are not copied into this Git repository. The current product-intent and transition documents are `Red/ATLAS_FINAL_TARGET_DESIGN_LOCK_2026-10-12.md` and `Red/ATLAS_TRANSITION_ARCHITECTURE_DESIGN_V1.md`. Owner decisions and the engineering execution runbook are in the same directory. The target design does not amend the frozen implementation contracts.

The immutable authorities and verified SHA-256 values are:

| Authority | SHA-256 |
| --- | --- |
| V1 implementation clarification/freeze | `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c` |
| V2 intraday intelligence freeze | `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78` |
| Agent intelligence extension freeze | `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d` |

Relevant owner architecture decisions in the read-only source set are `Red/ADR-001_Binance_Causal_Market_Data_Linux.md`, `Red/ADR-004_Mature_Libraries_Pretrained_ML.md`, and `Red/ADR-010_Cached_Cross_Coin_Relationships_And_Peer_Context.md`. The tracked Agent freeze is [here](v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md). Freeze hashes are rechecked in the S47 handoff. S47 also reads ADR-002/005/006/007/008, with ADR-009/010 for effort and peer boundaries.

## Active implementation and test entrypoints

Installed command names are declared in [`pyproject.toml`](../pyproject.toml):

| Command | Module |
| --- | --- |
| `atlas-product` | `atlas.v2.product:main` |
| `atlas-v2-projection` | `atlas.v2.desktop.ipc:main` |
| `atlas-desktop` | `atlas.desktop.app:main` |
| `atlas-v2-public-qualification` | `atlas.v2.data.qualification:main` |
| `atlas-v2-public-soak` | `atlas.v2.data.soak:main` |
| `atlas-ops` | `atlas.v2.runtime.ops_supervisor:main` |
| `atlas-agent-worker` | `atlas.v2.agent_intelligence.worker:worker_main` |
| `atlas-agent-broker` | `atlas.v2.agent_intelligence.broker:broker_main` |

Source seams used by the S46 work:

- Causal technical indicators: [`technical.py`](../src/atlas/v2/features/technical.py), consumed by the feature pipeline and S1/S2/S6; tests include [`test_session014_core.py`](../tests/v2/test_session014_core.py) and [`test_session037_feature_prefix.py`](../tests/v2/test_session037_feature_prefix.py).
- Feature structure and artifact assembly: [`structure.py`](../src/atlas/v2/features/structure.py) and [`pipeline.py`](../src/atlas/v2/features/pipeline.py).
- Existing S6 BTC beta/residual and hourly-bar research semantics: [`s6_cross_section.py`](../src/atlas/v2/strategies/s6_cross_section.py); regressions include [`test_session021_s6.py`](../tests/v2/test_session021_s6.py) and [`test_session041_s6_cohort.py`](../tests/v2/test_session041_s6_cohort.py).
- Causal bar and exact instrument revision contracts: [`bars.py`](../src/atlas/v2/data/bars.py) and [`instruments.py`](../src/atlas/v2/instruments.py).
- S45 due-work `content_hash` fix: [`repository.py`](../src/atlas/v2/memory/repository.py); preserve this behavior.

Root `log.md` is the rolling coordinator record; `translog.md` is append-only chronological evidence. Installation routes currently disagree on DuckDB: `requirements-lock.txt` pins 1.5.5, while `uv.lock` resolves 1.5.6. S45 records the `requirements-lock.txt` environment; S46 leaves both files unchanged absent an approved dependency review.

## Previous completed checkpoint — S46

The [S46 implementation handoff](v2/ATLAS_S46_IMPLEMENTATION_HANDOFF.md) records cleanup disposition, technical-library parity, peer-cache contracts and negative cases, retained test results, dependencies and remaining qualification gates. Machine-readable report hashes and JUnit summaries are in [`transition-s46-evidence/evidence_manifest.json`](v2/transition-s46-evidence/evidence_manifest.json).

## Previous completed checkpoint — S45

The [S45 factual handoff](v2/ATLAS_TRANSITION_S45_FACTUAL_HANDOFF.md) records implementation, test counts, and open gates. The [decision register](v2/ATLAS_TRANSITION_S45_DESIGN_DECISION_REGISTER.md) and [documentation-readiness record](v2/ATLAS_TRANSITION_S45_DOC_FINALIZATION_READINESS.md) retain the owner decision boundaries. Machine-readable and test evidence live in [`transition-s45-evidence/`](v2/transition-s45-evidence/) with [`evidence_manifest.json`](v2/transition-s45-evidence/evidence_manifest.json).

S45 results include 61/61 production outcome tests, 15/15 S33 maturity, 25/25 durable-capture, 35/35 combined event/calendar, and 1/1 each for V1 golden and S44 M15 compatibility. These are historical results; consult the S46 handoff for fresh S46 results.

## Historical evidence retained

- **S41 recovery and failures:** root `SESSION041_*.md` / `SESSION041_*.json`, `docs/v2/SESSION041_*.md` / `.json`, and `docs/v2/session041-evidence/`. These include failed capacity, recovery, authority, and resilience evidence. Keep the failed attempts discoverable.
- **TQ-01 Linux qualification:** [`ATLAS_TRANSITION_TQ01_LINUX_QUALIFICATION.md`](v2/ATLAS_TRANSITION_TQ01_LINUX_QUALIFICATION.md), [`ATLAS_TRANSITION_TQ01_VERSIONED_AMENDMENT_DRAFT.md`](v2/ATLAS_TRANSITION_TQ01_VERSIONED_AMENDMENT_DRAFT.md), and `transition-tq01-evidence/`.
- **S44 close-driven M15:** [`test_session044_close_driven_m15.py`](../tests/v2/test_session044_close_driven_m15.py) and the S44 entries preserved in root logs and `docs/v2/`.
- **V1 comparator and persistence compatibility:** [`test_v1_golden_baseline.py`](../tests/contract/test_v1_golden_baseline.py) and `tests/persistence/`; these remain protected replay evidence.

## Open qualifications carried from S45

- Fake Bybit source/supervisor fixture: `TEST GATE`; `INCOMPLETE_SNAPSHOT` and a noncausal dependency `ValueError` remain unresolved.
- Injected TQ-01 breadth stall: `TEST GATE`; queue high-water/current 480/512 and one rejected frame; S45 80/120 frames/s runs yielded no valid sustained drain metrics.
- Real Binance final-M15 delivery within close+5s: `UNVERIFIED`.
- Linux F1/A2 sustained capacity: `TEST GATE`.
- Authenticated/native Binance protection and capital: unqualified; capital and assisted execution remain disabled.
- Combined calendar/event saturation assertion: `TEST GATE`; the isolated suites pass, while two combined runs saw `CALL_OUTCOME_UNKNOWN` instead of the expected saturation reason.
- Existing `TECHNICAL_V1` accepts sparse finalized bars without a gap label; gap-sensitive future use remains `TEST GATE`.
- Economic value: `NOT ESTIMABLE`.

## Cleanup disposition

The Section 5 runbook action register was used as a review lead. The narrow reverse-reference inventory found technical helpers referenced by S1/S2/S6 and `features/pipeline.py`; feature and strategy tests exercise those functions. Root logs, V1 golden artifacts, persisted/migration readers, and S41/TQ/S44 failure reports retain functional or audit consumers. No production helper, source file, generated artifact, or historical record was proven safe to delete. **DELETE NONE.**
