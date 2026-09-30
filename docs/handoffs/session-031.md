# Session 031 — Public-Shadow Campaign Readiness and Evidence Controls

## Checkpoint

- Required starting branch/SHA: `impl/session-030-agent-prospective-shadow-runtime-readiness` / `8ed124c4127c94c2c6e394a2bf6f941c3a5f6b57`.
- S30 tested implementation ancestor: `732032bfa053447c089985e58d42cf5232733504`.
- Branch: `impl/session-031-public-shadow-campaign-readiness-evidence-controls`.
- Tested implementation commit: `df9f8e807497a449631d0a46f3c15050c3a4f952`.
- The tested implementation branch tip was fetched and verified on GitHub at that SHA before this documentation-only closeout. The final documentation tip is reported separately after its push.
- No merge was performed. No earlier session history was rewritten.

The three binding authority documents matched their required SHA-256 values and were read in full. The 33-page `ATLAS_72_Hour_Consultation.pdf` was also read in full, with tables checked against rendered page layout. It remains consultation only. Its proposed replacement for the eight-week prospective floor is not accepted; the existing eight-week floor and all other scientific/economic gates are unchanged.

The consultation review separates reusable infrastructure, confirmed gaps, recommendations that fit existing authority, recommendations requiring a versioned amendment, and work deferred beyond S31 in [the validation record](../v2/SESSION031_PUBLIC_SHADOW_READINESS_VALIDATION.json).

## Implementation

The change adds an opt-in, credential-free Bybit public REST intake for BTCUSDT and ETHUSDT USDT-linear perpetuals. `BybitPublicCycleSourceV1` returns bounded immutable observations for product metadata, confirmed M1/M15/H1/H4 bars, recent public trades, BBO, mark/index and ticker fields. The adapter owns no repository; `ProductionOpsCyclePortV1` persists through the existing `PublicCollectorV2`, Parquet archive and index, then applies existing source-health and recovery rules. The default production factory remains archive/index-only. Binance, WebSocket collection, historical trade backfill and fresh-host qualification remain outside this checkpoint.

The deterministic fake-source path traversed the real collector, archive/index, recovery reconciliation, production composition and supervisor receipt. Its deliberately short history produced two `NO_CANDIDATE` calendar entries with `NOT_APPLICABLE` admission; no artificial candidate was introduced. Source receipt timestamps and health reconciliation remain at or after actual receipt time.

The read-only readiness evaluator reports causal S1/S2/S3 prerequisites and exact missing reasons. It keeps the S3 1-minute policy origin separate from the current M15 production handoff cadence. Production cadence and frozen strategy policies were not changed. The report and evidence inventory remain non-qualifying when coverage is missing.

Campaign metadata carries the accepted S30 and freeze identities, lock hashes, baseline policy identities, host/clock declaration, source and instrument scope, resource bounds, existing S23 experiment/multiplicity history, intervention receipts and holdout prohibition. It grants no authority. Lane identities A–E preserve the separate baseline, adaptive research, historical, future challenger and fault-replica evidence classes. The S23 17-attempt history and zero remaining attempt budget were preserved; no S31 trial was authorized.

The preflight emits JSON and a concise summary from an `OpsRepository` opened read-only. It inventories public receipts, source health, strategy/calendar evidence, science and trial support, the S30 critic observations available in the ops store, resources, recovery and prospective evidence. Missing values stay null or explicitly gated. No mature outcome was generated. Existing outcome contracts, validators, indexers and the S30 linker do not qualify a continuous producer; the remaining status is `TEST GATE — PRODUCTION_MATURED_OUTCOME_PRODUCER_NOT_QUALIFIED`.

The seven-section fresh-host/WSL runbook is [here](../v2/session031-wsl-fresh-host-preflight.md). It covers locked setup, Linux-side WAL/Parquet storage, UTC/NTP, durability, capacity, public networking, process ownership, interruption accounting and safe read-only preflight commands. No owner host was configured or qualified.

## Validation

| Check | Result |
| --- | --- |
| S31 focused readiness/source/fault tests | 26 passed, 0 skipped, 0 failed |
| Full V2 suite | 573 passed, 2 skipped, 0 failed; 1,160.03 seconds |
| Full non-V2 suite | 485 passed, 3 skipped, 0 failed; 352.98 seconds |
| Contract and V1 golden command | 10 passed, 0 skipped, 0 failed |
| Isolated locked S28/S29 SDK MockTransport cases | 2 passed, 0 skipped, 0 failed; offline temporary environment |
| Ruff | Passed |
| Mypy | Passed; 204 source files |
| Compileall, core `pip check`, `git diff --check` | Passed |
| Isolated agent lock `uv pip check` | Passed; 26 locked packages compatible |
| V1 golden JSON SHA-256 | `be2a54d2bf9a3a168fe850877d51ef241d8ae8cdad837b872270cc3a30166251` |
| Requirements lock SHA-256 values | Unchanged; recorded in validation JSON |

The two V2 skips are optional local MockTransport tests because the core environment lacks the provider SDK modules. Both passed separately from the unchanged agent lock using `UV_OFFLINE=1`. The three non-V2 skips are Bybit authenticated testnet, Binance authenticated testnet, and public testnet connectivity opt-ins. No external network request occurred during tests.

The S30 representative receipt hash, 11-stage order hash, receipt/pipeline wire checks, V1 risk hash and S1/S2/S3/selection hashes are recorded in the validation JSON. No provider configuration, model, dependency lock, execution path, risk policy, strategy policy or capital behavior changed.

## Gates and safety

- S31 readiness components: `IMPLEMENTED / TESTED` offline.
- Engineering ceiling: `ENGINEERING_PASS`, subject to independent coordinating review.
- Genuine fresh-host public ingestion: `UNVERIFIED / TEST GATE`.
- Complete source/cadence coverage, native S3 M1, qualified bootstrap, deployed mature outcome producer, continuous 72-hour campaign and genuine prospective critic: `TEST GATE`.
- Supported economic value: `NOT ESTIMABLE`.
- Capital and assisted execution: disabled. Critic decision/admission influence: false and prohibited.
- Final holdout: `UNASSIGNED / UNTOUCHED`.
- No real DeepSeek, OpenAI, NVIDIA NIM, public market, authenticated venue or account calls were made. No 72-hour campaign or host endurance run was started.

The proposed consultation replacement for the eight-week floor was rejected. Hour-30/hour-36 campaign clocks were not started; the existing M1 24-hour out-of-fold embargo was not shortened. A 72-hour segment cannot substitute for the existing eight-week, 200 genuinely matured opportunity, regime/state coverage, dependence-aware inference, after-cost evidence and degradation gates. Those floors do not guarantee promotion.

**Session 031 implementation is submitted for independent coordinating principal engineering review. Engineering test results and the pushed branch are not self-acceptance. No merge or Session 032 is authorized.**
