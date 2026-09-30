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

## Independent-review remediation — 2026-09-30

This is an additive record of the remediation. The original S31 validation above remains historical evidence of the original implementation and has not been rewritten to imply these defects were absent.

### Repository and authority

- Reviewed remote S31 tip: `a941c4dc7a6f7411e2a4c208c70af9889ae3e048`; it descended from the accepted S30 base `8ed124c4127c94c2c6e394a2bf6f941c3a5f6b57`. The original tested S31 commit remains `df9f8e807497a449631d0a46f3c15050c3a4f952`.
- Tested remediation implementation: `659e9833753602c77c877bd1fc08b18e848eb43f`, directly parented by the reviewed S31 tip. The two original S31 commits were not rewritten.
- The three authority hashes remain V1 `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`, amended V2 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, and agent freeze `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.
- Both dependency locks are unchanged: `requirements-lock.txt` `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`; `requirements-agent-lock.txt` `47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3`. The V1 golden hash remains `be2a54d2bf9a3a168fe850877d51ef241d8ae8cdad837b872270cc3a30166251`.
- The consultation PDF remains consultation only. The eight-week prospective floor, 200 genuinely matured opportunity minimum, regime coverage, dependence-aware inference, chronology, research embargoes and final-holdout protection remain unchanged.

The issue-by-issue reproduction log, exact regression names, budgets, lock/freeze identities, and validation commands are in the additive [independent-review remediation record](../v2/SESSION031_INDEPENDENT_REVIEW_REMEDIATION_V1.json). The prior [validation record](../v2/SESSION031_PUBLIC_SHADOW_READINESS_VALIDATION.json) and [readiness gate](../v2/PUBLIC_SHADOW_CAMPAIGN_READINESS_GATE_V1.json) link to it; the gate content hash was recomputed after adding that link.

### Findings and corrections

| Finding | Original behavior and pre-fix reproduction | Remediation and final regression evidence | Remaining status |
| --- | --- | --- | --- |
| A — acquisition time | A deterministic advancing-clock reproduction on `a941c4d` failed the post-acquisition availability assertion: earliest availability was `1800000100000000` ns while the latest response receipt was `1800001400000000` ns. The equivalent fixture passed against `659e983`. | Actual receipt timestamps remain intact; ingestion/availability is at the completed acquisition boundary, and later data is deferred to the next eligible frozen cycle. `test_advancing_receipts_are_deferred_to_the_next_causal_cycle`, expiry and restart tests pass. | `TESTED`; live latency remains `UNVERIFIED / TEST GATE`. |
| B — trade recovery | The original bar-overlap predicate returned `true` for a contiguous-bar snapshot without proving trade continuity. The latest-100 REST trade page has no historical cursor. | Recovery evidence now separates endpoint reachability, bars, observed trades and trade continuity. Disconnect/rate-limit regressions pass and leave eligibility closed; S3 reports continuity as unproven. | Trade-history recovery remains `TEST GATE`; no backfill or WebSocket system was added. |
| C — calendar origins | On `a941c4d`, the persisted `T + 2s` receipt/cutoff handoff was not matched to its final M15 bar close at `T`; the pre-fix fixture reported zero observed handoffs. | Reconciliation now follows exact event, trigger, observation, bar, product and receipt references; full instrument identity is used. Late, duplicate, invalid, missing, and same-revision/different-instrument cases pass. | `TESTED`; a missing slot is not interpreted as no opportunity. |
| D — science inventory | On `a941c4d`, the pre-fix fixture reported zero M0 calibration artifacts despite one qualifying persisted as-of artifact. | Supported M0/M1, analogue, pretrade, execution, cost and Discovery Lab records are queried and typed/hash validated. Future evidence is excluded; unsupported research calibration is unavailable rather than zero. Malformed index rows now gate science counts to `null`. | `TESTED`; unavailable stores remain explicitly unavailable. |
| E — pagination | On `a941c4d`, the pagination/snapshot regression failed because `OpsRepository.read_snapshot` did not exist. | Read-only snapshot transactions and stable keyset pagination cover equal timestamps and concurrent writers. The >10,000 record test passes; interruption, invalid rows and resource bounds report incompleteness instead of partial denominators. | `TESTED` within the stated bounded work budget. |
| F — S3 provenance | On `a941c4d`, the production preflight fixture had no per-instrument persisted-lineage view. The old numerical fixture could pass arrays with placeholder references. | A separate persisted-lineage validator checks M1 bars, exact trade-derived VWAP refs, residual links, availability, health, BBO and event evidence. Made-up refs, future data, noncontiguous/duplicate bars and mismatched instruments fail. | Deployed S3 stays `NOT ESTIMABLE` / `TEST GATE` because bounded REST cannot prove trade continuity; native M1 cadence is unchanged. |
| Latency — whole request budget | The original snapshot had no request/duration accounting, so the deterministic whole-acquisition budget regression failed on `a941c4d`. | The 13 sequential market requests are bounded by one five-second monotonic deadline; one optional metadata bootstrap request is separately counted and shares that deadline. No retry, thread pool, request burst or widened event window was added. Timeout, slow, partial, rate-limit, stale-event and recovery tests pass. | Live throughput and owner-host timing remain `UNVERIFIED / TEST GATE`. |

### Corrected files and review ownership

The tested implementation changes exactly these files relative to `a941c4d`:

- `src/atlas/v2/data/bybit_source.py`
- `src/atlas/v2/data/collector.py`
- `src/atlas/v2/memory/repository.py`
- `src/atlas/v2/runtime/production.py`
- `src/atlas/v2/science/session031_preflight.py`
- `src/atlas/v2/science/session031_readiness.py`
- `src/atlas/v2/science/session031_s3_provenance.py`
- `tests/v2/test_session031_acquisition_recovery_remediation.py`
- `tests/v2/test_session031_preflight_remediation.py`
- `tests/v2/test_session031_s3_persisted_lineage.py`

Subagent A had primary ownership of acquisition and recovery changes/tests. Subagent B had primary ownership of read-only reporting, repository pagination and preflight tests. The main agent reviewed those changes and owned the additive collector metadata, persisted S3 lineage, integration, final regressions, documentation and commit verification. Agents shared the workspace; their primary ownership was disjoint, main-agent follow-up edits were sequential, and no overlapping edits remained in the final diff. These were development workers only; no deployed ATLAS agent runtime was created.

### Final validation at tested implementation SHA

| Gate | Result |
| --- | --- |
| Full V2: `.venv/bin/python -m pytest -q -o addopts='' tests/v2` | 602 passed, 2 skipped, 0 failed; 1,000.51 seconds |
| Full non-V2: `.venv/bin/python -m pytest -q -o addopts='' tests --ignore=tests/v2` | 485 passed, 3 skipped, 0 failed; 218.21 seconds |
| Contract/golden command | 10 passed, 0 skipped, 0 failed; 4.99 seconds |
| Focused final preflight remediation module | 11 passed, 0 failed; 17.03 seconds |
| Changed-seam integration command | 53 passed, 0 failed; 243.07 seconds; included again in full V2 |
| Ruff / mypy / compileall / pip check / diff check | Passed; mypy checked 205 source files; no broken requirements |
| Golden and dependency lock hashes | Match the exact required hashes above |

The two V2 skips are optional local SDK MockTransport tests because those SDKs are absent from the core environment. The three non-V2 skips are opt-in Bybit authenticated testnet, Binance authenticated testnet and public testnet connectivity. The isolated locked SDK MockTransport evidence from the original S31 record remains unchanged.

A value-suppressing scan of the staged implementation diff found zero high-confidence secret matches; no matched values were printed. No real `.env` files were found or staged. No external test network calls, paid provider calls, authenticated venue/account calls or real public-market requests were made. GitHub use was limited to repository fetch, push and remote SHA verification.

### Safety and remaining gates

Capital and assisted execution remain disabled. Critic decision and admission influence remain false. No provider/model substitution occurred. The final holdout was not accessed. Economic value remains `NOT ESTIMABLE`. The existing 11-stage pipeline, V1/S1/S2/S3 policies, S30 critic boundary, single-writer repository and default archive-only production behavior remain unchanged.

Owner Windows/WSL host qualification, continuous public-data continuity, full source/cadence coverage, S2/S3 warmup, native S3 M1 cadence, historical bootstrap, continuous matured-outcome production and a 72-hour public campaign remain `UNVERIFIED` or `TEST GATE`. The eight-week prospective/200 matured opportunity floor and scientific/economic gates have not started, been shortened or passed. The maximum engineering ceiling remains `ENGINEERING_PASS`, subject to independent coordinating acceptance.

The implementation commit is `659e9833753602c77c877bd1fc08b18e848eb43f`; it was pushed and fetched from GitHub at that exact intermediate remote tip. The documentation-only closeout commit is the containing commit for this final record; its exact post-push GitHub tip is reported in the returned handoff. No merge or Session 032 is authorized.
