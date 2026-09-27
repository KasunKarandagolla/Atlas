# Session 024 — Venue qualification, capital bridge, and failure hardening

## Checkpoint and result

- Required starting checkpoint: `665f44e3ecd99f7228cb5736b15f00f6ff6d1985` on `impl/session-023-v2-m1-analogue-discovery-selection-gate`.
- Branch: `impl/session-024-v2-venue-qualification-capital-bridge-hardening`.
- Final implementation commit: `1d76d5d13a6cc51654d68a0830b43224c031d2f6`. It contains the Phase-4 bridge, qualification matrix, gate, and tests. The handoff is a documentation-only successor; the final pushed branch tip is verified and reported in the Session-024 closeout response.
- Both authoritative freeze documents and the Session-020–023 handoffs were read before editing. The V1 runtime, approval, reservation, protection, and recovery stack and the current V2 frozen-action, admission, and RiskPolicyV2 contracts were inspected.
- The two pre-existing untracked files `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md` and `atlas-session005.zip` remain untouched and uncommitted. No Session 025 work was started.

## Changed files

- `src/atlas/runtime/phase4_v2.py`
- `tests/runtime/test_phase4_v2_bridge.py`
- `tests/runtime/test_phase4_v2_live_risk.py`
- `tests/runtime/test_phase4_v1_boundary_failures.py`
- `tests/runtime/test_phase4_nautilus_rc5_capabilities.py`
- `tests/runtime/test_phase4_authority_boundary.py`
- `tests/integration/test_phase4_testnet_qualification.py`
- `docs/v2/SESSION024_VENUE_CAPABILITIES.json`
- `docs/v2/PHASE4_VENUE_CAPITAL_GATE.json`
- `docs/v2/SESSION024_FAILURE_MATRIX.json`
- `docs/handoffs/session-024.md`

## Implementation

- Added `V2CapitalBridgeEnvelope`, version `V2_CAPITAL_BRIDGE_ENVELOPE_V1`, in `src/atlas/runtime/phase4_v2.py`. The frozen envelope uses canonical JSON SHA-256 content hashing. The module SHA-256 is `08d8091af437361d3dea8f7300d673da15af955ac13b17d66bf2cfe38edf2b64`.
- The bridge binds the exact CandidateSet and selected candidate, frozen action and version, sizing, V1 RiskPolicy and RiskPolicyV2 hashes, economic evaluation, capability snapshot/profile, product revision, cost model, hashed account scope, instrument, quantity, side, collar, absolute stop and trigger basis, horizon, expiry, risk, margin, and leverage. It rejects non-candidate economics, expiry, OOD/rejection, unsupported capability, and mismatched or non-durable material. Strategy science is not rerun.
- A supported venue profile must have every required capability row marked `TESTED` with matching redacted authenticated-testnet evidence and be current. No venue-qualified profile exists in this environment, so no actual capital bridge instance or instance hash was created. No `SUPPORTED` status was fabricated.
- `persist_v2_bridged_trade_plan()` stores the exact V1 plan before approval. `prepare_v2_bridge_entry()` rechecks current venue evidence and risk policies, binds the V2 rolling-risk result, then delegates to the existing V1 shell. The V1 shell remains responsible for single-use approval, atomic approval-consumption/intent/reservation persistence, command persistence, send-start persistence, writer checks, and Nautilus dispatch. No shared V1 runtime source changed.
- Approval invalidation follows the bridge hash and V1 plan identity; edits to action, sizing, policy, capability, account, venue, or expiry produce a different plan identity. UNKNOWN and partial exposure remain reserved and block additional opening risk.
- RiskPolicyV2 revalidation binds the current V1 and V2 policy hashes and requires a current account snapshot, exposure reconciliation, possible-risk aggregates, and underlying open/reservation sources. Exposure evidence must be no more than one second old at cutoff. The rolling interval is `(cutoff - 24h, cutoff]`; ACTUAL losses are summed independently, so profits do not replenish allowance. SIMULATED and COUNTERFACTUAL outcomes are excluded. UNKNOWN/PARTIAL exposure and the existing V1 reservations/intents are included; effective opening concurrency remains one.

## Venue qualification

### Bybit

- Engineering implementation: `TESTED`. The installed rc5 Bybit API exposes `BybitNativeTpSlParams` with stop-loss, trigger-basis, order-type, and full-position fields; the submit API surface exposes `reduce_only` and `position_idx`. The preserved V1 `BybitProtectionPort` remains narrow; normal orders continue through Nautilus.
- Actual testnet qualification: `UNVERIFIED`, `TEST GATE`. No Bybit testnet credentials or authenticated qualification harness were available. No account profile hash, account identity hash, fee/filter revision, or authenticated evidence refs exist. BTCUSDT/ETHUSDT one-way isolated USDT linear perpetual, IOC, full-position MarkPrice stop, partial-fill protection quantity, execution reconciliation, funding, and query-retention behavior remain unverified against an actual account.
- The capability artifact has 20 required Bybit rows. Its SHA-256 is `568aa60ba10d072d977a0cf0690ad89464eb8ea768261234bc027980f2c02c69`.

### Binance

- Engineering implementation: `TESTED` for the installed rc5 surface and required-capability matrix. Source inspection of the pinned adapter found a separate conditional algo-order path; it does not establish an entry-attached native protection contract.
- Native protection: `TEST GATE`. No second protection transport or OMS was added. Binance actual testnet qualification is `UNVERIFIED`; no authenticated credentials, account identity, or qualification evidence was available. Binance remains unsupported for capital until venue-native entry protection and its partial-fill/reconciliation behavior are established.
- The capability artifact has 17 required Binance rows. It shares the SHA-256 above; there is no qualified profile instance/hash.

### Installed Nautilus identity and official documentation

- Installed distribution: `nautilus_trader==2.0.0rc5`, Python `cp312-cp312-manylinux_2_34_x86_64`, source commit `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`.
- Compiled `_libnautilus.cpython-312-x86_64-linux-gnu.so` SHA-256: `a53dd5a24fe77f4c66169af84ee9c7e292018010922e3dce9a8b6c9f7f3b7021`.
- Installed Bybit adapter stub SHA-256: `7e12c3cfafbd6821b899464f1be0995d21976d5cc43dfe95701e87e3c4c2a83b`. Binance adapter stub SHA-256: `f9dc108e1a772abb74a7517b65d474f59705afa070302e527596088e3cc01d1f`.
- Official sources checked against the pinned artifact include the [NautilusTrader 2.0.0rc5 release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc5), [Bybit order creation](https://bybit-exchange.github.io/docs/v5/order/create-order), [Bybit position trading-stop](https://bybit-exchange.github.io/docs/v5/position/trading-stop), [Bybit position mode](https://bybit-exchange.github.io/docs/v5/position/position-mode), and Binance [USDⓈ-M trade](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade), [account](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account), and [user-data stream](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/user-data-streams) documentation. The matrix also records the exact pinned adapter source URLs.
- Testnet/demo environments actually exercised: **none**. Capability fixtures and offline adapter/source checks are engineering evidence only; authenticated venue evidence refs remain empty.

## Recovery, protection, and fencing

- UNKNOWN submit, negative lookup, lost ACK, late-fill, duplicate-event, partial-fill, cancellation race, stale-protection, emergency-exit, and flat-certificate behavior is exercised by deterministic local V1/runtime tests and the V1 fault harness. These tests are not authenticated venue evidence. UNKNOWN retains the original 32-character client-order identity and reservation; time or an empty lookup alone does not authorize retry or release.
- Existing V1 recovery requires durable reconciliation evidence for flat certification; contradictory later evidence reopens recovery. Existing reduce-only closing paths and protection-deadline behavior remain unchanged. Protection/closing actions still require their persisted V1 authorization and reconciliation facts.
- The failure matrix reports offline engineering as `TESTED` and actual exchange fault injection as `UNVERIFIED`. It covers GUI/research/model failure, persistence and send boundaries, UNKNOWN recovery, partial/cancel races, protection failures, metadata/book staleness, changed policies/capabilities, and external positions/orders. Matrix SHA-256: `a8b8ecb5cf55b85af5ed8aa3ec169a5c4973565e3b1ec85263f2f25df75fa91b`.
- Cross-host writer fencing is `BLOCKED BY ENVIRONMENT`. The local writer lock and journal epoch fence only local writers; a larger local epoch cannot fence another host. Replacement procedure: externally disable the old host’s ability to authenticate/send using the qualified operational fence; retain evidence that the old writer cannot trade; reconcile venue orders, executions, positions, protection, and cash; preserve unresolved risk; only then start/recover the replacement writer and issue a new local epoch. Resume new risk only after current recovery and protection evidence pass. This procedure was documented, not exercised.

## Validation and evidence

- Full repository run on the final implementation: **837 passed, 3 skipped** (`pytest -o addopts='' -q --tb=no`). The skips are the existing opt-in V1 venue check and two Session-024 testnet gates.
- Session-024 focused tests: **31 passed, 2 skipped**. The skipped Bybit/Binance tests did not contact a venue because explicit opt-in, credentials, and a credential-safe qualification harness were absent.
- Existing complete V2 tests within the aggregate run: **392 passed**; Session-023 **90**, Session-022 **47**, Session-021 **35**, and Session-020 **19**. The Session-020 E2E/IPC seam is included. Existing V1 suite: **414 passed, 1 skipped**. The standalone V1 golden recomputation test passed (**1 passed**).
- Focused frozen-identity regression group: **108 passed**. An independent check recomputed the current S1/S2/S3/S4/S6, selector, analogue, and discovery identities and matched all ten preserved/new Session-023 identity entries, including the evidence matrix and holdout population.
- Ruff passed repository-wide. Mypy passed for **183 source files plus all focused Session-024 tests**. `compileall` passed. `git diff --check` passed. `pip check` found no broken requirements. Hash-locked offline install dry-run passed with no dependency changes.
- `requirements-lock.txt` remains unchanged at SHA-256 `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`.
- Credential-pattern scan covered the ten implementation and qualification-artifact files, found zero high-confidence matches, and printed no matched values. Testnet credential presence was checked as booleans only; no credential values or account identifiers were recorded. `.env.example` placeholders were not changed. No authenticated testnet request or mainnet order was made.
- Session-024 phase gate SHA-256: `b5a6715136ec2aa405c802eff3e0c5a24676ed716065097cb7bdcf9c76dd30f0`. Phase-4 engineering, bridge implementation, RiskPolicyV2 binding, UNKNOWN/recovery, failure matrix, and V1 regression are `TESTED`; Bybit and Binance actual testnet status remain `UNVERIFIED` / `TEST GATE`; Binance protection remains `TEST GATE`; cross-host fencing is `BLOCKED BY ENVIRONMENT`; economics are `NOT ESTIMABLE`; capital remains disabled.

## Preserved identities

- S1 `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`
- S2 `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`
- S3 `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`
- S4 `327c816792290ceaf64ef6dd6b6e90e0382c04782e1dc7d9ca5895ce7f2ae146`
- S6 `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`
- S1/S2 selector `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`
- Evidence matrix `294b47506e8a7b2a73275a70e13494acd37c4f1216df53e422f0ad863f848b81`
- Analogue `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`
- Session-023 discovery `846fd322e5d0a8d91a4ae659771f7e37f26bcff384508a5895f154a93b0cfac3`
- Holdout population contract `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`

## Remaining work before Session 025

1. The coordinating ChatGPT must independently inspect the actual pushed Session-024 branch-tip SHA, diff, tests, artifacts, and handoff before Session 025 is authorized.
2. If testnet qualification is pursued, provide credentials only through the existing secure environment/config mechanism and add/run a credential-safe harness. Qualify the exact Bybit account/profile and retain redacted request, execution, protection, history, funding, and reconciliation evidence. Do not use mainnet.
3. Keep Binance `UNVERIFIED` / `TEST GATE` for capital unless the pinned adapter or a separately reviewed mechanism proves the required native protection and reconciliation contract.
4. Demonstrate external cross-host writer fencing and its evidence; a local lock/epoch is insufficient.
5. Preserve `NOT ESTIMABLE` economics and disabled capital until independent economic evidence and the explicit final release/RiskPolicy gate exist.

Session 025 has not started.
