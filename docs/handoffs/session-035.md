# Session 035 — Public Host and Source Qualification

Date: 2026-10-01
Branch: `impl/session-035-public-host-source-qualification`
Accepted base: `impl/session-034-native-s3-m1-warmup-readiness` at `e13994752f165ad8b42f7f7c441076078d4388cc`
Tested S34 implementation ancestor: `cec7cc253058823a3298e3b09c6afff537c8f504`
Tested S35 implementation: `03bf3f78cf002669003b7f667c5de95bb93fa347`

## A — Repository and Scope

GitHub was fetched before implementation. The accepted S34 remote tip matched the required SHA, the tested S34 SHA was in its ancestry, and no reviewed S35 or newer accepted checkpoint existed. Session 035 was created from that verified base. The owner-provided PDF, freeze documents, and archive remain unmodified and untracked.

The implementation reuses the accepted Bybit REST adapter, public WebSocket adapter, bounded handoff, continuity/archive path, production port, supervisor, S3 forward evidence, CandidateSet/calendar handling, and S33 outcome maintenance. It adds an offline-by-default host runner and a public trade-completeness assessment. The capability matrix, strategy thresholds, and frozen strategy contracts were not rewritten.

## B — Provider Semantics

Official Bybit pages were reviewed on 2026-10-01:

- [V5 publicTrade WebSocket](https://bybit-exchange.github.io/docs/v5/websocket/public/trade): trade identity `i`, cross sequence `seq`; futures/spot messages can contain up to 1024 trades, and multiple messages may share `seq`.
- [V5 recent public trades](https://bybit-exchange.github.io/docs/v5/market/recent-trade): linear page limit is 1–1000. The documented request has no cursor or start/end-time replay parameter.
- [Historical data download](https://www.bybit.com/en/derivative-activity/history-data): linked from the recent-trades page; the reviewed page exposed a dynamic product catalog but no exact trade-set, repair, receipt-time, revision, or correction contract.
- [V5 orderbook WebSocket](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook) and [V5 connection](https://bybit-exchange.github.io/docs/v5/ws/connect): reviewed for the approved book topics and public linear endpoint.

The existing source cannot detect and repair every missed trade interval or prove that an S3 minute VWAP uses a fully supported trade population. A matching bounded REST suffix, healthy connection, increasing `seq`, or valid book sequence does not supply that proof. Trade completeness remains **NOT ESTIMABLE / TEST GATE**. The exact reason codes and contract questions A–J are recorded in [the Session 035 qualification artifact](../v2/SESSION035_PUBLIC_HOST_SOURCE_QUALIFICATION.json). No capability status was promoted.

The single metadata bootstrap request failed before returning an exact current `InstrumentKeyV2` contract revision. The assessment records the requested Bybit MAINNET linear BTCUSDT/ETHUSDT scope, an empty exact-key set, and `BYBIT_METADATA_BOOTSTRAP_UNAVAILABLE`; it does not substitute a fixture or synthetic revision.

## C — Host Qualification

The host probe classified the host as ordinary Linux, not WSL, using a Linux filesystem. The disposable qualification path was writable, had about 105 GB free, and passed exclusive writer-lock acquisition. Runtime SQLite was 3.50.4 with WAL and synchronous FULL. Python was 3.12.13. The UTC clock did not regress. Queue limits and dependency-lock hashes are recorded in the qualification JSON. No absolute root path, username, private IP, MAC, serial, environment value, or credential was recorded.

## D — One Bounded Public Smoke

One explicitly enabled smoke ran for 15.313 seconds against the Bybit public mainnet path, below its 30-second declared duration and 180-second absolute ceiling. It used one WebSocket connection attempt and made one public REST metadata request. There was no automatic retry or fallback.

The transport connection opened, but no subscription acknowledgement or data frame arrived before the bounded acknowledgement deadline. The REST metadata request had no successful response. No recent-trade REST request was made for either symbol. Queue high-water and rejected-frame counts were zero; there were no disconnects before planned shutdown, archived frames, continuity reports, trades, or native decision receipts. The resulting public runtime, REST, WebSocket, book-continuity, and observed-trade gates are **TEST GATE**. This is an environment-limited smoke, not a feed qualification.

The sanitized evidence is in [session-035-public-smoke-summary.json](../evidence/session-035-public-smoke-summary.json). Raw frames were not committed. A read-only Session 031 preflight was run at the smoke cutoff; it found zero public receipts, zero calendar entries, zero matured outcomes, disabled capital/assisted influence, and economics NOT ESTIMABLE.

## E — S3 Status

Trade completeness is **NOT ESTIMABLE** and S3 strategy input readiness remains **NOT ESTIMABLE**. The following S34 contract is unchanged:

- 10,081 contiguous M1 observations;
- 120 strictly preceding residual observations;
- trade-derived VWAP;
- fresh BBO no older than one second.

No smoke observations count toward warmup. No reconstructed history is labeled `ACTUAL_SYSTEM`. S3 warmup, strategy qualification, and prospective shadow were not promoted. S34 CandidateSet/calendar behavior and the S33 outcome path remain intact.

## F — Validation

- Focused Session 035 and affected seams: **97 passed**.
- Contract and V1 golden: **10 passed**.
- One full V2 run: **757 passed, 2 skipped**.
- Ruff, mypy, compileall, pip check, and diff checks passed.
- V1 golden and both dependency-lock hashes match their required values.
- Full non-V2 was skipped because changes are confined to V2 data transport/runtime qualification and V2 tests; shared V1 contracts and dependencies were untouched.

After the full V2 run, the only source adjustment was a typing-only annotation/cast cleanup requested by mypy. The S35 focused tests and final static/dependency checks passed again after that cleanup; no runtime behavior changed.

The smoke summary and the generated gate record exact counters. The value-suppressing secret scan is included in the final repository verification.

## G — Safety and Authority

The smoke was public market data only. The adapter required no exchange credentials; no credential, account, order, provider/model, or paid API call was made. Capital and assisted execution remain disabled. Critic authority is ZERO. RiskPolicy, strategy thresholds, economics claims, and the final holdout were not changed or exercised.

The consultation PDF was treated as consultation only. The accepted economic floor remains at least eight weeks of genuine prospective shadow and at least 200 genuinely matured opportunities, with regime coverage, dependence-aware inference, multiplicity discipline, exact chronology, and a protected final holdout.

## H — Remaining Gates

1. Independently qualify a Bybit trade source with exact replay/repair semantics, or separately review a versioned S3 evidence amendment.
2. Resolve exact current instrument revisions when public metadata is available.
3. Qualify longer live-public continuity and accumulate the frozen S3 forward warmup.
4. Separately authorize any 72-hour endurance run.
5. Complete the accepted prospective shadow and mature-outcome, regime, dependence, multiplicity, chronology, and final-holdout requirements.
6. Complete separate capital and execution qualification.

No merge, 72-hour run, capital authorization, or Session 036 is authorized by this handoff.
