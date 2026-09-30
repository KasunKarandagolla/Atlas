# Session 032 — Public Stream Continuity and Recovery Engineering

## Checkpoint

- Repository: `KasunKarandagolla/Atlas`.
- Branch: `impl/session-032-public-stream-continuity-recovery`.
- Accepted starting tip: `cd0cf72fa1dedca765c298a281633ee1b7056614` on `impl/session-031-public-shadow-campaign-readiness-evidence-controls`.
- S31 tested implementation ancestor: `659e9833753602c77c877bd1fc08b18e848eb43f`.
- S32 implementation commit exercised by all final regression commands: `41f0d063efd130b988918953eb22837203e6b3f2`.
- The implementation commit was created from the independently fetched and verified accepted tip. S32 did not exist remotely at preflight. S27, S30 and S31 accepted ancestry was verified. The final documentation commit and exact pushed remote tip are reported in the session closeout response after fresh remote verification.
- No merge was performed. No history was rewritten. User-owned consultation/freeze files and archive were left untracked and unstaged.

The three accepted authority documents were read in full and their hashes match: V1 `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`, amended V2 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, and Agent `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`. The 33-page `ATLAS_72_Hour_Consultation.pdf` was read in full and remains consultation only. The eight-week prospective floor and 200 genuinely matured-opportunity minimum remain unchanged.

## Implementation

S32 connects the existing allowlisted public WebSocket capture to the existing supervisor-owned production collector through an explicit opt-in factory, `create_bybit_public_ws_port()`. Its only subscriptions are Bybit USDT-linear `BTCUSDT` and `ETHUSDT` public trade and depth-50 book channels. The ordinary `create_production_port()` remains archive/index-only, and `create_bybit_public_port()` remains the S31 REST path. The opt-in producer starts from the supervisor recovery lifecycle.

`PublicStreamSourceV2` provides one bounded producer thread/event loop, a finite retry budget, bounded backoff, a byte/item-capped immutable handoff, actual local receipt timestamps, exact frame bytes and SHA-256, explicit topic identity, overflow/error/heartbeat/disconnect observations, and bounded controller drains. It cannot access credentials, repositories, SQLite, archives, accounts, orders, or capital. The accepted `websockets==17.0.1` dependency is reused; dependency locks did not change. The producer was exercised with a fake in-memory source only.

`PublicStreamContinuityReportV1` records source, full instrument/contract revision, exact channel, metadata identity, recovery epoch, as-of cutoff, source health, local book sequence, trade observations, BBO freshness, gaps, capability semantics, repair limits, and strategy qualification as separate fields. The existing Bybit translator is reused: `u` update IDs are checked locally, `seq` is not promoted into a stronger continuity guarantee, nonconsecutive `u` and `u=1` resets fail closed, and restart/reconnect requires fresh local snapshot state. A one-frame cold snapshot does not make a valid BBO report.

For trades, `i` is an idempotency identity, not a replay cursor; `seq` can be shared across grouped records. Identical duplicates are idempotent, conflicting payloads and out-of-order observations are classified, and observed rows are durably archived through the existing collector/archive path. There is no declared historical cursor or repair capability. A disconnect, restart, heartbeat timeout, queue overflow, malformed frame, or unacknowledged controller buffer creates durable uncertainty. S32 creates a durable initial checkpoint before producer startup so frames that remain queued and are lost across a restart cannot disappear without an uncertainty record. No gap-repair receipt is manufactured.

The dedicated `PublicStreamTradeObservationIndexV1` keeps WebSocket trade observations outside the generic bar-decision observation index and shared public-source health requirement. This prevents an unqualified stream from blocking existing S1/S2 REST decisions or being confused with S3 continuity. Stream observations do not qualify strategy input. The S2 2,901 contiguous M15-bar minimum and S3 10,081 M1 observation, trade, VWAP and residual requirements are unchanged. No native S3 M1 production handoff was added.

### Exact implementation diff

- `src/atlas/v2/data/bybit_source.py`
- `src/atlas/v2/data/collector.py`
- `src/atlas/v2/data/public_microstructure_ws.py`
- `src/atlas/v2/data/public_stream_continuity.py`
- `src/atlas/v2/data/public_stream_source.py`
- `src/atlas/v2/runtime/ops_supervisor.py`
- `src/atlas/v2/runtime/production.py`
- `tests/v2/test_session032_public_stream_continuity.py`
- `tests/v2/test_session032_public_stream_integration.py`
- `tests/v2/test_session032_public_stream_source.py`
- `tests/v2/test_session032_public_ws_acquisition.py`

## Worker ownership

- **Worker A — bounded intake:** owned `public_microstructure_ws.py`, `public_stream_source.py`, and the acquisition/source tests. Its focused command, including existing S22 WebSocket coverage, passed 42 tests with no skips or failures. Ruff, module mypy and compileall passed. It did not commit, push, or use a live network.
- **Worker B — continuity and recovery:** owned `public_stream_continuity.py` and its focused tests. Fifteen tests passed with no skips or failures. Ruff, module mypy and compileall passed. It did not commit, push, or use a live network.
- **Main agent:** owned the production/supervisor integration, collector persistence changes, integration test, all reviews, final regression, documentation and Git operations. The focused S32 suite passed 31 tests; the changed-seam set, including S27/S30/S31 paths, passed 152 tests. Worker file ownership was disjoint; integration edits were sequential and no conflicts remain.

## Validation

Final regressions ran after implementation was frozen and committed, against `41f0d063efd130b988918953eb22837203e6b3f2`:

| Gate | Result |
| --- | --- |
| Focused S32 suite | 31 passed, 0 skipped, 0 failed |
| Changed-seam integration set | 152 passed, 0 skipped, 0 failed |
| Full V2: `.venv/bin/python -m pytest -q -o addopts='' tests/v2` | 633 passed, 2 skipped, 0 failed; 1,385.93 seconds |
| Full non-V2: `.venv/bin/python -m pytest -q -o addopts='' tests --ignore=tests/v2` | 485 passed, 3 skipped, 0 failed; 345.95 seconds |
| Contract/V1 golden command | 10 passed, 0 skipped, 0 failed; 6.60 seconds |
| Ruff: `.venv/bin/ruff check .` | Passed |
| Mypy: `.venv/bin/mypy src` | Passed; 207 source files |
| Compileall: `.venv/bin/python -m compileall -q src tests` | Passed |
| Dependency check: `.venv/bin/python -m pip check` | Passed; no broken requirements |
| `git diff --check` | Passed |

The two V2 skips are optional offline SDK MockTransport tests whose required `openai` and `httpx2` modules are absent from the core environment. The three non-V2 skips are two authenticated venue qualification cases behind `ATLAS_RUN_VENUE_QUALIFICATION=1` and the explicit public-testnet-connectivity opt-in. No test credentials or network were used. The exact locations and reasons are recorded in the [validation artifact](../v2/SESSION032_PUBLIC_STREAM_CONTINUITY_VALIDATION.json).

The V1 golden JSON remains `be2a54d2bf9a3a168fe850877d51ef241d8ae8cdad837b872270cc3a30166251`. `requirements-lock.txt` remains `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`; `requirements-agent-lock.txt` remains `47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3`. A value-suppressing scan of the staged implementation changes found zero private-key, provider-token, bearer-literal, or credential-assignment matches; matched values were not printed.

The complete source capability/continuity limitations are in the [engineering gate](../v2/PUBLIC_STREAM_CONTINUITY_ENGINEERING_GATE_V1.json) and [validation record](../v2/SESSION032_PUBLIC_STREAM_CONTINUITY_VALIDATION.json). Both separate transport receipt, current source health, local order-book sequence, observed trades, completeness proof, and strategy qualification. They state that Bybit trade completeness is unproven and S32 strategy input remains unqualified.

## Limits and safety

- Only deterministic fake streams were used. Real public networking, exchange cadence, reconnect behavior on a live host, and host endurance are `UNVERIFIED / TEST GATE`.
- No seven-day trade history, full historical VWAP continuity, native M1 decision coverage, qualified mature outcomes, or incremental economic value is claimed. Economic value remains `NOT ESTIMABLE`.
- Owner Windows/WSL qualification, full source/cadence, historical warmup, a continuous matured-outcome producer, 72-hour endurance, and prospective economics remain separate gates.
- No real public-network smoke, credentials, authenticated venue/account request, order, paid provider call, or persistent campaign was used or started.
- Capital and assisted execution remain disabled. The critic remains hidden, nonblocking and zero authority. The protected holdout remains untouched. No arbitrary S3 promotion is possible.
- Maximum ceiling: `ENGINEERING_PASS`, subject to independent coordinating review. Passing tests and this handoff are not acceptance.
- No merge, real-public campaign, capital authorization, or Session 033 is authorized.

The detailed versioned evidence report is [SESSION032_PUBLIC_STREAM_CONTINUITY_VALIDATION.json](../v2/SESSION032_PUBLIC_STREAM_CONTINUITY_VALIDATION.json). The exact source-specific capability gate is [PUBLIC_STREAM_CONTINUITY_ENGINEERING_GATE_V1.json](../v2/PUBLIC_STREAM_CONTINUITY_ENGINEERING_GATE_V1.json).
