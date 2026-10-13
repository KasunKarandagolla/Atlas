# ATLAS RED R4 integration handoff

Machine-readable evidence inventory: [`ATLAS_RED_R4_EVIDENCE_MANIFEST.json`](ATLAS_RED_R4_EVIDENCE_MANIFEST.json).

Date: 2026-10-13
Branch: `impl/red-r4-gemma-demo-integration-2026-10-13`
Starting commit: `4faf9a065eb00df705694bc107401c41c0b322b2`

## Engineering status

| Area | Status | Evidence |
|---|---|---|
| Google native Gemma adapter | IMPLEMENTED | [`gemma_native.py`](../../src/atlas/v2/research/gemma_native.py) |
| Durable production-Brief shadow intent/result record | IMPLEMENTED | [`memory.py`](../../src/atlas/v2/research/memory.py) |
| Futures Demo request-generation boundary | IMPLEMENTED | [`binance_futures_demo_r4.py`](../../src/atlas/runtime/binance_futures_demo_r4.py) |
| Typed account, instrument, margin, order, fill, and protection readback contracts | IMPLEMENTED | [`binance_futures_demo_r4.py`](../../src/atlas/runtime/binance_futures_demo_r4.py) |
| Fixture-backed manual composition check | TESTED | Fake Gemini response, SQLite restart readback, DEMO_ONLY signed request construction; one synthetic model response, no network |
| New pytest suites | TEST GATE | Test files are present; this runtime has no `pytest` executable or module |
| Complete ten-stage production-composition end-to-end path | TEST GATE | Current fake composition does not execute public-source ingestion, production A0, risk sizing, exact-action evaluation, or full position protection/reconciliation |
| Whole-tree `compileall` | TESTED | `src`, `scripts`, and `tests` compile under Python 3.12.14 |
| Pinned lock/package consistency | TESTED | `uv lock --check --offline` resolves 39 locked packages; `uv pip check` reports 131 installed distributions compatible |
| Freeze hashes, diff whitespace, custom secret-pattern scan | TESTED | Three exact freeze hashes match; `git diff --check` is clean; all 12 staged files scanned with zero pattern findings; `gitleaks` is unavailable |
| Ruff and baseline-aware mypy | TEST GATE | Neither module is installed in this runtime |
| Actual Gemma model identity/capabilities | UNVERIFIED | No model endpoint call occurred |
| Real Binance public market data | BLOCKED BY ENVIRONMENT | Current enforced network policy has no Binance host grant |
| Authenticated Futures Demo read | BLOCKED BY ENVIRONMENT | No demo credential binding is configured and the demo host is outside the enforced allowlist |
| Rolling 24-hour write ceiling | NOT ESTIMABLE | The container exposes cumulative device writes, not the required rolling window |
| Full promotion suite and static gates | TEST GATE | pytest, Ruff, and mypy are unavailable here; no dependency installation was performed |

The coordinator model was confirmed by the owner as GPT-6 Luna. No subagents were used.

## Starting point and frozen authorities

The implementation branch started from owner-verified R3A completion commit `4faf9a065eb00df705694bc107401c41c0b322b2`. That commit descends from S49 and the R3A implementation line. The pre-change commit diff contained only the R3A completion handoff, evidence manifest, and root logs.

The attached source bundle SHA-256 is `bd3e258007ab6cc8ceee7d8b564320e4acc0ed86066fc6a6c65e0947fad8b825`. All requested owner sources were read. The archive omitted two optional consolidated chat instructions; every requested index, target lock, owner decision, transition architecture, ADR-001/002/003/005/006/007/008, and runbook section 2/3/4/5/7 was present and matched its manifest size and hash.

The three frozen authorities match their required SHA-256 values:

- V1: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`
- V2: `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`
- Agent: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`

The S47 comparative-selection amendment remains `PROPOSED — NOT ACCEPTED`, with effective identity `NONE`. DeepSeek remains the preferred future production decision provider. A0 remains deterministic and unchanged.

## Implemented provider path

`GeminiNativeClient` uses Google's native `generateContent` REST shape at the pinned host `generativelanguage.googleapis.com` and exact model `gemma-4-31b-it`. It sends the key in the `x-goog-api-key` header. The transport pins the model path and host, refuses redirects, disables retries, bounds response bytes, applies deadlines, normalizes status errors, and drops raw error bodies. It exposes no arbitrary URL or tool surface.

The public Google API discovery document was fetched twice through the enforced proxy: 384,161 response bytes per fetch, 768,322 bytes total. It documented `POST v1beta/{+model}:generateContent`, `responseSchema`, `responseMimeType`, and `maxOutputTokens`. These were documentation fetches only; they did not call a model or authenticate to an account. Wire overhead was not observable.

Capability probes are implemented to report model lookup identity, text-bearing content, structured JSON parsing, usage metadata availability, sanitized provider errors, and body-byte counts. Actual model availability and these capabilities remain UNVERIFIED because no model request occurred. Read-only tools, cancellation, and pricing are reported as unsupported or unverified unless separately established. No Gemma tool request is sent. The S47 validator binds proposals to the exact Brief snapshot, cutoff, candidate, action hash, and registered evidence. Only `SELECT_ONE`, `REJECT_ALL`, or `ABSTAIN` can pass.

The smoke authorization and research authorization are separate. Smoke requests require free-tier evidence, no-billing authorization, at most two calls, 4 KB input, and 256 output tokens. A shadow research call uses a one-call permit bound to a durable intent and the existing S47 job budgets; smoke ceilings do not become research policy. Both paths require explicit authorization. No real call was made because this session could not verify the configured project's free-tier/no-billing state.

`ResearchMemoryV1.build_production_gemma_job` compiles only the persisted `COMPLETE` production Brief into a typed S47 job under the existing writer. `prepare_gemma_shadow_dispatch` binds that job to the same production Brief and memory identity, then writes a one-call intent with its source population and decision-calendar references. `record_gemma_shadow_result` stores only a validated proposal, sanitized status, returned identity, usage metadata when returned, and transport body-byte counts. It stores no raw provider response or prompt. Result authority is `ZERO_AUTHORITY_SHADOW`. The worker and writer remain separate, so model latency does not enter the synchronous 80 ms projection path. A repeated or uncertain intent returns an audit-only disposition with no automatic retry. Since the current production Brief is `BLOCKED`, the new compiler returns no job in this session.

The current production Brief is `BLOCKED`; its genuine indexed M15 technical fact remains preserved. Missing required typed fact families and exact supported economics prevent dispatch. The actual Brief adapter therefore does not dispatch Gemma in this session.

## Implemented Futures Demo boundary

`DemoOnlyAdapter` has a separate `DEMO_ONLY` identity, hardcoded REST `https://demo-fapi.binance.com`, and WebSocket identity `wss://demo-fstream.binance.com`. It rejects other endpoint values and exposes signed GET request construction only. It has no network sender, order submitter, cancel sender, or endpoint fallback. Its read allowlist contains only explicit Futures account/reconciliation GET paths. Timestamp, receive window, parameter ordering, HMAC-SHA256 signature, and API-key header construction are typed and fixture-testable. Typed fake-response contracts normalize account balances/positions, MARKET_LOT_SIZE filters, margin settings, regular order states, fills, and conditional-stop observations. Atlas's existing `verify_binance_protection` and durable `FillDeduplicator` are used for protection verification and duplicate/restart fill evidence; no new risk or protection engine was added.

`DemoOnlyCredentials.from_environment` accepts only `BINANCE_FUTURES_DEMO_API_KEY` and `BINANCE_FUTURES_DEMO_API_SECRET` after requiring the `DEMO_ONLY` identity. It rejects live, testnet, and legacy alias environment names and endpoint overrides. Fixture credentials require an explicit test-only switch. The new state-path helper places its SQLite file, order state, logs, and account snapshots under a dedicated `DEMO_ONLY/binance-futures` root.

Demo order requests are typed and generation-only. Known order states are normalized; unknown and missing states raise. Cancellation is represented only by a fake observed `CANCELED` response; there is no cancellation request path. The R4 fixture suite calls Atlas's existing protection verifier and durable fill deduplicator for fake stop verification and restart duplicate handling. The duplicate-fill restart subset ran manually; the verifier assertions remain a `TEST GATE` because pytest and Nautilus Trader are absent. No exchange request, demo order, live order, private account read, cancellation, fill, or protective stop was sent. Existing Atlas lifecycle, protection, risk, and reconciliation components remain unchanged and retain their existing closed entry gates.

The runtime reports current HTTP network policy as `enforced` with `generativelanguage.googleapis.com` as the task-specific host. Neither `fapi.binance.com` nor `demo-fapi.binance.com` appears in its allowed host set. The new adapter also contains no production REST hostname and has no send method. Together these provide explicit evidence that live Binance routes were unreachable from this session and cannot be selected through this adapter.

## Test evidence and limitations

The new fixture suites are:

- [`test_session055_gemma_native.py`](../../tests/v2/test_session055_gemma_native.py)
- [`test_session055_r4_synthetic_e2e.py`](../../tests/v2/test_session055_r4_synthetic_e2e.py)
- [`test_session055_r4_production_gate.py`](../../tests/v2/test_session055_r4_production_gate.py)
- [`test_session055_binance_demo_only.py`](../../tests/runtime/test_session055_binance_demo_only.py)

They cover native model lookup/request shaping, returned identity, structured output, usage metadata, malformed/missing responses, prompt-injection binding, tool rejection, deadlines, response bounds, rate-limit normalization, credential redaction, request ceilings, DEMO_ONLY HMAC/path guards, typed account/instrument/margin/order/fill/protection response contracts, simulated cancellation and partial fill, durable duplicate-fill restart, offline order generation, and unknown exchange state. A manual fake-response composition check exercised the S47 provider validator, durable SQLite intent/result and restart readback, a DEMO_ONLY signed request, and request-only order construction. The pytest files are present but could not be executed here; only the separate manual fixture check is TESTED. All evidence is synthetic.

`pytest` was unavailable (`python -m pytest` reports `No module named pytest`). Hypothesis, Nautilus Trader, DuckDB, and PyArrow are also absent; a direct production-fixture import stopped on missing Hypothesis. Ruff and mypy executables are absent. The S47/S48, R1 golden, R2 capture, production lifecycle/recovery, and complete repository suites were not run in this session. The 563/563 R3A scoped result is historical evidence, not a new test pass. A synthetic production-path regression uses the existing 80 ms projection budget and accepts its safe `DEFERRED` disposition; the budget was not increased.

No authenticated Binance response was simulated as real venue evidence. The manual fixture composition joins an S47 Brief fixture, fake Gemma response, durable memory and an offline Demo-only request. It does not compose public-source ingestion, causal feature qualification, deterministic production A0 selection, independent production risk sizing, exact-action evaluation, a genuine production Brief, or a protected-position lifecycle/reconciliation path. It does not establish actual Futures Demo execution. The public source transport already exists in Atlas; this runtime could not access its Binance host. No real public Binance observation was received.

## A0 equivalence and remaining R3A gates

The owner-verified R3A evidence at [`final-a0-cross-commit-equivalence.json`](transition-r3a-completion-evidence/final-a0-cross-commit-equivalence.json) records exact reference, content-hash, timestamp, and metadata equality between accepted R2, R3A disabled, and R3A completion-enabled snapshots. Each contains one receipt, three candidate sets, one sizing decision, one action, one evaluation, one decision-calendar entry, and zero `DecisionCalendarV2` objects. The evidence records all three equality flags as true for the exact listed artifact sets. This R3A result is historical. **R4 full A0 identity equivalence remains a TEST GATE:** the production regression that snapshots A0 around a blocked dispatch is present but pytest was unavailable. The manual fixture confirms its Brief hash and economics are unchanged; it does not claim a new full A0 hash comparison. R4 code writes separate Gemma shadow memory artifacts and does not mutate A0, economics, the original decision calendar, sizing, admission, protection, or execution.

Remaining R3A gates:

- Whole-schedule denominator and missing expected slots remain `NOT ESTIMABLE`; no denominator was reconstructed from surviving receipts.
- Required typed production Brief evidence and exact economics remain a dispatch gate; unsupported or blocked Briefs cannot reach Gemma.
- Durable invalidation/replay for this new provider shadow result has no R4 regression and remains a `TEST GATE`.
- R4-specific Decision Calendar and complete A0 artifact-set/hash equivalence remain a `TEST GATE`; the preserved calendar references and synthetic Brief comparison are not a full production replay.
- The complete execution lifecycle path through demo fill, protection, reconciliation, restart, and fail-closed kill behavior remains a `TEST GATE`; R4 only normalizes synthetic responses and reuses the existing Atlas fill/protection components.
- Optional projection retains its 80 ms default. Historical default-budget trials were deferred at 83.4/92.6/85.0 ms; the timeout was not raised.
- The historical R2 cadence failure remains a `TEST GATE`; its original raw failure was unavailable and ten controlled trials did not reproduce it.
- The full repository suite remains a `TEST GATE` for engineering promotion.

## Actual requests and resource evidence

- Gemini model/API calls: **0**. No API key value was read, printed, persisted, returned, or committed.
- Google API discovery GETs: **2**, with **384,161 response bytes each**. These are documentation requests, not provider calls.
- Binance public data observations: **0**, because the enforced network policy blocked Binance hosts.
- Authenticated Futures Demo requests: **0**. No demo credential was configured in this runtime.
- Demo or live orders: **0**.
- Rolling 24-hour writes: `NOT ESTIMABLE`. Cgroup device writes are cumulative and cannot establish the rolling window; no compliance claim is made.
- Provider token usage and price: `UNVERIFIED`; no provider response was received.
- The direct fake composition/typed readback process used 0.354 CPU seconds and peaked at 33,380 KiB RSS. A separate fake capability probe carried a 761-byte request body and 515 response-body bytes across two mock responses; an invalid-schema fixture used 315 response-body bytes. These are fixture measurements, not provider traffic. The two documentation GETs returned 384,161 bytes each; total wire overhead was not observable.
- CPU/RSS for a complete test run, total network wire bytes, and service impact under live load: `NOT ESTIMABLE`.

## Precise next step

After independent review of this pushed branch and explicit owner approval for read-only access, configure a separate trusted runtime with only Futures Demo credentials in `BINANCE_FUTURES_DEMO_API_KEY` and `BINANCE_FUTURES_DEMO_API_SECRET`; verify those credentials against Binance Futures Demo and confirm that no live/testnet credential aliases or endpoint overrides are present. Allowlist only the documented Demo hosts for that runtime, then run signed read-only `/fapi/v3/account` and `/fapi/v1/exchangeInfo` requests with bounded receipts. Keep the order sender absent. Actual demo order placement remains a separate `TEST GATE` until the endpoint guard, kill switch, fill-time protection, cancellation, duplicate-event handling, recovery, and restart reconciliation are independently verified and explicitly approved.

No merge, deployment, provider smoke request, authenticated exchange request, or order was initiated by this session.
