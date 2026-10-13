# ATLAS RED R6 Binance Demo results

## Configuration and probe boundary

The current Cloud status is connected, restricted, and policy-enforced. `demo-fapi.binance.com` is allowed; production `fapi.binance.com` is not. The configured environment value was checked as `DEMO_ONLY`, and dedicated Demo credential presence was checked without reading or printing either value. The probe now uses `urllib.request.ProxyHandler()` so the managed proxy policy applies; it pins `https://demo-fapi.binance.com`, rejects redirects, keeps TLS verification, limits response bodies to 65,536 bytes, and has no production fallback or order path.

Offline endpoint, HMAC, identity, and response contract tests pass. A new probe regression confirms that a denied clock read is sanitized and prevents a signed account request. A second fixture test exercises the successful read-only sequence and confirms the API-key header appears only on the signed account request, never on public clock or instrument metadata requests.

## Actual Demo requests

Two actual public `GET /fapi/v1/time` attempts returned HTTP 451 (`Unknown`). Attempt 1 predates timestamp instrumentation; its timestamp was not captured and its response body was not read or stored. Attempt 2 began at `2026-10-13T18:07:52.643759Z`; HTTP 451 was observed at `2026-10-13T18:07:53.402061Z`. Both used the environment-managed proxy. The response does not identify whether the proxy or upstream service issued the status.

- Signed account GETs: **0**.
- Public instrument-information GETs: **0**.
- Position, open-order, fill, or conditional-order reads: **0**.
- Order-changing requests: **0**; **ZERO ORDERS**.
- Account/exposure state: **UNKNOWN**, because no authenticated account response was received.
- Fills, protection, and terminal reconciliation: **NONE OBSERVED / NOT RUN**.
- No state or account payload was persisted or printed; no credential was printed.

The second attempt stopped before constructing or sending the signed account request because the server-time gate failed. The signed account read and public instrument read must be completed after the approved Demo clock endpoint becomes reachable. No trade plan was prepared and no order-changing approval was used. Full order lifecycle, protection, restart, and reconciliation remain TEST GATE; this report does not treat a prepared request as an executed trade or protection as confirmed.

Exact safe evidence and the timestamp gap for attempt 1 are in `transition-r6-evidence/demo-read-probe.json`.
