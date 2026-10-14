# ATLAS RED R9 Local API Qualification

**Result: `API_SETUP_PASS`**, based on the required inherited environment markers and genuine bounded requests. No production key, private payload, signed URL, or order request was used or saved.

## Environment and Gemini

All seven required variables were present. `GEMMA_MODEL` was exactly `gemma-4-26b-a4b-it`; the Free Tier marker was `true`; the Demo environment and read-only approval markers matched their required values. Values and secret lengths were not recorded.

The metadata probe returned HTTP 200, `models/gemma-4-26b-a4b-it`, and `generateContent`. The smoke then made exactly one generation request: HTTP 200, exact returned model, text present, JSON parse and response schema valid, the synthetic `ABSTAIN` content matched, finish reason `STOP`, 117 prompt / 105 candidate / 222 total tokens, 1,001-byte request body, 758-byte response, and 4,752.690 ms generation latency. No response text or raw provider body was saved. The smoke's metadata GET also returned HTTP 200 in 6,457.998 ms; the earlier standalone metadata probe was a separate one-GET qualification.

`ATLAS_GEMINI_FREE_TIER_VERIFIED=true` is the inherited owner attestation for the intended no-charge project. The Gemini response does not identify its Google project; that binding is therefore recorded as owner-attested, not independently echoed by the provider. The request used no paid fallback, tools, retry, private market data, or non-synthetic content.

## Binance Futures Demo

The bounded sequence to `demo-fapi.binance.com` returned HTTP 200 for server time, signed read-only `/fapi/v3/account`, and public `/fapi/v1/exchangeInfo`. The validated clock skew was 625 ms. The account payload was parsed in memory only; it was not printed, indexed, or persisted. The probe submitted zero orders.

The bounded exchangeInfo-only follow-up returned HTTP 200 and 742 symbols: 604 `TRADING`, 61 `SETTLING`, 64 `PENDING_TRADING`, 12 `DELIVERING`, and one `PRE_SETTLE`. Of those, 528 were `TRADING` USDT perpetuals with a minimum-notional filter; 527 also had positive price and quantity filters. BTCUSDT, ETHUSDT, and 0GUSDT had exchange status `TRADING` and valid price, quantity, minimum-notional, and order-type metadata. These are exchange metadata findings, not account, margin, or execution authorization.

The first server-time attempt failed at the network layer and stopped before signed reads. DNS resolution and a TLS 1.3 handshake to the exact Demo host succeeded. After adding a sanitized transport-exception class to the probe and testing it with a fixture, one bounded recheck completed all three approved GETs. No blind retry or order-changing call occurred.

## Binance production public data

The existing `BroadPublicCycleSourceV2` and `BinanceUsdMPublicReaderV2` path reached the documented production REST host without credentials. Server time and exchangeInfo returned HTTP 200. The source parsed 927 metadata rows into 658 USDT linear perpetual products, of which 525 were `TRADING`; it retained status distinctions and product filters. The actual source cycle observed 525 book-ticker rows, 658 mark/index/funding rows, 525 24-hour ticker rows, and 658 product-metadata records. BTCUSDT, ETHUSDT, and 0GUSDT had causal receipt times and translated tick/quantity/notional filters. The latest receipt was 2,014 ms old at summary time. Raw payloads were not saved by the probe.

The source reported 2,366 records and a complete bulk-market snapshot. Its measured cycle was 5,185.917 ms, 185.917 ms over the declared 5-second acquisition budget; bar and history enrichment were deferred. This is recorded for the subsequent performance and cycle-bound review.

The existing `PublicStreamSourceV2` captured a 12-second unauthenticated WebSocket observation from `fstream.binance.com`: 265 frames (BTCUSDT 110, ETHUSDT 150, 0GUSDT 5), zero rejected frames, zero reconnects, 230.555 ms p50 and 445.884 ms maximum event-to-receipt latency. The bounded source closed successfully. Frames stayed in process memory and were not saved.

## Focused checks

- Gemma adapter tests: 17 passed. New model identity and legacy-job tests failed against the old 31B pin, then passed after the 26B correction.
- Demo read-probe tests: 5 passed, including network-error redaction and status/filter classification.
- No repository-wide test suite was run before the API setup gate.

Sanitized machine-readable records are in [`transition-r9-local-evidence/api-qualification.json`](transition-r9-local-evidence/api-qualification.json). The provider's project binding remains owner-attested; full public bar history, a production Decision Brief, and downstream engineering qualification are outside this API gate.
