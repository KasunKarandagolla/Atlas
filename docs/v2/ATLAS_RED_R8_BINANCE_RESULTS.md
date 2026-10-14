# ATLAS RED R8 Binance results

## Cloud Demo read

The current managed network policy was restricted and reported `policy_state: unknown`. The Demo host was allowlisted. One bounded unauthenticated GET to `https://demo-fapi.binance.com/fapi/v1/time` through the managed proxy returned HTTP 451 without a valid server time. Per the request, all further Demo reads stopped.

- Server-time GETs: 1; HTTP 451.
- Signed account reads: 0.
- `exchangeInfo` reads: 0.
- Other Demo reads: 0.
- Order-changing requests and orders submitted: **0**.

HTTP 451 does not prove an invalid API key. Because the request did not yield valid clock evidence, no signed request was attempted and no account, position, margin or open-order state was observed.

## Genuine production market data

The production Binance host `fapi.binance.com` was absent from the allowed-host list. Production public market requests: zero. No R8 product/universe metadata, BTC/ETH context, altcoin observation, finalized bar, stream, freshness, archive/replay, gap or reconnect evidence was obtained. Status: **BLOCKED BY ENVIRONMENT**.

R7's local Linux read sequence returned HTTP 200 for server time, signed Demo account and Demo exchange metadata; 742 instrument entries passed structural validation. Those R7 reads were local, not Cloud or production-market observations, and are not counted as R8 success.

## Demo execution, protection and supervision

The R8 run submitted no orders. Binance-specific entry/fill reconciliation, UNKNOWN/lost-ACK handling, native conditional stops, stop quantity coverage, cancel/fill races, restart with exposure, emergency flatten and verified-flat cleanup were not qualified. Status: **TEST GATE**. Local-only stop logic is not treated as native exchange protection, and no real Demo order is authorized by this evidence.

The frozen V1 comparator and existing read-only boundary were preserved. The sanitized request record is [cloud-preflight.json](transition-r8-evidence/cloud-preflight.json).
