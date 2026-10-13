# ATLAS RED R6 supervision and recovery

R6 corrected the inherited source cycle that prevented event receipts, expiry records, and `OpsSupervisorReceiptV1` from being durably published for the fake Bybit path. Typed product metadata is now registered only after the corresponding archived observation is validated; the derived diagnostic identity is no longer treated as an artifact dependency. Restart, deadline, rate-limit, disconnect, receipt-idempotency, and public-source readiness regressions pass in the final 150-case focused group and full suite.

Binance command readiness remains enforced. The original three acknowledgement/error cases used a dispatcher without the required readiness proof; generic dispatch coverage now uses the Bybit path, and Binance error coverage uses a valid readiness context. No readiness check was removed.

All restart, fill, protection, UNKNOWN, and kill behavior observed in R6 came from offline tests and synthetic fixtures. The read-only Demo clock gate returned HTTP 451, so no account or position response was obtained. Current account, position, exposure, protection, and terminal state are therefore **UNKNOWN / NOT OBSERVED**. Atlas cannot claim a flat account or confirmed protection from this run. **ZERO ORDERS** were submitted.

Remaining supervision gates are: reachable Demo clock and authenticated account reads; complete durable Binance intent/fill/protection/cancel/replace handling; restart reconciliation against exchange state; and fault-injection of actual Demo transport boundaries. No live endpoint or capital authority was enabled.
