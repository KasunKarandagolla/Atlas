# ATLAS RED R7 — Binance Futures Demo read results

**Status: TESTED, read-only Demo connectivity.** No orders were submitted.

The repository was verified at `/home/kasun/Music/atlas`. The R7 worktree starts from the latest fetched R6 GitHub tip, `e402e4e81d1d6983d5065a816934f70f66e2c9c0` on `fix/red-r6-full-workflow-demo-qualification-2026-10-13`.

The inherited environment contained `ATLAS_BINANCE_FUTURES_ENV`, `BINANCE_FUTURES_DEMO_API_KEY`, and `BINANCE_FUTURES_DEMO_API_SECRET`. Only presence was recorded. An unauthenticated `GET /fapi/v1/time` to `demo-fapi.binance.com` returned HTTP 200 before the authenticated sequence was attempted.

The existing bounded sequence then completed its server-time GET, signed read-only `GET /fapi/v3/account`, and public `GET /fapi/v1/exchangeInfo`. Each returned HTTP 200. The Demo account response was a JSON object of 3,437 bytes; its contents were not printed, saved, or included in this report. The instrument response was 902,495 bytes with 742 symbol entries, all passing the existing structural check. Clock skew was 122 ms. The sequence persisted no state and submitted zero orders. Timestamps and sanitized counters are in [the probe evidence](transition-r7-evidence/binance-demo-read-probe.json).

## Root cause and fix

The first supported-Python run completed the account read but stopped at `exchangeInfo`: the R6 probe applied a 65,536-byte cap to every response, while the live instrument payload was 902,495 bytes. The over-limit condition raised an uncaught exception and emitted a traceback. The R7 fix preserves the existing 65,536-byte clock/account limits, gives instrument metadata a strict 8 MiB cap, and converts any cap breach into a sanitized failure record. The 8 MiB limit remains finite and the response body is not persisted.

One earlier invocation used system Python 3.10 and failed before making requests because the project requires Python 3.12 or newer. The probe was rerun successfully with the repository Python 3.12 environment.

Authenticated execution lifecycle, order protection, cancellation, restart reconciliation, capital qualification, and live trading remain separate gates. This public instrument response does not establish market-data qualification.
