# ATLAS RED R9 local execution and protection

## Observed venue facts

The bounded read-only Binance Futures Demo sequence returned HTTP 200 for server time, signed account read, and `exchangeInfo`. Metadata reported BTCUSDT, ETHUSDT, and 0GUSDT as `TRADING`, with price/quantity precision and minimum-notional filters. The public production metadata also exposed the supported conditional order type names. These reads do not establish a current Demo position, account mode, margin state, or order permission.

No order-changing Demo endpoint was called. No exact immutable Demo plan is proposed because native order/readback lifecycle evidence and the full production Brief did not qualify.

## Existing offline architecture

The repository pins `nautilus-trader==2.0.0rc5`. Its tagged adapter documentation routes USDⓈ-M conditional orders through the futures algo-order endpoint, uses a different cancel endpoint for those orders, refuses to blindly retry ambiguous order commands, and documents that Binance futures brackets/OCO are not supported as linked orders. For `closePosition`, the adapter must omit both `quantity` and Binance `reduceOnly`; the Nautilus order still requires the local reduce-only and exact linked-position checks. Existing Binance adapter and recovery tests cover durable outbox commands, single-writer/fencing checks, deterministic client identities, partial fills and duplicate event handling, UNKNOWN outcomes, startup reconciliation, conditional-order identity/readback, close-position stop requests, missing protection, and safe refusal on unresolved evidence. The implementation keeps the Binance-specific conditional order path distinct from the Bybit comparator.

The exact [Nautilus v2.0.0rc5 Binance integration contract](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc5/docs/integrations/binance.md) and [Binance USDⓈ-M trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-m-futures/api/rest-api/trade) were consulted. The Binance docs specify `/fapi/v1/algoOrder` for conditional stop/take-profit orders and separate signed query/cancel contracts. Documentation and offline fixtures are not evidence that this pinned adapter successfully protected a live R9 position.

## Qualification result

**TEST GATE:** no live position, conditional algo order, fill, lost ACK, cancel/fill race, restart-with-exposure, emergency halt, or verified-flat cleanup was exercised. Existing tests are adversarial fixtures, not exchange certification. Protection must remain deterministic and independent of Gemma; no agent received capital or order authority. Zero orders were submitted.
