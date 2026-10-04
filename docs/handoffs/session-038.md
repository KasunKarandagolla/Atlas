# ATLAS S38 public stream reliability handoff

S38 is a bounded corrective engineering session for the failed owner Windows public-source run `8b83143aec6549f5b504b0f5e7e703a9`. The failed run remains retained and is not rewritten as a pass.

The independently verified starting point was `impl/session-037-final-engineering-intelligence-closure` at `a51a302e09bda84449ab3867d28df545d39445f2`. Its S36 ancestry was verified. Work was performed on `fix/session-038-public-stream-reliability`; implementation commit `56153a61ba5125af1355e866505f96387c843fe7`.

The root cause was composition starvation. The writer drained at most 32 frames once, then synchronously performed the bounded Bybit REST snapshot (the live report measured 4.95 seconds mean and 6.453 seconds maximum), maintenance, reporting and a one-second sleep. The producer queue therefore reached its 512-item ceiling. Overflow is intentionally sticky and terminal for that producer, so later cycles correctly remained `INCOMPLETE_SNAPSHOT`, stale or missing rather than fabricating continuity.

The correction gives the existing writer a cooperative service seam. A single bounded REST worker returns immutable snapshots while the writer services the FIFO handoff at a maximum 20 ms wait interval. The writer also services the stream at event and maintenance boundaries and during the existing idle interval. A failed stream batch is fail-closed and requires a writer restart; in-memory continuity state is not reused as if a rolled-back durable batch had committed. No second SQLite writer was introduced.

Every drained frame is first retained as exact bytes in a bounded `PublicStreamTransportBatchV1` archive. This preserves unbound or malformed frames for reconstruction. The existing typed L2/trade archives and continuity reports remain authoritative for interpreted evidence. A shared atomic composition reduces commit overhead. Exact immutable identities use memoized hashes, and valid parsed frames no longer pay a redundant `FRAME_RECEIVED` transition; malformed and empty frames retain explicit evidence. The live replay cache is a bounded recent window; older identity resolution remains through durable indexes and fails closed.

Historical producer errors are now applied only when their timestamp belongs to the active connection epoch. A stale error cannot poison a fresh epoch, while disconnect, overflow, sequence faults and unsupported Bybit completeness remain explicit. Service telemetry records frame and call counts, maximum service gap and duration, and the transport batch reference.

Periodic read-only tuning export moved to a one-slot background worker. It never opens a second writable repository, never queues unbounded reruns, and publishes only a sanitized stable error class. The writer remains the sole persistence owner.

Validation performed:

- 47 focused S32/S38 stream, acquisition, report-worker and continuity tests passed in the first seam run.
- Contract and integration tests, including `tests/contract`, `tests/integration`, `tests/v2/test_contracts.py`, S32 stream tests and S38 tests passed (68 tests, two pre-existing optional skips).
- Ruff passed for the changed source and tests.
- Mypy passed for all changed files and S38 tests.
- `compileall` passed for source and tests.
- The offline fixture profile measured 0.63 CPU seconds for a cold 32-frame interpreted batch after the hot-path correction; the separate 2048-item synthetic-cache profile is retained as diagnostic evidence, not a qualification claim.

Frozen authority remains unchanged: capital and assisted execution are disabled, model/critic authority is `ZERO`, RiskPolicy and leverage semantics are unchanged, Bybit trade completeness remains `NOT ESTIMABLE`/`TEST GATE`, and no provider or model version was silently changed. No live provider call or private credential was used.

The package and owner retest are still gates. This handoff therefore remains `DEVELOPMENT NOT READY` until the exact native Windows package is produced and the owner repeats a genuine public run. The target result is actual sustainable receipts and qualified health from evidence; no health gate was weakened.
