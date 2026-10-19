# G1-B offline performance measurement contract

Recorded before new G1-B benchmark or profile runs on 2026-10-19 (Asia/Colombo). Source checkpoint: externally reviewed commit `e75a83226cea2cf8dfee10823541848e8ffb0de4`. This file registers comparison boundaries only; it does not change production requirements or admit a smaller population.

## Fixed profiles

All product counts below are named synthetic fixtures made with the existing `test_session041_scale_review.py` source shape. They are not claims about the current live Binance universe.

| Profile | Population | Cycles | Purpose |
| --- | ---: | ---: | --- |
| `G1A_MAX_TWO_VENUE_4096` | 2,048 Binance + 2,048 Bybit | 3 | Exact unmodified G1 maximum-breadth helper call |
| `G1B_BINANCE_128` | 128 Binance | 1 | Lower-scale point |
| `G1B_BINANCE_512` | 512 Binance | 1 | Intermediate point |
| `G1B_BINANCE_1024` | 1,024 Binance | 1 | Intermediate point |
| `G1B_BINANCE_2048` | 2,048 Binance | 1 | Intermediate point |
| `G1B_BINANCE_4096` | 4,096 Binance | 1 | Binance-only upper fixture |

The original profile is run before edits to benchmark or production source. Its workload, source records, transaction rules, timer placement, exact receipt/index checks, population checks, and restart readers remain the G1 parent definition. Profile extensions must retain the same Binance three-row record shape and all native collector, archive, receipt, SQLite, workset, and readback owners. The production cadence remains ten seconds.

## Measurement modes

**A — Unmodified G1 benchmark.** Invoke the unchanged parent helper as `measure_bounded_cycles(root, per_venue=2048, cycles=3)`. Preserve each existing adoption and workset timer, exact rows, and post-cycle checks. Also record the helper's elapsed wall time to expose audit and setup cost outside its production-service timers.

**B — Low-overhead production-path timing.** Use the same deterministic inputs and source/build hashes. Time only existing source preparation (separately), the complete `_persist_broad_public_snapshot` call, and the complete `publish_broad_workset` call with monotonic timers. Do not run cProfile or recursive resource sampling during these intervals. Report the sum and full interval from the start of adoption to workset completion, as well as setup and audit work outside that boundary. Do not remove durable writes, FULL/WAL settings, fsync, receipt/index creation, or mandatory publication.

**C — Instrumented diagnostic.** Repeat the identical stress input and transaction behavior while collecting hierarchical function/call counts, inclusive and exclusive CPU/wall samples, resource samples, SQLite statement/transaction counts, file/archive bytes, and filesystem writes. Compare the same top-level service boundary with A/B. Record the observed overhead as `C - B`; do not present instrumented timings as service certification.

The run records at least p50/p95/p99/max for repeated samples, and explicitly reports `n` where it is too small for meaningful tail percentiles. It separately reports raw bytes, canonical observation/index/archive bytes, logical SQLite growth, WAL and temporary growth, physical writes exposed by `/proc/self/io`, RSS/PSS/private/HWM, CPU, and restart/readback cost. Resource sampling remains outside timed service calls in B; C states its sampling cadence and overhead.

Every result includes the source revision, dirty-diff identity if any, interpreter/dependency versions, fixture-profile ID, input/population hashes, exact command, timing-boundary diagram, timer/call counts, failures, and output hashes. Exact identifier/population/receipt/chronology and independent raw/archive/readback checks run after the service boundary and are reported separately.

No network, private source, provider, paid service, model inference, execution, activation, or public collection is part of these profiles. No capacity constant, retention obligation, source population, cadence, evidence grade, or authority changes under this contract.
