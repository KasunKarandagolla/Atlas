# G1-B broad observation performance correction

**Offline engineering result: G1_SERVICE_CORRECTED on the tested synthetic maximum-breadth workload; overall G1 remains TEST GATE.** This isolated branch starts at the exact independently reviewed `e75a83226cea2cf8dfee10823541848e8ffb0de4`. The supplied PASS applies only to that parent’s reviewed engineering corrections. It does not accept this change or qualify a real host, live source, endurance, T+5, economics, policy or capital.

The change removes a second exact metadata read of the same immutable public observation indexes during one broad cycle. On the registered 4,096-product, two-venue maximum, the unchanged ten-second service limit now passes in all three low-overhead cycles: **9.364, 9.775 and 9.734 seconds**. Each run retained and independently re-read all 24,576 raw/index observations and 40,977 domain artifacts. The logical store identity and all 384 Parquet file hashes are byte-for-byte equal between the unmodified and corrected runs.

## Why the cycle exceeded cadence

The maximum fixture creates 4,096 synthetic products (2,048 per venue), 8,192 source records per cycle and 24,576 source records over three cycles. The unchanged A helper measured 21.264, 23.106 and 24.531 seconds for its original adoption-plus-workset timer. B put low-overhead timers around the production adoption path, actual acquisition-receipt lookup and workset publication; the exact-index audit runs afterward and remains part of the run’s full byte/readback checks.

| Measurement | Cycle 1 | Cycle 2 | Cycle 3 | Samples |
| --- | ---: | ---: | ---: | ---: |
| A: original G1 service timer | 21.264 s | 23.106 s | 24.531 s | 3 |
| B before: adoption | 19.188 s | 19.173 s | 20.386 s | 3 |
| B before: receipt lookup | 0.005 s | 0.004 s | 0.006 s | 3 |
| B before: workset publication | 2.433 s | 3.120 s | 3.430 s | 3 |
| B before: production service boundary | 21.626 s | 22.296 s | 23.821 s | 3 |
| B after: adoption | 6.537 s | 6.506 s | 6.420 s | 3 |
| B after: receipt lookup | 0.004 s | 0.005 s | 0.004 s | 3 |
| B after: workset publication | 2.823 s | 3.264 s | 3.310 s | 3 |
| B after: production service boundary | **9.364 s** | **9.775 s** | **9.734 s** | 3 |

The B service boundary is adoption start through workset completion. It retains Parquet writes/fsync, SQLite WAL/FULL persistence, receipt and reconciliation artifacts, stream-service callbacks and workset publication. It excludes fixture construction and the independent receipt/index, restart and raw-archive audits; those checks still run and their timings/outputs are preserved separately. No row, required product or durable write was removed from the service path. Three service samples support min/median/max only; p95/p99 would imply unsupported tail precision.

The exact source trace identified the repeat. The adoption path read every persisted public index in 32-reference pages and validated the exact references. `PublicCollectorV2.reconcile_after_reconnect` then requested the same complete source reference set again in 128-reference pages. A low-overhead callsite trace measured 64 repeat lookups for the 8,192 references, taking 12.471 seconds in aggregate (median 209.6 ms, maximum 271.3 ms). The original one-cycle cProfile call graph reported 840 `get_artifact_metadata_by_refs` calls, including those 64 reconciliation pages; `artifact_entry_from_storage_row` and compact `decode_metadata` each ran 16,384 times.

## Correction and preserved behavior

`ProductionOpsCyclePortV1._persist_broad_public_snapshot` now retains the exact source-index entries returned by its existing post-archive readback, grouped by source, for the lifetime of that adoption call. It passes those values through a private collector parameter to the existing bounded reconciliation loop. The collector checks the exact ref set and repeats its artifact-type, hash-shape, availability and source-identity checks against the supplied values. Callbacks still run before every original 128-reference page. Other reconciliation callers keep the existing repository-read path. Incomplete or source-mismatched durable rows fail the existing source acceptance path and prevent healthy reconciliation; the private cache path independently rejects an incomplete exact-reference set or mismatched source.

The retained entries come from the same sole-writer repository’s durable post-archive read, and public index rows are immutable. The new values are discarded at the end of that cycle. No schema, serialization, evidence grade, clock, source authority, health transition, population, callback schedule, retention rule or storage constant changed. A regression test proves the cached path does not fetch those observation refs again, preserves all three service callbacks for 257 references, and rejects a mismatched source. Existing default-read reconciliation and broad conflict/failure tests remain in the affected regression selection.

The corrected three-cycle B mean is 9.625 seconds, versus 22.581 seconds before the change: **57.4% lower** for the same 4,096 products and 8,192 rows per cycle. The 2,048-product Binance-only comparison moved from 16.685 seconds to 5.321 seconds in one sample; it confirms scaling improvement but is not a tail estimate. The 1,024 Binance-only baseline was 6.276 seconds. The 4,096 Binance-only fixture is outside the existing 2,048-products-per-venue limit: persistence retained its 12,288 rows, then workset publication correctly rejected it with `PER_VENUE_UNIVERSE_POPULATION_OVERFLOW`. No cap was changed to admit that synthetic profile.

After the repeat was removed, one-cycle cProfile showed 776 metadata lookup calls and 8,192 metadata decodes, down from 840 and 16,384 on the same maximum population. The remaining profiled work centers on durable archive flush and canonical serialization (`flush_archive` 5.794 s cumulative and `canonical_json` 5.443 s cumulative under the instrumented run). Those inclusive cProfile times overlap and are not service-certified timings. The low-overhead maximum already passes; archive and immutable encoding work were left intact.

## Observer effect and resources

The existing qualification `ResourceSampler` samples every 50 ms and recursively stats files. On the unmodified three-cycle maximum run, the B median moved from 22.296 to 25.587 seconds when this sampler ran, a 3.291-second / 14.8% increase (`n=3`). After the correction, the sampled and unsampled ranges overlap: 8.288–9.937 seconds with the sampler and 9.364–9.775 seconds without it. Treat these as observer comparisons, not a precise isolated sampler coefficient.

The combined cProfile plus 100 ms `/proc` sampler increased the unmodified A-style service observations to 53.137, 53.380 and 52.988 seconds. The post-change one-cycle instrumented service was 15.822 seconds versus 9.364 seconds in the corresponding first low-overhead B cycle. Thus C materially perturbs the service boundary and is diagnostic only. The post-change C profile sampled 208 times; it is not a three-cycle p95/p99 measurement.

With the existing 50 ms sampler, the maximum pre-change sample showed 439,549,952-byte peak RSS, 449,220,608-byte process HWM, 438,963,200-byte PSS, 438,571,008-byte private memory, 7,992,832-byte sampled WAL and 138,763,967 bytes across files under the probe root. The corrected run showed 432,578,560-byte peak RSS, 443,449,344-byte HWM, 431,985,664-byte PSS, 431,587,328-byte private memory, the same sampled WAL and the same root-file total. These are Linux cloud fixture measurements, not a 4 GB host certificate.

## Storage and retained evidence

The correction does not reduce storage growth. Each maximum cycle retains 8,192 source rows and 1,959,592 raw payload bytes.

| Cycle | SQLite logical growth | Closed Parquet growth | Combined logical growth | `/proc/self/io` process writes |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 39,391,232 B | 3,113,139 B | 42,504,371 B | 823,439,360 B |
| 2 | 37,580,800 B | 3,114,006 B | 40,694,806 B | 1,037,242,368 B |
| 3 | 37,584,896 B | 3,113,974 B | 40,698,870 B | 1,085,104,128 B |

The sampled WAL was 7,992,832 bytes per cycle. The corrected run’s raw IDs, exact source-reference IDs, indexed domain rows, worksets, logical-store hash and every Parquet SHA-256 match the unmodified B run. The `/proc/self/io` values total approximately 2.946 GB across the helper; they are process-reported writes, not device-level physical I/O or a wear certificate.

The unchanged report-local S41 assessor uses the 42,504,371-byte maximum sample, 17,280 ten-second cycles, 1.5× dynamic-metadata overhead and 1.25× reserve. Its 48-hour diagnostic requirement remains **1,377,141,631,200 bytes**, with the owner reserve set to zero. This is a conditional synthetic projection, not measured endurance or a production retention budget. The existing Linux host-path preflight recorded 30,218,625,024 free bytes against its separate 62,930,192,327-byte minimum and failed `INSUFFICIENT_DISK_HEADROOM`; the actual 4 GB computer has not been tested. `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` remains `None`. No retention, eviction, queue or reserve policy was changed.

## Profile and decision matrix

The following Binance-only baseline profiles use the fixed synthetic three-row Binance source shape and one cycle each. They are not claims about the live Binance listing count.

| Synthetic Binance products | Source rows | Baseline B service | Corrected B service | Status |
| ---: | ---: | ---: | ---: | --- |
| 128 | 384 | 0.562 s | not repeated | Baseline point |
| 512 | 1,536 | 2.347 s | not repeated | Baseline point |
| 1,024 | 3,072 | 6.276 s | not repeated | Baseline point |
| 2,048 | 6,144 | 16.685 s | 5.321 s | Corrected profile passes in one sample |
| 4,096 | 12,288 | 39.443 s through adoption/receipt; workset rejected | not repeated | Outside frozen per-venue universe cap |
| 2,048 Binance + 2,048 Bybit | 8,192 per cycle | 21.626–23.821 s | **9.364–9.775 s** | Three-cycle configured maximum stress passes on this offline cloud fixture |

**Session conclusion:** G1’s production-equivalent service boundary is corrected for the tested and admitted maximum stress fixture. Overall G1 remains **TEST GATE** because the existing storage projection is far above observed cloud free space, the actual 4 GB target and Windows resource path are unverified, source timing/continuity and endurance were not run, and the full native decision-to-T+5 path remains unmeasured. This session did not collect market data, activate OBSERVE, or advance economic claims.

After independent review of the exact pushed SHA, the next smallest infrastructure action is the offline maximum-breadth retest on the selected physical host with its real storage path. If the real host-path preflight rejects disk reserve, stop for a separately reviewed storage-contract decision; do not alter `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2`. Public collection still requires a new, separately bounded owner request after host and source gates pass. The existing G2 genuine P1 provenance and typed-reader work remains Sol-owned.

See [the measurement contract](G1B_MEASUREMENT_CONTRACT.md), [retest procedure](G1B_TARGET_HOST_RETEST.md), [unmodified A baseline](BASELINE_A_UNMODIFIED.md), and the `evidence/` JSON runs. Exact artifact hashes are recorded in `evidence/SHA256SUMS`.
