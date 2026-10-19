# Baseline A — unmodified G1 maximum-breadth fixture

Run 2026-10-19 (UTC), on the isolated G1-B branch before benchmark or production code edits. Source fixture and production owners are the exact `e75a83226cea2cf8dfee10823541848e8ffb0de4` blobs recorded in [the complete run JSON](evidence/benchmark-a-unmodified.json). Command: `measure_bounded_cycles(root, per_venue=2048, cycles=3)`, with network and child-process audit events denied.

This preserves the parent helper's original service timer definition and all its persistence and verification operations. The service figure below is the original adoption timer plus the original workset-publication timer. Per-cycle exact receipt/index checks run between those timers; setup, input construction, and full immutable-domain restart verification run outside them. This is why the 99.06-second whole-helper wall time is not the ten-second cadence figure.

| Cycle | Products | Raw/index rows | Adoption | Workset publish | Original service sum | SQLite logical growth | Archive growth | `/proc/self/io` writes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4,096 | 8,192 | 18.799 s | 2.465 s | 21.264 s | 39,391,232 B | 3,113,139 B | 823,414,784 B |
| 2 | 4,096 | 8,192 | 19.827 s | 3.278 s | 23.106 s | 37,580,800 B | 3,114,006 B | 1,037,234,176 B |
| 3 | 4,096 | 8,192 | 20.997 s | 3.535 s | 24.531 s | 37,584,896 B | 3,113,974 B | 1,085,112,320 B |

There are only three cycles; min/median/max are descriptive, not stable p95/p99 claims. Service time increases across this run. A later G1 result recorded 21.5–23.7 seconds; this reproduction is 21.26–24.53 seconds, so it confirms the breach and its variability rather than a faster passing result. No profile in this run meets ten seconds.

Each cycle preserves all 8,192 generated raw records and public indexes. Across three cycles that is 24,576 records. After restart, the unchanged helper verifies every workset cutoff and full universe, every exact public-index reference/metadata payload, and every stored immutable domain entry; it reports 40,977 total indexed domain artifacts verified. All rows were retained, with zero rejected receipt indexes. Final database plus archive logical growth was 40,698,870 B in cycle 3. Process-reported writes total 2,945,761,280 B over the helper, including SQLite index/WAL work and audit/recovery operations; this is not a device-level physical-write certificate.

The original fixture prebuilds synthetic `RawObservationV2` inputs before each adoption timer. It does not time public transport, venue parsing, acquisition, enrichment, decision work, or a real stream-service callback. It therefore establishes the cost of the existing persistence/controller/workset path only. Independent baseline resource sampling was not enabled, and the helper does not produce per-function call attribution. Those are addressed only in C, with their observer cost stated separately.
