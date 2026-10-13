# ATLAS RED R6 performance results

## Full repository run

The final full suite collected 2,914 cases: 2,906 passed, zero failed, and eight were skipped. Wall time was 821.494 seconds; child CPU was 729.44 user seconds and 34.68 system seconds; peak child RSS was 1,728,376 KiB (about 1.65 GiB). The slowest test was the synthetic production M1/S3 closure case at 249.785 seconds, followed by the synthetic maximum-breadth case at 110.573 seconds. No production network or AI provider request occurred during the suite.

## Synthetic queue, stream, and storage probes

The mixed broad workload covered 1,024 synthetic contracts. It offered, accepted, drained, and delivered 157 frames with zero rejects. Handoff queue high-water was 27 of 512 items; capture high-water was six batches of a 64-batch limit, with a 16-frame batch limit. Maximum measured runtime service gap was 1.047 seconds against a 1.5-second test budget. During a 0.505-second injected commit stall, 34 frames were captured. Archive stall was 1.000 seconds, the maximum archive flush was 64 rows, and read-only report export completed in 0.904 seconds. The test itself marks `capacity_qualified=false`; these are synthetic fixture measurements, not a live feed qualification.

The synthetic 403-row metadata test compressed two rows and reduced metadata bytes from 807,696 to 395,508 (51.0%). Total SQLite size fell from 1,937,408 to 1,527,808 bytes (21.1%). These measurements do not establish rolling production storage growth.

## Not measured

No real market network latency/bytes, Binance capture queues, reconnect behavior, 72-hour continuity, 24-hour storage growth, Gemma latency/tokens/cost, authenticated Demo latency, or Windows resource use was measured. There is no production bottleneck claim; the slowest observed work was CPU-bound synthetic test workload. Detailed raw and machine-readable metrics are in `transition-r6-evidence/performance-results.json`.
