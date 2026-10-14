# ATLAS RED R9 local tests and performance

## Regression result

The four disjoint repository partitions exercised 2,924 cases: 2,913 passed, eight were skipped, and three failed. The failures are retained as **TEST GATE / BLOCKED BY ENVIRONMENT**, not waived:

- The mixed 1,024-contract public workload hit `PREVENTIVE_CAPTURE_PRESSURE_STOP`. It reproduced on unchanged R8 source. The safety stop stayed active.
- The normal production Brief/memory projection missed its existing 80 ms budget on both R9 and unchanged R8 source.
- The projection deadline case failed twice and passed once across three R9 isolated runs; it passed once on R8. Its result is timing-sensitive.

The exact-base control compared the same tests and owning modules at R8 `ef40afb0bf7172f703ffd8016a3e0086396e845d`. No queue, storage, or projection threshold was relaxed. The captured JUnit and timing records are in [`transition-r9-local-evidence/full-suite`](transition-r9-local-evidence/full-suite); the comparison is summarized in `failure-reproduction-summary.json`.

Scanner regressions pass **47/47** after the final source edit. This covers prospective registration, denominator semantics, missed-slot durability, deduplication, and restart reconstruction through the existing runner and archive.

The completed repository partitions were run before credential-variable stripping was added to the test launcher. The changed scanner, Gemma adapter, and Demo probe fixtures were then rerun with those three credential variables removed: 47/47 scanner tests and 33/33 combined scanner-runner/Gemma/Demo-probe tests passed; the offscreen desktop smoke also passed in a scrubbed environment. A supplemental full-suite replay with credentials removed was stopped at 39% after about 19 minutes because the test process spent sustained time in ext4 journal-commit I/O wait. It produced no JUnit report and is not counted. Thus the full suite count above is complete, but the full-suite credential-stripped replay remains **TEST GATE / BLOCKED BY ENVIRONMENT**.

## Static and dependency checks

- `ruff check src scripts tests`: pass.
- `python -m compileall -q src scripts tests`: pass.
- `uv lock --check`: pass; 39 locked resolutions.
- `uv pip check`: pass; 35 installed distributions compatible.
- `mypy src`: 53 diagnostics across 17 files, matching exact R8 baseline diagnostics with zero added or removed diagnostics.
- Frozen V1, V2, and Agent V1 authority SHA-256 values match their required values. No dependency or license files changed.

Sanitized command results and mypy outputs are included in the evidence directory and manifest.

## Headless desktop smoke

The existing PySide6 desktop started under Qt's offscreen platform with the credential variables removed from its subprocess environment. The startup/owner-guidance smoke confirmed the storage-preflight control, read-only health indicator, and `STANDARD` discovery-effort default. It reported no credential inputs or exchange execution controls, and started no live run; capital and assisted modes remained false. This checks startup and those controls only. The full live status matrix could not be observed because the production source runtime failed its storage preflight.

## Measured resource and source observations

The host is a two-logical-CPU Intel Celeron N3060. During the completed full-suite partitions, observed job CPU share was 75–78%; the largest partition reached 981,900 KiB peak RSS. The supplemental credential-stripped replay was at 39% when stopped, with its Python process in `jbd2_log_wait_commit`. The full-suite runs are therefore host-specific and do not establish target-system capacity.

The two storage preflight attempts generated 1,536 synthetic frames and measured 24 archive extents plus 24 capture receipts. Sustained mean batch service was 336.992 ms and 326.775 ms; burst mean was 308.589 ms and 327.812 ms, against the configured 200 ms maximum. Archive fsync means were 62.110 ms and 66.032 ms; receipt fsync means were 28.393 ms and 25.199 ms. The maximum-breadth cycle measurement was unavailable, and the production runtime failed closed before opening its writer or source. Exact observations are in `storage-preflight.json`.

The genuine public REST snapshot comprised 2,366 records in 5.186 seconds against a 5-second acquisition budget. The bounded 12-second WebSocket observation delivered 265 frames with zero queue rejections and zero reconnects. These adapter measurements do not establish durable capture throughput or long-run availability. No full-cycle CPU, sustained storage rate, or durable scanner latency was measured.

## Evidence boundary

The successful API probes, fixture regression tests, and short source observations are distinct evidence classes. No full connected source-to-Brief workflow, economic profitability estimate, production periodic scanner schedule, or live Demo position-protection lifecycle was qualified. Economics remain **NOT ESTIMABLE**; the full production chain and venue lifecycle remain **TEST GATE**.
