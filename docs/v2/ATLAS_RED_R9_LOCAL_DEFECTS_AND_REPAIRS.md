# ATLAS RED R9 local defects and repairs

## Reproduced and repaired

### Future scanner slots were absent from the durable denominator

The R8 ledger registered only after a due scan started. The new test first failed because `ScannerRunner.schedule_future_slots` did not exist. The existing `ScannerSlotLedger` and `ScannerRunner` now register a prospective aligned horizon before work begins, preserve policy and eligible-universe-scope identity, reconstruct the horizon after restart, and mark only registered incomplete slots missed after their deadlines. Exact progress rows are deduplicated and must reference the durable slot registration. A deterministic summary reports due, completed, missed, failed, source-unavailable, expired, incomplete, and future registered counts.

The repair does not synthesize past registrations. The existing `missed_slots` method remains a non-durable diagnostic and is not treated as the expected-slot denominator. The focused red case is green; scanner tests pass 47/47.

The API is wired and exercised through the existing `ScannerRunner` and durable archive, but this checkout has no periodic production scheduler entrypoint that invokes `schedule_future_slots` and `reconcile_missed_slots`. A live whole-calendar population is therefore still **TEST GATE**.

### Demo probe hid transport failure classification

The first bounded Demo time read failed before an authenticated call. The probe now reports a sanitized exception class; a fixture verifies the classification does not expose request secrets. A single diagnosed recheck completed the approved time/account/exchangeInfo GET sequence. No blind retry or order call occurred.

### Gemma adapter still selected 31B

The provider was pinned to the old 31B identity. Focused tests failed against that source pin, then passed after the minimal 26B selection correction. The current adapter rejects 31B as a current configured/returned identity while preserving old job hashes and historical evidence; it does not relabel prior responses.

### Connected production fixture stops at the typed-Brief safety gate

Added `test_session056_r9_connected_fixture.py` against the existing `ProductionOpsCyclePortV1` and `OpsSupervisorV2`, using their documented source/input fixture ports. It carries a persisted candidate, exact action and evaluation through the production Brief/memory projection, asks the real `ResearchMemoryV1` job builder to compile the result, and reopens the repository. The production Brief is blocked because typed S47 sections are not reconstructed; the job builder returns no job, no provider intent or Demo order intent is persisted, and restart replay returns the same Brief/memory refs. The test and existing fake Gemma-to-memory-to-Demo request fixture pass 2/2 together under `SYNTHETIC_INTEGRATION` evidence.

This verifies the connected fail-closed outcome. It does not repair the missing typed Brief source readers or create a successful production-bound Gemma job. That successful single composition remains **TEST GATE**.

## Open reproduced gates

- The production runtime rejects the ext4 path because sustained and burst batch-service means exceed the registered threshold; maximum-breadth storage capacity is unmeasured. No guard was weakened.
- Genuine production source records were not durably passed into the scanner or Brief because the runtime stopped before source construction.
- The complete production Brief, bounded job permit, real market research, economics, and the single connected end-to-end composition remain **TEST GATE** or **NOT ESTIMABLE**.
- Binance conditional-order behavior, protection coverage, UNKNOWN-state recovery, and restart with exposure were not qualified against a live Demo position. Offline contracts do not replace that gate.

### Repository suite resource failures

The complete partitioned run recorded 2,924 cases, 2,913 passes, eight skips, and three failures. `test_mixed_1024_contract_public_workload_under_bounded_stalls` reached the runtime's `PREVENTIVE_CAPTURE_PRESSURE_STOP` on the available two-core Celeron host. Both `test_normal_production_run_persists_brief_and_memory_without_dispatch` and `test_deadline_reached_during_projection_expires_without_changing_receipt` exceeded the existing 80 ms projection budget on R9 at least once. The unchanged tests and production modules were run on exact R8 source: mixed capacity and normal projection failed there too, while the deadline test passed once. Repeated R9 deadline runs produced two failures and one pass. These are recorded as host capacity and timing **TEST GATE** results; no queue, deadline, or projection guard was widened.

No frozen authority or S47 decision was changed. Detailed test outputs and final gate classifications are in the R9 evidence manifest.
