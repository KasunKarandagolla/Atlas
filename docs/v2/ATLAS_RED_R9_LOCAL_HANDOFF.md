# ATLAS RED R9 local handoff

**Branch:** `fix/red-r9-local-complete-cycle-2026-10-14`

**Base:** R8 `ef40afb0bf7172f703ffd8016a3e0086396e845d`
**Status:** API setup passed. Full engineering promotion is not qualified.

## Completed

- Qualified the inherited Gemini and Binance Demo credentials with bounded real read requests, and observed genuine Binance public REST and WebSocket market data. See [API qualification](ATLAS_RED_R9_LOCAL_API_QUALIFICATION.md).
- Updated the Gemma adapter to select `gemma-4-26b-a4b-it` while retaining legacy 31B job identity handling and rejecting model mismatches.
- Extended the existing scanner ledger with prospective scheduled slots, restart reconstruction, idempotent overdue-miss recording, registration-bound progress, and a registered-slot denominator. Scanner tests pass 47/47.
- Reproduced and repaired the first-provider registration gap in the new prospective calendar test. No source capture or scanner work was attempted through the production runtime after its owner preflight rejected the selected ext4 path.
- Ran the repository suite in the R8 four-part file layout: 2,924 cases, 2,913 passes, 8 skips, and 3 resource/timing failures. Two failures also reproduce on exact R8 source; the third is timing-variable on R9. See [tests and performance](ATLAS_RED_R9_LOCAL_TESTS_PERFORMANCE.md).

## Qualification gates

- **API_SETUP_PASS:** genuine Gemma 26B synthetic `ABSTAIN` generation; signed Demo account read; Demo exchange metadata; public production REST and a 12-second public WebSocket observation. No order-changing request was sent.
- **TEST GATE:** production `run_component` stops at `OWNER_STORAGE_PREFLIGHT_REJECTED`. Ext4 batch-service means exceeded the registered 200 ms limit, and the maximum-breadth capacity measurement remains unavailable. The guard was preserved.
- **TEST GATE:** no durable genuine source-to-Decision-Brief run, no connected 21-stage Gemma workflow, no live scanner denominator populated by production capture, and no exact Demo execution plan. The prospective calendar and restart repair are implemented and tested through `ScannerRunner`, but no periodic production scheduler entrypoint is wired in this checkout.
- **TESTED, synthetic fail-closed composition:** existing production ops with fixture market inputs persisted the action/evaluation and blocked Brief/memory, refused a Gemma job, showed Demo unavailable, and replayed after restart. The separate existing complete-Brief fake-Gemma/memory/Demo-request fixture passed. A successful single market-to-Gemma-to-Demo chain remains **TEST GATE** because the production Brief is blocked on unreconstructed typed sections.
- **NOT ESTIMABLE:** genuine after-cost opportunity economics and the eight-week/200-matured-opportunity floor.
- **TEST GATE:** Binance native protection/reconciliation/restart lifecycle remains fixture-qualified only. Orders submitted: zero.
- **TESTED, limited scope:** the offscreen PySide6 startup/owner-guidance smoke found the storage-preflight control, health indicator, and `STANDARD` discovery-effort default; no live run started. It does not qualify the full runtime status matrix.
- **BLOCKED BY ENVIRONMENT / TEST GATE:** the 1,024-contract mixed workload hit the existing preventive capture stop on this two-core desktop; the 80 ms optional Brief/memory projection exceeded budget in timing-sensitive tests. No throughput or protection limits were relaxed.
- **TEST GATE / BLOCKED BY ENVIRONMENT:** the completed full suite predates explicit credential-variable stripping; changed scanner/Gemma/Demo fixtures and desktop smoke passed with credentials removed. A supplemental scrubbed full-suite replay stopped at 39% during sustained ext4 journal I/O wait and produced no JUnit result.
- **UNVERIFIED:** the exact runtime model label could not be independently inspected. No subagents were used.

The promotion ladder remains `INTEGRATED → ENGINEERING_PASS → HISTORICAL_DIAGNOSTIC → PROSPECTIVE_SHADOW → INCREMENTAL_VALUE_PASS → DECISION_ELIGIBLE`. This work does not promote Atlas or change trading authority. S47 remains `PROPOSED — NOT ACCEPTED`; effective discretionary policy remains `NONE`.

Additional evidence is in [end-to-end](ATLAS_RED_R9_LOCAL_END_TO_END.md), [defects and repairs](ATLAS_RED_R9_LOCAL_DEFECTS_AND_REPAIRS.md), [execution and protection](ATLAS_RED_R9_LOCAL_EXECUTION_PROTECTION.md), [tests and performance](ATLAS_RED_R9_LOCAL_TESTS_PERFORMANCE.md), and the [evidence manifest](ATLAS_RED_R9_LOCAL_EVIDENCE_MANIFEST.json). The sanitized storage rejection is in [`storage-preflight.json`](transition-r9-local-evidence/storage-preflight.json).
