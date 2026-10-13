# ATLAS RED R2 — Lossless Public-Source Delivery Handoff

## Disposition

R2 adds a bounded delivery-path correction and preserves the queue, capture, receipt, and pressure limits. The 1,024-contract mixed integration test and the controlled 160-frame/s archive/receipt stall regression pass on this host. This is a bounded synthetic correctness result. It does not qualify sustained Binance capture, native execution, or release readiness.

Implementation commit: `910d4001323f4a6136af886df618b99a6c8fb8aa`.

## Starting point and authority

The isolated branch `fix/red-r2-mixed-source-delivery-2026-10-13` starts at reviewed R1 HEAD `81ebc2ae67d9e52a79557e01a96517a4956daa64`, the exact remote tip of `fix/red-r1-final-correctness-2026-10-13`. R1 implementation `9930fc834022ce3e8f5bc2239bd60867a251f46e` and S49 ancestor `a73c92d83ca8b2fe5163e636b2c9732361b35e23` are both ancestors. The preflight examined 62 remote-tracking refs; no newer implementation descendant was present. The read-only audit ref `e2d3451a071c124375fa2a8869a4217273686652` diverges from the S49 ancestor and was not used as a base. See `transition-r2-evidence/git-preflight-r2.json`.

Implementation commit `910d4001323f4a6136af886df618b99a6c8fb8aa` was pushed to the isolated R2 branch and independently verified with `git ls-remote`; it exactly matched local HEAD and the R1 starting SHA remains its ancestor. The exact command output and timestamp are in `transition-r2-evidence/push-implementation.json`.

Frozen V1, V2, and Agent V1 documents were read in full during preflight and rehashed after implementation. Before and after SHA-256 values match their required digests: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`, `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, and `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`. No frozen source or accepted historical evidence was edited. AUD-01, AUD-02, and AUD-06 remain separately gated.

The coordinator used GPT-6 Luna as requested. No subagents were used because exact subagent model inheritance could not be verified.

## Cause and rejected hypotheses

Historical S49 evidence records one BYBIT offer rejected as `SHARED_LIMIT` at 480 queued items of the 512 aggregate budget, while BINANCE was idle and retained its protected 32-item reserve. The 16,000,000-byte limit was not reached. The 480 boundary is the correct fairness decision and rejection category; changing reserve allocation would bypass protection rather than repair delivery.

The measured avoidable bottleneck was serial capture durability service. Broad capture sealed one descriptor of at most 16 frames, then synchronously fsynced its archive extent and receipt journal before servicing more frames. Under a deterministic 160-frame/s workload with 80 ms injected after each sync, that path serviced fewer frames than offered. Before the production correction the controlled regression accepted and later drained 1,216 frames, then rejected the next offer at BYBIT's protected-reserve boundary: 1,217 offered = 1,216 accepted + 1 recorded `SHARED_LIMIT` rejection; queue high-water 480/512 items, 74,616 bytes, BINANCE reservation 0, loss latched.

The unchanged mixed test did not reproduce the queue rejection on this host in its two pre-fix runs. Both instead stopped at its later archive-stall wait: the fixture patched `write_chunks`, while current capture reaches `prepare_frame_views`, and its trade seed did not reach the L2 archive path. This is recorded as a fixture/host limitation, not as a reproduced queue failure. Historical S49 evidence plus the deterministic pre-fix delivery regression establish the delivery failure; the mixed fixture was then corrected to drive the actual production archive path with a valid `orderbook.50` frame while retaining its producer rates, deadlines, and core assertions.

SQLite/report head-of-line blocking was not the cause of the controlled queue rejection: capture runs independently, and the final mixed workload recorded capture progress during the injected commit and archive stalls. Byte exhaustion, an aggregate 512-item ceiling, and an incorrect reserve-denial latch were also not supported by the counters.

## Bounded correction

- `durable_public_capture.py` now reuses the existing bounded archive `seal_many` path for at most two FIFO descriptors per group. Each descriptor remains at most 16 frames; each descriptor keeps its own immutable extent, batch reference, frame identity, payload hash, receive/availability timestamps, and FIFO indexes. Grouping uses the existing 200 ms accumulation window and is restricted to the broad 16-frame path when pending descriptor headroom permits it. Pressure-stop behavior and repeated immutable chunk IDs retain the former one-at-a-time path.
- `capture_receipts.py` adds `append_many` for at most four individually framed and checksummed receipts, written in order with one file fsync. A partial or failed append makes that journal writer terminal; the prior prefix remains readable.
- `broad_stream_source.py` now projects the capture worker's terminal `FAILED` state and error code through the broad wrapper. The archive-failure regression first exposed the wrapper reporting `CLOSED` despite `RAW_CAPTURE_FAILED_OSERROR`; the wrapper now preserves that terminal health signal.
- `test_session040_capture_receipts.py`, `test_session041_capture_delivery_batching.py`, and `test_session041_mixed_broad_capacity.py` cover batching, deliberate reserve loss, shared-capacity fairness, fixed sync stalls, partial flush/order, receipt/archive failures, and end-to-end persistence. The mixed test now waits for the production frame-view archive stall and asserts payload hashes and persisted artifact references.

No queue or byte limit, public drain limit, 16-frame broad capture bound, pending descriptor reserve, pressure threshold, producer rate, or deadline was increased. There is no secondary database writer, frame coalescing, aggregate substitution, loss suppression, or provider/venue network activity.

## Red-to-green and conservation

`red-adversarial-160fps-pre-fix/` retains the pre-fix rejection evidence. The final implementation passed two independent repetitions of 1,280 offered frames at measured 159.9997 and 159.9987 frames/s. Each run recorded 1,280 accepted, drained, sealed, and durably reread frames; 0 rejection; clear loss latch; 40 archive sync groups and 40 receipt sync groups, each carrying two extents/receipts. Queue high-water was 28 items/4,368 bytes and 27 items/4,212 bytes. Full raw output is in `repeated-delivery-workload/`.

The final 1,024-contract mixed test passed 1/1. It offered and accepted 157 frames, drained, sealed, delivered, and interpreted all 157 exactly once, with zero rejection. The 156 trade frames matched persisted trade IDs, payload hashes, and artifact references; the remaining frame was the valid order-book snapshot used to exercise the archive path. The test observed a 0.508 s commit stall with 66 frames captured during the stall, a 0.910 s read-only report export, and a 1.000 s archive preparation stall during which captured frames rose from 130 to 157. Universe/workset publication completed through the production service path during capture; the one-page read-only export completed without changing the artifact count. Queue high-water was 25/512 items; capture pending high-water was 6/64 descriptors; maximum interpreted batch was 16.

The grouped archive failure test verifies that 32 accepted and drained frames remain in terminal in-progress state, with 0 captured/delivered frames and `FAILED / RAW_CAPTURE_FAILED_OSERROR`; it does not report those frames as durable delivery. The deliberate reserve test verifies 481 offered = 480 accepted + 1 explicitly rejected `SHARED_LIMIT`, with loss latched. Successful workload conservation is therefore:

`offered = accepted + accurately recorded rejections`; `accepted = drained = sealed = delivered = interpreted` for the mixed successful run, and `offered = accepted = drained = sealed = durably reread` for each repeated 1,280-frame run. Pending/error states remain explicit and are not counted as delivery.

The item cap remains 512, aggregate payload cap 16,000,000 bytes, maximum public drain 64, broad capture descriptor bound 16 frames, and pending descriptor cap 64. The focused matrix exercises the idle protected lane, two busy lanes, exact item/byte rejection boundaries, pressure stop, one- and five-second bounded writer/report stalls, FIFO order, late event times, duplicate trade IDs, 16+14 partial flush, receipt interruption, archive failure, and recovery/restart paths.

## Test results

| Final-code group | Result | Evidence |
|---|---:|---|
| Capture receipts/durability, delivery adversarial tests, pressure reserve, broad source, paging, recovery queue | 63/63 passed | `final-capture-matrix-run3/` |
| New delivery adversarial file, standalone rerun | 5/5 passed | `final-grouped-adversarial-run4/` |
| Session-041 1,024-contract mixed test | 1/1 passed | `final-session041-mixed-r2/` |
| Public runtime integration and Binance qualification | 40/40 passed | `public-runtime-qualification-r2/` |
| Live health, native resilience, owner runtime, recovery adoption | 149/150 passed; desktop smoke failed because `PySide6` is absent | `final-health-recovery-r2/` |
| S27 production/supervisor and decision calendar; S44 M15; S46 peer; S48 protection/recovery; dynamic universe | 137/137 passed | `s27_s44_s46_s48/` |
| R1 V1 goldens/snapshot/qualification, risk/causal, outcome/hash/maturity consumers | 145/146 passed; the one basket outcome failure reproduces on starting SHA `81ebc2ae...` | `consumer-compatibility-r2/`, `baseline-basket-gate/` |

Counts are per command and overlap: the 63-test matrix includes the five new delivery tests, which were also run separately. The Session-041 basket outcome failure is a confirmed pre-existing baseline failure, not an R2 regression. The owner-runtime GUI smoke is blocked by the missing optional Qt package in this headless environment. The full repository suite was not rerun: S49 previously collected 2,783 tests but stopped at 1,196, so that partial run is not a pass.

## Measurements and environment

The controlled repeated workloads ran on Python 3.12.14, Linux 6.18.44, x86_64, with 3 logical CPUs exposed. The two 1,280-frame runs took 8.548 s and 8.604 s; peak sampled child RSS was 108,773,376 and 110,108,672 bytes; user/system CPU was 1.125/0.078 s and 1.188/0.088 s. Linux `ru_oublock` reported 8,192 output bytes per run; this is a block-I/O counter, not the total of filesystem writes. The mixed run took 14.038 s, peak child RSS 277,635,072 bytes, user/system CPU 7.585/0.337 s, and `ru_oublock` 5,066,752 bytes. Its observed SQLite WAL was 6,353,072 bytes after first publication and 6,373,672 bytes after mixed ingestion/stop. Collector archive flushes handled 64 rows each; the longest measured flush was 28.2 ms.

These are bounded local synthetic measurements. No venue/network byte counters apply because no network connection was made. Temporary retained test data and workspace/disk snapshots are listed in the evidence directory; prior rolling 24-hour disk-write usage is **NOT ESTIMABLE**, so no claim is made that the owner’s rolling write budget was satisfied. RSS is a measured peak, not evidence of a leak.

At the final disk snapshot, the dedicated `/tmp` roots retained 27,258,824 bytes for the capture matrix, 17,245,375 bytes for the mixed run, and 246,621 bytes for each repeated delivery run; the compressed R2 evidence tree retained 503,860 bytes. A 17,009,236-byte local mixed-workload artifact tree at `.r2-temporary-workload/` was retained for audit but excluded from the commits. These are end-state retained sizes, not total bytes written. Per-process `ru_oublock` counters are reported above separately.

## Static, security, and frozen-source checks

Ruff lint, `compileall`, `uv lock --check --python 3.12`, `uv pip check`, locked/offline `uv sync --dry-run`, and `git diff --check` passed. The new test file passes Ruff formatting. Five pre-existing modified Python files fail Ruff format in both the starting baseline and working tree; no whole-file formatting churn was introduced. Targeted mypy reports the same 33 pre-existing diagnostics on baseline and working tree, with zero added or resolved diagnostics. No dependency or lockfile changed. Pytest JUnit reports are stored as deterministic gzip files because traceback source context contains trailing spaces; every report decompresses byte-for-byte to the original XML, and both content and compressed hashes are recorded.

The three frozen authority hashes match before and after. No external secret scanner is installed; the recorded local scan checked added source, tests, handoff, and evidence for common AWS/GitHub credentials, bearer tokens, private-key PEM markers, and long credential assignments, with no matches. Secret scan and static details are in `transition-r2-evidence/static-final/`.

## Remaining gates and limits

- **TEST GATE:** full repository regression remains unrun/incomplete; the prior 2,783-test collection stopped at 1,196.
- **TEST GATE / BLOCKED BY ENVIRONMENT:** desktop smoke cannot import `PySide6`; Windows qualification remains environment-blocked.
- **TEST GATE:** the known Session-041 basket outcome consumer case fails identically on the R1 starting SHA and R2; it remains outside this delivery correction.
- **UNVERIFIED:** the unchanged S49 test's original queue rejection was not reproduced on this host, although its controlled pre-fix reproduction and current green integration are retained.
- **NOT ESTIMABLE:** sustained Binance delivery, native execution, capital eligibility, and prior rolling 24-hour write usage.
- S49's production agent remains disabled; three readiness-context cases remain open; AUD-01, AUD-02, and AUD-06 remain separately amendment-gated.

No authenticated exchange access, private endpoint, account/order action, paid provider, agent token use, trading enablement, merge, or deployment was performed. Do not interpret this correction as `ENGINEERING_PASS`, live readiness, or capital eligibility.

## Rollback

To revert the production/test correction while retaining the recorded evidence, run `git revert 910d4001323f4a6136af886df618b99a6c8fb8aa` on the R2 branch. The documentation/evidence receipt is a separate commit and can be retained for audit.

The machine-readable evidence and hashes are in [`ATLAS_RED_R2_EVIDENCE_MANIFEST.json`](ATLAS_RED_R2_EVIDENCE_MANIFEST.json).
