# ATLAS S45 — factual validation and handoff

**Session:** S45 targeted repairs and pre-implementation design closure
**Date:** 2026-10-12 (Asia/Colombo)
**Branch:** `fix/session-045-outcome-and-design-closure`
**Worktree:** `/home/kasun/Music/atlas-s45-worktree`
**Base:** `origin/qualification/tq01-linux-2026-10-11 @ 46eb72d2611475d1e38444db287557c8154c5216`
**S44 ancestor:** `origin/fix/session-044-practical-trading-delivery @ 8961b032b108376bfbcd15e2ed8dc8fcbd48a645`
**Repository:** `KasunKarandagolla/Atlas`; original dirty checkout preserved.

## Result

**IMPLEMENTED:** the due-work discovery projection now selects the immutable `content_hash` required by its parser and uses the parsed storage entry rather than reconstructing a row with `content_hash=artifact_ref`. Malformed/incomplete rows remain reachable for explicit quarantine; no unexpected `IndexError` is swallowed. Two dedicated regressions cover immutable identity, old/new origins, idempotent cursor replay, restart, malformed/incomplete metadata and quarantine.

**TESTED:** after the repair, the production outcome batch passed 61/61 and S33 maturity passed 15/15. The previously failing production batch was 43/59 with 16 failures; the same batch now passes 61/61. S33 was 11/15 with 4 failures at TQ-01 and now passes 15/15. A focused 5-test due-work group passed, including the original reproducer and migration/quarantine tests.

**TESTED:** durable-capture tests now wait for capture/terminal receipt state rather than treating a pending-frame count as a durable-completion barrier. The changed fault module and journal integration passed 25/25; no capture production code or pending count was cleared. S45’s combined event/calendar comparison passed 35/35 and the isolated saturation case passed 1/1; the TQ-01 combined discrepancy was not reproduced, and its original cause remains unknown. V1 golden and S44 compatibility each passed 1/1.

**TEST GATE:** the older fake Bybit source-to-supervisor fixture still produces `INCOMPLETE_SNAPSHOT` and a `ValueError` from `record_computation` (`derived computation has unavailable or noncausal dependency`). The exact offending dependency is unknown. The failing legacy Bybit fixture is not the owner’s initial Binance-only profile; no Bybit capability or failure evidence was removed.

**TEST GATE:** the 1,024-contract synthetic stream fixture overflowed its shared handoff under a 160 frames/s producer target while a writer/report stall was injected. Observed high-water and occupancy were 480/512 and one frame was rejected; source reserves 32 items for the other stream lane, so the active lane could not consume that reserved capacity. This is a bounded burst/blocked-writer reproduction, not sustained producer-versus-consumer throughput. S45’s scaled 80/120 frames/s copies did not reach valid final metrics (80: archive stall synchronization was not reached; 120: capture reached terminal failure). The attempt consumed 2:49 wall, 90.02 s user CPU, 6.57 s system CPU, peaked at 185,800 KiB RSS and wrote 830,720 512-byte accounting blocks (~406 MiB by `/usr/time` block units); host I/O PSI rose to 4.89% `some` / 0.74% `full` avg10 after the run. No further capacity loop was run.

**NOT ESTIMABLE:** source delivery on a real venue, sustained breadth, daily/eight-week archive growth, venue wire behavior, strategy/model economic value and profitability. Capital and assisted execution remain disabled.

## P0 defect and evidence

At the base source SHA-256 `f986b3920d11db783ad9196ede110b9c7ec29b63201c38235c6448dc6206b2b7`, `discover_due_work` selected `artifact_ref`, type, timestamps and metadata but omitted `content_hash`; `artifact_entry_from_storage_row` then read `row["content_hash"]` and raised `IndexError: No item with that key`. The focused pre-change test reproduced this exact failure (1/1), 26.73 s wall, 9.14 s user CPU, 1.02 s system CPU, 112,840 KiB max RSS.

The implementation adds `content_hash` to that projection, parses the full immutable row, resolves the lane from parsed metadata, and uses storage `content_hash` in the malformed-metadata fallback. Existing safety/quarantine, cursor and atomic-new-origin paths remain. There is no new catch for `IndexError`.

| Run | Command scope | Result | Timing / evidence |
|---|---|---:|---|
| Before repair | `tests/v2/test_session037_due_work.py::test_terminal_history_is_retired_and_new_opportunity_is_immediately_due` | **FAIL 1/1**, exact `IndexError` above | [`JUnit`](transition-s45-evidence/p0_due_work_before.junit.xml), [`time`](transition-s45-evidence/p0_due_work_before.time.txt) |
| Focused after repair | Original reproducer, two new regressions, schema migration and quarantine cases | **PASS 5/5** | 15.95 s wall, 6.24 s user CPU, 100,604 KiB max RSS. [`JUnit`](transition-s45-evidence/p0_due_work_focused.junit.xml) |
| Production path | `tests/v2/test_session033_outcome_resolution.py tests/v2/test_session033_outcome_runtime.py tests/v2/test_session037_action_outcome_producer.py tests/v2/test_session037_due_work.py` | TQ-01 **43/59 pass, 16 fail** → S45 **PASS 61/61** | 1:37.77 wall, 23.21 s user CPU, 3.50 s system CPU, 109,068 KiB max RSS. [`old JUnit`](transition-tq01-evidence/tq01_production_due_work_outcomes_junit.xml), [`S45 JUnit`](transition-s45-evidence/p0_production_batch.junit.xml) |
| S33 maturity | `tests/v2/test_session033_outcome_maturity.py` | TQ-01 **11/15 pass, 4 fail** → S45 **PASS 15/15** | 21.15 s wall, 5.46 s user CPU, 58,796 KiB max RSS. [`old JUnit`](transition-tq01-evidence/tq01_outcome_maturity_isolated_junit.xml), [`S45 JUnit`](transition-s45-evidence/s33_maturity.junit.xml) |

The TQ-01 1/1 synthetic lifecycle remains a test fixture, not the production due-work scheduler. The S45 production batch is the current scheduler evidence.

## Other bounded findings

| Area | S45 result and remaining evidence |
|---|---|
| Calendar/event saturation | TQ-01 combined run had 34/35 pass twice (`CALL_OUTCOME_UNKNOWN` observed where one fixture expected `BROKER_SATURATED`); isolated saturation passed there. S45 combined calendar/event tests passed 35/35 and isolated saturation passed 1/1. Timing/order versus shared-state root cause remains `UNVERIFIED`; do not weaken fail-closed UNKNOWN semantics. S45 timing: combined 13.26 s, 61,004 KiB max RSS. |
| Capture frame/clock faults | TQ-01 suite 23/25. The two expectation failures used pending-frame count while one accepted frame was in-progress or while a failure receipt had not yet become durable. An intermediate S45 test draft failed 2/2 when it read terminal state before the receipt append completed; a receipt barrier and exact pending-state assertions were added. Final combined capture journal/durable-capture tests passed 25/25, 1:36.81 wall, 34.54 s CPU, 124,228 KiB max RSS. No unresolved frame was dropped or cleared. |
| Legacy Bybit fake supervisor | Fake-only fixture diagnostic: two products, 22 observations, 14 reader calls; no supervisor receipt or decision calendar; source-history row healthy at cutoff, but collection ended `INCOMPLETE_SNAPSHOT` with `ValueError` and no event receipt. Stack reaches `chronology.record_computation` from `_compose_public_event_inputs`. Offending dependency remains unknown. Preserve fail-closed result and fixture. |
| S6 / market correlation | Source confirms S6 uses causal completed H1 log returns, exact common timestamps, 720-hour history, BTC sample beta and residual score; missing peer history, zero variance, zero residual volatility and latest-four gaps return named unavailable states. Built-in Python versus NumPy float64 720-observation reference parity passed: max absolute delta `2.7e-15`; synthetic BTC/ETH correlation `0.99764` made a two-factor design condition number ~25,930. This only demonstrates collinearity sensitivity, not a market condition. V1 joint BTC/ETH risk paths and scanner clusters remain distinct existing functions. No new covariance engine or shared factor interface is justified now. Existing V1 cluster input is an untimestamped sequence and zero variance maps to `0.0`; treat clusters as research redundancy only until callers prove exact timestamp alignment. |
| Linux capacity | TQ fixture used 1,024 synthetic contracts, nominal 160 frames/s, bounded writer/report and archive stalls; queue high-water=480, queue current=480, max=512, rejected=1, overflow latched. The selected lane preserves the other lane’s 32-item floor. There is no sustained workload, observed producer/consumer rate pair, p95/p99, daily bytes or storage admission. S45 80/120 scaled variants both failed before valid measurement; host I/O pressure elevated, so stopped. Existing 512-item / 16 MB bounds remain unchanged. |

## Resource context and measurement limits

The Linux host is x86_64, CPython 3.12.13, two logical CPUs, approximately 3.7 GiB RAM, 4 GiB swap, and ~88–90 GiB free disk during S45. Before the scaled comparison, available RAM was ~1.8 GiB, load `2.58 / 2.78 / 2.65`, I/O PSI `some avg10=2.33%`, `full avg10=0.77%`. After the failed comparison, available RAM was ~1.7 GiB, load `4.19 / 3.35 / 2.89`, I/O PSI `some avg10=4.89%`, `full avg10=0.74%`. These are host-wide observations; they do not identify ATLAS-exclusive CPU or storage service rate.

**Measured:** process time/RSS and output blocks for the bounded S45 fixture; queue nominal producer target and observed occupancy/rejection in prior TQ fixture; local numeric parity runtime.
**Inferred only:** the 160 frames/s producer would fill a 480-item active-lane allowance in about 3 seconds if it sustained its target without service. Actual frames/s and end-to-end drain rate were not recorded, so this is not a throughput estimate.
**Not measured:** ATLAS sustained market-hour CPU/RSS, per-hour raw/index/WAL bytes, network rate, queue distribution, production archive/index flush percentiles, crash-recovery time or Windows/Linux parity under the same workload.

Ranked optimization candidates, pending measured exclusive hotspots:

1. **Avoid duplicate parsing/serialization and repeated canonical hashing** if profiles show this work repeats. Expected benefit: lower CPU/allocation per frame. Risk: changing immutable bytes, hashes or causal references. Proof: same raw/evidence hashes and per-frame outputs at identical 1/10/24-symbol test workloads, with CPU and deadline metrics.
2. **Group writer/index work in existing bounded batches** if SQLite/WAL commit and index flush dominate. Expected benefit: fewer transaction/fsync costs. Risk: writer blockage, atomicity or late causal publication. Proof: crash-at-each-boundary replay, exact no-loss identity, unchanged 512/16 MB bounds, commit/archive p95 and backlog drain.
3. **Schedule independent archive/index preparation away from the capture/service critical interval** only if stage timing establishes contention. Expected benefit: reduce writer service gaps. Risk: stale output, reordered evidence or unbounded RAM. Proof: injected stalls and reconnects with exact order, available-at and receipt parity, bounded queue and full drain.

These are experiment candidates, not accepted optimizations. No feature, requested observation or safety obligation is reduced.

## Tests and commands

All commands ran in `/home/kasun/Music/atlas-s45-worktree`, using `/home/kasun/Music/atlas/.venv/bin/python`, with `PYTHONPATH="$PWD/src:$PWD/tests"` where the tests require it. JUnit and `time -v` files are retained under [`transition-s45-evidence/`](transition-s45-evidence/). Exact before-repair, production-batch and other source commands are in the appended S45 section of root [`translog.md`](../../translog.md).

| Gate | Result |
|---|---:|
| P0 focused due-work regression/migration/quarantine | 5/5 `TESTED` |
| P0 production outcome batch | 61/61 `TESTED` |
| S33 maturity | 15/15 `TESTED` |
| Event/calendar combined | 35/35 `TESTED` |
| Event saturation isolated | 1/1 `TESTED` |
| Capture fault targets after durable barriers | 2/2 `TESTED` |
| Capture journal + durable capture modules | 25/25 `TESTED` |
| V1 golden baseline | 1/1 `TESTED` |
| S44 M15/profile compatibility | 1/1 `TESTED` |
| S6 NumPy/built-in covariance reference parity | `TESTED`, 720 observations |
| Capacity fixture variants 80/120 frames/s | 0/2 complete measurements; failures retained as described |
| Bybit fake source-to-supervisor | `TEST GATE`; unresolved fixture dependency |

## Diffs, hashes, freezes and dependencies

- Source fix is limited to `src/atlas/v2/memory/repository.py`; tests add two due-work regressions and repair capture synchronization/receipt assertions in `tests/v2/test_session040_durable_capture.py`.
- P0 source SHA-256 at the tested TQ base: `f986b3920d11db783ad9196ede110b9c7ec29b63201c38235c6448dc6206b2b7`.
- P0 source SHA-256 after the repair: `047eeaec0ec5b0bfff06ee8c50ce446468cce6db6fba9ff73f9063bb77cad018`.
- Accepted freeze hashes remain V1 `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`, V2 `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`, Agent V1 `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.
- `requirements-lock.txt` and `pyproject.toml` were not changed. No dependency was installed in ATLAS. No API, market endpoint, account, key, order or paid call was used.
- Evidence manifest `docs/v2/transition-s45-evidence/evidence_manifest.json`: SHA-256 `8fefae92f4eb980ba279559bfbd89bc22f7c5f7f46497cce893e550f8fcd6816`, 44 retained files / 128,436 bytes. `verify_s45_evidence.py` checks artifact hashes, bytes, parsed JUnit totals, report-local links and credential-shaped patterns in changed/untracked files; its own hash is included, while the manifest intentionally does not self-hash.
- No decision identity, risk parameter, queue limit, deadline, schema, freeze, policy hash, feature set or trading/capital flag was changed.
- Existing root `log.md` is updated only as coordinator checkpoint; pre-existing log entries remain intact. `translog.md` is append-only and already contains the S45 opening record.

**Final static/diff checks:** Ruff lint, targeted `compileall`, `pip check`, the retained evidence audit and `git diff --cached --check` scoped to authored source, tests, reports and coordinator logs pass. Whole-file `ruff format --check` still reports formatting changes for the three touched legacy Python files; the same exact files from base `HEAD` also fail, so no broad unrelated reformat was applied. Targeted mypy still reports one unchanged repository return-type incompatibility at line 2124. A full staged `git diff --cached --check` reports trailing spaces in byte-preserved raw pytest XML/traceback and diagnostic patch artifacts; authored files are clean and captured evidence was left unchanged for hash fidelity.

## New and remaining gates

**Closed for this narrow repair:** due-work `content_hash` projection/parser defect; production outcome maturity path exercised by 61/61 current tests; S33 maturity current test path; capture test synchronization false expectations.

**Still open:** event/calendar root cause (not reproduced); Bybit fake chronology dependency; valid queue capacity comparison and sustained capture/SQLite/WAL/archive measurements; public Binance delivery and actual M15 deadline; exact installed Nautilus conditional-order wire; account/margin and native protection races; ML/agent runtime and after-cost contribution; 8-week/200-opportunity prospective evidence; Windows qualification; live/capital authorization.

**One next bounded action:** independently review the pushed S45 branch diff, decision register and factual report against the three accepted freeze hashes. Do not start implementation of the future agent authority or position-management changes until the reviewer accepts a separate versioned amendment. Any public venue diagnostic also needs a separate bounded authorization and existing A2/source preflight.

The final report/evidence commit and final branch-tip SHA are recorded in the root S45 handoff after push; this report does not claim its own commit hash.
