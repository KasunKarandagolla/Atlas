# ATLAS RED R8 tests and performance

## Test results

The final source tree was tested in four disjoint file partitions, with `TMPDIR=/workspace/work`. The external short temp root kept qualification fixtures outside the source tree and within Unix socket path limits, while avoiding the constrained `/tmp` reserve.

| Partition | Collected | Failures | Errors | Skips | Wall seconds | Peak child RSS KiB |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 733 | 0 | 0 | 3 | 54.961 | 766,880 |
| 2 | 740 | 0 | 0 | 4 | 289.825 | 544,764 |
| 3 | 741 | 0 | 0 | 0 | 158.072 | 299,104 |
| 4 | 706 | 0 | 0 | 1 | 191.502 | 1,024,304 |
| **Total** | **2,920** | **0** | **0** | **8** | **694.360** | **1,024,304 max** |

The suite executed 2,912 non-skipped cases. Batch 4 emitted four existing `record_property`/`junit_family=xunit2` warnings; they were not test errors. The full partition summary and sanitized console records are under `transition-r8-evidence/`.

Additional final-tree focus: scanner tests 46/46 and worker isolation tests 6/6. The duplicate-registration test was observed red before the idempotency repair and green afterward. The worker timeout regression failed on the exact R7 base and is green on R8; a new test confirms direct Bubblewrap calls keep the original session default.

## Static and repository checks

- **TESTED:** `.venv/bin/ruff check src scripts tests` — all checks passed.
- **TESTED:** `.venv/bin/python -m compileall -q src scripts tests`.
- **TESTED:** `uv lock --check` and `uv pip check --python .venv/bin/python` — 34 installed packages compatible; no new dependency or lock change.
- **TESTED:** three frozen authority SHA-256 values match the required values.
- **TESTED:** `git diff --check`.
- **TEST GATE:** source mypy reports 53 diagnostics across 17 files on both R7 and R8; normalized delta is zero new and zero fixed across the baseline. See `transition-r8-evidence/static-checks.json`.

## Performance and limits

The wrapper measured cumulative child-process user/system CPU by partition: 57.768/5.260, 273.142/10.693, 97.176/8.325, and 127.074/9.763 seconds. Summed across the sequential partitions, child CPU was 555.160 user and 34.041 system seconds over 694.360 seconds wall time. The maximum peak RSS of a child was 1,024,304 KiB; this is not whole-system aggregate memory.

The one R8 Binance Demo server-time request took about 1.393 seconds from recorded request timestamps and returned HTTP 451. Response/network byte counts were not retained. No genuine stream, production queue, rolling storage/write rate, or provider call ran, so those live capacity/latency metrics are **NOT ESTIMABLE**. Gemma calls: zero. The 80 ms optional projection guard and fail-safe fixture tests remain in place, but this run did not measure a genuine 80 ms production latency distribution. Synthetic fixture throughput is not live venue capacity, and no rolling 24/72-hour claim is made.
