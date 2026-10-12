# ATLAS S49 offline integration evidence

Generated: `2026-10-12T19:37:16.317859+00:00`
Overall: **FAIL**

Aggregated cases: **910 total, 905 passed, 5 failed/gated**.

| Suite | Status | Tests | Failures | Duration (s) | Peak RSS (KiB) |
|---|---:|---:|---:|---:|---:|
| production-agent-bridge-gate | TEST GATE | 1 | 1 | 635.991 | 204168 |
| candidate-risk-economics-calendar | PASS | 78 | 0 | 3036.567 | 386716 |
| sources-features-strategies-universe | FAIL | 238 | 1 | 2282.874 | 789296 |
| brief-agent-memory-events-outcomes | PASS | 243 | 0 | 404.716 | 131292 |
| lifecycle-protection-recovery-bybit-v1 | PASS | 160 | 0 | 134.865 | 246884 |
| ipc-desktop-brokers-workers-and-capture | PASS | 160 | 0 | 259.732 | 268724 |
| known-inherited-readiness-tests | TEST GATE | 29 | 3 | 18.995 | 152884 |
| known-inherited-s6-ops-contract-test | PASS | 1 | 0 | 15.886 | 56872 |
| desktop-product-offscreen-smoke | PASS | — | — | 3.536 | 72704 |

The source group’s mixed-load handoff assertion fails reproducibly (one rejected frame at queue high-water 480/512). The strict production-agent bridge and three inherited Binance readiness fixtures remain `TEST GATE`. A passing component suite is not production connectivity or external qualification. Exact invocation reports, JUnit, and logs are retained in the sibling run directories.
