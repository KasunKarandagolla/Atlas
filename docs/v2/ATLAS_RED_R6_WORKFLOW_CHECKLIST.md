# ATLAS RED R6 workflow checklist

| Stage | Status | Evidence | Remaining gate |
|---|---|---|---|
| R5 ancestry, worktree isolation, owner sources, and frozen authorities | TESTED | Required R5 base and all three frozen SHA-256 values match | Remote push verification is recorded in the delivery receipt |
| Five inherited Session031 regressions | TESTED | Red reproduction 5/36 failed before correction; final selection passes | None for these five tested regressions |
| Other 9 inherited full-suite failures | TESTED | All original cases mapped and resolved as code defects, stale fixtures/contracts, or the already landed deterministic-clock correction; final full suite has zero failures | The full suite still has eight explicit skips |
| Full repository suite | TESTED | 2,914 collected; 2,906 passed; 0 failed; 8 skipped | Skipped opt-in, native-Windows, and optional-dependency checks remain separate gates |
| Production Binance market observations | BLOCKED BY ENVIRONMENT | Current enforced allowlist excludes production REST/stream hosts; no request was made | Obtain an approved policy path to documented public production endpoints; do not substitute Demo data |
| Broad eligible altcoin source coverage and replay | TEST GATE | Repository synthetic coverage passes | Genuine broad population, finalized bars, timestamps, revisions, gaps, reconnects, archive and replay are not observed |
| Scanner expected-slot registration and complete calendar | TEST GATE | Existing decision calendar tests pass | Register future expected slots before execution and reconcile missed, rejected, empty, expired, and failed slots under policy identity |
| Independent quantitative assessment | NOT ESTIMABLE | Synthetic contracts and tests pass | Genuine causally available market, spread, liquidity, fee, funding, and action-specific evidence are absent |
| Production decision Brief | BLOCKED | Existing projections preserve missingness and authority boundaries | No genuine COMPLETE Brief can be constructed without source and economics evidence |
| Gemma transport timeout | IMPLEMENTED / TESTED | Configurable deadline-bounded timeout and offline provider tests pass | No real provider response was requested in R6 |
| Gemma no-billing/provider qualification | UNVERIFIED | Gemini host and key readiness are reported; no provider call was made | Verify no-billing/free-tier evidence and authorize the bounded synthetic request |
| Genuine agent research round and tool capability | UNVERIFIED | Offline S47 fake-provider tests are included in the full suite | A genuine COMPLETE Brief and verified provider capability are prerequisites; tool calling remains unverified |
| Durable memory and due-work invalidation | TEST GATE | Existing synthetic memory, restart, and due-work tests pass | Full immutable invalidation and schedule recovery across source revisions, policy changes, expiry, and restart remain unqualified |
| Binance Demo offline signing and contract tests | TESTED | Binance Demo guard suite passes; public calls carry no API key | No order lifecycle or remote protection was qualified |
| Binance Demo server-time read | BLOCKED BY ENVIRONMENT | Two GETs returned HTTP 451; proxy preserved; second attempt timestamped | Resolve the 451 through the approved network path |
| Signed account and public instrument-information reads | NOT ATTEMPTED | Clock validation gate stopped the probe | Perform only after server-time succeeds; account state remains UNKNOWN |
| Full Demo execution lifecycle, protection, restart, and reconciliation | TEST GATE | Typed offline request, response, fill, and recovery fixtures pass | Durable Demo wire/lifecycle behavior, protection confirmation, and UNKNOWN-state reconciliation are incomplete or unverified |
| One actual Demo trade | ZERO ORDERS | No trade plan, account state, or order-changing approval was used | Exact immutable plan, independent risk/protection review, and separate approval are required after all prior gates |
| Position supervision, event processing, kill and recovery | TESTED WITH FIXTURES | Session031 and repository lifecycle/recovery suites pass | No actual Demo position exists; real protection and terminal flatness are unverified |
| Performance and storage | TESTED WITH SYNTHETIC WORKLOADS | Full suite resource profile, mixed 1,024-contract probe, and compressed metadata measurements | Live feed latency, 72-hour continuity, rolling storage, provider cost, and Windows resource use are not estimable |
| Scientific/economic qualification | NOT ESTIMABLE | No prospective real opportunities were collected | Frozen eight-week and 200-matured-opportunity evidence floor remains unmet |
| Commit, push, and remote SHA | TESTED | Branch and exact remote SHA are listed in the manifest and delivery receipt | No merge, deployment, or self-acceptance |

The completed regression suite does not close external data, economic, provider, or exchange gates. No new skip or xfail was added.
