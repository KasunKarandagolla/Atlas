# ATLAS RED R8 defects and repairs

## 1. A due scanner slot could disappear before any durable record

**Reproduced on R7:** a provider callback required a persisted `scanner_slot_registration`; the first callback ran before any registration existed. The sanitized baseline record is [r7-red-reproduction.txt](transition-r8-evidence/r7-red-reproduction.txt).

**Repair:** added `ScannerSlotLedger` on the existing `ResearchArtifactArchive` and integrated it into the Phase-5 `ScannerRunner`. A LIVE run now registers the stable slot identity, due/deadline, policy version/hash and eligible-scope contract, then writes `STARTED` before providers run. Source outages and scan failures preserve a typed progress reason. Retry reuses the immutable registration; changed policy/scope is rejected; LIVE mode without an archive fails closed.

**Additional defect found during final review:** retry registration at a later clock value initially created another content-addressed registration because `registered_at_ns` changed. A focused red test reproduced differing registration references. The ledger now validates and returns the original registration. Final scanner tests: 46 tests, zero failures/errors/skips.

**Limit:** this records a slot when the current due runner starts, before the scan. It does not schedule a prospective horizon or create missed-slot records while the process is offline. The whole expected calendar denominator remains **TEST GATE**.

## 2. Local worker timeout left an escaped child process

**Reproduced on R7:** `test_provider_retains_os_limits_and_timeout_cleanup` left the injected sleeper visible after `LocalProcessProvider` raised `WORKER_TIMEOUT`. The worker had created a second session inside the provider-owned process group, so the provider's group termination did not cover the sandbox payload. No process was manually killed; the test's unique fixture process expired on its own.

**Repair:** retained Bubblewrap's `--new-session` default for direct callers and probes, and added an explicit option for `LocalProcessProvider` to disable only the nested worker session. The provider's top-level `Popen(start_new_session=True)` remains the owning process group; the timeout signal now reaches the supervised payload, while Bubblewrap's `--die-with-parent` and PID namespace clean up the tested helper started in its own session. A new regression confirms direct callers retain the original default. Worker isolation tests: 6/6; the timeout test is green in the final full suite.

## 3. Gates left open

- No complete 21-stage production composition was exercised. Existing full-suite tests remain per-component and use controlled fixtures.
- The scanner denominator, genuine production economics/Brief, real Gemma output, production market feed and Binance Demo native-protection lifecycle remain unqualified.
- No frozen authority, dependency lock, user configuration or exchange execution policy was changed. No order-changing request was sent.

Red/green summaries are in `transition-r8-evidence/scanner-slot-ledger-red-green.json` and `transition-r8-evidence/worker-timeout-red-green.json`.
