# ATLAS RED R6 handoff

**Status: source and test work is implemented and tested. External market, provider, and Demo execution qualification remains blocked or unverified. This is not a production-readiness claim.**

R6 was created in isolated branch `fix/red-r6-full-workflow-demo-qualification-2026-10-13` from the required R5 commit `9c92aad2331e138e165553497f511c562a6ebcf5`.

The implementation and test commit is `ee9038ade842b3a23bc77679dccbc6fb6e04fa51`; its pushed remote ref was independently checked against the exact SHA. Final handoff and evidence files follow in a documentation-only commit on the same branch.

The reports and evidence were pushed as `4fede7afbd141732a5bab661c68db3b0fee3b793`; `git ls-remote` matched that commit exactly. A final append-only delivery receipt follows.

## Work completed

- Reproduced the five inherited Session031 failures, corrected typed product-metadata publication and causal dependency handling, and verified the affected consumers.
- Fixed compact public-stream locator availability lookup used by basket chronology.
- Corrected stale readiness and source fixtures without weakening Binance command-readiness enforcement.
- Clarified the future-product metadata error for the broad export contract.
- Replaced Gemma's historical 15-second transport cap with a configurable 60-second default, bounded to 120 seconds and always capped by the remaining absolute deadline. Automatic retry remains disabled.
- Fixed the read-only Demo probe to use the configured proxy, keep the Demo host fixed, reject redirects, bound response size, and emit sanitized timestamped receipts. It supports the requested public instrument-info read only after clock and account checks succeed.

## Verification

- Focused R6 selection: **150 passed, 0 failed, 0 skipped**.
- Full repository: **2,914 collected; 2,906 passed, 0 failed, 8 skipped**. The exact JUnit and resource results are in `transition-r6-evidence/`.
- Ruff, compileall, frozen lock, and dependency consistency pass. Mypy is unchanged from R5: 53 existing errors in 17 files, normalized message delta zero.
- All three frozen authority hashes match. S47 remains **PROPOSED — NOT ACCEPTED**, with effective identity `NONE`.

## External qualification

- Production Binance REST and stream hosts are not in the enforced Cloud allowlist; **zero production market-data requests and zero actual market observations** occurred.
- Two read-only Demo server-time GETs returned HTTP 451. One attempt lacks timestamps because the earlier probe version did not record them; the second is timestamped. The account GET and public instrument-info GET were not attempted. Account and exposure state remain **UNKNOWN**. **ZERO ORDERS.**
- No R6 Gemini API calls were made. Free-tier/no-billing evidence was not available, so there is no R6 generated response or genuine agent research round.
- No genuine complete production Brief, action-specific economics, prospective economics sample, or complete scanner schedule denominator was qualified.

The immediate external gate is to make the approved Demo server-time endpoint reachable through the enforced network path, then complete the signed account and public instrument reads. The complete execution lifecycle and exact immutable trade plan still require engineering and independent approval before any order-changing operation. See the checklist, issue register, and separate reports listed below.

User selected GPT-6 Luna; exact runtime identity is not independently attested in this environment. No subagents were used. No capital, provider, execution, merge, or deployment authority was added.
