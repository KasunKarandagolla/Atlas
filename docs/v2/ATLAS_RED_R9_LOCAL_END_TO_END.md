# ATLAS RED R9 local end-to-end evidence

## Track A — genuine external evidence

**TESTED through source adapters; TEST GATE for durable downstream composition.** The existing Binance broad public REST path observed 2,366 records across BTC, ETH, and a broad altcoin universe; the existing WebSocket capture path received and drained 265 frames over 12 seconds. No credentials were used for production market data. The REST cycle took 5.186 seconds against its 5-second acquisition budget; bar/history enrichment was deferred.

The public probes kept data in process memory and saved no raw market payloads. The Atlas production runtime then rejected startup at the owner storage preflight, before it constructed the production source or opened the research writer. Consequently no genuine capture→bars→features→scanner→action→economics→Brief chain was produced. The current production Brief is not established, and economics remain **NOT ESTIMABLE**.

The one live Gemma request used a synthetic prompt and returned a schema-valid `ABSTAIN`. It was an API smoke, not a production research job. No market data or exact action was sent to Gemma.

## Track B — connected fixture coverage

**TESTED connected synthetic fail-closed path; successful 21-stage composition remains TEST GATE.** A new test runs the existing `ProductionOpsCyclePortV1` and `OpsSupervisorV2` with `ReconciledFixturePublicSource` through candidate/action/evaluation persistence, production Brief and memory projection, the Gemma job gate, Demo availability, and restart replay. The generated Brief is `BLOCKED` because production typed sections are not reconstructed; economics stay `NOT_ESTIMABLE`, `ResearchMemoryV1` creates no Gemma job, and no provider intent or Demo order intent is written. This is an expected safe terminal path, tagged `SYNTHETIC_INTEGRATION`.

In the same focused run, the existing R4 synthetic complete-Brief fixture passes through fake Gemma response validation, durable shadow memory, and Demo request generation without dispatch. This fixture is separate from the production-source fixture because the production Brief gate correctly refuses to create a job. The combined focused command passed 2/2; it does not establish a single successful market-to-Gemma-to-Demo lifecycle.

The R9 prospective scanner test joins scheduling, durable ledger recovery, missed-slot reconciliation, and denominator reporting through the production `ScannerRunner`/`ResearchArtifactArchive`. It uses deterministic scanner fixtures and is not genuine market evidence. Empty/unregistered historical intervals are not added to the durable denominator.

Failure and missingness branches are preserved by existing production and fixture contracts: no candidate, hard-risk rejection, unavailable economics, stale/future evidence, invalid or abstaining provider output, timeout, unavailable source, unknown order state, missing protection, and restart. No fixture output is classified as genuine production evidence.

## Terminal status

No order was submitted. The engine was not promoted. The detailed storage gate and source measurements are in [API qualification](ATLAS_RED_R9_LOCAL_API_QUALIFICATION.md); exact test totals and resource measurements are in [tests and performance](ATLAS_RED_R9_LOCAL_TESTS_PERFORMANCE.md). The connected fixture result is machine-recorded in [`connected-fixture.json`](transition-r9-local-evidence/connected-fixture.json).
