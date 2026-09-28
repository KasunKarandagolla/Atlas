# Session 027 — V2 continuous non-capital ops supervisor

## Outcome

Implemented and tested a bounded foreground `atlas-ops` supervisor for the existing deterministic V2 research/shadow pipeline. Session 027 is sequential repository bookkeeping only; this work does not add a seventh V2 phase or reopen any frozen V1/V2 phase.

The supervisor owns one long-lived, local writable `OpsRepository`, recovers durable watches and subscriptions before collection, gates decisions on reconciled current source health, validates causal artifact availability, checkpoints ordered pipeline stages, and persists content-addressed event and cycle receipts. It passes the repository to its injected composition port; existing collectors and services reuse that repository. The CLI takes a deployment `module:factory` adapter so source and production pipeline composition stay injected and deterministic tests need neither sleeping nor network access.

The adapter composes the current multi-sleeve CandidateSet, selector, hard-risk sizing, exact action freeze, economic evaluation, M1 and analogue APIs, and decision calendar. It preserves S1–S3 exact-action roles, the S4–S7 context/research exclusions, and S8 basket-only semantics. CandidateSet selection happens before sizing and evaluation. M1 and analogue outputs are bound to the frozen action and carry zero selector, risk, and admission authority. Primary stage references preserve the adapter's declared order.

No strategy rules, V1/V2 identity, selector order, risk limits, economic contracts, capability qualification, agent provider history, capital controls, or protection/recovery semantics changed. No exchange credential, order, approval, live-control database, agent provider, broker, or worker was used.

## Repository identity

- Starting branch and SHA: `impl/session-026-agent-intelligence-offline-infrastructure` at `8f915f85798ff5777592e7f187641ad5684699ff`.
- Session-026 tested implementation: `29271fcb65b2f1c0763aac50d8f67fa4049afef6`.
- Accepted Session-025 release base: `impl/session-025-v2-release-engineering-shadow-closure` at `21055dcb5496665aaf165bde1e4f7e73db773192`.
- Frozen V1 baseline: `impl/session-009-v1-phase0-6-stabilization` at `146bae0a2f10e2f794cbed3441123071c55a2baf`.
- Implementation branch: `impl/session-027-v2-continuous-ops-supervisor`.
- Tested implementation commit: `8de91429786a29c463d97149a711296850dc513d`.
- Remote tip after the implementation push was independently verified as `8de91429786a29c463d97149a711296850dc513d`.
- A documentation-only closeout commit follows. Its final remote tip is reported separately after push because a commit cannot contain its own resulting SHA.
- No merge was performed.

## Changed files

- `pyproject.toml` — registers the `atlas-ops` command.
- `src/atlas/v2/runtime/__init__.py` — exports the V2 supervisor API.
- `src/atlas/v2/runtime/ops_supervisor.py` — recovery-first bounded cycle, source-health and chronology gates, ordered stage checkpoints, event idempotency, operational receipts, and foreground CLI/loop.
- `tests/v2/test_session027_ops_supervisor.py` — deterministic lifecycle, production seam composition, safety, sleeve contract, idempotency, and restart tests.
- `docs/v2/SESSION027_OPS_RUNTIME_VALIDATION.json` — machine-readable validation results.
- `docs/handoffs/session-027.md` — this handoff.

Pre-existing untracked freeze-document copies and `atlas-session005.zip` were left untouched and were not committed.

## Authority and identity preservation

The following accepted artifacts and dependency locks are unchanged from the verified start point:

| Artifact | SHA-256 |
| --- | --- |
| V1 golden baseline | `be2a54d2bf9a3a168fe850877d51ef241d8ae8cdad837b872270cc3a30166251` |
| V1 freeze document | `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c` |
| Amended V2 freeze document | `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78` |
| Agent-intelligence freeze | `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d` |
| Session-025 release manifest | `9f26f4f73c14f5e70cee3c47df28b8e68f2cc05988b8bce8a9c49ce580a90e69` |
| Phase-5 release gate | `fe47c1f3d284c622e6f63a2b0a1eb5ac6faa8babf27ccabd4cce8975c93aa32e` |
| Session-026 agent gate | `0288cba49682a2e31b0b0103ce898312e410f64509f4dd14b35f368f2cdd0d2f` |
| Session-026 validation | `f2b65610bd7a8edd5879e1c9f27f5ab61dbdaebf626ba9b1bf5b684c74e4d6fa` |
| `requirements-lock.txt` | `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b` |
| `requirements-agent-lock.txt` | `47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3` |

Both dependency locks are byte-for-byte unchanged. The existing holdout population identity is `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`, still `UNASSIGNED / UNTOUCHED` and not viewed.

## Validation

| Command | Result |
| --- | --- |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-v2-final.xml tests/v2` | 463 passed, 0 skipped, 0 failed; 500.29 seconds |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-v1.xml tests --ignore=tests/v2` | 485 passed, 3 skipped, 0 failed; 646.90 seconds |
| `.venv/bin/pytest -q -o addopts='' tests/v2/test_session027_ops_supervisor.py` | 17 passed |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-seams.xml tests/v2/test_session020_phase2_e2e.py tests/v2/test_session023_integration.py tests/v2/test_session025_release.py tests/v2/test_session026_agent_broker_ownership.py tests/v2/test_session026_agent_intelligence.py tests/v2/test_worker_isolation.py` | 63 passed |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-hashes.xml tests/v2/test_contracts.py tests/contract/test_contracts.py tests/contract/test_v1_golden_baseline.py` | 10 passed; includes V1 golden recomputation |
| `.venv/bin/ruff check src/atlas/v2/runtime tests/v2/test_session027_ops_supervisor.py` | Passed |
| `.venv/bin/mypy src` | Passed; 194 source files |
| `.venv/bin/python -m compileall -q src` | Passed |
| `.venv/bin/pip check` | No broken requirements |
| `.venv/bin/atlas-ops --help` | Passed after editable install with `--no-deps` |

The three V1 skips were the Bybit and Binance authenticated testnet qualification opt-ins and the opt-in public testnet connectivity check. No authenticated exchange test or public network smoke ran. The genuine uninterrupted 72-hour public soak was not run and remains `BLOCKED BY ENVIRONMENT`.

The broad V2 run includes Session-020/023 integration coverage, Session-025 release regressions, Session-026 agent ownership/isolation regressions, and the updated Session-027 tests. V1 golden recomputation, V2 serialization/hash invariants, and both lock hashes were verified. `gitleaks`, `trufflehog`, and `detect-secrets` were unavailable; a value-suppressing tracked/untracked text-path and changed-file fallback scan is recorded in the validation artifact.

No GitHub Actions run was used as evidence. GitHub CI status is `UNVERIFIED`.

## Ownership, recovery, and terminal status

- `atlas-ops` is the only writable ops repository owner in this runtime. Collector and pipeline adapters receive and reuse the supplied repository; projection access remains read-only.
- Restart tests restored a durable active watch and subscription plan. Source health stayed `INCOMPLETE_SNAPSHOT` until the collector's reconnect reconciliation completed.
- Replaying a completed immutable event after process restart returned the same receipt and did not rerun the production composition. The CandidateSet, frozen action, evaluation, and decision-calendar references did not duplicate.
- Injected failures after UNIVERSE, CANDIDATE_SET, HARD_RISK, and ECONOMIC_EVALUATION checkpoints resumed after process restart without reordering or backdating stages.
- Missing or future causal/risk evidence terminated as `NOT_ESTIMABLE`; no action or evaluation followed failed hard-risk sizing.
- S4–S7 remained excluded from exact-action selection and S8 remained basket-only.
- The deterministic economic fixture used explicitly unqualified synthetic capability evidence and returned `NOT_ESTIMABLE`; it is not venue qualification or an economics claim.

## Acceptance state and remaining gates

- Continuous non-capital ops supervisor: `IMPLEMENTED` / `TESTED`.
- Agent offline infrastructure: `IMPLEMENTED` / `TESTED`, unchanged.
- Agent live shadow: `NOT IMPLEMENTED` / `TEST GATE`.
- Agent decision influence: `TEST GATE`.
- Economics: `NOT ESTIMABLE`.
- Genuine 72-hour public soak: `BLOCKED BY ENVIRONMENT`.
- Venue/account qualification: `UNVERIFIED` / `TEST GATE`.
- Capital: disabled. Assisted execution: disabled.
- DeepSeek/NIM amendment and live agent shadow remain out of scope.

The next action is independent reviewer inspection of the pushed Session-027 branch. No merge, provider change, agent shadow, or capital enablement is requested here.

## Bounded production-composition remediation closeout (2026-09-28)

This section records the requested remediation of the reviewer blocker. The original Session-027 history and validation above remain intact.

- Prior Session-027 branch tip before remediation: `24e965ac64f3337bdb484df3aed4e0741ebd2bc3`.
- Remediation implementation commit: `ae6722a30745992b8d930b9d9e11750832145297`.
- Tested SHA: `ae6722a30745992b8d930b9d9e11750832145297`; the complete code/test tree at this SHA passed the recorded Session-027, regression, V2 and V1 runs.
- Final documentation-only tip SHA is reported separately in the final pushed-branch closeout because a commit cannot contain its own resulting SHA.
- Branch: `impl/session-027-v2-continuous-ops-supervisor`. No merge was performed.

### Remediation changes

- `src/atlas/v2/runtime/production.py` — adds `ProductionOpsCyclePortV1` (`ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1`) and the `create_production_port` factory. It reuses the supervisor-owned `OpsRepository`, restores `PublicCollectorV2` cursors/watches/subscriptions and source-health state, waits for reconnect reconciliation before processing durable event handoffs, and calls the existing CandidateSet/acceptance, hard-risk sizing, `freeze_action`, economic evaluation, M1/analogue and decision-calendar APIs.
- `src/atlas/v2/runtime/ops_supervisor.py` — makes that credential-free built-in factory the normal `atlas-ops` default. `--adapter` remains an explicit override.
- `src/atlas/v2/runtime/__init__.py` — exports the production adapter and factory.
- `src/atlas/v2/science/outcomes.py` — adds typed validation for the operational `OPS_RUNTIME_GATE` decision-calendar evidence used when mandatory risk/economic inputs are missing.
- `tests/v2/test_session027_ops_supervisor.py` — removes the test-only `ExistingV2CompositionPort` implementation.
- `tests/v2/test_session027_production.py` — exercises the production adapter, existing API calls, chronology, missing/future evidence gates, action-bound ZERO-authority diagnostics, sleeve boundaries, default CLI, restart/idempotency and injected crash recovery.
- `docs/v2/SESSION027_OPS_RUNTIME_VALIDATION.json` — appends the machine-readable bounded-remediation results.
- `docs/handoffs/session-027.md` — this appended remediation record.

The built-in `IndexedPublicCycleSourceV1` consumes durable `OpsDecisionEventSourceV1` public-source handoffs and `IndexedProductionEventInputsV1` consumes only event-referenced, cutoff-available universe, candidate, scanner and feature artifacts. The adapter makes no network/provider request and does not synthesize account, hard-risk, capability, qualification, venue or economic evidence. Missing mandatory evidence is persisted as a `NOT_ESTIMABLE` runtime decision/calendar state; a normal public-only run may correctly stop there. Tests inject deterministic typed event inputs into the production adapter, rather than substituting another pipeline implementation.

### Remediation validation

| Command | Result |
| --- | --- |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-remediation-focused.xml tests/v2/test_session027_ops_supervisor.py tests/v2/test_session027_production.py` | 28 passed, 0 skipped, 0 failed |
| `.venv/bin/pytest -q tests/v2/test_session020_*.py tests/v2/test_session023_*.py tests/v2/test_session025_release.py tests/v2/test_session026_*.py` | 163 passed, 0 skipped, 0 failed |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-remediation-v2.xml tests/v2` | 474 passed, 0 skipped, 0 failed; 1,150.38 seconds |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-remediation-v1.xml tests --ignore=tests/v2` | 485 passed, 3 skipped, 0 failed; 476.06 seconds |
| `.venv/bin/pytest -q -o addopts='' --junitxml=/tmp/session027-remediation-golden.xml tests/contract/test_v1_golden_baseline.py` | 1 passed; V1 golden recomputation |
| `.venv/bin/ruff check .` | Passed |
| `.venv/bin/mypy src` | Passed; 195 source files |
| `.venv/bin/python -m compileall -q src tests` | Passed |
| `.venv/bin/python -m pip check` | Passed; no broken requirements |

The three V1 skips were the Bybit and Binance authenticated testnet qualification opt-ins and the opt-in public testnet connectivity check. The V1 and V2 freeze, release, Session-026 and dependency-lock hashes listed above were recomputed and matched. The local value-suppressing secret scan examined 465 text files and found zero high-confidence patterns; `gitleaks`, `trufflehog` and `detect-secrets` are not installed.

### Remediation runtime and authority status

- Single-writer ownership: `OpsSupervisorV2` opens and owns the sole writable ops repository; `PublicCollectorV2`, source adapters and pipeline composition reuse that repository instance.
- Recovery: the production adapter restored the collector cursor, active watch and subscription plan. Restarted `HEALTHY_CURRENT` state returned to `INCOMPLETE_SNAPSHOT` and remained there until explicit overlap/missed-interval reconciliation.
- Idempotency: restarting the production adapter returned the same immutable receipt and did not duplicate CandidateSet, sizing, action, evaluation or decision-calendar artifacts.
- Crash recovery: injected crashes after `UNIVERSE`, `CANDIDATE_SET`, `HARD_RISK` and `ECONOMIC_EVALUATION` resumed in fixed stage order without backdating.
- M1 and analogue diagnostics remained bound to the frozen action and retained `ZERO` selector/risk/admission authority.
- S1–S3 remain the only exact-action candidate sleeves; S4–S7 remain context/research and S8 remains research-basket-only.
- Economics: `NOT ESTIMABLE`. Agent mode: `DISABLED`. Capital: disabled. Assisted execution: disabled.
- Final holdout: `UNASSIGNED / UNTOUCHED`; not inspected. V1 live-control database: untouched. No order, approval, protection, recovery-capital or authenticated-venue boundary was reached.
- Network activity was limited to the required Git fetch/push. No public-market, venue, LLM or external-provider request ran. No GitHub Actions run exists or is claimed. The 72-hour public soak was not run and is not claimed as passed.

The handoff is now returned to the coordinating reviewer for independent inspection of the pushed Session-027 branch.

## Second bounded production-composition remediation closeout (2026-09-29)

This is a second bounded remediation within Session 027. It does not create Session 028. The earlier Session-027 records above are retained as history. This remediation restores the accepted decision-calendar wire contract and closes the built-in production-composition blockers.

- Required starting remote tip: 4ff75d3f164c8e1d1181bf8d92e67d5dca39b0ec; verified before editing.
- Prior remediation tested implementation: ae6722a30745992b8d930b9d9e11750832145297.
- Implementation and tested commit: 39afc3a13b1459063766a94b7ba0e97953270a47.
- The final documentation-only remote tip is reported in the final closeout; a commit cannot contain its own resulting SHA.
- Branch: impl/session-027-v2-continuous-ops-supervisor. No merge was performed.

### Changed files

- src/atlas/v2/data/collector.py — indexes durable typed source-health and exact public observation reconciliation evidence.
- src/atlas/v2/data/history.py — reconstructs archived public observations with content/hash/index checks and cutoff-safe availability.
- src/atlas/v2/runtime/production.py — composes durable event handoff, as-of universe/history, existing causal features, S1/S2/S3 coordinators, CandidateSet/selector, indexed hard-risk and economics inputs, action, evaluation, diagnostics and calendar through the built-in adapter.
- src/atlas/v2/science/outcomes.py — removes the unversioned OPS_RUNTIME_GATE enum branch and validation; DecisionCalendarEntryV2 retains its accepted wire identity.
- tests/v2/test_session027_production.py — adds default-path integration and indexed evidence, ambiguity/future, fail-closed, calendar round-trip, restart/idempotency and no-network/capital-boundary regressions.
- docs/v2/SESSION027_OPS_RUNTIME_VALIDATION.json — adds this second remediation record without replacing earlier history.
- docs/handoffs/session-027.md — this appended closeout.

### Built-in composition result

The default factory identity is ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1 at atlas.v2.runtime.production:create_production_port, using IndexedPublicCycleSourceV1 and IndexedProductionEventInputsV1. The decisive test calls create_production_port() without a custom public source, StaticInputsProvider, custom inputs provider or external adapter.

From persisted causal public evidence the default composition reconciles source health, resolves or produces a content-addressed decision-event handoff, builds event-cutoff universe/as-of joins, calls feature_snapshot, invokes the existing S1ShadowCoordinator, S2ShadowCoordinator and S3ShadowCoordinator, persists every actual exact candidate before selection, and uses the existing CandidateSet and deterministic selector. The fixture selects an S2 candidate created by the coordinator; it does not pre-create CandidateActionV2.

The default path resolves exact typed risk evidence, reaches existing hard-risk sizing, and freezes the exact action only after SIZED. Quantity comes from hard risk. It resolves the existing economic input types and calls run_phase2_economic_evaluation; the result is NOT ESTIMABLE. M1 and analogue diagnostics bind the same frozen action hash and retain ZERO authority. No TradePlan, order, approval, capital or assisted-execution path is created.

### Decision calendar, ownership and recovery

The OPS_RUNTIME_GATE mutation has been removed. Regression tests prove accepted DecisionCalendarEntryV2 values and serialized round trips remain unchanged. Runtime-only blockers stay in the operational receipt; accepted calendar semantics are used only for established states.

Restarting the supervisor and creating a fresh default adapter against the same database did not duplicate event identity, CandidateSet, sizing decision, frozen action, evaluation or calendar entry. A selection-stage interruption recovered through the built-in path. OpsSupervisorV2 owns one writable OpsRepository shared by collection and production composition; no second writer or agent DB writer starts. No agent worker, broker, provider or dispatch is imported or started.

### Tests and checks

| Command | Result |
| --- | --- |
| .venv/bin/python -m pytest -q -o addopts='' tests/v2/test_session027_ops_supervisor.py tests/v2/test_session027_production.py | 31 passed, 0 skipped, 0 failed |
| .venv/bin/python -m pytest -q -o addopts='' --junitxml=/tmp/session027-second-remediation-seams.xml tests/v2/test_session020_*.py tests/v2/test_session023_*.py tests/v2/test_session025_release.py tests/v2/test_session026_*.py tests/v2/test_worker_isolation.py | 168 passed, 0 skipped, 0 failed |
| .venv/bin/python -m pytest -q -o addopts='' --junitxml=/tmp/session027-second-remediation-v2.xml tests/v2 | 477 passed, 0 skipped, 0 failed; 1,032.66 seconds |
| .venv/bin/python -m pytest -q -o addopts='' --junitxml=/tmp/session027-second-remediation-v1.xml tests --ignore=tests/v2 | 485 passed, 3 skipped, 0 failed; 510.464 seconds |
| .venv/bin/python -m pytest -q -o addopts='' --junitxml=/tmp/session027-second-remediation-contracts.xml tests/v2/test_contracts.py tests/contract/test_contracts.py tests/contract/test_v1_golden_baseline.py | 10 passed, 0 skipped, 0 failed; includes V1 golden recomputation |
| .venv/bin/ruff check . | Passed |
| .venv/bin/mypy src | Passed; 195 source files |
| .venv/bin/python -m compileall -q src tests | Passed |
| .venv/bin/python -m pip check | Passed; no broken requirements |
| .venv/bin/atlas-ops --help | Passed |
| .venv/bin/atlas-ops --db <temporary ops.sqlite> --once | Passed without --adapter |

The three non-V2 skips were the Bybit and Binance authenticated testnet qualification opt-ins and the public testnet connectivity opt-in. The two accepted dependency locks and the V1/V2 freeze, golden, release and gate hashes were recomputed and unchanged. A value-suppressing fallback signature scan checked 400 tracked/untracked text paths, skipped one binary file, and found zero high-confidence key/token/private-key patterns. gitleaks, trufflehog and detect-secrets were unavailable.

No public-market or venue request, authenticated call, LLM/provider request, or GitHub Actions run occurred. Network activity was limited to Git fetch/push. No 72-hour soak was run. Economics is NOT ESTIMABLE. Final holdout remains UNASSIGNED / UNTOUCHED. Agent mode is DISABLED; capital and assisted execution remain disabled. No profitability claim is made.
