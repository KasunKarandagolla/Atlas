# Session 026 — Agent Intelligence Offline Infrastructure

## Checkpoint

- Branch: `impl/session-026-agent-intelligence-offline-infrastructure`
- Original Session-026 starting checkpoint: `21055dcb5496665aaf165bde1e4f7e73db773192`
- Single-writer remediation starting checkpoint: `84e4cf31b65940a98650cf1848b5635f9d57ec63`
- Tested implementation commit (source and tests): `29271fcb65b2f1c0763aac50d8f67fa4049afef6`
- Architecture authority: `docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md`
- Required and verified SHA-256: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`

The authority file was copied verbatim before source edits. Its digest matches the supplied SHA-256. The accepted V1/V2 freezes and Session-023/024/025 handoffs were read before implementation.

## Single-writer remediation

The deterministic research controller (`atlas-ops/controller`) is the sole writer of the agent namespace in `ops.sqlite`. `OPS_SCHEMA_VERSION` remains 1; the additive agent namespace is version 2. The watch-transition `ops_outbox` and V1 live-control persistence were not changed.

The controller persists each immutable dispatch authorization in `agent_broker_dispatches` inside its transaction before creating the signed, short-lived broker capability. This binds the job, immutable request key/hash, persisted attempt ID and call index, lease epoch, nonce/authorization ID, evidence hash, model-profile hash, provider/model, deadline/expiry, spend reservation, and token limits. A failed durable write returns no capability and prevents the provider call. Each retry reserves a new `AgentAttemptV1` and its own authorization, within the three-call limit.

The inference broker has no `ops.sqlite` path, repository, or persistence dependency. It verifies the signed capability and exact request/evidence/attempt scope, enforces expiry and fixed provider/model/token limits, and uses only bounded process-local replay suppression. It has no lifecycle authority or persistent replay store. The worker retains no database, credential, venue, or live-control authority.

The new ownership tests prove authorization-before-capability ordering, fail-closed persistence, capability tamper/scope/expiry rejection, stale-lease and duplicate-authorization rejection, retry fencing, broker restart/duplicate-result authority behavior, and the single-writer component boundary. Gate fields `ops_sqlite_single_writer` and `inference_broker_has_no_ops_db_write_authority` are both `TESTED`.

## Preserved behavior and boundaries

The offline proposer still produces only quarantined structured `ResearchProposalV1` artifacts. It cannot accept or complete a Discovery experiment, view or spend the final holdout, execute generated code, change multiplicity budgets, or promote itself. Existing Discovery, forecast-worker, selector, risk, admission, TradePlan, approval, desktop, execution, recovery, and capital contracts were not modified. No live shadow, action-critic consumption, event extraction runtime, or admission mapping was added.

The operational provider remains behind `ResearchProposalProvider`; `ActionAssessmentProvider` and `EventExtractionProvider` are contracts only. The optional framework is `pydantic-ai-slim[openai]==2.51.0` with `openai==3.19.2` in the isolated, hash-locked `requirements-agent-lock.txt`. It is absent from the normal/live environment. Provider credentials stay in the fixed-function inference broker. No hosted tools, web, shell, Python execution, MCP, filesystem tools, or provider fallback are enabled.

## Validation

- Focused Session-026 agent and broker ownership suite: **43 passed**.
- Complete V2: **446 passed**.
- Complete V1: **485 passed, 3 skipped**.
- Discovery/Session-023 holdout regressions: **90 passed**.
- Persistence, Session-024 authority/recovery/fault regressions: **218 passed, 3 skipped**.
- Desktop/IPC, disabled-path, Session-025 release and V1 golden regressions: **18 passed**, including **1 V1 golden pass**.
- Ruff, mypy (194 source files), compileall, `git diff --check`, and core `pip check`: **PASS**.
- Core hash-locked offline dry run: **PASS**; `requirements-lock.txt` remains unchanged at SHA-256 `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`.
- Agent lock installed in an isolated Python 3.12 environment using `--require-hashes`; agent `pip check`: **PASS**. Agent lock SHA-256: `47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3`.
- `pip-audit` against both locks: **PASS**, no known vulnerabilities found.
- Live-runtime disabled-path smoke: **PASS** with `pydantic_ai` absent; runtime reported `RECOVERING`, `new_risk_allowed=False`, and `NO ORDERS SUBMITTED`.
- Tracked high-confidence credential scan: **PASS**, zero matches; no matched values emitted.
- Real OpenAI provider smoke: **BLOCKED BY ENVIRONMENT**; no provider key was available and no provider request was made.
- No CI run is claimed.

Discovery identity remains `846fd322e5d0a8d91a4ae659771f7e37f26bcff384508a5895f154a93b0cfac3`; the final-holdout population identity remains `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`. The holdout remains `UNASSIGNED / UNTOUCHED` and unviewed. Accepted selector/model/risk identities remain recorded in the gate and validation reports.

## Closeout status

- Offline agent infrastructure: **IMPLEMENTED / TESTED**.
- `ops.sqlite` agent namespace single writer: **TESTED** (`atlas-ops/controller` only).
- Inference broker ops database write authority: **TESTED absent**.
- Live-shadow agent: **NOT IMPLEMENTED; UNVERIFIED / TEST GATE**.
- Decision influence: **TEST GATE**.
- Economics: **NOT ESTIMABLE**.
- Capital and assisted mode: **disabled**.
- Bybit/Binance qualification: unchanged.

Session-025 does not ship a general continuous `atlas-ops` daemon. Any later hidden-live-shadow integration still needs a bounded continuous non-capital ops runtime/supervisor or an equivalent proven deployment orchestrator. This checkpoint does not start live-shadow work.

A prior diagnostic exposed host environment output, including third-party provider credential values, in an agent tool result. This process exception was disclosed to the user; no values are repeated here. The values were not written to repository files, SQLite artifacts, fixtures, or committed logs. The user was advised to rotate/revoke exposed credentials; rotation status is unknown. No real-provider test was performed for this remediation.

See [the exact gate](../v2/AGENT_INTELLIGENCE_EXTENSION_GATE.json) and [the detailed validation report](../v2/SESSION026_AGENT_VALIDATION.json). The coordinating ChatGPT must independently inspect the pushed Session-026 SHA before authorizing another checkpoint.
