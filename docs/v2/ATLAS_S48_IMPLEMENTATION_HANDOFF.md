# Atlas RED S48 implementation handoff

Status: **IMPLEMENTED / TESTED** for the bounded offline changes below. Broader test and native qualification gates remain **TEST GATE**. Independent acceptance: **NONE**. No merge, deployment, capital enablement, paid provider call, real/demo order or S49 launch.

Accepted base and fresh remote tip: `b0db7d0527e68c737eefd564a87f740f5929c5f6`, branch `impl/session-047-economics-agent-decision-brief`; independently verified ancestor source `e1f0c17b68f9add063e9d6a78b5c8ddbfee3dede`. Delivery branch: `impl/session-048-position-lifecycle-protection`, isolated `/home/kasun/Music/atlas-s48`. Pushed implementation SHA: `9a48489b2f6939f1586ee6e11923ee3426937342`; exact remote match verified with `git ls-remote origin refs/heads/impl/session-048-position-lifecycle-protection`. The [push receipt](transition-s48-evidence/implementation_push.json) records the command/identity. The final documentation commit cannot contain its own SHA; its exact remote HEAD is independently checked with `git ls-remote` and returned to the owner. No newer unaccepted branch was used.

Actual local turn metadata verifies `gpt-6.1-sol`; no subagents. Original dirty checkout, earlier worktrees and owner `Red` sources were preserved. `AGENTS.md`, all three frozen authorities in full, required owner design/ADR/runbook sources and current repository/S47 evidence were reloaded. Exact source identities: [preflight](transition-s48-evidence/preflight.json), [authority isolation](transition-s48-evidence/authority_isolation.json). Frozen SHA-256 values remain:

- V1: `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`.
- V2 amended: `e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78`.
- Agent: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.

Serena 1.7.0 is activated on S48 and used for bounded symbol exploration. Codex CLI 0.161.0 and installed Context7 plugin 1.0.1 were verified. Context7 callable MCP tools are unavailable in this session: **BLOCKED BY ENVIRONMENT**. Its skill was read; exact official documentation and installed artifacts supplied the necessary audit. No installation or Repomix. Development tools/credentials are absent from runtime agent contracts.

## S47 issue dispositions

**TESTED — Decision Brief provenance.** Red reproduction let fabricated RSI99 cite an unrelated timely indexed event and enter COMPLETE. Research brief identity advances to `ATLAS_DECISION_BRIEF_RESEARCH_V2`. `ATLAS_BRIEF_FACT_PROVENANCE_V1` identifies source type, registered transformation, feature IDs/version, source cutoff and exact source/execution keys. The closed `FeatureArtifactV2` / `FEATURE_VALUES_V1` reader reconstructs the independently archived typed source, checks its existing seal, indexed type/hash/creation/availability, ACTUAL_SYSTEM chronology, causal input refs, exact product and values/missingness. Registered projection expiry is at most one hour after source availability. Unsupported source types and legacy unbound generic/technical facts cannot enter COMPLETE. Changing supplied values or hashing them does not verify a source. The artifact store/producer remains the trusted archive boundary; this does not qualify raw market data or authenticate an exchange wire.

Modified archived values with an old seal, wrong type/transformation/feature/product/version/cutoff/availability, future or reconstructed data, stale foreign data, contradictory same-cutoff claims and restart are covered. Foreign facts carry EXTERNAL_CONTEXT, never local executable identity. No selector, critic, M0 economics or agent trading permission changed.

**TESTED — schema assertions.** Reproduced both original S26 `3 == 1` failures against accepted code. Changed only the two assertions to Ops schema v3; retained agent schema v1→v2 migration, columns and unchanged Ops schema checks. No production schema or migration changed.

**TESTED — saturation transport.** Deterministic slot exhaustion proved the listener replied/closed before reading a complete bounded request. It now drains one bounded frame under the existing 100-ms timeout before sending saturation rejection. Queue/handler capacity is unchanged. A received rejection is BROKER_SATURATED; uncertain send/read remains CALL_OUTCOME_UNKNOWN. The original unmodified calendar/event tests pass in the repaired consumer and broader calendar runs. The unread-close/reset explanation accounts for the reproduced mechanism; no universal transport reliability or measured latency claim is made.

Focused S47 consumers passed **154/154 before lifecycle implementation**. The first 60-test green checkpoint and all red/intermediate failures remain retained. Two extended fixture positives initially failed because archived FrozenMap arrays needed conversion back to JSON wire; corrected source readback passed. See the manifest for exact commands, XML/raw hashes and chronological source snapshots.

## Lifecycle implementation and reuse

Nautilus remains the single normal transport/OMS owner. Existing SQLite journal, action/approval/reservation identities, exact command persistence, UNKNOWN send fencing, fill deduplication, recovery, flat certificates, risk reduction and S45/S47 work/memory remain in place. Bybit V1 attached IOC full native MarkPrice protection, fixed time exit, no TP/adds/pyramiding and strict UNKNOWN recovery are unchanged.

`ProtectionDeadlineMachine` reuses the mature verifier rather than duplicating semantics. Confirmed observations expire; contradictions including quantity, epoch, trigger basis, missing/placeholder refs and future observations require recovery. Additional fills invalidate the old quantity obligation. Local timer checks do not invoke an LLM or remove a stop. Existing durable recovery-action generation/qualified dispatch remains responsible for effects. This is tested local supervision logic, not sustained live supervisory qualification.

Binance conditional readback now requires CONDITIONAL, exact algo fields, positive numeric algo identity, STOP_MARKET, MARK_PRICE, closing direction/full-position semantics and consistent response quantity/reduceOnly. Normal-order fallback names cannot certify native protection. Recovery optionally binds the existing ProductContractV2 key/ref into durable query facts, checks repair writer/position epochs and product identity, and retains the original API/recovery gates.

`PositionLifecycleProjectionV1` reads bounded existing commands/recovery queries (32 commands, 16 query records), actual signed exposure, product/venue/account/epoch bindings, source receipts, current-state freshness and qualified conditional readbacks. Unresolved opening identity, incomplete/contradictory data, external/orphan orders or wrong writer remains UNKNOWN. Flatness without outstanding orders is FLAT_PENDING_ECONOMICS. Every projection permanently forbids opening admission, reservation release and maturity. Legacy unbound captures remain UNKNOWN. The selected demo runtime exposes a writer-fenced read-only consumer; no second journal, queue, OMS or automatic management dispatcher was created.

Quote/funding checks are **scope diagnostics**, not source verification or executable economics. Actual local bid/ask/depth, fill probability, fee tier, funding calendar, mark/index definitions, contract limits/revision and account/margin state still require their existing venue-specific qualification before any executable action. Research on both venues is allowed; only one credential writer is allowed. InstrumentKeyV2/ProductContractV2 identities are reused; no new venue enum, routing optimizer or cross-venue capital expansion.

The single `ATLAS_PARTIAL_TP_2R_HALF_SHADOW_V1` comparator has no transport or journal mutation and is disconnected from the runtime agent. It blocks unknown/stale protection, local-context reasons, stop-first/flat races, unsupported or expired review and optional model outage. Ambiguous stop/TP same-bar ordering is NOT ESTIMABLE. Its results never authorize execution or protection cancellation. [Management amendment V1](ATLAS_S48_MANAGEMENT_RESEARCH_AMENDMENT_PROPOSED_V1.md) and S47 comparison remain **PROPOSED**; accepted operative authority is absent.

## Binance technical reuse

Pinned Skills Hub revision `9960c675387bd27f8866645b83693c1fa87242f6`; exact skill/reference and current official API response hashes, installed NautilusTrader 2.0.0rc5 stubs/WHEEL/compiled files are in [upstream identities](transition-s48-evidence/upstream_sources.json). [Comparison](ATLAS_S48_BINANCE_SKILLS_HUB_CONTRACT_COMPARISON.md) separates implemented contracts, demonstrated readback gaps, installed adapter UNVERIFIED and duplicate tools avoided. Two old positive fake conditional fixtures add only the documented discriminator; assertions and qualification flags are unchanged and their preceding failures are retained. No fabricated native qualification was added.

Request parameter prohibition and response echoes differ. Installed factory tests do not prove compiled wire omission of quantity/reduceOnly with closePosition, partial-fill event ordering, retained conditional history or atomic/near-atomic entry protection. These remain **UNVERIFIED / TEST GATE**. No Binance CLI, keys, signed endpoint or native order was used. Funding/currency/fee observations reuse existing economic evidence; actual liquidation/ADL delivery and final account cash completeness remain unqualified. Community strategy profitability is not established.

## Verification ledger

Exact reports/commands, failures/skips, source-tree identities, raw and normalized hashes: [manifest](transition-s48-evidence/evidence_manifest.json). Requirement/fault coverage and its limits: [lifecycle map](transition-s48-evidence/lifecycle_requirement_map.json). Counts overlap and must not be summed into opportunities. No full-repository suite claim.

- S47 repaired consumers: **154 pass**.
- Initial protection consumers: **37 pass**; lifecycle consumers: **101 pass**; shadow/protection consumers: **83 pass**; extended focused checkpoint: **83 pass**.
- Runtime/persistence/V1/risk/writer impacts: **456 pass, 3 failures, 1 skip**. Failures are the three S41 Binance mock-port cases lacking current readiness context; accepted-base archive reproduces all three (**17 pass, 3 failures**). Keep **TEST GATE**; no readiness weakening or synthetic positive proof. Skip is the explicit opt-in public testnet connectivity test.
- Agent/calendar/event consumers: **184 pass**, including original unmodified combined calendar/event suite.
- Peer chronology/features/S6, due-work/outcomes/maturity/capture/replay/M15: **141 pass**.
- Broker/worker/ops/memory: **56 pass, 1 failure**. S27 exact sleeve assertion omits accepted S6 approval-pending text; the accepted base reproduces it. Keep **TEST GATE**; no selector/assertion change.
- Final delivery-focused source/test run: **175 pass**, no failures/skips. Native timer wall-clock failure in an intermediate run is retained; unchanged rerun passed. No sustained timer/stream qualification follows from that pass.

Ruff lint, compileall, pip check, uv Python-3.12 lock check, freeze hashes and diff/secret review are recorded. Newly added Python files are formatted. The accepted peer formatting baseline remains byte-identical and fails formatting in both base/current. Uncached full-source mypy: **52 errors in both accepted baseline and current**, matching messages/counts after line-shift normalization, **zero added diagnostics**. This is baseline-aware evidence, not a repository type-check pass. Four changed/new module checks pass with imports skipped; earlier new naming/type errors were corrected and retained. No broad cleanup. No dependency/lock/license change; existing DuckDB 1.5.5 installed/requirements versus uv.lock 1.5.6 discrepancy remains. Optional gates/skips were not hidden or changed.

## Changed source and tests

Source: `src/atlas/runtime/{binance_demo,binance_reconciliation,protection_deadline,selected_demo,position_lifecycle}.py`; `src/atlas/v2/agent_intelligence/event_extraction_transport.py`; `src/atlas/v2/research/{decision_brief,management_shadow}.py`.

Tests: `tests/v2/{test_session026_agent_intelligence,test_session048_s47_repairs,test_session048_management_shadow}.py`; `tests/runtime/{test_session041_binance_demo,test_session041_binance_reconciliation,test_session048_position_protection,test_session048_lifecycle_projection}.py`. Exact hashes are in the manifest. Routing updates: existing README/docs index; coordinator checkpoint and append-only translog. Generated local Serena state is not product source.

## Remaining gates and rollback

**TEST GATE:** inherited S41 readiness/S27 sleeve tests; native Binance wire and fill-time protection equivalence; actual cash/fee/funding/margin/liquidation/ADL completeness; sustained supervisor/capacity; broader-altcoin capital; independently accepted discretionary management. Historical Linux F1/A2, sustained drain, fake Bybit chronology and real final-M15 delivery qualifications remain unchanged. **NOT ESTIMABLE:** after-cost incremental value and CPU/RSS/latency/bandwidth/token savings. Test wall times are recorded only as test execution timing.

Prospective economic floor remains eight genuine weeks and 200 matured opportunities with regime/dependence coverage; it is not an automatic pass. Capital and assisted execution remain disabled.

Rollback is a new reviewed revert of this implementation on an isolated branch, keeping durable journals, evidence, pending commands and native protection intact; never reset the owner's dirty checkout, delete a stop or infer resolution of UNKNOWN. No schema rollback is required. Research V2 brief artifacts remain immutable history; regenerated unbound V1 facts must not regain trust. The next bounded action is independent GitHub review of exact pushed source/evidence, then a separately scoped decision on retained test fixtures and remaining qualification. S49 depends on acceptance and any separately required amendment; this handoff does not launch it.
