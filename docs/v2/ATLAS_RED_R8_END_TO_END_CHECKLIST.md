# ATLAS RED R8 end-to-end checklist

Track A means genuine external evidence in this R8 run. Track B means controlled offline tests. A `TESTED` Track B row describes existing per-component tests; it does not imply a single 21-stage integration run.

Production paths in the table are relative to `src/atlas/v2/`, except `scanner/*` paths under `src/atlas/scanner/` and `atlas/runtime/binance_demo.py` under `src/`. Test names beginning `test_session` are under `tests/v2/`; explicitly prefixed scanner and runtime tests are under their named directories.

| # | Workflow stage | Track A | Track B | Production path and representative tests exercised in the final suite |
|---:|---|---|---|---|
| 1 | Market observation | **BLOCKED BY ENVIRONMENT** | **TESTED** | `runtime/production.py`; `test_session041_broad_public_source.py`. No production Binance market request was possible. |
| 2 | Durable capture and source identity | **BLOCKED BY ENVIRONMENT** | **TESTED** | `data/durable_public_capture.py`; `test_session040_durable_capture.py`, `test_session041_capture_delivery_batching.py`. No R8 public observation was archived. |
| 3 | Product metadata | **BLOCKED BY ENVIRONMENT** | **TESTED** | `runtime/broad_universe.py`; `test_session041_dynamic_universe.py`, `test_session041_binance_qualification.py`. R7's 742-item Demo read is historical, not R8 market evidence. |
| 4 | Availability timestamps | **BLOCKED BY ENVIRONMENT** | **TESTED** | `features/pipeline.py`; `test_session034_causal_timing.py`, `test_phase1_data_model_integration.py`. |
| 5 | Finalized M15/H1/H4 bars | **BLOCKED BY ENVIRONMENT** | **TESTED** | `data/bars.py`, `features/candles.py`; `test_session037_bar_repair.py`, `test_session034_causal_timing.py`. |
| 6 | Technical features | **BLOCKED BY ENVIRONMENT** | **TESTED** | `features/technical.py`, `features/pipeline.py`; `test_phase1_data_model_integration.py`, `test_session037_feature_prefix.py`. |
| 7 | BTC/ETH and correlated-peer context | **BLOCKED BY ENVIRONMENT** | **TESTED** | `features/peer_context.py`; `test_session046_peer_context.py`, `test_session047_peer_chronology.py`. No genuine current context was observed. |
| 8 | Cheap broad-altcoin screening | **BLOCKED BY ENVIRONMENT** | **TESTED** | `scanner/cheap_scan.py`; `tests/scanner/test_phase5_end_to_end.py`, `tests/scanner/test_scanner_selection.py`. |
| 9 | Adaptive shortlist | **BLOCKED BY ENVIRONMENT** | **TESTED** | `scanner/ranking.py`, `scanner/selection.py`; `tests/scanner/test_scanner_ranking.py`, `tests/scanner/test_scanner_selection.py`. |
| 10 | Hard-risk reservation and sizing | **BLOCKED BY ENVIRONMENT** | **TESTED** | `runtime/full_strategy_surface.py`; `test_session041_strategy_surface.py`. No live or Demo risk reservation was created. |
| 11 | Exact hypothetical actions | **BLOCKED BY ENVIRONMENT** | **TESTED** | `science/pretrade.py`; `test_session041_s6_action_contract.py`. No R8 production action or order was created. |
| 12 | Realistic after-cost economics | **NOT ESTIMABLE** | **TESTED** | `runtime/economic_binding.py`; `test_session037_economic_binding.py`, `test_session036_economic_chronology.py`. Genuine cost inputs were unavailable. |
| 13 | M0/M1, scenarios, expected shortfall and qualified ML | **UNVERIFIED** | **TESTED** | `science/scenario_engine.py`; `test_session019_scenarios.py`, `test_session037_tuning_analysis.py`. No production-qualified estimate was available. |
| 14 | Complete Decision Brief | **NOT ESTIMABLE** | **TESTED** | `runtime/production_intelligence.py`, `research/decision_brief.py`; `test_session053_production_completion.py`, `test_session047_decision_brief.py`. |
| 15 | Bounded Gemma research | **TEST GATE** | **TESTED** | `agent_intelligence/provider.py`, `agent_intelligence/controller.py`; `test_session055_gemma_native.py`, `test_session047_research_agent.py`. No R8 provider request. |
| 16 | Agent response validation | **TEST GATE** | **TESTED** | `agent_intelligence/validation.py`; `test_session047_research_agent.py`, `test_session048_s47_repairs.py`. No real response; S47 amendment remains unaccepted. |
| 17 | Durable memoization and due work | **UNVERIFIED** | **TESTED** | `memory/repository.py`, `research/memory.py`; `test_session047_research_memory.py`, `test_session037_due_work.py`. Full production dispatch/invalidation composition remains open. |
| 18 | Outcome/maturity calendar | **TEST GATE** | **TESTED** | `runtime/outcome_maturity.py`; `test_session033_outcome_maturity.py`, `test_session034_scientific_calendar_closure.py`. The full expected scanner/calendar denominator is not established. |
| 19 | Exchange lifecycle and protection readiness | **TEST GATE** | **TEST GATE** | `runtime/binance_qualification.py`, `atlas/runtime/binance_demo.py`; `test_session041_binance_qualification.py`, `tests/runtime/test_session041_binance_demo.py`. These do not demonstrate Binance-native protection lifecycle. |
| 20 | Restart and recovery | **UNVERIFIED** | **TESTED** | `scanner/slot_ledger.py`, `models/local_process.py`; `tests/scanner/test_scanner_runner.py`, `test_worker_isolation.py`, `test_session041_recovery_queue.py`. Whole-chain recovery is not qualified. |
| 21 | Evidence and performance reports | **BLOCKED BY ENVIRONMENT** | **TESTED** | `runtime/read_only_report_worker.py`; `test_session038_report_worker.py`, `test_session036_tuning_export.py`. Live throughput, rolling capacity and economic qualification remain unavailable. |

## Scanner slot ledger scope

The existing Phase-5 LIVE runner now persists the current due slot before the first provider call. It records its policy version/hash, due time/deadline, declared eligible scope and append-only progress. Tests cover source outage, restart/retry, duplicate registration at a later timestamp, policy conflict, and fail-closed behavior without an archive.

This does not register a future horizon in advance. When Atlas is offline, it does not synthesize missed slots or a historical denominator. Progress records support `MISSED` and `EXPIRED`, but an automated scheduler does not yet publish those states. The complete calendar remains **TEST GATE**.

## Track summary

- **Track A:** zero genuine production market observations, zero genuine COMPLETE Briefs, zero Gemma generations, and zero exchange orders. The Binance Demo server-time request returned HTTP 451; production market access was absent from the managed allowlist.
- **Track B:** the full repository suite exercised existing per-component fixture contracts. It did not run a single production-composition test spanning all 21 stages. That integration gate remains **TEST GATE**.
- Economic value is **NOT ESTIMABLE** without genuine prospective inputs and matured outcomes.
