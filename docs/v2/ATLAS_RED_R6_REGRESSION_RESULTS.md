# ATLAS RED R6 regression results

R5 recorded 2,886 passes, 14 failures, and 8 skips. The five inherited Session031 failures were reproduced on the required R5 base before edits: 36 tests, 5 failures. R6 then ran 150 focused tests with 150 passing and completed the full repository run with 2,914 collected, 2,906 passing, zero failures, and 8 skips.

## Original 14 failures

| Original failure | Cause and correction | R6 disposition |
|---|---|---|
| `test_advancing_receipts_are_deferred_to_the_next_causal_cycle` | Bybit product metadata was archived but not registered as typed `ProductMetadataEvidenceV1`; feature dependencies also included a derived diagnostic identity that was not an artifact. Publish and validate typed metadata evidence, and keep only actual artifacts in causal refs. | PASS |
| `test_rate_limit_and_disconnect_fail_closed_and_recovery_does_not_claim_trade_repair` | The same absent typed metadata dependency aborted the source cycle with `ValueError`. The corrected artifact path lets rate-limit/disconnect recovery complete without claiming trade repair. | PASS |
| `test_restart_keeps_epoch_bound_and_does_not_duplicate_event_or_receipt` | Collection failed before event receipt references were persisted. Typed product-metadata publication restores the durable cycle. | PASS |
| `test_deadline_crossed_during_acquisition_is_deferred_then_recorded_expired` | Missing causal metadata prevented the expired event from being recorded. The corrected dependency publication permits the expected expiry evidence. | PASS |
| `test_fake_bybit_source_flows_through_collector_archive_index_supervisor_and_calendar` | The cycle stopped before it could publish its supervisor receipt because product metadata had no typed artifact ref. The archive/index/supervisor path now completes. | PASS |
| `test_ack_loss_duplicate_error_and_local_enqueue_never_authorize_resend[None-dispatch_persisted_demo_command]` | A Binance fixture called the real command dispatcher without a readiness proof. The runtime correctly rejected it. Keep generic unknown-ack coverage on Bybit; cover Binance outcomes through the existing valid-readiness test. | PASS; production readiness enforcement unchanged |
| Same test `[TimeoutError-dispatch_persisted_demo_command]` | Same missing-context fixture issue. | PASS; production readiness enforcement unchanged |
| Same test `[ValueError-dispatch_persisted_demo_command]` | Same missing-context fixture issue. | PASS; production readiness enforcement unchanged |
| `test_installed_public_run_connects_context_to_writer_and_closes` | Product fixture `Context` rejected the documented `event_extraction_provider=None` constructor kwarg. Updated the fixture to accept and assert the exact kwarg. | PASS |
| `test_complete_observed_path_keeps_economic_result_not_estimable` | `effective_available_at_ns` did not resolve availability for a durable compact `PublicStreamFrameIndexV1` locator. Added lookup of the locator's verified stored `available_at_ns`. | PASS |
| `test_adoption_retains_all_rows_and_reconciles_identity_published_by_stream_service[False]` | Test `SimpleNamespace` omitted the required `current_products` value. Corrected the fixture. | PASS |
| Same adoption test `[True]` | Same omitted fixture field. | PASS |
| `test_broad_universe_union_lookup_keeps_future_product_cutoff` | Behavior was fail-closed, but the error text did not match the public “unavailable” contract. Clarified the error message. | PASS |
| `test_gapped_feature_is_preserved_as_not_estimable_not_as_a_value` | R5 identified wall-clock sensitivity and made the test clock deterministic; the R6 full run independently confirms the final R5-base correction. | PASS |

No blanket skip or xfail was added, and no safety assertion was weakened. Binance readiness failures were resolved in test setup, not by removing the readiness gate. The compact locator fix uses the durable extent registration's availability timestamp; it does not invent market observations.

## Final test evidence

- Session031 red reproduction: 36 total, 5 failures before the production fix.
- Focused regression selection on the final code: 150 passed, 0 failed, 0 skipped; 16.664 seconds JUnit case time.
- Full repository suite: 2,914 total, 2,906 passed, 0 failed, 8 skipped. The eight skips are listed in `transition-r6-evidence/full-suite-final.junit.xml`; they cover explicit opt-in/native-Windows checks and two optional provider imports.
- Static checks: Ruff, compileall, frozen lock, and `uv pip check` pass. Source mypy remains at the exact R5 baseline of 53 errors in 17 files with zero normalized message delta.

The full suite is a software regression result, not live market, provider, Demo, economic, or production qualification.
