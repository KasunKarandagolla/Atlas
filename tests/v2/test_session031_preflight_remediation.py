"""Adversarial origin, science-inventory, and pagination regressions for S31."""

from __future__ import annotations

from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.science.discovery import DiscoveryExperimentV2
from atlas.v2.science.m0 import M0CalibrationV2
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.scenario_engine import PretradeExecutionScenarioV2, ScenarioGenerationStatusV2
from atlas.v2.science.session031_preflight import build_public_shadow_preflight_v1

AS_OF = 1_800_000_000_000_000
M15_NS = 15 * 60 * 1_000_000_000
M1_NS = 60 * 1_000_000_000
ORIGIN = (AS_OF // M15_NS) * M15_NS


def _key(symbol: str, revision: str) -> InstrumentKeyV2:
    return InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        symbol, symbol.removesuffix("USDT"), "USDT", "USDT", sha256_json(revision),
    )


def _instrument_identity(key: InstrumentKeyV2) -> str:
    return sha256_json(key.to_dict())


def _product(key: InstrumentKeyV2) -> ProductContractV2:
    return ProductContractV2(
        key, ORIGIN - M15_NS, ORIGIN - M15_NS, ORIGIN - M15_NS,
        Decimal("1"), Decimal("0.1"), Decimal("0.01"), Decimal("0.01"),
        TradingStatusV2.TRADING, sha256_json({"product": key.to_dict()}),
    )


def _index_product(repo: OpsRepository, key: InstrumentKeyV2) -> ProductContractV2:
    product = _product(key)
    repo.register_artifact(ArtifactIndexEntryV2(
        product.content_hash, "ProductContractV2", product.content_hash,
        product.effective_at_ns, product.available_at_ns, {"product": product.to_dict()},
    ))
    return product


def _persist_handoff(
    repo: OpsRepository,
    key: InstrumentKeyV2,
    *,
    origin_ns: int,
    received_at_ns: int,
    available_at_ns: int,
    information_cutoff_ns: int,
    processing_at_ns: int | None = None,
    duplicate_nonce: str = "one",
    mismatched_key: InstrumentKeyV2 | None = None,
) -> OpsDecisionEventV1:
    product = _index_product(repo, key)
    record_id = sha256_json({"record": key.contract_revision, "origin": origin_ns, "nonce": duplicate_nonce})
    bar_body = {
        "record_id": record_id,
        "raw_payload_hash": sha256_json({"payload": record_id}),
        "instrument_revision": key.contract_revision,
        "interval": "15M",
        "open_at_ns": origin_ns - M15_NS,
        "close_at_ns": origin_ns,
        "open": "100", "high": "101", "low": "99", "close": "100",
        "volume": "10", "final": True,
    }
    bar_ref = sha256_json(bar_body)
    observation_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
    observation_key = mismatched_key or key
    repo.register_artifact(ArtifactIndexEntryV2(
        observation_ref, "PublicObservationIndexV2", sha256_json({"observation": record_id}),
        received_at_ns, available_at_ns,
        {
            "record_id": record_id, "source_id": "bybit-public",
            "instrument_revision": key.contract_revision,
            "instrument_key_json": observation_key.to_canonical_json(),
            "event_at_ns": origin_ns, "published_at_ns": origin_ns,
            "availability_class": "ACTUAL_SYSTEM", "bar_content_hash": bar_ref,
        },
    ))
    repo.register_artifact(ArtifactIndexEntryV2(
        bar_ref, "CausalBarV2", bar_ref, available_at_ns, available_at_ns,
        {"bar": bar_body, "source_observation_ref": observation_ref},
    ))
    trigger_body = {
        "version": "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1",
        "source_observation_ref": observation_ref,
        "bar_ref": bar_ref,
        "product_ref": product.content_hash,
        "source_id": "bybit-public",
        "source_event_at_ns": origin_ns,
        "source_published_at_ns": origin_ns,
        "received_at_ns": received_at_ns,
        "available_at_ns": available_at_ns,
        "information_cutoff_ns": information_cutoff_ns,
        "authority": "ZERO",
    }
    trigger_ref = sha256_json(trigger_body)
    repo.register_artifact(ArtifactIndexEntryV2(
        trigger_ref, "OpsPublicFinalBarTriggerV1", trigger_ref,
        available_at_ns, available_at_ns, {"trigger": trigger_body},
    ))
    event = OpsDecisionEventV1(
        sha256_json({"version": "OPS_DECISION_EVENT_FROM_FINAL_BAR_V1", "trigger_ref": trigger_ref}),
        "CONFIRMED_15M_CLOSE", "bybit-public", trigger_ref,
        origin_ns, origin_ns, received_at_ns, available_at_ns, information_cutoff_ns,
        information_cutoff_ns + 5_000_000_000, (trigger_ref, observation_ref, bar_ref, product.content_hash),
    )
    repo.register_artifact(ArtifactIndexEntryV2(
        event.content_hash, "OpsDecisionEventSourceV1", event.content_hash,
        information_cutoff_ns, information_cutoff_ns,
        {"event": event.to_dict(), "trigger_record_id": record_id},
    ))
    if processing_at_ns is not None:
        receipt_body = {
            "decision_event": event.to_dict(), "created_at_ns": processing_at_ns,
            "terminal_status": "NO_TRADE",
        }
        receipt_hash = sha256_json(receipt_body)
        receipt_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptV1", "content_hash": receipt_hash})
        repo.register_artifact(ArtifactIndexEntryV2(
            receipt_ref, "OpsSupervisorReceiptV1", receipt_hash,
            processing_at_ns, processing_at_ns, {"receipt": receipt_body},
        ))
        identity_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": event.event_id})
        identity_body = {"event_id": event.event_id, "receipt_ref": receipt_ref}
        repo.register_artifact(ArtifactIndexEntryV2(
            identity_ref, "OpsSupervisorReceiptIdentityV1", sha256_json(identity_body),
            processing_at_ns, processing_at_ns, identity_body,
        ))
    return event


def _science_entry(index: int, artifact_type: str = "M0CalibrationV2", *, available_at_ns: int = AS_OF):
    calibration = M0CalibrationV2(
        sha256_json({"action": index}), AS_OF - 100, sha256_json({"oof": index}),
        index + 1, Decimal("0.25"), "CALIBRATED", None,
    )
    body = calibration.to_dict()
    ref = sha256_json(body)
    metadata_key = {
        "M0CalibrationV2": "calibration",
        "M1SupportV2": "support",
        "DiscoveryAttemptV2": "attempt",
    }[artifact_type]
    if artifact_type != "M0CalibrationV2":
        body = {"version": "M1_SUPPORT_V2_V1", "action_hash": sha256_json(index), "cutoff_ns": AS_OF}
        if artifact_type == "M1SupportV2":
            body = {"version": "M1_SUPPORT_V2_V1", "action_hash": sha256_json(index), "cutoff_ns": AS_OF,
                    "compatible_training_row_refs": [], "independent_training_row_refs": [], "status": "INSUFFICIENT"}
            body.update({"provenance_counts": [], "execution_state_counts": [], "missing_feature_counts": [],
                         "start_ns": None, "end_ns": None})
        if artifact_type == "DiscoveryAttemptV2":
            body = {"version": "DISCOVERY_ATTEMPT_V2_V2", "experiment_ref": sha256_json("exp"),
                    "experiment_id": "exp", "attempt_id": f"attempt-{index}", "started_at_ns": AS_OF,
                    "state": "FAILED"}
        ref = sha256_json(body)
    return ArtifactIndexEntryV2(ref, artifact_type, ref, AS_OF - 100, available_at_ns,
                                {metadata_key: body})


def _pretrade_entry(*, available_at_ns: int) -> ArtifactIndexEntryV2:
    cutoff = AS_OF - 100
    inputs = tuple(CausalInputV2(sha256_json(f"pretrade-input-{name}"), name, cutoff, cutoff)
                   for name in ("MODEL", "CALIBRATION", "EXECUTION_MODEL"))
    scenario = PretradeExecutionScenarioV2(
        sha256_json("pretrade-action"), sha256_json("pretrade-action-artifact"), cutoff,
        inputs[0], inputs[1], inputs[2], (), (), (), (), (), "fixture-generator-v1",
        sha256_json("pretrade-scenario-set"), cutoff, cutoff, available_at_ns,
        available_at_ns + 100, ScenarioGenerationStatusV2.NOT_ESTIMABLE, False,
        "typed fixture has no qualified inputs", 31, 1,
    )
    ref = scenario.content_hash
    return ArtifactIndexEntryV2(ref, "PretradeExecutionScenarioV2", ref, cutoff,
                                available_at_ns, {"scenario": scenario.to_dict()})


def _discovery_experiment_entry() -> ArtifactIndexEntryV2:
    experiment = DiscoveryExperimentV2(
        "fixture-experiment", "fixture-family", "numerical_challengers", ("candles",),
        ("CUT_OFF_AVAILABLE_ONLY",), 1, 1, sha256_json("baseline"), ("net_value",),
        "chronology-v1", "purge-embargo-v1", "fixture-family", "stop-at-budget",
        sha256_json("untouched-holdout"), "UNTOUCHED", True, AS_OF - 100,
    )
    body = experiment.to_dict()
    return ArtifactIndexEntryV2(
        experiment.content_hash, "DiscoveryExperimentV2", experiment.content_hash,
        AS_OF - 100, AS_OF, {"experiment": body},
    )


def _public_observation_entry(index: int, *, available_at_ns: int) -> ArtifactIndexEntryV2:
    record_id = sha256_json({"public-observation": index})
    body_hash = sha256_json({"payload": index})
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
    return ArtifactIndexEntryV2(
        ref, "PublicObservationIndexV2", body_hash, AS_OF - 100, available_at_ns,
        {"record_id": record_id, "source_id": "bybit-public",
         "instrument_revision": sha256_json("btc-r1" if index % 2 == 0 else "eth-r1"),
         "event_type": "TRADE", "event_at_ns": AS_OF - 100, "availability_class": "ACTUAL_SYSTEM"},
    )


def test_origin_matching_uses_persisted_bar_close_and_instrument(tmp_path):
    db = tmp_path / "origins.sqlite"
    btc = _key("BTCUSDT", "btc-r1")
    eth = _key("ETHUSDT", "eth-r1")
    receipt_and_cutoff = ORIGIN + 2_000_000_000
    processed = receipt_and_cutoff + 1_000_000_000
    with OpsRepository(db) as repo:
        _index_product(repo, btc)
        _index_product(repo, eth)
        _persist_handoff(
            repo, btc, origin_ns=ORIGIN, received_at_ns=receipt_and_cutoff,
            available_at_ns=receipt_and_cutoff, information_cutoff_ns=receipt_and_cutoff,
            processing_at_ns=processed,
        )
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(
            repo, as_of_ns=processed, campaign_start_ns=ORIGIN - M15_NS,
        )
    funnel = report["reports"]["strategy_and_funnel"]
    assert funnel["actually_observed_15m_handoffs"] == [ORIGIN]
    assert funnel["expected_15m_origin_slots"] == [ORIGIN]
    assert receipt_and_cutoff not in funnel["expected_15m_origin_slots"]
    btc_slots = funnel["per_instrument_origin_accounting"][_instrument_identity(btc)]["slots"]
    eth_slots = funnel["per_instrument_origin_accounting"][_instrument_identity(eth)]["slots"]
    assert btc_slots[0]["bar_close_decision_origin_ns"] == ORIGIN
    assert btc_slots[0]["information_cutoff_ns"] == ORIGIN + 2_000_000_000
    assert btc_slots[0]["actual_received_at_ns"] == ORIGIN + 2_000_000_000
    assert btc_slots[0]["processing_at_ns"] == processed
    assert btc_slots[0]["decision_status"] == "TIMELY_PROCESSED"
    assert eth_slots[0]["decision_status"] == "MISSING_DECISION_HANDOFF"
    assert eth_slots[0]["missing_decision_handoff"] is True
    assert funnel["expected_s3_native_m1_origins_remain_separate_from_m15_handoffs"] is True
    assert len(funnel["expected_s3_native_m1_origins"]["expected_native_s3_one_minute_origins"]) == 15


def test_late_duplicate_invalid_and_missing_handoffs_are_not_collapsed(tmp_path):
    db = tmp_path / "handoff-classes.sqlite"
    btc = _key("BTCUSDT", "btc-r1")
    cutoff = ORIGIN + 2_000_000_000
    deadline = cutoff + 5_000_000_000
    with OpsRepository(db) as repo:
        _index_product(repo, btc)
        first = _persist_handoff(
            repo, btc, origin_ns=ORIGIN, received_at_ns=cutoff, available_at_ns=cutoff,
            information_cutoff_ns=cutoff, processing_at_ns=deadline + 1,
        )
        _persist_handoff(
            repo, btc, origin_ns=ORIGIN, received_at_ns=cutoff, available_at_ns=cutoff,
            information_cutoff_ns=cutoff, processing_at_ns=cutoff + 1,
            duplicate_nonce="duplicate",
        )
        _persist_handoff(
            repo, btc, origin_ns=ORIGIN, received_at_ns=cutoff, available_at_ns=cutoff,
            information_cutoff_ns=cutoff, processing_at_ns=cutoff + 1,
            duplicate_nonce="mismatch", mismatched_key=_key("ETHUSDT", "eth-wrong"),
        )
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(
            repo, as_of_ns=ORIGIN + M15_NS + 1, campaign_start_ns=ORIGIN - M15_NS,
        )
    funnel = report["reports"]["strategy_and_funnel"]
    assert funnel["duplicate_handoff_count"] == 1
    assert any(row["reason"] == "TRIGGER_BAR_INSTRUMENT_OR_CHRONOLOGY_MISMATCH"
               for row in funnel["invalid_handoffs"])
    btc_slots = funnel["per_instrument_origin_accounting"][_instrument_identity(btc)]["slots"]
    assert btc_slots[0]["decision_status"] == "DUPLICATE_HANDOFF"
    assert btc_slots[1]["decision_status"] == "MISSING_DECISION_HANDOFF"
    assert btc_slots[1]["missing_source_evidence"] == ["NO_VALID_TRIGGER_BAR_EVIDENCE"]
    assert first.event_id in {item["event_id"] for item in funnel["validated_handoffs"]}
    assert {item["decision_status"] for item in funnel["validated_handoffs"]} == {
        "INELIGIBLE_LATE_PROCESSING", "TIMELY_PROCESSED",
    }


def test_same_revision_does_not_merge_distinct_instrument_origins(tmp_path):
    db = tmp_path / "same-revision-different-instruments.sqlite"
    btc = _key("BTCUSDT", "shared-revision")
    eth = _key("ETHUSDT", "shared-revision")
    cutoff = ORIGIN + 2_000_000_000
    with OpsRepository(db) as repo:
        _index_product(repo, btc)
        _index_product(repo, eth)
        _persist_handoff(
            repo, btc, origin_ns=ORIGIN, received_at_ns=cutoff,
            available_at_ns=cutoff, information_cutoff_ns=cutoff,
        )
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(
            repo, as_of_ns=ORIGIN + 3_000_000_000, campaign_start_ns=ORIGIN - M15_NS,
        )
    accounting = report["reports"]["strategy_and_funnel"]["per_instrument_origin_accounting"]
    assert len(accounting) == 2
    assert accounting[_instrument_identity(btc)]["slots"][0]["decision_status"] == "HANDOFF_PERSISTED_UNRESOLVED"
    assert accounting[_instrument_identity(eth)]["slots"][0]["decision_status"] == "MISSING_DECISION_HANDOFF"


def test_science_inventory_counts_as_of_payloads_and_keeps_unavailable_distinct(tmp_path):
    db = tmp_path / "science.sqlite"
    valid = _science_entry(1)
    future = _science_entry(2, available_at_ns=AS_OF + 1)
    body = {"version": "M0_CHRONOLOGICAL_CALIBRATION_V1"}
    malformed_ref = sha256_json(body)
    malformed = ArtifactIndexEntryV2(
        malformed_ref, "M0CalibrationV2", malformed_ref, AS_OF - 100, AS_OF, {"calibration": body},
    )
    wrong_content_ref = sha256_json("conflicting-ref")
    conflict = ArtifactIndexEntryV2(
        wrong_content_ref, "M0CalibrationV2", sha256_json("different-content"), AS_OF - 100, AS_OF,
        {"calibration": valid.metadata["calibration"]},
    )
    with OpsRepository(db) as repo:
        repo.register_artifacts((valid, future, malformed, conflict, _discovery_experiment_entry()))
    before = db.stat().st_mtime_ns
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(repo, as_of_ns=AS_OF)
    science = report["reports"]["science_and_research"]
    assert science["available_artifact_counts"]["M0CalibrationV2"] == 1
    assert science["available_artifact_counts"]["M1ModelArtifactV2"] == 0
    assert science["available_artifact_counts"]["ResearchCalibrationV2"] is None
    assert science["available_artifact_counts"]["DiscoveryExperimentV2"] == 1
    assert science["artifact_inventory"]["research_calibration"]["reason"]
    assert science["artifact_inventory"]["M0_calibration"]["indexed_count"] == 3
    assert science["artifact_inventory"]["M0_calibration"]["validated_payload_count"] == 1
    assert science["artifact_inventory"]["M0_calibration"]["invalid_payload_count"] == 2
    assert db.stat().st_mtime_ns == before


def test_preflight_keeps_s3_unqualified_without_persisted_lineage_and_trade_continuity(tmp_path):
    db = tmp_path / "s3-production-gate.sqlite"
    product = _product(_key("BTCUSDT", "btc-r1"))
    with OpsRepository(db) as repo:
        repo.register_artifact(ArtifactIndexEntryV2(
            product.content_hash, "ProductContractV2", product.content_hash,
            product.effective_at_ns, product.available_at_ns, {"product": product.to_dict()},
        ))
    before = db.stat().st_mtime_ns
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(repo, as_of_ns=AS_OF)
    readiness, = report["reports"]["strategy_and_funnel"]["per_instrument_readiness"]
    assert readiness["sleeves"]["S3"]["status"] == "NOT_ESTIMABLE"
    assert "S3_TRADE_CONTINUITY_UNPROVEN_BY_BOUNDED_REST_RECENT_TRADE_HISTORY" in (
        readiness["sleeves"]["S3"]["reason_codes"]
    )
    assert readiness["s3_persisted_lineage"]["trade_continuity_status"] == (
        "UNVERIFIABLE_BOUNDED_REST_RECENT_TRADE_HISTORY"
    )
    assert readiness["s3_persisted_lineage"]["qualification_status"] == "NOT ESTIMABLE"
    assert db.stat().st_mtime_ns == before


def test_pretrade_typed_artifact_hash_and_as_of_availability_are_counted_correctly(tmp_path):
    db = tmp_path / "typed-pretrade-inventory.sqlite"
    with OpsRepository(db) as repo:
        repo.register_artifacts((
            _pretrade_entry(available_at_ns=AS_OF),
            _pretrade_entry(available_at_ns=AS_OF + 1),
        ))
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(repo, as_of_ns=AS_OF)
    science = report["reports"]["science_and_research"]
    assert science["available_artifact_counts"]["PretradeExecutionScenarioV2"] == 1
    assert science["artifact_inventory"]["pretrade_scenarios"]["indexed_count"] == 1
    assert science["artifact_inventory"]["pretrade_scenarios"]["validated_payload_count"] == 1


def test_keyset_inventory_over_ten_thousand_is_complete_and_bounded(tmp_path):
    db = tmp_path / "large-inventory.sqlite"
    total_calibrations = 7_500
    total_support = 3_001
    future_count = 111
    entries = [
        _science_entry(index, "M0CalibrationV2", available_at_ns=AS_OF + 1 if index < future_count else AS_OF)
        for index in range(total_calibrations)
    ]
    entries.extend(_science_entry(index, "M1SupportV2") for index in range(total_support))
    expected_as_of = total_calibrations - future_count + total_support
    observation_count = 500
    observations_future = 50
    entries.extend(_public_observation_entry(
        index, available_at_ns=AS_OF + 1 if index < observations_future else AS_OF,
    ) for index in range(observation_count))
    expected_as_of += observation_count - observations_future
    with OpsRepository(db) as repo:
        repo.register_artifacts(entries)
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(
            repo, as_of_ns=AS_OF, inventory_page_size=137,
        )
    inventory = report["artifact_inventory"]
    science = report["reports"]["science_and_research"]
    assert inventory["complete"] is True
    assert inventory["records_processed"] == expected_as_of
    assert inventory["pages_processed"] > 1
    assert inventory["page_size"] == 137
    assert inventory["as_of_filter"] == "available_at_ns <= as_of_ns applied in SQL before keyset pagination"
    assert inventory["invalid_entry_count"] == 0
    assert inventory["detail_completeness"] is True
    assert inventory["retained_detail_entries"] == 0
    assert report["reports"]["public_ingestion"]["observed_raw_receipts"] == observation_count - observations_future
    assert set(report["reports"]["public_ingestion"]["instruments_by_revision"]) == {
        sha256_json("btc-r1"), sha256_json("eth-r1"),
    }
    assert len(report["reports"]["public_ingestion"]["chronology"]) <= 200
    assert science["available_artifact_counts"]["M0CalibrationV2"] == total_calibrations - future_count
    assert science["available_artifact_counts"]["M1ModelArtifactV2"] == total_support


def test_interrupted_inventory_is_explicit_and_never_reports_partial_counts_as_complete(tmp_path):
    db = tmp_path / "interrupted.sqlite"
    with OpsRepository(db) as repo:
        repo.register_artifacts(_science_entry(index) for index in range(1_300))
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(
            repo, as_of_ns=AS_OF, inventory_page_size=100, inventory_max_pages=2,
        )
    inventory = report["artifact_inventory"]
    science = report["reports"]["science_and_research"]
    assert inventory["complete"] is False
    assert inventory["reason"] == "PAGE_BUDGET_EXCEEDED"
    assert inventory["records_processed"] == 200
    assert science["available_artifact_counts"]["M0CalibrationV2"] is None
    assert science["artifact_inventory"]["M0_calibration"]["status"] == "TEST GATE"


def test_keyset_pages_have_no_equal_timestamp_gaps_and_snapshot_excludes_concurrent_writer(tmp_path):
    db = tmp_path / "snapshot.sqlite"
    entries = [_science_entry(index) for index in range(51)]
    with OpsRepository(db) as repo:
        repo.register_artifacts(entries)
    expected = sorted((item.artifact_ref for item in entries), reverse=True)
    with OpsRepository(db, read_only=True) as reader:
        def read_all() -> list[str]:
            refs: list[str] = []
            cursor = None
            while True:
                page = reader.artifact_entries_by_types_page(
                    ("M0CalibrationV2",), as_of_ns=AS_OF, after=cursor, limit=7,
                )
                refs.extend(item.artifact_ref for item in page.entries)
                if page.next_cursor is None or len(page.entries) < 7:
                    break
                cursor = page.next_cursor
            return refs

        with reader.read_snapshot():
            first = reader.artifact_entries_by_types_page(
                ("M0CalibrationV2",), as_of_ns=AS_OF, limit=7,
            )
            with OpsRepository(db) as writer:
                writer.register_artifact(_science_entry(90_000))
            refs = [item.artifact_ref for item in first.entries]
            cursor = first.next_cursor
            while cursor is not None:
                page = reader.artifact_entries_by_types_page(
                    ("M0CalibrationV2",), as_of_ns=AS_OF, after=cursor, limit=7,
                )
                refs.extend(item.artifact_ref for item in page.entries)
                cursor = page.next_cursor if len(page.entries) == 7 else None
        assert refs == expected
        assert read_all() == sorted([*expected, _science_entry(90_000).artifact_ref], reverse=True)


def test_malformed_page_row_is_counted_and_cursor_still_advances(tmp_path):
    db = tmp_path / "invalid-row.sqlite"
    with OpsRepository(db) as repo:
        repo.register_artifact(_science_entry(1))
        bad_ref = sha256_json("bad-json-row")
        repo._connection.execute(
            "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) "
            "VALUES(?,?,?,?,?,?)",
            (bad_ref, "M0CalibrationV2", bad_ref, AS_OF - 100, AS_OF, "{"),
        )
    with OpsRepository(db, read_only=True) as repo, repo.read_snapshot():
        first = repo.artifact_entries_by_types_page(("M0CalibrationV2",), as_of_ns=AS_OF, limit=1)
        second = repo.artifact_entries_by_types_page(
            ("M0CalibrationV2",), as_of_ns=AS_OF, after=first.next_cursor, limit=1,
        )
    assert first.next_cursor is not None
    assert second.next_cursor is not None
    assert first.invalid_entry_count + second.invalid_entry_count == 1


def test_malformed_index_row_blocks_science_zero_or_passing_counts(tmp_path):
    db = tmp_path / "invalid-science-index-row.sqlite"
    valid = _science_entry(1)
    with OpsRepository(db) as repo:
        repo.register_artifact(valid)
        bad_ref = sha256_json("malformed-science-index-row")
        repo._connection.execute(
            "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) "
            "VALUES(?,?,?,?,?,?)",
            (bad_ref, "M0CalibrationV2", bad_ref, AS_OF - 100, AS_OF, "{"),
        )
    with OpsRepository(db, read_only=True) as repo:
        report = build_public_shadow_preflight_v1(repo, as_of_ns=AS_OF)
    inventory = report["artifact_inventory"]
    science = report["reports"]["science_and_research"]
    assert inventory["complete"] is True
    assert inventory["invalid_entry_count"] == 1
    assert science["available_artifact_counts"]["M0CalibrationV2"] is None
    assert science["artifact_inventory"]["M0_calibration"]["status"] == "TEST GATE"
    assert science["artifact_inventory"]["M0_calibration"]["reason"] == (
        "MALFORMED_ARTIFACT_INDEX_ENTRY_PRESENT"
    )
