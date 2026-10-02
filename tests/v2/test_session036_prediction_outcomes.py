"""Diagnostic labels bind exact source prices and sealed completion identities."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.research_prediction_outcomes import (
    MAINTENANCE_INTERVAL_NS_V1,
    ResearchPredictionOutcomeMaintenanceV1,
    _log_return,
    _producer,
    index_research_prediction_outcome_v1,
    validate_research_prediction_outcome_v1,
)

from . import test_session036_research_model_shadow as shadow_tests


def _matured_fixture(repo, tmp_path):
    receipt, bars, now = shadow_tests._fixture(repo)
    maintenance = ResearchPredictionOutcomeMaintenanceV1(
        run_id="run-model-shadow", config_hash="b" * 64, archive_root=tmp_path,
        clock_ns=lambda: now[0],
        bar_reader=lambda *_args, **kwargs: tuple(
            bar for bar in bars if bar.observation_index_ref in
            {entry.artifact_ref for entry in kwargs["index_entries"]}),
    )
    maintenance.register_target(repo, available_at_ns=receipt.event.information_cutoff_ns - 1)
    shadow_tests._shadow(tmp_path, now, lambda *_args, **_kwargs: tuple(bars))(receipt, receipt.content_hash, repo)
    terminal = repo.artifact_entries("ResearchModelTerminalV1")[0]
    origin = bars[-1].bar
    boundary = origin.close_at_ns + BarIntervalV2.M15.duration_ns
    now[0] = boundary + 1
    raw = RawObservationV2.build(instrument_revision=origin.instrument_revision,
        source_id="BYBIT_PUBLIC_WS", event_type="BAR_15M", event_at_ns=boundary,
        received_at_ns=now[0], ingested_at_ns=now[0], available_at_ns=now[0],
        translation_version="prediction-outcome-fixture-v1", sequence=str(boundary),
        payload=b"future-final-bar-fixture", availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)
    bar = CausalBarV2(raw, BarIntervalV2.M15, origin.close_at_ns, boundary,
        Decimal("103"), Decimal("103"), Decimal("103"), Decimal("103"), Decimal("1"), True)
    ref = sha256_json({"fixture_observation": raw.record_id})
    key = _producer(repo, terminal.artifact_ref, now[0])[2].request.instrument_key
    repo.register_artifact(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", raw.content_hash,
        now[0], now[0], {"instrument_key_json": key.to_canonical_json(),
            "instrument_revision": key.contract_revision, "event_type": raw.event_type,
            "event_at_ns": raw.event_at_ns, "record_id": raw.record_id,
            "raw_payload_hash": raw.raw_payload_hash, "availability_class": raw.availability_class.value,
            "bar_content_hash": bar.content_hash}))
    bars = list(bars)
    bars.append(IndexedCausalBarV2(bar, ref))
    item = maintenance._measure(repo, terminal.artifact_ref, BarIntervalV2.M15.duration_ns, now[0])
    assert item.label_state == "MATURED"
    validate_research_prediction_outcome_v1(repo, item)
    return maintenance, item, now


def test_rehashed_substituted_endpoint_price_is_rejected(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _maintenance, item, now = _matured_fixture(repo, tmp_path)
        entry = repo.get_artifact(item.measurement_ref)
        measurement = dict(entry.metadata["measurement"])
        measurement["horizon_bar"] = dict(measurement["horizon_bar"], close="104", high="104")
        forged_ref = sha256_json(measurement)
        repo.register_artifact(ArtifactIndexEntryV2(forged_ref, "ResearchPredictionMeasurementV1",
            forged_ref, now[0], now[0], {"measurement": measurement}))
        forged = replace(item, measurement_ref=forged_ref, measured_horizon_close=Decimal("104"),
            measured_log_return=_log_return(item.measured_origin_close, Decimal("104")))
        with pytest.raises(ValueError, match="exact indexed canonical bars"):
            validate_research_prediction_outcome_v1(repo, forged)
        assert index_research_prediction_outcome_v1(repo, item) == item.content_hash


def test_restart_does_not_reconstruct_completed_endpoint_evidence(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        maintenance, item, now = _matured_fixture(repo, tmp_path)
        assert maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])["labels_written"] == 1
        assert repo.artifact_entries("ResearchPredictionCompletionIdentityV1")
        now[0] += MAINTENANCE_INTERVAL_NS_V1
        maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])  # Advance the sweep checkpoint.
        now[0] += MAINTENANCE_INTERVAL_NS_V1
        resumed = ResearchPredictionOutcomeMaintenanceV1(run_id=item.run_id, config_hash=item.config_hash,
            archive_root=tmp_path, clock_ns=lambda: now[0],
            bar_reader=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("completed raw reread")))
        assert resumed.run_cycle(repo, evidence_cutoff_ns=now[0])["labels_written"] == 0
