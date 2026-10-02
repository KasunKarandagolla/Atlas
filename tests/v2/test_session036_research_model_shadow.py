"""Postreceipt diagnostics retain negative origins and exact source identities."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pyarrow.parquet as pq

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1
from atlas.v2.runtime.research_model_shadow import StatisticalResearchShadowV1
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from . import test_session034_scientific_calendar_closure as s34


def _fixture(repo):
    product, _bar, event, _refs, now, port = s34._native_fixture(repo)
    result, _checkpoints = s34._process(repo, event, now, port)
    receipt = OpsSupervisorReceiptV1("MODEL_SHADOW_FIXTURE_V1", "a" * 64, event,
        "UNKNOWN", (), result, now)
    repo.register_artifact(ArtifactIndexEntryV2(receipt.content_hash, "OpsSupervisorReceiptV1",
        receipt.content_hash, now, now, {"receipt": receipt.to_dict()}))
    bars = []
    interval = 900_000_000_000
    final_close = event.information_cutoff_ns // interval * interval
    for index in range(3):
        close = final_close - (2 - index) * interval
        raw = RawObservationV2.build(instrument_revision=product.key.contract_revision,
            source_id="BYBIT_PUBLIC_WS", event_type="BAR_15M", event_at_ns=close,
            received_at_ns=event.information_cutoff_ns, ingested_at_ns=event.information_cutoff_ns,
            available_at_ns=event.information_cutoff_ns, translation_version="model-shadow-fixture-v1",
            sequence=str(close), payload=str(index).encode(), availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)
        price = Decimal(100 + index)
        bar = CausalBarV2(raw, BarIntervalV2.M15, close - interval, close,
            price, price, price, price, Decimal("1"), True)
        ref = sha256_json({"fixture_observation": raw.record_id})
        repo.register_artifact(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", raw.content_hash,
            raw.available_at_ns, raw.available_at_ns,
            {"instrument_key_json": product.key.to_canonical_json(), "bar_content_hash": bar.content_hash}))
        bars.append(IndexedCausalBarV2(bar, ref))
    return receipt, tuple(bars), [now + 1]


def _shadow(tmp_path, now, reader):
    return StatisticalResearchShadowV1(run_id="run-model-shadow", config_hash="b" * 64,
        source_sha="c" * 40, environment_lock_hash="d" * 64, archive_root=tmp_path,
        clock_ns=lambda: now[0], bar_reader=reader)


def test_negative_receipt_generates_typed_causal_forecast_and_survives_restart(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        receipt, bars, now = _fixture(repo)
        calls = []

        def reader(*_args, **kwargs):
            calls.append(kwargs)
            return bars

        shadow = _shadow(tmp_path, now, reader)
        shadow(receipt, receipt.content_hash, repo)
        assert calls[0]["limit"] == 128
        request = repo.artifact_entries("ResearchModelRequestV1")[0]
        terminal = repo.artifact_entries("ResearchModelTerminalV1")[0]
        assert request.available_at_ns > receipt.event.information_cutoff_ns
        assert request.metadata["routing"]["action_hash"] is None
        assert request.metadata["routing"]["decision_calendar_ref"] in receipt.calendar_refs
        assert request.metadata["routing"]["decision_event_ref"] == receipt.event.content_hash
        assert terminal.metadata["routing"]["usable"] is True
        assert terminal.metadata["routing"]["authority"] == "ZERO"
        assert repo.artifact_entries("ResearchModelValuesV1")
        identity = TuningRunIdentityV1("run-model-shadow", "b" * 64, "c" * 40, 0)
        exported = export_tuning_snapshot(path, tmp_path / "reports", identity, cutoff_ns=now[0])
        rows = pq.read_table(tmp_path / "reports" / identity.run_id / exported["partition"]).to_pylist()
        forecast = next(row for row in rows if row["row_kind"] == "MODEL_FORECAST")
        values = next(row for row in rows if row["row_kind"] == "MODEL_VALUES")
        assert forecast["decision_ref"] in receipt.calendar_refs
        assert forecast["request_ref"] == request.artifact_ref
        assert forecast["values_evidence_ref"] == values["artifact_ref"]
        assert forecast["model_sample_counts_json"] == '{"log_return:900000000000":2}'
        assert "log_return:900000000000:mean" in values["model_values_json"]
        assert exported["report"]["model_request_rows"] == exported["report"]["model_terminal_rows"] == 1
        assert not any(row["row_kind"] == "INVALID" and row["artifact_type"].startswith("ResearchModel") for row in rows)
    with OpsRepository(path) as repo:
        resumed = _shadow(tmp_path, now, lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not rerun")))
        resumed(receipt, receipt.content_hash, repo)
        assert len(repo.artifact_entries("ResearchModelTerminalV1")) == 1


def test_expired_receipt_retains_missing_case_without_reading_history(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        receipt, _bars, now = _fixture(repo)
        now[0] = receipt.event.deadline_ns
        shadow = _shadow(tmp_path, now, lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("expired")))
        shadow(receipt, receipt.content_hash, repo)
        diagnostic = repo.artifact_entries("ResearchModelShadowDiagnosticV1")[0].metadata["routing"]
        assert diagnostic["reason_code"] == "MODEL_ORIGINAL_DEADLINE_EXPIRED"
        assert diagnostic["status"] == "NOT ESTIMABLE"
        assert not repo.artifact_entries("ResearchModelRequestV1")


def test_wrong_revision_is_not_admitted_as_statistical_input(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        receipt, bars, now = _fixture(repo)
        raw = bars[0].bar.raw
        wrong_raw = RawObservationV2.build(instrument_revision="e" * 64, source_id=raw.source_id,
            event_type=raw.event_type, event_at_ns=raw.event_at_ns, received_at_ns=raw.received_at_ns,
            ingested_at_ns=raw.ingested_at_ns, available_at_ns=raw.available_at_ns,
            translation_version=raw.translation_version, sequence=raw.sequence, payload=b"wrong-revision",
            availability_class=raw.availability_class)
        wrong = replace(bars[0], bar=replace(bars[0].bar, raw=wrong_raw))
        shadow = _shadow(tmp_path, now, lambda *_args, **_kwargs: (wrong, *bars[1:]))
        shadow(receipt, receipt.content_hash, repo)
        diagnostic = repo.artifact_entries("ResearchModelShadowDiagnosticV1")[0].metadata["routing"]
        assert diagnostic["status"] == "NOT ESTIMABLE"
        assert diagnostic["reason_code"] == "MODEL_CAUSAL_SOURCE_IDENTITY_INVALID"
        assert not repo.artifact_entries("ResearchModelRequestV1")


def test_stale_context_cannot_predict_a_horizon_that_already_elapsed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        receipt, bars, now = _fixture(repo)
        shadow = _shadow(tmp_path, now, lambda *_args, **_kwargs: bars[:-1])
        shadow(receipt, receipt.content_hash, repo)
        diagnostic = repo.artifact_entries("ResearchModelShadowDiagnosticV1")[0].metadata["routing"]
        assert diagnostic["status"] == "NOT ESTIMABLE"
        assert diagnostic["reason_code"] == "MODEL_LATEST_CAUSAL_M15_CLOSE_UNAVAILABLE"
        assert not repo.artifact_entries("ResearchModelRequestV1")
