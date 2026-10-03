"""Compact component diagnostics preserve missingness, pairing and report scope."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import (
    PIPELINE_STAGE_ORDER,
    OpsDecisionResultV1,
    OpsStageResultV1,
    OpsStageStatusV1,
    OpsSupervisorReceiptV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
    PipelineStageV1,
)
from atlas.v2.science import tuning_export
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from .test_session014_core import KEY

IDENTITY = TuningRunIdentityV1("run-037-analysis", "a" * 64, "b" * 40, 0)


def feature(repo, at, value):
    source_body = {"fixture_source": at}
    source_ref = sha256_json(source_body)
    repo.register_artifact(ArtifactIndexEntryV2(source_ref, "FixtureSourceV1", source_ref, at, at, source_body))
    item = FeatureArtifactV2(ArtifactEnvelope(1, "features-" + str(at), at, at, "fixture", (source_ref,)),
        KEY, "INTRADAY_CORE_V1", at, at,
        {"m15.atr14": FeatureValueV2(value, "price", "WARMUP_MISSING" if value is None else None),
         "regime.trend_state": FeatureValueV2(1, "-1_down_0_mixed_1_up"),
         "regime.event_state": FeatureValueV2(None, "state", "EVENT_CONTEXT_UNAVAILABLE")},
        source_ref, ReplayViewV2.ACTUAL_SYSTEM)
    entry = ArtifactIndexEntryV2(item.content_hash, "FeatureArtifactV2", item.content_hash, at, at,
                                {"feature": item.to_dict()})
    repo.register_artifact(entry)
    return item, entry


def report(tmp_path, rows):
    partition = tmp_path / "analysis.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=tuning_export._schema()), partition)
    return tuning_export._reconciled_report(tmp_path, None, partition, IDENTITY, seconds=10)


def test_feature_projection_and_descriptive_stability_preserve_missing_values(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        feature(repo, 10, Decimal("2"))
        feature(repo, 20, Decimal("4"))
        feature(repo, 30, None)
        exported = export_tuning_snapshot(repo.path, tmp_path / "reports", IDENTITY, cutoff_ns=30)
    result = exported["report"]["recorded_opportunity_reconciliation"]
    assert result["status"] == "TESTED"
    rows = result["descriptive_feature_stability_by_six_hour_bucket"]
    atr = next(row for row in rows if row["feature_id"] == "m15.atr14")
    assert atr["recorded_snapshots"] == 3 and atr["missing_snapshots"] == 1
    assert atr["observed_mean"] == 3 and atr["observed_stddev"] == 1
    assert result["expected_unregistered_origin_count"] is None
    assert result["feature_stability_claim"].startswith("DESCRIPTIVE")
    parquet = pq.read_table(tmp_path / "reports" / IDENTITY.run_id / exported["partition"]).to_pylist()
    missing = next(row for row in parquet if row["decision_at_ns"] == 30)
    assert json.loads(missing["features_json"])["m15.atr14"]["value"] is None
    assert json.loads(missing["regimes_json"])["regime.event_state"]["missing_reason"] == "EVENT_CONTEXT_UNAVAILABLE"


def test_feature_tamper_or_missing_source_is_invalid_not_synthetic_regime(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _item, entry = feature(repo, 10, Decimal("2"))
        body = json.loads(canonical_json(entry.metadata))
        body["feature"]["values"]["regime.trend_state"]["value"] = -1
        with pytest.raises(ValueError):
            tuning_export._validated_row(repo, replace(entry, metadata=body))
        repo._connection.execute("DELETE FROM artifact_index WHERE artifact_type='FixtureSourceV1'")
        with pytest.raises(ValueError, match="reconstructable"):
            tuning_export._validated_row(repo, entry)


def test_feature_projection_rejects_raw_input_after_market_cutoff_before_publication(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        item, entry = feature(repo, 20, Decimal("2"))
        earlier = replace(item, envelope=replace(item.envelope, content_hash=""),
                          information_cutoff_ns=10, confirmed_at_ns=10)
        forged = replace(entry, artifact_ref=earlier.content_hash, content_hash=earlier.content_hash,
                         metadata={"feature": earlier.to_dict()})
        repo.register_artifact(forged)
        with pytest.raises(ValueError, match="causal input"):
            tuning_export._validated_row(repo, forged)


def test_exact_action_disagreement_never_pairs_other_action_or_missing_value(tmp_path):
    rows = []
    for index, (kind, action, value, status) in enumerate((
            ("M0PredictionV2", "exact-A", "2", "AVAILABLE"),
            ("M1PredictionV2", "exact-A", "-1", "AVAILABLE"),
            ("M0PredictionV2", "exact-B", "3", "AVAILABLE"),
            ("M1PredictionV2", "exact-C", "4", "AVAILABLE"),
            ("M0PredictionV2", "exact-D", None, "NOT_ESTIMABLE"),
            ("M1PredictionV2", "exact-D", "5", "AVAILABLE"))):
        rows.append({"row_kind": "ACTION_PREDICTION", "artifact_type": kind,
                     "artifact_ref": str(index), "action_artifact_ref": action, "action_hash": action,
                     "expected_net_value": value, "status": status, "available_at_ns": 10,
                     "method_id": kind, "method_config_hash": kind, "policy_hash": "policy",
                     "instrument_key_json": "instrument-A", "metrics_json": "{}"})
    result = report(tmp_path, rows)
    groups = result["m0_m1_exact_action_disagreement"]
    assert sum(row["exact_action_rows"] for row in groups) == 4
    paired = next(row for row in groups if row["paired_estimates"] == 1)
    assert paired["mean_absolute_disagreement"] == 3 and paired["opposed_sign_estimates"] == 1
    assert all(row["mean_absolute_disagreement"] is None for row in groups if row["paired_estimates"] == 0)
    assert "REQUIRE_DEPENDENCE_AWARE" in result["independence"]
    assert result["economic_significance"] == "NOT ESTIMABLE"


def test_stage_checkpoint_projection_and_latency_keep_unknown_compute_time(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        event_id = "c" * 64
        stage = OpsStageResultV1(PipelineStageV1.UNIVERSE, OpsStageStatusV1.COMPLETE, (), 100)
        entry = ArtifactIndexEntryV2(OpsSupervisorV2._checkpoint_ref(event_id, stage.stage),
            "OpsSupervisorStageCheckpointV1", stage.content_hash, 100, 100,
            {"event_id": event_id, "stage_result": stage.to_dict()})
        repo.register_artifact(entry)
        valid = tuning_export._validated_row(repo, entry)
        with pytest.raises(ValueError, match="identity"):
            tuning_export._validated_row(repo, replace(entry, artifact_ref="d" * 64))
    following = dict(valid, source_stage="CAUSAL_FEATURES", stage_order=1, stage_completed_at_ns=160)
    receipt = {"row_kind": "PIPELINE_RECEIPT", "event_id": event_id,
               "source_event_at_ns": 50, "received_at_ns": 60, "information_cutoff_ns": 70,
               "available_at_ns": 180, "status": "NO_CANDIDATE"}
    result = report(tmp_path, [valid, following, receipt])
    first = next(row for row in result["pipeline_stage_completion_latency"] if row["source_stage"] == "UNIVERSE")
    second = next(row for row in result["pipeline_stage_completion_latency"] if row["source_stage"] == "CAUSAL_FEATURES")
    assert first["mean_completion_interval_ns"] is None
    assert second["mean_completion_interval_ns"] == 60 and second["mean_cutoff_to_completion_ns"] == 90
    assert result["pipeline_receipt_latency"][0]["mean_source_to_receipt_ns"] == 10
    assert "NOT_ISOLATED_COMPUTE" in result["stage_latency_definition"]


def test_registered_route_without_recorded_output_stays_visible(tmp_path):
    registered = [{"route_ref": "route-A", "provider_key": "fixed-A", "manifest_hash": "manifest-A"},
                  {"route_ref": "route-B", "provider_key": "fixed-B", "manifest_hash": "manifest-B"}]
    rows = [{"row_kind": "MODEL_REGISTRY", "registered_routes_json": canonical_json(registered)},
            {"row_kind": "MODEL_REQUEST", "route_ref": "route-A"},
            {"row_kind": "MODEL_TERMINAL", "route_ref": "route-A"}]
    result = report(tmp_path, rows)
    absent = next(row for row in result["registered_model_route_coverage"] if row["route_ref"] == "route-B")
    assert absent["request_rows"] == absent["terminal_rows"] == absent["matured_labels"] == 0


def test_real_supervisor_receipt_uses_wrapped_identity_and_recorded_times(tmp_path):
    from .test_session027_ops_supervisor import make_event

    event = make_event()
    completed_at = event.information_cutoff_ns + 100
    stages = tuple(OpsStageResultV1(stage, OpsStageStatusV1.SKIPPED, (), completed_at)
                   for stage in PIPELINE_STAGE_ORDER)
    receipt = OpsSupervisorReceiptV1("fixture-runtime", "e" * 64, event, "HEALTHY_CURRENT", (),
        OpsDecisionResultV1(stages, OpsTerminalStatusV1.NO_CANDIDATE), completed_at)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(ArtifactIndexEntryV2(event.content_hash, "OpsDecisionEventSourceV1",
            event.content_hash, event.information_cutoff_ns, event.information_cutoff_ns,
            {"event": event.to_dict()}))
        receipt_ref = OpsSupervisorV2._persist_final_receipt(repo, receipt)
        entry = repo.get_artifact(receipt_ref)
        assert entry is not None and entry.artifact_ref != entry.content_hash
        row = tuning_export._validated_row(repo, entry)
        assert row["row_kind"] == "PIPELINE_RECEIPT"
        assert row["information_cutoff_ns"] == event.information_cutoff_ns
        with pytest.raises(ValueError, match="identity"):
            tuning_export._validated_row(repo, replace(entry, artifact_ref=entry.content_hash))
    result = report(tmp_path, [row])
    assert result["pipeline_receipt_latency"][0]["mean_cutoff_to_terminal_ns"] == 100


def test_real_action_predictions_preserve_registered_configuration_and_exact_dependencies(tmp_path):
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.m0 import fit_m0
    from atlas.v2.science.m1 import M1_POLICY_HASH, fit_m1
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .session023_support import feature_candidate
    from .test_session017_risk import CUTOFF, risk_case, size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, candidate_factory=lambda _repo, _universe, _product: feature_candidate(repo))
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        _, m0, *_ = fit_m0(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1)
        m1 = fit_m1(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 2, dependency_lock_hash=sha256_json("fixture-lock"))
        rows = [tuning_export._validated_row(repo, repo.get_artifact(item.content_hash))
                for item in (m0, m1.prediction)]
        assert all(row["action_artifact_ref"] == action.content_hash for row in rows)
        assert rows[1]["method_config_hash"] != M1_POLICY_HASH
        assert all(row["status"] == "NOT_ESTIMABLE" and row["expected_net_value"] is None for row in rows)
        entry = repo.get_artifact(m0.content_hash)
        malformed = m0.to_dict() | {"support_ref": action.content_hash}
        ref = sha256_json(malformed)
        with pytest.raises(ValueError, match="dependency"):
            tuning_export._validated_row(repo, replace(entry, artifact_ref=ref, content_hash=ref,
                metadata={"prediction": malformed}))
    result = report(tmp_path, rows)
    assert result["m0_m1_exact_action_disagreement"][0]["paired_estimates"] == 0
    assert len(result["registered_action_method_comparisons"]) == 2


def test_stage_intervals_require_recorded_adjacent_stage_completions(tmp_path):
    rows = [{"row_kind": "PIPELINE_STAGE", "event_id": "event-A", "source_stage": "UNIVERSE",
             "stage_order": 0, "stage_completed_at_ns": 100, "status": "COMPLETE"},
            {"row_kind": "PIPELINE_STAGE", "event_id": "event-A", "source_stage": "CANDIDATE_SET",
             "stage_order": 2, "stage_completed_at_ns": 160, "status": "COMPLETE"}]
    result = report(tmp_path, rows)
    second = next(row for row in result["pipeline_stage_completion_latency"]
                  if row["source_stage"] == "CANDIDATE_SET")
    assert second["consecutive_completion_pairs"] == 0
    assert second["mean_completion_interval_ns"] is None
    assert second["missing_predecessor_checkpoints"] == 1


def test_bounded_recent_report_keeps_useful_window_and_marks_omitted_history(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        feature(repo, 10, Decimal("2"))
        first = export_tuning_snapshot(repo.path, tmp_path / "reports", IDENTITY, cutoff_ns=10)
        feature(repo, 20, Decimal("4"))
        monkeypatch.setattr(tuning_export, "MAX_ANALYSIS_PARTITIONS", 1)
        second = export_tuning_snapshot(repo.path, tmp_path / "reports", IDENTITY, cutoff_ns=20)
    result = second["report"]["recorded_opportunity_reconciliation"]
    assert first["report"]["recorded_opportunity_reconciliation"]["history_omitted"] is False
    assert second["report"]["status"] == result["status"] == "TEST GATE"
    assert result["scope"] == "VALIDATED_RECENT_PARTITION_WINDOW" and result["history_omitted"] is True
    assert result["partition_count"] == 1
    assert result["window_source_rowid_from"] > first["through_rowid"]
    assert result["descriptive_feature_stability_by_six_hour_bucket"]


@pytest.mark.parametrize("depth,status,net", [("100", "FULL_FILL", "239.855"), ("0", "NO_FILL", "0")])
def test_exact_lifecycle_economics_duration_margin_roi_and_restart(tmp_path, depth, status, net):
    from atlas.v2.runtime.action_outcome_producer import (
        RetrospectiveActionOutcomeProducerV1,
        index_action_replay_source_evidence,
    )

    from .test_session037_action_outcome_producer import _fixture

    db = tmp_path / "ops.sqlite"
    with OpsRepository(db) as repo:
        _case, calendar, evidence = _fixture(repo, depth=depth)
        index_action_replay_source_evidence(repo, evidence)
        RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 1)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        summary = repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0]
        row = tuning_export._validated_row(repo, summary)
        assert row["row_kind"] == "ACTION_LIFECYCLE" and row["status"] == status
        assert row["net_payoff"] == net
        assert row["funding_cashflow"] == "0"
        assert Decimal(row["frozen_sizing_margin"]) > 0
        assert Decimal(row["net_margin_roi"]) == Decimal(net) / Decimal(row["frozen_sizing_margin"])
        assert row["entry_to_exit_duration_ns"] == (4 * 3_600_000_000_000 if depth == "100" else None)
        assert Decimal(row["fees"]) == (Decimal("5.145") if depth == "100" else Decimal(0))
        first = export_tuning_snapshot(repo.path, tmp_path / "exports", IDENTITY, cutoff_ns=summary.available_at_ns)
        result = first["report"]["recorded_opportunity_reconciliation"]
        assert result["action_replay_lifecycle_diagnostics"][0]["resolved_payoffs"] == 1
        assert "NO_LEVERAGE_RULE" in result["action_margin_roi_scope"]
        altered = json.loads(canonical_json(summary.metadata))
        altered["summary"]["exit_reason"] = "FORGED"
        with pytest.raises(ValueError, match="identity"):
            tuning_export._validated_row(repo, replace(summary, metadata=altered))
        with pytest.raises(ValueError, match="locator"):
            tuning_export._validated_row(repo, replace(summary, artifact_ref=sha256_json("wrong-locator")))
        with pytest.raises(ValueError, match="publication"):
            tuning_export._validated_row(repo, replace(summary, available_at_ns=summary.available_at_ns + 1))
    with OpsRepository(db) as repo:
        assert tuning_export._validated_row(repo, repo.get_artifact(summary.artifact_ref)) == row
        restarted = export_tuning_snapshot(repo.path, tmp_path / "exports", IDENTITY, cutoff_ns=summary.available_at_ns)
        assert restarted["through_rowid"] == first["through_rowid"]


def test_lifecycle_refuses_payoff_link_tamper_with_self_consistent_summary_hash(tmp_path):
    from atlas.v2.runtime.action_outcome_producer import (
        RetrospectiveActionOutcomeProducerV1,
        index_action_replay_source_evidence,
    )

    from .test_session037_action_outcome_producer import _fixture

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _case, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 1)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        summary = repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0]
        altered = json.loads(canonical_json(summary.metadata))
        altered["summary"]["filled_quantity"] = "1234"
        entry = replace(summary, content_hash=sha256_json(altered["summary"]), metadata=altered)
        with pytest.raises(ValueError, match="fill summary"):
            tuning_export._validated_row(repo, entry)


def test_unsupported_lifecycle_never_fills_missing_economics_with_zero(tmp_path):
    from atlas.v2.runtime.action_outcome_producer import (
        RetrospectiveActionOutcomeProducerV1,
        index_action_replay_source_evidence,
    )

    from .test_session037_action_outcome_producer import _fixture

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _case, calendar, evidence = _fixture(repo, depth=None)
        index_action_replay_source_evidence(repo, evidence)
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 1)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "NOT_ESTIMABLE"
        row = tuning_export._validated_row(repo, repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0])
        assert row["net_payoff"] is row["fees"] is row["funding_cashflow"] is row["net_margin_roi"] is None
        assert row["entry_to_exit_duration_ns"] is None
        assert Decimal(row["frozen_sizing_margin"]) > 0


def test_computation_projection_exact_publication_latency_and_tamper(tmp_path):
    from atlas.v2.chronology import record_computation

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        raw_ref = sha256_json("raw")
        output_ref = sha256_json("output")
        repo.register_artifacts((ArtifactIndexEntryV2(raw_ref, "RawV1", raw_ref, 10, 10, {}),
            ArtifactIndexEntryV2(output_ref, "ActionArtifactV2", output_ref, 15, 15, {"input_refs": [raw_ref]})))
        receipt_ref = record_computation(repo, artifact_ref=output_ref, information_cutoff_ns=10,
            started_ns=11, finished_ns=14, available_ns=15, input_refs=(raw_ref,), deadline_ns=20)
        receipt = repo.get_artifact(receipt_ref)
        row = tuning_export._validated_row(repo, receipt)
        assert row["information_cutoff_ns"] == 10 and row["available_at_ns"] == 15
        assert row["computation_started_ns"] == 11 and row["computation_finished_ns"] == 14
        assert row["computation_duration_ns"] == 3 and row["publication_latency_ns"] == 5
        assert row["source_stage"] == "ActionArtifactV2"
        feature_row = dict(row, artifact_ref="feature-computation", source_stage="FeatureArtifactV2",
            computation_duration_ns=8)
        groups = report(tmp_path, [row, feature_row])["derived_computation_publication_latency"]
        assert {item["source_stage"] for item in groups} == {"ActionArtifactV2", "FeatureArtifactV2"}
        assert {item["mean_computation_duration_ns"] for item in groups} == {3, 8}
        altered = json.loads(canonical_json(receipt.metadata))
        altered["chronology"]["artifact_content_hash"] = sha256_json("forged")
        with pytest.raises(ValueError, match="artifact"):
            tuning_export._validated_row(repo, replace(receipt,
                content_hash=sha256_json(altered["chronology"]), metadata=altered))
        repo._connection.execute("DELETE FROM artifact_index WHERE artifact_ref=?", (raw_ref,))
        with pytest.raises(ValueError, match="dependency"):
            tuning_export._validated_row(repo, receipt)


def test_prerequisite_projection_keeps_unavailable_facts_and_real_latency(tmp_path):
    from atlas.v2.runtime.research_prerequisites import publish_research_prerequisites

    from .test_session017_risk import risk_case
    from .test_session037_research_prerequisites import _clock, _event

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        event = _event(case.candidate.decision_at_ns)
        publication = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=_clock(event.information_cutoff_ns))
        entry = repo.get_artifact(publication.inventory_ref)
        row = tuning_export._validated_row(repo, entry)
        assert row["row_kind"] == "PREREQUISITES" and row["status"] == "NOT_ESTIMABLE"
        assert row["information_cutoff_ns"] == event.information_cutoff_ns
        assert row["publication_latency_ns"] == 30 and row["computation_duration_ns"] == 10
        assert row["net_payoff"] is None and row["net_margin_roi"] is None
        assert "AUTHENTICATED_ACCOUNT_EVIDENCE_UNAVAILABLE_IN_PUBLIC_SHADOW" in row["reason_codes"]
        exported = export_tuning_snapshot(repo.path, tmp_path / "exports", IDENTITY, cutoff_ns=entry.available_at_ns)
        report_body = exported["report"]["recorded_opportunity_reconciliation"]
        assert report_body["research_prerequisite_missingness"][0]["inventory_rows"] == 1
        assert report_body["derived_computation_publication_latency"][0]["mean_publication_latency_ns"] == 30


@pytest.mark.parametrize("kind", ["OpsActiveWorkPressureV1", "ActiveTrainingWorkPressureV1"])
def test_pressure_export_reports_bound_and_explicit_missingness(tmp_path, kind):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = {"version": kind, "authority": "ZERO", "reason": "RAW_POPULATION_OVERFLOW",
            "limit": 4096, "observed_count": 4097, "has_more": True, "invalid_entry_count": 0}
        ref = sha256_json(body)
        entry = ArtifactIndexEntryV2(ref, kind, ref, 20, 20, {"pressure": body})
        repo.register_artifact(entry)
        row = tuning_export._validated_row(repo, entry)
        assert row["row_kind"] == "PRESSURE"
        assert json.loads(row["metrics_json"])["observed_count"] == 4097
        assert "RAW_POPULATION_OVERFLOW" in row["reason_codes"]
        assert row["net_payoff"] is None and row["net_margin_roi"] is None
        exported = export_tuning_snapshot(repo.path, tmp_path / "reports", IDENTITY, cutoff_ns=20)
        assert exported["report"]["recorded_opportunity_reconciliation"]["active_work_pressure"][0]["pressure_rows"] == 1


def test_ready_history_pressure_preserves_exact_frame_and_prefix_counts(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = {"version": "OpsActiveWorkPressureV1", "lane": "ACTIVE_HISTORY",
            "interval": "1H", "instrument_key_json": KEY.to_canonical_json(),
            "information_cutoff_ns": 20, "ready": True, "reason_code": "EXACT_PREFIX_AVAILABLE",
            "processed_bar_count": 1200, "max_rows_per_cycle": 128, "authority": "ZERO"}
        ref = sha256_json(body)
        entry = ArtifactIndexEntryV2(ref, body["version"], ref, 20, 20, {"pressure": body})
        repo.register_artifact(entry)
        row = tuning_export._validated_row(repo, entry)
        assert row["source_stage"] == "ACTIVE_HISTORY:1H" and row["status"] == "AVAILABLE"
        assert row["instrument_key_json"] == KEY.to_canonical_json()
        assert json.loads(row["metrics_json"])["processed_bar_count"] == 1200
        assert row["net_payoff"] is None


@pytest.mark.parametrize("kind", ["NewsEventV2", "PublicContextCycleReportV1"])
def test_public_context_compact_extraction_omits_raw_text_and_urls(tmp_path, kind):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        raw_ref = sha256_json("retained-raw")
        body = {"event_id": "event-037-news", "confidence": "0.75", "queue_items": 2,
            "reason": "EXTRACTION_UNAVAILABLE", "raw_ref": raw_ref,
            "title": "Sensitive retained headline", "supporting_spans": ["Retained article text"],
            "source_url": "https://official.example/retained", "raw_text": "Full retained feed text"}
        ref = sha256_json(body)
        entry = ArtifactIndexEntryV2(ref, kind, ref, 20, 20, {"event" if kind == "NewsEventV2" else "report": body})
        repo.register_artifact(entry)
        row = tuning_export._validated_row(repo, entry)
        metrics = json.loads(row["metrics_json"])
        assert any(key.endswith("confidence") and value == "0.75" for key, value in metrics.items())
        assert raw_ref in row["evidence_refs"]
        wire = canonical_json(row)
        assert all(value not in wire for value in ("Sensitive retained headline", "Retained article text",
            "https://official.example/retained", "Full retained feed text"))
        exported = export_tuning_snapshot(repo.path, tmp_path / "reports", IDENTITY, cutoff_ns=20)
        rows = pq.read_table(tmp_path / "reports" / IDENTITY.run_id / exported["partition"]).to_pylist()
        assert len(rows) == 1 and rows[0]["artifact_type"] == kind
