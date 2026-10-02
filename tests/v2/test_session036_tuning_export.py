"""Read-only incremental analysis must preserve denominators and exact identities."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.agent_intelligence.shadow_measurement import ActionCriticShadowObservationV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.science import tuning_export
from atlas.v2.science.outcomes import index_matured_outcome
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from . import test_session018_remediation as s18
from . import test_session034_scientific_calendar_closure as s34

IDENTITY = TuningRunIdentityV1("run-036", "a" * 64, "b" * 40, 0)


def _sample(repo: OpsRepository, at: int, *, label: str = "NORMAL") -> str:
    body = {"status": label, "rss_bytes": 123456, "wal_bytes": 100, "cpu_percent": 2.5,
            "secret": "never-export-me", "prompt": "private-provider-prompt"}
    ref = sha256_json({"at": at, "body": body})
    repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchResourceSampleV1", ref, at, at, body))
    return ref


def _rows(root: Path, manifest: dict):
    return pq.read_table(root / IDENTITY.run_id / manifest["partition"]).to_pylist()


def test_native_calendar_and_late_missingness_keep_separate_denominators(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        product, bar, event, _refs, now, port = s34._native_fixture(repo)
        s34._process(repo, event, now, port)
        later_close = bar.close_at_ns + s34.MINUTE_NS
        late_raw = replace(bar.raw, event_at_ns=later_close, received_at_ns=later_close + 6 * s34.NS,
                           ingested_at_ns=later_close + 6 * s34.NS, available_at_ns=later_close + 6 * s34.NS)
        late_bar = replace(bar, raw=late_raw, open_at_ns=later_close - s34.MINUTE_NS, close_at_ns=later_close)
        production._persist_late_s3_m1_origin_gate(repo, product, late_bar,
                                                 observed_at_ns=later_close + 6 * s34.NS)
        count_before = repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY,
                                         cutoff_ns=later_close + 7 * s34.NS, batch_size=2)
        assert repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == count_before
    # The inherited S34 fixture deliberately uses a minimal fake health body.
    # It must remain visible as invalid rather than acquiring fabricated health.
    assert manifest["validation_failures"] == {"PublicSourceHealthV2": 1}
    assert manifest["report"]["calendar_rows"] == 1
    assert manifest["report"]["missing_origin_rows"] == 1
    assert manifest["report"]["outcome_rows"] == 0
    rows = _rows(tmp_path / "reports", manifest)
    decision = next(row for row in rows if row["row_kind"] == "DECISION")
    assert decision["selection_state"] == "NOT_ESTIMABLE" and decision["candidate_ref"] is None
    missing = next(row for row in rows if row["row_kind"] == "MISSINGNESS")
    assert missing["status"] == "TEST GATE" and missing["net_payoff"] is None
    assert missing["origin_ref"] and missing["evidence_refs"]


def test_incremental_bounded_export_and_retry_preserve_exact_rows_and_secrets(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        first = _sample(repo, 10)
        second = _sample(repo, 20)
        initial = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100, max_rows=1)
        assert initial["has_more"] is True and initial["report"]["status"] == "TEST GATE"
        following = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100, max_rows=1)
        retry = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100, max_rows=1)
    assert retry == following
    assert following["has_more"] is False
    assert following["report"]["recorded_resource_and_latency_summaries"][
        "ResearchResourceSampleV1:rss_bytes"]["count"] == 2
    rows = _rows(tmp_path / "reports", initial) + _rows(tmp_path / "reports", following)
    assert [row["artifact_ref"] for row in rows] == [first, second]
    assert "never-export-me" not in canonical_json(rows)
    assert "private-provider-prompt" not in canonical_json(rows)
    assert all(row["config_hash"] == IDENTITY.config_hash for row in rows)


def test_snapshot_is_consistent_and_late_insert_with_old_timestamp_is_exported_next(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    reports = tmp_path / "reports"
    with OpsRepository(path) as writer:
        _sample(writer, 10)
        original = tuning_export._validated_row
        inserted = False

        def insert_during_read(repository, entry):
            nonlocal inserted
            if not inserted:
                _sample(writer, 5, label="LATE_RECEIPT")
                inserted = True
            return original(repository, entry)

        monkeypatch.setattr(tuning_export, "_validated_row", insert_during_read)
        first = export_tuning_snapshot(path, reports, IDENTITY, cutoff_ns=100)
        second = export_tuning_snapshot(path, reports, IDENTITY, cutoff_ns=100)
    assert first["rows_written"] == second["rows_written"] == 1
    assert second["after_rowid"] == first["through_rowid"]
    assert _rows(reports, second)[0]["available_at_ns"] == 5


def test_future_row_cannot_move_cursor_past_unseen_evidence(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _sample(repo, 200)
        _sample(repo, 10)
        blocked = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100)
        complete = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=201)
    assert blocked["through_rowid"] == 0 and blocked["blocked_future_evidence"] is True
    assert complete["rows_written"] == 2 and complete["blocked_future_evidence"] is False


def test_future_insert_after_complete_report_cannot_reuse_complete_status(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _sample(repo, 10)
        initial = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100)
        _sample(repo, 200)
        blocked = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100)
        retry = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=100)
    assert initial["report"]["status"] == "TESTED"
    assert blocked["through_rowid"] == initial["through_rowid"]
    assert blocked["blocked_future_evidence"] is True
    assert blocked["report"]["status"] == "TEST GATE"
    assert retry == blocked


def test_product_telemetry_keeps_recorded_units_without_invented_utilization(tmp_path):
    path = tmp_path / "ops.sqlite"
    telemetry = {"schema_version": 1, "run_id": IDENTITY.run_id, "config_hash": IDENTITY.config_hash,
                 "epoch_id": "epoch-036", "available_at_ns": 10, "authority": "ZERO",
                 "source_health": "HEALTHY",
                 "cpu_seconds": 3.5, "threads": 7, "disk_free_bytes": 100000,
                 "rss_bytes": 20000, "handles": 31, "peak_rss_bytes": None,
                 "db_bytes": 1024, "wal_bytes": 2048}
    ref = sha256_json(telemetry)
    with OpsRepository(path) as repo:
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchRunTelemetryV1", ref, 10, 10,
                                                   {"telemetry": telemetry}))
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=10)
    row = _rows(tmp_path / "reports", manifest)[0]
    metrics = json.loads(row["metrics_json"])
    for name in ("cpu_seconds", "threads", "disk_free_bytes", "rss_bytes", "handles", "db_bytes", "wal_bytes"):
        assert metrics["telemetry." + name] == telemetry[name]
    assert not any("cpu_percent" in name for name in metrics)
    assert row["epoch_id"] == "epoch-036" and "HEALTHY" in row["reason_codes"]
    assert manifest["report"]["opportunity_denominator"] == "NOT_ESTIMABLE_UNTIL_ORIGIN_AND_STAGE_RECONCILIATION"


def test_exact_outcome_join_is_validated_and_later_maturity_is_incremental(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _case, _action, _payoff, outcome = s18._payoff_case(repo)
        earlier = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=outcome.available_at_ns)
        index_matured_outcome(repo, outcome)
        later = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=outcome.available_at_ns)
    assert earlier["report"]["outcome_rows"] == 0
    assert later["report"]["outcome_rows"] == 1 and later["validation_failures"] == {}
    row = next(row for row in _rows(tmp_path / "reports", later) if row["row_kind"] == "OUTCOME")
    assert row["decision_ref"] == outcome.decision_ref and row["action_hash"] == outcome.action_hash
    assert row["provenance"] == "SIMULATED" and row["net_payoff"] == outcome.to_dict()["net_payoff"]


def test_wrong_action_outcome_is_missingness_and_never_a_payoff(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _case, _action, _payoff, outcome = s18._payoff_case(repo)
        wrong = replace(outcome, action_hash="d" * 64)
        repo.register_artifact(ArtifactIndexEntryV2(wrong.content_hash, "MaturedOutcomeV2", wrong.content_hash,
                                                   wrong.matured_at_ns, wrong.available_at_ns,
                                                   {"outcome": wrong.to_dict()}))
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=wrong.available_at_ns)
    assert manifest["validation_failures"] == {"MaturedOutcomeV2": 1}
    assert manifest["report"]["outcome_rows"] == 0 and manifest["report"]["invalid_rows"] == 1
    row = next(row for row in _rows(tmp_path / "reports", manifest) if row["row_kind"] == "INVALID")
    assert row["net_payoff"] is None


def test_critic_matching_action_without_exact_request_chain_is_invalid(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _case, _action, _payoff, outcome = s18._payoff_case(repo)
        observation = ActionCriticShadowObservationV1(
            originating_receipt_ref="c" * 64, decision_calendar_ref=outcome.decision_ref,
            candidate_set_ref=outcome.candidate_set_ref, packet_ref="d" * 64, packet_hash="e" * 64,
            request_id="request-036", request_ref="f" * 64, request_hash="f" * 64,
            action_artifact_ref=outcome.action_artifact_ref, action_hash=outcome.action_hash,
            critic_terminal_status="UNAVAILABLE", terminal_reason_code="PROVIDER_UNAVAILABLE",
            accepted_shadow_evidence=False, finding_types=(), dispatch_authorized_at_ns=None,
            result_received_at_ns=None, dispatch_to_result_latency_ns=None,
            provider_profile_hash="1" * 64, model_profile_hash="2" * 64,
            decision_influence=False, admission_influence=False, deterministic_terminal_status="COMPLETE",
            deterministic_admission_status=outcome.admission_state.value, recorded_at_ns=outcome.available_at_ns,
        )
        repo.register_artifact(ArtifactIndexEntryV2(
            observation.content_hash, observation.VERSION, observation.content_hash,
            observation.recorded_at_ns, observation.recorded_at_ns, {"observation": observation.to_dict()}))
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=outcome.available_at_ns)
    assert manifest["validation_failures"] == {"ActionCriticShadowObservationV1": 1}
    assert manifest["report"]["intelligence_rows"] == 0
    row = next(row for row in _rows(tmp_path / "reports", manifest)
               if row["artifact_type"] == observation.VERSION)
    assert row["row_kind"] == "INVALID" and row["action_hash"] is None


def test_identity_drift_and_corrupt_checkpoint_fail_closed(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _sample(repo, 1)
    reports = tmp_path / "reports"
    export_tuning_snapshot(path, reports, IDENTITY, cutoff_ns=10)
    with pytest.raises(ValueError, match="identity conflicts"):
        export_tuning_snapshot(path, reports, replace(IDENTITY, config_hash="c" * 64), cutoff_ns=10)
    head = reports / IDENTITY.run_id / "head.json"
    manifest_sha = json.loads(head.read_text())["manifest_sha256"]
    manifest_path = reports / IDENTITY.run_id / "manifests" / f"{manifest_sha}.json"
    data = json.loads(manifest_path.read_text())
    data["through_rowid"] = 999
    manifest_path.write_text(canonical_json(data))
    with pytest.raises(ValueError, match="checkpoint hash"):
        export_tuning_snapshot(path, reports, IDENTITY, cutoff_ns=11)


def test_snapshot_timeout_does_not_advance_manifest(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _sample(repo, 1)
    times = iter((0.0, 2.0))
    monkeypatch.setattr(tuning_export.time, "monotonic", lambda: next(times))
    with pytest.raises(tuning_export.TuningExportBudgetExceeded):
        export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=10, max_snapshot_seconds=1)
    assert not (tmp_path / "reports" / IDENTITY.run_id / "head.json").exists()
