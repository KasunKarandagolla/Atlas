"""Interrupted payoff publication recovers a causally valid lifecycle receipt."""

import json

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.action_outcome_producer import (
    RetrospectiveActionOutcomeProducerV1,
    _inputs,
    _validate_source,
    index_action_replay_source_evidence,
)
from atlas.v2.science.replay import replay_action
from atlas.v2.science.tuning_export import _validated_row

from .test_session037_action_outcome_producer import _fixture


def _interrupted_payoff(repo):
    _, calendar, evidence = _fixture(repo)
    index_action_replay_source_evidence(repo, evidence)
    indexed = repo.get_artifact(calendar.content_hash)
    path = _validate_source(repo, evidence, indexed, evidence.available_at_ns)
    inputs = _inputs(repo, evidence, evidence.available_at_ns, path)
    payoff = replay_action(repo, **inputs, replay_cutoff_ns=evidence.available_at_ns,
        production_clock_ns=lambda: evidence.available_at_ns + 1)
    assert repo.artifact_entries("ActionReplayLifecycleSummaryV1") == ()
    return calendar, evidence, payoff


def test_interrupted_payoff_recovers_exportable_lifecycle_after_restart(tmp_path):
    db = tmp_path / "ops.sqlite"
    with OpsRepository(db) as repo:
        calendar, evidence, payoff = _interrupted_payoff(repo)
    later_cutoff = evidence.available_at_ns + 100
    with OpsRepository(db) as repo:
        producer = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: later_cutoff + 1)
        result = producer(repo, repo.get_artifact(calendar.content_hash), later_cutoff)
        assert result.status == "EXISTING" and result.payoff_ref == payoff.content_hash
        summary = repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0]
        body = summary.metadata["summary"]
        assert body["evidence_cutoff_ns"] == evidence.available_at_ns
        assert body["payoff_available_at_ns"] == payoff.available_at_ns < later_cutoff
        assert summary.available_at_ns == later_cutoff + 1
        row = _validated_row(repo, summary)
        assert row["row_kind"] == "ACTION_LIFECYCLE"
        assert row["net_payoff"] == "239.855"
        assert row["provenance"] == "SIMULATED"
        repeated = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: later_cutoff + 20)(
            repo, repo.get_artifact(calendar.content_hash), later_cutoff + 10)
        assert repeated.status == "EXISTING" and repeated.payoff_ref == payoff.content_hash
        assert repo.artifact_entries("ActionReplayLifecycleSummaryV1") == (summary,)


@pytest.mark.parametrize("repair_hash", [False, True])
def test_recovered_lifecycle_tamper_is_rejected_by_export_and_producer(tmp_path, repair_hash):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        calendar, evidence, _ = _interrupted_payoff(repo)
        later_cutoff = evidence.available_at_ns + 100
        producer = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: later_cutoff + 1)
        assert producer(repo, repo.get_artifact(calendar.content_hash), later_cutoff).status == "EXISTING"
        summary = repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0]
        repo._connection.execute("UPDATE artifact_index SET metadata_json=json_set(metadata_json, "
            "'$.summary.evidence_cutoff_ns', ?) WHERE artifact_ref=?", (later_cutoff, summary.artifact_ref))
        if repair_hash:
            metadata = json.loads(repo._connection.execute("SELECT metadata_json FROM artifact_index "
                "WHERE artifact_ref=?", (summary.artifact_ref,)).fetchone()[0])
            repo._connection.execute("UPDATE artifact_index SET content_hash=? WHERE artifact_ref=?",
                (sha256_json(metadata["summary"]), summary.artifact_ref))
        with pytest.raises(ValueError, match="identity, locator or publication mismatch"):
            _validated_row(repo, repo.get_artifact(summary.artifact_ref))
        assert producer(repo, repo.get_artifact(calendar.content_hash), later_cutoff).reason_code == "REPLAY_EVIDENCE_INVALID"
