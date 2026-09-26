"""Session-021 research evidence remains outside the frozen S1/S2 selector."""

from __future__ import annotations

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.selection import SELECTION_POLICY_HASH
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY

from .test_session016_candidate_selection import (
    CUTOFF,
    assemble,
    candidate,
    evidence,
    index,
    universe,
)


def test_session021_artifacts_do_not_change_s1_s2_candidateset_bytes_or_selector_hash(tmp_path) -> None:
    snapshot = universe()
    action = candidate()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        index(repository, action)
        rank_ref = evidence(repository, action, snapshot, 1)
        baseline = assemble(repository, snapshot, (action,), {action.candidate_id: (rank_ref,)})
        assert baseline.selection_policy_hash == SELECTION_POLICY_HASH
        assert S1_POLICY.policy_hash == "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"
        assert S2_POLICY.policy_hash == "fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd"
        research_types = (
            "S3MeanReversionStateV2",
            "S3TradeVwapSnapshotV2",
            "S6CrossSectionStateV2",
            "S6HypothesisV2",
            "EventSafetyGateV2",
            "EventReactionArtifactV2",
            "EventAlertV2",
        )
        for artifact_type in research_types:
            ref = sha256_json({"session021_artifact_type": artifact_type})
            repository.register_artifact(ArtifactIndexEntryV2(
                ref, artifact_type, ref, CUTOFF, CUTOFF,
                {"sleeve": artifact_type, "selector_influence": "ZERO"},
            ))
        repeated = assemble(repository, snapshot, (action,), {action.candidate_id: (rank_ref,)})
        assert repeated.to_canonical_json() == baseline.to_canonical_json()
        assert repeated.content_hash == baseline.content_hash
        assert repeated.selection_policy_hash == "36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac"
