"""Later computation cannot widen the immutable market information cutoff."""
from dataclasses import replace

import pytest

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.chronology import VERSION, causal_artifact, chronology_ref, record_computation
from atlas.v2.contracts import FeatureArtifactV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import SizingStatus, size_selected_candidate
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.runtime.production import (
    ProductionEventInputsV1,
    _load_public_composition,
    _seal_public_composition,
)
from atlas.v2.science.action import freeze_action
from atlas.v2.science.m0 import action_features
from atlas.v2.science.research_selection import (
    assemble_multisleeve_research_candidate_set,
    research_selection_universe,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .session023_support import feature_candidate
from .test_session014_core import KEY, bar
from .test_session016_candidate_selection import evidence, index
from .test_session017_risk import CUTOFF, risk_case, source


class Clock:
    def __init__(self, at=CUTOFF):
        self.at = at

    def __call__(self):
        self.at += 1
        return self.at


def _size(repo, case, clock):
    return size_selected_candidate(repo, candidate_set=case.candidate_set, candidate=case.candidate,
        universe=case.universe, policy=S1_POLICY, product=case.product, v1=case.v1, v2=case.v2,
        account=case.account, exposures=case.exposures, outcomes=case.outcomes, venue=case.venue,
        stress=case.stress, fee=case.fee, cutoff_ns=CUTOFF, clock_ns=clock)


def _action(repo, case, sizing, clock):
    return freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
        sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2, clock_ns=clock)


def _delayed_case(repo):
    case = risk_case(repo, candidate_factory=lambda r, _u, _p: feature_candidate(r))
    original = FeatureArtifactV2.from_dict(json_value(repo.get_artifact(case.candidate.snapshot_hash).metadata["feature"]))
    feature = replace(original, envelope=replace(original.envelope, content_hash="",
        created_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3))
    repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2", feature.content_hash,
        feature.envelope.created_at_ns, feature.envelope.available_at_ns, {"feature": feature.to_dict()}))
    record_computation(repo, artifact_ref=feature.content_hash, information_cutoff_ns=CUTOFF,
        started_ns=CUTOFF + 1, finished_ns=CUTOFF + 2, available_ns=CUTOFF + 3,
        input_refs=feature.envelope.input_refs, deadline_ns=case.candidate.deadline_ns)
    case.universe = research_selection_universe(case.universe)
    repo.register_artifact(ArtifactIndexEntryV2(case.universe.content_hash, "UniverseContractV2",
        case.universe.content_hash, CUTOFF, CUTOFF, {"universe": case.universe.to_dict()}))
    case.candidate = replace(case.candidate, snapshot_hash=feature.content_hash,
        envelope=replace(case.candidate.envelope, content_hash="", created_at_ns=CUTOFF + 5,
            available_at_ns=CUTOFF + 6, input_refs=(feature.content_hash,)))
    index(repo, case.candidate, universe_ref=case.universe.content_hash)
    record_computation(repo, artifact_ref=case.candidate.content_hash, information_cutoff_ns=CUTOFF,
        started_ns=CUTOFF + 4, finished_ns=CUTOFF + 5, available_ns=CUTOFF + 6,
        input_refs=(feature.content_hash, case.universe.content_hash), deadline_ns=case.candidate.deadline_ns)
    event = "session037-chronology-selection"
    rank = evidence(repo, case.candidate, case.universe, 1, event=event)
    clock = Clock(CUTOFF + 6)
    case.candidate_set = assemble_multisleeve_research_candidate_set(repo, universe=case.universe,
        decision_event_id=event, cutoff_ns=CUTOFF, candidates=(case.candidate,),
        policies={S1_POLICY.policy_hash: S1_POLICY}, scanner_evidence_refs={case.candidate.candidate_id: (rank,)},
        clock_ns=clock, deadline_ns=case.candidate.deadline_ns)
    return case, feature, clock


def test_delayed_feature_selection_sizing_action_keep_original_m0_cutoff(tmp_path):
    with OpsRepository(tmp_path / "delayed.sqlite") as repo:
        case, feature, clock = _delayed_case(repo)
        sizing = _size(repo, case, clock)
        assert sizing.status == SizingStatus.SIZED
        action = _action(repo, case, sizing, clock)
        times = (feature.envelope.available_at_ns, case.candidate.envelope.available_at_ns,
            case.candidate_set.envelope.available_at_ns, sizing.available_at_ns, action.available_at_ns)
        assert times == tuple(CUTOFF + offset for offset in (3, 6, 9, 12, 15))
        for ref in (feature.content_hash, case.candidate.content_hash, case.candidate_set.content_hash,
                    sizing.content_hash, action.content_hash):
            receipt = repo.get_artifact(chronology_ref(ref))
            assert receipt is not None and receipt.artifact_type == VERSION
            body = receipt.metadata["chronology"]
            assert body["market_information_cutoff_ns"] == CUTOFF and body["authority"] == "ZERO"
            immediate_input_cutoff = max((CUTOFF, *(repo.get_artifact(dep).available_at_ns
                for dep in body["input_refs"])))
            assert body["information_cutoff_ns"] == immediate_input_cutoff
            assert immediate_input_cutoff <= body["computation_started_ns"]
            assert causal_artifact(repo, ref, cutoff_ns=CUTOFF, consumer_at_ns=action.available_at_ns,
                                   deadline_ns=case.candidate.deadline_ns)
            assert not causal_artifact(repo, ref, cutoff_ns=CUTOFF, consumer_at_ns=CUTOFF,
                                       deadline_ns=case.candidate.deadline_ns)
        with pytest.raises(ValueError, match="unavailable"):
            action_features(repo, action.content_hash, cutoff_ns=CUTOFF)
        vector = action_features(repo, action.content_hash, cutoff_ns=CUTOFF, consumer_at_ns=clock.at)
        assert vector.information_cutoff_ns == CUTOFF
        assert vector.feature_artifact_ref == feature.content_hash
        assert vector.candidate_ref == case.candidate.content_hash and vector.action_hash == action.action.action_hash


def test_restart_reuses_exact_sizing_and_action_publications(tmp_path):
    path = tmp_path / "restart.sqlite"
    with OpsRepository(path) as repo:
        case, _, clock = _delayed_case(repo)
        sizing = _size(repo, case, clock)
        action = _action(repo, case, sizing, clock)
        receipt_count = len(repo.artifact_entries(VERSION))
    with OpsRepository(path) as repo:
        clock = Clock(action.available_at_ns + 10)
        repeated_sizing = _size(repo, case, clock)
        repeated_action = _action(repo, case, repeated_sizing, clock)
        assert repeated_sizing.content_hash == sizing.content_hash
        assert repeated_action.content_hash == action.content_hash
        assert repeated_sizing.available_at_ns == sizing.available_at_ns
        assert repeated_action.available_at_ns == action.available_at_ns
        assert len(repo.artifact_entries(VERSION)) == receipt_count


@pytest.mark.parametrize("change", [{"information_cutoff_ns": CUTOFF},
    {"market_information_cutoff_ns": CUTOFF + 1}, {"undocumented_cutoff_rescue": True}])
def test_hash_valid_receipt_cannot_backdate_input_cutoff_or_widen_market_prefix(tmp_path, change):
    from atlas.v2._serialization import canonical_json

    with OpsRepository(tmp_path / "receipt.sqlite") as repo:
        case, _, clock = _delayed_case(repo)
        ref = chronology_ref(case.candidate.content_hash)
        receipt = repo.get_artifact(ref)
        body = {**receipt.metadata["chronology"], **change}
        repo._connection.execute("UPDATE artifact_index SET metadata_json=?,content_hash=? WHERE artifact_ref=?",
            (canonical_json({"chronology": body}), sha256_json(body), ref))
        assert not causal_artifact(repo, case.candidate.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=clock.at, deadline_ns=case.candidate.deadline_ns)


def test_actual_feature_publication_uses_clock_and_rejects_future_bar(tmp_path):
    with OpsRepository(tmp_path / "feature.sqlite") as repo:
        health_ref = source(repo, "known-feature-health", CUTOFF)
        join = JoinedBars(KEY, CUTOFF, (), (), (), "NOT_ESTIMABLE", "MISSING_FRAMES", health_ref)
        clock = Clock()
        feature = feature_snapshot(join, clock_ns=clock)
        assert feature.information_cutoff_ns == CUTOFF
        assert feature.envelope.created_at_ns == CUTOFF + 2
        assert feature.envelope.available_at_ns == CUTOFF + 3
        repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2", feature.content_hash,
            feature.envelope.created_at_ns, feature.envelope.available_at_ns, {"feature": feature.to_dict()}))
        record_computation(repo, artifact_ref=feature.content_hash, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 1, finished_ns=feature.envelope.created_at_ns,
            available_ns=feature.envelope.available_at_ns, input_refs=feature.envelope.input_refs,
            deadline_ns=CUTOFF + 100)
        future = bar(CUTOFF // 900_000_000_000)
        with pytest.raises(ValueError, match="unavailable at cutoff"):
            feature_snapshot(replace(join, m15=(future,)), clock_ns=Clock(CUTOFF + 10))


def test_raw_after_cutoff_cannot_receive_or_be_laundered_through_receipt(tmp_path):
    with OpsRepository(tmp_path / "raw.sqlite") as repo:
        raw_ref = source(repo, "future-public-observation", CUTOFF + 1)
        with pytest.raises(ValueError, match="exact publication"):
            record_computation(repo, artifact_ref=raw_ref, information_cutoff_ns=CUTOFF,
                started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
                input_refs=(), deadline_ns=CUTOFF + 100)
        # Even a hash-valid forged receipt cannot extend the closed derived type
        # set to raw public observations.
        raw = repo.get_artifact(raw_ref)
        body = {"version": VERSION, "artifact_ref": raw_ref, "artifact_content_hash": raw.content_hash,
            "artifact_type": raw.artifact_type, "information_cutoff_ns": CUTOFF,
            "computation_started_ns": CUTOFF, "computation_finished_ns": CUTOFF + 1,
            "available_at_ns": CUTOFF + 1, "consumer_deadline_ns": CUTOFF + 100,
            "consumer_eligible": True, "input_refs": [], "authority": "ZERO"}
        repo.register_artifact(ArtifactIndexEntryV2(chronology_ref(raw_ref), VERSION, sha256_json(body),
            CUTOFF + 1, CUTOFF + 1, {"chronology": body}))
        assert not causal_artifact(repo, raw_ref, cutoff_ns=CUTOFF,
                                  consumer_at_ns=CUTOFF + 100, deadline_ns=CUTOFF + 100)
        derived_ref = sha256_json({"future-dependent-feature": raw_ref})
        repo.register_artifact(ArtifactIndexEntryV2(derived_ref, "FeatureArtifactV2", derived_ref,
            CUTOFF + 2, CUTOFF + 2, {}))
        with pytest.raises(ValueError, match="noncausal dependency"):
            record_computation(repo, artifact_ref=derived_ref, information_cutoff_ns=CUTOFF,
                started_ns=CUTOFF + 2, finished_ns=CUTOFF + 2, available_ns=CUTOFF + 2,
                input_refs=(raw_ref,), deadline_ns=CUTOFF + 100)


def test_corrupt_derived_receipt_prevents_m0_consumption(tmp_path):
    with OpsRepository(tmp_path / "corrupt.sqlite") as repo:
        case, feature, clock = _delayed_case(repo)
        sizing = _size(repo, case, clock)
        action = _action(repo, case, sizing, clock)
        repo._connection.execute("UPDATE artifact_index SET metadata_json=json_set(metadata_json, '$.chronology.information_cutoff_ns',?) WHERE artifact_ref=?",
            (CUTOFF + 1, chronology_ref(feature.content_hash)))
        assert not causal_artifact(repo, action.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=clock.at, deadline_ns=case.candidate.deadline_ns)
        with pytest.raises(ValueError, match="chronology"):
            action_features(repo, action.content_hash, cutoff_ns=CUTOFF, consumer_at_ns=clock.at)


def test_late_sizing_is_auditable_but_not_consumer_eligible(tmp_path):
    with OpsRepository(tmp_path / "late-sizing.sqlite") as repo:
        case, _, _ = _delayed_case(repo)
        deadline = case.candidate.deadline_ns
        clock = Clock(deadline - 1)
        sizing = _size(repo, case, clock)
        assert sizing.status == SizingStatus.NOT_ESTIMABLE
        assert sizing.reasons == ("SIZING_DEADLINE_EXPIRED",)
        body = repo.get_artifact(chronology_ref(sizing.content_hash)).metadata["chronology"]
        assert body["consumer_eligible"] is False and body["available_at_ns"] > deadline
        assert not causal_artifact(repo, sizing.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=clock.at, deadline_ns=deadline)
        with pytest.raises(ValueError):
            _action(repo, case, sizing, Clock(clock.at))
        assert repo.artifact_entries("ActionArtifactV2") == ()


def test_action_cannot_publish_after_original_deadline(tmp_path):
    with OpsRepository(tmp_path / "late-action.sqlite") as repo:
        case, _, clock = _delayed_case(repo)
        sizing = _size(repo, case, clock)
        with pytest.raises(ValueError, match="deadline"):
            _action(repo, case, sizing, Clock(case.candidate.deadline_ns - 1))
        assert repo.artifact_entries("ActionArtifactV2") == ()


def test_no_candidate_composition_roundtrip_preserves_raw_event_identity(tmp_path):
    with OpsRepository(tmp_path / "composition.sqlite") as repo:
        case = risk_case(repo)
        trigger = source(repo, "public-final-bar-trigger", CUTOFF)
        event = OpsDecisionEventV1(sha256_json({"event": trigger}), "PUBLIC_FINAL_BAR", "fixture-public",
            trigger, CUTOFF, CUTOFF, CUTOFF, CUTOFF, CUTOFF, CUTOFF + 100, (trigger,))
        original_wire, original_hash = event.to_dict(), event.content_hash
        inputs = ProductionEventInputsV1(case.universe, (), {}, {}, {}, (), (trigger,), ("S1:WARMUP",))
        _seal_public_composition(repo, event, inputs, clock_ns=Clock())
        loaded = _load_public_composition(repo, event)
        assert loaded is not None and loaded.universe.content_hash == case.universe.content_hash
        assert loaded.candidates == () and loaded.generation_missing_reasons == ("S1:WARMUP",)
        assert loaded.causal_source_refs == (trigger,)
        assert event.to_dict() == original_wire and event.content_hash == original_hash
        assert event.causal_input_refs == (trigger,)
        composition = repo.artifact_entries("OpsPublicDerivedCompositionV1")[0]
        assert composition.available_at_ns > event.information_cutoff_ns
        assert composition.artifact_ref not in event.causal_input_refs


def test_delayed_candidate_set_indexes_exact_cutoff_calendar_and_rejects_bad_receipt(tmp_path):
    from atlas.v2.science.outcomes import (
        AdmissionStateV2,
        DecisionCalendarEntryV2,
        DecisionSourceStageV2,
        SelectionStateV2,
        index_decision_calendar_entry,
    )

    with OpsRepository(tmp_path / "calendar.sqlite") as repo:
        case, _, _ = _delayed_case(repo)
        available = case.candidate_set.envelope.available_at_ns
        calendar = DecisionCalendarEntryV2(case.candidate_set.content_hash,
            case.candidate.content_hash, S1_POLICY.policy_id, S1_POLICY.version,
            S1_POLICY.policy_hash, CUTOFF, SelectionStateV2.SELECTED,
            AdmissionStateV2.NOT_EVALUATED, None, None, DecisionSourceStageV2.CANDIDATE_SET,
            (), case.candidate_set.content_hash, available, available)
        assert available > CUTOFF
        assert index_decision_calendar_entry(repo, calendar) == calendar.content_hash
        indexed = repo.get_artifact(calendar.content_hash)
        assert indexed.available_at_ns == available
        assert indexed.metadata["decision_entry"]["decision_at_ns"] == CUTOFF
        assert repo.due_work_items("ACTION_OUTCOME", as_of_ns=CUTOFF) == ()
        assert [item.source_ref for item in repo.due_work_items("ACTION_OUTCOME", as_of_ns=available)] == [calendar.content_hash]
        repo._connection.execute("UPDATE artifact_index SET metadata_json=json_set(metadata_json, '$.chronology.information_cutoff_ns', ?) WHERE artifact_ref=?",
            (CUTOFF + 1, chronology_ref(case.candidate_set.content_hash)))
        with pytest.raises(ValueError, match="exact causal computation receipt"):
            index_decision_calendar_entry(repo, calendar)


def test_production_hard_risk_calendar_waits_for_exact_action_publication(tmp_path):
    from atlas.v2.runtime.production import _persist_sizing_calendar
    from atlas.v2.science.outcomes import AdmissionStateV2

    with OpsRepository(tmp_path / "hard-risk-calendar.sqlite") as repo:
        case, _, clock = _delayed_case(repo)
        sizing = _size(repo, case, clock)
        action = _action(repo, case, sizing, clock)
        assert sizing.available_at_ns < action.available_at_ns
        ref = _persist_sizing_calendar(repo, case.candidate_set, case.candidate, sizing,
            AdmissionStateV2.RISK_SIZED, action=action)
        indexed = repo.get_artifact(ref)
        body = indexed.metadata["decision_entry"]
        assert body["decision_at_ns"] == CUTOFF
        assert body["source_artifact_ref"] == sizing.content_hash
        assert body["action_artifact_ref"] == action.content_hash
        assert indexed.available_at_ns >= action.available_at_ns
        assert repo.due_work_items("ACTION_OUTCOME", as_of_ns=sizing.available_at_ns) == ()
        assert [item.source_ref for item in repo.due_work_items(
            "ACTION_OUTCOME", as_of_ns=indexed.available_at_ns)] == [ref]


def test_default_port_expires_watch_on_restart_without_reopening_terminal_state(tmp_path):
    from atlas.v2.contracts import WatchStateV2
    from atlas.v2.runtime.production import ProductionOpsCyclePortV1

    from .test_memory import watch

    path = tmp_path / "watch-recovery.sqlite"
    with OpsRepository(path) as repo:
        repo.create_watch(replace(watch("expired", expires=120), required_next_event="BAR_CLOSE_15M"))
        repo.create_watch(replace(watch("active", expires=200), required_next_event="BAR_CLOSE_15M"))
        port = ProductionOpsCyclePortV1(clock_ns=lambda: 150)
        port.recover(repo, now_ns=150)
        expired = repo.get_watch("expired")
        assert expired.state == WatchStateV2.EXPIRED
        assert expired.updated_at_ns == 150 and expired.state_version == 1
        assert tuple(item.watch_id for item in repo.list_active_watches(limit=513)) == ("active",)
        outbox = repo.pending_outbox()
        assert len(outbox) == 1
        expired_outbox_id = outbox[0].outbox_id
        port.close()

    with OpsRepository(path) as repo:
        port = ProductionOpsCyclePortV1(clock_ns=lambda: 160)
        port.recover(repo, now_ns=160)
        assert repo.get_watch("expired") == expired
        assert tuple(item.outbox_id for item in repo.pending_outbox()) == (expired_outbox_id,)
        assert repo.get_watch("active").state == WatchStateV2.DETECTED
        port.close()
