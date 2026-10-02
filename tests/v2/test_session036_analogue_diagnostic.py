from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, ArtifactIndexPageV2, OpsRepository
from atlas.v2.runtime.analogue_diagnostic import run_analogue_diagnostic_v1

from .test_session023_analogue import evidence_bound_action


def execute(repo: OpsRepository, **kwargs: Any) -> Any:
    case, action, *_ = evidence_bound_action(repo, **kwargs)
    result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
        candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
        available_at_ns=action.available_at_ns + 1)
    return case, action, result


def test_real_compatibility_query_and_empty_population_are_persisted_with_exact_action(tmp_path: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, result = execute(repo)
        assert result.result.compatibility_key is not None
        assert result.result.support_status == "NOT_ESTIMABLE"
        assert result.result.compatible_population_count == 0
        receipt = repo.get_artifact(result.retrieval_receipt_ref).metadata["receipt"]
        assert receipt["action_hash"] == action.action.action_hash
        assert receipt["candidate_set_ref"] == case.candidate_set.content_hash
        query = repo.get_artifact(receipt["query_ref"])
        assert query.metadata["query"]["information_cutoff_ns"] == case.candidate.decision_at_ns
        policy = repo.get_artifact(receipt["retrieval_policy_ref"])
        assert policy.metadata["policy"]["episode_policy"] == "UTC_DAY_SHARED_CAUSAL_MARKET_STREAM_V1"
        assert policy.content_hash == sha256_json(policy.metadata["policy"])
        assert receipt["authority"] == "ZERO"
        replay = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=action.available_at_ns + 2)
        assert replay.result_ref == result.result_ref
        assert repo.get_artifact(result.result_ref).available_at_ns == action.available_at_ns + 1


@pytest.mark.parametrize("missing,expected", [
    ({"liquidity": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE"),
    ({"cost": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_COST_EVIDENCE"),
    ({"funding": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_FUNDING_EVIDENCE"),
])
def test_missing_required_contracts_are_not_replaced_by_empty_history(tmp_path: Any, missing: Any, expected: str) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _case, _action, result = execute(repo, **missing)
        assert result.reason == expected
        assert result.result.compatibility_key is None
        assert result.result.weighted_estimate is None


def test_real_post_cutoff_action_availability_is_explicit_without_backdating(tmp_path: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        later = replace(action, available_at_ns=action.available_at_ns + 1)
        result = run_analogue_diagnostic_v1(repo, action=later, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=later.available_at_ns + 1)
        assert result.reason == "NOT_ESTIMABLE_ACTION_ARTIFACT_AVAILABLE_AFTER_MARKET_CUTOFF"
        assert repo.get_artifact(result.result_ref).available_at_ns == later.available_at_ns + 1


def test_population_overflow_and_corruption_are_explicit_not_partial_fits(tmp_path: Any, monkeypatch: Any) -> None:
    from atlas.v2.runtime import analogue_diagnostic as runtime

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        calls: list[int] = []
        fake = ArtifactIndexEntryV2("a" * 64, "MaturedOutcomeV2", "a" * 64, 1, 1, {})

        def overflow(*_args: Any, **kwargs: Any) -> ArtifactIndexPageV2:
            calls.append(kwargs["limit"])
            return ArtifactIndexPageV2((fake,), (1, "a" * 64), 0, ((1, "a" * 64),))

        monkeypatch.setattr(runtime, "MAX_ANALOGUE_OUTCOMES_V1", 0)
        monkeypatch.setattr(repo, "artifact_entries_by_types_page", overflow)
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=action.available_at_ns + 1)
        assert result.reason == "NOT_ESTIMABLE_ANALOGUE_OUTCOME_POPULATION_EXCEEDS_BOUND"
        assert calls == [128]
        assert result.result.weighted_estimate is None


def test_controller_binding_error_is_rejected_before_persistence(tmp_path: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        with pytest.raises(ValueError, match="exact frozen"):
            run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
                candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns + 1,
                available_at_ns=action.available_at_ns + 2)
        assert repo.artifact_entries("RuntimeAnalogueRetrievalReceiptV1") == ()


def test_actual_completion_clock_crossing_deadline_retains_expired_diagnostic(tmp_path: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        finished = case.candidate.deadline_ns + 1
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=action.available_at_ns + 1, clock_ns=lambda: finished)
        assert result.reason == "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED"
        assert repo.get_artifact(result.result_ref).available_at_ns == finished
        receipt = repo.get_artifact(result.retrieval_receipt_ref).metadata["receipt"]
        assert receipt["available_at_ns"] == finished


def test_completion_exactly_on_deadline_is_not_classified_as_late(tmp_path: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=case.candidate.deadline_ns, clock_ns=lambda: case.candidate.deadline_ns)
        assert "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED" not in result.result.reasons
        assert result.result.compatibility_key is not None
        assert repo.get_artifact(result.result_ref).available_at_ns == case.candidate.deadline_ns


def test_invalid_population_index_is_not_an_empty_population(tmp_path: Any, monkeypatch: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        monkeypatch.setattr(repo, "artifact_entries_by_types_page", lambda *_args, **_kwargs:
            ArtifactIndexPageV2((), (1, "invalid"), 1, ((1, "invalid"),)))
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=action.available_at_ns + 1)
        assert result.reason == "NOT_ESTIMABLE_ANALOGUE_OUTCOME_INDEX_INVALID"
        assert result.result.compatibility_key is None
        assert result.result.weighted_estimate is None


def test_corrupt_regime_feature_type_fails_closed(tmp_path: Any, monkeypatch: Any) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        original = repo.get_artifact

        def wrong_type(ref: str) -> Any:
            entry = original(ref)
            return replace(entry, artifact_type="UnrelatedArtifactV1") if ref == case.candidate.snapshot_hash else entry

        monkeypatch.setattr(repo, "get_artifact", wrong_type)
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=case.candidate.decision_at_ns,
            available_at_ns=action.available_at_ns + 1)
        assert result.result.support_status == "NOT_ESTIMABLE"
        assert result.result.weighted_estimate is None


def test_matured_source_is_routed_to_existing_revalidated_retrieval(tmp_path: Any, monkeypatch: Any) -> None:
    """A fake eligible label port exercises composition; science conformance has its own suite."""
    from atlas.v2.runtime import analogue_diagnostic as runtime
    from atlas.v2.science import analogue as science
    from atlas.v2.science.analogue import AnalogueTrainingObservationV2
    from atlas.v2.science.m0 import action_features

    from .test_session018_remediation import _payoff_case

    with OpsRepository(tmp_path / "source-fixture.sqlite") as source_repo:
        _source_case, _source_action, _payoff, original = _payoff_case(source_repo)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, *_ = evidence_bound_action(repo)
        cutoff = case.candidate.decision_at_ns
        day = 86_400_000_000_000
        decision = cutoff - 10 * day
        end = decision + case.candidate.horizon_end_ns - cutoff
        outcome = replace(original, decision_at_ns=decision, horizon_end_ns=end,
            matured_at_ns=end + 1, available_at_ns=end + 2,
            policy_id=action.action.policy_id, policy_version=action.action.policy_version,
            policy_hash=action.action.policy_hash, instrument_revision=action.action.key.contract_revision,
            venue=action.action.key.venue.value, product=action.action.key.product.value)
        entry = ArtifactIndexEntryV2(outcome.content_hash, "MaturedOutcomeV2", outcome.content_hash,
            outcome.matured_at_ns, outcome.available_at_ns, {"outcome": outcome.to_dict()})
        compatibility = science.build_analogue_compatibility(repo, action_ref=action.content_hash)
        vector = action_features(repo, action.content_hash, cutoff_ns=cutoff)
        values = dict(zip(vector.feature_order, vector.values, strict=True))
        calls: list[dict[str, Any]] = []

        def source_port(_repo: Any, **kwargs: Any) -> AnalogueTrainingObservationV2:
            calls.append(kwargs)
            names = kwargs["feature_names"]
            numbers = tuple(None if name.startswith("value:") and values.get("missing:" + name[6:])
                            else values[name] for name in names)
            return AnalogueTrainingObservationV2(outcome.content_hash, outcome.action_hash,
                outcome.candidate_ref, outcome.candidate_set_ref, compatibility.compatibility_key, names,
                numbers, tuple(name for name, value in zip(names, numbers, strict=True) if value is None),
                decision, end, outcome.available_at_ns, kwargs["episode_id"], kwargs["regime_id"],
                outcome.net_payoff, outcome.provenance.value, outcome.execution_state.value, sha256_json("fake-witness"))

        original_compatibility = runtime.build_analogue_compatibility
        monkeypatch.setattr(runtime, "build_analogue_compatibility", lambda repository, *, action_ref:
            original_compatibility(repository, action_ref=action_ref) if action_ref == action.content_hash else compatibility)
        original_regime = runtime._regime
        monkeypatch.setattr(runtime, "_regime", lambda repository, candidate_ref, at:
            original_regime(repository, candidate_ref, at) if candidate_ref == case.candidate.content_hash else "FAKE_SOURCE_REGIME")
        monkeypatch.setattr(runtime, "observation_from_matured_outcome", source_port)
        monkeypatch.setattr(science, "observation_from_matured_outcome", source_port)
        monkeypatch.setattr(repo, "artifact_entries_by_types_page", lambda *_args, **_kwargs:
            ArtifactIndexPageV2((entry,), (entry.created_at_ns, entry.artifact_ref), 0,
                               ((entry.created_at_ns, entry.artifact_ref),)))
        result = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, cutoff_ns=cutoff, available_at_ns=action.available_at_ns + 1)
        assert result.result.compatible_population_count == 1
        assert result.result.neighbor_refs == (outcome.content_hash,)
        assert result.result.weighted_estimate is None  # One label cannot establish support.
        assert len(calls) == 2  # Initial source load and existing repository retrieval revalidation.
        assert all(call["outcome_ref"] == outcome.content_hash and call["cutoff_ns"] == cutoff for call in calls)
