"""Operational ceilings refuse entire populations and preserve causal cutoffs."""

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science import m0, m1
from atlas.v2.science.active_training import (
    FIT_ROW_LIMIT,
    PRESSURE_VERSION,
    RAW_ENTRY_LIMIT,
    TrainingPressureError,
    bounded_training_entries,
    require_row_budget,
)

from .test_session023_m1 import training_row


def _entries(repo, count, *, kind="MaturedOutcomeV2", at=10):
    entries = []
    for i in range(count):
        ref = sha256_json([kind, i, at])
        entries.append(ArtifactIndexEntryV2(ref, kind, ref, at, at, {}))
    repo.register_artifacts(entries)


def _forbid_scan(*_args, **_kwargs):
    raise AssertionError("active training must not scan an unbounded namespace")


def test_raw_bound_complete_page_future_rows_and_whole_overflow(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _entries(repo, RAW_ENTRY_LIMIT)
        _entries(repo, 1, at=20)
        monkeypatch.setattr(repo, "artifact_entries", _forbid_scan)
        assert len(bounded_training_entries(repo, 10)) == RAW_ENTRY_LIMIT
        assert m1.build_m1_training_rows(repo, cutoff_ns=10) == ()
        with pytest.raises(TrainingPressureError) as caught:
            m1.build_m1_training_rows(repo, cutoff_ns=20)
        assert caught.value.pressure["reason"] == "RAW_POPULATION_OVERFLOW"
        assert caught.value.pressure["observed_count"] == RAW_ENTRY_LIMIT + 1
        receipt = repo.get_artifact(caught.value.receipt_ref)
        assert receipt.artifact_type == PRESSURE_VERSION
        assert receipt.metadata["pressure"]["population_selection"] == "WHOLE_OPERATION_REFUSED_NO_SUBSET"
        assert receipt.available_at_ns > 20  # Pressure has its own publication time.
        with pytest.raises(TrainingPressureError):
            m0._training_rows(repo, 20)


def test_malformed_raw_index_refuses_fit_with_pressure(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _entries(repo, 1)
        repo._connection.execute("UPDATE artifact_index SET metadata_json='malformed' WHERE artifact_type='MaturedOutcomeV2'")
        with pytest.raises(TrainingPressureError) as caught:
            bounded_training_entries(repo, 10)
        assert caught.value.pressure["reason"] == "INVALID_INDEX_ROWS"
        assert caught.value.pressure["invalid_rows"] == 1


def test_fitting_bound_accepts_all_512_and_refuses_513_before_any_fit(tmp_path, monkeypatch):
    rows = tuple(training_row(i) for i in range(FIT_ROW_LIMIT + 1))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        require_row_budget(repo, rows[:-1], cutoff_ns=10)
        with pytest.raises(TrainingPressureError) as caught:
            require_row_budget(repo, rows, cutoff_ns=10)
        assert caught.value.pressure["limit"] == FIT_ROW_LIMIT
        assert caught.value.pressure["observed_count"] == FIT_ROW_LIMIT + 1
        assert repo.get_artifact(caught.value.receipt_ref) is not None
    monkeypatch.setattr(m1, "_make_estimator", _forbid_scan)
    with pytest.raises(TrainingPressureError):
        m1.chronological_oof(rows)
    with pytest.raises(TrainingPressureError):
        m1.build_walk_forward_chronology(rows, as_of_ns=600 * m1.DAY_NS)
    with pytest.raises(TrainingPressureError):
        m1.choose_parameters(rows[:-1], rows[-1:])


def test_holdout_state_checks_are_bounded_and_global_spending_cannot_be_reset(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        monkeypatch.setattr(repo, "artifact_entries", _forbid_scan)
        key = sha256_json("bounded-reservation")
        cutoff = 400 * m1.DAY_NS
        first = m1.reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=cutoff, available_at_ns=cutoff + 1)
        state_ref = sha256_json("future-spent")
        repo.register_artifact(ArtifactIndexEntryV2(state_ref, "DiscoveryHoldoutStateV2", state_ref,
            cutoff + 20, cutoff + 20, {"holdout_state": {"holdout_ref": first[2], "state": "SPENT"}}))
        with pytest.raises(ValueError, match="SPENT"):
            m1.reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=cutoff, available_at_ns=cutoff + 2)
        _entries(repo, RAW_ENTRY_LIMIT, kind="DiscoveryHoldoutStateV2", at=cutoff)
        with pytest.raises(TrainingPressureError):
            m1.reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=cutoff, available_at_ns=cutoff + 20)


def test_holdout_reserves_only_exact_visible_membership_without_parsing_labels(tmp_path, monkeypatch):
    policy = sha256_json("membership-policy")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        monkeypatch.setattr(repo, "artifact_entries", _forbid_scan)
        expected = []
        for decision, available in ((9, 20), (10, 20), (19, 30), (20, 20)):
            # This is membership metadata, deliberately not a usable payoff label.
            body = {"policy_hash": policy, "decision_at_ns": decision,
                "available_at_ns": available, "net_payoff": "NOT_A_TRAINING_TARGET"}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, "MaturedOutcomeV2", ref,
                available, available, {"outcome": body}))
            if 10 <= decision < 20 and available <= 20:
                expected.append(ref)
        assert m1._reserved_holdout_refs(repo, cutoff_ns=20, policy_hash=policy,
            start_ns=10, end_ns=20) == tuple(expected)
        body = {"policy_hash": policy, "decision_at_ns": 11, "available_at_ns": 30}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "MaturedOutcomeV2", ref, 20, 20, {"outcome": body}))
        with pytest.raises(ValueError, match="membership.*availability"):
            m1._reserved_holdout_refs(repo, cutoff_ns=20, policy_hash=policy, start_ns=10, end_ns=20)


def test_m1_later_action_requires_exact_computation_receipt_without_moving_market_cutoff(tmp_path):
    from atlas.v2.chronology import chronology_ref
    from atlas.v2.science.action import freeze_action
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .session023_support import research_case
    from .test_session017_risk import CUTOFF, size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY,
            v1=case.v1, v2=case.v2, clock_ns=lambda: CUTOFF + 1)
        result = m1.fit_m1(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 2, dependency_lock_hash=sha256_json("lock"))
        assert result.prediction.information_cutoff_ns == CUTOFF
        assert result.prediction.available_at_ns == CUTOFF + 2
        receipt = repo.get_artifact(chronology_ref(action.content_hash))
        assert receipt.metadata["chronology"]["information_cutoff_ns"] == CUTOFF
        repo._connection.execute("DELETE FROM artifact_index WHERE artifact_ref=?", (receipt.artifact_ref,))
        with pytest.raises(ValueError, match="noncausal"):
            m1.fit_m1(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
                cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 3, dependency_lock_hash=sha256_json("lock"))
