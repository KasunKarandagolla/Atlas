"""Causal compatibility-first retrieval and dependence support regressions."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.analogue import (
    AnalogueCompatibilityV2,
    AnalogueQueryV2,
    AnalogueTrainingObservationV2,
    action_semantics_hash,
    build_analogue_query,
    estimate_analogue,
    estimate_causal_analogue,
    observation_from_matured_outcome,
)
from atlas.v2.science.m1 import DAY_NS, HOUR_NS


def compatibility(**kwargs):
    base = AnalogueCompatibilityV2(sha256_json("policy"), "LONG", sha256_json("semantics"),
        HOUR_NS, "BYBIT", "LINEAR_PERPETUAL", "MINUTE_REPLAY", "LOG_BASE_NOTIONAL", "DEEP",
        sha256_json(["a", "b"]), (True, False), sha256_json("net-costs"))
    return replace(base, **kwargs)


def query():
    return AnalogueQueryV2(*(sha256_json(name) for name in ("query", "action", "candidate", "set")),
        500 * DAY_NS, 500 * DAY_NS + HOUR_NS, compatibility(), ("a", "b"), (5.0, None),
        (499 * DAY_NS, None), ("b",), "CALM")


def observation(index, *, value=5.0, episode=None):
    decision = (index + 1) * 7 * DAY_NS
    return AnalogueTrainingObservationV2(*(sha256_json([index, name]) for name in
        ("outcome", "action", "candidate", "set")), compatibility().compatibility_key,
        ("a", "b"), (value, None), ("b",), decision, decision + HOUR_NS,
        decision + HOUR_NS + 1, sha256_json(["episode", index if episode is None else episode]),
        "CALM", Decimal(index), "SIMULATED", "NO_FILL" if index % 3 == 0 else "FULL_FILL", sha256_json([index, "eligible"]))


def test_distance_determinism_missingness_and_supported_nonparametric_estimate():
    q = query()
    rows = tuple(observation(i) for i in range(30))
    result = estimate_analogue(q, rows)
    assert result == estimate_analogue(q, rows[::-1])
    assert result.support_status == "SUPPORTED" and result.weighted_estimate == Decimal("14.5")
    assert result.missing_features == ("b",) and result.independent_support_count == 30
    assert all(neighbor.distance == 0 for neighbor in result.neighbors)
    assert result.effective_sample_size > Decimal("29.9")


@pytest.mark.parametrize("dimension,value", [("policy_hash", sha256_json("different")),
    ("side", "SHORT"), ("holding_horizon_ns", 2 * HOUR_NS), ("venue", "BINANCE"),
    ("product", "SPOT"), ("execution_mode", "ACTUAL"), ("cost_semantics_hash", sha256_json("different-costs"))])
def test_incompatible_actions_cannot_be_neighbours(dimension, value):
    incompatible = replace(observation(0), compatibility_key=compatibility(**{dimension: value}).compatibility_key)
    result = estimate_analogue(query(), (incompatible,))
    assert result.compatible_population_count == 0 and result.support_status == "NOT_ESTIMABLE"


def test_future_unmatured_overlap_and_future_tail_do_not_change_prior_query():
    q = query()
    rows = tuple(observation(i, value=float(i + 10)) for i in range(30))
    baseline = estimate_analogue(q, rows)
    future = observation(1000, value=1e30)
    unmatured = replace(observation(40), label_available_at_ns=q.information_cutoff_ns + 1)
    overlapping_target = replace(observation(41), decision_at_ns=q.information_cutoff_ns - HOUR_NS,
        horizon_end_ns=q.information_cutoff_ns, label_available_at_ns=q.information_cutoff_ns)
    assert estimate_analogue(q, (*rows, future, unmatured, overlapping_target)) == baseline
    with pytest.raises(ValueError, match="revised"):
        estimate_analogue(q, (*rows, replace(rows[0], net_payoff=Decimal("999"))))


def test_one_episode_and_overlapping_labels_do_not_create_independent_support():
    q = query()
    same_episode = tuple(observation(i, episode="one") for i in range(30))
    result = estimate_analogue(q, same_episode)
    assert result.independent_support_count == 1 and result.effective_sample_size == Decimal(1)
    assert result.weighted_estimate is None and result.support_status == "NOT_ESTIMABLE"
    clustered = tuple(replace(observation(i), decision_at_ns=100 * DAY_NS + i,
        horizon_end_ns=100 * DAY_NS + i + HOUR_NS,
        label_available_at_ns=100 * DAY_NS + i + HOUR_NS + 1) for i in range(30))
    assert estimate_analogue(q, clustered).independent_support_count == 1
    concentrated = tuple(observation(i, episode="dependent" if i < 10 else i) for i in range(30))
    weighted = estimate_analogue(q, concentrated)
    assert weighted.independent_support_count == 21 and weighted.effective_sample_size < Decimal(20)
    assert weighted.support_status == "NOT_ESTIMABLE" and weighted.weighted_estimate is None


def test_feature_availability_and_support_floors_cannot_be_bypassed():
    with pytest.raises(ValueError, match="future"):
        replace(query(), feature_available_at_ns=(query().information_cutoff_ns + 1, None))
    with pytest.raises(ValueError, match="availability"):
        replace(query(), feature_available_at_ns=(None, None))
    with pytest.raises(ValueError, match="configuration"):
        estimate_analogue(query(), (), minimum_independent_support=1)


def test_repository_source_loader_revalidates_exact_honest_label_and_causal_features(tmp_path, monkeypatch):
    from atlas.v2.science.outcomes import index_matured_outcome

    from . import test_session017_risk as risk_module
    from .session023_support import feature_candidate
    from .test_session018_remediation import _payoff_case

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        monkeypatch.setattr(risk_module, "candidate", lambda policy=risk_module.S1_POLICY, key=risk_module.KEY,
            **kwargs: feature_candidate(repo, policy, key, **kwargs))
        case, action, _, outcome = _payoff_case(repo)
        index_matured_outcome(repo, outcome)
        names = ("action.quantity_log_notional", "value:h1.roc10")
        compat = compatibility(policy_hash=action.action.policy_hash, action_representation_hash=action_semantics_hash(action.action.to_dict()),
            holding_horizon_ns=case.candidate.horizon_end_ns - case.candidate.decision_at_ns,
            feature_schema_hash=sha256_json(list(names)), feature_availability=(True, True))
        source = observation_from_matured_outcome(repo, outcome_ref=outcome.content_hash,
            cutoff_ns=outcome.available_at_ns, compatibility=compat, feature_names=names,
            episode_id=sha256_json("one"), regime_id="CALM")
        assert source.net_payoff == outcome.net_payoff and source.action_hash == action.action.action_hash
        with pytest.raises(ValueError, match="future"):
            observation_from_matured_outcome(repo, outcome_ref=outcome.content_hash,
                cutoff_ns=outcome.available_at_ns - 1, compatibility=compat, feature_names=names,
                episode_id=sha256_json("one"), regime_id="CALM")
        q = build_analogue_query(repo, action_ref=action.content_hash, candidate_ref=case.candidate.content_hash,
            candidate_set_ref=case.candidate_set.content_hash, cutoff_ns=case.candidate.decision_at_ns,
            compatibility=compat, feature_names=names, regime_id="CALM")
        assert estimate_causal_analogue(repo, q, (source,), compatibility_contracts={compat.compatibility_key: compat}).neighbor_refs == ()
        with pytest.raises(ValueError, match="reproduce"):
            estimate_causal_analogue(repo, replace(q, values=(999.0, 1.0)), (), compatibility_contracts={})
