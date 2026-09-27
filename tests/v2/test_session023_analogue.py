"""Causal compatibility-first retrieval and dependence support regressions."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.microstructure import BookLevelV2, L2DeltaV2, L2SnapshotV2, SequenceValidBookV2
from atlas.v2.data.microstructure_archive import L2RawFrameV2
from atlas.v2.instruments import ProductContractV2, TradingStatusV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import freeze_action
from atlas.v2.science.analogue import (
    AnalogueCompatibilityV2,
    AnalogueNotEstimableError,
    AnalogueQueryV2,
    AnalogueTrainingObservationV2,
    action_semantics_hash,
    build_analogue_compatibility,
    build_analogue_query,
    estimate_analogue,
    observation_from_matured_outcome,
)
from atlas.v2.science.costs import (
    ActionCostContractV2,
    FeeScheduleV2,
    FundingScheduleV2,
    index_action_cost_contract,
    index_cost_evidence,
)
from atlas.v2.science.m1 import DAY_NS, HOUR_NS
from atlas.v2.science.replay import ReplayAssumptionsV2, index_replay_assumptions


def compatibility(**kwargs):
    base = AnalogueCompatibilityV2(sha256_json("policy"), "LONG", sha256_json("semantics"),
        HOUR_NS, "BYBIT", "LINEAR_PERPETUAL", sha256_json("instrument"), sha256_json("execution-contract"),
        sha256_json("quantity-participation"), sha256_json("liquidity-evidence"),
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


def evidence_bound_action(repo, *, liquidity=True, cost=True, funding=True):
    import hashlib

    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .session023_support import feature_candidate
    from .test_session017_risk import CUTOFF, risk_case, size, source
    from .test_session022_s4_microstructure import key as mainnet_key

    key = mainnet_key()
    source_id, channel, cutoff = "BYBIT_TEST_PUBLIC_WS", "orderbook.50.BTCUSDT", CUTOFF
    health = PublicSourceHealthV2(source_id, cutoff - 3_000_000_000, cutoff - 3_000_000_000,
        PublicSourceStateV2.HEALTHY_CURRENT, "analogue-health", "session023 typed S4 fixture")
    repo.register_artifact(ArtifactIndexEntryV2(health.content_hash, "PublicSourceHealthV2",
        health.content_hash, health.available_at_ns, health.available_at_ns, health.to_dict()))

    def raw_frame(frame_type, raw_text, at_ns, first, last, previous):
        payload = raw_text.encode()
        payload_hash = hashlib.sha256(payload).hexdigest()
        frame = L2RawFrameV2(key, source_id, channel, frame_type, payload, payload_hash, at_ns, at_ns, at_ns,
            first, last, previous, "BYBIT_U", "HEALTHY_CURRENT", "ACTUAL_SYSTEM", health.content_hash)
        metadata = {**frame.metadata_dict(), "raw_payload_hex": payload.hex()}
        repo.register_artifact(ArtifactIndexEntryV2(frame.record_id, "L2RawFrameV2", sha256_json(metadata),
            at_ns, at_ns, metadata))
        return payload_hash

    snapshot_at, delta_at = cutoff - 2_000_000_000, cutoff - 1_000_000_000
    snapshot_raw = raw_frame("SNAPSHOT", '{"kind":"snapshot","seq":100}', snapshot_at, None, 100, None)
    delta_raw = raw_frame("DELTA", '{"kind":"delta","seq":101}', delta_at, 101, 101, 100)
    book = SequenceValidBookV2(instrument=key, source_id=source_id, channel=channel,
        sequence_semantics="BYBIT_U", warmup_ns=0, stale_ns=2_000_000_000, declared_cadence_ns=1_000_000_000)
    snapshot = L2SnapshotV2(key, source_id, channel, "BYBIT_U", 100, snapshot_at, snapshot_at, snapshot_at,
        (BookLevelV2(Decimal("100"), Decimal("10")),),
        (BookLevelV2(Decimal("102"), Decimal("10")),), snapshot_raw, "HEALTHY_CURRENT", 50,
        source_health_ref=health.content_hash)
    delta = L2DeltaV2(key, source_id, channel, "BYBIT_U", 101, 101, 100, delta_at, delta_at, delta_at,
        (BookLevelV2(Decimal("100"), Decimal("11")),),
        (BookLevelV2(Decimal("102"), Decimal("9")),), delta_raw, "HEALTHY_CURRENT",
        source_health_ref=health.content_hash)
    assert book.apply_snapshot(snapshot).state.value == "WARMING"
    assert book.apply_delta(delta).state.value == "VALID"
    s4 = book.feature(cutoff_ns=cutoff, depth_bands_bps=(Decimal("200"),))
    assert s4.estimable and s4.cutoff_ns == cutoff and s4.instrument == key
    s4_ref = s4.content_hash
    repo.register_artifact(ArtifactIndexEntryV2(s4_ref, "S4FeatureArtifactV2", s4_ref, cutoff, cutoff,
        {"feature": s4.to_dict()}))

    fee_source = source(repo, "analogue-fee-source")
    funding_source = source(repo, "analogue-funding-source")
    cost_source = source(repo, "analogue-cost-source")
    fee = FeeScheduleV2(key, cutoff, Decimal("0.001"), Decimal("0.001"), fee_source)
    schedule = FundingScheduleV2(cutoff, (), True, funding_source)
    index_cost_evidence(repo, fee)
    index_cost_evidence(repo, schedule)
    assumptions = ReplayAssumptionsV2(0, 0, 0, 0, Decimal("1"), Decimal("0"))
    assumptions_ref = index_replay_assumptions(repo, assumptions, cutoff)
    fee_ref = fee.content_hash
    funding_ref = schedule.content_hash if funding else None
    contract = ActionCostContractV2(key, S1_POLICY.policy_hash, cutoff, fee_ref, schedule.content_hash,
        assumptions_ref, cost_source)
    contract_ref = index_action_cost_contract(repo, contract) if cost else sha256_json("missing-action-cost-contract")
    product = ProductContractV2(key, 0, 0, 0, Decimal("1"), Decimal("0.01"), Decimal("0.1"), Decimal("0.1"),
        TradingStatusV2.TRADING, source(repo, "analogue-product"), min_notional=Decimal("10"),
        max_qty=Decimal("1000"), fee_schedule_ref=fee_ref, funding_schedule_ref=funding_ref)
    case = risk_case(repo, cutoff_ns=cutoff, product_override=product,
        candidate_factory=lambda repository, _universe, product_contract: feature_candidate(repository,
            S1_POLICY, product_contract.key, cost_model_ref=contract_ref,
            liquidity_ref=s4_ref if liquidity else None))
    sizing = size(repo, case)
    action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
        sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
    return case, action, s4, book, health, fee, schedule, assumptions


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
    ("product", "SPOT"), ("instrument_key_hash", sha256_json("other-instrument")),
    ("execution_contract_hash", sha256_json("different-execution")),
    ("quantity_participation_key", sha256_json("different-quantity")),
    ("liquidity_regime_key", sha256_json("different-liquidity")),
    ("feature_availability", (False, True)), ("cost_semantics_hash", sha256_json("different-costs"))])
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


def test_repository_source_loader_revalidates_exact_honest_label_and_causal_features(tmp_path):
    from atlas.v2.science.outcomes import index_matured_outcome

    from .session023_support import feature_candidate
    from .test_session018_remediation import _payoff_case

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        candidate = feature_candidate(repo)
        candidate = replace(candidate, horizon_end_ns=candidate.decision_at_ns + 4 * 60 * 60 * 1_000_000_000)
        case, action, _, outcome = _payoff_case(repo, candidate_override=candidate)
        index_matured_outcome(repo, outcome)
        names = ("action.quantity_log_notional", "value:h1.roc10")
        compat = compatibility(policy_hash=action.action.policy_hash, action_representation_hash=action_semantics_hash(action.action.to_dict()),
            holding_horizon_ns=case.candidate.horizon_end_ns - case.candidate.decision_at_ns,
            feature_schema_hash=sha256_json(list(names)), feature_availability=(True, True))
        with pytest.raises(AnalogueNotEstimableError, match="NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE"):
            observation_from_matured_outcome(repo, outcome_ref=outcome.content_hash,
                cutoff_ns=outcome.available_at_ns, compatibility=compat, feature_names=names,
                episode_id=sha256_json("one"), regime_id="CALM")
        with pytest.raises(AnalogueNotEstimableError, match="NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE"):
            build_analogue_compatibility(repo, action_ref=action.content_hash)
        with pytest.raises(ValueError, match="exact frozen action"):
            build_analogue_query(repo, action_ref=action.content_hash, candidate_ref=case.candidate.content_hash,
                candidate_set_ref=case.candidate_set.content_hash, cutoff_ns=case.candidate.decision_at_ns + 1,
                compatibility=compat, feature_names=names, regime_id="CALM")


@pytest.mark.parametrize(("field", "value"), [
    ("execution_contract_hash", sha256_json("caller-execution-mode")),
    ("quantity_participation_key", sha256_json("caller-quantity-participation")),
    ("liquidity_regime_key", sha256_json("caller-liquidity-regime")),
    ("cost_semantics_hash", sha256_json("caller-cost-semantics")),
    ("feature_availability", (False,)),
    ("action_representation_hash", sha256_json("caller-action-semantics")),
    ("holding_horizon_ns", 2 * HOUR_NS),
    ("venue", "CALLER_VENUE"),
    ("product", "CALLER_PRODUCT"),
])
def test_repository_analogue_rejects_each_caller_tampered_compatibility_dimension(tmp_path, field, value):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, _, _, _, _, _, _ = evidence_bound_action(repo)
        compatibility_evidence = build_analogue_compatibility(repo, action_ref=action.content_hash)
        tampered = replace(compatibility_evidence, **{field: value})
        with pytest.raises(ValueError):
            build_analogue_query(repo, action_ref=action.content_hash,
                candidate_ref=case.candidate.content_hash, candidate_set_ref=case.candidate_set.content_hash,
                cutoff_ns=case.candidate.decision_at_ns, compatibility=tampered,
                feature_names=("action.quantity_log_notional", "value:h1.roc10"), regime_id="CALM")


def test_repository_compatibility_is_derived_deterministically_and_ignores_later_revisions(tmp_path):
    from dataclasses import replace as dataclass_replace

    from atlas.v2.science.costs import index_cost_evidence

    from .test_session017_risk import CUTOFF, source

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, s4, book, _, fee, funding, _ = evidence_bound_action(repo)
        original = build_analogue_compatibility(repo, action_ref=action.content_hash)
        assert original == build_analogue_compatibility(repo, action_ref=action.content_hash)
        names = ("action.quantity_log_notional", "value:h1.roc10")
        earlier_query = build_analogue_query(repo, action_ref=action.content_hash,
            candidate_ref=case.candidate.content_hash, candidate_set_ref=case.candidate_set.content_hash,
            cutoff_ns=CUTOFF, compatibility=original, feature_names=names, regime_id="CALM")
        later_s4 = book.feature(cutoff_ns=CUTOFF + 1, depth_bands_bps=(Decimal("200"),))
        repo.register_artifact(ArtifactIndexEntryV2(later_s4.content_hash, "S4FeatureArtifactV2",
            later_s4.content_hash, CUTOFF + 1, CUTOFF + 1, {"feature": later_s4.to_dict()}))
        later_fee_source = source(repo, "analogue-fee-revision", CUTOFF + 1)
        later_funding_source = source(repo, "analogue-funding-revision", CUTOFF + 1)
        index_cost_evidence(repo, dataclass_replace(fee, available_at_ns=CUTOFF + 1,
            entry_taker_rate=Decimal("0.02"), source_ref=later_fee_source))
        index_cost_evidence(repo, dataclass_replace(funding, available_at_ns=CUTOFF + 1,
            source_ref=later_funding_source))
        assert build_analogue_compatibility(repo, action_ref=action.content_hash) == original
        assert build_analogue_query(repo, action_ref=action.content_hash,
            candidate_ref=case.candidate.content_hash, candidate_set_ref=case.candidate_set.content_hash,
            cutoff_ns=CUTOFF, compatibility=original, feature_names=names, regime_id="CALM") == earlier_query

    with OpsRepository(tmp_path / "independent.sqlite") as independent_repo:
        _, independent_action, _, _, _, _, _, _ = evidence_bound_action(independent_repo)
        independent = build_analogue_compatibility(independent_repo, action_ref=independent_action.content_hash)
        assert independent_action.content_hash == action.content_hash
        assert independent.compatibility_key == original.compatibility_key


@pytest.mark.parametrize(("kwargs", "reason"), [
    ({"liquidity": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_LIQUIDITY_EVIDENCE"),
    ({"cost": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_COST_EVIDENCE"),
    ({"funding": False}, "NOT_ESTIMABLE_MISSING_CUTOFF_KNOWN_FUNDING_EVIDENCE"),
])
def test_repository_compatibility_names_missing_cutoff_liquidity_participation_or_cost(tmp_path, kwargs, reason):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, action, _, _, _, _, _, _ = evidence_bound_action(repo, **kwargs)
        with pytest.raises(AnalogueNotEstimableError, match=reason):
            build_analogue_compatibility(repo, action_ref=action.content_hash)
