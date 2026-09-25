"""Session 016 S2 causal compression, trigger, frozen management and variants."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, EligibilityStatusV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2
from atlas.v2.features.joins import asof_join
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import StrategyEligibilityV2, UniverseContractV2, UniverseEntryV2, VenueV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.strategies.s1_trend import S1_POLICY, ExecutableQuote
from atlas.v2.strategies.s2_breakout import (
    POLICY_ID,
    QUANTILE_VERSION,
    S2_POLICY,
    S2ShadowCoordinator,
    breakout_side,
    compression_passes,
    empirical_quantile,
    experiment_specs,
    failed_break_exit,
    range_width_valid,
)

from .test_session014_core import KEY, bar
from .test_session014_s1 import health

M15 = BarIntervalV2.M15.duration_ns
TRIGGER_INDEX = 2900


def fixture(*, trigger_close: str = "100.6", trigger_volume: str = "11",
            history_count: int = 2900, key=KEY):
    store = CausalBarStoreV2()
    for i in range(history_count):
        if i < history_count - 20:
            close = "99" if i % 2 else "101"
            store.append(bar(i, close=close, high=str(Decimal(close) + Decimal("0.3")),
                             low=str(Decimal(close) - Decimal("0.3")), key=key))
        else:
            store.append(bar(i, close="100", high="100.3", low="99.7", key=key))
    trigger = bar(history_count, close=trigger_close,
                  high=str(max(Decimal(trigger_close), Decimal("100.3")) + Decimal("0.1")),
                  low=str(min(Decimal(trigger_close), Decimal("99.7")) - Decimal("0.1")),
                  volume=trigger_volume, key=key)
    store.append(trigger)
    cutoff = trigger.close_at_ns
    store.append(bar(cutoff // BarIntervalV2.H1.duration_ns - 1, interval=BarIntervalV2.H1, key=key))
    store.append(bar(cutoff // BarIntervalV2.H4.duration_ns - 1, interval=BarIntervalV2.H4, key=key))
    joined = asof_join(store, key, cutoff_ns=cutoff, source_health=health(cutoff), trigger_ref=trigger.content_hash)
    feature = feature_snapshot(replace(joined, m15=joined.m15[-60:]))
    product_ref = sha256_json({"product": key.to_dict()})
    universe = UniverseContractV2(ArtifactEnvelope(1, "u016", cutoff, cutoff, "fixture", (product_ref,)),
        "fixture", cutoff, S2_POLICY.policy_hash,
        (UniverseEntryV2(key, product_ref, True, True, True, True, False,
            FrozenMap({POLICY_ID: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE),
                       S1_POLICY.policy_id: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE)}), ()),))
    quote = ExecutableQuote(key, Decimal(trigger_close) - Decimal("0.01"), Decimal(trigger_close), cutoff, cutoff,
                            sha256_json({"quote": cutoff, "key": key.to_dict()}))
    return store, joined, feature, universe, quote


def test_exact_history_long_candidate_and_frozen_evidence(tmp_path):
    _, joined, feature, universe, quote = fixture()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S2ShadowCoordinator(repository).on_trigger_close(joined, feature, universe=universe, bbo=quote)
        assert result.status == "CANDIDATE", result.reason
        candidate = result.candidate
        assert candidate is not None and candidate.quantity is None
        assert candidate.policy_hash == S2_POLICY.policy_hash
        assert candidate.side.value == "LONG" and candidate.stop_price == Decimal("99.7")
        assert candidate.entry_reference == quote.ask
        assert candidate.entry_collar == quote.ask * Decimal("1.0005")
        assert candidate.horizon_end_ns - candidate.decision_at_ns == 2 * BarIntervalV2.H1.duration_ns
        setup = repository.get_artifact(result.setup_ref)
        trigger = repository.get_artifact(result.trigger_ref)
        assert setup is not None and trigger is not None
        assert setup.metadata["quantile_convention"] == QUANTILE_VERSION
        assert len(setup.metadata["range_refs"]) == 20
        assert joined.m15[-1].content_hash not in setup.metadata["range_refs"]
        assert setup.metadata["atr_ref"] == joined.m15[-2].content_hash
        assert trigger.metadata["spread"] == "0.01"
        assert len(setup.metadata["comparison_refs"]) == 2880
        assert trigger.metadata["bbo_ref"] == quote.evidence_ref
        assert repository.get_artifact(candidate.content_hash) is not None
        assert repository.get_artifact(S2_POLICY.policy_hash) is not None
        first = bar(TRIGGER_INDEX + 1, close="100", high="100.4", low="99.6")
        second = bar(TRIGGER_INDEX + 2, close="101", high="101.2", low="100.8")
        third = bar(TRIGGER_INDEX + 3, close="100", high="100.4", low="99.6")
        body = setup.metadata.to_dict()
        assert failed_break_exit(candidate, body, (first, second, third)) == first.content_hash
        assert failed_break_exit(candidate, body, (second, third)) is None
        second_inside = bar(TRIGGER_INDEX + 2, close="100", high="100.4", low="99.6")
        assert failed_break_exit(candidate, body, (second, second_inside)) == second_inside.content_hash
        assert failed_break_exit(candidate, body, (second,)) is None


def test_insufficient_history_and_missing_context_fail_closed(tmp_path):
    _, joined, feature, universe, quote = fixture(history_count=2899)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S2ShadowCoordinator(repository).on_trigger_close(joined, feature, universe=universe, bbo=quote)
        assert result.status == "NOT_ESTIMABLE" and "30_DAY" in result.reason
    _, joined, feature, universe, quote = fixture()
    with OpsRepository(tmp_path / "ops2.sqlite") as repository:
        coordinator = S2ShadowCoordinator(repository)
        assert coordinator.on_trigger_close(replace(joined, h1=()), feature, universe=universe, bbo=quote).status == "NOT_ESTIMABLE"
        assert coordinator.on_trigger_close(replace(joined, h4=()), feature, universe=universe, bbo=quote).status == "NOT_ESTIMABLE"
        stale = replace(quote, observed_at_ns=joined.cutoff_ns - 6_000_000_000)
        assert coordinator.on_trigger_close(joined, feature, universe=universe, bbo=stale).status == "NOT_ESTIMABLE"
        wrong_revision = replace(KEY, contract_revision="f" * 64)
        wrong_venue = replace(KEY, venue=VenueV2.BINANCE)
        assert coordinator.on_trigger_close(replace(joined, key=wrong_revision), feature,
                                            universe=universe, bbo=quote).status == "NOT_ESTIMABLE"
        assert coordinator.on_trigger_close(replace(joined, key=wrong_venue), feature,
                                            universe=universe, bbo=quote).status == "NOT_ESTIMABLE"


def test_s2_variants_and_quantile_identity():
    specs = experiment_specs()
    assert len({item.policy_hash for item in specs}) == 5
    assert all(item.capital_status == "SHADOW_ONLY" for item in specs)
    assert S2_POLICY.policy_hash == experiment_specs()[3].policy_hash
    assert not S2_POLICY.optional_features
    assert specs[-1].entry_rule["entry_mode"] != S2_POLICY.entry_rule["entry_mode"]
    assert empirical_quantile((4.0, 1.0, 2.0, 3.0, 5.0), .2) == 1.0
    assert S1_POLICY.policy_hash == "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"


def test_strict_predicates_and_inclusive_width_boundaries():
    assert not compression_passes(1.0, 1.0, 0.9, 1.0)
    assert not compression_passes(0.9, 1.0, 1.0, 1.0)
    assert range_width_valid(Decimal("0.5"), Decimal(1))
    assert range_width_valid(Decimal(3), Decimal(1))
    assert not range_width_valid(Decimal("0.4999"), Decimal(1))
    assert not range_width_valid(Decimal("3.0001"), Decimal(1))
    assert breakout_side(Decimal("100.1"), Decimal(100), Decimal(99), Decimal(1)) is None
    assert breakout_side(Decimal("100.1001"), Decimal(100), Decimal(99), Decimal(1)).value == "LONG"
    assert breakout_side(Decimal("98.9"), Decimal(100), Decimal(99), Decimal(1)) is None
    assert breakout_side(Decimal("98.8999"), Decimal(100), Decimal(99), Decimal(1)).value == "SHORT"


def test_short_volume_and_future_tail(tmp_path):
    store, joined, feature, universe, quote = fixture(trigger_close="99.4")
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S2ShadowCoordinator(repository)
        decision = coordinator.on_trigger_close(joined, feature, universe=universe, bbo=quote)
        assert decision.status == "CANDIDATE", decision.reason
        assert decision.candidate is not None
        assert decision.candidate.side.value == "SHORT"
        assert decision.candidate.stop_price == Decimal("100.3")
        assert decision.candidate.entry_reference == quote.bid
        assert decision.candidate.entry_collar == quote.bid * Decimal("0.9995")
        old_hash = decision.candidate.content_hash
        store.append(bar(TRIGGER_INDEX + 1, close="100000", high="100001", low="99999", volume="1000000"))
        replay = asof_join(store, KEY, cutoff_ns=joined.cutoff_ns,
                           source_health=health(joined.cutoff_ns), trigger_ref=joined.m15[-1].content_hash)
        again = coordinator.on_trigger_close(replay, feature, universe=universe, bbo=quote)
        assert again.candidate is not None and again.candidate.content_hash == old_hash
        assert again.setup_ref == decision.setup_ref
        equal_volume = replace(joined, m15=joined.m15[:-1] + (bar(TRIGGER_INDEX, close="99.4",
            high="100.4", low="99.3", volume="10"),))
        equal_feature = feature_snapshot(replace(equal_volume, m15=equal_volume.m15[-60:]))
        assert coordinator.on_trigger_close(equal_volume, equal_feature, universe=universe,
                                            bbo=quote).reason == "VOLUME_RULE_FAILED"


@pytest.mark.parametrize("close", ["100", "100.35"])
def test_inside_or_subthreshold_close_has_no_candidate(tmp_path, close):
    _, joined, feature, universe, quote = fixture(trigger_close=close)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S2ShadowCoordinator(repository).on_trigger_close(joined, feature, universe=universe, bbo=quote)
        assert result.status == "NO_CANDIDATE" and result.reason == "TRIGGER_RULE_FAILED"
        assert repository.artifact_entries("CandidateActionV2") == ()
