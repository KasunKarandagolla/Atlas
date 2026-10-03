"""Bounded exact indicator prefixes preserve frozen S1/S2 policy decisions."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.active_history import ALGORITHM_VERSION, TAIL_LIMITS, advance
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.strategies.s1_trend import (
    S1_POLICY,
    ExecutableQuote,
    MarkIndexEvidence,
    S1ShadowCoordinator,
)
from atlas.v2.strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator

from .test_session014_core import KEY, bar
from .test_session014_s1 import SETUP, asof_join, clear, store_for_side, universe_at
from .test_session016_s2 import fixture


def state_for(bars, interval):
    inputs = tuple(IndexedCausalBarV2(item, sha256_json({"artifact_type": "PublicObservationIndexV2",
                                                      "record_id": item.raw.record_id})) for item in bars)
    state = None
    for offset in range(0, len(inputs), 128):
        state = advance(state, inputs[offset:offset + 128], key=KEY, interval=interval)
    assert state is not None
    return state


def economics(candidate):
    assert candidate is not None
    return (candidate.policy_hash, candidate.key, candidate.side, candidate.decision_at_ns,
            candidate.deadline_ns, candidate.horizon_end_ns, candidate.entry_reference,
            candidate.entry_collar, candidate.stop_price, candidate.quantity)


@pytest.mark.parametrize("h4_count", (2 * TAIL_LIMITS[BarIntervalV2.H4] + 17,
                                     3 * TAIL_LIMITS[BarIntervalV2.H4] + 17))
def test_s1_full_prefix_watch_and_candidate_equal_bounded_exact_state(tmp_path, h4_count):
    store = CausalBarStoreV2()
    for i in range(h4_count):
        store.append(bar(i, interval=BarIntervalV2.H4, close=str(Decimal(100) + Decimal(i) / 100)))
    h1_count = 4 * h4_count
    for i in range(h1_count):
        close = Decimal(100) + Decimal(i) / 100
        store.append(bar(i, interval=BarIntervalV2.H1, close=str(close),
                         low=str(close - Decimal("0.3")), high=str(close + Decimal("0.3"))))
    price = Decimal(100) + Decimal(h1_count - 1) / 100
    m15_count = 4 * h1_count
    for i in range(m15_count - 60, m15_count):
        store.append(bar(i, close=str(price), high=str(price + Decimal("0.45")),
                         low=str(price - Decimal("0.45"))))
    cutoff = h1_count * BarIntervalV2.H1.duration_ns
    full = asof_join(store, KEY, cutoff_ns=cutoff)
    h1 = state_for(full.h1, BarIntervalV2.H1)
    h4 = state_for(full.h4, BarIntervalV2.H4)
    bounded = replace(full, h1=h1.bars, h4=h4.bars)
    feature = feature_snapshot(full)
    with OpsRepository(tmp_path / "full.sqlite") as left, OpsRepository(tmp_path / "bounded.sqlite") as right:
        original = S1ShadowCoordinator(left)
        exact = S1ShadowCoordinator(right)
        a = original.create_watch(full, feature, event_gate=clear(cutoff), universe=universe_at(cutoff))
        b = exact.create_watch(bounded, feature, event_gate=clear(cutoff), universe=universe_at(cutoff),
                               h1_history=h1, h4_history=h4)
        assert a.status == b.status == "WATCH", (a.reason, b.reason)
        assert a.watch is not None and b.watch is not None
        a_setup, b_setup = left.get_artifact(a.watch.thesis_hash), right.get_artifact(b.watch.thesis_hash)
        assert a_setup is not None and b_setup is not None
        for name in ("side", "setup_window_extreme", "setup_bar_refs", "h4_ref", "h1_ref", "policy_hash"):
            assert a_setup.metadata[name] == b_setup.metadata[name]
        assert b_setup.metadata["indicator_algorithm_version"] == ALGORITHM_VERSION
        assert set(b_setup.metadata["indicator_history_state_refs"]) == {h1.content_hash, h4.content_hash}
        assert {h1.content_hash, h4.content_hash}.issubset(b.watch.evidence_refs)
        trigger_price = price + Decimal("0.65")
        trigger = bar(m15_count, close=str(trigger_price))
        store.append(trigger)
        at = trigger.close_at_ns
        full_trigger = asof_join(store, KEY, cutoff_ns=at)
        bounded_trigger = replace(full_trigger, h1=h1.bars, h4=h4.bars)
        feature_trigger = feature_snapshot(full_trigger)
        quote = ExecutableQuote(KEY, trigger_price - Decimal("0.01"), trigger_price + Decimal("0.01"),
                                at, at, sha256_json({"quote": at}))
        mark = MarkIndexEvidence(KEY, trigger_price, trigger_price, at, sha256_json({"mark": at}))
        aa = original.on_bar(a.watch.watch_id, full_trigger, feature_trigger,
                             event_gate=clear(at), bbo=quote, mark_index=mark)
        bb = exact.on_bar(b.watch.watch_id, bounded_trigger, feature_trigger,
                          event_gate=clear(at), bbo=quote, mark_index=mark, h1_history=h1, h4_history=h4)
        assert aa.status == bb.status == "CANDIDATE", (aa.reason, bb.reason)
        assert economics(aa.candidate) == economics(bb.candidate)
        assert bb.candidate is not None
        assert {h1.content_hash, h4.content_hash}.issubset(bb.candidate.envelope.input_refs)
        assert bb.candidate.policy_hash == S1_POLICY.policy_hash


@pytest.mark.parametrize("count", (2 * TAIL_LIMITS[BarIntervalV2.M15] + 17,
                                   3 * TAIL_LIMITS[BarIntervalV2.M15] + 17))
def test_s2_full_prefix_candidate_economics_and_statistics_equal_exact_tail(tmp_path, count):
    _, full, feature, universe, quote = fixture(history_count=count)
    history = state_for(full.m15, BarIntervalV2.M15)
    bounded = replace(full, m15=history.bars)
    with OpsRepository(tmp_path / "full.sqlite") as left, OpsRepository(tmp_path / "bounded.sqlite") as right:
        a = S2ShadowCoordinator(left).on_trigger_close(full, feature, universe=universe, bbo=quote)
        b = S2ShadowCoordinator(right).on_trigger_close(bounded, feature, universe=universe, bbo=quote,
                                                       m15_history=history)
        assert a.status == b.status == "CANDIDATE", (a.reason, b.reason)
        assert economics(a.candidate) == economics(b.candidate)
        assert a.setup_ref is not None and b.setup_ref is not None
        aa, bb = left.get_artifact(a.setup_ref), right.get_artifact(b.setup_ref)
        assert aa is not None and bb is not None
        for name in ("first_comparison_width20", "first_comparison_atr14", "latest_bollinger_width20",
                     "width_percentile_20", "latest_atr14", "atr_median", "comparison_refs", "range_refs"):
            assert aa.metadata[name] == bb.metadata[name], name
        assert bb.metadata["indicator_algorithm_version"] == ALGORITHM_VERSION
        assert bb.metadata["indicator_history_state_refs"] == (history.content_hash,)
        assert bb.metadata["indicator_prefix_bar_count"] == count + 1
        assert len(bb.metadata["indicator_history_refs"]) <= TAIL_LIMITS[BarIntervalV2.M15]
        assert b.candidate is not None and history.content_hash in b.candidate.envelope.input_refs
        assert b.candidate.policy_hash == S2_POLICY.policy_hash


def test_supplied_mismatched_or_future_history_refuses_without_legacy_fallback(tmp_path):
    _, joined, feature, universe, quote = fixture()
    history = state_for(joined.m15, BarIntervalV2.M15)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S2ShadowCoordinator(repository)
        changed = replace(joined, m15=joined.m15[:-1] + (bar(len(joined.m15) - 1, close="100.7"),))
        assert coordinator.on_trigger_close(changed, feature, universe=universe, bbo=quote,
            m15_history=history).reason == "EXACT_PREFIX_HISTORY_MISMATCH"
        for wrong in (replace(history, max_source_available_at_ns=joined.cutoff_ns + 1),
                      replace(history, key=replace(KEY, native_symbol="ETHUSDT"))):
            decision = coordinator.on_trigger_close(joined, feature, universe=universe, bbo=quote,
                                                      m15_history=wrong)
            assert decision.status == "NOT_ESTIMABLE"
            assert decision.reason == "EXACT_PREFIX_HISTORY_MISMATCH"
        assert repository.artifact_entries("CandidateActionV2") == ()


def test_s1_mismatched_setup_and_trigger_history_refuse_without_watch_mutation(tmp_path):
    store = store_for_side("LONG")
    joined = asof_join(store, KEY, cutoff_ns=SETUP)
    feature = feature_snapshot(joined)
    h1 = state_for(joined.h1, BarIntervalV2.H1)
    h4 = state_for(joined.h4, BarIntervalV2.H4)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)
        future = replace(h1, max_source_available_at_ns=SETUP + 1)
        failed = coordinator.create_watch(joined, feature, event_gate=clear(SETUP),
            universe=universe_at(SETUP), h1_history=future, h4_history=h4)
        assert failed.reason == "EXACT_PREFIX_HISTORY_MISMATCH" and failed.watch is None
        valid = coordinator.create_watch(joined, feature, event_gate=clear(SETUP),
            universe=universe_at(SETUP), h1_history=h1, h4_history=h4)
        assert valid.watch is not None
        wrong = replace(h4, key=replace(KEY, native_symbol="ETHUSDT"))
        refused = coordinator.on_bar(valid.watch.watch_id, joined, feature,
            event_gate=clear(SETUP), bbo=None, mark_index=None, h1_history=h1, h4_history=wrong)
        assert refused.status == "NOT_ESTIMABLE" and refused.reason == "EXACT_PREFIX_HISTORY_MISMATCH"
        assert repository.get_watch(valid.watch.watch_id) == valid.watch
