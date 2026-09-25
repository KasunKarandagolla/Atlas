"""Bounded public-bar to durable S1 shadow candidate integration."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, EligibilityStatusV2, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.features.candles import CausalTrade
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.joins import asof_join as causal_asof_join
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import InstrumentKeyV2, StrategyEligibilityV2, UniverseContractV2, UniverseEntryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import (
    POLICY_ID,
    S1_POLICY,
    EventGate,
    EventState,
    ExecutableQuote,
    MarkIndexEvidence,
    S1ShadowCoordinator,
    experiment_specs,
)

from .test_session014_core import KEY, bar

HOUR = BarIntervalV2.H1.duration_ns
M15 = BarIntervalV2.M15.duration_ns
SETUP = 200 * HOUR


def health(at: int, state: PublicSourceStateV2 = PublicSourceStateV2.HEALTHY_CURRENT) -> PublicSourceHealthV2:
    return PublicSourceHealthV2("fixture-public", at, at, state, sha256_json({"health": at, "state": state.value}), "fixture")


def asof_join(store: CausalBarStoreV2, key: InstrumentKeyV2, *, cutoff_ns: int, **kwargs: Any) -> JoinedBars:
    return causal_asof_join(store, key, cutoff_ns=cutoff_ns, source_health=kwargs.pop("source_health", health(cutoff_ns)), **kwargs)


def store_for_side(side: str) -> CausalBarStoreV2:
    store = CausalBarStoreV2()
    direction = Decimal(1 if side == "LONG" else -1)
    for i in range(50):
        price = Decimal(100) + direction * Decimal(i) * Decimal("0.1")
        store.append(bar(i, interval=BarIntervalV2.H4, close=str(price)))
    for i in range(148, 200):
        price = Decimal(100) + direction * Decimal(i - 150) * Decimal("0.1")
        low = price - Decimal("0.3")
        high = price + Decimal("0.3")
        if i == 197:
            if side == "LONG":
                low = Decimal("103.5")
            else:
                high = Decimal("96.5")
        store.append(bar(i, interval=BarIntervalV2.H1, close=str(price), high=str(high), low=str(low)))
    price = Decimal("104.9") if side == "LONG" else Decimal("95.1")
    for i in range(740, 800):
        store.append(bar(i, close=str(price), high=str(price + Decimal("0.45")), low=str(price - Decimal("0.45"))))
    return store


def universe_at(cutoff: int) -> UniverseContractV2:
    product_ref = sha256_json({"product": KEY.to_dict(), "revision": KEY.contract_revision})
    entry = UniverseEntryV2(KEY, product_ref, True, True, True, True, False,
                            FrozenMap({POLICY_ID: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE)}), ())
    envelope = ArtifactEnvelope(1, "u014", cutoff, cutoff, "universe-fixture-v1", (product_ref,))
    return UniverseContractV2(envelope, "fixture-v1", cutoff, S1_POLICY.policy_hash, (entry,))


def clear(at: int) -> EventGate:
    return EventGate(EventState.CLEAR, at, sha256_json({"gate": at}), "EVENT_GATE_V1")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_s1_watch_restart_candidate_and_handoff(tmp_path, side: str) -> None:
    store = store_for_side(side)
    assert KEY.contract_revision != KEY.content_hash
    assert all(item.instrument_revision == KEY.contract_revision for item in store.all_versions())
    joined = asof_join(store, KEY, cutoff_ns=SETUP)
    assert joined.status == "AVAILABLE"
    feature = feature_snapshot(joined)
    assert feature.content_hash == feature_snapshot(joined).content_hash
    assert feature.values["m15.atr14"].value is not None
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        coordinator = S1ShadowCoordinator(repository)
        decision = coordinator.create_watch(joined, feature, event_gate=clear(SETUP), universe=universe_at(SETUP))
        assert decision.status == "WATCH", decision.reason
        assert decision.watch is not None
        watch_id = decision.watch.watch_id
        assert decision.watch.state == WatchStateV2.WAITING_FOR_EVENT
        assert coordinator.create_watch(joined, feature, event_gate=clear(SETUP), universe=universe_at(SETUP)).watch == decision.watch
        assert coordinator.on_bar(watch_id, joined, feature, event_gate=clear(SETUP), bbo=None, mark_index=None).reason == "PRE_WATCH_OR_UNCONFIRMED_BAR"
    with OpsRepository(path) as repository:
        coordinator = S1ShadowCoordinator(repository)
        recovered = repository.recover_active_watches(now_ns=SETUP)
        assert (watch_id, "CANDLE_CLOSED_15M") in recovered.required_events
        assert coordinator.on_bar(watch_id, joined, feature, event_gate=clear(SETUP),
                                  bbo=None, mark_index=None).reason == "PRE_WATCH_OR_UNCONFIRMED_BAR"
        trigger_price = "105.55" if side == "LONG" else "94.45"
        trigger = bar(800, close=trigger_price)
        store.append(trigger)
        cutoff = trigger.close_at_ns
        triggered = asof_join(store, KEY, cutoff_ns=cutoff, trigger_ref=trigger.content_hash)
        triggered_feature = feature_snapshot(triggered)
        quote_price = Decimal(trigger_price)
        bbo = ExecutableQuote(KEY, quote_price - Decimal("0.01"), quote_price + Decimal("0.01"), cutoff, cutoff,
                              sha256_json({"bbo": cutoff}))
        mark = MarkIndexEvidence(KEY, quote_price, quote_price, cutoff, sha256_json({"mark": cutoff}))
        result = coordinator.on_bar(watch_id, triggered, triggered_feature, event_gate=clear(cutoff), bbo=bbo, mark_index=mark)
        assert result.status == "CANDIDATE", result.reason
        assert result.candidate is not None and result.watch is not None
        candidate = result.candidate
        assert candidate.quantity is None and candidate.policy_hash == S1_POLICY.policy_hash
        assert candidate.snapshot_hash == triggered_feature.content_hash
        assert candidate.side.value == side
        expected_reference = bbo.ask if side == "LONG" else bbo.bid
        assert candidate.entry_reference == expected_reference
        assert candidate.entry_collar == expected_reference * (Decimal("1.0005") if side == "LONG" else Decimal("0.9995"))
        setup = repository.get_artifact(result.watch.thesis_hash)
        assert setup is not None
        extreme = Decimal(str(setup.metadata["setup_window_extreme"]))
        atr = Decimal(str(triggered_feature.values["m15.atr14"].value))
        assert candidate.stop_price == extreme + (Decimal("-0.25") if side == "LONG" else Decimal("0.25")) * atr
        index = repository.get_artifact(candidate.content_hash)
        assert index is not None and index.metadata["candidate"]["candidate_id"] == candidate.candidate_id
        assert index.metadata["bbo_ref"] == bbo.evidence_ref and index.metadata["bbo_observed_at_ns"] == cutoff
        assert coordinator.on_bar(watch_id, triggered, triggered_feature, event_gate=clear(cutoff), bbo=bbo, mark_index=mark).candidate is None
        acceptance_body = {"status": "ACCEPTED", "watch_id": watch_id, "candidate_ref": candidate.content_hash,
                           "feature_hash": candidate.snapshot_hash, "pipeline_version": "research-only-v1"}
        acceptance_ref = sha256_json(acceptance_body)
        with pytest.raises(ValueError, match="indexed research-pipeline acceptance"):
            coordinator.accept_handoff(watch_id, candidate.content_hash,
                pipeline_acceptance_ref=acceptance_ref, accepted_at_ns=cutoff)
        repository.register_artifact(ArtifactIndexEntryV2(acceptance_ref, "ResearchCandidateAcceptanceV1",
            acceptance_ref, cutoff, cutoff, acceptance_body))
        handed = coordinator.accept_handoff(watch_id, candidate.content_hash,
            pipeline_acceptance_ref=acceptance_ref, accepted_at_ns=cutoff)
        assert handed.state == WatchStateV2.HANDED_OFF and handed.handoff_receipt is not None
        receipt = repository.get_artifact(handed.handoff_receipt)
        assert receipt is not None and receipt.metadata["candidate_ref"] == candidate.content_hash
        assert receipt.metadata["feature_hash"] == candidate.snapshot_hash
        assert repository.schema_version == 1
        for forbidden_type in ("CandidateSetV2", "TradePlanEnvelopeV2", "EvaluationArtifactV2", "Approval", "Reservation", "Order"):
            assert repository.artifact_entries(forbidden_type) == ()
        assert not hasattr(coordinator, "submit_order") and not hasattr(coordinator, "reserve_capital")


def test_s1_blocked_unknown_missing_and_variants(tmp_path) -> None:
    store = store_for_side("LONG")
    join = asof_join(store, KEY, cutoff_ns=SETUP)
    feature = feature_snapshot(join)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)
        assert coordinator.create_watch(join, feature, event_gate=EventGate(EventState.BLOCKED, SETUP, "event", "v1"), universe=universe_at(SETUP)).reason == "EVENT_BLOCKED"
        assert coordinator.create_watch(join, feature, event_gate=EventGate(EventState.UNKNOWN, SETUP, "event", "v1"), universe=universe_at(SETUP)).status == "NOT_ESTIMABLE"
        assert coordinator.create_watch(join, feature, event_gate=clear(SETUP), universe=universe_at(SETUP)).status == "WATCH"
    specs = experiment_specs()
    assert len({spec.policy_hash for spec in specs}) == len(specs)
    assert all(spec.capital_status == "SHADOW_ONLY" for spec in specs)


def test_four_bar_expiry_and_stale_evidence(tmp_path) -> None:
    store = store_for_side("LONG")
    setup_join = asof_join(store, KEY, cutoff_ns=SETUP)
    setup_feature = feature_snapshot(setup_join)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)
        watch = coordinator.create_watch(setup_join, setup_feature, event_gate=clear(SETUP), universe=universe_at(SETUP)).watch
        assert watch is not None
        for offset in range(1, 5):
            next_bar = bar(799 + offset, close="104.9")
            store.append(next_bar)
            join = asof_join(store, KEY, cutoff_ns=next_bar.close_at_ns)
            feature = feature_snapshot(join)
            quote = ExecutableQuote(KEY, Decimal("104.89"), Decimal("104.91"), next_bar.close_at_ns,
                                    next_bar.close_at_ns, sha256_json({"quote": offset}))
            mark = MarkIndexEvidence(KEY, Decimal("104.9"), Decimal("104.9"), next_bar.close_at_ns,
                                     sha256_json({"mark": offset}))
            if offset == 1:
                stale = ExecutableQuote(KEY, quote.bid, quote.ask, next_bar.close_at_ns - 6_000_000_000,
                                         next_bar.close_at_ns - 6_000_000_000, "stale")
                assert coordinator.on_bar(watch.watch_id, join, feature, event_gate=clear(next_bar.close_at_ns),
                                          bbo=stale, mark_index=mark).candidate is None
            result = coordinator.on_bar(watch.watch_id, join, feature, event_gate=clear(next_bar.close_at_ns),
                                        bbo=quote, mark_index=mark)
            if offset < 4:
                assert result.watch is not None and result.watch.state == WatchStateV2.WAITING_FOR_EVENT
            else:
                assert result.reason == "FOUR_BAR_EXPIRY"
                assert result.watch is not None and result.watch.state == WatchStateV2.EXPIRED
        assert repository.get_watch(watch.watch_id).state == WatchStateV2.EXPIRED  # type: ignore[union-attr]


def test_join_boundaries_missing_and_revision(tmp_path) -> None:
    store = store_for_side("LONG")
    at_hour = asof_join(store, KEY, cutoff_ns=SETUP)
    assert at_hour.h1[-1].close_at_ns == SETUP and at_hour.h4[-1].close_at_ns == SETUP
    before = asof_join(store, KEY, cutoff_ns=SETUP - 1)
    assert before.h1[-1].close_at_ns == SETUP - HOUR
    assert before.h4[-1].close_at_ns == SETUP - 4 * HOUR
    assert asof_join(store, KEY, cutoff_ns=SETUP, source_health=health(SETUP, PublicSourceStateV2.STALE)).status == "NOT_ESTIMABLE"
    assert causal_asof_join(store, KEY, cutoff_ns=SETUP).status == "NOT_ESTIMABLE"
    wrong_source = PublicSourceHealthV2("other-source", SETUP, SETUP, PublicSourceStateV2.HEALTHY_CURRENT, "other", "fixture")
    assert asof_join(store, KEY, cutoff_ns=SETUP, source_health=wrong_source).reason == "SOURCE_HEALTH_SOURCE_MISMATCH"
    forming = replace(bar(800, close="105.55"), final=False)
    assert store.append(forming) is False
    assert asof_join(store, KEY, cutoff_ns=SETUP + M15).m15[-1].close_at_ns == SETUP
    other_key = type(KEY)(KEY.venue, KEY.environment, KEY.product, KEY.native_symbol, KEY.base_asset_id,
                          KEY.quote_asset, KEY.settlement_asset, "other-revision")
    assert asof_join(store, other_key, cutoff_ns=SETUP).status == "NOT_ESTIMABLE"
    old_hash = feature_snapshot(at_hour).content_hash
    store.append(bar(800, close="105.55"))
    assert feature_snapshot(asof_join(store, KEY, cutoff_ns=SETUP)).content_hash == old_hash
    trade = CausalTrade(KEY, Decimal("105"), Decimal("2"), SETUP - HOUR, SETUP, "trade-ref")
    future = CausalTrade(KEY, Decimal("999"), Decimal("1"), SETUP + 1, SETUP + 1, "future-trade")
    with_vwap = feature_snapshot(at_hour, trades=(trade,))
    assert with_vwap.values["location.utc_day_trade_vwap"].value == Decimal("105")
    assert "trade-ref" in with_vwap.envelope.input_refs
    assert feature_snapshot(at_hour, trades=(trade, future)).content_hash == with_vwap.content_hash
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        assert S1ShadowCoordinator(repository).create_watch(at_hour, with_vwap, event_gate=clear(SETUP),
            universe=universe_at(SETUP)).reason == "OPTIONAL_INPUT_REQUIRES_POLICY_VARIANT"


def test_setup_rule_failures_and_missing_timeframe(tmp_path) -> None:
    join = asof_join(store_for_side("LONG"), KEY, cutoff_ns=SETUP)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)

        def decision(changed):
            return coordinator.create_watch(changed, feature_snapshot(changed),
                                            event_gate=clear(SETUP), universe=universe_at(SETUP))

        no_h4 = replace(join, h4=(), status="NOT_ESTIMABLE", reason="MISSING_4H")
        assert decision(no_h4).status == "NOT_ESTIMABLE"
        last_h4 = replace(join.h4[-1], close=Decimal("101"), low=Decimal("100"))
        wrong_close = replace(join, h4=join.h4[:-1] + (last_h4,))
        assert decision(wrong_close).reason == "SETUP_RULE_FAILED"
        descending = tuple(bar(i, interval=BarIntervalV2.H4, close=str(Decimal(100) - Decimal(i) / 10)) for i in range(49))
        spike = bar(49, interval=BarIntervalV2.H4, close="100")
        ordering_fail = replace(join, h4=descending + (spike,))
        assert decision(ordering_fail).reason == "SETUP_RULE_FAILED"
        no_touch = tuple(replace(item, low=item.close - Decimal("0.1")) for item in join.h1[-3:])
        assert decision(replace(join, h1=join.h1[:-3] + no_touch)).reason == "SETUP_RULE_FAILED"
        failed_reclaim = replace(join.h1[-1], close=Decimal("102"), low=Decimal("101"))
        assert decision(replace(join, h1=join.h1[:-1] + (failed_reclaim,))).reason == "SETUP_RULE_FAILED"


def test_trigger_rejection_evidence(tmp_path) -> None:
    store = store_for_side("LONG")
    setup = asof_join(store, KEY, cutoff_ns=SETUP)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)
        watch = coordinator.create_watch(setup, feature_snapshot(setup),
                                         event_gate=clear(SETUP), universe=universe_at(SETUP)).watch
        assert watch is not None
        trigger = bar(800, close="105.55")
        store.append(trigger)
        cutoff = trigger.close_at_ns
        join = asof_join(store, KEY, cutoff_ns=cutoff)
        feature = feature_snapshot(join)
        mark = MarkIndexEvidence(KEY, Decimal("105.55"), Decimal("105.55"), cutoff, "mark")

        def check(ask: str, *, age: int = 0, gate: EventGate | None = None):
            bbo = ExecutableQuote(KEY, Decimal(ask) - Decimal("0.01"), Decimal(ask), cutoff - age,
                                  cutoff - age, "bbo")
            return coordinator.on_bar(watch.watch_id, join, feature, event_gate=gate or clear(cutoff),
                                      bbo=bbo, mark_index=mark)

        assert check("105.55", age=6_000_000_000).reason == "BBO_STALE_OR_UNAVAILABLE"
        assert check("103.7").reason == "STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR"
        assert check("110").reason == "STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR"
        assert check("105.55", gate=EventGate(EventState.BLOCKED, cutoff, "blocked", "v1")).reason == "EVENT_BLOCKED"
        assert check("105.55", gate=EventGate(EventState.UNKNOWN, cutoff, "unknown", "v1")).status == "NOT_ESTIMABLE"
        assert repository.get_watch(watch.watch_id).state == WatchStateV2.WAITING_FOR_EVENT  # type: ignore[union-attr]


def test_contrary_confirmed_four_hour_invalidates(tmp_path) -> None:
    store = store_for_side("LONG")
    for i in range(200, 203):
        price = Decimal(100) + Decimal(i - 150) / 10
        store.append(bar(i, interval=BarIntervalV2.H1, close=str(price),
                         low="104.0" if i == 201 else str(price - Decimal("0.3"))))
    for i in range(800, 812):
        store.append(bar(i, close="104.9"))
    setup_at = 203 * HOUR
    join = asof_join(store, KEY, cutoff_ns=setup_at)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S1ShadowCoordinator(repository)
        watch = coordinator.create_watch(join, feature_snapshot(join),
                                         event_gate=clear(setup_at), universe=universe_at(setup_at)).watch
        assert watch is not None
        store.append(bar(50, interval=BarIntervalV2.H4, close="90"))
        store.append(bar(203, interval=BarIntervalV2.H1, close="105.3"))
        for i in range(812, 816):
            store.append(bar(i, close="104.9"))
        cutoff = 204 * HOUR
        new_join = asof_join(store, KEY, cutoff_ns=cutoff)
        result = coordinator.on_bar(watch.watch_id, new_join,
            feature_snapshot(new_join), event_gate=clear(cutoff), bbo=None, mark_index=None)
        assert result.reason == "CONTRARY_CONFIRMED_4H_REGIME"
        assert result.watch is not None and result.watch.state == WatchStateV2.INVALIDATED
