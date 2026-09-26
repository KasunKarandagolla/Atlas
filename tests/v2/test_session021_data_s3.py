"""Session-021 additive bar support and S3 mathematical contracts."""

from __future__ import annotations

import math
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    EligibilityStatusV2,
    FeatureArtifactV2,
    FeatureValueV2,
    OpportunityWatchV2,
    ReplayViewV2,
    V2Side,
    WatchStateV2,
)
from atlas.v2.data.bars import (
    BarIntervalV2,
    CausalBarStoreV2,
    close_boundary_ns,
    translate_final_bar,
)
from atlas.v2.data.binance import BinanceUsdMPublicReaderV2
from atlas.v2.data.binance import translate_kline as translate_binance_kline
from atlas.v2.data.bybit import BybitPublicReaderV2
from atlas.v2.data.bybit import translate_kline as translate_bybit_kline
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.public_http import PublicVenueV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.data.subscriptions import _WATCH_EVENT_CHANNELS
from atlas.v2.data.universe import ComputeTierV2, session021_strategy_history_days_v2, subscription_channels_for_tier
from atlas.v2.instruments import (
    ProductContractV2,
    StrategyEligibilityV2,
    TradingStatusV2,
    UniverseContractV2,
    UniverseEntryV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    AbnormalityEvidenceV2,
    AbnormalityStateV2,
    CalendarCoverageV2,
    EventSafetyGateBuilderV2,
    ScheduledEventV2,
)
from atlas.v2.selection import SELECTION_POLICY_HASH, assemble_candidate_set
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote
from atlas.v2.strategies.s3_mean_reversion import (
    MAX_HOLD_NS,
    POLICY_ID,
    POLICY_VERSION,
    S3_POLICY,
    CausalTradeV2,
    S3ShadowCoordinator,
    TradeVwapSnapshotV2,
    _bar_available_at_view,
    deviation_exceeds_watch_threshold,
    effective_stop_valid,
    fit_ar1,
    moves_toward_frozen_vwap,
    preceding_standardization_residuals,
    round_stop,
    standardized_current,
    strong_trend_rejected,
    utc_day_trade_vwap,
)

from .test_session014_core import KEY

NS = 1_000_000_000


class _RecordingPublicClient:
    def __init__(self, venue: PublicVenueV2) -> None:
        self.venue = venue
        self.calls: list[tuple[str, dict[str, str | int] | None]] = []

    def get(self, path: str, params: dict[str, str | int] | None = None):
        self.calls.append((path, params))
        return None


def test_public_venue_kline_mappings_include_additive_one_and_five_minute_intervals() -> None:
    bybit_client = _RecordingPublicClient(PublicVenueV2.BYBIT)
    binance_client = _RecordingPublicClient(PublicVenueV2.BINANCE)
    bybit = BybitPublicReaderV2(bybit_client)  # type: ignore[arg-type]
    binance = BinanceUsdMPublicReaderV2(binance_client)  # type: ignore[arg-type]

    for interval, bybit_text, binance_text in (
        (BarIntervalV2.M1, "1", "1m"),
        (BarIntervalV2.M5, "5", "5m"),
        (BarIntervalV2.M15, "15", "15m"),
        (BarIntervalV2.H1, "60", "1h"),
        (BarIntervalV2.H4, "240", "4h"),
    ):
        bybit.klines("BTCUSDT", interval)
        binance.klines("BTCUSDT", interval)
        assert bybit_client.calls[-1] == (
            "/v5/market/kline",
            {"category": "linear", "symbol": "BTCUSDT", "interval": bybit_text, "limit": 200},
        )
        assert binance_client.calls[-1] == (
            "/fapi/v1/klines",
            {"symbol": "BTCUSDT", "interval": binance_text, "limit": 200},
        )
H = "a" * 64


def test_intervals_channels_and_bybit_final_versus_forming() -> None:
    assert BarIntervalV2.M1.duration_ns == 60 * NS
    assert BarIntervalV2.M5.duration_ns == 300 * NS
    assert BarIntervalV2.M15.duration_ns == 900 * NS
    assert BarIntervalV2.H1.duration_ns == 3600 * NS
    assert BarIntervalV2.H4.duration_ns == 14400 * NS
    assert _WATCH_EVENT_CHANNELS["BAR_CLOSE_1M"].value == "KLINE_1M"
    assert _WATCH_EVENT_CHANNELS["BAR_CLOSE_5M"].value == "KLINE_5M"
    assert {channel.value for channel in subscription_channels_for_tier(ComputeTierV2.TIER_3)} >= {
        "KLINE_1M", "KLINE_5M", "TRADES", "BOOK_TICKER",
    }
    assert "KLINE_1M" not in {channel.value for channel in subscription_channels_for_tier(ComputeTierV2.TIER_2)}
    assert session021_strategy_history_days_v2() == {
        "S3_VWAP_STAT_MEAN_REVERSION": 7,
        "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH": 30,
    }
    one_minute = BarIntervalV2.M1
    row = ["60000", "100", "101", "99", "100.5", "2", "200"]
    raw, values, opened, final = translate_bybit_kline(
        row, key=KEY, interval=one_minute, received_at_ns=120 * NS,
        server_time_ns=120 * NS, source_id="BYBIT_PUBLIC_HTTP",
    )
    assert final
    assert raw.event_type == "BAR_1M"
    assert close_boundary_ns(opened, one_minute) == 120 * NS
    assert translate_final_bar(raw=raw, interval=one_minute, open_at_ns=opened, values=values, final=final) is not None
    forming_raw, forming_values, forming_open, is_final = translate_bybit_kline(
        row, key=KEY, interval=one_minute, received_at_ns=119 * NS,
        server_time_ns=119 * NS, source_id="BYBIT_PUBLIC_HTTP",
    )
    assert not is_final
    assert translate_final_bar(raw=forming_raw, interval=one_minute, open_at_ns=forming_open,
                               values=forming_values, final=is_final) is None


def test_binance_five_minute_translation_and_append_only_replay_availability() -> None:
    binance_key = replace(KEY, venue=VenueV2.BINANCE, contract_revision=sha256_json({"venue": "BINANCE"}))
    opened_ns = 300 * NS
    row = [300_000, "100", "102", "99", "101", "5", 599_999, "500", 3, "2", "200", "0"]
    raw, values, opened, final = translate_binance_kline(
        row, key=binance_key, interval=BarIntervalV2.M5, received_at_ns=600 * NS,
        server_time_ns=600 * NS, source_id="BINANCE_PUBLIC_HTTP",
    )
    assert final and opened == opened_ns and raw.event_type == "BAR_5M"
    bar = translate_final_bar(raw=raw, interval=BarIntervalV2.M5, open_at_ns=opened,
                              values=values, final=final)
    assert bar is not None and bar.close_at_ns == 600 * NS

    late_actual = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="BYBIT_PUBLIC_HTTP", event_type="BAR_1M",
        event_at_ns=60 * NS, received_at_ns=600 * NS, ingested_at_ns=600 * NS, available_at_ns=600 * NS,
        payload={"close": "100"}, translation_version="fixture", sequence=str(0),
    )
    actual_bar = translate_final_bar(raw=late_actual, interval=BarIntervalV2.M1, open_at_ns=0,
                                     values={"open": "100", "high": "100", "low": "100", "close": "100"},
                                     final=True)
    assert actual_bar is not None
    replay_raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="PUBLIC_ARCHIVE", event_type="BAR_1M",
        event_at_ns=60 * NS, received_at_ns=900 * NS, ingested_at_ns=900 * NS, available_at_ns=900 * NS,
        payload={"close": "100"}, translation_version="fixture", sequence=str(0),
        availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET, replay_available_at_ns=65 * NS,
    )
    replay_bar = translate_final_bar(raw=replay_raw, interval=BarIntervalV2.M1, open_at_ns=0,
                                     values={"open": "100", "high": "100", "low": "100", "close": "100"},
                                     final=True)
    assert replay_bar is not None
    actual_store, replay_store = CausalBarStoreV2(), CausalBarStoreV2()
    assert actual_store.append(actual_bar)
    assert replay_store.append(replay_bar)
    assert actual_store.as_of(KEY.contract_revision, BarIntervalV2.M1, information_cutoff_ns=100 * NS,
                              availability_class=AvailabilityClassV2.ACTUAL_SYSTEM) == ()
    corrected_raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="BYBIT_PUBLIC_HTTP", event_type="BAR_1M",
        event_at_ns=60 * NS, received_at_ns=700 * NS, ingested_at_ns=700 * NS, available_at_ns=700 * NS,
        payload={"close": "100.25"}, translation_version="fixture-correction", sequence="m1-correction",
        revision_of=actual_bar.raw.record_id,
    )
    corrected_bar = translate_final_bar(
        raw=corrected_raw, interval=BarIntervalV2.M1, open_at_ns=0,
        values={"open": "100", "high": "100.25", "low": "100", "close": "100.25"}, final=True,
    )
    assert corrected_bar is not None and actual_store.append(corrected_bar)
    assert actual_store.as_of(KEY.contract_revision, BarIntervalV2.M1, information_cutoff_ns=650 * NS,
                              availability_class=AvailabilityClassV2.ACTUAL_SYSTEM) == (actual_bar,)
    assert actual_store.as_of(KEY.contract_revision, BarIntervalV2.M1, information_cutoff_ns=800 * NS,
                              availability_class=AvailabilityClassV2.ACTUAL_SYSTEM) == (corrected_bar,)
    assert replay_store.as_of(KEY.contract_revision, BarIntervalV2.M1, information_cutoff_ns=70 * NS,
                              availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET) == (replay_bar,)
    assert actual_store.as_of(KEY.contract_revision, BarIntervalV2.M1, information_cutoff_ns=700 * NS,
                              availability_class=AvailabilityClassV2.ACTUAL_SYSTEM) == (corrected_bar,)
    assert _bar_available_at_view(replay_bar, 70 * NS, "RECONSTRUCTED_MARKET")
    assert not _bar_available_at_view(replay_bar, 700 * NS, "ACTUAL_SYSTEM")


def test_s3_exact_120_preceding_observation_standardization_and_strict_watch_edges() -> None:
    history = tuple(math.sin(index / 9) + index / 10_000 for index in range(120))
    score, center, sigma = standardized_current(history, 2.0)
    assert center == pytest.approx(sum(history) / 120)
    assert score == pytest.approx((2 - center) / sigma)
    assert sigma > 0
    with pytest.raises(ValueError, match="exactly 120"):
        standardized_current(history[:-1], 2)
    with pytest.raises(ValueError, match="exactly 120"):
        standardized_current((*history, 5), 2)
    assert not deviation_exceeds_watch_threshold(2.0)
    assert not deviation_exceeds_watch_threshold(-2.0)
    assert deviation_exceeds_watch_threshold(2.000001)
    assert deviation_exceeds_watch_threshold(-2.000001)
    assert not deviation_exceeds_watch_threshold(float("nan"))


def test_s3_preceding_standardization_window_excludes_latest_and_uses_cutoff_prefix() -> None:
    from atlas.v2.strategies.s3_mean_reversion import ResidualObservationV2

    items = tuple(ResidualObservationV2(KEY, sha256_json({"bar": index}), H, index * 60, index * 60,
                                         float(index), "ACTUAL_SYSTEM") for index in range(122))
    window = preceding_standardization_residuals(items[:121])
    assert window == tuple(float(index) for index in range(120))
    assert 120.0 not in window
    cutoff_prefix = items[:121]
    with_future_tail = items[:122]
    assert preceding_standardization_residuals(cutoff_prefix) == preceding_standardization_residuals(
        with_future_tail[:121]
    )


@pytest.mark.parametrize("phi", [0.0, -0.2, 1.0, 1.01])
def test_s3_ar1_rejects_phi_outside_open_interval(phi: float) -> None:
    values = [1.0]
    for _ in range(12):
        values.append(phi * values[-1] + (0.1 if phi == 1.0 else 0.0))
    with pytest.raises(ValueError, match="phi"):
        fit_ar1(values)


@pytest.mark.parametrize("minutes", [5.0, 30.0])
def test_s3_ar1_half_life_inclusive_boundaries(minutes: float) -> None:
    phi = math.exp(-math.log(2.0) / minutes)
    values = [phi**index for index in range(40)]
    _, actual_phi, half_life = fit_ar1(values)
    assert actual_phi == pytest.approx(phi, abs=1e-9)
    assert half_life == pytest.approx(minutes, abs=1e-8)


def test_s3_adverse_stops_use_conservative_tick_rounding() -> None:
    long_stop = round_stop(Decimal("100"), 0.02, V2Side.LONG, Decimal("0.1"))
    short_stop = round_stop(Decimal("100"), 0.02, V2Side.SHORT, Decimal("0.1"))
    assert long_stop == Decimal("98.0")
    assert short_stop == Decimal("102.1")
    with pytest.raises(ValueError, match="positive and finite"):
        round_stop(Decimal("100"), 0.0, V2Side.LONG, Decimal("0.1"))
    with pytest.raises(ValueError, match="positive and finite"):
        round_stop(Decimal("100"), 0.02, V2Side.LONG, Decimal("0"))
    assert effective_stop_valid(Decimal("100.2"), Decimal("100.1"), Decimal("0.1"), V2Side.LONG)
    assert not effective_stop_valid(Decimal("100.05"), Decimal("100"), Decimal("0.1"), V2Side.LONG)
    assert not effective_stop_valid(Decimal("100"), Decimal("100.1"), Decimal("0.1"), V2Side.LONG)


def test_s3_later_close_must_move_strictly_toward_frozen_vwap_and_adx_rejection_is_strict() -> None:
    assert moves_toward_frozen_vwap(0.03, 0.02)
    assert moves_toward_frozen_vwap(-0.03, -0.02)
    assert not moves_toward_frozen_vwap(0.03, 0.03)
    assert not moves_toward_frozen_vwap(0.03, 0.04)
    assert strong_trend_rejected(25.0001)
    assert not strong_trend_rejected(25.0)


def test_s3_trade_vwap_is_trade_only_and_actual_or_replay_availability_is_explicit() -> None:
    health = PublicSourceHealthV2("PUBLIC_TRADES", 100, 100,
                                  PublicSourceStateV2.HEALTHY_CURRENT, H, "current")
    trade = CausalTradeV2(KEY, sha256_json({"raw": 1}), "PUBLIC_TRADES", "trade-1", 90, 100, 100,
                          Decimal("10"), Decimal("2"), "BUY")
    snapshot = utc_day_trade_vwap((trade,), key=KEY, cutoff_ns=100, source_health=health)
    assert snapshot is not None and snapshot.vwap == Decimal("10")
    assert snapshot.trade_refs == (trade.raw_observation_ref,)
    assert utc_day_trade_vwap((), key=KEY, cutoff_ns=100, source_health=health) is None
    stale = replace(health, observed_at_ns=1, available_at_ns=1)
    assert utc_day_trade_vwap((trade,), key=KEY, cutoff_ns=61 * NS + 100,
                              source_health=stale) is None

    replay_trade = CausalTradeV2(
        KEY, sha256_json({"raw": 2}), "PUBLIC_TRADES", "trade-replay", 90, 1_000, 1_000,
        Decimal("12"), Decimal("1"), None, AvailabilityClassV2.RECONSTRUCTED_MARKET, 95,
    )
    replay_health = PublicSourceHealthV2("PUBLIC_TRADES", 96, 96,
                                         PublicSourceStateV2.HEALTHY_CURRENT, H, "replay")
    assert utc_day_trade_vwap((replay_trade,), key=KEY, cutoff_ns=100,
                              source_health=replay_health, replay_view="ACTUAL_SYSTEM") is None
    replay = utc_day_trade_vwap((replay_trade,), key=KEY, cutoff_ns=100,
                                source_health=replay_health, replay_view="RECONSTRUCTED_MARKET")
    assert replay is not None and replay.vwap == Decimal("12") and replay.available_at_ns == 95


def test_s3_subsequent_reversion_emits_unsized_candidate_with_frozen_vwap_and_selector_zero(tmp_path) -> None:
    cutoff = 600 * NS
    setup_cutoff = cutoff - BarIntervalV2.M1.duration_ns
    source_health = PublicSourceHealthV2("PUBLIC_BARS", cutoff, cutoff,
                                         PublicSourceStateV2.HEALTHY_CURRENT, H, "fixture")
    trade_health = PublicSourceHealthV2("PUBLIC_TRADES", setup_cutoff, setup_cutoff,
                                         PublicSourceStateV2.HEALTHY_CURRENT, sha256_json({"trade-health": 1}), "fixture")
    trade_ref = sha256_json({"historical-trade": 1})
    vwap = TradeVwapSnapshotV2(KEY, 0, setup_cutoff, setup_cutoff, Decimal("100"),
                               (trade_ref,), trade_health.content_hash)
    setup_ref = sha256_json({"s3-setup": "positive residual"})
    setup_body = {"status": "WATCH", "residual": 0.1, "frozen_vwap": "100",
                  "residual_sigma": 0.01}
    feature_envelope = ArtifactEnvelope(1, "s3-feature", cutoff, cutoff, "fixture", ())
    feature = FeatureArtifactV2(feature_envelope, KEY, "S3_CONTEXT_V1", cutoff, cutoff, FrozenMap({
        "s3.research_state": FeatureValueV2(1.0, "FLAG", None),
    }), source_health.content_hash, ReplayViewV2.ACTUAL_SYSTEM)
    product = ProductContractV2(KEY, setup_cutoff, setup_cutoff, setup_cutoff,
                                Decimal("1"), Decimal("0.1"), Decimal("0.01"), Decimal("0"),
                                TradingStatusV2.TRADING, sha256_json({"product": KEY.to_dict()}))
    product_ref = product.content_hash
    entry = UniverseEntryV2(
        KEY, product_ref, True, True, False, False, False,
        FrozenMap({POLICY_ID: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE)}), (),
    )
    universe = UniverseContractV2(
        ArtifactEnvelope(1, "s3-universe", cutoff, cutoff, "fixture", (product_ref,)),
        "session021", cutoff, SELECTION_POLICY_HASH, (entry,),
    )
    quote_ref = sha256_json({"bbo": cutoff})
    quote = ExecutableQuote(KEY, Decimal("100.0"), Decimal("100.1"), cutoff - NS // 2, cutoff, quote_ref)
    open_at = cutoff - BarIntervalV2.M1.duration_ns
    trigger_raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id=source_health.source_id,
        event_type="BAR_1M", event_at_ns=cutoff, received_at_ns=cutoff, ingested_at_ns=cutoff,
        available_at_ns=cutoff, payload={"trigger": 1}, translation_version="s3-test",
        sequence=str(open_at),
    )
    trigger = translate_final_bar(
        raw=trigger_raw, interval=BarIntervalV2.M1, open_at_ns=open_at,
        values={"open": "100.2", "high": "100.2", "low": "100.0", "close": "100.05"},
        final=True,
    )
    assert trigger is not None
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        calendar_source_ref = sha256_json({"calendar-source": cutoff})
        abnormality_source_ref = sha256_json({"abnormality-source": cutoff})
        repository.register_artifact(ArtifactIndexEntryV2(
            calendar_source_ref, "CalendarSourceFixtureV2", calendar_source_ref,
            cutoff, cutoff, {"ref": calendar_source_ref},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            abnormality_source_ref, "AbnormalitySourceFixtureV2", abnormality_source_ref,
            cutoff, cutoff, {"ref": abnormality_source_ref},
        ))
        coverage = CalendarCoverageV2(
            "SCHEDULE_FIXTURE", 0, cutoff + BarIntervalV2.H4.duration_ns,
            cutoff, cutoff, cutoff, True, "schedule-r1", calendar_source_ref, "VERIFIED",
        )
        abnormality = AbnormalityEvidenceV2(
            AbnormalityStateV2.NORMAL, cutoff, cutoff, abnormality_source_ref,
        )
        safety_gate = EventSafetyGateBuilderV2(repository).evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(),
            abnormality=abnormality, incidents=(),
        )
        event_gate = safety_gate.to_s1_event_gate()
        gate_ref = safety_gate.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(
            vwap.content_hash, "S3TradeVwapSnapshotV2", vwap.content_hash,
            setup_cutoff, setup_cutoff, {"vwap": vwap.to_dict()},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            setup_ref, "S3MeanReversionStateV2", setup_ref, setup_cutoff, setup_cutoff,
            {"state": setup_body},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            feature.content_hash, "FeatureArtifactV2", feature.content_hash,
            cutoff, cutoff, {"feature": feature.to_dict()},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            universe.content_hash, "UniverseContractV2", universe.content_hash,
            cutoff, cutoff, {"universe": universe.to_dict()},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            trigger.content_hash, "CausalBarV2", trigger.content_hash,
            cutoff, cutoff, {"bar": trigger.to_dict()},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            quote_ref, "ExecutableQuoteFixtureV2", quote_ref, cutoff, cutoff, {"bbo": "fixture"},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            source_health.content_hash, "PublicSourceHealthV2", source_health.content_hash,
            cutoff, cutoff, {"health": source_health.to_dict()},
        ))
        repository.register_artifact(ArtifactIndexEntryV2(
            trade_health.content_hash, "PublicSourceHealthV2", trade_health.content_hash,
            setup_cutoff, setup_cutoff, {"health": trade_health.to_dict()},
        ))
        watch_id = sha256_json({"watch": "S3 setup"})
        watch = OpportunityWatchV2(
            watch_id, KEY, POLICY_ID, POLICY_VERSION, S3_POLICY.policy_hash,
            WatchStateV2.DETECTED, 0, setup_cutoff, setup_cutoff, setup_ref,
            tuple(sorted({setup_ref, vwap.content_hash, feature.content_hash,
                          universe.content_hash, gate_ref, quote_ref, trade_health.content_hash})),
            "BAR_CLOSE_1M", setup_cutoff + MAX_HOLD_NS, setup_cutoff,
        )
        repository.create_watch(watch)
        waiting = repository.transition_watch(
            watch_id, expected_state_version=0, event_id=sha256_json({"watch": watch_id, "wait": 1}),
            event_at_ns=setup_cutoff, transition_at_ns=setup_cutoff,
            target_state=WatchStateV2.WAITING_FOR_EVENT,
            outbox_id=sha256_json({"watch": watch_id, "outbox": 1}),
        ).watch
        assert waiting.state == WatchStateV2.WAITING_FOR_EVENT
        coordinator = S3ShadowCoordinator(repository)
        invalid_gate = EventGate(EventState.CLEAR, cutoff, sha256_json({"unpersisted-gate": cutoff}),
                                 "EVENT_SAFETY_GATE_V2_1")
        assert not coordinator._event_gate_is_current_s7_projection(invalid_gate, cutoff)
        calendar_event_ref = sha256_json({"scheduled-event-source": cutoff})
        repository.register_artifact(ArtifactIndexEntryV2(
            calendar_event_ref, "ScheduledEventSourceFixtureV2", calendar_event_ref,
            cutoff, cutoff, {"ref": calendar_event_ref},
        ))
        scheduled = ScheduledEventV2(
            sha256_json({"scheduled-event": cutoff}), "US_CPI", cutoff, "schedule-r1",
            "SCHEDULE_FIXTURE", cutoff - 2, cutoff - 1, cutoff, calendar_event_ref,
        )
        blocked_safety_gate = EventSafetyGateBuilderV2(repository).evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(scheduled,),
            abnormality=abnormality, incidents=(),
        )
        blocked_projection = blocked_safety_gate.to_s1_event_gate()
        assert coordinator._event_gate_is_current_s7_projection(blocked_projection, cutoff)
        blocked = coordinator.on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=vwap, residual_sigma=0.01,
            quote=quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=blocked_projection, bar_health=source_health,
        )
        assert blocked.reason == "EVENT_GATE_UNKNOWN_OR_BLOCKED"
        unknown = coordinator.on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=vwap, residual_sigma=0.01,
            quote=quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=EventGate(EventState.UNKNOWN, cutoff, H, "EVENT_SAFETY_GATE_V2_1"),
            bar_health=source_health,
        )
        assert unknown.reason == "EVENT_GATE_UNKNOWN_OR_BLOCKED"
        stale_quote = replace(quote, observed_at_ns=cutoff - 2 * NS, available_at_ns=cutoff - 2 * NS)
        stale_bbo = coordinator.on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=vwap, residual_sigma=0.01,
            quote=stale_quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=event_gate, bar_health=source_health,
        )
        assert stale_bbo.reason == "BBO_STALE_OR_UNAVAILABLE"
        future_vwap = TradeVwapSnapshotV2(
            KEY, 0, cutoff, cutoff, Decimal("99.9"), (sha256_json({"future-trade": 1}),),
            trade_health.content_hash,
        )
        moved_target = coordinator.on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=future_vwap, residual_sigma=0.01,
            quote=quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=event_gate, bar_health=source_health,
        )
        assert moved_target.reason == "FROZEN_ENTRY_VWAP_MISMATCH"
        result = S3ShadowCoordinator(repository).on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=vwap, residual_sigma=0.01,
            quote=quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=event_gate,
            bar_health=source_health,
        )
        assert result.status == "CANDIDATE" and result.candidate is not None
        assert result.candidate.side == V2Side.SHORT and result.candidate.quantity is None
        assert result.candidate.stop_price == Decimal("101.1")
        assert result.candidate.entry_collar == Decimal("100.0")
        assert result.candidate.horizon_end_ns == cutoff + MAX_HOLD_NS
        assert result.watch is not None and result.watch.state == WatchStateV2.HANDED_OFF
        stored = repository.get_artifact(result.candidate.content_hash)
        assert stored is not None and stored.metadata["selector_influence"] == "ZERO"
        with pytest.raises(ValueError, match="candidate policy identity mismatch"):
            assemble_candidate_set(
                repository, universe=universe, decision_event_id="session021-selector-boundary",
                cutoff_ns=cutoff, candidates=(result.candidate,),
                policies={S3_POLICY.policy_hash: S3_POLICY}, scanner_evidence_refs={},
            )
        duplicate = S3ShadowCoordinator(repository).on_subsequent_bar(
            watch_id=watch_id, trigger=trigger, cutoff_ns=cutoff, frozen_vwap=vwap, residual_sigma=0.01,
            quote=quote, tick_size=Decimal("0.1"), feature=feature, universe=universe,
            event_gate=event_gate,
            bar_health=source_health,
        )
        assert duplicate.candidate is None
        assert duplicate.reason == "DUPLICATE_TRIGGER"
        assert len(repository.artifact_entries("CandidateActionV2")) == 1
