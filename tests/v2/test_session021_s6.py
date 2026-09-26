"""Deterministic S6 breadth, synchronization, rank and exact-action tests."""

from __future__ import annotations

import math
from decimal import Decimal
from functools import lru_cache

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, EligibilityStatusV2, OpportunityWatchV2, V2Side, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2, close_boundary_ns
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductTypeV2,
    StrategyEligibilityV2,
    UniverseContractV2,
    UniverseEntryV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.selection import SELECTION_POLICY_HASH
from atlas.v2.strategies.s6_cross_section import (
    POLICY_ID,
    S6_POLICY,
    LiquidityFundingEvidenceV2,
    S6ShadowCoordinator,
    _bar_available,
)

NS = 1_000_000_000
HOUR = BarIntervalV2.H1.duration_ns
H4 = BarIntervalV2.H4.duration_ns
M15 = BarIntervalV2.M15.duration_ns
CUTOFF = 1_800_000_000_000_000_000
CUTOFF -= CUTOFF % H4


def _keys(count: int = 20) -> tuple[InstrumentKeyV2, ...]:
    symbols = ["BTCUSDT", *(f"A{index:02d}USDT" for index in range(1, count))]
    result = []
    for symbol in symbols:
        base = symbol[:-4]
        result.append(InstrumentKeyV2(
            VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
            symbol, base, "USDT", "USDT", sha256_json({"symbol": symbol, "venue": "BYBIT"}),
        ))
    return tuple(result)


def _universe(keys: tuple[InstrumentKeyV2, ...]) -> UniverseContractV2:
    entries = []
    refs = []
    for key in keys:
        product_ref = sha256_json({"product": key.to_dict()})
        refs.append(product_ref)
        entries.append(UniverseEntryV2(
            key, product_ref, True, True, False, False, False,
            FrozenMap({POLICY_ID: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE)}), (),
        ))
    entries.sort(key=lambda entry: entry.key.to_canonical_json())
    return UniverseContractV2(
        ArtifactEnvelope(1, "session021-universe", CUTOFF, CUTOFF, "test-universe", tuple(sorted(refs))),
        "session021-universe-v1", CUTOFF, SELECTION_POLICY_HASH, tuple(entries),
    )


def _bar(key: InstrumentKeyV2, interval: BarIntervalV2, opened: int,
         price: float, *, volume: float = 10.0, source: str = "PUBLIC_BARS") -> CausalBarV2:
    close_at = close_boundary_ns(opened, interval)
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision, source_id=source,
        event_type=f"BAR_{interval.value}", event_at_ns=close_at,
        received_at_ns=close_at, ingested_at_ns=close_at, available_at_ns=close_at,
        payload={"open": price, "close": price, "volume": volume},
        translation_version="session021-fixture", sequence=str(opened),
    )
    px = Decimal(str(price))
    return CausalBarV2(raw, interval, opened, close_at, px, px, px, px, Decimal(str(volume)), True)


@lru_cache(maxsize=4)
def _market(keys: tuple[InstrumentKeyV2, ...], *, constant_btc: bool = False):
    end = CUTOFF
    start_close = end - 30 * 24 * HOUR
    hourly: dict[InstrumentKeyV2, tuple[CausalBarV2, ...]] = {}
    four_hour: dict[InstrumentKeyV2, tuple[CausalBarV2, ...]] = {}
    evidence = {}
    health = PublicSourceHealthV2("SESSION021_LIQUIDITY_FUNDING", CUTOFF, CUTOFF,
                                  PublicSourceStateV2.HEALTHY_CURRENT, sha256_json({"health": CUTOFF}), "fixture")
    for key_index, key in enumerate(keys):
        closes: list[CausalBarV2] = []
        price = 100.0
        for index in range(721):
            if key_index == 0:
                hourly_return = 0.0 if constant_btc else 0.0002 * math.sin(index * 0.13) + 0.00003 * math.cos(index * 0.07)
            else:
                noise_key = key_index if key_index not in (6,) else 5
                noise = 0.00008 * math.sin(index * 0.19 + noise_key * 0.41)
                if index >= 717:
                    noise += 0.0018 if key_index in (1, 2) else -0.0018 if key_index in (18, 19) else 0
                btc_return = 0.0 if constant_btc else 0.0002 * math.sin(index * 0.13) + 0.00003 * math.cos(index * 0.07)
                hourly_return = 0.8 * btc_return + noise
            price *= math.exp(hourly_return)
            close_at = start_close + index * HOUR
            opened = close_at - HOUR
            closes.append(_bar(key, BarIntervalV2.H1, opened, price))
        hourly[key] = tuple(closes)
        h4_end = CUTOFF - CUTOFF % H4
        trend_bars: list[CausalBarV2] = []
        for index in range(50):
            trend_price: float
            if key_index in (1, 2, 0):
                trend_price = 100 + index
            elif key_index in (18, 19):
                trend_price = 150 - index
            else:
                trend_price = 100 + math.sin(index * 0.3)
            close_at = h4_end - (49 - index) * H4
            trend_bars.append(_bar(key, BarIntervalV2.H4, close_at - H4, float(trend_price)))
        four_hour[key] = tuple(trend_bars)
        evidence[key] = LiquidityFundingEvidenceV2(
            key, CUTOFF, CUTOFF, Decimal("3"), Decimal("20000000"), Decimal("0.0001"),
            sha256_json({"liquidity": key.to_dict()}), sha256_json({"funding": key.to_dict()}), health,
        )
    return hourly, four_hour, evidence


def _evaluate(tmp_path, keys, *, hourly=None, four_hour=None, evidence=None):
    if hourly is None or four_hour is None or evidence is None:
        hourly, four_hour, evidence = _market(keys)
    tmp_path.mkdir(parents=True, exist_ok=True)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        _seed_evidence(repository, evidence)
        result = S6ShadowCoordinator(repository).evaluate(
            universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0],
            hourly_bars=hourly, four_hour_bars=four_hour, evidence=evidence,
        )
        return result, repository


def _seed_evidence(repository: OpsRepository, evidence) -> None:
    for item in evidence.values():
        for ref, kind in ((item.liquidity_ref, "LiquiditySourceFixtureV2"),
                          (item.funding_ref, "FundingSourceFixtureV2")):
            repository.register_artifact(ArtifactIndexEntryV2(
                ref, kind, ref, item.available_at_ns, item.available_at_ns, {"evidence_ref": ref},
            ))


def test_s6_requires_20_point_in_time_strategy_eligible_instruments(tmp_path) -> None:
    keys = _keys(19)
    result, _ = _evaluate(tmp_path, keys, hourly={}, four_hour={}, evidence={})
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "INSUFFICIENT_STRATEGY_ELIGIBLE_BREADTH_LT_20"
    assert result.state.eligible_breadth == 19
    assert result.state.replay_view == "ACTUAL_SYSTEM"


def test_s6_market_availability_view_excludes_reconstructed_bars_from_actual_system() -> None:
    key = _keys(1)[0]
    actual = _bar(key, BarIntervalV2.H1, CUTOFF - HOUR, 100)
    replay_raw = RawObservationV2.build(
        instrument_revision=key.contract_revision, source_id="PUBLIC_ARCHIVE", event_type="BAR_1H",
        event_at_ns=CUTOFF, received_at_ns=CUTOFF + NS, ingested_at_ns=CUTOFF + NS,
        available_at_ns=CUTOFF + NS, payload={"open": 100, "high": 100, "low": 100, "close": 100},
        translation_version="session021-replay", sequence="replay",
        availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET, replay_available_at_ns=CUTOFF,
    )
    replay = CausalBarV2(replay_raw, BarIntervalV2.H1, CUTOFF - HOUR, CUTOFF,
                         actual.open, actual.high, actual.low, actual.close, actual.volume, True)
    assert _bar_available(actual, CUTOFF, "ACTUAL_SYSTEM")
    assert not _bar_available(replay, CUTOFF, "ACTUAL_SYSTEM")
    assert _bar_available(replay, CUTOFF, "RECONSTRUCTED_MARKET")


def test_s6_20_instruments_calculate_ranked_betas_scores_and_deciles(tmp_path) -> None:
    keys = _keys(20)
    hourly, four_hour, evidence = _market(keys)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S6ShadowCoordinator(repository)
        _seed_evidence(repository, evidence)
        result = coordinator.evaluate(universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0],
                                      hourly_bars=hourly, four_hour_bars=four_hour, evidence=evidence)
        assert result.status == "AVAILABLE"
        assert result.state.eligible_breadth == 20
        assert result.state.decile_size == 2
        scored = [row for row in result.state.rows if row.eligibility == "ELIGIBLE"]
        assert len(scored) == 19
        assert sum(row.decile == "TOP" for row in scored) == 2
        assert sum(row.decile == "BOTTOM" for row in scored) == 2
        assert all(row.beta_btc is not None for row in scored)
        assert all(row.residual_volatility is not None and row.residual_volatility > 0 for row in scored)
        assert len(result.hypotheses) == 4
        equal_rows = [row for row in scored if row.key in (keys[5], keys[6])]
        assert equal_rows[0].score is not None and equal_rows[0].score == equal_rows[1].score
        assert equal_rows[0].rank is not None and equal_rows[1].rank is not None
        assert equal_rows[0].rank < equal_rows[1].rank
        assert all(repository.get_artifact(item.hypothesis_id) is not None for item in result.hypotheses)
        assert all(item.to_dict()["exact_action_status"] == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
                   for item in result.hypotheses)
        one_missing = dict(evidence)
        one_missing[keys[3]] = LiquidityFundingEvidenceV2(
            keys[3], CUTOFF, CUTOFF, Decimal("3"), Decimal("20000000"), Decimal("0.0001"),
            sha256_json({"missing-liquidity": keys[3].to_dict()}), evidence[keys[3]].funding_ref,
            evidence[keys[3]].source_health,
        )
        with OpsRepository(tmp_path / "missing-source.sqlite") as missing_repository:
            _seed_evidence(missing_repository, evidence)
            missing_result = S6ShadowCoordinator(missing_repository).evaluate(
                universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0],
                hourly_bars=hourly, four_hour_bars=four_hour, evidence=one_missing,
            )
            missing_row = next(row for row in missing_result.state.rows if row.key == keys[3])
            assert missing_row.eligibility == "EXCLUDED"
            assert missing_row.reason == "FUNDING_OR_LIQUIDITY_EVIDENCE_MISSING_OR_STALE"


def test_s6_exact_hourly_intersection_missing_peer_btc_and_zero_variance_are_named(tmp_path) -> None:
    keys = _keys(20)
    hourly, four_hour, evidence = _market(keys)
    broken = dict(hourly)
    broken[keys[1]] = tuple(bar for bar in broken[keys[1]] if bar.close_at_ns != CUTOFF - HOUR)
    with OpsRepository(tmp_path / "missing.sqlite") as repository:
        _seed_evidence(repository, evidence)
        result = S6ShadowCoordinator(repository).evaluate(
            universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0], hourly_bars=broken,
            four_hour_bars=four_hour, evidence=evidence,
        )
        missing = next(row for row in result.state.rows if row.key == keys[1])
        assert missing.reason == "MISSING_PEER_HISTORY_OR_SYNCHRONIZED_HOURLY_INTERSECTION"
        assert result.status == "NOT_ESTIMABLE"
    with OpsRepository(tmp_path / "btc-missing.sqlite") as repository:
        result = S6ShadowCoordinator(repository).evaluate(
            universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0], hourly_bars={},
            four_hour_bars={}, evidence={},
        )
        assert result.reason == "BTC_PROXY_UNAVAILABLE"
    flat_btc = tuple(_bar(keys[0], BarIntervalV2.H1, CUTOFF - 30 * 24 * HOUR - HOUR + index * HOUR,
                           100.0) for index in range(721))
    with OpsRepository(tmp_path / "btc-flat.sqlite") as repository:
        result = S6ShadowCoordinator(repository).evaluate(
            universe=_universe(keys), cutoff_ns=CUTOFF, btc_proxy=keys[0],
            hourly_bars={keys[0]: flat_btc}, four_hour_bars={}, evidence={},
        )
        assert result.reason == "BTC_PROXY_ZERO_OR_INVALID_VARIANCE"


def test_s6_late_appends_do_not_change_earlier_cutoff_state_or_full_key_rank(tmp_path) -> None:
    keys = _keys(20)
    hourly, four_hour, evidence = _market(keys)
    base, _ = _evaluate(tmp_path / "base", keys, hourly=hourly, four_hour=four_hour, evidence=evidence)
    extended = dict(hourly)
    for key in keys:
        extended[key] = (*hourly[key], _bar(key, BarIntervalV2.H1, CUTOFF, 1_000.0))
    extended_h4 = dict(four_hour)
    for key in keys:
        extended_h4[key] = (*four_hour[key], _bar(key, BarIntervalV2.H4, CUTOFF, 1_000.0))
    tail, _ = _evaluate(tmp_path / "tail", keys, hourly=extended, four_hour=extended_h4, evidence=evidence)
    assert tail.state.content_hash == base.state.content_hash
    assert [row.key.to_dict() for row in tail.state.rows] == [row.key.to_dict() for row in base.state.rows]
    assert not any(entry.capital_eligible for entry in _universe(keys).entries)


def test_s6_subsequent_trigger_stops_at_unfrozen_stop_contract(tmp_path) -> None:
    keys = _keys(20)
    from atlas.v2.strategies.s6_cross_section import S6HypothesisV2

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coordinator = S6ShadowCoordinator(repository)
        hypothesis_id = sha256_json({"session021": "hypothesis"})
        hypothesis = S6HypothesisV2(hypothesis_id, keys[1], S6_POLICY.policy_hash,
                                    sha256_json({"state": 1}), V2Side.LONG, CUTOFF, 1.0, 0.5,
                                    "FIXTURE", sha256_json({"watch": 1}), "ACTUAL_SYSTEM")
        repository.register_artifact(ArtifactIndexEntryV2(
            hypothesis_id, "S6HypothesisV2", hypothesis_id, CUTOFF, CUTOFF,
            {"hypothesis": hypothesis.to_dict()},
        ))
        watch = OpportunityWatchV2(
            hypothesis.watch_id, hypothesis.key, POLICY_ID, "1.0.0-shadow-research",
            S6_POLICY.policy_hash, WatchStateV2.DETECTED, 0, CUTOFF, CUTOFF, hypothesis_id,
            tuple(sorted({hypothesis_id, hypothesis.state_ref})), "BAR_CLOSE_15M",
            CUTOFF + M15 + 1, CUTOFF,
        )
        repository.create_watch(watch)
        waiting = repository.transition_watch(
            hypothesis.watch_id, expected_state_version=0,
            event_id=sha256_json({"watch": hypothesis.watch_id, "wait": 1}),
            event_at_ns=CUTOFF, transition_at_ns=CUTOFF,
            target_state=WatchStateV2.WAITING_FOR_EVENT,
            outbox_id=sha256_json({"watch": hypothesis.watch_id, "outbox": 1}),
        ).watch
        assert waiting.state == WatchStateV2.WAITING_FOR_EVENT
        prior = _bar(hypothesis.key, BarIntervalV2.M15, CUTOFF - M15, 100.0)
        trigger = _bar(hypothesis.key, BarIntervalV2.M15, CUTOFF, 102.0)
        state, reason = coordinator.confirm_trigger(hypothesis_id=hypothesis.hypothesis_id,
                                                   previous_15m=prior, trigger_15m=trigger,
                                                   cutoff_ns=CUTOFF + M15)
        assert state == "NOT_ESTIMABLE"
        assert reason == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
        assert len(repository.artifact_entries("S6ConfirmedTriggerV2")) == 1
        stored_watch = repository.get_watch(hypothesis.watch_id)
        assert stored_watch is not None and stored_watch.state == WatchStateV2.CONFIRMED
