"""Causal, bounded and read-only H1 peer relationship context."""

from __future__ import annotations

from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.features.peer_context import (
    PeerRelationshipCache,
    PeerRelationshipRequestV1,
    build_peer_relationship,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.strategies.s6_cross_section import S6_POLICY, S6ShadowCoordinator

from .test_session021_s6 import CUTOFF, HOUR, _bar, _keys, _market, _seed_evidence, _universe


@pytest.fixture(scope="module")
def peer_fixture(tmp_path_factory):
    keys = _keys(20)
    universe = _universe(keys)
    hourly, four_hour, evidence = _market(keys)
    path = tmp_path_factory.mktemp("s46-peer-s6") / "ops.sqlite"
    with OpsRepository(path) as repository:
        _seed_evidence(repository, evidence)
        decision = S6ShadowCoordinator(repository).evaluate(
            universe=universe,
            cutoff_ns=CUTOFF,
            btc_proxy=keys[0],
            hourly_bars=hourly,
            four_hour_bars=four_hour,
            evidence=evidence,
        )
    assert decision.state.status == "AVAILABLE"
    return {"keys": keys, "universe": universe, "hourly": hourly, "four_hour": four_hour,
            "evidence": evidence, "state": decision.state}


def _request(peer_fixture, *, anchor_index=1, peer_index=2, anchor_bars=None, peer_bars=None,
             btc_bars=None, state=None, computed_offset=1, available_offset=2,
             replay_view="ACTUAL_SYSTEM", lookback_returns=720):
    keys = peer_fixture["keys"]
    return PeerRelationshipRequestV1(
        anchor_key=keys[anchor_index],
        peer_key=keys[peer_index],
        btc_proxy_key=keys[0],
        universe=peer_fixture["universe"],
        s6_state=state or peer_fixture["state"],
        anchor_bars=tuple(anchor_bars if anchor_bars is not None else peer_fixture["hourly"][keys[anchor_index]]),
        peer_bars=tuple(peer_bars if peer_bars is not None else peer_fixture["hourly"][keys[peer_index]]),
        btc_bars=tuple(btc_bars if btc_bars is not None else peer_fixture["hourly"][keys[0]]),
        cutoff_ns=CUTOFF,
        computed_at_ns=CUTOFF + computed_offset,
        available_at_ns=CUTOFF + available_offset,
        lookback_returns=lookback_returns,
        replay_view=replay_view,
        anchor_evidence=peer_fixture["evidence"].get(keys[anchor_index]),
        peer_evidence=peer_fixture["evidence"].get(keys[peer_index]),
    )


def _reconstructed(bar: CausalBarV2) -> CausalBarV2:
    raw = RawObservationV2.build(
        instrument_revision=bar.instrument_revision,
        source_id=bar.raw.source_id,
        event_type=bar.raw.event_type,
        event_at_ns=bar.close_at_ns,
        received_at_ns=bar.raw.received_at_ns,
        ingested_at_ns=bar.raw.ingested_at_ns,
        available_at_ns=bar.raw.available_at_ns,
        payload={"source_record_id": bar.raw.record_id, "bar": bar.to_dict()},
        translation_version="s46-reconstructed-fixture",
        sequence=str(bar.open_at_ns),
        availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
        replay_available_at_ns=bar.raw.available_at_ns,
    )
    return replace(bar, raw=raw)


def _late_revision(bar: CausalBarV2) -> CausalBarV2:
    late_at = bar.close_at_ns + HOUR
    raw = RawObservationV2.build(
        instrument_revision=bar.instrument_revision,
        source_id=bar.raw.source_id,
        event_type=bar.raw.event_type,
        event_at_ns=bar.close_at_ns,
        received_at_ns=late_at,
        ingested_at_ns=late_at,
        available_at_ns=late_at,
        payload={"late_revision_of": bar.raw.record_id, "close": str(bar.close)},
        translation_version="s46-late-revision-fixture",
        revision_of=bar.raw.record_id,
    )
    return replace(bar, raw=raw)


def _with_reconstructed_state(peer_fixture, tmp_path):
    keys = peer_fixture["keys"]
    hourly = {key: tuple(_reconstructed(bar) for bar in values)
              for key, values in peer_fixture["hourly"].items()}
    evidence = peer_fixture["evidence"]
    with OpsRepository(tmp_path / "ops-reconstructed.sqlite") as repository:
        _seed_evidence(repository, evidence)
        decision = S6ShadowCoordinator(repository).evaluate(
            universe=peer_fixture["universe"], cutoff_ns=CUTOFF, btc_proxy=keys[0],
            hourly_bars=hourly, four_hour_bars=peer_fixture["four_hour"], evidence=evidence,
            replay_view="RECONSTRUCTED_MARKET",
        )
    return hourly, decision.state


def test_pair_context_uses_exact_common_h1_returns_and_existing_s6_betas(peer_fixture) -> None:
    request = _request(peer_fixture)
    result = build_peer_relationship(request)
    rows = {row.key: row for row in peer_fixture["state"].rows}

    assert result.status == "AVAILABLE"
    assert result.interval == "1H"
    assert result.lookback_returns == 720
    assert result.sample_count == 720
    assert len(result.common_timestamps_ns) == result.sample_count
    assert tuple(sorted(result.common_timestamps_ns)) == result.common_timestamps_ns
    assert result.covariance is not None
    assert -1 <= result.correlation <= 1
    assert result.beta_peer_on_anchor is not None
    assert result.beta_anchor_btc == rows[request.anchor_key].beta_btc
    assert result.beta_peer_btc == rows[request.peer_key].beta_btc
    assert result.s6_state_ref == peer_fixture["state"].content_hash
    assert peer_fixture["universe"].content_hash in result.source_evidence_refs
    assert result.common_factor_caveat == (
        "RAW_PAIR_METRICS_INCLUDE_SHARED_BTC_FACTOR; BTC_ADJUSTED_CORRELATION_USES_EXISTING_S6_BETAS"
    )
    assert result.content_hash == build_peer_relationship(request).content_hash
    assert S6_POLICY.policy_hash == "a8ec8cf56a06de32ebc7f92beb9f895dce6229dac87860b23528cca49069c029"
    short_lookback = build_peer_relationship(_request(peer_fixture, lookback_returns=24))
    assert short_lookback.lookback_returns == short_lookback.sample_count == 24


def test_future_tail_and_late_revision_do_not_change_point_in_time_identity(peer_fixture) -> None:
    base = _request(peer_fixture)
    baseline = build_peer_relationship(base)
    anchor = list(base.anchor_bars)
    peer = list(base.peer_bars)
    btc = list(base.btc_bars)
    anchor.append(_late_revision(anchor[-1]))
    anchor.append(_bar(base.anchor_key, BarIntervalV2.H1, CUTOFF, 999.0))
    peer.append(_bar(base.peer_key, BarIntervalV2.H1, CUTOFF, 1.0))
    btc.append(_bar(base.btc_proxy_key, BarIntervalV2.H1, CUTOFF, 500.0))
    future_and_late = build_peer_relationship(_request(
        peer_fixture, anchor_bars=anchor, peer_bars=peer, btc_bars=btc,
    ))

    assert future_and_late.common_timestamps_ns == baseline.common_timestamps_ns
    assert future_and_late.source_evidence_refs == baseline.source_evidence_refs
    assert future_and_late.content_hash == baseline.content_hash


def test_missing_misaligned_and_gapped_inputs_are_explicitly_unavailable(peer_fixture) -> None:
    missing = build_peer_relationship(_request(peer_fixture, anchor_bars=()))
    missing_peer = build_peer_relationship(_request(peer_fixture, peer_bars=()))
    keys = peer_fixture["keys"]
    peer = tuple(item for i, item in enumerate(peer_fixture["hourly"][keys[2]]) if i not in {51, 52})
    misaligned = build_peer_relationship(_request(peer_fixture, peer_bars=peer))

    assert missing.status == "NOT_ESTIMABLE"
    assert missing.reason == "ANCHOR_HISTORY_MISSING"
    assert missing.correlation is None and missing.beta_peer_on_anchor is None
    assert missing_peer.status == "NOT_ESTIMABLE"
    assert missing_peer.reason == "PEER_HISTORY_MISSING"
    assert misaligned.status == "NOT_ESTIMABLE"
    assert misaligned.reason == "COMMON_H1_GAP"
    assert misaligned.covariance is None and misaligned.correlation is None


def test_zero_variance_and_btc_collinearity_never_emit_numeric_zero(peer_fixture) -> None:
    flat_anchor = tuple(_bar(peer_fixture["keys"][1], BarIntervalV2.H1, item.open_at_ns,
                             100.0) for item in peer_fixture["hourly"][peer_fixture["keys"][1]])
    zero_variance = build_peer_relationship(_request(peer_fixture, anchor_bars=flat_anchor))
    btc_pair = build_peer_relationship(_request(peer_fixture, anchor_index=0, peer_index=1))

    assert zero_variance.status == "NOT_ESTIMABLE"
    assert zero_variance.reason == "ZERO_VARIANCE_ANCHOR"
    assert zero_variance.covariance is None and zero_variance.correlation is None
    assert zero_variance.beta_peer_on_anchor is None
    assert btc_pair.btc_adjusted_correlation is None
    assert btc_pair.btc_adjustment_reason == "ZERO_VARIANCE_ANCHOR_RESIDUAL"
    assert btc_pair.correlation is not None and btc_pair.correlation > 0
    identical_pattern = build_peer_relationship(_request(peer_fixture, anchor_index=5, peer_index=6))
    assert identical_pattern.correlation == pytest.approx(1.0)
    assert identical_pattern.btc_adjusted_correlation == pytest.approx(1.0)


def test_reconstructed_view_is_explicit_and_never_mixed_with_actual_s6_state(peer_fixture, tmp_path) -> None:
    reconstructed_bars, state = _with_reconstructed_state(peer_fixture, tmp_path)
    result = build_peer_relationship(_request(
        peer_fixture,
        anchor_bars=reconstructed_bars[peer_fixture["keys"][1]],
        peer_bars=reconstructed_bars[peer_fixture["keys"][2]],
        btc_bars=reconstructed_bars[peer_fixture["keys"][0]],
        state=state,
        replay_view="RECONSTRUCTED_MARKET",
    ))
    assert result.status == "AVAILABLE"
    assert result.data_status == "RECONSTRUCTED_MARKET"
    assert "RECONSTRUCTED_MARKET_VIEW" in result.status_flags
    assert result.btc_adjusted_correlation is not None

    with pytest.raises(ValueError, match="availability view"):
        build_peer_relationship(_request(
            peer_fixture,
            anchor_bars=reconstructed_bars[peer_fixture["keys"][1]],
            peer_bars=reconstructed_bars[peer_fixture["keys"][2]],
            btc_bars=reconstructed_bars[peer_fixture["keys"][0]],
            replay_view="RECONSTRUCTED_MARKET",
        ))


def test_cache_rebuilds_after_restart_and_invalidates_on_source_change(peer_fixture) -> None:
    request = _request(peer_fixture)
    cache = PeerRelationshipCache(max_entries=2)
    first = cache.compute(request)
    assert cache.lookup(request, decision_cutoff_ns=CUTOFF + 3).status == "AVAILABLE"

    restarted = PeerRelationshipCache(max_entries=2)
    assert len(restarted) == 0
    assert restarted.lookup(request, decision_cutoff_ns=CUTOFF + 3).status == "CACHE_MISS"
    rebuilt = restarted.compute(request)
    assert rebuilt.content_hash == first.content_hash

    changed = list(request.peer_bars)
    original = changed[-2]
    changed[-2] = _bar(request.peer_key, BarIntervalV2.H1, original.open_at_ns,
                       float(original.close), source="SOURCE_HASH_REVISION")
    changed_request = _request(peer_fixture, peer_bars=changed)
    assert restarted.lookup(changed_request, decision_cutoff_ns=CUTOFF + 3).status == "CACHE_MISS"
    changed_result = restarted.compute(changed_request)
    assert changed_result.source_hash != rebuilt.source_hash


def test_cache_cutoff_ttl_and_h1_revision_invalidation(peer_fixture) -> None:
    request = _request(peer_fixture)
    cache = PeerRelationshipCache(max_entries=2)
    cache.compute(request)
    assert cache.lookup(request, decision_cutoff_ns=CUTOFF + 1).status == "NOT_YET_AVAILABLE"
    assert cache.lookup(request, decision_cutoff_ns=CUTOFF + 3).status == "AVAILABLE"
    assert cache.lookup(request, decision_cutoff_ns=CUTOFF + HOUR).status == "STALE_CACHE"

    short_ttl = PeerRelationshipCache(max_entries=1, ttl_ns=HOUR // 2)
    short_ttl.compute(request)
    assert short_ttl.lookup(request, decision_cutoff_ns=CUTOFF + HOUR // 2 + 1).status == "STALE_CACHE"

    cache.compute(request)
    new_bar = _bar(request.anchor_key, BarIntervalV2.H1, CUTOFF, 101.0)
    assert cache.invalidate_for_completed_h1(request.anchor_key, new_bar) == 1
    assert cache.lookup(request, decision_cutoff_ns=CUTOFF + 3).status == "CACHE_MISS"

    cache.compute(request)
    assert cache.invalidate_source_change(request.peer_key) == 1
    cache.compute(request)
    assert cache.invalidate_gap(request.peer_key) == 1


def test_cache_expired_before_publication_is_stale_not_not_yet_available(peer_fixture) -> None:
    base = _request(peer_fixture)
    request = _request(
        peer_fixture,
        anchor_bars=base.anchor_bars[:-2],
        peer_bars=base.peer_bars[:-2],
        btc_bars=base.btc_bars[:-2],
        available_offset=HOUR,
    )
    cache = PeerRelationshipCache()
    relationship = cache.compute(request)

    assert relationship.valid_until_ns < relationship.available_at_ns
    assert cache.lookup(request, decision_cutoff_ns=request.cutoff_ns + 1).status == "STALE_CACHE"


def test_new_product_revision_is_not_eligible_for_old_universe_cache(peer_fixture) -> None:
    request = _request(peer_fixture)
    cache = PeerRelationshipCache(max_entries=2)
    cache.compute(request)
    new_peer = replace(request.peer_key, contract_revision=sha256_json({"new_revision": "listing"}))

    with pytest.raises(ValueError, match="universe revision"):
        replace(request, peer_key=new_peer)
    assert cache.invalidate_product_revision(request.peer_key, new_peer) == 1
    assert len(cache) == 0


def test_cache_eviction_is_bounded_fifo_and_pair_only_has_no_venue_side_effects(peer_fixture, monkeypatch) -> None:
    import socket

    monkeypatch.setattr(socket, "socket", lambda *_a, **_k: pytest.fail("peer context opened a socket"))
    cache = PeerRelationshipCache(max_entries=2)
    requests = tuple(_request(peer_fixture, peer_index=i) for i in (2, 3, 4))
    for request in requests:
        cache.compute(request)

    assert len(cache) == 2
    assert cache.lookup(requests[0], decision_cutoff_ns=CUTOFF + 3).status == "CACHE_MISS"
    assert cache.lookup(requests[1], decision_cutoff_ns=CUTOFF + 3).status == "AVAILABLE"
    assert cache.lookup(requests[2], decision_cutoff_ns=CUTOFF + 3).status == "AVAILABLE"
