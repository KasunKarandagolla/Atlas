"""S41 S6 cohort stays aligned with the production BTC proxy identity."""

from __future__ import annotations

from dataclasses import replace

from atlas.v2._serialization import sha256_json
from atlas.v2.instruments import EnvironmentV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.full_strategy_surface import compose_full_strategy_surface
from atlas.v2.strategies.s6_cross_section import (
    S6ShadowCoordinator,
    _same_btc_proxy_cohort,
)

from .test_session021_s6 import CUTOFF, _keys, _market, _seed_evidence, _universe


def test_mixed_environment_peer_cannot_pollute_s6_proxy_cohort(tmp_path) -> None:
    keys = _keys(20)
    mixed_peer = replace(keys[1], environment=EnvironmentV2.DEMO)
    universe = _universe((*keys, mixed_peer))
    hourly, four_hour, evidence = _market(keys)

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        _seed_evidence(repository, evidence)
        result = S6ShadowCoordinator(repository).evaluate(
            universe=universe,
            cutoff_ns=CUTOFF,
            btc_proxy=keys[0],
            hourly_bars=hourly,
            four_hour_bars=four_hour,
            evidence=evidence,
        )

    assert result.status == "AVAILABLE"
    assert result.state.eligible_breadth == 20
    assert len(result.state.rows) == 20
    assert all(row.key != mixed_peer for row in result.state.rows)
    assert sum(row.eligibility == "ELIGIBLE" for row in result.state.rows) == 19


def test_s6_proxy_cohort_matches_full_venue_environment_product_identity() -> None:
    proxy, peer = _keys(2)
    assert _same_btc_proxy_cohort(peer, proxy)
    assert not _same_btc_proxy_cohort(
        replace(peer, environment=EnvironmentV2.TESTNET), proxy,
    )


def test_full_strategy_surface_keeps_bybit_and_binance_s6_cohorts_distinct(tmp_path) -> None:
    bybit = _keys(20)
    binance = tuple(replace(
        key,
        venue=VenueV2.BINANCE,
        contract_revision=sha256_json({"symbol": key.native_symbol, "venue": "BINANCE"}),
    ) for key in bybit)
    universe = _universe((*bybit, *binance))

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            universe.content_hash, universe.ARTIFACT_TYPE, universe.content_hash,
            CUTOFF, CUTOFF, {"universe": universe.to_dict()},
        ))
        result = compose_full_strategy_surface(
            repository,
            universe=universe,
            cutoff_ns=CUTOFF,
            s6_hourly_bars={},
            s6_four_hour_bars={},
            s6_evidence={},
            s6_btc_proxies=(bybit[0], binance[0]),
            clock_ns=lambda: CUTOFF + 1,
        )

        role = repository.get_artifact(result.role_refs["S6"][0])

    assert role is not None
    cohorts = role.metadata["role"]["payload"]["cohorts"]
    assert [item["venue"] for item in cohorts] == ["BINANCE", "BYBIT"]
    assert all(item["state"]["eligible_breadth"] == 20 for item in cohorts)
    assert all(len(item["state"]["rows"]) == 20 for item in cohorts)
    assert {row["key"]["venue"] for item in cohorts for row in item["state"]["rows"]} == {
        "BINANCE", "BYBIT",
    }
