from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import EPOCH_NS, HOUR_NS, universe_for

from atlas.scanner import EligibilityStatus, ListingStatus, UniverseEntry, build_universe_snapshot


def test_universe_snapshot_is_immutable_hashed_and_retains_historical_exclusions():
    snapshot = universe_for(EPOCH_NS)
    assert snapshot.hash() == universe_for(EPOCH_NS).hash()
    assert snapshot.entry("LUNAUSDT").eligibility_status is EligibilityStatus.EXCLUDED
    assert snapshot.entry("LUNAUSDT").listing_status is ListingStatus.DELISTED
    assert snapshot.entry("LUNAUSDT").exclusion_reason == "DELISTED_RETAINED_IN_HISTORICAL_UNIVERSE"
    assert tuple(entry.instrument for entry in snapshot.entries) == tuple(sorted(entry.instrument for entry in snapshot.entries))


def test_universe_snapshot_rejects_future_and_backdated_availability():
    snapshot = universe_for(EPOCH_NS)
    with pytest.raises(ValueError, match="not available"):
        build_universe_snapshot(snapshot_id="bad", observed_at_ns=EPOCH_NS, available_at_ns=EPOCH_NS,
                                venue="BYBIT", version="V1",
                                entries=(replace(snapshot.entry("BTCUSDT"), available_at_ns=EPOCH_NS + HOUR_NS),),
                                source_ref="bad")
    with pytest.raises(ValueError, match="precedes observation"):
        build_universe_snapshot(snapshot_id="bad", observed_at_ns=EPOCH_NS, available_at_ns=EPOCH_NS - 1,
                                venue="BYBIT", version="V1", entries=snapshot.entries, source_ref="bad")


def test_universe_hash_binds_contract_spec_and_rejects_future_contract_metadata():
    snapshot = universe_for(EPOCH_NS)
    revised = build_universe_snapshot(
        snapshot_id=snapshot.snapshot_id, observed_at_ns=snapshot.observed_at_ns,
        available_at_ns=snapshot.available_at_ns, venue=snapshot.venue, version=snapshot.version,
        entries=tuple(replace(entry, contract_spec_hash=f"revised-{entry.contract_spec_hash}")
                      for entry in snapshot.entries),
        source_ref=snapshot.source_ref)
    assert revised.hash() != snapshot.hash()
    with pytest.raises(ValueError, match="not available by the universe cutoff"):
        replace(snapshot.entry("BTCUSDT"),
                contract_spec_available_at_ns=snapshot.entry("BTCUSDT").available_at_ns + 1)


def test_ranking_eligibility_is_separate_from_capital_eligibility():
    snapshot = universe_for(EPOCH_NS)
    capital = {entry.instrument for entry in snapshot.entries if entry.capital_enabled}
    assert capital == {"BTCUSDT", "ETHUSDT"}
    assert snapshot.entry("SOLUSDT").eligibility_status is EligibilityStatus.ELIGIBLE
    assert snapshot.entry("SOLUSDT").capital_enabled is False
    with pytest.raises(ValueError, match="BTCUSDT/ETHUSDT"):
        UniverseEntry(EPOCH_NS, EPOCH_NS, EPOCH_NS, "BYBIT", "SOLUSDT", "PERP",
                      ListingStatus.LISTED, EligibilityStatus.ELIGIBLE, None, "ref", True,
                      "contract-sol", "contract-hash-sol", EPOCH_NS)
