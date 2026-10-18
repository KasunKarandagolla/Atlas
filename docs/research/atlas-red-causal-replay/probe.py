"""Deterministic synthetic availability counterexample, runnable at the parent.

Use PYTHONPATH pointing at the checkout under test. No source health or trading
qualification is claimed. The native feature owner supplies every result.
"""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.features.candles import DAY_NS, CausalTrade
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

KEY = InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                      "BTCUSDT", "BTC", "USDT", "USDT", "a" * 64)
CUTOFF = 2 * DAY_NS + 5_000_000_000
ACTUAL_READ = 1_792_332_000_000_000_000


def bar(index):
    opened = index * BarIntervalV2.M15.duration_ns
    closed = opened + BarIntervalV2.M15.duration_ns
    raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="SYNTHETIC_COUNTEREXAMPLE", event_type="BAR_15M",
        received_at_ns=ACTUAL_READ, ingested_at_ns=ACTUAL_READ, available_at_ns=ACTUAL_READ,
        event_at_ns=closed, payload={"fixture_index": index}, translation_version="SYNTHETIC_PROBE_V1",
        availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET, replay_available_at_ns=closed + 5_000_000_000,
    )
    return CausalBarV2(raw, BarIntervalV2.M15, opened, closed, Decimal("100"), Decimal("101"),
                       Decimal("99"), Decimal("100"), Decimal("1"), True)


def probe():
    bars = tuple(bar(i) for i in range(96, 192))
    join = JoinedBars(KEY, CUTOFF, (), (), bars, "NOT_ESTIMABLE", "SOURCE_HEALTH_UNKNOWN", sha256_json("health unknown"))
    trade = CausalTrade(KEY, Decimal("10"), Decimal("2"), 2 * DAY_NS + 1, 2 * DAY_NS + 2, "synthetic-actual-only", "1")
    f = feature_snapshot(join, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET, trades=(trade,))
    fields = ("location.prior_day_high", "location.prior_day_low", "location.utc_day_trade_vwap_partial_to_cutoff")
    return {"grade": "SYNTHETIC_OFFLINE", "native_feature_version": f.feature_set_version,
            "values": {name: f.to_dict()["values"][name] for name in fields},
            "source_receipt_after_historical_cutoff": all(b.raw.received_at_ns > CUTOFF for b in bars),
            "receipt_unchanged": all(b.raw.received_at_ns == ACTUAL_READ for b in bars),
            "deadline_not_extended_ns": 5_000_000_000,
            "future_dependency_refused": _future_guard(join), "authority": "ZERO"}


def _future_guard(join):
    try:
        feature_snapshot(replace(join, m15=join.m15 + (bar(192),)), replay_view=ReplayViewV2.RECONSTRUCTED_MARKET)
    except ValueError as exc:
        return str(exc)
    raise ValueError("FUTURE_DEPENDENCY_NOT_REFUSED")


if __name__ == "__main__":
    print(json.dumps(probe(), sort_keys=True, indent=2))
