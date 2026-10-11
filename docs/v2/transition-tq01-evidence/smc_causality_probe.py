"""Prefix probe for SMC's centered FVG/swing outputs vs ATLAS close events."""
from __future__ import annotations

import json
from decimal import Decimal

import pandas as pd
from smartmoneyconcepts import smc

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.features.structure import structure_events
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2


def main() -> None:
    rows = [
        (10, 10, 11, 9), (10, 10, 11, 9), (11, 12, 13, 10.5),
        (13, 14, 15, 12.5), (13, 13, 14, 12), (13, 11, 13.5, 10.5),
        (11, 10, 12, 9.5), (10, 12, 13, 9.8), (12, 11, 13, 10),
        (11, 10, 12, 9), (10, 13, 14, 9.8), (13, 12, 14, 11),
    ]
    frame = pd.DataFrame(rows, columns=("open", "close", "high", "low"))
    frame = frame[["open", "high", "low", "close"]].astype(float)
    full_fvg = smc.fvg(frame)
    prefix_fvg = smc.fvg(frame.iloc[:3])
    swing_values = [10, 11, 12, 15, 13, 12, 11, 10, 9]
    swing_frame = pd.DataFrame({"open": [x - 0.5 for x in swing_values],
        "high": swing_values, "low": [x - 1 for x in swing_values],
        "close": [x - 0.5 for x in swing_values]})
    full_swings = smc.swing_highs_lows(swing_frame, swing_length=2)
    prefix_swings = smc.swing_highs_lows(swing_frame.iloc[:5], swing_length=2)

    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, ProductTypeV2.LINEAR_PERPETUAL,
                          "BTCUSDT", "bitcoin", "USDT", "USDT", "e" * 64)
    bars = []
    interval = BarIntervalV2.M15
    for i, (opening, close, high, low) in enumerate(rows):
        close_ns = (i + 1) * interval.duration_ns
        raw = RawObservationV2.build(instrument_revision=key.contract_revision,
            source_id="smc-causality-fixture", event_type=f"BAR_{interval.value}",
            received_at_ns=close_ns, ingested_at_ns=close_ns, available_at_ns=close_ns,
            event_at_ns=close_ns, translation_version="tq01-smc-probe-v1",
            payload={"index": i, "close": close})
        bars.append(CausalBarV2(raw, interval, i * interval.duration_ns, close_ns,
            Decimal(str(opening)), Decimal(str(high)), Decimal(str(low)), Decimal(str(close)),
            Decimal("1"), True))
    bars = tuple(bars)
    causal_events = structure_events(bars[:4], (None,) * 4)
    atlas_fvg_confirmation_indexes = [i for i, bar in enumerate(bars[:4])
        if any(event.kind == "FVG_BULL" and event.bar_ref == bar.content_hash for event in causal_events)]

    output = {
        "package": "smartmoneyconcepts==0.0.27",
        "fixture": "deterministic synthetic OHLC, no venue data",
        "fvg_prefix_index_2": None if pd.isna(prefix_fvg.FVG.iloc[2]) else int(prefix_fvg.FVG.iloc[2]),
        "fvg_full_index_2": None if pd.isna(full_fvg.FVG.iloc[2]) else int(full_fvg.FVG.iloc[2]),
        "fvg_changed_when_future_bar_added": bool(pd.isna(prefix_fvg.FVG.iloc[2]) and full_fvg.FVG.iloc[2] == 1),
        "fvg_future_uses_shift_minus_one": True,
        "smc_swing_prefix_first5": [None if pd.isna(x) else int(x) for x in prefix_swings.HighLow],
        "smc_swing_full_first5": [None if pd.isna(x) else int(x) for x in full_swings.HighLow.iloc[:5]],
        "smc_swing_prefix_changed": not full_swings.HighLow.iloc[:5].reset_index(drop=True).equals(
            prefix_swings.HighLow.reset_index(drop=True)),
        "swing_lookahead_bars_each_side": 2,
        "atlas_fvg_confirmation_indexes": atlas_fvg_confirmation_indexes,
        "evidence_class": "SYNTHETIC_CAUSALITY_PROBE_NOT_MARKET_OR_PERFORMANCE_EVIDENCE",
    }
    print(json.dumps(output, sort_keys=True))
    if not output["fvg_changed_when_future_bar_added"] or not output["smc_swing_prefix_changed"]:
        raise SystemExit("expected centered SMC prefix revisions were not observed")


if __name__ == "__main__":
    main()
