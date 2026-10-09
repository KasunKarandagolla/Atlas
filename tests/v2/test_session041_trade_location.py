from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.features.candles import (
    CausalTrade,
    CausalTradeAnchor,
    TradeLocationConfig,
    anchored_vwap,
    fixed_tick_volume_profile,
)
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import TRADE_LOCATION_FEATURE_SET_VERSION, feature_snapshot
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

CUTOFF = 1_750_000_100_000_000_000
START = CUTOFF - 60_000_000_000
KEY = InstrumentKeyV2(
    VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
    "BTCUSDT", "BTC", "USDT", "USDT", "a" * 64,
)


def ref(value: object) -> str:
    return sha256_json({"trade_location_test": value})


def trade(identity: str | None, price: str, qty: str, event: int, available: int | None = None) -> CausalTrade:
    return CausalTrade(KEY, Decimal(price), Decimal(qty), event, event if available is None else available,
                       ref(["trade", identity, price, qty, event]), identity)


def product() -> ProductContractV2:
    return ProductContractV2(KEY, START, START, START, Decimal("1"), Decimal("0.5"), Decimal("0.1"),
                              Decimal("0.1"), TradingStatusV2.TRADING, ref("product"))


def test_anchored_vwap_is_weighted_and_uses_only_cutoff_available_trades() -> None:
    anchor = CausalTradeAnchor(KEY, START, START + 2, ref("anchor"))
    rows = (trade("a", "100", "2", START), trade("b", "102", "1", START + 1),
            trade("future", "900", "500", CUTOFF + 1))
    result = anchored_vwap(rows, key=KEY, cutoff_ns=CUTOFF, anchor=anchor)
    assert result.status == "AVAILABLE"
    assert result.value == Decimal(302) / Decimal(3)
    assert result.trade_count == 2
    assert result.input_refs == tuple(sorted((anchor.ref, rows[0].ref, rows[1].ref)))
    assert result.to_dict()["capital_authority"] == "ZERO"


def test_missing_native_identity_or_late_anchor_is_not_estimable() -> None:
    missing_identity = anchored_vwap(
        (trade(None, "100", "1", START),), key=KEY, cutoff_ns=CUTOFF,
        anchor=CausalTradeAnchor(KEY, START, START, ref("anchor")),
    )
    late_anchor = anchored_vwap(
        (trade("a", "100", "1", START),), key=KEY, cutoff_ns=CUTOFF,
        anchor=CausalTradeAnchor(KEY, START, CUTOFF + 1, ref("late-anchor")),
    )
    assert (missing_identity.status, missing_identity.reason, missing_identity.value) == (
        "NOT_ESTIMABLE", "NATIVE_TRADE_ID_UNAVAILABLE", None,
    )
    assert (late_anchor.status, late_anchor.reason, late_anchor.value) == (
        "NOT_ESTIMABLE", "ANCHOR_NOT_AVAILABLE_AT_CUTOFF", None,
    )


def test_fixed_profile_bins_exact_ticks_and_tie_breaks_to_lower_price() -> None:
    result = fixed_tick_volume_profile(
        (trade("a", "100.5", "2", START), trade("b", "101", "2", START + 1),
         trade("c", "100.5", "1", START + 2)),
        key=KEY, cutoff_ns=CUTOFF, window_start_ns=START, product=product(),
    )
    assert result.status == "AVAILABLE"
    assert result.value == Decimal("100.5")
    assert result.bins == ((201, Decimal("100.5"), Decimal("3")), (202, Decimal("101.0"), Decimal("2")))
    assert result.trade_count == 3


def test_profile_refuses_off_tick_or_unidentified_trades() -> None:
    off_tick = fixed_tick_volume_profile(
        (trade("a", "100.4", "1", START),), key=KEY, cutoff_ns=CUTOFF,
        window_start_ns=START, product=product(),
    )
    no_identity = fixed_tick_volume_profile(
        (trade(None, "100.5", "1", START),), key=KEY, cutoff_ns=CUTOFF,
        window_start_ns=START, product=product(),
    )
    assert (off_tick.status, off_tick.reason, off_tick.value) == ("NOT_ESTIMABLE", "OFF_TICK_TRADE_PRICE", None)
    assert (no_identity.status, no_identity.reason, no_identity.value) == (
        "NOT_ESTIMABLE", "NATIVE_TRADE_ID_UNAVAILABLE", None,
    )


def test_pipeline_keeps_missing_trade_location_explicit_and_opt_in() -> None:
    health_ref = ref("health")
    join = JoinedBars(KEY, CUTOFF, (), (), (), "NOT_ESTIMABLE", "MISSING_FRAMES", health_ref)
    trades = (trade("a", "100.5", "1", START),)
    default = feature_snapshot(join, trades=trades, replay_view=ReplayViewV2.ACTUAL_SYSTEM)
    enabled = feature_snapshot(
        join, trades=trades, replay_view=ReplayViewV2.ACTUAL_SYSTEM,
        trade_location=TradeLocationConfig(
            anchor=CausalTradeAnchor(KEY, START, START, ref("anchor")),
            profile_window_start_ns=START, product=product(),
        ),
    )
    assert default.values["location.anchored_vwap"].value is None
    assert default.values["location.volume_profile"].value is None
    assert enabled.values["location.anchored_vwap"].value == Decimal("100.5")
    assert enabled.values["location.volume_profile"].value == Decimal("100.5")
    assert enabled.feature_set_version == TRADE_LOCATION_FEATURE_SET_VERSION
    assert enabled.envelope.input_refs != default.envelope.input_refs


def test_pipeline_does_not_reuse_actual_trade_availability_for_reconstructed_replay() -> None:
    join = JoinedBars(KEY, CUTOFF, (), (), (), "NOT_ESTIMABLE", "MISSING_FRAMES", ref("health"))
    result = feature_snapshot(
        join,
        trades=(trade("a", "100.5", "1", START),),
        replay_view=ReplayViewV2.RECONSTRUCTED_MARKET,
        trade_location=TradeLocationConfig(
            anchor=CausalTradeAnchor(KEY, START, START, ref("anchor")),
            profile_window_start_ns=START,
            product=product(),
        ),
    )
    assert result.values["location.anchored_vwap"].value is None
    assert result.values["location.anchored_vwap"].missing_reason == "REPLAY_TRADE_AVAILABILITY_UNAVAILABLE"
    assert result.values["location.volume_profile"].value is None
    assert result.values["location.volume_profile"].missing_reason == "REPLAY_TRADE_AVAILABILITY_UNAVAILABLE"
