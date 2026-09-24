"""Completed-bar continuous geometry and causal location features."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from statistics import pstdev

from atlas.domain.money import ensure_positive_decimal
from atlas.v2._serialization import nonblank, timestamp
from atlas.v2.data.bars import CausalBarV2
from atlas.v2.instruments import InstrumentKeyV2

DAY_NS = 86_400_000_000_000


def candle_geometry(bar: CausalBarV2, *, atr: float | None, previous: CausalBarV2 | None = None,
                    volume_history: Sequence[CausalBarV2] = ()) -> dict[str, float | None]:
    if not bar.final or (previous is not None and (not previous.final or previous.close_at_ns > bar.open_at_ns)):
        raise ValueError("candle geometry requires completed causal bars")
    if previous is not None and previous.instrument_revision != bar.instrument_revision:
        raise ValueError("candle history instrument revision mismatch")
    if any(not item.final or item.instrument_revision != bar.instrument_revision for item in volume_history):
        raise ValueError("candle volume history requires final same-revision bars")
    body = float(bar.close - bar.open)
    candle_range = float(bar.high - bar.low)
    denominator = atr if atr is not None and atr > 0 else None
    volumes = [float(x.volume) for x in volume_history if x.close_at_ns < bar.close_at_ns and x.final]
    sigma = pstdev(volumes) if len(volumes) >= 2 else 0.0
    return {
        "signed_body_atr": body / denominator if denominator else None,
        "upper_wick_atr": float(bar.high - max(bar.open, bar.close)) / denominator if denominator else None,
        "lower_wick_atr": float(min(bar.open, bar.close) - bar.low) / denominator if denominator else None,
        "range_atr": candle_range / denominator if denominator else None,
        "close_position": float((bar.close - bar.low) / (bar.high - bar.low)) if bar.high > bar.low else None,
        "gap": float(bar.open - previous.close) if previous is not None else None,
        "volume_z": (float(bar.volume) - sum(volumes) / len(volumes)) / sigma if sigma > 0 else None,
    }


@dataclass(frozen=True)
class CausalTrade:
    key: InstrumentKeyV2
    price: Decimal
    quantity: Decimal
    event_at_ns: int
    available_at_ns: int
    ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("VWAP trade requires full InstrumentKeyV2")
        object.__setattr__(self, "price", ensure_positive_decimal(self.price, field="trade.price"))
        object.__setattr__(self, "quantity", ensure_positive_decimal(self.quantity, field="trade.quantity"))
        timestamp(self.event_at_ns, field="trade.event_at_ns")
        timestamp(self.available_at_ns, field="trade.available_at_ns")
        nonblank(self.ref, field="trade.ref")
        if self.available_at_ns < self.event_at_ns:
            raise ValueError("trade availability or ref invalid")


def utc_day_vwap(trades: Sequence[CausalTrade], *, key: InstrumentKeyV2, cutoff_ns: int) -> tuple[Decimal | None, tuple[str, ...]]:
    if any(trade.key != key for trade in trades):
        raise ValueError("VWAP cannot join different instrument revisions")
    day_start = cutoff_ns // DAY_NS * DAY_NS
    selected = sorted((trade for trade in trades if day_start <= trade.event_at_ns <= cutoff_ns and trade.available_at_ns <= cutoff_ns), key=lambda x: (x.event_at_ns, x.ref))
    if any(trade.price <= 0 or trade.quantity <= 0 for trade in selected):
        raise ValueError("trade VWAP needs positive price and quantity")
    total = sum((trade.quantity for trade in selected), Decimal(0))
    if not total:
        return None, ()
    return sum((trade.price * trade.quantity for trade in selected), Decimal(0)) / total, tuple(trade.ref for trade in selected)


def prior_utc_day_range(bars: Sequence[CausalBarV2], *, cutoff_ns: int) -> tuple[Decimal | None, Decimal | None, tuple[str, ...]]:
    start = cutoff_ns // DAY_NS * DAY_NS - DAY_NS
    selected = [bar for bar in bars if bar.final and start <= bar.open_at_ns and bar.close_at_ns <= start + DAY_NS and bar.raw.available_at_ns <= cutoff_ns]
    if not selected:
        return None, None, ()
    return max(bar.high for bar in selected), min(bar.low for bar in selected), tuple(bar.content_hash for bar in selected)
