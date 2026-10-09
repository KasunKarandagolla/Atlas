"""Completed-bar continuous geometry and causal location features."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, DecimalException, localcontext
from statistics import pstdev
from typing import Any

from atlas.domain.money import ensure_positive_decimal
from atlas.v2._serialization import nonblank, sha256_json, sha256_ref, timestamp
from atlas.v2.data.bars import CausalBarV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2

DAY_NS = 86_400_000_000_000
TRADE_LOCATION_VERSION = "CAUSAL_TRADE_LOCATION_DIAGNOSTICS_V1"
MAX_LOCATION_TRADES = 8192
MAX_PROFILE_BINS = 4096
MAX_LOCATION_WINDOW_NS = 7 * DAY_NS


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
    trade_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("VWAP trade requires full InstrumentKeyV2")
        object.__setattr__(self, "price", ensure_positive_decimal(self.price, field="trade.price"))
        object.__setattr__(self, "quantity", ensure_positive_decimal(self.quantity, field="trade.quantity"))
        timestamp(self.event_at_ns, field="trade.event_at_ns")
        timestamp(self.available_at_ns, field="trade.available_at_ns")
        nonblank(self.ref, field="trade.ref")
        if self.trade_id is not None:
            nonblank(self.trade_id, field="trade.trade_id")
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


@dataclass(frozen=True)
class CausalTradeAnchor:
    """Declared event anchor with actual availability of its confirmation."""

    key: InstrumentKeyV2
    event_at_ns: int
    available_at_ns: int
    ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("trade anchor requires an exact InstrumentKeyV2")
        timestamp(self.event_at_ns, field="anchor.event_at_ns")
        timestamp(self.available_at_ns, field="anchor.available_at_ns")
        if self.available_at_ns < self.event_at_ns:
            raise ValueError("anchor confirmation availability precedes event")
        sha256_ref(self.ref, field="anchor.ref")

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key.to_dict(), "event_at_ns": self.event_at_ns,
                "available_at_ns": self.available_at_ns, "ref": self.ref}


@dataclass(frozen=True)
class TradeLocationConfig:
    """Explicit diagnostic inputs; this declaration asserts no trade coverage."""

    anchor: CausalTradeAnchor | None = None
    profile_window_start_ns: int | None = None
    product: ProductContractV2 | None = None

    def __post_init__(self) -> None:
        if self.anchor is not None and not isinstance(self.anchor, CausalTradeAnchor):
            raise ValueError("unsupported anchor configuration")
        if self.profile_window_start_ns is not None:
            timestamp(self.profile_window_start_ns, field="profile_window_start_ns")
        if self.product is not None and not isinstance(self.product, ProductContractV2):
            raise ValueError("unsupported product metadata configuration")

    def to_dict(self) -> dict[str, Any]:
        return {"version": TRADE_LOCATION_VERSION,
                "anchor": self.anchor.to_dict() if self.anchor else None,
                "profile_window_start_ns": self.profile_window_start_ns,
                "product_ref": self.product.content_hash if self.product else None,
                "maximum_trades": MAX_LOCATION_TRADES, "maximum_bins": MAX_PROFILE_BINS,
                "maximum_window_ns": MAX_LOCATION_WINDOW_NS,
                "capital_authority": "ZERO", "coverage": "UNVERIFIED"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class TradeLocationDiagnostic:
    method: str
    status: str
    reason: str | None
    value: Decimal | None
    cutoff_ns: int
    window_start_ns: int | None
    input_refs: tuple[str, ...] = ()
    trade_count: int = 0
    total_quantity: Decimal | None = None
    # Occupied tick indices, exact prices and source trade quantities.
    bins: tuple[tuple[int, Decimal, Decimal], ...] = ()
    tick_size: Decimal | None = None
    input_identity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"version": TRADE_LOCATION_VERSION, "method": self.method,
                "status": self.status, "reason": self.reason,
                "value": str(self.value) if self.value is not None else None,
                "cutoff_ns": self.cutoff_ns, "window_start_ns": self.window_start_ns,
                "input_refs": list(self.input_refs), "trade_count": self.trade_count,
                "total_quantity": str(self.total_quantity) if self.total_quantity is not None else None,
                "bins": [{"tick_index": index, "price": str(price), "quantity": str(quantity)}
                         for index, price, quantity in self.bins],
                "tick_size": str(self.tick_size) if self.tick_size is not None else None,
                "input_identity": self.input_identity, "coverage": "UNVERIFIED",
                "quantity_unit": "SOURCE_TRADE_QUANTITY", "capital_authority": "ZERO"}


class _LocationNotEstimable(Exception):
    pass


def _location_require(condition: bool, reason: str) -> None:
    if not condition:
        raise _LocationNotEstimable(reason)


def _bounded_decimal(value: Decimal) -> bool:
    exponent = value.as_tuple().exponent
    return (
        value.is_finite()
        and value > 0
        and len(value.as_tuple().digits) <= 64
        and isinstance(exponent, int)
        and abs(exponent) <= 64
    )


def _location_window(
    trades: Sequence[CausalTrade], *, key: InstrumentKeyV2, cutoff_ns: int, start_ns: int | None,
) -> tuple[CausalTrade, ...]:
    _location_require(isinstance(key, InstrumentKeyV2), "EXACT_INSTRUMENT_KEY_UNAVAILABLE")
    timestamp(cutoff_ns, field="cutoff_ns")
    _location_require(start_ns is not None, "EXPLICIT_WINDOW_UNAVAILABLE")
    assert start_ns is not None
    timestamp(start_ns, field="window_start_ns")
    _location_require(0 <= cutoff_ns - start_ns <= MAX_LOCATION_WINDOW_NS, "WINDOW_OUTSIDE_BOUND")
    _location_require(len(trades) <= MAX_LOCATION_TRADES, "TRADE_COUNT_BOUND_EXCEEDED")
    _location_require(all(isinstance(trade, CausalTrade) and trade.key == key for trade in trades),
                      "TRADE_INSTRUMENT_MISMATCH")
    # Ineligible future rows do not influence identity or duplicate detection.
    selected = tuple(sorted((trade for trade in trades if start_ns <= trade.event_at_ns <= cutoff_ns
                             and trade.available_at_ns <= cutoff_ns), key=lambda trade: (trade.event_at_ns, trade.ref)))
    _location_require(bool(selected), "CAUSAL_TRADES_UNAVAILABLE")
    identities: set[str] = set()
    refs: set[str] = set()
    for trade in selected:
        _location_require(trade.trade_id is not None, "NATIVE_TRADE_ID_UNAVAILABLE")
        assert trade.trade_id is not None
        _location_require(trade.trade_id == trade.trade_id.strip(), "INVALID_NATIVE_TRADE_ID")
        sha256_ref(trade.ref, field="trade.ref")
        _location_require(trade.ref == trade.ref.strip(), "INVALID_TRADE_REF")
        _location_require(trade.trade_id not in identities and trade.ref not in refs, "DUPLICATE_TRADE_IDENTITY")
        _location_require(_bounded_decimal(trade.price) and _bounded_decimal(trade.quantity), "NUMERIC_INPUT_BOUND_EXCEEDED")
        identities.add(trade.trade_id)
        refs.add(trade.ref)
    return selected


def _trade_location_identity(method: str, selected: Sequence[CausalTrade], *, key: InstrumentKeyV2,
                             cutoff_ns: int, start_ns: int, configuration: Any) -> str:
    return sha256_json({"version": TRADE_LOCATION_VERSION, "method": method, "key": key.to_dict(),
                        "cutoff_ns": cutoff_ns, "window_start_ns": start_ns, "configuration": configuration,
                        "rows": [{"trade_id": trade.trade_id, "ref": trade.ref,
                                  "event_at_ns": trade.event_at_ns, "available_at_ns": trade.available_at_ns,
                                  "price": trade.price, "quantity": trade.quantity} for trade in selected]})


def anchored_vwap(
    trades: Sequence[CausalTrade], *, key: InstrumentKeyV2, cutoff_ns: int,
    anchor: CausalTradeAnchor | None = None,
) -> TradeLocationDiagnostic:
    """Quantity-weighted observed trades from a cutoff-known event anchor.

    This is a diagnostic over supplied trades and does not certify completeness.
    The anchor event is included; confirmation may occur later, by cutoff.
    """
    method = "ANCHORED_TRADE_VWAP"
    start = anchor.event_at_ns if isinstance(anchor, CausalTradeAnchor) else None
    refs: tuple[str, ...] = ()
    try:
        _location_require(isinstance(anchor, CausalTradeAnchor), "CAUSAL_ANCHOR_UNAVAILABLE")
        assert anchor is not None
        _location_require(anchor.key == key, "ANCHOR_INSTRUMENT_MISMATCH")
        _location_require(anchor.event_at_ns <= anchor.available_at_ns <= cutoff_ns, "ANCHOR_NOT_AVAILABLE_AT_CUTOFF")
        refs = (anchor.ref,)
        selected = _location_window(trades, key=key, cutoff_ns=cutoff_ns, start_ns=start)
        refs = tuple(sorted({anchor.ref} | {trade.ref for trade in selected}))
        with localcontext() as context:
            context.prec = 512
            total = sum((trade.quantity for trade in selected), Decimal(0))
            numerator = sum((trade.price * trade.quantity for trade in selected), Decimal(0))
            context.prec = 28
            value = numerator / total
        identity = _trade_location_identity(method, selected, key=key, cutoff_ns=cutoff_ns,
                                           start_ns=anchor.event_at_ns, configuration=anchor.to_dict())
        return TradeLocationDiagnostic(method, "AVAILABLE", None, value, cutoff_ns, start,
                                       refs, len(selected), total, input_identity=identity)
    except (_LocationNotEstimable, ValueError, DecimalException) as exc:
        reason = str(exc) if isinstance(exc, _LocationNotEstimable) else "INVALID_TRADE_LOCATION_INPUT"
        return TradeLocationDiagnostic(method, "NOT_ESTIMABLE", reason, None, cutoff_ns, start, refs)


def fixed_tick_volume_profile(
    trades: Sequence[CausalTrade], *, key: InstrumentKeyV2, cutoff_ns: int,
    window_start_ns: int | None = None, product: ProductContractV2 | None = None,
) -> TradeLocationDiagnostic:
    """Exact price/tick bins of supplied trade quantities; value is POC price.

    Prices must lie exactly on the declared product tick grid. The lowest tick
    wins equal-volume POC ties. No candle-derived volumes or value-area rule.
    """
    method = "FIXED_TICK_TRADE_VOLUME_PROFILE"
    refs: tuple[str, ...] = ()
    try:
        _location_require(isinstance(product, ProductContractV2), "PRODUCT_TICK_METADATA_UNAVAILABLE")
        assert product is not None
        _location_require(product.key == key, "PRODUCT_INSTRUMENT_MISMATCH")
        _location_require(product.effective_at_ns <= cutoff_ns and product.available_at_ns <= cutoff_ns,
                          "PRODUCT_NOT_AVAILABLE_AT_CUTOFF")
        sha256_ref(product.metadata_ref, field="product.metadata_ref")
        _location_require(_bounded_decimal(product.tick_size), "INVALID_OR_UNBOUNDED_TICK_SIZE")
        refs = tuple(sorted({product.content_hash, product.metadata_ref}))
        selected = _location_window(trades, key=key, cutoff_ns=cutoff_ns, start_ns=window_start_ns)
        assert window_start_ns is not None
        # Product revision must be effective throughout the requested window.
        _location_require(product.effective_at_ns <= window_start_ns, "PRODUCT_REVISION_AFTER_WINDOW_START")
        refs = tuple(sorted(set(refs) | {trade.ref for trade in selected}))
        tick_numerator, tick_denominator = product.tick_size.as_integer_ratio()
        bins: dict[int, Decimal] = {}
        with localcontext() as context:
            context.prec = 512
            for trade in selected:
                price_numerator, price_denominator = trade.price.as_integer_ratio()
                index, remainder = divmod(price_numerator * tick_denominator, price_denominator * tick_numerator)
                _location_require(remainder == 0, "OFF_TICK_TRADE_PRICE")
                bins[index] = bins.get(index, Decimal(0)) + trade.quantity
            _location_require(max(bins) - min(bins) + 1 <= MAX_PROFILE_BINS, "PROFILE_TICK_SPAN_BOUND_EXCEEDED")
            rows = tuple((index, product.tick_size * index, bins[index]) for index in sorted(bins))
            total = sum(bins.values(), Decimal(0))
            poc = min(bins, key=lambda index: (-bins[index], index))
            value = product.tick_size * poc
        identity = _trade_location_identity(method, selected, key=key, cutoff_ns=cutoff_ns,
                                           start_ns=window_start_ns, configuration=product.content_hash)
        return TradeLocationDiagnostic(method, "AVAILABLE", None, value, cutoff_ns, window_start_ns,
                                       refs, len(selected), total, rows, product.tick_size, identity)
    except (_LocationNotEstimable, ValueError, DecimalException) as exc:
        reason = str(exc) if isinstance(exc, _LocationNotEstimable) else "INVALID_TRADE_LOCATION_INPUT"
        return TradeLocationDiagnostic(method, "NOT_ESTIMABLE", reason, None, cutoff_ns, window_start_ns, refs)
