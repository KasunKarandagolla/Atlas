"""Incremental confirmed 15M price structure; research morphology only."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2


@dataclass(frozen=True)
class Swing:
    kind: str
    price: Decimal
    pivot_ref: str
    pivot_at_ns: int
    confirmation_ref: str
    confirmed_at_ns: int


@dataclass(frozen=True)
class StructureEvent:
    kind: str
    bar_ref: str
    confirmed_at_ns: int
    swing_ref: str | None = None
    related_ref: str | None = None
    price: Decimal | None = None
    age_bars: int | None = None
    touches: int | None = None
    mitigated: bool | None = None
    normalized_distance: float | None = None


def confirmed_swings(bars: tuple[CausalBarV2, ...]) -> tuple[Swing, ...]:
    _validate(bars)
    output: list[Swing] = []
    for i in range(2, len(bars) - 2):
        candidate = bars[i]
        neighborhood = bars[i - 2:i] + bars[i + 1:i + 3]
        if all(candidate.high > other.high for other in neighborhood):
            output.append(Swing("HIGH", candidate.high, candidate.content_hash, candidate.close_at_ns,
                                bars[i + 2].content_hash, bars[i + 2].close_at_ns))
        if all(candidate.low < other.low for other in neighborhood):
            output.append(Swing("LOW", candidate.low, candidate.content_hash, candidate.close_at_ns,
                                bars[i + 2].content_hash, bars[i + 2].close_at_ns))
    return tuple(output)


def _validate(bars: tuple[CausalBarV2, ...]) -> None:
    if any(not bar.final or bar.interval != BarIntervalV2.M15 for bar in bars):
        raise ValueError("structure requires final 15M bars")
    if any(bar.instrument_revision != bars[0].instrument_revision for bar in bars) if bars else False:
        raise ValueError("mixed instrument revision")
    if any(bars[i].close_at_ns >= bars[i + 1].close_at_ns for i in range(len(bars) - 1)):
        raise ValueError("unordered bars")


def structure_events(bars: tuple[CausalBarV2, ...], atr_values: tuple[float | None, ...]) -> tuple[StructureEvent, ...]:
    """At each close use swings confirmed strictly before the current bar."""
    _validate(bars)
    if len(atr_values) != len(bars):
        raise ValueError("ATR series length mismatch")
    events: list[StructureEvent] = []
    direction: str | None = None
    broken: set[str] = set()
    blocks: list[tuple[StructureEvent, int, int, bool]] = []
    for i, bar in enumerate(bars):
        if i >= 2:
            older = bars[i - 2]
            if bar.low > older.high:
                events.append(StructureEvent("FVG_BULL", bar.content_hash, bar.close_at_ns, related_ref=older.content_hash, price=older.high))
            if bar.high < older.low:
                events.append(StructureEvent("FVG_BEAR", bar.content_hash, bar.close_at_ns, related_ref=older.content_hash, price=older.low))
        atr = atr_values[i]
        updated_blocks: list[tuple[StructureEvent, int, int, bool]] = []
        for block, created_index, touches, mitigated in blocks:
            assert block.price is not None
            touched = bar.low <= block.price <= bar.high
            touches += int(touched)
            mitigated = mitigated or touched  # exact-level price touch proxy only
            updated_blocks.append((block, created_index, touches, mitigated))
            events.append(StructureEvent(block.kind.replace("CANDIDATE", "UPDATE"), bar.content_hash,
                bar.close_at_ns, swing_ref=block.swing_ref, related_ref=block.related_ref, price=block.price,
                age_bars=i - created_index, touches=touches, mitigated=mitigated,
                normalized_distance=float(abs(bar.close - block.price) / Decimal(str(atr))) if atr is not None and atr > 0 else None))
        blocks = updated_blocks
        if atr is None or atr <= 0:
            continue
        threshold = Decimal(str(atr)) * Decimal("0.1")
        known = [s for s in confirmed_swings(bars[:i]) if s.confirmed_at_ns < bar.close_at_ns]
        for kind, swing_kind in (("UP", "HIGH"), ("DOWN", "LOW")):
            swings = [s for s in known if s.kind == swing_kind]
            if not swings:
                continue
            swing = swings[-1]
            crossed = bar.close > swing.price + threshold if kind == "UP" else bar.close < swing.price - threshold
            pierced = bar.high > swing.price + threshold and bar.close <= swing.price if kind == "UP" else bar.low < swing.price - threshold and bar.close >= swing.price
            if pierced:
                events.append(StructureEvent("SWEEP_" + kind, bar.content_hash, bar.close_at_ns, swing_ref=swing.pivot_ref, price=swing.price))
            if crossed and swing.pivot_ref not in broken:
                change = direction is not None and direction != kind
                events.append(StructureEvent("CHOCH_" + kind if change else "BOS_" + kind,
                                             bar.content_hash, bar.close_at_ns, swing_ref=swing.pivot_ref, price=swing.price))
                broken.add(swing.pivot_ref)
                direction = kind
                opposite = [x for x in bars[max(0, i - 5):i] if (x.close < x.open if kind == "UP" else x.close > x.open)]
                if opposite:
                    candle = opposite[-1]
                    price = candle.low if kind == "UP" else candle.high
                    block = StructureEvent("ORDER_BLOCK_CANDIDATE_" + kind, bar.content_hash, bar.close_at_ns,
                                           swing_ref=swing.pivot_ref, related_ref=candle.content_hash, price=price,
                                           age_bars=i - bars.index(candle), touches=0, mitigated=False,
                                           normalized_distance=float(abs(bar.close - price) / Decimal(str(atr))))
                    events.append(block)
                    blocks.append((block, bars.index(candle), 0, False))
    return tuple(events)


@dataclass(frozen=True)
class ZoneVersion:
    zone_id: str
    version: int
    kind: str
    price: Decimal
    created_at_ns: int
    updated_at_ns: int
    pivot_refs: tuple[str, ...]
    touches: int
    broken: bool


def support_resistance(bars: tuple[CausalBarV2, ...], atr_values: tuple[float | None, ...]) -> tuple[ZoneVersion, ...]:
    """Append immutable versions. Merge distance is fixed at creation event."""
    _validate(bars)
    if len(atr_values) != len(bars):
        raise ValueError("ATR series length mismatch")
    latest: dict[str, ZoneVersion] = {}
    versions: list[ZoneVersion] = []
    for i, bar in enumerate(bars):
        new = [s for s in confirmed_swings(bars[:i + 1]) if s.confirmed_at_ns == bar.close_at_ns]
        atr = atr_values[i]
        for swing in sorted(new, key=lambda s: (s.kind, s.pivot_ref)):
            distance = Decimal(str(atr)) * Decimal("0.25") if atr is not None and atr > 0 else None
            eligible = sorted((z for z in latest.values() if z.kind == swing.kind and not z.broken and distance is not None and abs(z.price - swing.price) <= distance),
                              key=lambda z: (abs(z.price - swing.price), z.zone_id))
            if eligible:
                old = eligible[0]
                updated = ZoneVersion(old.zone_id, old.version + 1, old.kind, old.price, old.created_at_ns, bar.close_at_ns,
                                      old.pivot_refs + (swing.pivot_ref,), old.touches, old.broken)
            else:
                updated = ZoneVersion(swing.pivot_ref, 1, swing.kind, swing.price, bar.close_at_ns, bar.close_at_ns,
                                      (swing.pivot_ref,), 0, False)
            latest[updated.zone_id] = updated
            versions.append(updated)
        # A newly created zone cannot receive a touch from its confirming bar.
        for old in tuple(sorted(latest.values(), key=lambda z: z.zone_id)):
            if old.created_at_ns >= bar.close_at_ns or old.broken:
                continue
            touched = bar.low <= old.price <= bar.high
            broken = bar.close > old.price if old.kind == "HIGH" else bar.close < old.price
            if touched or broken:
                updated = ZoneVersion(old.zone_id, old.version + 1, old.kind, old.price, old.created_at_ns,
                                      bar.close_at_ns, old.pivot_refs, old.touches + int(touched), broken)
                latest[old.zone_id] = updated
                versions.append(updated)
    return tuple(versions)


@dataclass(frozen=True)
class ConfirmedLeg:
    start: Swing
    end: Swing
    direction: str
    amplitude: Decimal
    duration_ns: int
    confirmed_at_ns: int


def confirmed_legs(swings: tuple[Swing, ...]) -> tuple[ConfirmedLeg, ...]:
    legs: list[ConfirmedLeg] = []
    for swing in swings:
        if not legs:
            if swings[0] is swing:
                continue
            previous = next((s for s in reversed(swings[:swings.index(swing)]) if s.kind != swing.kind), None)
        else:
            previous = legs[-1].end
            if previous.kind == swing.kind:
                continue
        if previous is None or previous.pivot_at_ns >= swing.pivot_at_ns:
            continue
        legs.append(ConfirmedLeg(previous, swing, "UP" if swing.price > previous.price else "DOWN",
                                 abs(swing.price - previous.price), swing.pivot_at_ns - previous.pivot_at_ns,
                                 swing.confirmed_at_ns))
    return tuple(legs)


def fibonacci(leg: ConfirmedLeg) -> dict[str, Decimal]:
    sign = Decimal(1) if leg.direction == "UP" else Decimal(-1)
    return {ratio: leg.end.price - sign * leg.amplitude * Decimal(ratio) for ratio in ("0.382", "0.5", "0.618")}


def morphology(legs: tuple[ConfirmedLeg, ...], *, bars: tuple[CausalBarV2, ...] = ()) -> dict[str, float | None]:
    if not legs:
        return {"amplitude": None, "duration_ns": None, "retracement_ratio": None,
                "extension_ratio": None, "overlap": None, "acceleration": None, "volume_behavior": None,
                "multi_scale_ratio": None}
    current = legs[-1]
    previous = legs[-2] if len(legs) >= 2 else None
    prior_two = legs[-3] if len(legs) >= 3 else None
    def leg_volume(leg: ConfirmedLeg) -> float | None:
        observed = [float(bar.volume) for bar in bars if bar.final and
                    leg.start.pivot_at_ns < bar.close_at_ns <= leg.end.pivot_at_ns and
                    bar.close_at_ns <= leg.confirmed_at_ns]
        return sum(observed) / len(observed) if observed else None

    current_volume = leg_volume(current) if bars else None
    previous_volume = leg_volume(previous) if bars and previous else None
    return {
        "amplitude": float(current.amplitude), "duration_ns": float(current.duration_ns),
        "retracement_ratio": float(current.amplitude / previous.amplitude) if previous and previous.amplitude else None,
        "extension_ratio": float(current.amplitude / prior_two.amplitude) if prior_two and prior_two.amplitude else None,
        "overlap": float(max(Decimal(0), min(max(current.start.price, current.end.price), max(previous.start.price, previous.end.price)) - max(min(current.start.price, current.end.price), min(previous.start.price, previous.end.price)))) if previous else None,
        "acceleration": float(current.amplitude / Decimal(current.duration_ns) - previous.amplitude / Decimal(previous.duration_ns)) if previous and current.duration_ns and previous.duration_ns else None,
        "volume_behavior": current_volume / previous_volume if current_volume is not None and previous_volume is not None and previous_volume > 0 else None,
        "multi_scale_ratio": float(current.amplitude / sum((x.amplitude for x in legs[-3:]), Decimal(0))),
    }
