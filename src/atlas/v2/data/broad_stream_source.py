"""Bounded dual-venue stream plan and capture composition for broad V2 data."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

from .._serialization import sha256_json, timestamp
from ..instruments import EnvironmentV2, InstrumentKeyV2, ProductContractV2, ProductTypeV2, TradingStatusV2, VenueV2
from ..memory.repository import OpsRepository
from .durable_public_capture import DurablePublicCaptureV1, SealedPublicTransportV1
from .microstructure import L2SnapshotV2
from .public_microstructure_ws import (
    DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
    DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
    DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
    CapturedPublicFrameV2,
    SharedPublicFrameBudgetV1,
    parse_binance_aggtrade,
    parse_binance_depth_frame,
    parse_binance_rest_snapshot,
    parse_bybit_orderbook_frame,
    parse_bybit_trades,
)
from .public_stream_source import PublicStreamSourceV2

MAX_BROAD_STREAM_INSTRUMENTS_V2 = 16
MAX_BROAD_STREAM_DEPTH_INSTRUMENTS_V2 = 8
MAX_BROAD_STREAM_WATCHES_V2 = 512
MAX_BROAD_STREAM_TOPICS_V2 = 32
MAX_BROAD_STREAM_QUEUE_ITEMS_V2 = DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS
MAX_BROAD_STREAM_QUEUE_BYTES_V2 = DEFAULT_PUBLIC_FRAME_QUEUE_BYTES
MAX_BROAD_STREAM_DRAIN_ITEMS_V2 = DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS
# Broad ingestion interprets each sealed unit on the sole repository writer.
# Smaller extents cap per-call interpretation work; the 64-entry pending-batch
# budget still provides bounded burst capacity across the enabled venues.
MAX_BROAD_CAPTURE_BATCH_FRAMES_V2 = 16


@dataclass(frozen=True)
class BroadStreamIdentityV2:
    venue: VenueV2
    channel: str
    key: InstrumentKeyV2


@dataclass(frozen=True)
class BroadPublicStreamPlanV2:
    created_at_ns: int
    plan_id: str
    keys: tuple[InstrumentKeyV2, ...]
    lane_topics: Mapping[str, tuple[str, ...]]
    identities: tuple[BroadStreamIdentityV2, ...]
    source_refs: tuple[str, ...]

    @classmethod
    def build(
        cls,
        products: tuple[ProductContractV2, ...],
        tiers: Mapping[InstrumentKeyV2, Any],
        *,
        created_at_ns: int,
        benchmark_keys: tuple[InstrumentKeyV2, ...] = (),
        open_positions: tuple[InstrumentKeyV2, ...] = (),
        active_watch_keys: tuple[InstrumentKeyV2, ...] = (),
    ) -> BroadPublicStreamPlanV2:
        timestamp(created_at_ns, field="stream plan creation time")
        product_by_key = {item.key: item for item in products}
        if len(product_by_key) != len(products):
            raise ValueError("stream plan products must have unique full instrument identities")
        watch_set = set(active_watch_keys)
        if len(watch_set) != len(active_watch_keys) or len(watch_set) > MAX_BROAD_STREAM_WATCHES_V2:
            raise ValueError("broad stream active watch population exceeds its explicit bound")
        selected = {
            key for key, tier in tiers.items()
            if int(tier) >= 3
        } | set(benchmark_keys) | set(open_positions) | watch_set
        if len(selected) > MAX_BROAD_STREAM_INSTRUMENTS_V2:
            raise ValueError("broad stream instrument population exceeds its explicit cap")
        if not selected:
            raise ValueError("broad stream plan requires at least one tier3, benchmark, position, or watch key")
        for key in selected:
            product = product_by_key.get(key)
            if (product is None or key.venue not in (VenueV2.BYBIT, VenueV2.BINANCE)
                    or key.environment != EnvironmentV2.MAINNET or key.product != ProductTypeV2.LINEAR_PERPETUAL
                    or key.quote_asset != "USDT" or key.settlement_asset != "USDT"
                    or product.available_at_ns > created_at_ns
                    or product.trading_status != TradingStatusV2.TRADING):
                raise ValueError("broad stream key lacks an observed active USDT linear contract")
        depth_keys = {key for key in selected if int(tiers.get(key, 0)) >= 3 or key in open_positions or key in watch_set}
        if len(depth_keys) > MAX_BROAD_STREAM_DEPTH_INSTRUMENTS_V2:
            raise ValueError("broad stream deep subscription population exceeds its explicit cap")
        bybit_topics: list[str] = []
        binance_depth: list[str] = []
        binance_trades: list[str] = []
        identities: list[BroadStreamIdentityV2] = []
        for key in sorted(selected, key=lambda item: item.to_canonical_json()):
            if key.venue == VenueV2.BYBIT:
                trade = f"publicTrade.{key.native_symbol}"
                channels = ((f"orderbook.50.{key.native_symbol}", trade)
                            if key in depth_keys else (trade,))
                bybit_topics.extend(channels)
            else:
                depth = f"{key.native_symbol.lower()}@depth@100ms"
                trade = f"{key.native_symbol.lower()}@aggTrade"
                channels = (depth, trade) if key in depth_keys else (trade,)
                if key in depth_keys:
                    binance_depth.append(depth)
                binance_trades.append(trade)
            identities.extend(BroadStreamIdentityV2(key.venue, channel, key) for channel in channels)
        if len(identities) > MAX_BROAD_STREAM_TOPICS_V2:
            raise ValueError("broad stream topic population exceeds its explicit cap")
        lanes: dict[str, tuple[str, ...]] = {}
        if bybit_topics:
            lanes["BYBIT"] = tuple(sorted(bybit_topics))
        if binance_depth:
            lanes["BINANCE_DEPTH"] = tuple(sorted(binance_depth))
        if binance_trades:
            lanes["BINANCE_MARKET"] = tuple(sorted(binance_trades))
        source_refs = tuple(sorted({product_by_key[key].content_hash for key in selected}))
        body = {"created_at_ns": created_at_ns, "keys": [k.to_dict() for k in sorted(selected, key=lambda x: x.to_canonical_json())],
                "lane_topics": lanes, "source_refs": list(source_refs)}
        return cls(created_at_ns, sha256_json({"artifact_type": "BroadPublicStreamPlanV2", "plan": body}),
                   tuple(sorted(selected, key=lambda x: x.to_canonical_json())), MappingProxyType(lanes),
                   tuple(sorted(identities, key=lambda row: (row.venue.value, row.channel))), source_refs)

    def key_for_frame(self, frame: CapturedPublicFrameV2) -> InstrumentKeyV2:
        lane = ("BYBIT" if frame.venue == VenueV2.BYBIT else
                "BINANCE_DEPTH" if "@depth" in frame.channel else "BINANCE_MARKET")
        if frame.source_id != f"{lane}_PUBLIC_WS_BROAD_V2":
            raise ValueError("stream frame source identity differs from its planned public lane")
        matches = [identity.key for identity in self.identities
                   if identity.venue == frame.venue and identity.channel == frame.channel]
        if len(matches) != 1:
            raise ValueError("stream frame does not bind exactly one venue-aware instrument identity")
        return matches[0]

    def parse_frame(self, frame: CapturedPublicFrameV2, *, processed_at_ns: int | None = None,
                    source_health: str = "UNKNOWN", source_health_ref: str | None = None) -> tuple[Any, ...]:
        key = self.key_for_frame(frame)
        if frame.venue == VenueV2.BYBIT:
            if frame.channel.startswith("orderbook.50."):
                return (parse_bybit_orderbook_frame(frame, instrument=key, processed_at_ns=processed_at_ns,
                    source_health=source_health, source_health_ref=source_health_ref),)
            if frame.channel.startswith("publicTrade."):
                return parse_bybit_trades(frame, instrument=key, processed_at_ns=processed_at_ns,
                    source_health=source_health, source_health_ref=source_health_ref)
        elif frame.venue == VenueV2.BINANCE:
            if "@depth" in frame.channel:
                return (parse_binance_depth_frame(frame, instrument=key, processed_at_ns=processed_at_ns,
                    source_health=source_health, source_health_ref=source_health_ref),)
            if frame.channel.endswith("@aggTrade"):
                return (parse_binance_aggtrade(frame, instrument=key, processed_at_ns=processed_at_ns,
                    source_health=source_health, source_health_ref=source_health_ref),)
        raise ValueError("broad stream frame channel is not in the frozen public market subset")

    def parse_binance_snapshot(self, key: InstrumentKeyV2, *, raw_payload_bytes: bytes,
                               source_id: str, received_at_ns: int, available_at_ns: int,
                               processed_at_ns: int, declared_depth: int = 100) -> L2SnapshotV2:
        if key.venue != VenueV2.BINANCE or key not in self.keys:
            raise ValueError("snapshot key is not an active Binance stream-plan identity")
        return parse_binance_rest_snapshot(
            raw_payload_bytes, instrument=key, source_id=source_id,
            channel="USD-M depth snapshot REST", received_at_ns=received_at_ns,
            available_at_ns=available_at_ns, processed_at_ns=processed_at_ns,
            declared_depth=declared_depth, source_health="UNKNOWN",
        )


class BroadPublicStreamSourceV2:
    """Composable stream lanes whose aggregate queues preserve 512/16MB caps."""

    def __init__(self, plan: BroadPublicStreamPlanV2, *, stream_factories: Mapping[str, Callable] | None = None,
                 clock_ns: Callable[[], int] | None = None) -> None:
        self.plan = plan
        self.topics = tuple(sorted(topic for topics in plan.lane_topics.values() for topic in topics))
        self.venue = "BROAD"
        self._clock_ns = clock_ns
        factories = dict(stream_factories or {})
        lane_names = tuple(sorted(plan.lane_topics))
        if set(factories) - set(lane_names):
            raise ValueError("stream factory provided for an absent plan lane")
        self._queue_budget = SharedPublicFrameBudgetV1(
            max_items=MAX_BROAD_STREAM_QUEUE_ITEMS_V2,
            max_bytes=MAX_BROAD_STREAM_QUEUE_BYTES_V2,
            reserve_items_per_lane=min(32, MAX_BROAD_STREAM_QUEUE_ITEMS_V2 // len(lane_names)),
            reserve_bytes_per_lane=min(1_000_000, MAX_BROAD_STREAM_QUEUE_BYTES_V2 // len(lane_names)),
        )
        self._lanes: dict[str, PublicStreamSourceV2] = {}
        for name in lane_names:
            topics = plan.lane_topics[name]
            venue = VenueV2.BYBIT if name == "BYBIT" else VenueV2.BINANCE
            kwargs: dict[str, Any] = {
                "venue": venue, "topics": topics,
                "source_id": f"{name}_PUBLIC_WS_BROAD_V2",
                # Local queue ceilings equal the global ceilings; the shared
                # budget enforces the aggregate and protects every other lane's
                # reserve, allowing active lanes to borrow idle capacity.
                "max_queue_items": MAX_BROAD_STREAM_QUEUE_ITEMS_V2,
                "max_queue_bytes": MAX_BROAD_STREAM_QUEUE_BYTES_V2,
                "max_drain_items": MAX_BROAD_STREAM_DRAIN_ITEMS_V2,
                "shared_budget": self._queue_budget, "budget_lane": name,
            }
            if name in factories:
                kwargs["stream_factory"] = factories[name]
            if clock_ns is not None:
                kwargs["clock_ns"] = clock_ns
            self._lanes[name] = PublicStreamSourceV2(**kwargs)
        self._drain_rotation = 0
        self._started: list[str] = []

    @property
    def lane_names(self) -> tuple[str, ...]:
        return tuple(self._lanes)

    def start(self) -> None:
        try:
            for name, source in self._lanes.items():
                source.start()
                self._started.append(name)
        except Exception:
            self.close()
            raise

    def drain(self, *, max_items: int | None = None) -> tuple[CapturedPublicFrameV2, ...]:
        limit = MAX_BROAD_STREAM_DRAIN_ITEMS_V2 if max_items is None else max_items
        if type(limit) is not int or not 0 < limit <= MAX_BROAD_STREAM_DRAIN_ITEMS_V2:
            raise ValueError("global stream drain exceeds the fixed batch bound")
        names = tuple(self._lanes)
        order = names[self._drain_rotation:] + names[:self._drain_rotation]
        self._drain_rotation = (self._drain_rotation + 1) % len(names)
        rows: list[CapturedPublicFrameV2] = []
        remaining = limit
        current_order = order
        # Allocate work among lanes that currently have data, then repeat to
        # redistribute any quota left unused by a quiet or concurrently drained
        # lane. This keeps the global drain bound while making a single busy lane
        # able to use the full capture batch.
        while remaining > 0:
            active = [name for name in current_order
                      if self._lanes[name].status().handoff.queue_items > 0]
            if not active:
                break
            quota = max(1, remaining // len(active))
            drained = 0
            for name in active:
                if remaining <= 0:
                    break
                batch = self._lanes[name].drain(max_items=min(quota, remaining))
                rows.extend(batch)
                drained += len(batch)
                remaining -= len(batch)
            if drained == 0:
                break
            # Rotate the first lane serviced if another bounded pass is needed.
            first = current_order[0]
            current_order = current_order[1:] + (first,)
        return tuple(sorted(rows, key=lambda frame: (frame.received_at_ns, frame.venue.value, frame.channel)))

    def request_close(self) -> None:
        for source in self._lanes.values():
            source.request_close()

    def close(self) -> None:
        errors = []
        for source in self._lanes.values():
            try:
                source.close()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("one or more broad public stream lanes failed bounded close") from errors[0]

    def status(self) -> Any:
        lane_status = {name: source.status() for name, source in self._lanes.items()}
        handoffs = [item.handoff for item in lane_status.values()]
        queued_items = sum(item.queue_items for item in handoffs)
        queued_bytes = sum(item.queue_bytes for item in handoffs)
        budget_items, budget_bytes, budget_high_items, budget_high_bytes, _ = self._queue_budget.snapshot()
        if queued_items != budget_items or queued_bytes != budget_bytes:
            raise RuntimeError("broad public handoff accounting differs from its shared budget")
        state = "FAILED" if any(item.state == "FAILED" for item in lane_status.values()) else (
            "CLOSED" if all(item.state == "CLOSED" for item in lane_status.values()) else
            "RUNNING" if all(item.state == "RUNNING" for item in lane_status.values()) else
            "DEGRADED" if any(item.state == "RUNNING" for item in lane_status.values()) else "CREATED")
        handoff = SimpleNamespace(
            venue="BROAD", topics=self.topics, queue_items=queued_items, queue_bytes=queued_bytes,
            max_queue_items=MAX_BROAD_STREAM_QUEUE_ITEMS_V2,
            max_queue_bytes=MAX_BROAD_STREAM_QUEUE_BYTES_V2,
            max_drain_items=MAX_BROAD_STREAM_DRAIN_ITEMS_V2,
            high_water_items=budget_high_items,
            high_water_bytes=budget_high_bytes,
            frames_received=sum(item.frames_received for item in handoffs),
            frames_drained=sum(item.frames_drained for item in handoffs),
            controls_received=sum(item.controls_received for item in handoffs),
            frames_rejected=sum(item.frames_rejected for item in handoffs),
            closed_rejections=sum(item.closed_rejections for item in handoffs),
            overflowed=any(item.overflowed for item in handoffs),
            backpressure=any(item.backpressure for item in handoffs),
            connected=all(item.connected for item in handoffs), closed=all(item.closed for item in handoffs),
            disconnect_count=sum(item.disconnect_count for item in handoffs),
            last_disconnect_at_ns=max((item.last_disconnect_at_ns for item in handoffs if item.last_disconnect_at_ns is not None), default=None),
            heartbeat_count=sum(item.heartbeat_count for item in handoffs),
            last_heartbeat_at_ns=max((item.last_heartbeat_at_ns for item in handoffs if item.last_heartbeat_at_ns is not None), default=None),
            last_activity_at_ns=max((item.last_activity_at_ns for item in handoffs if item.last_activity_at_ns is not None), default=None),
            last_error_code=next((item.last_error_code for item in handoffs if item.last_error_code), None),
            last_error_at_ns=max((item.last_error_at_ns for item in handoffs if item.last_error_at_ns is not None), default=None),
        )
        return SimpleNamespace(state=state, attempt_count=sum(s.attempt_count for s in lane_status.values()),
                               reconnect_count=sum(s.reconnect_count for s in lane_status.values()),
                               last_error_code=next((s.last_error_code for s in lane_status.values() if s.last_error_code), None),
                               handoff=handoff, lanes=lane_status, plan_id=self.plan.plan_id)


class BroadDurablePublicCaptureV2:
    """One S40 capture over all venue lanes under global frozen budgets.

    Keeping a single DurablePublicCaptureV1 means the 64 pending descriptor
    limit, FIFO archive, and receipt chronology apply across both venues.
    Lane separation remains in each frame's venue/source/channel identity and
    in the wrapped stream status.
    """

    def __init__(self, source: BroadPublicStreamSourceV2, *, clock_ns: Callable[[], int] | None = None) -> None:
        self.source = source
        self.venue = "BROAD"
        self.topics = source.topics
        options: dict[str, Any] = {"capture_batch_frames": MAX_BROAD_CAPTURE_BATCH_FRAMES_V2}
        if clock_ns is not None:
            options["clock_ns"] = clock_ns
        self._capture = DurablePublicCaptureV1(source, **options)

    def configure_capture(self, run_root: Path, *, capture_epoch: str = "0" * 64) -> None:
        # Extents share the run-level ``ops-public-extents`` namespace used by
        # SealedPublicTransportV1.adopt/read_extent. A nested capture root here
        # would make raw bytes durable but unreadable by the sole writer.
        self._capture.configure_capture(run_root, capture_epoch=capture_epoch)

    def recover_controller_capture(self, repository: OpsRepository) -> None:
        self._capture.recover_controller_capture(repository)

    def start(self) -> None:
        self._capture.start()

    def drain_sealed_transport(self) -> SealedPublicTransportV1 | None:
        return self._capture.drain_sealed_transport()

    def mark_controller_capture_clean(self, repository: OpsRepository) -> None:
        self._capture.mark_controller_capture_clean(repository)

    def request_pressure_stop(self) -> None:
        self._capture.request_pressure_stop()

    def close(self) -> None:
        self._capture.close()

    def status(self) -> Any:
        source_status = self.source.status()
        capture_status = self._capture.status()
        capture = dict(capture_status.capture)
        capture["version"] = "BROAD_DURABLE_PUBLIC_CAPTURE_V2"
        capture["lanes"] = source_status.lanes
        return SimpleNamespace(**vars(source_status), capture=capture,
                               pending_frames=capture_status.pending_frames)
