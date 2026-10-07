"""Bounded native Nautilus host for the Bybit demo OMS boundary.

Construction only builds in-process SDK objects. ``run`` is the sole method
that starts the native node or opens a venue connection.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import queue
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, cast

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.domain.execution import Observation, validate_client_order_id
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.bybit_demo import (
    BybitDemoIdentity,
    DemoCredential,
    build_bybit_demo_config,
    dispatch_persisted_bybit_command,
)
from atlas.runtime.fill_dedup import FillRecord, OrderStatusRecord
from atlas.v2.instruments import ProductContractV2

COMMAND_QUEUE_CAPACITY = 32
EVENT_QUEUE_CAPACITY = 256
MAX_COMMANDS_PER_TICK = 8
TICK_INTERVAL_NS = 100_000_000
RECONCILIATION_INTERVAL_NS = 1_500_000_000
_ALLOWED_COMMANDS = frozenset({CommandType.CANCEL_ENTRY, CommandType.SUBMIT_EXIT,
                               CommandType.FLATTEN})


@dataclass(frozen=True)
class BybitNativeEvent:
    event_type: str
    client_order_id: str | None
    order_id: str | None
    instrument_id: str | None
    status: str | None
    quantity: str | None
    price: str | None
    received_at_ns: int
    trade_id: str | None = None
    side: str | None = None
    fee: str | None = None
    fee_currency: str | None = None
    trade_time_ns: int | None = None
    cumulative_quantity: str | None = None
    average_price: str | None = None


class _StrategyHost:
    """Small OMS facade consumed by ``BybitNautilusDemoPort``."""

    def __init__(self, strategy: Any) -> None:
        self.strategy = strategy

    @property
    def order_factory(self) -> Any:
        return self.strategy.order_factory

    def submit_order(self, order: Any, *, params: dict[str, Any] | None = None) -> None:
        self.strategy.submit_order(order, params=params)

    def cancel_order(self, order: Any) -> None:
        self.strategy.cancel_order(order.client_order_id)

    def find_order(self, client_order_id: str) -> Any:
        from nautilus_trader.model import ClientOrderId

        return self.strategy.cache.order(ClientOrderId(client_order_id))


def _safe_text(value: Any, *, maximum: int = 128) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        text = str(value)
        return text if text and len(text) <= maximum else None
    return None


def _native_id(value: Any, native_type: type) -> str | None:
    # Only stringify an SDK identity of the expected kind, never arbitrary objects.
    return _safe_text(str(value) if isinstance(value, native_type) else value)


def _native_decimal(value: Any, native_type: type, *, positive: bool = False) -> str | None:
    if isinstance(value, native_type):
        value = cast(Any, value).as_decimal()
    if not isinstance(value, (str, Decimal, int)) or isinstance(value, bool):
        return None
    try:
        number = Decimal(value)
    except (ValueError, ArithmeticError):
        return None
    if not number.is_finite() or number < 0 or (positive and number == 0):
        return None
    return _safe_text(str(number))


def _native_strategy_type() -> type:
    from nautilus_trader.trading import Strategy

    class BybitDemoStrategy(Strategy):
        def __init__(self) -> None:
            super().__init__()
            self.runtime: BybitNativeNode | None = None
            self._port: Any | None = None
            self._timer_started = False

        def bind_runtime(self, runtime: BybitNativeNode) -> None:
            self.runtime = runtime

        def on_start(self) -> None:
            if self.runtime is None:
                self.fault()
                return
            self._port = self.runtime.make_port(_StrategyHost(self))
            self._timer_started = True
            self.clock.set_timer_ns(
                "atlas-bybit-command-drain",
                TICK_INTERVAL_NS,
                callback=self._on_drain_timer,
                fire_immediately=True,
            )

        def on_stop(self) -> None:
            if self._timer_started:
                self.clock.cancel_timer("atlas-bybit-command-drain")
                self._timer_started = False

        def _capture(self, event: Any) -> None:
            if self.runtime is None:
                return
            order = self.cache.order(event.client_order_id)
            self.runtime.capture_native_event(event, self.clock.timestamp_ns(), order=order)

        def on_order_submitted(self, event: Any) -> None:
            self._capture(event)

        def on_order_accepted(self, event: Any) -> None:
            self._capture(event)

        def on_order_rejected(self, event: Any) -> None:
            self._capture(event)

        def on_order_canceled(self, event: Any) -> None:
            self._capture(event)

        def on_order_expired(self, event: Any) -> None:
            self._capture(event)

        def on_order_filled(self, event: Any) -> None:
            self._capture(event)

        def on_order_updated(self, event: Any) -> None:
            self._capture(event)

        def on_order_cancel_rejected(self, event: Any) -> None:
            self._capture(event)

        def on_order_modify_rejected(self, event: Any) -> None:
            self._capture(event)

        def _on_drain_timer(self, _event: Any) -> None:
            if self.runtime is None or self._port is None:
                return
            if self.runtime.stopped or self.runtime.queue_overflow:
                self.runtime.drain_events_to_journal()
                return
            self.runtime.request_reconciliation()
            if self.runtime.stopped:
                self.degrade()
                return
            self.runtime.drain_events_to_journal()
            if self.runtime.stopped or not self.runtime.event_queue.empty():
                return
            if not self.runtime.reconciliation_ready:
                return
            for _ in range(MAX_COMMANDS_PER_TICK):
                if self.runtime.stopped or self.runtime.queue_overflow:
                    break
                # Keep UNSENT commands queued while the worker refreshes account evidence.
                try:
                    self.runtime._validate_snapshot(
                        self.runtime.account_snapshot_getter(), self.clock.timestamp_ns(),
                    )
                except Exception:
                    break
                try:
                    command_id = self.runtime.command_queue.get_nowait()
                except queue.Empty:
                    break
                with self.runtime._queue_lock:
                    self.runtime._queued_command_ids.discard(command_id)
                self.runtime.command_queue.task_done()
                self.runtime._dispatch(command_id, self._port, self.clock.timestamp_ns())

    return BybitDemoStrategy


class BybitNativeNode:
    """One-process Bybit demo writer with bounded queues and no retry loop."""

    def __init__(
        self,
        *,
        identity: BybitDemoIdentity,
        credential: DemoCredential,
        product: ProductContractV2,
        journal: SQLiteJournal,
        writer_epoch: int,
        assert_writer: Callable[[], None],
        reconcile_once: Callable[[], None],
        account_snapshot_getter: Callable[[], Any],
        market_filters: Any,
        port_factory: Callable[..., Any] | None = None,
        node_builder_factory: Callable[..., Any] | None = None,
    ) -> None:
        if writer_epoch < 0:
            raise ValueError("invalid Bybit writer epoch")
        if (product.key.venue.value != "BYBIT"
                or product.key.environment.value != identity.environment):
            raise ValueError("Bybit demo product identity required")
        self.identity = identity
        self.journal = journal
        self.writer_epoch = writer_epoch
        self.assert_writer = assert_writer
        self.reconcile_once = reconcile_once
        self.account_snapshot_getter = account_snapshot_getter
        self.market_filters = market_filters
        self._product = product
        self._port_factory = port_factory
        self.command_queue: queue.Queue[str] = queue.Queue(maxsize=COMMAND_QUEUE_CAPACITY)
        self.event_queue: queue.Queue[BybitNativeEvent] = queue.Queue(maxsize=EVENT_QUEUE_CAPACITY)
        self._queued_command_ids: set[str] = set()
        self._queue_lock = threading.Lock()
        self._run_lock = asyncio.Lock()
        self._run_task: asyncio.Future[None] | None = None
        self._disposed_unstarted = False
        self._reconciliation_lock = threading.Lock()
        self._reconciliation_worker: threading.Thread | None = None
        self._last_reconciliation_request_ns = 0
        self.reconciliation_ready = False
        self.stopped = False
        self.queue_overflow = False
        self.last_failure_code: str | None = None
        self.strategy: Any | None = None

        strategy_type = _native_strategy_type()
        self.strategy = strategy_type()
        self.strategy.bind_runtime(self)
        native_config = build_bybit_demo_config(identity, credential)
        if node_builder_factory is None:
            from nautilus_trader.common import Environment
            from nautilus_trader.live import LiveNode
            from nautilus_trader.model import TraderId

            builder = LiveNode.builder(
                "atlas-bybit-demo",
                TraderId("ATLAS-BYBIT-DEMO"),
                Environment.LIVE,
            )
        else:
            builder = node_builder_factory("atlas-bybit-demo", identity, native_config)
        builder = builder.add_exec_client("BYBIT", self._factory(), native_config)
        builder = builder.with_reconciliation(True)
        self.node = builder.build()
        self.node.add_strategy(self.strategy)
        # rc5 transfers the wrapper into run_async; this handle stays usable.
        self._node_handle = self.node.handle()

    @staticmethod
    def _factory() -> Any:
        from nautilus_trader.adapters.bybit import BybitExecutionClientFactory

        return BybitExecutionClientFactory()

    def make_port(self, host: Any) -> Any:
        if self._port_factory is not None:
            return self._port_factory(host, self.account_snapshot_getter, self.market_filters)
        from atlas.runtime.bybit_demo import BybitNautilusDemoPort

        return BybitNautilusDemoPort(
            self.identity,
            host,
            self._product,
            account_snapshot_getter=self.account_snapshot_getter,
            market_filters=self.market_filters,
        )

    def bind_product(self, product: ProductContractV2) -> None:
        """Internal construction hook used before the strategy asks for its port."""
        self._product = product

    def _validate_snapshot(self, snapshot: Any, now_ns: int) -> None:
        from atlas.runtime.bybit_demo import BybitDemoSnapshot
        if (not isinstance(snapshot, BybitDemoSnapshot)
                or not snapshot.profile_qualified or snapshot.native_symbol != self._product.key.native_symbol
                or getattr(snapshot, "identity_hash", None) != self.identity.content_hash
                or getattr(snapshot, "eligible", False) is not True
                or type(getattr(snapshot, "captured_at_ns", None)) is not int
                or not 0 <= now_ns - snapshot.captured_at_ns <= 2_000_000_000):
            raise PersistenceError("Bybit account snapshot is not qualified")

    def _validate_intent_scope(self, intent_id: str, client_order_id: str) -> None:
        scopes = set()
        for command in self.journal.load_commands_for_intent(intent_id):
            payload = json.loads(command.payload)
            if payload.get("client_order_id") == client_order_id:
                scopes.add((payload.get("symbol"), payload.get("instrument_ref"), payload.get("identity_hash")))
        if scopes != {(self._product.key.native_symbol, self._product.content_hash, self.identity.content_hash)}:
            raise ValueError("Bybit native event scope does not match durable intent")

    def request_reconciliation(self) -> None:
        """Start at most one read worker; never queue timer ticks behind a slow read."""
        if self.stopped or self.queue_overflow:
            return
        with self._reconciliation_lock:
            if self._reconciliation_worker is not None and self._reconciliation_worker.is_alive():
                return
            monotonic_ns = time.monotonic_ns()
            if monotonic_ns - self._last_reconciliation_request_ns < RECONCILIATION_INTERVAL_NS:
                return
            self._last_reconciliation_request_ns = monotonic_ns

            def reconcile() -> None:
                try:
                    self.assert_writer()
                    self.reconcile_once()
                    self.assert_writer()
                    self.reconciliation_ready = True
                except Exception:
                    self.stopped = True
                    self.last_failure_code = "BYBIT_RECONCILIATION_UNAVAILABLE"

            self._reconciliation_worker = threading.Thread(
                target=reconcile, name="atlas-bybit-reconciliation", daemon=True,
            )
            self._reconciliation_worker.start()

    def enqueue_command(self, command_id: str) -> None:
        if self.stopped or self.queue_overflow:
            raise PersistenceError("Bybit native runtime is stopped")
        command = self.journal.load_command(command_id)
        if command.command_type not in _ALLOWED_COMMANDS:
            raise PersistenceError("Bybit native command type refused")
        if command.send_started_at_ns is not None or command.outcome != CommandOutcome.UNSENT:
            raise PersistenceError("Bybit command requires reconciliation")
        with self._queue_lock:
            if command_id in self._queued_command_ids:
                return
            try:
                self.command_queue.put_nowait(command_id)
            except queue.Full:
                self._trip_overflow()
                raise PersistenceError("Bybit command queue overflow") from None
            self._queued_command_ids.add(command_id)

    def _dispatch(self, command_id: str, port: Any, now_ns: int) -> None:
        if self.stopped or self.queue_overflow:
            return
        try:
            snapshot = self.account_snapshot_getter()
            self._validate_snapshot(snapshot, now_ns)
            result = dispatch_persisted_bybit_command(
                journal=self.journal,
                command_id=command_id,
                port=port,
                now_ns=now_ns,
                writer_epoch=self.writer_epoch,
                assert_writer=self._assert_effect_writer,
            )
            self._push_event(BybitNativeEvent(
                event_type="COMMAND_OUTCOME",
                client_order_id=None,
                order_id=command_id,
                instrument_id=None,
                status=result.outcome.value,
                quantity=None,
                price=None,
                received_at_ns=now_ns,
            ))
        except Exception:
            self.last_failure_code = "BYBIT_NATIVE_COMMAND_UNRESOLVED"
            # The dispatcher writes UNKNOWN before effect and refuses replay.
            # Never publish exception text or retry the command automatically.

    def _assert_effect_writer(self) -> None:
        if self.stopped or self.queue_overflow:
            raise PersistenceError("Bybit native runtime is stopped")
        self.assert_writer()
        if self.stopped or self.queue_overflow:
            raise PersistenceError("Bybit native runtime is stopped")

    def capture_native_event(self, raw_event: Any, received_at_ns: int, *, order: Any = None) -> None:
        from nautilus_trader import model

        event_type = type(raw_event).__name__
        if event_type not in {
            "OrderSubmitted", "OrderAccepted", "OrderRejected", "OrderCanceled", "OrderExpired",
            "OrderFilled", "OrderUpdated", "OrderCancelRejected", "OrderModifyRejected",
        }:
            return
        if not isinstance(raw_event, getattr(model, event_type)) or type(received_at_ns) is not int or received_at_ns <= 0:
            self._capture_unresolved(event_type, received_at_ns, "BYBIT_NATIVE_EVENT_FIELDS_UNRESOLVED")
            return
        raw_client_id = _native_id(getattr(raw_event, "client_order_id", None), model.ClientOrderId)
        client_id: str | None = None
        if raw_client_id is not None:
            try:
                client_id = validate_client_order_id(raw_client_id)
            except ValueError:
                self._capture_unresolved(event_type, received_at_ns, "BYBIT_NATIVE_EVENT_ID_UNRESOLVED")
                return
        instrument_id = _native_id(getattr(raw_event, "instrument_id", None), model.InstrumentId)
        account_id = _native_id(getattr(raw_event, "account_id", None), model.AccountId)
        if (client_id is None or instrument_id != f"{self._product.key.native_symbol}-LINEAR.BYBIT"
                or (account_id is not None and account_id != f"BYBIT-{self.identity.account_scope_ref}")):
            self._capture_unresolved(event_type, received_at_ns, "BYBIT_NATIVE_EVENT_ID_UNRESOLVED")
            return
        quantity = _native_decimal(getattr(raw_event, "last_qty", getattr(raw_event, "quantity", None)), model.Quantity,
                                   positive=event_type == "OrderFilled")
        price = _native_decimal(getattr(raw_event, "last_px", getattr(raw_event, "price", None)), model.Price,
                                positive=event_type == "OrderFilled")
        trade_id = _native_id(getattr(raw_event, "trade_id", None), model.TradeId)
        event_time = getattr(raw_event, "ts_event", None)
        fee = None
        fee_currency = None
        commission = getattr(raw_event, "commission", None)
        if isinstance(commission, model.Money):
            fee = _native_decimal(commission.as_decimal(), model.Quantity)
            fee_currency = _native_id(commission.currency, model.Currency)
        side_value = getattr(raw_event, "order_side", None)
        side = "Buy" if side_value == model.OrderSide.BUY else "Sell" if side_value == model.OrderSide.SELL else None
        if event_type == "OrderFilled" and (
            quantity is None or price is None or trade_id is None or side is None
            or fee is None or fee_currency is None or type(event_time) is not int
            or not 0 < event_time <= received_at_ns
        ):
            self._capture_unresolved(event_type, received_at_ns, "BYBIT_NATIVE_EVENT_FIELDS_UNRESOLVED")
            return
        status = {
            "OrderSubmitted": "SUBMITTED", "OrderAccepted": "NEW", "OrderRejected": "REJECTED",
            "OrderCanceled": "CANCELED", "OrderExpired": "EXPIRED",
        }.get(event_type, "UNKNOWN")
        if event_type == "OrderFilled" and order is not None:
            native_status = getattr(order, "status", None)
            if native_status == model.OrderStatus.FILLED:
                status = "FILLED"
            elif native_status == model.OrderStatus.PARTIALLY_FILLED:
                status = "PARTIALLY_FILLED"
        observation = BybitNativeEvent(
            event_type=event_type,
            client_order_id=client_id,
            order_id=_native_id(getattr(raw_event, "venue_order_id", None), model.VenueOrderId),
            instrument_id=instrument_id,
            status=status,
            quantity=quantity,
            price=price,
            received_at_ns=received_at_ns,
            trade_id=trade_id, side=side, fee=fee, fee_currency=fee_currency,
            trade_time_ns=event_time if type(event_time) is int and 0 < event_time <= received_at_ns else None,
            cumulative_quantity=_native_decimal(getattr(order, "filled_qty", None), model.Quantity),
            average_price=_native_decimal(getattr(order, "avg_px", None), model.Price),
        )
        self._push_event(observation)

    def _capture_unresolved(self, event_type: str, received_at_ns: int, code: str) -> None:
        self.last_failure_code = code
        self.stopped = True
        if type(received_at_ns) is int and received_at_ns > 0:
            self._push_event(BybitNativeEvent(event_type, None, None, None, "UNKNOWN",
                                              None, None, received_at_ns))

    def drain_events_to_journal(self, max_items: int = 64) -> tuple[BybitNativeEvent, ...]:
        """Persist a bounded event batch before returning observations for publication.

        Status and fills are evidence only. They never release a reservation,
        resolve UNKNOWN sends, or infer that a canceled order had no fills.
        """
        if type(max_items) is not int or not 1 <= max_items <= 64:
            raise ValueError("invalid Bybit native event drain bound")
        published: list[BybitNativeEvent] = []
        for _ in range(max_items):
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self.journal._transaction_lock.acquire()
            try:
                self.assert_writer()
                raw_hash = hashlib.sha256(json.dumps(asdict(event), sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                intent = (self.journal.load_intent_by_client_order_id(event.client_order_id)
                          if event.client_order_id is not None else None)
                unresolved = intent is None and event.event_type != "COMMAND_OUTCOME"
                if intent is not None and event.order_id is not None and event.client_order_id is not None:
                    self._validate_intent_scope(intent.intent_id, event.client_order_id)
                    prior_fills = self.journal.load_execution_evidence(client_order_id=event.client_order_id)
                    if event.event_type == "OrderFilled":
                        if (event.trade_id is None or event.side is None or event.quantity is None
                                or event.price is None or event.fee is None or event.fee_currency is None
                                or event.trade_time_ns is None or event.instrument_id is None):
                            raise ValueError("Bybit native fill fields unresolved")
                        execution_id = f"BYBIT:{self.identity.content_hash}:{self._product.key.native_symbol}:{event.trade_id}"
                        fill = FillRecord(
                            execution_id, event.order_id, event.client_order_id, intent.intent_id,
                            event.instrument_id, event.side, Decimal(event.quantity), Decimal(event.price),
                            Decimal(event.fee), event.fee_currency, event.trade_time_ns, event.received_at_ns,
                            "BYBIT_NATIVE_ORDER_FILLED", raw_hash,
                        )
                        prior = next((item for item in prior_fills if item.execution_id == execution_id), None)
                        if prior is None:
                            self.journal.append_execution_evidence(fill)
                            prior_fills.append(fill)
                        elif _fill_identity(prior) != _fill_identity(fill):
                            raise ValueError("conflicting Bybit native execution payload")
                    cumulative = sum((fill.qty for fill in prior_fills if fill.order_id == event.order_id), Decimal("0"))
                    if event.cumulative_quantity is not None:
                        cumulative = max(cumulative, Decimal(event.cumulative_quantity))
                    cum_value = sum((fill.qty * fill.price for fill in prior_fills if fill.order_id == event.order_id), Decimal("0"))
                    avg_price = Decimal(event.average_price) if event.average_price is not None else None
                    if avg_price is None and cumulative > 0 and cum_value > 0:
                        avg_price = cum_value / cumulative
                    status = event.status or "UNKNOWN"
                    if event.event_type in {"OrderCanceled", "OrderExpired"} and event.cumulative_quantity is None:
                        status = "UNKNOWN"
                    self.journal.append_order_status_observation(OrderStatusRecord(
                        event.order_id, event.client_order_id, intent.intent_id, status,
                        cumulative, sum((fill.fee for fill in prior_fills if fill.order_id == event.order_id), Decimal("0")),
                        cum_value, avg_price, event.received_at_ns, "BYBIT_NATIVE_ORDER_EVENT", raw_hash,
                    ))
                self.journal.append_observation(Observation(
                    observation_id=uuid.uuid4().hex,
                    source="BYBIT_NATIVE_UNKNOWN_RECEIPT" if unresolved else "BYBIT_NATIVE_ORDER_EVENT",
                    venue_identity=self.identity.content_hash, source_time_ns=event.trade_time_ns,
                    receive_time_ns=event.received_at_ns, raw_hash=raw_hash,
                    completeness="UNRESOLVED_CLIENT_INTENT" if unresolved else "EXACT_TYPED_NATIVE_EVENT",
                ))
                if unresolved:
                    self.last_failure_code = "BYBIT_NATIVE_EVENT_ASSOCIATION_UNRESOLVED"
                    self.stopped = True
                published.append(event)
            except Exception:
                self.stopped = True
                self.last_failure_code = "BYBIT_NATIVE_EVENT_PERSISTENCE_FAILED"
                break
            finally:
                self.journal._transaction_lock.release()
                self.event_queue.task_done()
        return tuple(published)

    def _push_event(self, event: BybitNativeEvent) -> None:
        try:
            self.event_queue.put_nowait(event)
        except queue.Full:
            self._trip_overflow()

    def _trip_overflow(self) -> None:
        self.queue_overflow = True
        self.stopped = True
        self.last_failure_code = "BYBIT_NATIVE_QUEUE_OVERFLOW"
        if self.strategy is not None:
            try:
                self.strategy.degrade()
            except Exception:
                pass
        if hasattr(self, "_node_handle"):
            try:
                self._node_handle.stop()
            except Exception:
                pass

    async def run(self) -> None:
        """Explicitly start the native node; no auto-start occurs at construction."""
        async with self._run_lock:
            if self.stopped or self._run_task is not None:
                raise PersistenceError("Bybit native runtime cannot start")
            try:
                self.assert_writer()
                self._run_task = asyncio.ensure_future(self.node.run_async())
            except Exception:
                self.stopped = True
                self.last_failure_code = "BYBIT_NATIVE_NODE_START_FAILED"
                raise PersistenceError("Bybit native node start failed") from None
        try:
            await asyncio.shield(self._run_task)
        except asyncio.CancelledError:
            self.stopped = True
            try:
                self._node_handle.stop()
            except Exception:
                self.last_failure_code = "BYBIT_NATIVE_NODE_STOP_FAILED"
            raise
        except Exception:
            self.stopped = True
            self.last_failure_code = "BYBIT_NATIVE_NODE_FAILED"
            raise PersistenceError("Bybit native node failed") from None
        finally:
            self.stopped = True

    def dispose_unstarted(self) -> None:
        """Dispose an unused SDK wrapper exactly once, while it is still owned."""
        if self._run_task is not None:
            raise PersistenceError("Bybit native wrapper was consumed by run")
        if not getattr(self, "_disposed_unstarted", False):
            self.node.dispose()
            self._disposed_unstarted = True

    async def stop(self, *, timeout_s: float = 10.0) -> None:
        """Bounded shutdown; a timeout keeps live tasks visible to the lease owner."""
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or not 0 < timeout_s <= 30:
            raise ValueError("invalid bybit native stop timeout")
        deadline = time.monotonic() + timeout_s
        self.stopped = True
        try:
            self._node_handle.stop()
        except Exception:
            self.last_failure_code = "BYBIT_NATIVE_NODE_STOP_FAILED"
        task = self._run_task
        if task is not None:
            done, _pending = await asyncio.wait({task}, timeout=max(0.0, deadline - time.monotonic()))
            if task not in done:
                self.last_failure_code = "BYBIT_NATIVE_NODE_STOP_TIMEOUT"
                raise PersistenceError("Bybit native node still active; writer must remain held")
            if task.cancelled():
                self.last_failure_code = "BYBIT_NATIVE_NODE_STOP_FAILED"
            else:
                try:
                    task.result()
                except Exception:
                    self.last_failure_code = "BYBIT_NATIVE_NODE_STOP_FAILED"
        if task is None:
            try:
                self.dispose_unstarted()
            except Exception:
                self.last_failure_code = "BYBIT_NATIVE_NODE_DISPOSE_FAILED"
        worker = self._reconciliation_worker
        if worker is not None:
            await asyncio.to_thread(worker.join, max(0.0, deadline - time.monotonic()))
            if worker.is_alive():
                self.last_failure_code = "BYBIT_RECONCILIATION_STOP_TIMEOUT"
                raise PersistenceError("Bybit reconciliation still active; writer must remain held")
        # The native task and read worker are quiescent; preserve queued receipts
        # before the held writer/journal can be released by the owning session.
        for _ in range(EVENT_QUEUE_CAPACITY // 64):
            if self.event_queue.empty():
                break
            self.drain_events_to_journal()


def _fill_identity(fill: FillRecord) -> tuple[Any, ...]:
    """Broker execution identity excludes receipt time and transport-specific hash."""
    return (fill.execution_id, fill.order_id, fill.client_order_id, fill.intent_id, fill.instrument,
            fill.side, fill.qty, fill.price, fill.fee, fill.fee_currency, fill.trade_time_ns)


def create_bybit_native_node(**kwargs: Any) -> BybitNativeNode:
    """Construct native objects without starting the venue connection."""
    return BybitNativeNode(**kwargs)
