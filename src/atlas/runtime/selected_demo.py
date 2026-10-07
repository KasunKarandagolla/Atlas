"""Explicit run-bound demo qualification, separate from public research.

Opening and assisted authority remain disabled. Construction performs no I/O;
the held lease, protected credential retrieval, and reads begin in ``open``.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from atlas.domain.execution import Observation
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.binance_demo import (
    BinanceDemoIdentity,
    BinanceDemoReader,
    BinanceMarketFilters,
    opening_gate_reason,
)
from atlas.runtime.binance_reconciliation import (
    BinanceAccountSnapshot,
    capture_account_snapshot,
    reconcile_binance_execution_history,
)
from atlas.runtime.bybit_demo import (
    BybitDemoIdentity,
    BybitDemoReader,
    BybitDemoSnapshot,
    BybitMarketFilters,
    capture_bybit_account_snapshot,
)
from atlas.runtime.bybit_demo import (
    opening_gate_reason as bybit_opening_gate_reason,
)
from atlas.runtime.writer_lock import WriterLock, WriterOwnership
from atlas.v2._serialization import sha256_json
from atlas.v2.data.binance import translate_exchange_info
from atlas.v2.data.bybit import translate_instrument_info
from atlas.v2.instruments import EnvironmentV2, ProductContractV2


class SelectedDemoQualification:
    """One selected venue/account credential writer in the installed session."""

    def __init__(self, *, run: Path, configuration: Any, secret_store: Any,
                 lease_root: Path, clock_ns: Callable[[], int] = time.time_ns,
                 reader_factory: Callable[..., Any] = BinanceDemoReader,
                 node_factory: Callable[..., Any] | None = None) -> None:
        if (configuration.schema_version != 2 or configuration.execution_profile == "DISABLED"
                or configuration.account_scope_ref is None or configuration.credential_ref is None
                or configuration.capability_profile_ref is None):
            raise ValueError("explicit demo qualification run configuration required")
        if run.resolve().parent != Path(configuration.data_root).resolve():
            raise ValueError("demo run is outside its immutable data root")
        self.run = run
        self.configuration = configuration
        self.secret_store = secret_store
        # This common machine/user location is independent of selected venue,
        # account, credential reference and research data root.
        self.lease = WriterLock(lease_root / "selected-demo-writer.lock")
        self.clock_ns = clock_ns
        self.reader_factory = reader_factory
        self.node_factory = node_factory
        self.ownership: WriterOwnership | None = None
        self.journal: SQLiteJournal | None = None
        self.reader: Any = None
        self.identity: BinanceDemoIdentity | BybitDemoIdentity | None = None
        self.product: ProductContractV2 | None = None
        self.market_filters: BinanceMarketFilters | BybitMarketFilters | None = None
        self._credential: Any = None
        self._snapshot: BinanceAccountSnapshot | BybitDemoSnapshot | None = None
        self._snapshot_lock = threading.Lock()
        self._fingerprint: str | None = None
        self.node: Any = None
        self._last_history_at_ns = -1
        self._history_cursor: str | None = None
        self._history_query_offset = 0
        self.closed = False

    def assert_writer(self) -> None:
        ownership = self.ownership
        if self.closed or ownership is None or self.lease.ownership != ownership or self.journal is None:
            raise PersistenceError("selected demo writer is fenced")
        try:
            recorded = self.lease.path.read_text(encoding="utf-8")
        except OSError:
            raise PersistenceError("selected demo writer identity unavailable") from None
        if recorded != f"{ownership.writer_epoch}:{ownership.writer_id}" or not self.journal.is_open:
            raise PersistenceError("selected demo writer epoch is stale")

    def open(self) -> dict[str, Any]:
        if self.ownership is not None or self.closed:
            raise PersistenceError("selected demo qualification cannot be reopened")
        self.ownership = self.lease.acquire()
        try:
            config = self.configuration
            self.journal = SQLiteJournal(self.run / "demo-control.sqlite")
            self._credential = self.secret_store.get_demo_exchange_credentials(
                config.credential_ref, config.selected_execution_venue, config.execution_environment)
            # Opaque run references may use sha256: syntax. Convert only their
            # representation, while config_hash retains the exact owner values.
            scope = hashlib.sha256(config.account_scope_ref.encode()).hexdigest()
            credential_ref = hashlib.sha256(config.credential_ref.encode()).hexdigest()
            identity_type = BinanceDemoIdentity if config.selected_execution_venue == "BINANCE" else BybitDemoIdentity
            self.identity = identity_type(scope, credential_ref, config.execution_environment)
            binding_path = self.run / "demo-account-binding.json"
            if binding_path.exists():
                if binding_path.is_symlink() or binding_path.stat().st_size > 4096:
                    raise PersistenceError("invalid demo account binding")
                binding = json.loads(binding_path.read_text(encoding="utf-8"))
                ref = binding.pop("content_hash", None)
                if (sha256_json(binding) != ref or binding.get("config_hash") != config.content_hash
                        or binding.get("identity_hash") != self.identity.content_hash
                        or not isinstance(binding.get("fingerprint"), str)
                        or len(binding["fingerprint"]) != 64):
                    raise PersistenceError("demo account binding identity mismatch")
                self._fingerprint = binding["fingerprint"]
            reader_factory = self.reader_factory
            if config.selected_execution_venue == "BYBIT" and reader_factory is BinanceDemoReader:
                reader_factory = BybitDemoReader
            self.reader = reader_factory(identity=self.identity, credential=self._credential,
                                              clock_ns=self.clock_ns)
            self.refresh_account()
            if config.execution_profile == "DEMO_NATIVE_OMS_QUALIFICATION":
                bybit = config.selected_execution_venue == "BYBIT"
                receipt = self.reader.read_with_receipt("/v5/market/instruments-info",
                    {"category": "linear", "symbol": config.execution_instrument_symbol}) if bybit else \
                    self.reader.read_with_receipt("/fapi/v1/exchangeInfo")
                if receipt.identity_hash != self.identity.content_hash:
                    raise PersistenceError("demo metadata identity mismatch")
                translator = translate_instrument_info if bybit else translate_exchange_info
                products = translator(receipt.payload,
                    environment=EnvironmentV2(config.execution_environment),
                    observed_at_ns=receipt.received_at_ns, available_at_ns=receipt.received_at_ns)
                matched = [item for item in products if item.key.native_symbol == config.execution_instrument_symbol]
                if len(matched) != 1:
                    raise PersistenceError("demo native instrument not uniquely observed")
                self.product = matched[0]
                metadata_rows = receipt.payload["result"]["list"] if bybit else receipt.payload["symbols"]
                rows = [row for row in metadata_rows if row.get("symbol") == config.execution_instrument_symbol]
                if len(rows) != 1:
                    raise PersistenceError("demo native market filters not uniquely observed")
                filters_type = BybitMarketFilters if bybit else BinanceMarketFilters
                self.market_filters = filters_type.from_metadata(rows[0], product=self.product,
                    received_at_ns=receipt.received_at_ns)
                from atlas.runtime.binance_native import BinanceNativeNode
                from atlas.runtime.bybit_native import BybitNativeNode

                factory: Any = self.node_factory or (BybitNativeNode if bybit else BinanceNativeNode)
                self.node = factory(identity=self.identity, credential=self._credential,
                    product=self.product, journal=self.journal,
                    writer_epoch=self.ownership.writer_epoch, assert_writer=self.assert_writer,
                    reconcile_once=self.reconcile_once, account_snapshot_getter=self.account_snapshot,
                    market_filters=self.market_filters)
            self.assert_writer()
            return self.status()
        except Exception:
            self.close()
            raise

    def refresh_account(self) -> None:
        self.assert_writer()
        snapshot = capture_bybit_account_snapshot(self.reader, expected_account_fingerprint=self._fingerprint,
            native_symbol=self.configuration.execution_instrument_symbol or "BTCUSDT") if \
            self.configuration.selected_execution_venue == "BYBIT" else \
            capture_account_snapshot(self.reader, expected_account_fingerprint=self._fingerprint)
        self.assert_writer()
        if snapshot.account_fingerprint is not None and self._fingerprint is None:
            self._fingerprint = snapshot.account_fingerprint
            binding = {"version": "SELECTED_DEMO_ACCOUNT_BINDING_V1",
                       "config_hash": self.configuration.content_hash, "identity_hash": snapshot.identity_hash,
                       "fingerprint": snapshot.account_fingerprint, "received_at_ns": snapshot.captured_at_ns}
            binding["content_hash"] = sha256_json(binding)
            with (self.run / "demo-account-binding.json").open("x", encoding="utf-8") as handle:
                json.dump(binding, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
        body = {"version": "SELECTED_DEMO_ACCOUNT_RECEIPT_V1", "config_hash": self.configuration.content_hash,
                "identity_hash": snapshot.identity_hash, "received_at_ns": snapshot.captured_at_ns,
                "fingerprint": snapshot.account_fingerprint, "fingerprint_status": snapshot.account_fingerprint_status,
                "eligible": snapshot.eligible, "reasons": list(snapshot.reasons),
                "source_hashes": sorted({row.raw_payload_hash for row in snapshot.receipts}) if \
                    isinstance(snapshot, BybitDemoSnapshot) else sorted({row.source_hash for row in (
                    snapshot.account, snapshot.dual_side, snapshot.multi_assets,
                    *snapshot.positions, *snapshot.symbol_configs, *snapshot.balances)}),
                "capital_enabled": False, "assisted_enabled": False}
        ref = sha256_json(body)
        assert self.journal is not None
        self.journal.append_observation(Observation(uuid.uuid4().hex, "SELECTED_DEMO_ACCOUNT_RECEIPT_V1",
            snapshot.identity_hash, None, snapshot.captured_at_ns, ref, None, None,
            "COMPLETE_PROFILE" if snapshot.eligible else "UNQUALIFIED_PROFILE"))
        with self._snapshot_lock:
            self._snapshot = snapshot

    def account_snapshot(self) -> BinanceAccountSnapshot | BybitDemoSnapshot:
        self.assert_writer()
        with self._snapshot_lock:
            if self._snapshot is None:
                raise PersistenceError("demo account snapshot unavailable")
            return self._snapshot

    def reconcile_once(self) -> None:
        self.refresh_account()
        now_ns = self.clock_ns()
        if self.configuration.selected_execution_venue == "BINANCE" and (
                self._last_history_at_ns < 0 or now_ns - self._last_history_at_ns >= 10_000_000_000):
            self.assert_writer()
            assert self.journal is not None
            result = reconcile_binance_execution_history(self.reader, self.journal, max_intents=1,
                max_queries=1, query_offset=self._history_query_offset,
                after_command_id=self._history_cursor, assert_writer=self.assert_writer)
            self._history_cursor = result.next_command_id
            self._history_query_offset = result.next_query_offset
            self._last_history_at_ns = now_ns

    def status(self) -> dict[str, Any]:
        snapshot = self.account_snapshot()
        return {"version": "SELECTED_DEMO_QUALIFICATION_V1", "config_hash": self.configuration.content_hash,
                "selected_execution_venue": self.configuration.selected_execution_venue,
                "environment": self.configuration.execution_environment,
                "execution_profile": self.configuration.execution_profile,
                "writer_epoch": self.ownership.writer_epoch if self.ownership else None,
                "account_profile": "TESTED" if snapshot.eligible else "TEST GATE",
                "account_profile_reasons": list(snapshot.reasons),
                "account_identity": getattr(snapshot, "account_identity_status", snapshot.account_fingerprint_status),
                "opening_gate": bybit_opening_gate_reason() if self.configuration.selected_execution_venue == "BYBIT" else opening_gate_reason(),
                "native_oms_constructed": self.node is not None,
                "capital_enabled": False, "assisted_enabled": False}

    def capture_recovery(self, *, history_start_ns: int) -> Any:
        """Explicit quiescent evidence capture; never release a reservation."""
        from atlas.runtime.binance_reconciliation import capture_binance_recovery_cycle

        self.assert_writer()
        if not isinstance(self.identity, BinanceDemoIdentity):
            raise PersistenceError("selected venue does not use Binance recovery evidence")

        def assert_quiescent() -> None:
            self.assert_writer()
            if self.node is not None:
                task = getattr(self.node, "_run_task", None)
                worker = getattr(self.node, "_reconciliation_worker", None)
                if ((task is not None and not task.done()) or (worker is not None and worker.is_alive())
                        or not self.node.command_queue.empty() or not self.node.event_queue.empty()):
                    raise PersistenceError("stop and drain native host before recovery capture")

        assert_quiescent()
        assert self.journal is not None and self.ownership is not None
        return capture_binance_recovery_cycle(self.reader, self.journal,
            symbol=self.configuration.execution_instrument_symbol or "BTCUSDT",
            writer_id=self.ownership.writer_id, writer_epoch=self.ownership.writer_epoch,
            runtime_instance_id=self.ownership.writer_id, position_epoch=self.ownership.writer_epoch,
            history_start_ns=history_start_ns, expected_account_fingerprint=self._fingerprint,
            assert_writer=self.assert_writer, assert_quiescent=assert_quiescent)

    def close(self) -> None:
        # A live native host must stop and join before releasing this lease.
        task = getattr(self.node, "_run_task", None)
        if task is not None and not task.done():
            raise PersistenceError("stop and join native demo host before releasing writer")
        worker = getattr(self.node, "_reconciliation_worker", None)
        if worker is not None and worker.is_alive():
            raise PersistenceError("join native demo reconciliation before releasing writer")
        if self.node is not None and task is None:
            dispose = getattr(self.node, "dispose_unstarted", None)
            if callable(dispose):
                dispose()
            else:
                self.node.node.dispose()
        self.closed = True
        if self.journal is not None:
            self.journal.close()
            self.journal = None
        self._credential = None
        self.reader = None
        self.node = None
        self.lease.release()
        self.ownership = None
