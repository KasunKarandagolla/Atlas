"""Explicit run-bound demo qualification, separate from public research.

Opening and assisted authority remain disabled. Construction performs no I/O;
the held lease, protected credential retrieval, and reads begin in ``open``.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
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
from atlas.runtime.binance_readiness import (
    BinanceCommandReadinessProofV1,
    build_binance_command_readiness,
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
    persist_bybit_product_contract_snapshot,
    reconcile_bybit_commands,
)
from atlas.runtime.bybit_demo import (
    opening_gate_reason as bybit_opening_gate_reason,
)
from atlas.runtime.writer_lock import WriterLock, WriterOwnership
from atlas.v2._serialization import sha256_json
from atlas.v2.data.binance import translate_exchange_info
from atlas.v2.data.bybit import translate_instrument_info
from atlas.v2.instruments import EnvironmentV2, ProductContractV2


def _publish_immutable_json(path: Path, encoded: bytes) -> None:
    """Durably publish one create-once JSON binding without replacing a prior value."""
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if os.name == "nt":
            import ctypes

            library_loader = getattr(ctypes, "WinDLL", None)
            if library_loader is None:
                raise OSError("Windows durable file API is unavailable")
            kernel32 = library_loader("kernel32", use_last_error=True)
            move_file = kernel32.MoveFileExW
            move_file.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
            move_file.restype = ctypes.c_int
            if not move_file(str(temporary), str(path), 0x00000008):  # MOVEFILE_WRITE_THROUGH; no replace
                last_error = getattr(ctypes, "get_last_error", lambda: 0)()
                raise OSError(last_error, "durable immutable file publication failed")
        else:
            os.link(temporary, path)
            temporary.unlink()
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except (OSError, AttributeError):
        raise PersistenceError("Binance product contract binding could not be durably published") from None
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


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
        if run.resolve().parent != Path(configuration.data_root).resolve() / "runs":
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
        self._instrument_metadata_receipt: Any = None
        self.market_filters: BinanceMarketFilters | BybitMarketFilters | None = None
        self._credential: Any = None
        self._snapshot: BinanceAccountSnapshot | BybitDemoSnapshot | None = None
        self._snapshot_lock = threading.Lock()
        self._fingerprint: str | None = None
        self.node: Any = None
        self._last_history_at_ns = -1
        self._history_cursor: str | None = None
        self._history_query_offset = 0
        self._binance_history_reason = "BINANCE_HISTORY_NOT_OBSERVED"
        self._binance_readiness_reason = "BINANCE_COMMAND_READINESS_NOT_OBSERVED"
        self._bybit_history_cursor: str | None = None
        self._bybit_history_cycle_failed = False
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
                self._instrument_metadata_receipt = receipt
                metadata_rows = receipt.payload["result"]["list"] if bybit else receipt.payload["symbols"]
                rows = [row for row in metadata_rows if row.get("symbol") == config.execution_instrument_symbol]
                if len(rows) != 1:
                    raise PersistenceError("demo native market filters not uniquely observed")
                current_product = matched[0]
                self.product = current_product if bybit else self._bind_binance_product_contract(
                    current_product, rows[0], receipt,
                )
                filters_type = BybitMarketFilters if bybit else BinanceMarketFilters
                self.market_filters = filters_type.from_metadata(rows[0], product=self.product,
                    received_at_ns=receipt.received_at_ns)
                if bybit:
                    assert isinstance(self.identity, BybitDemoIdentity) and isinstance(self._snapshot, BybitDemoSnapshot)
                    self._instrument_metadata_receipt = receipt
                    persist_bybit_product_contract_snapshot(self.journal, identity=self.identity,
                        snapshot=self._snapshot, product=self.product, instrument_metadata_receipt=receipt,
                        assert_writer=self.assert_writer)
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

    def _bind_binance_product_contract(self, current: ProductContractV2, metadata_row: Any,
                                       receipt: Any) -> ProductContractV2:
        """Keep the run's product hash stable while requiring an exact current revision."""
        try:
            canonical_receipt = json.dumps(receipt.payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (AttributeError, TypeError, ValueError):
            raise PersistenceError("Binance product metadata receipt is invalid") from None
        if (self.configuration.selected_execution_venue != "BINANCE"
                or self.configuration.execution_profile != "DEMO_NATIVE_OMS_QUALIFICATION"
                or self.configuration.execution_instrument_symbol != current.key.native_symbol
                or current.key.venue.value != "BINANCE"
                or current.key.environment.value != self.configuration.execution_environment
                or current.key.contract_revision != sha256_json(dict(metadata_row))
                or current.metadata_ref != current.key.contract_revision
                or getattr(receipt, "identity_hash", None) != getattr(self.identity, "content_hash", None)
                or getattr(receipt, "endpoint", None) != "/fapi/v1/exchangeInfo"
                or type(getattr(receipt, "received_at_ns", None)) is not int
                or receipt.received_at_ns <= 0
                or receipt.received_at_ns > self.clock_ns()
                or current.effective_at_ns != receipt.received_at_ns
                or current.observed_at_ns != receipt.received_at_ns
                or current.available_at_ns != receipt.received_at_ns
                or getattr(receipt, "canonical_payload", None) != canonical_receipt
                or hashlib.sha256(canonical_receipt.encode()).hexdigest()
                != getattr(receipt, "canonical_payload_hash", None)
                or not isinstance(getattr(receipt, "raw_payload_hash", None), str)
                or len(receipt.raw_payload_hash) != 64
                or any(character not in "0123456789abcdef" for character in receipt.raw_payload_hash)):
            raise PersistenceError("Binance product contract binding scope is invalid")
        journal = self.journal
        identity = self.identity
        if journal is None or not isinstance(identity, BinanceDemoIdentity):
            raise PersistenceError("Binance product contract binding journal is unavailable")
        self.assert_writer()
        metadata_observation_id = uuid.uuid4().hex
        journal.append_observation(Observation(
            metadata_observation_id,
            "BINANCE_DEMO_PRODUCT_METADATA_RECEIPT_V1",
            identity.content_hash,
            None,
            receipt.received_at_ns,
            receipt.raw_payload_hash,
            "/fapi/v1/exchangeInfo",
            None,
            "EXACT_CURRENT_METADATA_RECEIPT",
        ))
        path = self.run / "binance-product-binding.json"
        try:
            info = path.lstat()
        except FileNotFoundError:
            info = None
        except OSError:
            raise PersistenceError("Binance product contract binding is unavailable") from None
        if info is not None:
            if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_size > 65_536:
                raise PersistenceError("Binance product contract binding is invalid")
            try:
                stored = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                raise PersistenceError("Binance product contract binding is unreadable") from None
            fields = {
                "version", "config_hash", "identity_hash", "native_symbol", "environment",
                "metadata_row", "metadata_row_hash", "metadata_receipt_hash", "metadata_received_at_ns",
                "metadata_observation_id",
                "product_contract", "product_contract_hash", "content_hash",
            }
            if not isinstance(stored, dict) or set(stored) != fields:
                raise PersistenceError("Binance product contract binding fields are invalid")
            observation_id = stored.get("metadata_observation_id")
            if (not isinstance(observation_id, str) or len(observation_id) != 32
                    or any(character not in "0123456789abcdef" for character in observation_id)):
                raise PersistenceError("Binance product metadata receipt identity is invalid")
            body = {key: value for key, value in stored.items() if key != "content_hash"}
            original_receipt = journal.get_observation_by_id(observation_id)
            if (stored.get("version") != "BINANCE_DEMO_PRODUCT_BINDING_V2"
                    or stored.get("content_hash") != sha256_json(body)
                    or stored.get("config_hash") != self.configuration.content_hash
                    or stored.get("identity_hash") != identity.content_hash
                    or stored.get("native_symbol") != current.key.native_symbol
                    or stored.get("environment") != self.configuration.execution_environment
                    or not isinstance(stored.get("metadata_row"), dict)
                    or stored.get("metadata_row") != dict(metadata_row)
                    or stored.get("metadata_row_hash") != current.key.contract_revision
                    or sha256_json(stored["metadata_row"]) != current.key.contract_revision
                    or not isinstance(stored.get("metadata_receipt_hash"), str)
                    or len(stored["metadata_receipt_hash"]) != 64
                    or any(character not in "0123456789abcdef" for character in stored["metadata_receipt_hash"])
                    or type(stored.get("metadata_received_at_ns")) is not int
                    or not isinstance(original_receipt, dict)
                    or original_receipt.get("source") != "BINANCE_DEMO_PRODUCT_METADATA_RECEIPT_V1"
                    or original_receipt.get("venue_identity") != identity.content_hash
                    or original_receipt.get("receive_time_ns") != stored.get("metadata_received_at_ns")
                    or original_receipt.get("raw_hash") != stored.get("metadata_receipt_hash")
                    or original_receipt.get("request_id") != "/fapi/v1/exchangeInfo"
                    or original_receipt.get("completeness") != "EXACT_CURRENT_METADATA_RECEIPT"):
                raise PersistenceError("Binance product contract binding does not match current metadata")
            try:
                product = ProductContractV2.from_dict(stored["product_contract"])
                normalized_current = replace(
                    current,
                    effective_at_ns=product.effective_at_ns,
                    observed_at_ns=product.observed_at_ns,
                    available_at_ns=product.available_at_ns,
                )
            except (TypeError, ValueError):
                raise PersistenceError("Binance product contract binding product is invalid") from None
            if (product.content_hash != stored.get("product_contract_hash")
                    or product.key.content_hash != current.key.content_hash
                    or normalized_current.content_hash != product.content_hash
                    or product.effective_at_ns != stored["metadata_received_at_ns"]
                    or product.observed_at_ns != stored["metadata_received_at_ns"]
                    or product.available_at_ns != stored["metadata_received_at_ns"]):
                raise PersistenceError("Binance product contract binding changed")
            return product

        body = {
            "version": "BINANCE_DEMO_PRODUCT_BINDING_V2",
            "config_hash": self.configuration.content_hash,
            "identity_hash": identity.content_hash,
            "native_symbol": current.key.native_symbol,
            "environment": self.configuration.execution_environment,
            "metadata_row": dict(metadata_row),
            "metadata_row_hash": current.key.contract_revision,
            "metadata_receipt_hash": receipt.raw_payload_hash,
            "metadata_received_at_ns": receipt.received_at_ns,
            "metadata_observation_id": metadata_observation_id,
            "product_contract": current.to_dict(),
            "product_contract_hash": current.content_hash,
        }
        body["content_hash"] = sha256_json(body)
        encoded = (json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        if len(encoded) > 65_536:
            raise PersistenceError("Binance product contract binding exceeds its storage bound")
        _publish_immutable_json(path, encoded)
        return current

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

    def create_binance_risk_reduction_shell(self) -> Any:
        """Bind durable, reduction-only controls to this run's selected Binance writer."""
        self.assert_writer()
        if (self.configuration.selected_execution_venue != "BINANCE"
                or self.configuration.execution_profile != "DEMO_NATIVE_OMS_QUALIFICATION"
                or self.node is None or getattr(self.node, "risk_reduction_profile", None)
                != "BINANCE_DEMO_REDUCE_ONLY_V1"):
            raise PersistenceError("selected run has no qualified Binance reduction command port")
        from atlas.runtime.assisted_control import AssistedControlShell

        assert self.journal is not None and self.ownership is not None
        return AssistedControlShell(
            journal=self.journal,
            runtime_instance_id=self.ownership.writer_id,
            writer_id=self.ownership.writer_id,
            writer_epoch=self.ownership.writer_epoch,
            assisted_enabled=False,
            nautilus_port=self.node,
            live_writer_lock=self.lease,
        )

    def reconcile_once(self) -> bool | BinanceCommandReadinessProofV1 | None:
        self.refresh_account()
        now_ns = self.clock_ns()
        if self.configuration.selected_execution_venue == "BYBIT":
            if now_ns - self._last_history_at_ns < 10_000_000_000:
                return False
            self._last_history_at_ns = now_ns
            assert (self.journal is not None and self.product is not None
                    and self._instrument_metadata_receipt is not None
                    and isinstance(self.identity, BybitDemoIdentity))
            snapshot = self.account_snapshot()
            if not isinstance(snapshot, BybitDemoSnapshot):
                raise PersistenceError("selected Bybit account snapshot has a different venue")
            bybit_result = reconcile_bybit_commands(self.reader, self.journal,
                native_symbol=self.product.key.native_symbol,
                snapshot=snapshot, product=self.product,
                instrument_metadata_receipt=self._instrument_metadata_receipt,
                assert_writer=self.assert_writer, max_commands=1,
                after_command_id=self._bybit_history_cursor)
            self._bybit_history_cycle_failed = self._bybit_history_cycle_failed or bool(bybit_result.reasons)
            if bybit_result.has_more_commands:
                self._bybit_history_cursor = bybit_result.next_command_id
                return False
            self._bybit_history_cursor = None
            ready = bybit_result.ready and not self._bybit_history_cycle_failed
            self._bybit_history_cycle_failed = False
            return ready
        assert self.configuration.selected_execution_venue == "BINANCE"
        self.assert_writer()
        assert self.journal is not None
        if self._last_history_at_ns < 0 or now_ns - self._last_history_at_ns >= 10_000_000_000:
            # Historical diagnostics never confer command or recovery authority.
            # Exact UNSENT reductions obtain their own current, durable proof.
            try:
                binance_result = reconcile_binance_execution_history(self.reader, self.journal, max_intents=1,
                    max_queries=1, query_offset=self._history_query_offset,
                    after_command_id=self._history_cursor, assert_writer=self.assert_writer)
                self._history_cursor = binance_result.next_command_id
                self._history_query_offset = binance_result.next_query_offset
                self._binance_history_reason = (
                    "BINANCE_HISTORY_DIAGNOSTIC_INCOMPLETE" if binance_result.reasons
                    else "BINANCE_HISTORY_DIAGNOSTIC_OBSERVED")
            except (PersistenceError, ValueError, ArithmeticError):
                self._binance_history_reason = "BINANCE_HISTORY_DIAGNOSTIC_UNAVAILABLE"
            self._last_history_at_ns = now_ns
        snapshot = self.account_snapshot()
        if (not isinstance(self.identity, BinanceDemoIdentity)
                or not isinstance(snapshot, BinanceAccountSnapshot)
                or self.product is None or self.node is None or self.ownership is None):
            self._binance_readiness_reason = "BINANCE_NATIVE_COMMAND_CONTEXT_UNAVAILABLE"
            return None
        try:
            proof = build_binance_command_readiness(self.reader, self.journal,
                identity=self.identity, product=self.product, snapshot=snapshot,
                writer_epoch=self.ownership.writer_epoch, native_generation=self.node.state_generation,
                assert_writer=self.assert_writer)
        except (PersistenceError, ValueError, ArithmeticError):
            self._binance_readiness_reason = "BINANCE_COMMAND_READINESS_UNAVAILABLE"
            return None
        self._binance_readiness_reason = (
            "BINANCE_EXACT_REDUCTION_READY" if proof is not None else "BINANCE_NO_QUALIFIED_PENDING_COMMAND")
        return proof

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
                "command_readiness_reason": (getattr(self.node, "readiness_reason", None)
                    or self._binance_readiness_reason) if self.configuration.selected_execution_venue == "BINANCE"
                    else "BYBIT_COMMAND_RECONCILIATION_PROFILE",
                "history_diagnostic_reason": self._binance_history_reason
                    if self.configuration.selected_execution_venue == "BINANCE" else "BYBIT_HISTORY_PROFILE",
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
