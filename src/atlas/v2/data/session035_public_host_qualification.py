"""Bounded, credential-free Session-035 Linux and Bybit public qualification.

Without ``--real-public-smoke`` this module performs local host checks only.
The opt-in path composes the accepted public REST/WS adapters with the existing
single-writer supervisor and never grants S3 trade completeness.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform as platform_module
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Linux hosts report an environment gate
    fcntl = None  # type: ignore[assignment]

try:
    import resource
except ImportError:  # pragma: no cover - non-Linux hosts report an environment gate
    resource = None  # type: ignore[assignment]

from .._serialization import canonical_json, sha256_json, timestamp
from ..instruments import VenueV2
from .bybit_source import BybitPublicCycleSourceV1, BybitPublicSnapshotV1
from .capabilities import default_evidence_capability_matrix_v2
from .public_microstructure_ws import (
    DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
    DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
    DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
    PUBLIC_WS_RECEIVE_QUEUE_ITEMS,
    bybit_btc_eth_linear_topics,
)
from .public_stream_source import PublicStreamSourceV2

MAX_REAL_SMOKE_SECONDS = 180
DEFAULT_REAL_SMOKE_SECONDS = 30
MIN_REAL_SMOKE_SECONDS = 20
SMOKE_SHUTDOWN_RESERVE_NS = 8_000_000_000
SMOKE_CYCLE_INTERVAL_SECONDS = 0.25
SUBSCRIPTION_ACK_DEADLINE_SECONDS = 15
MAX_DIAGNOSTIC_TRADE_IDENTITIES = 50_000
_ROOT = Path(__file__).resolve().parents[4]
_DOCS = (
    "https://bybit-exchange.github.io/docs/v5/websocket/public/trade",
    "https://bybit-exchange.github.io/docs/v5/market/recent-trade",
    "https://www.bybit.com/en/derivative-activity/history-data",
    "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook",
    "https://bybit-exchange.github.io/docs/v5/ws/connect",
)
_REASON_CODES = (
    "BYBIT_PUBLIC_TRADE_NO_REPLAY_CURSOR",
    "BYBIT_RECENT_TRADE_BOUNDED_SUFFIX_ONLY",
    "BYBIT_WS_DISCONNECT_INTERVAL_NO_REPAIR",
    "BYBIT_HISTORICAL_ARCHIVE_CONTRACT_UNSPECIFIED",
    "BYBIT_SOURCE_REVISION_SEMANTICS_UNSPECIFIED",
    "S3_TRADE_POPULATION_COVERAGE_UNPROVEN",
)


class WriterLockBusy(RuntimeError):
    """The qualification database already has a controller writer."""


@contextmanager
def controller_writer_lock(lock_path: Path) -> Iterator[None]:
    """Acquire a process-wide, nonblocking lock before opening the ops database."""
    if sys.platform != "linux" or fcntl is None:
        raise OSError("controller writer lock requires Linux flock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise WriterLockBusy("qualification controller lock is already held") from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def classify_host(os_name: str, kernel_release: str) -> tuple[str, str]:
    """Return a sanitized host class and the project status for qualification."""
    if os_name != "linux":
        return "UNSUPPORTED", "BLOCKED BY ENVIRONMENT"
    folded = kernel_release.lower()
    if "microsoft" in folded or "wsl" in folded:
        return "WSL", "TESTED"
    return "LINUX", "TESTED"


def classify_filesystem_path(path: str | Path, *, host_class: str) -> str:
    """Classify a data path without returning mount points or machine paths."""
    value = os.path.abspath(os.fspath(path)).replace("\\", "/")
    path_parts = value.split("/")
    if (len(path_parts) > 2 and path_parts[1].lower() == "mnt" and len(path_parts[2]) == 1
            and path_parts[2].isalpha()):
        return "WINDOWS_MOUNT"
    if os.name == "nt":
        return "WINDOWS_FILESYSTEM"
    mount_type: str | None = None
    try:
        resolved = os.path.realpath(value)
        best_length = -1
        with open("/proc/mounts", encoding="utf-8") as mounts:
            for line in mounts:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mount_point = fields[1].replace("\\040", " ").replace("\\011", "\t")
                if (resolved == mount_point or resolved.startswith(mount_point.rstrip("/") + "/")):
                    if len(mount_point) > best_length:
                        mount_type = fields[2].lower()
                        best_length = len(mount_point)
    except OSError:
        mount_type = None
    if mount_type in {"9p", "drvfs", "cifs", "smb3", "fuseblk"}:
        return "WINDOWS_MOUNT"
    return "LINUX_GUEST_FILESYSTEM" if host_class == "WSL" else "LINUX_FILESYSTEM"


def _read_process_rss_bytes() -> int | None:
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        return int(fields[1]) * int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, IndexError):
        try:
            if resource is None:
                return None
            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return int(value if sys.platform == "darwin" else value * 1024)
        except (AttributeError, OSError, ValueError):
            return None


def _sha256_file(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(65_536), b""):
                digest.update(block)
        return digest.hexdigest()
    except OSError:
        return None


def _git_identity() -> tuple[str, bool]:
    safe_env = {
        "PATH": "/usr/bin:/bin",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
    }
    git_path = next((path for path in ("/usr/bin/git", "/bin/git") if Path(path).is_file()), None)
    if git_path is None:
        return "UNKNOWN", True
    try:
        revision = subprocess.run(
            [git_path, "-C", str(_ROOT), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True, timeout=2, env=safe_env,
        ).stdout.strip()
        dirty = subprocess.run(
            [git_path, "-C", str(_ROOT), "diff", "--quiet", "HEAD", "--"],
            capture_output=True, timeout=2, env=safe_env,
        ).returncode != 0
        if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
            return "UNKNOWN", True
        return revision, dirty
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN", True


def _configuration_identity() -> str:
    matrix = default_evidence_capability_matrix_v2()
    return sha256_json({
        "qualification_version": "SESSION035_PUBLIC_HOST_QUALIFICATION_V1",
        "topics": list(bybit_btc_eth_linear_topics()),
        "queue_items": DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
        "queue_bytes": DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
        "receive_queue_items": PUBLIC_WS_RECEIVE_QUEUE_ITEMS,
        "drain_items": DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
        "controller_frames_per_cycle": 32,
        "capability_matrix_ref": matrix.content_hash,
        "trade_completeness_proven": False,
    })


def _safe_file_sizes(root: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for name in ("ops.sqlite", "ops.sqlite-wal", "ops.sqlite-shm"):
        try:
            values[name] = (root / name).stat().st_size
        except OSError:
            values[name] = 0
    return values


def inspect_host(root: Path, *, lock_already_held: bool = False) -> dict[str, Any]:
    """Probe the disposable ops path, SQLite mode, lock and sanitized host facts."""
    os_class, host_status = classify_host(sys.platform, platform_module.release())
    path_class = classify_filesystem_path(root, host_class=os_class)
    start_utc = time.time_ns()
    start_mono = time.monotonic_ns()
    rss_start = _read_process_rss_bytes()
    try:
        disk = shutil.disk_usage(root if root.exists() else root.parent)
        free_bytes = disk.free
        writable = os.access(root if root.exists() else root.parent, os.R_OK | os.W_OK)
    except OSError:
        free_bytes = 0
        writable = False
    lock_ok = lock_already_held
    sqlite_result: dict[str, Any] = {
        "runtime_version": sqlite3.sqlite_version,
        "journal_mode": "NOT TESTED",
        "synchronous": "NOT TESTED",
    }
    db_status = "TEST GATE"
    if host_status == "TESTED" and path_class != "WINDOWS_MOUNT" and writable:
        try:
            root.mkdir(parents=True, exist_ok=True)
            if not lock_already_held:
                with controller_writer_lock(root / "controller.lock"):
                    lock_ok = True
                    sqlite_result, db_status = _probe_ops_database(root)
            else:
                sqlite_result, db_status = _probe_ops_database(root)
        except WriterLockBusy:
            lock_ok = False
            db_status = "TEST GATE"
        except (OSError, RuntimeError, sqlite3.Error, ValueError):
            db_status = "TEST GATE"
    elif host_status != "TESTED":
        host_status = "BLOCKED BY ENVIRONMENT"
    elif path_class == "WINDOWS_MOUNT":
        db_status = "TEST GATE"
    rss_end = _read_process_rss_bytes()
    finish_utc = time.time_ns()
    elapsed_mono = max(0, time.monotonic_ns() - start_mono)
    root_sha, tracked_dirty = _git_identity()
    core_lock = _sha256_file(_ROOT / "requirements-lock.txt")
    agent_lock = _sha256_file(_ROOT / "requirements-agent-lock.txt")
    host_ok = (
        host_status == "TESTED" and path_class != "WINDOWS_MOUNT" and writable and lock_ok
        and db_status == "TESTED"
    )
    return {
        "schema_version": 1,
        "host_status": "TESTED" if host_ok else host_status if host_status == "BLOCKED BY ENVIRONMENT" else "TEST GATE",
        "host_class": os_class,
        "python_version": platform_module.python_version(),
        "dependency_lock_hashes": {
            "requirements-lock.txt": core_lock,
            "requirements-agent-lock.txt": agent_lock,
        },
        "sqlite": sqlite_result,
        "paths": {
            "ops_database": "ops.sqlite",
            "archive": "ops-observations",
            "websocket_raw_archive": "ops-l2-frames",
            "filesystem_class": path_class,
            "path_absolute_values_emitted": False,
        },
        "filesystem_check": {
            "writable": writable,
            "free_bytes": free_bytes,
            "configuration_changed": False,
        },
        "clock": {
            "utc_start_ns": start_utc,
            "utc_end_ns": finish_utc,
            "monotonic_elapsed_ns": elapsed_mono,
            "wall_clock_regressed": finish_utc < start_utc,
        },
        "single_writer_lock": {"acquired": lock_ok, "status": "TESTED" if lock_ok else "TEST GATE"},
        "resource_observation": {"process_rss_start_bytes": rss_start, "process_rss_end_bytes": rss_end},
        "runtime_identity": {
            "git_sha": root_sha,
            "tracked_working_tree_dirty": tracked_dirty,
            "configuration_sha256": _configuration_identity(),
        },
    }


def _probe_ops_database(root: Path) -> tuple[dict[str, Any], str]:
    from ..memory.repository import OpsRepository

    database = root / "ops.sqlite"
    if database.exists() and database.stat().st_size > 0:
        # An existing nonempty database is not accepted as a disposable root.
        return {
            "runtime_version": sqlite3.sqlite_version,
            "journal_mode": "EXISTING DATABASE",
            "synchronous": "NOT TESTED",
        }, "TEST GATE"
    with OpsRepository(database) as repository:
        journal_mode = str(repository._connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        synchronous_value = int(repository._connection.execute("PRAGMA synchronous").fetchone()[0])
    synchronous = "FULL" if synchronous_value == 2 else f"VALUE_{synchronous_value}"
    result = {
        "runtime_version": sqlite3.sqlite_version,
        "journal_mode": journal_mode.upper(),
        "synchronous": synchronous,
    }
    return result, "TESTED" if journal_mode == "wal" and synchronous_value == 2 else "TEST GATE"


def compare_trade_overlap(
    websocket_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    rest_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    """Compare one bounded REST suffix with observed WS IDs; never infer coverage."""
    result: dict[str, Any] = {}
    for symbol in ("BTCUSDT", "ETHUSDT"):
        websocket = websocket_rows.get(symbol, {})
        rest = rest_rows.get(symbol, {})
        ws_times_raw = [row.get("event_time_ns") for row in websocket.values()]
        rest_times_raw = [row.get("event_time_ns") for row in rest.values()]
        if (not ws_times_raw or not rest_times_raw
                or any(type(value) is not int for value in (*ws_times_raw, *rest_times_raw))):
            result[symbol] = {
                "statuses": ["NOT ESTIMABLE"],
                "ws_trade_ids_observed": len(websocket),
                "rest_trade_ids_observed": len(rest),
                "overlapping_ids": 0,
                "missing_from_ws": 0,
                "missing_from_rest": 0,
                "conflicting_content": 0,
                "shared_event_time_window": None,
                "completeness_proven": False,
            }
            continue
        ws_times = cast(list[int], ws_times_raw)
        rest_times = cast(list[int], rest_times_raw)
        overlap_start_ns = max(min(ws_times), min(rest_times))
        overlap_end_ns = min(max(ws_times), max(rest_times))
        if overlap_start_ns > overlap_end_ns:
            result[symbol] = {
                "statuses": ["NOT ESTIMABLE"],
                "ws_trade_ids_observed": len(websocket),
                "rest_trade_ids_observed": len(rest),
                "overlapping_ids": 0,
                "missing_from_ws": 0,
                "missing_from_rest": 0,
                "conflicting_content": 0,
                "shared_event_time_window": None,
                "completeness_proven": False,
            }
            continue
        ws_window = {
            identity: row for identity, row in websocket.items()
            if overlap_start_ns <= row["event_time_ns"] <= overlap_end_ns
        }
        rest_window = {
            identity: row for identity, row in rest.items()
            if overlap_start_ns <= row["event_time_ns"] <= overlap_end_ns
        }
        common = set(ws_window).intersection(rest_window)
        conflicting = sum(_trade_overlap_conflicts(ws_window[item], rest_window[item]) for item in common)
        missing_from_ws = len(set(rest_window) - set(ws_window))
        missing_from_rest = len(set(ws_window) - set(rest_window))
        statuses: list[str] = []
        if conflicting:
            statuses.append("CONFLICTING_CONTENT")
        if missing_from_ws:
            statuses.append("MISSING_FROM_WS")
        if missing_from_rest:
            statuses.append("MISSING_FROM_REST")
        if not statuses and common:
            statuses.append("MATCHED")
        if not statuses:
            statuses.append("NOT ESTIMABLE")
        result[symbol] = {
            "statuses": statuses,
            "ws_trade_ids_observed": len(websocket),
            "rest_trade_ids_observed": len(rest),
            "overlapping_ids": len(common),
            "missing_from_ws": missing_from_ws,
            "missing_from_rest": missing_from_rest,
            "conflicting_content": conflicting,
            "shared_event_time_window": [overlap_start_ns, overlap_end_ns],
            "ws_ids_in_shared_event_time_window": len(ws_window),
            "rest_ids_in_shared_event_time_window": len(rest_window),
            "completeness_proven": False,
        }
    return result


def _trade_overlap_conflicts(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    for field in ("price", "size", "event_time_ns", "side"):
        if left.get(field) != right.get(field):
            return True
    return left.get("seq") is not None and right.get("seq") is not None and left.get("seq") != right.get("seq")


def adjudicate_observed_interval(
    *,
    subscription_acknowledged: bool,
    frame_count: int,
    trade_count: int,
    book_sequence_valid: bool,
    disconnect_count: int = 0,
    queue_overflow: bool = False,
    transport_error: bool = False,
    conflicting_trade_ids: int = 0,
) -> dict[str, Any]:
    """Score only the observed interval; transport facts never prove completeness."""
    failed_closed = disconnect_count > 0 or queue_overflow or transport_error or conflicting_trade_ids > 0
    websocket_status = (
        "TESTED" if subscription_acknowledged and frame_count > 0 and not failed_closed else "TEST GATE"
    )
    return {
        "public_websocket_transport": websocket_status,
        "book_continuity": "TESTED" if book_sequence_valid and not failed_closed else "TEST GATE",
        "observed_trade_evidence": (
            "TESTED" if trade_count > 0 and not failed_closed else "TEST GATE"
        ),
        "trade_completeness": "NOT ESTIMABLE",
        "trade_completeness_proven": False,
        "s3_strategy_input_readiness": "NOT ESTIMABLE",
    }


def build_bybit_trade_completeness_assessment(
    instrument_keys: Sequence[Any], *, assessed_at_ns: int,
) -> dict[str, Any]:
    """Record the current Bybit source contract gaps for exact instrument keys."""
    from ..instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2

    timestamp(assessed_at_ns, field="assessed_at_ns")
    expected = {"BTCUSDT", "ETHUSDT"}
    keys = tuple(instrument_keys)
    if any(not isinstance(key, InstrumentKeyV2) for key in keys):
        raise ValueError("assessment requires exact InstrumentKeyV2 identities")
    if {key.native_symbol for key in keys} != expected or len(keys) != 2:
        raise ValueError("assessment requires exactly BTCUSDT and ETHUSDT")
    if any(
        key.venue != VenueV2.BYBIT or key.environment != EnvironmentV2.MAINNET
        or key.product != ProductTypeV2.LINEAR_PERPETUAL for key in keys
    ):
        raise ValueError("assessment keys must be Bybit MAINNET linear perpetuals")
    matrix = default_evidence_capability_matrix_v2()
    return {
        "schema_version": 1,
        "artifact_type": "BybitPublicTradeCompletenessAssessmentV1",
        "source_id": "BYBIT_PUBLIC_WS",
        "channel": "publicTrade.{native_symbol}",
        "channels": [
            {"instrument_key": key.to_dict(), "channel": f"publicTrade.{key.native_symbol}"}
            for key in sorted(keys, key=lambda item: item.native_symbol)
        ],
        "assessed_at_ns": assessed_at_ns,
        "instrument_keys": [key.to_dict() for key in sorted(keys, key=lambda item: item.native_symbol)],
        "provider_documentation": {"retrieved_on": "2026-10-01", "refs": list(_DOCS)},
        "capability_matrix_ref": matrix.content_hash,
        "websocket_semantics": {
            "trade_identity_field": "i",
            "cross_sequence_field": "seq",
            "maximum_trades_per_futures_message": 1024,
            "multiple_messages_may_share_seq": True,
            "seq_is_a_complete_population_cursor": False,
            "missed_message_detection_guaranteed": False,
        },
        "rest_repair_semantics": {
            "endpoint": "GET /v5/market/recent-trade",
            "window": "recent bounded suffix",
            "linear_maximum_page_size": 1000,
            "query_cursor": False,
            "start_time_or_end_time_replay": False,
            "exact_historical_replay_contract": False,
        },
        "historical_archive_review": {
            "official_recent_trade_docs_link_to_downloadable_archive": True,
            "archive_page_ref": "https://www.bybit.com/en/derivative-activity/history-data",
            "page_observation": "dynamic data-product catalog; no trade-set completeness, cursor, revision, or receipt-time contract surfaced",
            "qualifies_as_exact_repair_source": False,
        },
        "recovery_semantics": {
            "disconnect_interval_repair": False,
            "restart_reconciliation_cursor": False,
            "historical_repair_preserves_original_event_and_later_receipt_time": False,
            "deterministic_source_revision_correction_id": False,
            "s3_minute_trade_population_support_provable": False,
        },
        "exact_completeness_contract_answers": {
            "A_complete_population_sequence_or_cursor": {"supported": False, "answer": None},
            "B_every_trade_for_exact_symbol_guaranteed": False,
            "C_multiple_messages_sharing_seq_handled": True,
            "D_missed_message_detection_guaranteed": False,
            "E_missed_interval_repair_supported": False,
            "F_exact_historical_trade_set_replay_supported": False,
            "G_original_event_and_later_receipt_time_preserved": False,
            "H_reconnect_and_restart_reconciled": False,
            "I_source_revision_or_correction_deterministic": False,
            "J_each_s3_minute_vwap_interval_fully_supported": False,
        },
        "smoke_evidence_refs": [],
        "trade_completeness_status": "NOT ESTIMABLE",
        "gate_status": "TEST GATE",
        "completeness_proven": False,
        "s3_warmup_contract": {
            "required_contiguous_m1_observations": 10_081,
            "strictly_preceding_residual_observations": 120,
            "trade_derived_vwap_required": True,
            "fresh_bbo_max_age_ns": 1_000_000_000,
            "readiness": "NOT ESTIMABLE",
        },
        "capital_enabled": False,
        "assisted_enabled": False,
        "reason_codes": list(_REASON_CODES),
        "authority": "ZERO",
    }


def _normalized_trade(row: Mapping[str, Any]) -> tuple[str, dict[str, Any]] | None:
    identity = row.get("i", row.get("execId"))
    price = row.get("p", row.get("price"))
    size = row.get("v", row.get("size"))
    event_ms = row.get("T", row.get("time"))
    side = row.get("S", row.get("side"))
    if identity is None or price is None or size is None or event_ms is None:
        return None
    try:
        if isinstance(event_ms, bool):
            return None
        event_ns = int(event_ms) * 1_000_000
        normalized = {
            "price": str(Decimal(str(price)).normalize()),
            "size": str(Decimal(str(size)).normalize()),
            "event_time_ns": event_ns,
            "side": str(side) if side is not None else None,
            "seq": str(row["seq"]) if row.get("seq") is not None else None,
        }
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None
    return str(identity), normalized


class _SnapshotCaptureSource:
    """Capture the accepted adapter's single bounded REST snapshot for overlap checks."""

    def __init__(self, source: BybitPublicCycleSourceV1) -> None:
        self.source = source
        self.snapshot: BybitPublicSnapshotV1 | None = None
        self.recent_trade_requests = {"BTCUSDT": 0, "ETHUSDT": 0}
        setattr(source, "reader", _SingleRecentTradeReader(source.reader, self.recent_trade_requests))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.source, name)

    def acquire_snapshot(self, *, now_ns: int) -> BybitPublicSnapshotV1:
        self.snapshot = self.source.acquire_snapshot(now_ns=now_ns)
        return self.snapshot


class _SingleRecentTradeReader:
    """Count and cap REST trade-overlap calls while delegating the accepted reader."""

    def __init__(self, reader: Any, counts: dict[str, int]) -> None:
        self._reader = reader
        self._counts = counts

    def __getattr__(self, name: str) -> Any:
        return getattr(self._reader, name)

    def recent_trades(self, symbol: str, *, limit: int = 100) -> Any:
        if symbol not in self._counts or self._counts[symbol] >= 1:
            raise RuntimeError("recent public trade diagnostic request exceeded its one-per-symbol bound")
        self._counts[symbol] += 1
        return self._reader.recent_trades(symbol, limit=limit)


def _rest_trade_rows(snapshot: BybitPublicSnapshotV1 | None) -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = {"BTCUSDT": {}, "ETHUSDT": {}}
    if snapshot is None:
        return result
    for record in snapshot.records:
        if record.observation.event_type != "TRADE":
            continue
        try:
            row = json.loads(record.raw_payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, Mapping) and record.instrument_key.native_symbol in result:
            parsed = _normalized_trade(row)
            if parsed is not None:
                result[record.instrument_key.native_symbol][parsed[0]] = parsed[1]
    return result


def _archive_trade_rows(root: Path) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, Any]]:
    """Read archived chunks one at a time and keep only a bounded ID summary."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return {"BTCUSDT": {}, "ETHUSDT": {}}, {
            "malformed_frames": 0, "archive_chunks_read": 0, "duplicate_ids": 0,
            "conflicting_ids": 0, "same_seq_multiple_messages": 0, "diagnostic_scan_truncated": False,
            "trade_rows_observed": 0, "frame_counts_by_topic": {},
        }
    result: dict[str, dict[str, dict[str, Any]]] = {"BTCUSDT": {}, "ETHUSDT": {}}
    malformed_frames = 0
    chunks_read = 0
    duplicate_ids = 0
    conflicting_ids = 0
    same_seq_multiple_messages = 0
    diagnostic_scan_truncated = False
    trade_rows_observed = 0
    frame_counts_by_topic: dict[str, int] = {}
    sequence_first_frame: dict[tuple[str, str], tuple[str, int]] = {}
    sequence_seen_frames: set[tuple[str, str, str, int]] = set()
    for path in sorted((root / "ops-l2-frames").glob("*.parquet")):
        chunks_read += 1
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            batch_size=16,
            columns=["channel", "frame_type", "raw_payload_bytes", "raw_payload_hash", "received_at_ns"],
        ):
            for row in batch.to_pylist():
                channel = str(row["channel"])
                if channel in bybit_btc_eth_linear_topics():
                    frame_counts_by_topic[channel] = frame_counts_by_topic.get(channel, 0) + 1
                if not channel.startswith("publicTrade."):
                    continue
                symbol = channel.removeprefix("publicTrade.")
                if symbol not in result:
                    continue
                if str(row["frame_type"]).startswith("MALFORMED_FRAME"):
                    malformed_frames += 1
                    continue
                try:
                    frame = json.loads(row["raw_payload_bytes"])
                    data = frame.get("data") if isinstance(frame, Mapping) else None
                    if not isinstance(data, list):
                        malformed_frames += 1
                        continue
                    for trade_row in data:
                        parsed = _normalized_trade(trade_row) if isinstance(trade_row, Mapping) else None
                        if parsed is None:
                            malformed_frames += 1
                            continue
                        trade_rows_observed += 1
                        identity, content = parsed
                        raw_hash = str(row["raw_payload_hash"])
                        received_at_ns = int(row["received_at_ns"])
                        sequence = content.get("seq")
                        if sequence is not None:
                            sequence_key = (symbol, str(sequence))
                            frame_identity = (symbol, str(sequence), raw_hash, received_at_ns)
                            prior_frame = sequence_first_frame.get(sequence_key)
                            if frame_identity not in sequence_seen_frames:
                                if prior_frame is not None:
                                    same_seq_multiple_messages += 1
                                if len(sequence_seen_frames) < MAX_DIAGNOSTIC_TRADE_IDENTITIES:
                                    sequence_seen_frames.add(frame_identity)
                                    sequence_first_frame.setdefault(sequence_key, (raw_hash, received_at_ns))
                                else:
                                    diagnostic_scan_truncated = True
                        symbol_rows = result[symbol]
                        if identity in symbol_rows:
                            duplicate_ids += 1
                            if symbol_rows[identity] != content:
                                conflicting_ids += 1
                            continue
                        if sum(map(len, result.values())) >= MAX_DIAGNOSTIC_TRADE_IDENTITIES:
                            diagnostic_scan_truncated = True
                            continue
                        symbol_rows[identity] = content
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                    malformed_frames += 1
    return result, {
        "malformed_frames": malformed_frames,
        "archive_chunks_read": chunks_read,
        "duplicate_ids": duplicate_ids,
        "conflicting_ids": conflicting_ids,
        "same_seq_multiple_messages": same_seq_multiple_messages,
        "diagnostic_scan_truncated": diagnostic_scan_truncated,
        "trade_rows_observed": trade_rows_observed,
        "frame_counts_by_topic": frame_counts_by_topic,
    }


def _bounded_entries(repository: Any, artifact_type: str, *, limit: int = 10_000) -> tuple[Any, ...]:
    return repository.artifact_entries_by_types((artifact_type,), limit=limit)


def _collect_runtime_summary(
    repository: Any,
    root: Path,
    *,
    snapshot: BybitPublicSnapshotV1 | None,
    receipts_seen: int,
    m1_receipts: int,
    m15_receipts: int,
    candidate_receipts: int,
    calendar_receipts: int,
    s3_not_estimable_receipts: int,
    outcome_reports_seen: int,
    recent_trade_requests_by_symbol: Mapping[str, int],
) -> dict[str, Any]:
    health_entries = _bounded_entries(repository, "PublicStreamSourceHealthV1")
    health_transitions: dict[str, int] = {}
    prior_states: dict[str, str] = {}
    for entry in sorted(health_entries, key=lambda item: (item.available_at_ns, item.artifact_ref)):
        health = entry.metadata.get("health")
        transport = entry.metadata.get("transport")
        if not isinstance(health, Mapping) or not isinstance(transport, Mapping):
            continue
        channel = str(transport.get("channel", "UNKNOWN"))
        state = str(health.get("state", "UNKNOWN"))
        if prior_states.get(channel) != state:
            health_transitions[channel] = health_transitions.get(channel, 0) + 1
            prior_states[channel] = state

    report_entries = _bounded_entries(repository, "PublicStreamContinuityReportV1")
    latest_reports: dict[str, Any] = {}
    for entry in report_entries:
        report = entry.metadata.get("report")
        if isinstance(report, Mapping):
            channel = str(report.get("channel", "UNKNOWN"))
            prior = latest_reports.get(channel)
            if prior is None or (entry.available_at_ns, entry.artifact_ref) > prior[:2]:
                latest_reports[channel] = (entry.available_at_ns, entry.artifact_ref, report)

    conflict_entries = _bounded_entries(repository, "PublicStreamContinuityEventV1")
    conflicting_trade_ids = 0
    malformed_observations = 0
    for entry in conflict_entries:
        observation = entry.metadata.get("observation")
        decision = entry.metadata.get("decision")
        if not isinstance(observation, Mapping) or not isinstance(decision, Mapping):
            continue
        if decision.get("classification") == "CONFLICTING_TRADE_ID":
            conflicting_trade_ids += 1
        if observation.get("kind") == "MALFORMED_FRAME":
            malformed_observations += 1

    websocket_rows, archive_stats = _archive_trade_rows(root)
    rest_rows = _rest_trade_rows(snapshot)
    overlap = compare_trade_overlap(websocket_rows, rest_rows)

    archive_entries = _bounded_entries(repository, "L2FrameArchiveCheckpointV2")
    archive_watermarks: dict[str, Any] = {}
    for entry in archive_entries:
        metadata = entry.metadata
        channel = str(metadata.get("channel", "UNKNOWN"))
        item = archive_watermarks.setdefault(channel, {"chunk_count": 0, "frame_count": 0, "last_available_at_ns": 0})
        item["chunk_count"] += 1
        item["frame_count"] += int(metadata.get("frame_count", 0))
        item["last_available_at_ns"] = max(item["last_available_at_ns"], entry.available_at_ns)
        if entry.available_at_ns >= item["last_available_at_ns"]:
            item["last_checkpoint_ref"] = entry.artifact_ref
            item["last_payload_hash"] = metadata.get("last_payload_hash")

    native_event_entries = _bounded_entries(repository, "OpsDecisionEventSourceV1")
    native_origins = {"M1": 0, "M15": 0}
    for entry in native_event_entries:
        event = entry.metadata.get("event")
        if not isinstance(event, Mapping):
            continue
        event_type = str(event.get("event_type", ""))
        if event_type == "CONFIRMED_1M_CLOSE":
            native_origins["M1"] += 1
        elif event_type == "CONFIRMED_15M_CLOSE":
            native_origins["M15"] += 1

    s3_readiness_entries = _bounded_entries(repository, "S3NativeWarmupReadinessV1")
    readiness_not_estimable = sum(
        isinstance(entry.metadata.get("readiness"), Mapping)
        and entry.metadata["readiness"].get("status") == "NOT_ESTIMABLE"
        for entry in s3_readiness_entries
    )
    maintenance_entries = _bounded_entries(repository, "OutcomeMaturityCycleReportV1")
    maintenance_count = len(maintenance_entries)
    matured_count = sum(
        int(entry.metadata.get("report", {}).get("matured_count", 0))
        for entry in maintenance_entries if isinstance(entry.metadata.get("report"), Mapping)
    )
    return {
        "public_rest": {
            "status": "TESTED" if snapshot is not None and snapshot.successful_request_count > 0 else "TEST GATE",
            "market_request_count": snapshot.request_count if snapshot is not None else 0,
            "successful_market_request_count": snapshot.successful_request_count if snapshot is not None else 0,
            "metadata_request_count": snapshot.bootstrap_request_count if snapshot is not None else 0,
            "successful_metadata_request_count": (
                snapshot.successful_bootstrap_request_count if snapshot is not None else 0
            ),
            "recent_trade_page_limit_per_symbol": 100,
            "recent_trade_requests_per_symbol": dict(recent_trade_requests_by_symbol),
            "recent_trade_overlap": overlap,
        },
        "public_websocket": {
            "status": "UNVERIFIED",
            "topics": list(bybit_btc_eth_linear_topics()),
            "connection_attempt_limit": 1,
            "automatic_retry": False,
        },
        "source_health_transitions": health_transitions,
        "continuity_reports": {
            "count": len(report_entries),
            "latest_by_channel": {
                channel: {"ref": value[1], "as_of_ns": value[2].get("as_of_ns"),
                          "source_current": value[2].get("source_current"),
                          "book_sequence_valid": value[2].get("book_sequence_valid"),
                          "observed_trade_count": value[2].get("observed_trade_count"),
                          "trade_completeness_proven": False,
                          "gap_count": value[2].get("gap_count"),
                          "reason_codes": value[2].get("reasons", [])}
                for channel, value in latest_reports.items()
            },
        },
        "trade_observations": {
            "status": "TESTED" if sum(map(len, websocket_rows.values())) > 0 else "TEST GATE",
            "btc_trade_ids_observed": len(websocket_rows["BTCUSDT"]),
            "eth_trade_ids_observed": len(websocket_rows["ETHUSDT"]),
            "trade_rows_observed": archive_stats["trade_rows_observed"],
            "duplicate_ids": archive_stats["duplicate_ids"],
            "conflicting_ids": max(conflicting_trade_ids, archive_stats["conflicting_ids"]),
            "diagnostic_scan_truncated": archive_stats["diagnostic_scan_truncated"],
        "malformed_frames": max(archive_stats["malformed_frames"], malformed_observations),
            "same_seq_across_multiple_messages": archive_stats["same_seq_multiple_messages"],
            "seq_behavior": (
                "same seq observed across messages where counted; documented shared-seq behavior is accepted"
            ),
            "archive_chunks_read": archive_stats["archive_chunks_read"],
            "completeness_proven": False,
        },
        "frame_counts_by_topic": archive_stats["frame_counts_by_topic"],
        "book_continuity": {
            "by_channel": {
                channel: value[2].get("book_sequence_valid")
                for channel, value in latest_reports.items() if channel.startswith("orderbook.")
            },
        },
        "archive_index_watermarks": archive_watermarks,
        "supervisor": {
            "receipt_count": receipts_seen,
            "m1_origin_count": native_origins["M1"],
            "m15_origin_count": native_origins["M15"],
            "m1_supervisor_receipts": m1_receipts,
            "m15_supervisor_receipts": m15_receipts,
            "candidate_set_receipts": candidate_receipts,
            "decision_calendar_entries": calendar_receipts,
            "s3_not_estimable_receipts": s3_not_estimable_receipts + readiness_not_estimable,
            "outcome_maintenance_reports": max(outcome_reports_seen, maintenance_count),
            "matured_outcome_count": matured_count,
        },
    }


def _merge_receipt_counts(
    result: Any,
    counts: dict[str, int],
) -> None:
    from ..runtime.ops_supervisor import PipelineStageV1, OpsTerminalStatusV1

    counts["supervisor_cycles"] += 1
    for receipt in result.event_receipts:
        counts["decision_receipts"] += 1
        if receipt.event.event_type == "CONFIRMED_1M_CLOSE":
            counts["m1_receipts"] += 1
        elif receipt.event.event_type == "CONFIRMED_15M_CLOSE":
            counts["m15_receipts"] += 1
        candidate = receipt.result.stages[tuple(PipelineStageV1).index(PipelineStageV1.CANDIDATE_SET)]
        calendar = receipt.result.stages[tuple(PipelineStageV1).index(PipelineStageV1.DECISION_CALENDAR)]
        counts["candidate_receipts"] += bool(candidate.artifact_refs)
        counts["calendar_receipts"] += len(receipt.calendar_refs)
        if receipt.event.event_type == "CONFIRMED_1M_CLOSE" and (
            receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
            or "BYBIT_TRADE_COMPLETENESS_UNPROVEN" in (receipt.result.missing_reason or "")
        ):
            counts["s3_not_estimable_receipts"] += 1


def _status_from_source(source: PublicStreamSourceV2) -> Any:
    return source.status()


def _real_public_smoke(
    root: Path,
    host: Mapping[str, Any],
    *,
    duration_seconds: int,
    clock_ns: Callable[[], int],
    monotonic_ns: Callable[[], int],
    sleep_fn: Callable[[float], None],
    runtime_builder: Callable[..., tuple[Any, Any, Any]] | None = None,
) -> dict[str, Any]:
    if type(duration_seconds) is not int or not MIN_REAL_SMOKE_SECONDS <= duration_seconds <= MAX_REAL_SMOKE_SECONDS:
        raise ValueError("real public smoke duration must be from 20 through 180 seconds")
    if host.get("host_status") != "TESTED" or host.get("paths", {}).get("filesystem_class") == "WINDOWS_MOUNT":
        return {"status": "BLOCKED BY ENVIRONMENT", "reason_code": "PUBLIC_HOST_PATH_NOT_QUALIFIED"}

    started_at_ns = clock_ns()
    started_mono_ns = monotonic_ns()
    sizes_at_start = _safe_file_sizes(root)
    deadline_ns = started_mono_ns + duration_seconds * 1_000_000_000
    activity_deadline_ns = deadline_ns - SMOKE_SHUTDOWN_RESERVE_NS
    rest_capture: _SnapshotCaptureSource | None = None
    stream: PublicStreamSourceV2 | Any | None = None
    supervisor: Any | None = None
    supervisor_close_attempted = False
    port: Any | None = None
    indexed_source: Any = None
    counters = {
        "supervisor_cycles": 0,
        "decision_receipts": 0,
        "m1_receipts": 0,
        "m15_receipts": 0,
        "candidate_receipts": 0,
        "calendar_receipts": 0,
        "s3_not_estimable_receipts": 0,
    }
    runtime_status = "TEST GATE"
    stop_reason = "REAL_PUBLIC_SMOKE_NOT_COMPLETED"
    connection_opened = False
    ack_deadline_ns = min(
        activity_deadline_ns,
        started_mono_ns + SUBSCRIPTION_ACK_DEADLINE_SECONDS * 1_000_000_000,
    )
    try:
        if runtime_builder is None:
            from ..runtime.ops_supervisor import OpsSupervisorV2
            from ..runtime.production import IndexedPublicCycleSourceV1, create_bybit_public_ws_port

            rest_capture = _SnapshotCaptureSource(BybitPublicCycleSourceV1())
            stream = PublicStreamSourceV2(
                venue=VenueV2.BYBIT,
                topics=bybit_btc_eth_linear_topics(),
                source_id="BYBIT_PUBLIC_WS",
                max_attempts=1,
                initial_backoff_seconds=0,
                max_backoff_seconds=0,
            )
            port = create_bybit_public_ws_port(public_source=rest_capture, public_stream_source=stream)
            supervisor = OpsSupervisorV2(root / "ops.sqlite", port, clock_ns=clock_ns, monotonic_ns=monotonic_ns)
            indexed_source = IndexedPublicCycleSourceV1()
        else:
            port, stream, supervisor = runtime_builder(root=root, clock_ns=clock_ns, monotonic_ns=monotonic_ns)
            rest_capture = getattr(port, "public_source", None)
            indexed_source = getattr(port, "indexed_source", None)

        initial = supervisor.run_once()
        _merge_receipt_counts(initial, counters)
        if hasattr(port, "public_source") and runtime_builder is None:
            # One REST snapshot is enough for this smoke; later cycles consume only indexed evidence.
            port.public_source = indexed_source

        subscription_acknowledged = False
        while monotonic_ns() < activity_deadline_ns:
            source_status = _status_from_source(stream)
            handoff = source_status.handoff
            connection_opened = connection_opened or handoff.connected or handoff.successful_subscription_ack_count > 0
            if getattr(handoff, "successful_subscription_ack_count", 0) > 0:
                subscription_acknowledged = True
                break
            if getattr(source_status, "attempt_count", 0) >= 1 and (
                source_status.state in {"FAILED", "EXHAUSTED"} or not handoff.connected
                and getattr(handoff, "last_error_code", None) is not None
            ):
                stop_reason = "PUBLIC_WEBSOCKET_CONNECTION_FAILED_NO_RETRY"
                break
            if monotonic_ns() >= ack_deadline_ns:
                stop_reason = "SUBSCRIPTION_ACK_NOT_OBSERVED_WITHIN_BOUND"
                break
            result = supervisor.run_once()
            _merge_receipt_counts(result, counters)
            sleep_fn(min(SMOKE_CYCLE_INTERVAL_SECONDS, max(0, activity_deadline_ns - monotonic_ns()) / 1e9))

        if subscription_acknowledged:
            runtime_status = "TESTED"
            stop_reason = "FIXED_DURATION_COMPLETE"
            while monotonic_ns() < activity_deadline_ns:
                source_status = _status_from_source(stream)
                handoff = source_status.handoff
                connection_opened = connection_opened or handoff.connected or handoff.successful_subscription_ack_count > 0
                if handoff.overflowed:
                    runtime_status = "TEST GATE"
                    stop_reason = "PUBLIC_FRAME_QUEUE_OVERFLOW"
                    break
                if handoff.disconnect_count > 0 or source_status.state in {"FAILED", "EXHAUSTED"}:
                    runtime_status = "TEST GATE"
                    stop_reason = "PUBLIC_WEBSOCKET_DISCONNECT_OR_ERROR"
                    break
                result = supervisor.run_once()
                _merge_receipt_counts(result, counters)
                sleep_fn(min(SMOKE_CYCLE_INTERVAL_SECONDS, max(0, activity_deadline_ns - monotonic_ns()) / 1e9))
        elif runtime_status != "TEST GATE":
            runtime_status = "BLOCKED BY ENVIRONMENT"

        before_close_status = _status_from_source(stream)
        connection_opened = (
            connection_opened or before_close_status.handoff.connected
            or before_close_status.handoff.successful_subscription_ack_count > 0
        )
        while before_close_status.handoff.queue_items and monotonic_ns() < activity_deadline_ns:
            result = supervisor.run_once()
            _merge_receipt_counts(result, counters)
            before_close_status = _status_from_source(stream)
            connection_opened = connection_opened or before_close_status.handoff.connected
        status_before_close = before_close_status
        if status_before_close.handoff.queue_items:
            runtime_status = "TEST GATE"
            stop_reason = "PENDING_FRAMES_AT_STOP"
        supervisor_close_attempted = True
        supervisor.close()
        supervisor = None
        finish_ns = clock_ns()
        elapsed_ns = max(0, monotonic_ns() - started_mono_ns)
        with _readonly_repository(root / "ops.sqlite") as repository:
            summary = _collect_runtime_summary(
                repository,
                root,
                snapshot=rest_capture.snapshot if isinstance(rest_capture, _SnapshotCaptureSource) else None,
                receipts_seen=counters["decision_receipts"],
                m1_receipts=counters["m1_receipts"],
                m15_receipts=counters["m15_receipts"],
                candidate_receipts=counters["candidate_receipts"],
                calendar_receipts=counters["calendar_receipts"],
                s3_not_estimable_receipts=counters["s3_not_estimable_receipts"],
                outcome_reports_seen=0,
                recent_trade_requests_by_symbol=(
                    rest_capture.recent_trade_requests
                    if isinstance(rest_capture, _SnapshotCaptureSource)
                    else {"BTCUSDT": 0, "ETHUSDT": 0}
                ),
            )
        current_source_status = _status_from_source(stream)
        handoff = current_source_status.handoff
        ack_ok = handoff.successful_subscription_ack_count > 0 and set(handoff.subscription_ack_topics) == set(
            bybit_btc_eth_linear_topics()
        )
        websocket_status = "TESTED" if (
            ack_ok and handoff.frames_received > 0 and not handoff.overflowed and not handoff.closed_rejections
            and status_before_close.handoff.disconnect_count == 0
            and status_before_close.state not in {"FAILED", "EXHAUSTED"}
        ) else "TEST GATE"
        reports = summary["continuity_reports"]["latest_by_channel"]
        book_is_valid = all(
            reports.get(topic, {}).get("book_sequence_valid") is True
            for topic in ("orderbook.50.BTCUSDT", "orderbook.50.ETHUSDT")
        )
        observation_gates = adjudicate_observed_interval(
            subscription_acknowledged=ack_ok,
            frame_count=handoff.frames_received,
            trade_count=summary["trade_observations"]["trade_rows_observed"],
            book_sequence_valid=book_is_valid,
            disconnect_count=status_before_close.handoff.disconnect_count,
            queue_overflow=handoff.overflowed or bool(handoff.closed_rejections),
            transport_error=bool(status_before_close.last_error_code or status_before_close.handoff.last_error_code),
            conflicting_trade_ids=summary["trade_observations"]["conflicting_ids"],
        )
        runtime_status = "TESTED" if runtime_status == "TESTED" and ack_ok and websocket_status == "TESTED" else (
            runtime_status if runtime_status == "BLOCKED BY ENVIRONMENT" else "TEST GATE"
        )
        if handoff.overflowed or handoff.closed_rejections or status_before_close.handoff.disconnect_count or observation_gates[
            "observed_trade_evidence"
        ] == "TEST GATE" and summary["trade_observations"]["conflicting_ids"]:
            runtime_status = "TEST GATE"
        summary["public_websocket"]["status"] = websocket_status
        summary.update({
            "status": runtime_status,
            "started_at_ns": started_at_ns,
            "finished_at_ns": finish_ns,
            "duration_ns": elapsed_ns,
            "duration_seconds": elapsed_ns / 1_000_000_000,
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "topics": list(bybit_btc_eth_linear_topics()),
            "connection_attempts": current_source_status.attempt_count,
            "connection_epochs": [1] if connection_opened else [],
            "connection_opened": connection_opened,
            "subscription_ack_count": handoff.successful_subscription_ack_count,
            "subscription_acknowledged": ack_ok,
            "frame_counts": {
                "network_frames_offered": handoff.frames_offered,
                "received": handoff.frames_received,
                "drained": handoff.frames_drained,
                "control_frames": handoff.controls_received,
                "archived_by_topic": summary["frame_counts_by_topic"],
                "network_bytes_offered": handoff.frame_bytes_offered,
                "bytes_received": handoff.frame_bytes_received,
                "bytes_drained": handoff.frame_bytes_drained,
                "first_receipt_at_ns": handoff.first_frame_received_at_ns,
                "last_receipt_at_ns": handoff.last_frame_received_at_ns,
            },
            "queue": {
                "high_water_items": handoff.high_water_items,
                "high_water_bytes": handoff.high_water_bytes,
                "capacity_items": handoff.max_queue_items,
                "capacity_bytes": handoff.max_queue_bytes,
                "frames_rejected": handoff.frames_rejected,
                "closed_rejections": handoff.closed_rejections,
                "rejected_total": handoff.frames_rejected + handoff.closed_rejections,
                "overflowed": handoff.overflowed,
                "backpressure": handoff.backpressure,
                "pending_items_at_close": handoff.queue_items,
            },
            "disconnect_count": status_before_close.handoff.disconnect_count,
            "planned_shutdown_disconnect_count": max(
                0, handoff.disconnect_count - status_before_close.handoff.disconnect_count,
            ),
            "error_code": status_before_close.last_error_code or status_before_close.handoff.last_error_code,
            "source_state_before_shutdown": status_before_close.state,
            "malformed_frame_count": summary["trade_observations"]["malformed_frames"],
            "qualification": {
                "public_runtime": runtime_status,
                "public_rest_transport": summary["public_rest"]["status"],
                **observation_gates,
                "public_websocket_transport": websocket_status,
                "book_continuity": observation_gates["book_continuity"],
                "observed_trade_evidence": observation_gates["observed_trade_evidence"],
                "trade_completeness": observation_gates["trade_completeness"],
                "trade_completeness_reason_codes": list(_REASON_CODES),
                "s1_s2_live_cadence": "TESTED" if counters["m15_receipts"] else "TEST GATE",
                "s3_native_cadence": "TESTED" if counters["m1_receipts"] else "TEST GATE",
                "economics": "NOT ESTIMABLE",
                "capital": "DISABLED",
                "assisted": "DISABLED",
                "critic_authority": "ZERO",
            },
            "stop_reason": stop_reason,
            "real_network_calls_enabled": True,
            "credentialed_calls": 0,
            "account_calls": 0,
            "order_calls": 0,
            "provider_model_calls": 0,
            "raw_frames_committed": False,
            "host_private_identifiers_emitted": False,
        })
        if isinstance(rest_capture, _SnapshotCaptureSource) and rest_capture.snapshot is not None:
            exact_keys = {
                record.instrument_key for record in rest_capture.snapshot.records
                if record.instrument_key.native_symbol in {"BTCUSDT", "ETHUSDT"}
            }
            if {item.native_symbol for item in exact_keys} == {"BTCUSDT", "ETHUSDT"}:
                summary["trade_completeness_assessment"] = build_bybit_trade_completeness_assessment(
                    tuple(exact_keys), assessed_at_ns=finish_ns,
                )
        summary["sqlite_sizes_bytes"] = _safe_file_sizes(root)
        summary["smoke_start_sizes_bytes"] = sizes_at_start
        summary["sqlite_size_deltas_bytes"] = {
            key: summary["sqlite_sizes_bytes"][key] - sizes_at_start[key]
            for key in sizes_at_start
        }
        summary["resource_observation"] = {
            **host.get("resource_observation", {}),
            "process_rss_end_bytes": _read_process_rss_bytes(),
        }
        if elapsed_ns > duration_seconds * 1_000_000_000:
            summary["status"] = "TEST GATE"
            summary["stop_reason"] = "DECLARED_DURATION_EXCEEDED"
        return summary
    except Exception as exc:
        if supervisor is not None and not supervisor_close_attempted:
            supervisor_close_attempted = True
            try:
                supervisor.close()
            except Exception:
                pass
        status = "BLOCKED BY ENVIRONMENT" if isinstance(exc, (OSError, TimeoutError)) else "TEST GATE"
        snapshot = rest_capture.snapshot if isinstance(rest_capture, _SnapshotCaptureSource) else None
        source = rest_capture.source if isinstance(rest_capture, _SnapshotCaptureSource) else None
        recent_trade_requests = (
            rest_capture.recent_trade_requests
            if isinstance(rest_capture, _SnapshotCaptureSource) else {"BTCUSDT": 0, "ETHUSDT": 0}
        )
        market_requests = snapshot.request_count if snapshot is not None else 0
        successful_market_requests = snapshot.successful_request_count if snapshot is not None else 0
        metadata_requests = snapshot.bootstrap_request_count if snapshot is not None else int(
            getattr(source, "_bootstrap_request_count", 0)
        )
        successful_metadata_requests = snapshot.successful_bootstrap_request_count if snapshot is not None else int(
            getattr(source, "_successful_bootstrap_request_count", 0)
        )
        return {
            "status": status,
            "started_at_ns": started_at_ns,
            "finished_at_ns": clock_ns(),
            "duration_seconds": max(0, monotonic_ns() - started_mono_ns) / 1_000_000_000,
            "stop_reason": type(exc).__name__,
            "connection_attempts": stream.status().attempt_count if stream is not None else 0,
            "public_rest": {
                "status": "TESTED" if successful_market_requests else "TEST GATE",
                "market_request_count": market_requests,
                "successful_market_request_count": successful_market_requests,
                "metadata_request_count": metadata_requests,
                "successful_metadata_request_count": successful_metadata_requests,
                "recent_trade_requests_per_symbol": dict(recent_trade_requests),
            },
            "raw_frames_committed": False,
            "host_private_identifiers_emitted": False,
            "trade_completeness": "NOT ESTIMABLE",
            "trade_completeness_reason_codes": list(_REASON_CODES),
            "qualification": {
                "public_runtime": status,
                "public_rest_transport": "TEST GATE",
                "public_websocket_transport": "TEST GATE",
                "book_continuity": "TEST GATE",
                "observed_trade_evidence": "TEST GATE",
                "trade_completeness": "NOT ESTIMABLE",
                "s3_strategy_input_readiness": "NOT ESTIMABLE",
                "economics": "NOT ESTIMABLE",
                "capital": "DISABLED",
            },
        }


@contextmanager
def _readonly_repository(path: Path) -> Iterator[Any]:
    from ..memory.repository import OpsRepository

    repository = OpsRepository(path, read_only=True)
    try:
        yield repository
    finally:
        repository.close()


def run_public_host_source_qualification(
    *,
    data_root: Path | None = None,
    real_public_smoke: bool = False,
    duration_seconds: int = DEFAULT_REAL_SMOKE_SECONDS,
    clock_ns: Callable[[], int] = time.time_ns,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
    sleep_fn: Callable[[float], None] = time.sleep,
    runtime_builder: Callable[..., tuple[Any, Any, Any]] | None = None,
) -> dict[str, Any]:
    """Run local checks; network is unreachable from this path without opt-in."""
    if type(duration_seconds) is not int or not MIN_REAL_SMOKE_SECONDS <= duration_seconds <= MAX_REAL_SMOKE_SECONDS:
        raise ValueError("duration_seconds must be from 20 through the 180 second smoke ceiling")
    if data_root is None:
        root = Path(tempfile.mkdtemp(prefix="atlas-session035-", dir="/tmp"))
    else:
        root = Path(data_root)
    if real_public_smoke and (root.is_symlink() or not root.name.startswith("atlas-session035-")):
        return {
            "schema_version": 1,
            "status": "TEST GATE",
            "network_calls": 0,
            "real_public_smoke_opt_in": True,
            "real_public_smoke": {"status": "TEST GATE", "reason_code": "DISPOSABLE_ROOT_REQUIRED"},
        }
    try:
        os_class, os_status = classify_host(sys.platform, platform_module.release())
        path_class = classify_filesystem_path(root, host_class=os_class)
        if os_status == "TESTED" and path_class != "WINDOWS_MOUNT":
            root.mkdir(parents=True, exist_ok=True)
        existing_names = {entry.name for entry in root.iterdir()} if root.exists() else set()
        if existing_names - {"controller.lock"}:
            raise ValueError("qualification data root must be empty and disposable")
    except (OSError, ValueError) as exc:
        return {
            "schema_version": 1,
            "status": "TEST GATE" if isinstance(exc, ValueError) else "BLOCKED BY ENVIRONMENT",
            "reason_code": type(exc).__name__,
            "network_calls": 0,
            "real_public_smoke_opt_in": real_public_smoke,
            "real_public_smoke": {
                "status": "TEST GATE" if isinstance(exc, ValueError) else "BLOCKED BY ENVIRONMENT",
                "reason_code": type(exc).__name__,
            },
        }

    if os_status != "TESTED" or path_class == "WINDOWS_MOUNT":
        host = inspect_host(root)
        host["host_status"] = "BLOCKED BY ENVIRONMENT" if os_status != "TESTED" else "TEST GATE"
        result: dict[str, Any] = {
            "schema_version": 1,
            "host": host,
            "network_calls": 0,
            "real_public_smoke_opt_in": real_public_smoke,
            "real_public_smoke": {"status": "BLOCKED BY ENVIRONMENT", "reason_code": "HOST_OR_PATH_UNSUPPORTED"},
        }
        return result

    try:
        with controller_writer_lock(root / "controller.lock"):
            host = inspect_host(root, lock_already_held=True)
            result = {
                "schema_version": 1,
                "host": host,
                "network_calls": 0,
                "real_public_smoke_opt_in": real_public_smoke,
                "queue_configuration": {
                    "queue_items": DEFAULT_PUBLIC_FRAME_QUEUE_ITEMS,
                    "queue_bytes": DEFAULT_PUBLIC_FRAME_QUEUE_BYTES,
                    "receive_queue_items": PUBLIC_WS_RECEIVE_QUEUE_ITEMS,
                    "controller_frames_per_cycle": 32,
                    "default_drain_items": DEFAULT_PUBLIC_FRAME_DRAIN_ITEMS,
                },
            }
            if real_public_smoke:
                if host["host_status"] != "TESTED":
                    result["real_public_smoke"] = {
                        "status": "BLOCKED BY ENVIRONMENT", "reason_code": "HOST_QUALIFICATION_FAILED",
                    }
                else:
                    # This call is the only branch that constructs venue transports.
                    smoke = _real_public_smoke(
                        root, host, duration_seconds=duration_seconds,
                        clock_ns=clock_ns, monotonic_ns=monotonic_ns, sleep_fn=sleep_fn,
                        runtime_builder=runtime_builder,
                    )
                    result["real_public_smoke"] = smoke
                    rest = smoke.get("public_rest", {})
                    result["network_calls"] = {
                        "bybit_public_rest_gets": int(rest.get("market_request_count", 0))
                        + int(rest.get("metadata_request_count", 0)),
                        "bybit_public_websocket_connection_attempts": int(smoke.get("connection_attempts", 0)),
                        "credentialed_calls": 0,
                        "account_calls": 0,
                        "order_calls": 0,
                        "provider_model_calls": 0,
                    }
            else:
                result["real_public_smoke"] = {"status": "UNVERIFIED", "reason_code": "EXPLICIT_OPT_IN_REQUIRED"}
            return result
    except WriterLockBusy:
        return {
            "schema_version": 1,
            "status": "TEST GATE",
            "host": {"host_status": "TEST GATE", "single_writer_lock": {"acquired": False}},
            "network_calls": 0,
            "real_public_smoke_opt_in": real_public_smoke,
            "real_public_smoke": {"status": "TEST GATE", "reason_code": "SINGLE_WRITER_LOCK_BUSY"},
        }
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        return {
            "schema_version": 1,
            "status": "TEST GATE",
            "reason_code": type(exc).__name__,
            "network_calls": 0,
            "real_public_smoke_opt_in": real_public_smoke,
            "real_public_smoke": {"status": "TEST GATE", "reason_code": "HOST_QUALIFICATION_FAILED"},
        }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, help="empty disposable root for ops.sqlite and local raw archives")
    parser.add_argument("--real-public-smoke", action="store_true", help="explicitly enable one public Bybit smoke")
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_REAL_SMOKE_SECONDS)
    parser.add_argument("--summary-path", type=Path, help="write the sanitized JSON summary to this path")
    args = parser.parse_args(argv)
    result = run_public_host_source_qualification(
        data_root=args.data_root,
        real_public_smoke=args.real_public_smoke,
        duration_seconds=args.duration_seconds,
    )
    serialized = canonical_json(result)
    print(serialized)
    if args.summary_path is not None:
        args.summary_path.write_text(serialized + "\n", encoding="utf-8")
    return 0 if result.get("host", {}).get("host_status") in {"TESTED", "BLOCKED BY ENVIRONMENT"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
