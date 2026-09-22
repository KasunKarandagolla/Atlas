"""Credential-free public-market collection boundary. No order/private APIs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from atlas.domain.enums import AvailabilityClass

from .models import DataKind, MarketRecord, make_record


@dataclass(frozen=True)
class PublicRawEvent:
    record_id: str
    instrument: str
    data_kind: DataKind
    source_event_at_ns: int | None
    source_published_at_ns: int | None
    payload: dict[str, Any]
    evidence_ref: str
    bar_start_ns: int | None = None
    bar_end_ns: int | None = None


class PublicMarketCollector:
    """Converts public adapter events to causal records using the actual local receipt clock."""

    def __init__(self, clock_ns: Callable[[], int], pipeline_version: str = "phase3-v1"):
        self.clock_ns = clock_ns
        self.pipeline_version = pipeline_version

    def ingest(self, event: PublicRawEvent) -> MarketRecord:
        received = self.clock_ns()
        processed = self.clock_ns()
        available = max(received, processed, event.bar_end_ns or 0, event.source_published_at_ns or 0)
        recorded = self.clock_ns()
        return make_record(
            record_id=event.record_id,
            source_id="BYBIT_PUBLIC",
            venue="BYBIT",
            instrument=event.instrument,
            data_kind=event.data_kind,
            payload=event.payload,
            received_at_ns=received,
            processed_at_ns=processed,
            available_at_ns=available,
            data_ingested_at_ns=received,
            recorded_at_ns=max(recorded, processed),
            evidence_ref=event.evidence_ref,
            source_event_at_ns=event.source_event_at_ns,
            source_published_at_ns=event.source_published_at_ns,
            bar_start_ns=event.bar_start_ns,
            bar_end_ns=event.bar_end_ns,
            availability_class=AvailabilityClass.ACTUAL_OBSERVED,
            pipeline_version=self.pipeline_version,
        )


def _field(event: Any, name: str, default: Any = None) -> Any:
    if isinstance(event, Mapping):
        return event.get(name, default)
    return getattr(event, name, default)


def _ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if hasattr(value, "value"):
        return int(value.value)
    if hasattr(value, "timestamp_ns"):
        return int(value.timestamp_ns)
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1_000_000_000)
    return int(value)


def _symbol(event: Any, instrument: str | None) -> str:
    raw = instrument or _field(event, "instrument_id") or _field(event, "symbol")
    if raw is None:
        raise ValueError("public event instrument is required")
    value = str(raw).split(".")[0].split("-")[0]
    if value not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("public event outside BTCUSDT/ETHUSDT scope")
    return value


def translate_nautilus_public_event(
    event: Any, *, instrument: str | None = None, record_id: str | None = None, evidence_ref: str | None = None
) -> PublicRawEvent:
    """Translate supported public Nautilus/Bybit event shapes to ``PublicRawEvent``.

    The adapter is deliberately read-only.  Unsupported/private event types
    fail closed instead of being guessed into a market record.
    """
    name = (
        type(event).__name__.lower()
        if not isinstance(event, Mapping)
        else str(event.get("type", event.get("event_type", ""))).lower()
    )
    bar_type = str(_field(event, "bar_type", "")).lower()
    if "bar" in name or bar_type:
        if "1-minute" in bar_type or "1m" in bar_type:
            kind = (
                DataKind.BAR_1M_MARK
                if "mark" in bar_type
                else DataKind.BAR_1M_INDEX
                if "index" in bar_type
                else DataKind.BAR_1M_LAST
            )
        elif "1-hour" in bar_type or "1h" in bar_type:
            kind = DataKind.BAR_1H_LAST
        else:
            raise ValueError(f"unsupported public bar type: {bar_type or name}")
        payload = {
            k: str(_field(event, k)) for k in ("open", "high", "low", "close", "volume") if _field(event, k) is not None
        }
        source_event = _ns(_field(event, "ts_event"))
        bar_start = _ns(_field(event, "bar_start_ns", _field(event, "ts_event")))
        bar_end = _ns(_field(event, "bar_end_ns", _field(event, "ts_init", source_event)))
    elif "quote" in name:
        kind = DataKind.QUOTE_TOP
        payload = {
            k: str(_field(event, k))
            for k in ("bid_price", "ask_price", "bid_size", "ask_size")
            if _field(event, k) is not None
        }
        source_event = _ns(_field(event, "ts_event"))
        bar_start = bar_end = None
    elif "trade" in name:
        kind = DataKind.TRADE
        payload = {
            k: str(_field(event, k)) for k in ("price", "size", "aggressor_side") if _field(event, k) is not None
        }
        source_event = _ns(_field(event, "ts_event"))
        bar_start = bar_end = None
    elif "funding" in name:
        kind = DataKind.FUNDING
        payload = {k: str(_field(event, k)) for k in ("rate", "next_funding_ns") if _field(event, k) is not None}
        source_event = _ns(_field(event, "ts_event", _field(event, "funding_time_ns")))
        bar_start = bar_end = None
    elif "interest" in name or "open_interest" in name:
        kind = DataKind.OPEN_INTEREST
        payload = {k: str(_field(event, k)) for k in ("open_interest", "value") if _field(event, k) is not None}
        source_event = _ns(_field(event, "ts_event"))
        bar_start = bar_end = None
    elif "depth" in name or "orderbook" in name:
        kind = DataKind.DEPTH
        payload = {"bids": _field(event, "bids", []), "asks": _field(event, "asks", [])}
        source_event = _ns(_field(event, "ts_event"))
        bar_start = bar_end = None
    else:
        raise ValueError(f"unsupported public event shape: {type(event).__name__}")
    symbol = _symbol(event, instrument)
    identity = record_id or str(_field(event, "trade_id", _field(event, "id", "")))
    if not identity:
        identity = hashlib.sha256(
            json.dumps(
                {"instrument": symbol, "kind": kind.value, "source_event_at_ns": source_event, "payload": payload},
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    return PublicRawEvent(
        identity,
        symbol,
        kind,
        source_event,
        _ns(_field(event, "ts_init")) or source_event,
        payload,
        evidence_ref or f"nautilus-public:{identity}",
        bar_start,
        bar_end,
    )


def build_nautilus_public_testnet_config():
    """Offline construction only; no connect. Public live/testnet collection remains environment-dependent."""
    from atlas.runtime.nautilus_boundary import build_offline_rc5_bybit_configs

    return build_offline_rc5_bybit_configs().data
