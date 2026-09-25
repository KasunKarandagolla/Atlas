"""Collection-only boundary for exact public frames; transport qualification is separate."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import nonblank, sha256_json, timestamp
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.public_http import PublicHttpResponseV2, PublicVenueV2
from atlas.v2.data.raw import AppendStatusV2, RawObservationV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2


def _obj(raw: bytes) -> Mapping[str, Any]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("public frame must be an object")
    return value


def _int(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


@dataclass(frozen=True)
class ForwardReceiptV2:
    observation_ref: str
    raw_payload_hash: str
    usable: bool
    reason: str


def _indexed_observation_ref(collector: PublicCollectorV2, obs: RawObservationV2) -> str:
    index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": obs.record_id})
    indexed = collector.repository.get_artifact(index_ref)
    if indexed is None:
        raise RuntimeError("persisted observation index missing after archive flush")
    return indexed.content_hash


class BinanceDepthCaptureV2:
    """Persist REST snapshot and USD-M diff frames; never heal a gap with later deltas.

    A caller supplies exact bytes and capture-time clock values from a public
    WebSocket transport. This boundary does not assert that such a transport is
    deployed or qualified. Fresh snapshot plus bridging diff is required on every
    restart and reconnect.
    """

    def __init__(self, collector: PublicCollectorV2, key: InstrumentKeyV2, *, depth: int = 100) -> None:
        if key.venue != "BINANCE" or depth not in (5, 10, 20, 50, 100, 500, 1000):
            raise ValueError("Binance USD-M key and declared depth required")
        self.collector = collector
        self.key = key
        self.depth = depth
        self.source_id = f"BINANCE_USDM_DEPTH_{depth}_{key.native_symbol}"
        self.snapshot_update_id: int | None = None
        self.last_update_id: int | None = None
        self.valid = False

    def _save(self, raw: bytes, *, event_type: str, event_at_ns: int | None, received_at_ns: int,
              sequence: int) -> ForwardReceiptV2:
        timestamp(received_at_ns, field="actual receipt")
        now = self.collector.clock_ns()
        if now < received_at_ns:
            raise ValueError("ingestion cannot predate actual receipt")
        obs = RawObservationV2.build(instrument_revision=self.key.contract_revision, source_id=self.source_id,
            event_type=event_type, received_at_ns=received_at_ns, ingested_at_ns=now,
            available_at_ns=now, translation_version="BINANCE_USDM_DEPTH_CAPTURE_V1",
            payload=raw, event_at_ns=event_at_ns, sequence=sequence)
        result = self.collector.ingest(obs, raw_payload=raw)
        if result.append.status == AppendStatusV2.CONFLICT_QUARANTINED:
            self.valid = False
            return ForwardReceiptV2(obs.content_hash, obs.raw_payload_hash, False, "CONFLICT")
        self.collector.flush_archive()
        if result.append.status == AppendStatusV2.DUPLICATE:
            return ForwardReceiptV2(_indexed_observation_ref(self.collector, obs),
                                    obs.raw_payload_hash, self.valid, "DUPLICATE")
        cursor = {"source_id": self.source_id, "last_update_id": self.last_update_id,
                  "snapshot_update_id": self.snapshot_update_id, "sequence_valid": self.valid,
                  "observation_ref": obs.content_hash}
        ref = sha256_json({"artifact_type": "ForwardDepthCursorV2", "cursor": cursor})
        self.collector.repository.register_artifact(ArtifactIndexEntryV2(ref, "ForwardDepthCursorV2", ref,
            now, now, cursor))
        return ForwardReceiptV2(obs.content_hash, obs.raw_payload_hash, self.valid, "SEQUENCE_VALID" if self.valid else "INCOMPLETE_SNAPSHOT")

    def snapshot(self, response: PublicHttpResponseV2) -> ForwardReceiptV2:
        if response.venue != PublicVenueV2.BINANCE or response.path != "/fapi/v1/depth":
            raise ValueError("exact public Binance depth response required")
        body = _obj(response.raw_body)
        update_id = _int(body.get("lastUpdateId"), "lastUpdateId")
        self.valid = False
        self.snapshot_update_id = update_id
        self.last_update_id = None
        self.collector.mark_incomplete_snapshot(self.source_id, at_ns=response.received_at_ns,
            details="snapshot archived; bridging diff still required")
        return self._save(response.raw_body, event_type="L2_SNAPSHOT", event_at_ns=None,
                          received_at_ns=response.received_at_ns, sequence=update_id)

    def delta(self, raw: bytes, *, received_at_ns: int) -> ForwardReceiptV2:
        body = _obj(raw)
        if body.get("e") != "depthUpdate" or body.get("s") != self.key.native_symbol:
            raise ValueError("wrong depth stream or symbol")
        first = _int(body.get("U"), "U")
        last = _int(body.get("u"), "u")
        previous = _int(body.get("pu"), "pu")
        event_at = _int(body.get("E"), "E") * 1_000_000
        if first > last:
            raise ValueError("invalid depth range")
        if self.valid and last == self.last_update_id:
            return self._save(raw, event_type="L2_DELTA", event_at_ns=event_at,
                              received_at_ns=received_at_ns, sequence=last)
        if self.snapshot_update_id is None:
            self.valid = False
        elif self.last_update_id is None:
            if last < self.snapshot_update_id:
                pass  # old buffered frame, archived but unusable
            elif first <= self.snapshot_update_id <= last:
                self.valid = True
                self.last_update_id = last
            else:
                self.valid = False
                self.collector.mark_incomplete_snapshot(self.source_id, at_ns=self.collector.clock_ns(),
                    details="snapshot/delta bridge failed; fresh snapshot required")
                self.snapshot_update_id = None
        elif self.valid and previous == self.last_update_id:
            self.last_update_id = last
        else:
            self.valid = False
            self.snapshot_update_id = None
            self.last_update_id = None
            self.collector.on_disconnect(self.source_id, at_ns=self.collector.clock_ns())
            self.collector.mark_incomplete_snapshot(self.source_id, at_ns=self.collector.clock_ns(),
                details="depth sequence gap/reset; fresh snapshot required")
        receipt = self._save(raw, event_type="L2_DELTA", event_at_ns=event_at,
                             received_at_ns=received_at_ns, sequence=last)
        if self.valid:
            self.collector.reconcile_after_reconnect(self.source_id, at_ns=self.collector.clock_ns(),
                complete_snapshot=True, missed_interval_repaired=True)
        return receipt

    def disconnected(self, at_ns: int) -> None:
        self.valid = False
        self.snapshot_update_id = None
        self.last_update_id = None
        self.collector.on_disconnect(self.source_id, at_ns=at_ns)

    def source_state(self) -> PublicSourceStateV2 | None:
        latest = self.collector.health.latest(self.source_id)
        return latest.state if latest is not None else None


def bybit_trade_semantics(row: Mapping[str, Any]) -> tuple[str, str, str]:
    """V5 side is taker side; quantity is base asset for linear contracts."""
    raw_id = row.get("i")
    if not isinstance(raw_id, str):
        raise ValueError("trade id must be a string")
    trade_id = nonblank(raw_id, field="trade id")
    side = row.get("S")
    if side not in ("Buy", "Sell"):
        raise ValueError("Bybit trade side must be documented taker Buy/Sell")
    quantity = Decimal(str(row.get("v")))
    if not quantity.is_finite() or quantity <= 0:
        raise ValueError("trade base quantity invalid")
    return trade_id, str(side), "BASE_ASSET"


def bybit_liquidation_semantics(row: Mapping[str, Any]) -> tuple[str, str]:
    """V5 all-liquidation side is liquidated position side, not order aggression."""
    side = row.get("S")
    if side not in ("Buy", "Sell"):
        raise ValueError("liquidation side must be Buy/Sell")
    quantity = Decimal(str(row.get("v")))
    if not quantity.is_finite() or quantity <= 0:
        raise ValueError("liquidated base quantity invalid")
    return str(side), "LIQUIDATED_POSITION_SIDE_BASE_ASSET"


def capture_bybit_public_frame(
    collector: PublicCollectorV2, key: InstrumentKeyV2, raw: bytes, *,
    channel: str, received_at_ns: int,
) -> ForwardReceiptV2:
    """Archive exact trade/liquidation frame bytes; completeness remains unknown."""
    if key.venue != "BYBIT" or channel not in ("publicTrade", "allLiquidation"):
        raise ValueError("only declared Bybit linear public evidence channels are accepted")
    body = _obj(raw)
    if body.get("topic") != f"{channel}.{key.native_symbol}" or not isinstance(body.get("data"), list):
        raise ValueError("public frame topic/data mismatch")
    rows = body["data"]
    if not rows:
        raise ValueError("empty public evidence frame")
    for row in rows:
        if not isinstance(row, Mapping) or row.get("s") != key.native_symbol:
            raise ValueError("public event instrument mismatch")
        if channel == "publicTrade":
            bybit_trade_semantics(row)
        else:
            bybit_liquidation_semantics(row)
    now = collector.clock_ns()
    if now < received_at_ns:
        raise ValueError("ingestion cannot predate actual frame receipt")
    source_id = f"BYBIT_{channel}_{key.native_symbol}"
    collector.mark_incomplete_snapshot(source_id, at_ns=now,
        details="raw public frames archived; feed completeness and censoring unqualified")
    obs = RawObservationV2.build(instrument_revision=key.contract_revision, source_id=source_id,
        event_type=channel, received_at_ns=received_at_ns, ingested_at_ns=now, available_at_ns=now,
        translation_version="BYBIT_PUBLIC_FRAME_CAPTURE_V1", payload=raw,
        event_at_ns=max(_int(row.get("T"), "event T") for row in rows) * 1_000_000,
        sequence=hashlib.sha256(raw).hexdigest())
    result = collector.ingest(obs, raw_payload=raw)
    if result.append.status == AppendStatusV2.CONFLICT_QUARANTINED:
        return ForwardReceiptV2(obs.content_hash, obs.raw_payload_hash, False, "CONFLICT")
    collector.flush_archive()
    return ForwardReceiptV2(_indexed_observation_ref(collector, obs), obs.raw_payload_hash,
                            False, "COVERAGE_UNVERIFIED")


def capture_official_material(
    collector: PublicCollectorV2, key: InstrumentKeyV2, raw: bytes, *,
    source_id: str, publication_claim_ns: int | None, received_at_ns: int,
    revision_of: str | None = None,
) -> ForwardReceiptV2:
    """Preserve exact supplied official material; source identity is caller evidence."""
    nonblank(source_id, field="official source_id")
    if not raw:
        raise ValueError("official material body cannot be empty")
    now = collector.clock_ns()
    if now < received_at_ns:
        raise ValueError("ingestion cannot predate actual receipt")
    obs = RawObservationV2.build(instrument_revision=key.contract_revision, source_id=source_id,
        event_type="OFFICIAL_RAW_MATERIAL", received_at_ns=received_at_ns,
        ingested_at_ns=now, available_at_ns=now, translation_version="OFFICIAL_RAW_CAPTURE_V1",
        payload=raw, published_at_ns=publication_claim_ns, revision_of=revision_of,
        sequence=hashlib.sha256(raw).hexdigest())
    collector.mark_incomplete_snapshot(source_id, at_ns=now,
        details="official source publication/coverage unqualified; raw body archived")
    result = collector.ingest(obs, raw_payload=raw)
    if result.append.status == AppendStatusV2.CONFLICT_QUARANTINED:
        return ForwardReceiptV2(obs.content_hash, obs.raw_payload_hash, False, "CONFLICT")
    collector.flush_archive()
    return ForwardReceiptV2(_indexed_observation_ref(collector, obs), obs.raw_payload_hash,
                            False, "SOURCE_UNVERIFIED")
