"""Forward-only depth capture and documented side semantics."""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2.data.binance import BinanceUsdMPublicReaderV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.forward import (
    BinanceDepthCaptureV2,
    bybit_liquidation_semantics,
    bybit_trade_semantics,
    capture_bybit_public_frame,
    capture_official_material,
)
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.public_http import PublicHttpClientV2, PublicHttpResponseV2, PublicVenueV2
from atlas.v2.instruments import InstrumentRegistryV2, ProductContractV2, TradingStatusV2, VenueV2
from atlas.v2.memory.repository import OpsRepository

from .test_session014_core import KEY


def depth_frame(first, last, previous):
    return json.dumps({"e": "depthUpdate", "s": "BTCUSDT", "E": 1000,
        "U": first, "u": last, "pu": previous, "b": [["100", "1"]], "a": [["101", "2"]]},
        separators=(",", ":")).encode()


def setup(tmp_path):
    key = replace(KEY, venue=VenueV2.BINANCE)
    registry = InstrumentRegistryV2()
    registry.register(ProductContractV2(key, 0, 0, 0, Decimal("1"), Decimal("0.1"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, "a" * 64))
    time = [2_000_000_000]
    repo = OpsRepository(tmp_path / "ops.sqlite")
    archive = ParquetObservationArchiveV2(tmp_path / "archive")
    collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: time[0], archive=archive)
    return key, registry, repo, archive, collector, time


def test_depth_capture_gap_restart_and_exact_bytes(tmp_path):
    key, registry, repo, archive, collector, clock = setup(tmp_path)
    cap = BinanceDepthCaptureV2(collector, key)
    snap_raw = b'{"lastUpdateId":10,"bids":[["100","1"]],"asks":[["101","2"]]}'
    snap = PublicHttpResponseV2(PublicVenueV2.BINANCE, "/fapi/v1/depth", json.loads(snap_raw), snap_raw, clock[0] - 1, 200)
    first = cap.snapshot(snap)
    assert first.raw_payload_hash == hashlib.sha256(snap_raw).hexdigest()
    assert not first.usable and cap.source_state() == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
    with pytest.raises(ValueError):
        cap.snapshot(replace(snap, path="/fapi/v1/klines"))
    with pytest.raises(ValueError):
        cap.delta(b'{"e":"kline","s":"BTCUSDT","E":1000}', received_at_ns=clock[0] - 1)
    clock[0] += 10
    frame = depth_frame(10, 11, 9)
    one = cap.delta(frame, received_at_ns=clock[0] - 1)
    assert one.usable and cap.source_state() == PublicSourceStateV2.HEALTHY_CURRENT
    clock[0] += 2
    repeated = cap.delta(frame, received_at_ns=clock[0] - 1)
    assert repeated.observation_ref == one.observation_ref and repeated.reason == "DUPLICATE"
    assert len(repo.artifact_entries("ForwardDepthCursorV2")) == 2
    clock[0] += 10
    assert not cap.delta(depth_frame(13, 13, 12), received_at_ns=clock[0] - 2).usable
    assert cap.source_state() == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
    clock[0] += 10
    assert not cap.delta(depth_frame(14, 14, 13), received_at_ns=clock[0] - 2).usable
    assert cap.source_state() != PublicSourceStateV2.HEALTHY_CURRENT
    repo.close()
    clock[0] += 10
    with OpsRepository(tmp_path / "ops.sqlite") as reopened:
        restarted = PublicCollectorV2(repository=reopened, registry=registry, clock_ns=lambda: clock[0], archive=archive)
        cold = BinanceDepthCaptureV2(restarted, key)
        assert not cold.valid and cold.snapshot_update_id is None
        assert reopened.artifact_entries("ForwardDepthCursorV2")
        assert not cold.delta(depth_frame(15, 15, 14), received_at_ns=clock[0] - 2).usable
        assert cold.source_state() != PublicSourceStateV2.HEALTHY_CURRENT
        clock[0] += 10
        new_snap = PublicHttpResponseV2(PublicVenueV2.BINANCE, "/fapi/v1/depth",
            json.loads(snap_raw.replace(b"10", b"20", 1)), snap_raw.replace(b"10", b"20", 1), clock[0] - 1, 200)
        cold.snapshot(new_snap)
        clock[0] += 10
        assert cold.delta(depth_frame(20, 21, 19), received_at_ns=clock[0] - 1).usable


def test_public_depth_allowlist_and_semantics():
    calls = []
    def getter(url, timeout):
        calls.append(url)
        return 200, b'{"lastUpdateId":10}'
    reader = BinanceUsdMPublicReaderV2(PublicHttpClientV2(PublicVenueV2.BINANCE,
        clock_ns=lambda: 10, getter=getter))
    assert reader.depth_snapshot("BTCUSDT", limit=100).path == "/fapi/v1/depth"
    assert calls == ["https://fapi.binance.com/fapi/v1/depth?limit=100&symbol=BTCUSDT"]
    with pytest.raises(ValueError):
        reader.depth_snapshot("BTCUSDT", limit=7)
    assert bybit_trade_semantics({"i": "trade-1", "S": "Buy", "v": "0.5"}) == ("trade-1", "Buy", "BASE_ASSET")
    assert bybit_liquidation_semantics({"S": "Buy", "v": "0.5"}) == ("Buy", "LIQUIDATED_POSITION_SIDE_BASE_ASSET")
    with pytest.raises(ValueError):
        bybit_trade_semantics({"i": "trade-1", "S": "Unknown", "v": "0.5"})


def test_depth_conflicting_duplicate_quarantines_and_blocks_health(tmp_path):
    key, _, repo, _, collector, clock = setup(tmp_path)
    cap = BinanceDepthCaptureV2(collector, key)
    raw = b'{"lastUpdateId":10,"bids":[],"asks":[]}'
    cap.snapshot(PublicHttpResponseV2(PublicVenueV2.BINANCE, "/fapi/v1/depth",
        json.loads(raw), raw, clock[0] - 1, 200))
    clock[0] += 10
    first = depth_frame(10, 11, 9)
    assert cap.delta(first, received_at_ns=clock[0] - 1).usable
    clock[0] += 10
    changed = first.replace(b'"100"', b'"99"')
    conflict = cap.delta(changed, received_at_ns=clock[0] - 1)
    assert conflict.reason == "CONFLICT" and not cap.valid
    assert cap.source_state() == PublicSourceStateV2.SEQUENCE_GAP_CONFLICT
    assert repo.artifact_entries("PublicDuplicateConflictV2")
    repo.close()


def test_raw_trade_liquidation_and_official_material_capture(tmp_path):
    _, registry, repo, _, collector, clock = setup(tmp_path)
    registry.register(ProductContractV2(KEY, 0, 0, 0, Decimal("1"), Decimal("0.1"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, "b" * 64))
    trade = b'{"topic":"publicTrade.BTCUSDT","data":[{"s":"BTCUSDT","T":1000,"i":"id-1","S":"Buy","v":"0.5"}]}'
    receipt = capture_bybit_public_frame(collector, KEY, trade, channel="publicTrade", received_at_ns=clock[0] - 1)
    assert receipt.raw_payload_hash == hashlib.sha256(trade).hexdigest() and not receipt.usable
    assert capture_bybit_public_frame(collector, KEY, trade, channel="publicTrade",
        received_at_ns=clock[0] - 1).observation_ref == receipt.observation_ref
    liquidation = b'{"topic":"allLiquidation.BTCUSDT","data":[{"s":"BTCUSDT","T":1001,"S":"Sell","v":"2","p":"99"}]}'
    assert capture_bybit_public_frame(collector, KEY, liquidation, channel="allLiquidation",
        received_at_ns=clock[0] - 1).reason == "COVERAGE_UNVERIFIED"
    news = b'{"announcement":"maintenance"}'
    material = capture_official_material(collector, KEY, news, source_id="OFFICIAL_BYBIT_ANNOUNCEMENT",
        publication_claim_ns=1000, received_at_ns=clock[0] - 1)
    assert material.raw_payload_hash == hashlib.sha256(news).hexdigest()
    assert repo.artifact_entries("PublicObservationIndexV2")
    repo.close()
