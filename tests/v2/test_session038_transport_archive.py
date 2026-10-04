from __future__ import annotations

import hashlib
import json

import pyarrow.parquet as pq

from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.data.public_transport_archive import archive_transport_batch
from atlas.v2.instruments import VenueV2
from atlas.v2.memory.repository import OpsRepository


def test_transport_archive_retains_exact_unbound_frame_bytes(tmp_path) -> None:
    raw = json.dumps({"topic": "publicTrade.BTCUSDT", "data": []}, separators=(",", ":")).encode()
    frame = CapturedPublicFrameV2(
        VenueV2.BYBIT, "BYBIT_PUBLIC_WS", "publicTrade.BTCUSDT", raw,
        hashlib.sha256(raw).hexdigest(), 100, 101, 1,
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        ref = archive_transport_batch(repository, (frame,), clock_ns=lambda: 102, floor_ns=100)
        assert ref is not None
        entry = repository.get_artifact(ref)
        assert entry is not None
        path = tmp_path / "ops-public-transport" / str(entry.metadata["batch"]["archive_path_name"])
        assert pq.read_table(path).to_pylist()[0]["raw_payload_bytes"] == raw
