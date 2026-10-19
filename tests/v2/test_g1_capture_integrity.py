"""G1 regression counterexamples; fixtures never certify observed market evidence."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

from atlas.v2.data.capture_receipts import CaptureReceiptBindingV1, CaptureReceiptJournalV1
from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.data.public_archive_extents import PublicArchiveSegmentWriterV1, read_extent
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.instruments import VenueV2
from atlas.v2.memory.repository import OpsRepository


def sealed(root, *, payload=b'{"fixture":1}', received=100, epoch=3):
    source = SimpleNamespace(venue=VenueV2.BYBIT, topics=("publicTrade.BTCUSDT",))
    capture = DurablePublicCaptureV1(source, clock_ns=lambda: 200, capture_payload_metrics=True)
    capture.configure_capture(root)
    capture._journal = CaptureReceiptJournalV1(
        root / "ops-public-capture", binding=CaptureReceiptBindingV1("standalone-capture-fixture", "0" * 64, "0" * 64)
    )
    frame = CapturedPublicFrameV2(
        VenueV2.BYBIT,
        "BYBIT_PUBLIC_WS_BTC_ETH_LINEAR_V1",
        "publicTrade.BTCUSDT",
        payload,
        hashlib.sha256(payload).hexdigest(),
        received,
        received,
        epoch,
    )
    writer = PublicArchiveSegmentWriterV1(root / "ops-public-extents")
    batch = capture._seal(writer, (frame,))
    return capture, batch, frame


def test_transport_adoption_is_atomic_even_without_outer_composition(tmp_path):
    _, batch, frame = sealed(tmp_path)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo._connection.execute("""CREATE TEMP TRIGGER reject_batch BEFORE INSERT ON artifact_index
            WHEN NEW.artifact_type='PublicStreamTransportBatchV2'
            BEGIN SELECT RAISE(ABORT, 'injected batch failure'); END""")
        with pytest.raises(sqlite3.IntegrityError):
            batch.adopt(repo)
        assert repo.get_artifact(batch.extent.artifact_ref) is None
        assert repo.get_artifact(batch.batch.artifact_ref) is None
        assert batch.read_unpublished(tmp_path) == (frame,)
        repo._connection.execute("DROP TRIGGER reject_batch")
        assert batch.adopt(repo) == (frame,)
        assert batch.adopt(repo) == (frame,)
        assert len(repo.artifact_entries("PublicStreamTransportBatchV2")) == 1
    for _ in range(3):
        with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
            assert (
                read_extent(reader, batch.extent.artifact_ref).to_pylist()[0]["raw_payload_bytes"]
                == frame.raw_payload_bytes
            )


def test_capture_payload_maximum_never_decreases_across_batches(tmp_path):
    capture, batch, frame = sealed(tmp_path, payload=b"x" * 4096)
    capture._record_captured_batch(batch, (frame,), started=0, record_archive_metrics=False)
    smaller = replace(frame, raw_payload_bytes=b"x", raw_payload_hash=hashlib.sha256(b"x").hexdigest())
    capture._record_captured_batch(batch, (smaller,), started=0, record_archive_metrics=False)
    assert capture._payload_size_max == 4096
    assert capture._payload_size_min == 1
    assert capture._payload_bytes_written == 4097


@pytest.mark.parametrize("fault", ["disk_full", "fsync", "commit"])
def test_failed_storage_does_not_publish_a_transport_batch(tmp_path, monkeypatch, fault):
    import errno

    from atlas.v2.data import capture_receipts, public_archive_extents

    if fault == "disk_full":
        monkeypatch.setattr(
            public_archive_extents.os, "fsync", lambda _: (_ for _ in ()).throw(OSError(errno.ENOSPC, "injected"))
        )
        with pytest.raises(OSError):
            sealed(tmp_path)
        with OpsRepository(tmp_path / "ops.sqlite", read_only=False) as repo:
            assert not repo.artifact_entries("PublicStreamTransportBatchV2")
    elif fault == "fsync":
        capture, batch, frame = sealed(tmp_path)
        monkeypatch.setattr(
            capture_receipts.os, "fsync", lambda _: (_ for _ in ()).throw(OSError("injected sync failure"))
        )
        with pytest.raises(OSError):
            capture._journal.append({"authority": "ZERO", "fixture": 2})
        with pytest.raises(RuntimeError, match="terminal"):
            capture._journal.append({"authority": "ZERO", "fixture": 3})
        assert batch.read_unpublished(tmp_path) == (frame,)
    else:
        _, batch, frame = sealed(tmp_path)
        with OpsRepository(tmp_path / "ops.sqlite") as repo:
            connection = repo._connection

            class FailedCommit:
                def __getattr__(self, name):
                    return getattr(connection, name)

                def commit(self):
                    raise sqlite3.OperationalError("injected WAL commit failure")

            monkeypatch.setattr(repo, "_connection", FailedCommit())
            with pytest.raises(sqlite3.OperationalError):
                batch.adopt(repo)
            assert repo.get_artifact(batch.extent.artifact_ref) is None
            assert repo.get_artifact(batch.batch.artifact_ref) is None
            assert not connection.in_transaction
            assert repo.persistence_metrics()["transaction_failed"]
            assert batch.read_unpublished(tmp_path) == (frame,)
