from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
from atlas.v2.data.public_archive_extents import read_extent_entry
from atlas.v2.data.public_evidence_preparation import (
    MAX_PREPARATION_FRAMES_V2,
    MAX_PREPARATION_TRADE_ROWS_V2,
    PublicEvidencePreparationError,
    PublicEvidencePreparationRequestV2,
    PublicEvidencePreparationTimeout,
    PublicEvidencePreparationWorkerV2,
    _execute_request,
    _parse_input,
    read_preparation_input,
    read_preparation_result,
    write_preparation_blob,
    write_preparation_input,
)
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductTypeV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository, PublicAdoptionCursorV2
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2


def _slice(generation: int, *, frame: int = 0, start: int = 0, end: int = 64,
           next_frame: int = 0, next_trade: int = 64):
    output = {"records": [generation], "result_path": f"private/prepared-{generation}.json"}
    cursor = PublicAdoptionCursorV2(
        run_id="run-s41", descriptor_hash="a" * 64, extent_hash="b" * 64,
        batch_hash="c" * 64, plan_hash="d" * 64, product_hashes=("e" * 64,),
        preparation_version="PUBLIC_EVIDENCE_PREPARATION_V2", frame_ordinal=frame,
        trade_ordinal_start=start, trade_ordinal_end=end, next_frame_ordinal=next_frame,
        next_trade_ordinal=next_trade, logical_started_at_ns=100 + generation,
        logical_finished_at_ns=200 + generation, staged_output_hash=sha256_json(output),
        cursor_generation=generation,
    )
    return cursor, {"cursor": cursor.to_dict(), "output": output}


def test_rebuildable_worker_inputs_are_atomic_without_duplicate_fsync(tmp_path, monkeypatch):
    import atlas.v2.data.public_evidence_preparation as preparation

    def unexpected_fsync(_descriptor):
        raise AssertionError("rebuildable worker input must not fsync a second copy")

    monkeypatch.setattr(preparation.os, "fsync", unexpected_fsync)
    path = tmp_path / "private" / "request.json"
    expected = {"frames": [{"ordinal": 0, "payload": "from-sealed-transport"}]}
    digest, size = write_preparation_input(path, expected)
    assert size == path.stat().st_size
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert json.loads(path.read_text(encoding="utf-8")) == expected

    blob_path = tmp_path / "private" / "derived.arrow"
    blob_digest, blob_size = write_preparation_blob(blob_path, b"derived-from-sealed-transport")
    assert blob_size == len(blob_path.read_bytes())
    assert hashlib.sha256(blob_path.read_bytes()).hexdigest() == blob_digest


def test_private_adoption_stage_cursor_are_atomic_and_not_public(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        cursor, payload = _slice(1)
        with pytest.raises(RuntimeError, match="abort staged slice"), repository.atomic_composition():
            repository.stage_public_adoption_slice_v2(cursor, payload)
            raise RuntimeError("abort staged slice")
        assert repository.public_adoption_stages_v2("run-s41", "a" * 64) == (None, ())

        repository.stage_public_adoption_slice_v2(cursor, payload)
        stored_cursor, stages = repository.public_adoption_stages_v2("run-s41", "a" * 64)
        assert stored_cursor == cursor
        assert len(stages) == 1 and stages[0].cursor == cursor
        assert stages[0].payload["output"]["records"] == (1,)
        assert stages[0].payload["output"]["result_path"] == "private/prepared-1.json"
        assert repository.get_artifact(cursor.descriptor_hash) is None
        assert repository._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0] == 0


def test_private_adoption_cursor_requires_exact_binding_and_contiguous_ordinals(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        first, first_payload = _slice(1)
        repository.stage_public_adoption_slice_v2(first, first_payload)
        next_output = {"records": [2], "result_path": "private/prepared-2.json"}
        bad_binding = PublicAdoptionCursorV2(
            run_id=first.run_id, descriptor_hash=first.descriptor_hash, extent_hash=first.extent_hash,
            batch_hash=first.batch_hash, plan_hash="f" * 64, product_hashes=first.product_hashes,
            preparation_version=first.preparation_version, frame_ordinal=0, trade_ordinal_start=64,
            trade_ordinal_end=128, next_frame_ordinal=0, next_trade_ordinal=128,
            logical_started_at_ns=300, logical_finished_at_ns=400,
            staged_output_hash=sha256_json(next_output), cursor_generation=2,
        )
        with pytest.raises(ValueError, match="binding changed"):
            repository.stage_public_adoption_slice_v2(
                bad_binding, {"cursor": bad_binding.to_dict(), "output": next_output})

        gap, gap_payload = _slice(2, frame=0, start=65, end=129, next_frame=0, next_trade=129)
        with pytest.raises(ValueError, match="not contiguous"):
            repository.stage_public_adoption_slice_v2(gap, gap_payload)


def test_private_adoption_stage_detects_payload_corruption_and_enforces_chunk_bound(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        cursor, payload = _slice(1)
        repository.stage_public_adoption_slice_v2(cursor, payload)
        with repository._transaction() as connection:
            connection.execute(
                "UPDATE public_evidence_stage_v2 SET payload_json=? WHERE run_id=? AND descriptor_hash=?",
                (json.dumps({"corrupt": True}), cursor.run_id, cursor.descriptor_hash),
            )
        with pytest.raises(RuntimeError, match="slice hash"):
            repository.public_adoption_stages_v2(cursor.run_id, cursor.descriptor_hash)

        huge_output = {"body": "x" * (4 * 1024 * 1024)}
        huge_cursor = PublicAdoptionCursorV2(
            run_id="large", descriptor_hash="1" * 64, extent_hash="2" * 64,
            batch_hash="3" * 64, plan_hash="4" * 64, product_hashes=("5" * 64,),
            preparation_version="PUBLIC_EVIDENCE_PREPARATION_V2", frame_ordinal=0,
            trade_ordinal_start=0, trade_ordinal_end=1, next_frame_ordinal=1, next_trade_ordinal=0,
            logical_started_at_ns=1, logical_finished_at_ns=2,
            staged_output_hash=sha256_json(huge_output), cursor_generation=1,
        )
        with pytest.raises(ValueError, match="4 MiB"):
            repository.stage_public_adoption_slice_v2(
                huge_cursor, {"cursor": huge_cursor.to_dict(), "output": huge_output})


def test_writable_open_migrates_schema_v1_to_additive_v2(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository, repository._transaction() as connection:
        connection.execute("DROP TABLE public_evidence_stage_v2")
        connection.execute("DROP TABLE public_adoption_cursor_v2")
        connection.execute("DELETE FROM schema_meta WHERE namespace='atlas-ops'")
        connection.execute("INSERT INTO schema_meta(namespace,schema_version) VALUES('atlas-ops',1)")
    with OpsRepository(path) as migrated:
        assert migrated.schema_version == 2
        assert migrated._connection.execute(
            "SELECT schema_version FROM schema_meta WHERE namespace='atlas-ops'"
        ).fetchone()[0] == 2
        assert migrated._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='public_adoption_cursor_v2'"
        ).fetchone() is not None


def _parse_request(root, *, start: int, end: int, rows: int = 96):
    revision = sha256_json({"revision": "worker-test"})
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          "BTCUSDT", "BTC", "USDT", "USDT", revision)
    trades = [{"i": f"trade-{index}", "S": "Buy", "p": "100.01", "v": "0.01", "T": index + 1}
              for index in range(rows)]
    raw = json.dumps({"topic": "publicTrade.BTCUSDT", "data": trades}, separators=(",", ":")).encode()
    frame = {"venue": "BYBIT", "source_id": "BYBIT_PUBLIC_WS_BROAD_V2",
        "channel": "publicTrade.BTCUSDT", "raw_payload_b64": base64.b64encode(raw).decode(),
        "raw_payload_hash": hashlib.sha256(raw).hexdigest(), "received_at_ns": 1_000_000,
        "available_at_ns": 1_000_000, "connection_epoch": 1}
    input_path = root / "private" / f"input-{start}.json"
    manifest = {"frames": [{
        "frame_ordinal": 0, "frame": frame, "instrument": key.to_dict(),
        "processed_at_ns": 1_000_001, "source_health": "UNKNOWN",
        "source_health_ref": None, "trade_ordinal_start": start,
    }]}
    input_hash, input_bytes = write_preparation_input(input_path, manifest)
    assert input_bytes <= 4 * 1024 * 1024
    return PublicEvidencePreparationRequestV2(
        "PARSE", f"job-{start}", "worker-test-run", str(root), "a" * 64, input_hash,
        str(input_path), str(root / "private" / f"result-{start}.json"), "d" * 64,
        key.content_hash, 0, start, end, processed_at_ns=1_000_001,
    ), trades


def _bybit_manifest_frame(key, *, ordinal: int, row_count: int, trade_start: int = 0):
    trades = [{"i": f"{ordinal}-{index}", "S": "Buy", "p": "100.01", "v": "0.01", "T": index + 1}
              for index in range(row_count)]
    raw = json.dumps({"topic": f"publicTrade.{key.native_symbol}", "data": trades},
                     separators=(",", ":")).encode()
    return {
        "frame_ordinal": ordinal,
        "frame": {"venue": "BYBIT", "source_id": "BYBIT_PUBLIC_WS_BROAD_V2",
                  "channel": f"publicTrade.{key.native_symbol}",
                  "raw_payload_b64": base64.b64encode(raw).decode(),
                  "raw_payload_hash": hashlib.sha256(raw).hexdigest(),
                  "received_at_ns": 1_000_000, "available_at_ns": 1_000_000,
                  "connection_epoch": 1},
        "instrument": key.to_dict(), "processed_at_ns": 1_000_001,
        "source_health": "UNKNOWN", "source_health_ref": None,
        "trade_ordinal_start": trade_start,
    }, trades


def _parse_manifest_request(root, entries, *, key):
    input_path = root / "private" / "manifest.json"
    input_hash, input_bytes = write_preparation_input(input_path, {"frames": entries})
    assert input_bytes <= 4 * 1024 * 1024
    first_frame = entries[0]["frame"]
    first_bybit_trade = (first_frame["venue"] == "BYBIT"
                         and first_frame["channel"].startswith("publicTrade."))
    trade_start = entries[0]["trade_ordinal_start"]
    return PublicEvidencePreparationRequestV2(
        "PARSE", "manifest-job", "worker-test-run", str(root), "a" * 64, input_hash,
        str(input_path), str(root / "private" / "manifest-result.json"), "d" * 64,
        key.content_hash, 0, trade_start,
        trade_start + MAX_PREPARATION_TRADE_ROWS_V2 if first_bybit_trade else 1,
        processed_at_ns=entries[0]["processed_at_ns"],
    )


def test_spawn_parse_worker_slices_trade_rows_and_binds_file_result(tmp_path):
    first, trades = _parse_request(tmp_path, start=0, end=64)
    worker = PublicEvidencePreparationWorkerV2()
    try:
        start = 0
        payload_hashes = []
        results = []
        while start < len(trades):
            request, _ = (first, trades) if start == 0 else _parse_request(
                tmp_path, start=start, end=start + 64,
            )
            result = worker.wait(request)
            output = read_preparation_result(request, result)
            frame_result = output["frame_results"][0]
            assert len(frame_result["events"]) <= 64
            assert len(frame_result["trade_payload_hashes"]) == len(frame_result["events"])
            assert frame_result["trade_total"] == len(trades)
            assert frame_result["trade_ordinal_start"] == start
            assert start < frame_result["trade_ordinal_end"] <= min(start + 64, len(trades))
            payload_hashes.extend(frame_result["trade_payload_hashes"])
            results.append(result)
            start = frame_result["trade_ordinal_end"]
        assert payload_hashes == [sha256_json(row) for row in trades]
        assert max(result.output_bytes for result in results) <= 4 * 1024 * 1024
        assert max(result.result_records for result in results) <= 64
        assert max(result.execution_duration_ns for result in results) < 1_500_000_000
    finally:
        worker.close()


def test_many_row_frame_keeps_slice_identity_and_worker_job_bounded(tmp_path):
    request, trades = _parse_request(tmp_path, start=0, end=64, rows=8_000)
    worker = PublicEvidencePreparationWorkerV2()
    try:
        result = worker.wait(request)
        output = read_preparation_result(request, result)
        frame_result = output["frame_results"][0]
        end = frame_result["trade_ordinal_end"]
        assert 0 < end <= 64
        assert len(frame_result["trade_payload_hashes"]) == len(frame_result["events"]) == end
        assert frame_result["trade_payload_hashes"] == [sha256_json(row) for row in trades[:end]]
        assert BroadPublicRuntimeV2._next_manifest_cursor((frame_result,)) == (0, end)
        assert result.execution_duration_ns < 1_500_000_000
    finally:
        worker.close()


def test_parse_manifest_processes_sixteen_frames_with_sixty_four_total_rows(tmp_path, monkeypatch):
    revision = sha256_json({"revision": "manifest-boundary"})
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          "BTCUSDT", "BTC", "USDT", "USDT", revision)
    entries_and_rows = [_bybit_manifest_frame(key, ordinal=ordinal, row_count=4)
                        for ordinal in range(MAX_PREPARATION_FRAMES_V2)]
    entries = [entry for entry, _rows in entries_and_rows]
    request = _parse_manifest_request(tmp_path, entries, key=key)
    # Hold the cooperative clock fixed so this boundary test is independent of
    # scheduler load while still exercising the exact worker slice function.
    monkeypatch.setattr("atlas.v2.data.public_evidence_preparation.time.monotonic_ns", lambda: 10)
    output = _parse_input(request, read_preparation_input(request), {})
    assert len(output["frame_results"]) == MAX_PREPARATION_FRAMES_V2
    assert sum(len(result["trade_payload_hashes"]) for result in output["frame_results"]) == 64
    assert sum(len(result["events"]) for result in output["frame_results"]) == 64
    assert [result["frame_ordinal"] for result in output["frame_results"]] == list(range(16))
    assert all(len(result["trade_payload_hashes"]) == 4 for result in output["frame_results"])

    class _Plan:
        @staticmethod
        def key_for_frame(_frame):
            return key

    runtime = BroadPublicRuntimeV2.__new__(BroadPublicRuntimeV2)
    runtime.plan = _Plan()
    runtime._products = {key: object()}
    frames = tuple(CapturedPublicFrameV2(
        VenueV2.BYBIT, entry["frame"]["source_id"], entry["frame"]["channel"],
        base64.b64decode(entry["frame"]["raw_payload_b64"]), entry["frame"]["raw_payload_hash"],
        entry["frame"]["received_at_ns"], entry["frame"]["available_at_ns"],
        entry["frame"]["connection_epoch"],
    ) for entry in entries)
    body = {"request_hash": request.request_hash, **output}
    sealed = SimpleNamespace(batch=SimpleNamespace(artifact_ref=request.descriptor_hash))
    kwargs = {"sealed": sealed, "plan_hash": request.plan_hash, "frames": frames,
              "manifest": read_preparation_input(request)}
    validated = runtime._validate_prepared_parse_body(request, body, **kwargs)
    decoded = runtime._decode_and_bind_manifest_events(validated, frames)
    assert len(decoded) == 16
    assert sum(len(events) for _ordinal, events, _hashes, _processed in decoded) == 64
    for changed_results in (
        [*body["frame_results"][:1],
         {**body["frame_results"][1], "frame_ordinal": 2}, *body["frame_results"][2:]],
        [*body["frame_results"][:1],
         {**body["frame_results"][1], "frame_ordinal": 0}, *body["frame_results"][2:]],
        list(reversed(body["frame_results"])),
    ):
        with pytest.raises(ValueError):
            runtime._validate_prepared_parse_body(
                request, {**body, "frame_results": changed_results}, **kwargs,
            )


def test_parse_manifest_trade_row_cap_includes_binance_aggtrade(tmp_path, monkeypatch):
    bybit_revision = sha256_json({"revision": "bybit-manifest"})
    bybit_key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET,
                                ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT",
                                bybit_revision)
    bybit, bybit_rows = _bybit_manifest_frame(bybit_key, ordinal=0, row_count=63)
    binance_revision = sha256_json({"revision": "binance-manifest"})
    binance_key = InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET,
                                  ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT",
                                  binance_revision)
    agg_row = {"a": 77, "p": "100.01", "q": "0.01", "T": 1, "m": False}
    raw = json.dumps({"stream": "btcusdt@aggTrade", "data": agg_row}, separators=(",", ":")).encode()
    binance = {
        "frame_ordinal": 1,
        "frame": {"venue": "BINANCE", "source_id": "BINANCE_PUBLIC_WS_BROAD_V2",
                  "channel": "btcusdt@aggTrade", "raw_payload_b64": base64.b64encode(raw).decode(),
                  "raw_payload_hash": hashlib.sha256(raw).hexdigest(), "received_at_ns": 1_000_000,
                  "available_at_ns": 1_000_000, "connection_epoch": 1},
        "instrument": binance_key.to_dict(), "processed_at_ns": 1_000_001,
        "source_health": "UNKNOWN", "source_health_ref": None, "trade_ordinal_start": 0,
    }
    extra_binance = {**binance, "frame_ordinal": 2}
    request = _parse_manifest_request(tmp_path, [bybit, binance, extra_binance], key=bybit_key)
    monkeypatch.setattr("atlas.v2.data.public_evidence_preparation.time.monotonic_ns", lambda: 10)
    output = _parse_input(request, read_preparation_input(request), {})
    results = output["frame_results"]
    assert [result["frame_ordinal"] for result in results] == [0, 1]
    assert sum(len(result["trade_payload_hashes"]) for result in results) == 64
    assert results[0]["trade_payload_hashes"] == [sha256_json(row) for row in bybit_rows]
    assert results[1]["trade_payload_hashes"] == [sha256_json(agg_row)]


def test_parse_result_validation_accepts_binance_first_frame(tmp_path, monkeypatch):
    revision = sha256_json({"revision": "binance-first"})
    key = InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET,
                          ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", revision)
    trade = {"a": 77, "p": "100.01", "q": "0.01", "T": 1, "m": False}
    raw = json.dumps({"stream": "btcusdt@aggTrade", "data": trade}, separators=(",", ":")).encode()
    entry = {
        "frame_ordinal": 0,
        "frame": {"venue": "BINANCE", "source_id": "BINANCE_PUBLIC_WS_BROAD_V2",
                  "channel": "btcusdt@aggTrade", "raw_payload_b64": base64.b64encode(raw).decode(),
                  "raw_payload_hash": hashlib.sha256(raw).hexdigest(), "received_at_ns": 1_000_000,
                  "available_at_ns": 1_000_000, "connection_epoch": 1},
        "instrument": key.to_dict(), "processed_at_ns": 1_000_001,
        "source_health": "UNKNOWN", "source_health_ref": None, "trade_ordinal_start": 0,
    }
    request = _parse_manifest_request(tmp_path, [entry], key=key)
    monkeypatch.setattr("atlas.v2.data.public_evidence_preparation.time.monotonic_ns", lambda: 10)
    output = _parse_input(request, read_preparation_input(request), {})
    body = {"request_hash": request.request_hash, **output}
    frame = CapturedPublicFrameV2(
        VenueV2.BINANCE, entry["frame"]["source_id"], entry["frame"]["channel"], raw,
        entry["frame"]["raw_payload_hash"], entry["frame"]["received_at_ns"],
        entry["frame"]["available_at_ns"], entry["frame"]["connection_epoch"],
    )

    class _Plan:
        @staticmethod
        def key_for_frame(_frame):
            return key

    runtime = BroadPublicRuntimeV2.__new__(BroadPublicRuntimeV2)
    runtime.plan = _Plan()
    runtime._products = {key: object()}
    sealed = SimpleNamespace(batch=SimpleNamespace(artifact_ref=request.descriptor_hash))
    validated = runtime._validate_prepared_parse_body(
        request, body, sealed=sealed, plan_hash=request.plan_hash, frames=(frame,),
        manifest=read_preparation_input(request),
    )
    assert len(validated) == 1
    assert validated[0]["trade_payload_hashes"] == [sha256_json(trade)]


def test_parse_result_validation_rejects_corrupt_bindings_and_zero_progress(tmp_path):
    request, trades = _parse_request(tmp_path, start=0, end=64)
    manifest = read_preparation_input(request)
    worker = PublicEvidencePreparationWorkerV2()
    try:
        completion = worker.wait(request)
        valid_body = read_preparation_result(request, completion)
    finally:
        worker.close()
    frame_input = manifest["frames"][0]
    raw = base64.b64decode(frame_input["frame"]["raw_payload_b64"])
    frame = CapturedPublicFrameV2(
        VenueV2.BYBIT, frame_input["frame"]["source_id"], frame_input["frame"]["channel"],
        raw, frame_input["frame"]["raw_payload_hash"], frame_input["frame"]["received_at_ns"],
        frame_input["frame"]["available_at_ns"], frame_input["frame"]["connection_epoch"],
    )
    key = InstrumentKeyV2.from_dict(frame_input["instrument"])

    class _Plan:
        @staticmethod
        def key_for_frame(_frame):
            return key

    runtime = BroadPublicRuntimeV2.__new__(BroadPublicRuntimeV2)
    runtime.plan = _Plan()
    runtime._products = {key: object()}
    sealed = SimpleNamespace(batch=SimpleNamespace(artifact_ref=request.descriptor_hash))
    kwargs = {"sealed": sealed, "plan_hash": request.plan_hash,
              "frames": (frame,), "manifest": manifest}
    validated = runtime._validate_prepared_parse_body(request, valid_body, **kwargs)
    assert len(validated) == 1
    assert validated[0]["trade_payload_hashes"] == [sha256_json(row) for row in trades[:64]]

    bad_job = {**valid_body, "job_id": "another-job"}
    duplicate = {**valid_body, "frame_results": [valid_body["frame_results"][0]] * 2}
    out_of_order_result = {**valid_body["frame_results"][0], "frame_ordinal": 1}
    out_of_order = {**valid_body, "frame_results": [out_of_order_result]}
    for invalid in (bad_job, duplicate, out_of_order):
        with pytest.raises(ValueError):
            runtime._validate_prepared_parse_body(request, invalid, **kwargs)

    empty_request, _ = _parse_request(tmp_path, start=0, end=64, rows=0)
    empty_manifest = read_preparation_input(empty_request)
    empty_frame_input = empty_manifest["frames"][0]
    empty_raw = base64.b64decode(empty_frame_input["frame"]["raw_payload_b64"])
    empty_frame = CapturedPublicFrameV2(
        VenueV2.BYBIT, empty_frame_input["frame"]["source_id"], empty_frame_input["frame"]["channel"],
        empty_raw, empty_frame_input["frame"]["raw_payload_hash"],
        empty_frame_input["frame"]["received_at_ns"], empty_frame_input["frame"]["available_at_ns"],
        empty_frame_input["frame"]["connection_epoch"],
    )
    empty_worker = PublicEvidencePreparationWorkerV2()
    try:
        empty_completion = empty_worker.wait(empty_request)
        empty_body = read_preparation_result(empty_request, empty_completion)
    finally:
        empty_worker.close()
    empty = runtime._validate_prepared_parse_body(
        empty_request, empty_body, sealed=sealed, plan_hash=empty_request.plan_hash,
        frames=(empty_frame,), manifest=empty_manifest,
    )
    assert empty[0]["trade_ordinal_end"] == empty[0]["trade_total"] == 0

    noncanonical = PublicEvidencePreparationRequestV2.from_dict({
        **request.to_dict(), "trade_ordinal_end": 63,
    })
    with pytest.raises(ValueError, match="BROAD_PREPARATION_RESULT_BINDING_INVALID"):
        runtime._validate_prepared_parse_body(noncanonical, valid_body, **kwargs)


def test_spawn_parse_worker_fails_closed_on_crash_timeout_and_corrupt_result(tmp_path):
    crashed_request, _ = _parse_request(tmp_path, start=0, end=64)
    crashed = PublicEvidencePreparationWorkerV2()
    try:
        crashed.submit(crashed_request)
        crashed.terminate_for_test()
        with pytest.raises(PublicEvidencePreparationError, match="exited|pipe closed"):
            crashed.poll()
    finally:
        crashed.close()

    timeout_request, _ = _parse_request(tmp_path, start=0, end=64)
    timed = PublicEvidencePreparationWorkerV2()
    try:
        with pytest.raises(PublicEvidencePreparationTimeout):
            timed.wait(timeout_request, timeout_s=0.000001)
    finally:
        timed.close()

    corrupt_request, _ = _parse_request(tmp_path, start=0, end=64)
    corrupt = PublicEvidencePreparationWorkerV2()
    try:
        completion = corrupt.wait(corrupt_request)
        Path(completion.output_path).write_text('{"request_hash":"wrong"}')
        with pytest.raises(ValueError, match="bytes or hash mismatch"):
            read_preparation_result(corrupt_request, completion)
    finally:
        corrupt.close()


def test_sealed_archive_stays_private_when_publication_transaction_rolls_back(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from atlas.v2.memory.repository import ArtifactIndexEntryV2

    run_root = tmp_path / "run-archive-rollback"
    run_root.mkdir()
    key = InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT", "BTC", "USDT", "USDT", sha256_json({"revision": "archive-test"}),
    )
    raw_payload = b'{"topic":"publicTrade.BTCUSDT","data":[]}'
    frame = L2RawFrameV2(
        key, "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT", "TRADE_FRAME_TEST",
        raw_payload, hashlib.sha256(raw_payload).hexdigest(), 1, 2, 3,
        None, None, None, "BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR", "HEALTHY_CURRENT",
        "ACTUAL_SYSTEM",
    )
    archive = L2FrameArchiveV2(run_root.parent / "ops-l2-frames", None, compact_live=True,
                               clock_ns=lambda: 3)
    batch = archive.prepare_chunks(((frame,),))
    chunk = batch.chunks[0]
    stream = pa.BufferOutputStream()
    with pa.ipc.new_stream(stream, chunk.table.schema) as writer:
        writer.write_table(chunk.table)
    input_path = run_root / ".public-evidence-preparation" / "seal.arrow"
    input_hash, _ = write_preparation_blob(input_path, stream.getvalue().to_pybytes())
    output_path = input_path.with_name("seal-result.json")
    request = PublicEvidencePreparationRequestV2(
        "SEAL", "seal-rollback-job", run_root.name, str(run_root), "f" * 64, input_hash,
        str(input_path), str(output_path), "d" * 64, key.content_hash, 0, 0, 0,
        namespace="ops-l2-frames", chunk_id=chunk.chunk_id, floor_ns=3, clock_at_ns=4,
    )
    db_path = run_root / "ops.sqlite"
    result = _execute_request(request, None, run_root / "ops-public-extents", {})
    result.pop("_writer")
    seal_metrics = result["seal_metrics"]
    assert all(type(seal_metrics[name]) is int and seal_metrics[name] >= 0 for name in (
        "arrow_encode_ns", "compression_ns", "write_ns", "fsync_ns", "rename_ns",
        "directory_sync_ns",
    ))
    extent = result["extent"]
    entry = ArtifactIndexEntryV2(
        extent["artifact_ref"], extent["artifact_type"], extent["content_hash"],
        extent["created_at_ns"], extent["available_at_ns"], extent["metadata"],
    )

    with OpsRepository(db_path) as repository:
        with pytest.raises(RuntimeError, match="abort public archive publication"), repository.atomic_composition():
            archive.publish_prepared(batch, (entry,), repository=repository)
            raise RuntimeError("abort public archive publication")
        assert repository._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0] == 0
    segment = run_root / "ops-public-extents" / extent["metadata"]["extent"]["segment_name"]
    assert segment.is_file()
    assert read_extent_entry(run_root / "ops-public-extents", entry).num_rows == 1


def test_seal_worker_batches_all_slice_chunks_into_one_segment_write(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from atlas.v2.data.public_archive_extents import PublicArchiveSegmentWriterV1

    run_root = tmp_path / "run-seal-batch"
    run_root.mkdir()
    key = InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT", "BTC", "USDT", "USDT", sha256_json({"revision": "seal-batch-test"}),
    )
    frames = (
        L2RawFrameV2(
            key, "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT", "TRADE_FRAME_TEST",
            b'{"topic":"publicTrade.BTCUSDT","data":[]}',
            hashlib.sha256(b'{"topic":"publicTrade.BTCUSDT","data":[]}').hexdigest(),
            1, 2, 3, None, None, None, "BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR",
            "HEALTHY_CURRENT", "ACTUAL_SYSTEM",
        ),
        L2RawFrameV2(
            key, "BYBIT_PUBLIC_WS_BROAD_V2", "orderbook.50.BTCUSDT", "BOOK_DELTA_TEST",
            b'{"topic":"orderbook.50.BTCUSDT","data":{}}',
            hashlib.sha256(b'{"topic":"orderbook.50.BTCUSDT","data":{}}').hexdigest(),
            1, 2, 3, 10, 10, 9, "BYBIT_U", "HEALTHY_CURRENT", "ACTUAL_SYSTEM",
        ),
    )
    archive = L2FrameArchiveV2(run_root / "ops-l2-frames", None, compact_live=True,
                               clock_ns=lambda: 4)
    batch = archive.prepare_chunks(((frames[0],), (frames[1],)))
    chunk_ids = tuple(chunk.chunk_id for chunk in batch.chunks)
    rows = []
    for chunk in batch.chunks:
        for row in chunk.table.to_pylist():
            rows.append({**row, "_atlas_chunk_id": chunk.chunk_id,
                         "_atlas_floor_ns": chunk.frames[-1].available_at_ns})
    combined = pa.Table.from_pylist(rows)
    input_path = run_root / ".public-evidence-preparation" / "seal-batch.arrow"
    stream = pa.BufferOutputStream()
    with pa.ipc.new_stream(stream, combined.schema) as writer:
        writer.write_table(combined)
    input_hash, _ = write_preparation_blob(input_path, stream.getvalue().to_pybytes())
    batch_id = sha256_json({"version": "PUBLIC_ARCHIVE_SEAL_BATCH_V1",
                            "chunk_ids": list(chunk_ids)})
    request = PublicEvidencePreparationRequestV2(
        "SEAL", "seal-batch-job", run_root.name, str(run_root), "f" * 64, input_hash,
        str(input_path), str(input_path.with_name("seal-batch-result.json")), "d" * 64,
        key.content_hash, 0, 0, 0, namespace="ops-l2-frames", chunk_id=batch_id,
        floor_ns=3, clock_at_ns=4,
    )
    extent_root = run_root / "ops-public-extents"
    seal_writer = PublicArchiveSegmentWriterV1(extent_root)
    seal_calls = []
    original_seal_many = seal_writer.seal_many

    def counted_seal_many(specs):
        seal_calls.append(len(specs))
        return original_seal_many(specs)

    seal_writer.seal_many = counted_seal_many
    result = _execute_request(request, seal_writer, extent_root, {})
    extents = result["extents"]
    assert result["chunk_ids"] == list(chunk_ids)
    assert result["result_records"] == 2
    assert result["seal_metrics"]["extent_count"] == 2
    assert seal_calls == [2]
    assert len({item["metadata"]["extent"]["segment_name"] for item in extents}) == 1
    assert len({item["metadata"]["extent"]["offset"] for item in extents}) == 2
