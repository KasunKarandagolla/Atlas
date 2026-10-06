"""Bounded receipt journals preserve exact descriptors and reject interrupted tails."""
from __future__ import annotations

import json
import threading
from dataclasses import replace

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data import capture_receipts as module
from atlas.v2.data.capture_receipts import (
    CaptureReceiptBindingV1,
    CaptureReceiptJournalV1,
    CaptureReceiptLocationV1,
    read_capture_receipt,
    read_last_capture_receipt,
)

BINDING = CaptureReceiptBindingV1("run-s40-fixture", "a" * 64, "b" * 64)


def receipt(counter: int) -> dict:
    return {"version": "SEALED_PUBLIC_TRANSPORT_V1", "authority": "ZERO",
            "batch_ref": sha256_json({"counter": counter}), "counter": counter,
            "raw_archive_descriptor": {"offset": counter * 256, "length": 256, "sha256": "c" * 64}}


def latest(root):
    return read_last_capture_receipt(root, expected_run_id=BINDING.run_id,
                                    expected_configuration_hash=BINDING.configuration_hash)


def test_many_receipts_amortize_files_and_keep_exact_immutable_prefixes(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "MAX_JOURNAL_BYTES", 4096)
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    locations = []
    for counter in range(200):
        locations.append(journal.append(receipt(counter)))
    files = list(tmp_path.glob("capture-*.bin"))
    assert 1 < len(files) < 30
    assert all(file.stat().st_size <= 4096 for file in files)
    assert len(list(tmp_path.iterdir())) == len(files) + 1
    for counter, location in enumerate(locations):
        evidence = read_capture_receipt(tmp_path, location, expected_binding=BINDING)
        assert evidence.receipt == receipt(counter) and evidence.binding == BINDING
    evidence = latest(tmp_path)
    assert evidence.location == locations[-1] and evidence.receipt == receipt(199)


def test_latest_pointer_changes_only_at_rotation_and_reopen_uses_a_new_epoch(tmp_path):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    first = journal.append(receipt(1))
    pointer = tmp_path / module.LATEST_POINTER_NAME
    original = pointer.read_bytes(), pointer.stat().st_mtime_ns
    second = journal.append(receipt(2))
    assert first.journal_name == second.journal_name
    assert original == (pointer.read_bytes(), pointer.stat().st_mtime_ns)
    new_binding = replace(BINDING, capture_epoch="d" * 64)
    reopened = CaptureReceiptJournalV1(tmp_path, binding=new_binding)
    third = reopened.append(receipt(3))
    assert third.journal_name != first.journal_name
    assert read_capture_receipt(tmp_path, first, expected_binding=BINDING).receipt == receipt(1)
    assert latest(tmp_path).binding == new_binding
    assert latest(tmp_path).receipt == receipt(3)


@pytest.mark.parametrize("tail", [b"x", b"ATCAPR1\n", b"\x00" * 48, b"ATCAPE1\n"])
def test_interrupted_tail_is_retained_and_reopen_cannot_repair_or_ignore_it(tmp_path, tail):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    location = journal.append(receipt(1))
    path = tmp_path / location.journal_name
    with path.open("ab") as handle:
        handle.write(tail)
    damaged = path.read_bytes()
    with pytest.raises(ValueError):
        latest(tmp_path)
    with pytest.raises(ValueError):
        CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    assert path.read_bytes() == damaged
    assert read_capture_receipt(tmp_path, location, expected_binding=BINDING).receipt == receipt(1)


def test_changed_record_bytes_and_changed_binding_fail_strict_validation(tmp_path):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    location = journal.append(receipt(1))
    with pytest.raises(ValueError, match="binding"):
        read_capture_receipt(tmp_path, location, expected_binding=replace(BINDING, capture_epoch="d" * 64))
    with pytest.raises(ValueError, match="different run"):
        read_last_capture_receipt(tmp_path, expected_run_id="another-run",
                                  expected_configuration_hash=BINDING.configuration_hash)
    path = tmp_path / location.journal_name
    with path.open("r+b") as handle:
        handle.seek(location.offset + module._RECORD.size + 4)
        byte = handle.read(1)
        handle.seek(-1, 1)
        handle.write(bytes([byte[0] ^ 1]))
    with pytest.raises(ValueError, match="checksum"):
        latest(tmp_path)


def test_empty_pointerless_root_is_distinct_from_orphan_or_empty_latest_file(tmp_path):
    assert latest(tmp_path) is None
    orphan = tmp_path / "capture-orphan.bin"
    orphan.write_bytes(b"partial")
    with pytest.raises(ValueError, match="pointer missing"):
        latest(tmp_path)
    orphan.unlink()  # Fixture cleanup only; production never deletes retained evidence.
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    journal._rotate()
    with pytest.raises(ValueError, match="incomplete"):
        latest(tmp_path)


def test_failed_append_is_terminal_and_valid_previous_receipt_is_retained(tmp_path, monkeypatch):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    previous = journal.append(receipt(1))
    previous_bytes = (tmp_path / previous.journal_name).read_bytes()
    original = module.os.fsync
    failures = []

    def fail_once(fd):
        failures.append(fd)
        raise OSError("injected receipt durability failure")

    monkeypatch.setattr(module.os, "fsync", fail_once)
    with pytest.raises(OSError, match="durability"):
        journal.append(receipt(2))
    assert failures
    with pytest.raises(RuntimeError, match="terminal"):
        journal.append(receipt(3))
    monkeypatch.setattr(module.os, "fsync", original)
    assert (tmp_path / previous.journal_name).read_bytes().startswith(previous_bytes)
    assert read_capture_receipt(tmp_path, previous, expected_binding=BINDING).receipt == receipt(1)


def test_pointer_failure_retains_orphan_header_and_does_not_publish_receipt(tmp_path, monkeypatch):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)

    def fail_replace(*args):
        raise OSError("injected pointer replacement failure")

    monkeypatch.setattr(module, "_replace_durable_pointer", fail_replace)
    with pytest.raises(OSError, match="replacement"):
        journal.append(receipt(1))
    assert len(list(tmp_path.glob("capture-*.bin"))) == 1
    assert len(list(tmp_path.glob("pointer-*.tmp"))) == 1
    assert not (tmp_path / module.LATEST_POINTER_NAME).exists()
    with pytest.raises(ValueError, match="pointer missing"):
        latest(tmp_path)
    with pytest.raises(RuntimeError, match="terminal"):
        journal.append(receipt(2))


def test_pointer_tamper_and_receipt_location_overflow_are_rejected(tmp_path):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    location = journal.append(receipt(1))
    pointer = tmp_path / module.LATEST_POINTER_NAME
    wire = json.loads(pointer.read_bytes())
    wire["pointer"]["header_length"] += 1
    pointer.write_text(canonical_json(wire))
    with pytest.raises(ValueError, match="pointer checksum"):
        latest(tmp_path)
    with pytest.raises(ValueError, match="bounds"):
        CaptureReceiptLocationV1(location.journal_name, module.MAX_JOURNAL_BYTES, 100, "a" * 64)


def test_oversized_receipt_and_second_thread_cannot_mutate_the_journal(tmp_path):
    journal = CaptureReceiptJournalV1(tmp_path, binding=BINDING)
    location = journal.append(receipt(1))
    before = (tmp_path / location.journal_name).read_bytes()
    with pytest.raises(ValueError, match="bound"):
        journal.append({"authority": "ZERO", "large": "x" * module.MAX_RECEIPT_BYTES})
    failures = []

    def second_writer():
        try:
            journal.append(receipt(2))
        except Exception as exc:
            failures.append(type(exc).__name__)

    thread = threading.Thread(target=second_writer)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and failures == ["RuntimeError"]
    assert before == (tmp_path / location.journal_name).read_bytes()
