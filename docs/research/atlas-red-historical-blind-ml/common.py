"""Small research artifact functions; no decision, outcome, venue or model API."""

from __future__ import annotations

import contextlib
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OLD = HERE.parent / "atlas-red-e1b"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(OLD))
SECOND = 1_000_000_000
M15 = 900 * SECOND
HOUR = 4 * M15
DAY = 24 * HOUR
PROTOCOL_SHA = "d41d00ea63561d67604b912808e5417dcced4f39606c75ac9318dbd0379ebcdd"
GRADE = "PRICE_DIAGNOSTIC_ONLY"


def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(obj):
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


def save(path, obj):
    pathlib.Path(path).write_text(json.dumps(obj, sort_keys=True, indent=2, allow_nan=False) + "\n")


def load(path):
    return json.loads(pathlib.Path(path).read_text())


def protocol():
    if sha(HERE / "PROTOCOL.json") != PROTOCOL_SHA:
        raise ValueError("SEALED_PROTOCOL_CHANGED")
    return load(HERE / "PROTOCOL.json")


def verify_prior_inputs():
    gate = load(HERE / "GATE_A_FEASIBILITY.json")
    pins = {**gate["freeze_sha256"], **gate["pinned_prior_artifacts_and_reused_code"]}
    for name, expected in pins.items():
        if sha(ROOT / name) != expected:
            raise ValueError("REVIEWED_PRIOR_INPUT_IDENTITY_CHANGED:" + name)
    return pins


def ns(text):
    return int(dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()) * SECOND


def iso(at):
    return dt.datetime.fromtimestamp(int(at) / SECOND, dt.UTC).isoformat()


@contextlib.contextmanager
def csv_writer(path, fields):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as raw,
        gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped,
        io.TextIOWrapper(zipped, encoding="utf-8", newline="") as text,
    ):
        writer = csv.DictWriter(text, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        yield writer


def read_csv(path):
    with gzip.open(path, "rt", newline="") as f:
        yield from csv.DictReader(f)


def write_csv(path, rows, fields=None):
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None and fields is None:
        raise ValueError("EMPTY_CSV_REQUIRES_SCHEMA")
    with csv_writer(path, fields or list(first)) as writer:
        if first is not None:
            writer.writerow(first)
        writer.writerows(iterator)
