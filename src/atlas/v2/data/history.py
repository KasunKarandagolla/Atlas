"""Bounded import of local public-data JSONL into reconstructed V2 evidence."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from .bars import BarIntervalV2, CausalBarV2, close_boundary_ns
from .raw import AvailabilityClassV2, RawObservationV2


class ArchiveRecordKindV2(StrEnum):
    PUBLIC_OBSERVATION = "PUBLIC_OBSERVATION"
    HISTORICAL_IMPORT = "HISTORICAL_IMPORT"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"


@dataclass(frozen=True)
class ImportedObservationV2:
    row_number: int
    observation: RawObservationV2
    raw_payload_bytes: bytes
    bar: CausalBarV2 | None = None
    source_file_sha256: str | None = None
    source_chunk_id: str | None = None
    record_kind: ArchiveRecordKindV2 = ArchiveRecordKindV2.PUBLIC_OBSERVATION

    def __post_init__(self) -> None:
        if type(self.row_number) is not int or self.row_number <= 0:
            raise ValueError("import row_number must be a positive integer")
        if not isinstance(self.raw_payload_bytes, bytes):
            raise ValueError("raw_payload_bytes must be exact immutable bytes")
        if (self.source_file_sha256 is None) != (self.source_chunk_id is None):
            raise ValueError("historical source file hash and chunk ID must be supplied together")
        if self.source_file_sha256 is not None:
            sha256_ref(self.source_file_sha256, field="source_file_sha256")
            sha256_ref(self.source_chunk_id or "", field="source_chunk_id")
        try:
            object.__setattr__(self, "record_kind", ArchiveRecordKindV2(self.record_kind))
        except (ValueError, TypeError) as exc:
            raise ValueError("unknown public archive record kind") from exc


@dataclass(frozen=True)
class HistoricalImportBatchV2:
    source_id: str
    file_sha256: str
    chunk_id: str
    imported_at_ns: int
    replay_lag_ns: int
    observations: tuple[ImportedObservationV2, ...]
    bars: tuple[CausalBarV2, ...] = ()

    @property
    def record_count(self) -> int:
        return len(self.observations)


class ImportQuarantinedV2(ValueError):
    def __init__(self, message: str, *, file_sha256: str, row_number: int | None = None) -> None:
        super().__init__(message)
        self.file_sha256 = file_sha256
        self.row_number = row_number


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON number is unsupported: {value}")


class HistoricalImporterV2:
    """Imports one bounded JSONL file atomically; malformed chunks produce no observations."""

    def __init__(self, *, clock_ns=time.time_ns, receipt_skew_ns: int = 5_000_000_000) -> None:
        if receipt_skew_ns < 0:
            raise ValueError("receipt_skew_ns cannot be negative")
        self.clock_ns = clock_ns
        self.receipt_skew_ns = receipt_skew_ns

    def import_jsonl(
        self,
        path: str | Path,
        *,
        source_id: str,
        instrument_revision: str,
        imported_at_ns: int,
        replay_lag_ns: int,
        max_file_bytes: int = 64 * 1024 * 1024,
        chunk_rows: int = 500,
    ) -> tuple[HistoricalImportBatchV2, ...]:
        source_id = nonblank(source_id, field="source_id")
        timestamp(imported_at_ns, field="imported_at_ns")
        if replay_lag_ns < 0 or max_file_bytes <= 0 or chunk_rows <= 0:
            raise ValueError("replay lag, file size and chunk row limits are invalid")
        source_path = Path(path)
        if source_path.stat().st_size > max_file_bytes:
            oversized_hash = hashlib.sha256()
            with source_path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    oversized_hash.update(chunk)
            raise ImportQuarantinedV2(
                "historical source exceeds bounded import size", file_sha256=oversized_hash.hexdigest()
            )
        raw_file = source_path.read_bytes()
        file_hash = hashlib.sha256(raw_file).hexdigest()
        if abs(imported_at_ns - self.clock_ns()) > self.receipt_skew_ns:
            raise ImportQuarantinedV2(
                "supplied import receipt timestamp is not close to the current ATLAS clock", file_sha256=file_hash
            )
        try:
            text = raw_file.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ImportQuarantinedV2("historical source is not UTF-8", file_sha256=file_hash) from exc
        parsed: list[tuple[int, dict[str, Any], str]] = []
        for row_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(
                    line,
                    object_pairs_hook=_strict_json_object,
                    parse_constant=_reject_json_constant,
                )
                if not isinstance(value, dict):
                    raise ValueError("each JSONL row must be an object")
                expected = {"event_type", "event_at_ns", "payload"}
                allowed = expected | {"sequence", "published_at_ns", "revision_of", "quality_flags"}
                unknown = set(value) - allowed
                missing = expected - set(value)
                if unknown or missing:
                    raise ValueError(f"unknown={sorted(unknown)} missing={sorted(missing)}")
                if not isinstance(value["payload"], (dict, list)):
                    raise ValueError("payload must be an object or array")
                if "quality_flags" in value and not isinstance(value["quality_flags"], list):
                    raise ValueError("quality_flags must be an array")
                if "quality_flags" in value and any(not isinstance(flag, str) or not flag.strip() for flag in value["quality_flags"]):
                    raise ValueError("quality_flags must contain non-empty strings")
                if "sequence" in value and value["sequence"] is not None and (
                    isinstance(value["sequence"], bool) or not isinstance(value["sequence"], (str, int))
                ):
                    raise ValueError("sequence must be an integer, string or null")
                parsed.append((row_number, value, line))
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                raise ImportQuarantinedV2(
                    f"corrupt or unsupported JSONL row {row_number}: {exc}", file_sha256=file_hash, row_number=row_number
                ) from exc
        if not parsed:
            raise ImportQuarantinedV2("historical source contains no observations", file_sha256=file_hash)

        batches: list[HistoricalImportBatchV2] = []
        for first in range(0, len(parsed), chunk_rows):
            group = parsed[first : first + chunk_rows]
            chunk_number = first // chunk_rows
            chunk_id = sha256_json(
                {
                    "file_sha256": file_hash,
                    "source_id": source_id,
                    "instrument_revision": instrument_revision,
                    "chunk_number": chunk_number,
                    "row_count": len(group),
                }
            )
            observations: list[ImportedObservationV2] = []
            bars: list[CausalBarV2] = []
            for row_number, value, line in group:
                event_at = value["event_at_ns"]
                timestamp(event_at, field=f"row[{row_number}].event_at_ns")
                published_at = value.get("published_at_ns")
                if published_at is not None:
                    timestamp(published_at, field=f"row[{row_number}].published_at_ns")
                replay_available = event_at + replay_lag_ns
                flags = tuple(sorted(set(value.get("quality_flags", ())) | {"HISTORICAL_IMPORT"}))
                observation = RawObservationV2.build(
                    instrument_revision=instrument_revision,
                    source_id=source_id,
                    event_type=nonblank(value["event_type"], field="event_type"),
                    event_at_ns=event_at,
                    published_at_ns=published_at,
                    received_at_ns=imported_at_ns,
                    ingested_at_ns=imported_at_ns,
                    available_at_ns=imported_at_ns,
                    payload=line,
                    translation_version="historical-jsonl-import-v1",
                    revision_of=value.get("revision_of"),
                    quality_flags=flags,
                    availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
                    sequence=value.get("sequence"),
                    replay_available_at_ns=replay_available,
                )
                bar: CausalBarV2 | None = None
                if value["event_type"] in {f"BAR_{interval.value}" for interval in BarIntervalV2}:
                    interval = BarIntervalV2(str(value["event_type"]).removeprefix("BAR_"))
                    bar_payload = value["payload"]
                    required_bar_fields = {"open_at_ns", "open", "high", "low", "close", "volume", "final"}
                    if not isinstance(bar_payload, dict) or set(bar_payload) != required_bar_fields:
                        raise ImportQuarantinedV2(
                            f"historical bar row {row_number} must contain exactly {sorted(required_bar_fields)}",
                            file_sha256=file_hash,
                            row_number=row_number,
                        )
                    if bar_payload["final"] is not True:
                        raise ImportQuarantinedV2(
                            f"historical policy-ready bar row {row_number} is not final",
                            file_sha256=file_hash,
                            row_number=row_number,
                        )
                    open_at = bar_payload["open_at_ns"]
                    close_at = close_boundary_ns(open_at, interval)
                    if event_at < close_at:
                        raise ImportQuarantinedV2(
                            f"historical bar row {row_number} event timestamp precedes its close boundary",
                            file_sha256=file_hash,
                            row_number=row_number,
                        )
                    bar = CausalBarV2(
                            observation,
                            interval,
                            open_at,
                            close_at,
                            Decimal(str(bar_payload["open"])),
                            Decimal(str(bar_payload["high"])),
                            Decimal(str(bar_payload["low"])),
                            Decimal(str(bar_payload["close"])),
                            Decimal(str(bar_payload["volume"])),
                            True,
                        )
                    bars.append(bar)
                observations.append(
                    ImportedObservationV2(
                        row_number,
                        observation,
                        line.encode("utf-8"),
                        bar,
                        source_file_sha256=file_hash,
                        source_chunk_id=chunk_id,
                        record_kind=ArchiveRecordKindV2.HISTORICAL_IMPORT,
                    )
                )
            batches.append(
                HistoricalImportBatchV2(
                    source_id, file_hash, chunk_id, imported_at_ns, replay_lag_ns, tuple(observations), tuple(bars)
                )
            )
        return tuple(batches)


class ParquetObservationArchiveV2:
    """Immutable Parquet chunks for public/research observations, outside ops.sqlite."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_batch(self, batch: HistoricalImportBatchV2) -> Path:
        return self.write_observation_chunk(batch.chunk_id, batch.observations)

    def write_observation_chunk(self, chunk_id: str, observations: tuple[ImportedObservationV2, ...] | list[ImportedObservationV2]) -> Path:
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not observations:
            raise ValueError("Parquet observation chunk cannot be empty")
        nonblank(chunk_id, field="chunk_id")
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / f"{chunk_id}.parquet"
        rows = []
        for item in observations:
            observation = item.observation
            rows.append(
                {
                    "record_id": observation.record_id,
                    "instrument_revision": observation.instrument_revision,
                    "source_id": observation.source_id,
                    "event_type": observation.event_type,
                    "event_at_ns": observation.event_at_ns,
                    "published_at_ns": observation.published_at_ns,
                    "received_at_ns": observation.received_at_ns,
                    "ingested_at_ns": observation.ingested_at_ns,
                    "available_at_ns": observation.available_at_ns,
                    "replay_available_at_ns": observation.replay_available_at_ns,
                    "raw_payload_hash": observation.raw_payload_hash,
                    "translation_version": observation.translation_version,
                    "revision_of": observation.revision_of,
                    "quality_flags_json": canonical_json(list(observation.quality_flags)),
                    "availability_class": observation.availability_class.value,
                    "sequence_json": canonical_json(observation.sequence),
                    "observation_json": canonical_json(observation.to_dict()),
                    "raw_payload_bytes": item.raw_payload_bytes,
                    "import_row_number": item.row_number,
                    "bar_json": canonical_json(item.bar.to_dict() if item.bar is not None else None),
                    "source_file_sha256": item.source_file_sha256,
                    "source_chunk_id": item.source_chunk_id,
                    "archive_record_kind": item.record_kind.value,
                }
            )
        table = pa.Table.from_pylist(rows)
        temporary = target.with_suffix(".parquet.tmp")
        pq.write_table(table, temporary, compression="zstd")
        if target.exists():
            existing = pq.read_table(target)
            candidate = pq.read_table(temporary)
            identity_columns = (
                "record_id", "raw_payload_hash", "raw_payload_bytes", "import_row_number", "bar_json",
                "source_file_sha256", "source_chunk_id",
                "archive_record_kind",
            )
            equal = all(existing[name].to_pylist() == candidate[name].to_pylist() for name in identity_columns)
            if not equal:
                temporary.unlink(missing_ok=True)
                raise ValueError("deterministic Parquet chunk identity conflicts with archived content")
            temporary.unlink(missing_ok=True)
            return target
        temporary.replace(target)
        return target
