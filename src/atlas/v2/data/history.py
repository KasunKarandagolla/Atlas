"""Bounded import of local public-data JSONL into reconstructed V2 evidence."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from ..instruments import InstrumentKeyV2
from ..memory.repository import OpsRepository
from .bars import BarIntervalV2, CausalBarV2, close_boundary_ns
from .raw import AvailabilityClassV2, RawObservationV2

MAX_CAUSAL_ARCHIVE_FILES = 20_000
MAX_CAUSAL_ARCHIVE_ROWS = 2_000_000
MAX_NATIVE_M1_ORIGIN_CHUNK_ROWS = 10_000
MAX_ARCHIVE_CHUNK_FILE_BYTES = 256 * 1024 * 1024
MAX_ARCHIVE_CHUNK_DECODED_BYTES = 256 * 1024 * 1024


class ArchiveScanBoundExceededV2(RuntimeError):
    """The archive could not be scanned completely within its safety budget."""

    def __init__(self, bound: str, maximum: int) -> None:
        super().__init__(f"causal archive reconstruction exceeded its {bound} scan bound ({maximum})")
        self.bound = bound
        self.maximum = maximum


def _open_bounded_archive_chunk_v2(path: Path) -> Any:
    """Check immutable segment allocation budgets before decoding Arrow batches."""
    import pyarrow.parquet as pq

    if path.stat().st_size > MAX_ARCHIVE_CHUNK_FILE_BYTES:
        raise ArchiveScanBoundExceededV2("chunk-file-bytes", MAX_ARCHIVE_CHUNK_FILE_BYTES)
    parquet = pq.ParquetFile(path)
    decoded_bytes = sum(parquet.metadata.row_group(index).total_byte_size
                        for index in range(parquet.metadata.num_row_groups))
    if decoded_bytes > MAX_ARCHIVE_CHUNK_DECODED_BYTES:
        parquet.close()
        raise ArchiveScanBoundExceededV2("chunk-decoded-bytes", MAX_ARCHIVE_CHUNK_DECODED_BYTES)
    return parquet


def causal_revision_order_key(effective_available_at_ns: int, record_id: str) -> tuple[int, str]:
    """Stable latest-known revision ordering shared by chart and feature reconstruction."""
    return effective_available_at_ns, record_id


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
                if "quality_flags" in value and any(
                    not isinstance(flag, str) or not flag.strip() for flag in value["quality_flags"]
                ):
                    raise ValueError("quality_flags must contain non-empty strings")
                if (
                    "sequence" in value
                    and value["sequence"] is not None
                    and (isinstance(value["sequence"], bool) or not isinstance(value["sequence"], (str, int)))
                ):
                    raise ValueError("sequence must be an integer, string or null")
                parsed.append((row_number, value, line))
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                raise ImportQuarantinedV2(
                    f"corrupt or unsupported JSONL row {row_number}: {exc}",
                    file_sha256=file_hash,
                    row_number=row_number,
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

    def write_observation_chunk(
        self, chunk_id: str, observations: tuple[ImportedObservationV2, ...] | list[ImportedObservationV2]
    ) -> Path:
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
                "record_id",
                "raw_payload_hash",
                "raw_payload_bytes",
                "import_row_number",
                "bar_json",
                "source_file_sha256",
                "source_chunk_id",
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


@dataclass(frozen=True)
class IndexedCausalBarV2:
    """A final bar reconstructed from its exact archived observation and ops index."""

    bar: CausalBarV2
    observation_index_ref: str


@dataclass(frozen=True)
class IndexedPublicObservationV2:
    """Exact archived public payload paired with its immutable ops index ref."""

    observation: RawObservationV2
    raw_payload_bytes: bytes
    observation_index_ref: str


def _indexed_archive_paths_v1(
    root: Path, entries: tuple[Any, ...],
) -> tuple[tuple[Path, frozenset[str]], ...]:
    """Resolve only accepted immutable chunk locators; never rediscover by glob."""
    by_chunk: dict[str, set[str]] = {}
    records: dict[str, str] = {}
    for entry in entries:
        chunk = entry.metadata.get("archive_chunk_id")
        record = entry.metadata.get("record_id")
        if not isinstance(chunk, str) or not isinstance(record, str) or not record.strip():
            raise ValueError("accepted source index has no exact immutable archive locator")
        sha256_ref(chunk, field="archive_chunk_id")
        if records.setdefault(record, chunk) != chunk:
            raise ValueError("accepted source record has conflicting immutable chunk locators")
        by_chunk.setdefault(chunk, set()).add(record)
    if len(by_chunk) > MAX_CAUSAL_ARCHIVE_FILES:
        raise ArchiveScanBoundExceededV2("file-count", MAX_CAUSAL_ARCHIVE_FILES)
    paths = []
    for chunk in sorted(by_chunk):
        path = root / f"{chunk}.parquet"
        if path.is_symlink() or not path.is_file():
            raise ValueError("accepted source index points to a missing or unsafe immutable archive chunk")
        paths.append((path, frozenset(by_chunk[chunk])))
    return tuple(paths)


def reconstruct_public_observations_from_archive(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    instrument_revision: str,
    information_cutoff_ns: int,
    event_types: tuple[str, ...],
    limit: int = 100_000,
    key: InstrumentKeyV2 | None = None,
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
) -> tuple[IndexedPublicObservationV2, ...]:
    """Read exact persisted public payloads after checking archive and index identities."""
    import json

    from .._serialization import sha256_json

    sha256_ref(instrument_revision, field="instrument_revision")
    if key is not None and (not isinstance(key, InstrumentKeyV2) or key.contract_revision != instrument_revision):
        raise ValueError("public archive source key conflicts with its exact revision")
    view = AvailabilityClassV2(availability_class)
    if view != AvailabilityClassV2.ACTUAL_SYSTEM:
        raise ValueError("public payload reconstruction requires actual-system source availability")
    timestamp(information_cutoff_ns, field="information_cutoff_ns")
    kinds = tuple(sorted(set(event_types)))
    if not kinds or any(not isinstance(item, str) or not item.strip() for item in kinds):
        raise ValueError("public observation reconstruction requires event types")
    if type(limit) is not int or not 1 <= limit <= 100_000:
        raise ValueError("public observation reconstruction limit is outside its bound")
    root = Path(archive_root)
    if not root.exists() or not root.is_dir():
        return ()
    source_entries = repository.public_archive_history_entries(
        instrument_revision=instrument_revision, event_types=kinds,
        information_cutoff_ns=information_cutoff_ns, limit=limit,
        instrument_key_json=key.to_canonical_json() if key is not None else None,
        availability_class=view.value,
    )
    paths = _indexed_archive_paths_v1(root, source_entries)
    columns = {
        "record_id", "instrument_revision", "event_type", "available_at_ns", "raw_payload_hash",
        "observation_json", "raw_payload_bytes", "archive_record_kind",
    }
    found: dict[str, IndexedPublicObservationV2] = {}
    examined = 0
    for path, selected_record_ids in paths:
        parquet = None
        try:
            parquet = _open_bounded_archive_chunk_v2(path)
            if parquet.metadata.num_rows > MAX_CAUSAL_ARCHIVE_ROWS - examined:
                raise ArchiveScanBoundExceededV2("row-count", MAX_CAUSAL_ARCHIVE_ROWS)
            if not columns.issubset(set(parquet.schema.names)):
                continue
            for batch in parquet.iter_batches(columns=sorted(columns), batch_size=512):
                for row in batch.to_pylist():
                    examined += 1
                    if examined > MAX_CAUSAL_ARCHIVE_ROWS:
                        raise ArchiveScanBoundExceededV2("row-count", MAX_CAUSAL_ARCHIVE_ROWS)
                    if row["record_id"] not in selected_record_ids:
                        continue
                    if (row["instrument_revision"] != instrument_revision or row["event_type"] not in kinds
                            or row["archive_record_kind"] != ArchiveRecordKindV2.PUBLIC_OBSERVATION.value
                            or type(row["available_at_ns"]) is not int
                            or row["available_at_ns"] > information_cutoff_ns):
                        continue
                    raw_bytes = row["raw_payload_bytes"]
                    if not isinstance(raw_bytes, bytes) or hashlib.sha256(raw_bytes).hexdigest() != row["raw_payload_hash"]:
                        continue
                    observation = RawObservationV2.from_dict(json.loads(row["observation_json"]))
                    if (observation.record_id != row["record_id"]
                            or observation.instrument_revision != instrument_revision
                            or observation.event_type != row["event_type"]
                            or observation.available_at_ns != row["available_at_ns"]
                            or observation.availability_class != view
                            or observation.raw_payload_hash != row["raw_payload_hash"]):
                        continue
                    index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                                             "record_id": observation.record_id})
                    found[observation.record_id] = IndexedPublicObservationV2(observation, raw_bytes, index_ref)
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
        finally:
            if parquet is not None:
                parquet.close()
    indexed = repository.get_artifact_metadata_by_refs(
        tuple(item.observation_index_ref for item in found.values())
    )
    rows = []
    for item in found.values():
        observation = item.observation
        entry = indexed.get(item.observation_index_ref)
        metadata = entry.get("metadata") if entry is not None else None
        if (entry is None or entry.get("artifact_type") != "PublicObservationIndexV2"
                or entry.get("content_hash") != observation.content_hash
                or entry.get("available_at_ns") != observation.available_at_ns
                or not isinstance(metadata, Mapping)
                or metadata.get("record_id") != observation.record_id
                or metadata.get("instrument_revision") != instrument_revision
                or key is not None and metadata.get("instrument_key_json") != key.to_canonical_json()
                or metadata.get("event_at_ns") != observation.event_at_ns
                or metadata.get("published_at_ns") != observation.published_at_ns
                or metadata.get("raw_payload_hash") != observation.raw_payload_hash):
            continue
        rows.append(item)
    rows.sort(key=lambda item: (item.observation.available_at_ns, item.observation.record_id))
    return tuple(rows[-limit:])


def reconstruct_causal_bars_from_archive(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    key: InstrumentKeyV2,
    interval: BarIntervalV2,
    information_cutoff_ns: int,
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
    limit: int = 100_000,
) -> tuple[IndexedCausalBarV2, ...]:
    """Rebuild immutable bars from collected Parquet rows and their exact ops refs.

    The archived observation format intentionally carries a contract revision,
    while InstrumentKeyV2 owns the full instrument identity. PublicCollectorV2
    persists the canonical key beside its observation index after resolving that
    revision uniquely. Old or ambiguous index rows are not admitted here.
    """
    import json

    from .._serialization import sha256_json

    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("causal archive reconstruction requires a full InstrumentKeyV2")
    frame = BarIntervalV2(interval)
    view = AvailabilityClassV2(availability_class)
    if view not in (AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET):
        raise ValueError("causal archive reconstruction requires actual or reconstructed availability")
    timestamp(information_cutoff_ns, field="information_cutoff_ns")
    if type(limit) is not int or not 1 <= limit <= 100_000:
        raise ValueError("causal archive reconstruction limit is outside the supported bound")
    root = Path(archive_root)
    if not root.exists() or not root.is_dir():
        return ()

    selected: dict[int, tuple[tuple[int, str], IndexedCausalBarV2]] = {}
    indexed_candidates: dict[str, tuple[int, tuple[int, str], IndexedCausalBarV2]] = {}
    examined = 0
    columns = {
        "record_id",
        "instrument_revision",
        "event_type",
        "available_at_ns",
        "replay_available_at_ns",
        "availability_class",
        "raw_payload_hash",
        "observation_json",
        "raw_payload_bytes",
        "bar_json",
        "archive_record_kind",
    }
    source_entries = repository.public_archive_history_entries(
        instrument_revision=key.contract_revision, event_types=(f"BAR_{frame.value}",),
        information_cutoff_ns=information_cutoff_ns, limit=limit,
        instrument_key_json=key.to_canonical_json(), availability_class=view.value,
    )
    paths = _indexed_archive_paths_v1(root, source_entries)
    for path, selected_record_ids in paths:
        parquet = None
        try:
            parquet = _open_bounded_archive_chunk_v2(path)
            if parquet.metadata.num_rows > MAX_CAUSAL_ARCHIVE_ROWS - examined:
                raise ArchiveScanBoundExceededV2("row-count", MAX_CAUSAL_ARCHIVE_ROWS)
            if not columns.issubset(set(parquet.schema.names)):
                continue
            for batch in parquet.iter_batches(columns=sorted(columns), batch_size=512):
                for row in batch.to_pylist():
                    examined += 1
                    if examined > MAX_CAUSAL_ARCHIVE_ROWS:
                        raise ArchiveScanBoundExceededV2("row-count", MAX_CAUSAL_ARCHIVE_ROWS)
                    if row["record_id"] not in selected_record_ids:
                        continue
                    if (
                        row["instrument_revision"] != key.contract_revision
                        or row["event_type"] != f"BAR_{frame.value}"
                        or row["archive_record_kind"] != ArchiveRecordKindV2.PUBLIC_OBSERVATION.value
                        or row["availability_class"] != view.value
                    ):
                        continue
                    available = (
                        row["available_at_ns"]
                        if view == AvailabilityClassV2.ACTUAL_SYSTEM
                        else row["replay_available_at_ns"]
                    )
                    if type(available) is not int or available > information_cutoff_ns:
                        continue
                    raw_bytes = row["raw_payload_bytes"]
                    if (
                        not isinstance(raw_bytes, bytes)
                        or hashlib.sha256(raw_bytes).hexdigest() != row["raw_payload_hash"]
                    ):
                        continue
                    observation = RawObservationV2.from_dict(json.loads(row["observation_json"]))
                    if (
                        observation.record_id != row["record_id"]
                        or observation.instrument_revision != key.contract_revision
                        or observation.raw_payload_hash != row["raw_payload_hash"]
                        or observation.available_at_ns != row["available_at_ns"]
                        or observation.replay_available_at_ns != row["replay_available_at_ns"]
                        or observation.availability_class != view
                    ):
                        continue
                    raw_bar = json.loads(row["bar_json"])
                    if not isinstance(raw_bar, dict) or raw_bar.get("final") is not True:
                        continue
                    open_at = raw_bar.get("open_at_ns")
                    if type(open_at) is not int:
                        continue
                    close_at = close_boundary_ns(open_at, frame)
                    if view == AvailabilityClassV2.RECONSTRUCTED_MARKET and available < close_at:
                        continue
                    bar = CausalBarV2(
                        observation,
                        frame,
                        open_at,
                        close_at,
                        Decimal(str(raw_bar["open"])),
                        Decimal(str(raw_bar["high"])),
                        Decimal(str(raw_bar["low"])),
                        Decimal(str(raw_bar["close"])),
                        Decimal(str(raw_bar["volume"])),
                        True,
                    )
                    if bar.content_hash != sha256_json(raw_bar):
                        continue
                    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
                    revision_key = causal_revision_order_key(available, observation.record_id)
                    indexed_candidates[ref] = (bar.open_at_ns, revision_key, IndexedCausalBarV2(bar, ref))
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
        finally:
            if parquet is not None:
                parquet.close()
    indexed = repository.get_artifact_metadata_by_refs(tuple(indexed_candidates))
    for ref, (open_at, revision_key, candidate) in indexed_candidates.items():
        entry = indexed.get(ref)
        metadata = entry.get("metadata") if entry is not None else None
        bar = candidate.bar
        observation = bar.raw
        if (entry is None or entry.get("artifact_type") != "PublicObservationIndexV2"
                or entry.get("content_hash") != observation.content_hash
                or entry.get("available_at_ns") != observation.available_at_ns
                or not isinstance(metadata, Mapping)
                or metadata.get("record_id") != observation.record_id
                or metadata.get("instrument_revision") != key.contract_revision
                or metadata.get("instrument_key_json") != key.to_canonical_json()
                or metadata.get("availability_class") != view.value
                or metadata.get("replay_available_at_ns") != observation.replay_available_at_ns
                or metadata.get("bar_content_hash") != bar.content_hash
                or metadata.get("raw_payload_hash") != observation.raw_payload_hash):
            continue
        current = selected.get(open_at)
        if current is None or revision_key > current[0]:
            selected[open_at] = (revision_key, candidate)
    rows = [selected[open_at][1] for open_at in sorted(selected)]
    return tuple(rows[-limit:])


def reconstruct_native_m1_bars_from_index_page(
    repository: OpsRepository, archive_root: str | Path, *, key: InstrumentKeyV2,
    index_entries: tuple[Any, ...], max_origins: int = 4,
) -> tuple[IndexedCausalBarV2, ...]:
    """Compatibility port for bounded exact native M1 reconstruction."""
    if type(max_origins) is not int or not 1 <= max_origins <= 4:
        raise ValueError("native M1 reconstruction page exceeds the fixed origin work bound")
    return reconstruct_native_bars_from_index_page(
        repository, archive_root, key=key, interval=BarIntervalV2.M1,
        index_entries=index_entries, max_origins=max_origins,
    )


def reconstruct_native_bars_from_index_page(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    key: InstrumentKeyV2,
    interval: BarIntervalV2,
    index_entries: tuple[Any, ...],
    max_origins: int = 4,
) -> tuple[IndexedCausalBarV2, ...]:
    """Rebuild only a bounded page of exact M1 origins from their indexed chunks.

    Unlike full-history reconstruction, this reads only the immutable Parquet
    chunks named by the already paged public-observation index rows. Each
    chunk has a hard row bound and the selected record ids are checked against
    both the archived bytes and the complete typed source index.
    """
    import json

    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("native bar reconstruction requires full InstrumentKeyV2")
    if type(max_origins) is not int or not 1 <= max_origins <= 128:
        raise ValueError("native bar reconstruction page exceeds the fixed origin work bound")
    if len(index_entries) > max_origins:
        raise ValueError("native bar reconstruction received more rows than its bounded page")
    if interval not in (BarIntervalV2.M1, BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4):
        raise ValueError("native indexed reconstruction only supports registered causal intervals")
    event_type = "BAR_" + interval.value
    if not index_entries:
        return ()

    from ..memory.repository import ArtifactIndexEntryV2

    by_chunk: dict[str, dict[str, ArtifactIndexEntryV2]] = {}
    for entry in index_entries:
        if not isinstance(entry, ArtifactIndexEntryV2):
            raise ValueError("native bar source page contains an invalid artifact entry")
        metadata = entry.metadata
        record_id = metadata.get("record_id")
        chunk_id = metadata.get("archive_chunk_id")
        if (entry.artifact_type != "PublicObservationIndexV2"
                or not isinstance(record_id, str) or not record_id.strip()
                or not isinstance(chunk_id, str) or not chunk_id.strip()
                or metadata.get("event_type") != event_type
                or metadata.get("instrument_key_json") != key.to_canonical_json()
                or metadata.get("instrument_revision") != key.contract_revision
                or metadata.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value
                or type(metadata.get("event_at_ns")) is not int
                or entry.artifact_ref != sha256_json({
                    "artifact_type": "PublicObservationIndexV2", "record_id": record_id,
                })):
            raise ValueError("native bar source page index identity is invalid")
        sha256_ref(chunk_id, field="archive_chunk_id")
        if record_id in by_chunk.setdefault(chunk_id, {}):
            raise ValueError("native bar source page repeats an exact source record")
        by_chunk[chunk_id][record_id] = entry

    root = Path(archive_root)
    rows_by_ref: dict[str, IndexedCausalBarV2] = {}
    required_columns = {
        "record_id", "instrument_revision", "event_type", "event_at_ns", "published_at_ns",
        "received_at_ns", "ingested_at_ns", "available_at_ns", "replay_available_at_ns",
        "raw_payload_hash", "translation_version", "revision_of", "quality_flags_json",
        "availability_class", "observation_json", "raw_payload_bytes", "bar_json",
        "archive_record_kind",
    }
    for chunk_id, expected_rows in by_chunk.items():
        path = root / f"{chunk_id}.parquet"
        if path.is_symlink() or not path.is_file():
            raise ValueError("native bar source page points to a missing or unsafe archive chunk")
        parquet = _open_bounded_archive_chunk_v2(path)
        if (parquet.metadata.num_rows > MAX_NATIVE_M1_ORIGIN_CHUNK_ROWS
                or not required_columns.issubset(set(parquet.schema.names))):
            raise ArchiveScanBoundExceededV2("native-bar-chunk-row-count", MAX_NATIVE_M1_ORIGIN_CHUNK_ROWS)
        found: set[str] = set()
        for batch in parquet.iter_batches(columns=sorted(required_columns), batch_size=256):
            for row in batch.to_pylist():
                record_id = row.get("record_id")
                index_entry = expected_rows.get(record_id) if isinstance(record_id, str) else None
                if index_entry is None:
                    continue
                observation_bytes = row.get("raw_payload_bytes")
                if (not isinstance(observation_bytes, bytes)
                        or hashlib.sha256(observation_bytes).hexdigest() != row.get("raw_payload_hash")
                        or row.get("archive_record_kind") != ArchiveRecordKindV2.PUBLIC_OBSERVATION.value
                        or row.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value
                        or row.get("event_type") != event_type):
                    raise ValueError("native bar archived source row failed its exact byte or class check")
                observation_wire = json.loads(row["observation_json"])
                observation = RawObservationV2.from_dict(observation_wire)
                if canonical_json(observation.to_dict()) != row["observation_json"]:
                    raise ValueError("native bar archived observation JSON is not canonical")
                metadata = index_entry.metadata
                if (observation.record_id != record_id
                        or observation.instrument_revision != key.contract_revision
                        or observation.source_id != metadata.get("source_id")
                        or observation.event_type != event_type
                        or observation.event_at_ns != metadata.get("event_at_ns")
                        or observation.published_at_ns != metadata.get("published_at_ns")
                        or observation.received_at_ns != index_entry.created_at_ns
                        or observation.available_at_ns != index_entry.available_at_ns
                        or observation.content_hash != index_entry.content_hash
                        or observation.raw_payload_hash != metadata.get("raw_payload_hash")
                        or observation.translation_version != metadata.get("translation_version")
                        or observation.revision_of != metadata.get("revision_of")
                        or observation.quality_flags != tuple(metadata.get("quality_flags", ()))
                        or observation.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                        or observation.replay_available_at_ns is not None):
                    raise ValueError("native bar archived observation conflicts with its exact source index")
                bar_wire = json.loads(row["bar_json"])
                if not isinstance(bar_wire, Mapping):
                    raise ValueError("native bar index points to an archived row without a typed bar")
                if type(bar_wire.get("final")) is not bool:
                    raise ValueError("native bar archived bar has an invalid final flag")
                if canonical_json(dict(bar_wire)) != row["bar_json"]:
                    raise ValueError("native bar archived bar JSON is not canonical")
                if bar_wire.get("final") is not True:
                    # A typed source index may point at a forming bar; it is
                    # simply not an accountable final origin.
                    if sha256_json(dict(bar_wire)) != metadata.get("bar_content_hash"):
                        raise ValueError("native bar non-final bar conflicts with its source index")
                    found.add(record_id)
                    continue
                if (type(bar_wire.get("open_at_ns")) is not int
                        or type(bar_wire.get("close_at_ns")) is not int):
                    raise ValueError("native bar archived bar timestamps are malformed")
                bar = CausalBarV2(
                    observation,
                    BarIntervalV2(str(bar_wire["interval"])),
                    int(bar_wire["open_at_ns"]),
                    int(bar_wire["close_at_ns"]),
                    Decimal(str(bar_wire["open"])),
                    Decimal(str(bar_wire["high"])),
                    Decimal(str(bar_wire["low"])),
                    Decimal(str(bar_wire["close"])),
                    Decimal(str(bar_wire["volume"])),
                    bar_wire["final"],
                )
                if (bar.interval != interval or not bar.final
                        or bar.instrument_revision != key.contract_revision
                        or bar.raw.content_hash != observation.content_hash
                        or bar.close_at_ns != observation.event_at_ns
                        or bar.close_at_ns != metadata.get("event_at_ns")
                        or bar.raw.available_at_ns != index_entry.available_at_ns
                        or bar.content_hash != metadata.get("bar_content_hash")
                        or sha256_json(dict(bar_wire)) != bar.content_hash):
                    raise ValueError("native bar bar does not verify against its exact source origin")
                rows_by_ref[index_entry.artifact_ref] = IndexedCausalBarV2(bar, index_entry.artifact_ref)
                found.add(record_id)
        if found != set(expected_rows):
            raise ValueError("native bar source records are missing from their exact archive chunks")

    result = tuple(sorted(rows_by_ref.values(), key=lambda item: (
        item.bar.close_at_ns, item.bar.raw.available_at_ns, item.observation_index_ref,
    )))
    if len(result) > max_origins:
        raise ValueError("native bar archive reconstruction exceeded its bounded result count")
    return result


def reconstruct_indexed_causal_bars_v1(
    repository: OpsRepository, archive_root: str | Path, *, key: InstrumentKeyV2,
    interval: BarIntervalV2, index_entries: tuple[Any, ...], information_cutoff_ns: int,
) -> tuple[IndexedCausalBarV2, ...]:
    """Verify a bounded exact-target revision inventory against accepted DB and bytes."""
    cutoff = timestamp(information_cutoff_ns, field="information_cutoff_ns")
    if len(index_entries) > 128:
        raise ValueError("exact target reconstruction exceeds its 128-revision bound")
    indexed = repository.get_artifact_metadata_by_refs(tuple(entry.artifact_ref for entry in index_entries))
    for entry in index_entries:
        persisted = indexed.get(entry.artifact_ref)
        if (persisted is None or persisted.get("artifact_type") != entry.artifact_type
                or persisted.get("content_hash") != entry.content_hash
                or persisted.get("available_at_ns") != entry.available_at_ns
                or canonical_json(persisted.get("metadata")) != canonical_json(entry.metadata)
                or entry.available_at_ns > cutoff):
            raise ValueError("exact target source index is unavailable or conflicts with accepted persistence")
    return reconstruct_native_bars_from_index_page(repository, archive_root, key=key,
        interval=interval, index_entries=index_entries, max_origins=128)
