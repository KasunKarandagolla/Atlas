"""Point-in-time proof for qualified V1 feature snapshots.

The frozen FeatureSnapshot hash deliberately remains a five-field historical
identity. This module adds a separate receipt verified against immutable
MarketRecord rows in the content-addressed V1 Parquet archive.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any

from atlas.data.archive import ParquetArchive
from atlas.data.models import DataKind, MarketRecord
from atlas.domain.enums import AvailabilityClass
from atlas.strategy.features import HOUR_NS, WINDOW_CLOSES, FeatureValues, HourlyClose, feature_values

SOURCE_PROOF_VERSION = "FEATURE_SNAPSHOT_SOURCE_PROOF_V1"
MAX_SOURCE_ARCHIVE_FILES = 20_000
MAX_SOURCE_ARCHIVE_ROWS = 2_000_000
MAX_SOURCE_ARCHIVE_BYTES = 1_000_000_000
MAX_SOURCE_ARCHIVE_CHUNK_BYTES = 256 * 1024 * 1024
MAX_SOURCE_ARCHIVE_CHUNK_DECODED_BYTES = 256 * 1024 * 1024


class SourceAvailabilityMode(StrEnum):
    ACTUAL_SYSTEM = "ACTUAL_SYSTEM"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"


class SourceProofError(ValueError):
    """The archive cannot establish the exact causal feature source window."""


def _canonical_hash(value: Any) -> str:
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise SourceProofError("source proof values are not canonical JSON") from exc
    return hashlib.sha256(body.encode()).hexdigest()


def feature_values_hash(values: FeatureValues) -> str:
    """Hash every frozen feature output, including all 720 returns."""
    return _canonical_hash(
        {
            "end_at_ns": values.end_at_ns,
            "returns": values.returns,
            "variance": values.variance,
            "sigma": values.sigma,
            "z": values.z,
        }
    )


def _as_market_record(row: dict[str, object]) -> MarketRecord:
    values = dict(row)
    stored_fingerprint = values.pop("record_fingerprint", None)
    values["data_kind"] = DataKind(str(values["data_kind"]))
    values["availability_class"] = AvailabilityClass(str(values["availability_class"]))
    dependency_ids = values["dependency_ids"]
    if not isinstance(dependency_ids, (list, tuple)) or any(not isinstance(item, str) for item in dependency_ids):
        raise SourceProofError("archived record dependencies are malformed")
    values["dependency_ids"] = tuple(dependency_ids)
    record = MarketRecord(**values)  # type: ignore[arg-type]
    if stored_fingerprint != record.record_fingerprint:
        raise SourceProofError(f"archived record fingerprint mismatch: {record.record_id}")
    return record


class ParquetFeatureSourceStore:
    """Read-only resolver for exact V1 MarketRecord rows in the archive.

    Each immutable chunk is rehashed using the archive's canonical row format;
    each row is reconstructed and its content hash and full causal fingerprint
    are recomputed. The configured archive root is the trust boundary.
    """

    def __init__(self, root: str | Path):
        self.archive = ParquetArchive(root)

    def records(self, *, instrument: str, start_at_ns: int, end_at_ns: int) -> tuple[MarketRecord, ...]:
        paths = sorted(self.archive.root.rglob("part-*.parquet"))
        if len(paths) > MAX_SOURCE_ARCHIVE_FILES:
            raise SourceProofError("source archive scan exceeds file bound")
        if instrument not in {"BTCUSDT", "ETHUSDT"} or start_at_ns > end_at_ns:
            raise SourceProofError("invalid source archive lookup scope")
        output: list[MarketRecord] = []
        seen: set[str] = set()
        scanned_rows = 0
        scanned_bytes = 0
        for path in paths:
            file_bytes = path.stat().st_size
            scanned_bytes += file_bytes
            if file_bytes > MAX_SOURCE_ARCHIVE_CHUNK_BYTES or scanned_bytes > MAX_SOURCE_ARCHIVE_BYTES:
                raise SourceProofError("source archive scan exceeds byte bound")
            try:
                import pyarrow.parquet as pq

                parquet = pq.ParquetFile(path)
                decoded_bytes = sum(
                    parquet.metadata.row_group(index).total_byte_size
                    for index in range(parquet.metadata.num_row_groups)
                )
                parquet.close()
            except (OSError, ValueError) as exc:
                raise SourceProofError(f"source archive chunk cannot be inspected: {path.name}") from exc
            if decoded_bytes > MAX_SOURCE_ARCHIVE_CHUNK_DECODED_BYTES:
                raise SourceProofError("source archive chunk exceeds decoded byte bound")
            rows = self.archive.read_rows(path)
            scanned_rows += len(rows)
            if scanned_rows > MAX_SOURCE_ARCHIVE_ROWS:
                raise SourceProofError("source archive scan exceeds row bound")
            records = tuple(_as_market_record(row) for row in rows)
            canonical_rows = [record.to_dict() for record in sorted(records, key=lambda item: item.record_id)]
            digest = hashlib.sha256(
                json.dumps(canonical_rows, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            if path.name != f"part-{digest}.parquet":
                raise SourceProofError(f"source archive chunk hash mismatch: {path.name}")
            for record in records:
                if self.archive._partition(record) != path.parent:
                    raise SourceProofError(f"source archive partition mismatch: {record.record_id}")
                if (
                    record.instrument == instrument
                    and record.data_kind is DataKind.BAR_1H_LAST
                    and record.bar_end_ns is not None
                    and start_at_ns <= record.bar_end_ns <= end_at_ns
                ):
                    if record.record_id in seen:
                        raise SourceProofError(f"duplicate archived source identity: {record.record_id}")
                    seen.add(record.record_id)
                    output.append(record)
        return tuple(output)


@dataclass(frozen=True)
class FeatureSnapshotSourceProofV1:
    """Durable source identity for one complete qualified V1 feature window."""

    schema_version: str
    availability_mode: SourceAvailabilityMode
    instrument: str
    slot_at_ns: int
    computed_at_ns: int
    information_cutoff_ns: int
    snapshot_hash: str
    feature_values_hash: str
    source_record_ids: tuple[str, ...]
    source_record_fingerprints: tuple[str, ...]
    source_content_hashes: tuple[str, ...]
    source_ids: tuple[str, ...]
    source_end_at_ns: tuple[int, ...]
    actual_available_at_ns: tuple[int, ...]
    replay_available_at_ns: tuple[int | None, ...]
    effective_available_at_ns: tuple[int, ...]
    availability_classes: tuple[str, ...]
    source_revisions: tuple[str | None, ...]
    revision_of: tuple[str | None, ...]
    chronology_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != SOURCE_PROOF_VERSION:
            raise SourceProofError("unsupported V1 source proof version")
        try:
            object.__setattr__(self, "availability_mode", SourceAvailabilityMode(self.availability_mode))
        except (TypeError, ValueError) as exc:
            raise SourceProofError("unknown source availability mode") from exc
        if self.instrument not in {"BTCUSDT", "ETHUSDT"}:
            raise SourceProofError("source proof instrument is outside frozen V1 scope")
        if (
            type(self.slot_at_ns) is not int
            or type(self.computed_at_ns) is not int
            or type(self.information_cutoff_ns) is not int
            or not self.slot_at_ns <= self.computed_at_ns <= self.slot_at_ns + 30_000_000_000
            or self.information_cutoff_ns != self.computed_at_ns
        ):
            raise SourceProofError("source proof computation/cutoff chronology is invalid")
        for value in (self.snapshot_hash, self.feature_values_hash, self.chronology_hash):
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise SourceProofError("source proof requires canonical SHA-256 identities")
        columns = (
            self.source_record_ids,
            self.source_record_fingerprints,
            self.source_content_hashes,
            self.source_ids,
            self.source_end_at_ns,
            self.actual_available_at_ns,
            self.replay_available_at_ns,
            self.effective_available_at_ns,
            self.availability_classes,
            self.source_revisions,
            self.revision_of,
        )
        if any(not isinstance(column, tuple) or len(column) != WINDOW_CLOSES for column in columns):
            raise SourceProofError("source proof requires exactly 721 source observations")
        if len(set(self.source_record_ids)) != WINDOW_CLOSES:
            raise SourceProofError("source proof record identities must be unique")
        if any(
            not isinstance(value, str) or not value or value != value.strip()
            for value in self.source_record_ids + self.source_ids
        ):
            raise SourceProofError("source proof identities must be canonical and nonblank")
        if any(
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
            for value in self.source_record_fingerprints + self.source_content_hashes
        ):
            raise SourceProofError("source proof record hashes must be canonical SHA-256 values")
        if any(
            self.source_end_at_ns[index] != self.slot_at_ns - (WINDOW_CLOSES - index - 1) * HOUR_NS
            for index in range(WINDOW_CLOSES)
        ):
            raise SourceProofError("source proof timestamps are not the exact contiguous V1 window")
        if any(
            self.effective_available_at_ns[index] > self.information_cutoff_ns
            or self.effective_available_at_ns[index] < self.source_end_at_ns[index]
            or self.actual_available_at_ns[index] < self.source_end_at_ns[index]
            for index in range(WINDOW_CLOSES)
        ):
            raise SourceProofError("source proof availability is outside the information cutoff")

    def evidence_hash(self) -> str:
        return _canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "availability_mode": self.availability_mode.value,
            "instrument": self.instrument,
            "slot_at_ns": self.slot_at_ns,
            "computed_at_ns": self.computed_at_ns,
            "information_cutoff_ns": self.information_cutoff_ns,
            "snapshot_hash": self.snapshot_hash,
            "feature_values_hash": self.feature_values_hash,
            "source_record_ids": list(self.source_record_ids),
            "source_record_fingerprints": list(self.source_record_fingerprints),
            "source_content_hashes": list(self.source_content_hashes),
            "source_ids": list(self.source_ids),
            "source_end_at_ns": list(self.source_end_at_ns),
            "actual_available_at_ns": list(self.actual_available_at_ns),
            "replay_available_at_ns": list(self.replay_available_at_ns),
            "effective_available_at_ns": list(self.effective_available_at_ns),
            "availability_classes": list(self.availability_classes),
            "source_revisions": list(self.source_revisions),
            "revision_of": list(self.revision_of),
            "chronology_hash": self.chronology_hash,
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> FeatureSnapshotSourceProofV1:
        tuple_fields = {
            "source_record_ids",
            "source_record_fingerprints",
            "source_content_hashes",
            "source_ids",
            "source_end_at_ns",
            "actual_available_at_ns",
            "replay_available_at_ns",
            "effective_available_at_ns",
            "availability_classes",
            "source_revisions",
            "revision_of",
        }
        body = dict(value)
        for field in tuple_fields:
            raw = body[field]
            if not isinstance(raw, (list, tuple)):
                raise SourceProofError(f"serialized {field} must be an array")
            body[field] = tuple(raw)
        return cls(**body)  # type: ignore[arg-type]


def _effective_available(record: MarketRecord, mode: SourceAvailabilityMode) -> int | None:
    if mode is SourceAvailabilityMode.ACTUAL_SYSTEM:
        if record.availability_class is not AvailabilityClass.ACTUAL_OBSERVED:
            return None
        return record.available_at_ns
    if record.availability_class is not AvailabilityClass.RECONSTRUCTED_PUBLIC:
        return None
    return record.replay_available_at_ns


def _close(
    record: MarketRecord, *, instrument: str, end_at_ns: int, mode: SourceAvailabilityMode, cutoff_ns: int
) -> tuple[HourlyClose, int] | None:
    if (
        record.instrument != instrument
        or record.data_kind is not DataKind.BAR_1H_LAST
        or record.bar_end_ns != end_at_ns
        or record.bar_start_ns != end_at_ns - HOUR_NS
        or record.source_event_at_ns is None
        or not end_at_ns - HOUR_NS <= record.source_event_at_ns <= end_at_ns
        or record.record_id != record.record_id.strip()
        or record.source_id != record.source_id.strip()
        or record.venue != record.venue.strip()
        or record.pipeline_version != record.pipeline_version.strip()
        or (record.source_published_at_ns is not None and record.source_published_at_ns > cutoff_ns)
        or (record.source_valid_from_ns is not None and record.source_valid_from_ns > cutoff_ns)
        or (
            record.source_revision is not None
            and (not record.source_revision.strip() or record.source_revision != record.source_revision.strip())
        )
    ):
        return None
    effective = _effective_available(record, mode)
    if effective is None or effective > cutoff_ns or effective < end_at_ns:
        return None
    raw_close = record.payload.get("close")
    if raw_close is None:
        return None
    try:
        close_value = Decimal(str(raw_close))
    except (InvalidOperation, ValueError):
        return None
    if not close_value.is_finite() or close_value <= 0:
        return None
    return HourlyClose(end_at_ns, close_value, effective, record.record_id), effective


def _select_window(
    records: tuple[MarketRecord, ...], *, instrument: str, slot_at_ns: int, cutoff_ns: int, mode: SourceAvailabilityMode
) -> tuple[tuple[MarketRecord, ...], tuple[HourlyClose, ...], tuple[int, ...]]:
    selected_records: list[MarketRecord] = []
    closes: list[HourlyClose] = []
    effective_times: list[int] = []
    by_end: dict[int, list[MarketRecord]] = {}
    for record in records:
        if (
            record.instrument == instrument
            and record.data_kind is DataKind.BAR_1H_LAST
            and record.bar_end_ns is not None
        ):
            by_end.setdefault(record.bar_end_ns, []).append(record)
    for index in range(WINDOW_CLOSES):
        end_at_ns = slot_at_ns - (WINDOW_CLOSES - index - 1) * HOUR_NS
        candidates: list[tuple[MarketRecord, HourlyClose, int]] = []
        for record in by_end.get(end_at_ns, ()):
            result = _close(record, instrument=instrument, end_at_ns=end_at_ns, mode=mode, cutoff_ns=cutoff_ns)
            if result is not None:
                close, effective = result
                candidates.append((record, close, effective))
        if not candidates:
            raise SourceProofError(f"missing or unavailable hourly source at {end_at_ns}")
        if any(candidate[0].revision_of is not None for candidate in candidates):
            raise SourceProofError(f"revised hourly source is not eligible for new qualification at {end_at_ns}")
        candidates.sort(key=lambda item: (item[2], item[0].record_id))
        if len(candidates) > 1:
            for previous, current in zip(candidates, candidates[1:], strict=False):
                prev_record, _, prev_at = previous
                current_record, _, current_at = current
                if (
                    current_at <= prev_at
                    or current_record.revision_of != prev_record.record_id
                    or current_record.source_id != prev_record.source_id
                ):
                    raise SourceProofError(f"conflicting or ambiguous hourly source at {end_at_ns}")
        record, close, effective = candidates[-1]
        if record.revision_of is not None and len(candidates) == 1:
            raise SourceProofError(f"revision lineage is incomplete at {end_at_ns}")
        selected_records.append(record)
        closes.append(close)
        effective_times.append(effective)
    if (
        len({record.source_id for record in selected_records}) != 1
        or len({record.venue for record in selected_records}) != 1
        or len({record.pipeline_version for record in selected_records}) != 1
    ):
        raise SourceProofError("hourly source identity changes within the qualified window")
    return tuple(selected_records), tuple(closes), tuple(effective_times)


def _chronology_hash(records: tuple[MarketRecord, ...], effective: tuple[int, ...]) -> str:
    return _canonical_hash(
        [
            {
                "record_id": record.record_id,
                "source_id": record.source_id,
                "venue": record.venue,
                "pipeline_version": record.pipeline_version,
                "source_event_at_ns": record.source_event_at_ns,
                "bar_start_ns": record.bar_start_ns,
                "bar_end_ns": record.bar_end_ns,
                "source_revision": record.source_revision,
                "revision_of": record.revision_of,
                "availability_class": record.availability_class.value,
                "available_at_ns": record.available_at_ns,
                "replay_available_at_ns": record.replay_available_at_ns,
                "effective_available_at_ns": effective[index],
                "record_fingerprint": record.record_fingerprint,
            }
            for index, record in enumerate(records)
        ]
    )


def _make_proof(
    snapshot: Any, records: tuple[MarketRecord, ...], effective: tuple[int, ...], mode: SourceAvailabilityMode
) -> FeatureSnapshotSourceProofV1:
    if any(record.bar_end_ns is None for record in records):
        raise SourceProofError("selected source record has no finalized bar end")
    return FeatureSnapshotSourceProofV1(
        schema_version=SOURCE_PROOF_VERSION,
        availability_mode=mode,
        instrument=snapshot.instrument,
        slot_at_ns=snapshot.slot_at_ns,
        computed_at_ns=snapshot.computed_at_ns,
        information_cutoff_ns=snapshot.computed_at_ns,
        snapshot_hash=snapshot.snapshot_hash(),
        feature_values_hash=feature_values_hash(snapshot.values),
        source_record_ids=tuple(record.record_id for record in records),
        source_record_fingerprints=tuple(record.record_fingerprint for record in records),
        source_content_hashes=tuple(record.content_hash for record in records),
        source_ids=tuple(record.source_id for record in records),
        source_end_at_ns=tuple(record.bar_end_ns for record in records if record.bar_end_ns is not None),
        actual_available_at_ns=tuple(record.available_at_ns for record in records),
        replay_available_at_ns=tuple(record.replay_available_at_ns for record in records),
        effective_available_at_ns=effective,
        availability_classes=tuple(record.availability_class.value for record in records),
        source_revisions=tuple(record.source_revision for record in records),
        revision_of=tuple(record.revision_of for record in records),
        chronology_hash=_chronology_hash(records, effective),
    )


def build_snapshot_with_source_proof(
    instrument: str,
    slot_at_ns: int,
    computed_at_ns: int,
    store: ParquetFeatureSourceStore,
    *,
    availability_mode: SourceAvailabilityMode = SourceAvailabilityMode.ACTUAL_SYSTEM,
) -> tuple[Any, FeatureSnapshotSourceProofV1]:
    """Build from archived records and emit a receipt bound to the complete source window."""
    from atlas.strategy.crypto_trend_24h_v1 import SNAPSHOT_DEADLINE_NS, FeatureSnapshot

    if type(store) is not ParquetFeatureSourceStore:
        raise SourceProofError("qualified V1 snapshots require the configured content-addressed archive")
    if (
        type(slot_at_ns) is not int
        or type(computed_at_ns) is not int
        or not (slot_at_ns <= computed_at_ns <= slot_at_ns + SNAPSHOT_DEADLINE_NS)
    ):
        raise SourceProofError("snapshot computation must fall from the decision slot through T+30")
    mode = SourceAvailabilityMode(availability_mode)
    archived = store.records(
        instrument=instrument,
        start_at_ns=slot_at_ns - (WINDOW_CLOSES - 1) * HOUR_NS,
        end_at_ns=slot_at_ns,
    )
    records, closes, effective = _select_window(
        archived, instrument=instrument, slot_at_ns=slot_at_ns, cutoff_ns=computed_at_ns, mode=mode
    )
    values = feature_values(closes, decision_at_ns=computed_at_ns)
    snapshot = FeatureSnapshot(
        instrument, slot_at_ns, computed_at_ns, values, tuple(record.record_id for record in records)
    )
    return snapshot, _make_proof(snapshot, records, effective, mode)


def verify_snapshot_source_proof(
    snapshot: Any, proof: FeatureSnapshotSourceProofV1, store: ParquetFeatureSourceStore
) -> bool:
    """Re-open the immutable archive and verify every source and feature value."""
    if type(store) is not ParquetFeatureSourceStore or type(proof) is not FeatureSnapshotSourceProofV1:
        return False
    try:
        records = store.records(
            instrument=snapshot.instrument,
            start_at_ns=snapshot.slot_at_ns - (WINDOW_CLOSES - 1) * HOUR_NS,
            end_at_ns=snapshot.slot_at_ns,
        )
        selected, closes, effective = _select_window(
            records,
            instrument=snapshot.instrument,
            slot_at_ns=snapshot.slot_at_ns,
            cutoff_ns=snapshot.computed_at_ns,
            mode=proof.availability_mode,
        )
        reconstructed = feature_values(closes, decision_at_ns=snapshot.computed_at_ns)
        if reconstructed != snapshot.values:
            return False
        expected = _make_proof(snapshot, selected, effective, proof.availability_mode)
        return expected == proof and expected.source_record_ids == snapshot.source_record_ids
    except Exception:
        return False
