"""Append-only Parquet/Arrow market archive and read-only DuckDB research boundary."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .models import MarketRecord, hash_payload


class EnvironmentDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class ArchiveWriteResult:
    path: str
    content_hash: str
    row_count: int
    created: bool


class ParquetArchive:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _day(ns: int) -> str:
        return datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC).strftime("%Y-%m-%d")

    def _partition(self, record: MarketRecord) -> Path:
        return (
            self.root
            / f"data_kind={record.data_kind.value}"
            / f"venue={record.venue}"
            / f"instrument={record.instrument}"
            / f"date={self._day(record.source_event_at_ns or record.received_at_ns)}"
        )

    @staticmethod
    def _stored_fingerprint(row: dict[str, object]) -> str:
        fingerprint = row.get("record_fingerprint")
        if isinstance(fingerprint, str) and fingerprint:
            return fingerprint
        legacy = dict(row)
        legacy.pop("record_fingerprint", None)
        if "payload_json" in legacy:
            legacy["payload"] = json.loads(str(legacy.pop("payload_json")))
        if "dependency_ids_json" in legacy:
            legacy["dependency_ids"] = json.loads(str(legacy.pop("dependency_ids_json")))
        return hash_payload(legacy)

    def write_batch(self, records: list[MarketRecord]) -> ArchiveWriteResult:
        if not records:
            raise ValueError("records required")
        kind = {r.data_kind for r in records}
        venue = {r.venue for r in records}
        inst = {r.instrument for r in records}
        day = {self._day(r.source_event_at_ns or r.received_at_ns) for r in records}
        if len(kind) != 1 or len(venue) != 1 or len(inst) != 1 or len(day) != 1:
            raise ValueError("batch must belong to one deterministic partition")
        rows = [r.to_dict() for r in sorted(records, key=lambda x: x.record_id)]
        payload = json.dumps(rows, sort_keys=True, separators=(",", ":"))
        h = hashlib.sha256(payload.encode()).hexdigest()
        d = self._partition(records[0])
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"part-{h}.parquet"
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise EnvironmentDependencyError("pyarrow is required for the frozen Parquet/Arrow archive") from exc
        # Content-addressed filenames prevent replacement, while this identity
        # check prevents two different logical payloads for one record_id from
        # silently coexisting in an immutable partition.
        batch_ids: dict[str, str] = {}
        for row in rows:
            fingerprint = row["record_fingerprint"]
            previous = batch_ids.get(row["record_id"])
            if previous is not None and previous != fingerprint:
                raise ValueError(f"conflicting logical identity for record_id={row['record_id']}")
            batch_ids[row["record_id"]] = fingerprint
        existing_ids: dict[str, str] = {}
        for existing in self.root.rglob("part-*.parquet"):
            table = pq.read_table(existing)
            for row in table.to_pylist():
                record_id = str(row["record_id"])
                fingerprint = self._stored_fingerprint(row)
                previous = existing_ids.get(record_id)
                if previous is not None and previous != fingerprint:
                    raise ValueError(f"conflicting logical identity for record_id={record_id}")
                existing_ids[record_id] = fingerprint
        for row in rows:
            old = existing_ids.get(row["record_id"])
            if old is not None and old != row["record_fingerprint"]:
                raise ValueError(f"conflicting logical identity for record_id={row['record_id']}")
        if path.exists():
            return ArchiveWriteResult(str(path), h, len(rows), False)
        # Payload/dependency IDs are encoded as canonical JSON strings for a stable flat Arrow schema.
        flat = []
        for row in rows:
            row = dict(row)
            row["payload_json"] = json.dumps(row.pop("payload"), sort_keys=True, separators=(",", ":"))
            row["dependency_ids_json"] = json.dumps(row.pop("dependency_ids"), separators=(",", ":"))
            flat.append(row)
        table = pa.Table.from_pylist(flat)
        fd, tmp = tempfile.mkstemp(prefix=".atlas-", suffix=".parquet", dir=d)
        os.close(fd)
        try:
            pq.write_table(table, tmp, compression="zstd")
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return ArchiveWriteResult(str(path), h, len(rows), True)

    @staticmethod
    def read_rows(path: str | Path) -> list[dict[str, object]]:
        """Read archive rows without turning the research store authoritative."""
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise EnvironmentDependencyError("pyarrow is required for the frozen Parquet/Arrow archive") from exc
        rows = []
        for row in pq.read_table(path).to_pylist():
            row["payload"] = json.loads(row.pop("payload_json"))
            row["dependency_ids"] = tuple(json.loads(row.pop("dependency_ids_json")))
            rows.append(row)
        return rows


class DuckDBResearchCatalog:
    """Research analytics only. Never authoritative live state."""

    def query_parquet(self, glob_path: str, sql_where: str = "TRUE"):
        try:
            import duckdb
        except ImportError as exc:
            raise EnvironmentDependencyError("duckdb is required for research analytics") from exc
        con = duckdb.connect(":memory:")
        try:
            return con.execute(f"SELECT * FROM read_parquet(?) WHERE {sql_where}", [glob_path]).fetchall()
        finally:
            con.close()
