"""Narrow, fail-closed bridges for forward S3 market evidence.

This module does not decide that Bybit trades are complete. It reconstructs
only the exact S32 WebSocket trade archive/index contract, exposes a cutoff
view of the existing sequence-valid book, and reports S3 warmup readiness
from immutable typed evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, sha256_json, sha256_ref, strict_fields, timestamp
from ..instruments import InstrumentKeyV2, ProductContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..strategies.s1_trend import EventGate, EventState, ExecutableQuote
from ..strategies.s3_mean_reversion import (
    AR_OBSERVATION_COUNT,
    BBO_MAX_AGE_NS,
    SOURCE_HEALTH_MAX_AGE_NS,
    STANDARDIZATION_COUNT,
    CausalTradeV2,
    ResidualObservationV2,
    TradeVwapSnapshotV2,
    validate_historical_residual_v2,
)
from .bars import BarIntervalV2, CausalBarV2
from .health import PublicSourceHealthV2, PublicSourceStateV2
from .microstructure import AvailabilityViewV2, BookStateV2, SequenceValidBookV2
from .public_archive_extents import read_public_chunk
from .public_evidence_checkpoint import (
    BOOK_CHECKPOINT_TYPE,
    CONTINUITY_CHECKPOINT_TYPE,
    validate_book_checkpoint,
    validate_continuity_checkpoint,
)
from .public_stream_continuity import PublicStreamContinuityStateV1
from .raw import AvailabilityClassV2, RawObservationV2

BYBIT_PUBLIC_WS_SOURCE_ID_V1 = "BYBIT_PUBLIC_WS"
BYBIT_TRADE_TRANSLATION_V1 = "bybit-public-ws-trade-v1"
TRADE_INDEX_TYPE_V1 = "PublicStreamTradeObservationIndexV1"
GENERIC_OBSERVATION_INDEX_TYPE_V2 = "PublicObservationIndexV2"
CONTINUITY_REPORT_TYPE_V1 = "PublicStreamContinuityReportV1"
CONTINUITY_STATE_TYPE_V1 = "PublicStreamContinuityStateV1"
STREAM_HEALTH_TYPE_V1 = "PublicStreamSourceHealthV1"
PUBLIC_STREAM_STALE_NS_V1 = 30_000_000_000
MAX_ARCHIVE_FILES = 20_000
MAX_ARCHIVE_ROWS = 2_000_000
MAX_RECONSTRUCTED_TRADES = 100_000
MAX_ACTIVE_TRADE_ARCHIVE_FILES = 64
MAX_ACTIVE_TRADE_ARCHIVE_ROWS = 16_384
MAX_REPORTED_GAP_OPENS = 1_000


@dataclass(frozen=True)
class S3NativeComputationContextV1:
    """Fixed market cutoff and honest timing for one S34 derived computation.

    ``evidence_cutoff_ns`` is the immutable source boundary. The computation
    and its derived output may occur later, but never after the consumer's
    fixed decision deadline. Persisted artifact indexes can use a later
    availability timestamp, provided it remains at or before that deadline.
    """

    evidence_cutoff_ns: int
    computation_started_ns: int
    computation_finished_ns: int
    consumer_deadline_ns: int

    def __post_init__(self) -> None:
        for name in (
            "evidence_cutoff_ns", "computation_started_ns", "computation_finished_ns",
            "consumer_deadline_ns",
        ):
            timestamp(getattr(self, name), field=f"s3_computation.{name}")
        if not (
            self.evidence_cutoff_ns <= self.computation_started_ns
            <= self.computation_finished_ns <= self.consumer_deadline_ns
        ):
            raise ValueError("S3 computation timing violates the fixed causal deadline")

    @property
    def produced_at_ns(self) -> int:
        """The earliest honest availability of the completed derived result."""
        return self.computation_finished_ns

    def to_dict(self) -> dict[str, int]:
        return {
            "schema_version": 1,
            "evidence_cutoff_ns": self.evidence_cutoff_ns,
            "computation_started_ns": self.computation_started_ns,
            "computation_finished_ns": self.computation_finished_ns,
            "produced_at_ns": self.produced_at_ns,
            "consumer_deadline_ns": self.consumer_deadline_ns,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> S3NativeComputationContextV1:
        fields = {
            "schema_version", "evidence_cutoff_ns", "computation_started_ns",
            "computation_finished_ns", "produced_at_ns", "consumer_deadline_ns",
        }
        body = strict_fields(value, expected=fields, required=fields, name=cls.__name__)
        if type(body["schema_version"]) is not int or body["schema_version"] != 1:
            raise ValueError("unsupported S3 computation context schema")
        parsed = cls(
            body["evidence_cutoff_ns"], body["computation_started_ns"],
            body["computation_finished_ns"], body["consumer_deadline_ns"],
        )
        if body["produced_at_ns"] != parsed.produced_at_ns:
            raise ValueError("S3 produced time must equal computation completion")
        return parsed


def _validate_computation_context(
    context: S3NativeComputationContextV1 | None,
    *,
    cutoff_ns: int,
) -> None:
    if context is not None and context.evidence_cutoff_ns != cutoff_ns:
        raise ValueError("S3 computation context cannot rebase the fixed evidence cutoff")


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON object key")
        result[name] = value
    return result


def _json_object(raw: bytes) -> Mapping[str, Any]:
    value = json.loads(raw, object_pairs_hook=_strict_json_object)
    if not isinstance(value, Mapping):
        raise ValueError("trade payload must be one JSON object")
    if canonical_json(value).encode("utf-8") != raw:
        raise ValueError("trade payload is not the exact canonical archived row")
    return value


def _valid_report_context(
    repository: OpsRepository,
    *,
    product: ProductContractV2,
    cutoff_ns: int,
    report_ref: str,
    channel: str,
) -> tuple[Mapping[str, Any], PublicSourceHealthV2, PublicStreamContinuityStateV1] | None:
    """Load the exact S32 report, source health and continuity state prefix."""
    if (product.effective_at_ns > cutoff_ns or product.observed_at_ns > cutoff_ns
            or product.available_at_ns > cutoff_ns):
        return None
    sha256_ref(report_ref, field="continuity_report_ref")
    report_entry = repository.get_artifact(report_ref)
    if (report_entry is None or report_entry.artifact_type != CONTINUITY_REPORT_TYPE_V1
            or report_entry.content_hash != report_ref or report_entry.available_at_ns > cutoff_ns):
        return None
    report = report_entry.metadata.get("report")
    if not isinstance(report, Mapping):
        return None
    try:
        report_body = dict(report)
        report_instrument = InstrumentKeyV2.from_dict(report_body["instrument"])
        report_body_hash = sha256_json({"artifact_type": CONTINUITY_REPORT_TYPE_V1, "report": report_body})
        report_as_of = report_body["as_of_ns"]
        report_health_ref = report_body["source_health_ref"]
        state_ref = report_entry.metadata["state_ref"]
        timestamp(report_as_of, field="continuity_report.as_of_ns")
        sha256_ref(report_health_ref, field="continuity_report.source_health_ref")
        sha256_ref(state_ref, field="continuity_report.state_ref")
        if "storage_version" in report_entry.metadata:
            from .public_evidence_checkpoint import resolve_report_transport

            resolve_report_transport(repository, report_entry.metadata, available_at_ns=report_entry.available_at_ns)
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return None
    if (report_body_hash != report_ref or report_entry.available_at_ns > cutoff_ns
            or report_as_of > report_entry.available_at_ns
            or (report_entry.available_at_ns != report_as_of
                and report_entry.metadata.get("storage_version") != "PUBLIC_CONTINUITY_REPORT_INDEX_V2")
            or report_entry.metadata.get("source_health_ref") != report_health_ref
            or report_as_of > cutoff_ns or report_body.get("schema_version") != 1
            or report_instrument != product.key or report_body.get("contract_revision") != product.key.contract_revision
            or report_body.get("source_id") != BYBIT_PUBLIC_WS_SOURCE_ID_V1
            or report_body.get("channel") != channel
            or report_body.get("metadata_ref") != product.metadata_ref
            or report_body.get("metadata_current") is not True
            or report_body.get("source_current") is not True
            or report_body.get("source_health_epoch_match") is not True
            or report_body.get("gap_count") != 0
            or report_body.get("trade_completeness_proven") is not False
            or report_body.get("strategy_input_qualified") is not False):
        return None
    if cutoff_ns - report_as_of > PUBLIC_STREAM_STALE_NS_V1:
        return None

    health_entry = repository.get_artifact(report_health_ref)
    if (health_entry is None or health_entry.artifact_type != STREAM_HEALTH_TYPE_V1
            or health_entry.content_hash != report_health_ref or health_entry.available_at_ns > cutoff_ns):
        return None
    health_body = health_entry.metadata.get("health")
    if not isinstance(health_body, Mapping):
        return None
    try:
        health = PublicSourceHealthV2.from_dict(health_body)
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return None
    if (health.content_hash != report_health_ref or health.source_id != BYBIT_PUBLIC_WS_SOURCE_ID_V1
            or health.state != PublicSourceStateV2.HEALTHY_CURRENT
            or health.observed_at_ns > cutoff_ns or health.available_at_ns > cutoff_ns
            or cutoff_ns - health.observed_at_ns > PUBLIC_STREAM_STALE_NS_V1
            or report_body.get("source_health_epoch_match") is not True):
        return None

    state_entry = repository.get_artifact(state_ref)
    if (state_entry is None or state_entry.artifact_type not in (CONTINUITY_STATE_TYPE_V1, CONTINUITY_CHECKPOINT_TYPE)
            or state_entry.content_hash != state_ref or state_entry.available_at_ns > cutoff_ns):
        return None
    try:
        state = validate_continuity_checkpoint(repository, state_entry)
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return None
    if (state.instrument != product.key
            or state.source_id != BYBIT_PUBLIC_WS_SOURCE_ID_V1 or state.channel != channel
            or state.metadata_ref != product.metadata_ref
            or state.epoch_id != report_body.get("epoch_id")
            or state.recovery_epoch != report_body.get("recovery_epoch")
            or state.current_recovery_ref != report_body.get("current_recovery_ref")
            or state.gap_count != 0 or state.transport_disconnected
            or state.last_transport_receipt_at_ns is None
            or state.last_transport_receipt_at_ns > cutoff_ns
            or cutoff_ns - state.last_transport_receipt_at_ns > PUBLIC_STREAM_STALE_NS_V1):
        return None
    return report_body, health, state


@dataclass(frozen=True)
class S3ForwardTradeEvidenceV1:
    """Observed WS trade rows plus their explicit, non-completeness evidence."""

    key: InstrumentKeyV2
    cutoff_ns: int
    trades: tuple[CausalTradeV2, ...]
    trade_refs: tuple[str, ...]
    continuity_report_ref: str | None
    source_health_ref: str | None
    recovery_epoch: int | None
    observed_trade_count: int
    trade_completeness_proven: bool
    status: str
    reason_codes: tuple[str, ...]
    computation_context: S3NativeComputationContextV1 | None = None
    continuity_report_as_of_ns: int | None = None
    source_health_observed_at_ns: int | None = None
    source_health_available_at_ns: int | None = None

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="s3_trade_evidence.cutoff_ns")
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("forward trade evidence requires exact InstrumentKeyV2")
        if self.trade_completeness_proven is not False:
            raise ValueError("S32 Bybit WS evidence cannot claim trade completeness")
        if self.status not in {"OBSERVED", "NOT_ESTIMABLE", "TEST GATE", "BLOCKED BY ENVIRONMENT"}:
            raise ValueError("unsupported S3 forward trade evidence status")
        if type(self.observed_trade_count) is not int or self.observed_trade_count < 0:
            raise ValueError("observed trade count must be nonnegative")
        for ref in self.trade_refs:
            sha256_ref(ref, field="trade_ref")
        if tuple(sorted(set(self.trade_refs))) != self.trade_refs:
            raise ValueError("trade refs must be sorted and unique")
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise ValueError("reason codes must be sorted and unique")
        if self.continuity_report_ref is not None:
            sha256_ref(self.continuity_report_ref, field="continuity_report_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        if self.recovery_epoch is not None and (type(self.recovery_epoch) is not int or self.recovery_epoch < 0):
            raise ValueError("recovery epoch must be nonnegative")
        _validate_computation_context(self.computation_context, cutoff_ns=self.cutoff_ns)
        for name in (
            "continuity_report_as_of_ns", "source_health_observed_at_ns", "source_health_available_at_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=f"s3_trade_evidence.{name}")
                if value > self.cutoff_ns:
                    raise ValueError(f"{name} exceeds the fixed evidence cutoff")

    def to_dict(self) -> dict[str, Any]:
        body = {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "cutoff_ns": self.cutoff_ns,
            "trades": [trade.to_dict() for trade in self.trades],
            "trade_refs": list(self.trade_refs),
            "continuity_report_ref": self.continuity_report_ref,
            "source_health_ref": self.source_health_ref,
            "recovery_epoch": self.recovery_epoch,
            "observed_trade_count": self.observed_trade_count,
            "trade_completeness_proven": False,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
        }
        if self.computation_context is not None:
            body.update({
                "computation_context": self.computation_context.to_dict(),
                "produced_at_ns": self.computation_context.produced_at_ns,
                "continuity_report_as_of_ns": self.continuity_report_as_of_ns,
                "source_health_observed_at_ns": self.source_health_observed_at_ns,
                "source_health_available_at_ns": self.source_health_available_at_ns,
            })
        return body

    def with_computation_context(
        self,
        context: S3NativeComputationContextV1,
        *,
        repository: OpsRepository | None = None,
    ) -> S3ForwardTradeEvidenceV1:
        """Return this fixed-cutoff result with actual computation timing."""
        _validate_computation_context(context, cutoff_ns=self.cutoff_ns)
        report_as_of = self.continuity_report_as_of_ns
        health_observed = self.source_health_observed_at_ns
        health_available = self.source_health_available_at_ns
        if repository is not None:
            report_as_of, health_observed, health_available = _persisted_source_times(
                repository, report_ref=self.continuity_report_ref,
                health_ref=self.source_health_ref, cutoff_ns=self.cutoff_ns,
            )
        return replace(
            self, computation_context=context,
            continuity_report_as_of_ns=report_as_of,
            source_health_observed_at_ns=health_observed,
            source_health_available_at_ns=health_available,
        )


def _reconstruct_s3_stream_trade_evidence_at_cutoff(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    product: ProductContractV2,
    cutoff_ns: int,
    continuity_report_ref: str,
    limit: int = MAX_RECONSTRUCTED_TRADES,
) -> S3ForwardTradeEvidenceV1:
    """Reconstruct exact S32 Bybit WS trade rows without upgrading coverage.

    Only ``PublicStreamTradeObservationIndexV1`` entries are admitted. The
    generic public-observation reconstruction path remains unchanged. Trade
    IDs remain identities and are never treated as replay cursors.
    """
    timestamp(cutoff_ns, field="cutoff_ns")
    if not isinstance(product, ProductContractV2):
        raise ValueError("S3 stream reconstruction requires ProductContractV2")
    if (product.key.venue.value != "BYBIT" or product.key.contract_revision == ""
            or product.effective_at_ns > cutoff_ns or product.observed_at_ns > cutoff_ns
            or product.available_at_ns > cutoff_ns):
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), None, None, None, 0, False, "NOT_ESTIMABLE",
            ("PRODUCT_CONTRACT_NOT_AVAILABLE_AT_CUTOFF",),
        )
    if type(limit) is not int or not 1 <= limit <= MAX_RECONSTRUCTED_TRADES:
        raise ValueError("S3 stream reconstruction limit is outside its bound")
    channel = f"publicTrade.{product.key.native_symbol}"
    context = _valid_report_context(
        repository, product=product, cutoff_ns=cutoff_ns,
        report_ref=continuity_report_ref, channel=channel,
    )
    if context is None:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, None, None, 0, False,
            "NOT_ESTIMABLE", ("S32_CONTINUITY_OR_CURRENT_HEALTH_UNAVAILABLE",),
        )
    report, health, state = context
    # The exact current report confirms that the local stream evidence is
    # accepted and current. It deliberately does not repair or certify older
    # coverage; Bybit `i` is only a trade identity.
    expected_trade_id_semantics = (
        "i is a trade identity; exact duplicate comparison uses canonical per-trade row hash when supplied; "
        "seq may be shared across grouped records"
    )
    expected_recovery_semantics = (
        "no declared cursor or historical repair; disconnect/reconnect intervals remain unsupported"
    )
    if (report.get("trade_id_semantics") != expected_trade_id_semantics
            or report.get("trade_recovery_semantics") != expected_recovery_semantics
            or "TRADE_COMPLETENESS_UNSUPPORTED_NO_DECLARED_CURSOR_OR_HISTORY_REPAIR"
            not in report.get("reasons", ())):
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S32_TRADE_SEMANTICS_NOT_ACCEPTED",),
        )
    if _continuity_invalidation_between(
        repository, product.key, channel=channel,
        after_ns=int(report["as_of_ns"]), through_ns=cutoff_ns,
    ) is not None:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_CONTINUITY_GAP_AT_CUTOFF",),
        )

    root = Path(archive_root)
    if not root.exists() or not root.is_dir():
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "TEST GATE", ("S3_FORWARD_TRADE_ARCHIVE_UNAVAILABLE",),
        )
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "BLOCKED BY ENVIRONMENT",
            ("PARQUET_READER_UNAVAILABLE",),
        )

    report_entry = repository.get_artifact(continuity_report_ref)
    state_entry = repository.get_artifact(str(report_entry.metadata["state_ref"])) if report_entry is not None else None
    legacy_reconstruction = state_entry is not None and state_entry.artifact_type == CONTINUITY_STATE_TYPE_V1
    day_start = cutoff_ns - cutoff_ns % 86_400_000_000_000
    entries = repository.public_stream_trade_entries(product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
        event_from_ns=day_start, cutoff_ns=cutoff_ns, limit=limit)
    if len(entries) > limit:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, len(entries), False, "NOT_ESTIMABLE", ("S3_TRADE_ACTIVE_INDEX_BOUND_EXCEEDED",),
        )
    if repository.generic_public_stream_observation_exists(product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                                                          cutoff_ns=cutoff_ns):
        return S3ForwardTradeEvidenceV1(product.key, cutoff_ns, (), (), continuity_report_ref,
            health.content_hash, state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_WS_TRADE_INDEX_TYPE_INVALID",))
    chunk_ids = {entry.metadata.get("archive_chunk_id") for entry in entries}
    try:
        for chunk_id in chunk_ids:
            if not isinstance(chunk_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", chunk_id):
                raise ValueError("trade archive chunk missing")
    except (TypeError, ValueError):
        return S3ForwardTradeEvidenceV1(product.key, cutoff_ns, (), (), continuity_report_ref,
            health.content_hash, state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_ARCHIVE_OR_INDEX_INVALID",))
    if len(chunk_ids) > MAX_ACTIVE_TRADE_ARCHIVE_FILES:
        return S3ForwardTradeEvidenceV1(product.key, cutoff_ns, (), (), continuity_report_ref,
            health.content_hash, state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_ARCHIVE_FILE_BOUND_EXCEEDED",))
    paths = [root / (chunk_id + ".parquet") for chunk_id in sorted(str(value) for value in chunk_ids)]
    # Preserve the accepted legacy offline archive audit. The installed writer
    # produces V2 compact state and uses only the bounded indexed path above.
    if legacy_reconstruction:
        paths = sorted(path for path in root.glob("*.parquet") if path.is_file() and not path.is_symlink())
        if len(paths) > MAX_ARCHIVE_FILES:
            return S3ForwardTradeEvidenceV1(product.key, cutoff_ns, (), (), continuity_report_ref,
                health.content_hash, state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_ARCHIVE_FILE_BOUND_EXCEEDED",))
    required_columns = {
        "record_id", "instrument_revision", "source_id", "event_type", "event_at_ns",
        "published_at_ns", "received_at_ns", "ingested_at_ns", "available_at_ns",
        "raw_payload_hash", "observation_json", "raw_payload_bytes", "archive_record_kind",
    }
    raw_rows: dict[str, tuple[RawObservationV2, bytes, str]] = {}
    examined = 0
    archive_conflict = False
    archive_error = False
    for path in paths:
        try:
            if legacy_reconstruction:
                if path.is_symlink() or not path.is_file():
                    raise ValueError("trade archive absent or symbolic")
                parquet = pq.ParquetFile(path)
                if not required_columns.issubset(set(parquet.schema.names)):
                    continue
                batches = parquet.iter_batches(columns=sorted(required_columns), batch_size=512)
            else:
                table = read_public_chunk(repository, root, path.stem)
                if not required_columns.issubset(set(table.schema.names)):
                    raise ValueError("trade archive column contract invalid")
                batches = iter(table.select(sorted(required_columns)).to_batches(max_chunksize=512))
            for batch in batches:
                for row in batch.to_pylist():
                    examined += 1
                    if examined > (MAX_ARCHIVE_ROWS if legacy_reconstruction else MAX_ACTIVE_TRADE_ARCHIVE_ROWS):
                        return S3ForwardTradeEvidenceV1(
                            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
                            state.recovery_epoch, 0, False, "NOT_ESTIMABLE",
                            ("S3_TRADE_ARCHIVE_ROW_BOUND_EXCEEDED",),
                        )
                    if (row.get("instrument_revision") != product.key.contract_revision
                            or row.get("event_type") != "TRADE"
                            or row.get("source_id") != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                            or row.get("archive_record_kind") != "PUBLIC_OBSERVATION"
                            or type(row.get("available_at_ns")) is not int
                            or row.get("available_at_ns") > cutoff_ns):
                        continue
                    payload_bytes = row.get("raw_payload_bytes")
                    if not isinstance(payload_bytes, bytes):
                        archive_error = True
                        continue
                    try:
                        observation_json = json.loads(
                            row["observation_json"], object_pairs_hook=_strict_json_object,
                        )
                        observation = RawObservationV2.from_dict(observation_json)
                    except (ArithmeticError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                        archive_error = True
                        continue
                    if (observation.record_id != row.get("record_id")
                            or observation.instrument_revision != row.get("instrument_revision")
                            or observation.source_id != row.get("source_id")
                            or observation.event_type != row.get("event_type")
                            or observation.event_at_ns != row.get("event_at_ns")
                            or observation.published_at_ns != row.get("published_at_ns")
                            or observation.received_at_ns != row.get("received_at_ns")
                            or observation.ingested_at_ns != row.get("ingested_at_ns")
                            or observation.available_at_ns != row.get("available_at_ns")
                            or observation.raw_payload_hash != row.get("raw_payload_hash")
                            or hashlib.sha256(payload_bytes).hexdigest() != observation.raw_payload_hash):
                        archive_error = True
                        continue
                    prior = raw_rows.get(observation.record_id)
                    if prior is not None and (prior[0].raw_payload_hash != observation.raw_payload_hash
                                              or prior[1] != payload_bytes):
                        archive_conflict = True
                    raw_rows[observation.record_id] = (observation, payload_bytes, path.stem)
        except (OSError, ValueError, TypeError, KeyError):
            archive_error = True


    index_by_record: dict[str, Any] = {}
    invalid_index = False
    for entry in entries:
        metadata = entry.metadata
        if entry.available_at_ns > cutoff_ns:
            continue
        if (metadata.get("instrument_revision") != product.key.contract_revision
                or metadata.get("source_id") != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                or metadata.get("event_type") != "TRADE"
                or metadata.get("instrument_key_json") != product.key.to_canonical_json()):
            continue
        record_id = metadata.get("record_id")
        if not isinstance(record_id, str):
            invalid_index = True
            continue
        expected_ref = sha256_json({"artifact_type": TRADE_INDEX_TYPE_V1, "record_id": record_id})
        archive_row = raw_rows.get(record_id)
        if (entry.artifact_ref != expected_ref or entry.artifact_type != TRADE_INDEX_TYPE_V1
                or archive_row is None):
            invalid_index = True
            continue
        observation, raw_bytes, chunk_id = archive_row
        if (entry.content_hash != observation.content_hash
                or entry.created_at_ns != observation.received_at_ns
                or entry.available_at_ns != observation.available_at_ns
                or metadata.get("archive_chunk_id") != chunk_id
                or metadata.get("record_id") != observation.record_id
                or metadata.get("source_id") != observation.source_id
                or metadata.get("event_type") != observation.event_type
                or metadata.get("instrument_revision") != observation.instrument_revision
                or metadata.get("instrument_key_json") != product.key.to_canonical_json()
                or metadata.get("event_at_ns") != observation.event_at_ns
                or metadata.get("published_at_ns") != observation.published_at_ns
                or metadata.get("translation_version") != observation.translation_version
                or metadata.get("revision_of") != observation.revision_of
                or tuple(metadata.get("quality_flags", ())) != observation.quality_flags
                or metadata.get("availability_class") != observation.availability_class.value
                or metadata.get("replay_available_at_ns") != observation.replay_available_at_ns
                or metadata.get("raw_payload_hash") != observation.raw_payload_hash
                or observation.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or observation.replay_available_at_ns is not None
                or observation.translation_version != BYBIT_TRADE_TRANSLATION_V1
                or "TRADE_COMPLETENESS_UNPROVEN" not in observation.quality_flags
                or observation.event_at_ns is None or observation.event_at_ns > cutoff_ns
                or observation.received_at_ns > cutoff_ns or observation.available_at_ns > cutoff_ns
                or (observation.published_at_ns is not None and observation.published_at_ns > cutoff_ns)):
            invalid_index = True
            continue
        index_by_record[record_id] = (entry, observation, raw_bytes)

    if archive_conflict:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_ARCHIVE_PAYLOAD_CONFLICT",),
        )
    if archive_error or invalid_index:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_TRADE_ARCHIVE_OR_INDEX_INVALID",),
        )

    reconstructed: list[CausalTradeV2] = []
    for _record_id, (entry, observation, raw_bytes) in sorted(index_by_record.items()):
        try:
            payload = _json_object(raw_bytes)
            trade_id = payload.get("i")
            symbol = payload.get("s")
            event_ms = payload.get("T")
            if isinstance(event_ms, bool) or not isinstance(event_ms, (int, str)):
                raise ValueError("Bybit matched timestamp is missing")
            if isinstance(event_ms, str) and (not event_ms.isdecimal()):
                raise ValueError("Bybit matched timestamp is invalid")
            event_at_ns = int(event_ms) * 1_000_000
            if (symbol != product.key.native_symbol or not isinstance(trade_id, (str, int))
                    or isinstance(trade_id, bool) or str(trade_id) != str(observation.sequence)
                    or event_at_ns != observation.event_at_ns):
                raise ValueError("Bybit trade identity or event time does not match its raw observation")
            side = {"Buy": "BUY", "Sell": "SELL"}.get(str(payload.get("S")), "UNKNOWN")
            price = Decimal(str(payload["p"]))
            quantity = Decimal(str(payload["v"]))
            trade = CausalTradeV2(
                product.key, entry.artifact_ref, BYBIT_PUBLIC_WS_SOURCE_ID_V1, str(trade_id),
                observation.event_at_ns, observation.received_at_ns, observation.available_at_ns,
                price, quantity, side, AvailabilityClassV2.ACTUAL_SYSTEM,
            )
            reconstructed.append(trade)
        except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return S3ForwardTradeEvidenceV1(
                product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
                state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("S3_WS_TRADE_PAYLOAD_INVALID",),
            )

    reconstructed.sort(key=lambda item: (item.event_at_ns, item.trade_id, item.raw_observation_ref))
    if len(reconstructed) > limit:
        reconstructed = reconstructed[-limit:]
    if not reconstructed:
        return S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), continuity_report_ref, health.content_hash,
            state.recovery_epoch, 0, False, "NOT_ESTIMABLE", ("NO_EXACT_CUTOFF_AVAILABLE_WS_TRADES",),
        )
    selected_refs = tuple(sorted({trade.raw_observation_ref for trade in reconstructed}))
    reasons = ("BYBIT_TRADE_COMPLETENESS_UNPROVEN",)
    return S3ForwardTradeEvidenceV1(
        product.key, cutoff_ns, tuple(reconstructed), selected_refs, continuity_report_ref,
        health.content_hash, state.recovery_epoch, len(reconstructed), False, "TEST GATE", reasons,
    )


def _persisted_source_times(
    repository: OpsRepository,
    *,
    report_ref: str | None,
    health_ref: str | None,
    cutoff_ns: int,
) -> tuple[int | None, int | None, int | None]:
    report_as_of: int | None = None
    health_observed: int | None = None
    health_available: int | None = None
    report_entry = repository.get_artifact(report_ref) if report_ref is not None else None
    report = report_entry.metadata.get("report") if report_entry is not None else None
    if (report_entry is not None and report_entry.artifact_type == CONTINUITY_REPORT_TYPE_V1
            and report_entry.content_hash == report_ref and isinstance(report, Mapping)):
        candidate = report.get("as_of_ns")
        if type(candidate) is int and candidate <= cutoff_ns:
            report_as_of = candidate
    health_entry = repository.get_artifact(health_ref) if health_ref is not None else None
    health_body = health_entry.metadata.get("health") if health_entry is not None else None
    if (health_entry is not None and health_entry.artifact_type == STREAM_HEALTH_TYPE_V1
            and health_entry.content_hash == health_ref and isinstance(health_body, Mapping)):
        try:
            health = PublicSourceHealthV2.from_dict(health_body)
        except (ArithmeticError, KeyError, TypeError, ValueError):
            health = None
        if health is not None and health.content_hash == health_ref:
            if health.observed_at_ns <= cutoff_ns:
                health_observed = health.observed_at_ns
            if health.available_at_ns <= cutoff_ns:
                health_available = health.available_at_ns
    return report_as_of, health_observed, health_available


def reconstruct_s3_stream_trade_evidence(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    product: ProductContractV2,
    cutoff_ns: int,
    continuity_report_ref: str,
    limit: int = MAX_RECONSTRUCTED_TRADES,
    computation_context: S3NativeComputationContextV1 | None = None,
) -> S3ForwardTradeEvidenceV1:
    """Reconstruct source rows at ``cutoff_ns`` and optionally record S34 timing.

    The context only timestamps the derived validation. It cannot widen the
    archive/index/report resolver cutoff supplied as ``cutoff_ns``.
    """
    _validate_computation_context(computation_context, cutoff_ns=cutoff_ns)
    result = _reconstruct_s3_stream_trade_evidence_at_cutoff(
        repository, archive_root, product=product, cutoff_ns=cutoff_ns,
        continuity_report_ref=continuity_report_ref, limit=limit,
    )
    if computation_context is None:
        return result
    report_as_of, health_observed, health_available = _persisted_source_times(
        repository, report_ref=result.continuity_report_ref,
        health_ref=result.source_health_ref, cutoff_ns=cutoff_ns,
    )
    return replace(
        result, computation_context=computation_context,
        continuity_report_as_of_ns=report_as_of,
        source_health_observed_at_ns=health_observed,
        source_health_available_at_ns=health_available,
    )


@dataclass(frozen=True)
class S3QuoteBridgeResultV1:
    """Serializable cutoff-bound projection of the accepted sequence-valid book."""

    key: InstrumentKeyV2
    contract_revision: str
    cutoff_ns: int
    continuity_report_ref: str
    source_health_ref: str
    sequence_state: str
    recovery_epoch: int
    sequence_feature_ref: str
    bid: str | None
    ask: str | None
    observed_at_ns: int | None
    available_at_ns: int | None
    bbo_age_ns: int | None
    input_refs: tuple[str, ...]
    status: str
    reason_code: str | None
    evidence_ref: str
    computation_context: S3NativeComputationContextV1 | None = None
    continuity_report_as_of_ns: int | None = None
    source_health_observed_at_ns: int | None = None
    source_health_available_at_ns: int | None = None

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="quote_bridge.cutoff_ns")
        for name in ("continuity_report_ref", "source_health_ref", "sequence_feature_ref", "evidence_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.contract_revision != self.key.contract_revision:
            raise ValueError("quote contract revision differs from full instrument identity")
        _validate_computation_context(self.computation_context, cutoff_ns=self.cutoff_ns)
        for name in (
            "continuity_report_as_of_ns", "source_health_observed_at_ns", "source_health_available_at_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=f"quote_bridge.{name}")
                if value > self.cutoff_ns:
                    raise ValueError(f"{name} exceeds the fixed evidence cutoff")
        if type(self.recovery_epoch) is not int or self.recovery_epoch < 0:
            raise ValueError("quote recovery epoch must be nonnegative")
        for ref in self.input_refs:
            sha256_ref(ref, field="quote input ref")
        if tuple(sorted(set(self.input_refs))) != self.input_refs:
            raise ValueError("quote input refs must be sorted and unique")
        if self.status not in {"AVAILABLE", "NOT_ESTIMABLE"}:
            raise ValueError("unsupported sequence book quote status")
        if self.status == "AVAILABLE":
            if (self.reason_code is not None or self.bid is None or self.ask is None
                    or self.observed_at_ns is None or self.available_at_ns is None
                    or self.bbo_age_ns is None):
                raise ValueError("available quote evidence is incomplete")
            if Decimal(self.bid) >= Decimal(self.ask):
                raise ValueError("S3 quote bridge requires bid strictly below ask")
            timestamp(self.observed_at_ns, field="quote.observed_at_ns")
            timestamp(self.available_at_ns, field="quote.available_at_ns")
            if self.available_at_ns > self.cutoff_ns or self.observed_at_ns > self.available_at_ns:
                raise ValueError("quote evidence is not available at its cutoff")
            if self.bbo_age_ns != self.cutoff_ns - self.observed_at_ns or self.bbo_age_ns > BBO_MAX_AGE_NS:
                raise ValueError("quote evidence violates S3's one-second BBO limit")
        elif not self.reason_code:
            raise ValueError("unavailable quote requires a reason code")
        if (self.bid is None) != (self.ask is None):
            raise ValueError("quote bid and ask must be present together")

    def _body(self) -> dict[str, Any]:
        body = {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "contract_revision": self.contract_revision,
            "cutoff_ns": self.cutoff_ns,
            "continuity_report_ref": self.continuity_report_ref,
            "source_health_ref": self.source_health_ref,
            "sequence_state": self.sequence_state,
            "recovery_epoch": self.recovery_epoch,
            "sequence_feature_ref": self.sequence_feature_ref,
            "bid": self.bid,
            "ask": self.ask,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "bbo_age_ns": self.bbo_age_ns,
            "input_refs": list(self.input_refs),
            "status": self.status,
            "reason_code": self.reason_code,
        }
        if self.computation_context is not None:
            body.update({
                "computation_context": self.computation_context.to_dict(),
                "produced_at_ns": self.computation_context.produced_at_ns,
                "source_available_at_ns": self.available_at_ns,
                "continuity_report_as_of_ns": self.continuity_report_as_of_ns,
                "source_health_observed_at_ns": self.source_health_observed_at_ns,
                "source_health_available_at_ns": self.source_health_available_at_ns,
            })
        return body

    def to_dict(self) -> dict[str, Any]:
        body = self._body()
        if sha256_json({"artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": body}) != self.evidence_ref:
            raise ValueError("quote evidence ref does not match its serialized body")
        return {**body, "evidence_ref": self.evidence_ref}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> S3QuoteBridgeResultV1:
        base_fields = {
            "schema_version", "key", "contract_revision", "cutoff_ns", "continuity_report_ref",
            "source_health_ref", "sequence_state", "recovery_epoch", "sequence_feature_ref", "bid", "ask",
            "observed_at_ns", "available_at_ns", "bbo_age_ns", "input_refs", "status", "reason_code",
            "evidence_ref",
        }
        timed_fields = {
            "computation_context", "produced_at_ns", "source_available_at_ns",
            "continuity_report_as_of_ns", "source_health_observed_at_ns", "source_health_available_at_ns",
        }
        body = strict_fields(
            value, expected=base_fields | timed_fields, required=base_fields, name=cls.__name__,
        )
        present_timing = set(body) & timed_fields
        if present_timing and present_timing != timed_fields:
            raise ValueError("timed S3 quote bridge must carry its complete timing fields")
        if type(body["schema_version"]) is not int or body["schema_version"] != 1:
            raise ValueError("unsupported S3 quote bridge schema")
        if not isinstance(body["key"], Mapping) or not isinstance(body["input_refs"], (tuple, list)):
            raise ValueError("S3 quote bridge identity or refs have invalid wire types")
        context = (
            S3NativeComputationContextV1.from_dict(body["computation_context"])
            if present_timing else None
        )
        if present_timing:
            if context is None:
                raise ValueError("timed S3 quote bridge has no computation context")
            if body["produced_at_ns"] != context.produced_at_ns:
                raise ValueError("quote bridge produced time differs from computation completion")
            if body["source_available_at_ns"] != body["available_at_ns"]:
                raise ValueError("quote bridge source availability alias is inconsistent")
        parsed = cls(
            InstrumentKeyV2.from_dict(body["key"]), str(body["contract_revision"]), body["cutoff_ns"],
            str(body["continuity_report_ref"]), str(body["source_health_ref"]), str(body["sequence_state"]),
            body["recovery_epoch"], str(body["sequence_feature_ref"]),
            str(body["bid"]) if body["bid"] is not None else None,
            str(body["ask"]) if body["ask"] is not None else None,
            body["observed_at_ns"], body["available_at_ns"], body["bbo_age_ns"],
            tuple(body["input_refs"]), str(body["status"]),
            str(body["reason_code"]) if body["reason_code"] is not None else None,
            str(body["evidence_ref"]),
            context,
            body.get("continuity_report_as_of_ns"),
            body.get("source_health_observed_at_ns"),
            body.get("source_health_available_at_ns"),
        )
        parsed.to_dict()
        return parsed

    def with_computation_context(
        self,
        context: S3NativeComputationContextV1,
        *,
        repository: OpsRepository | None = None,
    ) -> S3QuoteBridgeResultV1:
        """Attach production timing while retaining source quote times."""
        _validate_computation_context(context, cutoff_ns=self.cutoff_ns)
        report_as_of = self.continuity_report_as_of_ns
        health_observed = self.source_health_observed_at_ns
        health_available = self.source_health_available_at_ns
        if repository is not None:
            report_as_of, health_observed, health_available = _persisted_source_times(
                repository, report_ref=self.continuity_report_ref,
                health_ref=self.source_health_ref, cutoff_ns=self.cutoff_ns,
            )
        timed = replace(
            self, computation_context=context,
            continuity_report_as_of_ns=report_as_of,
            source_health_observed_at_ns=health_observed,
            source_health_available_at_ns=health_available,
        )
        evidence_ref = sha256_json({
            "artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": timed._body(),
        })
        return replace(timed, evidence_ref=evidence_ref)

    @property
    def quote(self) -> ExecutableQuote | None:
        if self.status != "AVAILABLE" or self.bid is None or self.ask is None:
            return None
        assert self.observed_at_ns is not None and self.available_at_ns is not None
        return ExecutableQuote(self.key, Decimal(self.bid), Decimal(self.ask), self.observed_at_ns,
                               self.available_at_ns, self.evidence_ref)


def _sequence_valid_s3_quote_at_cutoff(
    repository: OpsRepository,
    book: SequenceValidBookV2,
    *,
    product: ProductContractV2,
    cutoff_ns: int,
    continuity_report_ref: str,
) -> S3QuoteBridgeResultV1:
    """Bridge an existing S32 sequence book to S3's one-second BBO contract."""
    timestamp(cutoff_ns, field="cutoff_ns")
    if (book.instrument != product.key or book.source_id != BYBIT_PUBLIC_WS_SOURCE_ID_V1
            or book.channel != f"orderbook.50.{product.key.native_symbol}"):
        return _unavailable_quote_result(
            product.key, cutoff_ns, continuity_report_ref, "BOOK_IDENTITY_MISMATCH", book,
        )
    context = _valid_report_context(
        repository, product=product, cutoff_ns=cutoff_ns,
        report_ref=continuity_report_ref, channel=book.channel,
    )
    if context is None:
        return _unavailable_quote_result(
            product.key, cutoff_ns, continuity_report_ref, "S32_CONTINUITY_OR_HEALTH_UNAVAILABLE", book,
        )
    report, health, state = context
    feature = book.feature(cutoff_ns=cutoff_ns, availability_view=AvailabilityViewV2.ACTUAL_RECEIPT)
    reason: str | None = None
    support_refs: tuple[str, ...] = ()
    latest_bbo = report.get("latest_valid_bbo")
    if report.get("book_sequence_valid") is not True or not isinstance(latest_bbo, Mapping):
        reason = str(report.get("bbo_stale_or_unavailable_reason") or "BOOK_SEQUENCE_NOT_VALID")
    elif feature.instrument != product.key or feature.cutoff_ns != cutoff_ns:
        reason = "BOOK_FEATURE_CUTOFF_OR_IDENTITY_MISMATCH"
    elif feature.sequence_state != BookStateV2.VALID or feature.bbo is None or feature.data_age_ns is None:
        reason = feature.missing_reason or f"BOOK_SEQUENCE_{feature.sequence_state.value}"
    elif feature.source_health != PublicSourceStateV2.HEALTHY_CURRENT.value:
        reason = "BOOK_SOURCE_HEALTH_NOT_CURRENT"
    elif feature.source_health_ref != health.content_hash or feature.recovery_epoch != state.recovery_epoch:
        reason = "BOOK_HEALTH_OR_RECOVERY_EPOCH_MISMATCH"
    elif feature.data_age_ns > BBO_MAX_AGE_NS:
        reason = "BOOK_BBO_OLDER_THAN_S3_MAX_AGE"
    elif Decimal(feature.bbo[0]) >= Decimal(feature.bbo[1]):
        reason = "BOOK_BBO_CROSSED_OR_LOCKED"
    else:
        try:
            report_bid = Decimal(str(latest_bbo["bid_price"]))
            report_ask = Decimal(str(latest_bbo["ask_price"]))
            report_received = latest_bbo["received_at_ns"]
            report_age = latest_bbo["data_age_ns"]
            report_refs = tuple(sorted(set(latest_bbo["input_refs"])))
            timestamp(report_received, field="report.bbo.received_at_ns")
        except (ArithmeticError, KeyError, TypeError, ValueError):
            reason = "BOOK_REPORT_BBO_MALFORMED"
        else:
            if (report_bid != Decimal(feature.bbo[0]) or report_ask != Decimal(feature.bbo[1])
                    or report_received != cutoff_ns - feature.data_age_ns
                    or report_age != feature.data_age_ns or not set(feature.input_refs).issubset(report_refs)):
                reason = "BOOK_REPORT_BBO_INPUT_MISMATCH"
            else:
                valid_support_refs = _valid_book_support_refs(
                    repository, product=product, refs=report_refs, health_ref=health.content_hash,
                    channel=book.channel, epoch_id=state.epoch_id, as_of_ns=cutoff_ns,
                    latest_bbo_received_at_ns=report_received,
                )
                if valid_support_refs is None:
                    reason = "BOOK_BBO_SUPPORTING_REFS_UNAVAILABLE_OR_MISMATCHED"
                else:
                    support_refs = valid_support_refs
    return _quote_result_from_feature(
        product.key, cutoff_ns, continuity_report_ref, health.content_hash,
        state.recovery_epoch, feature, reason, additional_refs=support_refs,
    )


def _timed_quote_bridge(
    repository: OpsRepository,
    result: S3QuoteBridgeResultV1,
    *,
    computation_context: S3NativeComputationContextV1 | None,
) -> S3QuoteBridgeResultV1:
    if computation_context is None:
        return result
    _validate_computation_context(computation_context, cutoff_ns=result.cutoff_ns)
    report_as_of, health_observed, health_available = _persisted_source_times(
        repository, report_ref=result.continuity_report_ref,
        health_ref=result.source_health_ref, cutoff_ns=result.cutoff_ns,
    )
    timed = replace(
        result,
        computation_context=computation_context,
        continuity_report_as_of_ns=report_as_of,
        source_health_observed_at_ns=health_observed,
        source_health_available_at_ns=health_available,
    )
    evidence_ref = sha256_json({
        "artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": timed._body(),
    })
    return replace(timed, evidence_ref=evidence_ref)


def sequence_valid_s3_quote(
    repository: OpsRepository,
    book: SequenceValidBookV2,
    *,
    product: ProductContractV2,
    cutoff_ns: int,
    continuity_report_ref: str,
    computation_context: S3NativeComputationContextV1 | None = None,
) -> S3QuoteBridgeResultV1:
    """Project the book at the fixed cutoff and timestamp derived validation."""
    _validate_computation_context(computation_context, cutoff_ns=cutoff_ns)
    result = _sequence_valid_s3_quote_at_cutoff(
        repository, book, product=product, cutoff_ns=cutoff_ns,
        continuity_report_ref=continuity_report_ref,
    )
    return _timed_quote_bridge(repository, result, computation_context=computation_context)


def _quote_from_valid_continuity_report_at_cutoff(
    repository: OpsRepository,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
    continuity_report_ref: str,
) -> S3QuoteBridgeResultV1:
    """Rebuild an S3 BBO from the persisted S32 sequence-book report.

    The report is the existing ``SequenceValidBookV2`` output. This function
    does not maintain or replay a second order book.
    """
    timestamp(cutoff_ns, field="cutoff_ns")
    channel = f"orderbook.50.{product.key.native_symbol}"
    context = _valid_report_context(
        repository, product=product, cutoff_ns=cutoff_ns,
        report_ref=continuity_report_ref, channel=channel,
    )
    if context is None:
        return _report_quote_unavailable(
            product.key, cutoff_ns, continuity_report_ref, "S32_CONTINUITY_OR_HEALTH_UNAVAILABLE",
        )
    report, health, state = context
    report_entry = repository.get_artifact(continuity_report_ref)
    assert report_entry is not None
    state_ref = str(report_entry.metadata["state_ref"])
    if report.get("book_sequence_valid") is not True:
        return _report_quote_unavailable(
            product.key, cutoff_ns, continuity_report_ref,
            str(report.get("bbo_stale_or_unavailable_reason") or "BOOK_SEQUENCE_NOT_VALID"),
            health_ref=health.content_hash, recovery_epoch=state.recovery_epoch, state_ref=state_ref,
        )
    raw_bbo = report.get("latest_valid_bbo")
    if not isinstance(raw_bbo, Mapping):
        return _report_quote_unavailable(
            product.key, cutoff_ns, continuity_report_ref, "BOOK_BBO_EVIDENCE_MISSING",
            health_ref=health.content_hash, recovery_epoch=state.recovery_epoch, state_ref=state_ref,
        )
    try:
        bid = Decimal(str(raw_bbo["bid_price"]))
        ask = Decimal(str(raw_bbo["ask_price"]))
        received = raw_bbo["received_at_ns"]
        report_age = raw_bbo["data_age_ns"]
        input_refs = tuple(sorted(set(raw_bbo["input_refs"])))
        timestamp(received, field="bbo.received_at_ns")
        if type(report_age) is not int or report_age < 0 or not input_refs:
            raise ValueError("BBO age and refs are invalid")
        for ref in input_refs:
            sha256_ref(ref, field="BBO supporting ref")
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return _report_quote_unavailable(
            product.key, cutoff_ns, continuity_report_ref, "BOOK_BBO_EVIDENCE_MALFORMED",
            health_ref=health.content_hash, recovery_epoch=state.recovery_epoch, state_ref=state_ref,
        )
    report_as_of = int(report["as_of_ns"])
    age_at_cutoff = cutoff_ns - received
    reason: str | None = None
    if bid <= 0 or ask <= 0 or bid >= ask:
        reason = "BOOK_BBO_CROSSED_LOCKED_OR_NONPOSITIVE"
    elif report_age != report_as_of - received or report_age < 0:
        reason = "BOOK_BBO_REPORT_AGE_MISMATCH"
    elif received > report_as_of or report_as_of > cutoff_ns:
        reason = "BOOK_BBO_NOT_CUTOFF_AVAILABLE"
    elif age_at_cutoff < 0 or age_at_cutoff > BBO_MAX_AGE_NS:
        reason = "BOOK_BBO_OLDER_THAN_S3_MAX_AGE"
    support_refs = _valid_book_support_refs(
        repository, product=product, refs=input_refs, health_ref=health.content_hash,
        channel=channel, epoch_id=state.epoch_id, as_of_ns=cutoff_ns,
        latest_bbo_received_at_ns=received,
        expected_bbo=(str(bid), str(ask)),
    )
    if reason is None and support_refs is None:
        reason = "BOOK_BBO_SUPPORTING_REFS_UNAVAILABLE_OR_MISMATCHED"
    if reason is None and _continuity_invalidation_between(
        repository, product.key, channel=channel,
        after_ns=report_as_of, through_ns=cutoff_ns,
    ) is not None:
        reason = "BOOK_CONTINUITY_INVALIDATED_BEFORE_CUTOFF"
    sequence_view_ref = sha256_json({
        "artifact_type": "S3SequenceValidBookCutoffViewV1",
        "continuity_report_ref": continuity_report_ref,
        "state_ref": state_ref,
        "report_as_of_ns": report_as_of,
        "cutoff_ns": cutoff_ns,
        "latest_valid_bbo": dict(raw_bbo),
        "sequence_state": "VALID",
        "recovery_epoch": state.recovery_epoch,
    })
    refs = tuple(sorted(set(input_refs) | set(support_refs or ()) | {
        continuity_report_ref, health.content_hash, state_ref, sequence_view_ref,
    }))
    body = {
        "schema_version": 1, "key": product.key.to_dict(),
        "contract_revision": product.key.contract_revision, "cutoff_ns": cutoff_ns,
        "continuity_report_ref": continuity_report_ref, "source_health_ref": health.content_hash,
        "sequence_state": "VALID" if reason is None else "INVALID",
        "recovery_epoch": state.recovery_epoch, "sequence_feature_ref": sequence_view_ref,
        "bid": str(bid) if reason is None else None,
        "ask": str(ask) if reason is None else None,
        "observed_at_ns": received if reason is None else None,
        "available_at_ns": report_as_of if reason is None else None,
        "bbo_age_ns": age_at_cutoff if reason is None else None,
        "input_refs": list(refs), "status": "AVAILABLE" if reason is None else "NOT_ESTIMABLE",
        "reason_code": reason,
    }
    evidence_ref = sha256_json({"artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": body})
    sequence_state = "VALID" if reason is None else "INVALID"
    quote_bid = str(bid) if reason is None else None
    quote_ask = str(ask) if reason is None else None
    quote_observed_at = received if reason is None else None
    quote_available_at = report_as_of if reason is None else None
    quote_age = age_at_cutoff if reason is None else None
    quote_status = "AVAILABLE" if reason is None else "NOT_ESTIMABLE"
    return S3QuoteBridgeResultV1(
        product.key, product.key.contract_revision, cutoff_ns, continuity_report_ref,
        health.content_hash, sequence_state, state.recovery_epoch, sequence_view_ref,
        quote_bid, quote_ask, quote_observed_at, quote_available_at,
        quote_age, refs, quote_status, reason, evidence_ref,
    )


def quote_from_valid_continuity_report(
    repository: OpsRepository,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
    continuity_report_ref: str,
    computation_context: S3NativeComputationContextV1 | None = None,
) -> S3QuoteBridgeResultV1:
    """Rebuild the source cutoff view and attach honest derived timing."""
    _validate_computation_context(computation_context, cutoff_ns=cutoff_ns)
    result = _quote_from_valid_continuity_report_at_cutoff(
        repository, product, cutoff_ns=cutoff_ns, continuity_report_ref=continuity_report_ref,
    )
    return _timed_quote_bridge(repository, result, computation_context=computation_context)


def _continuity_invalidation_between(
    repository: OpsRepository, key: InstrumentKeyV2, *, channel: str,
    after_ns: int, through_ns: int,
) -> str | None:
    for entry in repository.public_stream_continuity_invalidations(key,
            source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel, after_ns=after_ns, through_ns=through_ns):
        if not after_ns < entry.available_at_ns <= through_ns:
            continue
        observation = entry.metadata.get("observation")
        if not isinstance(observation, Mapping) or not isinstance(observation.get("instrument"), Mapping):
            continue
        try:
            event_key = InstrumentKeyV2.from_dict(observation["instrument"])
        except (TypeError, ValueError):
            continue
        if (event_key == key and observation.get("source_id") == BYBIT_PUBLIC_WS_SOURCE_ID_V1
                and observation.get("channel") == channel):
            return entry.artifact_ref
    return None


def _valid_book_support_refs(
    repository: OpsRepository,
    *,
    product: ProductContractV2,
    refs: tuple[str, ...],
    health_ref: str,
    channel: str,
    epoch_id: str,
    as_of_ns: int,
    latest_bbo_received_at_ns: int | None = None,
    expected_bbo: tuple[str, str] | None = None,
) -> tuple[str, ...] | None:
    anchors = [entry for ref in refs if (entry := repository.get_artifact(ref)) is not None
               and entry.artifact_type == BOOK_CHECKPOINT_TYPE]
    if anchors:
        if len(anchors) != 1 or set(refs) != {anchors[0].artifact_ref, health_ref}:
            return None
        health_anchor = repository.get_artifact(health_ref)
        if health_anchor is None:
            return None
        try:
            checkpoint_body = validate_book_checkpoint(repository, anchors[0], as_of_ns=as_of_ns)
            if (checkpoint_body["instrument"] != product.key.to_dict() or checkpoint_body["source_id"] != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                    or checkpoint_body["channel"] != channel or checkpoint_body["metadata_ref"] != product.metadata_ref
                    or checkpoint_body["epoch_id"] != epoch_id
                    or checkpoint_body["as_of_ns"] != health_anchor.available_at_ns
                    or checkpoint_body["sequence_state"] != "VALID" or checkpoint_body["received_at_ns"] != latest_bbo_received_at_ns
                    or (expected_bbo is not None and checkpoint_body["bbo"] != list(expected_bbo))):
                return None
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return None
        return tuple(sorted(refs))
    if health_ref not in refs:
        return None
    frame_entries = repository.artifact_entries("PublicStreamFrameIndexV1")
    health_entries = repository.artifact_entries(STREAM_HEALTH_TYPE_V1)
    frames_by_raw_hash: dict[str, list[Any]] = {}
    for entry in frame_entries:
        raw_hash = entry.metadata.get("raw_payload_hash")
        if isinstance(raw_hash, str):
            frames_by_raw_hash.setdefault(raw_hash, []).append(entry)
    health_by_ref = {entry.artifact_ref: entry for entry in health_entries}
    support_refs: set[str] = set()
    frame_support_by_raw_ref: dict[str, list[ArtifactIndexEntryV2]] = {}
    for ref in refs:
        if ref in health_by_ref:
            entry = health_by_ref[ref]
            body = entry.metadata.get("health")
            transport = entry.metadata.get("transport")
            if (entry.content_hash != ref or entry.available_at_ns > as_of_ns
                    or not isinstance(body, Mapping) or not isinstance(transport, Mapping)):
                return None
            try:
                health = PublicSourceHealthV2.from_dict(body)
            except (ArithmeticError, KeyError, TypeError, ValueError):
                return None
            transport_instrument = transport.get("instrument")
            if not isinstance(transport_instrument, Mapping):
                return None
            try:
                exact_instrument = InstrumentKeyV2.from_dict(transport_instrument)
            except (TypeError, ValueError):
                return None
            if (health.content_hash != ref or health.source_id != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                    or exact_instrument != product.key
                    or health.available_at_ns > as_of_ns
                    or transport.get("source_id") != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                    or transport.get("channel") != channel
                    or transport.get("metadata_ref") != product.metadata_ref
                    or transport.get("epoch_id") != epoch_id):
                return None
            support_refs.add(entry.artifact_ref)
            continue
        candidates = frames_by_raw_hash.get(ref, ())
        matched = False
        for entry in candidates:
            metadata = entry.metadata
            try:
                instrument = InstrumentKeyV2.from_dict(metadata["instrument"])
            except (KeyError, TypeError, ValueError):
                continue
            record_id = metadata.get("record_id")
            if not isinstance(record_id, str):
                continue
            try:
                sha256_ref(record_id, field="book_frame.record_id")
            except ValueError:
                continue
            expected_ref = sha256_json({
                "artifact_type": "PublicStreamFrameIndexV1",
                "record_id": record_id,
            })
            if (entry.artifact_ref == expected_ref
                    and entry.content_hash == sha256_json(dict(metadata))
                    and entry.available_at_ns == metadata.get("available_at_ns")
                    and entry.available_at_ns <= as_of_ns
                    and type(metadata.get("received_at_ns")) is int
                    and metadata["received_at_ns"] <= entry.available_at_ns
                    and metadata["received_at_ns"] <= as_of_ns
                    and instrument == product.key
                    and metadata.get("instrument_hash") == product.key.content_hash
                    and metadata.get("source_id") == BYBIT_PUBLIC_WS_SOURCE_ID_V1
                    and metadata.get("channel") == channel
                    and metadata.get("available_at_ns") <= as_of_ns
                    and metadata.get("authority") == "ZERO"):
                matched = True
                support_refs.add(entry.artifact_ref)
                frame_support_by_raw_ref.setdefault(ref, []).append(entry)
        if not matched:
            return None
    if latest_bbo_received_at_ns is not None:
        timestamp(latest_bbo_received_at_ns, field="latest_bbo_received_at_ns")
        latest_frame_refs = {
            entry.artifact_ref for raw_ref, entries in frame_support_by_raw_ref.items()
            if raw_ref in refs for entry in entries
            if entry.metadata.get("received_at_ns") == latest_bbo_received_at_ns
        }
        if not latest_frame_refs:
            return None
        support_refs.update(latest_frame_refs)
    return tuple(sorted(support_refs)) if support_refs else None


def _report_quote_unavailable(
    key: InstrumentKeyV2, cutoff_ns: int, report_ref: str, reason: str, *,
    health_ref: str | None = None, recovery_epoch: int = 0, state_ref: str | None = None,
) -> S3QuoteBridgeResultV1:
    try:
        sha256_ref(report_ref, field="continuity_report_ref")
    except ValueError:
        report_ref = sha256_json({"missing_continuity_report_ref": str(report_ref)})
    health_ref = health_ref or sha256_json({"missing_s3_stream_health": key.to_dict()})
    state_ref = state_ref or sha256_json({"missing_s3_book_state": key.to_dict()})
    view_ref = sha256_json({
        "artifact_type": "S3SequenceValidBookCutoffViewV1", "continuity_report_ref": report_ref,
        "state_ref": state_ref, "cutoff_ns": cutoff_ns, "reason": reason,
    })
    refs = tuple(sorted({report_ref, health_ref, state_ref, view_ref}))
    body = {
        "schema_version": 1, "key": key.to_dict(), "contract_revision": key.contract_revision,
        "cutoff_ns": cutoff_ns, "continuity_report_ref": report_ref, "source_health_ref": health_ref,
        "sequence_state": "INVALID", "recovery_epoch": recovery_epoch,
        "sequence_feature_ref": view_ref, "bid": None, "ask": None,
        "observed_at_ns": None, "available_at_ns": None, "bbo_age_ns": None,
        "input_refs": list(refs), "status": "NOT_ESTIMABLE", "reason_code": reason,
    }
    evidence_ref = sha256_json({"artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": body})
    return S3QuoteBridgeResultV1(
        key, key.contract_revision, cutoff_ns, report_ref, health_ref, "INVALID", recovery_epoch,
        view_ref, None, None, None, None, None, refs, "NOT_ESTIMABLE", reason, evidence_ref,
    )


def _unavailable_quote_result(
    key: InstrumentKeyV2, cutoff_ns: int, report_ref: str, reason: str, book: SequenceValidBookV2,
) -> S3QuoteBridgeResultV1:
    placeholder = book.feature(cutoff_ns=cutoff_ns, availability_view=AvailabilityViewV2.ACTUAL_RECEIPT)
    return _quote_result_from_feature(
        key, cutoff_ns, report_ref, placeholder.source_health_ref or sha256_json({"missing": "health"}),
        placeholder.recovery_epoch, placeholder, reason,
    )


def _quote_result_from_feature(
    key: InstrumentKeyV2,
    cutoff_ns: int,
    continuity_report_ref: str,
    source_health_ref: str,
    recovery_epoch: int,
    feature: Any,
    reason: str | None,
    *,
    additional_refs: Sequence[str] = (),
) -> S3QuoteBridgeResultV1:
    refs = tuple(sorted(set(feature.input_refs) | set(additional_refs)
                        | {continuity_report_ref, source_health_ref}))
    available = feature.cutoff_ns
    age = feature.data_age_ns
    if reason is None and (feature.bbo is None or age is None or feature.sequence_state != BookStateV2.VALID):
        reason = feature.missing_reason or "BOOK_BBO_UNAVAILABLE"
    bid = feature.bbo[0] if reason is None and feature.bbo is not None else None
    ask = feature.bbo[1] if reason is None and feature.bbo is not None else None
    observed = cutoff_ns - age if reason is None and age is not None else None
    if reason is None and (bid is None or ask is None or Decimal(bid) >= Decimal(ask)):
        reason = "BOOK_BBO_CROSSED_OR_LOCKED"
        bid = ask = observed = None
    body = {
        "schema_version": 1,
        "key": key.to_dict(),
        "contract_revision": key.contract_revision,
        "cutoff_ns": cutoff_ns,
        "continuity_report_ref": continuity_report_ref,
        "source_health_ref": source_health_ref,
        "sequence_state": feature.sequence_state.value,
        "recovery_epoch": recovery_epoch,
        "sequence_feature_ref": feature.content_hash,
        "bid": bid,
        "ask": ask,
        "observed_at_ns": observed,
        "available_at_ns": available if reason is None else None,
        "bbo_age_ns": age if reason is None else None,
        "input_refs": list(refs),
        "status": "AVAILABLE" if reason is None else "NOT_ESTIMABLE",
        "reason_code": reason,
    }
    evidence_ref = sha256_json({"artifact_type": "S3SequenceBookQuoteEvidenceV1", "evidence": body})
    return S3QuoteBridgeResultV1(
        key, key.contract_revision, cutoff_ns, continuity_report_ref, source_health_ref,
        feature.sequence_state.value, recovery_epoch, feature.content_hash,
        bid, ask, observed, body["available_at_ns"], body["bbo_age_ns"], refs,
        body["status"], reason, evidence_ref,
    )


@dataclass(frozen=True)
class S3WarmupReadinessV1:
    """Read-only S3 evidence inventory; completeness remains an explicit hard gate."""

    key: InstrumentKeyV2
    cutoff_ns: int
    required_m1_bars: int
    observed_contiguous_m1_bars: int
    earliest_valid_m1_close_ns: int | None
    latest_valid_m1_close_ns: int | None
    gaps_open_at_ns: tuple[int, ...]
    gap_count: int
    gaps_truncated: bool
    required_residuals: int
    valid_residual_count: int
    contiguous_residual_count: int
    required_trade_vwap_refs: int
    valid_trade_vwap_count: int
    observed_trade_count: int
    bar_source_health: str
    trade_source_health: str
    current_bbo_age_ns: int | None
    event_gate_status: str
    point_in_time_universe_status: str
    contract_revision: str
    availability_view: str
    recovery_epoch: int | None
    trade_completeness_status: str
    trade_completeness_proven: bool
    status: str
    gate_status: str
    reason_codes: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    trade_recovery_epoch: int | None = None
    book_recovery_epoch: int | None = None
    trade_evidence_status: str = "NOT_ESTIMABLE"
    trade_evidence_reason_codes: tuple[str, ...] = ()
    computation_context: S3NativeComputationContextV1 | None = None

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="s3_warmup.cutoff_ns")
        _validate_computation_context(self.computation_context, cutoff_ns=self.cutoff_ns)
        if self.required_m1_bars != AR_OBSERVATION_COUNT or self.required_residuals != AR_OBSERVATION_COUNT:
            raise ValueError("S3 warmup thresholds must preserve the frozen 10,081 observation requirement")
        if self.required_trade_vwap_refs != AR_OBSERVATION_COUNT:
            raise ValueError("S3 warmup requires one exact trade VWAP ref per frozen residual")
        if self.trade_completeness_proven is not False:
            raise ValueError("Bybit S32 WS evidence cannot qualify S3 trade completeness")
        for name in ("recovery_epoch", "trade_recovery_epoch", "book_recovery_epoch"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.availability_view not in {"ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"}:
            raise ValueError("unknown S3 warmup availability view")
        if self.status not in {"READY", "NOT_ESTIMABLE"} or self.gate_status not in {"READY", "TEST GATE"}:
            raise ValueError("unsupported S3 warmup status")
        if self.status == "READY" or self.gate_status != "TEST GATE":
            raise ValueError("S3 cannot be READY while exact Bybit trade completeness is unproven")
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise ValueError("S3 warmup reasons must be sorted and unique")
        if tuple(sorted(set(self.evidence_refs))) != self.evidence_refs:
            raise ValueError("S3 warmup refs must be sorted and unique")
        if self.trade_evidence_status not in {
            "OBSERVED", "NOT_ESTIMABLE", "TEST GATE", "BLOCKED BY ENVIRONMENT",
        }:
            raise ValueError("unsupported S3 trade evidence status")
        if tuple(sorted(set(self.trade_evidence_reason_codes))) != self.trade_evidence_reason_codes:
            raise ValueError("S3 trade evidence reasons must be sorted and unique")

    def to_dict(self) -> dict[str, Any]:
        body = {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "cutoff_ns": self.cutoff_ns,
            "required_m1_bars": self.required_m1_bars,
            "observed_contiguous_m1_bars": self.observed_contiguous_m1_bars,
            "earliest_valid_m1_close_ns": self.earliest_valid_m1_close_ns,
            "latest_valid_m1_close_ns": self.latest_valid_m1_close_ns,
            "gaps_open_at_ns": list(self.gaps_open_at_ns),
            "gap_count": self.gap_count,
            "gaps_truncated": self.gaps_truncated,
            "required_residuals": self.required_residuals,
            "valid_residual_count": self.valid_residual_count,
            "contiguous_residual_count": self.contiguous_residual_count,
            "required_preceding_standardization_residuals": STANDARDIZATION_COUNT,
            "valid_preceding_standardization_residuals": min(
                STANDARDIZATION_COUNT, max(0, self.contiguous_residual_count - 1),
            ),
            "required_trade_vwap_refs": self.required_trade_vwap_refs,
            "valid_trade_vwap_count": self.valid_trade_vwap_count,
            "observed_trade_count": self.observed_trade_count,
            "bar_source_health": self.bar_source_health,
            "trade_source_health": self.trade_source_health,
            "current_bbo_age_ns": self.current_bbo_age_ns,
            "event_gate_status": self.event_gate_status,
            "point_in_time_universe_status": self.point_in_time_universe_status,
            "contract_revision": self.contract_revision,
            "availability_view": self.availability_view,
            "recovery_epoch": self.recovery_epoch,
            "source_recovery_epochs": {
                "trade": self.trade_recovery_epoch,
                "book": self.book_recovery_epoch,
            },
            "trade_evidence_status": self.trade_evidence_status,
            "trade_evidence_reason_codes": list(self.trade_evidence_reason_codes),
            "trade_completeness_status": self.trade_completeness_status,
            "trade_completeness_proven": False,
            "status": self.status,
            "gate_status": self.gate_status,
            "reason_codes": list(self.reason_codes),
            "evidence_refs": list(self.evidence_refs),
        }
        if self.computation_context is not None:
            body["computation_context"] = self.computation_context.to_dict()
            body["produced_at_ns"] = self.computation_context.produced_at_ns
        return body

    def with_computation_context(
        self,
        context: S3NativeComputationContextV1,
    ) -> S3WarmupReadinessV1:
        """Return readiness timestamped after its computation, still diagnostic."""
        _validate_computation_context(context, cutoff_ns=self.cutoff_ns)
        return replace(self, computation_context=context)


def evaluate_s3_warmup_readiness(
    *,
    key: InstrumentKeyV2,
    cutoff_ns: int,
    bars: Sequence[CausalBarV2],
    residuals: Sequence[ResidualObservationV2],
    trade_vwaps: Sequence[TradeVwapSnapshotV2],
    trades: Sequence[CausalTradeV2],
    bar_source_health: PublicSourceHealthV2 | None,
    trade_source_health: PublicSourceHealthV2 | None,
    quote: ExecutableQuote | None,
    event_gate: EventGate | None,
    point_in_time_universe_eligible: bool,
    availability_view: AvailabilityClassV2 | str = AvailabilityClassV2.ACTUAL_SYSTEM,
    recovery_epoch: int | None = None,
    trade_recovery_epoch: int | None = None,
    book_recovery_epoch: int | None = None,
    trade_evidence_status: str = "NOT_ESTIMABLE",
    trade_evidence_reason_codes: Sequence[str] = (),
    additional_evidence_refs: Sequence[str] = (),
    computation_context: S3NativeComputationContextV1 | None = None,
) -> S3WarmupReadinessV1:
    """Derive exact S3 warmup counts without maintaining mutable readiness state.

    This report may show reconstructed history counts for diagnostics, but its
    final status remains NOT_ESTIMABLE until a separately accepted source
    contract can prove complete trade coverage. S32 Bybit WS and recent REST
    observations do not provide that proof.
    """
    timestamp(cutoff_ns, field="cutoff_ns")
    _validate_computation_context(computation_context, cutoff_ns=cutoff_ns)
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("S3 warmup requires full InstrumentKeyV2")
    view = AvailabilityClassV2(availability_view)
    if view not in {AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET}:
        raise ValueError("S3 warmup supports only explicit actual or reconstructed views")
    if type(point_in_time_universe_eligible) is not bool:
        raise ValueError("point-in-time universe state must be bool")
    for name, value in (("recovery_epoch", recovery_epoch),
                        ("trade_recovery_epoch", trade_recovery_epoch),
                        ("book_recovery_epoch", book_recovery_epoch)):
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError(f"{name} must be nonnegative")
    selected_trade_epoch = trade_recovery_epoch if trade_recovery_epoch is not None else recovery_epoch
    selected_epoch = selected_trade_epoch if selected_trade_epoch is not None else book_recovery_epoch
    if trade_evidence_status not in {
        "OBSERVED", "NOT_ESTIMABLE", "TEST GATE", "BLOCKED BY ENVIRONMENT",
    }:
        raise ValueError("unsupported S3 trade evidence status")
    trade_reasons = tuple(sorted(set(trade_evidence_reason_codes)))
    refs: set[str] = set()
    for ref in additional_evidence_refs:
        sha256_ref(ref, field="S3 warmup additional evidence ref")
        refs.add(ref)

    reasons: set[str] = {"TEST_GATE_BYBIT_TRADE_COMPLETENESS_UNPROVEN"}
    visible_bars: list[CausalBarV2] = []
    for bar in bars:
        if (bar.final and bar.interval == BarIntervalV2.M1
                and bar.instrument_revision == key.contract_revision
                and bar.raw.availability_class == view and bar.close_at_ns <= cutoff_ns):
            effective_available = (bar.raw.available_at_ns if view == AvailabilityClassV2.ACTUAL_SYSTEM
                                   else bar.replay_available_at_ns)
            if effective_available is not None and effective_available <= cutoff_ns:
                visible_bars.append(bar)
    by_open: dict[int, CausalBarV2] = {}
    conflicted_opens: set[int] = set()
    for bar in sorted(visible_bars, key=lambda row: (row.open_at_ns, row.content_hash)):
        old = by_open.get(bar.open_at_ns)
        if old is None:
            by_open[bar.open_at_ns] = bar
        elif old.content_hash != bar.content_hash:
            conflicted_opens.add(bar.open_at_ns)
    ordered_bars = tuple(by_open[opened] for opened in sorted(by_open))
    gaps: list[int] = []
    gap_count = 0
    for left, right in zip(ordered_bars, ordered_bars[1:], strict=False):
        distance = right.open_at_ns - left.open_at_ns
        if distance != BarIntervalV2.M1.duration_ns:
            step = BarIntervalV2.M1.duration_ns
            missing_count = max(1, (distance + step - 1) // step - 1)
            gap_count += missing_count
            remaining = MAX_REPORTED_GAP_OPENS - len(gaps)
            if remaining > 0:
                gaps.extend(
                    left.open_at_ns + offset * step
                    for offset in range(1, min(missing_count, remaining) + 1)
                )
    gap_count += len(conflicted_opens)
    if len(gaps) < MAX_REPORTED_GAP_OPENS:
        gaps.extend(sorted(conflicted_opens)[:MAX_REPORTED_GAP_OPENS - len(gaps)])
    contiguous_tail: list[CausalBarV2] = []
    for bar in reversed(ordered_bars):
        if conflicted_opens and bar.open_at_ns in conflicted_opens:
            break
        if contiguous_tail and contiguous_tail[-1].open_at_ns - bar.open_at_ns != BarIntervalV2.M1.duration_ns:
            break
        contiguous_tail.append(bar)
    contiguous_tail.reverse()
    bar_by_ref = {bar.content_hash: bar for bar in contiguous_tail}
    vwap_by_ref: dict[str, TradeVwapSnapshotV2] = {}
    for snapshot in trade_vwaps:
        if snapshot.key == key and snapshot.replay_view == view.value and snapshot.available_at_ns <= cutoff_ns:
            vwap_by_ref[snapshot.content_hash] = snapshot
    residual_by_bar: dict[str, ResidualObservationV2] = {}
    residual_vwap_refs: set[str] = set()
    def cutoff_visible_trade(trade: CausalTradeV2) -> bool:
        effective_available = trade.effective_available_at(view.value)
        return (
            trade.key == key and trade.availability_class.value == view.value
            and effective_available is not None and effective_available <= cutoff_ns
            and trade.event_at_ns <= cutoff_ns
            and (view != AvailabilityClassV2.ACTUAL_SYSTEM or trade.received_at_ns <= cutoff_ns)
        )

    available_trade_refs = {
        trade.raw_observation_ref for trade in trades if cutoff_visible_trade(trade)
    }
    for residual in residuals:
        residual_bar = bar_by_ref.get(residual.bar_ref)
        residual_vwap = vwap_by_ref.get(residual.vwap_ref)
        if residual_bar is None or residual_vwap is None:
            continue
        if (residual.key != key or residual.replay_view != view.value
                or residual.close_at_ns != residual_bar.close_at_ns or residual.available_at_ns > cutoff_ns
                or residual_vwap.key != key or residual_vwap.information_cutoff_ns != residual_bar.close_at_ns
                or residual_vwap.available_at_ns > residual.available_at_ns
                or not set(residual_vwap.trade_refs).issubset(available_trade_refs)):
            continue
        if validate_historical_residual_v2(
            residual, key=key, bar=residual_bar, vwap=residual_vwap, replay_view=view.value,
        ) is not None:
            continue
        residual_by_bar[residual_bar.content_hash] = residual
        residual_vwap_refs.add(residual_vwap.content_hash)
        refs.update((residual_bar.content_hash, residual.content_hash, residual_vwap.content_hash,
                     *residual_vwap.trade_refs, residual_vwap.source_health_ref))
    valid_residuals = tuple(sorted(residual_by_bar.values(), key=lambda item: item.close_at_ns))
    contiguous_residual_count = 0
    for bar in reversed(contiguous_tail):
        if bar.content_hash not in residual_by_bar:
            break
        contiguous_residual_count += 1

    actual_trades = tuple(trade for trade in trades if cutoff_visible_trade(trade))
    refs.update(trade.raw_observation_ref for trade in actual_trades)
    if len(contiguous_tail) < AR_OBSERVATION_COUNT:
        reasons.add("INSUFFICIENT_CONTIGUOUS_M1_BARS")
    if len(valid_residuals) < AR_OBSERVATION_COUNT or contiguous_residual_count < AR_OBSERVATION_COUNT:
        reasons.add("INSUFFICIENT_EXACT_TRADE_VWAP_RESIDUALS")
    if len(valid_residuals) < STANDARDIZATION_COUNT + 1:
        reasons.add("INSUFFICIENT_120_PRECEDING_RESIDUALS")
    if gaps:
        reasons.add("M1_HISTORY_GAP_OR_CONFLICT")
    if view == AvailabilityClassV2.RECONSTRUCTED_MARKET:
        reasons.add("RECONSTRUCTED_MARKET_CANNOT_QUALIFY_ACTUAL_SYSTEM_WARMUP")

    bar_health_state = "MISSING"
    if bar_source_health is not None:
        refs.add(bar_source_health.content_hash)
        bar_health_state = bar_source_health.state.value
        if (bar_source_health.state != PublicSourceStateV2.HEALTHY_CURRENT
                or bar_source_health.available_at_ns > cutoff_ns or bar_source_health.observed_at_ns > cutoff_ns
                or cutoff_ns - bar_source_health.observed_at_ns > SOURCE_HEALTH_MAX_AGE_NS
                or any(bar.raw.source_id != bar_source_health.source_id for bar in contiguous_tail)):
            reasons.add("BAR_SOURCE_HEALTH_NOT_CURRENT_AT_CUTOFF")
    else:
        reasons.add("BAR_SOURCE_HEALTH_MISSING")
    trade_health_state = "MISSING"
    if trade_source_health is not None:
        refs.add(trade_source_health.content_hash)
        trade_health_state = trade_source_health.state.value
        if (trade_source_health.state != PublicSourceStateV2.HEALTHY_CURRENT
                or trade_source_health.available_at_ns > cutoff_ns or trade_source_health.observed_at_ns > cutoff_ns
                or cutoff_ns - trade_source_health.observed_at_ns > SOURCE_HEALTH_MAX_AGE_NS):
            reasons.add("TRADE_SOURCE_HEALTH_NOT_CURRENT_AT_CUTOFF")
    else:
        reasons.add("TRADE_SOURCE_HEALTH_MISSING")
    if any(trade.source_id != trade_source_health.source_id for trade in actual_trades) if trade_source_health else actual_trades:
        reasons.add("TRADE_SOURCE_HEALTH_IDENTITY_MISMATCH")

    if quote is None or quote.key != key or not quote.valid_at(cutoff_ns, BBO_MAX_AGE_NS) or quote.bid >= quote.ask:
        reasons.add("S3_BBO_UNAVAILABLE_OR_STALE")
        bbo_age = None
    else:
        refs.add(quote.evidence_ref)
        bbo_age = cutoff_ns - quote.observed_at_ns
    gate_status = "UNKNOWN"
    if event_gate is not None:
        refs.add(event_gate.evidence_ref)
        gate_status = event_gate.state.value if event_gate.valid_at(cutoff_ns) else "STALE"
    if event_gate is None or gate_status in {"UNKNOWN", "STALE"}:
        reasons.add("EVENT_GATE_NOT_CURRENT")
    if event_gate is not None and gate_status == EventState.BLOCKED.value:
        reasons.add("EVENT_GATE_BLOCKED")
    universe_status = "ELIGIBLE" if point_in_time_universe_eligible else "INELIGIBLE_OR_UNKNOWN"
    if not point_in_time_universe_eligible:
        reasons.add("POINT_IN_TIME_UNIVERSE_NOT_ELIGIBLE")
    for bar in contiguous_tail:
        refs.add(bar.content_hash)
    earliest = contiguous_tail[0].close_at_ns if contiguous_tail else None
    latest = contiguous_tail[-1].close_at_ns if contiguous_tail else None
    return S3WarmupReadinessV1(
        key, cutoff_ns, AR_OBSERVATION_COUNT, len(contiguous_tail), earliest, latest,
        tuple(sorted(set(gaps))), gap_count, gap_count > len(set(gaps)),
        AR_OBSERVATION_COUNT, len(valid_residuals),
        contiguous_residual_count, AR_OBSERVATION_COUNT, len(residual_vwap_refs),
        len(actual_trades), bar_health_state, trade_health_state, bbo_age, gate_status,
        universe_status, key.contract_revision, view.value, selected_epoch, "NOT_PROVEN", False,
        "NOT_ESTIMABLE", "TEST GATE", tuple(sorted(reasons)), tuple(sorted(refs)),
        selected_trade_epoch, book_recovery_epoch, trade_evidence_status, trade_reasons,
        computation_context,
    )
