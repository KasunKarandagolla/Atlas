"""Typed reconciliation evidence and immutable recovery-run membership."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.domain.time import ensure_utc_ns


class QueryType(StrEnum):
    OPEN_ORDERS = "open_orders"
    ORDER_HISTORY = "order_history"
    EXECUTION_HISTORY = "execution_history"
    POSITIONS = "positions"
    WALLET_BALANCE = "wallet_balance"
    TRADING_STOP = "trading_stop"
    CONDITIONAL_ORDERS = "conditional_orders"
    TRANSACTION_LOG = "transaction_log"


class QueryScope(StrEnum):
    ACCOUNT = "account"
    INSTRUMENT = "instrument"


class QueryStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"


class Completeness(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE_PAGINATED = "incomplete_paginated"
    INCOMPLETE_TRUNCATED = "incomplete_truncated"
    INCOMPLETE_RETENTION_LIMIT = "incomplete_retention_limit"
    UNKNOWN = "unknown"


class ReconciliationRunState(StrEnum):
    OPEN = "OPEN"
    COMPLETE = "COMPLETE"


ACCOUNT_SCOPED = frozenset({QueryType.WALLET_BALANCE, QueryType.TRANSACTION_LOG})
INSTRUMENT_SCOPED = frozenset(
    {
        QueryType.OPEN_ORDERS,
        QueryType.ORDER_HISTORY,
        QueryType.EXECUTION_HISTORY,
        QueryType.POSITIONS,
        QueryType.TRADING_STOP,
        QueryType.CONDITIONAL_ORDERS,
    }
)

# This is the capital-control minimum.  A run may add diagnostic query types,
# but callers cannot remove any of these V1 execution-risk queries.
DEFAULT_EXECUTION_RISK_QUERIES = (
    QueryType.OPEN_ORDERS,
    QueryType.CONDITIONAL_ORDERS,
    QueryType.ORDER_HISTORY,
    QueryType.EXECUTION_HISTORY,
    QueryType.POSITIONS,
    QueryType.WALLET_BALANCE,
    QueryType.TRADING_STOP,
)


def compute_evidence_hash(payload: dict[str, Any]) -> str:
    """Hash a canonical JSON payload for compatibility and test fixtures."""

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _coalesce(segments: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    if not segments:
        return ()
    ordered = sorted(segments)
    result: list[tuple[int, int]] = []
    for start, end in ordered:
        ensure_utc_ns(start, field="retention_start")
        ensure_utc_ns(end, field="retention_end")
        if end < start:
            raise ValueError("retention segment invalid")
        if result and start <= result[-1][1] + 1:
            result[-1] = (result[-1][0], max(result[-1][1], end))
        else:
            result.append((start, end))
    return tuple(result)


def covers_interval(segments: tuple[tuple[int, int], ...], start: int, end: int) -> bool:
    return any(a <= start and b >= end for a, b in _coalesce(segments))


@dataclass(frozen=True, init=False)
class ReconciliationQueryEvidence:
    query_id: str
    query_type: QueryType
    scope: QueryScope
    account: str
    instrument: str | None
    requested_interval_start_ns: int | None
    requested_interval_end_ns: int | None
    pagination_cursors: tuple[str, ...]
    pages_observed: int
    total_records_returned: int
    completeness: Completeness
    status: QueryStatus
    source_time_ns: int | None
    receipt_time_ns: int
    request_ids: tuple[str, ...]
    retention_segments: tuple[tuple[int, int], ...]
    facts: dict[str, Any]
    evidence_hash: str
    error_message: str | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Construct v5 evidence while accepting the historical v4 shape."""

        names = tuple(self.__dataclass_fields__)
        old_names = (
            "query_id",
            "query_type",
            "account",
            "instrument",
            "requested_interval_start_ns",
            "requested_interval_end_ns",
            "pagination_cursors",
            "pages_observed",
            "total_records_returned",
            "completeness",
            "status",
            "source_time_ns",
            "receipt_time_ns",
            "request_ids",
            "retention_coverage_start_ns",
            "retention_coverage_end_ns",
            "evidence_hash",
            "error_message",
        )
        if len(args) == len(names):
            values = dict(zip(names, args, strict=True))
        elif len(args) == len(old_names):
            values = dict(zip(old_names, args, strict=True))
        elif args:
            raise TypeError(f"expected {len(names)} v5 or {len(old_names)} v4 positional fields")
        else:
            values = {}
        values.update(kwargs)

        query_type = values.get("query_type")
        if "scope" not in values:
            if query_type in ACCOUNT_SCOPED:
                values["scope"] = QueryScope.ACCOUNT
                values["instrument"] = None
            else:
                values["scope"] = QueryScope.ACCOUNT if values.get("instrument") is None else QueryScope.INSTRUMENT
        if "retention_segments" not in values:
            start = values.pop("retention_coverage_start_ns", None)
            end = values.pop("retention_coverage_end_ns", None)
            values["retention_segments"] = () if start is None or end is None else ((start, end),)
        else:
            values.pop("retention_coverage_start_ns", None)
            values.pop("retention_coverage_end_ns", None)
        values.setdefault("facts", {})

        for name in names:
            if name not in values:
                raise TypeError(f"missing required field: {name}")
            object.__setattr__(self, name, values[name])
        self.__post_init__()

    def __post_init__(self) -> None:
        if not self.query_id.strip() or not self.account.strip():
            raise ValueError("query/account required")
        if self.query_type in ACCOUNT_SCOPED and (self.scope != QueryScope.ACCOUNT or self.instrument is not None):
            raise ValueError("account-scoped query must use instrument=None")
        if self.query_type in INSTRUMENT_SCOPED and (self.scope != QueryScope.INSTRUMENT or not self.instrument):
            raise ValueError("instrument-scoped query requires instrument")
        if self.requested_interval_start_ns is not None:
            ensure_utc_ns(self.requested_interval_start_ns, field="requested_start")
        if self.requested_interval_end_ns is not None:
            ensure_utc_ns(self.requested_interval_end_ns, field="requested_end")
        if (
            self.requested_interval_start_ns is not None
            and self.requested_interval_end_ns is not None
            and self.requested_interval_end_ns < self.requested_interval_start_ns
        ):
            raise ValueError("requested interval invalid")
        ensure_utc_ns(self.receipt_time_ns, field="receipt_time_ns")
        if self.source_time_ns is not None:
            ensure_utc_ns(self.source_time_ns, field="source_time_ns")
        if self.pages_observed < 0 or self.total_records_returned < 0:
            raise ValueError("negative page/record count")
        object.__setattr__(self, "pagination_cursors", tuple(self.pagination_cursors))
        object.__setattr__(self, "request_ids", tuple(self.request_ids))
        object.__setattr__(self, "retention_segments", _coalesce(tuple(self.retention_segments)))
        object.__setattr__(self, "facts", dict(self.facts))
        if len(self.evidence_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.evidence_hash
        ):
            raise ValueError("evidence_hash must be sha256")

    def canonical_payload(self) -> dict[str, Any]:
        """Return every immutable field that the stored hash must bind."""

        return {
            "query_id": self.query_id,
            "query_type": self.query_type.value,
            "scope": self.scope.value,
            "account": self.account,
            "instrument": self.instrument,
            "requested_interval_start_ns": self.requested_interval_start_ns,
            "requested_interval_end_ns": self.requested_interval_end_ns,
            "pagination_cursors": list(self.pagination_cursors),
            "pages_observed": self.pages_observed,
            "total_records_returned": self.total_records_returned,
            "completeness": self.completeness.value,
            "status": self.status.value,
            "source_time_ns": self.source_time_ns,
            "receipt_time_ns": self.receipt_time_ns,
            "request_ids": list(self.request_ids),
            "retention_segments": [list(segment) for segment in self.retention_segments],
            "facts": self.facts,
            "error_message": self.error_message,
        }

    def expected_evidence_hash(self) -> str:
        return compute_evidence_hash(self.canonical_payload())

    @property
    def hash_binds_payload(self) -> bool:
        return self.evidence_hash == self.expected_evidence_hash()

    @property
    def is_empty_result(self) -> bool:
        return self.total_records_returned == 0

    @property
    def is_incomplete(self) -> bool:
        return self.completeness != Completeness.COMPLETE

    @property
    def retention_coverage_start_ns(self) -> int | None:
        return self.retention_segments[0][0] if self.retention_segments else None

    @property
    def retention_coverage_end_ns(self) -> int | None:
        return self.retention_segments[-1][1] if self.retention_segments else None

    @property
    def can_certify_absence(self) -> bool:
        return (
            self.hash_binds_payload
            and self.status == QueryStatus.SUCCESS
            and self.completeness == Completeness.COMPLETE
            and self.is_empty_result
            and self.requested_interval_start_ns is not None
            and self.requested_interval_end_ns is not None
            and covers_interval(
                self.retention_segments,
                self.requested_interval_start_ns,
                self.requested_interval_end_ns,
            )
        )


def make_query_evidence(**values: Any) -> ReconciliationQueryEvidence:
    """Build evidence with its hash calculated from its complete payload."""

    values["evidence_hash"] = "0" * 64
    provisional = ReconciliationQueryEvidence(**values)
    values["evidence_hash"] = provisional.expected_evidence_hash()
    return ReconciliationQueryEvidence(**values)


def merge_query_evidence(
    items: list[ReconciliationQueryEvidence],
) -> ReconciliationQueryEvidence | None:
    if not items:
        return None
    first = items[0]
    for evidence in items[1:]:
        if (
            evidence.query_type,
            evidence.scope,
            evidence.account,
            evidence.instrument,
            evidence.requested_interval_start_ns,
            evidence.requested_interval_end_ns,
        ) != (
            first.query_type,
            first.scope,
            first.account,
            first.instrument,
            first.requested_interval_start_ns,
            first.requested_interval_end_ns,
        ):
            raise ValueError("cannot merge inconsistent query evidence")
    severity = {
        Completeness.COMPLETE: 0,
        Completeness.INCOMPLETE_PAGINATED: 1,
        Completeness.INCOMPLETE_TRUNCATED: 2,
        Completeness.INCOMPLETE_RETENTION_LIMIT: 3,
        Completeness.UNKNOWN: 4,
    }
    completeness = max((item.completeness for item in items), key=severity.__getitem__)
    if any(item.status in (QueryStatus.FAILED, QueryStatus.TIMEOUT) for item in items):
        status = QueryStatus.FAILED
    elif any(item.status in (QueryStatus.PARTIAL, QueryStatus.RATE_LIMITED) for item in items):
        status = QueryStatus.PARTIAL
    else:
        status = QueryStatus.SUCCESS
    segments = _coalesce(tuple(segment for item in items for segment in item.retention_segments))
    return make_query_evidence(
        query_id=f"{first.query_id}-merged",
        query_type=first.query_type,
        scope=first.scope,
        account=first.account,
        instrument=first.instrument,
        requested_interval_start_ns=first.requested_interval_start_ns,
        requested_interval_end_ns=first.requested_interval_end_ns,
        pagination_cursors=tuple(cursor for item in items for cursor in item.pagination_cursors),
        pages_observed=sum(item.pages_observed for item in items),
        total_records_returned=sum(item.total_records_returned for item in items),
        completeness=completeness,
        status=status,
        source_time_ns=max(item.source_time_ns or 0 for item in items) or None,
        receipt_time_ns=max(item.receipt_time_ns for item in items),
        request_ids=tuple(request_id for item in items for request_id in item.request_ids),
        retention_segments=segments,
        facts={"merged_evidence_hashes": [item.evidence_hash for item in items]},
        error_message=None,
    )


@dataclass(frozen=True)
class ReconciliationRun:
    run_id: str
    account: str
    instrument: str | None
    writer_id: str
    writer_epoch: int
    runtime_instance_id: str
    started_at_ns: int
    completed_at_ns: int | None
    required_query_types: tuple[QueryType, ...] = DEFAULT_EXECUTION_RISK_QUERIES
    state: ReconciliationRunState = ReconciliationRunState.OPEN

    def __post_init__(self) -> None:
        for name in ("run_id", "account", "writer_id", "runtime_instance_id"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} required")
        ensure_utc_ns(self.started_at_ns, field="started_at_ns")
        if self.state != ReconciliationRunState.OPEN:
            raise ValueError("caller-created reconciliation runs must be OPEN")
        if self.completed_at_ns is not None:
            raise ValueError("caller-created reconciliation runs cannot have completion time")
        required = tuple(self.required_query_types)
        if len(set(required)) != len(required):
            raise ValueError("required query types must be unique")
        missing = set(DEFAULT_EXECUTION_RISK_QUERIES) - set(required)
        if missing:
            names = ", ".join(sorted(query.value for query in missing))
            raise ValueError(f"required query set omits mandatory evidence: {names}")
        object.__setattr__(self, "required_query_types", required)

    @classmethod
    def from_persisted(
        cls,
        *,
        run_id: str,
        account: str,
        instrument: str | None,
        writer_id: str,
        writer_epoch: int,
        runtime_instance_id: str,
        started_at_ns: int,
        completed_at_ns: int | None,
        required_query_types: tuple[QueryType, ...],
        state: ReconciliationRunState,
    ) -> ReconciliationRun:
        """Load a run whose COMPLETE state was produced transactionally."""

        run = object.__new__(cls)
        for name, value in {
            "run_id": run_id,
            "account": account,
            "instrument": instrument,
            "writer_id": writer_id,
            "writer_epoch": writer_epoch,
            "runtime_instance_id": runtime_instance_id,
            "started_at_ns": started_at_ns,
            "completed_at_ns": completed_at_ns,
            "required_query_types": tuple(required_query_types),
            "state": state,
        }.items():
            object.__setattr__(run, name, value)
        return run


@dataclass(frozen=True)
class ReconciliationEvidenceBundle:
    run: ReconciliationRun
    queries: tuple[ReconciliationQueryEvidence, ...]
    overall_status: QueryStatus
    overall_completeness: Completeness
    missing_required_types: tuple[QueryType, ...]
    invalid_evidence_ids: tuple[str, ...] = ()

    @property
    def complete_for_recovery(self) -> bool:
        return (
            self.run.state == ReconciliationRunState.COMPLETE
            and self.overall_status == QueryStatus.SUCCESS
            and self.overall_completeness == Completeness.COMPLETE
            and not self.missing_required_types
            and not self.invalid_evidence_ids
        )


def build_reconciliation_bundle(
    run: ReconciliationRun,
    queries: tuple[ReconciliationQueryEvidence, ...],
) -> ReconciliationEvidenceBundle:
    query_types = {query.query_type for query in queries}
    missing = (
        tuple(run.required_query_types)
        if run.state != ReconciliationRunState.COMPLETE
        else tuple(query_type for query_type in run.required_query_types if query_type not in query_types)
    )
    invalid: list[str] = []
    for evidence in queries:
        if evidence.account != run.account:
            raise ValueError("query account does not match run")
        if evidence.scope == QueryScope.INSTRUMENT and evidence.instrument != run.instrument:
            raise ValueError("query instrument does not match run")
        if not evidence.hash_binds_payload:
            invalid.append(evidence.query_id)
    statuses = {evidence.status for evidence in queries}
    completeness_values = {evidence.completeness for evidence in queries}
    if statuses & {QueryStatus.FAILED, QueryStatus.TIMEOUT}:
        status = QueryStatus.FAILED
    elif statuses & {QueryStatus.PARTIAL, QueryStatus.RATE_LIMITED}:
        status = QueryStatus.PARTIAL
    else:
        status = QueryStatus.SUCCESS
    severity = {
        Completeness.COMPLETE: 0,
        Completeness.INCOMPLETE_PAGINATED: 1,
        Completeness.INCOMPLETE_TRUNCATED: 2,
        Completeness.INCOMPLETE_RETENTION_LIMIT: 3,
        Completeness.UNKNOWN: 4,
    }
    completeness = max(completeness_values, key=severity.__getitem__) if completeness_values else Completeness.UNKNOWN
    return ReconciliationEvidenceBundle(
        run,
        queries,
        status,
        completeness,
        missing,
        tuple(invalid),
    )
