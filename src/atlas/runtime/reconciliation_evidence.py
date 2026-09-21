"""REST/reconciliation query evidence types (freeze §1.4, §1.6).

Negative lookup alone must NEVER establish no order.
Create typed query/reconciliation evidence representing:
- query type, account/instrument
- requested time interval, pagination cursors
- pages observed, completeness
- source/receipt timestamps, request IDs
- history/retention coverage
- evidence hash
- success/failure/partial status

Incomplete != empty.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from atlas.domain.time import ensure_utc_ns


class QueryType(StrEnum):
    """Types of reconciliation queries."""
    OPEN_ORDERS = "open_orders"
    ORDER_HISTORY = "order_history"
    EXECUTION_HISTORY = "execution_history"
    POSITIONS = "positions"
    WALLET_BALANCE = "wallet_balance"
    TRADING_STOP = "trading_stop"
    CONDITIONAL_ORDERS = "conditional_orders"
    TRANSACTION_LOG = "transaction_log"


class QueryStatus(StrEnum):
    """Query execution status."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"


class Completeness(StrEnum):
    """Query result completeness."""
    COMPLETE = "complete"
    INCOMPLETE_PAGINATED = "incomplete_paginated"
    INCOMPLETE_TRUNCATED = "incomplete_truncated"
    INCOMPLETE_RETENTION_LIMIT = "incomplete_retention_limit"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ReconciliationQueryEvidence:
    """Complete evidence for a reconciliation query."""

    query_id: str
    query_type: QueryType
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
    retention_coverage_start_ns: int | None
    retention_coverage_end_ns: int | None
    evidence_hash: str
    error_message: str | None

    def __post_init__(self) -> None:
        if not self.query_id or not self.query_id.strip():
            raise ValueError("query_id must be non-blank")
        if not isinstance(self.query_type, QueryType):
            raise ValueError("query_type must be QueryType")
        if not self.account or not self.account.strip():
            raise ValueError("account must be non-blank")
        if self.instrument is not None and (not isinstance(self.instrument, str) or not self.instrument.strip()):
            raise ValueError("instrument must be non-blank if present")
        if self.requested_interval_start_ns is not None:
            ensure_utc_ns(self.requested_interval_start_ns, field="requested_interval_start_ns")
        if self.requested_interval_end_ns is not None:
            ensure_utc_ns(self.requested_interval_end_ns, field="requested_interval_end_ns")
        if (
            self.requested_interval_start_ns is not None
            and self.requested_interval_end_ns is not None
            and self.requested_interval_end_ns < self.requested_interval_start_ns
        ):
            raise ValueError("requested interval end cannot precede start")
        if not isinstance(self.pages_observed, int) or isinstance(self.pages_observed, bool) or self.pages_observed < 0:
            raise ValueError("pages_observed must be int >= 0")
        if not isinstance(self.total_records_returned, int) or isinstance(self.total_records_returned, bool) or self.total_records_returned < 0:
            raise ValueError("total_records_returned must be int >= 0")
        if not isinstance(self.completeness, Completeness):
            raise ValueError("completeness must be Completeness")
        if not isinstance(self.status, QueryStatus):
            raise ValueError("status must be QueryStatus")
        if self.source_time_ns is not None:
            ensure_utc_ns(self.source_time_ns, field="source_time_ns")
        ensure_utc_ns(self.receipt_time_ns, field="receipt_time_ns")
        if self.retention_coverage_start_ns is not None:
            ensure_utc_ns(self.retention_coverage_start_ns, field="retention_coverage_start_ns")
        if self.retention_coverage_end_ns is not None:
            ensure_utc_ns(self.retention_coverage_end_ns, field="retention_coverage_end_ns")
        if (
            self.retention_coverage_start_ns is not None
            and self.retention_coverage_end_ns is not None
            and self.retention_coverage_end_ns < self.retention_coverage_start_ns
        ):
            raise ValueError("retention coverage end cannot precede start")
        if not self.evidence_hash or not self.evidence_hash.strip():
            raise ValueError("evidence_hash must be non-blank")
        if len(self.evidence_hash) != 64:
            raise ValueError("evidence_hash must be 64 hex chars")

    @property
    def is_empty_result(self) -> bool:
        """True if query returned zero records."""
        return self.total_records_returned == 0

    @property
    def is_incomplete(self) -> bool:
        """True if query did not return complete result set."""
        return self.completeness != Completeness.COMPLETE

    @property
    def can_certify_absence(self) -> bool:
        """Can this query certify absence of orders/positions?

        Only if: status=SUCCESS, completeness=COMPLETE, is_empty_result=True,
        and retention coverage includes the full interval of interest.
        """
        return (
            self.status == QueryStatus.SUCCESS
            and self.completeness == Completeness.COMPLETE
            and self.is_empty_result
            and self.requested_interval_start_ns is not None
            and self.requested_interval_end_ns is not None
            and self.retention_coverage_start_ns is not None
            and self.retention_coverage_end_ns is not None
            and self.retention_coverage_start_ns <= self.requested_interval_start_ns
            and self.retention_coverage_end_ns >= self.requested_interval_end_ns
        )


def compute_evidence_hash(payload: dict[str, Any]) -> str:
    """Compute SHA256 hash of query response payload."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReconciliationEvidenceBundle:
    """Bundle of all query evidence for a reconciliation cycle."""

    reconciliation_run_id: str
    account: str
    instrument: str | None
    started_at_ns: int
    completed_at_ns: int
    queries: tuple[ReconciliationQueryEvidence, ...]
    overall_status: QueryStatus
    overall_completeness: Completeness

    def __post_init__(self) -> None:
        if not self.reconciliation_run_id or not self.reconciliation_run_id.strip():
            raise ValueError("reconciliation_run_id must be non-blank")
        if not self.account or not self.account.strip():
            raise ValueError("account must be non-blank")
        ensure_utc_ns(self.started_at_ns, field="started_at_ns")
        ensure_utc_ns(self.completed_at_ns, field="completed_at_ns")
        if self.completed_at_ns < self.started_at_ns:
            raise ValueError("completed_at_ns cannot precede started_at_ns")
        if not isinstance(self.overall_status, QueryStatus):
            raise ValueError("overall_status must be QueryStatus")
        if not isinstance(self.overall_completeness, Completeness):
            raise ValueError("overall_completeness must be Completeness")


def merge_query_evidence(
    evidence_list: list[ReconciliationQueryEvidence],
) -> ReconciliationQueryEvidence | None:
    """Merge multiple query evidences of the same type (e.g., paginated results).

    Returns merged evidence or None if list empty.
    """
    if not evidence_list:
        return None

    first = evidence_list[0]
    # Verify all same query_type, account, instrument
    for e in evidence_list[1:]:
        if e.query_type != first.query_type:
            raise ValueError("cannot merge different query_type")
        if e.account != first.account:
            raise ValueError("cannot merge different account")
        if e.instrument != first.instrument:
            raise ValueError("cannot merge different instrument")
        if (
            e.requested_interval_start_ns != first.requested_interval_start_ns
            or e.requested_interval_end_ns != first.requested_interval_end_ns
        ):
            raise ValueError("cannot merge inconsistent requested intervals")

    # Merge paginated results
    all_cursors: list[str] = []
    all_request_ids: list[str] = []
    total_records = 0
    for e in evidence_list:
        all_cursors.extend(e.pagination_cursors)
        all_request_ids.extend(e.request_ids)
        total_records += e.total_records_returned

    # Overall completeness is the worst of all
    # Larger severity wins.  The previous min() selected COMPLETE when any
    # page was incomplete, which could incorrectly certify a negative lookup.
    completeness_severity = {
        Completeness.COMPLETE: 0,
        Completeness.INCOMPLETE_PAGINATED: 1,
        Completeness.INCOMPLETE_TRUNCATED: 2,
        Completeness.INCOMPLETE_RETENTION_LIMIT: 3,
        Completeness.UNKNOWN: 4,
    }
    worst_completeness = max(
        (e.completeness for e in evidence_list),
        key=lambda c: completeness_severity[c],
    )

    # Overall status: if any failed, mark partial
    overall_status = QueryStatus.SUCCESS
    for e in evidence_list:
        if e.status in (QueryStatus.FAILED, QueryStatus.TIMEOUT):
            overall_status = QueryStatus.FAILED
            break
        elif e.status == QueryStatus.PARTIAL or e.status == QueryStatus.RATE_LIMITED:
            overall_status = QueryStatus.PARTIAL

    # Merge evidence hash
    merged_payload = {
        "queries": [e.evidence_hash for e in evidence_list],
    }
    merged_hash = compute_evidence_hash(merged_payload)

    return ReconciliationQueryEvidence(
        query_id=first.query_id + "-merged",
        query_type=first.query_type,
        account=first.account,
        instrument=first.instrument,
        requested_interval_start_ns=first.requested_interval_start_ns,
        requested_interval_end_ns=first.requested_interval_end_ns,
        pagination_cursors=tuple(all_cursors),
        pages_observed=sum(e.pages_observed for e in evidence_list),
        total_records_returned=total_records,
        completeness=worst_completeness,
        status=overall_status,
        source_time_ns=first.source_time_ns,
        receipt_time_ns=max(e.receipt_time_ns for e in evidence_list),
        request_ids=tuple(all_request_ids),
        retention_coverage_start_ns=(
            min(e.retention_coverage_start_ns for e in evidence_list
                if e.retention_coverage_start_ns is not None)
            if all(e.retention_coverage_start_ns is not None for e in evidence_list)
            else None
        ),
        retention_coverage_end_ns=(
            max(e.retention_coverage_end_ns for e in evidence_list
                if e.retention_coverage_end_ns is not None)
            if all(e.retention_coverage_end_ns is not None for e in evidence_list)
            else None
        ),
        evidence_hash=merged_hash,
        error_message=None,
    )
