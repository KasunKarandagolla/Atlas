"""Bounded, restart-safe production of already-supported matured outcomes.

The coordinator owns no connection, worker or queue.  Each call uses the
controller's existing :class:`OpsRepository`, scans one stable calendar page,
and advances an immutable cursor only after the page has been handled.  The
per-decision status artifacts describe production progress; they are not
outcome labels and cannot be used as payoff evidence.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from atlas.v2._serialization import json_value, sha256_json, strict_fields, timestamp
from atlas.v2.memory.repository import (
    ArtifactIndexEntryV2,
    ArtifactIndexPageV2,
    ArtifactMetadataIdentityPageV1,
    OpsRepository,
)
from atlas.v2.science.outcomes import (
    DecisionCalendarEntryV2,
    LabelStateV2,
    MaturedOutcomeV2,
    index_matured_outcome,
)

try:  # Kept as a module seam so focused tests and the resolver can be injected.
    from atlas.v2.science.outcome_resolution import OutcomeResolutionV1, resolve_decision_outcome
except ImportError:  # The resolver is supplied by the parallel implementation task.
    OutcomeResolutionV1 = Any  # type: ignore[misc,assignment]
    resolve_decision_outcome = None  # type: ignore[assignment]


DECISION_PAGE_SIZE = 8
MAX_OUTCOMES_ATTEMPTED = 8
CHECKPOINT_READ_LIMIT = 2
MAX_METADATA_IDENTITY_LOOKUPS_PER_DECISION = 16
MAX_METADATA_IDENTITY_MATCHES_PER_QUERY = 8
MAX_RAW_EVIDENCE_ROWS_PER_DECISION = 1_024
MAX_REPLAY_ARTIFACTS_PER_DECISION = 512
MAX_CANDIDATE_DECISIONS_INSPECTED = DECISION_PAGE_SIZE
MAX_ARTIFACT_PAGES_READ = (
    2
    + MAX_OUTCOMES_ATTEMPTED * (MAX_METADATA_IDENTITY_LOOKUPS_PER_DECISION + 2)
    + (MAX_CANDIDATE_DECISIONS_INSPECTED - MAX_OUTCOMES_ATTEMPTED)
)
MAX_RAW_EVIDENCE_ROWS_PER_CYCLE = (
    2
    + MAX_CANDIDATE_DECISIONS_INSPECTED
    + MAX_OUTCOMES_ATTEMPTED * (MAX_RAW_EVIDENCE_ROWS_PER_DECISION + 2
                                + MAX_METADATA_IDENTITY_MATCHES_PER_QUERY + 1)
    + (MAX_CANDIDATE_DECISIONS_INSPECTED - MAX_OUTCOMES_ATTEMPTED)
    * (MAX_METADATA_IDENTITY_MATCHES_PER_QUERY + 1)
)
MAX_REPLAY_ARTIFACTS_PER_CYCLE = MAX_OUTCOMES_ATTEMPTED * MAX_REPLAY_ARTIFACTS_PER_DECISION
MAX_RETAINED_IN_MEMORY_WORK_ITEMS = DECISION_PAGE_SIZE + 2

DECISION_CALENDAR_ARTIFACT_TYPE = "DecisionCalendarEntryV2"
OUTCOME_ARTIFACT_TYPE = "MaturedOutcomeV2"
CHECKPOINT_ARTIFACT_TYPE = "OutcomeMaturityCheckpointV1"
STATUS_ARTIFACT_TYPE = "OutcomeMaturityStatusV1"
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


@dataclass(frozen=True)
class OutcomeMaturityCheckpointV1:
    """Monotonic keyset position; each cursor state is a separate artifact."""

    generation: int
    cursor_created_at_ns: int | None
    cursor_artifact_ref: str | None
    written_at_ns: int

    VERSION = "OutcomeMaturityCheckpointV1"

    def __post_init__(self) -> None:
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("checkpoint generation must be a nonnegative integer")
        if (self.cursor_created_at_ns is None) != (self.cursor_artifact_ref is None):
            raise ValueError("checkpoint cursor key must be fully present or absent")
        if self.cursor_created_at_ns is not None:
            timestamp(self.cursor_created_at_ns, field="cursor_created_at_ns")
            if not isinstance(self.cursor_artifact_ref, str):
                raise ValueError("checkpoint cursor ref must be text")
            from atlas.v2._serialization import sha256_ref

            sha256_ref(self.cursor_artifact_ref, field="cursor_artifact_ref")
        timestamp(self.written_at_ns, field="written_at_ns")

    @property
    def cursor(self) -> tuple[int, str] | None:
        if self.cursor_created_at_ns is None or self.cursor_artifact_ref is None:
            return None
        return self.cursor_created_at_ns, self.cursor_artifact_ref

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OutcomeMaturityCheckpointV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported outcome maturity checkpoint")
        return cls(**row)


@dataclass(frozen=True)
class OutcomeMaturityStatusV1:
    """Immutable non-label lifecycle classification for one exact decision."""

    decision_ref: str
    status: str
    horizon_end_ns: int | None
    reason_code: str | None
    observed_at_ns: int

    VERSION = "OutcomeMaturityStatusV1"
    _ALLOWED = frozenset({"PENDING", "MATURABLE", "MATURED", "UNRESOLVED", "CENSORED", "UNSUPPORTED"})

    def __post_init__(self) -> None:
        from atlas.v2._serialization import nonblank, sha256_ref

        sha256_ref(self.decision_ref, field="decision_ref")
        if self.status not in self._ALLOWED:
            raise ValueError("unsupported outcome lifecycle status")
        if self.horizon_end_ns is not None:
            timestamp(self.horizon_end_ns, field="horizon_end_ns")
        timestamp(self.observed_at_ns, field="observed_at_ns")
        if self.reason_code is not None:
            nonblank(self.reason_code, field="reason_code")
            if _SAFE_CODE.fullmatch(self.reason_code) is None:
                raise ValueError("outcome lifecycle reason code must be sanitized")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OutcomeMaturityStatusV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported outcome maturity status")
        return cls(**row)


@dataclass(frozen=True)
class OutcomeMaturityCycleReportV1:
    """Bounded operational observations from one maturity coordinator call."""

    cycle_at_ns: int
    decisions_inspected: int = 0
    outcomes_attempted: int = 0
    outcomes_indexed: int = 0
    pending_count: int = 0
    maturable_count: int = 0
    matured_count: int = 0
    unresolved_count: int = 0
    censored_count: int = 0
    unsupported_count: int = 0
    invalid_calendar_entries: int = 0
    conflicting_decisions: int = 0
    status_artifacts_written: int = 0
    existing_outcomes_checked: int = 0
    artifact_pages_read: int = 0
    raw_evidence_rows_inspected: int = 0
    replay_artifacts_resolved: int = 0
    retained_work_items: int = 0
    bounded_work_exhausted: bool = False
    wrapped: bool = False
    oldest_pending_age_ns: int | None = None
    oldest_maturable_age_ns: int | None = None
    checkpoint_ref: str | None = None
    failure_code: str | None = None
    failure_type: str | None = None

    VERSION = "OutcomeMaturityCycleReportV1"

    def __post_init__(self) -> None:
        timestamp(self.cycle_at_ns, field="cycle_at_ns")
        for name in (
            "decisions_inspected", "outcomes_attempted", "outcomes_indexed", "pending_count",
            "maturable_count", "matured_count", "unresolved_count", "censored_count",
            "unsupported_count", "invalid_calendar_entries", "conflicting_decisions",
            "status_artifacts_written", "existing_outcomes_checked", "artifact_pages_read",
            "raw_evidence_rows_inspected", "replay_artifacts_resolved", "retained_work_items",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        for name in ("oldest_pending_age_ns", "oldest_maturable_age_ns"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be nonnegative or absent")
        if self.checkpoint_ref is not None:
            from atlas.v2._serialization import sha256_ref

            sha256_ref(self.checkpoint_ref, field="checkpoint_ref")
        for name in ("failure_code", "failure_type"):
            value = getattr(self, name)
            if value is not None and _SAFE_CODE.fullmatch(value) is None:
                raise ValueError(f"{name} must be sanitized")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}}


def _safe_exception_type(error: BaseException) -> str:
    name = type(error).__name__.upper()
    safe = re.sub(r"[^A-Z0-9_]", "_", name)[:48]
    return safe if safe and safe[0].isalpha() else "UNKNOWN_EXCEPTION"


def _safe_reason_code(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and _SAFE_CODE.fullmatch(value):
        return value
    return "UNSAFE_REASON_CODE"


def _read_checkpoint(
    repository: OpsRepository, now_ns: int,
) -> tuple[OutcomeMaturityCheckpointV1 | None, str | None, int]:
    page = repository.artifact_entries_by_types_page(
        (CHECKPOINT_ARTIFACT_TYPE,), as_of_ns=now_ns, limit=CHECKPOINT_READ_LIMIT
    )
    if page.invalid_entry_count:
        return None, "MALFORMED_CHECKPOINT_INDEX", len(page.entries) + page.invalid_entry_count
    if not page.entries:
        return None, None, 0
    if len(page.entries) == 2 and page.entries[0].created_at_ns == page.entries[1].created_at_ns:
        return None, "CONFLICTING_CHECKPOINT_TIME", len(page.entries)
    latest = page.entries[0]
    try:
        body = latest.metadata.get("checkpoint")
        if (latest.artifact_type != CHECKPOINT_ARTIFACT_TYPE or not isinstance(body, Mapping)
                or latest.content_hash != latest.artifact_ref or latest.artifact_ref != sha256_json(body)):
            return None, "MALFORMED_CHECKPOINT", len(page.entries)
        checkpoint = OutcomeMaturityCheckpointV1.from_dict(json_value(body))
        if (checkpoint.content_hash != latest.artifact_ref or checkpoint.written_at_ns != latest.created_at_ns
                or checkpoint.written_at_ns != latest.available_at_ns):
            return None, "CONFLICTING_CHECKPOINT_CONTENT", len(page.entries)
        return checkpoint, None, len(page.entries)
    except (TypeError, ValueError, KeyError):
        return None, "MALFORMED_CHECKPOINT", len(page.entries)


def _write_checkpoint(
    repository: OpsRepository,
    *,
    generation: int,
    cursor: tuple[int, str] | None,
    now_ns: int,
) -> str:
    checkpoint = OutcomeMaturityCheckpointV1(
        generation,
        None if cursor is None else cursor[0],
        None if cursor is None else cursor[1],
        now_ns,
    )
    repository.register_artifact(ArtifactIndexEntryV2(
        checkpoint.content_hash, CHECKPOINT_ARTIFACT_TYPE, checkpoint.content_hash,
        now_ns, now_ns, {"checkpoint": checkpoint.to_dict()},
    ))
    return checkpoint.content_hash


@dataclass
class _CycleReadBudget:
    pages: int = 0
    rows: int = 0
    replay_artifacts: int = 0


class _ReadBudgetExceeded(RuntimeError):
    """Raised before any resolver call can exceed its declared work budget."""


class _BudgetedRepository:
    """Duck-typed, per-decision budget view over the controller's one repository."""

    def __init__(self, repository: OpsRepository, totals: _CycleReadBudget) -> None:
        self._repository = repository
        self._totals = totals
        self._rows = 0
        self._pages = 0
        self._replay_artifacts = 0
        self._identity_lookups = 0
        self._resolver_active = False

    @property
    def read_only(self) -> bool:
        return self._repository.read_only

    def _reserve(self, *, rows: int = 0, pages: int = 0, replay: int = 0) -> None:
        reserved_rows = MAX_METADATA_IDENTITY_MATCHES_PER_QUERY + 1 if self._resolver_active else 0
        reserved_pages = 1 if self._resolver_active else 0
        if (self._rows + rows > MAX_RAW_EVIDENCE_ROWS_PER_DECISION
                or self._rows + rows + reserved_rows > MAX_RAW_EVIDENCE_ROWS_PER_DECISION
                or self._pages + pages > MAX_METADATA_IDENTITY_LOOKUPS_PER_DECISION
                or self._pages + pages + reserved_pages > MAX_METADATA_IDENTITY_LOOKUPS_PER_DECISION
                or self._replay_artifacts + replay > MAX_REPLAY_ARTIFACTS_PER_DECISION
                or self._totals.rows + rows > MAX_RAW_EVIDENCE_ROWS_PER_CYCLE
                or self._totals.pages + pages > MAX_ARTIFACT_PAGES_READ
                or self._totals.replay_artifacts + replay > MAX_REPLAY_ARTIFACTS_PER_CYCLE):
            raise _ReadBudgetExceeded("outcome resolution exceeded its bounded read budget")
        self._rows += rows
        self._pages += pages
        self._replay_artifacts += replay
        self._totals.rows += rows
        self._totals.pages += pages
        self._totals.replay_artifacts += replay

    def begin_resolver(self) -> None:
        self._resolver_active = True

    def end_resolver(self) -> None:
        self._resolver_active = False

    def get_artifact(self, artifact_ref: str) -> ArtifactIndexEntryV2 | None:
        self._reserve(rows=1)
        entry = self._repository.get_artifact(artifact_ref)
        if entry is not None and entry.artifact_type in {"PolicyPayoffV2", "ReplayPathV2"}:
            self._reserve(replay=1)
        return entry

    def artifact_entries_by_metadata_identity(
        self,
        artifact_type: str,
        metadata_path: tuple[str, ...] | list[str],
        identity_value: str,
        *,
        as_of_ns: int,
        after: tuple[int, str] | None = None,
        limit: int = MAX_METADATA_IDENTITY_MATCHES_PER_QUERY,
    ) -> ArtifactMetadataIdentityPageV1:
        if limit > MAX_METADATA_IDENTITY_MATCHES_PER_QUERY:
            raise _ReadBudgetExceeded("metadata identity query exceeds its match bound")
        if self._identity_lookups >= MAX_METADATA_IDENTITY_LOOKUPS_PER_DECISION:
            raise _ReadBudgetExceeded("metadata identity query count exceeds its bound")
        self._identity_lookups += 1
        self._reserve(rows=limit + 1, pages=1)
        page = self._repository.artifact_entries_by_metadata_identity(
            artifact_type, metadata_path, identity_value, as_of_ns=as_of_ns, after=after, limit=limit
        )
        replay_rows = sum(entry.artifact_type in {"PolicyPayoffV2", "ReplayPathV2"} for entry in page.entries)
        if replay_rows:
            self._reserve(replay=replay_rows)
        return page

    def artifact_entries_by_types_page(
        self,
        artifact_types: tuple[str, ...] | list[str],
        *,
        as_of_ns: int,
        after: tuple[int, str] | None = None,
        limit: int = DECISION_PAGE_SIZE,
    ) -> ArtifactIndexPageV2:
        if limit > DECISION_PAGE_SIZE:
            raise _ReadBudgetExceeded("typed page query exceeds its row bound")
        self._reserve(rows=limit, pages=1)
        page = self._repository.artifact_entries_by_types_page(
            artifact_types, as_of_ns=as_of_ns, after=after, limit=limit
        )
        replay_rows = sum(entry.artifact_type in {"PolicyPayoffV2", "ReplayPathV2"} for entry in page.entries)
        if replay_rows:
            self._reserve(replay=replay_rows)
        return page

    def register_artifact(self, entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        return self._repository.register_artifact(entry)

    def register_artifacts(self, entries: tuple[ArtifactIndexEntryV2, ...]) -> tuple[ArtifactIndexEntryV2, ...]:
        return self._repository.register_artifacts(entries)


def _validate_calendar_entry(entry: ArtifactIndexEntryV2, now_ns: int) -> DecisionCalendarEntryV2:
    if (entry.artifact_type != DECISION_CALENDAR_ARTIFACT_TYPE or entry.content_hash != entry.artifact_ref
            or entry.available_at_ns > now_ns):
        raise ValueError("decision calendar index identity or availability mismatch")
    body = entry.metadata.get("decision_entry")
    if not isinstance(body, Mapping):
        raise ValueError("decision calendar typed body is unavailable")
    decision = DecisionCalendarEntryV2.from_dict(json_value(body))
    if (decision.content_hash != entry.artifact_ref or entry.created_at_ns != decision.created_at_ns
            or entry.available_at_ns != decision.available_at_ns):
        raise ValueError("decision calendar content or chronology mismatch")
    return decision


def _record_status(
    repository: _BudgetedRepository,
    *,
    decision_ref: str,
    status: str,
    horizon_end_ns: int | None,
    reason_code: str | None,
    now_ns: int,
) -> tuple[str | None, str | None]:
    status_record = OutcomeMaturityStatusV1(
        decision_ref, status, horizon_end_ns, _safe_reason_code(reason_code), now_ns
    )
    try:
        page = repository.artifact_entries_by_metadata_identity(
            STATUS_ARTIFACT_TYPE, ("status", "decision_ref"), decision_ref,
            as_of_ns=now_ns, limit=MAX_METADATA_IDENTITY_MATCHES_PER_QUERY,
        )
    except _ReadBudgetExceeded:
        return None, "STATUS_READ_BUDGET_EXHAUSTED"
    if page.invalid_entry_count:
        return None, "MALFORMED_STATUS_INDEX"
    if len(page.entries) > 1 and page.entries[0].created_at_ns == page.entries[1].created_at_ns:
        return None, "AMBIGUOUS_STATUS_ORDER"
    if page.entries:
        latest = page.entries[0]
        body = latest.metadata.get("status")
        try:
            previous = OutcomeMaturityStatusV1.from_dict(json_value(body)) if isinstance(body, Mapping) else None
            if (latest.artifact_type != STATUS_ARTIFACT_TYPE or latest.content_hash != latest.artifact_ref
                    or not isinstance(previous, OutcomeMaturityStatusV1)
                    or previous.content_hash != latest.artifact_ref
                    or latest.created_at_ns != previous.observed_at_ns
                    or latest.available_at_ns != previous.observed_at_ns
                    or previous.decision_ref != decision_ref):
                return None, "MALFORMED_STATUS_ARTIFACT"
            if (previous.status, previous.horizon_end_ns, previous.reason_code) == (
                    status_record.status, status_record.horizon_end_ns, status_record.reason_code):
                return None, None
            if previous.status in {"MATURED", "CENSORED", "UNSUPPORTED"}:
                return None, "TERMINAL_STATUS_REGRESSION"
            if (previous.status == "UNRESOLVED" and previous.reason_code != "CYCLE_OUTCOME_BUDGET"
                    and status_record.status in {"PENDING", "MATURABLE"}):
                return None, "NONMONOTONIC_STATUS_TRANSITION"
        except (TypeError, ValueError, KeyError):
            return None, "MALFORMED_STATUS_ARTIFACT"
    repository.register_artifact(ArtifactIndexEntryV2(
        status_record.content_hash, STATUS_ARTIFACT_TYPE, status_record.content_hash,
        now_ns, now_ns, {"status": status_record.to_dict()},
    ))
    return status_record.content_hash, None


def _failure_report(now_ns: int, code: str, error: BaseException | None = None, **values: Any) -> OutcomeMaturityCycleReportV1:
    return OutcomeMaturityCycleReportV1(
        cycle_at_ns=now_ns,
        failure_code=code,
        failure_type=None if error is None else _safe_exception_type(error),
        **values,
    )


def run_outcome_maturity_cycle(repository: OpsRepository, now_ns: int) -> OutcomeMaturityCycleReportV1:
    """Inspect and classify one bounded page of exact decision-calendar rows.

    The cycle never manufactures a target, uses wall time to infer a horizon,
    admits post-cutoff artifacts, or changes a decision receipt.  It indexes
    only a terminal typed outcome returned by the evidence resolver and then
    re-runs the existing ``index_matured_outcome`` validator.
    """
    timestamp(now_ns, field="outcome maturity cycle time")
    if repository.read_only:
        return _failure_report(now_ns, "WRITER_REPOSITORY_REQUIRED")
    resolver = resolve_decision_outcome
    if resolver is None:
        return _failure_report(now_ns, "RESOLVER_UNAVAILABLE")

    try:
        checkpoint, checkpoint_error, checkpoint_rows = _read_checkpoint(repository, now_ns)
    except Exception as error:
        return _failure_report(now_ns, "CHECKPOINT_READ_FAILURE", error)
    if checkpoint_error is not None:
        return _failure_report(now_ns, checkpoint_error, raw_evidence_rows_inspected=checkpoint_rows,
                               artifact_pages_read=1)
    if checkpoint is not None and now_ns <= checkpoint.written_at_ns:
        return _failure_report(
            now_ns, "CHECKPOINT_CLOCK_NOT_ADVANCED", checkpoint_ref=checkpoint.content_hash,
            raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1,
        )

    cursor = checkpoint.cursor if checkpoint is not None else None
    generation = checkpoint.generation if checkpoint is not None else 0
    try:
        page: ArtifactIndexPageV2 = repository.artifact_entries_by_types_page(
            (DECISION_CALENDAR_ARTIFACT_TYPE,), as_of_ns=now_ns, after=cursor, limit=DECISION_PAGE_SIZE
        )
    except Exception as error:
        return _failure_report(now_ns, "CALENDAR_PAGE_READ_FAILURE", error,
                               raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1)
    base_rows = checkpoint_rows + len(page.entries) + page.invalid_entry_count
    base_pages = 2

    if not page.entries:
        if checkpoint is None and cursor is None:
            return OutcomeMaturityCycleReportV1(
                cycle_at_ns=now_ns, invalid_calendar_entries=page.invalid_entry_count,
                artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows,
            )
        try:
            checkpoint_ref = _write_checkpoint(repository, generation=generation + 1, cursor=None, now_ns=now_ns)
        except Exception as error:
            return _failure_report(now_ns, "CHECKPOINT_WRITE_FAILURE", error,
                                   invalid_calendar_entries=page.invalid_entry_count,
                                   artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)
        return OutcomeMaturityCycleReportV1(
            cycle_at_ns=now_ns, invalid_calendar_entries=page.invalid_entry_count,
            wrapped=True, checkpoint_ref=checkpoint_ref, artifact_pages_read=base_pages,
            raw_evidence_rows_inspected=base_rows,
        )

    if page.next_cursor is None or (cursor is not None and page.next_cursor >= cursor):
        return _failure_report(now_ns, "NON_MONOTONIC_CALENDAR_CURSOR",
                               decisions_inspected=len(page.entries),
                               invalid_calendar_entries=page.invalid_entry_count,
                               artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)

    totals = _CycleReadBudget(pages=base_pages, rows=base_rows)
    pending = maturable = matured = unresolved = censored = unsupported = 0
    outcomes_attempted = outcomes_indexed = conflicting = status_written = existing_outcomes_checked = 0
    invalid_calendar = page.invalid_entry_count
    oldest_pending_age: int | None = None
    oldest_maturable_age: int | None = None
    cycle_failure: str | None = None
    cycle_failure_type: str | None = None
    attempt_bound_deferred = False

    for indexed_calendar in page.entries:
        try:
            decision = _validate_calendar_entry(indexed_calendar, now_ns)
        except (KeyError, TypeError, ValueError):
            invalid_calendar += 1
            budgeted = _BudgetedRepository(repository, totals)
            try:
                status_ref, status_error = _record_status(
                    budgeted, decision_ref=indexed_calendar.artifact_ref, status="UNRESOLVED",
                    horizon_end_ns=None, reason_code="MALFORMED_CALENDAR_ENTRY", now_ns=now_ns,
                )
                status_written += status_ref is not None
                if status_error is not None and cycle_failure is None:
                    cycle_failure = status_error
            except Exception as error:
                if cycle_failure is None:
                    cycle_failure = "MALFORMED_CALENDAR_STATUS_FAILURE"
                    cycle_failure_type = _safe_exception_type(error)
            continue
        budgeted = _BudgetedRepository(repository, totals)

        def record(
            status_value: str,
            horizon_value: int | None,
            reason_value: str | None,
            *,
            _budgeted: _BudgetedRepository = budgeted,
            _decision_ref: str = indexed_calendar.artifact_ref,
        ) -> None:
            nonlocal status_written, cycle_failure, cycle_failure_type
            if status_value in {"MATURED", "CENSORED"}:
                reason_value = "TERMINAL_OUTCOME_SUPPORTED"
            try:
                status_ref, status_error = _record_status(
                    _budgeted, decision_ref=_decision_ref, status=status_value,
                    horizon_end_ns=horizon_value, reason_code=reason_value, now_ns=now_ns,
                )
                status_written += status_ref is not None
                if status_error is not None and cycle_failure is None:
                    cycle_failure = status_error
            except Exception as error:
                if cycle_failure is None:
                    cycle_failure = "STATUS_WRITE_FAILURE"
                    cycle_failure_type = _safe_exception_type(error)

        if outcomes_attempted >= MAX_OUTCOMES_ATTEMPTED:
            attempt_bound_deferred = True
            unresolved += 1
            record("UNRESOLVED", None, "CYCLE_OUTCOME_BUDGET")
            continue
        outcomes_attempted += 1

        try:
            outcome_page = budgeted.artifact_entries_by_metadata_identity(
                OUTCOME_ARTIFACT_TYPE, ("outcome", "decision_ref"), indexed_calendar.artifact_ref,
                as_of_ns=now_ns, limit=1,
            )
            existing_outcomes_checked += 1
            if outcome_page.invalid_entry_count or outcome_page.has_more:
                conflicting += 1
                unresolved += 1
                record("UNRESOLVED", None, "CONFLICTING_OUTCOME")
                continue
            existing_outcome: MaturedOutcomeV2 | None = None
            existing_ref: str | None = None
            if outcome_page.entries:
                existing_entry = outcome_page.entries[0]
                body = existing_entry.metadata.get("outcome")
                if (existing_entry.artifact_type != OUTCOME_ARTIFACT_TYPE or not isinstance(body, Mapping)
                        or existing_entry.content_hash != existing_entry.artifact_ref):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", None, "MALFORMED_OUTCOME")
                    continue
                existing_outcome = MaturedOutcomeV2.from_dict(json_value(body))
                existing_ref = existing_entry.artifact_ref
                if (existing_outcome.content_hash != existing_ref
                        or existing_outcome.decision_ref != indexed_calendar.artifact_ref
                        or existing_entry.created_at_ns != existing_outcome.matured_at_ns
                        or existing_entry.available_at_ns != existing_outcome.available_at_ns):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", existing_outcome.horizon_end_ns, "CONFLICTING_OUTCOME")
                    continue
                if existing_outcome.label_state not in (LabelStateV2.MATURED, LabelStateV2.CENSORED):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", existing_outcome.horizon_end_ns, "NONTERMINAL_OUTCOME_EXISTS")
                    continue
                if index_matured_outcome(cast(OpsRepository, budgeted), existing_outcome) != existing_ref:
                    raise ValueError("stored outcome identity changed")
                resolved_status = "MATURED" if existing_outcome.label_state == LabelStateV2.MATURED else "CENSORED"
                matured += resolved_status == "MATURED"
                censored += resolved_status == "CENSORED"
                record(resolved_status, existing_outcome.horizon_end_ns, "EXISTING_VALIDATED_OUTCOME")
                continue

            budgeted.begin_resolver()
            try:
                resolution = resolver(cast(OpsRepository, budgeted), indexed_calendar, now_ns)
                status_value = getattr(resolution, "status", None)
                status_value = getattr(status_value, "value", status_value)
                horizon = getattr(resolution, "horizon_end_ns", None)
                reason = _safe_reason_code(getattr(resolution, "reason_code", None))
                outcome = getattr(resolution, "outcome", None)
                if getattr(resolution, "decision_ref", indexed_calendar.artifact_ref) != indexed_calendar.artifact_ref:
                    raise ValueError("resolver returned another decision identity")
                if status_value not in OutcomeMaturityStatusV1._ALLOWED:
                    raise ValueError("resolver returned an unsupported lifecycle state")
                if horizon is not None:
                    timestamp(horizon, field="resolved horizon_end_ns")
                if status_value in {"UNRESOLVED", "MATURABLE"} and horizon is not None and horizon <= now_ns:
                    maturable += 1
                    age = max(0, now_ns - decision.decision_at_ns)
                    oldest_maturable_age = age if oldest_maturable_age is None else max(oldest_maturable_age, age)
                if status_value == "MATURED":
                    if not isinstance(outcome, MaturedOutcomeV2):
                        raise ValueError("matured resolver state lacks a typed outcome")
                    if (outcome.decision_ref != indexed_calendar.artifact_ref
                            or outcome.label_state != LabelStateV2.MATURED):
                        raise ValueError("resolved outcome decision or terminal state mismatch")
                    if (outcome.horizon_end_ns != horizon or outcome.horizon_end_ns > now_ns
                            or outcome.matured_at_ns > now_ns or outcome.available_at_ns > now_ns):
                        raise ValueError("resolved outcome is not causally available")
                    if index_matured_outcome(cast(OpsRepository, budgeted), outcome) != outcome.content_hash:
                        raise ValueError("outcome index returned a different identity")
                    outcomes_indexed += 1
                    matured += 1
                elif status_value == "CENSORED":
                    if (not isinstance(outcome, MaturedOutcomeV2)
                            or outcome.label_state != LabelStateV2.CENSORED
                            or outcome.decision_ref != indexed_calendar.artifact_ref
                            or outcome.horizon_end_ns != horizon
                            or outcome.horizon_end_ns > now_ns
                            or outcome.matured_at_ns > now_ns or outcome.available_at_ns > now_ns):
                        raise ValueError("censored outcome requires exact terminal evidence and chronology")
                    if index_matured_outcome(cast(OpsRepository, budgeted), outcome) != outcome.content_hash:
                        raise ValueError("censored outcome index returned a different identity")
                    outcomes_indexed += 1
                    censored += 1
                elif status_value == "UNRESOLVED":
                    if outcome is not None:
                        raise ValueError("interim unresolved state cannot create an outcome label")
                    unresolved += 1
                elif status_value == "UNSUPPORTED":
                    if outcome is not None:
                        raise ValueError("unsupported target cannot create an outcome label")
                    unsupported += 1
                elif status_value == "PENDING":
                    if outcome is not None or horizon is None or horizon <= now_ns:
                        raise ValueError("pending state requires a future exact horizon and no label")
                    pending += 1
                    age = max(0, now_ns - decision.decision_at_ns)
                    oldest_pending_age = age if oldest_pending_age is None else max(oldest_pending_age, age)
                elif status_value == "MATURABLE":
                    if outcome is not None or horizon is None or horizon > now_ns:
                        raise ValueError("maturable state requires an elapsed exact horizon and no label")
            finally:
                budgeted.end_resolver()
            record(status_value, horizon, reason)
        except Exception as error:
            unresolved += 1
            if cycle_failure is None:
                cycle_failure = "RESOLUTION_OR_INDEX_FAILURE"
                cycle_failure_type = _safe_exception_type(error)
            record("UNRESOLVED", None, "RESOLUTION_OR_INDEX_FAILURE")

    try:
        checkpoint_ref = _write_checkpoint(
            repository, generation=generation, cursor=page.next_cursor, now_ns=now_ns
        )
    except Exception as error:
        return _failure_report(
            now_ns, "CHECKPOINT_WRITE_FAILURE", error,
            decisions_inspected=len(page.entries), outcomes_attempted=outcomes_attempted,
            outcomes_indexed=outcomes_indexed, pending_count=pending, maturable_count=maturable,
            matured_count=matured, unresolved_count=unresolved, censored_count=censored,
            unsupported_count=unsupported, invalid_calendar_entries=invalid_calendar,
            conflicting_decisions=conflicting, status_artifacts_written=status_written,
            existing_outcomes_checked=existing_outcomes_checked, artifact_pages_read=totals.pages,
            raw_evidence_rows_inspected=totals.rows, replay_artifacts_resolved=totals.replay_artifacts,
            retained_work_items=min(MAX_RETAINED_IN_MEMORY_WORK_ITEMS, len(page.entries) + 2),
            bounded_work_exhausted=(len(page.entries) >= DECISION_PAGE_SIZE or invalid_calendar > 0
                                    or attempt_bound_deferred),
        )

    return OutcomeMaturityCycleReportV1(
        cycle_at_ns=now_ns,
        decisions_inspected=len(page.entries),
        outcomes_attempted=outcomes_attempted,
        outcomes_indexed=outcomes_indexed,
        pending_count=pending,
        maturable_count=maturable,
        matured_count=matured,
        unresolved_count=unresolved,
        censored_count=censored,
        unsupported_count=unsupported,
        invalid_calendar_entries=invalid_calendar,
        conflicting_decisions=conflicting,
        status_artifacts_written=status_written,
        existing_outcomes_checked=existing_outcomes_checked,
        artifact_pages_read=totals.pages,
        raw_evidence_rows_inspected=totals.rows,
        replay_artifacts_resolved=totals.replay_artifacts,
        retained_work_items=min(MAX_RETAINED_IN_MEMORY_WORK_ITEMS, len(page.entries) + 2),
        bounded_work_exhausted=(len(page.entries) >= DECISION_PAGE_SIZE or invalid_calendar > 0
                                or attempt_bound_deferred),
        oldest_pending_age_ns=oldest_pending_age,
        oldest_maturable_age_ns=oldest_maturable_age,
        checkpoint_ref=checkpoint_ref,
        failure_code=cycle_failure,
        failure_type=cycle_failure_type,
    )
