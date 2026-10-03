"""Bounded, restart-safe production of already-supported matured outcomes.

The coordinator owns no connection, worker or queue.  Each call uses the
controller's existing :class:`OpsRepository`, scans one stable calendar page,
and advances an immutable cursor only after the page has been handled.  The
per-decision status artifacts describe production progress; they are not
outcome labels and cannot be used as payoff evidence.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
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
# A supported source may bind 1,024 raw rows, 256 minute artifacts, 256
# funding records, and 256 closed bars with their raw records, plus exact
# decision/action/cost identities and bounded resolver metadata pages.
MAX_RAW_EVIDENCE_ROWS_PER_DECISION = 4_096
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
OUTCOME_MAINTENANCE_BUDGET_VERSION = "OutcomeMaintenanceBudgetV1"
# Engineering allowance: 5% of the existing 1 s S3 BBO freshness window.
# Real-host adequacy remains UNVERIFIED / TEST GATE.
OUTCOME_MAINTENANCE_BUDGET_NS_V1 = 50_000_000
MAX_OUTCOME_MAINTENANCE_BUDGET_NS = 1_000_000_000
OUTCOME_DUE_RETRY_INTERVAL_NS_V1 = 60_000_000_000

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
    accounted_invalid_raw_keys: tuple[tuple[int, str], ...] = ()

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
            # Raw-key traversal preserves malformed refs exactly, including blank text.
        timestamp(self.written_at_ns, field="written_at_ns")
        for created_at_ns, artifact_ref in self.accounted_invalid_raw_keys:
            timestamp(created_at_ns, field="invalid raw created_at_ns")
            if not isinstance(artifact_ref, str):
                raise ValueError("invalid raw artifact_ref must be text")
        if len(set(self.accounted_invalid_raw_keys)) != len(self.accounted_invalid_raw_keys):
            raise ValueError("invalid raw key accounting must be unique")

    @property
    def cursor(self) -> tuple[int, str] | None:
        if self.cursor_created_at_ns is None or self.cursor_artifact_ref is None:
            return None
        return self.cursor_created_at_ns, self.cursor_artifact_ref

    def to_dict(self) -> dict[str, Any]:
        result = {
            "version": self.VERSION,
            "generation": self.generation,
            "cursor_created_at_ns": self.cursor_created_at_ns,
            "cursor_artifact_ref": self.cursor_artifact_ref,
            "written_at_ns": self.written_at_ns,
        }
        if self.accounted_invalid_raw_keys:
            result["accounted_invalid_raw_keys"] = [
                {"created_at_ns": created_at_ns, "artifact_ref": artifact_ref}
                for created_at_ns, artifact_ref in self.accounted_invalid_raw_keys
            ]
        return result

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OutcomeMaturityCheckpointV1:
        base_fields = {"generation", "cursor_created_at_ns", "cursor_artifact_ref", "written_at_ns"}
        fields = base_fields | {"version", "accounted_invalid_raw_keys"}
        row = dict(strict_fields(data, expected=fields, required=base_fields | {"version"}, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported outcome maturity checkpoint")
        raw_keys = row.pop("accounted_invalid_raw_keys", ())
        if not isinstance(raw_keys, (tuple, list)):
            raise ValueError("invalid raw key accounting must be a sequence")
        parsed_keys: list[tuple[int, str]] = []
        for item in raw_keys:
            if not isinstance(item, Mapping) or set(item) != {"created_at_ns", "artifact_ref"}:
                raise ValueError("invalid raw key accounting item is malformed")
            parsed_keys.append((item["created_at_ns"], item["artifact_ref"]))
        row["accounted_invalid_raw_keys"] = tuple(parsed_keys)
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
    evidence_cutoff_ns: int | None = None
    computation_started_ns: int | None = None
    computation_finished_ns: int | None = None
    maintenance_budget_version: str = OUTCOME_MAINTENANCE_BUDGET_VERSION
    maintenance_budget_ns: int = OUTCOME_MAINTENANCE_BUDGET_NS_V1
    maintenance_budget_status: str = "WITHIN_BUDGET"
    maintenance_budget_overrun_ns: int = 0
    maintenance_budget_qualification: str = "UNVERIFIED / TEST GATE"
    invalid_calendar_raw_keys: tuple[tuple[int, str], ...] = ()
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
    due_work: Mapping[str, Any] | None = None
    due_work_discovery: Mapping[str, Any] | None = None

    VERSION = "OutcomeMaturityCycleReportV1"

    def __post_init__(self) -> None:
        timestamp(self.cycle_at_ns, field="cycle_at_ns")
        for name in ("evidence_cutoff_ns", "computation_started_ns", "computation_finished_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if (self.computation_started_ns is not None and self.computation_finished_ns is not None
                and self.computation_finished_ns < self.computation_started_ns):
            raise ValueError("maintenance computation timestamps are not monotonic")
        if type(self.maintenance_budget_ns) is not int or not 1 <= self.maintenance_budget_ns <= MAX_OUTCOME_MAINTENANCE_BUDGET_NS:
            raise ValueError("maintenance budget must be between 1 ns and 1 s")
        if (type(self.maintenance_budget_overrun_ns) is not int
                or self.maintenance_budget_overrun_ns < 0):
            raise ValueError("maintenance budget overrun must be nonnegative")
        if self.maintenance_budget_status not in {
            "WITHIN_BUDGET", "MAINTENANCE_BUDGET_EXHAUSTED", "MAINTENANCE_DEADLINE_OVERRUN",
        }:
            raise ValueError("unsupported maintenance budget status")
        if self.maintenance_budget_status == "MAINTENANCE_DEADLINE_OVERRUN" and self.maintenance_budget_overrun_ns < 1:
            raise ValueError("deadline overrun status requires a positive overrun duration")
        for created_at_ns, artifact_ref in self.invalid_calendar_raw_keys:
            timestamp(created_at_ns, field="invalid_calendar_raw_key.created_at_ns")
            if not isinstance(artifact_ref, str):
                raise ValueError("invalid_calendar_raw_key.artifact_ref must be text")
        if len(self.invalid_calendar_raw_keys) > DECISION_PAGE_SIZE:
            raise ValueError("invalid raw calendar key report exceeds page bound")
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
    repository: OpsRepository, as_of_ns: int,
) -> tuple[OutcomeMaturityCheckpointV1 | None, str | None, int]:
    page = repository.artifact_entries_by_types_page(
        (CHECKPOINT_ARTIFACT_TYPE,), as_of_ns=as_of_ns, limit=CHECKPOINT_READ_LIMIT
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
    accounted_invalid_raw_keys: tuple[tuple[int, str], ...] = (),
) -> str:
    checkpoint = OutcomeMaturityCheckpointV1(
        generation,
        None if cursor is None else cursor[0],
        None if cursor is None else cursor[1],
        now_ns,
        accounted_invalid_raw_keys,
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


class _MaintenanceBudgetExpired(RuntimeError):
    """Raised before a resolver starts another evidence query after deadline."""


class _BudgetedRepository:
    """Duck-typed, per-decision budget view over the controller's one repository."""

    def __init__(
        self,
        repository: OpsRepository,
        totals: _CycleReadBudget,
        *,
        resolver_read_allowed: Callable[[], bool] | None = None,
    ) -> None:
        self._repository = repository
        self._totals = totals
        self._rows = 0
        self._pages = 0
        self._replay_artifacts = 0
        self._identity_lookups = 0
        self._resolver_active = False
        self._resolver_read_allowed = resolver_read_allowed
        self._artifact_cache: dict[str, ArtifactIndexEntryV2 | None] = {}

    @property
    def read_only(self) -> bool:
        return self._repository.read_only

    def _reserve(self, *, rows: int = 0, pages: int = 0, replay: int = 0) -> None:
        if (self._resolver_active and self._resolver_read_allowed is not None
                and not self._resolver_read_allowed()):
            raise _MaintenanceBudgetExpired("maintenance deadline elapsed before evidence query")
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
        if artifact_ref in self._artifact_cache:
            return self._artifact_cache[artifact_ref]
        self._reserve(rows=1)
        entry = self._repository.get_artifact(artifact_ref)
        if entry is not None and entry.artifact_type in {"PolicyPayoffV2", "ReplayPathV2"}:
            self._reserve(replay=1)
        self._artifact_cache[artifact_ref] = entry
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
        self._artifact_cache.pop(entry.artifact_ref, None)
        return self._repository.register_artifact(entry)

    def register_artifacts(self, entries: tuple[ArtifactIndexEntryV2, ...]) -> tuple[ArtifactIndexEntryV2, ...]:
        for entry in entries:
            self._artifact_cache.pop(entry.artifact_ref, None)
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
    production_clock_ns: Callable[[], int],
) -> tuple[str | None, str | None]:
    try:
        lookup_at_ns = production_clock_ns()
        page = repository.artifact_entries_by_metadata_identity(
            STATUS_ARTIFACT_TYPE, ("status", "decision_ref"), decision_ref,
            as_of_ns=lookup_at_ns, limit=MAX_METADATA_IDENTITY_MATCHES_PER_QUERY,
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
                    status, horizon_end_ns, _safe_reason_code(reason_code)):
                return None, None
            if previous.status in {"MATURED", "CENSORED", "UNSUPPORTED"}:
                return None, "TERMINAL_STATUS_REGRESSION"
            if (previous.status == "UNRESOLVED" and previous.reason_code != "CYCLE_OUTCOME_BUDGET"
                    and status in {"PENDING", "MATURABLE"}):
                return None, "NONMONOTONIC_STATUS_TRANSITION"
        except (TypeError, ValueError, KeyError):
            return None, "MALFORMED_STATUS_ARTIFACT"
    # Status validation and existing-history checks are complete before the
    # availability timestamp is sampled and the immutable record is written.
    observed_at_ns = production_clock_ns()
    status_record = OutcomeMaturityStatusV1(
        decision_ref, status, horizon_end_ns, _safe_reason_code(reason_code), observed_at_ns
    )
    repository.register_artifact(ArtifactIndexEntryV2(
        status_record.content_hash, STATUS_ARTIFACT_TYPE, status_record.content_hash,
        observed_at_ns, observed_at_ns, {"status": status_record.to_dict()},
    ))
    return status_record.content_hash, None


def run_outcome_maturity_cycle(
    repository: OpsRepository,
    evidence_cutoff_ns: int | None = None,
    *,
    now_ns: int | None = None,
    production_clock_ns: Callable[[], int] | None = None,
    monotonic_ns: Callable[[], int] | None = None,
    maintenance_budget_ns: int = OUTCOME_MAINTENANCE_BUDGET_NS_V1,
    use_due_work: bool = True,
    action_producer: Callable[..., Any] | None = None,
) -> OutcomeMaturityCycleReportV1:
    """Process one bounded, causally fixed and restart-safe calendar page.

    ``evidence_cutoff_ns`` is immutable for the entire attempt.  Production
    artifacts use ``production_clock_ns`` and elapsed-work limits use only
    ``monotonic_ns``.  The elapsed-time allowance is cooperative: a currently
    running SQLite operation or resolver cannot be preempted.
    """
    if evidence_cutoff_ns is None:
        if now_ns is None:
            raise TypeError("evidence_cutoff_ns is required")
        evidence_cutoff_ns = now_ns
    elif now_ns is not None and now_ns != evidence_cutoff_ns:
        raise ValueError("now_ns and evidence_cutoff_ns must agree")
    timestamp(evidence_cutoff_ns, field="outcome maturity evidence cutoff")
    if (type(maintenance_budget_ns) is not int or not 1 <= maintenance_budget_ns
            <= MAX_OUTCOME_MAINTENANCE_BUDGET_NS):
        raise ValueError("maintenance_budget_ns must be between 1 ns and 1 s")

    utc_clock = production_clock_ns or time.time_ns
    mono_clock = monotonic_ns or time.monotonic_ns
    last_utc_ns = evidence_cutoff_ns

    def production_now_ns() -> int:
        nonlocal last_utc_ns
        observed = timestamp(utc_clock(), field="outcome maturity production time")
        last_utc_ns = max(last_utc_ns, observed)
        return last_utc_ns

    monotonic_started_ns = mono_clock()
    if type(monotonic_started_ns) is not int or monotonic_started_ns < 0:
        raise ValueError("monotonic_ns must return a nonnegative integer")
    monotonic_deadline_ns = monotonic_started_ns + maintenance_budget_ns
    computation_started_ns = production_now_ns()
    budget_status = "WITHIN_BUDGET"
    budget_overrun_ns = 0
    invalid_raw_keys: list[tuple[int, str]] = []
    due_discovery: Mapping[str, Any] | None = None

    def _budget_expired() -> bool:
        observed = mono_clock()
        if type(observed) is not int or observed < monotonic_started_ns:
            raise ValueError("monotonic_ns must return a nondecreasing integer")
        return observed >= monotonic_deadline_ns

    def _operation_finished(operation_started_ns: int) -> bool:
        nonlocal budget_status, budget_overrun_ns
        observed = mono_clock()
        if type(observed) is not int or observed < monotonic_started_ns:
            raise ValueError("monotonic_ns must return a nondecreasing integer")
        if budget_status == "MAINTENANCE_DEADLINE_OVERRUN":
            budget_overrun_ns = max(budget_overrun_ns, max(1, observed - monotonic_deadline_ns))
            return True
        if budget_status == "MAINTENANCE_BUDGET_EXHAUSTED":
            return True
        if observed < monotonic_deadline_ns:
            return False
        if operation_started_ns < monotonic_deadline_ns:
            budget_status = "MAINTENANCE_DEADLINE_OVERRUN"
            budget_overrun_ns = max(1, observed - monotonic_deadline_ns)
        else:
            budget_status = "MAINTENANCE_BUDGET_EXHAUSTED"
        return True

    def _report(**values: Any) -> OutcomeMaturityCycleReportV1:
        nonlocal budget_status, budget_overrun_ns
        elapsed_at_return = mono_clock()
        if type(elapsed_at_return) is not int or elapsed_at_return < monotonic_started_ns:
            raise ValueError("monotonic_ns must return a nondecreasing integer")
        if elapsed_at_return >= monotonic_deadline_ns and budget_status == "WITHIN_BUDGET":
            budget_status = "MAINTENANCE_DEADLINE_OVERRUN"
            budget_overrun_ns = max(1, elapsed_at_return - monotonic_deadline_ns)
        finished_at_ns = production_now_ns()
        failure_code = values.pop("failure_code", None)
        failure_type = values.pop("failure_type", None)
        if failure_code is None:
            if budget_status != "WITHIN_BUDGET":
                failure_code = budget_status
            elif invalid_raw_keys:
                failure_code = "MALFORMED_CALENDAR_INDEX_ROWS"
        return OutcomeMaturityCycleReportV1(
            cycle_at_ns=finished_at_ns,
            evidence_cutoff_ns=evidence_cutoff_ns,
            computation_started_ns=computation_started_ns,
            computation_finished_ns=finished_at_ns,
            maintenance_budget_version=OUTCOME_MAINTENANCE_BUDGET_VERSION,
            maintenance_budget_ns=maintenance_budget_ns,
            maintenance_budget_status=budget_status,
            maintenance_budget_overrun_ns=budget_overrun_ns,
            invalid_calendar_raw_keys=tuple(invalid_raw_keys),
            failure_code=failure_code,
            failure_type=failure_type,
            due_work=repository.due_work_pressure("ACTION_OUTCOME", as_of_ns=evidence_cutoff_ns,
                page_limit=DECISION_PAGE_SIZE) if use_due_work and not repository.read_only else None,
            due_work_discovery=due_discovery,
            **values,
        )

    if repository.read_only:
        return _report(failure_code="WRITER_REPOSITORY_REQUIRED")
    resolver = resolve_decision_outcome
    if resolver is None:
        return _report(failure_code="RESOLVER_UNAVAILABLE")

    checkpoint_read_started = mono_clock()
    try:
        checkpoint, checkpoint_error, checkpoint_rows = _read_checkpoint(
            repository, production_now_ns(),
        )
    except Exception as error:
        return _report(failure_code="CHECKPOINT_READ_FAILURE", failure_type=_safe_exception_type(error))
    checkpoint_read_overrun = _operation_finished(checkpoint_read_started)
    if checkpoint_error is not None:
        return _report(failure_code=checkpoint_error, raw_evidence_rows_inspected=checkpoint_rows,
                       artifact_pages_read=1)
    if checkpoint is not None and evidence_cutoff_ns <= checkpoint.written_at_ns:
        return _report(
            failure_code="CHECKPOINT_CLOCK_NOT_ADVANCED", checkpoint_ref=checkpoint.content_hash,
            raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1,
        )
    if checkpoint_read_overrun:
        return _report(raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1)

    if _budget_expired():
        budget_status = "MAINTENANCE_BUDGET_EXHAUSTED"
        return _report(raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1)

    cursor = checkpoint.cursor if checkpoint is not None else None
    generation = checkpoint.generation if checkpoint is not None else 0
    page_read_started = mono_clock()
    try:
        if use_due_work:
            due_discovery = repository.discover_due_work()
            page = repository.due_artifact_page("ACTION_OUTCOME", as_of_ns=evidence_cutoff_ns,
                limit=DECISION_PAGE_SIZE)
        else:
            page = repository.artifact_entries_by_types_page(
                (DECISION_CALENDAR_ARTIFACT_TYPE,), as_of_ns=evidence_cutoff_ns,
                after=cursor, limit=DECISION_PAGE_SIZE,
            )
    except Exception as error:
        return _report(failure_code="CALENDAR_PAGE_READ_FAILURE", failure_type=_safe_exception_type(error),
                       raw_evidence_rows_inspected=checkpoint_rows, artifact_pages_read=1)
    page_read_overrun = _operation_finished(page_read_started)
    raw_keys = page.raw_keys
    if not raw_keys and page.entries:
        raw_keys = tuple((entry.created_at_ns, entry.artifact_ref) for entry in page.entries)
    base_rows = checkpoint_rows + len(raw_keys)
    base_pages = 2

    if not raw_keys:
        if use_due_work:
            return _report(checkpoint_ref=None if checkpoint is None else checkpoint.content_hash,
                artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)
        if page_read_overrun:
            return _report(
                checkpoint_ref=None if checkpoint is None else checkpoint.content_hash,
                artifact_pages_read=base_pages,
                raw_evidence_rows_inspected=base_rows,
            )
        if checkpoint is None and cursor is None:
            return _report(artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)
        try:
            checkpoint_ref: str | None = _write_checkpoint(
                repository, generation=generation + 1, cursor=None, now_ns=production_now_ns(),
            )
        except Exception as error:
            return _report(failure_code="CHECKPOINT_WRITE_FAILURE", failure_type=_safe_exception_type(error),
                           artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)
        return _report(wrapped=True, checkpoint_ref=checkpoint_ref, artifact_pages_read=base_pages,
                       raw_evidence_rows_inspected=base_rows)

    if (page.next_cursor is None or page.next_cursor != raw_keys[-1]
            or (not use_due_work and cursor is not None and page.next_cursor >= cursor)):
        return _report(failure_code="NON_MONOTONIC_CALENDAR_CURSOR",
                       decisions_inspected=len(page.entries), invalid_calendar_entries=page.invalid_entry_count,
                       artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)
    entry_by_key = {(entry.created_at_ns, entry.artifact_ref): entry for entry in page.entries}
    raw_key_set = set(raw_keys)
    if (len(entry_by_key) != len(page.entries)
            or any(key not in raw_key_set for key in entry_by_key)
            or len(raw_keys) - len(entry_by_key) != page.invalid_entry_count):
        return _report(failure_code="MALFORMED_RAW_KEY_PAGE",
                       decisions_inspected=len(page.entries), invalid_calendar_entries=page.invalid_entry_count,
                       artifact_pages_read=base_pages, raw_evidence_rows_inspected=base_rows)

    totals = _CycleReadBudget(pages=base_pages, rows=base_rows)
    pending = maturable = matured = unresolved = censored = unsupported = 0
    outcomes_attempted = outcomes_indexed = conflicting = status_written = existing_outcomes_checked = 0
    invalid_calendar = decisions_inspected = 0
    oldest_pending_age: int | None = None
    oldest_maturable_age: int | None = None
    cycle_failure: str | None = None
    cycle_failure_type: str | None = None
    attempt_bound_deferred = False
    last_accounted_cursor = cursor
    processed_raw_keys = 0

    def record_status(
        budgeted: _BudgetedRepository,
        *,
        decision_ref: str,
        status: str,
        horizon: int | None,
        reason: str | None,
    ) -> None:
        nonlocal status_written, cycle_failure, cycle_failure_type
        try:
            status_ref, status_error = _record_status(
                budgeted, decision_ref=decision_ref, status=status,
                horizon_end_ns=horizon, reason_code=reason,
                production_clock_ns=production_now_ns,
            )
            status_written += status_ref is not None
            if status_error is not None and cycle_failure is None:
                cycle_failure = status_error
            if use_due_work and status_error in {"MALFORMED_STATUS_INDEX", "AMBIGUOUS_STATUS_ORDER",
                    "MALFORMED_STATUS_ARTIFACT", "TERMINAL_STATUS_REGRESSION", "NONMONOTONIC_STATUS_TRANSITION"}:
                repository.quarantine_due_work("ACTION_OUTCOME", decision_ref, reason_code=status_error)
            if use_due_work and status_error is None:
                if reason in {"MALFORMED_CALENDAR_ENTRY", "CONFLICTING_OUTCOME",
                        "MALFORMED_OUTCOME", "NONTERMINAL_OUTCOME_EXISTS"}:
                    repository.quarantine_due_work("ACTION_OUTCOME", decision_ref, reason_code=reason)
                elif status in {"MATURED", "CENSORED", "UNSUPPORTED"}:
                    repository.retire_due_work("ACTION_OUTCOME", decision_ref, reason_code=status)
                else:
                    due_at_ns = max(evidence_cutoff_ns + OUTCOME_DUE_RETRY_INTERVAL_NS_V1,
                        horizon or evidence_cutoff_ns) if status == "PENDING" else (
                            evidence_cutoff_ns + OUTCOME_DUE_RETRY_INTERVAL_NS_V1)
                    repository.reschedule_due_work("ACTION_OUTCOME", decision_ref,
                        due_at_ns=due_at_ns, reason_code=reason or status)
        except Exception as error:
            if cycle_failure is None:
                cycle_failure = "STATUS_WRITE_FAILURE"
                cycle_failure_type = _safe_exception_type(error)

    def process_calendar_entry(indexed_calendar: ArtifactIndexEntryV2) -> bool:
        """Process one decoded raw entry; False leaves this exact key unadvanced."""
        nonlocal pending, maturable, matured, unresolved, censored, unsupported
        nonlocal outcomes_attempted, outcomes_indexed, conflicting, existing_outcomes_checked
        nonlocal invalid_calendar, oldest_pending_age, oldest_maturable_age
        nonlocal cycle_failure, cycle_failure_type, attempt_bound_deferred

        try:
            decision = _validate_calendar_entry(indexed_calendar, evidence_cutoff_ns)
        except (KeyError, TypeError, ValueError):
            invalid_calendar += 1
            invalid_raw_keys.append((indexed_calendar.created_at_ns, indexed_calendar.artifact_ref))
            budgeted = _BudgetedRepository(repository, totals)
            record_status(
                budgeted, decision_ref=indexed_calendar.artifact_ref, status="UNRESOLVED",
                horizon=None, reason="MALFORMED_CALENDAR_ENTRY",
            )
            return True

        budgeted = _BudgetedRepository(
            repository, totals,
            resolver_read_allowed=lambda: mono_clock() < monotonic_deadline_ns,
        )

        def record(status: str, horizon: int | None, reason: str | None) -> None:
            if status in {"MATURED", "CENSORED"}:
                reason = "TERMINAL_OUTCOME_SUPPORTED"
            record_status(
                budgeted, decision_ref=indexed_calendar.artifact_ref, status=status,
                horizon=horizon, reason=reason,
            )

        if outcomes_attempted >= MAX_OUTCOMES_ATTEMPTED:
            attempt_bound_deferred = True
            unresolved += 1
            record("UNRESOLVED", None, "CYCLE_OUTCOME_BUDGET")
            return True
        outcomes_attempted += 1
        outcome_read_started = mono_clock()
        try:
            outcome_page = budgeted.artifact_entries_by_metadata_identity(
                OUTCOME_ARTIFACT_TYPE, ("outcome", "decision_ref"), indexed_calendar.artifact_ref,
                as_of_ns=evidence_cutoff_ns, limit=1,
            )
            existing_outcomes_checked += 1
            outcome_read_overrun = _operation_finished(outcome_read_started)
            if outcome_page.invalid_entry_count or outcome_page.has_more:
                conflicting += 1
                unresolved += 1
                record("UNRESOLVED", None, "CONFLICTING_OUTCOME")
                return True

            if outcome_page.entries:
                existing_entry = outcome_page.entries[0]
                body = existing_entry.metadata.get("outcome")
                if (existing_entry.artifact_type != OUTCOME_ARTIFACT_TYPE or not isinstance(body, Mapping)
                        or existing_entry.content_hash != existing_entry.artifact_ref):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", None, "MALFORMED_OUTCOME")
                    return True
                existing_outcome = MaturedOutcomeV2.from_dict(json_value(body))
                existing_ref = existing_entry.artifact_ref
                if (existing_outcome.content_hash != existing_ref
                        or existing_outcome.decision_ref != indexed_calendar.artifact_ref
                        or existing_entry.created_at_ns != existing_outcome.matured_at_ns
                        or existing_entry.available_at_ns != existing_outcome.available_at_ns):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", existing_outcome.horizon_end_ns, "CONFLICTING_OUTCOME")
                    return True
                if existing_outcome.label_state not in (LabelStateV2.MATURED, LabelStateV2.CENSORED):
                    conflicting += 1
                    unresolved += 1
                    record("UNRESOLVED", existing_outcome.horizon_end_ns, "NONTERMINAL_OUTCOME_EXISTS")
                    return True
                if index_matured_outcome(cast(OpsRepository, budgeted), existing_outcome) != existing_ref:
                    raise ValueError("stored outcome identity changed")
                resolved_status = "MATURED" if existing_outcome.label_state == LabelStateV2.MATURED else "CENSORED"
                matured += resolved_status == "MATURED"
                censored += resolved_status == "CENSORED"
                record(resolved_status, existing_outcome.horizon_end_ns, "EXISTING_VALIDATED_OUTCOME")
                return True

            # A slow lookup may return after the deadline. Do not start a new
            # resolver unit in that case; leave this key reachable for retry.
            if outcome_read_overrun or _budget_expired():
                return False

            budgeted.begin_resolver()
            try:
                if use_due_work and decision.action_artifact_ref is not None:
                    from .action_outcome_producer import RetrospectiveActionOutcomeProducerV1
                    producer = action_producer or RetrospectiveActionOutcomeProducerV1(clock_ns=production_now_ns)
                    production = producer(cast(OpsRepository, budgeted), indexed_calendar,
                        evidence_cutoff_ns, clock_ns=production_now_ns)
                    # Newly published payoff becomes readable only in a subsequent
                    # fixed-cutoff cycle; later computation never extends this one.
                    if production.payoff_ref is not None and (payoff_entry := budgeted.get_artifact(production.payoff_ref)) is not None and payoff_entry.available_at_ns > evidence_cutoff_ns:
                        unresolved += 1
                        record("UNRESOLVED", None, "ACTION_PAYOFF_AWAITING_NEXT_CUTOFF")
                        return True
                resolution = resolver(
                    cast(OpsRepository, budgeted), indexed_calendar, evidence_cutoff_ns,
                    clock_ns=production_now_ns,
                )
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
                if status_value in {"UNRESOLVED", "MATURABLE"} and horizon is not None and horizon <= evidence_cutoff_ns:
                    maturable += 1
                    age = max(0, evidence_cutoff_ns - decision.decision_at_ns)
                    oldest_maturable_age = age if oldest_maturable_age is None else max(oldest_maturable_age, age)
                if status_value == "MATURED":
                    if not isinstance(outcome, MaturedOutcomeV2):
                        raise ValueError("matured resolver state lacks a typed outcome")
                    if (outcome.decision_ref != indexed_calendar.artifact_ref
                            or outcome.label_state != LabelStateV2.MATURED):
                        raise ValueError("resolved outcome decision or terminal state mismatch")
                    if (outcome.horizon_end_ns != horizon or outcome.horizon_end_ns > evidence_cutoff_ns
                            or outcome.available_at_ns < max(computation_started_ns, evidence_cutoff_ns)):
                        raise ValueError("resolved outcome is not available after validation")
                    if index_matured_outcome(cast(OpsRepository, budgeted), outcome) != outcome.content_hash:
                        raise ValueError("outcome index returned a different identity")
                    outcomes_indexed += 1
                    matured += 1
                elif status_value == "CENSORED":
                    if (not isinstance(outcome, MaturedOutcomeV2)
                            or outcome.label_state != LabelStateV2.CENSORED
                            or outcome.decision_ref != indexed_calendar.artifact_ref
                            or outcome.horizon_end_ns != horizon
                            or outcome.horizon_end_ns > evidence_cutoff_ns
                            or outcome.available_at_ns < max(computation_started_ns, evidence_cutoff_ns)):
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
                    if outcome is not None or horizon is None or horizon <= evidence_cutoff_ns:
                        raise ValueError("pending state requires a future exact horizon and no label")
                    pending += 1
                    age = max(0, evidence_cutoff_ns - decision.decision_at_ns)
                    oldest_pending_age = age if oldest_pending_age is None else max(oldest_pending_age, age)
                elif status_value == "MATURABLE":
                    if outcome is not None or horizon is None or horizon > evidence_cutoff_ns:
                        raise ValueError("maturable state requires an elapsed exact horizon and no label")
            finally:
                budgeted.end_resolver()
                resolver_finished_mono = mono_clock()
                if resolver_finished_mono >= monotonic_deadline_ns:
                    nonlocal_budget_overrun = max(1, resolver_finished_mono - monotonic_deadline_ns)
                    # Report the non-preemptible operation honestly, then finish
                    # this decision and stop before beginning another one.
                    budget_state[0] = "MAINTENANCE_DEADLINE_OVERRUN"
                    budget_overrun_state[0] = nonlocal_budget_overrun
            record(status_value, horizon, reason)
        except _MaintenanceBudgetExpired:
            unresolved += 1
            if cycle_failure is None:
                cycle_failure = "MAINTENANCE_DEADLINE_OVERRUN"
            record("UNRESOLVED", None, "MAINTENANCE_DEADLINE_OVERRUN")
        except Exception as error:
            unresolved += 1
            if cycle_failure is None:
                cycle_failure = "RESOLUTION_OR_INDEX_FAILURE"
                cycle_failure_type = _safe_exception_type(error)
            record("UNRESOLVED", None, "RESOLUTION_OR_INDEX_FAILURE")
        return True

    # Mutable holders let the nested resolver-completion block update the
    # call's budget state without widening the per-decision closure surface.
    budget_state = [budget_status]
    budget_overrun_state = [budget_overrun_ns]
    last_accounted_cursor = cursor
    processed_raw_keys = 0
    page_read_overrun_pending = page_read_overrun
    for raw_key in raw_keys:
        if page_read_overrun_pending:
            budget_status = "MAINTENANCE_DEADLINE_OVERRUN"
            budget_overrun_ns = max(1, mono_clock() - monotonic_deadline_ns)
            break
        if _budget_expired():
            budget_status = "MAINTENANCE_BUDGET_EXHAUSTED"
            break
        indexed_calendar = entry_by_key.get(raw_key)
        unit_started = mono_clock()
        if indexed_calendar is None:
            invalid_calendar += 1
            invalid_raw_keys.append(raw_key)
            if cycle_failure is None:
                cycle_failure = "MALFORMED_CALENDAR_INDEX_ROWS"
            completed = True
            if use_due_work:
                repository.quarantine_due_work("ACTION_OUTCOME", raw_key[1],
                    reason_code="MALFORMED_CALENDAR_INDEX_ROWS")
        else:
            decisions_inspected += 1
            completed = process_calendar_entry(indexed_calendar)
        if not completed:
            if _operation_finished(unit_started):
                pass
            elif _budget_expired():
                budget_status = "MAINTENANCE_BUDGET_EXHAUSTED"
            break
        last_accounted_cursor = raw_key
        processed_raw_keys += 1
        if budget_state[0] != "WITHIN_BUDGET":
            budget_status = budget_state[0]
            budget_overrun_ns = budget_overrun_state[0]
            break
        if _operation_finished(unit_started):
            break

    cursor_after_cycle = page.next_cursor if processed_raw_keys == len(raw_keys) else last_accounted_cursor
    checkpoint_ref = checkpoint.content_hash if checkpoint is not None else None
    if cursor_after_cycle != cursor or (use_due_work and processed_raw_keys):
        try:
            checkpoint_started = mono_clock()
            checkpoint_ref = _write_checkpoint(
                repository, generation=generation + 1, cursor=cursor_after_cycle,
                now_ns=production_now_ns(), accounted_invalid_raw_keys=tuple(invalid_raw_keys),
            )
            if _operation_finished(checkpoint_started):
                pass
        except Exception as error:
            if cycle_failure is None:
                cycle_failure = "CHECKPOINT_WRITE_FAILURE"
                cycle_failure_type = _safe_exception_type(error)

    if budget_status == "WITHIN_BUDGET" and budget_state[0] != "WITHIN_BUDGET":
        budget_status = budget_state[0]
        budget_overrun_ns = budget_overrun_state[0]
    if budget_status != "WITHIN_BUDGET" and cycle_failure is None:
        cycle_failure = budget_status
    return _report(
        decisions_inspected=decisions_inspected,
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
        retained_work_items=min(MAX_RETAINED_IN_MEMORY_WORK_ITEMS, processed_raw_keys + 2),
        bounded_work_exhausted=(len(raw_keys) >= DECISION_PAGE_SIZE or invalid_calendar > 0
                                or attempt_bound_deferred or budget_status != "WITHIN_BUDGET"),
        wrapped=False,
        oldest_pending_age_ns=oldest_pending_age,
        oldest_maturable_age_ns=oldest_maturable_age,
        checkpoint_ref=checkpoint_ref,
        failure_code=cycle_failure,
        failure_type=cycle_failure_type,
    )
