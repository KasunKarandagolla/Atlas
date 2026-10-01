"""Immutable identity and replay rules for native S3 one-minute events.

This module contains only the event-origin rules. Production owns persistence,
source health, and supervisor invocation; this module never writes repository
state or changes an existing event's cutoff/deadline.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from .._serialization import canonical_json, sha256_json, sha256_ref, strict_fields, timestamp
from ..data.bars import BarIntervalV2, CausalBarV2
from ..data.raw import AvailabilityClassV2
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2

S3_M1_EVENT_TYPE = "CONFIRMED_1M_CLOSE"
S3_M1_EVENT_ID_VERSION = "OPS_DECISION_EVENT_FROM_NATIVE_M1_ORIGIN_V1"
S3_M1_ORIGIN_VERSION = "S3_NATIVE_M1_ORIGIN_V1"
S3_M1_DEFAULT_MAX_LATENESS_NS = 5_000_000_000
S3_M1_ACCOUNTING_CHECKPOINT_TYPE = "S3NativeM1OriginAccountingCheckpointV1"
MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE = 4
_LATE_ORIGIN_REASON = "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE"
_MISSED_ORIGIN_REASON = "NATIVE_M1_ORIGIN_EVENT_PREREQUISITE_UNAVAILABLE"
_NANOSECONDS_PER_MINUTE = 60_000_000_000


class S3M1BarDisposition(StrEnum):
    """Whether a finalized minute bar may create its first immutable event."""

    TIMELY = "TIMELY"
    WRONG_INTERVAL = "WRONG_INTERVAL"
    NOT_FINAL = "NOT_FINAL"
    NON_ACTUAL_AVAILABILITY = "NON_ACTUAL_AVAILABILITY"
    FUTURE_AVAILABLE = "FUTURE_AVAILABLE"
    LATE = "LATE"


class S3M1ReplayDisposition(StrEnum):
    """Disposition of one already-persisted native M1 event on a restart."""

    REUSE = "REUSE"
    NOT_YET_AVAILABLE = "NOT_YET_AVAILABLE"
    EXPIRED = "EXPIRED"
    ALREADY_PROCESSED = "ALREADY_PROCESSED"


class S3M1OriginAccountingAction(StrEnum):
    """Durable accounting work needed for one exact discovered M1 close."""

    CREATE_TIMELY_EVENT = "CREATE_TIMELY_EVENT"
    CREATE_LATE_TEST_GATE = "CREATE_LATE_TEST_GATE"
    REUSE_TIMELY_EVENT = "REUSE_TIMELY_EVENT"
    REUSE_LATE_TEST_GATE = "REUSE_LATE_TEST_GATE"


@dataclass(frozen=True)
class S3M1OriginAccountingPlan:
    """One oldest-first origin disposition; persistence remains controller-owned."""

    instrument_key: InstrumentKeyV2
    close_at_ns: int
    origin_ref: str
    bar_ref: str
    deadline_ns: int
    action: S3M1OriginAccountingAction
    existing_accounting_ref: str | None = None

    def __post_init__(self) -> None:
        _validate_origin(self.instrument_key, self.close_at_ns)
        sha256_ref(self.origin_ref, field="origin_ref")
        sha256_ref(self.bar_ref, field="bar_ref")
        timestamp(self.deadline_ns, field="deadline_ns")
        if self.origin_ref != s3_m1_origin_ref(self.instrument_key, self.close_at_ns):
            raise ValueError("origin accounting plan has a conflicting exact identity")
        if self.deadline_ns != self.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS:
            raise ValueError("origin accounting plan changed the frozen five-second deadline")
        if self.existing_accounting_ref is not None:
            sha256_ref(self.existing_accounting_ref, field="existing_accounting_ref")


@dataclass(frozen=True)
class S3NativeM1OriginAccountingCheckpointV1:
    """Immutable, revision-bound progress through one source-availability window.

    The close cursor advances only after every source origin before it has an
    immutable event or explicit late gate. A completed window advances the
    availability watermark; the next window starts with no close cursor, so a
    late-arriving older close is still discovered without replaying prior
    source history.
    """

    instrument_key: InstrumentKeyV2
    generation: int
    previous_checkpoint_ref: str | None
    source_available_from_ns: int
    source_available_through_ns: int
    source_scan_after_close_at_ns: int | None
    last_accounted_close_at_ns: int | None
    scan_complete: bool
    created_at_ns: int
    available_at_ns: int
    authority: str = "ZERO"

    VERSION = S3_M1_ACCOUNTING_CHECKPOINT_TYPE

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_key, InstrumentKeyV2):
            raise ValueError("origin checkpoint requires the full InstrumentKeyV2")
        if type(self.generation) is not int or self.generation <= 0:
            raise ValueError("origin checkpoint generation must be positive")
        if (self.generation == 1) != (self.previous_checkpoint_ref is None):
            raise ValueError("origin checkpoint predecessor does not match its generation")
        if self.previous_checkpoint_ref is not None:
            sha256_ref(self.previous_checkpoint_ref, field="previous_checkpoint_ref")
        timestamp(self.source_available_from_ns, field="source_available_from_ns")
        timestamp(self.source_available_through_ns, field="source_available_through_ns")
        if self.source_available_from_ns > self.source_available_through_ns:
            raise ValueError("origin checkpoint source window is reversed")
        if self.source_scan_after_close_at_ns is not None:
            _validate_origin(self.instrument_key, self.source_scan_after_close_at_ns)
        if self.last_accounted_close_at_ns is not None:
            _validate_origin(self.instrument_key, self.last_accounted_close_at_ns)
        if type(self.scan_complete) is not bool:
            raise ValueError("origin checkpoint scan_complete must be bool")
        if self.scan_complete and self.source_scan_after_close_at_ns is not None:
            raise ValueError("completed origin checkpoint cannot retain an active close cursor")
        if not self.scan_complete and self.source_scan_after_close_at_ns is None:
            raise ValueError("unfinished origin checkpoint must retain its close cursor")
        timestamp(self.created_at_ns, field="created_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("origin checkpoint availability precedes its creation")
        if self.source_available_through_ns > self.created_at_ns:
            raise ValueError("origin checkpoint claims a source watermark from its future")
        if self.authority != "ZERO":
            raise ValueError("native M1 origin checkpoints have zero authority")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "instrument_key": self.instrument_key.to_dict(),
            "generation": self.generation,
            "previous_checkpoint_ref": self.previous_checkpoint_ref,
            "source_available_from_ns": self.source_available_from_ns,
            "source_available_through_ns": self.source_available_through_ns,
            "source_scan_after_close_at_ns": self.source_scan_after_close_at_ns,
            "last_accounted_close_at_ns": self.last_accounted_close_at_ns,
            "scan_complete": self.scan_complete,
            "created_at_ns": self.created_at_ns,
            "available_at_ns": self.available_at_ns,
            "authority": self.authority,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> S3NativeM1OriginAccountingCheckpointV1:
        fields = {
            "version", "instrument_key", "generation", "previous_checkpoint_ref",
            "source_available_from_ns", "source_available_through_ns",
            "source_scan_after_close_at_ns", "last_accounted_close_at_ns", "scan_complete",
            "created_at_ns", "available_at_ns", "authority",
        }
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported native M1 origin checkpoint")
        row["instrument_key"] = InstrumentKeyV2.from_dict(row["instrument_key"])
        return cls(**row)


def advance_s3_m1_origin_checkpoint(
    previous: S3NativeM1OriginAccountingCheckpointV1 | None,
    key: InstrumentKeyV2,
    *,
    now_ns: int,
    source_available_through_ns: int,
    next_close_cursor_ns: int | None,
    accounted_close_at_ns: int | None,
    has_more: bool,
) -> S3NativeM1OriginAccountingCheckpointV1:
    """Build the next append-only checkpoint after a bounded page is durable."""
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("checkpoint advancement requires the full InstrumentKeyV2")
    timestamp(now_ns, field="now_ns")
    source_through = timestamp(source_available_through_ns, field="source_available_through_ns")
    if source_through > now_ns:
        raise ValueError("checkpoint source watermark cannot be ahead of controller time")
    if type(has_more) is not bool:
        raise ValueError("has_more must be bool")
    if next_close_cursor_ns is not None:
        _validate_origin(key, next_close_cursor_ns)
    if accounted_close_at_ns is not None:
        _validate_origin(key, accounted_close_at_ns)

    if previous is None:
        generation = 1
        predecessor = None
        source_from = 0
        window_through = source_through
        prior_cursor = None
        prior_accounted = None
    else:
        if previous.instrument_key != key:
            raise ValueError("native M1 origin checkpoint cannot cross instrument revisions")
        if now_ns < previous.available_at_ns:
            raise ValueError("native M1 origin checkpoint clock moved backward")
        generation = previous.generation + 1
        predecessor = previous.content_hash
        if previous.scan_complete:
            source_from = previous.source_available_through_ns
            if source_through <= source_from:
                raise ValueError("completed origin checkpoint requires an advancing source watermark")
            window_through = source_through
            prior_cursor = None
        else:
            source_from = previous.source_available_from_ns
            window_through = previous.source_available_through_ns
            if source_through != window_through:
                raise ValueError("active origin scan window cannot be rebased")
            prior_cursor = previous.source_scan_after_close_at_ns
        prior_accounted = previous.last_accounted_close_at_ns

    if has_more:
        if next_close_cursor_ns is None:
            raise ValueError("continuing origin scan requires a close cursor")
        if prior_cursor is not None and next_close_cursor_ns <= prior_cursor:
            raise ValueError("origin checkpoint close cursor did not advance")
        scan_complete = False
        cursor = next_close_cursor_ns
    else:
        scan_complete = True
        cursor = None

    accounted = prior_accounted
    if accounted_close_at_ns is not None:
        accounted = max(accounted or accounted_close_at_ns, accounted_close_at_ns)
    if prior_accounted is not None and (accounted is None or accounted < prior_accounted):
        raise ValueError("origin checkpoint accounted close regressed")
    if prior_cursor is not None and next_close_cursor_ns is not None and has_more:
        if next_close_cursor_ns <= prior_cursor:
            raise ValueError("origin checkpoint skipped or repeated close work")

    return S3NativeM1OriginAccountingCheckpointV1(
        key, generation, predecessor, source_from, window_through, cursor, accounted,
        scan_complete, now_ns, now_ns,
    )


class _DecisionEvent(Protocol):
    event_id: str
    event_type: str
    available_at_ns: int
    deadline_ns: int


def s3_m1_origin_ref(key: InstrumentKeyV2, close_at_ns: int) -> str:
    """Return the stable logical identity for one exact instrument and M1 close."""
    _validate_origin(key, close_at_ns)
    return sha256_json({
        "version": S3_M1_ORIGIN_VERSION,
        "instrument_key": key.to_dict(),
        "close_at_ns": close_at_ns,
    })


def s3_m1_event_id(key: InstrumentKeyV2, close_at_ns: int) -> str:
    """Return an event ID independent of raw-bar payload and recovery timing."""
    origin_ref = s3_m1_origin_ref(key, close_at_ns)
    return sha256_json({"version": S3_M1_EVENT_ID_VERSION, "origin_ref": origin_ref})


def s3_m1_origin_metadata(
    key: InstrumentKeyV2,
    close_at_ns: int,
    *,
    bar_ref: str,
) -> dict[str, object]:
    """Build durable source-entry metadata binding the first exact bar to its origin."""
    sha256_ref(bar_ref, field="bar_ref")
    return {
        "native_m1_origin": {
            "version": S3_M1_ORIGIN_VERSION,
            "origin_ref": s3_m1_origin_ref(key, close_at_ns),
            "instrument_key": key.to_dict(),
            "close_at_ns": close_at_ns,
            "bar_ref": bar_ref,
        }
    }


def find_s3_m1_origin_event(
    entries: Iterable[ArtifactIndexEntryV2],
    key: InstrumentKeyV2,
    close_at_ns: int,
) -> ArtifactIndexEntryV2 | None:
    """Find the one durable event for an origin, failing closed on conflicts.

    Entries from older M15 event sources have no ``native_m1_origin`` metadata
    and are ignored. A matching native origin must carry a verified immutable
    event body and the exact first bar ref in its causal inputs.
    """
    origin_ref = s3_m1_origin_ref(key, close_at_ns)
    event_id = s3_m1_event_id(key, close_at_ns)
    key_wire = key.to_dict()
    matching: dict[str, ArtifactIndexEntryV2] = {}

    for entry in entries:
        if entry.artifact_type != "OpsDecisionEventSourceV1":
            continue
        metadata = entry.metadata
        origin = metadata.get("native_m1_origin")
        event_body = metadata.get("event")

        # A stable event ID with missing origin metadata is corruption, not a
        # signal to create a replacement event.
        if not isinstance(origin, Mapping):
            if isinstance(event_body, Mapping) and event_body.get("event_id") == event_id:
                raise ValueError("native M1 event is missing its durable origin metadata")
            continue

        origin_ref_match = origin.get("origin_ref") == origin_ref
        try:
            origin_key_match = canonical_json(origin.get("instrument_key")) == canonical_json(key_wire)
        except ValueError:
            origin_key_match = False
        origin_close_match = origin.get("close_at_ns") == close_at_ns
        if not (origin_ref_match or (origin_key_match and origin_close_match)):
            continue

        if (origin.get("version") != S3_M1_ORIGIN_VERSION
                or origin.get("origin_ref") != origin_ref
                or not origin_key_match or not origin_close_match):
            raise ValueError("native M1 origin metadata conflicts with the requested exact origin")
        if not isinstance(event_body, Mapping):
            raise ValueError("native M1 source entry has no immutable event body")
        if (entry.artifact_ref != entry.content_hash
                or entry.content_hash != sha256_json(event_body)):
            raise ValueError("native M1 source entry event hash does not verify")
        if (event_body.get("event_id") != event_id
                or event_body.get("event_type") != S3_M1_EVENT_TYPE):
            raise ValueError("native M1 source entry has a conflicting event identity")
        if entry.available_at_ns != event_body.get("information_cutoff_ns"):
            raise ValueError("native M1 source entry availability conflicts with its frozen cutoff")

        bar_ref = origin.get("bar_ref")
        if not isinstance(bar_ref, str):
            raise ValueError("native M1 origin is missing its exact first bar ref")
        sha256_ref(bar_ref, field="native M1 bar_ref")
        causal_refs = event_body.get("causal_input_refs")
        trigger_ref = event_body.get("trigger_ref")
        if (not isinstance(causal_refs, (list, tuple)) or bar_ref not in causal_refs
                or trigger_ref not in causal_refs):
            raise ValueError("native M1 event does not bind its exact first bar")

        prior = matching.get(entry.artifact_ref)
        if prior is not None and (
            canonical_json(prior.metadata) != canonical_json(entry.metadata)
            or prior.created_at_ns != entry.created_at_ns
            or prior.available_at_ns != entry.available_at_ns
        ):
            raise ValueError("one native M1 artifact ref has conflicting source metadata")
        matching[entry.artifact_ref] = entry

    if len(matching) > 1:
        raise ValueError("one native M1 origin has multiple immutable decision events")
    return next(iter(matching.values()), None)


def find_s3_m1_origin_late_gate(
    entries: Iterable[ArtifactIndexEntryV2],
    key: InstrumentKeyV2,
    close_at_ns: int,
) -> ArtifactIndexEntryV2 | None:
    """Find and validate the one explicit TEST GATE for an exact late origin."""
    origin_ref = s3_m1_origin_ref(key, close_at_ns)
    key_wire = key.to_dict()
    matching: dict[str, ArtifactIndexEntryV2] = {}
    for entry in entries:
        if entry.artifact_type != "OpsPublicAcquisitionDeadlineGateV1":
            continue
        origin = entry.metadata.get("native_m1_origin")
        gate_body = entry.metadata.get("deadline_gate")
        top_origin_ref = entry.metadata.get("native_m1_origin_ref")
        origin_matches = isinstance(origin, Mapping) and (
            origin.get("origin_ref") == origin_ref
            or (origin.get("close_at_ns") == close_at_ns
                and canonical_json(origin.get("instrument_key")) == canonical_json(key_wire))
        )
        if top_origin_ref != origin_ref and not origin_matches:
            continue
        if top_origin_ref != origin_ref:
            raise ValueError("native M1 late gate top-level origin identity conflicts")
        if not isinstance(origin, Mapping) or not isinstance(gate_body, Mapping):
            raise ValueError("native M1 late gate is missing typed origin or gate evidence")
        if (origin.get("version") != S3_M1_ORIGIN_VERSION
                or origin.get("origin_ref") != origin_ref
                or canonical_json(origin.get("instrument_key")) != canonical_json(key_wire)
                or origin.get("close_at_ns") != close_at_ns):
            raise ValueError("native M1 late gate has a conflicting full origin identity")
        bar_ref = origin.get("bar_ref")
        if not isinstance(bar_ref, str):
            raise ValueError("native M1 late gate has no exact source bar ref")
        sha256_ref(bar_ref, field="native M1 gate bar_ref")
        if (entry.artifact_ref != entry.content_hash
                or entry.content_hash != sha256_json(gate_body)):
            raise ValueError("native M1 late gate content hash does not verify")
        if (gate_body.get("version") != "OPS_PUBLIC_ACQUISITION_DEADLINE_GATE_V1"
                or gate_body.get("native_m1_origin_ref") != origin_ref
                or gate_body.get("native_m1_bar_ref") != bar_ref
                or tuple(gate_body.get("event_ids", ())) != (s3_m1_event_id(key, close_at_ns),)
                or tuple(gate_body.get("deadlines_ns", ()))
                != (close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS,)
                or gate_body.get("eligible_cutoff_ns") != close_at_ns
                or gate_body.get("status") != "TEST GATE"
                or gate_body.get("reason_code") not in {_LATE_ORIGIN_REASON, _MISSED_ORIGIN_REASON}
                or gate_body.get("authority") != "ZERO"):
            raise ValueError("native M1 late gate has a conflicting fixed deadline or TEST GATE state")
        observed_at = gate_body.get("observed_at_ns")
        if type(observed_at) is not int:
            raise ValueError("native M1 late gate observation time is malformed")
        timestamp(observed_at, field="late gate observed_at_ns")
        if entry.available_at_ns != observed_at or entry.created_at_ns > observed_at:
            raise ValueError("native M1 late gate availability conflicts with its observation time")
        if (gate_body.get("reason_code") == _LATE_ORIGIN_REASON
                and observed_at <= close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS):
            raise ValueError("native M1 late gate was created before its fixed close deadline")
        if (gate_body.get("reason_code") == _MISSED_ORIGIN_REASON
                and observed_at > close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS):
            raise ValueError("native M1 missed-origin gate rebased a late origin")
        prior = matching.get(entry.artifact_ref)
        if prior is not None and (
            canonical_json(prior.metadata) != canonical_json(entry.metadata)
            or prior.created_at_ns != entry.created_at_ns
            or prior.available_at_ns != entry.available_at_ns
        ):
            raise ValueError("one native M1 late gate ref has conflicting metadata")
        matching[entry.artifact_ref] = entry
    if len(matching) > 1:
        raise ValueError("one native M1 origin has multiple immutable late gates")
    return next(iter(matching.values()), None)


def find_s3_m1_origin_accounting_state(
    event_entries: Iterable[ArtifactIndexEntryV2],
    gate_entries: Iterable[ArtifactIndexEntryV2],
    key: InstrumentKeyV2,
    close_at_ns: int,
) -> tuple[S3M1OriginAccountingAction, ArtifactIndexEntryV2] | None:
    """Resolve exactly one durable timely-event or late-gate state."""
    event = find_s3_m1_origin_event(event_entries, key, close_at_ns)
    gate = find_s3_m1_origin_late_gate(gate_entries, key, close_at_ns)
    if event is not None and gate is not None:
        raise ValueError("native M1 origin has both an immutable event and a contradictory late gate")
    if event is not None:
        return S3M1OriginAccountingAction.REUSE_TIMELY_EVENT, event
    if gate is not None:
        return S3M1OriginAccountingAction.REUSE_LATE_TEST_GATE, gate
    return None


def plan_s3_m1_origin_accounting(
    bars: Iterable[CausalBarV2],
    key: InstrumentKeyV2,
    *,
    now_ns: int,
    event_entries: Iterable[ArtifactIndexEntryV2] = (),
    gate_entries: Iterable[ArtifactIndexEntryV2] = (),
    after_close_at_ns: int | None = None,
    max_origins: int = MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
) -> tuple[S3M1OriginAccountingPlan, ...]:
    """Plan a bounded oldest-first accounting page without persisting state.

    Callers must write each new event or late gate before advancing their
    source-page checkpoint. Existing durable state is returned as REUSE so a
    crash between origin persistence and checkpoint persistence is idempotent.
    """
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("native M1 origin planning requires a full InstrumentKeyV2")
    timestamp(now_ns, field="now_ns")
    if after_close_at_ns is not None:
        _validate_origin(key, after_close_at_ns)
    if type(max_origins) is not int or not 1 <= max_origins <= MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE:
        raise ValueError("origin accounting page must be between 1 and the fixed per-cycle bound")

    event_rows = tuple(event_entries)
    gate_rows = tuple(gate_entries)
    by_close: dict[int, CausalBarV2] = {}
    for bar in bars:
        if not isinstance(bar, CausalBarV2):
            raise ValueError("native M1 origin page contains a non-bar value")
        # These inputs are already exact-key repository results. A mismatched
        # revision signals a corrupt source binding; reconstructed and forming
        # data are simply outside ACTUAL_SYSTEM origin accounting.
        if bar.raw.instrument_revision != key.contract_revision:
            raise ValueError("native M1 source bar conflicts with the requested contract revision")
        if (bar.interval != BarIntervalV2.M1 or not bar.final
                or bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM):
            continue
        _validate_origin(key, bar.close_at_ns)
        if after_close_at_ns is not None and bar.close_at_ns <= after_close_at_ns:
            continue
        prior = by_close.get(bar.close_at_ns)
        if prior is None or (bar.raw.available_at_ns, bar.content_hash) < (
            prior.raw.available_at_ns, prior.content_hash
        ):
            by_close[bar.close_at_ns] = bar

    plans: list[S3M1OriginAccountingPlan] = []
    for close_at_ns, bar in sorted(by_close.items())[:max_origins]:
        durable = find_s3_m1_origin_accounting_state(event_rows, gate_rows, key, close_at_ns)
        origin_ref = s3_m1_origin_ref(key, close_at_ns)
        deadline_ns = close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS
        if durable is not None:
            action, durable_entry = durable
            plans.append(S3M1OriginAccountingPlan(
                key, close_at_ns, origin_ref, bar.content_hash, deadline_ns, action,
                durable_entry.artifact_ref,
            ))
            continue
        disposition = classify_s3_m1_bar(bar, now_ns=now_ns)
        if disposition == S3M1BarDisposition.TIMELY:
            action = S3M1OriginAccountingAction.CREATE_TIMELY_EVENT
        elif disposition == S3M1BarDisposition.LATE:
            action = S3M1OriginAccountingAction.CREATE_LATE_TEST_GATE
        elif disposition == S3M1BarDisposition.FUTURE_AVAILABLE:
            # A repository page must be causally available by now_ns; a
            # future-dated member means source filtering was violated.
            raise ValueError("native M1 origin page contains future source availability")
        else:
            # The explicit eligibility checks above cover all non-accountable
            # bar classes; an unexpected classification fails closed.
            raise ValueError("native M1 source bar has an unsupported accounting disposition")
        plans.append(S3M1OriginAccountingPlan(
            key, close_at_ns, origin_ref, bar.content_hash, deadline_ns, action,
        ))
    return tuple(plans)


def find_s3_m1_origin_accounting_checkpoint(
    entries: Iterable[ArtifactIndexEntryV2],
    key: InstrumentKeyV2,
) -> S3NativeM1OriginAccountingCheckpointV1 | None:
    """Validate the full append-only checkpoint chain for one exact key."""
    checkpoints: dict[int, tuple[S3NativeM1OriginAccountingCheckpointV1, ArtifactIndexEntryV2]] = {}
    for entry in entries:
        if entry.artifact_type != S3_M1_ACCOUNTING_CHECKPOINT_TYPE:
            continue
        raw = entry.metadata.get("checkpoint")
        if not isinstance(raw, Mapping):
            raise ValueError("native M1 origin checkpoint has no typed body")
        body_key = raw.get("instrument_key")
        if not isinstance(body_key, Mapping):
            raise ValueError("native M1 origin checkpoint has no full instrument identity")
        checkpoint = S3NativeM1OriginAccountingCheckpointV1.from_dict(raw)
        if checkpoint.instrument_key != key:
            continue
        if (entry.artifact_ref != checkpoint.content_hash
                or entry.content_hash != checkpoint.content_hash
                or entry.created_at_ns != checkpoint.created_at_ns
                or entry.available_at_ns != checkpoint.available_at_ns):
            raise ValueError("native M1 origin checkpoint hash or index timing does not verify")
        prior = checkpoints.get(checkpoint.generation)
        if prior is not None and prior[0].content_hash != checkpoint.content_hash:
            raise ValueError("native M1 origin checkpoint generation has conflicting successors")
        checkpoints[checkpoint.generation] = (checkpoint, entry)

    if not checkpoints:
        return None
    generations = sorted(checkpoints)
    if generations != list(range(1, generations[-1] + 1)):
        raise ValueError("native M1 origin checkpoint chain has a missing generation")
    previous: S3NativeM1OriginAccountingCheckpointV1 | None = None
    for generation in generations:
        current = checkpoints[generation][0]
        if previous is None:
            if current.previous_checkpoint_ref is not None:
                raise ValueError("native M1 origin checkpoint chain has no valid root")
        else:
            if current.previous_checkpoint_ref != previous.content_hash:
                raise ValueError("native M1 origin checkpoint chain forks or skips its predecessor")
            if (current.last_accounted_close_at_ns is not None
                    and previous.last_accounted_close_at_ns is not None
                    and current.last_accounted_close_at_ns < previous.last_accounted_close_at_ns):
                raise ValueError("native M1 origin checkpoint accounted close regressed")
            if current.source_available_through_ns < previous.source_available_through_ns:
                raise ValueError("native M1 origin checkpoint source watermark regressed")
            if current.available_at_ns < previous.available_at_ns:
                raise ValueError("native M1 origin checkpoint time regressed")
            if previous.scan_complete:
                if current.source_available_from_ns < previous.source_available_through_ns:
                    raise ValueError("native M1 origin checkpoint overlaps a completed source window")
            elif (current.source_available_from_ns != previous.source_available_from_ns
                  or current.source_available_through_ns != previous.source_available_through_ns):
                raise ValueError("native M1 origin checkpoint rebased an unfinished source window")
            elif not current.scan_complete and (
                previous.source_scan_after_close_at_ns is None
                or current.source_scan_after_close_at_ns is None
                or current.source_scan_after_close_at_ns <= previous.source_scan_after_close_at_ns
            ):
                raise ValueError("native M1 origin checkpoint close cursor did not advance")
        previous = current
    return previous


def classify_s3_m1_bar(
    bar: CausalBarV2,
    *,
    now_ns: int,
    max_lateness_ns: int = S3_M1_DEFAULT_MAX_LATENESS_NS,
) -> S3M1BarDisposition:
    """Classify first-event eligibility without altering source timestamps."""
    timestamp(now_ns, field="now_ns")
    if type(max_lateness_ns) is not int or max_lateness_ns <= 0:
        raise ValueError("max_lateness_ns must be a positive integer")
    if bar.interval != BarIntervalV2.M1:
        return S3M1BarDisposition.WRONG_INTERVAL
    if not bar.final:
        return S3M1BarDisposition.NOT_FINAL
    if bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM:
        return S3M1BarDisposition.NON_ACTUAL_AVAILABILITY
    if now_ns < bar.raw.available_at_ns:
        return S3M1BarDisposition.FUTURE_AVAILABLE
    if (bar.raw.available_at_ns - bar.close_at_ns > max_lateness_ns
            or now_ns - bar.close_at_ns > max_lateness_ns):
        return S3M1BarDisposition.LATE
    return S3M1BarDisposition.TIMELY


def classify_s3_m1_replay(
    event: _DecisionEvent,
    *,
    now_ns: int,
    already_processed: bool = False,
) -> S3M1ReplayDisposition:
    """Reuse the frozen event or expose its existing expiry/receipt state.

    The comparison intentionally uses the stored event deadline. It never
    derives a new cutoff or deadline from ``now_ns`` during restart.
    """
    timestamp(now_ns, field="now_ns")
    if event.event_type != S3_M1_EVENT_TYPE:
        raise ValueError("replay classification requires a native M1 decision event")
    sha256_ref(event.event_id, field="event_id")
    timestamp(event.available_at_ns, field="event.available_at_ns")
    timestamp(event.deadline_ns, field="event.deadline_ns")
    if type(already_processed) is not bool:
        raise ValueError("already_processed must be bool")
    if already_processed:
        return S3M1ReplayDisposition.ALREADY_PROCESSED
    if now_ns < event.available_at_ns:
        return S3M1ReplayDisposition.NOT_YET_AVAILABLE
    if now_ns > event.deadline_ns:
        return S3M1ReplayDisposition.EXPIRED
    return S3M1ReplayDisposition.REUSE


def _validate_origin(key: InstrumentKeyV2, close_at_ns: int) -> None:
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("key must be an exact InstrumentKeyV2")
    timestamp(close_at_ns, field="close_at_ns")
    if close_at_ns % _NANOSECONDS_PER_MINUTE:
        raise ValueError("native M1 close origin must align to a UTC minute boundary")
