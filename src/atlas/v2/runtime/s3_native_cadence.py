"""Immutable identity and replay rules for native S3 one-minute events.

This module contains only the event-origin rules. Production owns persistence,
source health, and supervisor invocation; this module never writes repository
state or changes an existing event's cutoff/deadline.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Protocol

from .._serialization import canonical_json, sha256_json, sha256_ref, timestamp
from ..data.bars import BarIntervalV2, CausalBarV2
from ..data.raw import AvailabilityClassV2
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2

S3_M1_EVENT_TYPE = "CONFIRMED_1M_CLOSE"
S3_M1_EVENT_ID_VERSION = "OPS_DECISION_EVENT_FROM_NATIVE_M1_ORIGIN_V1"
S3_M1_ORIGIN_VERSION = "S3_NATIVE_M1_ORIGIN_V1"
S3_M1_DEFAULT_MAX_LATENESS_NS = 5_000_000_000
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
