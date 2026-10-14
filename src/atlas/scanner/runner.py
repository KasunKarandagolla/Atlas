"""Minimal synchronous unattended scanner runner with injected refresh ports."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from atlas.science.research_archive import ResearchArtifactArchive

from .alerts import AlertTransport, RecordingAlertTransport
from .calendar import ScannerCalendar
from .engine import Phase4Evaluator, ScanSlotResult, run_scan_slot
from .models import (
    SLOT_NS,
    CheapScanInput,
    ScannerPolicy,
    UniverseSnapshot,
    WarmupEvidence,
    content_hash,
)
from .slot_ledger import ScannerSlotLedger


class UniverseProvider(Protocol):
    def __call__(self, slot_at_ns: int) -> UniverseSnapshot: ...


class CheapInputProvider(Protocol):
    def __call__(self, slot_at_ns: int) -> Sequence[CheapScanInput]: ...


class WarmupEvidenceProvider(Protocol):
    def __call__(self, slot_at_ns: int) -> Sequence[WarmupEvidence]: ...


def due_slot(now_ns: int) -> int:
    """Latest frozen four-hour UTC slot that has started."""
    if now_ns < 0:
        raise ValueError("clock must be UTC nanoseconds")
    return (now_ns // SLOT_NS) * SLOT_NS


@dataclass(frozen=True)
class RunnerReceipt:
    slot_at_ns: int
    mode: str
    status: str
    result: ScanSlotResult | None = None
    reasons: tuple[str, ...] = ()
    registration_ref: str | None = None


def _assert_ordered(*, instrument: str, first: int | None, second: int | None, field: str) -> None:
    if first is not None and second is not None and first > second:
        raise ValueError(f"invalid warmup timestamp ordering for {instrument}: {field}")


def _assert_causal_inputs(*, slot_at_ns: int, now_ns: int, universe: UniverseSnapshot,
                          cheap_inputs: Sequence[CheapScanInput],
                          warmup_evidence: Sequence[WarmupEvidence]) -> None:
    """Refuse decision-time evidence not known by the slot, or warmup evidence not yet known now."""
    if universe.observed_at_ns > slot_at_ns or universe.available_at_ns > slot_at_ns:
        raise ValueError("future universe snapshot cannot be used for an earlier scanner slot")
    for entry in universe.entries:
        if entry.observed_at_ns > slot_at_ns or entry.available_at_ns > slot_at_ns:
            raise ValueError("future universe entry cannot be used for an earlier scanner slot")
    for value in cheap_inputs:
        if value.slot_at_ns != slot_at_ns:
            raise ValueError("cheap input belongs to a different scanner slot")
        if value.availability_cutoff_ns > slot_at_ns:
            raise ValueError("future cheap input cannot be used for an earlier scanner slot")
    for item in warmup_evidence:
        if item.slot_at_ns != slot_at_ns:
            raise ValueError("warmup evidence belongs to a different scanner slot")
        for timestamp in (item.job_enqueued_at_ns, item.job_started_at_ns, item.job_finished_at_ns):
            if timestamp is not None and timestamp > now_ns:
                raise ValueError("future warmup evidence cannot be used before it exists")
        _assert_ordered(instrument=item.instrument, first=item.job_enqueued_at_ns,
                        second=item.job_started_at_ns, field="enqueued>started")
        _assert_ordered(instrument=item.instrument, first=item.job_started_at_ns,
                        second=item.job_finished_at_ns, field="started>finished")
        _assert_ordered(instrument=item.instrument, first=item.job_enqueued_at_ns,
                        second=item.job_finished_at_ns, field="enqueued>finished")


class ScannerRunner:
    """Synchronous research/alert runner; never approves, reserves or executes."""

    def __init__(self, *, policy: ScannerPolicy, universe_provider: UniverseProvider,
                 cheap_input_provider: CheapInputProvider, warmup_evidence_provider: WarmupEvidenceProvider,
                 archive: ResearchArtifactArchive | None = None, calendar: ScannerCalendar | None = None,
                 evaluator: Phase4Evaluator | None = None, alert_transport: AlertTransport | None = None,
                 clock: Callable[[], int] | None = None):
        self.policy = policy
        self.universe_provider = universe_provider
        self.cheap_input_provider = cheap_input_provider
        self.warmup_evidence_provider = warmup_evidence_provider
        self.archive = archive
        self.calendar = calendar or ScannerCalendar(archive)
        self.slot_ledger = ScannerSlotLedger(archive) if archive is not None else None
        self.evaluator = evaluator
        self.alert_transport = alert_transport
        self.clock = clock or (lambda: 0)

    def _now(self, now_ns: int | None) -> int:
        return self.clock() if now_ns is None else now_ns

    def run_slot(self, slot_at_ns: int, *, mode: str = "CATCH_UP", now_ns: int | None = None) -> RunnerReceipt:
        if mode not in {"LIVE", "CATCH_UP"}:
            raise ValueError("runner mode must be LIVE or CATCH_UP")
        if slot_at_ns % SLOT_NS:
            raise ValueError("runner slots are frozen four-hour UTC slots")
        now = self._now(now_ns)
        if mode == "LIVE" and slot_at_ns != due_slot(now):
            raise ValueError("LIVE runner may only process the latest due slot; use CATCH_UP diagnostics")
        if mode == "LIVE" and (self.calendar.slot_rows(slot_at_ns)
                                or (self.slot_ledger is not None and self.slot_ledger.has_completed(slot_at_ns))):
            return RunnerReceipt(slot_at_ns, mode, "ALREADY_COMPLETED", None, ("SLOT_ALREADY_PERSISTED",))
        diagnostic = mode == "CATCH_UP"
        if not diagnostic and self.slot_ledger is None:
            raise ValueError("LIVE scanner requires an archive for durable slot registration")
        registration_ref = None
        attempt = 1
        if not diagnostic and self.slot_ledger is not None:
            registration_ref = self.slot_ledger.register(slot_at_ns=slot_at_ns, policy=self.policy,
                                                         registered_at_ns=now)
            prior_progress = self.slot_ledger.latest_progress(slot_at_ns)
            if prior_progress is not None:
                attempt = int(prior_progress["attempt"]) + 1
            assert registration_ref is not None
            self.slot_ledger.record_progress(registration_ref=registration_ref, slot_at_ns=slot_at_ns,
                attempt=attempt, status="STARTED", observed_at_ns=now)

        failure_status = "SOURCE_UNAVAILABLE"
        failure_reason = "UNIVERSE_SOURCE_UNAVAILABLE"
        try:
            # Registration and STARTED are durable before any provider can
            # begin work. A crash or source failure therefore leaves an
            # explicit expected slot in the archive.
            universe = self.universe_provider(slot_at_ns)
            failure_status = "FAILED"
            failure_reason = "UNIVERSE_CHRONOLOGY_INVALID"
            _assert_causal_inputs(slot_at_ns=slot_at_ns, now_ns=now, universe=universe, cheap_inputs=(),
                                  warmup_evidence=())
            if not diagnostic and self.slot_ledger is not None:
                assert registration_ref is not None
                self.slot_ledger.record_progress(registration_ref=registration_ref, slot_at_ns=slot_at_ns,
                    attempt=attempt, status="UNIVERSE_AVAILABLE", observed_at_ns=max(now, universe.available_at_ns),
                    universe_ref=universe.hash(), eligible_instruments=tuple(sorted(
                        entry.instrument for entry in universe.eligible_entries())))
            failure_status = "SOURCE_UNAVAILABLE"
            failure_reason = "CHEAP_INPUT_SOURCE_UNAVAILABLE"
            cheap_inputs = tuple(self.cheap_input_provider(slot_at_ns))
            failure_reason = "WARMUP_SOURCE_UNAVAILABLE"
            warmup_evidence = tuple(self.warmup_evidence_provider(slot_at_ns))
            failure_status = "FAILED"
            failure_reason = "INPUT_CHRONOLOGY_INVALID"
            _assert_causal_inputs(slot_at_ns=slot_at_ns, now_ns=now, universe=universe, cheap_inputs=cheap_inputs,
                                  warmup_evidence=warmup_evidence)
        except Exception:
            if not diagnostic and self.slot_ledger is not None:
                try:
                    assert registration_ref is not None
                    self.slot_ledger.record_progress(registration_ref=registration_ref, slot_at_ns=slot_at_ns,
                        attempt=attempt, status=failure_status, observed_at_ns=now,
                        reason_code=failure_reason)
                except Exception:
                    # A durable STARTED record still preserves the unfinished
                    # expected slot if recording the failure itself is blocked.
                    pass
            raise
        target_calendar = ScannerCalendar() if diagnostic else self.calendar
        target_archive = None if diagnostic else self.archive
        target_transport = RecordingAlertTransport() if diagnostic else self.alert_transport
        try:
            result = run_scan_slot(slot_at_ns=slot_at_ns, universe=universe, cheap_inputs=cheap_inputs,
                                   policy=self.policy, warmup_evidence=warmup_evidence,
                                   evaluator=self.evaluator, alert_transport=target_transport,
                                   archive=target_archive, calendar=target_calendar, now_ns=now,
                                   persist=not diagnostic)
        except Exception:
            if not diagnostic and self.slot_ledger is not None:
                try:
                    assert registration_ref is not None
                    self.slot_ledger.record_progress(registration_ref=registration_ref, slot_at_ns=slot_at_ns,
                        attempt=attempt, status="FAILED", observed_at_ns=now,
                        universe_ref=universe.hash(), eligible_instruments=tuple(sorted(
                            entry.instrument for entry in universe.eligible_entries())),
                        reason_code="SCAN_EXECUTION_FAILED")
                except Exception:
                    pass
            raise
        if not diagnostic and self.slot_ledger is not None:
            assert registration_ref is not None
            result_ref = content_hash({"universe_ref": universe.hash(),
                "calendar_row_refs": [row.hash() for row in result.calendar_rows]})
            self.slot_ledger.record_progress(registration_ref=registration_ref, slot_at_ns=slot_at_ns,
                attempt=attempt, status="COMPLETED", observed_at_ns=now, universe_ref=universe.hash(),
                eligible_instruments=tuple(sorted(entry.instrument for entry in universe.eligible_entries())),
                result_ref=result_ref)
        return RunnerReceipt(slot_at_ns, mode, "DIAGNOSTIC_ONLY" if diagnostic else "COMPLETED", result,
                             registration_ref=registration_ref)

    def run_due_slot(self, now_ns: int | None = None) -> RunnerReceipt:
        now = self._now(now_ns)
        return self.run_slot(due_slot(now), mode="LIVE", now_ns=now)

    def missed_slots(self, since_ns: int, now_ns: int | None = None) -> tuple[int, ...]:
        """Deterministic catch-up diagnostics; never supplies later data retroactively."""
        now = self._now(now_ns)
        latest = due_slot(now)
        if since_ns > latest:
            return ()
        first = ((since_ns + SLOT_NS - 1) // SLOT_NS) * SLOT_NS
        return tuple(slot for slot in range(first, latest, SLOT_NS)
                     if slot not in {row.scan_slot_at_ns for row in self.calendar.records()})

    def run_catch_up(self, slot_at_ns: int, *, now_ns: int | None = None) -> RunnerReceipt:
        return self.run_slot(slot_at_ns, mode="CATCH_UP", now_ns=now_ns)
