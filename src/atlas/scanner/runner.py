"""Minimal synchronous unattended scanner runner with injected refresh ports."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from atlas.science.research_archive import ResearchArtifactArchive

from .alerts import AlertTransport
from .calendar import ScannerCalendar
from .engine import Phase4Evaluator, ScanSlotResult, run_scan_slot
from .models import (
    SLOT_NS,
    CheapScanInput,
    ScannerPolicy,
    UniverseSnapshot,
    WarmupEvidence,
)


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


def _assert_causal_inputs(*, slot_at_ns: int, universe: UniverseSnapshot,
                          cheap_inputs: Sequence[CheapScanInput],
                          warmup_evidence: Sequence[WarmupEvidence]) -> None:
    """Refuse any provider evidence that was not known by the requested slot."""
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
            if timestamp is not None and timestamp > slot_at_ns:
                raise ValueError("future warmup evidence cannot be used for an earlier scanner slot")


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
        existing = self.calendar.slot_rows(slot_at_ns)
        if existing:
            return RunnerReceipt(slot_at_ns, mode, "ALREADY_COMPLETED", None, ("SLOT_ALREADY_PERSISTED",))
        universe = self.universe_provider(slot_at_ns)
        cheap_inputs = tuple(self.cheap_input_provider(slot_at_ns))
        warmup_evidence = tuple(self.warmup_evidence_provider(slot_at_ns))
        _assert_causal_inputs(slot_at_ns=slot_at_ns, universe=universe, cheap_inputs=cheap_inputs,
                              warmup_evidence=warmup_evidence)
        result = run_scan_slot(slot_at_ns=slot_at_ns, universe=universe, cheap_inputs=cheap_inputs,
                               policy=self.policy, warmup_evidence=warmup_evidence,
                               evaluator=self.evaluator, alert_transport=self.alert_transport,
                               archive=self.archive, calendar=self.calendar, now_ns=now)
        return RunnerReceipt(slot_at_ns, mode, "COMPLETED", result)

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
