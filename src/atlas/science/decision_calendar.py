"""Complete immutable BTC/ETH Phase-4 decision-calendar interface for Phase 5."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace

from atlas.science.evaluation import DecisionStatus
from atlas.science.phase4_engine import Phase4Evaluation
from atlas.science.research_archive import ResearchArtifactArchive


@dataclass(frozen=True)
class DecisionCalendarRecord:
    decision_slot_id: str
    slot_at_ns: int
    instrument: str
    strategy_version: str
    availability_cutoff_ns: int
    feature_snapshot_hash: str | None
    signal: str | None
    gate_status: str
    gate_reasons: tuple[str, ...]
    risk_status: str
    risk_reasons: tuple[str, ...]
    scenario_support_status: str
    b0_status: DecisionStatus
    a0_status: DecisionStatus
    trade_plan_id: str | None
    trade_plan_hash: str | None
    reasons: tuple[str, ...]
    pnl: str = "0"
    matured_outcome_ref: str | None = None
    matured_fill_status: str | None = None
    matured_exit_status: str | None = None

    def hash(self) -> str:
        payload = json.dumps(self, default=lambda value: value.value if hasattr(value, "value") else value.__dict__,
                             sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def mature(self, outcome_ref: str, pnl: str, *, fill_status: str | None = None,
               exit_status: str | None = None) -> DecisionCalendarRecord:
        settled = (outcome_ref, pnl, fill_status, exit_status)
        current = (self.matured_outcome_ref, self.pnl, self.matured_fill_status, self.matured_exit_status)
        if self.matured_outcome_ref is not None and current != settled:
            raise ValueError("immutable decision outcome conflict")
        return replace(self, matured_outcome_ref=outcome_ref, pnl=pnl, matured_fill_status=fill_status,
                       matured_exit_status=exit_status)


class DecisionCalendar:
    def __init__(self, archive: ResearchArtifactArchive | None = None):
        self._records: dict[tuple[int, str], DecisionCalendarRecord] = {}
        self._archive = archive

    def append(self, record: DecisionCalendarRecord) -> DecisionCalendarRecord:
        key = (record.slot_at_ns, record.instrument)
        existing = self._records.get(key)
        if existing is not None and existing != record:
            raise ValueError("one immutable outcome required per slot/instrument")
        self._records[key] = existing or record
        if self._archive is not None:
            self._archive.append("decision_calendar", record)
        return self._records[key]

    def records(self) -> tuple[DecisionCalendarRecord, ...]:
        return tuple(self._records[key] for key in sorted(self._records))

    def mature(self, slot_at_ns: int, instrument: str, outcome_ref: str, pnl: str, *,
               fill_status: str | None = None, exit_status: str | None = None) -> DecisionCalendarRecord:
        """Attach the later matured outcome; the earlier decision stays immutable."""
        key = (slot_at_ns, instrument)
        existing = self._records[key]
        updated = existing.mature(outcome_ref, pnl, fill_status=fill_status, exit_status=exit_status)
        self._records[key] = updated
        if self._archive is not None:
            self._archive.append("decision_calendar", updated)
        return updated


def record_from_evaluation(*, slot_id: str, strategy_version: str, availability_cutoff_ns: int,
                           evaluation: Phase4Evaluation, slot_at_ns: int, instrument: str,
                           feature_snapshot_hash: str, signal: str) -> DecisionCalendarRecord:
    evidence = evaluation.evidence
    plan = evaluation.trade_plan
    reasons = tuple(filter(None, (evaluation.a0.reason, *evidence.rejection_reasons,
                                  *evidence.not_estimable_reasons)))
    return DecisionCalendarRecord(slot_id, slot_at_ns, instrument, strategy_version, availability_cutoff_ns,
                                  feature_snapshot_hash, signal, evidence.market_gate_status.value,
                                  evidence.market_gate_reasons + evidence.event_gate_reasons,
                                  "PASS" if not evidence.risk_reasons else "FAIL", evidence.risk_reasons,
                                  "PASS" if not evidence.not_estimable_reasons else "NOT_ESTIMABLE",
                                  evaluation.b0.status, evaluation.a0.status, plan.plan_id if plan else None,
                                  plan.plan_hash() if plan else None, reasons)


def evaluate_complete_calendar(slot_times_ns: Sequence[int], evaluator: Callable[[int, str], DecisionCalendarRecord],
                               calendar: DecisionCalendar) -> tuple[DecisionCalendarRecord, ...]:
    """Evaluate every BTC/ETH four-hour slot; no trade-only filtering is possible."""
    for slot_at_ns in sorted(set(slot_times_ns)):
        if slot_at_ns % (4 * 3_600_000_000_000):
            raise ValueError("complete calendar accepts four-hour slots only")
        for instrument in ("BTCUSDT", "ETHUSDT"):
            record = evaluator(slot_at_ns, instrument)
            if record.slot_at_ns != slot_at_ns or record.instrument != instrument:
                raise ValueError("calendar evaluator returned mismatched slot/instrument")
            calendar.append(record)
    return calendar.records()
