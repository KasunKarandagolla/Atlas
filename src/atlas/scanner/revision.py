"""Minimal paired full-calendar scanner revision comparison."""

from __future__ import annotations

from collections.abc import Sequence
from statistics import fmean

from .models import DeadlineStatus, DecisionStatus, ScannerCalendarRow, ScannerRevisionComparison, WarmupState


def _key(row: ScannerCalendarRow) -> tuple[int, str]:
    return row.scan_slot_at_ns, row.instrument


def _average(values: Sequence[float]) -> float | None:
    return fmean(values) if values else None


def _enqueue_to_finish_ms(row: ScannerCalendarRow) -> float | None:
    started = row.model_job_enqueued_at_ns
    finished = row.model_job_finished_at_ns
    if started is None or finished is None:
        return None
    return (finished - started) / 1_000_000


def _looks_executed_only(rows: Sequence[ScannerCalendarRow]) -> bool:
    return bool(rows) and all(row.plan_status == DecisionStatus.TRADE_CANDIDATE.value for row in rows)


def compare_scanner_revisions(previous: Sequence[ScannerCalendarRow], revised: Sequence[ScannerCalendarRow], *,
                              previous_policy_version: str, revised_policy_version: str) -> ScannerRevisionComparison:
    """Compare identical slot/instrument pairs over the complete calendar."""
    if _looks_executed_only(previous) or _looks_executed_only(revised):
        raise ValueError("scanner revision comparison requires the complete calendar, not executed trades only")
    previous_map = {_key(row): row for row in previous}
    revised_map = {_key(row): row for row in revised}
    if len(previous_map) != len(tuple(previous)) or len(revised_map) != len(tuple(revised)):
        raise ValueError("duplicate scanner calendar keys")
    if set(previous_map) != set(revised_map):
        missing_left = sorted(set(previous_map) - set(revised_map))
        missing_right = sorted(set(revised_map) - set(previous_map))
        return ScannerRevisionComparison(previous_policy_version, revised_policy_version, 0, 0, "INVALID_UNPAIRED",
                                          None, 0, 0, 0, 0, 0, None,
                                         (f"UNPAIRED_PREVIOUS={len(missing_left)}",
                                          f"UNPAIRED_REVISED={len(missing_right)}"))
    keys = sorted(previous_map)

    def count(rows: dict[tuple[int, str], ScannerCalendarRow], predicate) -> int:  # type: ignore[no-untyped-def]
        return sum(1 for key in keys if predicate(rows[key]))

    compute_delays: list[float] = []
    for mapping in (previous_map, revised_map):
        for key in keys:
            delay = _enqueue_to_finish_ms(mapping[key])
            if delay is not None:
                compute_delays.append(delay)
    previous_delays = [delay for key in keys if (delay := _enqueue_to_finish_ms(previous_map[key])) is not None]
    revised_delays = [delay for key in keys if (delay := _enqueue_to_finish_ms(revised_map[key])) is not None]
    previous_delay = _average(previous_delays)
    revised_delay = _average(revised_delays)
    delay_delta = None if previous_delay is None or revised_delay is None else revised_delay - previous_delay
    selection_effect = (count(revised_map, lambda row: row.top_k_selected) - count(previous_map, lambda row: row.top_k_selected)) / len(keys)
    reasons: list[str] = []
    status = "COMPARABLE"
    if not compute_delays:
        reasons.append("NO_COMPUTE_DELAY_SUPPORT")
    return ScannerRevisionComparison(
        previous_policy_version=previous_policy_version,
        revised_policy_version=revised_policy_version,
        paired_slots=len({key[0] for key in keys}),
        paired_instruments=len(keys),
        status=status,
        compute_delay_delta_ms=delay_delta,
        warmup_miss_delta=count(revised_map, lambda row: row.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP)
        - count(previous_map, lambda row: row.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP),
        deadline_miss_delta=count(revised_map, lambda row: row.model_deadline_status is DeadlineStatus.MISSED)
        - count(previous_map, lambda row: row.model_deadline_status is DeadlineStatus.MISSED),
        rejection_delta=count(revised_map, lambda row: row.rejection_reason is not None or row.not_estimable_reason is not None)
        - count(previous_map, lambda row: row.rejection_reason is not None or row.not_estimable_reason is not None),
        trade_candidate_delta=count(revised_map, lambda row: row.plan_status == DecisionStatus.TRADE_CANDIDATE.value)
        - count(previous_map, lambda row: row.plan_status == DecisionStatus.TRADE_CANDIDATE.value),
        no_fill_delta=count(revised_map, lambda row: row.no_fill_reason is not None)
        - count(previous_map, lambda row: row.no_fill_reason is not None),
        selection_effect_delta=selection_effect,
        reasons=tuple(reasons),
    )
