"""Deep-data warmup state and compute-deadline outcomes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .models import DeadlineStatus, WarmupEvidence, WarmupState, WarmupStatus


def evaluate_warmup(*, slot_at_ns: int, requested_instruments: Sequence[str],
                    evidence: Mapping[str, WarmupEvidence], deadline_at_ns: int,
                    observed_instruments: Sequence[str] = (),
                    ) -> tuple[WarmupStatus, ...]:
    """Evaluate every observed instrument; missing warmup is never substituted."""
    requested = set(requested_instruments)
    instruments = sorted(requested | set(evidence) | set(observed_instruments))
    statuses: list[WarmupStatus] = []
    for instrument in instruments:
        item = evidence.get(instrument)
        if instrument not in requested:
            statuses.append(WarmupStatus(slot_at_ns, instrument, False, WarmupState.NOT_SELECTED,
                                         item.job_enqueued_at_ns if item else None,
                                         item.job_started_at_ns if item else None,
                                         item.job_finished_at_ns if item else None, None,
                                         DeadlineStatus.NOT_APPLICABLE, "NOT_DEEP_SELECTED"))
            continue
        if item is None:
            statuses.append(WarmupStatus(slot_at_ns, instrument, True, WarmupState.NOT_ESTIMABLE_WARMUP,
                                         None, None, None, deadline_at_ns, DeadlineStatus.MISSED,
                                         "WARMUP_EVIDENCE_ABSENT"))
            continue
        finished = item.job_finished_at_ns
        if finished is None:
            statuses.append(WarmupStatus(slot_at_ns, instrument, True, WarmupState.NOT_ESTIMABLE_WARMUP,
                                         item.job_enqueued_at_ns, item.job_started_at_ns, None, deadline_at_ns,
                                         DeadlineStatus.MISSED, item.reason or "WARMUP_JOB_NOT_FINISHED"))
            continue
        deadline = DeadlineStatus.MET if finished <= deadline_at_ns else DeadlineStatus.MISSED
        if not item.available:
            statuses.append(WarmupStatus(slot_at_ns, instrument, True, WarmupState.NOT_ESTIMABLE_WARMUP,
                                         item.job_enqueued_at_ns, item.job_started_at_ns, finished, deadline_at_ns,
                                         deadline, item.reason or "WARMUP_DATA_INCOMPLETE"))
            continue
        state = WarmupState.WARM_AVAILABLE if deadline is DeadlineStatus.MET else WarmupState.NOT_ESTIMABLE_WARMUP
        reason = "WARMUP_AVAILABLE" if deadline is DeadlineStatus.MET else "WARMUP_COMPLETED_AFTER_DEADLINE"
        statuses.append(WarmupStatus(slot_at_ns, instrument, True, state, item.job_enqueued_at_ns,
                                     item.job_started_at_ns, finished, deadline_at_ns, deadline, reason))
    return tuple(statuses)
