"""Versioned cheap scanner priority score.

This is deliberately an engineering heuristic for allocating compute.  It is
not an alpha model, never authorizes capital, and uses only caller-supplied
past-only causal inputs.
"""

from __future__ import annotations

import math
from collections.abc import Iterable

from .models import CheapScanInput, CheapScanObservation

CHEAP_SCANNER_VERSION = "CHEAP_PRIORITY_V1"
SCANNER_PRIORITY_LABEL = "SCANNER_PRIORITY_ONLY_NOT_ALPHA"
VOLATILITY_FLOOR = 1e-9


def _score(value: CheapScanInput) -> float:
    if not math.isfinite(value.return_24h) or not math.isfinite(value.volatility_24h):
        raise ValueError("cheap scan inputs must be finite")
    if value.volatility_24h < 0:
        raise ValueError("cheap scan volatility must be nonnegative")
    return abs(value.return_24h) / max(value.volatility_24h, VOLATILITY_FLOOR)


def cheap_scan(inputs: Iterable[CheapScanInput], *, scanner_policy_version: str,
               scorer_version: str = CHEAP_SCANNER_VERSION) -> tuple[CheapScanObservation, ...]:
    """Return one deterministic, auditable priority observation per instrument."""
    observations: list[CheapScanObservation] = []
    seen: set[str] = set()
    for value in inputs:
        if value.instrument in seen:
            raise ValueError("duplicate cheap-scan instrument")
        seen.add(value.instrument)
        if value.availability_cutoff_ns > value.slot_at_ns:
            raise ValueError("cheap scan used data after its availability cutoff")
        observations.append(CheapScanObservation(
            slot_at_ns=value.slot_at_ns,
            scanner_policy_version=scanner_policy_version,
            scorer_version=scorer_version,
            instrument=value.instrument,
            availability_cutoff_ns=value.availability_cutoff_ns,
            return_24h=value.return_24h,
            volatility_24h=value.volatility_24h,
            quote_volume_notional=value.quote_volume_notional,
            score=_score(value),
            input_hash=value.hash(),
            label=SCANNER_PRIORITY_LABEL,
        ))
    return tuple(sorted(observations, key=lambda item: item.instrument))
