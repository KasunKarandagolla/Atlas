"""Top-K/deep-budget and deterministic shadow exploration selection."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence

from .models import (
    CAPITAL_ENABLED_INSTRUMENTS,
    SLOT_NS,
    ExplorationSelection,
    RankBand,
    RankedObservation,
    ScannerSelection,
)

DEFAULT_TOP_K = 3
DEFAULT_DEEP_K = 5


def top_k_selection(ranked: Sequence[RankedObservation], *, top_k: int = DEFAULT_TOP_K,
                    deep_k: int = DEFAULT_DEEP_K,
                    capital_enabled: Sequence[str] = CAPITAL_ENABLED_INSTRUMENTS,
                    ) -> tuple[ScannerSelection, ...]:
    if top_k < 1 or deep_k < top_k:
        raise ValueError("top-K/deep-K must satisfy 0 < top_k <= deep_k")
    capital = set(capital_enabled)
    selections: list[ScannerSelection] = []
    for observation in sorted(ranked, key=lambda item: item.rank):
        is_top = observation.rank <= top_k
        is_capital = observation.instrument in capital
        is_deep = observation.rank <= deep_k or is_capital
        if is_top and is_capital:
            reason = "CAPITAL_WARM_AND_TOP_K"
        elif is_top:
            reason = "TOP_K"
        elif is_capital:
            reason = "CAPITAL_WARM"
        elif is_deep:
            reason = "DEEP_BUDGET"
        else:
            reason = "NOT_SELECTED"
        selections.append(ScannerSelection(
            slot_at_ns=observation.slot_at_ns,
            instrument=observation.instrument,
            cheap_score=observation.cheap_score,
            rank=observation.rank,
            rank_band=observation.rank_band,
            correlation_cluster=observation.correlation_cluster,
            top_k_selected=is_top,
            deep_selected=is_deep,
            selection_reason=reason,
        ))
    return tuple(selections)


def select_candidates(ranked: Sequence[RankedObservation], *, top_k: int = DEFAULT_TOP_K,
                      deep_k: int = DEFAULT_DEEP_K,
                      capital_enabled: Sequence[str] = CAPITAL_ENABLED_INSTRUMENTS,
                      ) -> tuple[ScannerSelection, ...]:
    """Backward-compatible explicit name for the frozen top-K/deep budget."""
    return top_k_selection(ranked, top_k=top_k, deep_k=deep_k, capital_enabled=capital_enabled)


def _band_members(ranked: Iterable[RankedObservation], band: RankBand) -> tuple[RankedObservation, ...]:
    return tuple(observation for observation in ranked if observation.rank_band is band)


def _selection_key(*, policy_version: str, universe_version: str, slot_at_ns: int, instrument: str) -> str:
    payload = "|".join((policy_version, universe_version, f"slot-{slot_at_ns}", instrument))
    return hashlib.sha256(payload.encode()).hexdigest()


def select_exploration(ranked: Sequence[RankedObservation], *, policy_version: str, universe_hash: str,
                       slot_at_ns: int, universe_version: str | None = None) -> ExplorationSelection:
    """One deterministic shadow candidate per slot from B1/B2/B3."""
    if slot_at_ns % SLOT_NS:
        raise ValueError("exploration is selected on four-hour UTC slots only")
    bands = (RankBand.B1, RankBand.B2, RankBand.B3)
    preferred = bands[(slot_at_ns // SLOT_NS) % len(bands)]
    available = {band: _band_members(ranked, band) for band in bands}
    if not any(available.values()):
        return ExplorationSelection(slot_at_ns, policy_version, universe_hash, None, None, None, 0.0,
                                    preferred, None, "NO_EXPLORATION_CANDIDATE_OUTSIDE_TOP_3")
    order = [bands[(bands.index(preferred) + offset) % len(bands)] for offset in range(len(bands))]
    selected_band = next(band for band in order if available[band])
    fallback = None if selected_band is preferred else f"{preferred.value}_EMPTY->{selected_band.value}"
    candidates = available[selected_band]
    resolved_universe = universe_version or universe_hash
    chosen = min(candidates, key=lambda item: (_selection_key(policy_version=policy_version,
                                                              universe_version=resolved_universe,
                                                              slot_at_ns=slot_at_ns,
                                                              instrument=item.instrument), item.instrument))
    return ExplorationSelection(
        slot_at_ns=slot_at_ns,
        policy_version=policy_version,
        universe_hash=universe_hash,
        band=selected_band,
        instrument=chosen.instrument,
        selection_key=_selection_key(policy_version=policy_version, universe_version=resolved_universe,
                                     slot_at_ns=slot_at_ns, instrument=chosen.instrument),
        inclusion_probability=1.0 / len(candidates),
        preferred_band=preferred,
        fallback_path=fallback,
        reason="DETERMINISTIC_BAND_HASH",
    )
