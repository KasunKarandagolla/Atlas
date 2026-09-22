"""Immutable chronological OOF forecast/residual archive."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .huber_mean import DAY_NS, HOUR_NS, HuberRidgeModel, MeanObservation

MAX_OOF_HISTORY_NS = 180 * DAY_NS
MIN_SYNCHRONIZED_OOF_NS = 60 * DAY_NS


@dataclass(frozen=True)
class OOFEntry:
    instrument: str
    origin_at_ns: int
    target_at_ns: int
    sigma: float
    z: float
    forecast: float
    model_manifest_hash: str
    fit_at_ns: int
    training_start_ns: int
    training_end_ns: int
    selected_ridge: float
    validation_intervals: tuple[tuple[int, int], ...]
    realized_return: float | None = None
    residual: float | None = None

    def mature(self, realized_return: float) -> OOFEntry:
        if self.realized_return is not None:
            if self.realized_return != realized_return:
                raise ValueError("immutable OOF result conflicts")
            return self
        return replace(self, realized_return=realized_return, residual=realized_return / self.sigma - self.forecast)


class OOFArchive:
    """Append-only in-memory representation suitable for immutable Arrow persistence."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, int], OOFEntry] = {}

    def forecast(self, observation: MeanObservation, model: HuberRidgeModel) -> OOFEntry:
        if model.fit_at_ns > observation.origin_at_ns:
            raise ValueError("OOF model fit is from the future")
        if model.training_end_ns > model.fit_at_ns or model.training_end_ns > observation.origin_at_ns:
            raise ValueError("OOF model includes labels unavailable at origin")
        key = (observation.instrument, observation.origin_at_ns)
        candidate = OOFEntry(observation.instrument, observation.origin_at_ns, observation.origin_at_ns + HOUR_NS,
                             observation.sigma, observation.z, model.forecast(observation.instrument, observation.z), model.manifest_hash(),
                             model.fit_at_ns, model.training_start_ns, model.training_end_ns, model.ridge, model.validation_intervals)
        existing = self._entries.get(key)
        if existing is not None and existing != candidate:
            raise ValueError("later refit may not rewrite OOF forecast")
        self._entries[key] = existing or candidate
        return self._entries[key]

    def mature(self, instrument: str, origin_at_ns: int, realized_return: float, available_at_ns: int) -> OOFEntry:
        key = (instrument, origin_at_ns)
        entry = self._entries[key]
        if available_at_ns < entry.target_at_ns:
            raise ValueError("future label has not matured")
        matured = entry.mature(realized_return)
        self._entries[key] = matured
        return matured

    def entries(self) -> tuple[OOFEntry, ...]:
        return tuple(sorted(self._entries.values(), key=lambda x: (x.origin_at_ns, x.instrument)))

    def retained_entries(self, as_of_ns: int) -> tuple[OOFEntry, ...]:
        """Research consumers never receive older-than-180-day residual history."""
        return tuple(x for x in self.entries() if x.target_at_ns > as_of_ns - MAX_OOF_HISTORY_NS)

    def synchronized_matured(self, as_of_ns: int) -> tuple[tuple[OOFEntry, OOFEntry], ...]:
        retained = self.retained_entries(as_of_ns)
        by_time: dict[int, dict[str, OOFEntry]] = {}
        for item in retained:
            if item.residual is not None:
                by_time.setdefault(item.origin_at_ns, {})[item.instrument] = item
        pairs = tuple((items["BTCUSDT"], items["ETHUSDT"]) for _, items in sorted(by_time.items()) if set(items) == {"BTCUSDT", "ETHUSDT"})
        if not pairs or pairs[-1][0].target_at_ns - pairs[0][0].origin_at_ns < MIN_SYNCHRONIZED_OOF_NS:
            raise ValueError("NOT_ESTIMABLE: fewer than 60 days synchronized OOF support")
        return pairs
