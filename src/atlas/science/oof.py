"""Immutable chronological OOF forecast/residual archive."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .huber_mean import HOUR_NS, HuberRidgeModel, MeanObservation


@dataclass(frozen=True)
class OOFEntry:
    instrument: str
    origin_at_ns: int
    target_at_ns: int
    sigma: float
    z: float
    forecast: float
    model_manifest_hash: str
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
        key = (observation.instrument, observation.origin_at_ns)
        candidate = OOFEntry(observation.instrument, observation.origin_at_ns, observation.origin_at_ns + HOUR_NS,
                             observation.sigma, observation.z, model.forecast(observation.instrument, observation.z), model.manifest_hash())
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
