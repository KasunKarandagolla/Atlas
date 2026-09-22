"""Immutable complete scanner decision calendar."""

from __future__ import annotations

from atlas.science.research_archive import ResearchArtifactArchive

from .models import HOUR_NS, ScannerCalendarRow, ScannerMaturation
from .persistence import SCANNER_OUTCOME_MATURATION

SCANNER_DECISION_CALENDAR = "scanner_decision_calendar"
DEFAULT_MATURATION_HORIZON_NS = 24 * HOUR_NS


class ScannerCalendar:
    def __init__(self, archive: ResearchArtifactArchive | None = None):
        self._rows: dict[tuple[int, str], ScannerCalendarRow] = {}
        self._maturations: dict[tuple[int, str], ScannerMaturation] = {}
        self._archive = archive

    def append(self, row: ScannerCalendarRow) -> ScannerCalendarRow:
        key = (row.scan_slot_at_ns, row.instrument)
        existing = self._rows.get(key)
        if existing is not None and existing != row:
            raise ValueError("one immutable scanner outcome required per slot/instrument")
        self._rows[key] = existing or row
        if self._archive is not None:
            self._archive.append(SCANNER_DECISION_CALENDAR, row)
        return self._rows[key]

    def records(self) -> tuple[ScannerCalendarRow, ...]:
        return tuple(self._rows[key] for key in sorted(self._rows))

    def slot_rows(self, slot_at_ns: int) -> tuple[ScannerCalendarRow, ...]:
        return tuple(row for row in self.records() if row.scan_slot_at_ns == slot_at_ns)

    def missing(self, slot_at_ns: int, expected_instruments: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(instrument for instrument in sorted(expected_instruments)
                     if (slot_at_ns, instrument) not in self._rows)

    def assert_complete(self, slot_at_ns: int, expected_instruments: tuple[str, ...]) -> None:
        missing = self.missing(slot_at_ns, expected_instruments)
        if missing:
            raise ValueError(f"incomplete scanner calendar: {len(missing)} missing slot/instrument rows")

    def mature(self, slot_at_ns: int, instrument: str, *, matured_at_ns: int,
               matured_counterfactual_label_id: str, realized_policy_outcome_id: str,
               counterfactual_value: float | None, outcome_status: str, evidence_ref: str,
               evidence_hash: str, horizon_ns: int = DEFAULT_MATURATION_HORIZON_NS,
               ) -> ScannerMaturation:
        """Append matured outcome evidence without rewriting the decision row."""
        key = (slot_at_ns, instrument)
        try:
            row = self._rows[key]
        except KeyError as exc:
            raise KeyError("scanner row must exist before maturation") from exc
        if horizon_ns < 0:
            raise ValueError("maturation horizon must be nonnegative")
        horizon_end_ns = row.scan_slot_at_ns + horizon_ns
        maturation = ScannerMaturation(
            scan_slot_at_ns=slot_at_ns,
            instrument=instrument,
            original_row_hash=row.hash(),
            matured_counterfactual_label_id=matured_counterfactual_label_id,
            realized_policy_outcome_id=realized_policy_outcome_id,
            counterfactual_value=counterfactual_value,
            outcome_status=outcome_status,
            matured_at_ns=matured_at_ns,
            horizon_end_ns=horizon_end_ns,
            evidence_ref=evidence_ref,
            evidence_hash=evidence_hash,
        )
        existing = self._maturations.get(key)
        if existing is not None:
            if existing == maturation:
                return existing
            raise ValueError("conflicting scanner maturation for slot/instrument")
        self._maturations[key] = maturation
        if self._archive is not None:
            self._archive.append(SCANNER_OUTCOME_MATURATION, maturation)
        return maturation

    def maturations(self) -> tuple[ScannerMaturation, ...]:
        return tuple(self._maturations[key] for key in sorted(self._maturations))

    def maturation(self, slot_at_ns: int, instrument: str) -> ScannerMaturation | None:
        return self._maturations.get((slot_at_ns, instrument))
