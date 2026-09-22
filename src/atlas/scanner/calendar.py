"""Immutable complete scanner decision calendar."""

from __future__ import annotations

from atlas.science.research_archive import ResearchArtifactArchive

from .models import ScannerCalendarRow

SCANNER_DECISION_CALENDAR = "scanner_decision_calendar"


class ScannerCalendar:
    def __init__(self, archive: ResearchArtifactArchive | None = None):
        self._rows: dict[tuple[int, str], ScannerCalendarRow] = {}
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
