"""PySide6 observer window. It has no runtime, order, or risk control handles."""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyqtgraph as pg
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from atlas.v2.desktop.ipc import IPCProtocolError, ProjectionClient, read_token
from atlas.v2.desktop.projection import DesktopChartSeriesV2, DesktopSnapshotV2

from .models import current_freshness, display_time, scanner_display


class AtlasDesktop(QMainWindow):
    def __init__(self, client: ProjectionClient) -> None:
        super().__init__()
        self.client = client
        self.snapshot: DesktopSnapshotV2 | None = None
        self.setWindowTitle("ATLAS V2 — Read-only evidence observer")
        self.resize(1280, 760)
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self._overview_tab()
        self._scanner_tab()
        self._chart_tab()
        self._watches_tab()
        self._evidence_tab()
        self.banner = QLabel("Connecting to the read-only projection service…")
        self.statusBar().addPermanentWidget(self.banner, 1)
        self.timer = QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.refresh)
        self.timer.start()
        self.refresh()

    def _table(self, headers: tuple[str, ...]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.horizontalHeader().setStretchLastSection(True)
        return table

    def _overview_tab(self) -> None:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        self.overview_summary = QLabel("No snapshot received")
        self.overview_summary.setWordWrap(True)
        layout.addWidget(self.overview_summary)
        self.status_table = self._table(("Status", "State", "Value", "Reason", "Observed UTC"))
        layout.addWidget(self.status_table)
        self.tabs.addTab(panel, "Overview")

    def _scanner_tab(self) -> None:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        self.scanner_table = self._table(("Venue", "Product", "Instrument", "Eligibility", "Observed", "Strategy / policy",
            "Rank", "Selection", "Watch", "Sizing", "Evaluation", "Reasons", "Expiry UTC", "Evidence", "Evidence class"))
        layout.addWidget(self.scanner_table)
        self.tabs.addTab(panel, "Scanner")

    def _chart_tab(self) -> None:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        self.chart_title = QLabel("Select an instrument from Scanner; chart uses persisted causal bars only.")
        layout.addWidget(self.chart_title)
        self.chart = pg.PlotWidget(axisItems={"bottom": pg.AxisItem(orientation="bottom")})
        self.chart.showGrid(x=True, y=True, alpha=0.2)
        self.chart.setLabel("left", "Price")
        layout.addWidget(self.chart)
        self.tabs.addTab(panel, "Chart")
        self.scanner_table.itemSelectionChanged.connect(self._load_selected_chart)

    def _watches_tab(self) -> None:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        self.watch_table = self._table(("Venue", "Product", "Instrument", "Strategy", "Policy hash", "State",
            "Created UTC", "Wake UTC", "Expiry UTC", "Invalidation", "Evidence refs", "Lifecycle"))
        layout.addWidget(self.watch_table)
        self.tabs.addTab(panel, "Watches")

    def _evidence_tab(self) -> None:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        self.evidence_table = self._table(("Artifact", "Version", "Content ref", "Provenance", "Decision UTC",
            "Event UTC", "Available UTC", "Source health", "Status", "Outcome target", "Reasons", "Class"))
        layout.addWidget(self.evidence_table)
        self.evidence_detail = QLabel("Select an evidence row to inspect its sanitized metadata.")
        self.evidence_detail.setWordWrap(True)
        layout.addWidget(self.evidence_detail)
        self.evidence_table.itemSelectionChanged.connect(self._show_evidence_detail)
        self.tabs.addTab(panel, "Evidence")

    @staticmethod
    def _fill(table: QTableWidget, rows: list[tuple[str, ...]]) -> None:
        table.setRowCount(len(rows))
        for r, values in enumerate(rows):
            for c, value in enumerate(values):
                table.setItem(r, c, QTableWidgetItem(str(value)))
        table.resizeColumnsToContents()

    def refresh(self) -> None:
        try:
            raw = self.client.request("snapshot")
            self.snapshot = DesktopSnapshotV2.from_dict(raw)
            freshness = current_freshness(self.snapshot, now_ns=time.time_ns())
            self.banner.setText(f"Projection service connected · snapshot {freshness.lower()} · observer only")
            self._render_snapshot()
        except (OSError, IPCProtocolError, ValueError, KeyError, TypeError) as exc:
            code = exc.code if isinstance(exc, IPCProtocolError) else "SERVICE_UNAVAILABLE"
            self.banner.setText(f"Projection service unavailable · {code} · reconnecting")

    def _render_snapshot(self) -> None:
        assert self.snapshot is not None
        snap = self.snapshot
        counts = ", ".join(f"{name}: {count}" for name, count in snap.overview.decision_counts) or "No terminal decisions"
        self.overview_summary.setText(
            f"Snapshot: {display_time(snap.generated_at_ns)} UTC · valid until {display_time(snap.valid_until_ns)} UTC\n"
            f"Universe {snap.overview.universe_instruments} · observed {snap.overview.observed_instruments} · "
            f"scanner eligible {snap.overview.scanner_eligible_instruments}\nDecisions: {counts}\n"
            f"Unavailable evidence: {', '.join(snap.overview.unavailable_fields) or 'none'}\n"
            "Economic value: NOT ESTIMABLE · venue qualification: UNVERIFIED · capital disabled"
        )
        self._fill(self.status_table, [(x.name, x.state, x.value, x.reason_code or "NONE", display_time(x.observed_at_ns))
            for x in snap.overview.statuses])
        self._fill(self.scanner_table, [scanner_display(x) for x in snap.scanner_rows])
        self._fill(self.watch_table, [(x.venue, x.product, x.symbol, x.strategy_id, x.policy_hash, x.state,
            display_time(x.created_at_ns), display_time(x.wake_at_ns), display_time(x.expires_at_ns),
            ", ".join(x.invalidation_codes) or "NONE", ", ".join(x.evidence_refs), x.current_status)
            for x in snap.watch_rows])
        self._fill(self.evidence_table, [(x.artifact_type, x.schema_version, x.content_ref, x.provenance,
            display_time(x.decision_at_ns), display_time(x.event_at_ns), display_time(x.available_at_ns),
            x.source_health, x.status, x.label_target or "NOT_APPLICABLE", ", ".join(x.reason_codes) or "NONE",
            "SYNTHETIC FIXTURE" if x.synthetic_fixture else "PERSISTED EVIDENCE") for x in snap.evidence])

    def _load_selected_chart(self) -> None:
        if self.snapshot is None:
            return
        row_index = self.scanner_table.currentRow()
        if row_index < 0 or row_index >= len(self.snapshot.scanner_rows):
            return
        row = self.snapshot.scanner_rows[row_index]
        try:
            raw = self.client.request("chart", {"key_json": row.key_json, "interval": "1H",
                "information_cutoff_ns": self.snapshot.generated_at_ns, "availability_view": "ACTUAL_SYSTEM", "limit": 2000})
            series = DesktopChartSeriesV2.from_dict(raw)
        except (OSError, IPCProtocolError, ValueError, KeyError, TypeError) as exc:
            code = exc.code if isinstance(exc, IPCProtocolError) else "CHART_UNAVAILABLE"
            self.chart.clear()
            self.chart_title.setText(f"Chart unavailable · {code}")
            return
        self._render_chart(series)

    def _render_chart(self, series: DesktopChartSeriesV2) -> None:
        self.chart.clear()
        self.chart_title.setText(
            f"{series.state} · {series.interval} · availability {series.availability_view} · "
            f"information cutoff {display_time(series.information_cutoff_ns)} UTC · "
            f"{series.reason_code or 'NO OVERLAYS'} · "
            f"{'SYNTHETIC FIXTURE' if series.synthetic_fixture else 'persisted evidence'}"
        )
        if series.state != "AVAILABLE" or not series.bars:
            return

        class Candles(pg.GraphicsObject):
            def __init__(self, bars: Any, interval_seconds: int) -> None:
                super().__init__()
                self.picture = pg.QtGui.QPicture()
                painter = pg.QtGui.QPainter(self.picture)
                for bar in bars:
                    x = bar.open_at_ns / 1_000_000_000
                    open_price, high_price, low_price, close_price = (
                        float(Decimal(getattr(bar, name))) for name in ("open", "high", "low", "close")
                    )
                    color = "#35b779" if close_price >= open_price else "#e65b6c"
                    painter.setPen(pg.mkPen(color, width=1))
                    painter.setBrush(pg.mkBrush(color))
                    painter.drawLine(pg.QtCore.QPointF(x, low_price), pg.QtCore.QPointF(x, high_price))
                    bottom, top = min(open_price, close_price), max(open_price, close_price)
                    painter.drawRect(pg.QtCore.QRectF(x - interval_seconds * 0.32, bottom,
                        interval_seconds * 0.64, max(top - bottom, abs(close_price) * 1e-7)))
                painter.end()

            def paint(self, painter: Any, option: Any, widget: Any = None) -> None:
                painter.drawPicture(0, 0, self.picture)

            def boundingRect(self) -> Any:
                return self.picture.boundingRect()

        interval_seconds = {"15M": 900, "1H": 3600, "4H": 14400}[series.interval]
        self.chart.addItem(Candles(series.bars, interval_seconds))
        first = series.bars[0].open_at_ns / 1_000_000_000
        last = series.bars[-1].open_at_ns / 1_000_000_000
        self.chart.setXRange(first - interval_seconds, last + interval_seconds, padding=0.01)
        ticks = []
        stride = max(1, len(series.bars) // 6)
        for bar in series.bars[::stride]:
            label = datetime.fromtimestamp(bar.open_at_ns / 1_000_000_000, tz=UTC).strftime("%Y-%m-%d\n%H:%M UTC")
            ticks.append((bar.open_at_ns / 1_000_000_000, label))
        self.chart.getAxis("bottom").setTicks([ticks])

    def _show_evidence_detail(self) -> None:
        if self.snapshot is None:
            return
        index = self.evidence_table.currentRow()
        if not 0 <= index < len(self.snapshot.evidence):
            return
        self.evidence_detail.setText(json.dumps(self.snapshot.evidence[index].to_dict(), sort_keys=True, indent=2))


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ATLAS V2 read-only desktop observer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--token-file", required=True)
    args = parser.parse_args()
    token = read_token(args.token_file)
    client = ProjectionClient(args.host, args.port, token)
    app = QApplication(sys.argv)
    app.setApplicationName("ATLAS V2 Observer")
    window = AtlasDesktop(client)
    window.show()
    raise SystemExit(app.exec())


if __name__ == "__main__":
    main()
