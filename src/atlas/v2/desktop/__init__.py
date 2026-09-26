"""Read-only, versioned desktop projection and local IPC for ATLAS V2."""

from .projection import DesktopChartSeriesV2, DesktopSnapshotV2, project_snapshot

__all__ = ["DesktopChartSeriesV2", "DesktopSnapshotV2", "project_snapshot"]
