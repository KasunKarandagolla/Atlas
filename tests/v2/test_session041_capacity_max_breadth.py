"""Offline max-breadth bulk persistence measurement; not live qualification."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from v2.test_session041_scale_review import measure_bounded_cycles


def _process_peak_rss_bytes() -> int | None:
    """Return the process high-water RSS where the host exposes it."""
    status = Path("/proc/self/status")
    if status.is_file():
        for line in status.read_text(encoding="ascii").splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    try:
        import resource
    except ImportError:  # pragma: no cover - Windows does not provide resource
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux and the other supported Unix hosts report KiB.
    return int(peak if sys.platform == "darwin" else peak * 1024)


def test_three_full_bulk_cycles_at_maximum_synthetic_breadth(tmp_path: Path) -> None:
    """Exercise 4096 products/8192 receipts per cycle through SQLite and Parquet.

    Inputs are deterministic synthetic venue-shaped public rows. This uses the
    production collector, repository, Parquet archive, broad workset publisher,
    repository restart, exact-ref hydration and archive readback through the
    existing measurement helper. It has no venue network, stream queue,
    enrichment, scheduler, decisions, concurrent reports or endurance phase.
    """
    peak_rss_before = _process_peak_rss_bytes()
    metrics = measure_bounded_cycles(tmp_path, per_venue=2048, cycles=3)
    peak_rss_after = _process_peak_rss_bytes()

    metrics["rss"] = {
        "process_peak_before_bytes": peak_rss_before,
        "process_peak_after_bytes": peak_rss_after,
        "scope": "PROCESS_HIGH_WATER; MAY INCLUDE EARLIER TESTS IN THIS PROCESS",
    }
    metrics["qualification_limits"] = [
        "Synthetic fixed metadata and receipts; no live network or venue behavior",
        "Bulk polling only; no public stream queue or capture worker",
        "No history/enrichment scheduler, decisions, reports, or concurrent WAL readers",
        "Three acquisitions are a bounded load probe, not sustained/endurance evidence",
    ]
    metrics["capacity_qualified"] = False
    print("SESSION041_MAX_BREADTH_CAPACITY=" + json.dumps(metrics, sort_keys=True))

    assert metrics["population"] == 4096
    assert metrics["cycles"] == 3
    assert [sample["record_count"] for sample in metrics["samples"]] == [8192] * 3
    assert metrics["integrity"]["exact_raw_rows_verified"] == 24_576
    assert metrics["integrity"]["exact_index_metadata_verified_after_restart"] == 24_576
    assert metrics["integrity"]["cutoff_worksets_verified_after_restart"] == 3
    assert metrics["capacity_qualified"] is False
