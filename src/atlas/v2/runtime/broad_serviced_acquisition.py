"""Broad REST acquisition using the single retained S40 worker contract."""

from __future__ import annotations

from typing import Any

from ..data.broad_public_source import BroadPublicSnapshotV2
from .serviced_acquisition import ServicedPublicAcquisitionV1


class BroadServicedPublicAcquisitionV2(ServicedPublicAcquisitionV1):
    expected_result_type = BroadPublicSnapshotV2

    def _empty_snapshot(self, *, now_ns: int, started_at: float,
                        failure_kind: str, reason: str) -> Any:
        # A pending read contributes no fabricated observation or venue receipt.
        # The original completion stays retained until the sole writer adopts it.
        return BroadPublicSnapshotV2(
            records=(), complete=False, failure_kind=failure_kind,
            failure_reason="BROAD_PUBLIC_ACQUISITION_PENDING_OR_FAILED",
            latest_received_at_ns=0, observed_at_ns=max(now_ns, self.clock_ns()),
            request_count=0, successful_request_count=0,
            acquisition_duration_ns=max(0, int((self.monotonic() - started_at) * 1_000_000_000)),
            bootstrap_request_count=0, successful_bootstrap_request_count=0,
            source_snapshot={"schema_version": 1, "pending": self.status()["pending"],
                "complete": False, "enabled_venues": [v.value for v in self.source.enabled_venues],
                "missingness": ["PUBLIC_ACQUISITION_COMPLETION"]},
        )
