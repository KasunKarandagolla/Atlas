from __future__ import annotations

from atlas.domain.enums import ReconciliationHealth
from atlas.runtime.recovery import RecoveryDecision, RecoveryProtectionEvidence, run_recovery

T0 = 1_700_000_000_000_000_000


def _kwargs():
    return {
        "recovery_run_id": "recovery-1", "writer_id": "writer-1", "writer_epoch": 1, "journal_schema_version": 4,
        "unresolved_intent_ids": (), "unresolved_command_ids": (), "unknown_command_ids": (),
        "reconciliation_health": ReconciliationHealth.CURRENT, "protection_uncertainty_summary": "diagnostic only",
        "started_at_ns": T0, "ended_at_ns": T0, "venue_observations_obtained": True,
        "venue_evidence_refs": ("query-1",), "prerequisites_ok": True,
    }


def test_recovery_ready_requires_positive_flat_or_current_protection_evidence():
    without = run_recovery(**_kwargs())
    assert without.decision == RecoveryDecision.REMAIN_RECOVERING
    with_flat = run_recovery(
        **_kwargs(), protection_evidence=RecoveryProtectionEvidence(True, False, ("flat-cert-1",))
    )
    assert with_flat.decision == RecoveryDecision.READY
