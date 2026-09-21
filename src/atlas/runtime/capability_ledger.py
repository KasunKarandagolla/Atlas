"""Capability evidence ledger (freeze §1.8, §1.1).

Tracks qualification state for each capability row:
- UNVERIFIED
- TESTED_OFFLINE
- TEST_GATE_TESTNET
- PASSED_TESTNET
- FAILED

Promotion to CapabilityStatus.SUPPORTED requires explicit qualification
operation and exact target profile evidence.
In THIS session, all six live Bybit capabilities MUST remain UNVERIFIED.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from atlas.domain.enums import CapabilityStatus

if TYPE_CHECKING:
    from atlas.persistence.sqlite import SQLiteJournal


class EvidenceState(StrEnum):
    """Evidence state for a capability."""
    UNVERIFIED = "UNVERIFIED"
    TESTED_OFFLINE = "TESTED_OFFLINE"
    TEST_GATE_TESTNET = "TEST_GATE_TESTNET"
    PASSED_TESTNET = "PASSED_TESTNET"
    FAILED = "FAILED"
    BLOCKED_BY_ENVIRONMENT = "BLOCKED_BY_ENVIRONMENT"


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _valid_profile_hash(value: str | None) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _valid_ref(value: str) -> bool:
    return bool(value.strip()) and value.strip().upper() not in {"PASSED", "FAILED", "REQUIRED", "PLACEHOLDER"}


@dataclass(frozen=True)
class CapabilityEvidence:
    """Evidence record for a single capability."""

    capability_name: str
    state: EvidenceState
    test_run_id: str | None
    evidence_refs: tuple[str, ...]
    test_timestamp_ns: int | None
    environment: str
    notes: str
    target_profile_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.capability_name or not self.capability_name.strip():
            raise ValueError("capability_name must be non-blank")
        if not isinstance(self.state, EvidenceState):
            raise ValueError("state must be EvidenceState")
        if self.test_run_id is not None and (not isinstance(self.test_run_id, str) or not self.test_run_id.strip()):
            raise ValueError("test_run_id must be non-blank if present")
        if self.test_timestamp_ns is not None:
            if not isinstance(self.test_timestamp_ns, int) or isinstance(self.test_timestamp_ns, bool) or self.test_timestamp_ns <= 0:
                raise ValueError("test_timestamp_ns must be positive int if present")
        if not self.environment or not self.environment.strip():
            raise ValueError("environment must be non-blank")
        if self.target_profile_hash is not None and not _valid_profile_hash(self.target_profile_hash):
            raise ValueError("target_profile_hash must be a 64 lowercase hex SHA256")


@dataclass(frozen=True)
class QualificationRecord:
    """Record of a qualification operation."""

    qualification_id: str
    capability_name: str
    previous_state: EvidenceState
    new_state: EvidenceState
    test_run_id: str
    evidence_refs: tuple[str, ...]
    qualified_by: str
    qualified_at_ns: int
    target_profile_hash: str | None

    def __post_init__(self) -> None:
        for f in ("qualification_id", "capability_name", "test_run_id", "qualified_by"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be non-blank")
        if not isinstance(self.previous_state, EvidenceState):
            raise ValueError("previous_state must be EvidenceState")
        if not isinstance(self.new_state, EvidenceState):
            raise ValueError("new_state must be EvidenceState")
        if not isinstance(self.qualified_at_ns, int) or isinstance(self.qualified_at_ns, bool) or self.qualified_at_ns <= 0:
            raise ValueError("qualified_at_ns must be positive int")


class CapabilityEvidenceLedger:
    """Ledger tracking capability evidence states.

    All six Bybit capabilities start as UNVERIFIED.
    Promotion to SUPPORTED requires explicit qualification with evidence.
    """

    REQUIRED_CAPABILITIES: tuple[str, ...] = (
        "entry_ioc_with_attached_full_mark_market_stop",
        "native_stop_visible_and_resizes_on_partial_fill",
        "reduce_only_wire_and_matching_enforcement",
        "ambiguous_submit_not_treated_as_definite_rejection",
        "external_native_stop_fill_reconciliation",
        "native_position_stop_read_and_repair_port",
    )

    def __init__(self, journal: SQLiteJournal | None = None) -> None:
        self._journal = journal
        self._evidence: dict[str, CapabilityEvidence] = {}
        self._qualification_history: list[QualificationRecord] = []
        # Initialize all as UNVERIFIED
        for cap in self.REQUIRED_CAPABILITIES:
            self._evidence[cap] = CapabilityEvidence(
                capability_name=cap,
                state=EvidenceState.UNVERIFIED,
                test_run_id=None,
                evidence_refs=(),
                test_timestamp_ns=None,
                environment="testnet",
                notes="Initial state - no testing performed",
            )
        if journal is not None:
            for name, evidence in journal.load_latest_capability_evidence().items():
                if name in self._evidence:
                    self._evidence[name] = evidence

    def get_evidence(self, capability_name: str) -> CapabilityEvidence | None:
        return self._evidence.get(capability_name)

    def get_all_evidence(self) -> dict[str, CapabilityEvidence]:
        return dict(self._evidence)

    def record_offline_test(
        self,
        capability_name: str,
        test_run_id: str,
        evidence_refs: tuple[str, ...],
        timestamp_ns: int,
        passed: bool,
        notes: str = "",
    ) -> CapabilityEvidence:
        """Record an offline test result (TESTED_OFFLINE or FAILED)."""
        if capability_name not in self.REQUIRED_CAPABILITIES:
            raise ValueError(f"Unknown capability: {capability_name}")

        current = self._evidence[capability_name]
        if current.state not in (EvidenceState.UNVERIFIED, EvidenceState.TESTED_OFFLINE, EvidenceState.FAILED):
            raise ValueError(f"Cannot record offline test for capability in state {current.state.value}")

        new_state = EvidenceState.TESTED_OFFLINE if passed else EvidenceState.FAILED
        evidence = CapabilityEvidence(
            capability_name=capability_name,
            state=new_state,
            test_run_id=test_run_id,
            evidence_refs=evidence_refs,
            test_timestamp_ns=timestamp_ns,
            environment="offline",
            notes=notes or f"Offline test {'passed' if passed else 'failed'}",
        )
        self._evidence[capability_name] = evidence
        if self._journal is not None:
            self._journal.append_capability_evidence(evidence)
        return evidence

    def record_testnet_gate(
        self,
        capability_name: str,
        test_run_id: str,
        evidence_refs: tuple[str, ...],
        timestamp_ns: int,
        passed: bool,
        notes: str = "",
        target_profile_hash: str | None = None,
    ) -> CapabilityEvidence:
        """Record a testnet gate result (TEST_GATE_TESTNET or FAILED)."""
        if capability_name not in self.REQUIRED_CAPABILITIES:
            raise ValueError(f"Unknown capability: {capability_name}")

        current = self._evidence[capability_name]
        if current.state not in (EvidenceState.UNVERIFIED, EvidenceState.TESTED_OFFLINE, EvidenceState.TEST_GATE_TESTNET, EvidenceState.FAILED):
            raise ValueError(f"Cannot record testnet gate for capability in state {current.state.value}")

        new_state = EvidenceState.TEST_GATE_TESTNET if passed else EvidenceState.FAILED
        evidence = CapabilityEvidence(
            capability_name=capability_name,
            state=new_state,
            test_run_id=test_run_id,
            evidence_refs=evidence_refs,
            test_timestamp_ns=timestamp_ns,
            environment="testnet",
            notes=notes or f"Testnet gate {'passed' if passed else 'failed'}",
            target_profile_hash=target_profile_hash,
        )
        self._evidence[capability_name] = evidence
        if self._journal is not None:
            self._journal.append_capability_evidence(evidence)
        return evidence

    def qualify_capability(
        self,
        capability_name: str,
        qualification_id: str,
        test_run_id: str,
        evidence_refs: tuple[str, ...],
        qualified_by: str,
        timestamp_ns: int,
        target_profile_hash: str | None = None,
    ) -> QualificationRecord:
        """Explicitly qualify a capability as PASSED_TESTNET.

        This is the ONLY way to reach PASSED_TESTNET.
        Requires: previous state is TEST_GATE_TESTNET with passing evidence.
        """
        if capability_name not in self.REQUIRED_CAPABILITIES:
            raise ValueError(f"Unknown capability: {capability_name}")

        current = self._evidence[capability_name]
        if current.state != EvidenceState.TEST_GATE_TESTNET:
            raise ValueError(f"Cannot qualify capability in state {current.state.value}; must be TEST_GATE_TESTNET")

        if not target_profile_hash or not _valid_profile_hash(target_profile_hash):
            raise ValueError("qualification requires an exact non-placeholder target profile hash")
        if current.test_run_id != test_run_id:
            raise ValueError("qualification test_run_id does not match the testnet gate evidence")
        if current.environment != "testnet":
            raise ValueError("qualification requires testnet gate evidence")
        if not evidence_refs or any(not _valid_ref(ref) for ref in evidence_refs):
            raise ValueError("qualification requires nonempty immutable evidence references")
        if tuple(evidence_refs) != current.evidence_refs:
            raise ValueError("qualification evidence references must exactly match the gate evidence")
        if current.target_profile_hash != target_profile_hash:
            raise ValueError("qualification target profile does not match gate evidence")

        evidence = CapabilityEvidence(
            capability_name=capability_name,
            state=EvidenceState.PASSED_TESTNET,
            test_run_id=test_run_id,
            evidence_refs=evidence_refs,
            test_timestamp_ns=timestamp_ns,
            environment="testnet",
            notes=f"Qualified by {qualified_by}",
            target_profile_hash=target_profile_hash,
        )
        self._evidence[capability_name] = evidence

        record = QualificationRecord(
            qualification_id=qualification_id,
            capability_name=capability_name,
            previous_state=current.state,
            new_state=evidence.state,
            test_run_id=test_run_id,
            evidence_refs=evidence_refs,
            qualified_by=qualified_by,
            qualified_at_ns=timestamp_ns,
            target_profile_hash=target_profile_hash,
        )
        self._qualification_history.append(record)
        if self._journal is not None:
            self._journal.append_capability_evidence(evidence)
            self._journal.append_capability_qualification(record)
        return record

    def get_qualification_history(self, capability_name: str | None = None) -> list[QualificationRecord]:
        if capability_name is None:
            return list(self._qualification_history)
        return [r for r in self._qualification_history if r.capability_name == capability_name]

    def all_unverified(self) -> bool:
        """Check if all capabilities are still UNVERIFIED."""
        return all(e.state == EvidenceState.UNVERIFIED for e in self._evidence.values())

    def any_supported_equivalent(self) -> bool:
        """Check if any capability has reached PASSED_TESTNET (equivalent to SUPPORTED)."""
        return any(e.state == EvidenceState.PASSED_TESTNET for e in self._evidence.values())

    def to_manifest_dict(self) -> dict[str, str]:
        """Export as capability status strings for manifest."""
        result = {}
        for cap in self.REQUIRED_CAPABILITIES:
            evidence = self._evidence[cap]
            # Map evidence state to CapabilityStatus
            if evidence.state == EvidenceState.PASSED_TESTNET:
                result[cap] = CapabilityStatus.SUPPORTED.value
            elif evidence.state == EvidenceState.FAILED:
                result[cap] = CapabilityStatus.FAILED.value
            elif evidence.state == EvidenceState.TEST_GATE_TESTNET:
                result[cap] = CapabilityStatus.UNSUPPORTED.value  # Gate not passed yet
            else:
                result[cap] = CapabilityStatus.UNVERIFIED.value
        return result
