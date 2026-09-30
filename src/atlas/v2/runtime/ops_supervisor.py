"""Single-writer, restart-safe supervisor for the V2 public research pipeline.

The supervisor owns one local ``OpsRepository`` for its full process lifetime.
Source and pipeline adapters receive that repository and must compose the
existing V2 collectors, coordinators, selector, risk, action, evaluation and
calendar APIs. This module records operational progress; it does not implement
market, strategy, selection, risk or economic rules.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from threading import Event
from typing import Any, Protocol, cast

from .._serialization import canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository

OPS_SUPERVISOR_VERSION = "ATLAS_OPS_SUPERVISOR_V2_V1"
_MAX_REFS_PER_STAGE = 256


class PipelineStageV1(StrEnum):
    UNIVERSE = "UNIVERSE"
    CAUSAL_FEATURES = "CAUSAL_FEATURES"
    WATCHES_AND_SLEEVES = "WATCHES_AND_SLEEVES"
    CANDIDATE_SET = "CANDIDATE_SET"
    SELECTION = "SELECTION"
    HARD_RISK = "HARD_RISK"
    FROZEN_ACTION = "FROZEN_ACTION"
    ECONOMIC_EVALUATION = "ECONOMIC_EVALUATION"
    M1_DIAGNOSTIC = "M1_DIAGNOSTIC"
    ANALOGUE_DIAGNOSTIC = "ANALOGUE_DIAGNOSTIC"
    DECISION_CALENDAR = "DECISION_CALENDAR"


PIPELINE_STAGE_ORDER = tuple(PipelineStageV1)
_CAUSAL_STAGES = frozenset(
    {
        PipelineStageV1.UNIVERSE,
        PipelineStageV1.CAUSAL_FEATURES,
        PipelineStageV1.WATCHES_AND_SLEEVES,
        PipelineStageV1.CANDIDATE_SET,
        PipelineStageV1.SELECTION,
        PipelineStageV1.HARD_RISK,
        PipelineStageV1.FROZEN_ACTION,
    }
)
_DIAGNOSTIC_STAGES = frozenset({PipelineStageV1.M1_DIAGNOSTIC, PipelineStageV1.ANALOGUE_DIAGNOSTIC})


class OpsTerminalStatusV1(StrEnum):
    COMPLETE = "COMPLETE"
    NO_CANDIDATE = "NO_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    EXPIRED = "EXPIRED"


class OpsStageStatusV1(StrEnum):
    COMPLETE = "COMPLETE"
    NO_CANDIDATE = "NO_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    SKIPPED = "SKIPPED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class OpsSourceStateV1:
    source_id: str
    state: str
    observed_at_ns: int | None = None
    available_at_ns: int | None = None

    def __post_init__(self) -> None:
        nonblank(self.source_id, field="source_id")
        nonblank(self.state, field="source state")
        if self.observed_at_ns is not None:
            timestamp(self.observed_at_ns, field="source observed_at_ns")
        if self.available_at_ns is not None:
            timestamp(self.available_at_ns, field="source available_at_ns")
        if self.observed_at_ns is not None and self.available_at_ns is not None:
            if self.available_at_ns < self.observed_at_ns:
                raise ValueError("source health cannot be available before observation")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "state": self.state,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
        }


@dataclass(frozen=True)
class OpsRecoverySnapshotV1:
    """Recovery-first view supplied after durable watches/subscriptions are restored."""

    required_source_ids: tuple[str, ...]
    source_states: tuple[OpsSourceStateV1, ...]
    restored_watch_ids: tuple[str, ...]
    required_subscription_ref: str | None
    reconciled: bool
    available_at_ns: int

    def __post_init__(self) -> None:
        timestamp(self.available_at_ns, field="recovery available_at_ns")
        if type(self.reconciled) is not bool:
            raise ValueError("reconciled must be bool")
        required = tuple(sorted(set(self.required_source_ids)))
        watches = tuple(sorted(set(self.restored_watch_ids)))
        states = tuple(sorted(self.source_states, key=lambda item: item.source_id))
        if len({item.source_id for item in states}) != len(states):
            raise ValueError("recovery source states must have unique source IDs")
        if any(not item for item in required) or any(not item for item in watches):
            raise ValueError("recovery source/watch identifiers must be non-empty")
        if self.required_subscription_ref is not None:
            sha256_ref(self.required_subscription_ref, field="required_subscription_ref")
        object.__setattr__(self, "required_source_ids", required)
        object.__setattr__(self, "restored_watch_ids", watches)
        object.__setattr__(self, "source_states", states)

    def to_dict(self) -> dict[str, object]:
        return {
            "required_source_ids": list(self.required_source_ids),
            "source_states": [item.to_dict() for item in self.source_states],
            "restored_watch_ids": list(self.restored_watch_ids),
            "required_subscription_ref": self.required_subscription_ref,
            "reconciled": self.reconciled,
            "available_at_ns": self.available_at_ns,
        }


@dataclass(frozen=True)
class OpsDecisionEventV1:
    """One immutable causal trigger; all decision inputs are named by artifact ref."""

    event_id: str
    event_type: str
    source_id: str
    trigger_ref: str
    source_event_at_ns: int
    source_published_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    information_cutoff_ns: int
    deadline_ns: int
    causal_input_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("event_id", "trigger_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("event_type", "source_id"):
            nonblank(getattr(self, name), field=name)
        timestamp(self.source_event_at_ns, field="source_event_at_ns")
        if self.source_published_at_ns is not None:
            timestamp(self.source_published_at_ns, field="source_published_at_ns")
        for name in ("received_at_ns", "available_at_ns", "information_cutoff_ns", "deadline_ns"):
            timestamp(getattr(self, name), field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("event availability cannot precede actual receipt")
        if self.source_event_at_ns > self.information_cutoff_ns:
            raise ValueError("source event time cannot follow the decision information cutoff")
        if self.source_published_at_ns is not None and (
            self.source_published_at_ns > self.received_at_ns or self.available_at_ns < self.source_published_at_ns
        ):
            raise ValueError("source publication, receipt and availability chronology conflicts")
        if self.information_cutoff_ns < self.available_at_ns:
            raise ValueError("decision cutoff cannot precede trigger availability")
        if self.deadline_ns < self.information_cutoff_ns:
            raise ValueError("decision deadline cannot precede its information cutoff")
        refs = tuple(sorted(set(self.causal_input_refs) | {self.trigger_ref}))
        for ref in refs:
            sha256_ref(ref, field="causal_input_ref")
        object.__setattr__(self, "causal_input_refs", refs)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_id": self.source_id,
            "trigger_ref": self.trigger_ref,
            "source_event_at_ns": self.source_event_at_ns,
            "source_published_at_ns": self.source_published_at_ns,
            "received_at_ns": self.received_at_ns,
            "available_at_ns": self.available_at_ns,
            "information_cutoff_ns": self.information_cutoff_ns,
            "deadline_ns": self.deadline_ns,
            "causal_input_refs": list(self.causal_input_refs),
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class OpsCycleBatchV1:
    events: tuple[OpsDecisionEventV1, ...]
    source_states: tuple[OpsSourceStateV1, ...]
    required_source_ids: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    reconciled: bool
    collected_at_ns: int

    def __post_init__(self) -> None:
        timestamp(self.collected_at_ns, field="collected_at_ns")
        if type(self.reconciled) is not bool:
            raise ValueError("reconciled must be bool")
        events = tuple(self.events)
        if len({item.event_id for item in events}) != len(events):
            raise ValueError("one cycle cannot contain conflicting duplicate decision event IDs")
        if events != tuple(
            sorted(events, key=lambda item: (item.available_at_ns, item.information_cutoff_ns, item.event_id))
        ):
            raise ValueError("decision events must be sorted in causal availability order")
        states = tuple(sorted(self.source_states, key=lambda item: item.source_id))
        if len({item.source_id for item in states}) != len(states):
            raise ValueError("cycle source states must have unique source IDs")
        required = tuple(sorted(set(self.required_source_ids)))
        refs = tuple(sorted(set(self.evidence_refs)))
        for ref in refs:
            sha256_ref(ref, field="cycle evidence ref")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "source_states", states)
        object.__setattr__(self, "required_source_ids", required)
        object.__setattr__(self, "evidence_refs", refs)


@dataclass(frozen=True)
class OpsStageResultV1:
    stage: PipelineStageV1
    status: OpsStageStatusV1
    artifact_refs: tuple[str, ...]
    completed_at_ns: int
    reason: str | None = None
    bound_action_hash: str | None = None
    authority: str = "ZERO"

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", PipelineStageV1(self.stage))
        object.__setattr__(self, "status", OpsStageStatusV1(self.status))
        timestamp(self.completed_at_ns, field="stage completed_at_ns")
        refs = tuple(self.artifact_refs)
        if len(set(refs)) != len(refs):
            raise ValueError("stage artifact refs must be unique")
        if len(refs) > _MAX_REFS_PER_STAGE:
            raise ValueError("stage artifact refs exceed the bounded receipt limit")
        for ref in refs:
            sha256_ref(ref, field="stage artifact_ref")
        object.__setattr__(self, "artifact_refs", refs)
        if self.reason is not None:
            nonblank(self.reason, field="stage reason")
        if self.authority != "ZERO":
            raise ValueError("supervisor stages have zero selector, risk and admission authority")
        if self.bound_action_hash is not None:
            sha256_ref(self.bound_action_hash, field="bound_action_hash")
        if self.stage == PipelineStageV1.FROZEN_ACTION and self.status == OpsStageStatusV1.COMPLETE:
            if self.bound_action_hash is None or not self.artifact_refs:
                raise ValueError("a frozen action stage must bind its exact action and indexed artifact")
        if self.stage in _DIAGNOSTIC_STAGES and self.status != OpsStageStatusV1.SKIPPED:
            if self.bound_action_hash is None:
                raise ValueError("M1/analogue diagnostics must bind the same frozen action")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "stage": self.stage.value,
            "status": self.status.value,
            "artifact_refs": list(self.artifact_refs),
            "completed_at_ns": self.completed_at_ns,
            "reason": self.reason,
            "bound_action_hash": self.bound_action_hash,
            "authority": self.authority,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, body: Mapping[str, object]) -> OpsStageResultV1:
        data = cast(Mapping[str, Any], body)
        refs = data.get("artifact_refs", ())
        if not isinstance(refs, (list, tuple)):
            raise ValueError("stage artifact_refs must be an array")
        return cls(
            PipelineStageV1(str(data["stage"])),
            OpsStageStatusV1(str(data["status"])),
            tuple(str(ref) for ref in refs),
            int(cast(Any, data["completed_at_ns"])),
            str(data["reason"]) if data.get("reason") is not None else None,
            str(data["bound_action_hash"]) if data.get("bound_action_hash") is not None else None,
            str(data.get("authority", "ZERO")),
        )


@dataclass(frozen=True)
class OpsDecisionResultV1:
    stages: tuple[OpsStageResultV1, ...]
    terminal_status: OpsTerminalStatusV1
    missing_reason: str | None = None

    def __post_init__(self) -> None:
        stages = tuple(self.stages)
        if tuple(item.stage for item in stages) != PIPELINE_STAGE_ORDER:
            raise ValueError("decision result must report every fixed supervisor stage in order")
        object.__setattr__(self, "stages", stages)
        object.__setattr__(self, "terminal_status", OpsTerminalStatusV1(self.terminal_status))
        if self.missing_reason is not None:
            nonblank(self.missing_reason, field="missing_reason")
        action = next(item for item in stages if item.stage == PipelineStageV1.FROZEN_ACTION)
        for diagnostic in (PipelineStageV1.M1_DIAGNOSTIC, PipelineStageV1.ANALOGUE_DIAGNOSTIC):
            result = stages[PIPELINE_STAGE_ORDER.index(diagnostic)]
            if result.status != OpsStageStatusV1.SKIPPED and result.bound_action_hash != action.bound_action_hash:
                raise ValueError("diagnostic evidence changed or detached from the frozen action")
        sizing = stages[PIPELINE_STAGE_ORDER.index(PipelineStageV1.HARD_RISK)]
        evaluation = stages[PIPELINE_STAGE_ORDER.index(PipelineStageV1.ECONOMIC_EVALUATION)]
        if sizing.status != OpsStageStatusV1.COMPLETE and (
            action.status != OpsStageStatusV1.SKIPPED or evaluation.status != OpsStageStatusV1.SKIPPED
        ):
            raise ValueError("a candidate without hard-risk sizing cannot progress to action/evaluation")

    def to_dict(self) -> dict[str, object]:
        return {
            "stages": [item.to_dict() for item in self.stages],
            "terminal_status": self.terminal_status.value,
            "missing_reason": self.missing_reason,
        }


@dataclass(frozen=True)
class OpsSupervisorReceiptV1:
    runtime_version: str
    cycle_id: str
    event: OpsDecisionEventV1
    source_health_state: str
    source_states: tuple[OpsSourceStateV1, ...]
    result: OpsDecisionResultV1
    created_at_ns: int
    agent_mode: str = "DISABLED"
    capital_enabled: bool = False
    assisted_enabled: bool = False

    def __post_init__(self) -> None:
        nonblank(self.runtime_version, field="runtime_version")
        sha256_ref(self.cycle_id, field="cycle_id")
        nonblank(self.source_health_state, field="source_health_state")
        timestamp(self.created_at_ns, field="created_at_ns")
        if self.agent_mode != "DISABLED" or self.capital_enabled or self.assisted_enabled:
            raise ValueError("continuous ops receipts require disabled agents, capital and assisted execution")
        object.__setattr__(self, "source_states", tuple(sorted(self.source_states, key=lambda item: item.source_id)))

    @property
    def candidate_set_ref(self) -> str | None:
        return self._stage_artifact(PipelineStageV1.CANDIDATE_SET)

    @property
    def sizing_ref(self) -> str | None:
        return self._stage_artifact(PipelineStageV1.HARD_RISK)

    @property
    def action_ref(self) -> str | None:
        return self._stage_artifact(PipelineStageV1.FROZEN_ACTION)

    @property
    def evaluation_ref(self) -> str | None:
        return self._stage_artifact(PipelineStageV1.ECONOMIC_EVALUATION)

    @property
    def calendar_refs(self) -> tuple[str, ...]:
        stage = self.result.stages[PIPELINE_STAGE_ORDER.index(PipelineStageV1.DECISION_CALENDAR)]
        return stage.artifact_refs

    def _stage_artifact(self, stage_name: PipelineStageV1) -> str | None:
        stage = self.result.stages[PIPELINE_STAGE_ORDER.index(stage_name)]
        # The adapter puts the primary output first. Additional outputs remain
        # in stage_refs in their deterministic, declared order.
        return stage.artifact_refs[0] if stage.artifact_refs else None

    def to_dict(self) -> dict[str, object]:
        stages = {item.stage: item for item in self.result.stages}
        m1 = stages[PipelineStageV1.M1_DIAGNOSTIC]
        analogue = stages[PipelineStageV1.ANALOGUE_DIAGNOSTIC]
        return {
            "schema_version": 1,
            "runtime_version": self.runtime_version,
            "cycle_id": self.cycle_id,
            "decision_event": self.event.to_dict(),
            "source_health_state": self.source_health_state,
            "source_states": [item.to_dict() for item in self.source_states],
            "stage_refs": {item.stage.value: list(item.artifact_refs) for item in self.result.stages},
            "stage_terminal_statuses": {item.stage.value: item.status.value for item in self.result.stages},
            "candidate_set_existed": bool(stages[PipelineStageV1.CANDIDATE_SET].artifact_refs),
            "candidate_set_ref": self.candidate_set_ref,
            "sizing_occurred": bool(stages[PipelineStageV1.HARD_RISK].artifact_refs),
            "sizing_ref": self.sizing_ref,
            "exact_action_existed": stages[PipelineStageV1.FROZEN_ACTION].status == OpsStageStatusV1.COMPLETE,
            "action_ref": self.action_ref,
            "action_hash": stages[PipelineStageV1.FROZEN_ACTION].bound_action_hash,
            "economic_evaluation_occurred": bool(stages[PipelineStageV1.ECONOMIC_EVALUATION].artifact_refs),
            "evaluation_ref": self.evaluation_ref,
            "m1_diagnostic_status": m1.status.value,
            "m1_action_hash": m1.bound_action_hash,
            "analogue_diagnostic_status": analogue.status.value,
            "analogue_action_hash": analogue.bound_action_hash,
            "decision_calendar_refs": list(self.calendar_refs),
            "terminal_status": self.result.terminal_status.value,
            "missing_or_blocking_reason": self.result.missing_reason,
            "created_at_ns": self.created_at_ns,
            "agent_mode": self.agent_mode,
            "capital_enabled": self.capital_enabled,
            "assisted_enabled": self.assisted_enabled,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class OpsCycleReceiptV1:
    cycle_id: str
    started_at_ns: int
    source_health_state: str
    event_receipt_refs: tuple[str, ...]
    event_ids: tuple[str, ...]
    failure_types: tuple[str, ...]
    recovered: bool
    content_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "cycle_id": self.cycle_id,
            "started_at_ns": self.started_at_ns,
            "source_health_state": self.source_health_state,
            "event_receipt_refs": list(self.event_receipt_refs),
            "event_ids": list(self.event_ids),
            "failure_types": list(self.failure_types),
            "recovered": self.recovered,
            "content_hash": self.content_hash,
        }


class OpsCyclePortV1(Protocol):
    """Injected app composition; all methods receive the supervisor-owned DB."""

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1: ...

    def collect(
        self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1
    ) -> OpsCycleBatchV1: ...

    def process_event(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        *,
        now_ns: int,
        source_health_state: str,
        completed_stages: Mapping[PipelineStageV1, OpsStageResultV1],
        checkpoint: Callable[[OpsStageResultV1], None],
    ) -> OpsDecisionResultV1: ...


@dataclass(frozen=True)
class OpsRunResultV1:
    cycle: OpsCycleReceiptV1
    event_receipts: tuple[OpsSupervisorReceiptV1, ...]


class OpsSupervisorV2:
    """Own one writable repository and drive bounded recovery/collect/decision cycles."""

    def __init__(
        self,
        database_path: str | Path,
        port: OpsCyclePortV1,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        sleep_fn: Callable[[float], None] = time.sleep,
        max_events_per_cycle: int = 64,
        post_receipt_shadow: Callable[[OpsSupervisorReceiptV1, str, OpsRepository], None] | None = None,
        post_cycle_maintenance: Callable[[OpsRepository, int], object] | None = None,
    ) -> None:
        raw_path = str(database_path)
        if raw_path.startswith("file:") or "://" in raw_path or not raw_path:
            raise ValueError("atlas-ops requires a local SQLite path")
        if type(max_events_per_cycle) is not int or not 1 <= max_events_per_cycle <= 1024:
            raise ValueError("max_events_per_cycle must be between 1 and 1024")
        self.database_path = raw_path
        self.port = port
        self.clock_ns = clock_ns
        self.sleep_fn = sleep_fn
        self.max_events_per_cycle = max_events_per_cycle
        self.post_receipt_shadow = post_receipt_shadow
        if post_cycle_maintenance is None:
            from .outcome_maturity import run_outcome_maturity_cycle

            post_cycle_maintenance = run_outcome_maturity_cycle
        self.post_cycle_maintenance = post_cycle_maintenance
        self.repository: OpsRepository | None = None
        self.recovery: OpsRecoverySnapshotV1 | None = None
        self._closed = False

    def __enter__(self) -> OpsSupervisorV2:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _ensure_open(self) -> OpsRepository:
        if self._closed:
            raise RuntimeError("ops supervisor is closed")
        if self.repository is None:
            self.repository = OpsRepository(self.database_path)
        return self.repository

    def _recover_first(self, repository: OpsRepository, now_ns: int) -> bool:
        if self.recovery is not None:
            return True
        snapshot = self.port.recover(repository, now_ns=now_ns)
        if snapshot.available_at_ns > now_ns:
            raise ValueError("recovery result cannot be available in the future")
        if any(
            (state.observed_at_ns is not None and state.observed_at_ns > now_ns)
            or (state.available_at_ns is not None and state.available_at_ns > now_ns)
            for state in snapshot.source_states
        ):
            raise ValueError("recovery source health cannot be available in the future")
        self.recovery = snapshot
        return True

    def close(self) -> None:
        if self._closed:
            return
        try:
            close_port = getattr(self.port, "close", None)
            if callable(close_port):
                close_port()
        finally:
            if self.repository is not None:
                self.repository.close()
                self.repository = None
            self._closed = True

    @staticmethod
    def _health_state(
        recovery: OpsRecoverySnapshotV1,
        required_source_ids: tuple[str, ...],
        source_states: tuple[OpsSourceStateV1, ...],
        reconciled: bool,
    ) -> str:
        required = tuple(sorted(set(recovery.required_source_ids) | set(required_source_ids)))
        states = {item.source_id: item.state for item in (*recovery.source_states, *source_states)}
        if not required:
            return "UNKNOWN"
        if any(source_id not in states for source_id in required):
            return "UNKNOWN"
        if not reconciled:
            return "INCOMPLETE_SNAPSHOT"
        required_states = tuple(states[source_id] for source_id in required)
        if all(state == "HEALTHY_CURRENT" for state in required_states):
            return "HEALTHY_CURRENT"
        return next((state for state in required_states if state != "HEALTHY_CURRENT"), "UNKNOWN")

    @staticmethod
    def _persist_trigger(repository: OpsRepository, event: OpsDecisionEventV1) -> None:
        identity_ref = sha256_json({"artifact_type": "OpsDecisionEventIdentityV1", "event_id": event.event_id})
        repository.register_artifact(
            ArtifactIndexEntryV2(
                identity_ref,
                "OpsDecisionEventIdentityV1",
                event.content_hash,
                event.available_at_ns,
                event.available_at_ns,
                {"event": event.to_dict()},
            )
        )

    @staticmethod
    def _receipt_identity_ref(event_id: str) -> str:
        return sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": event_id})

    @staticmethod
    def _read_final_receipt(repository: OpsRepository, event_id: str) -> OpsSupervisorReceiptV1 | None:
        identity = repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(event_id))
        if identity is None:
            return None
        receipt_ref = identity.metadata.get("receipt_ref")
        receipt_entry = repository.get_artifact(str(receipt_ref)) if isinstance(receipt_ref, str) else None
        body = receipt_entry.metadata.get("receipt") if receipt_entry is not None else None
        if receipt_entry is None or not isinstance(body, Mapping):
            raise RuntimeError("durable ops receipt identity points to missing or invalid receipt")
        if sha256_json(body) != receipt_entry.content_hash:
            raise RuntimeError("durable ops receipt content hash mismatch")
        # The receipt is kept as a fully inspectable immutable artifact. The caller
        # receives a lightweight restored instance through its canonical body.
        return _receipt_from_dict(body)

    @staticmethod
    def _persist_final_receipt(repository: OpsRepository, receipt: OpsSupervisorReceiptV1) -> str:
        body = receipt.to_dict()
        receipt_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptV1", "content_hash": receipt.content_hash})
        receipt_entry = ArtifactIndexEntryV2(
            receipt_ref,
            "OpsSupervisorReceiptV1",
            receipt.content_hash,
            receipt.created_at_ns,
            receipt.created_at_ns,
            {"receipt": body},
        )
        identity_ref = OpsSupervisorV2._receipt_identity_ref(receipt.event.event_id)
        identity_body = {"event_id": receipt.event.event_id, "receipt_ref": receipt_ref}
        identity_entry = ArtifactIndexEntryV2(
            identity_ref,
            "OpsSupervisorReceiptIdentityV1",
            sha256_json(identity_body),
            receipt.created_at_ns,
            receipt.created_at_ns,
            identity_body,
        )
        repository.register_artifacts((receipt_entry, identity_entry))
        return receipt_ref

    @staticmethod
    def _checkpoint_ref(event_id: str, stage: PipelineStageV1) -> str:
        return sha256_json(
            {"artifact_type": "OpsSupervisorStageCheckpointV1", "event_id": event_id, "stage": stage.value}
        )

    def _load_checkpoints(
        self, repository: OpsRepository, event: OpsDecisionEventV1
    ) -> dict[PipelineStageV1, OpsStageResultV1]:
        completed: dict[PipelineStageV1, OpsStageResultV1] = {}
        for stage in PIPELINE_STAGE_ORDER:
            entry = repository.get_artifact(self._checkpoint_ref(event.event_id, stage))
            if entry is None:
                continue
            if (
                entry.artifact_type != "OpsSupervisorStageCheckpointV1"
                or entry.metadata.get("event_id") != event.event_id
            ):
                raise RuntimeError("ops stage checkpoint identity mismatch")
            body = entry.metadata.get("stage_result")
            if not isinstance(body, Mapping):
                raise RuntimeError("ops stage checkpoint is missing typed stage result")
            result = OpsStageResultV1.from_dict(body)
            if result.stage != stage or result.content_hash != entry.content_hash:
                raise RuntimeError("ops stage checkpoint content hash or order mismatch")
            completed[stage] = result
        if tuple(completed) != PIPELINE_STAGE_ORDER[: len(completed)]:
            raise RuntimeError("ops stage checkpoints are not in fixed pipeline order")
        return completed

    def _checkpoint_stage(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        result: OpsStageResultV1,
        completed: dict[PipelineStageV1, OpsStageResultV1],
        *,
        now_ns: int,
    ) -> None:
        prior = completed.get(result.stage)
        if prior is not None:
            if prior != result:
                raise ValueError("retry produced conflicting output for a completed immutable pipeline stage")
            return
        next_stage = next((stage for stage in PIPELINE_STAGE_ORDER if stage not in completed), None)
        if next_stage != result.stage:
            raise ValueError("pipeline stage checkpoint skipped or reordered a required V2 stage")
        if result.status == OpsStageStatusV1.FAILED:
            raise ValueError("failed stages are retryable and cannot be committed as complete checkpoints")
        if result.completed_at_ns < event.available_at_ns or result.completed_at_ns > now_ns:
            raise ValueError("stage completion time is outside the observed event/cycle interval")
        limit = event.information_cutoff_ns if result.stage in _CAUSAL_STAGES else event.deadline_ns
        for ref in result.artifact_refs:
            entry = repository.get_artifact(ref)
            if entry is None or entry.available_at_ns > min(limit, now_ns):
                raise ValueError("pipeline stage output is missing or unavailable by the observed causal time")
            if result.completed_at_ns < entry.available_at_ns:
                raise ValueError("pipeline stage completion cannot precede its output availability")
        if result.bound_action_hash is not None:
            action_stage = completed.get(PipelineStageV1.FROZEN_ACTION)
            if result.stage in _DIAGNOSTIC_STAGES and (
                action_stage is None or action_stage.bound_action_hash != result.bound_action_hash
            ):
                raise ValueError("diagnostic output is not bound to the already frozen exact action")
        ref = self._checkpoint_ref(event.event_id, result.stage)
        body = result.to_dict()
        repository.register_artifact(
            ArtifactIndexEntryV2(
                ref,
                "OpsSupervisorStageCheckpointV1",
                result.content_hash,
                result.completed_at_ns,
                result.completed_at_ns,
                {"event_id": event.event_id, "stage_result": body},
            )
        )
        completed[result.stage] = result

    @staticmethod
    def _validate_result_against_checkpoints(
        result: OpsDecisionResultV1, completed: Mapping[PipelineStageV1, OpsStageResultV1]
    ) -> None:
        if len(completed) != len(PIPELINE_STAGE_ORDER):
            raise ValueError("pipeline returned before all fixed stage outcomes were checkpointed")
        for stage_result in result.stages:
            if completed.get(stage_result.stage) != stage_result:
                raise ValueError("pipeline final result differs from its durable stage checkpoint")

    @staticmethod
    def _terminal_without_pipeline(
        event: OpsDecisionEventV1,
        now_ns: int,
        status: OpsTerminalStatusV1,
        reason: str,
    ) -> OpsDecisionResultV1:
        stage_results: list[OpsStageResultV1] = []
        for stage in PIPELINE_STAGE_ORDER:
            terminal_stage = stage == PipelineStageV1.DECISION_CALENDAR
            stage_results.append(
                OpsStageResultV1(
                    stage,
                    OpsStageStatusV1.NOT_ESTIMABLE if terminal_stage else OpsStageStatusV1.SKIPPED,
                    (),
                    now_ns,
                    reason if terminal_stage else "UPSTREAM_SUPERVISOR_GATE",
                )
            )
        return OpsDecisionResultV1(tuple(stage_results), status, reason)

    def _make_event_receipt(
        self,
        event: OpsDecisionEventV1,
        result: OpsDecisionResultV1,
        *,
        now_ns: int,
        source_health_state: str,
        source_states: tuple[OpsSourceStateV1, ...],
    ) -> OpsSupervisorReceiptV1:
        cycle_id = sha256_json(
            {
                "runtime_version": OPS_SUPERVISOR_VERSION,
                "decision_event_id": event.event_id,
                "information_cutoff_ns": event.information_cutoff_ns,
            }
        )
        return OpsSupervisorReceiptV1(
            OPS_SUPERVISOR_VERSION,
            cycle_id,
            event,
            source_health_state,
            source_states,
            result,
            now_ns,
        )

    def run_once(self) -> OpsRunResultV1:
        """Run one bounded recovery/collection/event cycle without sleeping."""
        started_at_ns = timestamp(self.clock_ns(), field="cycle start")
        repository = self._ensure_open()
        drain_shadow = getattr(self.post_receipt_shadow, "drain_completed", None)
        if callable(drain_shadow):
            try:
                # Safe cycle boundary: bounded nonblocking poll; critic I/O never runs here.
                drain_shadow(max_items=2, repository=repository)
            except Exception:
                # Shadow persistence/projection cannot downgrade deterministic runtime health.
                pass
        failures: list[str] = []
        receipts: list[OpsSupervisorReceiptV1] = []
        batch: OpsCycleBatchV1 | None = None

        try:
            self._recover_first(repository, started_at_ns)
        except Exception as error:
            failures.append(type(error).__name__)
        recovery = self.recovery
        if recovery is not None:
            try:
                batch = self.port.collect(repository, now_ns=started_at_ns, recovery=recovery)
                if batch.collected_at_ns > started_at_ns:
                    raise ValueError("cycle collection cannot complete in the future")
                if any(
                    (state.observed_at_ns is not None and state.observed_at_ns > started_at_ns)
                    or (state.available_at_ns is not None and state.available_at_ns > started_at_ns)
                    for state in batch.source_states
                ):
                    raise ValueError("cycle source health cannot be available in the future")
                if len(batch.events) > self.max_events_per_cycle:
                    raise ValueError("cycle event count exceeds configured processing bound")
                for ref in batch.evidence_refs:
                    entry = repository.get_artifact(ref)
                    if entry is None or entry.available_at_ns > started_at_ns:
                        raise ValueError("cycle evidence ref is missing or unavailable at collection time")
                if any(
                    event.available_at_ns > started_at_ns or event.information_cutoff_ns > started_at_ns
                    for event in batch.events
                ):
                    raise ValueError("cycle source returned an event whose availability/cutoff is in the future")
            except Exception as error:
                failures.append(type(error).__name__)
                batch = None

        required_sources = batch.required_source_ids if batch is not None else ()
        source_states = batch.source_states if batch is not None else ()
        reconciliation_complete = batch.reconciled if batch is not None else False
        source_health = (
            self._health_state(recovery, required_sources, source_states, reconciliation_complete)
            if recovery is not None
            else "UNKNOWN"
        )
        event_ids: list[str] = []
        receipt_refs: list[str] = []

        if batch is not None and recovery is not None:
            for event in batch.events:
                event_ids.append(event.event_id)
                try:
                    self._persist_trigger(repository, event)
                    prior_receipt = self._read_final_receipt(repository, event.event_id)
                    if prior_receipt is not None:
                        if prior_receipt.event.content_hash != event.content_hash:
                            raise ValueError("decision event identity was replayed with conflicting immutable content")
                        receipts.append(prior_receipt)
                        identity_entry = repository.get_artifact(self._receipt_identity_ref(event.event_id))
                        if identity_entry is None:
                            raise RuntimeError("durable receipt identity disappeared during replay")
                        prior_receipt_ref = str(identity_entry.metadata["receipt_ref"])
                        receipt_refs.append(prior_receipt_ref)
                        if self.post_receipt_shadow is not None:
                            try:
                                self.post_receipt_shadow(prior_receipt, prior_receipt_ref, repository)
                            except Exception:
                                # Shadow failure must not alter the accepted deterministic cycle/receipt.
                                pass
                        continue

                    event_inputs_available = all(
                        (entry := repository.get_artifact(ref)) is not None
                        and entry.available_at_ns <= event.information_cutoff_ns
                        for ref in event.causal_input_refs
                    )
                    if not event_inputs_available:
                        result = self._terminal_without_pipeline(
                            event,
                            started_at_ns,
                            OpsTerminalStatusV1.NOT_ESTIMABLE,
                            "MISSING_OR_FUTURE_CAUSAL_EVENT_EVIDENCE",
                        )
                    elif started_at_ns > event.deadline_ns:
                        result = self._terminal_without_pipeline(
                            event,
                            started_at_ns,
                            OpsTerminalStatusV1.EXPIRED,
                            "DECISION_DEADLINE_EXPIRED_BEFORE_RECOVERY_REPLAY",
                        )
                    elif source_health != "HEALTHY_CURRENT":
                        result = self._terminal_without_pipeline(
                            event,
                            started_at_ns,
                            OpsTerminalStatusV1.NOT_ESTIMABLE,
                            f"SOURCE_HEALTH_{source_health}",
                        )
                    else:
                        completed = self._load_checkpoints(repository, event)

                        def checkpoint(
                            stage_result: OpsStageResultV1,
                            current_event: OpsDecisionEventV1 = event,
                            current_completed: dict[PipelineStageV1, OpsStageResultV1] = completed,
                        ) -> None:
                            self._checkpoint_stage(
                                repository,
                                current_event,
                                stage_result,
                                current_completed,
                                now_ns=started_at_ns,
                            )

                        result = self.port.process_event(
                            repository,
                            event,
                            now_ns=started_at_ns,
                            source_health_state=source_health,
                            completed_stages=completed,
                            checkpoint=checkpoint,
                        )
                        for stage_result in result.stages:
                            checkpoint(stage_result)
                        self._validate_result_against_checkpoints(result, completed)

                    receipt = self._make_event_receipt(
                        event,
                        result,
                        now_ns=started_at_ns,
                        source_health_state=source_health,
                        source_states=source_states,
                    )
                    receipt_ref = self._persist_final_receipt(repository, receipt)
                    receipts.append(receipt)
                    receipt_refs.append(receipt_ref)
                    if self.post_receipt_shadow is not None:
                        try:
                            self.post_receipt_shadow(receipt, receipt_ref, repository)
                        except Exception:
                            # Shadow failure must not alter the accepted deterministic cycle/receipt.
                            pass
                except Exception as error:
                    # Exception text can contain arbitrary adapter/provider content;
                    # persist only the type and immutable event identity.
                    failure = {
                        "event_id": event.event_id,
                        "attempted_at_ns": started_at_ns,
                        "failure_type": type(error).__name__,
                        "checkpoint_refs": [
                            self._checkpoint_ref(event.event_id, stage).removeprefix("sha256:")
                            for stage in self._load_checkpoints(repository, event)
                        ],
                    }
                    failure_ref = sha256_json({"artifact_type": "OpsSupervisorAttemptFailureV1", "failure": failure})
                    repository.register_artifact(
                        ArtifactIndexEntryV2(
                            failure_ref,
                            "OpsSupervisorAttemptFailureV1",
                            sha256_json(failure),
                            started_at_ns,
                            started_at_ns,
                            failure,
                        )
                    )
                    failures.append(type(error).__name__)

        cycle_body = {
            "version": OPS_SUPERVISOR_VERSION,
            "started_at_ns": started_at_ns,
            "source_health_state": source_health,
            "event_ids": event_ids,
            "event_receipt_refs": receipt_refs,
            "failure_types": failures,
            "recovered": recovery is not None,
        }
        cycle_id = sha256_json({"artifact_type": "OpsSupervisorCycleReceiptV1", "cycle": cycle_body})
        cycle = OpsCycleReceiptV1(
            cycle_id,
            started_at_ns,
            source_health,
            tuple(receipt_refs),
            tuple(event_ids),
            tuple(failures),
            recovery is not None,
            cycle_id,
        )
        repository.register_artifact(
            ArtifactIndexEntryV2(
                cycle_id, "OpsSupervisorCycleReceiptV1", cycle_id, started_at_ns, started_at_ns, cycle.to_dict()
            )
        )
        if self.post_cycle_maintenance is not None:
            try:
                # Decision receipts are sealed before bounded downstream outcome work begins.
                maintenance_report = self.post_cycle_maintenance(repository, started_at_ns)
                from .outcome_maturity import OutcomeMaturityCycleReportV1

                if isinstance(maintenance_report, OutcomeMaturityCycleReportV1):
                    report_body = maintenance_report.to_dict()
                    report_ref = sha256_json(report_body)
                    available_at_ns = max(started_at_ns, timestamp(self.clock_ns(), field="maintenance report time"))
                    repository.register_artifact(
                        ArtifactIndexEntryV2(
                            report_ref,
                            "OutcomeMaturityCycleReportV1",
                            report_ref,
                            available_at_ns,
                            available_at_ns,
                            {"report": report_body},
                        )
                    )
            except Exception as error:
                failure_type = type(error).__name__
                if not failure_type.isascii() or not failure_type.isidentifier() or len(failure_type) > 64:
                    failure_type = "Exception"
                failure = {
                    "version": "OPS_OUTCOME_MATURITY_FAILURE_V1",
                    "attempted_at_ns": started_at_ns,
                    "failure_type": failure_type,
                    "reason_code": "OUTCOME_MATURITY_CYCLE_FAILED",
                }
                failure_ref = sha256_json(failure)
                try:
                    repository.register_artifact(ArtifactIndexEntryV2(
                        failure_ref,
                        "OpsOutcomeMaturityFailureV1",
                        failure_ref,
                        started_at_ns,
                        started_at_ns,
                        failure,
                    ))
                except Exception:
                    # Downstream failure persistence must not alter the sealed cycle receipt.
                    pass
        return OpsRunResultV1(cycle, tuple(receipts))

    def run_forever(
        self,
        *,
        stop_requested: Callable[[], bool] | None = None,
        interval_s: float = 1.0,
        max_cycles: int | None = None,
    ) -> Iterator[OpsRunResultV1]:
        """Foreground loop over ``run_once``; process supervision stays external."""
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        if max_cycles is not None and (type(max_cycles) is not int or max_cycles < 1):
            raise ValueError("max_cycles must be positive when supplied")
        stop = stop_requested or (lambda: False)
        completed_cycles = 0
        while not stop() and (max_cycles is None or completed_cycles < max_cycles):
            yield self.run_once()
            completed_cycles += 1
            if not stop() and (max_cycles is None or completed_cycles < max_cycles):
                self.sleep_fn(interval_s)


def _receipt_from_dict(body: Mapping[str, object]) -> OpsSupervisorReceiptV1:
    """Restore an immutable event receipt without querying provider/runtime state."""
    body = cast(Mapping[str, Any], body)
    event_body = body["decision_event"]
    if not isinstance(event_body, Mapping):
        raise RuntimeError("stored supervisor receipt event is invalid")
    event_body = cast(Mapping[str, Any], event_body)
    event = OpsDecisionEventV1(
        str(event_body["event_id"]),
        str(event_body["event_type"]),
        str(event_body["source_id"]),
        str(event_body["trigger_ref"]),
        int(event_body["source_event_at_ns"]),
        int(event_body["source_published_at_ns"]) if event_body.get("source_published_at_ns") is not None else None,
        int(event_body["received_at_ns"]),
        int(event_body["available_at_ns"]),
        int(event_body["information_cutoff_ns"]),
        int(event_body["deadline_ns"]),
        tuple(str(ref) for ref in event_body["causal_input_refs"]),  # type: ignore[union-attr]
    )
    statuses = body.get("stage_terminal_statuses")
    refs = body.get("stage_refs")
    if not isinstance(statuses, Mapping) or not isinstance(refs, Mapping):
        raise RuntimeError("stored supervisor receipt stages are invalid")
    stages: list[OpsStageResultV1] = []
    action_hash = body.get("action_hash")
    body = cast(Mapping[str, Any], body)
    for stage in PIPELINE_STAGE_ORDER:
        stage_refs = refs.get(stage.value, ())
        if not isinstance(stage_refs, (list, tuple)):
            raise RuntimeError("stored supervisor stage refs are invalid")
        diagnostic_hash = (
            str(body.get("m1_action_hash" if stage == PipelineStageV1.M1_DIAGNOSTIC else "analogue_action_hash"))
            if stage in _DIAGNOSTIC_STAGES
            and body.get("m1_action_hash" if stage == PipelineStageV1.M1_DIAGNOSTIC else "analogue_action_hash")
            is not None
            else None
        )
        stages.append(
            OpsStageResultV1(
                stage,
                OpsStageStatusV1(str(statuses[stage.value])),
                tuple(str(ref) for ref in stage_refs),
                int(cast(Any, body["created_at_ns"])),
                str(body["missing_or_blocking_reason"]) if body.get("missing_or_blocking_reason") else None,
                str(action_hash)
                if stage == PipelineStageV1.FROZEN_ACTION and action_hash is not None
                else diagnostic_hash,
            )
        )
    result = OpsDecisionResultV1(
        tuple(stages),
        OpsTerminalStatusV1(str(body["terminal_status"])),
        str(body["missing_or_blocking_reason"]) if body.get("missing_or_blocking_reason") else None,
    )
    source_state_rows = body.get("source_states", ())
    if not isinstance(source_state_rows, (list, tuple)):
        raise RuntimeError("stored supervisor source states are invalid")
    source_states = tuple(
        OpsSourceStateV1(
            str(item["source_id"]),
            str(item["state"]),
            int(item["observed_at_ns"]) if item.get("observed_at_ns") is not None else None,
            int(item["available_at_ns"]) if item.get("available_at_ns") is not None else None,
        )
        for item in cast(Sequence[Mapping[str, Any]], source_state_rows)
    )
    return OpsSupervisorReceiptV1(
        str(body["runtime_version"]),
        str(body["cycle_id"]),
        event,
        str(body["source_health_state"]),
        source_states,
        result,
        int(cast(Any, body["created_at_ns"])),
        str(body["agent_mode"]),
        bool(body["capital_enabled"]),
        bool(body["assisted_enabled"]),
    )


def _import_port(factory_path: str) -> OpsCyclePortV1:
    module_name, separator, attribute = factory_path.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("adapter must use the explicit module:factory form")
    factory = getattr(importlib.import_module(module_name), attribute)
    port = factory()
    if not all(callable(getattr(port, name, None)) for name in ("recover", "collect", "process_event")):
        raise TypeError("ops adapter factory must return recover/collect/process_event ports")
    return port


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ATLAS V2 non-capital public research supervisor")
    parser.add_argument("--db", required=True, help="local ops.sqlite path owned by this process")
    parser.add_argument(
        "--adapter",
        default="atlas.v2.runtime.production:create_production_port",
        help=("optional explicit composition override in module:factory form; "
              "the built-in credential-free ATLAS production composition is the default"),
    )
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true", help="run one bounded cycle and exit")
    parser.add_argument("--action-critic-shadow", action="store_true",
        help="opt in to the post-receipt hidden zero-authority action critic")
    parser.add_argument("--broker-socket", default=os.environ.get("ATLAS_AGENT_BROKER_SOCKET"),
        help="existing fixed local inference-broker Unix socket")
    args = parser.parse_args(argv)
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    port = _import_port(args.adapter)
    shadow = None
    if args.action_critic_shadow:
        from .action_critic_shadow import _LazyActionAssessmentShadow

        shadow = _LazyActionAssessmentShadow(args.db, args.broker_socket)
    supervisor = OpsSupervisorV2(Path(args.db), port, post_receipt_shadow=shadow)
    try:
        if args.once:
            result = supervisor.run_once()
            print(canonical_json(result.cycle.to_dict()))
            return 1 if result.cycle.failure_types else 0
        stop = Event()
        try:
            for result in supervisor.run_forever(stop_requested=stop.is_set, interval_s=args.interval_seconds):
                print(canonical_json(result.cycle.to_dict()), flush=True)
        except KeyboardInterrupt:
            stop.set()
        return 0
    finally:
        supervisor.close()
        if shadow is not None:
            shadow.close()


if __name__ == "__main__":  # pragma: no cover - console entry point
    sys.exit(main())
