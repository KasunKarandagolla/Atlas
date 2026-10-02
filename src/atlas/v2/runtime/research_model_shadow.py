"""Postreceipt fixed statistical diagnostics with exact causal source references.

This lightweight deterministic provider runs on the accepted controller writer.
It has no action, risk, admission, execution or capital authority.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.history import ArchiveScanBoundExceededV2, IndexedCausalBarV2, reconstruct_causal_bars_from_archive
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.baseline import BaselineInputsV2, CausalCloseV2
from atlas.v2.models.research_routing import (
    ResearchModelRouterV1,
    StatisticalResearchProviderV1,
    statistical_research_request_v1,
    statistical_research_route_v1,
)
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1
from atlas.v2.science.outcomes import DecisionCalendarEntryV2, DecisionSourceStageV2, index_decision_calendar_entry

MAX_STATISTICAL_CONTEXT_V1 = 128


class _Unavailable(ValueError):
    pass


class StatisticalResearchShadowV1:
    """Seal one diagnostic or explicit missing case for every observed receipt."""

    def __init__(self, *, run_id: str, config_hash: str, source_sha: str,
                 environment_lock_hash: str, archive_root: str | Path,
                 clock_ns: Callable[[], int] = time.time_ns,
                 bar_reader: Callable[..., tuple[IndexedCausalBarV2, ...]] = reconstruct_causal_bars_from_archive):
        self.run_id, self.config_hash = run_id, config_hash
        self.archive_root, self.clock_ns, self.bar_reader = Path(archive_root), clock_ns, bar_reader
        self.route, self.manifest = statistical_research_route_v1(
            source_sha=source_sha, environment_lock_hash=environment_lock_hash)
        self._router: ResearchModelRouterV1 | None = None
        self._repository: OpsRepository | None = None

    def __call__(self, receipt: OpsSupervisorReceiptV1, receipt_ref: str, repository: OpsRepository) -> None:
        if self._repository is not None and repository is not self._repository:
            raise ValueError("statistical research shadow cannot switch operational writers")
        if self._router is None:
            self._repository = repository
            self._router = ResearchModelRouterV1(repository, run_id=self.run_id, config_hash=self.config_hash,
                routes=(self.route,), manifests={self.manifest.manifest_hash: self.manifest},
                providers={self.route.provider_key: StatisticalResearchProviderV1(clock_ns=self.clock_ns)},
                clock_ns=self.clock_ns)
        identity = sha256_json({"version": "ResearchModelShadowReceiptIdentityV1", "run_id": self.run_id,
                                "receipt_ref": receipt_ref, "route_ref": self.route.content_hash})
        prior = repository.get_artifact(identity)
        if prior is not None:
            binding = prior.metadata.get("routing")
            prior_diagnostic = repository.get_artifact(binding.get("diagnostic_ref", "")) if isinstance(binding, Mapping) else None
            if (prior.artifact_type != "ResearchModelShadowReceiptIdentityV1"
                    or not isinstance(binding, Mapping) or sha256_json(binding) != prior.content_hash
                    or prior_diagnostic is None or prior_diagnostic.artifact_type != "ResearchModelShadowDiagnosticV1"
                    or sha256_json(prior_diagnostic.metadata.get("routing")) != prior_diagnostic.content_hash
                    or prior_diagnostic.artifact_ref != prior_diagnostic.content_hash
                    or prior_diagnostic.metadata["routing"].get("run_id") != self.run_id
                    or prior_diagnostic.metadata["routing"].get("config_hash") != self.config_hash
                    or prior_diagnostic.metadata["routing"].get("receipt_ref") != receipt_ref
                    or prior_diagnostic.metadata["routing"].get("route_ref") != self.route.content_hash):
                raise ValueError("statistical shadow durable completion identity is invalid")
            return
        now = self.clock_ns()
        decision_ref = None
        snapshot_ref = None
        result = None
        reason = None
        key = None
        try:
            indexed_receipt = repository.get_artifact(receipt_ref)
            if (indexed_receipt is None or indexed_receipt.artifact_type != "OpsSupervisorReceiptV1"
                    or receipt_ref != receipt.content_hash or indexed_receipt.content_hash != receipt_ref
                    or canonical_json(indexed_receipt.metadata.get("receipt")) != canonical_json(receipt.to_dict())
                    or indexed_receipt.available_at_ns > now):
                raise _Unavailable("MODEL_ORIGINATING_RECEIPT_INVALID")
            if receipt.capital_enabled or receipt.assisted_enabled or receipt.agent_mode != "DISABLED":
                raise _Unavailable("MODEL_RECEIPT_AUTHORITY_INVALID")
            event_entry = repository.get_artifact(receipt.event.content_hash)
            if (event_entry is None or event_entry.artifact_type != "OpsDecisionEventSourceV1"
                    or canonical_json(event_entry.metadata.get("event")) != canonical_json(receipt.event.to_dict())
                    or event_entry.content_hash != receipt.event.content_hash or event_entry.available_at_ns > now):
                raise _Unavailable("MODEL_EXACT_EVENT_UNAVAILABLE")
            key = self._instrument(repository, receipt)
            for ref in sorted(receipt.calendar_refs):
                entry = repository.get_artifact(ref)
                if entry is None or entry.artifact_type != "DecisionCalendarEntryV2":
                    continue
                calendar = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
                if (calendar.candidate_set_ref == receipt.candidate_set_ref
                        and ((receipt.action_ref is None and calendar.source_stage == DecisionSourceStageV2.CANDIDATE_SET)
                             or (receipt.action_ref is not None and calendar.action_artifact_ref == receipt.action_ref))):
                    if index_decision_calendar_entry(repository, calendar) != ref or calendar.available_at_ns > now:
                        raise _Unavailable("MODEL_DECISION_CALENDAR_INVALID")
                    decision_ref = ref
                    break
            if decision_ref is None:
                raise _Unavailable("MODEL_DECISION_CALENDAR_UNAVAILABLE")
            if now >= receipt.event.deadline_ns:
                raise _Unavailable("MODEL_ORIGINAL_DEADLINE_EXPIRED")
            bars = self.bar_reader(repository, self.archive_root, key=key, interval=BarIntervalV2.M15,
                information_cutoff_ns=receipt.event.information_cutoff_ns,
                availability_class=AvailabilityClassV2.ACTUAL_SYSTEM, limit=MAX_STATISTICAL_CONTEXT_V1)
            if len(bars) > MAX_STATISTICAL_CONTEXT_V1:
                raise _Unavailable("MODEL_CAUSAL_CONTEXT_BOUND_EXCEEDED")
            if not bars:
                raise _Unavailable("MODEL_CAUSAL_M15_CONTEXT_UNAVAILABLE")
            latest_expected_close = receipt.event.information_cutoff_ns // 900_000_000_000 * 900_000_000_000
            if max(item.bar.close_at_ns for item in bars) != latest_expected_close:
                raise _Unavailable("MODEL_LATEST_CAUSAL_M15_CLOSE_UNAVAILABLE")
            for item in bars:
                entry = repository.get_artifact(item.observation_index_ref)
                if (entry is None or entry.artifact_type != "PublicObservationIndexV2"
                        or entry.content_hash != item.bar.raw.content_hash
                        or entry.available_at_ns != item.bar.raw.available_at_ns
                        or item.bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                        or item.bar.interval != BarIntervalV2.M15 or not item.bar.final
                        or item.bar.instrument_revision != key.contract_revision
                        or entry.metadata.get("instrument_key_json") != key.to_canonical_json()
                        or entry.metadata.get("bar_content_hash") != item.bar.content_hash):
                    raise _Unavailable("MODEL_CAUSAL_SOURCE_IDENTITY_INVALID")
            inputs = BaselineInputsV2(key, receipt.event.information_cutoff_ns,
                tuple(CausalCloseV2(item.bar.close_at_ns, item.bar.raw.available_at_ns, item.bar.close) for item in bars))
            # The source facts were cutoff-visible. Their derived packet is
            # published now, and is never relabeled as available at that cutoff.
            published = self.clock_ns()
            if published < now:
                raise _Unavailable("MODEL_CLOCK_REGRESSION")
            snapshot_ref = inputs.content_hash
            snapshot = repository.get_artifact(snapshot_ref)
            if snapshot is None:
                repository.register_artifact(ArtifactIndexEntryV2(snapshot_ref, "BaselineInputsV2", snapshot_ref,
                    published, published, {"baseline_inputs": inputs.to_dict(), "authority": "ZERO"}))
            elif (snapshot.artifact_type != "BaselineInputsV2" or snapshot.content_hash != snapshot_ref
                    or canonical_json(snapshot.metadata.get("baseline_inputs")) != canonical_json(inputs.to_dict())
                    or snapshot.available_at_ns > published):
                raise _Unavailable("MODEL_DERIVED_INPUT_SNAPSHOT_CONFLICT")
            request = statistical_research_request_v1(inputs, self.manifest,
                input_artifact_refs=tuple(sorted({item.observation_index_ref for item in bars})),
                action_artifact_ref=receipt.action_ref, decision_calendar_ref=decision_ref,
                deadline_ns=receipt.event.deadline_ns)
            result = self._router.execute(self.route.route_id, request, inputs.to_dict(),
                action_artifact_ref=receipt.action_ref,
                decision_calendar_ref=decision_ref, decision_event_ref=receipt.event.content_hash,
                derived_input_ref=snapshot_ref)
        except _Unavailable as exc:
            reason = str(exc)
        except ArchiveScanBoundExceededV2:
            reason = "MODEL_CAUSAL_ARCHIVE_SCAN_BOUND_EXCEEDED"
        except (ValueError, TypeError, KeyError, OSError, ArithmeticError):
            reason = "MODEL_CAUSAL_DIAGNOSTIC_EVIDENCE_INVALID"
        completed = self.clock_ns()
        if completed < now:
            raise ValueError("statistical research clock regressed before persistence")
        diagnostic = {"version": "ResearchModelShadowDiagnosticV1", "run_id": self.run_id,
            "config_hash": self.config_hash, "receipt_ref": receipt_ref, "route_ref": self.route.content_hash,
            "model_profile_hash": self.manifest.manifest_hash, "decision_calendar_ref": decision_ref,
            "action_artifact_ref": receipt.action_ref,
            "decision_event_ref": receipt.event.content_hash, "input_snapshot_ref": snapshot_ref,
            "instrument_key": key.to_dict() if key is not None else None,
            "information_cutoff_ns": receipt.event.information_cutoff_ns,
            "original_deadline_ns": receipt.event.deadline_ns, "available_at_ns": completed,
            "status": "IMPLEMENTED" if result is not None and result.run.usable else "NOT ESTIMABLE",
            "reason_code": reason if reason is not None else result.run.failure_code if result is not None else None,
            "request_ref": result.request_ref if result is not None else None,
            "terminal_ref": result.terminal_ref if result is not None else None,
            "authority": "ZERO"}
        diagnostic_ref = sha256_json(diagnostic)
        binding = {"version": "ResearchModelShadowReceiptIdentityV1", "diagnostic_ref": diagnostic_ref,
                   "authority": "ZERO"}
        repository.register_artifacts((
            ArtifactIndexEntryV2(diagnostic_ref, "ResearchModelShadowDiagnosticV1", diagnostic_ref,
                completed, completed, {"routing": diagnostic}),
            ArtifactIndexEntryV2(identity, "ResearchModelShadowReceiptIdentityV1", sha256_json(binding),
                completed, completed, {"routing": binding})))

    @staticmethod
    def _instrument(repository: OpsRepository, receipt: OpsSupervisorReceiptV1) -> InstrumentKeyV2:
        trigger = repository.get_artifact(receipt.event.trigger_ref)
        body = trigger.metadata.get("trigger") if trigger is not None else None
        if (trigger is None or trigger.artifact_type not in {"OpsPublicFinalBarTriggerV1", "OpsPublicNativeM1BarTriggerV1"}
                or not isinstance(body, Mapping) or sha256_json(body) != trigger.content_hash
                or trigger.content_hash != receipt.event.trigger_ref
                or trigger.available_at_ns > receipt.event.information_cutoff_ns):
            raise _Unavailable("MODEL_EXACT_PUBLIC_TRIGGER_UNAVAILABLE")
        product = repository.get_artifact(str(body.get("product_ref", "")))
        if (product is None or product.artifact_type != "ProductContractV2"
                or product.available_at_ns > receipt.event.information_cutoff_ns):
            raise _Unavailable("MODEL_EXACT_PRODUCT_UNAVAILABLE")
        raw = product.metadata.get("product_contract", product.metadata.get("product"))
        if isinstance(raw, str):
            raw = json.loads(raw)
        contract = ProductContractV2.from_dict(json_value(raw))
        if contract.content_hash != product.content_hash or contract.content_hash != body.get("product_ref"):
            raise _Unavailable("MODEL_EXACT_PRODUCT_IDENTITY_INVALID")
        return contract.key
