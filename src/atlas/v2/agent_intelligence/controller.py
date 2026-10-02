"""Deterministic job controller for quarantined Discovery Lab proposals only."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from atlas.v2._serialization import sha256_json
from atlas.v2.agent_intelligence.broker import (
    ActionAssessmentBrokerCapabilityV1,
    BrokerCapabilityV1,
    BrokerCapabilityV2,
)
from atlas.v2.agent_intelligence.contracts import (
    ActionAssessmentDispatchAuthorizationV1,
    ActionAssessmentProviderProfileV1,
    ActionAssessmentRequestV2,
    ActionAssessmentResultV2,
    ActionAssessmentValidationReceiptV1,
    AgentJobStateV1,
    AgentModelProfile,
    AgentModelProfileV1,
    AgentModelProfileV2,
    AgentValidationReceiptV1,
    BrokerDispatchAuthorizationV1,
    BrokerDispatchAuthorizationV2,
    ProviderResultV1,
    ResearchProposalProvider,
    ResearchProposalRequestV1,
    ResearchProposalV1,
    RevisionStatusV1,
    SealedActionAssessmentPacketV1,
)
from atlas.v2.agent_intelligence.evidence import BoundedResearchReadService, collect_job_evidence
from atlas.v2.agent_intelligence.persistence import (
    AGENT_NAMESPACE_WRITER_OWNER,
    ActionAssessmentRepository,
    AgentJobRepository,
)
from atlas.v2.agent_intelligence.provider import ResearchProviderUnavailable
from atlas.v2.agent_intelligence.validation import (
    validate_action_assessment_output,
    validate_proposal_output,
    validate_request_evidence_availability,
)
from atlas.v2.agent_intelligence.worker import AgentWorkerSupervisor, WorkerSandboxUnavailable
from atlas.v2.runtime.action_critic_dispatcher import (
    ActionAssessmentDispatchCompletionV1,
    ActionAssessmentDispatchIdentityV1,
    ActionAssessmentDispatchWorkV1,
)

LEASE_NS = 180_000_000_000
MAX_RETRYABLE_PROVIDER_ATTEMPTS = 3


@dataclass(frozen=True)
class ResearchJobOutcomeV1:
    job_id: str
    request_key: str
    lifecycle_state: AgentJobStateV1
    proposal: ResearchProposalV1 | None
    validation_receipt: AgentValidationReceiptV1 | None
    terminal_reason: str | None
    authoritative: bool


@dataclass(frozen=True)
class _DispatchContext:
    capability: str
    job_id: str
    attempt_id: str
    lease_epoch: int
    call_index: int


class WorkerBrokerResearchProvider:
    """ATLAS port adapter that invokes one sandboxed worker through the fixed broker socket."""

    def __init__(self, supervisor: AgentWorkerSupervisor, context: _DispatchContext, deadline_ns: int) -> None:
        self._supervisor = supervisor
        self._context = context
        self._deadline_ns = deadline_ns

    def propose(self, request: ResearchProposalRequestV1,
                evidence: Sequence[Mapping[str, Any]]) -> ProviderResultV1:
        context = self._context
        return self._supervisor.infer(capability=context.capability, job_id=context.job_id,
            attempt_id=context.attempt_id, lease_epoch=context.lease_epoch, call_index=context.call_index,
            request=request, evidence=evidence, deadline_ns=self._deadline_ns)


ProviderFactory = Callable[[_DispatchContext, int], ResearchProposalProvider]


class ResearchJobController:
    """Owns identity, deadlines, evidence scope, retries, persistence and validation."""

    def __init__(self, *, jobs: AgentJobRepository, evidence: BoundedResearchReadService,
                 profile: AgentModelProfile, capability_signing_key: bytes,
                 provider_factory: ProviderFactory, now_ns: Callable[[], int] = time.time_ns) -> None:
        if len(capability_signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        self._jobs = jobs
        self._evidence = evidence
        self._profile = profile
        self._signing_key = bytes(capability_signing_key)
        self._provider_factory = provider_factory
        self._now_ns = now_ns

    def run(self, request: ResearchProposalRequestV1, *, job_id: str | None = None) -> ResearchJobOutcomeV1:
        now = self._now_ns()
        validate_request_evidence_availability(request)
        if (request.model_profile_hash != self._profile.content_hash
                or request.prompt_contract_hash != self._profile.prompt_contract_hash
                or request.schema_hash != self._profile.schema_hash
                or request.tool_contract_hash != self._profile.tool_contract_hash):
            raise ValueError("offline agent job contract hashes do not match its immutable profile")
        self._evidence.authorize_request_manifest(request)
        job_id = job_id or str(uuid.uuid4())
        job = self._jobs.create_request(request, job_id=job_id, now_ns=now)
        if job.lifecycle_state in _TERMINAL:
            if job.lifecycle_state == AgentJobStateV1.VALIDATED:
                accepted = self._jobs.authoritative_result(request.request_key)
                if accepted is None:
                    raise RuntimeError("validated agent job is missing its authoritative result receipt")
                accepted_proposal, receipt = accepted
                return ResearchJobOutcomeV1(job.job_id, job.request_key, job.lifecycle_state,
                    accepted_proposal, receipt, None, True)
            return ResearchJobOutcomeV1(job.job_id, job.request_key, job.lifecycle_state, None, None,
                                        "REQUEST_ALREADY_TERMINAL", False)
        try:
            job = self._jobs.lease(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER, now_ns=now, lease_ns=LEASE_NS)
            job = self._jobs.start(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER,
                                   epoch=job.lease_epoch, now_ns=now)
        except ValueError as exc:
            return ResearchJobOutcomeV1(job.job_id, job.request_key, self._jobs.get_job(job.job_id).lifecycle_state,
                                        None, None, _safe_reason(str(exc)), False)
        evidence = collect_job_evidence(request, self._evidence, now_ns=now)
        for authorization, result in zip(request.evidence_manifest, evidence, strict=True):
            self._jobs.record_tool_call(request.request_key, authorization, status=result["status"],
                                        response=result, called_at_ns=now)
        available_refs: set[str] = set()
        for auth, result in zip(request.evidence_manifest, evidence, strict=True):
            if result["status"] == "PRESENT":
                available_refs.add(auth.artifact_ref)
                for row in result.get("rows", []):
                    if isinstance(row, Mapping) and isinstance(row.get("artifact_ref"), str):
                        available_refs.add(row["artifact_ref"])
        last_reason = "PROVIDER_UNAVAILABLE"
        for _ in range(min(request.max_model_calls, MAX_RETRYABLE_PROVIDER_ATTEMPTS)):
            now = self._now_ns()
            if now >= request.absolute_deadline_ns:
                self._jobs.transition_terminal(job.job_id, AgentJobStateV1.EXPIRED,
                                               now_ns=now, reason="absolute_deadline_elapsed")
                return self._outcome(job.job_id, request.request_key, None, None, "absolute_deadline_elapsed", False)
            try:
                attempt = self._jobs.reserve_attempt(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER,
                    epoch=job.lease_epoch, now_ns=now)
            except ValueError as exc:
                last_reason = _safe_reason(str(exc))
                break
            cap_expiry = min(now + 120_000_000_000, job.lease_expires_at_ns or now,
                             request.absolute_deadline_ns)
            dispatch_authorization = self._jobs.authorize_broker_dispatch(job.job_id, request, attempt,
                owner=AGENT_NAMESPACE_WRITER_OWNER, lease_epoch=job.lease_epoch,
                authorization_id=str(uuid.uuid4()), capability_nonce=str(uuid.uuid4()),
                evidence_hash=sha256_json(evidence), model_profile_hash=self._profile.content_hash,
                expires_at_ns=cap_expiry, authorized_at_ns=now)
            if isinstance(self._profile, AgentModelProfileV2):
                if not isinstance(dispatch_authorization, BrokerDispatchAuthorizationV2):
                    raise RuntimeError("DeepSeek profile received a non-V2 dispatch authorization")
                capability = BrokerCapabilityV2.issue_authorized(authorization=dispatch_authorization,
                    signing_key=self._signing_key).capability
            else:
                if not isinstance(dispatch_authorization, BrokerDispatchAuthorizationV1):
                    raise RuntimeError("OpenAI V1 profile received a non-V1 dispatch authorization")
                capability = BrokerCapabilityV1.issue_authorized(authorization=dispatch_authorization,
                    signing_key=self._signing_key).capability
            context = _DispatchContext(capability, job.job_id, attempt.attempt_id, job.lease_epoch,
                                       attempt.attempt_index)
            try:
                provider = self._provider_factory(context, request.absolute_deadline_ns)
                provider_result = provider.propose(request, evidence)
            except (ResearchProviderUnavailable, WorkerSandboxUnavailable) as exc:
                provider_result = ProviderResultV1("", None, None, False, False, 0, 0, None,
                    _safe_reason(getattr(exc, "code", "BROKER_UNAVAILABLE")),
                    bool(getattr(exc, "retryable", isinstance(exc, WorkerSandboxUnavailable))))
            except Exception:
                provider_result = ProviderResultV1("", None, None, False, False, 0, 0, None,
                    "PROVIDER_ERROR", False)
            revision_status = _revision_status(self._profile, provider_result.returned_model_id,
                                               provider_result.model_revision)
            persisted_revision = provider_result.model_revision if isinstance(self._profile, AgentModelProfileV1) \
                else None
            response_profile = replace(self._profile, returned_model_id=provider_result.returned_model_id,
                model_revision=persisted_revision, revision_status=RevisionStatusV1(revision_status))
            self._jobs.register_model_profile(response_profile, created_at_ns=self._now_ns())
            profile_metadata = {"provider": response_profile.provider,
                "requested_model_id": response_profile.requested_model_id,
                "returned_model_id": response_profile.returned_model_id,
                "model_revision": response_profile.model_revision,
                "revision_status": response_profile.revision_status.value,
                "model_profile_hash": response_profile.content_hash,
                "model_profile": response_profile.to_dict(),
                "reasoning_settings": response_profile.reasoning_settings.to_dict(),
                "max_input_tokens": response_profile.max_input_tokens,
                "max_output_tokens": response_profile.max_output_tokens,
                "prompt_contract_hash": response_profile.prompt_contract_hash,
                "schema_hash": response_profile.schema_hash, "tool_contract_hash": response_profile.tool_contract_hash,
                "runtime_dependency_hash": response_profile.runtime_dependency_hash,
                "pricing_schedule_id": response_profile.pricing_schedule_id,
                "input_tokens": provider_result.input_tokens, "output_tokens": provider_result.output_tokens,
                "provider_request_id": provider_result.provider_request_id,
                "failure_code": provider_result.failure_code, "refusal": provider_result.refusal,
                "truncated": provider_result.truncated}
            if isinstance(self._profile, AgentModelProfileV2):
                profile_metadata["provider_reported_revision"] = provider_result.model_revision
            self._jobs.append_attempt_outcome(attempt.attempt_id,
                {"version": "AgentAttemptOutcomeV1", "status": "RETURNED" if provider_result.failure_code is None
                 else "FAILED", "provider_metadata": profile_metadata}, at_ns=self._now_ns())
            if provider_result.failure_code is not None:
                last_reason = provider_result.failure_code
                if provider_result.retryable and attempt.attempt_index < request.max_model_calls:
                    continue
                state = AgentJobStateV1.UNAVAILABLE if provider_result.refusal or provider_result.retryable \
                    or provider_result.failure_code in {"PROVIDER_ERROR", "RATE_LIMITED", "PROVIDER_TIMEOUT",
                    "PROVIDER_UNAVAILABLE", "AGENT_DEPENDENCY_UNAVAILABLE", "BROKER_UNAVAILABLE"} \
                    else AgentJobStateV1.INVALID
                self._jobs.transition_terminal(job.job_id, state, now_ns=self._now_ns(), reason=last_reason)
                return self._outcome(job.job_id, request.request_key, None, None, last_reason, False)
            if provider_result.refusal:
                self._jobs.transition_terminal(job.job_id, AgentJobStateV1.UNAVAILABLE,
                                               now_ns=self._now_ns(), reason="PROVIDER_REFUSAL")
                return self._outcome(job.job_id, request.request_key, None, None, "PROVIDER_REFUSAL", False)
            if provider_result.truncated:
                provider_result = ProviderResultV1(provider_result.raw_output, provider_result.returned_model_id,
                    provider_result.model_revision, False, True, provider_result.input_tokens,
                    provider_result.output_tokens, provider_result.provider_request_id, "TRUNCATED", False)
            model_ok = (provider_result.returned_model_id == self._profile.requested_model_id
                or (provider_result.returned_model_id is not None
                    and isinstance(self._profile, AgentModelProfileV1)
                    and provider_result.returned_model_id.startswith(self._profile.requested_model_id + "-")
                    and provider_result.model_revision == provider_result.returned_model_id))
            if isinstance(self._profile, AgentModelProfileV2) and provider_result.model_revision is not None:
                model_ok = False
            token_ok = (provider_result.input_tokens <= request.max_input_tokens
                        and provider_result.output_tokens <= request.max_output_tokens)
            result_body = {"version": "AgentProviderResultV1", "raw_output": provider_result.raw_output,
                "proposal_hash": sha256_json({"raw_untrusted_output": provider_result.raw_output}),
                "provider_metadata": profile_metadata}
            result_id, eligible, result_hash = self._jobs.submit_result(job.job_id, attempt.attempt_id,
                owner=AGENT_NAMESPACE_WRITER_OWNER, epoch=job.lease_epoch, result=result_body,
                received_at_ns=self._now_ns())
            if not eligible:
                self._jobs.transition_terminal(job.job_id, AgentJobStateV1.EXPIRED,
                                               now_ns=self._now_ns(), reason="late_or_stale_worker_output")
                return self._outcome(job.job_id, request.request_key, None, None, "LATE_OUTPUT_INELIGIBLE", False)
            if not model_ok or not token_ok or provider_result.truncated:
                reason = "RETURNED_MODEL_ID_DRIFT" if not model_ok else (
                    "TOKEN_BUDGET_EXCEEDED" if not token_ok else "TRUNCATED_OUTPUT")
                receipt = AgentValidationReceiptV1.create(request.request_key,
                    sha256_json({"raw_untrusted_output": provider_result.raw_output}), result_hash,
                    "INVALID", (reason,), self._now_ns())
                self._jobs.finalize_validation(job.job_id, result_id, receipt, now_ns=self._now_ns())
                return self._outcome(job.job_id, request.request_key, None, receipt, reason, False)
            known = self._jobs.known_proposal_hashes(request.research_family_id,
                                                     excluding_request_key=request.request_key)
            validated_proposal, receipt = validate_proposal_output(request, provider_result.raw_output,
                now_ns=self._now_ns(), available_evidence_refs=available_refs,
                provider_result_hash=result_hash, known_proposal_hashes=known)
            authoritative = self._jobs.finalize_validation(job.job_id, result_id, receipt, now_ns=self._now_ns())
            final_job = self._jobs.get_job(job.job_id)
            return ResearchJobOutcomeV1(job.job_id, request.request_key, final_job.lifecycle_state,
                validated_proposal if authoritative else None, receipt,
                None if authoritative else "CONTRADICTORY_RESULT",
                authoritative)
        self._jobs.transition_terminal(job.job_id, AgentJobStateV1.UNAVAILABLE,
                                       now_ns=self._now_ns(), reason=last_reason)
        return self._outcome(job.job_id, request.request_key, None, None, last_reason, False)

    def _outcome(self, job_id: str, request_key: str, proposal: ResearchProposalV1 | None,
                 receipt: AgentValidationReceiptV1 | None, reason: str | None,
                 authoritative: bool) -> ResearchJobOutcomeV1:
        job = self._jobs.get_job(job_id)
        return ResearchJobOutcomeV1(job_id, request_key, job.lifecycle_state, proposal, receipt, reason, authoritative)


@dataclass(frozen=True)
class ActionAssessmentRunOutcomeV1:
    request_id: str
    packet_ref: str
    status: str
    result: ActionAssessmentResultV2 | None
    reason_code: str | None
    accepted_shadow_evidence: bool


class DirectActionAssessmentBrokerPort:
    """Direct local-broker port for one structured critic request; no worker or tools."""

    def __init__(self, broker_client: Any) -> None:
        self._broker_client = broker_client

    def assess(self, *, capability: str, authorization_id: str, attempt_id: str,
               request: ActionAssessmentRequestV2,
               packet: SealedActionAssessmentPacketV1) -> ProviderResultV1:
        return self._broker_client.assess_action_v1(capability=capability,
            authorization_id=authorization_id, attempt_id=attempt_id, request=request, packet=packet)


class ActionAssessmentController:
    """Prepare and finalize hidden critic work on the sole ops/controller writer."""

    def __init__(self, *, ledger: ActionAssessmentRepository,
                 profile: ActionAssessmentProviderProfileV1,
                 capability_signing_key: bytes | None, provider: DirectActionAssessmentBrokerPort | None,
                 now_ns: Callable[[], int] = time.time_ns) -> None:
        if capability_signing_key is not None and len(capability_signing_key) < 32:
            raise ValueError("broker capability signing key must contain at least 256 bits")
        if profile.price_schedule_hash != ledger.price_schedule.content_hash:
            raise ValueError("action-assessment profile and persisted price schedule differ")
        if profile.task_identity != "ActionAssessmentProvider":
            raise ValueError("action-assessment controller requires its distinct task profile")
        self._ledger = ledger
        self._profile = profile
        self._signing_key = bytes(capability_signing_key) if capability_signing_key is not None else None
        self._provider = provider
        self._now_ns = now_ns
        self._lock = threading.Lock()

    def prepare(self, request: ActionAssessmentRequestV2,
                packet: SealedActionAssessmentPacketV1
                ) -> ActionAssessmentDispatchWorkV1 | ActionAssessmentRunOutcomeV1:
        """Persist the complete dispatch chain before returning an authorized immutable work item."""
        with self._lock:
            self._check_binding(request, packet)
            now = self._now_ns()
            state = self._ledger.request_state(request.request_id)
            if state is not None and state["status"] is not None:
                return self._from_state(request, packet, state)
            if state is not None and state["authorization_id"] is not None:
                self._ledger.recover_dispatch_without_result(request.request_id, now_ns=now)
                return self._from_state(request, packet, self._required_state(request.request_id))
            if state is not None and state["attempt_id"] is not None:
                self._record_terminal(request, packet, "UNAVAILABLE", "ATTEMPT_INTERRUPTED_BEFORE_DISPATCH",
                                      now_ns=now, attempt_id=state["attempt_id"])
                return self._from_state(request, packet, self._required_state(request.request_id))
            self._ledger.persist_packet_request(packet, request, now_ns=now)
            if now >= request.deadline_ns:
                self._record_terminal(request, packet, "EXPIRED", "ORIGINAL_DECISION_DEADLINE_ELAPSED", now_ns=now)
                return self._from_state(request, packet, self._required_state(request.request_id))
            if self._provider is None or self._signing_key is None:
                self._record_terminal(request, packet, "UNAVAILABLE", "BROKER_UNAVAILABLE", now_ns=now)
                return self._from_state(request, packet, self._required_state(request.request_id))

            attempt_id = str(uuid.uuid5(uuid.UUID(request.request_id), "action-assessment-attempt-1"))
            reservation_id = str(uuid.uuid5(uuid.UUID(request.request_id), "action-assessment-cost-reservation-1"))
            authorization_id = str(uuid.uuid5(uuid.UUID(request.request_id), "action-assessment-dispatch-authorization-1"))
            capability_nonce = str(uuid.uuid5(uuid.UUID(request.request_id), "action-assessment-capability-nonce-1"))
            self._ledger.create_attempt(request.request_id, attempt_id=attempt_id, now_ns=now)
            reserved = self._ledger.price_schedule.worst_case_call_usd(input_tokens=12_000, output_tokens=2_048)
            try:
                self._ledger.reserve_cost(request.request_id, attempt_id=attempt_id, reservation_id=reservation_id,
                                          now_ns=now, reserved_usd=reserved)
            except Exception:
                self._record_terminal(request, packet, "UNAVAILABLE", "COST_RESERVATION_UNAVAILABLE",
                                      now_ns=self._now_ns(), attempt_id=attempt_id)
                return self._from_state(request, packet, self._required_state(request.request_id))

            authorized_at = self._now_ns()
            if authorized_at >= request.deadline_ns:
                self._record_terminal(request, packet, "EXPIRED", "ORIGINAL_DECISION_DEADLINE_ELAPSED",
                                      now_ns=authorized_at, attempt_id=attempt_id)
                return self._from_state(request, packet, self._required_state(request.request_id))
            expires_at = min(authorized_at + 120_000_000_000, request.deadline_ns)
            authorization = ActionAssessmentDispatchAuthorizationV1.create(
                task_identity=request.task_identity, packet_ref=packet.packet_ref, packet_hash=packet.content_hash,
                request_hash=request.content_hash, action_hash=packet.action_hash,
                profile_hash=self._profile.content_hash, provider_binding_hash=self._profile.provider_binding_hash,
                price_schedule_hash=self._profile.price_schedule_hash, provider=self._profile.provider,
                requested_model_id=self._profile.requested_model_id, endpoint=self._profile.endpoint,
                attempt_id=attempt_id, deadline_ns=request.deadline_ns, authorized_at_ns=authorized_at,
                expires_at_ns=expires_at, reservation_id=reservation_id, reserved_cost_usd=str(reserved),
                max_input_tokens=12_000, max_output_tokens=2_048, authorization_id=authorization_id,
                capability_nonce=capability_nonce)
            # Durable authorization and reservation are in SQLite before a signed capability exists.
            self._ledger.persist_dispatch_authorization(authorization, now_ns=authorized_at)
            capability = ActionAssessmentBrokerCapabilityV1.issue_authorized(
                authorization=authorization, signing_key=self._signing_key).capability
            identity = ActionAssessmentDispatchIdentityV1(
                request.request_id, request.content_hash, packet.packet_ref, packet.content_hash,
                packet.action_hash, attempt_id, authorization.authorization_id,
                authorization.authorization_hash, authorization.capability_nonce,
                self._profile.content_hash, authorized_at, request.deadline_ns)
            return ActionAssessmentDispatchWorkV1(identity, request, packet, authorization, capability)

    def finalize(self, work: ActionAssessmentDispatchWorkV1,
                 completion: ActionAssessmentDispatchCompletionV1) -> ActionAssessmentRunOutcomeV1:
        """Validate a returned completion and persist its sole terminal result on the writer thread."""
        with self._lock:
            if completion.identity != work.identity:
                raise ValueError("stale or unknown action-assessment dispatch completion identity")
            request, packet, provider_result = work.request, work.packet, completion.result
            self._check_binding(request, packet)
            state = self._required_state(request.request_id)
            # A restart or earlier terminal result wins over every late/duplicate worker return.
            if state["status"] is not None:
                return self._from_state(request, packet, state)
            _verify_durable_dispatch_state(state, work)
            if self._signing_key is None:
                raise ValueError("critic dispatch capability cannot be verified without its signing key")
            expected_capability = ActionAssessmentBrokerCapabilityV1.issue_authorized(
                authorization=work.authorization, signing_key=self._signing_key).capability
            if work.capability != expected_capability:
                raise ValueError("critic dispatch capability does not match its immutable authorization")
            finalized_at = self._now_ns()
            received_at = completion.received_at_ns
            if received_at > finalized_at:
                raise ValueError("critic completion timestamp is later than controller time")
            raw_hash = sha256_json({"raw_untrusted_action_assessment": provider_result.raw_output})
            provider_output_hash = sha256_json({"raw_output_hash": raw_hash,
                "returned_model_id": provider_result.returned_model_id,
                "failure_code": provider_result.failure_code, "refusal": provider_result.refusal,
                "truncated": provider_result.truncated,
                "input_tokens": provider_result.input_tokens, "output_tokens": provider_result.output_tokens})
            if received_at >= request.deadline_ns:
                receipt = ActionAssessmentValidationReceiptV1.create(request_hash=request.content_hash,
                    packet_ref=packet.packet_ref, packet_hash=packet.content_hash, action_hash=packet.action_hash,
                    provider_output_hash=provider_output_hash, status="LATE", reasons=("LATE_OUTPUT_INELIGIBLE",),
                    at_ns=received_at)
                self._ledger.record_validation(request.request_id, work.identity.attempt_id, receipt, now_ns=received_at)
                self._ledger.record_outcome(request.request_id, attempt_id=work.identity.attempt_id, status="EXPIRED",
                    result=None, provider_output_hash=provider_output_hash, failure_code="LATE_OUTPUT_INELIGIBLE",
                    received_at_ns=received_at, eligible=False)
                return self._from_state(request, packet, self._required_state(request.request_id))

            if provider_result.failure_code is not None or provider_result.refusal or provider_result.truncated:
                reason = _critic_failure_code(provider_result.failure_code or
                    ("PROVIDER_REFUSAL" if provider_result.refusal else "PROVIDER_TRUNCATED"))
                status = "REFUSED" if provider_result.refusal else (
                    "UNAVAILABLE" if reason in {"PROVIDER_TIMEOUT", "RATE_LIMITED", "PROVIDER_UNAVAILABLE",
                        "BROKER_UNAVAILABLE", "BROKER_SATURATED", "PROVIDER_ERROR"} else "INVALID")
                receipt = ActionAssessmentValidationReceiptV1.create(request_hash=request.content_hash,
                    packet_ref=packet.packet_ref, packet_hash=packet.content_hash, action_hash=packet.action_hash,
                    provider_output_hash=provider_output_hash, status=status, reasons=(reason,), at_ns=received_at)
                self._ledger.record_validation(request.request_id, work.identity.attempt_id, receipt, now_ns=received_at)
                self._ledger.record_outcome(request.request_id, attempt_id=work.identity.attempt_id, status=status,
                    result=None, provider_output_hash=provider_output_hash, failure_code=reason,
                    received_at_ns=received_at, eligible=False)
                return self._from_state(request, packet, self._required_state(request.request_id))

            if (provider_result.returned_model_id != request.requested_model_id
                    or provider_result.input_tokens > request.max_input_tokens
                    or provider_result.output_tokens > request.max_output_tokens):
                reason = "RETURNED_MODEL_ID_DRIFT" if provider_result.returned_model_id != request.requested_model_id \
                    else "TOKEN_LIMIT_EXCEEDED"
                receipt = ActionAssessmentValidationReceiptV1.create(request_hash=request.content_hash,
                    packet_ref=packet.packet_ref, packet_hash=packet.content_hash, action_hash=packet.action_hash,
                    provider_output_hash=provider_output_hash, status="INVALID", reasons=(reason,), at_ns=received_at)
                self._ledger.record_validation(request.request_id, work.identity.attempt_id, receipt, now_ns=received_at)
                self._ledger.record_outcome(request.request_id, attempt_id=work.identity.attempt_id, status="INVALID",
                    result=None, provider_output_hash=provider_output_hash, failure_code=reason,
                    received_at_ns=received_at, eligible=False)
                return self._from_state(request, packet, self._required_state(request.request_id))

            result, reasons = validate_action_assessment_output(request, packet, provider_result.raw_output,
                                                                 now_ns=received_at)
            valid = result is not None
            receipt = ActionAssessmentValidationReceiptV1.create(request_hash=request.content_hash,
                packet_ref=packet.packet_ref, packet_hash=packet.content_hash, action_hash=packet.action_hash,
                provider_output_hash=provider_output_hash, status="VALID" if valid else "INVALID",
                reasons=reasons, at_ns=received_at)
            self._ledger.record_validation(request.request_id, work.identity.attempt_id, receipt, now_ns=received_at)
            self._ledger.record_outcome(request.request_id, attempt_id=work.identity.attempt_id,
                status="COMPLETE" if valid else "INVALID", result=result.to_dict() if result is not None else None,
                provider_output_hash=provider_output_hash, failure_code=None if valid else reasons[0],
                received_at_ns=received_at, eligible=valid)
            return self._from_state(request, packet, self._required_state(request.request_id))

    def assess(self, request: ActionAssessmentRequestV2,
               packet: SealedActionAssessmentPacketV1) -> ActionAssessmentRunOutcomeV1:
        """Synchronous compatibility wrapper for existing focused tests and callers."""
        prepared = self.prepare(request, packet)
        if isinstance(prepared, ActionAssessmentRunOutcomeV1):
            return prepared
        if self._provider is None:
            return self._from_state(request, packet, self._required_state(request.request_id))
        start = max(prepared.identity.authorized_at_ns, self._now_ns())
        try:
            provider_result = self._provider.assess(capability=prepared.capability,
                authorization_id=prepared.identity.authorization_id,
                attempt_id=prepared.identity.attempt_id, request=request, packet=packet)
        except Exception as exc:
            reason = _critic_failure_code(getattr(exc, "code", "BROKER_UNAVAILABLE"))
            provider_result = ProviderResultV1("", None, None, False, False, 0, 0, None, reason)
        received = max(start, self._now_ns())
        completion = ActionAssessmentDispatchCompletionV1(prepared.identity, provider_result, start, received)
        return self.finalize(prepared, completion)

    def record_skip(self, receipt_ref: str, reason_code: str, *,
                    request: ActionAssessmentRequestV2 | None = None,
                    packet: SealedActionAssessmentPacketV1 | None = None) -> ActionAssessmentRunOutcomeV1 | None:
        now = self._now_ns()
        if request is None and packet is None:
            self._ledger.record_skip(receipt_ref, reason_code, now_ns=now)
            return None
        if request is None or packet is None:
            raise ValueError("shadow skip requires both its immutable request and packet")
        self._check_binding(request, packet)
        prior_state = self._ledger.request_state(request.request_id)
        if prior_state is not None and prior_state["status"] is not None:
            return self._from_state(request, packet, prior_state)
        self._ledger.record_skip(receipt_ref, reason_code, now_ns=now)
        self._ledger.persist_packet_request(packet, request, now_ns=now)
        state = self._ledger.request_state(request.request_id)
        if state is None:
            raise RuntimeError("persisted action-assessment skip request disappeared")
        if state["status"] is None:
            output_hash = sha256_json({"action_assessment_skip": reason_code})
            self._ledger.record_outcome(request.request_id, attempt_id=None, status="SKIPPED", result=None,
                provider_output_hash=output_hash, failure_code=reason_code, received_at_ns=now, eligible=False)
        return self._from_state(request, packet, self._required_state(request.request_id))

    def recover_open_dispatches(self) -> int:
        """Seal every prior-process authorization lacking a terminal result; never redispatch it."""
        self._assert_writer_thread()
        now = self._now_ns()
        request_ids = self._ledger.authorized_requests_without_terminal_result()
        for request_id in request_ids:
            self._ledger.recover_dispatch_without_result(request_id, now_ns=now)
        return len(request_ids)

    def pending_observation_records(self, *, limit: int = 2) -> tuple[Mapping[str, Any], ...]:
        self._assert_writer_thread()
        return self._ledger.unprojected_terminal_records(limit=limit)

    def mark_observation_projected(self, request_id: str, observation_ref: str, *, now_ns: int) -> None:
        self._assert_writer_thread()
        self._ledger.mark_observation_projected(request_id, observation_ref, now_ns=now_ns)

    def abandon_authorized_work(self, work: ActionAssessmentDispatchWorkV1, reason_code: str) -> ActionAssessmentRunOutcomeV1:
        """Seal a durable authorization that could not be handed to the reserved bounded queue."""
        with self._lock:
            state = self._required_state(work.request.request_id)
            if state["status"] is not None:
                return self._from_state(work.request, work.packet, state)
            _verify_durable_dispatch_state(state, work)
            reason = _critic_failure_code(reason_code)
            at_ns = self._now_ns()
            self._ledger.record_outcome(work.request.request_id, attempt_id=work.identity.attempt_id,
                status="UNAVAILABLE", result=None,
                provider_output_hash=sha256_json({"action_assessment_dispatch_failure": reason}),
                failure_code=reason, received_at_ns=at_ns, eligible=False)
            return self._from_state(work.request, work.packet, self._required_state(work.request.request_id))

    def fail_safe(self, *, receipt_ref: str, request: ActionAssessmentRequestV2,
                  packet: SealedActionAssessmentPacketV1, reason_code: str) -> ActionAssessmentRunOutcomeV1 | None:
        """Seal a safe unavailable terminal after coordinator errors; never redispatch."""
        state = self._ledger.request_state(request.request_id)
        now = self._now_ns()
        if state is None:
            self._ledger.record_skip(receipt_ref, _critic_failure_code(reason_code), now_ns=now)
            return None
        if state["status"] is not None:
            return self._from_state(request, packet, state)
        if state["authorization_id"] is not None:
            self._ledger.recover_dispatch_without_result(request.request_id, now_ns=now)
            return self._from_state(request, packet, self._required_state(request.request_id))
        self._record_terminal(request, packet, "UNAVAILABLE", _critic_failure_code(reason_code),
                              now_ns=now, attempt_id=state["attempt_id"])
        return self._from_state(request, packet, self._required_state(request.request_id))

    def _check_binding(self, request: ActionAssessmentRequestV2,
                       packet: SealedActionAssessmentPacketV1) -> None:
        if (request.packet_ref != packet.packet_ref or request.packet_hash != packet.content_hash
                or request.action_hash != packet.action_hash
                or request.profile_hash != self._profile.content_hash
                or request.provider_binding_hash != self._profile.provider_binding_hash
                or request.price_schedule_hash != self._profile.price_schedule_hash
                or request.deadline_ns != packet.original_deadline_d_ns
                or request.max_model_calls != 1 or request.max_dynamic_tools != 0
                or request.max_input_tokens != 12_000 or request.max_output_tokens != 2_048):
            raise ValueError("action-assessment controller binding mismatch")

    def _record_terminal(self, request: ActionAssessmentRequestV2, packet: SealedActionAssessmentPacketV1,
                         status: str, reason: str, *, now_ns: int, attempt_id: str | None = None) -> None:
        self._ledger.persist_packet_request(packet, request, now_ns=now_ns)
        output_hash = sha256_json({"action_assessment_terminal": reason})
        self._ledger.record_outcome(request.request_id, attempt_id=attempt_id, status=status, result=None,
            provider_output_hash=output_hash, failure_code=reason, received_at_ns=now_ns, eligible=False)

    def _required_state(self, request_id: str) -> dict[str, Any]:
        state = self._ledger.request_state(request_id)
        if state is None:
            raise RuntimeError("persisted action-assessment state disappeared")
        return state

    def _assert_writer_thread(self) -> None:
        assert_writer = getattr(self._ledger, "assert_writer_thread", None)
        if callable(assert_writer):
            assert_writer()

    @staticmethod
    def _from_state(request: ActionAssessmentRequestV2, packet: SealedActionAssessmentPacketV1,
                    state: Mapping[str, Any]) -> ActionAssessmentRunOutcomeV1:
        status = state.get("status") or "PENDING"
        result_data = state.get("result_json")
        result = None
        if isinstance(result_data, str):
            import json
            body = json.loads(result_data)
            result = ActionAssessmentResultV2.from_dict(body)
        return ActionAssessmentRunOutcomeV1(request.request_id, packet.packet_ref, status, result,
            state.get("failure_code"), bool(state.get("eligible")))


def _critic_failure_code(value: Any) -> str:
    allowed = {"BROKER_UNAVAILABLE", "BROKER_SATURATED", "PROVIDER_UNAVAILABLE", "PROVIDER_TIMEOUT", "RATE_LIMITED",
        "PROVIDER_REFUSAL", "PROVIDER_TRUNCATED", "PROVIDER_ERROR", "PROVIDER_AUTHENTICATION_FAILED",
        "PROVIDER_REQUEST_REJECTED", "RETURNED_MODEL_ID_DRIFT", "RETURNED_MODEL_ID_MISSING",
        "AGENT_DEPENDENCY_UNAVAILABLE", "INPUT_TOKEN_BUDGET_EXCEEDED", "TOKEN_LIMIT_EXCEEDED",
        "OUTPUT_SIZE_LIMIT", "MALFORMED_STRUCTURED_OUTPUT", "UNEXPECTED_TOOL_OUTPUT",
        "CALLBACK_FAILED", "BROKER_PROTOCOL_ERROR", "DISPATCH_HANDOFF_FAILED"}
    text = str(value)
    return text if text in allowed else "PROVIDER_ERROR"


def _verify_durable_dispatch_state(state: Mapping[str, Any], work: ActionAssessmentDispatchWorkV1) -> None:
    identity = work.identity
    try:
        request_body = json.loads(str(state["request_json"]))
        authorization_body = json.loads(str(state["authorization_json"]))
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("durable critic authorization is missing or malformed") from exc
    bindings = {
        "request_id": isinstance(request_body, Mapping) and request_body.get("request_id") == identity.request_id,
        "request_hash": state.get("request_hash") == identity.request_hash,
        "packet_ref": state.get("packet_ref") == identity.packet_ref,
        "packet_hash": state.get("packet_hash") == identity.packet_hash,
        "attempt_id": state.get("attempt_id") == identity.attempt_id,
        "authorization_id": state.get("authorization_id") == identity.authorization_id,
        "authorization_hash": state.get("authorization_hash") == identity.authorization_hash,
        "authorization_body": isinstance(authorization_body, Mapping)
            and authorization_body.get("authorization_hash") == identity.authorization_hash
            and authorization_body.get("request_hash") == identity.request_hash
            and authorization_body.get("packet_ref") == identity.packet_ref
            and authorization_body.get("packet_hash") == identity.packet_hash
            and authorization_body.get("action_hash") == identity.action_hash
            and authorization_body.get("attempt_id") == identity.attempt_id
            and authorization_body.get("authorization_id") == identity.authorization_id
            and authorization_body.get("capability_nonce") == identity.capability_nonce
            and authorization_body.get("authorized_at_ns") == identity.authorized_at_ns,
    }
    if not all(bindings.values()):
        failed = "_".join(name for name, matches in bindings.items() if not matches)
        raise ValueError(f"stale or mismatched durable critic dispatch identity: {failed}")


def _revision_status(profile: AgentModelProfile, returned_model_id: str | None,
                     revision: str | None) -> str:
    if revision:
        return "FIXED_REVISION" if isinstance(profile, AgentModelProfileV1) else "UNKNOWN"
    if returned_model_id == profile.requested_model_id:
        return "ALIAS_ONLY"
    return "UNKNOWN"


def _safe_reason(value: str) -> str:
    if not isinstance(value, str):
        return "PROVIDER_ERROR"
    clean = "".join(char for char in value if 32 <= ord(char) < 127)
    return clean[:160] or "PROVIDER_ERROR"


_TERMINAL = frozenset({AgentJobStateV1.VALIDATED, AgentJobStateV1.UNAVAILABLE, AgentJobStateV1.INVALID,
                       AgentJobStateV1.EXPIRED, AgentJobStateV1.CANCELLED})
