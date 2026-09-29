"""Deterministic job controller for quarantined Discovery Lab proposals only."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from atlas.v2._serialization import sha256_json
from atlas.v2.agent_intelligence.broker import BrokerCapabilityV1, BrokerCapabilityV2
from atlas.v2.agent_intelligence.contracts import (
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
)
from atlas.v2.agent_intelligence.evidence import BoundedResearchReadService, collect_job_evidence
from atlas.v2.agent_intelligence.persistence import AGENT_NAMESPACE_WRITER_OWNER, AgentJobRepository
from atlas.v2.agent_intelligence.provider import ResearchProviderUnavailable
from atlas.v2.agent_intelligence.validation import validate_proposal_output, validate_request_evidence_availability
from atlas.v2.agent_intelligence.worker import AgentWorkerSupervisor, WorkerSandboxUnavailable

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
