"""Small research ledger projection on existing ops artifacts and due-work.

No new table, queue framework, watch-outbox mutation or external effect. Only
an ops writer may call this service. Fake dispatch intent is persisted first;
uncertain attempts are never automatically retried, even after restart.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.agent import (
    FakeResearchProvider,
    ProviderCapabilitiesV1,
    ResearchBudgetV1,
    ResearchJobV1,
    preflight_reason,
    run_fake_research_job,
    validate_response,
)
from atlas.v2.research.decision_brief import DecisionBriefV1

RESEARCH_LANE = "ANALYSIS_RESEARCH_FAKE_V1"
INVALIDATORS = frozenset(
    {
        "SOURCE_GAP",
        "SOURCE_REVISION",
        "PRODUCT_REVISION",
        "WATCH_INVALIDATED",
        "POSITION_EPOCH_CHANGED",
        "POLICY_CHANGED",
        "MODEL_CHANGED",
    }
)


def _identity(job_id: str, stage: str) -> str:
    return sha256_json({"version": "RESEARCH_MEMORY_IDENTITY_V1", "job_id": job_id, "stage": stage})


def _memo_key(job: ResearchJobV1) -> str:
    return sha256_json(
        {
            "version": "RESEARCH_MEMO_KEY_V1",
            "brief_ref": job.brief.content_hash,
            "model_capability_ref": job.capabilities.content_hash,
            "budget": job.budget.to_dict(),
        }
    )


def _scope_key(job: ResearchJobV1) -> str:
    """One question per sealed slot/model; job ids, rebuild time and budgets cannot retry it."""
    body = job.brief.body
    return sha256_json(
        {
            "version": "RESEARCH_DISPATCH_SCOPE_V1",
            "slot_id": body["slot_id"],
            "snapshot_ref": body["snapshot_ref"],
            "cutoff_ns": body["cutoff_ns"],
            "deadline_ns": body["deadline_ns"],
            "candidate_set_ref": body.get("candidate_set_ref"),
            "policy_hash": body.get("research_policy_hash"),
            "schema_hash": body.get("schema_hash"),
            "provider_id": job.capabilities.provider_id,
            "model_id": job.capabilities.model_id,
            "capability_version": job.capabilities.version,
            "verification_ref": job.capabilities.verification_ref,
        }
    )


class ResearchMemoryV1:
    """Typed composition service for the existing authorized ops controller."""

    def __init__(self, repository: OpsRepository) -> None:
        self.repository = repository
        self._writer_thread = threading.get_ident()

    def _writer(self) -> None:
        if self.repository.read_only or threading.get_ident() != self._writer_thread:
            raise RuntimeError("research mutations belong to the existing ops writer thread")

    def _read(self, ref: str) -> Mapping[str, Any] | None:
        row = self.repository.get_artifact(ref)
        if row is None:
            return None
        body = row.metadata
        if row.content_hash != sha256_json(body) or body.get("authority") != "ZERO":
            raise ValueError("research memory content/authority mismatch")
        return body

    def _persist(self, ref: str, kind: str, body: Mapping[str, Any], *, now_ns: int) -> None:
        self._writer()
        timestamp(now_ns, field="memory.now_ns")
        self.repository.register_artifact(ArtifactIndexEntryV2(ref, kind, sha256_json(body), now_ns, now_ns, body))

    @staticmethod
    def production_projection_identity(receipt_ref: str) -> str:
        sha256_ref(receipt_ref, field="receipt_ref")
        return sha256_json({"version": "PRODUCTION_INTELLIGENCE_MEMORY_IDENTITY_V1", "receipt_ref": receipt_ref})

    @staticmethod
    def production_brief_identity(receipt_ref: str) -> str:
        sha256_ref(receipt_ref, field="receipt_ref")
        return sha256_json({"artifact_type": "ProductionDecisionBriefProjectionV1", "receipt_ref": receipt_ref})

    @staticmethod
    def production_memory_identity(receipt_ref: str) -> str:
        sha256_ref(receipt_ref, field="receipt_ref")
        return sha256_json({"artifact_type": "ProductionIntelligenceMemoryProjectionV1", "receipt_ref": receipt_ref})

    def persist_production_projection(
        self,
        *,
        receipt_ref: str,
        brief: Mapping[str, Any],
        brief_status: str,
        source_refs: tuple[str, ...],
        source_population_ref: str,
        now_ns: int,
        due_work_status: str = "UNAVAILABLE",
        due_work_reason: str = "NO_CAUSAL_DUE_WORK_JOIN_READER",
        due_work_refs: tuple[str, ...] = (),
        precommit_check: Callable[[], None] | None = None,
    ) -> tuple[str, str]:
        """Persist one zero-authority brief and memory pointer through the ops writer.

        This path records no provider request, due-work item, dispatch intent or
        retry. The receipt identity is the idempotency key, so replay returns the
        original immutable brief and memory artifacts.
        """
        self._writer()
        sha256_ref(receipt_ref, field="receipt_ref")
        sha256_ref(source_population_ref, field="source_population_ref")
        timestamp(now_ns, field="production_projection.now_ns")
        if brief_status not in {"COMPLETE", "BLOCKED", "EXPIRED"}:
            raise ValueError("invalid production brief status")
        sources = tuple(sorted(set(source_refs)))
        for ref in sources:
            sha256_ref(ref, field="source_ref")
        due_refs = tuple(sorted(set(due_work_refs)))
        for ref in due_refs:
            sha256_ref(ref, field="due_work_ref")

        identity_ref = self.production_projection_identity(receipt_ref)
        prior = self._read(identity_ref)
        if prior is not None:
            brief_ref, memory_ref = prior.get("brief_ref"), prior.get("memory_ref")
            if not isinstance(brief_ref, str) or not isinstance(memory_ref, str):
                raise ValueError("production memory identity is incomplete")
            brief_body = self._read(brief_ref)
            memory_body = self._read(memory_ref)
            if (
                brief_body is None
                or memory_body is None
                or brief_ref != self.production_brief_identity(receipt_ref)
                or memory_ref != self.production_memory_identity(receipt_ref)
                or brief_body.get("receipt_ref") != receipt_ref
                or memory_body.get("receipt_ref") != receipt_ref
                or memory_body.get("brief_ref") != brief_ref
                or brief_body.get("status") != memory_body.get("brief_status")
                or brief_body.get("source_refs") != memory_body.get("source_refs")
            ):
                raise ValueError("production memory identity points to conflicting projection")
            return brief_ref, memory_ref

        brief_body = {
            "version": "PRODUCTION_DECISION_BRIEF_PROJECTION_V1",
            "authority": "ZERO",
            "receipt_ref": receipt_ref,
            "source_population_ref": source_population_ref,
            "brief": dict(brief),
            "status": brief_status,
            "source_refs": list(sources),
        }
        brief_ref = self.production_brief_identity(receipt_ref)
        memory_ref = self.production_memory_identity(receipt_ref)
        memory_body = {
            "version": "PRODUCTION_INTELLIGENCE_MEMORY_PROJECTION_V1",
            "authority": "ZERO",
            "receipt_ref": receipt_ref,
            "brief_ref": brief_ref,
            "brief_status": brief_status,
            "source_population_ref": source_population_ref,
            "source_refs": list(sources),
            "created_at_ns": now_ns,
            "due_work_status": due_work_status,
            "due_work_reason": due_work_reason,
            "due_work_refs": list(due_refs),
            "provider_job_status": "NOT_CREATED_NO_AUTHORITY",
        }
        identity_body = {
            "version": "PRODUCTION_INTELLIGENCE_MEMORY_IDENTITY_V1",
            "authority": "ZERO",
            "receipt_ref": receipt_ref,
            "brief_ref": brief_ref,
            "memory_ref": memory_ref,
        }
        with self.repository.atomic_composition():
            self._persist(brief_ref, "ProductionDecisionBriefProjectionV1", brief_body, now_ns=now_ns)
            self._persist(memory_ref, "ProductionIntelligenceMemoryProjectionV1", memory_body, now_ns=now_ns)
            self._persist(identity_ref, "ProductionIntelligenceMemoryIdentityV1", identity_body, now_ns=now_ns)
            if precommit_check is not None:
                precommit_check()
        return brief_ref, memory_ref

    @staticmethod
    def production_gemma_intent_identity(receipt_ref: str, job_ref: str) -> str:
        sha256_ref(receipt_ref, field="receipt_ref")
        sha256_ref(job_ref, field="job_ref")
        return sha256_json({"version": "PRODUCTION_GEMMA_SHADOW_INTENT_IDENTITY_V1",
                            "receipt_ref": receipt_ref, "job_ref": job_ref})

    @staticmethod
    def production_gemma_result_identity(receipt_ref: str, job_ref: str) -> str:
        sha256_ref(receipt_ref, field="receipt_ref")
        sha256_ref(job_ref, field="job_ref")
        return sha256_json({"version": "PRODUCTION_GEMMA_SHADOW_RESULT_IDENTITY_V1",
                            "receipt_ref": receipt_ref, "job_ref": job_ref})

    def build_production_gemma_job(
        self,
        receipt_ref: str,
        *,
        capabilities: ProviderCapabilitiesV1,
        budget: ResearchBudgetV1,
        now_ns: int,
    ) -> ResearchJobV1 | None:
        """Compile a persisted complete production Brief into an S47 shadow job.

        This is writer-side preparation only. A missing/blocked/expired Brief
        returns no job; the method never creates a Brief, provider intent, or
        dispatch. Provider capability and budget claims remain caller evidence
        and are rechecked by the S47 preflight before a worker can call out.
        """
        self._writer()
        from atlas.v2.research.gemma_native import GEMMA_MODEL_ID

        sha256_ref(receipt_ref, field="receipt_ref")
        timestamp(now_ns, field="gemma_shadow.now_ns")
        if (capabilities.provider_id != "gemini" or capabilities.model_id != GEMMA_MODEL_ID
                or capabilities.fallback is not False):
            raise ValueError("production Gemma job provider identity is fixed")
        if (budget.max_model_calls != 1 or budget.max_tool_rounds != 0
                or budget.max_tool_rows != 0 or budget.max_tool_bytes != 0):
            raise ValueError("production Gemma job permits one call and no tool budget")
        projection_identity_ref = self.production_projection_identity(receipt_ref)
        projection_identity = self._read(projection_identity_ref)
        if projection_identity is None or projection_identity.get("receipt_ref") != receipt_ref:
            return None
        brief_ref, memory_ref = projection_identity.get("brief_ref"), projection_identity.get("memory_ref")
        if (brief_ref != self.production_brief_identity(receipt_ref)
                or memory_ref != self.production_memory_identity(receipt_ref)):
            return None
        projection = self._read(brief_ref)
        memory = self._read(memory_ref)
        if (projection is None or memory is None or projection.get("receipt_ref") != receipt_ref
                or memory.get("receipt_ref") != receipt_ref or memory.get("brief_ref") != brief_ref
                or memory.get("brief_status") != projection.get("status")
                or memory.get("source_refs") != projection.get("source_refs")
                or memory.get("source_population_ref") != projection.get("source_population_ref")):
            return None
        for artifact_ref in (projection_identity_ref, brief_ref, memory_ref):
            artifact = self.repository.get_artifact(artifact_ref)
            if artifact is None or artifact.available_at_ns > now_ns:
                return None
        receipt = self.repository.get_artifact(receipt_ref)
        if (receipt is None or receipt.artifact_type != "OpsSupervisorReceiptV1"
                or receipt.available_at_ns > now_ns or projection.get("status") != "COMPLETE"):
            return None
        value = projection.get("brief")
        if (not isinstance(value, Mapping) or value.get("status") != "COMPLETE"
                or not isinstance(value.get("reasons", ()), (list, tuple))
                or any(not isinstance(reason, str) for reason in value.get("reasons", ()))):
            return None
        body = value.get("body")
        if (not isinstance(body, Mapping) or body.get("authority") != "ZERO"
                or body.get("shadow_only") is not True or not body.get("shortlist")
                or not {"slot_id", "snapshot_ref", "cutoff_ns", "deadline_ns", "economics", "facts",
                        "tool_allowlist"}.issubset(body)
                or type(body.get("cutoff_ns")) is not int or type(body.get("deadline_ns")) is not int):
            return None
        brief = DecisionBriefV1("COMPLETE", tuple(value.get("reasons", ())), FrozenMap(body))
        job_id = sha256_json({"version": "PRODUCTION_GEMMA_SHADOW_JOB_ID_V1",
                              "receipt_ref": receipt_ref, "brief_ref": brief_ref})
        return ResearchJobV1(job_id, brief, capabilities, budget, now_ns)

    def _production_gemma_binding(self, receipt_ref: str, job: ResearchJobV1,
                                  *, now_ns: int) -> tuple[Mapping[str, Any] | None, str | None]:
        """Bind a provider job to the immutable, production-composed Brief."""
        sha256_ref(receipt_ref, field="receipt_ref")
        timestamp(now_ns, field="gemma_shadow.now_ns")
        identity = self._read(self.production_projection_identity(receipt_ref))
        if identity is None or identity.get("receipt_ref") != receipt_ref:
            return None, "PRODUCTION_BRIEF_UNAVAILABLE"
        brief_ref, memory_ref = identity.get("brief_ref"), identity.get("memory_ref")
        if (not isinstance(brief_ref, str) or not isinstance(memory_ref, str)
                or brief_ref != self.production_brief_identity(receipt_ref)
                or memory_ref != self.production_memory_identity(receipt_ref)):
            return None, "PRODUCTION_BRIEF_BINDING"
        projection = self._read(brief_ref)
        memory = self._read(memory_ref)
        if (projection is None or memory is None or projection.get("receipt_ref") != receipt_ref
                or memory.get("receipt_ref") != receipt_ref or memory.get("brief_ref") != brief_ref
                or memory.get("brief_status") != projection.get("status")):
            return None, "PRODUCTION_BRIEF_BINDING"
        stored_brief = projection.get("brief")
        if not isinstance(stored_brief, Mapping) or stored_brief.get("status") != projection.get("status"):
            return None, "PRODUCTION_BRIEF_BINDING"
        if canonical_json(stored_brief) != canonical_json(job.brief.to_dict()):
            return None, "PRODUCTION_BRIEF_MISMATCH"
        receipt = self.repository.get_artifact(receipt_ref)
        if (receipt is None or receipt.artifact_type != "OpsSupervisorReceiptV1"
                or receipt.available_at_ns > now_ns):
            return None, "PRODUCTION_RECEIPT_UNAVAILABLE"
        if projection.get("status") != "COMPLETE" or job.brief.status != "COMPLETE":
            return None, "PRODUCTION_BRIEF_NOT_COMPLETE"
        if (job.brief.body.get("authority") != "ZERO" or job.brief.body.get("shadow_only") is not True
                or not job.brief.body.get("shortlist")):
            return None, "PRODUCTION_BRIEF_NOT_SHADOW_ELIGIBLE"
        return projection, None

    def prepare_gemma_shadow_dispatch(
        self, job: ResearchJobV1, *, receipt_ref: str, now_ns: int,
    ) -> Mapping[str, Any]:
        """Persist a no-retry intent for a separate Gemma worker; never calls a provider.

        The ops writer uses this method before handing the immutable job to an
        isolated provider worker. That worker must return through
        ``record_gemma_shadow_result`` on the writer thread. No due-work item,
        A0 artifact, calendar row, order, or economic field is changed here.
        """
        self._writer()
        from atlas.v2.research.gemma_native import GEMMA_MODEL_ID

        if (job.capabilities.provider_id != "gemini" or job.capabilities.model_id != GEMMA_MODEL_ID
                or job.budget.max_model_calls != 1 or job.budget.max_tool_rounds != 0):
            raise ValueError("Gemma shadow request is outside the R4 test provider binding")
        projection, binding_error = self._production_gemma_binding(receipt_ref, job, now_ns=now_ns)
        if binding_error:
            return FrozenMap({"status": "LLM_NOT_CALLED", "reason": binding_error,
                              "authority_tag": "ZERO_AUTHORITY_SHADOW", "paid_dispatch": False})
        denied = preflight_reason(job, now_ns)
        if denied:
            return FrozenMap({"status": denied[0], "reason": denied[1],
                              "authority_tag": "ZERO_AUTHORITY_SHADOW", "paid_dispatch": False})
        assert projection is not None
        job_ref = job.request_hash
        intent_ref = self.production_gemma_intent_identity(receipt_ref, job_ref)
        result_ref = self.production_gemma_result_identity(receipt_ref, job_ref)
        existing = self._read(intent_ref)
        terminal = self._read(result_ref)
        if terminal is not None:
            return FrozenMap({"status": terminal["status"], "reason": terminal.get("reason"),
                              "intent_ref": intent_ref, "result_ref": result_ref,
                              "authority_tag": "ZERO_AUTHORITY_SHADOW", "audit_only": True})
        if existing is not None:
            return FrozenMap({"status": "UNKNOWN", "reason": "UNCERTAIN_DISPATCH_NO_RETRY",
                              "intent_ref": intent_ref, "authority_tag": "ZERO_AUTHORITY_SHADOW",
                              "audit_only": True})
        body = {
            "version": "PRODUCTION_GEMMA_SHADOW_INTENT_V1",
            "authority": "ZERO",
            "authority_tag": "ZERO_AUTHORITY_SHADOW",
            "receipt_ref": receipt_ref,
            "brief_ref": self.production_brief_identity(receipt_ref),
            "a0_population_ref": projection.get("source_population_ref"),
            "a0_evidence_refs": list(projection.get("source_refs", ())),
            "job_ref": job_ref,
            "provider_id": "gemini",
            "requested_model_id": GEMMA_MODEL_ID,
            "model_capability_ref": job.capabilities.content_hash,
            "calendar_slot_id": job.brief.body["slot_id"],
            "decision_calendar_refs": list(job.brief.body.get("calendar_refs", ())),
            "created_at_ns": now_ns,
            "paid_dispatch": False,
        }
        with self.repository.atomic_composition():
            self._persist(intent_ref, "ProductionGemmaShadowIntentV1", body, now_ns=now_ns)
        return FrozenMap({"status": "READY_TO_DISPATCH", "intent_ref": intent_ref,
                          "job_ref": job_ref, "request_hash": job.request_hash,
                          "authority_tag": "ZERO_AUTHORITY_SHADOW", "max_model_calls": 1,
                          "paid_dispatch": False})

    def record_gemma_shadow_result(
        self,
        job: ResearchJobV1,
        *,
        receipt_ref: str,
        response: Mapping[str, Any] | None,
        returned_model: str | None,
        usage_metadata: tuple[int | None, int | None, int | None] | None,
        transport_bytes: tuple[int | None, int | None] | None = None,
        now_ns: int,
        failure_code: str | None = None,
    ) -> Mapping[str, Any]:
        """Persist only the validated proposal and sanitized provider evidence."""
        self._writer()
        from atlas.v2.research.gemma_native import GEMMA_MODEL_ID

        projection, binding_error = self._production_gemma_binding(receipt_ref, job, now_ns=now_ns)
        if binding_error:
            raise ValueError("Gemma result production binding failed")
        assert projection is not None
        job_ref = job.request_hash
        intent_ref = self.production_gemma_intent_identity(receipt_ref, job_ref)
        result_ref = self.production_gemma_result_identity(receipt_ref, job_ref)
        intent = self._read(intent_ref)
        if intent is None or intent.get("job_ref") != job_ref:
            raise ValueError("durable Gemma intent is required before result persistence")
        prior = self._read(result_ref)
        if prior is not None:
            return FrozenMap(prior)

        status, reason, decision = "UNKNOWN", "PROVIDER_RESULT_UNAVAILABLE", None
        if failure_code is not None:
            validation_failures = {"MALFORMED_RESULT", "STALE_SNAPSHOT", "JOB_BINDING", "EVIDENCE_SCOPE",
                                   "CANDIDATE_ACTION_SCOPE"}
            allowed_failures = {
                "DEADLINE_EXPIRED", "PROVIDER_TIMEOUT", "RATE_LIMITED", "PROVIDER_UNAVAILABLE",
                "PROVIDER_AUTHENTICATION_FAILED", "PROVIDER_REQUEST_REJECTED", "PROVIDER_ERROR",
                "PROVIDER_REFUSAL", "MALFORMED_STRUCTURED_OUTPUT", "RETURNED_MODEL_IDENTITY",
                "MISSING_PROVIDER_RESPONSE", "INCOMPLETE_PROVIDER_RESPONSE", "OUTPUT_SIZE_LIMIT",
                "UNSUPPORTED_PROVIDER_CAPABILITY", "INPUT_BUDGET_EXCEEDED", "OUTPUT_BUDGET_EXCEEDED",
                "REQUEST_BUDGET_EXCEEDED", "RESPONSE_SIZE_LIMIT", "INVALID_PROVIDER_JSON",
                "INVALID_PROVIDER_RESPONSE", "UNSAFE_REDIRECT", "CONFIGURED_MODEL_MISMATCH",
                "NO_SUPPORTED_CHOICE", "PROVIDER_MODEL_BINDING", *validation_failures,
                "PROVIDER_AUTHORIZATION", "JOB_BUDGET_BINDING", "PROBE_REQUIRES_SMOKE_PERMIT",
            }
            prefixed_validation_failure = (failure_code.startswith("INVALID_RESPONSE_")
                                           and failure_code.removeprefix("INVALID_RESPONSE_") in validation_failures)
            preflight_failures = {"JOB_NOT_YET_AVAILABLE", "DEADLINE_EXPIRED", "NO_SUPPORTED_CHOICE",
                                  "UNKNOWN_PRICE", "UNVERIFIED_CAPABILITY", "UNVERIFIED_IMAGE_CAPABILITY",
                                  "BUDGET_EXHAUSTED"}
            prefixed_preflight_failure = (failure_code.startswith("PREFLIGHT_")
                                          and failure_code.removeprefix("PREFLIGHT_") in preflight_failures)
            if failure_code not in allowed_failures and not prefixed_validation_failure and not prefixed_preflight_failure:
                raise ValueError("unregistered sanitized Gemma failure code")
            if failure_code in validation_failures or prefixed_validation_failure:
                status = "INVALID"
            elif failure_code == "PREFLIGHT_DEADLINE_EXPIRED":
                status = "EXPIRED"
            reason = failure_code
        elif now_ns >= job.brief.body["deadline_ns"]:
            status, reason = "EXPIRED", "LATE_RESPONSE"
        elif returned_model not in {GEMMA_MODEL_ID, f"models/{GEMMA_MODEL_ID}"}:
            status, reason = "INVALID", "RETURNED_MODEL_IDENTITY"
        elif response is None:
            status, reason = "UNKNOWN", "MISSING_PROVIDER_RESPONSE"
        else:
            validation_reason = validate_response(job, response)
            if validation_reason:
                status, reason = "INVALID", validation_reason
            else:
                status, reason, decision = "VALID_SHADOW", None, dict(response)

        usage: dict[str, int | None] | None = None
        if usage_metadata is not None:
            if (not isinstance(usage_metadata, tuple) or len(usage_metadata) != 3
                    or any(value is not None and (type(value) is not int or value < 0)
                           for value in usage_metadata)):
                raise ValueError("invalid Gemini usage metadata")
            usage = {"prompt_tokens": usage_metadata[0], "candidate_tokens": usage_metadata[1],
                     "total_tokens": usage_metadata[2]}
        byte_counts: dict[str, int | None] | None = None
        if transport_bytes is not None:
            if (not isinstance(transport_bytes, tuple) or len(transport_bytes) != 2
                    or any(value is not None and (type(value) is not int or value < 0)
                           for value in transport_bytes)):
                raise ValueError("invalid Gemini transport byte metadata")
            byte_counts = {"request_body_bytes": transport_bytes[0], "response_body_bytes": transport_bytes[1]}
        body = {
            "version": "PRODUCTION_GEMMA_SHADOW_RESULT_V1",
            "authority": "ZERO",
            "authority_tag": "ZERO_AUTHORITY_SHADOW",
            "receipt_ref": receipt_ref,
            "brief_ref": self.production_brief_identity(receipt_ref),
            "a0_population_ref": projection.get("source_population_ref"),
            "a0_evidence_refs": list(projection.get("source_refs", ())),
            "job_ref": job_ref,
            "intent_ref": intent_ref,
            "status": status,
            "reason": reason,
            "provider_id": "gemini",
            "requested_model_id": GEMMA_MODEL_ID,
            "returned_model_id": returned_model,
            "decision": decision,
            "usage_metadata": usage,
            "transport_bytes": byte_counts,
            "calendar_slot_id": job.brief.body["slot_id"],
            "decision_calendar_refs": list(job.brief.body.get("calendar_refs", ())),
            "recorded_at_ns": now_ns,
            "paid_dispatch": False,
        }
        with self.repository.atomic_composition():
            self._persist(result_ref, "ProductionGemmaShadowResultV1", body, now_ns=now_ns)
        return FrozenMap(body)

    def request(self, job_id: str) -> Mapping[str, Any] | None:
        return self._read(_identity(job_id, "request"))

    def schedule(self, job: ResearchJobV1, *, event_ref: str, now_ns: int) -> Mapping[str, Any]:
        self._writer()
        sha256_ref(event_ref, field="event_ref")
        timestamp(now_ns, field="schedule.now_ns")
        old = self.request(job.job_id)
        if old is not None and old["request_hash"] != job.request_hash:
            raise ValueError("job id conflicts with immutable request")
        source = self.repository.get_artifact(event_ref)
        unavailable = (
            source is None
            or source.available_at_ns > job.brief.body["cutoff_ns"]
            or source.available_at_ns > now_ns
            or self.repository.effective_available_at_ns(event_ref) is None
            or self.repository.effective_available_at_ns(event_ref) > job.brief.body["cutoff_ns"]
        )
        body = {
            "version": "ANALYSIS_RESEARCH_REQUEST_V1",
            "authority": "ZERO",
            "job_id": job.job_id,
            "request_hash": job.request_hash,
            "brief_ref": job.brief.content_hash,
            "snapshot_ref": job.brief.body["snapshot_ref"],
            "cutoff_ns": job.brief.body["cutoff_ns"],
            "deadline_ns": job.brief.body["deadline_ns"],
            "created_at_ns": job.created_at_ns,
            "calendar_slot_id": job.brief.body["slot_id"],
            "calendar_denominator": 1,
            "model_ref": job.capabilities.content_hash,
            "budget_ref": sha256_json(job.budget.to_dict()),
            "memo_key": _memo_key(job),
        }
        trigger_ref = sha256_json(
            {"version": "RESEARCH_TRIGGER_IDENTITY_V1", "job_id": job.job_id, "event_ref": event_ref}
        )
        trigger = {
            "version": "RESEARCH_TRIGGER_V1",
            "authority": "ZERO",
            "job_id": job.job_id,
            "request_hash": job.request_hash,
            "event_ref": event_ref,
            "actual_available_at_ns": source.available_at_ns if source else None,
            "status": "SOURCE_UNAVAILABLE" if unavailable else "RECORDED",
            "calendar_slot_id": job.brief.body["slot_id"],
        }
        with self.repository.atomic_composition():
            if old is None:
                self._persist(_identity(job.job_id, "request"), "AnalysisResearchRequestV1", body, now_ns=now_ns)
                brief_body = {**job.brief.to_dict(), "authority": "ZERO"}
                prior_brief = self._read(job.brief.content_hash)
                if prior_brief is None:
                    self._persist(job.brief.content_hash, "DecisionBriefResearchV1", brief_body, now_ns=now_ns)
                elif sha256_json(prior_brief) != sha256_json(brief_body):
                    raise ValueError("brief immutable content conflict")
            # Preserve each distinct origin, dedupe repeated delivery without changing first availability.
            if self._read(trigger_ref) is None:
                self._persist(trigger_ref, "AnalysisResearchTriggerV1", trigger, now_ns=now_ns)
            terminal = self._read(_identity(job.job_id, "terminal"))
            if terminal is not None:
                return terminal
            if unavailable:
                return self._terminal(job, "LLM_NOT_CALLED", "SOURCE_UNAVAILABLE_AT_CUTOFF", now_ns=now_ns)
            if now_ns >= job.brief.body["deadline_ns"]:
                return self._terminal(job, "EXPIRED", "DEADLINE_EXPIRED", now_ns=now_ns)
            self.repository.enqueue_due_work(
                lane=RESEARCH_LANE,
                work_id=job.job_id,
                source_ref=_identity(job.job_id, "request"),
                created_at_ns=self.repository.get_artifact(_identity(job.job_id, "request")).created_at_ns,
                due_at_ns=now_ns,
                payload={"request_hash": job.request_hash, "authority": "ZERO"},
            )
        return FrozenMap({**body, "status": "DUE"})

    def _terminal(
        self,
        job: ResearchJobV1,
        status: str,
        reason: str | None,
        *,
        now_ns: int,
        result: Mapping[str, Any] | None = None,
        memo_ref: str | None = None,
    ) -> Mapping[str, Any]:
        body = {
            "version": "RESEARCH_TERMINAL_V1",
            "authority": "ZERO",
            "job_id": job.job_id,
            "request_hash": job.request_hash,
            "snapshot_ref": job.brief.body["snapshot_ref"],
            "status": status,
            "reason": reason,
            "recorded_at_ns": now_ns,
            "result": result,
            "memo_ref": memo_ref,
            "calendar_slot_id": job.brief.body["slot_id"],
            "calendar_denominator": 1,
            "paid_dispatch": False,
        }
        with self.repository.atomic_composition():
            self._persist(_identity(job.job_id, "terminal"), "AnalysisResearchTerminalV1", body, now_ns=now_ns)
            # If scheduling failed pre-dispatch this is an idempotent no-op on absent work.
            self.repository.retire_due_work(RESEARCH_LANE, job.job_id, reason_code=status)
        return FrozenMap(body)

    def memo(self, job: ResearchJobV1, *, now_ns: int) -> Mapping[str, Any] | None:
        key = _memo_key(job)
        # One fixed invalidation sentinel preserves the first invalidator forever.
        invalid = self._read(sha256_json({"version": "RESEARCH_MEMO_INVALID_V1", "memo_key": key}))
        if invalid is not None and invalid["available_at_ns"] <= now_ns:
            return None
        memo = self._read(key)
        if memo is None or memo["available_at_ns"] > now_ns or now_ns >= memo["deadline_ns"]:
            return None
        if memo["brief_ref"] != job.brief.content_hash or memo["model_ref"] != job.capabilities.content_hash:
            return None
        return memo

    def invalidate(self, job: ResearchJobV1, *, source_ref: str, cause: str, now_ns: int) -> str:
        self._writer()
        sha256_ref(source_ref, field="invalidator.source_ref")
        if cause not in INVALIDATORS:
            raise ValueError("unregistered invalidator")
        source = self.repository.get_artifact(source_ref)
        if source is None or source.available_at_ns > now_ns:
            raise ValueError("invalidator unavailable")
        key = _memo_key(job)
        ref = sha256_json({"version": "RESEARCH_MEMO_INVALID_V1", "memo_key": key})
        origin_ref = sha256_json(
            {
                "version": "RESEARCH_INVALIDATION_EVENT_IDENTITY_V1",
                "memo_key": key,
                "source_ref": source_ref,
                "cause": cause,
            }
        )
        body = {
            "version": "RESEARCH_MEMO_INVALID_V1",
            "authority": "ZERO",
            "memo_key": key,
            "source_ref": source_ref,
            "cause": cause,
            "available_at_ns": now_ns,
        }
        with self.repository.atomic_composition():
            if self._read(ref) is None:
                self._persist(ref, "AnalysisResearchInvalidationV1", body, now_ns=now_ns)
            if self._read(origin_ref) is None:
                self._persist(origin_ref, "AnalysisResearchInvalidationEventV1", body, now_ns=now_ns)
        return ref

    def run_fake(
        self,
        job: ResearchJobV1,
        provider: FakeResearchProvider,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        fault: Callable[[str], None] | None = None,
    ) -> Mapping[str, Any]:
        self._writer()
        if type(provider) is not FakeResearchProvider or job.capabilities.provider_id != "fixture":
            raise ValueError("only fake local provider is authorized")
        old = self.request(job.job_id)
        if old is None or old["request_hash"] != job.request_hash:
            raise ValueError("immutable research request must be scheduled first")
        now = clock_ns()
        checked_refs = (*(_identity(job.job_id, stage) for stage in ("request", "intent", "terminal")), _scope_key(job))
        for checked_ref in checked_refs:
            row = self.repository.get_artifact(checked_ref)
            if row is not None and row.available_at_ns > now:
                # Read disposition only; original immutable terminal is unchanged.
                return FrozenMap(
                    {
                        "status": "UNKNOWN",
                        "reason": "PERSISTED_STATE_NOT_AVAILABLE",
                        "job_id": job.job_id,
                        "request_hash": job.request_hash,
                        "authority": "ZERO",
                        "audit_only": True,
                        "paid_dispatch": False,
                    }
                )
        terminal = self._read(_identity(job.job_id, "terminal"))
        if terminal is not None:
            return terminal
        intent = self._read(_identity(job.job_id, "intent"))
        if intent is not None:
            return self._terminal(job, "UNKNOWN", "UNCERTAIN_DISPATCH_NO_RETRY", now_ns=now)
        invalid = self._read(sha256_json({"version": "RESEARCH_MEMO_INVALID_V1", "memo_key": _memo_key(job)}))
        if invalid is not None and invalid["available_at_ns"] <= now:
            return self._terminal(job, "LLM_NOT_CALLED", "SNAPSHOT_INVALIDATED", now_ns=now)
        denied = preflight_reason(job, now)
        if denied:
            return self._terminal(job, *denied, now_ns=now)
        memo = self.memo(job, now_ns=now)
        if memo is not None:
            return self._terminal(job, "MEMO_REUSED", None, now_ns=now, memo_ref=_memo_key(job))
        scoped = self._read(_scope_key(job))
        if scoped is not None:
            source_terminal = self._read(_identity(scoped["job_id"], "terminal"))
            prior_invalid = self._read(
                sha256_json({"version": "RESEARCH_MEMO_INVALID_V1", "memo_key": scoped["memo_key"]})
            )
            if prior_invalid is not None and prior_invalid["available_at_ns"] <= now:
                return self._terminal(job, "LLM_NOT_CALLED", "SNAPSHOT_INVALIDATED", now_ns=now)
            if (
                source_terminal is not None
                and source_terminal["status"] == "VALID_SHADOW"
                and self.repository.get_artifact(_identity(scoped["job_id"], "terminal")).available_at_ns <= now
            ):
                return self._terminal(job, "MEMO_REUSED", None, now_ns=now, memo_ref=scoped["memo_key"])
            return self._terminal(job, "UNKNOWN", "SCOPED_DISPATCH_NO_RETRY", now_ns=now)
        if fault:
            fault("BEFORE_INTENT")
        with self.repository.atomic_composition():
            self._persist(
                _scope_key(job),
                "AnalysisResearchScopeIntentV1",
                {
                    "version": "RESEARCH_DISPATCH_SCOPE_V1",
                    "authority": "ZERO",
                    "job_id": job.job_id,
                    "request_hash": job.request_hash,
                    "memo_key": _memo_key(job),
                    "paid_dispatch": False,
                    "available_at_ns": now,
                },
                now_ns=now,
            )
            self._persist(
                _identity(job.job_id, "intent"),
                "AnalysisResearchFakeIntentV1",
                {
                    "version": "RESEARCH_FAKE_INTENT_V1",
                    "authority": "ZERO",
                    "job_id": job.job_id,
                    "request_hash": job.request_hash,
                    "persisted_at_ns": now,
                    "paid_dispatch": False,
                },
                now_ns=now,
            )
        if fault:
            fault("AFTER_INTENT")
        result = run_fake_research_job(job, provider, clock_ns=clock_ns)
        if fault:
            fault("AFTER_RESPONSE")
        received = clock_ns()
        if result.status == "VALID_SHADOW" and received >= job.brief.body["deadline_ns"]:
            from dataclasses import replace

            result = replace(
                result, status="EXPIRED", reason="LATE_PERSISTENCE", decision=None, received_at_ns=received
            )
        with self.repository.atomic_composition():
            terminal = self._terminal(
                job, result.status, result.reason, now_ns=received, result=json_value(result.to_dict())
            )
            if result.status == "VALID_SHADOW":
                key = _memo_key(job)
                if self._read(key) is None:
                    self._persist(
                        key,
                        "AnalysisResearchMemoV1",
                        {
                            "version": "RESEARCH_MEMO_V1",
                            "authority": "ZERO",
                            "brief_ref": job.brief.content_hash,
                            "model_ref": job.capabilities.content_hash,
                            "cutoff_ns": job.brief.body["cutoff_ns"],
                            "deadline_ns": job.brief.body["deadline_ns"],
                            "available_at_ns": received,
                            "source_terminal_ref": _identity(job.job_id, "terminal"),
                            "result_ref": result.content_hash,
                        },
                        now_ns=received,
                    )
            if fault:
                fault("BEFORE_RESULT_COMMIT")
        if fault:
            fault("AFTER_RESULT_COMMIT")
        return terminal

    def record_late_result(self, job: ResearchJobV1, result: Mapping[str, Any], *, now_ns: int) -> str:
        """Retain a late/duplicate result as audit only; never change terminal UNKNOWN."""
        self._writer()
        request = self.request(job.job_id)
        if request is None or request["request_hash"] != job.request_hash:
            raise ValueError("late result request binding")
        if result.get("job_id") != job.job_id or result.get("request_hash") != job.request_hash:
            raise ValueError("late result identity mismatch")
        from atlas.v2._serialization import canonical_json

        if len(canonical_json(result).encode("utf-8")) > job.budget.max_tool_bytes:
            raise ValueError("late result exceeds bounded evidence budget")
        body = {
            "version": "RESEARCH_LATE_RESULT_V1",
            "authority": "ZERO",
            "job_id": job.job_id,
            "request_hash": job.request_hash,
            "result": result,
            "disposition": "AUDIT_ONLY_NO_AUTHORITY",
            "available_at_ns": now_ns,
        }
        ref = sha256_json(
            {"version": "RESEARCH_LATE_RESULT_IDENTITY_V1", "job_id": job.job_id, "result_hash": sha256_json(result)}
        )
        if self._read(ref) is None:
            self._persist(ref, "AnalysisResearchLateResultV1", body, now_ns=now_ns)
        return ref

    def due(self, *, now_ns: int, limit: int = 8) -> tuple[Mapping[str, Any], ...]:
        """Read-only bounded projection; invoking this never dispatches a model."""
        rows = self.repository.due_work_items(RESEARCH_LANE, as_of_ns=now_ns, limit=limit)
        return tuple(
            (
                self._read(row.source_ref)
                if self.repository.get_artifact(row.source_ref) is not None
                and self.repository.get_artifact(row.source_ref).available_at_ns <= now_ns
                else None
            )
            or FrozenMap(
                {
                    "status": "UNKNOWN",
                    "reason": "DUE_SOURCE_MISSING",
                    "source_ref": row.source_ref,
                    "work_id": row.work_id,
                    "authority": "ZERO",
                }
            )
            for row in rows
        )
