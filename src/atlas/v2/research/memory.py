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

from atlas.v2._serialization import FrozenMap, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.agent import FakeResearchProvider, ResearchJobV1, preflight_reason, run_fake_research_job

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
