"""Provider-neutral proposed-role contracts with a deliberately fake-only runner.

Nothing here calls a provider, grants admission, mutates a plan, or changes the
frozen critic/extractor. Retrieved source text is untrusted data in typed fields.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from atlas.v2._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from atlas.v2.research.decision_brief import TOOLS, DecisionBriefV1

TOOL_VERSION = "RESEARCH_READ_TOOLS_V1"


@dataclass(frozen=True)
class ProviderCapabilitiesV1:
    version: str
    provider_id: str
    model_id: str
    structured_output: bool
    bounded_tools: bool
    returned_identity_verified: bool
    cancellation_verified: bool
    token_accounting_verified: bool
    pricing_verified: bool
    context_limit: int | None
    images: bool
    rate_constraints: str
    retention_constraints: str
    verification_ref: str | None
    fallback: bool = False

    def __post_init__(self) -> None:
        for n in ("version", "provider_id", "model_id", "rate_constraints", "retention_constraints"):
            nonblank(getattr(self, n), field=n)
        for n in (
            "structured_output",
            "bounded_tools",
            "returned_identity_verified",
            "cancellation_verified",
            "token_accounting_verified",
            "pricing_verified",
            "images",
            "fallback",
        ):
            if type(getattr(self, n)) is not bool:
                raise ValueError("capability must be explicitly verified boolean")
        if self.fallback:
            raise ValueError("provider fallback is forbidden")
        if self.context_limit is not None and (type(self.context_limit) is not int or self.context_limit <= 0):
            raise ValueError("invalid context limit")
        if self.verification_ref is not None:
            sha256_ref(self.verification_ref, field="verification_ref")

    @property
    def content_hash(self) -> str:
        return sha256_json(asdict(self))


@dataclass(frozen=True)
class ResearchBudgetV1:
    version: str
    max_model_calls: int
    max_tool_rounds: int
    max_tool_rows: int
    max_tool_bytes: int
    max_projected_tokens: int

    def __post_init__(self) -> None:
        nonblank(self.version, field="budget.version")
        for n in ("max_model_calls", "max_tool_rounds", "max_tool_rows", "max_tool_bytes", "max_projected_tokens"):
            if type(getattr(self, n)) is not int or getattr(self, n) < 0:
                raise ValueError("invalid resource budget")
        if self.max_tool_rounds > 1 or self.max_model_calls > 2:
            raise ValueError("initial fake research configuration permits one supplementary round")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchJobV1:
    job_id: str
    brief: DecisionBriefV1
    capabilities: ProviderCapabilitiesV1
    budget: ResearchBudgetV1
    created_at_ns: int

    def __post_init__(self) -> None:
        nonblank(self.job_id, field="job_id")
        timestamp(self.created_at_ns, field="created_at_ns")
        if self.brief.body.get("authority") != "ZERO" or self.brief.body.get("shadow_only") is not True:
            raise ValueError("research job requires zero-authority shadow brief")

    @property
    def request_hash(self) -> str:
        return sha256_json(
            {
                "version": "RESEARCH_JOB_V1",
                "job_id": self.job_id,
                "brief_ref": self.brief.content_hash,
                "capabilities": self.capabilities.content_hash,
                "budget": self.budget.to_dict(),
                "created_at_ns": self.created_at_ns,
            }
        )


@dataclass(frozen=True)
class ToolRequestV1:
    job_id: str
    snapshot_ref: str
    candidate_scope: tuple[str, ...]
    cutoff_ns: int
    deadline_ns: int
    allowed_tool_name: str
    tool_version: str
    validated_parameters: FrozenMap
    resource_budget: str

    def __post_init__(self) -> None:
        for name in ("job_id", "allowed_tool_name", "tool_version"):
            nonblank(getattr(self, name), field=name)
        for name in ("snapshot_ref", "resource_budget"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("cutoff_ns", "deadline_ns"):
            timestamp(getattr(self, name), field=name)
        scope = tuple(self.candidate_scope)
        if scope != tuple(sorted(set(scope))):
            raise ValueError("tool candidate scope must be sorted and unique")
        for ref in scope:
            nonblank(ref, field="candidate_scope")
        object.__setattr__(self, "candidate_scope", scope)
        object.__setattr__(self, "validated_parameters", FrozenMap(self.validated_parameters))


@dataclass(frozen=True)
class ToolReceiptV1:
    status: str
    tool_name: str
    content: FrozenMap
    input_refs: tuple[str, ...]
    actual_available_at_ns: int | None
    latency_ns: int
    bytes_returned: int
    missingness: tuple[str, ...]
    request_ref: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "tool_name": self.tool_name,
            "content": self.content.to_dict(),
            "input_refs": list(self.input_refs),
            "actual_available_at_ns": self.actual_available_at_ns,
            "latency_ns": self.latency_ns,
            "bytes_returned": self.bytes_returned,
            "missingness": list(self.missingness),
            "request_ref": self.request_ref,
            "content_hash": self.content_hash,
        }

    @property
    def content_hash(self) -> str:
        # Content identity is replayable; actual run latency is separate telemetry.
        return sha256_json(
            {
                "version": TOOL_VERSION,
                "status": self.status,
                "content": self.content,
                "input_refs": self.input_refs,
                "available_at_ns": self.actual_available_at_ns,
                "missingness": self.missingness,
                "request_ref": self.request_ref,
            }
        )


class ReadOnlyResearchGatewayV1:
    """Only sealed brief projections. No repository, path, SQL, network or code port."""

    def __init__(self, job: ResearchJobV1) -> None:
        self.job = job
        self.rounds = 0
        self.bytes_used = 0

    def inspect(self, request: ToolRequestV1, *, now_ns: int) -> ToolReceiptV1:
        started = time.perf_counter_ns()
        job, body = self.job, self.job.brief.body
        missing: list[str] = []
        scope = tuple(sorted(body.get("shortlist", ())))
        if request.allowed_tool_name not in TOOLS or request.allowed_tool_name not in body.get("tool_allowlist", ()):
            missing.append("UNSUPPORTED_TOOL")
        if (
            request.job_id != job.job_id
            or request.snapshot_ref != body["snapshot_ref"]
            or request.candidate_scope != scope
            or request.cutoff_ns != body["cutoff_ns"]
            or request.deadline_ns != body["deadline_ns"]
            or request.tool_version != TOOL_VERSION
            or request.resource_budget != sha256_json(job.budget.to_dict())
        ):
            missing.append("INVALID_SCOPE")
        parameters = request.validated_parameters
        if set(parameters) - {"candidate_id", "section"}:
            missing.append("INVALID_PARAMETERS")
        candidate = parameters.get("candidate_id")
        if candidate is not None and candidate not in scope:
            missing.append("INVALID_SCOPE")
        section = parameters.get("section")
        allowed_sections = {
            "completed_candles",
            "technical",
            "structure",
            "liquidity",
            "derivatives",
            "source_health",
            "btc_eth",
            "events",
            "prior_analysis",
            "watches",
            "invalidators",
            "due_work",
            "outcomes",
        }
        if section is not None and section not in allowed_sections:
            missing.append("INVALID_PARAMETERS")
        if now_ns < job.created_at_ns:
            missing.append("JOB_NOT_YET_AVAILABLE")
        if now_ns >= body["deadline_ns"]:
            missing.append("DEADLINE_EXPIRED")
        if self.rounds >= job.budget.max_tool_rounds:
            missing.append("BUDGET_DENIED")
        rows: list[Any] = []
        if not missing:
            if request.allowed_tool_name == "inspect_economics_v1":
                rows = [
                    e
                    for e in body.get("economics", ())
                    if e["candidate_id"] in scope and (candidate is None or e["candidate_id"] == candidate)
                ]
            else:
                memory = request.allowed_tool_name == "inspect_memory_v1"
                memory_sections = {"events", "prior_analysis", "watches", "invalidators", "due_work", "outcomes"}
                rows = [
                    f
                    for f in body.get("facts", ())
                    if (f["section"] in memory_sections) == memory and (section is None or f["section"] == section)
                ]
            if request.allowed_tool_name == "inspect_economics_v1":
                if any(
                    row.get("information_cutoff_ns", body["cutoff_ns"]) != body["cutoff_ns"]
                    or row.get("available_at_ns", job.created_at_ns) > now_ns
                    for row in rows
                ):
                    missing.append("FUTURE_TOOL_SOURCE")
            elif any("available_at_ns" in row and row["available_at_ns"] > body["cutoff_ns"] for row in rows):
                missing.append("FUTURE_TOOL_SOURCE")
            encoded = canonical_json(rows).encode("utf-8")
            if len(rows) > job.budget.max_tool_rows or len(encoded) + self.bytes_used > job.budget.max_tool_bytes:
                missing.append("BUDGET_DENIED")
        self.rounds += 1
        if missing:
            rows = []
        content = FrozenMap(
            {
                "source_text_trust": "UNTRUSTED_DATA",
                "rows": rows,
                "job_id": job.job_id,
                "snapshot_ref": body["snapshot_ref"],
                "cutoff_ns": body["cutoff_ns"],
            }
        )
        size = len(canonical_json(content).encode("utf-8"))
        if not missing and size + self.bytes_used > job.budget.max_tool_bytes:
            missing = ["BUDGET_DENIED"]
            content = FrozenMap({"rows": [], "source_text_trust": "UNTRUSTED_DATA"})
            size = len(canonical_json(content).encode("utf-8"))
        self.bytes_used += size
        refs = tuple(sorted({row.get("source_ref") for row in rows if row.get("source_ref")}))
        if request.allowed_tool_name == "inspect_economics_v1" and not missing:
            refs = tuple(sorted({ref for row in rows for ref in row.get("evidence_refs", {}).values()}))
        available = max(
            (row.get("available_at_ns", body.get("built_at_ns", job.created_at_ns)) for row in rows), default=None
        )
        return ToolReceiptV1(
            "DENIED" if missing else "OK" if rows else "MISSING",
            request.allowed_tool_name,
            content,
            refs,
            available,
            time.perf_counter_ns() - started,
            size,
            tuple(sorted(set(missing))) if missing else () if rows else ("NO_REGISTERED_EVIDENCE",),
            sha256_json(asdict(request)),
        )


class OpportunityResearchProviderV1(Protocol):
    """Future adapter seam. Implementing a real provider requires separate authority."""

    def respond(self, job: ResearchJobV1, receipts: tuple[ToolReceiptV1, ...]) -> Mapping[str, Any] | ToolRequestV1: ...


class FakeResearchProvider:
    def __init__(
        self, scripted: Sequence[Mapping[str, Any] | ToolRequestV1 | Exception], *, returned_model: str = "fixture-v1"
    ) -> None:
        self.scripted = tuple(scripted)
        self.returned_model = returned_model
        self.calls = 0

    def respond(self, job: ResearchJobV1, receipts: tuple[ToolReceiptV1, ...]) -> Mapping[str, Any] | ToolRequestV1:
        index = self.calls
        self.calls += 1
        if index >= len(self.scripted):
            raise RuntimeError("fake script exhausted")
        result = self.scripted[index]
        if isinstance(result, Exception):
            raise result
        return result


@dataclass(frozen=True)
class ResearchResultV1:
    job_id: str
    request_hash: str
    status: str
    reason: str | None
    decision: FrozenMap | None
    tool_receipts: tuple[ToolReceiptV1, ...]
    received_at_ns: int
    returned_model: str | None
    authority: str = "ZERO"

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": "FAKE_RESEARCH_RESULT_V1",
            "job_id": self.job_id,
            "request_hash": self.request_hash,
            "status": self.status,
            "reason": self.reason,
            "decision": self.decision,
            "tool_receipts": [r.to_dict() for r in self.tool_receipts],
            "received_at_ns": self.received_at_ns,
            "returned_model": self.returned_model,
            "authority": self.authority,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def preflight_reason(job: ResearchJobV1, now_ns: int) -> tuple[str, str] | None:
    body, caps = job.brief.body, job.capabilities
    if now_ns < job.created_at_ns:
        return "LLM_NOT_CALLED", "JOB_NOT_YET_AVAILABLE"
    if now_ns >= body["deadline_ns"]:
        return "EXPIRED", "DEADLINE_EXPIRED"
    if job.brief.status != "COMPLETE" or not body.get("shortlist"):
        return "LLM_NOT_CALLED", "NO_SUPPORTED_CHOICE"
    if not caps.pricing_verified:
        return "BUDGET_DENIED", "UNKNOWN_PRICE"
    if (
        caps.verification_ref is None
        or not caps.structured_output
        or not caps.returned_identity_verified
        or not caps.cancellation_verified
        or not caps.token_accounting_verified
        or caps.context_limit is None
    ):
        return "LLM_NOT_CALLED", "UNVERIFIED_CAPABILITY"
    if body.get("image_refs") and not caps.images:
        return "LLM_NOT_CALLED", "UNVERIFIED_IMAGE_CAPABILITY"
    projected = len(canonical_json(job.brief.to_dict()).encode("utf-8"))
    if projected > min(caps.context_limit, job.budget.max_projected_tokens) or job.budget.max_model_calls < 1:
        return "BUDGET_DENIED", "BUDGET_EXHAUSTED"
    return None


def validate_response(job: ResearchJobV1, output: Any) -> str | None:
    fields = {"job_id", "snapshot_ref", "cutoff_ns", "kind", "candidate_id", "action_hash", "evidence_refs"}
    if not isinstance(output, Mapping) or set(output) != fields:
        return "MALFORMED_RESULT"
    body = job.brief.body
    if output["snapshot_ref"] != body["snapshot_ref"]:
        return "STALE_SNAPSHOT"
    if (
        output["job_id"] != job.job_id
        or type(output["cutoff_ns"]) is not int
        or output["cutoff_ns"] != body["cutoff_ns"]
    ):
        return "JOB_BINDING"
    if output["kind"] not in ("SELECT_ONE", "REJECT_ALL", "ABSTAIN"):
        return "MALFORMED_RESULT"
    refs = output["evidence_refs"]
    allowed_refs = {ref for row in body.get("economics", ()) for ref in row.get("evidence_refs", {}).values()}
    allowed_refs.update(f["source_ref"] for f in body.get("facts", ()))
    if (
        not isinstance(refs, (list, tuple))
        or len(refs) > 16
        or any(not isinstance(r, str) or r not in allowed_refs for r in refs)
    ):
        return "EVIDENCE_SCOPE"
    if output["kind"] == "SELECT_ONE":
        allowed = {
            (e["candidate_id"], e["action_hash"])
            for e in body.get("economics", ())
            if e.get("supported_choice") is True and e["candidate_id"] in body.get("shortlist", ())
        }
        if (
            not isinstance(output["candidate_id"], str)
            or not isinstance(output["action_hash"], str)
            or (output["candidate_id"], output["action_hash"]) not in allowed
        ):
            return "CANDIDATE_ACTION_SCOPE"
    elif output["candidate_id"] is not None or output["action_hash"] is not None:
        return "MALFORMED_RESULT"
    return None


def run_fake_research_job(
    job: ResearchJobV1, provider: FakeResearchProvider, *, clock_ns: Callable[[], int] = time.time_ns
) -> ResearchResultV1:
    if type(provider) is not FakeResearchProvider or job.capabilities.provider_id != "fixture":
        raise ValueError("S47 permits only the local fake provider")
    receipts: list[ToolReceiptV1] = []
    now = clock_ns()

    def result(status: str, reason: str | None = None, decision: Mapping[str, Any] | None = None) -> ResearchResultV1:
        received_at = clock_ns()
        if status == "VALID_SHADOW" and received_at >= job.brief.body["deadline_ns"]:
            status, reason, decision = "EXPIRED", "LATE_RESPONSE", None
        return ResearchResultV1(
            job.job_id,
            job.request_hash,
            status,
            reason,
            FrozenMap(decision) if decision is not None else None,
            tuple(receipts),
            received_at,
            provider.returned_model if provider.calls else None,
        )

    denied = preflight_reason(job, now)
    if denied:
        return result(*denied)
    gateway = ReadOnlyResearchGatewayV1(job)
    for attempt in range(job.budget.max_model_calls):
        if clock_ns() >= job.brief.body["deadline_ns"]:
            return result("EXPIRED", "DEADLINE_EXPIRED")
        try:
            output = provider.respond(job, tuple(receipts))
        except Exception:
            # Persisted caller must retain uncertainty; no automatic second attempt.
            return result("UNKNOWN", "PROVIDER_FAILURE")
        if provider.returned_model != job.capabilities.model_id:
            return result("INVALID", "RETURNED_MODEL_IDENTITY")
        if clock_ns() >= job.brief.body["deadline_ns"]:
            return result("EXPIRED", "LATE_RESPONSE")
        if isinstance(output, ToolRequestV1):
            if not job.capabilities.bounded_tools:
                return result("INVALID", "UNVERIFIED_CAPABILITY")
            receipt = gateway.inspect(output, now_ns=clock_ns())
            receipts.append(receipt)
            if receipt.status == "DENIED":
                reason = receipt.missingness[0]
                return result("BUDGET_DENIED" if "BUDGET_DENIED" in receipt.missingness else "INVALID", reason)
            # Host scheduler authorizes a supplementary fake result round only
            # within remaining rounds, bytes, token projection and original deadline.
            growth = len(canonical_json([r.to_dict() for r in receipts]).encode("utf-8"))
            if attempt + 1 >= job.budget.max_model_calls or len(
                canonical_json(job.brief.to_dict()).encode("utf-8")
            ) + growth > min(job.budget.max_projected_tokens, job.capabilities.context_limit or 0):
                return result("BUDGET_DENIED", "BUDGET_EXHAUSTED")
            continue
        validation_reason = validate_response(job, output)
        return result("INVALID", validation_reason) if validation_reason else result("VALID_SHADOW", decision=output)
    return result("BUDGET_DENIED", "BUDGET_EXHAUSTED")


def unverified_provider_capabilities(provider_id: str, model_id: str) -> ProviderCapabilitiesV1:
    """Describe an inert future adapter; compatibility is never inferred from a name."""
    return ProviderCapabilitiesV1(
        "UNVERIFIED_RESEARCH_PROVIDER_V1",
        provider_id,
        model_id,
        False,
        False,
        False,
        False,
        False,
        False,
        None,
        False,
        "UNVERIFIED",
        "UNVERIFIED",
        None,
    )


@dataclass(frozen=True)
class FutureCheapScreenRequestV1:
    """A quarantined, single-instrument news lead for a distinct future slot.

    Construction neither schedules work nor inserts a candidate into a sealed
    slot. A future deterministic screening policy must validate it separately.
    """

    origin_job_id: str
    source_event_ref: str
    source_available_at_ns: int
    instrument_key_ref: str
    origin_cutoff_ns: int
    future_slot_id: str
    future_cutoff_ns: int
    future_deadline_ns: int

    def __post_init__(self) -> None:
        for name in ("origin_job_id", "future_slot_id"):
            nonblank(getattr(self, name), field=name)
        for name in ("source_event_ref", "instrument_key_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("source_available_at_ns", "origin_cutoff_ns", "future_cutoff_ns", "future_deadline_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.source_available_at_ns <= self.origin_cutoff_ns < self.future_cutoff_ns < self.future_deadline_ns:
            raise ValueError("news screening must be causal and use a distinct future cutoff")

    @property
    def content_hash(self) -> str:
        return sha256_json({"version": "FUTURE_CHEAP_SCREEN_PROPOSAL_V1", "authority": "ZERO", **asdict(self)})
