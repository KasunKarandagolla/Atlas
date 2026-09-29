"""Session-028 offline tests for additive DeepSeek provider/model support."""

from __future__ import annotations

import ast
import base64
import hashlib
import hmac
import json
import sqlite3
import sys
import time
import uuid
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json
from atlas.v2.agent_intelligence.broker import (
    BrokerCapabilityV1,
    BrokerCapabilityV2,
    BrokerProtocolError,
    InferenceBroker,
)
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1, ProviderPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import (
    AgentEvidenceRefV1,
    AgentJobStateV1,
    AgentModelProfileV1,
    AgentModelProfileV2,
    BrokerDispatchAuthorizationV1,
    BrokerDispatchAuthorizationV2,
    ProviderResultV1,
    ResearchProposalRequestV1,
    RevisionStatusV1,
)
from atlas.v2.agent_intelligence.controller import ResearchJobController
from atlas.v2.agent_intelligence.evidence import BoundedResearchReadService
from atlas.v2.agent_intelligence.persistence import AGENT_NAMESPACE_WRITER_OWNER, AgentJobRepository
from atlas.v2.agent_intelligence.profile import deepseek_v41_flash_model_profile, initial_model_profile
from atlas.v2.agent_intelligence.provider import (
    SYSTEM_PROMPT_V1,
    DeepSeekResponsesResearchProposalProvider,
    ResearchProviderUnavailable,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.discovery import DISCOVERY_LAB_HASH
from tests.v2.test_session016_candidate_selection import CUTOFF
from tests.v2.test_session023_discovery_s8 import experiment as make_experiment

ROOT = Path(__file__).resolve().parents[2]
DEEPSEEK_PRICING = ROOT / "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json"
OPENAI_PRICING = ROOT / "configs/agent_intelligence/provider_pricing_v1.json"
AGENT_LOCK = ROOT / "requirements-agent-lock.txt"
AGENT_FREEZE_HASH = "d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d"
OPENAI_PRICE_FILE_HASH = "f55b39ee654ea08836de76b10126b34ac7f5e70c9c1d9aebacb7b0db5c4073ef"
OPENAI_LOCK_HASH = "47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3"
GRAMMAR = ("AND", "OR", "GT", "GTE", "LT", "LTE", "EQ", "RISING", "FALLING", "CROSS_ABOVE", "CROSS_BELOW")
REASONING_SENTINEL = "FAKE-REASONING-CONTENT-MUST-NEVER-PERSIST"


def _proposal_wire(request: ResearchProposalRequestV1) -> dict[str, Any]:
    experiment_ref = request.experiment_ref
    return {
        "version": "RESEARCH_PROPOSAL_V1",
        "research_family_id": request.research_family_id,
        "proposal_id": "session028-proposal",
        "proposal_version": 1,
        "causal_hypothesis": "A registered feature may identify a falsifiable development slice.",
        "proposed_rule": {"operator": "GTE", "feature_family": "candles", "feature_name": "close_return",
                          "threshold": "0.010", "children": []},
        "feature_dependencies": ["candles/close_return"],
        "evidence_refs": [experiment_ref, request.baseline_policy_ref],
        "availability_requirements": ["candles/close_return:PREDECISION_REQUIRED", "MISSINGNESS=EXPLICIT"],
        "falsifier": "The relationship disappears under chronological validation after costs.",
        "target_population": "Registered instruments with sufficient causal history.",
        "horizon": "The preregistered development horizon.",
        "cost_semantics": "Apply registered spread, fee and funding costs.",
        "intended_ablation": "Compare against the unchanged registered baseline.",
        "development_slices": [experiment_ref],
        "known_failed_predecessors": [],
        "proposal_lineage": [experiment_ref],
        "requested_deterministic_followup_evaluation_type": "DEVELOPMENT_WALK_FORWARD_REPLAY",
    }


class _DeepSeekFixture:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.ops = OpsRepository(path)
        self.experiment = make_experiment(self.ops, budget=4, parameter_budget=4)
        self.read_ops = OpsRepository(path, read_only=True)
        self.evidence = BoundedResearchReadService(self.read_ops)
        self.schedule = DeepSeekPriceScheduleV1.load(DEEPSEEK_PRICING)
        self.profile = deepseek_v41_flash_model_profile(price_schedule=self.schedule, agent_lock_path=AGENT_LOCK)
        self.jobs = AgentJobRepository(path, self.schedule)
        self.jobs.register_model_profile(self.profile, created_at_ns=CUTOFF)
        self.now_ns = time.time_ns()

    def evidence_ref(self, tool_name: str, artifact_ref: str) -> AgentEvidenceRefV1:
        return AgentEvidenceRefV1(tool_name, artifact_ref, CUTOFF + 100_000)

    def request(self, **changes: Any) -> ResearchProposalRequestV1:
        request = ResearchProposalRequestV1(
            request_id=str(uuid.uuid4()), research_family_id=self.experiment.family_id,
            experiment_ref=self.experiment.content_hash, preregistration_ref=self.experiment.content_hash,
            development_cutoff_ns=CUTOFF + 100_000, outcome_maturity_cutoff_ns=CUTOFF + 99_000,
            allowed_feature_families=("candles",), allowed_operation_grammar=GRAMMAR,
            attempt_history_refs=(), remaining_attempt_budget=4, remaining_parameter_search_budget=4,
            multiplicity_family=self.experiment.multiplicity_family_id,
            baseline_policy_ref=self.experiment.baseline_policy_ref,
            cost_evaluation_target=FrozenMap({"metric": "net_after_registered_costs", "scope": "development"}),
            inaccessible_holdout_identities=(self.experiment.final_holdout_ref,),
            evidence_manifest=(
                self.evidence_ref("get_registered_artifact", self.experiment.content_hash),
                self.evidence_ref("inspect_failed_discovery_variants", self.experiment.content_hash),
                self.evidence_ref("get_registered_policy_or_model_manifest", self.experiment.baseline_policy_ref),
            ), prompt_contract_version="DISCOVERY_PROPOSER_PROMPT_V1",
            prompt_contract_hash=self.profile.prompt_contract_hash, model_profile_hash=self.profile.content_hash,
            tool_contract_version="DISCOVERY_READ_TOOLS_V1", tool_contract_hash=self.profile.tool_contract_hash,
            proposal_schema_version="RESEARCH_PROPOSAL_V1", schema_hash=self.profile.schema_hash,
            absolute_deadline_ns=self.now_ns + 100_000_000_000, max_model_calls=3,
            max_read_tool_calls=8, max_input_tokens=12_000, max_output_tokens=4_000,
            max_job_cost_usd="0.0252", daily_cost_budget_usd="5.25")
        return replace(request, **changes) if changes else request

    def close(self) -> None:
        self.jobs.close()
        self.read_ops.close()
        self.ops.close()


@pytest.fixture
def ds_env(tmp_path: Path):
    fixture = _DeepSeekFixture(tmp_path / "ops.sqlite")
    try:
        yield fixture
    finally:
        fixture.close()


class _FakeHTTPClient:
    def __init__(self, *, trust_env: bool) -> None:
        self.trust_env = trust_env


class _SDKStatusError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__("offline HTTP status fixture")
        self.status_code = status_code


class _FakeResponses:
    def __init__(self, outcome: Any, calls: list[dict[str, Any]]) -> None:
        self.outcome = outcome
        self.calls = calls

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def _sdk_response(request: ResearchProposalRequestV1, *, model: str = "deepseek-flash",
                  status: str = "completed", reason: str | None = None,
                  output_text: str | None = None) -> dict[str, Any]:
    items: list[dict[str, Any]] = [{"type": "reasoning", "content": [{"type": "reasoning_text",
        "text": REASONING_SENTINEL}]}]
    if output_text is not None:
        items.append({"type": "message", "role": "assistant", "status": "completed",
            "content": [{"type": "output_text", "text": output_text}]})
    return {"id": "resp_session028_fake", "model": model, "status": status,
        "incomplete_details": {"reason": reason} if reason else None, "output": items,
        "usage": {"input_tokens": 111, "output_tokens": 222}}


def _install_fake_sdk(monkeypatch: pytest.MonkeyPatch, outcome: Any):
    calls: list[dict[str, Any]] = []
    clients: list[dict[str, Any]] = []

    def http_client_factory(*, trust_env: bool):
        client = _FakeHTTPClient(trust_env=trust_env)
        clients.append({"http_client": client})
        return client

    class FakeOpenAI:
        def __init__(self, *, api_key: str, base_url: str, max_retries: int,
                     timeout: float, http_client: _FakeHTTPClient) -> None:
            # Keep only booleans/status; never retain or inspect any key characters.
            clients[-1].update({"credential_available": bool(api_key), "base_url": base_url,
                "max_retries": max_retries, "timeout": timeout, "trust_env": http_client.trust_env})
            self.responses = _FakeResponses(outcome, calls)

        async def close(self) -> None:
            return None

    monkeypatch.setitem(sys.modules, "httpx2", SimpleNamespace(AsyncClient=http_client_factory))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(AsyncOpenAI=FakeOpenAI))
    return calls, clients


def _provider(profile: AgentModelProfileV2) -> DeepSeekResponsesResearchProposalProvider:
    assert profile.provider == "deepseek"
    return DeepSeekResponsesResearchProposalProvider("offline-test-placeholder")


def _write_json(directory: Path, name: str, body: dict[str, Any]) -> Path:
    path = directory / name
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def test_deepseek_profile_wire_hash_endpoint_limits_and_peak_price(ds_env: _DeepSeekFixture) -> None:
    profile = ds_env.profile
    restored = AgentModelProfileV2.from_dict(profile.to_dict())
    schedule = ds_env.schedule
    assert restored == profile
    assert restored.content_hash == profile.content_hash
    assert profile.revision_status == RevisionStatusV1.ALIAS_ONLY
    assert profile.model_family == "DeepSeek-V4.1-Flash"
    assert profile.provider == "deepseek" and profile.requested_model_id == "deepseek-flash"
    assert profile.base_url == "https://api.deepseek.com" and profile.endpoint_path == "/responses"
    assert schedule.endpoint == "https://api.deepseek.com/responses"
    assert profile.reasoning_setting_id == "DEEPSEEK_RESPONSES_REASONING_EFFORT_HIGH_V1"
    assert profile.reasoning_settings.to_dict() == {"effort": "high"}
    assert (profile.max_input_tokens, profile.max_output_tokens) == (12_000, 4_000)
    assert (profile.max_model_calls_per_job, profile.max_read_tool_calls_per_job, profile.max_concurrent_jobs) == (3, 8, 1)
    assert schedule.worst_case_call_usd() == Decimal("0.008400")
    assert schedule.worst_case_call_usd(input_tokens=12_000, output_tokens=0) == Decimal("0.003600")
    assert schedule.cache_hit_input_usd_per_million == Decimal("0.006")
    assert schedule.cache_miss_input_usd_per_million == Decimal("0.30")
    assert schedule.output_usd_per_million == Decimal("1.20")
    assert schedule.worst_case_call_usd() * schedule.maximum_model_calls_per_job == Decimal("0.025200")
    with pytest.raises(ValueError, match="provider/model/endpoint"):
        replace(profile, requested_model_id="deepseek-v4-flash")
    with pytest.raises(ValueError, match="provider/model/endpoint"):
        replace(profile, base_url="https://arbitrary.example")
    with pytest.raises(TypeError):
        DeepSeekResponsesResearchProposalProvider("offline-test-placeholder", model_id="other")  # type: ignore[call-arg]


def test_unknown_deepseek_pricing_is_not_dispatchable(tmp_path: Path) -> None:
    body = json.loads(DEEPSEEK_PRICING.read_text(encoding="utf-8"))
    body["prices_usd_per_million_tokens"]["cache_miss_input"] = "0.01"
    altered = _write_json(tmp_path, "unknown-deepseek-pricing.json", body)
    with pytest.raises(ValueError, match="approved conservative peak rates"):
        DeepSeekPriceScheduleV1.load(altered)


def test_openai_astra_v1_profile_and_price_artifact_remain_unchanged(tmp_path: Path) -> None:
    schedule = ProviderPriceScheduleV1.load(OPENAI_PRICING)
    profile = initial_model_profile(price_schedule=schedule, agent_lock_path=AGENT_LOCK)
    restored = AgentModelProfileV1.from_dict(profile.to_dict())
    assert restored == profile
    assert restored.to_dict()["version"] == "AgentModelProfileV1"
    assert restored.provider == "openai" and restored.requested_model_id == "gpt-6-astra"
    assert restored.reasoning_settings.to_dict() == {"effort": "medium"}
    assert hashlib.sha256(OPENAI_PRICING.read_bytes()).hexdigest() == OPENAI_PRICE_FILE_HASH
    assert hashlib.sha256(AGENT_LOCK.read_bytes()).hexdigest() == OPENAI_LOCK_HASH
    assert restored.pricing_schedule_id == "OPENAI_GPT_6_ASTRA_STANDARD_SHORT_2026_09_V1"
    with pytest.raises(ValueError, match="pinned to OpenAI GPT-6 Astra"):
        replace(profile, requested_model_id="deepseek-flash")
    altered_schedule = json.loads(OPENAI_PRICING.read_text(encoding="utf-8"))
    altered_schedule["requested_model_id"] = "deepseek-flash"
    with pytest.raises(ValueError, match="not authorized"):
        ProviderPriceScheduleV1.load(_write_json(tmp_path, "altered-openai-pricing.json", altered_schedule))


def test_deepseek_responses_request_is_fixed_and_discards_reasoning(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    request = ds_env.request()
    final_json = json.dumps(_proposal_wire(request), separators=(",", ":"))
    calls, clients = _install_fake_sdk(monkeypatch, _sdk_response(request, output_text=final_json))
    provider = _provider(ds_env.profile)
    result = provider.propose(request, [])
    assert result.raw_output == final_json
    assert REASONING_SENTINEL not in result.raw_output
    assert result.returned_model_id == "deepseek-flash" and result.model_revision is None
    assert (result.input_tokens, result.output_tokens) == (111, 222)
    assert result.provider_request_id == "resp_session028_fake"
    assert len(calls) == 1 and len(clients) == 1
    assert clients[0] == {"http_client": clients[0]["http_client"], "credential_available": True,
        "base_url": "https://api.deepseek.com", "max_retries": 0, "trust_env": False,
        "timeout": pytest.approx(30.0)}
    assert clients[0]["http_client"].trust_env is False
    assert calls[0]["model"] == "deepseek-flash"
    assert calls[0]["reasoning"] == {"effort": "high"}
    assert calls[0]["max_output_tokens"] == 4_000
    assert calls[0]["text"]["format"]["type"] == "json_schema"
    assert calls[0]["text"]["format"]["schema"]
    assert "tools" not in calls[0]
    assert calls[0]["instructions"] == SYSTEM_PROMPT_V1


@pytest.mark.parametrize("limits", [{"max_input_tokens": 12_001}, {"max_output_tokens": 4_001}])
def test_deepseek_adapter_rejects_requests_above_exact_token_ceilings(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch, limits: dict[str, int]) -> None:
    request = replace(ds_env.request(), **limits)
    calls, _clients = _install_fake_sdk(monkeypatch, AssertionError("provider should not be called"))
    with pytest.raises(ResearchProviderUnavailable, match="TOKEN_OR_JOB_LIMIT_EXCEEDED"):
        _provider(ds_env.profile).propose(request, [])
    assert calls == []


def test_locked_openai_sdk_uses_exact_deepseek_responses_wire_without_network(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    openai = pytest.importorskip("openai")
    httpx2 = pytest.importorskip("httpx2")
    request = ds_env.request()
    final_json = json.dumps(_proposal_wire(request), separators=(",", ":"))
    requests: list[Any] = []

    def transport(incoming: Any) -> Any:
        assert str(incoming.url) == "https://api.deepseek.com/responses"
        wire = json.loads(incoming.content)
        requests.append(wire)
        response = {"id": "resp_sdk_mock", "object": "response", "created_at": 1,
            "status": "completed", "model": "deepseek-flash", "output": [{"type": "message",
                "id": "msg_sdk_mock", "status": "completed", "role": "assistant",
                "content": [{"type": "output_text", "text": final_json}]}],
            "usage": {"input_tokens": 123, "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 321, "output_tokens_details": {"reasoning_tokens": 11}, "total_tokens": 444},
            "store": False}
        return httpx2.Response(200, json=response, request=incoming)

    original_async_client = httpx2.AsyncClient

    class OfflineAsyncClient(original_async_client):
        def __init__(self, *, trust_env: bool, **kwargs: Any) -> None:
            super().__init__(trust_env=trust_env, transport=httpx2.MockTransport(transport), **kwargs)

    monkeypatch.setattr(httpx2, "AsyncClient", OfflineAsyncClient)
    assert openai.AsyncOpenAI
    result = _provider(ds_env.profile).propose(request, [])
    assert result.raw_output == final_json
    assert result.provider_request_id == "resp_sdk_mock"
    assert result.returned_model_id == "deepseek-flash"
    assert result.input_tokens == 123 and result.output_tokens == 321
    assert len(requests) == 1
    assert requests[0]["model"] == "deepseek-flash"
    assert requests[0]["reasoning"] == {"effort": "high"}
    assert requests[0]["max_output_tokens"] == 4_000
    assert requests[0]["text"]["format"]["type"] == "json_schema"
    assert "tools" not in requests[0]


@pytest.mark.parametrize(
    ("sdk_response", "expected"),
    [
        (lambda req: _sdk_response(req, status="incomplete", reason="content_filter"), "refusal"),
        (lambda req: _sdk_response(req, status="incomplete", reason="max_output_tokens"), "truncated"),
        (lambda req: _sdk_response(req, model="deepseek-v4-pro", output_text="{}"), "drift"),
        (lambda req: _sdk_response(req, output_text=None), "malformed"),
    ],
)
def test_refusal_truncation_model_drift_and_missing_output_fail_closed(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch, sdk_response: Any, expected: str) -> None:
    request = ds_env.request()
    _install_fake_sdk(monkeypatch, sdk_response(request))
    result = _provider(ds_env.profile).propose(request, [])
    assert result.raw_output == ""
    if expected == "refusal":
        assert result.refusal is True
    elif expected == "truncated":
        assert result.truncated is True
    elif expected == "drift":
        assert result.failure_code == "RETURNED_MODEL_ID_DRIFT"
        assert result.returned_model_id == "deepseek-v4-pro"
    else:
        assert result.failure_code == "MALFORMED_STRUCTURED_OUTPUT"


@pytest.mark.parametrize(("exception", "failure", "retryable"), [
    (TimeoutError("offline timeout"), "PROVIDER_TIMEOUT", True),
    (_SDKStatusError(429), "RATE_LIMITED", True),
    (_SDKStatusError(503), "PROVIDER_UNAVAILABLE", True),
])
def test_timeout_rate_limit_and_provider_failure_map_without_leaking_exception(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch,
        exception: Exception, failure: str, retryable: bool) -> None:
    request = ds_env.request()
    _install_fake_sdk(monkeypatch, exception)
    with pytest.raises(ResearchProviderUnavailable) as error:
        _provider(ds_env.profile).propose(request, [])
    assert error.value.code == failure and error.value.retryable is retryable
    assert "offline timeout" not in str(error.value)


def test_controller_persists_v2_authorization_before_capability_and_validates_only_final_json(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    request = ds_env.request()
    final_json = json.dumps(_proposal_wire(request), separators=(",", ":"))
    calls, _clients = _install_fake_sdk(monkeypatch, _sdk_response(request, output_text=final_json))
    provider = _provider(ds_env.profile)
    broker = InferenceBroker(provider, signing_key=b"d" * 32, model_profile=ds_env.profile,
                             price_schedule_hash=ds_env.schedule.content_hash)
    dispatches: list[str] = []
    issue = BrokerCapabilityV2.issue_authorized

    def check_persisted(*, authorization: Any, signing_key: bytes):
        row = ds_env.jobs._connection.execute(
            "SELECT dispatch_hash,model_profile_hash,attempt_index FROM agent_broker_dispatches WHERE attempt_id=?",
            (authorization.attempt_id,)).fetchone()
        assert row is not None
        assert row[0] == authorization.authorization_hash
        assert row[1] == ds_env.profile.content_hash
        assert row[2] == authorization.call_index
        dispatches.append(authorization.provider_binding_hash)
        return issue(authorization=authorization, signing_key=signing_key)

    monkeypatch.setattr(BrokerCapabilityV2, "issue_authorized", staticmethod(check_persisted))

    class BrokerPort:
        def __init__(self, context: Any, deadline: int) -> None:
            self.context = context
            self.deadline = deadline

        def propose(self, inner_request: ResearchProposalRequestV1, evidence: Any) -> ProviderResultV1:
            context = self.context
            return broker.infer(capability=context.capability, job_id=context.job_id,
                attempt_id=context.attempt_id, lease_epoch=context.lease_epoch,
                call_index=context.call_index, request_data=inner_request.to_dict(), evidence=evidence,
                now_ns=ds_env.now_ns + 10)

    controller = ResearchJobController(jobs=ds_env.jobs, evidence=ds_env.evidence, profile=ds_env.profile,
        capability_signing_key=b"d" * 32,
        provider_factory=lambda context, deadline: BrokerPort(context, deadline), now_ns=lambda: ds_env.now_ns)
    outcome = controller.run(request, job_id=str(uuid.uuid4()))
    assert outcome.lifecycle_state == AgentJobStateV1.VALIDATED and outcome.authoritative, (
        outcome.terminal_reason, outcome.validation_receipt.reasons if outcome.validation_receipt else None)
    assert outcome.proposal is not None
    assert len(dispatches) == 1 and dispatches[0] == ds_env.profile.provider_binding_hash
    assert len(calls) == 1
    assert REASONING_SENTINEL not in caplog.text
    assert REASONING_SENTINEL not in canonical_json(outcome.proposal.to_dict())
    assert outcome.validation_receipt is not None
    assert REASONING_SENTINEL not in canonical_json(outcome.validation_receipt.to_dict())
    with sqlite3.connect(ds_env.path) as connection:
        stored = "\n".join(row[0] for row in connection.execute(
            "SELECT result_json FROM agent_results").fetchall())
        stored += "\n" + "\n".join(row[0] for row in connection.execute(
            "SELECT outcome_json FROM agent_attempt_outcomes").fetchall())
        stored += "\n" + "\n".join(row[0] for row in connection.execute(
            "SELECT attempt_json FROM agent_attempts").fetchall())
        stored += "\n" + "\n".join(row[0] for row in connection.execute(
            "SELECT receipt_json FROM agent_validation_receipts").fetchall())
        assert REASONING_SENTINEL not in stored
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM agent_broker_dispatches").fetchone()[0] == 1


def test_malformed_final_json_remains_invalid(ds_env: _DeepSeekFixture,
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = ds_env.request()
    calls, _clients = _install_fake_sdk(monkeypatch, _sdk_response(request, output_text="{not-json"))
    broker = InferenceBroker(_provider(ds_env.profile), signing_key=b"m" * 32,
        model_profile=ds_env.profile, price_schedule_hash=ds_env.schedule.content_hash)

    class BrokerPort:
        def __init__(self, context: Any) -> None:
            self.context = context

        def propose(self, inner_request: ResearchProposalRequestV1, evidence: Any) -> ProviderResultV1:
            return broker.infer(capability=self.context.capability, job_id=self.context.job_id,
                attempt_id=self.context.attempt_id, lease_epoch=self.context.lease_epoch,
                call_index=self.context.call_index, request_data=inner_request.to_dict(), evidence=evidence,
                now_ns=ds_env.now_ns + 10)

    controller = ResearchJobController(jobs=ds_env.jobs, evidence=ds_env.evidence, profile=ds_env.profile,
        capability_signing_key=b"m" * 32, provider_factory=lambda context, _deadline: BrokerPort(context),
        now_ns=lambda: ds_env.now_ns)
    outcome = controller.run(request, job_id=str(uuid.uuid4()))
    assert outcome.lifecycle_state == AgentJobStateV1.INVALID and not outcome.authoritative
    assert len(calls) == 1
    with sqlite3.connect(ds_env.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0


def test_v2_capability_cross_provider_and_cross_profile_replay_fail_closed(
        ds_env: _DeepSeekFixture, tmp_path: Path) -> None:
    request = ds_env.request()
    job = ds_env.jobs.create_request(request, job_id=str(uuid.uuid4()), now_ns=ds_env.now_ns)
    leased = ds_env.jobs.lease(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER,
        now_ns=ds_env.now_ns, lease_ns=90_000_000_000)
    ds_env.jobs.start(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER,
        epoch=leased.lease_epoch, now_ns=ds_env.now_ns)
    attempt = ds_env.jobs.reserve_attempt(job.job_id, owner=AGENT_NAMESPACE_WRITER_OWNER,
        epoch=leased.lease_epoch, now_ns=ds_env.now_ns + 1)
    evidence = [{"tool_name": auth.tool_name, "artifact_ref": auth.artifact_ref, "cursor": auth.cursor,
        "status": "PRESENT", "rows": []} for auth in request.evidence_manifest]
    authorization = ds_env.jobs.authorize_broker_dispatch(job.job_id, request, attempt,
        owner=AGENT_NAMESPACE_WRITER_OWNER, lease_epoch=leased.lease_epoch,
        authorization_id=str(uuid.uuid4()), capability_nonce=str(uuid.uuid4()),
        evidence_hash=sha256_json(evidence), model_profile_hash=ds_env.profile.content_hash,
        expires_at_ns=ds_env.now_ns + 30_000_000_000, authorized_at_ns=ds_env.now_ns + 2)
    assert isinstance(authorization, BrokerDispatchAuthorizationV2)
    capability = BrokerCapabilityV2.issue_authorized(authorization=authorization,
        signing_key=b"x" * 32)
    fake = SimpleNamespace(provider_id="deepseek", requested_model_id="deepseek-flash",
        endpoint="https://api.deepseek.com/responses", propose=lambda *_args: pytest.fail("provider called"))
    broker = InferenceBroker(fake, signing_key=b"x" * 32, model_profile=ds_env.profile,
        price_schedule_hash=ds_env.schedule.content_hash)

    openai_schedule = ProviderPriceScheduleV1.load(OPENAI_PRICING)
    v1_profile = initial_model_profile(price_schedule=openai_schedule, agent_lock_path=AGENT_LOCK)
    openai_auth = BrokerDispatchAuthorizationV1.create(
            job_id=str(uuid.uuid4()), request_key="a" * 64, request_hash="b" * 64,
            attempt_id=str(uuid.uuid4()), call_index=1, lease_epoch=1, authorization_id=str(uuid.uuid4()),
            capability_nonce=str(uuid.uuid4()), evidence_hash=sha256_json([]),
            model_profile_hash=v1_profile.content_hash, provider="openai", requested_model_id="gpt-6-astra",
            deadline_ns=ds_env.now_ns + 20_000_000_000, authorized_at_ns=ds_env.now_ns + 1,
            expires_at_ns=ds_env.now_ns + 10_000_000_000, budget_reservation_id=str(uuid.uuid4()),
            reserved_cost_usd="0.35", max_input_tokens=12_000, max_output_tokens=4_000)
    openai_capability = BrokerCapabilityV1.issue_authorized(authorization=openai_auth,
        signing_key=b"x" * 32)
    with pytest.raises(BrokerProtocolError, match="CAPABILITY_PROFILE_VERSION_MISMATCH"):
        broker.infer(capability=openai_capability.capability, job_id=job.job_id, attempt_id=attempt.attempt_id,
            lease_epoch=leased.lease_epoch, call_index=attempt.attempt_index,
            request_data=request.to_dict(), evidence=evidence, now_ns=ds_env.now_ns + 3)

    # A freshly signed capability for the same provider but another model is rejected at the broker fence.
    payload_part, _signature_part = capability.capability.split(".", 1)
    forged_body = json.loads(base64.urlsafe_b64decode(payload_part.encode("ascii")).decode("utf-8"))
    forged_body["requested_model_id"] = "deepseek-v4-flash"
    forged_payload = canonical_json(forged_body).encode("utf-8")
    forged_signature = hmac.new(b"x" * 32, forged_payload, hashlib.sha256).digest()
    forged_capability = (base64.urlsafe_b64encode(forged_payload).decode("ascii") + "."
        + base64.urlsafe_b64encode(forged_signature).decode("ascii"))
    with pytest.raises(BrokerProtocolError, match="INVALID_OR_EXPIRED_CAPABILITY"):
        broker.infer(capability=forged_capability, job_id=job.job_id, attempt_id=attempt.attempt_id,
            lease_epoch=leased.lease_epoch, call_index=attempt.attempt_index,
            request_data=request.to_dict(), evidence=evidence, now_ns=ds_env.now_ns + 3)

    alternate_profile = replace(ds_env.profile, runtime_dependency_hash="f" * 64)
    alternate_provider = SimpleNamespace(provider_id="deepseek", requested_model_id="deepseek-flash",
        endpoint="https://api.deepseek.com/responses", propose=lambda *_args: pytest.fail("provider called"))
    alternate_broker = InferenceBroker(alternate_provider, signing_key=b"x" * 32,
        model_profile=alternate_profile, price_schedule_hash=ds_env.schedule.content_hash)
    with pytest.raises(BrokerProtocolError, match="CAPABILITY_PROFILE_BINDING_MISMATCH"):
        alternate_broker.infer(capability=capability.capability, job_id=job.job_id,
            attempt_id=attempt.attempt_id, lease_epoch=leased.lease_epoch, call_index=attempt.attempt_index,
            request_data=request.to_dict(), evidence=evidence, now_ns=ds_env.now_ns + 3)


def test_deepseek_timeout_retries_only_deepseek_and_persists_three_authorized_attempts(
        ds_env: _DeepSeekFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    request = ds_env.request()
    calls, clients = _install_fake_sdk(monkeypatch, TimeoutError("offline fake timeout"))
    broker = InferenceBroker(_provider(ds_env.profile), signing_key=b"r" * 32,
        model_profile=ds_env.profile, price_schedule_hash=ds_env.schedule.content_hash)

    class BrokerPort:
        def __init__(self, context: Any) -> None:
            self.context = context

        def propose(self, inner_request: ResearchProposalRequestV1, evidence: Any) -> ProviderResultV1:
            return broker.infer(capability=self.context.capability, job_id=self.context.job_id,
                attempt_id=self.context.attempt_id, lease_epoch=self.context.lease_epoch,
                call_index=self.context.call_index, request_data=inner_request.to_dict(), evidence=evidence,
                now_ns=ds_env.now_ns + 10)

    controller = ResearchJobController(jobs=ds_env.jobs, evidence=ds_env.evidence, profile=ds_env.profile,
        capability_signing_key=b"r" * 32, provider_factory=lambda context, _deadline: BrokerPort(context),
        now_ns=lambda: ds_env.now_ns)
    outcome = controller.run(request, job_id=str(uuid.uuid4()))
    assert outcome.lifecycle_state == AgentJobStateV1.UNAVAILABLE
    assert len(calls) == 3 and len(clients) == 3
    assert all(client["base_url"] == "https://api.deepseek.com" and client["max_retries"] == 0 for client in clients)
    with sqlite3.connect(ds_env.path) as connection:
        rows = connection.execute("SELECT attempt_index,model_profile_hash,dispatch_hash FROM agent_broker_dispatches "
                                  "ORDER BY attempt_index").fetchall()
        assert [row[0] for row in rows] == [1, 2, 3]
        assert all(row[1] == ds_env.profile.content_hash for row in rows)
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0


def test_worker_and_broker_authority_boundaries_and_disabled_ops_runtime_remain_separate() -> None:
    worker_path = ROOT / "src/atlas/v2/agent_intelligence/worker.py"
    broker_path = ROOT / "src/atlas/v2/agent_intelligence/broker.py"
    worker = worker_path.read_text(encoding="utf-8")
    broker = broker_path.read_text(encoding="utf-8")
    worker_tree = ast.parse(worker)
    worker_imports = {node.module or "" for node in ast.walk(worker_tree) if isinstance(node, ast.ImportFrom)}
    worker_imports.update(alias.name for node in ast.walk(worker_tree) if isinstance(node, ast.Import)
                           for alias in node.names)
    assert not any(name == "sqlite3" or name.startswith("openai") or name.startswith("httpx")
                   for name in worker_imports)
    assert "DEEPSEEK_API_KEY" not in worker and "OPENAI_API_KEY" not in worker
    assert "--unshare-all" in worker and "--clearenv" in worker and "--share-net" not in worker
    assert "sqlite3" not in broker and "OpsRepository" not in broker and "ops.sqlite" not in broker
    secret_owners: dict[str, list[str]] = {"OPENAI_API_KEY": [], "DEEPSEEK_API_KEY": []}
    for source in (ROOT / "src/atlas").rglob("*.py"):
        body = source.read_text(encoding="utf-8")
        for secret_name in secret_owners:
            if secret_name in body:
                secret_owners[secret_name].append(source.relative_to(ROOT).as_posix())
    assert secret_owners == {"OPENAI_API_KEY": ["src/atlas/v2/agent_intelligence/broker.py"],
        "DEEPSEEK_API_KEY": ["src/atlas/v2/agent_intelligence/broker.py"]}
    production = (ROOT / "src/atlas/v2/runtime/production.py").read_text(encoding="utf-8")
    supervisor = (ROOT / "src/atlas/v2/runtime/ops_supervisor.py").read_text(encoding="utf-8")
    assert "agent_intelligence" not in production and "agent_intelligence" not in supervisor


def test_static_scope_contains_no_capital_or_venue_authority() -> None:
    source = (ROOT / "src/atlas/v2/agent_intelligence/provider.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imports.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)
    assert not any("execution" in name or "capital" in name or "venue" in name for name in imports)
    assert "TradePlan" not in source and "RiskPolicy" not in source
    assert DISCOVERY_LAB_HASH
