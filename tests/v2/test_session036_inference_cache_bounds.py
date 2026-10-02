"""Continuous broker operation retains replay protection within bounded resources."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from atlas.v2.agent_intelligence import broker as broker_module
from atlas.v2.agent_intelligence.broker import BrokerProtocolError, InferenceBroker
from atlas.v2.agent_intelligence.contracts import AgentJobStateV1

from .test_session026_agent_intelligence import (
    _authorized_capability,
    _FakeProvider,
    _result,
)
from .test_session026_agent_intelligence import (
    env as env,
)


def _arguments(fixture: Any, *, offset_ns: int = 0) -> dict[str, Any]:
    family = fixture.jobs._connection.execute(
        "SELECT initial_attempts-proposal_jobs_started AS attempts,"
        "initial_parameter_units-parameter_units_used AS parameters "
        "FROM agent_family_budgets WHERE family_id=?", (fixture.experiment.family_id,),
    ).fetchone()
    request = fixture.request(**({"remaining_attempt_budget": family["attempts"],
                                 "remaining_parameter_search_budget": family["parameters"]}
                                if family is not None else {}))
    evidence = [{"tool_name": item.tool_name, "artifact_ref": item.artifact_ref,
                 "cursor": item.cursor, "status": "PRESENT", "rows": []}
                for item in request.evidence_manifest]
    now = fixture.now_ns + offset_ns
    job, leased, attempt, _, capability = _authorized_capability(fixture, request, evidence, now_ns=now)
    # Simulate terminal controller accounting between operations. The broker
    # must still protect every signed capability until its own expiry, including
    # replay after the controller has advanced to a new request.
    fixture.jobs.transition_terminal(job.job_id, AgentJobStateV1.UNAVAILABLE,
        now_ns=now + 3, reason="OFFLINE_BROKER_BOUNDARY_FIXTURE")
    return {"capability": capability.capability, "job_id": job.job_id,
            "attempt_id": attempt.attempt_id, "lease_epoch": leased.lease_epoch,
            "call_index": attempt.attempt_index, "request_data": request.to_dict(),
            "evidence": evidence, "now_ns": now + 3}


def test_expired_completions_release_capacity_without_permitting_replay(
        env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(broker_module, "MAX_REPLAY_CACHE_ENTRIES", 2)
    provider = _FakeProvider(_result("{}"))
    broker = InferenceBroker(provider, signing_key=b"b" * 32)
    first, second, excess = [_arguments(env) for _ in range(3)]
    assert broker.infer(**first) == broker.infer(**first)
    broker.infer(**second)
    with pytest.raises(BrokerProtocolError, match="REPLAY_CACHE_FULL"):
        broker.infer(**excess)
    assert provider.calls == 2 and len(broker._seen) == 2
    later = _arguments(env, offset_ns=31_000_000_000)
    broker.infer(**later)
    assert provider.calls == 3 and len(broker._seen) == 1
    with pytest.raises(BrokerProtocolError, match="INVALID_OR_EXPIRED_CAPABILITY"):
        broker.infer(**{**first, "now_ns": later["now_ns"]})
    # The wall clock cannot make a removed completion dispatchable again.
    with pytest.raises(BrokerProtocolError, match="BROKER_CLOCK_REGRESSION"):
        broker.infer(**first)
    assert provider.calls == 3


def test_distinct_authorizations_share_one_provider_slot_and_release_after_completion(env: Any) -> None:
    entered, release = threading.Event(), threading.Event()

    class BlockingProvider(_FakeProvider):
        def propose(self, request: Any, evidence: Any) -> Any:
            entered.set()
            assert release.wait(timeout=5), "test did not release the provider"
            return super().propose(request, evidence)

    provider = BlockingProvider(_result("{}"))
    broker = InferenceBroker(provider, signing_key=b"b" * 32)
    first, second = _arguments(env), _arguments(env)
    with ThreadPoolExecutor(max_workers=1) as worker:
        pending = worker.submit(broker.infer, **first)
        try:
            assert entered.wait(timeout=5)
            with pytest.raises(BrokerProtocolError, match="DISPATCH_ALREADY_IN_PROGRESS"):
                broker.infer(**first)
            with pytest.raises(BrokerProtocolError, match="BROKER_SATURATED"):
                broker.infer(**second)
            assert len(broker._busy) == 1 and not broker._seen
        finally:
            release.set()
        assert pending.result(timeout=5) == _result("{}")
    broker.infer(**second)
    assert provider.calls == 2 and not broker._busy
