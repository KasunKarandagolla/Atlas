"""Routing seals causal inputs before effects and never changes frozen actions."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import CandidateSelectionStatus
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.baseline import BaselineInputsV2, CausalCloseV2
from atlas.v2.models.protocol import ModelRequestV2
from atlas.v2.models.provider import DeterministicFakeProvider, ModelProvider
from atlas.v2.models.research_routing import (
    ResearchModelRouterV1,
    ResearchModelRouteV1,
    StatisticalResearchProviderV1,
    statistical_research_request_v1,
    statistical_research_route_v1,
)
from atlas.v2.models.worker_protocol import ModelProviderError
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
    index_decision_calendar_entry,
)

from . import test_model_runtime as models
from . import test_session018_remediation as science


def _setup(repo, *, provider=None, now=None):
    case, action, _payoff, _outcome = science._payoff_case(repo)
    model = models.manifest()
    route = ResearchModelRouteV1("baseline-diagnostic", "statistical", model.manifest_hash,
                                 FrozenMap({"variant": "fixed-v1"}))
    now = now or [action.available_at_ns + 1]
    request = ModelRequestV2.build(input_artifact_refs=(case.candidate.content_hash,), input_hash="a" * 64,
        instrument_key=action.action.key, policy_context_ref=action.content_hash,
        model_manifest_hash=model.manifest_hash, information_cutoff_ns=case.candidate.decision_at_ns,
        requested_targets=("log_return",), requested_horizons=(900000000000,),
        requested_quantiles=(), deadline_ns=case.candidate.deadline_ns, seed=7,
        resource_budget={"latency_ms": 1000})
    router = ResearchModelRouterV1(repo, run_id="run-model-036", config_hash="b" * 64,
        routes=(route,), manifests={model.manifest_hash: model},
        providers={} if provider is None else {"statistical": provider}, clock_ns=lambda: now[0])
    return router, route, model, request, action, now


def test_packet_is_persisted_before_inference_and_duplicate_reopen_reuses_result(tmp_path):
    path = tmp_path / "ops.sqlite"
    observed = []
    with OpsRepository(path) as repo:
        class InspectProvider(DeterministicFakeProvider):
            def infer(self, request, manifest, inputs, *, started_at_ns):
                rows = repo._connection.execute(
                    "SELECT metadata_json FROM artifact_index WHERE artifact_type='ResearchModelRequestV1'").fetchall()
                assert len(rows) == 1 and "worker_packet_hash" in rows[0][0]
                observed.append(request.input_hash)
                return super().infer(request, manifest, inputs, started_at_ns=started_at_ns)

        now = [science.CUTOFF + 1]
        provider = InspectProvider(clock_ns=lambda: now[0])
        router, route, model, request, action, now = _setup(repo, provider=provider, now=now)
        action_before = repo.get_artifact(action.content_hash)
        first = router.execute(route.route_id, request, {"closes": [100, 101]}, action_artifact_ref=action.content_hash)
        duplicate = router.execute(route.route_id, request, {"closes": [100, 101]}, action_artifact_ref=action.content_hash)
        assert duplicate == first and first.run.usable
        assert repo.get_artifact(action.content_hash) == action_before
    now[0] += 1
    with OpsRepository(path) as reopened:
        resumed = ResearchModelRouterV1(reopened, run_id="run-model-036", config_hash="b" * 64,
            routes=(route,), manifests={model.manifest_hash: model}, providers={"statistical": provider},
            clock_ns=lambda: now[0])
        assert resumed.execute(route.route_id, request, {"closes": [100, 101]},
                               action_artifact_ref=action.content_hash) == first
    assert observed == [request.input_hash]


@pytest.mark.parametrize("failure", ["PROVIDER_UNAVAILABLE", "PROVIDER_TIMEOUT", "MODEL_OUTPUT_INVALID", "LATE_OR_EXPIRED"])
def test_absent_failed_malformed_and_late_models_persist_terminal_without_fallback(tmp_path, failure):
    now = [science.CUTOFF + 1]
    class Failing(ModelProvider):
        def infer(self, *args, **kwargs):
            raise ModelProviderError("PROVIDER_TIMEOUT", "secret text must not enter research evidence")
    class Malformed(ModelProvider):
        def infer(self, *args, **kwargs):
            return {"unexpected": "SDK object"}
    provider = {"PROVIDER_UNAVAILABLE": None, "PROVIDER_TIMEOUT": Failing(),
                "MODEL_OUTPUT_INVALID": Malformed(), "LATE_OR_EXPIRED":
                DeterministicFakeProvider(clock_ns=lambda: now[0], receive_lag_ns=10_000_000_000)}[failure]
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, _model, request, action, _now = _setup(repo, provider=provider, now=now)
        result = router.execute(route.route_id, request, {"closes": [1]}, action_artifact_ref=action.content_hash)
        assert not result.run.usable and result.run.failure_code == failure
        assert repo.get_artifact(result.terminal_ref).metadata["routing"]["failure_code"] == failure
        assert (result.forecast_ref is not None) == (failure == "LATE_OR_EXPIRED")
        assert "secret text" not in str(repo.get_artifact(result.terminal_ref).metadata)


def test_input_config_and_route_drift_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, model, request, action, now = _setup(repo)
        router.execute(route.route_id, request, {"closes": [1]}, action_artifact_ref=action.content_hash)
        with pytest.raises(ValueError, match="different exact inputs"):
            router.execute(route.route_id, request, {"closes": [2]}, action_artifact_ref=action.content_hash)
        with pytest.raises(ValueError, match="undeclared"):
            router.execute("hidden-fallback", request, {}, action_artifact_ref=action.content_hash)
        with pytest.raises(ValueError, match="configuration changed"):
            ResearchModelRouterV1(repo, run_id="run-model-036", config_hash="b" * 64,
                routes=(replace(route, settings=FrozenMap({"variant": "hidden-v2"})),),
                manifests={model.manifest_hash: model}, providers={}, clock_ns=lambda: now[0])


def test_future_input_is_rejected_before_request_persistence(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, _model, request, action, _now = _setup(repo)
        future = ModelRequestV2.build(**{key: value for key, value in request.__dict__.items()
                                       if key not in {"request_id", "input_artifact_refs"}},
                                    input_artifact_refs=(action.content_hash,))
        # The action is derived after the market cutoff in this fixture; use an
        # explicitly post-cutoff operational entry if the fixture action is older.
        from atlas.v2.memory.repository import ArtifactIndexEntryV2
        repo.register_artifact(ArtifactIndexEntryV2("f" * 64, "FutureFixture", "f" * 64,
            request.information_cutoff_ns + 1, request.information_cutoff_ns + 1, {}))
        future = ModelRequestV2.build(**{key: value for key, value in future.__dict__.items()
                                       if key not in {"request_id", "input_artifact_refs"}}, input_artifact_refs=("f" * 64,))
        with pytest.raises(ValueError, match="unavailable at its fixed cutoff"):
            router.execute(route.route_id, future, {}, action_artifact_ref=action.content_hash)
        assert repo._connection.execute(
            "SELECT count(*) FROM artifact_index WHERE artifact_type='ResearchModelRequestV1'").fetchone()[0] == 0


def test_expired_request_retains_negative_case_without_invoking_model(tmp_path):
    now = [science.CUTOFF + 1]
    provider = DeterministicFakeProvider(clock_ns=lambda: now[0])
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, _model, request, action, _now = _setup(repo, provider=provider, now=now)
        now[0] = request.deadline_ns
        result = router.execute(route.route_id, request, {}, action_artifact_ref=action.content_hash)
        assert not result.run.usable and result.run.failure_code == "STALE_BEFORE_EXECUTION"
        assert not provider.calls and result.forecast_ref is None
        assert repo.get_artifact(result.request_ref) is not None
        assert repo.get_artifact(result.terminal_ref).available_at_ns == request.deadline_ns


def test_partial_completion_restart_preserves_request_without_rerunning_provider(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    now = [science.CUTOFF + 1]
    provider = DeterministicFakeProvider(clock_ns=lambda: now[0])
    with OpsRepository(path) as repo:
        router, route, model, request, action, _now = _setup(repo, provider=provider, now=now)
        original = repo.register_artifacts

        def fail_terminal(entries):
            if any(entry.artifact_type == "ResearchModelTerminalV1" for entry in entries):
                raise OSError("simulated interruption during completion persistence")
            return original(entries)

        monkeypatch.setattr(repo, "register_artifacts", fail_terminal)
        with pytest.raises(OSError, match="simulated interruption"):
            router.execute(route.route_id, request, {}, action_artifact_ref=action.content_hash)
        assert len(provider.calls) == 1
    with OpsRepository(path) as reopened:
        resumed = ResearchModelRouterV1(reopened, run_id="run-model-036", config_hash="b" * 64,
            routes=(route,), manifests={model.manifest_hash: model}, providers={"statistical": provider},
            clock_ns=lambda: now[0])
        result = resumed.execute(route.route_id, request, {}, action_artifact_ref=action.content_hash)
        assert result.run.failure_code == "MODEL_COMPLETION_LOST_ON_RESTART" and not result.run.usable
        assert resumed.execute(route.route_id, request, {}, action_artifact_ref=action.content_hash) == result
        assert len(provider.calls) == 1


def test_fixed_statistical_lane_retains_exact_numbers_and_reopens_without_computation(tmp_path):
    path = tmp_path / "ops.sqlite"
    now = [science.CUTOFF + 1]
    with OpsRepository(path) as repo:
        case, action, _payoff, _outcome = science._payoff_case(repo)
        route, manifest = statistical_research_route_v1(source_sha="a" * 40, environment_lock_hash="b" * 64)
        inputs = BaselineInputsV2(action.action.key, science.CUTOFF, (
            CausalCloseV2(science.CUTOFF - 900_000_000_000, science.CUTOFF - 900_000_000_000, Decimal("100")),
            CausalCloseV2(science.CUTOFF, science.CUTOFF, Decimal("110")),
        ))
        request = statistical_research_request_v1(inputs, manifest,
            input_artifact_refs=(case.candidate.content_hash,), action_artifact_ref=action.content_hash,
            deadline_ns=case.candidate.deadline_ns, quantiles=())
        provider = StatisticalResearchProviderV1(clock_ns=lambda: now[0])
        router = ResearchModelRouterV1(repo, run_id="run-statistical", config_hash="c" * 64, routes=(route,),
            manifests={manifest.manifest_hash: manifest}, providers={route.provider_key: provider},
            clock_ns=lambda: now[0])
        result = router.execute(route.route_id, request, inputs.to_dict(), action_artifact_ref=action.content_hash)
        assert result.run.usable
        values_ref = result.run.artifact.values_ref
        values = repo.get_artifact(values_ref)
        assert values.artifact_type == "ResearchModelValuesV1"
        assert sha256_json(values.metadata["model_values"]) == values_ref
        assert float(values.metadata["model_values"]["log_return:900000000000:mean"]) == pytest.approx(0.0953101798)
        assert repo.get_artifact(result.terminal_ref).metadata["routing"]["values_evidence_ref"] == values_ref
    with OpsRepository(path) as reopened:
        resumed = ResearchModelRouterV1(reopened, run_id="run-statistical", config_hash="c" * 64, routes=(route,),
            manifests={manifest.manifest_hash: manifest}, providers={}, clock_ns=lambda: now[0])
        assert resumed.execute(route.route_id, request, inputs.to_dict(), action_artifact_ref=action.content_hash) == result


def _negative_calendar(repo, *, event_deadline_ns):
    case, _action, _payoff, _outcome = science._payoff_case(repo)
    identity = dict(repo.get_artifact(case.candidate_set.content_hash).metadata["identity"])
    event_id = sha256_json({"fixture": "negative-model-origin"})
    identity.update(candidate_refs=(), decision_event_id=event_id)
    candidate_set = replace(case.candidate_set, candidates=(), selected_candidate_id=None,
        selection_status=CandidateSelectionStatus.NO_CANDIDATE, decision_event_id=event_id,
        envelope=replace(case.candidate_set.envelope, artifact_id=sha256_json(identity), content_hash=""))
    science._index_fixture_candidate_set(repo, candidate_set, identity)
    calendar = DecisionCalendarEntryV2(candidate_set.content_hash, None, "RESEARCH_SELECTION", "1",
        candidate_set.selection_policy_hash, science.CUTOFF, SelectionStateV2.NO_CANDIDATE,
        AdmissionStateV2.NOT_APPLICABLE, None, None, DecisionSourceStageV2.CANDIDATE_SET, (),
        candidate_set.content_hash, science.CUTOFF, science.CUTOFF)
    decision_ref = index_decision_calendar_entry(repo, calendar)
    event_body = {"schema_version": 1, "event_id": event_id, "event_type": "FIXTURE_ORIGIN",
        "source_id": "FIXTURE_PUBLIC", "trigger_ref": case.candidate.content_hash,
        "source_event_at_ns": science.CUTOFF, "source_published_at_ns": None,
        "received_at_ns": science.CUTOFF, "available_at_ns": science.CUTOFF,
        "information_cutoff_ns": science.CUTOFF, "deadline_ns": event_deadline_ns,
        "causal_input_refs": [case.candidate.content_hash]}
    event_ref = sha256_json(event_body)
    repo.register_artifact(ArtifactIndexEntryV2(event_ref, "OpsDecisionEventSourceV1", event_ref,
        science.CUTOFF, science.CUTOFF, {"event": event_body}))
    return decision_ref, event_ref


def test_no_candidate_diagnostic_binds_calendar_and_retains_absent_action(tmp_path):
    now = [science.CUTOFF + 1]
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, _model, request, _action, _now = _setup(repo,
            provider=DeterministicFakeProvider(clock_ns=lambda: now[0]), now=now)
        decision_ref, event_ref = _negative_calendar(repo, event_deadline_ns=request.deadline_ns)
        request = ModelRequestV2.build(**{key: value for key, value in request.__dict__.items()
                                       if key not in {"request_id", "policy_context_ref"}},
                                       policy_context_ref=decision_ref)
        result = router.execute(route.route_id, request, {},
            decision_calendar_ref=decision_ref, decision_event_ref=event_ref)
        assert result.run.usable
        terminal = repo.get_artifact(result.terminal_ref).metadata["routing"]
        assert terminal["action_hash"] is None and terminal["action_artifact_ref"] is None
        assert terminal["decision_calendar_ref"] == decision_ref and terminal["decision_event_ref"] == event_ref
        assert repo.get_artifact(decision_ref).metadata["decision_entry"]["selection_state"] == "NO_CANDIDATE"


@pytest.mark.parametrize("changed", ("deadline", "revision", "context"))
def test_decision_diagnostic_rejects_changed_deadline_revision_or_context(tmp_path, changed):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        router, route, _model, request, _action, _now = _setup(repo)
        decision_ref, event_ref = _negative_calendar(repo, event_deadline_ns=request.deadline_ns)
        fields = {key: value for key, value in request.__dict__.items() if key != "request_id"}
        fields["policy_context_ref"] = decision_ref
        if changed == "deadline":
            fields["deadline_ns"] += 1
        elif changed == "revision":
            fields["instrument_key"] = replace(request.instrument_key, contract_revision="f" * 64)
        else:
            fields["policy_context_ref"] = "unrelated-context"
        changed_request = ModelRequestV2.build(**fields)
        with pytest.raises(ValueError):
            router.execute(route.route_id, changed_request, {},
                decision_calendar_ref=decision_ref, decision_event_ref=event_ref)
        assert repo._connection.execute(
            "SELECT count(*) FROM artifact_index WHERE artifact_type='ResearchModelRequestV1'").fetchone()[0] == 0
