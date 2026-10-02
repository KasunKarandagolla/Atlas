"""Small durable, zero-authority routing lane over the existing model ABI.

The calling controller owns the writer and scheduling. This service neither
opens another database nor changes an action, admission, risk or execution.
"""

from __future__ import annotations

import re
import threading
import time
from abc import abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.baseline import BaselineInputsV2, StatisticalBaselineV2
from atlas.v2.models.protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2, PromotionStatusV2
from atlas.v2.models.provider import ModelArenaV2, ModelProvider, ModelRunV2
from atlas.v2.models.worker_protocol import ModelProviderError, ProviderStateV2, WorkerRequestV2
from atlas.v2.science.outcomes import (
    _resolve_candidate_set,
    _resolve_decision_calendar_entry,
    index_decision_calendar_entry,
)


class ResearchValuesProviderV1(ModelProvider):
    """Provider whose exact numerical output can be retained by the writer."""

    @abstractmethod
    def values_for(self, artifact: ForecastArtifactV2) -> FrozenMap | None:
        raise NotImplementedError


class StatisticalResearchProviderV1(ResearchValuesProviderV1):
    """Capture the existing empirical baseline output with one bounded cache slot."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns) -> None:
        self._baseline = StatisticalBaselineV2(clock_ns=clock_ns)
        self._last: tuple[str, FrozenMap] | None = None
        self.clock_ns = clock_ns

    def infer(self, request: ModelRequestV2, manifest: ModelManifestV2,
              inputs: Mapping[str, Any], *, started_at_ns: int) -> ForecastArtifactV2:
        self._last = None
        if (manifest.provider != "statistical-baseline-v1"
                or manifest.checkpoint_id != "empirical-horizon-return-v1"):
            raise ModelProviderError("MANIFEST_MISMATCH", "statistical research provider identity changed")
        try:
            typed_inputs = BaselineInputsV2.from_dict(json_value(FrozenMap(inputs)))
        except (TypeError, ValueError, KeyError) as exc:
            raise ModelProviderError("BASELINE_INPUT_INVALID", "statistical baseline inputs are invalid") from exc
        if len(typed_inputs.closes) > min(manifest.context_limit, 4096):
            raise ModelProviderError("BASELINE_INPUT_TOO_LARGE", "baseline input exceeds its registered context bound")
        computation_at_ns = self.clock_ns()
        if computation_at_ns < started_at_ns:
            raise ModelProviderError("CLOCK_REGRESSION", "research model clock regressed before computation")
        output = self._baseline.predict(request, manifest, typed_inputs, completed_at_ns=computation_at_ns)
        completed_at_ns = self.clock_ns()
        if completed_at_ns < computation_at_ns:
            raise ModelProviderError("CLOCK_REGRESSION", "research model clock regressed during computation")
        artifact = replace(output.artifact, inference_started_ns=started_at_ns,
                           completed_ns=completed_at_ns, received_ns=completed_at_ns)
        self._last = (artifact.content_hash, output.values)
        return artifact

    def values_for(self, artifact: ForecastArtifactV2) -> FrozenMap | None:
        if self._last is None or self._last[0] != artifact.content_hash:
            return None
        return self._last[1]


def statistical_research_route_v1(*, source_sha: str, environment_lock_hash: str,
                                  route_id: str = "statistical-baseline-v1") -> tuple[ResearchModelRouteV1, ModelManifestV2]:
    """Declare the existing fixed baseline as a zero-authority diagnostic lane."""
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("statistical research manifest requires the exact source SHA")
    sha256_ref(environment_lock_hash, field="environment_lock_hash")
    manifest = ModelManifestV2(
        provider="statistical-baseline-v1", source_repository="https://github.com/KasunKarandagolla/Atlas",
        source_commit=source_sha, checkpoint_id="empirical-horizon-return-v1", checkpoint_revision="1",
        weight_sha256=(), code_license_ref="ATLAS_REPOSITORY_LICENSE", weight_license_ref="NO_TRAINED_WEIGHTS",
        allowed_use_status="RESEARCH_ONLY", preprocessing_hash=sha256_json({"input": "BaselineInputsV2", "version": 1}),
        postprocessing_hash=sha256_json({"algorithm": "empirical-horizon-return-v1", "quantile": "LOWER_ORDER_STATISTIC"}),
        environment_lock_hash=environment_lock_hash, device="cpu", precision="fp64",
        supported_inputs=("BaselineInputsV2",), supported_outputs=("log_return",), context_limit=4096,
        contamination_class="CAUSAL_INPUT_ONLY", promotion_status=PromotionStatusV2.INTEGRATED)
    route = ResearchModelRouteV1(route_id, "statistical-baseline-v1", manifest.manifest_hash,
                                 FrozenMap({"variant": "fixed-v1", "context_limit": 4096}))
    return route, manifest


def statistical_research_request_v1(inputs: BaselineInputsV2, manifest: ModelManifestV2, *,
        input_artifact_refs: tuple[str, ...], deadline_ns: int,
        action_artifact_ref: str | None = None, decision_calendar_ref: str | None = None,
        horizons: tuple[int, ...] = (900_000_000_000,),
        quantiles: tuple[Decimal, ...] = (Decimal("0.05"), Decimal("0.5"), Decimal("0.95")),
        seed: int = 7) -> ModelRequestV2:
    """Build an immutable existing ABI request using the original caller deadline."""
    context_ref = action_artifact_ref if action_artifact_ref is not None else decision_calendar_ref
    if context_ref is None:
        raise ValueError("statistical research request requires an action or decision context")
    context_ref = sha256_ref(context_ref, field="action or decision context ref")
    if (manifest.provider != "statistical-baseline-v1"
            or manifest.checkpoint_id != "empirical-horizon-return-v1"):
        raise ValueError("statistical request requires its exact declared provider identity")
    if not horizons or not set(horizons).issubset({900_000_000_000, 3_600_000_000_000, 14_400_000_000_000}):
        raise ValueError("statistical request horizons exceed the existing provider capability")
    return ModelRequestV2.build(input_artifact_refs=input_artifact_refs, input_hash=inputs.content_hash,
        instrument_key=inputs.instrument_key, policy_context_ref=context_ref,
        model_manifest_hash=manifest.manifest_hash, information_cutoff_ns=inputs.information_cutoff_ns,
        requested_targets=("log_return",), requested_horizons=horizons, requested_quantiles=quantiles,
        deadline_ns=deadline_ns, seed=seed, resource_budget={"latency_ms": 1000, "memory_mb": 256})


@dataclass(frozen=True)
class ResearchModelRouteV1:
    route_id: str
    provider_key: str
    manifest_hash: str
    settings: FrozenMap

    def __post_init__(self) -> None:
        for name in ("route_id", "provider_key"):
            if not isinstance(getattr(self, name), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", getattr(self, name)):
                raise ValueError("research route identity must be bounded")
        sha256_ref(self.manifest_hash, field="manifest_hash")
        settings = self.settings if isinstance(self.settings, FrozenMap) else FrozenMap(self.settings)
        if len(canonical_json(settings.to_dict()).encode()) > 16_384:
            raise ValueError("research model settings exceed their byte limit")
        object.__setattr__(self, "settings", settings)

    def to_dict(self) -> dict[str, Any]:
        return {"version": "ResearchModelRouteV1", "route_id": self.route_id,
                "provider_key": self.provider_key, "manifest_hash": self.manifest_hash,
                "settings": self.settings.to_dict(), "authority": "ZERO"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ResearchModelRoutingResultV1:
    request_ref: str
    terminal_ref: str
    forecast_ref: str | None
    run: ModelRunV2


def _index(repository: OpsRepository, artifact_type: str, body: Mapping[str, Any], at_ns: int) -> str:
    ref = sha256_json(body)
    prior = repository.get_artifact(ref)
    if prior is not None:
        if (prior.artifact_type != artifact_type or prior.content_hash != ref
                or canonical_json(prior.metadata) != canonical_json({"routing": body})):
            raise ValueError("immutable routing artifact conflicts with persisted evidence")
        return ref
    repository.register_artifact(ArtifactIndexEntryV2(ref, artifact_type, ref, at_ns, at_ns, {"routing": body}))
    return ref


class ResearchModelRouterV1:
    """Explicit immutable provider selection; synchronous calls need bounded providers.

    Construct and call on the controller's existing writer thread. A caller may
    schedule this research lane after its deterministic decision is sealed. The
    model ABI's semantic input hash is preserved; the full WorkerRequest content
    hash independently binds the exact transported input bytes.
    """

    def __init__(self, repository: OpsRepository, *, run_id: str, config_hash: str,
                 routes: tuple[ResearchModelRouteV1, ...], manifests: Mapping[str, ModelManifestV2],
                 providers: Mapping[str, ModelProvider], clock_ns: Callable[[], int] = time.time_ns) -> None:
        if repository.read_only:
            raise ValueError("research routing requires the existing operational writer")
        if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", run_id):
            raise ValueError("research routing run identity must be bounded")
        sha256_ref(config_hash, field="config_hash")
        if not routes or len(routes) > 16 or len({route.route_id for route in routes}) != len(routes):
            raise ValueError("research routes must be a bounded unique registry")
        self.repository = repository
        self.run_id, self.config_hash, self.clock_ns = run_id, config_hash, clock_ns
        self._writer_thread = threading.get_ident()
        self._routes = {route.route_id: route for route in routes}
        self._manifests = dict(manifests)
        self._providers = dict(providers)
        if set(self._manifests) != {route.manifest_hash for route in routes}:
            raise ValueError("research routing manifests must match the bounded declared registry")
        if (not set(self._providers).issubset({route.provider_key for route in routes})
                or any(not isinstance(provider, ModelProvider) for provider in self._providers.values())):
            raise TypeError("research routing accepts only declared typed model providers")
        at_ns = clock_ns()
        timestamp(at_ns, field="route registered_at_ns")
        for route in routes:
            manifest = self._manifests.get(route.manifest_hash)
            if manifest is None or manifest.manifest_hash != route.manifest_hash:
                raise ValueError("registered route requires its exact manifest")
            # Reuse the protocol's deny list for settings as well as transported inputs.
            from atlas.v2.models.worker_protocol import _reject_sensitive_fields

            _reject_sensitive_fields(route.settings.to_dict(), field_name="routing_settings")
            repository.register_model_manifest(manifest)
        registry = {"version": "ResearchModelRoutingRegistryV1", "run_id": run_id, "config_hash": config_hash,
                    "routes": [route.to_dict() for route in sorted(routes, key=lambda route: route.route_id)],
                    "authority": "ZERO"}
        registry_key = sha256_json({"version": "ResearchModelRunRegistryIdentityV1", "run_id": run_id})
        registry_binding = {"version": "ResearchModelRunRegistryIdentityV1", "run_id": run_id,
                            "config_hash": config_hash, "registry_ref": sha256_json(registry), "authority": "ZERO"}
        prior = repository.get_artifact(registry_key)
        if prior is not None and (canonical_json(prior.metadata) != canonical_json({"routing": registry_binding})
                                  or prior.content_hash != sha256_json(registry_binding)):
            raise ValueError("research routing configuration changed within an immutable run")
        self.registry_ref = _index(repository, "ResearchModelRoutingRegistryV1", registry, at_ns)
        if prior is None:
            repository.register_artifact(ArtifactIndexEntryV2(registry_key, "ResearchModelRunRegistryIdentityV1",
                sha256_json(registry_binding), at_ns, at_ns, {"routing": registry_binding}))

    def execute(self, route_id: str, request: ModelRequestV2, inputs: Mapping[str, Any], *,
                action_artifact_ref: str | None = None, decision_calendar_ref: str | None = None,
                decision_event_ref: str | None = None,
                derived_input_ref: str | None = None) -> ResearchModelRoutingResultV1:
        if threading.get_ident() != self._writer_thread:
            raise RuntimeError("research routing persistence belongs to the controller writer thread")
        route = self._routes.get(route_id)
        if route is None or route.manifest_hash != request.model_manifest_hash:
            raise ValueError("undeclared route or changed model manifest")
        manifest = self.repository.get_model_manifest(route.manifest_hash)
        if manifest is None:
            raise ValueError("registered model manifest disappeared")
        # Snapshot the transported input before sealing so caller mutation cannot
        # change inference after the durable request has been written.
        packet = WorkerRequestV2(request, manifest, FrozenMap(inputs))
        if (len(canonical_json(packet.to_dict()).encode()) > 1_048_576
                or len(request.input_artifact_refs) > 128 or len(request.requested_targets) > 16
                or len(request.requested_horizons) > 8 or len(request.requested_quantiles) > 64):
            raise ValueError("research model input exceeds its bound")
        now = self.clock_ns()
        timestamp(now, field="dispatch_at_ns")
        if request.information_cutoff_ns > now:
            raise ValueError("research model cutoff is in the future")
        for ref in request.input_artifact_refs:
            sha256_ref(ref, field="model input artifact ref")
            entry = self.repository.get_artifact(ref)
            if entry is None or entry.available_at_ns > request.information_cutoff_ns:
                raise ValueError("model input was unavailable at its fixed cutoff")
        if derived_input_ref is not None:
            sha256_ref(derived_input_ref, field="derived_input_ref")
            snapshot = self.repository.get_artifact(derived_input_ref)
            if (snapshot is None or snapshot.artifact_type != "BaselineInputsV2"
                    or snapshot.content_hash != request.input_hash or derived_input_ref != request.input_hash
                    or snapshot.available_at_ns > now
                    or canonical_json(snapshot.metadata.get("baseline_inputs")) != canonical_json(inputs)):
                raise ValueError("derived research input lacks its exact actual publication snapshot")
        if action_artifact_ref is None and decision_calendar_ref is None:
            raise ValueError("research request requires its exact action or decision calendar binding")
        action_hash = None
        if action_artifact_ref is not None:
            sha256_ref(action_artifact_ref, field="action_artifact_ref")
            action = self.repository.get_artifact(action_artifact_ref)
            body = action.metadata.get("action_artifact") if action is not None else None
            identity = action.metadata.get("action_identity") if action is not None else None
            if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > now
                    or not isinstance(body, Mapping) or not isinstance(identity, Mapping)
                    or sha256_json(body) != action_artifact_ref or action.content_hash != action_artifact_ref
                    or canonical_json(identity.get("key")) != canonical_json(request.instrument_key.to_dict())
                    or sha256_json(identity) != body.get("action_hash")):
                raise ValueError("research request lacks its exact available frozen action")
            action_hash = body["action_hash"]
        if decision_calendar_ref is not None:
            self._validate_calendar_binding(request, decision_calendar_ref, decision_event_ref,
                                            action_artifact_ref, now)
        elif decision_event_ref is not None:
            raise ValueError("decision event requires its exact decision calendar binding")
        seal = {"version": "ResearchModelRequestV1", "run_id": self.run_id, "config_hash": self.config_hash,
                "registry_ref": self.registry_ref, "route_ref": route.content_hash,
                "provider_key": route.provider_key, "worker_packet": packet.to_dict(),
                "worker_packet_hash": packet.content_hash, "model_profile_hash": manifest.manifest_hash,
                "action_artifact_ref": action_artifact_ref, "action_hash": action_hash,
                "decision_calendar_ref": decision_calendar_ref, "decision_event_ref": decision_event_ref,
                "derived_input_ref": derived_input_ref,
                "authority": "ZERO"}
        request_ref = sha256_json(seal)
        request_key = sha256_json({"version": "ResearchModelRequestIdentityV1", "run_id": self.run_id,
            "route_ref": route.content_hash, "request_id": request.request_id})
        request_binding = {"version": "ResearchModelRequestIdentityV1", "request_ref": request_ref, "authority": "ZERO"}
        prior_binding = self.repository.get_artifact(request_key)
        if prior_binding is not None and (canonical_json(prior_binding.metadata) != canonical_json({"routing": request_binding})
                                         or prior_binding.content_hash != sha256_json(request_binding)):
            raise ValueError("model request identity was reused with different exact inputs")
        completion_key = sha256_json({"version": "ResearchModelCompletionIdentityV1", "request_ref": request_ref})
        old_completion = self.repository.get_artifact(completion_key)
        if old_completion is not None:
            return self._load_result(old_completion, request_ref)
        old_request = self.repository.get_artifact(request_ref)
        if old_request is not None:
            if canonical_json(old_request.metadata.get("routing")) != canonical_json(seal):
                raise ValueError("persisted research packet conflicts with exact routing identity")
            run = ModelRunV2(request.request_id, ProviderStateV2.FAILED, None, False,
                             "MODEL_COMPLETION_LOST_ON_RESTART", request.input_hash, manifest.manifest_hash, 0, 0)
        else:
            entries = [ArtifactIndexEntryV2(request_ref, "ResearchModelRequestV1", request_ref,
                                           now, now, {"routing": seal})]
            if prior_binding is None:
                entries.append(ArtifactIndexEntryV2(request_key, "ResearchModelRequestIdentityV1",
                    sha256_json(request_binding), now, now, {"routing": request_binding}))
            self.repository.register_artifacts(entries)
            # A fresh one-request arena has no lifetime request/cache growth. Durable
            # idempotency belongs to the existing ops writer, including failures.
            arena = ModelArenaV2(max_queue_size=1, clock_ns=self.clock_ns)
            provider = self._providers.get(route.provider_key)
            if now >= request.deadline_ns:
                run = ModelRunV2(request.request_id, ProviderStateV2.STALE_DISCARDED, None, False,
                                 "STALE_BEFORE_EXECUTION", request.input_hash, manifest.manifest_hash, 0, 0)
            elif provider is None:
                run = ModelRunV2(request.request_id, ProviderStateV2.FAILED, None, False,
                                 "PROVIDER_UNAVAILABLE", request.input_hash, manifest.manifest_hash, 0, 0)
            else:
                arena.enqueue(request, manifest, packet.inputs, provider_key=route.provider_key)
                try:
                    arena_run = arena.run_next(provider, provider_key=route.provider_key)
                    if arena_run is None:
                        raise RuntimeError("sealed model request lost its single arena item")
                    run = arena_run
                except Exception:
                    # A malformed provider return cannot erase the sealed request
                    # or escape into deterministic admission.
                    run = ModelRunV2(request.request_id, ProviderStateV2.FAILED, None, False,
                                     "MODEL_OUTPUT_INVALID", request.input_hash, manifest.manifest_hash,
                                     0, max(0, self.clock_ns() - now))
        completed = self.clock_ns()
        if completed < now:
            raise ValueError("research model clock regressed before durable completion")
        forecast_ref = None
        values_evidence_ref = None
        if run.artifact is not None:
            artifact = run.artifact
            provider = self._providers.get(route.provider_key)
            if isinstance(provider, ResearchValuesProviderV1):
                try:
                    values = provider.values_for(artifact)
                except Exception:
                    values = None
                    run = ModelRunV2(run.request_id, ProviderStateV2.FAILED, artifact, False,
                        "MODEL_VALUES_INVALID", run.input_hash, run.manifest_hash,
                        run.queue_wait_ns, run.run_elapsed_ns)
                if values is None and run.usable:
                    run = ModelRunV2(run.request_id, ProviderStateV2.FAILED, artifact, False,
                        "MODEL_VALUES_UNAVAILABLE", run.input_hash, run.manifest_hash,
                        run.queue_wait_ns, run.run_elapsed_ns)
                if values is not None:
                    if (sha256_json(values) != artifact.values_ref
                            or len(canonical_json(values).encode()) > 65_536):
                        run = ModelRunV2(run.request_id, ProviderStateV2.FAILED, run.artifact, False,
                            "MODEL_VALUES_INVALID", run.input_hash, run.manifest_hash,
                            run.queue_wait_ns, run.run_elapsed_ns)
                    else:
                        values_evidence_ref = artifact.values_ref
                        prior_values = self.repository.get_artifact(values_evidence_ref)
                        metadata = {"model_values": values.to_dict(), "authority": "ZERO"}
                        if prior_values is not None:
                            if (prior_values.artifact_type != "ResearchModelValuesV1"
                                    or prior_values.content_hash != values_evidence_ref
                                    or canonical_json(prior_values.metadata) != canonical_json(metadata)):
                                raise ValueError("model values conflict with immutable evidence")
                        else:
                            self.repository.register_artifact(ArtifactIndexEntryV2(values_evidence_ref,
                                "ResearchModelValuesV1", values_evidence_ref, completed, completed, metadata))
            forecast_ref = _index(self.repository, "ResearchModelForecastV1", {
                "version": "ResearchModelForecastV1", "request_ref": request_ref,
                "forecast": artifact.to_dict(), "values_evidence_ref": values_evidence_ref,
                "authority": "ZERO"}, completed)
        failure = run.failure_code
        if failure is not None and not re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", failure):
            failure = "PROVIDER_EXCEPTION"
            run = ModelRunV2(run.request_id, run.state, run.artifact, False, failure, run.input_hash,
                             run.manifest_hash, run.queue_wait_ns, run.run_elapsed_ns)
        terminal = {"version": "ResearchModelTerminalV1", "request_ref": request_ref,
                    "run_id": self.run_id, "config_hash": self.config_hash, "route_ref": route.content_hash,
                    "action_artifact_ref": action_artifact_ref, "action_hash": action_hash,
                    "decision_calendar_ref": decision_calendar_ref, "decision_event_ref": decision_event_ref,
                    "derived_input_ref": derived_input_ref,
                    "request_id": run.request_id, "state": run.state.value, "usable": run.usable,
                    "failure_code": failure, "input_hash": run.input_hash, "manifest_hash": run.manifest_hash,
                    "queue_wait_ns": run.queue_wait_ns, "run_elapsed_ns": run.run_elapsed_ns,
                    "forecast_ref": forecast_ref, "values_evidence_ref": values_evidence_ref, "authority": "ZERO"}
        terminal_ref = sha256_json(terminal)
        completion = {"version": "ResearchModelCompletionIdentityV1", "request_ref": request_ref,
                      "terminal_ref": terminal_ref, "authority": "ZERO"}
        self.repository.register_artifacts((
            ArtifactIndexEntryV2(terminal_ref, "ResearchModelTerminalV1", terminal_ref,
                                completed, completed, {"routing": terminal}),
            ArtifactIndexEntryV2(completion_key, "ResearchModelCompletionIdentityV1",
                sha256_json(completion), completed, completed, {"routing": completion}),
        ))
        return ResearchModelRoutingResultV1(request_ref, terminal_ref, forecast_ref, run)

    def _validate_calendar_binding(self, request: ModelRequestV2, decision_ref: str,
            event_ref: str | None, action_ref: str | None, now_ns: int) -> None:
        sha256_ref(decision_ref, field="decision_calendar_ref")
        calendar = _resolve_decision_calendar_entry(self.repository, decision_ref)
        if calendar.available_at_ns > now_ns or calendar.decision_at_ns != request.information_cutoff_ns:
            raise ValueError("research decision calendar was unavailable or has a different market cutoff")
        if action_ref is not None and calendar.action_artifact_ref != action_ref:
            raise ValueError("research action and decision calendar disagree")
        if action_ref is None and request.policy_context_ref != decision_ref:
            raise ValueError("decision-only research context must bind its exact calendar")
        candidate_set, _identity = _resolve_candidate_set(self.repository, calendar.candidate_set_ref)
        if candidate_set.envelope.available_at_ns > now_ns:
            raise ValueError("research CandidateSet was unavailable at dispatch")
        if calendar.candidate_ref is not None:
            entry = self.repository.get_artifact(calendar.candidate_ref)
            candidate_body = entry.metadata.get("candidate") if entry is not None else None
            if (entry is None or entry.artifact_type != "CandidateActionV2"
                    or not isinstance(candidate_body, Mapping)):
                raise ValueError("research decision lacks its exact candidate instrument")
            candidate = CandidateActionV2.from_dict(json_value(candidate_body))
            if (candidate.content_hash != calendar.candidate_ref or candidate.key != request.instrument_key
                    or candidate.deadline_ns != request.deadline_ns):
                raise ValueError("research candidate instrument revision or original deadline changed")
        else:
            entry = self.repository.get_artifact(candidate_set.universe_ref)
            universe_body = entry.metadata.get("universe") if entry is not None else None
            if (entry is None or entry.artifact_type != "UniverseContractV2"
                    or not isinstance(universe_body, Mapping) or entry.available_at_ns > now_ns):
                raise ValueError("research decision lacks its exact universe instrument")
            universe = UniverseContractV2.from_dict(json_value(universe_body))
            if (universe.content_hash != candidate_set.universe_ref or entry.content_hash != universe.content_hash
                    or not any(item.key == request.instrument_key for item in universe.entries)):
                raise ValueError("research instrument revision is absent from the exact decision universe")
        if event_ref is None:
            raise ValueError("research decision requires its exact event and original deadline")
        event_ref = sha256_ref(event_ref, field="decision_event_ref")
        event = self.repository.get_artifact(event_ref)
        event_body = event.metadata.get("event") if event is not None else None
        if (event is None or event.artifact_type != "OpsDecisionEventSourceV1"
                or event.available_at_ns > now_ns or event.content_hash != event_ref
                or not isinstance(event_body, Mapping) or sha256_json(event_body) != event_ref
                or event_body.get("event_id") != candidate_set.decision_event_id
                or event_body.get("information_cutoff_ns") != request.information_cutoff_ns
                or event_body.get("deadline_ns") != request.deadline_ns):
            raise ValueError("research decision requires its exact event and original deadline")
        # Reuse the existing scientific producer validator, including negative
        # states and immutable source-stage identities. Existing rows are reused.
        index_decision_calendar_entry(self.repository, calendar)

    def _load_result(self, completion: ArtifactIndexEntryV2, request_ref: str) -> ResearchModelRoutingResultV1:
        binding = completion.metadata["routing"]
        if (completion.artifact_type != "ResearchModelCompletionIdentityV1"
                or binding["request_ref"] != request_ref or sha256_json(binding) != completion.content_hash):
            raise ValueError("model completion identity is corrupt")
        terminal_ref = binding["terminal_ref"]
        entry = self.repository.get_artifact(terminal_ref)
        if entry is None:
            raise ValueError("model completion lost its exact terminal artifact")
        body = entry.metadata["routing"]
        if (entry.artifact_type != "ResearchModelTerminalV1" or entry.content_hash != terminal_ref
                or sha256_json(body) != terminal_ref or body["request_ref"] != request_ref
                or body["run_id"] != self.run_id or body["config_hash"] != self.config_hash or body["authority"] != "ZERO"):
            raise ValueError("model terminal identity is corrupt")
        forecast_ref = body["forecast_ref"]
        artifact = None
        if forecast_ref is not None:
            forecast = self.repository.get_artifact(forecast_ref)
            if (forecast is None or forecast.artifact_type != "ResearchModelForecastV1"
                    or forecast.content_hash != forecast_ref
                    or sha256_json(forecast.metadata["routing"]) != forecast_ref
                    or forecast.metadata["routing"]["request_ref"] != request_ref):
                raise ValueError("model terminal lost its exact forecast")
            artifact = ForecastArtifactV2.from_dict(json_value(forecast.metadata["routing"]["forecast"]))
            values_ref = body["values_evidence_ref"]
            if values_ref is not None:
                values = self.repository.get_artifact(values_ref)
                if (values is None or values.artifact_type != "ResearchModelValuesV1"
                        or values.content_hash != values_ref or artifact.values_ref != values_ref
                        or sha256_json(values.metadata.get("model_values")) != values_ref
                        or values.metadata.get("authority") != "ZERO"):
                    raise ValueError("model terminal lost its exact numerical values")
        run = ModelRunV2(body["request_id"], ProviderStateV2(body["state"]), artifact, body["usable"],
                         body["failure_code"], body["input_hash"], body["manifest_hash"],
                         body["queue_wait_ns"], body["run_elapsed_ns"])
        return ResearchModelRoutingResultV1(request_ref, terminal_ref, forecast_ref, run)
