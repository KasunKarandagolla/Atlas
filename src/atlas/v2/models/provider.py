"""Provider-neutral Model Arena queue, validation, deadlines and archive."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .._serialization import FrozenMap, canonical_json, sha256_json
from ..contracts import ForecastStatusV2
from .protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2
from .worker_protocol import ModelProviderError, ProviderStateV2


class ModelProvider(ABC):
    """Provider interface; implementations return evidence and have no decision hooks."""

    @abstractmethod
    def infer(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        started_at_ns: int,
    ) -> ForecastArtifactV2:
        raise NotImplementedError


@dataclass(frozen=True)
class QueuedModelRequestV2:
    request: ModelRequestV2
    manifest: ModelManifestV2
    inputs: Mapping[str, Any]
    provider_key: str
    enqueued_at_ns: int


@dataclass(frozen=True)
class ModelRunV2:
    request_id: str
    state: ProviderStateV2
    artifact: ForecastArtifactV2 | None
    usable: bool
    failure_code: str | None
    input_hash: str
    manifest_hash: str
    queue_wait_ns: int
    run_elapsed_ns: int


class ForecastArchiveV2:
    """Content-addressed immutable forecast archive with optional local JSON persistence."""

    def __init__(self, root: str | Path | None = None) -> None:
        self._artifacts: dict[str, ForecastArtifactV2] = {}
        self.root = Path(root) if root is not None else None

    def _path(self, request_id: str) -> Path:
        if self.root is None:
            raise RuntimeError("forecast archive has no durable root")
        return self.root / f"{sha256_json({'request_id': request_id})}.json"

    def append(self, artifact: ForecastArtifactV2) -> None:
        previous = self.get(artifact.request_id)
        if previous is not None and previous.content_hash != artifact.content_hash:
            raise ValueError("request_id already archived with different forecast content")
        if previous is None and self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)
            target = self._path(artifact.request_id)
            temporary = target.with_suffix(".json.tmp")
            temporary.write_text(canonical_json(artifact.to_dict()), encoding="utf-8")
            if target.exists():
                existing = self._load(target)
                temporary.unlink(missing_ok=True)
                if existing.content_hash != artifact.content_hash:
                    raise ValueError("request_id already archived with different forecast content")
            else:
                temporary.replace(target)
        self._artifacts.setdefault(artifact.request_id, artifact)

    def get(self, request_id: str) -> ForecastArtifactV2 | None:
        value = self._artifacts.get(request_id)
        if value is None and self.root is not None:
            target = self._path(request_id)
            if target.exists():
                value = self._load(target)
                if value.request_id != request_id:
                    raise RuntimeError("stored forecast archive identity mismatch")
                self._artifacts[request_id] = value
        return value

    @staticmethod
    def _load(path: Path) -> ForecastArtifactV2:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return ForecastArtifactV2.from_dict(value)
        except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise RuntimeError("stored forecast archive is corrupt or has an unknown schema") from exc


class ModelArenaV2:
    def __init__(self, *, max_queue_size: int, clock_ns: Callable[[], int], archive: ForecastArchiveV2 | None = None) -> None:
        if type(max_queue_size) is not int or max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer")
        self.max_queue_size = max_queue_size
        self.clock_ns = clock_ns
        self.archive = archive or ForecastArchiveV2()
        self._queue: deque[QueuedModelRequestV2] = deque()
        self._request_keys: set[str] = set()
        self._request_payloads: dict[str, tuple[str, str, str]] = {}

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    def enqueue(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        provider_key: str,
    ) -> str:
        if manifest.manifest_hash != request.model_manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "request manifest hash does not match provider manifest")
        try:
            frozen_inputs = json.loads(canonical_json(dict(inputs)))
        except (TypeError, ValueError) as exc:
            raise ModelProviderError("INPUT_SERIALIZATION_INVALID", "model inputs are not deterministic JSON data") from exc
        if not isinstance(frozen_inputs, dict):
            raise ModelProviderError("INPUT_SERIALIZATION_INVALID", "model input root must be a JSON object")
        payload_identity = (provider_key, manifest.manifest_hash, sha256_json(frozen_inputs))
        if request.request_id in self._request_keys:
            if self._request_payloads[request.request_id] != payload_identity:
                raise ModelProviderError("IDEMPOTENCY_CONFLICT", "request_id was reused with different immutable input")
            return request.request_id
        if len(self._queue) >= self.max_queue_size:
            raise ModelProviderError("QUEUE_FULL", "bounded Model Arena queue is full")
        now = self.clock_ns()
        self._queue.append(QueuedModelRequestV2(request, manifest, frozen_inputs, provider_key, now))
        self._request_keys.add(request.request_id)
        self._request_payloads[request.request_id] = payload_identity
        return request.request_id

    def run_next(
        self,
        provider: ModelProvider,
        *,
        provider_key: str,
        retries: int = 0,
        retry_wait: Callable[[int], None] | None = None,
    ) -> ModelRunV2 | None:
        if not self._queue:
            return None
        queued = self._queue.popleft()
        request = queued.request
        now = self.clock_ns()
        queue_wait = max(0, now - queued.enqueued_at_ns)
        if now >= request.deadline_ns:
            return ModelRunV2(
                request.request_id, ProviderStateV2.STALE_DISCARDED, None, False, "STALE_BEFORE_EXECUTION",
                request.input_hash, request.model_manifest_hash, queue_wait, 0,
            )
        if provider_key != queued.provider_key:
            return ModelRunV2(
                request.request_id, ProviderStateV2.FAILED, None, False, "PROVIDER_UNAVAILABLE",
                request.input_hash, request.model_manifest_hash, queue_wait, 0,
            )
        started = now
        attempts = 0
        while True:
            try:
                artifact = provider.infer(request, queued.manifest, queued.inputs, started_at_ns=started)
                break
            except ModelProviderError as exc:
                if not exc.retryable or attempts >= retries:
                    return ModelRunV2(
                        request.request_id, ProviderStateV2.FAILED, None, False, exc.code,
                        request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started),
                    )
                attempts += 1
                if retry_wait is not None:
                    retry_wait(attempts)
                if self.clock_ns() >= request.deadline_ns:
                    return ModelRunV2(
                        request.request_id, ProviderStateV2.STALE_DISCARDED, None, False, "RETRY_DEADLINE_EXPIRED",
                        request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started),
                    )
            except Exception:
                return ModelRunV2(
                    request.request_id, ProviderStateV2.FAILED, None, False, "PROVIDER_EXCEPTION",
                    request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started),
                )
        if artifact.request_id != request.request_id:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "REQUEST_ID_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.input_hash != request.input_hash:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "INPUT_HASH_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.model_manifest_hash != queued.manifest.manifest_hash:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "MANIFEST_HASH_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.expires_ns != request.deadline_ns:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "DEADLINE_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.targets != request.requested_targets:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "TARGETS_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.horizons != request.requested_horizons:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "HORIZONS_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        if artifact.native_quantiles != request.requested_quantiles:
            return ModelRunV2(request.request_id, ProviderStateV2.FAILED, artifact, False, "QUANTILES_MISMATCH", request.input_hash, request.model_manifest_hash, queue_wait, max(0, self.clock_ns() - started))
        self.archive.append(artifact)
        now = self.clock_ns()
        elapsed = max(0, now - started)
        usable = (
            artifact.status in (ForecastStatusV2.AVAILABLE, ForecastStatusV2.PARTIAL)
            and not artifact.missing_outputs
            and artifact.completed_ns <= now
            and artifact.received_ns <= now
            and artifact.received_ns <= request.deadline_ns
            and now <= request.deadline_ns
            and artifact.is_usable_at(now)
        )
        state = ProviderStateV2.AVAILABLE if usable else ProviderStateV2.PARTIAL if artifact.status == ForecastStatusV2.PARTIAL else ProviderStateV2.FAILED
        failure = None if usable else ("LATE_OR_EXPIRED" if artifact.received_ns > request.deadline_ns or now > request.deadline_ns else "OUTPUT_INCOMPLETE_OR_FAILED")
        return ModelRunV2(request.request_id, state, artifact, usable, failure, request.input_hash, queued.manifest.manifest_hash, queue_wait, elapsed)


class DeterministicFakeProvider(ModelProvider):
    """Tiny deterministic forecast provider for ABI/integration tests only."""

    def __init__(self, *, clock_ns: Callable[[], int], compute_ns: int = 0, receive_lag_ns: int = 0, missing_outputs: tuple[str, ...] = ()) -> None:
        self.clock_ns = clock_ns
        self.compute_ns = compute_ns
        self.receive_lag_ns = receive_lag_ns
        self.missing_outputs = tuple(sorted(set(missing_outputs)))
        self.calls: list[tuple[str, int]] = []

    def infer(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        started_at_ns: int,
    ) -> ForecastArtifactV2:
        if manifest.manifest_hash != request.model_manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "provider manifest changed")
        self.calls.append((request.request_id, request.deadline_ns))
        completed = max(started_at_ns + self.compute_ns, self.clock_ns())
        received = completed + self.receive_lag_ns
        missing = self.missing_outputs
        status = ForecastStatusV2.PARTIAL if missing else ForecastStatusV2.AVAILABLE
        values_hash = sha256_json({"targets": request.requested_targets, "horizons": request.requested_horizons, "fixture": "zero-return"})
        return ForecastArtifactV2(
            request_id=request.request_id,
            model_manifest_hash=manifest.manifest_hash,
            input_hash=request.input_hash,
            inference_started_ns=started_at_ns,
            completed_ns=completed,
            received_ns=received,
            expires_ns=request.deadline_ns,
            targets=request.requested_targets,
            horizons=request.requested_horizons,
            native_quantiles=request.requested_quantiles,
            values_ref=values_hash,
            samples_ref=None,
            missing_outputs=missing,
            units=FrozenMap(dict.fromkeys(request.requested_targets, "log_return")),
            resource_metrics=FrozenMap({"provider": "deterministic-fake-v1", "input_fields": len(inputs)}),
            status=status,
        )
