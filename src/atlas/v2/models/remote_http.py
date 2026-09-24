"""TLS-only remote inference provider with original-deadline timeouts and no auth headers."""

from __future__ import annotations

import json
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .._serialization import canonical_json
from .protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2
from .provider import ModelProvider
from .worker_protocol import ModelProviderError, WorkerRequestV2, strict_worker_response


class JsonPoster(Protocol):
    def __call__(self, url: str, body: bytes, timeout_s: float, headers: Mapping[str, str]) -> tuple[int, bytes]: ...


def _tls_post(url: str, body: bytes, timeout_s: float, headers: Mapping[str, str]) -> tuple[int, bytes]:
    request = Request(url, data=body, method="POST", headers=dict(headers))
    try:
        with urlopen(request, timeout=timeout_s, context=ssl.create_default_context()) as response:
            return int(response.status), response.read(65_537)
    except HTTPError as exc:
        exc.read(1024)
        raise ModelProviderError(
            "REMOTE_HTTP_ERROR",
            f"remote worker returned HTTP {exc.code}",
            retryable=exc.code == 429 or 500 <= exc.code < 600,
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ModelProviderError("REMOTE_TIMEOUT", f"remote worker transport failed: {type(exc).__name__}", retryable=True) from exc


class RemoteHTTPProvider(ModelProvider):
    def __init__(
        self,
        endpoint: str,
        *,
        timeout_s: float = 3.0,
        max_response_bytes: int = 65_536,
        clock_ns: Callable[[], int] = time.time_ns,
        poster: JsonPoster = _tls_post,
    ) -> None:
        parsed = urlsplit(endpoint)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("remote inference endpoint must be an explicit HTTPS URL without credentials/query/fragment")
        if timeout_s <= 0 or max_response_bytes <= 0:
            raise ValueError("remote timeout and response bound must be positive")
        self.endpoint = endpoint
        self.timeout_s = timeout_s
        self.max_response_bytes = max_response_bytes
        self.clock_ns = clock_ns
        self.poster = poster

    def infer(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        started_at_ns: int,
    ) -> ForecastArtifactV2:
        if manifest.manifest_hash != request.model_manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "remote request manifest hash mismatch")
        remaining_s = min(self.timeout_s, max(0.0, (request.deadline_ns - self.clock_ns()) / 1_000_000_000))
        if remaining_s <= 0:
            raise ModelProviderError("REMOTE_DEADLINE_EXPIRED", "request deadline expired before remote call")
        body = canonical_json(WorkerRequestV2(request, manifest, dict(inputs)).to_dict()).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-ID": request.request_id,
            "Idempotency-Key": request.request_id,
        }
        try:
            status, response_body = self.poster(self.endpoint, body, remaining_s, headers)
        except ModelProviderError:
            raise
        except Exception as exc:
            raise ModelProviderError("REMOTE_TRANSPORT_FAILURE", f"remote transport failed: {type(exc).__name__}", retryable=True) from exc
        if len(response_body) > self.max_response_bytes:
            raise ModelProviderError("REMOTE_RESPONSE_TOO_LARGE", "remote response exceeded configured byte limit")
        if status == 429 or status >= 500:
            raise ModelProviderError("REMOTE_HTTP_ERROR", f"remote worker returned HTTP {status}", retryable=True)
        if status < 200 or status >= 300:
            raise ModelProviderError("REMOTE_HTTP_ERROR", f"remote worker returned HTTP {status}")
        try:
            response = strict_worker_response(json.loads(response_body))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise ModelProviderError("REMOTE_RESPONSE_INVALID", "remote response is malformed or uses an unknown schema") from exc
        if response.request_id != request.request_id:
            raise ModelProviderError("REQUEST_ID_MISMATCH", "remote response request_id mismatch")
        if response.model_manifest_hash != manifest.manifest_hash:
            raise ModelProviderError("MANIFEST_HASH_MISMATCH", "remote response manifest hash mismatch")
        if response.input_hash != request.input_hash:
            raise ModelProviderError("INPUT_HASH_MISMATCH", "remote response input hash mismatch")
        # received_ns is assigned by ATLAS after the network response arrives, never by the remote worker.
        received_at_ns = max(self.clock_ns(), response.completed_ns)
        return replace(response, received_ns=received_at_ns)
