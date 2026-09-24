"""Bounded local worker launched only inside an OS sandbox."""

from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from .._serialization import sha256_json
from .protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2
from .provider import ModelProvider
from .sandbox import BubblewrapSandboxV2
from .worker_protocol import ModelProviderError, WorkerRequestV2, canonical_worker_request, strict_worker_response

_WORKER_PROGRAM = r'''import hashlib,json,sys,time
raw=sys.stdin.buffer.read(1_048_577)
if len(raw)>1_048_576: raise SystemExit(21)
r=json.loads(raw)
if set(r)!={"schema_version","request","manifest","inputs"} or r["schema_version"]!=1: raise SystemExit(22)
q=r["request"]
m=r["manifest"]
started=time.time_ns()
values={"targets":q["requested_targets"],"horizons":q["requested_horizons"],"fixture":"deterministic-zero"}
encoded=json.dumps(values,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
answer={"schema_version":1,"request_id":q["request_id"],"model_manifest_hash":hashlib.sha256(json.dumps({"contract_type":"ModelManifestV2","manifest":m},sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest(),"input_hash":q["input_hash"],"inference_started_ns":started,"completed_ns":time.time_ns(),"received_ns":time.time_ns(),"expires_ns":q["deadline_ns"],"targets":q["requested_targets"],"horizons":q["requested_horizons"],"native_quantiles":q["requested_quantiles"],"values_ref":hashlib.sha256(encoded).hexdigest(),"samples_ref":None,"missing_outputs":[],"units":{target:"log_return" for target in q["requested_targets"]},"resource_metrics":{"worker":"deterministic-fixture-v1","input_keys":len(r["inputs"])},"status":"AVAILABLE"}
sys.stdout.write(json.dumps(answer,sort_keys=True,separators=(",",":")))
'''

_ENV_ALLOWLIST = ("LANG", "LC_ALL", "TZ", "PYTHONIOENCODING")


class LocalProcessProvider(ModelProvider):
    def __init__(
        self,
        *,
        clock_ns=time.time_ns,
        max_input_bytes: int = 1_048_576,
        max_output_bytes: int = 65_536,
        max_memory_mb: int = 1024,
        timeout_s: float = 5.0,
        worker_program: str = _WORKER_PROGRAM,
        approved_read_only_paths: tuple[str | Path, ...] = (),
    ) -> None:
        if max_input_bytes <= 0 or max_output_bytes <= 0 or max_memory_mb <= 0 or timeout_s <= 0:
            raise ValueError("local worker bounds must be positive")
        self.clock_ns = clock_ns
        self.max_input_bytes = max_input_bytes
        self.max_output_bytes = max_output_bytes
        self.max_memory_mb = max_memory_mb
        self.timeout_s = timeout_s
        self._worker_program = worker_program
        self.sandbox = BubblewrapSandboxV2(approved_read_only_paths)

    @staticmethod
    def allowlisted_environment(parent: Mapping[str, str] | None = None) -> dict[str, str]:
        source = os.environ if parent is None else parent
        return {"PATH": "/usr/bin:/bin", **{key: source[key] for key in _ENV_ALLOWLIST if key in source}}

    @staticmethod
    def probe_environment_keys(parent: Mapping[str, str] | None = None) -> tuple[str, ...]:
        program = "import json,os;print(json.dumps(sorted(os.environ)))"
        sandbox = BubblewrapSandboxV2()
        try:
            result = subprocess.run(
                sandbox.command(program),
                check=False,
                capture_output=True,
                timeout=2,
                env=LocalProcessProvider.allowlisted_environment(parent),
                cwd=os.path.abspath(os.sep),
                close_fds=True,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "isolated worker sandbox probe failed") from exc
        if result.returncode != 0:
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "isolated worker sandbox probe failed")
        value = json.loads(result.stdout)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise ModelProviderError("WORKER_PROBE_MALFORMED", "isolated worker environment probe was malformed")
        return tuple(value)

    def infer(
        self,
        request: ModelRequestV2,
        manifest: ModelManifestV2,
        inputs: Mapping[str, Any],
        *,
        started_at_ns: int,
    ) -> ForecastArtifactV2:
        if manifest.manifest_hash != request.model_manifest_hash:
            raise ModelProviderError("MANIFEST_MISMATCH", "worker manifest hash does not match request")
        worker_request = WorkerRequestV2(request, manifest, dict(inputs))
        payload = canonical_worker_request(worker_request)
        if len(payload) > self.max_input_bytes:
            raise ModelProviderError("WORKER_INPUT_TOO_LARGE", "worker input exceeds configured byte limit")
        remaining_s = min(self.timeout_s, max(0.0, (request.deadline_ns - self.clock_ns()) / 1_000_000_000))
        if remaining_s <= 0:
            raise ModelProviderError("WORKER_DEADLINE_EXPIRED", "request expired before worker launch")
        if os.name != "posix":
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "local worker sandbox requires POSIX limits")
        environment = self.allowlisted_environment()

        def apply_resource_limits() -> None:
            import resource

            output_limit = self.max_output_bytes + 1
            resource.setrlimit(resource.RLIMIT_FSIZE, (output_limit, output_limit))
            cpu_limit = max(1, math.ceil(remaining_s))
            resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit, cpu_limit + 1))
            memory_limit = self.max_memory_mb * 1024 * 1024
            if hasattr(resource, "RLIMIT_AS"):
                resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

        try:
            with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
                process = subprocess.Popen(
                    self.sandbox.command(self._worker_program),
                    stdin=subprocess.PIPE,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    env=environment,
                    cwd=os.path.abspath(os.sep),
                    close_fds=True,
                    start_new_session=True,
                    preexec_fn=apply_resource_limits,
                )
                try:
                    process.communicate(input=payload, timeout=remaining_s)
                except subprocess.TimeoutExpired as exc:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                    raise ModelProviderError("WORKER_TIMEOUT", "local worker exceeded its original deadline") from exc
                # A worker must not leave helper processes running after its own response.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                stdout_file.seek(0)
                stdout = stdout_file.read(self.max_output_bytes + 1)
                stderr_file.seek(0)
                stderr = stderr_file.read(self.max_output_bytes + 1)
        except (OSError, subprocess.SubprocessError) as exc:
            raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", f"local worker sandbox could not start: {type(exc).__name__}") from exc
        if len(stdout) > self.max_output_bytes or len(stderr) > self.max_output_bytes:
            raise ModelProviderError("WORKER_OUTPUT_TOO_LARGE", "local worker output exceeded configured byte limit")
        if process.returncode != 0:
            if b"bwrap:" in stderr:
                raise ModelProviderError("WORKER_SANDBOX_UNAVAILABLE", "local worker sandbox setup failed")
            raise ModelProviderError("WORKER_CRASH", f"local worker exited with status {process.returncode}")
        try:
            response = strict_worker_response(json.loads(stdout))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise ModelProviderError("WORKER_RESPONSE_INVALID", "local worker returned malformed or unknown-schema output") from exc
        if response.request_id != request.request_id:
            raise ModelProviderError("REQUEST_ID_MISMATCH", "local worker response request_id mismatch")
        if response.model_manifest_hash != manifest.manifest_hash or response.input_hash != request.input_hash:
            raise ModelProviderError("WORKER_HASH_MISMATCH", "local worker response hash mismatch")
        received = max(self.clock_ns(), response.completed_ns)
        return replace(response, received_ns=received)


def fixture_manifest_hash(manifest: ModelManifestV2) -> str:
    """Small helper used by tests to assert deterministic manifest identity."""
    return sha256_json({"contract_type": "ModelManifestV2", "manifest": manifest.to_dict()})
