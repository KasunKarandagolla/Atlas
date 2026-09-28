"""One-shot, namespace-sandboxed worker. It only talks to a mounted broker socket."""

from __future__ import annotations

import json
import os
import socket
import stat
import struct
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, strict_fields
from atlas.v2.agent_intelligence.broker import (
    InferenceBrokerClient,
    _result_wire,
)
from atlas.v2.agent_intelligence.contracts import ProviderResultV1, ResearchProposalRequestV1
from atlas.v2.models.worker_protocol import _reject_sensitive_fields

MAX_WORKER_STDIN_BYTES = 256_000


class WorkerSandboxUnavailable(RuntimeError):
    pass


def _limits() -> None:
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))
    resource.setrlimit(resource.RLIMIT_NOFILE, (24, 24))
    resource.setrlimit(resource.RLIMIT_CPU, (40, 40))
    resource.setrlimit(resource.RLIMIT_AS, (1_500_000_000, 1_500_000_000))
    os.environ.clear()
    os.environ.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": "/home/atlas/src",
                       "ATLAS_AGENT_BROKER_SOCKET": "/run/atlas-agent-broker.sock",
                       "ATLAS_AGENT_WORKER_SANDBOX": "1"})
    os.chdir("/")


def _read_stdin() -> dict[str, Any]:
    header = sys.stdin.buffer.read(4)
    if len(header) != 4:
        raise ValueError("WORKER_INPUT_TRUNCATED")
    (length,) = struct.unpack("!I", header)
    if not 0 < length <= MAX_WORKER_STDIN_BYTES:
        raise ValueError("WORKER_INPUT_SIZE_LIMIT")
    body = sys.stdin.buffer.read(length)
    if len(body) != length:
        raise ValueError("WORKER_INPUT_TRUNCATED")
    value = json.loads(body.decode("utf-8"))
    fields = {"version", "capability", "job_id", "attempt_id", "lease_epoch", "call_index", "request", "evidence"}
    payload = dict(strict_fields(value, expected=fields, required=fields, name="AgentWorkerInputV1"))
    _reject_sensitive_fields(payload["request"], field_name="agent_request")
    _reject_sensitive_fields(payload["evidence"], field_name="bounded_evidence")
    return payload


def _worker_main() -> int:
    request_data = _read_stdin()
    request = ResearchProposalRequestV1.from_dict(request_data["request"])
    evidence = request_data["evidence"]
    if not isinstance(evidence, list) or len(evidence) != len(request.evidence_manifest):
        raise ValueError("EVIDENCE_SCOPE_MISMATCH")
    broker_path = os.environ.get("ATLAS_AGENT_BROKER_SOCKET")
    if broker_path != "/run/atlas-agent-broker.sock":
        raise ValueError("BROKER_CAPABILITY_CHANNEL_MISSING")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(2.0)
        sock.connect(broker_path)
        result = InferenceBrokerClient(sock).propose(capability=request_data["capability"],
            job_id=request_data["job_id"], attempt_id=request_data["attempt_id"],
            lease_epoch=request_data["lease_epoch"], call_index=request_data["call_index"],
            request=request, evidence=evidence)
        output = {"version": "AgentWorkerOutputV1", "ok": True, "result": _result_wire(result)}
    finally:
        sock.close()
    body = canonical_json(output).encode("utf-8")
    if len(body) > MAX_WORKER_STDIN_BYTES:
        raise ValueError("WORKER_OUTPUT_SIZE_LIMIT")
    sys.stdout.buffer.write(struct.pack("!I", len(body)) + body)
    sys.stdout.buffer.flush()
    return 0


def worker_main() -> int:
    """Console script. The supervisor normally invokes this inside bubblewrap."""
    try:
        if os.environ.get("ATLAS_AGENT_WORKER_SANDBOX") != "1":
            raise WorkerSandboxUnavailable("agent worker requires the namespace sandbox")
        return _worker_main()
    except Exception:
        # Never echo provider text, capability values, paths, or exception details.
        sys.stderr.write("AGENT_WORKER_FAILED\n")
        return 2


def _sandbox_command(broker_socket: str | Path, bwrap: str) -> list[str]:
    socket_path = Path(broker_socket)
    try:
        socket_metadata = socket_path.lstat()
    except OSError as exc:
        raise WorkerSandboxUnavailable("inference broker socket is unavailable") from exc
    if not stat.S_ISSOCK(socket_metadata.st_mode):
        raise WorkerSandboxUnavailable("inference broker endpoint is not a local socket")
    repo_root = Path(__file__).resolve().parents[4]
    source_root = repo_root / "src"
    executable = Path(sys.executable).resolve()
    runtime_root = Path(sys.base_prefix).resolve()
    python_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages = Path(sys.prefix).resolve() / "lib" / python_version / "site-packages"
    command = [bwrap, "--die-with-parent", "--unshare-all", "--new-session", "--clearenv"]
    for directory in ("/usr", "/lib", "/lib64"):
        path = Path(directory)
        if path.exists():
            command.extend(("--ro-bind", str(path), str(path)))
    mount_targets = (runtime_root, repo_root / ".venv", Path("/home/atlas/src"))
    directories: set[Path] = {Path("/home"), Path("/home/atlas"), Path("/run"), *mount_targets}
    for target in mount_targets:
        cursor = target.parent
        while cursor != Path("/"):
            directories.add(cursor)
            cursor = cursor.parent
    for mount_dir in sorted(directories, key=lambda item: (len(item.parts), str(item))):
        command.extend(("--dir", str(mount_dir)))
    command.extend(("--ro-bind", str(runtime_root), str(runtime_root)))
    venv = repo_root / ".venv"
    if venv.exists():
        command.extend(("--ro-bind", str(venv), str(venv)))
    command.extend(("--ro-bind", str(source_root), "/home/atlas/src"))
    command.extend(("--ro-bind", str(socket_path), "/run/atlas-agent-broker.sock"))
    command.extend(("--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--chdir", "/"))
    command.extend(("--setenv", "PYTHONDONTWRITEBYTECODE", "1"))
    command.extend(("--setenv", "PYTHONPATH", f"/home/atlas/src:{site_packages}"))
    command.extend(("--setenv", "ATLAS_AGENT_BROKER_SOCKET", "/run/atlas-agent-broker.sock"))
    command.extend(("--setenv", "ATLAS_AGENT_WORKER_SANDBOX", "1"))
    command.extend((str(executable), "-m", "atlas.v2.agent_intelligence.worker"))
    return command


class AgentWorkerSupervisor:
    """Runs a single-use worker behind network/filesystem namespaces and a mounted broker socket."""

    def __init__(self, broker_socket: str | Path, *, bwrap: str = "/usr/bin/bwrap") -> None:
        socket_path = Path(broker_socket)
        if not socket_path.is_absolute() or len(str(socket_path)) > 100:
            raise ValueError("inference broker socket must be an absolute short local path")
        self._broker_socket = str(socket_path)
        self._bwrap = bwrap

    def infer(self, *, capability: str, job_id: str, attempt_id: str, lease_epoch: int, call_index: int,
              request: ResearchProposalRequestV1, evidence: Sequence[Mapping[str, Any]],
              deadline_ns: int) -> ProviderResultV1:
        if not Path(self._bwrap).is_file():
            raise WorkerSandboxUnavailable("bubblewrap namespace sandbox is unavailable")
        payload = {"version": "AgentWorkerInputV1", "capability": capability, "job_id": job_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch, "call_index": call_index,
            "request": request.to_dict(), "evidence": list(evidence)}
        encoded = canonical_json(payload).encode("utf-8")
        if not encoded or len(encoded) > MAX_WORKER_STDIN_BYTES:
            raise ValueError("worker request exceeds its IPC bound")
        remaining = max(0.1, (deadline_ns - time.time_ns()) / 1_000_000_000)
        try:
            process = subprocess.run(_sandbox_command(self._broker_socket, self._bwrap),
                input=struct.pack("!I", len(encoded)) + encoded, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, timeout=min(remaining, 40), check=False,
                close_fds=True, preexec_fn=_limits,
                env={"PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": "/home/atlas/src",
                     "ATLAS_AGENT_BROKER_SOCKET": "/run/atlas-agent-broker.sock"}, cwd="/")
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise WorkerSandboxUnavailable("isolated worker dispatch failed or exceeded its deadline") from exc
        if process.returncode != 0 or len(process.stdout) < 4:
            raise WorkerSandboxUnavailable("isolated worker did not return a complete result")
        (size,) = struct.unpack("!I", process.stdout[:4])
        if size == 0 or size > MAX_WORKER_STDIN_BYTES or len(process.stdout) != size + 4:
            raise WorkerSandboxUnavailable("isolated worker response is malformed")
        response = json.loads(process.stdout[4:].decode("utf-8"))
        if not isinstance(response, Mapping) or response.get("version") != "AgentWorkerOutputV1" \
                or response.get("ok") is not True or not isinstance(response.get("result"), Mapping):
            raise WorkerSandboxUnavailable("isolated worker response is invalid")
        from .broker import _result_from_wire
        return _result_from_wire(response["result"])


if __name__ == "__main__":  # pragma: no cover - exercised through the isolated subprocess entry point
    raise SystemExit(worker_main())
