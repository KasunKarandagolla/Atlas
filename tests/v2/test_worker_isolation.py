"""Adversarial qualification of the real Linux local-worker sandbox."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from atlas.v2.models.local_process import _WORKER_PROGRAM, LocalProcessProvider
from atlas.v2.models.sandbox import BubblewrapSandboxV2
from atlas.v2.models.worker_protocol import ModelProviderError


def _run(sandbox: BubblewrapSandboxV2, program: str) -> dict[str, object]:
    result = subprocess.run(
        sandbox.command(program), capture_output=True, text=True, timeout=5,
        cwd="/", close_fds=True, check=False,
        env=LocalProcessProvider.allowlisted_environment(),
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)  # type: ignore[no-any-return]


def test_real_bubblewrap_denies_exact_fake_secrets_host_proc_and_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with tempfile.TemporaryDirectory(prefix="atlas-fake-home-", dir=Path.home()) as home_dir:
        fake_home = Path(home_dir)
        protected = {
            "exchange": fake_home / "exchange-secret.txt",
            "env": fake_home / ".env",
            "database": tmp_path / "v1-live-control.sqlite",
            "account": fake_home / "account-private-token.txt",
        }
        for name, path in protected.items():
            path.write_text(f"FAKE-{name}-TEST-VALUE", encoding="utf-8")
        marker = "FAKE-PARENT-ENV-TEST-VALUE"
        monkeypatch.setenv("ATLAS_FAKE_SESSION_SECRET", marker)
        parent_pid = os.getpid()
        program = f'''
import json, os, socket
paths = {repr({name: str(path) for name, path in protected.items()})}
readable = {{name: os.path.exists(path) or _read(path) for name, path in paths.items()}}
'''
        # Define the read attempt before evaluating every exact, source-embedded pathname.
        program = program.replace("readable =", '''def _read(path):
    try:
        with open(path, "rb") as source: source.read()
        return True
    except OSError:
        return False
readable =''')
        program += f'''
proc_root_readable = {{name: os.path.exists("/proc/1/root" + path) or _read("/proc/1/root" + path)
                      for name, path in paths.items()}}
home_names = os.listdir({str(Path.home())!r})
proc_pids = [int(name) for name in os.listdir("/proc") if name.isdigit()]
try:
    parent_env = open("/proc/{parent_pid}/environ", "rb").read().decode(errors="ignore")
except OSError:
    parent_env = ""
found_env = []
for pid in proc_pids:
    try:
        found_env.append(open(f"/proc/{{pid}}/environ", "rb").read().decode(errors="ignore"))
    except OSError:
        pass
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1)
try:
    sock.connect(("1.1.1.1", 443))
    network_connected = True
except OSError:
    network_connected = False
finally:
    sock.close()
print(json.dumps({{
    "readable": readable, "proc_root_readable": proc_root_readable, "home_names": home_names,
    "parent_pid_visible": {parent_pid} in proc_pids,
    "parent_env_leaked": {marker!r} in parent_env,
    "any_proc_env_leaked": any({marker!r} in item for item in found_env),
    "child_env_leaked": "ATLAS_FAKE_SESSION_SECRET" in os.environ,
    "pid_namespace_isolated": os.readlink("/proc/self/ns/pid") != {os.readlink('/proc/self/ns/pid')!r},
    "net_namespace_isolated": os.readlink("/proc/self/ns/net") != {os.readlink('/proc/self/ns/net')!r},
    "network_connected": network_connected,
}}))
'''
        result = _run(BubblewrapSandboxV2(), program)
        assert result["readable"] == dict.fromkeys(protected, False)
        assert result["proc_root_readable"] == dict.fromkeys(protected, False)
        assert isinstance(result["home_names"], list)
        assert fake_home.name not in result["home_names"]
        assert "Music" not in result["home_names"]
        assert not result["parent_pid_visible"]
        assert not result["parent_env_leaked"] and not result["any_proc_env_leaked"]
        assert not result["child_env_leaked"]
        assert result["pid_namespace_isolated"] and result["net_namespace_isolated"]
        assert not result["network_connected"]


def test_approved_artifact_is_read_only_and_only_tmp_is_writable(tmp_path: Path) -> None:
    artifact = tmp_path / "fixture-model.bin"
    artifact.write_bytes(b"FAKE-MODEL-FIXTURE")
    hidden_neighbor = tmp_path / "unapproved-sample.txt"
    hidden_neighbor.write_text("FAKE-UNAPPROVED-VALUE", encoding="utf-8")
    result = _run(BubblewrapSandboxV2((artifact,)), '''
import json, os
with open("/artifacts/0", "rb") as source: value = source.read().decode()
def can_write(path):
    try:
        with open(path, "wb") as target: target.write(b"changed")
        return True
    except OSError:
        return False
tmp_path = "/tmp/worker-fixture"
with open(tmp_path, "wb") as target: target.write(b"temporary")
print(json.dumps({"value": value, "artifact_writable": can_write("/artifacts/0"),
    "root_writable": can_write("/outside"), "tmp_value": open(tmp_path, "rb").read().decode(),
    "neighbor_visible": os.path.exists("'''+str(hidden_neighbor)+'''"),
    "host_tmp_visible": os.path.exists("'''+str(artifact)+'''"),
}))
''')
    assert result == {
        "value": "FAKE-MODEL-FIXTURE", "artifact_writable": False,
        "root_writable": False, "tmp_value": "temporary", "neighbor_visible": False,
        "host_tmp_visible": False,
    }
    assert artifact.read_bytes() == b"FAKE-MODEL-FIXTURE"


def test_inherited_descriptor_cannot_read_protected_file(tmp_path: Path) -> None:
    protected = tmp_path / "fake-exchange-secret.txt"
    protected.write_bytes(b"FAKE-DESCRIPTOR-TEST-VALUE")
    with protected.open("rb") as source:
        descriptor = os.dup(source.fileno())
        try:
            os.set_inheritable(descriptor, True)
            program = f'''
import json, os
try:
    data = os.read({descriptor}, 100).decode()
except OSError:
    data = ""
print(json.dumps({{"descriptor_leaked": "FAKE-DESCRIPTOR-TEST-VALUE" in data,
                  "exact_path_visible": os.path.exists({str(protected)!r})}}))
'''
            result = _run(BubblewrapSandboxV2(), program)
        finally:
            os.close(descriptor)
    assert result == {"descriptor_leaked": False, "exact_path_visible": False}


def test_sandbox_mount_validation_and_missing_backend_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = tmp_path / "model.bin"
    fixture.write_bytes(b"fixture")
    for paths in ((Path.home(),), (Path("/"),), (Path("/tmp"),), (fixture, fixture), (tmp_path / "missing",)):
        with pytest.raises(ModelProviderError) as error:
            BubblewrapSandboxV2(paths)
        assert error.value.code == "WORKER_MOUNT_UNSAFE"
    secret = tmp_path / ".env"
    secret.write_text("FAKE=VALUE", encoding="utf-8")
    with pytest.raises(ModelProviderError, match="protected path"):
        BubblewrapSandboxV2((secret,))
    disguised_db = tmp_path / "model-checkpoint.bin"
    disguised_db.write_bytes(b"SQLite format 3\x00" + b"FAKE-DATABASE-CONTENT")
    with pytest.raises(ModelProviderError, match="SQLite database"):
        BubblewrapSandboxV2((disguised_db,))
    monkeypatch.setattr("atlas.v2.models.sandbox.shutil.which", lambda name: None)
    with pytest.raises(ModelProviderError) as error:
        LocalProcessProvider()
    assert error.value.code == "WORKER_SANDBOX_UNAVAILABLE"


def test_provider_retains_os_limits_and_timeout_cleanup() -> None:
    # The worker checks its actual inherited kernel limits, then emits the valid fixture response.
    prelude = '''import resource
assert resource.getrlimit(resource.RLIMIT_AS)[0] <= 256 * 1024 * 1024
assert resource.getrlimit(resource.RLIMIT_FSIZE)[0] <= 65_537
assert resource.getrlimit(resource.RLIMIT_CPU)[0] <= 5
'''
    from tests.v2.test_model_runtime import manifest, request

    model = manifest()
    now = time.time_ns()
    req = request(model, now_ns=now, cutoff=now - 1, deadline=now + 10_000_000_000)
    artifact = LocalProcessProvider(max_memory_mb=256, timeout_s=3, worker_program=prelude + _WORKER_PROGRAM).infer(
        req, model, {"features": [1]}, started_at_ns=now,
    )
    assert artifact.request_id == req.request_id
    marker = f"atlas-worker-timeout-{os.getpid()}-{time.time_ns()}"
    sleeper = f'''import subprocess, sys, time
subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30) # {marker}"], start_new_session=True)
time.sleep(30)
'''
    with pytest.raises(ModelProviderError) as error:
        LocalProcessProvider(timeout_s=1.0, worker_program=sleeper).infer(req, model, {}, started_at_ns=now)
    assert error.value.code == "WORKER_TIMEOUT"
    for pid in Path("/proc").iterdir():
        if not pid.name.isdigit():
            continue
        try:
            command = (pid / "cmdline").read_bytes()
        except OSError:
            continue
        assert marker.encode() not in command
    with pytest.raises(ModelProviderError) as error:
        LocalProcessProvider(max_output_bytes=100, worker_program="print('x'*1000)").infer(
            req, model, {}, started_at_ns=now,
        )
    assert error.value.code == "WORKER_OUTPUT_TOO_LARGE"
