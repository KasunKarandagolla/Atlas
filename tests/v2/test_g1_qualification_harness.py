"""Qualification imports cannot convert partial or fabricated records to a pass."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from atlas.v2._serialization import sha256_json
from scripts.qualify_public_observation import PROFILE, VERSION, distribution, validate_result


def seal(body):
    body["content_hash"] = sha256_json({k: v for k, v in body.items() if k != "content_hash"})
    return body


def resource_fixture():
    sample = dict.fromkeys(
        [
            "offset_ns",
            "scheduler_delay_ns",
            "process_tree_count",
            "rss_bytes",
            "pss_bytes",
            "private_bytes",
            "process_tree_hwm_bytes",
            "available_memory_bytes",
            "cpu_ns",
            "wal_bytes",
            "temporary_bytes",
            "free_disk_bytes",
        ],
        1,
    )
    return {
        "sample_count": 2,
        "samples": [sample, dict(sample)],
        **dict.fromkeys(
            [
                "peak_rss_bytes",
                "process_tree_hwm_bytes",
                "peak_pss_bytes",
                "peak_private_bytes",
                "peak_wal_bytes",
                "peak_temporary_bytes",
            ],
            1,
        ),
    }


def fixture():
    result = {
        "profile": "normal",
        "population": {
            "offered": 512,
            "sealed": 512,
            "adopted": 512,
            "recovered_readback": 512,
            "rejected": 0,
            "lost": 0,
            "duplicate_transport": 0,
            "unavailable": 0,
        },
        "restart_readback_passes": 2,
        "latencies": {"adoption": distribution([1, 2, 3])},
    }
    return seal(
        {
            "version": VERSION,
            "profile": PROFILE,
            "profile_hash": sha256_json(PROFILE),
            "capacity_qualified": False,
            "actual_host_qualified": False,
            "network": "DISABLED",
            "authority": "ZERO",
            "capture": [result, {**result, "profile": "adverse"}],
            "resources": resource_fixture(),
            "breadth": None,
            "status": "TEST GATE",
            "maximum_breadth_requested": False,
            "storage_latencies": {"service": distribution([1, 2, 3])},
            "isolation": {"network_denied": 0, "subprocess_denied": 0},
        }
    )


@pytest.mark.parametrize(
    "change", ["hash", "population", "quantile", "empty_samples", "restart", "resources", "profile", "authority"]
)
def test_incomplete_or_rehashed_fabricated_records_are_rejected(change):
    body = fixture()
    if change == "hash":
        body["content_hash"] = "0" * 64
    elif change == "population":
        body["capture"][0]["population"]["sealed"] = 511
    elif change == "quantile":
        body["capture"][0]["latencies"]["adoption"]["p99_ns"] = 0
    elif change == "empty_samples":
        body["capture"][0]["latencies"]["adoption"]["samples_ns"] = []
    elif change == "restart":
        body["capture"][0]["restart_readback_passes"] = 0
    elif change == "resources":
        body["resources"]["sample_count"] = 0
    elif change == "profile":
        body["capture"] = body["capture"][:1]
    else:
        body["actual_host_qualified"] = True
    if change != "hash":
        seal(body)
    with pytest.raises((ValueError, KeyError)):
        validate_result(body)


def test_percentiles_are_nearest_rank_and_do_not_invent_samples():
    values = list(range(1, 101))
    metric = distribution(values)
    assert (metric["p50_ns"], metric["p95_ns"], metric["p99_ns"], metric["maximum_ns"]) == (50, 95, 99, 100)
    assert metric["count"] == 100
    for invalid in ([], [True], [-1], [1.5]):
        with pytest.raises(ValueError):
            distribution(invalid)


def test_worker_disables_network_subprocess_and_removes_credentials():
    code = """
import os,socket,subprocess
from scripts.qualify_public_observation import _fence
os.environ['BINANCE_FUTURES_DEMO_API_KEY']='synthetic-test-only'
a=_fence()
assert 'BINANCE_FUTURES_DEMO_API_KEY' not in os.environ
for action in [lambda:socket.getaddrinfo('example.invalid',443),
               lambda:socket.socket().connect(('127.0.0.1',9)),
               lambda:subprocess.run(['must-never-launch'])]:
 try: action()
 except PermissionError: pass
 else: raise AssertionError('side effect reachable')
assert a=={'network_denied':2,'subprocess_denied':1}
print('DISABLED')
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "DISABLED"


def test_public_diagnostic_is_separate_and_preserves_linux_restriction(tmp_path, monkeypatch):
    from atlas.v2.runtime import binance_qualification as public

    contract = public.profile_contract()
    assert contract["execution_profile"] == contract["provider_profile"] == "DISABLED"
    assert contract["credential_ref"] is None and contract["account_scope_ref"] is None
    monkeypatch.setattr(public.sys, "platform", "win32")
    with pytest.raises(ValueError, match="REQUIRES_LINUX"):
        public.validate_selected_path(tmp_path)


def test_capacity_probe_rejects_missing_maximum_breadth_population():
    body = fixture()
    body["breadth"] = {"population": 4095, "cycles": 3, "sample_count": 3, "integrity": {}}
    with pytest.raises((ValueError, KeyError)):
        validate_result(seal(body))


def test_fixture_records_never_grant_hardware_authority():
    # Even an internally consistent fixture is not authenticated provenance.
    body = fixture()
    validate_result(body)
    assert not body["actual_host_qualified"] and not body["capacity_qualified"]
    assert json.loads(json.dumps(body)) == body
