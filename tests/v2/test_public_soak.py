from __future__ import annotations

import json

import pytest

from atlas.v2.data.soak import PublicSoakRunnerV2


def test_soak_runner_writes_resumable_durable_sanitized_records(tmp_path) -> None:
    current = [0]

    def sleep(seconds: float) -> None:
        current[0] += int(seconds * 1_000_000_000)

    evidence_path = tmp_path / "soak.jsonl"
    runner = PublicSoakRunnerV2(
        probe=lambda: {"BYBIT_PUBLIC": {"state": "HEALTHY_CURRENT", "payload_sha256": "a" * 64}},
        clock_ns=lambda: current[0],
        sleep=sleep,
    )
    result = runner.run(evidence_path, duration_ns=2_000_000_000, interval_s=1, run_id="fixture-run")
    assert result["status"] == "TESTED"
    records = [json.loads(line) for line in evidence_path.read_text(encoding="utf-8").splitlines()]
    assert [item["record_type"] for item in records] == ["HEADER", "SAMPLE", "SAMPLE", "END"]
    assert all("raw_payload" not in json.dumps(item) for item in records)
    with pytest.raises(ValueError, match="completed"):
        runner.run(evidence_path, duration_ns=2_000_000_000, interval_s=1,
                   run_id="fixture-run", resume=True)


def test_soak_resume_records_interruption_and_starts_a_new_continuous_segment(tmp_path) -> None:
    current = [100_000_000_000]

    def sleep(seconds: float) -> None:
        current[0] += int(seconds * 1_000_000_000)

    evidence_path = tmp_path / "partial-soak.jsonl"
    header = {
        "schema_version": 1,
        "record_type": "HEADER",
        "run_id": "resume-me",
        "started_at_ns": 0,
        "started_at_utc": "1970-01-01T00:00:00+00:00",
        "continuous_segment_started_at_ns": 0,
        "duration_ns": 1_000_000_000,
        "interval_s": 1,
        "environment": {"credential_mode": "PUBLIC_ONLY"},
    }
    PublicSoakRunnerV2._append(evidence_path, header)
    PublicSoakRunnerV2._append(evidence_path, {
        "record_type": "SAMPLE", "run_id": "resume-me", "observed_at_ns": 1_000_000_000,
        "observed_at_utc": "1970-01-01T00:00:01+00:00",
        "sources": {"BYBIT_PUBLIC": {"state": "HEALTHY_CURRENT"}},
    })
    result = PublicSoakRunnerV2(
        probe=lambda: {"BYBIT_PUBLIC": {"state": "HEALTHY_CURRENT"}},
        clock_ns=lambda: current[0],
        sleep=sleep,
    ).run(evidence_path, duration_ns=1_000_000_000, interval_s=1, run_id="resume-me", resume=True)
    records = [json.loads(line) for line in evidence_path.read_text(encoding="utf-8").splitlines()]
    resume = next(item for item in records if item["record_type"] == "RESUME")
    assert resume["interrupted"] is True
    assert resume["continuous_segment_started_at_ns"] == 100_000_000_000
    assert result["continuous_duration_ns"] == 1_000_000_000
