"""Foreground, durable and resumable public-data soak runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .binance import BinanceUsdMPublicReaderV2
from .bybit import BybitPublicReaderV2
from .public_http import PublicDataError

_NS_PER_HOUR = 3_600_000_000_000


def _utc(at_ns: int) -> str:
    return datetime.fromtimestamp(at_ns / 1_000_000_000, tz=UTC).isoformat()


def _public_probe() -> dict[str, Any]:
    observations: dict[str, Any] = {}
    probes = (
        ("BYBIT_PUBLIC", BybitPublicReaderV2(), "server_time_ns", "ticker", "BTCUSDT"),
        ("BINANCE_USDM_PUBLIC", BinanceUsdMPublicReaderV2(), "server_time_ns", "book_ticker", "BTCUSDT"),
    )
    for source_id, reader, time_method, data_method, symbol in probes:
        try:
            server_time = getattr(reader, time_method)()
            response = getattr(reader, data_method)(symbol)
            observations[source_id] = {
                "state": "HEALTHY_CURRENT",
                "server_time_ns": server_time,
                "received_at_ns": response.received_at_ns,
                "http_status": response.status_code,
                "response_bytes": len(response.raw_body),
                "payload_sha256": hashlib.sha256(response.raw_body).hexdigest(),
            }
        except Exception as exc:
            state = "DEGRADED_RATE_LIMITED" if isinstance(exc, PublicDataError) and exc.rate_limited else "DISCONNECTED"
            observations[source_id] = {"state": state, "failure_type": type(exc).__name__}
    return observations


class PublicSoakRunnerV2:
    """Runs probes in the foreground and fsyncs each sanitized sample."""

    def __init__(
        self,
        *,
        probe: Callable[[], Mapping[str, Any]] = _public_probe,
        clock_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.probe = probe
        self.clock_ns = clock_ns
        self.sleep = sleep

    def run(
        self,
        evidence_path: str | Path,
        *,
        duration_ns: int = 72 * _NS_PER_HOUR,
        interval_s: float = 60,
        run_id: str | None = None,
        resume: bool = False,
    ) -> dict[str, Any]:
        path = Path(evidence_path)
        if duration_ns <= 0 or interval_s <= 0:
            raise ValueError("soak duration and interval must be positive")
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self.clock_ns()
        if resume:
            if not run_id or not path.exists():
                raise ValueError("resuming requires an existing evidence file and exact run_id")
            records = self._read(path)
            header = records[0]
            if header.get("record_type") != "HEADER" or header.get("run_id") != run_id:
                raise ValueError("soak resume run_id does not match the evidence header")
            if any(record.get("record_type") == "END" for record in records):
                raise ValueError("completed soak evidence cannot be resumed")
            if header.get("duration_ns") != duration_ns:
                raise ValueError("soak resume duration differs from the original run")
            prior_samples = [record for record in records if record.get("record_type") == "SAMPLE"]
            last_at = int(prior_samples[-1]["observed_at_ns"]) if prior_samples else int(header["started_at_ns"])
            interrupted = now - last_at > max(2 * int(interval_s * 1_000_000_000), 60 * 1_000_000_000)
            resume_records = [record for record in records if record.get("record_type") == "RESUME"]
            previous_segment = (
                int(resume_records[-1]["continuous_segment_started_at_ns"])
                if resume_records
                else int(header.get("continuous_segment_started_at_ns", header["started_at_ns"]))
            )
            segment_started = now if interrupted else previous_segment
            header["continuous_segment_started_at_ns"] = segment_started
            self._append(path, {"record_type": "RESUME", "run_id": run_id, "observed_at_ns": now,
                                "observed_at_utc": _utc(now), "interrupted": interrupted,
                                "continuous_segment_started_at_ns": segment_started})
            already_failed = any(
                any(item.get("state") != "HEALTHY_CURRENT" for item in sample.get("sources", {}).values())
                for sample in prior_samples
            )
        else:
            if path.exists():
                raise ValueError("evidence path already exists; use --resume with its run_id")
            run_id = run_id or uuid.uuid4().hex
            header = {
                "schema_version": 1,
                "record_type": "HEADER",
                "run_id": run_id,
                "started_at_ns": now,
                "started_at_utc": _utc(now),
                "continuous_segment_started_at_ns": now,
                "duration_ns": duration_ns,
                "interval_s": interval_s,
                "environment": {"python": platform.python_version(), "platform": platform.system(),
                                "credential_mode": "PUBLIC_ONLY"},
            }
            self._append(path, header)
            already_failed = False
            segment_started = now

        healthy = not already_failed
        target_end = segment_started + duration_ns
        while self.clock_ns() < target_end:
            observed = self.clock_ns()
            sources = dict(self.probe())
            failed = any(not isinstance(item, Mapping) or item.get("state") != "HEALTHY_CURRENT" for item in sources.values())
            healthy = healthy and not failed
            self._append(path, {
                "record_type": "SAMPLE",
                "run_id": run_id,
                "observed_at_ns": observed,
                "observed_at_utc": _utc(observed),
                "sources": sources,
            })
            remaining_s = (target_end - self.clock_ns()) / 1_000_000_000
            if remaining_s > 0:
                self.sleep(min(interval_s, remaining_s))
        ended = self.clock_ns()
        result = {
            "run_id": run_id,
            "started_at_utc": _utc(segment_started),
            "ended_at_utc": _utc(ended),
            "continuous_duration_ns": max(0, ended - segment_started),
            "duration_ns": duration_ns,
            "status": "TESTED" if healthy and ended - segment_started >= duration_ns else "TEST GATE",
        }
        self._append(path, {"record_type": "END", "run_id": run_id, **result})
        return result

    @staticmethod
    def _append(path: Path, record: Mapping[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            import os

            os.fsync(stream.fileno())

    @staticmethod
    def _read(path: Path) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        try:
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"soak evidence line {line_number} is not an object")
                result.append(value)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("soak evidence is unreadable or corrupt") from exc
        if not result:
            raise ValueError("soak evidence is empty")
        return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, help="durable sanitized JSONL evidence path")
    parser.add_argument("--duration-hours", type=float, default=72.0)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--resume-run-id")
    arguments = parser.parse_args(argv)
    result = PublicSoakRunnerV2().run(
        arguments.evidence,
        duration_ns=int(arguments.duration_hours * _NS_PER_HOUR),
        interval_s=arguments.interval_seconds,
        run_id=arguments.resume_run_id,
        resume=arguments.resume_run_id is not None,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "TESTED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
