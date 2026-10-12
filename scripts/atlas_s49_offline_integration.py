#!/usr/bin/env python3
"""Run the bounded, offline S49 production and lifecycle integration checks.

No authenticated exchange or external provider is configured by this runner.
The production agent-bridge assertion is intentionally strict and currently
reports TEST GATE while the actual Ops event receipt says agent_mode=DISABLED.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PYTEST = [sys.executable, "-m", "pytest", "-q", "--disable-warnings"]


def _offline_namespace(command: list[str]) -> list[str]:
    bubblewrap = shutil.which("bwrap")
    if bubblewrap is None:
        raise RuntimeError("S49 offline simulation requires bubblewrap network-namespace isolation")
    return [
        bubblewrap,
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--bind",
        str(ROOT),
        str(ROOT),
        "--bind",
        "/tmp",
        "/tmp",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--",
        *command,
    ]


SUITES: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    (
        "production-agent-bridge-gate",
        (
            "tests/v2/test_session027_production.py::test_default_production_composes_public_to_risk_action_and_economics_after_restart",
        ),
        (),
    ),
    (
        "candidate-risk-economics-calendar",
        (
            "tests/v2/test_session016_candidate_selection.py",
            "tests/v2/test_session016_integration.py",
            "tests/v2/test_session019_admission.py",
            "tests/v2/test_session019_evaluation_integration.py",
            "tests/v2/test_session019_m0.py",
            "tests/v2/test_session019_scenarios.py",
            "tests/v2/test_session020_phase2_e2e.py",
            "tests/v2/test_session021_s6.py",
            "tests/v2/test_session021_selection_boundary.py",
            "tests/v2/test_session027_production.py",
            "-k",
            "not test_default_production_composes_public_to_risk_action_and_economics_after_restart",
        ),
        (),
    ),
    (
        "sources-features-strategies-universe",
        (
            "tests/v2/test_session020_archive_remediation.py",
            "tests/v2/test_session021_universe.py",
            "tests/v2/test_session037_active_history.py",
            "tests/v2/test_session037_chronology.py",
            "tests/v2/test_session037_due_work.py",
            "tests/v2/test_session037_economic_binding.py",
            "tests/v2/test_session037_feature_prefix.py",
            "tests/v2/test_session037_product_refresh.py",
            "tests/v2/test_session037_research_prerequisites.py",
            "tests/v2/test_session037_strategy_prefix.py",
            "tests/v2/test_session041_capacity_max_breadth.py",
            "tests/v2/test_session041_capture_pressure_reserve.py",
            "tests/v2/test_session041_dynamic_universe.py",
            "tests/v2/test_session041_production_breadth.py",
            "tests/v2/test_session041_public_runtime_integration.py",
            "tests/v2/test_session041_s6_cohort.py",
            "tests/v2/test_session041_strategy_surface.py",
            "tests/v2/test_session041_mixed_broad_capacity.py",
            "tests/v2/test_session041_broad_stream_source.py",
            "tests/v2/test_session041_stream_service_paging.py",
            "tests/v2/test_session041_binance_qualification.py",
            "tests/v2/test_session044_close_driven_m15.py",
            "tests/v2/test_session046_peer_context.py",
        ),
        (),
    ),
    (
        "brief-agent-memory-events-outcomes",
        (
            "tests/v2/test_session033_outcome_maturity.py",
            "tests/v2/test_session033_outcome_resolution.py",
            "tests/v2/test_session033_outcome_runtime.py",
            "tests/v2/test_session037_action_outcome_producer.py",
            "tests/v2/test_session037_economic_sources.py",
            "tests/v2/test_session037_installed_economics.py",
            "tests/v2/test_session037_public_context.py",
            "tests/v2/test_session041_event_extraction.py",
            "tests/v2/test_session041_event_extraction_transport.py",
            "tests/v2/test_session041_official_calendar.py",
            "tests/v2/test_session041_public_context_extraction_async.py",
            "tests/v2/test_session047_decision_brief.py",
            "tests/v2/test_session047_event_classification.py",
            "tests/v2/test_session047_peer_chronology.py",
            "tests/v2/test_session047_research_agent.py",
            "tests/v2/test_session047_research_memory.py",
            "tests/v2/test_session048_s47_repairs.py",
            "tests/science/test_decision_calendar.py",
        ),
        (),
    ),
    (
        "lifecycle-protection-recovery-bybit-v1",
        (
            "tests/contract/test_v1_golden_baseline.py",
            "tests/runtime/test_session041_bybit_demo.py",
            "tests/runtime/test_session041_bybit_native.py",
            "tests/runtime/test_session041_bybit_recovery.py",
            "tests/runtime/test_session041_binance_demo.py",
            "tests/runtime/test_session041_binance_native.py",
            "tests/runtime/test_session041_binance_reconciliation.py",
            "tests/runtime/test_session041_binance_unsent_recovery.py",
            "tests/runtime/test_session048_lifecycle_projection.py",
            "tests/runtime/test_session048_position_protection.py",
            "tests/v2/test_session037_lifecycle_interruption.py",
            "tests/v2/test_session048_management_shadow.py",
        ),
        (),
    ),
    (
        "ipc-desktop-brokers-workers-and-capture",
        (
            "tests/v2/test_session020_desktop_ipc.py",
            "tests/v2/test_session026_agent_broker_ownership.py",
            "tests/v2/test_session036_windows_broker.py",
            "tests/v2/test_session036_writer_broker_bounds.py",
            "tests/v2/test_session038_report_worker.py",
            "tests/v2/test_session040_capture_receipts.py",
            "tests/v2/test_session040_durable_capture.py",
            "tests/v2/test_worker_isolation.py",
            "tests/scanner/test_scanner_calendar.py",
            "tests/v2/test_session041_product_configuration.py",
            "tests/v2/test_session041_production_breadth.py",
        ),
        (),
    ),
    (
        "known-inherited-readiness-tests",
        (
            "tests/runtime/test_session041_binance_readiness.py",
            "tests/runtime/test_session041_execution_final_review.py",
        ),
        (
            "tests.runtime.test_session041_execution_final_review::test_ack_loss_duplicate_error_and_local_enqueue_never_authorize_resend[None-dispatch_persisted_demo_command]",
            "tests.runtime.test_session041_execution_final_review::test_ack_loss_duplicate_error_and_local_enqueue_never_authorize_resend[TimeoutError-dispatch_persisted_demo_command]",
            "tests.runtime.test_session041_execution_final_review::test_ack_loss_duplicate_error_and_local_enqueue_never_authorize_resend[ValueError-dispatch_persisted_demo_command]",
        ),
    ),
    (
        "known-inherited-s6-ops-contract-test",
        (
            "tests/v2/test_session027_ops_supervisor.py::test_non_action_sleeve_contracts_and_s8_basket_role_remain_explicit",
        ),
        (),
    ),
)


def _test_counts(xml_path: Path) -> dict[str, int]:
    import xml.etree.ElementTree as ET

    if not xml_path.exists():
        return {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    root = ET.parse(xml_path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    return {
        key: sum(int(item.attrib.get(key, "0")) for item in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }


def _test_failures(xml_path: Path) -> dict[str, str]:
    import xml.etree.ElementTree as ET

    if not xml_path.exists():
        return {}
    root = ET.parse(xml_path).getroot()
    failures: dict[str, str] = {}
    for item in root.iter("testcase"):
        problem = next((child for child in item if child.tag in {"failure", "error"}), None)
        if problem is not None:
            name = f"{item.attrib.get('classname', '')}::{item.attrib.get('name', '')}"
            failures[name] = f"{problem.attrib.get('message', '')} {problem.text or ''}".strip()
    return failures


def _invoke(
    name: str,
    targets: tuple[str, ...],
    evidence: Path,
    *,
    strict_gate: bool = False,
    expected_gate_failures: tuple[str, ...] = (),
) -> dict[str, Any]:
    log_dir = evidence / "runs"
    log_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    xml_path = log_dir / f"{safe}.junit.xml"
    log_path = log_dir / f"{safe}.log"
    time_path = log_dir / f"{safe}.time.txt"
    pytest_cmd = PYTEST + [*targets, f"--junitxml={xml_path}"]
    timed = Path("/usr/bin/time").is_file()
    cmd = (["/usr/bin/time", "-v", "-o", str(time_path)] + pytest_cmd) if timed else pytest_cmd
    cmd = _offline_namespace(cmd)
    env = os.environ.copy()
    env.update({"ATLAS_OFFLINE_SIMULATION": "1", "PYTHONHASHSEED": "0"})
    env["PYTHONPATH"] = os.pathsep.join(
        str(path)
        for path in (
            ROOT / "src",
            ROOT / "docs/v2/transition-s47-evidence",
            ROOT / "scripts",
            ROOT,
        )
    )
    for key in tuple(env):
        if any(part in key.upper() for part in ("KEY", "SECRET", "TOKEN", "CREDENTIAL")):
            env.pop(key, None)
    env["ATLAS_AGENT_PROVIDER_PROFILE"] = "DISABLED"
    if strict_gate:
        env["ATLAS_S49_REQUIRE_AGENT_BRIDGE"] = "1"
    else:
        env.pop("ATLAS_S49_REQUIRE_AGENT_BRIDGE", None)
    started = time.monotonic()
    proc = subprocess.run(cmd, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - started
    output = proc.stdout
    log_path.write_text(output, encoding="utf-8")
    failure_map = _test_failures(xml_path)
    expected_agent_gate = strict_gate and proc.returncode != 0 and "ATLAS_S49_AGENT_INTEGRATION_GATE" in output
    expected_inherited_gate = (
        bool(expected_gate_failures)
        and proc.returncode != 0
        and set(failure_map) == set(expected_gate_failures)
        and all("Binance command readiness context missing" in failure_map[name] for name in expected_gate_failures)
    )
    expected_gate = expected_agent_gate or expected_inherited_gate
    status = "TEST GATE" if expected_gate else ("PASS" if proc.returncode == 0 else "FAIL")
    usage: dict[str, Any] = {"available": False}
    if timed and time_path.exists():
        raw_time = time_path.read_text(encoding="utf-8", errors="replace")

        def metric(label: str, pattern: str, cast: Any) -> Any:
            match = re.search(pattern, raw_time, flags=re.MULTILINE)
            return cast(match.group(1)) if match else None

        usage = {
            "available": True,
            "user_cpu_seconds": metric("user", r"User time \(seconds\):\s*([0-9.]+)", float),
            "system_cpu_seconds": metric("system", r"System time \(seconds\):\s*([0-9.]+)", float),
            "max_rss_kib": metric("max_rss", r"Maximum resident set size \(kbytes\):\s*(\d+)", int),
            "file_system_input_blocks": metric("fs_input", r"File system inputs:\s*(\d+)", int),
            "file_system_output_blocks": metric("fs_output", r"File system outputs:\s*(\d+)", int),
            "raw": raw_time,
        }
    return {
        "name": name,
        "status": status,
        "return_code": proc.returncode,
        "expected_gate": expected_gate,
        "expected_gate_failures": sorted(expected_gate_failures) if expected_inherited_gate else [],
        "duration_seconds": round(elapsed, 3),
        "command": pytest_cmd,
        "junit": str(xml_path.relative_to(ROOT)),
        "log": str(log_path.relative_to(ROOT)),
        "resource_usage": usage,
        **_test_counts(xml_path),
    }


def _desktop_smoke(evidence: Path) -> dict[str, Any]:
    log_dir = evidence / "runs"
    log_dir.mkdir(parents=True, exist_ok=True)
    smoke_command = [
        sys.executable,
        "-c",
        (
            "import sys; from atlas.v2.product import main; "
            "sys.argv=['atlas-product','--desktop-smoke','--data-root','/tmp/atlas-s49-desktop-smoke']; "
            "raise SystemExit(main())"
        ),
    ]
    time_path = log_dir / "desktop-smoke.time.txt"
    timed = Path("/usr/bin/time").is_file()
    command = (["/usr/bin/time", "-v", "-o", str(time_path)] + smoke_command) if timed else smoke_command
    command = _offline_namespace(command)
    env = os.environ.copy()
    env.update({"QT_QPA_PLATFORM": "offscreen", "PYTHONHASHSEED": "0"})
    env["PYTHONPATH"] = os.pathsep.join(
        str(path)
        for path in (
            ROOT / "src",
            ROOT / "docs/v2/transition-s47-evidence",
            ROOT / "scripts",
            ROOT,
        )
    )
    for key in tuple(env):
        if any(part in key.upper() for part in ("KEY", "SECRET", "TOKEN", "CREDENTIAL")):
            env.pop(key, None)
    env["ATLAS_AGENT_PROVIDER_PROFILE"] = "DISABLED"
    started = time.monotonic()
    proc = subprocess.run(command, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - started
    (log_dir / "desktop-smoke.log").write_text(proc.stdout, encoding="utf-8")
    try:
        payload = json.loads(next(line for line in proc.stdout.splitlines() if line.startswith("{")))
    except (StopIteration, json.JSONDecodeError):
        payload = None
    passed = (
        proc.returncode == 0
        and isinstance(payload, dict)
        and payload.get("status") == "TESTED"
        and payload.get("external_provider_options_present") is False
        and payload.get("authenticated_execution_options_present") is False
        and payload.get("credential_inputs_present") is False
        and payload.get("exchange_execution_controls_present") is False
    )
    usage: dict[str, Any] = {"available": False}
    if timed and time_path.exists():
        raw_time = time_path.read_text(encoding="utf-8", errors="replace")
        rss_match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", raw_time)
        usage = {"available": True, "max_rss_kib": int(rss_match.group(1)) if rss_match else None, "raw": raw_time}
    return {
        "name": "desktop-product-offscreen-smoke",
        "status": "PASS" if passed else "FAIL",
        "return_code": proc.returncode,
        "duration_seconds": round(elapsed, 3),
        "command": smoke_command,
        "log": str((log_dir / "desktop-smoke.log").relative_to(ROOT)),
        "resource_usage": usage,
        "observed": payload,
    }


def _scenario_matrix(suites: list[dict[str, Any]]) -> list[dict[str, str]]:
    by_name = {item["name"]: item["status"] for item in suites}
    return [
        {
            "scenario": "A: actual public-to-candidate/risk/economics/calendar/restart pipeline",
            "status": by_name.get("production-agent-bridge-gate", "NOT RUN"),
            "limit": "Synthetic indexed market data and fake transports; model bridge remains disconnected.",
        },
        {
            "scenario": "A: isolated brief, fake agent, memory, due-work and maturity consumers",
            "status": by_name.get("brief-agent-memory-events-outcomes", "NOT RUN"),
            "limit": "Component tests do not create a production caller.",
        },
        {
            "scenario": "B: Bybit V1 comparator, protection, lifecycle, UNKNOWN and recovery",
            "status": by_name.get("lifecycle-protection-recovery-bybit-v1", "NOT RUN"),
            "limit": "Fake transport; no native venue wire qualification.",
        },
        {
            "scenario": "C: persisted IPC and current product GUI smoke",
            "status": by_name.get("ipc-desktop-brokers-workers-and-capture", "NOT RUN"),
            "limit": "Offline populated IPC tests plus offscreen launch; S47 brief and S48 lifecycle are not unified in GUI.",
        },
        {
            "scenario": "Inherited readiness and S6 Ops contract gates",
            "status": "SEE INDIVIDUAL SUITES",
            "limit": "Retained unchanged and reported individually.",
        },
        {
            "scenario": "D: bounded process resource probe",
            "status": "MEASURED PER SUITE WALL/CPU/PEAK RSS/FILE I/O",
            "limit": "No full-feed sustained-capacity, queue-drain or feed-deadline qualification.",
        },
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, default=ROOT / "docs/v2/transition-s49-evidence")
    parser.add_argument("--only", action="append", choices=[item[0] for item in SUITES])
    args = parser.parse_args()
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    selected = [item for item in SUITES if not args.only or item[0] in args.only]
    results: list[dict[str, Any]] = []
    for name, targets, expected_gate_failures in selected:
        print(f"[S49] {name}: running", flush=True)
        result = _invoke(
            name,
            targets,
            evidence,
            strict_gate=name == "production-agent-bridge-gate",
            expected_gate_failures=expected_gate_failures,
        )
        results.append(result)
        print(
            f"[S49] {name}: {result['status']} ({result.get('tests', 0)} tests, {result['duration_seconds']}s)",
            flush=True,
        )
    if not args.only or "ipc-desktop-brokers-workers-and-capture" in args.only:
        smoke = _desktop_smoke(evidence)
        results.append(smoke)
        print(f"[S49] {smoke['name']}: {smoke['status']} ({smoke['duration_seconds']}s)", flush=True)
    unexpected = [item for item in results if item["status"] == "FAIL"]
    gate = any(item["status"] == "TEST GATE" for item in results)
    overall = "TEST GATE" if gate and not unexpected else ("FAIL" if unexpected else "PASS")
    report = {
        "schema": "ATLAS_S49_OFFLINE_INTEGRATION_REPORT_V1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "offline": True,
        "network_guard": "BUBBLEWRAP_UNSHARE_NET plus Python socket/URL non-loopback guard",
        "network_access": "OS_NETWORK_NAMESPACE_DISABLED_AND_PYTHON_NON_LOOPBACK_BLOCKED",
        "provider_calls": 0,
        "authenticated_exchange_calls": 0,
        "overall_status": overall,
        "research_shadow_release_candidate": overall == "PASS",
        "capital_enabled": False,
        "assisted_enabled": False,
        "runtime_model_provider": "FAKE_ONLY_IN_UNIT_CONSUMER_TESTS; NOT CONNECTED TO PRODUCTION",
        "suite_results": results,
        "scenario_matrix": _scenario_matrix(results),
        "limitations": [
            "Production Ops event receipt records agent_mode=DISABLED; the strict integration assertion therefore remains TEST GATE.",
            "Production and lifecycle fixtures use synthetic or archived/indexed evidence and fake transport.",
            "No Windows host is available in the current Linux environment.",
            "Wall times are measured; process peak RSS, full-feed latency and sustained capacity are not qualified by this runner.",
        ],
    }
    (evidence / "offline_integration_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    rows = [
        "# ATLAS S49 offline integration run",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        f"Overall: **{overall}**",
        "",
        "| Suite | Status | Tests | Failures | Duration (s) |",
        "|---|---:|---:|---:|---:|",
    ]
    for item in results:
        rows.append(
            f"| {item['name']} | {item['status']} | {item.get('tests', '—')} | {item.get('failures', '—')} | {item['duration_seconds']} |"
        )
    rows.extend(
        [
            "",
            "A TEST GATE is retained when a strictly asserted integration capability is absent; it is not counted as a successful complete workflow.",
            "",
        ]
    )
    (evidence / "offline_integration_report.md").write_text("\n".join(rows), encoding="utf-8")
    if unexpected:
        return 1
    if gate:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
