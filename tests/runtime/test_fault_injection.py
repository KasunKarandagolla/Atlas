from __future__ import annotations

from tests.support.fault_injection import SECTION_1_8_SCENARIOS, run_all_fault_scenarios


def test_all_offline_fault_scenarios_drive_real_mechanics():
    results = run_all_fault_scenarios()
    assert len(results) == len(SECTION_1_8_SCENARIOS)
    assert all(result.passed for result in results), [
        (result.scenario.fault_type.value, result.assertions, result.mismatch_details)
        for result in results
        if not result.passed
    ]


def test_fault_harness_does_not_report_live_qualification():
    results = run_all_fault_scenarios()
    assert all(result.scenario.parameters is None for result in results)
    assert all("testnet" not in result.scenario.description.lower() for result in results)
