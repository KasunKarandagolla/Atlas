from __future__ import annotations

from dataclasses import replace

import pytest
from support.scanner_fixture import scanner_fixture

from atlas.scanner import compare_scanner_revisions, run_scan_slot
from atlas.scanner.persistence import persist_scanner_revision_comparison
from atlas.science.research_archive import ResearchArtifactArchive


def test_revision_comparison_uses_pair_matched_full_calendar():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    first = run_scan_slot(slot_at_ns=slot, universe=fixture.universes[0], cheap_inputs=fixture.cheap_inputs[0],
                          policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[0],
                          evaluator=fixture.evaluator, persist=False)
    comparison = compare_scanner_revisions(first.calendar_rows, first.calendar_rows,
                                           previous_policy_version="V1", revised_policy_version="V2")
    assert comparison.status == "COMPARABLE"
    assert comparison.paired_instruments == len(first.calendar_rows)
    assert comparison.deadline_miss_delta == 0


def test_unpaired_or_executed_only_calendars_are_not_valid_comparisons():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    first = run_scan_slot(slot_at_ns=slot, universe=fixture.universes[0], cheap_inputs=fixture.cheap_inputs[0],
                          policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[0],
                          evaluator=fixture.evaluator, persist=False)
    unpaired = compare_scanner_revisions(first.calendar_rows, first.calendar_rows[:-1],
                                         previous_policy_version="V1", revised_policy_version="V2")
    assert unpaired.status == "INVALID_UNPAIRED"
    executed_only = tuple(replace(row, plan_status="TRADE_CANDIDATE") for row in first.calendar_rows)
    with pytest.raises(ValueError, match="complete calendar"):
        compare_scanner_revisions(first.calendar_rows, executed_only,
                                  previous_policy_version="V1", revised_policy_version="V2")


def test_revision_comparison_artifact_reuses_research_archive(tmp_path):
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    result = run_scan_slot(slot_at_ns=slot, universe=fixture.universes[0], cheap_inputs=fixture.cheap_inputs[0],
                           policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[0],
                           evaluator=fixture.evaluator, persist=False)
    comparison = compare_scanner_revisions(result.calendar_rows, result.calendar_rows,
                                           previous_policy_version="V1", revised_policy_version="V2")
    archive = ResearchArtifactArchive(tmp_path / "research")
    path = persist_scanner_revision_comparison(archive, comparison)
    assert path.exists() and path.name.startswith("part-")
