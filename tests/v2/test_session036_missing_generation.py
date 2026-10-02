"""Missing sleeve evidence cannot become a successful negative opportunity."""

from atlas.v2.contracts import CandidateSelectionStatus
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.outcomes import _resolve_candidate_set
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .session023_support import research_case
from .test_session016_candidate_selection import CUTOFF, evidence


def test_missing_sleeve_blocks_selection_and_retains_all_competitors(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        case = research_case(repository)
        refs = {item.candidate_id: (evidence(repository, item, case.universe, rank, event="missing-sleeve"),)
                for rank, item in enumerate(case.competitors, start=1)}
        result = assemble_multisleeve_research_candidate_set(repository, universe=case.universe,
            decision_event_id="missing-sleeve", cutoff_ns=CUTOFF, candidates=case.competitors,
            policies={item.policy_hash: item for item in (S1_POLICY, S2_POLICY, S3_POLICY)},
            scanner_evidence_refs=refs, generation_missing_reasons=("S1:EVENT_GATE_UNKNOWN_OR_STALE",))
        assert result.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE
        assert result.selected_candidate_id is None and len(result.candidates) == len(case.competitors)
        restored, identity = _resolve_candidate_set(repository, result.content_hash)
        assert restored == result
        assert tuple(identity["generation_missing_reasons"]) == ("S1:EVENT_GATE_UNKNOWN_OR_STALE",)


def test_empty_missing_generation_and_supported_empty_are_distinct(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        case = research_case(repository)
        arguments = {"universe": case.universe, "cutoff_ns": CUTOFF, "candidates": (), "policies": {}, "scanner_evidence_refs": {}}
        supported = assemble_multisleeve_research_candidate_set(repository,
            decision_event_id="supported-empty", **arguments)
        missing = assemble_multisleeve_research_candidate_set(repository,
            decision_event_id="missing-empty", generation_missing_reasons=("CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE",),
            **arguments)
        assert supported.selection_status == CandidateSelectionStatus.NO_CANDIDATE
        assert missing.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE
        assert _resolve_candidate_set(repository, missing.content_hash)[0] == missing
