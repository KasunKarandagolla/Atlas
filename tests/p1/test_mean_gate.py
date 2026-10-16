from atlas.v2.contracts import CandidateEvaluationSetV1, EligibleSetV1
from atlas.v2.science.mean_models import fit_m0_mean


def test_new_contracts_have_distinct_versions():
    assert CandidateEvaluationSetV1.VERSION == "ATLAS_RED_MEAN_ACTION_EVALUATION_V1"
    assert EligibleSetV1.VERSION != CandidateEvaluationSetV1.VERSION
    assert callable(fit_m0_mean)


def test_duplicate_json_fields_and_byte_caps_fail_closed():
    import pytest

    from atlas.v2.science.mean_research import ResearchLedgerV1

    with pytest.raises(ValueError, match="duplicate"):
        ResearchLedgerV1.from_json('{"version":"a","version":"b"}')
    with pytest.raises(ValueError, match="byte cap"):
        ResearchLedgerV1.from_json(" " * 10, maximum_bytes=5)
