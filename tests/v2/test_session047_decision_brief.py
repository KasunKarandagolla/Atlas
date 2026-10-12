"""Research brief bounds, full calendar and causal negative cases."""

from dataclasses import replace

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ArtifactEnvelope, CandidateSelectionStatus, CandidateSetV2
from atlas.v2.research.decision_brief import BriefLimitsV1, BriefRequestV1, build_decision_brief

from .test_session016_candidate_selection import CUTOFF, EVENT, SELECTION_POLICY_HASH, universe


def request():
    u = universe()
    cs = CandidateSetV2(
        ArtifactEnvelope(1, "cs", CUTOFF, CUTOFF, "fixture", (u.content_hash,)),
        EVENT,
        u.content_hash,
        SELECTION_POLICY_HASH,
        (),
        None,
        "FROZEN_A0",
        CandidateSelectionStatus.NO_CANDIDATE,
    )
    return BriefRequestV1(
        EVENT,
        CUTOFF,
        CUTOFF + 100,
        CUTOFF + 1,
        cs,
        u,
        sha256_json({"snapshot": 1}),
        sha256_json({"research_policy": 1}),
        BriefLimitsV1("FIXTURE_LIMITS_V1", 20, 40000, 5, 0, 40000),
        (),
        (),
        (),
    )


def test_empty_slot_preserves_a0_and_calendar():
    r = request()
    brief = build_decision_brief(r)
    assert brief.status == "COMPLETE"
    assert brief.body["calendar_denominator"] == 1
    assert brief.body["a0_selected_candidate_id"] is None
    assert brief.body["slot_status"] == "NO_CANDIDATE"
    assert brief.body["shortlist"] == ()
    assert brief.content_hash == build_decision_brief(r).content_hash


def test_no_quiet_truncation_and_deadline():
    r = request()
    tiny = replace(r, limits=replace(r.limits, max_bytes=20))
    assert build_decision_brief(tiny).status == "BLOCKED"
    assert "BYTE_LIMIT" in build_decision_brief(tiny).reasons
    assert build_decision_brief(replace(r, built_at_ns=r.deadline_ns)).status == "EXPIRED"


def test_revised_universe_is_not_accepted():
    r = request()
    wrong = replace(
        r, universe=replace(r.universe, envelope=replace(r.universe.envelope, artifact_id="revision", content_hash=""))
    )
    assert "UNIVERSE_IDENTITY" in build_decision_brief(wrong).reasons


def test_all_screened_rows_remain_without_invented_economics():
    from atlas.v2.contracts import CandidateSetEntryV2, EligibilityStatusV2, V2Side

    from .test_session014_core import KEY

    r = request()
    rows = tuple(
        CandidateSetEntryV2(
            str(i),
            "S1",
            KEY,
            V2Side.LONG,
            (),
            status,
            rejection_reason="REJECTED" if status == EligibilityStatusV2.INELIGIBLE else None,
        )
        for i, status in enumerate(
            (EligibilityStatusV2.ELIGIBLE, EligibilityStatusV2.INELIGIBLE, EligibilityStatusV2.NOT_ESTIMABLE)
        )
    )
    cs = replace(r.candidate_set, candidates=rows, envelope=replace(r.candidate_set.envelope, content_hash=""))
    brief = build_decision_brief(replace(r, candidate_set=cs))
    assert len(brief.body["calendar"]) == 3
    assert brief.body["shortlist"] == ()
    assert all(row["economic_status"] == "NOT_ESTIMABLE" for row in brief.body["calendar"])
    assert build_decision_brief(replace(r, candidate_set=cs, limits=replace(r.limits, max_rows=2))).status == "BLOCKED"


def test_future_fact_stale_context_and_revision_are_blocked():
    from atlas.v2._serialization import FrozenMap
    from atlas.v2.research.decision_brief import BriefFactV1

    from .test_session016_candidate_selection import alternate_key

    r = request()
    f = BriefFactV1(
        "btc_eth",
        sha256_json("fact"),
        None,
        CUTOFF + 1,
        CUTOFF + 1,
        CUTOFF + 20,
        "AVAILABLE",
        FrozenMap({"units": "PRICE", "value": 1}),
        (),
    )
    assert "FUTURE_FACT" in build_decision_brief(replace(r, facts=(f,))).reasons
    stale = replace(f, observed_at_ns=CUTOFF - 10, available_at_ns=CUTOFF - 10, valid_until_ns=CUTOFF - 1)
    assert "STALE_CONTEXT" in build_decision_brief(replace(r, facts=(stale,))).reasons
    wrong = replace(stale, key=alternate_key("SOLUSDT"), valid_until_ns=CUTOFF + 20)
    assert "PRODUCT_REVISION" in build_decision_brief(replace(r, facts=(wrong,))).reasons


def test_sparse_technical_and_future_tail_do_not_qualify_gap_sensitive_values():
    from atlas.v2.data.bars import BarIntervalV2
    from atlas.v2.research.decision_brief import technical_fact

    from .test_session014_core import KEY, bar

    # Existing core fixture supplies point-in-time completed M15 bars.
    bars = tuple(bar(i, close=str(100 + i), interval=BarIntervalV2.M15) for i in range(4))
    cutoff = bars[2].raw.available_at_ns
    kwargs = {
        "key": KEY,
        "source_ref": sha256_json("features"),
        "cutoff_ns": cutoff,
        "valid_until_ns": cutoff + 100,
        "values": {"ema20": "101"},
    }
    prefix = technical_fact(bars=bars[:3], **kwargs)
    assert prefix == technical_fact(bars=bars, **kwargs)
    gapped = technical_fact(bars=(bars[0], bars[2]), **kwargs)
    assert gapped.status == "NOT_ESTIMABLE"
    assert len(gapped.values["features"]) == 0
    assert "SPARSE_BAR_FEATURE_UNQUALIFIED" in gapped.missingness


def test_optional_ml_wrong_horizon_missing_checkpoint_no_value():
    from atlas.v2._serialization import FrozenMap
    from atlas.v2.research.decision_brief import ForecastEvidenceV1

    from .test_session014_core import KEY

    r = request()
    refs = [sha256_json(x) for x in ("source", "model", "checkpoint", "calibration")]
    f = ForecastEvidenceV1(
        *refs,
        KEY,
        "CLOSE_RETURN",
        900,
        CUTOFF,
        CUTOFF,
        True,
        False,
        ("NO_MARKET_QUALIFICATION",),
        FrozenMap({"forecast": 999}),
    )
    brief = build_decision_brief(replace(r, forecasts=(f,), forecast_horizon_ns=100))
    assert brief.body["ml"][0]["status"] == "NOT_ESTIMABLE"
    assert brief.body["ml"][0]["values"] is None


def test_actual_evaluator_unestimable_result_is_projected_without_rerunning_or_rescue(tmp_path, monkeypatch):
    from atlas.v2.research.decision_brief import EconomicsEvidenceV1

    from . import test_session019_evaluation_integration as consumer

    original = consumer.run_phase2_economic_evaluation
    captured = []

    def observe(repo, **kwargs):
        result = original(repo, **kwargs)
        cs, candidate = kwargs["candidate_set"], kwargs["candidate"]
        from atlas.v2._serialization import json_value
        from atlas.v2.instruments import UniverseContractV2

        row = repo.get_artifact(cs.universe_ref)
        universe_value = UniverseContractV2.from_dict(json_value(row.metadata["universe"]))
        r = BriefRequestV1(
            cs.decision_event_id,
            candidate.decision_at_ns,
            candidate.deadline_ns,
            result.evaluation.available_at_ns,
            cs,
            universe_value,
            candidate.snapshot_hash,
            sha256_json("research"),
            BriefLimitsV1("FIXTURE_V1", 20, 100000, 10, 0, 100000),
            (),
            (EconomicsEvidenceV1(candidate, kwargs["action"], result),),
            (),
        )
        brief = build_decision_brief(r, repository=repo)
        captured.append(brief)
        assert brief.body["shortlist"] == ()
        assert brief.body["economics"][0]["status"] == "NOT_ESTIMABLE"
        assert brief.body["economics"][0]["evaluation"] is None
        missing = brief.body["economics"][0]["missingness"]
        assert "MATURE_LABELS_MISSING_OR_FUTURE" in missing
        assert "COST_FILL_SUPPORT_MISSING" in missing
        assert (
            "ORIGINAL_DEADLINE_BINDING"
            in build_decision_brief(replace(r, deadline_ns=r.deadline_ns + 1), repository=repo).reasons
        )
        return result

    monkeypatch.setattr(consumer, "run_phase2_economic_evaluation", observe)
    consumer.test_unestimable_exact_action_persists_amended_evaluation_and_terminal_calendar(tmp_path, monkeypatch)
    assert len(captured) == 1


def test_existing_m1_and_analogue_diagnostics_keep_their_original_support(tmp_path_factory):
    from atlas.v2.research.decision_brief import EconomicsEvidenceV1

    from .test_session041_frozen_action_comparison import real_pipeline

    generator = real_pipeline.__wrapped__(tmp_path_factory)
    fixture = next(generator)
    try:
        c = fixture.case.candidate
        r = BriefRequestV1(
            fixture.case.candidate_set.decision_event_id,
            c.decision_at_ns,
            c.deadline_ns,
            fixture.evaluated.evaluation.available_at_ns,
            fixture.case.candidate_set,
            fixture.case.universe,
            c.snapshot_hash,
            sha256_json("research"),
            BriefLimitsV1("FIXTURE_V1", 30, 100000, 10, 0, 100000),
            (),
            (EconomicsEvidenceV1(c, fixture.action, fixture.evaluated, fixture.m1_run, fixture.analogue_run.result),),
            (),
        )
        brief = build_decision_brief(r, repository=fixture.repo)
        assert brief.status == "COMPLETE"
        row = brief.body["economics"][0]
        assert row["m1"]["prediction_ref"] == fixture.m1_run.prediction.content_hash
        assert row["analogue"]["result_ref"] == fixture.analogue_run.result_ref
        assert row["status"] == "NOT_ESTIMABLE"
        assert brief.body["shortlist"] == ()
        assert fixture.case.candidate_set.selected_candidate_id == c.candidate_id
    finally:
        generator.close()


def test_future_ml_inputs_and_full_serialized_bounds():
    from atlas.v2._serialization import FrozenMap, canonical_json
    from atlas.v2.research.decision_brief import ForecastEvidenceV1

    from .test_session014_core import KEY

    r = request()
    refs = [sha256_json(x) for x in ("source", "model", "checkpoint", "calibration")]
    f = ForecastEvidenceV1(
        *refs,
        KEY,
        "CLOSE_RETURN",
        100,
        CUTOFF + 1,
        CUTOFF + 1,
        True,
        True,
        ("UNQUALIFIED",),
        FrozenMap({"forecast": 999}),
    )
    assert "FUTURE_ML_INPUT" in build_decision_brief(replace(r, forecasts=(f,))).reasons
    for bound in (100, 1000, 10000):
        test_request = replace(r, limits=replace(r.limits, max_bytes=bound))
        brief = build_decision_brief(test_request)
        if brief.status == "COMPLETE":
            assert len(canonical_json(brief.to_dict()).encode("utf-8")) <= bound
        else:
            assert "BYTE_LIMIT" in brief.reasons
