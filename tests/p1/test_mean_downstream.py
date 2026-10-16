import hashlib
import struct
import zlib
from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.models.mean_timeseries import (
    NumericalModelManifestV1,
    TimeMajorResearchInputV1,
    TimeMajorResearchOutputV1,
    forecast_log_return,
    log_price_target,
)
from atlas.v2.research.mean_effort import CHEAP_REFRESH_NS, EffortContractV1, VerifiedRescreenV1, admit_rescreen
from atlas.v2.research.mean_evidence import (
    MeanDecisionBriefV1,
    NumericalFactV1,
    RendererBenchmarkV1,
    neutral_chart_question,
    select_renderer_benchmark,
    validate_chart_image,
    verify_chart_claim,
)
from atlas.v2.research.mean_tools import (
    TOOL_FAMILIES,
    RegisteredToolReceiptV1,
    RegisteredToolRequestV1,
    ResearchWorkBudgetV1,
    execute_registered_tool,
)

from .conftest import H, T
from .test_mean_evaluation import run
from .test_mean_selection import bundle_for


@pytest.mark.parametrize("checkpoint", ["NX-AI/TiRex-2", "autogluon/chronos-2-small", "NeoQuasar/Kronos-mini"])
def test_time_major_model_contract_positive_negative_units_masks_cutoffs(checkpoint):
    kronos = checkpoint == "NeoQuasar/Kronos-mini"
    manifest = NumericalModelManifestV1(
        checkpoint,
        "synthetic-pinned-revision",
        (H("weights"),),
        "NeoQuasar/Kronos-Tokenizer-2k" if kronos else None,
        "pinned-tokenizer" if kronos else None,
        (H("tokenizer"),) if kronos else (),
        H("lock"),
        H("pre"),
        H("post"),
        H("source"),
        512,
        64,
        (D(".1"), D(".5"), D(".9")),
        ("open", "high", "low", "close", "volume", "amount") if kronos else ("log_close",),
        ("USDT/base", "USDT/base", "USDT/base", "USDT/base", "base", "USDT") if kronos else ("log(USDT/base)",),
        "RAW_OHLCV_AMOUNT" if kronos else "LOG_CLOSE_USDT_PER_BASE",
        "EMPIRICAL_TRAJECTORY_QUANTILES" if kronos else "NATIVE_ONLY",
        None,
        "UNKNOWN",
        6101,
        16,
    )
    values = (
        ((D("100"), D("101"), D("99"), D("100"), D("2"), D("200")),) * 3
        if kronos
        else ((log_price_target(D("100")),),) * 3
    )
    req = TimeMajorResearchInputV1(
        manifest,
        H("instrument"),
        H("source"),
        T,
        100,
        (T - 200, T - 100, T),
        (T - 200, T - 100, T),
        values,
        tuple(tuple(False for _ in row) for row in values),
        (),
        ((), (), ()),
        (),
        (),
        (),
        2,
        (D(".1"), D(".5"), D(".9")),
    )
    assert TimeMajorResearchInputV1.from_dict(req.to_dict()) == req
    unit = "USDT/base" if kronos else "log(USDT/base)"
    out = TimeMajorResearchOutputV1(
        req, T, T + 10, T + 100, req.requested_quantiles, ((D("1"), D("2"), D("3")),) * 2, unit, "ENGINEERING_FIXTURE"
    )
    assert out.request.horizon_end_ns == T + 200
    with pytest.raises(ValueError, match="quantile"):
        replace(req, requested_quantiles=(D(".05"),))
    with pytest.raises(ValueError, match="future"):
        replace(req, available_times_ns=(T - 200, T - 100, T + 1))
    with pytest.raises(ValueError, match="mask"):
        replace(req, missing_mask=tuple(tuple(True for _ in row) for row in values))
    with pytest.raises(ValueError, match="shape"):
        replace(req, values=tuple(tuple(row[0] for row in values) for _ in values))
    if kronos:
        with pytest.raises(ValueError, match="tokenizer"):
            replace(manifest, tokenizer="NeoQuasar/Kronos-Tokenizer-base")
    else:
        future = replace(
            req,
            future_covariate_names=("calendar_hour",),
            future_covariate_units=("UTC_hour_0_23",),
            future_covariates=((D("1"),), (D("2"),)),
            future_known_at_ns=(T, T),
        )
        assert future.future_covariates[0] == (D("1"),)
        with pytest.raises(ValueError, match="future"):
            replace(future, future_covariate_names=("realized_funding",))
    assert forecast_log_return(log_price_target(D("110")), origin_price=D("100")) == pytest.approx(
        D("0.0953101798043249")
    )


@pytest.mark.parametrize("family", tuple(TOOL_FAMILIES))
def test_eight_registered_tool_families_execute_bounded_scope(family):
    operation = TOOL_FAMILIES[family][0]
    fact = NumericalFactV1("known", H("instrument"), H("source"), D("1"), "unit", T - 1, T, T, None)
    req = RegisteredToolRequestV1(
        H("job"),
        H("parent"),
        H("snapshot"),
        H("population"),
        H("action"),
        family,
        operation,
        (fact.content_hash,),
        (H("instrument"),),
        T,
        T,
        T + 5_000_000_000,
        ResearchWorkBudgetV1(cpu_budget_ns=1_000_000_000, wall_budget_ns=1_000_000_000),
        "one registered fact",
        "offline research",
        "idempotency",
    )
    result = execute_registered_tool(req, facts=(fact,), now_ns=T)
    # HAR needs mature history and refuses instead of inventing a forecast.
    assert result.status == ("UNAVAILABLE" if operation == "HAR_VOL_FORECAST" else "COMPLETE")
    assert RegisteredToolReceiptV1.from_dict(result.to_dict()) == result
    assert (
        execute_registered_tool(req, facts=(fact,), now_ns=T, consumed_keys=("idempotency",)).status == "BUDGET_DENIED"
    )
    assert execute_registered_tool(req, facts=(replace(fact, available_at_ns=T + 1),), now_ns=T).status in (
        "INVALID",
        "NEEDS_NEW_SNAPSHOT",
    )
    assert execute_registered_tool(req, facts=(fact,), now_ns=req.deadline_ns).status == "DEADLINE_EXPIRED"
    with pytest.raises(ValueError, match="unregistered"):
        replace(req, operation="exec_python")
    with pytest.raises(ValueError, match="unregistered"):
        replace(req, protected_holdout=True)


def test_registered_ar1_calculates_new_information():
    facts = tuple(
        NumericalFactV1(
            "residual",
            H("instrument"),
            H(f"source{i}"),
            D(str(0.9**i)),
            "log_return",
            T - 100 + i,
            T - 50 + i,
            T - 50 + i,
            None,
        )
        for i in range(20)
    )
    req = RegisteredToolRequestV1(
        H("job"),
        H("parent"),
        H("snapshot"),
        H("population"),
        None,
        "request_registered_work",
        "AR1_HALF_LIFE",
        tuple(f.content_hash for f in facts),
        (H("instrument"),),
        T,
        T,
        T + 5_000_000_000,
        ResearchWorkBudgetV1(cpu_budget_ns=1_000_000_000, wall_budget_ns=1_000_000_000),
        "half life",
        "research",
        "ar1",
    )
    result = execute_registered_tool(req, facts=facts, now_ns=T)
    assert result.status == "COMPLETE"
    assert result.output["phi"] == pytest.approx(0.9)
    assert result.output["half_life_steps"] == pytest.approx(6.57881347896)


def png(width, height, *, bomb=False):
    def chunk(kind, payload):
        return (
            struct.pack(">I", len(payload))
            + kind
            + payload
            + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
        )

    data = b"\0" * (height * (1 + 4 * width) + (1 if bomb else 0))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(data))
        + chunk(b"IEND", b"")
    )


def test_chart_pixels_byte_mime_hash_bomb_wrong_feed_future_and_claim_contract(case):
    e = run(case)
    bundle, fact = bundle_for(case, e, e.members[0].action_ref)
    data = png(bundle.width, bundle.height)
    args = {
        "image_bytes": data,
        "mime": "image/png",
        "expected_image_hash": hashlib.sha256(data).hexdigest(),
        "completed_at_ns": T + 1,
    }
    assert validate_chart_image(bundle, **args)
    for changed in (
        {"mime": "image/jpeg"},
        {"expected_image_hash": H("wrong")},
        {"completed_at_ns": bundle.deadline_ns},
    ):
        with pytest.raises(ValueError):
            validate_chart_image(bundle, **(args | changed))
    data = png(bundle.width, bundle.height, bomb=True)
    with pytest.raises(ValueError, match="compressed"):
        validate_chart_image(
            bundle,
            image_bytes=data,
            mime="image/png",
            expected_image_hash=hashlib.sha256(data).hexdigest(),
            completed_at_ns=T + 1,
        )
    with pytest.raises(ValueError, match="future"):
        replace(bundle, facts=(replace(fact, available_at_ns=T + 1),))
    from atlas.v2.research.mean_evidence import EvidenceClaimV1

    claim = EvidenceClaimV1(bundle.content_hash, fact.content_hash, fact.value, fact.unit, "AGREES")
    assert verify_chart_claim(bundle, claim)
    spot = replace(bundle, feed_kind="INDEPENDENT_SPOT")
    assert spot.evidence_status == "DISPLAY_ONLY"
    assert not verify_chart_claim(spot, replace(claim, bundle_ref=spot.content_hash))
    for strategy in (*tuple(f"S{i}" for i in range(1, 9)), "OWNED_POSITION"):
        assert "do not invent" in neutral_chart_question(strategy)


def test_frozen_renderer_comparison_objective_ties_and_missing_target(case):
    e = run(case)
    bundle, _ = bundle_for(case, e, e.members[0].action_ref)
    rows = tuple(
        RendererBenchmarkV1(
            bundle.content_hash,
            route,
            os,
            H(f"host-{os}"),
            "CELERON",
            4 * 1024**3,
            True,
            True,
            True,
            1000,
            200,
            100 * 1024**2,
            200 * 1024**2,
            100,
            3000,
            D("0"),
            10000,
            100,
        )
        for route in ("LOCAL_AGG", "SUPPLIED_DATA_REMOTE")
        for os in ("WINDOWS", "LINUX")
    )
    assert select_renderer_benchmark(rows, bundle=bundle) == "LOCAL_AGG"
    with pytest.raises(ValueError, match="BLOCKED"):
        select_renderer_benchmark(rows[:1], bundle=bundle)
    with pytest.raises(ValueError, match="BLOCKED"):
        select_renderer_benchmark(tuple(replace(r, safety_p99_ms=201) for r in rows), bundle=bundle)


def test_brief_completeness_and_effort_rescreen_invariants(case):
    e = run(case)
    inp = list(case.inputs.values())[0]
    facts = tuple(
        NumericalFactV1(name, inp.product.key.content_hash, H(name), D("1"), "unit", T - 1, T, T, None)
        for name in ("btc", "eth", "peers", "sources", "strategy")
    )
    brief = MeanDecisionBriefV1(e, facts, ("btc", "eth", "peers", "sources", "strategy"), (), (), (), (), (), T + 10)
    assert brief.status == "OFFLINE_COMPLETE"
    with pytest.raises(ValueError, match="completeness"):
        replace(brief, facts=facts[1:])
    assert replace(brief, facts=facts[1:], missing_reasons=("BTC_READER_UNAVAILABLE",)).status == "BLOCKED"
    for effort, multiplier in (("LOW", D(".25")), ("NORMAL", D("1")), ("HIGH", D("2"))):
        contract = EffortContractV1(effort, effort, T, H("command"), (H("native-clock"),))
        assert (
            contract.optional_refill_multiplier == multiplier and contract.mandatory_observation_ns == CHEAP_REFRESH_NS
        )
        with pytest.raises(ValueError, match="mandatory"):
            replace(contract, mandatory_observation_ns=20_000_000_000)
    request = VerifiedRescreenV1(
        H("event"),
        H("source"),
        H("asset"),
        H("mapping"),
        H("setup"),
        T - 10,
        T - 1,
        T,
        T + 10_000_000_000,
        T + 60_000_000_000,
        T + 120_000_000_000,
        H("parent"),
        True,
    )
    assert admit_rescreen(
        request, prior_keys=(), asset_requests_last_hour=0, global_requests_last_hour=0, asset_last_accepted_ns=None
    )
    assert not admit_rescreen(
        request,
        prior_keys=(request.dedupe_key,),
        asset_requests_last_hour=0,
        global_requests_last_hour=0,
        asset_last_accepted_ns=None,
    )
    assert not admit_rescreen(
        request, prior_keys=(), asset_requests_last_hour=6, global_requests_last_hour=0, asset_last_accepted_ns=None
    )
    with pytest.raises(ValueError, match="backdated"):
        replace(request, due_at_ns=T)


def test_registered_prefit_mean_tool_executes_and_rejects_changed_features(case):
    from atlas.v2.research.mean_tools import execute_mean_predict_tool, mean_tool_feature_ref
    from atlas.v2.science.mean_evaluation import independent_mean_features

    from .test_mean_risk_cash import prepare

    inp, _, action = prepare(case, 2)
    features = independent_mean_features(action, origin_rank=2, context=inp.context, consumer_ns=T + 10)
    feature_ref = mean_tool_feature_ref(
        action_ref=action.content_hash,
        model_ref=inp.model.content_hash,
        compatibility_ref=inp.model.compatibility_ref,
        features=features,
        raw_cutoff_ns=T,
    )
    req = RegisteredToolRequestV1(
        H("job"),
        H("parent"),
        H("snapshot"),
        H("population"),
        action.content_hash,
        "inspect_economics",
        "M0_MEAN_PREDICT",
        (inp.model.content_hash, feature_ref),
        (inp.product.key.content_hash,),
        T,
        T,
        T + 1_000_000_000,
        ResearchWorkBudgetV1(),
        "conditional mean",
        "research",
        "predict-once",
    )
    result = execute_mean_predict_tool(
        req, model=inp.model, features=features, compatibility_ref=inp.model.compatibility_ref, now_ns=T
    )
    assert result.status == "COMPLETE" and D(result.output["mean_cash"]) == 5
    changed = execute_mean_predict_tool(
        req,
        model=inp.model,
        features=tuple(1.0 for _ in features),
        compatibility_ref=inp.model.compatibility_ref,
        now_ns=T,
    )
    assert changed.status == "INVALID"


def test_kronos_raw_trajectory_quantiles_are_time_major_and_coherent():
    from atlas.v2.models.mean_timeseries import kronos_close_quantiles

    paths = tuple(((D("100"), D("110"), D("90"), D(close), D("2"), D("200")),) * 2 for close in ("95", "100", "105"))
    values = kronos_close_quantiles(paths, horizon_steps=2, quantiles=(D(".1"), D(".5"), D(".9")))
    assert values == ((D("95"), D("100"), D("105")),) * 2
    with pytest.raises(ValueError, match="incoherent"):
        kronos_close_quantiles(
            (((D("100"), D("90"), D("110"), D("100"), D("2"), D("200")),),), horizon_steps=1, quantiles=(D(".5"),)
        )


@pytest.mark.parametrize("strategy", ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "OWNED_POSITION"])
def test_strategy_specific_numeric_chart_predicates_and_missing_source_refusal(case, strategy):
    from atlas.v2.research.mean_evidence import CHART_FACT_UNITS, CHART_PREDICATES, evaluate_registered_chart_predicate

    e = run(case)
    bundle, _ = bundle_for(case, e, e.members[0].action_ref)
    values = {
        "S1": {
            "h4.close": 110,
            "h4.ema20": 105,
            "h4.ema50": 100,
            **{f"h1.window{i}.low": 99 for i in range(3)},
            **{f"h1.window{i}.ema20": 100 for i in range(3)},
            "s1.setup_confirmed_at_ns": T - 100,
            "s1.trigger_closed_at_ns": T - 1,
            "h1.ema20": 100,
            "h1.close": 105,
            "h1.ema50": 99,
            "m15.close": 105,
            "m15.prior_high": 104,
        },
        "S2": {
            "m15.pre_trigger_atr": 1,
            "s2.frozen_high": 100,
            "m15.close": 101,
            "s2.width": 1,
            "s2.width_training_q20": 2,
            "s2.atr": 1,
            "s2.atr_training_median": 2,
            "s2.trigger_volume": 3,
            "s2.prior_volume_median": 2,
        },
        "S3": {
            "s3.phi": ".9",
            "s3.watch_z": -3,
            "s3.half_life_minutes": 10,
            "m15.adx14": 20,
            "s3.latest_residual": ".5",
            "s3.prior_residual": 1,
        },
        "S4": {"flow.sequence_valid": 1, "flow.absorption_registered_predicate": 1, "flow.observation_count": 10},
        "S5": {
            "crowding.coverage_complete": 1,
            "crowding.structural_break_confirmed": 1,
            "crowding.liquidation_intensity_confirmed": 1,
        },
        "S6": {
            "relative.rank_quantile": ".95",
            "relative.common_instrument_count": 20,
            "relative.calendar_aligned": 1,
            "relative.own_trend_aligned": 1,
        },
        "S7": {
            "event.source_verified": 1,
            "event.asset_mapping_verified": 1,
            "event.source_conflict": 0,
            "event.received_at_ns": T - 10,
        },
        "S8": {
            "pair.calendar_aligned": 1,
            "pair.beta_frozen_at_origin": 1,
            "pair.both_leg_costs_known": 1,
            "pair.residual_z": 3,
        },
        "OWNED_POSITION": {
            "position.current_quantity": 1,
            "position.sealed_horizon_end_ns": T + 1000,
            "position.current_stop": 100,
            "position.sealed_stop": 99,
        },
    }[strategy]
    facts = tuple(
        NumericalFactV1(
            name, bundle.key.content_hash, H(name), D(str(value)), CHART_FACT_UNITS[name], T - 1, T, T, None
        )
        for name, value in values.items()
    )
    bundle = replace(bundle, strategy=strategy, facts=facts)
    assert evaluate_registered_chart_predicate(bundle, CHART_PREDICATES[strategy]) == "AGREES"
    assert evaluate_registered_chart_predicate(replace(bundle, facts=()), CHART_PREDICATES[strategy]) == "INSUFFICIENT"
