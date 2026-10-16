from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    EligibilityStatusV2,
    EvaluationCandidateV1,
    V2Side,
)
from atlas.v2.instruments import (
    InstrumentKeyV2,
    ProductContractV2,
    StrategyEligibilityV2,
    TradingStatusV2,
    UniverseContractV2,
    UniverseEntryV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import IndependentRiskProfileV1, MeanMarginTierV1, OfflineRiskSnapshotV1
from atlas.v2.science.costs import FeeScheduleV2, FundingScheduleV2
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import (
    CandidateMeanInputsV1,
    CashAllowanceV1,
    MeanContextV1,
    MeanEvaluationPolicyV1,
    assemble_mean_evaluation_population,
    index_mean_artifact,
)
from atlas.v2.science.mean_models import MeanTrainingRowV1
from atlas.v2.science.mean_scenarios import MeanJointPathV1, MeanMarketPointV1, MeanScenarioSetV1
from atlas.v2.strategies.s1_trend import S1_POLICY

T = 400 * DAY_NS


def H(label):
    return sha256_json({"synthetic_p1": label})


def training_rows(compatibility, *, cash=D("5"), count=40, start_day=1, constant=False):
    return tuple(
        MeanTrainingRowV1(
            H(f"outcome-{i}-{cash}-{start_day}"),
            H(f"action-{i}"),
            compatibility,
            (start_day + i) * DAY_NS,
            (start_day + i) * DAY_NS,
            (start_day + i) * DAY_NS + 100,
            (start_day + i) * DAY_NS + 101,
            (1.0, 6.0 + (i % 2) * 2, 0.01 + (i % 2) * 0.02, 2.0 + (i % 2) * 4, 1.0 + (i % 2) * 4, float(i % 2))
            if not constant
            else (1.0, 6.0, 0.01, 4.0, 1.0, 0.0),
            cash,
            "CAUSAL_REPLAY",
            "FULL_FILL",
            "QUALIFIED_MODELLED",
        )
        for i in range(count)
    )


@pytest.fixture
def case(tmp_path):
    repo = OpsRepository(tmp_path / "mean.sqlite")
    policy = MeanEvaluationPolicyV1(H("evaluation-policy-source"), D("0"), D("0"), False, D("1000"))
    snapshot = OfflineRiskSnapshotV1(
        H("fake-scope"),
        "BINANCE",
        "TESTNET",
        "LINEAR_PERPETUAL",
        1,
        1,
        1,
        1,
        H("permission-observe"),
        H("offline-risk-policy"),
        T - 10,
        T - 5,
        T + 60_000_000_000,
        H("snapshot-source"),
        D("1000"),
        D("900"),
        D("800"),
        D("100"),
        D("0"),
        D("0"),
        (),
        ("joint-gap",),
        True,
    )
    universe_ref, instruments_ref = H("universe"), H("instruments")
    for ref in (universe_ref, instruments_ref, policy.source_ref, snapshot.source_ref):
        repo.register_artifact(
            ArtifactIndexEntryV2(
                ref,
                "SyntheticP1Source",
                ref,
                T - 100,
                T - 100,
                {"provenance": "SYNTHETIC_FIXTURE", "authority": "ZERO"},
            )
        )
    index_mean_artifact(repo, snapshot, available_at_ns=T - 5)
    index_mean_artifact(repo, policy, available_at_ns=T - 100)
    inputs, candidates = {}, []
    for i, target in enumerate(("-2", "5", "0", "9"), 1):
        key = InstrumentKeyV2(
            "BINANCE", "TESTNET", "LINEAR_PERPETUAL", f"COIN{i}USDT", f"coin{i}", "USDT", "USDT", H(f"contract{i}")
        )
        product = ProductContractV2(
            key,
            0,
            0,
            0,
            D("1"),
            D(".01"),
            D(".1"),
            D(".1"),
            TradingStatusV2.TRADING,
            H(f"metadata{i}"),
            D("1"),
            D("100"),
        )
        cid, setup = H(f"candidate{i}"), H(f"setup{i}")
        candidate = CandidateActionV2(
            ArtifactEnvelope(1, cid, T, T, "P1_SYNTHETIC", (setup,)),
            cid,
            key,
            S1_POLICY.policy_hash,
            setup,
            V2Side.LONG,
            T,
            T + int(S1_POLICY.entry_rule["candidate_deadline_offset_ns"]),
            T + 4 * 3_600_000_000_000,
            D("100"),
            D("100.05"),
            D("99"),
            0,
            "UNESTIMATED",
        )
        fees = FeeScheduleV2(key, 0, D(".001"), D(".001"), H(f"fees-source{i}"))
        funding = FundingScheduleV2(0, (), True, H(f"zero-funding{i}"))
        risk = IndependentRiskProfileV1(
            H("risk-source"),
            product.content_hash,
            snapshot.risk_policy_ref,
            snapshot.permission_ref,
            0,
            DAY_NS,
            D("20"),
            D("100"),
            D("100"),
            D("100"),
            D("200"),
            D("2000"),
            D("1000"),
            D("2000"),
            (D("1"), D("2")),
            (MeanMarginTierV1(D("10000"), D(".5"), D(".01"), D("0"), D("2")),),
            D("0"),
            D("0"),
            D("0"),
            D("90"),
            D("0"),
            D("1"),
            H("qualification"),
        )
        compat, execution = H(f"compat{i}"), H(f"execution{i}")
        from .evidence_fixtures import verified_calibration, verified_scenarios
        model, calibration, scope, hold, _, _ = verified_calibration(repo, product=product, policy=policy,
            compatibility_ref=compat, cash=target, now_ns=T)
        points = (
            MeanMarketPointV1(T + 1, D("100"), D("99.99"), D("100.01"), D("100"), D("100"), H("quote1")),
            MeanMarketPointV1(
                candidate.horizon_end_ns, D("102"), D("101.99"), D("102.01"), D("100"), D("100"), H("quote2")
            ),
        )
        scenarios = MeanScenarioSetV1(
            product.content_hash,
            snapshot.content_hash,
            compat,
            H("template-source"),
            execution,
            H("execution-calibration"),
            T,
            T + DAY_NS,
            T - 100,
            0,
            0,
            (MeanJointPathV1(H("common-path"), D("1"), D("0"), points, ()),),
            ("joint-gap",),
            (D("90"),),
            True,
            True,
            "CAUSAL_REPLAY",
        )
        scenarios = verified_scenarios(repo,scenarios,scope=scope,hold=hold,policy=policy,snapshot=snapshot)
        allowances = tuple(
            CashAllowanceV1(
                model.content_hash, policy.content_hash, compat, H(f"{kind}{i}"), 0, T + DAY_NS, kind, D("0")
            )
            for kind in ("RESIDUAL_EXECUTION_BIAS", "NUMERICAL_APPROXIMATION", "INCREMENTAL_FUTURE_PROGRAMME")
        )
        context = MeanContextV1(candidate.content_hash, T, H(f"context{i}"), compat, 0.0, T, T - 1, 1_000_000_000)
        inp = CandidateMeanInputsV1(
            candidate, product, S1_POLICY, fees, funding, risk, model, calibration, scenarios, *allowances, context
        )
        # Consume the real historical candidate/product reader layouts.
        repo.register_artifact(
            ArtifactIndexEntryV2(
                candidate.content_hash,
                "CandidateActionV2",
                candidate.content_hash,
                T,
                T,
                {"candidate": candidate.to_dict()},
            )
        )
        repo.register_artifact(
            ArtifactIndexEntryV2(
                product.content_hash, "ProductContractV2", product.content_hash, 0, 0, {"product": product.to_dict()}
            )
        )
        repo.register_artifact(
            ArtifactIndexEntryV2(setup, "SyntheticP1FeaturePrefix", setup, T, T, {"synthetic_p1": f"setup{i}"})
        )
        for obj in vars(inp).values():
            at = T if isinstance(obj, CandidateActionV2) else getattr(obj, "available_at_ns", T - 1)
            index_mean_artifact(repo, obj, available_at_ns=at)
        source = H(f"candidate-source{i}")
        repo.register_artifact(
            ArtifactIndexEntryV2(source, "SyntheticP1Source", source, T - 100, T - 100, {"authority": "ZERO"})
        )
        candidates.append(
            EvaluationCandidateV1(
                cid,
                candidate.content_hash,
                i,
                (source, setup),
                D("1"),
                False,
                D("1"),
                D("1"),
                D("1"),
                D("1"),
                1,
                ("UNSUPPORTED_SOURCE",) if i == 3 else (),
            )
        )
        inputs[candidate.content_hash] = inp
    products = tuple(inp.product for inp in inputs.values())
    entries = tuple(
        sorted(
            (
                UniverseEntryV2(
                    p.key,
                    p.content_hash,
                    True,
                    True,
                    True,
                    True,
                    False,
                    FrozenMap({S1_POLICY.policy_id: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE)}),
                    (),
                )
                for p in products
            ),
            key=lambda e: e.key.to_canonical_json(),
        )
    )
    universe = UniverseContractV2(
        ArtifactEnvelope(
            1, "mean-fixture-universe", T, T, "P1_SYNTHETIC", tuple(sorted(p.content_hash for p in products))
        ),
        "P1_SYNTHETIC",
        T,
        policy.content_hash,
        entries,
    )
    index_mean_artifact(repo, universe, available_at_ns=T)
    population = assemble_mean_evaluation_population(
        repo,
        universe=universe,
        products=products,
        candidates=candidates,
        snapshot=snapshot,
        policy=policy,
        event_id="synthetic-slot",
        slot_ns=T,
        cutoff_ns=T,
        deadline_ns=T + int(S1_POLICY.entry_rule["candidate_deadline_offset_ns"]),
        cpu_budget_ns=10_000_000_000,
        wall_budget_ns=20_000_000_000,
    )

    def clock():
        return T + 10

    yield SimpleNamespace(
        repo=repo, population=population, snapshot=snapshot, policy=policy, inputs=inputs, clock=clock
    )
    repo.close()
