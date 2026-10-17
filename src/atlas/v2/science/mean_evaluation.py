"""Connected offline evaluation-before-selection route for the new mean policy.

Uses the existing artifact index, risk, fee/funding and portfolio ES owners.
There is no broker/OMS import, fitting, heavy optional loading or live dispatch.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, ClassVar, cast

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json
from atlas.v2.contracts import (
    ActionEvaluationV1,
    CandidateActionV2,
    CandidateEvaluationSetV1,
    EligibleSetV1,
    EvaluationWorkV1,
    PolicySpecV2,
)
from atlas.v2.instruments import ProductContractV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import IndependentRiskProfileV1, OfflineRiskSnapshotV1, size_independent_candidate
from atlas.v2.science.action import ExactActionV1, freeze_independent_action
from atlas.v2.science.costs import FeeScheduleV2, FundingScheduleV2, value_mean_cash_path
from atlas.v2.science.mean_common import ZERO, OfflineArtifact, nonnegative
from atlas.v2.science.mean_models import MeanModelV1, predict_mean
from atlas.v2.science.mean_research import MeanSelectionCalibrationV1
from atlas.v2.science.mean_scenarios import MeanScenarioSetV1, replay_independent_action


@dataclass(frozen=True)
class MeanEvaluationPolicyV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_EVALUATION_POLICY_OFFLINE_V1"
    source_ref: str
    materiality_cash: Decimal
    risk_lambda: Decimal
    require_es: bool
    portfolio_es_limit_cash: Decimal
    es_confidence: Decimal = Decimal("0.975")
    authority: str = "OFFLINE_RESEARCH_ONLY"

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.materiality_cash, self.risk_lambda, self.portfolio_es_limit_cash)
        if (
            self.authority != "OFFLINE_RESEARCH_ONLY"
            or not ZERO < self.es_confidence < 1
            or (not self.require_es and self.risk_lambda != 0)
        ):
            raise ValueError("unsupported soft-risk preference/authority")


@dataclass(frozen=True)
class CashAllowanceV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CASH_ALLOWANCE_V1"
    model_ref: str
    policy_ref: str
    compatibility_ref: str
    source_ref: str
    available_at_ns: int
    expires_at_ns: int
    kind: str
    cash: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.cash)
        if (
            self.kind not in ("RESIDUAL_EXECUTION_BIAS", "NUMERICAL_APPROXIMATION", "INCREMENTAL_FUTURE_PROGRAMME")
            or self.available_at_ns >= self.expires_at_ns
        ):
            raise ValueError("distinct cash allowance identity required")


@dataclass(frozen=True)
class MeanContextV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_ORIGIN_KNOWN_ACTION_CONTEXT_V1"
    candidate_ref: str
    available_at_ns: int
    source_ref: str
    compatibility_ref: str
    market_state: float
    raw_source_available_at_ns: int
    source_observed_at_ns: int
    maximum_source_age_ns: int
    setup_parameters: FrozenMap = FrozenMap()
    missing_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            not self.source_observed_at_ns <= self.raw_source_available_at_ns <= self.available_at_ns
            or self.maximum_source_age_ns <= 0
        ):
            raise ValueError("source observation/availability/freshness contract invalid")


@dataclass(frozen=True)
class CandidateMeanInputsV1:
    candidate: CandidateActionV2
    product: ProductContractV2
    policy: PolicySpecV2
    fees: FeeScheduleV2
    funding: FundingScheduleV2
    risk: IndependentRiskProfileV1
    model: MeanModelV1
    calibration: MeanSelectionCalibrationV1
    scenarios: MeanScenarioSetV1
    execution_bias: CashAllowanceV1
    numerical: CashAllowanceV1
    incremental_programme: CashAllowanceV1
    context: MeanContextV1


def index_mean_artifact(
    repo: OpsRepository, artifact: Any, *, available_at_ns: int, input_refs: tuple[str, ...] = ()
) -> str:
    """Use the existing immutable ops artifact owner, never a new store."""
    body = artifact.to_dict()
    digest = artifact.content_hash if hasattr(artifact, "content_hash") else sha256_json(body)
    if repo.get_artifact(digest) is not None:
        _require(repo, artifact, available_at_ns)
        return digest
    for ref in input_refs:
        item = repo.get_artifact(ref)
        if item is None or item.available_at_ns > available_at_ns:
            raise ValueError("unavailable input while indexing mean artifact")
    repo.register_artifact(
        ArtifactIndexEntryV2(
            digest,
            type(artifact).__name__,
            digest,
            available_at_ns,
            available_at_ns,
            {"mean_artifact": body, "input_refs": list(input_refs), "authority": "ZERO"},
        )
    )
    return digest


def read_mean_artifact(repo: OpsRepository, ref: str, cls: Any, *, consumer_ns: int) -> Any:
    item = repo.get_artifact(ref)
    if item is None or item.artifact_type != cls.__name__ or item.available_at_ns > consumer_ns:
        raise ValueError("missing/late/incompatible mean artifact")
    result = cls.from_dict(item.metadata["mean_artifact"])
    if result.content_hash != ref or item.content_hash != ref or item.metadata.get("authority") != "ZERO":
        raise ValueError("mean artifact content/authority mismatch")
    return result


def _stored_body(item):
    if "mean_artifact" in item.metadata:
        return item.metadata["mean_artifact"]
    for name in ("candidate", "product", "policy", "universe"):
        if name in item.metadata:
            return item.metadata[name]
    return item.metadata


def _require(repo: OpsRepository, obj: Any, cutoff_ns: int) -> None:
    item = repo.get_artifact(obj.content_hash)
    legacy = isinstance(
        obj, (CandidateActionV2, ProductContractV2, PolicySpecV2, FeeScheduleV2, FundingScheduleV2, UniverseContractV2)
    )
    if (
        item is None
        or item.artifact_type != type(obj).__name__
        or item.content_hash != obj.content_hash
        or item.available_at_ns > cutoff_ns
        or (not legacy and item.metadata.get("authority") != "ZERO")
        or canonical_json(_stored_body(item)) != canonical_json(obj.to_dict())
    ):
        raise ValueError("INPUT_MISSING_CHANGED_OR_FUTURE")


def assemble_mean_evaluation_population(
    repo: OpsRepository,
    *,
    universe: UniverseContractV2,
    products: tuple[ProductContractV2, ...],
    candidates,
    snapshot: OfflineRiskSnapshotV1,
    policy: MeanEvaluationPolicyV1,
    event_id: str,
    slot_ns: int,
    cutoff_ns: int,
    deadline_ns: int,
    cpu_budget_ns: int,
    wall_budget_ns: int,
) -> CandidateEvaluationSetV1:
    _require(repo, universe, cutoff_ns)
    by_product = {p.content_hash: p for p in products}
    if len(by_product) != len(products) or set(by_product) != {e.product_ref for e in universe.entries}:
        raise ValueError("COMPLETE_UNIVERSE_PRODUCT_REVISIONS_REQUIRED")
    for product in products:
        _require(repo, product, cutoff_ns)
        if not any(e.product_ref == product.content_hash and e.key == product.key for e in universe.entries):
            raise ValueError("UNIVERSE_PRODUCT_IDENTITY_MISMATCH")
    body = {
        "version": "ATLAS_RED_INSTRUMENT_REVISION_MANIFEST_V1",
        "universe_ref": universe.content_hash,
        "product_refs": sorted(by_product),
    }
    ref = sha256_json(body)
    repo.register_artifact(
        ArtifactIndexEntryV2(
            ref,
            "MeanInstrumentRevisionManifestV1",
            ref,
            cutoff_ns,
            cutoff_ns,
            {"mean_artifact": body, "authority": "ZERO"},
        )
    )
    result = CandidateEvaluationSetV1(
        event_id,
        slot_ns,
        cutoff_ns,
        deadline_ns,
        universe.content_hash,
        ref,
        snapshot.content_hash,
        snapshot.permission_ref,
        policy.content_hash,
        tuple(candidates),
        cpu_budget_ns,
        wall_budget_ns,
    )
    index_mean_artifact(
        repo,
        result,
        available_at_ns=cutoff_ns,
        input_refs=(universe.content_hash, ref, snapshot.content_hash, policy.content_hash),
    )
    return result


def independent_mean_features(
    action: ExactActionV1, *, origin_rank: int, context: MeanContextV1, consumer_ns: int
) -> tuple[float, ...]:
    if (
        context.candidate_ref != action.candidate_ref
        or context.raw_source_available_at_ns > action.cutoff_ns
        or context.available_at_ns > consumer_ns
        or consumer_ns - context.source_observed_at_ns > context.maximum_source_age_ns
        or context.missing_reasons
    ):
        raise ValueError("MISSING_OR_FUTURE_CONTEXT")
    return (
        float(action.side == "LONG"),
        math.log1p(float(action.sizing.notional_cash)),
        float(abs(action.entry_collar - action.stop_price) / action.entry_collar),
        (action.horizon_end_ns - action.cutoff_ns) / 3_600_000_000_000.0,
        float(origin_rank),
        context.market_state,
    )


def _evaluate_one(
    repo,
    action: ExactActionV1,
    inp: CandidateMeanInputsV1,
    *,
    rank: int,
    policy: MeanEvaluationPolicyV1,
    snapshot: OfflineRiskSnapshotV1,
    now_ns: int,
) -> ActionEvaluationV1:
    reasons: list[str] = []
    values: list[Decimal | None] = [None] * 8
    try:
        model, calibration, scenarios = inp.model, inp.calibration, inp.scenarios
        from atlas.v2.science.mean_calibration import verify_mean_calibration
        from atlas.v2.science.mean_scenario_evidence import verify_scenario_evidence

        verify_mean_calibration(
            repo,
            calibration,
            model=model,
            policy_ref=policy.content_hash,
            product_ref=action.product_ref,
            cutoff_ns=action.cutoff_ns,
            now_ns=now_ns,
        )
        from atlas.v2.science.mean_scenario_evidence import ScenarioSourceV1

        scenario_source = read_mean_artifact(repo, scenarios.source_ref, ScenarioSourceV1, consumer_ns=now_ns)
        if scenario_source.scope_ref != calibration.source_ref:
            raise ValueError("CALIBRATION_SCENARIO_SCOPE_MISMATCH")
        scenario_proof = verify_scenario_evidence(
            repo,
            scenarios,
            product_ref=action.product_ref,
            policy=policy,
            snapshot=snapshot,
            compatibility_ref=model.compatibility_ref,
            cutoff_ns=action.cutoff_ns,
            now_ns=now_ns,
        )
        if (
            calibration.model_ref != model.content_hash
            or calibration.policy_ref != policy.content_hash
            or calibration.compatibility_ref != inp.context.compatibility_ref
            or calibration.status != "SUPPORTED_OFFLINE"
            or calibration.allowance_cash is None
            or calibration.available_at_ns > action.cutoff_ns
            or now_ns >= calibration.expires_at_ns
            or scenarios.compatibility_ref != model.compatibility_ref
            or scenarios.account_snapshot_ref != snapshot.content_hash
            or model.provenance != scenarios.provenance
        ):
            raise ValueError("UNSUPPORTED_OR_INCOMPATIBLE_CALIBRATION")
        allowances = (inp.execution_bias, inp.numerical, inp.incremental_programme)
        expected = ("RESIDUAL_EXECUTION_BIAS", "NUMERICAL_APPROXIMATION", "INCREMENTAL_FUTURE_PROGRAMME")
        if len({x.source_ref for x in allowances} | {calibration.source_ref}) != 4:
            raise ValueError("ALLOWANCE_SOURCE_OVERLAP")
        for a, kind in zip(allowances, expected, strict=True):
            if (
                a.kind != kind
                or a.model_ref != model.content_hash
                or a.policy_ref != policy.content_hash
                or a.compatibility_ref != model.compatibility_ref
                or a.available_at_ns > action.cutoff_ns
                or now_ns >= a.expires_at_ns
            ):
                raise ValueError("STALE_OR_INCOMPATIBLE_ALLOWANCE")
        features = independent_mean_features(action, origin_rank=rank, context=inp.context, consumer_ns=now_ns)
        mean = predict_mean(
            model, features, compatibility_ref=inp.context.compatibility_ref, cutoff_ns=action.cutoff_ns, now_ns=now_ns
        )
        paths = replay_independent_action(action, scenarios, quantity_step=inp.product.qty_step)
        cash = tuple(
            value_mean_cash_path(
                action=action,
                path=p,
                fee=inp.fees,
                funding_schedule=inp.funding,
                base_units_per_contract=inp.product.base_units_per_contract,
            )
            for p in paths
        )
        if any(p.trading_cash is None for p in cash):
            raise ValueError("UNRESOLVED_CASH_PATH")
        if any(
            not mean_path_margin_pass(action, inp, market_path, execution_path)
            for market_path, execution_path in zip(scenarios.paths, paths, strict=True)
        ):
            reasons.append("PATH_MAINTENANCE_OR_LIQUIDATION_FAILED")
        if scenarios.stress_ids != snapshot.stress_ids:
            raise ValueError("JOINT_STRESS_IDENTITY_MISMATCH")
        for i, price in enumerate(scenarios.stress_exit_prices):
            direction = Decimal(1) if action.side == "LONG" else Decimal(-1)
            pnl = direction * action.quantity * inp.product.base_units_per_contract * (price - action.entry_collar)
            costs = (
                action.quantity
                * inp.product.base_units_per_contract
                * (action.entry_collar * inp.fees.entry_taker_rate + price * inp.fees.exit_taker_rate)
                + action.quantity * (inp.risk.adverse_funding_per_unit_cash + inp.risk.execution_loss_per_unit_cash)
                + action.quantity**2 * inp.risk.impact_per_unit_squared_cash
            )
            combined = pnl - costs + sum((e.joint_stress_pnl_cash[i] for e in snapshot.exposures), ZERO)
            if -combined > inp.risk.joint_stress_limit_cash:
                reasons.append("JOINT_PORTFOLIO_STRESS_FAILED")
            stressed_notional = action.quantity * inp.product.base_units_per_contract * price
            tier = next((t for t in inp.risk.tiers if stressed_notional <= t.notional_ceiling_cash), None)
            if (
                tier is None
                or action.sizing.margin_cash + pnl - costs
                < max(ZERO, stressed_notional * tier.maintenance_fraction - tier.maintenance_deduction_cash)
                + inp.risk.liquidation_buffer_cash
            ):
                reasons.append("STRESS_MAINTENANCE_OR_LIQUIDATION_FAILED")
        from atlas.v2.science.portfolio import empirical_es_cash

        bounded_es = empirical_es_cash(
            tuple(
                -p.background_pnl_cash - cast(Decimal, c.trading_cash)
                for p, c in zip(scenarios.paths, cash, strict=True)
            ),
            tuple(p.probability for p in scenarios.paths),
            scenario_proof.confidence,
        )
        if bounded_es > scenario_proof.scenario_es_cash:
            raise ValueError("SCENARIO_ECONOMIC_LOSS_OUTSIDE_QUALIFIED_ENVELOPE")
        delta_risk = ZERO
        if policy.require_es:
            if not scenarios.probability_support or not scenarios.es_qualified:
                raise ValueError("ES_NOT_ESTIMABLE")
            from atlas.v2.science.portfolio import empirical_es_cash

            probabilities = tuple(p.probability for p in scenarios.paths)
            before = tuple(-p.background_pnl_cash for p in scenarios.paths)
            after = tuple(
                -p.background_pnl_cash - cast(Decimal, c.trading_cash)
                for p, c in zip(scenarios.paths, cash, strict=True)
            )
            es_before = empirical_es_cash(before, probabilities, policy.es_confidence)
            es_after = empirical_es_cash(after, probabilities, policy.es_confidence)
            delta_risk = es_after - es_before
            if es_after > policy.portfolio_es_limit_cash:
                reasons.append("PORTFOLIO_ES_FAILED")
        score = (
            mean
            - calibration.allowance_cash
            - inp.execution_bias.cash
            - inp.numerical.cash
            - inp.incremental_programme.cash
        )
        j = score - policy.risk_lambda * delta_risk
        values = [
            mean,
            calibration.allowance_cash,
            inp.execution_bias.cash,
            inp.numerical.cash,
            inp.incremental_programme.cash,
            score,
            delta_risk,
            j,
        ]
        if score <= 0 or j <= policy.materiality_cash:
            reasons.append("NONPOSITIVE_OR_IMMATERIAL_ECONOMICS")
    except ValueError as exc:
        reasons.append(str(exc))
    status = "UNAVAILABLE" if values[0] is None else "INELIGIBLE" if reasons else "ELIGIBLE"
    return ActionEvaluationV1(
        action.population_ref,
        action.candidate_ref,
        action.content_hash,
        snapshot.content_hash,
        inp.model.content_hash,
        inp.calibration.content_hash,
        inp.scenarios.content_hash,
        inp.execution_bias.content_hash,
        inp.numerical.content_hash,
        inp.incremental_programme.content_hash,
        values[0],
        values[1],
        values[2],
        values[3],
        values[4],
        values[5],
        values[6],
        values[7],
        now_ns,
        status,
        tuple(reasons),
    )


def run_independent_mean_evaluation(
    repo: OpsRepository,
    *,
    population: CandidateEvaluationSetV1,
    snapshot: OfflineRiskSnapshotV1,
    policy: MeanEvaluationPolicyV1,
    inputs: Mapping[str, CandidateMeanInputsV1],
    clock_ns: Callable[[], int] = time.time_ns,
    monotonic_ns: Callable[[], int] = time.perf_counter_ns,
    cpu_ns: Callable[[], int] = time.thread_time_ns,
) -> EligibleSetV1:
    if (
        population.account_snapshot_ref != snapshot.content_hash
        or population.permission_ref != snapshot.permission_ref
        or population.evaluation_policy_ref != policy.content_hash
    ):
        raise ValueError("POPULATION_SNAPSHOT_POLICY_MISMATCH")
    _require(repo, population, clock_ns())
    _require(repo, snapshot, population.cutoff_ns)
    _require(repo, policy, population.cutoff_ns)
    universe_entry = repo.get_artifact(population.universe_ref)
    manifest = repo.get_artifact(population.instrument_revision_ref)
    if universe_entry is None or manifest is None or manifest.available_at_ns > population.cutoff_ns:
        raise ValueError("UNIVERSE_REVISION_EVIDENCE_UNAVAILABLE")
    universe = UniverseContractV2.from_dict(json_value(_stored_body(universe_entry)))
    _require(repo, universe, population.cutoff_ns)
    manifest_body = _stored_body(manifest)
    if (
        sha256_json(manifest_body) != population.instrument_revision_ref
        or manifest_body.get("universe_ref") != universe.content_hash
        or set(manifest_body.get("product_refs", ())) != {e.product_ref for e in universe.entries}
    ):
        raise ValueError("INSTRUMENT_REVISION_MANIFEST_MISMATCH")
    if set(inputs) - {c.candidate_ref for c in population.candidates}:
        raise ValueError("OUTSIDE_POPULATION_INPUT")
    start_wall, start_cpu = monotonic_ns(), cpu_ns()
    work: dict[str, EvaluationWorkV1] = {}
    evaluations: list[ActionEvaluationV1] = []
    attempts = 0
    # Fair exploration and source usefulness are scheduling signals, never
    # unsupported confidence bounds or a permanent top-three cut.
    remaining = list(population.candidates)
    processed = 0
    explored = False
    best_cash = ZERO
    while remaining:
        if processed < 2:
            row = min(remaining, key=lambda c: (bool(c.missing_reasons), -c.usefulness, c.initial_rank, c.candidate_id))
        else:
            row = min(
                remaining,
                key=lambda c: (
                    not (c.exploration and not explored),
                    -evaluation_work_priority(c, best_cash=best_cash, cpu_budget_ns=population.cpu_budget_ns),
                    c.initial_rank,
                    c.candidate_id,
                ),
            )
        remaining.remove(row)
        processed += 1
        explored = explored or row.exploration
        now = clock_ns()
        used_cpu, used_wall = cpu_ns() - start_cpu, monotonic_ns() - start_wall
        status, reason = "UNAVAILABLE", "MISSING_EVIDENCE"
        sizing_ref = action_ref = evaluation_ref = None
        attempt = None
        started_cpu = cpu_ns()
        if now >= population.deadline_ns:
            status, reason = "NOT_EVALUATED_DEADLINE", "ORIGINAL_DEADLINE_EXPIRED"
        elif used_cpu + row.estimated_cpu_ns > population.cpu_budget_ns or used_wall >= population.wall_budget_ns:
            status, reason = "NOT_EVALUATED_RESOURCE", "MEASURED_WORK_BUDGET"
        elif row.missing_reasons:
            reason = ";".join(row.missing_reasons)
        elif row.candidate_ref in inputs:
            attempts += 1
            attempt = attempts
            inp = inputs[row.candidate_ref]
            try:
                if (
                    inp.candidate.content_hash != row.candidate_ref
                    or inp.candidate.candidate_id != row.candidate_id
                    or inp.candidate.decision_at_ns != population.cutoff_ns
                    or inp.candidate.deadline_ns != population.deadline_ns
                ):
                    raise ValueError("CANDIDATE_ORIGINAL_IDENTITY_MISMATCH")
                if not any(
                    e.key == inp.product.key and e.product_ref == inp.product.content_hash and e.scanner_eligible
                    for e in universe.entries
                ):
                    raise ValueError("OUTSIDE_ASOF_UNIVERSE")
                for obj in vars(inp).values():
                    derived = isinstance(obj, (CandidateActionV2, MeanContextV1))
                    _require(repo, obj, now if derived else population.cutoff_ns)
                if (
                    inp.candidate.envelope.available_at_ns > now
                    or inp.context.raw_source_available_at_ns > population.cutoff_ns
                ):
                    raise ValueError("DERIVED_INPUT_FUTURE_OR_NEW_RAW_FACT")
                if inp.candidate.snapshot_hash not in row.source_refs:
                    raise ValueError("CANDIDATE_RAW_FEATURE_PREFIX_REQUIRED")
                for ref in row.source_refs:
                    source = repo.get_artifact(ref)
                    if source is None or source.available_at_ns > population.cutoff_ns:
                        raise ValueError("CANDIDATE_SOURCE_MISSING_OR_FUTURE")
                sizing = size_independent_candidate(
                    candidate=inp.candidate,
                    product=inp.product,
                    fee=inp.fees,
                    snapshot=snapshot,
                    profile=inp.risk,
                    cutoff_ns=population.cutoff_ns,
                    now_ns=now,
                )
                sizing_ref = index_mean_artifact(repo, sizing, available_at_ns=now)
                if sizing.status != "SIZED":
                    raise ValueError(";".join(sizing.reasons))
                action = freeze_independent_action(
                    candidate=inp.candidate,
                    population_ref=population.content_hash,
                    sizing=sizing,
                    product=inp.product,
                    policy=inp.policy,
                    permission_ref=population.permission_ref,
                    funding_ref=inp.funding.content_hash,
                    execution_ref=inp.scenarios.execution_ref,
                    model_ref=inp.model.content_hash,
                    source_refs=tuple(sorted((*row.source_refs, inp.context.content_hash))),
                    setup_parameters=inp.context.setup_parameters,
                )
                action_ref = index_mean_artifact(repo, action, available_at_ns=now)
                evaluation = _evaluate_one(
                    repo, action, inp, rank=row.initial_rank, policy=policy, snapshot=snapshot, now_ns=now
                )
                finished = clock_ns()
                if (
                    finished >= population.deadline_ns
                    or cpu_ns() - start_cpu > population.cpu_budget_ns
                    or monotonic_ns() - start_wall > population.wall_budget_ns
                ):
                    evaluation = replace(
                        evaluation, status="UNAVAILABLE", reasons=("EVALUATION_FINISHED_OUTSIDE_BUDGET",)
                    )
                evaluation = replace(evaluation, available_at_ns=finished)
                evaluation_ref = index_mean_artifact(
                    repo, evaluation, available_at_ns=finished, input_refs=(action_ref, population.content_hash)
                )
                if evaluation.status == "ELIGIBLE":
                    from atlas.v2.science.mean_evaluation_validation import _publication_entry

                    # Only this actual checked loop creates the ephemeral writer
                    # capability. No callable publisher can stamp supplied values.
                    # Public/imported requests independently reconstruct offline.
                    with repo.atomic_composition():
                        entry = _publication_entry(repo, evaluation, inp)
                        prior = getattr(repo, "_mean_evaluation_writes", {})
                        repo._mean_evaluation_writes = {**prior, entry.artifact_ref: entry}
                        try:
                            repo.register_artifact(entry)
                        finally:
                            repo._mean_evaluation_writes = prior
                evaluations.append(evaluation)
                if evaluation.status == "ELIGIBLE":
                    assert evaluation.score_cash is not None
                    best_cash = max(best_cash, evaluation.score_cash)
                status, reason = "EVALUATED", ""
            except ValueError as exc:
                reason = str(exc)
        finished = clock_ns()
        work[row.candidate_ref] = EvaluationWorkV1(
            row.candidate_ref,
            attempt,
            status,
            now,
            finished,
            max(0, cpu_ns() - started_cpu),
            sizing_ref,
            action_ref,
            evaluation_ref,
            (reason,) if reason else (),
        )
    result = EligibleSetV1(
        population,
        tuple(evaluations),
        tuple(work[c.candidate_ref] for c in population.candidates),
        clock_ns(),
        max(0, cpu_ns() - start_cpu),
        max(0, monotonic_ns() - start_wall),
    )
    index_mean_artifact(
        repo,
        result,
        available_at_ns=result.sealed_at_ns,
        input_refs=(population.content_hash, *(e.content_hash for e in evaluations)),
    )
    return result


def mean_path_margin_pass(action: ExactActionV1, inp: CandidateMeanInputsV1, path, cash_path) -> bool:
    """Conservative isolated-collateral envelope at every mark AND payment.

    Closing releases proportional initial margin. Realized gains and funding
    credits cannot auto-top-up the remaining position; paid fees, slippage and
    adverse funding are reserved against its envelope. Trading cash itself is
    still charged exactly once by value_mean_cash_path. This is an offline
    hard-risk bound, not a claim about native liquidation or guaranteed stops.
    """
    from atlas.v2.science.costs import fee_cash

    direction = Decimal(1) if action.side == "LONG" else Decimal(-1)
    b = inp.product.base_units_per_contract
    q = entered = entry_value = fees = slippage = funding = ZERO
    by_time = {point.at_ns: point.mark for point in path.points}
    payments = {payment.at_ns: payment for payment in cash_path.funding}
    times = sorted(set(by_time) | set(payments) | {fill.at_ns for fill in cash_path.fills})
    mark = ZERO

    def margin_ok() -> bool:
        if q == 0:
            return True
        notional = q * b * mark
        tier = next((t for t in inp.risk.tiers if notional <= t.notional_ceiling_cash), None)
        if tier is None:
            return False
        average = entry_value / entered
        equity = (
            action.sizing.margin_cash * q / action.quantity
            + direction * q * b * (mark - average)
            - fees
            - slippage
            + min(ZERO, funding)
        )
        maintenance = max(ZERO, notional * tier.maintenance_fraction - tier.maintenance_deduction_cash)
        return equity >= maintenance + inp.risk.liquidation_buffer_cash

    for at in times:
        mark = payments[at].mark_price if at in payments else by_time[at]
        if at in payments:
            payment = payments[at]
            funding -= direction * q * b * payment.mark_price * payment.signed_rate
        if not margin_ok():
            return False
        for fill in cash_path.fills:
            if fill.at_ns != at:
                continue
            if fill.leg == "ENTRY":
                q += fill.quantity
                entered += fill.quantity
                entry_value += fill.quantity * fill.executable_price
            else:
                q -= fill.quantity
            fees += fee_cash(
                fill.quantity,
                fill.executable_price,
                b,
                inp.fees.entry_taker_rate if fill.leg == "ENTRY" else inp.fees.exit_taker_rate,
            )
            slippage += fill.additional_slippage_cash
            if not margin_ok():
                return False
    return True


def evaluation_work_priority(candidate, *, best_cash: Decimal, cpu_budget_ns: int) -> Decimal:
    """Unitless effort heuristic, not a confidence or economic admission bound.

    Proxy cash uses a fixed one-USDT normalization; unknown proxy contributes
    no invented estimate. CPU and wall budgets use actual measured work.
    """
    proxy = max(ZERO, candidate.proxy_cash - best_cash) if candidate.proxy_cash is not None else ZERO
    cost = Decimal(candidate.estimated_cpu_ns) / max(1, cpu_budget_ns)
    return (
        candidate.usefulness + candidate.information_gain + candidate.freshness + candidate.uncertainty + proxy - cost
    )
