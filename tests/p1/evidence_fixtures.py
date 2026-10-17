"""Real offline producers over explicitly synthetic, indexed causal evidence."""

from dataclasses import replace
from decimal import Decimal as D
from unittest.mock import patch

from atlas.v2._serialization import sha256_json
from atlas.v2.science.mean_calibration import (
    FEATURE_REF,
    GRADE,
    MeanCalibrationProtocolV1,
    MeanEvidenceScopeV1,
    MeanOriginEvidenceV1,
    MeanOutcomeEvidenceV1,
    calibrate_verified_selected_policy,
    produce_verified_mean_oof,
)
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import index_mean_artifact
from atlas.v2.science.mean_models import fit_m0_mean
from atlas.v2.science.mean_population import (
    record_population_outcome,
    record_population_registration,
    record_population_slot,
)
from atlas.v2.science.mean_research import ProtectedMeanHoldoutV1, reserve_mean_holdout
from atlas.v2.science.mean_scenario_evidence import (
    ScenarioExecutionModelV1,
    ScenarioMarketEvidenceV1,
    ScenarioObservationV1,
    ScenarioProbabilityCalibrationV1,
    ScenarioProtocolV1,
    ScenarioSourceV1,
    qualify_scenarios,
)


def register_synthetic_population(repo, protocol, slots):
    """Explicit synthetic-clock simulation; never historical market provenance."""
    with patch("atlas.v2.science.mean_population._observed_now_ns", return_value=protocol.registered_at_ns):
        return record_population_registration(repo, protocol=protocol,
            slots=tuple((s, s, s + 1_000_000_000) for s in slots),
            producer_id="P1_SYNTHETIC_PREOUTCOME_RECORDER")


def record_synthetic_slot(repo, protocol, at, decisions):
    with patch("atlas.v2.science.mean_population._observed_now_ns", return_value=at):
        return record_population_slot(repo, protocol=protocol, slot_ns=at, decisions=decisions)


def record_synthetic_outcome(repo, protocol, evidence):
    with patch("atlas.v2.science.mean_population._observed_now_ns", return_value=evidence.available_at_ns):
        return record_population_outcome(repo, protocol=protocol, outcome=evidence)


def verified_calibration(repo, *, product, policy, compatibility_ref, cash, now_ns, model_expiry=None,
                         residual_errors=None, unequal=False):
    scope = MeanEvidenceScopeV1(
        product.content_hash,
        policy.content_hash,
        compatibility_ref,
        FEATURE_REF,
        DAY_NS,
        (1.0, 6.0, 0.01, 2.0, 1.0, 0.0),
        (1.0, 8.0, 0.03, 6.0, 5.0, 1.0),
        0,
        now_ns + DAY_NS,
        "CAUSAL_REPLAY",
        GRADE,
    )
    index_mean_artifact(repo, scope, available_at_ns=0)
    hold = ProtectedMeanHoldoutV1(
        policy.content_hash,
        compatibility_ref,
        scope.content_hash,
        0,
        now_ns + 10 * DAY_NS,
        now_ns + 30 * DAY_NS,
        (sha256_json({"protected": scope.content_hash}),),
    )
    reserve_mean_holdout(repo, hold)
    protocol = MeanCalibrationProtocolV1(
        scope.content_hash, hold.content_hash, DAY_NS // 2, 100 * DAY_NS, 320 * DAY_NS,
        resamples=10000 if residual_errors is not None else 100
    )
    index_mean_artifact(repo, protocol, available_at_ns=protocol.registered_at_ns)
    slots = tuple((100 + 7 * k) * DAY_NS for k in range(30))
    registered_slots = tuple(at + j for k, at in enumerate(slots)
        for j in range(3 if unequal and k % 3 == 0 else 1))
    register_synthetic_population(repo, protocol, registered_slots)
    rows = []
    for i, day in enumerate(tuple(range(1, 41)) + tuple(100 + 7 * k for k in range(30))):
        features = (1.0, 6.0 + (i % 2) * 2, 0.01 + (i % 2) * 0.02, 2.0 + (i % 2) * 4, 1.0 + (i % 2) * 4, float(i % 2))
        origin = MeanOriginEvidenceV1(
            scope.content_hash, f"synthetic-action-{day}", day * DAY_NS, day * DAY_NS, features
        )
        index_mean_artifact(repo, origin, available_at_ns=origin.available_at_ns)
        if day >= 100 and residual_errors is None:
            record_synthetic_slot(repo, protocol, origin.origin_ns,
                                  ((origin.content_hash, "SELECTED", "original synthetic policy decision"),))
        if day >= 100 and residual_errors is not None:
            continue
        evidence = MeanOutcomeEvidenceV1(
            scope.content_hash,
            origin.content_hash,
            day * DAY_NS,
            (day + 1) * DAY_NS,
            (day + 1) * DAY_NS + 1,
            features,
            D(cash),
            "FULL_FILL",
        )
        index_mean_artifact(repo, evidence, available_at_ns=evidence.available_at_ns)
        if day >= 100:
            record_synthetic_outcome(repo, protocol, evidence)
        row = evidence.training_row(scope)
        index_mean_artifact(repo, row, available_at_ns=row.label_available_at_ns)
        rows.append(row)
    training = tuple(rows)
    if residual_errors is None:
        residuals = produce_verified_mean_oof(repo, rows, protocol=protocol)
    else:
        residuals = []
        # Separate fixture cell: the recorder observes all alternatives before
        # outcomes are revealed. Its declared policy selects the variable action.
        for k, error in enumerate(residual_errors):
            at = slots[k]
            for j in range(3 if unequal and k % 3 == 0 else 1):
                origin = MeanOriginEvidenceV1(scope.content_hash, f"bootstrap-{k}-{j}", at+j, at+j,
                                             training[0].features)
                index_mean_artifact(repo, origin, available_at_ns=at+j)
                # At j=0 the same source snapshot includes the ordinary candidate.
                ordinary = MeanOriginEvidenceV1(scope.content_hash, f"synthetic-action-{100+7*k}",
                    at, at, (1.0, 6.0 + (k % 2)*2, 0.01 + (k % 2)*0.02,
                             2.0 + (k % 2)*4, 1.0 + (k % 2)*4, float(k % 2)))
                decisions = ((origin.content_hash, "SELECTED", "original synthetic variable-policy decision"),)
                if j == 0:
                    decisions += ((ordinary.content_hash, "REJECTED", "pre-outcome reference candidate"),)
                record_synthetic_slot(repo, protocol, at+j, decisions)
                evidence = MeanOutcomeEvidenceV1(scope.content_hash, origin.content_hash, at+j, at+j + DAY_NS,
                    at+j + DAY_NS + 1, origin.features, D(cash) - error, "FULL_FILL")
                index_mean_artifact(repo, evidence, available_at_ns=evidence.available_at_ns)
                record_synthetic_outcome(repo, protocol, evidence)
                row = evidence.training_row(scope)
                index_mean_artifact(repo, row, available_at_ns=row.label_available_at_ns)
                residuals.extend(produce_verified_mean_oof(repo, (*training, row), protocol=protocol))
        residuals = tuple(residuals)
    model = fit_m0_mean(
        rows,
        compatibility_ref=compatibility_ref,
        cutoff_ns=now_ns - 1000,
        available_at_ns=now_ns - 900,
        expires_at_ns=model_expiry or now_ns + DAY_NS,
        provenance="CAUSAL_REPLAY",
    )
    index_mean_artifact(repo, model, available_at_ns=model.available_at_ns)
    calibration = calibrate_verified_selected_policy(
        repo, model=model, protocol=protocol, residuals=residuals, cutoff_ns=now_ns - 700, expires_at_ns=now_ns + DAY_NS
    )
    return model, calibration, scope, hold, protocol, residuals


def verified_scenarios(repo, scenarios, *, scope, hold, policy, snapshot):
    now_ns = scenarios.cutoff_ns
    templates = []
    origins = []
    for path in scenarios.paths:
        origin = 10 * DAY_NS
        origins.append(origin)
        points = []
        for point in path.points:
            at = origin + point.at_ns - now_ns
            raw = ScenarioMarketEvidenceV1(
                scope.content_hash,
                at,
                at + 1,
                point.mark,
                point.bid,
                point.ask,
                point.bid_quantity,
                point.ask_quantity,
                None,
                point.closed_m15_price,
            )
            index_mean_artifact(repo, raw, available_at_ns=raw.available_at_ns)
            points.append(replace(point, at_ns=at, source_ref=raw.content_hash))
        payments = []
        for pay in path.funding:
            at = origin + pay.at_ns - now_ns
            raw = ScenarioMarketEvidenceV1(
                scope.content_hash,
                at,
                at + 1,
                pay.mark_price,
                pay.mark_price,
                pay.mark_price,
                D("0"),
                D("0"),
                pay.signed_rate,
            )
            index_mean_artifact(repo, raw, available_at_ns=raw.available_at_ns)
            payments.append(replace(pay, at_ns=at, source_ref=raw.content_hash))
        templates.append(replace(path, points=tuple(points), funding=tuple(payments)))
    source = ScenarioSourceV1(
        scope.content_hash,
        snapshot.content_hash,
        now_ns - 4,
        now_ns + DAY_NS,
        tuple(origins),
        tuple(templates),
        scenarios.stress_ids,
        scenarios.stress_exit_prices,
    )
    index_mean_artifact(repo, source, available_at_ns=source.available_at_ns)
    protocol = ScenarioProtocolV1(
        scope.content_hash, hold.content_hash, DAY_NS // 2, 42 * DAY_NS, 44 * DAY_NS, 300 * DAY_NS
    )
    index_mean_artifact(repo, protocol, available_at_ns=protocol.registered_at_ns)
    train = []
    validation = []
    for target, days in [(train, range(1, 41)), (validation, range(44, 244))]:
        for i, day in enumerate(days):
            observation = ScenarioObservationV1(
                scope.content_hash,
                day * DAY_NS,
                (day + 1) * DAY_NS,
                (day + 1) * DAY_NS + 1,
                scenarios.paths[i % len(scenarios.paths)].common_path_id,
                scenarios.entry_delay_ns,
                scenarios.exit_delay_ns,
                D("0"),
            )
            index_mean_artifact(repo, observation, available_at_ns=observation.available_at_ns)
            target.append(observation.content_hash)
    execution = ScenarioExecutionModelV1(
        scope.content_hash,
        protocol.content_hash,
        tuple(train),
        now_ns - 4,
        now_ns + DAY_NS,
        scenarios.entry_delay_ns,
        scenarios.exit_delay_ns,
    )
    index_mean_artifact(repo, execution, available_at_ns=execution.available_at_ns)
    calibration = ScenarioProbabilityCalibrationV1(
        source.content_hash,
        execution.content_hash,
        protocol.content_hash,
        tuple(train),
        tuple(validation),
        now_ns - 3,
        now_ns + DAY_NS,
        tuple(p.common_path_id for p in scenarios.paths),
        tuple(p.probability for p in scenarios.paths),
    )
    index_mean_artifact(repo, calibration, available_at_ns=calibration.available_at_ns)
    shifted = tuple(
        replace(
            t,
            points=tuple(replace(p, at_ns=p.at_ns + now_ns - o) for p in t.points),
            funding=tuple(replace(p, at_ns=p.at_ns + now_ns - o) for p in t.funding),
        )
        for t, o in zip(templates, origins, strict=True)
    )
    from atlas.v2.science.mean_scenarios import MeanScenarioSetV1

    body = scenarios.to_dict()
    body.pop("qualification_ref", None)
    body["version"] = MeanScenarioSetV1.VERSION
    base = MeanScenarioSetV1.from_dict(body)
    base = replace(
        base,
        source_ref=source.content_hash,
        execution_ref=execution.content_hash,
        calibration_ref=calibration.content_hash,
        source_available_at_ns=source.available_at_ns,
        paths=shifted,
    )
    return qualify_scenarios(
        repo,
        base,
        policy=policy,
        snapshot=snapshot,
        scenario_es_cash=D("0"),
        available_at_ns=now_ns - 1,
        expires_at_ns=now_ns + DAY_NS,
    )
