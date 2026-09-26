"""Negative provenance and causal-lineage regressions for Session-018 review."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import ACTUAL_CLOSE_PROVENANCE, ActualClosedPositionSourceV2, index_research_evidence
from atlas.v2.science.outcomes import (
    ActualActionPositionBindingV2,
    CalendarStateV2,
    DiagnosticTargetEvidenceV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
    executable_action_value_training_eligible,
    index_actual_action_position_binding,
    index_diagnostic_target_evidence,
    index_matured_outcome,
    matured_diagnostic_eligible,
    realized_risk_source,
)
from atlas.v2.science.pretrade import (
    JOINT_DIMENSIONS,
    CausalInputV2,
    JointScenarioPayloadV2,
    JointScenarioV2,
    PretradeDerivedEvidenceV2,
    PretradeScenarioArtifactV2,
    ScenarioStatusV2,
    index_joint_scenario_payload,
    index_pretrade_derived_evidence,
    index_pretrade_scenario,
)
from atlas.v2.science.replay import HOUR_NS, ReplayStatusV2

from .test_session014_core import KEY
from .test_session017_replay import minute, replay_context, run
from .test_session017_risk import CUTOFF, actual_outcome, risk_case


def _index(repo, ref, kind, at, body):
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at, at, body))


def _payoff_case(repo, *, entry_depth="100", entry_ask="100"):
    case = risk_case(repo)
    context = replay_context(repo, case, minutes=(
        minute(CUTOFF, ask=entry_ask, ask_depth=entry_depth),
        minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110", mark_low="110", mark_high="110",
               last_low="110", last_high="110")))
    payoff = run(repo, case, context)
    action = context[0]
    fees = (payoff.entry.fee if payoff.entry else Decimal(0)) + sum((x.fee for x in payoff.exits), Decimal(0))
    funding = sum((cash for _, cash in payoff.funding_cashflows), Decimal(0))
    assert payoff.payoff is not None
    item = MaturedOutcomeV2(
        decision_ref=sha256_json("decision"), candidate_set_ref=case.candidate_set.content_hash,
        candidate_ref=case.candidate.content_hash, policy_id=action.action.policy_id,
        policy_version=action.action.policy_version, policy_hash=action.action.policy_hash,
        action_hash=action.action.action_hash, action_artifact_ref=action.content_hash,
        action_absence_reason=None, instrument_revision=KEY.contract_revision, venue=KEY.venue.value,
        product=KEY.product.value, decision_at_ns=CUTOFF, horizon_end_ns=case.candidate.horizon_end_ns,
        matured_at_ns=payoff.available_at_ns, available_at_ns=payoff.available_at_ns + 1,
        label_definition="net_action_value_v2", label_view="RECONSTRUCTED_MARKET",
        calendar_state=CalendarStateV2(payoff.status.value), label_state=LabelStateV2.MATURED,
        provenance=OutcomeProvenanceV2.SIMULATED, payoff_unit="USDT", quantity_unit="CONTRACTS",
        gross_payoff=payoff.payoff + fees - funding, fees=fees, funding_cashflow=funding,
        net_payoff=payoff.payoff, fill_quantity=payoff.filled_quantity,
        requested_quantity=action.action.quantity, mfe=None, mae=None,
        evidence_refs=(payoff.content_hash,), execution_evidence_ref=payoff.content_hash,
        extrema_evidence_ref=None, actual_closed_source_ref=None, evidence_resolution="MINUTE",
        evidence_quality="REPLAY_BOUND", ambiguity=(), outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE,
        diagnostic_value=None, diagnostic_unit=None, diagnostic_evidence_ref=None,
        actual_action_binding_ref=None)
    return case, action, payoff, item


def test_matured_unselected_rejected_diagnostic_and_separate_eligibility(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, include_s2=True)
        candidate = case.s2_candidate
        assert candidate is not None
        body = {"target": "future_mid_return_v1", "candidate_ref": candidate.content_hash,
                "horizon_end_ns": CUTOFF + HOUR_NS, "value": "0.02", "unit": "FRACTION"}
        source_ref = sha256_json(body)
        index_research_evidence(repo, "CausalMarketDiagnosticV2", source_ref, CUTOFF + HOUR_NS + 1, body)
        definition = {"label_definition": "future_mid_return_v1", "unit": "FRACTION"}
        definition_ref = sha256_json(definition)
        index_research_evidence(repo, "DiagnosticTargetDefinitionV2", definition_ref, CUTOFF, definition)
        diagnostic = DiagnosticTargetEvidenceV2(sha256_json("decision"), case.candidate_set.content_hash,
            candidate.content_hash, "future_mid_return_v1", definition_ref, CUTOFF, CUTOFF + HOUR_NS,
            Decimal("0.02"), "FRACTION", (source_ref,), CUTOFF + HOUR_NS + 1, CUTOFF + HOUR_NS + 1)
        late_definition = {"label_definition": "future_mid_return_v1", "unit": "FRACTION", "revision": 2}
        late_ref = sha256_json(late_definition)
        index_research_evidence(repo, "DiagnosticTargetDefinitionV2", late_ref, CUTOFF + 1, late_definition)
        with pytest.raises(ValueError):
            index_diagnostic_target_evidence(repo, replace(diagnostic, target_declaration_ref=late_ref))
        index_diagnostic_target_evidence(repo, diagnostic)
        base = {"decision_ref": sha256_json("decision"), "candidate_set_ref": case.candidate_set.content_hash,
            "candidate_ref": candidate.content_hash, "policy_id": "S2_COMPRESSION_BREAKOUT",
            "policy_version": "1", "policy_hash": candidate.policy_hash,
            "action_hash": None, "action_artifact_ref": None, "action_absence_reason": "NOT_RISK_SIZED",
            "instrument_revision": KEY.contract_revision, "venue": KEY.venue.value, "product": KEY.product.value,
            "decision_at_ns": CUTOFF, "horizon_end_ns": CUTOFF + HOUR_NS,
            "matured_at_ns": CUTOFF + HOUR_NS + 1, "available_at_ns": CUTOFF + HOUR_NS + 2,
            "label_definition": "future_mid_return_v1", "label_view": "RECONSTRUCTED_MARKET",
            "label_state": LabelStateV2.MATURED, "provenance": OutcomeProvenanceV2.COUNTERFACTUAL,
            "payoff_unit": "USDT", "quantity_unit": "CONTRACTS", "gross_payoff": None, "fees": None,
            "funding_cashflow": None, "net_payoff": None, "fill_quantity": None, "requested_quantity": None,
            "mfe": None, "mae": None, "evidence_refs": tuple(sorted((diagnostic.content_hash, source_ref))),
            "execution_evidence_ref": None,
            "extrema_evidence_ref": None, "actual_closed_source_ref": None, "evidence_resolution": "FINAL_BAR",
            "evidence_quality": "CAUSAL_MARKET_ONLY", "ambiguity": (),
            "outcome_target": OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
            "diagnostic_value": Decimal("0.02"), "diagnostic_unit": "FRACTION",
            "diagnostic_evidence_ref": diagnostic.content_hash, "actual_action_binding_ref": None}
        for state in (CalendarStateV2.UNSELECTED, CalendarStateV2.REJECTED):
            item = MaturedOutcomeV2(calendar_state=state, **base)
            assert MaturedOutcomeV2.from_dict(item.to_dict()) == item
            assert index_matured_outcome(repo, item) == item.content_hash
            assert matured_diagnostic_eligible(item, item.available_at_ns)
            assert not matured_diagnostic_eligible(item, item.available_at_ns - 1)
            assert not executable_action_value_training_eligible(item, item.available_at_ns)
            assert not executable_action_value_training_eligible(item, item.available_at_ns + 1)
            with pytest.raises(ValueError):
                replace(item, fill_quantity=Decimal("1"))
            with pytest.raises(ValueError):
                replace(item, net_payoff=Decimal("1"))
            with pytest.raises(ValueError):
                index_matured_outcome(repo, replace(item, diagnostic_value=Decimal("0.03")))
        censored = replace(item, label_state=LabelStateV2.CENSORED, diagnostic_value=None,
                           diagnostic_evidence_ref=None,
                           reason="INSUFFICIENT_MARKET_EVIDENCE")
        assert not matured_diagnostic_eligible(censored, censored.available_at_ns)
        for state in (CalendarStateV2.SELECTED, CalendarStateV2.NO_TRADE,
                      CalendarStateV2.NOT_ESTIMABLE, CalendarStateV2.EXPIRED):
            assert replace(censored, calendar_state=state).calendar_state == state
        assert replace(censored, calendar_state=CalendarStateV2.NO_CANDIDATE,
            candidate_ref=None, instrument_revision=None, venue=None, product=None).candidate_ref is None
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "version": "future"})
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "unknown": 1})
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "diagnostic_value": "0.020"})
        with pytest.raises(ValueError):
            replace(item, outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE)


def test_exact_policy_payoff_fill_components_and_generic_rejection(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, payoff, item = _payoff_case(repo, entry_depth="10")
        assert payoff.status == ReplayStatusV2.PARTIAL_FILL
        assert index_matured_outcome(repo, item) == item.content_hash
        assert executable_action_value_training_eligible(item, item.available_at_ns)
        assert not executable_action_value_training_eligible(item, item.available_at_ns - 1)
        assert MaturedOutcomeV2.from_dict(item.to_dict()).content_hash == item.content_hash
        with pytest.raises(ValueError):
            replace(item, matured_at_ns=item.horizon_end_ns - 1)
        with pytest.raises(ValueError):
            replace(item, available_at_ns=item.matured_at_ns - 1)
        for changed in (replace(item, decision_ref=sha256_json("another-decision")),
                        replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL),
                        replace(item, gross_payoff=item.gross_payoff + Decimal("1"),
                                net_payoff=item.net_payoff + Decimal("1")),
                        replace(item, evidence_refs=tuple(sorted((item.execution_evidence_ref,
                                                                   sha256_json("extra-evidence")))))):
            assert changed.content_hash != item.content_hash
        generic = sha256_json({"fixture": 1})
        _index(repo, generic, "ReplayExecutionV2", item.matured_at_ns, {"fixture": 1})
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, execution_evidence_ref=generic,
                evidence_refs=(generic,)))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
                execution_evidence_ref=generic, evidence_refs=(generic,)))
        for change in ({"action_hash": sha256_json("other-action")},
                       {"fill_quantity": item.fill_quantity + Decimal("1")},
                       {"calendar_state": CalendarStateV2.FULL_FILL},
                       {"gross_payoff": item.gross_payoff + Decimal("1"), "net_payoff": item.net_payoff + Decimal("1")},
                       {"fees": item.fees + Decimal("1"), "net_payoff": item.net_payoff - Decimal("1")},
                       {"funding_cashflow": Decimal("1"), "net_payoff": item.net_payoff + Decimal("1")}):
            with pytest.raises(ValueError):
                index_matured_outcome(repo, replace(item, **change))
        with pytest.raises(ValueError):
            replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
                    action_hash=None, action_artifact_ref=None, action_absence_reason="REJECTED")
        # A content-addressed frozen action B still cannot use payoff A.
        indexed_action = repo.get_artifact(action.content_hash)
        assert indexed_action is not None
        identity_b = json_value(indexed_action.metadata["action_identity"])
        identity_b["quantity"] = canonical_decimal_str(action.action.quantity + Decimal("0.1"))
        action_hash_b = sha256_json(identity_b)
        artifact_b = json_value(indexed_action.metadata["action_artifact"])
        artifact_b["action_hash"] = action_hash_b
        action_ref_b = sha256_json(artifact_b)
        _index(repo, action_ref_b, "ActionArtifactV2", CUTOFF,
               {"action_artifact": artifact_b, "action_identity": identity_b})
        other = replace(item, action_hash=action_hash_b, action_artifact_ref=action_ref_b)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, other)
        assert case.candidate_set.content_hash == item.candidate_set_ref
        assert action.content_hash == payoff.action_artifact_ref


def test_no_fill_and_unresolved_replay_cannot_mature_as_fill(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, payoff, item = _payoff_case(repo, entry_ask="101")
        assert payoff.status == ReplayStatusV2.NO_FILL
        assert item.fill_quantity == 0
        index_matured_outcome(repo, item)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, calendar_state=CalendarStateV2.PARTIAL_FILL,
                fill_quantity=Decimal("1")))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, net_payoff=Decimal("1"), gross_payoff=Decimal("1")))
        with pytest.raises(ValueError):
            replace(item, calendar_state=CalendarStateV2.SELECTED)


def _pretrade_case(repo):
    def h(label):
        return sha256_json(label)
    identity = {"version": "TEST_FROZEN_ACTION", "fixture": "pretrade-action"}
    action_hash = sha256_json(identity)
    artifact = {"action_hash": action_hash, "version": "TEST_ACTION_ARTIFACT"}
    action_ref, set_id = sha256_json(artifact), h("common-set")
    inputs = (CausalInputV2(h("model"), "ModelManifestV2", 90, 91),
              CausalInputV2(h("calibration"), "CalibrationV2", 92, 93),
              CausalInputV2(h("execution-model"), "ExecutionModelV2", 94, 95),
              CausalInputV2(h("source"), "RawObservationV2", 96, 97))
    _index(repo, action_ref, "ActionArtifactV2", 90,
           {"action_artifact": artifact, "action_identity": identity})
    for item in inputs:
        _index(repo, item.ref, item.kind, item.available_at_ns, {})
    dummy = h("dummy")
    base = PretradeScenarioArtifactV2(action_hash, action_ref, 100, "gen1", "m1", "s1",
        inputs[0], inputs[1], (inputs[3],), (dummy,), inputs[2],
        (JointScenarioV2(h("path-a"), h("row-a"), Decimal("0.25")),
         JointScenarioV2(h("path-b"), h("row-b"), Decimal("0.75"))),
        set_id, JOINT_DIMENSIONS, (dummy,), dummy, dummy, dummy, dummy,
        "NET_CASHFLOW_ONCE_V1", 101, 106, 107, 110, ScenarioStatusV2.AVAILABLE)
    rows = []
    payloads = []
    for label, weight in (("path-a", "0.25"), ("path-b", "0.75")):
        data_body = {"payload": label}
        data_ref = sha256_json(data_body)
        _index(repo, data_ref, "JointScenarioDataV2", 103, data_body)
        payload = JointScenarioPayloadV2(action_hash, action_ref, 100, set_id, h(label), "gen1", "s1",
            base.causal_input_manifest_hash, JOINT_DIMENSIONS, data_ref, 101, 103, 104)
        index_joint_scenario_payload(repo, payload)
        rows.append(JointScenarioV2(h(label), payload.content_hash, Decimal(weight)))
        payloads.append(payload)
    roles = ("SUPPORT", "STRESS", "OUTCOME_DISPERSION", "ESTIMATION_UNCERTAINTY",
             "EXECUTION_UNCERTAINTY", "NUMERICAL_ERROR")
    derived = {}
    for role in roles:
        result_body = {"role": role}
        result_ref = sha256_json(result_body)
        _index(repo, result_ref, "PretradeDerivedResultV2", 104, result_body)
        item = PretradeDerivedEvidenceV2(role, action_hash, action_ref, 100, set_id,
            base.causal_input_manifest_hash, result_ref, 101, 105, 105)
        index_pretrade_derived_evidence(repo, item)
        derived[role] = item
    scenario = replace(base, rows=tuple(rows), support_refs=(derived["SUPPORT"].content_hash,),
        deterministic_stress_refs=(derived["STRESS"].content_hash,),
        outcome_dispersion_ref=derived["OUTCOME_DISPERSION"].content_hash,
        estimation_uncertainty_ref=derived["ESTIMATION_UNCERTAINTY"].content_hash,
        execution_uncertainty_ref=derived["EXECUTION_UNCERTAINTY"].content_hash,
        numerical_error_ref=derived["NUMERICAL_ERROR"].content_hash)
    return scenario, payloads, derived


def test_pretrade_joint_manifest_set_action_and_derived_binding(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        scenario, payloads, derived = _pretrade_case(repo)
        assert PretradeScenarioArtifactV2.from_dict(scenario.to_dict()) == scenario
        assert index_pretrade_scenario(repo, scenario) == scenario.content_hash
        assert payloads[0].available_at_ns > scenario.information_cutoff_ns
        assert derived["SUPPORT"].available_at_ns > scenario.information_cutoff_ns
        assert scenario.rows[0].probability + scenario.rows[1].probability == Decimal(1)
        assert derived["STRESS"].content_hash not in {row.joint_payload_ref for row in scenario.rows}
        for change in ({"source_inputs": (CausalInputV2(scenario.source_inputs[0].ref,
                            "RawObservationV2", 101, 101),)},
                       {"model_input": CausalInputV2(scenario.model_input.ref, "ModelManifestV2", 101, 101)},
                       {"available_at_ns": scenario.action_expires_at_ns},
                       {"joint_dimensions": ("price_mark_index",)},
                       {"deterministic_stress_refs": (scenario.rows[0].joint_payload_ref,)}):
            with pytest.raises(ValueError):
                replace(scenario, **change)
        for changed in (replace(scenario, action_hash=sha256_json("other-action")),
                        replace(scenario, information_cutoff_ns=99),
                        replace(scenario, model_version="other-model"),
                        replace(scenario, calibration_input=CausalInputV2(scenario.calibration_input.ref,
                            "CalibrationV2", 91, 93)),
                        replace(scenario, rows=(JointScenarioV2(scenario.rows[0].joint_path_id,
                            scenario.rows[0].joint_payload_ref, Decimal("0.5")),
                            JointScenarioV2(scenario.rows[1].joint_path_id,
                            scenario.rows[1].joint_payload_ref, Decimal("0.5"))))):
            assert changed.content_hash != scenario.content_hash
        for change in ({"action_hash": sha256_json("other-action")},
                       {"common_scenario_set_id": sha256_json("other-set")},
                       {"model_version": "other-model"},
                       {"calibration_input": CausalInputV2(scenario.calibration_input.ref,
                           "CalibrationV2", 91, 93)}):
            with pytest.raises(ValueError):
                index_pretrade_scenario(repo, replace(scenario, **change))
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario,
                rows=(JointScenarioV2(scenario.rows[0].joint_path_id, payloads[0].content_hash, Decimal("0.2")),)))
        with pytest.raises(ValueError):
            PretradeScenarioArtifactV2.from_dict({**scenario.to_dict(), "version": "future"})
        with pytest.raises(ValueError):
            JointScenarioPayloadV2.from_dict({**payloads[0].to_dict(), "unknown": 1})
        with pytest.raises(ValueError):
            PretradeDerivedEvidenceV2.from_dict({**derived["SUPPORT"].to_dict(), "unknown": 1})
        no_est = replace(scenario, status=ScenarioStatusV2.NOT_ESTIMABLE, rows=(), support_refs=(),
            deterministic_stress_refs=(), outcome_dispersion_ref=None, estimation_uncertainty_ref=None,
            execution_uncertainty_ref=None, numerical_error_ref=None, inability_reason="NO_DEPTH_SUPPORT")
        assert no_est.rows == ()
        with pytest.raises(ValueError):
            replace(no_est, status=ScenarioStatusV2.AVAILABLE, inability_reason=None)


def test_pretrade_payload_and_derived_cross_action_and_future_compute(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        scenario, payloads, derived = _pretrade_case(repo)
        h = sha256_json
        bad = replace(payloads[0], action_hash=h("other-action"))
        index_joint_scenario_payload(repo, bad)
        rows = (JointScenarioV2(bad.joint_path_id, bad.content_hash, Decimal("0.25")), scenario.rows[1])
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario, rows=rows))
        for change in ({"common_scenario_set_id": h("other-set")},
                       {"causal_input_manifest_hash": h("other-manifest")},
                       {"information_cutoff_ns": 99}):
            altered = replace(payloads[0], **change)
            index_joint_scenario_payload(repo, altered)
            with pytest.raises(ValueError):
                index_pretrade_scenario(repo, replace(scenario, rows=(
                    JointScenarioV2(altered.joint_path_id, altered.content_hash, Decimal("0.25")),
                    scenario.rows[1])))
        future = replace(payloads[0], computed_at_ns=108, available_at_ns=108)
        # A forged early index timestamp cannot hide the payload's later completion.
        _index(repo, future.content_hash, "JointScenarioPayloadV2", 102,
               {"joint_payload": future.to_dict()})
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario, rows=(
                JointScenarioV2(future.joint_path_id, future.content_hash, Decimal("0.25")), scenario.rows[1])))
        for role in ("SUPPORT", "STRESS", "ESTIMATION_UNCERTAINTY"):
            wrong = replace(derived[role], action_hash=h("other-action"))
            index_pretrade_derived_evidence(repo, wrong)
            field = {"SUPPORT": "support_refs", "STRESS": "deterministic_stress_refs",
                     "ESTIMATION_UNCERTAINTY": "estimation_uncertainty_ref"}[role]
            value = (wrong.content_hash,) if role in ("SUPPORT", "STRESS") else wrong.content_hash
            with pytest.raises(ValueError):
                index_pretrade_scenario(repo, replace(scenario, **{field: value}))
        wrong_cutoff = replace(derived["EXECUTION_UNCERTAINTY"], information_cutoff_ns=99)
        index_pretrade_derived_evidence(repo, wrong_cutoff)
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario,
                execution_uncertainty_ref=wrong_cutoff.content_hash))
        for kind in ("ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"):
            with pytest.raises(ValueError):
                CausalInputV2(h(kind), kind, 90, 91)
        with pytest.raises(ValueError):
            replace(scenario, rows=(JointScenarioV2(scenario.rows[0].joint_path_id,
                scenario.rows[0].joint_payload_ref, Decimal("0.3")), scenario.rows[1]))


def test_actual_binding_exact_account_epoch_and_no_research_risk_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, action, _, sim = _payoff_case(repo)
        closed = actual_outcome(repo, sim.horizon_end_ns, sim.matured_at_ns,
                                sim.net_payoff, account_scope="SHADOW_FAKE_ACCOUNT")
        source_entry = repo.get_artifact(closed.position_ref)
        assert source_entry is not None
        source_body = source_entry.metadata
        source = ActualClosedPositionSourceV2(source_body["account_scope"], source_body["position_epoch_id"],
            KEY, source_body["close_at_ns"], Decimal(source_body["realized_net_pnl"]),
            source_body["available_at_ns"], ACTUAL_CLOSE_PROVENANCE,
            source_body["execution_source_ref"], source_body["economic_source_ref"])
        shared = {"action_hash": sim.action_hash, "action_artifact_ref": action.content_hash,
            "candidate_ref": sim.candidate_ref, "candidate_set_ref": sim.candidate_set_ref,
            "actual_closed_source_ref": source.content_hash, "account_scope": source.account_scope,
            "position_epoch_id": source.position_epoch_id}
        link_body = {**shared, "source_system": "VENUE_RECONCILED_ACTION_POSITION",
                     "execution_source_ref": source.execution_source_ref}
        link_ref = sha256_json(link_body)
        _index(repo, link_ref, "V2ActualActionPositionLinkObservationV1", sim.matured_at_ns, link_body)
        economics_body = {**shared, "source_system": "ACCOUNT_RECONCILED_ACTION_CASH",
            "economic_source_ref": source.economic_source_ref,
            "gross_payoff": canonical_decimal_str(sim.gross_payoff),
            "fees": canonical_decimal_str(sim.fees),
            "funding_cashflow": canonical_decimal_str(sim.funding_cashflow),
            "net_payoff": canonical_decimal_str(sim.net_payoff),
            "requested_quantity": canonical_decimal_str(sim.requested_quantity),
            "fill_quantity": canonical_decimal_str(sim.fill_quantity),
            "fill_status": sim.calendar_state.value}
        economics_ref = sha256_json(economics_body)
        _index(repo, economics_ref, "V2ActualActionEconomicsObservationV1", sim.matured_at_ns, economics_body)
        binding = ActualActionPositionBindingV2(sim.action_hash, action.content_hash, sim.candidate_ref,
            sim.candidate_set_ref, source.content_hash, source.account_scope, source.position_epoch_id,
            link_ref, economics_ref, sim.matured_at_ns)
        index_actual_action_position_binding(repo, binding)
        actual = replace(sim, provenance=OutcomeProvenanceV2.ACTUAL, label_view="ACTUAL_SYSTEM",
            actual_closed_source_ref=source.content_hash, actual_action_binding_ref=binding.content_hash,
            execution_evidence_ref=binding.content_hash,
            evidence_refs=tuple(sorted((source.content_hash, binding.content_hash))))
        index_matured_outcome(repo, actual)
        assert realized_risk_source(repo, actual, source) == source.content_hash
        with pytest.raises(ValueError):
            realized_risk_source(repo, sim, source)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(actual, action_hash=sha256_json("other-action")))
        other = actual_outcome(repo, sim.horizon_end_ns, sim.matured_at_ns, sim.net_payoff,
                               account_scope="OTHER_ACCOUNT")
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(actual, actual_closed_source_ref=other.position_ref,
                evidence_refs=tuple(sorted((binding.content_hash, other.position_ref)))))
        with pytest.raises(ValueError):
            index_actual_action_position_binding(repo, replace(binding, position_epoch_id=sha256_json("other-epoch")))
