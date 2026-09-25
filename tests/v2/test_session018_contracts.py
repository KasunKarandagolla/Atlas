"""Session-018 causal, provenance and immutable contract fixtures."""
from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.outcomes import (
    CalendarStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    index_matured_outcome,
    realized_risk_source,
    training_eligible,
)
from atlas.v2.science.pretrade import (
    JOINT_DIMENSIONS,
    CausalInputV2,
    JointScenarioV2,
    PretradeScenarioArtifactV2,
    ScenarioStatusV2,
    index_pretrade_scenario,
)
from atlas.v2.selection import SELECTION_POLICY_HASH
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY

from .test_session014_core import KEY
from .test_session017_risk import actual_outcome

H = "a" * 64
B = "b" * 64
C = "c" * 64
D = "d" * 64
E = "e" * 64
F = "f" * 64
G = sha256_json("joint-a")
JOINT_B = sha256_json("joint-b")
J = sha256_json("stress")
K = sha256_json("dispersion")
L = sha256_json("estimation")
M = sha256_json("execution")
N = sha256_json("numerical")
ACTION_REF = sha256_json("action-artifact")


def outcome(**changes):
    values = {"decision_ref": H, "candidate_set_ref": B, "candidate_ref": C, "policy_id": "S1",
        "policy_version": "1", "policy_hash": D, "action_hash": E, "action_artifact_ref": ACTION_REF,
        "action_absence_reason": None,
        "instrument_revision": KEY.contract_revision, "venue": "BYBIT", "product": "LINEAR_PERPETUAL",
        "decision_at_ns": 100, "horizon_end_ns": 200, "matured_at_ns": 210, "available_at_ns": 220,
        "label_definition": "net_action_value_v1", "label_view": "RECONSTRUCTED_MARKET",
        "calendar_state": CalendarStateV2.PARTIAL_FILL, "label_state": LabelStateV2.MATURED,
        "provenance": OutcomeProvenanceV2.SIMULATED, "payoff_unit": "USDT", "quantity_unit": "CONTRACTS",
        "gross_payoff": Decimal("4"), "fees": Decimal("1"),
        "funding_cashflow": Decimal("-0.5"), "net_payoff": Decimal("2.5"),
        "fill_quantity": Decimal("0.5"), "requested_quantity": Decimal("1"), "mfe": None, "mae": None,
        "evidence_refs": (F,), "execution_evidence_ref": F, "extrema_evidence_ref": None,
        "actual_closed_source_ref": None,
        "evidence_resolution": "minute", "evidence_quality": "PARTIAL", "ambiguity": ("IOC_SECOND_SCALE",), "reason": None}
    values.update(changes)
    return MaturedOutcomeV2(**values)


def scenario(**changes):
    inputs = (CausalInputV2(B, "ModelManifestV2", 90, 91),
              CausalInputV2(C, "CalibrationV2", 92, 93),
              CausalInputV2(D, "ExecutionModelV2", 94, 95))
    values = {"action_hash": H, "action_artifact_ref": E, "information_cutoff_ns": 100,
        "generation_version": "gen1", "model_version": "m1", "scenario_version": "s1",
        "model_input": inputs[0], "calibration_input": inputs[1], "source_inputs": (CausalInputV2(F, "RawObservationV2", 96, 97),),
        "support_refs": (B,), "execution_model_input": inputs[2],
        "rows": (JointScenarioV2(B, G, Decimal("0.25")), JointScenarioV2(C, JOINT_B, Decimal("0.75"))),
        "common_scenario_set_id": F, "joint_dimensions": JOINT_DIMENSIONS,
        "deterministic_stress_refs": (J,), "outcome_dispersion_ref": K, "estimation_uncertainty_ref": L,
        "execution_uncertainty_ref": M, "numerical_error_ref": N, "cost_accounting_version": "NET_CASHFLOW_ONCE_V1",
        "created_at_ns": 101, "computed_at_ns": 102, "available_at_ns": 103, "action_expires_at_ns": 110,
        "status": ScenarioStatusV2.AVAILABLE, "inability_reason": None}
    values.update(changes)
    return PretradeScenarioArtifactV2(**values)


def test_outcome_roundtrip_hash_calendar_and_chronology():
    item = outcome()
    assert MaturedOutcomeV2.from_dict(json.loads(json.dumps(item.to_dict()))).content_hash == item.content_hash
    assert item.net_payoff == Decimal("2.5")
    assert replace(item, mfe=Decimal("3"), mae=Decimal("-2"), extrema_evidence_ref=B,
        evidence_refs=(B, F)).mfe == Decimal("3")
    for change in ({"candidate_ref": F}, {"provenance": OutcomeProvenanceV2.COUNTERFACTUAL},
                   {"gross_payoff": Decimal("5"), "net_payoff": Decimal("3.5")}, {"evidence_refs": (B, F)}):
        assert replace(item, **change).content_hash != item.content_hash
    for state in (CalendarStateV2.UNSELECTED, CalendarStateV2.REJECTED, CalendarStateV2.NO_TRADE,
                  CalendarStateV2.NOT_ESTIMABLE, CalendarStateV2.EXPIRED):
        assert outcome(calendar_state=state, action_hash=None, action_artifact_ref=None,
            action_absence_reason="not risk sized",
            fill_quantity=None, execution_evidence_ref=None, evidence_refs=(),
            label_state=LabelStateV2.CENSORED, gross_payoff=None, fees=None, funding_cashflow=None,
            net_payoff=None, reason="no executable label").calendar_state == state
    assert outcome(calendar_state=CalendarStateV2.SELECTED, label_state=LabelStateV2.UNRESOLVED,
        gross_payoff=None, fees=None, funding_cashflow=None, net_payoff=None,
        fill_quantity=None, mfe=None, mae=None, reason="horizon evidence pending").calendar_state == CalendarStateV2.SELECTED
    assert outcome(candidate_ref=None, instrument_revision=None, venue=None, product=None,
        calendar_state=CalendarStateV2.NO_CANDIDATE, action_hash=None, action_artifact_ref=None,
        action_absence_reason="no candidate",
        fill_quantity=None, execution_evidence_ref=None, evidence_refs=(), label_state=LabelStateV2.UNRESOLVED,
        gross_payoff=None, fees=None, funding_cashflow=None, net_payoff=None, reason="no candidate")
    assert outcome(calendar_state=CalendarStateV2.NO_FILL, fill_quantity=Decimal(0), gross_payoff=Decimal(0),
        fees=Decimal(0), funding_cashflow=Decimal(0), net_payoff=Decimal(0)).fill_quantity == 0
    with pytest.raises(ValueError):
        outcome(calendar_state=CalendarStateV2.NO_FILL, fill_quantity=Decimal(0), gross_payoff=Decimal(0),
            fees=Decimal(0), funding_cashflow=Decimal(0), net_payoff=Decimal(0),
            execution_evidence_ref=None, evidence_refs=())
    assert not training_eligible(item, 219) and training_eligible(item, 220)
    with pytest.raises(ValueError):
        replace(item, matured_at_ns=199)
    with pytest.raises(ValueError):
        replace(item, available_at_ns=209)
    with pytest.raises(ValueError):
        replace(item, net_payoff=Decimal(4))
    with pytest.raises(ValueError):
        replace(item, mfe=Decimal("3"), mae=Decimal("-2"))
    with pytest.raises(ValueError):
        replace(item, provenance=OutcomeProvenanceV2.ACTUAL)
    with pytest.raises(ValueError):
        replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL, execution_evidence_ref=None)
    with pytest.raises(ValueError):
        replace(item, calendar_state=CalendarStateV2.REJECTED, action_hash=None, action_artifact_ref=None,
                action_absence_reason="not risk sized")
    with pytest.raises(ValueError):
        outcome(calendar_state=CalendarStateV2.REJECTED, action_hash=None, action_artifact_ref=None,
            action_absence_reason="not risk sized", fill_quantity=None, execution_evidence_ref=None,
            evidence_refs=())
    assert outcome(calendar_state=CalendarStateV2.NO_TRADE, action_hash=None, action_artifact_ref=None,
        action_absence_reason="admission declined", gross_payoff=Decimal(0), fees=Decimal(0),
        funding_cashflow=Decimal(0), net_payoff=Decimal(0), fill_quantity=Decimal(0),
        execution_evidence_ref=None, evidence_refs=()).net_payoff == 0
    wire = item.to_dict()
    for bad in ({**wire, "unknown": 1}, {**wire, "version": "future"}, {**wire, "fees": "2"}):
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict(bad)


def test_outcome_index_and_actual_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(ArtifactIndexEntryV2(ACTION_REF, "ActionArtifactV2", ACTION_REF, 100, 100,
            {"action_artifact": {"action_hash": E, "candidate_ref": C, "candidate_set_ref": B},
             "action_identity": {"policy_id": "S1", "policy_version": "1", "policy_hash": D,
                                 "key": KEY.to_dict()}}))
        repo.register_artifact(ArtifactIndexEntryV2(F, "ReplayExecutionV2", F, 210, 210, {"fixture": 1}))
        sim = outcome()
        assert index_matured_outcome(repo, sim) == sim.content_hash
        assert index_matured_outcome(repo, sim) == sim.content_hash
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(sim, policy_hash=H))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(sim, matured_at_ns=209, available_at_ns=220))
        closed = actual_outcome(repo, 200, 210, Decimal("2.5"))
        source_entry = repo.get_artifact(closed.position_ref)
        assert source_entry is not None
        from atlas.v2.risk import ACTUAL_CLOSE_PROVENANCE, ActualClosedPositionSourceV2
        body = dict(source_entry.metadata)
        source = ActualClosedPositionSourceV2(body["account_scope"], body["position_epoch_id"], KEY,
            body["close_at_ns"], Decimal(body["realized_net_pnl"]), body["available_at_ns"],
            ACTUAL_CLOSE_PROVENANCE, body["execution_source_ref"], body["economic_source_ref"])
        actual = replace(sim, provenance=OutcomeProvenanceV2.ACTUAL, label_view="ACTUAL_SYSTEM",
                         actual_closed_source_ref=source.content_hash,
                         evidence_refs=tuple(sorted((F, source.content_hash))))
        index_matured_outcome(repo, actual)
        assert realized_risk_source(actual, source) == source.content_hash
        with pytest.raises(ValueError):
            realized_risk_source(sim, source)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(actual, net_payoff=Decimal("9"), gross_payoff=Decimal("10.5")))


def test_pretrade_roundtrip_causal_weights_and_binding(tmp_path):
    item = scenario()
    assert PretradeScenarioArtifactV2.from_dict(item.to_dict()).content_hash == item.content_hash
    for change in ({"action_hash": B}, {"information_cutoff_ns": 99}, {"model_version": "m2"},
                   {"calibration_input": CausalInputV2(C, "CalibrationV2", 91, 93)},
                   {"rows": (JointScenarioV2(B, G, Decimal("0.5")), JointScenarioV2(C, JOINT_B, Decimal("0.5")))}):
        assert replace(item, **change).content_hash != item.content_hash
    for change in ({"source_inputs": (CausalInputV2(F, "RawObservationV2", 101, 101),)},
                   {"model_input": CausalInputV2(B, "ModelManifestV2", 101, 101)},
                   {"available_at_ns": 110}, {"rows": (JointScenarioV2(B, G, Decimal("0.2")),)},
                   {"deterministic_stress_refs": (G, J)}, {"joint_dimensions": ("price_mark_index",)}):
        with pytest.raises(ValueError):
            replace(item, **change)
    with pytest.raises(ValueError):
        CausalInputV2(B, "ReplayPathV2", 80, 90)
    with pytest.raises(ValueError):
        CausalInputV2(B, "PolicyPayoffV2", 80, 90)
    no_est = scenario(status=ScenarioStatusV2.NOT_ESTIMABLE, rows=(), support_refs=(),
        outcome_dispersion_ref=None, estimation_uncertainty_ref=None, execution_uncertainty_ref=None,
        numerical_error_ref=None, inability_reason="NO_DEPTH_SUPPORT")
    assert no_est.rows == ()
    for bad in ({**item.to_dict(), "unknown": 1}, {**item.to_dict(), "version": "future"},
                {**item.to_dict(), "model_version": "revised"}):
        with pytest.raises(ValueError):
            PretradeScenarioArtifactV2.from_dict(bad)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        for ref, kind, at, metadata in ((E, "ActionArtifactV2", 90, {"action_artifact": {"action_hash": H}}),
            (B, "ModelManifestV2", 91, {}), (C, "CalibrationV2", 93, {}),
            (D, "ExecutionModelV2", 95, {}), (F, "RawObservationV2", 97, {}),
            (G, "JointScenarioPayloadV2", 102, {"joint_path_id": B, "joint_dimensions": list(JOINT_DIMENSIONS)}),
            (JOINT_B, "JointScenarioPayloadV2", 102, {"joint_path_id": C, "joint_dimensions": list(JOINT_DIMENSIONS)}),
            (J, "DeterministicStressV2", 102, {}), (K, "OutcomeDispersionV2", 102, {}),
            (L, "EstimationUncertaintyV2", 102, {}), (M, "ExecutionUncertaintyV2", 102, {}),
            (N, "NumericalErrorV2", 102, {})):
            repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at, at, metadata))
        assert index_pretrade_scenario(repo, item) == item.content_hash
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(item, action_hash=B))


def test_capability_matrix_and_frozen_hashes():
    matrix = json.loads(Path("docs/v2/EVIDENCE_CAPABILITY_MATRIX_V1.json").read_text())
    assert matrix["schema_version"] == 1
    required = {"L2_BBO", "L2_DEPTH", "TRADES_AGGRESSOR", "OPEN_INTEREST", "FUNDING_CURRENT_PREDICTED",
        "FUNDING_SETTLED", "BASIS_MARK_INDEX_LAST", "LIQUIDATIONS", "NEWS_EVENT_RAW", "EVENT_EXTRACTION",
        "INSTRUMENT_FILTER_UNIVERSE", "FEE_REVISION", "COLLECTOR_HEALTH", "CLOCK_LATENCY",
        "OFFLINE_RECONNECT", "INCLUSION_RETENTION"}
    assert {row["family"] for row in matrix["rows"]} == required
    fields = {"source_channel", "instrument_product_scope", "fields_units", "cadence_resolution",
        "sequence_update_semantics", "timestamp_semantics", "raw_payload_retention", "gap_reset_reconnect",
        "current_coverage", "known_limitations", "permitted_uses", "prohibited_uses", "status"}
    assert all(fields <= row.keys() and all(row[key] for key in fields) for row in matrix["rows"])
    assert S1_POLICY.policy_hash == "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"
    assert S2_POLICY.policy_hash == "fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd"
    assert SELECTION_POLICY_HASH == "36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac"
